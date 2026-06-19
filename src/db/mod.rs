// Copyright (c) 2012 The LevelDB-Go Authors. All rights reserved.
// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Ported from Pebble's pebble.go, open.go, commit.go, and flushable.go.

//! The database: opening, reading, and writing an on-disk LSM store.
//!
//! [`Db::open`] opens (or creates) a store: it locates the current MANIFEST via the
//! atomic marker files Pebble writes, replays it into a [`VersionSet`], recovers any
//! write-ahead logs into a fresh memtable, then rotates in a new MANIFEST and WAL for the
//! session. Writes ([`Db::set`], [`Db::delete`], [`Db::apply`]) assign sequence numbers,
//! append to the WAL, and update the mutable memtable; when it fills it is flushed
//! synchronously to an L0 sstable and recorded in the MANIFEST. Reads consult the
//! mutable memtable, then immutable memtables, then the sstables of each level.
//!
//! Scope: a synchronous, single-MANIFEST, single-WAL engine. Flushes happen inline when
//! the memtable fills or on an explicit [`Db::flush`], and leveled compaction runs inline
//! afterward (L0-by-count and L1+ by-size triggers). Durability uses buffered writes (no
//! explicit fsync yet).

mod compaction;
mod filenames;
mod merging_iter;

pub use merging_iter::DbIterator;
use merging_iter::InternalIter;

use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::base::comparer::{Comparer, DefaultComparer};
use crate::base::internal_key::{InternalKeyKind, SeqNum};
use crate::batch::Batch;
use crate::manifest::{FileMetadata, NUM_LEVELS, NewFileEntry, VersionEdit, VersionSet};
use crate::memtable::MemTable;
use crate::record;
use crate::sstable::{Reader, Writer, WriterOptions};
use crate::{Error, Result};

/// Options for opening a database.
#[derive(Clone)]
pub struct Options {
    /// The user-key comparer. Must match the store's recorded comparer name.
    pub comparer: Arc<dyn Comparer>,
    /// The memtable arena size in bytes; the memtable is flushed once it fills.
    pub mem_table_size: usize,
    /// Create the store if it does not already exist.
    pub create_if_missing: bool,
    /// Open read-only: no WAL, no flushing, no MANIFEST rotation.
    pub read_only: bool,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            comparer: Arc::new(DefaultComparer),
            mem_table_size: 4 << 20,
            create_if_missing: true,
            read_only: false,
        }
    }
}

/// Mutable database state guarded by a single mutex.
struct State {
    vs: VersionSet,
    mem: Arc<MemTable>,
    imm: Vec<Arc<MemTable>>,
    wal: Option<record::Writer<File>>,
    wal_number: u64,
    manifest: Option<record::Writer<File>>,
    read_only: bool,
}

/// An on-disk LSM key-value database.
pub struct Db {
    dir: PathBuf,
    cmp: Arc<dyn Comparer>,
    mem_table_size: usize,
    state: Mutex<State>,
    cache: Mutex<HashMap<u64, Arc<Reader>>>,
}

impl Db {
    /// Opens the database in `dir`, creating it if `opts.create_if_missing` and absent.
    pub fn open(dir: impl AsRef<Path>, opts: Options) -> Result<Db> {
        let dir = dir.as_ref().to_path_buf();
        let exists = dir.exists();
        if !exists {
            if opts.read_only || !opts.create_if_missing {
                return Err(Error::InvalidState(format!(
                    "db: directory {} does not exist",
                    dir.display()
                )));
            }
            std::fs::create_dir_all(&dir)?;
        }

        let names: Vec<String> = std::fs::read_dir(&dir)?
            .filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().into_string().ok())
            .collect();

        let cmp = opts.comparer.clone();
        let mem = Arc::new(MemTable::new(cmp.clone(), opts.mem_table_size));

        let mut vs = match filenames::current_manifest(&names) {
            Some(manifest_name) => {
                let bytes = std::fs::read(dir.join(&manifest_name))?;
                let vs = VersionSet::load(&bytes, cmp.clone())?;
                // Recover every WAL present (each record is a batch).
                recover_wals(&dir, &names, &mem, vs)?
            }
            None => {
                if !opts.create_if_missing {
                    return Err(Error::corruption(
                        "db: no MANIFEST and create_if_missing=false",
                    ));
                }
                VersionSet::new(cmp.clone())
            }
        };

        if opts.read_only {
            let state = State {
                vs,
                mem,
                imm: Vec::new(),
                wal: None,
                wal_number: 0,
                manifest: None,
                read_only: true,
            };
            return Ok(Db {
                dir,
                cmp,
                mem_table_size: opts.mem_table_size,
                state: Mutex::new(state),
                cache: Mutex::new(HashMap::new()),
            });
        }

        // Read-write: create the LOCK file Pebble expects (advisory only for now; true
        // OS-level locking to prevent concurrent opens is future work), then rotate in a
        // fresh MANIFEST and WAL for this session.
        let _ = File::create(dir.join("LOCK"));
        let manifest_num = vs.allocate_file_number();
        let wal_number = vs.allocate_file_number();
        vs.log_number = wal_number;

        let mut manifest =
            record::Writer::new(File::create(dir.join(filenames::manifest(manifest_num)))?);
        manifest.write_record(&vs.snapshot_edit().encode())?;
        manifest.flush()?;
        update_marker(&dir, &names, &filenames::manifest(manifest_num))?;

        let wal = record::Writer::with_log_num(
            File::create(dir.join(wal_filename(wal_number)))?,
            wal_number as u32,
        );

        let state = State {
            vs,
            mem,
            imm: Vec::new(),
            wal: Some(wal),
            wal_number,
            manifest: Some(manifest),
            read_only: false,
        };
        Ok(Db {
            dir,
            cmp,
            mem_table_size: opts.mem_table_size,
            state: Mutex::new(state),
            cache: Mutex::new(HashMap::new()),
        })
    }

    /// Opens the database in `dir` read-only.
    pub fn open_read_only(dir: impl AsRef<Path>, mut opts: Options) -> Result<Db> {
        opts.read_only = true;
        opts.create_if_missing = false;
        Db::open(dir, opts)
    }

    /// The largest sequence number assigned so far.
    pub fn last_sequence(&self) -> SeqNum {
        self.state.lock().unwrap().vs.last_sequence
    }

    /// Sets `key` to `value`.
    pub fn set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut b = Batch::new();
        b.set(key, value);
        self.apply(b)
    }

    /// Deletes `key`.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let mut b = Batch::new();
        b.delete(key);
        self.apply(b)
    }

    /// Atomically applies all operations in `batch`. Alias for [`Db::apply`].
    pub fn write(&self, batch: Batch) -> Result<()> {
        self.apply(batch)
    }

    /// Atomically applies all operations in `batch`.
    pub fn apply(&self, mut batch: Batch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let mut state = self.state.lock().unwrap();
        if state.read_only {
            return Err(Error::InvalidState("db: opened read-only".into()));
        }

        // Flush once the memtable has used half its arena, leaving the rest as headroom
        // for this batch (the arena fills faster than wire bytes due to per-node
        // overhead, so the threshold is measured in arena bytes).
        if state.mem.size() as usize >= self.mem_table_size / 2 {
            self.flush_locked(&mut state)?;
        }

        let base = state.vs.last_sequence + 1;
        batch.set_seqnum(base);
        if let Some(wal) = state.wal.as_mut() {
            wal.write_record(batch.as_bytes())?;
            wal.flush()?;
        }
        state.mem.apply(&batch)?;
        state.vs.last_sequence = base + u64::from(batch.count()) - 1;
        Ok(())
    }

    /// Flushes the active memtable to an L0 sstable.
    pub fn flush(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if state.read_only {
            return Err(Error::InvalidState("db: opened read-only".into()));
        }
        self.flush_locked(&mut state)
    }

    /// Flushes the current memtable (if non-empty) to a new L0 sstable, rotates the WAL,
    /// records the change in the MANIFEST, and removes the now-obsolete logs.
    fn flush_locked(&self, state: &mut State) -> Result<()> {
        if state.mem.is_empty() {
            return Ok(());
        }
        let mem = Arc::clone(&state.mem);
        let file_num = state.vs.allocate_file_number();
        let meta = write_memtable_to_sstable(&self.dir, &self.cmp, file_num, &mem)?;

        let new_wal = state.vs.allocate_file_number();
        let wal = record::Writer::with_log_num(
            File::create(self.dir.join(wal_filename(new_wal)))?,
            new_wal as u32,
        );
        let old_wal = state.wal_number;
        state.wal = Some(wal);
        state.wal_number = new_wal;
        state.mem = Arc::new(MemTable::new(self.cmp.clone(), self.mem_table_size));
        state.vs.log_number = new_wal;

        let edit = VersionEdit {
            log_number: Some(new_wal),
            next_file_number: Some(state.vs.next_file_number),
            last_sequence: Some(state.vs.last_sequence),
            new_files: vec![NewFileEntry { level: 0, meta }],
            ..Default::default()
        };
        state.vs.apply(&edit)?;
        if let Some(mw) = state.manifest.as_mut() {
            mw.write_record(&edit.encode())?;
            mw.flush()?;
        }

        // Remove logs that predate the new WAL; their data is now in the sstable.
        for num in [old_wal] {
            if num != 0 && num != new_wal {
                let _ = std::fs::remove_file(self.dir.join(wal_filename(num)));
            }
        }

        // Keep the LSM in shape (e.g. drain L0 once it accumulates enough files).
        self.maybe_compact(state)?;
        Ok(())
    }

    /// Looks up `key`, returning its value or `None` if absent or deleted.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let snapshot = self.state.lock().unwrap().vs.last_sequence;
        self.get_at(key, snapshot)
    }

    /// Looks up `key` as visible at sequence number `snapshot`.
    pub fn get_at(&self, key: &[u8], snapshot: SeqNum) -> Result<Option<Vec<u8>>> {
        // Snapshot the volatile state under the lock, then read without holding it.
        let (mem, imm, version) = {
            let state = self.state.lock().unwrap();
            (
                Arc::clone(&state.mem),
                state.imm.clone(),
                state.vs.current.clone(),
            )
        };

        if let Some(hit) = mem.get(key, snapshot) {
            return Ok(visible_value(hit));
        }
        for m in imm.iter().rev() {
            if let Some(hit) = m.get(key, snapshot) {
                return Ok(visible_value(hit));
            }
        }
        for level in 0..NUM_LEVELS {
            for f in version.overlapping(self.cmp.as_ref(), level, key) {
                let reader = self.open_reader(f.file_num)?;
                if let Some((kind, value)) = reader.get(key, snapshot)? {
                    return Ok(visible_value((kind, value)));
                }
            }
        }
        Ok(None)
    }

    /// Returns a forward iterator over all keys at the latest sequence number.
    pub fn iter(&self) -> Result<DbIterator> {
        let snapshot = self.state.lock().unwrap().vs.last_sequence;
        self.iter_at(snapshot)
    }

    /// Returns a forward iterator over all keys as visible at `snapshot`.
    pub fn iter_at(&self, snapshot: SeqNum) -> Result<DbIterator> {
        let (mem, imm, version) = {
            let state = self.state.lock().unwrap();
            (
                Arc::clone(&state.mem),
                state.imm.clone(),
                state.vs.current.clone(),
            )
        };

        let mut sources: Vec<Box<dyn InternalIter>> = Vec::new();
        sources.push(Box::new(mem.scan()));
        for m in imm.iter().rev() {
            sources.push(Box::new(m.scan()));
        }
        for level in version.levels.iter() {
            for f in level {
                let reader = self.open_reader(f.file_num)?;
                sources.push(Box::new(reader.iter()?));
            }
        }
        DbIterator::new(sources, snapshot, self.cmp.clone())
    }

    /// Opens (or returns a cached) reader for the sstable with the given file number.
    fn open_reader(&self, file_num: u64) -> Result<Arc<Reader>> {
        if let Some(r) = self.cache.lock().unwrap().get(&file_num) {
            return Ok(Arc::clone(r));
        }
        let bytes = std::fs::read(self.dir.join(filenames::table(file_num)))?;
        let reader = Arc::new(Reader::open(bytes, self.cmp.clone())?);
        self.cache
            .lock()
            .unwrap()
            .insert(file_num, Arc::clone(&reader));
        Ok(reader)
    }

    /// The user-key comparer.
    pub fn comparer(&self) -> &Arc<dyn Comparer> {
        &self.cmp
    }

    /// The number of live sstables at each level (`[0]` is L0). Useful for tests and
    /// metrics.
    pub fn level_file_counts(&self) -> [usize; NUM_LEVELS] {
        let state = self.state.lock().unwrap();
        std::array::from_fn(|i| state.vs.current.levels[i].len())
    }

    /// Returns a point-in-time [`Metrics`] snapshot of the LSM tree.
    pub fn metrics(&self) -> Metrics {
        let state = self.state.lock().unwrap();
        let level_files = std::array::from_fn(|i| state.vs.current.levels[i].len());
        let level_bytes =
            std::array::from_fn(|i| state.vs.current.levels[i].iter().map(|f| f.size).sum());
        Metrics {
            level_files,
            level_bytes,
            last_sequence: state.vs.last_sequence,
        }
    }

    /// Captures a read snapshot at the current sequence number. Reads through the
    /// returned [`Snapshot`] see a consistent view even as later writes are applied.
    pub fn snapshot(&self) -> Snapshot<'_> {
        Snapshot {
            db: self,
            seqnum: self.last_sequence(),
        }
    }
}

/// A consistent read view of the database at a fixed sequence number.
pub struct Snapshot<'a> {
    db: &'a Db,
    seqnum: SeqNum,
}

impl Snapshot<'_> {
    /// The sequence number this snapshot reads at.
    pub fn sequence_number(&self) -> SeqNum {
        self.seqnum
    }

    /// Looks up `key` as of the snapshot.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db.get_at(key, self.seqnum)
    }

    /// Returns a forward iterator over the snapshot's view.
    pub fn iter(&self) -> Result<DbIterator> {
        self.db.iter_at(self.seqnum)
    }
}

/// A point-in-time summary of the LSM tree's shape.
#[derive(Clone, Debug)]
pub struct Metrics {
    /// Number of sstables at each level (`[0]` is L0).
    pub level_files: [usize; NUM_LEVELS],
    /// Total sstable bytes at each level.
    pub level_bytes: [u64; NUM_LEVELS],
    /// The largest sequence number assigned so far.
    pub last_sequence: SeqNum,
}

/// Maps a `(kind, value)` lookup result to a user-visible value, treating tombstones as
/// absent.
fn visible_value((kind, value): (InternalKeyKind, Vec<u8>)) -> Option<Vec<u8>> {
    match kind {
        InternalKeyKind::Delete | InternalKeyKind::SingleDelete | InternalKeyKind::DeleteSized => {
            None
        }
        _ => Some(value),
    }
}

/// The filename of the WAL with the given number.
fn wal_filename(num: u64) -> String {
    format!("{num:06}.log")
}

/// Replays every `*.log` file present (in increasing number order) into `mem`, returning
/// the version set with `last_sequence` advanced past the recovered batches.
fn recover_wals(
    dir: &Path,
    names: &[String],
    mem: &Arc<MemTable>,
    mut vs: VersionSet,
) -> Result<VersionSet> {
    let mut logs: Vec<(u64, &String)> = names
        .iter()
        .filter_map(|n| {
            n.strip_suffix(".log")
                .and_then(|s| s.parse().ok())
                .map(|num| (num, n))
        })
        .collect();
    logs.sort_by_key(|(num, _)| *num);

    let mut last_seq = vs.last_sequence;
    for (num, name) in logs {
        let bytes = std::fs::read(dir.join(name))?;
        let mut reader = record::Reader::new(std::io::Cursor::new(bytes), num as u32);
        while let Some(rec) = reader.read_record()? {
            let batch = Batch::from_bytes(rec)?;
            if batch.is_empty() {
                continue;
            }
            mem.apply(&batch)?;
            last_seq = last_seq.max(batch.seqnum() + u64::from(batch.count()) - 1);
        }
    }
    vs.last_sequence = last_seq;
    Ok(vs)
}

/// Writes every entry of `mem` (in internal-key order) to `<dir>/<file_num>.sst`, and
/// returns the file's metadata.
fn write_memtable_to_sstable(
    dir: &Path,
    cmp: &Arc<dyn Comparer>,
    file_num: u64,
    mem: &Arc<MemTable>,
) -> Result<FileMetadata> {
    let path = dir.join(filenames::table(file_num));
    let file = File::create(&path)?;
    let mut w = Writer::new(file, cmp.clone(), WriterOptions::default());

    let mut it = mem.iter();
    it.first();
    let mut smallest: Option<Vec<u8>> = None;
    let mut largest: Vec<u8> = Vec::new();
    let mut smallest_seq = u64::MAX;
    let mut largest_seq = 0u64;
    let mut key_buf = Vec::new();
    while it.valid() {
        key_buf.clear();
        key_buf.extend_from_slice(it.user_key());
        key_buf.extend_from_slice(&it.trailer().to_le_bytes());
        w.add(&key_buf, it.value())?;
        if smallest.is_none() {
            smallest = Some(key_buf.clone());
        }
        largest.clear();
        largest.extend_from_slice(&key_buf);
        let seq = it.trailer() >> 8;
        smallest_seq = smallest_seq.min(seq);
        largest_seq = largest_seq.max(seq);
        it.next();
    }
    w.finish()?;

    Ok(FileMetadata {
        file_num,
        size: std::fs::metadata(&path)?.len(),
        smallest: smallest.unwrap_or_default(),
        largest,
        smallest_seqnum: smallest_seq.min(largest_seq),
        largest_seqnum: largest_seq,
    })
}

/// Writes a new `manifest` marker pointing at `value`, with an `iter` one greater than
/// any existing marker, and removes superseded marker files.
fn update_marker(dir: &Path, names: &[String], value: &str) -> Result<()> {
    let mut max_iter = 0u64;
    let mut old: Vec<&String> = Vec::new();
    for n in names {
        if let Some(rest) = n.strip_prefix("marker.manifest.") {
            old.push(n);
            if let Some((iter_str, _)) = rest.split_once('.')
                && let Ok(iter) = iter_str.parse::<u64>()
            {
                max_iter = max_iter.max(iter);
            }
        }
    }
    let new_name = format!("marker.manifest.{:06}.{}", max_iter + 1, value);
    std::fs::write(dir.join(&new_name), b"")?;
    for n in old {
        let _ = std::fs::remove_file(dir.join(n));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_dir() -> PathBuf {
        let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir().join(format!("pebbledb-dbw-{}-{}", std::process::id(), n));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn collect(db: &Db) -> Vec<(String, String)> {
        let mut it = db.iter().unwrap();
        let mut out = Vec::new();
        it.first().unwrap();
        while it.valid() {
            out.push((
                String::from_utf8(it.key().to_vec()).unwrap(),
                String::from_utf8(it.value().to_vec()).unwrap(),
            ));
            it.next().unwrap();
        }
        out
    }

    #[test]
    fn create_set_get_delete() {
        let dir = temp_dir();
        let db = Db::open(&dir, Options::default()).unwrap();
        db.set(b"a", b"1").unwrap();
        db.set(b"b", b"2").unwrap();
        db.set(b"c", b"3").unwrap();
        assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
        db.delete(b"b").unwrap();
        assert_eq!(db.get(b"b").unwrap(), None);
        db.set(b"a", b"1-new").unwrap();
        assert_eq!(db.get(b"a").unwrap(), Some(b"1-new".to_vec()));
        assert_eq!(
            collect(&db),
            vec![
                ("a".to_string(), "1-new".to_string()),
                ("c".to_string(), "3".to_string())
            ]
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn flush_then_read_from_sstable() {
        let dir = temp_dir();
        let db = Db::open(&dir, Options::default()).unwrap();
        for i in 0..50u32 {
            db.set(
                format!("key{i:03}").as_bytes(),
                format!("val{i}").as_bytes(),
            )
            .unwrap();
        }
        db.flush().unwrap(); // memtable -> L0 sstable
        // After flush the memtable is empty; reads come from the sstable.
        assert_eq!(db.get(b"key000").unwrap(), Some(b"val0".to_vec()));
        assert_eq!(db.get(b"key049").unwrap(), Some(b"val49".to_vec()));
        // Write more after flush; live in the new memtable.
        db.set(b"key049", b"updated").unwrap();
        assert_eq!(db.get(b"key049").unwrap(), Some(b"updated".to_vec()));
        assert_eq!(collect(&db).len(), 50);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn reopen_recovers_from_wal() {
        let dir = temp_dir();
        {
            let db = Db::open(&dir, Options::default()).unwrap();
            db.set(b"persisted", b"yes").unwrap();
            db.set(b"x", b"1").unwrap();
            db.delete(b"x").unwrap();
            // No explicit flush: data lives only in the WAL + memtable.
        }
        // Reopen: the WAL is replayed into a fresh memtable.
        let db = Db::open(&dir, Options::default()).unwrap();
        assert_eq!(db.get(b"persisted").unwrap(), Some(b"yes".to_vec()));
        assert_eq!(db.get(b"x").unwrap(), None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn reopen_recovers_from_flushed_sstable() {
        let dir = temp_dir();
        {
            let db = Db::open(&dir, Options::default()).unwrap();
            db.set(b"flushed", b"data").unwrap();
            db.flush().unwrap();
            db.set(b"afterflush", b"more").unwrap();
        }
        let db = Db::open(&dir, Options::default()).unwrap();
        assert_eq!(db.get(b"flushed").unwrap(), Some(b"data".to_vec()));
        assert_eq!(db.get(b"afterflush").unwrap(), Some(b"more".to_vec()));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn auto_flush_on_full_memtable() {
        let dir = temp_dir();
        // Tiny memtable forces several automatic flushes.
        let opts = Options {
            mem_table_size: 16 * 1024,
            ..Default::default()
        };
        let db = Db::open(&dir, opts).unwrap();
        for i in 0..2000u32 {
            db.set(format!("k{i:06}").as_bytes(), format!("v{i:06}").as_bytes())
                .unwrap();
        }
        // Spot-check values spanning multiple flushed sstables and the live memtable.
        assert_eq!(db.get(b"k000000").unwrap(), Some(b"v000000".to_vec()));
        assert_eq!(db.get(b"k001000").unwrap(), Some(b"v001000".to_vec()));
        assert_eq!(db.get(b"k001999").unwrap(), Some(b"v001999".to_vec()));
        assert_eq!(db.get(b"k002000").unwrap(), None);
        assert_eq!(collect(&db).len(), 2000);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn compaction_drains_l0_and_preserves_reads() {
        let dir = temp_dir();
        let opts = Options {
            mem_table_size: 16 * 1024,
            ..Default::default()
        };
        let db = Db::open(&dir, opts).unwrap();
        // Many distinct keys across many flushes; L0 should be compacted into deeper
        // levels rather than growing without bound.
        for i in 0..3000u32 {
            db.set(format!("k{i:06}").as_bytes(), format!("v{i:06}").as_bytes())
                .unwrap();
        }
        let counts = db.level_file_counts();
        assert!(counts[0] < 4, "L0 should be drained, got {counts:?}");
        assert!(
            counts[1..].iter().sum::<usize>() > 0,
            "deeper levels should hold files, got {counts:?}"
        );
        // All reads remain correct after compaction.
        for i in (0..3000u32).step_by(137) {
            assert_eq!(
                db.get(format!("k{i:06}").as_bytes()).unwrap(),
                Some(format!("v{i:06}").into_bytes())
            );
        }
        assert_eq!(collect(&db).len(), 3000);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn compaction_collapses_overwrites_and_deletes() {
        let dir = temp_dir();
        let opts = Options {
            mem_table_size: 16 * 1024,
            ..Default::default()
        };
        let db = Db::open(&dir, opts).unwrap();
        // Repeatedly overwrite a small key set, then delete half — forcing flushes and
        // compactions that must collapse history correctly.
        for round in 0..200u32 {
            for k in 0..20u32 {
                db.set(
                    format!("key{k:02}").as_bytes(),
                    format!("round{round}").as_bytes(),
                )
                .unwrap();
            }
        }
        for k in 0..10u32 {
            db.delete(format!("key{k:02}").as_bytes()).unwrap();
        }
        db.flush().unwrap();

        // Deleted keys are gone; survivors hold the final round's value.
        for k in 0..10u32 {
            assert_eq!(db.get(format!("key{k:02}").as_bytes()).unwrap(), None);
        }
        for k in 10..20u32 {
            assert_eq!(
                db.get(format!("key{k:02}").as_bytes()).unwrap(),
                Some(b"round199".to_vec())
            );
        }
        assert_eq!(collect(&db).len(), 10);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn read_only_rejects_writes() {
        let dir = temp_dir();
        {
            let db = Db::open(&dir, Options::default()).unwrap();
            db.set(b"k", b"v").unwrap();
            db.flush().unwrap();
        }
        let db = Db::open_read_only(&dir, Options::default()).unwrap();
        assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
        assert!(db.set(b"x", b"y").is_err());
        std::fs::remove_dir_all(&dir).ok();
    }
}
