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
//! afterward (L0-by-count and L1+ by-size triggers). The WAL is fsynced after each write
//! when `Options::wal_sync` is set (the default), and the MANIFEST is fsynced on every
//! edit.

mod compaction;
mod filenames;
mod merging_iter;

use merging_iter::InternalIter;
pub use merging_iter::{DbIterator, IterOptions};

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::base::comparer::{Comparer, DefaultComparer};
use crate::base::internal_key::{InternalKey, InternalKeyKind, SeqNum, encoded_user_key};
use crate::base::range_del::max_covering_seqnum;
use crate::base::range_key::RangeKeyEntry;
use crate::batch::Batch;
use crate::manifest::{FileMetadata, NUM_LEVELS, NewFileEntry, VersionEdit, VersionSet};
use crate::memtable::MemTable;
use crate::record;
use crate::sstable::{Reader, Writer, WriterOptions};
use crate::vfs::{DirLock, DiskFs, Fs, WritableFile};
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
    /// fsync the WAL after every write so committed data survives a crash (default
    /// `true`). Disabling trades durability for throughput.
    pub wal_sync: bool,
    /// Optional listener notified of flush and compaction events.
    pub event_listener: Option<Arc<dyn EventListener>>,
    /// Optional merge operator. Required to read keys written with `merge`; without it,
    /// a merge resolves to its newest operand.
    pub merger: Option<Arc<dyn crate::base::merge::Merger>>,
    /// Size in bytes of the shared block cache for decompressed blocks (default 8 MiB).
    /// Zero disables block caching.
    pub block_cache_size: usize,
    /// Maximum number of sstable readers kept open (default 1000). The least-recently
    /// used reader is evicted when the limit is exceeded.
    pub max_open_files: usize,
    /// The filesystem the database performs all of its I/O through (default [`DiskFs`]).
    /// Use [`crate::vfs::MemFs`] for fully in-memory operation.
    pub fs: Arc<dyn Fs>,
}

/// A listener notified of background-style events (flushes and compactions). All methods
/// have default no-op implementations, so implementors override only what they need.
pub trait EventListener: Send + Sync {
    /// Called after a memtable is flushed to an L0 sstable.
    fn on_flush_end(&self, _file_num: u64, _bytes: u64) {}
    /// Called after a compaction completes.
    fn on_compaction_end(&self, _output_level: usize, _input_files: usize, _output_files: usize) {}
}

impl Default for Options {
    fn default() -> Self {
        Options {
            comparer: Arc::new(DefaultComparer),
            mem_table_size: 4 << 20,
            create_if_missing: true,
            read_only: false,
            wal_sync: true,
            event_listener: None,
            merger: None,
            block_cache_size: 8 << 20,
            max_open_files: 1000,
            fs: Arc::new(DiskFs),
        }
    }
}

/// Mutable database state guarded by a single mutex.
struct State {
    vs: VersionSet,
    mem: Arc<MemTable>,
    imm: Vec<Arc<MemTable>>,
    wal: Option<record::Writer<Box<dyn WritableFile>>>,
    wal_number: u64,
    manifest: Option<record::Writer<Box<dyn WritableFile>>>,
    read_only: bool,
    /// Number of memtable flushes performed this session.
    flush_count: u64,
    /// Number of compactions performed this session.
    compaction_count: u64,
}

/// An on-disk LSM key-value database.
pub struct Db {
    dir: PathBuf,
    cmp: Arc<dyn Comparer>,
    mem_table_size: usize,
    wal_sync: bool,
    state: Mutex<State>,
    cache: Mutex<HashMap<u64, Arc<Reader>>>,
    /// Sequence numbers of currently-open snapshots. Compaction retains the versions
    /// they need.
    snapshots: Mutex<Vec<SeqNum>>,
    /// Optional event listener.
    listener: Option<Arc<dyn EventListener>>,
    /// Optional merge operator.
    merger: Option<Arc<dyn crate::base::merge::Merger>>,
    /// Shared block cache (None when disabled).
    block_cache: Option<Arc<crate::cache::BlockCache>>,
    /// Maximum number of cached open readers.
    max_open_files: usize,
    /// The filesystem all I/O goes through.
    fs: Arc<dyn Fs>,
    /// The held exclusive directory lock (released on drop). `None` when read-only.
    _lock: Option<Box<dyn DirLock>>,
}

impl Db {
    /// Opens the database in `dir`, creating it if `opts.create_if_missing` and absent.
    pub fn open(dir: impl AsRef<Path>, opts: Options) -> Result<Db> {
        let dir = dir.as_ref().to_path_buf();
        let fs = opts.fs.clone();
        let exists = fs.exists(&dir);
        if !exists {
            if opts.read_only || !opts.create_if_missing {
                return Err(Error::InvalidState(format!(
                    "db: directory {} does not exist",
                    dir.display()
                )));
            }
            fs.create_dir_all(&dir)?;
        }

        let mut names: Vec<String> = fs.list(&dir)?;
        names.sort();

        let cmp = opts.comparer.clone();
        let mem = Arc::new(MemTable::new(cmp.clone(), opts.mem_table_size));

        let mut vs = match filenames::current_manifest(&names) {
            Some(manifest_name) => {
                let bytes = fs.read(&dir.join(&manifest_name))?;
                let vs = VersionSet::load(&bytes, cmp.clone())?;
                // Recover every WAL present (each record is a batch).
                recover_wals(fs.as_ref(), &dir, &names, &mem, vs)?
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
                flush_count: 0,
                compaction_count: 0,
            };
            return Ok(Db {
                dir,
                cmp,
                mem_table_size: opts.mem_table_size,
                wal_sync: opts.wal_sync,
                state: Mutex::new(state),
                cache: Mutex::new(HashMap::new()),
                snapshots: Mutex::new(Vec::new()),
                listener: opts.event_listener.clone(),
                merger: opts.merger.clone(),
                block_cache: if opts.block_cache_size > 0 {
                    Some(Arc::new(crate::cache::BlockCache::new(
                        opts.block_cache_size,
                    )))
                } else {
                    None
                },
                max_open_files: opts.max_open_files.max(1),
                fs,
                _lock: None,
            });
        }

        // Read-write: take the exclusive directory lock to prevent concurrent opens, then
        // rotate in a fresh MANIFEST and WAL for this session.
        let lock = fs.lock(&dir.join("LOCK"))?;
        let manifest_num = vs.allocate_file_number();
        let wal_number = vs.allocate_file_number();
        vs.log_number = wal_number;

        let mut manifest =
            record::Writer::new(fs.create(&dir.join(filenames::manifest(manifest_num)))?);
        manifest.write_record(&vs.snapshot_edit().encode())?;
        manifest.sync_all()?;
        update_marker(
            fs.as_ref(),
            &dir,
            &names,
            &filenames::manifest(manifest_num),
        )?;
        fs.sync_dir(&dir)?;

        let wal = record::Writer::with_log_num(
            fs.create(&dir.join(wal_filename(wal_number)))?,
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
            flush_count: 0,
            compaction_count: 0,
        };
        Ok(Db {
            dir,
            cmp,
            mem_table_size: opts.mem_table_size,
            wal_sync: opts.wal_sync,
            state: Mutex::new(state),
            cache: Mutex::new(HashMap::new()),
            snapshots: Mutex::new(Vec::new()),
            listener: opts.event_listener.clone(),
            merger: opts.merger.clone(),
            block_cache: if opts.block_cache_size > 0 {
                Some(Arc::new(crate::cache::BlockCache::new(
                    opts.block_cache_size,
                )))
            } else {
                None
            },
            max_open_files: opts.max_open_files.max(1),
            fs,
            _lock: Some(lock),
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

    /// Records a merge operand for `key`, combined with prior values by the configured
    /// merge operator at read time.
    pub fn merge(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut b = Batch::new();
        b.merge(key, value);
        self.apply(b)
    }

    /// Deletes every key in the half-open user-key range `[start, end)`.
    pub fn delete_range(&self, start: &[u8], end: &[u8]) -> Result<()> {
        let mut b = Batch::new();
        b.delete_range(start, end);
        self.apply(b)
    }

    /// Sets a range key over `[start, end)` at `suffix` to `value`.
    pub fn range_key_set(
        &self,
        start: &[u8],
        end: &[u8],
        suffix: &[u8],
        value: &[u8],
    ) -> Result<()> {
        let mut b = Batch::new();
        b.range_key_set(start, end, suffix, value);
        self.apply(b)
    }

    /// Removes the range key at `suffix` over `[start, end)`.
    pub fn range_key_unset(&self, start: &[u8], end: &[u8], suffix: &[u8]) -> Result<()> {
        let mut b = Batch::new();
        b.range_key_unset(start, end, suffix);
        self.apply(b)
    }

    /// Deletes all range keys over `[start, end)`.
    pub fn range_key_delete(&self, start: &[u8], end: &[u8]) -> Result<()> {
        let mut b = Batch::new();
        b.range_key_delete(start, end);
        self.apply(b)
    }

    /// Returns the range-key entries covering `user_key`, newest first (by sequence
    /// number), across the memtable, immutable memtables, and all sstables.
    ///
    /// This returns the raw entries; coalescing `RANGEKEYSET`/`UNSET`/`DEL` into an
    /// effective set of suffix/value pairs (and iterator masking) is refined in a later
    /// phase.
    pub fn range_keys_covering(&self, user_key: &[u8]) -> Result<Vec<RangeKeyEntry>> {
        let (mem, imm, version) = {
            let state = self.state.lock().unwrap();
            (
                Arc::clone(&state.mem),
                state.imm.clone(),
                state.vs.current.clone(),
            )
        };
        let cmp = self.cmp.as_ref();
        let mut out = Vec::new();
        let mut collect = |entries: Vec<RangeKeyEntry>| -> Result<()> {
            for e in entries {
                if e.covers(cmp, user_key)? {
                    out.push(e);
                }
            }
            Ok(())
        };
        collect(mem.range_keys())?;
        for m in imm.iter().rev() {
            collect(m.range_keys())?;
        }
        for level in version.levels.iter() {
            for f in level {
                let reader = self.open_reader(f.file_num)?;
                collect(reader.range_keys().to_vec())?;
            }
        }
        out.sort_by_key(|e| std::cmp::Reverse(e.seqnum));
        Ok(out)
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
            if self.wal_sync {
                wal.sync_all()?;
            } else {
                wal.flush()?;
            }
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
        let meta =
            write_memtable_to_sstable(self.fs.as_ref(), &self.dir, &self.cmp, file_num, &mem)?;
        let flushed_bytes = meta.size;

        let new_wal = state.vs.allocate_file_number();
        let wal = record::Writer::with_log_num(
            self.fs.create(&self.dir.join(wal_filename(new_wal)))?,
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
            mw.sync_all()?;
        }
        state.flush_count += 1;
        if let Some(l) = &self.listener {
            l.on_flush_end(file_num, flushed_bytes);
        }

        // Remove logs that predate the new WAL; their data is now in the sstable.
        for num in [old_wal] {
            if num != 0 && num != new_wal {
                let _ = self.fs.remove(&self.dir.join(wal_filename(num)));
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
    ///
    /// Sources are consulted newest-first. As each is examined its covering range
    /// tombstones (with `seqnum <= snapshot`) raise a running maximum; the first source
    /// that holds a point entry for `key` decides the result: the entry is visible only
    /// if its sequence number exceeds every covering tombstone seen so far (and is a
    /// non-tombstone kind).
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

        let cmp = self.cmp.as_ref();
        let mut max_rts = 0u64;
        // Merge operands gathered newest-first until a terminator (Set/Delete/range
        // tombstone) is reached.
        let mut operands: Vec<Vec<u8>> = Vec::new();
        let mut base: Option<Vec<u8>> = None;

        // Resolves a source's versions into the running merge state; returns true if a
        // terminator was reached (no need to consult older sources).
        let mut resolve_versions =
            |versions: Vec<(SeqNum, InternalKeyKind, Vec<u8>)>, max_rts: u64| -> Option<bool> {
                for (seq, kind, value) in versions {
                    if seq <= max_rts {
                        base = None;
                        return Some(true); // shadowed by a range tombstone: acts as delete
                    }
                    match kind {
                        InternalKeyKind::Merge => operands.push(value),
                        InternalKeyKind::Set | InternalKeyKind::SetWithDelete => {
                            base = Some(value);
                            return Some(true);
                        }
                        InternalKeyKind::Delete
                        | InternalKeyKind::SingleDelete
                        | InternalKeyKind::DeleteSized => {
                            base = None;
                            return Some(true);
                        }
                        _ => {}
                    }
                }
                None
            };

        let mut terminated = false;
        // Mutable memtable.
        max_rts = max_rts.max(max_covering_seqnum(
            &mem.range_tombstones(),
            cmp,
            key,
            snapshot,
        ));
        if resolve_versions(mem.lookup_versions(key, snapshot), max_rts).is_some() {
            terminated = true;
        }
        // Immutable memtables, newest first.
        if !terminated {
            for m in imm.iter().rev() {
                max_rts = max_rts.max(max_covering_seqnum(
                    &m.range_tombstones(),
                    cmp,
                    key,
                    snapshot,
                ));
                if resolve_versions(m.lookup_versions(key, snapshot), max_rts).is_some() {
                    terminated = true;
                    break;
                }
            }
        }
        // Sstables: L0 newest-first, then L1..L6.
        if !terminated {
            'levels: for level in 0..NUM_LEVELS {
                for f in version.overlapping(cmp, level, key) {
                    let reader = self.open_reader(f.file_num)?;
                    max_rts = max_rts.max(max_covering_seqnum(
                        reader.range_tombstones(),
                        cmp,
                        key,
                        snapshot,
                    ));
                    if resolve_versions(reader.lookup_versions(key, snapshot)?, max_rts).is_some() {
                        break 'levels;
                    }
                }
            }
        }

        if operands.is_empty() {
            // No merge operands: plain point semantics.
            return Ok(base);
        }
        // Apply merge operands (chronological / oldest-first) over the base value.
        operands.reverse();
        match &self.merger {
            Some(m) => Ok(Some(m.full_merge(key, base.as_deref(), &operands))),
            None => Ok(operands.pop()), // no merger configured: newest operand
        }
    }

    /// Returns a forward iterator over all keys at the latest sequence number.
    pub fn iter(&self) -> Result<DbIterator> {
        let snapshot = self.state.lock().unwrap().vs.last_sequence;
        self.iter_at(snapshot)
    }

    /// Returns a forward iterator over all keys as visible at `snapshot`.
    pub fn iter_at(&self, snapshot: SeqNum) -> Result<DbIterator> {
        self.iter_at_with_options(snapshot, IterOptions::default())
    }

    /// Returns an iterator with bounds (and other [`IterOptions`]) over the latest view.
    pub fn iter_with_options(&self, opts: IterOptions) -> Result<DbIterator> {
        let snapshot = self.state.lock().unwrap().vs.last_sequence;
        self.iter_at_with_options(snapshot, opts)
    }

    /// Returns an iterator with bounds over the view as visible at `snapshot`.
    pub fn iter_at_with_options(&self, snapshot: SeqNum, opts: IterOptions) -> Result<DbIterator> {
        let (mem, imm, version) = {
            let state = self.state.lock().unwrap();
            (
                Arc::clone(&state.mem),
                state.imm.clone(),
                state.vs.current.clone(),
            )
        };

        let mut sources: Vec<Box<dyn InternalIter>> = Vec::new();
        let mut tombstones = mem.range_tombstones();
        sources.push(Box::new(mem.scan()));
        for m in imm.iter().rev() {
            tombstones.extend(m.range_tombstones());
            sources.push(Box::new(m.scan()));
        }
        for level in version.levels.iter() {
            for f in level {
                let reader = self.open_reader(f.file_num)?;
                tombstones.extend_from_slice(reader.range_tombstones());
                sources.push(Box::new(reader.iter()?));
            }
        }
        DbIterator::with_options(
            sources,
            snapshot,
            self.cmp.clone(),
            tombstones,
            self.merger.clone(),
            opts,
        )
    }

    /// Opens (or returns a cached) reader for the sstable with the given file number.
    fn open_reader(&self, file_num: u64) -> Result<Arc<Reader>> {
        if let Some(r) = self.cache.lock().unwrap().get(&file_num) {
            return Ok(Arc::clone(r));
        }
        let bytes = self.fs.read(&self.dir.join(filenames::table(file_num)))?;
        let reader = Arc::new(Reader::open_with_cache(
            bytes,
            self.cmp.clone(),
            file_num,
            self.block_cache.clone(),
        )?);
        let mut cache = self.cache.lock().unwrap();
        // Bound the number of open readers. Once at capacity, drop entries that are not
        // referenced elsewhere before inserting the new reader.
        while cache.len() >= self.max_open_files {
            let victim = cache
                .iter()
                .find(|(_, r)| Arc::strong_count(r) == 1)
                .map(|(&k, _)| k);
            match victim {
                Some(k) => {
                    cache.remove(&k);
                }
                None => break, // every cached reader is in use; allow temporary overflow
            }
        }
        cache.insert(file_num, Arc::clone(&reader));
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
        let (block_cache_hits, block_cache_misses) = match &self.block_cache {
            Some(c) => (c.hits(), c.misses()),
            None => (0, 0),
        };
        Metrics {
            level_files,
            level_bytes,
            last_sequence: state.vs.last_sequence,
            flush_count: state.flush_count,
            compaction_count: state.compaction_count,
            block_cache_hits,
            block_cache_misses,
        }
    }

    /// Captures a read snapshot at the current sequence number. Reads through the
    /// returned [`Snapshot`] see a consistent view even as later writes are applied, and
    /// compaction retains every version the snapshot can observe until it is dropped.
    pub fn snapshot(&self) -> Snapshot<'_> {
        let seqnum = self.last_sequence();
        self.snapshots.lock().unwrap().push(seqnum);
        Snapshot { db: self, seqnum }
    }

    /// The sorted sequence numbers of currently-open snapshots.
    fn open_snapshots(&self) -> Vec<SeqNum> {
        let mut s = self.snapshots.lock().unwrap().clone();
        s.sort_unstable();
        s.dedup();
        s
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

impl Drop for Snapshot<'_> {
    fn drop(&mut self) {
        let mut snaps = self.db.snapshots.lock().unwrap();
        if let Some(pos) = snaps.iter().position(|&s| s == self.seqnum) {
            snaps.swap_remove(pos);
        }
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
    /// Number of memtable flushes performed this session.
    pub flush_count: u64,
    /// Number of compactions performed this session.
    pub compaction_count: u64,
    /// Number of block-cache hits so far (0 if caching is disabled).
    pub block_cache_hits: u64,
    /// Number of block-cache misses so far (0 if caching is disabled).
    pub block_cache_misses: u64,
}

/// The filename of the WAL with the given number.
fn wal_filename(num: u64) -> String {
    format!("{num:06}.log")
}

/// Replays every `*.log` file present (in increasing number order) into `mem`, returning
/// the version set with `last_sequence` advanced past the recovered batches.
fn recover_wals(
    fs: &dyn Fs,
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
        let bytes = fs.read(&dir.join(name))?;
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
    fs: &dyn Fs,
    dir: &Path,
    cmp: &Arc<dyn Comparer>,
    file_num: u64,
    mem: &Arc<MemTable>,
) -> Result<FileMetadata> {
    let path = dir.join(filenames::table(file_num));
    let file = fs.create(&path)?;
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

    // Write the memtable's range tombstones, in start-key order, into the range-del
    // block. Each contributes its bounds to the file's key range.
    let mut tombstones = mem.range_tombstones();
    tombstones.sort_by(|a, b| {
        cmp.compare(&a.start, &b.start)
            .then(b.seqnum.cmp(&a.seqnum))
    });
    for t in &tombstones {
        let start_ikey =
            InternalKey::new(t.start.clone(), t.seqnum, InternalKeyKind::RangeDelete).encode();
        w.add(&start_ikey, &t.end)?;
        if smallest.is_none()
            || cmp.compare(&t.start, encoded_user_key(smallest.as_ref().unwrap()))
                == std::cmp::Ordering::Less
        {
            smallest = Some(start_ikey.clone());
        }
        // The tombstone's exclusive end extends the largest user-key bound.
        if largest.is_empty()
            || cmp.compare(&t.end, encoded_user_key(&largest)) == std::cmp::Ordering::Greater
        {
            largest =
                InternalKey::new(t.end.clone(), t.seqnum, InternalKeyKind::RangeDelete).encode();
        }
        smallest_seq = smallest_seq.min(t.seqnum);
        largest_seq = largest_seq.max(t.seqnum);
    }

    // Write the memtable's range keys, in internal-key order, into the range-key block.
    let mut range_keys = mem.range_keys();
    range_keys.sort_by(|a, b| {
        cmp.compare(&a.start, &b.start)
            .then(b.seqnum.cmp(&a.seqnum))
            .then(b.kind.as_u8().cmp(&a.kind.as_u8()))
    });
    for rk in &range_keys {
        let start_ikey = InternalKey::new(rk.start.clone(), rk.seqnum, rk.kind).encode();
        w.add(&start_ikey, &rk.value)?;
        if smallest.is_none()
            || cmp.compare(&rk.start, encoded_user_key(smallest.as_ref().unwrap()))
                == std::cmp::Ordering::Less
        {
            smallest = Some(start_ikey.clone());
        }
        if let Ok(end) = rk.end()
            && (largest.is_empty()
                || cmp.compare(&end, encoded_user_key(&largest)) == std::cmp::Ordering::Greater)
        {
            largest = InternalKey::new(end, rk.seqnum, rk.kind).encode();
        }
        smallest_seq = smallest_seq.min(rk.seqnum);
        largest_seq = largest_seq.max(rk.seqnum);
    }

    let mut file = w.finish()?;
    file.sync_all()?;

    Ok(FileMetadata {
        file_num,
        size: fs.size(&path)?,
        smallest: smallest.unwrap_or_default(),
        largest,
        smallest_seqnum: smallest_seq.min(largest_seq),
        largest_seqnum: largest_seq,
    })
}

/// Writes a new `manifest` marker pointing at `value`, with an `iter` one greater than
/// any existing marker, and removes superseded marker files.
fn update_marker(fs: &dyn Fs, dir: &Path, names: &[String], value: &str) -> Result<()> {
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
    fs.create(&dir.join(&new_name))?.sync_all()?;
    for n in old {
        let _ = fs.remove(&dir.join(n));
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
    fn range_deletions_in_memtable() {
        let dir = temp_dir();
        let db = Db::open(&dir, Options::default()).unwrap();
        for i in 0..20u32 {
            db.set(format!("k{i:02}").as_bytes(), format!("v{i}").as_bytes())
                .unwrap();
        }
        // Delete the range [k05, k15).
        db.delete_range(b"k05", b"k15").unwrap();
        for i in 0..20u32 {
            let key = format!("k{i:02}");
            let got = db.get(key.as_bytes()).unwrap();
            if (5..15).contains(&i) {
                assert_eq!(got, None, "{key} should be range-deleted");
            } else {
                assert_eq!(got, Some(format!("v{i}").into_bytes()), "{key}");
            }
        }
        // A point write after the range delete resurrects that key.
        db.set(b"k07", b"new7").unwrap();
        assert_eq!(db.get(b"k07").unwrap(), Some(b"new7".to_vec()));
        // Iteration also hides range-deleted keys.
        let live: Vec<_> = collect(&db).into_iter().map(|(k, _)| k).collect();
        assert!(!live.contains(&"k06".to_string()));
        assert!(live.contains(&"k07".to_string()));
        assert!(live.contains(&"k15".to_string()));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn range_deletions_survive_flush_and_compaction() {
        let dir = temp_dir();
        let opts = Options {
            mem_table_size: 16 * 1024,
            ..Default::default()
        };
        let db = Db::open(&dir, opts).unwrap();
        // Write keys, delete a wide range, then write enough to force flushes and a
        // compaction so the range tombstone must persist through both.
        for i in 0..400u32 {
            db.set(format!("key{i:05}").as_bytes(), b"v").unwrap();
        }
        db.delete_range(b"key00100", b"key00300").unwrap();
        for i in 400..2000u32 {
            db.set(format!("key{i:05}").as_bytes(), b"v").unwrap();
        }
        db.flush().unwrap();

        // Reopen to ensure the tombstone persisted to disk and is reapplied.
        drop(db);
        let db = Db::open(&dir, Options::default()).unwrap();
        assert_eq!(db.get(b"key00050").unwrap(), Some(b"v".to_vec()));
        assert_eq!(db.get(b"key00100").unwrap(), None);
        assert_eq!(db.get(b"key00200").unwrap(), None);
        assert_eq!(db.get(b"key00299").unwrap(), None);
        assert_eq!(db.get(b"key00300").unwrap(), Some(b"v".to_vec()));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn range_keys_set_and_query() {
        use crate::base::internal_key::InternalKeyKind;
        let dir = temp_dir();
        let db = Db::open(&dir, Options::default()).unwrap();
        db.range_key_set(b"b", b"f", b"@10", b"hello").unwrap();
        db.range_key_set(b"d", b"h", b"@20", b"world").unwrap();

        // "e" is covered by both spans [b,f) and [d,h).
        let rks = db.range_keys_covering(b"e").unwrap();
        assert_eq!(rks.len(), 2);
        assert!(rks.iter().all(|r| r.kind == InternalKeyKind::RangeKeySet));
        // "a" is covered by neither.
        assert!(db.range_keys_covering(b"a").unwrap().is_empty());
        // "c" only by [b,f).
        assert_eq!(db.range_keys_covering(b"c").unwrap().len(), 1);

        // Range keys survive a flush + reopen.
        db.flush().unwrap();
        drop(db);
        let db = Db::open(&dir, Options::default()).unwrap();
        assert_eq!(db.range_keys_covering(b"e").unwrap().len(), 2);
        let rk = &db.range_keys_covering(b"c").unwrap()[0];
        assert_eq!(rk.end().unwrap(), b"f");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn range_key_delete_clears() {
        let dir = temp_dir();
        let db = Db::open(&dir, Options::default()).unwrap();
        db.range_key_set(b"a", b"z", b"@1", b"v").unwrap();
        assert_eq!(db.range_keys_covering(b"m").unwrap().len(), 1);
        // A range-key delete is recorded as a covering entry (newest first).
        db.range_key_delete(b"a", b"z").unwrap();
        let rks = db.range_keys_covering(b"m").unwrap();
        assert_eq!(rks.len(), 2);
        assert_eq!(
            rks[0].kind,
            crate::base::internal_key::InternalKeyKind::RangeKeyDelete
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn open_snapshot_survives_compaction() {
        let dir = temp_dir();
        let opts = Options {
            mem_table_size: 16 * 1024,
            ..Default::default()
        };
        let db = Db::open(&dir, opts).unwrap();
        // Establish an initial value, then take a snapshot pinning it.
        db.set(b"pinned", b"v1").unwrap();
        let snap = db.snapshot();
        // Overwrite the key many times and churn enough to force flushes + compaction.
        db.set(b"pinned", b"v2").unwrap();
        for i in 0..3000u32 {
            db.set(format!("k{i:06}").as_bytes(), b"x").unwrap();
        }
        db.flush().unwrap();

        // The snapshot still observes the pinned version despite compaction.
        assert_eq!(snap.get(b"pinned").unwrap(), Some(b"v1".to_vec()));
        // The live database sees the latest.
        assert_eq!(db.get(b"pinned").unwrap(), Some(b"v2".to_vec()));

        // After the snapshot is dropped, the old version may be collapsed; the latest
        // remains correct regardless.
        drop(snap);
        db.set(b"trigger", b"z").unwrap();
        db.flush().unwrap();
        assert_eq!(db.get(b"pinned").unwrap(), Some(b"v2".to_vec()));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn metrics_and_event_listener() {
        use std::sync::atomic::{AtomicU64, Ordering};
        struct Counter {
            flushes: AtomicU64,
            compactions: AtomicU64,
        }
        impl EventListener for Counter {
            fn on_flush_end(&self, _file_num: u64, bytes: u64) {
                assert!(bytes > 0);
                self.flushes.fetch_add(1, Ordering::Relaxed);
            }
            fn on_compaction_end(&self, _lvl: usize, inputs: usize, _outputs: usize) {
                assert!(inputs > 0);
                self.compactions.fetch_add(1, Ordering::Relaxed);
            }
        }
        let counter = Arc::new(Counter {
            flushes: AtomicU64::new(0),
            compactions: AtomicU64::new(0),
        });
        let dir = temp_dir();
        let opts = Options {
            mem_table_size: 16 * 1024,
            event_listener: Some(counter.clone()),
            ..Default::default()
        };
        let db = Db::open(&dir, opts).unwrap();
        for i in 0..3000u32 {
            db.set(format!("k{i:06}").as_bytes(), b"v").unwrap();
        }
        db.flush().unwrap();

        let m = db.metrics();
        assert!(m.flush_count >= 1, "flushes: {}", m.flush_count);
        assert!(
            m.compaction_count >= 1,
            "compactions: {}",
            m.compaction_count
        );
        assert_eq!(m.flush_count, counter.flushes.load(Ordering::Relaxed));
        assert_eq!(
            m.compaction_count,
            counter.compactions.load(Ordering::Relaxed)
        );
        assert!(m.level_bytes.iter().sum::<u64>() > 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn merge_operator_resolves_across_levels() {
        use crate::base::merge::ConcatMerger;
        let dir = temp_dir();
        let opts = Options {
            mem_table_size: 16 * 1024,
            merger: Some(Arc::new(ConcatMerger)),
            ..Default::default()
        };
        let db = Db::open(&dir, opts.clone()).unwrap();

        // Base value then several merge operands.
        db.set(b"k", b"A").unwrap();
        db.merge(b"k", b"B").unwrap();
        db.merge(b"k", b"C").unwrap();
        assert_eq!(db.get(b"k").unwrap(), Some(b"ABC".to_vec()));

        // A key with only merges (no base).
        db.merge(b"only", b"x").unwrap();
        db.merge(b"only", b"y").unwrap();
        assert_eq!(db.get(b"only").unwrap(), Some(b"xy".to_vec()));

        // Iteration resolves merges too.
        let got = collect(&db);
        assert!(got.contains(&("k".to_string(), "ABC".to_string())));
        assert!(got.contains(&("only".to_string(), "xy".to_string())));

        // Force flushes + compaction with more operands, then reopen.
        for i in 0..3000u32 {
            db.set(format!("f{i:06}").as_bytes(), b"v").unwrap();
        }
        db.merge(b"k", b"D").unwrap();
        db.flush().unwrap();
        drop(db);
        let db = Db::open(&dir, opts).unwrap();
        assert_eq!(db.get(b"k").unwrap(), Some(b"ABCD".to_vec()));
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
