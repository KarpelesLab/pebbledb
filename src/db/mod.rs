// Copyright (c) 2012 The LevelDB-Go Authors. All rights reserved.
// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Ported from Pebble's pebble.go / open.go (read path), get_iter.go, and
// merging_iter.go.

//! The database read path: opening an existing store and reading from it.
//!
//! [`Db::open_read_only`] locates the current MANIFEST (via the atomic marker files Pebble
//! writes, falling back to the highest-numbered `MANIFEST-*`), replays it into a
//! [`VersionSet`], and serves reads from the resulting sstables. [`Db::get`] performs a
//! leveled point lookup (L0 newest-first, then L1..L6), and [`Db::iter`] returns a
//! snapshot-consistent forward iterator built from a merging iterator over all live
//! sstables.
//!
//! Scope: read-only open of an existing store; the write path (WAL, flush, compaction)
//! arrives in later phases. WAL replay is not performed, so any unflushed mutations in a
//! store's logs are not visible here yet.

mod filenames;
mod merging_iter;

pub use merging_iter::DbIterator;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::base::comparer::{Comparer, DefaultComparer};
use crate::base::internal_key::{InternalKeyKind, SeqNum};
use crate::manifest::{NUM_LEVELS, VersionSet};
use crate::sstable::Reader;
use crate::{Error, Result};

/// Options for opening a database.
#[derive(Clone)]
pub struct Options {
    /// The user-key comparer. Must match the one the store was created with (checked
    /// against the MANIFEST's recorded name).
    pub comparer: Arc<dyn Comparer>,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            comparer: Arc::new(DefaultComparer),
        }
    }
}

/// A read-only handle to an on-disk database.
pub struct Db {
    dir: PathBuf,
    cmp: Arc<dyn Comparer>,
    vs: VersionSet,
    /// Lazily-opened sstable readers, keyed by file number.
    cache: Mutex<HashMap<u64, Arc<Reader>>>,
}

impl Db {
    /// Opens the database in `dir` for reading.
    pub fn open_read_only(dir: impl AsRef<Path>, opts: Options) -> Result<Db> {
        let dir = dir.as_ref().to_path_buf();
        let names: Vec<String> = std::fs::read_dir(&dir)?
            .filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().into_string().ok())
            .collect();

        let manifest_name = filenames::current_manifest(&names)
            .ok_or_else(|| Error::corruption("db: no MANIFEST found"))?;
        let manifest_bytes = std::fs::read(dir.join(&manifest_name))?;
        let vs = VersionSet::load(&manifest_bytes, opts.comparer.clone())?;

        Ok(Db {
            dir,
            cmp: opts.comparer,
            vs,
            cache: Mutex::new(HashMap::new()),
        })
    }

    /// The largest sequence number recorded in the MANIFEST, used as the default read
    /// snapshot.
    pub fn last_sequence(&self) -> SeqNum {
        self.vs.last_sequence
    }

    /// Opens (or returns a cached) reader for the sstable with the given file number.
    fn open_reader(&self, file_num: u64) -> Result<Arc<Reader>> {
        if let Some(r) = self.cache.lock().unwrap().get(&file_num) {
            return Ok(Arc::clone(r));
        }
        let path = self.dir.join(filenames::table(file_num));
        let bytes = std::fs::read(path)?;
        let reader = Arc::new(Reader::open(bytes, self.cmp.clone())?);
        self.cache
            .lock()
            .unwrap()
            .insert(file_num, Arc::clone(&reader));
        Ok(reader)
    }

    /// Looks up `key`, returning its value or `None` if absent or deleted. Reads at the
    /// latest committed sequence number.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_at(key, self.vs.last_sequence)
    }

    /// Looks up `key` as visible at sequence number `snapshot`.
    pub fn get_at(&self, key: &[u8], snapshot: SeqNum) -> Result<Option<Vec<u8>>> {
        // Consult levels from newest data to oldest: L0 (already ordered newest-first),
        // then L1..L6. The first file that contains a visible entry for `key` wins.
        for level in 0..NUM_LEVELS {
            for f in self.vs.current.overlapping(self.cmp.as_ref(), level, key) {
                let reader = self.open_reader(f.file_num)?;
                if let Some((kind, value)) = reader.get(key, snapshot)? {
                    return Ok(match kind {
                        InternalKeyKind::Delete
                        | InternalKeyKind::SingleDelete
                        | InternalKeyKind::DeleteSized => None,
                        _ => Some(value),
                    });
                }
            }
        }
        Ok(None)
    }

    /// Returns a forward iterator over all keys, reading at the latest committed sequence
    /// number.
    pub fn iter(&self) -> Result<DbIterator> {
        self.iter_at(self.vs.last_sequence)
    }

    /// Returns a forward iterator over all keys as visible at `snapshot`.
    pub fn iter_at(&self, snapshot: SeqNum) -> Result<DbIterator> {
        let mut sources = Vec::new();
        for level in self.vs.current.levels.iter() {
            for f in level {
                let reader = self.open_reader(f.file_num)?;
                sources.push(reader.iter()?);
            }
        }
        DbIterator::new(sources, snapshot, self.cmp.clone())
    }

    /// The user-key comparer.
    pub fn comparer(&self) -> &Arc<dyn Comparer> {
        &self.cmp
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::internal_key::{InternalKey, make_trailer};
    use crate::manifest::{FileMetadata, NewFileEntry, VersionEdit};
    use crate::sstable::{Writer, WriterOptions};
    use std::sync::atomic::{AtomicU64, Ordering};

    static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_dir() -> PathBuf {
        let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir().join(format!("pebbledb-db-{}-{}", std::process::id(), n));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    /// Writes the given sorted (internal key, value) entries to `<dir>/<num>.sst` and
    /// returns its file metadata.
    fn write_table(dir: &Path, num: u64, entries: &[(Vec<u8>, Vec<u8>)]) -> FileMetadata {
        let cmp: Arc<dyn Comparer> = Arc::new(DefaultComparer);
        let file = std::fs::File::create(dir.join(filenames::table(num))).unwrap();
        let mut w = Writer::new(file, cmp, WriterOptions::default());
        let mut smallest_seq = u64::MAX;
        let mut largest_seq = 0u64;
        for (k, v) in entries {
            w.add(k, v).unwrap();
            let seq = crate::base::internal_key::encoded_trailer(k) >> 8;
            smallest_seq = smallest_seq.min(seq);
            largest_seq = largest_seq.max(seq);
        }
        w.finish().unwrap();
        FileMetadata {
            file_num: num,
            size: std::fs::metadata(dir.join(filenames::table(num)))
                .unwrap()
                .len(),
            smallest: entries.first().unwrap().0.clone(),
            largest: entries.last().unwrap().0.clone(),
            smallest_seqnum: smallest_seq,
            largest_seqnum: largest_seq,
        }
    }

    fn ik(user: &str, seq: u64, kind: InternalKeyKind) -> Vec<u8> {
        // make_trailer is exercised indirectly; keep the import meaningful.
        let _ = make_trailer(seq, kind);
        InternalKey::new(user.as_bytes().to_vec(), seq, kind).encode()
    }

    /// Builds a small two-level database on disk and returns its directory.
    fn build_db() -> PathBuf {
        let dir = temp_dir();
        // L6 (older): a=a6, b=b6, c=c6, d=d6.
        let l6 = write_table(
            &dir,
            1,
            &[
                (ik("a", 1, InternalKeyKind::Set), b"a6".to_vec()),
                (ik("b", 2, InternalKeyKind::Set), b"b6".to_vec()),
                (ik("c", 3, InternalKeyKind::Set), b"c6".to_vec()),
                (ik("d", 4, InternalKeyKind::Set), b"d6".to_vec()),
            ],
        );
        // L0 (newer): b deleted, c updated, e added.
        let l0 = write_table(
            &dir,
            2,
            &[
                (ik("b", 12, InternalKeyKind::Delete), b"".to_vec()),
                (ik("c", 13, InternalKeyKind::Set), b"c-new".to_vec()),
                (ik("e", 14, InternalKeyKind::Set), b"e0".to_vec()),
            ],
        );

        let snapshot = VersionEdit {
            comparer_name: Some("leveldb.BytewiseComparator".to_string()),
            log_number: Some(0),
            next_file_number: Some(3),
            last_sequence: Some(14),
            new_files: vec![
                NewFileEntry { level: 0, meta: l0 },
                NewFileEntry { level: 6, meta: l6 },
            ],
            ..Default::default()
        };
        let manifest = VersionSet::write_manifest(&[snapshot]).unwrap();
        std::fs::write(dir.join(filenames::manifest(3)), &manifest).unwrap();
        // Atomic marker pointing at the manifest.
        std::fs::write(
            dir.join(format!("marker.manifest.000001.{}", filenames::manifest(3))),
            b"",
        )
        .unwrap();
        dir
    }

    #[test]
    fn open_and_get_across_levels() {
        let dir = build_db();
        let db = Db::open_read_only(&dir, Options::default()).unwrap();
        assert_eq!(db.last_sequence(), 14);

        // a only in L6.
        assert_eq!(db.get(b"a").unwrap(), Some(b"a6".to_vec()));
        // b deleted in L0 -> not found.
        assert_eq!(db.get(b"b").unwrap(), None);
        // c updated in L0 -> newer value wins.
        assert_eq!(db.get(b"c").unwrap(), Some(b"c-new".to_vec()));
        // d only in L6.
        assert_eq!(db.get(b"d").unwrap(), Some(b"d6".to_vec()));
        // e only in L0.
        assert_eq!(db.get(b"e").unwrap(), Some(b"e0".to_vec()));
        // missing.
        assert_eq!(db.get(b"z").unwrap(), None);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn get_respects_snapshot() {
        let dir = build_db();
        let db = Db::open_read_only(&dir, Options::default()).unwrap();
        // At snapshot 5, the L0 mutations (seq 12-14) are invisible.
        assert_eq!(db.get_at(b"b", 5).unwrap(), Some(b"b6".to_vec()));
        assert_eq!(db.get_at(b"c", 5).unwrap(), Some(b"c6".to_vec()));
        assert_eq!(db.get_at(b"e", 5).unwrap(), None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn iterate_collapses_versions_and_tombstones() {
        let dir = build_db();
        let db = Db::open_read_only(&dir, Options::default()).unwrap();
        let mut it = db.iter().unwrap();
        let mut got = Vec::new();
        it.first().unwrap();
        while it.valid() {
            got.push((
                String::from_utf8(it.key().to_vec()).unwrap(),
                String::from_utf8(it.value().to_vec()).unwrap(),
            ));
            it.next().unwrap();
        }
        // b is deleted; c shows the newer value; order is by user key.
        assert_eq!(
            got,
            vec![
                ("a".to_string(), "a6".to_string()),
                ("c".to_string(), "c-new".to_string()),
                ("d".to_string(), "d6".to_string()),
                ("e".to_string(), "e0".to_string()),
            ]
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn iterate_at_old_snapshot() {
        let dir = build_db();
        let db = Db::open_read_only(&dir, Options::default()).unwrap();
        let mut it = db.iter_at(5).unwrap();
        let mut keys = Vec::new();
        it.first().unwrap();
        while it.valid() {
            keys.push(String::from_utf8(it.key().to_vec()).unwrap());
            it.next().unwrap();
        }
        // At snapshot 5 only the original L6 keys a,b,c,d are visible.
        assert_eq!(keys, vec!["a", "b", "c", "d"]);
        std::fs::remove_dir_all(&dir).ok();
    }
}
