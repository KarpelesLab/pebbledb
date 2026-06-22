// Copyright (c) 2012 The LevelDB-Go Authors. All rights reserved.
// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Ported from Pebble's internal/manifest (version.go, version_set.go).

//! The MANIFEST: the durable record of which sstables make up the LSM tree.
//!
//! A [`Version`] is the set of live sstables at each level. A [`VersionSet`] tracks the
//! current version plus the engine's bookkeeping counters (next file number, last
//! sequence number, WAL log number). The on-disk MANIFEST is a [`crate::record`]
//! log of [`VersionEdit`]s; replaying them with [`VersionSet::load`] reconstructs the
//! current version, and new edits are appended with [`VersionEdit::encode`].

mod version_edit;

pub use version_edit::{FileMetadata, NUM_LEVELS, NewFileEntry, VersionEdit};

use std::io::Cursor;
use std::sync::Arc;

use crate::base::comparer::Comparer;
use crate::base::internal_key::{compare_encoded, encoded_user_key};
use crate::record;
use crate::{Error, Result};

/// The set of live sstables at each level of the LSM tree.
///
/// Level 0 may contain overlapping files and is kept ordered newest-first (by largest
/// sequence number). Levels 1 and above are non-overlapping and kept ordered by smallest
/// key.
#[derive(Clone, Default)]
pub struct Version {
    /// Files at each level, `levels[0]` being L0.
    pub levels: [Vec<Arc<FileMetadata>>; NUM_LEVELS],
}

impl Version {
    /// The total number of files across all levels.
    pub fn num_files(&self) -> usize {
        self.levels.iter().map(|l| l.len()).sum()
    }

    /// Returns the files at `level` that may contain `user_key`, in the order they should
    /// be consulted (newest first). For L0 every overlapping file is returned; for L1+ at
    /// most one file can overlap.
    pub fn overlapping(
        &self,
        cmp: &dyn Comparer,
        level: usize,
        user_key: &[u8],
    ) -> Vec<Arc<FileMetadata>> {
        self.levels[level]
            .iter()
            .filter(|f| {
                cmp.compare(encoded_user_key(&f.smallest), user_key) != std::cmp::Ordering::Greater
                    && cmp.compare(encoded_user_key(&f.largest), user_key)
                        != std::cmp::Ordering::Less
            })
            .cloned()
            .collect()
    }
}

/// Tracks the current [`Version`] and the engine's persistent counters, applying
/// [`VersionEdit`]s as the LSM evolves.
pub struct VersionSet {
    cmp: Arc<dyn Comparer>,
    /// The comparer name recorded in the MANIFEST.
    pub comparer_name: String,
    /// The minimum WAL log number whose records may still be unflushed.
    pub log_number: u64,
    /// The next file number to allocate.
    pub next_file_number: u64,
    /// The last sequence number assigned.
    pub last_sequence: u64,
    /// The current version.
    pub current: Version,
    /// Native blob files (Pebble `FormatValueSeparation`): blob file ID → physical `<num>.blob`
    /// file number. Populated from `NewBlobFile` / `DeletedBlobFile` MANIFEST records; used to
    /// resolve an sstable's blob references (an inline handle's `reference_id` → blob file ID →
    /// this map → file number). Empty for databases that do not separate values into blob files.
    pub blob_files: std::collections::HashMap<u64, u64>,
}

impl VersionSet {
    /// Creates an empty version set ordered by `cmp`.
    pub fn new(cmp: Arc<dyn Comparer>) -> VersionSet {
        let comparer_name = cmp.name().to_string();
        VersionSet {
            cmp,
            comparer_name,
            log_number: 0,
            next_file_number: 1,
            last_sequence: 0,
            current: Version::default(),
            blob_files: std::collections::HashMap::new(),
        }
    }

    /// Applies a single edit to the current version and counters.
    pub fn apply(&mut self, edit: &VersionEdit) -> Result<()> {
        if let Some(name) = &edit.comparer_name {
            self.comparer_name = name.clone();
        }
        if let Some(n) = edit.log_number {
            self.log_number = n;
        }
        if let Some(n) = edit.next_file_number {
            self.next_file_number = n;
        }
        if let Some(n) = edit.last_sequence {
            self.last_sequence = n;
        }
        for (level, file_num) in &edit.deleted_files {
            self.current.levels[*level].retain(|f| f.file_num != *file_num);
        }
        for nf in &edit.new_files {
            self.current.levels[nf.level].push(Arc::new(nf.meta.clone()));
        }
        for &(file_id, file_num, _size, _value_size) in &edit.new_blob_files {
            self.blob_files.insert(file_id, file_num);
        }
        for (file_id, _file_num) in &edit.deleted_blob_files {
            self.blob_files.remove(file_id);
        }
        // Re-sort affected levels.
        let touched: std::collections::BTreeSet<usize> = edit
            .new_files
            .iter()
            .map(|f| f.level)
            .chain(edit.deleted_files.iter().map(|(l, _)| *l))
            .collect();
        for level in touched {
            self.sort_level(level);
        }
        Ok(())
    }

    fn sort_level(&mut self, level: usize) {
        let cmp = self.cmp.clone();
        if level == 0 {
            // Newest first: by largest sequence number, then file number, descending.
            self.current.levels[0].sort_by(|a, b| {
                b.largest_seqnum
                    .cmp(&a.largest_seqnum)
                    .then(b.file_num.cmp(&a.file_num))
            });
        } else {
            // Non-overlapping, ordered by smallest key.
            self.current.levels[level]
                .sort_by(|a, b| compare_encoded(cmp.as_ref(), &a.smallest, &b.smallest));
        }
    }

    /// Reconstructs a version set by replaying every edit in a MANIFEST file's bytes.
    pub fn load(manifest: &[u8], cmp: Arc<dyn Comparer>) -> Result<VersionSet> {
        let mut vs = VersionSet::new(cmp);
        let mut reader = record::Reader::new(Cursor::new(manifest), 0);
        while let Some(rec) = reader.read_record()? {
            let edit = VersionEdit::decode(&rec)?;
            vs.apply(&edit)?;
        }
        if vs.comparer_name != vs.cmp.name() {
            return Err(Error::Corruption(format!(
                "manifest: comparer mismatch: file has {:?}, opened with {:?}",
                vs.comparer_name,
                vs.cmp.name()
            )));
        }
        Ok(vs)
    }

    /// Encodes the current state as a single "snapshot" version edit suitable for the
    /// first record of a freshly written MANIFEST: the comparer name, counters, and every
    /// live file as a new-file record.
    pub fn snapshot_edit(&self) -> VersionEdit {
        let mut edit = VersionEdit {
            comparer_name: Some(self.comparer_name.clone()),
            log_number: Some(self.log_number),
            next_file_number: Some(self.next_file_number),
            last_sequence: Some(self.last_sequence),
            ..Default::default()
        };
        for (level, files) in self.current.levels.iter().enumerate() {
            for f in files {
                edit.new_files.push(NewFileEntry {
                    level,
                    meta: (**f).clone(),
                });
            }
        }
        edit
    }

    /// Writes a complete MANIFEST (a record log) containing `edits` in order, returning
    /// the encoded bytes. Typically the first edit is a [`VersionSet::snapshot_edit`].
    pub fn write_manifest(edits: &[VersionEdit]) -> Result<Vec<u8>> {
        let mut w = record::Writer::new(Vec::new());
        for edit in edits {
            w.write_record(&edit.encode())?;
        }
        w.finish()
    }

    /// Allocates and returns the next file number.
    pub fn allocate_file_number(&mut self) -> u64 {
        let n = self.next_file_number;
        self.next_file_number += 1;
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::comparer::DefaultComparer;
    use crate::base::internal_key::{InternalKey, InternalKeyKind};

    fn meta(num: u64, small: &str, large: &str, ss: u64, ls: u64) -> FileMetadata {
        FileMetadata {
            file_num: num,
            size: 1000,
            smallest: InternalKey::new(small.as_bytes().to_vec(), ss, InternalKeyKind::Set)
                .encode(),
            largest: InternalKey::new(large.as_bytes().to_vec(), ls, InternalKeyKind::Set).encode(),
            smallest_seqnum: ss,
            largest_seqnum: ls,
            blob_refs: Vec::new(),
            pebble_blob_refs: Vec::new(),
            backing: None,
            has_spans: None,
        }
    }

    #[test]
    fn load_populates_blob_file_registry() {
        // Replaying a MANIFEST whose edit records native blob files and a file's blob references
        // must populate both the blob-file registry and the file's pebble_blob_refs.
        let cmp: Arc<dyn Comparer> = Arc::new(DefaultComparer);
        let mut m = meta(5, "a", "z", 1, 2);
        m.pebble_blob_refs = vec![(6, 100)];
        let edit = VersionEdit {
            comparer_name: Some(cmp.name().to_string()),
            next_file_number: Some(10),
            new_files: vec![NewFileEntry { level: 0, meta: m }],
            new_blob_files: vec![(6, 6, 500, 100)],
            ..Default::default()
        };
        let mut manifest = Vec::new();
        let mut w = crate::record::Writer::new(&mut manifest);
        w.write_record(&edit.encode()).unwrap();
        w.finish().unwrap();

        let vs = VersionSet::load(&manifest, cmp).unwrap();
        assert_eq!(
            vs.blob_files.get(&6),
            Some(&6),
            "blob-file registry populated"
        );
        assert_eq!(vs.current.levels[0][0].pebble_blob_refs, vec![(6, 100)]);
    }

    #[test]
    fn apply_adds_and_removes_files() {
        let cmp: Arc<dyn Comparer> = Arc::new(DefaultComparer);
        let mut vs = VersionSet::new(cmp);
        let e1 = VersionEdit {
            next_file_number: Some(12),
            last_sequence: Some(100),
            log_number: Some(3),
            new_files: vec![
                NewFileEntry {
                    level: 1,
                    meta: meta(10, "m", "z", 1, 2),
                },
                NewFileEntry {
                    level: 1,
                    meta: meta(11, "a", "f", 3, 4),
                },
            ],
            ..Default::default()
        };
        vs.apply(&e1).unwrap();
        assert_eq!(vs.next_file_number, 12);
        assert_eq!(vs.last_sequence, 100);
        assert_eq!(vs.log_number, 3);
        // L1 is sorted by smallest key: file 11 ("a") before file 10 ("m").
        assert_eq!(
            vs.current.levels[1]
                .iter()
                .map(|f| f.file_num)
                .collect::<Vec<_>>(),
            vec![11, 10]
        );

        let e2 = VersionEdit {
            deleted_files: vec![(1, 11)],
            new_files: vec![NewFileEntry {
                level: 1,
                meta: meta(13, "g", "l", 5, 6),
            }],
            ..Default::default()
        };
        vs.apply(&e2).unwrap();
        assert_eq!(
            vs.current.levels[1]
                .iter()
                .map(|f| f.file_num)
                .collect::<Vec<_>>(),
            vec![13, 10]
        );
    }

    #[test]
    fn l0_sorted_newest_first() {
        let cmp: Arc<dyn Comparer> = Arc::new(DefaultComparer);
        let mut vs = VersionSet::new(cmp);
        vs.apply(&VersionEdit {
            new_files: vec![
                NewFileEntry {
                    level: 0,
                    meta: meta(1, "a", "z", 10, 20),
                },
                NewFileEntry {
                    level: 0,
                    meta: meta(2, "a", "z", 30, 40),
                },
                NewFileEntry {
                    level: 0,
                    meta: meta(3, "a", "z", 21, 25),
                },
            ],
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            vs.current.levels[0]
                .iter()
                .map(|f| f.file_num)
                .collect::<Vec<_>>(),
            vec![2, 3, 1] // by largest_seqnum desc: 40, 25, 20
        );
    }

    #[test]
    fn manifest_write_then_load() {
        let cmp: Arc<dyn Comparer> = Arc::new(DefaultComparer);
        let snapshot = VersionEdit {
            comparer_name: Some("leveldb.BytewiseComparator".to_string()),
            log_number: Some(5),
            next_file_number: Some(30),
            last_sequence: Some(999),
            new_files: vec![
                NewFileEntry {
                    level: 0,
                    meta: meta(20, "a", "c", 100, 110),
                },
                NewFileEntry {
                    level: 6,
                    meta: meta(21, "d", "z", 1, 50),
                },
            ],
            ..Default::default()
        };
        let edit2 = VersionEdit {
            deleted_files: vec![(0, 20)],
            new_files: vec![NewFileEntry {
                level: 1,
                meta: meta(22, "a", "c", 100, 110),
            }],
            next_file_number: Some(31),
            ..Default::default()
        };
        let manifest = VersionSet::write_manifest(&[snapshot, edit2]).unwrap();

        let vs = VersionSet::load(&manifest, cmp).unwrap();
        assert_eq!(vs.log_number, 5);
        assert_eq!(vs.next_file_number, 31);
        assert_eq!(vs.last_sequence, 999);
        assert_eq!(vs.current.levels[0].len(), 0); // file 20 deleted
        assert_eq!(vs.current.levels[1].len(), 1);
        assert_eq!(vs.current.levels[6].len(), 1);
        assert_eq!(vs.current.num_files(), 2);
    }

    #[test]
    fn load_rejects_comparer_mismatch() {
        // A custom comparer with a different name.
        struct Other;
        impl Comparer for Other {
            fn name(&self) -> &str {
                "custom.Comparer"
            }
            fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
                a.cmp(b)
            }
            fn abbreviated_key(&self, _k: &[u8]) -> u64 {
                0
            }
            fn separator(&self, dst: &mut Vec<u8>, a: &[u8], _b: &[u8]) {
                dst.extend_from_slice(a);
            }
            fn successor(&self, dst: &mut Vec<u8>, a: &[u8]) {
                dst.extend_from_slice(a);
            }
        }
        let manifest = VersionSet::write_manifest(&[VersionEdit {
            comparer_name: Some("leveldb.BytewiseComparator".to_string()),
            ..Default::default()
        }])
        .unwrap();
        assert!(VersionSet::load(&manifest, Arc::new(Other)).is_err());
    }

    #[test]
    fn overlapping_files_for_key() {
        let cmp: Arc<dyn Comparer> = Arc::new(DefaultComparer);
        let mut vs = VersionSet::new(cmp.clone());
        vs.apply(&VersionEdit {
            new_files: vec![
                NewFileEntry {
                    level: 6,
                    meta: meta(1, "a", "f", 1, 2),
                },
                NewFileEntry {
                    level: 6,
                    meta: meta(2, "g", "m", 1, 2),
                },
                NewFileEntry {
                    level: 6,
                    meta: meta(3, "n", "z", 1, 2),
                },
            ],
            ..Default::default()
        })
        .unwrap();
        let hits = vs.current.overlapping(cmp.as_ref(), 6, b"h");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].file_num, 2);
        assert!(vs.current.overlapping(cmp.as_ref(), 6, b"zz").is_empty());
    }
}
