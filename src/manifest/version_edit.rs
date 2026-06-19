// Copyright (c) 2012 The LevelDB-Go Authors. All rights reserved.
// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Ported from Pebble's internal/manifest/version_edit.go.

//! Version edits: the tagged records that make up the MANIFEST.
//!
//! The MANIFEST is a [`crate::record`] log whose records are *version edits*. A
//! version edit is a sequence of `(tag, payload)` fields describing a delta to the LSM:
//! files added or removed at each level, and updates to the comparator name, log number,
//! next file number, and last sequence number. Replaying every edit in order
//! reconstructs the current set of live sstables.
//!
//! Scope: the common tags plus the `NewFile`/`NewFile2`/`NewFile3`/`NewFile4` (point-key)
//! file records are decoded; `NewFile5` (range-key bounds), blob files, virtual backing
//! tables beyond simple skipping, column families, and excise records are not yet
//! handled. Edits are written using `NewFile2`, which Pebble reads.

use crate::base::varint::{get_uvarint, put_uvarint};
use crate::{Error, Result};

/// The number of levels in the LSM tree (L0..L6).
pub const NUM_LEVELS: usize = 7;

// Version edit field tags (part of the on-disk format).
const TAG_COMPARATOR: u64 = 1;
const TAG_LOG_NUMBER: u64 = 2;
const TAG_NEXT_FILE_NUMBER: u64 = 3;
const TAG_LAST_SEQUENCE: u64 = 4;
const TAG_COMPACT_POINTER: u64 = 5;
const TAG_DELETED_FILE: u64 = 6;
const TAG_NEW_FILE: u64 = 7;
const TAG_PREV_LOG_NUMBER: u64 = 9;
const TAG_TABLE_MARKED_FOR_COMPACTION: u64 = 11;
const TAG_NEW_FILE2: u64 = 100;
const TAG_NEW_FILE3: u64 = 102;
const TAG_NEW_FILE4: u64 = 103;
const TAG_NEW_FILE5: u64 = 104;
const TAG_CREATED_BACKING_TABLE: u64 = 105;
const TAG_REMOVED_BACKING_TABLE: u64 = 106;

// Custom sub-tags within NewFile4/NewFile5.
const CUSTOM_TAG_TERMINATE: u64 = 1;
const CUSTOM_TAG_NEEDS_COMPACTION: u64 = 2;
const CUSTOM_TAG_CREATION_TIME: u64 = 6;
const CUSTOM_TAG_NO_RANGE_KEY_SETS: u64 = 7;
const CUSTOM_TAG_NON_SAFE_IGNORE_MASK: u64 = 1 << 6;

/// Metadata describing a single sstable in the LSM tree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileMetadata {
    /// The file's number (its name is `<file_num>.sst`).
    pub file_num: u64,
    /// The file's size in bytes.
    pub size: u64,
    /// The smallest internal key in the file (encoded).
    pub smallest: Vec<u8>,
    /// The largest internal key in the file (encoded).
    pub largest: Vec<u8>,
    /// The smallest sequence number among the file's keys.
    pub smallest_seqnum: u64,
    /// The largest sequence number among the file's keys.
    pub largest_seqnum: u64,
}

/// A newly added file together with the level it was added to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewFileEntry {
    /// The level the file lives at (0..[`NUM_LEVELS`]).
    pub level: usize,
    /// The file's metadata.
    pub meta: FileMetadata,
}

/// A delta to the LSM state, read from / written to the MANIFEST.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VersionEdit {
    /// The comparer name, set in the first edit of a MANIFEST.
    pub comparer_name: Option<String>,
    /// The WAL log number at or above which records are not yet flushed.
    pub log_number: Option<u64>,
    /// A previous log number (legacy; usually 0).
    pub prev_log_number: Option<u64>,
    /// The next file number to allocate.
    pub next_file_number: Option<u64>,
    /// The last sequence number assigned.
    pub last_sequence: Option<u64>,
    /// Files removed, as `(level, file_num)` pairs.
    pub deleted_files: Vec<(usize, u64)>,
    /// Files added.
    pub new_files: Vec<NewFileEntry>,
}

/// A cursor for decoding a version edit's field stream.
struct Decoder<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Decoder<'a> {
    fn new(b: &'a [u8]) -> Self {
        Decoder { b, pos: 0 }
    }

    fn remaining(&self) -> bool {
        self.pos < self.b.len()
    }

    fn uvarint(&mut self) -> Result<u64> {
        let (v, n) = get_uvarint(&self.b[self.pos..])
            .ok_or_else(|| Error::corruption("version edit: truncated uvarint"))?;
        self.pos += n;
        Ok(v)
    }

    fn bytes(&mut self) -> Result<&'a [u8]> {
        let len = self.uvarint()? as usize;
        let end = self
            .pos
            .checked_add(len)
            .filter(|&e| e <= self.b.len())
            .ok_or_else(|| Error::corruption("version edit: truncated bytes"))?;
        let out = &self.b[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    fn level(&mut self) -> Result<usize> {
        let l = self.uvarint()? as usize;
        if l >= NUM_LEVELS {
            return Err(Error::corruption("version edit: level out of range"));
        }
        Ok(l)
    }
}

impl VersionEdit {
    /// Decodes a version edit from its on-disk byte representation.
    pub fn decode(data: &[u8]) -> Result<VersionEdit> {
        let mut d = Decoder::new(data);
        let mut edit = VersionEdit::default();
        while d.remaining() {
            let tag = d.uvarint()?;
            match tag {
                TAG_COMPARATOR => {
                    let name = d.bytes()?;
                    edit.comparer_name = Some(
                        std::str::from_utf8(name)
                            .map_err(|_| Error::corruption("version edit: non-utf8 comparator"))?
                            .to_string(),
                    );
                }
                TAG_LOG_NUMBER => edit.log_number = Some(d.uvarint()?),
                TAG_PREV_LOG_NUMBER => edit.prev_log_number = Some(d.uvarint()?),
                TAG_NEXT_FILE_NUMBER => edit.next_file_number = Some(d.uvarint()?),
                TAG_LAST_SEQUENCE => edit.last_sequence = Some(d.uvarint()?),
                TAG_COMPACT_POINTER => {
                    let _level = d.level()?;
                    let _key = d.bytes()?;
                }
                TAG_DELETED_FILE => {
                    let level = d.level()?;
                    let file_num = d.uvarint()?;
                    edit.deleted_files.push((level, file_num));
                }
                TAG_NEW_FILE | TAG_NEW_FILE2 | TAG_NEW_FILE3 | TAG_NEW_FILE4 | TAG_NEW_FILE5 => {
                    let entry = decode_new_file(&mut d, tag)?;
                    edit.new_files.push(entry);
                }
                TAG_TABLE_MARKED_FOR_COMPACTION => {
                    let _level = d.level()?;
                    let _file_num = d.uvarint()?;
                }
                TAG_CREATED_BACKING_TABLE => {
                    // diskFileNum + size; skipped (virtual-table support not implemented).
                    let _disk_file_num = d.uvarint()?;
                    let _size = d.uvarint()?;
                }
                TAG_REMOVED_BACKING_TABLE => {
                    let _disk_file_num = d.uvarint()?;
                }
                other => {
                    return Err(Error::Corruption(format!(
                        "version edit: unsupported tag {other}"
                    )));
                }
            }
        }
        Ok(edit)
    }

    /// Encodes the version edit to bytes, using `NewFile2` for added files.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        if let Some(name) = &self.comparer_name {
            put_uvarint(&mut buf, TAG_COMPARATOR);
            put_bytes(&mut buf, name.as_bytes());
        }
        if let Some(n) = self.log_number {
            put_uvarint(&mut buf, TAG_LOG_NUMBER);
            put_uvarint(&mut buf, n);
        }
        if let Some(n) = self.prev_log_number {
            put_uvarint(&mut buf, TAG_PREV_LOG_NUMBER);
            put_uvarint(&mut buf, n);
        }
        if let Some(n) = self.next_file_number {
            put_uvarint(&mut buf, TAG_NEXT_FILE_NUMBER);
            put_uvarint(&mut buf, n);
        }
        if let Some(n) = self.last_sequence {
            put_uvarint(&mut buf, TAG_LAST_SEQUENCE);
            put_uvarint(&mut buf, n);
        }
        for (level, file_num) in &self.deleted_files {
            put_uvarint(&mut buf, TAG_DELETED_FILE);
            put_uvarint(&mut buf, *level as u64);
            put_uvarint(&mut buf, *file_num);
        }
        for nf in &self.new_files {
            put_uvarint(&mut buf, TAG_NEW_FILE2);
            put_uvarint(&mut buf, nf.level as u64);
            put_uvarint(&mut buf, nf.meta.file_num);
            put_uvarint(&mut buf, nf.meta.size);
            put_bytes(&mut buf, &nf.meta.smallest);
            put_bytes(&mut buf, &nf.meta.largest);
            put_uvarint(&mut buf, nf.meta.smallest_seqnum);
            put_uvarint(&mut buf, nf.meta.largest_seqnum);
        }
        buf
    }
}

/// Appends a length-prefixed byte string.
fn put_bytes(dst: &mut Vec<u8>, s: &[u8]) {
    put_uvarint(dst, s.len() as u64);
    dst.extend_from_slice(s);
}

/// Decodes a `NewFile*` record (the level and file metadata) for the given tag.
fn decode_new_file(d: &mut Decoder<'_>, tag: u64) -> Result<NewFileEntry> {
    let level = d.level()?;
    let file_num = d.uvarint()?;
    if tag == TAG_NEW_FILE3 {
        let _path_id = d.uvarint()?;
    }
    let size = d.uvarint()?;

    if tag == TAG_NEW_FILE5 {
        return Err(Error::Unsupported(
            "version edit: NewFile5 (range-key bounds) not yet supported",
        ));
    }

    let smallest = d.bytes()?.to_vec();
    let largest = d.bytes()?.to_vec();

    let (smallest_seqnum, largest_seqnum) = if tag != TAG_NEW_FILE {
        (d.uvarint()?, d.uvarint()?)
    } else {
        (0, 0)
    };

    if tag == TAG_NEW_FILE4 {
        // Custom tags terminated by CUSTOM_TAG_TERMINATE. Safe-to-ignore tags carry a
        // single bytes field; tags at/above the non-safe mask are not handled.
        loop {
            let custom = d.uvarint()?;
            if custom == CUSTOM_TAG_TERMINATE {
                break;
            }
            match custom {
                CUSTOM_TAG_NEEDS_COMPACTION
                | CUSTOM_TAG_CREATION_TIME
                | CUSTOM_TAG_NO_RANGE_KEY_SETS => {
                    let _field = d.bytes()?;
                }
                c if c & CUSTOM_TAG_NON_SAFE_IGNORE_MASK == 0 => {
                    // Unknown but safe to ignore: format is always a bytes field.
                    let _field = d.bytes()?;
                }
                other => {
                    return Err(Error::Corruption(format!(
                        "version edit: unsupported custom tag {other}"
                    )));
                }
            }
        }
    }

    Ok(NewFileEntry {
        level,
        meta: FileMetadata {
            file_num,
            size,
            smallest,
            largest,
            smallest_seqnum,
            largest_seqnum,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::internal_key::{InternalKey, InternalKeyKind};

    fn meta(num: u64, small: &str, large: &str, ss: u64, ls: u64) -> FileMetadata {
        FileMetadata {
            file_num: num,
            size: 1024 * num,
            smallest: InternalKey::new(small.as_bytes().to_vec(), ss, InternalKeyKind::Set)
                .encode(),
            largest: InternalKey::new(large.as_bytes().to_vec(), ls, InternalKeyKind::Set).encode(),
            smallest_seqnum: ss,
            largest_seqnum: ls,
        }
    }

    #[test]
    fn encode_decode_roundtrip() {
        let edit = VersionEdit {
            comparer_name: Some("leveldb.BytewiseComparator".to_string()),
            log_number: Some(7),
            prev_log_number: Some(0),
            next_file_number: Some(20),
            last_sequence: Some(1234),
            deleted_files: vec![(0, 5), (1, 6)],
            new_files: vec![
                NewFileEntry {
                    level: 0,
                    meta: meta(10, "a", "m", 100, 200),
                },
                NewFileEntry {
                    level: 1,
                    meta: meta(11, "n", "z", 50, 60),
                },
            ],
        };
        let bytes = edit.encode();
        let got = VersionEdit::decode(&bytes).unwrap();
        assert_eq!(got, edit);
    }

    #[test]
    fn empty_edit_roundtrips() {
        let edit = VersionEdit::default();
        assert_eq!(VersionEdit::decode(&edit.encode()).unwrap(), edit);
    }

    #[test]
    fn decode_new_file4_with_creation_time_custom_tag() {
        // Hand-encode a NewFile4 record with a creation-time custom tag, as Pebble emits.
        let mut buf = Vec::new();
        put_uvarint(&mut buf, TAG_NEW_FILE4);
        put_uvarint(&mut buf, 2); // level
        put_uvarint(&mut buf, 42); // file_num
        put_uvarint(&mut buf, 4096); // size
        let small = InternalKey::new(b"aaa".to_vec(), 9, InternalKeyKind::Set).encode();
        let large = InternalKey::new(b"zzz".to_vec(), 3, InternalKeyKind::Set).encode();
        put_bytes(&mut buf, &small);
        put_bytes(&mut buf, &large);
        put_uvarint(&mut buf, 3); // smallest seqnum
        put_uvarint(&mut buf, 9); // largest seqnum
        // creation-time custom tag: a bytes field holding a uvarint timestamp.
        put_uvarint(&mut buf, CUSTOM_TAG_CREATION_TIME);
        let mut ct = Vec::new();
        put_uvarint(&mut ct, 1_700_000_000);
        put_bytes(&mut buf, &ct);
        put_uvarint(&mut buf, CUSTOM_TAG_TERMINATE);

        let edit = VersionEdit::decode(&buf).unwrap();
        assert_eq!(edit.new_files.len(), 1);
        let nf = &edit.new_files[0];
        assert_eq!(nf.level, 2);
        assert_eq!(nf.meta.file_num, 42);
        assert_eq!(nf.meta.size, 4096);
        assert_eq!(nf.meta.smallest, small);
        assert_eq!(nf.meta.largest, large);
        assert_eq!((nf.meta.smallest_seqnum, nf.meta.largest_seqnum), (3, 9));
    }

    #[test]
    fn new_file5_is_unsupported() {
        let mut buf = Vec::new();
        put_uvarint(&mut buf, TAG_NEW_FILE5);
        put_uvarint(&mut buf, 0);
        put_uvarint(&mut buf, 1);
        put_uvarint(&mut buf, 100);
        assert!(VersionEdit::decode(&buf).is_err());
    }
}
