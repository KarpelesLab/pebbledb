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
//! Scope: every `NewFile`/`NewFile2`/`NewFile3`/`NewFile4`/`NewFile5` file record is
//! decoded, including the full custom-tag set — creation time, virtual backing tables,
//! synthetic prefix/suffix, and blob references — so a MANIFEST written by upstream Pebble
//! parses without error (details this engine does not model are parsed for stream
//! alignment but discarded). Edits are written using `NewFile2`, which Pebble reads.
//! Excise records, standalone blob-file records, and column-family records remain
//! explicitly unsupported (they are rejected rather than mis-parsed).

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
const TAG_NEW_BLOB_FILE: u64 = 107;
const TAG_DELETED_BLOB_FILE: u64 = 108;

// Custom sub-tags within NewFile4/NewFile5.
const CUSTOM_TAG_TERMINATE: u64 = 1;
const CUSTOM_TAG_NEEDS_COMPACTION: u64 = 2;
const CUSTOM_TAG_CREATION_TIME: u64 = 6;
const CUSTOM_TAG_NO_RANGE_KEY_SETS: u64 = 7;
/// pebbledb-private hint: whether the file carries range tombstones / range keys (one payload
/// byte, 0 or 1). Chosen in the safe-to-ignore range (no [`CUSTOM_TAG_NON_SAFE_IGNORE_MASK`]
/// bit) and encoded length-prefixed, so a reader that does not recognize it — including
/// upstream Pebble — skips it via its default custom-tag handling. Not a Pebble tag.
const CUSTOM_TAG_SPAN_HINT: u64 = 8;
/// pebbledb-private hint: the blob-file numbers this sstable references, as concatenated
/// uvarints in one length-prefixed field. Lets blob-file GC learn an sstable's references from
/// the MANIFEST at open instead of re-reading the sstable's metaindex. Safe-to-ignore range,
/// length-prefixed, so upstream Pebble skips it (Pebble records its own richer blob references
/// under `CUSTOM_TAG_BLOB_REFERENCES`, from which we also recover the file numbers). Not a
/// Pebble tag.
const CUSTOM_TAG_BLOB_REFS: u64 = 9;
const CUSTOM_TAG_NON_SAFE_IGNORE_MASK: u64 = 1 << 6;
const CUSTOM_TAG_PATH_ID: u64 = 65;
const CUSTOM_TAG_VIRTUAL: u64 = 66;
const CUSTOM_TAG_SYNTHETIC_PREFIX: u64 = 67;
const CUSTOM_TAG_SYNTHETIC_SUFFIX: u64 = 68;
const CUSTOM_TAG_BLOB_REFERENCES: u64 = 69;
const CUSTOM_TAG_BLOB_REFERENCES2: u64 = 70;

// NewFile5 bounds-marker bit flags: whether the file has point keys, and whether the
// file's overall smallest / largest bound is taken from the point keys (vs the range keys).
const BOUNDS_MASK_CONTAINS_POINT_KEYS: u8 = 1 << 0;
const BOUNDS_MASK_SMALLEST_IS_POINT: u8 = 1 << 1;
const BOUNDS_MASK_LARGEST_IS_POINT: u8 = 1 << 2;

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
    /// Blob file numbers this sstable references (for blob-file GC). **In-memory only** — not
    /// serialized to the MANIFEST; repopulated at open by scanning the sstable's metaindex.
    /// Empty for tables that reference no blob files.
    pub blob_refs: Vec<u64>,
    /// Blob references for a value-separated table (`FormatValueSeparation`): the ordered
    /// `(blob_file_id, value_size)` pairs this sstable references, from the `customTagBlobReferences`
    /// custom tag on a `NewFile4`/`NewFile5` record. An inline blob handle's `reference_id` indexes
    /// this list; the ID maps to a physical blob file number via the version's blob-file registry,
    /// and `value_size` is the bytes of values referenced (used by Pebble's GC accounting). Empty
    /// for tables with no separated values.
    pub pebble_blob_refs: Vec<(u64, u64)>,
    /// For a **virtual sstable**, the file number of the physical backing sstable it is a
    /// bounded view of (`<backing>.sst` on disk); the view is restricted to `[smallest,
    /// largest]`. `None` for an ordinary physical table (which is its own backing). Persisted
    /// in the MANIFEST via the `CUSTOM_TAG_VIRTUAL` custom tag.
    pub backing: Option<u64>,
    /// Whether the file carries range tombstones and/or range keys: `Some(true)`/`Some(false)`
    /// when known, `None` when unknown (a file from upstream Pebble, an older pebbledb, or one
    /// whose hint was not recorded). A read iterator must collect every file's spans up front
    /// (they shadow keys in other levels), but a `Some(false)` file can be skipped without
    /// opening it. Persisted in the MANIFEST via the pebbledb-private, safe-to-ignore
    /// `CUSTOM_TAG_SPAN_HINT` custom tag (a reader that does not understand it — including
    /// Pebble — simply ignores it). Purely an optimization: an absent or wrong-toward-`true`
    /// value only costs an extra file open, never correctness.
    pub has_spans: Option<bool>,
}

impl FileMetadata {
    /// The physical sstable file number to read from: the backing file for a virtual table,
    /// otherwise the table's own number.
    pub fn physical_num(&self) -> u64 {
        self.backing.unwrap_or(self.file_num)
    }
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
    /// Native blob files added (Pebble `FormatValueSeparation`), as `(blob_file_id, file_num,
    /// size, value_size)` — the logical blob file id, the physical `<file_num>.blob`, the file's
    /// total size, and the total bytes of values it holds.
    pub new_blob_files: Vec<(u64, u64, u64, u64)>,
    /// Native blob files removed, as `(blob_file_id, file_num)` pairs.
    pub deleted_blob_files: Vec<(u64, u64)>,
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

    fn byte(&mut self) -> Result<u8> {
        let b = *self
            .b
            .get(self.pos)
            .ok_or_else(|| Error::corruption("version edit: truncated byte"))?;
        self.pos += 1;
        Ok(b)
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
                TAG_NEW_BLOB_FILE => {
                    // file_id, disk_file_num, size, value_size, creation_time.
                    let file_id = d.uvarint()?;
                    let file_num = d.uvarint()?;
                    let size = d.uvarint()?;
                    let value_size = d.uvarint()?;
                    let _creation_time = d.uvarint()?;
                    edit.new_blob_files
                        .push((file_id, file_num, size, value_size));
                }
                TAG_DELETED_BLOB_FILE => {
                    let file_id = d.uvarint()?;
                    let file_num = d.uvarint()?;
                    edit.deleted_blob_files.push((file_id, file_num));
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
            // A virtual table (backing tag), a span hint, or recorded blob references need the
            // custom-tag-capable NewFile4 layout; plain physical tables with none stay on
            // NewFile2.
            let needs_custom = nf.meta.backing.is_some()
                || nf.meta.has_spans.is_some()
                || !nf.meta.blob_refs.is_empty()
                || !nf.meta.pebble_blob_refs.is_empty();
            let tag = if needs_custom {
                TAG_NEW_FILE4
            } else {
                TAG_NEW_FILE2
            };
            put_uvarint(&mut buf, tag);
            put_uvarint(&mut buf, nf.level as u64);
            put_uvarint(&mut buf, nf.meta.file_num);
            put_uvarint(&mut buf, nf.meta.size);
            put_bytes(&mut buf, &nf.meta.smallest);
            put_bytes(&mut buf, &nf.meta.largest);
            put_uvarint(&mut buf, nf.meta.smallest_seqnum);
            put_uvarint(&mut buf, nf.meta.largest_seqnum);
            if needs_custom {
                if let Some(backing) = nf.meta.backing {
                    put_uvarint(&mut buf, CUSTOM_TAG_VIRTUAL);
                    put_uvarint(&mut buf, backing);
                }
                if let Some(has_spans) = nf.meta.has_spans {
                    // Length-prefixed single byte so unknown readers skip it cleanly.
                    put_uvarint(&mut buf, CUSTOM_TAG_SPAN_HINT);
                    put_bytes(&mut buf, &[u8::from(has_spans)]);
                }
                if !nf.meta.blob_refs.is_empty() {
                    // pebbledb-private: the referenced blob-file numbers as concatenated
                    // uvarints inside one length-prefixed field, so unknown readers skip it.
                    let mut payload = Vec::new();
                    for &num in &nf.meta.blob_refs {
                        put_uvarint(&mut payload, num);
                    }
                    put_uvarint(&mut buf, CUSTOM_TAG_BLOB_REFS);
                    put_bytes(&mut buf, &payload);
                }
                if !nf.meta.pebble_blob_refs.is_empty() {
                    // Pebble's standard blob-reference tag: depth, count, then (blob_file_id,
                    // value_size) per reference. Reference depth is encoded as 1.
                    put_uvarint(&mut buf, CUSTOM_TAG_BLOB_REFERENCES);
                    put_uvarint(&mut buf, 1); // blob reference depth
                    put_uvarint(&mut buf, nf.meta.pebble_blob_refs.len() as u64);
                    for &(file_id, value_size) in &nf.meta.pebble_blob_refs {
                        put_uvarint(&mut buf, file_id);
                        put_uvarint(&mut buf, value_size);
                    }
                }
                put_uvarint(&mut buf, CUSTOM_TAG_TERMINATE);
            }
        }
        for &(file_id, file_num, size, value_size) in &self.new_blob_files {
            // tagNewBlobFile: file_id, file_num, size, value_size, creation_time.
            put_uvarint(&mut buf, TAG_NEW_BLOB_FILE);
            put_uvarint(&mut buf, file_id);
            put_uvarint(&mut buf, file_num);
            put_uvarint(&mut buf, size);
            put_uvarint(&mut buf, value_size);
            put_uvarint(&mut buf, 0); // creation time
        }
        for (file_id, file_num) in &self.deleted_blob_files {
            put_uvarint(&mut buf, TAG_DELETED_BLOB_FILE);
            put_uvarint(&mut buf, *file_id);
            put_uvarint(&mut buf, *file_num);
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
///
/// Handles the LevelDB/RocksDB `NewFile`/`NewFile2`/`NewFile3`/`NewFile4` layouts and the
/// Pebble `NewFile5` (range-key bounds) layout, including the full custom-tag set
/// (creation time, virtual backing tables, synthetic prefix/suffix, and blob references).
/// Information this engine does not model (virtual/synthetic/blob details) is parsed for
/// stream-position correctness but discarded; the file's overall key bounds are kept.
fn decode_new_file(d: &mut Decoder<'_>, tag: u64) -> Result<NewFileEntry> {
    let level = d.level()?;
    let file_num = d.uvarint()?;
    if tag == TAG_NEW_FILE3 {
        let _path_id = d.uvarint()?;
    }
    let size = d.uvarint()?;

    // Key bounds. NewFile5 introduces a bounds marker selecting the file's overall
    // smallest/largest from its point-key and/or range-key bounds.
    let (smallest, largest) = if tag == TAG_NEW_FILE5 {
        let marker = d.byte()?;
        let (mut sp, mut lp) = (None, None);
        if marker & BOUNDS_MASK_CONTAINS_POINT_KEYS != 0 {
            sp = Some(d.bytes()?.to_vec());
            lp = Some(d.bytes()?.to_vec());
        }
        let sr = d.bytes()?.to_vec();
        let lr = d.bytes()?.to_vec();
        let smallest = if marker & BOUNDS_MASK_SMALLEST_IS_POINT != 0 {
            sp.ok_or_else(|| {
                Error::corruption("version edit: NewFile5 smallest is point but no point keys")
            })?
        } else {
            sr
        };
        let largest = if marker & BOUNDS_MASK_LARGEST_IS_POINT != 0 {
            lp.ok_or_else(|| {
                Error::corruption("version edit: NewFile5 largest is point but no point keys")
            })?
        } else {
            lr
        };
        (smallest, largest)
    } else {
        (d.bytes()?.to_vec(), d.bytes()?.to_vec())
    };

    let (smallest_seqnum, largest_seqnum) = if tag != TAG_NEW_FILE {
        (d.uvarint()?, d.uvarint()?)
    } else {
        (0, 0)
    };

    let (backing, has_spans, blob_refs, pebble_blob_refs) =
        if tag == TAG_NEW_FILE4 || tag == TAG_NEW_FILE5 {
            decode_custom_tags(d)?
        } else {
            (None, None, Vec::new(), Vec::new())
        };

    Ok(NewFileEntry {
        level,
        meta: FileMetadata {
            file_num,
            size,
            smallest,
            largest,
            smallest_seqnum,
            largest_seqnum,
            // From our private blob-refs tag if present; otherwise empty (the engine falls back
            // to scanning the sstable's metaindex at open, e.g. for upstream-Pebble records).
            blob_refs,
            // Upstream Pebble's blob references (ordered blob file IDs), from the standard
            // customTagBlobReferences tag; empty for pebbledb-written tables.
            pebble_blob_refs,
            backing,
            has_spans,
        },
    })
}

/// Consumes the custom-tag stream of a `NewFile4`/`NewFile5` record up to the terminator,
/// returning the backing-file number (virtual tables), the span hint, and the referenced
/// blob-file numbers (all pebbledb-private where applicable). Each tag's payload is parsed
/// exactly so the stream stays aligned; payloads for features this engine does not model are
/// discarded.
#[allow(clippy::type_complexity)]
fn decode_custom_tags(
    d: &mut Decoder<'_>,
) -> Result<(Option<u64>, Option<bool>, Vec<u64>, Vec<(u64, u64)>)> {
    let mut backing = None;
    let mut has_spans = None;
    let mut blob_refs = Vec::new();
    let mut pebble_blob_refs = Vec::new();
    loop {
        let custom = d.uvarint()?;
        match custom {
            CUSTOM_TAG_TERMINATE => break,
            CUSTOM_TAG_CREATION_TIME
            | CUSTOM_TAG_NO_RANGE_KEY_SETS
            | CUSTOM_TAG_NEEDS_COMPACTION => {
                let _field = d.bytes()?;
            }
            CUSTOM_TAG_SPAN_HINT => {
                // pebbledb-private: a single byte, 0 or 1.
                let field = d.bytes()?;
                has_spans = Some(field.first().copied().unwrap_or(0) != 0);
            }
            CUSTOM_TAG_BLOB_REFS => {
                // pebbledb-private: concatenated uvarint blob-file numbers in one field.
                let field = d.bytes()?;
                let mut off = 0;
                while off < field.len() {
                    let (num, used) = get_uvarint(&field[off..]).ok_or_else(|| {
                        Error::corruption("version edit: malformed blob-refs custom tag")
                    })?;
                    blob_refs.push(num);
                    off += used;
                }
            }
            CUSTOM_TAG_VIRTUAL => {
                // Virtual table: the backing file's disk number follows.
                backing = Some(d.uvarint()?);
            }
            CUSTOM_TAG_SYNTHETIC_PREFIX | CUSTOM_TAG_SYNTHETIC_SUFFIX => {
                let _field = d.bytes()?;
            }
            CUSTOM_TAG_BLOB_REFERENCES | CUSTOM_TAG_BLOB_REFERENCES2 => {
                // Pebble's standard blob-reference tag: depth, count, then per-reference
                // (blob_file_id, value_size) [+ backing_value_size for the v2 tag]. We keep the
                // ordered blob file IDs so an inline handle's reference_id can be resolved.
                let _depth = d.uvarint()?;
                let n = d.uvarint()?;
                for _ in 0..n {
                    let file_id = d.uvarint()?;
                    let value_size = d.uvarint()?;
                    if custom == CUSTOM_TAG_BLOB_REFERENCES2 {
                        let _backing_value_size = d.uvarint()?;
                    }
                    pebble_blob_refs.push((file_id, value_size));
                }
            }
            CUSTOM_TAG_PATH_ID => {
                return Err(Error::Unsupported(
                    "version edit: NewFile path-id field not supported",
                ));
            }
            c if c & CUSTOM_TAG_NON_SAFE_IGNORE_MASK == 0 => {
                // Unknown but safe to ignore: the format is always a single bytes field.
                let _field = d.bytes()?;
            }
            other => {
                return Err(Error::Corruption(format!(
                    "version edit: unsupported custom tag {other}"
                )));
            }
        }
    }
    Ok((backing, has_spans, blob_refs, pebble_blob_refs))
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
            blob_refs: Vec::new(),
            pebble_blob_refs: Vec::new(),
            backing: None,
            has_spans: None,
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
            new_blob_files: Vec::new(),
            deleted_blob_files: Vec::new(),
        };
        let bytes = edit.encode();
        let got = VersionEdit::decode(&bytes).unwrap();
        assert_eq!(got, edit);
    }

    #[test]
    fn blob_file_records_and_pebble_refs_roundtrip() {
        // A new file referencing Pebble blob files, plus NewBlobFile/DeletedBlobFile records,
        // round-trip through encode/decode.
        let mut m = meta(10, "a", "z", 100, 200);
        m.pebble_blob_refs = vec![(3, 111), (5, 222)];
        let edit = VersionEdit {
            new_files: vec![NewFileEntry { level: 1, meta: m }],
            new_blob_files: vec![(3, 6, 500, 111), (5, 7, 600, 222)],
            deleted_blob_files: vec![(2, 4)],
            ..Default::default()
        };
        let got = VersionEdit::decode(&edit.encode()).unwrap();
        assert_eq!(
            got.new_files[0].meta.pebble_blob_refs,
            vec![(3, 111), (5, 222)]
        );
        assert_eq!(got.new_blob_files, vec![(3, 6, 500, 111), (5, 7, 600, 222)]);
        assert_eq!(got.deleted_blob_files, vec![(2, 4)]);
    }

    #[test]
    fn decodes_native_blob_file_tags() {
        // A Pebble FormatValueSeparation MANIFEST records native blob files via tag 107
        // (file_id, file_num, size, value_size, creation_time) and removals via tag 108. Our
        // decoder must accept them so a format-24 MANIFEST can be replayed.
        let mut buf = Vec::new();
        put_uvarint(&mut buf, TAG_NEW_BLOB_FILE);
        put_uvarint(&mut buf, 3); // file_id
        put_uvarint(&mut buf, 6); // file_num
        put_uvarint(&mut buf, 4096); // size
        put_uvarint(&mut buf, 2048); // value_size
        put_uvarint(&mut buf, 0); // creation_time
        put_uvarint(&mut buf, TAG_DELETED_BLOB_FILE);
        put_uvarint(&mut buf, 1); // file_id
        put_uvarint(&mut buf, 2); // file_num
        let got = VersionEdit::decode(&buf).unwrap();
        assert_eq!(got.new_blob_files, vec![(3, 6, 4096, 2048)]);
        assert_eq!(got.deleted_blob_files, vec![(1, 2)]);
    }

    #[test]
    fn span_hint_roundtrips() {
        // A file with a recorded span hint encodes the pebbledb-private custom tag and decodes
        // back to the same value; absent hints stay absent (plain NewFile2).
        for hint in [Some(true), Some(false), None] {
            let mut m = meta(10, "a", "m", 100, 200);
            m.has_spans = hint;
            let edit = VersionEdit {
                new_files: vec![NewFileEntry { level: 2, meta: m }],
                ..Default::default()
            };
            let got = VersionEdit::decode(&edit.encode()).unwrap();
            assert_eq!(got.new_files[0].meta.has_spans, hint);
            assert_eq!(got, edit);
        }
    }

    #[test]
    fn blob_refs_roundtrip() {
        // Blob references encode the private custom tag and decode back to the same list;
        // an empty list stays empty (and keeps the file on NewFile2).
        let mut m = meta(10, "a", "m", 100, 200);
        m.blob_refs = vec![3, 7, 42];
        let edit = VersionEdit {
            new_files: vec![NewFileEntry { level: 1, meta: m }],
            ..Default::default()
        };
        let got = VersionEdit::decode(&edit.encode()).unwrap();
        assert_eq!(got.new_files[0].meta.blob_refs, vec![3, 7, 42]);
        assert_eq!(got, edit);

        // Empty refs: no tag, plain round-trip.
        let edit2 = VersionEdit {
            new_files: vec![NewFileEntry {
                level: 1,
                meta: meta(11, "n", "z", 5, 9),
            }],
            ..Default::default()
        };
        let got2 = VersionEdit::decode(&edit2.encode()).unwrap();
        assert!(got2.new_files[0].meta.blob_refs.is_empty());
        assert_eq!(got2, edit2);
    }

    #[test]
    fn unknown_safe_custom_tag_is_ignored() {
        // A NewFile4 record carrying an unknown safe-to-ignore custom tag (length-prefixed)
        // decodes without error, mirroring how upstream Pebble skips our span-hint tag.
        let mut buf = Vec::new();
        put_uvarint(&mut buf, TAG_NEW_FILE4);
        put_uvarint(&mut buf, 1); // level
        put_uvarint(&mut buf, 42); // file_num
        put_uvarint(&mut buf, 100); // size
        put_bytes(&mut buf, b"aaaaaaaa"); // smallest (encoded internal key, opaque here)
        put_bytes(&mut buf, b"zzzzzzzz"); // largest
        put_uvarint(&mut buf, 5); // smallest seqnum
        put_uvarint(&mut buf, 9); // largest seqnum
        put_uvarint(&mut buf, 9); // an unknown safe-to-ignore tag (no non-safe bit)
        put_bytes(&mut buf, b"whatever"); // its length-prefixed payload
        put_uvarint(&mut buf, CUSTOM_TAG_TERMINATE);
        let got = VersionEdit::decode(&buf).unwrap();
        assert_eq!(got.new_files.len(), 1);
        assert_eq!(got.new_files[0].meta.file_num, 42);
        assert_eq!(got.new_files[0].meta.has_spans, None);
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
    fn virtual_file_backing_round_trips_through_manifest() {
        // A virtual sstable (backing = Some) and a physical one in the same edit.
        let mut virt = meta(7, "b", "m", 4, 9);
        virt.backing = Some(3); // a bounded view over physical file 3
        let phys = meta(8, "p", "z", 2, 6);
        let edit = VersionEdit {
            new_files: vec![
                NewFileEntry {
                    level: 5,
                    meta: virt.clone(),
                },
                NewFileEntry {
                    level: 5,
                    meta: phys.clone(),
                },
            ],
            ..Default::default()
        };
        let decoded = VersionEdit::decode(&edit.encode()).unwrap();
        assert_eq!(decoded.new_files.len(), 2);
        let dv = &decoded.new_files[0].meta;
        assert_eq!(dv.file_num, 7);
        assert_eq!(
            dv.backing,
            Some(3),
            "virtual backing must survive the round trip"
        );
        assert_eq!(dv.physical_num(), 3);
        assert_eq!(dv.smallest, virt.smallest);
        assert_eq!(dv.largest, virt.largest);
        let dp = &decoded.new_files[1].meta;
        assert_eq!(dp.file_num, 8);
        assert_eq!(dp.backing, None, "physical file has no backing");
        assert_eq!(dp.physical_num(), 8);
    }

    #[test]
    fn decode_new_file5_with_point_and_range_bounds_and_custom_tags() {
        // Hand-encode a NewFile5 record (Pebble's range-key layout): bounds marker with
        // point keys present and the overall smallest/largest taken from the point keys,
        // followed by virtual and blob-reference custom tags this engine parses but does
        // not model.
        let mut buf = Vec::new();
        put_uvarint(&mut buf, TAG_NEW_FILE5);
        put_uvarint(&mut buf, 1); // level
        put_uvarint(&mut buf, 77); // file_num
        put_uvarint(&mut buf, 8192); // size

        let sp = InternalKey::new(b"aaa".to_vec(), 50, InternalKeyKind::Set).encode();
        let lp = InternalKey::new(b"mmm".to_vec(), 40, InternalKeyKind::Set).encode();
        let sr = InternalKey::new(b"bbb".to_vec(), 49, InternalKeyKind::RangeKeySet).encode();
        let lr = InternalKey::new(b"nnn".to_vec(), 41, InternalKeyKind::RangeKeySet).encode();
        let marker = BOUNDS_MASK_CONTAINS_POINT_KEYS
            | BOUNDS_MASK_SMALLEST_IS_POINT
            | BOUNDS_MASK_LARGEST_IS_POINT;
        buf.push(marker);
        put_bytes(&mut buf, &sp);
        put_bytes(&mut buf, &lp);
        put_bytes(&mut buf, &sr);
        put_bytes(&mut buf, &lr);
        put_uvarint(&mut buf, 10); // smallest seqnum
        put_uvarint(&mut buf, 50); // largest seqnum

        // Virtual backing table custom tag.
        put_uvarint(&mut buf, CUSTOM_TAG_VIRTUAL);
        put_uvarint(&mut buf, 12); // backing file num
        // Blob references (v2) custom tag: depth, count, then per-ref fields.
        put_uvarint(&mut buf, CUSTOM_TAG_BLOB_REFERENCES2);
        put_uvarint(&mut buf, 1); // depth
        put_uvarint(&mut buf, 2); // count
        for (fid, vsz, bsz) in [(1u64, 100u64, 110u64), (2, 200, 210)] {
            put_uvarint(&mut buf, fid);
            put_uvarint(&mut buf, vsz);
            put_uvarint(&mut buf, bsz);
        }
        put_uvarint(&mut buf, CUSTOM_TAG_TERMINATE);

        let edit = VersionEdit::decode(&buf).unwrap();
        assert_eq!(edit.new_files.len(), 1);
        let nf = &edit.new_files[0];
        assert_eq!(nf.level, 1);
        assert_eq!(nf.meta.file_num, 77);
        assert_eq!(nf.meta.size, 8192);
        // Overall bounds taken from the point keys per the marker.
        assert_eq!(nf.meta.smallest, sp);
        assert_eq!(nf.meta.largest, lp);
        assert_eq!((nf.meta.smallest_seqnum, nf.meta.largest_seqnum), (10, 50));
    }

    #[test]
    fn decode_new_file5_range_keys_only() {
        // A file with no point keys: the bounds come entirely from range keys.
        let mut buf = Vec::new();
        put_uvarint(&mut buf, TAG_NEW_FILE5);
        put_uvarint(&mut buf, 0);
        put_uvarint(&mut buf, 5);
        put_uvarint(&mut buf, 256);
        let sr = InternalKey::new(b"k1".to_vec(), 9, InternalKeyKind::RangeKeySet).encode();
        let lr = InternalKey::new(b"k9".to_vec(), 8, InternalKeyKind::RangeKeySet).encode();
        buf.push(0); // marker: no point keys, bounds from range keys
        put_bytes(&mut buf, &sr);
        put_bytes(&mut buf, &lr);
        put_uvarint(&mut buf, 8);
        put_uvarint(&mut buf, 9);
        put_uvarint(&mut buf, CUSTOM_TAG_TERMINATE);

        let edit = VersionEdit::decode(&buf).unwrap();
        let nf = &edit.new_files[0];
        assert_eq!(nf.meta.smallest, sr);
        assert_eq!(nf.meta.largest, lr);
    }

    #[test]
    fn excise_and_blob_file_records_are_rejected_not_misparsed() {
        // These tags are explicitly unsupported; decoding must error rather than silently
        // mis-parse the remainder of the edit.
        for tag in [
            10u64, /* excise */
            107,   /* new blob file */
            200,   /* column family */
        ] {
            let mut buf = Vec::new();
            put_uvarint(&mut buf, tag);
            assert!(
                VersionEdit::decode(&buf).is_err(),
                "tag {tag} should be rejected"
            );
        }
    }
}
