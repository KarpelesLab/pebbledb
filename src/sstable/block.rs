// Copyright (c) 2011 The LevelDB-Go Authors. All rights reserved.
// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Ported from Pebble's sstable/block package (block.go, compression.go) and
// sstable/rowblk (rowblk_iter.go).

//! sstable blocks: the shared on-disk container for data, index, and metaindex blocks.
//!
//! A block is a sequence of prefix-compressed key/value entries followed by a restart
//! array, then a 5-byte trailer:
//!
//! ```text
//! entry*:        shared:uvarint | unshared:uvarint | value_len:uvarint | key_suffix | value
//! restarts:      u32-le * num_restarts
//! num_restarts:  u32-le
//! trailer:       compression_type:u8 | checksum:u32-le
//! ```
//!
//! `shared` is the number of leading bytes the entry's key shares with the previous
//! entry's key; at a *restart point* `shared` is 0 (the full key is stored). The trailer
//! lives outside the block proper: its checksum covers the block bytes plus the
//! compression-type byte, and equals [`crate::crc::masked_crc32c`] for CRC32C.

use std::sync::Arc;

use crate::base::comparer::Comparer;
use crate::base::internal_key::compare_encoded;
use crate::base::varint::{get_u32_le, get_uvarint, put_uvarint};
use crate::crc::masked_crc32c;
use crate::{Error, Result};

/// The length of a block's trailer: a 1-byte compression type plus a 4-byte checksum.
pub const TRAILER_LEN: usize = 5;

/// A pointer to a block within an sstable file: a byte offset and the block's length
/// (excluding the trailer).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BlockHandle {
    /// Byte offset of the block within the file.
    pub offset: u64,
    /// Length of the block in bytes, not counting the [`TRAILER_LEN`] trailer.
    pub length: u64,
}

impl BlockHandle {
    /// Decodes a handle (`offset:uvarint`, `length:uvarint`) from the front of `src`,
    /// returning the handle and the number of bytes consumed, or `None` if malformed.
    pub fn decode(src: &[u8]) -> Option<(BlockHandle, usize)> {
        let (offset, n) = get_uvarint(src)?;
        let (length, m) = get_uvarint(&src[n..])?;
        Some((BlockHandle { offset, length }, n + m))
    }

    /// Appends the encoded handle to `dst`.
    pub fn encode_to(&self, dst: &mut Vec<u8>) {
        put_uvarint(dst, self.offset);
        put_uvarint(dst, self.length);
    }
}

/// The compression applied to a block, identified by the trailer's first byte. Matches
/// RocksDB's `CompressionType` values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompressionType {
    /// No compression (indicator byte 0).
    None,
    /// Snappy block compression (indicator byte 1).
    Snappy,
    /// Zstandard, with a varint decompressed-length prefix (indicator byte 7).
    Zstd,
}

impl CompressionType {
    /// The on-disk indicator byte.
    pub fn as_u8(self) -> u8 {
        match self {
            CompressionType::None => 0,
            CompressionType::Snappy => 1,
            CompressionType::Zstd => 7,
        }
    }

    /// Decodes the compression indicator byte, erroring on indicators this port does not
    /// implement (zlib, bzip2, lz4, …).
    pub fn from_u8(b: u8) -> Result<CompressionType> {
        match b {
            0 => Ok(CompressionType::None),
            1 => Ok(CompressionType::Snappy),
            7 => Ok(CompressionType::Zstd),
            other => Err(Error::Corruption(format!(
                "sstable: unsupported compression indicator {other}"
            ))),
        }
    }
}

/// The checksum algorithm protecting each block, identified in the footer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChecksumType {
    /// No checksum (0).
    None,
    /// CRC32C with the RocksDB mask (1).
    Crc32c,
    /// 32-bit xxHash (2). Not yet implemented.
    XxHash,
    /// Low 32 bits of xxHash64 (3). Not yet implemented.
    XxHash64,
}

impl ChecksumType {
    /// Decodes the checksum-type byte.
    pub fn from_u8(b: u8) -> Result<ChecksumType> {
        match b {
            0 => Ok(ChecksumType::None),
            1 => Ok(ChecksumType::Crc32c),
            2 => Ok(ChecksumType::XxHash),
            3 => Ok(ChecksumType::XxHash64),
            other => Err(Error::Corruption(format!(
                "sstable: unknown checksum type {other}"
            ))),
        }
    }
}

/// Reads, verifies, and decompresses the block referenced by `handle` from the in-memory
/// sstable `file`, returning the decompressed block bytes (without the trailer).
pub fn read_block(file: &[u8], handle: BlockHandle, checksum: ChecksumType) -> Result<Arc<[u8]>> {
    let start = handle.offset as usize;
    let len = handle.length as usize;
    let trailer_start = start
        .checked_add(len)
        .ok_or_else(|| Error::corruption("sstable: block handle overflow"))?;
    let end = trailer_start + TRAILER_LEN;
    if end > file.len() {
        return Err(Error::corruption("sstable: block handle out of bounds"));
    }
    let raw = &file[start..trailer_start];
    let type_byte = file[trailer_start];
    let stored = get_u32_le(&file[trailer_start + 1..end]).unwrap();

    // The checksum covers the block data plus the compression-type byte.
    match checksum {
        ChecksumType::None => {}
        ChecksumType::Crc32c => {
            let computed = masked_crc32c(&file[start..trailer_start + 1]);
            if computed != stored {
                return Err(Error::corruption("sstable: block checksum mismatch"));
            }
        }
        ChecksumType::XxHash | ChecksumType::XxHash64 => {
            return Err(Error::Unsupported("sstable: xxhash block checksum"));
        }
    }

    decompress(CompressionType::from_u8(type_byte)?, raw)
}

fn decompress(kind: CompressionType, raw: &[u8]) -> Result<Arc<[u8]>> {
    match kind {
        CompressionType::None => Ok(Arc::from(raw)),
        CompressionType::Snappy => {
            let out = compcol::vec::decompress_to_vec::<compcol::snappy::Snappy>(raw)
                .map_err(|e| Error::Corruption(format!("sstable: snappy decode: {e:?}")))?;
            Ok(Arc::from(out))
        }
        CompressionType::Zstd => {
            // Pebble prefixes the zstd frame with a varint of the decompressed length.
            let (_decoded_len, n) = get_uvarint(raw)
                .ok_or_else(|| Error::corruption("sstable: bad zstd length prefix"))?;
            let out = compcol::vec::decompress_to_vec::<compcol::zstd::Zstd>(&raw[n..])
                .map_err(|e| Error::Corruption(format!("sstable: zstd decode: {e:?}")))?;
            Ok(Arc::from(out))
        }
    }
}

/// An iterator over the key/value entries of a single decoded block.
///
/// Keys are returned as raw byte slices. For data and index blocks these are encoded
/// internal keys; for the metaindex they are block names. Seeking uses internal-key
/// order via [`compare_encoded`].
pub struct BlockIter {
    block: Arc<[u8]>,
    /// Offset where the restart array begins.
    restarts: usize,
    num_restarts: usize,
    /// Start offset of the current entry; `restarts` once exhausted.
    offset: usize,
    /// Start offset of the entry following the current one.
    next_offset: usize,
    /// The current entry's full (reconstructed) key.
    key: Vec<u8>,
    /// The current entry's value as a `(start, end)` range into `block`.
    value: (usize, usize),
    valid: bool,
}

impl BlockIter {
    /// Wraps a decoded block, reading its restart array.
    pub fn new(block: Arc<[u8]>) -> Result<BlockIter> {
        if block.len() < 4 {
            return Err(Error::corruption("sstable: block too small"));
        }
        let num_restarts = get_u32_le(&block[block.len() - 4..]).unwrap() as usize;
        let restarts = block
            .len()
            .checked_sub(4 * (1 + num_restarts))
            .ok_or_else(|| Error::corruption("sstable: bad restart count"))?;
        Ok(BlockIter {
            block,
            restarts,
            num_restarts,
            offset: 0,
            next_offset: 0,
            key: Vec::new(),
            value: (0, 0),
            valid: false,
        })
    }

    /// Whether the iterator is positioned at a valid entry.
    pub fn valid(&self) -> bool {
        self.valid
    }

    /// The current entry's key (an encoded internal key for data/index blocks).
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// The current entry's value.
    pub fn value(&self) -> &[u8] {
        &self.block[self.value.0..self.value.1]
    }

    fn restart_offset(&self, i: usize) -> usize {
        get_u32_le(&self.block[self.restarts + i * 4..]).unwrap() as usize
    }

    /// Decodes the entry starting at `p`, using the current `key` as the prefix source,
    /// and positions the iterator on it.
    fn decode_at(&mut self, p: usize) {
        if p >= self.restarts {
            self.valid = false;
            self.offset = self.restarts;
            return;
        }
        let mut q = p;
        let (shared, n) = get_uvarint(&self.block[q..]).expect("entry shared");
        q += n;
        let (unshared, n) = get_uvarint(&self.block[q..]).expect("entry unshared");
        q += n;
        let (value_len, n) = get_uvarint(&self.block[q..]).expect("entry value_len");
        q += n;
        let (shared, unshared, value_len) =
            (shared as usize, unshared as usize, value_len as usize);

        self.key.truncate(shared);
        self.key.extend_from_slice(&self.block[q..q + unshared]);
        q += unshared;
        self.value = (q, q + value_len);
        self.offset = p;
        self.next_offset = q + value_len;
        self.valid = true;
    }

    /// Positions at the first entry.
    pub fn first(&mut self) {
        self.key.clear();
        self.decode_at(0);
    }

    /// Advances to the next entry, becoming invalid past the end.
    pub fn next(&mut self) {
        if !self.valid {
            return;
        }
        self.decode_at(self.next_offset);
    }

    /// Reads only the (full) key at restart point `i`, whose entry has `shared == 0`.
    fn key_at_restart(&self, i: usize) -> &[u8] {
        let mut q = self.restart_offset(i);
        let (_shared, n) = get_uvarint(&self.block[q..]).expect("restart shared");
        q += n;
        let (unshared, n) = get_uvarint(&self.block[q..]).expect("restart unshared");
        q += n;
        let (_value_len, n) = get_uvarint(&self.block[q..]).expect("restart value_len");
        q += n;
        &self.block[q..q + unshared as usize]
    }

    /// Positions at the first entry whose key is `>= target` (an encoded internal key),
    /// using `cmp` for the user-key portion. Becomes invalid if no such entry exists.
    pub fn seek_ge(&mut self, target: &[u8], cmp: &dyn Comparer) {
        // Binary search the restart points for the first whose key is >= target.
        let mut lo = 0usize;
        let mut hi = self.num_restarts;
        while lo < hi {
            let mid = (lo + hi) / 2;
            let key = self.key_at_restart(mid);
            if compare_encoded(cmp, key, target) == std::cmp::Ordering::Less {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        // Start the linear scan from the restart just before `lo` (the last whose key is
        // < target), or the first restart.
        let start = if lo > 0 { lo - 1 } else { 0 };
        self.key.clear();
        if self.num_restarts == 0 {
            self.valid = false;
            return;
        }
        self.decode_at(self.restart_offset(start));
        while self.valid {
            if compare_encoded(cmp, &self.key, target) != std::cmp::Ordering::Less {
                return;
            }
            self.next();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::comparer::DefaultComparer;
    use crate::base::internal_key::{InternalKey, InternalKeyKind, make_trailer};
    use crate::base::varint::put_uvarint;

    /// Builds a block from already-sorted (key, value) pairs with the given restart
    /// interval, returning the raw block (entries + restart array + count), no trailer.
    fn build_block(entries: &[(&[u8], &[u8])], restart_interval: usize) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut restarts = Vec::new();
        let mut prev: &[u8] = &[];
        for (i, (key, value)) in entries.iter().enumerate() {
            let shared = if i % restart_interval == 0 {
                restarts.push(buf.len() as u32);
                0
            } else {
                let max = prev.len().min(key.len());
                let mut s = 0;
                while s < max && prev[s] == key[s] {
                    s += 1;
                }
                s
            };
            put_uvarint(&mut buf, shared as u64);
            put_uvarint(&mut buf, (key.len() - shared) as u64);
            put_uvarint(&mut buf, value.len() as u64);
            buf.extend_from_slice(&key[shared..]);
            buf.extend_from_slice(value);
            prev = key;
        }
        if restarts.is_empty() {
            restarts.push(0);
        }
        for r in &restarts {
            buf.extend_from_slice(&r.to_le_bytes());
        }
        buf.extend_from_slice(&(restarts.len() as u32).to_le_bytes());
        buf
    }

    fn ikey(user: &[u8], seq: u64, kind: InternalKeyKind) -> Vec<u8> {
        InternalKey::new(user.to_vec(), seq, kind).encode()
    }

    #[test]
    fn block_handle_roundtrip() {
        let h = BlockHandle {
            offset: 123456,
            length: 789,
        };
        let mut buf = Vec::new();
        h.encode_to(&mut buf);
        let (got, n) = BlockHandle::decode(&buf).unwrap();
        assert_eq!(got, h);
        assert_eq!(n, buf.len());
    }

    #[test]
    fn iterate_block_forward() {
        let keys: Vec<Vec<u8>> = ["a", "ab", "abc", "b", "bcd"]
            .iter()
            .map(|k| ikey(k.as_bytes(), 1, InternalKeyKind::Set))
            .collect();
        let entries: Vec<(&[u8], &[u8])> = keys
            .iter()
            .map(|k| (k.as_slice(), b"v".as_slice()))
            .collect();
        let block = build_block(&entries, 2);
        let mut it = BlockIter::new(Arc::from(block)).unwrap();
        it.first();
        let mut got = Vec::new();
        while it.valid() {
            got.push(it.key().to_vec());
            assert_eq!(it.value(), b"v");
            it.next();
        }
        assert_eq!(got, keys);
    }

    #[test]
    fn seek_ge_finds_entries() {
        let cmp = DefaultComparer;
        let users = ["a", "c", "e", "g", "i"];
        let keys: Vec<Vec<u8>> = users
            .iter()
            .map(|k| ikey(k.as_bytes(), 5, InternalKeyKind::Set))
            .collect();
        let entries: Vec<(&[u8], &[u8])> = keys
            .iter()
            .map(|k| (k.as_slice(), b"x".as_slice()))
            .collect();
        let block = build_block(&entries, 2);
        let mut it = BlockIter::new(Arc::from(block)).unwrap();

        // Seek to exactly "e".
        it.seek_ge(&ikey(b"e", 5, InternalKeyKind::Set), &cmp);
        assert!(it.valid());
        assert_eq!(it.key(), ikey(b"e", 5, InternalKeyKind::Set).as_slice());

        // Seek to "d" lands on "e".
        it.seek_ge(&ikey(b"d", 100, InternalKeyKind::Set), &cmp);
        assert!(it.valid());
        assert_eq!(&it.key()[..1], b"e");

        // Seek to "a" lands on first entry.
        it.seek_ge(&ikey(b"a", 5, InternalKeyKind::Set), &cmp);
        assert_eq!(&it.key()[..1], b"a");

        // Seek past the end -> invalid.
        it.seek_ge(&ikey(b"z", 5, InternalKeyKind::Set), &cmp);
        assert!(!it.valid());
    }

    #[test]
    fn read_block_roundtrip_uncompressed() {
        // Build a block, append an uncompressed trailer, and read it back.
        let keys: Vec<Vec<u8>> = ["k1", "k2"]
            .iter()
            .map(|k| ikey(k.as_bytes(), 1, InternalKeyKind::Set))
            .collect();
        let entries: Vec<(&[u8], &[u8])> = keys
            .iter()
            .map(|k| (k.as_slice(), b"val".as_slice()))
            .collect();
        let block = build_block(&entries, 1);

        let mut file = vec![0u8; 7]; // some leading bytes so offset != 0
        let offset = file.len();
        file.extend_from_slice(&block);
        file.push(CompressionType::None.as_u8());
        let checksum = masked_crc32c(&file[offset..offset + block.len() + 1]);
        file.extend_from_slice(&checksum.to_le_bytes());

        let handle = BlockHandle {
            offset: offset as u64,
            length: block.len() as u64,
        };
        let decoded = read_block(&file, handle, ChecksumType::Crc32c).unwrap();
        let mut it = BlockIter::new(decoded).unwrap();
        it.first();
        assert_eq!(it.key(), keys[0].as_slice());
        assert_eq!(it.value(), b"val");

        // Corrupting a byte is detected.
        let mut bad = file.clone();
        bad[offset] ^= 0xff;
        assert!(read_block(&bad, handle, ChecksumType::Crc32c).is_err());
    }

    #[test]
    fn trailer_marks_unused_for_now() {
        // make_trailer is exercised indirectly above; this keeps the import meaningful.
        let _ = make_trailer(1, InternalKeyKind::Set);
    }
}
