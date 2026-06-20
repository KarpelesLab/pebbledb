// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Models Pebble's separate blob files (sstable/blob).

//! Blob files: cross-table out-of-line value storage.
//!
//! A blob file stores large point-key values in their own file, separate from the sstables
//! that reference them — Pebble's *blob files*, distinct from the in-table [value
//! blocks](super::valblk) that live inside a single sstable. Separating values across files
//! lets a compaction rewrite the key/handle structure of an sstable without rewriting the
//! (often much larger) values, and lets several sstable generations share one copy of a
//! value.
//!
//! A referencing sstable stores, in place of the value, a value-prefix byte of kind
//! [`KIND_BLOB`] followed by a [`BlobHandle`] `(value_len, block_num, offset_in_block)`. The
//! blob *file* itself is identified by the sstable's blob-file reference (carried in the
//! MANIFEST), so the handle locates the value *within* that file.
//!
//! File layout — value blocks, then an index block, then a fixed footer:
//!
//! ```text
//! value_block*                          (each: body | compression:u8 | checksum:u32)
//! index_block                           rows of (offset:uvarint, length:uvarint), block i = row i
//! footer (25 bytes): index_offset:u64le | index_length:u64le | checksum:u8 | magic[8]
//! ```
//!
//! The block framing (compression + checksum trailer) is shared with the sstable block
//! writer, so blob blocks decompress and verify through the same [`read_block`] path. The
//! structure mirrors upstream Pebble; exact cross-implementation byte-parity (the footer
//! magic in particular) is validated by the Go interop CI rather than in-crate.

use crate::base::varint::{get_uvarint, put_uvarint};
use crate::sstable::block::{BlockHandle, ChecksumType, CompressionType, read_block};
use crate::sstable::writer::write_block;
use crate::{Error, Result};

/// The value-prefix kind marking a blob reference (top two bits of the prefix byte = `2`).
pub const KIND_BLOB: u8 = 2;

/// 8-byte magic ending every blob file. (pebbledb blob format; exact byte-parity with
/// upstream Pebble's blob-file magic is a Go-interop-CI concern.)
const BLOB_MAGIC: &[u8; 8] = b"PDBLOB01";
/// Fixed footer: `index_offset:u64 | index_length:u64 | checksum:u8 | magic[8]`.
const FOOTER_LEN: usize = 8 + 8 + 1 + 8;
/// Target uncompressed size of a value block before a new one is started.
const TARGET_BLOB_BLOCK: usize = 1 << 16;

/// A reference to a value stored in a blob file: which value block, the byte offset within
/// the (decompressed) block, and the value's length. The blob *file* is identified
/// separately by the referencing sstable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlobHandle {
    /// Length of the value in bytes.
    pub value_len: u32,
    /// The 0-indexed value block number within the blob file.
    pub block_num: u32,
    /// Byte offset of the value within the (decompressed) value block.
    pub offset_in_block: u32,
}

/// Encodes a blob handle (`value_len`, `block_num`, `offset_in_block` as uvarints).
pub fn encode_handle(h: BlobHandle) -> Vec<u8> {
    let mut dst = Vec::new();
    put_uvarint(&mut dst, u64::from(h.value_len));
    put_uvarint(&mut dst, u64::from(h.block_num));
    put_uvarint(&mut dst, u64::from(h.offset_in_block));
    dst
}

/// Encodes a blob *reference* as stored in an sstable data block (after the `KIND_BLOB`
/// value-prefix byte): a `file_index` into the table's blob-reference list, then the handle.
pub fn encode_ref(file_index: u32, h: BlobHandle) -> Vec<u8> {
    let mut dst = Vec::new();
    put_uvarint(&mut dst, u64::from(file_index));
    put_uvarint(&mut dst, u64::from(h.value_len));
    put_uvarint(&mut dst, u64::from(h.block_num));
    put_uvarint(&mut dst, u64::from(h.offset_in_block));
    dst
}

/// Decodes a blob reference (see [`encode_ref`]): `(file_index, handle)`.
pub fn decode_ref(src: &[u8]) -> Result<(u32, BlobHandle)> {
    let (file_index, n0) =
        get_uvarint(src).ok_or_else(|| Error::corruption("blob: bad ref file_index"))?;
    let h = decode_handle(&src[n0..])?;
    Ok((file_index as u32, h))
}

/// Decodes a blob handle (`value_len`, `block_num`, `offset_in_block` as uvarints).
pub fn decode_handle(src: &[u8]) -> Result<BlobHandle> {
    let (value_len, n1) =
        get_uvarint(src).ok_or_else(|| Error::corruption("blob: bad handle value_len"))?;
    let (block_num, n2) =
        get_uvarint(&src[n1..]).ok_or_else(|| Error::corruption("blob: bad handle block_num"))?;
    let (offset, _) =
        get_uvarint(&src[n1 + n2..]).ok_or_else(|| Error::corruption("blob: bad handle offset"))?;
    Ok(BlobHandle {
        value_len: value_len as u32,
        block_num: block_num as u32,
        offset_in_block: offset as u32,
    })
}

fn checksum_byte(c: ChecksumType) -> u8 {
    match c {
        ChecksumType::None => 0,
        ChecksumType::Crc32c => 1,
        ChecksumType::XxHash => 2,
        ChecksumType::XxHash64 => 3,
    }
}

/// Writes a blob file: values are appended with [`add`](BlobFileWriter::add), each returning
/// the [`BlobHandle`] the sstable should store, and [`finish`](BlobFileWriter::finish)
/// produces the complete file bytes.
pub struct BlobFileWriter {
    out: Vec<u8>,
    offset: u64,
    block_buf: Vec<u8>,
    block_num: u32,
    blocks: Vec<BlockHandle>,
    compression: CompressionType,
    checksum: ChecksumType,
}

impl BlobFileWriter {
    /// Creates a blob-file writer with the given block compression and checksum.
    pub fn new(compression: CompressionType, checksum: ChecksumType) -> BlobFileWriter {
        BlobFileWriter {
            out: Vec::new(),
            offset: 0,
            block_buf: Vec::new(),
            block_num: 0,
            blocks: Vec::new(),
            compression,
            checksum,
        }
    }

    /// Appends `value`, returning the handle that locates it. Values accumulate into a value
    /// block until it reaches the target size, then a new block is started.
    pub fn add(&mut self, value: &[u8]) -> Result<BlobHandle> {
        if !self.block_buf.is_empty() && self.block_buf.len() + value.len() > TARGET_BLOB_BLOCK {
            self.flush_block()?;
        }
        let offset_in_block = self.block_buf.len() as u32;
        self.block_buf.extend_from_slice(value);
        Ok(BlobHandle {
            value_len: value.len() as u32,
            block_num: self.block_num,
            offset_in_block,
        })
    }

    fn flush_block(&mut self) -> Result<()> {
        if self.block_buf.is_empty() {
            return Ok(());
        }
        let body = std::mem::take(&mut self.block_buf);
        let handle = write_block(
            &mut self.out,
            &mut self.offset,
            &body,
            self.compression,
            self.checksum,
        )?;
        self.blocks.push(handle);
        self.block_num += 1;
        Ok(())
    }

    /// Whether any value has been added.
    pub fn is_empty(&self) -> bool {
        self.block_buf.is_empty() && self.blocks.is_empty()
    }

    /// Finishes the file: flushes the open value block, writes the index block, and appends
    /// the footer. Returns the complete blob-file bytes.
    pub fn finish(mut self) -> Result<Vec<u8>> {
        self.flush_block()?;
        // Index block: one (offset, length) row per value block, in block-number order.
        let mut idx = Vec::new();
        for h in &self.blocks {
            put_uvarint(&mut idx, h.offset);
            put_uvarint(&mut idx, h.length);
        }
        let index_handle = write_block(
            &mut self.out,
            &mut self.offset,
            &idx,
            CompressionType::None,
            self.checksum,
        )?;
        self.out
            .extend_from_slice(&index_handle.offset.to_le_bytes());
        self.out
            .extend_from_slice(&index_handle.length.to_le_bytes());
        self.out.push(checksum_byte(self.checksum));
        self.out.extend_from_slice(BLOB_MAGIC);
        Ok(self.out)
    }
}

/// Reads a blob file produced by [`BlobFileWriter`], resolving [`BlobHandle`]s to value bytes.
pub struct BlobFileReader {
    data: Vec<u8>,
    checksum: ChecksumType,
    /// On-disk handle of each value block, indexed by block number.
    blocks: Vec<BlockHandle>,
}

impl BlobFileReader {
    /// Parses the footer and index of a blob file held wholly in memory.
    pub fn open(data: Vec<u8>) -> Result<BlobFileReader> {
        if data.len() < FOOTER_LEN {
            return Err(Error::corruption("blob: file shorter than footer"));
        }
        let f = &data[data.len() - FOOTER_LEN..];
        if &f[17..25] != BLOB_MAGIC {
            return Err(Error::corruption("blob: bad footer magic"));
        }
        let index_offset = u64::from_le_bytes(f[0..8].try_into().unwrap());
        let index_length = u64::from_le_bytes(f[8..16].try_into().unwrap());
        let checksum = ChecksumType::from_u8(f[16])?;
        let index = read_block(
            &data,
            BlockHandle {
                offset: index_offset,
                length: index_length,
            },
            checksum,
        )?;
        let mut blocks = Vec::new();
        let mut i = 0;
        while i < index.len() {
            let (offset, n1) = get_uvarint(&index[i..])
                .ok_or_else(|| Error::corruption("blob: bad index offset"))?;
            let (length, n2) = get_uvarint(&index[i + n1..])
                .ok_or_else(|| Error::corruption("blob: bad index length"))?;
            blocks.push(BlockHandle { offset, length });
            i += n1 + n2;
        }
        Ok(BlobFileReader {
            data,
            checksum,
            blocks,
        })
    }

    /// Resolves `handle` to the stored value bytes.
    pub fn get(&self, handle: BlobHandle) -> Result<Vec<u8>> {
        let bh = self
            .blocks
            .get(handle.block_num as usize)
            .ok_or_else(|| Error::corruption("blob: block number out of range"))?;
        let block = read_block(&self.data, *bh, self.checksum)?;
        let start = handle.offset_in_block as usize;
        let end = start
            .checked_add(handle.value_len as usize)
            .ok_or_else(|| Error::corruption("blob: handle length overflow"))?;
        if end > block.len() {
            return Err(Error::corruption("blob: handle out of range"));
        }
        Ok(block[start..end].to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handle_roundtrip() {
        let h = BlobHandle {
            value_len: 4321,
            block_num: 7,
            offset_in_block: 9999,
        };
        assert_eq!(decode_handle(&encode_handle(h)).unwrap(), h);
    }

    fn roundtrip_with(compression: CompressionType, n: usize, vlen: usize) {
        let mut w = BlobFileWriter::new(compression, ChecksumType::Crc32c);
        let mut handles = Vec::new();
        let values: Vec<Vec<u8>> = (0..n)
            .map(|i| {
                let mut v = vec![(i % 251) as u8; vlen];
                v[0] = i as u8;
                v
            })
            .collect();
        for v in &values {
            handles.push(w.add(v).unwrap());
        }
        let bytes = w.finish().unwrap();
        let r = BlobFileReader::open(bytes).unwrap();
        for (h, v) in handles.iter().zip(&values) {
            assert_eq!(&r.get(*h).unwrap(), v);
        }
    }

    #[test]
    fn roundtrip_single_block_uncompressed() {
        roundtrip_with(CompressionType::None, 10, 100);
    }

    #[test]
    fn roundtrip_many_blocks_spill() {
        // Values large enough that several value blocks are produced.
        roundtrip_with(CompressionType::None, 40, 5000);
    }

    #[test]
    fn roundtrip_snappy_and_zstd() {
        roundtrip_with(CompressionType::Snappy, 30, 4000);
        roundtrip_with(CompressionType::Zstd, 30, 4000);
    }

    #[test]
    fn empty_file_has_no_blocks() {
        let w = BlobFileWriter::new(CompressionType::None, ChecksumType::Crc32c);
        assert!(w.is_empty());
        let bytes = w.finish().unwrap();
        let r = BlobFileReader::open(bytes).unwrap();
        assert!(r.blocks.is_empty());
    }

    #[test]
    fn bad_magic_is_rejected() {
        let mut bytes = BlobFileWriter::new(CompressionType::None, ChecksumType::Crc32c)
            .finish()
            .unwrap();
        let n = bytes.len();
        bytes[n - 1] ^= 0xff; // corrupt the magic
        assert!(BlobFileReader::open(bytes).is_err());
    }

    #[test]
    fn out_of_range_handle_errors() {
        let mut w = BlobFileWriter::new(CompressionType::None, ChecksumType::Crc32c);
        let h = w.add(b"hello").unwrap();
        let r = BlobFileReader::open(w.finish().unwrap()).unwrap();
        // A length past the block end is rejected rather than panicking.
        let bad = BlobHandle {
            value_len: h.value_len + 1000,
            ..h
        };
        assert!(r.get(bad).is_err());
    }
}
