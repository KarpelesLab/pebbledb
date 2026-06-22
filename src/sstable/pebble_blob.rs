// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

//! Reader for Pebble's native blob file format (`FormatValueSeparation`, table format v6+).
//!
//! This is distinct from [`super::blob`], pebbledb's own sibling-file value-separation scheme.
//! Pebble's native blob file separates large values into a standalone `.blob` file whose layout
//! is built on the same [`super::colblk`] columnar blocks and [`super::block`] trailer framing:
//!
//! ```text
//! value block 0    (colblk: 1 RawBytes column of values)  | trailer
//! value block 1    ...                                    | trailer
//! ...
//! index block      (4-byte countVirtualBlocks header + colblk: virtualBlocks uint, offsets uint)
//! footer (38 bytes)
//! ```
//!
//! The index block's `offsets` column has `countBlocks + 1` entries: `offsets[i]..offsets[i+1]`
//! is value block `i`'s byte range (data + trailer) in the file. A [`Handle`] addresses a value
//! by `(block_id, value_id)`; for a blob file that has not been rewritten (`countVirtualBlocks
//! == 0`) the block ID is the physical block index, and the value ID indexes the values within
//! that block. Footer (little-endian): `crc(4) | index_offset(8) | index_length(8) |
//! checksum_type(1) | format(1) | original_file_num(8) | magic(8)`.

use std::sync::Arc;

use crate::{Error, Result};

use super::block::{BlockHandle, ChecksumType, TRAILER_LEN, read_block};
use super::colblk;

/// Pebble's blob file magic (`🪳🦀`), distinct from the sstable magic.
const BLOB_FILE_MAGIC: &[u8; 8] = b"\xf0\x9f\xaa\xb3\xf0\x9f\xa6\x80";
/// The fixed blob-file footer length.
const FOOTER_LEN: usize = 38;
/// The only blob file format version pebbledb reads (Pebble's `FileFormatV1`).
const BLOB_FORMAT_V1: u8 = 1;
/// The blob index block prefixes its columnar header with a 4-byte `countVirtualBlocks` field.
const INDEX_CUSTOM_HEADER_LEN: usize = 4;
/// Column indices within the blob index block.
const INDEX_COL_OFFSETS: usize = 1;
/// The single column of a blob value block (raw value bytes).
const VALUE_COL: usize = 0;

/// Identifies a value within a Pebble blob file: the physical block and the value within it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Handle {
    /// The block within the blob file (a physical index when the file is not rewritten).
    pub block_id: u32,
    /// The value within the block.
    pub value_id: u32,
}

/// Reads values from a Pebble native blob file held in memory.
pub struct PebbleBlobReader {
    file: Arc<[u8]>,
    checksum: ChecksumType,
    /// `offsets[i]..offsets[i+1]` is the byte range (data + trailer) of value block `i`.
    offsets: Vec<u64>,
}

impl PebbleBlobReader {
    /// Opens a Pebble blob file, parsing its footer and index block.
    pub fn open(file: impl Into<Arc<[u8]>>) -> Result<PebbleBlobReader> {
        let file: Arc<[u8]> = file.into();
        if file.len() < FOOTER_LEN {
            return Err(Error::corruption("pebble blob: file smaller than footer"));
        }
        let f = &file[file.len() - FOOTER_LEN..];
        if &f[30..38] != BLOB_FILE_MAGIC {
            return Err(Error::corruption("pebble blob: bad magic"));
        }
        let index_offset = u64::from_le_bytes(f[4..12].try_into().unwrap());
        let index_length = u64::from_le_bytes(f[12..20].try_into().unwrap());
        let checksum = ChecksumType::from_u8(f[20])?;
        let format = f[21];
        if format != BLOB_FORMAT_V1 {
            return Err(Error::corruption("pebble blob: unsupported format"));
        }

        let index = read_block(
            &file,
            BlockHandle {
                offset: index_offset,
                length: index_length,
            },
            checksum,
        )?;
        // 4-byte custom header: countVirtualBlocks (unused here — we read non-rewritten files'
        // physical block offsets directly). The columnar header's row count is countBlocks; the
        // offsets column has countBlocks + 1 entries.
        let header = colblk::BlockHeader::parse_at(&index, INDEX_CUSTOM_HEADER_LEN)?;
        let count_blocks = header.rows as usize;
        let (offsets, _) = colblk::decode_uint_column(
            &index,
            header.columns[INDEX_COL_OFFSETS].page_offset as usize,
            count_blocks + 1,
        )?;

        Ok(PebbleBlobReader {
            file,
            checksum,
            offsets,
        })
    }

    /// The number of value blocks in the file.
    pub fn num_blocks(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    /// Decodes all values in value block `block_id`.
    fn read_block_values(&self, block_id: usize) -> Result<Vec<Vec<u8>>> {
        let start = *self
            .offsets
            .get(block_id)
            .ok_or_else(|| Error::corruption("pebble blob: block id out of range"))?
            as usize;
        let end = *self
            .offsets
            .get(block_id + 1)
            .ok_or_else(|| Error::corruption("pebble blob: block id out of range"))?
            as usize;
        // The offset range covers the block data plus its trailer; read_block adds the trailer.
        let data_len = end
            .checked_sub(start)
            .and_then(|n| n.checked_sub(TRAILER_LEN))
            .ok_or_else(|| Error::corruption("pebble blob: bad block range"))?;
        let block = read_block(
            &self.file,
            BlockHandle {
                offset: start as u64,
                length: data_len as u64,
            },
            self.checksum,
        )?;
        let header = colblk::BlockHeader::parse_at(&block, 0)?;
        let rows = header.rows as usize;
        let (values, _) =
            colblk::decode_raw_bytes(&block, header.columns[VALUE_COL].page_offset as usize, rows)?;
        Ok(values.iter().map(|v| v.to_vec()).collect())
    }

    /// Reads the value addressed by `handle`.
    pub fn get(&self, handle: Handle) -> Result<Vec<u8>> {
        let values = self.read_block_values(handle.block_id as usize)?;
        values
            .get(handle.value_id as usize)
            .cloned()
            .ok_or_else(|| Error::corruption("pebble blob: value id out of range"))
    }

    /// Returns every value in the file, in block then value order.
    pub fn iter_all(&self) -> Result<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        for b in 0..self.num_blocks() {
            out.extend(self.read_block_values(b)?);
        }
        Ok(out)
    }
}
