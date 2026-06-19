// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Ported from Pebble's sstable/colblk package (the columnar block format header).

//! The Pebble columnar block format (table format versions v5–v8).
//!
//! A columnar block stores data column-by-column rather than row-by-row. Every columnar
//! block begins with a header describing its schema, followed by each column's data, and a
//! trailing padding byte:
//!
//! ```text
//! +-----------+
//! | Vers (1B) |
//! +-------------------+----------------------+
//! | # columns (2B LE) | # rows (4B LE)       |
//! +-----------+-------+----------------------+
//! | Type (1B) | Page offset (4B LE)         |  column 0 header
//! +-----------+-----------------------------+
//! | ...                                     |  ...
//! +-----------+-----------------------------+
//! |  column 0 data ...                      |
//! |  column 1 data ...                      |
//! |  ...                                    |
//! +-------------+
//! | Unused (1B) |  trailing padding byte
//! +-------------+
//! ```
//!
//! This module parses the block header and locates each column's data. The per-column
//! codecs ([`DataType::Uint`] width/delta encoding, [`DataType::PrefixBytes`] prefix
//! compression, [`DataType::Bytes`], [`DataType::Bool`] bitmaps) and the data / index /
//! keyspan block schemas built on top of them are still being ported; see the crate
//! `ROADMAP.md`. Until then, [`crate::sstable::Reader`] reports a clear error for columnar
//! tables rather than misreading them.

use crate::{Error, Result};

/// The size in bytes of the fixed part of a columnar block header (version + column count
/// + row count), before the per-column headers.
const BLOCK_HEADER_BASE_LEN: usize = 1 + 2 + 4;
/// The size in bytes of a single column header (data type + page offset).
const COLUMN_HEADER_LEN: usize = 1 + 4;

/// The data type of a column in a columnar block. The on-disk values match Pebble's
/// `colblk.DataType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataType {
    /// An unset / invalid column.
    Invalid,
    /// One boolean per row, stored as a bitmap.
    Bool,
    /// One unsigned integer per row (width selected by the column's uint encoding, with
    /// optional delta encoding from a per-column constant).
    Uint,
    /// A variable-length byte string per row.
    Bytes,
    /// Variable-length, lexicographically-sorted byte strings with prefix compression.
    PrefixBytes,
}

impl DataType {
    /// Maps an on-disk data-type byte to a [`DataType`].
    pub fn from_u8(b: u8) -> Result<DataType> {
        Ok(match b {
            0 => DataType::Invalid,
            1 => DataType::Bool,
            2 => DataType::Uint,
            3 => DataType::Bytes,
            4 => DataType::PrefixBytes,
            other => {
                return Err(Error::Corruption(format!(
                    "colblk: unknown column data type {other}"
                )));
            }
        })
    }
}

/// One column's header: its data type and the byte offset within the block where its data
/// begins.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ColumnHeader {
    /// The column's data type.
    pub data_type: DataType,
    /// The offset, from the start of the block, of this column's data.
    pub page_offset: u32,
}

/// The parsed header of a columnar block: its schema and row count, plus a reference to the
/// underlying block bytes for locating column data.
#[derive(Clone, Debug)]
pub struct BlockHeader {
    /// The columnar block format version (the leading header byte).
    pub version: u8,
    /// The number of rows encoded in the block.
    pub rows: u32,
    /// One header per column, in order.
    pub columns: Vec<ColumnHeader>,
}

impl BlockHeader {
    /// Parses the header of a columnar block. Validates that every column's data offset
    /// lies within the block.
    pub fn parse(block: &[u8]) -> Result<BlockHeader> {
        if block.len() < BLOCK_HEADER_BASE_LEN {
            return Err(Error::corruption("colblk: block smaller than header"));
        }
        let version = block[0];
        let num_columns = u16::from_le_bytes([block[1], block[2]]) as usize;
        let rows = u32::from_le_bytes([block[3], block[4], block[5], block[6]]);

        let headers_end = BLOCK_HEADER_BASE_LEN + num_columns * COLUMN_HEADER_LEN;
        if block.len() < headers_end + 1 {
            // +1 for the trailing padding byte.
            return Err(Error::corruption(
                "colblk: block truncated in column headers",
            ));
        }

        let mut columns = Vec::with_capacity(num_columns);
        for i in 0..num_columns {
            let off = BLOCK_HEADER_BASE_LEN + i * COLUMN_HEADER_LEN;
            let data_type = DataType::from_u8(block[off])?;
            let page_offset = u32::from_le_bytes([
                block[off + 1],
                block[off + 2],
                block[off + 3],
                block[off + 4],
            ]);
            if page_offset as usize > block.len() {
                return Err(Error::corruption("colblk: column offset past end of block"));
            }
            columns.push(ColumnHeader {
                data_type,
                page_offset,
            });
        }

        Ok(BlockHeader {
            version,
            rows,
            columns,
        })
    }

    /// The byte range of column `i`'s data within the block, `[start, end)`. The end is the
    /// next column's offset, or the block's trailing padding byte for the last column.
    pub fn column_range(&self, i: usize, block_len: usize) -> Option<(usize, usize)> {
        let start = self.columns.get(i)?.page_offset as usize;
        let end = match self.columns.get(i + 1) {
            Some(next) => next.page_offset as usize,
            None => block_len.saturating_sub(1), // exclude the trailing padding byte
        };
        if end < start || end > block_len {
            return None;
        }
        Some((start, end))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a minimal columnar block with the given column (type, data) pairs.
    fn build(version: u8, rows: u32, cols: &[(DataType, &[u8])]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(version);
        buf.extend_from_slice(&(cols.len() as u16).to_le_bytes());
        buf.extend_from_slice(&rows.to_le_bytes());
        // Compute where column data starts (after all column headers).
        let mut offset = (BLOCK_HEADER_BASE_LEN + cols.len() * COLUMN_HEADER_LEN) as u32;
        let mut offsets = Vec::new();
        for (_, data) in cols {
            offsets.push(offset);
            offset += data.len() as u32;
        }
        for ((ty, _), off) in cols.iter().zip(&offsets) {
            buf.push(match ty {
                DataType::Invalid => 0,
                DataType::Bool => 1,
                DataType::Uint => 2,
                DataType::Bytes => 3,
                DataType::PrefixBytes => 4,
            });
            buf.extend_from_slice(&off.to_le_bytes());
        }
        for (_, data) in cols {
            buf.extend_from_slice(data);
        }
        buf.push(0); // trailing padding byte
        buf
    }

    #[test]
    fn parses_header_and_locates_columns() {
        let block = build(
            1,
            3,
            &[
                (DataType::Uint, &[1, 2, 3, 4]),
                (DataType::Bytes, &[9, 9, 9]),
            ],
        );
        let h = BlockHeader::parse(&block).unwrap();
        assert_eq!(h.version, 1);
        assert_eq!(h.rows, 3);
        assert_eq!(h.columns.len(), 2);
        assert_eq!(h.columns[0].data_type, DataType::Uint);
        assert_eq!(h.columns[1].data_type, DataType::Bytes);

        let (s0, e0) = h.column_range(0, block.len()).unwrap();
        assert_eq!(&block[s0..e0], &[1, 2, 3, 4]);
        let (s1, e1) = h.column_range(1, block.len()).unwrap();
        assert_eq!(&block[s1..e1], &[9, 9, 9]);
    }

    #[test]
    fn rejects_truncated_and_bad_type() {
        assert!(BlockHeader::parse(&[0u8; 3]).is_err());
        // A header claiming 2 columns but with no room for them.
        let mut b = Vec::new();
        b.push(1u8);
        b.extend_from_slice(&2u16.to_le_bytes());
        b.extend_from_slice(&0u32.to_le_bytes());
        assert!(BlockHeader::parse(&b).is_err());
    }

    #[test]
    fn unknown_data_type_is_rejected() {
        assert!(DataType::from_u8(9).is_err());
    }
}
