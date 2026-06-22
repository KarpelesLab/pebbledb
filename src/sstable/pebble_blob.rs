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

/// A blob handle as encoded inline within an sstable value column (after the value-prefix byte).
/// It does not name the blob file directly: `reference_id` indexes into the sstable's ordered blob
/// references (recorded in the MANIFEST) to find the blob file number; `block_id` / `value_id` then
/// locate the value within that file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InlineHandle {
    /// Index into the sstable's blob references (→ a blob file number).
    pub reference_id: u32,
    /// The value's length in bytes.
    pub value_len: u32,
    /// The block within the referenced blob file.
    pub block_id: u32,
    /// The value within that block.
    pub value_id: u32,
}

impl InlineHandle {
    /// The `(block_id, value_id)` location of this handle's value within its blob file.
    pub fn location(self) -> Handle {
        Handle {
            block_id: self.block_id,
            value_id: self.value_id,
        }
    }
}

/// Mask selecting the value-kind bits of a value-prefix byte (Pebble's `valueKindMask`).
pub const VALUE_KIND_MASK: u8 = 0xC0;
/// Value-prefix kind bits indicating an in-sstable value-block handle.
pub const VALUE_KIND_VALUE_BLOCK_HANDLE: u8 = 0x80;
/// Value-prefix kind bits indicating an inline blob handle (a reference into a native blob file).
pub const VALUE_KIND_BLOB_HANDLE: u8 = 0x40;

/// Resolves a value stored in a native blob file. `file_num` is the blob file's number (obtained
/// by mapping an [`InlineHandle::reference_id`] through the sstable's blob references); the handle
/// locates the value within that file.
pub trait NativeBlobResolver: Send + Sync {
    /// Fetches the value at `handle` from blob file `file_num`.
    fn get(&self, file_num: u64, handle: Handle) -> Result<Vec<u8>>;
}

/// Decodes an inline blob handle: four uvarints `(reference_id, value_len, block_id, value_id)`.
/// `src` must start at the first varint (i.e. after any value-prefix byte).
pub fn decode_inline_handle(src: &[u8]) -> Result<InlineHandle> {
    use crate::base::varint::get_uvarint;
    let (reference_id, n1) = get_uvarint(src)
        .ok_or_else(|| Error::corruption("pebble blob: bad inline reference id"))?;
    let (value_len, n2) = get_uvarint(&src[n1..])
        .ok_or_else(|| Error::corruption("pebble blob: bad inline value len"))?;
    let (block_id, n3) = get_uvarint(&src[n1 + n2..])
        .ok_or_else(|| Error::corruption("pebble blob: bad inline block id"))?;
    let (value_id, _) = get_uvarint(&src[n1 + n2 + n3..])
        .ok_or_else(|| Error::corruption("pebble blob: bad inline value id"))?;
    Ok(InlineHandle {
        reference_id: reference_id as u32,
        value_len: value_len as u32,
        block_id: block_id as u32,
        value_id: value_id as u32,
    })
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

/// Column-header length in a colblk block (data type byte + 4-byte page offset).
const COLUMN_HEADER_LEN: usize = 5;
/// Colblk data-type byte for a uint column.
const COL_TYPE_UINT: u8 = 2;
/// Colblk data-type byte for a raw-bytes column.
const COL_TYPE_BYTES: u8 = 3;

/// Writes a Pebble native blob file. Values are appended with [`add_value`](Self::add_value),
/// which returns the [`Handle`] locating each, and the file is serialized with
/// [`finish`](Self::finish). All values are placed in a single value block (block 0), which is a
/// valid non-rewritten blob file; the output round-trips through [`PebbleBlobReader`] and is
/// readable by upstream Pebble.
pub struct PebbleBlobWriter {
    compression: super::block::CompressionType,
    checksum: ChecksumType,
    original_file_num: u64,
    values: Vec<Vec<u8>>,
}

impl PebbleBlobWriter {
    /// Creates a writer. `original_file_num` is recorded in the footer (the file's own number).
    pub fn new(
        compression: super::block::CompressionType,
        checksum: ChecksumType,
        original_file_num: u64,
    ) -> PebbleBlobWriter {
        PebbleBlobWriter {
            compression,
            checksum,
            original_file_num,
            values: Vec::new(),
        }
    }

    /// Appends a value, returning the handle that locates it (block 0, the value's index).
    pub fn add_value(&mut self, value: &[u8]) -> Handle {
        let value_id = self.values.len() as u32;
        self.values.push(value.to_vec());
        Handle {
            block_id: 0,
            value_id,
        }
    }

    /// Builds a colblk value block: a single RawBytes column of the values.
    fn value_block(&self) -> Vec<u8> {
        let rows = self.values.len();
        let mut buf = Vec::new();
        buf.push(1); // colblk version 1
        buf.extend_from_slice(&1u16.to_le_bytes()); // 1 column
        buf.extend_from_slice(&(rows as u32).to_le_bytes());
        let header_at = buf.len();
        buf.resize(header_at + COLUMN_HEADER_LEN, 0);
        let refs: Vec<&[u8]> = self.values.iter().map(|v| v.as_slice()).collect();
        let col_off = buf.len();
        super::colblk::encode_raw_bytes(&refs, col_off, &mut buf);
        buf.push(0); // trailing padding
        buf[header_at] = COL_TYPE_BYTES;
        buf[header_at + 1..header_at + 5].copy_from_slice(&(col_off as u32).to_le_bytes());
        buf
    }

    /// Builds the colblk index block: a 4-byte `countVirtualBlocks` (0) custom header, then two
    /// uint columns — `virtualBlocks` (empty) and `offsets` (`countBlocks + 1` entries).
    fn index_block(&self, offsets: &[u64]) -> Vec<u8> {
        let count_blocks = offsets.len() - 1;
        let mut buf = Vec::new();
        buf.extend_from_slice(&0u32.to_le_bytes()); // countVirtualBlocks = 0
        buf.push(1); // colblk version 1
        buf.extend_from_slice(&2u16.to_le_bytes()); // 2 columns
        buf.extend_from_slice(&(count_blocks as u32).to_le_bytes());
        let header_at = buf.len();
        buf.resize(header_at + 2 * COLUMN_HEADER_LEN, 0);
        // The virtualBlocks column has zero rows (this file is not rewritten), so it occupies no
        // bytes: it shares the offsets column's start offset. Encoding a 0-row column as an empty
        // page (rather than writing an encoding byte) is what Pebble's colblk expects.
        let off_off = buf.len();
        super::colblk::encode_uint_column(offsets, off_off, &mut buf);
        buf.push(0); // trailing padding
        buf[header_at] = COL_TYPE_UINT;
        buf[header_at + 1..header_at + 5].copy_from_slice(&(off_off as u32).to_le_bytes());
        buf[header_at + COLUMN_HEADER_LEN] = COL_TYPE_UINT;
        buf[header_at + COLUMN_HEADER_LEN + 1..header_at + COLUMN_HEADER_LEN + 5]
            .copy_from_slice(&(off_off as u32).to_le_bytes());
        buf
    }

    /// Serializes the blob file: the value block, the index block, and the 38-byte footer.
    pub fn finish(self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        let mut offset = 0u64;

        // One value block holding every value.
        let vblock = self.value_block();
        let vhandle = super::writer::write_block(
            &mut buf,
            &mut offset,
            &vblock,
            self.compression,
            self.checksum,
        )?;
        // offsets: each block's start, plus the end of the last block (start + body + trailer).
        let offsets = vec![
            vhandle.offset,
            vhandle.offset + vhandle.length + TRAILER_LEN as u64,
        ];

        // Index block (uncompressed, matching Pebble's metadata blocks).
        let iblock = self.index_block(&offsets);
        let ihandle = super::writer::write_block(
            &mut buf,
            &mut offset,
            &iblock,
            super::block::CompressionType::None,
            self.checksum,
        )?;

        // Footer (38 bytes): crc(4) | index_offset(8) | index_length(8) | checksum_type(1) |
        // format(1) | original_file_num(8) | magic(8). The CRC is a masked CRC32C over bytes 4..38.
        let mut footer = vec![0u8; FOOTER_LEN];
        footer[4..12].copy_from_slice(&ihandle.offset.to_le_bytes());
        footer[12..20].copy_from_slice(&ihandle.length.to_le_bytes());
        footer[20] = match self.checksum {
            ChecksumType::None => 0,
            ChecksumType::Crc32c => 1,
            ChecksumType::XxHash => 2,
            ChecksumType::XxHash64 => 3,
        };
        footer[21] = BLOB_FORMAT_V1;
        footer[22..30].copy_from_slice(&self.original_file_num.to_le_bytes());
        footer[30..38].copy_from_slice(BLOB_FILE_MAGIC);
        let crc = crate::crc::masked_crc32c(&footer[4..FOOTER_LEN]);
        footer[0..4].copy_from_slice(&crc.to_le_bytes());
        buf.extend_from_slice(&footer);
        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writer_roundtrips_through_reader() {
        use super::super::block::{ChecksumType, CompressionType};
        let mut w = PebbleBlobWriter::new(CompressionType::None, ChecksumType::Crc32c, 7);
        let values: Vec<Vec<u8>> = (0..20).map(|i| format!("value-{i}").into_bytes()).collect();
        let mut handles = Vec::new();
        for v in &values {
            handles.push(w.add_value(v));
        }
        let bytes = w.finish().unwrap();

        let r = PebbleBlobReader::open(bytes).unwrap();
        assert_eq!(r.iter_all().unwrap(), values);
        for (i, h) in handles.iter().enumerate() {
            assert_eq!(r.get(*h).unwrap(), values[i]);
        }
    }

    #[test]
    fn decodes_inline_handle() {
        // `00 3c 00 00` = (reference_id=0, value_len=60, block_id=0, value_id=0), the encoding of
        // a separated 60-byte value observed in a real Pebble v6 data block (after the 0x40 prefix).
        let h = decode_inline_handle(&[0x00, 0x3c, 0x00, 0x00]).unwrap();
        assert_eq!(
            h,
            InlineHandle {
                reference_id: 0,
                value_len: 60,
                block_id: 0,
                value_id: 0,
            }
        );
        assert_eq!(
            h.location(),
            Handle {
                block_id: 0,
                value_id: 0
            }
        );

        // value_id 4 in the same block/reference.
        let h2 = decode_inline_handle(&[0x00, 0x3c, 0x00, 0x04]).unwrap();
        assert_eq!(h2.value_id, 4);
        assert_eq!(h2.value_len, 60);
    }
}
