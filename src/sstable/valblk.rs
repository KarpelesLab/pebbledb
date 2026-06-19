// Copyright (c) 2022 The LevelDB-Go Authors. All rights reserved.
// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Ported from Pebble's sstable/valblk (valblk.go).

//! Value blocks (Pebble format v3+): out-of-line storage for point-key values.
//!
//! In a v3+ table every point value stored in a data block is prefixed by a single
//! *value-prefix* byte. Its top two bits are the value kind: `0` means the value is
//! stored in place (the bytes after the prefix are the value); `1` means the bytes after
//! the prefix are a *value handle* `(value_len, block_num, offset_in_block)` pointing
//! into a value block.
//!
//! The metaindex entry `pebble.value_index` holds a [`IndexHandle`] describing the
//! value-block index — a sequence of fixed-width `(block_num, offset, length)` rows that
//! map a block number to the value block's on-disk [`BlockHandle`].

use crate::base::varint::{get_uvarint, put_uvarint};
use crate::sstable::block::BlockHandle;
use crate::{Error, Result};

/// The value kind in a value-prefix byte.
pub fn value_kind(prefix: u8) -> u8 {
    (prefix >> 6) & 0x3
}

/// In-place value kind.
pub const KIND_IN_PLACE: u8 = 0;
/// Value-handle kind (points into a value block).
pub const KIND_HANDLE: u8 = 1;

/// A pointer to a value within a value block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ValueHandle {
    /// Length of the value in bytes.
    pub value_len: u32,
    /// The 0-indexed value block number.
    pub block_num: u32,
    /// Byte offset of the value within the (decompressed) value block.
    pub offset_in_block: u32,
}

/// Encodes a value handle (`value_len`, `block_num`, `offset_in_block` as uvarints).
pub fn encode_handle(h: ValueHandle) -> Vec<u8> {
    let mut dst = Vec::new();
    put_uvarint(&mut dst, u64::from(h.value_len));
    put_uvarint(&mut dst, u64::from(h.block_num));
    put_uvarint(&mut dst, u64::from(h.offset_in_block));
    dst
}

/// Decodes a value handle (`value_len`, `block_num`, `offset_in_block` as uvarints).
pub fn decode_handle(src: &[u8]) -> Result<ValueHandle> {
    let (value_len, n1) =
        get_uvarint(src).ok_or_else(|| Error::corruption("valblk: bad handle value_len"))?;
    let (block_num, n2) =
        get_uvarint(&src[n1..]).ok_or_else(|| Error::corruption("valblk: bad handle block_num"))?;
    let (offset, _) = get_uvarint(&src[n1 + n2..])
        .ok_or_else(|| Error::corruption("valblk: bad handle offset"))?;
    Ok(ValueHandle {
        value_len: value_len as u32,
        block_num: block_num as u32,
        offset_in_block: offset as u32,
    })
}

/// The metaindex `pebble.value_index` entry: the index block's handle plus the
/// byte-widths of the three columns in each index row.
#[derive(Clone, Copy, Debug)]
pub struct IndexHandle {
    /// On-disk handle of the value-block index block.
    pub handle: BlockHandle,
    /// Width in bytes of the block-number column.
    pub block_num_len: u8,
    /// Width in bytes of the block-offset column.
    pub block_offset_len: u8,
    /// Width in bytes of the block-length column.
    pub block_length_len: u8,
}

/// Decodes an [`IndexHandle`] from a metaindex value.
pub fn decode_index_handle(src: &[u8]) -> Result<IndexHandle> {
    let (handle, n) =
        BlockHandle::decode(src).ok_or_else(|| Error::corruption("valblk: bad index handle"))?;
    if src.len() != n + 3 {
        return Err(Error::corruption("valblk: bad index handle trailer"));
    }
    Ok(IndexHandle {
        handle,
        block_num_len: src[n],
        block_offset_len: src[n + 1],
        block_length_len: src[n + 2],
    })
}

/// Encodes the metaindex `pebble.value_index` value: the index block handle followed by
/// the three column byte-widths.
pub fn encode_index_handle(ih: &IndexHandle) -> Vec<u8> {
    let mut dst = Vec::new();
    ih.handle.encode_to(&mut dst);
    dst.push(ih.block_num_len);
    dst.push(ih.block_offset_len);
    dst.push(ih.block_length_len);
    dst
}

/// The minimum number of bytes needed to little-endian encode `v` (at least 1).
pub fn min_width(v: u64) -> u8 {
    let mut w = 1u8;
    while w < 8 && v >= (1u64 << (8 * w as u32)) {
        w += 1;
    }
    w
}

/// Appends the low `width` bytes of `v` in little-endian order.
pub fn put_uint_le(dst: &mut Vec<u8>, v: u64, width: u8) {
    for i in 0..width {
        dst.push((v >> (8 * i as u32)) as u8);
    }
}

/// Reads a little-endian unsigned integer of `width` bytes from the front of `src`.
fn read_uint_le(src: &[u8], width: usize) -> u64 {
    let mut v = 0u64;
    for (i, &b) in src.iter().take(width).enumerate() {
        v |= u64::from(b) << (8 * i);
    }
    v
}

/// Decodes the value-block index block into the list of value-block handles, indexed by
/// block number.
pub fn decode_index(data: &[u8], ih: &IndexHandle) -> Result<Vec<BlockHandle>> {
    let row =
        ih.block_num_len as usize + ih.block_offset_len as usize + ih.block_length_len as usize;
    if row == 0 {
        return Err(Error::corruption("valblk: zero-width index rows"));
    }
    let mut handles = Vec::new();
    let mut i = 0;
    while i + row <= data.len() {
        let r = &data[i..i + row];
        let block_num = read_uint_le(r, ih.block_num_len as usize);
        let offset = read_uint_le(
            &r[ih.block_num_len as usize..],
            ih.block_offset_len as usize,
        );
        let length = read_uint_le(
            &r[ih.block_num_len as usize + ih.block_offset_len as usize..],
            ih.block_length_len as usize,
        );
        if block_num as usize != handles.len() {
            return Err(Error::corruption(
                "valblk: non-consecutive value block numbers",
            ));
        }
        handles.push(BlockHandle { offset, length });
        i += row;
    }
    Ok(handles)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::varint::put_uvarint;

    #[test]
    fn value_prefix_kinds() {
        assert_eq!(value_kind(0b0000_0000), KIND_IN_PLACE);
        assert_eq!(value_kind(0b0100_0000), KIND_HANDLE);
        // Lower bits (set-same-prefix, short attribute) don't affect the kind.
        assert_eq!(value_kind(0b0001_0101), KIND_IN_PLACE);
        assert_eq!(value_kind(0b0110_0101), KIND_HANDLE);
    }

    #[test]
    fn handle_roundtrip() {
        let mut buf = Vec::new();
        put_uvarint(&mut buf, 1234); // value_len
        put_uvarint(&mut buf, 2); // block_num
        put_uvarint(&mut buf, 5678); // offset
        let h = decode_handle(&buf).unwrap();
        assert_eq!(
            h,
            ValueHandle {
                value_len: 1234,
                block_num: 2,
                offset_in_block: 5678
            }
        );
    }

    #[test]
    fn index_handle_and_decode() {
        // Build an index handle: block handle (offset=100,len=12) + (2,4,2) widths.
        let mut enc = Vec::new();
        BlockHandle {
            offset: 100,
            length: 12,
        }
        .encode_to(&mut enc);
        enc.extend_from_slice(&[2, 4, 2]);
        let ih = decode_index_handle(&enc).unwrap();
        assert_eq!(ih.block_num_len, 2);
        assert_eq!(ih.block_offset_len, 4);
        assert_eq!(ih.block_length_len, 2);

        // Two rows: block 0 @ (offset 0, len 50), block 1 @ (offset 55, len 60).
        let mut data = Vec::new();
        data.extend_from_slice(&0u16.to_le_bytes());
        data.extend_from_slice(&0u32.to_le_bytes());
        data.extend_from_slice(&50u16.to_le_bytes());
        data.extend_from_slice(&1u16.to_le_bytes());
        data.extend_from_slice(&55u32.to_le_bytes());
        data.extend_from_slice(&60u16.to_le_bytes());
        let handles = decode_index(&data, &ih).unwrap();
        assert_eq!(handles.len(), 2);
        assert_eq!(
            handles[0],
            BlockHandle {
                offset: 0,
                length: 50
            }
        );
        assert_eq!(
            handles[1],
            BlockHandle {
                offset: 55,
                length: 60
            }
        );
    }
}
