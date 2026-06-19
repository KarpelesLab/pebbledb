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
//! This module provides the block header parser, the per-column codecs — the uint column
//! ([`encode_uint_column`]/[`decode_uint_column`], variable width + optional delta base),
//! the raw-bytes column ([`encode_raw_bytes`]/[`decode_raw_bytes`], an offsets table plus
//! concatenated data), and the bool bitmap ([`encode_bitmap`]/[`decode_bitmap`]) — all
//! matching Pebble's exact on-disk layouts, and the three columnar block formats built on
//! them, each read + write: the data block ([`DataBlockBuilder`] / [`DataBlockReader`]),
//! the index block ([`IndexBlockBuilder`] / [`decode_index_block`]), and the keyspan block
//! ([`KeyspanBlockBuilder`] / [`decode_keyspan_block`]).
//!
//! The [`DataType::PrefixBytes`] bundle prefix-compression codec
//! ([`encode_prefix_bytes`]/[`decode_prefix_bytes`]) is also implemented. These blocks are
//! assembled into a complete columnar sstable by [`crate::sstable::columnar`] (read +
//! write). Note: `PrefixBytes`'s offset sub-table uses the standard uint-column encoding
//! here; Pebble omits the always-zero delta base in the rare wide-offset case, so
//! byte-for-byte interchange of a delta-encoded offset table — and reading Pebble's
//! production tables, which use their application key schema — is validated by the interop
//! CI (see `ROADMAP.md`).

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

// ---------------------------------------------------------------------------
// Column codecs
//
// These reproduce Pebble's `colblk` per-column on-disk encodings exactly, so the bytes
// are compatible: the uint column (variable width + optional delta base), the raw-bytes
// column (an offsets table + concatenated data), and the bool bitmap column.
// ---------------------------------------------------------------------------

/// Rounds `pos` up to the next multiple of `align` (a power of two: 1, 2, 4, or 8).
fn align_up(pos: usize, align: usize) -> usize {
    debug_assert!(align.is_power_of_two());
    (pos + align - 1) & !(align - 1)
}

/// The number of bytes needed to represent `v`: 0, 1, 2, 4, or 8.
fn byte_width(v: u64) -> usize {
    match 64 - v.leading_zeros() {
        0 => 0,
        1..=8 => 1,
        9..=16 => 2,
        17..=32 => 4,
        _ => 8,
    }
}

const UINT_DELTA_BIT: u8 = 1 << 7;

/// Chooses the uint encoding byte (width in the low bits, delta flag in the high bit) for
/// a column of values in `[min, max]`, mirroring Pebble's `DetermineUintEncoding`.
fn determine_uint_encoding(min: u64, max: u64, rows: usize) -> u8 {
    let mut b = byte_width(max - min);
    if b == 8 {
        return 8;
    }
    let mut is_delta = max >= (1u64 << (b * 8));
    if is_delta && rows < 8 {
        let b_no_delta = byte_width(max);
        if rows * (b_no_delta - b) < 8 {
            b = b_no_delta;
            is_delta = false;
        }
    }
    b as u8 | if is_delta { UINT_DELTA_BIT } else { 0 }
}

fn read_uint_le(b: &[u8], width: usize) -> u64 {
    let mut v = 0u64;
    for (i, &byte) in b.iter().take(width).enumerate() {
        v |= (byte as u64) << (i * 8);
    }
    v
}

/// Encodes a uint column at `offset` within `buf`, growing `buf` as needed. Returns the
/// offset just past the column.
pub fn encode_uint_column(values: &[u64], offset: usize, buf: &mut Vec<u8>) -> usize {
    let rows = values.len();
    let (min, max) = values
        .iter()
        .fold((u64::MAX, 0u64), |(lo, hi), &v| (lo.min(v), hi.max(v)));
    let (min, max) = if rows == 0 { (0, 0) } else { (min, max) };
    let enc = determine_uint_encoding(min, max, rows);
    let width = (enc & !UINT_DELTA_BIT) as usize;
    let is_delta = enc & UINT_DELTA_BIT != 0;

    if buf.len() < offset {
        buf.resize(offset, 0);
    }
    buf.truncate(offset);
    buf.push(enc);
    let base = if is_delta {
        buf.extend_from_slice(&min.to_le_bytes());
        min
    } else {
        0
    };
    if width == 0 {
        return buf.len();
    }
    let aligned = align_up(buf.len(), width);
    buf.resize(aligned, 0);
    for &v in values {
        let delta = v - base;
        buf.extend_from_slice(&delta.to_le_bytes()[..width]);
    }
    buf.len()
}

/// Decodes a uint column of `rows` values starting at `off` within `b`. Returns the values
/// and the offset just past the column.
pub fn decode_uint_column(b: &[u8], mut off: usize, rows: usize) -> Result<(Vec<u64>, usize)> {
    let enc = *b
        .get(off)
        .ok_or_else(|| Error::corruption("colblk: truncated uint column"))?;
    off += 1;
    let width = (enc & !UINT_DELTA_BIT) as usize;
    let is_delta = enc & UINT_DELTA_BIT != 0;
    if !matches!(width, 0 | 1 | 2 | 4 | 8) {
        return Err(Error::corruption("colblk: invalid uint width"));
    }
    let base = if is_delta {
        let bytes = b
            .get(off..off + 8)
            .ok_or_else(|| Error::corruption("colblk: truncated uint base"))?;
        off += 8;
        u64::from_le_bytes(bytes.try_into().unwrap())
    } else {
        0
    };
    if width == 0 {
        return Ok((vec![base; rows], off));
    }
    off = align_up(off, width);
    let end = off + rows * width;
    if b.len() < end {
        return Err(Error::corruption("colblk: truncated uint values"));
    }
    let mut out = Vec::with_capacity(rows);
    for i in 0..rows {
        out.push(base + read_uint_le(&b[off + i * width..], width));
    }
    Ok((out, end))
}

/// Encodes a raw-bytes column (an offsets table of `count+1` entries followed by the
/// concatenated slice data) at `offset` within `buf`. Returns the offset just past it.
pub fn encode_raw_bytes(slices: &[&[u8]], offset: usize, buf: &mut Vec<u8>) -> usize {
    if slices.is_empty() {
        buf.truncate(offset);
        return offset;
    }
    let mut offsets = Vec::with_capacity(slices.len() + 1);
    let mut acc = 0u64;
    offsets.push(0);
    for s in slices {
        acc += s.len() as u64;
        offsets.push(acc);
    }
    let after_offsets = encode_uint_column(&offsets, offset, buf);
    // String data follows the offset table; offsets are relative to here.
    for s in slices {
        buf.extend_from_slice(s);
    }
    let _ = after_offsets;
    buf.len()
}

/// Decodes a raw-bytes column of `count` slices starting at `off`. Returns the slices and
/// the offset just past the column.
pub fn decode_raw_bytes(b: &[u8], off: usize, count: usize) -> Result<(Vec<&[u8]>, usize)> {
    if count == 0 {
        return Ok((Vec::new(), off));
    }
    let (offsets, data_start) = decode_uint_column(b, off, count + 1)?;
    let mut out = Vec::with_capacity(count);
    let mut end = data_start;
    for i in 0..count {
        let s = data_start + offsets[i] as usize;
        let e = data_start + offsets[i + 1] as usize;
        let slice = b
            .get(s..e)
            .ok_or_else(|| Error::corruption("colblk: raw-bytes slice out of range"))?;
        out.push(slice);
        end = e;
    }
    Ok((out, end))
}

const BITMAP_ZERO: u8 = 0;
const BITMAP_DEFAULT: u8 = 1;

/// Encodes a bool column as a bitmap at `offset` within `buf`. Returns the offset past it.
pub fn encode_bitmap(bits: &[bool], offset: usize, buf: &mut Vec<u8>) -> usize {
    buf.truncate(offset);
    if !bits.iter().any(|&x| x) {
        buf.push(BITMAP_ZERO);
        return buf.len();
    }
    buf.push(BITMAP_DEFAULT);
    let aligned = align_up(buf.len(), 8);
    buf.resize(aligned, 0);
    let n = bits.len();
    let primary_words = n.div_ceil(64);
    let summary_words = primary_words.div_ceil(64);
    let mut words = vec![0u64; primary_words + summary_words];
    for (i, &set) in bits.iter().enumerate() {
        if set {
            words[i / 64] |= 1u64 << (i % 64);
        }
    }
    for w in 0..primary_words {
        if words[w] != 0 {
            words[primary_words + w / 64] |= 1u64 << (w % 64);
        }
    }
    for w in &words {
        buf.extend_from_slice(&w.to_le_bytes());
    }
    buf.len()
}

/// Decodes a bool bitmap of `bit_count` bits starting at `off`. Returns the bools and the
/// offset just past the bitmap.
pub fn decode_bitmap(b: &[u8], mut off: usize, bit_count: usize) -> Result<(Vec<bool>, usize)> {
    let enc = *b
        .get(off)
        .ok_or_else(|| Error::corruption("colblk: truncated bitmap"))?;
    off += 1;
    if enc == BITMAP_ZERO {
        return Ok((vec![false; bit_count], off));
    }
    off = align_up(off, 8);
    let primary_words = bit_count.div_ceil(64);
    let summary_words = primary_words.div_ceil(64);
    let end = off + (primary_words + summary_words) * 8;
    if b.len() < end {
        return Err(Error::corruption("colblk: truncated bitmap data"));
    }
    let mut out = Vec::with_capacity(bit_count);
    for i in 0..bit_count {
        let word_off = off + (i / 64) * 8;
        let word = u64::from_le_bytes(b[word_off..word_off + 8].try_into().unwrap());
        out.push(word & (1u64 << (i % 64)) != 0);
    }
    Ok((out, end))
}

// ---------------------------------------------------------------------------
// PrefixBytes column
//
// Lexicographically-sorted byte slices with bundle-based prefix compression. The column
// stores 1 block-wide prefix, one prefix per bundle of `bundleSize` keys, and one suffix
// per key. A key is reconstructed as block_prefix ++ bundle_prefix ++ suffix. Layout:
// 1 byte log2(bundleSize), then an offsets table (end offset of each slice; the first
// slice — the block prefix — implicitly starts at 0), then the concatenated slice data.
// ---------------------------------------------------------------------------

/// Longest common prefix length of `a` and `b`.
fn lcp(a: &[u8], b: &[u8]) -> usize {
    let mut i = 0;
    let max = a.len().min(b.len());
    while i < max && a[i] == b[i] {
        i += 1;
    }
    i
}

/// Encodes a `PrefixBytes` column of lexicographically-sorted `keys` (bundle size a power
/// of two) at `offset` within `buf`. Returns the offset just past the column.
pub fn encode_prefix_bytes(
    keys: &[&[u8]],
    bundle_size: usize,
    offset: usize,
    buf: &mut Vec<u8>,
) -> usize {
    assert!(bundle_size.is_power_of_two() && bundle_size >= 1);
    buf.truncate(offset);
    buf.push(bundle_size.trailing_zeros() as u8);
    let n = keys.len();
    if n == 0 {
        // A single empty block-prefix slice.
        encode_uint_column(&[0], buf.len(), buf);
        return buf.len();
    }
    let block_prefix_len = lcp(keys[0], keys[n - 1]);
    let num_bundles = n.div_ceil(bundle_size);

    // Build the slices in storage order: block prefix, then per bundle [prefix, suffixes].
    let mut slices: Vec<&[u8]> = Vec::with_capacity(1 + num_bundles + n);
    slices.push(&keys[0][..block_prefix_len]);
    for b in 0..num_bundles {
        let start = b * bundle_size;
        let end = ((b + 1) * bundle_size).min(n);
        let bundle_lcp = lcp(keys[start], keys[end - 1]).max(block_prefix_len);
        slices.push(&keys[start][block_prefix_len..bundle_lcp]);
        for key in &keys[start..end] {
            slices.push(&key[bundle_lcp..]);
        }
    }

    // End offsets of each slice within the concatenated data section.
    let mut offsets = Vec::with_capacity(slices.len());
    let mut acc = 0u64;
    for s in &slices {
        acc += s.len() as u64;
        offsets.push(acc);
    }
    encode_uint_column(&offsets, buf.len(), buf);
    for s in &slices {
        buf.extend_from_slice(s);
    }
    buf.len()
}

/// Decodes a `PrefixBytes` column of `n` keys starting at `off`. Returns the reconstructed
/// keys and the offset just past the column.
pub fn decode_prefix_bytes(b: &[u8], mut off: usize, n: usize) -> Result<(Vec<Vec<u8>>, usize)> {
    let log2 = *b
        .get(off)
        .ok_or_else(|| Error::corruption("colblk: truncated prefix-bytes"))?;
    off += 1;
    let bundle_size = 1usize << log2;
    if n == 0 {
        let (_, end) = decode_uint_column(b, off, 1)?;
        return Ok((Vec::new(), end));
    }
    let num_bundles = n.div_ceil(bundle_size);
    let num_slices = 1 + num_bundles + n;
    let (offsets, data_start) = decode_uint_column(b, off, num_slices)?;
    let slice = |k: usize| -> Result<&[u8]> {
        let s = if k == 0 { 0 } else { offsets[k - 1] as usize };
        let e = offsets[k] as usize;
        b.get(data_start + s..data_start + e)
            .ok_or_else(|| Error::corruption("colblk: prefix-bytes slice out of range"))
    };
    let block_prefix = slice(0)?;
    let mut end = data_start;
    if let Some(last) = offsets.last() {
        end = data_start + *last as usize;
    }
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let bundle = i / bundle_size;
        let prefix_slice = 1 + bundle * (1 + bundle_size);
        let suffix_slice = prefix_slice + 1 + (i - bundle * bundle_size);
        let mut key = Vec::with_capacity(block_prefix.len());
        key.extend_from_slice(block_prefix);
        key.extend_from_slice(slice(prefix_slice)?);
        key.extend_from_slice(slice(suffix_slice)?);
        out.push(key);
    }
    Ok((out, end))
}

// ---------------------------------------------------------------------------
// Columnar data block
//
// A concrete columnar data block built from the column primitives above: three columns
// per row — the user key (raw bytes), the internal-key trailer (uint), and the value (raw
// bytes). This is the columnar analogue of the row-oriented data block: it stores the same
// information column-by-column and reconstructs `(internal_key, value)` pairs on read.
// ---------------------------------------------------------------------------

/// The columnar block format version this engine writes.
const DATA_BLOCK_VERSION: u8 = 1;
/// Column indices within a [`DataBlock`].
const COL_USER_KEY: usize = 0;
const COL_TRAILER: usize = 1;
const COL_VALUE: usize = 2;
const DATA_BLOCK_COLUMNS: usize = 3;

/// Builds a columnar data block from `(user_key, trailer, value)` rows.
#[derive(Default)]
pub struct DataBlockBuilder {
    user_keys: Vec<Vec<u8>>,
    trailers: Vec<u64>,
    values: Vec<Vec<u8>>,
}

impl DataBlockBuilder {
    /// Creates an empty builder.
    pub fn new() -> DataBlockBuilder {
        DataBlockBuilder::default()
    }

    /// Appends one row: the user key, the internal-key trailer, and the value.
    pub fn add(&mut self, user_key: &[u8], trailer: u64, value: &[u8]) {
        self.user_keys.push(user_key.to_vec());
        self.trailers.push(trailer);
        self.values.push(value.to_vec());
    }

    /// Number of rows added so far.
    pub fn rows(&self) -> usize {
        self.trailers.len()
    }

    /// Serializes the block: a header, the three columns, and the trailing padding byte.
    pub fn finish(&self) -> Vec<u8> {
        let rows = self.trailers.len();
        let mut buf = Vec::new();
        // Header: version, column count, row count.
        buf.push(DATA_BLOCK_VERSION);
        buf.extend_from_slice(&(DATA_BLOCK_COLUMNS as u16).to_le_bytes());
        buf.extend_from_slice(&(rows as u32).to_le_bytes());
        // Reserve space for the column headers; fill the offsets in as columns are written.
        let headers_at = buf.len();
        buf.resize(headers_at + DATA_BLOCK_COLUMNS * COLUMN_HEADER_LEN, 0);

        let key_refs: Vec<&[u8]> = self.user_keys.iter().map(|k| k.as_slice()).collect();
        let val_refs: Vec<&[u8]> = self.values.iter().map(|v| v.as_slice()).collect();

        let key_off = buf.len();
        encode_raw_bytes(&key_refs, key_off, &mut buf);
        let trailer_off = buf.len();
        encode_uint_column(&self.trailers, trailer_off, &mut buf);
        let value_off = buf.len();
        encode_raw_bytes(&val_refs, value_off, &mut buf);

        // Trailing padding byte (lets a column end be represented by a one-past pointer).
        buf.push(0);

        // Backfill the column headers (type + page offset).
        for (i, (ty, off)) in [
            (DataType::Bytes, key_off),
            (DataType::Uint, trailer_off),
            (DataType::Bytes, value_off),
        ]
        .iter()
        .enumerate()
        {
            let h = headers_at + i * COLUMN_HEADER_LEN;
            buf[h] = match ty {
                DataType::Bytes => 3,
                DataType::Uint => 2,
                _ => unreachable!(),
            };
            buf[h + 1..h + 5].copy_from_slice(&(*off as u32).to_le_bytes());
        }
        buf
    }
}

/// A decoded data-block row: `(user_key, internal-key trailer, value)`.
pub type DataBlockRow = (Vec<u8>, u64, Vec<u8>);

/// A reader over a columnar data block produced by [`DataBlockBuilder`], reconstructing the
/// `(user_key, trailer, value)` rows.
pub struct DataBlockReader<'a> {
    block: &'a [u8],
    header: BlockHeader,
}

impl<'a> DataBlockReader<'a> {
    /// Parses the block header.
    pub fn new(block: &'a [u8]) -> Result<DataBlockReader<'a>> {
        let header = BlockHeader::parse(block)?;
        if header.columns.len() != DATA_BLOCK_COLUMNS {
            return Err(Error::corruption(
                "colblk: unexpected data-block column count",
            ));
        }
        Ok(DataBlockReader { block, header })
    }

    /// The number of rows in the block.
    pub fn rows(&self) -> usize {
        self.header.rows as usize
    }

    /// Decodes all rows as `(user_key, trailer, value)`.
    pub fn decode_all(&self) -> Result<Vec<DataBlockRow>> {
        let rows = self.rows();
        let key_off = self.header.columns[COL_USER_KEY].page_offset as usize;
        let trailer_off = self.header.columns[COL_TRAILER].page_offset as usize;
        let value_off = self.header.columns[COL_VALUE].page_offset as usize;

        let (keys, _) = decode_raw_bytes(self.block, key_off, rows)?;
        let (trailers, _) = decode_uint_column(self.block, trailer_off, rows)?;
        let (values, _) = decode_raw_bytes(self.block, value_off, rows)?;

        Ok((0..rows)
            .map(|i| (keys[i].to_vec(), trailers[i], values[i].to_vec()))
            .collect())
    }
}

// ---------------------------------------------------------------------------
// Columnar index block
//
// Maps each data block's separator key to its block handle. Columns: separator key (raw
// bytes), block offset (uint), block length (uint).
// ---------------------------------------------------------------------------

const INDEX_BLOCK_COLUMNS: usize = 3;
const IDX_COL_SEPARATOR: usize = 0;
const IDX_COL_OFFSET: usize = 1;
const IDX_COL_LENGTH: usize = 2;

/// One index entry: a separator key and the `(offset, length)` of the block it points at.
pub type IndexEntry = (Vec<u8>, u64, u64);

/// Builds a columnar index block.
#[derive(Default)]
pub struct IndexBlockBuilder {
    separators: Vec<Vec<u8>>,
    offsets: Vec<u64>,
    lengths: Vec<u64>,
}

impl IndexBlockBuilder {
    /// Creates an empty index-block builder.
    pub fn new() -> IndexBlockBuilder {
        IndexBlockBuilder::default()
    }

    /// Adds an entry mapping `separator` to the block at `(offset, length)`.
    pub fn add(&mut self, separator: &[u8], offset: u64, length: u64) {
        self.separators.push(separator.to_vec());
        self.offsets.push(offset);
        self.lengths.push(length);
    }

    /// Serializes the index block.
    pub fn finish(&self) -> Vec<u8> {
        let rows = self.offsets.len();
        let mut buf = Vec::new();
        buf.push(DATA_BLOCK_VERSION);
        buf.extend_from_slice(&(INDEX_BLOCK_COLUMNS as u16).to_le_bytes());
        buf.extend_from_slice(&(rows as u32).to_le_bytes());
        let headers_at = buf.len();
        buf.resize(headers_at + INDEX_BLOCK_COLUMNS * COLUMN_HEADER_LEN, 0);

        let sep_refs: Vec<&[u8]> = self.separators.iter().map(|s| s.as_slice()).collect();
        let sep_off = buf.len();
        encode_raw_bytes(&sep_refs, sep_off, &mut buf);
        let off_off = buf.len();
        encode_uint_column(&self.offsets, off_off, &mut buf);
        let len_off = buf.len();
        encode_uint_column(&self.lengths, len_off, &mut buf);
        buf.push(0); // trailing padding

        for (i, (ty, off)) in [
            (3u8, sep_off), // Bytes
            (2u8, off_off), // Uint
            (2u8, len_off), // Uint
        ]
        .iter()
        .enumerate()
        {
            let h = headers_at + i * COLUMN_HEADER_LEN;
            buf[h] = *ty;
            buf[h + 1..h + 5].copy_from_slice(&(*off as u32).to_le_bytes());
        }
        buf
    }
}

/// Reads a columnar index block, returning its entries.
pub fn decode_index_block(block: &[u8]) -> Result<Vec<IndexEntry>> {
    let header = BlockHeader::parse(block)?;
    if header.columns.len() != INDEX_BLOCK_COLUMNS {
        return Err(Error::corruption(
            "colblk: unexpected index-block column count",
        ));
    }
    let rows = header.rows as usize;
    let (seps, _) = decode_raw_bytes(
        block,
        header.columns[IDX_COL_SEPARATOR].page_offset as usize,
        rows,
    )?;
    let (offsets, _) = decode_uint_column(
        block,
        header.columns[IDX_COL_OFFSET].page_offset as usize,
        rows,
    )?;
    let (lengths, _) = decode_uint_column(
        block,
        header.columns[IDX_COL_LENGTH].page_offset as usize,
        rows,
    )?;
    Ok((0..rows)
        .map(|i| (seps[i].to_vec(), offsets[i], lengths[i]))
        .collect())
}

// ---------------------------------------------------------------------------
// Columnar keyspan block
//
// Encodes fragmented key spans (range deletions / range keys). Columns: start user key
// (raw bytes), end user key (raw bytes), trailer (uint), value (raw bytes).
// ---------------------------------------------------------------------------

const KEYSPAN_BLOCK_COLUMNS: usize = 4;

/// One key span: `(start, end, trailer, value)`.
pub type KeyspanEntry = (Vec<u8>, Vec<u8>, u64, Vec<u8>);

/// Builds a columnar keyspan block.
#[derive(Default)]
pub struct KeyspanBlockBuilder {
    starts: Vec<Vec<u8>>,
    ends: Vec<Vec<u8>>,
    trailers: Vec<u64>,
    values: Vec<Vec<u8>>,
}

impl KeyspanBlockBuilder {
    /// Creates an empty keyspan-block builder.
    pub fn new() -> KeyspanBlockBuilder {
        KeyspanBlockBuilder::default()
    }

    /// Adds a fragmented span `[start, end)` with the given trailer and value.
    pub fn add(&mut self, start: &[u8], end: &[u8], trailer: u64, value: &[u8]) {
        self.starts.push(start.to_vec());
        self.ends.push(end.to_vec());
        self.trailers.push(trailer);
        self.values.push(value.to_vec());
    }

    /// Serializes the keyspan block.
    pub fn finish(&self) -> Vec<u8> {
        let rows = self.trailers.len();
        let mut buf = Vec::new();
        buf.push(DATA_BLOCK_VERSION);
        buf.extend_from_slice(&(KEYSPAN_BLOCK_COLUMNS as u16).to_le_bytes());
        buf.extend_from_slice(&(rows as u32).to_le_bytes());
        let headers_at = buf.len();
        buf.resize(headers_at + KEYSPAN_BLOCK_COLUMNS * COLUMN_HEADER_LEN, 0);

        let start_refs: Vec<&[u8]> = self.starts.iter().map(|s| s.as_slice()).collect();
        let end_refs: Vec<&[u8]> = self.ends.iter().map(|s| s.as_slice()).collect();
        let val_refs: Vec<&[u8]> = self.values.iter().map(|s| s.as_slice()).collect();

        let start_off = buf.len();
        encode_raw_bytes(&start_refs, start_off, &mut buf);
        let end_off = buf.len();
        encode_raw_bytes(&end_refs, end_off, &mut buf);
        let trailer_off = buf.len();
        encode_uint_column(&self.trailers, trailer_off, &mut buf);
        let value_off = buf.len();
        encode_raw_bytes(&val_refs, value_off, &mut buf);
        buf.push(0);

        for (i, (ty, off)) in [
            (3u8, start_off),
            (3u8, end_off),
            (2u8, trailer_off),
            (3u8, value_off),
        ]
        .iter()
        .enumerate()
        {
            let h = headers_at + i * COLUMN_HEADER_LEN;
            buf[h] = *ty;
            buf[h + 1..h + 5].copy_from_slice(&(*off as u32).to_le_bytes());
        }
        buf
    }
}

/// Reads a columnar keyspan block, returning its spans.
pub fn decode_keyspan_block(block: &[u8]) -> Result<Vec<KeyspanEntry>> {
    let header = BlockHeader::parse(block)?;
    if header.columns.len() != KEYSPAN_BLOCK_COLUMNS {
        return Err(Error::corruption(
            "colblk: unexpected keyspan-block column count",
        ));
    }
    let rows = header.rows as usize;
    let (starts, _) = decode_raw_bytes(block, header.columns[0].page_offset as usize, rows)?;
    let (ends, _) = decode_raw_bytes(block, header.columns[1].page_offset as usize, rows)?;
    let (trailers, _) = decode_uint_column(block, header.columns[2].page_offset as usize, rows)?;
    let (values, _) = decode_raw_bytes(block, header.columns[3].page_offset as usize, rows)?;
    Ok((0..rows)
        .map(|i| {
            (
                starts[i].to_vec(),
                ends[i].to_vec(),
                trailers[i],
                values[i].to_vec(),
            )
        })
        .collect())
}

#[cfg(test)]
mod block_tests {
    use super::*;

    #[test]
    fn index_block_roundtrips() {
        let mut b = IndexBlockBuilder::new();
        b.add(b"apple", 0, 4096);
        b.add(b"mango", 4096, 8192);
        b.add(b"zebra", 12288, 1000);
        let block = b.finish();
        let got = decode_index_block(&block).unwrap();
        assert_eq!(
            got,
            vec![
                (b"apple".to_vec(), 0, 4096),
                (b"mango".to_vec(), 4096, 8192),
                (b"zebra".to_vec(), 12288, 1000),
            ]
        );
    }

    #[test]
    fn keyspan_block_roundtrips() {
        let mut b = KeyspanBlockBuilder::new();
        b.add(b"a", b"e", 0xff00, b"");
        b.add(b"f", b"g", 0x1234, b"payload");
        let block = b.finish();
        let got = decode_keyspan_block(&block).unwrap();
        assert_eq!(
            got,
            vec![
                (b"a".to_vec(), b"e".to_vec(), 0xff00, b"".to_vec()),
                (b"f".to_vec(), b"g".to_vec(), 0x1234, b"payload".to_vec()),
            ]
        );
    }
}

#[cfg(test)]
mod data_block_tests {
    use super::*;

    #[test]
    fn data_block_roundtrips() {
        let mut b = DataBlockBuilder::new();
        let rows = [
            (&b"apple"[..], 0x0102u64, &b"red"[..]),
            (b"banana", 0x0203, b""),
            (b"cherry", 0xdead_beef, b"a longer value here"),
        ];
        for (k, t, v) in rows {
            b.add(k, t, v);
        }
        let block = b.finish();
        let r = DataBlockReader::new(&block).unwrap();
        assert_eq!(r.rows(), 3);
        let got = r.decode_all().unwrap();
        for (i, (k, t, v)) in rows.iter().enumerate() {
            assert_eq!(got[i].0, *k);
            assert_eq!(got[i].1, *t);
            assert_eq!(got[i].2, *v);
        }
    }

    #[test]
    fn empty_data_block_roundtrips() {
        let block = DataBlockBuilder::new().finish();
        let r = DataBlockReader::new(&block).unwrap();
        assert_eq!(r.rows(), 0);
        assert!(r.decode_all().unwrap().is_empty());
    }
}

#[cfg(test)]
mod codec_tests {
    use super::*;

    #[test]
    fn uint_column_roundtrips_all_widths() {
        for values in [
            vec![],
            vec![0u64; 5],           // all zero -> width 0
            vec![7u64; 4],           // all equal -> const (delta width 0)
            vec![1, 2, 3, 250],      // 1 byte
            vec![1, 2, 65000],       // 2 bytes
            vec![1, 100, 5_000_000], // 4 bytes
            vec![1, u64::MAX],       // 8 bytes
            vec![1000, 1001, 1002],  // delta from a base
        ] {
            let mut buf = Vec::new();
            let end = encode_uint_column(&values, 0, &mut buf);
            assert_eq!(end, buf.len());
            let (got, off) = decode_uint_column(&buf, 0, values.len()).unwrap();
            assert_eq!(got, values, "roundtrip {values:?}");
            assert_eq!(off, end);
        }
    }

    #[test]
    fn raw_bytes_column_roundtrips() {
        let slices: Vec<&[u8]> = vec![b"apple", b"", b"banana", b"c"];
        let mut buf = Vec::new();
        let end = encode_raw_bytes(&slices, 0, &mut buf);
        let (got, off) = decode_raw_bytes(&buf, 0, slices.len()).unwrap();
        assert_eq!(got, slices);
        assert_eq!(off, end);
    }

    #[test]
    fn prefix_bytes_roundtrips() {
        // The 15-key example from Pebble's prefix_bytes.go doc, plus simpler cases.
        let example: Vec<&[u8]> = vec![
            b"aaabbbc",
            b"aaabbbcc",
            b"aaabbbcde",
            b"aaabbbce",
            b"aaabbbdee",
            b"aaabbbdee",
            b"aaabbbdee",
            b"aaabbbeff",
            b"aaabbe",
            b"aaabbeef",
            b"aaabbeef",
            b"aaabc",
            b"aabcceef",
            b"aabcceef",
            b"aabcceef",
        ];
        for (keys, bundle) in [
            (vec![], 4usize),
            (vec![&b"only"[..]], 4),
            (vec![&b"a"[..], b"b", b"c"], 2),
            (example.clone(), 4),
            (example.clone(), 1),
            (example, 16),
        ] {
            let mut buf = Vec::new();
            let end = encode_prefix_bytes(&keys, bundle, 0, &mut buf);
            let (got, off) = decode_prefix_bytes(&buf, 0, keys.len()).unwrap();
            let want: Vec<Vec<u8>> = keys.iter().map(|k| k.to_vec()).collect();
            assert_eq!(got, want, "bundle={bundle}");
            assert_eq!(off, end);
        }
    }

    #[test]
    fn bitmap_column_roundtrips() {
        for bits in [
            vec![false; 10],
            vec![true, false, true, true, false],
            (0..200).map(|i| i % 3 == 0).collect::<Vec<_>>(),
        ] {
            let mut buf = Vec::new();
            let end = encode_bitmap(&bits, 0, &mut buf);
            let (got, off) = decode_bitmap(&buf, 0, bits.len()).unwrap();
            assert_eq!(got, bits);
            assert_eq!(off, end);
        }
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
