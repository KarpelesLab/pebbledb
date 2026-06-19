// Copyright (c) 2011 The LevelDB-Go Authors. All rights reserved.
// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Ported from Pebble's sstable/writer.go, block/blockenc.go, and rowblk.

//! Writing sorted-string tables.
//!
//! [`Writer`] consumes internal key/value pairs in strictly increasing internal-key
//! order, packs them into prefix-compressed data blocks (see [`super::block`]), records
//! a separator and handle for each block in the index, and finishes with a metaindex
//! block and the footer. The output is readable by [`super::Reader`] and, for the
//! supported formats, by Pebble/RocksDB.
//!
//! Scope: row-based data/index blocks, single-level index, CRC32C checksums, in-place
//! values, no filter or properties blocks yet. The default format is
//! [`TableFormat::Pebble(2)`](super::TableFormat::Pebble).

use std::io::Write;
use std::sync::Arc;

use crate::base::comparer::Comparer;
use crate::base::internal_key::compare_encoded;
use crate::base::varint::put_uvarint;
use crate::crc::{Crc32c, mask};
use crate::{Error, Result};

use super::block::{BlockHandle, ChecksumType, CompressionType, TRAILER_LEN};
use super::{
    LEVELDB_MAGIC, MAGIC_LEN, PEBBLE_MAGIC, ROCKSDB_FOOTER_LEN, ROCKSDB_MAGIC, TableFormat,
    VERSION_LEN,
};

/// The longest a block handle can be when encoded (two 10-byte varints).
const BLOCK_HANDLE_MAX_LEN: usize = 20;

/// Options controlling how an sstable is written.
#[derive(Clone, Debug)]
pub struct WriterOptions {
    /// Target uncompressed size of a data block before it is flushed (default 4096).
    pub block_size: usize,
    /// Number of entries between restart points (default 16).
    pub block_restart_interval: usize,
    /// Block compression (default [`CompressionType::Snappy`]).
    pub compression: CompressionType,
    /// Block checksum algorithm (default [`ChecksumType::Crc32c`]).
    pub checksum: ChecksumType,
    /// The table format to emit (default `Pebble(2)`).
    pub table_format: TableFormat,
}

impl Default for WriterOptions {
    fn default() -> Self {
        WriterOptions {
            block_size: 4096,
            block_restart_interval: 16,
            compression: CompressionType::Snappy,
            checksum: ChecksumType::Crc32c,
            table_format: TableFormat::Pebble(2),
        }
    }
}

/// The length of the common prefix of `a` and `b`.
fn common_prefix(a: &[u8], b: &[u8]) -> usize {
    let n = a.len().min(b.len());
    let mut i = 0;
    while i < n && a[i] == b[i] {
        i += 1;
    }
    i
}

/// Accumulates prefix-compressed entries into a single block.
struct BlockBuilder {
    buf: Vec<u8>,
    restarts: Vec<u32>,
    restart_interval: usize,
    counter: usize,
    last_key: Vec<u8>,
}

impl BlockBuilder {
    fn new(restart_interval: usize) -> BlockBuilder {
        BlockBuilder {
            buf: Vec::new(),
            restarts: Vec::new(),
            restart_interval: restart_interval.max(1),
            counter: 0,
            last_key: Vec::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Estimated size of the finished block in bytes.
    fn size_estimate(&self) -> usize {
        self.buf.len() + (self.restarts.len() + 1) * 4
    }

    fn add(&mut self, key: &[u8], value: &[u8]) {
        let shared = if self.counter == 0 {
            self.restarts.push(self.buf.len() as u32);
            0
        } else {
            common_prefix(&self.last_key, key)
        };
        put_uvarint(&mut self.buf, shared as u64);
        put_uvarint(&mut self.buf, (key.len() - shared) as u64);
        put_uvarint(&mut self.buf, value.len() as u64);
        self.buf.extend_from_slice(&key[shared..]);
        self.buf.extend_from_slice(value);

        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.counter += 1;
        if self.counter == self.restart_interval {
            self.counter = 0;
        }
    }

    /// Appends the restart array and returns the finished block bytes.
    fn finish(&mut self) -> &[u8] {
        if self.restarts.is_empty() {
            self.restarts.push(0);
        }
        for r in &self.restarts {
            self.buf.extend_from_slice(&r.to_le_bytes());
        }
        self.buf
            .extend_from_slice(&(self.restarts.len() as u32).to_le_bytes());
        &self.buf
    }

    fn reset(&mut self) {
        self.buf.clear();
        self.restarts.clear();
        self.counter = 0;
        self.last_key.clear();
    }
}

/// Writes an sstable to an underlying byte sink.
pub struct Writer<W: Write> {
    w: W,
    opts: WriterOptions,
    cmp: Arc<dyn Comparer>,
    offset: u64,
    data_block: BlockBuilder,
    index_block: BlockBuilder,
    /// The last internal key added, used to enforce ordering and as a block separator.
    last_key: Vec<u8>,
    num_entries: u64,
    finished: bool,
}

impl<W: Write> Writer<W> {
    /// Creates a writer over `w` using `opts` and the user-key comparer `cmp`.
    pub fn new(w: W, cmp: Arc<dyn Comparer>, opts: WriterOptions) -> Writer<W> {
        let ri = opts.block_restart_interval;
        Writer {
            w,
            opts,
            cmp,
            offset: 0,
            data_block: BlockBuilder::new(ri),
            index_block: BlockBuilder::new(ri),
            last_key: Vec::new(),
            num_entries: 0,
            finished: false,
        }
    }

    /// Adds an entry. `internal_key` is an encoded internal key and must be strictly
    /// greater (in internal-key order) than every previously added key.
    pub fn add(&mut self, internal_key: &[u8], value: &[u8]) -> Result<()> {
        if !self.last_key.is_empty()
            && compare_encoded(self.cmp.as_ref(), &self.last_key, internal_key)
                != std::cmp::Ordering::Less
        {
            return Err(Error::InvalidState(
                "sstable: keys must be added in strictly increasing order".into(),
            ));
        }
        self.data_block.add(internal_key, value);
        self.last_key.clear();
        self.last_key.extend_from_slice(internal_key);
        self.num_entries += 1;

        if self.data_block.size_estimate() >= self.opts.block_size {
            self.flush_data_block()?;
        }
        Ok(())
    }

    /// Flushes the current data block (if non-empty) and records its index entry. The
    /// separator is the block's last key, which is `>=` every key in the block and `<`
    /// the first key of the next block.
    fn flush_data_block(&mut self) -> Result<()> {
        if self.data_block.is_empty() {
            return Ok(());
        }
        let handle = {
            let raw = self.data_block.finish();
            write_block(&mut self.w, &mut self.offset, raw, &self.opts)?
        };
        self.data_block.reset();

        let mut handle_enc = Vec::with_capacity(BLOCK_HANDLE_MAX_LEN);
        handle.encode_to(&mut handle_enc);
        // last_key is the separator for the just-flushed block.
        let sep = self.last_key.clone();
        self.index_block.add(&sep, &handle_enc);
        Ok(())
    }

    /// Finishes the table: flushes the final data block, writes the index and metaindex
    /// blocks and the footer, and returns the inner writer.
    pub fn finish(mut self) -> Result<W> {
        self.flush_data_block()?;

        // Metaindex block (currently empty: no filter/properties blocks yet).
        let mut metaindex = BlockBuilder::new(self.opts.block_restart_interval);
        let metaindex_handle = write_block(
            &mut self.w,
            &mut self.offset,
            metaindex.finish(),
            &self.opts,
        )?;

        // Index block.
        let index_handle = {
            let raw = self.index_block.finish();
            write_block(&mut self.w, &mut self.offset, raw, &self.opts)?
        };

        let footer = encode_footer(
            self.opts.table_format,
            self.opts.checksum,
            metaindex_handle,
            index_handle,
        )?;
        self.w.write_all(&footer)?;
        self.finished = true;
        Ok(self.w)
    }

    /// The number of entries added so far.
    pub fn num_entries(&self) -> u64 {
        self.num_entries
    }
}

/// Compresses `raw`, appends the trailer (compression byte + checksum), writes the whole
/// block to `w`, advances `*offset`, and returns the block's handle.
fn write_block(
    w: &mut impl Write,
    offset: &mut u64,
    raw: &[u8],
    opts: &WriterOptions,
) -> Result<BlockHandle> {
    let (type_byte, body) = compress(raw, opts.compression)?;

    let handle = BlockHandle {
        offset: *offset,
        length: body.len() as u64,
    };
    w.write_all(&body)?;
    w.write_all(&[type_byte])?;

    let checksum = match opts.checksum {
        ChecksumType::Crc32c => mask(Crc32c::new().update(&body).update(&[type_byte]).finish()),
        ChecksumType::None => 0,
        ChecksumType::XxHash | ChecksumType::XxHash64 => {
            return Err(Error::Unsupported("sstable: xxhash block checksum"));
        }
    };
    w.write_all(&checksum.to_le_bytes())?;

    *offset += body.len() as u64 + TRAILER_LEN as u64;
    Ok(handle)
}

/// Returns the compression-type byte and the (possibly compressed) block body. Falls
/// back to no compression if compression would not shrink the block.
fn compress(raw: &[u8], compression: CompressionType) -> Result<(u8, Vec<u8>)> {
    match compression {
        CompressionType::None => Ok((CompressionType::None.as_u8(), raw.to_vec())),
        CompressionType::Snappy => {
            let c = compcol::vec::compress_to_vec::<compcol::snappy::Snappy>(raw)
                .map_err(|e| Error::Corruption(format!("sstable: snappy encode: {e:?}")))?;
            if c.len() < raw.len() {
                Ok((CompressionType::Snappy.as_u8(), c))
            } else {
                Ok((CompressionType::None.as_u8(), raw.to_vec()))
            }
        }
        CompressionType::Zstd => {
            let mut body = Vec::new();
            put_uvarint(&mut body, raw.len() as u64);
            let c = compcol::vec::compress_to_vec::<compcol::zstd::Zstd>(raw)
                .map_err(|e| Error::Corruption(format!("sstable: zstd encode: {e:?}")))?;
            body.extend_from_slice(&c);
            if body.len() < raw.len() {
                Ok((CompressionType::Zstd.as_u8(), body))
            } else {
                Ok((CompressionType::None.as_u8(), raw.to_vec()))
            }
        }
    }
}

/// Encodes the footer for the given format (LevelDB 48-byte or RocksDB/Pebble 53-byte).
fn encode_footer(
    format: TableFormat,
    checksum: ChecksumType,
    metaindex: BlockHandle,
    index: BlockHandle,
) -> Result<Vec<u8>> {
    match format {
        TableFormat::LevelDB => {
            // [metaindex handle][index handle][padding][magic:8]
            let mut buf = Vec::with_capacity(super::LEVELDB_FOOTER_LEN);
            metaindex.encode_to(&mut buf);
            index.encode_to(&mut buf);
            buf.resize(2 * BLOCK_HANDLE_MAX_LEN, 0);
            buf.extend_from_slice(LEVELDB_MAGIC);
            Ok(buf)
        }
        TableFormat::RocksDBv2 | TableFormat::Pebble(_) => {
            let (magic, version): (&[u8; 8], u32) = match format {
                TableFormat::RocksDBv2 => (ROCKSDB_MAGIC, 2),
                TableFormat::Pebble(v) => {
                    if v > 5 {
                        return Err(Error::Unsupported(
                            "sstable: writing Pebblev6+ footer not supported",
                        ));
                    }
                    (PEBBLE_MAGIC, u32::from(v))
                }
                TableFormat::LevelDB => unreachable!(),
            };
            // [checksum:1][metaindex handle][index handle][padding][version:4][magic:8]
            let mut buf = Vec::with_capacity(ROCKSDB_FOOTER_LEN);
            buf.push(match checksum {
                ChecksumType::None => 0,
                ChecksumType::Crc32c => 1,
                ChecksumType::XxHash => 2,
                ChecksumType::XxHash64 => 3,
            });
            metaindex.encode_to(&mut buf);
            index.encode_to(&mut buf);
            buf.resize(1 + 2 * BLOCK_HANDLE_MAX_LEN, 0);
            buf.extend_from_slice(&version.to_le_bytes());
            buf.extend_from_slice(magic);
            debug_assert_eq!(buf.len(), ROCKSDB_FOOTER_LEN);
            debug_assert_eq!(MAGIC_LEN + VERSION_LEN, 12);
            Ok(buf)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::comparer::DefaultComparer;
    use crate::base::internal_key::{InternalKey, InternalKeyKind};
    use crate::sstable::Reader;

    fn ikey(user: &[u8], seq: u64, kind: InternalKeyKind) -> Vec<u8> {
        InternalKey::new(user.to_vec(), seq, kind).encode()
    }

    fn build(entries: &[(Vec<u8>, Vec<u8>)], opts: WriterOptions) -> Vec<u8> {
        let cmp: Arc<dyn Comparer> = Arc::new(DefaultComparer);
        let mut w = Writer::new(Vec::new(), cmp, opts);
        for (k, v) in entries {
            w.add(k, v).unwrap();
        }
        w.finish().unwrap()
    }

    fn roundtrip_with(compression: CompressionType, block_size: usize, format: TableFormat) {
        let cmp: Arc<dyn Comparer> = Arc::new(DefaultComparer);
        // Many keys with shared prefixes, forcing multiple data blocks at small sizes.
        let mut entries = Vec::new();
        for i in 0..500u32 {
            let user = format!("key/{i:08}");
            entries.push((
                ikey(user.as_bytes(), 100 + i as u64, InternalKeyKind::Set),
                format!("value-{i}").into_bytes(),
            ));
        }
        let opts = WriterOptions {
            block_size,
            compression,
            table_format: format,
            ..Default::default()
        };
        let file = build(&entries, opts);

        let reader = Arc::new(Reader::open(file, cmp).unwrap());
        assert_eq!(reader.format(), format);

        // Point lookups for every key.
        for i in 0..500u32 {
            let user = format!("key/{i:08}");
            let got = reader.get(user.as_bytes(), 10_000).unwrap();
            assert_eq!(
                got,
                Some((InternalKeyKind::Set, format!("value-{i}").into_bytes())),
                "lookup {user}"
            );
        }
        // Missing key.
        assert_eq!(reader.get(b"key/99999999", 10_000).unwrap(), None);

        // Full ordered iteration returns all entries in order.
        let mut it = reader.iter().unwrap();
        let mut count = 0;
        let mut ok = it.first().unwrap();
        let mut prev: Option<Vec<u8>> = None;
        while ok {
            let k = it.key().to_vec();
            if let Some(p) = &prev {
                assert!(compare_encoded(&DefaultComparer, p, &k) == std::cmp::Ordering::Less);
            }
            prev = Some(k);
            count += 1;
            ok = it.next().unwrap();
        }
        assert_eq!(count, 500);
    }

    #[test]
    fn roundtrip_uncompressed_single_block() {
        roundtrip_with(CompressionType::None, 1 << 20, TableFormat::Pebble(2));
    }

    #[test]
    fn roundtrip_uncompressed_many_blocks() {
        roundtrip_with(CompressionType::None, 256, TableFormat::Pebble(2));
    }

    #[test]
    fn roundtrip_snappy_many_blocks() {
        roundtrip_with(CompressionType::Snappy, 256, TableFormat::Pebble(2));
    }

    #[test]
    fn roundtrip_zstd_many_blocks() {
        roundtrip_with(CompressionType::Zstd, 256, TableFormat::Pebble(2));
    }

    #[test]
    fn roundtrip_rocksdb_format() {
        roundtrip_with(CompressionType::Snappy, 512, TableFormat::RocksDBv2);
    }

    #[test]
    fn out_of_order_add_is_rejected() {
        let cmp: Arc<dyn Comparer> = Arc::new(DefaultComparer);
        let mut w = Writer::new(Vec::new(), cmp, WriterOptions::default());
        w.add(&ikey(b"b", 1, InternalKeyKind::Set), b"1").unwrap();
        assert!(w.add(&ikey(b"a", 1, InternalKeyKind::Set), b"2").is_err());
    }

    #[test]
    fn get_respects_tombstones() {
        let cmp: Arc<dyn Comparer> = Arc::new(DefaultComparer);
        // Two versions of "k": newer is a Delete.
        let entries = vec![
            (ikey(b"k", 20, InternalKeyKind::Delete), Vec::new()),
            (ikey(b"k", 10, InternalKeyKind::Set), b"old".to_vec()),
        ];
        let file = build(&entries, WriterOptions::default());
        let reader = Reader::open(file, cmp).unwrap();
        // At snapshot 25, the newest entry is the tombstone.
        assert_eq!(
            reader.get(b"k", 25).unwrap(),
            Some((InternalKeyKind::Delete, Vec::new()))
        );
        // At snapshot 15, the value is visible.
        assert_eq!(
            reader.get(b"k", 15).unwrap(),
            Some((InternalKeyKind::Set, b"old".to_vec()))
        );
    }
}
