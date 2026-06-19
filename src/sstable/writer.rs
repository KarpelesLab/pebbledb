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
//! Scope: row-based data/index blocks with single- or two-level indexes, CRC32C or
//! xxHash64 checksums, in-place values, a bloom filter, and a properties block. The
//! default format is [`TableFormat::Pebble(2)`](super::TableFormat::Pebble).

use std::io::Write;
use std::sync::Arc;

use crate::base::comparer::Comparer;
use crate::base::internal_key::compare_encoded;
use crate::base::varint::put_uvarint;
use crate::crc::{Crc32c, mask};
use crate::{Error, Result};

use super::block::{BlockHandle, ChecksumType, CompressionType, TRAILER_LEN};
use super::properties::{
    BINARY_SEARCH_INDEX, META_PROPERTIES_NAME, META_RANGE_DEL_NAME, Properties, TWO_LEVEL_INDEX,
};
use super::{
    LEVELDB_MAGIC, MAGIC_LEN, PEBBLE_MAGIC, ROCKSDB_FOOTER_LEN, ROCKSDB_MAGIC, TableFormat,
    VERSION_LEN,
};

/// The RocksDB compression-name string recorded in the properties block.
fn compression_name(c: CompressionType) -> &'static str {
    match c {
        CompressionType::None => "NoCompression",
        CompressionType::Snappy => "Snappy",
        CompressionType::Zstd => "ZSTD",
    }
}

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
    /// Bloom filter bits-per-key, or `None` to omit the filter (default `Some(10)`).
    pub filter_policy: Option<u32>,
    /// Target size of a lower-level index block (index partition) before a new one is
    /// started; once more than one partition is produced the table uses a two-level
    /// index (default 256 KiB).
    pub index_block_size: usize,
}

impl Default for WriterOptions {
    fn default() -> Self {
        WriterOptions {
            block_size: 4096,
            block_restart_interval: 16,
            compression: CompressionType::Snappy,
            checksum: ChecksumType::Crc32c,
            table_format: TableFormat::Pebble(2),
            filter_policy: Some(10),
            index_block_size: 256 << 10,
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
    /// Range-deletion entries (start internal key -> end user key), written as a
    /// separate block referenced from the metaindex.
    range_del_block: BlockBuilder,
    /// The current lower-level index block (index partition).
    index_partition: BlockBuilder,
    /// The top-level index: separator -> index-partition handle.
    top_level_index: BlockBuilder,
    /// The separator most recently added to the current index partition (its last key).
    last_index_sep: Vec<u8>,
    /// Number of index partitions flushed so far.
    partition_count: u64,
    /// Handle of the first (and possibly only) index partition.
    first_partition_handle: Option<BlockHandle>,
    /// Total size of all index partitions.
    index_size: u64,
    /// The last point internal key added, used to enforce ordering and as a block
    /// separator.
    last_key: Vec<u8>,
    /// The last range-deletion internal key added, used to enforce ordering.
    last_range_del_key: Vec<u8>,
    num_entries: u64,
    num_deletions: u64,
    num_range_deletions: u64,
    raw_key_size: u64,
    raw_value_size: u64,
    data_size: u64,
    num_data_blocks: u64,
    filter: Option<super::filter::FilterWriter>,
    finished: bool,
}

impl<W: Write> Writer<W> {
    /// Creates a writer over `w` using `opts` and the user-key comparer `cmp`.
    pub fn new(w: W, cmp: Arc<dyn Comparer>, opts: WriterOptions) -> Writer<W> {
        let ri = opts.block_restart_interval;
        let filter = opts.filter_policy.map(super::filter::FilterWriter::new);
        Writer {
            w,
            opts,
            cmp,
            offset: 0,
            data_block: BlockBuilder::new(ri),
            range_del_block: BlockBuilder::new(ri),
            index_partition: BlockBuilder::new(ri),
            top_level_index: BlockBuilder::new(ri),
            last_index_sep: Vec::new(),
            partition_count: 0,
            first_partition_handle: None,
            index_size: 0,
            last_key: Vec::new(),
            last_range_del_key: Vec::new(),
            num_entries: 0,
            num_deletions: 0,
            num_range_deletions: 0,
            raw_key_size: 0,
            raw_value_size: 0,
            data_size: 0,
            num_data_blocks: 0,
            filter,
            finished: false,
        }
    }

    /// Adds an entry. `internal_key` is an encoded internal key and must be strictly
    /// greater (in internal-key order) than every previously added key.
    ///
    /// A [`RangeDelete`](crate::base::internal_key::InternalKeyKind::RangeDelete) entry
    /// (`internal_key` = `(start, seq, RangeDelete)`, `value` = end user key) is routed
    /// to the range-deletion block rather than a data block.
    pub fn add(&mut self, internal_key: &[u8], value: &[u8]) -> Result<()> {
        use crate::base::internal_key::InternalKeyKind;
        let kind = crate::base::internal_key::trailer_kind(
            crate::base::internal_key::encoded_trailer(internal_key),
        );

        // Point keys and range deletions are independent sorted streams (range dels go
        // in their own block), so each enforces its own increasing-key order.
        if kind == InternalKeyKind::RangeDelete {
            if !self.last_range_del_key.is_empty()
                && compare_encoded(self.cmp.as_ref(), &self.last_range_del_key, internal_key)
                    != std::cmp::Ordering::Less
            {
                return Err(Error::InvalidState(
                    "sstable: range-del keys must be added in increasing order".into(),
                ));
            }
            self.last_range_del_key.clear();
            self.last_range_del_key.extend_from_slice(internal_key);
            self.range_del_block.add(internal_key, value);
            self.num_range_deletions += 1;
            self.num_deletions += 1;
            return Ok(());
        }

        if !self.last_key.is_empty()
            && compare_encoded(self.cmp.as_ref(), &self.last_key, internal_key)
                != std::cmp::Ordering::Less
        {
            return Err(Error::InvalidState(
                "sstable: keys must be added in strictly increasing order".into(),
            ));
        }
        self.last_key.clear();
        self.last_key.extend_from_slice(internal_key);

        self.data_block.add(internal_key, value);
        self.num_entries += 1;
        self.raw_key_size += internal_key.len() as u64;
        self.raw_value_size += value.len() as u64;
        if matches!(
            kind,
            InternalKeyKind::Delete | InternalKeyKind::SingleDelete | InternalKeyKind::DeleteSized
        ) {
            self.num_deletions += 1;
        }
        if let Some(fw) = self.filter.as_mut() {
            fw.add_key(crate::base::internal_key::encoded_user_key(internal_key));
        }

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
            write_block(
                &mut self.w,
                &mut self.offset,
                raw,
                self.opts.compression,
                self.opts.checksum,
            )?
        };
        self.data_block.reset();
        self.data_size += handle.length;
        self.num_data_blocks += 1;

        let mut handle_enc = Vec::with_capacity(BLOCK_HANDLE_MAX_LEN);
        handle.encode_to(&mut handle_enc);
        // last_key is the separator for the just-flushed block; add it to the current
        // index partition and remember it as the partition's running last key.
        self.index_partition.add(&self.last_key, &handle_enc);
        self.last_index_sep.clear();
        self.last_index_sep.extend_from_slice(&self.last_key);

        if self.index_partition.size_estimate() >= self.opts.index_block_size {
            self.flush_index_partition()?;
        }
        Ok(())
    }

    /// Writes the current index partition (if non-empty) and records it in the top-level
    /// index, keyed by the partition's last separator.
    fn flush_index_partition(&mut self) -> Result<()> {
        if self.index_partition.is_empty() {
            return Ok(());
        }
        let handle = {
            let raw = self.index_partition.finish();
            write_block(
                &mut self.w,
                &mut self.offset,
                raw,
                self.opts.compression,
                self.opts.checksum,
            )?
        };
        self.index_partition.reset();
        self.index_size += handle.length;
        if self.first_partition_handle.is_none() {
            self.first_partition_handle = Some(handle);
        }
        self.partition_count += 1;

        let mut handle_enc = Vec::with_capacity(BLOCK_HANDLE_MAX_LEN);
        handle.encode_to(&mut handle_enc);
        let sep = self.last_index_sep.clone();
        self.top_level_index.add(&sep, &handle_enc);
        Ok(())
    }

    /// Finishes the table: flushes the final data block, then writes the index block, the
    /// properties block, the metaindex block, and the footer (the canonical Pebble /
    /// RocksDB order), and returns the inner writer.
    pub fn finish(mut self) -> Result<W> {
        self.flush_data_block()?;

        // Filter block (uncompressed), if a filter policy is configured and any keys
        // were added. Recorded in the metaindex under "fullfilter.<policy name>".
        let mut meta_entries: Vec<(String, BlockHandle)> = Vec::new();
        let mut filter_size = 0u64;
        let mut filter_policy_name = String::new();

        // Range-deletion block (compressed like data), referenced under
        // "rocksdb.range_del2".
        if !self.range_del_block.is_empty() {
            let raw = self.range_del_block.finish();
            let handle = write_block(
                &mut self.w,
                &mut self.offset,
                raw,
                self.opts.compression,
                self.opts.checksum,
            )?;
            meta_entries.push((META_RANGE_DEL_NAME.to_string(), handle));
        }
        if let Some(fw) = self.filter.as_ref()
            && let Some(filter_bytes) = fw.finish()
        {
            filter_policy_name = fw.policy_name();
            filter_size = filter_bytes.len() as u64;
            let handle = write_block(
                &mut self.w,
                &mut self.offset,
                &filter_bytes,
                CompressionType::None,
                self.opts.checksum,
            )?;
            meta_entries.push((super::filter::metaindex_key(&filter_policy_name), handle));
        }

        // Flush the final index partition, then decide single- vs two-level. A single
        // partition is used directly as the index; multiple partitions are summarized by
        // a top-level index block.
        self.flush_index_partition()?;
        let (index_handle, index_type, top_level_index_size) = if self.partition_count <= 1 {
            (
                self.first_partition_handle.unwrap_or_default(),
                BINARY_SEARCH_INDEX,
                0,
            )
        } else {
            let raw = self.top_level_index.finish();
            let top = write_block(
                &mut self.w,
                &mut self.offset,
                raw,
                self.opts.compression,
                self.opts.checksum,
            )?;
            (top, TWO_LEVEL_INDEX, top.length)
        };

        // Properties block (uncompressed meta block), referenced from the metaindex.
        let props = Properties {
            num_entries: self.num_entries,
            raw_key_size: self.raw_key_size,
            raw_value_size: self.raw_value_size,
            num_deletions: self.num_deletions,
            num_range_deletions: self.num_range_deletions,
            num_data_blocks: self.num_data_blocks,
            data_size: self.data_size,
            index_size: self.index_size,
            index_type,
            top_level_index_size,
            filter_size,
            comparer_name: self.cmp.name().to_string(),
            merger_name: "nullptr".to_string(),
            property_collectors: "[]".to_string(),
            compression_name: compression_name(self.opts.compression).to_string(),
            filter_policy: filter_policy_name,
            ..Default::default()
        };
        let props_handle = {
            // Meta blocks use a restart interval of 1 (no prefix compression).
            let mut pb = BlockBuilder::new(1);
            for (name, value) in props.encode() {
                pb.add(name.as_bytes(), &value);
            }
            write_block(
                &mut self.w,
                &mut self.offset,
                pb.finish(),
                CompressionType::None,
                self.opts.checksum,
            )?
        };
        meta_entries.push((META_PROPERTIES_NAME.to_string(), props_handle));

        // Metaindex block (uncompressed): meta-block name -> handle, sorted by name.
        meta_entries.sort_by(|a, b| a.0.cmp(&b.0));
        let metaindex_handle = {
            let mut mi = BlockBuilder::new(1);
            for (name, handle) in &meta_entries {
                let mut handle_enc = Vec::new();
                handle.encode_to(&mut handle_enc);
                mi.add(name.as_bytes(), &handle_enc);
            }
            write_block(
                &mut self.w,
                &mut self.offset,
                mi.finish(),
                CompressionType::None,
                self.opts.checksum,
            )?
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

    /// The number of bytes written to the underlying sink so far (excluding any data
    /// still buffered in the open data block). Useful for deciding when to split output
    /// files during compaction.
    pub fn estimated_size(&self) -> u64 {
        self.offset
    }
}

/// Compresses `raw` with `compression`, appends the trailer (compression byte +
/// `checksum`), writes the whole block to `w`, advances `*offset`, and returns the
/// block's handle.
fn write_block(
    w: &mut impl Write,
    offset: &mut u64,
    raw: &[u8],
    compression: CompressionType,
    checksum: ChecksumType,
) -> Result<BlockHandle> {
    let (type_byte, body) = compress(raw, compression)?;

    let handle = BlockHandle {
        offset: *offset,
        length: body.len() as u64,
    };
    w.write_all(&body)?;
    w.write_all(&[type_byte])?;

    let sum = match checksum {
        ChecksumType::Crc32c => mask(Crc32c::new().update(&body).update(&[type_byte]).finish()),
        ChecksumType::None => 0,
        ChecksumType::XxHash64 => {
            let mut h = crate::xxhash::XxHash64::new();
            h.update(&body);
            h.update(&[type_byte]);
            h.finish() as u32
        }
        ChecksumType::XxHash => {
            return Err(Error::Unsupported("sstable: xxhash32 block checksum"));
        }
    };
    w.write_all(&sum.to_le_bytes())?;

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
    fn roundtrip_xxhash64_checksum() {
        let cmp: Arc<dyn Comparer> = Arc::new(DefaultComparer);
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..200u32)
            .map(|i| {
                (
                    ikey(
                        format!("k{i:04}").as_bytes(),
                        i as u64 + 1,
                        InternalKeyKind::Set,
                    ),
                    format!("v{i}").into_bytes(),
                )
            })
            .collect();
        let opts = WriterOptions {
            block_size: 256,
            checksum: ChecksumType::XxHash64,
            ..Default::default()
        };
        let file = build(&entries, opts);
        let reader = Arc::new(Reader::open(file, cmp).unwrap());
        assert_eq!(reader.checksum_type(), ChecksumType::XxHash64);
        for i in 0..200u32 {
            assert_eq!(
                reader.get(format!("k{i:04}").as_bytes(), 10_000).unwrap(),
                Some((InternalKeyKind::Set, format!("v{i}").into_bytes()))
            );
        }
    }

    #[test]
    fn properties_block_roundtrips() {
        let cmp: Arc<dyn Comparer> = Arc::new(DefaultComparer);
        let entries: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (ikey(b"a", 5, InternalKeyKind::Set), b"v1".to_vec()),
            (ikey(b"b", 6, InternalKeyKind::Delete), Vec::new()),
            (ikey(b"c", 7, InternalKeyKind::Set), b"v3".to_vec()),
        ];
        let opts = WriterOptions {
            block_size: 1 << 20, // single data block
            compression: CompressionType::Snappy,
            ..Default::default()
        };
        let file = build(&entries, opts);
        let reader = Reader::open(file, cmp).unwrap();
        let p = reader.properties();
        assert_eq!(p.num_entries, 3);
        assert_eq!(p.num_deletions, 1);
        assert_eq!(p.num_data_blocks, 1);
        assert_eq!(p.comparer_name, "leveldb.BytewiseComparator");
        assert_eq!(p.merger_name, "nullptr");
        assert_eq!(p.compression_name, "Snappy");
        assert!(p.index_size > 0);
        assert!(p.raw_value_size >= 4); // "v1" + "v3"
    }

    #[test]
    fn bloom_filter_written_and_used() {
        let cmp: Arc<dyn Comparer> = Arc::new(DefaultComparer);
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..500u32)
            .map(|i| {
                (
                    ikey(
                        format!("present{i:04}").as_bytes(),
                        i as u64 + 1,
                        InternalKeyKind::Set,
                    ),
                    b"v".to_vec(),
                )
            })
            .collect();
        let file = build(
            &entries,
            WriterOptions {
                block_size: 256,
                ..Default::default()
            },
        );
        let reader = Reader::open(file, cmp).unwrap();
        assert_eq!(
            reader.properties().filter_policy,
            "rocksdb.BuiltinBloomFilter"
        );
        assert!(reader.properties().filter_size > 0);
        // Present keys are found; absent keys are (almost always) rejected.
        assert_eq!(
            reader.get(b"present0250", 10_000).unwrap(),
            Some((InternalKeyKind::Set, b"v".to_vec()))
        );
        assert_eq!(reader.get(b"absent9999", 10_000).unwrap(), None);

        // With the filter disabled the table is still correct.
        let nofilter = build(
            &entries,
            WriterOptions {
                block_size: 256,
                filter_policy: None,
                ..Default::default()
            },
        );
        let reader2 = Reader::open(nofilter, Arc::new(DefaultComparer)).unwrap();
        assert!(reader2.properties().filter_policy.is_empty());
        assert_eq!(
            reader2.get(b"present0001", 10_000).unwrap(),
            Some((InternalKeyKind::Set, b"v".to_vec()))
        );
    }

    #[test]
    fn two_level_index_roundtrip() {
        let cmp: Arc<dyn Comparer> = Arc::new(DefaultComparer);
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..1000u32)
            .map(|i| {
                (
                    ikey(
                        format!("k{i:06}").as_bytes(),
                        i as u64 + 1,
                        InternalKeyKind::Set,
                    ),
                    format!("value-{i}").into_bytes(),
                )
            })
            .collect();
        // Tiny data + index block sizes force many data blocks and several index
        // partitions, triggering a two-level index.
        let opts = WriterOptions {
            block_size: 128,
            index_block_size: 96,
            ..Default::default()
        };
        let file = build(&entries, opts);
        let reader = Arc::new(Reader::open(file, cmp).unwrap());
        assert!(
            reader.properties().is_two_level_index(),
            "expected a two-level index"
        );
        assert!(reader.properties().top_level_index_size > 0);

        // Point lookups traverse both index levels.
        for i in (0..1000u32).step_by(53) {
            assert_eq!(
                reader.get(format!("k{i:06}").as_bytes(), 100_000).unwrap(),
                Some((InternalKeyKind::Set, format!("value-{i}").into_bytes())),
                "lookup k{i:06}"
            );
        }
        assert_eq!(reader.get(b"k999999", 100_000).unwrap(), None);

        // Full iteration flattens the two-level index and returns all entries in order.
        let mut it = reader.iter().unwrap();
        let mut count = 0;
        let mut ok = it.first().unwrap();
        while ok {
            count += 1;
            ok = it.next().unwrap();
        }
        assert_eq!(count, 1000);
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
