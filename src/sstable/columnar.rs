// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

//! A columnar sstable writer and reader (Pebble table format v5+).
//!
//! This composes the [`super::colblk`] columnar block formats into a complete sstable byte
//! stream and reads it back, exercising the columnar **data** and **index** blocks
//! end-to-end through the same block-trailer framing (compression byte + checksum), footer,
//! metaindex, and properties block used by the row-based writer.
//!
//! Each data block stores a run of `(internal key, value)` rows column-by-column; the
//! columnar index block maps each data block's last key to its on-disk handle. A lookup
//! binary-searches the index for the first block whose last key is `>= target`, then scans
//! that block's rows.
//!
//! Scope: keys are laid out through a pluggable [`super::keyschema::KeySchema`]. This writer
//! uses [`super::keyschema::DefaultKeySchema`] — the schema a general Pebble KV store uses
//! once columnar is enabled, `colblk.DefaultKeySchema(comparer, 16)`: a `PrefixBytes` prefix
//! column (split by the comparer) plus a `Bytes` suffix column. The schema name is recorded
//! as a table user property so a reader can select the matching decomposition. Byte-for-byte
//! interchange with a columnar table written by Pebble is validated by the interop CI (see
//! `ROADMAP.md`); CockroachDB's `cockroachkvs` schema is a separate, opt-in case.

use std::sync::Arc;

use crate::base::comparer::Comparer;
use crate::base::internal_key::{
    InternalKeyKind, encoded_trailer, encoded_user_key, trailer_kind, trailer_seqnum,
};
use crate::base::range_del::RangeTombstone;
use crate::base::range_key::{
    RangeKeyEntry, SuffixValue, encode_del_value, encode_set_value, encode_unset_value,
};
use crate::{Error, Result};

use super::block::{BlockHandle, BlockIter, ChecksumType, CompressionType, read_block};
use super::colblk;
use super::keyschema::{DefaultKeySchema, KeySchema};
use super::properties::{
    META_PROPERTIES_NAME, META_RANGE_DEL_NAME, META_RANGE_KEY_NAME, Properties,
};
use super::writer::{BlockBuilder, WriterOptions, encode_footer, write_block};
use super::{TableFormat, parse_footer};

/// Target uncompressed size of a columnar data block before it is flushed.
const TARGET_DATA_BLOCK_SIZE: usize = 32 * 1024;

/// User-property key under which the columnar key-schema name is recorded.
const KEY_SCHEMA_PROPERTY: &str = "pebbledb.key_schema";

/// Writes a columnar sstable to an in-memory buffer.
pub struct ColumnarWriter {
    cmp: Arc<dyn Comparer>,
    schema: DefaultKeySchema,
    opts: WriterOptions,
    buf: Vec<u8>,
    offset: u64,
    /// The current data block's rows, encoded through the key schema at flush time.
    data: Vec<(Vec<u8>, u64, Vec<u8>)>,
    approx_block_bytes: usize,
    index: colblk::IndexBlockBuilder,
    last_key: Vec<u8>,
    num_entries: u64,
}

impl ColumnarWriter {
    /// Creates a columnar writer. `opts.table_format` must be a columnar Pebble format
    /// (v5+); otherwise it is forced to `Pebble(5)`.
    pub fn new(cmp: Arc<dyn Comparer>, mut opts: WriterOptions) -> ColumnarWriter {
        if !matches!(opts.table_format, TableFormat::Pebble(v) if v >= 5) {
            opts.table_format = TableFormat::Pebble(5);
        }
        let schema = DefaultKeySchema::with_default_bundle_size(cmp.clone());
        ColumnarWriter {
            cmp,
            schema,
            opts,
            buf: Vec::new(),
            offset: 0,
            data: Vec::new(),
            approx_block_bytes: 0,
            index: colblk::IndexBlockBuilder::new(),
            last_key: Vec::new(),
            num_entries: 0,
        }
    }

    /// Adds a point entry. `internal_key` is the encoded internal key (user key + trailer);
    /// keys must be added in ascending internal-key order.
    pub fn add(&mut self, internal_key: &[u8], value: &[u8]) -> Result<()> {
        let user_key = encoded_user_key(internal_key);
        let trailer = encoded_trailer(internal_key);
        self.data.push((user_key.to_vec(), trailer, value.to_vec()));
        self.approx_block_bytes += user_key.len() + value.len() + 16;
        self.last_key = internal_key.to_vec();
        self.num_entries += 1;
        if self.approx_block_bytes >= TARGET_DATA_BLOCK_SIZE {
            self.flush_data_block()?;
        }
        Ok(())
    }

    fn flush_data_block(&mut self) -> Result<()> {
        if self.data.is_empty() {
            return Ok(());
        }
        let mut builder = colblk::SchemaDataBlockBuilder::new(&self.schema);
        for (user_key, trailer, value) in &self.data {
            builder.add(user_key, *trailer, value);
        }
        let block = builder.finish();
        let handle = write_block(
            &mut self.buf,
            &mut self.offset,
            &block,
            self.opts.compression,
            self.opts.checksum,
        )?;
        // Index entry: the block's last key -> its handle.
        self.index.add(&self.last_key, handle.offset, handle.length);
        self.data.clear();
        self.approx_block_bytes = 0;
        Ok(())
    }

    /// Finishes the table, returning the complete sstable bytes.
    pub fn finish(mut self) -> Result<Vec<u8>> {
        self.flush_data_block()?;

        // Columnar index block.
        let index_block = self.index.finish();
        let index_handle = write_block(
            &mut self.buf,
            &mut self.offset,
            &index_block,
            self.opts.compression,
            self.opts.checksum,
        )?;

        // Properties block (uncompressed), referenced from the metaindex.
        let mut user_properties = std::collections::BTreeMap::new();
        // Record the key schema so a reader selects the matching decomposition.
        user_properties.insert(
            KEY_SCHEMA_PROPERTY.to_string(),
            self.schema.name().as_bytes().to_vec(),
        );
        let props = Properties {
            num_entries: self.num_entries,
            comparer_name: self.cmp.name().to_string(),
            merger_name: "nullptr".to_string(),
            property_collectors: "[]".to_string(),
            compression_name: "NoCompression".to_string(),
            user_properties,
            ..Default::default()
        };
        let mut pb = BlockBuilder::new(1);
        for (name, value) in props.encode() {
            pb.add(name.as_bytes(), &value);
        }
        let props_handle = write_block(
            &mut self.buf,
            &mut self.offset,
            pb.finish(),
            CompressionType::None,
            self.opts.checksum,
        )?;

        // Metaindex block (uncompressed): one entry for the properties block.
        let mut mi = BlockBuilder::new(1);
        let mut ph = Vec::new();
        props_handle.encode_to(&mut ph);
        mi.add(META_PROPERTIES_NAME.as_bytes(), &ph);
        let metaindex_handle = write_block(
            &mut self.buf,
            &mut self.offset,
            mi.finish(),
            CompressionType::None,
            self.opts.checksum,
        )?;

        let footer = encode_footer(
            self.opts.table_format,
            self.opts.checksum,
            metaindex_handle,
            index_handle,
        )?;
        self.buf.extend_from_slice(&footer);
        Ok(self.buf)
    }
}

/// Reads a columnar sstable produced by [`ColumnarWriter`].
pub struct ColumnarReader {
    file: Arc<[u8]>,
    cmp: Arc<dyn Comparer>,
    schema: DefaultKeySchema,
    checksum: ChecksumType,
    /// The metaindex block handle, used to locate the keyspan (range-del / range-key) blocks.
    metaindex: BlockHandle,
    /// Index entries: each data block's last user key and its handle.
    index: Vec<(Vec<u8>, BlockHandle)>,
}

impl ColumnarReader {
    /// Opens a columnar table held in memory.
    pub fn open(file: impl Into<Arc<[u8]>>, cmp: Arc<dyn Comparer>) -> Result<ColumnarReader> {
        let file: Arc<[u8]> = file.into();
        let footer = parse_footer(&file)?;
        if !matches!(footer.format, TableFormat::Pebble(v) if v >= 5) {
            return Err(Error::corruption("columnar: not a columnar table"));
        }
        let index_block = read_block(&file, footer.index, footer.checksum)?;
        let entries = colblk::decode_index_block(&index_block)?;
        let index = entries
            .into_iter()
            .map(|(sep, off, len)| {
                (
                    encoded_user_key(&sep).to_vec(),
                    BlockHandle {
                        offset: off,
                        length: len,
                    },
                )
            })
            .collect();
        let schema = DefaultKeySchema::with_default_bundle_size(cmp.clone());
        Ok(ColumnarReader {
            file,
            cmp,
            schema,
            checksum: footer.checksum,
            metaindex: footer.metaindex,
            index,
        })
    }

    /// Reads the metaindex and decodes the columnar keyspan blocks (range deletions and range
    /// keys), converting them to the engine's `RangeTombstone` / `RangeKeyEntry` representation.
    ///
    /// Columnar range-del / range-key blocks use the boundary-based keyspan layout
    /// ([`colblk::decode_keyspan_block`]); each fragment's keys are re-encoded into the same
    /// row-format payload (`varstr(end) | …`) the rest of the engine consumes, so a columnar
    /// table with spans surfaces them identically to a row table.
    pub fn keyspans(&self) -> Result<(Vec<RangeTombstone>, Vec<RangeKeyEntry>)> {
        let metaindex = read_block(&self.file, self.metaindex, self.checksum)?;
        let mut it = BlockIter::new(metaindex)?;
        it.first();
        let mut range_del_handle = None;
        let mut range_key_handle = None;
        while it.valid() {
            let key = it.key();
            if key == META_RANGE_DEL_NAME.as_bytes() {
                range_del_handle = BlockHandle::decode(it.value()).map(|(h, _)| h);
            } else if key == META_RANGE_KEY_NAME.as_bytes() {
                range_key_handle = BlockHandle::decode(it.value()).map(|(h, _)| h);
            }
            it.next();
        }

        let mut range_dels = Vec::new();
        if let Some(handle) = range_del_handle {
            let block = read_block(&self.file, handle, self.checksum)?;
            for span in colblk::decode_keyspan_block(&block)? {
                for k in &span.keys {
                    range_dels.push(RangeTombstone::new(
                        span.start.clone(),
                        span.end.clone(),
                        trailer_seqnum(k.trailer),
                    ));
                }
            }
        }

        let mut range_keys = Vec::new();
        if let Some(handle) = range_key_handle {
            let block = read_block(&self.file, handle, self.checksum)?;
            for span in colblk::decode_keyspan_block(&block)? {
                for k in &span.keys {
                    let kind = trailer_kind(k.trailer);
                    let value = match kind {
                        InternalKeyKind::RangeKeySet => encode_set_value(
                            &span.end,
                            &[SuffixValue {
                                suffix: k.suffix.clone(),
                                value: k.value.clone(),
                            }],
                        ),
                        InternalKeyKind::RangeKeyUnset => {
                            encode_unset_value(&span.end, std::slice::from_ref(&k.suffix))
                        }
                        InternalKeyKind::RangeKeyDelete => encode_del_value(&span.end),
                        _ => {
                            return Err(Error::corruption(
                                "columnar: unexpected range-key kind in keyspan block",
                            ));
                        }
                    };
                    range_keys.push(RangeKeyEntry {
                        kind,
                        start: span.start.clone(),
                        seqnum: trailer_seqnum(k.trailer),
                        value,
                    });
                }
            }
        }

        Ok((range_dels, range_keys))
    }

    /// Reads and decodes the columnar data block at `handle` into its rows.
    fn read_data_block(&self, handle: BlockHandle) -> Result<Vec<colblk::DataBlockRow>> {
        let block = read_block(&self.file, handle, self.checksum)?;
        colblk::SchemaDataBlockReader::new(&block, &self.schema)?.decode_all()
    }

    /// Returns every `(internal_key, value)` pair in the table, in order.
    pub fn iter_all(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut out = Vec::new();
        for (_, handle) in &self.index {
            for (user_key, trailer, value) in self.read_data_block(*handle)? {
                let mut ik = user_key;
                ik.extend_from_slice(&trailer.to_le_bytes());
                out.push((ik, value));
            }
        }
        Ok(out)
    }

    /// Looks up `user_key`, returning the value of its newest entry, or `None`. Within a
    /// user key, rows are ordered newest-first (descending trailer), so the first match is
    /// the newest.
    pub fn get(&self, user_key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Find the first data block whose last user key is >= user_key.
        let blk = self
            .index
            .iter()
            .find(|(last, _)| self.cmp.compare(last, user_key) != std::cmp::Ordering::Less);
        let Some((_, handle)) = blk else {
            return Ok(None);
        };
        for (k, _trailer, value) in self.read_data_block(*handle)? {
            if self.cmp.compare(&k, user_key) == std::cmp::Ordering::Equal {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    /// The number of data blocks (index entries) in the table.
    pub fn num_data_blocks(&self) -> usize {
        self.index.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::comparer::DefaultComparer;
    use crate::base::internal_key::{InternalKey, InternalKeyKind};

    fn ikey(user: &[u8], seq: u64) -> Vec<u8> {
        InternalKey::new(user.to_vec(), seq, InternalKeyKind::Set).encode()
    }

    #[test]
    fn columnar_table_roundtrips_through_writer_and_reader() {
        let cmp: Arc<dyn Comparer> = Arc::new(DefaultComparer);
        let mut w = ColumnarWriter::new(
            cmp.clone(),
            WriterOptions {
                // Small blocks so multiple data blocks + index entries are exercised.
                compression: CompressionType::None,
                ..Default::default()
            },
        );
        let n = 2000u32;
        for i in 0..n {
            let k = ikey(format!("key{i:05}").as_bytes(), (n - i) as u64);
            w.add(&k, format!("value{i}").as_bytes()).unwrap();
        }
        let bytes = w.finish().unwrap();

        let r = ColumnarReader::open(bytes, cmp).unwrap();
        assert!(r.num_data_blocks() >= 1);

        // Full ordered iteration matches what was written.
        let all = r.iter_all().unwrap();
        assert_eq!(all.len(), n as usize);
        for (i, (ik, v)) in all.iter().enumerate() {
            assert_eq!(encoded_user_key(ik), format!("key{i:05}").as_bytes());
            assert_eq!(v.as_slice(), format!("value{i}").as_bytes());
        }

        // Point lookups.
        assert_eq!(r.get(b"key00000").unwrap().as_deref(), Some(&b"value0"[..]));
        assert_eq!(
            r.get(b"key01999").unwrap().as_deref(),
            Some(&b"value1999"[..])
        );
        assert_eq!(r.get(b"key02000").unwrap(), None);
        assert_eq!(r.get(b"missing").unwrap(), None);
    }

    #[test]
    fn columnar_table_records_key_schema_property() {
        let cmp: Arc<dyn Comparer> = Arc::new(DefaultComparer);
        let mut w = ColumnarWriter::new(
            cmp.clone(),
            WriterOptions {
                compression: CompressionType::None,
                ..Default::default()
            },
        );
        w.add(&ikey(b"a", 1), b"v").unwrap();
        let bytes = w.finish().unwrap();
        // The schema name should appear verbatim somewhere in the table bytes (it is stored
        // as a user property in the properties block).
        let name = b"DefaultKeySchema(leveldb.BytewiseComparator,16)";
        assert!(
            bytes.windows(name.len()).any(|w| w == name),
            "key-schema property not found in table"
        );
        // And the table still round-trips.
        let r = ColumnarReader::open(bytes, cmp).unwrap();
        assert_eq!(r.get(b"a").unwrap().as_deref(), Some(&b"v"[..]));
    }

    #[test]
    fn columnar_table_roundtrips_keys_with_shared_prefixes() {
        // Keys sharing a long prefix exercise the schema's PrefixBytes prefix column; the
        // DefaultComparer yields an empty suffix for every key.
        let cmp: Arc<dyn Comparer> = Arc::new(DefaultComparer);
        let mut w = ColumnarWriter::new(
            cmp.clone(),
            WriterOptions {
                compression: CompressionType::None,
                ..Default::default()
            },
        );
        let n = 500u32;
        for i in 0..n {
            let k = ikey(
                format!("shared/prefix/key{i:05}").as_bytes(),
                (n - i) as u64,
            );
            w.add(&k, format!("v{i}").as_bytes()).unwrap();
        }
        let bytes = w.finish().unwrap();
        let r = ColumnarReader::open(bytes, cmp).unwrap();
        let all = r.iter_all().unwrap();
        assert_eq!(all.len(), n as usize);
        for (i, (ik, v)) in all.iter().enumerate() {
            assert_eq!(
                encoded_user_key(ik),
                format!("shared/prefix/key{i:05}").as_bytes()
            );
            assert_eq!(v.as_slice(), format!("v{i}").as_bytes());
        }
        assert_eq!(
            r.get(b"shared/prefix/key00250").unwrap().as_deref(),
            Some(&b"v250"[..])
        );
    }

    #[test]
    fn empty_columnar_table_roundtrips() {
        let cmp: Arc<dyn Comparer> = Arc::new(DefaultComparer);
        let w = ColumnarWriter::new(cmp.clone(), WriterOptions::default());
        let bytes = w.finish().unwrap();
        let r = ColumnarReader::open(bytes, cmp).unwrap();
        assert_eq!(r.num_data_blocks(), 0);
        assert!(r.iter_all().unwrap().is_empty());
        assert_eq!(r.get(b"anything").unwrap(), None);
    }

    #[test]
    fn rejects_non_columnar_table() {
        // A row-format table must be rejected by the columnar reader.
        let cmp: Arc<dyn Comparer> = Arc::new(DefaultComparer);
        let mut w = super::super::Writer::new(Vec::new(), cmp.clone(), WriterOptions::default());
        w.add(&ikey(b"a", 1), b"v").unwrap();
        let bytes = w.finish().unwrap();
        assert!(ColumnarReader::open(bytes, cmp).is_err());
    }
}
