// Copyright (c) 2011 The LevelDB-Go Authors. All rights reserved.
// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Ported from Pebble's sstable/properties.go.

//! The sstable properties block: table-level metadata stored under the metaindex key
//! `rocksdb.properties`.
//!
//! Each property is one block entry whose key is the property name and whose value is
//! the property's encoding: integers as LEB128 uvarints, strings as raw bytes, booleans
//! as a single `0`/`1` byte. Entries are sorted by name. Many integer/string properties
//! are always written (Pebble's `encodeempty` option) even when zero/empty, so that
//! files round-trip and remain RocksDB-compatible.

use crate::base::varint::{get_uvarint, put_uvarint};

// Property names (part of the on-disk format).
const NUM_ENTRIES: &str = "rocksdb.num.entries";
const RAW_KEY_SIZE: &str = "rocksdb.raw.key.size";
const RAW_VALUE_SIZE: &str = "rocksdb.raw.value.size";
const NUM_DELETIONS: &str = "rocksdb.deleted.keys";
const NUM_RANGE_DELETIONS: &str = "rocksdb.num.range-deletions";
const NUM_DATA_BLOCKS: &str = "rocksdb.num.data.blocks";
const DATA_SIZE: &str = "rocksdb.data.size";
const INDEX_SIZE: &str = "rocksdb.index.size";
const INDEX_TYPE: &str = "rocksdb.block.based.table.index.type";
const TOP_LEVEL_INDEX_SIZE: &str = "rocksdb.top-level.index.size";
const FILTER_SIZE: &str = "rocksdb.filter.size";
const FILTER_POLICY: &str = "rocksdb.filter.policy";
const COMPARATOR: &str = "rocksdb.comparator";
const MERGER: &str = "rocksdb.merge.operator";
const NUM_MERGE_OPERANDS: &str = "rocksdb.merge.operands";
const PROPERTY_COLLECTORS: &str = "rocksdb.property.collectors";
const COMPRESSION: &str = "rocksdb.compression";

/// Index types recorded in [`Properties::index_type`].
pub const BINARY_SEARCH_INDEX: u32 = 0;
/// A two-level (partitioned) index.
pub const TWO_LEVEL_INDEX: u32 = 2;

/// Table-level metadata from (or destined for) the properties block.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Properties {
    /// Total number of key/value entries.
    pub num_entries: u64,
    /// Sum of the encoded internal-key sizes.
    pub raw_key_size: u64,
    /// Sum of the value sizes.
    pub raw_value_size: u64,
    /// Number of point + range deletions.
    pub num_deletions: u64,
    /// Number of range deletions.
    pub num_range_deletions: u64,
    /// Number of data blocks.
    pub num_data_blocks: u64,
    /// Total size of the data blocks.
    pub data_size: u64,
    /// Size of the index block.
    pub index_size: u64,
    /// Index type ([`BINARY_SEARCH_INDEX`] or [`TWO_LEVEL_INDEX`]).
    pub index_type: u32,
    /// Size of the top-level index block (two-level indexes).
    pub top_level_index_size: u64,
    /// Size of the filter block, if any.
    pub filter_size: u64,
    /// Number of merge operands.
    pub num_merge_operands: u64,
    /// The user-key comparer name.
    pub comparer_name: String,
    /// The merge-operator name (`"nullptr"` when none).
    pub merger_name: String,
    /// The filter policy name (empty when none).
    pub filter_policy: String,
    /// The block compression name.
    pub compression_name: String,
    /// The property-collector names (`"[]"` when none).
    pub property_collectors: String,
    /// Arbitrary user / block-property entries (collector outputs and any properties this
    /// reader does not model), preserved verbatim for round-trip and filtering.
    pub user_properties: std::collections::BTreeMap<String, Vec<u8>>,
}

impl Properties {
    /// Builds the sorted `(name, value)` entries for the properties block.
    pub fn encode(&self) -> Vec<(String, Vec<u8>)> {
        let mut m: Vec<(String, Vec<u8>)> = Vec::new();
        let mut put_u = |name: &str, v: u64| m.push((name.to_string(), uvarint_bytes(v)));

        // `encodeempty` integer properties: always written.
        put_u(NUM_ENTRIES, self.num_entries);
        put_u(RAW_KEY_SIZE, self.raw_key_size);
        put_u(RAW_VALUE_SIZE, self.raw_value_size);
        put_u(NUM_DELETIONS, self.num_deletions);
        put_u(NUM_RANGE_DELETIONS, self.num_range_deletions);
        put_u(NUM_DATA_BLOCKS, self.num_data_blocks);
        put_u(DATA_SIZE, self.data_size);
        put_u(NUM_MERGE_OPERANDS, self.num_merge_operands);
        put_u(INDEX_TYPE, u64::from(self.index_type));
        put_u(FILTER_SIZE, self.filter_size);

        // Optional integer properties: written only when non-zero.
        if self.index_size != 0 {
            m.push((INDEX_SIZE.to_string(), uvarint_bytes(self.index_size)));
        }
        if self.top_level_index_size != 0 {
            m.push((
                TOP_LEVEL_INDEX_SIZE.to_string(),
                uvarint_bytes(self.top_level_index_size),
            ));
        }

        // String properties.
        m.push((
            COMPARATOR.to_string(),
            self.comparer_name.clone().into_bytes(),
        ));
        m.push((MERGER.to_string(), self.merger_name.clone().into_bytes()));
        m.push((
            PROPERTY_COLLECTORS.to_string(),
            self.property_collectors.clone().into_bytes(),
        ));
        if !self.compression_name.is_empty() {
            m.push((
                COMPRESSION.to_string(),
                self.compression_name.clone().into_bytes(),
            ));
        }
        if !self.filter_policy.is_empty() {
            m.push((
                FILTER_POLICY.to_string(),
                self.filter_policy.clone().into_bytes(),
            ));
        }

        // User / block-property entries (e.g. block-property-collector outputs).
        for (k, v) in &self.user_properties {
            m.push((k.clone(), v.clone()));
        }

        m.sort_by(|a, b| a.0.cmp(&b.0));
        m
    }

    /// The set of known (modelled) property names; any other entry is a user property.
    fn is_known_property(name: &[u8]) -> bool {
        const KNOWN: &[&str] = &[
            NUM_ENTRIES,
            RAW_KEY_SIZE,
            RAW_VALUE_SIZE,
            NUM_DELETIONS,
            NUM_RANGE_DELETIONS,
            NUM_DATA_BLOCKS,
            DATA_SIZE,
            INDEX_SIZE,
            INDEX_TYPE,
            TOP_LEVEL_INDEX_SIZE,
            FILTER_SIZE,
            NUM_MERGE_OPERANDS,
            COMPARATOR,
            MERGER,
            FILTER_POLICY,
            COMPRESSION,
            PROPERTY_COLLECTORS,
        ];
        KNOWN.iter().any(|k| k.as_bytes() == name)
    }

    /// Decodes a property `(name, value)` entry into `self`.
    pub fn decode_entry(&mut self, name: &[u8], value: &[u8]) {
        let uv = || get_uvarint(value).map(|(v, _)| v).unwrap_or(0);
        let s = || String::from_utf8_lossy(value).into_owned();
        match name {
            n if n == NUM_ENTRIES.as_bytes() => self.num_entries = uv(),
            n if n == RAW_KEY_SIZE.as_bytes() => self.raw_key_size = uv(),
            n if n == RAW_VALUE_SIZE.as_bytes() => self.raw_value_size = uv(),
            n if n == NUM_DELETIONS.as_bytes() => self.num_deletions = uv(),
            n if n == NUM_RANGE_DELETIONS.as_bytes() => self.num_range_deletions = uv(),
            n if n == NUM_DATA_BLOCKS.as_bytes() => self.num_data_blocks = uv(),
            n if n == DATA_SIZE.as_bytes() => self.data_size = uv(),
            n if n == INDEX_SIZE.as_bytes() => self.index_size = uv(),
            n if n == INDEX_TYPE.as_bytes() => self.index_type = uv() as u32,
            n if n == TOP_LEVEL_INDEX_SIZE.as_bytes() => self.top_level_index_size = uv(),
            n if n == FILTER_SIZE.as_bytes() => self.filter_size = uv(),
            n if n == NUM_MERGE_OPERANDS.as_bytes() => self.num_merge_operands = uv(),
            n if n == COMPARATOR.as_bytes() => self.comparer_name = s(),
            n if n == MERGER.as_bytes() => self.merger_name = s(),
            n if n == FILTER_POLICY.as_bytes() => self.filter_policy = s(),
            n if n == COMPRESSION.as_bytes() => self.compression_name = s(),
            n if n == PROPERTY_COLLECTORS.as_bytes() => self.property_collectors = s(),
            _ => {
                // Unknown / user property (e.g. a block-property-collector output): keep it.
                if !Self::is_known_property(name) {
                    self.user_properties
                        .insert(String::from_utf8_lossy(name).into_owned(), value.to_vec());
                }
            }
        }
    }

    /// Whether the table uses a two-level index.
    pub fn is_two_level_index(&self) -> bool {
        self.index_type == TWO_LEVEL_INDEX
    }
}

fn uvarint_bytes(v: u64) -> Vec<u8> {
    let mut b = Vec::new();
    put_uvarint(&mut b, v);
    b
}

/// The metaindex key under which the properties block is stored.
pub const META_PROPERTIES_NAME: &str = "rocksdb.properties";

/// The metaindex key under which the v2 range-deletion block is stored.
pub const META_RANGE_DEL_NAME: &str = "rocksdb.range_del2";

/// The metaindex key under which the range-key block is stored.
pub const META_RANGE_KEY_NAME: &str = "pebble.range_key";

/// The metaindex key under which the value-block index is stored.
pub const META_VALUE_INDEX_NAME: &str = "pebble.value_index";

/// The metaindex key under which pebbledb stores the table's blob-reference list (the blob
/// file numbers referenced by `KIND_BLOB` values). pebbledb-specific.
pub const META_BLOB_REFS_NAME: &str = "pebbledb.blob_refs";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_through_entries() {
        let props = Properties {
            num_entries: 1000,
            raw_key_size: 12345,
            raw_value_size: 67890,
            num_deletions: 5,
            num_range_deletions: 0,
            num_data_blocks: 7,
            data_size: 40000,
            index_size: 256,
            index_type: TWO_LEVEL_INDEX,
            top_level_index_size: 64,
            filter_size: 0,
            num_merge_operands: 0,
            comparer_name: "leveldb.BytewiseComparator".to_string(),
            merger_name: "nullptr".to_string(),
            filter_policy: String::new(),
            compression_name: "Snappy".to_string(),
            property_collectors: "[]".to_string(),
            user_properties: Default::default(),
        };
        let entries = props.encode();
        // Entries must be sorted by name.
        for w in entries.windows(2) {
            assert!(w[0].0 < w[1].0, "{} !< {}", w[0].0, w[1].0);
        }
        let mut decoded = Properties::default();
        for (name, value) in &entries {
            decoded.decode_entry(name.as_bytes(), value);
        }
        assert_eq!(decoded, props);
        assert!(decoded.is_two_level_index());
    }

    #[test]
    fn unknown_properties_are_preserved_as_user_properties() {
        let mut p = Properties::default();
        p.decode_entry(b"some.user.property", b"whatever");
        assert_eq!(
            p.user_properties
                .get("some.user.property")
                .map(|v| v.as_slice()),
            Some(&b"whatever"[..])
        );
        // They round-trip through encode/decode.
        let mut d = Properties::default();
        for (name, value) in &p.encode() {
            d.decode_entry(name.as_bytes(), value);
        }
        assert_eq!(d.user_properties, p.user_properties);
    }
}
