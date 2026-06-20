// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Ported from Pebble's sstable/colblk KeySchema / DefaultKeySchema.

//! Key schemas for columnar data blocks.
//!
//! A columnar data block does not store each user key as one opaque byte string. Instead a
//! *key schema* decides how the user key is decomposed across one or more columns. This lets
//! the writer exploit structure in the keys — most importantly, that adjacent keys share a
//! long common *prefix* and differ only in a short trailing *suffix* (an MVCC timestamp /
//! version, in Pebble's intended use). The schema therefore typically stores:
//!
//! * the **prefix** (the user key with its suffix stripped, via [`Comparer::split`]) in a
//!   [`DataType::PrefixBytes`] column, which prefix-compresses lexicographically-sorted
//!   slices in bundles; and
//! * the **suffix** (the trailing version bytes) in a separate [`DataType::Bytes`] column.
//!
//! This module provides:
//!
//! * the [`KeySchema`] trait — split a key, encode a run of keys into columns, and decode
//!   (reconstruct) the i-th key back out; and
//! * [`DefaultKeySchema`] — the concrete schema a general Pebble KV store uses once columnar
//!   blocks are enabled, modelled after Pebble's `colblk.DefaultKeySchema(comparer, 16)`: a
//!   `PrefixBytes` prefix column plus a `Bytes` suffix column.
//!
//! # Cross-implementation parity
//!
//! The exact on-disk bytes produced here are *not* required to match upstream Pebble
//! byte-for-byte within this crate; byte-level interop with Pebble's production columnar
//! tables is validated separately by the Go interop CI (see `ROADMAP.md`). What this module
//! guarantees in-crate is a self-consistent round-trip: any run of keys encoded through a
//! schema decodes back to exactly the same keys.

use std::sync::Arc;

use crate::Result;
use crate::base::comparer::Comparer;

use super::colblk::{
    DataType, decode_prefix_bytes, decode_raw_bytes, encode_prefix_bytes, encode_raw_bytes,
};

/// The default `PrefixBytes` bundle size used by [`DefaultKeySchema`], matching Pebble's
/// `colblk.DefaultKeySchema(comparer, 16)`.
pub const DEFAULT_BUNDLE_SIZE: usize = 16;

/// The layout of one column written by a [`KeySchema`]: its data type and the column index it
/// occupies within the data block's key columns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeyColumn {
    /// The column's on-disk data type.
    pub data_type: DataType,
}

/// Decomposes user keys into the columns of a columnar data block.
///
/// A schema is responsible for the *key* columns only; the surrounding block (trailer and
/// value columns, the header, etc.) is assembled by the data-block builder. Implementations
/// must satisfy the round-trip contract: for any slice of keys, [`encode_keys`] followed by
/// [`decode_key`] for each index reproduces the original keys exactly.
///
/// [`encode_keys`]: KeySchema::encode_keys
/// [`decode_key`]: KeySchema::decode_key
pub trait KeySchema: Send + Sync {
    /// The schema's stable name, recorded in table properties so a reader can select the
    /// matching schema.
    fn name(&self) -> &str;

    /// The data types of the key columns this schema writes, in column order.
    fn columns(&self) -> Vec<KeyColumn>;

    /// Splits `key` into `(prefix, suffix)`, where `prefix` is the key without its version
    /// suffix and `suffix` is the trailing version bytes (possibly empty).
    fn split<'k>(&self, key: &'k [u8]) -> (&'k [u8], &'k [u8]);

    /// Encodes `keys` into the schema's key columns, appending them to `buf` starting at the
    /// current end of `buf`. Returns the byte offset within `buf` at which each column's data
    /// begins, in column order (so the caller can record per-column page offsets in the block
    /// header). `buf` already contains everything written before the key columns.
    fn encode_keys(&self, keys: &[&[u8]], buf: &mut Vec<u8>) -> Vec<usize>;

    /// Decodes all `rows` keys from `block`, given the per-column starting offsets
    /// (`col_offsets`, in the order returned by [`columns`]). Returns the reconstructed keys.
    ///
    /// [`columns`]: KeySchema::columns
    fn decode_keys(&self, block: &[u8], col_offsets: &[usize], rows: usize)
    -> Result<Vec<Vec<u8>>>;

    /// Decodes a single key by index. The default implementation decodes all keys and indexes
    /// into them; schemas may override for efficiency.
    fn decode_key(
        &self,
        block: &[u8],
        col_offsets: &[usize],
        rows: usize,
        i: usize,
    ) -> Result<Vec<u8>> {
        Ok(self.decode_keys(block, col_offsets, rows)?.swap_remove(i))
    }
}

/// The schema a general Pebble KV store uses once columnar blocks are enabled, modelled after
/// `colblk.DefaultKeySchema(comparer, 16)`.
///
/// It writes two key columns:
///
/// 1. a [`DataType::PrefixBytes`] column of the key prefixes (the user key with its version
///    suffix stripped via the comparer's [`Comparer::split`]), prefix-compressed in bundles
///    of [`DefaultKeySchema::bundle_size`]; and
/// 2. a [`DataType::Bytes`] column of the corresponding suffixes (the trailing version bytes,
///    which may be empty).
///
/// A key is reconstructed as `prefix ++ suffix`.
pub struct DefaultKeySchema {
    cmp: Arc<dyn Comparer>,
    bundle_size: usize,
    name: String,
}

impl DefaultKeySchema {
    /// Creates a default key schema using `cmp` to split keys and the given `PrefixBytes`
    /// bundle size (a power of two `>= 1`). The conventional value is [`DEFAULT_BUNDLE_SIZE`].
    pub fn new(cmp: Arc<dyn Comparer>, bundle_size: usize) -> DefaultKeySchema {
        assert!(
            bundle_size.is_power_of_two() && bundle_size >= 1,
            "bundle size must be a power of two >= 1"
        );
        // The schema name encodes the comparer it is bound to plus its bundle size, mirroring
        // how Pebble derives a DefaultKeySchema name from the comparer + bundle size.
        let name = format!("DefaultKeySchema({},{bundle_size})", cmp.name());
        DefaultKeySchema {
            cmp,
            bundle_size,
            name,
        }
    }

    /// Creates a default key schema with the conventional bundle size of
    /// [`DEFAULT_BUNDLE_SIZE`].
    pub fn with_default_bundle_size(cmp: Arc<dyn Comparer>) -> DefaultKeySchema {
        DefaultKeySchema::new(cmp, DEFAULT_BUNDLE_SIZE)
    }

    /// The `PrefixBytes` bundle size this schema uses.
    pub fn bundle_size(&self) -> usize {
        self.bundle_size
    }
}

/// Column index of the prefix column within [`DefaultKeySchema`]'s key columns.
const COL_PREFIX: usize = 0;
/// Column index of the suffix column within [`DefaultKeySchema`]'s key columns.
const COL_SUFFIX: usize = 1;

impl KeySchema for DefaultKeySchema {
    fn name(&self) -> &str {
        &self.name
    }

    fn columns(&self) -> Vec<KeyColumn> {
        vec![
            KeyColumn {
                data_type: DataType::PrefixBytes,
            },
            KeyColumn {
                data_type: DataType::Bytes,
            },
        ]
    }

    fn split<'k>(&self, key: &'k [u8]) -> (&'k [u8], &'k [u8]) {
        let n = self.cmp.split(key);
        // `split` returns the prefix length; defensively clamp to the key length so a
        // misbehaving comparer cannot cause an out-of-bounds slice.
        let n = n.min(key.len());
        (&key[..n], &key[n..])
    }

    fn encode_keys(&self, keys: &[&[u8]], buf: &mut Vec<u8>) -> Vec<usize> {
        let mut prefixes: Vec<&[u8]> = Vec::with_capacity(keys.len());
        let mut suffixes: Vec<&[u8]> = Vec::with_capacity(keys.len());
        for &k in keys {
            let (p, s) = self.split(k);
            prefixes.push(p);
            suffixes.push(s);
        }

        let prefix_off = buf.len();
        encode_prefix_bytes(&prefixes, self.bundle_size, prefix_off, buf);
        let suffix_off = buf.len();
        encode_raw_bytes(&suffixes, suffix_off, buf);

        vec![prefix_off, suffix_off]
    }

    fn decode_keys(
        &self,
        block: &[u8],
        col_offsets: &[usize],
        rows: usize,
    ) -> Result<Vec<Vec<u8>>> {
        let prefix_off = col_offsets[COL_PREFIX];
        let suffix_off = col_offsets[COL_SUFFIX];
        let (prefixes, _) = decode_prefix_bytes(block, prefix_off, rows)?;
        let (suffixes, _) = decode_raw_bytes(block, suffix_off, rows)?;
        Ok((0..rows)
            .map(|i| {
                let mut key = Vec::with_capacity(prefixes[i].len() + suffixes[i].len());
                key.extend_from_slice(&prefixes[i]);
                key.extend_from_slice(suffixes[i]);
                key
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::comparer::{Comparer, DefaultComparer};
    use std::cmp::Ordering;

    /// A test comparer that splits a key at the last '@' byte, treating everything from the
    /// '@' onward (inclusive) as the version suffix — a stand-in for an MVCC suffix.
    struct AtSuffixComparer;

    impl Comparer for AtSuffixComparer {
        fn name(&self) -> &str {
            "test.AtSuffixComparer"
        }
        fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
            a.cmp(b)
        }
        fn abbreviated_key(&self, _key: &[u8]) -> u64 {
            0
        }
        fn split(&self, key: &[u8]) -> usize {
            match key.iter().rposition(|&b| b == b'@') {
                Some(i) => i,
                None => key.len(),
            }
        }
        fn separator(&self, dst: &mut Vec<u8>, a: &[u8], _b: &[u8]) {
            dst.extend_from_slice(a);
        }
        fn successor(&self, dst: &mut Vec<u8>, a: &[u8]) {
            dst.extend_from_slice(a);
        }
    }

    /// Encodes `keys` through `schema` into a fresh buffer (with some leading bytes to ensure
    /// non-zero column offsets are handled), then decodes each key back and asserts equality.
    fn roundtrip(schema: &dyn KeySchema, keys: &[&[u8]]) {
        let mut buf = vec![0xAA, 0xBB, 0xCC]; // simulate a preceding header
        let offs = schema.encode_keys(keys, &mut buf);
        assert_eq!(offs.len(), schema.columns().len());

        // decode_keys reconstructs all keys.
        let all = schema.decode_keys(&buf, &offs, keys.len()).unwrap();
        let want: Vec<Vec<u8>> = keys.iter().map(|k| k.to_vec()).collect();
        assert_eq!(all, want);

        // decode_key reconstructs each key individually.
        for (i, k) in keys.iter().enumerate() {
            let got = schema.decode_key(&buf, &offs, keys.len(), i).unwrap();
            assert_eq!(&got, &k.to_vec(), "key {i}");
        }
    }

    #[test]
    fn split_default_comparer_has_no_suffix() {
        let schema = DefaultKeySchema::with_default_bundle_size(Arc::new(DefaultComparer));
        let (p, s) = schema.split(b"hello");
        assert_eq!(p, b"hello");
        assert_eq!(s, b"");
    }

    #[test]
    fn split_at_suffix_comparer() {
        let schema = DefaultKeySchema::new(Arc::new(AtSuffixComparer), 16);
        let (p, s) = schema.split(b"user@1234");
        assert_eq!(p, b"user");
        assert_eq!(s, b"@1234");
        // No suffix marker.
        let (p, s) = schema.split(b"plain");
        assert_eq!(p, b"plain");
        assert_eq!(s, b"");
        // Empty key.
        let (p, s) = schema.split(b"");
        assert_eq!(p, b"");
        assert_eq!(s, b"");
    }

    #[test]
    fn name_includes_comparer_and_bundle() {
        let schema = DefaultKeySchema::new(Arc::new(DefaultComparer), 16);
        assert_eq!(
            schema.name(),
            "DefaultKeySchema(leveldb.BytewiseComparator,16)"
        );
        let schema2 = DefaultKeySchema::new(Arc::new(DefaultComparer), 4);
        assert_eq!(
            schema2.name(),
            "DefaultKeySchema(leveldb.BytewiseComparator,4)"
        );
    }

    #[test]
    fn columns_are_prefixbytes_then_bytes() {
        let schema = DefaultKeySchema::with_default_bundle_size(Arc::new(DefaultComparer));
        let cols = schema.columns();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].data_type, DataType::PrefixBytes);
        assert_eq!(cols[1].data_type, DataType::Bytes);
    }

    #[test]
    fn roundtrip_no_suffix_default_comparer() {
        let schema = DefaultKeySchema::with_default_bundle_size(Arc::new(DefaultComparer));
        let keys: Vec<&[u8]> = vec![b"apple", b"apricot", b"banana", b"cherry", b"cherrypie"];
        roundtrip(&schema, &keys);
    }

    #[test]
    fn roundtrip_single_key() {
        let schema = DefaultKeySchema::with_default_bundle_size(Arc::new(DefaultComparer));
        roundtrip(&schema, &[b"only"]);
        let at = DefaultKeySchema::new(Arc::new(AtSuffixComparer), 16);
        roundtrip(&at, &[b"only@99"]);
    }

    #[test]
    fn roundtrip_empty_block() {
        let schema = DefaultKeySchema::with_default_bundle_size(Arc::new(DefaultComparer));
        roundtrip(&schema, &[]);
    }

    #[test]
    fn roundtrip_shared_prefix_varied_suffix() {
        let schema = DefaultKeySchema::new(Arc::new(AtSuffixComparer), 16);
        // Many keys sharing the "users/" prefix, with varied (and some empty) suffixes.
        let keys: Vec<&[u8]> = vec![
            b"users/alice",
            b"users/alice@100",
            b"users/alice@99",
            b"users/alice@1",
            b"users/bob",
            b"users/bob@50",
            b"users/carol@7",
            b"users/carol@6",
            b"users/dave",
            b"users/dave@1000000",
            b"users/eve@1",
            b"users/eve@1",
        ];
        roundtrip(&schema, &keys);
    }

    #[test]
    fn roundtrip_all_same_prefix() {
        let schema = DefaultKeySchema::new(Arc::new(AtSuffixComparer), 4);
        let keys: Vec<&[u8]> = vec![
            b"k@1", b"k@2", b"k@3", b"k@4", b"k@5", b"k@6", b"k@7", b"k@8", b"k@9",
        ];
        roundtrip(&schema, &keys);
    }

    #[test]
    fn roundtrip_all_empty_suffix() {
        let schema = DefaultKeySchema::new(Arc::new(AtSuffixComparer), 16);
        let keys: Vec<&[u8]> = vec![b"aaa", b"aab", b"abc", b"abd", b"zzz"];
        roundtrip(&schema, &keys);
    }

    #[test]
    fn roundtrip_multiple_blocks_independently() {
        // Encoding several independent "blocks" through the same schema must each round-trip;
        // exercises that encode/decode carry no hidden state between calls.
        let schema = DefaultKeySchema::new(Arc::new(AtSuffixComparer), 16);
        let block_a: Vec<&[u8]> = vec![b"a/1@5", b"a/1@4", b"a/2", b"a/2@9"];
        let block_b: Vec<&[u8]> = vec![b"z/longprefixhere@1", b"z/longprefixhere@0", b"z/zz"];
        roundtrip(&schema, &block_a);
        roundtrip(&schema, &block_b);
    }

    #[test]
    fn roundtrip_many_bundles() {
        // More than one bundle (bundle size 4, 30 keys) with shared prefixes & suffixes.
        let schema = DefaultKeySchema::new(Arc::new(AtSuffixComparer), 4);
        let owned: Vec<Vec<u8>> = (0..30)
            .map(|i| format!("row/{:04}@{}", i / 2, i % 5).into_bytes())
            .collect();
        let keys: Vec<&[u8]> = owned.iter().map(|k| k.as_slice()).collect();
        roundtrip(&schema, &keys);
    }
}
