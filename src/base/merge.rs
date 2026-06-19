// Copyright (c) 2012 The LevelDB-Go Authors. All rights reserved.
// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Modeled on Pebble's base.Merger.

//! The merge operator: read-modify-write semantics for the `MERGE` operation.
//!
//! A `MERGE` records an *operand* for a key rather than a full value. When the key is
//! read (or compacted), all of its merge operands are combined — in chronological order,
//! oldest first — on top of the most recent base value (a `SET`), or on top of nothing if
//! the key was deleted or never set, to produce the effective value. The combining logic
//! is supplied by a [`Merger`].

/// Combines merge operands into a value.
///
/// `full_merge` receives the operands for a single key in **chronological order (oldest
/// first)** and the base value they apply on top of (the most recent `SET`, or `None` if
/// the key has no surviving point value). Implementations fold the operands over the
/// base and return the resulting value.
pub trait Merger: Send + Sync {
    /// The merger's name. Persisted in the MANIFEST/properties and checked on open.
    fn name(&self) -> &str;

    /// Merges `operands` (oldest first) on top of `existing`, returning the value.
    fn full_merge(&self, key: &[u8], existing: Option<&[u8]>, operands: &[Vec<u8>]) -> Vec<u8>;
}

/// A simple [`Merger`] that concatenates the base value and all operands in chronological
/// order. Useful as a default and for tests (e.g. an append-only log value).
pub struct ConcatMerger;

impl Merger for ConcatMerger {
    fn name(&self) -> &str {
        "pebbledb.concat"
    }

    fn full_merge(&self, _key: &[u8], existing: Option<&[u8]>, operands: &[Vec<u8>]) -> Vec<u8> {
        let mut out = existing.map(|e| e.to_vec()).unwrap_or_default();
        for op in operands {
            out.extend_from_slice(op);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concat_merger_folds_oldest_first() {
        let m = ConcatMerger;
        // No base: just the operands concatenated oldest-first.
        assert_eq!(
            m.full_merge(b"k", None, &[b"a".to_vec(), b"b".to_vec()]),
            b"ab"
        );
        // With a base value, operands append after it.
        assert_eq!(
            m.full_merge(b"k", Some(b"base"), &[b"-x".to_vec(), b"-y".to_vec()]),
            b"base-x-y"
        );
        // No operands returns the base unchanged.
        assert_eq!(m.full_merge(b"k", Some(b"v"), &[]), b"v");
    }
}
