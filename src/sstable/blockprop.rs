// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Modeled on Pebble's sstable.BlockPropertyCollector / BlockPropertyFilter.

//! Block properties: pluggable per-table summaries that let whole sstables be skipped.
//!
//! A [`BlockPropertyCollector`] accumulates a small summary over every entry written to a
//! table (for example the minimum and maximum of a key suffix), and its serialized output
//! is stored in the table's properties block. At read time a [`BlockPropertyFilter`]
//! inspects that stored value and decides whether the table could contain anything of
//! interest — if not, the table is skipped entirely.
//!
//! This is the Pebble *mechanism*; a concrete collector such as CockroachDB's MVCC-time
//! collector is application code built on top of it. Properties are stored per table (the
//! coarsest, highest-value granularity); per-block properties are a future refinement.

/// The properties-block key prefix under which a collector's output is stored.
pub(crate) const BLOCK_PROPERTY_PREFIX: &str = "pebble.bpc.";

/// Accumulates a per-table property over the entries written to an sstable.
pub trait BlockPropertyCollector: Send {
    /// The collector's name; the stored property is keyed by it.
    fn name(&self) -> &str;
    /// Feeds one entry (the encoded internal key and its value) to the collector.
    fn add(&mut self, internal_key: &[u8], value: &[u8]);
    /// Serializes the accumulated property for the whole table.
    fn finish(&mut self) -> Vec<u8>;
}

/// Decides, from a table's stored property value, whether the table may contain data the
/// reader is interested in.
pub trait BlockPropertyFilter {
    /// The name of the collector whose property this filter reads.
    fn name(&self) -> &str;
    /// Returns whether the table (whose property is `prop`) may intersect the query, i.e.
    /// whether it must be read. Returning `false` allows the table to be skipped.
    fn intersects(&self, prop: &[u8]) -> bool;
}
