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
    /// Serializes the property for the data block just finished and resets the per-block
    /// accumulator (the per-*table* accumulation is unaffected). Returning an empty vector
    /// means the collector contributes no per-block property. The default opts out of
    /// per-block properties; collectors that support block-level filtering override it.
    fn finish_data_block(&mut self) -> Vec<u8> {
        Vec::new()
    }
}

/// Encodes per-block `(collector-name, property)` pairs for storage trailing a data block's
/// index entry: `uvarint(count)` then, per pair, `uvarint(name_len) name uvarint(prop_len)
/// prop`. An empty slice encodes to empty (nothing is appended to the index entry).
pub(crate) fn encode_block_props(props: &[(String, Vec<u8>)]) -> Vec<u8> {
    use crate::base::varint::put_uvarint;
    if props.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    put_uvarint(&mut out, props.len() as u64);
    for (name, prop) in props {
        put_uvarint(&mut out, name.len() as u64);
        out.extend_from_slice(name.as_bytes());
        put_uvarint(&mut out, prop.len() as u64);
        out.extend_from_slice(prop);
    }
    out
}

/// Decodes per-block properties produced by [`encode_block_props`]. Returns an empty vector
/// for empty input or on any malformation (treated as "no per-block properties").
pub(crate) fn decode_block_props(src: &[u8]) -> Vec<(String, Vec<u8>)> {
    fn parse(src: &[u8]) -> Option<Vec<(String, Vec<u8>)>> {
        use crate::base::varint::get_uvarint;
        let mut p = 0usize;
        let uv = |p: &mut usize| -> Option<u64> {
            let (v, n) = get_uvarint(src.get(*p..)?)?;
            *p += n;
            Some(v)
        };
        let count = uv(&mut p)?;
        let mut out = Vec::new();
        for _ in 0..count {
            let nl = uv(&mut p)? as usize;
            let name = String::from_utf8(src.get(p..p + nl)?.to_vec()).ok()?;
            p += nl;
            let pl = uv(&mut p)? as usize;
            let prop = src.get(p..p + pl)?.to_vec();
            p += pl;
            out.push((name, prop));
        }
        Some(out)
    }
    if src.is_empty() {
        return Vec::new();
    }
    parse(src).unwrap_or_default()
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
