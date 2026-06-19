// Copyright (c) 2011 The LevelDB-Go Authors. All rights reserved.
// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Ported from Pebble's sstable/tablefilters/bloom (bloom.go, bits.go).

//! Bloom filters for sstables, in RocksDB's cache-line-local full-filter format.
//!
//! A table-level filter is built over the user keys of a table and stored as a block
//! referenced from the metaindex under the key `fullfilter.<policy name>`. The filter
//! lets `Get` skip a table that cannot contain the looked-up key. The bit layout, hash,
//! and `nLines`/`nProbes` derivation match Pebble (and hence RocksDB) exactly, so the
//! filter bytes are interchangeable.

const CACHE_LINE_SIZE: usize = 64;
const CACHE_LINE_BITS: u64 = (CACHE_LINE_SIZE as u64) * 8;

/// Probes per key indexed by bits-per-key (clamped to 10), matching Pebble's table.
const PROBES: [u32; 11] = [0, 1, 1, 2, 3, 3, 4, 4, 5, 5, 6];

fn calculate_probes(bits_per_key: u32) -> u32 {
    PROBES[bits_per_key.min(PROBES.len() as u32 - 1) as usize]
}

/// Hashes `b` with Pebble's Murmur-like hash (seed `0xbc9f1d34`). The trailing bytes are
/// sign-extended through `i8`, matching RocksDB.
pub fn hash(b: &[u8]) -> u32 {
    const SEED: u32 = 0xbc9f_1d34;
    const M: u32 = 0xc6a4_a793;
    let mut h = SEED ^ (b.len() as u32).wrapping_mul(M);
    let mut chunks = b.chunks_exact(4);
    for c in &mut chunks {
        let w =
            u32::from(c[0]) | u32::from(c[1]) << 8 | u32::from(c[2]) << 16 | u32::from(c[3]) << 24;
        h = h.wrapping_add(w);
        h = h.wrapping_mul(M);
        h ^= h >> 16;
    }
    let rem = chunks.remainder();
    match rem.len() {
        3 => {
            h = h.wrapping_add((i32::from(rem[2] as i8) as u32) << 16);
            h = h.wrapping_add((i32::from(rem[1] as i8) as u32) << 8);
            h = h.wrapping_add(i32::from(rem[0] as i8) as u32);
            h = h.wrapping_mul(M);
            h ^= h >> 24;
        }
        2 => {
            h = h.wrapping_add((i32::from(rem[1] as i8) as u32) << 8);
            h = h.wrapping_add(i32::from(rem[0] as i8) as u32);
            h = h.wrapping_mul(M);
            h ^= h >> 24;
        }
        1 => {
            h = h.wrapping_add(i32::from(rem[0] as i8) as u32);
            h = h.wrapping_mul(M);
            h ^= h >> 24;
        }
        _ => {}
    }
    h
}

fn num_lines(num_hashes: u64, bits_per_key: u32) -> u32 {
    let n = (num_hashes * u64::from(bits_per_key)).div_ceil(CACHE_LINE_BITS);
    // Force odd, so more hash bits influence the chosen line.
    (n | 1) as u32
}

/// Sets the `nProbes` bits for hash `h` within its cache line of `filter`.
fn set_bits(filter: &mut [u8], n_lines: u32, n_probes: u32, mut h: u32) {
    let delta = h.rotate_right(17);
    let line = (h % n_lines) as usize * CACHE_LINE_SIZE;
    for _ in 0..n_probes {
        let byte = line + ((h >> 3) as usize & (CACHE_LINE_SIZE - 1));
        filter[byte] |= 1 << (h & 7);
        h = h.wrapping_add(delta);
    }
}

/// Builds the filter bytes for the given key hashes and bits-per-key, or `None` if there
/// are no keys. The format is `nLines * 64` bytes of bits, then a probe-count byte, then
/// the line count as a little-endian `u32`.
pub fn build(hashes: &[u32], bits_per_key: u32) -> Option<Vec<u8>> {
    if hashes.is_empty() {
        return None;
    }
    let n_probes = calculate_probes(bits_per_key);
    let n_lines = num_lines(hashes.len() as u64, bits_per_key);
    let n_bytes = n_lines as usize * CACHE_LINE_SIZE;
    let mut filter = vec![0u8; n_bytes + 5];
    for &h in hashes {
        set_bits(&mut filter, n_lines, n_probes, h);
    }
    filter[n_bytes] = n_probes as u8;
    filter[n_bytes + 1..n_bytes + 5].copy_from_slice(&n_lines.to_le_bytes());
    Some(filter)
}

/// Reports whether `key` may be present according to `filter` (built by [`build`]).
/// `false` is definitive; `true` may be a false positive.
pub fn may_contain(filter: &[u8], key: &[u8]) -> bool {
    if filter.len() <= 5 {
        return false;
    }
    let n = filter.len() - 5;
    let n_probes = u32::from(filter[n]);
    let n_lines = u32::from_le_bytes(filter[n + 1..n + 5].try_into().unwrap());
    if n_lines == 0 || 8 * (n as u64 / u64::from(n_lines)) != CACHE_LINE_BITS {
        // Unrecognized layout (e.g. a newer encoding): conservatively assume present.
        return true;
    }
    let mut h = hash(key);
    let delta = h.rotate_right(17);
    let line = (h % n_lines) as usize * CACHE_LINE_SIZE;
    for _ in 0..n_probes {
        let byte = line + ((h >> 3) as usize & (CACHE_LINE_SIZE - 1));
        if filter[byte] & (1 << (h & 7)) == 0 {
            return false;
        }
        h = h.wrapping_add(delta);
    }
    true
}

/// The filter-policy name for `bits_per_key` (`rocksdb.BuiltinBloomFilter` for 10).
pub fn policy_name(bits_per_key: u32) -> String {
    if bits_per_key == 10 {
        "rocksdb.BuiltinBloomFilter".to_string()
    } else {
        format!("bloom({bits_per_key})")
    }
}

/// The metaindex key under which a filter with the given policy name is stored.
pub fn metaindex_key(policy_name: &str) -> String {
    format!("fullfilter.{policy_name}")
}

/// Accumulates key hashes while a table is written, then builds the filter.
pub struct FilterWriter {
    bits_per_key: u32,
    hashes: Vec<u32>,
    last_key: Vec<u8>,
    has_last: bool,
}

impl FilterWriter {
    /// Creates a writer for the given bits-per-key.
    pub fn new(bits_per_key: u32) -> FilterWriter {
        FilterWriter {
            bits_per_key,
            hashes: Vec::new(),
            last_key: Vec::new(),
            has_last: false,
        }
    }

    /// Adds a user key, skipping consecutive duplicates.
    pub fn add_key(&mut self, key: &[u8]) {
        if self.has_last && self.last_key == key {
            return;
        }
        self.hashes.push(hash(key));
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.has_last = true;
    }

    /// The policy name for this writer.
    pub fn policy_name(&self) -> String {
        policy_name(self.bits_per_key)
    }

    /// Builds the filter bytes, or `None` if no keys were added.
    pub fn finish(&self) -> Option<Vec<u8>> {
        build(&self.hashes, self.bits_per_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn present_keys_always_match() {
        let mut w = FilterWriter::new(10);
        let keys: Vec<String> = (0..1000).map(|i| format!("key-{i:05}")).collect();
        for k in &keys {
            w.add_key(k.as_bytes());
        }
        let filter = w.finish().unwrap();
        // No false negatives: every inserted key must report present.
        for k in &keys {
            assert!(may_contain(&filter, k.as_bytes()), "missing {k}");
        }
    }

    #[test]
    fn absent_keys_mostly_rejected() {
        let mut w = FilterWriter::new(10);
        for i in 0..1000 {
            w.add_key(format!("present-{i:05}").as_bytes());
        }
        let filter = w.finish().unwrap();
        let mut false_positives = 0;
        for i in 0..10_000 {
            if may_contain(&filter, format!("absent-{i:05}").as_bytes()) {
                false_positives += 1;
            }
        }
        // ~1% expected at 10 bits/key; allow generous headroom.
        assert!(
            false_positives < 500,
            "too many false positives: {false_positives}"
        );
    }

    #[test]
    fn empty_filter() {
        assert!(FilterWriter::new(10).finish().is_none());
        assert!(!may_contain(&[], b"x"));
    }

    #[test]
    fn policy_and_metaindex_names() {
        assert_eq!(policy_name(10), "rocksdb.BuiltinBloomFilter");
        assert_eq!(policy_name(20), "bloom(20)");
        assert_eq!(
            metaindex_key(&policy_name(10)),
            "fullfilter.rocksdb.BuiltinBloomFilter"
        );
    }

    #[test]
    fn duplicate_keys_deduped() {
        let mut w = FilterWriter::new(10);
        w.add_key(b"a");
        w.add_key(b"a");
        w.add_key(b"b");
        // Two unique keys -> two hashes.
        assert_eq!(w.hashes.len(), 2);
    }
}
