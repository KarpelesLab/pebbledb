// Copyright (c) 2011 The LevelDB-Go Authors. All rights reserved.
// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Ported from Pebble's internal/base/comparer.go.

//! Key comparison.
//!
//! A [`Comparer`] defines the total order over user keys used throughout the engine,
//! along with the auxiliary operations Pebble needs to build compact sstable index
//! keys ([`Comparer::separator`] / [`Comparer::successor`]) and to accelerate
//! comparisons ([`Comparer::abbreviated_key`]).
//!
//! [`DefaultComparer`] is the bytewise comparer named `leveldb.BytewiseComparator`;
//! it is byte-for-byte compatible with LevelDB / RocksDB / Pebble defaults.

use std::cmp::Ordering;

/// Defines the ordering over user keys and the operations derived from it.
///
/// This mirrors Pebble's `base.Comparer`. It is object-safe, so a database can hold an
/// `Arc<dyn Comparer>` selected by [`Comparer::name`] at open time.
///
/// All `dst`-style methods *append* to `dst` rather than overwriting it, matching the
/// upstream Go convention of passing a reusable buffer.
pub trait Comparer: Send + Sync {
    /// The name of the comparer.
    ///
    /// The name is persisted in the sstable/MANIFEST and checked on open: a database
    /// written with one comparer cannot be read with a differently-named one. For the
    /// bytewise comparer this is `"leveldb.BytewiseComparator"` and must not change.
    fn name(&self) -> &str;

    /// Returns the ordering of `a` relative to `b`.
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering;

    /// Returns whether `a` and `b` are equal. Equivalent to `compare(a, b).is_eq()` but
    /// may be implemented more cheaply.
    fn equal(&self, a: &[u8], b: &[u8]) -> bool {
        self.compare(a, b) == Ordering::Equal
    }

    /// Returns a fixed-length, order-preserving-ish prefix of `key` as a `u64`, used to
    /// speed up comparisons (e.g. in the merging iterator heap). It is *not* required to
    /// be a perfect order embedding: if `compare(a, b) < 0` then
    /// `abbreviated_key(a) <= abbreviated_key(b)`, and the full keys are consulted on
    /// ties.
    fn abbreviated_key(&self, key: &[u8]) -> u64;

    /// Splits `key` into a prefix and a suffix, returning the length of the prefix. For
    /// the default comparer there is no suffix, so this returns `key.len()`.
    fn split(&self, key: &[u8]) -> usize {
        key.len()
    }

    /// Appends to `dst` a key in the range `[a, b)` that is `>= a` and `< b`, preferring
    /// the shortest such key. Requires `a < b`. Used to shorten sstable index keys.
    fn separator(&self, dst: &mut Vec<u8>, a: &[u8], b: &[u8]);

    /// Appends to `dst` a key `>= a`, preferring the shortest such key. Used to shorten
    /// the largest key of an sstable.
    fn successor(&self, dst: &mut Vec<u8>, a: &[u8]);

    /// Appends to `dst` the immediate successor of `a`: the smallest key strictly
    /// greater than `a` such that there is no possible key between them. The default is
    /// `a` followed by a zero byte.
    fn immediate_successor(&self, dst: &mut Vec<u8>, a: &[u8]) {
        dst.extend_from_slice(a);
        dst.push(0x00);
    }
}

/// The length of the longest prefix common to `a` and `b`.
fn common_prefix(a: &[u8], b: &[u8]) -> usize {
    let n = a.len().min(b.len());
    let mut i = 0;
    while i < n && a[i] == b[i] {
        i += 1;
    }
    i
}

/// The bytewise comparer, named `leveldb.BytewiseComparator`.
///
/// Keys are ordered lexicographically by their raw bytes — identical to Rust's slice
/// ordering, to Go's `bytes.Compare`, and to the LevelDB/RocksDB default.
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultComparer;

impl Comparer for DefaultComparer {
    fn name(&self) -> &str {
        // This name is part of the C++ LevelDB implementation's default file format and
        // must not be changed.
        "leveldb.BytewiseComparator"
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }

    fn equal(&self, a: &[u8], b: &[u8]) -> bool {
        a == b
    }

    fn abbreviated_key(&self, key: &[u8]) -> u64 {
        if key.len() >= 8 {
            u64::from_be_bytes(key[..8].try_into().unwrap())
        } else if key.is_empty() {
            // Avoid a `<< 64` shift overflow; the abbreviation of the empty key is 0.
            0
        } else {
            let mut v: u64 = 0;
            for &b in key {
                v <<= 8;
                v |= u64::from(b);
            }
            // key.len() is in 1..=7, so the shift count is in 8..=56 and cannot overflow.
            v << (8 * (8 - key.len() as u32))
        }
    }

    fn separator(&self, dst: &mut Vec<u8>, a: &[u8], b: &[u8]) {
        assert!(!a.is_empty() && !b.is_empty(), "empty keys");

        let i = common_prefix(a, b);
        let n = dst.len();
        dst.extend_from_slice(a);

        if i == a.len() || i == b.len() {
            // Do not shorten if one string is a prefix of the other.
            return;
        }
        if a[i] >= b[i] {
            // b is smaller than a, or a is already the shortest possible.
            return;
        }
        // a[i] < b[i] from here, so a[i] <= 0xfe and the increments below cannot wrap.
        if i < b.len() - 1 || a[i] + 1 < b[i] {
            let idx = i + n;
            dst[idx] += 1;
            dst.truncate(idx + 1);
            return;
        }
        // Otherwise increment the first non-0xff byte after position i.
        let mut idx = i + n + 1;
        while idx < dst.len() {
            if dst[idx] != 0xff {
                dst[idx] += 1;
                dst.truncate(idx + 1);
                return;
            }
            idx += 1;
        }
        // a is all 0xff after the common prefix; leave the full copy of a in dst.
    }

    fn successor(&self, dst: &mut Vec<u8>, a: &[u8]) {
        for i in 0..a.len() {
            if a[i] != 0xff {
                dst.extend_from_slice(&a[..=i]);
                let last = dst.len() - 1;
                dst[last] += 1;
                return;
            }
        }
        // a is a run of 0xffs; leave it alone.
        dst.extend_from_slice(a);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sep(a: &[u8], b: &[u8]) -> Vec<u8> {
        let mut dst = Vec::new();
        DefaultComparer.separator(&mut dst, a, b);
        dst
    }

    fn succ(a: &[u8]) -> Vec<u8> {
        let mut dst = Vec::new();
        DefaultComparer.successor(&mut dst, a);
        dst
    }

    #[test]
    fn name_is_stable() {
        assert_eq!(DefaultComparer.name(), "leveldb.BytewiseComparator");
    }

    #[test]
    fn compare_is_bytewise() {
        assert_eq!(DefaultComparer.compare(b"a", b"b"), Ordering::Less);
        assert_eq!(DefaultComparer.compare(b"abc", b"abc"), Ordering::Equal);
        assert_eq!(DefaultComparer.compare(b"ab", b"a"), Ordering::Greater);
        assert!(DefaultComparer.equal(b"x", b"x"));
        assert!(!DefaultComparer.equal(b"x", b"y"));
    }

    #[test]
    fn abbreviated_key_matches_pebble() {
        // >= 8 bytes: big-endian of the first 8 bytes.
        assert_eq!(
            DefaultComparer.abbreviated_key(b"abcdefghij"),
            u64::from_be_bytes(*b"abcdefgh")
        );
        // < 8 bytes: big-endian, left-justified (shifted up).
        assert_eq!(
            DefaultComparer.abbreviated_key(b"ab"),
            0x6162_0000_0000_0000
        );
        assert_eq!(DefaultComparer.abbreviated_key(b""), 0);
        // Order is preserved on the abbreviation.
        assert!(DefaultComparer.abbreviated_key(b"a") < DefaultComparer.abbreviated_key(b"b"));
    }

    #[test]
    fn separator_examples() {
        // Differ before the end: bump the first distinguishing byte of a and truncate.
        assert_eq!(sep(b"abc", b"acc"), b"ac".to_vec());
        // b is the immediate +1 of a at the last position: cannot shorten that way.
        assert_eq!(sep(b"abc", b"abd"), b"abc".to_vec());
        // One is a prefix of the other: cannot shorten.
        assert_eq!(sep(b"ab", b"abc"), b"ab".to_vec());
        assert_eq!(sep(b"abc", b"ab"), b"abc".to_vec());
        // When the distinguishing byte can't be bumped (b is exactly a[i]+1 at b's last
        // byte), the next non-0xff byte is bumped, skipping any 0xff run.
        assert_eq!(sep(b"x\xff\xffm", b"y"), b"x\xff\xffn".to_vec());
    }

    #[test]
    fn separator_appends_to_existing_dst() {
        let mut dst = b"PRE".to_vec();
        DefaultComparer.separator(&mut dst, b"abc", b"acc");
        assert_eq!(dst, b"PREac".to_vec());
    }

    #[test]
    fn successor_examples() {
        assert_eq!(succ(b"abc"), b"b".to_vec());
        assert_eq!(succ(b"\xff\xffx"), b"\xff\xffy".to_vec());
        // All 0xff: unchanged.
        assert_eq!(succ(b"\xff\xff"), b"\xff\xff".to_vec());
    }

    #[test]
    fn immediate_successor_appends_zero() {
        let mut dst = Vec::new();
        DefaultComparer.immediate_successor(&mut dst, b"abc");
        assert_eq!(dst, b"abc\x00".to_vec());
    }
}
