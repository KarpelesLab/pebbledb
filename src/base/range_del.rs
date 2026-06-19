// Copyright (c) 2018 The LevelDB-Go Authors. All rights reserved.
// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Concepts ported from Pebble's internal/rangedel and internal/keyspan.

//! Range tombstones: deletions over a half-open user-key span `[start, end)`.
//!
//! A range tombstone with sequence number `s` deletes every point key in `[start, end)`
//! whose sequence number is less than `s`. Range tombstones are stored separately from
//! point keys — in the memtable's range-del list and in an sstable's range-del block —
//! and applied during reads and compaction.

use crate::base::comparer::Comparer;

/// A range tombstone over `[start, end)` at a sequence number.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeTombstone {
    /// Inclusive start user key.
    pub start: Vec<u8>,
    /// Exclusive end user key.
    pub end: Vec<u8>,
    /// The tombstone's sequence number.
    pub seqnum: u64,
}

impl RangeTombstone {
    /// Creates a range tombstone.
    pub fn new(start: impl Into<Vec<u8>>, end: impl Into<Vec<u8>>, seqnum: u64) -> RangeTombstone {
        RangeTombstone {
            start: start.into(),
            end: end.into(),
            seqnum,
        }
    }

    /// Whether the tombstone covers `user_key` (i.e. `start <= user_key < end`).
    pub fn covers(&self, cmp: &dyn Comparer, user_key: &[u8]) -> bool {
        cmp.compare(&self.start, user_key) != std::cmp::Ordering::Greater
            && cmp.compare(user_key, &self.end) == std::cmp::Ordering::Less
    }
}

/// Returns the largest sequence number among the tombstones in `tombstones` that cover
/// `user_key` and have `seqnum <= snapshot`, or 0 if none cover it.
///
/// Sequence number 0 is reserved (no real key uses it), so 0 unambiguously means "no
/// covering tombstone".
pub fn max_covering_seqnum(
    tombstones: &[RangeTombstone],
    cmp: &dyn Comparer,
    user_key: &[u8],
    snapshot: u64,
) -> u64 {
    let mut max = 0;
    for t in tombstones {
        if t.seqnum <= snapshot && t.seqnum > max && t.covers(cmp, user_key) {
            max = t.seqnum;
        }
    }
    max
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::comparer::DefaultComparer;

    #[test]
    fn covers_half_open() {
        let cmp = DefaultComparer;
        let t = RangeTombstone::new(b"b".to_vec(), b"d".to_vec(), 5);
        assert!(!t.covers(&cmp, b"a"));
        assert!(t.covers(&cmp, b"b")); // inclusive start
        assert!(t.covers(&cmp, b"c"));
        assert!(!t.covers(&cmp, b"d")); // exclusive end
        assert!(!t.covers(&cmp, b"e"));
    }

    #[test]
    fn max_covering_respects_snapshot_and_overlap() {
        let cmp = DefaultComparer;
        let ts = vec![
            RangeTombstone::new(b"a".to_vec(), b"m".to_vec(), 10),
            RangeTombstone::new(b"c".to_vec(), b"f".to_vec(), 25),
            RangeTombstone::new(b"x".to_vec(), b"z".to_vec(), 50),
        ];
        // "d" is covered by seq 10 and 25; newest within snapshot 30 is 25.
        assert_eq!(max_covering_seqnum(&ts, &cmp, b"d", 30), 25);
        // At snapshot 20, the seq-25 tombstone is invisible, so 10.
        assert_eq!(max_covering_seqnum(&ts, &cmp, b"d", 20), 10);
        // "p" is covered by nothing.
        assert_eq!(max_covering_seqnum(&ts, &cmp, b"p", 100), 0);
    }
}
