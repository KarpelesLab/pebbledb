// Copyright (c) 2012 The LevelDB-Go Authors. All rights reserved.
// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Ported from Pebble's merging_iter.go and db_iter.go (forward path).

//! A forward, snapshot-consistent iterator over the merged contents of many sstables.
//!
//! [`MergingIter`] interleaves several [`TableIter`]s, always exposing the smallest
//! internal key across them. [`DbIterator`] sits on top, collapsing the multiple
//! internal-key versions of each user key down to the single newest one visible at a
//! snapshot, and hiding tombstones.

use std::sync::Arc;

use crate::Result;
use crate::base::comparer::Comparer;
use crate::base::internal_key::{
    InternalKeyKind, SeqNum, compare_encoded, encoded_trailer, encoded_user_key, trailer_kind,
    trailer_seqnum,
};
use crate::base::range_del::{RangeTombstone, max_covering_seqnum};
use crate::memtable::OwnedMemIter;
use crate::sstable::TableIter;

/// A forward iterator over a sorted stream of encoded internal keys. Implemented by both
/// sstable and memtable iterators so a [`MergingIter`] can interleave them.
pub(crate) trait InternalIter {
    fn first(&mut self) -> Result<()>;
    fn valid(&self) -> bool;
    fn key(&self) -> &[u8];
    fn value(&self) -> &[u8];
    fn advance(&mut self) -> Result<()>;
}

impl InternalIter for TableIter {
    fn first(&mut self) -> Result<()> {
        TableIter::first(self).map(|_| ())
    }
    fn valid(&self) -> bool {
        TableIter::valid(self)
    }
    fn key(&self) -> &[u8] {
        TableIter::key(self)
    }
    fn value(&self) -> &[u8] {
        TableIter::value(self)
    }
    fn advance(&mut self) -> Result<()> {
        TableIter::next(self).map(|_| ())
    }
}

impl InternalIter for OwnedMemIter {
    fn first(&mut self) -> Result<()> {
        OwnedMemIter::first(self);
        Ok(())
    }
    fn valid(&self) -> bool {
        OwnedMemIter::valid(self)
    }
    fn key(&self) -> &[u8] {
        OwnedMemIter::key(self)
    }
    fn value(&self) -> &[u8] {
        OwnedMemIter::value(self)
    }
    fn advance(&mut self) -> Result<()> {
        OwnedMemIter::next(self);
        Ok(())
    }
}

/// Interleaves several internal iterators, exposing the globally smallest internal key.
///
/// Used both by [`DbIterator`] (with snapshot collapsing on top) and directly by
/// compaction (which keeps the newest version of each user key).
pub(crate) struct MergingIter {
    sources: Vec<Box<dyn InternalIter>>,
    cmp: Arc<dyn Comparer>,
    /// Index of the source holding the current smallest key, if any.
    cur: Option<usize>,
}

impl MergingIter {
    pub(crate) fn new(
        mut sources: Vec<Box<dyn InternalIter>>,
        cmp: Arc<dyn Comparer>,
    ) -> Result<MergingIter> {
        for s in &mut sources {
            s.first()?;
        }
        let mut m = MergingIter {
            sources,
            cmp,
            cur: None,
        };
        m.refresh_min();
        Ok(m)
    }

    /// Recomputes which source holds the smallest current internal key.
    fn refresh_min(&mut self) {
        let mut best: Option<usize> = None;
        for (i, s) in self.sources.iter().enumerate() {
            if !s.valid() {
                continue;
            }
            match best {
                None => best = Some(i),
                Some(b) => {
                    if compare_encoded(self.cmp.as_ref(), s.key(), self.sources[b].key())
                        == std::cmp::Ordering::Less
                    {
                        best = Some(i);
                    }
                }
            }
        }
        self.cur = best;
    }

    pub(crate) fn valid(&self) -> bool {
        self.cur.is_some()
    }

    pub(crate) fn key(&self) -> &[u8] {
        self.sources[self.cur.expect("valid")].key()
    }

    pub(crate) fn value(&self) -> &[u8] {
        self.sources[self.cur.expect("valid")].value()
    }

    /// Advances past the current smallest key.
    pub(crate) fn advance(&mut self) -> Result<()> {
        let i = self.cur.expect("valid");
        self.sources[i].advance()?;
        self.refresh_min();
        Ok(())
    }
}

/// A forward iterator over a database's user keys at a fixed snapshot.
pub struct DbIterator {
    merge: MergingIter,
    snapshot: SeqNum,
    cmp: Arc<dyn Comparer>,
    cur_key: Vec<u8>,
    cur_value: Vec<u8>,
    valid: bool,
    started: bool,
    /// All range tombstones visible at the snapshot, across every source.
    range_tombstones: Vec<RangeTombstone>,
    /// Optional merge operator used to resolve merge operands.
    merger: Option<Arc<dyn crate::base::merge::Merger>>,
}

impl DbIterator {
    pub(crate) fn new(
        sources: Vec<Box<dyn InternalIter>>,
        snapshot: SeqNum,
        cmp: Arc<dyn Comparer>,
        range_tombstones: Vec<RangeTombstone>,
        merger: Option<Arc<dyn crate::base::merge::Merger>>,
    ) -> Result<DbIterator> {
        let merge = MergingIter::new(sources, cmp.clone())?;
        Ok(DbIterator {
            merge,
            snapshot,
            cmp,
            merger,
            range_tombstones,
            cur_key: Vec::new(),
            cur_value: Vec::new(),
            valid: false,
            started: false,
        })
    }

    /// Positions at the first visible key. Idempotent before the first [`next`](Self::next).
    pub fn first(&mut self) -> Result<()> {
        if !self.started {
            self.started = true;
            self.advance_to_next_user_key()?;
        }
        Ok(())
    }

    /// Whether the iterator is positioned at a valid key.
    pub fn valid(&self) -> bool {
        self.valid
    }

    /// The current user key.
    pub fn key(&self) -> &[u8] {
        debug_assert!(self.valid);
        &self.cur_key
    }

    /// The current value.
    pub fn value(&self) -> &[u8] {
        debug_assert!(self.valid);
        &self.cur_value
    }

    /// Advances to the next visible key.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<()> {
        if !self.started {
            return self.first();
        }
        self.advance_to_next_user_key()
    }

    /// Walks the merged stream, collapsing each user key to its newest version visible at
    /// the snapshot and skipping tombstones, until a visible value is found or the input
    /// is exhausted.
    fn advance_to_next_user_key(&mut self) -> Result<()> {
        while self.merge.valid() {
            let ukey = encoded_user_key(self.merge.key()).to_vec();
            // A range tombstone covering the key terminates merges/values at its seqnum.
            let max_rts = max_covering_seqnum(
                &self.range_tombstones,
                self.cmp.as_ref(),
                &ukey,
                self.snapshot,
            );

            // Gather merge operands (newest first) until a terminator: a Set (base
            // value), a deletion, or a covering range tombstone.
            let mut operands: Vec<Vec<u8>> = Vec::new();
            let mut base: Option<Vec<u8>> = None;
            let mut decided = false;

            while self.merge.valid()
                && self.cmp.compare(encoded_user_key(self.merge.key()), &ukey)
                    == std::cmp::Ordering::Equal
            {
                if !decided {
                    let trailer = encoded_trailer(self.merge.key());
                    let seq = trailer_seqnum(trailer);
                    if seq <= self.snapshot {
                        if seq <= max_rts {
                            decided = true; // deleted by a range tombstone
                        } else {
                            match trailer_kind(trailer) {
                                InternalKeyKind::Merge => {
                                    operands.push(self.merge.value().to_vec())
                                }
                                InternalKeyKind::Set | InternalKeyKind::SetWithDelete => {
                                    base = Some(self.merge.value().to_vec());
                                    decided = true;
                                }
                                InternalKeyKind::Delete
                                | InternalKeyKind::SingleDelete
                                | InternalKeyKind::DeleteSized => decided = true,
                                _ => {}
                            }
                        }
                    }
                }
                self.merge.advance()?;
            }

            let value = if operands.is_empty() {
                match base {
                    Some(v) => v,
                    None => continue, // deleted or no visible version
                }
            } else {
                operands.reverse(); // chronological (oldest first)
                match &self.merger {
                    Some(m) => m.full_merge(&ukey, base.as_deref(), &operands),
                    None => operands.pop().unwrap(), // no merger: newest operand
                }
            };
            self.cur_key = ukey;
            self.cur_value = value;
            self.valid = true;
            return Ok(());
        }
        self.valid = false;
        Ok(())
    }
}
