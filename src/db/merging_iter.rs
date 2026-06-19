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
struct MergingIter {
    sources: Vec<Box<dyn InternalIter>>,
    cmp: Arc<dyn Comparer>,
    /// Index of the source holding the current smallest key, if any.
    cur: Option<usize>,
}

impl MergingIter {
    fn new(mut sources: Vec<Box<dyn InternalIter>>, cmp: Arc<dyn Comparer>) -> Result<MergingIter> {
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

    fn valid(&self) -> bool {
        self.cur.is_some()
    }

    fn key(&self) -> &[u8] {
        self.sources[self.cur.expect("valid")].key()
    }

    fn value(&self) -> &[u8] {
        self.sources[self.cur.expect("valid")].value()
    }

    /// Advances past the current smallest key.
    fn advance(&mut self) -> Result<()> {
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
}

impl DbIterator {
    pub(crate) fn new(
        sources: Vec<Box<dyn InternalIter>>,
        snapshot: SeqNum,
        cmp: Arc<dyn Comparer>,
    ) -> Result<DbIterator> {
        let merge = MergingIter::new(sources, cmp.clone())?;
        Ok(DbIterator {
            merge,
            snapshot,
            cmp,
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
            let mut chosen: Option<(InternalKeyKind, Vec<u8>)> = None;

            // Versions of the same user key arrive newest-first (trailer descending).
            while self.merge.valid()
                && self.cmp.compare(encoded_user_key(self.merge.key()), &ukey)
                    == std::cmp::Ordering::Equal
            {
                if chosen.is_none() {
                    let trailer = encoded_trailer(self.merge.key());
                    if trailer_seqnum(trailer) <= self.snapshot {
                        chosen = Some((trailer_kind(trailer), self.merge.value().to_vec()));
                    }
                }
                self.merge.advance()?;
            }

            match chosen {
                Some((kind, value)) => match kind {
                    InternalKeyKind::Delete
                    | InternalKeyKind::SingleDelete
                    | InternalKeyKind::DeleteSized => continue, // tombstone: skip this key
                    _ => {
                        self.cur_key = ukey;
                        self.cur_value = value;
                        self.valid = true;
                        return Ok(());
                    }
                },
                None => continue, // no version visible at this snapshot
            }
        }
        self.valid = false;
        Ok(())
    }
}
