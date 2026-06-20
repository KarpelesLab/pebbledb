// Copyright (c) 2012 The LevelDB-Go Authors. All rights reserved.
// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Ported from Pebble's merging_iter.go and db_iter.go.

//! A bidirectional, snapshot-consistent iterator over the merged contents of many
//! sstables and memtables.
//!
//! [`MergingIter`] interleaves several internal iterators, exposing the globally smallest
//! key going forward or the largest going reverse, and re-seeks the other sources when the
//! direction flips. [`DbIterator`] sits on top, collapsing the multiple internal-key
//! versions of each user key down to the single newest one visible at a snapshot, hiding
//! tombstones, applying range-tombstone shadowing and the merge operator, and honoring
//! key bounds and prefix restrictions.

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

/// A bidirectional, seekable iterator over a sorted stream of encoded internal keys.
/// Implemented by both sstable and memtable iterators so a [`MergingIter`] can interleave
/// them in either direction.
pub(crate) trait InternalIter {
    fn first(&mut self) -> Result<()>;
    fn last(&mut self) -> Result<()>;
    fn seek_ge(&mut self, target: &[u8]) -> Result<()>;
    fn seek_lt(&mut self, target: &[u8]) -> Result<()>;
    fn valid(&self) -> bool;
    fn key(&self) -> &[u8];
    fn value(&self) -> &[u8];
    fn advance(&mut self) -> Result<()>;
    fn retreat(&mut self) -> Result<()>;
}

impl InternalIter for TableIter {
    fn first(&mut self) -> Result<()> {
        TableIter::first(self).map(|_| ())
    }
    fn last(&mut self) -> Result<()> {
        TableIter::last(self).map(|_| ())
    }
    fn seek_ge(&mut self, target: &[u8]) -> Result<()> {
        TableIter::seek_ge(self, target).map(|_| ())
    }
    fn seek_lt(&mut self, target: &[u8]) -> Result<()> {
        TableIter::seek_lt(self, target).map(|_| ())
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
    fn retreat(&mut self) -> Result<()> {
        TableIter::prev(self).map(|_| ())
    }
}

impl InternalIter for OwnedMemIter {
    fn first(&mut self) -> Result<()> {
        OwnedMemIter::first(self);
        Ok(())
    }
    fn last(&mut self) -> Result<()> {
        OwnedMemIter::last(self);
        Ok(())
    }
    fn seek_ge(&mut self, target: &[u8]) -> Result<()> {
        OwnedMemIter::seek_ge(self, target);
        Ok(())
    }
    fn seek_lt(&mut self, target: &[u8]) -> Result<()> {
        OwnedMemIter::seek_lt(self, target);
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
    fn retreat(&mut self) -> Result<()> {
        OwnedMemIter::prev(self);
        Ok(())
    }
}

/// Interleaves several internal iterators, exposing the globally smallest internal key.
///
/// Used both by [`DbIterator`] (with snapshot collapsing on top) and directly by
/// compaction (which keeps the newest version of each user key).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Dir {
    Forward,
    Reverse,
}

pub(crate) struct MergingIter {
    sources: Vec<Box<dyn InternalIter>>,
    cmp: Arc<dyn Comparer>,
    /// Index of the source holding the current key (smallest going forward, largest going
    /// reverse), if any.
    cur: Option<usize>,
    dir: Dir,
}

impl MergingIter {
    pub(crate) fn new(
        sources: Vec<Box<dyn InternalIter>>,
        cmp: Arc<dyn Comparer>,
    ) -> Result<MergingIter> {
        let mut m = MergingIter {
            sources,
            cmp,
            cur: None,
            dir: Dir::Forward,
        };
        m.first()?;
        Ok(m)
    }

    /// Positions every source at its first entry and selects the global minimum.
    pub(crate) fn first(&mut self) -> Result<()> {
        for s in &mut self.sources {
            s.first()?;
        }
        self.dir = Dir::Forward;
        self.refresh_min();
        Ok(())
    }

    /// Positions every source at its last entry and selects the global maximum.
    pub(crate) fn last(&mut self) -> Result<()> {
        for s in &mut self.sources {
            s.last()?;
        }
        self.dir = Dir::Reverse;
        self.refresh_max();
        Ok(())
    }

    /// Seeks every source to the first key `>= target` and selects the global minimum.
    pub(crate) fn seek_ge(&mut self, target: &[u8]) -> Result<()> {
        for s in &mut self.sources {
            s.seek_ge(target)?;
        }
        self.dir = Dir::Forward;
        self.refresh_min();
        Ok(())
    }

    /// Seeks every source to the last key `< target` and selects the global maximum.
    pub(crate) fn seek_lt(&mut self, target: &[u8]) -> Result<()> {
        for s in &mut self.sources {
            s.seek_lt(target)?;
        }
        self.dir = Dir::Reverse;
        self.refresh_max();
        Ok(())
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

    /// Recomputes which source holds the largest current internal key.
    fn refresh_max(&mut self) {
        let mut best: Option<usize> = None;
        for (i, s) in self.sources.iter().enumerate() {
            if !s.valid() {
                continue;
            }
            match best {
                None => best = Some(i),
                Some(b) => {
                    if compare_encoded(self.cmp.as_ref(), s.key(), self.sources[b].key())
                        == std::cmp::Ordering::Greater
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

    /// Advances past the current smallest key. Switches direction first if needed: every
    /// non-current source (which, going reverse, sits at a key `<` the current) is
    /// re-seeked to the first key `>` the current.
    pub(crate) fn advance(&mut self) -> Result<()> {
        if self.dir != Dir::Forward {
            let key = self.sources[self.cur.expect("valid")].key().to_vec();
            for (i, s) in self.sources.iter_mut().enumerate() {
                if Some(i) == self.cur {
                    continue;
                }
                // Move to the first key strictly greater than `key`.
                s.seek_ge(&key)?;
                if s.valid() && compare_encoded(self.cmp.as_ref(), s.key(), &key).is_eq() {
                    s.advance()?;
                }
            }
            self.dir = Dir::Forward;
        }
        let i = self.cur.expect("valid");
        self.sources[i].advance()?;
        self.refresh_min();
        Ok(())
    }

    /// Steps back past the current largest key. Switches direction first if needed: every
    /// non-current source (which, going forward, sits at a key `>` the current) is
    /// re-seeked to the last key `<` the current.
    pub(crate) fn retreat(&mut self) -> Result<()> {
        if self.dir != Dir::Reverse {
            let key = self.sources[self.cur.expect("valid")].key().to_vec();
            for (i, s) in self.sources.iter_mut().enumerate() {
                if Some(i) == self.cur {
                    continue;
                }
                // Move to the last key strictly less than `key`.
                s.seek_lt(&key)?;
            }
            self.dir = Dir::Reverse;
        }
        let i = self.cur.expect("valid");
        self.sources[i].retreat()?;
        self.refresh_max();
        Ok(())
    }
}

/// Bounds and key-type selection for a [`DbIterator`].
#[derive(Clone, Default)]
pub struct IterOptions {
    /// Inclusive lower bound on user keys; keys below it are not produced.
    pub lower_bound: Option<Vec<u8>>,
    /// Exclusive upper bound on user keys; keys at or above it are not produced.
    pub upper_bound: Option<Vec<u8>>,
}

/// A bidirectional iterator over a database's user keys at a fixed snapshot.
///
/// Collapses the multiple internal-key versions of each user key down to the single
/// newest one visible at the snapshot, hides tombstones, applies range-tombstone
/// shadowing and the merge operator, and honors the configured key bounds.
pub struct DbIterator {
    merge: MergingIter,
    snapshot: SeqNum,
    cmp: Arc<dyn Comparer>,
    cur_key: Vec<u8>,
    cur_value: Vec<u8>,
    valid: bool,
    dir: Dir,
    /// All range tombstones visible at the snapshot, across every source.
    range_tombstones: Vec<RangeTombstone>,
    /// All range-key entries visible at the snapshot, across every source. Surfaced at the
    /// current position via [`DbIterator::range_keys`].
    range_keys: Vec<crate::base::range_key::RangeKeyEntry>,
    /// Optional merge operator used to resolve merge operands.
    merger: Option<Arc<dyn crate::base::merge::Merger>>,
    lower_bound: Option<Vec<u8>>,
    upper_bound: Option<Vec<u8>>,
    /// When set, iteration is restricted to keys having this prefix.
    prefix: Option<Vec<u8>>,
}

impl DbIterator {
    pub(crate) fn with_options(
        sources: Vec<Box<dyn InternalIter>>,
        snapshot: SeqNum,
        cmp: Arc<dyn Comparer>,
        range_tombstones: Vec<RangeTombstone>,
        range_keys: Vec<crate::base::range_key::RangeKeyEntry>,
        merger: Option<Arc<dyn crate::base::merge::Merger>>,
        opts: IterOptions,
    ) -> Result<DbIterator> {
        let merge = MergingIter::new(sources, cmp.clone())?;
        Ok(DbIterator {
            merge,
            snapshot,
            cmp,
            merger,
            range_tombstones,
            range_keys,
            cur_key: Vec::new(),
            cur_value: Vec::new(),
            valid: false,
            dir: Dir::Forward,
            lower_bound: opts.lower_bound,
            upper_bound: opts.upper_bound,
            prefix: None,
        })
    }

    /// The range-key entries covering the current position, newest first, visible at the
    /// iterator's snapshot. Empty when not positioned at a valid key or when no range keys
    /// cover it. (Surfacing range keys alongside points; full `RANGEKEYSET`/`UNSET`/`DEL`
    /// coalescing and masking is layered on top of these raw entries.)
    pub fn range_keys(&self) -> Vec<crate::base::range_key::RangeKeyEntry> {
        if !self.valid {
            return Vec::new();
        }
        self.range_keys
            .iter()
            .filter(|e| {
                e.seqnum <= self.snapshot
                    && e.covers(self.cmp.as_ref(), &self.cur_key).unwrap_or(false)
            })
            .cloned()
            .collect()
    }

    /// The *effective* range keys at the current position: the covering
    /// `RANGEKEYSET`/`UNSET`/`DEL` entries coalesced into the set of `(suffix, value)`
    /// pairs in force, sorted by suffix. Newer entries win; a `RANGEKEYUNSET` removes a
    /// suffix and a `RANGEKEYDEL` removes everything older. (Pebble's range-key
    /// coalescing, on top of the raw entries from [`DbIterator::range_keys`].)
    pub fn coalesced_range_keys(&self) -> Vec<crate::base::range_key::SuffixValue> {
        use crate::base::internal_key::InternalKeyKind;
        use crate::base::range_key::{decode_end, decode_set_suffix_values, decode_unset_suffixes};
        use std::collections::BTreeMap;

        // Covering entries, newest sequence number first.
        let mut covering = self.range_keys();
        covering.sort_by(|a, b| b.seqnum.cmp(&a.seqnum));

        let mut decided: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
        let mut set: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        for e in &covering {
            match e.kind {
                InternalKeyKind::RangeKeyDelete => break, // everything older is deleted
                InternalKeyKind::RangeKeySet => {
                    let Ok((_, payload)) = decode_end(e.kind, &e.value) else {
                        continue;
                    };
                    let Ok(svs) = decode_set_suffix_values(payload) else {
                        continue;
                    };
                    for sv in svs {
                        if decided.insert(sv.suffix.clone()) {
                            set.insert(sv.suffix, sv.value);
                        }
                    }
                }
                InternalKeyKind::RangeKeyUnset => {
                    let Ok((_, payload)) = decode_end(e.kind, &e.value) else {
                        continue;
                    };
                    let Ok(suffixes) = decode_unset_suffixes(payload) else {
                        continue;
                    };
                    for s in suffixes {
                        decided.insert(s); // shadows older SETs for this suffix
                    }
                }
                _ => {}
            }
        }
        set.into_iter()
            .map(|(suffix, value)| crate::base::range_key::SuffixValue { suffix, value })
            .collect()
    }

    /// Whether the iterator is positioned at a valid key.
    pub fn valid(&self) -> bool {
        self.valid
    }

    /// Replaces the iterator's key bounds (inclusive lower, exclusive upper). This
    /// invalidates the current position (Pebble's `SetBounds` semantics): the caller must
    /// re-seek with [`first`](Self::first) / [`last`](Self::last) / [`seek_ge`](Self::seek_ge)
    /// before reading again. Reusing an iterator with new bounds avoids rebuilding the
    /// underlying merge.
    pub fn set_bounds(&mut self, lower: Option<Vec<u8>>, upper: Option<Vec<u8>>) {
        self.lower_bound = lower;
        self.upper_bound = upper;
        self.valid = false;
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

    fn below_lower(&self, ukey: &[u8]) -> bool {
        self.lower_bound
            .as_deref()
            .is_some_and(|lb| self.cmp.compare(ukey, lb) == std::cmp::Ordering::Less)
    }

    fn at_or_above_upper(&self, ukey: &[u8]) -> bool {
        self.upper_bound
            .as_deref()
            .is_some_and(|ub| self.cmp.compare(ukey, ub) != std::cmp::Ordering::Less)
    }

    /// The largest internal-key trailer (smallest sort) for a user key.
    fn key_with_max_trailer(&self, ukey: &[u8]) -> Vec<u8> {
        let mut k = ukey.to_vec();
        k.extend_from_slice(&u64::MAX.to_le_bytes());
        k
    }

    /// The smallest internal-key trailer (largest sort) for a user key.
    fn key_with_min_trailer(&self, ukey: &[u8]) -> Vec<u8> {
        let mut k = ukey.to_vec();
        k.extend_from_slice(&0u64.to_le_bytes());
        k
    }

    /// Positions at the first visible key (honoring the lower bound).
    pub fn first(&mut self) -> Result<()> {
        self.prefix = None;
        match &self.lower_bound {
            Some(lb) => {
                let target = self.key_with_max_trailer(&lb.clone());
                self.merge.seek_ge(&target)?;
            }
            None => self.merge.first()?,
        }
        self.dir = Dir::Forward;
        self.advance_to_next_user_key()
    }

    /// Positions at the last visible key (honoring the upper bound).
    pub fn last(&mut self) -> Result<()> {
        self.prefix = None;
        match &self.upper_bound {
            Some(ub) => {
                let target = self.key_with_max_trailer(&ub.clone());
                self.merge.seek_lt(&target)?;
            }
            None => self.merge.last()?,
        }
        self.dir = Dir::Reverse;
        self.advance_to_prev_user_key()
    }

    /// Positions at the first visible key `>= target` (clamped to the lower bound).
    pub fn seek_ge(&mut self, target: &[u8]) -> Result<()> {
        self.prefix = None;
        let key = if self.below_lower(target) {
            self.lower_bound.clone().unwrap()
        } else {
            target.to_vec()
        };
        let ikey = self.key_with_max_trailer(&key);
        self.merge.seek_ge(&ikey)?;
        self.dir = Dir::Forward;
        self.advance_to_next_user_key()
    }

    /// Positions at the last visible key `< target` (clamped to the upper bound).
    pub fn seek_lt(&mut self, target: &[u8]) -> Result<()> {
        self.prefix = None;
        let key = match &self.upper_bound {
            Some(ub) if self.cmp.compare(target, ub) == std::cmp::Ordering::Greater => ub.clone(),
            _ => target.to_vec(),
        };
        let ikey = self.key_with_max_trailer(&key);
        self.merge.seek_lt(&ikey)?;
        self.dir = Dir::Reverse;
        self.advance_to_prev_user_key()
    }

    /// Positions at the first visible key `>= target` that shares `target`'s prefix,
    /// restricting subsequent forward iteration to that prefix. `prefix` must be a prefix
    /// of `target` (typically the two are equal).
    pub fn seek_prefix_ge(&mut self, prefix: &[u8], target: &[u8]) -> Result<()> {
        self.seek_ge(target)?;
        self.prefix = Some(prefix.to_vec());
        if self.valid && !self.cur_key.starts_with(prefix) {
            self.valid = false;
        }
        Ok(())
    }

    /// Advances to the next visible key.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<()> {
        if !self.valid {
            return Ok(());
        }
        if self.dir == Dir::Reverse {
            // Reposition forward strictly past the current key, then collapse.
            let target = self.key_with_min_trailer(&self.cur_key.clone());
            self.merge.seek_ge(&target)?;
            let ck = self.cur_key.clone();
            while self.merge.valid()
                && self
                    .cmp
                    .compare(encoded_user_key(self.merge.key()), &ck)
                    .is_eq()
            {
                self.merge.advance()?;
            }
            self.dir = Dir::Forward;
        }
        self.advance_to_next_user_key()
    }

    /// Steps back to the previous visible key.
    pub fn prev(&mut self) -> Result<()> {
        if !self.valid {
            return Ok(());
        }
        if self.dir == Dir::Forward {
            // Reposition reverse strictly before the current key, then collapse.
            let target = self.key_with_max_trailer(&self.cur_key.clone());
            self.merge.seek_lt(&target)?;
            self.dir = Dir::Reverse;
        }
        self.advance_to_prev_user_key()
    }

    /// Resolves the visible value of a user key from its versions, ordered newest-first.
    /// Returns `None` if the key is deleted or has no version visible at the snapshot.
    fn resolve(
        &self,
        ukey: &[u8],
        versions: &[(SeqNum, InternalKeyKind, Vec<u8>)],
    ) -> Option<Vec<u8>> {
        let max_rts = max_covering_seqnum(
            &self.range_tombstones,
            self.cmp.as_ref(),
            ukey,
            self.snapshot,
        );
        let mut operands: Vec<Vec<u8>> = Vec::new();
        let mut base: Option<Vec<u8>> = None;
        for (seq, kind, value) in versions {
            if *seq > self.snapshot {
                continue;
            }
            if *seq <= max_rts {
                break; // shadowed by a range tombstone
            }
            match kind {
                InternalKeyKind::Merge => operands.push(value.clone()),
                InternalKeyKind::Set | InternalKeyKind::SetWithDelete => {
                    base = Some(value.clone());
                    break;
                }
                InternalKeyKind::Delete
                | InternalKeyKind::SingleDelete
                | InternalKeyKind::DeleteSized => break,
                _ => {}
            }
        }
        if operands.is_empty() {
            base
        } else {
            operands.reverse(); // chronological (oldest first)
            Some(match &self.merger {
                Some(m) => m.full_merge(ukey, base.as_deref(), &operands),
                None => operands.pop().unwrap(), // no merger: newest operand
            })
        }
    }

    /// Walks the merged stream forward, collapsing each user key to its newest visible
    /// version, until an in-bounds visible key is found or the input is exhausted.
    fn advance_to_next_user_key(&mut self) -> Result<()> {
        while self.merge.valid() {
            let ukey = encoded_user_key(self.merge.key()).to_vec();
            if self.at_or_above_upper(&ukey) || !self.prefix_ok(&ukey) {
                self.valid = false;
                return Ok(());
            }
            // Gather all versions of this user key (newest first); they are contiguous.
            let mut versions = Vec::new();
            while self.merge.valid()
                && self
                    .cmp
                    .compare(encoded_user_key(self.merge.key()), &ukey)
                    .is_eq()
            {
                let trailer = encoded_trailer(self.merge.key());
                versions.push((
                    trailer_seqnum(trailer),
                    trailer_kind(trailer),
                    self.merge.value().to_vec(),
                ));
                self.merge.advance()?;
            }
            if self.below_lower(&ukey) {
                continue;
            }
            if let Some(value) = self.resolve(&ukey, &versions) {
                self.cur_key = ukey;
                self.cur_value = value;
                self.valid = true;
                return Ok(());
            }
        }
        self.valid = false;
        Ok(())
    }

    /// Walks the merged stream backward, collapsing each user key to its newest visible
    /// version, until an in-bounds visible key is found or the input is exhausted.
    fn advance_to_prev_user_key(&mut self) -> Result<()> {
        while self.merge.valid() {
            let ukey = encoded_user_key(self.merge.key()).to_vec();
            if self.below_lower(&ukey) || !self.prefix_ok(&ukey) {
                self.valid = false;
                return Ok(());
            }
            // Going reverse, all versions of this user key are contiguous and arrive
            // oldest-first; collect then reverse to newest-first for resolution.
            let mut versions = Vec::new();
            while self.merge.valid()
                && self
                    .cmp
                    .compare(encoded_user_key(self.merge.key()), &ukey)
                    .is_eq()
            {
                let trailer = encoded_trailer(self.merge.key());
                versions.push((
                    trailer_seqnum(trailer),
                    trailer_kind(trailer),
                    self.merge.value().to_vec(),
                ));
                self.merge.retreat()?;
            }
            versions.reverse();
            if self.at_or_above_upper(&ukey) {
                continue;
            }
            if let Some(value) = self.resolve(&ukey, &versions) {
                self.cur_key = ukey;
                self.cur_value = value;
                self.valid = true;
                return Ok(());
            }
        }
        self.valid = false;
        Ok(())
    }

    fn prefix_ok(&self, ukey: &[u8]) -> bool {
        self.prefix.as_deref().is_none_or(|p| ukey.starts_with(p))
    }
}
