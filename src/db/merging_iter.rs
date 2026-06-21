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
    /// If the current entry's value is stored in a blob file, the `(blob_file_num, handle)`
    /// reference — so a compaction can preserve it without rewriting the value. Default `None`
    /// (memtable and other sources hold values inline).
    fn blob_ref(&self) -> Option<(u64, crate::sstable::blob::BlobHandle)> {
        None
    }
    /// An independent copy of this iterator over the same (`Arc`-shared) sources, at the same
    /// position — backs `DbIterator::clone`.
    fn clone_box(&self) -> Box<dyn InternalIter>;
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
    fn blob_ref(&self) -> Option<(u64, crate::sstable::blob::BlobHandle)> {
        TableIter::blob_ref(self)
    }
    fn clone_box(&self) -> Box<dyn InternalIter> {
        Box::new(self.clone())
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
    fn clone_box(&self) -> Box<dyn InternalIter> {
        Box::new(self.clone())
    }
}

/// Restricts an inner [`InternalIter`] to the inclusive encoded-internal-key range
/// `[lo, hi]`. Used to read a **virtual sstable** — a bounded view over a shared physical
/// backing table — so it yields only the keys within the virtual file's bounds.
pub(crate) struct BoundedIter {
    inner: Box<dyn InternalIter>,
    lo: Vec<u8>,
    hi: Vec<u8>,
    cmp: Arc<dyn Comparer>,
    valid: bool,
}

impl BoundedIter {
    pub(crate) fn new(
        inner: Box<dyn InternalIter>,
        lo: Vec<u8>,
        hi: Vec<u8>,
        cmp: Arc<dyn Comparer>,
    ) -> BoundedIter {
        BoundedIter {
            inner,
            lo,
            hi,
            cmp,
            valid: false,
        }
    }

    /// Recomputes validity from the inner position, requiring it to lie within `[lo, hi]`.
    fn refresh(&mut self) {
        self.valid = self.inner.valid()
            && compare_encoded(self.cmp.as_ref(), self.inner.key(), &self.lo)
                != std::cmp::Ordering::Less
            && compare_encoded(self.cmp.as_ref(), self.inner.key(), &self.hi)
                != std::cmp::Ordering::Greater;
    }

    /// After a reverse positioning, steps back over any keys above `hi`, then refreshes.
    fn settle_reverse(&mut self) -> Result<()> {
        while self.inner.valid()
            && compare_encoded(self.cmp.as_ref(), self.inner.key(), &self.hi)
                == std::cmp::Ordering::Greater
        {
            self.inner.retreat()?;
        }
        self.refresh();
        Ok(())
    }
}

impl InternalIter for BoundedIter {
    fn first(&mut self) -> Result<()> {
        let lo = self.lo.clone();
        self.inner.seek_ge(&lo)?;
        self.refresh();
        Ok(())
    }
    fn last(&mut self) -> Result<()> {
        self.inner.last()?;
        self.settle_reverse()
    }
    fn seek_ge(&mut self, target: &[u8]) -> Result<()> {
        // Clamp the seek target up to the lower bound.
        let t = if compare_encoded(self.cmp.as_ref(), target, &self.lo) == std::cmp::Ordering::Less
        {
            self.lo.clone()
        } else {
            target.to_vec()
        };
        self.inner.seek_ge(&t)?;
        self.refresh();
        Ok(())
    }
    fn seek_lt(&mut self, target: &[u8]) -> Result<()> {
        self.inner.seek_lt(target)?;
        self.settle_reverse()
    }
    fn valid(&self) -> bool {
        self.valid
    }
    fn key(&self) -> &[u8] {
        self.inner.key()
    }
    fn value(&self) -> &[u8] {
        self.inner.value()
    }
    fn advance(&mut self) -> Result<()> {
        self.inner.advance()?;
        // Past the upper bound ends forward iteration.
        self.valid = self.inner.valid()
            && compare_encoded(self.cmp.as_ref(), self.inner.key(), &self.hi)
                != std::cmp::Ordering::Greater;
        Ok(())
    }
    fn retreat(&mut self) -> Result<()> {
        self.inner.retreat()?;
        // Before the lower bound ends reverse iteration.
        self.valid = self.inner.valid()
            && compare_encoded(self.cmp.as_ref(), self.inner.key(), &self.lo)
                != std::cmp::Ordering::Less;
        Ok(())
    }
    fn blob_ref(&self) -> Option<(u64, crate::sstable::blob::BlobHandle)> {
        self.inner.blob_ref()
    }
    fn clone_box(&self) -> Box<dyn InternalIter> {
        Box::new(BoundedIter {
            inner: self.inner.clone_box(),
            lo: self.lo.clone(),
            hi: self.hi.clone(),
            cmp: self.cmp.clone(),
            valid: self.valid,
        })
    }
}

/// Presents a sorted, non-overlapping sequence of internal iterators — the files of one L1+
/// level, or one L0 sublevel — as a single ordered iterator. The [`MergingIter`] then pays one
/// source per run rather than one per file (its per-step cost is linear in the source count),
/// and seeks binary-search to the containing part. The parts must be ascending and
/// non-overlapping by key range; `bounds[i]` is part `i`'s `[smallest, largest]` encoded
/// internal-key range.
pub(crate) struct ConcatIter {
    parts: Vec<Box<dyn InternalIter>>,
    bounds: Vec<(Vec<u8>, Vec<u8>)>,
    cmp: Arc<dyn Comparer>,
    cur: Option<usize>,
}

impl ConcatIter {
    pub(crate) fn new(
        parts: Vec<Box<dyn InternalIter>>,
        bounds: Vec<(Vec<u8>, Vec<u8>)>,
        cmp: Arc<dyn Comparer>,
    ) -> ConcatIter {
        debug_assert_eq!(parts.len(), bounds.len());
        ConcatIter {
            parts,
            bounds,
            cmp,
            cur: None,
        }
    }

    /// Positions on the first valid entry of the first part at or after index `i`.
    fn first_from(&mut self, i: usize) -> Result<()> {
        let mut j = i;
        while j < self.parts.len() {
            self.parts[j].first()?;
            if self.parts[j].valid() {
                self.cur = Some(j);
                return Ok(());
            }
            j += 1;
        }
        self.cur = None;
        Ok(())
    }

    /// Positions on the last valid entry of the first part at or before index `i`.
    fn last_from(&mut self, i: isize) -> Result<()> {
        let mut j = i;
        while j >= 0 {
            self.parts[j as usize].last()?;
            if self.parts[j as usize].valid() {
                self.cur = Some(j as usize);
                return Ok(());
            }
            j -= 1;
        }
        self.cur = None;
        Ok(())
    }
}

impl InternalIter for ConcatIter {
    fn first(&mut self) -> Result<()> {
        self.first_from(0)
    }
    fn last(&mut self) -> Result<()> {
        self.last_from(self.parts.len() as isize - 1)
    }
    fn seek_ge(&mut self, target: &[u8]) -> Result<()> {
        // First part whose largest bound is >= target.
        let mut lo = 0usize;
        let mut hi = self.parts.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            if compare_encoded(self.cmp.as_ref(), &self.bounds[mid].1, target)
                == std::cmp::Ordering::Less
            {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo >= self.parts.len() {
            self.cur = None;
            return Ok(());
        }
        self.parts[lo].seek_ge(target)?;
        if self.parts[lo].valid() {
            self.cur = Some(lo);
            Ok(())
        } else {
            // `target` is above this part's keys; the answer (if any) opens the next part.
            self.first_from(lo + 1)
        }
    }
    fn seek_lt(&mut self, target: &[u8]) -> Result<()> {
        // Last part whose smallest bound is < target (the one before the first with smallest >=).
        let mut lo = 0usize;
        let mut hi = self.parts.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            if compare_encoded(self.cmp.as_ref(), &self.bounds[mid].0, target)
                == std::cmp::Ordering::Less
            {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == 0 {
            self.cur = None;
            return Ok(());
        }
        let idx = lo - 1;
        self.parts[idx].seek_lt(target)?;
        if self.parts[idx].valid() {
            self.cur = Some(idx);
            Ok(())
        } else {
            self.last_from(idx as isize - 1)
        }
    }
    fn valid(&self) -> bool {
        self.cur.is_some_and(|i| self.parts[i].valid())
    }
    fn key(&self) -> &[u8] {
        self.parts[self.cur.expect("valid")].key()
    }
    fn value(&self) -> &[u8] {
        self.parts[self.cur.expect("valid")].value()
    }
    fn advance(&mut self) -> Result<()> {
        let i = self.cur.expect("advance on invalid concat iter");
        self.parts[i].advance()?;
        if !self.parts[i].valid() {
            self.first_from(i + 1)?;
        }
        Ok(())
    }
    fn retreat(&mut self) -> Result<()> {
        let i = self.cur.expect("retreat on invalid concat iter");
        self.parts[i].retreat()?;
        if !self.parts[i].valid() {
            self.last_from(i as isize - 1)?;
        }
        Ok(())
    }
    fn blob_ref(&self) -> Option<(u64, crate::sstable::blob::BlobHandle)> {
        self.cur.and_then(|i| self.parts[i].blob_ref())
    }
    fn clone_box(&self) -> Box<dyn InternalIter> {
        Box::new(ConcatIter {
            parts: self.parts.iter().map(|p| p.clone_box()).collect(),
            bounds: self.bounds.clone(),
            cmp: self.cmp.clone(),
            cur: self.cur,
        })
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

impl Clone for MergingIter {
    fn clone(&self) -> MergingIter {
        MergingIter {
            sources: self.sources.iter().map(|s| s.clone_box()).collect(),
            cmp: self.cmp.clone(),
            cur: self.cur,
            dir: self.dir,
        }
    }
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

    /// The current entry's blob reference, if its value lives in a blob file.
    pub(crate) fn blob_ref(&self) -> Option<(u64, crate::sstable::blob::BlobHandle)> {
        self.sources[self.cur.expect("valid")].blob_ref()
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

/// Which kinds of keys an iterator surfaces (Pebble's `IterKeyType`).
///
/// Note: Pebble's default is [`PointsOnly`](IterKeyType::PointsOnly); pebbledb defaults to
/// [`PointsAndRanges`](IterKeyType::PointsAndRanges) so that `range_keys()` is populated on a
/// plain `db.iter()` without opting in. Set `key_type` explicitly for Pebble-exact behavior.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum IterKeyType {
    /// Only point keys are produced; range keys are not surfaced (range-key *masking* and
    /// range-*deletion* shadowing still apply — those affect point visibility, not surfacing).
    PointsOnly,
    /// Only range keys are produced: iteration walks the defragmented range-key spans, and
    /// `key()` is each span's start bound. Point keys are skipped.
    RangesOnly,
    /// Both point keys and range keys: point-key positions with their covering range keys
    /// surfaced via [`DbIterator::range_keys`].
    #[default]
    PointsAndRanges,
}

/// Bounds and key-type selection for a [`DbIterator`].
#[derive(Clone, Default)]
pub struct IterOptions {
    /// Which key kinds to surface (points, ranges, or both). See [`IterKeyType`].
    pub key_type: IterKeyType,
    /// Inclusive lower bound on user keys; keys below it are not produced.
    pub lower_bound: Option<Vec<u8>>,
    /// Exclusive upper bound on user keys; keys at or above it are not produced.
    pub upper_bound: Option<Vec<u8>>,
    /// Enables range-key masking (Pebble's `RangeKeyMasking.Suffix`). When set, point keys
    /// covered by a range key whose suffix is `>= ` this value are hidden if the point's own
    /// suffix sorts after that range key's suffix — the MVCC "a range deletion at timestamp T
    /// hides older point versions" behavior. Suffixes are extracted with the comparer's
    /// [`split`](crate::base::comparer::Comparer::split); with the default (suffix-less)
    /// comparer no point ever has a suffix, so masking never fires.
    pub range_key_masking_suffix: Option<Vec<u8>>,
    /// Table-level block-property filters (Pebble's `BlockPropertyFilters`). An sstable whose
    /// recorded property is ruled out by *any* of these filters has its point keys skipped
    /// during iteration; the table's range tombstones and range keys are still consulted, so
    /// shadowing remains correct. Empty (default) disables filtering.
    pub block_property_filters:
        Vec<std::sync::Arc<dyn crate::sstable::blockprop::BlockPropertyFilter>>,
    /// Restrict iteration to data that is guaranteed durable — i.e. already flushed to
    /// sstables (Pebble's `OnlyReadGuaranteedDurable`). When set, the mutable and immutable
    /// memtables (and their range tombstones / range keys) are excluded, so only state that
    /// would survive a process crash without an OS flush is visible.
    pub only_durable: bool,
}

/// A bidirectional iterator over a database's user keys at a fixed snapshot.
///
/// Collapses the multiple internal-key versions of each user key down to the single
/// newest one visible at the snapshot, hides tombstones, applies range-tombstone
/// shadowing and the merge operator, and honors the configured key bounds.
/// `Clone` yields an independent cursor over the same pinned sources and snapshot, at the
/// same position; advancing one does not affect the other.
#[derive(Clone)]
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
    /// When set, point keys masked by a covering range key (see
    /// [`IterOptions::range_key_masking_suffix`]) are hidden.
    mask_suffix: Option<Vec<u8>>,
    /// When set, iteration is restricted to keys having this prefix.
    prefix: Option<Vec<u8>>,
    /// Which key kinds this iterator surfaces.
    key_type: IterKeyType,
    /// Defragmented range-key spans `(start, end)` used by [`IterKeyType::RangesOnly`]
    /// iteration; empty in the other modes.
    fragments: Vec<(Vec<u8>, Vec<u8>)>,
    /// Current index into `fragments` for `RangesOnly` iteration.
    frag_idx: usize,
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
        let mut it = DbIterator {
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
            mask_suffix: opts.range_key_masking_suffix,
            prefix: None,
            key_type: opts.key_type,
            fragments: Vec::new(),
            frag_idx: 0,
        };
        if it.key_type == IterKeyType::RangesOnly {
            it.fragments = it.compute_fragments();
        }
        Ok(it)
    }

    /// Builds the defragmented range-key spans for [`IterKeyType::RangesOnly`] iteration:
    /// fragments the visible range keys at every start/end boundary, drops sub-intervals with
    /// no effective range key, then coalesces adjacent intervals that carry the identical set
    /// of `(suffix, value)` pairs into one span. The result is sorted by start key.
    fn compute_fragments(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        use std::cmp::Ordering;
        let visible: Vec<&crate::base::range_key::RangeKeyEntry> = self
            .range_keys
            .iter()
            .filter(|e| e.seqnum <= self.snapshot)
            .collect();
        if visible.is_empty() {
            return Vec::new();
        }
        // Distinct boundary keys, sorted.
        let mut bounds: Vec<Vec<u8>> = Vec::new();
        for e in &visible {
            bounds.push(e.start.clone());
            if let Ok(end) = e.end() {
                bounds.push(end);
            }
        }
        bounds.sort_by(|a, b| self.cmp.compare(a, b));
        bounds.dedup();

        // One candidate fragment per [bound[i], bound[i+1]) that has an effective key set.
        let mut frags: Vec<(Vec<u8>, Vec<u8>, Vec<crate::base::range_key::SuffixValue>)> =
            Vec::new();
        for w in bounds.windows(2) {
            let (lo, hi) = (&w[0], &w[1]);
            let coalesced = self.coalesce(self.covering_range_keys(lo));
            if coalesced.is_empty() {
                continue;
            }
            // Coalesce with the previous fragment if it abuts and carries the same key set.
            if let Some(last) = frags.last_mut()
                && self.cmp.compare(&last.1, lo) == Ordering::Equal
                && last.2 == coalesced
            {
                last.1 = hi.clone();
            } else {
                frags.push((lo.clone(), hi.clone(), coalesced));
            }
        }
        frags.into_iter().map(|(s, e, _)| (s, e)).collect()
    }

    /// The range-key entries covering the current position, newest first, visible at the
    /// iterator's snapshot. Empty when not positioned at a valid key or when no range keys
    /// cover it. (Surfacing range keys alongside points; full `RANGEKEYSET`/`UNSET`/`DEL`
    /// coalescing and masking is layered on top of these raw entries.)
    pub fn range_keys(&self) -> Vec<crate::base::range_key::RangeKeyEntry> {
        if !self.valid || self.key_type == IterKeyType::PointsOnly {
            return Vec::new();
        }
        self.covering_range_keys(&self.cur_key)
    }

    /// The range-key entries covering `ukey` and visible at the snapshot, in source order.
    fn covering_range_keys(&self, ukey: &[u8]) -> Vec<crate::base::range_key::RangeKeyEntry> {
        self.range_keys
            .iter()
            .filter(|e| {
                e.seqnum <= self.snapshot && e.covers(self.cmp.as_ref(), ukey).unwrap_or(false)
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
        if !self.valid || self.key_type == IterKeyType::PointsOnly {
            return Vec::new();
        }
        self.coalesce(self.covering_range_keys(&self.cur_key))
    }

    /// Coalesces a set of covering range-key entries into the effective `(suffix, value)`
    /// pairs in force, newest entry winning. Shared by [`coalesced_range_keys`](Self::coalesced_range_keys)
    /// and range-key masking.
    fn coalesce(
        &self,
        mut covering: Vec<crate::base::range_key::RangeKeyEntry>,
    ) -> Vec<crate::base::range_key::SuffixValue> {
        use crate::base::internal_key::InternalKeyKind;
        use crate::base::range_key::{decode_end, decode_set_suffix_values, decode_unset_suffixes};
        use std::collections::BTreeMap;

        // Covering entries, newest sequence number first.
        covering.sort_by_key(|e| std::cmp::Reverse(e.seqnum));

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

    /// Reconfigures the iterator in place (Pebble's `Iterator.SetOptions`): updates the key
    /// bounds, key-type selection, and range-key mask without rebuilding the underlying merge.
    /// The position is invalidated — re-seek with [`first`](Self::first) / [`last`](Self::last)
    /// / [`seek_ge`](Self::seek_ge) before reading again.
    ///
    /// Block-property filters are fixed when the iterator is created (they select which sstable
    /// sources are read), so `opts.block_property_filters` is ignored here; create a new
    /// iterator to change them.
    pub fn set_options(&mut self, opts: IterOptions) {
        self.lower_bound = opts.lower_bound;
        self.upper_bound = opts.upper_bound;
        self.mask_suffix = opts.range_key_masking_suffix;
        self.key_type = opts.key_type;
        self.prefix = None;
        // RangesOnly walks a precomputed fragment list; (re)build or drop it to match the mode.
        self.fragments = if self.key_type == IterKeyType::RangesOnly {
            self.compute_fragments()
        } else {
            Vec::new()
        };
        self.frag_idx = 0;
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

    /// Whether `ukey` is hidden by range-key masking. Mirrors Pebble's `rangeKeyMasking`:
    /// the active mask suffix is the *smallest* covering range-key suffix that is itself
    /// `>=` the configured masking suffix; a point is masked when its own suffix sorts
    /// strictly after that active suffix. Points without a suffix are never masked.
    fn masked(&self, ukey: &[u8]) -> bool {
        use std::cmp::Ordering;
        let Some(mask) = self.mask_suffix.as_deref() else {
            return false;
        };
        let point_suffix = &ukey[self.cmp.split(ukey)..];
        if point_suffix.is_empty() {
            return false;
        }
        // Active mask suffix = smallest covering SET suffix that is >= the masking suffix.
        let mut active: Option<Vec<u8>> = None;
        for sv in self.coalesce(self.covering_range_keys(ukey)) {
            if sv.suffix.is_empty() || self.cmp.compare(&sv.suffix, mask) == Ordering::Less {
                continue;
            }
            if active
                .as_deref()
                .is_none_or(|a| self.cmp.compare(&sv.suffix, a) == Ordering::Less)
            {
                active = Some(sv.suffix);
            }
        }
        active
            .as_deref()
            .is_some_and(|a| self.cmp.compare(a, point_suffix) == Ordering::Less)
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
        if self.key_type == IterKeyType::RangesOnly {
            self.frag_seek_forward(0);
            return Ok(());
        }
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
        if self.key_type == IterKeyType::RangesOnly {
            self.frag_seek_reverse(self.fragments.len() as isize - 1);
            return Ok(());
        }
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
        if self.key_type == IterKeyType::RangesOnly {
            // The first fragment that covers or follows `target` (its end is past `target`).
            let i = self
                .fragments
                .iter()
                .position(|(_, e)| self.cmp.compare(e, target) == std::cmp::Ordering::Greater)
                .unwrap_or(self.fragments.len());
            self.frag_seek_forward(i);
            return Ok(());
        }
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
        if self.key_type == IterKeyType::RangesOnly {
            // The last fragment that starts strictly before `target`.
            let i = self
                .fragments
                .iter()
                .rposition(|(s, _)| self.cmp.compare(s, target) == std::cmp::Ordering::Less)
                .map(|p| p as isize)
                .unwrap_or(-1);
            self.frag_seek_reverse(i);
            return Ok(());
        }
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
        if self.key_type == IterKeyType::RangesOnly {
            self.frag_seek_forward(self.frag_idx + 1);
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
        if self.key_type == IterKeyType::RangesOnly {
            self.frag_seek_reverse(self.frag_idx as isize - 1);
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
            if self.masked(&ukey) {
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
            if self.masked(&ukey) {
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

    // --- RangesOnly fragment iteration --------------------------------------------------

    /// Whether fragment `(s, e)` overlaps the iterator's key bounds (`end` is exclusive).
    fn frag_in_bounds(&self, s: &[u8], e: &[u8]) -> bool {
        use std::cmp::Ordering;
        if let Some(lb) = &self.lower_bound
            && self.cmp.compare(e, lb) != Ordering::Greater
        {
            return false; // span ends at or before the lower bound
        }
        if let Some(ub) = &self.upper_bound
            && self.cmp.compare(s, ub) != Ordering::Less
        {
            return false; // span starts at or after the upper bound
        }
        true
    }

    /// The surfaced key for a fragment start: the start bound clamped up to the lower bound.
    fn frag_key(&self, s: &[u8]) -> Vec<u8> {
        match &self.lower_bound {
            Some(lb) if self.cmp.compare(s, lb) == std::cmp::Ordering::Less => lb.clone(),
            _ => s.to_vec(),
        }
    }

    /// Positions at the first in-bounds fragment at index `>= from` (forward).
    fn frag_seek_forward(&mut self, from: usize) {
        let mut i = from;
        while i < self.fragments.len() {
            let (s, e) = (self.fragments[i].0.clone(), self.fragments[i].1.clone());
            if self.frag_in_bounds(&s, &e) {
                self.frag_idx = i;
                self.cur_key = self.frag_key(&s);
                self.cur_value = Vec::new();
                self.valid = true;
                self.dir = Dir::Forward;
                return;
            }
            i += 1;
        }
        self.valid = false;
    }

    /// Positions at the last in-bounds fragment at index `<= from` (reverse).
    fn frag_seek_reverse(&mut self, from: isize) {
        let mut i = from;
        while i >= 0 {
            let idx = i as usize;
            let (s, e) = (self.fragments[idx].0.clone(), self.fragments[idx].1.clone());
            if self.frag_in_bounds(&s, &e) {
                self.frag_idx = idx;
                self.cur_key = self.frag_key(&s);
                self.cur_value = Vec::new();
                self.valid = true;
                self.dir = Dir::Reverse;
                return;
            }
            i -= 1;
        }
        self.valid = false;
    }
}
