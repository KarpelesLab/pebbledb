// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Ported from Pebble's indexed batch (batch.go: read-your-own-writes).

//! Indexed batches: a write batch you can also read from before it is committed.
//!
//! [`IndexedBatch`] wraps a [`Batch`] and maintains a sorted in-memory index of its point
//! operations (and its range deletions), so [`IndexedBatch::get`] and
//! [`IndexedBatch::scan`] return a view of the database **as if the batch were already
//! applied** — Pebble's "read-your-own-writes". Operations within the batch are ordered, so
//! a later op in the same batch overrides an earlier one, and a `delete_range` shadows both
//! earlier batch writes and committed database keys in its span.
//!
//! Range *keys* are recorded for commit but are not reflected in the batch's own read view.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::base::merge::Merger;
use crate::batch::Batch;
use crate::{Db, Result};

/// The committed-or-pending state of a single key within the batch.
enum Base {
    /// Last point op was a `Set` with this value.
    Set(Vec<u8>),
    /// Last point op was a `Delete` / `SingleDelete`.
    Deleted,
    /// Only merge operands so far (no base Set/Delete in the batch).
    Unset,
}

/// Accumulated point operations for one key, with the batch-order of the latest.
struct PointAccum {
    /// Order (position in the batch) of the most recent op touching this key.
    order: u32,
    base: Base,
    /// Merge operands applied after `base`, oldest-first.
    merges: Vec<Vec<u8>>,
}

/// A write batch that also supports reads reflecting its own pending writes.
pub struct IndexedBatch {
    batch: Batch,
    order: u32,
    /// Point ops, keyed by user key and kept sorted for iteration.
    points: BTreeMap<Vec<u8>, PointAccum>,
    /// Range deletions as `(start, end, order)`.
    range_dels: Vec<(Vec<u8>, Vec<u8>, u32)>,
    merger: Option<Arc<dyn Merger>>,
}

impl IndexedBatch {
    /// Creates an empty indexed batch using `merger` to resolve `merge` reads.
    pub fn new(merger: Option<Arc<dyn Merger>>) -> IndexedBatch {
        IndexedBatch {
            batch: Batch::new(),
            order: 0,
            points: BTreeMap::new(),
            range_dels: Vec::new(),
            merger,
        }
    }

    fn next_order(&mut self) -> u32 {
        let o = self.order;
        self.order += 1;
        o
    }

    /// Sets `key` to `value`.
    pub fn set(&mut self, key: &[u8], value: &[u8]) {
        let order = self.next_order();
        self.batch.set(key, value);
        self.points.insert(
            key.to_vec(),
            PointAccum {
                order,
                base: Base::Set(value.to_vec()),
                merges: Vec::new(),
            },
        );
    }

    /// Deletes `key`.
    pub fn delete(&mut self, key: &[u8]) {
        let order = self.next_order();
        self.batch.delete(key);
        self.points.insert(
            key.to_vec(),
            PointAccum {
                order,
                base: Base::Deleted,
                merges: Vec::new(),
            },
        );
    }

    /// Merges `value` into `key`.
    pub fn merge(&mut self, key: &[u8], value: &[u8]) {
        let order = self.next_order();
        self.batch.merge(key, value);
        let e = self.points.entry(key.to_vec()).or_insert(PointAccum {
            order,
            base: Base::Unset,
            merges: Vec::new(),
        });
        e.order = order;
        e.merges.push(value.to_vec());
    }

    /// Deletes the half-open user-key range `[start, end)`.
    pub fn delete_range(&mut self, start: &[u8], end: &[u8]) {
        let order = self.next_order();
        self.batch.delete_range(start, end);
        self.range_dels.push((start.to_vec(), end.to_vec(), order));
    }

    /// The number of operations in the batch.
    pub fn count(&self) -> u32 {
        self.batch.count()
    }

    /// Consumes the indexed batch, returning the underlying [`Batch`] for `Db::write`.
    pub fn into_batch(self) -> Batch {
        self.batch
    }

    /// The largest order among range deletions covering `key`, or `-1` if none.
    fn covering_rd_order(&self, db: &Db, key: &[u8]) -> i64 {
        let cmp = db.comparer().as_ref();
        self.range_dels
            .iter()
            .filter(|(s, e, _)| {
                cmp.compare(s, key) != std::cmp::Ordering::Greater
                    && cmp.compare(key, e) == std::cmp::Ordering::Less
            })
            .map(|(_, _, o)| *o as i64)
            .max()
            .unwrap_or(-1)
    }

    /// Reads `key` as visible through the batch: the batch's pending write if any (a later
    /// `delete_range` or point op wins), otherwise the committed database value.
    pub fn get(&self, db: &Db, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let p = self.points.get(key);
        let p_order = p.map(|e| e.order as i64).unwrap_or(-1);
        if self.covering_rd_order(db, key) > p_order {
            // A batch range deletion is the most recent op affecting this key.
            return Ok(None);
        }
        let Some(e) = p else {
            // No batch op for this key: fall through to the database.
            return db.get(key);
        };
        let base: Option<Vec<u8>> = match &e.base {
            Base::Set(v) => Some(v.clone()),
            Base::Deleted => None,
            Base::Unset => db.get(key)?,
        };
        if e.merges.is_empty() {
            return Ok(base);
        }
        Ok(Some(match &self.merger {
            Some(m) => m.full_merge(key, base.as_deref(), &e.merges),
            None => e.merges.last().unwrap().clone(),
        }))
    }

    /// Returns the database's contents as visible through the batch, in sorted order
    /// (read-your-own-writes). This materializes the merged view; for very large databases
    /// prefer committing the batch and using `Db::iter`.
    pub fn scan(&self, db: &Db) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // Union of committed keys and batch point keys.
        let mut keys: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
        let mut it = db.iter()?;
        it.first()?;
        while it.valid() {
            keys.insert(it.key().to_vec());
            it.next()?;
        }
        for k in self.points.keys() {
            keys.insert(k.clone());
        }
        let mut out = Vec::new();
        for k in keys {
            if let Some(v) = self.get(db, &k)? {
                out.push((k, v));
            }
        }
        Ok(out)
    }
}
