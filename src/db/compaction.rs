// Copyright (c) 2012 The LevelDB-Go Authors. All rights reserved.
// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Ported from Pebble's compaction.go and compaction_picker.go (leveled path).

//! Leveled compaction.
//!
//! After a flush, [`Db`] calls [`Db::maybe_compact`], which repeatedly picks and runs
//! compactions until the LSM is in shape. A compaction merges the chosen files from one
//! level with the overlapping files of the next, collapses each user key to its newest
//! version (dropping shadowed versions, and dropping tombstones when compacting into the
//! bottom level), and writes the result as new sstables in the output level. The inputs
//! are then removed and the change recorded in the MANIFEST.
//!
//! Scope: an L0-by-file-count trigger plus an L1+ size trigger, single-key-range output
//! splitting by target file size. Compaction keeps the newest version of each key within
//! every open-snapshot stripe, so versions an open snapshot can observe are retained;
//! tombstones are elided only at the bottom level and only above all snapshots.

use std::sync::Arc;

use crate::Result;
use crate::base::internal_key::{InternalKeyKind, encoded_trailer, encoded_user_key, trailer_kind};
use crate::base::range_del::RangeTombstone;
use crate::base::range_key::RangeKeyEntry;
use crate::manifest::{FileMetadata, NUM_LEVELS, NewFileEntry, Version, VersionEdit};
use crate::sstable::{Writer, WriterOptions};
use crate::vfs::{Fs, WritableFile};

use super::merging_iter::{InternalIter, MergingIter};
use super::{DbInner, State, filenames};

/// Safety cap on compactions per `maybe_compact` call.
const MAX_COMPACTIONS_PER_CALL: usize = 100;

/// A chosen compaction: merge `inputs` (from `level`) with `overlap` (from `output_level`).
struct Compaction {
    level: usize,
    output_level: usize,
    inputs: Vec<Arc<FileMetadata>>,
    overlap: Vec<Arc<FileMetadata>>,
}

/// The size budget for a level before it is compacted downward.
fn level_budget(level: usize) -> u64 {
    // L1 = 10 MiB, growing 10x per level.
    let mut budget = 10u64 << 20;
    for _ in 1..level {
        budget = budget.saturating_mul(10);
    }
    budget
}

impl DbInner {
    /// Compacts the entire key space toward the bottom level (Pebble's `Compact` over the
    /// full range). Equivalent to `compact_range(None, None)`.
    pub fn compact(&self) -> Result<()> {
        self.compact_range(None, None)
    }

    /// Manually compacts every level overlapping the user-key range `[start, end)` down
    /// toward the bottom level. `None` bounds mean unbounded. Flushes the memtable first
    /// so its data participates. Useful to reclaim space after a large range delete.
    pub fn compact_range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Result<()> {
        if self.state.lock().unwrap().read_only {
            return Err(crate::Error::InvalidState("db: opened read-only".into()));
        }
        // Flush the memtable (and drain the immutable queue) so its data participates.
        self.flush()?;
        let mut state = self.state.lock().unwrap();

        // Walk levels from the top, pushing any file overlapping the range one level down,
        // until the data has reached the bottom level. The loop is bounded by the work
        // actually available (each pass strictly reduces the files above the bottom that
        // overlap the range, or stops).
        for _ in 0..MAX_COMPACTIONS_PER_CALL {
            let mut did_work = false;
            for level in 0..NUM_LEVELS - 1 {
                let inputs: Vec<_> = self
                    .range_overlap(&state.vs.current, level, start, end)
                    .collect();
                if inputs.is_empty() {
                    continue;
                }
                let (min, max) = match key_range(self.cmp.as_ref(), &inputs) {
                    Some(r) => r,
                    None => continue,
                };
                let overlap =
                    overlapping(self.cmp.as_ref(), &state.vs.current, level + 1, &min, &max);
                let c = Compaction {
                    level,
                    output_level: level + 1,
                    inputs,
                    overlap,
                };
                self.run_compaction(&mut state, c)?;
                did_work = true;
            }
            if !did_work {
                break;
            }
        }
        Ok(())
    }

    /// Files at `level` whose user-key range intersects the (optionally unbounded)
    /// `[start, end)` range.
    fn range_overlap<'a>(
        &'a self,
        version: &'a Version,
        level: usize,
        start: Option<&'a [u8]>,
        end: Option<&'a [u8]>,
    ) -> impl Iterator<Item = Arc<FileMetadata>> + 'a {
        version.levels[level].iter().filter_map(move |f| {
            let s = encoded_user_key(&f.smallest);
            let l = encoded_user_key(&f.largest);
            // [s, l] intersects [start, end): l >= start and s < end.
            let after_start =
                start.is_none_or(|st| self.cmp.compare(l, st) != std::cmp::Ordering::Less);
            let before_end =
                end.is_none_or(|en| self.cmp.compare(s, en) == std::cmp::Ordering::Less);
            (after_start && before_end).then(|| f.clone())
        })
    }

    /// Picks and runs compactions until none are needed (or a safety cap is hit).
    pub(super) fn maybe_compact(&self, state: &mut State) -> Result<()> {
        // Service files queued by read-triggered compaction first.
        self.run_read_compactions(state)?;
        // Opportunistically drop whole files shadowed by a covering range tombstone before
        // scoring levels — this is free (a MANIFEST edit) and shrinks the work below.
        self.delete_only_compact(state)?;
        // Rewrite bottom-level files that carry now-droppable tombstones.
        self.elision_only_compact(state)?;
        for _ in 0..MAX_COMPACTIONS_PER_CALL {
            match self.pick_compaction(&state.vs.current) {
                Some(c) => self.run_compaction(state, c)?,
                None => break,
            }
        }
        // With levels within budget, compact tombstone-dense files downward to reclaim space.
        for _ in 0..MAX_COMPACTIONS_PER_CALL {
            if !self.tombstone_density_compact(state)? {
                break;
            }
        }
        Ok(())
    }

    /// Read-triggered compaction (Pebble): compact files queued in `state.read_queue` because
    /// reads repeatedly passed through them to find keys in deeper levels. Each queued L1+
    /// file is compacted one level down (rewriting when there is overlapping data below,
    /// otherwise moving), reducing the read amplification of hot ranges. L0 entries are
    /// skipped (a single L0 file may not hold the newest version of its keys).
    pub(super) fn run_read_compactions(&self, state: &mut State) -> Result<()> {
        while let Some(file_num) = {
            let q = &mut state.read_queue;
            (!q.is_empty()).then(|| q.remove(0))
        } {
            state.read_miss.remove(&file_num);
            // Locate the file and its current level (it may have moved or been dropped).
            let mut found = None;
            for level in 1..NUM_LEVELS - 1 {
                if let Some(f) = state.vs.current.levels[level]
                    .iter()
                    .find(|f| f.file_num == file_num)
                {
                    found = Some((level, f.clone()));
                    break;
                }
            }
            let Some((level, f)) = found else {
                continue;
            };
            let Some((min, max)) = key_range(self.cmp.as_ref(), std::slice::from_ref(&f)) else {
                continue;
            };
            let overlap = overlapping(self.cmp.as_ref(), &state.vs.current, level + 1, &min, &max);
            self.log(&format!(
                "read-triggered compaction of sstable {file_num} at L{level}"
            ));
            let c = Compaction {
                level,
                output_level: level + 1,
                inputs: vec![f],
                overlap,
            };
            self.run_compaction(state, c)?;
        }
        Ok(())
    }

    /// Tombstone-density compaction (Pebble): compact a file whose point-tombstone fraction
    /// exceeds [`Options::tombstone_dense_compaction_threshold`] toward the bottom, even
    /// though its level is within budget, so its accumulated tombstones stop lingering.
    /// Returns whether a compaction was run.
    ///
    /// When the next level holds overlapping data the compaction rewrites it, applying the
    /// tombstones and reclaiming the shadowed space; when it does not, the file is moved down
    /// a level (a cheap MANIFEST edit). Either way the file descends until it reaches the
    /// bottom level, where the elision-only pass finally drops the dead tombstones. Each step
    /// strictly increases the file's level, so the per-call loop terminates.
    fn tombstone_density_compact(&self, state: &mut State) -> Result<bool> {
        if self.tombstone_dense_compaction_threshold > 1.0 {
            return Ok(false);
        }
        let version = state.vs.current.clone();
        // Start at L1: L0 files are not range-partitioned, so a single L0 file may not hold
        // the newest version of its keys — compacting it down in isolation could push a newer
        // version below an older one. (Whole-L0 compactions are handled by the score picker.)
        for level in 1..NUM_LEVELS - 1 {
            for f in &version.levels[level] {
                let reader = self.open_reader(f.file_num)?;
                let props = reader.properties();
                if props.num_entries == 0 {
                    continue;
                }
                let frac = props.num_deletions as f64 / props.num_entries as f64;
                if frac < self.tombstone_dense_compaction_threshold {
                    continue;
                }
                let Some((min, max)) = key_range(self.cmp.as_ref(), std::slice::from_ref(f)) else {
                    continue;
                };
                let overlap = overlapping(self.cmp.as_ref(), &version, level + 1, &min, &max);
                self.log(&format!(
                    "tombstone-density compaction of sstable {} at L{level} (fraction {frac:.2})",
                    f.file_num
                ));
                let c = Compaction {
                    level,
                    output_level: level + 1,
                    inputs: vec![f.clone()],
                    overlap,
                };
                self.run_compaction(state, c)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Delete-only compaction (Pebble): drop files that are entirely shadowed by a covering
    /// range tombstone, via a MANIFEST edit alone — no rewrite. A file in a level strictly
    /// deeper than a range tombstone qualifies when its whole key range lies within the
    /// tombstone's `[start, end)` span and *every* version it holds predates the tombstone
    /// (`largest_seqnum < tombstone.seqnum`), so the tombstone deletes all of it.
    ///
    /// Conservative: skipped entirely while any snapshot is open, since a snapshot at a
    /// sequence number between the file's versions and the tombstone could still observe the
    /// pre-deletion state.
    fn delete_only_compact(&self, state: &mut State) -> Result<()> {
        if !self.open_snapshots().is_empty() {
            return Ok(());
        }
        let version = state.vs.current.clone();
        // Every range tombstone in the LSM, tagged with the level it lives in.
        let mut tombstones: Vec<(usize, Vec<u8>, Vec<u8>, u64)> = Vec::new();
        for (level, files) in version.levels.iter().enumerate() {
            for f in files {
                let reader = self.open_reader(f.file_num)?;
                for t in reader.range_tombstones() {
                    tombstones.push((level, t.start.clone(), t.end.clone(), t.seqnum));
                }
            }
        }
        if tombstones.is_empty() {
            return Ok(());
        }
        // Files in strictly deeper levels fully contained in a tombstone span and older than it.
        let mut to_drop: Vec<(usize, u64)> = Vec::new();
        let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for (tlevel, start, end, seq) in &tombstones {
            for (dlevel, files) in version.levels.iter().enumerate().skip(tlevel + 1) {
                for f in files {
                    if f.largest_seqnum >= *seq || seen.contains(&f.file_num) {
                        continue;
                    }
                    let fs = encoded_user_key(&f.smallest);
                    let fl = encoded_user_key(&f.largest);
                    // [fs, fl] ⊆ [start, end): start <= fs and fl < end.
                    let contained = self.cmp.compare(start, fs) != std::cmp::Ordering::Greater
                        && self.cmp.compare(fl, end) == std::cmp::Ordering::Less;
                    if contained {
                        to_drop.push((dlevel, f.file_num));
                        seen.insert(f.file_num);
                    }
                }
            }
        }
        if to_drop.is_empty() {
            return Ok(());
        }
        let mut edit = VersionEdit {
            next_file_number: Some(state.vs.next_file_number),
            last_sequence: Some(state.vs.last_sequence),
            ..Default::default()
        };
        for (level, file_num) in &to_drop {
            edit.deleted_files.push((*level, *file_num));
        }
        state.vs.apply(&edit)?;
        if let Some(mw) = state.manifest.as_mut() {
            mw.write_record(&edit.encode())?;
            mw.sync_all()?;
        }
        state.compaction_count += 1;
        for (_, file_num) in &to_drop {
            self.cache.lock().unwrap().remove(file_num);
            self.clean_file(&self.dir.join(filenames::table(*file_num)));
            if let Some(l) = &self.listener {
                l.on_table_deleted(*file_num);
            }
            self.log(&format!(
                "delete-only compaction dropped sstable {file_num}"
            ));
        }
        Ok(())
    }

    /// Elision-only compaction (Pebble): rewrite a single bottom-level file that carries
    /// now-droppable tombstones, physically removing point/range tombstones and the versions
    /// they shadow. At the bottom level a tombstone deletes nothing below it, so it is dead
    /// weight; rewriting reclaims that space. Reuses the normal compaction path with the
    /// output level equal to the input (bottom) level, which both elides tombstones and
    /// keeps every version an open snapshot can still observe (stripe logic).
    ///
    /// Conservative: skipped while any snapshot is open. With no snapshots every version sits
    /// in the top stripe, so all tombstones are elided and the rewritten file carries none —
    /// guaranteeing the rewrite cannot re-trigger itself.
    fn elision_only_compact(&self, state: &mut State) -> Result<()> {
        if !self.open_snapshots().is_empty() {
            return Ok(());
        }
        let bottom = NUM_LEVELS - 1;
        let version = state.vs.current.clone();
        // Pick the first bottom file that actually carries tombstones to drop.
        let mut target = None;
        for f in &version.levels[bottom] {
            let reader = self.open_reader(f.file_num)?;
            let props = reader.properties();
            if props.num_deletions > 0 || props.num_range_deletions > 0 {
                target = Some(f.clone());
                break;
            }
        }
        let Some(file) = target else {
            return Ok(());
        };
        self.log(&format!(
            "elision-only compaction rewriting sstable {} at L{bottom}",
            file.file_num
        ));
        let c = Compaction {
            level: bottom,
            output_level: bottom,
            inputs: vec![file],
            overlap: Vec::new(),
        };
        self.run_compaction(state, c)
    }

    /// The compaction score of a level: how far over its trigger it is. A level with the
    /// highest score above 1.0 is the most in need of compaction. L0 is scored by file
    /// count; L1+ by total size against the level's byte budget.
    fn level_score(&self, version: &Version, level: usize) -> f64 {
        if level == 0 {
            version.levels[0].len() as f64 / self.l0_compaction_threshold as f64
        } else {
            let total: u64 = version.levels[level].iter().map(|f| f.size).sum();
            total as f64 / level_budget(level) as f64
        }
    }

    /// Chooses the next compaction, if any level needs one. Levels are scored and the
    /// highest-scoring level above its trigger is compacted first (Pebble's score-based
    /// picker), rather than always preferring L0.
    fn pick_compaction(&self, version: &Version) -> Option<Compaction> {
        // Find the most-overloaded level (score >= 1.0).
        let mut best_level = None;
        let mut best_score = 1.0;
        for level in 0..NUM_LEVELS - 1 {
            if version.levels[level].is_empty() {
                continue;
            }
            let score = self.level_score(version, level);
            if score >= best_score {
                best_score = score;
                best_level = Some(level);
            }
        }
        let level = best_level?;
        self.build_compaction(version, level)
    }

    /// Builds the compaction descriptor for `level` -> `level+1`: all of L0 (which may
    /// overlap internally) or the first file of an L1+ level, plus the overlapping files
    /// of the output level.
    fn build_compaction(&self, version: &Version, level: usize) -> Option<Compaction> {
        let inputs = if level == 0 {
            version.levels[0].clone()
        } else {
            vec![version.levels[level].first()?.clone()]
        };
        let (min, max) = key_range(self.cmp.as_ref(), &inputs)?;
        let overlap = overlapping(self.cmp.as_ref(), version, level + 1, &min, &max);
        Some(Compaction {
            level,
            output_level: level + 1,
            inputs,
            overlap,
        })
    }

    /// Executes a compaction: merges the inputs, writes outputs, and records the edit.
    fn run_compaction(&self, state: &mut State, c: Compaction) -> Result<()> {
        // Move compaction: a single input file that does not overlap any file in the output
        // level can be relevelled by a MANIFEST edit alone — no rewrite. (Pebble's move
        // compaction.) The file's sstable content is independent of its level, so its keys,
        // tombstones, and range keys are all carried correctly by the move. (An elision-only
        // compaction, where the output level equals the input level, must instead rewrite.)
        if c.inputs.len() == 1 && c.overlap.is_empty() && c.output_level != c.level {
            return self.run_move_compaction(state, c);
        }
        if let Some(l) = &self.listener {
            l.on_compaction_begin(c.output_level, c.inputs.len() + c.overlap.len());
        }
        // Build a merging iterator over every input file and collect their range
        // tombstones, which must be carried to the output (otherwise the deletions
        // would be lost).
        let mut sources: Vec<Box<dyn InternalIter>> = Vec::new();
        let mut tombstones: Vec<RangeTombstone> = Vec::new();
        let mut range_keys: Vec<RangeKeyEntry> = Vec::new();
        for f in c.inputs.iter().chain(c.overlap.iter()) {
            let reader = self.open_reader(f.file_num)?;
            tombstones.extend_from_slice(reader.range_tombstones());
            range_keys.extend_from_slice(reader.range_keys());
            sources.push(Box::new(reader.iter()?));
        }
        let mut merge = MergingIter::new(sources, self.cmp.clone())?;

        // Tombstones (point and range) can be dropped only when compacting into the
        // lowest level, where there is no older data left to shadow. Range keys are
        // always carried (their resolution is deferred to read time).
        let drop_tombstones = c.output_level == NUM_LEVELS - 1;
        let write_tombstones = !drop_tombstones && !tombstones.is_empty();
        let write_range_keys = !range_keys.is_empty();
        tombstones.sort_by(|a, b| {
            self.cmp
                .compare(&a.start, &b.start)
                .then(b.seqnum.cmp(&a.seqnum))
        });
        range_keys.sort_by(|a, b| {
            self.cmp
                .compare(&a.start, &b.start)
                .then(b.seqnum.cmp(&a.seqnum))
                .then(b.kind.as_u8().cmp(&a.kind.as_u8()))
        });

        // Open snapshots define stripe boundaries: within each stripe (the versions
        // between two consecutive snapshot sequence numbers) only the newest version is
        // kept, so every snapshot can still observe the version it needs.
        let snapshots = self.open_snapshots();

        let mut outputs: Vec<FileMetadata> = Vec::new();
        let mut builder: Option<OutputBuilder> = None;
        let mut prev_user: Option<Vec<u8>> = None;
        let mut prev_stripe = usize::MAX;
        // Whether a terminator (Set or a point deletion) has already been written for the
        // current (user key, stripe). Once one is, older versions in the same stripe are
        // shadowed and dropped. Merge operands are *not* terminators, so they accumulate.
        let mut terminated = false;

        while merge.valid() {
            let ikey = merge.key().to_vec();
            let value = merge.value().to_vec();
            merge.advance()?;

            let ukey = encoded_user_key(&ikey);
            let seq = encoded_trailer(&ikey) >> 8;
            let stripe = snapshot_stripe(&snapshots, seq);
            let kind = trailer_kind(encoded_trailer(&ikey));
            let same_user = prev_user
                .as_deref()
                .is_some_and(|p| self.cmp.compare(p, ukey) == std::cmp::Ordering::Equal);
            if !(same_user && stripe == prev_stripe) {
                terminated = false; // entered a new (user key, stripe)
            }
            if terminated {
                continue; // shadowed by a terminator already written for this stripe
            }
            prev_user = Some(ukey.to_vec());
            prev_stripe = stripe;

            // A point key covered by an input range tombstone with a higher sequence
            // number in the same snapshot stripe is deleted by it: drop the key (and, as a
            // terminator, the older versions in this stripe). Same-stripe is required so a
            // snapshot positioned between the key and the tombstone still sees the key.
            // Without this, eliding the range tombstone at the bottom level would resurface
            // the very keys it deleted.
            if !is_tombstone(kind)
                && tombstones.iter().any(|t| {
                    t.seqnum > seq
                        && snapshot_stripe(&snapshots, t.seqnum) == stripe
                        && t.covers(self.cmp.as_ref(), ukey)
                })
            {
                terminated = true;
                continue;
            }

            // Tombstones may be elided only at the bottom level and only in the top
            // stripe (no open snapshot can observe them); doing so also shadows older
            // versions in this stripe.
            if drop_tombstones && is_tombstone(kind) && stripe == 0 {
                terminated = true;
                continue;
            }
            // A Set or a (retained) point deletion terminates the stripe; merges do not.
            if !matches!(kind, InternalKeyKind::Merge) {
                terminated = true;
            }

            if builder.is_none() {
                builder = Some(self.new_output(
                    state,
                    &tombstones,
                    write_tombstones,
                    &range_keys,
                    write_range_keys,
                )?);
            }
            let b = builder.as_mut().unwrap();
            b.add(&ikey, &value)?;
            if b.writer.estimated_size() >= self.target_file_size {
                outputs.push(builder.take().unwrap().finish()?);
            }
        }
        if let Some(b) = builder.take() {
            outputs.push(b.finish()?);
        }

        // If the compaction produced only range deletions/keys (no surviving point keys),
        // still emit a file to carry them.
        if outputs.is_empty() && (write_tombstones || write_range_keys) {
            let b = self.new_output(
                state,
                &tombstones,
                write_tombstones,
                &range_keys,
                write_range_keys,
            )?;
            outputs.push(b.finish()?);
        }

        // Record the edit: delete every input, add every output to the output level.
        let mut edit = VersionEdit {
            next_file_number: Some(state.vs.next_file_number),
            last_sequence: Some(state.vs.last_sequence),
            ..Default::default()
        };
        for f in &c.inputs {
            edit.deleted_files.push((c.level, f.file_num));
        }
        for f in &c.overlap {
            edit.deleted_files.push((c.output_level, f.file_num));
        }
        let num_outputs = outputs.len();
        for meta in outputs {
            edit.new_files.push(NewFileEntry {
                level: c.output_level,
                meta,
            });
        }

        state.vs.apply(&edit)?;
        if let Some(mw) = state.manifest.as_mut() {
            mw.write_record(&edit.encode())?;
            mw.sync_all()?;
        }
        state.compaction_count += 1;
        if let Some(l) = &self.listener {
            l.on_compaction_end(
                c.output_level,
                c.inputs.len() + c.overlap.len(),
                num_outputs,
            );
        }

        // Dispose of the obsolete input files (cache + on disk, via the cleaner).
        for (_, file_num) in &edit.deleted_files {
            self.cache.lock().unwrap().remove(file_num);
            self.clean_file(&self.dir.join(filenames::table(*file_num)));
            if let Some(l) = &self.listener {
                l.on_table_deleted(*file_num);
            }
        }
        Ok(())
    }

    /// Relevels a single non-overlapping file from `c.level` to `c.output_level` via a
    /// MANIFEST edit, without rewriting it (Pebble's move compaction). The file keeps its
    /// number and stays in place on disk; only its level changes.
    fn run_move_compaction(&self, state: &mut State, c: Compaction) -> Result<()> {
        if let Some(l) = &self.listener {
            l.on_compaction_begin(c.output_level, c.inputs.len());
        }
        let meta = c.inputs[0].as_ref().clone();
        let file_num = meta.file_num;
        let mut edit = VersionEdit {
            next_file_number: Some(state.vs.next_file_number),
            last_sequence: Some(state.vs.last_sequence),
            ..Default::default()
        };
        edit.deleted_files.push((c.level, file_num));
        edit.new_files.push(NewFileEntry {
            level: c.output_level,
            meta,
        });

        state.vs.apply(&edit)?;
        if let Some(mw) = state.manifest.as_mut() {
            mw.write_record(&edit.encode())?;
            mw.sync_all()?;
        }
        state.compaction_count += 1;
        // No file is removed from disk: the moved sstable is reused at its new level.
        self.log(&format!(
            "moved sstable {file_num} from L{} to L{}",
            c.level, c.output_level
        ));
        if let Some(l) = &self.listener {
            l.on_compaction_end(c.output_level, c.inputs.len(), 1);
        }
        Ok(())
    }

    /// Creates a fresh output builder, seeding it with the carried range tombstones and
    /// range keys.
    fn new_output(
        &self,
        state: &mut State,
        tombstones: &[RangeTombstone],
        write_tombstones: bool,
        range_keys: &[RangeKeyEntry],
        write_range_keys: bool,
    ) -> Result<OutputBuilder> {
        let file_num = state.vs.allocate_file_number();
        let mut b = OutputBuilder::new(self, file_num)?;
        if write_tombstones {
            for t in tombstones {
                b.add_range_del(&t.start, &t.end, t.seqnum)?;
            }
        }
        if write_range_keys {
            for rk in range_keys {
                b.add_range_key(rk)?;
            }
        }
        Ok(b)
    }
}

/// Accumulates entries into one output sstable during compaction, tracking the file's
/// key-range and sequence-number bounds across both point keys and range tombstones.
struct OutputBuilder {
    file_num: u64,
    path: std::path::PathBuf,
    writer: Writer<Box<dyn WritableFile>>,
    cmp_dyn: Arc<dyn crate::base::comparer::Comparer>,
    fs: Arc<dyn Fs>,
    smallest: Option<Vec<u8>>,
    largest: Option<Vec<u8>>,
    smallest_seq: u64,
    largest_seq: u64,
}

impl OutputBuilder {
    fn new(db: &DbInner, file_num: u64) -> Result<OutputBuilder> {
        let path = db.dir.join(filenames::table(file_num));
        let mut writer = Writer::new(
            db.fs.create(&path)?,
            db.cmp.clone(),
            WriterOptions::default(),
        );
        for factory in &db.block_property_collectors {
            writer.add_block_property_collector(factory());
        }
        Ok(OutputBuilder {
            file_num,
            path,
            writer,
            cmp_dyn: db.cmp.clone(),
            fs: db.fs.clone(),
            smallest: None,
            largest: None,
            smallest_seq: u64::MAX,
            largest_seq: 0,
        })
    }

    /// Updates the key-range bounds with an encoded internal key.
    fn extend_bounds(&mut self, ikey: &[u8], seq: u64) {
        let cmp = self.cmp_dyn.as_ref();
        let ukey = encoded_user_key(ikey);
        if self
            .smallest
            .as_deref()
            .is_none_or(|s| cmp.compare(ukey, encoded_user_key(s)) == std::cmp::Ordering::Less)
        {
            self.smallest = Some(ikey.to_vec());
        }
        if self
            .largest
            .as_deref()
            .is_none_or(|l| cmp.compare(ukey, encoded_user_key(l)) == std::cmp::Ordering::Greater)
        {
            self.largest = Some(ikey.to_vec());
        }
        self.smallest_seq = self.smallest_seq.min(seq);
        self.largest_seq = self.largest_seq.max(seq);
    }

    fn add(&mut self, ikey: &[u8], value: &[u8]) -> Result<()> {
        self.writer.add(ikey, value)?;
        self.extend_bounds(ikey, encoded_trailer(ikey) >> 8);
        Ok(())
    }

    fn add_range_del(&mut self, start: &[u8], end: &[u8], seqnum: u64) -> Result<()> {
        use crate::base::internal_key::{InternalKey, InternalKeyKind};
        let start_ikey =
            InternalKey::new(start.to_vec(), seqnum, InternalKeyKind::RangeDelete).encode();
        self.writer.add(&start_ikey, end)?;
        self.extend_bounds(&start_ikey, seqnum);
        // The exclusive end extends the largest user-key bound.
        let end_ikey =
            InternalKey::new(end.to_vec(), seqnum, InternalKeyKind::RangeDelete).encode();
        self.extend_bounds(&end_ikey, seqnum);
        Ok(())
    }

    fn add_range_key(&mut self, rk: &RangeKeyEntry) -> Result<()> {
        use crate::base::internal_key::InternalKey;
        let start_ikey = InternalKey::new(rk.start.clone(), rk.seqnum, rk.kind).encode();
        self.writer.add(&start_ikey, &rk.value)?;
        self.extend_bounds(&start_ikey, rk.seqnum);
        if let Ok(end) = rk.end() {
            let end_ikey = InternalKey::new(end, rk.seqnum, rk.kind).encode();
            self.extend_bounds(&end_ikey, rk.seqnum);
        }
        Ok(())
    }

    fn finish(self) -> Result<FileMetadata> {
        let mut file = self.writer.finish()?;
        file.sync_all()?;
        let size = self.fs.size(&self.path)?;
        Ok(FileMetadata {
            file_num: self.file_num,
            size,
            smallest: self.smallest.unwrap_or_default(),
            largest: self.largest.unwrap_or_default(),
            smallest_seqnum: self.smallest_seq.min(self.largest_seq),
            largest_seqnum: self.largest_seq,
        })
    }
}

/// Returns the snapshot stripe a version with sequence number `seq` belongs to: the
/// number of open snapshots whose sequence number is `>= seq`. Versions in the same
/// stripe (no snapshot boundary between them) are interchangeable, so only the newest is
/// kept. Stripe `0` is the top stripe, above every snapshot.
fn snapshot_stripe(snapshots: &[u64], seq: u64) -> usize {
    snapshots.iter().filter(|&&s| s >= seq).count()
}

fn is_tombstone(kind: InternalKeyKind) -> bool {
    matches!(
        kind,
        InternalKeyKind::Delete | InternalKeyKind::SingleDelete | InternalKeyKind::DeleteSized
    )
}

/// The `[min, max]` user-key range spanned by `files` (encoded user keys).
fn key_range(
    cmp: &dyn crate::base::comparer::Comparer,
    files: &[Arc<FileMetadata>],
) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut min: Option<Vec<u8>> = None;
    let mut max: Option<Vec<u8>> = None;
    for f in files {
        let s = encoded_user_key(&f.smallest);
        let l = encoded_user_key(&f.largest);
        if min
            .as_deref()
            .is_none_or(|m| cmp.compare(s, m) == std::cmp::Ordering::Less)
        {
            min = Some(s.to_vec());
        }
        if max
            .as_deref()
            .is_none_or(|m| cmp.compare(l, m) == std::cmp::Ordering::Greater)
        {
            max = Some(l.to_vec());
        }
    }
    Some((min?, max?))
}

/// Files at `level` whose user-key range intersects `[min, max]`.
fn overlapping(
    cmp: &dyn crate::base::comparer::Comparer,
    version: &Version,
    level: usize,
    min: &[u8],
    max: &[u8],
) -> Vec<Arc<FileMetadata>> {
    version.levels[level]
        .iter()
        .filter(|f| {
            let s = encoded_user_key(&f.smallest);
            let l = encoded_user_key(&f.largest);
            // Ranges [s, l] and [min, max] intersect iff s <= max and l >= min.
            cmp.compare(s, max) != std::cmp::Ordering::Greater
                && cmp.compare(l, min) != std::cmp::Ordering::Less
        })
        .cloned()
        .collect()
}
