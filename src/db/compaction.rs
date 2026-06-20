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

use std::io::Write;
use std::sync::Arc;

use crate::Result;
use crate::base::internal_key::{InternalKeyKind, encoded_trailer, encoded_user_key, trailer_kind};
use crate::base::range_del::RangeTombstone;
use crate::base::range_key::RangeKeyEntry;
use crate::manifest::{FileMetadata, NUM_LEVELS, NewFileEntry, Version, VersionEdit};
use crate::sstable::Writer;
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
    /// Intermediate-level files (at `level + 1`) for a **multilevel** compaction whose output
    /// goes to `level + 2`. Empty for an ordinary two-level compaction.
    mid: Vec<Arc<FileMetadata>>,
}

/// The size budget for a level before it is compacted downward: `l1_max_bytes` at L1,
/// growing 10x per level (Pebble's `LBaseMaxBytes` shape).
fn level_budget(l1_max_bytes: u64, level: usize) -> u64 {
    let mut budget = l1_max_bytes.max(1);
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

        // Walk levels from the top, pushing any file overlapping the range one level down,
        // until the data has reached the bottom level. The loop is bounded by the work
        // actually available (each pass strictly reduces the files above the bottom that
        // overlap the range, or stops). Each compaction is built under a brief lock (marking
        // its inputs) and run off-lock.
        for _ in 0..MAX_COMPACTIONS_PER_CALL {
            let mut did_work = false;
            for level in 0..NUM_LEVELS - 1 {
                let c = {
                    let mut state = self.state.lock().unwrap();
                    let inputs: Vec<_> = self
                        .range_overlap(&state.vs.current, level, start, end)
                        .filter(|f| !state.compacting.contains(&f.file_num))
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
                        mid: Vec::new(),
                    };
                    if Self::any_compacting(&state, &c) {
                        continue;
                    }
                    Self::mark_compacting(&mut state, &c);
                    c
                };
                self.run_compaction(c)?;
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

    /// The output sstable size target for a compaction producing files at `level`: the
    /// per-level override from `level_target_file_sizes` if present, else `target_file_size`.
    fn target_file_size_for(&self, level: usize) -> u64 {
        self.level_target_file_sizes
            .get(level)
            .copied()
            .filter(|&s| s > 0)
            .unwrap_or(self.target_file_size)
    }

    /// Marks a compaction's inputs as in-progress so other workers' pickers skip them.
    fn mark_compacting(state: &mut State, c: &Compaction) {
        for f in c.inputs.iter().chain(c.mid.iter()).chain(c.overlap.iter()) {
            state.compacting.insert(f.file_num);
        }
    }

    /// Whether any of a candidate compaction's inputs are already being compacted.
    fn any_compacting(state: &State, c: &Compaction) -> bool {
        c.inputs
            .iter()
            .chain(c.mid.iter())
            .chain(c.overlap.iter())
            .any(|f| state.compacting.contains(&f.file_num))
    }

    /// Picks and runs compactions until none are needed (or a safety cap is hit). Each
    /// compaction is picked under a brief lock (marking its inputs `compacting`) and then run
    /// off-lock by [`run_compaction`](Self::run_compaction), so this is safe to call from
    /// several background workers at once — they pick disjoint inputs.
    pub(super) fn maybe_compact(&self) -> Result<()> {
        // Service files queued by read-triggered compaction first.
        self.run_read_compactions()?;
        // Opportunistically drop whole files shadowed by a covering range tombstone before
        // scoring levels — this is free (a MANIFEST edit) and shrinks the work below.
        self.delete_only_compact()?;
        // Rewrite bottom-level files that carry now-droppable tombstones.
        self.elision_only_compact()?;
        for _ in 0..MAX_COMPACTIONS_PER_CALL {
            let c = {
                let mut state = self.state.lock().unwrap();
                match self.pick_compaction(&state.vs.current) {
                    Some(c) if !Self::any_compacting(&state, &c) => {
                        Self::mark_compacting(&mut state, &c);
                        Some(c)
                    }
                    _ => None,
                }
            };
            match c {
                Some(c) => self.run_compaction(c)?,
                None => break,
            }
        }
        // With levels within budget, compact tombstone-dense files downward to reclaim space.
        for _ in 0..MAX_COMPACTIONS_PER_CALL {
            if !self.tombstone_density_compact()? {
                break;
            }
        }
        // Reap any obsolete files whose last in-flight reader has since gone away.
        self.collect_obsolete();
        Ok(())
    }

    /// Read-triggered compaction (Pebble): compact files queued in `state.read_queue` because
    /// reads repeatedly passed through them to find keys in deeper levels. Each queued L1+
    /// file is compacted one level down (rewriting when there is overlapping data below,
    /// otherwise moving), reducing the read amplification of hot ranges. L0 entries are
    /// skipped (a single L0 file may not hold the newest version of its keys).
    pub(super) fn run_read_compactions(&self) -> Result<()> {
        loop {
            // Build the next read-triggered compaction under a brief lock, marking its inputs.
            let c = {
                let mut state = self.state.lock().unwrap();
                let Some(file_num) = ({
                    let q = &mut state.read_queue;
                    (!q.is_empty()).then(|| q.remove(0))
                }) else {
                    break;
                };
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
                let Some((min, max)) = key_range(self.cmp.as_ref(), std::slice::from_ref(&f))
                else {
                    continue;
                };
                let overlap =
                    overlapping(self.cmp.as_ref(), &state.vs.current, level + 1, &min, &max);
                let c = Compaction {
                    level,
                    output_level: level + 1,
                    inputs: vec![f],
                    overlap,
                    mid: Vec::new(),
                };
                if Self::any_compacting(&state, &c) {
                    continue;
                }
                Self::mark_compacting(&mut state, &c);
                self.log(&format!(
                    "read-triggered compaction of sstable {file_num} at L{level}"
                ));
                c
            };
            self.run_compaction(c)?;
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
    fn tombstone_density_compact(&self) -> Result<bool> {
        if self.tombstone_dense_compaction_threshold > 1.0 {
            return Ok(false);
        }
        // Find a candidate and mark it under a brief lock, then run it off-lock.
        let c = {
            let mut state = self.state.lock().unwrap();
            let version = state.vs.current.clone();
            // Start at L1: L0 files are not range-partitioned, so a single L0 file may not hold
            // the newest version of its keys — compacting it down in isolation could push a
            // newer version below an older one. (Whole-L0 compactions go through the picker.)
            let mut chosen = None;
            'outer: for level in 1..NUM_LEVELS - 1 {
                for f in &version.levels[level] {
                    if state.compacting.contains(&f.file_num) {
                        continue;
                    }
                    let reader = self.open_reader(f.file_num)?;
                    let props = reader.properties();
                    if props.num_entries == 0 {
                        continue;
                    }
                    let frac = props.num_deletions as f64 / props.num_entries as f64;
                    if frac < self.tombstone_dense_compaction_threshold {
                        continue;
                    }
                    let Some((min, max)) = key_range(self.cmp.as_ref(), std::slice::from_ref(f))
                    else {
                        continue;
                    };
                    let overlap = overlapping(self.cmp.as_ref(), &version, level + 1, &min, &max);
                    self.log(&format!(
                        "tombstone-density compaction of sstable {} at L{level} (fraction {frac:.2})",
                        f.file_num
                    ));
                    chosen = Some(Compaction {
                        level,
                        output_level: level + 1,
                        inputs: vec![f.clone()],
                        overlap,
                        mid: Vec::new(),
                    });
                    break 'outer;
                }
            }
            let Some(c) = chosen else {
                return Ok(false);
            };
            Self::mark_compacting(&mut state, &c);
            c
        };
        self.run_compaction(c)?;
        Ok(true)
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
    fn delete_only_compact(&self) -> Result<()> {
        if !self.open_snapshots().is_empty() {
            return Ok(());
        }
        let mut state = self.state.lock().unwrap();
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
        let mut to_drop: Vec<(usize, Arc<FileMetadata>)> = Vec::new();
        let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for (tlevel, start, end, seq) in &tombstones {
            for (dlevel, files) in version.levels.iter().enumerate().skip(tlevel + 1) {
                for f in files {
                    // Skip files an in-progress compaction is using as input — dropping them
                    // would pull the rug out from under that compaction's reads.
                    if f.largest_seqnum >= *seq
                        || seen.contains(&f.file_num)
                        || state.compacting.contains(&f.file_num)
                    {
                        continue;
                    }
                    let fs = encoded_user_key(&f.smallest);
                    let fl = encoded_user_key(&f.largest);
                    // [fs, fl] ⊆ [start, end): start <= fs and fl < end.
                    let contained = self.cmp.compare(start, fs) != std::cmp::Ordering::Greater
                        && self.cmp.compare(fl, end) == std::cmp::Ordering::Less;
                    if contained {
                        to_drop.push((dlevel, f.clone()));
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
        for (level, f) in &to_drop {
            edit.deleted_files.push((*level, f.file_num));
        }
        state.vs.apply(&edit)?;
        if let Some(mw) = state.manifest.as_mut() {
            mw.write_record(&edit.encode())?;
            mw.sync_all()?;
        }
        state.compaction_count += 1;
        // Defer deletion until no in-flight read still references these files.
        for (_, f) in &to_drop {
            state.obsolete.push((f.file_num, f.clone()));
            self.log(&format!(
                "delete-only compaction dropped sstable {}",
                f.file_num
            ));
        }
        drop(state);
        self.collect_obsolete();
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
    fn elision_only_compact(&self) -> Result<()> {
        if !self.open_snapshots().is_empty() {
            return Ok(());
        }
        let bottom = NUM_LEVELS - 1;
        // Build the candidate compaction under a brief lock, marking its input.
        let c = {
            let mut state = self.state.lock().unwrap();
            let version = state.vs.current.clone();
            let mut target = None;
            for f in &version.levels[bottom] {
                if state.compacting.contains(&f.file_num) {
                    continue;
                }
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
                mid: Vec::new(),
            };
            Self::mark_compacting(&mut state, &c);
            c
        };
        self.run_compaction(c)
    }

    /// The compaction score of a level: how far over its trigger it is. A level with the
    /// highest score above 1.0 is the most in need of compaction. L0 is scored by file
    /// count; L1+ by total size against the level's byte budget.
    fn level_score(&self, version: &Version, level: usize) -> f64 {
        if level == 0 {
            version.levels[0].len() as f64 / self.l0_compaction_threshold as f64
        } else {
            let total: u64 = version.levels[level].iter().map(|f| f.size).sum();
            total as f64 / level_budget(self.l1_max_bytes, level) as f64
        }
    }

    /// Whether a score-based compaction is available that does not collide with one already in
    /// progress — used as a background-worker wake predicate.
    pub(super) fn compaction_available(&self, state: &State) -> bool {
        self.pick_compaction(&state.vs.current)
            .is_some_and(|c| !Self::any_compacting(state, &c))
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

        // Multilevel compaction (Pebble): when the output (`level+1`) and the level below it
        // (`level+2`) both already hold overlapping data, a plain `level -> level+1`
        // compaction would immediately invite a `level+1 -> level+2` one. Fold all three
        // levels into a single compaction that writes straight to `level+2`, avoiding
        // rewriting the intermediate data twice. Only done for a single-file `level` input
        // (so the merged size stays bounded) and when it would not reach below the bottom.
        if level + 2 < NUM_LEVELS && inputs.len() == 1 && !overlap.is_empty() {
            // Combined key range of `level` + `level+1` inputs.
            let mut combined = inputs.clone();
            combined.extend(overlap.iter().cloned());
            if let Some((cmin, cmax)) = key_range(self.cmp.as_ref(), &combined) {
                let overlap2 = overlapping(self.cmp.as_ref(), version, level + 2, &cmin, &cmax);
                if !overlap2.is_empty() {
                    return Some(Compaction {
                        level,
                        output_level: level + 2,
                        inputs,
                        mid: overlap,      // the intermediate `level+1` files
                        overlap: overlap2, // the `level+2` output-level files
                    });
                }
            }
        }

        Some(Compaction {
            level,
            output_level: level + 1,
            inputs,
            overlap,
            mid: Vec::new(),
        })
    }

    /// Executes a compaction whose inputs were marked `compacting` by the picker. The
    /// expensive merge and sstable writes run **without** the state lock — only the output
    /// file-number reservation (phase A) and the MANIFEST edit (phase C) hold it — so reads,
    /// writes, and other compactions proceed concurrently. The input files' `compacting`
    /// marks are cleared when the edit applies (or, for a move, inside `run_move_compaction`).
    fn run_compaction(&self, c: Compaction) -> Result<()> {
        // Phase A (locked): a single non-overlapping file is relevelled by a MANIFEST edit
        // alone (move compaction); otherwise reserve enough output file numbers — output
        // bytes never exceed total input bytes — and snapshot the open-snapshot list.
        let (output_nums, snapshots) = {
            let mut state = self.state.lock().unwrap();
            if c.inputs.len() == 1
                && c.overlap.is_empty()
                && c.mid.is_empty()
                && c.output_level != c.level
            {
                return self.run_move_compaction(&mut state, c);
            }
            let total_in: u64 = c
                .inputs
                .iter()
                .chain(c.mid.iter())
                .chain(c.overlap.iter())
                .map(|f| f.size)
                .sum();
            let target = self.target_file_size_for(c.output_level).max(1);
            let n = (total_in / target + 2) as usize;
            let nums: Vec<u64> = (0..n).map(|_| state.vs.allocate_file_number()).collect();
            (nums, self.open_snapshots())
        };
        let mut output_nums = output_nums.into_iter();

        let input_count = c.inputs.len() + c.mid.len() + c.overlap.len();
        if !c.mid.is_empty() {
            self.log(&format!(
                "multilevel compaction L{}+L{}+L{} -> L{}",
                c.level,
                c.level + 1,
                c.output_level,
                c.output_level
            ));
        }
        if let Some(l) = &self.listener {
            l.on_compaction_begin(c.output_level, input_count);
        }
        // Build a merging iterator over every input file and collect their range
        // tombstones, which must be carried to the output (otherwise the deletions
        // would be lost). For a multilevel compaction the intermediate (`mid`) files are
        // merged too.
        let mut sources: Vec<Box<dyn InternalIter>> = Vec::new();
        let mut tombstones: Vec<RangeTombstone> = Vec::new();
        let mut range_keys: Vec<RangeKeyEntry> = Vec::new();
        for f in c.inputs.iter().chain(c.mid.iter()).chain(c.overlap.iter()) {
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

        // `snapshots` (the open-snapshot stripe boundaries) was captured under the phase-A
        // lock. A snapshot opening during this unlocked write takes a sequence number above
        // all compacted data, so it lands in the top stripe — whose newest version per key is
        // always kept — and is therefore unaffected by this compaction.

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
            // A blob-referenced value can be carried over to the output without rewriting it
            // (cross-sstable sharing); otherwise take the resolved value bytes.
            let blob_ref = merge.blob_ref();
            let value = if blob_ref.is_none() {
                merge.value().to_vec()
            } else {
                Vec::new()
            };
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
                let num = output_nums.next().ok_or_else(|| {
                    crate::Error::InvalidState(
                        "compaction: out of preallocated file numbers".into(),
                    )
                })?;
                builder = Some(self.new_output(
                    num,
                    &tombstones,
                    write_tombstones,
                    &range_keys,
                    write_range_keys,
                )?);
            }
            let b = builder.as_mut().unwrap();
            match blob_ref {
                Some((blob_num, handle)) => b.add_preserved_blob(&ikey, blob_num, handle)?,
                None => b.add(&ikey, &value)?,
            }
            if b.writer.estimated_size() >= self.target_file_size_for(c.output_level) {
                outputs.push(builder.take().unwrap().finish()?);
            }
        }
        if let Some(b) = builder.take() {
            outputs.push(b.finish()?);
        }

        // If the compaction produced only range deletions/keys (no surviving point keys),
        // still emit a file to carry them.
        if outputs.is_empty() && (write_tombstones || write_range_keys) {
            let num = output_nums.next().ok_or_else(|| {
                crate::Error::InvalidState("compaction: out of preallocated file numbers".into())
            })?;
            let b = self.new_output(
                num,
                &tombstones,
                write_tombstones,
                &range_keys,
                write_range_keys,
            )?;
            outputs.push(b.finish()?);
        }

        // Phase C (locked): record the edit — delete every input, add every output to the
        // output level — apply it, and clear the inputs' `compacting` marks.
        let mut state = self.state.lock().unwrap();
        let mut edit = VersionEdit {
            next_file_number: Some(state.vs.next_file_number),
            last_sequence: Some(state.vs.last_sequence),
            ..Default::default()
        };
        for f in &c.inputs {
            edit.deleted_files.push((c.level, f.file_num));
        }
        // Intermediate-level files (multilevel) live at `level + 1`.
        for f in &c.mid {
            edit.deleted_files.push((c.level + 1, f.file_num));
        }
        for f in &c.overlap {
            edit.deleted_files.push((c.output_level, f.file_num));
        }
        let num_outputs = outputs.len();
        let output_bytes: u64 = outputs.iter().map(|m| m.size).sum();
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
        state.compaction_bytes += output_bytes;
        // Clear the inputs' `compacting` marks now that they are removed from the version.
        for f in c.inputs.iter().chain(c.mid.iter()).chain(c.overlap.iter()) {
            state.compacting.remove(&f.file_num);
        }
        drop(state);
        // Wake other workers to pick up any follow-up compaction this one enabled.
        self.work_cv.notify_all();
        if let Some(l) = &self.listener {
            l.on_compaction_end(c.output_level, input_count, num_outputs);
        }

        // Mark the obsolete input files for deletion. They are only removed once no live
        // version snapshot (held by an in-flight read) still references them — see
        // `collect_obsolete` — so a concurrent read can't open a file mid-deletion.
        self.enqueue_obsolete(c.inputs.iter().chain(c.mid.iter()).chain(c.overlap.iter()));
        self.collect_obsolete();
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
        // Clear the input's `compacting` mark (it was set by the picker).
        state.compacting.remove(&file_num);
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
        file_num: u64,
        tombstones: &[RangeTombstone],
        write_tombstones: bool,
        range_keys: &[RangeKeyEntry],
        write_range_keys: bool,
    ) -> Result<OutputBuilder> {
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
            // Blob-file rewrite: compaction reads each input value (resolving any blob
            // reference) and, when blob separation is enabled, re-stores large values in this
            // output's own blob file — so large values stay out of the sstable across
            // compactions. Each input's blob file becomes obsolete with its sstable.
            super::engine_writer_options(
                db.value_block_threshold,
                db.blob_value_threshold,
                file_num,
            ),
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

    /// Adds a point entry whose value stays in the existing blob file `blob_num` (cross-sstable
    /// blob sharing): the value is not rewritten, only the reference is carried over.
    fn add_preserved_blob(
        &mut self,
        ikey: &[u8],
        blob_num: u64,
        handle: crate::sstable::blob::BlobHandle,
    ) -> Result<()> {
        self.writer.add_preserved_blob(ikey, blob_num, handle)?;
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

    fn finish(mut self) -> Result<FileMetadata> {
        let blob_bytes = self.writer.take_blob_file()?;
        let blob_refs = self.writer.blob_refs().to_vec();
        let mut file = self.writer.finish()?;
        file.sync_all()?;
        // Write this output's sibling blob file, if blob rewrite separated any new values.
        if let Some(b) = &blob_bytes {
            let blob_path = self.path.with_file_name(filenames::blob(self.file_num));
            let mut bf = self.fs.create(&blob_path)?;
            bf.write_all(b)?;
            bf.sync_all()?;
        }
        let size = self.fs.size(&self.path)?;
        Ok(FileMetadata {
            file_num: self.file_num,
            size,
            smallest: self.smallest.unwrap_or_default(),
            largest: self.largest.unwrap_or_default(),
            smallest_seqnum: self.smallest_seq.min(self.largest_seq),
            largest_seqnum: self.largest_seq,
            blob_refs,
            backing: None,
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
