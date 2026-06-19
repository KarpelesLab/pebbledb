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
//! splitting by target file size. Because open snapshots are not tracked, compaction
//! keeps only the newest version of each key; reads at old explicit snapshots may
//! therefore observe collapsed history.

use std::fs::File;
use std::sync::Arc;

use crate::Result;
use crate::base::internal_key::{InternalKeyKind, encoded_trailer, encoded_user_key, trailer_kind};
use crate::manifest::{FileMetadata, NUM_LEVELS, NewFileEntry, Version, VersionEdit};
use crate::sstable::{Writer, WriterOptions};

use super::merging_iter::{InternalIter, MergingIter};
use super::{Db, State, filenames};

/// Number of L0 files that triggers an L0 -> L1 compaction.
const L0_COMPACTION_THRESHOLD: usize = 4;
/// Target size of an output sstable before it is split.
const TARGET_FILE_SIZE: u64 = 2 << 20;
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

impl Db {
    /// Picks and runs compactions until none are needed (or a safety cap is hit).
    pub(super) fn maybe_compact(&self, state: &mut State) -> Result<()> {
        for _ in 0..MAX_COMPACTIONS_PER_CALL {
            match self.pick_compaction(&state.vs.current) {
                Some(c) => self.run_compaction(state, c)?,
                None => break,
            }
        }
        Ok(())
    }

    /// Chooses the next compaction, if any level needs one.
    fn pick_compaction(&self, version: &Version) -> Option<Compaction> {
        // L0: trigger on file count.
        if version.levels[0].len() >= L0_COMPACTION_THRESHOLD {
            let inputs = version.levels[0].clone();
            let (min, max) = key_range(self.cmp.as_ref(), &inputs)?;
            let overlap = overlapping(self.cmp.as_ref(), version, 1, &min, &max);
            return Some(Compaction {
                level: 0,
                output_level: 1,
                inputs,
                overlap,
            });
        }
        // L1..L(max-1): trigger on total size.
        for level in 1..NUM_LEVELS - 1 {
            let total: u64 = version.levels[level].iter().map(|f| f.size).sum();
            if total > level_budget(level) && !version.levels[level].is_empty() {
                let file = version.levels[level][0].clone();
                let min = encoded_user_key(&file.smallest).to_vec();
                let max = encoded_user_key(&file.largest).to_vec();
                let overlap = overlapping(self.cmp.as_ref(), version, level + 1, &min, &max);
                return Some(Compaction {
                    level,
                    output_level: level + 1,
                    inputs: vec![file],
                    overlap,
                });
            }
        }
        None
    }

    /// Executes a compaction: merges the inputs, writes outputs, and records the edit.
    fn run_compaction(&self, state: &mut State, c: Compaction) -> Result<()> {
        // Build a merging iterator over every input file.
        let mut sources: Vec<Box<dyn InternalIter>> = Vec::new();
        for f in c.inputs.iter().chain(c.overlap.iter()) {
            let reader = self.open_reader(f.file_num)?;
            sources.push(Box::new(reader.iter()?));
        }
        let mut merge = MergingIter::new(sources, self.cmp.clone())?;

        // Tombstones can be dropped only when compacting into the lowest level.
        let drop_tombstones = c.output_level == NUM_LEVELS - 1;

        let mut outputs: Vec<FileMetadata> = Vec::new();
        let mut builder: Option<OutputBuilder> = None;
        let mut prev_user: Option<Vec<u8>> = None;

        while merge.valid() {
            let ikey = merge.key().to_vec();
            let value = merge.value().to_vec();
            merge.advance()?;

            let ukey = encoded_user_key(&ikey);
            let is_new = prev_user
                .as_deref()
                .is_none_or(|p| self.cmp.compare(p, ukey) != std::cmp::Ordering::Equal);
            if !is_new {
                continue; // an older, shadowed version of the same user key
            }
            prev_user = Some(ukey.to_vec());

            let kind = trailer_kind(encoded_trailer(&ikey));
            if drop_tombstones && is_tombstone(kind) {
                continue;
            }

            if builder.is_none() {
                let file_num = state.vs.allocate_file_number();
                builder = Some(OutputBuilder::new(self, file_num)?);
            }
            let b = builder.as_mut().unwrap();
            b.add(&ikey, &value)?;
            if b.writer.estimated_size() >= TARGET_FILE_SIZE {
                outputs.push(builder.take().unwrap().finish()?);
            }
        }
        if let Some(b) = builder.take() {
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
        for meta in outputs {
            edit.new_files.push(NewFileEntry {
                level: c.output_level,
                meta,
            });
        }

        state.vs.apply(&edit)?;
        if let Some(mw) = state.manifest.as_mut() {
            mw.write_record(&edit.encode())?;
            mw.flush()?;
        }

        // Remove the obsolete input files from the cache and disk.
        for (_, file_num) in &edit.deleted_files {
            self.cache.lock().unwrap().remove(file_num);
            let _ = std::fs::remove_file(self.dir.join(filenames::table(*file_num)));
        }
        Ok(())
    }
}

/// Accumulates entries into one output sstable during compaction.
struct OutputBuilder {
    file_num: u64,
    path: std::path::PathBuf,
    writer: Writer<File>,
    smallest: Option<Vec<u8>>,
    largest: Vec<u8>,
    smallest_seq: u64,
    largest_seq: u64,
}

impl OutputBuilder {
    fn new(db: &Db, file_num: u64) -> Result<OutputBuilder> {
        let path = db.dir.join(filenames::table(file_num));
        let writer = Writer::new(
            File::create(&path)?,
            db.cmp.clone(),
            WriterOptions::default(),
        );
        Ok(OutputBuilder {
            file_num,
            path,
            writer,
            smallest: None,
            largest: Vec::new(),
            smallest_seq: u64::MAX,
            largest_seq: 0,
        })
    }

    fn add(&mut self, ikey: &[u8], value: &[u8]) -> Result<()> {
        self.writer.add(ikey, value)?;
        if self.smallest.is_none() {
            self.smallest = Some(ikey.to_vec());
        }
        self.largest.clear();
        self.largest.extend_from_slice(ikey);
        let seq = encoded_trailer(ikey) >> 8;
        self.smallest_seq = self.smallest_seq.min(seq);
        self.largest_seq = self.largest_seq.max(seq);
        Ok(())
    }

    fn finish(self) -> Result<FileMetadata> {
        self.writer.finish()?;
        let size = std::fs::metadata(&self.path)?.len();
        Ok(FileMetadata {
            file_num: self.file_num,
            size,
            smallest: self.smallest.unwrap_or_default(),
            largest: self.largest,
            smallest_seqnum: self.smallest_seq.min(self.largest_seq),
            largest_seqnum: self.largest_seq,
        })
    }
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
