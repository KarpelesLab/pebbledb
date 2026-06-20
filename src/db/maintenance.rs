// Copyright (c) 2013 The LevelDB-Go Authors. All rights reserved.
// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Ported from Pebble's checkpoint.go and ingest.go.

//! Database maintenance operations: consistent checkpoints and external sstable ingestion.
//!
//! [`Db::checkpoint`] writes a self-contained, openable copy of the database into another
//! directory. [`Db::ingest`] adds externally-built sstables to the LSM, stamping every
//! one of their keys with a single freshly-assigned sequence number (so the ingested data
//! sorts after everything already present) and recording the new files in the MANIFEST.

use std::path::Path;
use std::sync::Arc;

use crate::Result;
use crate::base::internal_key::{InternalKey, InternalKeyKind, encoded_user_key, trailer_kind};
use crate::manifest::{FileMetadata, NewFileEntry, VersionEdit};
use crate::record;
use crate::sstable::{Reader, Writer};
use crate::vfs::WritableFile;

use super::{DbInner, filenames, update_marker};

/// Options for [`Db::checkpoint_with_options`](DbInner::checkpoint_with_options), mirroring
/// Pebble's `CheckpointOptions`.
#[derive(Clone)]
pub struct CheckpointOptions {
    /// Flush the memtable before checkpointing so in-memory data is captured (default `true`).
    /// When `false`, only data already in sstables is copied.
    pub flush: bool,
    /// If non-empty, only sstables overlapping one of these inclusive-start/exclusive-end
    /// `[start, end)` key spans are copied (Pebble's `RestrictToSpans`).
    pub spans: Vec<(Vec<u8>, Vec<u8>)>,
}

impl Default for CheckpointOptions {
    fn default() -> CheckpointOptions {
        CheckpointOptions {
            flush: true,
            spans: Vec::new(),
        }
    }
}

impl DbInner {
    /// Writes a consistent, self-contained copy of the database into `dest`, which must
    /// not already be an open database. The active memtable is first flushed so all data
    /// lives in sstables; those sstables are copied and a fresh single-record MANIFEST
    /// (plus its marker) describing them is written. The result can be opened like any
    /// other database.
    pub fn checkpoint(&self, dest: impl AsRef<Path>) -> Result<()> {
        self.checkpoint_with_options(dest, &CheckpointOptions::default())
    }

    /// Like [`checkpoint`](Self::checkpoint) but honoring [`CheckpointOptions`]: optionally
    /// skipping the pre-checkpoint flush (capturing only already-durable data) and/or
    /// restricting the copy to sstables overlapping a set of key spans (Pebble's
    /// `RestrictToSpans`). A span-restricted checkpoint's MANIFEST lists only the copied
    /// files, so it opens as a self-contained database over just those spans.
    pub fn checkpoint_with_options(
        &self,
        dest: impl AsRef<Path>,
        opts: &CheckpointOptions,
    ) -> Result<()> {
        let dest = dest.as_ref();
        self.fs.create_dir_all(dest)?;

        // Make every live key durable in an sstable before snapshotting the version.
        if opts.flush && !self.state.lock().unwrap().read_only {
            self.flush()?;
        }
        let state = self.state.lock().unwrap();
        let mut edit = state.vs.snapshot_edit();

        // Restrict to files overlapping one of the requested spans, if any.
        if !opts.spans.is_empty() {
            let cmp = self.cmp.as_ref();
            edit.new_files.retain(|nf| {
                let lo = encoded_user_key(&nf.meta.smallest);
                let hi = encoded_user_key(&nf.meta.largest);
                opts.spans.iter().any(|(start, end)| {
                    // [lo, hi] intersects [start, end): hi >= start and lo < end.
                    cmp.compare(hi, start) != std::cmp::Ordering::Less
                        && cmp.compare(lo, end) == std::cmp::Ordering::Less
                })
            });
        }

        // Copy each retained sstable verbatim, along with its sibling blob file (the
        // out-of-line large values) when present — without it the copy's blob references
        // would be unresolvable.
        for nf in &edit.new_files {
            let name = filenames::table(nf.meta.file_num);
            let bytes = self.fs.read(&self.dir.join(&name))?;
            let mut w = self.fs.create(&dest.join(&name))?;
            w.write_all(&bytes)?;
            w.sync_all()?;

            let blob_name = filenames::blob(nf.meta.file_num);
            if self.fs.exists(&self.dir.join(&blob_name)) {
                let blob_bytes = self.fs.read(&self.dir.join(&blob_name))?;
                let mut bw = self.fs.create(&dest.join(&blob_name))?;
                bw.write_all(&blob_bytes)?;
                bw.sync_all()?;
            }
        }

        // Write a fresh MANIFEST holding just the snapshot edit, and point a marker at it.
        let manifest_name = filenames::manifest(1);
        let mut mw = record::Writer::new(self.fs.create(&dest.join(&manifest_name))?);
        mw.write_record(&edit.encode())?;
        mw.sync_all()?;
        update_marker(self.fs.as_ref(), dest, &[], &manifest_name)?;
        self.fs.sync_dir(dest)?;
        Ok(())
    }

    /// Ingests externally-built sstables at `paths` into the database. Every key in every
    /// file is assigned one freshly-allocated sequence number per file (so ingested data
    /// shadows nothing it shouldn't and is shadowed by nothing older), and the rewritten
    /// files are added to L0 and recorded in the MANIFEST.
    ///
    /// The files are rewritten through this database's sstable writer rather than linked,
    /// so their keys carry the assigned sequence number directly — functionally identical
    /// to Pebble's global-sequence-number ingestion.
    pub fn ingest(&self, paths: &[impl AsRef<Path>]) -> Result<()> {
        if paths.is_empty() {
            return Ok(());
        }
        let mut state = self.state.lock().unwrap();
        if state.read_only {
            return Err(crate::Error::InvalidState("db: opened read-only".into()));
        }

        let mut new_files = Vec::new();
        for path in paths {
            let seqnum = state.vs.last_sequence + 1;
            state.vs.last_sequence = seqnum;
            let file_num = state.vs.allocate_file_number();
            let meta = self.rewrite_external(path.as_ref(), file_num, seqnum)?;
            new_files.push(NewFileEntry { level: 0, meta });
        }

        let edit = VersionEdit {
            next_file_number: Some(state.vs.next_file_number),
            last_sequence: Some(state.vs.last_sequence),
            new_files,
            ..Default::default()
        };
        state.vs.apply(&edit)?;
        if let Some(mw) = state.manifest.as_mut() {
            mw.write_record(&edit.encode())?;
            mw.sync_all()?;
        }
        drop(state);
        // The ingested files may have piled onto L0; keep the LSM in shape (off the lock).
        self.maybe_compact()?;
        if let Some(l) = &self.listener {
            l.on_ingest_end(paths.len());
        }
        Ok(())
    }

    /// Atomically replaces the data in `[start, end)` with the contents of the external
    /// sstables at `paths`: the range is excised (range-deleted) first, then the files are
    /// ingested at a newer sequence number so the ingested data wins (Pebble's
    /// `IngestAndExcise`).
    pub fn ingest_and_excise(
        &self,
        paths: &[impl AsRef<Path>],
        start: &[u8],
        end: &[u8],
    ) -> Result<()> {
        self.delete_range(start, end)?;
        self.ingest(paths)
    }

    /// Reads the external sstable at `src`, rewrites its point keys, range tombstones, and
    /// range keys into `<dir>/<file_num>.sst` with every entry stamped at `seqnum`, and
    /// returns the resulting file's metadata.
    fn rewrite_external(&self, src: &Path, file_num: u64, seqnum: u64) -> Result<FileMetadata> {
        let bytes = self.fs.read(src)?;
        let reader = Arc::new(Reader::open(bytes, self.cmp.clone())?);

        let path = self.dir.join(filenames::table(file_num));
        let mut w = Writer::new(
            self.fs.create(&path)?,
            self.cmp.clone(),
            // Honor the engine's value separation when rewriting the ingested table, so large
            // ingested values land in value blocks / a sibling blob file (ingest-with-blobs).
            super::engine_writer_options(
                self.value_block_threshold,
                self.blob_value_threshold,
                file_num,
            ),
        );

        let mut smallest: Option<Vec<u8>> = None;
        let mut largest: Vec<u8> = Vec::new();
        let cmp = self.cmp.clone();
        let note = |ikey: &[u8], smallest: &mut Option<Vec<u8>>, largest: &mut Vec<u8>| {
            let uk = encoded_user_key(ikey);
            if smallest
                .as_deref()
                .is_none_or(|s| cmp.compare(uk, encoded_user_key(s)) == std::cmp::Ordering::Less)
            {
                *smallest = Some(ikey.to_vec());
            }
            if largest.is_empty()
                || cmp.compare(uk, encoded_user_key(largest)) == std::cmp::Ordering::Greater
            {
                *largest = ikey.to_vec();
            }
        };

        // Point keys, restamped at the ingest sequence number.
        let mut it = reader.iter()?;
        it.first()?;
        while it.valid() {
            let kind = trailer_kind(crate::base::internal_key::encoded_trailer(it.key()));
            let ikey = InternalKey::new(encoded_user_key(it.key()).to_vec(), seqnum, kind).encode();
            w.add(&ikey, it.value())?;
            note(&ikey, &mut smallest, &mut largest);
            it.next()?;
        }

        // Range tombstones.
        let mut tombstones = reader.range_tombstones().to_vec();
        tombstones.sort_by(|a, b| self.cmp.compare(&a.start, &b.start));
        for t in &tombstones {
            let start_ikey =
                InternalKey::new(t.start.clone(), seqnum, InternalKeyKind::RangeDelete).encode();
            w.add(&start_ikey, &t.end)?;
            note(&start_ikey, &mut smallest, &mut largest);
            let end_ikey =
                InternalKey::new(t.end.clone(), seqnum, InternalKeyKind::RangeDelete).encode();
            note(&end_ikey, &mut smallest, &mut largest);
        }

        // Range keys.
        let mut range_keys = reader.range_keys().to_vec();
        range_keys.sort_by(|a, b| self.cmp.compare(&a.start, &b.start));
        for rk in &range_keys {
            let start_ikey = InternalKey::new(rk.start.clone(), seqnum, rk.kind).encode();
            w.add(&start_ikey, &rk.value)?;
            note(&start_ikey, &mut smallest, &mut largest);
            if let Ok(end) = rk.end() {
                let end_ikey = InternalKey::new(end, seqnum, rk.kind).encode();
                note(&end_ikey, &mut smallest, &mut largest);
            }
        }

        let blob_bytes = w.take_blob_file()?;
        let blob_refs = w.blob_refs().to_vec();
        let mut file = w.finish()?;
        file.sync_all()?;
        // Write the rewritten table's sibling blob file, if any large values were separated.
        if let Some(b) = &blob_bytes {
            let mut bf = self.fs.create(&self.dir.join(filenames::blob(file_num)))?;
            bf.write_all(b)?;
            bf.sync_all()?;
        }

        Ok(FileMetadata {
            file_num,
            size: self.fs.size(&path)?,
            smallest: smallest.unwrap_or_default(),
            largest,
            smallest_seqnum: seqnum,
            largest_seqnum: seqnum,
            blob_refs,
            backing: None,
        })
    }
}
