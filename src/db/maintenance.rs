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
use crate::objstorage::remoteobjcat::{ObjType, RemoteObjectMetadata};
use crate::record;
use crate::sstable::{Reader, Writer};
use crate::vfs::WritableFile;

use super::{DbInner, filenames, update_marker, write_ext_catalog};

/// An sstable in external/shared storage to be ingested **by reference** (Pebble's `ExternalFile`):
/// its bytes stay in the remote backend under `obj_name` and are read at a synthetic sequence
/// number — never copied locally. Bounds are caller-provided (the file's key range). Synthetic
/// prefix/suffix key transforms are not yet supported.
#[derive(Clone, Debug)]
pub struct ExternalFile {
    /// Names the remote backend. Recorded for fidelity; the single configured backend is used.
    pub locator: String,
    /// The object's name within the remote backend.
    pub obj_name: String,
    /// The object's size in bytes (recorded in the catalog).
    pub size: u64,
    /// Inclusive smallest user key the file covers.
    pub start_key: Vec<u8>,
    /// Largest user key the file covers (inclusive iff `end_key_inclusive`).
    pub end_key: Vec<u8>,
    /// Whether `end_key` is inclusive.
    pub end_key_inclusive: bool,
    /// Whether the file contains point keys.
    pub has_point_key: bool,
    /// Whether the file contains range keys.
    pub has_range_key: bool,
}

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
        // The ingested data is assigned sequence numbers higher than every existing key. Point
        // reads consult memtables before L0, so a *stale* in-memory version of an ingested key
        // would wrongly shadow the newer ingested value. To prevent that, the engine flushes the
        // in-memory data to L0 (below the ingest) — but only when it actually overlaps the ingest.
        // For the common bulk-load case where the ingested keys are disjoint from anything in
        // memory, the flush is skipped entirely (Pebble's flushable ingest avoids the flush via a
        // queued flushable; we avoid it whenever there is nothing to shadow). Compute the ingested
        // files' point-key span up front to make that decision.
        let mut span: Option<(Vec<u8>, Vec<u8>)> = None;
        for path in paths {
            if let Some((lo, hi)) = self.external_point_bounds(path.as_ref())? {
                span = Some(match span {
                    None => (lo, hi),
                    Some((glo, ghi)) => (
                        if self.cmp.compare(&lo, &glo) == std::cmp::Ordering::Less {
                            lo
                        } else {
                            glo
                        },
                        if self.cmp.compare(&hi, &ghi) == std::cmp::Ordering::Greater {
                            hi
                        } else {
                            ghi
                        },
                    ),
                });
            }
        }

        let (plan, needs_flush): (Vec<(std::path::PathBuf, u64, u64)>, bool) = {
            let mut state = self.state.lock().unwrap();
            if state.read_only {
                return Err(crate::Error::InvalidState("db: opened read-only".into()));
            }
            let mut plan = Vec::new();
            for path in paths {
                let seqnum = state.vs.last_sequence + 1;
                state.vs.last_sequence = seqnum;
                let file_num = state.vs.allocate_file_number();
                plan.push((path.as_ref().to_path_buf(), file_num, seqnum));
            }
            // A flush is needed only if some in-memory point key falls within the ingested span
            // (mutable memtable or any immutable still awaiting flush).
            let needs_flush = if let Some((lo, hi)) = &span {
                let overlaps = |m: &Arc<crate::memtable::MemTable>| -> bool {
                    let mut it = m.iter();
                    it.seek_ge(lo, u64::MAX);
                    it.valid() && self.cmp.compare(it.user_key(), hi) != std::cmp::Ordering::Greater
                };
                overlaps(&state.mem) || state.imm.iter().any(overlaps)
            } else {
                false
            };
            if needs_flush {
                self.rotate_memtable(&mut state)?;
                self.work_cv.notify_all();
            }
            (plan, needs_flush)
        };

        // When the ingest overlaps in-memory data, drain the immutable queue to L0 off the lock so
        // the ingest is ordered above it. Disjoint ingests skip this and never block on a flush.
        if needs_flush {
            while self.flush_one()? {}
        }

        // Rewrite the external files at their reserved sequence numbers (off the lock).
        let mut new_files = Vec::new();
        for (path, file_num, seqnum) in &plan {
            let meta = self.rewrite_external(path, *file_num, *seqnum)?;
            self.upload_if_shared(*file_num)?;
            new_files.push(NewFileEntry { level: 0, meta });
        }
        // Make the ingested files' directory entries durable before the MANIFEST references them.
        self.fs.sync_dir(&self.dir)?;

        // Add the ingested files to L0.
        {
            let mut state = self.state.lock().unwrap();
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
        }
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

    /// Assigns this store's creator id (Pebble's `SetCreatorID`). Required before
    /// [`ingest_external_files`](Self::ingest_external_files). Must be non-zero and may be set only
    /// once (idempotent for the same id); persisted in the external-object catalog.
    pub fn set_creator_id(&self, id: u64) -> Result<()> {
        if id == 0 {
            return Err(crate::Error::InvalidState(
                "creator id must be non-zero".into(),
            ));
        }
        if self.state.lock().unwrap().read_only {
            return Err(crate::Error::InvalidState("db: opened read-only".into()));
        }
        let mut ext = self.ext_catalog.lock().unwrap();
        match ext.creator_id {
            Some(existing) if existing != id => {
                return Err(crate::Error::InvalidState("creator id already set".into()));
            }
            Some(_) => return Ok(()), // idempotent
            None => {}
        }
        ext.creator_id = Some(id);
        write_ext_catalog(self.fs.as_ref(), &self.dir, &mut ext)
    }

    /// This store's creator id, if set.
    pub fn creator_id(&self) -> Option<u64> {
        self.ext_catalog.lock().unwrap().creator_id
    }

    /// The external (reference-in-place) objects currently registered, as
    /// `(file_num, remote_object_name)` — a thin view of the object provider's external catalog.
    pub fn external_objects(&self) -> Vec<(u64, String)> {
        self.ext_catalog
            .lock()
            .unwrap()
            .objects
            .iter()
            .map(|(n, m)| (*n, m.custom_object_name.clone()))
            .collect()
    }

    /// Ingests sstables that live in external/shared storage **by reference** (Pebble's
    /// `IngestExternalFiles`): the files are not copied or rewritten — each is registered as a table
    /// whose bytes stay in the remote backend under its `obj_name`, read at a synthetic sequence
    /// number so the ingest wins over older data and is invisible to snapshots taken before it.
    /// Requires a remote backend and a creator id ([`set_creator_id`](Self::set_creator_id)).
    /// External objects are never deleted from remote storage (no-cleanup). The caller must supply
    /// accurate key bounds; the files are placed at L0.
    pub fn ingest_external_files(&self, external: &[ExternalFile]) -> Result<()> {
        if external.is_empty() {
            return Ok(());
        }
        if self.remote.is_none() {
            return Err(crate::Error::InvalidState(
                "ingest_external_files requires a remote backend".into(),
            ));
        }
        let creator_id = self.ext_catalog.lock().unwrap().creator_id.ok_or_else(|| {
            crate::Error::InvalidState(
                "set_creator_id required before ingest_external_files".into(),
            )
        })?;

        // Union of the ingested key spans, to decide whether a memtable flush is needed (a stale
        // in-memory key in the span would otherwise shadow the newer ingested value).
        let mut span: Option<(Vec<u8>, Vec<u8>)> = None;
        for f in external {
            let (lo, hi) = (f.start_key.clone(), f.end_key.clone());
            span = Some(match span {
                None => (lo, hi),
                Some((glo, ghi)) => (
                    if self.cmp.compare(&lo, &glo) == std::cmp::Ordering::Less {
                        lo
                    } else {
                        glo
                    },
                    if self.cmp.compare(&hi, &ghi) == std::cmp::Ordering::Greater {
                        hi
                    } else {
                        ghi
                    },
                ),
            });
        }

        // Reserve one fresh sequence number + file number per external file, and decide on a flush.
        let (plan, needs_flush): (Vec<(u64, u64)>, bool) = {
            let mut state = self.state.lock().unwrap();
            if state.read_only {
                return Err(crate::Error::InvalidState("db: opened read-only".into()));
            }
            let mut plan = Vec::new();
            for _ in external {
                let seqnum = state.vs.last_sequence + 1;
                state.vs.last_sequence = seqnum;
                let file_num = state.vs.allocate_file_number();
                plan.push((file_num, seqnum));
            }
            let needs_flush = if let Some((lo, hi)) = &span {
                let overlaps = |m: &Arc<crate::memtable::MemTable>| -> bool {
                    let mut it = m.iter();
                    it.seek_ge(lo, u64::MAX);
                    it.valid() && self.cmp.compare(it.user_key(), hi) != std::cmp::Ordering::Greater
                };
                overlaps(&state.mem) || state.imm.iter().any(overlaps)
            } else {
                false
            };
            if needs_flush {
                self.rotate_memtable(&mut state)?;
                self.work_cv.notify_all();
            }
            (plan, needs_flush)
        };
        if needs_flush {
            while self.flush_one()? {}
        }

        // Build each file's metadata (non-virtual; bounds from the caller-supplied keys at the
        // ingest seqnum) and its external-catalog entry. Persist the catalog FIRST so a crash
        // before the MANIFEST edit leaves only a dangling entry (pruned on the next open), never a
        // live file whose bytes cannot be located.
        let mut new_files = Vec::new();
        {
            let mut ext = self.ext_catalog.lock().unwrap();
            for (f, &(file_num, seqnum)) in external.iter().zip(plan.iter()) {
                let smallest =
                    InternalKey::new(f.start_key.clone(), seqnum, InternalKeyKind::Set).encode();
                let largest =
                    InternalKey::new(f.end_key.clone(), seqnum, InternalKeyKind::Set).encode();
                new_files.push(NewFileEntry {
                    level: 0,
                    meta: FileMetadata {
                        file_num,
                        size: f.size,
                        smallest,
                        largest,
                        smallest_seqnum: seqnum,
                        largest_seqnum: seqnum,
                        blob_refs: Vec::new(),
                        pebble_blob_refs: Vec::new(),
                        backing: None,
                        has_spans: None,
                    },
                });
                ext.objects.insert(
                    file_num,
                    RemoteObjectMetadata {
                        file_num,
                        obj_type: ObjType::Table,
                        creator_id,
                        creator_file_num: file_num,
                        cleanup_method: 1, // no-cleanup: never delete external bytes
                        locator: f.locator.clone(),
                        custom_object_name: f.obj_name.clone(),
                    },
                );
                ext.syn_seq.insert(file_num, seqnum);
            }
            write_ext_catalog(self.fs.as_ref(), &self.dir, &mut ext)?;
        }

        // Record the files in the MANIFEST (at L0), then keep the LSM in shape.
        {
            let mut state = self.state.lock().unwrap();
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
        }
        self.maybe_compact()?;
        if let Some(l) = &self.listener {
            l.on_ingest_end(external.len());
        }
        Ok(())
    }

    /// Rewrites shared/remote sstables (and their blob files) overlapping the user-key range
    /// `[start, end)` back to local storage (Pebble's `Download`), so that range no longer
    /// depends on the shared backend. Returns the number of objects moved local; a no-op when
    /// no remote backend is configured.
    pub fn download(&self, start: &[u8], end: &[u8]) -> Result<usize> {
        use std::cmp::Ordering;
        use std::io::Write;
        let Some(remote) = self.remote.clone() else {
            return Ok(0);
        };
        // The physical files overlapping the range (a virtual sstable contributes its backing).
        let phys: Vec<u64> = {
            let state = self.state.lock().unwrap();
            let cmp = self.cmp.as_ref();
            let mut set = std::collections::BTreeSet::new();
            for level in state.vs.current.levels.iter() {
                for f in level {
                    let fl = encoded_user_key(&f.largest);
                    let fsm = encoded_user_key(&f.smallest);
                    if cmp.compare(fl, start) != Ordering::Less
                        && cmp.compare(fsm, end) == Ordering::Less
                    {
                        set.insert(f.physical_num());
                    }
                }
            }
            set.into_iter().collect()
        };
        let mut moved = 0;
        for p in phys {
            for name in [filenames::table(p), filenames::blob(p)] {
                if remote.exists(&name) {
                    let bytes = remote.get(&name)?;
                    // Write local before deleting remote, so the object is always reachable.
                    let mut w = self.fs.create(&self.dir.join(&name))?;
                    w.write_all(&bytes)?;
                    w.sync_all()?;
                    remote.delete(&name)?;
                    moved += 1;
                }
            }
        }
        Ok(moved)
    }

    /// Reads the external sstable at `src`, rewrites its point keys, range tombstones, and
    /// range keys into `<dir>/<file_num>.sst` with every entry stamped at `seqnum`, and
    /// returns the resulting file's metadata.
    /// Reads the smallest and largest **point**-key user keys of an external sstable, or `None`
    /// when it has no point keys. Used to decide whether an ingest overlaps in-memory data and so
    /// must flush the memtable first.
    fn external_point_bounds(&self, src: &Path) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let bytes = self.fs.read(src)?;
        let reader = Arc::new(Reader::open(bytes, self.cmp.clone())?);
        let mut it = reader.iter()?;
        it.first()?;
        if !it.valid() {
            return Ok(None);
        }
        let lo = encoded_user_key(it.key()).to_vec();
        it.last()?;
        let hi = encoded_user_key(it.key()).to_vec();
        Ok(Some((lo, hi)))
    }

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
            pebble_blob_refs: Vec::new(),
            backing: None,
            // Rewritten external table: leave the span hint unknown so it is opened (correct).
            has_spans: None,
        })
    }
}
