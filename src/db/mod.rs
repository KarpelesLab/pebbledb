// Copyright (c) 2012 The LevelDB-Go Authors. All rights reserved.
// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Ported from Pebble's pebble.go, open.go, commit.go, and flushable.go.

//! The database: opening, reading, and writing an on-disk LSM store.
//!
//! [`Db::open`] opens (or creates) a store: it locates the current MANIFEST via the
//! atomic marker files Pebble writes, replays it into a [`VersionSet`], recovers any
//! un-flushed write-ahead logs into a fresh memtable, then rotates in a new MANIFEST and
//! WAL for the session and spawns a background flush/compaction worker. Writes
//! (`set`, `delete`, `apply`) assign sequence numbers, append to the WAL, and update the
//! mutable memtable; when it fills it is rotated into an immutable queue (a cheap,
//! non-blocking operation) and the worker flushes it to an L0 sstable off the writer's
//! path. Reads consult the mutable memtable, then the immutable memtables newest-first,
//! then the sstables of each level.
//!
//! Scope: a single-MANIFEST, single active-WAL engine with one background worker. The
//! memtable is rotated when it fills or on an explicit [`DbInner::flush`]; flushes run on the
//! worker (an explicit `flush` cooperates and waits for completion), and leveled
//! compaction runs after each flush (a score-based picker plus manual
//! [`DbInner::compact_range`]). The WAL is fsynced after each write when `Options::wal_sync` is
//! set (the default), and the MANIFEST is fsynced on every edit.

mod compaction;
mod filenames;
mod indexed_batch;
mod maintenance;
pub use maintenance::CheckpointOptions;
mod merging_iter;
mod options_file;

pub use indexed_batch::IndexedBatch;
use merging_iter::InternalIter;
pub use merging_iter::{DbIterator, IterKeyType, IterOptions};
pub use options_file::{FormatMajorVersion, OptionsFile};

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};

use crate::base::comparer::{Comparer, DefaultComparer};
use crate::base::internal_key::{InternalKey, InternalKeyKind, SeqNum, encoded_user_key};
use crate::base::range_del::{RangeTombstone, max_covering_seqnum};
use crate::base::range_key::RangeKeyEntry;
use crate::batch::Batch;
use crate::manifest::{FileMetadata, NUM_LEVELS, NewFileEntry, VersionEdit, VersionSet};
use crate::memtable::MemTable;
use crate::record;
use crate::sstable::{Reader, Writer, WriterOptions};
use crate::vfs::{DirLock, DiskFs, Fs, WritableFile};
use crate::{Error, Result};

/// A factory producing a fresh [`BlockPropertyCollector`](crate::sstable::blockprop::BlockPropertyCollector)
/// for each sstable written. Used in [`Options::block_property_collectors`].
pub type BlockPropertyCollectorFactory =
    Arc<dyn Fn() -> Box<dyn crate::sstable::blockprop::BlockPropertyCollector> + Send + Sync>;

/// Options for opening a database.
#[derive(Clone)]
pub struct Options {
    /// The user-key comparer. Must match the store's recorded comparer name.
    pub comparer: Arc<dyn Comparer>,
    /// The memtable arena size in bytes; the memtable is flushed once it fills.
    pub mem_table_size: usize,
    /// Create the store if it does not already exist.
    pub create_if_missing: bool,
    /// Open read-only: no WAL, no flushing, no MANIFEST rotation.
    pub read_only: bool,
    /// fsync the WAL after every write so committed data survives a crash (default
    /// `true`). Disabling trades durability for throughput.
    pub wal_sync: bool,
    /// Optional listener notified of flush and compaction events.
    pub event_listener: Option<Arc<dyn EventListener>>,
    /// Optional merge operator. Required to read keys written with `merge`; without it,
    /// a merge resolves to its newest operand.
    pub merger: Option<Arc<dyn crate::base::merge::Merger>>,
    /// Additional comparers, keyed by [`name`](Comparer::name), consulted when opening a
    /// store whose recorded comparer name differs from [`comparer`](Options::comparer). This
    /// is Pebble's comparer registry: it lets one process open stores written with different
    /// comparers without knowing which in advance. The recorded comparer must be either
    /// [`comparer`](Options::comparer) or present here, or the open fails.
    pub comparers: Vec<Arc<dyn Comparer>>,
    /// Additional merge operators, keyed by [`name`](crate::base::merge::Merger::name),
    /// consulted when opening a store whose recorded merger name differs from
    /// [`merger`](Options::merger). Pebble's merger registry.
    pub mergers: Vec<Arc<dyn crate::base::merge::Merger>>,
    /// Factories for the block-property collectors run over every sstable this store writes
    /// (at flush and compaction). Each factory produces a fresh collector per output file;
    /// the resulting properties are stored in the table and can be matched at read time with
    /// [`IterOptions::block_property_filters`]. Pebble's `BlockPropertyCollectors`.
    pub block_property_collectors: Vec<BlockPropertyCollectorFactory>,
    /// Size in bytes of the shared block cache for decompressed blocks (default 8 MiB).
    /// Zero disables block caching.
    pub block_cache_size: usize,
    /// Maximum number of sstable readers kept open (default 1000). The least-recently
    /// used reader is evicted when the limit is exceeded.
    pub max_open_files: usize,
    /// The filesystem the database performs all of its I/O through (default [`DiskFs`]).
    /// Use [`crate::vfs::MemFs`] for fully in-memory operation.
    pub fs: Arc<dyn Fs>,
    /// The on-disk format major version a newly created store is initialized to (default
    /// [`FormatMajorVersion::DEFAULT`]). Existing stores keep their recorded version.
    pub format_major_version: FormatMajorVersion,
    /// Directory the write-ahead log is written to. `None` (default) keeps WALs in the
    /// database directory.
    pub wal_dir: Option<PathBuf>,
    /// A secondary WAL directory to fail over to when a write to the primary WAL fails
    /// (e.g. a stalled or failing disk). On failure the current batch is re-logged here and
    /// subsequent WALs are created here; recovery scans every configured WAL directory.
    pub wal_failover_dir: Option<PathBuf>,
    /// Number of obsolete WAL files to keep for **recycling** rather than deleting (default
    /// `0`, disabled). When non-zero and a single WAL directory is configured, a flushed WAL's
    /// file is retained (up to this many) and reused in place for the next WAL — its
    /// already-allocated blocks are overwritten instead of creating and allocating a fresh
    /// file, cutting per-rotation filesystem overhead (Pebble's WAL recycling). Recovery reads
    /// recycled logs tolerantly, stopping at the stale tail left by the previous use. Recycling
    /// is skipped when a failover directory is configured.
    pub wal_recycle_limit: usize,
    /// Optional sink for informational/error log messages.
    pub logger: Option<Arc<dyn Logger>>,
    /// How obsolete files are disposed of (default: delete). Use [`ArchiveCleaner`] to
    /// retain them.
    pub cleaner: Arc<dyn Cleaner>,
    /// Maximum number of immutable memtables awaiting flush before writes stall (block)
    /// until the background worker catches up (default 4). Bounds memory when writes
    /// outrun flushing. Minimum 1.
    pub mem_table_stop_writes_threshold: usize,
    /// Number of L0 sstables that triggers an L0→L1 compaction (default 4).
    pub l0_compaction_threshold: usize,
    /// Target size in bytes of an output sstable before it is split during compaction
    /// (default 2 MiB).
    pub target_file_size: u64,
    /// Fraction of a file's entries that must be point tombstones before it is eligible for a
    /// **tombstone-density** compaction — compacted down to reclaim space even when its level
    /// is within budget (default 0.10, Pebble's default). Set to a value `> 1.0` to disable.
    pub tombstone_dense_compaction_threshold: f64,
    /// Number of "wasted" read seeks through a file — reads that passed through it to find the
    /// key in a deeper level — that trigger a **read-triggered compaction** of that file
    /// (Pebble's allowed-seeks heuristic, which it derives from file size; here a fixed knob).
    /// `0` disables read-triggered compactions (default 1024).
    pub read_compaction_threshold: u32,
    /// Size budget in bytes of the base level (L1) before it is compacted downward; deeper
    /// levels grow 10x per level (Pebble's `LBaseMaxBytes`, default 10 MiB).
    pub l1_max_bytes: u64,
    /// Rate in bytes/second at which obsolete files are deleted (Pebble's
    /// `TargetByteDeletionRate`). When non-zero, deletions are handed to a background pacer
    /// thread that spaces them out to avoid disk-I/O bursts; `0` (default) deletes inline and
    /// immediately.
    pub target_byte_deletion_rate: u64,
    /// When set, the filesystem is wrapped in a health checker that reports any operation
    /// taking at least this long to [`EventListener::on_disk_slow`]. `None` (default) leaves
    /// the filesystem unwrapped.
    pub disk_slow_threshold: Option<std::time::Duration>,
    /// Enables **value separation** in sstables written by flush and compaction: point values
    /// at least this many bytes are stored out-of-line in the table's value blocks rather than
    /// inline in data blocks (Pebble's value blocks). `None` (default) keeps all values inline.
    /// Enabling it writes tables in a value-block-capable format (Pebble v3).
    pub value_block_threshold: Option<usize>,
    /// Enables **blob files** for flush: point values at least this many bytes are stored in a
    /// separate `.blob` file alongside the sstable rather than inline or in value blocks
    /// (Pebble's blob files; takes precedence over [`value_block_threshold`](Self::value_block_threshold)).
    /// `None` (default) disables blob separation. Compaction resolves blob-referenced values
    /// back in place, so a compacted table holds no blob references and the input blob files
    /// become obsolete with their sstables. Enabling it writes tables in Pebble v3 format.
    pub blob_value_threshold: Option<usize>,
    /// Per-level output sstable size targets (Pebble's per-level `TargetFileSize`). Index `i`
    /// overrides [`target_file_size`](Options::target_file_size) for compactions whose output
    /// is level `i`; levels beyond the vector fall back to `target_file_size`. Empty (default)
    /// uses `target_file_size` everywhere.
    pub level_target_file_sizes: Vec<u64>,
    /// Maximum number of background compactions that may run concurrently (Pebble's
    /// `MaxConcurrentCompactions`, default 1). Compactions pick disjoint input files, so
    /// raising this lets independent compactions proceed in parallel on multiple cores.
    pub max_concurrent_compactions: usize,
}

/// A listener notified of background-style events (flushes and compactions). All methods
/// have default no-op implementations, so implementors override only what they need.
pub trait EventListener: Send + Sync {
    /// Called when a memtable flush begins.
    fn on_flush_begin(&self) {}
    /// Called after a memtable is flushed to an L0 sstable.
    fn on_flush_end(&self, _file_num: u64, _bytes: u64) {}
    /// Called when a compaction begins (with its output level and input-file count).
    fn on_compaction_begin(&self, _output_level: usize, _input_files: usize) {}
    /// Called when writes begin to stall (the immutable-memtable limit was reached).
    fn on_write_stall_begin(&self, _reason: &str) {}
    /// Called when a write stall ends.
    fn on_write_stall_end(&self) {}
    /// Called after a compaction completes.
    fn on_compaction_end(&self, _output_level: usize, _input_files: usize, _output_files: usize) {}
    /// Called when an sstable is created (by flush, compaction, or ingestion).
    fn on_table_created(&self, _file_num: u64) {}
    /// Called when an obsolete sstable is removed.
    fn on_table_deleted(&self, _file_num: u64) {}
    /// Called when external sstables are ingested.
    fn on_ingest_end(&self, _files: usize) {}
    /// Called when a new write-ahead log file is created (at open and on memtable rotation).
    fn on_wal_created(&self, _file_num: u64) {}
    /// Called when an obsolete write-ahead log file is removed.
    fn on_wal_deleted(&self, _file_num: u64) {}
    /// Called when a new MANIFEST is created (at open and on MANIFEST rotation).
    fn on_manifest_created(&self, _file_num: u64) {}
    /// Called when an obsolete MANIFEST is removed.
    fn on_manifest_deleted(&self, _file_num: u64) {}
    /// Called after the format major version is upgraded a step, with the new version.
    fn on_format_upgrade(&self, _version: u32) {}
    /// Called when a background flush or compaction fails, with a description of the error.
    fn on_background_error(&self, _error: &str) {}
    /// Called after table statistics are (re)computed, with the number of tables examined.
    fn on_table_stats_loaded(&self, _tables: usize) {}
    /// Called after an sstable is validated, with its file number and whether it passed.
    fn on_table_validated(&self, _file_num: u64, _ok: bool) {}
    /// Called when a filesystem operation exceeds the configured disk-slow threshold (routed
    /// from the health-checking vfs when [`Options::disk_slow_threshold`] is set).
    fn on_disk_slow(&self, _op: &str, _path: &Path, _duration: std::time::Duration) {}
}

/// A sink for the database's informational and error log messages, mirroring Pebble's
/// `Logger`. The default no-op `error` forwards to `info`.
pub trait Logger: Send + Sync {
    /// Logs an informational message.
    fn info(&self, msg: &str);
    /// Logs an error message (defaults to [`info`](Logger::info)).
    fn error(&self, msg: &str) {
        self.info(msg);
    }
}

/// Decides how an obsolete file is disposed of, mirroring Pebble's `Cleaner`. The default
/// [`DeleteCleaner`] removes it; [`ArchiveCleaner`] moves it into an archive directory.
pub trait Cleaner: Send + Sync {
    /// Disposes of the obsolete file at `path` on `fs`.
    fn clean(&self, fs: &dyn Fs, path: &Path) -> Result<()>;
}

/// A [`Cleaner`] that deletes obsolete files (the default).
#[derive(Debug, Default, Clone, Copy)]
pub struct DeleteCleaner;

impl Cleaner for DeleteCleaner {
    fn clean(&self, fs: &dyn Fs, path: &Path) -> Result<()> {
        fs.remove(path)?;
        Ok(())
    }
}

/// A [`Cleaner`] that moves obsolete files into an archive directory instead of deleting
/// them (Pebble's archive cleaner), useful for forensic recovery.
#[derive(Debug, Clone)]
pub struct ArchiveCleaner {
    /// The directory obsolete files are moved into.
    pub dir: PathBuf,
}

impl Cleaner for ArchiveCleaner {
    fn clean(&self, fs: &dyn Fs, path: &Path) -> Result<()> {
        fs.create_dir_all(&self.dir)?;
        let name = path
            .file_name()
            .ok_or_else(|| Error::InvalidState("cleaner: path has no file name".into()))?;
        fs.rename(path, &self.dir.join(name))?;
        Ok(())
    }
}

impl Default for Options {
    fn default() -> Self {
        Options {
            comparer: Arc::new(DefaultComparer),
            mem_table_size: 4 << 20,
            create_if_missing: true,
            read_only: false,
            wal_sync: true,
            wal_recycle_limit: 0,
            event_listener: None,
            merger: None,
            comparers: Vec::new(),
            mergers: Vec::new(),
            block_property_collectors: Vec::new(),
            block_cache_size: 8 << 20,
            max_open_files: 1000,
            fs: Arc::new(DiskFs),
            format_major_version: FormatMajorVersion::DEFAULT,
            wal_dir: None,
            wal_failover_dir: None,
            logger: None,
            cleaner: Arc::new(DeleteCleaner),
            mem_table_stop_writes_threshold: 4,
            l0_compaction_threshold: 4,
            target_file_size: 2 << 20,
            tombstone_dense_compaction_threshold: 0.10,
            read_compaction_threshold: 1024,
            l1_max_bytes: 10 << 20,
            target_byte_deletion_rate: 0,
            disk_slow_threshold: None,
            value_block_threshold: None,
            blob_value_threshold: None,
            level_target_file_sizes: Vec::new(),
            max_concurrent_compactions: 1,
        }
    }
}

/// Completion slot for one queued write in the group-commit pipeline. The commit leader
/// stores the result and signals; the submitting thread waits on it.
struct CommitSlot {
    result: Mutex<Option<Result<()>>>,
    cv: Condvar,
}

/// Group-commit queue plus its leadership flag, guarded by one mutex so that enqueueing a
/// write and claiming/relinquishing leadership are atomic. This closes the lost-leader race:
/// a writer either is picked up by the active leader's drain loop, or — because it observed
/// `leader == false` under the same lock the leader clears it under — becomes the leader
/// itself. No queued write can be stranded with no thread responsible for committing it.
#[derive(Default)]
struct CommitQueue {
    /// Writes awaiting commit, each with its batch and completion slot.
    items: Vec<(Batch, Arc<CommitSlot>)>,
    /// Whether a leader is currently draining the queue.
    leader: bool,
}

/// Queue of obsolete files awaiting paced deletion, plus a shutdown flag for the pacer.
#[derive(Default)]
struct DeleteQueue {
    items: Vec<(PathBuf, u64)>,
    shutdown: bool,
}

/// Mutable database state guarded by a single mutex.
struct State {
    vs: VersionSet,
    mem: Arc<MemTable>,
    /// Memtables rotated out of `mem` and awaiting flush, oldest first. Reads consult
    /// them newest-first (after `mem`). `imm_wals[i]` is the WAL number holding `imm[i]`'s
    /// data, removed once that memtable is flushed.
    imm: Vec<Arc<MemTable>>,
    imm_wals: Vec<u64>,
    /// File numbers of flushed WALs retained for recycling (see [`Options::wal_recycle_limit`]).
    /// Their files remain on disk and are reused in place for future WALs.
    wal_recycle: Vec<u64>,
    wal: Option<record::Writer<Box<dyn WritableFile>>>,
    wal_number: u64,
    /// Index into [`DbInner::wal_dirs`] of the directory the active WAL lives in. Advances
    /// on failover.
    wal_dir_idx: usize,
    manifest: Option<record::Writer<Box<dyn WritableFile>>>,
    read_only: bool,
    /// Set on close to tell the background worker to exit.
    shutdown: bool,
    /// Number of memtable flushes performed this session.
    flush_count: u64,
    /// Number of compactions performed this session.
    compaction_count: u64,
    /// Total bytes written by flushes this session (the denominator of write amplification).
    flush_bytes: u64,
    /// Total bytes written by compactions this session.
    compaction_bytes: u64,
    /// Per-file count of "wasted" read seeks: reads that passed through the file (it
    /// overlapped the key by range but held no version of it) before finding the key in a
    /// deeper level. Drives read-triggered compaction.
    read_miss: std::collections::HashMap<u64, u32>,
    /// Files whose wasted-seek count has crossed the read-compaction threshold and which are
    /// awaiting a read-triggered compaction. Drained by the background worker / `maybe_compact`.
    read_queue: Vec<u64>,
    /// File numbers currently being compacted by some worker. The compaction picker skips
    /// these so concurrent compactions never share inputs (and so a compaction's inputs stay
    /// present in the version until its edit applies).
    compacting: std::collections::HashSet<u64>,
    /// Files removed from the live version but not yet deleted from disk. Each is kept here
    /// with its `FileMetadata` so that, while an in-flight read still holds a snapshot that
    /// references the file (its `Arc` strong count > 1), the on-disk `.sst` is preserved. A
    /// file is deleted once only this list references it (count == 1).
    obsolete: Vec<(u64, Arc<crate::manifest::FileMetadata>)>,
}

/// Opens and caches blob files (an sstable's separately-stored large values) and resolves
/// blob handles for the engine's sstable readers. A blob file shares its sstable's file
/// number (named `<num>.blob`), so a handle plus the referencing table's number locate a value.
struct BlobStore {
    fs: Arc<dyn Fs>,
    dir: PathBuf,
    cache: Mutex<HashMap<u64, Arc<crate::sstable::blob::BlobFileReader>>>,
}

impl BlobStore {
    fn reader(&self, file_num: u64) -> Result<Arc<crate::sstable::blob::BlobFileReader>> {
        if let Some(r) = self.cache.lock().unwrap().get(&file_num) {
            return Ok(Arc::clone(r));
        }
        let bytes = self.fs.read(&self.dir.join(filenames::blob(file_num)))?;
        let r = Arc::new(crate::sstable::blob::BlobFileReader::open(bytes)?);
        self.cache.lock().unwrap().insert(file_num, Arc::clone(&r));
        Ok(r)
    }

    /// Drops a blob file's cached reader (called when its sstable is deleted).
    fn evict(&self, file_num: u64) {
        self.cache.lock().unwrap().remove(&file_num);
    }
}

impl crate::sstable::BlobResolver for BlobStore {
    fn resolve(&self, file_num: u64, handle: crate::sstable::blob::BlobHandle) -> Result<Vec<u8>> {
        self.reader(file_num)?.get(handle)
    }
}

/// The shared inner state of a [`Db`], held behind an `Arc` so the background flush worker
/// can operate on it concurrently with foreground reads and writes.
pub struct DbInner {
    dir: PathBuf,
    cmp: Arc<dyn Comparer>,
    mem_table_size: usize,
    wal_sync: bool,
    /// Number of obsolete WAL files to retain for recycling (see [`Options::wal_recycle_limit`]).
    wal_recycle_limit: usize,
    state: Mutex<State>,
    /// Serializes flush execution between the background worker and an explicit
    /// [`flush`](DbInner::flush), so a memtable is never flushed twice.
    flush_lock: Mutex<()>,
    /// Signaled when a memtable is rotated into `imm`, waking the background worker.
    work_cv: Condvar,
    /// Signaled when a flush completes, waking any waiter draining the immutable queue.
    drained_cv: Condvar,
    cache: Mutex<HashMap<u64, Arc<Reader>>>,
    /// Sequence numbers of currently-open snapshots. Compaction retains the versions
    /// they need.
    snapshots: Mutex<Vec<SeqNum>>,
    /// Optional event listener.
    listener: Option<Arc<dyn EventListener>>,
    /// Optional merge operator.
    merger: Option<Arc<dyn crate::base::merge::Merger>>,
    /// Shared block cache (None when disabled).
    block_cache: Option<Arc<crate::cache::BlockCache>>,
    /// Maximum number of cached open readers.
    max_open_files: usize,
    /// The filesystem all I/O goes through.
    fs: Arc<dyn Fs>,
    /// WAL directories, primary first then failover targets. The active WAL is created in
    /// the first that accepts it; recovery scans them all.
    wal_dirs: Vec<PathBuf>,
    /// The held exclusive directory lock (released on drop). `None` when read-only.
    _lock: Option<Box<dyn DirLock>>,
    /// The store's on-disk format major version.
    format_major_version: Mutex<FormatMajorVersion>,
    /// Optional log sink.
    logger: Option<Arc<dyn Logger>>,
    /// How obsolete files are disposed of.
    cleaner: Arc<dyn Cleaner>,
    /// Factories for block-property collectors run over every sstable written.
    block_property_collectors: Vec<BlockPropertyCollectorFactory>,
    /// Point-tombstone fraction that makes a file eligible for tombstone-density compaction.
    tombstone_dense_compaction_threshold: f64,
    /// Wasted-seek count that triggers a read-triggered compaction (0 disables).
    read_compaction_threshold: u32,
    /// Base-level (L1) size budget in bytes; deeper levels grow 10x per level.
    l1_max_bytes: u64,
    /// Maximum number of background compactions allowed to run concurrently.
    max_concurrent_compactions: usize,
    /// Minimum value size for out-of-line value-block storage; `None` keeps values inline.
    value_block_threshold: Option<usize>,
    /// Minimum value size for separate-blob-file storage at flush; `None` disables it.
    blob_value_threshold: Option<usize>,
    /// Opens and caches blob files, resolving blob-referenced values for sstable readers.
    blob_store: Arc<BlobStore>,
    /// Per-level output-sstable size targets; falls back to `target_file_size` past its end.
    level_target_file_sizes: Vec<u64>,
    /// Bytes/second deletion-pacing rate; `0` deletes inline (no pacer thread).
    target_byte_deletion_rate: u64,
    /// Queue of obsolete files awaiting paced deletion (only used when the rate is non-zero).
    delete_queue: Mutex<DeleteQueue>,
    /// Signals the deletion pacer that work was queued (or shutdown requested).
    delete_cv: Condvar,
    /// Group-commit queue + leadership flag (see [`CommitQueue`]).
    commit_q: Mutex<CommitQueue>,
    /// Immutable-memtable count at which writes stall.
    mem_stop_threshold: usize,
    /// L0 file count that triggers an L0→L1 compaction.
    l0_compaction_threshold: usize,
    /// Target output-sstable size before splitting during compaction.
    target_file_size: u64,
}

impl DbInner {
    /// Logs an informational message if a [`Logger`] is configured.
    fn log(&self, msg: &str) {
        if let Some(l) = &self.logger {
            l.info(msg);
        }
    }

    /// Disposes of the obsolete file at `path` via the configured [`Cleaner`]. With deletion
    /// pacing enabled (`target_byte_deletion_rate > 0`) the file is handed to the background
    /// pacer thread instead of being cleaned inline.
    fn clean_file(&self, path: &Path) {
        if self.target_byte_deletion_rate == 0 {
            let _ = self.cleaner.clean(self.fs.as_ref(), path);
            return;
        }
        let size = self.fs.size(path).unwrap_or(0);
        self.delete_queue
            .lock()
            .unwrap()
            .items
            .push((path.to_path_buf(), size));
        self.delete_cv.notify_one();
    }

    /// Records files removed from the live version as obsolete (pending deletion), holding a
    /// reference to each so it is not deleted while an in-flight read still references it.
    fn enqueue_obsolete<'a>(
        &self,
        files: impl Iterator<Item = &'a Arc<crate::manifest::FileMetadata>>,
    ) {
        let mut state = self.state.lock().unwrap();
        for f in files {
            state.obsolete.push((f.file_num, Arc::clone(f)));
        }
    }

    /// Deletes obsolete files no live version snapshot references any more (their `FileMetadata`
    /// `Arc` is held only by the obsolete list, i.e. strong count == 1). Files still referenced
    /// by an in-flight read are kept and retried on the next call.
    fn collect_obsolete(&self) {
        let (ready, candidate_blobs, live_blobs) = {
            let mut state = self.state.lock().unwrap();
            let mut ready = Vec::new();
            // Blob files referenced by the sstables being removed are candidates for deletion.
            let mut candidate_blobs: Vec<u64> = Vec::new();
            state.obsolete.retain(|(num, arc)| {
                if Arc::strong_count(arc) == 1 {
                    ready.push(*num);
                    candidate_blobs.extend(arc.blob_refs.iter().copied());
                    false
                } else {
                    true
                }
            });
            // A blob file is live while any current-version file — or any obsolete file still
            // held by an in-flight read — references it. Computed under the lock so it reflects
            // the version a concurrent compaction's just-applied output is already part of; a
            // candidate is only ever a blob the removed sstables referenced, so a concurrent
            // compaction's brand-new blob (referenced by no removed sstable) is never deleted.
            let mut live: std::collections::HashSet<u64> = std::collections::HashSet::new();
            for level in state.vs.current.levels.iter() {
                for f in level {
                    live.extend(f.blob_refs.iter().copied());
                }
            }
            for (_, arc) in &state.obsolete {
                live.extend(arc.blob_refs.iter().copied());
            }
            (ready, candidate_blobs, live)
        };
        for file_num in ready {
            self.cache.lock().unwrap().remove(&file_num);
            self.clean_file(&self.dir.join(filenames::table(file_num)));
            if let Some(l) = &self.listener {
                l.on_table_deleted(file_num);
            }
        }
        // Delete blob files the removed sstables referenced that no live file still needs.
        for blob in candidate_blobs {
            if !live_blobs.contains(&blob) {
                let blob_path = self.dir.join(filenames::blob(blob));
                if self.fs.exists(&blob_path) {
                    self.blob_store.evict(blob);
                    self.clean_file(&blob_path);
                }
            }
        }
    }

    /// The background deletion pacer: cleans queued obsolete files, sleeping between them so
    /// the byte-deletion rate stays under `target_byte_deletion_rate`. On shutdown it drains
    /// whatever remains immediately (unpaced) so nothing is left dangling.
    fn deleter_loop(&self) {
        loop {
            let (path, size) = {
                let mut q = self.delete_queue.lock().unwrap();
                while q.items.is_empty() && !q.shutdown {
                    q = self.delete_cv.wait(q).unwrap();
                }
                if q.shutdown {
                    // Drain everything immediately, then exit.
                    let drained = std::mem::take(&mut q.items);
                    drop(q);
                    for (path, _) in drained {
                        let _ = self.cleaner.clean(self.fs.as_ref(), &path);
                    }
                    return;
                }
                q.items.remove(0)
            };
            let _ = self.cleaner.clean(self.fs.as_ref(), &path);
            // Pace the next deletion proportionally to the bytes just reclaimed, but wake
            // immediately if shutdown is requested or more work arrives.
            let secs = (size as f64 / self.target_byte_deletion_rate as f64).min(60.0);
            if secs > 0.0 {
                let q = self.delete_queue.lock().unwrap();
                if !q.shutdown {
                    let _ = self
                        .delete_cv
                        .wait_timeout(q, std::time::Duration::from_secs_f64(secs))
                        .unwrap();
                }
            }
        }
    }
}

/// An on-disk LSM key-value database.
///
/// Owns the shared [`DbInner`] and the background flush/compaction worker thread, which is
/// signaled to stop and joined when the `Db` is dropped.
pub struct Db {
    inner: Arc<DbInner>,
    /// Background flush/compaction workers (`max_concurrent_compactions` of them); they pick
    /// disjoint compaction inputs so they run concurrently.
    workers: Vec<std::thread::JoinHandle<()>>,
    /// The deletion-pacing thread, present only when `target_byte_deletion_rate > 0`.
    deleter: Option<std::thread::JoinHandle<()>>,
}

impl std::ops::Deref for Db {
    type Target = DbInner;
    fn deref(&self) -> &DbInner {
        &self.inner
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        // Tell the worker to exit and wait for it. Any data still in memtables is durable
        // in the WALs and will be recovered on the next open.
        {
            let mut state = self.inner.state.lock().unwrap();
            state.shutdown = true;
        }
        self.inner.work_cv.notify_all();
        for h in self.workers.drain(..) {
            let _ = h.join();
        }
        // Stop the deletion pacer (it drains any queued files before exiting).
        if let Some(h) = self.deleter.take() {
            {
                let mut q = self.inner.delete_queue.lock().unwrap();
                q.shutdown = true;
            }
            self.inner.delete_cv.notify_all();
            let _ = h.join();
        }
    }
}

impl Db {
    /// Opens the database in `dir`, creating it if `opts.create_if_missing` and absent.
    pub fn open(dir: impl AsRef<Path>, opts: Options) -> Result<Db> {
        let inner = Arc::new(DbInner::open_inner(dir, opts)?);
        // Spawn the background flush/compaction worker pool for writable databases. Multiple
        // workers pick disjoint compaction inputs, so compactions run concurrently.
        let read_only = inner.state.lock().unwrap().read_only;
        let mut workers = Vec::new();
        if !read_only {
            for _ in 0..inner.max_concurrent_compactions.max(1) {
                let w = Arc::clone(&inner);
                workers.push(std::thread::spawn(move || w.background_loop()));
            }
        }
        // Spawn the deletion pacer only when pacing is enabled on a writable database.
        let deleter = if !read_only && inner.target_byte_deletion_rate > 0 {
            let d = Arc::clone(&inner);
            Some(std::thread::spawn(move || d.deleter_loop()))
        } else {
            None
        };
        Ok(Db {
            inner,
            workers,
            deleter,
        })
    }

    /// Opens the database in `dir` read-only.
    pub fn open_read_only(dir: impl AsRef<Path>, mut opts: Options) -> Result<Db> {
        opts.read_only = true;
        opts.create_if_missing = false;
        Db::open(dir, opts)
    }
}

impl DbInner {
    /// Opens (or creates) the database, building the shared inner state. The caller wraps
    /// the result in an `Arc` and spawns the worker.
    fn open_inner(dir: impl AsRef<Path>, opts: Options) -> Result<DbInner> {
        let dir = dir.as_ref().to_path_buf();
        // With a disk-slow threshold configured, wrap the filesystem in a health checker that
        // routes slow operations to the event listener's `on_disk_slow`.
        let fs: Arc<dyn Fs> = match opts.disk_slow_threshold {
            Some(threshold) => {
                let listener = opts.event_listener.clone();
                Arc::new(crate::vfs::DiskHealthCheckingFs::new(
                    opts.fs.clone(),
                    threshold,
                    Arc::new(move |info| {
                        if let Some(l) = &listener {
                            l.on_disk_slow(info.op, &info.path, info.duration);
                        }
                    }),
                ))
            }
            None => opts.fs.clone(),
        };

        // WAL directories: the primary (override or the db dir) first, then any failover
        // directory. Recovery scans them all; the active WAL is created in the first that
        // accepts it.
        let mut wal_dirs = vec![opts.wal_dir.clone().unwrap_or_else(|| dir.clone())];
        if let Some(f) = opts.wal_failover_dir.clone() {
            wal_dirs.push(f);
        }
        if !opts.read_only {
            for d in &wal_dirs {
                fs.create_dir_all(d)?;
            }
        }

        let exists = fs.exists(&dir);
        if !exists {
            if opts.read_only || !opts.create_if_missing {
                return Err(Error::InvalidState(format!(
                    "db: directory {} does not exist",
                    dir.display()
                )));
            }
            fs.create_dir_all(&dir)?;
        }

        let mut names: Vec<String> = fs.list(&dir)?;
        names.sort();

        // Read the existing OPTIONS file (if any) once: it records the comparer and merger
        // names the store was written with, which drive registry resolution below.
        let options_file = match filenames::current_options(&names) {
            Some(name) => {
                let text = String::from_utf8_lossy(&fs.read(&dir.join(&name))?).into_owned();
                Some(OptionsFile::decode(&text)?)
            }
            None => None,
        };

        // Resolve the effective comparer/merger by the store's recorded names, consulting the
        // registries (`Options::comparers` / `Options::mergers`). This lets one process open
        // stores written with different comparers without knowing which in advance.
        let cmp = resolve_comparer(
            &opts,
            options_file.as_ref().map(|o| o.comparer_name.as_str()),
        )?;
        let merger = resolve_merger(
            &opts,
            options_file.as_ref().and_then(|o| o.merger_name.as_deref()),
        );
        let mut mem = Arc::new(MemTable::new(cmp.clone(), opts.mem_table_size));

        // Resolve the format major version: an existing OPTIONS file's value (validated
        // against the resolved comparer), or the option for a fresh store.
        let format_major_version = match &options_file {
            Some(of) => {
                of.validate(cmp.name())?;
                of.format_major_version
            }
            None => opts.format_major_version,
        };

        let mut vs = match filenames::current_manifest(&names) {
            Some(manifest_name) => {
                let bytes = fs.read(&dir.join(&manifest_name))?;
                let vs = VersionSet::load(&bytes, cmp.clone())?;
                // Recover the un-flushed WALs across every WAL directory. Recycled WALs are read
                // tolerantly so recovery stops at the stale tail left by a previous use.
                recover_wals(fs.as_ref(), &wal_dirs, &mem, vs, opts.wal_recycle_limit > 0)?
            }
            None => {
                if !opts.create_if_missing {
                    return Err(Error::corruption(
                        "db: no MANIFEST and create_if_missing=false",
                    ));
                }
                VersionSet::new(cmp.clone())
            }
        };

        // Repopulate in-memory blob references for recovered files: the MANIFEST does not
        // persist them, so blob-file GC would otherwise think recovered tables reference no
        // blob files and could delete blobs still in use. Only needed when blob files exist,
        // so a store that never used blob separation pays nothing.
        if !opts.read_only && names.iter().any(|n| n.ends_with(".blob")) {
            for level in vs.current.levels.iter_mut() {
                for f in level.iter_mut() {
                    if !f.blob_refs.is_empty() {
                        continue;
                    }
                    let bytes = fs.read(&dir.join(filenames::table(f.file_num)))?;
                    let refs = Reader::open(bytes, cmp.clone())?.blob_refs().to_vec();
                    if !refs.is_empty() {
                        let mut meta = (**f).clone();
                        meta.blob_refs = refs;
                        *f = Arc::new(meta);
                    }
                }
            }
        }

        if opts.read_only {
            let state = State {
                vs,
                mem,
                imm: Vec::new(),
                imm_wals: Vec::new(),
                wal_recycle: Vec::new(),
                wal: None,
                wal_number: 0,
                wal_dir_idx: 0,
                manifest: None,
                read_only: true,
                shutdown: false,
                flush_count: 0,
                compaction_count: 0,
                flush_bytes: 0,
                compaction_bytes: 0,
                read_miss: std::collections::HashMap::new(),
                read_queue: Vec::new(),
                compacting: std::collections::HashSet::new(),
                obsolete: Vec::new(),
            };
            let blob_store = Arc::new(BlobStore {
                fs: Arc::clone(&fs),
                dir: dir.clone(),
                cache: Mutex::new(HashMap::new()),
            });
            return Ok(DbInner {
                dir,
                cmp,
                wal_dirs,
                blob_store,
                mem_table_size: opts.mem_table_size,
                wal_sync: opts.wal_sync,
                wal_recycle_limit: opts.wal_recycle_limit,
                state: Mutex::new(state),
                flush_lock: Mutex::new(()),
                work_cv: Condvar::new(),
                drained_cv: Condvar::new(),
                cache: Mutex::new(HashMap::new()),
                snapshots: Mutex::new(Vec::new()),
                listener: opts.event_listener.clone(),
                merger: merger.clone(),
                block_cache: if opts.block_cache_size > 0 {
                    Some(Arc::new(crate::cache::BlockCache::new(
                        opts.block_cache_size,
                    )))
                } else {
                    None
                },
                max_open_files: opts.max_open_files.max(1),
                fs,
                _lock: None,
                format_major_version: Mutex::new(format_major_version),
                logger: opts.logger.clone(),
                cleaner: opts.cleaner.clone(),
                block_property_collectors: opts.block_property_collectors.clone(),
                tombstone_dense_compaction_threshold: opts.tombstone_dense_compaction_threshold,
                read_compaction_threshold: opts.read_compaction_threshold,
                l1_max_bytes: opts.l1_max_bytes.max(1),
                max_concurrent_compactions: opts.max_concurrent_compactions.max(1),
                value_block_threshold: opts.value_block_threshold,
                blob_value_threshold: opts.blob_value_threshold,
                level_target_file_sizes: opts.level_target_file_sizes.clone(),
                target_byte_deletion_rate: opts.target_byte_deletion_rate,
                delete_queue: Mutex::new(DeleteQueue::default()),
                delete_cv: Condvar::new(),
                commit_q: Mutex::new(CommitQueue::default()),
                mem_stop_threshold: opts.mem_table_stop_writes_threshold.max(1),
                l0_compaction_threshold: opts.l0_compaction_threshold.max(1),
                target_file_size: opts.target_file_size.max(1),
            });
        }

        // Read-write: take the exclusive directory lock to prevent concurrent opens, then
        // rotate in a fresh MANIFEST and WAL for this session.
        let lock = fs.lock(&dir.join("LOCK"))?;
        let manifest_num = vs.allocate_file_number();
        let wal_number = vs.allocate_file_number();
        vs.log_number = wal_number;

        let mut manifest =
            record::Writer::new(fs.create(&dir.join(filenames::manifest(manifest_num)))?);
        manifest.write_record(&vs.snapshot_edit().encode())?;
        manifest.sync_all()?;
        update_marker(
            fs.as_ref(),
            &dir,
            &names,
            &filenames::manifest(manifest_num),
        )?;
        fs.sync_dir(&dir)?;

        // If recovery loaded data from un-flushed WALs into the memtable, persist it to an
        // L0 sstable now. Otherwise advancing `log_number` to this session's WAL would
        // strand that data: the older WALs holding it become obsolete and are skipped on
        // the next open.
        if !mem.is_empty() {
            // Recovery flush goes to a single file (no split needed here).
            let file_num = vs.allocate_file_number();
            let metas = write_memtable_to_sstables(
                fs.as_ref(),
                &dir,
                &cmp,
                &[file_num],
                &mem,
                &opts.block_property_collectors,
                opts.target_file_size,
                opts.value_block_threshold,
                opts.blob_value_threshold,
            )?;
            let edit = VersionEdit {
                next_file_number: Some(vs.next_file_number),
                last_sequence: Some(vs.last_sequence),
                new_files: metas
                    .into_iter()
                    .map(|meta| NewFileEntry { level: 0, meta })
                    .collect(),
                ..Default::default()
            };
            vs.apply(&edit)?;
            manifest.write_record(&edit.encode())?;
            manifest.sync_all()?;
            mem = Arc::new(MemTable::new(cmp.clone(), opts.mem_table_size));
        }

        let (wal_writer, wal_dir_idx) = create_wal(fs.as_ref(), &wal_dirs, 0, wal_number)?;
        let wal = wal_writer;
        if let Some(l) = &opts.event_listener {
            l.on_manifest_created(manifest_num);
            l.on_wal_created(wal_number);
        }

        // Record the options for this session in a fresh OPTIONS file.
        let options_num = vs.allocate_file_number();
        let options_file = OptionsFile {
            comparer_name: cmp.name().to_string(),
            merger_name: opts.merger.as_ref().map(|m| m.name().to_string()),
            format_major_version,
        };
        {
            let mut of = fs.create(&dir.join(filenames::options(options_num)))?;
            of.write_all(options_file.encode().as_bytes())?;
            of.sync_all()?;
        }
        fs.sync_dir(&dir)?;

        let state = State {
            vs,
            mem,
            imm: Vec::new(),
            imm_wals: Vec::new(),
            wal_recycle: Vec::new(),
            wal: Some(wal),
            wal_number,
            wal_dir_idx,
            manifest: Some(manifest),
            read_only: false,
            shutdown: false,
            flush_count: 0,
            compaction_count: 0,
            flush_bytes: 0,
            compaction_bytes: 0,
            read_miss: std::collections::HashMap::new(),
            read_queue: Vec::new(),
            compacting: std::collections::HashSet::new(),
            obsolete: Vec::new(),
        };
        let blob_store = Arc::new(BlobStore {
            fs: Arc::clone(&fs),
            dir: dir.clone(),
            cache: Mutex::new(HashMap::new()),
        });
        Ok(DbInner {
            dir,
            cmp,
            wal_dirs,
            blob_store,
            mem_table_size: opts.mem_table_size,
            wal_sync: opts.wal_sync,
            wal_recycle_limit: opts.wal_recycle_limit,
            state: Mutex::new(state),
            flush_lock: Mutex::new(()),
            work_cv: Condvar::new(),
            drained_cv: Condvar::new(),
            cache: Mutex::new(HashMap::new()),
            snapshots: Mutex::new(Vec::new()),
            listener: opts.event_listener.clone(),
            merger: merger.clone(),
            block_cache: if opts.block_cache_size > 0 {
                Some(Arc::new(crate::cache::BlockCache::new(
                    opts.block_cache_size,
                )))
            } else {
                None
            },
            max_open_files: opts.max_open_files.max(1),
            fs,
            _lock: Some(lock),
            format_major_version: Mutex::new(format_major_version),
            logger: opts.logger.clone(),
            cleaner: opts.cleaner.clone(),
            block_property_collectors: opts.block_property_collectors.clone(),
            tombstone_dense_compaction_threshold: opts.tombstone_dense_compaction_threshold,
            read_compaction_threshold: opts.read_compaction_threshold,
            l1_max_bytes: opts.l1_max_bytes.max(1),
            max_concurrent_compactions: opts.max_concurrent_compactions.max(1),
            value_block_threshold: opts.value_block_threshold,
            blob_value_threshold: opts.blob_value_threshold,
            level_target_file_sizes: opts.level_target_file_sizes.clone(),
            target_byte_deletion_rate: opts.target_byte_deletion_rate,
            delete_queue: Mutex::new(DeleteQueue::default()),
            delete_cv: Condvar::new(),
            commit_q: Mutex::new(CommitQueue::default()),
            mem_stop_threshold: opts.mem_table_stop_writes_threshold.max(1),
            l0_compaction_threshold: opts.l0_compaction_threshold.max(1),
            target_file_size: opts.target_file_size.max(1),
        })
    }

    /// The store's current on-disk format major version.
    pub fn format_major_version(&self) -> FormatMajorVersion {
        *self.format_major_version.lock().unwrap()
    }

    /// Ratchets the on-disk format major version up to `target`, writing a new OPTIONS
    /// file. Ratcheting is monotonic: a `target` at or below the current version is a
    /// no-op, and one newer than this implementation supports is rejected.
    pub fn ratchet_format_major_version(&self, target: FormatMajorVersion) -> Result<()> {
        if target > FormatMajorVersion::NEWEST {
            return Err(Error::InvalidState(format!(
                "db: format_major_version {} exceeds the newest supported ({})",
                target.as_u32(),
                FormatMajorVersion::NEWEST.as_u32()
            )));
        }
        let mut cur = self.format_major_version.lock().unwrap();
        if target <= *cur {
            return Ok(());
        }
        if self.state.lock().unwrap().read_only {
            return Err(Error::InvalidState("db: opened read-only".into()));
        }
        // Ratchet one version at a time, running each version's migration and persisting a
        // fresh OPTIONS file after each step, so an interrupted upgrade leaves the store at
        // a well-defined intermediate version (Pebble's format-major-version migrations).
        while *cur < target {
            let next = FormatMajorVersion(cur.as_u32() + 1);
            self.run_format_migration(next)?;
            let options_num = self.state.lock().unwrap().vs.allocate_file_number();
            let of = OptionsFile {
                comparer_name: self.cmp.name().to_string(),
                merger_name: self.merger.as_ref().map(|m| m.name().to_string()),
                format_major_version: next,
            };
            {
                let mut f = self
                    .fs
                    .create(&self.dir.join(filenames::options(options_num)))?;
                f.write_all(of.encode().as_bytes())?;
                f.sync_all()?;
            }
            self.fs.sync_dir(&self.dir)?;
            *cur = next;
            self.log(&format!(
                "ratcheted format major version to {}",
                next.as_u32()
            ));
            if let Some(l) = &self.listener {
                l.on_format_upgrade(next.as_u32());
            }
        }
        Ok(())
    }

    /// Runs the on-disk migration required to move *to* format major version `v`. Most
    /// versions need no data migration in this engine (the formats are already supported);
    /// the per-version hook exists so future versions that require rewriting on-disk state
    /// have a defined place to do it.
    fn run_format_migration(&self, _v: FormatMajorVersion) -> Result<()> {
        Ok(())
    }

    /// Creates an [`IndexedBatch`]: a write batch you can read from before committing
    /// (read-your-own-writes). Commit it with [`DbInner::write`] after calling
    /// [`IndexedBatch::into_batch`].
    pub fn indexed_batch(&self) -> IndexedBatch {
        IndexedBatch::new(self.merger.clone())
    }

    /// The largest sequence number assigned so far.
    pub fn last_sequence(&self) -> SeqNum {
        self.state.lock().unwrap().vs.last_sequence
    }

    /// Sets `key` to `value`.
    pub fn set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut b = Batch::new();
        b.set(key, value);
        self.apply(b)
    }

    /// Deletes `key`.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let mut b = Batch::new();
        b.delete(key);
        self.apply(b)
    }

    /// Deletes `key` using single-delete semantics: valid only when `key` has at most one
    /// prior `set` not yet compacted away. Behaves as a deletion on read.
    pub fn single_delete(&self, key: &[u8]) -> Result<()> {
        let mut b = Batch::new();
        b.single_delete(key);
        self.apply(b)
    }

    /// Deletes `key`, recording the approximate deleted value size as a compaction hint
    /// (Pebblev4 `DELSIZED`). Behaves as a deletion on read.
    pub fn delete_sized(&self, key: &[u8], value_size: u64) -> Result<()> {
        let mut b = Batch::new();
        b.delete_sized(key, value_size);
        self.apply(b)
    }

    /// Appends opaque data to the write-ahead log without modifying the keyspace
    /// (Pebble's `LogData`). Useful for embedding application markers in the WAL.
    pub fn log_data(&self, data: &[u8]) -> Result<()> {
        let mut b = Batch::new();
        b.log_data(data);
        self.apply(b)
    }

    /// Records a merge operand for `key`, combined with prior values by the configured
    /// merge operator at read time.
    pub fn merge(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut b = Batch::new();
        b.merge(key, value);
        self.apply(b)
    }

    /// Deletes every key in the half-open user-key range `[start, end)`.
    pub fn delete_range(&self, start: &[u8], end: &[u8]) -> Result<()> {
        let mut b = Batch::new();
        b.delete_range(start, end);
        self.apply(b)
    }

    /// Removes every key in `[start, end)` and physically reclaims the space, by writing a
    /// range deletion over the span and then compacting it toward the bottom level so the
    /// covered data is dropped rather than just hidden. A simplified form of Pebble's
    /// `Excise` (which also rewrites partially-overlapping boundary files as virtual
    /// sstables; here the compaction rewrites them).
    pub fn excise(&self, start: &[u8], end: &[u8]) -> Result<()> {
        self.delete_range(start, end)?;
        self.compact_range(Some(start), Some(end))
    }

    /// Sets a range key over `[start, end)` at `suffix` to `value`.
    pub fn range_key_set(
        &self,
        start: &[u8],
        end: &[u8],
        suffix: &[u8],
        value: &[u8],
    ) -> Result<()> {
        let mut b = Batch::new();
        b.range_key_set(start, end, suffix, value);
        self.apply(b)
    }

    /// Removes the range key at `suffix` over `[start, end)`.
    pub fn range_key_unset(&self, start: &[u8], end: &[u8], suffix: &[u8]) -> Result<()> {
        let mut b = Batch::new();
        b.range_key_unset(start, end, suffix);
        self.apply(b)
    }

    /// Deletes all range keys over `[start, end)`.
    pub fn range_key_delete(&self, start: &[u8], end: &[u8]) -> Result<()> {
        let mut b = Batch::new();
        b.range_key_delete(start, end);
        self.apply(b)
    }

    /// Returns the range-key entries covering `user_key`, newest first (by sequence
    /// number), across the memtable, immutable memtables, and all sstables.
    ///
    /// This returns the raw entries; coalescing `RANGEKEYSET`/`UNSET`/`DEL` into an
    /// effective set of suffix/value pairs (and iterator masking) is refined in a later
    /// phase.
    pub fn range_keys_covering(&self, user_key: &[u8]) -> Result<Vec<RangeKeyEntry>> {
        let (mem, imm, version) = {
            let state = self.state.lock().unwrap();
            (
                Arc::clone(&state.mem),
                state.imm.clone(),
                state.vs.current.clone(),
            )
        };
        let cmp = self.cmp.as_ref();
        let mut out = Vec::new();
        let mut collect = |entries: Vec<RangeKeyEntry>| -> Result<()> {
            for e in entries {
                if e.covers(cmp, user_key)? {
                    out.push(e);
                }
            }
            Ok(())
        };
        collect(mem.range_keys())?;
        for m in imm.iter().rev() {
            collect(m.range_keys())?;
        }
        for level in version.levels.iter() {
            for f in level {
                let reader = self.open_reader(f.file_num)?;
                collect(reader.range_keys().to_vec())?;
            }
        }
        out.sort_by_key(|e| std::cmp::Reverse(e.seqnum));
        Ok(out)
    }

    /// Atomically applies all operations in `batch`. Alias for [`DbInner::apply`].
    pub fn write(&self, batch: Batch) -> Result<()> {
        self.apply(batch)
    }

    /// Atomically applies all operations in `batch`.
    ///
    /// Commits go through a **group-commit** pipeline: a write enqueues its batch and then
    /// either becomes the *leader* — flushing every currently-queued batch through a single
    /// WAL sync and a run of memtable applies — or waits for a concurrent leader to commit
    /// its batch. Under concurrency this amortizes one `fsync` across many writers; with a
    /// single writer it degrades to one batch per commit.
    pub fn apply(&self, batch: Batch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let slot = Arc::new(CommitSlot {
            result: Mutex::new(None),
            cv: Condvar::new(),
        });

        // Enqueue and claim or decline leadership atomically under the queue lock.
        let am_leader = {
            let mut q = self.commit_q.lock().unwrap();
            q.items.push((batch, Arc::clone(&slot)));
            if q.leader {
                false
            } else {
                q.leader = true;
                true
            }
        };

        if !am_leader {
            // A leader is active. Because we pushed our slot before observing `leader == true`
            // (both under the queue lock), the leader's drain loop is guaranteed to pick us up:
            // it only clears `leader` after seeing an empty queue under that same lock. Wait for
            // our result to be published.
            let mut r = slot.result.lock().unwrap();
            while r.is_none() {
                r = slot.cv.wait(r).unwrap();
            }
            return r.take().unwrap();
        }

        // We are the leader: drain and commit the queue until it is empty, then relinquish
        // leadership. Draining to empty (rather than committing a single snapshot) ensures any
        // write that enqueues while we hold leadership is committed by us — it cannot be
        // stranded, since a writer that sees `leader == true` becomes a follower we will drain.
        loop {
            let group = {
                let mut q = self.commit_q.lock().unwrap();
                if q.items.is_empty() {
                    q.leader = false;
                    break;
                }
                std::mem::take(&mut q.items)
            };
            let result = self.commit_group(&group);
            for (_, s) in &group {
                *s.result.lock().unwrap() = Some(clone_commit_result(&result));
                s.cv.notify_all();
            }
        }

        // Our own batch was committed in one of the iterations above.
        slot.result
            .lock()
            .unwrap()
            .take()
            .unwrap_or_else(|| Err(Error::InvalidState("commit: result not published".into())))
    }

    /// Commits a group of queued batches under one state-lock acquisition and a single WAL
    /// sync, assigning sequence numbers in queue order. Returns one shared result for the
    /// whole group (all batches in a group share fate: they are made durable and visible
    /// together, or fail together).
    fn commit_group(&self, group: &[(Batch, Arc<CommitSlot>)]) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if state.read_only {
            return Err(Error::InvalidState("db: opened read-only".into()));
        }

        // Write stall: if too many immutable memtables are awaiting flush, block until the
        // background worker drains the queue below the threshold (checked once for the group;
        // the wait releases the lock so the flush worker can make progress).
        let mut stalled = false;
        while state.imm.len() >= self.mem_stop_threshold {
            if !stalled {
                stalled = true;
                if let Some(l) = &self.listener {
                    l.on_write_stall_begin("too many immutable memtables");
                }
            }
            state = self.drained_cv.wait(state).unwrap();
        }
        if stalled && let Some(l) = &self.listener {
            l.on_write_stall_end();
        }

        for (batch, _) in group {
            self.commit_one(&mut state, batch)?;
        }
        Ok(())
    }

    /// Applies a single batch within a held state lock: memtable rotation, sequence
    /// assignment, WAL append+sync (with failover), and memtable apply. Write-stall
    /// backpressure is applied once per group by the caller (it owns the lock guard).
    fn commit_one(&self, state: &mut State, batch: &Batch) -> Result<()> {
        let mut batch = batch.clone();
        // Rotate once the memtable has used half its arena, leaving the rest as headroom
        // for this batch (the arena fills faster than wire bytes due to per-node
        // overhead, so the threshold is measured in arena bytes). The actual flush of the
        // rotated memtable runs on the background worker, off this writer's path.
        if state.mem.size() as usize >= self.mem_table_size / 2 {
            self.rotate_memtable(state)?;
            self.work_cv.notify_all();
        }

        let base = state.vs.last_sequence + 1;
        batch.set_seqnum(base);
        if state.wal.is_some() {
            self.append_to_wal(state, &batch)?;
        }
        state.mem.apply(&batch)?;
        state.vs.last_sequence = base + u64::from(batch.count()) - 1;
        Ok(())
    }

    /// Appends `batch` to the active WAL, failing over to the next WAL directory if the
    /// write (or its sync) fails — e.g. a stalled or failing disk. The batch is re-logged
    /// to the new WAL so it is durable before the write returns.
    fn append_to_wal(&self, state: &mut State, batch: &Batch) -> Result<()> {
        {
            let wal = state.wal.as_mut().expect("wal present");
            let res = wal.write_record(batch.as_bytes()).and_then(|_| {
                if self.wal_sync {
                    wal.sync_all()
                } else {
                    wal.flush()
                }
            });
            if res.is_ok() {
                return Ok(());
            }
            // Primary write failed: fall through to failover (if a directory is available).
            if state.wal_dir_idx + 1 >= self.wal_dirs.len() {
                return res; // no failover directory left
            }
        }
        // Rotate to a fresh WAL in the next directory and re-log the batch there so it is
        // durable. The failed WAL keeps its number in `imm_wals`-style cleanup via the
        // normal flush path is not applicable here, so it is left for the next open's
        // recovery scan / obsolete-file handling.
        let next_dir = state.wal_dir_idx + 1;
        let new_wal = state.vs.allocate_file_number();
        let (mut writer, dir_idx) =
            create_wal(self.fs.as_ref(), &self.wal_dirs, next_dir, new_wal)?;
        writer.write_record(batch.as_bytes())?;
        if self.wal_sync {
            writer.sync_all()?;
        } else {
            writer.flush()?;
        }
        if let Some(l) = &self.listener {
            l.on_wal_created(new_wal);
        }
        state.wal = Some(writer);
        state.wal_number = new_wal;
        state.wal_dir_idx = dir_idx;
        Ok(())
    }

    /// Flushes the active memtable to an L0 sstable, returning once all data written so
    /// far is durable in sstables (and any triggered compactions have run).
    pub fn flush(&self) -> Result<()> {
        {
            let mut state = self.state.lock().unwrap();
            if state.read_only {
                return Err(Error::InvalidState("db: opened read-only".into()));
            }
            self.rotate_memtable(&mut state)?;
        }
        // Drain the immutable queue synchronously (cooperating with the worker, which may
        // also be draining — `flush_one` is serialized by `flush_lock`).
        while self.flush_one()? {}
        Ok(())
    }

    /// Rotates the current memtable into the immutable queue and opens a fresh WAL for the
    /// new active memtable. Cheap: no sstable is written. A no-op if the memtable is empty.
    /// Opens the writer for a new WAL numbered `new_wal`, reusing a pooled obsolete WAL file
    /// in place when recycling is enabled (single directory, a number available). On any
    /// recycling error it falls back to creating a fresh file.
    fn create_or_recycle_wal(
        &self,
        state: &mut State,
        new_wal: u64,
    ) -> Result<(record::Writer<Box<dyn WritableFile>>, usize)> {
        if self.wal_recycle_limit > 0
            && self.wal_dirs.len() == 1
            && let Some(old) = state.wal_recycle.pop()
        {
            let dir = &self.wal_dirs[0];
            let from = dir.join(wal_filename(old));
            let to = dir.join(wal_filename(new_wal));
            // Rename the obsolete file to the new number and reopen it without truncation so its
            // already-allocated blocks are reused; the stale tail is handled by tolerant recovery.
            match self.fs.rename(&from, &to).and_then(|_| self.fs.reuse(&to)) {
                Ok(file) => {
                    if let Some(l) = &self.listener {
                        l.on_wal_created(new_wal);
                    }
                    return Ok((record::Writer::with_log_num(file, new_wal as u32), 0));
                }
                Err(_) => {
                    // Recycling failed; drop the stale files and fall through to a fresh WAL.
                    self.clean_file(&from);
                    self.clean_file(&to);
                }
            }
        }
        let r = create_wal(self.fs.as_ref(), &self.wal_dirs, state.wal_dir_idx, new_wal)?;
        if let Some(l) = &self.listener {
            l.on_wal_created(new_wal);
        }
        Ok(r)
    }

    fn rotate_memtable(&self, state: &mut State) -> Result<()> {
        if state.mem.is_empty() {
            return Ok(());
        }
        // Group commit defers WAL syncs, so the outgoing WAL may hold un-synced records that
        // belong to the memtable being rotated out. Sync it before it becomes immutable so
        // that data is durable even though it is not yet in an sstable.
        if self.wal_sync
            && let Some(w) = state.wal.as_mut()
        {
            let _ = w.sync_all();
        }
        let new_wal = state.vs.allocate_file_number();
        // Keep the active WAL directory across rotation (don't fall back to the primary if
        // we've already failed over to a secondary).
        let (wal, dir_idx) = self.create_or_recycle_wal(state, new_wal)?;
        state.wal_dir_idx = dir_idx;
        let old_mem = std::mem::replace(
            &mut state.mem,
            Arc::new(MemTable::new(self.cmp.clone(), self.mem_table_size)),
        );
        let old_wal = state.wal_number;
        state.wal = Some(wal);
        state.wal_number = new_wal;
        state.imm.push(old_mem);
        state.imm_wals.push(old_wal);
        // `log_number` (the oldest un-flushed WAL) is unchanged: it was `old_wal`, which is
        // now `imm_wals[0]` if this is the first immutable, or already older.
        Ok(())
    }

    /// Flushes the oldest immutable memtable (if any) to an L0 sstable, records it in the
    /// MANIFEST, removes its WAL, and runs any triggered compaction. Returns whether a
    /// memtable was flushed. The expensive sstable write happens with the state lock
    /// released so foreground reads and writes proceed; flushes are serialized by
    /// `flush_lock` so the worker and an explicit `flush` never double-flush.
    fn flush_one(&self) -> Result<bool> {
        let _flushing = self.flush_lock.lock().unwrap();

        let (mem, file_nums) = {
            let mut state = self.state.lock().unwrap();
            if state.imm.is_empty() {
                return Ok(false);
            }
            let mem = Arc::clone(&state.imm[0]);
            // Pre-allocate enough file numbers for flush splitting: the output (compressed) is
            // no larger than the arena, so ceil(arena / target) + 1 is a safe upper bound.
            // Unused numbers simply leave gaps.
            let n = ((mem.size() as u64).div_ceil(self.target_file_size).max(1) + 1) as usize;
            let file_nums: Vec<u64> = (0..n).map(|_| state.vs.allocate_file_number()).collect();
            (mem, file_nums)
        };
        if let Some(l) = &self.listener {
            l.on_flush_begin();
        }

        // Write the sstable(s) without holding the state lock, splitting large point-only
        // memtables at the target file size.
        let metas = write_memtable_to_sstables(
            self.fs.as_ref(),
            &self.dir,
            &self.cmp,
            &file_nums,
            &mem,
            &self.block_property_collectors,
            self.target_file_size,
            self.value_block_threshold,
            self.blob_value_threshold,
        )?;
        let flushed_bytes: u64 = metas.iter().map(|m| m.size).sum();
        let first_file_num = metas.first().map(|m| m.file_num).unwrap_or(file_nums[0]);
        let created: Vec<u64> = metas.iter().map(|m| m.file_num).collect();

        let mut state = self.state.lock().unwrap();
        // The new oldest un-flushed WAL once this immutable is removed.
        let new_log = state.imm_wals.get(1).copied().unwrap_or(state.wal_number);
        let edit = VersionEdit {
            log_number: Some(new_log),
            next_file_number: Some(state.vs.next_file_number),
            last_sequence: Some(state.vs.last_sequence),
            new_files: metas
                .into_iter()
                .map(|meta| NewFileEntry { level: 0, meta })
                .collect(),
            ..Default::default()
        };
        state.vs.apply(&edit)?;
        state.vs.log_number = new_log;
        if let Some(mw) = state.manifest.as_mut() {
            mw.write_record(&edit.encode())?;
            mw.sync_all()?;
        }
        state.imm.remove(0);
        let popped_wal = state.imm_wals.remove(0);
        state.flush_count += 1;
        state.flush_bytes += flushed_bytes;
        if popped_wal != 0 {
            // Recycling: with a single WAL directory and room in the pool, keep the file on
            // disk and remember its number so the next rotation can reuse it in place rather
            // than create and allocate a fresh file. Otherwise delete it. (Failover configs use
            // multiple directories and don't track which one holds each WAL, so recycling is
            // skipped there.)
            if self.wal_recycle_limit > 0
                && self.wal_dirs.len() == 1
                && state.wal_recycle.len() < self.wal_recycle_limit
            {
                state.wal_recycle.push(popped_wal);
            } else {
                // The WAL may live in any configured directory (failover); clean from each.
                for dir in &self.wal_dirs {
                    self.clean_file(&dir.join(wal_filename(popped_wal)));
                }
                if let Some(l) = &self.listener {
                    l.on_wal_deleted(popped_wal);
                }
            }
        }
        drop(state);
        // Keep the LSM in shape (e.g. drain L0 once it accumulates enough files). Runs off the
        // state lock so its compaction writes don't block foreground reads/writes.
        self.maybe_compact()?;

        self.log(&format!(
            "flushed memtable to {} sstable(s) starting at {first_file_num} ({flushed_bytes} bytes)",
            created.len()
        ));
        if let Some(l) = &self.listener {
            for file_num in &created {
                l.on_table_created(*file_num);
            }
            // One flush event per memtable flush (matching `flush_count`), carrying the total
            // bytes written across any split output files.
            l.on_flush_end(first_file_num, flushed_bytes);
        }
        self.drained_cv.notify_all();
        Ok(true)
    }

    /// The background worker loop: waits for a rotated memtable and flushes it (and runs
    /// any triggered compaction), until the database is dropped.
    fn background_loop(&self) {
        loop {
            {
                let mut state = self.state.lock().unwrap();
                // Wake for a memtable to flush, queued read-compactions, an available
                // score-based compaction, or shutdown. Several workers may wake together and
                // pick disjoint compactions (the `compacting` set keeps them from colliding).
                while state.imm.is_empty()
                    && state.read_queue.is_empty()
                    && !self.compaction_available(&state)
                    && !state.shutdown
                {
                    state = self.work_cv.wait(state).unwrap();
                }
                if state.shutdown {
                    return; // any pending data stays in the WALs for recovery
                }
            }
            // Flush one memtable (which also compacts), or — when there was nothing to flush —
            // run any available compactions directly. Both happen off the state lock, so
            // multiple workers run their (disjoint) compactions concurrently.
            let res = self.flush_one().and_then(|flushed| {
                if flushed {
                    Ok(())
                } else {
                    self.maybe_compact()
                }
            });
            if let Err(e) = res {
                if let Some(l) = &self.listener {
                    l.on_background_error(&e.to_string());
                }
                // Back off briefly so a persistent error doesn't spin the CPU.
                std::thread::yield_now();
            }
        }
    }

    /// Looks up `key`, returning its value or `None` if absent or deleted.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let snapshot = self.state.lock().unwrap().vs.last_sequence;
        self.get_at(key, snapshot)
    }

    /// Looks up `key` as visible at sequence number `snapshot`.
    ///
    /// Sources are consulted newest-first. As each is examined its covering range
    /// tombstones (with `seqnum <= snapshot`) raise a running maximum; the first source
    /// that holds a point entry for `key` decides the result: the entry is visible only
    /// if its sequence number exceeds every covering tombstone seen so far (and is a
    /// non-tombstone kind).
    pub fn get_at(&self, key: &[u8], snapshot: SeqNum) -> Result<Option<Vec<u8>>> {
        // Snapshot the volatile state under the lock, then read without holding it.
        let (mem, imm, version) = {
            let state = self.state.lock().unwrap();
            (
                Arc::clone(&state.mem),
                state.imm.clone(),
                state.vs.current.clone(),
            )
        };

        let cmp = self.cmp.as_ref();
        let mut max_rts = 0u64;
        // Merge operands gathered newest-first until a terminator (Set/Delete/range
        // tombstone) is reached.
        let mut operands: Vec<Vec<u8>> = Vec::new();
        let mut base: Option<Vec<u8>> = None;

        // Resolves a source's versions into the running merge state; returns true if a
        // terminator was reached (no need to consult older sources).
        let mut resolve_versions =
            |versions: Vec<(SeqNum, InternalKeyKind, Vec<u8>)>, max_rts: u64| -> Option<bool> {
                for (seq, kind, value) in versions {
                    if seq <= max_rts {
                        base = None;
                        return Some(true); // shadowed by a range tombstone: acts as delete
                    }
                    match kind {
                        InternalKeyKind::Merge => operands.push(value),
                        InternalKeyKind::Set | InternalKeyKind::SetWithDelete => {
                            base = Some(value);
                            return Some(true);
                        }
                        InternalKeyKind::Delete
                        | InternalKeyKind::SingleDelete
                        | InternalKeyKind::DeleteSized => {
                            base = None;
                            return Some(true);
                        }
                        _ => {}
                    }
                }
                None
            };

        let mut terminated = false;
        // Mutable memtable.
        max_rts = max_rts.max(max_covering_seqnum(
            &mem.range_tombstones(),
            cmp,
            key,
            snapshot,
        ));
        if resolve_versions(mem.lookup_versions(key, snapshot), max_rts).is_some() {
            terminated = true;
        }
        // Immutable memtables, newest first.
        if !terminated {
            for m in imm.iter().rev() {
                max_rts = max_rts.max(max_covering_seqnum(
                    &m.range_tombstones(),
                    cmp,
                    key,
                    snapshot,
                ));
                if resolve_versions(m.lookup_versions(key, snapshot), max_rts).is_some() {
                    terminated = true;
                    break;
                }
            }
        }
        // Sstables: L0 newest-first, then L1..L6. Files that overlap the key by range but hold
        // no version of it are "passed through"; if the key is then found in a deeper sstable,
        // those passes are charged as wasted seeks to drive read-triggered compaction.
        let mut passed: Vec<u64> = Vec::new();
        let mut resolved_in_sstable = false;
        if !terminated {
            'levels: for level in 0..NUM_LEVELS {
                for f in version.overlapping(cmp, level, key) {
                    let reader = self.open_reader(f.file_num)?;
                    max_rts = max_rts.max(max_covering_seqnum(
                        reader.range_tombstones(),
                        cmp,
                        key,
                        snapshot,
                    ));
                    if resolve_versions(reader.lookup_versions(key, snapshot)?, max_rts).is_some() {
                        resolved_in_sstable = true;
                        break 'levels;
                    }
                    // Only L1+ files are eligible for read-triggered compaction (L0 files are
                    // not range-partitioned, so a single-file compaction of one is unsafe).
                    if level > 0 {
                        passed.push(f.file_num);
                    }
                }
            }
        }
        if resolved_in_sstable && !passed.is_empty() {
            self.charge_read_seeks(&passed);
        }

        if operands.is_empty() {
            // No merge operands: plain point semantics.
            return Ok(base);
        }
        // Apply merge operands (chronological / oldest-first) over the base value.
        operands.reverse();
        match &self.merger {
            Some(m) => Ok(Some(m.full_merge(key, base.as_deref(), &operands))),
            None => Ok(operands.pop()), // no merger configured: newest operand
        }
    }

    /// Records wasted read seeks against `files` (read-triggered compaction). When a file's
    /// count crosses [`Options::read_compaction_threshold`] it is queued for compaction and
    /// the background worker is woken. Cheap and lock-light: only taken when a read actually
    /// passed through files to reach a deeper level, and a no-op when the feature is disabled.
    fn charge_read_seeks(&self, files: &[u64]) {
        let threshold = self.read_compaction_threshold;
        if threshold == 0 {
            return;
        }
        let mut state = self.state.lock().unwrap();
        if state.read_only {
            return;
        }
        let mut queued = false;
        for &file_num in files {
            let count = state.read_miss.entry(file_num).or_insert(0);
            *count += 1;
            if *count >= threshold && !state.read_queue.contains(&file_num) {
                state.read_queue.push(file_num);
                queued = true;
            }
        }
        if queued {
            self.work_cv.notify_all();
        }
    }

    /// Returns a forward iterator over all keys at the latest sequence number.
    pub fn iter(&self) -> Result<DbIterator> {
        let snapshot = self.state.lock().unwrap().vs.last_sequence;
        self.iter_at(snapshot)
    }

    /// Returns a forward iterator over all keys as visible at `snapshot`.
    pub fn iter_at(&self, snapshot: SeqNum) -> Result<DbIterator> {
        self.iter_at_with_options(snapshot, IterOptions::default())
    }

    /// Returns an iterator with bounds (and other [`IterOptions`]) over the latest view.
    pub fn iter_with_options(&self, opts: IterOptions) -> Result<DbIterator> {
        let snapshot = self.state.lock().unwrap().vs.last_sequence;
        self.iter_at_with_options(snapshot, opts)
    }

    /// Returns an iterator with bounds over the view as visible at `snapshot`.
    pub fn iter_at_with_options(&self, snapshot: SeqNum, opts: IterOptions) -> Result<DbIterator> {
        let (sources, tombstones, range_keys) = self.collect_iter_sources(&opts)?;
        DbIterator::with_options(
            sources,
            snapshot,
            self.cmp.clone(),
            tombstones,
            range_keys,
            self.merger.clone(),
            opts,
        )
    }

    /// Collects the merge sources (newest first), range tombstones, and range keys for an
    /// iterator over the current LSM, honoring `only_durable` and block-property filters.
    /// Shared by [`iter_at_with_options`](Self::iter_at_with_options) and the indexed-batch
    /// iterator, which prepends its own (newest) source.
    #[allow(clippy::type_complexity)]
    fn collect_iter_sources(
        &self,
        opts: &IterOptions,
    ) -> Result<(
        Vec<Box<dyn InternalIter>>,
        Vec<RangeTombstone>,
        Vec<RangeKeyEntry>,
    )> {
        let (mem, imm, version) = {
            let state = self.state.lock().unwrap();
            (
                Arc::clone(&state.mem),
                state.imm.clone(),
                state.vs.current.clone(),
            )
        };

        let mut sources: Vec<Box<dyn InternalIter>> = Vec::new();
        let mut tombstones = Vec::new();
        let mut range_keys = Vec::new();
        // In durable-only mode the memtables are skipped entirely: only flushed sstables (and
        // their tombstones / range keys) are visible.
        if !opts.only_durable {
            tombstones = mem.range_tombstones();
            range_keys = mem.range_keys();
            sources.push(Box::new(mem.scan()));
            for m in imm.iter().rev() {
                tombstones.extend(m.range_tombstones());
                range_keys.extend(m.range_keys());
                sources.push(Box::new(m.scan()));
            }
        }
        for level in version.levels.iter() {
            for f in level {
                let reader = self.open_reader(f.file_num)?;
                // Range tombstones and range keys must be consulted even when a block-property
                // filter rules the table's point keys out: they can shadow keys elsewhere.
                tombstones.extend_from_slice(reader.range_tombstones());
                range_keys.extend_from_slice(reader.range_keys());
                let point_excluded = opts
                    .block_property_filters
                    .iter()
                    .any(|filter| !reader.may_match_block_property(filter.as_ref()));
                if !point_excluded {
                    // Within a table that passes the table-level filter, skip individual data
                    // blocks ruled out by the same filters via their per-block properties.
                    sources.push(Box::new(
                        reader.iter_with_filters(&opts.block_property_filters)?,
                    ));
                }
            }
        }
        Ok((sources, tombstones, range_keys))
    }

    /// Returns a lazy iterator over the database **as if `batch` were already applied** on top
    /// of the current committed view (Pebble's indexed-batch iterator). The batch's point ops,
    /// range deletions, and range keys are layered above committed data by materializing them
    /// into a private memtable at sequence numbers just above the committed snapshot, then
    /// merged through the normal collapse / range-tombstone / range-key / masking machinery —
    /// so a batch `Set` shadows a committed value, a batch `delete_range` hides committed keys
    /// in its span, and batch `merge` operands fold over the committed base. Unlike
    /// [`IndexedBatch::scan`](crate::IndexedBatch::scan), nothing is materialized eagerly.
    pub(crate) fn iter_with_batch(&self, batch: &Batch, opts: IterOptions) -> Result<DbIterator> {
        // Snapshot the committed view, then place the batch just above it.
        let base = self.state.lock().unwrap().vs.last_sequence;
        let mut staged = batch.clone();
        staged.set_seqnum(base + 1);
        let count = u64::from(staged.count());
        let bmem = Arc::new(MemTable::new(self.cmp.clone(), self.mem_table_size.max(1)));
        bmem.apply(&staged)?;

        let (mut sources, mut tombstones, mut range_keys) = self.collect_iter_sources(&opts)?;
        // The batch is the newest source; its entries (seqnums > base) win over committed ones.
        let mut batch_sources: Vec<Box<dyn InternalIter>> = vec![Box::new(bmem.scan())];
        batch_sources.append(&mut sources);
        let mut batch_tombstones = bmem.range_tombstones();
        batch_tombstones.append(&mut tombstones);
        let mut batch_range_keys = bmem.range_keys();
        batch_range_keys.append(&mut range_keys);

        // The snapshot must cover every staged seqnum so all batch ops are visible.
        DbIterator::with_options(
            batch_sources,
            base + count,
            self.cmp.clone(),
            batch_tombstones,
            batch_range_keys,
            self.merger.clone(),
            opts,
        )
    }

    /// Scans the raw internal contents of the LSM (Pebble's `ScanInternal`): **every**
    /// version of every point key (including tombstones and superseded versions), in
    /// internal-key order, plus the range tombstones and range keys. Unlike [`DbInner::iter`],
    /// nothing is collapsed or hidden — this exposes the internal keyspace as replication
    /// and disaggregated-storage tooling needs it.
    pub fn scan_internal(&self) -> Result<InternalScan> {
        let (mem, imm, version) = {
            let state = self.state.lock().unwrap();
            (
                Arc::clone(&state.mem),
                state.imm.clone(),
                state.vs.current.clone(),
            )
        };
        let mut sources: Vec<Box<dyn InternalIter>> = Vec::new();
        let mut range_dels = mem.range_tombstones();
        let mut range_keys = mem.range_keys();
        sources.push(Box::new(mem.scan()));
        for m in imm.iter().rev() {
            range_dels.extend(m.range_tombstones());
            range_keys.extend(m.range_keys());
            sources.push(Box::new(m.scan()));
        }
        for level in version.levels.iter() {
            for f in level {
                let reader = self.open_reader(f.file_num)?;
                range_dels.extend_from_slice(reader.range_tombstones());
                range_keys.extend_from_slice(reader.range_keys());
                sources.push(Box::new(reader.iter()?));
            }
        }
        let mut merge = merging_iter::MergingIter::new(sources, self.cmp.clone())?;
        let mut points = Vec::new();
        while merge.valid() {
            points.push((merge.key().to_vec(), merge.value().to_vec()));
            merge.advance()?;
        }
        Ok(InternalScan {
            points,
            range_dels,
            range_keys,
        })
    }

    /// Opens (or returns a cached) reader for the sstable with the given file number.
    fn open_reader(&self, file_num: u64) -> Result<Arc<Reader>> {
        if let Some(r) = self.cache.lock().unwrap().get(&file_num) {
            return Ok(Arc::clone(r));
        }
        let bytes = self.fs.read(&self.dir.join(filenames::table(file_num)))?;
        let reader = Arc::new(
            Reader::open_with_cache(bytes, self.cmp.clone(), file_num, self.block_cache.clone())?
                .with_blob_resolver(
                    Arc::clone(&self.blob_store) as Arc<dyn crate::sstable::BlobResolver>
                ),
        );
        let mut cache = self.cache.lock().unwrap();
        // Bound the number of open readers. Once at capacity, drop entries that are not
        // referenced elsewhere before inserting the new reader.
        while cache.len() >= self.max_open_files {
            let victim = cache
                .iter()
                .find(|(_, r)| Arc::strong_count(r) == 1)
                .map(|(&k, _)| k);
            match victim {
                Some(k) => {
                    cache.remove(&k);
                }
                None => break, // every cached reader is in use; allow temporary overflow
            }
        }
        cache.insert(file_num, Arc::clone(&reader));
        Ok(reader)
    }

    /// The user-key comparer.
    pub fn comparer(&self) -> &Arc<dyn Comparer> {
        &self.cmp
    }

    /// The number of live sstables at each level (`[0]` is L0). Useful for tests and
    /// metrics.
    pub fn level_file_counts(&self) -> [usize; NUM_LEVELS] {
        let state = self.state.lock().unwrap();
        std::array::from_fn(|i| state.vs.current.levels[i].len())
    }

    /// Estimates the number of bytes of sstable storage occupied by keys in the user-key
    /// range `[start, end)`. A file fully contained in the range contributes its whole
    /// size; a file that only partially overlaps contributes a size proportional to the
    /// fraction of its key range that intersects `[start, end)` (a coarse estimate, since
    /// it does not read the file's index). Mirrors Pebble's `EstimateDiskUsage`.
    pub fn estimate_disk_usage(&self, start: &[u8], end: &[u8]) -> u64 {
        let state = self.state.lock().unwrap();
        let cmp = self.cmp.as_ref();
        let mut total = 0u64;
        for level in state.vs.current.levels.iter() {
            for f in level {
                let fs = encoded_user_key(&f.smallest);
                let fl = encoded_user_key(&f.largest);
                // No overlap: file entirely before start or at/after end.
                if cmp.compare(fl, start) == std::cmp::Ordering::Less
                    || cmp.compare(fs, end) != std::cmp::Ordering::Less
                {
                    continue;
                }
                // Fully contained: [fs, fl] within [start, end).
                if cmp.compare(fs, start) != std::cmp::Ordering::Less
                    && cmp.compare(fl, end) == std::cmp::Ordering::Less
                {
                    total += f.size;
                } else {
                    // Partial overlap: estimate proportionally by the byte-prefix overlap
                    // of the key ranges (coarse; avoids reading the file index).
                    total += estimate_partial_overlap(fs, fl, start, end, f.size);
                }
            }
        }
        total
    }

    /// Returns a point-in-time [`Metrics`] snapshot of the LSM tree.
    pub fn metrics(&self) -> Metrics {
        let state = self.state.lock().unwrap();
        let level_files = std::array::from_fn(|i| state.vs.current.levels[i].len());
        let level_bytes =
            std::array::from_fn(|i| state.vs.current.levels[i].iter().map(|f| f.size).sum());
        let (block_cache_hits, block_cache_misses) = match &self.block_cache {
            Some(c) => (c.hits(), c.misses()),
            None => (0, 0),
        };
        let total_sstables = level_files.iter().sum();
        let total_sstable_bytes = level_bytes.iter().sum();
        // Read amplification: L0 sublevels (overlapping L0 files packed into non-overlapping
        // layers) plus one per non-empty deeper level.
        let l0_sublevels = l0_sublevel_count(&state.vs.current.levels[0], self.cmp.as_ref());
        let read_amplification = l0_sublevels + level_files[1..].iter().filter(|&&n| n > 0).count();
        // Write amplification: total bytes written / bytes flushed.
        let write_amplification = if state.flush_bytes == 0 {
            0.0
        } else {
            (state.flush_bytes + state.compaction_bytes) as f64 / state.flush_bytes as f64
        };
        Metrics {
            level_files,
            level_bytes,
            last_sequence: state.vs.last_sequence,
            flush_count: state.flush_count,
            compaction_count: state.compaction_count,
            block_cache_hits,
            block_cache_misses,
            total_sstables,
            total_sstable_bytes,
            imm_count: state.imm.len(),
            mem_table_bytes: u64::from(state.mem.size()),
            open_snapshots: self.snapshots.lock().unwrap().len(),
            obsolete_files_pending: self.delete_queue.lock().unwrap().items.len(),
            l0_sublevels,
            read_amplification,
            write_amplification,
        }
    }

    /// Aggregates per-table statistics across all live sstables by reading each table's
    /// properties block (Pebble collects these in the background to drive
    /// tombstone-density and similar compaction heuristics). Reads go through the table
    /// cache, so repeated calls are cheap.
    pub fn table_stats(&self) -> Result<TableStats> {
        let files: Vec<u64> = {
            let state = self.state.lock().unwrap();
            state
                .vs
                .current
                .levels
                .iter()
                .flat_map(|lvl| lvl.iter().map(|f| f.file_num))
                .collect()
        };
        let mut stats = TableStats::default();
        for file_num in files {
            let reader = self.open_reader(file_num)?;
            let p = reader.properties();
            stats.tables += 1;
            stats.num_entries += p.num_entries;
            stats.num_deletions += p.num_deletions;
            stats.num_range_deletions += p.num_range_deletions;
        }
        if let Some(l) = &self.listener {
            l.on_table_stats_loaded(stats.tables);
        }
        Ok(stats)
    }

    /// Validates every live sstable by reading it end to end (which verifies each data
    /// block's checksum), firing [`EventListener::on_table_validated`] per file. Returns the
    /// number of files that failed validation; `Ok(0)` means the LSM's sstables are intact.
    pub fn validate_sstables(&self) -> Result<usize> {
        let files: Vec<u64> = {
            let state = self.state.lock().unwrap();
            state
                .vs
                .current
                .levels
                .iter()
                .flat_map(|lvl| lvl.iter().map(|f| f.file_num))
                .collect()
        };
        let mut failures = 0;
        for file_num in files {
            let ok = self.validate_one_table(file_num);
            if !ok {
                failures += 1;
            }
            if let Some(l) = &self.listener {
                l.on_table_validated(file_num, ok);
            }
        }
        Ok(failures)
    }

    /// Reads `file_num`'s sstable from first to last entry; returns whether it did so without
    /// error (a checksum failure or corruption returns `false`).
    fn validate_one_table(&self, file_num: u64) -> bool {
        let scan = || -> Result<()> {
            let reader = self.open_reader(file_num)?;
            let mut it = reader.iter()?;
            it.first()?;
            while it.valid() {
                // Touching the value forces any out-of-line value to resolve too.
                let _ = it.value();
                it.next()?;
            }
            Ok(())
        };
        scan().is_ok()
    }

    /// Returns a human-readable description of the LSM tree's current shape: one section
    /// per non-empty level listing each live sstable's file number, size, and user-key
    /// bounds (Pebble's LSM view, useful for debugging and tooling).
    pub fn lsm_view(&self) -> String {
        use std::fmt::Write as _;
        let state = self.state.lock().unwrap();
        let esc = |b: &[u8]| -> String {
            b.iter()
                .flat_map(|&c| {
                    if (0x20..0x7f).contains(&c) {
                        vec![c as char]
                    } else {
                        format!("\\x{c:02x}").chars().collect()
                    }
                })
                .collect()
        };
        let mut out = String::new();
        for (lvl, files) in state.vs.current.levels.iter().enumerate() {
            if files.is_empty() {
                continue;
            }
            let bytes: u64 = files.iter().map(|f| f.size).sum();
            let _ = writeln!(out, "L{lvl}: {} files, {bytes} bytes", files.len());
            for f in files {
                let _ = writeln!(
                    out,
                    "  {:06}.sst  {} bytes  [{} .. {}]",
                    f.file_num,
                    f.size,
                    esc(encoded_user_key(&f.smallest)),
                    esc(encoded_user_key(&f.largest)),
                );
            }
        }
        if out.is_empty() {
            out.push_str("(empty)\n");
        }
        out
    }

    /// Captures a read snapshot at the current sequence number. Reads through the
    /// returned [`Snapshot`] see a consistent view even as later writes are applied, and
    /// compaction retains every version the snapshot can observe until it is dropped.
    pub fn snapshot(&self) -> Snapshot<'_> {
        let seqnum = self.last_sequence();
        self.snapshots.lock().unwrap().push(seqnum);
        Snapshot { db: self, seqnum }
    }

    /// Creates an [`EventuallyFileOnlySnapshot`] scoped to `spans` (Pebble's EFOS): a
    /// consistent read view restricted to the given `[start, end)` key ranges. The memtable
    /// is flushed first so the snapshot's data is immediately backed by sstables ("file-only")
    /// rather than pinning the memtable. Like a regular snapshot it pins its sequence number,
    /// so compaction retains the versions it needs until it is dropped.
    pub fn new_eventually_file_only_snapshot(
        &self,
        spans: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<EventuallyFileOnlySnapshot<'_>> {
        // Realize the "file-only" property by flushing in-memory data to sstables.
        if !self.state.lock().unwrap().read_only {
            self.flush()?;
        }
        let seqnum = self.last_sequence();
        self.snapshots.lock().unwrap().push(seqnum);
        Ok(EventuallyFileOnlySnapshot {
            db: self,
            seqnum,
            spans,
        })
    }

    /// The sorted sequence numbers of currently-open snapshots.
    fn open_snapshots(&self) -> Vec<SeqNum> {
        let mut s = self.snapshots.lock().unwrap().clone();
        s.sort_unstable();
        s.dedup();
        s
    }
}

/// A consistent read view of the database at a fixed sequence number.
pub struct Snapshot<'a> {
    db: &'a DbInner,
    seqnum: SeqNum,
}

impl Snapshot<'_> {
    /// The sequence number this snapshot reads at.
    pub fn sequence_number(&self) -> SeqNum {
        self.seqnum
    }

    /// Looks up `key` as of the snapshot.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db.get_at(key, self.seqnum)
    }

    /// Returns a forward iterator over the snapshot's view.
    pub fn iter(&self) -> Result<DbIterator> {
        self.db.iter_at(self.seqnum)
    }

    /// Returns an iterator over the snapshot's view with the given bounds/options.
    pub fn iter_with_options(&self, opts: IterOptions) -> Result<DbIterator> {
        self.db.iter_at_with_options(self.seqnum, opts)
    }
}

impl Drop for Snapshot<'_> {
    fn drop(&mut self) {
        let mut snaps = self.db.snapshots.lock().unwrap();
        if let Some(pos) = snaps.iter().position(|&s| s == self.seqnum) {
            snaps.swap_remove(pos);
        }
    }
}

/// A consistent read view restricted to a set of key spans (Pebble's
/// `EventuallyFileOnlySnapshot`). Reads outside the registered spans are rejected. Created by
/// [`DbInner::new_eventually_file_only_snapshot`]; pins its sequence number until dropped.
pub struct EventuallyFileOnlySnapshot<'a> {
    db: &'a DbInner,
    seqnum: SeqNum,
    spans: Vec<(Vec<u8>, Vec<u8>)>,
}

impl EventuallyFileOnlySnapshot<'_> {
    /// The sequence number this snapshot reads at.
    pub fn sequence_number(&self) -> SeqNum {
        self.seqnum
    }

    /// The registered key spans this snapshot is scoped to.
    pub fn spans(&self) -> &[(Vec<u8>, Vec<u8>)] {
        &self.spans
    }

    fn covers(&self, key: &[u8]) -> bool {
        let cmp = self.db.cmp.as_ref();
        self.spans.iter().any(|(s, e)| {
            cmp.compare(key, s) != std::cmp::Ordering::Less
                && cmp.compare(key, e) == std::cmp::Ordering::Less
        })
    }

    /// Looks up `key` as of the snapshot. Errors if `key` is outside the registered spans.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if !self.covers(key) {
            return Err(Error::InvalidState(
                "efos: key is outside the snapshot's registered spans".into(),
            ));
        }
        self.db.get_at(key, self.seqnum)
    }

    /// Returns an iterator over `[start, end)`, which must lie within a single registered span.
    pub fn iter_span(&self, start: &[u8], end: &[u8]) -> Result<DbIterator> {
        let cmp = self.db.cmp.as_ref();
        let within = self.spans.iter().any(|(s, e)| {
            cmp.compare(start, s) != std::cmp::Ordering::Less
                && cmp.compare(end, e) != std::cmp::Ordering::Greater
        });
        if !within {
            return Err(Error::InvalidState(
                "efos: iteration range is not within a registered span".into(),
            ));
        }
        self.db.iter_at_with_options(
            self.seqnum,
            IterOptions {
                lower_bound: Some(start.to_vec()),
                upper_bound: Some(end.to_vec()),
                ..Default::default()
            },
        )
    }
}

impl Drop for EventuallyFileOnlySnapshot<'_> {
    fn drop(&mut self) {
        let mut snaps = self.db.snapshots.lock().unwrap();
        if let Some(pos) = snaps.iter().position(|&s| s == self.seqnum) {
            snaps.swap_remove(pos);
        }
    }
}

/// The raw internal contents of the LSM, as returned by [`DbInner::scan_internal`].
#[derive(Clone, Debug, Default)]
pub struct InternalScan {
    /// Every point entry as `(encoded internal key, value)`, in internal-key order
    /// (ascending user key, then descending sequence number) — all versions, including
    /// tombstones.
    pub points: Vec<(Vec<u8>, Vec<u8>)>,
    /// All range tombstones across the LSM.
    pub range_dels: Vec<RangeTombstone>,
    /// All range-key entries across the LSM.
    pub range_keys: Vec<RangeKeyEntry>,
}

/// Aggregate per-table statistics across all live sstables, from [`DbInner::table_stats`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TableStats {
    /// Number of live sstables examined.
    pub tables: usize,
    /// Total key/value entries across all tables.
    pub num_entries: u64,
    /// Total point + range deletions across all tables.
    pub num_deletions: u64,
    /// Total range deletions across all tables.
    pub num_range_deletions: u64,
}

/// A point-in-time summary of the LSM tree's shape.
#[derive(Clone, Debug)]
pub struct Metrics {
    /// Number of sstables at each level (`[0]` is L0).
    pub level_files: [usize; NUM_LEVELS],
    /// Total sstable bytes at each level.
    pub level_bytes: [u64; NUM_LEVELS],
    /// The largest sequence number assigned so far.
    pub last_sequence: SeqNum,
    /// Number of memtable flushes performed this session.
    pub flush_count: u64,
    /// Number of compactions performed this session.
    pub compaction_count: u64,
    /// Number of block-cache hits so far (0 if caching is disabled).
    pub block_cache_hits: u64,
    /// Number of block-cache misses so far (0 if caching is disabled).
    pub block_cache_misses: u64,
    /// Total number of live sstables across all levels.
    pub total_sstables: usize,
    /// Total bytes of live sstables across all levels.
    pub total_sstable_bytes: u64,
    /// Number of immutable memtables awaiting flush.
    pub imm_count: usize,
    /// Current mutable-memtable size in bytes.
    pub mem_table_bytes: u64,
    /// Number of currently-open snapshots.
    pub open_snapshots: usize,
    /// Obsolete files queued for paced deletion but not yet deleted (0 unless deletion
    /// pacing is enabled).
    pub obsolete_files_pending: usize,
    /// Number of L0 sublevels — overlapping L0 files packed into layers of non-overlapping
    /// files (L0's contribution to read amplification).
    pub l0_sublevels: usize,
    /// Worst-case number of sstables a point read may consult: L0 sublevels plus one per
    /// non-empty deeper level (a read-amplification estimate).
    pub read_amplification: usize,
    /// Total bytes written across flushes and compactions divided by bytes flushed — the
    /// write-amplification factor (`1.0` when nothing has been compacted; `0.0` before any
    /// flush).
    pub write_amplification: f64,
}

/// Creates an iterator over a set of sstables **without** ingesting them into a database,
/// merging their entries into one sorted view (Pebble's `NewExternalIter`). The files are
/// read through `opts.fs` with `opts.comparer`; `merge` keys resolve via `opts.merger`.
///
/// The newest version of each user key wins (files later in `paths` are treated as newer),
/// and each file's range tombstones are applied. Useful for reading or validating
/// externally-produced sstables before deciding whether to ingest them.
pub fn new_external_iter(opts: &Options, paths: &[impl AsRef<Path>]) -> Result<DbIterator> {
    let cmp = opts.comparer.clone();
    let mut sources: Vec<Box<dyn InternalIter>> = Vec::new();
    let mut tombstones = Vec::new();
    let mut range_keys = Vec::new();
    for path in paths {
        let bytes = opts.fs.read(path.as_ref())?;
        let reader = Arc::new(Reader::open(bytes, cmp.clone())?);
        tombstones.extend_from_slice(reader.range_tombstones());
        range_keys.extend_from_slice(reader.range_keys());
        sources.push(Box::new(reader.iter()?));
    }
    DbIterator::with_options(
        sources,
        SeqNum::MAX,
        cmp,
        tombstones,
        range_keys,
        opts.merger.clone(),
        IterOptions::default(),
    )
}

/// Maps a key to a fraction in `[0, 1]` using its leading bytes as a big-endian fixed
/// point, for coarse range-overlap estimation.
fn key_fraction(k: &[u8]) -> f64 {
    let mut v = 0.0f64;
    let mut scale = 1.0f64 / 256.0;
    for &b in k.iter().take(8) {
        v += b as f64 * scale;
        scale /= 256.0;
    }
    v
}

/// Estimates the bytes of a file whose key range `[fs, fl]` only partially overlaps the
/// query range `[start, end)`, proportional to the overlapping fraction of its key span.
fn estimate_partial_overlap(fs: &[u8], fl: &[u8], start: &[u8], end: &[u8], size: u64) -> u64 {
    let f0 = key_fraction(fs);
    let f1 = key_fraction(fl);
    let span = (f1 - f0).max(f64::MIN_POSITIVE);
    let lo = f0.max(key_fraction(start));
    let hi = f1.min(key_fraction(end));
    let overlap = (hi - lo).clamp(0.0, span);
    (size as f64 * (overlap / span)).round() as u64
}

/// The filename of the WAL with the given number.
fn wal_filename(num: u64) -> String {
    format!("{num:06}.log")
}

/// Replays the *un-flushed* `*.log` files (those with number `>= vs.log_number`, in
/// increasing order) into `mem`, returning the version set with `last_sequence` advanced
/// past the recovered batches.
///
/// WALs numbered below the manifest's `log_number` hold data already captured in sstables;
/// replaying them would inject stale versions into the memtable that, being consulted
/// first, would wrongly shadow the newer on-disk data.
fn recover_wals(
    fs: &dyn Fs,
    wal_dirs: &[PathBuf],
    mem: &Arc<MemTable>,
    mut vs: VersionSet,
    tolerant: bool,
) -> Result<VersionSet> {
    let min_unflushed = vs.log_number;
    // Collect un-flushed WALs from every WAL directory, keyed by number. WAL numbers are
    // globally monotonic, so the same number never appears in two directories.
    let mut logs: Vec<(u64, PathBuf)> = Vec::new();
    for dir in wal_dirs {
        let names = match fs.list(dir) {
            Ok(n) => n,
            Err(_) => continue, // a configured failover dir may not exist yet
        };
        for n in names {
            if let Some(num) = n.strip_suffix(".log").and_then(|s| s.parse::<u64>().ok())
                && num >= min_unflushed
            {
                logs.push((num, dir.join(&n)));
            }
        }
    }
    logs.sort_by_key(|(num, _)| *num);

    let mut last_seq = vs.last_sequence;
    for (num, path) in logs {
        let bytes = fs.read(&path)?;
        // With recycling, every WAL file may carry a stale tail from a previous use; read
        // tolerantly so recovery stops cleanly at the last complete record (the recyclable
        // record format's per-record log number marks where the stale tail begins).
        let mut reader = record::Reader::new(std::io::Cursor::new(bytes), num as u32);
        if tolerant {
            reader = reader.tolerant();
        }
        while let Some(rec) = reader.read_record()? {
            let batch = Batch::from_bytes(rec)?;
            if batch.is_empty() {
                continue;
            }
            mem.apply(&batch)?;
            last_seq = last_seq.max(batch.seqnum() + u64::from(batch.count()) - 1);
        }
    }
    vs.last_sequence = last_seq;
    Ok(vs)
}

/// Resolves the comparer to open a store with, given the comparer name recorded in its
/// OPTIONS file (if any). Prefers [`Options::comparer`] when its name matches (or nothing is
/// recorded), then searches the [`Options::comparers`] registry. Fails if the recorded
/// comparer is neither configured nor registered.
fn resolve_comparer(opts: &Options, recorded: Option<&str>) -> Result<Arc<dyn Comparer>> {
    let Some(name) = recorded.filter(|n| !n.is_empty()) else {
        return Ok(opts.comparer.clone());
    };
    if opts.comparer.name() == name {
        return Ok(opts.comparer.clone());
    }
    if let Some(c) = opts.comparers.iter().find(|c| c.name() == name) {
        return Ok(c.clone());
    }
    Err(Error::InvalidState(format!(
        "db: store recorded comparer {name:?} but it is neither Options::comparer ({:?}) nor in \
         Options::comparers; register it to open this store",
        opts.comparer.name()
    )))
}

/// Resolves the merge operator to open a store with, given the merger name recorded in its
/// OPTIONS file (if any). Prefers [`Options::merger`] when its name matches, then searches
/// the [`Options::mergers`] registry. Falls back to [`Options::merger`] (possibly `None`)
/// when the recorded merger is unknown — matching the engine's lenient handling of a merge
/// without an operator (it resolves to the newest operand).
fn resolve_merger(
    opts: &Options,
    recorded: Option<&str>,
) -> Option<Arc<dyn crate::base::merge::Merger>> {
    if let Some(name) = recorded.filter(|n| !n.is_empty()) {
        if let Some(m) = &opts.merger
            && m.name() == name
        {
            return Some(m.clone());
        }
        if let Some(m) = opts.mergers.iter().find(|m| m.name() == name) {
            return Some(m.clone());
        }
    }
    opts.merger.clone()
}

/// Creates a WAL file numbered `wal_number`, trying `wal_dirs[start_idx..]` in order until
/// one accepts it (failover). Returns the writer and the index of the directory used.
fn create_wal(
    fs: &dyn Fs,
    wal_dirs: &[PathBuf],
    start_idx: usize,
    wal_number: u64,
) -> Result<(record::Writer<Box<dyn WritableFile>>, usize)> {
    let mut last_err = None;
    for (i, dir) in wal_dirs.iter().enumerate().skip(start_idx) {
        match fs.create(&dir.join(wal_filename(wal_number))) {
            Ok(file) => {
                return Ok((record::Writer::with_log_num(file, wal_number as u32), i));
            }
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err
        .map(Error::from)
        .unwrap_or_else(|| Error::InvalidState("db: no WAL directory available".into())))
}

/// Writes every entry of `mem` (in internal-key order) to one or more sstables under `dir`,
/// returning their metadata. When the memtable holds only point keys (no range tombstones or
/// range keys), the point output is **split** into multiple files at `target_file_size`
/// boundaries (Pebble's flush splitting), using `file_nums` in order; range tombstones / range
/// keys force a single output file (fragmenting them across splits is not done here).
#[allow(clippy::too_many_arguments)]
/// Reproduces a group-commit result for each waiter. `Error` is not `Clone`, so an error is
/// re-expressed (preserving its message) — every batch in a failed group sees the failure.
fn clone_commit_result(r: &Result<()>) -> Result<()> {
    match r {
        Ok(()) => Ok(()),
        Err(e) => Err(Error::InvalidState(format!("group commit failed: {e}"))),
    }
}

/// Computes the number of **L0 sublevels** (Pebble's read-amplification measure for L0):
/// L0 files can overlap, so they are greedily packed — newest first — into the fewest layers
/// of mutually non-overlapping files. Within one sublevel a read touches at most one file, so
/// the sublevel count is L0's contribution to read amplification.
fn l0_sublevel_count(files: &[Arc<FileMetadata>], cmp: &dyn Comparer) -> usize {
    // Process newest-first so older versions settle into deeper sublevels.
    let mut ordered: Vec<&Arc<FileMetadata>> = files.iter().collect();
    ordered.sort_by_key(|f| std::cmp::Reverse(f.largest_seqnum));
    // Each sublevel records the [smallest, largest] user-key spans it already holds.
    let mut sublevels: Vec<Vec<(Vec<u8>, Vec<u8>)>> = Vec::new();
    for f in ordered {
        let fs = encoded_user_key(&f.smallest).to_vec();
        let fl = encoded_user_key(&f.largest).to_vec();
        // Place in the first sublevel with no range overlap; else start a new one.
        let mut placed = false;
        for sub in &mut sublevels {
            let overlaps = sub.iter().any(|(s, l)| {
                cmp.compare(&fs, l) != std::cmp::Ordering::Greater
                    && cmp.compare(s, &fl) != std::cmp::Ordering::Greater
            });
            if !overlaps {
                sub.push((fs.clone(), fl.clone()));
                placed = true;
                break;
            }
        }
        if !placed {
            sublevels.push(vec![(fs, fl)]);
        }
    }
    sublevels.len()
}

/// Builds the sstable [`WriterOptions`] the engine uses, enabling value-block separation in a
/// value-block-capable format when `value_block_threshold` is set.
fn engine_writer_options(
    value_block_threshold: Option<usize>,
    blob_value_threshold: Option<usize>,
    blob_file_num: u64,
) -> WriterOptions {
    let mut o = WriterOptions::default();
    if value_block_threshold.is_some() || blob_value_threshold.is_some() {
        o.table_format = crate::sstable::TableFormat::Pebble(3);
        o.value_block_threshold = value_block_threshold;
        o.blob_value_threshold = blob_value_threshold;
        o.blob_file_num = Some(blob_file_num);
    }
    o
}

/// Writes the blob file holding sstable `file_num`'s out-of-line values, if any were
/// separated. A no-op when `bytes` is `None`.
fn write_blob_file(fs: &dyn Fs, dir: &Path, file_num: u64, bytes: Option<&[u8]>) -> Result<()> {
    if let Some(b) = bytes {
        let mut bf = fs.create(&dir.join(filenames::blob(file_num)))?;
        bf.write_all(b)?;
        bf.sync_all()?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_memtable_to_sstables(
    fs: &dyn Fs,
    dir: &Path,
    cmp: &Arc<dyn Comparer>,
    file_nums: &[u64],
    mem: &Arc<MemTable>,
    collectors: &[BlockPropertyCollectorFactory],
    target_file_size: u64,
    value_block_threshold: Option<usize>,
    blob_value_threshold: Option<usize>,
) -> Result<Vec<FileMetadata>> {
    let has_spans = !mem.range_tombstones().is_empty() || !mem.range_keys().is_empty();
    let mut outputs: Vec<FileMetadata> = Vec::new();
    let mut nfi = 0usize;

    type StartedWriter = (u64, PathBuf, Writer<Box<dyn WritableFile>>);
    let start_writer = |nfi: &mut usize| -> Result<StartedWriter> {
        let file_num = *file_nums.get(*nfi).ok_or_else(|| {
            Error::InvalidState("flush: ran out of preallocated file numbers".into())
        })?;
        *nfi += 1;
        let path = dir.join(filenames::table(file_num));
        let mut w = Writer::new(
            fs.create(&path)?,
            cmp.clone(),
            engine_writer_options(value_block_threshold, blob_value_threshold, file_num),
        );
        for factory in collectors {
            w.add_block_property_collector(factory());
        }
        Ok((file_num, path, w))
    };

    let (mut file_num, mut path, mut w) = start_writer(&mut nfi)?;
    let mut smallest: Option<Vec<u8>> = None;
    let mut largest: Vec<u8> = Vec::new();
    let mut smallest_seq = u64::MAX;
    let mut largest_seq = 0u64;

    let mut it = mem.iter();
    it.first();
    let mut key_buf = Vec::new();
    while it.valid() {
        key_buf.clear();
        key_buf.extend_from_slice(it.user_key());
        key_buf.extend_from_slice(&it.trailer().to_le_bytes());
        w.add(&key_buf, it.value())?;
        if smallest.is_none() {
            smallest = Some(key_buf.clone());
        }
        largest.clear();
        largest.extend_from_slice(&key_buf);
        let seq = it.trailer() >> 8;
        smallest_seq = smallest_seq.min(seq);
        largest_seq = largest_seq.max(seq);
        let written_user = encoded_user_key(&key_buf).to_vec();
        it.next();

        // Split on a user-key boundary once the target size is reached, provided this is a
        // point-only memtable and more preallocated file numbers remain. Never split between
        // two internal versions of the same user key.
        if !has_spans
            && nfi < file_nums.len()
            && it.valid()
            && it.user_key() != written_user.as_slice()
            && w.estimated_size() >= target_file_size
        {
            let blob_bytes = w.take_blob_file()?;
            let blob_refs = w.blob_refs().to_vec();
            let mut f = w.finish()?;
            f.sync_all()?;
            write_blob_file(fs, dir, file_num, blob_bytes.as_deref())?;
            outputs.push(FileMetadata {
                file_num,
                size: fs.size(&path)?,
                smallest: smallest.take().unwrap_or_default(),
                largest: std::mem::take(&mut largest),
                smallest_seqnum: smallest_seq.min(largest_seq),
                largest_seqnum: largest_seq,
                blob_refs,
                backing: None,
            });
            let next = start_writer(&mut nfi)?;
            file_num = next.0;
            path = next.1;
            w = next.2;
            smallest_seq = u64::MAX;
            largest_seq = 0;
        }
    }

    // Write the memtable's range tombstones, in start-key order, into the range-del
    // block. Each contributes its bounds to the file's key range.
    let mut tombstones = mem.range_tombstones();
    tombstones.sort_by(|a, b| {
        cmp.compare(&a.start, &b.start)
            .then(b.seqnum.cmp(&a.seqnum))
    });
    for t in &tombstones {
        let start_ikey =
            InternalKey::new(t.start.clone(), t.seqnum, InternalKeyKind::RangeDelete).encode();
        w.add(&start_ikey, &t.end)?;
        if smallest.is_none()
            || cmp.compare(&t.start, encoded_user_key(smallest.as_ref().unwrap()))
                == std::cmp::Ordering::Less
        {
            smallest = Some(start_ikey.clone());
        }
        // The tombstone's exclusive end extends the largest user-key bound.
        if largest.is_empty()
            || cmp.compare(&t.end, encoded_user_key(&largest)) == std::cmp::Ordering::Greater
        {
            largest =
                InternalKey::new(t.end.clone(), t.seqnum, InternalKeyKind::RangeDelete).encode();
        }
        smallest_seq = smallest_seq.min(t.seqnum);
        largest_seq = largest_seq.max(t.seqnum);
    }

    // Write the memtable's range keys, in internal-key order, into the range-key block.
    let mut range_keys = mem.range_keys();
    range_keys.sort_by(|a, b| {
        cmp.compare(&a.start, &b.start)
            .then(b.seqnum.cmp(&a.seqnum))
            .then(b.kind.as_u8().cmp(&a.kind.as_u8()))
    });
    for rk in &range_keys {
        let start_ikey = InternalKey::new(rk.start.clone(), rk.seqnum, rk.kind).encode();
        w.add(&start_ikey, &rk.value)?;
        if smallest.is_none()
            || cmp.compare(&rk.start, encoded_user_key(smallest.as_ref().unwrap()))
                == std::cmp::Ordering::Less
        {
            smallest = Some(start_ikey.clone());
        }
        if let Ok(end) = rk.end()
            && (largest.is_empty()
                || cmp.compare(&end, encoded_user_key(&largest)) == std::cmp::Ordering::Greater)
        {
            largest = InternalKey::new(end, rk.seqnum, rk.kind).encode();
        }
        smallest_seq = smallest_seq.min(rk.seqnum);
        largest_seq = largest_seq.max(rk.seqnum);
    }

    let blob_bytes = w.take_blob_file()?;
    let blob_refs = w.blob_refs().to_vec();
    let mut file = w.finish()?;
    file.sync_all()?;
    write_blob_file(fs, dir, file_num, blob_bytes.as_deref())?;

    outputs.push(FileMetadata {
        file_num,
        size: fs.size(&path)?,
        smallest: smallest.unwrap_or_default(),
        largest,
        smallest_seqnum: smallest_seq.min(largest_seq),
        largest_seqnum: largest_seq,
        blob_refs,
        backing: None,
    });
    Ok(outputs)
}

/// Writes a new `manifest` marker pointing at `value`, with an `iter` one greater than
/// any existing marker, and removes superseded marker files.
fn update_marker(fs: &dyn Fs, dir: &Path, names: &[String], value: &str) -> Result<()> {
    let mut max_iter = 0u64;
    let mut old: Vec<&String> = Vec::new();
    for n in names {
        if let Some(rest) = n.strip_prefix("marker.manifest.") {
            old.push(n);
            if let Some((iter_str, _)) = rest.split_once('.')
                && let Ok(iter) = iter_str.parse::<u64>()
            {
                max_iter = max_iter.max(iter);
            }
        }
    }
    let new_name = format!("marker.manifest.{:06}.{}", max_iter + 1, value);
    fs.create(&dir.join(&new_name))?.sync_all()?;
    for n in old {
        let _ = fs.remove(&dir.join(n));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_dir() -> PathBuf {
        let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir().join(format!("pebbledb-dbw-{}-{}", std::process::id(), n));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn collect(db: &Db) -> Vec<(String, String)> {
        let mut it = db.iter().unwrap();
        let mut out = Vec::new();
        it.first().unwrap();
        while it.valid() {
            out.push((
                String::from_utf8(it.key().to_vec()).unwrap(),
                String::from_utf8(it.value().to_vec()).unwrap(),
            ));
            it.next().unwrap();
        }
        out
    }

    #[test]
    fn create_set_get_delete() {
        let dir = temp_dir();
        let db = Db::open(&dir, Options::default()).unwrap();
        db.set(b"a", b"1").unwrap();
        db.set(b"b", b"2").unwrap();
        db.set(b"c", b"3").unwrap();
        assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
        db.delete(b"b").unwrap();
        assert_eq!(db.get(b"b").unwrap(), None);
        db.set(b"a", b"1-new").unwrap();
        assert_eq!(db.get(b"a").unwrap(), Some(b"1-new".to_vec()));
        assert_eq!(
            collect(&db),
            vec![
                ("a".to_string(), "1-new".to_string()),
                ("c".to_string(), "3".to_string())
            ]
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn flush_then_read_from_sstable() {
        let dir = temp_dir();
        let db = Db::open(&dir, Options::default()).unwrap();
        for i in 0..50u32 {
            db.set(
                format!("key{i:03}").as_bytes(),
                format!("val{i}").as_bytes(),
            )
            .unwrap();
        }
        db.flush().unwrap(); // memtable -> L0 sstable
        // After flush the memtable is empty; reads come from the sstable.
        assert_eq!(db.get(b"key000").unwrap(), Some(b"val0".to_vec()));
        assert_eq!(db.get(b"key049").unwrap(), Some(b"val49".to_vec()));
        // Write more after flush; live in the new memtable.
        db.set(b"key049", b"updated").unwrap();
        assert_eq!(db.get(b"key049").unwrap(), Some(b"updated".to_vec()));
        assert_eq!(collect(&db).len(), 50);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn reopen_recovers_from_wal() {
        let dir = temp_dir();
        {
            let db = Db::open(&dir, Options::default()).unwrap();
            db.set(b"persisted", b"yes").unwrap();
            db.set(b"x", b"1").unwrap();
            db.delete(b"x").unwrap();
            // No explicit flush: data lives only in the WAL + memtable.
        }
        // Reopen: the WAL is replayed into a fresh memtable.
        let db = Db::open(&dir, Options::default()).unwrap();
        assert_eq!(db.get(b"persisted").unwrap(), Some(b"yes".to_vec()));
        assert_eq!(db.get(b"x").unwrap(), None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn reopen_recovers_from_flushed_sstable() {
        let dir = temp_dir();
        {
            let db = Db::open(&dir, Options::default()).unwrap();
            db.set(b"flushed", b"data").unwrap();
            db.flush().unwrap();
            db.set(b"afterflush", b"more").unwrap();
        }
        let db = Db::open(&dir, Options::default()).unwrap();
        assert_eq!(db.get(b"flushed").unwrap(), Some(b"data".to_vec()));
        assert_eq!(db.get(b"afterflush").unwrap(), Some(b"more".to_vec()));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn auto_flush_on_full_memtable() {
        let dir = temp_dir();
        // Tiny memtable forces several automatic flushes.
        let opts = Options {
            mem_table_size: 16 * 1024,
            ..Default::default()
        };
        let db = Db::open(&dir, opts).unwrap();
        for i in 0..2000u32 {
            db.set(format!("k{i:06}").as_bytes(), format!("v{i:06}").as_bytes())
                .unwrap();
        }
        // Spot-check values spanning multiple flushed sstables and the live memtable.
        assert_eq!(db.get(b"k000000").unwrap(), Some(b"v000000".to_vec()));
        assert_eq!(db.get(b"k001000").unwrap(), Some(b"v001000".to_vec()));
        assert_eq!(db.get(b"k001999").unwrap(), Some(b"v001999".to_vec()));
        assert_eq!(db.get(b"k002000").unwrap(), None);
        assert_eq!(collect(&db).len(), 2000);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn compaction_drains_l0_and_preserves_reads() {
        let dir = temp_dir();
        let opts = Options {
            mem_table_size: 16 * 1024,
            ..Default::default()
        };
        let db = Db::open(&dir, opts).unwrap();
        // Many distinct keys across many flushes; L0 should be compacted into deeper
        // levels rather than growing without bound.
        for i in 0..3000u32 {
            db.set(format!("k{i:06}").as_bytes(), format!("v{i:06}").as_bytes())
                .unwrap();
        }
        let counts = db.level_file_counts();
        assert!(counts[0] < 4, "L0 should be drained, got {counts:?}");
        assert!(
            counts[1..].iter().sum::<usize>() > 0,
            "deeper levels should hold files, got {counts:?}"
        );
        // All reads remain correct after compaction.
        for i in (0..3000u32).step_by(137) {
            assert_eq!(
                db.get(format!("k{i:06}").as_bytes()).unwrap(),
                Some(format!("v{i:06}").into_bytes())
            );
        }
        assert_eq!(collect(&db).len(), 3000);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn compaction_collapses_overwrites_and_deletes() {
        let dir = temp_dir();
        let opts = Options {
            mem_table_size: 16 * 1024,
            ..Default::default()
        };
        let db = Db::open(&dir, opts).unwrap();
        // Repeatedly overwrite a small key set, then delete half — forcing flushes and
        // compactions that must collapse history correctly.
        for round in 0..200u32 {
            for k in 0..20u32 {
                db.set(
                    format!("key{k:02}").as_bytes(),
                    format!("round{round}").as_bytes(),
                )
                .unwrap();
            }
        }
        for k in 0..10u32 {
            db.delete(format!("key{k:02}").as_bytes()).unwrap();
        }
        db.flush().unwrap();

        // Deleted keys are gone; survivors hold the final round's value.
        for k in 0..10u32 {
            assert_eq!(db.get(format!("key{k:02}").as_bytes()).unwrap(), None);
        }
        for k in 10..20u32 {
            assert_eq!(
                db.get(format!("key{k:02}").as_bytes()).unwrap(),
                Some(b"round199".to_vec())
            );
        }
        assert_eq!(collect(&db).len(), 10);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn range_deletions_in_memtable() {
        let dir = temp_dir();
        let db = Db::open(&dir, Options::default()).unwrap();
        for i in 0..20u32 {
            db.set(format!("k{i:02}").as_bytes(), format!("v{i}").as_bytes())
                .unwrap();
        }
        // Delete the range [k05, k15).
        db.delete_range(b"k05", b"k15").unwrap();
        for i in 0..20u32 {
            let key = format!("k{i:02}");
            let got = db.get(key.as_bytes()).unwrap();
            if (5..15).contains(&i) {
                assert_eq!(got, None, "{key} should be range-deleted");
            } else {
                assert_eq!(got, Some(format!("v{i}").into_bytes()), "{key}");
            }
        }
        // A point write after the range delete resurrects that key.
        db.set(b"k07", b"new7").unwrap();
        assert_eq!(db.get(b"k07").unwrap(), Some(b"new7".to_vec()));
        // Iteration also hides range-deleted keys.
        let live: Vec<_> = collect(&db).into_iter().map(|(k, _)| k).collect();
        assert!(!live.contains(&"k06".to_string()));
        assert!(live.contains(&"k07".to_string()));
        assert!(live.contains(&"k15".to_string()));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn range_deletions_survive_flush_and_compaction() {
        let dir = temp_dir();
        let opts = Options {
            mem_table_size: 16 * 1024,
            ..Default::default()
        };
        let db = Db::open(&dir, opts).unwrap();
        // Write keys, delete a wide range, then write enough to force flushes and a
        // compaction so the range tombstone must persist through both.
        for i in 0..400u32 {
            db.set(format!("key{i:05}").as_bytes(), b"v").unwrap();
        }
        db.delete_range(b"key00100", b"key00300").unwrap();
        for i in 400..2000u32 {
            db.set(format!("key{i:05}").as_bytes(), b"v").unwrap();
        }
        db.flush().unwrap();

        // Reopen to ensure the tombstone persisted to disk and is reapplied.
        drop(db);
        let db = Db::open(&dir, Options::default()).unwrap();
        assert_eq!(db.get(b"key00050").unwrap(), Some(b"v".to_vec()));
        assert_eq!(db.get(b"key00100").unwrap(), None);
        assert_eq!(db.get(b"key00200").unwrap(), None);
        assert_eq!(db.get(b"key00299").unwrap(), None);
        assert_eq!(db.get(b"key00300").unwrap(), Some(b"v".to_vec()));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn range_keys_set_and_query() {
        use crate::base::internal_key::InternalKeyKind;
        let dir = temp_dir();
        let db = Db::open(&dir, Options::default()).unwrap();
        db.range_key_set(b"b", b"f", b"@10", b"hello").unwrap();
        db.range_key_set(b"d", b"h", b"@20", b"world").unwrap();

        // "e" is covered by both spans [b,f) and [d,h).
        let rks = db.range_keys_covering(b"e").unwrap();
        assert_eq!(rks.len(), 2);
        assert!(rks.iter().all(|r| r.kind == InternalKeyKind::RangeKeySet));
        // "a" is covered by neither.
        assert!(db.range_keys_covering(b"a").unwrap().is_empty());
        // "c" only by [b,f).
        assert_eq!(db.range_keys_covering(b"c").unwrap().len(), 1);

        // Range keys survive a flush + reopen.
        db.flush().unwrap();
        drop(db);
        let db = Db::open(&dir, Options::default()).unwrap();
        assert_eq!(db.range_keys_covering(b"e").unwrap().len(), 2);
        let rk = &db.range_keys_covering(b"c").unwrap()[0];
        assert_eq!(rk.end().unwrap(), b"f");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn range_key_delete_clears() {
        let dir = temp_dir();
        let db = Db::open(&dir, Options::default()).unwrap();
        db.range_key_set(b"a", b"z", b"@1", b"v").unwrap();
        assert_eq!(db.range_keys_covering(b"m").unwrap().len(), 1);
        // A range-key delete is recorded as a covering entry (newest first).
        db.range_key_delete(b"a", b"z").unwrap();
        let rks = db.range_keys_covering(b"m").unwrap();
        assert_eq!(rks.len(), 2);
        assert_eq!(
            rks[0].kind,
            crate::base::internal_key::InternalKeyKind::RangeKeyDelete
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn open_snapshot_survives_compaction() {
        let dir = temp_dir();
        let opts = Options {
            mem_table_size: 16 * 1024,
            ..Default::default()
        };
        let db = Db::open(&dir, opts).unwrap();
        // Establish an initial value, then take a snapshot pinning it.
        db.set(b"pinned", b"v1").unwrap();
        let snap = db.snapshot();
        // Overwrite the key many times and churn enough to force flushes + compaction.
        db.set(b"pinned", b"v2").unwrap();
        for i in 0..3000u32 {
            db.set(format!("k{i:06}").as_bytes(), b"x").unwrap();
        }
        db.flush().unwrap();

        // The snapshot still observes the pinned version despite compaction.
        assert_eq!(snap.get(b"pinned").unwrap(), Some(b"v1".to_vec()));
        // The live database sees the latest.
        assert_eq!(db.get(b"pinned").unwrap(), Some(b"v2".to_vec()));

        // After the snapshot is dropped, the old version may be collapsed; the latest
        // remains correct regardless.
        drop(snap);
        db.set(b"trigger", b"z").unwrap();
        db.flush().unwrap();
        assert_eq!(db.get(b"pinned").unwrap(), Some(b"v2".to_vec()));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn metrics_and_event_listener() {
        use std::sync::atomic::{AtomicU64, Ordering};
        struct Counter {
            flushes: AtomicU64,
            compactions: AtomicU64,
        }
        impl EventListener for Counter {
            fn on_flush_end(&self, _file_num: u64, bytes: u64) {
                assert!(bytes > 0);
                self.flushes.fetch_add(1, Ordering::Relaxed);
            }
            fn on_compaction_end(&self, _lvl: usize, inputs: usize, _outputs: usize) {
                assert!(inputs > 0);
                self.compactions.fetch_add(1, Ordering::Relaxed);
            }
        }
        let counter = Arc::new(Counter {
            flushes: AtomicU64::new(0),
            compactions: AtomicU64::new(0),
        });
        let dir = temp_dir();
        let opts = Options {
            mem_table_size: 16 * 1024,
            event_listener: Some(counter.clone()),
            ..Default::default()
        };
        let db = Db::open(&dir, opts).unwrap();
        for i in 0..3000u32 {
            db.set(format!("k{i:06}").as_bytes(), b"v").unwrap();
        }
        db.flush().unwrap();

        let m = db.metrics();
        assert!(m.flush_count >= 1, "flushes: {}", m.flush_count);
        assert!(
            m.compaction_count >= 1,
            "compactions: {}",
            m.compaction_count
        );
        assert_eq!(m.flush_count, counter.flushes.load(Ordering::Relaxed));
        assert_eq!(
            m.compaction_count,
            counter.compactions.load(Ordering::Relaxed)
        );
        assert!(m.level_bytes.iter().sum::<u64>() > 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn lifecycle_event_listener_fires() {
        use std::sync::atomic::{AtomicU64, Ordering};
        #[derive(Default)]
        struct Counter {
            wal_created: AtomicU64,
            wal_deleted: AtomicU64,
            manifest_created: AtomicU64,
            format_upgrade: AtomicU64,
        }
        impl EventListener for Counter {
            fn on_wal_created(&self, _n: u64) {
                self.wal_created.fetch_add(1, Ordering::Relaxed);
            }
            fn on_wal_deleted(&self, _n: u64) {
                self.wal_deleted.fetch_add(1, Ordering::Relaxed);
            }
            fn on_manifest_created(&self, _n: u64) {
                self.manifest_created.fetch_add(1, Ordering::Relaxed);
            }
            fn on_format_upgrade(&self, _v: u32) {
                self.format_upgrade.fetch_add(1, Ordering::Relaxed);
            }
        }
        let counter = Arc::new(Counter::default());
        let dir = temp_dir();
        let opts = Options {
            // Start below the newest version so there is at least one upgrade step to take.
            format_major_version: FormatMajorVersion::MOST_COMPATIBLE,
            event_listener: Some(counter.clone()),
            ..Default::default()
        };
        let db = Db::open(&dir, opts).unwrap();
        // Open created the session MANIFEST and the first WAL.
        assert_eq!(counter.manifest_created.load(Ordering::Relaxed), 1);
        assert_eq!(counter.wal_created.load(Ordering::Relaxed), 1);

        // A flush rotates in a fresh WAL (created) and retires the old one (deleted).
        db.set(b"k", b"v").unwrap();
        db.flush().unwrap();
        assert!(counter.wal_created.load(Ordering::Relaxed) >= 2);
        assert!(counter.wal_deleted.load(Ordering::Relaxed) >= 1);

        // Ratcheting the format major version fires one upgrade event per step.
        db.ratchet_format_major_version(FormatMajorVersion::NEWEST)
            .unwrap();
        assert!(counter.format_upgrade.load(Ordering::Relaxed) >= 1);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn merge_operator_resolves_across_levels() {
        use crate::base::merge::ConcatMerger;
        let dir = temp_dir();
        let opts = Options {
            mem_table_size: 16 * 1024,
            merger: Some(Arc::new(ConcatMerger)),
            ..Default::default()
        };
        let db = Db::open(&dir, opts.clone()).unwrap();

        // Base value then several merge operands.
        db.set(b"k", b"A").unwrap();
        db.merge(b"k", b"B").unwrap();
        db.merge(b"k", b"C").unwrap();
        assert_eq!(db.get(b"k").unwrap(), Some(b"ABC".to_vec()));

        // A key with only merges (no base).
        db.merge(b"only", b"x").unwrap();
        db.merge(b"only", b"y").unwrap();
        assert_eq!(db.get(b"only").unwrap(), Some(b"xy".to_vec()));

        // Iteration resolves merges too.
        let got = collect(&db);
        assert!(got.contains(&("k".to_string(), "ABC".to_string())));
        assert!(got.contains(&("only".to_string(), "xy".to_string())));

        // Force flushes + compaction with more operands, then reopen.
        for i in 0..3000u32 {
            db.set(format!("f{i:06}").as_bytes(), b"v").unwrap();
        }
        db.merge(b"k", b"D").unwrap();
        db.flush().unwrap();
        drop(db);
        let db = Db::open(&dir, opts).unwrap();
        assert_eq!(db.get(b"k").unwrap(), Some(b"ABCD".to_vec()));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn read_only_rejects_writes() {
        let dir = temp_dir();
        {
            let db = Db::open(&dir, Options::default()).unwrap();
            db.set(b"k", b"v").unwrap();
            db.flush().unwrap();
        }
        let db = Db::open_read_only(&dir, Options::default()).unwrap();
        assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
        assert!(db.set(b"x", b"y").is_err());
        std::fs::remove_dir_all(&dir).ok();
    }
}
