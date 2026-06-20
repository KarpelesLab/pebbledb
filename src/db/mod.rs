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
mod merging_iter;
mod options_file;

pub use indexed_batch::IndexedBatch;
use merging_iter::InternalIter;
pub use merging_iter::{DbIterator, IterOptions};
pub use options_file::{FormatMajorVersion, OptionsFile};

use std::collections::HashMap;
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
        }
    }
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
}

/// The shared inner state of a [`Db`], held behind an `Arc` so the background flush worker
/// can operate on it concurrently with foreground reads and writes.
pub struct DbInner {
    dir: PathBuf,
    cmp: Arc<dyn Comparer>,
    mem_table_size: usize,
    wal_sync: bool,
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

    /// Disposes of the obsolete file at `path` via the configured [`Cleaner`].
    fn clean_file(&self, path: &Path) {
        let _ = self.cleaner.clean(self.fs.as_ref(), path);
    }
}

/// An on-disk LSM key-value database.
///
/// Owns the shared [`DbInner`] and the background flush/compaction worker thread, which is
/// signaled to stop and joined when the `Db` is dropped.
pub struct Db {
    inner: Arc<DbInner>,
    worker: Option<std::thread::JoinHandle<()>>,
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
        if let Some(h) = self.worker.take() {
            let _ = h.join();
        }
    }
}

impl Db {
    /// Opens the database in `dir`, creating it if `opts.create_if_missing` and absent.
    pub fn open(dir: impl AsRef<Path>, opts: Options) -> Result<Db> {
        let inner = Arc::new(DbInner::open_inner(dir, opts)?);
        // Spawn the background flush/compaction worker for writable databases.
        let worker = if inner.state.lock().unwrap().read_only {
            None
        } else {
            let w = Arc::clone(&inner);
            Some(std::thread::spawn(move || w.background_loop()))
        };
        Ok(Db { inner, worker })
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
        let fs = opts.fs.clone();

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
                // Recover the un-flushed WALs across every WAL directory.
                recover_wals(fs.as_ref(), &wal_dirs, &mem, vs)?
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

        if opts.read_only {
            let state = State {
                vs,
                mem,
                imm: Vec::new(),
                imm_wals: Vec::new(),
                wal: None,
                wal_number: 0,
                wal_dir_idx: 0,
                manifest: None,
                read_only: true,
                shutdown: false,
                flush_count: 0,
                compaction_count: 0,
            };
            return Ok(DbInner {
                dir,
                cmp,
                wal_dirs,
                mem_table_size: opts.mem_table_size,
                wal_sync: opts.wal_sync,
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
            let file_num = vs.allocate_file_number();
            let meta = write_memtable_to_sstable(
                fs.as_ref(),
                &dir,
                &cmp,
                file_num,
                &mem,
                &opts.block_property_collectors,
            )?;
            let edit = VersionEdit {
                next_file_number: Some(vs.next_file_number),
                last_sequence: Some(vs.last_sequence),
                new_files: vec![NewFileEntry { level: 0, meta }],
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
            wal: Some(wal),
            wal_number,
            wal_dir_idx,
            manifest: Some(manifest),
            read_only: false,
            shutdown: false,
            flush_count: 0,
            compaction_count: 0,
        };
        Ok(DbInner {
            dir,
            cmp,
            wal_dirs,
            mem_table_size: opts.mem_table_size,
            wal_sync: opts.wal_sync,
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
    pub fn apply(&self, mut batch: Batch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let mut state = self.state.lock().unwrap();
        if state.read_only {
            return Err(Error::InvalidState("db: opened read-only".into()));
        }

        // Write stall: if too many immutable memtables are awaiting flush, block until the
        // background worker drains the queue below the threshold. Bounds memory when writes
        // outrun flushing.
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

        // Rotate once the memtable has used half its arena, leaving the rest as headroom
        // for this batch (the arena fills faster than wire bytes due to per-node
        // overhead, so the threshold is measured in arena bytes). The actual flush of the
        // rotated memtable runs on the background worker, off this writer's path.
        if state.mem.size() as usize >= self.mem_table_size / 2 {
            self.rotate_memtable(&mut state)?;
            self.work_cv.notify_one();
        }

        let base = state.vs.last_sequence + 1;
        batch.set_seqnum(base);
        if state.wal.is_some() {
            self.append_to_wal(&mut state, &batch)?;
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
    fn rotate_memtable(&self, state: &mut State) -> Result<()> {
        if state.mem.is_empty() {
            return Ok(());
        }
        let new_wal = state.vs.allocate_file_number();
        // Keep the active WAL directory across rotation (don't fall back to the primary if
        // we've already failed over to a secondary).
        let (wal, dir_idx) =
            create_wal(self.fs.as_ref(), &self.wal_dirs, state.wal_dir_idx, new_wal)?;
        state.wal_dir_idx = dir_idx;
        if let Some(l) = &self.listener {
            l.on_wal_created(new_wal);
        }
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

        let (mem, file_num) = {
            let mut state = self.state.lock().unwrap();
            if state.imm.is_empty() {
                return Ok(false);
            }
            let mem = Arc::clone(&state.imm[0]);
            let file_num = state.vs.allocate_file_number();
            (mem, file_num)
        };
        if let Some(l) = &self.listener {
            l.on_flush_begin();
        }

        // Write the sstable without holding the state lock.
        let meta = write_memtable_to_sstable(
            self.fs.as_ref(),
            &self.dir,
            &self.cmp,
            file_num,
            &mem,
            &self.block_property_collectors,
        )?;
        let flushed_bytes = meta.size;

        let mut state = self.state.lock().unwrap();
        // The new oldest un-flushed WAL once this immutable is removed.
        let new_log = state.imm_wals.get(1).copied().unwrap_or(state.wal_number);
        let edit = VersionEdit {
            log_number: Some(new_log),
            next_file_number: Some(state.vs.next_file_number),
            last_sequence: Some(state.vs.last_sequence),
            new_files: vec![NewFileEntry { level: 0, meta }],
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
        if popped_wal != 0 {
            // The WAL may live in any configured directory (failover); clean from each.
            for dir in &self.wal_dirs {
                self.clean_file(&dir.join(wal_filename(popped_wal)));
            }
            if let Some(l) = &self.listener {
                l.on_wal_deleted(popped_wal);
            }
        }
        // Keep the LSM in shape (e.g. drain L0 once it accumulates enough files).
        self.maybe_compact(&mut state)?;
        drop(state);

        self.log(&format!(
            "flushed memtable to sstable {file_num} ({flushed_bytes} bytes)"
        ));
        if let Some(l) = &self.listener {
            l.on_table_created(file_num);
        }

        if let Some(l) = &self.listener {
            l.on_flush_end(file_num, flushed_bytes);
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
                while state.imm.is_empty() && !state.shutdown {
                    state = self.work_cv.wait(state).unwrap();
                }
                if state.shutdown {
                    return; // any pending data stays in the WALs for recovery
                }
            }
            // Flush outside the state lock guard; errors are surfaced via the next
            // foreground flush/open rather than panicking the worker.
            if let Err(e) = self.flush_one() {
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
        // Sstables: L0 newest-first, then L1..L6.
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
                        break 'levels;
                    }
                }
            }
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
                    sources.push(Box::new(reader.iter()?));
                }
            }
        }
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
        let reader = Arc::new(Reader::open_with_cache(
            bytes,
            self.cmp.clone(),
            file_num,
            self.block_cache.clone(),
        )?);
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
        Ok(stats)
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
        let mut reader = record::Reader::new(std::io::Cursor::new(bytes), num as u32);
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

/// Writes every entry of `mem` (in internal-key order) to `<dir>/<file_num>.sst`, and
/// returns the file's metadata.
fn write_memtable_to_sstable(
    fs: &dyn Fs,
    dir: &Path,
    cmp: &Arc<dyn Comparer>,
    file_num: u64,
    mem: &Arc<MemTable>,
    collectors: &[BlockPropertyCollectorFactory],
) -> Result<FileMetadata> {
    let path = dir.join(filenames::table(file_num));
    let file = fs.create(&path)?;
    let mut w = Writer::new(file, cmp.clone(), WriterOptions::default());
    for factory in collectors {
        w.add_block_property_collector(factory());
    }

    let mut it = mem.iter();
    it.first();
    let mut smallest: Option<Vec<u8>> = None;
    let mut largest: Vec<u8> = Vec::new();
    let mut smallest_seq = u64::MAX;
    let mut largest_seq = 0u64;
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
        it.next();
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

    let mut file = w.finish()?;
    file.sync_all()?;

    Ok(FileMetadata {
        file_num,
        size: fs.size(&path)?,
        smallest: smallest.unwrap_or_default(),
        largest,
        smallest_seqnum: smallest_seq.min(largest_seq),
        largest_seqnum: largest_seq,
    })
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
