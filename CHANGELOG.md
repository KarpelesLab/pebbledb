# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.2](https://github.com/KarpelesLab/pebbledb/compare/v0.0.1...v0.0.2) - 2026-06-24

### Added

- shared-storage ingest surface (IngestExternalFiles + SetCreatorID + ObjProvider)
- Db::apply_no_sync_wait + SyncHandle (Pebble's ApplyNoSyncWait)
- LazyValue / deferred value fetch (Pebble's LazyValue)
- ingest skips the memtable flush when disjoint from in-memory data
- Db::flush_async + FlushHandle (Pebble's AsyncFlush)
- Iterator::stats / reset_stats (Pebble's Iterator.Stats)
- limited iteration family (Pebble's *WithLimit)
- Iterator::next_prefix (Pebble's NextPrefix)
- MinLZ block compression (Pebble v2 indicator 8) read + write
- persist the objstorage Provider's shared-object catalog
- GC obsolete native blob files in collect_obsolete
- compaction re-separates large values (value-separated DBs stay separated)
- Pebble reads our value-separated databases (blob-values attribute)
- engine writes value-separated databases (format 24)
- value separation in the columnar writer (out-of-line large values)
- write table-format-v7 columnar sstables (Pebble reads them)
- encode native blob-file MANIFEST records + Pebble blob references
- encode Pebble v6/v7 sstable footers
- write separated (out-of-line) values in columnar data blocks
- colblk key-value block encoder (v6/v7 metaindex write foundation)
- native blob file writer (PebbleBlobWriter)
- implement Pebble's shared-object catalog format (remoteobjcat)
- read Pebble value-separation databases end-to-end (format 24)
- decode native blob-file MANIFEST tags (format-24 readability)
- resolve separated values in v6/v7 columnar tables (native blob)
- decode Pebble inline blob handles (value-separation read foundation)
- read Pebble table format v6/v7 sstables (columnar metaindex)
- read Pebble v2 native blob files (FormatValueSeparation)
- columnar compaction output — columnar databases stay columnar
- engine flushes columnar sstables at the columnar format version
- columnar writer emits range-del / range-key keyspan blocks (Rust→Go)
- columnar writer produces Pebble v2-readable sstables (Rust→Go)
- read out-of-line (value-block) columnar values from Pebble v2
- read columnar keyspan blocks (range-del / range-key) from Pebble v2
- read Pebble v2 columnar (FormatColumnarBlocks) sstables
- upgrade interop to Pebble v2 + write the format-version marker
- persist blob-file references in the MANIFEST
- latency-triggered WAL failover
- persist the per-file span hint in the MANIFEST
- score L0 compaction by sublevel count (Pebble's L0 score)
- prefix-extractor bloom filters + table skipping on seek_prefix_ge
- large batches commit as their own flushable memtable
- per-operation latency metrics (get / commit / flush / compaction)
- cloneable iterators (DbIterator: Clone)
- EFOS invalidation on overlapping excise (disjoint excise stays valid)

### Fixed

- colblk bitmap encoding bytes match Pebble (default=0, zero=1)
- never compact a partial L0 in compact_range (stale-read inversion)
- align compaction output splits to user-key boundaries
- sync the data directory before the MANIFEST references new sstables

### Other

- ROADMAP — ApplyNoSyncWait done; scope shared-storage ingest surface
- refresh ROADMAP — async flush, flushable ingest, testdata iterator suite
- data-driven iterator-trace directives + cases (testdata corpus)
- cover ingest in the metamorphic model test
- refresh ROADMAP — value separation, MinLZ, objstorage catalog, iterator API
- VersionSet::load populates the native blob-file registry
- verify native blob writer against upstream Pebble (Rust→Go)
- mark columnar parity complete; scope remaining byte-parity items
- note columnar out-of-line-value/bitmap gap in the data-block reader
- poll for L0 drain in full_lifecycle (fix macOS flake)
- ROADMAP — columnar read parity done (Pebble v2)
- ROADMAP — byte-parity unblocked by Pebble v2; columnar steps
- ROADMAP — note byte-parity interop is blocked on a tagged Pebble release
- README — lazy-open scans, prefix bloom, sublevel scoring, WAL latency failover
- lazy-open ConcatIter — a scan opens only the files it touches
- README — cloneable iterators, unbounded batches, EFOS disjoint-excise
- read each LSM level as one merge source via a concat iterator

## [0.0.1](https://github.com/KarpelesLab/pebbledb/compare/v0.0.0...v0.0.1) - 2026-06-21

### Added

- cargo-fuzz subcrate + fix two block-reader panics it found
- Db::download — rewrite shared sstables back to local storage
- wire shared/remote object storage into the engine
- Db::check_consistency — LSM level-consistency check
- virtual sstables — excise rewrites only boundary files
- cross-sstable blob sharing (compaction preserves blob references)
- ingest-with-blobs — separate large ingested values into blob files
- blob-file rewrite during compaction
- DbIterator::set_options — reconfigure an iterator in place
- wire blob files into the engine (flush, reads, compaction, lifecycle)
- blob-file format module (writer + reader)
- WAL recycling with tolerant-tail recovery
- IterKeyType — point / range / both iterator key-type selection
- lazy indexed-batch iterator (read-your-own-writes via DbIterator)

### Fixed

- compaction tombstone elision could resurrect deleted keys
- bidirectional Go Pebble interop (go.sum, CURRENT file, index.type encoding)
- arena UB (Miri), and make three platform-fragile tests deterministic
- green CI across platforms — excise/compaction race, Windows lock, Miri scope
- ingest flushes the memtable so it wins over overlapping in-memory keys
- group-commit lost-leader deadlock + robust elision test
- *(clippy)* use sort_by_key(Reverse) in coalesced_range_keys

### Other

- rewrite ROADMAP — drop completed items, break remaining work into small tasks
- rustfmt wrap the index.type property push
- untrack accidentally-committed worktree dirs; gitignore .claude/
- mark columnar DefaultKeySchema done in ROADMAP
- add columnar DefaultKeySchema and wire it into ColumnarWriter/Reader
- persist virtual-sstable backing reference in FileMetadata + MANIFEST
- file-indexed blob references (foundation for blob sharing)
- ROADMAP — decoder robustness fuzzing in place
- decoder robustness fuzzing — found & fixed a Batch decode panic
- README — group commit, concurrent scheduler, value separation, per-block properties
- mark compaction scheduler done in ROADMAP
- concurrent compaction scheduler + safe obsolete-file deletion
- mark group commit done in ROADMAP
- group commit (leader-follower commit pipeline)
- per-block properties + block-level filter skipping
- per-level target file sizes (Options::level_target_file_sizes)
- L0 sublevels (read-amplification measure)
- add a data-driven test harness with inline-scripted cases
- value separation via value blocks at flush/compaction
- read- and write-amplification metrics
- route disk-slow events to the EventListener (on_disk_slow)
- sstable validation + table-validated/stats-loaded events + Batch::reset
- deletion pacing (rate-limited obsolete-file removal)
- refresh README capabilities (full compaction suite, EFOS, only_durable, checkpoint/level-budget options)
- multilevel compaction + configurable base-level budget (LBaseMaxBytes)
- strengthen metamorphic harness (indexed batches, snapshot isolation, more seeds)
- EventuallyFileOnlySnapshot (span-scoped consistent snapshot)
- checkpoint options (flush toggle + RestrictToSpans)
- flush splitting (split large point-only memtables into multiple L0 files)
- read-triggered compaction (compact repeatedly-passed-through files)
- tombstone-density compaction (drain dense files toward the bottom)
- elision-only compaction (rewrite bottom files to drop dead tombstones)
- delete-only compaction (drop files shadowed by a covering range tombstone)
- move compaction (relevel a non-overlapping file via MANIFEST edit)
- IterOptions::only_durable (OnlyReadGuaranteedDurable)
- refresh README capabilities (masking, block-property filters/collectors, registry, events, find/bench CLI)
- add 'pebbledb bench' CLI subcommand
- wire block-property collectors into writers + filters into iteration
- comparer/merger name->impl registry resolved at open
- WAL/MANIFEST/format/background EventListener events
- range-key masking in the iterator (IterOptions::range_key_masking_suffix)
- ROADMAP done-line includes tunables, snapshot iter, find CLI
- Snapshot::iter_with_options + 'find' CLI subcommand
- configurable compaction tunables (L0 threshold, target file size)
- mark excise/ingest_and_excise/compact/table_stats done in ROADMAP
- Db::compact + Db::ingest_and_excise
- Db::table_stats (aggregate per-table deletion/entry counts)
- Db::excise (range removal + physical reclamation)
- refresh ROADMAP + README for the latest parity work
- step-wise format-major-version migrations
- LSM view (Db::lsm_view + db lsm CLI)
- write stalls (memtable-count throttling) + stall events
- richer Metrics + flush/compaction begin events
- range-key coalescing (RANGEKEYSET/UNSET/DEL -> effective set)
- objstorage provider (local + shared/remote storage)
- DbIterator::set_bounds (reuse iterator with new bounds)
- Db::scan_internal (raw internal-key scan)
- mark completed parity follow-ups in ROADMAP
- block-property collectors + filters (table-level)
- surface range keys during iteration (DbIterator::range_keys)
- disk-health-checking vfs (DiskSlow reporting)
- Db::single_delete / delete_sized / log_data write ops
- *(readme)* refresh status + add capabilities section
- external iterator (new_external_iter)
- indexed batches (read-your-own-writes)
- Logger hook + Cleaner interface (delete/archive)
- Db::estimate_disk_usage(start, end)
- ROADMAP enumerates full path to 100% Pebble parity
- trim ROADMAP to status summary + remaining follow-ups
- correct columnar interop target to colblk.DefaultKeySchema
- Phase 16 complete: PrefixBytes codec + end-to-end columnar sstable
- Phase 16: columnar index + keyspan blocks (read+write)
- Phase 16: columnar column codecs + columnar data block (read+write)
- Phase 16 (partial): columnar block header parser + DataType
- Phase 17: NewFile5 + full custom-tag MANIFEST decoding
- Phase 27: multi-directory WAL with failover
- Phase 21: background flush/compaction worker + non-blocking rotation
- Phase 30: metamorphic model test, interop CI, Miri — fixes 2 bugs
- Phase 24: score-based compaction picker + manual compact_range
- Phase 25: OPTIONS file + format major version
- Phase 26: checkpoints + external sstable ingestion
- Phase 29: pebbledb inspection CLI
- Phase 22: vfs abstraction (Fs/DiskFs/MemFs) + OS directory locking
- Phase 20: bidirectional + seekable iterator surface
- Phase 23: block cache + bounded table cache
- Phase 18: merge operator
- Phase 28: metrics & event listener
- Phase 19: snapshots affect compaction
- WAL/MANIFEST fsync durability (toward Phase 25)
- Phase 15: value blocks (Pebblev3+)
- Phase 14: range keys (RANGEKEYSET / UNSET / DEL)
- Phase 13: range deletions
- Phase 12d: two-level sstable indexes (completes Phase 12)
- Phase 12c: sstable bloom filters
- Phase 12b: sstable properties block
- Phase 12a: xxHash64 block checksums
- make full Pebble parity the explicit goal (nothing scoped out)
- Phase 11: hardening (snapshots, metrics, ergonomics, tests, docs)
- Phase 10: leveled compaction
- Phase 9: DB write path (WAL, recovery, flush)
- Phase 8: DB read path (open, leveled get, merging iterator)
- Phase 7: MANIFEST (version edits, Version, VersionSet)
- Phase 6: sstable writer
- Phase 5: sstable reader
- Phase 4: arena-backed skiplist memtable
- Phase 3: write batches
- Phase 2: record log (WAL/MANIFEST framing)
