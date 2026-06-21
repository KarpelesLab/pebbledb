# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
