# Roadmap

pebbledb is a Rust port of CockroachDB's Pebble, built bottom-up. Each phase is a
self-contained layer with its own tests, landing as one or more commits.

**Goal: a complete port of Pebble — 100% of its functionality and on-disk format.**
Nothing is permanently out of scope. The objective is full feature parity and
**binary compatibility** with Pebble's on-disk formats (sstable in every table-format
version, the write-ahead log, the MANIFEST, blob files, OPTIONS, markers) behind an
**idiomatic Rust API** with Pebble semantics. Anything Pebble can do, pebbledb will do;
anything Pebble can read or write, pebbledb will read and write.

Legend: `[x]` done · `[~]` in progress · `[ ]` not started.

The work is organized in two milestones. **Milestone 1** (Phases 0–11) is a complete,
working core engine and is done. **Milestone 2** (Phases 12+) extends it to full parity
with upstream Pebble. The split is purely about ordering of delivery, not scope — every
Milestone 2 phase is a committed goal.

## Milestone 1 — core engine (complete)

- [x] **Phase 0 — Scaffold.** Cargo manifest (MSRV 1.88, edition 2024, BSD-3-Clause),
  LICENSE/NOTICE/README, CI (fmt, clippy, docs, MSRV), release-plz, crate skeleton,
  `compcol` wired for compression.
- [x] **Phase 1 — base layer.** `Comparer` / `DefaultComparer` (bytewise), `InternalKey`
  / `InternalKeyKind` / `SeqNum` and the 8-byte trailer, varint + fixed-LE encoders,
  CRC32C (Castagnoli) with the RocksDB mask.
- [x] **Phase 2 — record log (WAL).** Reader/writer over 32 KiB blocks; 7-byte header
  (`crc32c | length | type`); full/first/middle/last records plus the recyclable and
  wal-sync record formats.
- [x] **Phase 3 — Batch.** 12-byte header (`seqnum: u64`, `count: u32`) followed by the
  op stream; encode/decode and apply to a memtable.
- [x] **Phase 4 — MemTable.** Arena-backed concurrent skiplist (port of `arenaskl`)
  ordered by internal key, with an iterator.
- [x] **Phase 5 — sstable read (row format).** Footer + magic + format versions,
  metaindex, single-level index, prefix-compressed data blocks with restart arrays,
  block trailer (compression byte + CRC32C), decompression via `compcol`, table iterator.
- [x] **Phase 6 — sstable write (row format).** Block builder, single-level index,
  writer producing output the reader (and Pebble/RocksDB) can read.
- [x] **Phase 7 — Manifest / Version.** `VersionEdit` tag stream (common tags +
  NewFile/2/3/4), `FileMetadata`, `Version` (L0..L6), `VersionSet`, MANIFEST as a record
  log, replay and write.
- [x] **Phase 8 — DB read path.** Open via the atomic marker → MANIFEST; leveled point
  lookups; snapshot-consistent merging iterator over memtables + levels.
- [x] **Phase 9 — DB write path.** WAL append, memtable rotation, synchronous flush of a
  memtable to an L0 sstable, crash recovery from the WAL.
- [x] **Phase 10 — Compaction (basic).** Inline leveled compaction: L0-by-count and L1+
  by-size triggers, newest-version collapse, bottom-level tombstone drop.
- [x] **Phase 11 — Hardening.** Top-level re-exports, doctested example, `Snapshot`,
  `Metrics`, `LOCK` file, end-to-end integration tests, API docs.

## Milestone 2 — full Pebble parity

Each phase below is a goal, not an exclusion. Ordering is approximate; phases will be
refined as they are reached.

- [x] **Phase 12 — sstable format completeness.** Two-level indexes (read + write), bloom
  filters (full filter: build, store, and use in `Get`/`SeekPrefixGE`), the complete
  properties block, full metaindex, xxHash64 block checksums, and per-block-kind options.
- [x] **Phase 13 — Range deletions.** RANGEDEL v1/v2 blocks (read + write),
  range-tombstone-aware iterators, `Get`, and compaction (truncation, fragmentation,
  eliding).
- [x] **Phase 14 — Range keys.** RANGEKEYSET / RANGEKEYUNSET / RANGEKEYDEL blocks,
  range-key iterators and masking, batch operations, and compaction of range keys.
- [x] **Phase 15 — Value blocks & blob files.** Pebblev3 value blocks (write + `Get`
  indirection), Pebblev4 DELSIZED tombstones, and blob files for separated values with
  their MANIFEST blob-file edits and references.
- [ ] **Phase 16 — Columnar blocks.** The Pebblev5+ columnar data / index / keyspan block
  formats (read + write), table-format versions v5–v8, and the v6/v7 footer checksum +
  attributes.
- [ ] **Phase 17 — MANIFEST completeness.** NewFile5, virtual/backing tables, excise
  records, column families, table-marked-for-compaction, blob-file edits;
  `BulkVersionEdit` accumulation; complete `FileMetadata` (virtual, synthetic
  prefix/suffix, range-key bounds, blob references).
- [x] **Phase 18 — Merge operator.** A pluggable `Merger` and full MERGE resolution in
  `Get`, iteration, and compaction.
- [x] **Phase 19 — Snapshots & sequence semantics.** Registered snapshots that hold back
  compaction (retain versions an open snapshot needs), correct SINGLEDEL semantics, and
  eventually-file-only snapshots.
- [x] **Phase 20 — Iterator surface.** Full bidirectional iteration through the whole DB
  (`first`/`last`/`next`/`prev`/`seek_ge`/`seek_lt`), with seek and reverse threaded down
  through `BlockIter`, `TableIter`, the memtable iterator, and a direction-switching
  `MergingIter`. `IterOptions` lower/upper bounds (inclusive/exclusive) and
  `seek_prefix_ge` prefix iteration on `DbIterator`, with reverse merge/range-tombstone
  resolution shared with the forward path. (Bloom-skip during `seek_prefix_ge` and
  point/range/both key-type selection remain a perf/range-key follow-up.)
- [ ] **Phase 21 — Concurrency & commit pipeline.** Group commit, a background flush
  worker, concurrent background compactions, non-blocking memtable rotation, and
  read/write concurrency under load.
- [x] **Phase 22 — vfs abstraction.** An `Fs` trait (`vfs` module) with `DiskFs` and
  `MemFs`, threaded through the whole engine: open/recovery, flush, compaction, MANIFEST,
  WAL, table reads, atomic markers, and directory syncing all go through it. True
  OS-level directory locking (`flock` on Unix via a zero-dependency `extern "C"`
  declaration, an exclusive lock file elsewhere), released on `Db` drop. Verified by a
  full open/flush/compact/reopen lifecycle running entirely on `MemFs` and a
  concurrent-open lock test.
- [x] **Phase 23 — Caches.** A sharded, byte-bounded LRU block cache (`cache::BlockCache`,
  keyed by `(file_num, block_offset)`) wired through the sstable reader's data- and
  value-block reads; a bounded table cache of open readers (`Options::max_open_files`,
  reference-count-aware eviction); cache sizing via `Options::block_cache_size`; and
  hit/miss counters surfaced in `Metrics::block_cache_{hits,misses}`.
- [x] **Phase 24 — Compaction completeness.** A score-based picker (each level scored
  against its trigger — L0 by file count, L1+ by size budget — picking the most
  overloaded level rather than always L0), manual `Db::compact_range` that drains a
  user-key range toward the bottom level, range-del/range-key carrying through
  compaction, and obsolete-file cleanup. (Read/write-amplification heuristics,
  elision-only and tombstone-density compactions, explicit L0 sublevels, and deletion
  pacing remain a follow-up refinement.)
- [x] **Phase 25 — Options & durability.** An INI-style `OPTIONS-NNNNNN` file
  (`OptionsFile`, Pebble-compatible layout) written on every read-write open and parsed +
  validated on reopen (comparer-name mismatch and too-new format are rejected). A
  `FormatMajorVersion` type with monotonic `Db::ratchet_format_major_version` that
  persists a new OPTIONS file, surfaced via `Db::format_major_version`. WAL fsync /
  no-sync modes (`Options::wal_sync`) and the block-cache / open-files / merger / listener
  options are wired. (Per-level option blocks, a comparer/merger registry for name→impl
  resolution, and on-disk format *migrations* beyond the version bump remain a follow-up.)
- [x] **Phase 26 — Ingestion & maintenance.** `Db::checkpoint` writes a self-contained,
  openable copy (flush, copy live sstables, fresh MANIFEST + marker). `Db::ingest` adds
  external sstables — rewritten through the engine's writer at one freshly-assigned
  sequence number per file (the functional equivalent of Pebble's global-seqnum
  ingestion), placed in L0 and recorded in the MANIFEST, carrying point keys, range
  tombstones, and range keys. Verified by checkpoint-reopen and ingest-shadow-and-reopen
  tests. (Excise, flushable ingests, and disk-usage estimation remain a follow-up.)
- [ ] **Phase 27 — WAL manager & failover.** The `pebble/wal` package: multiple WAL
  directories, failover, recycling, and the sync queue.
- [x] **Phase 28 — Metrics & observability.** Complete `Metrics`, an `EventListener`, and
  logging/tracing hooks matching Pebble's surface.
- [x] **Phase 29 — Tooling.** A `pebbledb` CLI binary (`src/bin/pebbledb.rs`) with
  `sstable dump`, `wal dump`, `manifest dump`, and `db get` / `db scan` (read-only)
  subcommands, with human-readable internal-key and byte-escaping formatting. Integration
  tests drive each subcommand against freshly-built on-disk files. (Further `pebble`
  subcommands — `bench`, `find`, space-amplification analysis — remain a follow-up.)
- [ ] **Phase 30 — Interop & correctness hardening.** Bidirectional interop verified in
  **GitHub Actions**: a CI job installs Go and `cockroachdb/pebble`, generates fixtures
  (sstables, WAL, MANIFEST, full DBs) that pebbledb must read, and verifies Go Pebble can
  read pebbledb's output — both directions, across table-format versions. Plus ports of
  Pebble's data-driven tests, metamorphic testing, fuzzing, and Miri over the `unsafe`
  arena code.

## Interop testing (CI)

Cross-implementation interop is validated in GitHub Actions rather than committed binary
fixtures or a local Go toolchain dependency: a workflow installs Go + Pebble, round-trips
data through both engines, and fails if the byte formats or semantics diverge. This keeps
the crate's own build dependency-light while still proving binary compatibility against
upstream on every run.

## Format references

Format details are reproduced from the upstream sources cited in [`NOTICE`](NOTICE):
Pebble, LevelDB-Go, RocksDB, and LevelDB.
