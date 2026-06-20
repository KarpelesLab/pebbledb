# Roadmap

pebbledb is a Rust port of CockroachDB's Pebble, built bottom-up.

**Goal: a complete port of Pebble — 100% of its functionality and on-disk format.**
Full feature parity and **binary compatibility** with Pebble's on-disk formats (sstable in
every table-format version, the write-ahead log, the MANIFEST, blob files, OPTIONS,
markers) behind an **idiomatic Rust API** with Pebble semantics.

## Status

The original 30-phase build-out — a complete, working engine with broad Pebble parity — is
**implemented, tested, and on `master`**. The "Remaining for 100% Pebble parity" section
below enumerates the subsystems and breadth still needed to fully match upstream. Quality
gates run clean on every commit: `cargo fmt --check`, `cargo clippy --all-targets
--all-features -D warnings`, `cargo test`, `cargo doc` (warnings denied), and
`cargo +1.88 check` (MSRV).

Implemented, by area:

- **Keys & encodings** — comparer, internal keys/trailers, varints, CRC32C (masked),
  xxHash64.
- **WAL** — 32 KiB record log (legacy / recyclable / wal-sync formats); multi-directory
  WAL with write-failover and dual-directory recovery.
- **MemTable** — arena-backed concurrent skiplist, bidirectional iterator.
- **sstable (row format)** — read + write: footer/magic for every supported table-format
  version, metaindex, single- and two-level indexes, prefix-compressed data blocks, bloom
  filters, properties, range-del / range-key blocks, value blocks (Pebblev3+), CRC32C /
  xxHash64 checksums, Snappy / Zstd via `compcol`.
- **sstable (columnar, v5–v8)** — the `colblk` column codecs (uint, raw-bytes, bool
  bitmap, `PrefixBytes`) and all three columnar block formats (data / index / keyspan),
  read + write; a complete columnar table writer/reader (`sstable::columnar`).
- **MANIFEST** — full `VersionEdit` decode incl. `NewFile5` and the `NewFile4/5` custom-tag
  set (virtual tables, synthetic prefix/suffix, blob references); replay + write.
- **DB** — open/recovery via atomic markers; get; bidirectional iteration with bounds and
  prefix seek; snapshots; merge operator; range deletions and range keys.
- **Write path & concurrency** — WAL append, non-blocking memtable rotation, a background
  flush/compaction worker behind `Arc<DbInner>`, score-based + manual compaction.
- **Caching** — sharded byte-bounded LRU block cache; bounded table cache.
- **Durability & options** — `OPTIONS` file, `FormatMajorVersion` ratcheting, WAL sync
  modes, vfs (`DiskFs` / `MemFs`) with OS directory locking.
- **Maintenance** — checkpoints, external sstable ingestion.
- **Tooling & testing** — `pebbledb` inspection CLI; a seeded metamorphic model test;
  bidirectional Go interop CI; a Miri job over the `unsafe` arena.

## Remaining for 100% Pebble parity

The baseline above is a complete, working engine. Reaching full parity with upstream
Pebble means adding the subsystems and breadth below. This list is scoped to **Pebble
itself**; see "CockroachDB boundary" for where we stop. Each group is a committed goal, not
an exclusion.

_Done so far: indexed batches (read-your-own-writes), `single_delete` / `delete_sized` /
`log_data`, `new_external_iter`, `ScanInternal`, iterator `set_bounds`, range-key surfacing
**and `RANGEKEYSET`/`UNSET`/`DEL` coalescing** during iteration **plus range-key masking**
(`IterOptions::range_key_masking_suffix`), the table-level
block-property collector/filter mechanism, the disk-health-checking vfs, the `objstorage`
provider (local + shared/remote), `EstimateDiskUsage`, `Db::table_stats`, richer `Metrics`,
the LSM view (+ `db lsm` and `find` CLI), flush/compaction **begin** + table/ingest +
**WAL/MANIFEST create-delete + format-upgrade + background-error** `EventListener` events,
**write stalls** with stall events, step-wise
**format-major-version migrations**, `Db::excise` / `ingest_and_excise` / `compact`,
configurable compaction tunables (`l0_compaction_threshold`, `target_file_size`),
`Snapshot::iter_with_options`, a `Logger`, and the `Cleaner` (delete/archive)._

### Batches & the write API
- Large batches handled as flushables. (The **lazy indexed-batch iterator** is done:
  `IndexedBatch::iter` / `iter_with_options` return a real `DbIterator` that layers the batch's
  pending point ops, range deletions, and range keys over the committed view through the normal
  iteration machinery — nothing materialized eagerly, unlike `scan`. `get`/`scan`
  read-your-own-writes and `Batch::reset` for reuse are also done.)

### Iterators
- (Done.) **`IterOptions`** is complete: **key-type selection** (`IterKeyType::PointsOnly` /
  `RangesOnly` / `PointsAndRanges`) — `PointsOnly` suppresses range-key surfacing (masking and
  range-deletion shadowing still apply), `RangesOnly` walks the defragmented range-key spans
  (`key()` is each span's start), `PointsAndRanges` (the pebbledb default) surfaces both. Also
  `SetBounds`, range-key surfacing + coalescing, **range-key masking**, **block-property filters
  wired into iteration** (table-level skipping via `IterOptions::block_property_filters`),
  **`OnlyReadGuaranteedDurable`** (`IterOptions::only_durable`), and `ScanInternal`.
- `SetOptions`, `Clone`; lazy values (`LazyValue`) and value fetching.
- Bloom-skip during `seek_prefix_ge`.

### Block properties
- (Done. **Per-block properties** are stored after each data block's index entry — collectors
  emit them via `BlockPropertyCollector::finish_data_block`, and `Reader::iter_with_filters`
  skips individual data blocks ruled out by `IterOptions::block_property_filters`, on top of
  table-level skipping. Collectors are wired into the flush/compaction writers via
  `Options::block_property_collectors`. The concrete MVCC-time collector is CockroachDB's.)

### Compaction
- (Done. **Compaction scheduler** — compactions run off the state lock (reserve+snapshot,
  write, then apply), pickers mark a `compacting` file set so `Options::max_concurrent_compactions`
  background workers compact disjoint files in parallel; obsolete files are deleted only once
  no in-flight read references them. **Multilevel compaction** — when a single-file `level → level+1` compaction's output
  would also overlap `level+2`, all three levels are folded into one compaction writing
  straight to `level+2`; the base-level budget is `Options::l1_max_bytes` (Pebble's
  `LBaseMaxBytes`). **Flush splitting** — a point-only memtable is split into multiple L0
  sstables at `Options::target_file_size` boundaries on flush. Plus, each driven by table
  stats / on-disk properties or read feedback: **move compactions** — relevelling a single
  non-overlapping
  file by a MANIFEST edit without rewriting; **delete-only compactions** — dropping files
  entirely shadowed by a covering range tombstone; **elision-only compactions** — rewriting a
  bottom file to drop its now-dead tombstones; **tombstone-density compactions** — pushing a
  file whose point-tombstone fraction exceeds `Options::tombstone_dense_compaction_threshold`
  toward the bottom for elision; and **read-triggered compactions** — a read queue
  (`Options::read_compaction_threshold` wasted seeks) that compacts repeatedly-passed-through
  L1+ files down.)
- Read/write-amplification *scoring* (the picker still scores L0 by raw file count). (Done:
  explicit **L0 sublevels** — overlapping L0 files packed into non-overlapping layers, surfaced
  as `Metrics::l0_sublevels` and folded into `read_amplification`; **deletion pacing** —
  `Options::target_byte_deletion_rate`, a background pacer spacing obsolete-file deletion.)

### Commit pipeline
- (Done.) **Group commit**: a leader-follower pipeline batches many concurrent committers
  through one WAL sync + a run of memtable applies (`apply` enqueues, then leads or waits).

### Snapshots
- (Done: the consistent seqnum-pinned snapshotting model, and **EventuallyFileOnlySnapshot**
  — `Db::new_eventually_file_only_snapshot(spans)` returns a consistent, span-scoped snapshot
  that flushes to become file-backed and rejects reads outside its spans. The disjoint-range
  excise optimization — letting an excise over ranges disjoint from the EFOS proceed without
  invalidating it — remains, pending virtual sstables.)

### Value separation & blob files
- **Value separation** is done for the **in-table value-block** form: with
  `Options::value_block_threshold` set, flush and compaction store large values out-of-line in
  the table's value blocks (Pebble v3 format), transparently re-separated through compaction
  and read back via the value-prefix path. The remaining, distinct form is separate **blob
  files** (cross-table value storage): the blob-file *writer*, **blob-file rewrite** during
  compaction, **ingest-with-blobs**, and blob-file references carried through the MANIFEST —
  a new on-disk format whose byte-parity needs the Go interop CI.

### Ingestion & maintenance
- **Virtual sstables** (so excise/ingest-and-excise rewrite only boundary files instead of
  compacting), **download** (rewrite remote/external files to local), and flushable ingests.
  (`Db::excise`, `Db::ingest_and_excise`, external sstable `ingest`, `Db::compact`, and
  `EstimateDiskUsage` exist; excise currently reclaims via compaction.)
- (Done: **checkpoint options** — `CheckpointOptions` with a flush toggle and
  `RestrictToSpans`-style span restriction, via `Db::checkpoint_with_options`.)

### Remote / disaggregated storage
- Wire the engine's sstable reads/writes onto the **`objstorage` provider** so shared
  (remote) tables participate in the LSM. (The provider abstraction — local + the
  `remote.Storage` interface with an in-memory backend — is implemented; concrete cloud
  backends like S3/GCS/Azure are application code.)

### WAL
- The full `pebble/wal` failover **manager** (have: multi-directory write-failover +
  recovery). (Done: **WAL recycling** — `Options::wal_recycle_limit` retains flushed WAL files
  and reuses them in place for new logs, reading recycled/torn tails tolerantly during
  recovery; and the **sync queue**, provided by group commit — concurrent committers are
  batched through a single WAL `fsync`.)

### vfs
- Syncing-FS guarantees and the remaining vfs surface. (Have: `DiskFs`, `MemFs`, directory
  locking + sync, and the disk-health-checking FS emitting `DiskSlow`.)

### Options, format & migrations
- Full **`Options`** surface. (**Per-level options** — per-level output target file sizes via
  `Options::level_target_file_sizes` — are done. Step-wise **format-major-version
  migrations** and the `OPTIONS` round-trip are done; per-version migrations are currently
  no-ops awaiting versions that need them; the **comparer/merger name→impl registry**
  (`Options::comparers` / `Options::mergers`, resolved against the store's recorded names at
  open) is done.)

### Observability & file management
- (Done — the full **`EventListener`** surface: flush/compaction begin+end, table
  created/deleted/**validated**, **table stats loaded**, ingest end, write-stall begin+end,
  WAL/MANIFEST create-delete, format upgrade, background-error, and **disk-slow** routed from
  the health-checking vfs via `Options::disk_slow_threshold`; plus `Db::validate_sstables` to
  drive table validation.)
- Further **`Metrics`** breadth (per-op latencies). (Have: core `Metrics` incl. **read- and
  write-amplification**, the LSM view, a `Logger`, the `Cleaner`, and memtable-count write
  stalls.)

### Columnar (key schema)
- Wire `colblk.DefaultKeySchema` (the schema a general Pebble KV store uses) into the
  columnar writer/reader so v5+ tables round-trip against Pebble. See interop steps below.
- Consistency checking (`level_checker`) over columnar tables.

### Tooling & testing
- (Have: `sstable`/`wal`/`manifest` dump, `db get`/`scan`/`lsm`, `find`, and `bench` CLIs; a
  seeded **metamorphic model test** covering points/deletes/range-deletes/indexed
  batches/snapshots/flush/compact/reopen across six seeds; a **data-driven test harness**
  (`tests/datadriven.rs`) with inline-scripted cases; and **decoder robustness fuzzing**
  (`tests/fuzz_decoders.rs`) that drives the batch/record/MANIFEST/sstable decoders with random
  and mutated-valid input — the in-crate, stable analogue of a libFuzzer corpus run over the
  same entry points. A coverage-guided **`cargo-fuzz` target** would need a `fuzz/` subcrate
  (the plan's "discuss before a sub-project"); porting Pebble's *own* `testdata` corpus needs
  the Go fixtures and the interop CI.)

## CockroachDB boundary

Some code in the Pebble repository is CockroachDB-specific. For these we implement **only
the Pebble-side mechanism / hook**, not the CockroachDB policy:

- **`cockroachkvs`** (MVCC key/timestamp schema) → we implement the pluggable
  `colblk.KeySchema` mechanism and `DefaultKeySchema`; not the Cockroach schema itself.
- **MVCC block-property collector** → we implement the block-property collector/filter
  mechanism; not the MVCC-time collector.
- **Cockroach's comparer / split** → we implement the `Comparer` trait (with `Split`) and
  the bytewise default; not Cockroach's `EngineComparer`.
- **Cloud remote backends** (S3/GCS/Azure `remote.Storage`) → we implement the `objstorage`
  / `remote.Storage` *interface* and a local backend; concrete cloud providers are
  application code.

## Interop testing (CI)

Cross-implementation interop is validated in GitHub Actions (`.github/workflows/interop.yml`)
rather than committed binary fixtures or a local Go toolchain dependency: a workflow
installs Go + Pebble, round-trips data through both engines, and fails if the byte formats
or semantics diverge.

The workflow currently round-trips at `FormatMostCompatible` (the **row** format), which
the engine reads and writes both ways. The remaining, well-specified interop work is
columnar:

1. **`colblk.DefaultKeySchema(comparer, 16)`** — the schema a general Pebble KV user gets
   once columnar is enabled (`FormatColumnarBlocks`+): a `PrefixBytes` prefix column (split
   by the comparer) + a `Bytes` suffix column. Implement this exact column decomposition in
   the columnar writer/reader, record the `KeySchema` name, then extend the workflow to
   round-trip a columnar table both ways.
2. **`cockroachkvs`** — CockroachDB's MVCC key/timestamp schema (vendored in the Pebble
   repo); only relevant for interop with a CockroachDB store.
3. The `PrefixBytes` delta-offset sub-encoding nuance (see `sstable::colblk`).

Note: columnar is opt-in in Pebble (`FormatDefault` → a row format), so a default
`pebble.Open` produces row-format tables, which pebbledb already reads and writes.

## Format references

Format details are reproduced from the upstream sources cited in [`NOTICE`](NOTICE):
Pebble, LevelDB-Go, RocksDB, and LevelDB.
