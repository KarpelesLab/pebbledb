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
the LSM view (+ `db lsm` and `find` CLI), flush/compaction **begin** + table/ingest
`EventListener` events, **write stalls** with stall events, step-wise
**format-major-version migrations**, `Db::excise` / `ingest_and_excise` / `compact`,
configurable compaction tunables (`l0_compaction_threshold`, `target_file_size`),
`Snapshot::iter_with_options`, a `Logger`, and the `Cleaner` (delete/archive)._

### Batches & the write API
- For indexed batches: a lazy batch iterator merged into `DbIterator` (today `get`/`scan`
  provide read-your-own-writes), batch reuse/reset, and large batches handled as flushables.

### Iterators
- Remaining **`IterOptions`**: key-type selection (point / range / both), **range-key
  masking**, table filters, **block-property filters wired into iteration**,
  `OnlyReadGuaranteedDurable`. (`SetBounds`, range-key surfacing + coalescing, and
  `ScanInternal` are done.)
- `SetOptions`, `Clone`; lazy values (`LazyValue`) and value fetching.
- Bloom-skip during `seek_prefix_ge`.

### Block properties
- **Per-block** (vs per-table) properties stored in the index, and filter-driven block
  skipping during iteration and compaction. (The table-level collector/filter mechanism is
  done; the concrete MVCC-time collector is CockroachDB's.)

### Compaction
- **Compaction scheduler**: multiple concurrent background compactions, prioritization.
- **Read-triggered compactions** (`read_compaction_queue`), **delete-only compactions**,
  **elision-only** and **tombstone-density** compactions, **move** compactions,
  **multilevel** compaction, and flush splitting. (Table stats — `Db::table_stats`,
  aggregate tombstone/range-key/entry counts — are available to drive these.)
- Read/write-amplification scoring, explicit **L0 sublevels**, and **deletion pacing**.

### Commit pipeline
- True **group commit** (batch many committers through one WAL sync + memtable apply).

### Snapshots
- **EventuallyFileOnlySnapshot** (EFOS) and the consistent `read_state` snapshotting model.

### Value separation & blob files
- **Value separation** at flush/compaction time (write large values to blob files),
  **blob-file rewrite** during compaction, **ingest-with-blobs**, and blob-file references
  carried through the MANIFEST. (Basic blob-file read + ingestion exist.)

### Ingestion & maintenance
- **Virtual sstables** (so excise/ingest-and-excise rewrite only boundary files instead of
  compacting), **download** (rewrite remote/external files to local), and flushable ingests.
  (`Db::excise`, `Db::ingest_and_excise`, external sstable `ingest`, `Db::compact`, and
  `EstimateDiskUsage` exist; excise currently reclaims via compaction.)
- Checkpoint options (flush-WAL, restrict to spans). (Basic checkpoint exists.)

### Remote / disaggregated storage
- Wire the engine's sstable reads/writes onto the **`objstorage` provider** so shared
  (remote) tables participate in the LSM. (The provider abstraction — local + the
  `remote.Storage` interface with an in-memory backend — is implemented; concrete cloud
  backends like S3/GCS/Azure are application code.)

### WAL
- The full `pebble/wal` failover **manager** (have: multi-directory write-failover +
  recovery), the **sync queue**, and WAL **recycling**.

### vfs
- Syncing-FS guarantees and the remaining vfs surface. (Have: `DiskFs`, `MemFs`, directory
  locking + sync, and the disk-health-checking FS emitting `DiskSlow`.)

### Options, format & migrations
- Full **`Options`** surface incl. per-level options and a **comparer/merger name→impl
  registry**. (Step-wise **format-major-version migrations** and the `OPTIONS` round-trip
  are done; per-version migrations are currently no-ops awaiting versions that need them.)

### Observability & file management
- Remaining **`EventListener`** events (manifest/WAL create-delete, table stats/validated,
  disk-slow, background-error). (Have: flush/compaction begin+end, table created/deleted,
  ingest end, write-stall begin+end.)
- Further **`Metrics`** breadth (per-op latencies, amplification). (Have: core `Metrics`,
  the LSM view, a `Logger`, the `Cleaner`, and memtable-count write stalls.)

### Columnar (key schema)
- Wire `colblk.DefaultKeySchema` (the schema a general Pebble KV store uses) into the
  columnar writer/reader so v5+ tables round-trip against Pebble. See interop steps below.
- Consistency checking (`level_checker`) over columnar tables.

### Tooling & testing
- Remaining `pebble` **CLI** subcommands (`bench`, `find`). (Have: `sstable`/`wal`/
  `manifest` dump, `db get`/`scan`/`lsm`.)
- Port Pebble's **data-driven test corpus** and a **metamorphic** harness; add a
  **libFuzzer** target. (A seeded model test provides randomized coverage today.)

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
