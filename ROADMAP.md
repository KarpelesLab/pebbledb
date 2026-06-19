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

### Batches & the write API
- **Indexed batches** (`NewIndexedBatch`): read-your-own-writes, a batch iterator merged
  into `DbIterator`, batch reuse/reset, and large batches handled as flushables.
- Remaining write-API surface: `apply` options, `DeleteSized` write path, empty-value and
  multi-op ergonomics parity.

### Iterators
- **Full `IterOptions`**: key-type selection (point / range / both), **range-key masking**,
  table filters, **block-property filters**, `OnlyReadGuaranteedDurable`.
- `SetBounds`, `SetOptions`, `Clone`; lazy values (`LazyValue`) and value fetching.
- **External iterators** (`NewExternalIter`: iterate sstables without ingesting) and
  **`ScanInternal`** (raw internal-key scan used by replication / disaggregated storage).
- Bloom-skip during `seek_prefix_ge`.

### Block properties
- The **block-property collector / filter mechanism** (Pebble): per-block and per-table
  property accumulation, storage in the (row and columnar) index/properties, and
  filter-driven block/table skipping during iteration and compaction. (The concrete
  MVCC-time collector is CockroachDB's; we implement the mechanism + any Pebble built-ins.)

### Compaction
- **Compaction scheduler**: multiple concurrent background compactions, prioritization.
- **Read-triggered compactions** (`read_compaction_queue`), **delete-only compactions**,
  **elision-only** and **tombstone-density** compactions, **move** compactions,
  **multilevel** compaction, and flush splitting.
- **Table stats**: background collection (tombstone/range-key counts, point-deletion bytes)
  feeding the above heuristics.
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
- **Excise** + **virtual sstables**, **`IngestAndExcise`**, **external-file ingestion**,
  **download** (rewrite remote/external files to local), flushable ingests, and
  **`EstimateDiskUsage`**.
- Checkpoint options (flush-WAL, restrict to spans). (Basic checkpoint exists.)

### Remote / disaggregated storage
- The **`objstorage` provider** abstraction (local provider + the `remote.Storage`
  interface, shared/external sstables, the shared-object cache). Concrete cloud backends
  (S3/GCS/Azure) are application-provided; we implement the Pebble interface and a
  local/in-memory backend.

### WAL
- The full `pebble/wal` failover **manager** (have: multi-directory write-failover +
  recovery), the **sync queue**, and WAL **recycling**.

### vfs
- **Disk-health-checking FS** (emit `DiskSlow`), syncing-FS guarantees, and the remaining
  vfs surface. (Have: `DiskFs`, `MemFs`, directory locking + sync.)

### Options, format & migrations
- Full **`Options`** surface incl. per-level options; a **comparer/merger name→impl
  registry**; **format-major-version migrations** (the on-disk upgrade step for each FMV,
  not just the recorded version); complete `OPTIONS` round-trip.

### Observability & file management
- Full **`EventListener`** event set (compaction/flush begin-end, manifest/table/WAL
  create-delete, table stats/validated, **write-stall** begin-end, disk-slow,
  background-error).
- Complete **`Metrics`**, a **`Logger`** hook, and **LSM view** debugging.
- **Obsolete-file deletion** + the **`Cleaner`** interface (delete vs archive).
- **Write stalls** (memtable/L0 stall thresholds and pacing).

### Columnar (key schema)
- Wire `colblk.DefaultKeySchema` (the schema a general Pebble KV store uses) into the
  columnar writer/reader so v5+ tables round-trip against Pebble. See interop steps below.
- Consistency checking (`level_checker`) over columnar tables.

### Tooling & testing
- Full `pebble` **CLI** (`db`, `sstable`, `manifest`, `wal`, `bench`, `find`, `lsm`).
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
