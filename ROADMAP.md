# Roadmap

pebbledb is a Rust port of CockroachDB's Pebble, built bottom-up.

**Goal: a complete port of Pebble — 100% of its functionality and on-disk format.**
Full feature parity and **binary compatibility** with Pebble's on-disk formats (sstable in
every table-format version, the write-ahead log, the MANIFEST, blob files, OPTIONS,
markers) behind an **idiomatic Rust API** with Pebble semantics.

## Status

The engine is feature-complete against upstream Pebble's behavior and reads/writes its
on-disk formats in both directions (verified by a bidirectional Go interop CI at the row
format). Everything in **Implemented** below is on `master`. Quality gates run clean on
every commit: `cargo fmt --check`, `cargo clippy --all-targets --all-features -D warnings`,
`cargo test`, `cargo doc` (warnings denied), `cargo +1.88 check` (MSRV), a Miri job over the
`unsafe` arena, and the Go interop workflow.

What's left (see **Remaining work**) is a short list of in-crate refinements/optimizations
plus cross-implementation **byte-parity** items that are validated by the Go interop CI.

### Implemented

- **Keys & encodings** — comparer (+`split`), internal keys/trailers, varints, CRC32C
  (masked), xxHash64.
- **WAL** — 32 KiB record log (legacy / recyclable / wal-sync formats); multi-directory
  write-failover + recovery; WAL recycling (`wal_recycle_limit`); sync queue via group commit.
- **MemTable** — arena-backed concurrent skiplist (Miri-clean), bidirectional iterator.
- **sstable (row)** — read+write across every supported table-format version: footer/magic,
  metaindex, single- and two-level indexes, prefix-compressed data blocks, bloom filters,
  properties, range-del / range-key blocks, value blocks (Pebblev3+), CRC32C / xxHash64,
  Snappy / Zstd.
- **sstable (columnar, v5–v8)** — `colblk` column codecs (uint, raw-bytes, bool bitmap,
  `PrefixBytes`), all three block formats (data / index / keyspan), a full columnar
  writer/reader, and an in-crate `KeySchema` + `DefaultKeySchema` (prefix+suffix split).
- **MANIFEST** — `VersionEdit` decode/replay/write incl. `NewFile4/5` custom tags (virtual
  tables, synthetic prefix/suffix, blob references).
- **Reads** — open/recovery via atomic markers (+ legacy `CURRENT`); `get`; bidirectional
  iteration with bounds, prefix seek, key-type selection, range-key surfacing/coalescing/
  masking, block-property filters (table- and per-block), `only_durable`, in-place
  `set_options`, `scan_internal`; snapshots + EventuallyFileOnlySnapshot; merge operator;
  range deletions and range keys.
- **Write path & concurrency** — WAL append, non-blocking memtable rotation, **group
  commit**, write stalls; indexed batches (read-your-own-writes + lazy iterator).
- **Compaction** — concurrent scheduler (`max_concurrent_compactions`, deferred obsolete
  deletion), multilevel, flush splitting, move / delete-only / elision-only /
  tombstone-density / read-triggered, L0 sublevels, deletion pacing, `l1_max_bytes`,
  per-level target file sizes. Tombstone elision is snapshot-stripe- and
  external-overlap-aware (never resurrects deleted keys).
- **Value separation** — in-table value blocks (`value_block_threshold`) and separate blob
  files (`blob_value_threshold`) with cross-sstable sharing and reference-based GC.
- **Ingestion & maintenance** — external sstable `ingest` (flushes the memtable for
  correctness), `excise` via **virtual sstables**, `ingest_and_excise`, `compact`,
  `download`, `checkpoint` (+ `CheckpointOptions`), `EstimateDiskUsage`, `check_consistency`.
- **Remote / disaggregated storage** — engine sstable/blob reads+writes wired onto a
  `RemoteStorage` backend (`remote_storage` + `create_on_shared`), probe-remote-then-local,
  ships with an in-memory backend.
- **Caching** — sharded byte-bounded LRU block cache; bounded table cache.
- **Options & durability** — `OPTIONS` file round-trip, `FormatMajorVersion` ratcheting,
  WAL sync modes, comparer/merger name→impl registry.
- **vfs** — `DiskFs`, `MemFs`, OS directory locking + sync, disk-health-checking FS
  (`DiskSlow`).
- **Observability** — full `EventListener` surface, `Metrics` (incl. read/write
  amplification), `Logger`, `Cleaner`, LSM view.
- **Tooling & testing** — `pebbledb` CLI (`sstable`/`wal`/`manifest` dump, `db
  get`/`scan`/`lsm`, `find`, `bench`); seeded metamorphic model test; data-driven harness;
  in-crate decoder fuzzing + an opt-in `cargo-fuzz` subcrate; bidirectional Go interop CI.

## Remaining work

The engine is complete; the items below are scoped small and grouped by kind. Anything
gated on the Go interop CI is under **Byte-parity & interop**.

### In-crate refinements & optimizations

- [ ] **`LazyValue` / deferred value fetch.** Avoid materializing a value until the caller
  asks for it (Pebble's `LazyValue`). Must be sound — no `unsafe` lifetime extension.
  - [ ] Add a `LazyValue` type that is either inline bytes or an *owned* fetch handle
    (table `Arc<Reader>` + value-block/blob handle), fetching on `.value()`.
  - [ ] Add an iterator accessor returning `LazyValue` (keep the eager `value()` for the
    common path) and thread it through the merging iterator.
  - [ ] Wire value-block and blob (`KIND_BLOB`) resolution to defer to `LazyValue::value()`.
  - [ ] Test: iterate without touching values does no value-block/blob reads (count via a
    probe `RemoteStorage`/reader).

- [ ] **Flushable ingest (queue, don't force-flush).** Today `ingest` forces a memtable flush
  for correctness; instead queue the ingested sstables as a flushable so writers aren't
  blocked.
  - [ ] Represent ingested sstables as a flushable entry in the memtable queue at the reserved
    seqnums.
  - [ ] Resolve it on flush by adding the files at the right level (no rewrite).
  - [ ] Test: concurrent writers proceed during an ingest; the ingested values still win over
    older unflushed keys.

- [ ] **Sublevel scoring + lazy run files (the second half of sublevel-aware reads).** The
  read path now presents each level (and each L0 sublevel) as one ordered `ConcatIter` run, so
  the merging iterator pays one source per run, not per file (done). What remains is letting L0
  *file count* grow (Pebble scores L0 by sublevel count) without paying for it:
  - [ ] Make `ConcatIter`/`build_run_iters` open a run's files lazily (on seek/cross), so a
    flat L0 of many files does not open them all up front when a scan starts.
  - [ ] Then switch the picker's L0 score from file count to sublevel count
    (`max(sublevels/l0_compaction_threshold, files/L0CompactionFileThreshold)`). Until lazy
    opening lands, file-count scoring is the right fit — it keeps a run's file set small, which
    bounds both scan-setup cost (files opened per run) and L0 read amplification.
  - [ ] Test: a deep (many-sublevel) L0 compacts before a flat (one-sublevel) L0; a flat L0
    still drains via the file-count guard; a scan over a flat L0 opens files lazily.

- [ ] **WAL failover manager parity.** Beyond the current multi-directory write-failover +
  recovery, match `pebble/wal`'s manager surface.
  - [ ] Failover health monitoring (latency-triggered secondary switch) + the related metrics.
  - [ ] Configurable failover policy on `Options`.

### Byte-parity & interop (Go CI)

These are correctness-vs-upstream checks; the formats are implemented to spec with in-crate
round-trip tests, but exact byte-parity is proven only by the Go interop workflow.

- [ ] **Columnar round-trip in interop.** Extend the workflow beyond the row format.
  - [ ] Match `colblk.DefaultKeySchema(comparer, 16)`'s exact `KeySchema` name string.
  - [ ] Verify/finish the `PrefixBytes` delta-offset sub-encoding (see `sstable::colblk`).
  - [ ] Extend `.github/workflows/interop.yml` to round-trip a columnar
    (`FormatColumnarBlocks`+) table both ways.
- [ ] **Blob file format byte-parity.** Diff `sstable::blob` output against a Pebble-written
  blob file in the interop workflow; reconcile magic/footer/handle encoding.
- [ ] **Persist blob references in the MANIFEST.** Record `FileMetadata::blob_refs` in the
  MANIFEST (the `NewFile5` blob-reference custom tag) instead of rescanning the metaindex at
  open; keep open-rescan as a fallback.
- [ ] **objstorage catalog byte-format.** Match Pebble's shared-object catalog on-disk format
  so a disaggregated store round-trips through the interop workflow.
- [ ] **Port Pebble's `testdata` corpus.** Vendor a subset of upstream data-driven fixtures
  (needs the Go toolchain in CI) and run them through the in-crate decoders/engine.

### Deferred / blocked

- **Per-version `FormatMajorVersion` migrations** — the ratchet exists; per-version migration
  steps are no-ops until a version actually requires data rewriting.
- **`cockroachkvs` MVCC schema** — CockroachDB-specific; out of scope (see boundary). We
  provide the pluggable mechanism, not the schema.

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

Cross-implementation interop is validated in GitHub Actions
(`.github/workflows/interop.yml`): the workflow installs Go + Pebble, round-trips data
through both engines, and fails if the byte formats or semantics diverge. It currently
round-trips the **row** format both ways (Go writes → pebbledb reads, and pebbledb writes →
Go reads). Extending it to the columnar format is the **Byte-parity & interop** work above.

Note: columnar is opt-in in Pebble (`FormatDefault` → a row format), so a default
`pebble.Open` produces row-format tables, which pebbledb already reads and writes.

## Format references

Format details are reproduced from the upstream sources cited in [`NOTICE`](NOTICE):
Pebble, LevelDB-Go, RocksDB, and LevelDB.
