# Roadmap

pebbledb is a Rust port of CockroachDB's Pebble, built bottom-up.

**Goal: a complete port of Pebble — 100% of its functionality and on-disk format.**
Full feature parity and **binary compatibility** with Pebble's on-disk formats (sstable in
every table-format version, the write-ahead log, the MANIFEST, blob files, OPTIONS,
markers) behind an **idiomatic Rust API** with Pebble semantics.

## Status

All planned phases (the core engine and full-parity work, originally Phases 0–30) are
**implemented, tested, and on `master`**. Quality gates run clean on every commit:
`cargo fmt --check`, `cargo clippy --all-targets --all-features -D warnings`, `cargo test`,
`cargo doc` (warnings denied), and `cargo +1.88 check` (MSRV).

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

## Remaining work (follow-ups)

These are refinements and breadth items beyond the implemented baseline; none block normal
use.

- **Columnar interop with Pebble's key schemas.** The columnar block formats and codecs
  are implemented and self-round-trip, but reading/writing a *Pebble-written* columnar
  table is gated on its pluggable `colblk.KeySchema`. See the interop follow-ups below.
- **Compaction heuristics.** Read/write-amplification scoring, elision-only and
  tombstone-density compactions, explicit L0 sublevels, deletion pacing.
- **Commit pipeline.** True group-commit batching of multiple committers; multiple
  concurrent background compaction threads.
- **MANIFEST breadth.** Full *virtual-table* support (not just decode), `BulkVersionEdit`
  accumulation, and excise / standalone blob-file / column-family records (currently
  rejected rather than supported).
- **Options breadth.** Per-level option blocks, a comparer/merger name→impl registry, and
  on-disk format *migrations* beyond the version bump.
- **Ingestion breadth.** Excise, flushable ingests, disk-usage estimation.
- **WAL throughput.** Inode/space recycling of WAL files, batched sync queue.
- **Iterator extras.** Bloom-skip during `seek_prefix_ge`; point/range/both key-type
  selection.
- **Tooling.** Additional `pebble` CLI subcommands (`bench`, `find`, space-amp analysis).
- **Testing.** Port Pebble's data-driven test corpus; add a libFuzzer target (the seeded
  model test provides randomized coverage in the meantime).

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
