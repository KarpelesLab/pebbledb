# pebbledb

A Rust port of [CockroachDB's Pebble](https://github.com/cockroachdb/pebble) — an
LSM (log-structured merge-tree) key-value storage engine in the LevelDB / RocksDB
lineage.

> **Status: broad parity implemented, pre-1.0.** The goal is a **complete** port of Pebble
> — 100% of its functionality and on-disk format, with nothing permanently out of scope.
> The full original build-out is implemented and tested: open/create, `set` / `delete` /
> `merge` / `delete_range` and (indexed) batches through a write-ahead log, background
> flush and leveled compaction, bidirectional snapshot-consistent iteration with bounds
> and prefix seek, range keys and range deletions, value blocks and blob-file reads, the
> merge operator, a block + table cache, the columnar table-format codecs, checkpoints and
> external-sstable ingestion, an `OPTIONS` file with format-major-version ratcheting, a
> `vfs` (`DiskFs` / `MemFs`) with OS directory locking, and multi-directory WAL failover —
> with the sstable / record-log / MANIFEST formats reproduced for binary compatibility.
> The remaining work toward full upstream parity (wiring the disaggregated `objstorage`
> provider into the engine, virtual sstables, and columnar key-schema byte-parity — most of it
> gated on the Go interop CI) is catalogued in
> [`ROADMAP.md`](ROADMAP.md). The public API is **not** yet stable.

## Capabilities

- **Writes**: `set` / `delete` / `single_delete` / `merge` / `delete_range`, range keys,
  atomic `Batch`es, and **indexed batches** (read-your-own-writes via `Db::indexed_batch`,
  including a **lazy `IndexedBatch::iter`** that layers pending writes over the committed view).
- **Reads**: point `get`, snapshots (incl. an **EventuallyFileOnlySnapshot** scoped to key
  spans), a **bidirectional** iterator (`first`/`last`/`next`/`prev`/`seek_ge`/`seek_lt`) with
  `IterOptions` bounds, `set_bounds`, `seek_prefix_ge`, **key-type selection**
  (`IterKeyType` points / ranges / both), range-key surfacing + coalescing,
  **range-key masking**, **block-property filters** that skip non-matching sstables, and
  `only_durable` (read only flushed data); `new_external_iter` reads sstables without
  ingesting them, and `scan_internal` exposes the raw internal keyspace.
- **Engine**: WAL with multi-directory failover, **group commit** (concurrent writers
  batched through one fsync), and optional **WAL recycling** (`Options::wal_recycle_limit`,
  reusing log files in place with tolerant tail recovery); **flush splitting**, and a full
  leveled compaction suite —
  score-based + manual `compact_range`, **multilevel**, **move**, **delete-only**,
  **elision-only**, **tombstone-density**, and **read-triggered** compactions run by a
  **concurrent compaction scheduler** (`Options::max_concurrent_compactions`) with paced
  obsolete-file deletion — plus write stalls, a sharded block cache, and `EstimateDiskUsage`.
- **Storage formats**: row-format sstables (every supported table-format version, two-level
  indexes, bloom filters, **value separation** into in-table value blocks
  (`Options::value_block_threshold`) or **separate blob files**
  (`Options::blob_value_threshold`), range-del/range-key blocks, **table- and per-block**
  property collectors/filters via `Options::block_property_collectors`) and the columnar
  (v5–v8) block codecs; CRC32C / xxHash64 checksums; Snappy / Zstd compression.
- **Operations**: `checkpoint` (with flush/span-restriction options), external sstable
  `ingest`, `Options` + `OPTIONS` file with a **comparer/merger name→impl registry**,
  tunable level budgets (`l1_max_bytes`), step-wise `FormatMajorVersion` migrations,
  `Metrics`, an `lsm_view`, an `EventListener` (flush/compaction, table, WAL/MANIFEST
  create-delete, format upgrade, write-stall, background-error), a `Logger`, and a `Cleaner`
  (delete or archive obsolete files).
- **Filesystem & objects**: an `Fs` trait with `DiskFs`, in-memory `MemFs`, and a
  disk-health-checking wrapper; an `objstorage` provider for local + shared/remote objects.
- **Tooling**: a `pebbledb` CLI (`sstable` / `wal` / `manifest` dump, `db get` / `scan` /
  `lsm`, `find`, and `bench`).

## Usage

```rust
use pebbledb::{Db, Options};

let db = Db::open("/path/to/db", Options::default())?;
db.set(b"hello", b"world")?;
assert_eq!(db.get(b"hello")?, Some(b"world".to_vec()));

let snap = db.snapshot();              // consistent read view
db.set(b"hello", b"again")?;
assert_eq!(snap.get(b"hello")?, Some(b"world".to_vec()));

let mut it = db.iter()?;               // ordered forward iteration
it.first()?;
while it.valid() {
    println!("{:?} => {:?}", it.key(), it.value());
    it.next()?;
}
# Ok::<(), pebbledb::Error>(())
```

## Goals

- **Binary-compatible on-disk format.** pebbledb reproduces Pebble's exact byte
  formats for sstables, the write-ahead log (record log), and the MANIFEST, so a
  database written by Pebble can be opened by pebbledb and vice-versa.
- **Idiomatic Rust API, Pebble semantics.** A `Result`-based, trait-driven Rust API
  whose operations and semantics mirror Pebble's `DB` / `Batch` / `Iterator`, rather
  than a literal transliteration of the Go method names.
- **Minimal dependencies.** A single crate with a small dependency surface;
  compression is provided by [`compcol`](https://github.com/KarpelesLab/compcol)
  (snappy + zstd).

## Minimum supported Rust version (MSRV)

Rust **1.88** (edition 2024). The MSRV is enforced in CI and treated as a contract;
bumping it is a breaking change.

## License & attribution

Licensed under the **BSD-3-Clause** license, the same license as upstream Pebble.
This project is a derivative work; the original copyright notice of *The LevelDB-Go
Authors* is retained. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE) for full
attribution to Pebble, LevelDB-Go, RocksDB, and LevelDB.
