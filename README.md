# pebbledb

A Rust port of [CockroachDB's Pebble](https://github.com/cockroachdb/pebble) — an
LSM (log-structured merge-tree) key-value storage engine in the LevelDB / RocksDB
lineage.

> **Status: functional core, pre-1.0; full parity in progress.** The goal is a
> **complete** port of Pebble — 100% of its functionality and on-disk format, with
> nothing permanently out of scope. The core engine (Milestone 1) is done: open/create a
> store, `set`/`delete`/batch writes through a write-ahead log, flush memtables to
> sstables, leveled compaction, point lookups and snapshot-consistent iterators, and
> recovery on reopen, with the sstable / record-log / MANIFEST formats reproduced for
> binary compatibility. Milestone 2 extends this to everything else Pebble does —
> two-level indexes, bloom filters, range keys and range deletions, value blocks and blob
> files, the columnar table formats, the merge operator, background/concurrent
> compaction, the full options and metrics surfaces, ingestion, and more. See
> [`ROADMAP.md`](ROADMAP.md) for the phase-by-phase plan. The public API is **not** yet
> stable.

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
