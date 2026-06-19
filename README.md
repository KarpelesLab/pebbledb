# pebbledb

A Rust port of [CockroachDB's Pebble](https://github.com/cockroachdb/pebble) — an
LSM (log-structured merge-tree) key-value storage engine in the LevelDB / RocksDB
lineage.

> **Status: functional, pre-1.0.** All roadmap phases are implemented: the engine
> opens/creates a store, supports `set`/`delete`/batch writes through a write-ahead log,
> flushes memtables to sstables, performs leveled compaction, reads via point lookups and
> snapshot-consistent iterators, and recovers on reopen. The on-disk formats (sstable,
> record log, MANIFEST) are reproduced for binary compatibility. Several advanced areas
> are intentionally scoped out for now — two-level sstable indexes, value blocks
> (Pebblev3+), the columnar format (Pebblev5+), xxHash checksums, range keys, virtual
> sstables, background/concurrent compaction, and `fsync`-level durability. See
> [`ROADMAP.md`](ROADMAP.md) for details. The public API is **not** yet stable.

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
