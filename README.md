# pebbledb

A Rust port of [CockroachDB's Pebble](https://github.com/cockroachdb/pebble) — an
LSM (log-structured merge-tree) key-value storage engine in the LevelDB / RocksDB
lineage.

> **Status: early development.** The crate is being built bottom-up, layer by layer.
> See [`ROADMAP.md`](ROADMAP.md) for what is implemented and what is planned. The
> public API is **not** yet stable.

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
