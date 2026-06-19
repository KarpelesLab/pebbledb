# Roadmap

pebbledb is a Rust port of CockroachDB's Pebble, built bottom-up. Each phase is a
self-contained layer with its own tests, landing as one or more commits. The goal is
**binary compatibility** with Pebble's on-disk formats (sstable, write-ahead log,
MANIFEST) and an **idiomatic Rust API** with Pebble semantics.

Legend: `[x]` done · `[~]` in progress · `[ ]` not started.

## Phases

- [x] **Phase 0 — Scaffold.** Cargo manifest (MSRV 1.88, edition 2024, BSD-3-Clause),
  LICENSE/NOTICE/README, CI (fmt, clippy, docs, MSRV), release-plz, crate skeleton,
  `compcol` wired for compression.
- [x] **Phase 1 — base layer.** `Comparer` / `DefaultComparer` (bytewise), `InternalKey`
  / `InternalKeyKind` / `SeqNum` and the 8-byte trailer, varint + fixed-LE encoders,
  CRC32C (Castagnoli) with the RocksDB mask.
- [x] **Phase 2 — record log (WAL).** Reader/writer over 32 KiB blocks; 7-byte header
  (`crc32c | length | type`); full/first/middle/last records plus the recyclable
  record format (carrying a log number). Read real Pebble WAL files.
- [x] **Phase 3 — Batch.** 12-byte header (`seqnum: u64`, `count: u32`) followed by the
  op stream; encode/decode and apply to a memtable.
- [x] **Phase 4 — MemTable.** Arena-backed concurrent skiplist (port of `arenaskl`)
  ordered by internal key, with an iterator.
- [ ] **Phase 5 — sstable read.** Footer + magic + format versions (RocksDBv2,
  Pebblev1..v5), metaindex, index (incl. two-level), prefix-compressed data blocks with
  restart arrays, block trailer (compression byte + checksum: CRC32C and xxhash64),
  decompression via `compcol`, bloom filters, properties, range-del / range-key blocks,
  and a table iterator. Read real Pebble sstables.
- [ ] **Phase 6 — sstable write.** Block builder (restart interval), filter builder,
  properties, and a writer producing output that Go Pebble can read.
- [ ] **Phase 7 — Manifest / Version.** `VersionEdit` tag stream, `FileMetadata`,
  `Version` (L0..L6) and `VersionSet`, MANIFEST as a record log, marker / CURRENT files,
  OPTIONS file, and the file lock. Open an existing DB's manifest.
- [ ] **Phase 8 — DB read path.** Open an existing Pebble DB read-only; merging iterator
  across memtables and levels with range-tombstone / range-key handling; `Get`;
  snapshots. Interop: read a Go-written DB.
- [ ] **Phase 9 — DB write path.** WAL append with group commit, memtable rotation,
  flush of a memtable to an L0 sstable, and crash recovery from the WAL.
- [ ] **Phase 10 — Compaction.** Compaction picker, L0→Lbase and leveled compaction,
  obsolete-file deletion, basic read/write-amplification accounting.
- [ ] **Phase 11 — Hardening.** Block and table caches, the full `Options` surface,
  metrics, a concurrency review, fuzzing, bidirectional interop tests against Go Pebble,
  and documentation.

## Format references

Format details are reproduced from the upstream sources cited in [`NOTICE`](NOTICE):
Pebble, LevelDB-Go, RocksDB, and LevelDB.
