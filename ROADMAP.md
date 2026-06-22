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

- [x] **Sublevel-aware reads + scoring.** The read path presents each level (and each L0
  sublevel) as one ordered `ConcatIter` run, opened **lazily** (a part's sstable reader is opened
  only when a seek lands in it; range tombstones / range keys are collected eagerly but skip
  files an in-memory per-file "has spans" hint marks span-free). The picker now scores L0 by
  **sublevel** count — `max(sublevels/l0_compaction_threshold, files/l0_compaction_file_threshold)`
  (Pebble's L0 score: sublevel threshold 4, file-count safety cap 500) — so a flat L0 of many
  disjoint files (one sublevel) no longer over-compacts, while overlapping flushes still trigger
  at the sublevel threshold.
  - [x] Persist the per-file span hint in the MANIFEST (a pebbledb-private, safe-to-ignore
    `NewFile4` custom tag that upstream Pebble skips — verified by the interop workflow) so a
    cold reopen seeds the hint from file metadata and the first scan skips span-free files
    without opening them to learn it.

- [x] **WAL failover manager parity.** Beyond the current multi-directory write-failover +
  recovery: a slow-but-successful WAL write (append + sync exceeding
  `Options::wal_failover_latency_threshold`) now proactively fails the WAL over to the next
  directory (Pebble's latency-triggered failover), counted in `Metrics::wal_failover_count`
  (which also covers error-triggered failovers). The switch is forward-only toward the last
  configured directory.
  - [ ] Deferred (niche): health-probe **switchback** to the primary once it recovers, and a
    richer per-directory health/latency-policy surface. The forward latency+error failover above
    covers the common slow/failing-disk case.

### Byte-parity & interop (Go CI)

These are correctness-vs-upstream checks; the formats are implemented to spec with in-crate
round-trip tests, but exact byte-parity is proven only by the Go interop workflow.

> **In progress (Pebble v2).** The interop workflow now pins **Pebble v2** (`pebble/v2`,
> v2.1.6), which is the tagged line that ships `FormatColumnarBlocks` and separate blob files —
> so these are no longer externally blocked. The row format round-trips both ways against v2
> (this required writing the `marker.format-version` file Pebble treats as authoritative). The
> columnar and blob round-trips are the remaining work; the reference source is vendored locally
> for byte comparison.

- [x] **Columnar read round-trip in interop.** The engine reads Pebble v2 columnar
  (`FormatColumnarBlocks`) sstables: the `colblk` reader/writer match Pebble v2.1.6's data block
  (4-byte custom header + seven columns: prefix, suffix, trailer, prefix-changed, value,
  is-value-external, is-obsolete), `sstable::Reader` decodes a columnar table and serves point
  lookups + iteration, and the open ceiling is raised to `COLUMNAR_BLOCKS` (19). Verified by a
  checked-in Pebble columnar sstable fixture and a `generate-columnar` interop CI step (Go writes
  columnar → Rust reads). New databases still default to the row format.
  - [x] **Columnar keyspan blocks (range-del / range-key).** The `colblk` keyspan block matches
    Pebble v2.1.6's boundary-based layout (a 4-byte custom header carrying the unique-boundary-key
    count, then five columns: boundary user keys, boundary key indices, key trailers, suffixes,
    values), and `ColumnarReader::keyspans` reads them via the metaindex (`rocksdb.range_del2` /
    `pebble.range_key`), reconstructs fragmented spans, and re-encodes them into the engine's
    `RangeTombstone` / `RangeKeyEntry` representation so the full read path applies them. Also
    fixed: the `colblk` bool-bitmap encoding bytes (`default = 0`, `zero = 1`) now match Pebble,
    so the data block's is-value-external column decodes correctly and out-of-line columnar values
    are detected and rejected (read support pending below). Verified by a checked-in Pebble
    columnar-spans fixture, the `generate-columnar-spans` interop CI step (Go writes → Rust reads),
    and a byte-identical regeneration of the fixture from the Go tool.
  - [x] **Out-of-line columnar values.** Out-of-line (value-block) columnar values — flagged by
    the is-value-external column — are now resolved: `SchemaDataBlockReader::decode_all` surfaces
    the external reference, and `ColumnarReader` reads the value-block index from the metaindex
    (`pebble.value_index`) and resolves each reference (value-prefix byte + value-block handle)
    against the value blocks. This also fixed PrefixBytes reconstruction of **exact-duplicate
    keys** (multiple versions of one user key, the case that produces value blocks under the
    default schema): an empty suffix slice now reuses the nearest non-empty suffix in the bundle
    instead of decoding to a truncated key. Verified by a checked-in Pebble value-block fixture
    (key written twice under a snapshot → older value separated) + a `generate-columnar-valueblock`
    interop CI step, regenerating the fixture byte-for-byte.
  - [x] **Columnar writer byte-parity (Rust→Go).** The `ColumnarWriter` now emits sstables that
    upstream Pebble v2.1.6 reads back byte-for-byte: point keys, range deletions, and range keys
    (the `add` method routes by kind into keyspan-block builders, as the row writer does). Two
    fixes made Pebble accept the output — emitting the schema name under the `pebble.colblk.schema`
    property tag Pebble reads, and writing the index block's fourth (block-properties) column its
    reader requires. Verified by a `columnar_sst_gen` example + a `verify-columnar-sst` interop
    command/CI step (Go reads the points and the keyspans).
  - [x] **Engine columnar flush.** At a columnar format major version
    (`FormatMajorVersion::COLUMNAR_BLOCKS`, 19) the engine flushes each memtable to a columnar
    sstable (points, range deletions, and range keys; values inline — value-block / blob
    separation is an optional space optimization Pebble does not require for correctness). Lower
    formats still flush row sstables, matching Pebble's format-gated behavior. Verified by an
    end-to-end test (flushed file is columnar; reads round-trip with the range deletion applied)
    and a Rust→Go interop step where the engine writes a columnar database that Pebble v2 opens
    and reads.
  - [x] **Columnar compaction output.** Compaction also emits columnar sstables at a columnar
    format major version (via a `CompactionWriter` enum wrapping the row or columnar writer), so a
    columnar database stays columnar end-to-end. Blob-referenced values are resolved to bytes and
    stored inline rather than preserved (the cross-sstable blob-sharing optimization is row-only).
    Verified by an end-to-end test (forced flushes + full compaction; every surviving sstable is
    columnar and overwritten values are correct) and a Rust→Go interop step where the engine
    writes, compacts, and Pebble v2 reads the columnar database. Optional value-block separation in
    columnar output remains a space optimization, not required for parity.
- [ ] **Blob file format byte-parity (`FormatValueSeparation`).** pebbledb has a working value
  separation scheme (`sstable::blob`, a sibling-file 1:1 design with reference-based GC), but it is
  *not* byte-identical to Pebble's native blob files. Pebble only writes separate `.blob` files at
  `FormatValueSeparation` — a format major version (~24) well above `FormatColumnarBlocks` (19) that
  also introduces a new sstable table format (`FormatTableFormatV6`, footer attributes) and the
  richer `NewFile5` blob-reference encoding (value sizes, reference depth, garbage ratio). Reaching
  byte-parity here is effectively implementing Pebble's whole value-separation subsystem (table v6 +
  native blob files + blob-ref MANIFEST encoding), a distinct multi-part effort rather than a
  format diff. (Confirmed against v2.1.6: a DB at format 19 with `ValueSeparationPolicy{Enabled}`
  writes no `.blob` — the policy is gated on `FormatValueSeparation`.) Progress:
  - [x] **Native blob file reader.** `sstable::pebble_blob::PebbleBlobReader` reads Pebble's native
    `.blob` format byte-for-byte: the 38-byte footer (`crc | index_offset | index_length |
    checksum_type | format | original_file_num | magic`, magic `🪳🦀`), the index block (4-byte
    `countVirtualBlocks` header + a `colblk` offsets column of `countBlocks + 1` entries), and each
    value block (a `colblk` single RawBytes column), resolving values by `(block_id, value_id)`.
    Verified by a checked-in real Pebble v2.1.6 blob fixture (`pebble_v2_blobfile.blob`).
  - [x] **Table format v6/v7 sstable reading (inline values).** The engine reads Pebble table
    format v6 (57-byte footer + footer checksum) and v7 (61-byte footer + attributes word)
    sstables. The footer-length handling already covered these; the remaining gap was that v6+
    stores the metaindex (and properties) as a **columnar key-value block** (`colblk` two RawBytes
    columns) rather than the legacy row block — `colblk::decode_key_value_block` + a format-aware
    metaindex reader handle both. Verified by a checked-in real Pebble v7 fixture
    (`pebble_v2_v7_inline.sst`, written at `FormatValueSeparation` with values inline). A v6/v7
    table whose values are *separated* additionally needs the items below.
  - [x] **Separated-value resolution (sstable level).** A v6/v7 columnar table whose values are
    separated into a native blob file now reads them: the value column holds a value-prefix byte
    (kind bits select value-block vs blob handle) followed by an inline blob handle
    (`reference_id, value_len, block_id, value_id`); `ColumnarReader::resolve_value` decodes it,
    maps `reference_id` through an attached blob-reference list to a blob file number, and fetches
    the value through a `pebble_blob::NativeBlobResolver`. Verified by checked-in real Pebble v7
    fixtures (`pebble_v2_v7_separated.{sst,blob}`): all five separated values resolve.
  - [x] **Full value-separation database read (`FormatValueSeparation`).** The engine opens a
    Pebble format-24 database whose values are separated into native blob files and reads them
    end-to-end. The MANIFEST decoder captures each sstable's blob references
    (`customTagBlobReferences`) and the `NewBlobFile`/`DeletedBlobFile` records into a version-level
    blob-file registry (blob file ID → physical `<num>.blob`); a `NativeBlobStore` opens those files
    via `PebbleBlobReader`; and at table open the engine resolves each file's `reference_id`s to blob
    file numbers and passes them + the resolver into the columnar reader (before its up-front
    materialization). The read format ceiling rises to `MAX_READABLE` = 24 (distinct from the
    writable `NEWEST` = 19). Verified by a Rust→Go interop step: Go writes a value-separated
    database, the engine opens it read-only and reads every separated value.
  - [ ] The value-separation **write** path (emit v6/v7 + separate values into native blob files).
    Progress:
    - [x] **Native blob file writer.** `pebble_blob::PebbleBlobWriter` writes Pebble's native
      `.blob` format (value block + index block + 38-byte footer with masked-CRC32C checksum),
      mirroring the reader. Verified both by a round-trip through `PebbleBlobReader` and by upstream
      Pebble: a `pebble_blob_gen` example + a `verify-pebble-blob` interop step open the written file
      with Pebble's own `blob.FileReader` and validate its layout. (Fixed: a 0-row colblk column —
      the empty `virtualBlocks` column — must occupy no bytes and share the next column's offset.)
    - [x] **v7 sstable write.** `ColumnarWriter` emits table-format-v7 sstables (61-byte footer with
      the feature-attributes word, columnar key-value metaindex + properties blocks, footer
      attributes derived to match Pebble's `toAttributes` from the properties) when its
      `table_format` is `Pebble(7)`. Verified by an in-crate round-trip and a Rust→Go interop step:
      Pebble v2.1.6 reads a v7 columnar sstable the writer produced.
    - [x] **`NewFile5`/`NewBlobFile` MANIFEST write.** `VersionEdit::encode` writes
      `tagNewBlobFile`/`tagDeletedBlobFile` and the standard `customTagBlobReferences` tag for a
      file's Pebble blob references (round-trip tested).
    - [ ] The flush/compaction value-separation policy (route large values through
      `PebbleBlobWriter`, emit inline handles, write the `blob-reference-index` block, add the
      `NewFile5`/`NewBlobFile` MANIFEST records at format 24) and raising the write format ceiling —
      the final assembly that makes the *engine* emit a value-separated database.
- [x] **Persist blob references in the MANIFEST.** `FileMetadata::blob_refs` is recorded via a
  pebbledb-private, safe-to-ignore custom tag (Pebble skips it), so blob-file GC recovers an
  sstable's references from the MANIFEST at open instead of re-reading its metaindex; the
  open-rescan remains as a fallback for legacy / upstream-Pebble records. (Byte-parity with
  Pebble's own richer `NewFile5` blob-reference encoding — value sizes, reference depth — is part
  of the blob byte-parity work above.)
- [x] **objstorage catalog on-disk format.** `objstorage::remoteobjcat` implements Pebble's
  shared-object catalog format (`REMOTE-OBJ-CATALOG`): a [record log](src/record) of catalog
  version-edits with `tagNewObject(1)` (file number, object type, creator ID, creator file number,
  cleanup method, optional locator/custom-name sub-tags, `0` terminator), `tagDeletedObject(2)`, and
  `tagCreatorID(3)`, plus replay into the accumulated object set. Encode/decode and full
  catalog-file round-trips are unit-tested; the format follows Pebble's
  `remoteobjcat.VersionEdit` byte-for-byte. (Wiring it into the probe-based
  [`Provider`](src/objstorage) read path, and a real-catalog interop diff via a Pebble
  shared-storage harness, remain as follow-ups.)
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
