// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

//! End-to-end integration tests driving pebbledb through its public API: open, write,
//! flush, compact, snapshot, iterate, reopen, and recover.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use std::sync::Arc;

use pebbledb::vfs::MemFs;
use pebbledb::{Batch, Db, IterOptions, Options};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let p = std::env::temp_dir().join(format!("pebbledb-e2e-{tag}-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn collect(db: &Db) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut it = db.iter().unwrap();
    let mut out = Vec::new();
    it.first().unwrap();
    while it.valid() {
        out.push((it.key().to_vec(), it.value().to_vec()));
        it.next().unwrap();
    }
    out
}

#[test]
fn full_lifecycle_with_reopen_and_compaction() {
    let dir = temp_dir("lifecycle");
    let opts = Options {
        mem_table_size: 32 * 1024, // small, to force flushes + compaction
        // Drain L0 by file count (these are disjoint, single-sublevel flushes); matches the
        // pre-sublevel-scoring default so "L0 stays bounded" holds.
        l0_compaction_file_threshold: 4,
        ..Default::default()
    };

    // Phase 1: write a few thousand keys, overwriting some.
    {
        let db = Db::open(&dir, opts.clone()).unwrap();
        for i in 0..4000u32 {
            db.set(format!("key{i:06}").as_bytes(), format!("v{i}").as_bytes())
                .unwrap();
        }
        // Overwrite the first 100 with new values via a batch.
        let mut batch = Batch::new();
        for i in 0..100u32 {
            batch.set(format!("key{i:06}").as_bytes(), b"overwritten");
        }
        db.write(batch).unwrap();
        // Delete a band of keys.
        for i in 2000..2100u32 {
            db.delete(format!("key{i:06}").as_bytes()).unwrap();
        }

        // Compaction keeps L0 bounded; the background worker drains it asynchronously, so poll
        // for the eventual drained state rather than asserting it synchronously (the check would
        // otherwise race a just-reached file-count trigger, as seen on slower CI runners).
        let mut drained = false;
        for _ in 0..200 {
            if db.metrics().level_files[0] < 4 {
                drained = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
        assert!(drained, "L0 not drained: {:?}", db.metrics().level_files);

        // Reads reflect the latest state.
        assert_eq!(db.get(b"key000000").unwrap(), Some(b"overwritten".to_vec()));
        assert_eq!(db.get(b"key003999").unwrap(), Some(b"v3999".to_vec()));
        assert_eq!(db.get(b"key002050").unwrap(), None); // deleted
    }

    // Phase 2: reopen and confirm durability across a close/open cycle.
    {
        let db = Db::open(&dir, opts.clone()).unwrap();
        assert_eq!(db.get(b"key000000").unwrap(), Some(b"overwritten".to_vec()));
        assert_eq!(db.get(b"key000099").unwrap(), Some(b"overwritten".to_vec()));
        assert_eq!(db.get(b"key000100").unwrap(), Some(b"v100".to_vec()));
        assert_eq!(db.get(b"key003999").unwrap(), Some(b"v3999".to_vec()));
        assert_eq!(db.get(b"key002000").unwrap(), None);
        assert_eq!(db.get(b"key002099").unwrap(), None);

        // 4000 written, 100 deleted -> 3900 live keys, in sorted order.
        let all = collect(&db);
        assert_eq!(all.len(), 3900);
        for w in all.windows(2) {
            assert!(w[0].0 < w[1].0, "iteration must be sorted");
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn snapshot_isolation() {
    let dir = temp_dir("snapshot");
    let db = Db::open(&dir, Options::default()).unwrap();
    db.set(b"k", b"v1").unwrap();
    let snap = db.snapshot();
    db.set(b"k", b"v2").unwrap();
    db.set(b"new", b"after-snapshot").unwrap();

    // The snapshot sees the old value and not the later insert.
    assert_eq!(snap.get(b"k").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(snap.get(b"new").unwrap(), None);
    // The live database sees the latest.
    assert_eq!(db.get(b"k").unwrap(), Some(b"v2".to_vec()));
    assert_eq!(db.get(b"new").unwrap(), Some(b"after-snapshot".to_vec()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn block_cache_serves_repeated_reads() {
    let dir = temp_dir("blockcache");
    let db = Db::open(&dir, Options::default()).unwrap();
    for i in 0..500u32 {
        db.set(format!("k{i:04}").as_bytes(), format!("v{i}").as_bytes())
            .unwrap();
    }
    db.flush().unwrap(); // push everything to an L0 sstable

    // First pass populates the block cache; the second pass should hit it.
    for _ in 0..2 {
        for i in 0..500u32 {
            assert_eq!(
                db.get(format!("k{i:04}").as_bytes()).unwrap(),
                Some(format!("v{i}").into_bytes())
            );
        }
    }
    let m = db.metrics();
    assert!(
        m.block_cache_hits > 0,
        "expected block-cache hits, got {}",
        m.block_cache_hits
    );

    let _ = std::fs::remove_dir_all(&dir);
}

fn collect_reverse(it: &mut pebbledb::DbIterator) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    it.last().unwrap();
    while it.valid() {
        out.push((it.key().to_vec(), it.value().to_vec()));
        it.prev().unwrap();
    }
    out
}

#[test]
fn reverse_iteration_matches_forward() {
    let dir = temp_dir("reverse");
    let opts = Options {
        mem_table_size: 16 * 1024, // force several flushes + levels
        ..Default::default()
    };
    let db = Db::open(&dir, opts).unwrap();
    for i in 0..1500u32 {
        db.set(format!("k{i:05}").as_bytes(), format!("v{i}").as_bytes())
            .unwrap();
    }
    // Overwrite some so multiple versions exist across levels.
    for i in 0..200u32 {
        db.set(format!("k{i:05}").as_bytes(), b"new").unwrap();
    }
    db.delete(b"k00500").unwrap();

    let forward = collect(&db);
    let mut it = db.iter().unwrap();
    let mut reverse = collect_reverse(&mut it);
    reverse.reverse();
    assert_eq!(forward, reverse, "reverse iteration must mirror forward");
    assert_eq!(forward.len(), 1499); // 1500 written, 1 deleted
    assert_eq!(db.get(b"k00000").unwrap(), Some(b"new".to_vec()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn seek_and_bounds() {
    let dir = temp_dir("seek-bounds");
    let db = Db::open(&dir, Options::default()).unwrap();
    for i in 0..100u32 {
        db.set(format!("k{i:03}").as_bytes(), format!("v{i}").as_bytes())
            .unwrap();
    }
    db.flush().unwrap();

    // seek_ge lands on the first key >= target.
    let mut it = db.iter().unwrap();
    it.seek_ge(b"k042").unwrap();
    assert!(it.valid());
    assert_eq!(it.key(), b"k042");
    it.next().unwrap();
    assert_eq!(it.key(), b"k043");

    // seek_ge to a gap rounds up.
    it.seek_ge(b"k0419").unwrap();
    assert_eq!(it.key(), b"k042");

    // seek_lt lands on the last key < target.
    it.seek_lt(b"k042").unwrap();
    assert_eq!(it.key(), b"k041");
    it.prev().unwrap();
    assert_eq!(it.key(), b"k040");

    // Direction change: seek_ge then prev.
    it.seek_ge(b"k050").unwrap();
    it.prev().unwrap();
    assert_eq!(it.key(), b"k049");
    it.next().unwrap();
    assert_eq!(it.key(), b"k050");

    // Bounds restrict the visible range.
    let mut bit = db
        .iter_with_options(IterOptions {
            lower_bound: Some(b"k010".to_vec()),
            upper_bound: Some(b"k020".to_vec()),
            ..Default::default()
        })
        .unwrap();
    bit.first().unwrap();
    assert_eq!(bit.key(), b"k010");
    let mut keys = Vec::new();
    while bit.valid() {
        keys.push(String::from_utf8(bit.key().to_vec()).unwrap());
        bit.next().unwrap();
    }
    assert_eq!(keys.first().unwrap(), "k010");
    assert_eq!(keys.last().unwrap(), "k019"); // upper bound exclusive
    assert_eq!(keys.len(), 10);

    // last() honors the upper bound.
    bit.last().unwrap();
    assert_eq!(bit.key(), b"k019");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn prefix_iteration() {
    let dir = temp_dir("prefix");
    let db = Db::open(&dir, Options::default()).unwrap();
    for k in ["aa1", "aa2", "ab1", "ba1", "bb1"] {
        db.set(k.as_bytes(), b"x").unwrap();
    }
    db.flush().unwrap();

    let mut it = db.iter().unwrap();
    it.seek_prefix_ge(b"aa", b"aa").unwrap();
    let mut keys = Vec::new();
    while it.valid() {
        keys.push(String::from_utf8(it.key().to_vec()).unwrap());
        it.next().unwrap();
    }
    assert_eq!(keys, ["aa1", "aa2"]); // stops at the prefix boundary

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn runs_fully_in_memory_on_memfs() {
    // The whole open/flush/compact/reopen lifecycle on an in-memory filesystem, never
    // touching disk. Cloning the MemFs shares the underlying tree, so a reopen sees the
    // same files.
    let fs = Arc::new(MemFs::new());
    let dir = "/db";
    let opts = || Options {
        fs: fs.clone(),
        mem_table_size: 16 * 1024,
        ..Default::default()
    };
    {
        let db = Db::open(dir, opts()).unwrap();
        for i in 0..1000u32 {
            db.set(format!("k{i:05}").as_bytes(), format!("v{i}").as_bytes())
                .unwrap();
        }
        db.delete(b"k00042").unwrap();
        db.flush().unwrap();
        assert_eq!(db.get(b"k00000").unwrap(), Some(b"v0".to_vec()));
        assert_eq!(db.get(b"k00042").unwrap(), None);
    }
    // Reopen against the same in-memory tree; data survives the WAL/MANIFEST round-trip.
    {
        let db = Db::open(dir, opts()).unwrap();
        assert_eq!(db.get(b"k00000").unwrap(), Some(b"v0".to_vec()));
        assert_eq!(db.get(b"k00999").unwrap(), Some(b"v999".to_vec()));
        assert_eq!(db.get(b"k00042").unwrap(), None);
        let all = collect(&db);
        assert_eq!(all.len(), 999);
    }
}

#[test]
fn directory_lock_blocks_second_open() {
    let dir = temp_dir("lock");
    let db = Db::open(&dir, Options::default()).unwrap();
    db.set(b"k", b"v").unwrap();
    // A second read-write open of the same directory must fail while the first holds the
    // exclusive lock.
    assert!(
        Db::open(&dir, Options::default()).is_err(),
        "second open should be blocked by the directory lock"
    );
    drop(db);
    // Once the first handle is dropped, the lock is released and reopening succeeds.
    let db2 = Db::open(&dir, Options::default()).unwrap();
    assert_eq!(db2.get(b"k").unwrap(), Some(b"v".to_vec()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn checkpoint_produces_openable_copy() {
    let dir = temp_dir("ckpt-src");
    let dest = temp_dir("ckpt-dst");
    {
        let db = Db::open(&dir, Options::default()).unwrap();
        for i in 0..300u32 {
            db.set(format!("k{i:04}").as_bytes(), format!("v{i}").as_bytes())
                .unwrap();
        }
        db.delete(b"k0100").unwrap();
        db.checkpoint(&dest).unwrap();
        // The source remains usable after checkpointing.
        assert_eq!(db.get(b"k0000").unwrap(), Some(b"v0".to_vec()));
    }
    // The checkpoint opens as a standalone database with the same contents.
    {
        let db = Db::open(&dest, Options::default()).unwrap();
        assert_eq!(db.get(b"k0000").unwrap(), Some(b"v0".to_vec()));
        assert_eq!(db.get(b"k0299").unwrap(), Some(b"v299".to_vec()));
        assert_eq!(db.get(b"k0100").unwrap(), None);
        assert_eq!(collect(&db).len(), 299);
    }
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&dest);
}

#[test]
fn ingest_external_sstable() {
    use pebbledb::base::internal_key::{InternalKey, InternalKeyKind};
    use pebbledb::sstable::{Writer, WriterOptions};

    let dir = temp_dir("ingest");
    let db = Db::open(&dir, Options::default()).unwrap();
    // Pre-existing data, including a key the ingest will shadow.
    db.set(b"apple", b"old").unwrap();
    db.set(b"cherry", b"keep").unwrap();
    db.flush().unwrap();

    // Build an external sstable (keys at seqnum 0, as an offline builder would produce).
    let ext = dir.join("external.sst");
    {
        let f = std::fs::File::create(&ext).unwrap();
        let cmp = std::sync::Arc::new(pebbledb::DefaultComparer);
        let mut w = Writer::new(f, cmp, WriterOptions::default());
        for (k, v) in [("apple", "new"), ("banana", "fresh")] {
            let ikey = InternalKey::new(k.as_bytes().to_vec(), 0, InternalKeyKind::Set).encode();
            w.add(&ikey, v.as_bytes()).unwrap();
        }
        w.finish().unwrap();
    }

    db.ingest(&[&ext]).unwrap();

    // The ingested values win over older data and add new keys; untouched keys remain.
    assert_eq!(db.get(b"apple").unwrap(), Some(b"new".to_vec()));
    assert_eq!(db.get(b"banana").unwrap(), Some(b"fresh".to_vec()));
    assert_eq!(db.get(b"cherry").unwrap(), Some(b"keep".to_vec()));

    // And it survives a reopen.
    drop(db);
    let db = Db::open(&dir, Options::default()).unwrap();
    assert_eq!(db.get(b"apple").unwrap(), Some(b"new".to_vec()));
    assert_eq!(db.get(b"banana").unwrap(), Some(b"fresh".to_vec()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn ingest_wins_over_overlapping_unflushed_memtable_key() {
    use pebbledb::base::internal_key::{InternalKey, InternalKeyKind};
    use pebbledb::sstable::{Writer, WriterOptions};

    let dir = temp_dir("ingest-overlap");
    let db = Db::open(&dir, Options::default()).unwrap();
    // A key sits in the *active memtable* (never flushed) when we ingest a newer value for it.
    db.set(b"k", b"memtable-old").unwrap();

    let ext = dir.join("external.sst");
    {
        let f = std::fs::File::create(&ext).unwrap();
        let cmp = std::sync::Arc::new(pebbledb::DefaultComparer);
        let mut w = Writer::new(f, cmp, WriterOptions::default());
        let ik = InternalKey::new(b"k".to_vec(), 0, InternalKeyKind::Set).encode();
        w.add(&ik, b"ingested-new").unwrap();
        w.finish().unwrap();
    }
    // Ingest without a manual flush: the ingested value is newer and must win, even though an
    // older version of the key is still in the memtable.
    db.ingest(&[&ext]).unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(b"ingested-new".to_vec()));

    // Survives reopen.
    drop(db);
    let db = Db::open(&dir, Options::default()).unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(b"ingested-new".to_vec()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn ingest_with_blob_separation() {
    use pebbledb::base::internal_key::{InternalKey, InternalKeyKind};
    use pebbledb::sstable::{Writer, WriterOptions};

    let dir = temp_dir("ingest-blob");
    // The ingesting store separates large values into blob files.
    let db = Db::open(
        &dir,
        Options {
            blob_value_threshold: Some(64),
            ..Default::default()
        },
    )
    .unwrap();

    // An external sstable carrying a large and a small value.
    let ext = dir.join("external.sst");
    let big = vec![b'Z'; 500];
    {
        let f = std::fs::File::create(&ext).unwrap();
        let cmp = std::sync::Arc::new(pebbledb::DefaultComparer);
        let mut w = Writer::new(f, cmp, WriterOptions::default());
        let ik =
            |k: &str| InternalKey::new(k.as_bytes().to_vec(), 0, InternalKeyKind::Set).encode();
        w.add(&ik("big"), &big).unwrap();
        w.add(&ik("small"), b"v").unwrap();
        w.finish().unwrap();
    }

    db.ingest(&[&ext]).unwrap();

    // The rewritten (ingested) table separated its large value into a blob file.
    let blobs = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|x| x == "blob"))
        .count();
    assert!(
        blobs >= 1,
        "ingest should write a blob file for large values"
    );

    assert_eq!(db.get(b"big").unwrap(), Some(big.clone()));
    assert_eq!(db.get(b"small").unwrap(), Some(b"v".to_vec()));

    // Survives reopen (blob references resolve after recovery).
    drop(db);
    let db = Db::open(
        &dir,
        Options {
            blob_value_threshold: Some(64),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(db.get(b"big").unwrap(), Some(big));
    assert_eq!(db.get(b"small").unwrap(), Some(b"v".to_vec()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn format_major_version_persists_and_ratchets() {
    use pebbledb::FormatMajorVersion;

    let dir = temp_dir("fmv");
    // Create at the most-compatible format.
    {
        let db = Db::open(
            &dir,
            Options {
                format_major_version: FormatMajorVersion::MOST_COMPATIBLE,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            db.format_major_version(),
            FormatMajorVersion::MOST_COMPATIBLE
        );
        db.set(b"k", b"v").unwrap();
        // Ratchet upward; a lower target is a no-op.
        db.ratchet_format_major_version(FormatMajorVersion::MOST_COMPATIBLE)
            .unwrap();
        db.ratchet_format_major_version(FormatMajorVersion::VALUE_BLOCKS)
            .unwrap();
        assert_eq!(db.format_major_version(), FormatMajorVersion::VALUE_BLOCKS);
    }
    // The ratcheted version is recovered from the OPTIONS file on reopen, ignoring the
    // option passed for fresh stores.
    {
        let db = Db::open(
            &dir,
            Options {
                format_major_version: FormatMajorVersion::MOST_COMPATIBLE,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(db.format_major_version(), FormatMajorVersion::VALUE_BLOCKS);
        assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn open_rejects_comparer_mismatch() {
    use pebbledb::{Comparer, DefaultComparer};
    use std::cmp::Ordering;

    // A comparer that behaves bytewise but reports a different name.
    struct Renamed(DefaultComparer);
    impl Comparer for Renamed {
        fn name(&self) -> &str {
            "test.RenamedComparator"
        }
        fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
            self.0.compare(a, b)
        }
        fn abbreviated_key(&self, key: &[u8]) -> u64 {
            self.0.abbreviated_key(key)
        }
        fn separator(&self, dst: &mut Vec<u8>, a: &[u8], b: &[u8]) {
            self.0.separator(dst, a, b)
        }
        fn successor(&self, dst: &mut Vec<u8>, a: &[u8]) {
            self.0.successor(dst, a)
        }
    }

    let dir = temp_dir("cmp-mismatch");
    {
        let db = Db::open(&dir, Options::default()).unwrap(); // default comparer name
        db.set(b"k", b"v").unwrap();
    }
    // Reopening with a differently-named comparer must fail validation.
    let err = Db::open(
        &dir,
        Options {
            comparer: Arc::new(Renamed(DefaultComparer)),
            ..Default::default()
        },
    );
    assert!(err.is_err(), "open with mismatched comparer should fail");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn comparer_registry_resolves_recorded_comparer() {
    use pebbledb::{Comparer, DefaultComparer};
    use std::cmp::Ordering;

    // A bytewise comparer reporting a non-default name.
    struct Renamed(DefaultComparer);
    impl Comparer for Renamed {
        fn name(&self) -> &str {
            "test.RegistryComparator"
        }
        fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
            self.0.compare(a, b)
        }
        fn abbreviated_key(&self, key: &[u8]) -> u64 {
            self.0.abbreviated_key(key)
        }
        fn separator(&self, dst: &mut Vec<u8>, a: &[u8], b: &[u8]) {
            self.0.separator(dst, a, b)
        }
        fn successor(&self, dst: &mut Vec<u8>, a: &[u8]) {
            self.0.successor(dst, a)
        }
    }

    let dir = temp_dir("cmp-registry");
    {
        // Create the store with the custom comparer as the primary comparer.
        let db = Db::open(
            &dir,
            Options {
                comparer: Arc::new(Renamed(DefaultComparer)),
                ..Default::default()
            },
        )
        .unwrap();
        db.set(b"k", b"v").unwrap();
        db.flush().unwrap();
    }

    // Reopen with the DEFAULT comparer primary, but the custom one in the registry: the
    // recorded name is resolved from `comparers`, so the open succeeds and data reads back.
    {
        let db = Db::open(
            &dir,
            Options {
                comparer: Arc::new(DefaultComparer),
                comparers: vec![Arc::new(Renamed(DefaultComparer))],
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
    }

    // Reopen with neither the matching comparer nor a registry entry: open must fail.
    let err = Db::open(
        &dir,
        Options {
            comparer: Arc::new(DefaultComparer),
            ..Default::default()
        },
    );
    assert!(
        err.is_err(),
        "open without the recorded comparer should fail"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// `Iterator::next_prefix` skips the remaining versions sharing the current key's prefix and
/// lands on the first key of the next prefix (Pebble's `NextPrefix`). Verified against a
/// suffix-aware (MVCC-style `prefix@suffix`) comparer, and that it equals `next` under the default
/// comparer (where every key is its own prefix).
#[test]
fn next_prefix_skips_versions_within_a_prefix() {
    use pebbledb::{Comparer, DefaultComparer};
    use std::cmp::Ordering;

    struct AtComparer(DefaultComparer);
    impl Comparer for AtComparer {
        fn name(&self) -> &str {
            "test.AtComparator"
        }
        fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
            self.0.compare(a, b)
        }
        fn abbreviated_key(&self, key: &[u8]) -> u64 {
            self.0.abbreviated_key(key)
        }
        fn split(&self, key: &[u8]) -> usize {
            key.iter().rposition(|&b| b == b'@').unwrap_or(key.len())
        }
        fn separator(&self, dst: &mut Vec<u8>, a: &[u8], b: &[u8]) {
            self.0.separator(dst, a, b)
        }
        fn successor(&self, dst: &mut Vec<u8>, a: &[u8]) {
            self.0.successor(dst, a)
        }
    }

    let dir = temp_dir("next-prefix");
    let db = Db::open(
        &dir,
        Options {
            comparer: Arc::new(AtComparer(DefaultComparer)),
            ..Default::default()
        },
    )
    .unwrap();
    // Three prefixes, several versions each (distinct user keys under this comparer).
    for k in [b"a@1".as_ref(), b"a@5", b"a@9", b"b@1", b"b@2", b"c@1"] {
        db.set(k, b"v").unwrap();
    }

    // next_prefix from the first key of each prefix lands on the first key of the next prefix.
    let mut it = db.iter().unwrap();
    let mut out = Vec::new();
    it.first().unwrap();
    while it.valid() {
        out.push(it.key().to_vec());
        it.next_prefix().unwrap();
    }
    assert_eq!(
        out,
        vec![b"a@1".to_vec(), b"b@1".to_vec(), b"c@1".to_vec()],
        "next_prefix should surface one key per prefix"
    );

    // A plain next() walks every version, confirming the data has multiple versions per prefix.
    let mut it = db.iter().unwrap();
    let mut all = Vec::new();
    it.first().unwrap();
    while it.valid() {
        all.push(it.key().to_vec());
        it.next().unwrap();
    }
    assert_eq!(all.len(), 6, "next() walks every version");

    let _ = std::fs::remove_dir_all(&dir);
}

/// `Iterator::stats` counts positioning work (Pebble's `Iterator.Stats`): forward/reverse seeks
/// and steps at the call level, plus the internal key versions scanned (which exceeds the user
/// keys surfaced when keys have multiple versions). `reset_stats` zeroes the counters.
#[test]
fn iterator_stats_count_positioning_work() {
    let dir = temp_dir("iter-stats");
    let db = Db::open(&dir, Options::default()).unwrap();
    // Three user keys, each overwritten twice (three internal versions apiece) in one memtable.
    for _ in 0..3 {
        for k in [b"a".as_ref(), b"b", b"c"] {
            db.set(k, b"v").unwrap();
        }
    }

    let mut it = db.iter().unwrap();
    it.first().unwrap(); // 1 forward seek
    let mut steps = 0;
    while it.valid() {
        it.next().unwrap(); // forward steps (incl. the one off the end)
        steps += 1;
    }
    let s = it.stats();
    assert_eq!(s.forward_seek_count, 1);
    assert_eq!(s.forward_step_count, steps);
    assert_eq!(s.reverse_seek_count, 0);
    // Three user keys × three versions = nine internal keys scanned.
    assert_eq!(s.internal_keys, 9, "internal versions scanned");

    it.reset_stats();
    assert_eq!(it.stats(), pebbledb::IteratorStats::default());
    it.last().unwrap(); // 1 reverse seek
    assert_eq!(it.stats().reverse_seek_count, 1);
    assert_eq!(it.stats().forward_seek_count, 0);

    let _ = std::fs::remove_dir_all(&dir);
}

/// The cooperative limited-iteration family (Pebble's `*WithLimit`): a step that lands at or
/// beyond the limit returns `AtLimit` and holds the over-limit key, which a later step (with a
/// limit past it, no limit, or a plain `next`/`prev`) resumes from without re-reading.
#[test]
fn limited_iteration_pauses_and_resumes() {
    use pebbledb::IterValidity::{AtLimit, Exhausted, Valid};

    let dir = temp_dir("with-limit");
    let db = Db::open(&dir, Options::default()).unwrap();
    for k in [b"a".as_ref(), b"b", b"c", b"d", b"e"] {
        db.set(k, b"v").unwrap();
    }

    // Forward with a moving limit, including a pause at "c" resumed by a higher limit.
    let mut it = db.iter().unwrap();
    it.first().unwrap();
    assert_eq!(it.key(), b"a");
    assert_eq!(it.next_with_limit(Some(b"c")).unwrap(), Valid); // b < c
    assert_eq!(it.key(), b"b");
    assert_eq!(it.next_with_limit(Some(b"c")).unwrap(), AtLimit); // c >= c, paused
    assert!(!it.valid());
    assert_eq!(it.next_with_limit(Some(b"z")).unwrap(), Valid); // resume c
    assert_eq!(it.key(), b"c");
    assert_eq!(it.next_with_limit(None).unwrap(), Valid); // d, no limit
    assert_eq!(it.key(), b"d");
    assert_eq!(it.next_with_limit(None).unwrap(), Valid); // e
    assert_eq!(it.next_with_limit(None).unwrap(), Exhausted);

    // A plain next() resumes a forward pause in place.
    let mut it = db.iter().unwrap();
    it.first().unwrap();
    assert_eq!(it.next_with_limit(Some(b"b")).unwrap(), AtLimit); // b paused
    assert!(!it.valid());
    it.next().unwrap();
    assert!(it.valid() && it.key() == b"b");
    it.next().unwrap();
    assert_eq!(it.key(), b"c");

    // Reverse with a limit (bounds the step from below): pause when the prev key drops below it.
    let mut it = db.iter().unwrap();
    it.last().unwrap();
    assert_eq!(it.key(), b"e");
    assert_eq!(it.prev_with_limit(Some(b"c")).unwrap(), Valid); // d >= c
    assert_eq!(it.key(), b"d");
    assert_eq!(it.prev_with_limit(Some(b"c")).unwrap(), Valid); // c >= c
    assert_eq!(it.key(), b"c");
    assert_eq!(it.prev_with_limit(Some(b"c")).unwrap(), AtLimit); // b < c, paused
    assert!(!it.valid());
    assert_eq!(it.prev_with_limit(Some(b"a")).unwrap(), Valid); // resume b
    assert_eq!(it.key(), b"b");

    // Seek variants apply the same limit check on landing.
    let mut it = db.iter().unwrap();
    assert_eq!(it.seek_ge_with_limit(b"b", Some(b"d")).unwrap(), Valid);
    assert_eq!(it.key(), b"b");
    assert_eq!(it.seek_ge_with_limit(b"d", Some(b"d")).unwrap(), AtLimit);
    assert!(!it.valid());
    assert_eq!(it.seek_lt_with_limit(b"d", Some(b"b")).unwrap(), Valid); // c >= b
    assert_eq!(it.key(), b"c");
    assert_eq!(it.seek_lt_with_limit(b"b", Some(b"b")).unwrap(), AtLimit); // a < b
    assert!(!it.valid());

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn range_key_masking_hides_older_point_versions() {
    use pebbledb::{Comparer, DefaultComparer};
    use std::cmp::Ordering;

    // An MVCC-ish comparer: a user key is `prefix@suffix`; `split` returns the prefix length
    // so the suffix (including the `@`) is `key[split..]`. Ordering stays bytewise, which is
    // all the masking mechanism needs (it compares suffix byte slices via the comparer).
    struct AtComparer(DefaultComparer);
    impl Comparer for AtComparer {
        fn name(&self) -> &str {
            "test.AtComparator"
        }
        fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
            self.0.compare(a, b)
        }
        fn abbreviated_key(&self, key: &[u8]) -> u64 {
            self.0.abbreviated_key(key)
        }
        fn split(&self, key: &[u8]) -> usize {
            // Prefix length = index of the last '@' (suffix includes the '@'); whole key if none.
            key.iter().rposition(|&b| b == b'@').unwrap_or(key.len())
        }
        fn separator(&self, dst: &mut Vec<u8>, a: &[u8], b: &[u8]) {
            self.0.separator(dst, a, b)
        }
        fn successor(&self, dst: &mut Vec<u8>, a: &[u8]) {
            self.0.successor(dst, a)
        }
    }

    let dir = temp_dir("rk-mask");
    let db = Db::open(
        &dir,
        Options {
            comparer: Arc::new(AtComparer(DefaultComparer)),
            ..Default::default()
        },
    )
    .unwrap();

    // Five point versions across three prefixes.
    for k in [b"a@1".as_ref(), b"a@5", b"b@1", b"b@9", b"c@1"] {
        db.set(k, b"v").unwrap();
    }
    // A masking range key over ["a","c") at suffix "@5".
    db.range_key_set(b"a", b"c", b"@5", b"rk").unwrap();

    let collect = |opts: IterOptions| -> Vec<Vec<u8>> {
        let mut it = db.iter_with_options(opts).unwrap();
        let mut out = Vec::new();
        it.first().unwrap();
        while it.valid() {
            out.push(it.key().to_vec());
            it.next().unwrap();
        }
        out
    };

    // Without masking, every point version is visible.
    assert_eq!(
        collect(IterOptions::default()),
        vec![
            b"a@1".to_vec(),
            b"a@5".to_vec(),
            b"b@1".to_vec(),
            b"b@9".to_vec(),
            b"c@1".to_vec()
        ],
    );

    // With masking at "@1", the active mask suffix is "@5" (the only covering range-key
    // suffix, and it is >= "@1"). Point keys covered by ["a","c") whose suffix sorts after
    // "@5" are hidden: only "b@9". "c@1" is outside the range (end exclusive), so it survives.
    assert_eq!(
        collect(IterOptions {
            range_key_masking_suffix: Some(b"@1".to_vec()),
            ..Default::default()
        }),
        vec![
            b"a@1".to_vec(),
            b"a@5".to_vec(),
            b"b@1".to_vec(),
            b"c@1".to_vec()
        ],
    );

    // Reverse iteration applies the same masking.
    let mut rev = Vec::new();
    let mut it = db
        .iter_with_options(IterOptions {
            range_key_masking_suffix: Some(b"@1".to_vec()),
            ..Default::default()
        })
        .unwrap();
    it.last().unwrap();
    while it.valid() {
        rev.push(it.key().to_vec());
        it.prev().unwrap();
    }
    assert_eq!(
        rev,
        vec![
            b"c@1".to_vec(),
            b"b@1".to_vec(),
            b"a@5".to_vec(),
            b"a@1".to_vec()
        ],
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn read_triggered_compaction_moves_a_passed_through_file() {
    // Low threshold so a handful of reads trigger it.
    let dir = temp_dir("read-triggered");
    let db = Db::open(
        &dir,
        Options {
            read_compaction_threshold: 4,
            // The four disjoint flushes below should merge by file count (one sublevel).
            l0_compaction_file_threshold: 4,
            ..Default::default()
        },
    )
    .unwrap();

    // Seed key "m" at the bottom level.
    db.set(b"m", b"v").unwrap();
    db.flush().unwrap();
    db.compact_range(None, None).unwrap();

    // Build an L1 file whose range [a..z] spans "m" but which contains no "m": four L0 flushes
    // of keys around "m" reach the file-count trigger and merge into a single L1 file.
    for k in [b"a".as_ref(), b"b", b"c", b"z"] {
        db.set(k, b"v").unwrap();
        db.flush().unwrap();
    }
    let m = db.metrics();
    assert_eq!(
        m.level_files[1], 1,
        "expected one L1 file spanning m: {:?}",
        m.level_files
    );

    // Reading "m" repeatedly passes through that L1 file (it spans "m" but lacks it) to reach
    // the bottom — charging wasted seeks until the file crosses the threshold and is queued.
    for _ in 0..6 {
        assert_eq!(db.get(b"m").unwrap(), Some(b"v".to_vec()));
    }
    // A synchronous flush drains the read-compaction queue (the background worker may also
    // have); either way the passed-through L1 file is compacted down off L1.
    db.set(b"trigger", b"v").unwrap();
    db.flush().unwrap();

    let m2 = db.metrics();
    assert_eq!(
        m2.level_files[1], 0,
        "the passed-through file should be moved off L1 by read-triggered compaction: {:?}",
        m2.level_files
    );
    // All data still readable.
    assert_eq!(db.get(b"m").unwrap(), Some(b"v".to_vec()));
    assert_eq!(db.get(b"a").unwrap(), Some(b"v".to_vec()));
    assert_eq!(db.get(b"z").unwrap(), Some(b"v".to_vec()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn tombstone_density_compaction_drains_a_dense_file() {
    let dir = temp_dir("tombstone-density");
    let db = Db::open(
        &dir,
        Options {
            // Merge the four flushes by file count into one dense L1 file (one sublevel).
            l0_compaction_file_threshold: 4,
            ..Default::default()
        },
    )
    .unwrap();

    // Four L0 flushes — including a batch of deletes — reach the L0 file-count trigger (4),
    // so the score picker merges them into a single L1 file. Because L1 is not the bottom
    // level, the 50 deletes are retained, making that L1 file tombstone-dense (~49%).
    for i in 0..100u32 {
        db.set(format!("k{i:03}").as_bytes(), b"v").unwrap();
    }
    db.flush().unwrap();
    for i in 0..50u32 {
        db.delete(format!("k{i:03}").as_bytes()).unwrap();
    }
    db.flush().unwrap();
    for tag in ["x1", "x2"] {
        db.set(tag.as_bytes(), b"v").unwrap();
        db.flush().unwrap();
    }

    // The score picker leaves L1 within budget, so only the tombstone-density pass acts on the
    // dense L1 file — pushing it down to the bottom level (everything above it is now empty).
    let m = db.metrics();
    assert_eq!(m.level_files[0], 0, "L0 drained: {:?}", m.level_files);
    assert_eq!(
        *m.level_files.last().unwrap(),
        1,
        "dense file pushed to the bottom level: {:?}",
        m.level_files
    );
    let above_bottom: usize = m.level_files[..m.level_files.len() - 1].iter().sum();
    assert_eq!(
        above_bottom, 0,
        "nothing should remain above the bottom: {:?}",
        m.level_files
    );

    // One more flush lets the elision-only pass drop the now-bottom tombstones.
    db.set(b"y1", b"v").unwrap();
    db.flush().unwrap();

    // Deletes were applied; survivors remain.
    assert_eq!(db.get(b"k000").unwrap(), None);
    assert_eq!(db.get(b"k049").unwrap(), None);
    assert_eq!(db.get(b"k050").unwrap(), Some(b"v".to_vec()));
    assert_eq!(db.get(b"k099").unwrap(), Some(b"v".to_vec()));
    assert_eq!(db.get(b"x1").unwrap(), Some(b"v".to_vec()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn elision_only_compaction_drops_bottom_tombstones() {
    use pebbledb::DefaultComparer;
    use pebbledb::sstable::Reader;

    // Total point tombstones recorded across every sstable currently on disk.
    let total_deletions = |dir: &std::path::Path| -> u64 {
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "sst"))
            .map(|p| {
                let bytes = std::fs::read(&p).unwrap();
                let r = Reader::open(bytes, Arc::new(DefaultComparer)).unwrap();
                r.properties().num_deletions
            })
            .sum()
    };

    let dir = temp_dir("elision");
    let db = Db::open(&dir, Options::default()).unwrap();

    // One sstable carrying a live key plus a point tombstone, moved to the bottom level.
    db.set(b"a", b"v").unwrap();
    db.delete(b"z").unwrap();
    db.flush().unwrap();
    db.compact_range(None, None).unwrap(); // single file, no overlap → moves down, tombstone kept

    // Writing another key triggers a flush whose maybe_compact runs the elision-only pass,
    // rewriting the bottom file without its now-dead tombstone. The concurrent background
    // scheduler may also run this pass on its own and defers obsolete-file deletion until no
    // reader references the old file, so the elided state is reached asynchronously — poll for
    // it rather than asserting a single transient moment.
    db.set(b"m", b"v").unwrap();
    db.flush().unwrap();

    let mut elided = false;
    for _ in 0..200 {
        if total_deletions(&dir) == 0 {
            elided = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    assert!(
        elided,
        "the bottom-level tombstone should have been elided (no point deletions remain on disk)"
    );
    // Data is unchanged by the rewrite.
    assert_eq!(db.get(b"a").unwrap(), Some(b"v".to_vec()));
    assert_eq!(db.get(b"m").unwrap(), Some(b"v".to_vec()));
    assert_eq!(db.get(b"z").unwrap(), None);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn delete_only_compaction_drops_shadowed_files() {
    let dir = temp_dir("delete-only");
    let db = Db::open(&dir, Options::default()).unwrap();

    // File A: 100 keys pushed to the bottom level (a single non-overlapping file → moves).
    for i in 0..100u32 {
        db.set(format!("k{i:03}").as_bytes(), b"v").unwrap();
    }
    db.compact_range(None, None).unwrap();

    // Exactly one sstable exists now — that's A. Capture its filename.
    let ssts: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".sst"))
        .collect();
    assert_eq!(ssts.len(), 1, "expected one sstable (A): {ssts:?}");
    let a_name = ssts.into_iter().next().unwrap();
    assert!(db.get(b"k050").unwrap().is_some());

    // A covering range tombstone (newer than A) plus a survivor key outside the span. The
    // flush runs maybe_compact, whose delete-only pass should drop A entirely.
    db.set(b"zzz", b"v").unwrap();
    db.delete_range(b"k000", b"k999").unwrap();
    db.flush().unwrap();

    // A's file is gone from disk (dropped, not rewritten); its keys read as deleted; the
    // out-of-range survivor remains.
    assert!(
        !dir.join(&a_name).exists(),
        "file A ({a_name}) should be dropped by delete-only compaction"
    );
    assert_eq!(db.get(b"k050").unwrap(), None);
    assert_eq!(db.get(b"zzz").unwrap(), Some(b"v".to_vec()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn move_compaction_relevels_without_rewriting() {
    let dir = temp_dir("move-compact");
    let db = Db::open(&dir, Options::default()).unwrap();
    for i in 0..50u32 {
        db.set(format!("k{i:03}").as_bytes(), b"v").unwrap();
    }
    db.flush().unwrap();

    let sst_names = |dir: &std::path::Path| -> Vec<String> {
        let mut v: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.ends_with(".sst"))
            .collect();
        v.sort();
        v
    };
    let before = sst_names(&dir);
    assert_eq!(before.len(), 1, "one L0 sstable expected: {before:?}");

    // Compact down: with a single non-overlapping file, each level step is a move, so the
    // very same sstable file (same number/name) is reused at its new level — not rewritten.
    db.compact_range(None, None).unwrap();
    let after = sst_names(&dir);
    assert_eq!(
        after, before,
        "move must preserve the sstable file: {before:?} -> {after:?}"
    );

    // Data survives the moves, and now lives below L0.
    assert_eq!(db.get(b"k025").unwrap(), Some(b"v".to_vec()));
    let m = db.metrics();
    assert_eq!(
        m.level_files[0], 0,
        "L0 should be empty after move: {:?}",
        m.level_files
    );
    assert_eq!(m.level_files[1..].iter().sum::<usize>(), 1);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn only_durable_iterator_excludes_memtable() {
    let dir = temp_dir("durable");
    let db = Db::open(&dir, Options::default()).unwrap();

    // Flush one key to an sstable (durable), keep another only in the memtable.
    db.set(b"flushed", b"v").unwrap();
    db.flush().unwrap();
    db.set(b"memonly", b"v").unwrap();

    let durable_keys = |db: &Db| -> Vec<Vec<u8>> {
        let mut it = db
            .iter_with_options(IterOptions {
                only_durable: true,
                ..Default::default()
            })
            .unwrap();
        let mut out = Vec::new();
        it.first().unwrap();
        while it.valid() {
            out.push(it.key().to_vec());
            it.next().unwrap();
        }
        out
    };

    // A normal iterator sees both; the durable-only iterator sees only the flushed key.
    assert_eq!(collect(&db).len(), 2);
    assert_eq!(durable_keys(&db), vec![b"flushed".to_vec()]);

    // After flushing the memtable, the durable view includes the previously memtable-only key.
    db.flush().unwrap();
    assert_eq!(
        durable_keys(&db),
        vec![b"flushed".to_vec(), b"memonly".to_vec()]
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn block_property_filter_skips_nonmatching_sstables() {
    use pebbledb::base::internal_key::encoded_user_key;
    use pebbledb::sstable::blockprop::{BlockPropertyCollector, BlockPropertyFilter};

    // A collector recording each table's [min, max] user key as two length-prefixed slices.
    struct MinMaxKey {
        min: Option<Vec<u8>>,
        max: Option<Vec<u8>>,
    }
    impl BlockPropertyCollector for MinMaxKey {
        fn name(&self) -> &str {
            "test.minmaxkey"
        }
        fn add(&mut self, ik: &[u8], _v: &[u8]) {
            let k = encoded_user_key(ik).to_vec();
            if self.min.as_ref().is_none_or(|m| &k < m) {
                self.min = Some(k.clone());
            }
            if self.max.as_ref().is_none_or(|m| &k > m) {
                self.max = Some(k);
            }
        }
        fn finish(&mut self) -> Vec<u8> {
            let mut out = Vec::new();
            for s in [self.min.take().unwrap(), self.max.take().unwrap()] {
                out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                out.extend_from_slice(&s);
            }
            out
        }
    }
    // A filter: does the table's [min, max] intersect the query range [lo, hi)?
    struct RangeFilter {
        lo: Vec<u8>,
        hi: Vec<u8>,
    }
    impl BlockPropertyFilter for RangeFilter {
        fn name(&self) -> &str {
            "test.minmaxkey"
        }
        fn intersects(&self, prop: &[u8]) -> bool {
            let rd = |off: usize| -> (Vec<u8>, usize) {
                let n = u32::from_le_bytes(prop[off..off + 4].try_into().unwrap()) as usize;
                (prop[off + 4..off + 4 + n].to_vec(), off + 4 + n)
            };
            let (min, o) = rd(0);
            let (max, _) = rd(o);
            min < self.hi && max >= self.lo
        }
    }

    let dir = temp_dir("bpc-filter");
    let db = Db::open(
        &dir,
        Options {
            block_property_collectors: vec![Arc::new(|| {
                Box::new(MinMaxKey {
                    min: None,
                    max: None,
                })
            })],
            ..Default::default()
        },
    )
    .unwrap();

    // Two flushes produce two L0 sstables with disjoint key ranges.
    for k in [b"d".as_ref(), b"h", b"m", b"q"] {
        db.set(k, b"v").unwrap();
    }
    db.flush().unwrap();
    for k in [b"t".as_ref(), b"w", b"z"] {
        db.set(k, b"v").unwrap();
    }
    db.flush().unwrap();

    let collect = |opts: IterOptions| -> Vec<Vec<u8>> {
        let mut it = db.iter_with_options(opts).unwrap();
        let mut out = Vec::new();
        it.first().unwrap();
        while it.valid() {
            out.push(it.key().to_vec());
            it.next().unwrap();
        }
        out
    };

    // No filter: all seven keys.
    assert_eq!(collect(IterOptions::default()).len(), 7);

    // Filter [a, r): intersects the first table (d..q) but not the second (t..z), so only the
    // first table's point keys are produced.
    let filtered = collect(IterOptions {
        block_property_filters: vec![Arc::new(RangeFilter {
            lo: b"a".to_vec(),
            hi: b"r".to_vec(),
        })],
        ..Default::default()
    });
    assert_eq!(
        filtered,
        vec![b"d".to_vec(), b"h".to_vec(), b"m".to_vec(), b"q".to_vec()],
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn eventually_file_only_snapshot_is_consistent_and_scoped() {
    let dir = temp_dir("efos");
    let db = Db::open(&dir, Options::default()).unwrap();
    for i in 0..30u32 {
        db.set(format!("k{i:03}").as_bytes(), b"v1").unwrap();
    }

    // An EFOS over [k005, k015) flushes and pins the current view.
    let efos = db
        .new_eventually_file_only_snapshot(vec![(b"k005".to_vec(), b"k015".to_vec())])
        .unwrap();

    // Mutate after the snapshot; the EFOS keeps the pre-mutation view.
    for i in 0..30u32 {
        db.set(format!("k{i:03}").as_bytes(), b"v2").unwrap();
    }

    // Reads inside the span see the consistent (pre-mutation) value.
    assert_eq!(efos.get(b"k010").unwrap(), Some(b"v1".to_vec()));
    // Reads outside the registered spans are rejected.
    assert!(efos.get(b"k000").is_err());
    assert!(efos.get(b"k020").is_err());

    // A span iterator yields the consistent view over the registered range.
    let mut it = efos.iter_span(b"k005", b"k015").unwrap();
    it.first().unwrap();
    let mut got = Vec::new();
    while it.valid() {
        got.push((
            String::from_utf8_lossy(it.key()).into_owned(),
            String::from_utf8_lossy(it.value()).into_owned(),
        ));
        it.next().unwrap();
    }
    assert_eq!(got.len(), 10); // k005..k014
    assert!(got.iter().all(|(_, v)| v == "v1"));
    assert_eq!(got[0].0, "k005");

    // Iterating outside the registered span is rejected.
    assert!(efos.iter_span(b"k000", b"k030").is_err());

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn large_batch_exceeding_memtable_commits_and_recovers() {
    let dir = temp_dir("large-batch");
    let opts = || Options {
        mem_table_size: 16 * 1024, // small arena; the batch is far larger
        ..Default::default()
    };
    let db = Db::open(&dir, opts()).unwrap();

    // One batch whose contents dwarf the memtable arena. Without flushable-batch handling
    // this overflows the arena and fails.
    let mut batch = pebbledb::Batch::new();
    for i in 0..2000u32 {
        batch.set(format!("k{i:05}").as_bytes(), &[b'v'; 64]);
    }
    db.write(batch).unwrap();

    // All keys are readable immediately (the batch's flushable is part of the read view).
    for i in 0..2000u32 {
        assert_eq!(
            db.get(format!("k{i:05}").as_bytes()).unwrap(),
            Some(vec![b'v'; 64]),
            "key k{i:05} missing after large batch"
        );
    }
    // Interleave smaller writes (these go to the regular active memtable) and re-check.
    db.set(b"small", b"x").unwrap();
    assert_eq!(db.get(b"small").unwrap(), Some(b"x".to_vec()));
    assert_eq!(db.get(b"k00000").unwrap(), Some(vec![b'v'; 64]));

    // Survives reopen (WAL recovery reconstructs the batch).
    drop(db);
    let db = Db::open(&dir, opts()).unwrap();
    for i in (0..2000u32).step_by(53) {
        assert_eq!(
            db.get(format!("k{i:05}").as_bytes()).unwrap(),
            Some(vec![b'v'; 64]),
            "key k{i:05} missing after reopen"
        );
    }
    assert_eq!(db.get(b"small").unwrap(), Some(b"x".to_vec()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn metrics_report_per_op_latencies() {
    let dir = temp_dir("op-latencies");
    let db = Db::open(
        &dir,
        Options {
            mem_table_size: 4 * 1024, // force flushes (and thus compactions)
            ..Default::default()
        },
    )
    .unwrap();
    for i in 0..500u32 {
        db.set(format!("k{i:04}").as_bytes(), &[b'v'; 32]).unwrap();
    }
    db.flush().unwrap();
    db.compact_range(None, None).unwrap();
    for i in 0..500u32 {
        let _ = db.get(format!("k{i:04}").as_bytes()).unwrap();
    }

    let m = db.metrics();
    // Each op type was exercised, so its count is non-zero.
    assert!(
        m.latencies.commit.count >= 500,
        "commits: {:?}",
        m.latencies.commit
    );
    assert!(m.latencies.get.count >= 500, "gets: {:?}", m.latencies.get);
    assert!(
        m.latencies.flush.count >= 1,
        "flushes: {:?}",
        m.latencies.flush
    );
    assert!(
        m.latencies.compaction.count >= 1,
        "compactions: {:?}",
        m.latencies.compaction
    );
    // avg never exceeds max for a populated stat.
    assert!(m.latencies.get.avg <= m.latencies.get.max);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn prefix_bloom_seek_is_correct_with_a_prefix_comparer() {
    use pebbledb::{Comparer, DefaultComparer};
    use std::cmp::Ordering as Ord;

    // A prefix-aware comparer: a key is "<prefix>#<suffix>"; the prefix is the part before the
    // first '#'. Ordering/index helpers delegate to bytewise. Declaring an extractor makes the
    // writer build the bloom over prefixes and the reader skip tables on a prefix seek.
    struct PrefixCmp(DefaultComparer);
    impl Comparer for PrefixCmp {
        fn name(&self) -> &str {
            "test.prefix"
        }
        fn compare(&self, a: &[u8], b: &[u8]) -> Ord {
            self.0.compare(a, b)
        }
        fn abbreviated_key(&self, k: &[u8]) -> u64 {
            self.0.abbreviated_key(k)
        }
        fn split(&self, key: &[u8]) -> usize {
            key.iter().position(|&b| b == b'#').unwrap_or(key.len())
        }
        fn separator(&self, dst: &mut Vec<u8>, a: &[u8], b: &[u8]) {
            self.0.separator(dst, a, b)
        }
        fn successor(&self, dst: &mut Vec<u8>, a: &[u8]) {
            self.0.successor(dst, a)
        }
        fn prefix_extractor_name(&self) -> Option<&str> {
            Some("test.prefix")
        }
    }

    let dir = temp_dir("prefix-bloom");
    let opts = || Options {
        comparer: Arc::new(PrefixCmp(DefaultComparer)),
        mem_table_size: 4 * 1024,
        ..Default::default()
    };
    let db = Db::open(&dir, opts()).unwrap();
    // Write several prefixes, each flushed to its own sstable so a prefix seek must locate the
    // right table(s) — and skip the others via their prefix bloom.
    for p in ["aaa", "bbb", "ccc", "ddd", "eee"] {
        for i in 0..20u32 {
            db.set(format!("{p}#{i:03}").as_bytes(), b"v").unwrap();
        }
        db.flush().unwrap();
    }

    let collect_prefix = |db: &Db, prefix: &[u8]| -> Vec<Vec<u8>> {
        let mut it = db.iter().unwrap();
        it.seek_prefix_ge(prefix, prefix).unwrap();
        let mut out = Vec::new();
        while it.valid() {
            out.push(it.key().to_vec());
            it.next().unwrap();
        }
        out
    };

    // Present prefixes return exactly their keys (a wrong bloom skip would lose these).
    for p in ["aaa", "ccc", "eee"] {
        let got = collect_prefix(&db, p.as_bytes());
        assert_eq!(got.len(), 20, "prefix {p}");
        assert!(got.iter().all(|k| k.starts_with(p.as_bytes())));
    }
    // Absent prefixes return nothing.
    for p in ["aa0", "zzz", "fff"] {
        assert!(
            collect_prefix(&db, p.as_bytes()).is_empty(),
            "absent prefix {p} returned keys"
        );
    }
    // Point lookups still work (the bloom is prefix-scoped, so it never rules out a real key).
    assert_eq!(db.get(b"bbb#005").unwrap(), Some(b"v".to_vec()));
    assert_eq!(db.get(b"bbb#999").unwrap(), None);

    // Survives reopen with the same comparer.
    drop(db);
    let db = Db::open(&dir, opts()).unwrap();
    assert_eq!(collect_prefix(&db, b"ddd").len(), 20);
    assert_eq!(db.get(b"aaa#000").unwrap(), Some(b"v".to_vec()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn iterator_clone_is_independent() {
    let dir = temp_dir("iter-clone");
    let db = Db::open(&dir, Options::default()).unwrap();
    for i in 0..10u32 {
        db.set(format!("k{i:02}").as_bytes(), format!("v{i}").as_bytes())
            .unwrap();
    }

    let mut a = db.iter().unwrap();
    a.first().unwrap();
    a.next().unwrap(); // a at k01
    assert_eq!(a.key(), b"k01");

    // Clone mid-iteration: the clone starts at the same position.
    let mut b = a.clone();
    assert_eq!(b.key(), b"k01");

    // Advancing each independently does not affect the other.
    a.next().unwrap(); // a -> k02
    assert_eq!(a.key(), b"k02");
    assert_eq!(b.key(), b"k01"); // b unchanged

    b.next().unwrap();
    b.next().unwrap(); // b -> k03
    assert_eq!(b.key(), b"k03");
    assert_eq!(a.key(), b"k02"); // a unchanged

    // Each can run to completion on its own.
    let mut count_a = 0;
    while a.valid() {
        count_a += 1;
        a.next().unwrap();
    }
    assert_eq!(count_a, 8); // k02..k09

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn efos_survives_disjoint_excise_but_not_overlapping() {
    let dir = temp_dir("efos-excise");
    let db = Db::open(&dir, Options::default()).unwrap();
    for i in 0..30u32 {
        db.set(format!("k{i:03}").as_bytes(), b"v1").unwrap();
    }

    // EFOS scoped to [k005, k015).
    let efos = db
        .new_eventually_file_only_snapshot(vec![(b"k005".to_vec(), b"k015".to_vec())])
        .unwrap();
    assert!(!efos.is_invalidated());

    // An excise disjoint from the span [k020, k025) leaves the EFOS valid and readable.
    db.excise(b"k020", b"k025").unwrap();
    assert!(
        !efos.is_invalidated(),
        "disjoint excise must not invalidate"
    );
    assert_eq!(efos.get(b"k010").unwrap(), Some(b"v1".to_vec()));

    // An excise overlapping the span [k012, k018) invalidates it; reads then error.
    db.excise(b"k012", b"k018").unwrap();
    assert!(efos.is_invalidated(), "overlapping excise must invalidate");
    assert!(efos.get(b"k010").is_err());
    assert!(efos.iter_span(b"k005", b"k015").is_err());

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn checkpoint_restricted_to_spans() {
    use pebbledb::CheckpointOptions;

    let src = temp_dir("ckpt-src");
    let dst = temp_dir("ckpt-dst");
    let db = Db::open(
        &src,
        Options {
            mem_table_size: 16 * 1024, // several sstables across the key space
            ..Default::default()
        },
    )
    .unwrap();
    for i in 0..2000u32 {
        db.set(format!("k{i:05}").as_bytes(), &[b'v'; 32]).unwrap();
    }
    db.flush().unwrap();

    // Checkpoint only the sstables overlapping [k00000, k00500).
    db.checkpoint_with_options(
        &dst,
        &CheckpointOptions {
            spans: vec![(b"k00000".to_vec(), b"k00500".to_vec())],
            ..Default::default()
        },
    )
    .unwrap();
    drop(db);

    // The checkpoint opens as a self-contained database holding (at least) the requested span
    // and not the far end of the key space.
    let ck = Db::open_read_only(&dst, Options::default()).unwrap();
    assert_eq!(ck.get(b"k00000").unwrap(), Some(vec![b'v'; 32]));
    assert_eq!(ck.get(b"k00400").unwrap(), Some(vec![b'v'; 32]));
    assert_eq!(
        ck.get(b"k01999").unwrap(),
        None,
        "out-of-span data excluded"
    );

    let _ = std::fs::remove_dir_all(&src);
    let _ = std::fs::remove_dir_all(&dst);
}

#[test]
fn group_commit_is_durable_across_concurrent_writers_and_reopen() {
    // Many writer threads commit concurrently through the group-commit pipeline; after a
    // clean close every committed write must be recoverable from the WAL on reopen.
    let dir = temp_dir("group-commit");
    {
        let db = Arc::new(
            Db::open(
                &dir,
                Options {
                    mem_table_size: 32 * 1024,
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        let writers = 8;
        let per_writer = 500u32;
        let mut handles = Vec::new();
        for w in 0..writers {
            let db = Arc::clone(&db);
            handles.push(std::thread::spawn(move || {
                for i in 0..per_writer {
                    let k = format!("w{w:02}-k{i:04}");
                    db.set(k.as_bytes(), format!("v{i}").as_bytes()).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // Do NOT flush — the data lives only in the WAL + memtable, so reopen must recover it.
    }

    // Reopen: recovery replays the WAL written by the group-commit pipeline.
    let db = Db::open(&dir, Options::default()).unwrap();
    for w in 0..8 {
        for i in [0u32, 1, 250, 499] {
            let k = format!("w{w:02}-k{i:04}");
            assert_eq!(
                db.get(k.as_bytes()).unwrap(),
                Some(format!("v{i}").into_bytes()),
                "lost {k} across group-commit + reopen"
            );
        }
    }
    assert_eq!(collect(&db).len(), 8 * 500);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn concurrent_compactions_stay_correct() {
    // With several compaction workers, a write-heavy workload (many overwrites + deletes,
    // forcing constant flushing and compaction) must remain correct: all live keys readable,
    // deleted keys absent, and the data durable across a reopen.
    let dir = temp_dir("concurrent-compactions");
    {
        let db = Db::open(
            &dir,
            Options {
                mem_table_size: 16 * 1024,
                l1_max_bytes: 64 * 1024, // keep levels small so compactions fire often
                max_concurrent_compactions: 4,
                ..Default::default()
            },
        )
        .unwrap();
        // Several rounds overwriting a moderate keyspace, deleting a band each round.
        for round in 0..6u32 {
            for k in 0..1500u32 {
                db.set(
                    format!("k{k:05}").as_bytes(),
                    format!("r{round}").as_bytes(),
                )
                .unwrap();
            }
            for k in 0..200u32 {
                db.delete(format!("k{k:05}").as_bytes()).unwrap();
            }
        }
        // Final state: k00000..k00199 deleted; k00200..k01499 hold "r5".
        for k in [0u32, 100, 199] {
            assert_eq!(db.get(format!("k{k:05}").as_bytes()).unwrap(), None);
        }
        for k in [200u32, 800, 1499] {
            assert_eq!(
                db.get(format!("k{k:05}").as_bytes()).unwrap(),
                Some(b"r5".to_vec())
            );
        }
        assert_eq!(collect(&db).len(), 1300);
    }
    // Durable across reopen (the WAL/MANIFEST written under concurrent compaction recovers).
    let db = Db::open(&dir, Options::default()).unwrap();
    assert_eq!(db.get(b"k00000").unwrap(), None);
    assert_eq!(db.get(b"k01499").unwrap(), Some(b"r5".to_vec()));
    assert_eq!(collect(&db).len(), 1300);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn per_level_target_file_size_splits_more() {
    // A tiny per-level target for L1 makes L0->L1 compaction emit many small files, where the
    // default 2 MiB target would produce just one.
    let dir = temp_dir("per-level-target");
    let db = Db::open(
        &dir,
        Options {
            mem_table_size: 16 * 1024,
            // Override only L1's target to 4 KiB (index 0 = L0, unused).
            level_target_file_sizes: vec![0, 4 * 1024],
            // Drain the disjoint (single-sublevel) L0 flushes into L1 by file count.
            l0_compaction_file_threshold: 4,
            ..Default::default()
        },
    )
    .unwrap();
    for i in 0..3000u32 {
        db.set(format!("k{i:05}").as_bytes(), &[b'v'; 40]).unwrap();
    }
    db.flush().unwrap();

    let m = db.metrics();
    assert!(
        m.level_files[1] > 1,
        "L1 should be split into many small files by the per-level target: {:?}",
        m.level_files
    );
    // Data is intact.
    assert_eq!(db.get(b"k00000").unwrap(), Some(vec![b'v'; 40]));
    assert_eq!(db.get(b"k02999").unwrap(), Some(vec![b'v'; 40]));
    assert_eq!(collect(&db).len(), 3000);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn per_block_property_filter_skips_blocks() {
    use pebbledb::base::internal_key::encoded_user_key;
    use pebbledb::sstable::blockprop::{BlockPropertyCollector, BlockPropertyFilter};

    // A collector tracking [min, max] user key both per data block and per table.
    #[derive(Default)]
    struct MinMax {
        table: Option<(Vec<u8>, Vec<u8>)>,
        block: Option<(Vec<u8>, Vec<u8>)>,
    }
    fn enc(span: &Option<(Vec<u8>, Vec<u8>)>) -> Vec<u8> {
        let (min, max) = span.clone().unwrap_or_default();
        let mut out = Vec::new();
        out.extend_from_slice(&(min.len() as u32).to_le_bytes());
        out.extend_from_slice(&min);
        out.extend_from_slice(&max);
        out
    }
    fn upd(span: &mut Option<(Vec<u8>, Vec<u8>)>, k: &[u8]) {
        match span {
            None => *span = Some((k.to_vec(), k.to_vec())),
            Some((mn, mx)) => {
                if k < mn.as_slice() {
                    *mn = k.to_vec();
                }
                if k > mx.as_slice() {
                    *mx = k.to_vec();
                }
            }
        }
    }
    impl BlockPropertyCollector for MinMax {
        fn name(&self) -> &str {
            "test.minmax"
        }
        fn add(&mut self, ik: &[u8], _v: &[u8]) {
            let k = encoded_user_key(ik).to_vec();
            upd(&mut self.table, &k);
            upd(&mut self.block, &k);
        }
        fn finish(&mut self) -> Vec<u8> {
            enc(&self.table)
        }
        fn finish_data_block(&mut self) -> Vec<u8> {
            let out = enc(&self.block);
            self.block = None; // reset per-block accumulator; table state persists
            out
        }
    }
    struct RangeFilter {
        lo: Vec<u8>,
        hi: Vec<u8>,
    }
    impl BlockPropertyFilter for RangeFilter {
        fn name(&self) -> &str {
            "test.minmax"
        }
        fn intersects(&self, prop: &[u8]) -> bool {
            let n = u32::from_le_bytes(prop[0..4].try_into().unwrap()) as usize;
            let min = &prop[4..4 + n];
            let max = &prop[4 + n..];
            min < self.hi.as_slice() && max >= self.lo.as_slice()
        }
    }

    let dir = temp_dir("per-block-bpc");
    let db = Db::open(
        &dir,
        Options {
            mem_table_size: 8 << 20, // one memtable → one sstable with many blocks
            l0_compaction_threshold: 100,
            block_property_collectors: vec![Arc::new(|| Box::new(MinMax::default()))],
            ..Default::default()
        },
    )
    .unwrap();
    for i in 0..4000u32 {
        db.set(format!("k{i:05}").as_bytes(), &[b'v'; 8]).unwrap();
    }
    db.flush().unwrap();

    let collect_filtered = |lo: &[u8], hi: &[u8]| -> Vec<Vec<u8>> {
        let mut it = db
            .iter_with_options(IterOptions {
                block_property_filters: vec![Arc::new(RangeFilter {
                    lo: lo.to_vec(),
                    hi: hi.to_vec(),
                })],
                ..Default::default()
            })
            .unwrap();
        let mut out = Vec::new();
        it.first().unwrap();
        while it.valid() {
            out.push(it.key().to_vec());
            it.next().unwrap();
        }
        out
    };

    // A narrow filter near the start: the table passes table-level (its range spans the key),
    // but most data blocks are skipped, so far fewer than all 4000 keys come back.
    let got = collect_filtered(b"k00000", b"k00010");
    assert!(
        got.contains(&b"k00000".to_vec()),
        "first block's keys should be present"
    );
    assert!(
        !got.contains(&b"k03999".to_vec()),
        "a far block should be skipped"
    );
    assert!(
        got.len() < 4000,
        "block skipping should drop most keys, got {}",
        got.len()
    );

    // No filter returns everything (sanity).
    let mut all = db.iter().unwrap();
    all.first().unwrap();
    let mut n = 0;
    while all.valid() {
        n += 1;
        all.next().unwrap();
    }
    assert_eq!(n, 4000);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn l0_sublevels_reflect_overlap() {
    // Disjoint L0 files pack into one sublevel; overlapping ones each need their own.
    let opts = || Options {
        l0_compaction_threshold: 100, // keep L0 files from being compacted away
        ..Default::default()
    };

    // Four flushes of disjoint single keys → 4 non-overlapping L0 files → 1 sublevel.
    let dir = temp_dir("l0-disjoint");
    let db = Db::open(&dir, opts()).unwrap();
    for k in ["a", "b", "c", "d"] {
        db.set(k.as_bytes(), b"v").unwrap();
        db.flush().unwrap();
    }
    let m = db.metrics();
    assert_eq!(
        m.level_files[0], 4,
        "expected 4 L0 files: {:?}",
        m.level_files
    );
    assert_eq!(m.l0_sublevels, 1, "disjoint files pack into one sublevel");
    assert_eq!(m.read_amplification, 1);
    drop(db);
    let _ = std::fs::remove_dir_all(&dir);

    // Four flushes overwriting the same key → 4 fully-overlapping L0 files → 4 sublevels.
    let dir = temp_dir("l0-overlap");
    let db = Db::open(&dir, opts()).unwrap();
    for i in 0..4u32 {
        db.set(b"same", format!("v{i}").as_bytes()).unwrap();
        db.flush().unwrap();
    }
    let m = db.metrics();
    assert_eq!(m.level_files[0], 4);
    assert_eq!(m.l0_sublevels, 4, "overlapping files each need a sublevel");
    assert_eq!(m.read_amplification, 4);
    assert_eq!(db.get(b"same").unwrap(), Some(b"v3".to_vec()));
    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn value_separation_round_trips_large_values() {
    use pebbledb::DefaultComparer;
    use pebbledb::sstable::{Reader, TableFormat};

    let dir = temp_dir("value-sep");
    let db = Db::open(
        &dir,
        Options {
            mem_table_size: 64 * 1024,
            // Values >= 64 bytes are stored out-of-line in value blocks.
            value_block_threshold: Some(64),
            ..Default::default()
        },
    )
    .unwrap();

    // A mix of small (inline) and large (separated) values.
    for i in 0..200u32 {
        let v = if i % 2 == 0 {
            vec![b'B'; 500]
        } else {
            vec![b's'; 8]
        };
        db.set(format!("k{i:04}").as_bytes(), &v).unwrap();
    }
    db.flush().unwrap();
    db.compact_range(None, None).unwrap();

    // The engine wrote tables in the value-block-capable format (Pebble v3).
    let mut saw_v3 = false;
    for e in std::fs::read_dir(&dir).unwrap().filter_map(|e| e.ok()) {
        let p = e.path();
        if p.extension().is_some_and(|x| x == "sst") {
            let bytes = std::fs::read(&p).unwrap();
            let r = Reader::open(bytes, Arc::new(DefaultComparer)).unwrap();
            if r.format() == TableFormat::Pebble(3) {
                saw_v3 = true;
            }
        }
    }
    assert!(
        saw_v3,
        "value separation should write the value-block format"
    );

    // Every value reads back correctly through flush + compaction.
    for i in 0..200u32 {
        let want = if i % 2 == 0 {
            vec![b'B'; 500]
        } else {
            vec![b's'; 8]
        };
        assert_eq!(
            db.get(format!("k{i:04}").as_bytes()).unwrap(),
            Some(want),
            "value mismatch at {i}"
        );
    }
    // Survives reopen too.
    drop(db);
    let db = Db::open(&dir, Options::default()).unwrap();
    assert_eq!(db.get(b"k0000").unwrap(), Some(vec![b'B'; 500]));
    assert_eq!(db.get(b"k0001").unwrap(), Some(vec![b's'; 8]));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn blob_files_separate_large_values_and_read_back() {
    let dir = temp_dir("blob-files");
    let big = |i: u32| vec![(b'A' as u32 + i % 26) as u8; 400];
    let small = |i: u32| format!("s{i}").into_bytes();

    let db = Db::open(
        &dir,
        Options {
            mem_table_size: 64 * 1024,
            // Values >= 64 bytes go to a separate .blob file (rather than value blocks).
            blob_value_threshold: Some(64),
            ..Default::default()
        },
    )
    .unwrap();

    for i in 0..150u32 {
        let v = if i % 2 == 0 { big(i) } else { small(i) };
        db.set(format!("k{i:04}").as_bytes(), &v).unwrap();
    }
    db.flush().unwrap();

    // The flush produced at least one blob file holding the large values.
    let blob_count = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|x| x == "blob"))
        .count();
    assert!(blob_count >= 1, "flush should have written a blob file");

    // Every value reads back correctly, resolving blob references for the large ones.
    let check_all = |db: &Db| {
        for i in 0..150u32 {
            let want = if i % 2 == 0 { big(i) } else { small(i) };
            assert_eq!(
                db.get(format!("k{i:04}").as_bytes()).unwrap(),
                Some(want),
                "value mismatch at {i}"
            );
        }
        // Iteration resolves blobs too.
        let mut it = db.iter().unwrap();
        it.first().unwrap();
        let mut n = 0;
        while it.valid() {
            n += 1;
            it.next().unwrap();
        }
        assert_eq!(n, 150);
    };
    check_all(&db);

    // Reopen: blob references resolve against the on-disk blob files after recovery.
    drop(db);
    let db = Db::open(
        &dir,
        Options {
            blob_value_threshold: Some(64),
            ..Default::default()
        },
    )
    .unwrap();
    check_all(&db);

    // Blob-file rewrite: compaction keeps large values in a (new) blob file rather than
    // re-inlining them into the sstable, and every value still reads correctly.
    db.compact_range(None, None).unwrap();
    check_all(&db);
    let blob_after = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|x| x == "blob"))
        .count();
    assert!(
        blob_after >= 1,
        "compaction should rewrite large values into a blob file, not re-inline them"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn blob_sharing_preserves_blob_files_across_compaction() {
    let dir = temp_dir("blob-share");
    let big = |i: u32| vec![b'a' + (i % 26) as u8; 600];
    let blob_names = |dir: &std::path::Path| -> std::collections::BTreeSet<String> {
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.ends_with(".blob"))
            .collect()
    };

    let opts = || Options {
        blob_value_threshold: Some(64),
        mem_table_size: 16 * 1024,
        ..Default::default()
    };
    let db = Db::open(&dir, opts()).unwrap();
    // Two flushes -> two L0 sstables, each with its own blob file.
    for i in 0..40u32 {
        db.set(format!("k{i:03}").as_bytes(), &big(i)).unwrap();
    }
    db.flush().unwrap();
    for i in 40..80u32 {
        db.set(format!("k{i:03}").as_bytes(), &big(i)).unwrap();
    }
    db.flush().unwrap();

    let blobs_before = blob_names(&dir);
    assert!(!blobs_before.is_empty(), "flush wrote blob files");

    // Compaction merges the sstables but PRESERVES references to the existing blob files
    // (cross-sstable sharing) — the values are not rewritten — so the original blob files
    // survive and no value rewrite occurs.
    db.compact_range(None, None).unwrap();
    let blobs_after = blob_names(&dir);
    assert!(
        blobs_before.iter().all(|b| blobs_after.contains(b)),
        "compaction should preserve (share) the existing blob files; before={blobs_before:?} after={blobs_after:?}"
    );

    // Values resolve through the preserved blob files, after compaction and after reopen.
    let check = |db: &Db| {
        for i in 0..80u32 {
            assert_eq!(
                db.get(format!("k{i:03}").as_bytes()).unwrap(),
                Some(big(i)),
                "value {i}"
            );
        }
    };
    check(&db);
    drop(db);
    let db = Db::open(&dir, opts()).unwrap();
    check(&db);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn disk_slow_events_are_routed_to_the_listener() {
    use pebbledb::EventListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[derive(Default)]
    struct Counter {
        slow: AtomicUsize,
    }
    impl EventListener for Counter {
        fn on_disk_slow(&self, _op: &str, _path: &std::path::Path, _d: Duration) {
            self.slow.fetch_add(1, Ordering::SeqCst);
        }
    }

    let dir = temp_dir("disk-slow");
    let counter = Arc::new(Counter::default());
    // A 1ns threshold makes every real filesystem op count as "slow", so any write routes an
    // on_disk_slow event — exercising the wiring deterministically without an artificial delay.
    let db = Db::open(
        &dir,
        Options {
            event_listener: Some(counter.clone()),
            disk_slow_threshold: Some(Duration::from_nanos(1)),
            ..Default::default()
        },
    )
    .unwrap();
    for i in 0..50u32 {
        db.set(format!("k{i:03}").as_bytes(), b"v").unwrap();
    }
    db.flush().unwrap();

    assert!(
        counter.slow.load(Ordering::SeqCst) > 0,
        "disk-slow events should be routed to the listener"
    );
    // Data is unaffected by the health-check wrapper.
    assert_eq!(db.get(b"k000").unwrap(), Some(b"v".to_vec()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn validate_sstables_and_stats_events() {
    use pebbledb::EventListener;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct Counter {
        validated_ok: AtomicUsize,
        stats_loaded: AtomicUsize,
    }
    impl EventListener for Counter {
        fn on_table_validated(&self, _file_num: u64, ok: bool) {
            if ok {
                self.validated_ok.fetch_add(1, Ordering::SeqCst);
            }
        }
        fn on_table_stats_loaded(&self, _tables: usize) {
            self.stats_loaded.fetch_add(1, Ordering::SeqCst);
        }
    }

    let dir = temp_dir("validate");
    let counter = Arc::new(Counter::default());
    let db = Db::open(
        &dir,
        Options {
            mem_table_size: 16 * 1024,
            event_listener: Some(counter.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    for i in 0..1000u32 {
        db.set(format!("k{i:05}").as_bytes(), &[b'v'; 48]).unwrap();
    }
    db.flush().unwrap();

    // Every live sstable validates cleanly.
    let failures = db.validate_sstables().unwrap();
    assert_eq!(failures, 0, "all sstables should validate");
    let m = db.metrics();
    assert_eq!(
        counter.validated_ok.load(Ordering::SeqCst),
        m.total_sstables,
        "one on_table_validated(ok) per live sstable"
    );

    // table_stats fires its loaded event.
    let _ = db.table_stats().unwrap();
    assert!(counter.stats_loaded.load(Ordering::SeqCst) >= 1);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn batch_reset_reuses_the_batch() {
    let dir = temp_dir("batch-reset");
    let db = Db::open(&dir, Options::default()).unwrap();

    let mut batch = Batch::new();
    batch.set(b"a", b"1");
    batch.set(b"b", b"2");
    db.write(batch.clone()).unwrap();

    // Reset clears the batch; reusing it after reset writes only the new ops.
    batch.reset();
    assert_eq!(batch.count(), 0);
    batch.set(b"c", b"3");
    db.write(batch).unwrap();

    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(db.get(b"c").unwrap(), Some(b"3".to_vec()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn deletion_pacing_defers_obsolete_file_removal() {
    let dir = temp_dir("delete-pacing");
    let db = Db::open(
        &dir,
        Options {
            mem_table_size: 16 * 1024,
            // 1 byte/sec: after the pacer deletes one file it sleeps for ~its size in seconds,
            // so the rest of a compaction's obsolete inputs stay queued.
            target_byte_deletion_rate: 1,
            // Build up levels by file count during the writes so the full compaction has many
            // input files to obsolete (disjoint flushes are a single sublevel otherwise).
            l0_compaction_file_threshold: 4,
            ..Default::default()
        },
    )
    .unwrap();
    for i in 0..2000u32 {
        db.set(format!("k{i:05}").as_bytes(), &[b'v'; 64]).unwrap();
    }
    db.flush().unwrap();
    // A full compaction obsoletes many input files, which are handed to the pacer.
    db.compact_range(None, None).unwrap();

    let m = db.metrics();
    assert!(
        m.obsolete_files_pending > 0,
        "paced deletions should leave obsolete files queued, got {}",
        m.obsolete_files_pending
    );
    // Reads are unaffected — they use the live version, not the obsolete inputs.
    assert_eq!(db.get(b"k00000").unwrap(), Some(vec![b'v'; 64]));
    assert_eq!(db.get(b"k01999").unwrap(), Some(vec![b'v'; 64]));
    assert_eq!(collect(&db).len(), 2000);

    // Dropping the database drains the queue (the pacer cleans the rest before exiting).
    drop(db);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn multilevel_compaction_folds_three_levels() {
    use pebbledb::Logger;
    use std::sync::Mutex;

    // Count multilevel-compaction log lines.
    struct CountLogger(Mutex<usize>);
    impl Logger for CountLogger {
        fn info(&self, msg: &str) {
            if msg.contains("multilevel compaction") {
                *self.0.lock().unwrap() += 1;
            }
        }
    }

    let dir = temp_dir("multilevel");
    let logger = Arc::new(CountLogger(Mutex::new(0)));
    // Tiny level budgets push data down into L2/L3 quickly, so a single-file L1/L2 compaction
    // finds overlap two levels down and folds all three levels together.
    let db = Db::open(
        &dir,
        Options {
            mem_table_size: 4 * 1024,
            target_file_size: 1024,
            l1_max_bytes: 1024,
            // Drain L0 by file count so data flows down into L1/L2/L3 for the fold.
            l0_compaction_file_threshold: 4,
            logger: Some(logger.clone()),
            ..Default::default()
        },
    )
    .unwrap();

    // A large keyspace with sizable values overflows the tiny budgets into L1, L2 and L3;
    // several overwrite rounds keep producing overlapping files at the upper levels.
    let value = |round: u32| format!("r{round:03}-{}", "x".repeat(40));
    for round in 0..3u32 {
        for k in 0..2000u32 {
            db.set(format!("k{k:05}").as_bytes(), value(round).as_bytes())
                .unwrap();
        }
    }
    db.flush().unwrap();
    db.compact_range(None, None).unwrap();

    assert!(
        *logger.0.lock().unwrap() >= 1,
        "expected at least one multilevel compaction"
    );
    // Final values are correct after all the compaction churn.
    assert_eq!(db.get(b"k00000").unwrap(), Some(value(2).into_bytes()));
    assert_eq!(db.get(b"k01999").unwrap(), Some(value(2).into_bytes()));
    assert_eq!(collect(&db).len(), 2000);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn flush_splitting_produces_multiple_l0_files() {
    // A large point-only memtable flushed with a small target file size splits into several
    // L0 sstables. A high L0 threshold keeps them at L0 so the split is observable.
    let dir = temp_dir("flush-split");
    let db = Db::open(
        &dir,
        Options {
            mem_table_size: 8 << 20,    // hold everything in one memtable
            target_file_size: 4 * 1024, // small, to force splits
            l0_compaction_threshold: 100,
            ..Default::default()
        },
    )
    .unwrap();

    for i in 0..4000u32 {
        // Distinct values limit compression so the output exceeds several target sizes.
        let v = format!("value-{i:08}-{i:08}-{i:08}");
        db.set(format!("k{i:06}").as_bytes(), v.as_bytes()).unwrap();
    }
    db.flush().unwrap();

    let sst_count = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|x| x == "sst"))
        .count();
    assert!(
        sst_count >= 2,
        "a large memtable should split into multiple L0 files, got {sst_count}"
    );
    let m = db.metrics();
    assert_eq!(
        m.level_files[0], sst_count,
        "all split files should be at L0: {:?}",
        m.level_files
    );

    // The split output reads back correctly across file boundaries.
    assert_eq!(
        db.get(b"k000000").unwrap(),
        Some(b"value-00000000-00000000-00000000".to_vec())
    );
    assert_eq!(
        db.get(b"k003999").unwrap(),
        Some(b"value-00003999-00003999-00003999".to_vec())
    );
    assert_eq!(collect(&db).len(), 4000);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn manual_compact_range_drains_upper_levels() {
    let dir = temp_dir("compact-range");
    let opts = Options {
        mem_table_size: 16 * 1024, // force multiple L0 files
        ..Default::default()
    };
    let db = Db::open(&dir, opts).unwrap();
    for i in 0..2000u32 {
        db.set(format!("k{i:05}").as_bytes(), format!("v{i}").as_bytes())
            .unwrap();
    }
    db.flush().unwrap();

    db.compact_range(None, None).unwrap();

    // After a full manual compaction L0 is empty and data has moved into deeper levels.
    let m = db.metrics();
    assert_eq!(
        m.level_files[0], 0,
        "L0 should be drained: {:?}",
        m.level_files
    );
    let deeper: usize = m.level_files[1..].iter().sum();
    assert!(deeper > 0, "data should live below L0: {:?}", m.level_files);

    // All data is still correct.
    assert_eq!(db.get(b"k00000").unwrap(), Some(b"v0".to_vec()));
    assert_eq!(db.get(b"k01999").unwrap(), Some(b"v1999".to_vec()));
    assert_eq!(collect(&db).len(), 2000);

    // A bounded range compaction is a no-op for correctness too.
    db.compact_range(Some(b"k00100"), Some(b"k00200")).unwrap();
    assert_eq!(db.get(b"k00150").unwrap(), Some(b"v150".to_vec()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn concurrent_writers_and_readers() {
    // Many writer threads share one Db (Arc), each owning a disjoint key range, while a
    // reader thread concurrently scans. The background flush/compaction worker runs the
    // whole time. Afterward every written key must be present exactly once.
    let dir = temp_dir("concurrent");
    let db = Arc::new(
        Db::open(
            &dir,
            Options {
                mem_table_size: 16 * 1024, // force background flushes during the run
                ..Default::default()
            },
        )
        .unwrap(),
    );

    let writers = 8;
    let per_writer = 1000u32;
    let mut handles = Vec::new();
    for w in 0..writers {
        let db = Arc::clone(&db);
        handles.push(std::thread::spawn(move || {
            for i in 0..per_writer {
                let k = format!("w{w:02}-k{i:05}");
                db.set(k.as_bytes(), format!("v{i}").as_bytes()).unwrap();
            }
        }));
    }
    // A concurrent reader: just exercises the read path under contention.
    let reader = {
        let db = Arc::clone(&db);
        std::thread::spawn(move || {
            for _ in 0..50 {
                let _ = db.get(b"w00-k00000").unwrap();
                let mut it = db.iter().unwrap();
                it.first().unwrap();
                let mut n = 0;
                while it.valid() {
                    n += 1;
                    it.next().unwrap();
                }
                let _ = n;
            }
        })
    };
    for h in handles {
        h.join().unwrap();
    }
    reader.join().unwrap();

    // Every key from every writer is present with the right value.
    for w in 0..writers {
        for i in [0u32, 1, 499, per_writer - 1] {
            let k = format!("w{w:02}-k{i:05}");
            assert_eq!(
                db.get(k.as_bytes()).unwrap(),
                Some(format!("v{i}").into_bytes()),
                "missing {k}"
            );
        }
    }
    assert_eq!(collect(&db).len(), (writers as usize) * per_writer as usize);

    let _ = std::fs::remove_dir_all(&dir);
}

/// A filesystem wrapper that injects write failures for files under a given directory
/// prefix once "tripped", used to exercise WAL failover. Reads always succeed.
mod fault {
    use std::io::{self, Write};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use pebbledb::vfs::{DirLock, Fs, MemFs, WritableFile};

    #[derive(Clone)]
    pub struct FaultFs {
        inner: MemFs,
        prefix: PathBuf,
        pub tripped: Arc<AtomicBool>,
    }

    impl FaultFs {
        pub fn new(prefix: impl Into<PathBuf>) -> FaultFs {
            FaultFs {
                inner: MemFs::new(),
                prefix: prefix.into(),
                tripped: Arc::new(AtomicBool::new(false)),
            }
        }
        fn faulting(&self, path: &Path) -> bool {
            path.starts_with(&self.prefix)
        }
    }

    struct FaultWritable {
        inner: Box<dyn WritableFile>,
        tripped: Arc<AtomicBool>,
        faulting: bool,
    }

    impl FaultWritable {
        fn check(&self) -> io::Result<()> {
            if self.faulting && self.tripped.load(Ordering::SeqCst) {
                Err(io::Error::other("injected WAL write failure"))
            } else {
                Ok(())
            }
        }
    }

    impl Write for FaultWritable {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.check()?;
            self.inner.write(buf)
        }
        fn flush(&mut self) -> io::Result<()> {
            self.check()?;
            self.inner.flush()
        }
    }

    impl WritableFile for FaultWritable {
        fn sync_all(&mut self) -> io::Result<()> {
            self.check()?;
            self.inner.sync_all()
        }
    }

    impl Fs for FaultFs {
        fn create(&self, path: &Path) -> io::Result<Box<dyn WritableFile>> {
            Ok(Box::new(FaultWritable {
                inner: self.inner.create(path)?,
                tripped: Arc::clone(&self.tripped),
                faulting: self.faulting(path),
            }))
        }
        fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
            self.inner.read(path)
        }
        fn remove(&self, path: &Path) -> io::Result<()> {
            self.inner.remove(path)
        }
        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            self.inner.rename(from, to)
        }
        fn list(&self, dir: &Path) -> io::Result<Vec<String>> {
            self.inner.list(dir)
        }
        fn create_dir_all(&self, path: &Path) -> io::Result<()> {
            self.inner.create_dir_all(path)
        }
        fn exists(&self, path: &Path) -> bool {
            self.inner.exists(path)
        }
        fn size(&self, path: &Path) -> io::Result<u64> {
            self.inner.size(path)
        }
        fn sync_dir(&self, dir: &Path) -> io::Result<()> {
            self.inner.sync_dir(dir)
        }
        fn lock(&self, path: &Path) -> io::Result<Box<dyn DirLock>> {
            self.inner.lock(path)
        }
    }
}

#[test]
fn wal_recycling_reuses_files_and_recovers() {
    let dir = temp_dir("wal-recycle");
    let opts = || Options {
        wal_recycle_limit: 2,
        mem_table_size: 4 * 1024, // tiny → frequent rotations, flushes, and recycling
        ..Default::default()
    };
    {
        let db = Db::open(&dir, opts()).unwrap();
        // A large first wave forces many rotations + flushes: obsolete WAL files go into the
        // recycle pool and are reused in place for later WALs.
        for i in 0..300u32 {
            db.set(
                format!("k{i:04}").as_bytes(),
                format!("value-{i}-xxxxxxxxxxxxxxxx").as_bytes(),
            )
            .unwrap();
        }
        db.flush().unwrap();
        // A second wave lands in (now recycled) WALs and is intentionally left un-flushed.
        for i in 300..360u32 {
            db.set(
                format!("k{i:04}").as_bytes(),
                format!("value-{i}-yy").as_bytes(),
            )
            .unwrap();
        }
        // Drop without an explicit flush: the second wave survives only in the WAL(s).
    }

    // Reopen: recovery replays the un-flushed WALs — recycled files included — stopping cleanly
    // at each stale tail. Every key must be present with the right value.
    let db = Db::open(&dir, opts()).unwrap();
    for i in 0..360u32 {
        let want = if i < 300 {
            format!("value-{i}-xxxxxxxxxxxxxxxx")
        } else {
            format!("value-{i}-yy")
        };
        assert_eq!(
            db.get(format!("k{i:04}").as_bytes()).unwrap(),
            Some(want.into_bytes()),
            "key k{i:04} missing or wrong after recycle + recover"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn wal_failover_to_secondary_directory() {
    use std::sync::atomic::Ordering;

    let fs = fault::FaultFs::new("/db/wal-primary");
    let opts = || Options {
        fs: Arc::new(fs.clone()),
        wal_dir: Some("/db/wal-primary".into()),
        wal_failover_dir: Some("/db/wal-secondary".into()),
        ..Default::default()
    };

    {
        let db = Db::open("/db/store", opts()).unwrap();
        // These writes go to the primary WAL.
        db.set(b"a", b"1").unwrap();
        db.set(b"b", b"2").unwrap();
        // Trip the primary WAL directory: subsequent WAL writes fail and the engine fails
        // over to the secondary directory, re-logging the batch there.
        fs.tripped.store(true, Ordering::SeqCst);
        db.set(b"c", b"3").unwrap();
        db.set(b"d", b"4").unwrap();
        // All four are readable in the live database.
        for (k, v) in [
            (&b"a"[..], &b"1"[..]),
            (b"b", b"2"),
            (b"c", b"3"),
            (b"d", b"4"),
        ] {
            assert_eq!(db.get(k).unwrap().as_deref(), Some(v));
        }
    }

    // Reopen (primary readable again): recovery scans both WAL directories and recovers
    // the pre-failover and post-failover batches alike.
    {
        fs.tripped.store(false, Ordering::SeqCst);
        let db = Db::open("/db/store", opts()).unwrap();
        for (k, v) in [
            (&b"a"[..], &b"1"[..]),
            (b"b", b"2"),
            (b"c", b"3"),
            (b"d", b"4"),
        ] {
            assert_eq!(db.get(k).unwrap().as_deref(), Some(v), "key after reopen");
        }
    }
}

#[test]
fn estimate_disk_usage_tracks_range() {
    let dir = temp_dir("disk-usage");
    let opts = Options {
        mem_table_size: 16 * 1024, // several sstables across the key space
        ..Default::default()
    };
    let db = Db::open(&dir, opts).unwrap();
    for i in 0..3000u32 {
        db.set(format!("k{i:05}").as_bytes(), &[b'v'; 64]).unwrap();
    }
    db.flush().unwrap();

    let full = db.estimate_disk_usage(b"k00000", b"k99999");
    assert!(full > 0, "full-range usage should be positive");

    // A sub-range estimates less than the whole, and an empty range is ~0.
    let part = db.estimate_disk_usage(b"k00000", b"k01000");
    assert!(
        part <= full,
        "sub-range {part} should not exceed full {full}"
    );
    let none = db.estimate_disk_usage(b"zzzz0", b"zzzz9");
    assert_eq!(none, 0, "disjoint range should estimate zero");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn logger_receives_messages_and_archive_cleaner_keeps_files() {
    use pebbledb::{ArchiveCleaner, Logger};
    use std::sync::Mutex;

    struct VecLogger(Mutex<Vec<String>>);
    impl Logger for VecLogger {
        fn info(&self, msg: &str) {
            self.0.lock().unwrap().push(msg.to_string());
        }
    }

    let dir = temp_dir("logger-archive");
    let archive = dir.join("archive");
    let logger = Arc::new(VecLogger(Mutex::new(Vec::new())));
    let opts = Options {
        mem_table_size: 16 * 1024,
        logger: Some(logger.clone()),
        cleaner: Arc::new(ArchiveCleaner {
            dir: archive.clone(),
        }),
        ..Default::default()
    };
    {
        let db = Db::open(&dir, opts).unwrap();
        for i in 0..2000u32 {
            db.set(format!("k{i:05}").as_bytes(), &[b'v'; 32]).unwrap();
        }
        db.flush().unwrap();
        db.compact_range(None, None).unwrap(); // produce obsolete inputs to archive
    }

    // The logger saw flush messages.
    let msgs = logger.0.lock().unwrap();
    assert!(
        msgs.iter().any(|m| m.contains("flushed memtable")),
        "expected flush log messages, got {msgs:?}"
    );

    // The archive cleaner moved obsolete files there instead of deleting them.
    let archived = std::fs::read_dir(&archive)
        .map(|rd| rd.count())
        .unwrap_or(0);
    assert!(archived > 0, "archive dir should hold obsolete files");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn indexed_batch_reads_its_own_writes() {
    let dir = temp_dir("indexed-batch");
    let db = Db::open(&dir, Options::default()).unwrap();
    db.set(b"a", b"committed-a").unwrap();
    db.set(b"b", b"committed-b").unwrap();
    db.set(b"old", b"present").unwrap();

    let mut ib = db.indexed_batch();
    ib.set(b"b", b"batch-b"); // overrides committed
    ib.set(b"c", b"batch-c"); // new key not in db
    ib.delete(b"a"); // hides committed
    ib.delete_range(b"old", b"oldz"); // range-deletes a committed key

    // Reads through the batch see its pending writes merged over the DB.
    assert_eq!(ib.get(&db, b"a").unwrap(), None); // deleted in batch
    assert_eq!(ib.get(&db, b"b").unwrap(), Some(b"batch-b".to_vec())); // overridden
    assert_eq!(ib.get(&db, b"c").unwrap(), Some(b"batch-c".to_vec())); // batch-only
    assert_eq!(ib.get(&db, b"old").unwrap(), None); // range-deleted
    // A committed key untouched by the batch still reads through.
    db.set(b"d", b"committed-d").unwrap();
    assert_eq!(ib.get(&db, b"d").unwrap(), Some(b"committed-d".to_vec()));

    // The merged scan reflects the same view, in sorted order.
    let scan = ib.scan(&db).unwrap();
    let got: Vec<(String, String)> = scan
        .iter()
        .map(|(k, v)| {
            (
                String::from_utf8_lossy(k).into_owned(),
                String::from_utf8_lossy(v).into_owned(),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            ("b".into(), "batch-b".into()),
            ("c".into(), "batch-c".into()),
            ("d".into(), "committed-d".into()),
        ]
    );

    // The DB itself is unchanged until the batch is committed.
    assert_eq!(db.get(b"a").unwrap(), Some(b"committed-a".to_vec()));
    db.write(ib.into_batch()).unwrap();
    assert_eq!(db.get(b"a").unwrap(), None);
    assert_eq!(db.get(b"b").unwrap(), Some(b"batch-b".to_vec()));
    assert_eq!(db.get(b"c").unwrap(), Some(b"batch-c".to_vec()));
    assert_eq!(db.get(b"old").unwrap(), None);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn indexed_batch_merges_over_committed_value() {
    use pebbledb::ConcatMerger;
    let dir = temp_dir("indexed-merge");
    let opts = Options {
        merger: Some(Arc::new(ConcatMerger)),
        ..Default::default()
    };
    let db = Db::open(&dir, opts).unwrap();
    db.set(b"k", b"base").unwrap();

    let mut ib = db.indexed_batch();
    ib.merge(b"k", b"+1");
    ib.merge(b"k", b"+2");
    // ConcatMerger appends operands to the existing value.
    assert_eq!(ib.get(&db, b"k").unwrap(), Some(b"base+1+2".to_vec()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn indexed_batch_iterator_merges_lazily_over_committed() {
    use pebbledb::ConcatMerger;
    let dir = temp_dir("indexed-batch-iter");
    let opts = Options {
        merger: Some(Arc::new(ConcatMerger)),
        ..Default::default()
    };
    let db = Db::open(&dir, opts).unwrap();
    db.set(b"a", b"committed-a").unwrap();
    db.set(b"b", b"committed-b").unwrap();
    db.set(b"m", b"base").unwrap();
    db.set(b"old", b"present").unwrap();

    let mut ib = db.indexed_batch();
    ib.set(b"b", b"batch-b"); // overrides committed
    ib.set(b"c", b"batch-c"); // new key not in db
    ib.delete(b"a"); // hides committed
    ib.merge(b"m", b"+x"); // folds over committed base via the merger
    ib.delete_range(b"old", b"oldz"); // range-deletes a committed key

    // Forward iteration over the lazy iterator reflects the merged view, in sorted order,
    // without materializing it.
    let mut it = ib.iter(&db).unwrap();
    it.first().unwrap();
    let mut fwd = Vec::new();
    while it.valid() {
        fwd.push((
            String::from_utf8_lossy(it.key()).into_owned(),
            String::from_utf8_lossy(it.value()).into_owned(),
        ));
        it.next().unwrap();
    }
    assert_eq!(
        fwd,
        vec![
            ("b".into(), "batch-b".into()),
            ("c".into(), "batch-c".into()),
            ("m".into(), "base+x".into()),
        ]
    );

    // Reverse iteration yields the same set in the opposite order.
    it.last().unwrap();
    let mut rev = Vec::new();
    while it.valid() {
        rev.push(String::from_utf8_lossy(it.key()).into_owned());
        it.prev().unwrap();
    }
    assert_eq!(rev, vec!["m", "c", "b"]);

    // Bounds apply through IterOptions.
    let mut it = ib
        .iter_with_options(
            &db,
            pebbledb::IterOptions {
                lower_bound: Some(b"c".to_vec()),
                upper_bound: Some(b"n".to_vec()),
                ..Default::default()
            },
        )
        .unwrap();
    it.first().unwrap();
    let mut bounded = Vec::new();
    while it.valid() {
        bounded.push(String::from_utf8_lossy(it.key()).into_owned());
        it.next().unwrap();
    }
    assert_eq!(bounded, vec!["c", "m"]);

    // The DB is unchanged until commit.
    assert_eq!(db.get(b"a").unwrap(), Some(b"committed-a".to_vec()));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn external_iter_merges_sstables_without_ingesting() {
    use pebbledb::base::internal_key::{InternalKey, InternalKeyKind};
    use pebbledb::new_external_iter;
    use pebbledb::sstable::{Writer, WriterOptions};

    let dir = temp_dir("external-iter");
    std::fs::create_dir_all(&dir).unwrap();
    let cmp = Arc::new(pebbledb::DefaultComparer);

    // Two sstables with interleaved and overlapping keys; the second (later in the list)
    // is treated as newer, so its value for "b" wins.
    let make = |path: &std::path::Path, kvs: &[(&str, &str)], seq: u64| {
        let f = std::fs::File::create(path).unwrap();
        let mut w = Writer::new(f, cmp.clone(), WriterOptions::default());
        for (k, v) in kvs {
            let ik = InternalKey::new(k.as_bytes().to_vec(), seq, InternalKeyKind::Set).encode();
            w.add(&ik, v.as_bytes()).unwrap();
        }
        w.finish().unwrap();
    };
    let f1 = dir.join("a.sst");
    let f2 = dir.join("b.sst");
    make(&f1, &[("a", "1"), ("b", "old"), ("d", "4")], 10);
    make(&f2, &[("b", "new"), ("c", "3")], 20);

    let opts = Options::default();
    let mut it = new_external_iter(&opts, &[f1, f2]).unwrap();
    it.first().unwrap();
    let mut got = Vec::new();
    while it.valid() {
        got.push((
            String::from_utf8_lossy(it.key()).into_owned(),
            String::from_utf8_lossy(it.value()).into_owned(),
        ));
        it.next().unwrap();
    }
    assert_eq!(
        got,
        vec![
            ("a".into(), "1".into()),
            ("b".into(), "new".into()), // newer file wins
            ("c".into(), "3".into()),
            ("d".into(), "4".into()),
        ]
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn single_delete_delete_sized_and_log_data() {
    let dir = temp_dir("del-variants");
    let db = Db::open(&dir, Options::default()).unwrap();
    db.set(b"s", b"v").unwrap();
    db.set(b"z", b"zz").unwrap();

    // single_delete and delete_sized both read back as absent.
    db.single_delete(b"s").unwrap();
    assert_eq!(db.get(b"s").unwrap(), None);
    db.delete_sized(b"z", 2).unwrap();
    assert_eq!(db.get(b"z").unwrap(), None);

    // log_data is a WAL-only marker; it does not change the keyspace.
    db.set(b"keep", b"1").unwrap();
    db.log_data(b"app-marker").unwrap();
    assert_eq!(db.get(b"keep").unwrap(), Some(b"1".to_vec()));

    // All of it survives flush + reopen.
    db.flush().unwrap();
    drop(db);
    let db = Db::open(&dir, Options::default()).unwrap();
    assert_eq!(db.get(b"s").unwrap(), None);
    assert_eq!(db.get(b"z").unwrap(), None);
    assert_eq!(db.get(b"keep").unwrap(), Some(b"1".to_vec()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn iterator_surfaces_range_keys_at_position() {
    let dir = temp_dir("iter-rangekeys");
    let db = Db::open(&dir, Options::default()).unwrap();
    db.set(b"a", b"1").unwrap();
    db.set(b"m", b"2").unwrap();
    db.set(b"z", b"3").unwrap();
    // A range key covering [b, p) at suffix "@t".
    db.range_key_set(b"b", b"p", b"@t", b"rkval").unwrap();

    let mut it = db.iter().unwrap();
    it.first().unwrap();
    while it.valid() {
        let rks = it.range_keys();
        match it.key() {
            b"a" => assert!(rks.is_empty(), "a is before the range key"),
            b"m" => {
                assert_eq!(rks.len(), 1, "m is covered by the range key");
                assert_eq!(rks[0].start, b"b");
                assert_eq!(rks[0].end().unwrap(), b"p");
            }
            b"z" => assert!(rks.is_empty(), "z is after the range key"),
            other => panic!("unexpected key {other:?}"),
        }
        it.next().unwrap();
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn scan_internal_exposes_all_versions_and_tombstones() {
    use pebbledb::base::internal_key::{InternalKeyKind, encoded_user_key, trailer_kind};

    let dir = temp_dir("scan-internal");
    let db = Db::open(&dir, Options::default()).unwrap();
    db.set(b"k", b"v1").unwrap();
    db.set(b"k", b"v2").unwrap(); // a newer version of the same key
    db.delete(b"gone").unwrap(); // a tombstone
    db.range_key_set(b"a", b"c", b"@1", b"rk").unwrap();
    let mut batch = Batch::new();
    batch.delete_range(b"x", b"y");
    db.write(batch).unwrap();

    let scan = db.scan_internal().unwrap();

    // Both versions of "k" are present (a plain iterator would collapse them).
    let k_versions: Vec<_> = scan
        .points
        .iter()
        .filter(|(ik, _)| encoded_user_key(ik) == b"k")
        .collect();
    assert_eq!(k_versions.len(), 2, "both versions of k must be exposed");

    // The point tombstone for "gone" is exposed as a Delete.
    let gone = scan
        .points
        .iter()
        .find(|(ik, _)| encoded_user_key(ik) == b"gone")
        .expect("gone tombstone present");
    let kind = trailer_kind(pebbledb::base::internal_key::encoded_trailer(&gone.0));
    assert_eq!(kind, InternalKeyKind::Delete);

    // The range deletion and range key are exposed separately.
    assert_eq!(scan.range_dels.len(), 1);
    assert_eq!(scan.range_dels[0].start, b"x");
    assert_eq!(scan.range_keys.len(), 1);
    assert_eq!(scan.range_keys[0].start, b"a");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn iterator_set_bounds_reuses_iterator() {
    let dir = temp_dir("set-bounds");
    let db = Db::open(&dir, Options::default()).unwrap();
    for i in 0..100u32 {
        db.set(format!("k{i:03}").as_bytes(), b"v").unwrap();
    }
    db.flush().unwrap();

    let collect_range = |it: &mut pebbledb::DbIterator| {
        it.first().unwrap();
        let mut ks = Vec::new();
        while it.valid() {
            ks.push(String::from_utf8_lossy(it.key()).into_owned());
            it.next().unwrap();
        }
        ks
    };

    let mut it = db
        .iter_with_options(IterOptions {
            lower_bound: Some(b"k010".to_vec()),
            upper_bound: Some(b"k020".to_vec()),
            ..Default::default()
        })
        .unwrap();
    let first = collect_range(&mut it);
    assert_eq!(first.first().unwrap(), "k010");
    assert_eq!(first.last().unwrap(), "k019");

    // Re-bound the same iterator to a different window and re-scan.
    it.set_bounds(Some(b"k050".to_vec()), Some(b"k053".to_vec()));
    let second = collect_range(&mut it);
    assert_eq!(second, vec!["k050", "k051", "k052"]);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn iterator_coalesces_range_keys() {
    let dir = temp_dir("rangekey-coalesce");
    let db = Db::open(&dir, Options::default()).unwrap();
    db.set(b"m", b"point").unwrap();
    // Two suffixed range keys over [a, z), then unset one of them.
    db.range_key_set(b"a", b"z", b"@1", b"v1").unwrap();
    db.range_key_set(b"a", b"z", b"@2", b"v2").unwrap();
    db.range_key_unset(b"a", b"z", b"@1").unwrap();

    let mut it = db.iter().unwrap();
    it.seek_ge(b"m").unwrap();
    assert!(it.valid());
    let eff = it.coalesced_range_keys();
    // @1 was unset; only @2 remains in force.
    assert_eq!(eff.len(), 1);
    assert_eq!(eff[0].suffix, b"@2");
    assert_eq!(eff[0].value, b"v2");

    // A range-key delete clears everything.
    db.range_key_delete(b"a", b"z").unwrap();
    let mut it = db.iter().unwrap();
    it.seek_ge(b"m").unwrap();
    assert!(it.coalesced_range_keys().is_empty());

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn iter_key_type_points_only_suppresses_range_keys() {
    use pebbledb::{IterKeyType, IterOptions};
    let dir = temp_dir("keytype-points");
    let db = Db::open(&dir, Options::default()).unwrap();
    db.set(b"a", b"1").unwrap();
    db.set(b"m", b"2").unwrap();
    db.range_key_set(b"b", b"p", b"@t", b"rk").unwrap();

    // PointsOnly: point keys still iterate, but range keys are not surfaced.
    let mut it = db
        .iter_with_options(IterOptions {
            key_type: IterKeyType::PointsOnly,
            ..Default::default()
        })
        .unwrap();
    it.first().unwrap();
    let mut pts = Vec::new();
    while it.valid() {
        pts.push(String::from_utf8_lossy(it.key()).into_owned());
        assert!(
            it.range_keys().is_empty(),
            "PointsOnly must not surface range keys"
        );
        it.next().unwrap();
    }
    assert_eq!(pts, vec!["a", "m"]);

    // The default (PointsAndRanges) still surfaces them at covered positions.
    let mut it = db.iter().unwrap();
    it.seek_ge(b"m").unwrap();
    assert_eq!(it.range_keys().len(), 1);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn iter_key_type_ranges_only_walks_spans() {
    use pebbledb::{IterKeyType, IterOptions};
    let dir = temp_dir("keytype-ranges");
    let db = Db::open(&dir, Options::default()).unwrap();
    // Points are ignored entirely in RangesOnly mode.
    db.set(b"a", b"1").unwrap();
    db.set(b"c", b"2").unwrap();
    db.set(b"q", b"3").unwrap();
    // Two abutting range keys with the same key set defragment into one span [b, k);
    // a separate span [p, t).
    db.range_key_set(b"b", b"f", b"@1", b"x").unwrap();
    db.range_key_set(b"f", b"k", b"@1", b"x").unwrap();
    db.range_key_set(b"p", b"t", b"@2", b"y").unwrap();

    let ranges_only = || {
        db.iter_with_options(IterOptions {
            key_type: IterKeyType::RangesOnly,
            ..Default::default()
        })
        .unwrap()
    };

    // Forward: the defragmented span starts, no point keys.
    let mut it = ranges_only();
    it.first().unwrap();
    let mut starts = Vec::new();
    while it.valid() {
        starts.push(String::from_utf8_lossy(it.key()).into_owned());
        it.next().unwrap();
    }
    assert_eq!(starts, vec!["b", "p"]);

    // The active range keys are surfaced at each span start.
    let mut it = ranges_only();
    it.first().unwrap();
    let eff = it.coalesced_range_keys();
    assert_eq!(eff.len(), 1);
    assert_eq!(eff[0].suffix, b"@1");
    assert_eq!(eff[0].value, b"x");

    // Reverse iteration yields the spans in the opposite order.
    let mut it = ranges_only();
    it.last().unwrap();
    let mut rev = Vec::new();
    while it.valid() {
        rev.push(String::from_utf8_lossy(it.key()).into_owned());
        it.prev().unwrap();
    }
    assert_eq!(rev, vec!["p", "b"]);

    // seek_ge lands on the span covering or following the target.
    let mut it = ranges_only();
    it.seek_ge(b"g").unwrap(); // inside [b, k)
    assert_eq!(it.key(), b"b");
    it.seek_ge(b"l").unwrap(); // between spans → next is [p, t)
    assert_eq!(it.key(), b"p");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn iterator_set_options_reconfigures_in_place() {
    use pebbledb::{IterKeyType, IterOptions};
    let dir = temp_dir("set-options");
    let db = Db::open(&dir, Options::default()).unwrap();
    for k in ["a", "b", "c", "d", "e"] {
        db.set(k.as_bytes(), b"v").unwrap();
    }
    db.range_key_set(b"a", b"z", b"@t", b"rk").unwrap();

    let mut it = db
        .iter_with_options(IterOptions {
            key_type: IterKeyType::PointsAndRanges,
            ..Default::default()
        })
        .unwrap();
    it.first().unwrap();
    assert_eq!(it.key(), b"a");
    assert_eq!(it.range_keys().len(), 1, "range key surfaced by default");

    // Reconfigure in place: PointsOnly within [c, e). Range keys no longer surfaced; bounds apply.
    it.set_options(IterOptions {
        key_type: IterKeyType::PointsOnly,
        lower_bound: Some(b"c".to_vec()),
        upper_bound: Some(b"e".to_vec()),
        ..Default::default()
    });
    it.first().unwrap();
    let mut got = Vec::new();
    while it.valid() {
        got.push(String::from_utf8_lossy(it.key()).into_owned());
        assert!(
            it.range_keys().is_empty(),
            "PointsOnly suppresses range keys"
        );
        it.next().unwrap();
    }
    assert_eq!(got, vec!["c", "d"]);

    // Reconfigure to RangesOnly: now walks the range-key span, not the points.
    it.set_options(IterOptions {
        key_type: IterKeyType::RangesOnly,
        ..Default::default()
    });
    it.first().unwrap();
    assert!(it.valid());
    assert_eq!(it.key(), b"a", "span [a, z) start");
    assert_eq!(it.coalesced_range_keys().len(), 1);
    it.next().unwrap();
    assert!(!it.valid(), "only one span");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn metrics_fields_and_begin_events() {
    use pebbledb::EventListener;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct Counter {
        flush_begin: AtomicUsize,
        compaction_begin: AtomicUsize,
    }
    impl EventListener for Counter {
        fn on_flush_begin(&self) {
            self.flush_begin.fetch_add(1, Ordering::SeqCst);
        }
        fn on_compaction_begin(&self, _lvl: usize, _inputs: usize) {
            self.compaction_begin.fetch_add(1, Ordering::SeqCst);
        }
    }

    let dir = temp_dir("metrics");
    let counter = Arc::new(Counter::default());
    let opts = Options {
        mem_table_size: 16 * 1024,
        event_listener: Some(counter.clone()),
        ..Default::default()
    };
    let db = Db::open(&dir, opts).unwrap();
    for i in 0..3000u32 {
        db.set(format!("k{i:05}").as_bytes(), &[b'v'; 50]).unwrap();
    }
    db.flush().unwrap();
    db.compact_range(None, None).unwrap();

    let m = db.metrics();
    assert!(m.total_sstables > 0);
    assert_eq!(
        m.total_sstables,
        m.level_files.iter().sum::<usize>(),
        "total must equal the per-level sum"
    );
    assert!(m.total_sstable_bytes > 0);
    assert_eq!(m.open_snapshots, 0);
    // Amplification metrics are populated after flushes + compactions.
    assert!(
        m.read_amplification >= 1,
        "read-amp: {}",
        m.read_amplification
    );
    assert!(
        m.write_amplification >= 1.0,
        "write-amp should be >= 1 after a compaction: {}",
        m.write_amplification
    );
    let _snap = db.snapshot();
    assert_eq!(db.metrics().open_snapshots, 1);

    assert!(counter.flush_begin.load(Ordering::SeqCst) >= 1);
    assert!(counter.compaction_begin.load(Ordering::SeqCst) >= 1);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn write_stall_engages_when_flush_falls_behind() {
    use pebbledb::EventListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    // A listener that slows down flushes and counts write-stall begins.
    #[derive(Default)]
    struct Slow {
        stalls: AtomicUsize,
    }
    impl EventListener for Slow {
        fn on_flush_begin(&self) {
            // Hold each flush until at least one write stall has been observed, so immutable
            // memtables deterministically pile up to the stop-writes threshold regardless of
            // disk speed. (A fixed sleep is timing-fragile: on slow-I/O platforms like Windows
            // CI — ~20x slower than Linux here — the flusher kept up and the stall never
            // engaged.) `on_write_stall_begin` fires before the writer blocks, so this never
            // deadlocks; the deadline is a safety valve in case no stall ever occurs.
            let start = std::time::Instant::now();
            while self.stalls.load(Ordering::SeqCst) == 0
                && start.elapsed() < Duration::from_secs(10)
            {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        fn on_write_stall_begin(&self, _reason: &str) {
            self.stalls.fetch_add(1, Ordering::SeqCst);
        }
    }

    let dir = temp_dir("write-stall");
    let slow = Arc::new(Slow::default());
    let opts = Options {
        mem_table_size: 4 * 1024,           // rotate often
        mem_table_stop_writes_threshold: 2, // stall after 2 immutables
        event_listener: Some(slow.clone()),
        ..Default::default()
    };
    let db = Db::open(&dir, opts).unwrap();
    // Write faster than the (deliberately slow) flush can drain.
    for i in 0..2000u32 {
        db.set(format!("k{i:05}").as_bytes(), &[b'v'; 64]).unwrap();
    }
    // Everything is still correct despite stalling.
    assert_eq!(db.get(b"k00000").unwrap(), Some(vec![b'v'; 64]));
    assert_eq!(db.get(b"k01999").unwrap(), Some(vec![b'v'; 64]));
    assert!(
        slow.stalls.load(Ordering::SeqCst) >= 1,
        "expected the write stall to engage at least once"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn format_ratchet_runs_stepwise_migrations() {
    use pebbledb::{FormatMajorVersion, Logger};
    use std::sync::Mutex;

    struct CountLogger(Mutex<usize>);
    impl Logger for CountLogger {
        fn info(&self, msg: &str) {
            if msg.contains("ratcheted format major version") {
                *self.0.lock().unwrap() += 1;
            }
        }
    }

    let dir = temp_dir("fmv-migrate");
    let logger = Arc::new(CountLogger(Mutex::new(0)));
    let opts = Options {
        format_major_version: FormatMajorVersion::MOST_COMPATIBLE, // = 1
        logger: Some(logger.clone()),
        ..Default::default()
    };
    let db = Db::open(&dir, opts).unwrap();
    db.ratchet_format_major_version(FormatMajorVersion::VALUE_BLOCKS) // = 9
        .unwrap();
    assert_eq!(db.format_major_version(), FormatMajorVersion::VALUE_BLOCKS);
    // One migration step per intermediate version (1 -> 9 == 8 steps).
    assert_eq!(*logger.0.lock().unwrap(), 8);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn excise_removes_and_reclaims_range() {
    let dir = temp_dir("excise");
    let opts = Options {
        mem_table_size: 16 * 1024,
        ..Default::default()
    };
    let db = Db::open(&dir, opts).unwrap();
    for i in 0..2000u32 {
        db.set(format!("k{i:05}").as_bytes(), &[b'v'; 32]).unwrap();
    }
    db.flush().unwrap();
    let before = db.estimate_disk_usage(b"k00500", b"k01000");
    assert!(before > 0);

    db.excise(b"k00500", b"k01000").unwrap();

    // Keys in the excised range are gone; neighbours remain.
    assert_eq!(db.get(b"k00500").unwrap(), None);
    assert_eq!(db.get(b"k00999").unwrap(), None);
    assert_eq!(db.get(b"k00499").unwrap(), Some(vec![b'v'; 32]));
    assert_eq!(db.get(b"k01000").unwrap(), Some(vec![b'v'; 32]));

    // The excised range no longer holds live keys on a full scan.
    let n_in_range = collect(&db)
        .iter()
        .filter(|(k, _)| {
            k.as_slice() >= b"k00500".as_slice() && k.as_slice() < b"k01000".as_slice()
        })
        .count();
    assert_eq!(n_in_range, 0);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn shared_objstorage_round_trips_and_survives_reopen() {
    use pebbledb::objstorage::{InMemoryRemote, RemoteStorage};

    let dir = temp_dir("objstore-shared");
    let remote: Arc<dyn RemoteStorage> = Arc::new(InMemoryRemote::new());
    let opts = || Options {
        remote_storage: Some(Arc::clone(&remote)),
        create_on_shared: true,
        mem_table_size: 16 * 1024,
        ..Default::default()
    };

    let db = Db::open(&dir, opts()).unwrap();
    for i in 0..400u32 {
        db.set(format!("k{i:04}").as_bytes(), format!("v{i}").as_bytes())
            .unwrap();
    }
    db.flush().unwrap();
    db.compact_range(None, None).unwrap();

    // The sstables live in the shared backend, not on the local filesystem.
    let remote_ssts = remote
        .list()
        .unwrap()
        .into_iter()
        .filter(|n| n.ends_with(".sst"))
        .count();
    assert!(remote_ssts >= 1, "expected sstables in the shared backend");
    let local_ssts = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|x| x == "sst"))
        .count();
    assert_eq!(local_ssts, 0, "no sstables should remain on local disk");

    let check = |db: &Db| {
        for i in 0..400u32 {
            assert_eq!(
                db.get(format!("k{i:04}").as_bytes()).unwrap(),
                Some(format!("v{i}").into_bytes()),
                "key {i}"
            );
        }
    };
    check(&db);

    // download() rewrites the live shared sstables back to local storage.
    let moved = db.download(b"", b"\xff\xff\xff\xff").unwrap();
    assert!(moved >= 1, "download should move shared objects local");
    assert!(
        std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .any(|e| e.path().extension().is_some_and(|x| x == "sst")),
        "sstables should now be on local disk"
    );
    check(&db);

    // The live set is now fully local: reopening with NO remote backend still reads everything.
    drop(db);
    let db = Db::open(&dir, Options::default()).unwrap();
    check(&db);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn check_consistency_passes_for_a_well_formed_lsm() {
    let dir = temp_dir("consistency");
    let db = Db::open(
        &dir,
        Options {
            mem_table_size: 16 * 1024,
            ..Default::default()
        },
    )
    .unwrap();
    // Build a multi-level tree with point keys, tombstones, and range keys.
    for i in 0..1500u32 {
        db.set(format!("k{i:05}").as_bytes(), &[b'v'; 24]).unwrap();
    }
    db.flush().unwrap();
    for i in (0..1500u32).step_by(7) {
        db.delete(format!("k{i:05}").as_bytes()).unwrap();
    }
    db.range_key_set(b"k00100", b"k00200", b"@t", b"rk")
        .unwrap();
    db.flush().unwrap();
    db.compact_range(None, None).unwrap();
    db.check_consistency()
        .expect("well-formed after compaction");

    // After an excise (which introduces virtual sstables) the tree is still consistent.
    db.excise(b"k00500", b"k01000").unwrap();
    db.check_consistency()
        .expect("well-formed after excise / virtual sstables");

    // And after reopen.
    drop(db);
    let db = Db::open(&dir, Options::default()).unwrap();
    db.check_consistency().expect("well-formed after reopen");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn excise_uses_virtual_sstables_without_rewriting() {
    let dir = temp_dir("excise-virtual");
    let ssts = |dir: &std::path::Path| -> std::collections::BTreeSet<String> {
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.ends_with(".sst"))
            .collect()
    };

    let db = Db::open(&dir, Options::default()).unwrap();
    for i in 0..200u32 {
        db.set(format!("k{i:04}").as_bytes(), b"v").unwrap();
    }
    db.flush().unwrap();
    db.compact_range(None, None).unwrap(); // collapse to a single bottom sstable
    let before = ssts(&dir);

    // Excise a middle span: [k0050, k0150) removes k0050..=k0149 (100 keys).
    db.excise(b"k0050", b"k0150").unwrap();

    // The physical sstable is reused as a virtual backing — no new sstable is written and the
    // straddling backing file is not deleted (two virtual views reference it).
    assert_eq!(
        ssts(&dir),
        before,
        "excise should reuse the physical file via virtual sstables, not rewrite it"
    );

    let check = |db: &Db| {
        assert_eq!(db.get(b"k0049").unwrap(), Some(b"v".to_vec()));
        assert_eq!(db.get(b"k0050").unwrap(), None);
        assert_eq!(db.get(b"k0149").unwrap(), None);
        assert_eq!(db.get(b"k0150").unwrap(), Some(b"v".to_vec()));
        assert_eq!(collect(db).len(), 100, "100 of 200 keys remain");
    };
    check(&db);

    // Reopen: the virtual files' backing references are repopulated and reads still resolve.
    drop(db);
    let db = Db::open(&dir, Options::default()).unwrap();
    check(&db);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn table_stats_aggregate_deletions() {
    let dir = temp_dir("table-stats");
    let db = Db::open(&dir, Options::default()).unwrap();
    for i in 0..200u32 {
        db.set(format!("k{i:03}").as_bytes(), b"v").unwrap();
    }
    for i in 0..30u32 {
        db.delete(format!("k{i:03}").as_bytes()).unwrap();
    }
    db.delete_range(b"k100", b"k110").unwrap();
    db.flush().unwrap();

    let stats = db.table_stats().unwrap();
    assert!(stats.tables >= 1);
    assert!(stats.num_entries >= 200, "entries: {}", stats.num_entries);
    assert!(
        stats.num_deletions >= 30,
        "deletions: {}",
        stats.num_deletions
    );
    assert!(stats.num_range_deletions >= 1);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn ingest_and_excise_replaces_a_range() {
    use pebbledb::base::internal_key::{InternalKey, InternalKeyKind};
    use pebbledb::sstable::{Writer, WriterOptions};

    let dir = temp_dir("ingest-excise");
    let db = Db::open(&dir, Options::default()).unwrap();
    // Pre-existing data across the range to be replaced.
    for k in ["b", "d", "f", "h"] {
        db.set(k.as_bytes(), b"old").unwrap();
    }
    db.flush().unwrap();

    // An external sstable holding the replacement keys (seqnum 0; ingestion restamps).
    let ext = dir.join("repl.sst");
    {
        let f = std::fs::File::create(&ext).unwrap();
        let mut w = Writer::new(f, db.comparer().clone(), WriterOptions::default());
        for k in ["c", "e"] {
            let ik = InternalKey::new(k.as_bytes().to_vec(), 0, InternalKeyKind::Set).encode();
            w.add(&ik, b"new").unwrap();
        }
        w.finish().unwrap();
    }

    // Atomically excise [c, g) and ingest the replacement.
    db.ingest_and_excise(&[&ext], b"c", b"g").unwrap();

    assert_eq!(db.get(b"b").unwrap(), Some(b"old".to_vec())); // outside excise
    assert_eq!(db.get(b"d").unwrap(), None); // excised, not replaced
    assert_eq!(db.get(b"f").unwrap(), None); // excised, not replaced
    assert_eq!(db.get(b"h").unwrap(), Some(b"old".to_vec())); // outside excise
    assert_eq!(db.get(b"c").unwrap(), Some(b"new".to_vec())); // ingested
    assert_eq!(db.get(b"e").unwrap(), Some(b"new".to_vec())); // ingested

    // The full-compaction convenience runs and preserves data.
    db.compact().unwrap();
    assert_eq!(db.get(b"c").unwrap(), Some(b"new".to_vec()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn l0_compaction_threshold_is_configurable() {
    // L0 is scored by sublevel count, so write *overlapping* flushes (each rewrite of the same
    // keys adds a sublevel). A high sublevel threshold lets them accumulate without compaction;
    // the default of 4 would have drained them — proving the tunable is wired through.
    let dir = temp_dir("l0-threshold");
    let opts = Options {
        mem_table_size: 4 * 1024,
        l0_compaction_threshold: 100,
        ..Default::default()
    };
    let db = Db::open(&dir, opts).unwrap();
    for _round in 0..12u32 {
        for k in 0..20u32 {
            db.set(format!("k{k:05}").as_bytes(), &[b'v'; 64]).unwrap();
        }
        db.flush().unwrap(); // each round overlaps the last → one more sublevel
    }
    let m = db.metrics();
    assert!(
        m.l0_sublevels > 4,
        "high sublevel threshold should let overlapping L0 accumulate, got {} sublevels {:?}",
        m.l0_sublevels,
        m.level_files
    );
    // Data is still correct.
    assert_eq!(db.get(b"k00000").unwrap(), Some(vec![b'v'; 64]));
    assert_eq!(db.get(b"k00019").unwrap(), Some(vec![b'v'; 64]));

    let _ = std::fs::remove_dir_all(&dir);
}

/// L0 is scored by **sublevel** count, not raw file count: overlapping flushes (which stack
/// into sublevels) trigger an L0→L1 compaction at `l0_compaction_threshold`, while disjoint
/// flushes (a single sublevel) do not — they accumulate until the file-count safety cap.
#[test]
fn l0_compaction_scores_by_sublevels() {
    // Flat L0: disjoint key ranges form one sublevel and do NOT trigger the sublevel threshold.
    let dir = temp_dir("sublevel-flat");
    let db = Db::open(
        &dir,
        Options {
            mem_table_size: 4 * 1024,
            ..Default::default()
        },
    )
    .unwrap();
    for g in 0..6u32 {
        for k in 0..20u32 {
            db.set(format!("k{g:02}{k:03}").as_bytes(), b"v").unwrap();
        }
        db.flush().unwrap();
    }
    let m = db.metrics();
    assert_eq!(
        m.l0_sublevels, 1,
        "disjoint flushes pack into one sublevel: {:?}",
        m.level_files
    );
    assert!(
        m.level_files[0] >= 6,
        "a flat (single-sublevel) L0 is not drained by the sublevel trigger, so files \
         accumulate (until the file-count cap): {:?}",
        m.level_files
    );
    drop(db);

    // Deep L0: repeatedly rewriting the same keys stacks sublevels past the threshold (4),
    // which triggers an L0→L1 compaction.
    let dir2 = temp_dir("sublevel-deep");
    let db2 = Db::open(
        &dir2,
        Options {
            mem_table_size: 4 * 1024,
            ..Default::default()
        },
    )
    .unwrap();
    for _round in 0..5u32 {
        for k in 0..20u32 {
            db2.set(format!("k{k:03}").as_bytes(), b"v").unwrap();
        }
        db2.flush().unwrap();
    }
    // The background worker drains L0 once the sublevel score crosses 1.0; poll for it.
    let mut drained = false;
    for _ in 0..200 {
        let m = db2.metrics();
        if m.level_files[1..].iter().sum::<usize>() > 0 && m.l0_sublevels < 4 {
            drained = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert!(
        drained,
        "overlapping flushes stack sublevels that trigger L0->L1: {:?}",
        db2.metrics().level_files
    );
    assert_eq!(db2.get(b"k000").unwrap(), Some(b"v".to_vec()));
    drop(db2);
}

#[test]
fn snapshot_iter_with_bounds() {
    let dir = temp_dir("snap-iter-bounds");
    let db = Db::open(&dir, Options::default()).unwrap();
    for i in 0..50u32 {
        db.set(format!("k{i:03}").as_bytes(), b"v1").unwrap();
    }
    let snap = db.snapshot();
    // Mutate after the snapshot; the snapshot's bounded iterator ignores the changes.
    for i in 0..50u32 {
        db.set(format!("k{i:03}").as_bytes(), b"v2").unwrap();
    }
    let mut it = snap
        .iter_with_options(IterOptions {
            lower_bound: Some(b"k010".to_vec()),
            upper_bound: Some(b"k015".to_vec()),
            ..Default::default()
        })
        .unwrap();
    it.first().unwrap();
    let mut got = Vec::new();
    while it.valid() {
        got.push((
            String::from_utf8_lossy(it.key()).into_owned(),
            String::from_utf8_lossy(it.value()).into_owned(),
        ));
        it.next().unwrap();
    }
    assert_eq!(got.len(), 5); // k010..k014
    assert!(got.iter().all(|(_, v)| v == "v1")); // snapshot view, pre-mutation
    assert_eq!(got[0].0, "k010");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn read_only_open_after_writes() {
    let dir = temp_dir("readonly");
    {
        let db = Db::open(&dir, Options::default()).unwrap();
        for i in 0..200u32 {
            db.set(format!("k{i:04}").as_bytes(), format!("v{i}").as_bytes())
                .unwrap();
        }
        db.flush().unwrap();
    }
    let db = Db::open_read_only(&dir, Options::default()).unwrap();
    assert_eq!(db.get(b"k0000").unwrap(), Some(b"v0".to_vec()));
    assert_eq!(db.get(b"k0199").unwrap(), Some(b"v199".to_vec()));
    assert!(db.set(b"x", b"y").is_err());

    let _ = std::fs::remove_dir_all(&dir);
}

/// Regression test for a compaction that split a user key's versions across two output
/// files. A compaction emits entries in (user-key asc, seqnum desc) order; the output was
/// previously cut whenever the running file reached `target_file_size`, even between two
/// internal versions of the *same* user key. That produced two same-level files overlapping
/// at that key — and because the picker compacts one file at a time, a later
/// L(n)->L(n+1) compaction could relevel the file holding the newer version below the one
/// holding the older version, so a point lookup then returned the stale value.
///
/// Several retained snapshots keep many Set versions of every key alive through compaction
/// (each version lands in its own snapshot stripe, so none are collapsed). Values larger
/// than a data block make the writer's size estimate advance roughly per entry, and a small
/// target file size then forces splits to land between a key's versions unless the split is
/// user-key-aligned. With `l1_max_bytes` set huge, no L1->L2 compaction runs to merge any
/// overlap away, so the post-compaction LSM is inspected directly: no two files at any level
/// below L0 may overlap in user-key range.
#[test]
fn compaction_splits_output_only_on_user_key_boundaries() {
    let dir = temp_dir("split-boundary");
    let opts = Options {
        mem_table_size: 8 * 1024,
        // Roughly two-and-a-bit large entries per output file, so a key's version chain
        // straddles a boundary unless the split is aligned to the user key.
        target_file_size: 14_000,
        // Never trigger an L1+ compaction, so any overlap a bad L0->L1 split created
        // survives for inspection instead of being merged away.
        l1_max_bytes: 1 << 40,
        l0_compaction_threshold: 2,
        max_concurrent_compactions: 1,
        ..Default::default()
    };
    let db = Db::open(&dir, opts).unwrap();

    // Incompressible 6 KiB values (> the 4 KiB data-block size, so each entry flushes its own
    // block and the writer's byte estimate advances per entry). xorshift keeps them
    // incompressible so block sizes stay ~6 KiB and splitting is fine-grained.
    let val = |kidx: usize, round: usize| -> Vec<u8> {
        let mut x = ((kidx as u64) << 20 | (round as u64) << 1) | 1;
        (0..6000)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect()
    };
    let key = |kidx: usize| format!("key{kidx:03}").into_bytes();
    const N: usize = 24;

    // v0 for every key, then a snapshot pinning it.
    for k in 0..N {
        db.set(&key(k), &val(k, 0)).unwrap();
    }
    let mut snaps = vec![db.snapshot()];

    // Several rounds of overwrites, each pinned by its own snapshot so every version survives
    // compaction in a distinct stripe. Flushes between rounds build L0 and drive L0->L1.
    let rounds = 4usize;
    for r in 1..=rounds {
        for k in 0..N {
            db.set(&key(k), &val(k, r)).unwrap();
        }
        db.flush().unwrap();
        snaps.push(db.snapshot());
    }
    db.flush().unwrap();

    // Invariant: below L0, files at the same level never overlap in user-key range. A
    // mid-key split would leave two files sharing a boundary key.
    let view = db.lsm_view();
    let mut level: Option<usize> = None;
    let mut prev_largest: Option<String> = None;
    for line in view.lines() {
        if let Some(rest) = line.strip_prefix('L') {
            // Level header like "L1: 3 files, ...".
            let lvl: usize = rest
                .split(':')
                .next()
                .and_then(|s| s.parse().ok())
                .expect("level header");
            level = Some(lvl);
            prev_largest = None;
            continue;
        }
        let lvl = match level {
            Some(l) => l,
            None => continue,
        };
        // File line: "  000007.sst  N bytes  [small .. large]".
        let Some(open) = line.find('[') else { continue };
        let Some(close) = line.find(']') else {
            continue;
        };
        let span = &line[open + 1..close];
        let mut parts = span.split(" .. ");
        let small = parts.next().unwrap_or("").to_string();
        let large = parts.next().unwrap_or("").to_string();
        if lvl >= 1 {
            if let Some(pl) = &prev_largest {
                assert!(
                    pl.as_str() < small.as_str(),
                    "L{lvl} files overlap in user-key range: previous largest {pl:?} \
                     >= next smallest {small:?}\n{view}"
                );
            }
            prev_largest = Some(large);
        }
    }

    // And the data reads back correctly: the live view sees the newest value, and each
    // snapshot still sees the version it pinned (snaps[i] was taken just after round i).
    for k in 0..N {
        assert_eq!(
            db.get(&key(k)).unwrap().as_deref(),
            Some(val(k, rounds).as_slice()),
            "live get for key{k:03}"
        );
        for (r, snap) in snaps.iter().enumerate() {
            assert_eq!(
                snap.get(&key(k)).unwrap().as_deref(),
                Some(val(k, r).as_slice()),
                "snapshot-after-round-{r} get for key{k:03}"
            );
        }
    }

    drop(snaps);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A bounded scan over a large run opens only the sstable(s) the seek lands in, not every file
/// — the lazy-open `ConcatIter`. Files written this session record a "no spans" hint, so the
/// eager range-tombstone/range-key pass skips opening them, and the point iterators are opened
/// lazily on seek. Verified by counting `.sst` reads through a wrapping `Fs`.
#[test]
fn bounded_scan_opens_only_the_files_it_touches() {
    use std::io;
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    use pebbledb::vfs::{DirLock, Fs, MemFs, WritableFile};

    // Wraps an inner `Fs`, counting reads of `.sst` files and delegating everything else.
    struct CountingFs {
        inner: Arc<dyn Fs>,
        sst_reads: Arc<AtomicUsize>,
    }
    impl Fs for CountingFs {
        fn create(&self, path: &Path) -> io::Result<Box<dyn WritableFile>> {
            self.inner.create(path)
        }
        fn reuse(&self, path: &Path) -> io::Result<Box<dyn WritableFile>> {
            self.inner.reuse(path)
        }
        fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
            if path.extension().is_some_and(|e| e == "sst") {
                self.sst_reads.fetch_add(1, AtomicOrdering::Relaxed);
            }
            self.inner.read(path)
        }
        fn remove(&self, path: &Path) -> io::Result<()> {
            self.inner.remove(path)
        }
        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            self.inner.rename(from, to)
        }
        fn list(&self, dir: &Path) -> io::Result<Vec<String>> {
            self.inner.list(dir)
        }
        fn create_dir_all(&self, path: &Path) -> io::Result<()> {
            self.inner.create_dir_all(path)
        }
        fn exists(&self, path: &Path) -> bool {
            self.inner.exists(path)
        }
        fn size(&self, path: &Path) -> io::Result<u64> {
            self.inner.size(path)
        }
        fn sync_dir(&self, dir: &Path) -> io::Result<()> {
            self.inner.sync_dir(dir)
        }
        fn lock(&self, path: &Path) -> io::Result<Box<dyn DirLock>> {
            self.inner.lock(path)
        }
    }

    let sst_reads = Arc::new(AtomicUsize::new(0));
    let fs: Arc<dyn Fs> = Arc::new(CountingFs {
        inner: Arc::new(MemFs::new()),
        sst_reads: Arc::clone(&sst_reads),
    });
    let db = Db::open(
        "/lazy-open",
        Options {
            fs: Arc::clone(&fs),
            mem_table_size: 8 * 1024,
            target_file_size: 1024, // small → many output files
            max_concurrent_compactions: 1,
            ..Default::default()
        },
    )
    .unwrap();

    // Many point-only keys with sizeable values, flushed and compacted toward the bottom level
    // so the run is many small, non-overlapping, span-free files.
    const N: u32 = 600;
    let val = vec![b'x'; 200];
    for i in 0..N {
        db.set(format!("key{i:04}").as_bytes(), &val).unwrap();
    }
    db.flush().unwrap();
    db.compact_range(None, None).unwrap();

    let counts = db.level_file_counts();
    let total_files: usize = counts.iter().sum();
    assert!(
        total_files >= 10,
        "expected the run to have many files, got {total_files} ({counts:?})"
    );

    // A bounded scan over three consecutive keys should open only the file they live in.
    sst_reads.store(0, AtomicOrdering::Relaxed);
    let mut it = db
        .iter_with_options(IterOptions {
            lower_bound: Some(b"key0100".to_vec()),
            upper_bound: Some(b"key0103".to_vec()),
            ..Default::default()
        })
        .unwrap();
    it.first().unwrap();
    let mut bounded = Vec::new();
    while it.valid() {
        bounded.push(it.key().to_vec());
        it.next().unwrap();
    }
    let bounded_reads = sst_reads.load(AtomicOrdering::Relaxed);
    assert_eq!(
        bounded,
        vec![
            b"key0100".to_vec(),
            b"key0101".to_vec(),
            b"key0102".to_vec()
        ],
    );

    // A full scan opens every file in the run.
    sst_reads.store(0, AtomicOrdering::Relaxed);
    let full = collect(&db);
    let full_reads = sst_reads.load(AtomicOrdering::Relaxed);
    assert_eq!(full.len(), N as usize);

    assert!(
        bounded_reads < full_reads,
        "bounded scan opened {bounded_reads} sstables; a full scan opened {full_reads} — the \
         bounded scan should open strictly fewer"
    );
    assert!(
        bounded_reads <= 2,
        "a 3-key bounded scan opened {bounded_reads} sstables (run has {total_files} files); \
         lazy open should touch at most the boundary file or two"
    );
}

/// The per-file "has spans" hint is persisted in the MANIFEST, so even a *cold reopen* (whose
/// in-memory hint starts empty) can skip opening span-free files on the very first scan — it is
/// seeded from the loaded file metadata. Without persistence the first post-reopen scan would
/// open every file in the run to learn the hint.
#[test]
fn span_hint_persists_across_reopen() {
    use std::io;
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    use pebbledb::vfs::{DirLock, Fs, MemFs, WritableFile};

    struct CountingFs {
        inner: Arc<dyn Fs>,
        sst_reads: Arc<AtomicUsize>,
    }
    impl Fs for CountingFs {
        fn create(&self, path: &Path) -> io::Result<Box<dyn WritableFile>> {
            self.inner.create(path)
        }
        fn reuse(&self, path: &Path) -> io::Result<Box<dyn WritableFile>> {
            self.inner.reuse(path)
        }
        fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
            if path.extension().is_some_and(|e| e == "sst") {
                self.sst_reads.fetch_add(1, AtomicOrdering::Relaxed);
            }
            self.inner.read(path)
        }
        fn remove(&self, path: &Path) -> io::Result<()> {
            self.inner.remove(path)
        }
        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            self.inner.rename(from, to)
        }
        fn list(&self, dir: &Path) -> io::Result<Vec<String>> {
            self.inner.list(dir)
        }
        fn create_dir_all(&self, path: &Path) -> io::Result<()> {
            self.inner.create_dir_all(path)
        }
        fn exists(&self, path: &Path) -> bool {
            self.inner.exists(path)
        }
        fn size(&self, path: &Path) -> io::Result<u64> {
            self.inner.size(path)
        }
        fn sync_dir(&self, dir: &Path) -> io::Result<()> {
            self.inner.sync_dir(dir)
        }
        fn lock(&self, path: &Path) -> io::Result<Box<dyn DirLock>> {
            self.inner.lock(path)
        }
    }

    let sst_reads = Arc::new(AtomicUsize::new(0));
    let fs: Arc<dyn Fs> = Arc::new(CountingFs {
        inner: Arc::new(MemFs::new()),
        sst_reads: Arc::clone(&sst_reads),
    });
    let opts = || Options {
        fs: Arc::clone(&fs),
        mem_table_size: 8 * 1024,
        target_file_size: 1024,
        max_concurrent_compactions: 1,
        ..Default::default()
    };

    // Build a run of many small, span-free, non-overlapping bottom-level files.
    const N: u32 = 600;
    let val = vec![b'x'; 200];
    {
        let db = Db::open("/span-persist", opts()).unwrap();
        for i in 0..N {
            db.set(format!("key{i:04}").as_bytes(), &val).unwrap();
        }
        db.flush().unwrap();
        db.compact_range(None, None).unwrap();
        drop(db);
    }

    // Cold reopen: the in-memory hint starts empty and is seeded from the MANIFEST.
    let db = Db::open("/span-persist", opts()).unwrap();
    let total_files: usize = db.level_file_counts().iter().sum();
    assert!(total_files >= 10, "expected many files, got {total_files}");

    sst_reads.store(0, AtomicOrdering::Relaxed);
    let mut it = db
        .iter_with_options(IterOptions {
            lower_bound: Some(b"key0100".to_vec()),
            upper_bound: Some(b"key0103".to_vec()),
            ..Default::default()
        })
        .unwrap();
    it.first().unwrap();
    let mut got = Vec::new();
    while it.valid() {
        got.push(it.key().to_vec());
        it.next().unwrap();
    }
    let reads = sst_reads.load(AtomicOrdering::Relaxed);
    assert_eq!(
        got,
        vec![
            b"key0100".to_vec(),
            b"key0101".to_vec(),
            b"key0102".to_vec()
        ],
    );
    assert!(
        reads <= 2,
        "after a cold reopen, a 3-key bounded scan opened {reads} sstables ({total_files} in \
         the run); the persisted span hint should let it skip the rest without opening them"
    );
}

/// A slow-but-successful WAL write proactively fails the WAL over to the secondary directory
/// when `wal_failover_latency_threshold` is set, and the data remains correct and recoverable.
#[test]
fn slow_wal_write_triggers_latency_failover() {
    use std::io::{self, Write};
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use pebbledb::vfs::{DirLock, Fs, MemFs, WritableFile};

    // A writable that sleeps on its durability barrier, simulating a slow disk.
    struct SlowWritable {
        inner: Box<dyn WritableFile>,
        delay: Duration,
    }
    impl Write for SlowWritable {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.inner.write(buf)
        }
        fn flush(&mut self) -> io::Result<()> {
            std::thread::sleep(self.delay);
            self.inner.flush()
        }
    }
    impl WritableFile for SlowWritable {
        fn sync_all(&mut self) -> io::Result<()> {
            std::thread::sleep(self.delay);
            self.inner.sync_all()
        }
    }

    // Makes `.log` writes under `slow_dir` slow; everything else delegates unchanged.
    struct SlowFs {
        inner: Arc<dyn Fs>,
        slow_dir: PathBuf,
        delay: Duration,
    }
    impl SlowFs {
        fn wrap(&self, path: &Path, w: Box<dyn WritableFile>) -> Box<dyn WritableFile> {
            if path.extension().is_some_and(|e| e == "log") && path.starts_with(&self.slow_dir) {
                Box::new(SlowWritable {
                    inner: w,
                    delay: self.delay,
                })
            } else {
                w
            }
        }
    }
    impl Fs for SlowFs {
        fn create(&self, path: &Path) -> io::Result<Box<dyn WritableFile>> {
            let w = self.inner.create(path)?;
            Ok(self.wrap(path, w))
        }
        fn reuse(&self, path: &Path) -> io::Result<Box<dyn WritableFile>> {
            let w = self.inner.reuse(path)?;
            Ok(self.wrap(path, w))
        }
        fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
            self.inner.read(path)
        }
        fn remove(&self, path: &Path) -> io::Result<()> {
            self.inner.remove(path)
        }
        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            self.inner.rename(from, to)
        }
        fn list(&self, dir: &Path) -> io::Result<Vec<String>> {
            self.inner.list(dir)
        }
        fn create_dir_all(&self, path: &Path) -> io::Result<()> {
            self.inner.create_dir_all(path)
        }
        fn exists(&self, path: &Path) -> bool {
            self.inner.exists(path)
        }
        fn size(&self, path: &Path) -> io::Result<u64> {
            self.inner.size(path)
        }
        fn sync_dir(&self, dir: &Path) -> io::Result<()> {
            self.inner.sync_dir(dir)
        }
        fn lock(&self, path: &Path) -> io::Result<Box<dyn DirLock>> {
            self.inner.lock(path)
        }
    }

    let primary = PathBuf::from("/lat-primary");
    let secondary = PathBuf::from("/lat-secondary");
    let fs: Arc<dyn Fs> = Arc::new(SlowFs {
        inner: Arc::new(MemFs::new()),
        slow_dir: primary.clone(),
        delay: Duration::from_millis(30),
    });

    let db = Db::open(
        &primary,
        Options {
            fs: Arc::clone(&fs),
            wal_failover_dir: Some(secondary.clone()),
            wal_failover_latency_threshold: Some(Duration::from_millis(5)),
            ..Default::default()
        },
    )
    .unwrap();

    // The first write hits the slow primary WAL (30ms > 5ms), tripping a latency failover; later
    // writes land on the fast secondary.
    db.set(b"k1", b"v1").unwrap();
    db.set(b"k2", b"v2").unwrap();
    db.set(b"k3", b"v3").unwrap();

    assert!(
        db.metrics().wal_failover_count >= 1,
        "a slow WAL write should trigger a latency failover, got {}",
        db.metrics().wal_failover_count
    );
    assert_eq!(db.get(b"k1").unwrap(), Some(b"v1".to_vec()));

    // The data survives a reopen — recovery replays both WAL directories.
    drop(db);
    let db = Db::open(
        &primary,
        Options {
            fs: Arc::clone(&fs),
            wal_failover_dir: Some(secondary.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    for (k, v) in [(&b"k1"[..], &b"v1"[..]), (b"k2", b"v2"), (b"k3", b"v3")] {
        assert_eq!(
            db.get(k).unwrap().as_deref(),
            Some(v),
            "key {k:?} after reopen"
        );
    }
}

/// Blob references are persisted in the MANIFEST, so a reopen recovers them from file metadata
/// instead of re-reading every sstable's metaindex — verified by counting sstable reads during
/// the reopen — and blob-backed values stay readable (their blob files are not GC'd).
#[test]
fn blob_refs_persist_across_reopen_skipping_rescan() {
    use std::io;
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    use pebbledb::vfs::{DirLock, Fs, MemFs, WritableFile};

    struct CountingFs {
        inner: Arc<dyn Fs>,
        sst_reads: Arc<AtomicUsize>,
    }
    impl Fs for CountingFs {
        fn create(&self, path: &Path) -> io::Result<Box<dyn WritableFile>> {
            self.inner.create(path)
        }
        fn reuse(&self, path: &Path) -> io::Result<Box<dyn WritableFile>> {
            self.inner.reuse(path)
        }
        fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
            if path.extension().is_some_and(|e| e == "sst") {
                self.sst_reads.fetch_add(1, AtomicOrdering::Relaxed);
            }
            self.inner.read(path)
        }
        fn remove(&self, path: &Path) -> io::Result<()> {
            self.inner.remove(path)
        }
        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            self.inner.rename(from, to)
        }
        fn list(&self, dir: &Path) -> io::Result<Vec<String>> {
            self.inner.list(dir)
        }
        fn create_dir_all(&self, path: &Path) -> io::Result<()> {
            self.inner.create_dir_all(path)
        }
        fn exists(&self, path: &Path) -> bool {
            self.inner.exists(path)
        }
        fn size(&self, path: &Path) -> io::Result<u64> {
            self.inner.size(path)
        }
        fn sync_dir(&self, dir: &Path) -> io::Result<()> {
            self.inner.sync_dir(dir)
        }
        fn lock(&self, path: &Path) -> io::Result<Box<dyn DirLock>> {
            self.inner.lock(path)
        }
    }

    let sst_reads = Arc::new(AtomicUsize::new(0));
    let fs: Arc<dyn Fs> = Arc::new(CountingFs {
        inner: Arc::new(MemFs::new()),
        sst_reads: Arc::clone(&sst_reads),
    });
    let opts = || Options {
        fs: Arc::clone(&fs),
        mem_table_size: 8 * 1024,
        // Separate large values into blob files so the sstables carry blob references.
        blob_value_threshold: Some(64),
        ..Default::default()
    };

    let big = vec![b'b'; 500];
    {
        let db = Db::open("/blob-persist", opts()).unwrap();
        for i in 0..40u32 {
            db.set(format!("k{i:03}").as_bytes(), &big).unwrap();
        }
        db.flush().unwrap();
        for i in 40..80u32 {
            db.set(format!("k{i:03}").as_bytes(), &big).unwrap();
        }
        db.flush().unwrap();
        drop(db);
    }

    // Reopen: blob references should come from the MANIFEST, so the open performs no sstable
    // reads to rediscover them.
    sst_reads.store(0, AtomicOrdering::Relaxed);
    let db = Db::open("/blob-persist", opts()).unwrap();
    let reads = sst_reads.load(AtomicOrdering::Relaxed);
    assert_eq!(
        reads, 0,
        "reopen re-read {reads} sstables for blob refs; they should come from the MANIFEST"
    );

    // The blob-backed values survive (their blob files were recognized as referenced).
    db.compact_range(None, None).unwrap();
    assert_eq!(db.get(b"k000").unwrap(), Some(big.clone()));
    assert_eq!(db.get(b"k079").unwrap(), Some(big.clone()));
}

/// Reads a real Pebble v2 (`FormatColumnarBlocks`) sstable — checked in as a fixture — through
/// the engine's `Reader`, proving columnar byte-parity offline (no Go toolchain needed). The
/// fixture was produced by Pebble v2.1.6 with keys key0000..key0099 => value0..value99.
#[test]
fn reads_pebble_v2_columnar_sstable_fixture() {
    use pebbledb::DefaultComparer;
    use pebbledb::sstable::Reader;

    let bytes = include_bytes!("fixtures/pebble_v2_columnar.sst").to_vec();
    let reader = Arc::new(Reader::open(bytes, Arc::new(DefaultComparer)).expect("open columnar"));

    // Point lookups resolve each key to its value at the latest sequence number.
    for i in 0..100u32 {
        let k = format!("key{i:04}");
        let got = reader
            .get(k.as_bytes(), u64::MAX)
            .expect("lookup")
            .map(|(_, v)| v);
        assert_eq!(
            got.as_deref(),
            Some(format!("value{i}").as_bytes()),
            "columnar get for {k}"
        );
    }

    // Forward iteration yields all 100 keys in order.
    let mut it = reader.iter().expect("iter");
    let mut n = 0u32;
    it.first().unwrap();
    while it.valid() {
        let want_k = format!("key{n:04}");
        assert_eq!(
            pebbledb::base::internal_key::encoded_user_key(it.key()),
            want_k.as_bytes(),
            "columnar scan key #{n}"
        );
        assert_eq!(it.value(), format!("value{n}").as_bytes());
        n += 1;
        it.next().unwrap();
    }
    assert_eq!(n, 100, "columnar scan should yield 100 keys");
}

/// Reads a real Pebble v2 columnar sstable that carries keyspans — a range deletion and a range
/// key — proving the columnar keyspan (boundary-based) block decodes byte-for-byte. The fixture
/// was produced by Pebble v2.1.6 with keys key00000..key00019 => value0..value19, a
/// `DeleteRange[key00005, key00010)`, and a `RangeKeySet[key00012, key00015)@1 = "rkval"`.
#[test]
fn reads_pebble_v2_columnar_keyspans_fixture() {
    use pebbledb::DefaultComparer;
    use pebbledb::base::internal_key::InternalKeyKind;
    use pebbledb::base::range_key::{decode_end, decode_set_suffix_values};
    use pebbledb::sstable::Reader;

    let bytes = include_bytes!("fixtures/pebble_v2_columnar_spans.sst").to_vec();
    let reader = Arc::new(Reader::open(bytes, Arc::new(DefaultComparer)).expect("open columnar"));

    // Point keys still read back — except key00005..key00009, which Pebble elided during the
    // flush because the DeleteRange[key00005, key00010) covers them at a higher sequence number.
    for i in 0..20u32 {
        let k = format!("key{i:05}");
        let got = reader
            .get(k.as_bytes(), u64::MAX)
            .expect("lookup")
            .map(|(_, v)| v);
        if (5..10).contains(&i) {
            assert_eq!(
                got, None,
                "{k} should have been elided by the range deletion"
            );
        } else {
            assert_eq!(got.as_deref(), Some(format!("value{i}").as_bytes()));
        }
    }

    // The range deletion [key00005, key00010) is surfaced.
    let dels = reader.range_tombstones();
    assert_eq!(dels.len(), 1, "one range deletion");
    assert_eq!(dels[0].start, b"key00005");
    assert_eq!(dels[0].end, b"key00010");

    // The range key set [key00012, key00015)@1 = "rkval" is surfaced.
    let rks = reader.range_keys();
    assert_eq!(rks.len(), 1, "one range key");
    assert_eq!(rks[0].kind, InternalKeyKind::RangeKeySet);
    assert_eq!(rks[0].start, b"key00012");
    let (end, payload) = decode_end(rks[0].kind, &rks[0].value).expect("decode end");
    assert_eq!(end, b"key00015");
    let svs = decode_set_suffix_values(payload).expect("decode set suffix/values");
    assert_eq!(svs.len(), 1);
    assert_eq!(svs[0].suffix, b"@1");
    assert_eq!(svs[0].value, b"rkval");
}

/// Reads a real Pebble v2 columnar sstable that stores an **out-of-line value** in a value
/// block (is-value-external). The fixture has key00002 written twice with a snapshot pinning
/// the older version, so the older SET's value is separated into a value block; reading it back
/// exercises the columnar value-block resolution path. Values: key00002 newest =
/// "NEWVALUE-" + 20×'n' (inline), older = "OLDVALUE-" + 20×'o' (out-of-line).
#[test]
fn reads_pebble_v2_columnar_value_block_fixture() {
    use pebbledb::DefaultComparer;
    use pebbledb::base::internal_key::encoded_user_key;
    use pebbledb::sstable::Reader;

    let bytes = include_bytes!("fixtures/pebble_v2_columnar_valueblock.sst").to_vec();
    let reader = Arc::new(Reader::open(bytes, Arc::new(DefaultComparer)).expect("open columnar"));

    let new_value = format!("NEWVALUE-{}", "n".repeat(20));
    let old_value = format!("OLDVALUE-{}", "o".repeat(20));

    // The newest version of key00002 resolves to the inline NEWVALUE.
    let got = reader
        .get(b"key00002", u64::MAX)
        .expect("lookup")
        .map(|(_, v)| v);
    assert_eq!(got.as_deref(), Some(new_value.as_bytes()));

    // A full internal-key scan surfaces BOTH versions of key00002, and the older one — stored
    // out-of-line in a value block — resolves to OLDVALUE.
    let mut it = reader.iter().expect("iter");
    let mut key2_values = Vec::new();
    it.first().unwrap();
    while it.valid() {
        if encoded_user_key(it.key()) == b"key00002" {
            key2_values.push(it.value().to_vec());
        }
        it.next().unwrap();
    }
    assert_eq!(key2_values.len(), 2, "two versions of key00002");
    assert!(
        key2_values.iter().any(|v| v == new_value.as_bytes()),
        "newest (inline) value present"
    );
    assert!(
        key2_values.iter().any(|v| v == old_value.as_bytes()),
        "older (out-of-line value-block) value resolved"
    );
}

/// At a columnar format major version the engine flushes to columnar sstables. This verifies the
/// flushed file is genuinely columnar, that point keys + a range deletion round-trip through a
/// reopen, and that the engine reads its own columnar output back correctly.
#[test]
fn engine_flushes_columnar_sstables_at_columnar_format() {
    use pebbledb::FormatMajorVersion;
    use pebbledb::sstable::Reader;

    let dir = temp_dir("columnar-flush");
    let opts = Options {
        format_major_version: FormatMajorVersion::COLUMNAR_BLOCKS,
        ..Default::default()
    };

    {
        let db = Db::open(&dir, opts.clone()).unwrap();
        for i in 0..50u32 {
            db.set(
                format!("key{i:04}").as_bytes(),
                format!("value{i}").as_bytes(),
            )
            .unwrap();
        }
        // A range deletion exercises the columnar keyspan write path.
        db.delete_range(b"key0010", b"key0020").unwrap();
        db.flush().unwrap();
    }

    // The flushed sstable(s) must be in the columnar block format.
    let mut found_columnar = false;
    for entry in std::fs::read_dir(&dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) == Some("sst") {
            let bytes = std::fs::read(&path).unwrap();
            let r = Reader::open(bytes, Arc::new(pebbledb::DefaultComparer)).unwrap();
            assert!(
                r.format().is_columnar(),
                "flushed sstable should be columnar"
            );
            found_columnar = true;
        }
    }
    assert!(found_columnar, "expected at least one flushed sstable");

    // Reopen and verify the engine reads its own columnar output, with the range deletion applied.
    {
        let db = Db::open(&dir, opts).unwrap();
        for i in 0..50u32 {
            let k = format!("key{i:04}");
            let got = db.get(k.as_bytes()).unwrap();
            if (10..20).contains(&i) {
                assert_eq!(got, None, "{k} should be removed by the range deletion");
            } else {
                assert_eq!(got.as_deref(), Some(format!("value{i}").as_bytes()));
            }
        }
    }
}

/// At a columnar format major version, compaction output is also columnar, so a database stays
/// columnar end-to-end. This forces several flushes then a full compaction and asserts every
/// surviving sstable is columnar and the (overwritten) values are correct.
#[test]
fn engine_compacts_to_columnar_sstables() {
    use pebbledb::FormatMajorVersion;
    use pebbledb::sstable::Reader;

    let dir = temp_dir("columnar-compact");
    let opts = Options {
        format_major_version: FormatMajorVersion::COLUMNAR_BLOCKS,
        mem_table_size: 16 * 1024, // small, to force multiple flushes
        ..Default::default()
    };

    let db = Db::open(&dir, opts).unwrap();
    // Two passes over the same keys: the second overwrites, so compaction must drop older
    // versions — exercising the merge/value path of the columnar compaction output.
    for pass in 0..2 {
        for i in 0..400u32 {
            db.set(
                format!("key{i:05}").as_bytes(),
                format!("v{i}-pass{pass}").as_bytes(),
            )
            .unwrap();
        }
    }
    db.flush().unwrap();
    db.compact_range(None, None).unwrap();

    // Every sstable on disk must be columnar (flush and compaction both emit columnar).
    let mut count = 0;
    for entry in std::fs::read_dir(&dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) == Some("sst") {
            let bytes = std::fs::read(&path).unwrap();
            let r = Reader::open(bytes, Arc::new(pebbledb::DefaultComparer)).unwrap();
            assert!(
                r.format().is_columnar(),
                "compacted sstable {path:?} should be columnar"
            );
            count += 1;
        }
    }
    assert!(count > 0, "expected sstables after compaction");

    // Values reflect the second pass.
    for i in 0..400u32 {
        let k = format!("key{i:05}");
        assert_eq!(
            db.get(k.as_bytes()).unwrap().as_deref(),
            Some(format!("v{i}-pass1").as_bytes())
        );
    }
}

/// At the value-separation format major version with a blob threshold, the engine flushes
/// table-format-v7 sstables and writes large values into native blob files; reopening reads them
/// back through the native-blob resolution path.
#[test]
fn engine_writes_and_reads_value_separated_database() {
    use pebbledb::FormatMajorVersion;

    let dir = temp_dir("value-separation");
    let opts = Options {
        format_major_version: FormatMajorVersion::VALUE_SEPARATION,
        blob_value_threshold: Some(20),
        ..Default::default()
    };

    let big = |i: u32| format!("bigvalue-{i}-{}", "x".repeat(40));
    {
        let db = Db::open(&dir, opts.clone()).unwrap();
        for i in 0..30u32 {
            db.set(format!("key{i:05}").as_bytes(), big(i).as_bytes())
                .unwrap();
        }
        db.flush().unwrap();
    }

    // A native blob file must have been written.
    let has_blob = std::fs::read_dir(&dir)
        .unwrap()
        .any(|e| e.unwrap().path().extension().and_then(|x| x.to_str()) == Some("blob"));
    assert!(has_blob, "expected a native blob file");

    // Reopen and read the separated values back through the engine.
    {
        let db = Db::open(&dir, opts).unwrap();
        for i in 0..30u32 {
            assert_eq!(
                db.get(format!("key{i:05}").as_bytes()).unwrap().as_deref(),
                Some(big(i).as_bytes()),
                "separated value for key{i:05}"
            );
        }
    }
}

/// At the value-separation format major version, a compaction of value-separated inputs must
/// stay separated: the merged columnar output is table-format-v7 and re-separates large values
/// into its own native blob file, rather than de-separating them inline. Verified by forcing
/// several separated L0 flushes, compacting them together, and reading the values back after a
/// reopen — which resolves them purely from the compacted file's MANIFEST-persisted blob
/// references and the blob file the compaction wrote.
#[test]
fn compaction_keeps_values_separated() {
    use pebbledb::FormatMajorVersion;

    let dir = temp_dir("value-separation-compact");
    let opts = Options {
        format_major_version: FormatMajorVersion::VALUE_SEPARATION,
        blob_value_threshold: Some(20),
        mem_table_size: 4 * 1024, // small, to force multiple separated L0 flushes
        ..Default::default()
    };

    let big = |i: u32| format!("bigvalue-{i}-{}", "x".repeat(60));
    let db = Db::open(&dir, opts.clone()).unwrap();
    // Several flushes, each producing its own value-separated L0 table + blob file.
    for batch in 0..4u32 {
        for i in 0..30u32 {
            let k = batch * 30 + i;
            db.set(format!("key{k:05}").as_bytes(), big(k).as_bytes())
                .unwrap();
        }
        db.flush().unwrap();
    }
    // Merge the separated L0 tables into the output level.
    db.compact_range(None, None).unwrap();

    // After a full-range compaction the only sstables left are the compaction's outputs. Each must
    // be a table-format-v7 table (footer version 7 + Pebble magic) — proving the compaction stayed
    // separated rather than de-separating to a v5 inline table — and a native blob file must be
    // present. Reading the v7 footer directly avoids materializing the table (which would need the
    // blob resolver the standalone `Reader::open` has no way to attach).
    const PEBBLE_MAGIC: &[u8; 8] = b"\xf0\x9f\xaa\xb3\xf0\x9f\xaa\xb3";
    let mut blob_files = 0;
    let mut sst_files = 0;
    for entry in std::fs::read_dir(&dir).unwrap() {
        let path = entry.unwrap().path();
        match path.extension().and_then(|e| e.to_str()) {
            Some("blob") => blob_files += 1,
            Some("sst") => {
                let bytes = std::fs::read(&path).unwrap();
                let n = bytes.len();
                assert_eq!(&bytes[n - 8..], PEBBLE_MAGIC, "pebble magic in {path:?}");
                let version = u32::from_le_bytes(bytes[n - 12..n - 8].try_into().unwrap());
                assert_eq!(
                    version, 7,
                    "compacted sstable {path:?} should be table-format-v7 (value-separated)"
                );
                sst_files += 1;
            }
            _ => {}
        }
    }
    assert!(sst_files > 0, "expected sstables after compaction");
    assert!(
        blob_files > 0,
        "compaction should keep values separated in a native blob file"
    );

    // Same-instance read after compaction (native_blob_refs updated in place).
    for k in 0..120u32 {
        assert_eq!(
            db.get(format!("key{k:05}").as_bytes()).unwrap().as_deref(),
            Some(big(k).as_bytes()),
            "separated value for key{k:05} same-instance after compaction"
        );
    }

    // A second write + compaction cycle. Its `collect_obsolete` pass frees the first cycle's now-
    // unreferenced inputs, so their native blob files must be deleted along with the sstables — not
    // left orphaned on disk. Each value-separated sstable owns exactly one blob (1:1), so after
    // collection the number of blob files must equal the number of v7 sstables on disk; an orphaned
    // blob (the pre-GC bug) would make blobs outnumber sstables.
    for k in 120..150u32 {
        db.set(format!("key{k:05}").as_bytes(), big(k).as_bytes())
            .unwrap();
    }
    db.flush().unwrap();
    db.compact_range(None, None).unwrap();

    let count_ext = |ext: &str| {
        std::fs::read_dir(&dir)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .and_then(|x| x.to_str())
                    == Some(ext)
            })
            .count()
    };
    assert_eq!(
        count_ext("blob"),
        count_ext("sst"),
        "obsolete native blob files must be GC'd with their sstables (no orphans)"
    );

    // Reopen and read every value back — resolution comes from the compacted file's
    // MANIFEST-persisted blob references, proving the compaction wrote them correctly.
    drop(db);
    let db = Db::open(&dir, opts).unwrap();
    for k in 0..150u32 {
        assert_eq!(
            db.get(format!("key{k:05}").as_bytes()).unwrap().as_deref(),
            Some(big(k).as_bytes()),
            "separated value for key{k:05} after compaction + reopen"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// Reads a real Pebble v2 native blob file (`FormatValueSeparation`, format 24) — checked in as a
/// fixture — through `pebble_blob::PebbleBlobReader`, proving byte-parity with Pebble's blob file
/// format. The fixture holds the separated values for keys key00000..key00029, each value being
/// "V<i>-" repeated 20 times.
#[test]
fn reads_pebble_v2_native_blob_file_fixture() {
    use pebbledb::sstable::pebble_blob::PebbleBlobReader;

    let bytes = include_bytes!("fixtures/pebble_v2_blobfile.blob").to_vec();
    let r = PebbleBlobReader::open(bytes).expect("open blob file");

    let values = r.iter_all().expect("iter all values");
    assert_eq!(values.len(), 30, "30 separated values");
    // The values are in insertion (key) order: value i = "V<i>-" * 20.
    for (i, v) in values.iter().enumerate() {
        let want = format!("V{i}-").repeat(20);
        assert_eq!(v.as_slice(), want.as_bytes(), "blob value {i}");
    }
}

/// Reads a real Pebble v2 table-format-v7 sstable (written at `FormatValueSeparation` but with
/// values stored inline — no value separation). The v7 footer is 61 bytes (it adds an attributes
/// word and a footer checksum over the v5 columnar footer); the data blocks are still columnar, so
/// the engine reads it through the same columnar path once the footer length is recognized.
#[test]
fn reads_pebble_v2_table_format_v7_inline_fixture() {
    use pebbledb::DefaultComparer;
    use pebbledb::sstable::Reader;

    let bytes = include_bytes!("fixtures/pebble_v2_v7_inline.sst").to_vec();
    let reader = Arc::new(Reader::open(bytes, Arc::new(DefaultComparer)).expect("open v7 sstable"));
    assert!(reader.format().is_columnar());

    for i in 0..50u32 {
        let k = format!("key{i:04}");
        let got = reader
            .get(k.as_bytes(), u64::MAX)
            .expect("lookup")
            .map(|(_, v)| v);
        assert_eq!(got.as_deref(), Some(format!("value{i}").as_bytes()), "{k}");
    }
}

/// Reads a real Pebble v2 table-format-v7 sstable whose values are *separated* into a native blob
/// file, resolving them via the columnar reader's native-blob resolver. Fixtures: a v7 sstable
/// (`pebble_v2_v7_separated.sst`) whose value column holds inline blob handles, and its blob file
/// (`pebble_v2_v7_separated.blob`). All five values (key00000..key00004 => "V<i>-"*20) are stored
/// out-of-line and must resolve through the handle → reference → blob-file chain.
#[test]
fn reads_pebble_v2_separated_v7_sstable_with_native_blob() {
    use pebbledb::DefaultComparer;
    use pebbledb::sstable::columnar::ColumnarReader;
    use pebbledb::sstable::pebble_blob::{Handle, NativeBlobResolver, PebbleBlobReader};
    use std::sync::Arc;

    // A resolver backed by the single blob file. The reference list maps reference_id 0 -> this
    // file's number; the resolver fetches by (block_id, value_id).
    struct OneFileResolver {
        file_num: u64,
        reader: PebbleBlobReader,
    }
    impl NativeBlobResolver for OneFileResolver {
        fn get(&self, file_num: u64, handle: Handle) -> pebbledb::Result<Vec<u8>> {
            assert_eq!(file_num, self.file_num);
            self.reader.get(handle)
        }
    }

    let blob_bytes = include_bytes!("fixtures/pebble_v2_v7_separated.blob").to_vec();
    let resolver = Arc::new(OneFileResolver {
        file_num: 6,
        reader: PebbleBlobReader::open(blob_bytes).expect("open blob"),
    });

    let sst = include_bytes!("fixtures/pebble_v2_v7_separated.sst").to_vec();
    let mut cr = ColumnarReader::open(sst, Arc::new(DefaultComparer)).expect("open sst");
    // reference_id 0 -> blob file number 6.
    cr.attach_blob_resolver(vec![6], resolver);

    let all = cr.iter_all().expect("iter_all");
    assert_eq!(all.len(), 5);
    for (i, (_ik, v)) in all.iter().enumerate() {
        let want = format!("V{i}-").repeat(20);
        assert_eq!(v.as_slice(), want.as_bytes(), "separated value {i}");
    }
}
