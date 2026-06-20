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

        // Compaction should have kept L0 bounded.
        let m = db.metrics();
        assert!(m.level_files[0] < 4, "L0 not drained: {:?}", m.level_files);

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
fn tombstone_density_compaction_drains_a_dense_file() {
    let dir = temp_dir("tombstone-density");
    let db = Db::open(&dir, Options::default()).unwrap();

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
    let f1 = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .find(|n| n.ends_with(".sst"))
        .unwrap();
    assert_eq!(
        total_deletions(&dir),
        1,
        "bottom file should carry the tombstone"
    );

    // Writing another key triggers a flush whose maybe_compact runs the elision-only pass,
    // rewriting the bottom file without its now-dead tombstone.
    db.set(b"m", b"v").unwrap();
    db.flush().unwrap();

    assert!(
        !dir.join(&f1).exists(),
        "bottom file {f1} should be rewritten by elision-only compaction"
    );
    assert_eq!(
        total_deletions(&dir),
        0,
        "the bottom-level tombstone should have been elided"
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
            std::thread::sleep(Duration::from_millis(5));
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
    // A high L0 threshold lets many L0 files accumulate without compaction (the default
    // of 4 would have drained them), proving the tunable is wired through.
    let dir = temp_dir("l0-threshold");
    let opts = Options {
        mem_table_size: 4 * 1024,
        l0_compaction_threshold: 100,
        ..Default::default()
    };
    let db = Db::open(&dir, opts).unwrap();
    for i in 0..400u32 {
        db.set(format!("k{i:05}").as_bytes(), &[b'v'; 64]).unwrap();
        if i % 20 == 19 {
            db.flush().unwrap(); // force an L0 file
        }
    }
    let m = db.metrics();
    assert!(
        m.level_files[0] > 4,
        "high L0 threshold should let L0 accumulate, got {:?}",
        m.level_files
    );
    // Data is still correct.
    assert_eq!(db.get(b"k00000").unwrap(), Some(vec![b'v'; 64]));
    assert_eq!(db.get(b"k00399").unwrap(), Some(vec![b'v'; 64]));

    let _ = std::fs::remove_dir_all(&dir);
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
