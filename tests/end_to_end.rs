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
