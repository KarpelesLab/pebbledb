// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

//! End-to-end integration tests driving pebbledb through its public API: open, write,
//! flush, compact, snapshot, iterate, reopen, and recover.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use pebbledb::{Batch, Db, Options};

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
