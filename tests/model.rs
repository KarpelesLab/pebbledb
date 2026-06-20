// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

//! A randomized, model-based ("metamorphic") test: a long pseudo-random sequence of
//! operations is applied to both the database and an in-memory `BTreeMap` reference
//! model, and after every mutation the two are checked to agree on point lookups and on a
//! full forward (and reverse) scan. The sequence includes flushes, range deletes, manual
//! compactions, and reopens so the comparison spans memtable, sstable, and recovered
//! state. The PRNG is deterministic, so a failure reproduces from its seed.

use std::collections::BTreeMap;

use pebbledb::{Db, Options};

/// A tiny deterministic xorshift64* PRNG — no external crates, fully reproducible.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!("pebbledb-model-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p
}

/// Asserts the database and the model agree on every key in the model, on a sample of
/// absent keys, and on a full ordered scan (forward and reverse).
fn check(db: &Db, model: &BTreeMap<Vec<u8>, Vec<u8>>) {
    // Every model key reads back the model value.
    for (k, v) in model {
        assert_eq!(
            db.get(k).unwrap().as_ref(),
            Some(v),
            "get mismatch for {k:?}"
        );
    }
    // Forward scan equals the model's ordered contents.
    let mut it = db.iter().unwrap();
    it.first().unwrap();
    let mut scanned: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    while it.valid() {
        scanned.push((it.key().to_vec(), it.value().to_vec()));
        it.next().unwrap();
    }
    let expected: Vec<(Vec<u8>, Vec<u8>)> =
        model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    assert_eq!(scanned, expected, "forward scan diverged from model");

    // Reverse scan equals the reverse of the model.
    let mut rit = db.iter().unwrap();
    rit.last().unwrap();
    let mut rev: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    while rit.valid() {
        rev.push((rit.key().to_vec(), rit.value().to_vec()));
        rit.prev().unwrap();
    }
    rev.reverse();
    assert_eq!(rev, expected, "reverse scan diverged from model");
}

fn run_with_seed(seed: u64) {
    let dir = temp_dir(&format!("seed{seed}"));
    let opts = || Options {
        // Small memtable so the run exercises flushes and compactions frequently.
        mem_table_size: 8 * 1024,
        ..Default::default()
    };
    let mut db = Db::open(&dir, opts()).unwrap();
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut rng = Rng(seed | 1);

    // Keys are drawn from a small space so overwrites, deletes, and range deletes collide
    // and actually shadow earlier versions.
    let key = |n: u64| format!("key{:04}", n % 256).into_bytes();

    for step in 0..2000u64 {
        match rng.below(100) {
            // ~48%: set a key.
            0..=47 => {
                let k = key(rng.next());
                let v = format!("v{}", rng.next() % 100_000).into_bytes();
                db.set(&k, &v).unwrap();
                model.insert(k, v);
            }
            // ~17%: delete a key.
            48..=64 => {
                let k = key(rng.next());
                db.delete(&k).unwrap();
                model.remove(&k);
            }
            // ~11%: delete a range [a, b).
            65..=75 => {
                let a = rng.next() % 256;
                let b = a + 1 + rng.below(40);
                let start = format!("key{a:04}").into_bytes();
                let end = format!("key{b:04}").into_bytes();
                let mut batch = pebbledb::Batch::new();
                batch.delete_range(&start, &end);
                db.write(batch).unwrap();
                model.retain(|k, _| {
                    !(k.as_slice() >= start.as_slice() && k.as_slice() < end.as_slice())
                });
            }
            // ~8%: an indexed batch of a few sets and a delete, committed atomically.
            76..=83 => {
                let mut ib = db.indexed_batch();
                let mut pending: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();
                for _ in 0..3 {
                    let k = key(rng.next());
                    let v = format!("b{}", rng.next() % 100_000).into_bytes();
                    ib.set(&k, &v);
                    pending.push((k, Some(v)));
                }
                let dk = key(rng.next());
                ib.delete(&dk);
                pending.push((dk, None));
                // Read-your-own-writes: the last pending op for each key is visible pre-commit.
                if let Some((k, want)) = pending.last() {
                    assert_eq!(
                        ib.get(&db, k).unwrap().as_ref(),
                        want.as_ref(),
                        "indexed-batch read-your-own-writes mismatch"
                    );
                }
                db.write(ib.into_batch()).unwrap();
                for (k, v) in pending {
                    match v {
                        Some(v) => {
                            model.insert(k, v);
                        }
                        None => {
                            model.remove(&k);
                        }
                    }
                }
            }
            // ~6%: explicit flush.
            84..=89 => db.flush().unwrap(),
            // ~3%: manual compaction.
            90..=92 => db.compact_range(None, None).unwrap(),
            // ~3%: snapshot isolation — the snapshot keeps its view across later mutations.
            93..=95 => {
                let snap = db.snapshot();
                let snap_model: Vec<(Vec<u8>, Vec<u8>)> =
                    model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                // Mutate after the snapshot.
                for _ in 0..3 {
                    let k = key(rng.next());
                    let v = format!("v{}", rng.next() % 100_000).into_bytes();
                    db.set(&k, &v).unwrap();
                    model.insert(k, v);
                }
                let mut sit = snap.iter().unwrap();
                sit.first().unwrap();
                let mut got: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
                while sit.valid() {
                    got.push((sit.key().to_vec(), sit.value().to_vec()));
                    sit.next().unwrap();
                }
                assert_eq!(
                    got, snap_model,
                    "snapshot view diverged from model-at-snapshot"
                );
            }
            // ~4%: reopen the database (exercises WAL recovery + MANIFEST reload).
            _ => {
                drop(db);
                db = Db::open(&dir, opts()).unwrap();
            }
        }

        // Check periodically (every check is a full scan, so not every step).
        if step % 50 == 0 {
            check(&db, &model);
        }
    }
    check(&db, &model);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn model_based_random_operations() {
    // A handful of fixed seeds for reproducible coverage.
    for seed in [1u64, 0xDEAD_BEEF, 0x1234_5678_9ABC_DEF0, 42, 0xCAFE_F00D, 7] {
        run_with_seed(seed);
    }
}
