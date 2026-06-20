// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

//! Smoke tests for the `pebbledb` inspection CLI: build a database, then drive the
//! binary's subcommands against the files it produced.

use std::process::Command;

use pebbledb::{Db, Options};

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!("pebbledb-cli-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn run(args: &[&str]) -> (bool, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_pebbledb"))
        .args(args)
        .output()
        .expect("spawn pebbledb");
    let mut s = String::from_utf8_lossy(&out.stdout).into_owned();
    s.push_str(&String::from_utf8_lossy(&out.stderr));
    (out.status.success(), s)
}

#[test]
fn db_scan_get_and_sstable_dump() {
    let dir = temp_dir("scan");
    {
        let db = Db::open(&dir, Options::default()).unwrap();
        db.set(b"alpha", b"one").unwrap();
        db.set(b"beta", b"two").unwrap();
        db.set(b"gamma", b"three").unwrap();
        db.flush().unwrap(); // produce an sstable + MANIFEST entry
    }

    // db scan lists every visible key.
    let (ok, out) = run(&["db", "scan", dir.to_str().unwrap()]);
    assert!(ok, "scan failed: {out}");
    assert!(out.contains("alpha => one"), "{out}");
    assert!(out.contains("gamma => three"), "{out}");
    assert!(out.contains("# 3 keys"), "{out}");

    // db get reads a single key.
    let (ok, out) = run(&["db", "get", dir.to_str().unwrap(), "beta"]);
    assert!(ok, "get failed: {out}");
    assert!(out.contains("two"), "{out}");

    // db get of a missing key fails.
    let (ok, _) = run(&["db", "get", dir.to_str().unwrap(), "missing"]);
    assert!(!ok, "get of missing key should fail");

    // sstable dump on the produced .sst file shows its entries.
    let sst = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.extension().is_some_and(|x| x == "sst"))
        .expect("an sstable should exist after flush");
    let (ok, out) = run(&["sstable", "dump", sst.to_str().unwrap()]);
    assert!(ok, "sstable dump failed: {out}");
    assert!(out.contains("# properties"), "{out}");
    assert!(out.contains("alpha @"), "{out}");
    assert!(out.contains("=> one"), "{out}");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn wal_dump_runs() {
    let dir = temp_dir("wal");
    {
        let db = Db::open(&dir, Options::default()).unwrap();
        // Do NOT flush, so the batches remain in the WAL on disk.
        db.set(b"x", b"1").unwrap();
        db.delete(b"y").unwrap();
    }
    let wal = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.extension().is_some_and(|x| x == "log"))
        .expect("a WAL should exist");
    let (ok, out) = run(&["wal", "dump", wal.to_str().unwrap()]);
    assert!(ok, "wal dump failed: {out}");
    assert!(out.contains("Set x => 1"), "{out}");
    assert!(out.contains("Delete y"), "{out}");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn db_lsm_runs() {
    let dir = temp_dir("lsm");
    {
        let db = Db::open(&dir, Options::default()).unwrap();
        for i in 0..50u32 {
            db.set(format!("k{i:03}").as_bytes(), b"v").unwrap();
        }
        db.flush().unwrap();
    }
    let (ok, out) = run(&["db", "lsm", dir.to_str().unwrap()]);
    assert!(ok, "db lsm failed: {out}");
    assert!(out.contains("files"), "{out}");
    assert!(out.contains(".sst"), "{out}");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn find_locates_a_key() {
    let dir = temp_dir("find");
    {
        let db = Db::open(&dir, Options::default()).unwrap();
        db.set(b"alpha", b"one").unwrap();
        db.flush().unwrap();
    }
    let (ok, out) = run(&["find", dir.to_str().unwrap(), "alpha"]);
    assert!(ok, "find failed: {out}");
    assert!(out.contains("found"), "{out}");
    assert!(out.contains("one"), "{out}");

    let (ok, out) = run(&["find", dir.to_str().unwrap(), "missing"]);
    assert!(ok); // not-found is a successful query
    assert!(out.contains("not found"), "{out}");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn bench_runs_and_reports() {
    let dir = temp_dir("bench");
    // A small count keeps the test fast; the binary creates the store itself.
    let (ok, out) = run(&["bench", dir.to_str().unwrap(), "500"]);
    assert!(ok, "bench failed: {out}");
    assert!(out.contains("write: 500 keys"), "{out}");
    assert!(out.contains("read:  500 keys"), "{out}");
    assert!(out.contains("500 found"), "{out}");
    assert!(out.contains("ops/sec"), "{out}");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn manifest_dump_runs() {
    let dir = temp_dir("manifest");
    {
        let db = Db::open(&dir, Options::default()).unwrap();
        db.set(b"k", b"v").unwrap();
        db.flush().unwrap();
    }
    let manifest = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("MANIFEST-"))
        })
        .expect("a MANIFEST should exist");
    let (ok, out) = run(&["manifest", "dump", manifest.to_str().unwrap()]);
    assert!(ok, "manifest dump failed: {out}");
    assert!(out.contains("edit "), "{out}");
    assert!(out.contains("version edits dumped"), "{out}");

    let _ = std::fs::remove_dir_all(&dir);
}
