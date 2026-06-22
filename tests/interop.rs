// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

//! Cross-implementation interop, driven from CI (`.github/workflows/interop.yml`).
//!
//! The Go `interop` tool (in `interop/go`) generates a real Pebble database; this test
//! opens it with the Rust engine and verifies the known keys. It is gated on the
//! `PEBBLEDB_INTEROP_DIR` environment variable, so a normal `cargo test` run (where the
//! variable is unset, and Go is not involved) skips it cleanly.

use pebbledb::{Db, Options};

#[test]
fn reads_database_written_by_go_pebble() {
    let Ok(dir) = std::env::var("PEBBLEDB_INTEROP_DIR") else {
        eprintln!("PEBBLEDB_INTEROP_DIR unset; skipping Go interop test");
        return;
    };

    let db = Db::open_read_only(&dir, Options::default()).expect("open Go-written DB read-only");
    for i in 0..100 {
        let k = format!("key{i:04}");
        let want = format!("value{i}");
        assert_eq!(
            db.get(k.as_bytes()).unwrap(),
            Some(want.into_bytes()),
            "key {k} read from Go-written Pebble DB"
        );
    }
}

/// Reads a Go-written **columnar** database that carries keyspans (a range deletion and a range
/// key). Gated on `PEBBLEDB_INTEROP_SPANS_DIR`. Verifies that the columnar keyspan blocks decode
/// through the full read path: the range deletion `[key00005, key00010)` removes those point keys,
/// while the surrounding keys remain. (The offline fixture test
/// `reads_pebble_v2_columnar_keyspans_fixture` additionally checks the range key payload.)
#[test]
fn reads_columnar_spans_database_written_by_go_pebble() {
    let Ok(dir) = std::env::var("PEBBLEDB_INTEROP_SPANS_DIR") else {
        eprintln!("PEBBLEDB_INTEROP_SPANS_DIR unset; skipping Go columnar-spans interop test");
        return;
    };

    let db = Db::open_read_only(&dir, Options::default()).expect("open Go-written spans DB");
    for i in 0..20u32 {
        let k = format!("key{i:05}");
        let got = db.get(k.as_bytes()).unwrap();
        if (5..10).contains(&i) {
            assert_eq!(got, None, "{k} should be removed by the range deletion");
        } else {
            assert_eq!(
                got.as_deref(),
                Some(format!("value{i}").as_bytes()),
                "key {k} read from Go-written columnar DB"
            );
        }
    }
}
