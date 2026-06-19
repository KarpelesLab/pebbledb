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
