// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

//! Writes a pebbledb database of known keys for the reverse interop direction (Go Pebble
//! reads what the Rust engine wrote). Used by `.github/workflows/interop.yml`.
//!
//! ```text
//! cargo run --example interop_gen -- <dir>
//! ```
//!
//! Writes `key0000..key0099` => `value0..value99` at the most-compatible format major
//! version, then flushes so the data lands in an sstable.

use pebbledb::{Db, FormatMajorVersion, Options};

fn main() {
    let dir = std::env::args().nth(1).expect("usage: interop_gen <dir>");
    let opts = Options {
        format_major_version: FormatMajorVersion::MOST_COMPATIBLE,
        ..Default::default()
    };
    let db = Db::open(&dir, opts).expect("open db");
    for i in 0..100 {
        let k = format!("key{i:04}");
        let v = format!("value{i}");
        db.set(k.as_bytes(), v.as_bytes()).expect("set");
    }
    db.flush().expect("flush");
    println!("wrote 100 keys to {dir}");
}
