// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

//! Writes a single columnar (Pebble `FormatColumnarBlocks`) sstable of known keys, for the
//! Rust→Go columnar interop direction (Go Pebble reads a columnar table the Rust writer
//! produced). Used by `.github/workflows/interop.yml`.
//!
//! ```text
//! cargo run --example columnar_sst_gen -- <file.sst>
//! ```
//!
//! Writes `key0000..key0099` => `value0..value99` as internal Set keys at descending sequence
//! numbers (so iteration order matches key order), using the columnar block layout.

use std::sync::Arc;

use pebbledb::DefaultComparer;
use pebbledb::base::internal_key::{InternalKey, InternalKeyKind};
use pebbledb::sstable::WriterOptions;
use pebbledb::sstable::columnar::ColumnarWriter;

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: columnar_sst_gen <file.sst>");
    let cmp = Arc::new(DefaultComparer);
    let mut w = ColumnarWriter::new(cmp, WriterOptions::default());
    let n = 100u64;
    for i in 0..n {
        let k = InternalKey::new(
            format!("key{i:04}").into_bytes(),
            n - i,
            InternalKeyKind::Set,
        )
        .encode();
        let v = format!("value{i}");
        w.add(&k, v.as_bytes()).expect("add");
    }
    let bytes = w.finish().expect("finish");
    std::fs::write(&path, &bytes).expect("write sst");
    println!("wrote {} columnar bytes to {path}", bytes.len());
}
