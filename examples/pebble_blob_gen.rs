// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

//! Writes a Pebble native blob file with `PebbleBlobWriter`, for the Rust→Go blob-format interop
//! check (Go Pebble reads it back via `blob.FileReader`).
//!
//! ```text
//! cargo run --example pebble_blob_gen -- <file.blob>
//! ```
//!
//! Writes 20 values `value-0..value-19`.

use pebbledb::sstable::block::{ChecksumType, CompressionType};
use pebbledb::sstable::pebble_blob::PebbleBlobWriter;

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: pebble_blob_gen <file.blob>");
    let mut w = PebbleBlobWriter::new(CompressionType::None, ChecksumType::Crc32c, 1);
    for i in 0..20 {
        w.add_value(format!("value-{i}").as_bytes());
    }
    let bytes = w.finish().expect("finish");
    std::fs::write(&path, &bytes).expect("write");
    println!("wrote {} blob bytes to {path}", bytes.len());
}
