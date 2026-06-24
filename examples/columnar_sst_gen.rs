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
use pebbledb::base::range_key::{SuffixValue, encode_set_value};
use pebbledb::sstable::WriterOptions;
use pebbledb::sstable::columnar::ColumnarWriter;

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: columnar_sst_gen <file.sst> [spans]");
    // Optional second arg: "spans" also writes a range deletion and a range key; "v7" writes a
    // table-format-v7 table (columnar metaindex + v7 footer) instead of the default v5.
    let arg2 = std::env::args().nth(2);
    let with_spans = arg2.as_deref() == Some("spans");
    let v7 = arg2.as_deref() == Some("v7");
    // "minlz" writes a table-format-v7 table whose blocks use MinLZ compression (Pebble v2's
    // compression indicator 8), so the Go side can read it back through Pebble's MinLZ decoder.
    let minlz = arg2.as_deref() == Some("minlz");

    let cmp = Arc::new(DefaultComparer);
    let opts = WriterOptions {
        table_format: if v7 || minlz {
            pebbledb::sstable::TableFormat::Pebble(7)
        } else {
            WriterOptions::default().table_format
        },
        compression: if minlz {
            pebbledb::sstable::block::CompressionType::MinLZ
        } else {
            WriterOptions::default().compression
        },
        ..Default::default()
    };
    let mut w = ColumnarWriter::new(cmp, opts);
    let n = 100u64;
    for i in 0..n {
        let k = InternalKey::new(
            format!("key{i:04}").into_bytes(),
            n - i,
            InternalKeyKind::Set,
        )
        .encode();
        // In MinLZ mode use a long, compressible value so the data block genuinely compresses
        // with MinLZ rather than falling back to no compression.
        let v = if minlz {
            format!("value{i}-{}", "x".repeat(80))
        } else {
            format!("value{i}")
        };
        w.add(&k, v.as_bytes()).expect("add");
    }
    if with_spans {
        // Range deletion [key0030, key0040) and range key set [key0050, key0060)@1 = "rkval".
        let rd = InternalKey::new(b"key0030".to_vec(), 1, InternalKeyKind::RangeDelete).encode();
        w.add(&rd, b"key0040").expect("add range del");
        let rk = InternalKey::new(b"key0050".to_vec(), 1, InternalKeyKind::RangeKeySet).encode();
        let rk_val = encode_set_value(
            b"key0060",
            &[SuffixValue {
                suffix: b"@1".to_vec(),
                value: b"rkval".to_vec(),
            }],
        );
        w.add(&rk, &rk_val).expect("add range key");
    }
    let bytes = w.finish().expect("finish");
    std::fs::write(&path, &bytes).expect("write sst");
    println!("wrote {} columnar bytes to {path}", bytes.len());
}
