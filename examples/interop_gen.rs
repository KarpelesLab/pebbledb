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
//! Writes `key0000..key0099` => `value0..value99` at a row (block-based) format major
//! version that upstream Pebble v2 still supports, then flushes so the data lands in an
//! sstable.

use pebbledb::{Db, FormatMajorVersion, Options};

fn main() {
    let dir = std::env::args()
        .nth(1)
        .expect("usage: interop_gen <dir> [columnar|separated]");
    // Optional second argument: "columnar" opens at the columnar format; "separated" opens at the
    // value-separation format with a blob threshold so large values land in native blob files.
    let mode = std::env::args().nth(2);
    let columnar = mode.as_deref() == Some("columnar");
    let separated = mode.as_deref() == Some("separated");
    let format_major_version = if separated {
        FormatMajorVersion::VALUE_SEPARATION
    } else if columnar {
        FormatMajorVersion::COLUMNAR_BLOCKS
    } else {
        // Pebble v2 dropped the oldest formats; FLUSHABLE_INGEST (13) is its minimum and is
        // still the classic row sstable layout, which Pebble v2 reads back.
        FormatMajorVersion::FLUSHABLE_INGEST
    };
    let opts = Options {
        format_major_version,
        // Separate values large enough that the known values (value0..value99) qualify.
        blob_value_threshold: if separated { Some(4) } else { None },
        ..Default::default()
    };
    let db = Db::open(&dir, opts).expect("open db");
    // Write the 100 keys in four flushed batches, so the columnar / value-separated modes produce
    // several L0 tables (each with its own native blob file when separating) that the compaction
    // below must merge — exercising compaction output, not just freshly-flushed L0 tables.
    for batch in 0..4 {
        for i in (batch * 25)..(batch * 25 + 25) {
            let k = format!("key{i:04}");
            let v = format!("value{i}");
            db.set(k.as_bytes(), v.as_bytes()).expect("set");
        }
        if columnar || separated {
            db.flush().expect("flush");
        }
    }
    db.flush().expect("flush");
    if columnar || separated {
        // Force a compaction so the interop also covers compaction output. For the value-separated
        // mode this proves a re-separating compaction (v7 output + a fresh native blob file) is
        // readable by upstream Pebble.
        db.compact_range(None, None).expect("compact");
    }
    println!("wrote 100 keys to {dir}");
}
