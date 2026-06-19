// Copyright (c) 2011 The LevelDB-Go Authors. All rights reserved.
// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

//! # pebbledb
//!
//! A Rust port of [CockroachDB's Pebble](https://github.com/cockroachdb/pebble), an
//! LSM key-value storage engine in the LevelDB / RocksDB lineage.
//!
//! The port aims for **binary compatibility** with Pebble's on-disk formats (sstable,
//! write-ahead log, and MANIFEST) and exposes an **idiomatic Rust API** with Pebble
//! semantics.
//!
//! This crate is under active, bottom-up development; see the `ROADMAP.md` file in the
//! repository for the current status. The public API is not yet stable.
//!
//! ## Attribution
//!
//! This is a derivative work of Pebble (and, transitively, LevelDB-Go / RocksDB /
//! LevelDB) and is distributed under the same BSD-3-Clause license. See the `LICENSE`
//! and `NOTICE` files in the repository for full attribution.
#![forbid(unsafe_op_in_unsafe_fn)]
#![warn(missing_docs)]

pub mod base;
pub mod batch;
pub mod crc;
mod error;
pub mod memtable;
pub mod record;

pub use error::{Error, Result};
