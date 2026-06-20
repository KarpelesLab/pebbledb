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
//! ## Example
//!
//! ```
//! use pebbledb::{Db, Options};
//!
//! # let dir = std::env::temp_dir().join("pebbledb-doc-example");
//! # let _ = std::fs::remove_dir_all(&dir);
//! let db = Db::open(&dir, Options::default())?;
//! db.set(b"hello", b"world")?;
//! assert_eq!(db.get(b"hello")?, Some(b"world".to_vec()));
//!
//! // A snapshot reads a consistent view even as later writes land.
//! let snap = db.snapshot();
//! db.set(b"hello", b"again")?;
//! assert_eq!(snap.get(b"hello")?, Some(b"world".to_vec()));
//! assert_eq!(db.get(b"hello")?, Some(b"again".to_vec()));
//!
//! db.delete(b"hello")?;
//! assert_eq!(db.get(b"hello")?, None);
//! # std::fs::remove_dir_all(&dir).ok();
//! # Ok::<(), pebbledb::Error>(())
//! ```
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
pub mod cache;
pub mod crc;
pub mod db;
mod error;
pub mod manifest;
pub mod memtable;
pub mod objstorage;
pub mod record;
pub mod sstable;
pub mod vfs;
pub mod xxhash;

pub use base::comparer::{Comparer, DefaultComparer};
pub use base::merge::{ConcatMerger, Merger};
pub use batch::Batch;
pub use db::{
    ArchiveCleaner, CheckpointOptions, Cleaner, Db, DbIterator, DeleteCleaner, EventListener,
    FormatMajorVersion, IndexedBatch, InternalScan, IterOptions, Logger, Metrics, Options,
    OptionsFile, Snapshot, TableStats, new_external_iter,
};
pub use error::{Error, Result};
