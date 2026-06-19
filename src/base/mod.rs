// Copyright (c) 2011 The LevelDB-Go Authors. All rights reserved.
// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

//! Foundational types shared across the engine: key comparison, internal keys, and the
//! low-level integer codecs used by every on-disk format.

pub mod comparer;
pub mod internal_key;
pub mod range_del;
pub mod range_key;
pub mod varint;

pub use comparer::{Comparer, DefaultComparer};
pub use internal_key::{
    InternalKey, InternalKeyKind, SEQNUM_BATCH_BIT, SEQNUM_MAX, SEQNUM_START, SEQNUM_ZERO, SeqNum,
    make_trailer, trailer_kind, trailer_seqnum,
};
pub use range_del::RangeTombstone;
