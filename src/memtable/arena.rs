// Copyright 2017 Dgraph Labs, Inc. and Contributors
// Modifications copyright (C) 2017 Andy Kimball and Contributors
// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
//
// Ported from Pebble's internal/arenaskl/arena.go (Apache-2.0); distributed
// under this crate's BSD-3-Clause license.

//! A fixed-capacity bump allocator backing the skiplist.
//!
//! The arena owns a single contiguous, 4-byte-aligned byte buffer that never moves or
//! reallocates. Space is handed out by a lock-free atomic bump pointer. Link fields are
//! accessed as [`AtomicU32`]s overlaid on the buffer; node header fields and key/value
//! bytes are written once (before the node is published via a release CAS on its links)
//! and thereafter read-only, so non-atomic access is data-race free given the happens-
//! before established by the link atomics.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU32, Ordering};

/// A lock-free, fixed-capacity arena.
pub(super) struct Arena {
    // Backing store, allocated as `u32` words to guarantee 4-byte alignment so that
    // `AtomicU32` overlays are aligned. Never reallocated. Boxed and kept alive for the
    // lifetime of `base`.
    _buf: Box<[UnsafeCell<u32>]>,
    base: *mut u8,
    cap: u32,
    n: AtomicU32,
}

impl Arena {
    /// Creates an arena with `size` bytes of capacity (rounded up to a multiple of 4).
    pub(super) fn new(size: usize) -> Arena {
        let words = size.div_ceil(4).max(1);
        let buf: Box<[UnsafeCell<u32>]> = (0..words).map(|_| UnsafeCell::new(0)).collect();
        let base = buf.as_ptr() as *mut u8;
        Arena {
            _buf: buf,
            base,
            cap: (words * 4) as u32,
            // Offset 0 is reserved so it can serve as a null node pointer.
            n: AtomicU32::new(1),
        }
    }

    /// The number of bytes handed out so far (saturating at capacity).
    pub(super) fn size(&self) -> u32 {
        self.n.load(Ordering::Acquire).min(self.cap)
    }

    /// Allocates `size` bytes aligned to `align` (a power of two), returning the offset
    /// or `None` if the arena is full.
    pub(super) fn alloc(&self, size: u32, align: u32) -> Option<u32> {
        debug_assert!(align.is_power_of_two());
        let padded = size + align - 1;
        // fetch_add returns the previous value; the new high-water mark is prev + padded.
        let new_size = self
            .n
            .fetch_add(padded, Ordering::AcqRel)
            .checked_add(padded)?;
        if new_size > self.cap {
            return None;
        }
        Some((new_size - size) & !(align - 1))
    }

    /// Returns the `AtomicU32` overlaid at byte `offset` (which must be 4-byte aligned
    /// and within an allocated node's link region).
    pub(super) fn link(&self, offset: usize) -> &AtomicU32 {
        debug_assert!(offset.is_multiple_of(4));
        debug_assert!(offset + 4 <= self.cap as usize);
        // SAFETY: `offset` is 4-aligned and in bounds; link fields are only ever
        // accessed through this atomic view, never as plain bytes.
        unsafe { &*(self.base.add(offset) as *const AtomicU32) }
    }

    /// Reads a little-endian `u32` written at `offset` (a node header field).
    pub(super) fn read_u32(&self, offset: usize) -> u32 {
        debug_assert!(offset + 4 <= self.cap as usize);
        // SAFETY: in bounds; the field was written before the node was published and is
        // immutable thereafter.
        unsafe { (self.base.add(offset) as *const u32).read_unaligned() }
    }

    /// Writes a little-endian `u32` at `offset` (called before the node is published).
    pub(super) fn write_u32(&self, offset: usize, value: u32) {
        debug_assert!(offset + 4 <= self.cap as usize);
        // SAFETY: in bounds; the region was just allocated and is exclusively owned by
        // the writer until publication.
        unsafe { (self.base.add(offset) as *mut u32).write_unaligned(value) }
    }

    /// Reads a little-endian `u64` written at `offset` (a node trailer).
    pub(super) fn read_u64(&self, offset: usize) -> u64 {
        debug_assert!(offset + 8 <= self.cap as usize);
        // SAFETY: see `read_u32`.
        unsafe { (self.base.add(offset) as *const u64).read_unaligned() }
    }

    /// Writes a little-endian `u64` at `offset` (called before the node is published).
    pub(super) fn write_u64(&self, offset: usize, value: u64) {
        debug_assert!(offset + 8 <= self.cap as usize);
        // SAFETY: see `write_u32`.
        unsafe { (self.base.add(offset) as *mut u64).write_unaligned(value) }
    }

    /// Borrows `len` bytes at `offset` (immutable node key/value data).
    pub(super) fn bytes(&self, offset: usize, len: usize) -> &[u8] {
        debug_assert!(offset + len <= self.cap as usize);
        // SAFETY: in bounds; key/value bytes are immutable after the node is published.
        unsafe { std::slice::from_raw_parts(self.base.add(offset), len) }
    }

    /// Copies `src` into the arena at `offset` (called before the node is published).
    pub(super) fn write_bytes(&self, offset: usize, src: &[u8]) {
        debug_assert!(offset + src.len() <= self.cap as usize);
        // SAFETY: in bounds; the region was just allocated and is exclusively owned by
        // the writer until publication. `src` cannot alias the arena.
        unsafe {
            std::ptr::copy_nonoverlapping(src.as_ptr(), self.base.add(offset), src.len());
        }
    }
}

// SAFETY: The arena's bytes are either accessed atomically (link fields) or written
// once before publication and read-only afterward, with happens-before provided by the
// link atomics. The raw `base` pointer aliases `_buf`, which lives as long as the arena.
unsafe impl Send for Arena {}
unsafe impl Sync for Arena {}
