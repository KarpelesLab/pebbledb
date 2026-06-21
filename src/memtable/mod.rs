// Copyright 2017 Dgraph Labs, Inc. and Contributors
// Modifications copyright (C) 2017 Andy Kimball and Contributors
// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
//
// The skiplist is a port of Pebble's internal/arenaskl (arena.go, node.go,
// skl.go, iterator.go), which is in turn derived from Andy Kimball's arenaskl
// and Dgraph's Badger. arenaskl is licensed under the Apache License 2.0; this
// file is distributed under this crate's BSD-3-Clause license. The memtable
// wrapper is ported from Pebble's memtable.go.

//! The memtable: an in-memory, ordered, concurrent map of internal keys to values,
//! backed by an arena-allocated skiplist.
//!
//! Writes go to the [`MemTable`] via [`MemTable::apply`], which expands a [`Batch`] into
//! one skiplist entry per operation, assigning each the sequence number
//! `batch.seqnum() + op_index`. Reads ([`MemTable::get`], [`MemTable::iter`]) run
//! concurrently with a single writer.
//!
//! The skiplist allocates all of its nodes and key/value bytes from a single
//! fixed-capacity arena. When the arena fills, inserts fail with
//! [`Error::InvalidState`] and the database rotates to a fresh memtable. This mirrors
//! Pebble, where the arena size bounds the memtable size.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::base::comparer::Comparer;
use crate::base::internal_key::{InternalKeyKind, SeqNum, make_trailer, trailer_kind};
use crate::base::range_del::RangeTombstone;
use crate::base::range_key::RangeKeyEntry;
use crate::batch::Batch;
use crate::{Error, Result};

mod arena;
use arena::Arena;

/// The maximum height of the skiplist tower.
const MAX_HEIGHT: usize = 20;

// Node layout within the arena. All offsets are relative to the node's start.
const OFF_KEY_OFFSET: usize = 0; // u32: arena offset of the key bytes
const OFF_KEY_SIZE: usize = 4; // u32
const OFF_TRAILER: usize = 8; // u64
const OFF_VALUE_SIZE: usize = 16; // u32
// bytes 20..24 are padding so the tower begins 8-byte aligned (matching arenaskl).
const TOWER_OFFSET: usize = 24;
const LINK_SIZE: usize = 8; // next: u32, prev: u32
const MAX_NODE_SIZE: usize = TOWER_OFFSET + MAX_HEIGHT * LINK_SIZE;
const NODE_ALIGN: u32 = 4;

/// `nil` node offset. Offset 0 is reserved by the arena so it can act as a null pointer.
const NIL: u32 = 0;

/// The probability that a node is promoted to the next level, `1/e`, expressed as the
/// per-level `u32` threshold table used by [`Skiplist::random_height`].
fn height_probabilities() -> [u32; MAX_HEIGHT] {
    let p_value = 1.0 / std::f64::consts::E;
    let mut probs = [0u32; MAX_HEIGHT];
    let mut p = 1.0f64;
    for slot in probs.iter_mut() {
        *slot = (f64::from(u32::MAX) * p) as u32;
        p *= p_value;
    }
    probs
}

/// An arena-backed, ordered skiplist of internal keys to values.
///
/// Keys are ordered by the user-key [`Comparer`] and then by trailer descending (newer
/// sequence numbers first), matching the internal-key order used throughout the engine.
/// A single writer may [`Skiplist::add`] concurrently with any number of readers.
pub struct Skiplist {
    arena: Arena,
    cmp: Arc<dyn Comparer>,
    head: u32,
    tail: u32,
    height: AtomicU32,
    probabilities: [u32; MAX_HEIGHT],
    rnd: AtomicU32,
}

impl Skiplist {
    /// Creates a skiplist with an arena of `arena_size` bytes.
    pub fn new(cmp: Arc<dyn Comparer>, arena_size: usize) -> Self {
        let arena = Arena::new(arena_size);
        // Allocate the head and tail sentinel nodes at full height.
        let head = new_raw_node(&arena, MAX_HEIGHT as u32, 0, 0).expect("arena too small for head");
        let tail = new_raw_node(&arena, MAX_HEIGHT as u32, 0, 0).expect("arena too small for tail");
        for i in 0..MAX_HEIGHT {
            set_next(&arena, head, i, tail);
            set_prev(&arena, head, i, NIL);
            set_next(&arena, tail, i, NIL);
            set_prev(&arena, tail, i, head);
        }
        Skiplist {
            arena,
            cmp,
            head,
            tail,
            height: AtomicU32::new(1),
            probabilities: height_probabilities(),
            rnd: AtomicU32::new(0x9E37_79B9),
        }
    }

    /// The number of bytes allocated from the arena.
    pub fn size(&self) -> u32 {
        self.arena.size()
    }

    /// Whether the skiplist contains no entries.
    pub fn is_empty(&self) -> bool {
        next_off(&self.arena, self.head, 0) == self.tail
    }

    fn random_height(&self) -> u32 {
        // xorshift32; the exact distribution is unimportant, only that it is roughly
        // geometric with parameter 1/e.
        let mut x = self.rnd.load(Ordering::Relaxed);
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        self.rnd.store(x, Ordering::Relaxed);
        let mut h = 1;
        while h < MAX_HEIGHT && x <= self.probabilities[h] {
            h += 1;
        }
        h as u32
    }

    /// Inserts `(user_key, trailer) -> value`.
    ///
    /// Returns [`Error::InvalidState`] if the arena is full, or [`Error::Corruption`] if
    /// an entry with the identical internal key already exists (which should not happen
    /// for distinct sequence numbers).
    pub fn add(&self, user_key: &[u8], trailer: u64, value: &[u8]) -> Result<()> {
        let height = self.random_height();

        // Grow the list height to accommodate the new node.
        let mut list_h = self.height.load(Ordering::Acquire);
        while height > list_h {
            match self.height.compare_exchange_weak(
                list_h,
                height,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(h) => list_h = h,
            }
        }

        let nd = new_node(&self.arena, height, user_key, trailer, value)?;

        // Find the predecessor/successor at each level the node occupies.
        let mut prev = [self.head; MAX_HEIGHT];
        let mut next = [self.tail; MAX_HEIGHT];
        let mut search_prev = self.head;
        for level in (0..height as usize).rev() {
            let (p, n, found) = self.find_splice_for_level(user_key, trailer, level, search_prev);
            if found {
                return Err(Error::corruption("memtable: duplicate internal key"));
            }
            prev[level] = p;
            next[level] = n;
            search_prev = p;
        }

        // Link the node in from the bottom up, retrying a level if a concurrent writer
        // changed it (only possible with multiple writers).
        for level in 0..height as usize {
            loop {
                let p = prev[level];
                let n = next[level];
                set_next(&self.arena, nd, level, n);
                set_prev(&self.arena, nd, level, p);
                if cas_next(&self.arena, p, level, n, nd) {
                    cas_prev(&self.arena, n, level, p, nd);
                    break;
                }
                let (np, nn, found) = self.find_splice_for_level(user_key, trailer, level, p);
                if found {
                    return Err(Error::corruption("memtable: duplicate internal key"));
                }
                prev[level] = np;
                next[level] = nn;
            }
        }
        Ok(())
    }

    /// Finds `(prev, next, found)` at `level`: the node just before and just after
    /// `(user_key, trailer)` on this level, starting the search from `start`.
    fn find_splice_for_level(
        &self,
        user_key: &[u8],
        trailer: u64,
        level: usize,
        start: u32,
    ) -> (u32, u32, bool) {
        let mut prev = start;
        loop {
            let next = next_off(&self.arena, prev, level);
            if next == self.tail {
                return (prev, next, false);
            }
            let next_key = key_bytes(&self.arena, next);
            match self.cmp.compare(user_key, next_key) {
                std::cmp::Ordering::Less => return (prev, next, false),
                std::cmp::Ordering::Equal => {
                    let nt = node_trailer(&self.arena, next);
                    if trailer == nt {
                        return (prev, next, true);
                    }
                    if trailer > nt {
                        // Larger trailer sorts first, so key < next: done.
                        return (prev, next, false);
                    }
                }
                std::cmp::Ordering::Greater => {}
            }
            prev = next;
        }
    }

    /// Returns `(prev, next)` at level 0 bracketing `(user_key, trailer)`.
    fn seek_for_splice(&self, user_key: &[u8], trailer: u64) -> (u32, u32) {
        let mut prev = self.head;
        let mut next = self.tail;
        let list_h = self.height.load(Ordering::Acquire) as usize;
        for level in (0..list_h).rev() {
            let (p, n, _) = self.find_splice_for_level(user_key, trailer, level, prev);
            prev = p;
            next = n;
        }
        (prev, next)
    }

    /// Returns an iterator over the skiplist.
    pub fn iter(&self) -> SklIterator<'_> {
        SklIterator {
            list: self,
            nd: self.head,
        }
    }
}

// SAFETY: All shared mutable state lives in the `Arena` (whose interior mutability is
// documented there) or in atomics. Inserts publish nodes via release CAS on the tower
// links, and readers observe them via acquire loads, so concurrent readers and a single
// writer are data-race free. The `Arc<dyn Comparer>` is `Send + Sync`.
unsafe impl Send for Skiplist {}
unsafe impl Sync for Skiplist {}

// --- Node field accessors --------------------------------------------------------

fn new_node(
    arena: &Arena,
    height: u32,
    user_key: &[u8],
    trailer: u64,
    value: &[u8],
) -> Result<u32> {
    let nd = new_raw_node(arena, height, user_key.len() as u32, value.len() as u32)?;
    arena.write_u64(nd as usize + OFF_TRAILER, trailer);
    let key_off = arena.read_u32(nd as usize + OFF_KEY_OFFSET) as usize;
    arena.write_bytes(key_off, user_key);
    arena.write_bytes(key_off + user_key.len(), value);
    Ok(nd)
}

fn new_raw_node(arena: &Arena, height: u32, key_size: u32, value_size: u32) -> Result<u32> {
    assert!((1..=MAX_HEIGHT as u32).contains(&height));
    let unused = (MAX_HEIGHT - height as usize) * LINK_SIZE;
    let node_size = (MAX_NODE_SIZE - unused) as u32;
    let offset = arena
        .alloc(node_size + key_size + value_size, NODE_ALIGN)
        .ok_or_else(|| Error::InvalidState("memtable: arena is full".into()))?;
    arena.write_u32(offset as usize + OFF_KEY_OFFSET, offset + node_size);
    arena.write_u32(offset as usize + OFF_KEY_SIZE, key_size);
    arena.write_u32(offset as usize + OFF_VALUE_SIZE, value_size);
    Ok(offset)
}

fn key_bytes(arena: &Arena, nd: u32) -> &[u8] {
    let off = arena.read_u32(nd as usize + OFF_KEY_OFFSET) as usize;
    let size = arena.read_u32(nd as usize + OFF_KEY_SIZE) as usize;
    arena.bytes(off, size)
}

fn value_bytes(arena: &Arena, nd: u32) -> &[u8] {
    let key_off = arena.read_u32(nd as usize + OFF_KEY_OFFSET) as usize;
    let key_size = arena.read_u32(nd as usize + OFF_KEY_SIZE) as usize;
    let value_size = arena.read_u32(nd as usize + OFF_VALUE_SIZE) as usize;
    arena.bytes(key_off + key_size, value_size)
}

fn node_trailer(arena: &Arena, nd: u32) -> u64 {
    arena.read_u64(nd as usize + OFF_TRAILER)
}

fn link_offset(nd: u32, level: usize) -> usize {
    nd as usize + TOWER_OFFSET + level * LINK_SIZE
}

fn next_off(arena: &Arena, nd: u32, level: usize) -> u32 {
    arena.link(link_offset(nd, level)).load(Ordering::Acquire)
}

fn prev_off(arena: &Arena, nd: u32, level: usize) -> u32 {
    arena
        .link(link_offset(nd, level) + 4)
        .load(Ordering::Acquire)
}

fn set_next(arena: &Arena, nd: u32, level: usize, val: u32) {
    arena
        .link(link_offset(nd, level))
        .store(val, Ordering::Release);
}

fn set_prev(arena: &Arena, nd: u32, level: usize, val: u32) {
    arena
        .link(link_offset(nd, level) + 4)
        .store(val, Ordering::Release);
}

fn cas_next(arena: &Arena, nd: u32, level: usize, old: u32, val: u32) -> bool {
    arena
        .link(link_offset(nd, level))
        .compare_exchange(old, val, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
}

fn cas_prev(arena: &Arena, nd: u32, level: usize, old: u32, val: u32) -> bool {
    arena
        .link(link_offset(nd, level) + 4)
        .compare_exchange(old, val, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
}

/// A forward/backward iterator over a [`Skiplist`].
pub struct SklIterator<'a> {
    list: &'a Skiplist,
    nd: u32,
}

impl<'a> SklIterator<'a> {
    /// Whether the iterator is positioned at a valid entry.
    pub fn valid(&self) -> bool {
        self.nd != self.list.head && self.nd != self.list.tail
    }

    /// The user key at the current position. Only valid when [`valid`](Self::valid).
    pub fn user_key(&self) -> &'a [u8] {
        key_bytes(&self.list.arena, self.nd)
    }

    /// The value at the current position.
    pub fn value(&self) -> &'a [u8] {
        value_bytes(&self.list.arena, self.nd)
    }

    /// The trailer at the current position.
    pub fn trailer(&self) -> u64 {
        node_trailer(&self.list.arena, self.nd)
    }

    /// The key kind at the current position.
    pub fn kind(&self) -> InternalKeyKind {
        trailer_kind(self.trailer())
    }

    /// Positions at the first entry whose internal key is `>= (user_key, trailer)`.
    pub fn seek_ge(&mut self, user_key: &[u8], trailer: u64) {
        let (_, next) = self.list.seek_for_splice(user_key, trailer);
        self.nd = next;
    }

    /// Positions at the last entry whose internal key is `< (user_key, trailer)`.
    pub fn seek_lt(&mut self, user_key: &[u8], trailer: u64) {
        let (prev, _) = self.list.seek_for_splice(user_key, trailer);
        self.nd = prev;
    }

    /// Positions at the first entry.
    pub fn first(&mut self) {
        self.nd = next_off(&self.list.arena, self.list.head, 0);
    }

    /// Positions at the last entry.
    pub fn last(&mut self) {
        self.nd = prev_off(&self.list.arena, self.list.tail, 0);
    }

    /// Advances to the next entry. Becomes invalid past the end.
    pub fn next(&mut self) {
        if self.nd != self.list.tail {
            self.nd = next_off(&self.list.arena, self.nd, 0);
        }
    }

    /// Steps back to the previous entry. Becomes invalid past the beginning.
    pub fn prev(&mut self) {
        if self.nd != self.list.head {
            self.nd = prev_off(&self.list.arena, self.nd, 0);
        }
    }
}

/// An in-memory write buffer: a [`Skiplist`] of point keys plus a list of range
/// tombstones, with batch application.
pub struct MemTable {
    skl: Skiplist,
    /// Range tombstones applied to this memtable, kept separate from point keys.
    range_dels: std::sync::Mutex<Vec<RangeTombstone>>,
    /// Range-key entries applied to this memtable, kept separate from point keys.
    range_keys: std::sync::Mutex<Vec<RangeKeyEntry>>,
}

impl MemTable {
    /// Creates a memtable with an arena of `arena_size` bytes ordered by `cmp`.
    pub fn new(cmp: Arc<dyn Comparer>, arena_size: usize) -> Self {
        MemTable {
            skl: Skiplist::new(cmp, arena_size),
            range_dels: std::sync::Mutex::new(Vec::new()),
            range_keys: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Applies every operation in `batch`, assigning the i-th operation the sequence
    /// number `batch.seqnum() + i`. [`InternalKeyKind::LogData`] operations are skipped
    /// (but still consume a sequence-number slot).
    pub fn apply(&self, batch: &Batch) -> Result<()> {
        let base = batch.seqnum();
        for (i, op) in batch.iter().enumerate() {
            let op = op?;
            let seqnum = base + i as u64;
            match op.kind {
                InternalKeyKind::LogData => continue,
                InternalKeyKind::RangeDelete => {
                    // start = key, end = value; kept in the range-del list, not the
                    // point-key skiplist.
                    self.range_dels.lock().unwrap().push(RangeTombstone::new(
                        op.key.to_vec(),
                        op.value.unwrap_or(&[]).to_vec(),
                        seqnum,
                    ));
                }
                InternalKeyKind::RangeKeySet
                | InternalKeyKind::RangeKeyUnset
                | InternalKeyKind::RangeKeyDelete => {
                    // start = key, value = encoded (end + payload); kept in the
                    // range-key list.
                    self.range_keys.lock().unwrap().push(RangeKeyEntry {
                        kind: op.kind,
                        start: op.key.to_vec(),
                        seqnum,
                        value: op.value.unwrap_or(&[]).to_vec(),
                    });
                }
                _ => {
                    let trailer = make_trailer(seqnum, op.kind);
                    self.skl.add(op.key, trailer, op.value.unwrap_or(&[]))?;
                }
            }
        }
        Ok(())
    }

    /// Returns every version of `user_key` visible at `snapshot`, newest first. Used to
    /// resolve merge operands.
    pub fn lookup_versions(
        &self,
        user_key: &[u8],
        snapshot: SeqNum,
    ) -> Vec<(SeqNum, InternalKeyKind, Vec<u8>)> {
        let trailer = (snapshot << 8) | 0xff;
        let mut it = self.skl.iter();
        it.seek_ge(user_key, trailer);
        let mut out = Vec::new();
        while it.valid()
            && self.skl.cmp.compare(it.user_key(), user_key) == std::cmp::Ordering::Equal
        {
            out.push((it.trailer() >> 8, it.kind(), it.value().to_vec()));
            it.next();
        }
        out
    }

    /// Returns a copy of the memtable's range tombstones.
    pub fn range_tombstones(&self) -> Vec<RangeTombstone> {
        self.range_dels.lock().unwrap().clone()
    }

    /// Returns a copy of the memtable's range-key entries.
    pub fn range_keys(&self) -> Vec<RangeKeyEntry> {
        self.range_keys.lock().unwrap().clone()
    }

    /// Inserts a single internal key/value pair directly.
    pub fn add(
        &self,
        user_key: &[u8],
        seqnum: SeqNum,
        kind: InternalKeyKind,
        value: &[u8],
    ) -> Result<()> {
        self.skl.add(user_key, make_trailer(seqnum, kind), value)
    }

    /// Looks up `user_key` as visible at `snapshot`, returning the kind and value of the
    /// most recent entry with sequence number `<= snapshot`, or `None` if there is none.
    ///
    /// A returned [`InternalKeyKind::Delete`] / [`InternalKeyKind::SingleDelete`] is a
    /// tombstone; the caller treats it as "not found" but must not look in older levels.
    pub fn get(&self, user_key: &[u8], snapshot: SeqNum) -> Option<(InternalKeyKind, Vec<u8>)> {
        self.lookup(user_key, snapshot).map(|(_, k, v)| (k, v))
    }

    /// Like [`MemTable::get`] but also returns the entry's sequence number, used by the
    /// database to compare point keys against range tombstones.
    pub fn lookup(
        &self,
        user_key: &[u8],
        snapshot: SeqNum,
    ) -> Option<(SeqNum, InternalKeyKind, Vec<u8>)> {
        // A trailer of (snapshot << 8 | 0xff) sorts immediately before any real entry at
        // `snapshot`, so SeekGE lands on the newest entry with seqnum <= snapshot.
        let trailer = (snapshot << 8) | 0xff;
        let mut it = self.skl.iter();
        it.seek_ge(user_key, trailer);
        if !it.valid() {
            return None;
        }
        if self.skl.cmp.compare(it.user_key(), user_key) != std::cmp::Ordering::Equal {
            return None;
        }
        Some((it.trailer() >> 8, it.kind(), it.value().to_vec()))
    }

    /// Whether the memtable has no point keys, range tombstones, or range keys.
    pub fn is_empty(&self) -> bool {
        self.skl.is_empty()
            && self.range_dels.lock().unwrap().is_empty()
            && self.range_keys.lock().unwrap().is_empty()
    }

    /// The number of bytes allocated from the arena.
    pub fn size(&self) -> u32 {
        self.skl.size()
    }

    /// An iterator over the memtable's internal keys.
    pub fn iter(&self) -> SklIterator<'_> {
        self.skl.iter()
    }

    /// Returns a forward iterator that owns a shared reference to the memtable, so it can
    /// be stored alongside sstable iterators in a merging iterator. It yields *encoded*
    /// internal keys (user key followed by the little-endian trailer).
    pub fn scan(self: &Arc<Self>) -> OwnedMemIter {
        let nd = self.skl.head;
        OwnedMemIter {
            mem: Arc::clone(self),
            nd,
            key_buf: Vec::new(),
        }
    }
}

/// A forward iterator over a [`MemTable`] that owns an `Arc` to it and produces encoded
/// internal keys.
#[derive(Clone)]
pub struct OwnedMemIter {
    mem: Arc<MemTable>,
    nd: u32,
    key_buf: Vec<u8>,
}

impl OwnedMemIter {
    fn rebuild_key(&mut self) {
        if self.valid() {
            let arena = &self.mem.skl.arena;
            self.key_buf.clear();
            self.key_buf.extend_from_slice(key_bytes(arena, self.nd));
            self.key_buf
                .extend_from_slice(&node_trailer(arena, self.nd).to_le_bytes());
        }
    }

    /// Positions at the first entry.
    pub fn first(&mut self) {
        self.nd = next_off(&self.mem.skl.arena, self.mem.skl.head, 0);
        self.rebuild_key();
    }

    /// Whether the iterator is at a valid entry.
    pub fn valid(&self) -> bool {
        self.nd != self.mem.skl.head && self.nd != self.mem.skl.tail
    }

    /// The current encoded internal key.
    pub fn key(&self) -> &[u8] {
        &self.key_buf
    }

    /// The current value.
    pub fn value(&self) -> &[u8] {
        value_bytes(&self.mem.skl.arena, self.nd)
    }

    /// Advances to the next entry.
    pub fn next(&mut self) {
        if self.nd != self.mem.skl.tail {
            self.nd = next_off(&self.mem.skl.arena, self.nd, 0);
        }
        self.rebuild_key();
    }

    /// Positions at the last entry.
    pub fn last(&mut self) {
        self.nd = prev_off(&self.mem.skl.arena, self.mem.skl.tail, 0);
        self.rebuild_key();
    }

    /// Steps back to the previous entry.
    pub fn prev(&mut self) {
        if self.nd != self.mem.skl.head {
            self.nd = prev_off(&self.mem.skl.arena, self.nd, 0);
        }
        self.rebuild_key();
    }

    /// Splits an encoded internal key into its user key and trailer.
    fn split(target: &[u8]) -> (&[u8], u64) {
        let n = target.len() - 8;
        let trailer = u64::from_le_bytes(target[n..].try_into().unwrap());
        (&target[..n], trailer)
    }

    /// Positions at the first entry whose internal key is `>= target`.
    pub fn seek_ge(&mut self, target: &[u8]) {
        let (uk, tr) = Self::split(target);
        let (_, next) = self.mem.skl.seek_for_splice(uk, tr);
        self.nd = next;
        self.rebuild_key();
    }

    /// Positions at the last entry whose internal key is `< target`.
    pub fn seek_lt(&mut self, target: &[u8]) {
        let (uk, tr) = Self::split(target);
        let (prev, _) = self.mem.skl.seek_for_splice(uk, tr);
        self.nd = prev;
        self.rebuild_key();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::comparer::DefaultComparer;

    fn skl() -> Skiplist {
        Skiplist::new(Arc::new(DefaultComparer), 1 << 20)
    }

    #[test]
    fn empty_skiplist() {
        let s = skl();
        assert!(s.is_empty());
        assert!(!s.iter().valid());
    }

    #[test]
    fn insert_and_iterate_in_order() {
        let s = skl();
        // Insert out of order; expect sorted iteration.
        for (k, seq) in [("c", 3u64), ("a", 1), ("b", 2), ("e", 5), ("d", 4)] {
            s.add(
                k.as_bytes(),
                make_trailer(seq, InternalKeyKind::Set),
                k.as_bytes(),
            )
            .unwrap();
        }
        assert!(!s.is_empty());
        let mut it = s.iter();
        it.first();
        let mut keys = Vec::new();
        while it.valid() {
            keys.push(String::from_utf8(it.user_key().to_vec()).unwrap());
            it.next();
        }
        assert_eq!(keys, ["a", "b", "c", "d", "e"]);

        // Reverse iteration.
        it.last();
        let mut rkeys = Vec::new();
        while it.valid() {
            rkeys.push(String::from_utf8(it.user_key().to_vec()).unwrap());
            it.prev();
        }
        assert_eq!(rkeys, ["e", "d", "c", "b", "a"]);
    }

    #[test]
    fn same_user_key_orders_newest_first() {
        let s = skl();
        s.add(b"k", make_trailer(1, InternalKeyKind::Set), b"v1")
            .unwrap();
        s.add(b"k", make_trailer(3, InternalKeyKind::Set), b"v3")
            .unwrap();
        s.add(b"k", make_trailer(2, InternalKeyKind::Set), b"v2")
            .unwrap();
        let mut it = s.iter();
        it.first();
        let mut seqs = Vec::new();
        while it.valid() {
            seqs.push(it.trailer() >> 8);
            it.next();
        }
        // Larger sequence numbers (newer) come first.
        assert_eq!(seqs, [3, 2, 1]);
    }

    #[test]
    fn seek_ge_and_lt() {
        let s = skl();
        for k in ["a", "c", "e", "g"] {
            s.add(k.as_bytes(), make_trailer(1, InternalKeyKind::Set), b"")
                .unwrap();
        }
        let mut it = s.iter();
        // SeekGE("b") -> "c"
        it.seek_ge(b"b", (10u64 << 8) | 0xff);
        assert!(it.valid());
        assert_eq!(it.user_key(), b"c");
        // SeekGE("e") -> "e"
        it.seek_ge(b"e", (10u64 << 8) | 0xff);
        assert_eq!(it.user_key(), b"e");
        // SeekLT("e") with the max trailer (smallest internal key for "e") positions
        // strictly before every "e" entry -> "c".
        it.seek_lt(b"e", u64::MAX);
        assert_eq!(it.user_key(), b"c");
        // SeekGE past end -> invalid
        it.seek_ge(b"z", (10u64 << 8) | 0xff);
        assert!(!it.valid());
    }

    #[test]
    fn memtable_get_respects_snapshot_and_tombstones() {
        let cmp: Arc<dyn Comparer> = Arc::new(DefaultComparer);
        let m = MemTable::new(cmp, 1 << 20);
        m.add(b"key", 10, InternalKeyKind::Set, b"v10").unwrap();
        m.add(b"key", 20, InternalKeyKind::Set, b"v20").unwrap();
        m.add(b"key", 30, InternalKeyKind::Delete, b"").unwrap();

        // Snapshot 25 sees v20.
        assert_eq!(
            m.get(b"key", 25),
            Some((InternalKeyKind::Set, b"v20".to_vec()))
        );
        // Snapshot 15 sees v10.
        assert_eq!(
            m.get(b"key", 15),
            Some((InternalKeyKind::Set, b"v10".to_vec()))
        );
        // Snapshot 35 sees the tombstone.
        assert_eq!(
            m.get(b"key", 35),
            Some((InternalKeyKind::Delete, Vec::new()))
        );
        // Below all writes: not present.
        assert_eq!(m.get(b"key", 5), None);
        // Unknown key.
        assert_eq!(m.get(b"missing", 100), None);
    }

    #[test]
    fn memtable_apply_assigns_sequence_numbers() {
        let cmp: Arc<dyn Comparer> = Arc::new(DefaultComparer);
        let m = MemTable::new(cmp, 1 << 20);
        let mut b = Batch::new();
        b.set(b"a", b"1");
        b.log_data(b"ignored"); // consumes index 1 but is not stored
        b.set(b"b", b"2");
        b.set_seqnum(100);
        m.apply(&b).unwrap();

        // op 0 -> seqnum 100, op 2 -> seqnum 102 (op 1 is LogData).
        let mut it = m.iter();
        it.seek_ge(b"a", u64::MAX);
        assert_eq!(it.trailer() >> 8, 100);
        it.seek_ge(b"b", u64::MAX);
        assert_eq!(it.trailer() >> 8, 102);
        assert_eq!(
            m.get(b"a", 200),
            Some((InternalKeyKind::Set, b"1".to_vec()))
        );
    }

    #[test]
    fn arena_full_returns_error() {
        // A tiny arena fills quickly; once full, add fails cleanly.
        let s = Skiplist::new(Arc::new(DefaultComparer), 2 * MAX_NODE_SIZE + 256);
        let mut filled = false;
        for i in 0..10_000u32 {
            let key = i.to_be_bytes();
            if s.add(&key, make_trailer(i as u64, InternalKeyKind::Set), b"value")
                .is_err()
            {
                filled = true;
                break;
            }
        }
        assert!(filled, "expected the arena to fill up");
    }

    #[test]
    fn concurrent_reads_during_writes() {
        use std::thread;
        // Miri models every memory access (and runs ~50x slower), so scale the work down under
        // it: a smaller arena and far fewer iterations still exercise the lock-free read/write
        // interleaving its data-race and aliasing checks care about, without taking forever.
        let (arena, total, reader_iters) = if cfg!(miri) {
            (1 << 18, 400u32, 40)
        } else {
            (8 << 20, 2000u32, 2000)
        };
        let s = Arc::new(Skiplist::new(Arc::new(DefaultComparer), arena));
        // Pre-populate so readers have something to find.
        for i in 0..100u32 {
            s.add(
                &i.to_be_bytes(),
                make_trailer(i as u64, InternalKeyKind::Set),
                b"v",
            )
            .unwrap();
        }
        let reader = {
            let s = Arc::clone(&s);
            thread::spawn(move || {
                for _ in 0..reader_iters {
                    let mut it = s.iter();
                    it.first();
                    let mut count = 0;
                    let mut last: Option<Vec<u8>> = None;
                    while it.valid() {
                        let k = it.user_key().to_vec();
                        if let Some(prev) = &last {
                            assert!(prev <= &k, "iteration must stay ordered");
                        }
                        last = Some(k);
                        count += 1;
                        it.next();
                    }
                    assert!(count >= 100);
                }
            })
        };
        // Single writer adds more entries concurrently.
        for i in 100..total {
            s.add(
                &i.to_be_bytes(),
                make_trailer(i as u64, InternalKeyKind::Set),
                b"v",
            )
            .unwrap();
        }
        reader.join().unwrap();
    }
}
