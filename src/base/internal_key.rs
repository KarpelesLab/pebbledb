// Copyright (c) 2011 The LevelDB-Go Authors. All rights reserved.
// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Ported from Pebble's internal/base/internal.go and internal/base/seqnum.go.

//! Internal keys: user keys tagged with a sequence number and a key kind.
//!
//! Every key stored in the engine is an *internal key* — the user key followed by an
//! 8-byte little-endian *trailer* that packs a 56-bit sequence number and an 8-bit
//! [`InternalKeyKind`]:
//!
//! ```text
//! trailer = (seqnum << 8) | kind
//! encoded = user_key ++ trailer.to_le_bytes()
//! ```
//!
//! Internal keys sort by user key ascending and then by trailer *descending*, so that
//! for a given user key the most recent (largest sequence number) update sorts first.

use std::cmp::Ordering;

use crate::base::comparer::Comparer;

/// A sequence number: a 56-bit monotonically increasing counter identifying the order
/// of writes. Stored in the high 56 bits of an encoded key's trailer.
pub type SeqNum = u64;

/// The zero sequence number. Compactions may zero out sequence numbers once a key is
/// known to be the oldest for its user key.
pub const SEQNUM_ZERO: SeqNum = 0;

/// The first sequence number assigned to a user-visible key. Sequence numbers below
/// [`SEQNUM_START`] are reserved.
pub const SEQNUM_START: SeqNum = 10;

/// The largest valid sequence number. Also used as an exclusive sentinel for range
/// boundaries.
pub const SEQNUM_MAX: SeqNum = (1 << 56) - 1;

/// Bit set on the sequence numbers of keys that belong to an as-yet-uncommitted batch,
/// preventing them from being visible to reads.
pub const SEQNUM_BATCH_BIT: SeqNum = 1 << 55;

/// The kind of an internal key, stored in the low byte of the trailer.
///
/// The numeric values are an on-disk format and must match Pebble exactly. Values that
/// Pebble reserves but does not use (column-family and transaction kinds) are omitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum InternalKeyKind {
    /// Point deletion (tombstone).
    Delete = 0,
    /// Point set: key → value.
    Set = 1,
    /// Merge operand.
    Merge = 2,
    /// Log-only data, not added to a memtable (used in the WAL/batch stream).
    LogData = 3,
    /// Single deletion: deletes a single prior [`Set`](Self::Set) of the same key.
    SingleDelete = 7,
    /// Range deletion: deletes the half-open user-key span `[start, end)`.
    RangeDelete = 15,
    /// Separator key used in sstable index entries.
    Separator = 17,
    /// A set that also acts as a deletion of any prior value (`SET` semantics that
    /// suppress merges below it).
    SetWithDelete = 18,
    /// Deletes a range of range keys.
    RangeKeyDelete = 19,
    /// Removes (unsets) a range key over a span.
    RangeKeyUnset = 20,
    /// Sets a range key over a span.
    RangeKeySet = 21,
    /// Marks an ingested sstable boundary.
    IngestSST = 22,
    /// A point deletion that carries the size of the value it deletes, enabling more
    /// accurate compaction accounting.
    DeleteSized = 23,
    /// Excises a span as part of an ingestion.
    Excise = 24,
    /// A synthetic key produced internally (never persisted as user data).
    SyntheticKey = 25,
    /// An ingested sstable boundary that references blob files.
    IngestSSTWithBlobs = 26,
    /// A span boundary marker.
    SpanBoundary = 30,
    /// An invalid / unrecognized kind. Equal to Pebble's
    /// `InternalKeyKindInvalid` (the sstable-internal obsolete mask, 191).
    Invalid = 191,
}

impl InternalKeyKind {
    /// The largest valid key kind (`SpanBoundary`). A key with a kind greater than this
    /// is invalid for ordering purposes.
    pub const MAX: InternalKeyKind = InternalKeyKind::SpanBoundary;

    /// The largest key kind that may appear inside an sstable (`DeleteSized`).
    pub const MAX_FOR_SSTABLE: InternalKeyKind = InternalKeyKind::DeleteSized;

    /// Bit OR-ed into a key kind inside an sstable to mark the key as obsolete.
    pub const SSTABLE_OBSOLETE_BIT: u8 = 64;

    /// Mask covering the obsolete bit plus all valid kind bits within an sstable.
    pub const SSTABLE_OBSOLETE_MASK: u8 = 191;

    /// Decodes a kind from its on-disk byte, returning [`InternalKeyKind::Invalid`] for
    /// unrecognized values.
    pub fn from_u8(b: u8) -> InternalKeyKind {
        use InternalKeyKind::*;
        match b {
            0 => Delete,
            1 => Set,
            2 => Merge,
            3 => LogData,
            7 => SingleDelete,
            15 => RangeDelete,
            17 => Separator,
            18 => SetWithDelete,
            19 => RangeKeyDelete,
            20 => RangeKeyUnset,
            21 => RangeKeySet,
            22 => IngestSST,
            23 => DeleteSized,
            24 => Excise,
            25 => SyntheticKey,
            26 => IngestSSTWithBlobs,
            30 => SpanBoundary,
            _ => Invalid,
        }
    }

    /// The on-disk byte for this kind.
    #[inline]
    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Builds a trailer from a sequence number and a kind: `(seqnum << 8) | kind`.
#[inline]
pub fn make_trailer(seqnum: SeqNum, kind: InternalKeyKind) -> u64 {
    (seqnum << 8) | u64::from(kind.as_u8())
}

/// Extracts the sequence number from a trailer.
#[inline]
pub fn trailer_seqnum(trailer: u64) -> SeqNum {
    trailer >> 8
}

/// Extracts the key kind from a trailer.
#[inline]
pub fn trailer_kind(trailer: u64) -> InternalKeyKind {
    InternalKeyKind::from_u8(trailer as u8)
}

/// An owned internal key: a user key plus a trailer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalKey {
    /// The user-visible key bytes.
    pub user_key: Vec<u8>,
    /// The packed `(seqnum << 8) | kind` trailer.
    pub trailer: u64,
}

impl InternalKey {
    /// Creates an internal key from its components.
    pub fn new(user_key: impl Into<Vec<u8>>, seqnum: SeqNum, kind: InternalKeyKind) -> InternalKey {
        InternalKey {
            user_key: user_key.into(),
            trailer: make_trailer(seqnum, kind),
        }
    }

    /// The sequence number.
    #[inline]
    pub fn seqnum(&self) -> SeqNum {
        trailer_seqnum(self.trailer)
    }

    /// The key kind.
    #[inline]
    pub fn kind(&self) -> InternalKeyKind {
        trailer_kind(self.trailer)
    }

    /// The length of this key's encoded form (`user_key.len() + 8`).
    #[inline]
    pub fn encoded_len(&self) -> usize {
        self.user_key.len() + 8
    }

    /// Appends the encoded form (`user_key ++ trailer_le`) to `dst`.
    pub fn encode_to(&self, dst: &mut Vec<u8>) {
        dst.extend_from_slice(&self.user_key);
        dst.extend_from_slice(&self.trailer.to_le_bytes());
    }

    /// Returns the encoded form as a freshly allocated vector.
    pub fn encode(&self) -> Vec<u8> {
        let mut dst = Vec::with_capacity(self.encoded_len());
        self.encode_to(&mut dst);
        dst
    }

    /// Decodes an internal key from its encoded form.
    ///
    /// If `encoded` is shorter than the 8-byte trailer it is treated as a key with an
    /// empty user key and an [`InternalKeyKind::Invalid`] trailer, matching Pebble's
    /// lenient `DecodeInternalKey`.
    pub fn decode(encoded: &[u8]) -> InternalKey {
        if encoded.len() < 8 {
            return InternalKey {
                user_key: encoded.to_vec(),
                trailer: u64::from(InternalKeyKind::Invalid.as_u8()),
            };
        }
        let n = encoded.len() - 8;
        InternalKey {
            user_key: encoded[..n].to_vec(),
            trailer: u64::from_le_bytes(encoded[n..].try_into().unwrap()),
        }
    }

    /// Returns whether the kind is a recognized, in-range kind.
    pub fn is_valid(&self) -> bool {
        self.kind() != InternalKeyKind::Invalid
            && self.kind().as_u8() <= InternalKeyKind::MAX.as_u8()
    }
}

/// The user-key portion of an encoded internal key (everything but the 8-byte trailer).
/// If `encoded` is shorter than 8 bytes the whole slice is returned.
#[inline]
pub fn encoded_user_key(encoded: &[u8]) -> &[u8] {
    if encoded.len() < 8 {
        encoded
    } else {
        &encoded[..encoded.len() - 8]
    }
}

/// The trailer of an encoded internal key, or the `Invalid` trailer if too short.
#[inline]
pub fn encoded_trailer(encoded: &[u8]) -> u64 {
    if encoded.len() < 8 {
        u64::from(InternalKeyKind::Invalid.as_u8())
    } else {
        let n = encoded.len() - 8;
        u64::from_le_bytes(encoded[n..].try_into().unwrap())
    }
}

/// Orders two *encoded* internal keys using `cmp` for the user-key portion.
///
/// User keys order ascending; for equal user keys, the larger trailer sorts first
/// (so newer sequence numbers, and ties broken by larger kind, come earlier).
pub fn compare_encoded(cmp: &dyn Comparer, a: &[u8], b: &[u8]) -> Ordering {
    match cmp.compare(encoded_user_key(a), encoded_user_key(b)) {
        Ordering::Equal => {
            // Larger trailer sorts first → reverse the natural u64 ordering.
            encoded_trailer(b).cmp(&encoded_trailer(a))
        }
        other => other,
    }
}

impl InternalKey {
    /// Orders this key against `other` using `cmp`, with the internal-key tie-break
    /// (larger trailer first).
    pub fn compare(&self, cmp: &dyn Comparer, other: &InternalKey) -> Ordering {
        match cmp.compare(&self.user_key, &other.user_key) {
            Ordering::Equal => other.trailer.cmp(&self.trailer),
            ord => ord,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::comparer::DefaultComparer;

    #[test]
    fn trailer_pack_unpack() {
        let t = make_trailer(0x0123_4567_89ab, InternalKeyKind::Set);
        assert_eq!(trailer_seqnum(t), 0x0123_4567_89ab);
        assert_eq!(trailer_kind(t), InternalKeyKind::Set);
        assert_eq!(t, (0x0123_4567_89ab << 8) | 1);
    }

    #[test]
    fn kind_roundtrip() {
        for k in [
            InternalKeyKind::Delete,
            InternalKeyKind::Set,
            InternalKeyKind::Merge,
            InternalKeyKind::RangeDelete,
            InternalKeyKind::RangeKeySet,
            InternalKeyKind::DeleteSized,
            InternalKeyKind::SpanBoundary,
        ] {
            assert_eq!(InternalKeyKind::from_u8(k.as_u8()), k);
        }
        assert_eq!(InternalKeyKind::from_u8(99), InternalKeyKind::Invalid);
        assert_eq!(InternalKeyKind::from_u8(255), InternalKeyKind::Invalid);
    }

    #[test]
    fn encode_decode_roundtrip() {
        let k = InternalKey::new(b"hello".to_vec(), 42, InternalKeyKind::Set);
        let enc = k.encode();
        assert_eq!(enc.len(), k.encoded_len());
        // user key followed by little-endian trailer.
        assert_eq!(&enc[..5], b"hello");
        assert_eq!(&enc[5..], &((42u64 << 8) | 1).to_le_bytes());

        let dec = InternalKey::decode(&enc);
        assert_eq!(dec, k);
        assert_eq!(dec.seqnum(), 42);
        assert_eq!(dec.kind(), InternalKeyKind::Set);
    }

    #[test]
    fn decode_short_key_is_invalid() {
        let dec = InternalKey::decode(b"abc");
        assert_eq!(dec.kind(), InternalKeyKind::Invalid);
        assert_eq!(dec.user_key, b"abc");
    }

    #[test]
    fn encoded_accessors_match_decode() {
        let k = InternalKey::new(b"world".to_vec(), 7, InternalKeyKind::Delete);
        let enc = k.encode();
        assert_eq!(encoded_user_key(&enc), b"world");
        assert_eq!(encoded_trailer(&enc), k.trailer);
        assert_eq!(trailer_seqnum(encoded_trailer(&enc)), 7);
    }

    #[test]
    fn ordering_user_key_then_trailer_desc() {
        let cmp = DefaultComparer;
        // Different user keys → user-key order wins.
        let a = InternalKey::new(b"a".to_vec(), 1, InternalKeyKind::Set);
        let b = InternalKey::new(b"b".to_vec(), 100, InternalKeyKind::Set);
        assert_eq!(a.compare(&cmp, &b), Ordering::Less);

        // Same user key → larger seqnum sorts first.
        let newer = InternalKey::new(b"k".to_vec(), 5, InternalKeyKind::Set);
        let older = InternalKey::new(b"k".to_vec(), 2, InternalKeyKind::Set);
        assert_eq!(newer.compare(&cmp, &older), Ordering::Less);

        // Same user key and seqnum → larger kind sorts first.
        let set = InternalKey::new(b"k".to_vec(), 5, InternalKeyKind::Set); // kind 1
        let del = InternalKey::new(b"k".to_vec(), 5, InternalKeyKind::Delete); // kind 0
        assert_eq!(set.compare(&cmp, &del), Ordering::Less);

        // The encoded comparison agrees with the structured one.
        assert_eq!(
            compare_encoded(&cmp, &newer.encode(), &older.encode()),
            Ordering::Less
        );
        assert_eq!(
            compare_encoded(&cmp, &set.encode(), &del.encode()),
            Ordering::Less
        );
    }
}
