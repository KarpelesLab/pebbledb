// Copyright (c) 2012 The LevelDB-Go Authors. All rights reserved.
// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Ported from Pebble's batch.go and batchrepr/reader.go.

//! Write batches: the atomic unit of mutation, and the payload of every WAL record.
//!
//! A batch's wire representation is a 12-byte header followed by a stream of operations:
//!
//! ```text
//! header: seqnum:u64-le | count:u32-le
//! op:     kind:u8 | key:varstr | value:varstr?
//! ```
//!
//! A `varstr` is a LEB128 length prefix followed by that many bytes. Whether an
//! operation carries a value depends on its [`InternalKeyKind`]: [`Set`], [`Merge`],
//! [`RangeDelete`] (value = end key), the range-key kinds, and a few internal kinds
//! carry a value; [`Delete`], [`SingleDelete`], and [`LogData`] do not.
//!
//! The `seqnum` is the base sequence number assigned to the batch when it is committed;
//! the i-th operation in the batch is assigned `seqnum + i`. It is zero until the batch
//! is applied.
//!
//! [`Set`]: InternalKeyKind::Set
//! [`Merge`]: InternalKeyKind::Merge
//! [`RangeDelete`]: InternalKeyKind::RangeDelete
//! [`Delete`]: InternalKeyKind::Delete
//! [`SingleDelete`]: InternalKeyKind::SingleDelete
//! [`LogData`]: InternalKeyKind::LogData

use crate::base::internal_key::InternalKeyKind;
use crate::base::varint::{get_uvarint, put_uvarint};
use crate::{Error, Result};

/// The length of the batch header in bytes.
pub const HEADER_LEN: usize = 12;

/// Offset of the 4-byte little-endian operation count within the header.
const COUNT_OFFSET: usize = 8;

/// Returns whether an operation of the given kind carries a value (a second `varstr`)
/// in addition to its key. Mirrors the decode switch in Pebble's `batchrepr.Reader`.
fn kind_has_value(kind: InternalKeyKind) -> bool {
    matches!(
        kind,
        InternalKeyKind::Set
            | InternalKeyKind::Merge
            | InternalKeyKind::RangeDelete
            | InternalKeyKind::RangeKeySet
            | InternalKeyKind::RangeKeyUnset
            | InternalKeyKind::RangeKeyDelete
            | InternalKeyKind::DeleteSized
            | InternalKeyKind::Excise
            | InternalKeyKind::IngestSSTWithBlobs
    )
}

/// Appends a `varstr` (LEB128 length prefix + bytes) to `dst`.
fn put_varstr(dst: &mut Vec<u8>, s: &[u8]) {
    put_uvarint(dst, s.len() as u64);
    dst.extend_from_slice(s);
}

/// Decodes a `varstr` from the front of `data`, returning `(rest, bytes)`.
fn get_varstr(data: &[u8]) -> Option<(&[u8], &[u8])> {
    let (len, n) = get_uvarint(data)?;
    let len = len as usize;
    let rest = &data[n..];
    if rest.len() < len {
        return None;
    }
    Some((&rest[len..], &rest[..len]))
}

/// A write batch: an ordered, atomically-applied group of operations.
///
/// Build one with [`Batch::new`] and the mutation methods, then commit it through the
/// database (later phases) or inspect/serialize it directly. The wire representation
/// returned by [`Batch::as_bytes`] is exactly what is written to the WAL.
#[derive(Clone, PartialEq, Eq)]
pub struct Batch {
    /// The full wire representation: 12-byte header followed by the op stream. The count
    /// field (bytes 8..12) is kept in sync with the operations on every append.
    repr: Vec<u8>,
}

impl Batch {
    /// Creates an empty batch with a zero sequence number and zero count.
    pub fn new() -> Self {
        Batch {
            repr: vec![0u8; HEADER_LEN],
        }
    }

    /// Wraps an existing wire representation, validating the header and that every
    /// operation decodes.
    pub fn from_bytes(repr: Vec<u8>) -> Result<Self> {
        if repr.len() < HEADER_LEN {
            return Err(Error::corruption(
                "batch: representation shorter than header",
            ));
        }
        let b = Batch { repr };
        // Validate that the body decodes cleanly and matches the header count.
        let mut n = 0u32;
        for op in b.iter() {
            op?;
            n = n.wrapping_add(1);
        }
        if n != b.count() {
            return Err(Error::corruption(format!(
                "batch: header count {} != decoded ops {n}",
                b.count()
            )));
        }
        Ok(b)
    }

    /// The base sequence number stored in the header.
    pub fn seqnum(&self) -> u64 {
        u64::from_le_bytes(self.repr[..COUNT_OFFSET].try_into().unwrap())
    }

    /// Sets the base sequence number in the header.
    pub fn set_seqnum(&mut self, seqnum: u64) {
        self.repr[..COUNT_OFFSET].copy_from_slice(&seqnum.to_le_bytes());
    }

    /// The number of operations in the batch.
    pub fn count(&self) -> u32 {
        u32::from_le_bytes(self.repr[COUNT_OFFSET..HEADER_LEN].try_into().unwrap())
    }

    /// Whether the batch contains no operations.
    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    /// The full wire representation (header + op stream).
    pub fn as_bytes(&self) -> &[u8] {
        &self.repr
    }

    /// Consumes the batch and returns its wire representation.
    pub fn into_bytes(self) -> Vec<u8> {
        self.repr
    }

    fn set_count(&mut self, count: u32) {
        self.repr[COUNT_OFFSET..HEADER_LEN].copy_from_slice(&count.to_le_bytes());
    }

    /// Appends one operation and bumps the count.
    fn add(&mut self, kind: InternalKeyKind, key: &[u8], value: Option<&[u8]>) {
        self.repr.push(kind.as_u8());
        put_varstr(&mut self.repr, key);
        if let Some(v) = value {
            put_varstr(&mut self.repr, v);
        }
        let next = self.count().wrapping_add(1);
        self.set_count(next);
    }

    /// Sets `key` to `value`.
    pub fn set(&mut self, key: &[u8], value: &[u8]) {
        self.add(InternalKeyKind::Set, key, Some(value));
    }

    /// Records a merge operand for `key`.
    pub fn merge(&mut self, key: &[u8], value: &[u8]) {
        self.add(InternalKeyKind::Merge, key, Some(value));
    }

    /// Deletes `key`.
    pub fn delete(&mut self, key: &[u8]) {
        self.add(InternalKeyKind::Delete, key, None);
    }

    /// Deletes `key`, which must have at most one prior [`Set`](InternalKeyKind::Set)
    /// not yet compacted away (single-delete semantics).
    pub fn single_delete(&mut self, key: &[u8]) {
        self.add(InternalKeyKind::SingleDelete, key, None);
    }

    /// Deletes every key in the half-open user-key range `[start, end)`.
    pub fn delete_range(&mut self, start: &[u8], end: &[u8]) {
        self.add(InternalKeyKind::RangeDelete, start, Some(end));
    }

    /// Appends opaque data that is logged to the WAL but not applied to the database.
    pub fn log_data(&mut self, data: &[u8]) {
        self.add(InternalKeyKind::LogData, data, None);
    }

    /// Iterates over the operations in the batch.
    pub fn iter(&self) -> BatchReader<'_> {
        BatchReader {
            data: &self.repr[HEADER_LEN..],
        }
    }
}

impl Default for Batch {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for Batch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Batch")
            .field("seqnum", &self.seqnum())
            .field("count", &self.count())
            .field("repr_len", &self.repr.len())
            .finish()
    }
}

/// One decoded operation from a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchOp<'a> {
    /// The operation's kind.
    pub kind: InternalKeyKind,
    /// The operation's key (for [`InternalKeyKind::LogData`], the logged bytes).
    pub key: &'a [u8],
    /// The operation's value, if its kind carries one.
    pub value: Option<&'a [u8]>,
}

/// An iterator over the operations in a [`Batch`]'s representation.
pub struct BatchReader<'a> {
    data: &'a [u8],
}

impl<'a> Iterator for BatchReader<'a> {
    type Item = Result<BatchOp<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.data.is_empty() {
            return None;
        }
        let kind = InternalKeyKind::from_u8(self.data[0]);
        if kind == InternalKeyKind::Invalid || kind.as_u8() > InternalKeyKind::MAX.as_u8() {
            self.data = &[];
            return Some(Err(Error::corruption(format!(
                "batch: invalid key kind {:#x}",
                self.data[0]
            ))));
        }
        let (rest, key) = match get_varstr(&self.data[1..]) {
            Some(v) => v,
            None => {
                self.data = &[];
                return Some(Err(Error::corruption("batch: truncated key")));
            }
        };
        let (rest, value) = if kind_has_value(kind) {
            match get_varstr(rest) {
                Some((r, v)) => (r, Some(v)),
                None => {
                    self.data = &[];
                    return Some(Err(Error::corruption("batch: truncated value")));
                }
            }
        } else {
            (rest, None)
        };
        self.data = rest;
        Some(Ok(BatchOp { kind, key, value }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_batch() {
        let b = Batch::new();
        assert_eq!(b.as_bytes().len(), HEADER_LEN);
        assert_eq!(b.count(), 0);
        assert_eq!(b.seqnum(), 0);
        assert!(b.is_empty());
        assert_eq!(b.iter().count(), 0);
    }

    #[test]
    fn set_layout_is_exact() {
        let mut b = Batch::new();
        b.set(b"a", b"b");
        // header: seqnum=0 (8 bytes), count=1 (4 bytes LE)
        let repr = b.as_bytes();
        assert_eq!(&repr[..8], &[0u8; 8]);
        assert_eq!(&repr[8..12], &1u32.to_le_bytes());
        // op: kind=Set(1), key varstr (len 1, 'a'), value varstr (len 1, 'b')
        assert_eq!(&repr[12..], &[1, 1, b'a', 1, b'b']);
    }

    #[test]
    fn seqnum_roundtrip() {
        let mut b = Batch::new();
        b.set_seqnum(0x0102_0304_0506_0708);
        assert_eq!(b.seqnum(), 0x0102_0304_0506_0708);
        assert_eq!(&b.as_bytes()[..8], &0x0102_0304_0506_0708u64.to_le_bytes());
    }

    #[test]
    fn mixed_ops_roundtrip() {
        let mut b = Batch::new();
        b.set(b"key1", b"value1");
        b.delete(b"key2");
        b.merge(b"counter", b"+1");
        b.single_delete(b"key3");
        b.delete_range(b"a", b"z");
        b.log_data(b"some log data");
        assert_eq!(b.count(), 6);

        let ops: Vec<BatchOp> = b.iter().collect::<Result<_>>().unwrap();
        assert_eq!(
            ops,
            vec![
                BatchOp {
                    kind: InternalKeyKind::Set,
                    key: b"key1",
                    value: Some(b"value1")
                },
                BatchOp {
                    kind: InternalKeyKind::Delete,
                    key: b"key2",
                    value: None
                },
                BatchOp {
                    kind: InternalKeyKind::Merge,
                    key: b"counter",
                    value: Some(b"+1")
                },
                BatchOp {
                    kind: InternalKeyKind::SingleDelete,
                    key: b"key3",
                    value: None
                },
                BatchOp {
                    kind: InternalKeyKind::RangeDelete,
                    key: b"a",
                    value: Some(b"z")
                },
                BatchOp {
                    kind: InternalKeyKind::LogData,
                    key: b"some log data",
                    value: None
                },
            ]
        );
    }

    #[test]
    fn from_bytes_roundtrip() {
        let mut b = Batch::new();
        b.set(b"x", b"1");
        b.delete(b"y");
        b.set_seqnum(99);
        let bytes = b.clone().into_bytes();

        let b2 = Batch::from_bytes(bytes).unwrap();
        assert_eq!(b2.seqnum(), 99);
        assert_eq!(b2.count(), 2);
        assert_eq!(b2, b);
    }

    #[test]
    fn from_bytes_rejects_short_and_corrupt() {
        assert!(Batch::from_bytes(vec![0u8; 4]).is_err());
        // count says 1 but no ops present.
        let mut bad = vec![0u8; HEADER_LEN];
        bad[COUNT_OFFSET] = 1;
        assert!(Batch::from_bytes(bad).is_err());
        // truncated value.
        let mut b = Batch::new();
        b.set(b"k", b"v");
        let mut bytes = b.into_bytes();
        bytes.pop(); // drop the value byte
        assert!(Batch::from_bytes(bytes).is_err());
    }

    #[test]
    fn large_key_value_varint_length() {
        let key = vec![b'k'; 200];
        let value = vec![b'v'; 1000];
        let mut b = Batch::new();
        b.set(&key, &value);
        let ops: Vec<BatchOp> = b.iter().collect::<Result<_>>().unwrap();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].key, &key[..]);
        assert_eq!(ops[0].value, Some(&value[..]));
    }
}
