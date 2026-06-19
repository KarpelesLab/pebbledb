// Copyright (c) 2020 The LevelDB-Go Authors. All rights reserved.
// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Ported from Pebble's internal/rangekey/rangekey.go.

//! Range keys: values (optionally keyed by a suffix) associated with a span of user-key
//! space `[start, end)`, independent of point keys.
//!
//! Three operations exist, each an [`InternalKeyKind`]:
//! - `RANGEKEYSET` sets one or more `(suffix, value)` pairs over `[start, end)`.
//! - `RANGEKEYUNSET` removes one or more suffixes over `[start, end)`.
//! - `RANGEKEYDEL` deletes all range keys over `[start, end)`, regardless of suffix.
//!
//! Each is stored as an internal key `(start, seqnum, kind)` whose value encodes the end
//! key followed by the operation-specific payload:
//!
//! ```text
//! RANGEKEYSET   value: varstr(end) | (varstr(suffix) varstr(value))*
//! RANGEKEYUNSET value: varstr(end) | varstr(suffix)*
//! RANGEKEYDEL   value: end           (raw, no length prefix)
//! ```

use crate::base::comparer::Comparer;
use crate::base::internal_key::InternalKeyKind;
use crate::base::varint::{get_uvarint, put_uvarint};
use crate::{Error, Result};

/// A `(suffix, value)` pair within a `RANGEKEYSET`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SuffixValue {
    /// The suffix the value is keyed under (empty for an unsuffixed range key).
    pub suffix: Vec<u8>,
    /// The value.
    pub value: Vec<u8>,
}

/// Appends a length-prefixed string.
fn put_varstr(dst: &mut Vec<u8>, s: &[u8]) {
    put_uvarint(dst, s.len() as u64);
    dst.extend_from_slice(s);
}

/// Decodes a length-prefixed string, returning `(rest, bytes)`.
fn get_varstr(data: &[u8]) -> Option<(&[u8], &[u8])> {
    let (len, n) = get_uvarint(data)?;
    let len = len as usize;
    let rest = &data[n..];
    if rest.len() < len {
        return None;
    }
    Some((&rest[len..], &rest[..len]))
}

/// Encodes a `RANGEKEYSET` value: `varstr(end)` followed by the suffix/value pairs.
pub fn encode_set_value(end: &[u8], suffix_values: &[SuffixValue]) -> Vec<u8> {
    let mut dst = Vec::new();
    put_varstr(&mut dst, end);
    for sv in suffix_values {
        put_varstr(&mut dst, &sv.suffix);
        put_varstr(&mut dst, &sv.value);
    }
    dst
}

/// Encodes a `RANGEKEYUNSET` value: `varstr(end)` followed by the suffixes.
pub fn encode_unset_value(end: &[u8], suffixes: &[Vec<u8>]) -> Vec<u8> {
    let mut dst = Vec::new();
    put_varstr(&mut dst, end);
    for s in suffixes {
        put_varstr(&mut dst, s);
    }
    dst
}

/// Encodes a `RANGEKEYDEL` value: the raw end key.
pub fn encode_del_value(end: &[u8]) -> Vec<u8> {
    end.to_vec()
}

/// Reads the end key (and the remaining payload) from a range-key value.
pub fn decode_end(kind: InternalKeyKind, data: &[u8]) -> Result<(&[u8], &[u8])> {
    match kind {
        InternalKeyKind::RangeKeyDelete => Ok((data, &[])),
        InternalKeyKind::RangeKeySet | InternalKeyKind::RangeKeyUnset => get_varstr(data)
            .map(|(rest, end)| (end, rest))
            .ok_or_else(|| Error::corruption("range key: bad end key")),
        _ => Err(Error::corruption("range key: not a range-key kind")),
    }
}

/// Decodes the suffix/value pairs of a `RANGEKEYSET` payload (after the end key).
pub fn decode_set_suffix_values(mut data: &[u8]) -> Result<Vec<SuffixValue>> {
    let mut out = Vec::new();
    while !data.is_empty() {
        let (rest, suffix) =
            get_varstr(data).ok_or_else(|| Error::corruption("range key: bad set suffix"))?;
        let (rest, value) =
            get_varstr(rest).ok_or_else(|| Error::corruption("range key: bad set value"))?;
        out.push(SuffixValue {
            suffix: suffix.to_vec(),
            value: value.to_vec(),
        });
        data = rest;
    }
    Ok(out)
}

/// Decodes the suffixes of a `RANGEKEYUNSET` payload (after the end key).
pub fn decode_unset_suffixes(mut data: &[u8]) -> Result<Vec<Vec<u8>>> {
    let mut out = Vec::new();
    while !data.is_empty() {
        let (rest, suffix) =
            get_varstr(data).ok_or_else(|| Error::corruption("range key: bad unset suffix"))?;
        out.push(suffix.to_vec());
        data = rest;
    }
    Ok(out)
}

/// A stored range-key entry: the internal-key components plus its raw encoded value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeKeyEntry {
    /// The operation kind (`RangeKeySet`, `RangeKeyUnset`, or `RangeKeyDelete`).
    pub kind: InternalKeyKind,
    /// The span's inclusive start user key.
    pub start: Vec<u8>,
    /// The entry's sequence number.
    pub seqnum: u64,
    /// The raw encoded value (end key + payload).
    pub value: Vec<u8>,
}

impl RangeKeyEntry {
    /// The span's exclusive end user key, decoded from the value.
    pub fn end(&self) -> Result<Vec<u8>> {
        Ok(decode_end(self.kind, &self.value)?.0.to_vec())
    }

    /// Whether the span covers `user_key` (`start <= user_key < end`).
    pub fn covers(&self, cmp: &dyn Comparer, user_key: &[u8]) -> Result<bool> {
        let end = self.end()?;
        Ok(
            cmp.compare(&self.start, user_key) != std::cmp::Ordering::Greater
                && cmp.compare(user_key, &end) == std::cmp::Ordering::Less,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::comparer::DefaultComparer;

    #[test]
    fn set_value_roundtrip() {
        let svs = vec![
            SuffixValue {
                suffix: b"@5".to_vec(),
                value: b"v5".to_vec(),
            },
            SuffixValue {
                suffix: b"@3".to_vec(),
                value: b"v3".to_vec(),
            },
        ];
        let encoded = encode_set_value(b"end", &svs);
        let (end, rest) = decode_end(InternalKeyKind::RangeKeySet, &encoded).unwrap();
        assert_eq!(end, b"end");
        assert_eq!(decode_set_suffix_values(rest).unwrap(), svs);
    }

    #[test]
    fn unset_value_roundtrip() {
        let suffixes = vec![b"@5".to_vec(), b"@3".to_vec()];
        let encoded = encode_unset_value(b"zzz", &suffixes);
        let (end, rest) = decode_end(InternalKeyKind::RangeKeyUnset, &encoded).unwrap();
        assert_eq!(end, b"zzz");
        assert_eq!(decode_unset_suffixes(rest).unwrap(), suffixes);
    }

    #[test]
    fn del_value_roundtrip() {
        let encoded = encode_del_value(b"theend");
        let (end, rest) = decode_end(InternalKeyKind::RangeKeyDelete, &encoded).unwrap();
        assert_eq!(end, b"theend");
        assert!(rest.is_empty());
    }

    #[test]
    fn entry_covers() {
        let cmp = DefaultComparer;
        let e = RangeKeyEntry {
            kind: InternalKeyKind::RangeKeySet,
            start: b"b".to_vec(),
            seqnum: 7,
            value: encode_set_value(
                b"d",
                &[SuffixValue {
                    suffix: vec![],
                    value: b"x".to_vec(),
                }],
            ),
        };
        assert_eq!(e.end().unwrap(), b"d");
        assert!(!e.covers(&cmp, b"a").unwrap());
        assert!(e.covers(&cmp, b"b").unwrap());
        assert!(e.covers(&cmp, b"c").unwrap());
        assert!(!e.covers(&cmp, b"d").unwrap());
    }
}
