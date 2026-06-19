// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

//! Little-endian fixed-width and LEB128 variable-width integer codecs.
//!
//! These match Go's `encoding/binary` (`PutUvarint`/`Uvarint`, `LittleEndian`) byte for
//! byte, which is what Pebble's batch, block, and footer encoders use.

/// Appends the LEB128 unsigned-varint encoding of `x` to `dst`.
///
/// Bytes carry 7 bits of payload each, least-significant group first; the high bit of a
/// byte is set on every byte except the last.
pub fn put_uvarint(dst: &mut Vec<u8>, mut x: u64) {
    while x >= 0x80 {
        dst.push((x as u8) | 0x80);
        x >>= 7;
    }
    dst.push(x as u8);
}

/// The number of bytes [`put_uvarint`] would emit for `x`.
pub fn uvarint_len(mut x: u64) -> usize {
    let mut n = 1;
    while x >= 0x80 {
        n += 1;
        x >>= 7;
    }
    n
}

/// Decodes a LEB128 unsigned varint from the front of `src`.
///
/// Returns `Some((value, bytes_consumed))`, or `None` if `src` is empty, the varint is
/// truncated, or it overflows `u64` (more than 10 bytes, or a 10th byte with extra
/// bits) — matching the error behavior of Go's `binary.Uvarint`.
pub fn get_uvarint(src: &[u8]) -> Option<(u64, usize)> {
    let mut x: u64 = 0;
    let mut shift = 0u32;
    for (i, &b) in src.iter().enumerate() {
        if i == 10 {
            // More than 10 bytes cannot fit in a u64.
            return None;
        }
        if b < 0x80 {
            // Final byte. The 10th byte (i == 9) may only contribute the single
            // remaining bit; anything larger overflows u64.
            if i == 9 && b > 1 {
                return None;
            }
            return Some((x | (u64::from(b) << shift), i + 1));
        }
        x |= u64::from(b & 0x7f) << shift;
        shift += 7;
    }
    None
}

/// Appends the 4-byte little-endian encoding of `x` to `dst`.
#[inline]
pub fn put_u32_le(dst: &mut Vec<u8>, x: u32) {
    dst.extend_from_slice(&x.to_le_bytes());
}

/// Appends the 8-byte little-endian encoding of `x` to `dst`.
#[inline]
pub fn put_u64_le(dst: &mut Vec<u8>, x: u64) {
    dst.extend_from_slice(&x.to_le_bytes());
}

/// Reads a 4-byte little-endian `u32` from the front of `src`, or `None` if too short.
#[inline]
pub fn get_u32_le(src: &[u8]) -> Option<u32> {
    src.get(..4)
        .map(|b| u32::from_le_bytes(b.try_into().unwrap()))
}

/// Reads an 8-byte little-endian `u64` from the front of `src`, or `None` if too short.
#[inline]
pub fn get_u64_le(src: &[u8]) -> Option<u64> {
    src.get(..8)
        .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uvarint_roundtrip_and_known_encodings() {
        // Known LEB128 encodings (same as Go's binary.PutUvarint).
        let cases: &[(u64, &[u8])] = &[
            (0, &[0x00]),
            (1, &[0x01]),
            (127, &[0x7f]),
            (128, &[0x80, 0x01]),
            (300, &[0xac, 0x02]),
            (16384, &[0x80, 0x80, 0x01]),
            (
                u64::MAX,
                &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01],
            ),
        ];
        for &(v, bytes) in cases {
            let mut buf = Vec::new();
            put_uvarint(&mut buf, v);
            assert_eq!(buf, bytes, "encoding of {v}");
            assert_eq!(uvarint_len(v), bytes.len(), "len of {v}");
            assert_eq!(get_uvarint(&buf), Some((v, bytes.len())), "decoding of {v}");
        }
    }

    #[test]
    fn uvarint_consumes_only_its_bytes() {
        let mut buf = Vec::new();
        put_uvarint(&mut buf, 300);
        buf.extend_from_slice(b"trailing");
        assert_eq!(get_uvarint(&buf), Some((300, 2)));
    }

    #[test]
    fn uvarint_rejects_truncated_and_overflow() {
        assert_eq!(get_uvarint(&[]), None);
        assert_eq!(get_uvarint(&[0x80]), None); // continuation with no terminator
        // 11 continuation bytes overflow u64.
        assert_eq!(get_uvarint(&[0x80; 11]), None);
        // 10th byte may only be 0 or 1.
        assert_eq!(
            get_uvarint(&[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02]),
            None
        );
    }

    #[test]
    fn fixed_width_roundtrip() {
        let mut buf = Vec::new();
        put_u32_le(&mut buf, 0xdead_beef);
        put_u64_le(&mut buf, 0x0123_4567_89ab_cdef);
        assert_eq!(get_u32_le(&buf), Some(0xdead_beef));
        assert_eq!(get_u64_le(&buf[4..]), Some(0x0123_4567_89ab_cdef));
        assert_eq!(get_u32_le(&[0, 1]), None);
        assert_eq!(get_u64_le(&[0; 7]), None);
    }
}
