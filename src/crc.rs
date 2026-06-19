// Copyright (c) 2011 The LevelDB-Go Authors. All rights reserved.
// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Ported from Pebble's internal/crc/crc.go (CRC32C + the RocksDB checksum mask).

//! CRC32C (Castagnoli) checksums and the RocksDB checksum mask.
//!
//! Pebble protects WAL records and sstable blocks with CRC32C — the CRC-32 variant
//! using the Castagnoli polynomial (`0x1EDC6F41`, reflected form `0x82F63B78`). Stored
//! checksums are additionally *masked* (a rotate-and-add) so that a CRC computed over a
//! buffer that itself contains a CRC does not trivially collide.

/// The reflected Castagnoli polynomial used by CRC32C.
const CASTAGNOLI_POLY: u32 = 0x82F6_3B78;

/// The additive constant of the RocksDB/LevelDB checksum mask.
const MASK_DELTA: u32 = 0xa282_ead8;

/// The 256-entry CRC32C lookup table, generated at compile time.
const TABLE: [u32; 256] = build_table();

const fn build_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut n = 0;
    while n < 256 {
        let mut crc = n as u32;
        let mut k = 0;
        while k < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ CASTAGNOLI_POLY
            } else {
                crc >> 1
            };
            k += 1;
        }
        table[n] = crc;
        n += 1;
    }
    table
}

/// A running CRC32C checksum.
///
/// Construct with [`Crc32c::new`], feed bytes with [`Crc32c::update`] (chainable), and
/// read the result with [`Crc32c::finish`]. For a one-shot checksum use [`crc32c`].
#[derive(Debug, Clone, Copy)]
pub struct Crc32c(u32);

impl Crc32c {
    /// Starts a new checksum.
    #[inline]
    pub fn new() -> Self {
        Crc32c(0xffff_ffff)
    }

    /// Folds `data` into the running checksum and returns `self` for chaining.
    #[inline]
    pub fn update(mut self, data: &[u8]) -> Self {
        let mut crc = self.0;
        for &b in data {
            crc = TABLE[((crc ^ u32::from(b)) & 0xff) as usize] ^ (crc >> 8);
        }
        self.0 = crc;
        self
    }

    /// Returns the final (unmasked) checksum value.
    #[inline]
    pub fn finish(self) -> u32 {
        self.0 ^ 0xffff_ffff
    }
}

impl Default for Crc32c {
    fn default() -> Self {
        Self::new()
    }
}

/// Computes the CRC32C checksum of `data` in one call.
#[inline]
pub fn crc32c(data: &[u8]) -> u32 {
    Crc32c::new().update(data).finish()
}

/// Applies the RocksDB checksum mask: `rotate_right(crc, 15) + MASK_DELTA`.
///
/// Stored block/record checksums are masked so a checksum computed over data that
/// itself embeds a checksum cannot collide with the embedded value.
#[inline]
pub fn mask(crc: u32) -> u32 {
    crc.rotate_left(17).wrapping_add(MASK_DELTA)
}

/// Inverts [`mask`].
#[inline]
pub fn unmask(masked: u32) -> u32 {
    masked.wrapping_sub(MASK_DELTA).rotate_left(15)
}

/// Computes the CRC32C of `data` and returns the masked value, as stored on disk.
#[inline]
pub fn masked_crc32c(data: &[u8]) -> u32 {
    mask(crc32c(data))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_check_value() {
        // The standard CRC32C check value for the ASCII string "123456789".
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    }

    #[test]
    fn empty_is_zero() {
        assert_eq!(crc32c(b""), 0);
    }

    #[test]
    fn incremental_matches_oneshot() {
        let data = b"the quick brown fox jumps over the lazy dog";
        let split = Crc32c::new()
            .update(&data[..10])
            .update(&data[10..])
            .finish();
        assert_eq!(split, crc32c(data));
    }

    #[test]
    fn mask_roundtrips() {
        for &c in &[0u32, 1, 0xffff_ffff, 0xdead_beef, crc32c(b"123456789")] {
            assert_eq!(unmask(mask(c)), c);
        }
    }

    #[test]
    fn masked_helper() {
        assert_eq!(masked_crc32c(b"hello"), mask(crc32c(b"hello")));
    }
}
