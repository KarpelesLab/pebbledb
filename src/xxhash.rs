// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// A from-scratch implementation of the XXH64 algorithm by Yann Collet
// (https://github.com/Cyan4973/xxHash), used by Pebble as an alternative sstable
// block checksum.

//! XXH64, one of the two block-checksum algorithms Pebble supports (the other being
//! CRC32C). Pebble stores the low 32 bits of `XXH64(block_data ++ compression_type)` —
//! unmasked — as the block checksum when the table's checksum type is xxHash64.

const PRIME1: u64 = 0x9E37_79B1_85EB_CA87;
const PRIME2: u64 = 0xC2B2_AE3D_27D4_EB4F;
const PRIME3: u64 = 0x1656_67B1_9E37_79F9;
const PRIME4: u64 = 0x85EB_CA77_C2B2_AE63;
const PRIME5: u64 = 0x27D4_EB2F_1656_67C5;

#[inline]
fn round(acc: u64, input: u64) -> u64 {
    acc.wrapping_add(input.wrapping_mul(PRIME2))
        .rotate_left(31)
        .wrapping_mul(PRIME1)
}

#[inline]
fn merge_round(mut acc: u64, val: u64) -> u64 {
    let val = round(0, val);
    acc ^= val;
    acc.wrapping_mul(PRIME1).wrapping_add(PRIME4)
}

/// A streaming XXH64 hasher.
#[derive(Debug, Clone)]
pub struct XxHash64 {
    total_len: u64,
    v1: u64,
    v2: u64,
    v3: u64,
    v4: u64,
    mem: [u8; 32],
    mem_size: usize,
    seed: u64,
}

impl XxHash64 {
    /// Creates a hasher with the given seed (Pebble uses seed 0).
    pub fn with_seed(seed: u64) -> XxHash64 {
        XxHash64 {
            total_len: 0,
            v1: seed.wrapping_add(PRIME1).wrapping_add(PRIME2),
            v2: seed.wrapping_add(PRIME2),
            v3: seed,
            v4: seed.wrapping_sub(PRIME1),
            mem: [0u8; 32],
            mem_size: 0,
            seed,
        }
    }

    /// Creates a hasher with seed 0.
    pub fn new() -> XxHash64 {
        Self::with_seed(0)
    }

    /// Feeds `input` into the hasher.
    pub fn update(&mut self, mut input: &[u8]) {
        self.total_len = self.total_len.wrapping_add(input.len() as u64);

        // Fill the 32-byte buffer first if it holds a partial block.
        if self.mem_size > 0 {
            let need = 32 - self.mem_size;
            let take = need.min(input.len());
            self.mem[self.mem_size..self.mem_size + take].copy_from_slice(&input[..take]);
            self.mem_size += take;
            input = &input[take..];
            if self.mem_size < 32 {
                return;
            }
            let mem = self.mem;
            self.v1 = round(self.v1, read_u64(&mem[0..8]));
            self.v2 = round(self.v2, read_u64(&mem[8..16]));
            self.v3 = round(self.v3, read_u64(&mem[16..24]));
            self.v4 = round(self.v4, read_u64(&mem[24..32]));
            self.mem_size = 0;
        }

        // Process full 32-byte blocks directly from the input.
        while input.len() >= 32 {
            self.v1 = round(self.v1, read_u64(&input[0..8]));
            self.v2 = round(self.v2, read_u64(&input[8..16]));
            self.v3 = round(self.v3, read_u64(&input[16..24]));
            self.v4 = round(self.v4, read_u64(&input[24..32]));
            input = &input[32..];
        }

        // Buffer the remainder.
        if !input.is_empty() {
            self.mem[..input.len()].copy_from_slice(input);
            self.mem_size = input.len();
        }
    }

    /// Returns the 64-bit digest. The hasher may continue to be used afterward.
    pub fn finish(&self) -> u64 {
        let mut h: u64 = if self.total_len >= 32 {
            self.v1
                .rotate_left(1)
                .wrapping_add(self.v2.rotate_left(7))
                .wrapping_add(self.v3.rotate_left(12))
                .wrapping_add(self.v4.rotate_left(18))
        } else {
            self.seed.wrapping_add(PRIME5)
        };
        if self.total_len >= 32 {
            h = merge_round(h, self.v1);
            h = merge_round(h, self.v2);
            h = merge_round(h, self.v3);
            h = merge_round(h, self.v4);
        }
        h = h.wrapping_add(self.total_len);

        let mut data = &self.mem[..self.mem_size];
        while data.len() >= 8 {
            let k = round(0, read_u64(&data[0..8]));
            h ^= k;
            h = h.rotate_left(27).wrapping_mul(PRIME1).wrapping_add(PRIME4);
            data = &data[8..];
        }
        if data.len() >= 4 {
            h ^= u64::from(read_u32(&data[0..4])).wrapping_mul(PRIME1);
            h = h.rotate_left(23).wrapping_mul(PRIME2).wrapping_add(PRIME3);
            data = &data[4..];
        }
        for &b in data {
            h ^= u64::from(b).wrapping_mul(PRIME5);
            h = h.rotate_left(11).wrapping_mul(PRIME1);
        }

        // Final avalanche.
        h ^= h >> 33;
        h = h.wrapping_mul(PRIME2);
        h ^= h >> 29;
        h = h.wrapping_mul(PRIME3);
        h ^= h >> 32;
        h
    }
}

impl Default for XxHash64 {
    fn default() -> Self {
        Self::new()
    }
}

#[inline]
fn read_u64(b: &[u8]) -> u64 {
    u64::from_le_bytes(b[..8].try_into().unwrap())
}

#[inline]
fn read_u32(b: &[u8]) -> u32 {
    u32::from_le_bytes(b[..4].try_into().unwrap())
}

/// Computes `XXH64(data)` with seed 0 in one call.
pub fn xxh64(data: &[u8]) -> u64 {
    let mut h = XxHash64::new();
    h.update(data);
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vectors() {
        // Canonical XXH64 test vectors (seed 0).
        assert_eq!(xxh64(b""), 0xEF46_DB37_51D8_E999);
        assert_eq!(
            xxh64(b"Nobody inspects the spammish repetition"),
            0xFBCE_A83C_8A37_8BF1
        );
    }

    #[test]
    fn streaming_matches_oneshot() {
        let data: Vec<u8> = (0..200u32).map(|i| (i * 7 % 251) as u8).collect();
        let oneshot = xxh64(&data);
        // Feed in awkward chunk sizes that straddle 32-byte block boundaries.
        for chunk in [1usize, 3, 7, 32, 31, 33] {
            let mut h = XxHash64::new();
            for part in data.chunks(chunk) {
                h.update(part);
            }
            assert_eq!(h.finish(), oneshot, "chunk size {chunk}");
        }
    }

    #[test]
    fn update_in_two_parts() {
        let data = b"the quick brown fox jumps over the lazy dog, repeatedly!";
        let mut h = XxHash64::new();
        h.update(&data[..10]);
        h.update(&data[10..]);
        assert_eq!(h.finish(), xxh64(data));
    }
}
