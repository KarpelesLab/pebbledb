// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

//! Robustness fuzzing of the on-disk decoders: feed each parser a large stream of random and
//! mutated-valid byte sequences and assert it never panics, hangs, or reads out of bounds —
//! it must always return `Ok` or a clean `Err`. This is the single-crate, stable-toolchain
//! analogue of a libFuzzer corpus run; a `cargo-fuzz` target would drive the same entry
//! points (kept as decode helpers below) under coverage-guided mutation.

use std::io::Cursor;

use pebbledb::DefaultComparer;
use pebbledb::batch::Batch;
use pebbledb::manifest::VersionEdit;
use pebbledb::record;
use pebbledb::sstable::Reader;
use std::sync::Arc;

/// A tiny deterministic xorshift64* PRNG (no external crates), so failures reproduce by seed.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
    fn byte(&mut self) -> u8 {
        (self.next() & 0xff) as u8
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// Each decoder entry point: it must never panic on arbitrary input. (A `cargo-fuzz` target
/// would call exactly these.)
fn drive_decoders(data: &[u8]) {
    // Batch wire format.
    let _ = Batch::from_bytes(data.to_vec());

    // Record log (WAL / MANIFEST framing).
    let mut r = record::Reader::new(Cursor::new(data.to_vec()), 0);
    for _ in 0..64 {
        match r.read_record() {
            Ok(Some(_)) => {}
            _ => break,
        }
    }

    // MANIFEST version edit.
    let _ = VersionEdit::decode(data);

    // sstable reader (footer/index/blocks).
    let _ = Reader::open(data.to_vec(), Arc::new(DefaultComparer));
}

#[test]
fn decoders_never_panic_on_random_input() {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    for _ in 0..20_000 {
        let len = rng.below(512);
        let mut buf = vec![0u8; len];
        for b in &mut buf {
            *b = rng.byte();
        }
        drive_decoders(&buf);
    }
}

#[test]
fn decoders_never_panic_on_mutated_valid_input() {
    // Start from a valid batch encoding and a valid record-log stream, then bit-flip /
    // truncate / extend them — the bug-rich region near "almost valid" framing. The log stream
    // is produced through a MemFs file so it is real record framing.
    let mut batch = Batch::new();
    batch.set(b"alpha", b"one");
    batch.set(b"beta", b"two");
    batch.delete(b"alpha");
    batch.merge(b"gamma", b"x");
    let batch_bytes = batch.as_bytes().to_vec();

    let log_bytes = {
        use pebbledb::vfs::Fs;
        let fs = pebbledb::vfs::MemFs::new();
        let path = std::path::Path::new("/log");
        {
            let mut w = record::Writer::new(fs.create(path).unwrap());
            w.write_record(&batch_bytes).unwrap();
            w.write_record(b"another record payload").unwrap();
            w.flush().unwrap();
        }
        fs.read(path).unwrap()
    };

    let seeds = [batch_bytes, log_bytes];
    let mut rng = Rng(0xDEAD_BEEF_CAFE_F00D);
    for _ in 0..20_000 {
        let mut buf = seeds[rng.below(seeds.len())].clone();
        if buf.is_empty() {
            continue;
        }
        match rng.below(4) {
            0 => {
                // Flip a random bit.
                let i = rng.below(buf.len());
                buf[i] ^= 1 << (rng.below(8));
            }
            1 => {
                // Truncate.
                buf.truncate(rng.below(buf.len()));
            }
            2 => {
                // Append junk.
                for _ in 0..rng.below(32) {
                    buf.push(rng.byte());
                }
            }
            _ => {
                // Overwrite a span with random bytes.
                let i = rng.below(buf.len());
                let n = rng.below(buf.len() - i + 1);
                for b in &mut buf[i..i + n] {
                    *b = rng.byte();
                }
            }
        }
        drive_decoders(&buf);
    }
}
