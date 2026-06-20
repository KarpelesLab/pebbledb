#![no_main]
//! Fuzz the sstable reader (footer / metaindex / index / data blocks): arbitrary bytes
//! must never panic. When the footer happens to parse, also walk the table iterator so the
//! block decoders see the mutated input, not just the footer.
use libfuzzer_sys::fuzz_target;
use std::sync::Arc;

use pebbledb::DefaultComparer;
use pebbledb::sstable::Reader;

fuzz_target!(|data: &[u8]| {
    let Ok(reader) = Reader::open(data.to_vec(), Arc::new(DefaultComparer)) else {
        return;
    };
    let reader = Arc::new(reader);
    if let Ok(mut it) = reader.iter() {
        let mut n = 0;
        while n < 1024 {
            match it.next() {
                Ok(true) => {
                    // Touch key + value so the block + value decoders run on mutated input.
                    let _ = it.key();
                    let _ = it.value();
                    n += 1;
                }
                _ => break,
            }
        }
    }
});
