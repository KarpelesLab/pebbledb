#![no_main]
//! Fuzz the batch wire-format decoder: arbitrary bytes must never panic, only
//! `Err` or a well-formed `Batch`.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(batch) = pebbledb::batch::Batch::from_bytes(data.to_vec()) {
        // Exercise the op iterator too — decoding the header is not enough.
        for op in batch.iter() {
            let _ = op;
        }
    }
});
