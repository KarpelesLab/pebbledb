#![no_main]
//! Fuzz the record-log (WAL / MANIFEST framing) reader: arbitrary bytes must never
//! panic, hang, or read out of bounds — only `Ok`/`Err`/clean EOF.
use libfuzzer_sys::fuzz_target;
use std::io::Cursor;

fuzz_target!(|data: &[u8]| {
    let mut r = pebbledb::record::Reader::new(Cursor::new(data.to_vec()), 0);
    // Bound the loop: a corrupt stream must not be able to make this spin forever.
    for _ in 0..1024 {
        match r.read_record() {
            Ok(Some(_)) => {}
            _ => break,
        }
    }
});
