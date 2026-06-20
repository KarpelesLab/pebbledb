#![no_main]
//! Fuzz the MANIFEST `VersionEdit` tag-stream decoder: arbitrary bytes must never panic.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = pebbledb::manifest::VersionEdit::decode(data);
});
