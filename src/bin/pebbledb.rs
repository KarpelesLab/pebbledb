// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// A small inspection CLI, in the spirit of Pebble's `pebble` tool.

//! `pebbledb` — a command-line tool for inspecting pebbledb / Pebble on-disk files.
//!
//! ```text
//! pebbledb sstable dump  <file.sst>     dump an sstable's entries and properties
//! pebbledb wal dump      <file.log>     dump a write-ahead log's batches
//! pebbledb manifest dump <MANIFEST>     dump a MANIFEST's version edits
//! pebbledb db get        <dir> <key>    read one key from a database (read-only)
//! pebbledb db scan       <dir>          scan all visible keys (read-only)
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use pebbledb::base::internal_key::{
    encoded_trailer, encoded_user_key, trailer_kind, trailer_seqnum,
};
use pebbledb::batch::Batch;
use pebbledb::manifest::VersionEdit;
use pebbledb::record;
use pebbledb::sstable::Reader;
use pebbledb::{Db, Options};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    match run(&refs) {
        Ok(()) => ExitCode::SUCCESS,
        Err(msg) => {
            eprintln!("error: {msg}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: &[&str]) -> Result<(), String> {
    match args {
        ["sstable" | "sst", "dump", file] => dump_sstable(file),
        ["wal", "dump", file] => dump_wal(file),
        ["manifest", "dump", file] => dump_manifest(file),
        ["db", "get", dir, key] => db_get(dir, key),
        ["db", "scan", dir] => db_scan(dir),
        ["db", "lsm", dir] => db_lsm(dir),
        ["find", dir, key] => db_find(dir, key),
        ["help" | "-h" | "--help"] | [] => {
            print_usage();
            Ok(())
        }
        _ => {
            print_usage();
            Err("unrecognized command".into())
        }
    }
}

fn print_usage() {
    eprintln!(
        "usage:\n  \
         pebbledb sstable dump  <file.sst>\n  \
         pebbledb wal dump      <file.log>\n  \
         pebbledb manifest dump <MANIFEST>\n  \
         pebbledb db get        <dir> <key>\n  \
         pebbledb db scan       <dir>\n  \
         pebbledb db lsm        <dir>\n  \
         pebbledb find          <dir> <key>"
    );
}

/// Renders a byte string, escaping non-printable bytes as `\xNN`.
fn show(bytes: &[u8]) -> String {
    let mut s = String::new();
    for &b in bytes {
        if (0x20..0x7f).contains(&b) && b != b'\\' {
            s.push(b as char);
        } else {
            s.push_str(&format!("\\x{b:02x}"));
        }
    }
    s
}

/// Splits an encoded internal key into a human-readable `user @seq KIND` description.
fn show_internal_key(ikey: &[u8]) -> String {
    if ikey.len() < 8 {
        return format!("{} (malformed: no trailer)", show(ikey));
    }
    let trailer = encoded_trailer(ikey);
    format!(
        "{} @{} {:?}",
        show(encoded_user_key(ikey)),
        trailer_seqnum(trailer),
        trailer_kind(trailer),
    )
}

fn dump_sstable(file: &str) -> Result<(), String> {
    let bytes = std::fs::read(file).map_err(|e| format!("read {file}: {e}"))?;
    let reader = Arc::new(
        Reader::open(bytes, Arc::new(pebbledb::DefaultComparer))
            .map_err(|e| format!("open sstable: {e}"))?,
    );

    let props = reader.properties();
    println!("# properties");
    println!("  num_entries:      {}", props.num_entries);
    println!("  num_deletions:    {}", props.num_deletions);
    println!("  num_range_deletions: {}", props.num_range_deletions);
    println!("  data_size:        {}", props.data_size);
    println!("  index_size:       {}", props.index_size);
    if !props.comparer_name.is_empty() {
        println!("  comparer:         {}", props.comparer_name);
    }
    if !props.compression_name.is_empty() {
        println!("  compression:      {}", props.compression_name);
    }

    println!("# entries");
    let mut it = reader.iter().map_err(|e| format!("iterate: {e}"))?;
    let mut n = 0u64;
    it.first().map_err(|e| format!("seek: {e}"))?;
    while it.valid() {
        println!("  {} => {}", show_internal_key(it.key()), show(it.value()));
        n += 1;
        it.next().map_err(|e| format!("advance: {e}"))?;
    }
    println!("# {n} entries dumped");
    Ok(())
}

/// Parses a WAL log number from a `NNNNNN.log` filename, defaulting to 0.
fn log_num_from_name(file: &str) -> u32 {
    std::path::Path::new(file)
        .file_stem()
        .and_then(|s| s.to_str())
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(0)
}

fn dump_wal(file: &str) -> Result<(), String> {
    let bytes = std::fs::read(file).map_err(|e| format!("read {file}: {e}"))?;
    let mut reader = record::Reader::new(std::io::Cursor::new(bytes), log_num_from_name(file));
    let mut rec_num = 0u64;
    loop {
        match reader.read_record() {
            Ok(Some(rec)) => {
                let batch = Batch::from_bytes(rec.to_vec()).map_err(|e| format!("batch: {e}"))?;
                println!(
                    "record {rec_num}: seqnum={} count={}",
                    batch.seqnum(),
                    batch.count()
                );
                for op in batch.iter() {
                    let op = op.map_err(|e| format!("batch op: {e}"))?;
                    match op.value {
                        Some(v) => {
                            println!("  {:?} {} => {}", op.kind, show(op.key), show(v))
                        }
                        None => println!("  {:?} {}", op.kind, show(op.key)),
                    }
                }
                rec_num += 1;
            }
            Ok(None) => break,
            Err(e) => return Err(format!("read record {rec_num}: {e}")),
        }
    }
    println!("# {rec_num} records dumped");
    Ok(())
}

fn dump_manifest(file: &str) -> Result<(), String> {
    let bytes = std::fs::read(file).map_err(|e| format!("read {file}: {e}"))?;
    // The MANIFEST is a record log whose records are encoded version edits.
    let mut reader = record::Reader::new(std::io::Cursor::new(bytes), 0);
    let mut n = 0u64;
    loop {
        match reader.read_record() {
            Ok(Some(rec)) => {
                let edit = VersionEdit::decode(&rec).map_err(|e| format!("decode edit: {e}"))?;
                print_edit(n, &edit);
                n += 1;
            }
            Ok(None) => break,
            Err(e) => return Err(format!("read record {n}: {e}")),
        }
    }
    println!("# {n} version edits dumped");
    Ok(())
}

fn print_edit(n: u64, edit: &VersionEdit) {
    println!("edit {n}:");
    if let Some(c) = &edit.comparer_name {
        println!("  comparer: {c}");
    }
    if let Some(v) = edit.log_number {
        println!("  log_number: {v}");
    }
    if let Some(v) = edit.next_file_number {
        println!("  next_file_number: {v}");
    }
    if let Some(v) = edit.last_sequence {
        println!("  last_sequence: {v}");
    }
    for (level, file_num) in &edit.deleted_files {
        println!("  delete: L{level} {file_num}");
    }
    for nf in &edit.new_files {
        println!(
            "  add: L{} file={} size={} [{} .. {}]",
            nf.level,
            nf.meta.file_num,
            nf.meta.size,
            show_internal_key(&nf.meta.smallest),
            show_internal_key(&nf.meta.largest),
        );
    }
}

fn open_ro(dir: &str) -> Result<Db, String> {
    Db::open_read_only(dir, Options::default()).map_err(|e| format!("open {dir}: {e}"))
}

fn db_get(dir: &str, key: &str) -> Result<(), String> {
    let db = open_ro(dir)?;
    match db.get(key.as_bytes()).map_err(|e| format!("get: {e}"))? {
        Some(v) => {
            println!("{}", show(&v));
            Ok(())
        }
        None => Err(format!("key not found: {key}")),
    }
}

fn db_scan(dir: &str) -> Result<(), String> {
    let db = open_ro(dir)?;
    let mut it = db.iter().map_err(|e| format!("iter: {e}"))?;
    it.first().map_err(|e| format!("seek: {e}"))?;
    let mut n = 0u64;
    while it.valid() {
        println!("{} => {}", show(it.key()), show(it.value()));
        n += 1;
        it.next().map_err(|e| format!("advance: {e}"))?;
    }
    println!("# {n} keys");
    Ok(())
}

fn db_lsm(dir: &str) -> Result<(), String> {
    let db = open_ro(dir)?;
    print!("{}", db.lsm_view());
    Ok(())
}

fn db_find(dir: &str, key: &str) -> Result<(), String> {
    let db = open_ro(dir)?;
    match db.get(key.as_bytes()).map_err(|e| format!("get: {e}"))? {
        Some(v) => {
            println!("found: {} => {}", key, show(&v));
            Ok(())
        }
        None => {
            println!("not found: {key}");
            Ok(())
        }
    }
}
