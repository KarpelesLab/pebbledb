// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

//! A small **data-driven** test harness in the style of Pebble's `testdata` suites: each
//! case is a script of newline-separated commands applied to a fresh database, with the
//! expected output declared inline. New scenarios are added by appending a `Case` — no Rust
//! changes needed — which keeps behavioral coverage easy to extend.
//!
//! Commands:
//! ```text
//! set <key> <value>      put a key
//! del <key>              delete a key
//! delrange <start> <end> delete the half-open range [start, end)
//! rangekey <s> <e> <suf> <val>   set a range key over [s,e) at suffix
//! merge <key> <value>    merge-append (ConcatMerger)
//! flush                  flush the memtable
//! compact                compact the whole key space
//! reopen                 close and reopen the database
//! get <key>              -> "<value>" or "<nil>"
//! scan                   -> "k=v" pairs, space-separated, in key order
//! ```

use std::fmt::Write as _;

use pebbledb::{ConcatMerger, Db, Options};
use std::sync::Arc;

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!("pebbledb-dd-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p
}

/// Runs `script` against a fresh database and returns the concatenated output of every query
/// command (`get` / `scan`), one line each.
fn run_script(dir: &std::path::Path, script: &str) -> String {
    let opts = || Options {
        mem_table_size: 8 * 1024,
        merger: Some(Arc::new(ConcatMerger)),
        ..Default::default()
    };
    let mut db = Db::open(dir, opts()).unwrap();
    let mut out = String::new();
    for line in script.lines().map(str::trim).filter(|l| !l.is_empty()) {
        let parts: Vec<&str> = line.split_whitespace().collect();
        match parts.as_slice() {
            ["set", k, v] => db.set(k.as_bytes(), v.as_bytes()).unwrap(),
            ["del", k] => db.delete(k.as_bytes()).unwrap(),
            ["delrange", s, e] => db.delete_range(s.as_bytes(), e.as_bytes()).unwrap(),
            ["rangekey", s, e, suf, val] => db
                .range_key_set(s.as_bytes(), e.as_bytes(), suf.as_bytes(), val.as_bytes())
                .unwrap(),
            ["merge", k, v] => db.merge(k.as_bytes(), v.as_bytes()).unwrap(),
            ["flush"] => db.flush().unwrap(),
            ["compact"] => db.compact_range(None, None).unwrap(),
            ["reopen"] => {
                drop(db);
                db = Db::open(dir, opts()).unwrap();
            }
            ["get", k] => {
                let v = db.get(k.as_bytes()).unwrap();
                let shown = v
                    .as_deref()
                    .map(|b| String::from_utf8_lossy(b).into_owned())
                    .unwrap_or_else(|| "<nil>".to_string());
                writeln!(out, "get {k} -> {shown}").unwrap();
            }
            ["scan"] => {
                let mut it = db.iter().unwrap();
                it.first().unwrap();
                let mut pairs = Vec::new();
                while it.valid() {
                    pairs.push(format!(
                        "{}={}",
                        String::from_utf8_lossy(it.key()),
                        String::from_utf8_lossy(it.value())
                    ));
                    it.next().unwrap();
                }
                writeln!(out, "scan -> {}", pairs.join(" ")).unwrap();
            }
            other => panic!("unknown command: {other:?}"),
        }
    }
    out
}

struct Case {
    name: &'static str,
    script: &'static str,
    expected: &'static str,
}

const CASES: &[Case] = &[
    Case {
        name: "overwrite-and-delete",
        script: "
            set a 1
            set b 2
            set a 3
            del b
            get a
            get b
            scan
        ",
        expected: "\
get a -> 3
get b -> <nil>
scan -> a=3
",
    },
    Case {
        name: "range-delete-spans-flush",
        script: "
            set k1 v1
            set k2 v2
            set k3 v3
            flush
            delrange k1 k3
            get k1
            get k2
            get k3
            scan
        ",
        expected: "\
get k1 -> <nil>
get k2 -> <nil>
get k3 -> v3
scan -> k3=v3
",
    },
    Case {
        name: "merge-accumulates-then-survives-compaction",
        script: "
            set k base
            merge k +1
            flush
            merge k +2
            compact
            get k
        ",
        expected: "\
get k -> base+1+2
",
    },
    Case {
        name: "durability-across-reopen",
        script: "
            set x 100
            set y 200
            del y
            reopen
            get x
            get y
            set z 300
            reopen
            get z
            scan
        ",
        expected: "\
get x -> 100
get y -> <nil>
get z -> 300
scan -> x=100 z=300
",
    },
    Case {
        name: "tombstone-then-resurrect",
        script: "
            set a 1
            flush
            del a
            flush
            get a
            set a 2
            get a
            compact
            get a
        ",
        expected: "\
get a -> <nil>
get a -> 2
get a -> 2
",
    },
];

#[test]
fn data_driven_cases() {
    for case in CASES {
        let dir = temp_dir(case.name);
        let got = run_script(&dir, case.script);
        assert_eq!(
            got, case.expected,
            "data-driven case {:?} diverged:\n--- got ---\n{got}--- want ---\n{}",
            case.name, case.expected
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
