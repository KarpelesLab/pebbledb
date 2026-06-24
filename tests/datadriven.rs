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
//!
//! Iterator directives drive a single persistent iterator and each emit a trace line of the
//! resulting position (`<key>=<value>`, `.` when invalid/exhausted, or `at-limit`). They mirror
//! Pebble's `iter` testdata blocks:
//! ```text
//! iter-bounds <lo> <hi>          (re)create the iterator with [lo, hi) bounds (use "." for none)
//! iter-first / iter-last
//! iter-next / iter-prev / iter-next-prefix
//! iter-seek-ge <key> / iter-seek-lt <key>
//! iter-next-limit <limit> / iter-prev-limit <limit>
//! iter-seek-ge-limit <key> <limit> / iter-seek-lt-limit <key> <limit>
//! ```
//! ```

use std::fmt::Write as _;

use pebbledb::{ConcatMerger, Db, DbIterator, IterOptions, IterValidity, Options};
use std::sync::Arc;

/// Renders the current iterator position as `<key>=<value>` or `.` when invalid.
fn pos(it: &DbIterator) -> String {
    if it.valid() {
        format!(
            "{}={}",
            String::from_utf8_lossy(it.key()),
            String::from_utf8_lossy(it.value())
        )
    } else {
        ".".to_string()
    }
}

/// Renders a limited-step outcome.
fn validity(it: &DbIterator, v: IterValidity) -> String {
    match v {
        IterValidity::Valid => format!("valid {}", pos(it)),
        IterValidity::AtLimit => "at-limit".to_string(),
        IterValidity::Exhausted => "exhausted".to_string(),
    }
}

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
    // A single persistent iterator driven by the `iter-*` directives. Recreated by `iter-bounds`
    // (and lazily on first use) so it observes the data written so far.
    let mut iter: Option<DbIterator> = None;
    let bound = |s: &str| (s != ".").then(|| s.as_bytes().to_vec());
    macro_rules! it_mut {
        () => {{
            if iter.is_none() {
                iter = Some(db.iter().unwrap());
            }
            iter.as_mut().unwrap()
        }};
    }
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
            ["iter-bounds", lo, hi] => {
                iter = Some(
                    db.iter_with_options(IterOptions {
                        lower_bound: bound(lo),
                        upper_bound: bound(hi),
                        ..Default::default()
                    })
                    .unwrap(),
                );
            }
            ["iter-first"] => {
                let it = it_mut!();
                it.first().unwrap();
                writeln!(out, "iter-first -> {}", pos(it)).unwrap();
            }
            ["iter-last"] => {
                let it = it_mut!();
                it.last().unwrap();
                writeln!(out, "iter-last -> {}", pos(it)).unwrap();
            }
            ["iter-next"] => {
                let it = it_mut!();
                it.next().unwrap();
                writeln!(out, "iter-next -> {}", pos(it)).unwrap();
            }
            ["iter-prev"] => {
                let it = it_mut!();
                it.prev().unwrap();
                writeln!(out, "iter-prev -> {}", pos(it)).unwrap();
            }
            ["iter-next-prefix"] => {
                let it = it_mut!();
                it.next_prefix().unwrap();
                writeln!(out, "iter-next-prefix -> {}", pos(it)).unwrap();
            }
            ["iter-seek-ge", k] => {
                let it = it_mut!();
                it.seek_ge(k.as_bytes()).unwrap();
                writeln!(out, "iter-seek-ge {k} -> {}", pos(it)).unwrap();
            }
            ["iter-seek-lt", k] => {
                let it = it_mut!();
                it.seek_lt(k.as_bytes()).unwrap();
                writeln!(out, "iter-seek-lt {k} -> {}", pos(it)).unwrap();
            }
            ["iter-next-limit", l] => {
                let it = it_mut!();
                let v = it.next_with_limit(Some(l.as_bytes())).unwrap();
                writeln!(out, "iter-next-limit {l} -> {}", validity(it, v)).unwrap();
            }
            ["iter-prev-limit", l] => {
                let it = it_mut!();
                let v = it.prev_with_limit(Some(l.as_bytes())).unwrap();
                writeln!(out, "iter-prev-limit {l} -> {}", validity(it, v)).unwrap();
            }
            ["iter-seek-ge-limit", k, l] => {
                let it = it_mut!();
                let v = it
                    .seek_ge_with_limit(k.as_bytes(), Some(l.as_bytes()))
                    .unwrap();
                writeln!(out, "iter-seek-ge-limit {k} {l} -> {}", validity(it, v)).unwrap();
            }
            ["iter-seek-lt-limit", k, l] => {
                let it = it_mut!();
                let v = it
                    .seek_lt_with_limit(k.as_bytes(), Some(l.as_bytes()))
                    .unwrap();
                writeln!(out, "iter-seek-lt-limit {k} {l} -> {}", validity(it, v)).unwrap();
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
    Case {
        name: "iter-forward-reverse-and-seek",
        script: "
            set a 1
            set b 2
            set c 3
            set d 4
            flush
            iter-first
            iter-next
            iter-next
            iter-seek-ge c
            iter-seek-lt c
            iter-last
            iter-prev
            iter-seek-ge z
        ",
        expected: "\
iter-first -> a=1
iter-next -> b=2
iter-next -> c=3
iter-seek-ge c -> c=3
iter-seek-lt c -> b=2
iter-last -> d=4
iter-prev -> c=3
iter-seek-ge z -> .
",
    },
    Case {
        name: "iter-bounds-restrict-range",
        script: "
            set a 1
            set b 2
            set c 3
            set d 4
            set e 5
            iter-bounds b d
            iter-first
            iter-next
            iter-next
            iter-last
            iter-seek-ge a
            iter-seek-ge d
        ",
        expected: "\
iter-first -> b=2
iter-next -> c=3
iter-next -> .
iter-last -> c=3
iter-seek-ge a -> b=2
iter-seek-ge d -> .
",
    },
    Case {
        name: "iter-limited-pause-and-resume",
        script: "
            set a 1
            set b 2
            set c 3
            set d 4
            flush
            iter-first
            iter-next-limit c
            iter-next-limit c
            iter-next-limit z
            iter-next-limit z
            iter-seek-ge-limit a c
            iter-seek-ge-limit c c
        ",
        expected: "\
iter-first -> a=1
iter-next-limit c -> valid b=2
iter-next-limit c -> at-limit
iter-next-limit z -> valid c=3
iter-next-limit z -> valid d=4
iter-seek-ge-limit a c -> valid a=1
iter-seek-ge-limit c c -> at-limit
",
    },
    Case {
        name: "iter-reverse-limited",
        script: "
            set a 1
            set b 2
            set c 3
            set d 4
            flush
            iter-last
            iter-prev-limit c
            iter-prev-limit c
            iter-prev-limit a
            iter-seek-lt-limit d b
            iter-seek-lt-limit b b
        ",
        expected: "\
iter-last -> d=4
iter-prev-limit c -> valid c=3
iter-prev-limit c -> at-limit
iter-prev-limit a -> valid b=2
iter-seek-lt-limit d b -> valid c=3
iter-seek-lt-limit b b -> at-limit
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
