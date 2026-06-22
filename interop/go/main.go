// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

// Command interop generates and verifies Pebble databases for cross-implementation
// testing against the Rust pebbledb port.
//
//	interop generate              <dir>   write known keys in the row (block) sstable format
//	interop generate-columnar     <dir>   write known keys in the columnar sstable format
//	interop generate-columnar-spans <dir> columnar keys + a range deletion and a range key
//	interop generate-columnar-valueblock <dir> columnar table with an out-of-line value block
//	interop verify                <dir>   open <dir> read-only and verify the known keys
//
// The keys are key0000..key0099 with values value0..value99.
package main

import (
	"bytes"
	"fmt"
	"os"
	"strings"

	"github.com/cockroachdb/pebble/v2"
)

const n = 100

func key(i int) []byte   { return []byte(fmt.Sprintf("key%04d", i)) }
func value(i int) []byte { return []byte(fmt.Sprintf("value%d", i)) }

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: interop <generate|verify> <dir>")
		os.Exit(2)
	}
	cmd, dir := os.Args[1], os.Args[2]
	switch cmd {
	case "generate":
		// FormatMinSupported is the oldest format v2 still supports — the classic row
		// (block-based) sstable layout.
		generate(dir, pebble.FormatMinSupported)
	case "generate-columnar":
		// FormatColumnarBlocks switches sstables to the columnar block layout.
		generate(dir, pebble.FormatColumnarBlocks)
	case "generate-columnar-spans":
		// A columnar table that also carries keyspans: a range deletion and a range key,
		// exercising the columnar (boundary-based) keyspan block format.
		generateColumnarSpans(dir)
	case "generate-columnar-valueblock":
		// A columnar table with an out-of-line value: a key written twice with a snapshot
		// pinning the older version, so the older SET's value is stored in a value block.
		generateColumnarValueBlock(dir)
	case "verify":
		verify(dir)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", cmd)
		os.Exit(2)
	}
}

func generate(dir string, format pebble.FormatMajorVersion) {
	db, err := pebble.Open(dir, &pebble.Options{
		FormatMajorVersion: format,
	})
	must(err)
	for i := 0; i < n; i++ {
		must(db.Set(key(i), value(i), pebble.Sync))
	}
	must(db.Flush())
	must(db.Close())
	fmt.Printf("generated %d keys in %s (format %v)\n", n, dir, format)
}

// generateColumnarSpans writes a columnar database that, beyond point keys, contains a range
// deletion and a range key, so the Rust reader can be checked against the columnar keyspan
// (boundary-based) block format. Matches tests/fixtures/pebble_v2_columnar_spans.sst.
func generateColumnarSpans(dir string) {
	db, err := pebble.Open(dir, &pebble.Options{
		FormatMajorVersion: pebble.FormatColumnarBlocks,
	})
	must(err)
	for i := 0; i < 20; i++ {
		must(db.Set([]byte(fmt.Sprintf("key%05d", i)), []byte(fmt.Sprintf("value%d", i)), pebble.Sync))
	}
	must(db.DeleteRange([]byte("key00005"), []byte("key00010"), pebble.Sync))
	must(db.RangeKeySet([]byte("key00012"), []byte("key00015"), []byte("@1"), []byte("rkval"), pebble.Sync))
	must(db.Flush())
	must(db.Close())
	fmt.Printf("generated columnar keys + spans in %s\n", dir)
}

// generateColumnarValueBlock writes a columnar database with an out-of-line value. key00002 is
// written, a snapshot is taken (pinning that version), then key00002 is overwritten; both
// versions survive the flush and share an identical user key, so Pebble stores the older SET's
// value in a value block (is-value-external). Matches
// tests/fixtures/pebble_v2_columnar_valueblock.sst.
func generateColumnarValueBlock(dir string) {
	db, err := pebble.Open(dir, &pebble.Options{
		FormatMajorVersion: pebble.FormatColumnarBlocks,
	})
	must(err)
	must(db.Set([]byte("key00002"), []byte("OLDVALUE-"+strings.Repeat("o", 20)), pebble.Sync))
	snap := db.NewSnapshot()
	must(db.Set([]byte("key00000"), []byte("v0"), pebble.Sync))
	must(db.Set([]byte("key00001"), []byte("v1"), pebble.Sync))
	must(db.Set([]byte("key00002"), []byte("NEWVALUE-"+strings.Repeat("n", 20)), pebble.Sync))
	must(db.Set([]byte("key00003"), []byte("v3"), pebble.Sync))
	must(db.Flush())
	must(snap.Close())
	must(db.Close())
	fmt.Printf("generated columnar value-block table in %s\n", dir)
}

func verify(dir string) {
	db, err := pebble.Open(dir, &pebble.Options{ReadOnly: true})
	must(err)
	defer db.Close()
	for i := 0; i < n; i++ {
		v, closer, err := db.Get(key(i))
		if err != nil {
			fmt.Fprintf(os.Stderr, "missing %s: %v\n", key(i), err)
			os.Exit(1)
		}
		if !bytes.Equal(v, value(i)) {
			fmt.Fprintf(os.Stderr, "mismatch for %s: got %q want %q\n", key(i), v, value(i))
			os.Exit(1)
		}
		closer.Close()
	}
	fmt.Printf("verified %d keys in %s\n", n, dir)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
