// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

// Command interop generates and verifies Pebble databases for cross-implementation
// testing against the Rust pebbledb port.
//
//	interop generate              <dir>   write known keys in the row (block) sstable format
//	interop generate-columnar     <dir>   write known keys in the columnar sstable format
//	interop generate-columnar-spans <dir> columnar keys + a range deletion and a range key
//	interop verify                <dir>   open <dir> read-only and verify the known keys
//
// The keys are key0000..key0099 with values value0..value99.
package main

import (
	"bytes"
	"fmt"
	"os"

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
