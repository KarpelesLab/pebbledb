// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

// Command interop generates and verifies Pebble databases for cross-implementation
// testing against the Rust pebbledb port.
//
//	interop generate          <dir>   write known keys in the row (block) sstable format
//	interop generate-columnar <dir>   write known keys in the columnar sstable format
//	interop verify            <dir>   open <dir> read-only and verify the known keys
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
