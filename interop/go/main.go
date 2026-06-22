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
//	interop generate-separated    <dir>   FormatValueSeparation DB with native blob files
//	interop verify                <dir>   open <dir> read-only and verify the known keys
//	interop verify-columnar-sst   <file>  read a Rust-written columnar .sst and verify the keys
//	interop verify-pebble-blob    <file>  read a Rust-written native .blob via Pebble's blob reader
//
// The keys are key0000..key0099 with values value0..value99.
package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/sstable/blob"
	"github.com/cockroachdb/pebble/v2/vfs"
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
	case "generate-separated":
		// A FormatValueSeparation (format 24) database whose values are separated into native
		// blob files.
		generateSeparated(dir)
	case "verify":
		verify(dir)
	case "verify-columnar-sst":
		// `dir` is actually a path to a single columnar .sst file written by the Rust engine.
		verifyColumnarSST(dir)
	case "verify-pebble-blob":
		// `dir` is a path to a native .blob file written by the Rust PebbleBlobWriter.
		verifyPebbleBlob(dir)
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

// generateSeparated writes a FormatValueSeparation (format 24) database with value separation
// enabled, so values are stored out-of-line in native blob files. Keys key00000..key00029 hold
// "V<i>-" repeated 20 times (each above the separation threshold).
func generateSeparated(dir string) {
	opts := &pebble.Options{FormatMajorVersion: pebble.FormatValueSeparation}
	opts.Experimental.ValueSeparationPolicy = func() pebble.ValueSeparationPolicy {
		return pebble.ValueSeparationPolicy{
			Enabled:               true,
			MinimumSize:           20,
			MaxBlobReferenceDepth: 10,
			TargetGarbageRatio:    1.0,
		}
	}
	db, err := pebble.Open(dir, opts)
	must(err)
	for i := 0; i < 30; i++ {
		v := strings.Repeat(fmt.Sprintf("V%d-", i), 20)
		must(db.Set([]byte(fmt.Sprintf("key%05d", i)), []byte(v), pebble.Sync))
	}
	must(db.Flush())
	must(db.Close())
	fmt.Printf("generated value-separated database in %s\n", dir)
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

// verifyColumnarSST opens a single columnar sstable written by the Rust engine and verifies the
// known keys read back through Pebble's own sstable reader — the Rust→Go columnar byte-parity
// direction. The file holds key0000..key0099 => value0..value99.
func verifyColumnarSST(path string) {
	data, err := os.ReadFile(path)
	must(err)
	r, err := sstable.NewMemReader(data, sstable.ReaderOptions{})
	must(err)
	defer r.Close()
	it, err := r.NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
	must(err)
	count := 0
	for kv := it.First(); kv != nil; kv = it.Next() {
		v, _, err := kv.Value(nil)
		must(err)
		wantK := fmt.Sprintf("key%04d", count)
		wantV := fmt.Sprintf("value%d", count)
		if string(kv.K.UserKey) != wantK || string(v) != wantV {
			fmt.Fprintf(os.Stderr, "mismatch at %d: key=%q value=%q\n", count, kv.K.UserKey, v)
			os.Exit(1)
		}
		count++
	}
	must(it.Error())
	if count != n {
		fmt.Fprintf(os.Stderr, "expected %d keys, got %d\n", n, count)
		os.Exit(1)
	}

	// Verify the keyspans the Rust writer emitted (range deletion + range key set), proving the
	// columnar keyspan blocks are byte-parseable by Pebble too.
	rdi, err := r.NewRawRangeDelIter(context.Background(), sstable.NoFragmentTransforms, sstable.NoReadEnv)
	must(err)
	rdCount := 0
	if rdi != nil {
		s, err := rdi.First()
		for ; err == nil && s != nil; s, err = rdi.Next() {
			if string(s.Start) != "key0030" || string(s.End) != "key0040" {
				fmt.Fprintf(os.Stderr, "unexpected range del [%s,%s)\n", s.Start, s.End)
				os.Exit(1)
			}
			rdCount++
		}
		must(err)
	}
	rki, err := r.NewRawRangeKeyIter(context.Background(), sstable.NoFragmentTransforms, sstable.NoReadEnv)
	must(err)
	rkCount := 0
	if rki != nil {
		s, err := rki.First()
		for ; err == nil && s != nil; s, err = rki.Next() {
			for _, k := range s.Keys {
				if string(s.Start) != "key0050" || string(s.End) != "key0060" ||
					string(k.Suffix) != "@1" || string(k.Value) != "rkval" {
					fmt.Fprintf(os.Stderr, "unexpected range key [%s,%s) %s=%s\n", s.Start, s.End, k.Suffix, k.Value)
					os.Exit(1)
				}
				rkCount++
			}
		}
		must(err)
	}
	if rdCount != 1 || rkCount != 1 {
		fmt.Fprintf(os.Stderr, "expected 1 range del + 1 range key, got %d + %d\n", rdCount, rkCount)
		os.Exit(1)
	}
	fmt.Printf("verified %d keys + %d range del + %d range key in columnar sstable %s\n",
		count, rdCount, rkCount, path)
}

// verifyPebbleBlob opens a native blob file written by the Rust PebbleBlobWriter through Pebble's
// own blob.FileReader and validates its structure via Layout (which reads the footer, index, and
// every value block) — the Rust→Go native-blob byte-parity direction.
func verifyPebbleBlob(path string) {
	f, err := vfs.Default.Open(path)
	must(err)
	readable, err := sstable.NewSimpleReadable(f)
	must(err)
	r, err := blob.NewFileReader(context.Background(), readable, blob.FileReaderOptions{})
	must(err)
	defer r.Close()
	layout, err := r.Layout()
	must(err)
	if !strings.Contains(layout, "values: 20") {
		fmt.Fprintf(os.Stderr, "unexpected blob layout (no 20 values):\n%s\n", layout)
		os.Exit(1)
	}
	fmt.Printf("verified native blob file %s (Pebble parsed it)\n", path)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
