// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

//! End-to-end integration tests driving pebbledb through its public API: open, write,
//! flush, compact, snapshot, iterate, reopen, and recover.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use std::sync::Arc;

use pebbledb::vfs::MemFs;
use pebbledb::{Batch, Db, IterOptions, Options};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let p = std::env::temp_dir().join(format!("pebbledb-e2e-{tag}-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn collect(db: &Db) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut it = db.iter().unwrap();
    let mut out = Vec::new();
    it.first().unwrap();
    while it.valid() {
        out.push((it.key().to_vec(), it.value().to_vec()));
        it.next().unwrap();
    }
    out
}

#[test]
fn full_lifecycle_with_reopen_and_compaction() {
    let dir = temp_dir("lifecycle");
    let opts = Options {
        mem_table_size: 32 * 1024, // small, to force flushes + compaction
        ..Default::default()
    };

    // Phase 1: write a few thousand keys, overwriting some.
    {
        let db = Db::open(&dir, opts.clone()).unwrap();
        for i in 0..4000u32 {
            db.set(format!("key{i:06}").as_bytes(), format!("v{i}").as_bytes())
                .unwrap();
        }
        // Overwrite the first 100 with new values via a batch.
        let mut batch = Batch::new();
        for i in 0..100u32 {
            batch.set(format!("key{i:06}").as_bytes(), b"overwritten");
        }
        db.write(batch).unwrap();
        // Delete a band of keys.
        for i in 2000..2100u32 {
            db.delete(format!("key{i:06}").as_bytes()).unwrap();
        }

        // Compaction should have kept L0 bounded.
        let m = db.metrics();
        assert!(m.level_files[0] < 4, "L0 not drained: {:?}", m.level_files);

        // Reads reflect the latest state.
        assert_eq!(db.get(b"key000000").unwrap(), Some(b"overwritten".to_vec()));
        assert_eq!(db.get(b"key003999").unwrap(), Some(b"v3999".to_vec()));
        assert_eq!(db.get(b"key002050").unwrap(), None); // deleted
    }

    // Phase 2: reopen and confirm durability across a close/open cycle.
    {
        let db = Db::open(&dir, opts.clone()).unwrap();
        assert_eq!(db.get(b"key000000").unwrap(), Some(b"overwritten".to_vec()));
        assert_eq!(db.get(b"key000099").unwrap(), Some(b"overwritten".to_vec()));
        assert_eq!(db.get(b"key000100").unwrap(), Some(b"v100".to_vec()));
        assert_eq!(db.get(b"key003999").unwrap(), Some(b"v3999".to_vec()));
        assert_eq!(db.get(b"key002000").unwrap(), None);
        assert_eq!(db.get(b"key002099").unwrap(), None);

        // 4000 written, 100 deleted -> 3900 live keys, in sorted order.
        let all = collect(&db);
        assert_eq!(all.len(), 3900);
        for w in all.windows(2) {
            assert!(w[0].0 < w[1].0, "iteration must be sorted");
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn snapshot_isolation() {
    let dir = temp_dir("snapshot");
    let db = Db::open(&dir, Options::default()).unwrap();
    db.set(b"k", b"v1").unwrap();
    let snap = db.snapshot();
    db.set(b"k", b"v2").unwrap();
    db.set(b"new", b"after-snapshot").unwrap();

    // The snapshot sees the old value and not the later insert.
    assert_eq!(snap.get(b"k").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(snap.get(b"new").unwrap(), None);
    // The live database sees the latest.
    assert_eq!(db.get(b"k").unwrap(), Some(b"v2".to_vec()));
    assert_eq!(db.get(b"new").unwrap(), Some(b"after-snapshot".to_vec()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn block_cache_serves_repeated_reads() {
    let dir = temp_dir("blockcache");
    let db = Db::open(&dir, Options::default()).unwrap();
    for i in 0..500u32 {
        db.set(format!("k{i:04}").as_bytes(), format!("v{i}").as_bytes())
            .unwrap();
    }
    db.flush().unwrap(); // push everything to an L0 sstable

    // First pass populates the block cache; the second pass should hit it.
    for _ in 0..2 {
        for i in 0..500u32 {
            assert_eq!(
                db.get(format!("k{i:04}").as_bytes()).unwrap(),
                Some(format!("v{i}").into_bytes())
            );
        }
    }
    let m = db.metrics();
    assert!(
        m.block_cache_hits > 0,
        "expected block-cache hits, got {}",
        m.block_cache_hits
    );

    let _ = std::fs::remove_dir_all(&dir);
}

fn collect_reverse(it: &mut pebbledb::DbIterator) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    it.last().unwrap();
    while it.valid() {
        out.push((it.key().to_vec(), it.value().to_vec()));
        it.prev().unwrap();
    }
    out
}

#[test]
fn reverse_iteration_matches_forward() {
    let dir = temp_dir("reverse");
    let opts = Options {
        mem_table_size: 16 * 1024, // force several flushes + levels
        ..Default::default()
    };
    let db = Db::open(&dir, opts).unwrap();
    for i in 0..1500u32 {
        db.set(format!("k{i:05}").as_bytes(), format!("v{i}").as_bytes())
            .unwrap();
    }
    // Overwrite some so multiple versions exist across levels.
    for i in 0..200u32 {
        db.set(format!("k{i:05}").as_bytes(), b"new").unwrap();
    }
    db.delete(b"k00500").unwrap();

    let forward = collect(&db);
    let mut it = db.iter().unwrap();
    let mut reverse = collect_reverse(&mut it);
    reverse.reverse();
    assert_eq!(forward, reverse, "reverse iteration must mirror forward");
    assert_eq!(forward.len(), 1499); // 1500 written, 1 deleted
    assert_eq!(db.get(b"k00000").unwrap(), Some(b"new".to_vec()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn seek_and_bounds() {
    let dir = temp_dir("seek-bounds");
    let db = Db::open(&dir, Options::default()).unwrap();
    for i in 0..100u32 {
        db.set(format!("k{i:03}").as_bytes(), format!("v{i}").as_bytes())
            .unwrap();
    }
    db.flush().unwrap();

    // seek_ge lands on the first key >= target.
    let mut it = db.iter().unwrap();
    it.seek_ge(b"k042").unwrap();
    assert!(it.valid());
    assert_eq!(it.key(), b"k042");
    it.next().unwrap();
    assert_eq!(it.key(), b"k043");

    // seek_ge to a gap rounds up.
    it.seek_ge(b"k0419").unwrap();
    assert_eq!(it.key(), b"k042");

    // seek_lt lands on the last key < target.
    it.seek_lt(b"k042").unwrap();
    assert_eq!(it.key(), b"k041");
    it.prev().unwrap();
    assert_eq!(it.key(), b"k040");

    // Direction change: seek_ge then prev.
    it.seek_ge(b"k050").unwrap();
    it.prev().unwrap();
    assert_eq!(it.key(), b"k049");
    it.next().unwrap();
    assert_eq!(it.key(), b"k050");

    // Bounds restrict the visible range.
    let mut bit = db
        .iter_with_options(IterOptions {
            lower_bound: Some(b"k010".to_vec()),
            upper_bound: Some(b"k020".to_vec()),
        })
        .unwrap();
    bit.first().unwrap();
    assert_eq!(bit.key(), b"k010");
    let mut keys = Vec::new();
    while bit.valid() {
        keys.push(String::from_utf8(bit.key().to_vec()).unwrap());
        bit.next().unwrap();
    }
    assert_eq!(keys.first().unwrap(), "k010");
    assert_eq!(keys.last().unwrap(), "k019"); // upper bound exclusive
    assert_eq!(keys.len(), 10);

    // last() honors the upper bound.
    bit.last().unwrap();
    assert_eq!(bit.key(), b"k019");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn prefix_iteration() {
    let dir = temp_dir("prefix");
    let db = Db::open(&dir, Options::default()).unwrap();
    for k in ["aa1", "aa2", "ab1", "ba1", "bb1"] {
        db.set(k.as_bytes(), b"x").unwrap();
    }
    db.flush().unwrap();

    let mut it = db.iter().unwrap();
    it.seek_prefix_ge(b"aa", b"aa").unwrap();
    let mut keys = Vec::new();
    while it.valid() {
        keys.push(String::from_utf8(it.key().to_vec()).unwrap());
        it.next().unwrap();
    }
    assert_eq!(keys, ["aa1", "aa2"]); // stops at the prefix boundary

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn runs_fully_in_memory_on_memfs() {
    // The whole open/flush/compact/reopen lifecycle on an in-memory filesystem, never
    // touching disk. Cloning the MemFs shares the underlying tree, so a reopen sees the
    // same files.
    let fs = Arc::new(MemFs::new());
    let dir = "/db";
    let opts = || Options {
        fs: fs.clone(),
        mem_table_size: 16 * 1024,
        ..Default::default()
    };
    {
        let db = Db::open(dir, opts()).unwrap();
        for i in 0..1000u32 {
            db.set(format!("k{i:05}").as_bytes(), format!("v{i}").as_bytes())
                .unwrap();
        }
        db.delete(b"k00042").unwrap();
        db.flush().unwrap();
        assert_eq!(db.get(b"k00000").unwrap(), Some(b"v0".to_vec()));
        assert_eq!(db.get(b"k00042").unwrap(), None);
    }
    // Reopen against the same in-memory tree; data survives the WAL/MANIFEST round-trip.
    {
        let db = Db::open(dir, opts()).unwrap();
        assert_eq!(db.get(b"k00000").unwrap(), Some(b"v0".to_vec()));
        assert_eq!(db.get(b"k00999").unwrap(), Some(b"v999".to_vec()));
        assert_eq!(db.get(b"k00042").unwrap(), None);
        let all = collect(&db);
        assert_eq!(all.len(), 999);
    }
}

#[test]
fn directory_lock_blocks_second_open() {
    let dir = temp_dir("lock");
    let db = Db::open(&dir, Options::default()).unwrap();
    db.set(b"k", b"v").unwrap();
    // A second read-write open of the same directory must fail while the first holds the
    // exclusive lock.
    assert!(
        Db::open(&dir, Options::default()).is_err(),
        "second open should be blocked by the directory lock"
    );
    drop(db);
    // Once the first handle is dropped, the lock is released and reopening succeeds.
    let db2 = Db::open(&dir, Options::default()).unwrap();
    assert_eq!(db2.get(b"k").unwrap(), Some(b"v".to_vec()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn checkpoint_produces_openable_copy() {
    let dir = temp_dir("ckpt-src");
    let dest = temp_dir("ckpt-dst");
    {
        let db = Db::open(&dir, Options::default()).unwrap();
        for i in 0..300u32 {
            db.set(format!("k{i:04}").as_bytes(), format!("v{i}").as_bytes())
                .unwrap();
        }
        db.delete(b"k0100").unwrap();
        db.checkpoint(&dest).unwrap();
        // The source remains usable after checkpointing.
        assert_eq!(db.get(b"k0000").unwrap(), Some(b"v0".to_vec()));
    }
    // The checkpoint opens as a standalone database with the same contents.
    {
        let db = Db::open(&dest, Options::default()).unwrap();
        assert_eq!(db.get(b"k0000").unwrap(), Some(b"v0".to_vec()));
        assert_eq!(db.get(b"k0299").unwrap(), Some(b"v299".to_vec()));
        assert_eq!(db.get(b"k0100").unwrap(), None);
        assert_eq!(collect(&db).len(), 299);
    }
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&dest);
}

#[test]
fn ingest_external_sstable() {
    use pebbledb::base::internal_key::{InternalKey, InternalKeyKind};
    use pebbledb::sstable::{Writer, WriterOptions};

    let dir = temp_dir("ingest");
    let db = Db::open(&dir, Options::default()).unwrap();
    // Pre-existing data, including a key the ingest will shadow.
    db.set(b"apple", b"old").unwrap();
    db.set(b"cherry", b"keep").unwrap();
    db.flush().unwrap();

    // Build an external sstable (keys at seqnum 0, as an offline builder would produce).
    let ext = dir.join("external.sst");
    {
        let f = std::fs::File::create(&ext).unwrap();
        let cmp = std::sync::Arc::new(pebbledb::DefaultComparer);
        let mut w = Writer::new(f, cmp, WriterOptions::default());
        for (k, v) in [("apple", "new"), ("banana", "fresh")] {
            let ikey = InternalKey::new(k.as_bytes().to_vec(), 0, InternalKeyKind::Set).encode();
            w.add(&ikey, v.as_bytes()).unwrap();
        }
        w.finish().unwrap();
    }

    db.ingest(&[&ext]).unwrap();

    // The ingested values win over older data and add new keys; untouched keys remain.
    assert_eq!(db.get(b"apple").unwrap(), Some(b"new".to_vec()));
    assert_eq!(db.get(b"banana").unwrap(), Some(b"fresh".to_vec()));
    assert_eq!(db.get(b"cherry").unwrap(), Some(b"keep".to_vec()));

    // And it survives a reopen.
    drop(db);
    let db = Db::open(&dir, Options::default()).unwrap();
    assert_eq!(db.get(b"apple").unwrap(), Some(b"new".to_vec()));
    assert_eq!(db.get(b"banana").unwrap(), Some(b"fresh".to_vec()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn format_major_version_persists_and_ratchets() {
    use pebbledb::FormatMajorVersion;

    let dir = temp_dir("fmv");
    // Create at the most-compatible format.
    {
        let db = Db::open(
            &dir,
            Options {
                format_major_version: FormatMajorVersion::MOST_COMPATIBLE,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            db.format_major_version(),
            FormatMajorVersion::MOST_COMPATIBLE
        );
        db.set(b"k", b"v").unwrap();
        // Ratchet upward; a lower target is a no-op.
        db.ratchet_format_major_version(FormatMajorVersion::MOST_COMPATIBLE)
            .unwrap();
        db.ratchet_format_major_version(FormatMajorVersion::VALUE_BLOCKS)
            .unwrap();
        assert_eq!(db.format_major_version(), FormatMajorVersion::VALUE_BLOCKS);
    }
    // The ratcheted version is recovered from the OPTIONS file on reopen, ignoring the
    // option passed for fresh stores.
    {
        let db = Db::open(
            &dir,
            Options {
                format_major_version: FormatMajorVersion::MOST_COMPATIBLE,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(db.format_major_version(), FormatMajorVersion::VALUE_BLOCKS);
        assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn open_rejects_comparer_mismatch() {
    use pebbledb::{Comparer, DefaultComparer};
    use std::cmp::Ordering;

    // A comparer that behaves bytewise but reports a different name.
    struct Renamed(DefaultComparer);
    impl Comparer for Renamed {
        fn name(&self) -> &str {
            "test.RenamedComparator"
        }
        fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
            self.0.compare(a, b)
        }
        fn abbreviated_key(&self, key: &[u8]) -> u64 {
            self.0.abbreviated_key(key)
        }
        fn separator(&self, dst: &mut Vec<u8>, a: &[u8], b: &[u8]) {
            self.0.separator(dst, a, b)
        }
        fn successor(&self, dst: &mut Vec<u8>, a: &[u8]) {
            self.0.successor(dst, a)
        }
    }

    let dir = temp_dir("cmp-mismatch");
    {
        let db = Db::open(&dir, Options::default()).unwrap(); // default comparer name
        db.set(b"k", b"v").unwrap();
    }
    // Reopening with a differently-named comparer must fail validation.
    let err = Db::open(
        &dir,
        Options {
            comparer: Arc::new(Renamed(DefaultComparer)),
            ..Default::default()
        },
    );
    assert!(err.is_err(), "open with mismatched comparer should fail");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn manual_compact_range_drains_upper_levels() {
    let dir = temp_dir("compact-range");
    let opts = Options {
        mem_table_size: 16 * 1024, // force multiple L0 files
        ..Default::default()
    };
    let db = Db::open(&dir, opts).unwrap();
    for i in 0..2000u32 {
        db.set(format!("k{i:05}").as_bytes(), format!("v{i}").as_bytes())
            .unwrap();
    }
    db.flush().unwrap();

    db.compact_range(None, None).unwrap();

    // After a full manual compaction L0 is empty and data has moved into deeper levels.
    let m = db.metrics();
    assert_eq!(
        m.level_files[0], 0,
        "L0 should be drained: {:?}",
        m.level_files
    );
    let deeper: usize = m.level_files[1..].iter().sum();
    assert!(deeper > 0, "data should live below L0: {:?}", m.level_files);

    // All data is still correct.
    assert_eq!(db.get(b"k00000").unwrap(), Some(b"v0".to_vec()));
    assert_eq!(db.get(b"k01999").unwrap(), Some(b"v1999".to_vec()));
    assert_eq!(collect(&db).len(), 2000);

    // A bounded range compaction is a no-op for correctness too.
    db.compact_range(Some(b"k00100"), Some(b"k00200")).unwrap();
    assert_eq!(db.get(b"k00150").unwrap(), Some(b"v150".to_vec()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn concurrent_writers_and_readers() {
    // Many writer threads share one Db (Arc), each owning a disjoint key range, while a
    // reader thread concurrently scans. The background flush/compaction worker runs the
    // whole time. Afterward every written key must be present exactly once.
    let dir = temp_dir("concurrent");
    let db = Arc::new(
        Db::open(
            &dir,
            Options {
                mem_table_size: 16 * 1024, // force background flushes during the run
                ..Default::default()
            },
        )
        .unwrap(),
    );

    let writers = 8;
    let per_writer = 1000u32;
    let mut handles = Vec::new();
    for w in 0..writers {
        let db = Arc::clone(&db);
        handles.push(std::thread::spawn(move || {
            for i in 0..per_writer {
                let k = format!("w{w:02}-k{i:05}");
                db.set(k.as_bytes(), format!("v{i}").as_bytes()).unwrap();
            }
        }));
    }
    // A concurrent reader: just exercises the read path under contention.
    let reader = {
        let db = Arc::clone(&db);
        std::thread::spawn(move || {
            for _ in 0..50 {
                let _ = db.get(b"w00-k00000").unwrap();
                let mut it = db.iter().unwrap();
                it.first().unwrap();
                let mut n = 0;
                while it.valid() {
                    n += 1;
                    it.next().unwrap();
                }
                let _ = n;
            }
        })
    };
    for h in handles {
        h.join().unwrap();
    }
    reader.join().unwrap();

    // Every key from every writer is present with the right value.
    for w in 0..writers {
        for i in [0u32, 1, 499, per_writer - 1] {
            let k = format!("w{w:02}-k{i:05}");
            assert_eq!(
                db.get(k.as_bytes()).unwrap(),
                Some(format!("v{i}").into_bytes()),
                "missing {k}"
            );
        }
    }
    assert_eq!(collect(&db).len(), (writers as usize) * per_writer as usize);

    let _ = std::fs::remove_dir_all(&dir);
}

/// A filesystem wrapper that injects write failures for files under a given directory
/// prefix once "tripped", used to exercise WAL failover. Reads always succeed.
mod fault {
    use std::io::{self, Write};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use pebbledb::vfs::{DirLock, Fs, MemFs, WritableFile};

    #[derive(Clone)]
    pub struct FaultFs {
        inner: MemFs,
        prefix: PathBuf,
        pub tripped: Arc<AtomicBool>,
    }

    impl FaultFs {
        pub fn new(prefix: impl Into<PathBuf>) -> FaultFs {
            FaultFs {
                inner: MemFs::new(),
                prefix: prefix.into(),
                tripped: Arc::new(AtomicBool::new(false)),
            }
        }
        fn faulting(&self, path: &Path) -> bool {
            path.starts_with(&self.prefix)
        }
    }

    struct FaultWritable {
        inner: Box<dyn WritableFile>,
        tripped: Arc<AtomicBool>,
        faulting: bool,
    }

    impl FaultWritable {
        fn check(&self) -> io::Result<()> {
            if self.faulting && self.tripped.load(Ordering::SeqCst) {
                Err(io::Error::other("injected WAL write failure"))
            } else {
                Ok(())
            }
        }
    }

    impl Write for FaultWritable {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.check()?;
            self.inner.write(buf)
        }
        fn flush(&mut self) -> io::Result<()> {
            self.check()?;
            self.inner.flush()
        }
    }

    impl WritableFile for FaultWritable {
        fn sync_all(&mut self) -> io::Result<()> {
            self.check()?;
            self.inner.sync_all()
        }
    }

    impl Fs for FaultFs {
        fn create(&self, path: &Path) -> io::Result<Box<dyn WritableFile>> {
            Ok(Box::new(FaultWritable {
                inner: self.inner.create(path)?,
                tripped: Arc::clone(&self.tripped),
                faulting: self.faulting(path),
            }))
        }
        fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
            self.inner.read(path)
        }
        fn remove(&self, path: &Path) -> io::Result<()> {
            self.inner.remove(path)
        }
        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            self.inner.rename(from, to)
        }
        fn list(&self, dir: &Path) -> io::Result<Vec<String>> {
            self.inner.list(dir)
        }
        fn create_dir_all(&self, path: &Path) -> io::Result<()> {
            self.inner.create_dir_all(path)
        }
        fn exists(&self, path: &Path) -> bool {
            self.inner.exists(path)
        }
        fn size(&self, path: &Path) -> io::Result<u64> {
            self.inner.size(path)
        }
        fn sync_dir(&self, dir: &Path) -> io::Result<()> {
            self.inner.sync_dir(dir)
        }
        fn lock(&self, path: &Path) -> io::Result<Box<dyn DirLock>> {
            self.inner.lock(path)
        }
    }
}

#[test]
fn wal_failover_to_secondary_directory() {
    use std::sync::atomic::Ordering;

    let fs = fault::FaultFs::new("/db/wal-primary");
    let opts = || Options {
        fs: Arc::new(fs.clone()),
        wal_dir: Some("/db/wal-primary".into()),
        wal_failover_dir: Some("/db/wal-secondary".into()),
        ..Default::default()
    };

    {
        let db = Db::open("/db/store", opts()).unwrap();
        // These writes go to the primary WAL.
        db.set(b"a", b"1").unwrap();
        db.set(b"b", b"2").unwrap();
        // Trip the primary WAL directory: subsequent WAL writes fail and the engine fails
        // over to the secondary directory, re-logging the batch there.
        fs.tripped.store(true, Ordering::SeqCst);
        db.set(b"c", b"3").unwrap();
        db.set(b"d", b"4").unwrap();
        // All four are readable in the live database.
        for (k, v) in [
            (&b"a"[..], &b"1"[..]),
            (b"b", b"2"),
            (b"c", b"3"),
            (b"d", b"4"),
        ] {
            assert_eq!(db.get(k).unwrap().as_deref(), Some(v));
        }
    }

    // Reopen (primary readable again): recovery scans both WAL directories and recovers
    // the pre-failover and post-failover batches alike.
    {
        fs.tripped.store(false, Ordering::SeqCst);
        let db = Db::open("/db/store", opts()).unwrap();
        for (k, v) in [
            (&b"a"[..], &b"1"[..]),
            (b"b", b"2"),
            (b"c", b"3"),
            (b"d", b"4"),
        ] {
            assert_eq!(db.get(k).unwrap().as_deref(), Some(v), "key after reopen");
        }
    }
}

#[test]
fn estimate_disk_usage_tracks_range() {
    let dir = temp_dir("disk-usage");
    let opts = Options {
        mem_table_size: 16 * 1024, // several sstables across the key space
        ..Default::default()
    };
    let db = Db::open(&dir, opts).unwrap();
    for i in 0..3000u32 {
        db.set(format!("k{i:05}").as_bytes(), &[b'v'; 64]).unwrap();
    }
    db.flush().unwrap();

    let full = db.estimate_disk_usage(b"k00000", b"k99999");
    assert!(full > 0, "full-range usage should be positive");

    // A sub-range estimates less than the whole, and an empty range is ~0.
    let part = db.estimate_disk_usage(b"k00000", b"k01000");
    assert!(
        part <= full,
        "sub-range {part} should not exceed full {full}"
    );
    let none = db.estimate_disk_usage(b"zzzz0", b"zzzz9");
    assert_eq!(none, 0, "disjoint range should estimate zero");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn logger_receives_messages_and_archive_cleaner_keeps_files() {
    use pebbledb::{ArchiveCleaner, Logger};
    use std::sync::Mutex;

    struct VecLogger(Mutex<Vec<String>>);
    impl Logger for VecLogger {
        fn info(&self, msg: &str) {
            self.0.lock().unwrap().push(msg.to_string());
        }
    }

    let dir = temp_dir("logger-archive");
    let archive = dir.join("archive");
    let logger = Arc::new(VecLogger(Mutex::new(Vec::new())));
    let opts = Options {
        mem_table_size: 16 * 1024,
        logger: Some(logger.clone()),
        cleaner: Arc::new(ArchiveCleaner {
            dir: archive.clone(),
        }),
        ..Default::default()
    };
    {
        let db = Db::open(&dir, opts).unwrap();
        for i in 0..2000u32 {
            db.set(format!("k{i:05}").as_bytes(), &[b'v'; 32]).unwrap();
        }
        db.flush().unwrap();
        db.compact_range(None, None).unwrap(); // produce obsolete inputs to archive
    }

    // The logger saw flush messages.
    let msgs = logger.0.lock().unwrap();
    assert!(
        msgs.iter().any(|m| m.contains("flushed memtable")),
        "expected flush log messages, got {msgs:?}"
    );

    // The archive cleaner moved obsolete files there instead of deleting them.
    let archived = std::fs::read_dir(&archive)
        .map(|rd| rd.count())
        .unwrap_or(0);
    assert!(archived > 0, "archive dir should hold obsolete files");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn indexed_batch_reads_its_own_writes() {
    let dir = temp_dir("indexed-batch");
    let db = Db::open(&dir, Options::default()).unwrap();
    db.set(b"a", b"committed-a").unwrap();
    db.set(b"b", b"committed-b").unwrap();
    db.set(b"old", b"present").unwrap();

    let mut ib = db.indexed_batch();
    ib.set(b"b", b"batch-b"); // overrides committed
    ib.set(b"c", b"batch-c"); // new key not in db
    ib.delete(b"a"); // hides committed
    ib.delete_range(b"old", b"oldz"); // range-deletes a committed key

    // Reads through the batch see its pending writes merged over the DB.
    assert_eq!(ib.get(&db, b"a").unwrap(), None); // deleted in batch
    assert_eq!(ib.get(&db, b"b").unwrap(), Some(b"batch-b".to_vec())); // overridden
    assert_eq!(ib.get(&db, b"c").unwrap(), Some(b"batch-c".to_vec())); // batch-only
    assert_eq!(ib.get(&db, b"old").unwrap(), None); // range-deleted
    // A committed key untouched by the batch still reads through.
    db.set(b"d", b"committed-d").unwrap();
    assert_eq!(ib.get(&db, b"d").unwrap(), Some(b"committed-d".to_vec()));

    // The merged scan reflects the same view, in sorted order.
    let scan = ib.scan(&db).unwrap();
    let got: Vec<(String, String)> = scan
        .iter()
        .map(|(k, v)| {
            (
                String::from_utf8_lossy(k).into_owned(),
                String::from_utf8_lossy(v).into_owned(),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            ("b".into(), "batch-b".into()),
            ("c".into(), "batch-c".into()),
            ("d".into(), "committed-d".into()),
        ]
    );

    // The DB itself is unchanged until the batch is committed.
    assert_eq!(db.get(b"a").unwrap(), Some(b"committed-a".to_vec()));
    db.write(ib.into_batch()).unwrap();
    assert_eq!(db.get(b"a").unwrap(), None);
    assert_eq!(db.get(b"b").unwrap(), Some(b"batch-b".to_vec()));
    assert_eq!(db.get(b"c").unwrap(), Some(b"batch-c".to_vec()));
    assert_eq!(db.get(b"old").unwrap(), None);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn indexed_batch_merges_over_committed_value() {
    use pebbledb::ConcatMerger;
    let dir = temp_dir("indexed-merge");
    let opts = Options {
        merger: Some(Arc::new(ConcatMerger)),
        ..Default::default()
    };
    let db = Db::open(&dir, opts).unwrap();
    db.set(b"k", b"base").unwrap();

    let mut ib = db.indexed_batch();
    ib.merge(b"k", b"+1");
    ib.merge(b"k", b"+2");
    // ConcatMerger appends operands to the existing value.
    assert_eq!(ib.get(&db, b"k").unwrap(), Some(b"base+1+2".to_vec()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn external_iter_merges_sstables_without_ingesting() {
    use pebbledb::base::internal_key::{InternalKey, InternalKeyKind};
    use pebbledb::new_external_iter;
    use pebbledb::sstable::{Writer, WriterOptions};

    let dir = temp_dir("external-iter");
    std::fs::create_dir_all(&dir).unwrap();
    let cmp = Arc::new(pebbledb::DefaultComparer);

    // Two sstables with interleaved and overlapping keys; the second (later in the list)
    // is treated as newer, so its value for "b" wins.
    let make = |path: &std::path::Path, kvs: &[(&str, &str)], seq: u64| {
        let f = std::fs::File::create(path).unwrap();
        let mut w = Writer::new(f, cmp.clone(), WriterOptions::default());
        for (k, v) in kvs {
            let ik = InternalKey::new(k.as_bytes().to_vec(), seq, InternalKeyKind::Set).encode();
            w.add(&ik, v.as_bytes()).unwrap();
        }
        w.finish().unwrap();
    };
    let f1 = dir.join("a.sst");
    let f2 = dir.join("b.sst");
    make(&f1, &[("a", "1"), ("b", "old"), ("d", "4")], 10);
    make(&f2, &[("b", "new"), ("c", "3")], 20);

    let opts = Options::default();
    let mut it = new_external_iter(&opts, &[f1, f2]).unwrap();
    it.first().unwrap();
    let mut got = Vec::new();
    while it.valid() {
        got.push((
            String::from_utf8_lossy(it.key()).into_owned(),
            String::from_utf8_lossy(it.value()).into_owned(),
        ));
        it.next().unwrap();
    }
    assert_eq!(
        got,
        vec![
            ("a".into(), "1".into()),
            ("b".into(), "new".into()), // newer file wins
            ("c".into(), "3".into()),
            ("d".into(), "4".into()),
        ]
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn single_delete_delete_sized_and_log_data() {
    let dir = temp_dir("del-variants");
    let db = Db::open(&dir, Options::default()).unwrap();
    db.set(b"s", b"v").unwrap();
    db.set(b"z", b"zz").unwrap();

    // single_delete and delete_sized both read back as absent.
    db.single_delete(b"s").unwrap();
    assert_eq!(db.get(b"s").unwrap(), None);
    db.delete_sized(b"z", 2).unwrap();
    assert_eq!(db.get(b"z").unwrap(), None);

    // log_data is a WAL-only marker; it does not change the keyspace.
    db.set(b"keep", b"1").unwrap();
    db.log_data(b"app-marker").unwrap();
    assert_eq!(db.get(b"keep").unwrap(), Some(b"1".to_vec()));

    // All of it survives flush + reopen.
    db.flush().unwrap();
    drop(db);
    let db = Db::open(&dir, Options::default()).unwrap();
    assert_eq!(db.get(b"s").unwrap(), None);
    assert_eq!(db.get(b"z").unwrap(), None);
    assert_eq!(db.get(b"keep").unwrap(), Some(b"1".to_vec()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn read_only_open_after_writes() {
    let dir = temp_dir("readonly");
    {
        let db = Db::open(&dir, Options::default()).unwrap();
        for i in 0..200u32 {
            db.set(format!("k{i:04}").as_bytes(), format!("v{i}").as_bytes())
                .unwrap();
        }
        db.flush().unwrap();
    }
    let db = Db::open_read_only(&dir, Options::default()).unwrap();
    assert_eq!(db.get(b"k0000").unwrap(), Some(b"v0".to_vec()));
    assert_eq!(db.get(b"k0199").unwrap(), Some(b"v199".to_vec()));
    assert!(db.set(b"x", b"y").is_err());

    let _ = std::fs::remove_dir_all(&dir);
}
