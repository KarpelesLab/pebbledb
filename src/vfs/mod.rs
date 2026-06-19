// Copyright (c) 2012 The LevelDB-Go Authors. All rights reserved.
// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Modeled on Pebble's `vfs` package (the FS / File abstraction, DiskFS, and MemFS).

//! A filesystem abstraction so the database can run on the real disk or fully in memory.
//!
//! [`Fs`] is the trait the engine uses for every file operation: creating and reading
//! files, listing and syncing directories, renaming, removing, and acquiring the
//! exclusive directory lock. Two implementations are provided:
//!
//! * [`DiskFs`] — backed by [`std::fs`], with real `fsync` of files and directories and an
//!   OS-level advisory lock (`flock` on Unix, an exclusive lock file elsewhere).
//! * [`MemFs`] — an in-memory tree, used by tests to exercise the full open/flush/compact
//!   lifecycle without touching disk.
//!
//! Files opened for writing implement [`WritableFile`] (`Write` plus `sync_all`); the
//! record-log and sstable writers are generic over it.

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// A file opened for writing, with an explicit durability barrier.
pub trait WritableFile: Write + Send {
    /// Flushes and persists all written data (and the file's metadata) to stable storage.
    fn sync_all(&mut self) -> io::Result<()>;
}

impl WritableFile for std::fs::File {
    fn sync_all(&mut self) -> io::Result<()> {
        std::fs::File::sync_all(self)
    }
}

// `Box<dyn WritableFile>` gets `Write` from std's blanket `impl<W: Write + ?Sized> Write
// for Box<W>` (a `dyn WritableFile` is `Write` via its supertrait); we only forward the
// `WritableFile::sync_all` extension so the boxed form can back the record/sstable writers.
impl WritableFile for Box<dyn WritableFile> {
    fn sync_all(&mut self) -> io::Result<()> {
        (**self).sync_all()
    }
}

/// An acquired exclusive lock on a database directory. Dropping it releases the lock.
pub trait DirLock: Send + Sync {}

/// A filesystem the database performs all of its I/O through.
pub trait Fs: Send + Sync {
    /// Creates (or truncates) the file at `path` for writing.
    fn create(&self, path: &Path) -> io::Result<Box<dyn WritableFile>>;
    /// Reads the entire contents of `path`.
    fn read(&self, path: &Path) -> io::Result<Vec<u8>>;
    /// Removes the file at `path`.
    fn remove(&self, path: &Path) -> io::Result<()>;
    /// Renames `from` to `to`, replacing `to` if it exists.
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    /// Lists the (non-recursive) entry names directly under `dir`.
    fn list(&self, dir: &Path) -> io::Result<Vec<String>>;
    /// Creates `path` and any missing parents.
    fn create_dir_all(&self, path: &Path) -> io::Result<()>;
    /// Whether `path` exists.
    fn exists(&self, path: &Path) -> bool;
    /// The size in bytes of the file at `path`.
    fn size(&self, path: &Path) -> io::Result<u64>;
    /// Persists `dir`'s entries (so a rename/create within it survives a crash). A no-op
    /// on filesystems where it is meaningless.
    fn sync_dir(&self, dir: &Path) -> io::Result<()>;
    /// Acquires the exclusive lock identified by the lock file `path`, failing if another
    /// process (or `Fs` instance) already holds it.
    fn lock(&self, path: &Path) -> io::Result<Box<dyn DirLock>>;
}

// ---------------------------------------------------------------------------
// DiskFs
// ---------------------------------------------------------------------------

/// An [`Fs`] backed by the real filesystem via [`std::fs`].
#[derive(Debug, Default, Clone, Copy)]
pub struct DiskFs;

impl Fs for DiskFs {
    fn create(&self, path: &Path) -> io::Result<Box<dyn WritableFile>> {
        Ok(Box::new(std::fs::File::create(path)?))
    }
    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        std::fs::read(path)
    }
    fn remove(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }
    fn list(&self, dir: &Path) -> io::Result<Vec<String>> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            if let Ok(name) = entry?.file_name().into_string() {
                out.push(name);
            }
        }
        Ok(out)
    }
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        std::fs::create_dir_all(path)
    }
    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }
    fn size(&self, path: &Path) -> io::Result<u64> {
        Ok(std::fs::metadata(path)?.len())
    }
    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        // Opening a directory and fsyncing it persists its entries on Unix; elsewhere we
        // best-effort skip (the platform either does it implicitly or lacks the concept).
        #[cfg(unix)]
        {
            let f = std::fs::File::open(dir)?;
            f.sync_all()?;
        }
        #[cfg(not(unix))]
        {
            let _ = dir;
        }
        Ok(())
    }
    fn lock(&self, path: &Path) -> io::Result<Box<dyn DirLock>> {
        disk_lock::acquire(path)
    }
}

/// OS-level directory locking for [`DiskFs`].
mod disk_lock {
    use super::{DirLock, Path};
    use std::fs::{File, OpenOptions};
    use std::io;

    /// A held lock: the open file handle keeps the lock for its lifetime. Dropping the
    /// `File` closes the descriptor, which releases the `flock`.
    struct Lock {
        _file: File,
    }
    impl DirLock for Lock {}

    #[cfg(unix)]
    pub(super) fn acquire(path: &Path) -> io::Result<Box<dyn DirLock>> {
        use std::os::unix::io::AsRawFd;

        // flock(2): LOCK_EX (2) | LOCK_NB (4). Linked from libc, always present.
        unsafe extern "C" {
            fn flock(fd: i32, operation: i32) -> i32;
        }
        const LOCK_EX: i32 = 2;
        const LOCK_NB: i32 = 4;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        // SAFETY: `fd` is a valid descriptor owned by `file` for the duration of the call.
        let rc = unsafe { flock(file.as_raw_fd(), LOCK_EX | LOCK_NB) };
        if rc != 0 {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "vfs: database is locked by another process",
            ));
        }
        Ok(Box::new(Lock { _file: file }))
    }

    #[cfg(not(unix))]
    pub(super) fn acquire(path: &Path) -> io::Result<Box<dyn DirLock>> {
        // Without flock, create the lock file exclusively: its presence is the lock. The
        // file is removed when the process that created it exits cleanly.
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!("vfs: database is locked ({e})"),
                )
            })?;
        Ok(Box::new(Lock { _file: file }))
    }
}

// ---------------------------------------------------------------------------
// MemFs
// ---------------------------------------------------------------------------

/// A shared in-memory filesystem tree.
type Tree = Arc<Mutex<MemState>>;

#[derive(Default)]
struct MemState {
    files: HashMap<PathBuf, Vec<u8>>,
    locks: std::collections::HashSet<PathBuf>,
}

/// An [`Fs`] that stores all files in memory. Cloning shares the same underlying tree.
#[derive(Clone, Default)]
pub struct MemFs {
    tree: Tree,
}

impl MemFs {
    /// Creates an empty in-memory filesystem.
    pub fn new() -> MemFs {
        MemFs::default()
    }
}

/// A handle that appends to an in-memory file on each write.
struct MemWritable {
    tree: Tree,
    path: PathBuf,
}

impl Write for MemWritable {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut st = self.tree.lock().unwrap();
        st.files
            .entry(self.path.clone())
            .or_default()
            .extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl WritableFile for MemWritable {
    fn sync_all(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct MemLock {
    tree: Tree,
    path: PathBuf,
}

impl DirLock for MemLock {}

impl Drop for MemLock {
    fn drop(&mut self) {
        self.tree.lock().unwrap().locks.remove(&self.path);
    }
}

fn not_found(path: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::NotFound,
        format!("vfs: no such file: {}", path.display()),
    )
}

impl Fs for MemFs {
    fn create(&self, path: &Path) -> io::Result<Box<dyn WritableFile>> {
        let mut st = self.tree.lock().unwrap();
        st.files.insert(path.to_path_buf(), Vec::new());
        Ok(Box::new(MemWritable {
            tree: Arc::clone(&self.tree),
            path: path.to_path_buf(),
        }))
    }
    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        let st = self.tree.lock().unwrap();
        st.files.get(path).cloned().ok_or_else(|| not_found(path))
    }
    fn remove(&self, path: &Path) -> io::Result<()> {
        let mut st = self.tree.lock().unwrap();
        st.files
            .remove(path)
            .map(|_| ())
            .ok_or_else(|| not_found(path))
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let mut st = self.tree.lock().unwrap();
        let data = st.files.remove(from).ok_or_else(|| not_found(from))?;
        st.files.insert(to.to_path_buf(), data);
        Ok(())
    }
    fn list(&self, dir: &Path) -> io::Result<Vec<String>> {
        let st = self.tree.lock().unwrap();
        let mut out = Vec::new();
        for p in st.files.keys() {
            if p.parent() == Some(dir)
                && let Some(name) = p.file_name().and_then(|n| n.to_str())
            {
                out.push(name.to_string());
            }
        }
        Ok(out)
    }
    fn create_dir_all(&self, _path: &Path) -> io::Result<()> {
        Ok(()) // directories are implicit in the flat map
    }
    fn exists(&self, path: &Path) -> bool {
        self.tree.lock().unwrap().files.contains_key(path)
    }
    fn size(&self, path: &Path) -> io::Result<u64> {
        let st = self.tree.lock().unwrap();
        st.files
            .get(path)
            .map(|d| d.len() as u64)
            .ok_or_else(|| not_found(path))
    }
    fn sync_dir(&self, _dir: &Path) -> io::Result<()> {
        Ok(())
    }
    fn lock(&self, path: &Path) -> io::Result<Box<dyn DirLock>> {
        let mut st = self.tree.lock().unwrap();
        if !st.locks.insert(path.to_path_buf()) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "vfs: database is already locked",
            ));
        }
        Ok(Box::new(MemLock {
            tree: Arc::clone(&self.tree),
            path: path.to_path_buf(),
        }))
    }
}

/// Reads `path` in full through a generic reader handle (used where a `Read` is needed).
pub fn read_to_vec(mut r: impl Read) -> io::Result<Vec<u8>> {
    let mut v = Vec::new();
    r.read_to_end(&mut v)?;
    Ok(v)
}

// ---------------------------------------------------------------------------
// DiskHealthCheckingFs
// ---------------------------------------------------------------------------

/// Details of a filesystem operation that exceeded the health-check threshold.
#[derive(Debug, Clone)]
pub struct DiskSlowInfo {
    /// The operation name (e.g. `"create"`, `"write"`, `"sync"`).
    pub op: &'static str,
    /// The file involved.
    pub path: PathBuf,
    /// How long the operation took.
    pub duration: std::time::Duration,
}

/// Callback invoked when an operation is slow.
type SlowCallback = Arc<dyn Fn(DiskSlowInfo) + Send + Sync>;

/// An [`Fs`] wrapper that times each operation and reports those exceeding a threshold via
/// a callback — Pebble's disk-health checking (the source of its `DiskSlow` event). Wrap
/// any inner filesystem; reads and writes are timed, including a writable file's
/// `write`/`sync_all`.
#[derive(Clone)]
pub struct DiskHealthCheckingFs {
    inner: Arc<dyn Fs>,
    threshold: std::time::Duration,
    on_slow: SlowCallback,
}

impl DiskHealthCheckingFs {
    /// Wraps `inner`, invoking `on_slow` whenever an operation takes at least `threshold`.
    pub fn new(
        inner: Arc<dyn Fs>,
        threshold: std::time::Duration,
        on_slow: SlowCallback,
    ) -> DiskHealthCheckingFs {
        DiskHealthCheckingFs {
            inner,
            threshold,
            on_slow,
        }
    }

    fn timed<T>(&self, op: &'static str, path: &Path, f: impl FnOnce() -> T) -> T {
        let start = std::time::Instant::now();
        let r = f();
        let elapsed = start.elapsed();
        if elapsed >= self.threshold {
            (self.on_slow)(DiskSlowInfo {
                op,
                path: path.to_path_buf(),
                duration: elapsed,
            });
        }
        r
    }
}

/// A writable file whose `write`/`sync_all` are timed by [`DiskHealthCheckingFs`].
struct HealthCheckedWritable {
    inner: Box<dyn WritableFile>,
    path: PathBuf,
    threshold: std::time::Duration,
    on_slow: SlowCallback,
}

impl Write for HealthCheckedWritable {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let start = std::time::Instant::now();
        let r = self.inner.write(buf);
        let elapsed = start.elapsed();
        if elapsed >= self.threshold {
            (self.on_slow)(DiskSlowInfo {
                op: "write",
                path: self.path.clone(),
                duration: elapsed,
            });
        }
        r
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl WritableFile for HealthCheckedWritable {
    fn sync_all(&mut self) -> io::Result<()> {
        let start = std::time::Instant::now();
        let r = self.inner.sync_all();
        let elapsed = start.elapsed();
        if elapsed >= self.threshold {
            (self.on_slow)(DiskSlowInfo {
                op: "sync",
                path: self.path.clone(),
                duration: elapsed,
            });
        }
        r
    }
}

impl Fs for DiskHealthCheckingFs {
    fn create(&self, path: &Path) -> io::Result<Box<dyn WritableFile>> {
        let inner = self.timed("create", path, || self.inner.create(path))?;
        Ok(Box::new(HealthCheckedWritable {
            inner,
            path: path.to_path_buf(),
            threshold: self.threshold,
            on_slow: Arc::clone(&self.on_slow),
        }))
    }
    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        self.timed("read", path, || self.inner.read(path))
    }
    fn remove(&self, path: &Path) -> io::Result<()> {
        self.timed("remove", path, || self.inner.remove(path))
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.timed("rename", from, || self.inner.rename(from, to))
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
        self.timed("sync_dir", dir, || self.inner.sync_dir(dir))
    }
    fn lock(&self, path: &Path) -> io::Result<Box<dyn DirLock>> {
        self.inner.lock(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memfs_roundtrip() {
        let fs = MemFs::new();
        let dir = Path::new("/db");
        {
            let mut f = fs.create(&dir.join("a.txt")).unwrap();
            f.write_all(b"hello ").unwrap();
            f.write_all(b"world").unwrap();
            f.sync_all().unwrap();
        }
        assert!(fs.exists(&dir.join("a.txt")));
        assert_eq!(fs.read(&dir.join("a.txt")).unwrap(), b"hello world");
        assert_eq!(fs.size(&dir.join("a.txt")).unwrap(), 11);

        fs.create(&dir.join("b.txt")).unwrap();
        let mut names = fs.list(dir).unwrap();
        names.sort();
        assert_eq!(names, ["a.txt", "b.txt"]);

        fs.rename(&dir.join("a.txt"), &dir.join("c.txt")).unwrap();
        assert!(!fs.exists(&dir.join("a.txt")));
        assert_eq!(fs.read(&dir.join("c.txt")).unwrap(), b"hello world");

        fs.remove(&dir.join("b.txt")).unwrap();
        assert!(!fs.exists(&dir.join("b.txt")));
    }

    #[test]
    fn memfs_lock_is_exclusive() {
        let fs = MemFs::new();
        let p = Path::new("/db/LOCK");
        let lock = fs.lock(p).unwrap();
        assert!(fs.lock(p).is_err(), "second lock must fail");
        drop(lock);
        assert!(fs.lock(p).is_ok(), "lock released on drop");
    }

    #[test]
    fn disk_health_check_reports_slow_writes() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        // An inner FS whose writable files sleep on write/sync, simulating a slow disk.
        struct SlowFs(MemFs);
        struct SlowWritable(Box<dyn WritableFile>);
        impl Write for SlowWritable {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                std::thread::sleep(Duration::from_millis(5));
                self.0.write(buf)
            }
            fn flush(&mut self) -> io::Result<()> {
                self.0.flush()
            }
        }
        impl WritableFile for SlowWritable {
            fn sync_all(&mut self) -> io::Result<()> {
                std::thread::sleep(Duration::from_millis(5));
                self.0.sync_all()
            }
        }
        impl Fs for SlowFs {
            fn create(&self, p: &Path) -> io::Result<Box<dyn WritableFile>> {
                Ok(Box::new(SlowWritable(self.0.create(p)?)))
            }
            fn read(&self, p: &Path) -> io::Result<Vec<u8>> {
                self.0.read(p)
            }
            fn remove(&self, p: &Path) -> io::Result<()> {
                self.0.remove(p)
            }
            fn rename(&self, a: &Path, b: &Path) -> io::Result<()> {
                self.0.rename(a, b)
            }
            fn list(&self, d: &Path) -> io::Result<Vec<String>> {
                self.0.list(d)
            }
            fn create_dir_all(&self, p: &Path) -> io::Result<()> {
                self.0.create_dir_all(p)
            }
            fn exists(&self, p: &Path) -> bool {
                self.0.exists(p)
            }
            fn size(&self, p: &Path) -> io::Result<u64> {
                self.0.size(p)
            }
            fn sync_dir(&self, d: &Path) -> io::Result<()> {
                self.0.sync_dir(d)
            }
            fn lock(&self, p: &Path) -> io::Result<Box<dyn DirLock>> {
                self.0.lock(p)
            }
        }

        let slow_count = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&slow_count);
        let fs = DiskHealthCheckingFs::new(
            Arc::new(SlowFs(MemFs::new())),
            Duration::from_millis(1),
            Arc::new(move |info: DiskSlowInfo| {
                assert!(info.op == "write" || info.op == "sync");
                c.fetch_add(1, Ordering::SeqCst);
            }),
        );
        let mut f = fs.create(Path::new("/db/x")).unwrap();
        f.write_all(b"data").unwrap();
        f.sync_all().unwrap();
        assert!(
            slow_count.load(Ordering::SeqCst) >= 2,
            "expected slow write + sync reports"
        );
    }

    #[test]
    fn diskfs_lock_is_exclusive() {
        let dir = std::env::temp_dir().join(format!("pebbledb-vfs-lock-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("LOCK");
        let fs = DiskFs;
        let lock = fs.lock(&p).unwrap();
        assert!(fs.lock(&p).is_err(), "second lock must fail");
        drop(lock);
        assert!(fs.lock(&p).is_ok(), "lock released on drop");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
