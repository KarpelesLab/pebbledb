// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Modeled on Pebble's objstorage / objstorage/remote packages.

//! Object storage: the abstraction that lets sstables live either on the local filesystem
//! or in shared/remote ("disaggregated") storage.
//!
//! [`Provider`] manages objects identified by a file number. Each object is either
//! **local** (stored on the [`crate::vfs::Fs`], the default) or **shared** (stored in a
//! pluggable [`RemoteStorage`] backend). A small in-memory catalog records which objects
//! are shared so reads are routed to the right place.
//!
//! This is the Pebble-side mechanism. A concrete cloud backend (S3, GCS, Azure, …) is
//! application code that implements [`RemoteStorage`]; an [`InMemoryRemote`] backend is
//! provided for tests and embedding.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::vfs::{Fs, WritableFile};

pub mod remoteobjcat;

/// A pluggable backend for shared/remote objects, addressed by an opaque name.
pub trait RemoteStorage: Send + Sync {
    /// Stores `data` under `name`, replacing any existing object.
    fn put(&self, name: &str, data: &[u8]) -> io::Result<()>;
    /// Reads the object stored under `name`.
    fn get(&self, name: &str) -> io::Result<Vec<u8>>;
    /// Removes the object stored under `name`.
    fn delete(&self, name: &str) -> io::Result<()>;
    /// Whether an object is stored under `name`.
    fn exists(&self, name: &str) -> bool;
    /// Lists the names of all stored objects.
    fn list(&self) -> io::Result<Vec<String>>;
}

/// An in-memory [`RemoteStorage`] backend for tests and embedding. Cloning shares storage.
#[derive(Clone, Default)]
pub struct InMemoryRemote {
    objects: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl InMemoryRemote {
    /// Creates an empty in-memory remote store.
    pub fn new() -> InMemoryRemote {
        InMemoryRemote::default()
    }
}

impl RemoteStorage for InMemoryRemote {
    fn put(&self, name: &str, data: &[u8]) -> io::Result<()> {
        self.objects
            .lock()
            .unwrap()
            .insert(name.to_string(), data.to_vec());
        Ok(())
    }
    fn get(&self, name: &str) -> io::Result<Vec<u8>> {
        self.objects
            .lock()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("remote: {name}")))
    }
    fn delete(&self, name: &str) -> io::Result<()> {
        self.objects.lock().unwrap().remove(name);
        Ok(())
    }
    fn exists(&self, name: &str) -> bool {
        self.objects.lock().unwrap().contains_key(name)
    }
    fn list(&self) -> io::Result<Vec<String>> {
        Ok(self.objects.lock().unwrap().keys().cloned().collect())
    }
}

/// Where an object lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Locator {
    /// On the local filesystem.
    Local,
    /// In the shared/remote backend.
    Shared,
}

/// Manages sstable-sized objects across local and shared storage, addressed by file number.
pub struct Provider {
    fs: Arc<dyn Fs>,
    dir: PathBuf,
    remote: Option<Arc<dyn RemoteStorage>>,
    /// File numbers known to be shared (the rest are local).
    catalog: Mutex<HashMap<u64, Locator>>,
}

impl Provider {
    /// A provider backed only by the local filesystem at `dir`.
    pub fn local(fs: Arc<dyn Fs>, dir: impl Into<PathBuf>) -> Provider {
        Provider {
            fs,
            dir: dir.into(),
            remote: None,
            catalog: Mutex::new(HashMap::new()),
        }
    }

    /// A provider that can also place objects in `remote` shared storage.
    pub fn with_remote(
        fs: Arc<dyn Fs>,
        dir: impl Into<PathBuf>,
        remote: Arc<dyn RemoteStorage>,
    ) -> Provider {
        Provider {
            fs,
            dir: dir.into(),
            remote: Some(remote),
            catalog: Mutex::new(HashMap::new()),
        }
    }

    fn object_name(num: u64) -> String {
        format!("{num:06}.sst")
    }

    /// Creates a local object for writing, returning a writable handle.
    pub fn create_local(&self, num: u64) -> io::Result<Box<dyn WritableFile>> {
        self.catalog.lock().unwrap().insert(num, Locator::Local);
        self.fs.create(&self.dir.join(Self::object_name(num)))
    }

    /// Writes `data` as a shared object in remote storage.
    pub fn put_shared(&self, num: u64, data: &[u8]) -> io::Result<()> {
        let remote = self
            .remote
            .as_ref()
            .ok_or_else(|| io::Error::other("objstorage: no remote backend configured"))?;
        remote.put(&Self::object_name(num), data)?;
        self.catalog.lock().unwrap().insert(num, Locator::Shared);
        Ok(())
    }

    /// Where object `num` lives, or `None` if unknown to this provider.
    pub fn locator(&self, num: u64) -> Option<Locator> {
        self.catalog.lock().unwrap().get(&num).copied()
    }

    /// Reads object `num` from wherever it lives (local filesystem or shared storage).
    pub fn read(&self, num: u64) -> io::Result<Vec<u8>> {
        match self.locator(num) {
            Some(Locator::Shared) => self
                .remote
                .as_ref()
                .ok_or_else(|| io::Error::other("objstorage: no remote backend configured"))?
                .get(&Self::object_name(num)),
            // Default to local (covers Local and unknown-but-on-disk objects).
            _ => self.fs.read(&self.dir.join(Self::object_name(num))),
        }
    }

    /// Removes object `num` from wherever it lives.
    pub fn remove(&self, num: u64) -> io::Result<()> {
        let loc = self.catalog.lock().unwrap().remove(&num);
        match loc {
            Some(Locator::Shared) => {
                if let Some(r) = &self.remote {
                    r.delete(&Self::object_name(num))?;
                }
                Ok(())
            }
            _ => self.fs.remove(&self.dir.join(Self::object_name(num))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vfs::MemFs;
    use std::io::Write;

    #[test]
    fn local_and_shared_objects_round_trip() {
        let fs: Arc<dyn Fs> = Arc::new(MemFs::new());
        fs.create_dir_all(std::path::Path::new("/db")).unwrap();
        let remote = Arc::new(InMemoryRemote::new());
        let p = Provider::with_remote(fs, "/db", remote.clone());

        // A local object.
        {
            let mut w = p.create_local(1).unwrap();
            w.write_all(b"local-data").unwrap();
            w.sync_all().unwrap();
        }
        assert_eq!(p.locator(1), Some(Locator::Local));
        assert_eq!(p.read(1).unwrap(), b"local-data");

        // A shared object goes to the remote backend.
        p.put_shared(2, b"shared-data").unwrap();
        assert_eq!(p.locator(2), Some(Locator::Shared));
        assert_eq!(p.read(2).unwrap(), b"shared-data");
        assert!(remote.exists("000002.sst"));

        // Removal routes to the right place.
        p.remove(2).unwrap();
        assert!(!remote.exists("000002.sst"));
        p.remove(1).unwrap();
        assert!(p.read(1).is_err());
    }

    #[test]
    fn put_shared_without_remote_errors() {
        let fs: Arc<dyn Fs> = Arc::new(MemFs::new());
        fs.create_dir_all(std::path::Path::new("/db")).unwrap();
        let p = Provider::local(fs, "/db");
        assert!(p.put_shared(1, b"x").is_err());
    }
}
