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
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::vfs::{Fs, WritableFile};

pub mod remoteobjcat;

use remoteobjcat::{CatalogContents, ObjType, RemoteObjectMetadata, read_catalog, write_catalog};

/// Marker type naming the current remote-object catalog file (Pebble's `remote-obj-catalog`).
const CATALOG_MARKER_PREFIX: &str = "marker.remote-obj-catalog.";

/// Filename of the catalog record-log file numbered `num` (`REMOTE-OBJ-CATALOG-<num:06>`).
fn catalog_filename(num: u64) -> String {
    format!("REMOTE-OBJ-CATALOG-{num:06}")
}

/// Returns the highest-iteration remote-obj-catalog marker in `dir` as `(iteration, value)`,
/// where `value` is the catalog filename the marker points at.
fn read_catalog_marker(fs: &dyn Fs, dir: &Path) -> io::Result<Option<(u64, String)>> {
    let mut best: Option<(u64, String)> = None;
    for name in fs.list(dir)? {
        let Some(rest) = name.strip_prefix(CATALOG_MARKER_PREFIX) else {
            continue;
        };
        if let Some((iter_str, value)) = rest.split_once('.')
            && let Ok(iter) = iter_str.parse::<u64>()
            && best.as_ref().is_none_or(|(b, _)| iter > *b)
        {
            best = Some((iter, value.to_string()));
        }
    }
    Ok(best)
}

/// Atomically repoints the remote-obj-catalog marker at `value` with iteration `iter`: a new
/// marker file is created (and synced) before the superseded markers are removed, so a crash at
/// any point leaves the highest-iteration marker pointing at a complete catalog.
fn update_catalog_marker(fs: &dyn Fs, dir: &Path, iter: u64, value: &str) -> io::Result<()> {
    let new_name = format!("{CATALOG_MARKER_PREFIX}{iter:06}.{value}");
    fs.create(&dir.join(&new_name))?.sync_all()?;
    for name in fs.list(dir)? {
        if name != new_name && name.starts_with(CATALOG_MARKER_PREFIX) {
            let _ = fs.remove(&dir.join(name));
        }
    }
    fs.sync_dir(dir)?;
    Ok(())
}

/// The persistent shared-object catalog state, maintained only when a remote backend is set.
#[derive(Default)]
struct CatalogPersist {
    /// Creator ID + the live remote objects (mirrors what is written to the catalog file).
    contents: CatalogContents,
    /// File number of the current on-disk catalog file (`0` = none written yet).
    num: u64,
}

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
    /// Persistent shared-object catalog (a `REMOTE-OBJ-CATALOG` record log located by an atomic
    /// marker), so the set of shared objects survives reopen and is byte-compatible with Pebble.
    persist: Mutex<CatalogPersist>,
}

impl Provider {
    /// A provider backed only by the local filesystem at `dir`.
    pub fn local(fs: Arc<dyn Fs>, dir: impl Into<PathBuf>) -> Provider {
        Provider {
            fs,
            dir: dir.into(),
            remote: None,
            catalog: Mutex::new(HashMap::new()),
            persist: Mutex::new(CatalogPersist::default()),
        }
    }

    /// A provider that can also place objects in `remote` shared storage. Any persisted
    /// `REMOTE-OBJ-CATALOG` in `dir` is replayed so previously-shared objects are known.
    pub fn with_remote(
        fs: Arc<dyn Fs>,
        dir: impl Into<PathBuf>,
        remote: Arc<dyn RemoteStorage>,
    ) -> io::Result<Provider> {
        let dir = dir.into();
        let mut catalog = HashMap::new();
        let mut persist = CatalogPersist::default();
        if let Some((iter, name)) = read_catalog_marker(fs.as_ref(), &dir)? {
            let bytes = fs.read(&dir.join(&name))?;
            let contents = read_catalog(&bytes).map_err(io::Error::other)?;
            for &num in contents.objects.keys() {
                catalog.insert(num, Locator::Shared);
            }
            persist.contents = contents;
            persist.num = iter;
        }
        Ok(Provider {
            fs,
            dir,
            remote: Some(remote),
            catalog: Mutex::new(catalog),
            persist: Mutex::new(persist),
        })
    }

    fn object_name(num: u64) -> String {
        format!("{num:06}.sst")
    }

    /// Rewrites the catalog file fresh (one snapshot edit) under a new number and atomically
    /// repoints the marker at it, then removes the superseded catalog file. Called while holding
    /// the `persist` guard so the on-disk catalog always reflects the in-memory state.
    fn write_catalog_locked(&self, p: &mut CatalogPersist) -> io::Result<()> {
        let next = p.num + 1;
        let bytes = write_catalog(&p.contents).map_err(io::Error::other)?;
        let name = catalog_filename(next);
        {
            let mut f = self.fs.create(&self.dir.join(&name))?;
            f.write_all(&bytes)?;
            f.sync_all()?;
        }
        update_catalog_marker(self.fs.as_ref(), &self.dir, next, &name)?;
        if p.num != 0 {
            let _ = self.fs.remove(&self.dir.join(catalog_filename(p.num)));
        }
        p.num = next;
        Ok(())
    }

    /// Creates a local object for writing, returning a writable handle.
    pub fn create_local(&self, num: u64) -> io::Result<Box<dyn WritableFile>> {
        self.catalog.lock().unwrap().insert(num, Locator::Local);
        self.fs.create(&self.dir.join(Self::object_name(num)))
    }

    /// Writes `data` as a shared object in remote storage and records it in the persistent catalog.
    pub fn put_shared(&self, num: u64, data: &[u8]) -> io::Result<()> {
        let remote = self
            .remote
            .as_ref()
            .ok_or_else(|| io::Error::other("objstorage: no remote backend configured"))?;
        remote.put(&Self::object_name(num), data)?;
        self.catalog.lock().unwrap().insert(num, Locator::Shared);
        let mut p = self.persist.lock().unwrap();
        // Assign this store's creator ID on its first shared object (Pebble does the same).
        let creator_id = *p.contents.creator_id.get_or_insert(1);
        p.contents.objects.insert(
            num,
            RemoteObjectMetadata {
                file_num: num,
                obj_type: ObjType::Table,
                creator_id,
                creator_file_num: num,
                cleanup_method: 0,
                locator: String::new(),
                custom_object_name: String::new(),
            },
        );
        self.write_catalog_locked(&mut p)
    }

    /// This store's creator ID, assigned on its first shared write (`None` if it has none).
    pub fn creator_id(&self) -> Option<u64> {
        self.persist.lock().unwrap().contents.creator_id
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

    /// Removes object `num` from wherever it lives, updating the catalog if it was shared.
    pub fn remove(&self, num: u64) -> io::Result<()> {
        let loc = self.catalog.lock().unwrap().remove(&num);
        match loc {
            Some(Locator::Shared) => {
                if let Some(r) = &self.remote {
                    r.delete(&Self::object_name(num))?;
                }
                let mut p = self.persist.lock().unwrap();
                if p.contents.objects.remove(&num).is_some() {
                    self.write_catalog_locked(&mut p)?;
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
        let p = Provider::with_remote(fs, "/db", remote.clone()).unwrap();

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

    #[test]
    fn catalog_persists_shared_objects_across_reopen() {
        let fs: Arc<dyn Fs> = Arc::new(MemFs::new());
        fs.create_dir_all(std::path::Path::new("/db")).unwrap();
        let remote = Arc::new(InMemoryRemote::new());

        // First session: share two objects, remove one. The catalog + marker land on the local fs.
        {
            let p = Provider::with_remote(fs.clone(), "/db", remote.clone()).unwrap();
            p.put_shared(7, b"seven").unwrap();
            p.put_shared(8, b"eight").unwrap();
            p.remove(7).unwrap();
            assert_eq!(p.creator_id(), Some(1));
        }
        // A marker and exactly one catalog file exist (superseded ones removed).
        let names = fs.list(std::path::Path::new("/db")).unwrap();
        assert_eq!(
            names
                .iter()
                .filter(|n| n.starts_with("marker.remote-obj-catalog."))
                .count(),
            1,
            "exactly one catalog marker"
        );
        assert_eq!(
            names
                .iter()
                .filter(|n| n.starts_with("REMOTE-OBJ-CATALOG-"))
                .count(),
            1,
            "exactly one catalog file"
        );

        // Second session: the persisted catalog is replayed — object 8 is known shared, 7 is gone,
        // the creator ID survived, and the shared value still reads back.
        let p = Provider::with_remote(fs, "/db", remote.clone()).unwrap();
        assert_eq!(p.locator(8), Some(Locator::Shared));
        assert_eq!(p.locator(7), None);
        assert_eq!(p.creator_id(), Some(1));
        assert_eq!(p.read(8).unwrap(), b"eight");
    }

    #[test]
    fn rewritten_catalog_replays_to_current_state() {
        // The catalog bytes a Provider writes must replay (via the format module) to exactly the
        // live shared set — the byte-level contract Pebble's reader relies on.
        let fs: Arc<dyn Fs> = Arc::new(MemFs::new());
        fs.create_dir_all(std::path::Path::new("/db")).unwrap();
        let remote = Arc::new(InMemoryRemote::new());
        let p = Provider::with_remote(fs.clone(), "/db", remote).unwrap();
        p.put_shared(3, b"three").unwrap();
        p.put_shared(4, b"four").unwrap();

        let (_, name) = read_catalog_marker(fs.as_ref(), std::path::Path::new("/db"))
            .unwrap()
            .expect("a catalog marker");
        let bytes = fs.read(&std::path::Path::new("/db").join(name)).unwrap();
        let contents = read_catalog(&bytes).unwrap();
        assert_eq!(contents.creator_id, Some(1));
        assert_eq!(
            contents.objects.keys().copied().collect::<Vec<_>>(),
            vec![3, 4]
        );
        assert_eq!(contents.objects[&3].obj_type, ObjType::Table);
    }
}
