// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

//! Pebble's shared-object (remote) catalog format (`REMOTE-OBJ-CATALOG`).
//!
//! When a database stores objects on shared/remote storage, Pebble records, in a catalog file,
//! which `(file number, type)` objects live remotely and how to locate them (the creating store's
//! ID and file number, an optional locator naming the backend, and an optional custom object
//! name). The catalog is a [record log](crate::record) of [`CatalogVersionEdit`]s — the same
//! framing as the WAL and MANIFEST — located via an atomic marker named `remote-obj-catalog`
//! pointing at `REMOTE-OBJ-CATALOG-<iter:06>`.
//!
//! This module implements the on-disk **format** (edit encode/decode + replaying a catalog file
//! into the current object set), matching Pebble byte-for-byte. [`Provider`](super::Provider) uses
//! it to persist its shared-object set: each shared put/remove rewrites the catalog file and
//! atomically repoints the `marker.remote-obj-catalog.*` marker, and a provider replays the marked
//! catalog on open.
//!
//! Edit encoding: a sequence of tagged records.
//! - `tagNewObject(1)`: `uvarint(file_num) uvarint(obj_type) uvarint(creator_id)
//!   uvarint(creator_file_num) uvarint(cleanup_method)` then optional sub-tags
//!   (`tagLocator(4) string` and/or `tagCustomName(5) string`) terminated by a `0`.
//! - `tagDeletedObject(2)`: `uvarint(file_num)`.
//! - `tagCreatorID(3)`: `uvarint(creator_id)`.
//!
//! A string is `uvarint(len)` followed by the bytes.

use std::collections::BTreeMap;
use std::io::Cursor;

use crate::base::varint::{get_uvarint, put_uvarint};
use crate::record;
use crate::{Error, Result};

const TAG_NEW_OBJECT: u64 = 1;
const TAG_DELETED_OBJECT: u64 = 2;
const TAG_CREATOR_ID: u64 = 3;
const TAG_NEW_OBJECT_LOCATOR: u64 = 4;
const TAG_NEW_OBJECT_CUSTOM_NAME: u64 = 5;

const OBJ_TYPE_TABLE: u64 = 1;
const OBJ_TYPE_BLOB: u64 = 2;

/// The on-disk object type of a cataloged remote object.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObjType {
    /// An sstable (`.sst`).
    Table,
    /// A blob file (`.blob`).
    Blob,
}

impl ObjType {
    fn to_u64(self) -> u64 {
        match self {
            ObjType::Table => OBJ_TYPE_TABLE,
            ObjType::Blob => OBJ_TYPE_BLOB,
        }
    }

    fn from_u64(v: u64) -> Result<ObjType> {
        match v {
            OBJ_TYPE_TABLE => Ok(ObjType::Table),
            OBJ_TYPE_BLOB => Ok(ObjType::Blob),
            other => Err(Error::Corruption(format!(
                "remote-obj-catalog: unknown object type {other}"
            ))),
        }
    }
}

/// Metadata for one cataloged remote object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteObjectMetadata {
    /// The object's file number within this store.
    pub file_num: u64,
    /// Whether the object is an sstable or a blob file.
    pub obj_type: ObjType,
    /// The ID of the store that created the object.
    pub creator_id: u64,
    /// The object's file number within the creating store.
    pub creator_file_num: u64,
    /// The shared-object cleanup method (`0` = ref-tracking, `1` = no cleanup).
    pub cleanup_method: u64,
    /// An optional locator naming the remote backend (empty if unset).
    pub locator: String,
    /// An optional name overriding the derived object name (empty if unset).
    pub custom_object_name: String,
}

/// A single catalog edit: objects added/removed and an optional creator-ID assignment.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CatalogVersionEdit {
    /// This store's creator ID, set in the catalog's first edit (`None` if unset in this edit).
    pub creator_id: Option<u64>,
    /// Objects added by this edit.
    pub new_objects: Vec<RemoteObjectMetadata>,
    /// File numbers of objects removed by this edit.
    pub deleted_objects: Vec<u64>,
}

fn put_string(buf: &mut Vec<u8>, s: &str) {
    put_uvarint(buf, s.len() as u64);
    buf.extend_from_slice(s.as_bytes());
}

impl CatalogVersionEdit {
    /// Encodes the edit to bytes, matching Pebble's `remoteobjcat.VersionEdit.Encode`.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        for o in &self.new_objects {
            put_uvarint(&mut buf, TAG_NEW_OBJECT);
            put_uvarint(&mut buf, o.file_num);
            put_uvarint(&mut buf, o.obj_type.to_u64());
            put_uvarint(&mut buf, o.creator_id);
            put_uvarint(&mut buf, o.creator_file_num);
            put_uvarint(&mut buf, o.cleanup_method);
            if !o.locator.is_empty() {
                put_uvarint(&mut buf, TAG_NEW_OBJECT_LOCATOR);
                put_string(&mut buf, &o.locator);
            }
            if !o.custom_object_name.is_empty() {
                put_uvarint(&mut buf, TAG_NEW_OBJECT_CUSTOM_NAME);
                put_string(&mut buf, &o.custom_object_name);
            }
            // Terminator for the optional sub-tags.
            put_uvarint(&mut buf, 0);
        }
        for &dfn in &self.deleted_objects {
            put_uvarint(&mut buf, TAG_DELETED_OBJECT);
            put_uvarint(&mut buf, dfn);
        }
        if let Some(id) = self.creator_id {
            put_uvarint(&mut buf, TAG_CREATOR_ID);
            put_uvarint(&mut buf, id);
        }
        buf
    }

    /// Decodes an edit from `data`, matching Pebble's `remoteobjcat.VersionEdit.Decode`.
    pub fn decode(data: &[u8]) -> Result<CatalogVersionEdit> {
        let mut d = Cursor2::new(data);
        let mut edit = CatalogVersionEdit::default();
        while let Some(tag) = d.maybe_uvarint()? {
            match tag {
                TAG_NEW_OBJECT => {
                    let file_num = d.uvarint()?;
                    let obj_type = ObjType::from_u64(d.uvarint()?)?;
                    let creator_id = d.uvarint()?;
                    let creator_file_num = d.uvarint()?;
                    let cleanup_method = d.uvarint()?;
                    let mut locator = String::new();
                    let mut custom_object_name = String::new();
                    loop {
                        let sub = d.uvarint()?;
                        match sub {
                            0 => break,
                            TAG_NEW_OBJECT_LOCATOR => locator = d.string()?,
                            TAG_NEW_OBJECT_CUSTOM_NAME => custom_object_name = d.string()?,
                            other => {
                                return Err(Error::Corruption(format!(
                                    "remote-obj-catalog: unknown new-object tag {other}"
                                )));
                            }
                        }
                    }
                    edit.new_objects.push(RemoteObjectMetadata {
                        file_num,
                        obj_type,
                        creator_id,
                        creator_file_num,
                        cleanup_method,
                        locator,
                        custom_object_name,
                    });
                }
                TAG_DELETED_OBJECT => edit.deleted_objects.push(d.uvarint()?),
                TAG_CREATOR_ID => edit.creator_id = Some(d.uvarint()?),
                other => {
                    return Err(Error::Corruption(format!(
                        "remote-obj-catalog: unknown tag {other}"
                    )));
                }
            }
        }
        Ok(edit)
    }
}

/// The accumulated contents of a catalog file: the store's creator ID (if set) and its live
/// remote objects keyed by file number.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CatalogContents {
    /// This store's creator ID, if assigned.
    pub creator_id: Option<u64>,
    /// Live remote objects, keyed by file number.
    pub objects: BTreeMap<u64, RemoteObjectMetadata>,
}

/// Replays a catalog file's bytes (a record log of [`CatalogVersionEdit`]s) into its accumulated
/// [`CatalogContents`].
pub fn read_catalog(bytes: &[u8]) -> Result<CatalogContents> {
    let mut contents = CatalogContents::default();
    let mut reader = record::Reader::new(Cursor::new(bytes), 0);
    while let Some(rec) = reader.read_record()? {
        let edit = CatalogVersionEdit::decode(&rec)?;
        if let Some(id) = edit.creator_id {
            contents.creator_id = Some(id);
        }
        for o in edit.new_objects {
            contents.objects.insert(o.file_num, o);
        }
        for dfn in edit.deleted_objects {
            contents.objects.remove(&dfn);
        }
    }
    Ok(contents)
}

/// Encodes a full catalog file from its contents: a single record holding one edit that recreates
/// the current state (creator ID + every live object), matching how Pebble writes a fresh catalog.
pub fn write_catalog(contents: &CatalogContents) -> Result<Vec<u8>> {
    let edit = CatalogVersionEdit {
        creator_id: contents.creator_id,
        new_objects: contents.objects.values().cloned().collect(),
        deleted_objects: Vec::new(),
    };
    let mut out = Vec::new();
    let mut w = record::Writer::new(&mut out);
    w.write_record(&edit.encode())?;
    w.finish()?;
    Ok(out)
}

/// A minimal byte cursor for the catalog's uvarint/string fields.
struct Cursor2<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Cursor2<'a> {
    fn new(b: &'a [u8]) -> Cursor2<'a> {
        Cursor2 { b, pos: 0 }
    }

    fn maybe_uvarint(&mut self) -> Result<Option<u64>> {
        if self.pos >= self.b.len() {
            return Ok(None);
        }
        Ok(Some(self.uvarint()?))
    }

    fn uvarint(&mut self) -> Result<u64> {
        let (v, n) = get_uvarint(&self.b[self.pos..])
            .ok_or_else(|| Error::corruption("remote-obj-catalog: truncated uvarint"))?;
        self.pos += n;
        Ok(v)
    }

    fn string(&mut self) -> Result<String> {
        let len = self.uvarint()? as usize;
        let end = self
            .pos
            .checked_add(len)
            .filter(|&e| e <= self.b.len())
            .ok_or_else(|| Error::corruption("remote-obj-catalog: truncated string"))?;
        let s = String::from_utf8(self.b[self.pos..end].to_vec())
            .map_err(|_| Error::corruption("remote-obj-catalog: non-utf8 string"))?;
        self.pos = end;
        Ok(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_edit_roundtrips() {
        let edit = CatalogVersionEdit {
            creator_id: Some(7),
            new_objects: vec![
                RemoteObjectMetadata {
                    file_num: 10,
                    obj_type: ObjType::Table,
                    creator_id: 7,
                    creator_file_num: 10,
                    cleanup_method: 0,
                    locator: String::new(),
                    custom_object_name: String::new(),
                },
                RemoteObjectMetadata {
                    file_num: 11,
                    obj_type: ObjType::Blob,
                    creator_id: 3,
                    creator_file_num: 99,
                    cleanup_method: 1,
                    locator: "s3-bucket".to_string(),
                    custom_object_name: "custom/name".to_string(),
                },
            ],
            deleted_objects: vec![5, 6],
        };
        let got = CatalogVersionEdit::decode(&edit.encode()).unwrap();
        assert_eq!(got, edit);
    }

    #[test]
    fn catalog_file_roundtrips() {
        let mut objects = BTreeMap::new();
        objects.insert(
            10,
            RemoteObjectMetadata {
                file_num: 10,
                obj_type: ObjType::Table,
                creator_id: 1,
                creator_file_num: 10,
                cleanup_method: 0,
                locator: String::new(),
                custom_object_name: String::new(),
            },
        );
        let contents = CatalogContents {
            creator_id: Some(1),
            objects,
        };
        let bytes = write_catalog(&contents).unwrap();
        assert_eq!(read_catalog(&bytes).unwrap(), contents);
    }

    #[test]
    fn deleted_object_removes_from_contents() {
        let mut buf = Vec::new();
        let mut w = record::Writer::new(&mut buf);
        w.write_record(
            &CatalogVersionEdit {
                creator_id: Some(1),
                new_objects: vec![RemoteObjectMetadata {
                    file_num: 10,
                    obj_type: ObjType::Table,
                    creator_id: 1,
                    creator_file_num: 10,
                    cleanup_method: 0,
                    locator: String::new(),
                    custom_object_name: String::new(),
                }],
                deleted_objects: Vec::new(),
            }
            .encode(),
        )
        .unwrap();
        w.write_record(
            &CatalogVersionEdit {
                creator_id: None,
                new_objects: Vec::new(),
                deleted_objects: vec![10],
            }
            .encode(),
        )
        .unwrap();
        w.finish().unwrap();
        let contents = read_catalog(&buf).unwrap();
        assert!(contents.objects.is_empty());
        assert_eq!(contents.creator_id, Some(1));
    }
}
