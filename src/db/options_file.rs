// Copyright (c) 2019 The LevelDB-Go Authors. All rights reserved.
// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Ported from Pebble's options.go (OPTIONS file (de)serialization) and
// format_major_version.go.

//! The `OPTIONS-NNNNNN` file and the format major version.
//!
//! Pebble writes a human-readable, INI-style `OPTIONS` file on open recording the options
//! a store was created/opened with. On reopen it is parsed and the comparer name is
//! validated against the configured comparer (opening with the wrong comparer is an
//! error). The `format_major_version` records which on-disk features are enabled and may
//! only ever *ratchet* upward.

use std::fmt::Write as _;

use crate::{Error, Result};

/// The on-disk format version a database has been upgraded to. It only ever increases;
/// each step may enable new on-disk features (range keys, value blocks, columnar blocks).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FormatMajorVersion(pub u32);

impl FormatMajorVersion {
    /// The most backwards-compatible format (RocksDBv2 sstables, no Pebble extensions).
    pub const MOST_COMPATIBLE: FormatMajorVersion = FormatMajorVersion(1);
    /// Range keys (`RANGEKEYSET`/`UNSET`/`DEL`) are permitted on disk.
    pub const RANGE_KEYS: FormatMajorVersion = FormatMajorVersion(7);
    /// Pebblev3 value blocks (separated values) are permitted on disk.
    pub const VALUE_BLOCKS: FormatMajorVersion = FormatMajorVersion(9);
    /// Flushable ingestion; the classic row (block-based) sstable layout. This is the minimum
    /// format major version upstream Pebble v2 still supports, so it is the most compatible
    /// format for cross-engine interop with current Pebble.
    pub const FLUSHABLE_INGEST: FormatMajorVersion = FormatMajorVersion(13);
    /// Columnar sstable blocks (Pebble v2's `FormatColumnarBlocks`). This engine can **read**
    /// columnar tables; it still writes the row format, which a columnar-format database may also
    /// contain.
    pub const COLUMNAR_BLOCKS: FormatMajorVersion = FormatMajorVersion(19);
    /// The newest format this implementation understands (can open / read).
    pub const NEWEST: FormatMajorVersion = FormatMajorVersion::COLUMNAR_BLOCKS;

    /// The default format for a freshly created database: the row layout, which is the most
    /// broadly compatible format current Pebble still reads and writes.
    pub const DEFAULT: FormatMajorVersion = FormatMajorVersion::FLUSHABLE_INGEST;

    /// The raw integer version.
    pub fn as_u32(self) -> u32 {
        self.0
    }
}

/// The parsed contents of an `OPTIONS` file (the fields this implementation tracks).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptionsFile {
    /// The comparer name; must match the store's configured comparer on open.
    pub comparer_name: String,
    /// The merge-operator name, if one was configured.
    pub merger_name: Option<String>,
    /// The on-disk format major version.
    pub format_major_version: FormatMajorVersion,
}

impl OptionsFile {
    /// Serializes to Pebble's INI-style `OPTIONS` layout.
    pub fn encode(&self) -> String {
        let mut s = String::new();
        s.push_str("[Version]\n");
        s.push_str("  pebble_version=0.1\n\n");
        s.push_str("[Options]\n");
        let _ = writeln!(s, "  comparer={}", self.comparer_name);
        if let Some(m) = &self.merger_name {
            let _ = writeln!(s, "  merger={m}");
        }
        let _ = writeln!(
            s,
            "  format_major_version={}",
            self.format_major_version.as_u32()
        );
        s
    }

    /// Parses an `OPTIONS` file. Unknown sections and keys are ignored, matching Pebble's
    /// forward-compatible behavior.
    pub fn decode(text: &str) -> Result<OptionsFile> {
        let mut comparer_name = String::new();
        let mut merger_name = None;
        let mut format_major_version = FormatMajorVersion::MOST_COMPATIBLE;

        for raw in text.lines() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('[') || line.starts_with('#') {
                continue;
            }
            let Some((key, value)) = line.split_once('=') else {
                continue;
            };
            let (key, value) = (key.trim(), value.trim());
            match key {
                "comparer" => comparer_name = value.to_string(),
                "merger" if value != "nullptr" && !value.is_empty() => {
                    merger_name = Some(value.to_string())
                }
                "format_major_version" => {
                    let v: u32 = value
                        .parse()
                        .map_err(|_| Error::corruption("options: bad format_major_version"))?;
                    format_major_version = FormatMajorVersion(v);
                }
                _ => {}
            }
        }

        Ok(OptionsFile {
            comparer_name,
            merger_name,
            format_major_version,
        })
    }

    /// Validates that this file is compatible with a store configured with `comparer_name`
    /// and an implementation that understands up to [`FormatMajorVersion::NEWEST`].
    pub fn validate(&self, comparer_name: &str) -> Result<()> {
        if !self.comparer_name.is_empty() && self.comparer_name != comparer_name {
            return Err(Error::InvalidState(format!(
                "options: comparer mismatch: file has {:?}, opened with {:?}",
                self.comparer_name, comparer_name
            )));
        }
        if self.format_major_version > FormatMajorVersion::NEWEST {
            return Err(Error::InvalidState(format!(
                "options: format_major_version {} is newer than this implementation supports ({})",
                self.format_major_version.as_u32(),
                FormatMajorVersion::NEWEST.as_u32()
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let o = OptionsFile {
            comparer_name: "leveldb.BytewiseComparator".into(),
            merger_name: Some("pebble.concatenate".into()),
            format_major_version: FormatMajorVersion::VALUE_BLOCKS,
        };
        let text = o.encode();
        assert!(text.contains("comparer=leveldb.BytewiseComparator"));
        assert!(text.contains("merger=pebble.concatenate"));
        assert!(text.contains("format_major_version=9"));
        assert_eq!(OptionsFile::decode(&text).unwrap(), o);
    }

    #[test]
    fn decode_ignores_unknown_keys_and_sections() {
        let text = "[Version]\n  pebble_version=0.1\n[Options]\n  comparer=c\n  \
                    bytes_per_sync=1048576\n  format_major_version=7\n";
        let o = OptionsFile::decode(text).unwrap();
        assert_eq!(o.comparer_name, "c");
        assert_eq!(o.format_major_version, FormatMajorVersion::RANGE_KEYS);
        assert_eq!(o.merger_name, None);
    }

    #[test]
    fn validate_rejects_comparer_mismatch() {
        let o = OptionsFile {
            comparer_name: "other".into(),
            merger_name: None,
            format_major_version: FormatMajorVersion::MOST_COMPATIBLE,
        };
        assert!(o.validate("leveldb.BytewiseComparator").is_err());
        assert!(o.validate("other").is_ok());
    }

    #[test]
    fn validate_rejects_future_format() {
        let o = OptionsFile {
            comparer_name: String::new(),
            merger_name: None,
            format_major_version: FormatMajorVersion(9999),
        };
        assert!(o.validate("c").is_err());
    }
}
