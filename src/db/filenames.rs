// Copyright (c) 2012 The LevelDB-Go Authors. All rights reserved.
// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Ported from Pebble's internal/base/filenames.go and vfs/atomicfs/marker.go.

//! Database file naming conventions and atomic-marker resolution.
//!
//! Pebble names files `<file_num:06>.sst`, `MANIFEST-<num:06>`, `OPTIONS-<num:06>`, and
//! `LOCK`. The current MANIFEST is identified by an *atomic marker*: a zero-byte file
//! named `marker.manifest.<iter:06>.<value>`, where `value` is the MANIFEST's filename
//! and the live marker is the one with the highest `iter`.

/// The filename of the sstable with the given file number.
pub fn table(file_num: u64) -> String {
    format!("{file_num:06}.sst")
}

/// The filename of the MANIFEST with the given file number.
// Used by the write path (Phase 9) and by tests; keep it available.
#[allow(dead_code)]
pub fn manifest(file_num: u64) -> String {
    format!("MANIFEST-{file_num:06}")
}

/// Parses the file number from a `MANIFEST-<num>` filename.
fn parse_manifest_num(name: &str) -> Option<u64> {
    name.strip_prefix("MANIFEST-")?.parse().ok()
}

/// Parses a `marker.<name>.<iter>.<value>` filename into `(name, iter, value)`.
fn parse_marker(name: &str) -> Option<(&str, u64, &str)> {
    let rest = name.strip_prefix("marker.")?;
    // name, iter, value — value may itself contain dots, so split into at most 3 parts.
    let mut parts = rest.splitn(3, '.');
    let marker_name = parts.next()?;
    let iter: u64 = parts.next()?.parse().ok()?;
    let value = parts.next()?;
    Some((marker_name, iter, value))
}

/// Determines the current MANIFEST filename from a directory listing.
///
/// Prefers the `manifest` atomic marker (highest `iter`); if no marker exists, falls
/// back to the highest-numbered `MANIFEST-*` file.
pub fn current_manifest(names: &[String]) -> Option<String> {
    let mut best: Option<(u64, String)> = None;
    for name in names {
        if let Some(("manifest", iter, value)) = parse_marker(name)
            && best.as_ref().is_none_or(|(i, _)| iter > *i)
        {
            best = Some((iter, value.to_string()));
        }
    }
    if let Some((_, value)) = best {
        return Some(value);
    }

    // Fallback: highest-numbered MANIFEST-*.
    names
        .iter()
        .filter_map(|n| parse_manifest_num(n).map(|num| (num, n.clone())))
        .max_by_key(|(num, _)| *num)
        .map(|(_, n)| n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filename_formats() {
        assert_eq!(table(5), "000005.sst");
        assert_eq!(table(123456), "123456.sst");
        assert_eq!(manifest(3), "MANIFEST-000003");
    }

    #[test]
    fn marker_parsing() {
        assert_eq!(
            parse_marker("marker.manifest.000007.MANIFEST-000123"),
            Some(("manifest", 7, "MANIFEST-000123"))
        );
        assert_eq!(parse_marker("MANIFEST-000001"), None);
    }

    #[test]
    fn current_manifest_prefers_highest_marker() {
        let names = vec![
            "MANIFEST-000001".to_string(),
            "MANIFEST-000005".to_string(),
            "marker.manifest.000001.MANIFEST-000001".to_string(),
            "marker.manifest.000002.MANIFEST-000005".to_string(),
            "000010.sst".to_string(),
        ];
        assert_eq!(current_manifest(&names).as_deref(), Some("MANIFEST-000005"));
    }

    #[test]
    fn current_manifest_falls_back_to_highest_numbered() {
        let names = vec![
            "MANIFEST-000001".to_string(),
            "MANIFEST-000009".to_string(),
            "000010.sst".to_string(),
        ];
        assert_eq!(current_manifest(&names).as_deref(), Some("MANIFEST-000009"));
    }

    #[test]
    fn current_manifest_none_when_absent() {
        assert_eq!(current_manifest(&["000010.sst".to_string()]), None);
    }
}
