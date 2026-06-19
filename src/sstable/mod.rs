// Copyright (c) 2011 The LevelDB-Go Authors. All rights reserved.
// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Ported from Pebble's sstable package (format.go, table.go, reader.go).

//! Reading sorted-string tables (sstables).
//!
//! An sstable is an immutable, sorted file of internal key/value pairs. Its layout is:
//!
//! ```text
//! [data block]+        prefix-compressed key/value entries (see [`block`])
//! [metaindex block]    names -> handles for the filter/properties/range blocks
//! [index block]        separator keys -> data block handles
//! [footer]             checksum type, metaindex & index handles, version, magic
//! ```
//!
//! [`Reader`] opens a table held entirely in memory, parses the footer, and supports
//! point lookups ([`Reader::get`]) and full ordered iteration ([`Reader::iter`]).
//!
//! Scope: this reader handles the row-based block format with single- and two-level
//! binary-search indexes, bloom filters, properties, range-del and range-key blocks,
//! value blocks (Pebblev3+ value prefixes and out-of-line values), and CRC32C or
//! xxHash64 checksums — the RocksDBv2 and Pebblev1..v4 table formats. The columnar block
//! format (Pebblev5+) and blob files are not yet supported.

pub mod block;
pub mod filter;
pub mod properties;
pub mod valblk;
pub mod writer;

pub use properties::Properties;
pub use writer::{Writer, WriterOptions};

use std::sync::Arc;

use crate::base::comparer::Comparer;
use crate::base::internal_key::{InternalKeyKind, SeqNum, encoded_user_key, trailer_kind};
use crate::base::range_del::RangeTombstone;
use crate::base::range_key::RangeKeyEntry;
use crate::{Error, Result};

use block::{BlockHandle, BlockIter, ChecksumType, read_block};

const MAGIC_LEN: usize = 8;
const VERSION_LEN: usize = 4;

const LEVELDB_MAGIC: &[u8; 8] = b"\x57\xfb\x80\x8b\x24\x75\x47\xdb";
const ROCKSDB_MAGIC: &[u8; 8] = b"\xf7\xcf\xf4\x85\xb7\x41\xe2\x88";
const PEBBLE_MAGIC: &[u8; 8] = b"\xf0\x9f\xaa\xb3\xf0\x9f\xaa\xb3";

const LEVELDB_FOOTER_LEN: usize = 48;
const ROCKSDB_FOOTER_LEN: usize = 53;

/// The table format: a (magic number, version) tuple that determines the footer layout
/// and which features the table may use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableFormat {
    /// Original LevelDB format (48-byte footer, implicit CRC32C, no checksum-type byte).
    LevelDB,
    /// RocksDB external format version 2 (53-byte footer).
    RocksDBv2,
    /// Pebble format version `v` (1-based). `v <= 5` uses the 53-byte footer.
    Pebble(u8),
}

impl TableFormat {
    fn footer_len(self) -> usize {
        match self {
            TableFormat::LevelDB => LEVELDB_FOOTER_LEN,
            TableFormat::RocksDBv2 => ROCKSDB_FOOTER_LEN,
            TableFormat::Pebble(v) => match v {
                1..=5 => ROCKSDB_FOOTER_LEN,
                6 => ROCKSDB_FOOTER_LEN + 4, // adds a footer checksum
                _ => ROCKSDB_FOOTER_LEN + 4 + 4, // v7+ also adds an attributes word
            },
        }
    }

    /// Whether point values carry a value-prefix byte and may reference value blocks
    /// (Pebble format v3+).
    fn prefixes_values(self) -> bool {
        matches!(self, TableFormat::Pebble(v) if v >= 3)
    }

    /// Whether the table uses the columnar block format (Pebble format v5+), which this
    /// reader does not yet support.
    fn is_columnar(self) -> bool {
        matches!(self, TableFormat::Pebble(v) if v >= 5)
    }
}

/// The decoded sstable footer.
#[derive(Clone, Copy, Debug)]
struct Footer {
    format: TableFormat,
    checksum: ChecksumType,
    index: BlockHandle,
    metaindex: BlockHandle,
}

fn parse_footer(file: &[u8]) -> Result<Footer> {
    let n = file.len();
    if n < LEVELDB_FOOTER_LEN {
        return Err(Error::corruption("sstable: file smaller than footer"));
    }
    let magic = &file[n - MAGIC_LEN..];

    if magic == LEVELDB_MAGIC {
        let f = &file[n - LEVELDB_FOOTER_LEN..];
        let (metaindex, m) = BlockHandle::decode(f)
            .ok_or_else(|| Error::corruption("sstable: bad metaindex handle"))?;
        let (index, _) = BlockHandle::decode(&f[m..])
            .ok_or_else(|| Error::corruption("sstable: bad index handle"))?;
        return Ok(Footer {
            format: TableFormat::LevelDB,
            checksum: ChecksumType::Crc32c,
            index,
            metaindex,
        });
    }

    if magic == ROCKSDB_MAGIC || magic == PEBBLE_MAGIC {
        let version = u32::from_le_bytes(
            file[n - MAGIC_LEN - VERSION_LEN..n - MAGIC_LEN]
                .try_into()
                .unwrap(),
        );
        let format = if magic == ROCKSDB_MAGIC {
            if version != 2 {
                return Err(Error::Corruption(format!(
                    "sstable: unsupported rocksdb version {version}"
                )));
            }
            TableFormat::RocksDBv2
        } else {
            if version == 0 || version > 8 {
                return Err(Error::Corruption(format!(
                    "sstable: unsupported pebble version {version}"
                )));
            }
            TableFormat::Pebble(version as u8)
        };
        let footer_len = format.footer_len();
        if n < footer_len {
            return Err(Error::corruption("sstable: file smaller than footer"));
        }
        let f = &file[n - footer_len..];
        let checksum = ChecksumType::from_u8(f[0])?;
        // Handles are encoded immediately after the checksum-type byte for all of these
        // formats; any v6+ footer checksum / attributes sit near the end and are ignored.
        let (metaindex, m) = BlockHandle::decode(&f[1..])
            .ok_or_else(|| Error::corruption("sstable: bad metaindex handle"))?;
        let (index, _) = BlockHandle::decode(&f[1 + m..])
            .ok_or_else(|| Error::corruption("sstable: bad index handle"))?;
        return Ok(Footer {
            format,
            checksum,
            index,
            metaindex,
        });
    }

    Err(Error::corruption("sstable: bad magic number"))
}

/// The decoded contents of an sstable's metaindex-referenced meta blocks.
struct MetaBlocks {
    props: Properties,
    filter: Option<Arc<[u8]>>,
    range_dels: Vec<RangeTombstone>,
    range_keys: Vec<RangeKeyEntry>,
    value_block_handles: Vec<BlockHandle>,
}

/// Reads the metaindex and the meta blocks it references (`rocksdb.properties`,
/// `fullfilter.*`, `rocksdb.range_del2`).
fn read_metaindex(file: &[u8], footer: &Footer) -> Result<MetaBlocks> {
    let metaindex = read_block(file, footer.metaindex, footer.checksum)?;
    let mut it = BlockIter::new(metaindex)?;
    it.first();
    let mut props_handle = None;
    let mut filter_handle = None;
    let mut range_del_handle = None;
    let mut range_key_handle = None;
    let mut value_index: Option<valblk::IndexHandle> = None;
    while it.valid() {
        let key = it.key();
        if key == properties::META_PROPERTIES_NAME.as_bytes() {
            props_handle = BlockHandle::decode(it.value()).map(|(h, _)| h);
        } else if key == properties::META_RANGE_DEL_NAME.as_bytes() {
            range_del_handle = BlockHandle::decode(it.value()).map(|(h, _)| h);
        } else if key == properties::META_RANGE_KEY_NAME.as_bytes() {
            range_key_handle = BlockHandle::decode(it.value()).map(|(h, _)| h);
        } else if key == properties::META_VALUE_INDEX_NAME.as_bytes() {
            value_index = Some(valblk::decode_index_handle(it.value())?);
        } else if key.starts_with(b"fullfilter.") {
            filter_handle = BlockHandle::decode(it.value()).map(|(h, _)| h);
        }
        it.next();
    }

    let mut props = Properties::default();
    if let Some(handle) = props_handle {
        let block = read_block(file, handle, footer.checksum)?;
        let mut pit = BlockIter::new(block)?;
        pit.first();
        while pit.valid() {
            props.decode_entry(pit.key(), pit.value());
            pit.next();
        }
    }

    let filter = match filter_handle {
        Some(handle) => Some(read_block(file, handle, footer.checksum)?),
        None => None,
    };

    let mut range_dels = Vec::new();
    if let Some(handle) = range_del_handle {
        let block = read_block(file, handle, footer.checksum)?;
        let mut rit = BlockIter::new(block)?;
        rit.first();
        while rit.valid() {
            // key = (start, seq, RangeDelete); value = end user key.
            let start = encoded_user_key(rit.key()).to_vec();
            let seqnum = crate::base::internal_key::trailer_seqnum(
                crate::base::internal_key::encoded_trailer(rit.key()),
            );
            range_dels.push(RangeTombstone::new(start, rit.value().to_vec(), seqnum));
            rit.next();
        }
    }

    let mut range_keys = Vec::new();
    if let Some(handle) = range_key_handle {
        let block = read_block(file, handle, footer.checksum)?;
        let mut rit = BlockIter::new(block)?;
        rit.first();
        while rit.valid() {
            let trailer = crate::base::internal_key::encoded_trailer(rit.key());
            range_keys.push(RangeKeyEntry {
                kind: trailer_kind(trailer),
                start: encoded_user_key(rit.key()).to_vec(),
                seqnum: crate::base::internal_key::trailer_seqnum(trailer),
                value: rit.value().to_vec(),
            });
            rit.next();
        }
    }

    let mut value_block_handles = Vec::new();
    if let Some(ih) = value_index {
        let index_block = read_block(file, ih.handle, footer.checksum)?;
        value_block_handles = valblk::decode_index(&index_block, &ih)?;
    }

    Ok(MetaBlocks {
        props,
        filter,
        range_dels,
        range_keys,
        value_block_handles,
    })
}

/// A reader over an in-memory sstable.
pub struct Reader {
    file: Arc<[u8]>,
    cmp: Arc<dyn Comparer>,
    footer: Footer,
    /// The decoded top-level index block, cached at open.
    index: Arc<[u8]>,
    /// The table's properties, parsed from the metaindex (default if absent).
    props: Properties,
    /// The table's bloom filter block, if present.
    filter: Option<Arc<[u8]>>,
    /// The table's range tombstones, parsed from the range-del block.
    range_dels: Vec<RangeTombstone>,
    /// The table's range-key entries, parsed from the range-key block.
    range_keys: Vec<RangeKeyEntry>,
    /// Whether the index is two-level (the footer index handle is the top-level index).
    two_level: bool,
    /// Whether point values carry a value-prefix byte (Pebble format v3+).
    prefixed_values: bool,
    /// Handles of the table's value blocks, indexed by block number.
    value_block_handles: Vec<BlockHandle>,
    /// This table's file number, used as the block-cache key prefix.
    file_num: u64,
    /// Optional shared block cache for decompressed blocks.
    block_cache: Option<Arc<crate::cache::BlockCache>>,
}

impl Reader {
    /// Opens an sstable held entirely in `file`, comparing user keys with `cmp`.
    pub fn open(file: impl Into<Arc<[u8]>>, cmp: Arc<dyn Comparer>) -> Result<Reader> {
        Reader::open_with_cache(file, cmp, 0, None)
    }

    /// Opens an sstable, tagging cached blocks with `file_num` and consulting the optional
    /// shared block cache.
    pub fn open_with_cache(
        file: impl Into<Arc<[u8]>>,
        cmp: Arc<dyn Comparer>,
        file_num: u64,
        block_cache: Option<Arc<crate::cache::BlockCache>>,
    ) -> Result<Reader> {
        let file: Arc<[u8]> = file.into();
        let footer = parse_footer(&file)?;
        if footer.format.is_columnar() {
            return Err(Error::Unsupported(
                "sstable: columnar block format (Pebblev5+) not yet supported",
            ));
        }
        let prefixed_values = footer.format.prefixes_values();
        let meta = read_metaindex(&file, &footer)?;
        let two_level = meta.props.is_two_level_index();
        let index = read_block(&file, footer.index, footer.checksum)?;
        Ok(Reader {
            file,
            cmp,
            footer,
            index,
            prefixed_values,
            value_block_handles: meta.value_block_handles,
            props: meta.props,
            filter: meta.filter,
            range_dels: meta.range_dels,
            range_keys: meta.range_keys,
            two_level,
            file_num,
            block_cache,
        })
    }

    /// The table's range tombstones.
    pub fn range_tombstones(&self) -> &[RangeTombstone] {
        &self.range_dels
    }

    /// The table's range-key entries.
    pub fn range_keys(&self) -> &[RangeKeyEntry] {
        &self.range_keys
    }

    /// The table's properties.
    pub fn properties(&self) -> &Properties {
        &self.props
    }

    /// The table's format.
    pub fn format(&self) -> TableFormat {
        self.footer.format
    }

    /// The checksum type protecting the table's blocks.
    pub fn checksum_type(&self) -> ChecksumType {
        self.footer.checksum
    }

    /// Reads and decodes the block referenced by `handle`, consulting the block cache
    /// (keyed by file number + offset) when one is configured.
    fn read_cached(&self, handle: BlockHandle) -> Result<Arc<[u8]>> {
        if let Some(cache) = &self.block_cache {
            let key = (self.file_num, handle.offset);
            if let Some(block) = cache.get(key) {
                return Ok(block);
            }
            let block = read_block(&self.file, handle, self.footer.checksum)?;
            cache.insert(key, Arc::clone(&block));
            return Ok(block);
        }
        read_block(&self.file, handle, self.footer.checksum)
    }

    /// Reads and decodes the data block referenced by `handle`.
    fn read_data_block(&self, handle: BlockHandle) -> Result<Arc<[u8]>> {
        self.read_cached(handle)
    }

    /// Resolves a value stored in a data block to the actual value bytes.
    ///
    /// For format versions without value prefixes the stored bytes *are* the value. For
    /// v3+ the first byte is a value-prefix: an in-place value is the remaining bytes; a
    /// value handle is decoded and the value is read from the referenced value block.
    pub(crate) fn resolve_value(&self, stored: &[u8]) -> Result<Vec<u8>> {
        if !self.prefixed_values {
            return Ok(stored.to_vec());
        }
        if stored.is_empty() {
            return Ok(Vec::new());
        }
        let kind = valblk::value_kind(stored[0]);
        match kind {
            valblk::KIND_IN_PLACE => Ok(stored[1..].to_vec()),
            valblk::KIND_HANDLE => {
                let h = valblk::decode_handle(&stored[1..])?;
                let block_handle = *self
                    .value_block_handles
                    .get(h.block_num as usize)
                    .ok_or_else(|| Error::corruption("sstable: value block number out of range"))?;
                let block = self.read_cached(block_handle)?;
                let start = h.offset_in_block as usize;
                let end = start + h.value_len as usize;
                if end > block.len() {
                    return Err(Error::corruption("sstable: value handle out of range"));
                }
                Ok(block[start..end].to_vec())
            }
            _ => Err(Error::Unsupported("sstable: blob-referenced value")),
        }
    }

    /// Returns an iterator over the top-level index block.
    fn index_iter(&self) -> Result<BlockIter> {
        BlockIter::new(Arc::clone(&self.index))
    }

    /// Resolves the handle of the data block that may contain `lookup`, walking one or
    /// two index levels as appropriate. Returns `None` if `lookup` is past the table.
    fn seek_data_handle(&self, lookup: &[u8]) -> Result<Option<BlockHandle>> {
        let mut index = self.index_iter()?;
        index.seek_ge(lookup, self.cmp.as_ref());
        if !index.valid() {
            return Ok(None);
        }
        let (handle, _) = BlockHandle::decode(index.value())
            .ok_or_else(|| Error::corruption("sstable: bad index entry handle"))?;
        if !self.two_level {
            return Ok(Some(handle));
        }
        // Two-level: `handle` points to a lower-level index partition.
        let partition = read_block(&self.file, handle, self.footer.checksum)?;
        let mut pit = BlockIter::new(partition)?;
        pit.seek_ge(lookup, self.cmp.as_ref());
        if !pit.valid() {
            return Ok(None);
        }
        let (data_handle, _) = BlockHandle::decode(pit.value())
            .ok_or_else(|| Error::corruption("sstable: bad index entry handle"))?;
        Ok(Some(data_handle))
    }

    /// Collects every data-block handle in the table, in order, flattening a two-level
    /// index. Used to drive full-table iteration.
    fn data_block_handles(&self) -> Result<Vec<BlockHandle>> {
        let mut handles = Vec::new();
        let mut index = self.index_iter()?;
        index.first();
        while index.valid() {
            let (handle, _) = BlockHandle::decode(index.value())
                .ok_or_else(|| Error::corruption("sstable: bad index entry handle"))?;
            if self.two_level {
                let partition = read_block(&self.file, handle, self.footer.checksum)?;
                let mut pit = BlockIter::new(partition)?;
                pit.first();
                while pit.valid() {
                    let (dh, _) = BlockHandle::decode(pit.value())
                        .ok_or_else(|| Error::corruption("sstable: bad index entry handle"))?;
                    handles.push(dh);
                    pit.next();
                }
            } else {
                handles.push(handle);
            }
            index.next();
        }
        Ok(handles)
    }

    /// Looks up `user_key` as visible at `snapshot`, returning the kind and value of the
    /// most recent matching entry with sequence number `<= snapshot`, or `None`.
    ///
    /// A returned [`InternalKeyKind::Delete`] / [`InternalKeyKind::SingleDelete`] is a
    /// tombstone: the caller treats it as absent for this table.
    pub fn get(
        &self,
        user_key: &[u8],
        snapshot: SeqNum,
    ) -> Result<Option<(InternalKeyKind, Vec<u8>)>> {
        Ok(self.lookup(user_key, snapshot)?.map(|(_, k, v)| (k, v)))
    }

    /// Like [`Reader::get`] but also returns the entry's sequence number, used by the
    /// database to compare point keys against range tombstones.
    pub fn lookup(
        &self,
        user_key: &[u8],
        snapshot: SeqNum,
    ) -> Result<Option<(SeqNum, InternalKeyKind, Vec<u8>)>> {
        // The bloom filter can rule the key out without touching any data block.
        if let Some(filter) = &self.filter
            && !filter::may_contain(filter, user_key)
        {
            return Ok(None);
        }

        // The lookup internal key sorts just before any real entry at `snapshot`.
        let mut lookup = user_key.to_vec();
        lookup.extend_from_slice(&(((snapshot << 8) | 0xff).to_le_bytes()));

        let handle = match self.seek_data_handle(&lookup)? {
            Some(h) => h,
            None => return Ok(None),
        };
        let data = self.read_data_block(handle)?;
        let mut it = BlockIter::new(data)?;
        it.seek_ge(&lookup, self.cmp.as_ref());
        if !it.valid() {
            return Ok(None);
        }
        if self.cmp.compare(encoded_user_key(it.key()), user_key) != std::cmp::Ordering::Equal {
            return Ok(None);
        }
        let trailer = crate::base::internal_key::encoded_trailer(it.key());
        let kind = trailer_kind(trailer);
        Ok(Some((trailer >> 8, kind, self.resolve_value(it.value())?)))
    }

    /// Returns every version of `user_key` visible at `snapshot`, newest first, used to
    /// resolve merge operands. (Bounded to the data block containing the key; keys with
    /// versions spanning a block boundary are handled by continuing into the next block.)
    pub fn lookup_versions(
        &self,
        user_key: &[u8],
        snapshot: SeqNum,
    ) -> Result<Vec<(SeqNum, InternalKeyKind, Vec<u8>)>> {
        let mut out = Vec::new();
        if let Some(filter) = &self.filter
            && !filter::may_contain(filter, user_key)
        {
            return Ok(out);
        }
        let mut lookup = user_key.to_vec();
        lookup.extend_from_slice(&(((snapshot << 8) | 0xff).to_le_bytes()));

        // Walk forward across data blocks while the user key matches.
        let handles = self.data_block_handles()?;
        // Find the first data block that may contain the key, then iterate from there.
        let start = match self.seek_data_handle(&lookup)? {
            Some(h) => handles.iter().position(|x| *x == h).unwrap_or(0),
            None => return Ok(out),
        };
        let mut first = true;
        for &handle in &handles[start..] {
            let data = self.read_data_block(handle)?;
            let mut it = BlockIter::new(data)?;
            if first {
                it.seek_ge(&lookup, self.cmp.as_ref());
                first = false;
            } else {
                it.first();
            }
            while it.valid() {
                match self.cmp.compare(encoded_user_key(it.key()), user_key) {
                    std::cmp::Ordering::Less => {
                        it.next();
                        continue;
                    }
                    std::cmp::Ordering::Greater => return Ok(out),
                    std::cmp::Ordering::Equal => {}
                }
                let trailer = crate::base::internal_key::encoded_trailer(it.key());
                out.push((
                    trailer >> 8,
                    trailer_kind(trailer),
                    self.resolve_value(it.value())?,
                ));
                it.next();
            }
        }
        Ok(out)
    }

    /// Returns an iterator over every entry in the table, in internal-key order.
    ///
    /// The iterator holds a shared reference to the reader, so it can outlive the
    /// borrow and be stored (e.g. in a merging iterator).
    pub fn iter(self: &Arc<Reader>) -> Result<TableIter> {
        let handles = self.data_block_handles()?;
        Ok(TableIter {
            reader: Arc::clone(self),
            handles,
            next_block: 0,
            data: None,
            cur_value: Vec::new(),
        })
    }
}

/// A forward iterator over all entries of an sstable. Data-block handles are resolved up
/// front (flattening any two-level index); each block is read and iterated in turn.
pub struct TableIter {
    reader: Arc<Reader>,
    handles: Vec<BlockHandle>,
    /// Index into `handles` of the next block to load.
    next_block: usize,
    data: Option<BlockIter>,
    /// The current entry's resolved value (value prefixes/handles already applied).
    cur_value: Vec<u8>,
}

impl TableIter {
    /// Loads the next data block in `handles`, if any, returning whether one was loaded.
    fn load_next_block(&mut self) -> Result<bool> {
        if self.next_block >= self.handles.len() {
            self.data = None;
            return Ok(false);
        }
        let handle = self.handles[self.next_block];
        self.next_block += 1;
        let block = self.reader.read_data_block(handle)?;
        let mut it = BlockIter::new(block)?;
        it.first();
        self.data = Some(it);
        Ok(true)
    }

    /// Resolves and caches the current entry's value.
    fn refresh_value(&mut self) -> Result<()> {
        if let Some(d) = self.data.as_ref()
            && d.valid()
        {
            self.cur_value = self.reader.resolve_value(d.value())?;
        }
        Ok(())
    }

    /// Advances to the first entry and returns whether one exists.
    pub fn first(&mut self) -> Result<bool> {
        self.next_block = 0;
        // Skip any empty data blocks.
        loop {
            if !self.load_next_block()? {
                return Ok(false);
            }
            if self.data.as_ref().is_some_and(|d| d.valid()) {
                self.refresh_value()?;
                return Ok(true);
            }
        }
    }

    /// Whether the iterator is at a valid entry.
    pub fn valid(&self) -> bool {
        self.data.as_ref().is_some_and(|d| d.valid())
    }

    /// The current entry's encoded internal key.
    pub fn key(&self) -> &[u8] {
        self.data.as_ref().expect("valid").key()
    }

    /// The current entry's resolved value.
    pub fn value(&self) -> &[u8] {
        &self.cur_value
    }

    /// Advances to the next entry. Returns whether the iterator remains valid.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<bool> {
        match self.data.as_mut() {
            Some(d) => {
                d.next();
                if d.valid() {
                    self.refresh_value()?;
                    return Ok(true);
                }
            }
            None => return Ok(false),
        }
        // Current data block exhausted; advance to the next non-empty block.
        loop {
            if !self.load_next_block()? {
                return Ok(false);
            }
            if self.data.as_ref().is_some_and(|d| d.valid()) {
                self.refresh_value()?;
                return Ok(true);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    // The reader is exercised end-to-end against the writer in the Phase 6 sstable
    // writer tests (build a table, then read it back). Footer parsing is covered there
    // and via the round-trip integration tests.
    use super::*;

    #[test]
    fn footer_too_short_is_rejected() {
        assert!(parse_footer(&[0u8; 8]).is_err());
    }

    #[test]
    fn bad_magic_is_rejected() {
        let buf = vec![0u8; 64];
        assert!(parse_footer(&buf).is_err());
    }
}
