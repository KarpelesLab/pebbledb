// Copyright (c) 2011 The LevelDB-Go Authors. All rights reserved.
// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.
//
// Ported from Pebble's record/record.go and record/log_writer.go.

//! The record log: the framing format used by the write-ahead log (WAL) and the
//! MANIFEST.
//!
//! A log file is a sequence of fixed-size 32 KiB *blocks*. Each block holds a sequence
//! of *chunks*; a logical record is one or more consecutive chunks. A record that does
//! not fit in the remaining space of a block is fragmented across blocks, so a chunk
//! never straddles a block boundary. Any trailing bytes of a block too small to hold a
//! chunk header are zero-padding.
//!
//! Three wire formats exist, distinguished by the chunk's encoding byte:
//!
//! ```text
//! legacy     (7 bytes):  checksum:u32 | length:u16 | type:u8
//! recyclable (11 bytes): checksum:u32 | length:u16 | type:u8 | log_num:u32
//! wal_sync   (19 bytes): checksum:u32 | length:u16 | type:u8 | log_num:u32 | synced:u64
//! ```
//!
//! All integers are little-endian. `length` is the size of the chunk's data payload
//! (not counting the header). The checksum is the masked CRC32C
//! ([`crate::crc::masked_crc32c`]) over the bytes from the `type` byte through the end
//! of the data payload (i.e. it covers `type`, the `log_num`/`synced` extension if
//! present, and the data).
//!
//! The recyclable and wal_sync formats embed the low 32 bits of the log file number so
//! that, when a log file is recycled (its space reused), stale chunks left over from a
//! previous use can be detected and treated as end-of-file.
//!
//! [`Reader`] transparently reads all three formats. [`Writer`] writes the legacy or
//! recyclable format (the wal_sync format is a durability optimization that Pebble can
//! read back as recyclable; we do not emit it).

use std::io::{Read, Write};

use crate::crc::masked_crc32c;
use crate::{Error, Result};

/// The fixed block size: 32 KiB. Every log file is a sequence of blocks of this size
/// (the final block may be shorter).
pub const BLOCK_SIZE: usize = 32 * 1024;

const LEGACY_HEADER_SIZE: usize = 7;
const RECYCLABLE_HEADER_SIZE: usize = 11;
const WAL_SYNC_HEADER_SIZE: usize = 19;

// Chunk encoding bytes. Part of the wire format; do not change.
const INVALID_CHUNK: u8 = 0;
const FULL_CHUNK: u8 = 1;
const FIRST_CHUNK: u8 = 2;
const MIDDLE_CHUNK: u8 = 3;
const LAST_CHUNK: u8 = 4;
const RECYCLABLE_FULL_CHUNK: u8 = 5;
const RECYCLABLE_FIRST_CHUNK: u8 = 6;
const RECYCLABLE_MIDDLE_CHUNK: u8 = 7;
const RECYCLABLE_LAST_CHUNK: u8 = 8;
const WAL_SYNC_FULL_CHUNK: u8 = 9;
const WAL_SYNC_FIRST_CHUNK: u8 = 10;
const WAL_SYNC_MIDDLE_CHUNK: u8 = 11;
const WAL_SYNC_LAST_CHUNK: u8 = 12;

/// Where a chunk sits within its logical record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Position {
    Full,
    First,
    Middle,
    Last,
}

/// The header layout of a chunk, decoded from its encoding byte.
#[derive(Debug, Clone, Copy)]
struct ChunkKind {
    position: Position,
    header_size: usize,
}

fn decode_encoding(enc: u8) -> Option<ChunkKind> {
    let (position, header_size) = match enc {
        FULL_CHUNK => (Position::Full, LEGACY_HEADER_SIZE),
        FIRST_CHUNK => (Position::First, LEGACY_HEADER_SIZE),
        MIDDLE_CHUNK => (Position::Middle, LEGACY_HEADER_SIZE),
        LAST_CHUNK => (Position::Last, LEGACY_HEADER_SIZE),
        RECYCLABLE_FULL_CHUNK => (Position::Full, RECYCLABLE_HEADER_SIZE),
        RECYCLABLE_FIRST_CHUNK => (Position::First, RECYCLABLE_HEADER_SIZE),
        RECYCLABLE_MIDDLE_CHUNK => (Position::Middle, RECYCLABLE_HEADER_SIZE),
        RECYCLABLE_LAST_CHUNK => (Position::Last, RECYCLABLE_HEADER_SIZE),
        WAL_SYNC_FULL_CHUNK => (Position::Full, WAL_SYNC_HEADER_SIZE),
        WAL_SYNC_FIRST_CHUNK => (Position::First, WAL_SYNC_HEADER_SIZE),
        WAL_SYNC_MIDDLE_CHUNK => (Position::Middle, WAL_SYNC_HEADER_SIZE),
        WAL_SYNC_LAST_CHUNK => (Position::Last, WAL_SYNC_HEADER_SIZE),
        _ => return None,
    };
    Some(ChunkKind {
        position,
        header_size,
    })
}

/// Whether the chunk carries an embedded log number (offset 7..11).
fn has_log_num(header_size: usize) -> bool {
    header_size >= RECYCLABLE_HEADER_SIZE
}

/// The wire format a [`Writer`] emits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// The 7-byte legacy format, used for the MANIFEST and non-recycled logs.
    Legacy,
    /// The 11-byte recyclable format, carrying the low 32 bits of the given log number.
    Recyclable(u32),
}

impl Format {
    fn header_size(self) -> usize {
        match self {
            Format::Legacy => LEGACY_HEADER_SIZE,
            Format::Recyclable(_) => RECYCLABLE_HEADER_SIZE,
        }
    }

    /// The encoding byte for a chunk at the given position in this format.
    fn encoding(self, position: Position) -> u8 {
        match self {
            Format::Legacy => match position {
                Position::Full => FULL_CHUNK,
                Position::First => FIRST_CHUNK,
                Position::Middle => MIDDLE_CHUNK,
                Position::Last => LAST_CHUNK,
            },
            Format::Recyclable(_) => match position {
                Position::Full => RECYCLABLE_FULL_CHUNK,
                Position::First => RECYCLABLE_FIRST_CHUNK,
                Position::Middle => RECYCLABLE_MIDDLE_CHUNK,
                Position::Last => RECYCLABLE_LAST_CHUNK,
            },
        }
    }
}

/// Reads logical records from a log file, transparently handling fragmentation across
/// blocks and all three wire formats.
///
/// Construct with [`Reader::new`], passing the log file's number (the low 32 bits) so
/// that recyclable/wal_sync chunks from an older use of a recycled file are recognized
/// as end-of-file. For the MANIFEST or any legacy-format log, pass `0`.
pub struct Reader<R> {
    inner: R,
    log_num: u32,
    block: Box<[u8; BLOCK_SIZE]>,
    /// Number of valid bytes in `block` (less than `BLOCK_SIZE` only for the last block).
    block_len: usize,
    /// Read cursor within `block`.
    pos: usize,
    /// Whether any block has been loaded yet.
    started: bool,
}

impl<R: Read> Reader<R> {
    /// Creates a reader over `inner`. `log_num` is the low 32 bits of this log file's
    /// number, used to reject stale chunks in recycled files; pass `0` for the MANIFEST.
    pub fn new(inner: R, log_num: u32) -> Self {
        Reader {
            inner,
            log_num,
            block: Box::new([0u8; BLOCK_SIZE]),
            block_len: 0,
            pos: 0,
            started: false,
        }
    }

    /// Reads the next block from the underlying reader into `block`, returning whether
    /// any bytes were read (`false` means clean end-of-file).
    fn fill_block(&mut self) -> Result<bool> {
        let mut filled = 0;
        while filled < BLOCK_SIZE {
            match self.inner.read(&mut self.block[filled..]) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(Error::Io(e)),
            }
        }
        self.block_len = filled;
        self.pos = 0;
        self.started = true;
        Ok(filled > 0)
    }

    /// Reads the next logical record, returning `Ok(None)` at end of file.
    ///
    /// Returns [`Error::Corruption`] on a checksum mismatch, a chunk that overflows its
    /// block, or a truncated record at end of file.
    pub fn read_record(&mut self) -> Result<Option<Vec<u8>>> {
        let mut rec: Vec<u8> = Vec::new();
        let mut fragmented = false;

        loop {
            // Ensure at least a minimal (legacy) header is available in the current
            // block; otherwise advance to the next block. Trailing bytes smaller than a
            // header are zero-padding.
            if !self.started || self.block_len - self.pos < LEGACY_HEADER_SIZE {
                if !self.fill_block()? {
                    if fragmented {
                        return Err(Error::corruption("record: truncated record at EOF"));
                    }
                    return Ok(None);
                }
                continue;
            }

            let p = self.pos;
            let checksum = u32::from_le_bytes(self.block[p..p + 4].try_into().unwrap());
            let length = u16::from_le_bytes(self.block[p + 4..p + 6].try_into().unwrap()) as usize;
            let enc = self.block[p + 6];

            if enc == INVALID_CHUNK {
                // A zeroed chunk marks the end of meaningful data in this block (the
                // writer zero-pads a block when the next chunk does not fit). The rest
                // of the block must be all zeroes.
                if self.block[p..self.block_len].iter().any(|&b| b != 0) {
                    return Err(Error::corruption("record: non-zero data in zeroed chunk"));
                }
                if !self.fill_block()? {
                    if fragmented {
                        return Err(Error::corruption("record: truncated record at EOF"));
                    }
                    return Ok(None);
                }
                continue;
            }

            let kind = decode_encoding(enc).ok_or_else(|| {
                Error::corruption(format!("record: invalid chunk encoding {enc}"))
            })?;
            let header_size = kind.header_size;

            if self.block_len - p < header_size + length {
                return Err(Error::corruption("record: chunk overflows block"));
            }

            if has_log_num(header_size) {
                let log_num = u32::from_le_bytes(self.block[p + 7..p + 11].try_into().unwrap());
                if log_num != self.log_num {
                    // A chunk from an older use of this recycled file: treat as EOF.
                    if fragmented {
                        return Err(Error::corruption("record: log number mismatch mid-record"));
                    }
                    return Ok(None);
                }
            }

            // The checksum covers the type byte through the end of the data payload.
            let crc_input = &self.block[p + 6..p + header_size + length];
            if masked_crc32c(crc_input) != checksum {
                return Err(Error::corruption("record: checksum mismatch"));
            }

            let data = &self.block[p + header_size..p + header_size + length];
            self.pos = p + header_size + length;

            match kind.position {
                Position::Full => {
                    if fragmented {
                        return Err(Error::corruption(
                            "record: unexpected full chunk mid-record",
                        ));
                    }
                    rec.extend_from_slice(data);
                    return Ok(Some(rec));
                }
                Position::First => {
                    if fragmented {
                        return Err(Error::corruption(
                            "record: unexpected first chunk mid-record",
                        ));
                    }
                    rec.extend_from_slice(data);
                    fragmented = true;
                }
                Position::Middle => {
                    if !fragmented {
                        return Err(Error::corruption("record: middle chunk without first"));
                    }
                    rec.extend_from_slice(data);
                }
                Position::Last => {
                    if !fragmented {
                        return Err(Error::corruption("record: last chunk without first"));
                    }
                    rec.extend_from_slice(data);
                    return Ok(Some(rec));
                }
            }
        }
    }
}

impl<R: Read> Iterator for Reader<R> {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read_record().transpose()
    }
}

/// Writes logical records to a log file, fragmenting each record across 32 KiB blocks
/// as needed.
///
/// Records written with [`Writer::write_record`] are accumulated into the current block
/// and flushed to the underlying writer as blocks fill; [`Writer::flush`] writes any
/// buffered tail, and [`Writer::finish`] flushes and returns the inner writer.
pub struct Writer<W> {
    inner: W,
    format: Format,
    block: Box<[u8; BLOCK_SIZE]>,
    /// Write cursor within the current block.
    block_pos: usize,
    /// Bytes of the current block already written to `inner`.
    flushed_pos: usize,
    /// Index of the current block, for computing record offsets.
    block_num: u64,
}

impl<W: Write> Writer<W> {
    /// Creates a writer emitting the legacy (7-byte header) format, as used for the
    /// MANIFEST.
    pub fn new(inner: W) -> Self {
        Self::with_format(inner, Format::Legacy)
    }

    /// Creates a writer emitting the recyclable (11-byte header) format carrying the low
    /// 32 bits of `log_num`, as used for write-ahead logs.
    pub fn with_log_num(inner: W, log_num: u32) -> Self {
        Self::with_format(inner, Format::Recyclable(log_num))
    }

    /// Creates a writer emitting the given [`Format`].
    pub fn with_format(inner: W, format: Format) -> Self {
        Writer {
            inner,
            format,
            block: Box::new([0u8; BLOCK_SIZE]),
            block_pos: 0,
            flushed_pos: 0,
            block_num: 0,
        }
    }

    /// Writes `data` as one logical record and returns the file offset at which it
    /// begins. The record is not necessarily durable until [`Writer::flush`] or
    /// [`Writer::finish`] is called.
    pub fn write_record(&mut self, mut data: &[u8]) -> Result<u64> {
        let header_size = self.format.header_size();
        let start_offset = self.block_num * BLOCK_SIZE as u64 + self.block_pos as u64;
        let mut first = true;

        loop {
            if BLOCK_SIZE - self.block_pos < header_size {
                // Not enough room for even a header: zero-pad the rest and flush.
                for b in &mut self.block[self.block_pos..BLOCK_SIZE] {
                    *b = 0;
                }
                self.block_pos = BLOCK_SIZE;
                self.flush_full_block()?;
            }

            let avail = BLOCK_SIZE - self.block_pos - header_size;
            let chunk_len = avail.min(data.len());
            let last = chunk_len == data.len();
            let position = match (first, last) {
                (true, true) => Position::Full,
                (true, false) => Position::First,
                (false, true) => Position::Last,
                (false, false) => Position::Middle,
            };

            let p = self.block_pos;
            self.block[p + 4..p + 6].copy_from_slice(&(chunk_len as u16).to_le_bytes());
            self.block[p + 6] = self.format.encoding(position);
            if let Format::Recyclable(log_num) = self.format {
                self.block[p + 7..p + 11].copy_from_slice(&log_num.to_le_bytes());
            }
            self.block[p + header_size..p + header_size + chunk_len]
                .copy_from_slice(&data[..chunk_len]);

            let checksum = masked_crc32c(&self.block[p + 6..p + header_size + chunk_len]);
            self.block[p..p + 4].copy_from_slice(&checksum.to_le_bytes());

            self.block_pos += header_size + chunk_len;
            data = &data[chunk_len..];
            first = false;

            if self.block_pos == BLOCK_SIZE {
                self.flush_full_block()?;
            }
            if last {
                break;
            }
        }

        Ok(start_offset)
    }

    /// Writes the completed current block (`flushed_pos..BLOCK_SIZE`) to the inner
    /// writer and starts a new block.
    fn flush_full_block(&mut self) -> Result<()> {
        self.inner
            .write_all(&self.block[self.flushed_pos..BLOCK_SIZE])?;
        self.block_pos = 0;
        self.flushed_pos = 0;
        self.block_num += 1;
        Ok(())
    }

    /// Writes any buffered bytes of the current (partial) block to the inner writer and
    /// flushes it, without ending the block. Subsequent records continue in the same
    /// block.
    pub fn flush(&mut self) -> Result<()> {
        if self.block_pos > self.flushed_pos {
            self.inner
                .write_all(&self.block[self.flushed_pos..self.block_pos])?;
            self.flushed_pos = self.block_pos;
        }
        self.inner.flush()?;
        Ok(())
    }

    /// Flushes any buffered data and returns the inner writer.
    pub fn finish(mut self) -> Result<W> {
        self.flush()?;
        Ok(self.inner)
    }
}

impl<W: crate::vfs::WritableFile> Writer<W> {
    /// Flushes buffered data and fsyncs the underlying file, making all records written
    /// so far durable. Used by the WAL and MANIFEST when synchronous commits are enabled.
    pub fn sync_all(&mut self) -> Result<()> {
        self.flush()?;
        self.inner.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn roundtrip(format: Format, log_num: u32, records: &[&[u8]]) {
        let mut w = Writer::with_format(Vec::new(), format);
        for r in records {
            w.write_record(r).unwrap();
        }
        let buf = w.finish().unwrap();

        let mut r = Reader::new(Cursor::new(buf), log_num);
        for expected in records {
            let got = r.read_record().unwrap().expect("record");
            assert_eq!(&got, expected);
        }
        assert!(r.read_record().unwrap().is_none(), "expected EOF");
    }

    #[test]
    fn legacy_header_layout_is_exact() {
        let mut w = Writer::new(Vec::new());
        w.write_record(b"hello").unwrap();
        let buf = w.finish().unwrap();

        // checksum(4) | length(2) | type(1) | data
        assert_eq!(buf.len(), LEGACY_HEADER_SIZE + 5);
        assert_eq!(&buf[4..6], &5u16.to_le_bytes()); // length
        assert_eq!(buf[6], FULL_CHUNK); // type
        assert_eq!(&buf[7..], b"hello"); // data
        let want = masked_crc32c(&buf[6..12]); // type byte through data
        assert_eq!(u32::from_le_bytes(buf[0..4].try_into().unwrap()), want);
    }

    #[test]
    fn recyclable_header_layout_is_exact() {
        let mut w = Writer::with_log_num(Vec::new(), 0xABCD_1234);
        w.write_record(b"hi").unwrap();
        let buf = w.finish().unwrap();

        assert_eq!(buf.len(), RECYCLABLE_HEADER_SIZE + 2);
        assert_eq!(&buf[4..6], &2u16.to_le_bytes());
        assert_eq!(buf[6], RECYCLABLE_FULL_CHUNK);
        assert_eq!(&buf[7..11], &0xABCD_1234u32.to_le_bytes()); // log number
        assert_eq!(&buf[11..], b"hi");
        let want = masked_crc32c(&buf[6..13]); // type + log_num + data
        assert_eq!(u32::from_le_bytes(buf[0..4].try_into().unwrap()), want);
    }

    #[test]
    fn roundtrip_small_records_legacy() {
        roundtrip(
            Format::Legacy,
            0,
            &[b"", b"a", b"hello", b"world", &[0u8; 100]],
        );
    }

    #[test]
    fn roundtrip_small_records_recyclable() {
        roundtrip(Format::Recyclable(7), 7, &[b"alpha", b"beta", b"gamma"]);
    }

    #[test]
    fn roundtrip_record_spanning_many_blocks() {
        // A 100 KiB record forces fragmentation across ~4 blocks.
        let big: Vec<u8> = (0..100_000).map(|i| (i % 251) as u8).collect();
        for format in [Format::Legacy, Format::Recyclable(1)] {
            let log_num = if let Format::Recyclable(n) = format {
                n
            } else {
                0
            };
            let mut w = Writer::with_format(Vec::new(), format);
            w.write_record(b"before").unwrap();
            w.write_record(&big).unwrap();
            w.write_record(b"after").unwrap();
            let buf = w.finish().unwrap();
            // The output is block-aligned except for the final partial block.
            assert!(buf.len() > BLOCK_SIZE * 3);

            let mut r = Reader::new(Cursor::new(buf), log_num);
            assert_eq!(r.read_record().unwrap().unwrap(), b"before");
            assert_eq!(r.read_record().unwrap().unwrap(), big);
            assert_eq!(r.read_record().unwrap().unwrap(), b"after");
            assert!(r.read_record().unwrap().is_none());
        }
    }

    #[test]
    fn record_filling_block_exactly_then_more() {
        // A record whose data is exactly one block minus a header should occupy the
        // first block precisely, and the next record begins in a fresh block.
        let exact = vec![0x5au8; BLOCK_SIZE - LEGACY_HEADER_SIZE];
        let mut w = Writer::new(Vec::new());
        w.write_record(&exact).unwrap();
        w.write_record(b"next").unwrap();
        let buf = w.finish().unwrap();
        assert_eq!(buf.len(), BLOCK_SIZE + LEGACY_HEADER_SIZE + 4);

        let mut r = Reader::new(Cursor::new(buf), 0);
        assert_eq!(r.read_record().unwrap().unwrap(), exact);
        assert_eq!(r.read_record().unwrap().unwrap(), b"next");
        assert!(r.read_record().unwrap().is_none());
    }

    #[test]
    fn recyclable_wrong_log_num_reads_as_eof() {
        let mut w = Writer::with_log_num(Vec::new(), 5);
        w.write_record(b"stale").unwrap();
        let buf = w.finish().unwrap();
        // Opening with a different log number treats the stale chunk as end-of-file.
        let mut r = Reader::new(Cursor::new(buf), 6);
        assert!(r.read_record().unwrap().is_none());
    }

    #[test]
    fn checksum_corruption_is_detected() {
        let mut w = Writer::new(Vec::new());
        w.write_record(b"important data").unwrap();
        let mut buf = w.finish().unwrap();
        let last = buf.len() - 1;
        buf[last] ^= 0xff; // flip a data byte
        let mut r = Reader::new(Cursor::new(buf), 0);
        match r.read_record() {
            Err(Error::Corruption(_)) => {}
            other => panic!("expected corruption, got {other:?}"),
        }
    }

    #[test]
    fn iterator_yields_all_records() {
        let mut w = Writer::new(Vec::new());
        for r in [b"one".as_slice(), b"two", b"three"] {
            w.write_record(r).unwrap();
        }
        let buf = w.finish().unwrap();
        let got: Vec<Vec<u8>> = Reader::new(Cursor::new(buf), 0)
            .collect::<Result<_>>()
            .unwrap();
        assert_eq!(
            got,
            vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()]
        );
    }
}
