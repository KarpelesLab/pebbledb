// Copyright (c) 2024 The pebbledb (Rust port) Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

//! Crate-wide error type.

use std::fmt;

/// The result type returned throughout pebbledb.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors produced by pebbledb.
///
/// This enum is intentionally coarse-grained for now and will grow as more of the
/// engine is implemented. Lower layers attach context via [`Error::Corruption`] and
/// the [`std::io::Error`] passthrough.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// An underlying I/O operation failed.
    Io(std::io::Error),
    /// On-disk data did not match the expected format (bad magic, checksum mismatch,
    /// truncated record, …). The string describes what was being decoded.
    Corruption(String),
    /// A requested key was not found. Lookups that distinguish "absent" from other
    /// failures return this rather than an `Option` at the lower layers.
    NotFound,
    /// An operation was attempted that the current state does not allow (e.g. writing
    /// to a read-only database).
    InvalidState(String),
    /// A feature or code path that is not yet implemented in this port.
    Unsupported(&'static str),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "io error: {e}"),
            Error::Corruption(msg) => write!(f, "corruption: {msg}"),
            Error::NotFound => write!(f, "not found"),
            Error::InvalidState(msg) => write!(f, "invalid state: {msg}"),
            Error::Unsupported(what) => write!(f, "unsupported: {what}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl Error {
    /// Construct a [`Error::Corruption`] from anything string-like.
    pub fn corruption(msg: impl Into<String>) -> Self {
        Error::Corruption(msg.into())
    }
}
