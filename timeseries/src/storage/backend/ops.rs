//! Plain storage data types for the native SlateDB backend.
//!
//! These are timeseries-owned copies of the value types that previously came
//! from `common::storage`. They carry no SlateDB types themselves; the
//! conversions to SlateDB equivalents live in [`super::slate`].

use bytes::Bytes;
use uuid::Uuid;

/// Identifies a checkpoint of the storage backend at a point in time.
///
/// Checkpoints capture a manifest snapshot that a reader can later open
/// against to get a consistent view of the database at the time the
/// checkpoint was created.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointInfo {
    pub id: Uuid,
    pub manifest_id: u64,
}

/// Time-to-live for a written record.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub enum Ttl {
    #[default]
    Default,
    NoExpiry,
    ExpireAfter(u64),
    ExpireAt(i64),
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct PutOptions {
    pub ttl: Ttl,
}

/// A record being put along with options specific to the put.
#[derive(Clone, Debug)]
pub struct PutRecordOp {
    pub record: Record,
    pub options: PutOptions,
}

impl PutRecordOp {
    pub fn new(record: Record) -> Self {
        Self {
            record,
            options: PutOptions::default(),
        }
    }

    pub fn new_with_options(record: Record, options: PutOptions) -> Self {
        Self { record, options }
    }

    pub fn with_options(self, options: PutOptions) -> Self {
        Self {
            record: self.record,
            options,
        }
    }
}

impl From<Record> for PutRecordOp {
    fn from(record: Record) -> Self {
        Self::new(record)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MergeOptions {
    pub ttl: Ttl,
}

/// A record written as part of a merge op along with options specific to the merge.
#[derive(Clone, Debug)]
pub struct MergeRecordOp {
    pub record: Record,
    pub options: MergeOptions,
}

impl MergeRecordOp {
    pub fn new(record: Record) -> Self {
        Self {
            record,
            options: MergeOptions::default(),
        }
    }

    pub fn new_with_ttl(record: Record, options: MergeOptions) -> Self {
        Self { record, options }
    }
}

impl From<Record> for MergeRecordOp {
    fn from(record: Record) -> Self {
        Self::new(record)
    }
}

/// A key/value pair read from or written to storage.
#[derive(Clone, Debug)]
pub struct Record {
    pub key: Bytes,
    pub value: Bytes,
}

impl Record {
    pub fn new(key: Bytes, value: Bytes) -> Self {
        Self { key, value }
    }

    pub fn empty(key: Bytes) -> Self {
        Self::new(key, Bytes::new())
    }
}

/// A single operation in an atomic batch.
#[derive(Clone, Debug)]
pub enum RecordOp {
    Put(PutRecordOp),
    Merge(MergeRecordOp),
    Delete(Bytes),
}

/// Options controlling the durability behavior of a write.
#[derive(Debug, Clone, Default)]
pub struct WriteOptions {
    /// When `true`, the operation does not return until the data has been
    /// persisted to durable storage. When `false` (the default), it returns as
    /// soon as the data is in memory.
    pub await_durable: bool,
}

/// Result of a write, carrying the sequence number assigned by SlateDB.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriteResult {
    pub seqnum: u64,
}

/// Error type for storage operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageError {
    /// Storage-related errors (e.g. SlateDB I/O).
    Storage(String),
    /// Internal errors.
    Internal(String),
}

impl std::error::Error for StorageError {}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            StorageError::Storage(msg) => write!(f, "Storage error: {}", msg),
            StorageError::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl StorageError {
    /// Converts any displayable error into [`StorageError::Storage`].
    pub fn from_storage(e: impl std::fmt::Display) -> Self {
        StorageError::Storage(e.to_string())
    }
}

/// Result type alias for storage operations.
pub type StorageResult<T> = std::result::Result<T, StorageError>;
