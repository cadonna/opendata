//! The minimal read abstraction that unifies the SlateDB read backends.
//!
//! Three concrete SlateDB types back reads in timeseries — a live `Db`
//! ([`super::slate::SlateDbStorage`]), a point-in-time `DbSnapshot`
//! ([`super::slate::SlateDbStorageSnapshot`]), and a read-only `DbReader`
//! ([`super::slate::SlateDbStorageReader`]). They share no common SlateDB
//! trait, and the per-bucket query reader (`MiniQueryReader`) is fed by both a
//! snapshot (writer queries) and a reader (read-only process). [`TsRead`] is
//! the local trait that lets those backends be used interchangeably; the
//! OpenTSDB loader methods are blanket-implemented over it.
//!
//! Writes need no such abstraction: in production the writer is always the
//! concrete `SlateDbStorage`, so its write methods are inherent.

use async_trait::async_trait;
use common::BytesRange;

use super::ops::{Record, StorageResult};

/// Iterator over storage records produced by [`TsRead::scan_iter`].
#[async_trait]
pub trait TsIterator {
    /// Returns the next record, or `Ok(None)` when exhausted.
    async fn next(&mut self) -> StorageResult<Option<Record>>;
}

/// Read operations shared by every SlateDB read backend.
#[async_trait]
pub trait TsRead: Send + Sync {
    /// Retrieves a single record by exact key. Returns `Ok(None)` if absent.
    async fn get(&self, key: bytes::Bytes) -> StorageResult<Option<Record>>;

    /// Returns an owned iterator over records in the given range.
    async fn scan_iter(
        &self,
        range: BytesRange,
    ) -> StorageResult<Box<dyn TsIterator + Send + 'static>>;

    /// Collects all records in the range into a `Vec`.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan(&self, range: BytesRange) -> StorageResult<Vec<Record>> {
        let mut iter = self.scan_iter(range).await?;
        let mut records = Vec::new();
        while let Some(record) = iter.next().await? {
            records.push(record);
        }
        Ok(records)
    }

    /// Releases backend resources. Default is a no-op.
    async fn close(&self) -> StorageResult<()> {
        Ok(())
    }
}

/// A consistent point-in-time read view. Marker over [`TsRead`].
pub trait TsSnapshot: TsRead {}
