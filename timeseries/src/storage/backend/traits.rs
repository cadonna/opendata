//! The minimal read abstraction that unifies the SlateDB read backends.
//!
//! This trait replaces `common::storage::StorageRead`/`StorageSnapshot` — the
//! storage *handles* timeseries no longer depends on. Everything else (record
//! and iterator types, ranges, errors) is reused from `common`.
//!
//! A trait is required because the per-bucket query reader (`MiniQueryReader`)
//! is fed by both a `DbSnapshot` (writer queries) and a `DbReader` (read-only
//! process), and SlateDB's own types share no common trait. Writes need no such
//! abstraction: the writer is always the concrete [`super::slate::SlateDbStorage`],
//! so its write methods are inherent.

use async_trait::async_trait;
use bytes::Bytes;
use common::BytesRange;
use common::storage::{Record, StorageIterator, StorageResult};

/// Read operations shared by every SlateDB read backend.
#[async_trait]
pub(crate) trait TsRead: Send + Sync {
    /// Retrieves a single record by exact key. Returns `Ok(None)` if absent.
    async fn get(&self, key: Bytes) -> StorageResult<Option<Record>>;

    /// Returns an owned iterator over records in the given range.
    async fn scan_iter(
        &self,
        range: BytesRange,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>>;

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
pub(crate) trait TsSnapshot: TsRead {}
