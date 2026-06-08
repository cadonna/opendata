//! Native SlateDB-backed storage for timeseries.
//!
//! [`SlateDbStorage`] owns a `slatedb::Db` and exposes reads via [`TsRead`] and
//! writes via inherent methods (the writer is always concrete, so no write
//! trait is needed). [`SlateDbStorageSnapshot`] and [`SlateDbStorageReader`]
//! wrap `DbSnapshot` and `DbReader` and provide read-only access via [`TsRead`].
//!
//! Value types (`Record`, `RecordOp`, `Ttl`, …), the iterator trait, the error
//! type, and the `Ttl`/options → SlateDB conversions are all reused from
//! `common::storage`; only the storage *handles* are native here.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use common::BytesRange;
use common::storage::{
    CheckpointInfo, MergeRecordOp, PutRecordOp, Record, RecordOp, StorageError, StorageIterator,
    StorageResult, WriteOptions, WriteResult,
};
use slatedb::IterationOrder;
use slatedb::config::{CheckpointOptions, CheckpointScope, ScanOptions};
use slatedb::{
    Db, DbIterator, DbReader, DbSnapshot, WriteBatch, config::WriteOptions as SlateDbWriteOptions,
};

use super::traits::{TsRead, TsSnapshot};

/// Returns the default scan options used for storage scans.
fn default_scan_options() -> ScanOptions {
    ScanOptions {
        durability_filter: Default::default(),
        dirty: false,
        read_ahead_bytes: 1024 * 1024,
        cache_blocks: true,
        max_fetch_tasks: 4,
        order: IterationOrder::Ascending,
        filter_context: None,
    }
}

/// SlateDB-backed storage.
///
/// SlateDB is an embedded key-value store built on object storage, providing
/// LSM-tree semantics with cloud-native durability.
pub(crate) struct SlateDbStorage {
    pub(crate) db: Arc<Db>,
}

impl SlateDbStorage {
    /// Creates a new `SlateDbStorage` wrapping the given SlateDB database.
    pub(crate) fn new(db: Arc<Db>) -> Self {
        Self { db }
    }

    /// Applies a batch of mixed operations atomically with default options
    /// (`await_durable: false`).
    pub(crate) async fn apply(&self, ops: Vec<RecordOp>) -> StorageResult<WriteResult> {
        self.apply_with_options(ops, WriteOptions::default()).await
    }

    /// Applies a batch of mixed operations atomically with custom options.
    pub(crate) async fn apply_with_options(
        &self,
        records: Vec<RecordOp>,
        options: WriteOptions,
    ) -> StorageResult<WriteResult> {
        let mut batch = WriteBatch::new();
        for op in records {
            match op {
                RecordOp::Put(op) => {
                    batch.put_with_options(op.record.key, op.record.value, &op.options.into())
                }
                RecordOp::Merge(op) => {
                    batch.merge_with_options(op.record.key, op.record.value, &op.options.into())
                }
                RecordOp::Delete(key) => batch.delete(key),
            }
        }
        self.write_batch(batch, options).await
    }

    /// Writes records with default options (`await_durable: false`).
    pub(crate) async fn put(&self, records: Vec<PutRecordOp>) -> StorageResult<WriteResult> {
        self.put_with_options(records, WriteOptions::default())
            .await
    }

    /// Writes records with custom options controlling durability.
    pub(crate) async fn put_with_options(
        &self,
        records: Vec<PutRecordOp>,
        options: WriteOptions,
    ) -> StorageResult<WriteResult> {
        let mut batch = WriteBatch::new();
        for op in records {
            batch.put_with_options(op.record.key, op.record.value, &op.options.into());
        }
        self.write_batch(batch, options).await
    }

    /// Merges records using the configured merge operator, default options.
    pub(crate) async fn merge(&self, records: Vec<MergeRecordOp>) -> StorageResult<WriteResult> {
        self.merge_with_options(records, WriteOptions::default())
            .await
    }

    /// Merges records with custom options. Returns a clear error if no merge
    /// operator is configured on the database.
    pub(crate) async fn merge_with_options(
        &self,
        records: Vec<MergeRecordOp>,
        options: WriteOptions,
    ) -> StorageResult<WriteResult> {
        let mut batch = WriteBatch::new();
        for op in records {
            batch.merge_with_options(op.record.key, op.record.value, &op.options.into());
        }
        let slate_options = SlateDbWriteOptions {
            await_durable: options.await_durable,
            ..SlateDbWriteOptions::default()
        };
        let write_handle = self
            .db
            .write_with_options(batch, &slate_options)
            .await
            .map_err(|e| {
                let error_msg = e.to_string();
                if error_msg.contains("merge operator") || error_msg.contains("not configured") {
                    StorageError::Storage(
                        "Merge operator not configured for this database".to_string(),
                    )
                } else {
                    StorageError::from_storage(e)
                }
            })?;
        Ok(WriteResult {
            seqnum: write_handle.seqnum(),
        })
    }

    async fn write_batch(
        &self,
        batch: WriteBatch,
        options: WriteOptions,
    ) -> StorageResult<WriteResult> {
        let slate_options = SlateDbWriteOptions {
            await_durable: options.await_durable,
            ..SlateDbWriteOptions::default()
        };
        let write_handle = self
            .db
            .write_with_options(batch, &slate_options)
            .await
            .map_err(StorageError::from_storage)?;
        Ok(WriteResult {
            seqnum: write_handle.seqnum(),
        })
    }

    /// Creates a point-in-time snapshot for consistent reads.
    pub(crate) async fn snapshot(&self) -> StorageResult<Arc<dyn TsSnapshot>> {
        let snapshot = self
            .db
            .snapshot()
            .await
            .map_err(StorageError::from_storage)?;
        Ok(Arc::new(SlateDbStorageSnapshot { snapshot }))
    }

    /// Flushes pending writes to durable storage.
    pub(crate) async fn flush(&self) -> StorageResult<()> {
        self.db.flush().await.map_err(StorageError::from_storage)?;
        Ok(())
    }

    /// Creates a durable checkpoint covering all data.
    pub(crate) async fn create_checkpoint(&self) -> StorageResult<CheckpointInfo> {
        let result = self
            .db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .map_err(StorageError::from_storage)?;
        Ok(CheckpointInfo {
            id: result.id,
            manifest_id: result.manifest_id,
        })
    }
}

#[async_trait]
impl TsRead for SlateDbStorage {
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get(&self, key: Bytes) -> StorageResult<Option<Record>> {
        let value = self
            .db
            .get(&key)
            .await
            .map_err(StorageError::from_storage)?;
        match value {
            Some(v) => Ok(Some(Record::new(key, v))),
            None => Ok(None),
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan_iter(
        &self,
        range: BytesRange,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        let iter = self
            .db
            .scan_with_options(range, &default_scan_options())
            .await
            .map_err(StorageError::from_storage)?;
        Ok(Box::new(SlateDbIterator { iter }))
    }

    async fn close(&self) -> StorageResult<()> {
        self.db.close().await.map_err(StorageError::from_storage)?;
        Ok(())
    }
}

pub(crate) struct SlateDbIterator {
    iter: DbIterator,
}

#[async_trait]
impl StorageIterator for SlateDbIterator {
    #[tracing::instrument(level = "trace", skip_all)]
    async fn next(&mut self) -> StorageResult<Option<Record>> {
        match self.iter.next().await.map_err(StorageError::from_storage)? {
            Some(entry) => Ok(Some(Record::new(entry.key, entry.value))),
            None => Ok(None),
        }
    }
}

/// SlateDB snapshot wrapper providing a consistent read-only view.
pub(crate) struct SlateDbStorageSnapshot {
    snapshot: Arc<DbSnapshot>,
}

#[async_trait]
impl TsRead for SlateDbStorageSnapshot {
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get(&self, key: Bytes) -> StorageResult<Option<Record>> {
        let value = self
            .snapshot
            .get(&key)
            .await
            .map_err(StorageError::from_storage)?;
        match value {
            Some(v) => Ok(Some(Record::new(key, v))),
            None => Ok(None),
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan_iter(
        &self,
        range: BytesRange,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        let iter = self
            .snapshot
            .scan_with_options(range, &default_scan_options())
            .await
            .map_err(StorageError::from_storage)?;
        Ok(Box::new(SlateDbIterator { iter }))
    }
}

impl TsSnapshot for SlateDbStorageSnapshot {}

/// Read-only SlateDB storage using `DbReader`.
///
/// Provides read-only access without fencing, so multiple readers can coexist
/// with a single writer.
pub(crate) struct SlateDbStorageReader {
    reader: Arc<DbReader>,
}

impl SlateDbStorageReader {
    /// Creates a new reader wrapping the given `DbReader`.
    pub(crate) fn new(reader: Arc<DbReader>) -> Self {
        Self { reader }
    }
}

#[async_trait]
impl TsRead for SlateDbStorageReader {
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get(&self, key: Bytes) -> StorageResult<Option<Record>> {
        let value = self
            .reader
            .get(&key)
            .await
            .map_err(StorageError::from_storage)?;
        match value {
            Some(v) => Ok(Some(Record::new(key, v))),
            None => Ok(None),
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan_iter(
        &self,
        range: BytesRange,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        let iter = self
            .reader
            .scan_with_options(range, &default_scan_options())
            .await
            .map_err(StorageError::from_storage)?;
        Ok(Box::new(SlateDbIterator { iter }))
    }

    async fn close(&self) -> StorageResult<()> {
        self.reader
            .close()
            .await
            .map_err(StorageError::from_storage)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::storage::{MergeOptions, PutOptions, Ttl};
    use slatedb::DbBuilder;
    use slatedb::config::Settings;
    use slatedb::object_store::memory::InMemory;
    use slatedb::{MergeOperator as SlateDbMergeOperator, MergeOperatorError};
    use slatedb_common::clock::MockSystemClock;

    #[tokio::test]
    async fn should_read_data_written_by_storage_via_reader() {
        let object_store = Arc::new(InMemory::new());
        let path = "/test/db";

        let db = DbBuilder::new(path, object_store.clone())
            .build()
            .await
            .unwrap();
        let storage = SlateDbStorage::new(Arc::new(db));

        storage
            .put(vec![
                Record::new(Bytes::from("key1"), Bytes::from("value1")).into(),
                Record::new(Bytes::from("key2"), Bytes::from("value2")).into(),
            ])
            .await
            .unwrap();
        storage.flush().await.unwrap();

        let reader = DbReader::builder(path, object_store.clone())
            .build()
            .await
            .unwrap();
        let storage_reader = SlateDbStorageReader::new(Arc::new(reader));

        let record = storage_reader.get(Bytes::from("key1")).await.unwrap();
        assert_eq!(record.unwrap().value, Bytes::from("value1"));
        let record = storage_reader.get(Bytes::from("key2")).await.unwrap();
        assert_eq!(record.unwrap().value, Bytes::from("value2"));
        let record = storage_reader.get(Bytes::from("key3")).await.unwrap();
        assert!(record.is_none());

        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_scan_data_written_by_storage_via_reader() {
        let object_store = Arc::new(InMemory::new());
        let path = "/test/db";

        let db = DbBuilder::new(path, object_store.clone())
            .build()
            .await
            .unwrap();
        let storage = SlateDbStorage::new(Arc::new(db));

        storage
            .put(vec![
                Record::new(Bytes::from("a"), Bytes::from("1")).into(),
                Record::new(Bytes::from("b"), Bytes::from("2")).into(),
                Record::new(Bytes::from("c"), Bytes::from("3")).into(),
            ])
            .await
            .unwrap();
        storage.flush().await.unwrap();

        let reader = DbReader::builder(path, object_store.clone())
            .build()
            .await
            .unwrap();
        let storage_reader = SlateDbStorageReader::new(Arc::new(reader));

        let mut iter = storage_reader
            .scan_iter(BytesRange::unbounded())
            .await
            .unwrap();
        let mut results = Vec::new();
        while let Some(record) = iter.next().await.unwrap() {
            results.push((record.key, record.value));
        }

        assert_eq!(results.len(), 3);
        assert_eq!(results[0], (Bytes::from("a"), Bytes::from("1")));
        assert_eq!(results[1], (Bytes::from("b"), Bytes::from("2")));
        assert_eq!(results[2], (Bytes::from("c"), Bytes::from("3")));

        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_set_expire_ts_based_on_ttl() {
        let object_store = Arc::new(InMemory::new());
        let path = "/test/ttl_db";
        let clock = Arc::new(MockSystemClock::new());

        let db = DbBuilder::new(path, object_store.clone())
            .with_settings(Settings {
                default_ttl: Some(30_000),
                ..Default::default()
            })
            .with_system_clock(clock.clone())
            .build()
            .await
            .unwrap();
        let storage = SlateDbStorage::new(Arc::new(db));

        storage
            .put(vec![
                PutRecordOp::new_with_options(
                    Record::new(Bytes::from("key1"), Bytes::from("value1")),
                    PutOptions {
                        ttl: Ttl::ExpireAfter(20_000),
                    },
                ),
                PutRecordOp::new_with_options(
                    Record::new(Bytes::from("key2"), Bytes::from("value2")),
                    PutOptions { ttl: Ttl::Default },
                ),
                PutRecordOp::new_with_options(
                    Record::new(Bytes::from("key3"), Bytes::from("value3")),
                    PutOptions { ttl: Ttl::NoExpiry },
                ),
            ])
            .await
            .unwrap();

        let kv1 = storage.db.get_key_value(b"key1").await.unwrap().unwrap();
        assert_eq!(kv1.expire_ts, Some(20_000));
        let kv2 = storage.db.get_key_value(b"key2").await.unwrap().unwrap();
        assert_eq!(kv2.expire_ts, Some(30_000));
        let kv3 = storage.db.get_key_value(b"key3").await.unwrap().unwrap();
        assert_eq!(kv3.expire_ts, None);

        storage.close().await.unwrap();
    }

    /// Simple merge operator that concatenates existing and new values.
    /// Implements SlateDB's `MergeOperator` directly (test-only).
    struct ConcatMergeOperator;

    impl SlateDbMergeOperator for ConcatMergeOperator {
        fn merge(
            &self,
            _key: &Bytes,
            existing_value: Option<Bytes>,
            value: Bytes,
        ) -> Result<Bytes, MergeOperatorError> {
            let mut result = existing_value.unwrap_or_default().to_vec();
            result.extend_from_slice(&value);
            Ok(Bytes::from(result))
        }

        fn merge_batch(
            &self,
            _key: &Bytes,
            existing_value: Option<Bytes>,
            operands: &[Bytes],
        ) -> Result<Bytes, MergeOperatorError> {
            if operands.is_empty() && existing_value.is_none() {
                return Err(MergeOperatorError::EmptyBatch);
            }
            let mut result = existing_value.unwrap_or_default().to_vec();
            for operand in operands {
                result.extend_from_slice(operand);
            }
            Ok(Bytes::from(result))
        }
    }

    #[tokio::test]
    async fn should_set_expire_ts_on_merge_records_based_on_ttl() {
        let object_store = Arc::new(InMemory::new());
        let path = "/test/merge_ttl_db";
        let clock = Arc::new(MockSystemClock::new());

        let db = DbBuilder::new(path, object_store.clone())
            .with_settings(Settings {
                default_ttl: Some(30_000),
                ..Default::default()
            })
            .with_system_clock(clock.clone())
            .with_merge_operator(Arc::new(ConcatMergeOperator))
            .build()
            .await
            .unwrap();
        let storage = SlateDbStorage::new(Arc::new(db));

        storage
            .merge(vec![
                MergeRecordOp::new_with_ttl(
                    Record::new(Bytes::from("key1"), Bytes::from("v1")),
                    MergeOptions {
                        ttl: Ttl::ExpireAfter(20_000),
                    },
                ),
                MergeRecordOp::new_with_ttl(
                    Record::new(Bytes::from("key2"), Bytes::from("v2")),
                    MergeOptions { ttl: Ttl::Default },
                ),
                MergeRecordOp::new_with_ttl(
                    Record::new(Bytes::from("key3"), Bytes::from("v3")),
                    MergeOptions { ttl: Ttl::NoExpiry },
                ),
            ])
            .await
            .unwrap();

        let kv1 = storage.db.get_key_value(b"key1").await.unwrap().unwrap();
        assert_eq!(kv1.value, Bytes::from("v1"));
        assert_eq!(kv1.expire_ts, Some(20_000));
        let kv2 = storage.db.get_key_value(b"key2").await.unwrap().unwrap();
        assert_eq!(kv2.value, Bytes::from("v2"));
        assert_eq!(kv2.expire_ts, Some(30_000));
        let kv3 = storage.db.get_key_value(b"key3").await.unwrap().unwrap();
        assert_eq!(kv3.value, Bytes::from("v3"));
        assert_eq!(kv3.expire_ts, None);

        storage.close().await.unwrap();
    }

    async fn reader_can_see(path: &str, object_store: Arc<InMemory>, key: &str) -> bool {
        let reader = DbReader::builder(path, object_store).build().await.unwrap();
        let storage_reader = SlateDbStorageReader::new(Arc::new(reader));
        storage_reader
            .get(Bytes::from(key.to_owned()))
            .await
            .unwrap()
            .is_some()
    }

    #[tokio::test]
    async fn put_defaults_to_not_await_durable() {
        let object_store = Arc::new(InMemory::new());
        let path = "/test/put_default_durability";

        let db = DbBuilder::new(path, object_store.clone())
            .build()
            .await
            .unwrap();
        let storage = SlateDbStorage::new(Arc::new(db));

        storage
            .put(vec![
                Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
            ])
            .await
            .unwrap();

        assert!(!reader_can_see(path, object_store.clone(), "k1").await);
        storage.flush().await.unwrap();
        assert!(reader_can_see(path, object_store.clone(), "k1").await);

        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn apply_with_await_durable_true_is_visible_to_reader() {
        let object_store = Arc::new(InMemory::new());
        let path = "/test/apply_durable";

        let db = DbBuilder::new(path, object_store.clone())
            .build()
            .await
            .unwrap();
        let storage = SlateDbStorage::new(Arc::new(db));

        storage
            .apply_with_options(
                vec![RecordOp::Put(
                    Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
                )],
                WriteOptions {
                    await_durable: true,
                },
            )
            .await
            .unwrap();

        assert!(reader_can_see(path, object_store.clone(), "k1").await);
        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn snapshot_sees_writes_made_before_it() {
        let object_store = Arc::new(InMemory::new());
        let path = "/test/snapshot";

        let db = DbBuilder::new(path, object_store.clone())
            .build()
            .await
            .unwrap();
        let storage = SlateDbStorage::new(Arc::new(db));

        storage
            .put(vec![
                Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
            ])
            .await
            .unwrap();

        let snapshot = storage.snapshot().await.unwrap();
        let record = snapshot.get(Bytes::from("k1")).await.unwrap();
        assert_eq!(record.unwrap().value, Bytes::from("v1"));

        storage.close().await.unwrap();
    }
}
