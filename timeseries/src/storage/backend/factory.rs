//! Factory functions that build native SlateDB storage from configuration.
//!
//! Replaces the previous use of `common::storage`'s `StorageBuilder` /
//! `create_storage_read` (which yield the `Arc<dyn Storage>` handles timeseries
//! no longer uses) with two focused constructors — [`build_storage`] for the
//! read/write `Db` and [`build_reader`] for the read-only `DbReader`.
//!
//! The reusable SlateDB-specific plumbing — object-store creation, the foyer
//! block-cache builder, the metrics recorder, and the merge-operator adapter —
//! is reused from `common::storage`, not duplicated.

use std::sync::Arc;

use common::storage::config::{SlateDbStorageConfig, StorageConfig};
use common::storage::factory::{build_split_cache, create_object_store};
use common::storage::metrics_recorder::MetricsRsRecorder;
use common::storage::slate::SlateDbStorage as CommonSlateDbStorage;
use common::storage::{MergeOperator, StorageError, StorageResult};
use slatedb::config::Settings;
use slatedb::{DbBuilder, DbReader};
use tracing::info;
use uuid::Uuid;

use super::slate::{SlateDbStorage, SlateDbStorageReader};

/// Builds a read/write [`SlateDbStorage`] from configuration.
///
/// Registers the metrics recorder and, when provided, the merge operator. The
/// `InMemory` config variant is not supported (Design B uses SlateDb over an
/// in-memory object store for tests).
pub(crate) async fn build_storage(
    config: &StorageConfig,
    merge_operator: Option<Arc<dyn MergeOperator>>,
) -> StorageResult<SlateDbStorage> {
    let slate_config = require_slatedb(config)?;
    let object_store = create_object_store(&slate_config.object_store)?;
    let settings = load_settings(slate_config)?;
    info!(
        "create slatedb storage with config: {:?}, settings: {:?}",
        slate_config, settings
    );

    let mut db_builder = DbBuilder::new(slate_config.path.clone(), object_store)
        .with_settings(settings)
        .with_metrics_recorder(Arc::new(MetricsRsRecorder));

    if let Some(cache) =
        build_split_cache(&slate_config.block_cache, &slate_config.meta_cache).await?
    {
        db_builder = db_builder.with_db_cache(cache);
    }
    if let Some(op) = merge_operator {
        let adapter = CommonSlateDbStorage::merge_operator_adapter(op);
        db_builder = db_builder.with_merge_operator(Arc::new(adapter));
    }

    let db = db_builder
        .build()
        .await
        .map_err(|e| StorageError::Storage(format!("Failed to create SlateDB: {}", e)))?;
    Ok(SlateDbStorage::new(Arc::new(db)))
}

/// Builds a read-only [`SlateDbStorageReader`] from configuration.
///
/// Uses `DbReader`, which does not participate in fencing, so it can coexist
/// with a live writer. When `checkpoint_id` is set, the reader is pinned to that
/// checkpoint and does not advance with newer writes.
pub(crate) async fn build_reader(
    config: &StorageConfig,
    reader_options: slatedb::config::DbReaderOptions,
    checkpoint_id: Option<Uuid>,
    merge_operator: Option<Arc<dyn MergeOperator>>,
) -> StorageResult<SlateDbStorageReader> {
    let slate_config = require_slatedb(config)?;
    let object_store = create_object_store(&slate_config.object_store)?;

    let mut builder = DbReader::builder(slate_config.path.clone(), object_store)
        .with_options(reader_options)
        .with_metrics_recorder(Arc::new(MetricsRsRecorder));
    if let Some(checkpoint_id) = checkpoint_id {
        builder = builder.with_checkpoint_id(checkpoint_id);
    }
    if let Some(op) = merge_operator {
        let adapter = CommonSlateDbStorage::merge_operator_adapter(op);
        builder = builder.with_merge_operator(Arc::new(adapter));
    }

    if let Some(cache) =
        build_split_cache(&slate_config.block_cache, &slate_config.meta_cache).await?
    {
        builder = builder.with_db_cache(cache);
    }

    let reader = builder
        .build()
        .await
        .map_err(|e| StorageError::Storage(format!("Failed to create SlateDB reader: {}", e)))?;
    Ok(SlateDbStorageReader::new(Arc::new(reader)))
}

fn require_slatedb(config: &StorageConfig) -> StorageResult<&SlateDbStorageConfig> {
    match config {
        StorageConfig::SlateDb(slate_config) => Ok(slate_config),
        StorageConfig::InMemory => Err(StorageError::Storage(
            "InMemory storage is not supported by timeseries; use SlateDb with an in-memory \
             object store"
                .to_string(),
        )),
    }
}

fn load_settings(slate_config: &SlateDbStorageConfig) -> StorageResult<Settings> {
    match &slate_config.settings_path {
        Some(path) => Settings::from_file(path).map_err(|e| {
            StorageError::Storage(format!("Failed to load SlateDB settings from {}: {}", path, e))
        }),
        None => Ok(Settings::load().unwrap_or_default()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::backend::traits::TsRead;
    use common::storage::config::{
        BlockCacheConfig, FoyerHybridCacheConfig, LocalObjectStoreConfig, ObjectStoreConfig,
    };

    fn foyer_cache_config(
        memory_capacity: u64,
        disk_capacity: u64,
        disk_path: String,
    ) -> FoyerHybridCacheConfig {
        FoyerHybridCacheConfig {
            memory_capacity,
            disk_capacity,
            disk_path,
            write_policy: Default::default(),
            flushers: 4,
            buffer_pool_size: None,
            submit_queue_size_threshold: 1024 * 1024 * 1024,
        }
    }

    fn slatedb_config_with_local_dir(dir: &std::path::Path) -> StorageConfig {
        StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: dir.to_str().unwrap().to_string(),
            }),
            settings_path: None,
            block_cache: None,
            meta_cache: None,
        })
    }

    #[tokio::test]
    async fn should_reject_in_memory_config() {
        let result = build_storage(&StorageConfig::InMemory, None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn should_create_storage_with_block_cache_from_config() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path().join("block-cache");
        std::fs::create_dir_all(&cache_dir).unwrap();

        let config = StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: tmp.path().join("obj").to_str().unwrap().to_string(),
            }),
            settings_path: None,
            block_cache: Some(BlockCacheConfig::FoyerHybrid(foyer_cache_config(
                1024 * 1024,
                4 * 1024 * 1024,
                cache_dir.to_str().unwrap().to_string(),
            ))),
            meta_cache: None,
        });

        let storage = build_storage(&config, None).await;
        assert!(storage.is_ok(), "expected config-driven block cache to work");
    }

    #[tokio::test]
    async fn should_create_reader_with_block_cache_from_config() {
        let tmp = tempfile::tempdir().unwrap();
        let obj_path = tmp.path().join("obj").to_str().unwrap().to_string();

        // Writer and reader are distinct instances (in production, distinct
        // processes), so they use separate local block-cache directories.
        let writer_cache = tmp.path().join("writer-cache");
        let reader_cache = tmp.path().join("reader-cache");
        std::fs::create_dir_all(&writer_cache).unwrap();
        std::fs::create_dir_all(&reader_cache).unwrap();

        let with_cache = |cache_dir: &std::path::Path| SlateDbStorageConfig {
            path: "data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: obj_path.clone(),
            }),
            settings_path: None,
            block_cache: Some(BlockCacheConfig::FoyerHybrid(foyer_cache_config(
                1024 * 1024,
                4 * 1024 * 1024,
                cache_dir.to_str().unwrap().to_string(),
            ))),
            meta_cache: None,
        };

        // Open a writer first so the reader has a manifest to read, then drop it
        // (SlateDB fencing) before opening the reader.
        let writer = build_storage(&StorageConfig::SlateDb(with_cache(&writer_cache)), None)
            .await
            .unwrap();
        drop(writer);

        let reader = build_reader(
            &StorageConfig::SlateDb(with_cache(&reader_cache)),
            slatedb::config::DbReaderOptions::default(),
            None,
            None,
        )
        .await;
        assert!(
            reader.is_ok(),
            "expected config-driven block cache on reader to work"
        );
    }

    #[tokio::test]
    async fn should_work_without_block_cache() {
        let tmp = tempfile::tempdir().unwrap();
        let config = slatedb_config_with_local_dir(tmp.path());
        let storage = build_storage(&config, None).await;
        assert!(storage.is_ok());
        storage.unwrap().close().await.unwrap();
    }
}
