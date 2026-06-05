//! Factory functions that build native SlateDB storage from configuration.
//!
//! Replaces the previous `common::storage::factory` (`StorageBuilder`,
//! `create_storage_read`, `StorageReaderRuntime`, `StorageSemantics`) with two
//! focused constructors — [`build_storage`] for the read/write `Db` and
//! [`build_reader`] for the read-only `DbReader`.

use std::sync::Arc;

use slatedb::config::Settings;
use slatedb::db_cache::DbCache;
use slatedb::db_cache::foyer_hybrid::FoyerHybridCache;
pub use slatedb::db_cache::{CachedEntry, CachedKey};
use slatedb::object_store::{self, ObjectStore};
use slatedb::{DbBuilder, DbReader, MergeOperator as SlateDbMergeOperator};
use tracing::info;
use uuid::Uuid;

use super::config::{
    BlockCacheConfig, FoyerWritePolicy, ObjectStoreConfig, SlateDbStorageConfig, StorageConfig,
};
use super::metrics::{MetricsRsRecorder, MixtricsBridge};
use super::ops::{StorageError, StorageResult};
use super::slate::{SlateDbStorage, SlateDbStorageReader};

/// A merge operator handed to SlateDB. Timeseries implements
/// `slatedb::MergeOperator` directly (see `storage::merge_operator`).
pub type SlateMergeOperator = Arc<dyn SlateDbMergeOperator + Send + Sync>;

/// Handle to a foyer hybrid cache we own and must close explicitly on shutdown.
///
/// TODO(slatedb 0.13): remove this once SlateDB's `DbCache` trait gains a
/// `close()` hook and `Db::close()` / `DbReader::close()` drive cache shutdown,
/// so callers won't need a side handle to close the hybrid cache.
pub(crate) type OwnedHybridCache = foyer::HybridCache<CachedKey, CachedEntry>;

/// Block cache we constructed internally — we keep the `HybridCache` handle so
/// we can `close().await` it from `TsRead::close()` rather than relying on
/// foyer's Drop-based close, which races runtime shutdown.
struct ManagedBlockCache {
    db_cache: Arc<dyn DbCache>,
    hybrid: OwnedHybridCache,
}

/// Builds a read/write [`SlateDbStorage`] from configuration.
///
/// Registers the metrics recorder and, when provided, the merge operator. The
/// `InMemory` config variant is not supported (see module docs); callers should
/// use SlateDb over an in-memory object store for tests.
pub async fn build_storage(
    config: &StorageConfig,
    merge_operator: Option<SlateMergeOperator>,
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

    let mut managed_cache: Option<OwnedHybridCache> = None;
    if let Some(managed) = create_block_cache_from_config(&slate_config.block_cache).await? {
        db_builder = db_builder.with_db_cache(managed.db_cache);
        managed_cache = Some(managed.hybrid);
    }
    if let Some(op) = merge_operator {
        db_builder = db_builder.with_merge_operator(op);
    }

    let db = db_builder
        .build()
        .await
        .map_err(|e| StorageError::Storage(format!("Failed to create SlateDB: {}", e)))?;
    Ok(SlateDbStorage::new_with_managed_cache(
        Arc::new(db),
        managed_cache,
    ))
}

/// Builds a read-only [`SlateDbStorageReader`] from configuration.
///
/// Uses `DbReader`, which does not participate in fencing, so it can coexist
/// with a live writer. When `checkpoint_id` is set, the reader is pinned to that
/// checkpoint and does not advance with newer writes.
pub async fn build_reader(
    config: &StorageConfig,
    reader_options: slatedb::config::DbReaderOptions,
    checkpoint_id: Option<Uuid>,
    merge_operator: Option<SlateMergeOperator>,
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
        builder = builder.with_merge_operator(op);
    }

    let mut managed_cache: Option<OwnedHybridCache> = None;
    if let Some(managed) = create_block_cache_from_config(&slate_config.block_cache).await? {
        builder = builder.with_db_cache(managed.db_cache);
        managed_cache = Some(managed.hybrid);
    }

    let reader = builder
        .build()
        .await
        .map_err(|e| StorageError::Storage(format!("Failed to create SlateDB reader: {}", e)))?;
    Ok(SlateDbStorageReader::new_with_managed_cache(
        Arc::new(reader),
        managed_cache,
    ))
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

/// Creates an object store from configuration without initializing SlateDB.
pub fn create_object_store(config: &ObjectStoreConfig) -> StorageResult<Arc<dyn ObjectStore>> {
    match config {
        ObjectStoreConfig::InMemory => Ok(Arc::new(object_store::memory::InMemory::new())),
        ObjectStoreConfig::Aws(aws_config) => {
            let store = object_store::aws::AmazonS3Builder::from_env()
                .with_region(&aws_config.region)
                .with_bucket_name(&aws_config.bucket)
                .build()
                .map_err(|e| {
                    StorageError::Storage(format!("Failed to create AWS S3 store: {}", e))
                })?;
            Ok(Arc::new(store))
        }
        ObjectStoreConfig::Local(local_config) => {
            std::fs::create_dir_all(&local_config.path).map_err(|e| {
                StorageError::Storage(format!(
                    "Failed to create storage directory '{}': {}",
                    local_config.path, e
                ))
            })?;
            let store = object_store::local::LocalFileSystem::new_with_prefix(&local_config.path)
                .map_err(|e| {
                    StorageError::Storage(format!(
                        "Failed to create local filesystem store: {}",
                        e
                    ))
                })?;
            Ok(Arc::new(store))
        }
    }
}

/// Creates a block cache from the serializable config, if present. Returns both
/// the `DbCache` trait object handed to SlateDB and a `HybridCache` handle the
/// caller keeps so it can close the cache deterministically on shutdown.
async fn create_block_cache_from_config(
    config: &Option<BlockCacheConfig>,
) -> StorageResult<Option<ManagedBlockCache>> {
    let Some(config) = config else {
        return Ok(None);
    };
    match config {
        BlockCacheConfig::FoyerHybrid(foyer_config) => {
            use foyer::{
                BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCacheBuilder,
                HybridCachePolicy, PsyncIoEngineConfig,
            };

            let memory_capacity = usize::try_from(foyer_config.memory_capacity).map_err(|_| {
                StorageError::Storage(format!(
                    "memory_capacity {} exceeds usize::MAX on this platform",
                    foyer_config.memory_capacity
                ))
            })?;
            let disk_capacity = usize::try_from(foyer_config.disk_capacity).map_err(|_| {
                StorageError::Storage(format!(
                    "disk_capacity {} exceeds usize::MAX on this platform",
                    foyer_config.disk_capacity
                ))
            })?;
            let buffer_pool_size = usize::try_from(foyer_config.effective_buffer_pool_size())
                .map_err(|_| {
                    StorageError::Storage(format!(
                        "buffer_pool_size {} exceeds usize::MAX on this platform",
                        foyer_config.effective_buffer_pool_size()
                    ))
                })?;
            let submit_queue_size_threshold =
                usize::try_from(foyer_config.submit_queue_size_threshold).map_err(|_| {
                    StorageError::Storage(format!(
                        "submit_queue_size_threshold {} exceeds usize::MAX on this platform",
                        foyer_config.submit_queue_size_threshold
                    ))
                })?;

            let policy = match foyer_config.write_policy {
                FoyerWritePolicy::WriteOnInsertion => HybridCachePolicy::WriteOnInsertion,
                FoyerWritePolicy::WriteOnEviction => HybridCachePolicy::WriteOnEviction,
            };

            let device = {
                #[cfg(target_os = "linux")]
                let builder = FsDeviceBuilder::new(&foyer_config.disk_path)
                    .with_capacity(disk_capacity)
                    .with_direct(true);
                #[cfg(not(target_os = "linux"))]
                let builder =
                    FsDeviceBuilder::new(&foyer_config.disk_path).with_capacity(disk_capacity);
                builder.build().map_err(|e| {
                    StorageError::Storage(format!("Failed to build foyer device: {}", e))
                })?
            };

            let cache = HybridCacheBuilder::new()
                .with_name("slatedb_block_cache")
                .with_metrics_registry(Box::new(MixtricsBridge))
                .with_policy(policy)
                .memory(memory_capacity)
                .with_weighter(|_, v: &CachedEntry| v.size())
                .storage()
                .with_io_engine_config(PsyncIoEngineConfig::new())
                .with_engine_config(
                    BlockEngineConfig::new(device)
                        .with_flushers(foyer_config.flushers)
                        .with_buffer_pool_size(buffer_pool_size)
                        .with_submit_queue_size_threshold(submit_queue_size_threshold),
                )
                .build()
                .await
                .map_err(|e| {
                    StorageError::Storage(format!("Failed to create hybrid cache: {}", e))
                })?;

            info!(
                memory_mb = foyer_config.memory_capacity / (1024 * 1024),
                disk_mb = foyer_config.disk_capacity / (1024 * 1024),
                disk_path = %foyer_config.disk_path,
                write_policy = ?foyer_config.write_policy,
                flushers = foyer_config.flushers,
                buffer_pool_mb = foyer_config.effective_buffer_pool_size() / (1024 * 1024),
                submit_queue_threshold_mb =
                    foyer_config.submit_queue_size_threshold / (1024 * 1024),
                "hybrid block cache enabled"
            );

            let db_cache =
                Arc::new(FoyerHybridCache::new_with_cache(cache.clone())) as Arc<dyn DbCache>;
            Ok(Some(ManagedBlockCache {
                db_cache,
                hybrid: cache,
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::backend::config::{
        FoyerHybridCacheConfig, LocalObjectStoreConfig, SlateDbStorageConfig,
    };
    use crate::storage::backend::traits::TsRead;

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
    async fn should_return_none_when_no_block_cache_configured() {
        let result = create_block_cache_from_config(&None).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn should_work_without_block_cache() {
        let tmp = tempfile::tempdir().unwrap();
        let config = slatedb_config_with_local_dir(tmp.path());
        let storage = build_storage(&config, None).await;
        assert!(storage.is_ok());
    }
}
