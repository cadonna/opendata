//! Native SlateDB storage backend for timeseries.
//!
//! This module replaces timeseries' dependency on `common::storage`. It owns
//! the SlateDB integration directly:
//!
//! - [`ops`] — plain value types (`Record`, `RecordOp`, `Ttl`, …) and the
//!   storage error type.
//! - [`traits`] — the minimal [`TsRead`]/[`TsSnapshot`] read abstraction that
//!   unifies the three SlateDB read backends. Writes use inherent methods on
//!   the concrete [`SlateDbStorage`].
//! - [`config`] — serde config types (kept compatible with existing YAML).
//! - [`factory`] — [`build_storage`]/[`build_reader`] constructors plus object
//!   store and foyer block-cache wiring.
//! - [`metrics`] — bridges SlateDB/foyer metrics into the `metrics` crate.
//! - [`slate`] — the concrete `SlateDbStorage`/`SlateDbStorageSnapshot`/
//!   `SlateDbStorageReader` and their `TsRead` impls.
//
// TODO(phase-3): remove these `allow`s once the rest of timeseries is
// re-anchored onto these types. Until then the module is additive: its items
// and convenience re-exports have no in-crate users yet.
#![allow(dead_code, unused_imports)]

pub(crate) mod config;
pub(crate) mod factory;
pub(crate) mod metrics;
pub(crate) mod ops;
pub(crate) mod slate;
pub(crate) mod traits;

pub(crate) use config::{
    AwsObjectStoreConfig, BlockCacheConfig, FoyerHybridCacheConfig, FoyerWritePolicy,
    LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig, StorageConfig,
};
pub(crate) use factory::{SlateMergeOperator, build_reader, build_storage, create_object_store};
pub(crate) use ops::{
    CheckpointInfo, MergeOptions, MergeRecordOp, PutOptions, PutRecordOp, Record, RecordOp,
    StorageError, StorageResult, Ttl, WriteOptions, WriteResult,
};
pub(crate) use slate::{SlateDbStorage, SlateDbStorageReader, SlateDbStorageSnapshot};
pub(crate) use traits::{TsIterator, TsRead, TsSnapshot};
