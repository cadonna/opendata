//! Native SlateDB storage backend for timeseries.
//!
//! This module replaces timeseries' use of the `common::storage` *handles*
//! (`Storage`/`StorageRead`/`StorageSnapshot`) with a direct SlateDB
//! integration. Everything reusable — value types (`Record`, `RecordOp`,
//! `Ttl`, …), the iterator and merge-operator traits, the error type, the
//! config types, the object-store / foyer-cache / metrics-recorder plumbing,
//! and the `Ttl`/options → SlateDB conversions — is reused from
//! `common::storage` rather than duplicated.
//!
//! - [`traits`] — the minimal [`TsRead`]/[`TsSnapshot`] read abstraction that
//!   unifies the three SlateDB read backends. Writes use inherent methods on
//!   the concrete [`SlateDbStorage`].
//! - [`slate`] — the concrete `SlateDbStorage`/`SlateDbStorageSnapshot`/
//!   `SlateDbStorageReader` wrappers over raw SlateDB types.
//! - [`factory`] — [`build_storage`]/[`build_reader`] constructors.
pub(crate) mod factory;
pub(crate) mod slate;
pub(crate) mod traits;

pub(crate) use factory::{build_reader, build_storage};
pub(crate) use slate::SlateDbStorage;
pub(crate) use traits::{TsRead, TsSnapshot};

#[cfg(any(test, feature = "testing"))]
pub(crate) use factory::in_memory_storage;
