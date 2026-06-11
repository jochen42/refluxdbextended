//! Background task that validates `PersistedFiles` parquet references
//! against the object store and evicts references whose objects do not
//! exist ("phantom refs").
//!
//! Why this is safe: the system invariant is upload-before-publish — a
//! parquet reference only ever appears after its object was successfully
//! uploaded, and deletions are announced via `removed_files` manifests
//! before the objects are removed (with a grace period). A reference whose
//! object is absent at listing time is therefore either a phantom (was
//! never uploaded, e.g. a corrupted manifest) or already announced as
//! removed — evicting it is correct in both cases. Uploads racing the scan
//! cannot be evicted: their references appear only after the object exists.
//!
//! Validation uses recursive LIST per node prefix rather than per-file
//! HEAD, so a full pass over tens of thousands of references costs a
//! handful of list pages.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use crate::write_buffer::persisted_files::PersistedFiles;
use futures_util::StreamExt;
use influxdb3_shutdown::ShutdownToken;
use object_store::ObjectStore;
use object_store::path::Path as ObjPath;
use observability_deps::tracing::{debug, info, warn};
use tokio::task::JoinHandle;

#[derive(Debug)]
pub struct RefValidatorArgs {
    pub object_store: Arc<dyn ObjectStore>,
    pub persisted_files: Arc<PersistedFiles>,
    pub interval: Duration,
    pub shutdown: ShutdownToken,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ValidationSummary {
    /// References that were checked against a successfully listed prefix.
    pub checked: usize,
    /// References evicted because their object does not exist.
    pub evicted: usize,
    /// References skipped because their prefix failed to list.
    pub skipped: usize,
}

pub fn spawn(args: RefValidatorArgs) -> JoinHandle<()> {
    tokio::spawn(async move { run(args).await })
}

async fn run(args: RefValidatorArgs) {
    let RefValidatorArgs {
        object_store,
        persisted_files,
        interval,
        shutdown,
    } = args;

    let cancel = shutdown.clone_cancellation_token();

    // Boot-time validation, then periodic.
    loop {
        let start = std::time::Instant::now();
        let summary = validate_once(&object_store, &persisted_files).await;
        if summary.evicted > 0 || summary.skipped > 0 {
            info!(
                checked = summary.checked,
                evicted = summary.evicted,
                skipped = summary.skipped,
                elapsed_ms = start.elapsed().as_millis() as u64,
                "parquet ref validation pass complete"
            );
        } else {
            debug!(
                checked = summary.checked,
                elapsed_ms = start.elapsed().as_millis() as u64,
                "parquet ref validation pass complete (all refs valid)"
            );
        }

        tokio::select! {
            _ = cancel.cancelled() => {
                debug!("ref validator shutting down");
                return;
            }
            _ = tokio::time::sleep(interval) => {}
        }
    }
}

/// One full validation pass: list every node prefix referenced by
/// `PersistedFiles` and evict references whose objects are missing.
pub async fn validate_once(
    object_store: &Arc<dyn ObjectStore>,
    persisted_files: &PersistedFiles,
) -> ValidationSummary {
    let refs = persisted_files.snapshot_all();
    if refs.is_empty() {
        return ValidationSummary::default();
    }

    // Group existing-object lookups by node prefix (first path segment) so
    // one recursive LIST per node covers all its files. `dbs` narrows the
    // listing to data files only.
    let mut prefixes: HashSet<String> = HashSet::new();
    for (_, _, files) in &refs {
        for file in files {
            if let Some(prefix) = file.path.split('/').next()
                && !prefix.is_empty()
            {
                prefixes.insert(prefix.to_string());
            }
        }
    }

    // Per-prefix existing paths; a prefix maps to None when its listing
    // failed — refs under it are skipped, never evicted on partial data.
    let mut existing: HashMap<String, Option<HashSet<String>>> = HashMap::new();
    for prefix in prefixes {
        let dir = ObjPath::from(format!("{prefix}/dbs"));
        let mut paths: HashSet<String> = HashSet::new();
        let mut listing = object_store.list(Some(&dir));
        let mut failed = false;
        while let Some(item) = listing.next().await {
            match item {
                Ok(meta) => {
                    paths.insert(meta.location.to_string());
                }
                Err(e) => {
                    warn!(
                        %prefix,
                        error = %e,
                        "listing failed during ref validation; skipping prefix"
                    );
                    failed = true;
                    break;
                }
            }
        }
        existing.insert(prefix, if failed { None } else { Some(paths) });
    }

    let mut summary = ValidationSummary::default();
    for (db_id, table_id, files) in refs {
        let mut missing = Vec::new();
        for file in files {
            let prefix = file.path.split('/').next().unwrap_or("");
            match existing.get(prefix) {
                Some(Some(paths)) => {
                    summary.checked += 1;
                    if !paths.contains(&file.path) {
                        warn!(
                            path = %file.path,
                            ?db_id,
                            ?table_id,
                            "evicting parquet ref: object does not exist"
                        );
                        missing.push(file);
                    }
                }
                // listing failed or prefix unknown — do not evict
                _ => summary.skipped += 1,
            }
        }
        if !missing.is_empty() {
            summary.evicted += missing.len();
            persisted_files.remove_persisted_files(&db_id, &table_id, &missing);
        }
    }

    summary
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ParquetFile, ParquetFileId};
    use influxdb3_id::{DbId, TableId};
    use object_store::memory::InMemory;

    fn file(id: u64, path: &str) -> ParquetFile {
        ParquetFile {
            id: ParquetFileId::from(id),
            path: path.to_string(),
            size_bytes: 1,
            row_count: 1,
            chunk_time: 0,
            min_time: 0,
            max_time: 1,
        }
    }

    async fn put(store: &InMemory, path: &str) {
        store
            .put(&ObjPath::from(path), bytes::Bytes::from_static(b"x").into())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn evicts_only_missing_refs() {
        let store = Arc::new(InMemory::new());
        put(&store, "node-a/dbs/1/0/f1.parquet").await;
        put(&store, "node-a/dbs/1/0/f2.parquet").await;

        let persisted = PersistedFiles::default();
        let db = DbId::from(1);
        let table = TableId::from(0);
        persisted.add_persisted_file(&db, &table, &file(1, "node-a/dbs/1/0/f1.parquet"));
        persisted.add_persisted_file(&db, &table, &file(2, "node-a/dbs/1/0/f2.parquet"));
        persisted.add_persisted_file(&db, &table, &file(3, "node-a/dbs/1/0/phantom.parquet"));

        let store: Arc<dyn ObjectStore> = store;
        let summary = validate_once(&store, &persisted).await;

        assert_eq!(summary.checked, 3);
        assert_eq!(summary.evicted, 1);
        assert_eq!(summary.skipped, 0);
        let remaining = persisted.get_files(db, table);
        assert_eq!(remaining.len(), 2);
        assert!(remaining.iter().all(|f| !f.path.contains("phantom")));
    }

    #[tokio::test]
    async fn handles_multiple_node_prefixes() {
        let store = Arc::new(InMemory::new());
        put(&store, "writer-0/dbs/1/0/w.parquet").await;
        put(&store, "compactor-0/dbs/1/0/c.parquet").await;

        let persisted = PersistedFiles::default();
        let db = DbId::from(1);
        let table = TableId::from(0);
        persisted.add_persisted_file(&db, &table, &file(1, "writer-0/dbs/1/0/w.parquet"));
        persisted.add_persisted_file(&db, &table, &file(2, "compactor-0/dbs/1/0/c.parquet"));
        persisted.add_persisted_file(&db, &table, &file(3, "compactor-0/dbs/1/0/gone.parquet"));

        let store: Arc<dyn ObjectStore> = store;
        let summary = validate_once(&store, &persisted).await;

        assert_eq!(summary.checked, 3);
        assert_eq!(summary.evicted, 1);
        assert_eq!(persisted.get_files(db, table).len(), 2);
    }

    #[tokio::test]
    async fn empty_state_is_noop() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let persisted = PersistedFiles::default();
        let summary = validate_once(&store, &persisted).await;
        assert_eq!(summary, ValidationSummary::default());
    }
}
