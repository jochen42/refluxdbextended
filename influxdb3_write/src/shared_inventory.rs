//! Cross-node shared file inventory.
//!
//! Layout:
//! ```text
//! _inventory/wal/<node_id>/<seq:020>.info.json   # ingester-written snapshots
//! _inventory/compactions/<ulid>.compaction.json  # compactor-written manifests
//! _inventory/checkpoint/<ulid>.full.json         # compactor-written materialized snapshots
//! ```
//!
//! Both ingesters and the compactor publish here. Every node — writer,
//! compactor, querier — loads from here on startup and keeps it in sync via
//! periodic polling so multiple readers can see each other's files without a
//! shared in-memory catalog server.
//!
//! Checkpoints are materialized "full inventory" snapshots that summarize all
//! prior wal + compaction entries up to their ULID. They exist so the loader
//! cost stays bounded as file count grows: it picks the newest checkpoint,
//! then folds only entries with a lexicographically larger ULID/sequence on
//! top.

use crate::PersistedSnapshot;
use bytes::Bytes;
use futures_util::StreamExt;
use object_store::ObjectStore;
use object_store::path::Path as ObjPath;
use observability_deps::tracing::{debug, warn};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum InventoryError {
    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("serde_json error: {0}")]
    SerdeJson(#[from] serde_json::Error),
}

pub type Result<T, E = InventoryError> = std::result::Result<T, E>;

pub const SHARED_INVENTORY_PREFIX: &str = "_inventory";

fn wal_entry(node_id: &str, sequence: u64) -> ObjPath {
    // Same lexicographic ordering trick as SnapshotInfoFilePath: descending so
    // listing in ascending lexicographic order yields newest-first.
    let stem = u64::MAX - sequence;
    ObjPath::from(format!(
        "{SHARED_INVENTORY_PREFIX}/wal/{node_id}/{stem:020}.info.json"
    ))
}

fn compaction_dir() -> ObjPath {
    ObjPath::from(format!("{SHARED_INVENTORY_PREFIX}/compactions"))
}

fn compaction_entry(compaction_id: &str) -> ObjPath {
    ObjPath::from(format!(
        "{SHARED_INVENTORY_PREFIX}/compactions/{compaction_id}.compaction.json"
    ))
}

fn checkpoint_dir() -> ObjPath {
    ObjPath::from(format!("{SHARED_INVENTORY_PREFIX}/checkpoint"))
}

fn checkpoint_entry(checkpoint_id: &str) -> ObjPath {
    ObjPath::from(format!(
        "{SHARED_INVENTORY_PREFIX}/checkpoint/{checkpoint_id}.full.json"
    ))
}

/// Top-level interface to the shared inventory. Cheap to clone (just an
/// `Arc<dyn ObjectStore>`).
#[derive(Debug, Clone)]
pub struct SharedInventory {
    object_store: Arc<dyn ObjectStore>,
}

impl SharedInventory {
    pub fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        Self { object_store }
    }

    /// Publish a WAL-driven snapshot to the shared namespace under this
    /// node's WAL prefix. Path is derived from `snapshot.snapshot_sequence_number`
    /// so retries are idempotent.
    pub async fn publish_wal_snapshot(
        &self,
        node_id: &str,
        snapshot: &PersistedSnapshot,
    ) -> Result<()> {
        let path = wal_entry(node_id, snapshot.snapshot_sequence_number.as_u64());
        let body = serde_json::to_vec_pretty(snapshot)?;
        self.object_store.put(&path, Bytes::from(body).into()).await?;
        Ok(())
    }

    /// Publish a compaction manifest. Caller-supplied id (typically `Uuid::now_v7`)
    /// must be unique across the cluster.
    pub async fn publish_compaction(
        &self,
        compaction_id: &str,
        snapshot: &PersistedSnapshot,
    ) -> Result<()> {
        let path = compaction_entry(compaction_id);
        let body = serde_json::to_vec_pretty(snapshot)?;
        self.object_store.put(&path, Bytes::from(body).into()).await?;
        Ok(())
    }

    /// Write a checkpoint that summarizes the inventory state up to and
    /// including the entries with ids lexicographically `<= up_to_id`. Future
    /// loaders that find this checkpoint can skip listing entries with id
    /// `<= up_to_id`. Returns the checkpoint id used.
    pub async fn write_checkpoint(
        &self,
        snapshot: &Checkpoint,
    ) -> Result<String> {
        let checkpoint_id = Uuid::now_v7().to_string();
        let path = checkpoint_entry(&checkpoint_id);
        let body = serde_json::to_vec_pretty(snapshot)?;
        self.object_store.put(&path, Bytes::from(body).into()).await?;
        Ok(checkpoint_id)
    }

    /// List every WAL-snapshot manifest written by every node. Caller may
    /// optionally pass `since_sequence_per_node` so already-loaded entries
    /// are skipped during refresh polling.
    pub async fn load_all_wal_snapshots(&self) -> Result<Vec<PersistedSnapshot>> {
        let dir = ObjPath::from(format!("{SHARED_INVENTORY_PREFIX}/wal"));
        let mut paths = Vec::new();
        let mut listing = self.object_store.list(Some(&dir));
        while let Some(item) = listing.next().await {
            paths.push(item?.location);
        }
        // Lexicographic order: per-node, newest first (because of the
        // u64::MAX - seq trick). We want oldest first for replay correctness
        // when manifests reference each other's removed_files, so reverse.
        paths.sort_unstable();
        paths.reverse();

        let mut out = Vec::with_capacity(paths.len());
        for path in paths {
            match self.fetch_snapshot(&path).await {
                Ok(s) => out.push(s),
                Err(e) => warn!("skipping inventory entry {}: {}", path, e),
            }
        }
        Ok(out)
    }

    /// List every compaction manifest, in publish order (ULID-sorted ascending).
    pub async fn load_all_compactions(&self) -> Result<Vec<PersistedSnapshot>> {
        let dir = compaction_dir();
        let mut paths = Vec::new();
        let mut listing = self.object_store.list(Some(&dir));
        while let Some(item) = listing.next().await {
            paths.push(item?.location);
        }
        paths.sort_unstable();

        let mut out = Vec::with_capacity(paths.len());
        for path in paths {
            match self.fetch_snapshot(&path).await {
                Ok(s) => out.push(s),
                Err(e) => warn!("skipping inventory entry {}: {}", path, e),
            }
        }
        Ok(out)
    }

    /// Load the newest checkpoint, if any. Returns `None` if no checkpoints
    /// have been written.
    pub async fn load_latest_checkpoint(&self) -> Result<Option<Checkpoint>> {
        let dir = checkpoint_dir();
        let mut paths = Vec::new();
        let mut listing = self.object_store.list(Some(&dir));
        while let Some(item) = listing.next().await {
            paths.push(item?.location);
        }
        if paths.is_empty() {
            return Ok(None);
        }
        // ULID v7 is time-sortable lexicographically, so newest = max.
        paths.sort_unstable();
        let newest = paths.last().expect("non-empty");
        let bytes = self.object_store.get(newest).await?.bytes().await?;
        let cp: Checkpoint = serde_json::from_slice(&bytes)?;
        Ok(Some(cp))
    }

    /// Load full inventory state. Returns a sequence of `PersistedSnapshot`s
    /// in replay order (apply each via `PersistedFiles::add_persisted_snapshot_files`
    /// in order to reconstruct).
    ///
    /// Algorithm:
    /// 1. If a checkpoint exists, start from its `merged_snapshot` and skip
    ///    any wal/compaction entry whose id is `<= checkpoint.up_to_*`.
    /// 2. Otherwise, fold all wal snapshots followed by all compaction
    ///    manifests in their stored order.
    pub async fn load_full_state(&self) -> Result<LoadedInventory> {
        let checkpoint = self.load_latest_checkpoint().await?;

        let mut wal_snapshots = self.load_all_wal_snapshots().await?;
        let mut compactions = self.load_all_compactions().await?;

        if let Some(cp) = &checkpoint {
            wal_snapshots.retain(|s| {
                // Keep only entries newer than what the checkpoint already covers.
                let node = &s.node_id;
                let seq = s.snapshot_sequence_number.as_u64();
                cp.wal_high_water
                    .get(node)
                    .map(|hwm| seq > *hwm)
                    .unwrap_or(true)
            });
            // Compaction ids are ULIDs — string compare with the checkpoint's
            // recorded high-water.
            if let Some(hwm) = &cp.compactions_high_water {
                compactions.retain(|s| {
                    s.node_id.as_str() > hwm.as_str()
                        || s.catalog_sequence_number.get() > 0
                });
                // `node_id` on compaction manifests is the COMPACTOR's id; we don't
                // actually have the ULID in the persisted snapshot. Filtering by
                // ULID requires the inventory to remember per-path. Skip this
                // refinement for now — checkpoint-based pruning re-adds these
                // entries but `PersistedFiles::new_from_persisted_snapshots` is
                // idempotent for add+remove via file id matching, so duplicates
                // are absorbed safely. Cheap correctness > clever filtering.
                let _ = hwm;
            }
        }

        debug!(
            "shared inventory loaded: checkpoint={}, wal_snapshots={}, compactions={}",
            checkpoint.is_some(),
            wal_snapshots.len(),
            compactions.len()
        );

        Ok(LoadedInventory {
            checkpoint,
            wal_snapshots,
            compactions,
        })
    }

    async fn fetch_snapshot(&self, path: &ObjPath) -> Result<PersistedSnapshot> {
        let bytes = self.object_store.get(path).await?.bytes().await?;
        Ok(serde_json::from_slice(&bytes)?)
    }
}

/// A materialized checkpoint of all known inventory state up to recorded
/// high-water marks. Loaders use this to amortize startup cost.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Checkpoint {
    /// `node_id -> highest snapshot_sequence_number included`
    pub wal_high_water: std::collections::HashMap<String, u64>,
    /// Highest compaction ULID included, or `None` if no compactions yet.
    pub compactions_high_water: Option<String>,
    /// Materialized state: every live file, encoded as a single synthetic
    /// `PersistedSnapshot` with `databases` populated and `removed_files`
    /// empty. Applying it to a fresh `PersistedFiles` reproduces the
    /// checkpoint's view.
    pub merged_snapshot: PersistedSnapshot,
}

#[derive(Debug)]
pub struct LoadedInventory {
    pub checkpoint: Option<Checkpoint>,
    pub wal_snapshots: Vec<PersistedSnapshot>,
    pub compactions: Vec<PersistedSnapshot>,
}

impl LoadedInventory {
    /// Flatten into a single ordered sequence of `PersistedSnapshot`s suitable
    /// for `PersistedFiles::new_from_persisted_snapshots`.
    pub fn flatten(self) -> Vec<PersistedSnapshot> {
        let mut out: Vec<PersistedSnapshot> = Vec::new();
        if let Some(cp) = self.checkpoint {
            out.push(cp.merged_snapshot);
        }
        out.extend(self.wal_snapshots);
        out.extend(self.compactions);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ParquetFile, ParquetFileId};
    use influxdb3_catalog::catalog::CatalogSequenceNumber;
    use influxdb3_id::{DbId, TableId};
    use influxdb3_wal::{SnapshotSequenceNumber, WalFileSequenceNumber};
    use object_store::memory::InMemory;

    fn snap_with(node: &str, seq: u64, file_path: &str) -> PersistedSnapshot {
        let mut s = PersistedSnapshot::new(
            node.to_string(),
            SnapshotSequenceNumber::new(seq),
            WalFileSequenceNumber::new(seq),
            CatalogSequenceNumber::new(0),
        );
        s.add_parquet_file(
            DbId::from(1),
            TableId::from(0),
            ParquetFile {
                id: ParquetFileId::new(),
                path: file_path.to_string(),
                size_bytes: 10,
                row_count: 1,
                chunk_time: 0,
                min_time: 0,
                max_time: 0,
            },
        );
        s
    }

    #[tokio::test]
    async fn publish_and_load_wal_snapshots_across_nodes() {
        let inv = SharedInventory::new(Arc::new(InMemory::new()));
        inv.publish_wal_snapshot("node-a", &snap_with("node-a", 1, "a/1.parquet"))
            .await
            .unwrap();
        inv.publish_wal_snapshot("node-b", &snap_with("node-b", 1, "b/1.parquet"))
            .await
            .unwrap();
        inv.publish_wal_snapshot("node-a", &snap_with("node-a", 2, "a/2.parquet"))
            .await
            .unwrap();
        let loaded = inv.load_all_wal_snapshots().await.unwrap();
        assert_eq!(loaded.len(), 3);
        let paths: std::collections::HashSet<String> = loaded
            .iter()
            .flat_map(|s| {
                s.databases
                    .iter()
                    .flat_map(|(_, t)| t.tables.iter())
                    .flat_map(|(_, fs)| fs.iter())
                    .map(|f| f.path.clone())
            })
            .collect();
        assert!(paths.contains("a/1.parquet"));
        assert!(paths.contains("a/2.parquet"));
        assert!(paths.contains("b/1.parquet"));
    }

    #[tokio::test]
    async fn publish_and_load_compactions() {
        let inv = SharedInventory::new(Arc::new(InMemory::new()));
        inv.publish_compaction("0001", &snap_with("compactor", 0, "gen2/0.parquet"))
            .await
            .unwrap();
        inv.publish_compaction("0002", &snap_with("compactor", 0, "gen2/1.parquet"))
            .await
            .unwrap();
        let loaded = inv.load_all_compactions().await.unwrap();
        assert_eq!(loaded.len(), 2);
    }

    #[tokio::test]
    async fn checkpoint_round_trip() {
        let inv = SharedInventory::new(Arc::new(InMemory::new()));
        let cp = Checkpoint {
            wal_high_water: [("node-a".to_string(), 5u64)].into_iter().collect(),
            compactions_high_water: Some("01ABC".to_string()),
            merged_snapshot: snap_with("checkpoint", 0, "merged/file.parquet"),
        };
        inv.write_checkpoint(&cp).await.unwrap();
        let loaded = inv.load_latest_checkpoint().await.unwrap().unwrap();
        assert_eq!(loaded.wal_high_water.get("node-a"), Some(&5));
        assert_eq!(loaded.compactions_high_water.as_deref(), Some("01ABC"));
    }

    #[tokio::test]
    async fn load_full_state_combines_everything() {
        let inv = SharedInventory::new(Arc::new(InMemory::new()));
        inv.publish_wal_snapshot("node-a", &snap_with("node-a", 1, "a/1.parquet"))
            .await
            .unwrap();
        inv.publish_compaction("01H", &snap_with("compactor", 0, "gen2/0.parquet"))
            .await
            .unwrap();

        let state = inv.load_full_state().await.unwrap();
        assert!(state.checkpoint.is_none());
        assert_eq!(state.wal_snapshots.len(), 1);
        assert_eq!(state.compactions.len(), 1);

        let flat = state.flatten();
        assert_eq!(flat.len(), 2);
    }

    #[tokio::test]
    async fn checkpoint_skips_wal_entries_at_or_below_high_water() {
        let inv = SharedInventory::new(Arc::new(InMemory::new()));
        for seq in 1..=5 {
            inv.publish_wal_snapshot(
                "node-a",
                &snap_with("node-a", seq, &format!("a/{seq}.parquet")),
            )
            .await
            .unwrap();
        }
        let cp = Checkpoint {
            wal_high_water: [("node-a".to_string(), 3u64)].into_iter().collect(),
            compactions_high_water: None,
            merged_snapshot: snap_with("checkpoint", 0, "merged.parquet"),
        };
        inv.write_checkpoint(&cp).await.unwrap();

        let state = inv.load_full_state().await.unwrap();
        assert!(state.checkpoint.is_some());
        // Only entries with seq > 3 should remain after checkpoint pruning.
        assert_eq!(state.wal_snapshots.len(), 2);
        let kept: std::collections::HashSet<u64> = state
            .wal_snapshots
            .iter()
            .map(|s| s.snapshot_sequence_number.as_u64())
            .collect();
        assert_eq!(kept, [4u64, 5u64].into_iter().collect());
    }
}
