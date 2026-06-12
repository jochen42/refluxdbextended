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
use std::collections::HashMap;
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

/// Prometheus metrics for shared-inventory publishes. A failed publish means
/// peers won't see the snapshot/manifest — the silent failure mode behind the
/// phantom-ref incident class — so it must be visible on a dashboard.
#[derive(Debug)]
pub struct SharedInventoryMetrics {
    publishes: metric::Metric<metric::U64Counter>,
}

impl SharedInventoryMetrics {
    pub fn new(registry: &metric::Registry) -> Self {
        let publishes = registry.register_metric::<metric::U64Counter>(
            "influxdb3_shared_inventory_publish",
            "shared inventory publishes by kind and result",
        );
        Self { publishes }
    }

    fn record(&self, kind: &'static str, ok: bool) {
        self.publishes
            .recorder(&[("kind", kind), ("result", if ok { "ok" } else { "error" })])
            .inc(1);
    }
}

/// Top-level interface to the shared inventory. Cheap to clone (an
/// `Arc<dyn ObjectStore>` plus an optional `Arc` of metrics).
#[derive(Debug, Clone)]
pub struct SharedInventory {
    object_store: Arc<dyn ObjectStore>,
    metrics: Option<Arc<SharedInventoryMetrics>>,
}

impl SharedInventory {
    pub fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            object_store,
            metrics: None,
        }
    }

    /// Attach publish metrics. Clones share the same recorders.
    pub fn with_metrics(mut self, registry: &metric::Registry) -> Self {
        self.metrics = Some(Arc::new(SharedInventoryMetrics::new(registry)));
        self
    }

    fn record_publish(&self, kind: &'static str, ok: bool) {
        if let Some(m) = &self.metrics {
            m.record(kind, ok);
        }
    }

    /// WAL file sequence number covered by the node's newest published
    /// snapshot, or `None` when the node never published one. Cheap: the
    /// inverted-stem naming makes the first listed entry the newest, so
    /// this is one LIST page + one GET.
    pub async fn latest_covered_wal_seq(&self, node_id: &str) -> Result<Option<u64>> {
        let dir = ObjPath::from(format!("{SHARED_INVENTORY_PREFIX}/wal/{node_id}"));
        let mut listing = self.object_store.list(Some(&dir));
        let Some(first) = listing.next().await else {
            return Ok(None);
        };
        let bytes = self.object_store.get(&first?.location).await?.bytes().await?;
        let snapshot: PersistedSnapshot = serde_json::from_slice(&bytes)?;
        Ok(Some(snapshot.wal_file_sequence_number.as_u64()))
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
        let result: Result<()> = async {
            let body = serde_json::to_vec_pretty(snapshot)?;
            self.object_store.put(&path, Bytes::from(body).into()).await?;
            Ok(())
        }
        .await;
        self.record_publish("wal_snapshot", result.is_ok());
        result
    }

    /// Publish a compaction manifest. Caller-supplied id (typically `Uuid::now_v7`)
    /// must be unique across the cluster.
    pub async fn publish_compaction(
        &self,
        compaction_id: &str,
        snapshot: &PersistedSnapshot,
    ) -> Result<()> {
        let path = compaction_entry(compaction_id);
        let result: Result<()> = async {
            let body = serde_json::to_vec_pretty(snapshot)?;
            self.object_store.put(&path, Bytes::from(body).into()).await?;
            Ok(())
        }
        .await;
        self.record_publish("compaction", result.is_ok());
        result
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
        let result: Result<String> = async {
            let body = serde_json::to_vec_pretty(snapshot)?;
            self.object_store.put(&path, Bytes::from(body).into()).await?;
            Ok(checkpoint_id)
        }
        .await;
        self.record_publish("checkpoint", result.is_ok());
        result
    }

    /// List every WAL-snapshot manifest written by every node. Caller passes
    /// `since_sequence_per_node` so already-loaded entries are skipped during
    /// refresh polling — pass an empty map to load everything. Filtering
    /// happens against the path stem so already-seen entries skip the GET.
    pub async fn load_all_wal_snapshots(
        &self,
        since_sequence_per_node: &HashMap<String, u64>,
    ) -> Result<Vec<PersistedSnapshot>> {
        let dir = ObjPath::from(format!("{SHARED_INVENTORY_PREFIX}/wal"));
        let mut filtered: Vec<(u64, ObjPath)> = Vec::new();
        let mut listing = self.object_store.list(Some(&dir));
        while let Some(item) = listing.next().await {
            let location = item?.location;
            // parts: ["_inventory", "wal", "<node_id>", "<stem>.info.json"]
            let parts: Vec<_> = location.parts().collect();
            if parts.len() < 4 {
                continue;
            }
            let node_id = parts[2].as_ref().to_string();
            let filename = parts[3].as_ref().to_string();
            let stem = filename.trim_end_matches(".info.json");
            let Ok(reversed) = stem.parse::<u64>() else {
                continue;
            };
            let seq = u64::MAX - reversed;
            let since = since_sequence_per_node
                .get(&node_id)
                .copied()
                .unwrap_or(0);
            if seq > since {
                filtered.push((seq, location));
            }
        }
        // Ascending by sequence: replay order, so `removed_files` references
        // resolve against earlier manifests.
        filtered.sort_unstable_by_key(|(s, _)| *s);

        let mut out = Vec::with_capacity(filtered.len());
        for (_, path) in filtered {
            match self.fetch_snapshot(&path).await {
                Ok(s) => out.push(s),
                Err(e) => warn!("skipping inventory entry {}: {}", path, e),
            }
        }
        Ok(out)
    }

    /// List every compaction manifest. Returns `(compaction_id, snapshot)`
    /// pairs in publish order (ULID-sorted ascending). If `since_compaction_id`
    /// is `Some`, only entries with a lexicographically greater id are
    /// returned — the path stem is the ULID so this filtering happens before
    /// the GET.
    pub async fn load_all_compactions(
        &self,
        since_compaction_id: Option<&str>,
    ) -> Result<Vec<(String, PersistedSnapshot)>> {
        let dir = compaction_dir();
        let mut filtered: Vec<(String, ObjPath)> = Vec::new();
        let mut listing = self.object_store.list(Some(&dir));
        while let Some(item) = listing.next().await {
            let location = item?.location;
            let parts: Vec<_> = location.parts().collect();
            let Some(filename) = parts.last().map(|p| p.as_ref().to_string()) else {
                continue;
            };
            let id = filename.trim_end_matches(".compaction.json").to_string();
            if let Some(since) = since_compaction_id {
                if id.as_str() <= since {
                    continue;
                }
            }
            filtered.push((id, location));
        }
        filtered.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        let mut out = Vec::with_capacity(filtered.len());
        for (id, path) in filtered {
            match self.fetch_snapshot(&path).await {
                Ok(s) => out.push((id, s)),
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
        let since_per_node = checkpoint
            .as_ref()
            .map(|cp| cp.wal_high_water.clone())
            .unwrap_or_default();
        let since_compaction = checkpoint
            .as_ref()
            .and_then(|cp| cp.compactions_high_water.as_deref());

        let wal_snapshots = self.load_all_wal_snapshots(&since_per_node).await?;
        let compactions_raw = self.load_all_compactions(since_compaction).await?;

        // Track watermarks so a downstream poller can start from here without
        // re-fetching anything we already saw.
        let mut wal_watermarks = since_per_node;
        for s in &wal_snapshots {
            let entry = wal_watermarks
                .entry(s.node_id.clone())
                .or_insert(0);
            let seq = s.snapshot_sequence_number.as_u64();
            if seq > *entry {
                *entry = seq;
            }
        }
        let compaction_watermark = compactions_raw
            .last()
            .map(|(id, _)| id.clone())
            .or_else(|| since_compaction.map(str::to_string));

        let compactions: Vec<PersistedSnapshot> =
            compactions_raw.into_iter().map(|(_, s)| s).collect();

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
            wal_watermarks,
            compaction_watermark,
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
    /// Highest WAL snapshot sequence seen per node — feeds the inventory
    /// poller's starting cursor.
    pub wal_watermarks: HashMap<String, u64>,
    /// Highest compaction ULID seen — feeds the inventory poller's starting
    /// cursor for compaction listings.
    pub compaction_watermark: Option<String>,
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
    async fn publish_records_metrics() {
        use metric::{Attributes, Metric, U64Counter};
        let registry = metric::Registry::default();
        let inv = SharedInventory::new(Arc::new(InMemory::new())).with_metrics(&registry);

        inv.publish_wal_snapshot("node-a", &snap_with("node-a", 1, "a/1.parquet"))
            .await
            .unwrap();
        inv.publish_compaction("c1", &snap_with("comp", 0, "c/1.parquet"))
            .await
            .unwrap();

        let publishes = registry
            .get_instrument::<Metric<U64Counter>>("influxdb3_shared_inventory_publish")
            .unwrap();
        for kind in ["wal_snapshot", "compaction"] {
            assert_eq!(
                1,
                publishes
                    .get_observer(&Attributes::from(&[("kind", kind), ("result", "ok")]))
                    .unwrap()
                    .fetch(),
                "expected one ok publish for kind {kind}"
            );
        }
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
        let loaded = inv
            .load_all_wal_snapshots(&HashMap::new())
            .await
            .unwrap();
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
        let loaded = inv.load_all_compactions(None).await.unwrap();
        assert_eq!(loaded.len(), 2);
        let ids: Vec<_> = loaded.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(ids, vec!["0001", "0002"]);
    }

    #[tokio::test]
    async fn since_cursors_skip_already_seen_entries() {
        let inv = SharedInventory::new(Arc::new(InMemory::new()));
        for seq in 1..=5u64 {
            inv.publish_wal_snapshot(
                "node-a",
                &snap_with("node-a", seq, &format!("a/{seq}.parquet")),
            )
            .await
            .unwrap();
        }
        let since: HashMap<String, u64> =
            [("node-a".to_string(), 3u64)].into_iter().collect();
        let loaded = inv.load_all_wal_snapshots(&since).await.unwrap();
        let seqs: Vec<u64> = loaded
            .iter()
            .map(|s| s.snapshot_sequence_number.as_u64())
            .collect();
        assert_eq!(seqs, vec![4, 5]);

        for id in ["01A", "01B", "01C"] {
            inv.publish_compaction(id, &snap_with("compactor", 0, "x.parquet"))
                .await
                .unwrap();
        }
        let loaded = inv.load_all_compactions(Some("01A")).await.unwrap();
        let ids: Vec<&str> = loaded.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(ids, vec!["01B", "01C"]);
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
