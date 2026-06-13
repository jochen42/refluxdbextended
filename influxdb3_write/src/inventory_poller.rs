//! Background task that re-reads `_inventory/*` periodically and folds new
//! WAL snapshots and compaction manifests into the local `PersistedFiles`
//! state. Without this, a querier loaded its inventory exactly once at
//! startup and never saw peer writes — the only fix today is a process
//! restart.
//!
//! The poller is the single writer of `PersistedFiles` after the initial
//! load. It also pulls the catalog forward to the highest
//! `catalog_sequence_number` referenced by any new snapshot, so newly-added
//! tables become queryable without restart.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::shared_inventory::SharedInventory;
use crate::wal_tail::WalTailBuffer;
use crate::write_buffer::persisted_files::PersistedFiles;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_shutdown::ShutdownToken;
use observability_deps::tracing::{debug, warn};
use tokio::task::JoinHandle;

#[derive(Debug)]
pub struct InventoryPollerArgs {
    pub inventory: SharedInventory,
    pub persisted_files: Arc<PersistedFiles>,
    pub catalog: Arc<Catalog>,
    pub interval: Duration,
    pub initial_wal_watermarks: HashMap<String, u64>,
    pub initial_compaction_watermark: Option<String>,
    pub shutdown: ShutdownToken,
    /// When present, the poller notifies the WAL tail of each persisted
    /// snapshot's covered-through `wal_file_sequence_number`, so the tail
    /// drops files that are now redundant with persisted parquet.
    pub wal_tail: Option<Arc<WalTailBuffer>>,
    pub metric_registry: Arc<metric::Registry>,
    /// Flipped to `true` after the first successful tick, so a querier can gate
    /// query readiness on having converged its loaded view with the live shared
    /// inventory before serving (avoids empty results in the cold-load window).
    /// `None` leaves readiness ungated.
    pub first_tick_ready: Option<Arc<std::sync::atomic::AtomicBool>>,
    /// Published after each successful tick with the poller's current cursors,
    /// so the compactor (on the same node) can stamp them as the high-water
    /// marks of the inventory checkpoint it writes. `None` on non-compactor
    /// nodes, which write no checkpoints.
    pub watermarks_out: Option<SharedInventoryWatermarks>,
}

/// How far the inventory poller has folded — the high-water marks describing a
/// node's `PersistedFiles` view. Shared so the compactor can stamp them into the
/// checkpoint it writes, letting loaders trust the checkpoint's `merged_snapshot`
/// and skip every wal/compaction manifest at or below these marks instead of
/// replaying the whole history on top.
#[derive(Debug, Default, Clone)]
pub struct InventoryWatermarks {
    /// `node_id -> highest folded snapshot_sequence_number`.
    pub wal: HashMap<String, u64>,
    /// Highest folded compaction id (ULID), or `None`.
    pub compaction: Option<String>,
}

impl InventoryWatermarks {
    /// Build a shared handle seeded with the boot-load cursors, so a reader sees
    /// sane marks before the first poll tick republishes them.
    pub fn shared(
        wal: HashMap<String, u64>,
        compaction: Option<String>,
    ) -> SharedInventoryWatermarks {
        Arc::new(parking_lot::RwLock::new(Self { wal, compaction }))
    }
}

pub type SharedInventoryWatermarks = Arc<parking_lot::RwLock<InventoryWatermarks>>;

#[derive(Debug)]
struct PollerMetrics {
    ticks: metric::Metric<metric::U64Counter>,
    folded: metric::Metric<metric::U64Counter>,
    duration: metric::DurationHistogram,
}

impl PollerMetrics {
    fn new(registry: &metric::Registry) -> Self {
        let ticks = registry.register_metric::<metric::U64Counter>(
            "influxdb3_inventory_poll_ticks",
            "inventory poller ticks by result",
        );
        let folded = registry.register_metric::<metric::U64Counter>(
            "influxdb3_inventory_folded",
            "manifests folded from the shared inventory by kind",
        );
        let duration = registry
            .register_metric::<metric::DurationHistogram>(
                "influxdb3_inventory_poll_duration",
                "wall-clock duration of inventory poll ticks",
            )
            .recorder(&[]);
        Self {
            ticks,
            folded,
            duration,
        }
    }
}

/// Counts of manifests applied by one poll tick.
#[derive(Debug, Default, Clone, Copy)]
struct TickSummary {
    wal_snapshots: usize,
    compactions: usize,
}

impl TickSummary {
    fn total(&self) -> usize {
        self.wal_snapshots + self.compactions
    }
}

pub fn spawn(args: InventoryPollerArgs) -> JoinHandle<()> {
    tokio::spawn(async move { run(args).await })
}

async fn run(args: InventoryPollerArgs) {
    let InventoryPollerArgs {
        inventory,
        persisted_files,
        catalog,
        interval,
        initial_wal_watermarks,
        initial_compaction_watermark,
        shutdown,
        wal_tail,
        metric_registry,
        first_tick_ready,
        watermarks_out,
    } = args;

    let metrics = PollerMetrics::new(&metric_registry);
    let mut wal_cursors = initial_wal_watermarks;
    let mut compaction_cursor = initial_compaction_watermark;
    let cancel = shutdown.clone_cancellation_token();

    loop {
        // Tick first (no initial sleep) so the loaded view converges with the
        // live inventory as soon as possible — this is what gates query
        // readiness, so any delay here is a 503 window for the querier.
        let start = std::time::Instant::now();
        let outcome = tick(
            &inventory,
            &persisted_files,
            &catalog,
            &mut wal_cursors,
            &mut compaction_cursor,
            wal_tail.as_deref(),
        )
        .await;
        metrics.duration.record(start.elapsed());
        match outcome {
            Ok(applied) => {
                metrics.ticks.recorder(&[("result", "ok")]).inc(1);
                if applied.wal_snapshots > 0 {
                    metrics
                        .folded
                        .recorder(&[("kind", "wal_snapshot")])
                        .inc(applied.wal_snapshots as u64);
                }
                if applied.compactions > 0 {
                    metrics
                        .folded
                        .recorder(&[("kind", "compaction")])
                        .inc(applied.compactions as u64);
                }
                if applied.total() > 0 {
                    debug!(
                        applied = applied.total(),
                        "inventory poller applied new snapshots"
                    );
                }
                // Converged with the live inventory at least once — safe to
                // serve queries now (idempotent thereafter).
                if let Some(ready) = &first_tick_ready {
                    ready.store(true, std::sync::atomic::Ordering::Relaxed);
                }
                // Publish the advanced cursors so a co-located compactor stamps
                // them as the checkpoint high-water. Always <= what snapshot_all()
                // reflects (cursors advance only via folds already applied to the
                // shared PersistedFiles), so the checkpoint base is never ahead of
                // its marks — the safe direction.
                if let Some(out) = &watermarks_out {
                    *out.write() = InventoryWatermarks {
                        wal: wal_cursors.clone(),
                        compaction: compaction_cursor.clone(),
                    };
                }
            }
            Err(e) => {
                metrics.ticks.recorder(&[("result", "error")]).inc(1);
                warn!("inventory poll tick failed: {}", e);
            }
        }

        tokio::select! {
            _ = cancel.cancelled() => {
                debug!("inventory poller shutting down");
                return;
            }
            _ = tokio::time::sleep(interval) => {}
        }
    }
}

async fn tick(
    inventory: &SharedInventory,
    persisted_files: &PersistedFiles,
    catalog: &Catalog,
    wal_cursors: &mut HashMap<String, u64>,
    compaction_cursor: &mut Option<String>,
    wal_tail: Option<&WalTailBuffer>,
) -> Result<TickSummary, crate::shared_inventory::InventoryError> {
    // Pull catalog forward unconditionally on every tick. Without this, a
    // querier never sees new databases or tables until something writes an
    // inventory entry referencing them — and on a fresh stack the inventory
    // is empty until the writer's first WAL flush, so the querier returns
    // "database not found" for several seconds after the first write.
    // `update_to_sequence_number` walks the `_catalog/catalogs/*` log until
    // NOT_FOUND, so passing a sentinel high value just pulls everything new.
    let before_seq = catalog.sequence_number().get();
    match catalog
        .update_to_sequence_number(influxdb3_catalog::catalog::CatalogSequenceNumber::new(
            u64::MAX - 1,
        ))
        .await
    {
        Ok(()) => {
            let after_seq = catalog.sequence_number().get();
            if after_seq != before_seq {
                observability_deps::tracing::info!(
                    before_seq, after_seq,
                    "inventory poller advanced catalog"
                );
            } else {
                debug!(seq = before_seq, "inventory poller catalog tick (no advance)");
            }
        }
        Err(e) => warn!("catalog refresh failed during inventory poll: {}", e),
    }

    let new_wal = inventory.load_all_wal_snapshots(wal_cursors).await?;
    let new_comp = inventory
        .load_all_compactions(compaction_cursor.as_deref())
        .await?;

    if new_wal.is_empty() && new_comp.is_empty() {
        return Ok(TickSummary::default());
    }

    let mut applied = TickSummary::default();
    for s in new_wal {
        let node = s.node_id.clone();
        let seq = s.snapshot_sequence_number.as_u64();
        let wal_seq = s.wal_file_sequence_number.as_u64();
        persisted_files.add_persisted_snapshot_files(s);
        let entry = wal_cursors.entry(node.clone()).or_insert(0);
        if seq > *entry {
            *entry = seq;
        }
        if let Some(tail) = wal_tail {
            tail.evict_up_to(&node, wal_seq);
        }
        applied.wal_snapshots += 1;
    }
    if let Some((last_id, _)) = new_comp.last() {
        *compaction_cursor = Some(last_id.clone());
    }
    for (_, s) in new_comp {
        // Compaction manifests carry the highest covered WAL seq per source
        // writer too. `node_id` on these is the compactor's id; the actual
        // covered writer ids live inside `databases` -> tables -> files. We
        // approximate by evicting using `wal_file_sequence_number` against
        // EVERY known writer; the high-water in WalTailBuffer is monotonic
        // so spurious evictions for non-matching writers stay no-ops.
        let wal_seq = s.wal_file_sequence_number.as_u64();
        if let Some(tail) = wal_tail {
            // The compactor doesn't track per-writer high-water in its
            // manifest, so we leave fine-grained eviction to the next
            // WAL snapshot from each writer.
            let _ = (tail, wal_seq);
        }
        persisted_files.add_persisted_snapshot_files(s);
        applied.compactions += 1;
    }

    Ok(applied)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared_inventory::SharedInventory;
    use crate::{ParquetFile, ParquetFileId, PersistedSnapshot};
    use influxdb3_catalog::catalog::{Catalog, CatalogSequenceNumber};
    use influxdb3_id::{DbId, TableId};
    use influxdb3_shutdown::ShutdownManager;
    use influxdb3_wal::{SnapshotSequenceNumber, WalFileSequenceNumber};
    use object_store::memory::InMemory;

    fn snap(node: &str, seq: u64, file_path: &str) -> PersistedSnapshot {
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
    async fn tick_picks_up_new_wal_snapshots() {
        let object_store = Arc::new(InMemory::new());
        let inv = SharedInventory::new(object_store.clone());
        let persisted = Arc::new(PersistedFiles::new(None));
        let catalog = Arc::new(
            Catalog::new_in_memory("test").await.unwrap(),
        );

        // initial: write one snapshot before poller starts
        inv.publish_wal_snapshot("writer-1", &snap("writer-1", 1, "a/1.parquet"))
            .await
            .unwrap();
        let mut wal_cursors: HashMap<String, u64> = HashMap::new();
        let mut comp_cursor: Option<String> = None;
        let n = tick(&inv, &persisted, &catalog, &mut wal_cursors, &mut comp_cursor, None)
            .await
            .unwrap();
        assert_eq!(n.total(), 1);
        assert_eq!(wal_cursors.get("writer-1"), Some(&1));

        // empty tick: nothing new
        let n = tick(&inv, &persisted, &catalog, &mut wal_cursors, &mut comp_cursor, None)
            .await
            .unwrap();
        assert_eq!(n.total(), 0);

        // publish another, tick picks it up
        inv.publish_wal_snapshot("writer-1", &snap("writer-1", 2, "a/2.parquet"))
            .await
            .unwrap();
        let n = tick(&inv, &persisted, &catalog, &mut wal_cursors, &mut comp_cursor, None)
            .await
            .unwrap();
        assert_eq!(n.total(), 1);
        assert_eq!(n.wal_snapshots, 1);
        assert_eq!(wal_cursors.get("writer-1"), Some(&2));

        // metrics reflect both
        let (count, _, _) = {
            use influxdb3_telemetry::ParquetMetrics;
            persisted.get_metrics()
        };
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn spawn_runs_until_shutdown() {
        let object_store = Arc::new(InMemory::new());
        let inv = SharedInventory::new(object_store.clone());
        let persisted = Arc::new(PersistedFiles::new(None));
        let catalog = Arc::new(
            Catalog::new_in_memory("test").await.unwrap(),
        );

        let shutdown_mgr = ShutdownManager::new_testing();
        let token = shutdown_mgr.register();
        let handle = spawn(InventoryPollerArgs {
            inventory: inv.clone(),
            persisted_files: Arc::clone(&persisted),
            catalog: Arc::clone(&catalog),
            interval: Duration::from_millis(20),
            initial_wal_watermarks: HashMap::new(),
            initial_compaction_watermark: None,
            shutdown: token,
            wal_tail: None,
            metric_registry: Arc::new(metric::Registry::default()),
            first_tick_ready: None,
            watermarks_out: None,
        });

        inv.publish_wal_snapshot("w1", &snap("w1", 1, "a.parquet"))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        {
            use influxdb3_telemetry::ParquetMetrics;
            let (count, _, _) = persisted.get_metrics();
            assert_eq!(count, 1, "poller should have picked up the snapshot");
        }

        shutdown_mgr.shutdown();
        // join should complete promptly
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("poller did not exit after shutdown")
            .unwrap();
    }

    #[tokio::test]
    async fn first_tick_flips_readiness_flag() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let object_store = Arc::new(InMemory::new());
        let inv = SharedInventory::new(object_store.clone());
        let persisted = Arc::new(PersistedFiles::new(None));
        let catalog = Arc::new(Catalog::new_in_memory("test").await.unwrap());
        let ready = Arc::new(AtomicBool::new(false));
        let shutdown_mgr = ShutdownManager::new_testing();

        // Long interval: the flag must be set by the immediate first tick, not a
        // later periodic one — proves the tick-first ordering.
        let handle = spawn(InventoryPollerArgs {
            inventory: inv.clone(),
            persisted_files: Arc::clone(&persisted),
            catalog: Arc::clone(&catalog),
            interval: Duration::from_secs(3600),
            initial_wal_watermarks: HashMap::new(),
            initial_compaction_watermark: None,
            shutdown: shutdown_mgr.register(),
            wal_tail: None,
            metric_registry: Arc::new(metric::Registry::default()),
            first_tick_ready: Some(Arc::clone(&ready)),
            watermarks_out: None,
        });

        let mut flipped = false;
        for _ in 0..200 {
            if ready.load(Ordering::Relaxed) {
                flipped = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(flipped, "readiness flag should flip after the first tick");

        shutdown_mgr.shutdown();
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
    }

    #[tokio::test]
    async fn publishes_watermarks_after_tick() {
        let object_store = Arc::new(InMemory::new());
        let inv = SharedInventory::new(object_store.clone());
        let persisted = Arc::new(PersistedFiles::new(None));
        let catalog = Arc::new(Catalog::new_in_memory("test").await.unwrap());
        let watermarks = InventoryWatermarks::shared(HashMap::new(), None);
        let shutdown_mgr = ShutdownManager::new_testing();

        inv.publish_wal_snapshot("w1", &snap("w1", 7, "a.parquet"))
            .await
            .unwrap();

        let handle = spawn(InventoryPollerArgs {
            inventory: inv.clone(),
            persisted_files: Arc::clone(&persisted),
            catalog: Arc::clone(&catalog),
            interval: Duration::from_millis(20),
            initial_wal_watermarks: HashMap::new(),
            initial_compaction_watermark: None,
            shutdown: shutdown_mgr.register(),
            wal_tail: None,
            metric_registry: Arc::new(metric::Registry::default()),
            first_tick_ready: None,
            watermarks_out: Some(Arc::clone(&watermarks)),
        });

        // After folding the published snapshot, the poller republishes its
        // advanced cursor for w1.
        let mut got = None;
        for _ in 0..200 {
            if let Some(seq) = watermarks.read().wal.get("w1").copied() {
                got = Some(seq);
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(got, Some(7), "poller should publish w1 high-water = 7");

        shutdown_mgr.shutdown();
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
    }
}
