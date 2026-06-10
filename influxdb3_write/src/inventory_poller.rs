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
    } = args;

    let mut wal_cursors = initial_wal_watermarks;
    let mut compaction_cursor = initial_compaction_watermark;
    let cancel = shutdown.clone_cancellation_token();

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                debug!("inventory poller shutting down");
                return;
            }
            _ = tokio::time::sleep(interval) => {}
        }

        match tick(
            &inventory,
            &persisted_files,
            &catalog,
            &mut wal_cursors,
            &mut compaction_cursor,
            wal_tail.as_deref(),
        )
        .await
        {
            Ok(applied) if applied > 0 => {
                debug!(applied, "inventory poller applied new snapshots");
            }
            Ok(_) => {}
            Err(e) => warn!("inventory poll tick failed: {}", e),
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
) -> Result<usize, crate::shared_inventory::InventoryError> {
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
        return Ok(0);
    }

    let mut applied = 0;
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
        applied += 1;
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
        applied += 1;
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
        assert_eq!(n, 1);
        assert_eq!(wal_cursors.get("writer-1"), Some(&1));

        // empty tick: nothing new
        let n = tick(&inv, &persisted, &catalog, &mut wal_cursors, &mut comp_cursor, None)
            .await
            .unwrap();
        assert_eq!(n, 0);

        // publish another, tick picks it up
        inv.publish_wal_snapshot("writer-1", &snap("writer-1", 2, "a/2.parquet"))
            .await
            .unwrap();
        let n = tick(&inv, &persisted, &catalog, &mut wal_cursors, &mut comp_cursor, None)
            .await
            .unwrap();
        assert_eq!(n, 1);
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
}
