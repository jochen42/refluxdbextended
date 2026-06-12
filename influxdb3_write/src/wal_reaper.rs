//! Orphan-WAL reaper: adopts the WAL of dead writers.
//!
//! A hard-killed writer leaves flushed-but-unsnapshotted WAL files under
//! its node prefix. Until something replays them they are invisible to
//! queries (the inventory only covers snapshotted data) — silent data
//! loss if the writer never comes back, e.g. after a scale-in or a
//! permanent instance replacement. The reaper runs on the compactor (the
//! cluster's janitorial singleton), finds such prefixes, takes the dead
//! writer's per-node lease, replays + snapshots its WAL through the
//! ordinary write-buffer machinery, and releases the lease.
//!
//! A returning writer is safe throughout: it blocks on its per-node lease
//! during boot (serve's acquire-wait loop) until the drain finishes, and
//! the drain is idempotent — replay re-persists the same parquet paths
//! and snapshot publishes are keyed by sequence.

use std::collections::HashSet;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use influxdb3_cache::distinct_cache::DistinctCacheProvider;
use influxdb3_cache::last_cache::LastCacheProvider;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_shutdown::{ShutdownManager, ShutdownToken};
use influxdb3_wal::{WalConfig, object_store::load_all_wal_file_paths};
use iox_time::TimeProvider;
use object_store::ObjectStore;
use object_store::path::Path as ObjPath;
use observability_deps::tracing::{debug, info, warn};
use tokio::task::JoinHandle;

use crate::leases::{Lease, LeaseConfig};
use crate::persister::Persister;
use crate::shared_inventory::SharedInventory;
use crate::write_buffer::{WriteBufferImpl, WriteBufferImplArgs};


#[derive(Debug)]
pub struct WalReaperArgs {
    pub object_store: Arc<dyn ObjectStore>,
    pub inventory: SharedInventory,
    pub executor: Arc<iox_query::exec::Executor>,
    pub time_provider: Arc<dyn TimeProvider>,
    /// WAL config for the drain write buffer; gen1_duration should match
    /// the writers' so adopted rows land in the same chunk layout.
    pub wal_config: WalConfig,
    /// TTL used when taking a dead writer's lease; should match the
    /// writers' `--writer-lease-ttl`.
    pub lease_ttl: Duration,
    pub interval: Duration,
    /// This process' own node id — never reaped.
    pub own_node_id: String,
    pub shutdown: ShutdownToken,
    pub metric_registry: Arc<metric::Registry>,
    pub wal_replay_concurrency_limit: usize,
    pub parquet_snapshot_concurrency_limit: NonZeroUsize,
}

pub fn spawn(args: WalReaperArgs) -> JoinHandle<()> {
    tokio::spawn(async move {
        let drains = args
            .metric_registry
            .register_metric::<metric::U64Counter>(
                "influxdb3_wal_reaper_drains",
                "orphan-WAL drains executed by the reaper, by result",
            );
        let cancel = args.shutdown.clone_cancellation_token();
        info!(interval = ?args.interval, "starting orphan-WAL reaper");
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    debug!("wal reaper shutting down");
                    return;
                }
                _ = tokio::time::sleep(args.interval) => {}
            }
            match tick(&args).await {
                Ok(drained) => {
                    if drained > 0 {
                        drains.recorder(&[("result", "ok")]).inc(drained);
                    }
                }
                Err(e) => {
                    drains.recorder(&[("result", "error")]).inc(1);
                    warn!(error = %e, "wal reaper tick failed");
                }
            }
        }
    })
}

async fn tick(args: &WalReaperArgs) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let mut drained = 0;
    for node_id in discover_node_prefixes(&args.object_store).await? {
        if node_id == args.own_node_id {
            continue;
        }
        if !has_unsnapshotted_wal(args, &node_id).await? {
            continue;
        }

        // Take the dead writer's per-node lease. A live writer keeps its
        // lease refreshed, so try_acquire fails and we skip — only a
        // genuinely dead node's WAL gets adopted.
        let lease = Arc::new(Lease::new(
            LeaseConfig::new(
                ObjPath::from(format!("_locks/writer-{node_id}.lease")),
                format!("reaper-{}", args.own_node_id),
                args.lease_ttl,
            ),
            Arc::clone(&args.object_store),
        ));
        let now_ms = args.time_provider.now().timestamp_millis();
        if !lease.try_acquire(now_ms).await? {
            debug!(node_id, "writer lease still held; not adopting WAL");
            continue;
        }

        // Keep the lease refreshed for the duration of the drain, and
        // release it (so a returning writer can start) when done.
        let lease_guard = ShutdownManager::new_testing();
        let lease_task = crate::leases::run(
            Arc::clone(&lease),
            Arc::clone(&args.time_provider),
            lease_guard.register(),
            None,
        );

        info!(node_id, "adopting orphaned WAL");
        let result = drain_node(args, &node_id).await;

        lease_guard.shutdown();
        let _ = lease_task.await;

        match result {
            Ok(()) => {
                info!(node_id, "orphaned WAL drained and published");
                drained += 1;
            }
            Err(e) => {
                warn!(node_id, error = %e, "orphaned WAL drain failed; will retry");
            }
        }
    }
    Ok(drained)
}

/// Writer node ids known to the cluster: every writer leaves a per-node
/// lease file under `_locks/` (kept after a hard kill — exactly the case
/// we care about) and every snapshot publish leaves entries under
/// `_inventory/wal/`. The union covers any writer that ever flushed WAL.
/// Deliberately not a root `list_with_delimiter`: not all backends
/// support it (LocalFileSystem), and it would surface unrelated prefixes.
async fn discover_node_prefixes(
    object_store: &Arc<dyn ObjectStore>,
) -> Result<Vec<String>, object_store::Error> {
    let mut out: HashSet<String> = HashSet::new();

    let locks_dir = ObjPath::from("_locks");
    let mut listing = object_store.list(Some(&locks_dir));
    while let Some(item) = listing.next().await {
        let location = item?.location;
        if let Some(node) = location
            .filename()
            .and_then(|f| f.strip_prefix("writer-"))
            .and_then(|f| f.strip_suffix(".lease"))
        {
            out.insert(node.to_string());
        }
    }

    let inventory_dir = ObjPath::from(format!(
        "{}/wal",
        crate::shared_inventory::SHARED_INVENTORY_PREFIX
    ));
    let mut listing = object_store.list(Some(&inventory_dir));
    while let Some(item) = listing.next().await {
        let location = item?.location;
        // parts: ["_inventory", "wal", "<node_id>", "<stem>.info.json"]
        let parts: Vec<_> = location.parts().collect();
        if parts.len() >= 4 {
            out.insert(parts[2].as_ref().to_string());
        }
    }

    Ok(out.into_iter().collect())
}

/// True when `{node_id}/wal/` holds files beyond the newest WAL sequence
/// covered by the node's published inventory snapshots.
async fn has_unsnapshotted_wal(
    args: &WalReaperArgs,
    node_id: &str,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let paths =
        load_all_wal_file_paths(Arc::clone(&args.object_store), node_id.to_string()).await?;
    let Some(max_seq) = paths.iter().filter_map(parse_wal_seq).max() else {
        return Ok(false);
    };
    let covered = args
        .inventory
        .latest_covered_wal_seq(node_id)
        .await?
        .unwrap_or(0);
    Ok(max_seq > covered)
}

fn parse_wal_seq(path: &ObjPath) -> Option<u64> {
    let stem = path.filename()?.strip_suffix(".wal")?;
    stem.parse::<u64>().ok()
}

/// Replay and snapshot the orphan's WAL through a short-lived
/// `WriteBufferImpl` on its node prefix. Construction replays the WAL
/// beyond the node's last snapshot; `force_flush_buffer` then snapshots
/// everything buffered, which persists parquet, publishes to the shared
/// inventory, and deletes the snapshotted WAL files.
async fn drain_node(
    args: &WalReaperArgs,
    node_id: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let persister = Arc::new(Persister::new(
        Arc::clone(&args.object_store),
        node_id,
        Arc::clone(&args.time_provider),
        None,
    ));
    // Fresh, private catalog instance (and metric registry) for the drain.
    // The cache providers and write buffer subscribe to their catalog's
    // update channels by fixed names; sharing the process' long-lived
    // catalog would collide with the subscriptions the main components
    // already hold — and leak dead subscribers into it when the drain
    // buffer is dropped.
    let drain_metrics = Arc::new(metric::Registry::default());
    let catalog = Arc::new(
        Catalog::open_shared(
            Arc::clone(&args.object_store),
            Arc::clone(&args.time_provider),
            Arc::clone(&drain_metrics),
        )
        .await?,
    );
    let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog)).await?;
    let distinct_cache = DistinctCacheProvider::new_from_catalog(
        Arc::clone(&args.time_provider),
        Arc::clone(&catalog),
    )
    .await?;

    // Dedicated shutdown domain so the drain buffer's background tasks
    // (flush ticker etc.) stop when the drain is done — they must not
    // keep appending Noop WAL files to a prefix we just emptied.
    let drain_shutdown = ShutdownManager::new_testing();

    let write_buffer = WriteBufferImpl::new(WriteBufferImplArgs {
        persister,
        catalog,
        last_cache,
        distinct_cache,
        time_provider: Arc::clone(&args.time_provider),
        executor: Arc::clone(&args.executor),
        wal_config: args.wal_config,
        parquet_cache: None,
        metric_registry: drain_metrics,
        snapshotted_wal_files_to_keep: 0,
        query_file_limit: None,
        n_snapshots_to_load_on_start: NonZeroU64::new(1_000).unwrap(),
        shutdown: drain_shutdown.register(),
        wal_replay_concurrency_limit: args.wal_replay_concurrency_limit,
        parquet_snapshot_concurrency_limit: args.parquet_snapshot_concurrency_limit,
        shared_inventory: Some(args.inventory.clone()),
    })
    .await?;

    let result: Result<(), Box<dyn std::error::Error + Send + Sync>> = async {
        let wal = write_buffer.wal();
        if let Some((snapshot_done, snapshot_info, snapshot_permit)) =
            wal.force_flush_buffer().await
        {
            let details = snapshot_done
                .await
                .map_err(|e| format!("snapshot did not complete: {e}"))?;
            debug!(node_id, ?details, "drain snapshot completed");
            wal.cleanup_snapshot(snapshot_info, snapshot_permit).await;
        }
        Ok(())
    }
    .await;

    drain_shutdown.shutdown();
    drain_shutdown.join().await;
    result
}
