//! Background task for enforcing retention policies by deleting expired data.

use crate::table_index_cache::TableIndexCache;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_id::{DbId, TableId};
use influxdb3_shutdown::ShutdownToken;
use iox_time::TimeProvider;
use observability_deps::tracing::{debug, error, info, warn};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A periodic table index sync slower than this is logged at `warn`.
const SLOW_TABLE_INDEX_SYNC: Duration = Duration::from_secs(300);

/// Handles periodic enforcement of retention policies by deleting expired parquet files.
///
/// Each check also syncs the table indices from newly persisted snapshots, whether or not any
/// retention policy exists, so the snapshot backlog converted at startup stays small.
#[derive(Debug)]
pub struct RetentionPeriodHandler {
    table_index_cache: TableIndexCache,
    catalog: Arc<Catalog>,
    time_provider: Arc<dyn TimeProvider>,
    check_interval: Duration,
    node_id: String,
}

impl RetentionPeriodHandler {
    /// Create a new retention period handler.
    pub fn new(
        table_index_cache: TableIndexCache,
        catalog: Arc<Catalog>,
        time_provider: Arc<dyn TimeProvider>,
        check_interval: Duration,
        node_id: String,
    ) -> Self {
        Self {
            table_index_cache,
            catalog,
            time_provider,
            check_interval,
            node_id,
        }
    }

    /// Run the background task that periodically checks and enforces retention policies.
    pub async fn background_task(self: Arc<Self>, shutdown_token: ShutdownToken) {
        info!(
            check_interval_seconds = self.check_interval.as_secs(),
            "Starting retention period handler background task"
        );

        loop {
            let next_check_time = self.time_provider.now() + self.check_interval;

            tokio::select! {
                _ = self.time_provider.sleep_until(next_check_time) => {
                    self.check_and_enforce_retention().await;
                }
                _ = shutdown_token.wait_for_shutdown() => {
                    info!("Retention period handler shutting down");
                    break;
                }
            }
        }
    }

    /// Check all tables for retention policies and enforce them.
    async fn check_and_enforce_retention(&self) {
        self.sync_table_indices().await;

        debug!("Checking retention policies");

        // Get the retention period cutoff map from the catalog
        let cutoff_map = self.catalog.get_retention_period_cutoff_map();

        if cutoff_map.is_empty() {
            debug!("No retention policies configured");
            return;
        }

        info!(
            num_retention_policies = cutoff_map.len(),
            "Processing retention policies"
        );

        // Process each database and table with a retention policy
        for ((db_id, table_id), cutoff_time_ns) in cutoff_map {
            self.enforce_retention_for_table(db_id, table_id, cutoff_time_ns)
                .await;
        }
    }

    /// Fold newly persisted snapshots into the table indices.
    ///
    /// Without this, the conversion marker only advances at startup (or when a purge runs), so
    /// the next startup has to convert every snapshot since the last one before WAL replay.
    async fn sync_table_indices(&self) {
        let start = Instant::now();
        match self.table_index_cache.sync_from_persisted_snapshots().await {
            Ok(()) => {
                let elapsed = start.elapsed();
                if elapsed > SLOW_TABLE_INDEX_SYNC {
                    warn!(
                        duration_ms = elapsed.as_millis(),
                        "Slow table index sync from persisted snapshots"
                    );
                } else {
                    info!(
                        duration_ms = elapsed.as_millis(),
                        "Synced table indices from persisted snapshots"
                    );
                }
            }
            Err(e) => {
                error!(error = %e, "Failed to sync table indices from persisted snapshots");
            }
        }
    }

    /// Enforce retention for a specific table.
    async fn enforce_retention_for_table(
        &self,
        db_id: DbId,
        table_id: TableId,
        cutoff_time_ns: i64,
    ) {
        debug!(
            ?db_id,
            ?table_id,
            cutoff_time_ns,
            "Enforcing retention policy for table"
        );

        match self
            .table_index_cache
            .purge_expired(&self.node_id, db_id, table_id, cutoff_time_ns)
            .await
        {
            Ok(()) => {
                info!(
                    ?db_id,
                    ?table_id,
                    cutoff_time_ns,
                    "Successfully enforced retention policy"
                );
            }
            Err(e) => {
                error!(
                    ?db_id,
                    ?table_id,
                    cutoff_time_ns,
                    error = %e,
                    "Failed to enforce retention policy"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests;
