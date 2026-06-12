//! Querier-side `WriteBuffer` composition. Wraps the local
//! `WriteBufferImpl` (which serves persisted parquet files via the
//! inventory poller in Layer A) with an optional `RemoteWriteBuffer`
//! (Layer B) and `WalTailBuffer` (Layer C). Implements `WriteBuffer` by
//! delegating writes to the local buffer (which errors with
//! `NoWriteInReadOnly` on the querier) and merging chunks from all three
//! sources when reads happen.
//!
//! Chunk-order policy (highest wins, IOx ReorgPlanner dedupes by primary
//! key + time):
//! - Persisted (Layer A): provenance-banded order from
//!   `chunk::persisted_chunk_order` (generation band + WAL sequence)
//! - WAL tail (Layer C): `i64::MAX - 2`
//! - Remote hot (Layer B): `i64::MAX - 1`
//! - Local hot (writer / all): `i64::MAX`
//!
//! All sources share the per-table partition id from
//! `chunk::table_partition_id`, so overlapping rows dedupe across sources
//! and across writers.

use std::sync::Arc;

use async_trait::async_trait;
use data_types::NamespaceName;
use datafusion::catalog::Session;
use datafusion::common::DataFusionError;
use influxdb3_cache::distinct_cache::DistinctCacheProvider;
use influxdb3_cache::last_cache::LastCacheProvider;
use influxdb3_catalog::catalog::{Catalog, DatabaseSchema, TableDefinition};
use influxdb3_id::{DbId, TableId};
use influxdb3_wal::Wal;
use iox_query::QueryChunk;
use iox_time::Time;
use observability_deps::tracing::info;

use crate::remote_write_buffer::{RemoteWriteBuffer, batches_to_buffer_chunks};
use crate::wal_tail::WalTailBuffer;
use crate::write_buffer::WriteBufferImpl;
use crate::{
    BufferedWriteRequest, Bufferer, ChunkContainer, ChunkFilter, DistinctCacheManager,
    LastCacheManager, ParquetFile, PersistedSnapshotVersion, Precision, WriteBuffer,
};

/// Ordering used so the dedupe layer prefers more-recent provenance.
pub const CHUNK_ORDER_REMOTE_HOT: i64 = i64::MAX - 1;
pub const CHUNK_ORDER_WAL_TAIL: i64 = i64::MAX - 2;

pub struct CompositeWriteBuffer {
    local: Arc<WriteBufferImpl>,
    remote: Option<Arc<RemoteWriteBuffer>>,
    tail: Option<Arc<WalTailBuffer>>,
}

impl std::fmt::Debug for CompositeWriteBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompositeWriteBuffer")
            .field("has_remote", &self.remote.is_some())
            .field("has_tail", &self.tail.is_some())
            .finish_non_exhaustive()
    }
}

impl CompositeWriteBuffer {
    pub fn new(
        local: Arc<WriteBufferImpl>,
        remote: Option<Arc<RemoteWriteBuffer>>,
        tail: Option<Arc<WalTailBuffer>>,
    ) -> Self {
        Self {
            local,
            remote,
            tail,
        }
    }
}

#[async_trait]
impl Bufferer for CompositeWriteBuffer {
    async fn write_lp(
        &self,
        database: NamespaceName<'static>,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
        no_sync: bool,
    ) -> crate::write_buffer::Result<BufferedWriteRequest> {
        self.local
            .write_lp(database, lp, ingest_time, accept_partial, precision, no_sync)
            .await
    }

    fn catalog(&self) -> Arc<Catalog> {
        self.local.catalog()
    }

    fn wal(&self) -> Arc<dyn Wal> {
        self.local.wal()
    }

    fn parquet_files_filtered(
        &self,
        db_id: DbId,
        table_id: TableId,
        filter: &ChunkFilter<'_>,
    ) -> Vec<ParquetFile> {
        self.local.parquet_files_filtered(db_id, table_id, filter)
    }

    fn watch_persisted_snapshots(
        &self,
    ) -> tokio::sync::watch::Receiver<Option<PersistedSnapshotVersion>> {
        self.local.watch_persisted_snapshots()
    }

    async fn hot_record_batches(
        &self,
        db_id: DbId,
        table_id: TableId,
        time_min_ns: Option<i64>,
        time_max_ns: Option<i64>,
    ) -> Result<Vec<arrow::array::RecordBatch>, DataFusionError> {
        // The querier's composite is a *consumer* of hot chunks, not a
        // producer. Surface only what the local buffer holds (which in
        // querier mode is empty). The remote/tail sources are surfaced
        // through `get_table_chunks` instead.
        self.local
            .hot_record_batches(db_id, table_id, time_min_ns, time_max_ns)
            .await
    }
}

impl ChunkContainer for CompositeWriteBuffer {
    fn get_table_chunks(
        &self,
        db_schema: Arc<DatabaseSchema>,
        table_def: Arc<TableDefinition>,
        filter: &ChunkFilter<'_>,
        projection: Option<&Vec<usize>>,
        ctx: &dyn Session,
    ) -> crate::Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        let mut chunks = self.local.get_table_chunks(
            Arc::clone(&db_schema),
            Arc::clone(&table_def),
            filter,
            projection,
            ctx,
        )?;

        let local_count = chunks.len();

        let mut remote_count = 0;
        let mut remote_reachable = false;
        // Which writers' WAL tails still need to be consulted. `None` means
        // "all of them" (no remote layer, or every writer unreachable);
        // `Some(excluded)` lists writers whose hot rows Layer B already
        // delivered. With legacy unmapped configs (`--writer-urls` without
        // node ids) any Layer B success skips the tail entirely, preserving
        // the old all-or-nothing semantics.
        let mut tail_excluded: Option<std::collections::HashSet<String>> = None;
        let mut skip_tail = false;
        if let Some(remote) = &self.remote {
            // Block the current task on the remote fetch. `block_in_place`
            // ensures we don't starve the runtime; the surrounding
            // `get_table_chunks` is a sync trait so we have no async
            // hook to thread through.
            let db_id = db_schema.id;
            let table_id = table_def.table_id;
            let time_min_ns = filter.time_lower_bound_ns;
            let time_max_ns = filter.time_upper_bound_ns;
            let remote = Arc::clone(remote);
            let result = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async move {
                    remote
                        .fetch_hot_chunks(db_id, table_id, time_min_ns, time_max_ns)
                        .await
                })
            });
            if let Some(fetch) = result {
                remote_reachable = true;
                if fetch.fully_mapped {
                    tail_excluded = Some(fetch.reachable_node_ids);
                } else {
                    skip_tail = true;
                }
                if !fetch.batches.is_empty() {
                    let influx_schema = table_def.influx_schema().clone();
                    let remote_chunks = batches_to_buffer_chunks(
                        fetch.batches,
                        influx_schema,
                        CHUNK_ORDER_REMOTE_HOT,
                        db_schema.id,
                        table_def.table_id,
                    );
                    remote_count = remote_chunks.len();
                    chunks.extend(remote_chunks);
                }
            }
        }

        // Layer C (WAL tail) is the fallback for writer-unreachable
        // scenarios. A writer that answered Layer B is authoritative for
        // its own fresh rows, so re-reading that writer's WAL prefix is
        // wasted work — but with several writers, the ones that did NOT
        // answer must still be served from their tails or their fresh
        // rows silently vanish from results.
        let mut tail_count = 0;
        if let Some(tail) = &self.tail {
            if !skip_tail {
                let tail_chunks = tail.get_table_chunks(
                    Arc::clone(&db_schema),
                    Arc::clone(&table_def),
                    filter,
                    CHUNK_ORDER_WAL_TAIL,
                    tail_excluded.as_ref(),
                )?;
                tail_count = tail_chunks.len();
                chunks.extend(tail_chunks);
            }
        }

        info!(?db_schema.id, ?table_def.table_id,
            local = local_count, remote = remote_count,
            remote_reachable, tail = tail_count,
            total = chunks.len(),
            "composite get_table_chunks");
        Ok(chunks)
    }
}

#[async_trait::async_trait]
impl DistinctCacheManager for CompositeWriteBuffer {
    fn distinct_cache_provider(&self) -> Arc<DistinctCacheProvider> {
        self.local.distinct_cache_provider()
    }
}

#[async_trait::async_trait]
impl LastCacheManager for CompositeWriteBuffer {
    fn last_cache_provider(&self) -> Arc<LastCacheProvider> {
        self.local.last_cache_provider()
    }
}

impl WriteBuffer for CompositeWriteBuffer {
    fn persisted_files(&self) -> Arc<dyn std::any::Any + Send + Sync> {
        self.local.persisted_files()
    }
}
