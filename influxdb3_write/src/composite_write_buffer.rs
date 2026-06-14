//! Querier-side `WriteBuffer` composition. Wraps the local
//! `WriteBufferImpl` (which serves persisted parquet files via the
//! inventory poller in Layer A) with a `WalTailBuffer` (Layer C).
//! Implements `WriteBuffer` by delegating writes to the local buffer
//! (which errors with `NoWriteInReadOnly` on the querier) and merging
//! chunks from both sources when reads happen.
//!
//! Chunk-order policy (highest wins, IOx ReorgPlanner dedupes by primary
//! key + time):
//! - Persisted (Layer A): chunk's own `ChunkOrder` derived from gen+chunk_time
//! - WAL tail (Layer C): `i64::MAX - 2`
//! - Local hot (writer / all): `i64::MAX`

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

use crate::wal_tail::WalTailBuffer;
use crate::write_buffer::WriteBufferImpl;
use crate::{
    BufferedWriteRequest, Bufferer, ChunkContainer, ChunkFilter, DistinctCacheManager,
    LastCacheManager, ParquetFile, PersistedSnapshotVersion, Precision, WriteBuffer,
};

/// Ordering used so the dedupe layer prefers more-recent provenance.
pub const CHUNK_ORDER_WAL_TAIL: i64 = i64::MAX - 2;

pub struct CompositeWriteBuffer {
    local: Arc<WriteBufferImpl>,
    tail: Option<Arc<WalTailBuffer>>,
}

impl std::fmt::Debug for CompositeWriteBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompositeWriteBuffer")
            .field("has_tail", &self.tail.is_some())
            .finish_non_exhaustive()
    }
}

impl CompositeWriteBuffer {
    pub fn new(local: Arc<WriteBufferImpl>, tail: Option<Arc<WalTailBuffer>>) -> Self {
        Self { local, tail }
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

        // Layer C (WAL tail): surface the writer's un-persisted WAL rows so
        // recent writes are visible before their snapshot lands in Layer A.
        let mut tail_count = 0;
        if let Some(tail) = &self.tail {
            let tail_chunks = tail.get_table_chunks(
                Arc::clone(&db_schema),
                Arc::clone(&table_def),
                filter,
                CHUNK_ORDER_WAL_TAIL,
            )?;
            tail_count = tail_chunks.len();
            chunks.extend(tail_chunks);
        }

        info!(?db_schema.id, ?table_def.table_id,
            local = local_count, tail = tail_count,
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
