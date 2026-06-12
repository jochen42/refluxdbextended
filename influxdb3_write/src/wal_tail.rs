//! Querier-side WAL tail. Lists writers' `_wal/*` prefixes periodically,
//! downloads new WAL files, deserializes them, and replays the ops into
//! per-(writer, table) `BufferState`s. The result is hot data that's
//! readable seconds after ingest — bridging the gap between in-flight writes
//! and the next snapshot/inventory cycle.
//!
//! Eviction model: the inventory poller calls `evict_up_to` the instant a
//! snapshot publishes, dropping every tail file that snapshot covers. That is
//! the routine trimming path, and it only ever drops files already proven
//! redundant with `PersistedFiles`. The per-writer `max_files` cap is a
//! secondary OOM backstop — NOT the routine mechanism.
//!
//! The subtle, load-bearing consequence: because `evict_up_to` has already
//! removed everything persisted, every file the `max_files` cap could drop is
//! still UN-persisted. So the cap must stay larger than the writer's
//! unpersisted window or it punches a query-visible hole — rows that have left
//! the tail but whose parquet has not been published yet. The writer keeps the
//! most recent `--wal-snapshot-size` WAL periods buffered (up to 3x that before
//! force-snapshot), so the cap must comfortably exceed `--wal-snapshot-size`.
//! `add_file` warns loudly if the cap ever evicts an un-persisted file rather
//! than lose data silently. ReorgPlanner dedupes any tail/persisted overlap.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use data_types::{ChunkId, ChunkOrder, PartitionHashId, PartitionKey};
use datafusion::common::DataFusionError;
use influxdb3_catalog::catalog::{Catalog, DatabaseSchema, TableDefinition};
use influxdb3_shutdown::ShutdownToken;
use influxdb3_wal::object_store::load_all_wal_file_paths;
use influxdb3_wal::{WalFileSequenceNumber, serialize};
use iox_query::QueryChunk;
use iox_query::chunk_statistics::{NoColumnRanges, create_chunk_statistics};
use object_store::ObjectStore;
use object_store::path::Path as ObjPath;
use observability_deps::tracing::{debug, warn};
use parking_lot::RwLock;
use tokio::task::JoinHandle;

use crate::ChunkFilter;
use crate::chunk::BufferChunk;
use crate::write_buffer::queryable_buffer::BufferState;

/// Defaults intended for the e2e bench cadence (1s WAL flush). Larger
/// deployments should size `max_files_per_writer` against the gap between
/// flush and snapshot.
#[derive(Debug)]
pub struct WalTailBufferArgs {
    pub poll_interval: Duration,
    pub shutdown: ShutdownToken,
    pub metric_registry: Arc<metric::Registry>,
}

#[derive(Debug)]
struct WriterTail {
    files: BTreeMap<WalFileSequenceNumber, BufferState>,
}

impl WriterTail {
    fn new() -> Self {
        Self {
            files: BTreeMap::new(),
        }
    }

    /// Add a replayed WAL file, evicting the oldest once the per-writer cap is
    /// exceeded. The cap is an OOM backstop; `evict_up_to` does the routine,
    /// correctness-safe trimming. Any file still present here is therefore not
    /// yet known to be in `PersistedFiles`, so a cap eviction whose victim sits
    /// above `high_water` risks a query-visible hole and is warned about — it
    /// means the cap is smaller than the writer's unpersisted window.
    fn add_file(
        &mut self,
        seq: WalFileSequenceNumber,
        ops: &[influxdb3_wal::WalOp],
        catalog: Arc<Catalog>,
        max_files: usize,
        high_water: u64,
    ) {
        let mut state = BufferState::new(catalog);
        state.buffer_write_ops(ops);
        self.files.insert(seq, state);
        while self.files.len() > max_files {
            let Some((&oldest, _)) = self.files.iter().next() else {
                break;
            };
            if oldest.as_u64() > high_water {
                warn!(
                    evicted_seq = oldest.as_u64(),
                    persisted_high_water = high_water,
                    max_files,
                    "wal tail cap evicting an un-persisted WAL file: its rows are \
                     not yet in PersistedFiles, so recent queries will miss them \
                     until the writer's snapshot publishes. Raise \
                     --wal-tail-max-files above the writer's unpersisted window \
                     (comfortably over --wal-snapshot-size)."
                );
            }
            self.files.remove(&oldest);
        }
    }

    fn cursor(&self) -> WalFileSequenceNumber {
        self.files
            .keys()
            .last()
            .copied()
            .unwrap_or_else(|| WalFileSequenceNumber::new(0))
    }
}

#[derive(Debug)]
pub struct WalTailBuffer {
    object_store: Arc<dyn ObjectStore>,
    catalog: Arc<Catalog>,
    writer_node_ids: Vec<String>,
    max_files_per_writer: usize,
    state: RwLock<std::collections::HashMap<String, WriterTail>>,
    /// Per-writer high-water of WAL file sequence numbers known to be
    /// covered by persisted parquet. Tail entries `<= this` are redundant
    /// with persisted data — the planner would still dedupe correctly but
    /// it'd waste time materializing record batches we've already got in
    /// gen1+ parquet.
    persisted_high_water: RwLock<std::collections::HashMap<String, u64>>,
}

impl WalTailBuffer {
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        catalog: Arc<Catalog>,
        writer_node_ids: Vec<String>,
        max_files_per_writer: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            object_store,
            catalog,
            writer_node_ids,
            max_files_per_writer: max_files_per_writer.max(1),
            state: RwLock::new(std::collections::HashMap::new()),
            persisted_high_water: RwLock::new(std::collections::HashMap::new()),
        })
    }

    /// Inventory poller calls this after folding a `PersistedSnapshot`. Tail
    /// entries `<= seq` for that writer become redundant (their data is in
    /// `PersistedFiles` now) and get dropped immediately.
    pub fn evict_up_to(&self, writer_node_id: &str, seq: u64) {
        {
            let mut hw = self.persisted_high_water.write();
            let entry = hw.entry(writer_node_id.to_string()).or_insert(0);
            if seq > *entry {
                *entry = seq;
            } else {
                return;
            }
        }
        let mut guard = self.state.write();
        if let Some(tail) = guard.get_mut(writer_node_id) {
            let drop_keys: Vec<_> = tail
                .files
                .keys()
                .filter(|k| k.as_u64() <= seq)
                .copied()
                .collect();
            for k in drop_keys {
                tail.files.remove(&k);
            }
        }
    }

    pub fn spawn(self: Arc<Self>, args: WalTailBufferArgs) -> JoinHandle<()> {
        let poll_interval = args.poll_interval;
        let shutdown = args.shutdown;
        let files_metric = args.metric_registry.register_metric::<metric::U64Counter>(
            "influxdb3_wal_tail_files",
            "peer WAL files folded into the tail buffer, and tick errors",
        );
        let me = Arc::clone(&self);
        tokio::spawn(async move {
            let cancel = shutdown.clone_cancellation_token();
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        debug!("wal tail buffer shutting down");
                        return;
                    }
                    _ = tokio::time::sleep(poll_interval) => {}
                }
                match me.tick().await {
                    Ok(n) if n > 0 => {
                        files_metric.recorder(&[("result", "ok")]).inc(n as u64);
                    }
                    Ok(_) => {}
                    Err(e) => {
                        files_metric.recorder(&[("result", "error")]).inc(1);
                        warn!("wal tail tick failed: {}", e);
                    }
                }
            }
        })
    }

    pub async fn tick(&self) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        let mut applied = 0;
        for writer_id in &self.writer_node_ids {
            let cursor = self
                .state
                .read()
                .get(writer_id)
                .map(WriterTail::cursor)
                .unwrap_or_else(|| WalFileSequenceNumber::new(0));

            let high_water = self
                .persisted_high_water
                .read()
                .get(writer_id)
                .copied()
                .unwrap_or(0);
            let paths = load_all_wal_file_paths(
                Arc::clone(&self.object_store),
                writer_id.clone(),
            )
            .await?;
            for path in paths {
                let Some(seq) = parse_wal_seq(&path) else {
                    continue;
                };
                if seq <= cursor {
                    continue;
                }
                if seq.as_u64() <= high_water {
                    // already persisted; skip the GET + replay entirely
                    continue;
                }
                let bytes = self.object_store.get(&path).await?.bytes().await?;
                let contents = match serialize::verify_file_type_and_deserialize(bytes) {
                    Ok(c) => c,
                    Err(e) => {
                        warn!(path = %path, error = %e, "wal tail: skipping un-deserializable wal file");
                        continue;
                    }
                };
                {
                    let mut guard = self.state.write();
                    let tail = guard
                        .entry(writer_id.clone())
                        .or_insert_with(WriterTail::new);
                    tail.add_file(
                        contents.wal_file_number,
                        &contents.ops,
                        Arc::clone(&self.catalog),
                        self.max_files_per_writer,
                        high_water,
                    );
                }
                applied += 1;
            }
        }
        Ok(applied)
    }

    pub fn get_table_chunks(
        &self,
        db_schema: Arc<DatabaseSchema>,
        table_def: Arc<TableDefinition>,
        filter: &ChunkFilter<'_>,
        chunk_order: i64,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        let influx_schema = table_def.influx_schema();
        let mut out: Vec<Arc<dyn QueryChunk>> = Vec::new();
        let guard = self.state.read();
        for (_writer, tail) in guard.iter() {
            for (_seq, state) in tail.files.iter() {
                let Some(db_buffer) = state.db_to_table.get(&db_schema.id) else {
                    continue;
                };
                let Some(table_buffer) = db_buffer.get(&table_def.table_id) else {
                    continue;
                };
                let partitioned = table_buffer
                    .partitioned_record_batches(Arc::clone(&table_def), filter)
                    .map_err(|e| {
                        DataFusionError::Execution(format!("wal tail batches: {e}"))
                    })?;
                for (_, (ts_min_max, batches)) in partitioned {
                    if batches.is_empty() {
                        continue;
                    }
                    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                    let stats = create_chunk_statistics(
                        Some(row_count),
                        influx_schema,
                        Some(ts_min_max),
                        &NoColumnRanges,
                    );
                    out.push(Arc::new(BufferChunk {
                        batches,
                        schema: influx_schema.clone(),
                        stats: Arc::new(stats),
                        partition_id: PartitionHashId::new(
                            data_types::TableId::new(0),
                            &PartitionKey::from("wal-tail".to_string()),
                        ),
                        sort_key: None,
                        id: ChunkId::new(),
                        chunk_order: ChunkOrder::new(chunk_order),
                    }) as Arc<dyn QueryChunk>);
                }
            }
        }
        Ok(out)
    }
}

fn parse_wal_seq(path: &ObjPath) -> Option<WalFileSequenceNumber> {
    let filename = path.filename()?;
    let stem = filename.trim_end_matches(".wal");
    let n: u64 = stem.parse().ok()?;
    Some(WalFileSequenceNumber::new(n))
}

#[cfg(test)]
mod tests {
    use super::*;
    use influxdb3_catalog::catalog::Catalog;
    use object_store::memory::InMemory;

    #[test]
    fn parse_seq_handles_canonical_paths() {
        let p = ObjPath::from("writer-1/wal/00000000042.wal");
        let s = parse_wal_seq(&p).unwrap();
        assert_eq!(s.as_u64(), 42);
    }

    #[tokio::test]
    async fn cap_evicts_oldest_keeping_most_recent() {
        // Regression for the WAL-tail vs snapshot-window hole: the cap is an
        // OOM backstop that drops the OLDEST files. With a cap below the
        // writer's unpersisted window, the dropped files are still
        // un-persisted (seq > high_water) — exactly the silent data-loss case
        // add_file now warns about. Here we lock the eviction semantics: only
        // the most recent `max_files` survive.
        let catalog = Arc::new(Catalog::new_in_memory("t").await.unwrap());
        let mut tail = WriterTail::new();
        // high_water = 0 → every file is un-persisted, so each eviction is the
        // hole-punching case.
        for seq in 1..=3u64 {
            tail.add_file(
                WalFileSequenceNumber::new(seq),
                &[],
                Arc::clone(&catalog),
                2,
                0,
            );
        }
        let kept: Vec<u64> = tail.files.keys().map(|k| k.as_u64()).collect();
        assert_eq!(kept, vec![2, 3], "cap must keep the most recent files");
        assert_eq!(tail.cursor().as_u64(), 3);
    }

    #[tokio::test]
    async fn empty_writer_list_tick_is_noop() {
        let store = Arc::new(InMemory::new());
        let catalog = Arc::new(Catalog::new_in_memory("t").await.unwrap());
        let tail = WalTailBuffer::new(store, catalog, vec![], 16);
        let applied = tail.tick().await.unwrap();
        assert_eq!(applied, 0);
    }
}
