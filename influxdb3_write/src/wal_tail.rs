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

use arrow::array::RecordBatch;
use data_types::{ChunkId, ChunkOrder, PartitionHashId, PartitionKey, TimestampMinMax};
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
        let guard = self.state.read();

        // Coalesce batches across every un-persisted WAL file (and writer) by
        // partition (chunk_time) into one chunk per partition. The tail holds up
        // to --wal-tail-max-files files; emitting one chunk per (file x partition)
        // — as before — fans a single partition out into thousands of tiny chunks,
        // and the downstream dedup/merge then pays per-chunk overhead that
        // dominates query latency (the reason a large tail is slow). `files` is a
        // BTreeMap keyed by WAL sequence, so iteration is chronological: newer
        // writes land later in each partition's batch vec, and dedup (last in scan
        // order wins) keeps them.
        let mut by_partition: std::collections::HashMap<i64, (TimestampMinMax, Vec<RecordBatch>)> =
            std::collections::HashMap::new();
        for (_writer, tail) in guard.iter() {
            for (_seq, state) in tail.files.iter() {
                let Some(db_buffer) = state.db_to_table.get(&db_schema.id) else {
                    continue;
                };
                let Some(table_buffer) = db_buffer.get(&table_def.table_id) else {
                    continue;
                };
                // Skip a whole file's buffer before materializing anything when it
                // doesn't overlap the query's time window — this is what makes a
                // large tail cheap for time-bounded (recent) queries.
                let buffer_tmm = table_buffer.timestamp_min_max();
                if !filter.test_time_stamp_min_max(buffer_tmm.min, buffer_tmm.max) {
                    continue;
                }
                let partitioned = table_buffer
                    .partitioned_record_batches(Arc::clone(&table_def), filter)
                    .map_err(|e| {
                        DataFusionError::Execution(format!("wal tail batches: {e}"))
                    })?;
                for (chunk_time, (ts_min_max, batches)) in partitioned {
                    if batches.is_empty() {
                        continue;
                    }
                    let entry = by_partition
                        .entry(chunk_time)
                        .or_insert_with(|| (ts_min_max, Vec::new()));
                    entry.0 = entry.0.union(&ts_min_max);
                    entry.1.extend(batches);
                }
            }
        }

        let mut out: Vec<Arc<dyn QueryChunk>> = Vec::with_capacity(by_partition.len());
        for (_chunk_time, (ts_min_max, batches)) in by_partition {
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

    use crate::chunk::BufferChunk;
    use crate::test_helpers::TestWriter;
    use influxdb3_wal::WalOp;

    /// Build a tail buffer with `n_files` WAL files, each replaying `op`, under a
    /// single writer.
    fn tail_with_n_files(
        catalog: &Arc<Catalog>,
        op: &WalOp,
        n_files: u64,
    ) -> Arc<WalTailBuffer> {
        let buf = WalTailBuffer::new(
            Arc::new(InMemory::new()),
            Arc::clone(catalog),
            vec!["w1".to_string()],
            n_files as usize,
        );
        let mut wt = WriterTail::new();
        for seq in 1..=n_files {
            wt.add_file(
                WalFileSequenceNumber::new(seq),
                std::slice::from_ref(op),
                Arc::clone(catalog),
                n_files as usize,
                0,
            );
        }
        buf.state.write().insert("w1".to_string(), wt);
        buf
    }

    fn total_rows(chunks: &[Arc<dyn QueryChunk>]) -> usize {
        chunks
            .iter()
            .map(|c| {
                c.as_any()
                    .downcast_ref::<BufferChunk>()
                    .unwrap()
                    .batches
                    .iter()
                    .map(|b| b.num_rows())
                    .sum::<usize>()
            })
            .sum()
    }

    #[tokio::test]
    async fn coalesces_tail_chunks_per_partition() {
        let catalog = Arc::new(Catalog::new_in_memory("t").await.unwrap());
        let writer = TestWriter::new_with_catalog(Arc::clone(&catalog));
        // All rows fall in the same gen1 partition (tiny, close timestamps).
        let batch = writer
            .write_lp_to_write_batch("cpu,host=a usage=1i 1000", 0)
            .await;
        let buf = tail_with_n_files(&catalog, &WalOp::Write(batch), 5);

        let db_schema = catalog.db_schema(TestWriter::DB_NAME).unwrap();
        let table_def = db_schema.table_definition("cpu").unwrap();
        let filter = ChunkFilter::new(&table_def, &[]).unwrap();

        let chunks = buf
            .get_table_chunks(Arc::clone(&db_schema), Arc::clone(&table_def), &filter, 0)
            .unwrap();

        // 5 WAL files, one partition -> ONE coalesced chunk (was 5 per-file
        // chunks before the fix), and every row is preserved.
        assert_eq!(chunks.len(), 1, "tail chunks coalesce to one per partition");
        assert_eq!(total_rows(&chunks), 5, "all rows preserved across files");
    }

    #[tokio::test]
    async fn time_prunes_non_overlapping_tail_files() {
        let catalog = Arc::new(Catalog::new_in_memory("t").await.unwrap());
        let writer = TestWriter::new_with_catalog(Arc::clone(&catalog));
        // Data at t=1000ns.
        let batch = writer
            .write_lp_to_write_batch("cpu,host=a usage=1i 1000", 0)
            .await;
        let buf = tail_with_n_files(&catalog, &WalOp::Write(batch), 3);

        let db_schema = catalog.db_schema(TestWriter::DB_NAME).unwrap();
        let table_def = db_schema.table_definition("cpu").unwrap();

        // Window entirely after the data -> every file pruned, no materialization.
        let mut future = ChunkFilter::new(&table_def, &[]).unwrap();
        future.time_lower_bound_ns = Some(1_000_000_000_000);
        let pruned = buf
            .get_table_chunks(Arc::clone(&db_schema), Arc::clone(&table_def), &future, 0)
            .unwrap();
        assert!(pruned.is_empty(), "non-overlapping files are pruned");

        // Overlapping window -> data returned.
        let mut overlap = ChunkFilter::new(&table_def, &[]).unwrap();
        overlap.time_lower_bound_ns = Some(0);
        let kept = buf
            .get_table_chunks(Arc::clone(&db_schema), Arc::clone(&table_def), &overlap, 0)
            .unwrap();
        assert_eq!(kept.len(), 1, "overlapping window returns the partition");
        assert_eq!(total_rows(&kept), 3);
    }

    /// Unit-test benchmark: with a large tail (many un-persisted WAL files all
    /// touching one partition), the old code emitted one chunk per file — the
    /// fan-out that made a big tail slow downstream (dedup/merge cost scales with
    /// chunk count). Confirm the new code collapses it to a single chunk and stays
    /// fast. Run with: `cargo test -p influxdb3_write bench_tail_get_table_chunks -- --nocapture`.
    #[tokio::test]
    async fn bench_tail_get_table_chunks_at_scale() {
        let catalog = Arc::new(Catalog::new_in_memory("t").await.unwrap());
        let writer = TestWriter::new_with_catalog(Arc::clone(&catalog));
        let batch = writer
            .write_lp_to_write_batch("cpu,host=a usage=1i 1000", 0)
            .await;

        let db_schema = catalog.db_schema(TestWriter::DB_NAME).unwrap();
        let table_def = db_schema.table_definition("cpu").unwrap();
        let filter = ChunkFilter::new(&table_def, &[]).unwrap();

        for n_files in [100u64, 1000, 2000] {
            let buf = tail_with_n_files(&catalog, &WalOp::Write(batch.clone()), n_files);
            let start = std::time::Instant::now();
            let chunks = buf
                .get_table_chunks(Arc::clone(&db_schema), Arc::clone(&table_def), &filter, 0)
                .unwrap();
            let elapsed = start.elapsed();
            println!(
                "tail get_table_chunks: files={n_files:>4}  chunks_emitted={:>4} \
                 (pre-fix would be {n_files})  rows={:>5}  elapsed={elapsed:?}",
                chunks.len(),
                total_rows(&chunks),
            );
            assert_eq!(
                chunks.len(),
                1,
                "one partition coalesces to one chunk regardless of file count"
            );
            assert_eq!(total_rows(&chunks), n_files as usize);
        }
    }
}
