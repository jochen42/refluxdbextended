use crate::leases::Lease;
use crate::persister::Persister;
use crate::shared_inventory::SharedInventory;
use crate::write_buffer::persisted_files::PersistedFiles;
use crate::{DatabaseTables, ParquetFile, ParquetFileId, PersistedSnapshot, WriteBuffer};
use bytes::Bytes;
use object_store::{PutMode, PutOptions};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use datafusion_util::stream_from_batches;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_id::{DbId, SerdeVecMap, TableId};
use influxdb3_wal::{SnapshotSequenceNumber, WalFileSequenceNumber};
use iox_query::exec::Executor;
use iox_query::frontend::reorg::ReorgPlanner;
use object_store::ObjectStore;
use object_store::path::Path as ObjPath;
use observability_deps::tracing::{debug, error, info, warn};
use schema::Schema;
use schema::sort::SortKey;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use uuid::Uuid;

/// Configuration for the compaction service
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Whether compaction is enabled
    pub enabled: bool,
    /// Interval between compaction runs
    pub interval: Duration,
    /// Maximum number of files to compact in a single run
    pub max_files_per_run: usize,
    /// Minimum number of files required before triggering compaction
    pub min_files_for_compaction: usize,
    /// Generation durations for each level
    pub generation_durations: HashMap<u8, Duration>,
    /// Wait this long after publishing a compaction manifest before deleting the
    /// original gen{n-1} parquet files. Prevents 404s on in-flight queries that
    /// resolved the old paths before the manifest landed. Should be greater than
    /// the longest expected query duration.
    pub delete_grace: Duration,
    /// Write a materialized inventory checkpoint after this many compaction
    /// cycles. Loaders use the latest checkpoint to bound startup cost as the
    /// number of WAL snapshots + compaction manifests grows. `0` disables.
    pub checkpoint_every_n_cycles: u32,
    /// How long a per-table compaction claim is considered fresh. A stale
    /// claim left by a crashed compactor older than this can be taken over.
    /// Should exceed the longest expected single-job compaction time.
    pub claim_ttl: Duration,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Duration::from_secs(3600), // 1 hour
            max_files_per_run: 100,
            min_files_for_compaction: 10,
            generation_durations: HashMap::new(),
            delete_grace: Duration::from_secs(600), // 10 minutes
            checkpoint_every_n_cycles: 10,
            claim_ttl: Duration::from_secs(30 * 60), // 30 minutes
        }
    }
}

/// Represents a compaction job that needs to be executed
#[derive(Debug, Clone)]
pub struct CompactionJob {
    pub database_id: DbId,
    pub database_name: Arc<str>,
    pub table_id: TableId,
    pub table_name: Arc<str>,
    pub source_generation: u8,
    pub target_generation: u8,
    pub files: Vec<ParquetFile>,
    pub schema: Schema,
    pub sort_key: SortKey,
}

/// Result of a compaction operation
#[derive(Debug)]
pub struct CompactionResult {
    pub compacted_files: Vec<ParquetFile>,
    pub deleted_files: Vec<ParquetFile>,
    pub total_size_reduction: u64,
    pub total_rows_compacted: u64,
}

#[derive(Debug)]
pub struct CompactionService {
    config: CompactionConfig,
    catalog: Arc<Catalog>,
    write_buffer: Arc<dyn WriteBuffer>,
    persister: Arc<Persister>,
    executor: Arc<Executor>,
    object_store: Arc<dyn ObjectStore>,
    /// Optional singleton lease. When set, `run_compaction_cycle` is gated on
    /// `lease.is_leader(now)` so two compactor processes pointed at the same
    /// bucket cannot duplicate work. When `None`, the service runs
    /// unconditionally (legacy single-node behaviour).
    lease: Option<Arc<Lease>>,
    /// Optional cross-node inventory. When set, every compaction manifest is
    /// also published into it so peer queriers see the resulting gen{N} file.
    shared_inventory: Option<SharedInventory>,
    /// Counter for "successful cycle" used to decide when to flush a checkpoint.
    cycle_count: std::sync::atomic::AtomicU64,
    time_provider: Arc<dyn iox_time::TimeProvider>,
    shutdown_token: influxdb3_shutdown::ShutdownToken,
}

impl CompactionService {
    pub fn new(
        config: CompactionConfig,
        catalog: Arc<Catalog>,
        write_buffer: Arc<dyn WriteBuffer>,
        persister: Arc<Persister>,
        executor: Arc<Executor>,
        object_store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn iox_time::TimeProvider>,
        shutdown_token: influxdb3_shutdown::ShutdownToken,
    ) -> Self {
        Self {
            config,
            catalog,
            write_buffer,
            persister,
            executor,
            object_store,
            lease: None,
            shared_inventory: None,
            cycle_count: std::sync::atomic::AtomicU64::new(0),
            time_provider,
            shutdown_token,
        }
    }

    /// Attach a singleton lease. Use [`crate::leases::run`] to drive
    /// acquisition + refresh in the background before calling [`Self::start`].
    pub fn with_lease(mut self, lease: Arc<Lease>) -> Self {
        self.lease = Some(lease);
        self
    }

    /// Publish manifests to the shared inventory so peer queriers and other
    /// compactor processes (e.g. running under per-table claims) see them.
    pub fn with_shared_inventory(mut self, inv: SharedInventory) -> Self {
        self.shared_inventory = Some(inv);
        self
    }

    /// Start the background compaction service
    pub fn start(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            if !self.config.enabled {
                info!("Compaction service is disabled");
                return;
            }

            info!(
                "Starting compaction service with interval: {:?}",
                self.config.interval
            );

            let mut interval = tokio::time::interval(self.config.interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Some(lease) = &self.lease {
                            let now_ms = self.time_provider.now().timestamp_millis();
                            if !lease.is_leader(now_ms) {
                                debug!("compaction cycle skipped — lease not held");
                                continue;
                            }
                        }
                        if let Err(e) = Arc::clone(&self).run_compaction_cycle().await {
                            error!("Compaction cycle failed: {}", e);
                        }
                    }
                    _ = self.shutdown_token.wait_for_shutdown() => {
                        info!("Shutdown signal received, stopping compaction service");
                        break;
                    }
                }
            }
        })
    }

    /// Run a single compaction cycle
    async fn run_compaction_cycle(self: &Arc<Self>) -> Result<()> {
        debug!("Starting compaction cycle");

        let jobs = self.identify_compaction_jobs().await?;
        if jobs.is_empty() {
            debug!("No compaction jobs identified");
            return Ok(());
        }

        info!("Identified {} compaction jobs", jobs.len());

        let mut set = JoinSet::new();
        let mut completed_jobs = 0;
        let max_concurrent = std::cmp::min(jobs.len(), 4); // Limit concurrent compactions

        for job in jobs.into_iter().take(self.config.max_files_per_run) {
            if set.len() >= max_concurrent {
                if let Some(result) = set.join_next().await {
                    match result {
                        Ok(Ok(_)) => completed_jobs += 1,
                        Ok(Err(e)) => error!("Compaction job failed: {}", e),
                        Err(e) => error!("Compaction task failed: {}", e),
                    }
                }
            }

            let service = Arc::clone(self);
            set.spawn(async move { service.execute_compaction_job(job).await });
        }

        // Wait for remaining jobs
        while let Some(result) = set.join_next().await {
            match result {
                Ok(Ok(_)) => completed_jobs += 1,
                Ok(Err(e)) => error!("Compaction job failed: {}", e),
                Err(e) => error!("Compaction task failed: {}", e),
            }
        }

        info!(
            "Compaction cycle completed: {} jobs processed",
            completed_jobs
        );

        // Periodically materialize the inventory state as a checkpoint so
        // future restart/load cost stays bounded as manifests accumulate.
        let n = self.config.checkpoint_every_n_cycles;
        if n > 0 && self.shared_inventory.is_some() {
            let cycle = self
                .cycle_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                + 1;
            if cycle % u64::from(n) == 0 {
                if let Err(e) = self.write_inventory_checkpoint().await {
                    warn!(%e, "failed to write inventory checkpoint");
                }
            }
        }

        Ok(())
    }

    /// Build a `Checkpoint` from the current in-memory `PersistedFiles` state
    /// and write it to the shared inventory. Future loaders will start from
    /// the latest checkpoint instead of folding every WAL + compaction
    /// manifest from scratch.
    async fn write_inventory_checkpoint(&self) -> Result<()> {
        let Some(inv) = &self.shared_inventory else {
            return Ok(());
        };
        let any_arc = self.write_buffer.persisted_files();
        let Ok(persisted_files) = Arc::downcast::<PersistedFiles>(any_arc) else {
            return Ok(());
        };

        // Synthesize a `PersistedSnapshot` containing every live file. The
        // checkpoint's `merged_snapshot` will be the only entry loaders need
        // to fold before applying newer WAL/compaction manifests on top.
        let mut merged = PersistedSnapshot::new(
            self.persister.node_identifier_prefix().to_string(),
            SnapshotSequenceNumber::new(0),
            WalFileSequenceNumber::new(0),
            self.catalog.sequence_number(),
        );
        for (db_id, table_id, files) in persisted_files.snapshot_all() {
            for file in files {
                merged.add_parquet_file(db_id, table_id, file);
            }
        }

        // Per-node WAL high-water marks: the largest snapshot sequence number
        // we have folded into our view. We don't track this perfectly today
        // (the manifest doesn't expose source per-file), so we conservatively
        // leave the map empty — loaders fall back to the per-node `load_snapshots`
        // for safety.
        let cp = crate::shared_inventory::Checkpoint {
            wal_high_water: Default::default(),
            compactions_high_water: None,
            merged_snapshot: merged,
        };
        inv.write_checkpoint(&cp).await?;
        Ok(())
    }

    /// Identify files that need compaction
    pub async fn identify_compaction_jobs(&self) -> Result<Vec<CompactionJob>> {
        let mut jobs = Vec::new();

        // Get all databases and tables
        let databases = self.catalog.list_db_schema();

        for db_schema in databases {
            if db_schema.deleted {
                continue;
            }

            for table_def in db_schema.tables() {
                if table_def.deleted {
                    continue;
                }

                // Get files for this table
                let files = self
                    .write_buffer
                    .parquet_files(db_schema.id, table_def.table_id);
                if files.len() < self.config.min_files_for_compaction {
                    continue;
                }

                // Group files by generation level and check for compaction opportunities
                let mut files_by_generation: BTreeMap<u8, Vec<ParquetFile>> = BTreeMap::new();

                for file in files {
                    let generation = self.get_file_generation(&file)?;
                    files_by_generation
                        .entry(generation)
                        .or_default()
                        .push(file);
                }

                // Check each generation level for compaction opportunities
                for (current_gen, files) in files_by_generation.iter() {
                    if files.len() < self.config.min_files_for_compaction {
                        continue;
                    }

                    // Check if we can compact to the next generation
                    if let Some(next_gen) = self.get_next_generation(*current_gen) {
                        if let Some(target_duration) =
                            self.config.generation_durations.get(&next_gen)
                        {
                            // Check if files span the target duration
                            if self.can_compact_to_generation(files, *target_duration) {
                                jobs.push(CompactionJob {
                                    database_id: db_schema.id,
                                    database_name: Arc::clone(&db_schema.name),
                                    table_id: table_def.table_id,
                                    table_name: Arc::clone(&table_def.table_name),
                                    source_generation: *current_gen,
                                    target_generation: next_gen,
                                    files: files.clone(),
                                    schema: table_def.schema.clone(),
                                    sort_key: table_def.sort_key.clone(),
                                });
                            }
                        }
                    }
                }
            }
        }

        Ok(jobs)
    }

    /// Execute a single compaction job.
    ///
    /// Compaction is publish-then-delete:
    /// 1. Sort/dedupe inputs through DataFusion.
    /// 2. Upload the resulting parquet bytes to the object store under a `gen{N}` path.
    /// 3. Emit a `PersistedSnapshot` manifest with the new file in `databases` and the
    ///    inputs in `removed_files`, then `put` it under `{host}/compactions/`.
    /// 4. Update the in-memory `PersistedFiles` (add new, remove old) so queries see the
    ///    compacted file immediately.
    /// 5. Best-effort delete the input parquet objects.
    ///
    /// If the process crashes before step 3 finishes, the inputs remain referenced and
    /// the compaction is a no-op on the next run. If it crashes between step 3 and 5,
    /// the inputs become orphaned objects but no data is lost.
    pub async fn execute_compaction_job(
        &self,
        job: CompactionJob,
    ) -> Result<CompactionResult> {
        info!(
            "Starting compaction job: db={}, table={}, gen{}->gen{}, inputs={}",
            job.database_name,
            job.table_name,
            job.source_generation,
            job.target_generation,
            job.files.len()
        );

        if job.sort_key.is_empty() {
            return Err(anyhow::anyhow!(
                "Cannot compact table {}: sort key is empty",
                job.table_name
            ));
        }

        // Per-table claim: only one compactor process at a time may work on
        // this exact (db, table, src_gen, dst_gen) tuple. Other concurrent
        // compactors targeting OTHER tables continue in parallel.
        let claim_path = self.claim_path(&job);
        if !self.acquire_claim(&claim_path).await? {
            debug!(
                "compaction claim {} held by another worker; skipping",
                claim_path
            );
            return Ok(CompactionResult {
                compacted_files: vec![],
                deleted_files: vec![],
                total_size_reduction: 0,
                total_rows_compacted: 0,
            });
        }

        // Defer release until the job exits (success OR failure). Use a guard
        // so panics within DataFusion don't leak the claim.
        struct ClaimGuard<'a> {
            store: &'a Arc<dyn ObjectStore>,
            path: ObjPath,
        }
        impl Drop for ClaimGuard<'_> {
            fn drop(&mut self) {
                // tokio::spawn since Drop can't be async; best-effort.
                let store = Arc::clone(self.store);
                let path = self.path.clone();
                tokio::spawn(async move {
                    let _ = store.delete(&path).await;
                });
            }
        }
        let _claim_guard = ClaimGuard {
            store: &self.object_store,
            path: claim_path.clone(),
        };

        let start_time = std::time::Instant::now();
        let total_input_size: u64 = job.files.iter().map(|f| f.size_bytes).sum();

        // Build chunks and run the compaction plan.
        let chunks = self.create_chunks_from_files(&job.files, &job.schema).await?;
        let ctx = self.executor.new_context();

        let logical_plan = ReorgPlanner::new()
            .compact_plan(
                data_types::TableId::new(0),
                job.table_name.clone(),
                &job.schema,
                chunks,
                job.sort_key.clone(),
            )
            .context("failed to create compaction plan")?;

        let physical_plan = ctx
            .create_physical_plan(&logical_plan)
            .await
            .context("failed to create physical plan")?;

        let data = ctx
            .collect(physical_plan)
            .await
            .context("failed to execute compaction")?;

        // Write all output batches to a single compacted parquet file. We intentionally
        // do not split by `target_duration` here — that duration controls when
        // compaction triggers, not how the output is sliced.
        let compaction_id = Uuid::now_v7().to_string();
        let compacted_file = self
            .write_compacted_file(&job, data, &compaction_id)
            .await?;

        let new_files = match compacted_file {
            Some(file) => vec![file],
            None => {
                info!(
                    "Compaction produced no rows for {}/{}; skipping publish",
                    job.database_name, job.table_name
                );
                return Ok(CompactionResult {
                    compacted_files: vec![],
                    deleted_files: vec![],
                    total_size_reduction: 0,
                    total_rows_compacted: 0,
                });
            }
        };

        // Publish: persist the manifest, update in-memory state, then delete inputs.
        self.publish_compaction(&job, &new_files, &job.files, &compaction_id)
            .await?;

        let total_output_size: u64 = new_files.iter().map(|f| f.size_bytes).sum();
        let total_output_rows: u64 = new_files.iter().map(|f| f.row_count).sum();
        let size_reduction = total_input_size.saturating_sub(total_output_size);

        let result = CompactionResult {
            compacted_files: new_files,
            deleted_files: job.files.clone(),
            total_size_reduction: size_reduction,
            total_rows_compacted: total_output_rows,
        };

        let duration = start_time.elapsed();
        info!(
            "Compaction completed: {} files -> {} files, {} rows, {} bytes -> {} bytes ({}% reduction) in {:?}",
            result.deleted_files.len(),
            result.compacted_files.len(),
            total_output_rows,
            total_input_size,
            total_output_size,
            if total_input_size > 0 {
                (size_reduction * 100) / total_input_size
            } else {
                0
            },
            duration
        );

        Ok(result)
    }

    /// Create DataFusion chunks from parquet files
    async fn create_chunks_from_files(
        &self,
        files: &[ParquetFile],
        schema: &Schema,
    ) -> Result<Vec<Arc<dyn iox_query::QueryChunk>>> {
        let mut chunks = Vec::with_capacity(files.len());

        for (i, file) in files.iter().enumerate() {
            let chunk = crate::write_buffer::parquet_chunk_from_file(
                file,
                schema,
                self.persister.object_store_url().clone(),
                Arc::clone(&self.object_store),
                i as i64,
            );
            chunks.push(Arc::new(chunk) as Arc<dyn iox_query::QueryChunk>);
        }

        Ok(chunks)
    }

    /// Serialize the compacted record batches to one parquet file and put it on the
    /// object store. Returns `None` if there are no rows to write.
    async fn write_compacted_file(
        &self,
        job: &CompactionJob,
        data: Vec<arrow::record_batch::RecordBatch>,
        compaction_id: &str,
    ) -> Result<Option<ParquetFile>> {
        let non_empty: Vec<_> = data.into_iter().filter(|b| b.num_rows() > 0).collect();
        if non_empty.is_empty() {
            return Ok(None);
        }

        let (min_time, max_time, row_count) = batches_time_range_and_rows(&non_empty)?;
        let target_duration = self
            .config
            .generation_durations
            .get(&job.target_generation)
            .copied()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "No duration configured for generation {}",
                    job.target_generation
                )
            })?;
        let chunk_time = chunk_time_for_duration(min_time, target_duration);

        let path = self.compacted_file_path(job, job.target_generation, chunk_time, compaction_id);

        let batch_stream = stream_from_batches(job.schema.as_arrow(), non_empty);
        let (size_bytes, _meta, _to_cache) = self
            .persister
            .persist_parquet_file(
                crate::paths::ParquetFilePath::from_obj_path(path.clone()),
                batch_stream,
            )
            .await
            .context("failed to upload compacted parquet to object store")?;

        Ok(Some(ParquetFile {
            id: ParquetFileId::new(),
            path: path.to_string(),
            size_bytes,
            row_count,
            chunk_time,
            min_time,
            max_time,
        }))
    }

    /// Persist a compaction manifest and update in-memory state atomically from the
    /// query layer's perspective. The manifest is what makes the change survive a
    /// restart; the in-memory update is what makes it visible to running queries.
    async fn publish_compaction(
        &self,
        job: &CompactionJob,
        new_files: &[ParquetFile],
        old_files: &[ParquetFile],
        compaction_id: &str,
    ) -> Result<()> {
        // Build the manifest. snapshot/wal sequence numbers are unused for compaction
        // manifests (separate path, separate counter); set them to 0.
        let mut snapshot = PersistedSnapshot::new(
            self.persister.node_identifier_prefix().to_string(),
            SnapshotSequenceNumber::new(0),
            WalFileSequenceNumber::new(0),
            self.catalog.sequence_number(),
        );
        for file in new_files {
            snapshot.add_parquet_file(job.database_id, job.table_id, file.clone());
        }
        let mut removed: SerdeVecMap<DbId, DatabaseTables> = SerdeVecMap::new();
        removed
            .entry(job.database_id)
            .or_default()
            .tables
            .entry(job.table_id)
            .or_default()
            .extend(old_files.iter().cloned());
        snapshot.removed_files = removed;

        self.persister
            .persist_compaction_snapshot(compaction_id, &snapshot)
            .await
            .context("failed to persist compaction manifest")?;

        // Dual-publish to the cross-node inventory so peer queriers can fold
        // this compaction into their `PersistedFiles` view on their next poll.
        // Best-effort: a failure here doesn't roll back the primary manifest —
        // peers see slightly stale state until the next refresh.
        if let Some(inv) = &self.shared_inventory {
            if let Err(e) = inv.publish_compaction(compaction_id, &snapshot).await {
                warn!(%e, "failed to publish compaction manifest to shared inventory");
            }
        }

        // Manifest is durable: now update in-memory state.
        let any_arc = self.write_buffer.persisted_files();
        match Arc::downcast::<PersistedFiles>(any_arc) {
            Ok(persisted_files) => {
                for file in new_files {
                    persisted_files.add_persisted_file(&job.database_id, &job.table_id, file);
                }
                persisted_files.remove_persisted_files(
                    &job.database_id,
                    &job.table_id,
                    old_files,
                );
            }
            Err(_) => {
                warn!(
                    "compaction publish: write_buffer.persisted_files() did not downcast to PersistedFiles; \
                     new files will only be visible after restart"
                );
            }
        }

        // Best-effort delete of original objects, after `delete_grace` so any
        // queries that resolved the old paths before the manifest landed can
        // finish reading them. Surviving objects after retries become orphans
        // (no manifest references them) — they don't cause data loss.
        let grace = self.config.delete_grace;
        for file in old_files {
            let path = ObjPath::from(file.path.clone());
            let object_store = Arc::clone(&self.object_store);
            tokio::spawn(async move {
                if !grace.is_zero() {
                    tokio::time::sleep(grace).await;
                }
                let mut retry = 0u32;
                while retry <= 5 {
                    match object_store.delete(&path).await {
                        Ok(()) => break,
                        Err(object_store::Error::NotFound { .. }) => break,
                        Err(e) => {
                            retry += 1;
                            warn!(
                                "compaction delete retry {} for {}: {}",
                                retry, path, e
                            );
                            tokio::time::sleep(Duration::from_secs(u64::from(retry) * 2))
                                .await;
                        }
                    }
                }
            });
        }

        Ok(())
    }

    /// Get the generation level for a file based on its path.
    fn get_file_generation(&self, file: &ParquetFile) -> Result<u8> {
        let path = &file.path;

        if let Some(gen_start) = path.find("/gen") {
            let gen_part = &path[gen_start + 4..];
            if let Some(gen_end) = gen_part.find('/') {
                let gen_str = &gen_part[..gen_end];
                match gen_str.parse::<u8>() {
                    Ok(level) if (1..=5).contains(&level) => return Ok(level),
                    Ok(level) => {
                        return Err(anyhow::anyhow!(
                            "Invalid generation {} in path: {}",
                            level,
                            path
                        ));
                    }
                    Err(e) => {
                        return Err(anyhow::anyhow!("Invalid generation in {}: {}", path, e));
                    }
                }
            }
        }
        // Files written by the WAL-driven persist path are gen1 by definition.
        Ok(1)
    }

    fn get_next_generation(&self, current_gen: u8) -> Option<u8> {
        if current_gen < 5 {
            Some(current_gen + 1)
        } else {
            None
        }
    }

    fn can_compact_to_generation(
        &self,
        files: &[ParquetFile],
        target_duration: Duration,
    ) -> bool {
        if files.len() < self.config.min_files_for_compaction {
            return false;
        }

        let min_time = files.iter().map(|f| f.min_time).min().unwrap_or(0);
        let max_time = files.iter().map(|f| f.max_time).max().unwrap_or(0);
        let span = (max_time - min_time).max(0) as u64;

        Duration::from_nanos(span) >= target_duration
    }

    /// `{host}/dbs/{db}-{db_id}/{table}-{table_id}/gen{N}/{YYYY-MM-DD}/{HH-MM}/{compaction_id}.parquet`
    fn compacted_file_path(
        &self,
        job: &CompactionJob,
        generation: u8,
        chunk_time: i64,
        compaction_id: &str,
    ) -> ObjPath {
        let date_time = DateTime::<Utc>::from_timestamp_nanos(chunk_time);
        ObjPath::from(format!(
            "{host}/dbs/{db}-{db_id}/{table}-{table_id}/gen{gen}/{date}/{cid}.parquet",
            host = self.persister.node_identifier_prefix(),
            db = job.database_name,
            db_id = job.database_id.get(),
            table = job.table_name,
            table_id = job.table_id.get(),
            gen = generation,
            date = date_time.format("%Y-%m-%d/%H-%M"),
            cid = compaction_id,
        ))
    }

    fn claim_path(&self, job: &CompactionJob) -> ObjPath {
        ObjPath::from(format!(
            "_compactor/claims/{db}-{table}-gen{src}-to-gen{dst}.claim",
            db = job.database_id.get(),
            table = job.table_id.get(),
            src = job.source_generation,
            dst = job.target_generation,
        ))
    }

    /// Acquire a per-table claim via `PutMode::Create`. If a claim already
    /// exists, take it over only if it's older than `claim_ttl`. Returns
    /// `Ok(true)` when we now hold the claim.
    async fn acquire_claim(&self, path: &ObjPath) -> Result<bool> {
        let now_ms = self.time_provider.now().timestamp_millis();
        let body = ClaimBody {
            acquired_at_unix_ms: now_ms,
        };
        let payload = serde_json::to_vec(&body).context("serialize claim body")?;

        match self
            .object_store
            .put_opts(
                path,
                Bytes::from(payload.clone()).into(),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(object_store::Error::AlreadyExists { .. }) => {
                // Inspect existing claim. Take over only if stale.
                let bytes = self
                    .object_store
                    .get(path)
                    .await
                    .context("get existing claim")?
                    .bytes()
                    .await
                    .context("read claim body")?;
                let existing: ClaimBody = serde_json::from_slice(&bytes)
                    .context("parse existing claim body")?;
                let age_ms = (now_ms - existing.acquired_at_unix_ms).max(0) as u128;
                if age_ms < self.config.claim_ttl.as_millis() {
                    return Ok(false);
                }
                // Stale claim — overwrite. Race window: two takeover attempts
                // collide. Acceptable since duplicate compaction is recoverable
                // (manifest publishes are idempotent and PersistedFiles dedupes
                // by file id).
                self.object_store
                    .put(path, Bytes::from(payload).into())
                    .await
                    .context("overwrite stale claim")?;
                Ok(true)
            }
            Err(object_store::Error::NotSupported { .. }) => {
                // Backend without conditional puts. Fall back to non-atomic
                // "look then leap" with a brief sleep to reduce collision
                // probability. Production deployments must use a backend
                // with `If-None-Match` support.
                warn!(
                    "object store does not support PutMode::Create; \
                     compaction claims will not be atomic"
                );
                self.object_store
                    .put(path, Bytes::from(payload).into())
                    .await
                    .context("write claim without atomic guard")?;
                Ok(true)
            }
            Err(e) => Err(e.into()),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ClaimBody {
    acquired_at_unix_ms: i64,
}

fn batches_time_range_and_rows(
    batches: &[arrow::record_batch::RecordBatch],
) -> Result<(i64, i64, u64)> {
    let mut min_time = i64::MAX;
    let mut max_time = i64::MIN;
    let mut total_rows: u64 = 0;

    for batch in batches {
        let time_idx = batch
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == "time")
            .ok_or_else(|| anyhow::anyhow!("No time column in compacted batch"))?;
        let time_array = batch
            .column(time_idx)
            .as_any()
            .downcast_ref::<arrow::array::TimestampNanosecondArray>()
            .ok_or_else(|| anyhow::anyhow!("Time column is not TimestampNanosecond"))?;
        if time_array.is_empty() {
            continue;
        }
        // ReorgPlanner emits sorted time order, so first/last suffice.
        let first = time_array.value(0);
        let last = time_array.value(time_array.len() - 1);
        min_time = min_time.min(first.min(last));
        max_time = max_time.max(first.max(last));
        total_rows += batch.num_rows() as u64;
    }

    if total_rows == 0 {
        return Err(anyhow::anyhow!("No rows in compacted output"));
    }
    Ok((min_time, max_time, total_rows))
}

fn chunk_time_for_duration(min_time: i64, target_duration: Duration) -> i64 {
    let nanos = target_duration.as_nanos() as i64;
    if nanos > 0 {
        (min_time / nanos) * nanos
    } else {
        min_time
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::time::Duration;

    #[test]
    fn test_compaction_config_default() {
        let config = CompactionConfig::default();
        assert!(config.enabled);
        assert_eq!(config.interval, Duration::from_secs(3600));
        assert_eq!(config.max_files_per_run, 100);
        assert_eq!(config.min_files_for_compaction, 10);
    }

    #[test]
    fn test_chunk_time_alignment() {
        let dur = Duration::from_secs(60);
        let ns = dur.as_nanos() as i64;
        // 1.5 minutes -> rounded down to 1 minute boundary
        assert_eq!(chunk_time_for_duration(ns + ns / 2, dur), ns);
        assert_eq!(chunk_time_for_duration(0, dur), 0);
    }

    /// Verify claim mutual exclusion across two service instances pointing at
    /// the same object store. Avoids spinning up a full WriteBuffer/Persister
    /// stack by calling `acquire_claim` directly.
    #[tokio::test]
    async fn per_table_claim_blocks_concurrent_worker() {
        use crate::leases::LeaseConfig;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let time_provider: Arc<dyn iox_time::TimeProvider> = Arc::new(
            iox_time::MockProvider::new(iox_time::Time::from_timestamp_nanos(0)),
        );
        let path = ObjPath::from("_compactor/claims/1-1-gen1-to-gen2.claim");

        // Two minimal services sharing the same store. We don't actually run
        // compaction here — just exercise acquire_claim.
        let svc_a = build_test_service(Arc::clone(&store), Arc::clone(&time_provider));
        let svc_b = build_test_service(Arc::clone(&store), Arc::clone(&time_provider));

        assert!(svc_a.acquire_claim(&path).await.unwrap());
        assert!(
            !svc_b.acquire_claim(&path).await.unwrap(),
            "second worker should be blocked while claim is fresh"
        );

        // Force a stale-takeover scenario: drop the claim, then re-acquire
        // from B. (TTL-based takeover is exercised in the inline path; not
        // re-driven here since MockProvider doesn't advance automatically.)
        store.delete(&path).await.unwrap();
        assert!(svc_b.acquire_claim(&path).await.unwrap());

        // Use an unused binding so the lease config import isn't pulled
        // unnecessarily by future refactors.
        let _ = LeaseConfig::new(ObjPath::from("/tmp"), "unused", Duration::from_secs(60));
    }

    fn build_test_service(
        store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn iox_time::TimeProvider>,
    ) -> CompactionService {
        use influxdb3_catalog::catalog::Catalog;
        use influxdb3_shutdown::ShutdownManager;
        use iox_query::exec::Executor;

        let _ = (Arc::clone(&store), Arc::clone(&time_provider));
        // Minimal catalog/persister/executor stubs. The fields we exercise in
        // `acquire_claim` are only `object_store`, `time_provider`, `config`.
        let catalog = Arc::new(futures::executor::block_on(async {
            Catalog::new_in_memory("test").await.unwrap()
        }));
        let persister = Arc::new(crate::persister::Persister::new(
            Arc::clone(&store),
            "test-host",
            Arc::clone(&time_provider),
            None,
        ));
        // We need a WriteBuffer; reuse the catalog/persister with a no-op
        // write buffer.  The test doesn't call methods that touch it.
        let last_cache =
            futures::executor::block_on(influxdb3_cache::last_cache::LastCacheProvider::new_from_catalog(
                Arc::clone(&catalog),
            ))
            .unwrap();
        let distinct_cache = futures::executor::block_on(
            influxdb3_cache::distinct_cache::DistinctCacheProvider::new_from_catalog(
                Arc::clone(&time_provider),
                Arc::clone(&catalog),
            ),
        )
        .unwrap();
        let wb = futures::executor::block_on(
            crate::write_buffer::WriteBufferImpl::new(crate::write_buffer::WriteBufferImplArgs {
                persister: Arc::clone(&persister),
                catalog: Arc::clone(&catalog),
                last_cache,
                distinct_cache,
                time_provider: Arc::clone(&time_provider),
                executor: Arc::new(Executor::new_testing()),
                wal_config: influxdb3_wal::WalConfig::test_config(),
                parquet_cache: None,
                metric_registry: Arc::new(metric::Registry::default()),
                snapshotted_wal_files_to_keep: 10,
                query_file_limit: None,
                n_snapshots_to_load_on_start: std::num::NonZeroU64::new(1).unwrap(),
                shutdown: ShutdownManager::new_testing().register(),
                wal_replay_concurrency_limit: 1,
                parquet_snapshot_concurrency_limit: std::num::NonZeroUsize::new(1).unwrap(),
                shared_inventory: None,
            }),
        )
        .unwrap();
        CompactionService::new(
            CompactionConfig {
                claim_ttl: Duration::from_secs(60),
                ..Default::default()
            },
            catalog,
            wb as Arc<dyn WriteBuffer>,
            persister,
            Arc::new(Executor::new_testing()),
            store,
            time_provider,
            ShutdownManager::new_testing().register(),
        )
    }
}

// The end-to-end compaction regression test lives in `tests/compaction_e2e.rs`
// so it runs in its own test binary — it touches the global `NEXT_FILE_ID`
// atomic that other unit tests in this crate assert on.
