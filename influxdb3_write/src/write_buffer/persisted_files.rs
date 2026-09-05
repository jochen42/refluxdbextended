//! This tracks what files have been persisted by the write buffer, limited to the last 72 hours.
//! When queries come in they will combine whatever chunks exist from `QueryableBuffer` with
//! the persisted files to get the full set of data to query.

use std::sync::Arc;

use crate::deleter::ObjectDeleter;
use crate::table_index_cache::TableIndexCache;
use crate::{ChunkFilter, DatabaseTables};
use crate::{ParquetFile, PersistedSnapshot, PersistedSnapshotCheckpoint};
use hashbrown::{HashMap, HashSet};
use influxdb3_catalog::catalog::Catalog;
use influxdb3_id::TableId;
use influxdb3_id::{DbId, SerdeVecMap};
use influxdb3_telemetry::ParquetMetrics;
use observability_deps::tracing::{debug, trace};
use parking_lot::RwLock;

type DatabaseToTables = HashMap<DbId, TableToFiles>;
type TableToFiles = HashMap<TableId, Vec<ParquetFile>>;

#[derive(Debug, Default)]
pub struct PersistedFiles {
    inner: RwLock<Inner>,
    table_index_cache: Option<TableIndexCache>,
}

#[derive(Debug)]
enum DeletedTables {
    /// All tables in the database are marked for deletion
    All,
    /// A list of tables in the database that are marked for deletion
    List(HashSet<TableId>),
}

#[async_trait::async_trait]
impl ObjectDeleter for PersistedFiles {
    async fn delete_database(
        &self,
        db_id: DbId,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        {
            let mut inner = self.inner.write();
            inner.deleted_data.insert(db_id, DeletedTables::All);
        }

        // Purge from table index cache if available
        //
        // NOTE(wayne): in theory we could just leave actual purging of tables to individual
        // `delete_table` calls, but that would potentially removing data for tables that are
        // already removed from the catalog and PersistedFiles but not yet removed from the
        // object store, whereas explicitly purging by database through the TableIndexCache
        // ensures that we are deleting all table data from the object store
        if let Some(table_index_cache) = &self.table_index_cache {
            table_index_cache
                .purge_db(&db_id)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>)?;
        }

        Ok(())
    }

    async fn delete_table(
        &self,
        db_id: DbId,
        table_id: TableId,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        {
            let mut inner = self.inner.write();
            match inner.deleted_data.entry(db_id) {
                hashbrown::hash_map::Entry::Occupied(mut entry) => {
                    match entry.get_mut() {
                        DeletedTables::All => (), // already marked for deletion
                        DeletedTables::List(tables) => {
                            tables.insert(table_id);
                        }
                    }
                }
                hashbrown::hash_map::Entry::Vacant(entry) => {
                    entry.insert(DeletedTables::List(HashSet::from([table_id])));
                }
            }
        }
        if let Some(cache) = &self.table_index_cache {
            cache
                .purge_table(&db_id, &table_id)
                .await
                .map_err(Box::new)?
        }
        Ok(())
    }
}

impl PersistedFiles {
    pub fn new(table_index_cache: Option<TableIndexCache>) -> Self {
        Self {
            table_index_cache,
            ..Default::default()
        }
    }

    /// Create a new `PersistedFiles` from a list of persisted snapshots
    ///
    /// Accepts `Arc<Vec<PersistedSnapshot>>` to allow sharing the snapshot data
    /// between multiple consumers (e.g., PersistedFiles and background checkpoint building)
    /// without cloning the entire Vec.
    pub fn new_from_persisted_snapshots(
        table_index_cache: Option<TableIndexCache>,
        persisted_snapshots: Arc<Vec<PersistedSnapshot>>,
    ) -> Self {
        let inner = Inner::new_from_persisted_snapshots(persisted_snapshots);
        Self {
            table_index_cache,
            inner: RwLock::new(inner),
        }
    }

    /// Create a new `PersistedFiles` from checkpoints and additional snapshots.
    ///
    /// This is the preferred method when checkpoints are available, as it reduces
    /// the amount of data to process during startup:
    /// 1. Merge all checkpoints (one per month, sorted by month)
    /// 2. Apply pending_removed_files from each checkpoint to remove cross-month references
    /// 3. Apply additional snapshots (those newer than the latest checkpoint)
    pub fn new_from_checkpoints_and_snapshots(
        table_index_cache: Option<TableIndexCache>,
        checkpoints: Vec<PersistedSnapshotCheckpoint>,
        additional_snapshots: Vec<PersistedSnapshot>,
    ) -> Self {
        let inner = Inner::new_from_checkpoints_and_snapshots(checkpoints, additional_snapshots);
        Self {
            table_index_cache,
            inner: RwLock::new(inner),
        }
    }

    /// Add all files from a persisted snapshot to the tracked files.
    ///
    /// Deduplicates against existing files.
    ///
    /// Called from `Replica::reload_snapshots` during replica recovery.
    pub fn add_persisted_snapshot_files(&self, persisted_snapshot: PersistedSnapshot) {
        let mut inner = self.inner.write();
        inner.add_persisted_snapshot(persisted_snapshot, None);
    }

    /// Fold a compaction manifest, tagging any tombstone it creates with the
    /// compaction's id so the tombstone can be GC'd against the compaction
    /// high-water. Used by the inventory poller for the compaction stream,
    /// where a removal can arrive before the add it supersedes.
    pub fn add_persisted_compaction_files(
        &self,
        persisted_snapshot: PersistedSnapshot,
        compaction_id: &str,
    ) {
        let mut inner = self.inner.write();
        inner.add_persisted_snapshot(persisted_snapshot, Some(compaction_id));
    }

    /// Add a single parquet file to the tracked files for a specific table.
    ///
    /// Called from `QueryableBuffer` after persistence and from `Replica` during
    /// background cache loading.
    pub fn add_persisted_file(&self, db_id: &DbId, table_id: &TableId, parquet_file: &ParquetFile) {
        let mut inner = self.inner.write();
        inner.add_persisted_file(db_id, table_id, parquet_file);
    }

    /// Remove specific files from a table (for compaction). Only metrics for files
    /// that were actually present are deducted, so callers may pass a superset.
    pub fn remove_persisted_files(
        &self,
        db_id: &DbId,
        table_id: &TableId,
        files_to_remove: &[ParquetFile],
    ) {
        let mut inner = self.inner.write();
        let Some(tables) = inner.files.get_mut(db_id) else {
            return;
        };
        let Some(files) = tables.get_mut(table_id) else {
            return;
        };

        let paths_to_remove: HashSet<&String> = files_to_remove.iter().map(|f| &f.path).collect();

        let (actually_removed_count, actually_removed_size, actually_removed_rows) = files
            .iter()
            .filter(|f| paths_to_remove.contains(&f.path))
            .fold((0u64, 0u64, 0u64), |(c, s, r), f| {
                (c + 1, s + f.size_bytes, r + f.row_count)
            });

        files.retain(|file| !paths_to_remove.contains(&file.path));

        inner.parquet_files_size_mb -= as_mb(actually_removed_size);
        inner.parquet_files_row_count = inner
            .parquet_files_row_count
            .saturating_sub(actually_removed_rows);
        inner.parquet_files_count = inner
            .parquet_files_count
            .saturating_sub(actually_removed_count);
    }

    /// Snapshot every live file across all databases / tables. Used by the
    /// compactor when materializing a shared-inventory checkpoint.
    pub fn snapshot_all(&self) -> Vec<(DbId, TableId, Vec<ParquetFile>)> {
        let inner = self.inner.read();
        let mut out = Vec::new();
        for (db_id, tables) in &inner.files {
            for (table_id, files) in tables {
                if !files.is_empty() {
                    out.push((*db_id, *table_id, files.clone()));
                }
            }
        }
        out
    }

    /// Garbage-collect removed-file tombstones against the current high-water
    /// marks. Called by the inventory poller after each fold tick.
    pub fn gc_tombstones(
        &self,
        wal_high_water: &std::collections::HashMap<String, u64>,
        compactions_high_water: Option<&str>,
    ) {
        self.inner
            .write()
            .gc_tombstones(wal_high_water, compactions_high_water);
    }

    /// Snapshot the live removed-file tombstones for persistence in a
    /// shared-inventory checkpoint, split by GC key: gen1 `(path, wal_seq)` and
    /// compactor `(path, removing_compaction_id)`.
    #[allow(clippy::type_complexity)]
    pub fn tombstones_for_checkpoint(&self) -> (Vec<(String, u64)>, Vec<(String, String)>) {
        let inner = self.inner.read();
        let mut gen1 = Vec::new();
        let mut compaction = Vec::new();
        for (path, gc) in &inner.removed_tombstones {
            match gc {
                TombstoneGc::WalSeq { wal_seq, .. } => gen1.push((path.clone(), *wal_seq)),
                TombstoneGc::Compaction { removing_id } => {
                    compaction.push((path.clone(), removing_id.clone()))
                }
                // Not serialized: re-derived by the validator after restart.
                TombstoneGc::ObjectGone => {}
            }
        }
        (gen1, compaction)
    }

    /// Seed removed-file tombstones from a shared-inventory checkpoint before
    /// folding the newer WAL/compaction manifests on top of it.
    pub fn seed_tombstones(&self, gen1: Vec<(String, u64)>, compaction: Vec<(String, String)>) {
        self.inner.write().seed_tombstones(gen1, compaction);
    }

    /// Number of live removed-file tombstones (test/observability helper).
    pub fn tombstone_count(&self) -> usize {
        self.inner.read().removed_tombstones.len()
    }

    /// Record validator-confirmed missing object paths as durable suppression
    /// tombstones, so a later fold (e.g. a periodic full re-fold relisting a
    /// long-superseded gen1) cannot resurrect them into a phantom ref. Safe
    /// because object-store paths are write-once. Bounded by FIFO.
    pub fn tombstone_evicted_paths(&self, paths: impl IntoIterator<Item = String>) {
        self.inner.write().tombstone_evicted(paths);
    }

    /// Get the list of files for a given database and table, always return in descending order of min_time
    pub fn get_files(&self, db_id: DbId, table_id: TableId) -> Vec<ParquetFile> {
        self.get_files_filtered(db_id, table_id, &ChunkFilter::default())
    }

    /// Get the list of files for a given database and table, using the provided filter to filter results.
    ///
    /// Always return in descending order of min_time
    pub fn get_files_filtered(
        &self,
        db_id: DbId,
        table_id: TableId,
        filter: &ChunkFilter<'_>,
    ) -> Vec<ParquetFile> {
        let inner = self.inner.read();
        let mut files = inner
            .files
            .get(&db_id)
            .and_then(|tables| tables.get(&table_id))
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|file| filter.test_time_stamp_min_max(file.min_time, file.max_time))
            .collect::<Vec<_>>();

        files.sort_by_key(|f| std::cmp::Reverse(f.min_time));

        files
    }

    /// Remove files that are marked for deletion or that violate their retention period.
    pub fn remove_files_for_deletion(
        &self,
        catalog: Arc<Catalog>,
    ) -> SerdeVecMap<DbId, DatabaseTables> {
        let mut removed: SerdeVecMap<DbId, DatabaseTables> = SerdeVecMap::new();
        let mut removed_paths: HashSet<String> = HashSet::new();
        let mut size = 0;
        let mut row_count = 0;

        // First pass is under a read lock to permit queries running concurrently.
        {
            let mut queue_for_removal = |db_id: DbId, table_id: TableId, file: &ParquetFile| {
                // Guard to prevent adding a file more than once.
                if removed_paths.contains(&file.path) {
                    return;
                }

                size += file.size_bytes;
                row_count += file.row_count;
                removed
                    .entry(db_id)
                    .or_default()
                    .tables
                    .entry(table_id)
                    .or_default()
                    .push(file.clone());
                removed_paths.insert(file.path.clone());
            };

            let guard = self.inner.read();

            // Remove any data marked for hard-deletion.
            for (db_id, deleted) in guard.deleted_data.iter() {
                let Some(tables) = guard.files.get(db_id) else {
                    continue;
                };

                match deleted {
                    DeletedTables::All => {
                        for (table_id, files) in tables {
                            for file in files {
                                queue_for_removal(*db_id, *table_id, file);
                            }
                        }
                    }
                    DeletedTables::List(table_ids) => {
                        for (table_id, files) in table_ids.iter().filter_map(|table_id| {
                            tables.get(table_id).map(|file| (table_id, file))
                        }) {
                            for file in files {
                                queue_for_removal(*db_id, *table_id, file);
                            }
                        }
                    }
                }
            }

            let retention_periods = catalog.get_retention_period_cutoff_map();

            for ((db_id, table_id), cutoff) in retention_periods {
                // If the database or table is deleted, the files are already scheduled for deletion.
                match guard.deleted_data.get(&db_id) {
                    Some(DeletedTables::All) => {
                        continue;
                    }
                    Some(DeletedTables::List(tables)) if tables.contains(&table_id) => {
                        continue;
                    }
                    _ => {}
                }

                let Some(files) = guard.files.get(&db_id).and_then(|hm| hm.get(&table_id)) else {
                    continue;
                };
                for file in files {
                    // remove files if their max time (aka newest timestamp) is less than (aka older
                    // than) the cutoff timestamp for the retention period
                    if file.max_time < cutoff {
                        queue_for_removal(db_id, table_id, file);
                    }
                }
            }
        }

        // if no persisted files are found to be in violation of their retention period, then
        // return an empty result to avoid unnecessarily acquiring a write lock
        if removed.is_empty() {
            return removed;
        }

        let mut guard = self.inner.write();
        for (_, tables) in guard.files.iter_mut() {
            for (_, files) in tables.iter_mut() {
                files.retain(|file| !removed_paths.contains(&file.path))
            }
        }

        guard.parquet_files_count = guard
            .parquet_files_count
            .saturating_sub(removed_paths.len() as u64);
        guard.parquet_files_size_mb -= as_mb(size);
        guard.parquet_files_row_count = guard.parquet_files_row_count.saturating_sub(row_count);

        // The deleted data has been processed.
        guard.deleted_data = HashMap::new();

        removed
    }
}

impl ParquetMetrics for PersistedFiles {
    /// Get parquet file metrics, file count, row count and size in MB
    fn get_metrics(&self) -> (u64, f64, u64) {
        let inner = self.inner.read();
        (
            inner.parquet_files_count,
            inner.parquet_files_size_mb,
            inner.parquet_files_row_count,
        )
    }
}

#[derive(Debug, Default)]
struct Inner {
    /// The map of databases to tables to files
    pub files: DatabaseToTables,
    /// Overall count of the parquet files
    pub parquet_files_count: u64,
    /// Total size of all parquet files in MB
    pub parquet_files_size_mb: f64,
    /// Overall row count within the parquet files
    pub parquet_files_row_count: u64,
    /// Data that are marked for deletion.
    pub deleted_data: HashMap<DbId, DeletedTables>,
    /// Removed-file tombstones: object-store paths that a folded compaction
    /// `removed_files` referenced but which were not present at fold time (the
    /// removal landed before the add that introduces them — a reorder between
    /// the writer-snapshot and compaction streams, or across a checkpoint
    /// boundary). A subsequent add for a tombstoned path is suppressed, making
    /// the fold order-independent and preventing phantom refs to
    /// compaction-deleted files of any generation. The value is the
    /// garbage-collection key (see [`TombstoneGc`]). Object-store paths are
    /// write-once, so tombstoning one can never hide a live file — only a stale
    /// re-listing of a deleted one.
    pub removed_tombstones: HashMap<String, TombstoneGc>,
    /// Insertion order of [`TombstoneGc::ObjectGone`] tombstones, used to bound
    /// their count: they have no high-water GC trigger, so the oldest are
    /// dropped once `MAX_EVICTION_TOMBSTONES` is exceeded.
    pub eviction_tombstones: std::collections::VecDeque<String>,
}

/// Upper bound on retained [`TombstoneGc::ObjectGone`] tombstones. At the prod
/// cold-GC deletion rate (~2.4k gen1/day) this is ~80 days of headroom; the
/// FIFO only drops the oldest under genuine pathology, and a dropped-then-
/// re-added path is simply re-evicted and re-tombstoned by the next validator
/// pass.
const MAX_EVICTION_TOMBSTONES: usize = 200_000;

/// When a removed-file tombstone may be dropped: once the relevant high-water
/// mark passes the point at which any manifest could re-add the file.
#[derive(Debug, Clone)]
pub(crate) enum TombstoneGc {
    /// Writer gen1 file. Drop once `wal_high_water[node_id] >= wal_seq` — that
    /// WAL snapshot can no longer be re-folded, so it can't re-add the file.
    WalSeq { node_id: String, wal_seq: u64 },
    /// Compactor gen2+ file. Drop once `compactions_high_water >= removing_id`
    /// (the ULID of the compaction whose `removed_files` carried this path). The
    /// compaction that *adds* the file is older still, so once the high-water
    /// passes the remover it has also passed the adder and the file can't recur.
    Compaction { removing_id: String },
    /// A ref the validator confirmed gone from object store (the object's
    /// prefix LIST does not contain it). Unlike the two reorder cases above, the
    /// re-adding snapshot can carry a *higher* WAL seq than the file's own
    /// (e.g. a periodic full re-fold relists a long-superseded gen1), so there
    /// is no high-water that bounds it — keying by the file's seq would GC the
    /// tombstone before the re-add and let the phantom resurrect, evict, and
    /// resurrect again (the 469K-eviction churn). Paths are write-once, so
    /// suppressing a confirmed-gone one can never hide live data. Not GC'd by a
    /// high-water; bounded instead by a FIFO cap (see `eviction_tombstones`) and
    /// re-derived by the validator after restart, so it is never serialized into
    /// a checkpoint.
    ObjectGone,
}

/// Parse a writer-persisted gen1 parquet path,
/// `<node_id>/dbs/<db>/<table>/<date>/<hour>/<wal_seq:010>[-<ordinal>].parquet`,
/// into `(node_id, wal_seq)`. Anything else — compactor output lives under
/// `.../genN/...` with a ULID stem — yields `None`.
///
/// Since upstream 3.9.11 a gen1 chunk that splits into several buffer chunks
/// (a string column at the Arrow varchar limit) persists one file per split:
/// `<wal_seq:010>-<ordinal>.parquet` for ordinal >= 1, the bare name for
/// ordinal 0. Every split of one snapshot shares the WAL sequence, which is
/// the only part the tombstone GC keys on.
fn parse_gen1_path(path: &str) -> Option<(String, u64)> {
    let node_id = path.split('/').next()?.to_string();
    if node_id.is_empty() {
        return None;
    }
    let stem = path.strip_suffix(".parquet")?.rsplit('/').next()?;
    let (seq, ordinal) = match stem.split_once('-') {
        Some((seq, ordinal)) => (seq, Some(ordinal)),
        None => (stem, None),
    };
    let all_digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    if !all_digits(seq) || ordinal.is_some_and(|o| !all_digits(o)) {
        return None;
    }
    let wal_seq = seq.parse::<u64>().ok()?;
    Some((node_id, wal_seq))
}

impl Inner {
    /// Create from persisted snapshots via shared Arc reference.
    ///
    /// Uses reference-based iteration to avoid consuming the Arc'd Vec,
    /// allowing the same snapshot data to be shared with other consumers.
    pub(crate) fn new_from_persisted_snapshots(
        persisted_snapshots: Arc<Vec<PersistedSnapshot>>,
    ) -> Self {
        trace!(
            snapshot_count = persisted_snapshots.len(),
            "new_from_persisted_snapshots: starting"
        );
        let mut file_count: u64 = 0;
        let mut size_in_mb = 0.0;
        let mut row_count: u64 = 0;
        let mut removed_tombstones: HashMap<String, TombstoneGc> = HashMap::new();

        let files = persisted_snapshots.iter().fold(
            hashbrown::HashMap::new(),
            |mut files, persisted_snapshot| {
                // Boot fold: snapshots are applied in deterministic order
                // (merged checkpoint, then WAL ascending, then compactions
                // ascending), so a compaction id is not threaded here — gen1
                // tombstoning by path plus the seeded checkpoint tombstones
                // cover the reorderable cases.
                let delta = update_persisted_files_with_snapshot(
                    persisted_snapshot,
                    &mut files,
                    &mut removed_tombstones,
                    None,
                );
                file_count += delta.added_count;
                file_count = file_count.saturating_sub(delta.removed_count);
                size_in_mb += as_mb(delta.added_size_bytes);
                size_in_mb -= as_mb(delta.removed_size_bytes);
                row_count += delta.added_row_count;
                row_count = row_count.saturating_sub(delta.removed_row_count);
                files
            },
        );

        trace!(
            file_count,
            row_count, size_in_mb, "new_from_persisted_snapshots: completed"
        );
        Self {
            files,
            parquet_files_count: file_count,
            parquet_files_row_count: row_count,
            parquet_files_size_mb: size_in_mb,
            deleted_data: HashMap::new(),
            removed_tombstones,
            eviction_tombstones: std::collections::VecDeque::new(),
        }
    }

    /// Create from checkpoints and additional (newer) snapshots.
    pub(crate) fn new_from_checkpoints_and_snapshots(
        mut checkpoints: Vec<PersistedSnapshotCheckpoint>,
        additional_snapshots: Vec<PersistedSnapshot>,
    ) -> Self {
        debug!(
            checkpoint_count = checkpoints.len(),
            snapshot_count = additional_snapshots.len(),
            "new_from_checkpoints_and_snapshots: starting"
        );

        // Sort checkpoints by year_month to process in chronological order
        checkpoints.sort_by_key(|c| c.year_month);

        // Merge all checkpoints into one
        let merged_checkpoint = checkpoints.into_iter().reduce(|mut acc, checkpoint| {
            acc.merge(checkpoint);
            acc
        });

        // Convert merged checkpoint to Inner, or start with empty
        let mut inner = merged_checkpoint.map(Inner::from).unwrap_or_default();

        // Apply additional snapshots (deterministic boot order, no compaction
        // id threaded — see `new_from_persisted_snapshots`).
        for snapshot in additional_snapshots {
            inner.add_persisted_snapshot(snapshot, None);
        }

        debug!(
            file_count = inner.parquet_files_count,
            row_count = inner.parquet_files_row_count,
            size_in_mb = inner.parquet_files_size_mb,
            "new_from_checkpoints_and_snapshots: completed"
        );

        inner
    }

    /// Merges all files from a [`PersistedSnapshot`] into the persisted files hierarchy.
    ///
    /// Deduplicates incoming files. Called at runtime (not during initial load).
    pub(crate) fn add_persisted_snapshot(
        &mut self,
        persisted_snapshot: PersistedSnapshot,
        source_compaction_id: Option<&str>,
    ) {
        let delta = update_persisted_files_with_snapshot(
            &persisted_snapshot,
            &mut self.files,
            &mut self.removed_tombstones,
            source_compaction_id,
        );
        self.parquet_files_count += delta.added_count;
        self.parquet_files_count = self.parquet_files_count.saturating_sub(delta.removed_count);
        self.parquet_files_row_count += delta.added_row_count;
        self.parquet_files_row_count = self
            .parquet_files_row_count
            .saturating_sub(delta.removed_row_count);
        self.parquet_files_size_mb += as_mb(delta.added_size_bytes);
        self.parquet_files_size_mb -= as_mb(delta.removed_size_bytes);
    }

    /// Adds a single parquet file to the specified database and table.
    ///
    /// Creates db/table entries if needed. Skips duplicates.
    pub(crate) fn add_persisted_file(
        &mut self,
        db_id: &DbId,
        table_id: &TableId,
        parquet_file: &ParquetFile,
    ) {
        let existing_parquet_files = self
            .files
            .entry(*db_id)
            .or_default()
            .entry(*table_id)
            .or_default();
        if !existing_parquet_files.contains(parquet_file) {
            self.parquet_files_row_count += parquet_file.row_count;
            self.parquet_files_size_mb += as_mb(parquet_file.size_bytes);
            existing_parquet_files.push(parquet_file.clone());
        }
        self.parquet_files_count += 1;
    }

    /// Drop tombstones whose source WAL snapshot has been passed by the
    /// writer's high-water mark: that snapshot can no longer be re-folded, so
    /// it can never re-add the file and the tombstone is no longer needed.
    /// Bounds tombstone memory; correctness does not depend on it (paths are
    /// write-once, so a stale tombstone could only suppress a deleted file).
    fn gc_tombstones(
        &mut self,
        wal_high_water: &std::collections::HashMap<String, u64>,
        compactions_high_water: Option<&str>,
    ) {
        self.removed_tombstones.retain(|_path, gc| match gc {
            TombstoneGc::WalSeq { node_id, wal_seq } => {
                wal_high_water.get(node_id).copied().unwrap_or(0) < *wal_seq
            }
            TombstoneGc::Compaction { removing_id } => {
                // Keep until the compaction high-water reaches the removing id;
                // ULIDs are lexicographically time-ordered.
                compactions_high_water.is_none_or(|hw| hw < removing_id.as_str())
            }
            // No high-water bounds an ObjectGone re-add; kept until the FIFO cap
            // evicts it (see `tombstone_evicted`).
            TombstoneGc::ObjectGone => true,
        });
    }

    /// Record confirmed-gone paths as [`TombstoneGc::ObjectGone`] tombstones so
    /// a later fold cannot resurrect them. Bounded by `MAX_EVICTION_TOMBSTONES`
    /// via FIFO. A path already carrying a (reorder) tombstone is left as-is —
    /// that one has a proper high-water GC key and need not become unbounded.
    fn tombstone_evicted(&mut self, paths: impl IntoIterator<Item = String>) {
        for path in paths {
            if self.removed_tombstones.contains_key(&path) {
                continue;
            }
            self.removed_tombstones
                .insert(path.clone(), TombstoneGc::ObjectGone);
            self.eviction_tombstones.push_back(path);
        }
        while self.eviction_tombstones.len() > MAX_EVICTION_TOMBSTONES {
            let Some(oldest) = self.eviction_tombstones.pop_front() else {
                break;
            };
            // Only drop it if it is still the ObjectGone tombstone we inserted —
            // a later compaction removal may have overwritten it with a
            // high-water-GC'd key, which manages its own lifetime.
            if matches!(
                self.removed_tombstones.get(&oldest),
                Some(TombstoneGc::ObjectGone)
            ) {
                self.removed_tombstones.remove(&oldest);
            }
        }
    }

    /// Seed tombstones loaded from a shared-inventory checkpoint, so a freshly
    /// booted node suppresses re-adds of files compaction removed before the
    /// checkpoint (whose removal manifests sit below the loader's high-water
    /// and are therefore never re-folded).
    fn seed_tombstones(
        &mut self,
        gen1: impl IntoIterator<Item = (String, u64)>,
        compaction: impl IntoIterator<Item = (String, String)>,
    ) {
        for (path, wal_seq) in gen1 {
            if let Some(node_id) = path
                .split('/')
                .next()
                .filter(|n| !n.is_empty())
                .map(str::to_string)
            {
                self.removed_tombstones
                    .insert(path, TombstoneGc::WalSeq { node_id, wal_seq });
            }
        }
        for (path, removing_id) in compaction {
            self.removed_tombstones
                .insert(path, TombstoneGc::Compaction { removing_id });
        }
    }
}

impl From<PersistedSnapshotCheckpoint> for Inner {
    fn from(checkpoint: PersistedSnapshotCheckpoint) -> Self {
        let mut files: DatabaseToTables = HashMap::new();
        let mut file_count: u64 = 0;

        for (db_id, db_tables) in checkpoint.databases {
            for (table_id, parquet_files) in db_tables.tables {
                file_count += parquet_files.len() as u64;
                files
                    .entry(db_id)
                    .or_default()
                    .entry(table_id)
                    .or_default()
                    .extend(parquet_files);
            }
        }

        Self {
            files,
            parquet_files_count: file_count,
            parquet_files_size_mb: as_mb(checkpoint.parquet_size_bytes),
            parquet_files_row_count: checkpoint.row_count,
            deleted_data: HashMap::new(),
            removed_tombstones: HashMap::new(),
            eviction_tombstones: std::collections::VecDeque::new(),
        }
    }
}

fn as_mb(bytes: u64) -> f64 {
    let factor = (1_000 * 1_000) as f64;
    bytes as f64 / factor
}

/// Accumulated deltas from folding one [`PersistedSnapshot`] into the
/// db/table hierarchy. Only files actually added / actually present count,
/// so metrics stay consistent under deduplication.
#[derive(Debug, Default)]
struct SnapshotFoldDelta {
    added_count: u64,
    added_size_bytes: u64,
    added_row_count: u64,
    removed_count: u64,
    removed_size_bytes: u64,
    removed_row_count: u64,
}

/// Merges parquet files from a [`PersistedSnapshot`] into the db/table hierarchy.
///
/// Files are identified by their object-store path everywhere: `ParquetFileId`
/// is a process-local counter, so files persisted by different nodes (e.g.
/// writer gen1 inputs and compactor outputs) can share an id within the same
/// table, and the same file can arrive from different sources with different
/// ids (a checkpoint and a WAL manifest both carry it).
///
/// Adds are deduplicated by path; removals drop every copy with a matching
/// path. Anything weaker corrupts multi-node state: un-deduplicated boot
/// folds let checkpoint + WAL copies accumulate across restarts, and a
/// single-copy removal then leaves stale duplicates behind.
fn update_persisted_files_with_snapshot(
    persisted_snapshot: &PersistedSnapshot,
    db_to_tables: &mut HashMap<DbId, HashMap<TableId, Vec<ParquetFile>>>,
    tombstones: &mut HashMap<String, TombstoneGc>,
    source_compaction_id: Option<&str>,
) -> SnapshotFoldDelta {
    let mut delta = SnapshotFoldDelta::default();
    persisted_snapshot
        .databases
        .iter()
        .for_each(|(db_id, tables)| {
            let db_tables: &mut HashMap<TableId, Vec<ParquetFile>> = db_to_tables
                .entry(*db_id)
                .or_insert_with(|| HashMap::with_capacity(tables.tables.len()));

            tables
                .tables
                .iter()
                .for_each(|(table_id, new_parquet_files)| {
                    let table_files = db_tables
                        .entry(*table_id)
                        .or_insert_with(|| Vec::with_capacity(new_parquet_files.len()));
                    let mut seen_paths: HashSet<String> =
                        table_files.iter().map(|f| f.path.clone()).collect();
                    for file in new_parquet_files {
                        // Suppress a re-add of a file a prior fold already saw
                        // removed (removal-before-add reorder). Without this the
                        // add resurrects a compaction-deleted gen1 file → a
                        // phantom ref that 404s at query time.
                        if tombstones.contains_key(&file.path) {
                            continue;
                        }
                        if seen_paths.insert(file.path.clone()) {
                            delta.added_count += 1;
                            delta.added_size_bytes += file.size_bytes;
                            delta.added_row_count += file.row_count;
                            table_files.push(file.clone());
                        }
                    }
                });
        });

    // Remove files referenced by `removed_files`. A removal whose target is not
    // present yet (the writer snapshot that adds it has not been folded, or was
    // already folded into a checkpoint below the loader's high-water) is
    // recorded as a tombstone so the later add is suppressed — making the fold
    // order-independent. Tombstones are kept only for writer gen1 paths, the
    // one stream that can reorder against compaction manifests; they GC once
    // `wal_high_water` passes the source snapshot.
    persisted_snapshot
        .removed_files
        .iter()
        .for_each(|(db_id, tables)| {
            tables
                .tables
                .iter()
                .for_each(|(table_id, remove_parquet_files)| {
                    let remove_paths: HashSet<&str> = remove_parquet_files
                        .iter()
                        .map(|f| f.path.as_str())
                        .collect();

                    // Which targets are actually present right now? Captured
                    // before the retain so we can tell "removed" (was present)
                    // from "absent" (reordered ahead of its add) afterwards.
                    let present_targets: HashSet<String> = db_to_tables
                        .get(db_id)
                        .and_then(|t| t.get(table_id))
                        .map(|fs| {
                            fs.iter()
                                .filter(|f| remove_paths.contains(f.path.as_str()))
                                .map(|f| f.path.clone())
                                .collect()
                        })
                        .unwrap_or_default();

                    if let Some(table_files) = db_to_tables
                        .get_mut(db_id)
                        .and_then(|t| t.get_mut(table_id))
                    {
                        table_files.retain(|f| {
                            if remove_paths.contains(f.path.as_str()) {
                                delta.removed_count += 1;
                                delta.removed_size_bytes += f.size_bytes;
                                delta.removed_row_count += f.row_count;
                                false
                            } else {
                                true
                            }
                        });
                    }

                    // A removal target that was NOT present (absent because
                    // reordered ahead of its add) becomes a tombstone so the
                    // later add is suppressed. Present-and-removed targets need
                    // none — keeps the set bounded under normal compaction.
                    // gen1 writer paths GC against the WAL high-water; gen2+
                    // compactor paths GC against the removing compaction's id.
                    for f in remove_parquet_files {
                        if present_targets.contains(&f.path) {
                            continue;
                        }
                        let gc = if let Some((node_id, wal_seq)) = parse_gen1_path(&f.path) {
                            Some(TombstoneGc::WalSeq { node_id, wal_seq })
                        } else {
                            source_compaction_id.map(|id| TombstoneGc::Compaction {
                                removing_id: id.to_string(),
                            })
                        };
                        if let Some(gc) = gc {
                            tombstones.insert(f.path.clone(), gc);
                        }
                    }
                });
        });

    delta
}

#[cfg(test)]
mod tests;
