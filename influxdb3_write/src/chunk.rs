use arrow::array::RecordBatch;
use data_types::{ChunkId, ChunkOrder, PartitionHashId, PartitionKey};
use datafusion::common::Statistics;
use influxdb3_id::{DbId, TableId};
use iox_query::chunk_statistics::ChunkStatistics;
use iox_query::{QueryChunk, QueryChunkData};
use parquet_file::storage::DataSourceExecInput;
use schema::Schema;
use schema::sort::SortKey;
use std::any::Any;
use std::sync::Arc;

use crate::ParquetFile;

/// One partition per table, shared by EVERY chunk source — local hot
/// buffer, remote hot chunks, WAL tail, and persisted parquet.
///
/// The dedupe layer (`SplitDedup` in iox_query) only deduplicates chunks
/// that share a partition id; it sub-splits by time overlap afterwards, so
/// non-overlapping chunks still skip the dedupe cost. Distinct per-source
/// keys ("remote-hot", "wal-tail", chunk-time strings) made chunks from
/// different sources — or the same series via different writers —
/// invisible to each other and let duplicate rows surface in results.
pub fn table_partition_id(db_id: DbId, table_id: TableId) -> PartitionHashId {
    PartitionHashId::new(
        data_types::TableId::new(0),
        &PartitionKey::from(format!("{}/{}", db_id.get(), table_id.get())),
    )
}

/// Band size for [`persisted_chunk_order`]: one band per generation, with
/// room for WAL sequence numbers inside the gen1 band.
const CHUNK_ORDER_GEN_BAND: i64 = 1 << 40;

/// Chunk order for a persisted parquet file, encoding provenance recency
/// so the dedupe layer keeps the newest value of a primary key:
///
/// - in-memory sources stay above everything persisted
///   (`i64::MAX`, `MAX-1`, `MAX-2` — see `composite_write_buffer`)
/// - lower generations beat higher ones: a gen1 file is always *newer*
///   data than any compacted output it overlaps (gen1 files that fed a
///   compaction are removed; survivors postdate it)
/// - within gen1, the WAL sequence number from the filename orders files
///   from the same writer; sequences from different writers are not
///   comparable, which makes cross-writer last-write-wins approximate by
///   design.
pub fn persisted_chunk_order(file: &ParquetFile) -> ChunkOrder {
    let generation = i64::from(parse_generation(&file.path).unwrap_or(1).clamp(1, 5));
    let seq = parse_file_sequence(&file.path)
        .unwrap_or(0)
        .min(CHUNK_ORDER_GEN_BAND as u64 - 1) as i64;
    ChunkOrder::new((5 - generation) * CHUNK_ORDER_GEN_BAND + seq)
}

/// Parse the generation level from a parquet path (`.../genN/...`).
/// Returns `None` for paths without a generation segment — i.e. gen1 files
/// written by the WAL-driven persist path.
pub fn parse_generation(path: &str) -> Option<u8> {
    let gen_start = path.find("/gen")?;
    let gen_part = &path[gen_start + 4..];
    let gen_end = gen_part.find('/')?;
    gen_part[..gen_end].parse::<u8>().ok()
}

/// Parse the numeric filename stem of a gen1 parquet file
/// (`.../0000000598.parquet` → 598). Compacted files use ULID stems and
/// return `None`.
fn parse_file_sequence(path: &str) -> Option<u64> {
    let stem = path.rsplit('/').next()?.strip_suffix(".parquet")?;
    stem.parse::<u64>().ok()
}

#[derive(Debug)]
pub struct BufferChunk {
    pub batches: Vec<RecordBatch>,
    pub schema: Schema,
    pub stats: Arc<ChunkStatistics>,
    pub partition_id: PartitionHashId,
    pub sort_key: Option<SortKey>,
    pub id: data_types::ChunkId,
    pub chunk_order: data_types::ChunkOrder,
}

impl QueryChunk for BufferChunk {
    fn stats(&self) -> Arc<Statistics> {
        Arc::clone(&self.stats.statistics())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn partition_id(&self) -> &PartitionHashId {
        &self.partition_id
    }

    fn sort_key(&self) -> Option<&SortKey> {
        self.sort_key.as_ref()
    }

    fn id(&self) -> data_types::ChunkId {
        self.id
    }

    fn may_contain_pk_duplicates(&self) -> bool {
        true
    }

    fn data(&self) -> QueryChunkData {
        QueryChunkData::in_mem(self.batches.clone(), Arc::clone(self.schema.inner()))
    }

    fn chunk_type(&self) -> &str {
        "BufferChunk"
    }

    fn order(&self) -> data_types::ChunkOrder {
        self.chunk_order
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug)]
pub struct ParquetChunk {
    pub schema: Schema,
    pub stats: Arc<ChunkStatistics>,
    pub partition_id: PartitionHashId,
    pub sort_key: Option<SortKey>,
    pub id: ChunkId,
    pub chunk_order: ChunkOrder,
    pub parquet_exec: DataSourceExecInput,
}

impl QueryChunk for ParquetChunk {
    fn stats(&self) -> Arc<Statistics> {
        Arc::clone(&self.stats.statistics())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn partition_id(&self) -> &PartitionHashId {
        &self.partition_id
    }

    fn sort_key(&self) -> Option<&SortKey> {
        self.sort_key.as_ref()
    }

    fn id(&self) -> ChunkId {
        self.id
    }

    fn may_contain_pk_duplicates(&self) -> bool {
        false
    }

    fn data(&self) -> QueryChunkData {
        QueryChunkData::Parquet(self.parquet_exec.clone())
    }

    fn chunk_type(&self) -> &str {
        "Parquet"
    }

    fn order(&self) -> ChunkOrder {
        self.chunk_order
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(path: &str) -> ParquetFile {
        ParquetFile {
            id: influxdb3_id::ParquetFileId::from(0),
            path: path.to_string(),
            size_bytes: 1,
            row_count: 1,
            chunk_time: 0,
            min_time: 0,
            max_time: 1,
        }
    }

    #[test]
    fn partition_id_is_identical_across_sources_for_same_table() {
        let a = table_partition_id(DbId::new(3), TableId::new(9));
        let b = table_partition_id(DbId::new(3), TableId::new(9));
        let other = table_partition_id(DbId::new(3), TableId::new(10));
        assert_eq!(a, b);
        assert_ne!(a, other);
    }

    #[test]
    fn gen1_files_order_above_compacted_and_by_wal_seq() {
        let gen1_old = persisted_chunk_order(&file(
            "writer-1/dbs/38/9/2026-05-07/13-35/0000000598.parquet",
        ));
        let gen1_new = persisted_chunk_order(&file(
            "writer-1/dbs/38/9/2026-05-07/13-35/0000000600.parquet",
        ));
        let gen2 = persisted_chunk_order(&file(
            "compactor-1/compactions/gen2/38/9/01JXABCDEF.parquet",
        ));
        let gen5 = persisted_chunk_order(&file(
            "compactor-1/compactions/gen5/38/9/01JXABCDEF.parquet",
        ));
        assert!(gen1_new > gen1_old, "newer WAL seq must win within gen1");
        assert!(gen1_old > gen2, "gen1 survivors postdate compacted output");
        assert!(gen2 > gen5, "lower generations are newer data");
        // Everything persisted stays below the in-memory bands.
        assert!(gen1_new.get() < i64::MAX - 2);
    }

    #[test]
    fn parse_generation_reads_gen_segment() {
        assert_eq!(parse_generation("h/compactions/gen3/1/2/x.parquet"), Some(3));
        assert_eq!(parse_generation("h/dbs/1/2/2026-01-01/00-00/0000000001.parquet"), None);
    }
}
