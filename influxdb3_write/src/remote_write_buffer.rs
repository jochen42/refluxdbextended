//! Cross-node hot-chunks client. Lives on the querier; speaks HTTP to one or
//! more writers' `/api/v3/internal/hot_chunks` endpoints to fetch rows that
//! are still in the writer's in-memory `QueryableBuffer` (not yet flushed to
//! parquet or shared-inventory).
//!
//! Why HTTP and not Arrow Flight: the existing Flight setup in
//! `influxdb3_server::grpc` is a thin wrapper around iox's
//! `service_grpc_flight` and adding a new ticket type means touching iox.
//! HTTP + Arrow IPC stream is simpler and reuses the writer's existing
//! request pipeline.
//!
//! Failure mode: any remote call that times out or returns non-200 is
//! treated as "no hot rows from that writer". A composite write buffer can
//! still serve from persisted files + WAL tail. Errors are logged but never
//! propagated to query callers.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::ipc::reader::StreamReader;
use influxdb3_id::{DbId, TableId};
use observability_deps::tracing::{debug, warn};
use parking_lot::Mutex;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug)]
struct RemoteMetrics {
    requests: metric::Metric<metric::U64Counter>,
    duration: metric::DurationHistogram,
}

/// One writer endpoint, optionally tied to its WAL node id. The node id is
/// what lets the composite fall back to the WAL tail for exactly the
/// writers that did not answer Layer B; without it (legacy `--writer-urls`
/// config) the fallback stays all-or-nothing.
#[derive(Debug, Clone)]
pub struct RemoteWriterTarget {
    pub node_id: Option<String>,
    pub url: String,
}

/// Result of fanning a hot-chunks fetch out to every configured writer.
#[derive(Debug)]
pub struct HotChunksFetch {
    pub batches: Vec<RecordBatch>,
    /// Node ids of writers that answered successfully (only writers with a
    /// known node id appear here).
    pub reachable_node_ids: std::collections::HashSet<String>,
    /// True when every configured writer carries a node id, i.e. the
    /// caller can reason per-writer about who still needs the WAL tail.
    pub fully_mapped: bool,
}

#[derive(Debug)]
pub struct RemoteWriteBuffer {
    writers: Vec<RemoteWriterTarget>,
    client: reqwest::Client,
    request_timeout: Duration,
    /// Tracks per-URL warning suppression so a wedged writer doesn't flood
    /// the log. Key: URL, value: last-warn instant.
    last_warn: Mutex<HashMap<String, std::time::Instant>>,
    warn_interval: Duration,
    metrics: Option<RemoteMetrics>,
}

impl RemoteWriteBuffer {
    /// Legacy constructor: URLs without node-id mapping.
    pub fn new(writer_urls: Vec<String>, request_timeout: Duration) -> Self {
        Self::with_targets(
            writer_urls
                .into_iter()
                .map(|url| RemoteWriterTarget { node_id: None, url })
                .collect(),
            request_timeout,
        )
    }

    pub fn with_targets(writers: Vec<RemoteWriterTarget>, request_timeout: Duration) -> Self {
        let client = reqwest::Client::builder()
            .timeout(request_timeout)
            .build()
            .expect("reqwest client construction is infallible without TLS root setup");
        Self {
            writers,
            client,
            request_timeout,
            last_warn: Mutex::new(HashMap::new()),
            warn_interval: Duration::from_secs(60),
            metrics: None,
        }
    }

    /// Attach Prometheus metrics for the hot-chunks RPC.
    pub fn with_metrics(mut self, registry: &metric::Registry) -> Self {
        self.metrics = Some(RemoteMetrics {
            requests: registry.register_metric::<metric::U64Counter>(
                "influxdb3_remote_hot_chunks_requests",
                "querier-side hot-chunks RPCs to writers by result",
            ),
            duration: registry
                .register_metric::<metric::DurationHistogram>(
                    "influxdb3_remote_hot_chunks_duration",
                    "round-trip duration of querier-side hot-chunks RPCs",
                )
                .recorder(&[]),
        });
        self
    }

    pub fn writer_targets(&self) -> &[RemoteWriterTarget] {
        &self.writers
    }

    /// Fetch in-memory hot rows for (db, table) from every configured writer
    /// in the time window `[time_min_ns, time_max_ns]` (each side optional).
    /// Returns `Some(fetch)` when at least one writer responded successfully;
    /// `fetch.reachable_node_ids` tells the composite which writers' WAL
    /// tails are redundant. Returns `None` when every writer is unreachable,
    /// so the composite falls through to the WAL tail for all of them.
    pub async fn fetch_hot_chunks(
        &self,
        db_id: DbId,
        table_id: TableId,
        time_min_ns: Option<i64>,
        time_max_ns: Option<i64>,
    ) -> Option<HotChunksFetch> {
        if self.writers.is_empty() {
            return None;
        }
        let mut fetch = HotChunksFetch {
            batches: Vec::new(),
            reachable_node_ids: Default::default(),
            fully_mapped: self.writers.iter().all(|w| w.node_id.is_some()),
        };
        let mut any_success = false;
        for writer in &self.writers {
            let start = std::time::Instant::now();
            let outcome = self
                .fetch_from(&writer.url, db_id, table_id, time_min_ns, time_max_ns)
                .await;
            if let Some(m) = &self.metrics {
                m.duration.record(start.elapsed());
                let result = match &outcome {
                    Ok(_) => "ok",
                    Err(RemoteHotChunksError::Timeout) => "timeout",
                    Err(_) => "error",
                };
                m.requests.recorder(&[("result", result)]).inc(1);
            }
            match outcome {
                Ok(batches) => {
                    any_success = true;
                    if let Some(node_id) = &writer.node_id {
                        fetch.reachable_node_ids.insert(node_id.clone());
                    }
                    fetch.batches.extend(batches);
                }
                Err(e) => self.warn_once(&writer.url, &e),
            }
        }
        if any_success { Some(fetch) } else { None }
    }

    async fn fetch_from(
        &self,
        url: &str,
        db_id: DbId,
        table_id: TableId,
        time_min_ns: Option<i64>,
        time_max_ns: Option<i64>,
    ) -> Result<Vec<RecordBatch>, RemoteHotChunksError> {
        #[derive(Serialize)]
        struct Req {
            db_id: u32,
            table_id: u32,
            #[serde(skip_serializing_if = "Option::is_none")]
            time_min_ns: Option<i64>,
            #[serde(skip_serializing_if = "Option::is_none")]
            time_max_ns: Option<i64>,
        }
        let endpoint = format!(
            "{}/api/v3/internal/hot_chunks",
            url.trim_end_matches('/')
        );
        let body = Req {
            db_id: db_id.get(),
            table_id: table_id.get(),
            time_min_ns,
            time_max_ns,
        };
        let resp = tokio::time::timeout(
            self.request_timeout,
            self.client.post(&endpoint).json(&body).send(),
        )
        .await
        .map_err(|_| RemoteHotChunksError::Timeout)?
        .map_err(RemoteHotChunksError::Reqwest)?;

        if !resp.status().is_success() {
            return Err(RemoteHotChunksError::HttpStatus(resp.status().as_u16()));
        }
        let bytes = resp
            .bytes()
            .await
            .map_err(RemoteHotChunksError::Reqwest)?;
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        let cursor = std::io::Cursor::new(bytes);
        let reader = StreamReader::try_new(cursor, None)
            .map_err(RemoteHotChunksError::Arrow)?;
        let mut out = Vec::new();
        for batch in reader {
            out.push(batch.map_err(RemoteHotChunksError::Arrow)?);
        }
        debug!(
            url,
            ?db_id,
            ?table_id,
            count = out.len(),
            "remote hot chunks fetched"
        );
        Ok(out)
    }

    fn warn_once(&self, url: &str, e: &RemoteHotChunksError) {
        let mut guard = self.last_warn.lock();
        let now = std::time::Instant::now();
        let should_log = match guard.get(url) {
            Some(prev) if now.duration_since(*prev) < self.warn_interval => false,
            _ => true,
        };
        if should_log {
            warn!(url, error = %e, "remote hot-chunks fetch failed");
            guard.insert(url.to_string(), now);
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RemoteHotChunksError {
    #[error("request timed out")]
    Timeout,
    #[error("http error: {0}")]
    Reqwest(reqwest::Error),
    #[error("non-success http status: {0}")]
    HttpStatus(u16),
    #[error("arrow ipc decode error: {0}")]
    Arrow(arrow::error::ArrowError),
}

/// Wrap a list of remote `RecordBatch`es into a single in-memory
/// `BufferChunk` using the supplied influx schema. The schema must come
/// from the local catalog (not inferred from the wire payload) because the
/// arrow schema alone lacks the tag/field/time semantics the IOx planner
/// needs to recognize the chunk.
pub fn batches_to_buffer_chunks(
    batches: Vec<RecordBatch>,
    influx_schema: schema::Schema,
    chunk_order: i64,
    db_id: DbId,
    table_id: TableId,
) -> Vec<Arc<dyn iox_query::QueryChunk>> {
    use crate::chunk::BufferChunk;
    use data_types::{ChunkId, ChunkOrder};
    use iox_query::chunk_statistics::{NoColumnRanges, create_chunk_statistics};

    if batches.is_empty() {
        return Vec::new();
    }
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    if row_count == 0 {
        return Vec::new();
    }
    let stats = create_chunk_statistics(Some(row_count), &influx_schema, None, &NoColumnRanges);
    let chunk = BufferChunk {
        batches,
        schema: influx_schema,
        stats: Arc::new(stats),
        // Same per-table partition as every other chunk source so the
        // dedupe layer sees overlapping rows from other writers/sources.
        partition_id: crate::chunk::table_partition_id(db_id, table_id),
        sort_key: None,
        id: ChunkId::new(),
        chunk_order: ChunkOrder::new(chunk_order),
    };
    vec![Arc::new(chunk) as Arc<dyn iox_query::QueryChunk>]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn empty_urls_returns_empty() {
        let rwb = RemoteWriteBuffer::new(Vec::new(), Duration::from_millis(50));
        let out = rwb
            .fetch_hot_chunks(DbId::from(1), TableId::from(1), None, None)
            .await;
        assert!(out.is_none());
    }

    #[tokio::test]
    async fn unreachable_url_logs_but_does_not_error() {
        // Random unrouted port — connection refused, but caller still gets
        // an empty Vec.
        let rwb = RemoteWriteBuffer::new(
            vec!["http://127.0.0.1:1".to_string()],
            Duration::from_millis(150),
        );
        let out = rwb
            .fetch_hot_chunks(DbId::from(1), TableId::from(1), None, None)
            .await;
        assert!(out.is_none());
    }
}
