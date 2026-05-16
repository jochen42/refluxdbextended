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
pub struct RemoteWriteBuffer {
    writer_urls: Vec<String>,
    client: reqwest::Client,
    request_timeout: Duration,
    /// Tracks per-URL warning suppression so a wedged writer doesn't flood
    /// the log. Key: URL, value: last-warn instant.
    last_warn: Mutex<HashMap<String, std::time::Instant>>,
    warn_interval: Duration,
}

impl RemoteWriteBuffer {
    pub fn new(writer_urls: Vec<String>, request_timeout: Duration) -> Self {
        let client = reqwest::Client::builder()
            .timeout(request_timeout)
            .build()
            .expect("reqwest client construction is infallible without TLS root setup");
        Self {
            writer_urls,
            client,
            request_timeout,
            last_warn: Mutex::new(HashMap::new()),
            warn_interval: Duration::from_secs(60),
        }
    }

    pub fn writer_urls(&self) -> &[String] {
        &self.writer_urls
    }

    /// Fetch in-memory hot rows for (db, table) from every configured writer
    /// in the time window `[time_min_ns, time_max_ns]` (each side optional).
    /// Returns `Some(batches)` when at least one writer responded successfully
    /// — that signal lets the composite short-circuit Layer C, since the
    /// authoritative writer has already reported what's fresh. Returns
    /// `None` when every writer is unreachable, so the composite falls
    /// through to the WAL tail.
    pub async fn fetch_hot_chunks(
        &self,
        db_id: DbId,
        table_id: TableId,
        time_min_ns: Option<i64>,
        time_max_ns: Option<i64>,
    ) -> Option<Vec<RecordBatch>> {
        if self.writer_urls.is_empty() {
            return None;
        }
        let mut out: Vec<RecordBatch> = Vec::new();
        let mut any_success = false;
        for url in &self.writer_urls {
            match self
                .fetch_from(url, db_id, table_id, time_min_ns, time_max_ns)
                .await
            {
                Ok(batches) => {
                    any_success = true;
                    out.extend(batches);
                }
                Err(e) => self.warn_once(url, &e),
            }
        }
        if any_success { Some(out) } else { None }
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
) -> Vec<Arc<dyn iox_query::QueryChunk>> {
    use crate::chunk::BufferChunk;
    use data_types::{ChunkId, ChunkOrder, PartitionHashId, PartitionId, PartitionKey,
        TransitionPartitionId};
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
        partition_id: TransitionPartitionId::from_parts(
            PartitionId::new(0),
            Some(PartitionHashId::new(
                data_types::TableId::new(0),
                &PartitionKey::from("remote-hot".to_string()),
            )),
        ),
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
