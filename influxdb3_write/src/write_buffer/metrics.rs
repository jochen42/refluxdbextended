use metric::{Metric, Registry, U64Counter, U64Gauge};

#[derive(Debug)]
pub(super) struct WriteMetrics {
    write_lines_total: Metric<U64Counter>,
    write_lines_rejected_total: Metric<U64Counter>,
    write_bytes_total: Metric<U64Counter>,
    wal_buffer_size_bytes: U64Gauge,
}

pub(super) const WRITE_LINES_METRIC_NAME: &str = "influxdb3_write_lines";
pub(super) const WRITE_LINES_REJECTED_METRIC_NAME: &str = "influxdb3_write_lines_rejected";
pub(super) const WRITE_BYTES_METRIC_NAME: &str = "influxdb3_write_bytes";
pub(super) const WAL_BUFFER_SIZE_BYTES_METRIC_NAME: &str = "influxdb3_wal_buffer_size_bytes";

impl WriteMetrics {
    pub(super) fn new(metric_registry: &Registry) -> Self {
        let write_lines_total = metric_registry.register_metric::<U64Counter>(
            WRITE_LINES_METRIC_NAME,
            "track total number of lines written",
        );
        let write_lines_rejected_total = metric_registry.register_metric::<U64Counter>(
            WRITE_LINES_REJECTED_METRIC_NAME,
            "track total number of lines that were rejected",
        );
        let write_bytes_total = metric_registry.register_metric::<U64Counter>(
            WRITE_BYTES_METRIC_NAME,
            "track total number of bytes written",
        );
        let wal_buffer_size_bytes = metric_registry
            .register_metric::<U64Gauge>(
                WAL_BUFFER_SIZE_BYTES_METRIC_NAME,
                "current in-memory write buffer size in bytes, not yet flushed/persisted to parquet",
            )
            .recorder(&[]);
        Self {
            write_lines_total,
            write_lines_rejected_total,
            write_bytes_total,
            wal_buffer_size_bytes,
        }
    }

    pub(super) fn record_lines(&self, lines: u64) {
        self.write_lines_total.recorder([]).inc(lines);
    }

    pub(super) fn record_lines_rejected(&self, lines: u64) {
        self.write_lines_rejected_total.recorder([]).inc(lines);
    }

    pub(super) fn record_bytes(&self, bytes: u64) {
        self.write_bytes_total.recorder([]).inc(bytes);
    }

    pub(super) fn set_wal_buffer_size_bytes(&self, bytes: u64) {
        self.wal_buffer_size_bytes.set(bytes);
    }
}

#[cfg(test)]
mod tests;
