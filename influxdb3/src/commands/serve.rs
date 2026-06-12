//! Entrypoint for InfluxDB 3 Core Server

use crate::commands::create::token::AdminTokenFile;
use anyhow::{Context, bail};
use futures::{FutureExt, future::FusedFuture, pin_mut};
use influxdb3_authz::TokenAuthenticator;
use influxdb3_cache::{
    distinct_cache::DistinctCacheProvider,
    last_cache::{self, LastCacheProvider},
    parquet_cache::create_cached_obj_store_and_oracle,
};
use influxdb3_catalog::{CatalogError, catalog::Catalog};
use influxdb3_clap_blocks::plugins::{PackageManager, ProcessingEngineConfig};
use influxdb3_clap_blocks::{
    datafusion::IoxQueryDatafusionConfig, memory_size::MemorySizeMb,
    object_store::ObjectStoreConfig, socket_addr::SocketAddr, tokio::TokioDatafusionConfig,
};
use influxdb3_process::{
    INFLUXDB3_GIT_HASH, INFLUXDB3_VERSION, PROCESS_START_TIME, PROCESS_UUID_STR, ProcessUuidGetter,
    ProcessUuidWrapper,
};
use influxdb3_processing_engine::ProcessingEngineManagerImpl;
use influxdb3_processing_engine::environment::{
    DisabledManager, DisabledPackageManager, PipManager, PythonEnvironmentManager, UVManager,
};
use influxdb3_processing_engine::plugins::ProcessingEngineEnvironmentManager;
use influxdb3_processing_engine::virtualenv::find_python;
use influxdb3_query_executor::{CreateQueryExecutorArgs, QueryExecutorImpl};
use influxdb3_server::http::HttpApi;
use influxdb3_server::startup_probe::StartupProbe;
use influxdb3_server::{
    CommonServerState, CreateServerArgs, Server, serve, serve_admin_token_recovery_endpoint,
};
use influxdb3_shutdown::{ShutdownManager, ShutdownToken, wait_for_signal};
use influxdb3_sys_events::SysEventStore;
use influxdb3_telemetry::{
    ProcessingEngineMetrics, ServeInvocationMethod,
    store::{CreateTelemetryStoreArgs, TelemetryStore},
};
use influxdb3_wal::{Gen1Duration, WalConfig};
use influxdb3_write::table_index_cache::TableIndexCache;
use influxdb3_write::{
    WriteBuffer, deleter,
    persister::Persister,
    retention_period_handler::RetentionPeriodHandler,
    table_index_cache::TableIndexCacheConfig,
    write_buffer::{
        WriteBufferImpl, WriteBufferImplArgs, check_mem_and_force_snapshot_loop,
        persisted_files::PersistedFiles,
    },
};
use iox_query::exec::{DedicatedExecutor, Executor, ExecutorConfig, PerQueryMemoryPoolConfig};
use iox_time::{SystemProvider, TimeProvider};
use metric::U64Gauge;
use object_store::ObjectStore;
use object_store_metrics::ObjectStoreMetrics;
use observability_deps::tracing::*;
use panic_logging::SendPanicsToTracing;
use parquet_file::storage::{ParquetStorage, StorageId};
use std::collections::HashMap;
use std::str::FromStr;
use std::{
    env,
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
    time::Duration,
};
use std::{path::PathBuf, process::Command};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::time::Instant;
use tokio_rustls::rustls::{SupportedProtocolVersion, version::TLS12, version::TLS13};
use tokio_util::sync::CancellationToken;
use trace_exporters::TracingConfig;
use trace_http::ctx::TraceHeaderParser;
use trogging::cli::LoggingConfig;

use crate::commands::common::warn_use_of_deprecated_env_vars;

use super::helpers::DisableAuthzList;

#[cfg(all(feature = "jemalloc_replacing_malloc", not(target_env = "msvc")))]
mod jemalloc;

/// The default name of the influxdb data directory
pub const DEFAULT_DATA_DIRECTORY_NAME: &str = ".influxdb3";

/// The default bind address for the HTTP API.
pub const DEFAULT_HTTP_BIND_ADDR: &str = "0.0.0.0:8181";

/// The default bind address for admin token recovery HTTP API.
pub const DEFAULT_ADMIN_TOKEN_RECOVERY_BIND_ADDR: &str = "127.0.0.1:8182";

pub const DEFAULT_TELEMETRY_ENDPOINT: &str = "https://telemetry.v3.influxdata.com";

const MIN_SNAPSHOTS_TO_LOAD_ON_START: u64 = 100;

mod cli_params;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Cannot parse object store config: {0}")]
    ObjectStoreParsing(#[from] influxdb3_clap_blocks::object_store::ParseError),

    #[error("Tracing config error: {0}")]
    TracingConfig(#[from] trace_exporters::Error),

    #[error("Error initializing tokio runtime: {0}")]
    TokioRuntime(#[source] std::io::Error),

    #[error("Failed to bind address")]
    BindAddress(#[source] std::io::Error),

    #[error("Server error: {0}")]
    Server(#[source] influxdb3_server::Error),

    #[error("Token error: {0}")]
    TokenError(CatalogError),

    #[error("Write buffer error: {0}")]
    WriteBuffer(#[from] influxdb3_write::write_buffer::Error),

    #[error("invalid token: {0}")]
    InvalidToken(#[from] hex::FromHexError),

    #[error("failed to initialize write buffer: {0:?}")]
    WriteBufferInit(#[source] anyhow::Error),

    #[error("failed to initialize catalog: {0}")]
    InitializeCatalog(#[source] CatalogError),

    #[error("catalog error: {0}")]
    Catalog(#[from] CatalogError),

    #[error("failed to initialize last cache: {0}")]
    InitializeLastCache(#[source] last_cache::Error),

    #[error("failed to initialize distinct cache: {0:#}")]
    InitializeDistinctCache(#[source] influxdb3_cache::distinct_cache::ProviderError),

    #[error("lost backend")]
    LostBackend,

    #[error("lost HTTP/gRPC service")]
    LostHttpGrpc,

    #[error("lost admin token recovery service")]
    LostAdminTokenRecovery,

    #[error("tls requires both a cert and a key file to be passed in to work")]
    NoCertOrKeyFile,

    #[error("table cache index initialization failed: {0}")]
    TableIndexCacheInitialization(
        #[source] influxdb3_write::table_index_cache::TableIndexCacheError,
    ),

    #[error(
        "Must set INFLUXDB3_NODE_IDENTIFIER_PREFIX={0} to a valid env var value for the node id"
    )]
    NodeIdEnvVarMissing(String),

    #[error(
        "Python environment initialization failed: {0}\nPlease ensure Python and either pip or uv package manager is installed"
    )]
    PythonEnvironmentInitialization(
        #[source] influxdb3_processing_engine::environment::PluginEnvironmentError,
    ),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

// variable name and migration message tuples
const DEPRECATED_ENV_VARS: &[(&str, &str)] = &[(
    "INFLUXDB3_PARQUET_MEM_CACHE_SIZE_MB",
    "use INFLUXDB3_PARQUET_MEM_CACHE_SIZE instead, it is in MB or %",
)];

/// Role this server process plays.
///
/// `All` is the legacy single-node mode and the default. The split modes let
/// an operator scale ingest, compaction, and query independently against a
/// shared object store bucket — provided a single writer and a single
/// compactor are deployed (currently enforced by deployment, plus an optional
/// advisory lease on the compactor side).
///
/// MVP enforcement is limited to gating the compaction service per mode. The
/// HTTP listener is still bound in every mode; operators are expected to
/// route write traffic only to `Writer`/`All` instances and read traffic to
/// `Querier`/`Writer`/`All` instances at the load balancer. Hardening the
/// listener to refuse the wrong endpoints per mode is a follow-up.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum NodeMode {
    /// Ingest + query + compact, the way today's single binary runs.
    All,
    /// Accept writes, run the WAL, persist gen1 parquet. No compaction.
    /// Query endpoints remain available so this process can also serve
    /// reads of its own data; deploy a separate `--mode querier` if you
    /// want to scale reads independently.
    Writer,
    /// Compact only. No HTTP write handlers, no WAL, no query bind. Pairs
    /// with an advisory lease so two compactors against the same bucket
    /// do not duplicate work.
    Compactor,
    /// Read-only. No ingest, no compaction. Scales horizontally.
    Querier,
}

impl NodeMode {
    pub fn runs_ingest(self) -> bool {
        matches!(self, Self::All | Self::Writer)
    }
    pub fn runs_compaction(self) -> bool {
        matches!(self, Self::All | Self::Compactor)
    }
    pub fn runs_query(self) -> bool {
        matches!(self, Self::All | Self::Writer | Self::Querier)
    }
}

const MIN_REPLAY_PRELOAD_CONCURRENCY: usize = 10; // the min number of files that will be held in memory
fn wal_replay_concurrency_limit_default() -> String {
    std::cmp::max(num_cpus::get(), MIN_REPLAY_PRELOAD_CONCURRENCY).to_string()
}

fn parse_snapshot_concurrency_limit(s: &str) -> Result<NonZeroUsize, String> {
    let n: usize = s.parse().map_err(|e| format!("{e}"))?;
    NonZeroUsize::new(n)
        .ok_or_else(|| "snapshot concurrency limit must be greater than 0".to_string())
}

/// Try to keep all the memory size in MB instead of raw bytes, also allow
/// them to be configured as a percentage of total memory using MemorySizeMb
#[derive(Clone, Debug, clap::Parser)]
pub struct Config {
    /// object store options
    #[clap(flatten)]
    object_store_config: ObjectStoreConfig,

    /// logging options
    #[clap(flatten)]
    pub(crate) logging_config: LoggingConfig,

    /// tracing options
    #[clap(flatten)]
    pub(crate) tracing_config: TracingConfig,

    /// tokio datafusion config
    #[clap(flatten)]
    pub(crate) tokio_datafusion_config: TokioDatafusionConfig,

    /// iox_query extended DataFusion config
    #[clap(flatten)]
    pub(crate) iox_query_datafusion_config: IoxQueryDatafusionConfig,

    /// Maximum size of HTTP requests.
    #[clap(
        long = "max-http-request-size",
        env = "INFLUXDB3_MAX_HTTP_REQUEST_SIZE",
        default_value = "1073741824", // 1 GiB (removed crippled limit)
        action,
    )]
    pub max_http_request_size: usize,

    /// The address on which InfluxDB will serve HTTP API requests
    #[clap(
        long = "http-bind",
        env = "INFLUXDB3_HTTP_BIND_ADDR",
        default_value = DEFAULT_HTTP_BIND_ADDR,
        action,
    )]
    pub http_bind_address: SocketAddr,

    /// Enable admin token recovery endpoint on the specified address.
    /// Use flag alone for default address (127.0.0.1:8182) or provide a custom address.
    /// WARNING: This endpoint allows unauthenticated admin token regeneration - use with caution!
    #[clap(
        long = "admin-token-recovery-http-bind",
        env = "INFLUXDB3_ADMIN_TOKEN_RECOVERY_HTTP_BIND_ADDR",
        num_args = 0..=1,
        default_missing_value = DEFAULT_ADMIN_TOKEN_RECOVERY_BIND_ADDR,
        help = "Enable admin token recovery endpoint. Use flag alone for default address (127.0.0.1:8182) or with value for custom address",
        action,
    )]
    pub admin_token_recovery_bind_address: Option<SocketAddr>,

    /// Size of memory pool used during query exec, in megabytes.
    ///
    /// Can be given as absolute value or in percentage of the total available memory (e.g. `10%`).
    #[clap(
        long = "exec-mem-pool-bytes",
        env = "INFLUXDB3_EXEC_MEM_POOL_BYTES",
        default_value = "20%",
        action
    )]
    pub exec_mem_pool_bytes: MemorySizeMb,

    /// Flag to indicate that server should start without auth
    #[clap(long = "without-auth", env = "INFLUXDB3_START_WITHOUT_AUTH", action)]
    pub without_auth: bool,

    /// Disable authz for certain resources, allowed values are health,ping,metrics
    #[clap(long = "disable-authz", env = "INFLUXDB3_DISABLE_AUTHZ")]
    pub disable_authz: Option<DisableAuthzList>,

    /// Duration that the Parquet files get arranged into. The data timestamps will land each
    /// row into a file of this duration. 1m, 5m, 10m, 30m, 1h, 6h, 12h, 1d, and 7d are supported. These are known as
    /// "generation 1" files. The compactor in Pro can compact these into larger and longer
    /// generations.
    #[clap(
        long = "gen1-duration",
        env = "INFLUXDB3_GEN1_DURATION",
        default_value = "10m",
        action
    )]
    pub gen1_duration: Gen1Duration,

    /// Duration for generation 2 files (compacted from gen1 files). 1h, 6h, 12h, 1d, 7d, 30d are supported.
    /// These files are created by compacting multiple gen1 files together for better query performance.
    #[clap(
        long = "gen2-duration",
        env = "INFLUXDB3_GEN2_DURATION",
        action
    )]
    pub gen2_duration: Option<Gen1Duration>,

    /// Duration for generation 3 files (compacted from gen2 files). 1d, 7d, 30d, 90d are supported.
    /// These files are created by compacting multiple gen2 files together for long-term storage optimization.
    #[clap(
        long = "gen3-duration",
        env = "INFLUXDB3_GEN3_DURATION",
        action
    )]
    pub gen3_duration: Option<Gen1Duration>,

    /// Duration for generation 4 files (compacted from gen3 files). 7d, 30d, 90d, 365d are supported.
    /// These files are created by compacting multiple gen3 files together for archival storage.
    #[clap(
        long = "gen4-duration",
        env = "INFLUXDB3_GEN4_DURATION",
        action
    )]
    pub gen4_duration: Option<Gen1Duration>,

    /// Duration for generation 5 files (compacted from gen4 files). 30d, 90d, 365d are supported.
    /// These files are created by compacting multiple gen4 files together for long-term archival.
    #[clap(
        long = "gen5-duration",
        env = "INFLUXDB3_GEN5_DURATION",
        action
    )]
    pub gen5_duration: Option<Gen1Duration>,

    /// Enable automatic background compaction. When enabled, the system will automatically
    /// compact files from smaller generations into larger generations based on configured durations.
    #[clap(
        long = "enable-compaction",
        env = "INFLUXDB3_ENABLE_COMPACTION",
        default_value_t = true,
        action
    )]
    pub enable_compaction: bool,

    /// Role this process plays in a deployment. `all` (default) runs ingest,
    /// query, and compaction in one binary. `writer`, `compactor`, and `querier`
    /// let an operator split the responsibilities across multiple processes
    /// sharing the same object store bucket.
    #[clap(
        long = "mode",
        env = "INFLUXDB3_MODE",
        value_enum,
        default_value_t = NodeMode::All,
        action
    )]
    pub mode: NodeMode,

    /// TTL of the advisory compactor lease. A holder that fails to refresh
    /// within this window is considered crashed and a peer may take over.
    /// Set to `0s` to disable the lease entirely (single-node use only).
    #[clap(
        long = "compactor-lease-ttl",
        env = "INFLUXDB3_COMPACTOR_LEASE_TTL",
        default_value = "60s",
        action
    )]
    pub compactor_lease_ttl: humantime::Duration,

    /// TTL of the advisory writer lease. Mirrors `--compactor-lease-ttl` but
    /// for the singleton ingester. Acquired in `writer` mode only; ignored
    /// otherwise. Set to `0s` to disable.
    #[clap(
        long = "writer-lease-ttl",
        env = "INFLUXDB3_WRITER_LEASE_TTL",
        default_value = "60s",
        action
    )]
    pub writer_lease_ttl: humantime::Duration,

    /// Open the catalog under the global `_catalog/` prefix so every node in
    /// a multi-node deployment sees a single source of truth for schema,
    /// retention, and tables. Requires an object store backend that supports
    /// `If-None-Match: *` (S3, MinIO, Azure, GCS — InMemory and LocalFS for
    /// dev). Defaults to `true` when `--mode` is anything other than `all`.
    #[clap(
        long = "shared-catalog",
        env = "INFLUXDB3_SHARED_CATALOG",
        action,
        num_args = 0..=1,
        require_equals = false,
        default_missing_value = "true",
    )]
    pub shared_catalog: Option<bool>,

    /// How often the querier (and any reader-side mode) re-reads
    /// `_inventory/*` to pick up peer WAL snapshots and compaction manifests.
    /// Smaller values give fresher reads at the cost of more object-store
    /// list calls. Set to `0s` to disable. Active only when `--mode` is not
    /// `all`.
    #[clap(
        long = "inventory-poll-interval",
        env = "INFLUXDB3_INVENTORY_POLL_INTERVAL",
        default_value = "2s",
        action
    )]
    pub inventory_poll_interval: humantime::Duration,

    /// How often parquet references in the in-memory view are validated
    /// against the object store; references whose objects do not exist are
    /// evicted (defends against phantom refs from corrupted manifests).
    /// Runs once at startup and then at this interval. Costs one recursive
    /// LIST per node prefix per pass. Set to `0s` to disable.
    #[clap(
        long = "ref-validation-interval",
        env = "INFLUXDB3_REF_VALIDATION_INTERVAL",
        default_value = "1h",
        action
    )]
    pub ref_validation_interval: humantime::Duration,

    /// Writers as `node-id=url` pairs, comma-separated. Configures Layer B
    /// (hot-chunks RPC) and Layer C (WAL tail) together and ties each URL
    /// to its WAL prefix, so when only SOME writers answer Layer B the
    /// querier falls back to the WAL tail for exactly the others — required
    /// for correct freshness with multiple writers. Supersedes
    /// `--writer-urls` and `--writer-node-ids` when set. Example:
    /// `writer-0=http://writer-0:8181,writer-1=http://writer-1:8181`.
    #[clap(
        long = "writers",
        env = "INFLUXDB3_WRITERS",
        value_delimiter = ',',
        default_value = "",
        action
    )]
    pub writers: Vec<String>,

    /// Writer HTTP base URLs the querier will hit for hot in-memory rows
    /// (Layer B). Comma-separated; empty disables Layer B. Required for
    /// sub-second freshness in `--mode=querier`. Wire-format:
    /// `http://writer-1:8181,http://writer-2:8181`.
    /// Deprecated in favor of `--writers`: without the node-id mapping,
    /// Layer C is skipped whenever ANY writer answers Layer B.
    #[clap(
        long = "writer-urls",
        env = "INFLUXDB3_WRITER_URLS",
        value_delimiter = ',',
        default_value = "",
        action
    )]
    pub writer_urls: Vec<String>,

    /// Per-request timeout for remote hot-chunks fetches. On miss, the query
    /// continues with persisted + WAL-tail data without raising an error.
    #[clap(
        long = "remote-hot-timeout",
        env = "INFLUXDB3_REMOTE_HOT_TIMEOUT",
        default_value = "250ms",
        action
    )]
    pub remote_hot_timeout: humantime::Duration,

    /// Writer node-id prefixes whose `_wal/` directories the querier should
    /// tail (Layer C). Distinct from `--writer-urls` — these are object-store
    /// path prefixes (e.g. `writer-1`), not hosts. Comma-separated; empty
    /// disables Layer C.
    #[clap(
        long = "writer-node-ids",
        env = "INFLUXDB3_WRITER_NODE_IDS",
        value_delimiter = ',',
        default_value = "",
        action
    )]
    pub writer_node_ids: Vec<String>,

    /// WAL-tail listing cadence (Layer C). Matches writer flush cadence by
    /// default (`1s`). Set to `0s` to disable WAL tailing even when
    /// `--writer-node-ids` is set.
    #[clap(
        long = "wal-tail-poll-interval",
        env = "INFLUXDB3_WAL_TAIL_POLL_INTERVAL",
        default_value = "1s",
        action
    )]
    pub wal_tail_poll_interval: humantime::Duration,

    /// Upper bound on retained WAL files per writer in the tail buffer.
    /// Older files get evicted; their rows are already in `PersistedFiles`
    /// by the time we drop them.
    #[clap(
        long = "wal-tail-max-files",
        env = "INFLUXDB3_WAL_TAIL_MAX_FILES",
        default_value = "64",
        action
    )]
    pub wal_tail_max_files: usize,

    /// Interval between compaction runs. The compactor will check for files to compact at this interval.
    #[clap(
        long = "compaction-interval",
        env = "INFLUXDB3_COMPACTION_INTERVAL",
        default_value = "1h",
        action
    )]
    pub compaction_interval: humantime::Duration,

    /// Maximum number of files to compact in a single compaction run. This prevents overwhelming
    /// the system during large compaction operations.
    #[clap(
        long = "max-compaction-files",
        env = "INFLUXDB3_MAX_COMPACTION_FILES",
        default_value = "100",
        action
    )]
    pub max_compaction_files: usize,

    /// Minimum number of files required before triggering compaction to the next generation.
    /// This ensures compaction only happens when there are enough files to make it worthwhile.
    #[clap(
        long = "min-files-for-compaction",
        env = "INFLUXDB3_MIN_FILES_FOR_COMPACTION",
        default_value = "10",
        action
    )]
    pub min_files_for_compaction: usize,

    /// Wait this long after a compaction manifest is published before deleting the
    /// original gen{N-1} parquet files. Prevents 404 errors in queries that resolved
    /// the old paths before the manifest landed. Set to `0s` to delete immediately
    /// (only safe for single-node use where no remote querier exists).
    #[clap(
        long = "compaction-delete-grace",
        env = "INFLUXDB3_COMPACTION_DELETE_GRACE",
        default_value = "10m",
        action
    )]
    pub compaction_delete_grace: humantime::Duration,

    /// The amount of time that the server looks back on startup when populating the in-memory
    /// index of gen1 files.
    ///
    /// This has two dimensions of impact on performance. The first is in terms of S3 API usage on
    /// startup; the second is in terms of initial memory usage on startup. To get a rough sense of
    /// both of these performance impacts, take the --gen1-duration value and divide that number
    /// into this parameter to get the total number of gen1 index snapshots loaded on startup. You
    /// can then take that number and multiply it by a rough approximation of the gen1 metadata
    /// stored in this index to obtain a rough estimate of memory usage.
    ///
    /// As an example, let's say we have the following values:
    ///
    /// * Estimated average of 128 bytes of parquet file metadata
    /// * --gen1-duration value of 10 minutes
    /// * --gen1-lookback-duration value of 1 month
    ///
    /// This leads to 144 files per day for 30 days for a total of 4320 index snapshots read in
    /// (via object store API calls) on startup and a (very rough) memory consumption estimate of
    /// ~533 KB.
    #[clap(
        long = "gen1-lookback-duration",
        env = "INFLUXDB3_GEN1_LOOKBACK_DURATION",
        default_value = "1month",
        action
    )]
    pub gen1_lookback_duration: humantime::Duration,

    /// Interval to flush buffered data to a wal file. Writes that wait for wal confirmation will
    /// take as long as this interval to complete.
    #[clap(
        long = "wal-flush-interval",
        env = "INFLUXDB3_WAL_FLUSH_INTERVAL",
        default_value = "1s",
        action
    )]
    pub wal_flush_interval: humantime::Duration,

    /// The number of WAL files to attempt to remove in a snapshot. This times the interval will
    /// determine how often snapshot is taken.
    #[clap(
        long = "wal-snapshot-size",
        env = "INFLUXDB3_WAL_SNAPSHOT_SIZE",
        default_value = "600",
        action
    )]
    pub wal_snapshot_size: usize,

    /// The maximum number of writes requests that can be buffered before a flush must be run
    /// and succeed.
    #[clap(
        long = "wal-max-write-buffer-size",
        env = "INFLUXDB3_WAL_MAX_WRITE_BUFFER_SIZE",
        default_value = "100000",
        action
    )]
    pub wal_max_write_buffer_size: usize,

    /// Fail on error when replaying corrupt WAL files.
    ///
    /// When false (default), corrupt or truncated WAL files will be logged and skipped during startup.
    /// When true, the server will fail to start if any WAL files are corrupt.
    #[clap(
        long = "wal-replay-fail-on-error",
        env = "INFLUXDB3_WAL_REPLAY_FAIL_ON_ERROR",
        default_value_t = false,
        action
    )]
    pub wal_replay_fail_on_error: bool,

    /// Number of snapshotted wal files to retain in object store, wal flush does not clear
    /// the wal files immediately instead they are only deleted when snapshotted and num wal files
    /// count exceeds this size
    #[clap(
        long = "snapshotted-wal-files-to-keep",
        env = "INFLUXDB3_NUM_WAL_FILES_TO_KEEP",
        default_value = "300",
        action
    )]
    pub snapshotted_wal_files_to_keep: u64,

    /// Interval between snapshot checkpoint creation.
    ///
    /// Checkpoints aggregate multiple snapshots into a single file per month, speeding up
    /// server startup by reducing the number of files to load. Disabled by default.
    #[clap(
        long = "checkpoint-interval",
        env = "INFLUXDB3_CHECKPOINT_INTERVAL",
        action
    )]
    pub checkpoint_interval: Option<humantime::Duration>,

    // TODO - tune this default:
    /// The size of the query log. Up to this many queries will remain in the log before
    /// old queries are evicted to make room for new ones.
    #[clap(
        long = "query-log-size",
        env = "INFLUXDB3_QUERY_LOG_SIZE",
        default_value = "1000",
        action
    )]
    pub query_log_size: usize,

    #[clap(flatten)]
    pub node_id: NodeId,

    /// Maximum number of table indices to cache in memory.
    ///
    /// Defaults to 100 entries. Set to 0 for unlimited cache size.
    #[clap(
        long = "table-index-cache-max-entries",
        env = "INFLUXDB3_TABLE_INDEX_CACHE_MAX_ENTRIES",
        default_value = "100",
        action
    )]
    pub table_index_cache_max_entries: usize,

    /// Maximum concurrent operations between table index cache and object store.
    ///
    /// This limits how many parallel requests can be made to object storage
    /// when loading or updating table indices.
    #[clap(
        long = "table-index-cache-concurrency-limit",
        env = "INFLUXDB3_TABLE_INDEX_CACHE_CONCURRENCY_LIMIT",
        default_value = "20",
        action
    )]
    pub table_index_cache_concurrency_limit: usize,

    /// The interval at which retention policies are checked and enforced.
    ///
    /// Enter as a human-readable time, e.g., "30m", "1h", etc.
    #[clap(
        long = "retention-check-interval",
        env = "INFLUXDB3_RETENTION_CHECK_INTERVAL",
        default_value = "30m",
        action
    )]
    pub retention_check_interval: humantime::Duration,

    /// The size of the in-memory Parquet cache in megabytes or percentage of total available mem.
    /// breaking: removed parquet-mem-cache-size-mb and env var INFLUXDB3_PARQUET_MEM_CACHE_SIZE_MB
    #[clap(
        long = "parquet-mem-cache-size",
        env = "INFLUXDB3_PARQUET_MEM_CACHE_SIZE",
        default_value = "20%",
        action
    )]
    pub parquet_mem_cache_size: MemorySizeMb,

    /// The percentage of entries to prune during a prune operation on the in-memory Parquet cache.
    ///
    /// This must be a number between 0 and 1.
    #[clap(
        long = "parquet-mem-cache-prune-percentage",
        env = "INFLUXDB3_PARQUET_MEM_CACHE_PRUNE_PERCENTAGE",
        default_value = "0.1",
        action
    )]
    pub parquet_mem_cache_prune_percentage: ParquetCachePrunePercent,

    /// The interval on which to check if the in-memory Parquet cache needs to be pruned.
    ///
    /// Enter as a human-readable time, e.g., "1s", "100ms", "1m", etc.
    #[clap(
        long = "parquet-mem-cache-prune-interval",
        env = "INFLUXDB3_PARQUET_MEM_CACHE_PRUNE_INTERVAL",
        default_value = "1s",
        action
    )]
    pub parquet_mem_cache_prune_interval: humantime::Duration,

    /// Disable the in-memory Parquet cache. By default, the cache is enabled.
    #[clap(
        long = "disable-parquet-mem-cache",
        env = "INFLUXDB3_DISABLE_PARQUET_MEM_CACHE",
        default_value_t = false,
        action
    )]
    pub disable_parquet_mem_cache: bool,

    /// The duration from `now` to check if parquet files pulled in query path requires caching
    /// Enter as a human-readable time, e.g., "5h", "3d"
    #[clap(
        long = "parquet-mem-cache-query-path-duration",
        env = "INFLUXDB3_PARQUET_MEM_CACHE_QUERY_PATH_DURATION",
        default_value = "5h",
        action
    )]
    pub parquet_mem_cache_query_path_duration: humantime::Duration,

    /// The interval on which to evict expired entries from the Last-N-Value cache, expressed as a
    /// human-readable time, e.g., "20s", "1m", "1h".
    #[clap(
        long = "last-cache-eviction-interval",
        env = "INFLUXDB3_LAST_CACHE_EVICTION_INTERVAL",
        default_value = "10s",
        action
    )]
    pub last_cache_eviction_interval: humantime::Duration,

    /// The interval on which to evict expired entries from the Distinct Value cache, expressed as a
    /// human-readable time, e.g., "20s", "1m", "1h".
    #[clap(
        long = "distinct-cache-eviction-interval",
        env = "INFLUXDB3_DISTINCT_CACHE_EVICTION_INTERVAL",
        default_value = "10s",
        action
    )]
    pub distinct_cache_eviction_interval: humantime::Duration,

    /// The processing engine config.
    #[clap(flatten)]
    pub processing_engine_config: ProcessingEngineConfig,

    /// Threshold for internal buffer, can be either percentage or absolute value in MB.
    /// eg: 70% or 1000 MB
    #[clap(
        long = "force-snapshot-mem-threshold",
        env = "INFLUXDB3_FORCE_SNAPSHOT_MEM_THRESHOLD",
        default_value = "50%",
        action
    )]
    pub force_snapshot_mem_threshold: MemorySizeMb,

    /// Disable sending telemetry data to telemetry.v3.influxdata.com.
    #[clap(
        long = "disable-telemetry-upload",
        env = "INFLUXDB3_TELEMETRY_DISABLE_UPLOAD",
        default_value_t = true,
        hide = true,
        action
    )]
    pub disable_telemetry_upload: bool,

    /// Send telemetry data to the specified endpoint.
    #[clap(
        long = "telemetry-endpoint",
        env = "INFLUXDB3_TELEMETRY_ENDPOINT",
        default_value = DEFAULT_TELEMETRY_ENDPOINT,
        hide = true,
        action
    )]
    pub telemetry_endpoint: String,

    /// Information on how the serve command was used
    #[clap(
        long = "serve-invocation-method",
        env = "INFLUXDB3_SERVE_INVOCATION_METHOD",
        hide = true,
        value_parser = ServeInvocationMethod::parse,
        action
    )]
    #[arg(default_value_t = ServeInvocationMethod::Explicit)]
    pub serve_invocation_method: ServeInvocationMethod,

    /// Set the limit for number of parquet files allowed in a query. Defaults
    /// to 432 which is about 3 days worth of files using default settings.
    /// This number can be increased to allow more files to be queried, but
    /// query performance will likely suffer, RAM usage will spike, and the
    /// process might be OOM killed as a result. It would be better to specify
    /// smaller time ranges if possible in a query.
    #[clap(long = "query-file-limit", env = "INFLUXDB3_QUERY_FILE_LIMIT", action)]
    pub query_file_limit: Option<usize>,

    #[clap(long = "tls-key", env = "INFLUXDB3_TLS_KEY")]
    pub key_file: Option<PathBuf>,

    #[clap(long = "tls-cert", env = "INFLUXDB3_TLS_CERT")]
    pub cert_file: Option<PathBuf>,

    #[clap(
        long = "tls-minimum-version",
        env = "INFLUXDB3_TLS_MINIMUM_VERSION",
        default_value = "tls-1.2"
    )]
    pub tls_minimum_version: TlsMinimumVersion,

    /// Provide a file path to write the address that the server is listening on to.
    ///
    /// This is mainly intended for testing purposes and is not considered stable.
    #[clap(
        long = "tcp-listener-file-path",
        env = "INFLUXDB3_TCP_LISTINER_FILE_PATH",
        hide = true
    )]
    pub tcp_listener_file_path: Option<PathBuf>,

    /// Provide a file path to write the address that the admin token recovery server is listening on to.
    ///
    /// This is mainly intended for testing purposes and is not considered stable.
    #[clap(
        long = "admin-token-recovery-tcp-listener-file-path",
        env = "INFLUXDB3_ADMIN_TOKEN_RECOVERY_TCP_LISTENER_FILE_PATH",
        hide = true
    )]
    pub admin_token_recovery_tcp_listener_file_path: Option<PathBuf>,

    /// File path containing offline admin token (JSON format with token and metadata)
    #[clap(long = "admin-token-file", env = "INFLUXDB3_ADMIN_TOKEN_FILE")]
    pub admin_token_file: Option<PathBuf>,

    #[clap(
        long = "wal-replay-concurrency-limit",
        env = "INFLUXDB3_WAL_REPLAY_CONCURRENCY_LIMIT",
        default_value = wal_replay_concurrency_limit_default()
    )]
    pub wal_replay_concurrency_limit: usize,

    /// Maximum number of concurrent snapshot persistence tasks.
    /// Setting this too high can lead to OOM
    #[clap(
        long = "snapshot-concurrency-limit",
        env = "INFLUXDB3_SNAPSHOT_CONCURRENCY_LIMIT",
        default_value_t = NonZeroUsize::new(num_cpus::get()).expect("num_cpus returns non-zero"),
        value_parser = parse_snapshot_concurrency_limit,
    )]
    pub parquet_snapshot_concurrency_limit: NonZeroUsize,

    /// The duration from when a database or table is soft-deleted until the data is scheduled to
    /// be hard deleted.
    #[clap(
        long = "hard-delete-default-duration",
        env = "INFLUXDB3_HARD_DELETE_DEFAULT_DURATION",
        default_value_t = Catalog::DEFAULT_HARD_DELETE_DURATION.into(),
    )]
    pub hard_delete_default_duration: humantime::Duration,

    /// Grace period for hard deleted databases and tables before they are removed permanently from
    /// the catalog.
    #[clap(
        long = "delete-grace-period",
        env = "INFLUXDB3_DELETE_GRACE_PERIOD",
        default_value = "24h",
        action
    )]
    pub delete_grace_period: humantime::Duration,

    /// The cluster-id is an enterprise config option to identify nodes belonging to the same cluster.
    /// Core OSS is single node only. We've seen folks install Core OSS thinking they installed Enterprise.
    /// They use --cluster-id and get the error `unexpected argument` which is confusing. So we generate
    /// a custom error message if they use this arg.
    #[clap(long = "cluster-id", value_parser=fail_cluster_id)]
    pub cluster_id: Option<String>,
}

pub fn fail_cluster_id(_: &str) -> Result<String, anyhow::Error> {
    Err(anyhow::anyhow!(
        "You've incorrectly specified a cluster-id for InfluxDB 3 Core OSS.\n\nCluster-id is an InfluxDB 3 Enterprise parameter. \
    \nDid you install Core in an upgrade or run Core by mistake?\n\nRemove --cluster-id to run InfluxDB 3 Core OSS."
    ))
}

#[derive(Clone, Debug, clap::Args)]
#[group(required = true)]
pub struct NodeId {
    /// The node identifier used as a prefix in all object store file paths. This should be unique
    /// for any InfluxDB 3 Core servers that share the same object store configuration, i.e., the
    /// same bucket.
    #[clap(
        long = "node-id",
        // TODO: deprecate this alias in future version
        alias = "host-id",
        env = "INFLUXDB3_NODE_IDENTIFIER_PREFIX",
        group = "node_id",
        action
    )]
    pub prefix: Option<String>,

    /// Alternative to node-id which allows the node identifier to be derived from the specified
    /// environment variable. This allows the node identifier to be dynamically detected at runtime
    /// in environments like Docker Compose or Kubernetes.
    #[clap(
        long = "node-id-from-env",
        env = "INFLUXDB3_NODE_IDENTIFIER_FROM_ENV",
        group = "node_id",
        action
    )]
    pub from_env_var: Option<String>,
}

impl NodeId {
    pub(crate) fn get_node_id(&self) -> Result<String> {
        self.prefix.clone().map_or_else(
            || {
                std::env::var(
                    self.from_env_var
                        .clone()
                        .expect(".from_env_var must be Some if .prefix is None"),
                )
                .map_err(|_| {
                    Error::NodeIdEnvVarMissing(
                        self.from_env_var.clone().unwrap_or("missing".to_string()),
                    )
                })
            },
            Ok,
        )
    }
}

impl Config {
    fn get_node_id(&self) -> Result<String> {
        self.node_id.get_node_id()
    }
}

/// The minimum version of TLS to use for InfluxDB
#[derive(Debug, Clone, Copy, Default)]
pub enum TlsMinimumVersion {
    #[default]
    Tls1_2,
    Tls1_3,
}

impl FromStr for TlsMinimumVersion {
    type Err = String;

    fn from_str(s: &str) -> std::prelude::v1::Result<Self, Self::Err> {
        match s {
            "tls-1.2" => Ok(Self::Tls1_2),
            "tls-1.3" => Ok(Self::Tls1_3),
            _ => Err("Valid minimum version strings are tls-1.2 and tls-1.3".into()),
        }
    }
}

impl From<&TlsMinimumVersion> for &'static [&'static SupportedProtocolVersion] {
    fn from(val: &TlsMinimumVersion) -> Self {
        static TLS1_2: &[&SupportedProtocolVersion] = &[&TLS12, &TLS13];
        static TLS1_3: &[&SupportedProtocolVersion] = &[&TLS13];
        match val {
            TlsMinimumVersion::Tls1_2 => TLS1_2,
            TlsMinimumVersion::Tls1_3 => TLS1_3,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ParquetCachePrunePercent(f64);

impl From<ParquetCachePrunePercent> for f64 {
    fn from(value: ParquetCachePrunePercent) -> Self {
        value.0
    }
}

impl FromStr for ParquetCachePrunePercent {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::prelude::v1::Result<Self, Self::Err> {
        let p = s
            .parse::<f64>()
            .context("failed to parse prune percent as f64")?;
        if p <= 0.0 || p >= 1.0 {
            bail!("prune percent must be between 0 and 1");
        }
        Ok(Self(p))
    }
}

/// Helper function to set a generation duration in the catalog and update the HashMap.
/// Handles AlreadyExists and CannotChangeGenerationDuration errors gracefully.
async fn set_generation_duration_with_error_handling(
    catalog: &Arc<Catalog>,
    generation_durations: &mut std::collections::HashMap<u8, Duration>,
    level: u8,
    duration: Duration,
) -> Result<()> {
    match catalog.set_generation_duration(level, duration).await {
        Ok(_) | Err(CatalogError::AlreadyExists) => {
            generation_durations.insert(level, duration);
        }
        Err(CatalogError::CannotChangeGenerationDuration { .. }) => {
            let existing = catalog
                .get_generation_duration(level)
                .unwrap_or_else(|| panic!("catalog should contain existing gen{} duration", level));
            warn!(
                level,
                existing_secs = existing.as_secs(),
                provided_secs = duration.as_secs(),
                "cannot change the existing generation duration after it has been set"
            );
            generation_durations.insert(level, existing);
        }
        Err(e) => return Err(e.into()),
    }
    Ok(())
}

pub async fn command(config: Config, user_params: HashMap<String, String>) -> Result<()> {
    let node_id = config.get_node_id()?;

    // Check that both a cert file and key file are present if TLS is being set up
    match (&config.cert_file, &config.key_file) {
        (Some(_), None) | (None, Some(_)) => {
            return Err(Error::NoCertOrKeyFile);
        }
        (Some(_), Some(_)) | (None, None) => {}
    }

    // Bind the API address and answer health probes right away. The
    // startup work below (catalog load, WAL replay) can take minutes
    // against an object store; with the port closed that whole time,
    // orchestrator health checks (load balancers, MIG autohealing, docker
    // healthchecks) declare the node dead and kill it mid-replay — and
    // the replacement starts over with an even larger WAL backlog. The
    // probe returns 200 on the health/ping paths and 503 + Retry-After
    // everywhere else until the real server takes the listener over.
    let startup_probe = StartupProbe::spawn(
        TcpListener::bind(*config.http_bind_address)
            .await
            .map_err(Error::BindAddress)?,
        config.cert_file.as_ref(),
        config.key_file.as_ref(),
        (&config.tls_minimum_version).into(),
    )
    .map_err(Error::Server)?;

    let startup_timer = Instant::now();
    let num_cpus = num_cpus::get();
    let build_malloc_conf = build_malloc_conf();
    info!(
        node_id = %node_id,
        git_hash = %INFLUXDB3_GIT_HASH as &str,
        version = %INFLUXDB3_VERSION.as_ref() as &str,
        uuid = %PROCESS_UUID_STR.as_ref() as &str,
        num_cpus,
        product_name = influxdb3_server::PRODUCT_NAME,
        "server starting",
    );
    debug!(%build_malloc_conf, "build configuration");

    // check if any env vars that are deprecated is still being passed around and warn
    warn_use_of_deprecated_env_vars(DEPRECATED_ENV_VARS);

    let metrics = setup_metric_registry();

    // Install custom panic handler and forget about it.
    //
    // This leaks the handler and prevents it from ever being dropped during the
    // lifetime of the program - this is actually a good thing, as it prevents
    // the panic handler from being removed while unwinding a panic (which in
    // turn, causes a panic - see #548)
    let f = SendPanicsToTracing::new_with_metrics(&metrics);
    std::mem::forget(f);

    // When you have extra executor, you need separate metrics registry! It is not clear what
    // the impact would be
    // TODO: confirm this is not going to mess up downstream metrics consumers
    let write_path_metrics = setup_metric_registry();

    // Install custom panic handler and forget about it.
    //
    // This leaks the handler and prevents it from ever being dropped during the
    // lifetime of the program - this is actually a good thing, as it prevents
    // the panic handler from being removed while unwinding a panic (which in
    // turn, causes a panic - see #548)
    let write_path_panic_handler_fn = SendPanicsToTracing::new_with_metrics(&write_path_metrics);
    std::mem::forget(write_path_panic_handler_fn);

    // Construct a token to trigger clean shutdown
    let frontend_shutdown = CancellationToken::new();
    let shutdown_manager = ShutdownManager::new(frontend_shutdown.clone());

    let time_provider: Arc<dyn TimeProvider> = Arc::new(SystemProvider::new());
    let sys_events_store = Arc::new(SysEventStore::new(Arc::clone(&time_provider) as _));
    // setup base object store:
    let object_store: Arc<dyn ObjectStore> = config
        .object_store_config
        .make_object_store_with_metrics(&metrics)
        .map_err(Error::ObjectStoreParsing)?;

    // setup metrics'd object store:
    let object_store: Arc<dyn ObjectStore> = Arc::new(ObjectStoreMetrics::new(
        object_store,
        Arc::clone(&time_provider) as _,
        "main",
        &metrics,
        config.object_store_config.bucket.as_ref(),
    ));

    // setup cached object store:
    let (object_store, parquet_cache) = if !config.disable_parquet_mem_cache {
        info!("initialising parquet cache");
        let (object_store, parquet_cache) = create_cached_obj_store_and_oracle(
            object_store,
            Arc::clone(&time_provider) as _,
            Arc::clone(&metrics),
            config.parquet_mem_cache_size.as_num_bytes(),
            config.parquet_mem_cache_query_path_duration.into(),
            config.parquet_mem_cache_prune_percentage.into(),
            config.parquet_mem_cache_prune_interval.into(),
        );
        (object_store, Some(parquet_cache))
    } else {
        (object_store, None)
    };

    let trace_exporter = config.tracing_config.build()?;

    let parquet_store =
        ParquetStorage::new(Arc::clone(&object_store), StorageId::from("influxdb3"));

    let mut tokio_datafusion_config = config.tokio_datafusion_config;
    tokio_datafusion_config.num_threads = tokio_datafusion_config
        .num_threads
        .or_else(|| NonZeroUsize::new(num_cpus::get()))
        .or_else(|| NonZeroUsize::new(1));
    info!(
        num_threads = tokio_datafusion_config.num_threads.map(|n| n.get()),
        "Creating shared query executor"
    );

    let exec = Arc::new(Executor::new_with_config_and_executor(
        ExecutorConfig {
            target_query_partitions: tokio_datafusion_config.num_threads.unwrap(),
            object_stores: [&parquet_store]
                .into_iter()
                .map(|store| (store.id(), Arc::clone(store.object_store())))
                .collect(),
            metric_registry: Arc::clone(&metrics),
            mem_pool_size: config.exec_mem_pool_bytes.as_num_bytes(),
            // TODO: need to make these configurable?
            per_query_mem_pool_config: PerQueryMemoryPoolConfig::Disabled,
            heap_memory_limit: None,
        },
        DedicatedExecutor::new(
            "datafusion",
            tokio_datafusion_config
                .builder()
                .map(|mut builder| {
                    builder.enable_all();
                    builder
                })
                .map_err(Error::TokioRuntime)?,
            Arc::clone(&metrics),
        ),
    ));

    // Note: using same metrics registry causes runtime panic.
    let write_path_executor = Arc::new(Executor::new_with_config_and_executor(
        ExecutorConfig {
            // should this be divided? or should this contend for threads with executor that's
            // setup for querying only
            target_query_partitions: tokio_datafusion_config.num_threads.unwrap(),
            object_stores: [&parquet_store]
                .into_iter()
                .map(|store| (store.id(), Arc::clone(store.object_store())))
                .collect(),
            metric_registry: Arc::clone(&write_path_metrics),
            // use as much memory for persistence, can this be UnboundedMemoryPool?
            mem_pool_size: usize::MAX,
            // These are new additions, just skimming through the code it does not look like we can
            // achieve the same effect as having a separate executor. It looks like it's for "all"
            // queries, it'd be nice to have a filter to say when the query matches this pattern
            // apply these limits. If that's possible maybe we could avoid creating a separate
            // executor.
            per_query_mem_pool_config: PerQueryMemoryPoolConfig::Disabled,
            heap_memory_limit: None,
        },
        DedicatedExecutor::new(
            "datafusion_write_path",
            tokio_datafusion_config
                .builder()
                .map_err(Error::TokioRuntime)?,
            Arc::clone(&write_path_metrics),
        ),
    ));

    let trace_header_parser = TraceHeaderParser::new()
        .with_jaeger_trace_context_header_name(
            config
                .tracing_config
                .traces_jaeger_trace_context_header_name,
        )
        .with_jaeger_debug_name(config.tracing_config.traces_jaeger_debug_name);

    // Create table index cache configuration from CLI arguments
    let table_index_cache_config = TableIndexCacheConfig {
        max_entries: if config.table_index_cache_max_entries == 0 {
            None
        } else {
            Some(config.table_index_cache_max_entries)
        },
        concurrency_limit: config.table_index_cache_concurrency_limit,
    };

    let persister = Arc::new(Persister::new(
        Arc::clone(&object_store),
        node_id.as_str(),
        Arc::clone(&time_provider) as _,
        config.checkpoint_interval.map(|v| v.into()),
    ));

    let process_uuid_getter: Arc<dyn ProcessUuidGetter> = Arc::new(ProcessUuidWrapper::new());
    // Pick shared vs per-node catalog: explicit flag wins; otherwise default
    // to shared when the operator picked a split mode (writer/compactor/querier).
    let shared_catalog = config
        .shared_catalog
        .unwrap_or(config.mode != NodeMode::All);
    let catalog = if shared_catalog {
        info!("opening shared catalog at _catalog/");
        Catalog::open_shared_with_shutdown(
            node_id.as_str(),
            Arc::clone(&object_store),
            Arc::clone(&time_provider),
            Arc::clone(&metrics),
            shutdown_manager.register(),
            Arc::clone(&process_uuid_getter),
        )
        .await
        .map_err(Error::InitializeCatalog)?
    } else {
        Catalog::new_with_shutdown(
            node_id.as_str(),
            Arc::clone(&object_store),
            Arc::clone(&time_provider),
            Arc::clone(&metrics),
            shutdown_manager.register(),
            Arc::clone(&process_uuid_getter),
        )
        .await
        .map_err(Error::InitializeCatalog)?
    };
    info!(catalog_uuid = ?catalog.catalog_uuid(), "catalog initialized");

    let retention_handler_token = shutdown_manager.register();
    let _table_index_cache = initialize_table_index_cache(
        node_id.clone(),
        config.retention_check_interval.into(),
        table_index_cache_config,
        Arc::clone(&object_store),
        Arc::clone(&catalog),
        Arc::clone(&time_provider) as _,
        retention_handler_token,
    )
    .await
        .inspect_err(|_e| {
            warn!("TableIndexCache initialization failed, continuing in degraded state.");
            warn!("Without TableIndexCache, object store cleanup for retention policies and hard deletes will temporarily be unable to proceed; compacted data and queries should not be affected.");
        })
    .unwrap_or(None);

    // Initialize tokens from files if provided and auth is enabled
    if !config.without_auth {
        // Initialize admin token from file if provided
        if let Some(admin_token_file) = &config.admin_token_file
            && let Err(e) = initialize_admin_token_from_file(&catalog, admin_token_file).await
        {
            error!("Failed to initialize admin token from file: {}", e);
            return Err(e);
        }
    }

    // Capture and filter CLI parameters
    let cli_params = cli_params::capture_cli_params(user_params);

    let _ = catalog
        .register_node(
            &node_id,
            num_cpus as u64,
            vec![influxdb3_catalog::log::NodeMode::Core],
            process_uuid_getter,
            Some(cli_params),
        )
        .await
        .map_err(Error::InitializeCatalog)?;
    let node_def = catalog
        .node(&node_id)
        .expect("node should be registered in catalog");
    info!(instance_id = ?node_def.instance_id(), "catalog initialized");

    let last_cache = LastCacheProvider::new_from_catalog_with_background_eviction(
        Arc::clone(&catalog),
        config.last_cache_eviction_interval.into(),
    )
    .await
    .map_err(Error::InitializeLastCache)?;

    let distinct_cache = DistinctCacheProvider::new_from_catalog_with_background_eviction(
        Arc::clone(&time_provider) as _,
        Arc::clone(&catalog),
        config.distinct_cache_eviction_interval.into(),
    )
    .await
    .map_err(Error::InitializeDistinctCache)?;

    // Set the gen1 duration in the catalog; if already set, nothing happens; if set to a different
    // value, we emit a WARN; if some other error occurs we exit.
    let gen1_duration = match catalog
        .set_gen1_duration(config.gen1_duration.as_duration())
        .await
    {
        Ok(_) | Err(CatalogError::AlreadyExists) => config.gen1_duration,
        Err(CatalogError::CannotChangeGenerationDuration { .. }) => {
            let existing: Gen1Duration = catalog
                .get_generation_duration(1)
                .unwrap()
                .try_into()
                .expect("catalog should contain valid gen1 duration");
            warn!(
                existing_secs = existing.as_duration().as_secs(),
                provided_secs = config.gen1_duration.as_duration().as_secs(),
                "cannot change the existing gen1 duration after it has been set"
            );
            existing
        }
        Err(error) => return Err(Error::InitializeCatalog(error)),
    };

    let num_snapshots_to_load =
        config.gen1_lookback_duration.as_secs() / gen1_duration.as_duration().as_secs();

    let n_snapshots_to_load_on_start =
        NonZeroU64::new(MIN_SNAPSHOTS_TO_LOAD_ON_START.max(num_snapshots_to_load))
            .expect("n_snapshots_to_load_on_start is always >= 1");

    let wal_config = WalConfig {
        gen1_duration,
        max_write_buffer_size: config.wal_max_write_buffer_size,
        flush_interval: config.wal_flush_interval.into(),
        snapshot_size: config.wal_snapshot_size,
        wal_replay_fail_on_error: config.wal_replay_fail_on_error,
    };

    // Multi-node modes (writer/compactor/querier) participate in the shared
    // inventory: writers publish snapshots there, queriers load peers' files
    // from it, the compactor reads from it. `All` keeps legacy single-node
    // behavior unless the operator explicitly opts in.
    let shared_inventory = if config.mode != NodeMode::All {
        Some(
            influxdb3_write::shared_inventory::SharedInventory::new(Arc::clone(&object_store))
                .with_metrics(&metrics),
        )
    } else {
        None
    };

    // Writer lease: ensure no other process is also accepting writes against
    // this bucket. Acquired before WAL replay so a stale predecessor can't
    // race the WAL writer.
    let writer_lease_ttl: Duration = config.writer_lease_ttl.into();
    if matches!(config.mode, NodeMode::Writer) && !writer_lease_ttl.is_zero() {
        info!("acquiring writer lease (ttl={:?})", writer_lease_ttl);
        let owner = format!("{}-{}", node_id.as_str(), *PROCESS_UUID_STR);
        let writer_lease = Arc::new(influxdb3_write::leases::Lease::new(
            influxdb3_write::leases::LeaseConfig::new(
                object_store::path::Path::from("_locks/writer.lease"),
                owner,
                writer_lease_ttl,
            ),
            Arc::clone(&object_store),
        ));
        // A hard-killed predecessor (OOM, autohealing) never releases its
        // lease; the file only ages out over one TTL. Exiting immediately
        // would just crash-loop under a process supervisor until the TTL
        // passes, so wait it out here instead. A *live* holder keeps
        // refreshing, so its lease never expires and the deadline still
        // refuses a genuine duplicate writer.
        let deadline = Instant::now() + writer_lease_ttl + Duration::from_secs(5);
        loop {
            match writer_lease
                .try_acquire(time_provider.now().timestamp_millis())
                .await
            {
                Ok(true) => break,
                Ok(false) => {
                    if Instant::now() >= deadline {
                        return Err(Error::WriteBufferInit(anyhow::anyhow!(
                            "writer lease still held by a live process after waiting \
                             one TTL ({writer_lease_ttl:?}); refusing to start"
                        )));
                    }
                    info!(
                        ttl = ?writer_lease_ttl,
                        "writer lease held by a previous process; waiting for it to expire"
                    );
                }
                Err(e) => {
                    if Instant::now() >= deadline {
                        return Err(Error::WriteBufferInit(anyhow::anyhow!(
                            "failed to acquire writer lease: {e}"
                        )));
                    }
                    warn!(error = %e, "error acquiring writer lease; retrying");
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        influxdb3_write::leases::run(
            writer_lease,
            Arc::clone(&time_provider) as _,
            shutdown_manager.register(),
            Some(influxdb3_write::leases::LeaseMetrics::new(
                &metrics, "writer",
            )),
        );
    }

    let write_buffer_impl = WriteBufferImpl::new(WriteBufferImplArgs {
        persister: Arc::clone(&persister),
        catalog: Arc::clone(&catalog),
        last_cache,
        distinct_cache,
        time_provider: Arc::clone(&time_provider),
        executor: Arc::clone(&write_path_executor),
        wal_config,
        parquet_cache,
        metric_registry: Arc::clone(&metrics),
        snapshotted_wal_files_to_keep: config.snapshotted_wal_files_to_keep,
        query_file_limit: config.query_file_limit,
        n_snapshots_to_load_on_start,
        shutdown: shutdown_manager.register(),
        wal_replay_concurrency_limit: config.wal_replay_concurrency_limit,
        shared_inventory: shared_inventory.clone(),
        parquet_snapshot_concurrency_limit: config.parquet_snapshot_concurrency_limit,
    })
    .await
    .map_err(|e| Error::WriteBufferInit(e.into()))?;

    let persisted_files = write_buffer_impl.persisted_files();

    // Parquet ref validation: evict in-memory references whose objects do
    // not exist (phantom refs from corrupted manifests). Runs at boot and
    // periodically; on the compactor the next inventory checkpoint then
    // propagates the cleaned view durably.
    let ref_validation_interval: Duration = config.ref_validation_interval.into();
    if !ref_validation_interval.is_zero() {
        info!(
            interval = ?ref_validation_interval,
            "spawning parquet ref validator"
        );
        influxdb3_write::ref_validator::spawn(influxdb3_write::ref_validator::RefValidatorArgs {
            object_store: Arc::clone(&object_store),
            persisted_files: Arc::clone(&persisted_files),
            interval: ref_validation_interval,
            shutdown: shutdown_manager.register(),
            metric_registry: Arc::clone(&metrics),
        });
    }

    // Construct WAL tail (Layer C) here — earlier than where the composite
    // wraps the WriteBufferImpl — so the inventory poller can notify it of
    // `--writers` (node-id=url) supersedes the legacy `--writer-urls` /
    // `--writer-node-ids` pair and ties Layer B endpoints to Layer C WAL
    // prefixes for per-writer fallback.
    let writer_targets: Vec<influxdb3_write::remote_write_buffer::RemoteWriterTarget> = config
        .writers
        .iter()
        .filter(|s| !s.trim().is_empty())
        .map(|entry| {
            let (node_id, url) = entry.split_once('=').ok_or_else(|| {
                Error::WriteBufferInit(anyhow::anyhow!(
                    "invalid --writers entry {entry:?}: expected node-id=url"
                ))
            })?;
            Ok(influxdb3_write::remote_write_buffer::RemoteWriterTarget {
                node_id: Some(node_id.trim().to_string()),
                url: url.trim().to_string(),
            })
        })
        .collect::<Result<_, Error>>()?;

    // covered-through WAL sequences and the tail can drop redundant entries.
    let wal_tail_buffer = if matches!(config.mode, NodeMode::Querier) {
        let writer_node_ids: Vec<String> = if writer_targets.is_empty() {
            config
                .writer_node_ids
                .iter()
                .filter(|s| !s.trim().is_empty())
                .map(|s| s.trim().to_string())
                .collect()
        } else {
            writer_targets
                .iter()
                .filter_map(|t| t.node_id.clone())
                .collect()
        };
        let wal_tail_interval: Duration = config.wal_tail_poll_interval.into();
        if !writer_node_ids.is_empty() && !wal_tail_interval.is_zero() {
            info!(?writer_node_ids, interval = ?wal_tail_interval,
                "enabling layer C (wal tail)");
            let t = influxdb3_write::wal_tail::WalTailBuffer::new(
                Arc::clone(&object_store),
                Arc::clone(&catalog),
                writer_node_ids,
                config.wal_tail_max_files,
            );
            Arc::clone(&t).spawn(influxdb3_write::wal_tail::WalTailBufferArgs {
                poll_interval: wal_tail_interval,
                shutdown: shutdown_manager.register(),
                metric_registry: Arc::clone(&metrics),
            });
            Some(t)
        } else {
            None
        }
    } else {
        None
    };

    // Inventory poller: pulls peer WAL snapshots + compaction manifests into
    // PersistedFiles + catalog without a restart. Only meaningful in modes
    // that share an inventory namespace.
    let inventory_poll_interval: Duration = config.inventory_poll_interval.into();
    if config.mode != NodeMode::All
        && !inventory_poll_interval.is_zero()
    {
        if let Some(inv) = &shared_inventory {
            info!(
                interval = ?inventory_poll_interval,
                "spawning shared-inventory poller"
            );
            influxdb3_write::inventory_poller::spawn(
                influxdb3_write::inventory_poller::InventoryPollerArgs {
                    inventory: inv.clone(),
                    persisted_files: Arc::clone(&persisted_files),
                    catalog: Arc::clone(&catalog),
                    interval: inventory_poll_interval,
                    initial_wal_watermarks: write_buffer_impl.initial_wal_watermarks(),
                    initial_compaction_watermark: write_buffer_impl
                        .initial_compaction_watermark(),
                    shutdown: shutdown_manager.register(),
                    wal_tail: wal_tail_buffer.clone(),
                    metric_registry: Arc::clone(&metrics),
                },
            );
        }
    }

    let object_deleter = Some(Arc::clone(&persisted_files) as _);

    deleter::run(
        DeleteManagerArgs {
            catalog: Arc::clone(&catalog),
            time_provider: Arc::clone(&time_provider),
            object_deleter,
            delete_grace_period: *config.delete_grace_period,
        },
        shutdown_manager.register(),
    );

    info!("setting up background mem check for query buffer");
    background_buffer_checker(
        config.force_snapshot_mem_threshold.as_num_bytes(),
        &write_buffer_impl,
    )
    .await;

    // Set up generation durations in catalog for multi-level compaction
    let mut generation_durations = std::collections::HashMap::new();
    generation_durations.insert(1, config.gen1_duration.as_duration());
    
    if let Some(gen2_duration) = config.gen2_duration {
        set_generation_duration_with_error_handling(
            &catalog,
            &mut generation_durations,
            2,
            gen2_duration.as_duration(),
        ).await?;
    }
    if let Some(gen3_duration) = config.gen3_duration {
        set_generation_duration_with_error_handling(
            &catalog,
            &mut generation_durations,
            3,
            gen3_duration.as_duration(),
        ).await?;
    }
    if let Some(gen4_duration) = config.gen4_duration {
        set_generation_duration_with_error_handling(
            &catalog,
            &mut generation_durations,
            4,
            gen4_duration.as_duration(),
        ).await?;
    }
    if let Some(gen5_duration) = config.gen5_duration {
        set_generation_duration_with_error_handling(
            &catalog,
            &mut generation_durations,
            5,
            gen5_duration.as_duration(),
        ).await?;
    }

    // Compaction runs in `all` and `compactor` modes. `writer` and `querier`
    // are explicitly not allowed to compact even if `--enable-compaction`
    // is left at its default `true`.
    let should_compact = config.mode.runs_compaction() && config.enable_compaction;
    if should_compact {
        info!("setting up compaction service ({:?} mode)", config.mode);
        let compaction_config = influxdb3_write::compaction::CompactionConfig {
            enabled: true,
            interval: config.compaction_interval.into(),
            max_files_per_run: config.max_compaction_files,
            min_files_for_compaction: config.min_files_for_compaction,
            generation_durations,
            delete_grace: config.compaction_delete_grace.into(),
            checkpoint_every_n_cycles: 10,
            claim_ttl: Duration::from_secs(30 * 60),
        };

        let mut compaction_service = influxdb3_write::compaction::CompactionService::new(
            compaction_config,
            Arc::clone(&catalog),
            Arc::clone(&write_buffer_impl) as Arc<dyn WriteBuffer>,
            Arc::clone(&persister),
            Arc::clone(&write_path_executor),
            Arc::clone(&object_store),
            Arc::clone(&time_provider),
            shutdown_manager.register(),
            Arc::clone(&metrics),
        );
        if let Some(inv) = &shared_inventory {
            compaction_service = compaction_service.with_shared_inventory(inv.clone());
        }

        // Advisory compactor lease. Skipped when TTL=0 (single-node operator
        // opting out) or when running `all` mode (no peers expected).
        let lease_ttl: Duration = config.compactor_lease_ttl.into();
        if !lease_ttl.is_zero() && matches!(config.mode, NodeMode::Compactor) {
            info!("acquiring compactor lease (ttl={:?})", lease_ttl);
            let lease_owner = format!("{}-{}", node_id.as_str(), *PROCESS_UUID_STR);
            let lease = Arc::new(influxdb3_write::leases::Lease::new(
                influxdb3_write::leases::LeaseConfig::new(
                    object_store::path::Path::from("_locks/compactor.lease"),
                    lease_owner,
                    lease_ttl,
                ),
                Arc::clone(&object_store),
            ));
            influxdb3_write::leases::run(
                Arc::clone(&lease),
                Arc::clone(&time_provider) as _,
                shutdown_manager.register(),
                Some(influxdb3_write::leases::LeaseMetrics::new(
                    &metrics,
                    "compactor",
                )),
            );
            compaction_service = compaction_service.with_lease(lease);
        }

        let compaction_service = Arc::new(compaction_service);
        compaction_service.start();
        info!("compaction service started");
    } else if !config.mode.runs_compaction() {
        info!(
            "compaction service disabled (mode={:?} does not run compaction)",
            config.mode
        );
    } else {
        info!("compaction service disabled");
    }

    info!("setting up telemetry store");
    let telemetry_store = setup_telemetry_store(TelemetryStoreSetupArgs {
        object_store_config: &config.object_store_config,
        instance_id: node_def.instance_id(),
        num_cpus,
        persisted_files: Some(persisted_files),
        telemetry_endpoint: &config.telemetry_endpoint,
        disable_upload: config.disable_telemetry_upload,
        serve_invocation_method: config.serve_invocation_method,
        catalog_uuid: catalog.catalog_uuid().to_string(),
        processing_engine_metrics: Arc::clone(&catalog) as Arc<dyn ProcessingEngineMetrics>,
    })
    .await;

    // Querier wraps WriteBufferImpl in a CompositeWriteBuffer so reads see
    // hot rows from the writer (Layer B) and WAL tail (Layer C) in addition
    // to the locally-folded inventory (Layer A). Writer / compactor / all
    // pass the WriteBufferImpl through unchanged.
    let write_buffer: Arc<dyn WriteBuffer> = if matches!(config.mode, NodeMode::Querier) {
        let remote_targets: Vec<influxdb3_write::remote_write_buffer::RemoteWriterTarget> =
            if writer_targets.is_empty() {
                config
                    .writer_urls
                    .iter()
                    .filter(|s| !s.trim().is_empty())
                    .map(|s| influxdb3_write::remote_write_buffer::RemoteWriterTarget {
                        node_id: None,
                        url: s.trim().to_string(),
                    })
                    .collect()
            } else {
                writer_targets.clone()
            };
        let remote = (!remote_targets.is_empty()).then(|| {
            info!(?remote_targets, "enabling layer B (remote hot chunks)");
            Arc::new(
                influxdb3_write::remote_write_buffer::RemoteWriteBuffer::with_targets(
                    remote_targets,
                    config.remote_hot_timeout.into(),
                )
                .with_metrics(&metrics),
            )
        });

        Arc::new(influxdb3_write::composite_write_buffer::CompositeWriteBuffer::new(
            Arc::clone(&write_buffer_impl),
            remote,
            wal_tail_buffer.clone(),
        )) as Arc<dyn WriteBuffer>
    } else {
        write_buffer_impl
    };

    let common_state = CommonServerState::new(
        Arc::clone(&catalog),
        Arc::clone(&metrics),
        trace_exporter,
        trace_header_parser,
        Arc::clone(&telemetry_store),
    );

    if config.without_auth {
        warn!(
            "server started without auth (`--without-auth` switch), all token creation and regeneration of admin token endpoints are disabled"
        );
    }

    let query_executor = Arc::new(QueryExecutorImpl::new(CreateQueryExecutorArgs {
        catalog: write_buffer.catalog(),
        write_buffer: Arc::clone(&write_buffer),
        exec: Arc::clone(&exec),
        metrics: Arc::clone(&metrics),
        datafusion_config: Arc::new(config.iox_query_datafusion_config.build()),
        query_log_size: config.query_log_size,
        telemetry_store: Arc::clone(&telemetry_store),
        sys_events_store: Arc::clone(&sys_events_store),
        // convert to positive here so that we can avoid double negatives downstream
        started_with_auth: !config.without_auth,
        time_provider: Arc::clone(&time_provider) as _,
        processing_engine: None,
    }));

    // Take the listener back from the startup probe — same socket, so the
    // port (including an OS-assigned `:0`) never changes across handover.
    let listener = startup_probe.into_listener().await.map_err(Error::Server)?;

    // Only create recovery listener if explicitly enabled
    let admin_token_recovery_listener = if let Some(addr) = config.admin_token_recovery_bind_address
    {
        info!(%addr, "Admin token recovery endpoint enabled - WARNING: This allows unauthenticated admin token regeneration!");
        Some(TcpListener::bind(*addr).await.map_err(Error::BindAddress)?)
    } else {
        None
    };

    let processing_engine = ProcessingEngineManagerImpl::new(
        setup_processing_engine_env_manager(&config.processing_engine_config),
        write_buffer.catalog(),
        node_id,
        Arc::clone(&write_buffer) as Arc<dyn influxdb3_write::Bufferer>,
        Arc::clone(&query_executor) as _,
        Arc::clone(&time_provider) as _,
        sys_events_store,
    )
    .await
    .map_err(Error::PythonEnvironmentInitialization)?;

    // Update query executor with processing engine reference
    query_executor.set_processing_engine(Arc::clone(&processing_engine));

    let cert_file = config.cert_file;
    let key_file = config.key_file;

    // Start processing engine triggers
    Arc::clone(&processing_engine)
        .start_triggers()
        .await
        .expect("failed to start processing engine triggers");

    write_buffer
        .wal()
        .add_file_notifier(Arc::clone(&processing_engine) as _);

    let authorizer: Arc<dyn influxdb3_authz::AuthProvider> = if config.without_auth {
        Arc::new(influxdb3_authz::NoAuthAuthenticator)
    } else {
        Arc::new(TokenAuthenticator::new(
            Arc::clone(&catalog) as _,
            Arc::clone(&time_provider) as _,
        ))
    };

    let endpoint_policy = influxdb3_server::http::EndpointPolicy {
        allow_write: config.mode.runs_ingest(),
        allow_query: config.mode.runs_query(),
        // Only the writer (and legacy `all`) holds live in-memory rows that
        // a querier could fetch. Everywhere else this RPC is dead weight,
        // so disable it explicitly to make 405 the deliberate response.
        allow_internal_rpc: matches!(config.mode, NodeMode::All | NodeMode::Writer),
    };
    let http = Arc::new(
        HttpApi::new(
            common_state.clone(),
            Arc::clone(&time_provider) as _,
            Arc::clone(&write_buffer),
            Arc::clone(&query_executor) as _,
            Arc::clone(&processing_engine),
            config.max_http_request_size,
            Arc::clone(&authorizer),
        )
        .with_endpoint_policy(endpoint_policy),
    );

    // Only create recovery server if listener was created
    let admin_token_recovery_server = admin_token_recovery_listener.map(|listener| {
        Server::new(CreateServerArgs {
            common_state: common_state.clone(),
            http: Arc::clone(&http),
            authorizer: Arc::clone(&authorizer),
            listener,
            cert_file: cert_file.clone(),
            key_file: key_file.clone(),
            tls_minimum_version: (&config.tls_minimum_version).into(),
        })
    });

    let server = Server::new(CreateServerArgs {
        common_state,
        http,
        authorizer,
        listener,
        cert_file,
        key_file,
        tls_minimum_version: (&config.tls_minimum_version).into(),
    });

    // There are two different select! macros - tokio::select and futures::select
    //
    // tokio::select takes ownership of the passed future "moving" it into the
    // select block. This works well when not running select inside a loop, or
    // when using a future that can be dropped and recreated, often the case
    // with tokio's futures e.g. `channel.recv()`
    //
    // futures::select is more flexible as it doesn't take ownership of the provided
    // future. However, to safely provide this it imposes some additional
    // requirements
    //
    // All passed futures must implement FusedFuture - it is IB to poll a future
    // that has returned Poll::Ready(_). A FusedFuture has an is_terminated()
    // method that indicates if it is safe to poll - e.g. false if it has
    // returned Poll::Ready(_). futures::select uses this to implement its
    // functionality. futures::FutureExt adds a fuse() method that
    // wraps an arbitrary future and makes it a FusedFuture
    //
    // The additional requirement of futures::select is that if the future passed
    // outlives the select block, it must be Unpin or already Pinned

    // Create the FusedFutures that will be waited on before exiting the process
    let signal = wait_for_signal().fuse();
    let paths_without_authz: &'static Vec<&'static str> = config
        .disable_authz
        .unwrap_or_default()
        .get_mapped_endpoints();

    info!(
        ?paths_without_authz,
        "setting up server with authz disabled for paths"
    );

    let frontend = serve(
        server,
        frontend_shutdown.clone(),
        startup_timer,
        config.without_auth,
        paths_without_authz,
        config.tcp_listener_file_path,
    )
    .fuse();
    let backend = shutdown_manager.join().fuse();

    // Only start recovery endpoint if server was created
    let recovery_endpoint_enabled = admin_token_recovery_server.is_some();
    let recovery_frontend = if let Some(recovery_server) = admin_token_recovery_server {
        futures::future::Either::Left(
            serve_admin_token_recovery_endpoint(
                recovery_server,
                frontend_shutdown.clone(),
                config.admin_token_recovery_tcp_listener_file_path,
            )
            .fuse(),
        )
    } else {
        // Provide a future that never completes if recovery endpoint is disabled
        futures::future::Either::Right(
            futures::future::pending::<Result<(), influxdb3_server::Error>>().fuse(),
        )
    };

    // pin_mut constructs a Pin<&mut T> from a T by preventing moving the T
    // from the current stack frame and constructing a Pin<&mut T> to it
    pin_mut!(signal);
    pin_mut!(frontend);
    pin_mut!(backend);
    pin_mut!(recovery_frontend);

    let mut res = Ok(());
    let mut recovery_endpoint_active = recovery_endpoint_enabled;

    // Graceful shutdown can be triggered by sending SIGINT or SIGTERM to the
    // process, or by a background task exiting - most likely with an error
    while !frontend.is_terminated() {
        futures::select! {
            // External shutdown signal, e.g., `ctrl+c`
            _ = signal => info!("shutdown requested"),
            // `join` on the `ShutdownManager` has completed
            _ = backend => {
                // If something stops the process on the backend the frontend shutdown should have
                // been signaled in which case we can break the loop here once checking that it
                // has been cancelled.
                //
                // The select! could also pick this branch in the event that the frontend and
                // backend stop at the same time. That shouldn't be an issue so long as the frontend
                // has indeed stopped, so we check on exiting the loop that the frontend has
                // terminated before checking and waiting on the backend.
                if frontend_shutdown.is_cancelled() {
                    break;
                }
                error!("backend shutdown before frontend");
                res = res.and(Err(Error::LostBackend));
            }
            // HTTP/gRPC frontend has stopped
            result = frontend => {
                match result {
                    Ok(_) if frontend_shutdown.is_cancelled() => info!("HTTP/gRPC service shutdown"),
                    Ok(_) => {
                        error!("early HTTP/gRPC service exit");
                        res = res.and(Err(Error::LostHttpGrpc));
                    },
                    Err(error) => {
                        error!("HTTP/gRPC error");
                        res = res.and(Err(Error::Server(error)));
                    },
                }
            },
            // Admin token recovery endpoint has stopped
            recovery_result = recovery_frontend => {
                // Only process recovery endpoint results if it was actually enabled and active
                if recovery_endpoint_enabled && recovery_endpoint_active {
                    match recovery_result {
                        Ok(_) if frontend_shutdown.is_cancelled() => {
                            info!("Admin token recovery service shutdown");
                            // Only break if the main shutdown was also requested
                            if frontend.is_terminated() {
                                break;
                            }
                        }
                        Ok(_) => {
                            // Recovery endpoint can shut down normally after token regeneration
                            // This is expected behavior and should not cause an error
                            info!("Admin token recovery service exited normally after token regeneration");
                            recovery_endpoint_active = false;
                            // Since recovery_frontend is a FusedFuture, it won't be polled again
                            // after completion, so we don't need to do anything else
                            // Continue the loop - do NOT break or call shutdown
                            continue; // Skip shutdown_manager.shutdown() for this iteration
                        }
                        Err(error) => {
                            error!(%error, "admin token recovery service error");
                            res = res.and(Err(Error::Server(error)));
                            // Continue running the main server even if recovery endpoint had an error
                        }
                    }
                }
                // If recovery endpoint was disabled, this branch will never be taken again
                // because pending() futures never complete
            }
        }
        shutdown_manager.shutdown()
    }
    // ensure that the frontend has fully terminated so we dont close the connection on any clients
    if !frontend.is_terminated() {
        res = res.and(frontend.await.map_err(Error::Server));
    }
    info!("frontend shutdown completed");

    if !backend.is_terminated() {
        backend.await;
    }
    info!("backend shutdown completed");

    res
}

pub(crate) fn setup_processing_engine_env_manager(
    config: &ProcessingEngineConfig,
) -> ProcessingEngineEnvironmentManager {
    let package_manager: Arc<dyn PythonEnvironmentManager> = match config.package_manager {
        PackageManager::Discover => determine_package_manager(),
        PackageManager::Pip => Arc::new(PipManager),
        PackageManager::UV => Arc::new(UVManager),
        PackageManager::Disabled => Arc::new(DisabledPackageManager),
    };
    ProcessingEngineEnvironmentManager {
        plugin_dir: config.plugin_dir.clone(),
        virtual_env_location: config.virtual_env_location.clone(),
        package_manager,
        plugin_repo: config.plugin_repo.clone(),
    }
}

fn determine_package_manager() -> Arc<dyn PythonEnvironmentManager> {
    // Check for pip (highest preference)
    let python_exe = find_python();
    debug!("Running: {} -m pip --version", python_exe.display());

    if let Ok(output) = Command::new(&python_exe)
        .args(["-m", "pip", "--version"])
        .output()
        && output.status.success()
    {
        return Arc::new(PipManager);
    }

    // Check for uv second (ie, prefer python standalone pip)
    if let Ok(output) = Command::new("uv").arg("--version").output()
        && output.status.success()
    {
        return Arc::new(UVManager);
    }

    // If neither is available, return DisabledManager
    Arc::new(DisabledManager)
}

async fn initialize_table_index_cache(
    node_id: String,
    retention_check_interval: Duration,
    table_index_cache_config: TableIndexCacheConfig,
    object_store: Arc<dyn ObjectStore>,
    catalog: Arc<Catalog>,
    time_provider: Arc<dyn TimeProvider>,
    retention_handler_token: ShutdownToken,
) -> Result<Option<TableIndexCache>> {
    let table_index_cache = TableIndexCache::new(
        node_id.clone(),
        table_index_cache_config,
        Arc::clone(&object_store),
    );

    info!(
        node_id = node_id.clone(),
        max_entries = ?table_index_cache_config.max_entries,
        concurrency_limit = table_index_cache_config.concurrency_limit,
        "Initializing table index cache"
    );

    // Initialize table index cache from any existing snapshots
    //
    // This needs to happen before WAL snapshotting, retention handling, or hard deletion could
    // begin executing so we have a quiescent time during which we can transform
    // `PersistedSnapshot` to `TableIndexSnapshot` to `TableIndex` to completion.
    table_index_cache.initialize().await.map_err(|e| {
        warn!("Failed to initialize table index cache: {}", e);
        Error::WriteBufferInit(anyhow::anyhow!(
            "Failed to initialize table index cache: {}",
            e
        ))
    })?;

    // Create and start the retention period handler
    let retention_handler = Arc::new(RetentionPeriodHandler::new(
        table_index_cache.clone(),
        Arc::clone(&catalog),
        Arc::clone(&time_provider) as _,
        retention_check_interval,
        node_id.to_string(),
    ));

    tokio::spawn(async move {
        retention_handler
            .background_task(retention_handler_token)
            .await
    });

    Ok(Some(table_index_cache))
}

struct TelemetryStoreSetupArgs<'a> {
    object_store_config: &'a ObjectStoreConfig,
    instance_id: Arc<str>,
    num_cpus: usize,
    persisted_files: Option<Arc<PersistedFiles>>,
    telemetry_endpoint: &'a str,
    disable_upload: bool,
    catalog_uuid: String,
    serve_invocation_method: ServeInvocationMethod,
    processing_engine_metrics: Arc<dyn ProcessingEngineMetrics>,
}

async fn setup_telemetry_store(
    TelemetryStoreSetupArgs {
        object_store_config,
        instance_id,
        num_cpus,
        persisted_files,
        telemetry_endpoint,
        disable_upload,
        catalog_uuid,
        serve_invocation_method,
        processing_engine_metrics,
    }: TelemetryStoreSetupArgs<'_>,
) -> Arc<TelemetryStore> {
    let os = std::env::consts::OS;
    let influxdb_pkg_version = env!("CARGO_PKG_VERSION");
    let influxdb_pkg_name = env!("CARGO_PKG_NAME");
    // Following should show influxdb3-0.1.0
    let influx_version = format!("{influxdb_pkg_name}-{influxdb_pkg_version}");
    let obj_store_type = object_store_config.object_store;
    let storage_type = obj_store_type.as_str();

    if disable_upload {
        debug!("Initializing TelemetryStore with upload disabled.");
        TelemetryStore::new_without_background_runners(
            persisted_files.map(|p| p as _),
            processing_engine_metrics,
        )
    } else {
        debug!("Initializing TelemetryStore with upload enabled for {telemetry_endpoint}.");
        TelemetryStore::new(CreateTelemetryStoreArgs {
            instance_id,
            os: Arc::from(os),
            influx_version: Arc::from(influx_version),
            storage_type: Arc::from(storage_type),
            cores: num_cpus,
            persisted_files: persisted_files.map(|p| p as _),
            telemetry_endpoint: telemetry_endpoint.to_string(),
            catalog_uuid,
            serve_invocation_method,
            processing_engine_metrics,
        })
        .await
    }
}

async fn background_buffer_checker(
    mem_threshold_bytes: usize,
    write_buffer_impl: &Arc<WriteBufferImpl>,
) {
    debug!(mem_threshold_bytes, "setting up background buffer checker");
    check_mem_and_force_snapshot_loop(
        Arc::clone(write_buffer_impl),
        mem_threshold_bytes,
        Duration::from_secs(10),
    )
    .await;
}

#[cfg(all(
    feature = "jemalloc_replacing_malloc",
    not(target_env = "msvc"),
    not(feature = "disable_custom_global_allocator")
))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use influxdb3_write::deleter::DeleteManagerArgs;
#[cfg(tokio_unstable)]
use tokio_metrics_bridge::setup_tokio_metrics;

#[cfg(any(not(feature = "jemalloc_replacing_malloc"), target_env = "msvc"))]
pub fn build_malloc_conf() -> String {
    "system".to_string()
}

#[cfg(all(feature = "jemalloc_replacing_malloc", not(target_env = "msvc")))]
pub fn build_malloc_conf() -> String {
    tikv_jemalloc_ctl::config::malloc_conf::mib()
        .unwrap()
        .read()
        .unwrap()
        .to_string()
}

pub fn setup_metric_registry() -> Arc<metric::Registry> {
    let registry = Arc::new(metric::Registry::default());

    // See https://prometheus.io/docs/instrumenting/writing_clientlibs/#process-metrics
    registry
        .register_metric::<U64Gauge>(
            "process_start_time_seconds",
            "Start time of the process since unix epoch in seconds.",
        )
        .recorder(&[
            ("product_name", influxdb3_server::PRODUCT_NAME),
            ("version", INFLUXDB3_VERSION.as_ref()),
            ("git_hash", INFLUXDB3_GIT_HASH),
            ("uuid", PROCESS_UUID_STR.as_ref()),
        ])
        .set(PROCESS_START_TIME.timestamp() as u64);

    // Register jemalloc metrics
    #[cfg(all(feature = "jemalloc_replacing_malloc", not(target_env = "msvc")))]
    registry.register_instrument("jemalloc_metrics", jemalloc::JemallocMetrics::new);

    // Register tokio metric for main runtime
    #[cfg(tokio_unstable)]
    setup_tokio_metrics(
        tokio::runtime::Handle::current().metrics(),
        "main",
        Arc::clone(&registry),
    );

    registry
}

/// Initialize an admin token from a JSON file.
///
/// The token file must be in this format: `{"token": "apiv3_...", "name": "custom_name", "expiry_millis": 1234567890}`
///
/// If an admin token with the same name already exists, this function will succeed without creating a duplicate.
/// File permissions should be restricted (0600) to protect the token.
async fn initialize_admin_token_from_file(catalog: &Catalog, token_file: &PathBuf) -> Result<()> {
    use sha2::{Digest, Sha512};

    info!(
        "Initializing admin token from file: {}",
        token_file.display()
    );

    // Check file permissions on Unix systems
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let metadata = tokio::fs::metadata(token_file).await.map_err(|e| {
            Error::TokenError(CatalogError::unexpected(format!(
                "Failed to read admin token file metadata: {e}",
            )))
        })?;
        let mode = metadata.permissions().mode();
        if mode & 0o077 != 0 {
            warn!(
                "Admin token file has insecure permissions: {:o}. Consider using chmod 0600.",
                mode & 0o777
            );
        }
    }

    // Read file content
    let content = tokio::fs::read_to_string(token_file).await.map_err(|e| {
        Error::TokenError(CatalogError::unexpected(format!(
            "Failed to read admin token file: {e}",
        )))
    })?;

    // Parse JSON format
    let admin_token_file: AdminTokenFile = serde_json::from_str(&content).map_err(|e| {
        Error::TokenError(CatalogError::unexpected(format!(
            "Failed to parse admin token file as JSON: {e}",
        )))
    })?;

    info!(
        "Loaded admin token from file, name: {}",
        admin_token_file.name
    );

    let token = admin_token_file.token;
    let name = admin_token_file.name;
    let expiry_millis = admin_token_file.expiry_millis;

    // Validate token format
    if !token.starts_with("apiv3_") {
        return Err(Error::TokenError(CatalogError::unexpected(
            "Invalid token format: must start with 'apiv3_'",
        )));
    }

    // Compute hash from token (same as authentication does)
    let hash = Sha512::digest(&token).to_vec();

    // Create admin token with computed hash and name
    match catalog
        .create_named_admin_token_with_hash(name.clone(), hash, expiry_millis)
        .await
    {
        Ok(()) => {
            info!("Admin token '{}' initialized from file", name);
            Ok(())
        }
        Err(CatalogError::TokenNameAlreadyExists(existing_name)) => {
            info!(
                "Admin token '{}' already exists, skipping initialization",
                existing_name
            );
            Ok(())
        }
        Err(e) => Err(Error::TokenError(e)),
    }
}
