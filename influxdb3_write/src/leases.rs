//! Singleton leases backed by an object store.
//!
//! Used to enforce "only one compactor / writer at a time" when multiple
//! processes can reach the same backing bucket. The lease is a JSON object at
//! a well-known path containing `{owner, acquired_at, expires_at}`. Atomicity
//! relies on `object_store`'s `PutMode::Create` (If-None-Match: *) and
//! `PutMode::Update(version)` for refresh.
//!
//! Behaviour by backend:
//! - S3, MinIO (recent), Azure: full conditional support → safe.
//! - LocalFileSystem: supports Create+Update.
//! - InMemory: supports Create+Update.
//! - Some older S3-compatibles: `Create` returns `NotSupported` → we log a
//!   warning and fall back to non-atomic "read-check-write". Acceptable for
//!   dev; do not deploy in production behind such a backend.
//!
//! On graceful shutdown the lease file is deleted so a standby can take over
//! without waiting for TTL expiry.

use bytes::Bytes;
use metric::{Metric, U64Counter, U64Gauge};
use object_store::path::Path as ObjPath;
use object_store::{
    Error as ObjStoreError, ObjectStore, PutMode, PutOptions, PutResult, UpdateVersion,
};
use observability_deps::tracing::{debug, info, warn};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

/// Prometheus metrics for one lease. Construct with the lease's role name
/// (`"writer"` / `"compactor"`) and pass to [`run`].
#[derive(Debug)]
pub struct LeaseMetrics {
    is_leader: U64Gauge,
    operations: Metric<U64Counter>,
    lease_name: &'static str,
}

impl LeaseMetrics {
    pub fn new(registry: &metric::Registry, lease_name: &'static str) -> Self {
        let is_leader = registry
            .register_metric::<U64Gauge>(
                "influxdb3_lease_is_leader",
                "1 when this process holds the lease, 0 otherwise",
            )
            .recorder(&[("lease", lease_name)]);
        let operations = registry.register_metric::<U64Counter>(
            "influxdb3_lease_operations",
            "lease acquire/renew/release attempts by result",
        );
        Self {
            is_leader,
            operations,
            lease_name,
        }
    }

    fn record(&self, op: &'static str, result: &'static str) {
        self.operations
            .recorder(&[("lease", self.lease_name), ("op", op), ("result", result)])
            .inc(1);
    }

    fn set_leader(&self, leader: bool) {
        self.is_leader.set(u64::from(leader));
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LeaseError {
    #[error("object store error: {0}")]
    ObjectStore(#[from] ObjStoreError),
    #[error("serde_json error: {0}")]
    SerdeJson(#[from] serde_json::Error),
}

/// Construction parameters for a [`Lease`].
#[derive(Debug, Clone)]
pub struct LeaseConfig {
    /// Object store path that holds the lease JSON document.
    pub path: ObjPath,
    /// Stable identifier for the lease holder. Typically `node_id` plus a
    /// process UUID so a restart of the same node doesn't believe it's still
    /// the previous instance.
    pub owner: String,
    /// Total lifetime per refresh. A holder that fails to refresh within
    /// `ttl` is considered crashed and another process may take over.
    pub ttl: Duration,
    /// Refresh cadence. Should be well below `ttl` — `ttl / 3` is typical.
    pub refresh_interval: Duration,
}

impl LeaseConfig {
    pub fn new(path: ObjPath, owner: impl Into<String>, ttl: Duration) -> Self {
        let refresh_interval = ttl.checked_div(3).unwrap_or(Duration::from_secs(10));
        Self {
            path,
            owner: owner.into(),
            ttl,
            refresh_interval,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct LeaseDoc {
    owner: String,
    acquired_at_unix_ms: i64,
    expires_at_unix_ms: i64,
}

#[derive(Debug, Default)]
struct Inner {
    /// Set when this process is the current holder.
    held_version: Option<UpdateVersion>,
    /// Wall-clock millis. Cached so callers can check `is_leader()` cheaply.
    expires_at_unix_ms: i64,
}

/// Tracks lease ownership for one process.
///
/// Create with [`Lease::new`], then `Arc::new(lease).run(time_provider, shutdown)`
/// to start the background acquire-and-refresh loop. Callers gate work on
/// [`Lease::is_leader`].
#[derive(Debug)]
pub struct Lease {
    config: LeaseConfig,
    object_store: Arc<dyn ObjectStore>,
    inner: Mutex<Inner>,
}

impl Lease {
    pub fn new(config: LeaseConfig, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            config,
            object_store,
            inner: Mutex::new(Inner::default()),
        }
    }

    /// True if this process currently holds the lease and it has not yet expired
    /// according to wall-clock time provided by the caller.
    pub fn is_leader(&self, now_unix_ms: i64) -> bool {
        let inner = self.inner.lock();
        inner.held_version.is_some() && inner.expires_at_unix_ms > now_unix_ms
    }

    /// Single attempt to acquire (or take over an expired) lease. Returns `Ok(true)`
    /// if this process now holds it, `Ok(false)` if another live holder owns it.
    pub async fn try_acquire(&self, now_unix_ms: i64) -> Result<bool, LeaseError> {
        let new_doc = LeaseDoc {
            owner: self.config.owner.clone(),
            acquired_at_unix_ms: now_unix_ms,
            expires_at_unix_ms: now_unix_ms + self.config.ttl.as_millis() as i64,
        };
        let payload = serde_json::to_vec(&new_doc)?;

        // First, try Create — works only when no lease exists.
        match self
            .object_store
            .put_opts(
                &self.config.path,
                Bytes::from(payload.clone()).into(),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(put_result) => {
                self.record_acquired(put_result, new_doc.expires_at_unix_ms);
                info!(
                    "lease {} acquired by {} (no prior holder)",
                    self.config.path, self.config.owner
                );
                return Ok(true);
            }
            Err(ObjStoreError::AlreadyExists { .. }) => {
                // existing lease — fall through to the takeover path
            }
            Err(ObjStoreError::NotSupported { .. }) => {
                // Backend lacks atomic Create. Best effort.
                warn!(
                    "object store does not support atomic put-if-not-exists; \
                     lease {} will use non-atomic acquire and may briefly run \
                     duplicate holders under contention",
                    self.config.path
                );
                return self.acquire_non_atomic(new_doc, payload, now_unix_ms).await;
            }
            Err(e) => return Err(e.into()),
        }

        // Read current holder; only take over if their lease has expired.
        let existing_bytes = self.object_store.get(&self.config.path).await?.bytes().await?;
        let existing: LeaseDoc = serde_json::from_slice(&existing_bytes)?;
        if existing.expires_at_unix_ms > now_unix_ms {
            debug!(
                "lease {} held by {} until {} (current {})",
                self.config.path,
                existing.owner,
                existing.expires_at_unix_ms,
                now_unix_ms
            );
            return Ok(false);
        }

        // Expired holder. Take over via conditional Update so we don't race
        // a peer doing the same.
        let head = self.object_store.head(&self.config.path).await?;
        let version = UpdateVersion {
            e_tag: head.e_tag,
            version: head.version,
        };
        match self
            .object_store
            .put_opts(
                &self.config.path,
                Bytes::from(payload).into(),
                PutOptions::from(PutMode::Update(version)),
            )
            .await
        {
            Ok(put_result) => {
                self.record_acquired(put_result, new_doc.expires_at_unix_ms);
                info!(
                    "lease {} taken over by {} from expired holder {}",
                    self.config.path, self.config.owner, existing.owner
                );
                Ok(true)
            }
            Err(ObjStoreError::Precondition { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Refresh extends the current holder's expiry. Returns Ok(true) if the
    /// holder is still us afterwards, Ok(false) if we lost the lease (e.g.
    /// took too long and another process took over).
    pub async fn refresh(&self, now_unix_ms: i64) -> Result<bool, LeaseError> {
        let version = {
            let inner = self.inner.lock();
            match &inner.held_version {
                Some(v) => v.clone(),
                None => return Ok(false),
            }
        };
        let new_doc = LeaseDoc {
            owner: self.config.owner.clone(),
            acquired_at_unix_ms: now_unix_ms,
            expires_at_unix_ms: now_unix_ms + self.config.ttl.as_millis() as i64,
        };
        let payload = serde_json::to_vec(&new_doc)?;
        match self
            .object_store
            .put_opts(
                &self.config.path,
                Bytes::from(payload).into(),
                PutOptions::from(PutMode::Update(version)),
            )
            .await
        {
            Ok(put_result) => {
                self.record_acquired(put_result, new_doc.expires_at_unix_ms);
                Ok(true)
            }
            Err(ObjStoreError::Precondition { .. }) | Err(ObjStoreError::NotFound { .. }) => {
                self.inner.lock().held_version = None;
                warn!(
                    "lease {} lost during refresh — another process took over",
                    self.config.path
                );
                Ok(false)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Best-effort release. Called on graceful shutdown so a standby can pick up
    /// immediately instead of waiting for TTL expiry. Safe to call even if the
    /// lease is not held by us — the conditional delete protects against
    /// stomping a successor.
    pub async fn release(&self) {
        let Some(version) = self.inner.lock().held_version.clone() else {
            return;
        };
        // object_store has no `delete_if_match`. Best we can do is verify our
        // version with `head` first, then `delete`. Race window is tiny.
        if let Ok(head) = self.object_store.head(&self.config.path).await {
            if head.e_tag != version.e_tag {
                debug!(
                    "lease {} no longer ours at release time; leaving it",
                    self.config.path
                );
                return;
            }
        }
        if let Err(e) = self.object_store.delete(&self.config.path).await {
            warn!("failed to release lease {}: {}", self.config.path, e);
        } else {
            info!("lease {} released by {}", self.config.path, self.config.owner);
        }
    }

    fn record_acquired(&self, put: PutResult, expires_at_unix_ms: i64) {
        let mut inner = self.inner.lock();
        inner.held_version = Some(UpdateVersion::from(put));
        inner.expires_at_unix_ms = expires_at_unix_ms;
    }

    async fn acquire_non_atomic(
        &self,
        new_doc: LeaseDoc,
        payload: Vec<u8>,
        now_unix_ms: i64,
    ) -> Result<bool, LeaseError> {
        match self.object_store.get(&self.config.path).await {
            Ok(get) => {
                let bytes = get.bytes().await?;
                let existing: LeaseDoc = serde_json::from_slice(&bytes)?;
                if existing.expires_at_unix_ms > now_unix_ms && existing.owner != self.config.owner
                {
                    return Ok(false);
                }
            }
            Err(ObjStoreError::NotFound { .. }) => {}
            Err(e) => return Err(e.into()),
        }
        let put = self
            .object_store
            .put_opts(
                &self.config.path,
                Bytes::from(payload).into(),
                PutOptions::default(),
            )
            .await?;
        self.record_acquired(put, new_doc.expires_at_unix_ms);
        Ok(true)
    }
}

/// Spawn the background acquire-and-refresh loop. The returned task runs until
/// `shutdown` fires, at which point it releases the lease (if held) and exits.
pub fn run(
    lease: Arc<Lease>,
    time_provider: Arc<dyn iox_time::TimeProvider>,
    shutdown: influxdb3_shutdown::ShutdownToken,
    metrics: Option<LeaseMetrics>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let ttl = lease.config.ttl;
        let refresh = lease.config.refresh_interval;
        let standoff = ttl.checked_div(2).unwrap_or(Duration::from_secs(5));

        if let Some(m) = &metrics {
            // Reflect any leadership established before `run` was spawned
            // (e.g. the writer's synchronous startup try_acquire).
            m.set_leader(lease.is_leader(time_provider.now().timestamp_millis()));
        }

        loop {
            let now_ms = time_provider.now().timestamp_millis();
            let is_leader = lease.is_leader(now_ms);

            tokio::select! {
                biased;
                _ = shutdown.wait_for_shutdown() => {
                    lease.release().await;
                    if let Some(m) = &metrics {
                        m.record("release", "ok");
                        m.set_leader(false);
                    }
                    return;
                }
                _ = tokio::time::sleep(if is_leader { refresh } else { standoff }) => {}
            }

            let now_ms = time_provider.now().timestamp_millis();
            if lease.is_leader(now_ms) {
                let outcome = lease.refresh(now_ms).await;
                if let Some(m) = &metrics {
                    match &outcome {
                        Ok(true) => {
                            m.record("renew", "ok");
                            m.set_leader(true);
                        }
                        Ok(false) => {
                            m.record("renew", "lost");
                            m.set_leader(false);
                        }
                        Err(_) => m.record("renew", "error"),
                    }
                }
                match outcome {
                    Ok(true) => {}
                    Ok(false) => debug!("lease lost during refresh; will retry acquire"),
                    Err(e) => warn!("lease refresh error: {}", e),
                }
            } else {
                let outcome = lease.try_acquire(now_ms).await;
                if let Some(m) = &metrics {
                    match &outcome {
                        Ok(true) => {
                            m.record("acquire", "ok");
                            m.set_leader(true);
                        }
                        Ok(false) => {
                            m.record("acquire", "conflict");
                            m.set_leader(false);
                        }
                        Err(_) => m.record("acquire", "error"),
                    }
                }
                match outcome {
                    Ok(true) => {}
                    Ok(false) => debug!("lease still held by another process; standing by"),
                    Err(e) => warn!("lease acquire error: {}", e),
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use iox_time::{MockProvider, Time};
    use object_store::memory::InMemory;

    fn store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    #[tokio::test]
    async fn acquire_when_unheld() {
        let s = store();
        let lease = Lease::new(
            LeaseConfig::new(ObjPath::from("_locks/test.lease"), "node-a", Duration::from_secs(60)),
            Arc::clone(&s),
        );
        assert!(lease.try_acquire(1_000).await.unwrap());
        assert!(lease.is_leader(1_000));
    }

    #[tokio::test]
    async fn second_acquirer_rejected_while_active() {
        let s = store();
        let a = Lease::new(
            LeaseConfig::new(ObjPath::from("_locks/x.lease"), "node-a", Duration::from_secs(60)),
            Arc::clone(&s),
        );
        let b = Lease::new(
            LeaseConfig::new(ObjPath::from("_locks/x.lease"), "node-b", Duration::from_secs(60)),
            Arc::clone(&s),
        );
        assert!(a.try_acquire(1_000).await.unwrap());
        assert!(!b.try_acquire(2_000).await.unwrap(), "B should fail while A holds");
        assert!(a.is_leader(2_000));
        assert!(!b.is_leader(2_000));
    }

    #[tokio::test]
    async fn takeover_after_expiry() {
        let s = store();
        let a = Lease::new(
            LeaseConfig::new(ObjPath::from("_locks/y.lease"), "node-a", Duration::from_secs(60)),
            Arc::clone(&s),
        );
        let b = Lease::new(
            LeaseConfig::new(ObjPath::from("_locks/y.lease"), "node-b", Duration::from_secs(60)),
            Arc::clone(&s),
        );
        assert!(a.try_acquire(1_000).await.unwrap());
        // 120s later, A's 60s lease has expired.
        assert!(b.try_acquire(1_000 + 120_000).await.unwrap());
        assert!(!a.is_leader(1_000 + 120_000));
        assert!(b.is_leader(1_000 + 120_000));
    }

    #[tokio::test]
    async fn refresh_extends_expiry() {
        let s = store();
        let a = Lease::new(
            LeaseConfig::new(ObjPath::from("_locks/z.lease"), "node-a", Duration::from_secs(60)),
            Arc::clone(&s),
        );
        assert!(a.try_acquire(1_000).await.unwrap());
        assert!(a.refresh(30_000).await.unwrap());
        // Now expires at 30_000 + 60_000 = 90_000.
        assert!(a.is_leader(80_000));
        assert!(!a.is_leader(95_000));
    }

    #[tokio::test]
    async fn release_lets_peer_take_over_immediately() {
        let s = store();
        let a = Lease::new(
            LeaseConfig::new(ObjPath::from("_locks/r.lease"), "node-a", Duration::from_secs(60)),
            Arc::clone(&s),
        );
        let b = Lease::new(
            LeaseConfig::new(ObjPath::from("_locks/r.lease"), "node-b", Duration::from_secs(60)),
            Arc::clone(&s),
        );
        assert!(a.try_acquire(1_000).await.unwrap());
        a.release().await;
        assert!(b.try_acquire(2_000).await.unwrap());
    }

    // Confirm we don't grant leadership when wall clock has passed the expiry
    // even if our local in-memory state still says we hold the lease (e.g.
    // long-paused process).
    #[tokio::test]
    async fn is_leader_respects_wall_clock_expiry() {
        let s = store();
        let a = Lease::new(
            LeaseConfig::new(ObjPath::from("_locks/c.lease"), "node-a", Duration::from_secs(10)),
            Arc::clone(&s),
        );
        let _ = MockProvider::new(Time::from_timestamp_nanos(0));
        assert!(a.try_acquire(1_000).await.unwrap());
        assert!(a.is_leader(5_000));
        assert!(!a.is_leader(20_000));
    }
}
