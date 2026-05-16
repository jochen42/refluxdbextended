#!/usr/bin/env bash
#
# Full e2e benchmark: uncompacted baseline vs. post-compaction performance,
# same dataset, same querier process, same queries.
#
# Phases:
#   1. Bring up minio + writer + querier (no compactor).
#   2. Generate synthetic data.
#   3. Run benchmark suite → results/uncompacted.json
#   4. Start the compactor and wait for it to settle.
#   5. Re-run benchmark suite → results/compacted.json
#   6. Print a side-by-side comparison.
#
# Designed to be long-running. Sensible defaults match a "noticeable" workload
# (~1M points, gen1 = 5 min). Override via env to scale up or down.
#
# Expected to be invoked from `e2e/`:
#
#     ./run-bench.sh
#
# Image override:
#
#     INFLUXDB3_IMAGE=ghcr.io/metrico/influxdb3-unlocked:latest ./run-bench.sh

set -euo pipefail

cd "$(dirname "$0")"

: "${HOURS:=24}"
: "${HOSTS:=200}"
: "${POINTS_PER_HOST:=5000}"
# Smaller batches → more WAL files → more snapshots can fire during the
# settle window. Without enough WAL periods, the snapshot tracker won't
# emit a parquet at all (it needs >= snapshot_size + snapshot_size/2 periods).
: "${BATCH_SIZE:=1000}"
: "${RUNS_PER_QUERY:=5}"
: "${COMPACTION_SETTLE_MAX_SEC:=180}"
: "${COMPACTION_SETTLE_POLL_SEC:=10}"
: "${COMPACTION_SETTLE_STABLE_SEC:=30}"
: "${LOG_FILTER:=debug}"

export RUNS_PER_QUERY LOG_FILTER

log() { printf '\033[1;36m[%s]\033[0m %s\n' "$(date -u '+%H:%M:%S')" "$*"; }
err() { printf '\033[1;31m[%s]\033[0m %s\n' "$(date -u '+%H:%M:%S')" "$*" >&2; }

compose() { docker compose "$@"; }

cleanup() {
    # Dump every container's logs to results/ before we tear the stack down
    # — they live in the daemon and disappear with --remove-orphans.
    log "capturing container logs"
    mkdir -p results/logs
    for svc in minio writer querier compactor; do
        compose logs --no-color --timestamps "${svc}" \
            >"results/logs/${svc}.log" 2>/dev/null || true
    done
    log "tearing down stack"
    compose --profile compactor --profile tools down -v --remove-orphans || true
}
trap cleanup EXIT

# --- Phase 0: warm baseline ----------------------------------------------------
log "phase 0: starting writer + querier (no compactor)"
compose up -d --wait minio writer querier

# --- Phase 1: ingest -----------------------------------------------------------
log "phase 1: generating data (hours=${HOURS} hosts=${HOSTS} \
points-per-host=${POINTS_PER_HOST} batch-size=${BATCH_SIZE})"
compose run --rm gen \
    --hours "${HOURS}" \
    --hosts "${HOSTS}" \
    --points-per-host "${POINTS_PER_HOST}" \
    --batch-size "${BATCH_SIZE}"

# Writer's WAL flushes at gen1_duration cadence; the dataset only lands
# on object storage (and in the shared inventory) after at least one
# flush. Sleep accordingly. GEN1 defaults to 30s in this stack so the
# wait is short.
log "phase 1b: waiting for writer to flush WAL + publish inventory"
# Snapshot tracker fires only when the *last* WAL period crosses a gen1
# boundary. Since the last write's timestamp sits inside the current gen1
# bucket, we have to wait until a no-op WAL flush pushes the boundary
# past it. With gen1=1m a wait of ~120s gives that boundary plus a few
# extra flush intervals for the snapshot job to land in object store.
sleep "${INGEST_SETTLE_SEC:-120}"

# Querier loaded the catalog before the database existed. Force a reload
# so its catalog + inventory views see the writer's freshly persisted data.
log "phase 1c: restarting querier to pick up new catalog + inventory"
compose restart querier
compose up -d --wait querier

# --- Phase 2: uncompacted benchmark -------------------------------------------
log "phase 2: benchmarking uncompacted dataset"
compose run --rm -e RUNS_PER_QUERY="${RUNS_PER_QUERY}" bench --tag uncompacted

# Snapshot file count before compaction so the harness can show the delta
# alongside latency numbers.
uncompacted_files=$(compose exec -T minio mc \
    --config-dir /tmp/.mc ls --recursive "local/${BUCKET:-influxdb3-bench}" \
    2>/dev/null | wc -l || echo "unknown")
log "uncompacted parquet object count (best effort): ${uncompacted_files}"

# --- Phase 3: start compactor + wait for settle --------------------------------
log "phase 3: starting compactor; will poll for settle"
compose --profile compactor up -d --wait compactor

deadline=$(( $(date +%s) + COMPACTION_SETTLE_MAX_SEC ))
last_count=""
stable_for=0
while [ "$(date +%s)" -lt "${deadline}" ]; do
    # Count current parquet files. Falling counts mean compaction is still
    # working; two consecutive identical counts → settled.
    count=$(compose logs compactor 2>/dev/null \
        | grep -cE "Compaction completed: [0-9]+ files -> [0-9]+ files" || true)
    log "compactor cycles observed so far: ${count}"
    if [ "${count}" = "${last_count}" ] && [ "${count}" != "0" ]; then
        stable_for=$(( stable_for + COMPACTION_SETTLE_POLL_SEC ))
        if [ "${stable_for}" -ge "${COMPACTION_SETTLE_STABLE_SEC}" ]; then
            log "compactor appears settled (${stable_for}s without new cycles)"
            break
        fi
    else
        stable_for=0
    fi
    last_count="${count}"
    sleep "${COMPACTION_SETTLE_POLL_SEC}"
done

if [ "$(date +%s)" -ge "${deadline}" ]; then
    err "compactor did not settle within ${COMPACTION_SETTLE_MAX_SEC}s; \
proceeding anyway"
fi

# Force the querier to re-read the catalog + shared inventory so the
# compactor's new gen2/3 files and `removed_files` records are visible.
log "phase 3b: restarting querier to pick up compaction manifests"
compose restart querier
compose up -d --wait querier

# --- Phase 4: compacted benchmark ---------------------------------------------
log "phase 4: benchmarking compacted dataset"
compose run --rm -e RUNS_PER_QUERY="${RUNS_PER_QUERY}" bench --tag compacted

# --- Phase 5: comparison ------------------------------------------------------
log "phase 5: rendering comparison"
python3 - <<'PY'
import json, os, statistics
def load(tag):
    with open(f"results/{tag}.json") as fp:
        return json.load(fp)
unc = load("uncompacted")
com = load("compacted")
names = sorted(set(unc["results"]) | set(com["results"]))
print()
print(f"{'query':<22} {'uncompacted (s)':>18} {'compacted (s)':>16} "
      f"{'speedup':>10}")
print("-" * 70)
for n in names:
    u = unc["results"].get(n, {}).get("median_s")
    c = com["results"].get(n, {}).get("median_s")
    if u is None or c is None:
        continue
    speed = u / c if c > 0 else float("inf")
    print(f"{n:<22} {u:>18.3f} {c:>16.3f} {speed:>9.2f}x")
print()
print(f"runs per query: {unc['runs_per_query']}")
PY

log "done — full results in e2e/results/{uncompacted,compacted}.json"
