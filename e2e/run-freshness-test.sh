#!/usr/bin/env bash
#
# Sub-second read-your-writes validation for the querier-freshness stack
# (Layers A + C).
#
# Flow:
#   1. Bring up minio + writer + querier (no compactor).
#   2. Wait for both to be healthy.
#   3. Write a single LP row to writer.
#   4. Poll querier `SELECT COUNT(*) FROM sensors` in a tight loop.
#   5. Assert count >= 1 within FRESHNESS_DEADLINE_SEC (default 3s).
#
# Layer C (WAL tail) should make the row visible by replaying the writer's
# un-persisted WAL files, even after the writer becomes unreachable. Layer A
# picks up snapshots once the WAL flushes — used here as a sanity backstop.
#
# Invoke from `e2e/`:
#
#     ./run-freshness-test.sh
#
# Ports default to 8181/8281/9000; override with WRITER_PORT etc. to
# avoid conflicts on a busy host.

set -euo pipefail

cd "$(dirname "$0")"

: "${FRESHNESS_DEADLINE_SEC:=3}"
: "${POLL_INTERVAL_MS:=100}"
: "${LOG_FILTER:=info}"

export LOG_FILTER

log() { printf '\033[1;36m[%s]\033[0m %s\n' "$(date -u '+%H:%M:%S')" "$*"; }
err() { printf '\033[1;31m[%s]\033[0m %s\n' "$(date -u '+%H:%M:%S')" "$*" >&2; }

compose() { docker compose "$@"; }

cleanup() {
    log "capturing container logs"
    mkdir -p results/logs
    for svc in minio writer querier; do
        compose logs --no-color --timestamps "${svc}" \
            >"results/logs/${svc}.freshness.log" 2>/dev/null || true
    done
    log "tearing down stack"
    compose --profile tools down -v --remove-orphans || true
}
trap cleanup EXIT

# -- Phase 0: bring up writer + querier ----------------------------------------
log "phase 0: starting minio + writer + querier"
compose up -d --wait minio writer querier

writer_port="${WRITER_PORT:-8181}"
querier_port="${QUERIER_PORT:-8281}"

# -- Phase 1: write one row ---------------------------------------------------
log "phase 1: writing one LP row"
write_start_ns=$(python3 -c 'import time; print(time.time_ns())')
curl -sf -X POST "http://localhost:${writer_port}/api/v2/write?bucket=bench&precision=ns" \
    --data-binary "sensors,host=h1,region=us-east temp=42.0 ${write_start_ns}" \
    || { err "write failed"; exit 1; }

# -- Phase 2: poll querier for visibility -------------------------------------
log "phase 2: polling querier (deadline=${FRESHNESS_DEADLINE_SEC}s)"
poll_start=$(python3 -c 'import time; print(time.time())')
deadline_unix=$(python3 -c "import time; print(time.time() + ${FRESHNESS_DEADLINE_SEC})")
first_visible_s=""
sleep_s=$(awk "BEGIN { print ${POLL_INTERVAL_MS} / 1000 }")

while :; do
    now=$(python3 -c 'import time; print(time.time())')
    over=$(awk "BEGIN { print (${now} > ${deadline_unix}) }")
    if [ "${over}" = "1" ]; then
        break
    fi
    # Query returns CSV. Header + 1 data row when the count is 1.
    resp=$(curl -sf -G "http://localhost:${querier_port}/api/v3/query_sql" \
        --data-urlencode "db=bench" \
        --data-urlencode "q=SELECT COUNT(*) AS n FROM sensors" \
        --data-urlencode "format=csv" 2>/dev/null || true)
    # CSV looks like:  n\n1\n
    count=$(printf '%s\n' "${resp}" | awk -F, 'NR==2 {print $1}')
    if [ -n "${count}" ] && [ "${count}" -ge 1 ] 2>/dev/null; then
        first_visible_s=$(awk "BEGIN { printf \"%.3f\", ${now} - ${poll_start} }")
        log "row visible after ${first_visible_s}s"
        break
    fi
    sleep "${sleep_s}"
done

if [ -z "${first_visible_s}" ]; then
    err "row never became visible within ${FRESHNESS_DEADLINE_SEC}s"
    log "last querier response: ${resp}"
    exit 1
fi

# -- Phase 3: kill writer, verify already-visible data still queryable --------
log "phase 3: killing writer; querier should still see the row from cached state"
compose stop writer
sleep 1

resp=$(curl -sf -G "http://localhost:${querier_port}/api/v3/query_sql" \
    --data-urlencode "db=bench" \
    --data-urlencode "q=SELECT COUNT(*) AS n FROM sensors" \
    --data-urlencode "format=csv" 2>/dev/null || true)
count=$(printf '%s\n' "${resp}" | awk -F, 'NR==2 {print $1}')
if [ -n "${count}" ] && [ "${count}" -ge 1 ] 2>/dev/null; then
    log "post-writer-kill query still returns ${count} (resilience OK)"
else
    err "writer offline → querier returned ${resp}; freshness layers not "
    err "covering the gap"
    exit 1
fi

log "done — freshness OK in ${first_visible_s}s (deadline ${FRESHNESS_DEADLINE_SEC}s)"
