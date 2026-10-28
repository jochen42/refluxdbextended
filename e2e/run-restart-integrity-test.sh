#!/usr/bin/env bash
#
# Restart integrity test: does a rolling restart onto a new image keep every
# row, while the compactor carries a large pile of compaction manifests?
#
# Flow:
#   0. Start minio + writer + querier + compactor, all on OLD_IMAGE.
#   1. Backfill rows into a closed past window (restart_check.py churn) until
#      the compactor has MANIFEST_TARGET manifests. Verify against ground truth.
#   2. Restart the compactor on OLD_IMAGE (startup-time baseline).
#   3. Roll compactor → querier → writer onto NEW_IMAGE; after each restart
#      write a burst, let compaction run, verify.
#   4. Restart the compactor on NEW_IMAGE once more, burst, verify.
#   5. Fault: corrupt one manifest, restart the compactor, burst, verify.
#      Today the compactor then starts without ANY manifest (warn only); this
#      phase is reported as an expected failure until that is fixed, and does
#      not fail the run.
#
# Verification compares the querier's per-table count, sum(v), min/max(time)
# and per-hour counts with what churn wrote, waits for the view to match and
# then requires it to stay matched for VERIFY_STABLE_SEC.
#
# Invoke from `e2e/`:
#
#     ./run-restart-integrity-test.sh
#
# Report: results/restart/report.md; logs in results/restart/logs/.

set -euo pipefail

cd "$(dirname "$0")"

: "${OLD_IMAGE:=docker.io/jochen42/refluxdbextended:extended-3.9.13-3}"
: "${NEW_IMAGE:=docker.io/jochen42/refluxdbextended:extended-3.9.13-4}"
: "${MANIFEST_TARGET:=1000}"
: "${CHURN_MAX_SEC:=2400}"
: "${CHURN_CHUNK_ROUNDS:=200}"
: "${BURST_ROUNDS:=200}"
: "${POST_BURST_SEC:=60}"
: "${VERIFY_WAIT_SEC:=180}"
: "${VERIFY_STABLE_SEC:=60}"
: "${STARTUP_MAX_SEC:=1800}"
: "${LOG_FILTER:=info}"
: "${BUCKET:=influxdb3-restart}"
: "${WRITER_PORT:=18181}"
: "${QUERIER_PORT:=18281}"
: "${COMPACTOR_PORT:=18381}"
: "${MINIO_PORT:=19000}"
: "${MINIO_CONSOLE_PORT:=19001}"

export LOG_FILTER BUCKET WRITER_PORT QUERIER_PORT COMPACTOR_PORT MINIO_PORT MINIO_CONSOLE_PORT
export COMPOSE_FILE=docker-compose.yml:docker-compose.restart.yml
export COMPOSE_PROJECT_NAME=influxdb3-restart
export WRITER_IMAGE="${OLD_IMAGE}" QUERIER_IMAGE="${OLD_IMAGE}" COMPACTOR_IMAGE="${OLD_IMAGE}"

OUT=results/restart
EXPECTED="${OUT}/expected.json"
REPORT="${OUT}/report.md"
WRITER_URL="http://localhost:${WRITER_PORT}"
QUERIER_URL="http://localhost:${QUERIER_PORT}"

log() { printf '\033[1;36m[%s]\033[0m %s\n' "$(date -u '+%H:%M:%S')" "$*"; }
err() { printf '\033[1;31m[%s]\033[0m %s\n' "$(date -u '+%H:%M:%S')" "$*" >&2; }

compose() { docker compose --profile compactor "$@"; }

cleanup() {
    log "capturing container logs"
    mkdir -p "${OUT}/logs"
    for svc in minio writer querier compactor; do
        compose logs --no-color --timestamps "${svc}" \
            >"${OUT}/logs/${svc}.log" 2>/dev/null || true
    done
    if [ -z "${KEEP_STACK:-}" ]; then
        log "tearing down stack"
        compose --profile tools down -v --remove-orphans >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

rm -rf "${OUT}"
mkdir -p "${OUT}"
{
    echo "# Restart integrity test"
    echo
    echo "- old image: \`${OLD_IMAGE}\`"
    echo "- new image: \`${NEW_IMAGE}\`"
    echo "- started: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    echo
    echo "| phase | compactor | querier | writer | manifests | compactor ready (s) | manifest load | verify |"
    echo "|---|---|---|---|---|---|---|---|"
} >"${REPORT}"

failures=0

# Run an mc command inside the minio network.
mc_sh() {
    compose run --rm -T --no-deps --entrypoint sh minio-setup -c \
        "mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null && $1"
}

manifest_count() {
    mc_sh "mc ls --recursive local/${BUCKET}/compactor-1/compactions/ | wc -l" | tr -dc '0-9'
}

short() { printf '%s' "${1##*:}"; }

churn() {
    python3 restart_check.py churn --expected "${EXPECTED}" \
        --writer-url "${WRITER_URL}" --rounds "$1"
}

# Seconds from compactor container start to "Starting compaction service",
# plus the -4 "Loaded compaction manifests" line when present.
compactor_startup() {
    local cid started ready deadline
    cid=$(compose ps -q compactor)
    deadline=$(( $(date +%s) + STARTUP_MAX_SEC ))
    until docker logs "${cid}" 2>&1 | grep -q "Starting compaction service"; do
        if [ "$(date +%s)" -ge "${deadline}" ]; then
            STARTUP_SECS="timeout"
            LOAD_LINE="-"
            return 1
        fi
        sleep 1
    done
    started=$(docker inspect -f '{{.State.StartedAt}}' "${cid}")
    ready=$(docker logs --timestamps "${cid}" 2>&1 \
        | grep -m1 "Starting compaction service" | cut -d' ' -f1)
    STARTUP_SECS=$(python3 restart_check.py elapsed "${started}" "${ready}")
    LOAD_LINE=$(docker logs "${cid}" 2>&1 \
        | grep -E "Loaded compaction manifests|failed to load compaction manifests" \
        | tail -1 | sed -E 's/\x1b\[[0-9;]*m//g' \
        | grep -oE '(manifests=[0-9]+ duration_ms=[0-9]+|failed to load compaction manifests.*)' \
        || true)
    LOAD_LINE=${LOAD_LINE:-"(no log line)"}
}

restart_role() {
    local role=$1 image=$2
    case "${role}" in
        writer) export WRITER_IMAGE="${image}" ;;
        querier) export QUERIER_IMAGE="${image}" ;;
        compactor) export COMPACTOR_IMAGE="${image}" ;;
    esac
    log "restarting ${role} on $(short "${image}")"
    compose up -d --no-deps --force-recreate "${role}" >/dev/null
    if [ "${role}" != "compactor" ]; then
        local port
        port=$([ "${role}" = writer ] && echo "${WRITER_PORT}" || echo "${QUERIER_PORT}")
        local deadline=$(( $(date +%s) + STARTUP_MAX_SEC ))
        # /health answers during startup; /ping on the probe too. Wait for
        # a real API answer instead.
        until curl -sf -o /dev/null "http://localhost:${port}/api/v3/configure/database?format=json"; do
            if [ "$(date +%s)" -ge "${deadline}" ]; then
                err "${role} not serving after ${STARTUP_MAX_SEC}s"
                return 1
            fi
            sleep 1
        done
    fi
}

# phase <name> <expect_fail:0|1>: burst, settle, verify, report row.
verify_phase() {
    local name=$1 expect_fail=$2 manifests outcome
    manifests=$(manifest_count)
    log "${name}: verifying (${manifests} manifests)"
    if python3 restart_check.py verify --expected "${EXPECTED}" \
        --querier-url "${QUERIER_URL}" \
        --wait-sec "${VERIFY_WAIT_SEC}" --stable-sec "${VERIFY_STABLE_SEC}" \
        2>&1 | tee "${OUT}/${name}.verify.log"; then
        if [ "${expect_fail}" = 1 ]; then
            outcome="pass (expected failure did not occur)"
        else
            outcome="pass"
        fi
    else
        if [ "${expect_fail}" = 1 ]; then
            outcome="FAIL (expected, open fix)"
        else
            outcome="**FAIL**"
            failures=$((failures + 1))
        fi
    fi
    echo "| ${name} | $(short "${COMPACTOR_IMAGE}") | $(short "${QUERIER_IMAGE}") | $(short "${WRITER_IMAGE}") | ${manifests} | ${STARTUP_SECS:--} | ${LOAD_LINE:--} | ${outcome} |" >>"${REPORT}"
    log "${name}: ${outcome}"
}

burst() {
    log "burst: ${BURST_ROUNDS} rounds, then ${POST_BURST_SEC}s of compaction"
    churn "${BURST_ROUNDS}"
    sleep "${POST_BURST_SEC}"
}

# --- Phase 0 ------------------------------------------------------------------
log "phase 0: starting stack on $(short "${OLD_IMAGE}")"
compose up -d --wait minio writer querier >/dev/null
compose up -d --no-deps compactor >/dev/null
compactor_startup || true

# --- Phase 1: pile up manifests -----------------------------------------------
log "phase 1: churning until ${MANIFEST_TARGET} compaction manifests (max ${CHURN_MAX_SEC}s)"
deadline=$(( $(date +%s) + CHURN_MAX_SEC ))
while :; do
    churn "${CHURN_CHUNK_ROUNDS}"
    count=$(manifest_count)
    log "manifests: ${count}"
    [ "${count}" -ge "${MANIFEST_TARGET}" ] && break
    if [ "$(date +%s)" -ge "${deadline}" ]; then
        err "only ${count} manifests after ${CHURN_MAX_SEC}s; continuing with that"
        break
    fi
done
sleep "${POST_BURST_SEC}"
STARTUP_SECS="-" LOAD_LINE="-"
verify_phase "1-seed" 0

# --- Phase 2: restart on old image (baseline) ---------------------------------
restart_role compactor "${OLD_IMAGE}"
compactor_startup || err "compactor (old) did not start compaction in time"
burst
verify_phase "2-compactor-restart-old" 0

# --- Phase 3: roll onto new image ---------------------------------------------
restart_role compactor "${NEW_IMAGE}"
compactor_startup || err "compactor (new) did not start compaction in time"
burst
verify_phase "3a-compactor-new" 0

STARTUP_SECS="-" LOAD_LINE="-"
restart_role querier "${NEW_IMAGE}"
burst
verify_phase "3b-querier-new" 0

restart_role writer "${NEW_IMAGE}"
burst
verify_phase "3c-writer-new" 0

# --- Phase 4: restart again on new image --------------------------------------
restart_role compactor "${NEW_IMAGE}"
compactor_startup || err "compactor (new) did not start compaction in time"
burst
verify_phase "4-compactor-restart-new" 0
curl -sf "http://localhost:${COMPACTOR_PORT}/metrics" \
    | grep -E '^influxdb3_(compaction_manifests_load_pending|startup_phase|server_ready)' \
    >"${OUT}/compactor-metrics.txt" || true

# --- Phase 5: corrupt one manifest --------------------------------------------
victim=$(mc_sh "mc ls --recursive local/${BUCKET}/compactor-1/compactions/" \
    | awk '{print $NF}' | sort | awk '{a[NR]=$0} END {print a[int(NR/2)]}')
log "phase 5: corrupting compactor-1/compactions/${victim}"
mc_sh "echo '{' | mc pipe local/${BUCKET}/compactor-1/compactions/${victim}" >/dev/null
restart_role compactor "${NEW_IMAGE}"
compactor_startup || err "compactor did not start compaction after corruption"
burst
verify_phase "5-corrupt-manifest" 1

{
    echo
    echo "- finished: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    echo "- corrupted manifest: \`${victim}\`"
    echo "- rows written: $(python3 -c "import json;print(json.load(open('${EXPECTED}'))['next_g'])")"
    echo "- compactor metrics after phase 4:"
    echo
    echo '```'
    cat "${OUT}/compactor-metrics.txt" 2>/dev/null || true
    echo '```'
} >>"${REPORT}"

cat "${REPORT}"
if [ "${failures}" -gt 0 ]; then
    err "${failures} phase(s) failed"
    exit 1
fi
log "done — all non-fault phases passed"
