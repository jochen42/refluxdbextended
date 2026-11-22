#!/usr/bin/env bash
#
# In-place upgrade check: a bucket written by OLD_IMAGE (catalog v2) is taken
# over by NEW_IMAGE (catalog v3, one-way migration on first start).
#
# Flow:
#   1. OLD writer + compactor + querier, auth on. Create an admin token, two
#      databases, three scoped tokens; ingest 4h of data; let the compactor
#      build gen2 files. Record a fingerprint and the token matrix.
#   2. Stop everything (full stop, as the rollout requires).
#   3. NEW writer (runs the migration), then NEW compactor, then NEW querier.
#   4. Assert: row count and sum match what was ingested, tokens still valid with the same grants,
#      new writes visible, compactor keeps working, no ERROR/panic in logs.
#   5. Start the OLD querier against the migrated bucket and record what it
#      does (rollback-without-restore check; informational).
#
# Invoke from `e2e/`:
#
#     OLD_IMAGE=docker.io/jochen42/refluxdbextended:extended-3.9.13-5 \
#     NEW_IMAGE=influxdb3-unlocked:e2e-3.12 ./run-upgrade-test.sh
#
# Results land in results/upgrade/.

set -euo pipefail
cd "$(dirname "$0")"

: "${OLD_IMAGE:?set OLD_IMAGE}"
: "${NEW_IMAGE:?set NEW_IMAGE}"
: "${BUCKET:=influxdb3-upgrade}"
: "${LOG_FILTER:=info}"
: "${CHECKPOINT_TOKENS:=1}"
# Grants the NEW writer restores during the migration (--migration-token-permissions).
# Default: what the tokens were created with. Set it empty to see what the
# checkpoint alone leaves (the token matrix then differs on purpose).
MIGRATION_TOKEN_PERMISSIONS=${MIGRATION_TOKEN_PERMISSIONS-"ro=db:bench:read;roall=db:*:read;rw=db:bench:read,write"}
export MIGRATION_TOKEN_PERMISSIONS
export BUCKET LOG_FILTER
export COMPOSE_FILE=docker-compose.yml:docker-compose.upgrade.yml
export COMPOSE_PROJECT_NAME=e2e-upgrade

W="http://localhost:${WRITER_PORT:-8181}"
Q="http://localhost:${QUERIER_PORT:-8281}"
OUT=results/upgrade
mkdir -p "$OUT"
: >"$OUT/report.txt"

log()  { printf '\033[1;36m[%s]\033[0m %s\n' "$(date -u '+%H:%M:%S')" "$*" | tee -a "$OUT/report.txt"; }
fail=0
check() { # name expected actual
    if [ "$2" = "$3" ]; then log "PASS $1 ($3)"; else log "FAIL $1: expected $2, got $3"; fail=1; fi
}

compose() { docker compose "$@"; }
use_images() { export WRITER_IMAGE=$1 QUERIER_IMAGE=$1 COMPACTOR_IMAGE=$1; }

dump_logs() { # suffix
    for svc in writer querier compactor; do
        compose logs --no-color --timestamps "$svc" >"$OUT/$svc.$1.log" 2>/dev/null || true
    done
}

cleanup() {
    dump_logs final
    if [ "${KEEP_STACK:-0}" = 1 ]; then log "KEEP_STACK=1: stack left running (project $COMPOSE_PROJECT_NAME)"; return; fi
    compose --profile compactor --profile tools down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_http() { # url
    for _ in $(seq 1 120); do curl -sf "$1/health" >/dev/null 2>&1 && return 0; sleep 1; done
    return 1
}

# HTTP status helpers ---------------------------------------------------------
q_status() { # base token db
    curl -s -o /dev/null -w '%{http_code}' -G "$1/api/v3/query_sql" \
        -H "Authorization: Bearer $2" --data-urlencode "db=$3" \
        --data-urlencode "q=SELECT 1" --data-urlencode format=json
}
w3_status() { # base token db
    curl -s -o /dev/null -w '%{http_code}' -X POST "$1/api/v3/write_lp?db=$3&precision=nanosecond" \
        -H "Authorization: Bearer $2" --data-binary "probe,host=p v=1i $(date +%s)000000000"
}
w2_status() {
    curl -s -o /dev/null -w '%{http_code}' -X POST "$1/api/v2/write?bucket=$3&precision=ns" \
        -H "Authorization: Bearer $2" --data-binary "probe,host=p v=1i $(date +%s)000000000"
}
w1_status() {
    curl -s -o /dev/null -w '%{http_code}' -X POST "$1/write?db=$3&precision=ns" \
        -H "Authorization: Bearer $2" --data-binary "probe,host=p v=1i $(date +%s)000000000"
}
cfg_status() { # base token  (admin-only endpoint)
    curl -s -o /dev/null -w '%{http_code}' -X POST "$1/api/v3/configure/database" \
        -H "Authorization: Bearer $2" -H 'Content-Type: application/json' \
        -d "{\"db\":\"cfgprobe$RANDOM\"}"
}

# Matrix of (token, check) -> status, written as "name=status" lines.
token_matrix() { # writer_base querier_base
    local w=$1 q=$2
    {
        echo "ro.query.bench=$(q_status "$q" "$RO" bench)"
        echo "ro.query.other=$(q_status "$q" "$RO" other)"
        echo "ro.write3.bench=$(w3_status "$w" "$RO" bench)"
        echo "ro.write1.bench=$(w1_status "$w" "$RO" bench)"
        echo "ro.cfg=$(cfg_status "$w" "$RO")"
        echo "roall.query.bench=$(q_status "$q" "$ROALL" bench)"
        echo "roall.query.other=$(q_status "$q" "$ROALL" other)"
        echo "roall.write3.other=$(w3_status "$w" "$ROALL" other)"
        echo "rw.query.bench=$(q_status "$q" "$RW" bench)"
        echo "rw.write3.bench=$(w3_status "$w" "$RW" bench)"
        echo "rw.write3.other=$(w3_status "$w" "$RW" other)"
        echo "rw.write3.newdb=$(w3_status "$w" "$RW" newdb)"
        echo "admin.query.bench=$(q_status "$q" "$ADMIN" bench)"
        echo "admin.cfg=$(cfg_status "$w" "$ADMIN")"
        echo "bogus.query.bench=$(q_status "$q" apiv3_bogus bench)"
    }
}

fingerprint() { # querier_base   -> one line per host: host,count,sum
    curl -sf -G "$1/api/v3/query_sql" -H "Authorization: Bearer $ADMIN" \
        --data-urlencode db=bench --data-urlencode format=csv \
        --data-urlencode "q=SELECT host, COUNT(*) AS n, ROUND(SUM(temp), 3) AS s FROM sensors GROUP BY host ORDER BY host"
}

bucket_listing() { # file
    compose run --rm -T --entrypoint sh minio-setup -c \
        "mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null && mc ls -r local/${BUCKET}" >"$1" 2>&1 || true
}

create_token() { # name perm...
    local name=$1; shift
    local args=()
    for p in "$@"; do args+=(--permissions "$p"); done
    compose exec -T writer influxdb3 create token --name "$name" "${args[@]}" \
        --host http://localhost:8181 --token "$ADMIN" 2>&1 | grep -oE 'apiv3_[A-Za-z0-9_-]+' | head -1
}

# -- Phase 1: old build --------------------------------------------------------
log "phase 1: OLD=$OLD_IMAGE"
use_images "$OLD_IMAGE"
compose up -d --wait minio writer querier
compose --profile compactor up -d compactor
wait_http "$W"; wait_http "$Q"

ADMIN=$(compose exec -T writer influxdb3 create token --admin --host http://localhost:8181 2>&1 \
    | grep -oE 'apiv3_[A-Za-z0-9_-]+' | head -1)
[ -n "$ADMIN" ] || { log "FAIL could not create admin token"; exit 1; }
for db in bench other; do
    curl -sf -X POST "$W/api/v3/configure/database" -H "Authorization: Bearer $ADMIN" \
        -H 'Content-Type: application/json' -d "{\"db\":\"$db\"}" >/dev/null
done
RO=$(create_token ro db:bench:read)
ROALL=$(create_token roall 'db:*:read')
RW=$(create_token rw db:bench:read,write)
for t in RO ROALL RW; do [ -n "${!t}" ] || { log "FAIL could not create token $t"; exit 1; }; done
log "tokens created (admin, ro, roall, rw)"
printf 'ADMIN=%s\nRO=%s\nROALL=%s\nRW=%s\n' "$ADMIN" "$RO" "$ROALL" "$RW" >"$OUT/tokens.env"

# Prod condition: the 3.9 v2 checkpoint (every 100 catalog changes) stores
# every scoped token's actions as "all". Force one by creating 110 tables one
# write at a time, so the tokens above go through a checkpoint like prod's.
if [ "$CHECKPOINT_TOKENS" = 1 ]; then
    for i in $(seq 1 110); do
        curl -sf -X POST "$W/api/v3/write_lp?db=other" -H "Authorization: Bearer $ADMIN" \
            --data-binary "pad_$i v=1i" >/dev/null || { log "FAIL padding write $i"; exit 1; }
    done
    log "110 catalog changes written to force a v2 checkpoint"
fi

# 5 hosts × 4h at 10s spacing = 7200 rows, ending 10 minutes ago, written in
# 24 batches (one per 10 minutes of data) so the writer persists many gen1 files.
python3 - "$W" "$ADMIN" "$OUT/fingerprint.expected.csv" <<'PY'
import sys, time, urllib.request
base, tok, expected = sys.argv[1], sys.argv[2], sys.argv[3]
end = (int(time.time()) - 600) // 10 * 10
start = end - 4 * 3600
ts = list(range(start, end, 10))
for i in range(0, len(ts), 60):
    lines = [f"sensors,host=h{h},region=r{h%2} temp={(t % 997) / 10 + h} {t}000000000"
             for t in ts[i:i+60] for h in range(5)]
    for db in ("bench", "other"):
        req = urllib.request.Request(f"{base}/api/v3/write_lp?db={db}&precision=nanosecond",
                                     data="\n".join(lines).encode(), method="POST",
                                     headers={"Authorization": f"Bearer {tok}"})
        urllib.request.urlopen(req).read()
    time.sleep(1.5)
with open(expected, "w") as f:  # ground truth for the fingerprint checks
    f.write("host,n,s\n")
    for h in range(5):
        f.write(f"h{h},{len(ts)},{round(sum((t % 997) / 10 + h for t in ts), 3)}\n")
print("ingested", len(ts) * 5, "rows per db")
PY

log "waiting for the old compactor to produce gen2+ files (max 300s)"
for _ in $(seq 1 60); do
    bucket_listing "$OUT/bucket.old.txt"
    grep -qE '/(gen[2-9]|g[2-9])[/-]|compact' "$OUT/bucket.old.txt" && break
    sleep 5
done
sleep 20 # let the querier fold the latest inventory
token_matrix "$W" "$Q" >"$OUT/tokens.old.txt"
fingerprint "$Q" >"$OUT/fingerprint.old.csv"
log "old fingerprint: $(tr '\n' ' ' <"$OUT/fingerprint.old.csv")"
bucket_listing "$OUT/bucket.old.txt"
dump_logs old

# -- Phase 2: full stop --------------------------------------------------------
log "phase 2: full stop"
compose stop compactor querier writer

# -- Phase 3: new build, writer first -----------------------------------------
log "phase 3: NEW=$NEW_IMAGE (writer → compactor → querier)"
log "migration token permissions: ${MIGRATION_TOKEN_PERMISSIONS:-<none>}"
use_images "$NEW_IMAGE"
t0=$(date +%s)
compose up -d --no-deps --wait writer
log "new writer healthy after $(( $(date +%s) - t0 ))s"
compose --profile compactor up -d --no-deps compactor
compose up -d --no-deps --wait querier
wait_http "$Q"
bucket_listing "$OUT/bucket.new.txt"
sleep 20

fingerprint "$Q" >"$OUT/fingerprint.new.csv"
log "new fingerprint: $(tr '\n' ' ' <"$OUT/fingerprint.new.csv")"
# Compare against what was ingested, not against the old querier: 3.9 queriers
# can double-count WAL-tail rows (fixed in 930df03263), so the old reading is
# not a reliable baseline. Old vs new is reported for information.
fp_matches() { # file  -> "match" or the first differing host
    python3 - "$OUT/fingerprint.expected.csv" "$1" <<'PY'
import csv, sys
exp = {r["host"]: r for r in csv.DictReader(open(sys.argv[1]))}
got = {r["host"]: r for r in csv.DictReader(open(sys.argv[2]))}
bad = [h for h in exp if h not in got or int(got[h]["n"]) != int(exp[h]["n"])
       or abs(float(got[h]["s"]) - float(exp[h]["s"])) > 0.01]
print("match" if not bad and exp.keys() == got.keys() else f"differs at {bad or sorted(got.keys() ^ exp.keys())}")
PY
}
log "old querier vs ingested: $(fp_matches "$OUT/fingerprint.old.csv") (informational)"
check "new fingerprint matches ingested data" "match" "$(fp_matches "$OUT/fingerprint.new.csv")"
diag() { # tag: duplicated timestamps + plan, to tell WAL-tail overlap from parquet overlap
    local d="$OUT/diag.$1"
    sql() { curl -s -G "$Q/api/v3/query_sql" -H "Authorization: Bearer $ADMIN" --data-urlencode db=bench \
        --data-urlencode format=pretty --data-urlencode "q=$1"; }
    { date -u; sql "SELECT host, COUNT(*) n FROM sensors GROUP BY host ORDER BY host"
      sql "SELECT MIN(time) mn, MAX(time) mx, COUNT(*) dups FROM (SELECT time FROM sensors WHERE host='h0' GROUP BY time HAVING COUNT(*)>1)"
      sql "EXPLAIN ANALYZE SELECT host, COUNT(*) FROM sensors GROUP BY host"; } >"$d.txt" 2>&1
}
for i in 0 1 2 3 4 5; do diag "t$((i*10))s"; sleep 10; done
log "diag: dup counts over time: $(grep -h -A3 'dups' "$OUT"/diag.t*s.txt | grep -oE '\| [0-9]+ +\|$' | tr -d '| ' | tr '\n' ' ')"

token_matrix "$W" "$Q" >"$OUT/tokens.new.txt"
log "token matrix old → new:"
paste -d' ' "$OUT/tokens.old.txt" "$OUT/tokens.new.txt" | awk '{split($1,a,"=");split($2,b,"="); printf "  %-22s %s → %s%s\n", a[1], a[2], b[2], (a[2]==b[2]?"":"  (changed)")}' | tee -a "$OUT/report.txt"
check "token matrix unchanged (15 cases)" "$(md5 -q "$OUT/tokens.old.txt")" "$(md5 -q "$OUT/tokens.new.txt")"

# Freshness on the new stack.
ts=$(date +%s)000000000
curl -sf -X POST "$W/api/v3/write_lp?db=bench&precision=nanosecond" -H "Authorization: Bearer $RW" \
    --data-binary "sensors,host=fresh,region=r0 temp=1 $ts"
t0=$(python3 -c 'import time; print(time.time())'); vis=""
for _ in $(seq 1 50); do
    n=$(curl -sf -G "$Q/api/v3/query_sql" -H "Authorization: Bearer $RO" --data-urlencode db=bench \
        --data-urlencode format=csv --data-urlencode "q=SELECT COUNT(*) FROM sensors WHERE host='fresh'" | sed -n 2p)
    if [ "${n:-0}" -ge 1 ] 2>/dev/null; then vis=$(python3 -c "import time; print(f'{time.time()-$t0:.2f}')"); break; fi
    sleep 0.1
done
check "new row visible via RO token" yes "$([ -n "$vis" ] && echo yes || echo no)"
log "visible after ${vis:-never}s"

log "letting the new compactor run 60s"
sleep 60
dump_logs new
grep -hiE 'generation|gen[0-9]_duration|ladder' "$OUT/compactor.new.log" | head -5 | tee -a "$OUT/report.txt" || true
# The token matrix provokes authz/authn errors on purpose; ignore those.
errs=$(grep -hE ' ERROR |panicked' "$OUT/writer.new.log" "$OUT/querier.new.log" "$OUT/compactor.new.log" \
    | grep -vE 'Authorization error|cannot authenticate token|request was not authenticated' | tee "$OUT/errors.new.txt" | wc -l | tr -d ' ' || true)
check "no ERROR/panic lines in new logs" 0 "$errs"
grep -hiE 'migrat' "$OUT/writer.new.log" | head -5 | tee -a "$OUT/report.txt" || true

# -- Phase 4: old querier against the migrated bucket (informational) ---------
if [ "${SKIP_PHASE4:-0}" = 1 ]; then [ "$fail" = 0 ] && log "UPGRADE TEST PASSED" || log "UPGRADE TEST FAILED"; exit "$fail"; fi
log "phase 4: OLD querier against the migrated bucket"
compose stop querier
export QUERIER_IMAGE=$OLD_IMAGE
compose up -d --no-deps querier
ok=no
for _ in $(seq 1 60); do curl -sf "$Q/health" >/dev/null 2>&1 && { ok=yes; break; }; sleep 1; done
log "old querier healthy on migrated bucket: $ok"
if [ "$ok" = yes ]; then
    log "old querier sees fresh row: $(curl -s -G "$Q/api/v3/query_sql" -H "Authorization: Bearer $ADMIN" \
        --data-urlencode db=bench --data-urlencode format=csv \
        --data-urlencode "q=SELECT COUNT(*) FROM sensors WHERE host='fresh'" | tr '\n' ' ')"
fi
compose logs --no-color --timestamps querier >"$OUT/querier.old-on-v3.log" 2>/dev/null || true
tail -5 "$OUT/querier.old-on-v3.log" | tee -a "$OUT/report.txt"

[ "$fail" = 0 ] && log "UPGRADE TEST PASSED" || log "UPGRADE TEST FAILED"
exit "$fail"
