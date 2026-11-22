#!/usr/bin/env bash
# Same uncompacted dataset, two querier builds. Ingest with PROD writer (v2
# catalog), profile PROD querier; then NEW writer (migrates) + NEW querier.
set -uo pipefail
cd /Users/jochen.weber/_src/github.com/refluxdb/refluxdb/e2e
OUT=/private/tmp/claude-501/-Users-jochen-weber--src-github-com-refluxdb-refluxdb/cf08ed6a-ee08-4dac-b0d8-6727ccd3c261/scratchpad/regress
mkdir -p "$OUT"
PROD=docker.io/jochen42/refluxdbextended:extended-3.9.13-5
NEW=influxdb3-unlocked:e2e-3.12
PROD_E2E=influxdb3-unlocked:e2e-3.9.13-5
export INFLUXDB3_IMAGE=$NEW COMPACTOR_IMAGE=$NEW
export COMPOSE_FILE=docker-compose.yml:docker-compose.mixed.yml
export COMPOSE_PROJECT_NAME=e2e-regress BUCKET=influxdb3-regress QUERY_FILE_LIMIT=2000000
Q='SELECT region, AVG(temp) AS avg_temp, MAX(humidity) AS max_humid, COUNT(*) AS n FROM sensors GROUP BY region'
q() { curl -s -o "$2" -w '%{time_total}' --get http://localhost:8281/api/v3/query_sql \
        --data-urlencode db=bench --data-urlencode "q=$1" --data-urlencode format=json; }

profile() { # $1 tag
    local d="$OUT/$1"; mkdir -p "$d"
    for i in 1 2 3; do echo "query run$i $(q "$Q" "$d/result.json")s"; done | tee "$d/times.txt"
    for i in 1 2 3; do echo "explain(plan only) run$i $(q "EXPLAIN $Q" "$d/explain.json")s"; done | tee -a "$d/times.txt"
    echo "explain analyze $(q "EXPLAIN ANALYZE $Q" "$d/analyze.json")s" | tee -a "$d/times.txt"
    q "EXPLAIN ANALYZE VERBOSE $Q" "$d/analyze_verbose.json" >/dev/null
    docker compose logs --no-color querier >"$d/querier.log" 2>&1
    docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' | grep querier | tee -a "$d/times.txt"
}

echo "== up prod $(date -u +%T)"
export WRITER_IMAGE=$PROD QUERIER_IMAGE=$PROD
docker compose up -d --wait minio writer querier
docker compose run --rm --no-deps gen --hours 24 --hosts 200 --points-per-host 5000 --batch-size 1000 --parallel 4
sleep 120
docker compose exec -T minio sh -c 'find /data -name "*.parquet" | wc -l' | sed 's/^/parquet files: /'
profile prod-lto
grep -q '"n"' "$OUT/prod-lto/result.json" || { echo "no data, abort"; exit 1; }

echo "== prod source, e2e build flags $(date -u +%T)"
docker compose stop querier
export QUERIER_IMAGE=$PROD_E2E
docker compose up -d --wait --no-deps querier
sleep 30
profile prod-e2e

echo "== switch to new $(date -u +%T)"
docker compose stop querier writer
export WRITER_IMAGE=$NEW QUERIER_IMAGE=$NEW
docker compose up -d --wait --no-deps writer
docker compose up -d --wait --no-deps querier
sleep 30
profile new

# Same v3 bucket, new querier, cold→warm again to be sure ordering isn't the cause.
docker compose restart querier; sleep 30
profile new-restart
[ -n "${KEEP_STACK:-}" ] || docker compose --profile compactor --profile tools down -v --remove-orphans
echo "== done $(date -u +%T)"
