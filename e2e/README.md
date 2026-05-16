# End-to-end Compaction Benchmark

Docker-based benchmark that measures query latency against an
**uncompacted** dataset, runs the compactor, then re-measures the **same
queries** against the **same data** to quantify the speedup from
multi-level compaction.

The stack mirrors the multi-node deployment documented in the top-level
README:

```
MinIO  ──┬── writer    (--mode writer, ingest only)
         ├── compactor (--mode compactor, started in phase 3 only)
         └── querier   (--mode querier, read-only)
```

Everything shares one MinIO bucket; queries hit the querier exclusively so
the comparison is apples-to-apples.

## Prerequisites

- Docker + Docker Compose v2
- Python 3 on the host (only used to render the final comparison table)
- An `influxdb3-unlocked` image. Either:
  - Pull a published build:
    ```bash
    export INFLUXDB3_IMAGE=ghcr.io/metrico/influxdb3-unlocked:latest
    ```
  - Or build locally from the repo root:
    ```bash
    docker build -t influxdb3-unlocked:e2e .
    ```

## One command

```bash
cd e2e/
./run-bench.sh
```

Expect a long runtime. Defaults generate ~1M points across 200 hosts spanning
a 24-hour window, then run four queries five times each before and after
compaction. Plan for 15–60 minutes depending on hardware.

## Phases

| # | Phase | What happens |
|---|-------|--------------|
| 0 | Up | Start MinIO + bucket + writer + querier (compactor stays down) |
| 1 | Ingest | `gen` container writes synthetic line protocol via the writer |
| 2 | Bench uncompacted | `bench` runs every `queries/*.sql` against the querier; results in `results/uncompacted.json` |
| 3 | Compact | Start compactor; harness polls compactor logs until cycle count stops increasing for 60s (or `COMPACTION_SETTLE_MAX_SEC` elapses) |
| 4 | Bench compacted | Same queries, same querier; results in `results/compacted.json` |
| 5 | Report | Print median latency comparison + speedup factor |

## Knobs

All optional. Defaults match a "noticeable" workload.

| Env var | Default | Effect |
|---------|---------|--------|
| `INFLUXDB3_IMAGE` | `influxdb3-unlocked:e2e` | Image to run |
| `BUCKET` | `influxdb3-bench` | MinIO bucket name |
| `HOURS` | `24` | Time window covered by generated data |
| `HOSTS` | `200` | Distinct `host` tag values |
| `POINTS_PER_HOST` | `5000` | Points per host (total = HOSTS × this) |
| `BATCH_SIZE` | `5000` | LP lines per write request |
| `RUNS_PER_QUERY` | `5` | Repeats per query (median used in report) |
| `GEN1` | `5m` | Writer's gen1 duration |
| `GEN2` | `1h` | Compactor's gen2 target |
| `GEN3` | `1d` | Compactor's gen3 target |
| `COMPACTION_INTERVAL` | `30s` | How often the compactor checks for jobs |
| `MIN_FILES` | `2` | Min files before triggering a compaction |
| `COMPACTION_SETTLE_MAX_SEC` | `900` | Phase 3 timeout |
| `LOG_FILTER` | `info` | Server log level |
| `MINIO_PORT` | `9000` | Host MinIO S3 port |
| `WRITER_PORT` | `8181` | Host writer HTTP port |
| `QUERIER_PORT` | `8281` | Host querier HTTP port |

Scale up for stress:

```bash
HOURS=168 HOSTS=1000 POINTS_PER_HOST=20000 ./run-bench.sh
```

## Queries

Drop-in: add any `.sql` file in `queries/`. The runner picks them up
automatically. Shipped suite:

- `full_scan.sql` — region aggregation across the entire time range. Hits
  every parquet file; greatest expected speedup post-compaction.
- `hour_buckets.sql` — hourly downsampling with `APPROX_PERCENTILE_CONT`
  over a 24-hour window. Many files, heavy aggregation.
- `high_card_filter.sql` — per-host extremes over a 12-hour window with
  `LIMIT 50`. Tests per-file pruning.
- `narrow_window.sql` — last 15 minutes. Control query: post-compaction
  the recent window is still mostly gen1 so this should be roughly
  unchanged. If it moves a lot, the benchmark setup is off.

## Output

Each phase produces a JSON file:

```jsonc
// results/uncompacted.json
{
  "tag": "uncompacted",
  "runs_per_query": 5,
  "results": {
    "full_scan": {
      "sql": "...",
      "times_s": [0.41, 0.39, 0.43, 0.38, 0.40],
      "min_s": 0.38, "median_s": 0.40, "max_s": 0.43, "mean_s": 0.402,
      "response_bytes": 410
    }
  }
}
```

The final stdout table:

```
query                  uncompacted (s)    compacted (s)    speedup
----------------------------------------------------------------------
full_scan                       2.150            0.380       5.66x
high_card_filter                0.910            0.150       6.07x
hour_buckets                    1.870            0.420       4.45x
narrow_window                   0.130            0.110       1.18x
```

(Numbers are illustrative; your hardware will produce different absolute
values but speedup ratios on heavy queries should be similar.)

## Caveats

- **First-run JIT/cache effects.** Each query is warmed up once before
  timing starts. If you observe the first measured run consistently slower
  than subsequent ones, raise `RUNS_PER_QUERY`.
- **Compactor settle heuristic is log-based.** It watches the compactor's
  stdout for "Compaction completed: N files -> M files" lines. If the
  cluster is still doing background work the harness can't see, you may
  want to increase `COMPACTION_SETTLE_MAX_SEC` or sleep manually.
- **MinIO ≠ real S3.** Local MinIO is more permissive about request
  ordering and consistency than AWS S3. Real-world numbers will differ.
- **delete-grace is forced to 0s** in this stack so the post-compaction
  query view is deterministic. Production deployments should leave the
  10-minute default to protect remote queriers from 404s.
- **Generated data is dense and uniform.** Real workloads have bursty
  patterns and cardinality cliffs that change compaction value
  dramatically. Treat this benchmark as a relative comparison, not an
  absolute capacity-planning tool.

## Tear down

`run-bench.sh` registers a cleanup trap so a Ctrl-C or successful exit
tears the stack down. If something wedges, force it:

```bash
docker compose --profile compactor --profile tools down -v --remove-orphans
```
