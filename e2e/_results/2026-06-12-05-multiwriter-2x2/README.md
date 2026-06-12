# Multi-writer 2x2 bench (experiment/multi-writer)

Image: influxdb3-unlocked:e2e (git 51af1da0e0, experiment/multi-writer)
Topology: 2 writers (--multi-writer, spray ingest) + 2 queriers
(--writers node-id=url, runs alternated) + 1 compactor.
Knobs: HOURS=12 HOSTS=100 (500k points) PARALLEL=4 QUERY_FILE_LIMIT=2000000,
querier memory capped (2 GiB exec pool, 1 GiB parquet cache, 3.5 GiB cgroup).

| query | uncompacted (s) | compacted (s) | speedup |
|---|---|---|---|
| full_scan | 6.212 | 0.122 | 50.82x |
| high_card_filter | 7.085 | 0.117 | 60.38x |
| hour_buckets | 7.053 | 0.117 | 60.16x |
| narrow_window | 6.711 | 0.109 | 61.75x |

Correctness: full_scan and high_card_filter responses byte-identical
between phases (COUNT(*) included → no duplicate or lost rows across the
two writers, through compaction). hour_buckets/narrow_window differ by
1-2 bytes — float formatting from merge order, same as the v3.9.3
validation run. 91 compaction cycles; jobs merged inputs from both
writer prefixes (e.g. 536 files -> 1).

Workload was halved vs. the single-writer validation run (which used
24h/200 hosts): two writers double the gen1 file count (~290k files) and
two queriers scanning that concurrently exceeds a ~12 GiB docker VM —
the full-size run OOM-killed a querier. Numbers here are NOT directly
comparable to 2026-06-10-04; the uncompacted-vs-compacted ratio is the
signal.
