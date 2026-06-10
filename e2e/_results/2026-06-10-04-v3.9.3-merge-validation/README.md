# v3.9.3 merge validation run

Image: jochen42/refluxdbextended:latest (git 95f64ca154, upstream v3.9.3 merge)
Knobs: defaults + PARALLEL=4, QUERY_FILE_LIMIT=2000000 (v3.9.3 produces >100k gen1 files for this workload)

| query | uncompacted (s) | compacted (s) | speedup |
|---|---|---|---|
| full_scan | 13.112 | 0.016 | 804.98x |
| high_card_filter | 13.228 | 0.022 | 603.45x |
| hour_buckets | 13.402 | 0.020 | 681.22x |
| narrow_window | 13.135 | 0.021 | 626.18x |

Compaction settled in ~100s. Response bytes identical for full_scan and
high_card_filter; hour_buckets/narrow_window differ by 1-3 bytes (float
formatting from merge-order, same row counts).
