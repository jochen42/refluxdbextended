# v3.9.13 merge validation run (2026-09-05)

Branch `jochen42/merge-upstream-v3.9.13` at `dd57c85122` (+ Dockerfile.e2e jemalloc change),
i.e. upstream v3.9.13 merged onto extended-3.9.3-34, plus the NotFoundTolerantSource
optimizer-visibility fix (`23c4aa03e2`). Images built locally with `e2e/Dockerfile.e2e`
(arm64, Docker Desktop VM: 8 CPUs, 11.67 GiB). Reference: `docker.io/jochen42/refluxdbextended:3.9.3`
(= extended-3.9.3-34, the current prod build, git 8df2b8a).
Knobs: defaults + `PARALLEL=4 QUERY_FILE_LIMIT=2000000` (same as the v3.9.3 validation).

## Compaction benchmark (1M points, 200 hosts, 24h; 144,075 gen1 files uncompacted)

| query | prod 3.9.3 uncompacted | prod 3.9.3 compacted | speedup | **new 3.9.13 uncompacted** | **new 3.9.13 compacted** | speedup |
|---|---|---|---|---|---|---|
| full_scan        | 20.815 s | 0.049 s | 422× | **18.583 s** | **0.024 s** | 787× |
| high_card_filter | 29.782 s | 0.049 s | 605× | **19.247 s** | **0.023 s** | 833× |
| hour_buckets     | 29.387 s | 0.052 s | 562× | **19.343 s** | **0.021 s** | 911× |
| narrow_window    | 28.937 s | 0.046 s | 627× | **19.098 s** | **0.022 s** | 872× |

Medians of 5 runs; per-run numbers in `*/bench.log`. Compaction settled in ~100 s (prod) /
~108 s (new); both reached 290 compactor cycles. Querier RSS during the uncompacted phase
(`*/docker-stats.txt`): prod 2.5–4.1 GiB, new 4.0–4.5 GiB, both flat across queries.
`full_scan` plan shape is identical on both builds (`*/explain-full_scan.summary.txt`:
8 file groups → SortExec → SortPreservingMergeExec → DeduplicateExec); planning takes ~1 s
longer on the new build because the re-enabled optimizer passes now walk all 144k chunks.
The compacted queries are ~2× faster on the new build — that is where predicate pushdown,
the IOx cached loader and SplitDedup apply again (see "Findings").

## Freshness / mixed-version (rolling upgrade) runs — `freshness/`

| writer | querier | row visible after | post-writer-kill read |
|---|---|---|---|
| new 3.9.13 | new 3.9.13 | 0.640 s | OK |
| new 3.9.13 | prod 3.9.3 | 0.629 s | OK |
| prod 3.9.3 | new 3.9.13 | 0.613 s | OK |

Container logs confirm the mixed roles ran different builds (`git_hash=cad5416` new vs
`8df2b8a` prod). Covers WAL-tail replay of the other build's WAL files (3.9.13 adds a
per-file nonce as object metadata) and inventory folding across builds. Not covered here:
the `{walseq}-{n}.parquet` split-chunk suffix (needs a >2 GiB string column) — unit-tested
in `parse_gen1_path` instead. Rollout order within an environment: queriers → compactor →
writer, so readers understand new file names before a writer can produce them.

## Test suites

- `nextest-baseline-main.summary.txt`: `main` (8df2b8a + housekeeping) — 3193 run, **28 failed**
  (26 `iox_query::physical_optimizer::*` + 2 server dedup snapshots; see Findings).
- `nextest-merged-branch.summary.txt`: merge branch — **3249 run, 3249 passed**, 17 skipped.

## Findings

1. **Optimizer passes were blind to parquet scans since June** (`new-3.9.13-glibc/querier.log.excerpt`
   shows the passes active again). `NotFoundTolerantSource` (38b0cd556e) wraps every
   `ParquetSource`; `chunk_extraction`, `cached_parquet_data` and `predicate_pushdown` downcast
   `file_source()` to `ParquetSource` and silently no-op on the wrapper. Fixed in `23c4aa03e2`
   with `as_parquet_source` / `rewrap_like`.
2. **First new-build bench was OOM-killed — allocator, not code.** `Dockerfile.e2e` built without
   `jemalloc_replacing_malloc`; on glibc malloc the querier RSS climbed 23 → 45 s per `full_scan`
   run and hit 11.1 GB (`new-3.9.13-glibc/`, VM `dmesg` in `docker-vm-dmesg-oom.txt`). The prod
   image and the rebuilt e2e image (`--features aws,jemalloc_replacing_malloc`) stay flat at ~4 GiB
   over the same 144k tiny files. `Dockerfile.e2e` now builds with jemalloc.
3. `iox_query`'s `dedup_null_columns::test_tie_breaking` passes only with the workspace feature
   set (`schema/v3` unified by `influxdb3_server`); run it via `--workspace`, not `-p iox_query`.
