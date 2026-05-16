#!/usr/bin/env python3
"""
Run each query in `queries/*.sql` N times against the querier, recording wall
times. Output is a tag-named JSON file so two phases (uncompacted, compacted)
can be diffed by the harness.

Usage:
    docker compose run --rm bench --tag uncompacted
    docker compose run --rm bench --tag compacted
"""

import argparse
import glob
import json
import os
import statistics
import sys
import time
import urllib.parse
import urllib.request

QUERIER_URL = os.environ.get("QUERIER_URL", "http://querier:8181")
DB = os.environ.get("DB", "bench")
RUNS_PER_QUERY = int(os.environ.get("RUNS_PER_QUERY", "5"))


def run_query(sql: str) -> tuple[float, int]:
    """Returns (elapsed_seconds, response_bytes)."""
    params = urllib.parse.urlencode({"db": DB, "q": sql, "format": "csv"})
    url = f"{QUERIER_URL}/api/v3/query_sql?{params}"
    req = urllib.request.Request(url, method="GET")
    t0 = time.monotonic()
    with urllib.request.urlopen(req, timeout=600) as resp:
        body = resp.read()
        if resp.status >= 300:
            raise RuntimeError(
                f"query failed: HTTP {resp.status}: {body.decode()[:500]}"
            )
    return (time.monotonic() - t0, len(body))


def warm_up(sql: str) -> None:
    # Discard timing of the very first execution: it pays page-cache + parquet
    # metadata-cache misses that are not representative of steady-state load.
    try:
        run_query(sql)
    except Exception as e:
        print(f"  warmup failed (will still run): {e}", flush=True)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--tag", required=True, help="label written to results/<tag>.json")
    p.add_argument("--queries-dir", default="queries")
    args = p.parse_args()

    files = sorted(glob.glob(os.path.join(args.queries_dir, "*.sql")))
    if not files:
        print(f"no .sql files in {args.queries_dir}", file=sys.stderr)
        return 1

    print(
        f"benchmarking {len(files)} queries × {RUNS_PER_QUERY} runs (tag={args.tag})",
        flush=True,
    )

    out: dict = {
        "tag": args.tag,
        "runs_per_query": RUNS_PER_QUERY,
        "results": {},
    }
    for path in files:
        name = os.path.splitext(os.path.basename(path))[0]
        with open(path) as fp:
            sql = fp.read().strip()
        print(f"  {name}: warming...", flush=True)
        warm_up(sql)
        times: list[float] = []
        sizes: list[int] = []
        for i in range(RUNS_PER_QUERY):
            elapsed, size = run_query(sql)
            times.append(elapsed)
            sizes.append(size)
            print(
                f"    run {i + 1}/{RUNS_PER_QUERY}: {elapsed:.3f}s ({size} bytes)",
                flush=True,
            )
        out["results"][name] = {
            "sql": sql,
            "times_s": times,
            "min_s": min(times),
            "median_s": statistics.median(times),
            "max_s": max(times),
            "mean_s": statistics.fmean(times),
            "response_bytes": sizes[0],
        }

    os.makedirs("results", exist_ok=True)
    out_path = os.path.join("results", f"{args.tag}.json")
    with open(out_path, "w") as fp:
        json.dump(out, fp, indent=2)
    print(f"wrote {out_path}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
