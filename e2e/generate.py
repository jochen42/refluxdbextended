#!/usr/bin/env python3
"""
Synthetic time-series generator for the influxdb3-unlocked benchmark.

Emits a sensors-style schema with three tags (host, region, sensor) and three
fields (temp, humidity, pressure). Timestamps span a configurable window
ending at "now" so queries with `WHERE time > now() - X` exercise the full
range. Many small batches → many gen1 parquet files, which is the worst case
for the uncompacted baseline.

CLI is intentionally minimal so the harness can drive it via env vars + a
single positional flag set. Run from inside the compose `gen` service:

    docker compose run --rm gen \\
        --hours 24 --hosts 200 --batch-size 5000 --points-per-host 5000
"""

import argparse
import math
import os
import random
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

WRITER_URL = os.environ.get("WRITER_URL", "http://writer:8181")
DB = os.environ.get("DB", "bench")

REGIONS = ["us-east", "us-west", "eu-central", "ap-south"]


def write_lp(payload: bytes) -> None:
    # Retry transient 5xx / network errors. The writer occasionally returns
    # 500 under heavy ingest when catalog optimistic-CC retries pile up; the
    # write itself does succeed on the next attempt.
    attempts = 8
    backoff = 0.5
    for i in range(attempts):
        req = urllib.request.Request(
            f"{WRITER_URL}/api/v2/write?bucket={DB}&precision=ns",
            data=payload,
            method="POST",
        )
        req.add_header("Content-Type", "text/plain")
        try:
            with urllib.request.urlopen(req, timeout=300) as resp:
                if resp.status >= 300:
                    raise RuntimeError(
                        f"write failed: HTTP {resp.status}: "
                        f"{resp.read().decode()}"
                    )
                return
        except urllib.error.HTTPError as e:
            if e.code < 500 or i == attempts - 1:
                raise
            print(f"  [retry {i+1}/{attempts}] HTTP {e.code}, "
                  f"backing off {backoff:.1f}s", flush=True)
        except (urllib.error.URLError, ConnectionResetError, TimeoutError) as e:
            if i == attempts - 1:
                raise
            print(f"  [retry {i+1}/{attempts}] {type(e).__name__}: {e}, "
                  f"backing off {backoff:.1f}s", flush=True)
        time.sleep(backoff)
        backoff = min(backoff * 2, 8.0)


def generate(
    hours: int,
    hosts: int,
    points_per_host: int,
    batch_size: int,
    seed: int,
    parallel: int,
) -> None:
    rng = random.Random(seed)
    end_ns = time.time_ns()
    start_ns = end_ns - hours * 3600 * 1_000_000_000

    total_points = hosts * points_per_host
    print(
        f"plan: {hosts} hosts × {points_per_host} points "
        f"= {total_points} total points over {hours}h "
        f"(start={start_ns} end={end_ns}) parallel={parallel}",
        flush=True,
    )

    # Bounded backpressure: cap in-flight batches to 2x worker count so the
    # generator loop doesn't queue gigabytes of payloads in memory faster
    # than the writer can absorb them.
    in_flight = threading.Semaphore(max(parallel * 2, 2))
    progress_lock = threading.Lock()
    state = {"written": 0, "next_report": batch_size * 20}
    t0 = time.monotonic()

    def submit(payload: bytes, count: int) -> None:
        try:
            write_lp(payload)
        finally:
            in_flight.release()
        with progress_lock:
            state["written"] += count
            written = state["written"]
            if written >= state["next_report"]:
                elapsed = time.monotonic() - t0
                rate = written / max(elapsed, 0.001)
                print(
                    f"  progress: {written}/{total_points} "
                    f"({100 * written / total_points:.1f}%) "
                    f"at {rate:.0f} points/s",
                    flush=True,
                )
                state["next_report"] += batch_size * 20

    with ThreadPoolExecutor(max_workers=parallel) as pool:
        buf: list[str] = []
        for h in range(hosts):
            host = f"host-{h:05d}"
            region = REGIONS[h % len(REGIONS)]
            for s in range(points_per_host):
                ts = start_ns + (s * (end_ns - start_ns) // max(points_per_host - 1, 1))
                ts += (h * 31) % 997
                temp = 60.0 + 30.0 * math.sin(s * 0.001 + h)
                humidity = 30.0 + 50.0 * math.cos(s * 0.0007 + h)
                pressure = 1000.0 + 50.0 * math.sin(s * 0.0003 - h)
                sensor = f"s{(h + s) % 17}"
                line = (
                    f"sensors,host={host},region={region},sensor={sensor} "
                    f"temp={temp:.3f},humidity={humidity:.3f},pressure={pressure:.3f} "
                    f"{ts}"
                )
                buf.append(line)
                if len(buf) >= batch_size:
                    in_flight.acquire()
                    payload = "\n".join(buf).encode()
                    count = len(buf)
                    buf = []
                    pool.submit(submit, payload, count)
        if buf:
            in_flight.acquire()
            payload = "\n".join(buf).encode()
            count = len(buf)
            pool.submit(submit, payload, count)

    elapsed = time.monotonic() - t0
    written = state["written"]
    print(
        f"done: wrote {written} points in {elapsed:.1f}s "
        f"({written / max(elapsed, 0.001):.0f} points/s)",
        flush=True,
    )


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--hours", type=int, default=24,
                   help="time window covered by generated points")
    p.add_argument("--hosts", type=int, default=200,
                   help="distinct host tag values")
    p.add_argument("--points-per-host", type=int, default=5000,
                   help="points generated per host")
    p.add_argument("--batch-size", type=int, default=5000,
                   help="line-protocol lines per HTTP request")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--parallel", type=int,
                   default=int(os.environ.get("PARALLEL", "1")),
                   help="number of concurrent HTTP writers")
    args = p.parse_args()
    generate(args.hours, args.hosts, args.points_per_host, args.batch_size,
             args.seed, args.parallel)
    return 0


if __name__ == "__main__":
    sys.exit(main())
