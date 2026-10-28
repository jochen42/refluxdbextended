#!/usr/bin/env python3
"""
Ground-truth writer and checker for `run-restart-integrity-test.sh`.

`churn` backfills rows into a fixed, closed time window (a few days in the
past) so the compactor keeps producing compaction manifests while nothing
outside the harness ever writes into that window. Every row is unique
(per-row ns offset) and carries an integer field `v` = global row id, so the
expected per-table count, sum(v) and per-hour counts are known exactly and
recorded in `expected.json` after every round.

`verify` queries the querier and compares against `expected.json`: it waits
until the view matches (eventual consistency after a burst) and then keeps
checking for `--stable-sec` so a flapping view (rows lost or duplicated while
compaction swaps files) fails too.

`elapsed` prints the seconds between two RFC 3339 timestamps (docker logs).

Stdlib only; runs on the host against the published ports.
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

NS = 1_000_000_000
HOUR_NS = 3600 * NS
MINUTE_NS = 60 * NS


def load_expected(path: str, tables: int, hours: int) -> dict:
    if os.path.exists(path):
        with open(path) as fp:
            return json.load(fp)
    day_ns = 86400 * NS
    # Midnight UTC three days ago: old enough that no gen1/gen2 window is
    # still open, inside the default query lookback.
    window_start = (time.time_ns() // day_ns - 3) * day_ns
    return {
        "window_start_ns": window_start,
        "tables": tables,
        "hours": hours,
        "next_round": 0,
        "next_g": 0,
        "per_table": {},
    }


def save_expected(path: str, exp: dict) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w") as fp:
        json.dump(exp, fp, indent=1, sort_keys=True)
    os.replace(tmp, path)


def write_lp(url: str, db: str, payload: bytes) -> None:
    backoff = 0.5
    for attempt in range(8):
        req = urllib.request.Request(
            f"{url}/api/v2/write?bucket={db}&precision=ns",
            data=payload,
            method="POST",
            headers={"Content-Type": "text/plain"},
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                if resp.status < 300:
                    return
                raise RuntimeError(f"HTTP {resp.status}")
        except urllib.error.HTTPError as e:
            if e.code < 500 or attempt == 7:
                raise
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            if attempt == 7:
                raise
        time.sleep(backoff)
        backoff = min(backoff * 2, 8.0)


def churn(args) -> int:
    exp = load_expected(args.expected, args.tables, args.hours)
    tables, hours = exp["tables"], exp["hours"]
    start = exp["window_start_ns"]
    for _ in range(args.rounds):
        r = exp["next_round"]
        table = f"t{r % tables}"
        # Each table walks through the hours of the window, one minute slot
        # per visit, so every round lands in a single gen1 window and every
        # gen2 (1h) window keeps receiving new gen1 files.
        visit = r // tables
        hour = visit % hours
        minute = (visit // hours) % 60
        base = start + hour * HOUR_NS + minute * MINUTE_NS
        g0 = exp["next_g"]
        lines = []
        for i in range(args.rows_per_round):
            g = g0 + i
            host = f"h{g % args.hosts}"
            # g as ns offset keeps (series, time) unique for any run size.
            lines.append(f"{table},host={host} v={g}i {base + g}")
        write_lp(args.writer_url, args.db, "\n".join(lines).encode())

        t = exp["per_table"].setdefault(
            table, {"count": 0, "sum_v": 0, "min_ts": None, "max_ts": None, "hours": {}}
        )
        n = args.rows_per_round
        t["count"] += n
        t["sum_v"] += n * g0 + n * (n - 1) // 2
        lo, hi = base + g0, base + g0 + n - 1
        t["min_ts"] = lo if t["min_ts"] is None else min(t["min_ts"], lo)
        t["max_ts"] = hi if t["max_ts"] is None else max(t["max_ts"], hi)
        hkey = str(hour)
        t["hours"][hkey] = t["hours"].get(hkey, 0) + n

        exp["next_round"] = r + 1
        exp["next_g"] = g0 + n
        save_expected(args.expected, exp)
        if args.sleep > 0:
            time.sleep(args.sleep)
    print(
        f"churn: rounds={exp['next_round']} rows={exp['next_g']} "
        f"tables={sorted(exp['per_table'])}",
        flush=True,
    )
    return 0


def query(url: str, db: str, sql: str) -> list:
    qs = urllib.parse.urlencode({"db": db, "q": sql, "format": "json"})
    with urllib.request.urlopen(f"{url}/api/v3/query_sql?{qs}", timeout=60) as resp:
        return json.loads(resp.read() or b"[]")


def observe(args, exp: dict) -> list:
    """Return a list of mismatch strings; empty means the view matches."""
    problems = []
    start = exp["window_start_ns"]
    for table, want in sorted(exp["per_table"].items()):
        try:
            row = query(
                args.querier_url,
                args.db,
                f"SELECT count(*) AS n, sum(v) AS s, "
                f"CAST(min(time) AS BIGINT) AS lo, CAST(max(time) AS BIGINT) AS hi "
                f"FROM {table}",
            )[0]
            hours = query(
                args.querier_url,
                args.db,
                f"SELECT CAST((CAST(time AS BIGINT) - {start}) / {HOUR_NS} AS BIGINT) AS h, "
                f"count(*) AS n FROM {table} GROUP BY 1",
            )
        except Exception as e:  # noqa: BLE001 - any query failure is a finding
            problems.append(f"{table}: query failed: {e}")
            continue
        got = {
            "count": row.get("n"),
            "sum_v": row.get("s"),
            "min_ts": row.get("lo"),
            "max_ts": row.get("hi"),
        }
        for key, value in got.items():
            if value != want[key]:
                problems.append(f"{table}: {key} got={value} want={want[key]}")
        got_hours = {str(h["h"]): h["n"] for h in hours}
        for h in sorted(set(got_hours) | set(want["hours"]), key=int):
            if got_hours.get(h, 0) != want["hours"].get(h, 0):
                problems.append(
                    f"{table}: hour {h} got={got_hours.get(h, 0)} "
                    f"want={want['hours'].get(h, 0)}"
                )
    return problems


def verify(args) -> int:
    with open(args.expected) as fp:
        exp = json.load(fp)
    t0 = time.monotonic()
    problems = []
    while True:
        problems = observe(args, exp)
        if not problems:
            break
        if time.monotonic() - t0 > args.wait_sec:
            print(f"verify: view never matched within {args.wait_sec}s", flush=True)
            for p in problems[:40]:
                print(f"  {p}", flush=True)
            return 1
        time.sleep(2)
    matched_after = time.monotonic() - t0
    rows = sum(t["count"] for t in exp["per_table"].values())
    print(f"verify: matched after {matched_after:.1f}s ({rows} rows)", flush=True)

    checks = 0
    stable_until = time.monotonic() + args.stable_sec
    while time.monotonic() < stable_until:
        time.sleep(args.stable_interval)
        problems = observe(args, exp)
        checks += 1
        if problems:
            print(f"verify: view diverged during stability window (check {checks})", flush=True)
            for p in problems[:40]:
                print(f"  {p}", flush=True)
            return 1
    print(f"verify: stable for {args.stable_sec}s ({checks} checks)", flush=True)
    return 0


def elapsed(args) -> int:
    def parse(ts: str) -> datetime:
        ts = ts.strip().replace("Z", "+00:00")
        # Docker prints nanoseconds; datetime takes at most microseconds.
        if "." in ts:
            head, rest = ts.split(".", 1)
            frac, tz = rest[:-6], rest[-6:]
            ts = f"{head}.{frac[:6]}{tz}"
        return datetime.fromisoformat(ts).astimezone(timezone.utc)

    print(f"{(parse(args.end) - parse(args.start)).total_seconds():.1f}")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--expected", required=True)
    common.add_argument("--db", default="restart")

    c = sub.add_parser("churn", parents=[common])
    c.add_argument("--writer-url", required=True)
    c.add_argument("--rounds", type=int, required=True)
    c.add_argument("--rows-per-round", type=int, default=200)
    c.add_argument("--tables", type=int, default=4)
    c.add_argument("--hours", type=int, default=24)
    c.add_argument("--hosts", type=int, default=20)
    c.add_argument("--sleep", type=float, default=0.25)
    c.set_defaults(func=churn)

    v = sub.add_parser("verify", parents=[common])
    v.add_argument("--querier-url", required=True)
    v.add_argument("--wait-sec", type=float, default=120)
    v.add_argument("--stable-sec", type=float, default=60)
    v.add_argument("--stable-interval", type=float, default=5)
    v.set_defaults(func=verify)

    e = sub.add_parser("elapsed")
    e.add_argument("start")
    e.add_argument("end")
    e.set_defaults(func=elapsed)

    args = p.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
