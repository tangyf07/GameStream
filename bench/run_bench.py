#!/usr/bin/env python3
"""
Optional micro-benchmark for local_runner stages.
Writes ONLY measured numbers to bench/results/ — never invents figures.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--players", type=int, default=5000)
    ap.add_argument("--events", type=int, default=50000)
    args = ap.parse_args()

    out_dir = ROOT / "bench" / "results"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    raw = ROOT / "data" / "bench_raw" / f"events_{ts}.jsonl"
    raw.parent.mkdir(parents=True, exist_ok=True)
    db = ROOT / "data" / f"bench_{ts}.duckdb"

    results: dict = {
        "timestamp_utc": ts,
        "players": args.players,
        "events": args.events,
        "host_note": "measured on this machine; not a formal SLA claim",
        "stages": {},
    }

    # Simulate
    t0 = time.perf_counter()
    r = subprocess.run(
        [
            sys.executable,
            str(ROOT / "simulator" / "generate_events.py"),
            "--players",
            str(args.players),
            "--events",
            str(args.events),
            "--out",
            str(raw),
        ],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
    )
    t1 = time.perf_counter()
    results["stages"]["simulate_sec"] = round(t1 - t0, 3)
    results["stages"]["simulate_exit"] = r.returncode
    if r.returncode != 0:
        results["error"] = r.stderr[-2000:]
        _write(out_dir, ts, results)
        return 1

    # Pipeline skip-simulate
    t0 = time.perf_counter()
    r2 = subprocess.run(
        [
            sys.executable,
            str(ROOT / "pipeline" / "local_runner.py"),
            "--skip-simulate",
            "--out",
            str(raw),
            "--duckdb",
            str(db),
            "--players",
            str(args.players),
            "--events",
            str(args.events),
        ],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
    )
    t1 = time.perf_counter()
    results["stages"]["pipeline_sec"] = round(t1 - t0, 3)
    results["stages"]["pipeline_exit"] = r2.returncode
    results["stages"]["pipeline_stdout_tail"] = (r2.stdout or "")[-1500:]
    if r2.returncode != 0:
        results["error"] = (r2.stderr or "")[-2000:]

    _write(out_dir, ts, results)
    print(json.dumps(results, indent=2))
    return 0 if r2.returncode == 0 else 1


def _write(out_dir: Path, ts: str, results: dict) -> None:
    path = out_dir / f"bench_{ts}.json"
    path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"Wrote {path}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
