#!/usr/bin/env python3
"""GameStream G3 fixture generator: controlled disorder / late / duplicate event_ids.

Produces flat JSONL for topic gamestream.g3.events (NO nested payload — G2 lesson).

Watermark delay N (default 5s) must match flink/sql/g3_event_time_watermark.sql.
Produce ORDER = Kafka offset order = Flink processing order (single partition).

Semantics encoded in produce order:
  - in_order / ooo_within: should enter window W1 [base, base+1min)
  - dup_replay: same event_id as dup_first → counted once after dedup
  - close_w1: advances watermark past W1 end → closes W1
  - late_beyond_wm: event_time in W1 but arrives after WM >= W1 end → DROPPED
  - w2 / close_w2: optional second window so W2 also emits

Expected W1 (default): event_cnt=7, player_cnt=7  (late dropped, dup once)
Prints EXPECTED.json summary to stdout trailer / optional --expected-out.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


def ts(dt: datetime) -> str:
    """Flink JSON SQL timestamp: yyyy-MM-dd HH:mm:ss[.SSS]"""
    if dt.microsecond:
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def build_events(base: datetime, wm_sec: int) -> list[dict[str, Any]]:
    """Return events in PRODUCE order. wm_sec = watermark out-of-orderness."""
    w1 = base
    # Need event_time >= window_end + wm_sec to close tumble window ending at base+1min
    close_w1_et = base + timedelta(minutes=1, seconds=wm_sec + 5)  # e.g. 10:01:10 for wm=5
    late_et = base + timedelta(seconds=40)  # still in W1 chronologically, but late by produce order
    w2_et = base + timedelta(minutes=1, seconds=20)
    close_w2_et = base + timedelta(minutes=2, seconds=wm_sec + 5)

    # OOO within bound: after max_et=base+30s, WM=base+30-wm; send base+30-wm+2 (still > WM)
    ooo_high = base + timedelta(seconds=30)
    ooo_low = ooo_high - timedelta(seconds=max(1, wm_sec - 2))  # lag = wm-2 < wm

    rows: list[dict[str, Any]] = [
        # --- Phase A: in-order W1 ---
        dict(event_id="g3-in-01", event_type="login", event_time=ts(base + timedelta(seconds=5)),
             player_id=1001, server_id=1, tag="in_order"),
        dict(event_id="g3-in-02", event_type="login", event_time=ts(base + timedelta(seconds=15)),
             player_id=1002, server_id=1, tag="in_order"),
        dict(event_id="g3-in-03", event_type="enter_dungeon", event_time=ts(base + timedelta(seconds=25)),
             player_id=1003, server_id=1, tag="in_order"),
        # --- Phase B: out-of-order within watermark ---
        dict(event_id="g3-ooo-hi", event_type="login", event_time=ts(ooo_high),
             player_id=1004, server_id=1, tag="ooo_advance"),
        dict(event_id="g3-ooo-lo", event_type="login", event_time=ts(ooo_low),
             player_id=1005, server_id=1, tag="ooo_within"),
        # --- Phase C: duplicate event_id (replay) ---
        dict(event_id="g3-dup-01", event_type="recharge", event_time=ts(base + timedelta(seconds=35)),
             player_id=1006, server_id=1, tag="dup_first"),
        dict(event_id="g3-dup-01", event_type="recharge", event_time=ts(base + timedelta(seconds=35)),
             player_id=1006, server_id=1, tag="dup_replay"),
        # --- Phase D: more W1 ---
        dict(event_id="g3-in-04", event_type="logout", event_time=ts(base + timedelta(seconds=50)),
             player_id=1007, server_id=1, tag="in_order"),
        # --- Phase E: close W1 (WM >= base+1min) ---
        dict(event_id="g3-w2-01", event_type="login", event_time=ts(close_w1_et),
             player_id=2001, server_id=1, tag="close_w1"),
        # --- Phase F: LATE for W1 (dropped) ---
        dict(event_id="g3-late-01", event_type="login", event_time=ts(late_et),
             player_id=9001, server_id=1, tag="late_beyond_wm"),
        # --- Phase G: W2 body + close ---
        dict(event_id="g3-w2-02", event_type="login", event_time=ts(w2_et),
             player_id=2002, server_id=1, tag="w2"),
        dict(event_id="g3-w2-close", event_type="login", event_time=ts(close_w2_et),
             player_id=3001, server_id=1, tag="close_w2"),
    ]
    return rows


def expected_summary(base: datetime, wm_sec: int, rows: list[dict[str, Any]]) -> dict[str, Any]:
    w1_start = base
    w1_end = base + timedelta(minutes=1)
    w2_start = w1_end
    w2_end = base + timedelta(minutes=2)

    # Events that should be in W1 after dedup & late-drop (by tag / produce semantics)
    w1_keep_tags = {"in_order", "ooo_advance", "ooo_within", "dup_first"}
    # close_w1 / w2 / late / close_w2 / dup_replay excluded from W1 count
    w1_ids: set[str] = set()
    w1_players: set[int] = set()
    for r in rows:
        if r["tag"] in w1_keep_tags:
            w1_ids.add(r["event_id"])
            w1_players.add(r["player_id"])

    # W2: close_w1 event_time is in W2 if close_w1_et in [w2_start, w2_end)
    # close_w1_et = base + 1min + wm + 5 → in W2 for wm=5 (10:01:10)
    w2_keep_tags = {"close_w1", "w2"}
    w2_ids: set[str] = set()
    w2_players: set[int] = set()
    for r in rows:
        if r["tag"] in w2_keep_tags:
            w2_ids.add(r["event_id"])
            w2_players.add(r["player_id"])

    return {
        "watermark_seconds": wm_sec,
        "base_event_time": ts(base),
        "produce_count": len(rows),
        "unique_event_ids": len({r["event_id"] for r in rows}),
        "duplicate_event_ids": ["g3-dup-01"],
        "late_dropped_event_ids": ["g3-late-01"],
        "late_policy": "drop (Flink SQL default allowedLateness=0; no side-output in pure SQL)",
        "windows": [
            {
                "window_start": ts(w1_start),
                "window_end": ts(w1_end),
                "event_cnt": len(w1_ids),
                "player_cnt": len(w1_players),
                "note": "dedup keeps g3-dup-01 once; g3-late-01 excluded",
            },
            {
                "window_start": ts(w2_start),
                "window_end": ts(w2_end),
                "event_cnt": len(w2_ids),
                "player_cnt": len(w2_players),
                "note": "close_w1 + w2; close_w2 only advances WM",
            },
        ],
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default="/tmp/gamestream_g3_events.jsonl")
    ap.add_argument("--expected-out", default="/tmp/gamestream_g3_expected.json")
    ap.add_argument("--base", default="2026-09-07 10:00:00",
                    help="W1 start / base event_time (SQL timestamp)")
    ap.add_argument("--watermark-seconds", type=int, default=5)
    args = ap.parse_args()

    base = datetime.strptime(args.base, "%Y-%m-%d %H:%M:%S")
    rows = build_events(base, args.watermark_seconds)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    exp = expected_summary(base, args.watermark_seconds, rows)
    exp_path = Path(args.expected_out)
    exp_path.parent.mkdir(parents=True, exist_ok=True)
    exp_path.write_text(json.dumps(exp, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(f"[g3-gen] wrote {len(rows)} lines -> {out}", file=sys.stderr)
    print(f"[g3-gen] expected -> {exp_path}", file=sys.stderr)
    print(json.dumps(exp, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
