#!/usr/bin/env python3
"""Generate G6 bench events and optionally stamp produce_ts while writing JSONL for kafka-console-producer."""
from __future__ import annotations

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


EVENT_TYPES = [
    "login", "create_role", "enter_dungeon", "clear_dungeon", "death",
    "equip", "enhance", "recharge", "gacha", "friend", "logout",
]
WEIGHTS = [12, 1, 18, 10, 8, 7, 6, 2, 5, 3, 12]


def sql_ts(dt: datetime) -> str:
    # Flink json.timestamp-format.standard=SQL
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=10000)
    ap.add_argument("--players", type=int, default=500)
    ap.add_argument("--servers", type=int, default=4)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", required=True)
    ap.add_argument(
        "--stamp-produce-ts",
        action="store_true",
        help="Set produce_ts=now at write time (for accurate E2E). Default: placeholder then re-stamp.",
    )
    ap.add_argument(
        "--rate",
        type=float,
        default=0.0,
        help="If >0, sleep to approx events/sec while writing (slows produce for lag observation).",
    )
    args = ap.parse_args()

    rng = random.Random(args.seed)
    base = datetime(2026, 9, 7, 10, 0, 0)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    t0 = time.perf_counter()
    wall0 = datetime.now(timezone.utc).replace(tzinfo=None)
    n = 0
    with out.open("w", encoding="utf-8") as f:
        for i in range(args.events):
            et = rng.choices(EVENT_TYPES, weights=WEIGHTS, k=1)[0]
            etime = base + timedelta(seconds=rng.randint(0, 3600), milliseconds=rng.randint(0, 999))
            if args.stamp_produce_ts:
                pts = sql_ts(datetime.now(timezone.utc).replace(tzinfo=None))
            else:
                pts = sql_ts(wall0)  # placeholder; producer wrapper may rewrite
            row = {
                "event_id": f"g6-{args.seed}-{i}-{uuid.uuid4().hex[:8]}",
                "event_type": et,
                "event_time": sql_ts(etime),
                "player_id": rng.randint(1, args.players),
                "server_id": rng.randint(1, args.servers),
                "produce_ts": pts,
            }
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
            n += 1
            if args.rate > 0:
                target = t0 + (n / args.rate)
                now = time.perf_counter()
                if target > now:
                    time.sleep(target - now)
    t1 = time.perf_counter()
    wall1 = datetime.now(timezone.utc).replace(tzinfo=None)
    meta = {
        "events": n,
        "write_wall_sec": round(t1 - t0, 4),
        "write_events_per_sec": round(n / (t1 - t0), 3) if t1 > t0 else None,
        "wall_start_utc_naive": sql_ts(wall0),
        "wall_end_utc_naive": sql_ts(wall1),
        "out": str(out),
        "stamp_produce_ts": bool(args.stamp_produce_ts),
        "rate_target": args.rate or None,
    }
    print(json.dumps(meta, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
