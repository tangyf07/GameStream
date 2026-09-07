#!/usr/bin/env python3
"""Generate G8 steady-bench events (continuous load + Doris E2E probe batches).

Tracks cumulative unique players so expected Doris ADS (dau / pay_users) is known.
Never invents metrics — only emits event JSONL + expectation meta.
"""
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
# Bias away from recharge for background load so pay_users stays controllable via probes.
WEIGHTS = [14, 1, 18, 10, 8, 7, 6, 0, 5, 3, 12]  # recharge weight 0 in continuous


def sql_ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["continuous", "probe"], required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--meta-out", required=True)
    ap.add_argument("--state", required=True, help="JSON path tracking cumulative players / pay")
    ap.add_argument("--dt", default="2026-09-07")
    ap.add_argument("--server-id", type=int, default=88)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--rate", type=float, default=40.0, help="target events/sec while writing continuous file")
    ap.add_argument("--duration-sec", type=float, default=40.0)
    ap.add_argument("--pool-players", type=int, default=200, help="player_id pool for continuous load")
    ap.add_argument("--probe-new-players", type=int, default=20)
    ap.add_argument("--probe-pay-users", type=int, default=2)
    ap.add_argument("--round", type=int, default=1)
    args = ap.parse_args()

    rng = random.Random(args.seed + args.round * 1009 + (0 if args.mode == "continuous" else 17))
    state_path = Path(args.state)
    if state_path.exists() and state_path.stat().st_size > 0:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    else:
        state = {
            "players": [],
            "pay_players": [],
            "events_total": 0,
            "rounds": [],
        }
    players = set(int(x) for x in state.get("players") or [])
    pay_players = set(int(x) for x in state.get("pay_players") or [])

    base = datetime.strptime(args.dt + " 10:00:00", "%Y-%m-%d %H:%M:%S")
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    n = 0
    t0 = time.perf_counter()
    wall0 = datetime.now(timezone.utc).replace(tzinfo=None)
    new_players: list[int] = []
    new_pay: list[int] = []

    with out.open("w", encoding="utf-8") as f:
        if args.mode == "continuous":
            # Approximate duration*rate events; pace writes if rate>0
            target_n = max(1, int(round(args.duration_sec * args.rate)))
            pool = list(range(1, args.pool_players + 1))
            for i in range(target_n):
                et = rng.choices(EVENT_TYPES, weights=WEIGHTS, k=1)[0]
                pid = rng.choice(pool)
                players.add(pid)
                etime = base + timedelta(seconds=rng.randint(0, 3500), milliseconds=rng.randint(0, 999))
                pts = sql_ts(datetime.now(timezone.utc).replace(tzinfo=None))
                row = {
                    "event_id": f"g8s-r{args.round}-c-{i}-{uuid.uuid4().hex[:8]}",
                    "event_type": et,
                    "event_time": sql_ts(etime),
                    "player_id": pid,
                    "server_id": args.server_id,
                    "tag": f"continuous_r{args.round}",
                    "produce_ts": pts,
                }
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
                n += 1
                if args.rate > 0:
                    target = t0 + (n / args.rate)
                    now = time.perf_counter()
                    if target > now:
                        time.sleep(target - now)
        else:
            # Probe: brand-new players so DAU/pay expectations are exact deltas
            start_id = 100000 + args.round * 1000
            for j in range(args.probe_new_players):
                pid = start_id + j
                new_players.append(pid)
                players.add(pid)
                etime = base + timedelta(seconds=3600 + args.round * 60 + j, milliseconds=j)
                pts = sql_ts(datetime.now(timezone.utc).replace(tzinfo=None))
                row = {
                    "event_id": f"g8s-r{args.round}-p-login-{j}-{uuid.uuid4().hex[:8]}",
                    "event_type": "login",
                    "event_time": sql_ts(etime),
                    "player_id": pid,
                    "server_id": args.server_id,
                    "tag": f"probe_r{args.round}",
                    "produce_ts": pts,
                }
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
                n += 1
            pay_n = min(args.probe_pay_users, len(new_players))
            for j in range(pay_n):
                pid = new_players[j]
                new_pay.append(pid)
                pay_players.add(pid)
                etime = base + timedelta(seconds=3600 + args.round * 60 + 100 + j, milliseconds=j)
                pts = sql_ts(datetime.now(timezone.utc).replace(tzinfo=None))
                row = {
                    "event_id": f"g8s-r{args.round}-p-pay-{j}-{uuid.uuid4().hex[:8]}",
                    "event_type": "recharge",
                    "event_time": sql_ts(etime),
                    "player_id": pid,
                    "server_id": args.server_id,
                    "tag": f"probe_pay_r{args.round}",
                    "produce_ts": pts,
                }
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
                n += 1

    t1 = time.perf_counter()
    wall1 = datetime.now(timezone.utc).replace(tzinfo=None)
    expected_dau = len(players)
    expected_pay = len(pay_players)
    state["players"] = sorted(players)
    state["pay_players"] = sorted(pay_players)
    state["events_total"] = int(state.get("events_total") or 0) + n
    state["expected_dau"] = expected_dau
    state["expected_pay_users"] = expected_pay
    state["rounds"].append({
        "round": args.round,
        "mode": args.mode,
        "events": n,
        "new_players": new_players,
        "new_pay_players": new_pay,
        "expected_dau_after": expected_dau,
        "expected_pay_users_after": expected_pay,
    })
    state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

    meta = {
        "mode": args.mode,
        "round": args.round,
        "events": n,
        "write_wall_sec": round(t1 - t0, 4),
        "write_events_per_sec": round(n / (t1 - t0), 3) if t1 > t0 else None,
        "wall_start_utc_naive": sql_ts(wall0),
        "wall_end_utc_naive": sql_ts(wall1),
        "out": str(out),
        "server_id": args.server_id,
        "dt": args.dt,
        "new_players": new_players,
        "new_pay_players": new_pay,
        "expected_dau": expected_dau,
        "expected_pay_users": expected_pay,
        "rate_target": args.rate if args.mode == "continuous" else None,
        "duration_sec_target": args.duration_sec if args.mode == "continuous" else None,
    }
    Path(args.meta_out).write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(meta, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
