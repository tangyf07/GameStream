#!/usr/bin/env python3
"""
GameStream player-behavior event generator.

Scalable CLI: --players --events --out [--servers --days --seed --rate]
Default demo: 10_000 players / 200_000 events (laptop-friendly).
Emits JSON Lines with unified envelope + type-specific payload.
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

EVENT_TYPES = [
    "login",
    "create_role",
    "enter_dungeon",
    "clear_dungeon",
    "death",
    "equip",
    "enhance",
    "recharge",
    "gacha",
    "friend",
    "logout",
]

# Rough behavioral weights (not production calibrated — demo distribution only)
WEIGHTS = {
    "login": 12,
    "create_role": 1,
    "enter_dungeon": 18,
    "clear_dungeon": 10,
    "death": 8,
    "equip": 7,
    "enhance": 6,
    "recharge": 2,
    "gacha": 5,
    "friend": 3,
    "logout": 12,
}

DUNGEONS = list(range(1001, 1021))
CLASSES = [1, 2, 3, 4, 5]
CHANNELS = ["appstore", "google", "official", "huawei", "vivo"]
DEVICE_OS = ["ios", "android", "pc"]
PAY_PRODUCTS = ["com.gs.pass60", "com.gs.gem328", "com.gs.gem648", "com.gs.month"]
PAY_AMOUNTS_FEN = [600, 3000, 6800, 9800, 19800, 32800, 64800]


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _payload(rng: random.Random, etype: str, dungeon_id: int | None = None) -> dict[str, Any]:
    if etype == "login":
        return {
            "client_version": f"1.{rng.randint(0, 9)}.{rng.randint(0, 20)}",
            "device_os": rng.choice(DEVICE_OS),
            "channel": rng.choice(CHANNELS),
        }
    if etype == "create_role":
        return {
            "role_name": f"Hero_{rng.randint(10000, 99999)}",
            "class_id": rng.choice(CLASSES),
            "gender": rng.randint(0, 1),
        }
    if etype == "enter_dungeon":
        return {
            "dungeon_id": dungeon_id or rng.choice(DUNGEONS),
            "difficulty": rng.randint(1, 5),
            "party_size": rng.randint(1, 4),
        }
    if etype == "clear_dungeon":
        return {
            "dungeon_id": dungeon_id or rng.choice(DUNGEONS),
            "clear_time_sec": rng.randint(60, 1800),
            "stars": rng.randint(1, 3),
        }
    if etype == "death":
        return {
            "dungeon_id": dungeon_id or rng.choice(DUNGEONS + [None]),  # type: ignore[list-item]
            "killer_type": rng.choice(["monster", "boss", "pvp", "environment"]),
            "map_id": rng.randint(1, 50),
        }
    if etype == "equip":
        return {
            "item_id": rng.randint(20000, 29999),
            "slot": rng.choice(["weapon", "armor", "ring", "necklace", "boots"]),
            "rarity": rng.randint(1, 5),
        }
    if etype == "enhance":
        fl = rng.randint(0, 12)
        return {
            "item_id": rng.randint(20000, 29999),
            "from_level": fl,
            "to_level": fl + 1,
            "success": rng.random() < 0.7,
        }
    if etype == "recharge":
        return {
            "amount_fen": rng.choice(PAY_AMOUNTS_FEN),
            "product_id": rng.choice(PAY_PRODUCTS),
            "pay_channel": rng.choice(["alipay", "wechat", "apple", "google"]),
        }
    if etype == "gacha":
        return {
            "pool_id": rng.randint(1, 8),
            "draw_count": rng.choice([1, 10]),
            "cost_ticket": rng.choice([1, 10]),
            "got_ssr": rng.random() < 0.08,
        }
    if etype == "friend":
        return {
            "action": rng.choice(["add", "accept", "remove"]),
            "target_player_id": rng.randint(1, 10_000_000),
        }
    if etype == "logout":
        return {
            "reason": rng.choice(["user", "timeout", "kick", "crash"]),
            "online_sec": rng.randint(30, 14400),
        }
    return {}


def generate(
    players: int,
    events: int,
    servers: int,
    days: int,
    seed: int,
    out_path: Path,
) -> dict[str, int]:
    rng = random.Random(seed)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Pre-assign players to servers and roles
    player_server = {pid: rng.randint(1, servers) for pid in range(1, players + 1)}
    player_role = {pid: pid * 10 + rng.randint(1, 9) for pid in range(1, players + 1)}
    # ~15% never created role yet
    no_role = set(rng.sample(range(1, players + 1), k=max(1, players // 7)))
    for pid in no_role:
        player_role[pid] = 0

    start = datetime.now(timezone.utc) - timedelta(days=days)
    types = list(WEIGHTS.keys())
    weights = [WEIGHTS[t] for t in types]
    counts: dict[str, int] = {t: 0 for t in types}

    # Track open sessions for somewhat consistent login/logout pairing
    open_sessions: dict[int, str] = {}
    # Track last entered dungeon per player for clear/death coherence
    last_dungeon: dict[int, int] = {}

    with out_path.open("w", encoding="utf-8") as f:
        for i in range(events):
            pid = rng.randint(1, players)
            sid = player_server[pid]
            etype = rng.choices(types, weights=weights, k=1)[0]

            # Bias: if no open session, prefer login
            if pid not in open_sessions and etype not in ("login", "create_role"):
                if rng.random() < 0.6:
                    etype = "login"

            if etype == "login" or pid not in open_sessions:
                sess = str(uuid.uuid4())
                open_sessions[pid] = sess
            else:
                sess = open_sessions[pid]

            if etype == "create_role" and player_role[pid] == 0:
                player_role[pid] = pid * 10 + rng.randint(1, 9)

            role_id = player_role[pid]
            if etype == "create_role" and role_id == 0:
                role_id = pid * 10 + 1
                player_role[pid] = role_id

            offset_sec = rng.randint(0, max(1, days * 86400 - 1))
            etime = start + timedelta(seconds=offset_sec)

            dungeon_id = None
            if etype == "enter_dungeon":
                dungeon_id = rng.choice(DUNGEONS)
                last_dungeon[pid] = dungeon_id
            elif etype in ("clear_dungeon", "death"):
                dungeon_id = last_dungeon.get(pid, rng.choice(DUNGEONS))

            event = {
                "event_id": str(uuid.uuid4()),
                "event_type": etype,
                "event_time": _iso(etime),
                "player_id": pid,
                "role_id": role_id,
                "server_id": sid,
                "session_id": sess,
                "payload": _payload(rng, etype, dungeon_id),
            }

            if etype == "logout" and pid in open_sessions:
                del open_sessions[pid]

            f.write(json.dumps(event, ensure_ascii=False) + "\n")
            counts[etype] += 1

            if (i + 1) % 50000 == 0:
                print(f"  generated {i + 1}/{events} ...", file=sys.stderr)

    counts["_total"] = events
    counts["_players"] = players
    return counts


def main() -> int:
    p = argparse.ArgumentParser(description="GameStream event simulator")
    p.add_argument("--players", type=int, default=10_000, help="Unique player count")
    p.add_argument("--events", type=int, default=200_000, help="Total events to emit")
    p.add_argument("--servers", type=int, default=8, help="Game server shards")
    p.add_argument("--days", type=int, default=7, help="Spread events over N days")
    p.add_argument("--seed", type=int, default=42, help="RNG seed")
    p.add_argument(
        "--out",
        type=str,
        default="data/raw/events.jsonl",
        help="Output JSONL path",
    )
    p.add_argument(
        "--rate",
        type=float,
        default=0.0,
        help="Optional events/sec throttle (0 = no throttle; for live-ish demos)",
    )
    args = p.parse_args()

    out = Path(args.out)
    print(
        f"[simulator] players={args.players} events={args.events} "
        f"servers={args.servers} days={args.days} out={out}",
        file=sys.stderr,
    )
    if args.rate > 0:
        print(
            f"[simulator] --rate={args.rate} noted; batch write ignores throttle "
            f"(use streaming harness for paced emit)",
            file=sys.stderr,
        )

    counts = generate(
        players=args.players,
        events=args.events,
        servers=args.servers,
        days=args.days,
        seed=args.seed,
        out_path=out,
    )
    print(json.dumps(counts, indent=2, ensure_ascii=False))
    print(f"[simulator] wrote {counts['_total']} events -> {out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
