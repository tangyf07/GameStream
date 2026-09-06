#!/usr/bin/env python3
"""Publish JSONL events to Kafka (optional; e2e_g2.sh uses console-producer by default)."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="localhost:19092")
    p.add_argument("--topic", default="gamestream.ods.player_events")
    p.add_argument("--file", required=True, help="JSONL path")
    args = p.parse_args()
    try:
        from kafka import KafkaProducer  # type: ignore
    except ImportError:
        print(
            "kafka-python not installed. Prefer: docker exec -i gs-kafka "
            "kafka-console-producer ... < file.jsonl",
            file=sys.stderr,
        )
        return 2
    path = Path(args.file)
    prod = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: v if isinstance(v, bytes) else str(v).encode("utf-8"),
        acks="all",
        linger_ms=20,
    )
    n = 0
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            prod.send(args.topic, line.encode("utf-8"))
            n += 1
    prod.flush()
    print(f"published {n} messages -> {args.topic} @ {args.bootstrap}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
