#!/usr/bin/env python3
"""CLI entry: long-running G8 Doris ADS materializer (upsert-kafka → UNIQUE KEY)."""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.doris_ads_materializer import (  # noqa: E402
    DorisAdsMaterializer,
    MaterializerConfig,
    config_from_env,
)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="G8 resident Doris ADS materializer")
    ap.add_argument("--bootstrap", default=None, help="Kafka bootstrap (host:port)")
    ap.add_argument("--group", default=None, help="consumer group id")
    ap.add_argument("--topic-dau", default=None)
    ap.add_argument("--topic-pay", default=None)
    ap.add_argument("--doris-host", default=None)
    ap.add_argument("--doris-port", type=int, default=None)
    ap.add_argument("--doris-user", default=None)
    ap.add_argument("--doris-password", default=None)
    ap.add_argument("--doris-db", default=None)
    ap.add_argument("--log-level", default="INFO")
    ap.add_argument(
        "--once-smoke",
        action="store_true",
        help="Connect Kafka+Doris, print config, exit 0 (no consume loop)",
    )
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    cfg = config_from_env()
    if args.bootstrap:
        cfg.bootstrap = args.bootstrap
    if args.group:
        cfg.group_id = args.group
    if args.topic_dau:
        cfg.topic_dau = args.topic_dau
    if args.topic_pay:
        cfg.topic_pay = args.topic_pay
    if args.doris_host:
        cfg.doris_host = args.doris_host
    if args.doris_port is not None:
        cfg.doris_port = args.doris_port
    if args.doris_user:
        cfg.doris_user = args.doris_user
    if args.doris_password is not None:
        cfg.doris_password = args.doris_password
    if args.doris_db:
        cfg.doris_db = args.doris_db

    if args.once_smoke:
        print(
            {
                "bootstrap": cfg.bootstrap,
                "group": cfg.group_id,
                "topics": [cfg.topic_dau, cfg.topic_pay],
                "doris": f"{cfg.doris_host}:{cfg.doris_port}/{cfg.doris_db}",
                "semantics": "at-least-once; commit after Doris write; UNIQUE KEY upsert; tombstone=DELETE; NOT EO-2PC",
            }
        )
        # touch Doris
        from pipeline.doris_ads_materializer import DorisWriter

        w = DorisWriter(cfg)
        try:
            c = w.conn()
            with c.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            print("doris_ok=1")
        finally:
            w.close()
        return 0

    mat = DorisAdsMaterializer(cfg)
    mat.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
