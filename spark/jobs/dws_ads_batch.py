#!/usr/bin/env python3
"""
Spark batch: DWD parquet → DWS / ADS (backfill & retention/churn).

Runnable with local[*] if pyspark is installed. Same 口径 as DuckDB local_runner
and sql/metrics/*.sql.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dwd", default=str(ROOT / "data" / "dwd" / "player_events_clean.parquet"))
    ap.add_argument("--out", default=str(ROOT / "data" / "spark_out"))
    args = ap.parse_args()

    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except ImportError:
        print(
            "pyspark not installed. Optional: pip install pyspark\n"
            "Local demo already covered by DuckDB pipeline/local_runner.py",
            file=sys.stderr,
        )
        return 0  # soft-skip, not a hard failure for lite portfolio

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("GameStreamDwsAdsBatch")
        .getOrCreate()
    )
    dwd = spark.read.parquet(args.dwd)

    player_di = (
        dwd.groupBy("dt", "server_id", "player_id")
        .agg(
            F.count("*").alias("event_cnt"),
            F.countDistinct("session_id").alias("session_cnt"),
            F.sum(F.when(F.col("event_type") == "login", 1).otherwise(0)).alias("login_cnt"),
            F.sum(F.when(F.col("event_type") == "logout", 1).otherwise(0)).alias("logout_cnt"),
            F.sum(F.when(F.col("event_type") == "enter_dungeon", 1).otherwise(0)).alias(
                "enter_dungeon_cnt"
            ),
            F.sum(F.when(F.col("event_type") == "clear_dungeon", 1).otherwise(0)).alias(
                "clear_dungeon_cnt"
            ),
            F.sum(F.when(F.col("event_type") == "recharge", 1).otherwise(0)).alias("recharge_cnt"),
            F.sum(
                F.when(F.col("event_type") == "recharge", F.coalesce(F.col("amount_fen"), F.lit(0)))
                .otherwise(0)
            ).alias("recharge_fen"),
            F.sum(
                F.when(F.col("event_type") == "logout", F.coalesce(F.col("online_sec"), F.lit(0)))
                .otherwise(0)
            ).alias("online_sec_sum"),
        )
    )

    dau = player_di.groupBy("dt", "server_id").agg(
        F.countDistinct("player_id").alias("dau"),
        F.lit("ads_dau_di").alias("metric_id"),
    )

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    player_di.write.mode("overwrite").parquet(str(out / "dws_player_behavior_di"))
    dau.write.mode("overwrite").parquet(str(out / "ads_dau_di"))
    print(f"Wrote Spark outputs under {out}")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
