#!/usr/bin/env python3
"""
GameStream local lite end-to-end runner (no Docker).

simulate → ODS (parquet) → DWD (clean) → DWS → ADS in DuckDB.
Mirrors Flink/Spark SQL 口径 so interviewers can cross-read sql/ and flink/.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import duckdb
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from simulator.generate_events import generate  # noqa: E402
from pipeline.kafka_io import FileTopic  # noqa: E402


def load_yaml(name: str) -> dict:
    with (ROOT / "config" / name).open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_dirs(paths: dict) -> None:
    for key in ("data_root", "dws_dir", "ads_dir"):
        Path(paths[key]).mkdir(parents=True, exist_ok=True)
    Path(paths["raw_jsonl"]).parent.mkdir(parents=True, exist_ok=True)
    Path(paths["ods_parquet"]).parent.mkdir(parents=True, exist_ok=True)
    Path(paths["dwd_parquet"]).parent.mkdir(parents=True, exist_ok=True)
    Path(paths["duckdb"]).parent.mkdir(parents=True, exist_ok=True)


def step_simulate(args: argparse.Namespace, paths: dict) -> Path:
    out = Path(args.out or paths["raw_jsonl"])
    print(f"[1/5] Simulate events → {out}")
    t0 = time.time()
    counts = generate(
        players=args.players,
        events=args.events,
        servers=args.servers,
        days=args.days,
        seed=args.seed,
        out_path=out,
    )
    print(f"      done in {time.time() - t0:.1f}s; total={counts['_total']}")
    # file topic mirror (Kafka-shaped)
    topic_cfg = load_yaml("topics.yaml")
    ft = FileTopic(topic_cfg["file_topics"]["base_dir"], "gamestream.ods.player_events")
    ft.clear()
    n = ft.produce_file(out)
    print(f"      file-topic segments written, lines={n}")
    return out


def step_ods_dwd(con: duckdb.DuckDBPyConnection, paths: dict, raw: Path) -> None:
    print("[2/5] ODS → DWD (DuckDB)")
    t0 = time.time()
    # ODS: ingest JSONL, flatten payload keys of interest
    con.execute("CREATE SCHEMA IF NOT EXISTS ods")
    con.execute("CREATE SCHEMA IF NOT EXISTS dwd")
    con.execute("CREATE SCHEMA IF NOT EXISTS dws")
    con.execute("CREATE SCHEMA IF NOT EXISTS ads")

    con.execute(
        f"""
        CREATE OR REPLACE TABLE ods.player_events AS
        SELECT
            json_extract_string(j.json, '$.event_id') AS event_id,
            json_extract_string(j.json, '$.event_type') AS event_type,
            CAST(json_extract_string(j.json, '$.event_time') AS TIMESTAMP) AS event_time,
            CAST(json_extract(j.json, '$.player_id') AS BIGINT) AS player_id,
            CAST(json_extract(j.json, '$.role_id') AS BIGINT) AS role_id,
            CAST(json_extract(j.json, '$.server_id') AS INTEGER) AS server_id,
            json_extract_string(j.json, '$.session_id') AS session_id,
            json_extract(j.json, '$.payload') AS payload,
            CAST(json_extract(j.json, '$.payload.dungeon_id') AS INTEGER) AS dungeon_id,
            CAST(json_extract(j.json, '$.payload.amount_fen') AS BIGINT) AS amount_fen,
            CAST(json_extract(j.json, '$.payload.online_sec') AS INTEGER) AS online_sec
        FROM read_json_objects('{raw.as_posix()}', format='newline_delimited') AS j
        """
    )
    # Export ODS parquet
    con.execute(
        f"COPY ods.player_events TO '{Path(paths['ods_parquet']).as_posix()}' (FORMAT PARQUET)"
    )

    # DWD: clean — drop null event_id, invalid types, dedupe by event_id
    valid_types = (
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
    )
    types_sql = ", ".join(f"'{t}'" for t in valid_types)
    con.execute(
        f"""
        CREATE OR REPLACE TABLE dwd.player_events_clean AS
        SELECT
            event_id,
            event_type,
            event_time,
            CAST(event_time AS DATE) AS dt,
            player_id,
            role_id,
            server_id,
            session_id,
            payload,
            dungeon_id,
            amount_fen,
            online_sec
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_time) AS rn
            FROM ods.player_events
            WHERE event_id IS NOT NULL
              AND event_type IN ({types_sql})
              AND player_id IS NOT NULL
              AND player_id > 0
              AND event_time IS NOT NULL
        ) t
        WHERE rn = 1
        """
    )
    con.execute(
        f"COPY dwd.player_events_clean TO '{Path(paths['dwd_parquet']).as_posix()}' (FORMAT PARQUET)"
    )
    n_ods = con.execute("SELECT COUNT(*) FROM ods.player_events").fetchone()[0]
    n_dwd = con.execute("SELECT COUNT(*) FROM dwd.player_events_clean").fetchone()[0]
    print(f"      ODS={n_ods} DWD={n_dwd} in {time.time() - t0:.1f}s")


def step_dws(con: duckdb.DuckDBPyConnection, paths: dict) -> None:
    print("[3/5] DWS aggregates")
    t0 = time.time()
    # Player-day behavior fact
    con.execute(
        """
        CREATE OR REPLACE TABLE dws.player_behavior_di AS
        SELECT
            dt,
            server_id,
            player_id,
            COUNT(*) AS event_cnt,
            COUNT(DISTINCT session_id) AS session_cnt,
            SUM(CASE WHEN event_type = 'login' THEN 1 ELSE 0 END) AS login_cnt,
            SUM(CASE WHEN event_type = 'logout' THEN 1 ELSE 0 END) AS logout_cnt,
            SUM(CASE WHEN event_type = 'enter_dungeon' THEN 1 ELSE 0 END) AS enter_dungeon_cnt,
            SUM(CASE WHEN event_type = 'clear_dungeon' THEN 1 ELSE 0 END) AS clear_dungeon_cnt,
            SUM(CASE WHEN event_type = 'death' THEN 1 ELSE 0 END) AS death_cnt,
            SUM(CASE WHEN event_type = 'recharge' THEN 1 ELSE 0 END) AS recharge_cnt,
            COALESCE(SUM(CASE WHEN event_type = 'recharge' THEN amount_fen ELSE 0 END), 0) AS recharge_fen,
            COALESCE(SUM(CASE WHEN event_type = 'logout' THEN online_sec ELSE 0 END), 0) AS online_sec_sum,
            MIN(event_time) AS first_event_time,
            MAX(event_time) AS last_event_time
        FROM dwd.player_events_clean
        GROUP BY dt, server_id, player_id
        """
    )
    # Dungeon-day fact
    con.execute(
        """
        CREATE OR REPLACE TABLE dws.dungeon_behavior_di AS
        SELECT
            dt,
            server_id,
            dungeon_id,
            SUM(CASE WHEN event_type = 'enter_dungeon' THEN 1 ELSE 0 END) AS enter_cnt,
            SUM(CASE WHEN event_type = 'clear_dungeon' THEN 1 ELSE 0 END) AS clear_cnt,
            SUM(CASE WHEN event_type = 'death' THEN 1 ELSE 0 END) AS death_cnt
        FROM dwd.player_events_clean
        WHERE dungeon_id IS NOT NULL
        GROUP BY dt, server_id, dungeon_id
        """
    )
    # first_seen: first *observed* activity day per (player_id, server_id).
    # first_dt = MIN(dt) over ALL event types in the dataset — NOT registration-
    # only / create_role-only unless upstream filters to that event_type.
    con.execute(
        """
        CREATE OR REPLACE TABLE dws.player_first_seen AS
        SELECT
            player_id,
            server_id,
            MIN(dt) AS first_dt,
            MIN(event_time) AS first_event_time
        FROM dwd.player_events_clean
        GROUP BY player_id, server_id
        """
    )
    dws_dir = Path(paths["dws_dir"])
    for tbl in ("player_behavior_di", "dungeon_behavior_di", "player_first_seen"):
        con.execute(
            f"COPY dws.{tbl} TO '{(dws_dir / (tbl + '.parquet')).as_posix()}' (FORMAT PARQUET)"
        )
    print(f"      done in {time.time() - t0:.1f}s")


def step_ads(con: duckdb.DuckDBPyConnection, paths: dict) -> None:
    print("[4/5] ADS metrics")
    t0 = time.time()
    ads_dir = Path(paths["ads_dir"])
    ads_dir.mkdir(parents=True, exist_ok=True)

    # DAU: distinct players with any event that day
    con.execute(
        """
        CREATE OR REPLACE TABLE ads.ads_dau_di AS
        SELECT
            dt,
            server_id,
            COUNT(DISTINCT player_id) AS dau,
            'ads_dau_di' AS metric_id
        FROM dws.player_behavior_di
        GROUP BY dt, server_id
        """
    )

    # Retention N-day (1/3/7) based on first_seen cohort.
    # window_complete: only emit when max activity dt >= cohort_dt + n_days.
    con.execute(
        """
        CREATE OR REPLACE TABLE ads.ads_retention_nd AS
        WITH cohorts AS (
            SELECT player_id, server_id, first_dt AS cohort_dt
            FROM dws.player_first_seen
        ),
        activity AS (
            SELECT DISTINCT player_id, server_id, dt
            FROM dws.player_behavior_di
        ),
        bounds AS (
            SELECT MAX(dt) AS max_dt FROM dws.player_behavior_di
        ),
        exploded AS (
            SELECT c.cohort_dt, c.server_id, c.player_id, n.n_days
            FROM cohorts c
            CROSS JOIN (SELECT 1 AS n_days UNION ALL SELECT 3 UNION ALL SELECT 7) n
        )
        SELECT
            e.cohort_dt,
            e.server_id,
            e.n_days,
            COUNT(DISTINCT e.player_id) AS cohort_size,
            COUNT(DISTINCT CASE WHEN a.player_id IS NOT NULL THEN e.player_id END) AS retained_cnt,
            CASE WHEN COUNT(DISTINCT e.player_id) = 0 THEN NULL
                 ELSE COUNT(DISTINCT CASE WHEN a.player_id IS NOT NULL THEN e.player_id END) * 1.0
                      / COUNT(DISTINCT e.player_id)
            END AS retention_rate,
            TRUE AS window_complete,
            'ads_retention_nd' AS metric_id
        FROM exploded e
        CROSS JOIN bounds b
        LEFT JOIN activity a
          ON e.player_id = a.player_id
         AND e.server_id = a.server_id
         AND a.dt = e.cohort_dt + e.n_days
        WHERE b.max_dt IS NOT NULL
          AND b.max_dt >= e.cohort_dt + e.n_days
        GROUP BY e.cohort_dt, e.server_id, e.n_days
        """
    )

    # Online duration
    con.execute(
        """
        CREATE OR REPLACE TABLE ads.ads_online_duration_di AS
        SELECT
            p.dt,
            p.server_id,
            SUM(p.online_sec_sum) AS total_online_sec,
            COUNT(DISTINCT p.player_id) AS players,
            CASE WHEN COUNT(DISTINCT p.player_id) = 0 THEN NULL
                 ELSE SUM(p.online_sec_sum) * 1.0 / COUNT(DISTINCT p.player_id)
            END AS avg_online_sec,
            'ads_online_duration_di' AS metric_id
        FROM dws.player_behavior_di p
        GROUP BY p.dt, p.server_id
        """
    )

    # Pay rate
    con.execute(
        """
        CREATE OR REPLACE TABLE ads.ads_pay_rate_di AS
        SELECT
            p.dt,
            p.server_id,
            COUNT(DISTINCT p.player_id) AS dau,
            COUNT(DISTINCT CASE WHEN p.recharge_cnt > 0 THEN p.player_id END) AS pay_users,
            CASE WHEN COUNT(DISTINCT p.player_id) = 0 THEN NULL
                 ELSE COUNT(DISTINCT CASE WHEN p.recharge_cnt > 0 THEN p.player_id END) * 1.0
                      / COUNT(DISTINCT p.player_id)
            END AS pay_rate,
            'ads_pay_rate_di' AS metric_id
        FROM dws.player_behavior_di p
        GROUP BY p.dt, p.server_id
        """
    )

    # ARPU (CNY)
    con.execute(
        """
        CREATE OR REPLACE TABLE ads.ads_arpu_di AS
        SELECT
            p.dt,
            p.server_id,
            COUNT(DISTINCT p.player_id) AS dau,
            SUM(p.recharge_fen) / 100.0 AS revenue_cny,
            CASE WHEN COUNT(DISTINCT p.player_id) = 0 THEN NULL
                 ELSE (SUM(p.recharge_fen) / 100.0) / COUNT(DISTINCT p.player_id)
            END AS arpu_cny,
            'ads_arpu_di' AS metric_id
        FROM dws.player_behavior_di p
        GROUP BY p.dt, p.server_id
        """
    )

    # Dungeon clear rate
    con.execute(
        """
        CREATE OR REPLACE TABLE ads.ads_dungeon_clear_rate_di AS
        SELECT
            dt,
            server_id,
            dungeon_id,
            enter_cnt,
            clear_cnt,
            CASE WHEN enter_cnt = 0 THEN NULL ELSE clear_cnt * 1.0 / enter_cnt END AS clear_rate,
            'ads_dungeon_clear_rate_di' AS metric_id
        FROM dws.dungeon_behavior_di
        """
    )

    # Churn: active in last 7d window ending dt, silent in last 3d
    # Approximate with available demo days: for each dt, look back
    con.execute(
        """
        CREATE OR REPLACE TABLE ads.ads_churn_di AS
        WITH bounds AS (
            SELECT MIN(dt) AS min_dt, MAX(dt) AS max_dt FROM dws.player_behavior_di
        ),
        days AS (
            SELECT DISTINCT dt, server_id FROM dws.player_behavior_di
        ),
        active_7 AS (
            SELECT d.dt, d.server_id, p.player_id
            FROM days d
            JOIN dws.player_behavior_di p
              ON p.server_id = d.server_id
             AND p.dt BETWEEN d.dt - 6 AND d.dt
            GROUP BY d.dt, d.server_id, p.player_id
        ),
        active_3 AS (
            SELECT d.dt, d.server_id, p.player_id
            FROM days d
            JOIN dws.player_behavior_di p
              ON p.server_id = d.server_id
             AND p.dt BETWEEN d.dt - 2 AND d.dt
            GROUP BY d.dt, d.server_id, p.player_id
        )
        SELECT
            a7.dt,
            a7.server_id,
            COUNT(DISTINCT a7.player_id) AS active_7d_users,
            COUNT(DISTINCT CASE WHEN a3.player_id IS NULL THEN a7.player_id END) AS churn_risk_users,
            CASE WHEN COUNT(DISTINCT a7.player_id) = 0 THEN NULL
                 ELSE COUNT(DISTINCT CASE WHEN a3.player_id IS NULL THEN a7.player_id END) * 1.0
                      / COUNT(DISTINCT a7.player_id)
            END AS churn_risk_rate,
            'ads_churn_di' AS metric_id
        FROM active_7 a7
        LEFT JOIN active_3 a3
          ON a7.dt = a3.dt AND a7.server_id = a3.server_id AND a7.player_id = a3.player_id
        GROUP BY a7.dt, a7.server_id
        """
    )

    for tbl in (
        "ads_dau_di",
        "ads_retention_nd",
        "ads_online_duration_di",
        "ads_pay_rate_di",
        "ads_arpu_di",
        "ads_dungeon_clear_rate_di",
        "ads_churn_di",
    ):
        con.execute(
            f"COPY ads.{tbl} TO '{(ads_dir / (tbl + '.parquet')).as_posix()}' (FORMAT PARQUET)"
        )
    print(f"      done in {time.time() - t0:.1f}s")


def step_summary(con: duckdb.DuckDBPyConnection) -> None:
    print("[5/5] Summary snapshot")
    rows = con.execute(
        """
        SELECT dt, SUM(dau) AS dau_all
        FROM ads.ads_dau_di
        GROUP BY dt
        ORDER BY dt
        """
    ).fetchall()
    print("      DAU by day (all servers):")
    for r in rows:
        print(f"        {r[0]}  dau={r[1]}")
    pay = con.execute(
        """
        SELECT ROUND(AVG(pay_rate), 4) AS avg_pay_rate,
               ROUND(AVG(arpu_cny), 4) AS avg_arpu
        FROM ads.ads_pay_rate_di p
        JOIN ads.ads_arpu_di a USING (dt, server_id)
        """
    ).fetchone()
    print(f"      avg pay_rate={pay[0]}  avg arpu_cny={pay[1]}  (demo synthetic data)")


def main() -> int:
    paths = load_yaml("paths.yaml")["paths"]
    demo = load_yaml("topics.yaml").get("demo", {})

    ap = argparse.ArgumentParser(description="GameStream local lite runner")
    ap.add_argument("--players", type=int, default=demo.get("players", 10_000))
    ap.add_argument("--events", type=int, default=demo.get("events", 200_000))
    ap.add_argument("--servers", type=int, default=demo.get("servers", 8))
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--seed", type=int, default=demo.get("seed", 42))
    ap.add_argument("--out", type=str, default=None)
    ap.add_argument("--skip-simulate", action="store_true")
    ap.add_argument("--duckdb", type=str, default=None)
    args = ap.parse_args()

    # Resolve relative paths from repo root
    import os

    os.chdir(ROOT)
    ensure_dirs(paths)

    raw = Path(args.out or paths["raw_jsonl"])
    if not args.skip_simulate:
        step_simulate(args, paths)
    elif not raw.exists():
        print(f"ERROR: --skip-simulate but missing {raw}", file=sys.stderr)
        return 1

    db_path = args.duckdb or paths["duckdb"]
    con = duckdb.connect(db_path)
    try:
        step_ods_dwd(con, paths, raw)
        step_dws(con, paths)
        step_ads(con, paths)
        step_summary(con)
    finally:
        con.close()

    print(f"\n[OK] DuckDB → {db_path}")
    print(f"[OK] Parquet under data/ods|dwd|dws|ads")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
