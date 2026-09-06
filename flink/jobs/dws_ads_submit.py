"""
PyFlink 提交入口：DWS 日窗 + 近实时 ADS（需 Flink 集群 + Kafka）。

镜像 ods_clean_job.py：按序加载
  - flink/sql/02_dwd_dws_realtime.sql  （Tumble 1d keyed DWS）
  - flink/sql/03_ads_realtime.sql      （DAU / 付费 / ARPU / 在线 / 副本通关）

面试叙事：
  1) 生产先跑 01 ODS clean（event_id upsert），再本入口 02→03
  2) Checkpoint / RocksDB 见 flink/conf/checkpoint-recommendations.yaml（示例键，非实测）
  3) 留存/流失仍走 Spark + sql/metrics（03 SQL 不算）
  4) 本地无集群：pipeline/local_runner.py 同口径

相关：docs/flink-kafka-deep-dive.md · docs/interview-faq.md
"""
from __future__ import annotations

from pathlib import Path

SQL_FILES = (
    "02_dwd_dws_realtime.sql",
    "03_ads_realtime.sql",
)


def _exec_sql_file(t_env, path: Path) -> int:
    raw = path.read_text(encoding="utf-8")
    n = 0
    for stmt in raw.split(";"):
        s = stmt.strip()
        if not s:
            continue
        lines = [ln for ln in s.splitlines() if not ln.strip().startswith("--")]
        cleaned = "\n".join(lines).strip()
        if cleaned:
            t_env.execute_sql(cleaned)
            n += 1
    return n


def main() -> None:
    try:
        from pyflink.table import EnvironmentSettings, TableEnvironment
    except ImportError as e:
        raise SystemExit(
            "PyFlink not installed. Production: submit on Flink cluster.\n"
            "Local demo (same metrics口径): python pipeline/local_runner.py\n"
            f"Import error: {e}"
        )

    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)
    # 示例（部署时用 flink-conf 或此处 set；值见 conf 注释，勿当实测 SLA）：
    # t_env.get_config().set("execution.checkpointing.interval", "60s")
    # t_env.get_config().set("state.backend", "rocksdb")

    sql_dir = Path(__file__).resolve().parents[1] / "sql"
    total = 0
    for name in SQL_FILES:
        path = sql_dir / name
        if not path.is_file():
            raise SystemExit(f"Missing SQL file: {path}")
        total += _exec_sql_file(t_env, path)
        print(f"Submitted {name}")

    print(f"Done: {total} statements from {list(SQL_FILES)} (cluster must be up).")
    print("Retention/churn: spark/jobs/dws_ads_batch.py + sql/metrics/")


if __name__ == "__main__":
    main()
