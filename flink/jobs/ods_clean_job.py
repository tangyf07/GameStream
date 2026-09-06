"""
PyFlink 提交入口：ODS clean（需 Flink 集群 + Kafka）。

面试叙事（不要讲成 DuckDB-only）：
  1) 生产：sql-client / 本入口加载 flink/sql/01_ods_clean.sql
     Kafka source → watermark → 过滤 11 类型 → upsert-kafka DWD（event_id 幂等）
  2) Checkpoint：开启后故障从 offset+状态恢复；sink 用 upsert 扛至少一次重放
  3) 本地 lite：无集群时用 pipeline/local_runner.py 跑同口径，便于 Windows 演示

相关：
  - flink/sql/02_dwd_dws_realtime.sql  日窗 DWS（keyed state）
  - flink/sql/03_ads_realtime.sql      近实时 DAU/付费/ARPU/通关/在线
  - docs/flink-kafka-deep-dive.md      分区/语义/倾斜/迟到/恢复
"""
from __future__ import annotations

from pathlib import Path


def main() -> None:
    try:
        from pyflink.table import EnvironmentSettings, TableEnvironment
    except ImportError as e:
        raise SystemExit(
            "PyFlink not installed. Production: submit on Flink cluster.\n"
            "Local demo (same metrics口径): python pipeline/local_runner.py\n"
            f"Import error: {e}"
        )

    # Streaming + 建议在 flink-conf 配 checkpoint.interval / state.backend=rocksdb
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)
    # 示例：t_env.get_config().set("execution.checkpointing.interval", "60s")

    path = Path(__file__).resolve().parents[1] / "sql" / "01_ods_clean.sql"
    statements = path.read_text(encoding="utf-8").split(";")
    for stmt in statements:
        s = stmt.strip()
        if not s:
            continue
        lines = [ln for ln in s.splitlines() if not ln.strip().startswith("--")]
        cleaned = "\n".join(lines).strip()
        if cleaned:
            t_env.execute_sql(cleaned)
    print("Submitted ODS clean statements (cluster must be up).")
    print("Next: sql/02_dwd_dws_realtime.sql → sql/03_ads_realtime.sql")


if __name__ == "__main__":
    main()
