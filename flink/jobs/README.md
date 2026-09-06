# Flink Jobs（生产形状）

| Job | SQL | 要点 |
|-----|-----|------|
| ods_clean | `sql/01_ods_clean.sql` | Kafka raw → DWD；watermark 5s；`event_id` upsert 幂等；11 事件类型 |
| dws_realtime | `sql/02_dwd_dws_realtime.sql` | Tumble 1d keyed 聚合；watermark 10s；玩家日 DWS |
| ads_realtime | `sql/03_ads_realtime.sql` | 近实时 DAU / 付费率 / ARPU / 在线 / 副本通关；留存流失走批 |

## 提交

```bash
# SQL Client
sql-client.sh -f flink/sql/01_ods_clean.sql
sql-client.sh -f flink/sql/02_dwd_dws_realtime.sql
sql-client.sh -f flink/sql/03_ads_realtime.sql

# 或 PyFlink stub
flink run -py flink/jobs/ods_clean_job.py
```

## 与 lite 的关系

本地 Windows **不需要** Flink：`pipeline/local_runner.py` + DuckDB 产出同 metric_id。  
面试时应先讲本目录链路（Kafka→Flink→Doris），再说明 lite 是口径沙盒。  
深挖：`docs/flink-kafka-deep-dive.md`。
