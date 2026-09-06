# DataPilot / SQLGuard 指标契约

供 [DataPilot](https://github.com/tangyf07/DataPilot) 做 Schema/指标 RAG，经 [sql-write-gate](https://github.com/tangyf07/sql-write-gate) 只读查询。权威源：本文件 + `config/metrics.yaml` + `sql/metrics/*.sql`。

## 约定

| 字段 | 说明 |
|------|------|
| `metric_id` | 稳定主键，跨 DuckDB / Flink ADS / Doris 一致 |
| `table` | ADS 物理表名（DuckDB `ads.<table>`；Doris `ads.<table>`） |
| `sql_file` | 批口径 SQL |
| `flink_rt` | 是否有近实时 Flink（`flink/sql/03_ads_realtime.sql`） |
| `engine_lite` | DuckDB local_runner |
| `engine_batch` | Spark `spark/jobs/dws_ads_batch.py` 或 DuckDB 批 |

允许维度：`dt`, `server_id`, `dungeon_id`, `n_days`, `cohort_dt`。  
SQLGuard：默认只允许 `SELECT` 打 ADS；禁止写 ODS/DWD。

## 清单（核对一致）

| metric_id | table | sql_file | flink_rt | 备注 |
|-----------|-------|----------|----------|------|
| `ads_dau_di` | `ads.ads_dau_di` | `sql/metrics/ads_dau_di.sql` | yes | 当日去重 player |
| `ads_retention_nd` | `ads.ads_retention_nd` | `sql/metrics/ads_retention_nd.sql` | **no（批）** | n_days=1/3/7 |
| `ads_online_duration_di` | `ads.ads_online_duration_di` | `sql/metrics/ads_online_duration_di.sql` | yes | 秒；login→logout |
| `ads_pay_rate_di` | `ads.ads_pay_rate_di` | `sql/metrics/ads_pay_rate_di.sql` | yes | 付费人数/DAU |
| `ads_arpu_di` | `ads.ads_arpu_di` | `sql/metrics/ads_arpu_di.sql` | yes | 分→元 / DAU |
| `ads_dungeon_clear_rate_di` | `ads.ads_dungeon_clear_rate_di` | `sql/metrics/ads_dungeon_clear_rate_di.sql` | yes | 含 dungeon_id |
| `ads_churn_di` | `ads.ads_churn_di` | `sql/metrics/ads_churn_di.sql` | **no（批）** | 7 活 3 沉 |

## DataPilot 查询示例（只读）

```sql
SELECT dt, server_id, dau, metric_id
FROM ads.ads_dau_di
WHERE dt = DATE '2026-09-01' AND metric_id = 'ads_dau_di';
```

口径公式以 `config/metrics.yaml` 为准；改口径必须同步 yaml、sql/metrics、Flink ADS、本表四者。
