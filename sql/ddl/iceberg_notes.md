# Iceberg 表设计笔记（游戏行为湖仓）

与 Doris ADS 互补：Iceberg 扛 **明细 / DWD / 长周期回刷**；Doris 扛 **ADS 点查与看板**。

## 建议表

### `dwd.player_events_clean`（Iceberg）

| 字段 | 类型 | 说明 |
|------|------|------|
| event_id | string | 主键语义，写入幂等 |
| event_type | string | 11 类枚举 |
| event_time | timestamptz | 事件时间 |
| dt | date | 分区列 |
| player_id | long | |
| role_id | long | |
| server_id | int | 可选二级分区 / 桶 |
| session_id | string | |
| dungeon_id | int | 可空 |
| amount_fen | long | recharge |
| online_sec | int | logout |
| payload | string/json | 扩展 |

**分区**：`dt`（日）+ 可选 `bucket(player_id, 16)` 或 `server_id`。  
**写法**：Flink Iceberg sink upsert on `event_id`，或 append + 下游 MERGE。  
**演进**：payload 新字段用 Iceberg schema evolution，避免改 Doris 宽表。

### `dws.player_behavior_di`（Iceberg）

分区 `dt`；度量与 `sql/ddl/03_dws.sql` 一致。Spark 批 / Flink 日窗结果落地此处，再计算留存流失（跨分区 scan）。

### ADS 是否进 Iceberg？

- 短周期看板 → Doris（`doris_ads.sql`）。
- 训练特征 / 长期审计 → Iceberg `ads_*` 同构表，分区 `dt`，格式 Parquet，压缩 zstd。

## 与本仓路径映射

| Lake 层 | 本地 lite | 生产 |
|---------|-----------|------|
| ODS/DWD parquet | `data/ods` `data/dwd` | Iceberg `dwd.player_events_clean` |
| DWS | `data/dws/*.parquet` | Iceberg + 可选同步 Doris DWS |
| ADS | `data/ads/*.parquet` + DuckDB | Doris UNIQUE 表为主 |

时间旅行：排查「某日口径变更」时用 Iceberg snapshot/time travel 对比重跑结果；Doris 侧靠分区覆盖 + 变更日志。
