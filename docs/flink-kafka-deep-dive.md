# Flink × Kafka 面试深挖（对照本仓库）

定位：GameStream 是游戏行为实时链路，不是「把数导进 DuckDB」的故事。本地 lite 用 DuckDB **镜像口径**；生产形状在 `flink/sql`、`flink/jobs`、`docker-compose.yml`（Redpanda/Flink）。

---

## 1. Kafka：分区、消费组、语义

### 分区（Partition）

- Topic 按 partition 并行；**同一 key 进同一分区**才能保证该 key 的局部有序。
- GameStream：建议 `key = player_id`（或 `server_id:player_id`），保证同一玩家事件有序，便于 session 配对、去重窗口。
- 本仓 topic 名见 `config/topics.yaml`：
  - `gamestream.ods.player_events`（原始）
  - `gamestream.dwd.player_events_clean`
  - `gamestream.dws.player_behavior_di`
  - `gamestream.ads.metrics_stream`
- Lite：`pipeline/kafka_io.FileTopic` 用 `data/topics/<topic>/p{N}/*.jsonl` 模拟单分区 append-log。

### 消费组（Consumer Group）

- 一组 consumer 共享 `group.id`，每个 partition 同一时刻只被组内一个成员消费 → 水平扩展。
- Flink Kafka source 的 `properties.group.id`（见 `flink/sql/01_ods_clean.sql` 的 `gamestream-ods-clean`）用于 **offset 提交与再平衡**；Flink 真正的一致性更多依赖 **checkpoint 存的 offset**，而不是「只靠 broker 自动提交」。
- 作业升级/扩缩容：换 `group.id` 或从 savepoint 恢复，避免和旧作业抢同一组。

### 投递语义

| 语义 | 含义 | GameStream 实践 |
|------|------|-----------------|
| At-most-once | 丢可不重 | 观测类可接受，指标不推荐 |
| At-least-once | 可重不丢 | 默认；需下游幂等 |
| Exactly-once | 端到端一次 | Flink checkpoint + Kafka transactional / upsert-kafka 主键幂等 |

本仓 ODS→DWD：`PRIMARY KEY (event_id) NOT ENFORCED` + `upsert-kafka`（`01_ods_clean.sql`），用 **event_id 幂等** 扛 at-least-once 重复。

生产还要约定：producer `acks=all`、合适 `linger.ms`、分区数 ≥ 并行度，避免单分区热点（大 R 服 / 活动服）。

---

## 2. Flink：Watermark、Keyed State、Checkpoint / Savepoint、故障恢复

### Watermark（事件时间）

- 本仓：`WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND`（ODS）/ `'10' SECOND`（DWS），允许乱序 bounded out-of-orderness。
- Watermark =「小于等于该时间的数据大体到齐」；窗口触发依赖 watermark 推进，不是墙钟。
- 游戏场景：客户端改时、弱网重传 → 乱序常见；副本结算晚到几秒～几十秒要单独评估 idle source / 允许延迟。

对照：`flink/sql/01_ods_clean.sql`、`02_dwd_dws_realtime.sql`。

### Keyed State

- `keyBy(player_id)` / SQL `GROUP BY player_id, server_id` 后的状态（ValueState、MapState、窗口累加器）挂在 key 上。
- DWS 日窗：`TUMBLE(..., INTERVAL '1' DAY) GROUP BY server_id, player_id` → 每玩家每日一份聚合状态（`02_dwd_dws_realtime.sql`）。
- 面试点：状态后端 RocksDB vs HashMap；大 key 状态 → TTL、增量 checkpoint；热点 key（头部玩家）见倾斜一节。

### Checkpoint vs Savepoint

| | Checkpoint | Savepoint |
|--|------------|-----------|
| 触发 | 周期自动 | 人工/运维 |
| 用途 | 故障自动恢复 | 停机升级、改并行度、迁移 |
| 保留 | 通常滚动覆盖 | 显式保留直到删除 |

- Exactly-once 到 Kafka：checkpoint 屏障对齐 + 两阶段提交（Kafka sink transactional）。
- 本仓 upsert-kafka：以主键覆盖，语义接近「幂等 exactly-once 效果」，实现更简单，适合 ADS 覆盖写。

### 故障恢复

1. JM 从最近完成的 checkpoint 恢复算子状态与 Kafka 源 offset。
2. 下游幂等（event_id / ADS 主键）吸收重放。
3. 长时间失败：从 savepoint 冷启动；或从 Kafka earliest + 状态重建（贵）。
4. 对照 lite：DuckDB 批跑无状态恢复问题，但 **口径必须与 Flink 窗口定义一致**，否则面试会被问「两边对不上怎么办」。

入口 stub：`flink/jobs/ods_clean_job.py`（读 `flink/sql/01_ods_clean.sql` 提交 TableEnvironment）。

---

## 3. 数据倾斜治理（游戏行为特有）

现象：头部公会/活动服/爆款副本导致某 `player_id` 或 `dungeon_id` 流量远高于均值 → 单 task 堆积。

手段（由易到难）：

1. **分区键设计**：不要只用 `server_id`；玩家级聚合用 `player_id`，全局 TopN 用两阶段（局部 TopN → 全局）。
2. **盐拆 key**：`concat(player_id, '_', salt)` 打散，再二次聚合；适合短时尖刺。
3. **旁路热点**：识别超大 R，单独并行度/单独 topic。
4. **mini-batch / 局部聚合**：Flink miniBatch 降低状态更新频率。
5. **副本维度**：`dungeon_id` 倾斜时先按 `server_id, dungeon_id` 预聚合（本仓 DWS `dungeon_behavior_di` 思路）。

本仓本地 DuckDB 无倾斜问题；生产 Flink 面试应能把上述方案讲到「为何对游戏埋点有效」。

---

## 4. Late data（迟到数据）

- Allowed lateness：窗口关闭后仍接收一段时间迟到事件，更新结果（需 retract / upsert sink）。
- 超过 lateness：进 side output，落「补数通道」→ Spark/批作业回刷 ADS（本仓 `spark/jobs/dws_ads_batch.py` 承担留存/流失等长窗口回看）。
- 业务取舍：
  - **实时 DAU / 付费**：可接受分钟级修正（upsert）。
  - **留存 / 流失**：天然依赖多日窗口，适合批（`sql/metrics/ads_retention_nd.sql`、`ads_churn_di.sql`），Flink 只做近实时预览。

`03_ads_realtime.sql` 明确：**Retention & churn remain Spark/DuckDB batch**。

---

## 5. 与本仓库 SQL / Job 的对应关系

| 能力 | 生产产物 | Lite 镜像 |
|------|----------|-----------|
| ODS 清洗 + 11 类型过滤 + event_id | `flink/sql/01_ods_clean.sql`，Job stub `flink/jobs/ods_clean_job.py` | `local_runner.step_ods_dwd` |
| 日窗玩家行为 DWS | `flink/sql/02_dwd_dws_realtime.sql`（Tumble 1 day） | `step_dws` → `dws.player_behavior_di` |
| 实时 DAU / 付费率 / 通关率 | `flink/sql/03_ads_realtime.sql` | `step_ads` 全量 ADS |
| 留存 / 流失 / ARPU 批 | Spark + `sql/metrics/*.sql` | 同 DuckDB ADS |
| Topic / group.id | `config/topics.yaml` + Flink WITH 子句 | `FileTopic` |
| OLAP 外表 | `sql/ddl/doris_ads.sql`、Iceberg notes | Parquet / DuckDB |

面试叙事建议：**先讲 prod 链路（Kafka→Flink→Doris），再说明 lite 是可跑通的口径沙盒**，避免被理解成「只会 DuckDB」。

---

## 6. 快速自测问题（自问自答）

1. 为什么 watermark 是 `event_time - 5s` 而不是处理时间？→ 要对齐业务事件时间，抗乱序。
2. 重复消费会不会把 DAU 算双？→ DWD 按 `event_id` 幂等；DAU 是当日去重 player_id，重放同一事件不增。
3. 为何留存不放 Flink 日窗？→ 需 cohort 跨天 lookback，状态大、修正多，批更稳。
4. Checkpoint 失败怎么排查？→ 屏障对齐超时、状态过大、外部 sink 两阶段提交、反压。
