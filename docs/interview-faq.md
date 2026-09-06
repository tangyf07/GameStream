# GameStream 二面 FAQ（Kafka / Flink / 口径）

短答 + 仓库锚点。深挖长文见 [`flink-kafka-deep-dive.md`](flink-kafka-deep-dive.md)。**不要背造 Lag/Checkpoint/P95 数字。**

---

### 1. 为什么用事件时间 Watermark，而不是处理时间？

**短答：** 游戏埋点乱序（弱网重传、客户端时钟），窗口必须按 `event_time` 关窗；处理时间只反映作业墙钟，活动补报会进错日。  
**锚点：** `flink/sql/01_ods_clean.sql`（`event_time - 5s`）、`02_dwd_dws_realtime.sql`（`10s`）；深挖 §2、§5。

### 2. ODS 5s、DWS 10s 乱序界怎么选？

**短答：** ODS 靠近源、清洗轻，5s 够挡常见乱序；DWS/ADS 再放宽一点给上游链路缓冲。再大要靠 lateness / 批补数，而不是无限加大 watermark。  
**锚点：** 同上两个 SQL；迟到策略 `docs/flink-kafka-deep-dive.md` §11。

### 3. 稀疏游戏服会导致什么？怎么处理？

**短答：** 某分区长期无数据 → 全局 watermark 取 min 被拖住 → 日窗不触发。用 source **idleness**、过滤坏时钟，或避免「一服一分区且无空闲策略」。  
**锚点：** deep-dive §5；demo 服数 `config/topics.yaml` `demo.servers`。

### 4. DAU 如何去重？故障重放会不会翻倍？

**短答：** ADS 对当日 `player_id` 做 `COUNT(DISTINCT …)`；重放前 DWD 已用 `event_id` upsert 幂等，同一事件不会变成两个玩家。  
**锚点：** `flink/sql/01_ods_clean.sql`（PK `event_id`）、`03_ads_realtime.sql`（`ads_dau_di_rt`）；口径 `config/metrics.yaml` → `ads_dau_di`；批 SQL `sql/metrics/ads_dau_di.sql`。

### 5. 数据倾斜怎么讲（游戏场景）？

**短答：** 热点在大 R、活动服、爆款 `dungeon_id`。键用 `player_id`、两阶段 TopN、盐拆 key、副本先按 `server_id, dungeon_id` 预聚合；必要时热点旁路。  
**锚点：** deep-dive §10；副本聚合 `flink/sql/03_ads_realtime.sql`。

### 6. 迟到数据怎么办？

**短答：** 窗口允许短 lateness + upsert 修正看板；超窗进 side output，交给 Spark/DuckDB 批回刷。实时 DAU/付费可订正；留存不硬扛在流上。  
**锚点：** deep-dive §11；批入口 `spark/jobs/dws_ads_batch.py`；`03_ads_realtime.sql` 末尾注释。

### 7. 为什么留存 / 流失走批而不是 Flink 日窗？

**短答：** N 日留存要 cohort 跨天 lookback，状态与订正成本高；流失要「近 7 日活跃 × 近 3 日沉默」滑动判定，批 SQL 更稳、易对账。  
**锚点：** `sql/metrics/ads_retention_nd.sql`、`ads_churn_di.sql`；`config/metrics.yaml`；`flink/sql/03_ads_realtime.sql`（明确不算 retention/churn）。

### 8. upsert-kafka vs append-kafka 怎么选？

**短答：** 需要按主键覆盖、扛重放 → upsert（DWD `event_id`、DWS/ADS 日粒度指标）；需要不可变日志/审计明细 → append，并另做去重或 EO 事务。  
**锚点：** `01_ods_clean.sql`、`02_dwd_dws_realtime.sql`、`03_ads_realtime.sql` 的 WITH；deep-dive §3。

### 9. Checkpoint 和 Savepoint 区别？改并行度怎么做？

**短答：** Checkpoint 自动故障恢复；Savepoint 人工触发，用于升级/改并行度。流程：停作业取 savepoint → 改 parallelism → `-s` 恢复，保持算子 uid。  
**锚点：** deep-dive §2、§7；配置示例 `flink/conf/checkpoint-recommendations.yaml`。

### 10. RocksDB + 增量 checkpoint + TTL 各解决什么？

**短答：** RocksDB 扛海量 `player_id` 状态防堆 OOM；增量减小每次上传量；TTL 清理 session/去重等短生命周期状态，防流失玩家 key 永驻。  
**锚点：** deep-dive §6；`flink/conf/checkpoint-recommendations.yaml`。

### 11. 反压时 watermark / checkpoint 各有什么现象？

**短答：** 反压 → 处理变慢 → barrier 对齐变慢 → checkpoint 超时；同时事件时间推进慢，窗口推迟。先看 UI 哪段 busy、Kafka lag、sink 是否堵。  
**锚点：** deep-dive §4；勿编造本机未测的 lag 数字。

### 12. 充值金额如何避免重放双计？

**短答：** 进入 `SUM(amount_fen)` 前必须按 `event_id`（或支付订单号）幂等；本仓 ODS→DWD upsert 是闸。裸 at-least-once 累加 = 资金事故。  
**锚点：** deep-dive §8；`01_ods_clean.sql`；ARPU `03_ads_realtime.sql` / `sql/metrics/ads_arpu_di.sql`。

### 13. login→logout 在线时长怎么算？

**短答：** 口径上 session 配对；实现上 DWS 对 `logout.online_sec` 求和，再 ADS 人均。Flink 也可用 Keyed state 做 sessionize，TTL 管未闭合会话。  
**锚点：** `config/metrics.yaml` → `ads_online_duration_di`；`02_dwd_dws_realtime.sql`（`online_sec_sum`）；`sql/metrics/ads_online_duration_di.sql`。

### 14. Iceberg vs Doris 怎么取舍？

**短答：** Iceberg：DWD/明细、长周期回刷、schema 演进、时间旅行；Doris：ADS 点查与看板 UNIQUE 覆盖写。训练/审计可落 Iceberg 同构 ADS。  
**锚点：** `sql/ddl/iceberg_notes.md`、`sql/ddl/doris_ads.sql`。

### 15. 与 DataPilot / SQLGuard 如何对接指标？

**短答：** 稳定 `metric_id` + 约定维度；SQL 文本在 `sql/metrics/`；流批 ADS 都带同一 `metric_id` 字段，供外部引用与校验。  
**锚点：** `config/metrics.yaml`（含 `datapilot_contract`）；`flink/sql/03_ads_realtime.sql` 各 INSERT 的 `metric_id`；Doris 表同名字段。

### 16. lite DuckDB 和生产 Flink 差在哪（面试怎么说）？

**短答：** lite 对齐口径、可演示；没有 checkpoint 恢复、真实 lag、watermark/idleness、2PC。先讲 Kafka→Flink→Doris，再提 DuckDB 沙盒。  
**锚点：** deep-dive §9；`pipeline/local_runner.py`；`ARCHITECTURE.md`。

### 17. 从 ODS 到 ADS 作业怎么提交？

**短答：** 顺序跑三份 SQL，或 PyFlink stub：`ods_clean_job.py` → `dws_ads_submit.py`（02+03）。集群与 Kafka 需就绪；本机无集群用 local_runner。  
**锚点：** `flink/jobs/README.md`、`flink/jobs/ods_clean_job.py`、`flink/jobs/dws_ads_submit.py`；`flink/sql/01|02|03_*.sql`。

### 18. Topic / 消费组在哪里查？

**短答：** Topic 名统一在 `config/topics.yaml`；Flink `group.id` 写在各 SQL WITH（如 `gamestream-ods-clean`、`gamestream-dws-rt`、`gamestream-ads-dungeon-rt`）。  
**锚点：** `config/topics.yaml`；`flink/sql/*.sql`。
