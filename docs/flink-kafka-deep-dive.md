# Flink × Kafka 面试深挖（对照本仓库）

定位：GameStream 是游戏行为实时链路，不是「把数导进 DuckDB」的故事。本地 lite 用 DuckDB **镜像口径**；生产形状在 `flink/sql`、`flink/jobs`、`flink/conf`、`docker-compose.yml`（Redpanda/Flink）。

配套：[`docs/interview-faq.md`](interview-faq.md)（二面短答）· 配置示例 [`flink/conf/checkpoint-recommendations.yaml`](../flink/conf/checkpoint-recommendations.yaml)

---

## 1. Kafka：分区、消费组、语义

### 分区（Partition）

- Topic 按 partition 并行；**同一 key 进同一分区**才能保证该 key 的局部有序。
- GameStream：建议 `key = player_id`（或 `server_id:player_id`），保证同一玩家事件有序，便于 session 配对、去重窗口。
- 本仓 topic 名见 `config/topics.yaml`：
  - `gamestream.ods.player_events`（原始）
  - `gamestream.dwd.player_events_clean`
  - `gamestream.dws.player_behavior_di`
  - `gamestream.ads.metrics_stream`（及 pay_rate / arpu / online / dungeon 分 topic）
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

入口 stub：`flink/jobs/ods_clean_job.py`、`flink/jobs/dws_ads_submit.py`。

---

## 3. Checkpoint barrier 对齐 / 非对齐 / Kafka sink 2PC

### Barrier 对齐（Aligned Checkpoint）

1. JM 向所有 source 注入 **checkpoint barrier**（带 checkpoint id）。
2. 算子收到某输入通道的 barrier 后，**阻塞该通道**，等其它输入通道的同 id barrier 到齐（对齐）。
3. 对齐后对该算子状态做 snapshot，再把 barrier 向下游转发。
4. 全部算子完成 → checkpoint 成功；Kafka source 把 **消费 offset** 写入状态；Kafka transactional sink 进入 **预提交**。

游戏链路里 ODS→DWS→ADS 多级作业各自独立 checkpoint；跨作业靠 upsert 主键幂等衔接，而不是一个全局分布式事务。

### 非对齐 Checkpoint（Unaligned）

- 反压严重时，对齐会等慢通道 → checkpoint 超时。
- Unaligned：barrier 越过未处理完的 in-flight 数据，把 **缓冲区里的 inflight 记录**一并写入 checkpoint，减少阻塞。
- 代价：checkpoint 更大、恢复更复杂；适合「状态不大但反压明显」的链路。
- 本仓建议：先把反压与状态体积治好；再视超时情况打开（见 `flink/conf/checkpoint-recommendations.yaml` 注释键，**非实测值**）。

### Exactly-once 与 Kafka sink 两阶段提交（2PC）

典型路径（Kafka transactional sink / FlinkKafkaProducer 语义）：

1. **pre-commit**：checkpoint 成功瞬间，sink 把本轮写出事务标记为预提交（事务 id 进算子状态）。
2. **commit**：JM 通知所有 sink **正式 commit** Kafka 事务；下游消费者（`read_committed`）才可见。
3. 失败回滚：未 commit 的事务 abort；从上一完成 checkpoint 重放。

本仓实践对照：

| 路径 | 语义 | 适用 |
|------|------|------|
| `upsert-kafka` + `PRIMARY KEY` | 至少一次 + 主键覆盖 ≈ 幂等 EO | ODS→DWD（`event_id`）、DWS/ADS 日粒度覆盖（`01`/`02`/`03` SQL） |
| Kafka transactional sink | 真 2PC EO | 必须 append 且不能靠主键去重时 |
| 普通 at-least-once append | 可重复 | 必须配下游幂等或可重算 |

充值金额：**绝不能**在「可重复累加」的路径上无幂等键——见 §8。

---

## 4. 反压诊断与 Watermark 停滞

### 为何 watermark「卡住」

Watermark 是各并行 source / 分区 watermark 的 **min**。任一分区长期无新事件 → 全局 watermark 不推进 → 日窗不关、迟到判定失真。

游戏特有原因：

- 某 `server_id` 分区对应的服停服 / 低峰，长时间无埋点。
- 上游清洗作业反压，DWD topic 某分区 lag 拉大（消费侧表现为「事件时间不涨」）。
- 客户端批量补报：突然涌入大量旧 `event_time`，watermark 公式 `event_time - N秒` 被拖回去（bounded OOO 下通常取 max，但混入坏时钟要靠过滤）。

### 反压排查顺序（面试口述）

1. Flink UI：哪个算子 `backpressured` / `busy` 高？是 sink 慢还是聚合状态大？
2. Kafka：consumer lag（按 partition）；是否单分区热点（大 R / 活动服）。
3. Checkpoint：duration、alignment 耗时是否飙升（对齐等屏障）。
4. 状态：RocksDB 体积、GC、rocksdb 写入放大。
5. 外部：Doris Routine Load / upsert 目标是否打满。

本仓无编造 Lag/P95；生产应接监控后再填数。配置键示例见 `flink/conf/checkpoint-recommendations.yaml`。

---

## 5. Idle Source / Watermark Idleness（稀疏游戏服）

- 问题：8 个 `server_id`（见 `config/topics.yaml` demo.servers）若按服分区，夜深人少的服会 **拖死** 全局 watermark。
- 手段：
  1. Source 侧 **idleness**：分区超过阈值无记录则标记 idle，watermark 聚合时忽略该分区（Flink Kafka source / watermark strategy 的 idleness 配置）。
  2. 业务侧：过滤明显异常的未来/远古 `event_time`（客户端作弊改时）。
  3. 窗口侧：对「服级稀疏」指标改用处理时间心跳或按 `server_id` 独立水位（实现成本更高）。
- SQL 对照：本仓 watermark 写在表 DDL（`01` 5s / `02`/`03` 10s）；idleness 属于部署层 WITH / 代码 WatermarkStrategy，面试要能讲清「DDL 乱序界」与「空闲跳过」是两件事。

---

## 6. RocksDB State Backend + 增量 Checkpoint + TTL

### 为何玩家 keyed 状态用 RocksDB

- DWS：`GROUP BY server_id, player_id` 日窗累加器随 DAU 增长；HashMapStateBackend 全在堆上易 OOM。
- RocksDB：状态落本地磁盘，堆上主要是 block cache；适合「百万级 player_id × 多度量」。

### 增量 Checkpoint

- 全量：每次把整个 RocksDB 快照拷走 → 大状态作业慢、超时。
- 增量：只上传自上次 checkpoint 以来的 **SST 增量**；恢复时串联增量链。
- 本仓建议键（示例，非实测）：`state.backend: rocksdb`、`state.backend.incremental: true`（见 `flink/conf/checkpoint-recommendations.yaml`）。

### State TTL（玩家状态）

- Session 状态（login 未配对 logout）、短时去重 MapState 必须设 TTL，避免流失玩家 key 永驻。
- 日窗 tumble 在关窗后状态可清理；自定义 KeyedProcess 的 session 要显式 `StateTtlConfig`。
- 口径侧：在线时长依赖 login→logout 配对（`config/metrics.yaml` → `ads_online_duration_di`）；TTL 过短会丢未关会话，过长则状态膨胀——按「最长可玩会话 + 迟到裕量」定，不编造具体秒数。

---

## 7. Savepoint 升级 Playbook（改并行度）

目标：不停丢数地改 `parallelism`（例如从 4 → 8）或升级 SQL。

1. **停写窗口**（可选）：活动高峰错峰；或保证 Kafka 保留足够长，允许重放。
2. `flink savepoint <jobId> <savepointPath>`，确认成功。
3. 取消旧作业（`--with-savepoint` 更稳）。
4. 改并行度 / 发布新 jar 或新 SQL；**算子 uid 保持稳定**（SQL 作业注意 sink/source 名称变更会导致状态对不上）。
5. `flink run -s <savepointPath> ...` 从 savepoint 启动；Kafka 组策略：尽量复用 checkpoint 内 offset，避免和新 `group.id` 混用导致重复或空洞。
6. 校验：DWD `event_id` 计数、ADS 当日 DAU 与 lite/批旁路对比；异常则回滚到原 savepoint。
7. 本仓入口：`flink/jobs/ods_clean_job.py`、`dws_ads_submit.py`；SQL 在 `flink/sql/*.sql`。

改并行度时：max parallelism / key group 缩放要在作业初次创建时留余量；否则只能丢状态重放 Kafka。

---

## 8. 游戏语义：Sessionize 与充值 Exactly-once

### Sessionize：login → logout

- 目标：算出 `online_sec`（见 metrics `ads_online_duration_di`：session 由 login→logout 配对）。
- Flink 做法简述：
  - `keyBy(player_id[, session_id])`；ValueState 存 last_login_time。
  - 见 logout（或 payload.online_sec 已由客户端算好）则输出会话；本仓 DWS 对 `logout` 的 `online_sec` 做 SUM（`02_dwd_dws_realtime.sql`），lite 同口径。
  - 无 logout 的会话：定时器 + TTL 打「未闭合」标记，批作业可回补。
- 分区键必须保证同一玩家有序（§1），否则 session 乱序配对会算错时长。

### 充值：exactly-once vs at-least-once 的资金风险

| 做法 | 风险 |
|------|------|
| at-least-once + `SUM(amount_fen)` 无去重 | 故障重放 → **收入翻倍**，事故级 |
| upsert on `event_id` 后再聚合 | 重复事件覆盖，金额不双计（本仓 ODS→DWD） |
| 事务 sink EO + 下游只读 committed | 端到端一次，运维成本高 |
| 仅 ADS 层幂等、DWD 仍重复 | 中间明细错，ADS 对了也难审计 |

面试结论：**钱相关度量必须在进入累加前用业务幂等键（event_id / 订单号）去重**；本仓 `01_ods_clean.sql` 的 upsert-kafka 就是这道闸。看板可用 at-least-once + upsert；账务对账要 EO 或独立支付对账流。

---

## 9. 与 lite DuckDB 对比：你失去了什么

| 能力 | Flink + Kafka（prod） | DuckDB lite（`pipeline/local_runner.py`） |
|------|----------------------|------------------------------------------|
| 故障恢复 | Checkpoint / Savepoint 续跑 | 整段重跑；无算子状态恢复 |
| Consumer lag / 反压 | 可观测、可扩并行度 | FileTopic 无真实 lag |
| 事件时间窗口 | Watermark + tumble 真正按 event_time 关窗 | SQL 批式按 dt 聚合，**无 watermark 语义** |
| Exactly-once / 2PC | 可配 | 无 |
| 空闲分区 / idleness | 必须处理稀疏服 | 一次扫全量，无此问题 |
| 价值 | 生产形状、二面深挖 | Windows 可演示同 `metric_id` 口径 |

叙事：**先讲 prod（Kafka→Flink→Doris/Iceberg），再说明 lite 是口径沙盒**，避免被理解成「只会 DuckDB」。

---

## 10. 数据倾斜治理（游戏行为特有）

现象：头部公会/活动服/爆款副本导致某 `player_id` 或 `dungeon_id` 流量远高于均值 → 单 task 堆积。

手段（由易到难）：

1. **分区键设计**：不要只用 `server_id`；玩家级聚合用 `player_id`，全局 TopN 用两阶段（局部 TopN → 全局）。
2. **盐拆 key**：`concat(player_id, '_', salt)` 打散，再二次聚合；适合短时尖刺。
3. **旁路热点**：识别超大 R，单独并行度/单独 topic。
4. **mini-batch / 局部聚合**：Flink miniBatch 降低状态更新频率。
5. **副本维度**：`dungeon_id` 倾斜时先按 `server_id, dungeon_id` 预聚合（本仓 `03_ads_realtime.sql` 副本通关率按 `server_id, dungeon_id` 日窗）。

本仓本地 DuckDB 无倾斜问题；生产 Flink 面试应能把上述方案讲到「为何对游戏埋点有效」。

---

## 11. Late data（迟到数据）

- Allowed lateness：窗口关闭后仍接收一段时间迟到事件，更新结果（需 retract / upsert sink）。
- 超过 lateness：进 side output，落「补数通道」→ Spark/批作业回刷 ADS（本仓 `spark/jobs/dws_ads_batch.py` 承担留存/流失等长窗口回看）。
- 业务取舍：
  - **实时 DAU / 付费**：可接受分钟级修正（upsert）。
  - **留存 / 流失**：天然依赖多日窗口，适合批（`sql/metrics/ads_retention_nd.sql`、`ads_churn_di.sql`），Flink 只做近实时预览。

`03_ads_realtime.sql` 明确：**Retention & churn remain Spark/DuckDB batch**。

---

## 12. 与本仓库 SQL / Job 的对应关系

| 能力 | 生产产物 | Lite 镜像 |
|------|----------|-----------|
| ODS 清洗 + 11 类型过滤 + event_id | `flink/sql/01_ods_clean.sql`，Job `flink/jobs/ods_clean_job.py` | `local_runner.step_ods_dwd` |
| 日窗玩家行为 DWS | `flink/sql/02_dwd_dws_realtime.sql`（Tumble 1 day） | `step_dws` → `dws.player_behavior_di` |
| 实时 DAU / 付费率 / 通关率 | `flink/sql/03_ads_realtime.sql`，Job `flink/jobs/dws_ads_submit.py` | `step_ads` 全量 ADS |
| Checkpoint 配置示例 | `flink/conf/checkpoint-recommendations.yaml` | 无 |
| 留存 / 流失 / ARPU 批 | Spark + `sql/metrics/*.sql` | 同 DuckDB ADS |
| Topic / group.id | `config/topics.yaml` + Flink WITH 子句 | `FileTopic` |
| OLAP 外表 | `sql/ddl/doris_ads.sql`、`sql/ddl/iceberg_notes.md` | Parquet / DuckDB |

---

## 13. 快速自测问题（自问自答）

1. 为什么 watermark 是 `event_time - 5s` 而不是处理时间？→ 要对齐业务事件时间，抗乱序。
2. 重复消费会不会把 DAU 算双？→ DWD 按 `event_id` 幂等；DAU 是当日去重 player_id，重放同一事件不增。
3. 为何留存不放 Flink 日窗？→ 需 cohort 跨天 lookback，状态大、修正多，批更稳。
4. Checkpoint 失败怎么排查？→ 屏障对齐超时、状态过大、外部 sink 两阶段提交、反压。
5. 稀疏服为何要 idleness？→ 否则空闲分区拖住全局 watermark，日窗不关。
6. 充值为何不能裸 at-least-once SUM？→ 重放导致金额双计；先 event_id upsert。

更完整的二面问答见 [`docs/interview-faq.md`](interview-faq.md)。
