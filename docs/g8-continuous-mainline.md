# GameStream G8 — Continuous Doris ADS Mainline

**Scope:** **单一持续 Flink 作业**（Kafka → 清洗 / `event_id` 去重 → 日 DAU + 付费率 → upsert-kafka）。把 G3 watermark/去重、G4 checkpoint、G5 kill-TM 折进**同一条** Flink job。  
**连续 Doris ADS 可见性：** 由 **常驻 materializer** 消费 upsert-kafka → UNIQUE KEY（见 [`g8-resident-materializer.md`](g8-resident-materializer.md)）。本脚本内的 `materialize_doris` 保留为 **fallback / dev-only**（验收主路径勿再依赖「wait for expected then materialize」）。

**对照：**

| | G2 | G3–G5（旧） | **G8** |
|--|----|------------|--------|
| 运行模式 | batch + bounded Kafka | 各自独立 streaming 演练 | **一条** streaming 主流水线 |
| Doris ADS | 有界一次性灌数 | 多数写 Kafka / 专项表 | Flink 持续 upsert-kafka；**常驻 materializer** 物化 `ads_*`（脚本阶段=fallback） |
| 故障 | 无 | G5 单独 kill-TM | **同一 job** kill-TM 后继续更新 ADS |

**不在范围：** 编造吞吐/Lag/P95（小规模分块负载见 G8 steady bench）；扩展 metric zoo；宣称端到端 EO-2PC。常驻 materializer 见专项文档（本页叙述 Flink 主流水线）。

## 面试叙事（持续 ADS，不是有界批）

```
连续 produce JSONL（含 dup / 轻度乱序）
        │
        ▼
Kafka gamestream.g8.ods.events
        │
        ▼
Flink SQL job g8-continuous-ads  （streaming，常驻）
  • event_time + watermark = event_time - 5s   （G3）
  • ROW_NUMBER PARTITION BY event_id → first wins
  • unbounded GROUP BY (dt, server_id)
        → DAU / pay_rate（日桶可订正，非关窗丢弃）
  • checkpoint 10s + hashmap + file:///checkpoints （G4）
  • restart-strategy fixed-delay （G5）
  • upsert-kafka → gamestream.g8.ads_dau / ads_pay_rate（ALS + PK）
        │
        ▼
resident materializer（常驻）：upsert-kafka → mysql INSERT/DELETE → Doris UNIQUE KEY
  ※ 主路径；acceptance = produce + SELECT only
  ※ 脚本阶段 materialize_doris = fallback/dev only
  ※ Flink JDBC 的 MySQL `ON DUPLICATE KEY UPDATE` 会被 Doris FE 拒绝，故不直连 JDBC upsert
        │
        ▼
*** docker kill gs-flink-tm *** → 同 job 恢复
  dup 不双计；新事件继续抬 DAU
```

## 语义策略（诚实）

| 点 | 策略 |
|----|------|
| Event time | `dt = CAST(event_time AS DATE)`，不是处理时间 |
| Watermark | `event_time - 5s`；`idle-timeout=5s` 避免空闲卡死 |
| `event_id` 去重 | Rank first-wins（与 G4/G5 相同）；dup 不抬 DAU |
| 乱序 | 同日 OOO 仍进同一 `dt` 桶（fixture 含 `ooo_within_bound`） |
| 迟到 / 日桶 | **日 ADS = 开窗可订正**（unbounded key-by），不是 G3 分钟窗的关窗丢弃。G3 关窗 drop 仍由 `g3_stream_semantics` 证明 |
| Sink | Flink = upsert-kafka ALS；Doris = **常驻 materializer** UNIQUE KEY（脚本阶段=fallback）；at-least-once；**不**宣称 EO-2PC / JDBC upsert |
| State | `table.exec.state.ttl=1d`，parallelism=1，适配 ~7.6Gi |

## metric_id

与 `config/metrics.yaml` / DataPilot / SQLGuard 一致，**不改契约**：

- `ads_dau_di` — `COUNT(DISTINCT player_id)` per `(dt, server_id)`
- `ads_pay_rate_di` — `pay_users(recharge) / dau`

## Fixture 期望（脚本内写死，便于验收）

| 阶段 | `dau` | `pay_users` | 说明 |
|------|-------|-------------|------|
| batch1（6 唯一含 OOO + 2 dup） | **6** | **1** | 连续写入 #1 |
| batch2（+2 玩家，含新付费） | **8** | **2** | 连续写入 #2（证明非 G2 一次性） |
| kill-TM + recover + dup replay | **8** | **2** | 旧数据不双计 |
| batch3（+2 玩家） | **10** | **2** | 故障后 ADS 继续更新 |

## 怎么跑（WSL）

```bash
cd "$PWD"  # 仓库根目录
# 栈已 up；若刚改 compose restart-strategy：
# docker compose up -d --force-recreate jobmanager taskmanager

cp scripts/g8_continuous_mainline.sh /tmp/g8.sh
sed -i 's/\r$//' /tmp/g8.sh
GAMESTREAM_ROOT="$(cd "$(dirname "$0")/.." && pwd)" bash /tmp/g8.sh
```

结果：[`g8-continuous-mainline-result.txt`](g8-continuous-mainline-result.txt)。

## 文件

| Path | Role |
|------|------|
| `flink/sql/g8_continuous_ads.sql` | 持续 streaming SQL → upsert-kafka ADS |
| `scripts/g8_continuous_mainline.sh` | 连续灌数 + 查 ADS + kill-TM + 恢复证明（脚本物化=fallback） |
| `pipeline/doris_ads_materializer.py` / `scripts/g8_resident_materializer*` | **常驻** Doris materializer + acceptance |
| `sql/ddl/doris_ads_g2.sql` | 复用 G2 ADS DDL（`replication_num=1`） |

## Real run

WSL run **2026-09-07**（UTC `04:01:01` ≈ **12:01 Asia/Shanghai**）。job `d9662fc46977046e7ff35453824140ba`。

| 阶段 | Doris `dau` | `pay_users` | `pay_rate` |
|------|-------------|-------------|------------|
| after_batch1 | **6** | **1** | 0.1667 |
| after_batch2 | **8** | **2** | 0.25 |
| after TM kill + recover | **8** | **2** | 0.25 |
| after dup replay | **8** | **2** | 0.25 |
| after_batch3 | **10** | **2** | 0.2 |

- checkpoint before kill: path 含 `chk-8`；after recover completed count **8 → 20**（**未单独证明** job 一定从 `chk-8` 精确 restore——只证明恢复后继续完成 CP 且 ADS 语义正确）
- `docker kill gs-flink-tm` rc=0；compose/TM 未及时拉起时脚本 **honest fallback** `docker start`（结果里 `tm_start_fallback=1`——勿写成「compose 自动拉起已验证」）
- 最终 Doris：`ads_dau_di (2026-09-07,1)=10`；`ads_pay_rate_di pay_users=2 pay_rate=0.2`
- 全量粘贴：[`g8-continuous-mainline-result.txt`](g8-continuous-mainline-result.txt)

不编造吞吐/Lag/P95；小规模分块负载与批次可见性验证见 [`docs/g8-steady-bench.md`](g8-steady-bench.md)（数字只引 `bench/results/g8_*.json`）。
