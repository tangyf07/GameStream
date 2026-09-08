# GameStream

一句话：游戏玩家行为 **事件 → Kafka → Flink → `ads_dau_di` → Doris** 的实时指标链路（lite 可用 DuckDB 同口径）。给 [DataPilot](https://github.com/tangyf07/DataPilot) 问数、经 [SQLGuard](https://github.com/tangyf07/SQLGuard) 门禁消费。

## Golden Path

```mermaid
flowchart LR
  E[游戏行为事件] --> K[Kafka]
  K --> F[Flink 清洗/聚合]
  F --> M["ads_dau_di"]
  M --> D[Doris]
```

### 现场 6 步（固定顺序）

| # | 节拍 | 用什么（已有脚本，不扩功能） |
|---|------|------------------------------|
| 1 | **正常** | G2 E2E：`scripts/e2e_g2.sh` / `scripts/demo_golden_path.sh` → produce → Flink → Doris |
| 2 | **重复** | G3 `event_id` 去重（可选演练）：[`docs/g3-stream-semantics.md`](docs/g3-stream-semantics.md) |
| 3 | **迟到** | G3 watermark 迟到丢弃（可选，同 G3 脚本） |
| 4 | **kill TM** | G5：`scripts/g5_fault_drill.sh`（`docker kill gs-flink-tm`） |
| 5 | **恢复** | G5：checkpoint + restart-strategy 同 job 拉回；计数不双计 |
| 6 | **对 DAU** | G2 查 `ads.ads_dau_di`（`metric_id='ads_dau_di' AND dau>0`） |

完整命令与成功信号：[`docs/golden-path-demo.md`](docs/golden-path-demo.md)。招聘方第一屏只认这条链路 + 上表；G2–G8 长文见附录。

```bash
# 起栈 + 正常 + 对 DAU（骨干）
docker compose up -d
bash scripts/demo_golden_path.sh   # 或 e2e_g2.sh（WSL：先 cp /tmp + sed CRLF）
docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e \
  "SELECT dt, server_id, dau, metric_id FROM ads.ads_dau_di WHERE metric_id='ads_dau_di' AND dau>0 ORDER BY dt, server_id;"
```

对照：[`docs/e2e-g2-query-result.txt`](docs/e2e-g2-query-result.txt)。端口：Kafka `:19092` · Flink UI `:8081` · Doris MySQL `:9030`（BE 须 Alive）。

- DataPilot：https://github.com/tangyf07/DataPilot
- SQLGuard：https://github.com/tangyf07/SQLGuard
- 指标契约：[`docs/datapilot_contract.md`](docs/datapilot_contract.md) · [`config/metrics.yaml`](config/metrics.yaml)

## 写死口径：lite vs Docker 组件

| | **本机默认可跑（lite）** | **WSL Docker（G1 组件 + G2 E2E）** |
|--|--------------------------|-----------------------------------|
| 怎么跑 | `scripts/run_all.sh` / `run_all.ps1` → DuckDB | `docker compose up -d`（WSL） |
| 接入 | `FileTopic` JSONL | **Kafka** `apache/kafka:3.7`（`:19092`） |
| 流处理 | `pipeline/local_runner.py` | **Flink** JM/TM（UI `:8081`） |
| OLAP | Parquet + DuckDB | **Doris** FE/BE（HTTP `:8030` / MySQL `:9030`） |
| 端到端 | lite ADS 在 DuckDB | **G2 已验证**：Simulator→Kafka→Flink→Doris ADS |

**勿夸大：** G1 = 组件可起。**G2 = 有界 E2E**（`ads_dau_di`）。**G3–G5 = 独立演练**（重复/迟到/kill TM/恢复）；G8 持续主流水线见附录。未宣称 EO-2PC、K8s/Spark/Iceberg 生产部署、编造 SLA。lite 与 prod **同口径**。

## 如何跑 / 测试（lite）

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
bash scripts/quality_gate.sh
bash scripts/run_all.sh --players 2000 --events 20000
```

Windows：`.\scripts\quality_gate.ps1` / `.\scripts\run_all.ps1`。

## 事件与分层

11 类：`login | create_role | enter_dungeon | clear_dungeon | death | equip | enhance | recharge | gacha | friend | logout`（`simulator/`）。

ODS → DWD → DWS → ADS（DAU / 留存 / 在线时长 / 付费率 / ARPU / 副本通关率 / 流失）。演示 Golden Path 只验收 **`ads_dau_di`**。

---

## 附录 Appendix

### 三仓固化验收

[`docs/suite-acceptance.md`](docs/suite-acceptance.md)（GameStream `ed876e1`+ / SQLGuard `7dc85dd`=1.1.2 / DataPilot **`2541623`**；默认 **strict**；G8 不在 suite required 内）。

```bash
bash scripts/suite_acceptance.sh   # WSL：先 sed 去 CRLF
```

报告：[`docs/suite-acceptance-result.txt`](docs/suite-acceptance-result.txt)。

### G2 快速跑（WSL，小流量）

```bash
bash scripts/e2e_g2.sh
# 默认 --players 500 --events 3000；结果落 docs/e2e-g2-query-result.txt
```

细节：[`docs/e2e-g2.md`](docs/e2e-g2.md)。演示入口：[`docs/golden-path-demo.md`](docs/golden-path-demo.md)。

### G3 流语义（WSL，小流量）— 重复 / 迟到

```bash
cp scripts/g3_stream_semantics.sh /tmp/g3.sh && sed -i 's/\r$//' /tmp/g3.sh
GAMESTREAM_ROOT=/mnt/c/Users/tangy/source/repos/GameStream bash /tmp/g3.sh
```

细节：[`docs/g3-stream-semantics.md`](docs/g3-stream-semantics.md)（event time、watermark、迟到丢弃、`event_id` 去重）。**不含** G4–G6。

### G4 Checkpoint / 幂等（WSL，小流量）

```bash
docker compose up -d --force-recreate jobmanager taskmanager
cp scripts/g4_checkpoint_idempotency.sh /tmp/g4.sh && sed -i 's/\r$//' /tmp/g4.sh
GAMESTREAM_ROOT=/mnt/c/Users/tangy/source/repos/GameStream bash /tmp/g4.sh
```

细节：[`docs/g4-checkpoint-idempotency.md`](docs/g4-checkpoint-idempotency.md)。Doris = ALS + UNIQUE KEY，**不**宣称端到端 EO-2PC。

### G5 故障演练（WSL，kill TaskManager）— kill TM / 恢复

```bash
docker compose up -d --force-recreate jobmanager taskmanager
cp scripts/g5_fault_drill.sh /tmp/g5.sh && sed -i 's/\r$//' /tmp/g5.sh
GAMESTREAM_ROOT=/mnt/c/Users/tangy/source/repos/GameStream bash /tmp/g5.sh
```

细节：[`docs/g5-fault-drill.md`](docs/g5-fault-drill.md)。**不**宣称端到端 EO-2PC。

### G6 实测压测（WSL，Kafka sink）

```bash
cp scripts/g6_bench.sh /tmp/g6.sh && sed -i 's/\r$//' /tmp/g6.sh
GAMESTREAM_ROOT=/mnt/c/Users/tangy/source/repos/GameStream TIER=light bash /tmp/g6.sh
```

细节：[`docs/g6-bench.md`](docs/g6-bench.md)。**数字只引用**已提交的 [`bench/results/g6_*.json`](bench/results/)。

### G7 AI 闭环（WSL，SQLGuard → Doris ADS）

```bash
cp scripts/g7_closed_loop.sh /tmp/g7.sh && sed -i 's/\r$//' /tmp/g7.sh
GAMESTREAM_ROOT=/mnt/c/Users/tangy/source/repos/GameStream MODE=fixture bash /tmp/g7.sh
```

细节：[`docs/g7-closed-loop.md`](docs/g7-closed-loop.md)。消费**已有** ADS 行，不负责灌数；**不含** 新 UI / 编造指标。

### G8 单一持续 Flink 作业＋常驻 Doris materializer（WSL）

```bash
# continuous Flink upsert-kafka mainline (kill-TM proof; script materialize = fallback/dev)
cp scripts/g8_continuous_mainline.sh /tmp/g8.sh && sed -i 's/\r$//' /tmp/g8.sh
GAMESTREAM_ROOT=/mnt/c/Users/tangy/source/repos/GameStream bash /tmp/g8.sh

# resident materializer (primary continuous Doris visibility)
./scripts/run_g8_resident_materializer.sh start   # or: docker compose --profile materializer up -d
cp scripts/g8_resident_materializer_accept.sh /tmp/g8r.sh && sed -i 's/\r$//' /tmp/g8r.sh
GAMESTREAM_ROOT=/mnt/c/Users/tangy/source/repos/GameStream bash /tmp/g8r.sh
```

细节：[`docs/g8-continuous-mainline.md`](docs/g8-continuous-mainline.md) + [`docs/g8-resident-materializer.md`](docs/g8-resident-materializer.md)。Flink：Kafka→清洗/`event_id` 去重→日 DAU+付费率→**upsert-kafka ALS+PK**；Doris：**常驻 materializer** UNIQUE KEY 物化（acceptance = produce+SELECT only）。脚本阶段 `materialize_doris` 仅 **fallback/dev**。同 job kill-TM 恢复不双计；`tm_start_fallback=1` / chk-8 精确 restore 未单独证明。Flink JDBC MySQL upsert 方言 Doris 拒收故不用；**at-least-once + UNIQUE KEY**；**不**宣称 EO-2PC。勿编造 bench 数字。

### G8 小规模分块负载与批次可见性验证（WSL）

```bash
cp scripts/g8_steady_bench.sh /tmp/g8s.sh && sed -i 's/\r$//' /tmp/g8s.sh
GAMESTREAM_ROOT=/mnt/c/Users/tangy/source/repos/GameStream TIER=light bash /tmp/g8s.sh
# optional medium (only if light stable / mem ok):
# GAMESTREAM_ROOT=/mnt/c/Users/tangy/source/repos/GameStream TIER=medium bash /tmp/g8s.sh
```

细节：[`docs/g8-steady-bench.md`](docs/g8-steady-bench.md)。**数字只引用**已提交的 [`bench/results/g8_*.json`](bench/results/)。表述：两档各 2 次批次探针，**当时脚本物化路径**下约 **39–41s 可查**（n=2≈max，**勿卖 P95**；含 console-consumer 串行开销）。Flink in/s = **sum across operators，非 source 吞吐**。**不**把 G6 Kafka-only 数字标成 G8 Doris-visible。连续 Doris 可见性见常驻 materializer（[`docs/g8-resident-materializer.md`](docs/g8-resident-materializer.md)）；steady 脚本物化为历史对照 / fallback。

### 二面深挖

- [`docs/flink-kafka-deep-dive.md`](docs/flink-kafka-deep-dive.md)
- [`docs/g3-stream-semantics.md`](docs/g3-stream-semantics.md)
- [`docs/g5-fault-drill.md`](docs/g5-fault-drill.md)
- [`docs/interview-faq.md`](docs/interview-faq.md)
- `flink/conf/checkpoint-recommendations.yaml` · `flink/sql/*` · `flink/jobs/*`

### 压测

仅提交实测：`bench/results/g6_*.json` / `bench/results/g8_*.json` / `bench/results/bench_*.json`。无实测时 **不编造** Lag / Checkpoint / P95。G6：[`docs/g6-bench.md`](docs/g6-bench.md)。G8 分块负载：[`docs/g8-steady-bench.md`](docs/g8-steady-bench.md)。
