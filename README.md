# GameStream

游戏玩家行为**实时数据平台**一环，定位在 AI 驱动数据工程体系中（事件采集 → 流式清洗/聚合 → ADS 指标 → 可供 DataPilot / SQLGuard 消费），不是数仓作业题，也不是电商订单流水。

## 架构：prod vs lite

| | Production | Local lite（本机默认可跑） |
|--|------------|---------------------------|
| 接入 | Kafka / Redpanda | `pipeline/kafka_io.FileTopic`（JSONL 分段） |
| 流处理 | Flink SQL/Job（`flink/`） | DuckDB 镜像同口径（`pipeline/local_runner.py`） |
| 批补数 | Spark（留存/流失回看） | 同 DuckDB ADS |
| OLAP | Doris / Iceberg | Parquet + DuckDB |
| 编排 | docker-compose `profile=full`（可选） | `scripts/run_all.sh` |

口径以 `config/metrics.yaml` + `sql/metrics/*.sql` 为准；Flink/Spark/DuckDB 应对齐同一套 metric_id。

## 事件模型（11 类）

统一 envelope：`event_id, event_type, event_time, player_id, role_id, server_id, session_id, payload`。

类型：`login | create_role | enter_dungeon | clear_dungeon | death | equip | enhance | recharge | gacha | friend | logout`。

Schema：`simulator/schemas/events.json`。模拟器：`simulator/generate_events.py`。

## 分层与指标

- **ODS** 原始落地 → **DWD** 清洗去重 → **DWS** 玩家日/副本日汇总 → **ADS** 应用指标
- ADS：`DAU`、`N日留存(1/3/7)`、`日均在线时长`、`付费率`、`ARPU`、`副本通关率`、`流失风险`

定义见 `config/metrics.yaml`；SQL 见 `sql/metrics/`；DDL 见 `sql/ddl/`（含 Doris ADS / Iceberg 设计）。

## 如何跑 / 测试

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

bash scripts/quality_gate.sh          # pytest，失败即 exit 1
bash scripts/run_all.sh --players 2000 --events 20000

# 或直接
python pipeline/local_runner.py --players 2000 --events 20000 --days 7 --seed 42
```

Windows：`scripts\quality_gate.ps1` / `scripts\run_all.ps1`（可用环境变量 `PLAYERS`/`EVENTS`，或改脚本参数）。

产出：`data/gamestream.duckdb`、`data/ods|dwd|dws|ads/*.parquet`。

可选 Docker 全栈：`docker compose --profile full up`（Redpanda + Flink + ClickHouse 参考；本机演示不依赖）。

## 二面深挖

- Flink/Kafka 长文（barrier/2PC、反压、idleness、RocksDB、savepoint、充值幂等、vs DuckDB）→ [`docs/flink-kafka-deep-dive.md`](docs/flink-kafka-deep-dive.md)
- 二面短答 FAQ（watermark/DAU/倾斜/迟到/留存批/upsert/Iceberg·Doris/DataPilot）→ [`docs/interview-faq.md`](docs/interview-faq.md)
- Checkpoint 配置示例（键+注释，非实测）→ `flink/conf/checkpoint-recommendations.yaml`；Job：`flink/jobs/*`；SQL：`flink/sql/*`
- 口径 / OLAP：`config/metrics.yaml`、`sql/metrics/`、`sql/ddl/doris_ads.sql`、`sql/ddl/iceberg_notes.md`

## 压测

`bench/run_bench.py` 可测 simulate + pipeline 耗时。**未在本机跑过则视为未测，勿编造数字。** 结果写入 `bench/results/`（仅实测）。


---

## 11. 打包与推到 Windows（给搬运方）

1. 取 `/workspace/GameStream-build.zip`（已排除 `venv/`、`data/` 大文件、`__pycache__`）。
2. 解压到 Windows 工作区，例如 `D:\portfolio\GameStream`。
3. 执行：`.\scripts\run_all.ps1`（需 Python 3.11+）。
4. 首次 `git init` / 关联 remote 后 push；确认 `.gitignore` 已忽略 `data/`、`venv/`、`*.duckdb`。
5. 可选：在本机再跑 `python bench/run_bench.py`，只提交 `bench/results/` 里**实测** JSON。

## 压测（仅实测）

一次在构建机上的测量写入 `bench/results/bench_20260906T150843Z.json`：

- 5_000 players / 50_000 events
- simulate_sec ≈ 1.017；pipeline_sec ≈ 1.330
- 非 SLA；换机器请重跑 `python bench/run_bench.py`

Kafka Lag / Flink Checkpoint / P95 端到端未在本机无 broker 环境下测，不编造。

