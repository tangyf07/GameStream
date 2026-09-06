# GameStream

一句话：面向游戏玩家行为的**实时指标平台**（采集→清洗→分层 ADS），给 [DataPilot](https://github.com/tangyf07/DataPilot) 问数、经 [sql-write-gate](https://github.com/tangyf07/sql-write-gate)（SQLGuard）门禁消费——不是又一个数仓作业，也不是电商订单流水。

- DataPilot：https://github.com/tangyf07/DataPilot  
- SQLGuard（sql-write-gate）：https://github.com/tangyf07/sql-write-gate  
- 指标契约：[`docs/datapilot_contract.md`](docs/datapilot_contract.md) · [`config/metrics.yaml`](config/metrics.yaml)

```mermaid
flowchart LR
  NL[自然语言问数] --> DP[DataPilot]
  DP --> SG[SQLGuard / sql-write-gate]
  SG -->|只读 ADS SQL| GS[GameStream ADS]
  subgraph GS_pipe [GameStream 实时链路]
    E[游戏行为事件] --> K[Kafka]
    K --> F[Flink 清洗/聚合]
    F --> OLAP[Doris / Iceberg]
    E -. lite .-> DB[(DuckDB)]
    DB --> ADS[(ADS 指标表)]
    OLAP --> ADS
  end
  GS --- ADS
```

## 写死口径：lite vs 生产参考

| | **本机默认可跑（lite）** | **WSL Docker 生产组件（G1 已验证可起）** |
|--|--------------------------|----------------------------------------|
| 怎么跑 | `scripts/run_all.sh` / `run_all.ps1` → DuckDB | `docker compose up -d`（WSL） |
| 接入 | `FileTopic` JSONL | **Kafka** `apache/kafka:3.7`（`:19092`） |
| 流处理 | `pipeline/local_runner.py` | **Flink** JM/TM（UI `:8081`） |
| OLAP | Parquet + DuckDB | **Doris** FE/BE（HTTP `:8030` / MySQL `:9030`） |
| 状态 | 质量门可跑 | **WSL 已拉起并探测**；详见 [`docs/docker-compose-status.md`](docs/docker-compose-status.md) |

**勿夸大：** G1 = 组件可起 + 端口可达。Flink Job→Doris **端到端**属 G2，未在此宣称。lite 与 prod **同口径**（同一套 `metric_id`）。

## 事件与分层

11 类：`login | create_role | enter_dungeon | clear_dungeon | death | equip | enhance | recharge | gacha | friend | logout`（`simulator/`）。

ODS → DWD → DWS → ADS（DAU / 留存 / 在线时长 / 付费率 / ARPU / 副本通关率 / 流失）。

## 如何跑 / 测试

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
bash scripts/quality_gate.sh
bash scripts/run_all.sh --players 2000 --events 20000
```

Windows：`.\scripts\quality_gate.ps1` / `.\scripts\run_all.ps1`。

## 二面深挖

- [`docs/flink-kafka-deep-dive.md`](docs/flink-kafka-deep-dive.md)
- [`docs/interview-faq.md`](docs/interview-faq.md)
- `flink/conf/checkpoint-recommendations.yaml` · `flink/sql/*` · `flink/jobs/*`

## 压测

仅提交实测：`bench/results/*.json`。无 Kafka 集群时 **不编造** Lag / Checkpoint / P95。
