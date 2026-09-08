# Interview notes（非公开首页）

从 README 拆出的深挖入口。对外第一屏只看 Golden Path + Guarantees；此处给自己复盘用。

## 深挖入口

- [`flink-kafka-deep-dive.md`](flink-kafka-deep-dive.md)
- [`g3-stream-semantics.md`](g3-stream-semantics.md)
- [`g5-fault-drill.md`](g5-fault-drill.md)
- [`interview-faq.md`](interview-faq.md)（Kafka / Flink / 口径短答 + 仓库锚点）
- `flink/conf/checkpoint-recommendations.yaml` · `flink/sql/*` · `flink/jobs/*`

## 口径提醒（原 README 勿夸大）

- 不背未实测的 Lag / Checkpoint / P95。
- lite DuckDB ≠ 生产 Flink：无 checkpoint 恢复、真 Kafka lag、watermark/idleness、2PC。
- Doris 侧：at-least-once + UNIQUE KEY，**不**宣称端到端 EO-2PC。
- 充值金额进 `SUM` 前必须按 `event_id`（或订单号）幂等。

完整 Q&A 见 [`interview-faq.md`](interview-faq.md)。
