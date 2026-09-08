# Experiments 索引（G2–G8）

README 只保留 Golden Path + 保证 + 一条跑法 + 一条验证。细节与命令在此。

> WSL 从 `/mnt/c` 跑脚本：先 `cp` 到 `/tmp`，`sed -i 's/\r$//'`，再设 `GAMESTREAM_ROOT` 指向仓库根。

## G2 有界 E2E

```bash
bash scripts/e2e_g2.sh
# 或：bash scripts/demo_golden_path.sh
```

细节：[`e2e-g2.md`](e2e-g2.md) · 演示六节拍：[`golden-path-demo.md`](golden-path-demo.md) · 结果：[`e2e-g2-query-result.txt`](e2e-g2-query-result.txt)

## G3 流语义（重复 / 迟到）

```bash
cp scripts/g3_stream_semantics.sh /tmp/g3.sh && sed -i 's/\r$//' /tmp/g3.sh
GAMESTREAM_ROOT="$(cd "$(dirname "$0")/.." && pwd)" bash /tmp/g3.sh
```

细节：[`g3-stream-semantics.md`](g3-stream-semantics.md)（event time、watermark、迟到丢弃、`event_id` 去重）。**不含** G4–G6。

## G4 Checkpoint / 幂等

```bash
docker compose up -d --force-recreate jobmanager taskmanager
cp scripts/g4_checkpoint_idempotency.sh /tmp/g4.sh && sed -i 's/\r$//' /tmp/g4.sh
GAMESTREAM_ROOT="$(cd "$(dirname "$0")/.." && pwd)" bash /tmp/g4.sh
```

细节：[`g4-checkpoint-idempotency.md`](g4-checkpoint-idempotency.md)。Doris = ALS + UNIQUE KEY，**不**宣称 EO-2PC。

## G5 故障演练（kill TM / 恢复）

```bash
docker compose up -d --force-recreate jobmanager taskmanager
cp scripts/g5_fault_drill.sh /tmp/g5.sh && sed -i 's/\r$//' /tmp/g5.sh
GAMESTREAM_ROOT="$(cd "$(dirname "$0")/.." && pwd)" bash /tmp/g5.sh
```

细节：[`g5-fault-drill.md`](g5-fault-drill.md)。**不**宣称 EO-2PC。

## G6 实测压测（Kafka sink）

```bash
cp scripts/g6_bench.sh /tmp/g6.sh && sed -i 's/\r$//' /tmp/g6.sh
GAMESTREAM_ROOT="$(cd "$(dirname "$0")/.." && pwd)" TIER=light bash /tmp/g6.sh
```

细节：[`g6-bench.md`](g6-bench.md)。**数字只引用**已提交的 [`../bench/results/g6_*.json`](../bench/results/)。

## G7 AI 闭环（SQLGuard → Doris ADS）

```bash
cp scripts/g7_closed_loop.sh /tmp/g7.sh && sed -i 's/\r$//' /tmp/g7.sh
GAMESTREAM_ROOT="$(cd "$(dirname "$0")/.." && pwd)" MODE=fixture bash /tmp/g7.sh
```

细节：[`g7-closed-loop.md`](g7-closed-loop.md)。消费**已有** ADS 行，不负责灌数。

## G8 持续主流水线 + 常驻 materializer

```bash
cp scripts/g8_continuous_mainline.sh /tmp/g8.sh && sed -i 's/\r$//' /tmp/g8.sh
GAMESTREAM_ROOT="$(cd "$(dirname "$0")/.." && pwd)" bash /tmp/g8.sh

./scripts/run_g8_resident_materializer.sh start
cp scripts/g8_resident_materializer_accept.sh /tmp/g8r.sh && sed -i 's/\r$//' /tmp/g8r.sh
GAMESTREAM_ROOT="$(cd "$(dirname "$0")/.." && pwd)" bash /tmp/g8r.sh
```

细节：[`g8-continuous-mainline.md`](g8-continuous-mainline.md) · [`g8-resident-materializer.md`](g8-resident-materializer.md)。**at-least-once + UNIQUE KEY**；**不**宣称 EO-2PC。

## G8 分块负载 / 批次可见性

```bash
cp scripts/g8_steady_bench.sh /tmp/g8s.sh && sed -i 's/\r$//' /tmp/g8s.sh
GAMESTREAM_ROOT="$(cd "$(dirname "$0")/.." && pwd)" TIER=light bash /tmp/g8s.sh
```

细节：[`g8-steady-bench.md`](g8-steady-bench.md)。**数字只引用** [`../bench/results/g8_*.json`](../bench/results/)。

## 三仓固化验收

```bash
bash scripts/suite_acceptance.sh
```

细节：[`suite-acceptance.md`](suite-acceptance.md) · 报告：[`suite-acceptance-result.txt`](suite-acceptance-result.txt)。G8 不在 suite required 内。

## 组件状态

[`docker-compose-status.md`](docker-compose-status.md)
