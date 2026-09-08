# GameStream G8 — 小规模分块负载与批次可见性验证

**Scope:** 在 **G8 单一持续 Flink 作业＋脚本阶段物化 Doris** 主线上，做**小规模分块负载与批次可见性验证**（非「稳态压测」口径）— throughput、Kafka consumer lag、checkpoint duration、**Doris-query-visible 批次探针**、backpressure。  
**Not in scope:** metric zoo、编造 SLA、~7.6Gi 上 heavy tier、把 G6 Kafka-only E2E 标成 Doris-visible。常驻 materializer 见 [`g8-resident-materializer.md`](g8-resident-materializer.md)（本 bench 数字仍为**脚本物化**路径实测，勿与常驻路径混报）。

**Honesty rule:** every number in the summary table comes from a committed result JSON under `bench/results/g8_{light,medium}_*.json` (or is explicitly `未测到` with reason). **Never invent.**

## Contrast vs G6

| | G6 | **G8 分块负载** |
|--|----|----------------|
| Load shape | Burst produce then drain/stop | **分块持续 produce**（`duration_sec` × ≥2 rounds；小规模） |
| Baseline | none required | **Sample lag/CP/BP before load starts** |
| Sink path under test | Kafka out topic only | G8：**upsert-kafka → 脚本阶段 UNIQUE KEY 物化 → Doris ADS**（本 bench；常驻 writer 见专项） |
| E2E definition | `sink_ts - produce_ts` (Kafka) | **produce_probe_end → Doris SELECT matches expected dau/pay** |
| Numbers | `bench/results/g6_*.json` | `bench/results/g8_*.json` — **do not copy G6 into G8** |

## Sink honesty

- Flink continuous sink = **upsert-kafka (ALS + PK)**
- Doris ADS（本 bench）= **脚本阶段** **plain INSERT** on **UNIQUE KEY**（历史对照 / fallback）
- **Primary continuous path** = resident materializer（[`g8-resident-materializer.md`](g8-resident-materializer.md)）
- Flink JDBC MySQL `ON DUPLICATE KEY UPDATE` rejected by Doris FE — not used
- **NOT** end-to-end EO-2PC

## Environment

- WSL Docker stack (~7.6Gi): Kafka + Flink JM/TM + Doris FE/BE
- Base: G8 mainline SQL [`flink/sql/g8_continuous_ads.sql`](../flink/sql/g8_continuous_ads.sql) (topics/group remapped for bench isolation)
- Tiers: **light** and **medium** only (heavy disabled to avoid OOM)

## Definitions

| Metric | Definition used here | Source |
|--------|----------------------|--------|
| **Baseline** | Lag + Flink sample-once + checkpoints + vertices BP **before** continuous load | `baseline_before_load` + raw `lag_baseline.txt` |
| **Throughput (input)** | `total_events / total_produce_wall_sec` across continuous chunked produce + probes | `metrics.throughput.input_produce_events_per_sec` |
| **Throughput (Flink in/s)** | 采样窗口内 `numRecordsInPerSecond`：**跨算子合计（sum across operators），不是 source 吞吐**；勿当 Kafka source EPS。采样器优先 source vertex（见 `g6_sample_metrics`）；历史 JSON 仍为合计口径 | `metrics.throughput.flink_numRecordsInPerSecond` |
| **Kafka consumer lag** | `kafka-consumer-groups.sh --describe --group` (baseline / per-round / final) | `metrics.kafka_consumer_lag` + raw `lag_*.txt` |
| **Checkpoint duration** | Completed CP `end_to_end_duration` (ms) → avg / p50 / p95 | `metrics.checkpoint_duration` |
| **Doris-query-visible 批次探针** | 每轮 probe batch；`e2e_ms = doris_visible_wallclock - produce_probe_end`。路径：upsert-kafka ADS → **脚本** UNIQUE KEY 物化 → `SELECT`。含 console-consumer 串行开销 | `metrics.doris_e2e_latency` |
| **Backpressure** | `backPressuredTimeMsPerSecond` and/or `/vertices/:id/backpressure` | `metrics.backpressure` |

### How Doris-visible E2E is measured

1. Continuous load runs on `server_id=88` (player pool, no background recharges).
2. Probe batch adds **N new unique players** (+ M recharges among them). Generator tracks cumulative expected `dau` / `pay_users`.
3. Stamp `produce_epoch` = host wallclock at **end of probe produce**.
4. Loop: read latest upsert-kafka ADS → `INSERT` Doris UNIQUE KEY → `SELECT` ADS until `dau`/`pay_users` match expected (timeout → `未测到`).
5. Aggregate across successful rounds only. **两档各 2 次批次探针**（n=2）；**勿把 P95 当简历亮点**（n=2 ≈ max）。口径表述优先：「当前脚本物化路径下约 39–41s 可查」。含 console-consumer 串行开销。Same-host clocks.

## Tiers

| Tier | Default rate | Duration/round | Rounds | Intent |
|------|--------------|----------------|--------|--------|
| `light` | 30 evt/s | 30s | 2 | Should finish on ~7.6Gi without OOM |
| `medium` | 50 evt/s | 40s | 2 | Only if light is stable |

## Pipeline under test

```
continuous chunked JSONL (rate × duration) + probe batch
    → Kafka gamestream.g8s.ods.events
    → Flink SQL g8-continuous-ads (same G8 mainline semantics)
    → upsert-kafka gamestream.g8s.ads_dau / ads_pay_rate
    → script UNIQUE KEY materialize
    → Doris ads.ads_dau_di / ads.ads_pay_rate_di  (query-visible)
```

Orchestrator: [`scripts/g8_steady_bench.sh`](../scripts/g8_steady_bench.sh)  
Helpers: [`scripts/g8_steady_gen.py`](../scripts/g8_steady_gen.py), [`scripts/g8_steady_helpers.py`](../scripts/g8_steady_helpers.py), [`scripts/_g8s_lib.sh`](../scripts/_g8s_lib.sh)  
Reuses Flink sampler: [`scripts/g6_sample_metrics.py`](../scripts/g6_sample_metrics.py) (CP/BP/poll only — not G6 E2E numbers)

## How to reproduce (WSL)

```bash
cd "$PWD"  # 仓库根目录
# free memory if available << 1Gi before medium
docker compose ps

cp scripts/g8_steady_bench.sh /tmp/g8s.sh && sed -i 's/\r$//' /tmp/g8s.sh
# ensure helpers are LF (scripts sourced from GAMESTREAM_ROOT)
GAMESTREAM_ROOT="$(cd "$(dirname "$0")/.." && pwd)" TIER=light bash /tmp/g8s.sh
GAMESTREAM_ROOT="$(cd "$(dirname "$0")/.." && pwd)" TIER=medium bash /tmp/g8s.sh
```

Outputs:

- `bench/results/g8_<tier>_<ts>.json` — summary (committed)
- `bench/results/g8_raw_<tier>_<ts>/` — baseline, per-round lag/poll/e2e/CP, final snapshots
- `docs/g8-steady-bench-result.txt` — short human summary of last run

## Measured results

Fill **only** from committed JSON. If a field is missing, write `未测到`.

<!-- TABLE_START: populated after WSL run; values must match result file paths -->

## Measured results (from committed JSON only)

Primary runs: light `bench/results/g8_light_20260907T043148Z.json`, medium `bench/results/g8_medium_20260907T043820Z.json`.  
WSL run **2026-09-07**（UTC `04:31–04:46` ≈ **12:31–12:46 Asia/Shanghai**）. Rounds=2 each. Sink honesty: upsert-kafka ALS+PK → UNIQUE KEY materialize — **not** EO-2PC; **not** G6 Kafka-only E2E.

| Metric | Light (30 evt/s × 30s × 2) | Medium (50 evt/s × 40s × 2) | Source path |
|--------|----------------------------|-----------------------------|-------------|
| Throughput (input produce) | **11.097** evt/s (1844 events / 166.165s) | **17.914** evt/s (4088 events / 228.2069s) | `metrics.throughput.input_produce_events_per_sec` |
| Flink in/s（**sum across operators，非 source 吞吐**） | status=ok max=**161.7** avg=**58.693** | status=ok max=**316.867** avg=**121.699** | `metrics.throughput.flink_numRecordsInPerSecond` |
| Kafka consumer lag (baseline before load) | 未测到 — consumer group not created yet (`baseline_before_load=[]`) | 未测到 — same | `metrics.kafka_consumer_lag.baseline_before_load` |
| Kafka consumer lag (final) | lag=**0** (current=1844/1844) | lag=**0** (current=4088/4088) | `metrics.kafka_consumer_lag.final` |
| Checkpoint duration | n=10 avg=**22.7**ms p50=**22**ms p95=**32**ms | n=10 avg=**32.9**ms p50=**34**ms p95=**38**ms | `metrics.checkpoint_duration` |
| Doris 批次可见（约 39–41s 可查；n=2≈max，**勿卖 P95**） | n_ok=2 samples≈**39.4–39.5s**（p50=39368ms / 记作 p95=39499ms≈max） | n_ok=2 samples≈**39.9–40.6s**（p50=39948ms / 记作 p95=40572ms≈max） | `metrics.doris_e2e_latency` |
| Backpressure | observed=**False** max_ratio=**0.0** | observed=**False** max_ratio=**0.0** | `metrics.backpressure` |

Raw: `bench/results/g8_raw_light_20260907T043148Z/`, `bench/results/g8_raw_medium_20260907T043820Z/`. Human summary: [`g8-steady-bench-result.txt`](g8-steady-bench-result.txt).

<!-- TABLE_END -->

## Limitations

- ~7.6Gi WSL: light/medium only; Doris FE is memory-heavy.
- Doris 可见延迟含**脚本物化** + console-consumer 串行开销（诚实反映「ADS 可查」），不是纯 Flink sink 延迟。
- Flink in/s 历史样本为 **sum across operators**；**禁止**当作 source throughput / 简历单点吞吐。采样器现优先 source vertex，但已提交 JSON 数字不改写。
- 每档仅 n=2 探针；**P95≈max**，不能当稳态 P95 对外卖点。
- Flink metric id availability varies; absent → `未测到`.
- Kafka consumer-group lag may be unparseable until offsets commit on checkpoint; see raw `lag_*.txt`.

## Explicit non-claims

- Not a production capacity / SLA / 「稳态压测」number — 仅为小规模分块负载与批次可见性验证。
- Not G6 Kafka-only E2E relabeled as Doris-visible.
- Not end-to-end exactly-once into Doris (ALS + UNIQUE KEY only).
- 本结果 JSON 的 Doris 可见延迟 = **脚本物化**路径；常驻 materializer 另测另记。
