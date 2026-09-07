# GameStream G6 — Measured Bench (Docker / WSL)

**Scope:** G6 measured numbers only — throughput, Kafka consumer lag, checkpoint duration, E2E P95, backpressure.  
**Not in scope:** G7 polish/docs sprawl, skew-specialization theater, invented SLAs.

**Honesty rule:** every number in the summary table comes from a committed result JSON under `bench/results/g6_*.json` (or is explicitly `未测到` with reason). **Never invent.**

## Environment

- WSL Docker stack (~7.6Gi): Kafka + Flink JM/TM (+ Doris optional; G6 prefers **Kafka sink**)
- Base: G5 main; reuses streaming submit path like G4 (`sql-client` + Kafka connector jar)
- Checkpoint volume: `./flink/checkpoints:/checkpoints` (same as G4/G5)

## Definitions

| Metric | Definition used here | Source |
|--------|----------------------|--------|
| **Throughput (input)** | `events / produce_wall_sec` while `kafka-console-producer` ingests the JSONL | `metrics.throughput.input_produce_events_per_sec` |
| **Throughput (Flink)** | Sampled `numRecordsInPerSecond` via Flink REST `/jobs/:id/metrics` during drain poll | `metrics.throughput.flink_numRecordsInPerSecond` |
| **Kafka consumer lag** | `kafka-consumer-groups.sh --describe --group <Flink group>` (CURRENT vs LOG-END) | `metrics.kafka_consumer_lag` + raw `lag_*.txt` |
| **Checkpoint duration** | Completed CP `end_to_end_duration` (ms) from `/jobs/:id/checkpoints` history → avg / p50 / p95 | `metrics.checkpoint_duration` |
| **E2E P95** | `P95(sink_ts - produce_ts)` where `produce_ts` is stamped on host immediately before produce, `sink_ts` is Flink `CURRENT_TIMESTAMP` at sink `SELECT` | `metrics.e2e_latency` |
| **Backpressure** | `backPressuredTimeMsPerSecond` (REST metrics) and/or `/vertices/:id/backpressure` | `metrics.backpressure` |

### E2E latency caveats

- Clocks: host UTC vs Flink container UTC are probed into `clock_skew` in the result file. If skew ≫ latency, absolute E2E is **not trustworthy** — report skew and mark limitation.
- This is **not** event_time→ADS Doris wallclock (Doris skipped by default on fragile BE). It is **produce wallclock → Flink sink projection wallclock → Kafka out topic**.

## Tiers

| Tier | Default events | Intent |
|------|----------------|--------|
| `light` | 10_000 | Should finish on ~7.6Gi without OOM |
| `medium` | 30_000 | Optional; only if light is stable |

## Pipeline under test

```
JSONL (produce_ts stamped)
    → Kafka gamestream.g6.events
    → Flink SQL g6-bench (streaming, CP 10s, parallelism 1)
    → Kafka gamestream.g6.out (event + produce_ts + sink_ts)
```

SQL: [`flink/sql/g6_bench.sql`](../flink/sql/g6_bench.sql)  
Orchestrator: [`scripts/g6_bench.sh`](../scripts/g6_bench.sh)  
Helpers: [`scripts/g6_sample_metrics.py`](../scripts/g6_sample_metrics.py), [`scripts/g6_gen_and_produce.py`](../scripts/g6_gen_and_produce.py)

## How to reproduce (WSL)

```bash
# stack up (Kafka+Flink required). Do not casually kill Doris.
docker compose ps

cp scripts/g6_bench.sh /tmp/g6.sh && sed -i 's/\r$//' /tmp/g6.sh
cp scripts/g6_sample_metrics.py /tmp/g6_sample_metrics.py && sed -i 's/\r$//' /tmp/g6_sample_metrics.py
cp scripts/g6_gen_and_produce.py /tmp/g6_gen_and_produce.py && sed -i 's/\r$//' /tmp/g6_gen_and_produce.py
# scripts resolve via GAMESTREAM_ROOT; ensure helpers exist in repo scripts/

GAMESTREAM_ROOT=/mnt/c/Users/tangy/source/repos/GameStream \
  TIER=light \
  bash /tmp/g6.sh
```

Optional medium:

```bash
GAMESTREAM_ROOT=/mnt/c/Users/tangy/source/repos/GameStream TIER=medium bash /tmp/g6.sh
```

Outputs:

- `bench/results/g6_<tier>_<ts>.json` — summary (committed)
- `bench/results/g6_raw_<tier>_<ts>/` — lag txt, poll JSON, sink sample, CP raw
- `docs/g6-bench-result.txt` — short human summary of last run

## Measured results

Fill **only** from committed JSON. If a field is missing, write `未测到`.

<!-- TABLE_START: populated after WSL run; values must match result file paths -->

## Measured results (from committed JSON only)

Primary runs used in the table: light `bench/results/g6_light_20260907T020703Z.json`, medium `bench/results/g6_medium_20260907T020915Z.json`.  
Earlier light run also committed: `bench/results/g6_light_20260907T020246Z.json` (auditable).

| Metric | Light (10k) | Medium (30k) | Source file path |
|--------|-------------|--------------|------------------|
| Throughput (input produce) | 3094.347 evt/s | 6088.651 evt/s | `bench/results/g6_light_20260907T020703Z.json` / `bench/results/g6_medium_20260907T020915Z.json` → `metrics.throughput.input_produce_events_per_sec` |
| Throughput (Flink numRecordsInPerSecond) | 未测到 — job-level metric ids empty after drain | 未测到 — same | `bench/results/g6_light_20260907T020703Z.json` / `bench/results/g6_medium_20260907T020915Z.json` → `metrics.throughput.flink_numRecordsInPerSecond` |
| Kafka consumer lag (after drain) | lag=0 (current=10000/10000) | lag=0 (current=30000/30000) | `bench/results/g6_light_20260907T020703Z.json` / `bench/results/g6_medium_20260907T020915Z.json` → `metrics.kafka_consumer_lag.t1_after_drain` (+ raw `lag_t1.txt`) |
| Checkpoint duration | n=5 avg=32.2ms p50=18ms p95=92ms | n=6 avg=18.33ms p50=14ms p95=37ms | `bench/results/g6_light_20260907T020703Z.json` / `bench/results/g6_medium_20260907T020915Z.json` → `metrics.checkpoint_duration` |
| E2E P95 (sink_ts − produce_ts) | n=10000 p95=2930.0ms p50=2679.0ms avg=2654.061ms | n=30000 p95=4387.0ms p50=3803.0ms avg=3645.533ms | `bench/results/g6_light_20260907T020703Z.json` / `bench/results/g6_medium_20260907T020915Z.json` → `metrics.e2e_latency` |
| Backpressure | observed=False level=['ok'] ratio_max=0.0 | observed=False level=['ok'] ratio_max=0.0 | `bench/results/g6_light_20260907T020703Z.json` / `bench/results/g6_medium_20260907T020915Z.json` → `metrics.backpressure` (vertex REST); job-level `backPressuredTimeMsPerSecond` 未测到 |
| Clock skew (host vs Flink JM) | 0.0s | 1.0s | `clock_skew` in same JSON |

Raw sampler artifacts (no full sink dump): `bench/results/g6_raw_light_20260907T020703Z/`, `bench/results/g6_raw_medium_20260907T020915Z/` (plus earlier light raw). Full sink line counts recorded in each raw dir `sink_note.txt`; head sample in `sink_sample_head20.jsonl`.

<!-- TABLE_END -->

## Limitations

- ~7.6Gi WSL: keep tiers small; Flink TM ~1Gi / JM ~768Mi in compose.
- Doris not required for G6; no Doris ADS E2E latency claimed here.
- Flink metric id availability varies; if `numRecordsInPerSecond` / backpressure metrics are absent, status=`未测到` with reason — do not substitute guesses.
- Kafka consumer-group lag may show 0 or be unparseable if the connector commits offsets only on checkpoints; see raw `lag_*.txt`.
- E2E depends on clock alignment (`clock_skew` in JSON).

## Explicit non-claims

- Not a production capacity number / SLA.
- Not G7 documentation-only expansion.
- Not end-to-end exactly-once into Doris.
