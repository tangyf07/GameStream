# GameStream G3 — Event Time / Watermark / Disorder / Dedup

**Scope:** G3 stream semantics **only**.  
**Not in scope:** G4 checkpoint kill, G5 skew, G6 bench Lag/P95 numbers.

## What this proves

| Concern | Mechanism | Proof in fixture |
|--------|-----------|------------------|
| Event time | `WATERMARK FOR event_time AS event_time - INTERVAL 'N' SECOND` | Window keys are payload timestamps, not wall clock |
| Out-of-order within bound | Bounded OOO = N seconds (default **5**) | `g3-ooo-lo` lags `g3-ooo-hi` by &lt; N → counted in W1 |
| Late beyond watermark | Flink SQL **drop** (default `allowedLateness=0`) | `g3-late-01` arrives after W1 closed → **not** in W1 |
| Side output | **Not in pure SQL** | Would need DataStream `sideOutput`; we document drop + before/after counts |
| `event_id` dedup | `ROW_NUMBER` audit topic + `COUNT(DISTINCT event_id)` in window | Replay `g3-dup-01` twice → W1 `event_cnt=7` not 8 |

## Topology

```
scripts/g3_gen_disorder_events.py
        │ JSONL (flat fields)
        ▼
Kafka gamestream.g3.events  ──► Flink SQL (streaming)
                                   │  watermark N seconds
                                   │  ROW_NUMBER → upsert-kafka gamestream.g3.dedup_audit
                                   │  TUMBLE 1 min → COUNT(DISTINCT event_id)
                                   └─► Kafka gamestream.g3.window_results
                                            │
                                            └─ e2e loads → Doris ads.g3_window_demo
                                               (avoids dual window+JDBC on ~7.6Gi RAM)
```

## Why Rank is not chained into the window

Flink **window aggregation requires an append-only input**. `ROW_NUMBER…WHERE rn=1` is an upsert/changelog operator, so Rank→Window is rejected by the planner. G3 therefore:

1. Keeps the **ROW_NUMBER** pattern as a real INSERT into upsert-kafka `gamestream.g3.dedup_audit` (changelog sink; plain Kafka append sink rejects Deduplicate).
2. Uses **`COUNT(DISTINCT event_id)`** on the watermarked source for TUMBLE results (window-safe dedup).

Row-level upsert-kafka dedup remains the production ODS pattern (`flink/sql/01_ods_clean.sql`).

## Late-data policy (honest)

- **Policy:** drop after watermark (no lateness).
- **Side output:** not available in this pure SQL job.
- **How we prove drop:** generator prints expected W1 `event_cnt=7` / `player_cnt=7` excluding `g3-late-01` (`player_id=9001`). If late were kept, player_cnt would be 8.

## State / TTL boundaries

- Rank dedup audit: keyed state per `event_id`, **no `state.ttl.time`** configured → OK only for tiny demos.
- Window `COUNT(DISTINCT …)`: also keyed state per window; windows clear after fire (no allowed lateness).
- Idle timeout: `table.exec.source.idle-timeout=5s` so an idle Kafka source does not permanently stall watermarks after produce stops.

## How to run (WSL)

```bash
cd "$PWD"  # 仓库根目录
# stack already up from G1/G2
cp scripts/g3_stream_semantics.sh /tmp/g3_stream_semantics.sh
sed -i 's/\r$//' /tmp/g3_stream_semantics.sh
GAMESTREAM_ROOT="$(cd "$(dirname "$0")/.." && pwd)" bash /tmp/g3_stream_semantics.sh
```

Overrides:

```bash
G3_WATERMARK_SECONDS=5 G3_WAIT_SEC=45 bash /tmp/g3_stream_semantics.sh
```


## Phased produce (important)

Flink SQL watermarks are **periodic**. If all fixtures are produced in one burst, the late event can be ingested **before** the watermark catches up to `window_end`, so it is incorrectly counted in W1.

`scripts/g3_stream_semantics.sh` therefore produces in phases:

1. W1 + OOO + dup + `close_w1`
2. sleep ~20s (W1 fires with expected 7/7)
3. `g3-late-01` (should be dropped)
4. W2 remainder + sleep for W2 close

## Expected windows (default fixture)

Base `2026-09-07 10:00:00`, watermark **5s**, produce **12** lines / **11** unique ids:

| window | event_cnt | player_cnt | notes |
|--------|-----------|------------|-------|
| [10:00, 10:01) | **7** | **7** | dup once; late dropped; OOO within bound kept |
| [10:01, 10:02) | **2** | **2** | `close_w1` + `w2` |

## Real run sample

WSL run 2026-09-07 (UTC+8 ≈ 09:14): phased produce, watermark=5s, job `2b3d67892b65ade8c476874a333aac50`.
Full paste: `docs/g3-stream-semantics-result.txt`.

```
Kafka W1: {"window_start":"2026-09-07 10:00:00","window_end":"2026-09-07 10:01:00","event_cnt":7,"player_cnt":7}
Kafka W2: {"window_start":"2026-09-07 10:01:00","window_end":"2026-09-07 10:02:00","event_cnt":2,"player_cnt":2}
Doris ads.g3_window_demo:
2026-09-07 10:00:00 | 10:01:00 | event_cnt=7 | player_cnt=7
2026-09-07 10:01:00 | 10:02:00 | event_cnt=2 | player_cnt=2
OK window 10:00 (7/7) — dup once, late dropped
OK window 10:01 (2/2)
OVERALL: PASS
```

What the numbers show:
- Produce 12 lines / 11 unique `event_id`; W1 `event_cnt=7` ⇒ replay of `g3-dup-01` did **not** double-count.
- W1 `player_cnt=7` (not 8) ⇒ `g3-late-01` (`player_id=9001`) did **not** enter the closed window after phased produce.
- Dedup audit topic still lists `g3-late-01` (ROW_NUMBER is not watermark-filtered); window drop is the late policy proof.


## Files

| Path | Role |
|------|------|
| `flink/sql/g3_event_time_watermark.sql` | Streaming SQL |
| `scripts/g3_gen_disorder_events.py` | Controlled disorder/late/dup JSONL |
| `scripts/g3_stream_semantics.sh` | E2E: topic → Flink → sinks → compare |
| `sql/ddl/doris_g3.sql` | `ads.g3_window_demo` (`replication_num=1`) |
