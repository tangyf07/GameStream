# GameStream G4 — Checkpoint + Recharge Idempotency

**Scope:** G4 checkpoint enablement + restart recovery + recharge `event_id` idempotency **only**.  
**Not in scope:** G5 TaskManager kill drills, G6 skew, invented Lag/P95 bench numbers.

## Teachable narrative

```
produce recharge events (incl. duplicate event_id)
        │
        ▼
Flink SQL streaming job
  • checkpoint every 10s (aligned barriers)
  • barrier alignment → operator state snapshot
      - Rank dedup keyed state (seen event_id)
      - unbounded agg state (cnt / sum / players)
      - Kafka source offsets
  • externalized CP retained on cancel
        │
        ▼
cancel → restore from file:///checkpoints/... 
        │
        ▼
Kafka source resumes from offsets in CP
re-produce same event_ids → Rank absorbs → aggregates stable
new unique event_id → counts increment exactly once
```

## Checkpoint config (demo)

| Setting | Value | Where |
|--------|-------|--------|
| `execution.checkpointing.interval` | **10s** | compose `FLINK_PROPERTIES` + SQL `SET` |
| `execution.checkpointing.mode` | `EXACTLY_ONCE` | operator state + source offsets |
| `execution.checkpointing.externalized-checkpoint-retention` | `RETAIN_ON_CANCELLATION` | so cancel keeps a restore path |
| `state.backend.type` | `hashmap` | fine for tiny demo state |
| `state.checkpoints.dir` | `file:///checkpoints` | host volume on **JM and TM** |
| compose volume | `./flink/checkpoints:/checkpoints` | both `jobmanager` and `taskmanager` |

Production note: prefer RocksDB + durable object store (S3/HDFS); see `flink/conf/checkpoint-recommendations.yaml`.

## Pipeline

```
JSONL recharge events
        ▼
Kafka gamestream.g4.recharge
        ▼
Flink SQL (g4-checkpoint-idempotency)
  ROW_NUMBER PARTITION BY event_id → first wins
        ├─► upsert-kafka gamestream.g4.recharge_dedup
        └─► unbounded GROUP BY → upsert-kafka gamestream.g4.recharge_agg
                 │
                 └─ e2e loads → Doris ads.g4_recharge_di (UNIQUE KEY)
                    (optional; avoids JDBC during streaming on ~7.6Gi)
```

Why Rank → unbounded agg works here (unlike G3 Rank→TUMBLE): window ops need append-only input; unbounded aggregation accepts retract/changelog streams from `ROW_NUMBER`.

## Operator state + Kafka offsets on restart

| Component | What is in the checkpoint | After restore |
|-----------|---------------------------|---------------|
| Kafka source | partition offsets | resume **from CP offsets** (not necessarily consumer-group committed offsets) |
| Rank dedup | keyed state per `event_id` | already-seen ids stay suppressed |
| Agg | `recharge_cnt` / `amount_sum` / `player_cnt` | continues from snapshotted totals |
| Upsert-kafka sink | not a transactional EO sink | at-least-once writes; PK upsert is idempotent |

If the last completed CP is **after** all input was processed, cancel→restore alone will **not** replay history (offsets already at tip). The E2E script therefore **re-produces duplicate `event_id`s** after restore to prove Rank state came back. That matches the interview story: replay / duplicates are absorbed by idempotent keyed state.

## Sink consistency class (honest)

| Sink | Guarantee claimed here |
|------|------------------------|
| Flink checkpoint | Exactly-once **for Flink state + source offsets** (aligned CP) |
| upsert-kafka agg/dedup | **At-least-once** delivery + **primary-key idempotent upsert** — **not** Kafka transactional exactly-once |
| Doris `ads.g4_recharge_di` | Load/JDBC path is **at-least-once**; table **UNIQUE KEY(`metric_id`)** absorbs retries — **not** end-to-end EO-2PC |

Do **not** claim end-to-end exactly-once unless you wire a true transactional sink (e.g. Kafka `sink.delivery-guarantee=exactly-once` + `isolation.read_committed`, or a 2PC-capable connector) and prove it.

## Fixture expectations

Batch 1: 7 Kafka lines = **5 unique** `event_id` + 2 dups.

| `event_id` | amount |
|------------|--------|
| g4-r-01..05 | 6+12+30+68+128 = **244.00** |

| Stage | `recharge_cnt` | `amount_sum` |
|-------|----------------|--------------|
| After batch1 (dups included) | **5** | **244.00** |
| After restore + dup replay | **5** (stable) | **244.00** |
| After new `g4-r-06` (+98) | **6** | **342.00** |

Without dedup, batch1 would look like cnt=7 / sum=280.

## How to run (WSL)

```bash
cd "$PWD"  # 仓库根目录
# After compose change: recreate Flink so /checkpoints is mounted
docker compose up -d --force-recreate jobmanager taskmanager
# wait until UI :8081 has slots

cp scripts/g4_checkpoint_idempotency.sh /tmp/g4.sh
sed -i 's/\r$//' /tmp/g4.sh
GAMESTREAM_ROOT="$(cd "$(dirname "$0")/.." && pwd)" bash /tmp/g4.sh
```

Result paste: [`g4-checkpoint-idempotency-result.txt`](g4-checkpoint-idempotency-result.txt).


## Real run sample

WSL run **2026-09-07** (UTC `01:32:45` ≈ **09:32 Asia/Shanghai**).

- job1 `5f427702ae6ca072d625a183ff574419` → cancel after ≥1 completed CP
- restore path `file:/checkpoints/5f427702ae6ca072d625a183ff574419/chk-1`
- job2 `5e71911ff90391a1d4b9bbc251837334` restored via `execution.savepoint.path`
- Full paste: [`g4-checkpoint-idempotency-result.txt`](g4-checkpoint-idempotency-result.txt)

```
checkpoint completed_count=1  state_size=24585
before_cancel:            recharge_cnt=5 amount_sum=244 player_cnt=5
after_restore_dup_replay: recharge_cnt=5 amount_sum=244  (stable — no double count)
after_new g4-r-06 (+98):  recharge_cnt=6 amount_sum=342
Doris ads.g4_recharge_di: g4_recharge | 6 | 342.00 | 6
OVERALL: PASS
```

Notes from this run:
- REST `counts` also showed `failed: 1` alongside `completed: 1` (first trigger can fail while JM/TM settle on `file://` volume); restore used the **completed** external path.
- Dedup audit ended with 6 unique `event_id`s (`g4-r-01`…`06`).

## Files

| Path | Role |
|------|------|
| `docker-compose.yml` | JM/TM CP props + `./flink/checkpoints:/checkpoints` |
| `flink/sql/g4_checkpoint_idempotency.sql` | Streaming SQL + CP `SET`s |
| `scripts/g4_checkpoint_idempotency.sh` | Produce → CP → cancel → restore → prove stable counts |
| `sql/ddl/doris_g4.sql` | `ads.g4_recharge_di` (`replication_num=1`) |
| `flink/checkpoints/` | Host dir for `file:///checkpoints` (runtime artifacts gitignored) |

## Limitations

- HashMap + local `file://` volume is a **laptop demo**, not HA durable state.
- sql-client restore uses `execution.savepoint.path` pointing at an **externalized checkpoint** (compatible for this demo; production often uses explicit savepoints).
- Doris path is demo load from Kafka agg, not a Flink JDBC streaming sink under RAM pressure.
- No G5 TM-kill chaos, no G6 skew, no fabricated latency SLAs.
