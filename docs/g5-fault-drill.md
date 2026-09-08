# GameStream G5 — TaskManager Kill / Fault Drill

**Scope:** G5 only — real `docker kill gs-flink-tm`, Flink restart-strategy recovery from checkpoint, recharge `event_id` idempotency (reuse G4).  
**Not in scope:** G6 skew / invented Lag·P95, G7 polish-only.

## Interview narrative（项目里真杀过 TM）

```
produce recharge events (incl. duplicate event_id)
        │
        ▼
Flink SQL streaming job (g5-fault-drill)
  • checkpoint every 10s → barrier alignment
  • operator state snapshot
      - Rank dedup keyed state (seen event_id)
      - unbounded agg (cnt / sum / players)
      - Kafka source offsets
  • upsert-kafka sink (ALS + PK idempotent)
        │
        ▼
≥1 completed externalized CP on file:///checkpoints
        │
        ▼
*** docker kill gs-flink-tm ***   (JM + Kafka + Doris stay up)
        │
        ▼
compose restart: unless-stopped → TM container back
restart-strategy fixed-delay → SAME job RESTARTING → RUNNING
resume from last completed CP (offsets + Rank + agg)
        │
        ▼
re-produce duplicate event_ids → Rank absorbs → money stable
new unique event_id → counts +1 exactly once
```

Key interview points:

1. **Checkpoint** stores aligned operator state + Kafka source offsets (not “just a file dump”).
2. **Barrier** alignment makes the snapshot consistent across the DAG.
3. **Kill TM** (not JM) exercises *failover*, not cancel→manual restore (that was G4).
4. **restart-strategy** is what brings the *same* job back from the last completed CP.
5. **Replay / dups** after recover prove Rank state + sink idempotency — **prior money must not double**.

## Config delta vs G4

| Setting | Value | Why |
|--------|-------|-----|
| `restart-strategy.type` | `fixed-delay` | auto-recover after TM loss |
| `restart-strategy.fixed-delay.attempts` | `10` | enough for docker TM restart latency |
| `restart-strategy.fixed-delay.delay` | `5s` (SQL) / `10s` (compose) | backoff while slots return |
| `taskmanager.restart` | `unless-stopped` | docker brings TM container back |
| checkpoint props | same as G4 (10s, hashmap, `file:///checkpoints`) | reuse proven G4 volume |

## Pipeline

```
JSONL recharge events
        ▼
Kafka gamestream.g5.recharge
        ▼
Flink SQL (g5-fault-drill)
  ROW_NUMBER PARTITION BY event_id → first wins
        ├─► upsert-kafka gamestream.g5.recharge_dedup
        └─► unbounded GROUP BY → upsert-kafka gamestream.g5.recharge_agg
                 │
                 └─ load → Doris ads.g5_recharge_di (UNIQUE KEY)
```

## Fixture expectations

Same money shape as G4 (different `event_id` / `player_id` prefix):

| Stage | `recharge_cnt` | `amount_sum` |
|-------|----------------|--------------|
| After batch1 (5 unique + 2 dups) | **5** | **244.00** |
| After TM kill + recover + dup replay | **5** (stable) | **244.00** |
| After new `g5-r-06` (+98) | **6** | **342.00** |

## How to run (WSL)

```bash
cd "$PWD"  # 仓库根目录
# After compose change: recreate Flink so restart-strategy is live
docker compose up -d --force-recreate jobmanager taskmanager
# wait until UI :8081 has slots (≥1)

cp scripts/g5_fault_drill.sh /tmp/g5.sh
sed -i 's/\r$//' /tmp/g5.sh
GAMESTREAM_ROOT="$(cd "$(dirname "$0")/.." && pwd)" bash /tmp/g5.sh
```

Prefer **`docker kill gs-flink-tm`** (script default). Do **not** casually kill Doris/Kafka on ~7.6Gi.

Result paste: [`g5-fault-drill-result.txt`](g5-fault-drill-result.txt).

## Real run sample

WSL run **2026-09-07** (UTC `01:44:26` ≈ **09:44 Asia/Shanghai**).

- job `c76151f11a3062940253c74c0aa62199` (same id before/after failover)
- CP before kill: `file:/checkpoints/c76151f11a3062940253c74c0aa62199/chk-1` (completed=1)
- kill: `docker kill gs-flink-tm` → exit 137; compose policy `restart: unless-stopped`
- REST: `RUNNING` → (TM down, JM still sees stale slots briefly) → `RESTARTING` → `RUNNING`
- Full paste: [`g5-fault-drill-result.txt`](g5-fault-drill-result.txt)

```
before_tm_kill:            recharge_cnt=5 amount_sum=244
after_failover_settle:     recharge_cnt=5 amount_sum=244
after_failover_dup_replay: recharge_cnt=5 amount_sum=244  (stable — no double money)
after_new g5-r-06 (+98):   recharge_cnt=6 amount_sum=342
Doris ads.g5_recharge_di:  g5_recharge | 6 | 342.00 | 6
OVERALL: PASS
```

Timeline (UTC): kill `01:42:31` → RESTARTING `01:43:16` → RUNNING `01:43:24` → slots back `01:43:28`.


## Sink consistency class (honest)

| Layer | Claim |
|-------|--------|
| Flink CP + restart | Exactly-once **for Flink state + source offsets** on failover |
| upsert-kafka | **At-least-once** + PK upsert — **not** transactional EO |
| Doris UNIQUE KEY | Absorbs ALS retries — **not** end-to-end EO-2PC |

This drill proves **failover correctness** (no double money after TM kill), not two-phase commit across Flink↔Kafka↔Doris.

## Files

| Path | Role |
|------|------|
| `docker-compose.yml` | JM/TM CP + **restart-strategy** + TM `restart: unless-stopped` |
| `flink/sql/g5_fault_drill.sql` | Streaming SQL + CP + restart `SET`s |
| `scripts/g5_fault_drill.sh` | Produce → CP → **kill TM** → wait recover → prove counts |
| `sql/ddl/doris_g5.sql` | `ads.g5_recharge_di` |

## Limitations / honest failures

- HashMap + local `file://` is a laptop demo, not HA durable state.
- Sink remains ALS + idempotent upsert (same honesty as G4).
- Optional “brief lag then catch-up” is observed only if REST/consumer timing makes it obvious — **do not invent Lag numbers**.
- ~7.6Gi: kill **TM only**; Doris/Kafka restarts are out of scope for this drill.

### Honest failure from the real drill

- Compose had `restart: unless-stopped` on TM, but **Docker Desktop/WSL did not auto-restart `gs-flink-tm` promptly** after `docker kill` (stayed `Exited (137)` for ~40s+ while JM still briefly reported stale slots).
- Fix: operator/script issued `docker start gs-flink-tm`; script now includes an automatic **`docker start` fallback after 15s** if TM is still not `running`, and logs `tm_start_fallback=1`.
- Flink side worked as designed once slots returned: **same job id** `RESTARTING` → `RUNNING` from last completed CP; aggregates did not double-count.

## vs G4

| | G4 | G5 |
|--|----|----|
| Failure mode | `cancel` job → manual `execution.savepoint.path` restore | **`docker kill` TM** → automatic restart-strategy |
| Job identity | new job id after restore | **same job id** recovers RUNNING |
| Story focus | CP retain + Rank state after cancel | **「项目里真杀过 TM」** failover |
