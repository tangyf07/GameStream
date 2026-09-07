# GameStream G8 — Resident Doris ADS Materializer

**Scope:** Independent **long-running** consumer that continuously materializes Flink **upsert-kafka** ADS changelog (`ads_dau` / `ads_pay_rate`) into Doris **UNIQUE KEY** tables (`ads.ads_dau_di` / `ads.ads_pay_rate_di`).

**Why:** G8 Flink continuous sink is upsert-kafka (ALS + PK) because Doris FE rejects Flink JDBC MySQL `ON DUPLICATE KEY UPDATE`. Continuous Doris **query visibility** therefore needs a writer outside Flink. This resident process replaces the old「wait for expected then materialize」happy path.

**Not in scope:** metric zoo; invented bench numbers; end-to-end EO-2PC; Flink JDBC upsert dialect.

## Pipeline

```
Kafka ODS events
    → Flink g8-continuous-ads (upsert-kafka ALS+PK)
        → gamestream.g8.ads_dau / gamestream.g8.ads_pay_rate
            → **resident materializer** (commit offset only after Doris write)
                → Doris ads.ads_dau_di / ads.ads_pay_rate_di  (UNIQUE KEY)
```

Acceptance scripts **produce + SELECT only** — they must **not** drive the write path.

## Semantics (honest boundaries)

| Point | Behavior |
|-------|----------|
| Delivery | **at-least-once** — Kafka offsets commit **only after** Doris write confirmed |
| Failure | Doris briefly down → retry; **do not advance / commit offset** |
| Upsert | plain `INSERT` into UNIQUE KEY → replace; duplicate upserts idempotent |
| Tombstone | upsert-kafka null value → `DELETE` by `(dt, server_id)` |
| Kill/restart | uncommitted offsets redelivered; UNIQUE KEY prevents gauge double-count |
| Not claimed | EO-2PC; exactly-once into Doris; Flink JDBC upsert |

## How to run

### A) Standalone resident process (preferred on ~7.6Gi WSL)

```bash
cd /mnt/c/Users/tangy/source/repos/GameStream
python3 -m pip install -r requirements-materializer.txt

# foreground
./scripts/run_g8_resident_materializer.sh run

# or background
./scripts/run_g8_resident_materializer.sh start
./scripts/run_g8_resident_materializer.sh status
./scripts/run_g8_resident_materializer.sh stop
```

Defaults: Kafka `localhost:19092`, Doris `127.0.0.1:9030`, topics `gamestream.g8.ads_dau` / `gamestream.g8.ads_pay_rate`, group `gamestream-g8-doris-materializer`.

### B) Docker Compose profile

```bash
docker compose --profile materializer up -d g8-doris-materializer
docker logs -f gs-g8-doris-materializer
```

Compose uses in-network `kafka:9092` + `doris-fe:9030`. Mem limit 256m.

### Failure recovery

1. Materializer crash / `kill` → restart `run`/`start` or compose recreate.
2. It resumes from last **committed** offsets (group id stable).
3. Doris FE/BE flap → process retries with backoff; offsets stay put until writes succeed.
4. After catch-up, `SELECT` ADS should match latest upsert-kafka state (UNIQUE KEY replace).

## Acceptance

```bash
# stack up (Kafka + Flink JM/TM + Doris FE/BE); ~7.6Gi light path
cp scripts/g8_resident_materializer_accept.sh /tmp/g8r.sh
sed -i 's/\r$//' /tmp/g8r.sh
GAMESTREAM_ROOT=/mnt/c/Users/tangy/source/repos/GameStream bash /tmp/g8r.sh
```

Proves:

1. New events **continuously update** Doris ADS **without** script-phase materialize in the wait loop.
2. Kill materializer → Doris frozen while Flink still sinks Kafka.
3. Restart → catch-up to expected `dau`/`pay_users` (**no loss**).
4. Replay produce → ADS stable (**no double-count** via event_id dedup + UNIQUE KEY).

Result transcript: [`g8-resident-materializer-result.txt`](g8-resident-materializer-result.txt).

## Files

| Path | Role |
|------|------|
| `pipeline/doris_ads_materializer.py` | Core consumer + Doris writer |
| `scripts/g8_resident_materializer.py` | CLI |
| `scripts/run_g8_resident_materializer.sh` | systemd-like start/stop/run |
| `scripts/g8_resident_materializer_accept.sh` | produce + SELECT acceptance |
| `requirements-materializer.txt` | kafka-python + PyMySQL |
| `docker-compose.yml` profile `materializer` | optional container |

## Relation to script-phase materialize

| Path | Role |
|------|------|
| **Resident materializer (this doc)** | **Primary** continuous Doris visibility |
| `g8_continuous_mainline` / `g8_steady_*` script `materialize_doris` | **Fallback / dev-only** — labeled; may remain for offline drills when resident is not running |

G8 continuous Flink job itself is unchanged (upsert-kafka sink).


## Real run

WSL run **2026-09-07**（≈ **19:02 Asia/Shanghai**）。job `1099b41406c615898595bcc279deba2b`。Acceptance = produce + Doris SELECT only.

| 阶段 | Doris `dau` | `pay_users` | `pay_rate` | notes |
|------|-------------|-------------|------------|-------|
| after_batch1 (resident) | **4** | **1** | 0.25 | try=2, ~4s |
| after_batch2 (resident) | **6** | **2** | ≈0.333 | try=1, ~3s |
| kill materializer + 25s | **6** | **2** | — | Doris frozen; Flink still sank Kafka |
| restart catch-up | **8** | **2** | 0.25 | try=1; no loss |
| dup produce replay | **8** | **2** | 0.25 | no double-count |

全量：[`g8-resident-materializer-result.txt`](g8-resident-materializer-result.txt)。
