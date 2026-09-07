# GameStream G2 E2E — Simulator → Kafka → Flink → Doris ADS

**Status:** verified on WSL single-node Docker (~7.6Gi RAM) with small event volume.  
**Not in scope:** G3–G6 watermark/checkpoint failure drills, fabricated Lag/throughput/P95, Spark/Iceberg/K8s.

## Topology

| Step | Component | Endpoint / name |
|------|-----------|-----------------|
| Produce | `simulator/generate_events.py` + `kafka-console-producer` | topic `gamestream.ods.player_events` |
| Bootstrap (in-network) | Kafka | `kafka:9092` |
| Host produce port | Kafka EXTERNAL | `localhost:19092` |
| Transform | Flink 1.18 SQL Client | UI `http://127.0.0.1:8081` |
| Sink | Doris FE MySQL protocol | `doris-fe:9030` (JDBC) |
| ADS tables | `ads.ads_dau_di`, `ads.ads_pay_rate_di` | `metric_id` = table name |

## Sink choice

**Flink JDBC → Doris FE:9030** (MySQL driver), not flink-doris-connector.  
Reason: lighter deps for laptop RAM; Flink still owns clean/filter/aggregate. Doris **UNIQUE KEY + REPLACE** accepts re-INSERT. Job uses **`execution.runtime-mode=batch`** + Kafka **`scan.bounded.mode=latest-offset`** so aggregation is append-only and finishes.

Connectors (mounted at `/opt/flink/usrlib`):

- `flink-sql-connector-kafka-3.0.2-1.18.jar`
- `flink-connector-jdbc-3.1.2-1.17.jar`
- `mysql-connector-j-8.0.33.jar`

## metric_id

Aligned with `config/metrics.yaml`:

- `ads_dau_di` — `COUNT(DISTINCT player_id)` per `(dt, server_id)` on cleaned ODS events
- `ads_pay_rate_di` — pay_users (`event_type=recharge`) / dau

DDL: `sql/ddl/doris_ads_g2.sql` (`replication_num=1`, no dynamic_partition).

## How to run

```bash
cd /mnt/c/Users/tangy/source/repos/GameStream
docker compose up -d   # if not already
bash scripts/e2e_g2.sh
# overrides: PLAYERS=500 EVENTS=3000 DAYS=1 bash scripts/e2e_g2.sh
```

Submit-only (after data already in Kafka):

```bash
docker exec gs-flink-jm /opt/flink/bin/sql-client.sh -f /opt/flink/sql/g2_kafka_to_doris.sql
```

## Sample SQL

```sql
SELECT dt, server_id, dau, metric_id
FROM ads.ads_dau_di
ORDER BY dt, server_id;

SELECT dt, server_id, dau, pay_users, pay_rate, metric_id
FROM ads.ads_pay_rate_di
ORDER BY dt, server_id;
```

## Real query result

_(filled by `scripts/e2e_g2.sh` → `docs/e2e-g2-query-result.txt`; paste below after run)_

```
(pending e2e run)
```

## Known pitfalls (honest)

1. **Single BE → `replication_num=1`**. Default `doris_ads.sql` previously used `3` and CREATE fails with one BE.
2. **Dynamic partition** not used in G2 DDL; keeps bring-up simple.
3. **Flink image has no Kafka/JDBC connectors** — must mount `flink/jars` into JM/TM `/opt/flink/usrlib` and recreate containers after compose change.
4. **Produce before Flink submit** — bounded `latest-offset` captures offsets at job start; late produce is invisible to that run.
5. **Host RAM ~7.6Gi** — keep `EVENTS` to a few thousand; do not co-start Spark/K8s.
6. **Windows quoting** — run via `wsl -e bash .../script.sh` after `sed` CRLF; avoid complex PowerShell quoting.
7. **Doris BE must be Alive** (`SHOW BACKENDS`) before DDL; FE healthy alone is not enough.
8. **Payload JSON shape** — unused payload fields may be absent; Flink `ROW<>` + `json.ignore-parse-errors=true` tolerates sparse objects.

9. **`docker exec` sql-client classpath**: entrypoint sets `/opt/flink/usrlib` on JM/TM start, but `docker exec ... sql-client.sh` does **not**. Always pass `-j` for Kafka/JDBC/MySQL jars (see `scripts/e2e_g2.sh`).
10. **Doris 3.0 Unique DDL**: avoid legacy `REPLACE NULL_DEFAULT`; use plain `DEFAULT "0"` column defs for G2 demo tables (`sql/ddl/doris_ads_g2.sql`).
11. **`/mnt/c` script corruption**: trailing `r` on lines ending with `...manager` has been observed; copy scripts to `/tmp` before bash.
