#!/usr/bin/env bash
# GameStream G2 E2E: Simulator → Kafka → Flink (batch, bounded) → Doris ADS
# Prefer: cp scripts/e2e_g2.sh /tmp/e2e_g2.sh && sed -i 's/\r$//' /tmp/e2e_g2.sh && GAMESTREAM_ROOT=... bash /tmp/e2e_g2.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ROOT="${GAMESTREAM_ROOT:-$ROOT}"
if [[ ! -f "$ROOT/docker-compose.yml" ]]; then
  echo "ERROR: cannot find GameStream repo root (set GAMESTREAM_ROOT or run from repo scripts/)." >&2
  exit 1
fi
cd "$ROOT"

PLAYERS="${PLAYERS:-500}"
EVENTS="${EVENTS:-3000}"
SERVERS="${SERVERS:-4}"
DAYS="${DAYS:-1}"
SEED="${SEED:-42}"
TOPIC="${TOPIC:-gamestream.ods.player_events}"
EVENTS_FILE="${EVENTS_FILE:-/tmp/gamestream_g2_events.jsonl}"
RESULT_FILE="${RESULT_FILE:-docs/e2e-g2-query-result.txt}"

echo "[g2] root=$ROOT players=$PLAYERS events=$EVENTS days=$DAYS"

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing: $1"; exit 1; }; }
need docker
need python3

echo "[g2] 1/6 check stack"
docker compose ps
curl -sf http://127.0.0.1:8081/overview >/dev/null

for i in $(seq 1 30); do
  AVAIL=$(curl -sf http://127.0.0.1:8081/overview | sed -n 's/.*"slots-total":\([0-9]*\).*/\1/p')
  if [[ "${AVAIL:-0}" -ge 1 ]]; then
    echo "[g2] Flink slots-total=$AVAIL"
    break
  fi
  sleep 2
done

for i in $(seq 1 30); do
  if docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e "SHOW BACKENDS;" >/dev/null 2>&1; then
    break
  fi
  sleep 3
done
docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e "SHOW BACKENDS;" >/dev/null

echo "[g2] 2/6 ensure Flink usrlib jars visible"
if ! docker exec gs-flink-jm ls /opt/flink/usrlib/flink-sql-connector-kafka-3.0.2-1.18.jar >/dev/null 2>&1; then
  echo "[g2] jars missing — recreate jobmanager then taskmanager"
  docker compose up -d --force-recreate jobmanager
  docker compose up -d --force-recreate taskmanager
  sleep 10
fi
docker exec gs-flink-jm ls -la /opt/flink/usrlib/

echo "[g2] 3/6 Doris DDL (replication_num=1) + clear demo ADS for this-run proof"
docker exec -i gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot < sql/ddl/doris_ads_g2.sql
docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e "USE ads; SHOW TABLES;"
# Clear leftover rows so PASS cannot come from a prior run
docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e \
  "TRUNCATE TABLE ads.ads_dau_di; TRUNCATE TABLE ads.ads_pay_rate_di;" 2>/dev/null \
  || docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e \
    "DELETE FROM ads.ads_dau_di; DELETE FROM ads.ads_pay_rate_di;"
echo "[g2] demo ADS tables cleared"

echo "[g2] 4/6 Kafka topic + produce events"
docker exec gs-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --delete --topic "$TOPIC" 2>/dev/null || true
sleep 2
docker exec gs-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic "$TOPIC" --partitions 1 --replication-factor 1

python3 simulator/generate_events.py \
  --players "$PLAYERS" --events "$EVENTS" --servers "$SERVERS" --days "$DAYS" --seed "$SEED" \
  --out "$EVENTS_FILE"

echo "[g2] publishing $(wc -l < "$EVENTS_FILE") lines to $TOPIC"
docker exec -i gs-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic "$TOPIC" < "$EVENTS_FILE"
sleep 3

echo "[g2] 5/6 submit Flink SQL (batch + bounded Kafka → Doris JDBC)"
# docker exec bypasses entrypoint classpath — pass -j explicitly for sql-client factory discovery
set +e
docker exec gs-flink-jm /opt/flink/bin/sql-client.sh \
  -j /opt/flink/usrlib/flink-sql-connector-kafka-3.0.2-1.18.jar \
  -j /opt/flink/usrlib/flink-connector-jdbc-3.1.2-1.17.jar \
  -j /opt/flink/usrlib/mysql-connector-j-8.0.33.jar \
  -f /opt/flink/sql/g2_kafka_to_doris.sql
RC=$?
set -e
echo "[g2] sql-client exit=$RC"
if [[ "$RC" -ne 0 ]]; then
  echo "[g2] FAIL: sql-client returned rc=$RC (no false PASS)"
  docker logs gs-flink-jm 2>&1 | tail -n 80 || true
  exit "$RC"
fi

echo "[g2] 6/6 query ADS (this-run: metric_id=ads_dau_di AND dau>0)"
sleep 2
mkdir -p "$(dirname "$RESULT_FILE")"
{
  echo "=== ads_dau_di (metric_id=ads_dau_di AND dau>0) ==="
  docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e \
    "SELECT dt, server_id, dau, metric_id FROM ads.ads_dau_di WHERE metric_id='ads_dau_di' AND dau>0 ORDER BY dt, server_id;"
  echo
  echo "=== ads_pay_rate_di ==="
  docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e \
    "SELECT dt, server_id, dau, pay_users, pay_rate, metric_id FROM ads.ads_pay_rate_di ORDER BY dt, server_id;"
  echo
  echo "=== row counts (acceptance uses metric_id+dau, not COUNT alone) ==="
  docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e \
    "SELECT 'ads_dau_di' AS tbl, COUNT(*) AS n FROM ads.ads_dau_di WHERE metric_id='ads_dau_di' AND dau>0
     UNION ALL SELECT 'ads_pay_rate_di', COUNT(*) FROM ads.ads_pay_rate_di;"
} | tee "$RESULT_FILE"

DAU_N=$(docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -N -e \
  "SELECT COUNT(*) FROM ads.ads_dau_di WHERE metric_id='ads_dau_di' AND dau>0;")
if [[ "${DAU_N}" -lt 1 ]]; then
  echo "[g2] FAIL: no this-run rows with metric_id=ads_dau_di AND dau>0 (sql-client rc=$RC)"
  docker logs gs-flink-jm 2>&1 | tail -n 80 || true
  exit 2
fi
echo "[g2] OK: ads_dau_di rows=$DAU_N with metric_id=ads_dau_di AND dau>0 (saved $RESULT_FILE)"
