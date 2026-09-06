#!/usr/bin/env bash
# GameStream G2 E2E: Simulator → Kafka → Flink (batch bounded) → Doris ADS
# Intended to run inside WSL from repo root:
#   bash scripts/e2e_g2.sh
# Or from Windows (after CRLF strip):
#   wsl -e bash /mnt/c/Users/tangy/source/repos/GameStream/scripts/e2e_g2.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
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
docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e "SHOW BACKENDS;" >/dev/null

echo "[g2] 2/6 ensure Flink usrlib jars visible (recreate JM/TM if compose volumes changed)"
if ! docker exec gs-flink-jm ls /opt/flink/usrlib/flink-sql-connector-kafka-3.0.2-1.18.jar >/dev/null 2>&1; then
  echo "[g2] jars missing in container — docker compose up -d --force-recreate jobmanager taskmanager"
  docker compose up -d --force-recreate jobmanager taskmanager
  sleep 8
fi
docker exec gs-flink-jm ls -la /opt/flink/usrlib/

echo "[g2] 3/6 Doris DDL (replication_num=1)"
docker exec -i gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot < sql/ddl/doris_ads_g2.sql
docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e "USE ads; SHOW TABLES; DESC ads_dau_di; DESC ads_pay_rate_di;"

echo "[g2] 4/6 Kafka topic + produce events"
docker exec gs-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic "$TOPIC" --partitions 1 --replication-factor 1
# Truncate topic by deleting+recreating for idempotent demo (small scale)
docker exec gs-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$TOPIC" 2>/dev/null || true
sleep 2
docker exec gs-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic "$TOPIC" --partitions 1 --replication-factor 1

python3 simulator/generate_events.py \
  --players "$PLAYERS" --events "$EVENTS" --servers "$SERVERS" --days "$DAYS" --seed "$SEED" \
  --out "$EVENTS_FILE"

echo "[g2] publishing $(wc -l < "$EVENTS_FILE") lines to $TOPIC via console-producer"
# kafka-console-producer reads line-delimited messages from stdin
docker exec -i gs-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic "$TOPIC" < "$EVENTS_FILE"
sleep 2
MSG_CNT=$(docker exec gs-kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 --topic "$TOPIC" --time -1 2>/dev/null | awk -F: '{s+=$3} END {print s+0}')
echo "[g2] topic high-watermark offsets sum≈ $MSG_CNT"

echo "[g2] 5/6 submit Flink SQL (batch + bounded Kafka → Doris JDBC)"
# sql-client embedded mode; jars from /opt/flink/usrlib auto-loaded by Flink image
docker exec gs-flink-jm /opt/flink/bin/sql-client.sh -f /opt/flink/sql/g2_kafka_to_doris.sql

echo "[g2] wait briefly then query Doris ADS"
sleep 3

echo "[g2] 6/6 query ADS"
{
  echo "=== ads_dau_di ==="
  docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e \
    "SELECT dt, server_id, dau, metric_id FROM ads.ads_dau_di ORDER BY dt, server_id;"
  echo
  echo "=== ads_pay_rate_di ==="
  docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e \
    "SELECT dt, server_id, dau, pay_users, pay_rate, metric_id FROM ads.ads_pay_rate_di ORDER BY dt, server_id;"
  echo
  echo "=== row counts ==="
  docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e \
    "SELECT 'ads_dau_di' AS tbl, COUNT(*) AS n FROM ads.ads_dau_di UNION ALL SELECT 'ads_pay_rate_di', COUNT(*) FROM ads.ads_pay_rate_di;"
} | tee "$RESULT_FILE"

DAU_N=$(docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -N -e "SELECT COUNT(*) FROM ads.ads_dau_di;")
if [[ "${DAU_N}" -lt 1 ]]; then
  echo "[g2] FAIL: ads_dau_di empty"
  exit 2
fi
echo "[g2] OK: ads_dau_di rows=$DAU_N (saved $RESULT_FILE)"
