#!/usr/bin/env bash
# GameStream G8 steady-state bench (continuous load on G8 mainline)
# Prefer: cp to /tmp, sed CRLF, GAMESTREAM_ROOT=... TIER=light|medium bash
# Measures: throughput, Kafka lag, CP duration, Doris-query-visible E2E, backpressure
# Honesty: upsert-kafka continuous sink != Doris UNIQUE KEY materialize; NOT EO-2PC
# NEVER invent numbers — write 未测到 + reason when unavailable.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ROOT="${GAMESTREAM_ROOT:-$ROOT}"
if [[ ! -f "$ROOT/docker-compose.yml" ]]; then
  ROOT=/mnt/c/Users/tangy/source/repos/GameStream
fi
cd "$ROOT"

# shellcheck source=/dev/null
source "$ROOT/scripts/_g8s_lib.sh"

TIER="${TIER:-light}"
ROUNDS="${ROUNDS:-2}"
case "$TIER" in
  light)
    RATE="${RATE:-30}"
    DURATION_SEC="${DURATION_SEC:-30}"
    POOL_PLAYERS="${POOL_PLAYERS:-150}"
    PROBE_NEW="${PROBE_NEW:-20}"
    PROBE_PAY="${PROBE_PAY:-2}"
    ;;
  medium)
    RATE="${RATE:-50}"
    DURATION_SEC="${DURATION_SEC:-40}"
    POOL_PLAYERS="${POOL_PLAYERS:-250}"
    PROBE_NEW="${PROBE_NEW:-40}"
    PROBE_PAY="${PROBE_PAY:-4}"
    ;;
  *) echo "unknown TIER=$TIER (light|medium only)"; exit 1 ;;
esac

TOPIC_IN="${TOPIC_IN:-gamestream.g8s.ods.events}"
TOPIC_DAU="${TOPIC_DAU:-gamestream.g8s.ads_dau}"
TOPIC_PAY="${TOPIC_PAY:-gamestream.g8s.ads_pay_rate}"
G8_GROUP="${G8_GROUP:-gamestream-g8-steady-v1}"
DT="${G8_DT:-2026-09-07}"
SERVER_ID="${G8_SERVER_ID:-88}"
SQL_SRC="$ROOT/flink/sql/g8_continuous_ads.sql"
SQL_RUNTIME="/tmp/g8_steady_runtime.sql"
GEN_PY="$ROOT/scripts/g8_steady_gen.py"
HELP_PY="$ROOT/scripts/g8_steady_helpers.py"
SAMPLE_PY="$ROOT/scripts/g6_sample_metrics.py"
ADS_WAIT_SEC="${ADS_WAIT_SEC:-120}"
POLL_INTERVAL="${POLL_INTERVAL:-3}"
STATE_FILE="/tmp/g8_steady_state_${TIER}.json"
TS_UTC=$(date -u +%Y%m%dT%H%M%SZ)
RESULT_JSON="${RESULT_JSON:-$ROOT/bench/results/g8_${TIER}_${TS_UTC}.json}"
RESULT_TXT="${RESULT_TXT:-$ROOT/docs/g8-steady-bench-result.txt}"
RAW_DIR="${RAW_DIR:-$ROOT/bench/results/g8_raw_${TIER}_${TS_UTC}}"
mkdir -p "$(dirname "$RESULT_JSON")" "$RAW_DIR" "$(dirname "$RESULT_TXT")"
rm -f "$STATE_FILE"

echo "[g8s] root=$ROOT tier=$TIER rounds=$ROUNDS rate=$RATE duration=${DURATION_SEC}s"
echo "[g8s] result_json=$RESULT_JSON raw=$RAW_DIR"

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing: $1"; exit 1; }; }
need docker; need python3; need curl

MEM_NOTE=$(free -h 2>/dev/null | awk '/Mem:/ {printf "total=%s used=%s available=%s",$2,$3,$7}' || echo "free unavailable")
echo "[g8s] mem: $MEM_NOTE"

echo "[g8s] 1/10 stack check"
docker compose ps || true

echo "[g8s] wait Flink UI + slots"
UI_OK=0
for i in $(seq 1 60); do
  if curl -sf http://127.0.0.1:8081/overview >/dev/null; then
    read -r SLOTS TMS JOBS <<< "$(overview_slots)"
    echo "[g8s] Flink UI try=$i slots=$SLOTS tms=$TMS jobs=$JOBS"
    if [[ "${SLOTS:-0}" -ge 1 ]]; then UI_OK=1; break; fi
  fi
  sleep 2
done
[[ "$UI_OK" == "1" ]] || { echo "[g8s] FAIL: Flink not ready"; exit 1; }

docker exec gs-flink-jm ls /opt/flink/usrlib/flink-sql-connector-kafka-3.0.2-1.18.jar >/dev/null
mkdir -p "$ROOT/flink/checkpoints"
docker exec gs-flink-jm bash -lc 'ls -ld /checkpoints && touch /checkpoints/.g8s_write_test && rm -f /checkpoints/.g8s_write_test' || true

alive="false"
for i in $(seq 1 60); do
  if docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e "SHOW BACKENDS;" >/dev/null 2>&1; then
    alive=$(docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e "SHOW BACKENDS\G" 2>/dev/null | tr -d '\r' | awk -F': ' '/Alive:/ {print $2; exit}')
    echo "[g8s] Doris Alive=${alive:-?} try=$i"
    [[ "$alive" == "true" ]] && break
  fi
  sleep 5
done
[[ "$alive" == "true" ]] || { echo "[g8s] FAIL: Doris BE not Alive"; exit 1; }

echo "[g8s] 2/10 Doris ADS DDL + truncate bench rows server_id=$SERVER_ID"
docker exec -i gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot < "$ROOT/sql/ddl/doris_ads_g2.sql"
docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e \
  "DELETE FROM ads.ads_dau_di WHERE dt='${DT}' AND server_id=${SERVER_ID};
   DELETE FROM ads.ads_pay_rate_di WHERE dt='${DT}' AND server_id=${SERVER_ID};"

echo "[g8s] 3/10 Kafka topics recreate"
for t in "$TOPIC_IN" "$TOPIC_DAU" "$TOPIC_PAY"; do
  docker exec gs-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$t" 2>/dev/null || true
done
sleep 2
for t in "$TOPIC_IN" "$TOPIC_DAU" "$TOPIC_PAY"; do
  docker exec gs-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1
done

cancel_g8s() {
  local ids jid
  ids=$(python3 -c '
import json,urllib.request
try:
  data=json.load(urllib.request.urlopen("http://127.0.0.1:8081/jobs/overview",timeout=5))
except Exception:
  data={"jobs":[]}
for j in data.get("jobs",[]):
  name=j.get("name") or ""
  if ("g8-continuous-ads" in name or "g8-steady" in name) and j.get("state") in ("RUNNING","RESTARTING","CREATED","FAILING"):
    print(j["jid"])
')
  for jid in $ids; do
    echo "[g8s] cancel leftover $jid"
    curl -sf -X PATCH "http://127.0.0.1:8081/jobs/${jid}?mode=cancel" >/dev/null || true
  done
  sleep 3
}
cancel_g8s

echo "[g8s] 4/10 prepare SQL runtime (topics/group)"
cp "$SQL_SRC" "$SQL_RUNTIME"
sed -i "s/gamestream-g8-ads-v1/${G8_GROUP}/g" "$SQL_RUNTIME"
sed -i "s/gamestream\.g8\.ods\.events/${TOPIC_IN}/g" "$SQL_RUNTIME"
sed -i "s/gamestream\.g8\.ads_dau/${TOPIC_DAU}/g" "$SQL_RUNTIME"
sed -i "s/gamestream\.g8\.ads_pay_rate/${TOPIC_PAY}/g" "$SQL_RUNTIME"
docker cp "$SQL_RUNTIME" gs-flink-jm:/tmp/g8_steady_runtime.sql

echo "[g8s] 5/10 submit Flink G8 mainline SQL (upsert-kafka continuous)"
# Kill leftover clients in a SEPARATE exec — pkill -f sql-client would match this launcher argv and SIGTERM ourselves.
docker exec gs-flink-jm bash -lc 'pkill -f org.apache.flink.table.client.SqlClient || true' || true
sleep 1
docker exec gs-flink-jm bash -lc 'rm -f /tmp/g8s-sql-client.log; nohup /opt/flink/bin/sql-client.sh -j /opt/flink/usrlib/flink-sql-connector-kafka-3.0.2-1.18.jar -f /tmp/g8_steady_runtime.sql >/tmp/g8s-sql-client.log 2>&1 & echo $!'
sleep 10

JOB_ID=""
for i in $(seq 1 90); do
  JOB_ID=$(python3 -c '
import json,urllib.request
try:
  data=json.load(urllib.request.urlopen("http://127.0.0.1:8081/jobs/overview",timeout=5))
except Exception:
  data={"jobs":[]}
for j in data.get("jobs",[]):
  if "g8-continuous-ads" in j.get("name","") and j.get("state")=="RUNNING":
    print(j["jid"]); break
')
  if [[ -n "$JOB_ID" ]]; then
    echo "[g8s] job RUNNING id=$JOB_ID"
    break
  fi
  if [[ $((i % 5)) -eq 0 ]]; then
    docker exec gs-flink-jm tail -n 40 /tmp/g8s-sql-client.log 2>/dev/null || true
  fi
  sleep 2
done
[[ -n "$JOB_ID" ]] || { echo "[g8s] FAIL: job not RUNNING"; docker exec gs-flink-jm cat /tmp/g8s-sql-client.log || true; exit 2; }
echo "$JOB_ID" > "$RAW_DIR/job_id.txt"

echo "[g8s] 6/10 BASELINE sample BEFORE load"
sample_lag "baseline_before_load" "$RAW_DIR/lag_baseline.txt"
python3 "$SAMPLE_PY" --action sample-once --jid "$JOB_ID" > "$RAW_DIR/baseline_sample.json" || true
python3 "$SAMPLE_PY" --action checkpoints --jid "$JOB_ID" > "$RAW_DIR/baseline_checkpoints.json" || true
python3 "$SAMPLE_PY" --action vertices-bp --jid "$JOB_ID" > "$RAW_DIR/baseline_vertices_bp.json" || true
curl -sf "http://127.0.0.1:8081/jobs/${JOB_ID}/checkpoints" > "$RAW_DIR/baseline_checkpoints_raw.json" || true
echo "{\"tier\":\"$TIER\",\"mem_note\":\"$MEM_NOTE\",\"job_id\":\"$JOB_ID\",\"sampled_at\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" > "$RAW_DIR/baseline_meta.json"

ROUNDS_DIR="$RAW_DIR/rounds"
mkdir -p "$ROUNDS_DIR"
TOTAL_EVENTS=0
TOTAL_PRODUCE_SEC=0

echo "[g8s] 7/10 continuous load rounds=$ROUNDS"
for r in $(seq 1 "$ROUNDS"); do
  RDIR="$ROUNDS_DIR/round_${r}"
  mkdir -p "$RDIR"
  echo "[g8s] === round $r/$ROUNDS continuous rate=$RATE duration=${DURATION_SEC}s ==="

  CONT_FILE="/tmp/g8s_${TIER}_r${r}_continuous.jsonl"
  python3 "$GEN_PY" --mode continuous --round "$r" --out "$CONT_FILE" \
    --meta-out "$RDIR/gen_continuous.json" --state "$STATE_FILE" \
    --dt "$DT" --server-id "$SERVER_ID" --rate "$RATE" --duration-sec "$DURATION_SEC" \
    --pool-players "$POOL_PLAYERS" | tee "$RDIR/gen_continuous_stdout.json"

  POLL_SECS=$((DURATION_SEC + 25))
  python3 "$SAMPLE_PY" --action poll --jid "$JOB_ID" --seconds "$POLL_SECS" --interval "$POLL_INTERVAL" \
    --out "$RDIR/poll.json" > "$RDIR/poll_summary.json" &
  POLL_PID=$!

  sample_lag "round${r}_before_continuous" "$RDIR/lag_before.txt"

  python3 "$HELP_PY" --action produce-chunked --file "$CONT_FILE" --duration "$DURATION_SEC" \
    --topic "$TOPIC_IN" --meta-out "$RDIR/produce_continuous.json" | tee "$RDIR/produce_continuous_stdout.json"
  C_EVENTS=$(python3 -c "import json;print(json.load(open('$RDIR/produce_continuous.json')).get('events',0))")
  C_SEC=$(python3 -c "import json;print(json.load(open('$RDIR/produce_continuous.json')).get('produce_wall_sec',0))")
  TOTAL_EVENTS=$((TOTAL_EVENTS + C_EVENTS))
  TOTAL_PRODUCE_SEC=$(python3 -c "print(round(float('$TOTAL_PRODUCE_SEC')+float('$C_SEC'),4))")

  sample_lag "round${r}_after_continuous" "$RDIR/lag_after_continuous.txt"

  PROBE_FILE="/tmp/g8s_${TIER}_r${r}_probe.jsonl"
  python3 "$GEN_PY" --mode probe --round "$r" --out "$PROBE_FILE" \
    --meta-out "$RDIR/gen_probe.json" --state "$STATE_FILE" \
    --dt "$DT" --server-id "$SERVER_ID" \
    --probe-new-players "$PROBE_NEW" --probe-pay-users "$PROBE_PAY" | tee "$RDIR/gen_probe_stdout.json"

  WANT_DAU=$(python3 -c "import json;print(json.load(open('$RDIR/gen_probe.json'))['expected_dau'])")
  WANT_PAY=$(python3 -c "import json;print(json.load(open('$RDIR/gen_probe.json'))['expected_pay_users'])")

  PRODUCE_T0=$(date +%s.%N)
  produce_file "$PROBE_FILE"
  PRODUCE_T1=$(date +%s.%N)
  P_SEC=$(python3 -c "print(round(float('$PRODUCE_T1')-float('$PRODUCE_T0'),4))")
  P_EVENTS=$(wc -l < "$PROBE_FILE" | tr -d ' ')
  TOTAL_EVENTS=$((TOTAL_EVENTS + P_EVENTS))
  TOTAL_PRODUCE_SEC=$(python3 -c "print(round(float('$TOTAL_PRODUCE_SEC')+float('$P_SEC'),4))")
  echo "{\"events\":$P_EVENTS,\"produce_wall_sec\":$P_SEC,\"produce_epoch_end\":$PRODUCE_T1,\"want_dau\":$WANT_DAU,\"want_pay_users\":$WANT_PAY}" > "$RDIR/produce_probe.json"

  set +e
  python3 "$HELP_PY" --action wait-doris-e2e \
    --want-dau "$WANT_DAU" --want-pay "$WANT_PAY" --produce-epoch "$PRODUCE_T1" \
    --out "$RDIR/e2e_doris.json" --dt "$DT" --server-id "$SERVER_ID" \
    --topic-dau "$TOPIC_DAU" --topic-pay "$TOPIC_PAY" --wait-sec "$ADS_WAIT_SEC"
  E2E_RC=$?
  set -e

  sample_lag "round${r}_after_probe" "$RDIR/lag_after_probe.txt"
  wait "$POLL_PID" 2>/dev/null || true

  python3 "$SAMPLE_PY" --action checkpoints --jid "$JOB_ID" > "$RDIR/checkpoints.json" || true
  python3 "$SAMPLE_PY" --action vertices-bp --jid "$JOB_ID" > "$RDIR/vertices_bp.json" || true
  curl -sf "http://127.0.0.1:8081/jobs/${JOB_ID}/checkpoints" > "$RDIR/checkpoints_raw.json" || true

  echo "[g8s] round $r done e2e_rc=$E2E_RC want_dau=$WANT_DAU want_pay=$WANT_PAY"
done

echo "[g8s] 8/10 final snapshots"
sample_lag "final" "$RAW_DIR/lag_final.txt"
python3 "$SAMPLE_PY" --action checkpoints --jid "$JOB_ID" > "$RAW_DIR/checkpoints_final.json" || true
python3 "$SAMPLE_PY" --action sample-once --jid "$JOB_ID" > "$RAW_DIR/sample_final.json" || true
python3 "$SAMPLE_PY" --action vertices-bp --jid "$JOB_ID" > "$RAW_DIR/vertices_bp_final.json" || true
curl -sf "http://127.0.0.1:8081/jobs/${JOB_ID}/checkpoints" > "$RAW_DIR/checkpoints_raw_final.json" || true
cp "$STATE_FILE" "$RAW_DIR/player_state.json" 2>/dev/null || true

python3 "$HELP_PY" --action parse-lag --raw-dir "$RAW_DIR" --group "$G8_GROUP" >/dev/null

echo "[g8s] clock skew probe"
HOST_UTC=$(date -u +%Y-%m-%dT%H:%M:%SZ)
JM_UTC=$(docker exec gs-flink-jm date -u +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || echo "未测到")
TM_UTC=$(docker exec gs-flink-tm date -u +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || echo "未测到")
KFK_UTC=$(docker exec gs-kafka date -u +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || echo "未测到")
DORIS_UTC=$(docker exec gs-doris-fe date -u +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || echo "未测到")
python3 "$HELP_PY" --action clock-skew --out "$RAW_DIR/clock_skew.json" \
  --host-utc "$HOST_UTC" --jm-utc "$JM_UTC" --tm-utc "$TM_UTC" \
  --kafka-utc "$KFK_UTC" --doris-utc "$DORIS_UTC" >/dev/null

echo "[g8s] 9/10 assemble result JSON"
HOST=$(hostname 2>/dev/null || echo unknown)
export RAW_DIR RESULT_JSON RESULT_TXT TS_UTC TIER HOST MEM_NOTE JOB_ID G8_GROUP
export TOPIC_IN TOPIC_DAU TOPIC_PAY DT SERVER_ID ROUNDS RATE DURATION_SEC
export TOTAL_EVENTS TOTAL_PRODUCE_SEC ALIVE="$alive"
python3 "$HELP_PY" --action assemble

echo "[g8s] 10/10 cancel job"
curl -sf -X PATCH "http://127.0.0.1:8081/jobs/${JOB_ID}?mode=cancel" >/dev/null || true
sleep 2
echo "[g8s] DONE → $RESULT_JSON"
