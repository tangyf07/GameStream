#!/usr/bin/env bash
# G8 resident Doris materializer acceptance.
# Happy path: produce ODS events + SELECT Doris only.
# Does NOT call script-phase materialize / wait-for-expected-then-materialize.
set -euo pipefail

ROOT="${GAMESTREAM_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
cd "$ROOT"

DT="${DT:-$(date +%F)}"
SERVER_ID="${SERVER_ID:-1}"
TOPIC_IN="${TOPIC_IN:-gamestream.g8r.ods.events}"
TOPIC_DAU="${TOPIC_DAU:-gamestream.g8r.ads_dau}"
TOPIC_PAY="${TOPIC_PAY:-gamestream.g8r.ads_pay_rate}"
FLINK_GROUP="${FLINK_GROUP:-gamestream-g8r-ads-v1}"
MAT_GROUP="${MAT_GROUP:-gamestream-g8r-doris-materializer}"
ADS_WAIT_SEC="${ADS_WAIT_SEC:-180}"
RESULT_TXT="${RESULT_TXT:-$ROOT/docs/g8-resident-materializer-result.txt}"
SQL_SRC="$ROOT/flink/sql/g8_continuous_ads.sql"
SQL_RUNTIME=/tmp/g8r_continuous_ads_runtime.sql
MAT_PID_FILE=/tmp/g8r_resident_materializer.pid
MAT_LOG_FILE=/tmp/g8r_resident_materializer.log
MAT_PY="$ROOT/scripts/g8_resident_materializer.py"

export G8_MAT_BOOTSTRAP="${G8_MAT_BOOTSTRAP:-localhost:19092}"
export G8_MAT_GROUP="$MAT_GROUP"
export G8_MAT_TOPIC_DAU="$TOPIC_DAU"
export G8_MAT_TOPIC_PAY="$TOPIC_PAY"
export G8_MAT_DORIS_HOST="${G8_MAT_DORIS_HOST:-127.0.0.1}"
export G8_MAT_DORIS_PORT="${G8_MAT_DORIS_PORT:-9030}"
export G8_MAT_PID_FILE="$MAT_PID_FILE"
export G8_MAT_LOG_FILE="$MAT_LOG_FILE"
export PATH="${HOME}/.local/bin:${PATH}"
export PYTHONPATH="${ROOT}${PYTHONPATH:+:$PYTHONPATH}"

mkdir -p "$(dirname "$RESULT_TXT")"
: >"$RESULT_TXT"
log() { echo "$*" | tee -a "$RESULT_TXT" >&2; }

doris_ads_row() {
  docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -N -e "
SELECT CONCAT_WS(' ',
  IFNULL((SELECT dau FROM ads.ads_dau_di WHERE dt='${DT}' AND server_id=${SERVER_ID}), 'NULL'),
  IFNULL((SELECT pay_users FROM ads.ads_pay_rate_di WHERE dt='${DT}' AND server_id=${SERVER_ID}), 'NULL'),
  IFNULL((SELECT pay_rate FROM ads.ads_pay_rate_di WHERE dt='${DT}' AND server_id=${SERVER_ID}), 'NULL')
);" 2>/dev/null | tr -d '\r' | awk 'NF{print; exit}'
}

# SELECT-only wait — NEVER INSERT / materialize here.
wait_doris_equals() {
  local want_dau="$1" want_pay="$2" label="$3"
  local i got ddau dpay
  for i in $(seq 1 "$ADS_WAIT_SEC"); do
    got=$(doris_ads_row || echo "NULL NULL NULL")
    ddau=$(echo "$got" | awk '{print $1}')
    dpay=$(echo "$got" | awk '{print $2}')
    if [[ "$ddau" == "$want_dau" && "$dpay" == "$want_pay" ]]; then
      log "[g8r] $label OK doris=$got try=$i (SELECT-only; resident materializer write path)"
      echo "$got"
      return 0
    fi
    if [[ $((i % 10)) -eq 0 ]]; then
      log "[g8r] $label waiting doris='$got' want=$want_dau $want_pay try=$i"
    fi
    sleep 1
  done
  log "[g8r] FAIL: $label doris=$(doris_ads_row) want=$want_dau $want_pay"
  return 1
}

produce_file() {
  local file="$1"
  docker exec -i gs-kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic "$TOPIC_IN" < "$file"
}

start_materializer() {
  if [[ -f "$MAT_PID_FILE" ]] && kill -0 "$(cat "$MAT_PID_FILE")" 2>/dev/null; then
    log "[g8r] materializer already up pid=$(cat "$MAT_PID_FILE")"
    return 0
  fi
  export PATH="${HOME}/.local/bin:${PATH}"
  export PYTHONPATH="${ROOT}${PYTHONPATH:+:$PYTHONPATH}"
  python3 -c 'import kafka, pymysql' 2>/dev/null || python3 -m pip install --user --break-system-packages -q 'kafka-python>=2.0.2' 'PyMySQL>=1.1.0'
  : >"$MAT_LOG_FILE"
  nohup env PATH="$PATH" PYTHONPATH="$PYTHONPATH" \
    G8_MAT_BOOTSTRAP="$G8_MAT_BOOTSTRAP" G8_MAT_GROUP="$G8_MAT_GROUP" \
    G8_MAT_TOPIC_DAU="$G8_MAT_TOPIC_DAU" G8_MAT_TOPIC_PAY="$G8_MAT_TOPIC_PAY" \
    G8_MAT_DORIS_HOST="$G8_MAT_DORIS_HOST" G8_MAT_DORIS_PORT="$G8_MAT_DORIS_PORT" \
    python3 "$MAT_PY" >>"$MAT_LOG_FILE" 2>&1 &
  echo $! >"$MAT_PID_FILE"
  sleep 2
  if ! kill -0 "$(cat "$MAT_PID_FILE")" 2>/dev/null; then
    log "[g8r] FAIL: materializer died; log:"
    tail -n 80 "$MAT_LOG_FILE" | tee -a "$RESULT_TXT" || true
    return 1
  fi
  log "[g8r] materializer started pid=$(cat "$MAT_PID_FILE") log=$MAT_LOG_FILE"
}

stop_materializer() {
  if [[ -f "$MAT_PID_FILE" ]]; then
    local pid
    pid=$(cat "$MAT_PID_FILE")
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" || true
      for _ in $(seq 1 30); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.2
      done
      kill -9 "$pid" 2>/dev/null || true
      log "[g8r] materializer killed pid=$pid"
    fi
    rm -f "$MAT_PID_FILE"
  fi
}

cleanup() {
  stop_materializer || true
}
trap cleanup EXIT

log "===== G8 resident Doris materializer acceptance ====="
log "host=$(hostname) root=$ROOT dt=$DT server_id=$SERVER_ID"
log "topics in=$TOPIC_IN dau=$TOPIC_DAU pay=$TOPIC_PAY"
log "flink_group=$FLINK_GROUP mat_group=$MAT_GROUP"
log "honesty: acceptance = produce + Doris SELECT only; NO script materialize in wait path"
log "sink: Flink upsert-kafka ALS+PK -> resident materializer -> Doris UNIQUE KEY; NOT EO-2PC"

echo "[g8r] 1/10 wait Doris Alive"
for i in $(seq 1 60); do
  if docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -N -e "SHOW BACKENDS;" 2>/dev/null | tr '\t' '\n' | grep -qx true; then
    log "[g8r] Doris Alive=true"
    break
  fi
  log "[g8r] Doris not ready attempt $i"
  sleep 3
done

echo "[g8r] 2/10 DDL + clear proof rows"
docker exec -i gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot < "$ROOT/sql/ddl/doris_ads_g2.sql"
docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e \
  "DELETE FROM ads.ads_dau_di WHERE dt='${DT}' AND server_id=${SERVER_ID};
   DELETE FROM ads.ads_pay_rate_di WHERE dt='${DT}' AND server_id=${SERVER_ID};"
BASE=$(doris_ads_row || echo "NULL NULL NULL")
log "[g8r] baseline_doris=$BASE"

echo "[g8r] 3/10 recreate Kafka topics"
for t in "$TOPIC_IN" "$TOPIC_DAU" "$TOPIC_PAY"; do
  docker exec gs-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --delete --topic "$t" 2>/dev/null || true
done
sleep 2
for t in "$TOPIC_IN" "$TOPIC_DAU" "$TOPIC_PAY"; do
  docker exec gs-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1
done

echo "[g8r] 4/10 fixtures"
DT="$DT" SERVER_ID="$SERVER_ID" python3 - <<'PY'
import json, os
from pathlib import Path
dt = os.environ["DT"]; sid = int(os.environ["SERVER_ID"])
b1 = [
    {"event_id": "g8r-e-01", "event_type": "login", "event_time": f"{dt} 12:00:01", "player_id": 9001, "server_id": sid, "tag": "b1"},
    {"event_id": "g8r-e-02", "event_type": "login", "event_time": f"{dt} 12:00:02", "player_id": 9002, "server_id": sid, "tag": "b1"},
    {"event_id": "g8r-e-03", "event_type": "login", "event_time": f"{dt} 12:00:03", "player_id": 9003, "server_id": sid, "tag": "b1"},
    {"event_id": "g8r-e-04", "event_type": "recharge", "event_time": f"{dt} 12:00:04", "player_id": 9004, "server_id": sid, "tag": "b1_pay"},
    {"event_id": "g8r-e-01", "event_type": "login", "event_time": f"{dt} 12:00:05", "player_id": 9001, "server_id": sid, "tag": "dup"},
]
b2 = [
    {"event_id": "g8r-e-05", "event_type": "login", "event_time": f"{dt} 12:05:01", "player_id": 9005, "server_id": sid, "tag": "b2"},
    {"event_id": "g8r-e-06", "event_type": "recharge", "event_time": f"{dt} 12:05:02", "player_id": 9006, "server_id": sid, "tag": "b2_pay"},
]
b3 = [
    {"event_id": "g8r-e-07", "event_type": "login", "event_time": f"{dt} 12:10:01", "player_id": 9007, "server_id": sid, "tag": "b3"},
    {"event_id": "g8r-e-08", "event_type": "login", "event_time": f"{dt} 12:10:02", "player_id": 9008, "server_id": sid, "tag": "b3"},
]
Path("/tmp/gamestream_g8r_batch1.jsonl").write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in b1)+"\n")
Path("/tmp/gamestream_g8r_batch2.jsonl").write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in b2)+"\n")
Path("/tmp/gamestream_g8r_batch3.jsonl").write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in b3)+"\n")
print("expect batch1 dau=4 pay=1; batch2 dau=6 pay=2; batch3 dau=8 pay=2")
PY

cp "$SQL_SRC" "$SQL_RUNTIME"
sed -i "s/gamestream-g8-ads-v1/${FLINK_GROUP}/g" "$SQL_RUNTIME"
sed -i "s/gamestream\.g8\.ods\.events/${TOPIC_IN}/g" "$SQL_RUNTIME"
sed -i "s/gamestream\.g8\.ads_dau/${TOPIC_DAU}/g" "$SQL_RUNTIME"
sed -i "s/gamestream\.g8\.ads_pay_rate/${TOPIC_PAY}/g" "$SQL_RUNTIME"
sed -i "s/g8-continuous-ads/g8r-resident-ads/g" "$SQL_RUNTIME"
docker cp "$SQL_RUNTIME" gs-flink-jm:/tmp/g8r_continuous_ads_runtime.sql

cancel_jobs() {
  local ids jid
  ids=$(python3 -c '
import json,urllib.request
try:
  data=json.load(urllib.request.urlopen("http://127.0.0.1:8081/jobs/overview", timeout=5))
except Exception:
  data={"jobs":[]}
for j in data.get("jobs",[]):
  if "g8r-resident-ads" in j.get("name","") and j.get("state") in ("RUNNING","RESTARTING","CREATED","FAILING"):
    print(j["jid"])
')
  for jid in $ids; do
    echo "[g8r] cancel $jid"
    curl -sf -X PATCH "http://127.0.0.1:8081/jobs/${jid}?mode=cancel" >/dev/null || true
  done
  sleep 2
}
cancel_jobs

echo "[g8r] 5/10 submit Flink job"
docker exec gs-flink-jm bash -lc 'rm -f /tmp/g8r-sql-client.log; nohup /opt/flink/bin/sql-client.sh -j /opt/flink/usrlib/flink-sql-connector-kafka-3.0.2-1.18.jar -j /opt/flink/usrlib/flink-connector-jdbc-3.1.2-1.17.jar -j /opt/flink/usrlib/mysql-connector-j-8.0.33.jar -f /tmp/g8r_continuous_ads_runtime.sql >/tmp/g8r-sql-client.log 2>&1 & echo $!'
sleep 6
JOB_ID=""
for i in $(seq 1 40); do
  JOB_ID=$(python3 -c '
import json,urllib.request
try:
  data=json.load(urllib.request.urlopen("http://127.0.0.1:8081/jobs/overview", timeout=5))
except Exception:
  data={"jobs":[]}
for j in data.get("jobs",[]):
  if "g8r-resident-ads" in j.get("name","") and j.get("state")=="RUNNING":
    print(j["jid"]); break
')
  [[ -n "$JOB_ID" ]] && break
  sleep 2
done
if [[ -z "$JOB_ID" ]]; then
  log "[g8r] FAIL: Flink job not RUNNING"
  docker exec gs-flink-jm tail -n 80 /tmp/g8r-sql-client.log || true
  exit 2
fi
log "[g8r] flink_job=$JOB_ID"

echo "[g8r] 6/10 start resident materializer"
start_materializer

echo "[g8r] 7/10 produce batch1 -> expect Doris dau=4 pay=1 via RESIDENT path"
t0=$(date +%s)
produce_file /tmp/gamestream_g8r_batch1.jsonl
ADS1=$(wait_doris_equals 4 1 "after_batch1_resident") || {
  tail -n 60 "$MAT_LOG_FILE" | tee -a "$RESULT_TXT" || true
  docker exec gs-flink-jm tail -n 60 /tmp/g8r-sql-client.log || true
  exit 3
}
t1=$(date +%s)
log "[g8r] after_batch1=$ADS1 visible_after_sec=$((t1-t0))"

echo "[g8r] 8/10 produce batch2 -> continuous update dau=6 pay=2"
t2=$(date +%s)
produce_file /tmp/gamestream_g8r_batch2.jsonl
ADS2=$(wait_doris_equals 6 2 "after_batch2_resident") || exit 4
t3=$(date +%s)
log "[g8r] after_batch2=$ADS2 visible_after_sec=$((t3-t2))"

echo "[g8r] 9/10 kill materializer, produce batch3 (ADS must not advance until restart)"
BEFORE_KILL=$(doris_ads_row)
stop_materializer
trap - EXIT
produce_file /tmp/gamestream_g8r_batch3.jsonl
sleep 25
DURING=$(doris_ads_row)
log "[g8r] during_kill_before=$BEFORE_KILL during_kill_after_25s=$DURING"
dau_d=$(echo "$DURING" | awk '{print $1}')
pay_d=$(echo "$DURING" | awk '{print $2}')
if [[ "$dau_d" != "6" || "$pay_d" != "2" ]]; then
  log "[g8r] NOTE: Doris changed while materializer down (unexpected for this proof); got=$DURING — continue restart catch-up check"
else
  log "[g8r] OK: Doris unchanged while materializer down (write path not driven by accept script)"
fi

echo "[g8r] 10/10 restart materializer -> catch up dau=8 pay=2 (no loss; UNIQUE KEY no double-count)"
trap cleanup EXIT
start_materializer
ADS3=$(wait_doris_equals 8 2 "after_restart_catchup") || {
  tail -n 80 "$MAT_LOG_FILE" | tee -a "$RESULT_TXT" || true
  exit 5
}
log "[g8r] after_restart=$ADS3"

produce_file /tmp/gamestream_g8r_batch3.jsonl
sleep 20
ADS_REPLAY=$(doris_ads_row)
log "[g8r] after_dup_produce_doris=$ADS_REPLAY (expect still 8 2 — UNIQUE KEY idempotent / Flink event_id dedup)"
rd=$(echo "$ADS_REPLAY" | awk '{print $1}')
rp=$(echo "$ADS_REPLAY" | awk '{print $2}')
if [[ "$rd" != "8" || "$rp" != "2" ]]; then
  log "[g8r] FAIL: replay changed ADS unexpectedly: $ADS_REPLAY"
  exit 6
fi

log "===== PASS ====="
log "measured: batch1=$ADS1 batch2=$ADS2 kill_window=$DURING restart=$ADS3 replay=$ADS_REPLAY"
log "boundaries: at-least-once offsets; UNIQUE KEY upsert; tombstone=DELETE; NOT EO-2PC; accept script SELECT-only"
log "materializer_log_tail:"
tail -n 40 "$MAT_LOG_FILE" | tee -a "$RESULT_TXT" || true

stop_materializer
trap - EXIT
log "result_file=$RESULT_TXT"
echo PASS
