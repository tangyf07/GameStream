#!/usr/bin/env bash
# GameStream G8: Continuous Doris ADS mainline
# Prefer: copy to /tmp, sed CRLF, then bash (Windows /mnt/c quirk).
# Proves: continuous ADS updates + event_id dedup + TM kill restore on SAME job.
# Contrast: G2 = batch bounded one-shot. G3–G5 drills folded into this pipeline.
#
# Sink honesty: Flink upsert-kafka (ALS+PK) continuously.
# Doris UNIQUE KEY primary path = resident materializer (see g8_resident_materializer*).
# This script's materialize_doris() is FALLBACK/DEV only (wait-for-expected-then-materialize).
# Flink JDBC MySQL ON DUPLICATE KEY UPDATE rejected by Doris FE.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ROOT="${GAMESTREAM_ROOT:-$ROOT}"
if [[ ! -f "$ROOT/docker-compose.yml" ]]; then
  ROOT=/mnt/c/Users/tangy/source/repos/GameStream
fi
cd "$ROOT"

TOPIC_IN="${TOPIC_IN:-gamestream.g8.ods.events}"
TOPIC_DAU="${TOPIC_DAU:-gamestream.g8.ads_dau}"
TOPIC_PAY="${TOPIC_PAY:-gamestream.g8.ads_pay_rate}"
G8_GROUP="${G8_GROUP:-gamestream-g8-ads-v1}"
DT="${G8_DT:-2026-09-07}"
SERVER_ID="${G8_SERVER_ID:-1}"
RESULT_FILE="${RESULT_FILE:-docs/g8-continuous-mainline-result.txt}"
TRANSITIONS_FILE="${TRANSITIONS_FILE:-/tmp/g8_job_transitions.txt}"
SQL_SRC="$ROOT/flink/sql/g8_continuous_ads.sql"
SQL_RUNTIME="/tmp/g8_continuous_ads_runtime.sql"
CP_WAIT_SEC="${G8_CP_WAIT_SEC:-45}"
JOB_RECOVER_SEC="${G8_JOB_RECOVER_SEC:-120}"
ADS_WAIT_SEC="${G8_ADS_WAIT_SEC:-90}"

BATCH1=/tmp/gamestream_g8_batch1.jsonl
BATCH2=/tmp/gamestream_g8_batch2.jsonl
DUP_REPLAY=/tmp/gamestream_g8_dup_replay.jsonl
BATCH3=/tmp/gamestream_g8_batch3.jsonl

echo "[g8] root=$ROOT group=$G8_GROUP dt=$DT server_id=$SERVER_ID"

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing: $1"; exit 1; }; }
need docker
need python3
need curl

: > "$TRANSITIONS_FILE"
log_transition() {
  local msg="$1"
  local ts
  ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  echo "$ts $msg" | tee -a "$TRANSITIONS_FILE"
}

job_state() {
  local jid="$1"
  JOB="$jid" python3 -c '
import json,urllib.request,os
jid=os.environ["JOB"]
try:
  d=json.load(urllib.request.urlopen(f"http://127.0.0.1:8081/jobs/{jid}", timeout=5))
  print(d.get("state",""))
except Exception:
  print("GONE")
' 2>/dev/null || echo "GONE"
}

overview_slots() {
  python3 -c '
import json,urllib.request
try:
  d=json.load(urllib.request.urlopen("http://127.0.0.1:8081/overview", timeout=5))
  print(d.get("slots-total",0), d.get("taskmanagers",0), d.get("jobs-running",0))
except Exception:
  print("0 0 0")
' 2>/dev/null || echo "0 0 0"
}

read_kafka_ads() {
  # prints: dau pay_users pay_rate from latest upsert records for DT/SERVER
  local dau_file=/tmp/g8_kafka_dau.jsonl
  local pay_file=/tmp/g8_kafka_pay.jsonl
  rm -f "$dau_file" "$pay_file"
  timeout 10 docker exec gs-kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic "$TOPIC_DAU" \
    --from-beginning --property print.key=true --property key.separator="|" \
    --timeout-ms 7000 > "$dau_file" 2>/dev/null || true
  timeout 10 docker exec gs-kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic "$TOPIC_PAY" \
    --from-beginning --property print.key=true --property key.separator="|" \
    --timeout-ms 7000 > "$pay_file" 2>/dev/null || true
  DT="$DT" SERVER_ID="$SERVER_ID" python3 - <<'PY'
import json, os
from pathlib import Path
dt, sid = os.environ["DT"], int(os.environ["SERVER_ID"])

def latest(path, need_pay=False):
    latest=None
    p=Path(path)
    if not p.exists():
        return None
    for line in p.read_text().splitlines():
        line=line.strip()
        if not line:
            continue
        val=line.split("|",1)[1] if "|" in line else line
        try:
            o=json.loads(val)
        except Exception:
            continue
        # keys may be in value
        odt=str(o.get("dt",""))[:10]
        try:
            osid=int(o.get("server_id"))
        except Exception:
            continue
        if odt==dt and osid==sid:
            latest=o
    return latest

dau_o=latest("/tmp/g8_kafka_dau.jsonl")
pay_o=latest("/tmp/g8_kafka_pay.jsonl", True)
dau = dau_o.get("dau") if dau_o else None
pay = pay_o.get("pay_users") if pay_o else None
rate = pay_o.get("pay_rate") if pay_o else None
pdau = pay_o.get("dau") if pay_o else None
if dau is None and pdau is not None:
    dau=pdau
print(f"{dau if dau is not None else 'NULL'} {pay if pay is not None else 'NULL'} {rate if rate is not None else 'NULL'}")
PY
}

materialize_doris() {
  # FALLBACK/DEV ONLY — primary path is resident materializer.
  # Read latest kafka ads and INSERT into Doris UNIQUE KEY tables
  local raw
  raw=$(read_kafka_ads)
  local dau pay rate
  dau=$(echo "$raw" | awk '{print $1}')
  pay=$(echo "$raw" | awk '{print $2}')
  rate=$(echo "$raw" | awk '{print $3}')
  if [[ "$dau" == "NULL" || "$pay" == "NULL" ]]; then
    echo "[g8] materialize skip: kafka ads incomplete ($raw)" >&2
    return 1
  fi
  # rate may be scientific; pass through
  docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e "
INSERT INTO ads.ads_dau_di (dt, server_id, dau, metric_id)
VALUES ('${DT}', ${SERVER_ID}, ${dau}, 'ads_dau_di');
INSERT INTO ads.ads_pay_rate_di (dt, server_id, dau, pay_users, pay_rate, metric_id)
VALUES ('${DT}', ${SERVER_ID}, ${dau}, ${pay}, ${rate}, 'ads_pay_rate_di');
" >/dev/null
  echo "$raw"
}

doris_ads_row() {
  docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -N -e "
SELECT CONCAT_WS(' ',
  IFNULL((SELECT dau FROM ads.ads_dau_di WHERE dt='${DT}' AND server_id=${SERVER_ID}), 'NULL'),
  IFNULL((SELECT pay_users FROM ads.ads_pay_rate_di WHERE dt='${DT}' AND server_id=${SERVER_ID}), 'NULL'),
  IFNULL((SELECT pay_rate FROM ads.ads_pay_rate_di WHERE dt='${DT}' AND server_id=${SERVER_ID}), 'NULL')
);" 2>/dev/null | tr -d '\r' | tail -n 1
}

wait_ads_equals() {
  local want_dau="$1" want_pay="$2" label="$3"
  local got="" i kdau kpay
  for i in $(seq 1 "$ADS_WAIT_SEC"); do
    got=$(read_kafka_ads)
    kdau=$(echo "$got" | awk '{print $1}')
    kpay=$(echo "$got" | awk '{print $2}')
    if [[ "$kdau" == "$want_dau" && "$kpay" == "$want_pay" ]]; then
      mat=$(materialize_doris) || true
      drow=$(doris_ads_row)
      ddau=$(echo "$drow" | awk '{print $1}')
      dpay=$(echo "$drow" | awk '{print $2}')
      if [[ "$ddau" == "$want_dau" && "$dpay" == "$want_pay" ]]; then
        echo "[g8] $label OK kafka=$got doris=$drow" >&2
        echo "$drow"
        return 0
      fi
      echo "[g8] $label kafka OK ($got) doris pending ($drow) try=$i" >&2
    else
      if [[ $((i % 5)) -eq 0 ]]; then
        echo "[g8] $label waiting kafka='$got' want dau=$want_dau pay=$want_pay try=$i" >&2
      fi
    fi
    sleep 1
  done
  echo "[g8] FAIL: $label did not reach dau=$want_dau pay_users=$want_pay (last_kafka=$got doris=$(doris_ads_row))" >&2
  return 1
}

echo "[g8] 1/12 check stack + checkpoint volume"
docker compose ps
mkdir -p "$ROOT/flink/checkpoints"

echo "[g8] wait Flink UI + slots"
UI_OK=0
for i in $(seq 1 60); do
  if curl -sf http://127.0.0.1:8081/overview >/dev/null; then
    read -r SLOTS TMS JOBS <<< "$(overview_slots)"
    echo "[g8] Flink UI up try=$i slots-total=$SLOTS tms=$TMS jobs-running=$JOBS"
    if [[ "${SLOTS:-0}" -ge 1 ]]; then
      UI_OK=1
      break
    fi
  else
    echo "[g8] Flink UI not ready try=$i"
  fi
  sleep 2
done
if [[ "$UI_OK" != "1" ]]; then
  echo "[g8] FAIL: Flink UI/slots not ready"
  exit 1
fi

docker exec gs-flink-jm bash -lc 'ls -ld /checkpoints && touch /checkpoints/.g8_write_test && rm -f /checkpoints/.g8_write_test'

alive="false"
for i in $(seq 1 90); do
  if docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e "SHOW BACKENDS;" >/dev/null 2>&1; then
    alive=$(docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e "SHOW BACKENDS\G" 2>/dev/null | tr -d '\r' | awk -F': ' '/Alive:/ {print $2; exit}')
    echo "[g8] Doris attempt $i Alive=${alive:-?}"
    if [[ "${alive}" == "true" ]]; then
      break
    fi
  else
    echo "[g8] Doris FE not ready attempt $i"
  fi
  sleep 5
done
if [[ "${alive}" != "true" ]]; then
  echo "[g8] FAIL: Doris BE not Alive"
  exit 1
fi

echo "[g8] 2/12 jars"
docker exec gs-flink-jm ls /opt/flink/usrlib/flink-sql-connector-kafka-3.0.2-1.18.jar >/dev/null

echo "[g8] 3/12 Doris ADS DDL + truncate g8 proof rows"
docker exec -i gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot < "$ROOT/sql/ddl/doris_ads_g2.sql"
docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e \
  "DELETE FROM ads.ads_dau_di WHERE dt='${DT}' AND server_id=${SERVER_ID};
   DELETE FROM ads.ads_pay_rate_di WHERE dt='${DT}' AND server_id=${SERVER_ID};"

echo "[g8] 4/12 Kafka topics (recreate empty)"
for t in "$TOPIC_IN" "$TOPIC_DAU" "$TOPIC_PAY"; do
  docker exec gs-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --delete --topic "$t" 2>/dev/null || true
done
sleep 2
for t in "$TOPIC_IN" "$TOPIC_DAU" "$TOPIC_PAY"; do
  docker exec gs-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1
done

echo "[g8] 5/12 generate continuous fixtures"
DT="$DT" SERVER_ID="$SERVER_ID" python3 - <<'PY'
import json, os
from pathlib import Path
dt = os.environ["DT"]
sid = int(os.environ["SERVER_ID"])
b1 = [
    {"event_id": "g8-e-01", "event_type": "login",    "event_time": f"{dt} 10:00:01", "player_id": 8001, "server_id": sid, "tag": "b1"},
    {"event_id": "g8-e-02", "event_type": "login",    "event_time": f"{dt} 10:00:02", "player_id": 8002, "server_id": sid, "tag": "b1"},
    {"event_id": "g8-e-03", "event_type": "login",    "event_time": f"{dt} 10:00:03", "player_id": 8003, "server_id": sid, "tag": "b1"},
    {"event_id": "g8-e-04", "event_type": "login",    "event_time": f"{dt} 10:00:04", "player_id": 8004, "server_id": sid, "tag": "b1"},
    {"event_id": "g8-e-05", "event_type": "recharge", "event_time": f"{dt} 10:00:05", "player_id": 8005, "server_id": sid, "tag": "b1_pay"},
    {"event_id": "g8-e-01", "event_type": "login",    "event_time": f"{dt} 10:00:06", "player_id": 8001, "server_id": sid, "tag": "dup_in_batch"},
    {"event_id": "g8-e-05", "event_type": "recharge", "event_time": f"{dt} 10:00:07", "player_id": 8005, "server_id": sid, "tag": "dup_in_batch"},
    {"event_id": "g8-e-06", "event_type": "login",    "event_time": f"{dt} 09:59:58", "player_id": 8006, "server_id": sid, "tag": "ooo_within_bound"},
]
b2 = [
    {"event_id": "g8-e-07", "event_type": "login",    "event_time": f"{dt} 10:05:01", "player_id": 8007, "server_id": sid, "tag": "b2"},
    {"event_id": "g8-e-08", "event_type": "recharge", "event_time": f"{dt} 10:05:02", "player_id": 8008, "server_id": sid, "tag": "b2_pay"},
]
dups = [
    dict(b1[0], tag="post_tm_kill_replay", event_time=f"{dt} 10:10:00"),
    dict(b1[4], tag="post_tm_kill_replay", event_time=f"{dt} 10:10:01"),
    dict(b2[1], tag="post_tm_kill_replay", event_time=f"{dt} 10:10:02"),
]
b3 = [
    {"event_id": "g8-e-09", "event_type": "login", "event_time": f"{dt} 10:15:01", "player_id": 8009, "server_id": sid, "tag": "b3"},
    {"event_id": "g8-e-10", "event_type": "login", "event_time": f"{dt} 10:15:02", "player_id": 8010, "server_id": sid, "tag": "b3"},
]
Path("/tmp/gamestream_g8_batch1.jsonl").write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in b1) + "\n")
Path("/tmp/gamestream_g8_batch2.jsonl").write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in b2) + "\n")
Path("/tmp/gamestream_g8_dup_replay.jsonl").write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in dups) + "\n")
Path("/tmp/gamestream_g8_batch3.jsonl").write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in b3) + "\n")
print(f"batch1_lines={len(b1)} expect_dau=6 pay=1")
print(f"batch2_lines={len(b2)} expect_dau=8 pay=2")
print(f"dup_replay_lines={len(dups)} expect_stable")
print(f"batch3_lines={len(b3)} expect_dau=10 pay=2")
PY

cp "$SQL_SRC" "$SQL_RUNTIME"
sed -i "s/gamestream-g8-ads-v1/${G8_GROUP}/g" "$SQL_RUNTIME"
docker cp "$SQL_RUNTIME" gs-flink-jm:/tmp/g8_continuous_ads_runtime.sql
docker cp "$SQL_SRC" gs-flink-jm:/opt/flink/sql/g8_continuous_ads.sql 2>/dev/null || true

cancel_g8_jobs() {
  local ids jid
  ids=$(python3 -c '
import json,urllib.request
try:
    data=json.load(urllib.request.urlopen("http://127.0.0.1:8081/jobs/overview", timeout=5))
except Exception:
    data={"jobs":[]}
for j in data.get("jobs",[]):
    if "g8-continuous-ads" in j.get("name","") and j.get("state") in ("RUNNING","RESTARTING","CREATED","FAILING"):
        print(j["jid"])
')
  for jid in $ids; do
    echo "[g8] cancel leftover job $jid"
    curl -sf -X PATCH "http://127.0.0.1:8081/jobs/${jid}?mode=cancel" >/dev/null || true
  done
  sleep 3
}
cancel_g8_jobs

echo "[g8] 6/12 submit Flink streaming SQL (continuous upsert-kafka)"
docker exec gs-flink-jm bash -lc 'rm -f /tmp/g8-sql-client.log; nohup /opt/flink/bin/sql-client.sh -j /opt/flink/usrlib/flink-sql-connector-kafka-3.0.2-1.18.jar -j /opt/flink/usrlib/flink-connector-jdbc-3.1.2-1.17.jar -j /opt/flink/usrlib/mysql-connector-j-8.0.33.jar -f /tmp/g8_continuous_ads_runtime.sql >/tmp/g8-sql-client.log 2>&1 & echo $!'
sleep 8

JOB_ID=""
for i in $(seq 1 40); do
  JOB_ID=$(python3 -c '
import json,urllib.request
try:
    data=json.load(urllib.request.urlopen("http://127.0.0.1:8081/jobs/overview", timeout=5))
except Exception:
    data={"jobs":[]}
for j in data.get("jobs",[]):
    if "g8-continuous-ads" in j.get("name","") and j.get("state")=="RUNNING":
        print(j["jid"]); break
')
  if [[ -n "$JOB_ID" ]]; then
    log_transition "job RUNNING id=$JOB_ID"
    break
  fi
  if [[ $((i % 5)) -eq 0 ]]; then
    docker exec gs-flink-jm tail -n 40 /tmp/g8-sql-client.log 2>/dev/null || true
  fi
  sleep 2
done
if [[ -z "$JOB_ID" ]]; then
  echo "[g8] FAIL: Flink job not RUNNING"
  docker exec gs-flink-jm cat /tmp/g8-sql-client.log || true
  exit 2
fi
echo "$JOB_ID" > /tmp/g8_job_id.txt

echo "[g8] 7/12 produce batch1 → expect DAU=6 pay_users=1 (continuous write #1)"
docker exec -i gs-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic "$TOPIC_IN" < "$BATCH1"
ADS1=$(wait_ads_equals 6 1 "after_batch1") || {
  docker exec gs-flink-jm tail -n 100 /tmp/g8-sql-client.log || true
  exit 3
}
echo "$ADS1" > /tmp/g8_ads_after_batch1.txt
log_transition "ads_after_batch1=$ADS1"

echo "[g8] 8/12 produce batch2 → expect DAU=8 pay_users=2 (continuous write #2)"
docker exec -i gs-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic "$TOPIC_IN" < "$BATCH2"
ADS2=$(wait_ads_equals 8 2 "after_batch2") || {
  docker exec gs-flink-jm tail -n 100 /tmp/g8-sql-client.log || true
  exit 4
}
echo "$ADS2" > /tmp/g8_ads_after_batch2.txt
log_transition "ads_after_batch2=$ADS2"

echo "[g8] wait for >=1 completed checkpoint before TM kill"
CP_JSON=/tmp/g8_checkpoints.json
CP_COMPLETED=0
CP_PATH=""
for i in $(seq 1 "$CP_WAIT_SEC"); do
  curl -sf "http://127.0.0.1:8081/jobs/${JOB_ID}/checkpoints" > "$CP_JSON" || true
  eval "$(python3 - <<'PY'
import json
from pathlib import Path
p=Path("/tmp/g8_checkpoints.json")
completed=0
path=""
if p.exists() and p.stat().st_size>0:
    try:
        d=json.loads(p.read_text())
        completed=int(d.get("counts",{}).get("completed",0) or 0)
        latest=(d.get("latest") or {}).get("completed") or {}
        path=latest.get("external_path") or ""
    except Exception:
        pass
print(f"CP_COMPLETED={completed}")
print(f"CP_PATH={path}")
PY
)"
  echo "[g8] checkpoint poll $i completed=$CP_COMPLETED path=${CP_PATH:-none}"
  if [[ "${CP_COMPLETED:-0}" -ge 1 && -n "${CP_PATH}" ]]; then
    break
  fi
  sleep 1
done
if [[ "${CP_COMPLETED:-0}" -lt 1 ]]; then
  echo "[g8] FAIL: no completed checkpoint"
  cat "$CP_JSON" || true
  exit 5
fi
echo "$CP_COMPLETED" > /tmp/g8_cp_completed.txt
echo "$CP_PATH" > /tmp/g8_cp_path.txt
log_transition "checkpoint_completed=$CP_COMPLETED path=$CP_PATH"

STATE_BEFORE=$(job_state "$JOB_ID")
log_transition "pre_kill job_state=$STATE_BEFORE"

echo "[g8] 9/12 *** docker kill gs-flink-tm *** (same continuous job)"
set +e
docker kill gs-flink-tm
KILL_RC=$?
set -e
log_transition "docker_kill_rc=$KILL_RC"
sleep 2

echo "[g8] 10/12 wait TM back + job recover from CP"
TM_BACK=0
JOB_RECOVERED=0
LAST_ST=""
TM_START_FALLBACK=0
for i in $(seq 1 "$JOB_RECOVER_SEC"); do
  TM_ST=$(docker inspect -f '{{.State.Status}}' gs-flink-tm 2>/dev/null || echo missing)
  if [[ "$TM_ST" != "running" && "$i" -ge 15 && "$TM_START_FALLBACK" != "1" ]]; then
    echo "[g8] TM still $TM_ST after ${i}s — docker start gs-flink-tm (honest fallback)"
    docker start gs-flink-tm >/dev/null 2>&1 || true
    TM_START_FALLBACK=1
    log_transition "tm_start_fallback=1 after_sec=$i"
  fi
  read -r SLOTS TMS JOBS <<< "$(overview_slots)"
  ST=$(job_state "$JOB_ID")
  if [[ "$ST" != "$LAST_ST" ]]; then
    log_transition "job_state_transition $LAST_ST -> $ST (tm=$TM_ST slots=$SLOTS tms=$TMS)"
    LAST_ST="$ST"
  fi
  if [[ "$TM_ST" == "running" && "${SLOTS:-0}" -ge 1 ]]; then
    if [[ "$TM_BACK" != "1" ]]; then
      TM_BACK=1
      log_transition "tm_back slots=$SLOTS tms=$TMS"
    fi
  fi
  if [[ "$ST" == "RUNNING" && "$TM_BACK" == "1" ]]; then
    JOB_RECOVERED=1
    log_transition "job_recovered RUNNING after TM kill"
    break
  fi
  if [[ "$ST" == "FAILED" || "$ST" == "CANCELED" ]]; then
    if [[ $i -gt 30 ]]; then
      break
    fi
  fi
  if [[ $((i % 5)) -eq 0 ]]; then
    echo "[g8] recover poll $i tm=$TM_ST slots=$SLOTS job=$ST"
  fi
  sleep 1
done

if [[ "$TM_BACK" != "1" ]]; then
  echo "[g8] FAIL: TaskManager did not come back"
  docker ps -a --filter name=gs-flink-tm
  exit 6
fi
if [[ "$JOB_RECOVERED" != "1" ]]; then
  echo "[g8] FAIL: job did not return to RUNNING after TM kill (last=$(job_state "$JOB_ID"))"
  docker exec gs-flink-jm tail -n 80 /tmp/g8-sql-client.log || true
  exit 7
fi

sleep 5
# rematerialize current kafka state after failover
materialize_doris >/dev/null || true
ADS_FAILOVER=$(doris_ads_row)
echo "$ADS_FAILOVER" > /tmp/g8_ads_after_failover.txt
log_transition "ads_after_failover=${ADS_FAILOVER:-empty}"

echo "[g8] 11/12 post-failover dup replay → DAU must stay 8 / pay 2"
docker exec -i gs-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic "$TOPIC_IN" < "$DUP_REPLAY"
ADS_DUP=$(wait_ads_equals 8 2 "after_failover_dup_replay") || {
  docker exec gs-flink-jm tail -n 80 /tmp/g8-sql-client.log || true
  exit 8
}
echo "$ADS_DUP" > /tmp/g8_ads_after_dup.txt
log_transition "ads_after_dup=$ADS_DUP"

echo "[g8] produce batch3 → expect DAU=10 pay_users=2 (ADS keeps updating after failover)"
docker exec -i gs-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic "$TOPIC_IN" < "$BATCH3"
ADS3=$(wait_ads_equals 10 2 "after_batch3") || {
  docker exec gs-flink-jm tail -n 80 /tmp/g8-sql-client.log || true
  exit 9
}
echo "$ADS3" > /tmp/g8_ads_after_batch3.txt
log_transition "ads_after_batch3=$ADS3"

sleep 5
curl -sf "http://127.0.0.1:8081/jobs/${JOB_ID}/checkpoints" > /tmp/g8_cp_after_recover.json || true
CP_AFTER=$(python3 -c 'import json;from pathlib import Path;p=Path("/tmp/g8_cp_after_recover.json");d=json.loads(p.read_text()) if p.exists() and p.stat().st_size else {};print(d.get("counts",{}).get("completed",0))')

echo "[g8] 12/12 write result transcript"
mkdir -p "$(dirname "$RESULT_FILE")"
{
  echo "=== G8 Continuous Doris ADS mainline result ==="
  echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "job_id=$JOB_ID"
  echo "topic_in=$TOPIC_IN topics_ads=$TOPIC_DAU,$TOPIC_PAY group=$G8_GROUP"
  echo "dt=$DT server_id=$SERVER_ID"
  echo "checkpoint_completed_before_kill=$CP_COMPLETED path=$CP_PATH"
  echo "checkpoint_completed_after_recover=$CP_AFTER"
  echo "tm_kill_rc=$KILL_RC tm_start_fallback=$TM_START_FALLBACK"
  echo
  echo "--- ADS snapshots (Doris after kafka→UNIQUE KEY materialize) ---"
  echo "after_batch1(expect 6 1):     $(cat /tmp/g8_ads_after_batch1.txt)"
  echo "after_batch2(expect 8 2):     $(cat /tmp/g8_ads_after_batch2.txt)"
  echo "after_failover(expect ~8 2):  $(cat /tmp/g8_ads_after_failover.txt)"
  echo "after_dup_replay(expect 8 2): $(cat /tmp/g8_ads_after_dup.txt)"
  echo "after_batch3(expect 10 2):    $(cat /tmp/g8_ads_after_batch3.txt)"
  echo
  echo "--- Doris ads_dau_di (dt=$DT server_id=$SERVER_ID) ---"
  docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e \
    "SELECT dt, server_id, dau, metric_id FROM ads.ads_dau_di WHERE dt='${DT}' AND server_id=${SERVER_ID};"
  echo
  echo "--- Doris ads_pay_rate_di (dt=$DT server_id=$SERVER_ID) ---"
  docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e \
    "SELECT dt, server_id, dau, pay_users, pay_rate, metric_id FROM ads.ads_pay_rate_di WHERE dt='${DT}' AND server_id=${SERVER_ID};"
  echo
  echo "--- job transitions ---"
  cat "$TRANSITIONS_FILE"
  echo
  echo "--- sink honesty ---"
  echo "Flink checkpoint mode=EXACTLY_ONCE (internal)."
  echo "Continuous Flink sink = upsert-kafka (ALS + PK)."
  echo "Doris ADS = plain INSERT on UNIQUE KEY (ALS replace)."
  echo "Flink JDBC MySQL upsert (ON DUPLICATE KEY UPDATE) rejected by Doris FE — not used."
  echo "NOT end-to-end EO-2PC."
  echo
  echo "OVERALL: PASS"
} | tee "$RESULT_FILE"

echo "[g8] cancel g8 job to free TM slots (mainline proof done)"
curl -sf -X PATCH "http://127.0.0.1:8081/jobs/${JOB_ID}?mode=cancel" >/dev/null || true
sleep 2
echo "[g8] OK: continuous ADS + dedup + TM failover proven (saved $RESULT_FILE)"
