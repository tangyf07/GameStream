#!/usr/bin/env bash
# GameStream G5: TaskManager kill / failover drill (streaming)
# Prefer: copy to /tmp, sed CRLF, then bash (Windows /mnt/c quirk).
# Reuses G4 checkpoint + event_id Rank idempotency. NOT G6 Lag/P95, NOT G7.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ROOT="${GAMESTREAM_ROOT:-$ROOT}"
if [[ ! -f "$ROOT/docker-compose.yml" ]]; then
  ROOT=/mnt/c/Users/tangy/source/repos/GameStream
fi
cd "$ROOT"

TOPIC_IN="${TOPIC_IN:-gamestream.g5.recharge}"
TOPIC_DEDUP="${TOPIC_DEDUP:-gamestream.g5.recharge_dedup}"
TOPIC_AGG="${TOPIC_AGG:-gamestream.g5.recharge_agg}"
EVENTS_FILE="${EVENTS_FILE:-/tmp/gamestream_g5_recharge.jsonl}"
DUP_REPLAY_FILE="${DUP_REPLAY_FILE:-/tmp/gamestream_g5_dup_replay.jsonl}"
NEW_EVENT_FILE="${NEW_EVENT_FILE:-/tmp/gamestream_g5_new.jsonl}"
RESULT_FILE="${RESULT_FILE:-docs/g5-fault-drill-result.txt}"
TRANSITIONS_FILE="${TRANSITIONS_FILE:-/tmp/g5_job_transitions.txt}"
SQL_SRC="$ROOT/flink/sql/g5_fault_drill.sql"
SQL_RUNTIME="/tmp/g5_fault_drill_runtime.sql"
G5_GROUP="${G5_GROUP:-gamestream-g5-fault-v1}"
CP_WAIT_SEC="${G5_CP_WAIT_SEC:-45}"
TM_BACK_SEC="${G5_TM_BACK_SEC:-90}"
JOB_RECOVER_SEC="${G5_JOB_RECOVER_SEC:-120}"

echo "[g5] root=$ROOT group=$G5_GROUP cp_wait=${CP_WAIT_SEC}s"

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

echo "[g5] 1/11 check stack + checkpoint volume"
docker compose ps
mkdir -p "$ROOT/flink/checkpoints"

echo "[g5] wait Flink UI + slots"
UI_OK=0
for i in $(seq 1 60); do
  if curl -sf http://127.0.0.1:8081/overview >/dev/null; then
    read -r SLOTS TMS JOBS <<< "$(overview_slots)"
    echo "[g5] Flink UI up try=$i slots-total=$SLOTS tms=$TMS jobs-running=$JOBS"
    if [[ "${SLOTS:-0}" -ge 1 ]]; then
      UI_OK=1
      break
    fi
  else
    echo "[g5] Flink UI not ready try=$i"
  fi
  sleep 2
done
if [[ "$UI_OK" != "1" ]]; then
  echo "[g5] FAIL: Flink UI/slots not ready"
  exit 1
fi

docker exec gs-flink-jm bash -lc 'ls -ld /checkpoints && touch /checkpoints/.g5_write_test && rm -f /checkpoints/.g5_write_test'
docker exec gs-flink-tm bash -lc 'ls -ld /checkpoints'

alive="false"
for i in $(seq 1 90); do
  if docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e "SHOW BACKENDS;" >/dev/null 2>&1; then
    alive=$(docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e "SHOW BACKENDS\G" 2>/dev/null | tr -d '\r' | awk -F': ' '/Alive:/ {print $2; exit}')
    echo "[g5] Doris attempt $i Alive=${alive:-?}"
    if [[ "${alive}" == "true" ]]; then
      break
    fi
  else
    echo "[g5] Doris FE not ready attempt $i"
  fi
  sleep 5
done
if [[ "${alive}" != "true" ]]; then
  echo "[g5] WARN: Doris BE not Alive — Kafka proof still runs; Doris load may skip"
fi

echo "[g5] 2/11 jars"
docker exec gs-flink-jm ls /opt/flink/usrlib/flink-sql-connector-kafka-3.0.2-1.18.jar >/dev/null

echo "[g5] 3/11 Doris DDL (best-effort)"
if [[ "${alive}" == "true" ]]; then
  docker exec -i gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot < "$ROOT/sql/ddl/doris_g5.sql"
  docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e \
    "TRUNCATE TABLE ads.g5_recharge_di;" 2>/dev/null || true
fi

echo "[g5] 4/11 Kafka topics (recreate empty)"
for t in "$TOPIC_IN" "$TOPIC_DEDUP" "$TOPIC_AGG"; do
  docker exec gs-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --delete --topic "$t" 2>/dev/null || true
done
sleep 2
for t in "$TOPIC_IN" "$TOPIC_DEDUP" "$TOPIC_AGG"; do
  docker exec gs-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1
done

echo "[g5] 5/11 generate recharge fixtures (same money shape as G4)"
python3 - <<'PY'
import json
from pathlib import Path
rows = [
    {"event_id": "g5-r-01", "event_type": "recharge", "event_time": "2026-09-07 12:00:01",
     "player_id": 2001, "server_id": 1, "amount": 6.00, "tag": "unique"},
    {"event_id": "g5-r-02", "event_type": "recharge", "event_time": "2026-09-07 12:00:02",
     "player_id": 2002, "server_id": 1, "amount": 12.00, "tag": "unique"},
    {"event_id": "g5-r-03", "event_type": "recharge", "event_time": "2026-09-07 12:00:03",
     "player_id": 2003, "server_id": 1, "amount": 30.00, "tag": "unique"},
    {"event_id": "g5-r-04", "event_type": "recharge", "event_time": "2026-09-07 12:00:04",
     "player_id": 2004, "server_id": 1, "amount": 68.00, "tag": "unique"},
    {"event_id": "g5-r-05", "event_type": "recharge", "event_time": "2026-09-07 12:00:05",
     "player_id": 2005, "server_id": 1, "amount": 128.00, "tag": "unique"},
    {"event_id": "g5-r-01", "event_type": "recharge", "event_time": "2026-09-07 12:00:06",
     "player_id": 2001, "server_id": 1, "amount": 6.00, "tag": "dup_in_batch"},
    {"event_id": "g5-r-03", "event_type": "recharge", "event_time": "2026-09-07 12:00:07",
     "player_id": 2003, "server_id": 1, "amount": 30.00, "tag": "dup_in_batch"},
]
Path("/tmp/gamestream_g5_recharge.jsonl").write_text(
    "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n"
)
dups = [r for r in rows if r["tag"] == "dup_in_batch"]
dups.append(dict(rows[1], tag="post_tm_kill_replay", event_time="2026-09-07 12:01:00"))
Path("/tmp/gamestream_g5_dup_replay.jsonl").write_text(
    "\n".join(json.dumps(r, ensure_ascii=False) for r in dups) + "\n"
)
new = {"event_id": "g5-r-06", "event_type": "recharge", "event_time": "2026-09-07 12:02:00",
       "player_id": 2006, "server_id": 1, "amount": 98.00, "tag": "new_after_failover"}
Path("/tmp/gamestream_g5_new.jsonl").write_text(json.dumps(new, ensure_ascii=False) + "\n")
print(f"batch1_lines={len(rows)} unique=5 expected_sum=244.00 dup_replay_lines={len(dups)} new=1")
PY

cp "$SQL_SRC" "$SQL_RUNTIME"
sed -i "s/gamestream-g5-fault-v1/${G5_GROUP}/g" "$SQL_RUNTIME"
docker cp "$SQL_RUNTIME" gs-flink-jm:/tmp/g5_fault_drill_runtime.sql

cancel_g5_jobs() {
  local ids jid
  ids=$(python3 -c '
import json,urllib.request
try:
    data=json.load(urllib.request.urlopen("http://127.0.0.1:8081/jobs/overview", timeout=5))
except Exception:
    data={"jobs":[]}
for j in data.get("jobs",[]):
    if "g5-fault-drill" in j.get("name","") and j.get("state") in ("RUNNING","RESTARTING","CREATED","FAILING"):
        print(j["jid"])
')
  for jid in $ids; do
    echo "[g5] cancel leftover job $jid"
    curl -sf -X PATCH "http://127.0.0.1:8081/jobs/${jid}?mode=cancel" >/dev/null || true
  done
  sleep 2
}
cancel_g5_jobs

# Keep prior G4 checkpoint dirs; only clear if empty space is a concern — wipe soft for g5 clarity
docker exec gs-flink-jm bash -lc 'ls -la /checkpoints || true'

read_latest_agg() {
  local out=/tmp/g5_agg_snap.jsonl
  rm -f "$out"
  timeout 10 docker exec gs-kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic "$TOPIC_AGG" \
    --from-beginning --property print.key=true --property key.separator="|" \
    --timeout-ms 8000 > "$out" 2>/dev/null || true
  python3 - <<'PY'
import json
from pathlib import Path
path = Path("/tmp/g5_agg_snap.jsonl")
latest = None
if path.exists():
    for line in path.read_text().splitlines():
        line=line.strip()
        if not line:
            continue
        val = line.split("|", 1)[1] if "|" in line else line
        try:
            latest = json.loads(val)
        except Exception:
            pass
print(json.dumps(latest) if latest else "")
PY
}

wait_agg_equals() {
  local want_cnt="$1" want_sum="$2" label="$3"
  local got="" i
  for i in $(seq 1 36); do
    got=$(read_latest_agg)
    if [[ -n "$got" ]]; then
      ok=$(WANT_CNT="$want_cnt" WANT_SUM="$want_sum" GOT="$got" python3 -c '
import json,os
o=json.loads(os.environ["GOT"])
cnt=int(o.get("recharge_cnt",-1))
s=float(o.get("amount_sum",-1))
print("1" if cnt==int(os.environ["WANT_CNT"]) and abs(s-float(os.environ["WANT_SUM"]))<0.001 else "0")
')
      if [[ "$ok" == "1" ]]; then
        echo "[g5] $label OK agg=$got" >&2
        echo "$got"
        return 0
      fi
      echo "[g5] $label waiting agg=$got (want cnt=$want_cnt sum=$want_sum) try=$i" >&2
    else
      echo "[g5] $label waiting empty agg try=$i" >&2
    fi
    sleep 2
  done
  echo "[g5] FAIL: $label did not reach cnt=$want_cnt sum=$want_sum (last=$got)" >&2
  return 1
}

echo "[g5] 6/11 submit Flink streaming SQL"
docker exec gs-flink-jm bash -lc 'rm -f /tmp/g5-sql-client.log; nohup /opt/flink/bin/sql-client.sh -j /opt/flink/usrlib/flink-sql-connector-kafka-3.0.2-1.18.jar -j /opt/flink/usrlib/flink-connector-jdbc-3.1.2-1.17.jar -j /opt/flink/usrlib/mysql-connector-j-8.0.33.jar -f /tmp/g5_fault_drill_runtime.sql >/tmp/g5-sql-client.log 2>&1 & echo $!'
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
    if "g5-fault-drill" in j.get("name","") and j.get("state")=="RUNNING":
        print(j["jid"]); break
')
  if [[ -n "$JOB_ID" ]]; then
    log_transition "job RUNNING id=$JOB_ID"
    break
  fi
  if [[ $((i % 5)) -eq 0 ]]; then
    docker exec gs-flink-jm tail -n 30 /tmp/g5-sql-client.log 2>/dev/null || true
  fi
  sleep 2
done
if [[ -z "$JOB_ID" ]]; then
  echo "[g5] FAIL: Flink job not RUNNING"
  docker exec gs-flink-jm cat /tmp/g5-sql-client.log || true
  exit 2
fi
echo "$JOB_ID" > /tmp/g5_job_id.txt

echo "[g5] 7/11 produce batch1 (5 unique + 2 dups)"
docker exec -i gs-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic "$TOPIC_IN" < "$EVENTS_FILE"

echo "[g5] wait for agg = 5 / 244.00"
AGG_BEFORE=$(wait_agg_equals 5 244.00 "before_tm_kill") || {
  docker exec gs-flink-jm tail -n 80 /tmp/g5-sql-client.log || true
  exit 3
}
echo "$AGG_BEFORE" > /tmp/g5_agg_before.json
log_transition "agg_before=$AGG_BEFORE"

echo "[g5] wait for >=1 completed checkpoint (REST)"
CP_JSON=/tmp/g5_checkpoints.json
CP_COMPLETED=0
CP_PATH=""
for i in $(seq 1 "$CP_WAIT_SEC"); do
  curl -sf "http://127.0.0.1:8081/jobs/${JOB_ID}/checkpoints" > "$CP_JSON" || true
  eval "$(python3 - <<'PY'
import json
from pathlib import Path
p=Path("/tmp/g5_checkpoints.json")
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
  echo "[g5] checkpoint poll $i completed=$CP_COMPLETED path=${CP_PATH:-none}"
  if [[ "${CP_COMPLETED:-0}" -ge 1 && -n "${CP_PATH}" ]]; then
    break
  fi
  sleep 1
done
if [[ "${CP_COMPLETED:-0}" -lt 1 ]]; then
  echo "[g5] FAIL: no completed checkpoint"
  cat "$CP_JSON" || true
  docker exec gs-flink-jm ls -laR /checkpoints || true
  exit 4
fi
echo "$CP_COMPLETED" > /tmp/g5_cp_completed.txt
echo "$CP_PATH" > /tmp/g5_cp_path.txt
curl -sf "http://127.0.0.1:8081/jobs/${JOB_ID}/checkpoints" > /tmp/g5_cp_before_kill.json || true
log_transition "checkpoint_completed=$CP_COMPLETED path=$CP_PATH"

STATE_BEFORE=$(job_state "$JOB_ID")
log_transition "pre_kill job_state=$STATE_BEFORE"
docker inspect -f '{{.State.Status}} restart={{.HostConfig.RestartPolicy.Name}}' gs-flink-tm > /tmp/g5_tm_before.txt || true
log_transition "tm_before=$(cat /tmp/g5_tm_before.txt)"

echo "[g5] 8/11 *** docker kill gs-flink-tm *** (prefer kill; do NOT kill Doris/Kafka)"
TM_KILL_CMD="docker kill gs-flink-tm"
echo "[g5] running: $TM_KILL_CMD"
set +e
$TM_KILL_CMD
KILL_RC=$?
set -e
log_transition "docker_kill_rc=$KILL_RC"
sleep 2
docker ps -a --filter name=gs-flink-tm --format '{{.Names}} {{.Status}}' | tee /tmp/g5_tm_just_killed.txt || true
log_transition "tm_just_after_kill=$(cat /tmp/g5_tm_just_killed.txt | tr '\n' ' ')"

# Poll job state transitions while TM is down / coming back
echo "[g5] 9/11 wait TM back (restart: unless-stopped) + job recover from CP"
TM_BACK=0
JOB_RECOVERED=0
LAST_ST=""
TM_START_FALLBACK=0
for i in $(seq 1 "$JOB_RECOVER_SEC"); do
  # TM container status
  TM_ST=$(docker inspect -f '{{.State.Status}}' gs-flink-tm 2>/dev/null || echo missing)
  # Docker Desktop / WSL sometimes delays unless-stopped; after 15s force start
  if [[ "$TM_ST" != "running" && "$i" -ge 15 && "$TM_START_FALLBACK" != "1" ]]; then
    echo "[g5] TM still $TM_ST after ${i}s — docker start gs-flink-tm (honest fallback)"
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
  # If job FAILED permanently, abort
  if [[ "$ST" == "FAILED" || "$ST" == "CANCELED" ]]; then
    echo "[g5] WARN: job ended in $ST — check restart-strategy / attempts"
    # allow a few more seconds in case of race
    if [[ $i -gt 30 ]]; then
      break
    fi
  fi
  if [[ $((i % 5)) -eq 0 ]]; then
    echo "[g5] recover poll $i tm=$TM_ST slots=$SLOTS job=$ST"
  fi
  sleep 1
done

if [[ "$TM_BACK" != "1" ]]; then
  echo "[g5] FAIL: TaskManager did not come back (check compose restart: unless-stopped)"
  docker ps -a --filter name=gs-flink-tm
  exit 5
fi
if [[ "$JOB_RECOVERED" != "1" ]]; then
  echo "[g5] FAIL: job did not return to RUNNING after TM kill (last=$(job_state "$JOB_ID"))"
  curl -sf "http://127.0.0.1:8081/jobs/${JOB_ID}" | head -c 2000 || true
  echo
  docker exec gs-flink-jm tail -n 80 /tmp/g5-sql-client.log || true
  exit 6
fi

# Brief settle — optional lag catch-up observation (no invented numbers)
sleep 5
AGG_AFTER_FAILOVER=$(read_latest_agg)
echo "$AGG_AFTER_FAILOVER" > /tmp/g5_agg_after_failover.json
log_transition "agg_after_failover=${AGG_AFTER_FAILOVER:-empty}"

echo "[g5] 10/11 post-failover: re-produce dups (prove Rank state) + one new event"
docker exec -i gs-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic "$TOPIC_IN" < "$DUP_REPLAY_FILE"

AGG_AFTER_DUP=$(wait_agg_equals 5 244.00 "after_failover_dup_replay") || {
  docker exec gs-flink-jm tail -n 80 /tmp/g5-sql-client.log || true
  exit 7
}
echo "$AGG_AFTER_DUP" > /tmp/g5_agg_after_dup.json
log_transition "agg_after_dup=$AGG_AFTER_DUP"

docker exec -i gs-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic "$TOPIC_IN" < "$NEW_EVENT_FILE"
AGG_AFTER_NEW=$(wait_agg_equals 6 342.00 "after_new_event") || {
  docker exec gs-flink-jm tail -n 80 /tmp/g5-sql-client.log || true
  exit 8
}
echo "$AGG_AFTER_NEW" > /tmp/g5_agg_after_new.json
log_transition "agg_after_new=$AGG_AFTER_NEW"

sleep 8
curl -sf "http://127.0.0.1:8081/jobs/${JOB_ID}/checkpoints" > /tmp/g5_cp_after_recover.json || true
CP_AFTER=$(python3 -c 'import json;from pathlib import Path;p=Path("/tmp/g5_cp_after_recover.json");d=json.loads(p.read_text()) if p.exists() and p.stat().st_size else {};print(d.get("counts",{}).get("completed",0))')

echo "[g5] 11/11 write result + Doris load"
mkdir -p "$(dirname "$RESULT_FILE")"
DEDUP_OUT=/tmp/g5_dedup_out.jsonl
rm -f "$DEDUP_OUT"
timeout 10 docker exec gs-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic "$TOPIC_DEDUP" \
  --from-beginning --timeout-ms 8000 > "$DEDUP_OUT" 2>/dev/null || true

{
  echo "=== G5 TaskManager kill / fault drill result ==="
  echo "time_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "job_id=$JOB_ID"
  echo "group.id=$G5_GROUP"
  echo "kill_cmd=docker kill gs-flink-tm"
  echo "checkpoint_interval=10s state.backend=hashmap state.checkpoints.dir=file:///checkpoints"
  echo "restart-strategy=fixed-delay attempts=10 delay=5s(sql)/10s(compose)"
  echo "tm_compose_restart=unless-stopped"
  echo
  echo "=== checkpoint before kill ==="
  echo "completed_count=$CP_COMPLETED"
  echo "external_path=$CP_PATH"
  python3 - <<'PY'
import json
from pathlib import Path
p=Path("/tmp/g5_cp_before_kill.json")
if p.exists() and p.stat().st_size:
    d=json.loads(p.read_text())
    print("counts=", json.dumps(d.get("counts",{})))
    latest=(d.get("latest") or {}).get("completed") or {}
    keep=["id","status","external_path","checkpointed_size","state_size"]
    print("latest_completed=", json.dumps({k:latest.get(k) for k in keep if k in latest}, ensure_ascii=False))
PY
  echo
  echo "=== Flink REST job status transitions ==="
  cat "$TRANSITIONS_FILE"
  echo
  echo "=== aggregates ==="
  echo "before_tm_kill=$AGG_BEFORE"
  echo "after_failover_settle=${AGG_AFTER_FAILOVER:-}"
  echo "after_failover_dup_replay=$AGG_AFTER_DUP"
  echo "after_new_event=$AGG_AFTER_NEW"
  echo "expected: before=5/244; after dups still 5/244; after new=6/342 (NO double prior money)"
  echo
  echo "=== Kafka dedup audit (unique event_ids) ==="
  if [[ -s "$DEDUP_OUT" ]]; then
    python3 - <<'PY'
import json
from pathlib import Path
ids=[]
for line in Path("/tmp/g5_dedup_out.jsonl").read_text().splitlines():
    line=line.strip()
    if not line: continue
    try:
        o=json.loads(line)
        ids.append(o.get("event_id"))
    except Exception:
        pass
print("dedup_rows=", len(ids), "unique_ids=", len(set(ids)))
print("ids=", sorted(set(x for x in ids if x)))
PY
  else
    echo "(empty)"
  fi
  echo
  echo "=== checkpoints after recover completed=$CP_AFTER ==="
  echo
  echo "=== Doris load from final Kafka agg (ALS + UNIQUE KEY; NOT EO-2PC) ==="
  if [[ "${alive}" == "true" ]]; then
    python3 - <<'PY'
import json, subprocess
from pathlib import Path
o=json.loads(Path("/tmp/g5_agg_after_new.json").read_text())
sql=(
 "USE ads;\nTRUNCATE TABLE g5_recharge_di;\n"
 f"INSERT INTO g5_recharge_di(metric_id,recharge_cnt,amount_sum,player_cnt) VALUES ("
 f"'g5_recharge',{int(o['recharge_cnt'])},{float(o['amount_sum'])},{int(o['player_cnt'])});\n"
)
Path("/tmp/g5_doris_load.sql").write_text(sql)
subprocess.check_call(["docker","exec","-i","gs-doris-fe","mysql","-h127.0.0.1","-P9030","-uroot"],
                      stdin=open("/tmp/g5_doris_load.sql"))
print("loaded ads.g5_recharge_di from kafka agg")
PY
    echo "=== Doris ads.g5_recharge_di ==="
    docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e \
      "SELECT metric_id, recharge_cnt, amount_sum, player_cnt FROM ads.g5_recharge_di;"
  else
    echo "Doris BE not Alive — skipped load (Kafka proof stands)"
  fi
  echo
  echo "=== consistency class (honest) ==="
  echo "This is a FAIL-OVER drill (kill TM), not EO-2PC."
  echo "Flink checkpoint mode=EXACTLY_ONCE (operator state + Kafka source offsets)"
  echo "restart-strategy fixed-delay recovers the SAME job id from last completed CP"
  echo "Kafka upsert sink=at-least-once + PK idempotent upsert (NOT transactional EO)"
  echo "Doris UNIQUE KEY=idempotent absorb of ALS retries (NOT claim end-to-end EO-2PC)"
  echo
  echo "=== OVERALL ==="
  python3 - <<'PY'
import json
from pathlib import Path
b=json.loads(Path("/tmp/g5_agg_before.json").read_text())
d=json.loads(Path("/tmp/g5_agg_after_dup.json").read_text())
n=json.loads(Path("/tmp/g5_agg_after_new.json").read_text())
cp=int(Path("/tmp/g5_cp_completed.txt").read_text().strip() or "0")
ok = (int(b["recharge_cnt"])==5 and abs(float(b["amount_sum"])-244.0)<0.001
      and int(d["recharge_cnt"])==5 and abs(float(d["amount_sum"])-244.0)<0.001
      and int(n["recharge_cnt"])==6 and abs(float(n["amount_sum"])-342.0)<0.001
      and cp>=1)
print("PASS" if ok else "FAIL", "cp_completed=", cp)
print("before", b)
print("after_dup", d)
print("after_new", n)
PY
} | tee "$RESULT_FILE"

echo "[g5] cancel job $JOB_ID"
curl -sf -X PATCH "http://127.0.0.1:8081/jobs/${JOB_ID}?mode=cancel" >/dev/null || true

if grep -E 'PASS' "$RESULT_FILE" >/dev/null; then
  echo "[g5] OK saved $RESULT_FILE"
  exit 0
fi
echo "[g5] FAIL see $RESULT_FILE"
exit 9
