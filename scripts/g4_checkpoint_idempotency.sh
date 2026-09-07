#!/usr/bin/env bash
# GameStream G4: checkpoint + recharge event_id idempotency (streaming)
# Prefer: copy to /tmp, sed CRLF, then bash (Windows /mnt/c quirk).
# NOT G5 kill-TM, NOT G6 skew/Lag/P95.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ROOT="${GAMESTREAM_ROOT:-$ROOT}"
if [[ ! -f "$ROOT/docker-compose.yml" ]]; then
  ROOT=/mnt/c/Users/tangy/source/repos/GameStream
fi
cd "$ROOT"

TOPIC_IN="${TOPIC_IN:-gamestream.g4.recharge}"
TOPIC_DEDUP="${TOPIC_DEDUP:-gamestream.g4.recharge_dedup}"
TOPIC_AGG="${TOPIC_AGG:-gamestream.g4.recharge_agg}"
EVENTS_FILE="${EVENTS_FILE:-/tmp/gamestream_g4_recharge.jsonl}"
DUP_REPLAY_FILE="${DUP_REPLAY_FILE:-/tmp/gamestream_g4_dup_replay.jsonl}"
NEW_EVENT_FILE="${NEW_EVENT_FILE:-/tmp/gamestream_g4_new.jsonl}"
RESULT_FILE="${RESULT_FILE:-docs/g4-checkpoint-idempotency-result.txt}"
SQL_SRC="$ROOT/flink/sql/g4_checkpoint_idempotency.sql"
SQL_RUNTIME="/tmp/g4_checkpoint_idempotency_runtime.sql"
G4_GROUP="${G4_GROUP:-gamestream-g4-cp-v1}"
CP_WAIT_SEC="${G4_CP_WAIT_SEC:-40}"

echo "[g4] root=$ROOT group=$G4_GROUP cp_wait=${CP_WAIT_SEC}s"

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing: $1"; exit 1; }; }
need docker
need python3
need curl

echo "[g4] 1/10 check stack + checkpoint volume"
docker compose ps
mkdir -p "$ROOT/flink/checkpoints"

echo "[g4] wait Flink UI + slots"
UI_OK=0
for i in $(seq 1 60); do
  if curl -sf http://127.0.0.1:8081/overview >/dev/null; then
    AVAIL=$(curl -sf http://127.0.0.1:8081/overview | sed -n 's/.*"slots-total":\([0-9]*\).*/\1/p')
    echo "[g4] Flink UI up try=$i slots-total=${AVAIL:-0}"
    if [[ "${AVAIL:-0}" -ge 1 ]]; then
      UI_OK=1
      break
    fi
  else
    echo "[g4] Flink UI not ready try=$i"
  fi
  sleep 2
done
if [[ "$UI_OK" != "1" ]]; then
  echo "[g4] FAIL: Flink UI/slots not ready"
  exit 1
fi

docker exec gs-flink-jm bash -lc 'ls -ld /checkpoints && touch /checkpoints/.g4_write_test && rm -f /checkpoints/.g4_write_test'
docker exec gs-flink-tm bash -lc 'ls -ld /checkpoints'

alive="false"
for i in $(seq 1 90); do
  if docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e "SHOW BACKENDS;" >/dev/null 2>&1; then
    alive=$(docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e "SHOW BACKENDS\G" 2>/dev/null | tr -d '\r' | awk -F': ' '/Alive:/ {print $2; exit}')
    echo "[g4] Doris attempt $i Alive=${alive:-?}"
    if [[ "${alive}" == "true" ]]; then
      break
    fi
  else
    echo "[g4] Doris FE not ready attempt $i"
  fi
  sleep 5
done
if [[ "${alive}" != "true" ]]; then
  echo "[g4] WARN: Doris BE not Alive — Kafka proof still runs; Doris load may skip"
fi

echo "[g4] 2/10 jars"
docker exec gs-flink-jm ls /opt/flink/usrlib/flink-sql-connector-kafka-3.0.2-1.18.jar >/dev/null

echo "[g4] 3/10 Doris DDL (best-effort)"
if [[ "${alive}" == "true" ]]; then
  docker exec -i gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot < "$ROOT/sql/ddl/doris_g4.sql"
  docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e \
    "TRUNCATE TABLE ads.g4_recharge_di;" 2>/dev/null || true
fi

echo "[g4] 4/10 Kafka topics (recreate empty)"
for t in "$TOPIC_IN" "$TOPIC_DEDUP" "$TOPIC_AGG"; do
  docker exec gs-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --delete --topic "$t" 2>/dev/null || true
done
sleep 2
for t in "$TOPIC_IN" "$TOPIC_DEDUP" "$TOPIC_AGG"; do
  docker exec gs-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1
done

echo "[g4] 5/10 generate recharge fixtures"
python3 - <<'PY'
import json
from pathlib import Path
rows = [
    {"event_id": "g4-r-01", "event_type": "recharge", "event_time": "2026-09-07 12:00:01",
     "player_id": 1001, "server_id": 1, "amount": 6.00, "tag": "unique"},
    {"event_id": "g4-r-02", "event_type": "recharge", "event_time": "2026-09-07 12:00:02",
     "player_id": 1002, "server_id": 1, "amount": 12.00, "tag": "unique"},
    {"event_id": "g4-r-03", "event_type": "recharge", "event_time": "2026-09-07 12:00:03",
     "player_id": 1003, "server_id": 1, "amount": 30.00, "tag": "unique"},
    {"event_id": "g4-r-04", "event_type": "recharge", "event_time": "2026-09-07 12:00:04",
     "player_id": 1004, "server_id": 1, "amount": 68.00, "tag": "unique"},
    {"event_id": "g4-r-05", "event_type": "recharge", "event_time": "2026-09-07 12:00:05",
     "player_id": 1005, "server_id": 1, "amount": 128.00, "tag": "unique"},
    {"event_id": "g4-r-01", "event_type": "recharge", "event_time": "2026-09-07 12:00:06",
     "player_id": 1001, "server_id": 1, "amount": 6.00, "tag": "dup_replay"},
    {"event_id": "g4-r-03", "event_type": "recharge", "event_time": "2026-09-07 12:00:07",
     "player_id": 1003, "server_id": 1, "amount": 30.00, "tag": "dup_replay"},
]
Path("/tmp/gamestream_g4_recharge.jsonl").write_text(
    "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n"
)
dups = [r for r in rows if r["tag"] == "dup_replay"]
dups.append(dict(rows[1], tag="post_restore_replay", event_time="2026-09-07 12:01:00"))
Path("/tmp/gamestream_g4_dup_replay.jsonl").write_text(
    "\n".join(json.dumps(r, ensure_ascii=False) for r in dups) + "\n"
)
new = {"event_id": "g4-r-06", "event_type": "recharge", "event_time": "2026-09-07 12:02:00",
       "player_id": 1006, "server_id": 1, "amount": 98.00, "tag": "new_after_restore"}
Path("/tmp/gamestream_g4_new.jsonl").write_text(json.dumps(new, ensure_ascii=False) + "\n")
print(f"batch1_lines={len(rows)} unique=5 expected_sum=244.00 dup_replay_lines={len(dups)} new=1")
PY

cp "$SQL_SRC" "$SQL_RUNTIME"
sed -i "s/gamestream-g4-cp-v1/${G4_GROUP}/g" "$SQL_RUNTIME"
docker cp "$SQL_RUNTIME" gs-flink-jm:/tmp/g4_checkpoint_idempotency_runtime.sql

cancel_g4_jobs() {
  local ids jid
  ids=$(python3 -c '
import json,urllib.request
try:
    data=json.load(urllib.request.urlopen("http://127.0.0.1:8081/jobs/overview", timeout=5))
except Exception:
    data={"jobs":[]}
for j in data.get("jobs",[]):
    if "g4-checkpoint-idempotency" in j.get("name","") and j.get("state") in ("RUNNING","RESTARTING","CREATED"):
        print(j["jid"])
')
  for jid in $ids; do
    echo "[g4] cancel leftover job $jid"
    curl -sf -X PATCH "http://127.0.0.1:8081/jobs/${jid}?mode=cancel" >/dev/null || true
  done
  sleep 2
}
cancel_g4_jobs

docker exec gs-flink-jm bash -lc 'rm -rf /checkpoints/* 2>/dev/null; ls -la /checkpoints || true'

read_latest_agg() {
  local out=/tmp/g4_agg_snap.jsonl
  rm -f "$out"
  timeout 10 docker exec gs-kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic "$TOPIC_AGG" \
    --from-beginning --property print.key=true --property key.separator="|" \
    --timeout-ms 8000 > "$out" 2>/dev/null || true
  python3 - <<'PY'
import json
from pathlib import Path
path = Path("/tmp/g4_agg_snap.jsonl")
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
  for i in $(seq 1 30); do
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
        echo "[g4] $label OK agg=$got" >&2
        echo "$got"
        return 0
      fi
      echo "[g4] $label waiting agg=$got (want cnt=$want_cnt sum=$want_sum) try=$i" >&2
    else
      echo "[g4] $label waiting empty agg try=$i" >&2
    fi
    sleep 2
  done
  echo "[g4] FAIL: $label did not reach cnt=$want_cnt sum=$want_sum (last=$got)" >&2
  return 1
}

echo "[g4] 6/10 submit Flink streaming SQL (no savepoint)"
docker exec gs-flink-jm bash -lc 'rm -f /tmp/g4-sql-client.log; nohup /opt/flink/bin/sql-client.sh -j /opt/flink/usrlib/flink-sql-connector-kafka-3.0.2-1.18.jar -j /opt/flink/usrlib/flink-connector-jdbc-3.1.2-1.17.jar -j /opt/flink/usrlib/mysql-connector-j-8.0.33.jar -f /tmp/g4_checkpoint_idempotency_runtime.sql >/tmp/g4-sql-client.log 2>&1 & echo $!'
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
    if "g4-checkpoint-idempotency" in j.get("name","") and j.get("state")=="RUNNING":
        print(j["jid"]); break
')
  if [[ -n "$JOB_ID" ]]; then
    echo "[g4] job RUNNING id=$JOB_ID"
    break
  fi
  if [[ $((i % 5)) -eq 0 ]]; then
    docker exec gs-flink-jm tail -n 30 /tmp/g4-sql-client.log 2>/dev/null || true
  fi
  sleep 2
done
if [[ -z "$JOB_ID" ]]; then
  echo "[g4] FAIL: Flink job not RUNNING"
  docker exec gs-flink-jm cat /tmp/g4-sql-client.log || true
  exit 2
fi

echo "[g4] 7/10 produce batch1 (5 unique + 2 dups)"
docker exec -i gs-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic "$TOPIC_IN" < "$EVENTS_FILE"

echo "[g4] wait for agg = 5 / 244.00"
AGG_BEFORE=$(wait_agg_equals 5 244.00 "before_cancel") || {
  docker exec gs-flink-jm tail -n 80 /tmp/g4-sql-client.log || true
  exit 3
}
echo "$AGG_BEFORE" > /tmp/g4_agg_before.json

echo "[g4] wait for >=1 completed checkpoint (REST)"
CP_JSON=/tmp/g4_checkpoints.json
CP_COMPLETED=0
CP_PATH=""
for i in $(seq 1 "$CP_WAIT_SEC"); do
  curl -sf "http://127.0.0.1:8081/jobs/${JOB_ID}/checkpoints" > "$CP_JSON" || true
  eval "$(python3 - <<'PY'
import json
from pathlib import Path
p=Path("/tmp/g4_checkpoints.json")
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
  echo "[g4] checkpoint poll $i completed=$CP_COMPLETED path=${CP_PATH:-none}"
  if [[ "${CP_COMPLETED:-0}" -ge 1 && -n "${CP_PATH}" ]]; then
    break
  fi
  sleep 1
done
if [[ "${CP_COMPLETED:-0}" -lt 1 ]]; then
  echo "[g4] FAIL: no completed checkpoint"
  cat "$CP_JSON" || true
  docker exec gs-flink-jm ls -laR /checkpoints || true
  exit 4
fi
echo "$CP_COMPLETED" > /tmp/g4_cp_completed.txt
curl -sf "http://127.0.0.1:8081/jobs/${JOB_ID}/checkpoints" > /tmp/g4_cp_before_cancel.json || true
echo "[g4] completed_checkpoints=$CP_COMPLETED external_path=$CP_PATH"

echo "[g4] 8/10 cancel job (retain externalized CP)"
curl -sf -X PATCH "http://127.0.0.1:8081/jobs/${JOB_ID}?mode=cancel" >/dev/null || true
for i in $(seq 1 30); do
  st=$(JOB="$JOB_ID" python3 -c '
import json,urllib.request,os
jid=os.environ["JOB"]
try:
  d=json.load(urllib.request.urlopen(f"http://127.0.0.1:8081/jobs/{jid}", timeout=5))
  print(d.get("state",""))
except Exception:
  print("GONE")
')
  echo "[g4] job state after cancel: $st"
  if [[ "$st" == "CANCELED" || "$st" == "GONE" || "$st" == "FAILED" ]]; then
    break
  fi
  sleep 1
done
sleep 2
curl -sf "http://127.0.0.1:8081/jobs/${JOB_ID}/checkpoints" > /tmp/g4_cp_after_cancel.json || true
CP_PATH=$(python3 - <<'PY'
import json
from pathlib import Path
for fp in ("/tmp/g4_cp_after_cancel.json","/tmp/g4_cp_before_cancel.json"):
    p=Path(fp)
    if not p.exists() or p.stat().st_size==0:
        continue
    d=json.loads(p.read_text())
    latest=(d.get("latest") or {}).get("completed") or {}
    path=latest.get("external_path") or ""
    if path:
        print(path); break
PY
)
echo "[g4] restore path=$CP_PATH"
if [[ -z "$CP_PATH" ]]; then
  CP_PATH=$(docker exec gs-flink-jm bash -lc 'ls -1dt /checkpoints/*/chk-* 2>/dev/null | head -1')
  echo "[g4] filesystem fallback path=$CP_PATH"
fi
if [[ -z "$CP_PATH" ]]; then
  echo "[g4] FAIL: no checkpoint path to restore"
  docker exec gs-flink-jm ls -laR /checkpoints || true
  exit 5
fi
echo "$CP_PATH" > /tmp/g4_cp_path.txt

echo "[g4] 9/10 restore from checkpoint + re-produce dups"
python3 - "$SQL_SRC" "$SQL_RUNTIME" "$G4_GROUP" "$CP_PATH" <<'PY'
import sys
from pathlib import Path
src, dst, group, cp = sys.argv[1:5]
text = Path(src).read_text().replace("gamestream-g4-cp-v1", group)
lines = [ln for ln in text.splitlines() if "execution.savepoint.path" not in ln]
out = []
injected = False
for ln in lines:
    out.append(ln)
    if (not injected) and "pipeline.name" in ln:
        out.append(f"SET 'execution.savepoint.path' = '{cp}';")
        injected = True
if not injected:
    out.insert(0, f"SET 'execution.savepoint.path' = '{cp}';")
Path(dst).write_text("\n".join(out) + "\n")
print("wrote", dst, "savepoint=", cp, "group=", group)
PY
docker cp "$SQL_RUNTIME" gs-flink-jm:/tmp/g4_checkpoint_idempotency_runtime.sql

docker exec gs-flink-jm bash -lc 'rm -f /tmp/g4-sql-client-restore.log; nohup /opt/flink/bin/sql-client.sh -j /opt/flink/usrlib/flink-sql-connector-kafka-3.0.2-1.18.jar -j /opt/flink/usrlib/flink-connector-jdbc-3.1.2-1.17.jar -j /opt/flink/usrlib/mysql-connector-j-8.0.33.jar -f /tmp/g4_checkpoint_idempotency_runtime.sql >/tmp/g4-sql-client-restore.log 2>&1 & echo $!'
sleep 10

JOB_ID2=""
for i in $(seq 1 50); do
  JOB_ID2=$(python3 -c '
import json,urllib.request
try:
    data=json.load(urllib.request.urlopen("http://127.0.0.1:8081/jobs/overview", timeout=5))
except Exception:
    data={"jobs":[]}
cands=[]
for j in data.get("jobs",[]):
    if "g4-checkpoint-idempotency" in j.get("name","") and j.get("state")=="RUNNING":
        cands.append(j["jid"])
print(cands[-1] if cands else "")
')
  if [[ -n "$JOB_ID2" ]]; then
    echo "[g4] restored job RUNNING id=$JOB_ID2"
    break
  fi
  if [[ $((i % 5)) -eq 0 ]]; then
    docker exec gs-flink-jm tail -n 40 /tmp/g4-sql-client-restore.log 2>/dev/null || true
  fi
  sleep 2
done
if [[ -z "$JOB_ID2" ]]; then
  echo "[g4] FAIL: restore job not RUNNING"
  docker exec gs-flink-jm cat /tmp/g4-sql-client-restore.log || true
  exit 6
fi

echo "[g4] re-produce duplicate event_ids (post-restore; proves Rank state restored)"
docker exec -i gs-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic "$TOPIC_IN" < "$DUP_REPLAY_FILE"

AGG_AFTER_DUP=$(wait_agg_equals 5 244.00 "after_restore_dup_replay") || {
  docker exec gs-flink-jm tail -n 80 /tmp/g4-sql-client-restore.log || true
  exit 7
}
echo "$AGG_AFTER_DUP" > /tmp/g4_agg_after_dup.json

echo "[g4] produce ONE new unique recharge (98.00) -> expect 6 / 342.00"
docker exec -i gs-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic "$TOPIC_IN" < "$NEW_EVENT_FILE"
AGG_AFTER_NEW=$(wait_agg_equals 6 342.00 "after_new_event") || {
  docker exec gs-flink-jm tail -n 80 /tmp/g4-sql-client-restore.log || true
  exit 8
}
echo "$AGG_AFTER_NEW" > /tmp/g4_agg_after_new.json

sleep 12
curl -sf "http://127.0.0.1:8081/jobs/${JOB_ID2}/checkpoints" > /tmp/g4_cp_restored.json || true
CP2_COMPLETED=$(python3 -c 'import json;from pathlib import Path;p=Path("/tmp/g4_cp_restored.json");d=json.loads(p.read_text()) if p.exists() and p.stat().st_size else {};print(d.get("counts",{}).get("completed",0))')

echo "[g4] 10/10 write result + Doris load"
mkdir -p "$(dirname "$RESULT_FILE")"
DEDUP_OUT=/tmp/g4_dedup_out.jsonl
rm -f "$DEDUP_OUT"
timeout 10 docker exec gs-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic "$TOPIC_DEDUP" \
  --from-beginning --timeout-ms 8000 > "$DEDUP_OUT" 2>/dev/null || true

{
  echo "=== G4 checkpoint + idempotency result ==="
  echo "time_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "job1=$JOB_ID job2_restored=$JOB_ID2"
  echo "group.id=$G4_GROUP"
  echo "checkpoint_interval=10s state.backend=hashmap state.checkpoints.dir=file:///checkpoints"
  echo "compose_volume=./flink/checkpoints:/checkpoints (JM+TM)"
  echo
  echo "=== checkpoint (job1 before cancel) ==="
  echo "completed_count=$CP_COMPLETED"
  echo "external_path=$CP_PATH"
  python3 - <<'PY'
import json
from pathlib import Path
p=Path("/tmp/g4_cp_before_cancel.json")
if p.exists() and p.stat().st_size:
    d=json.loads(p.read_text())
    print("counts=", json.dumps(d.get("counts",{})))
    latest=(d.get("latest") or {}).get("completed") or {}
    keep=["id","status","external_path","checkpointed_size","state_size"]
    print("latest_completed=", json.dumps({k:latest.get(k) for k in keep if k in latest}, ensure_ascii=False))
PY
  echo
  echo "=== aggregates ==="
  echo "before_cancel=$AGG_BEFORE"
  echo "after_restore_dup_replay=$AGG_AFTER_DUP"
  echo "after_new_event=$AGG_AFTER_NEW"
  echo "expected: before=5/244; after dups still 5/244; after new=6/342"
  echo
  echo "=== Kafka dedup audit (unique event_ids) ==="
  if [[ -s "$DEDUP_OUT" ]]; then
    python3 - <<'PY'
import json
from pathlib import Path
ids=[]
for line in Path("/tmp/g4_dedup_out.jsonl").read_text().splitlines():
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
  echo "=== restored job checkpoints completed=$CP2_COMPLETED ==="
  echo
  echo "=== Doris load from final Kafka agg (ALS + UNIQUE KEY; NOT EO-2PC) ==="
  if [[ "${alive}" == "true" ]]; then
    python3 - <<'PY'
import json, subprocess
from pathlib import Path
o=json.loads(Path("/tmp/g4_agg_after_new.json").read_text())
sql=(
 "USE ads;\nTRUNCATE TABLE g4_recharge_di;\n"
 f"INSERT INTO g4_recharge_di(metric_id,recharge_cnt,amount_sum,player_cnt) VALUES ("
 f"'g4_recharge',{int(o['recharge_cnt'])},{float(o['amount_sum'])},{int(o['player_cnt'])});\n"
)
Path("/tmp/g4_doris_load.sql").write_text(sql)
subprocess.check_call(["docker","exec","-i","gs-doris-fe","mysql","-h127.0.0.1","-P9030","-uroot"],
                      stdin=open("/tmp/g4_doris_load.sql"))
print("loaded ads.g4_recharge_di from kafka agg")
PY
    echo "=== Doris ads.g4_recharge_di ==="
    docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e \
      "SELECT metric_id, recharge_cnt, amount_sum, player_cnt FROM ads.g4_recharge_di;"
  else
    echo "Doris BE not Alive — skipped load (Kafka proof stands)"
  fi
  echo
  echo "=== consistency class (honest) ==="
  echo "Flink checkpoint mode=EXACTLY_ONCE (operator state + Kafka source offsets)"
  echo "Kafka upsert sink=at-least-once + PK idempotent upsert (NOT transactional EO)"
  echo "Doris UNIQUE KEY=idempotent absorb of ALS retries (NOT claim end-to-end EO-2PC)"
  echo
  echo "=== OVERALL ==="
  python3 - <<'PY'
import json
from pathlib import Path
b=json.loads(Path("/tmp/g4_agg_before.json").read_text())
d=json.loads(Path("/tmp/g4_agg_after_dup.json").read_text())
n=json.loads(Path("/tmp/g4_agg_after_new.json").read_text())
cp=int(Path("/tmp/g4_cp_completed.txt").read_text().strip() or "0")
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

echo "[g4] cancel restored job $JOB_ID2"
curl -sf -X PATCH "http://127.0.0.1:8081/jobs/${JOB_ID2}?mode=cancel" >/dev/null || true

if grep -E 'PASS' "$RESULT_FILE" >/dev/null; then
  echo "[g4] OK saved $RESULT_FILE"
  exit 0
fi
echo "[g4] FAIL see $RESULT_FILE"
exit 9
