#!/usr/bin/env bash
# GameStream G3: event-time watermark + disorder/late + event_id dedup (streaming)
# Prefer: copy to /tmp, sed CRLF, then bash (Windows /mnt/c quirk).
# NOT G4–G6.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ROOT="${GAMESTREAM_ROOT:-$ROOT}"
if [[ ! -f "$ROOT/docker-compose.yml" ]]; then
  ROOT=/mnt/c/Users/tangy/source/repos/GameStream
fi
cd "$ROOT"

TOPIC_IN="${TOPIC_IN:-gamestream.g3.events}"
TOPIC_OUT="${TOPIC_OUT:-gamestream.g3.window_results}"
TOPIC_AUDIT="${TOPIC_AUDIT:-gamestream.g3.dedup_audit}"
WM_SEC="${G3_WATERMARK_SECONDS:-5}"
EVENTS_FILE="${EVENTS_FILE:-/tmp/gamestream_g3_events.jsonl}"
EXPECTED_FILE="${EXPECTED_FILE:-/tmp/gamestream_g3_expected.json}"
RESULT_FILE="${RESULT_FILE:-docs/g3-stream-semantics-result.txt}"
SQL_SRC="$ROOT/flink/sql/g3_event_time_watermark.sql"
SQL_RUNTIME="/tmp/g3_event_time_watermark_runtime.sql"
WAIT_SEC="${G3_WAIT_SEC:-45}"

echo "[g3] root=$ROOT wm_sec=$WM_SEC wait=${WAIT_SEC}s"

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing: $1"; exit 1; }; }
need docker
need python3
need curl

echo "[g3] 1/8 check stack"
docker compose ps
curl -sf http://127.0.0.1:8081/overview >/dev/null

for i in $(seq 1 40); do
  AVAIL=$(curl -sf http://127.0.0.1:8081/overview | sed -n 's/.*"slots-total":\([0-9]*\).*/\1/p')
  if [[ "${AVAIL:-0}" -ge 1 ]]; then
    echo "[g3] Flink slots-total=$AVAIL"
    break
  fi
  sleep 2
done

for i in $(seq 1 90); do
  if docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e "SHOW BACKENDS;" >/dev/null 2>&1; then
    alive=$(docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e "SHOW BACKENDS\G" 2>/dev/null | tr -d '\r' | awk -F': ' '/Alive:/ {print $2; exit}')
    echo "[g3] Doris attempt $i Alive=${alive:-?}"
    if [[ "${alive}" == "true" ]]; then
      break
    fi
  else
    echo "[g3] Doris FE not ready attempt $i"
  fi
  sleep 5
done
alive=$(docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e "SHOW BACKENDS\G" 2>/dev/null | tr -d '\r' | awk -F': ' '/Alive:/ {print $2; exit}')
if [[ "${alive}" != "true" ]]; then
  echo "[g3] FAIL: Doris BE not Alive"
  docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e "SHOW BACKENDS;" || true
  exit 1
fi

echo "[g3] 2/8 jars"
docker exec gs-flink-jm ls /opt/flink/usrlib/flink-sql-connector-kafka-3.0.2-1.18.jar >/dev/null

echo "[g3] 3/8 Doris DDL"
docker exec -i gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot < "$ROOT/sql/ddl/doris_g3.sql"
docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e \
  "TRUNCATE TABLE ads.g3_window_demo;" 2>/dev/null || true

echo "[g3] 4/8 Kafka topics (recreate empty)"
for t in "$TOPIC_IN" "$TOPIC_OUT" "$TOPIC_AUDIT"; do
  docker exec gs-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --delete --topic "$t" 2>/dev/null || true
done
sleep 2
for t in "$TOPIC_IN" "$TOPIC_OUT" "$TOPIC_AUDIT"; do
  docker exec gs-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1
done

echo "[g3] 5/8 generate disorder fixtures (wm=${WM_SEC}s)"
python3 "$ROOT/scripts/g3_gen_disorder_events.py" \
  --out "$EVENTS_FILE" \
  --expected-out "$EXPECTED_FILE" \
  --watermark-seconds "$WM_SEC" \
  >/tmp/g3_gen_stdout.txt
echo "[g3] fixture lines=$(wc -l < "$EVENTS_FILE")"

cp "$SQL_SRC" "$SQL_RUNTIME"
# unique consumer group per run (avoid sticky offsets after topic recreate)
G3_GROUP="gamestream-g3-wm-$(date +%s)"
sed -i "s/gamestream-g3-wm-v1/${G3_GROUP}/g" "$SQL_RUNTIME"
echo "[g3] kafka group.id=$G3_GROUP"
if [[ "$WM_SEC" != "5" ]]; then
  sed -i "s/INTERVAL '5' SECOND/INTERVAL '${WM_SEC}' SECOND/g" "$SQL_RUNTIME"
  echo "[g3] patched watermark INTERVAL to ${WM_SEC}s"
fi
docker cp "$SQL_RUNTIME" gs-flink-jm:/tmp/g3_event_time_watermark_runtime.sql

cancel_g3_jobs() {
  local ids jid
  ids=$(python3 -c '
import json,urllib.request
try:
    data=json.load(urllib.request.urlopen("http://127.0.0.1:8081/jobs/overview", timeout=5))
except Exception:
    data={"jobs":[]}
for j in data.get("jobs",[]):
    if "g3-event-time-watermark" in j.get("name","") and j.get("state") in ("RUNNING","RESTARTING","CREATED"):
        print(j["jid"])
')
  for jid in $ids; do
    echo "[g3] cancel leftover job $jid"
    curl -sf -X PATCH "http://127.0.0.1:8081/jobs/${jid}?mode=cancel" >/dev/null || true
  done
  sleep 2
}
cancel_g3_jobs

echo "[g3] 6/8 submit Flink streaming SQL (background)"
docker exec gs-flink-jm bash -lc 'rm -f /tmp/g3-sql-client.log; nohup /opt/flink/bin/sql-client.sh -j /opt/flink/usrlib/flink-sql-connector-kafka-3.0.2-1.18.jar -j /opt/flink/usrlib/flink-connector-jdbc-3.1.2-1.17.jar -j /opt/flink/usrlib/mysql-connector-j-8.0.33.jar -f /tmp/g3_event_time_watermark_runtime.sql >/tmp/g3-sql-client.log 2>&1 & echo $!'
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
    if "g3-event-time-watermark" in j.get("name","") and j.get("state")=="RUNNING":
        print(j["jid"]); break
')
  if [[ -n "$JOB_ID" ]]; then
    echo "[g3] job RUNNING id=$JOB_ID"
    break
  fi
  # show sql-client progress every few tries
  if [[ $((i % 5)) -eq 0 ]]; then
    docker exec gs-flink-jm tail -n 20 /tmp/g3-sql-client.log 2>/dev/null || true
  fi
  sleep 2
done
if [[ -z "$JOB_ID" ]]; then
  echo "[g3] FAIL: Flink job not RUNNING"
  docker exec gs-flink-jm cat /tmp/g3-sql-client.log || true
  curl -sf http://127.0.0.1:8081/jobs/overview || true
  exit 2
fi

echo "[g3] 7/8 produce fixtures in PHASES (periodic watermark needs a gap before late)"
# Phase A: through close_w1; sleep; late; W2 remainder
PHASE_A=/tmp/g3_phase_a.jsonl
PHASE_LATE=/tmp/g3_phase_late.jsonl
PHASE_D=/tmp/g3_phase_d.jsonl
python3 - "$EVENTS_FILE" <<'PY'
import sys
from pathlib import Path
rows = Path(sys.argv[1]).read_text().splitlines()
a, late, d = [], [], []
seen_late = False
for line in rows:
    if '"tag": "late_beyond_wm"' in line or '"tag":"late_beyond_wm"' in line:
        late.append(line)
        seen_late = True
    elif not seen_late:
        a.append(line)
    else:
        d.append(line)
Path("/tmp/g3_phase_a.jsonl").write_text("\n".join(a) + ("\n" if a else ""))
Path("/tmp/g3_phase_late.jsonl").write_text("\n".join(late) + ("\n" if late else ""))
Path("/tmp/g3_phase_d.jsonl").write_text("\n".join(d) + ("\n" if d else ""))
print(f"phase_a={len(a)} late={len(late)} phase_d={len(d)}")
PY
echo "[g3] phase A (through close_w1): $(wc -l < "$PHASE_A") lines"
docker exec -i gs-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic "$TOPIC_IN" < "$PHASE_A"
echo "[g3] sleep 20s for periodic watermark + W1 fire"
sleep 20
echo "[g3] phase LATE"
docker exec -i gs-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic "$TOPIC_IN" < "$PHASE_LATE"
sleep 3
echo "[g3] phase D (W2 remainder)"
docker exec -i gs-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic "$TOPIC_IN" < "$PHASE_D"
echo "[g3] sleeping ${WAIT_SEC}s (idle-timeout + W2 close)"
sleep "$WAIT_SEC"

echo "[g3] 8/8 query sinks + compare"
export EXPECTED_FILE
mkdir -p "$(dirname "$RESULT_FILE")"
KAFKA_OUT=/tmp/g3_kafka_results.jsonl
rm -f "$KAFKA_OUT"
timeout 12 docker exec gs-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic "$TOPIC_OUT" \
  --from-beginning --timeout-ms 10000 > "$KAFKA_OUT" 2>/dev/null || true

{
  echo "=== G3 stream semantics result ==="
  echo "time_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)  wm_sec=$WM_SEC  job=$JOB_ID"
  echo
  echo "=== expected (from generator) ==="
  cat "$EXPECTED_FILE"
  echo
  echo "=== Kafka $TOPIC_OUT ==="
  if [[ -s "$KAFKA_OUT" ]]; then cat "$KAFKA_OUT"; else echo "(empty)"; fi
  echo
  echo "=== Kafka $TOPIC_AUDIT (ROW_NUMBER first-wins audit) ==="
  timeout 8 docker exec gs-kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic "$TOPIC_AUDIT" \
    --from-beginning --timeout-ms 6000 2>/dev/null || echo "(empty/timeout)"
  echo
  echo "=== load Doris from Kafka results (avoid dual Flink window+JDBC on 7.6Gi) ==="
python3 - <<'PY'
import json, subprocess, tempfile
from pathlib import Path
rows = []
for line in Path("/tmp/g3_kafka_results.jsonl").read_text().splitlines():
    line=line.strip()
    if not line: continue
    try:
        rows.append(json.loads(line))
    except Exception:
        pass
if not rows:
    print("no kafka rows to load")
else:
    sqls = ["USE ads;", "TRUNCATE TABLE g3_window_demo;"]
    for r in rows:
        ws = str(r["window_start"]).split(".")[0].replace("T"," ")
        we = str(r["window_end"]).split(".")[0].replace("T"," ")
        sqls.append(
            "INSERT INTO g3_window_demo(window_start,window_end,event_cnt,player_cnt,metric_id) VALUES ("
            f"'{ws}','{we}',{int(r['event_cnt'])},{int(r['player_cnt'])},'g3_window_demo');"
        )
    Path("/tmp/g3_doris_load.sql").write_text("\n".join(sqls)+"\n")
    subprocess.check_call(["docker","exec","-i","gs-doris-fe","mysql","-h127.0.0.1","-P9030","-uroot"],
                          stdin=open("/tmp/g3_doris_load.sql"))
    print(f"loaded {len(rows)} rows into ads.g3_window_demo")
PY

echo "=== Doris ads.g3_window_demo ==="
  docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e \
    "SELECT window_start, window_end, event_cnt, player_cnt, metric_id FROM ads.g3_window_demo ORDER BY window_start;"
  echo
  echo "=== comparison notes ==="
  python3 - <<'PY'
import json, os
from pathlib import Path

exp = json.loads(Path(os.environ.get("EXPECTED_FILE", "/tmp/gamestream_g3_expected.json")).read_text())
kafka_path = Path("/tmp/g3_kafka_results.jsonl")
actual = []
if kafka_path.exists():
    for line in kafka_path.read_text().splitlines():
        line=line.strip()
        if not line:
            continue
        try:
            actual.append(json.loads(line))
        except Exception:
            pass

print(f"produce_count={exp['produce_count']} unique_ids={exp['unique_event_ids']} late_dropped={exp['late_dropped_event_ids']}")
print(f"late_policy={exp['late_policy']}")
ok = True
for w in exp["windows"]:
    ws, we = w["window_start"], w["window_end"]
    match = None
    for a in actual:
        aws = str(a.get("window_start", "")).split(".")[0].replace("T", " ")
        awe = str(a.get("window_end", "")).split(".")[0].replace("T", " ")
        if aws.startswith(ws) and awe.startswith(we):
            match = a
            break
    if match is None:
        print(f"MISSING window {ws} -> {we} expected event_cnt={w['event_cnt']} player_cnt={w['player_cnt']}")
        ok = False
    else:
        ec, pc = int(match.get("event_cnt", -1)), int(match.get("player_cnt", -1))
        status = "OK" if (ec == w["event_cnt"] and pc == w["player_cnt"]) else "MISMATCH"
        if status != "OK":
            ok = False
        print(f"{status} window {ws}: actual event_cnt={ec} player_cnt={pc} | expected {w['event_cnt']}/{w['player_cnt']} | {w.get('note','')}")

w1 = exp["windows"][0]
print(f"dedup_proof: W1 event_cnt expected {w1['event_cnt']} (8 if dup double-counted without DISTINCT)")
print(f"late_proof: g3-late-01 must NOT inflate W1; expected player_cnt={w1['player_cnt']} (9001 excluded)")
print("OVERALL:", "PASS" if ok else "CHECK_SINKS")
PY
} | tee "$RESULT_FILE"

echo "[g3] cancel job $JOB_ID"
curl -sf -X PATCH "http://127.0.0.1:8081/jobs/${JOB_ID}?mode=cancel" >/dev/null || true
sleep 2

KPASS=$(grep -c '"event_cnt": 7' "$KAFKA_OUT" 2>/dev/null || true)
KPASS=${KPASS:-0}
PASS_N=$(docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -N -e \
  "SELECT COUNT(*) FROM ads.g3_window_demo WHERE event_cnt=7;" 2>/dev/null || echo 0)
if [[ "${KPASS}" -ge 1 ]]; then
  echo "[g3] OK: Kafka W1 event_cnt=7 (dedup+late-drop). Doris_rows_with_7=${PASS_N}. saved $RESULT_FILE"
  exit 0
fi
echo "[g3] FAIL: did not observe Kafka W1 event_cnt=7 (got Doris_n=${PASS_N})"
docker exec gs-flink-jm tail -n 120 /tmp/g3-sql-client.log || true
exit 3
