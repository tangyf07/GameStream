#!/usr/bin/env bash
# GameStream G6: measured bench ONLY (throughput / lag / CP / E2E P95 / backpressure)
# Prefer: cp to /tmp, sed CRLF, GAMESTREAM_ROOT=... bash
# NOT G7 polish. NEVER invent numbers — write 未测到 + reason when unavailable.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ROOT="${GAMESTREAM_ROOT:-$ROOT}"
if [[ ! -f "$ROOT/docker-compose.yml" ]]; then
  ROOT=/mnt/c/Users/tangy/source/repos/GameStream
fi
cd "$ROOT"

TIER="${TIER:-light}"   # light | medium
case "$TIER" in
  light)  EVENTS="${EVENTS:-10000}"; PLAYERS="${PLAYERS:-500}"; RATE="${RATE:-0}" ;;
  medium) EVENTS="${EVENTS:-30000}"; PLAYERS="${PLAYERS:-1000}"; RATE="${RATE:-0}" ;;
  *) echo "unknown TIER=$TIER (light|medium)"; exit 1 ;;
esac

TOPIC_IN="${TOPIC_IN:-gamestream.g6.events}"
TOPIC_OUT="${TOPIC_OUT:-gamestream.g6.out}"
G6_GROUP="${G6_GROUP:-gamestream-g6-bench-v1}"
SQL_SRC="$ROOT/flink/sql/g6_bench.sql"
SQL_RUNTIME="/tmp/g6_bench_runtime.sql"
EVENTS_FILE="${EVENTS_FILE:-/tmp/gamestream_g6_events.jsonl}"
SINK_FILE="${SINK_FILE:-/tmp/gamestream_g6_sink.jsonl}"
POLL_SEC="${POLL_SEC:-45}"
DRAIN_SEC="${DRAIN_SEC:-90}"
SAMPLE_PY="$ROOT/scripts/g6_sample_metrics.py"
GEN_PY="$ROOT/scripts/g6_gen_and_produce.py"
TS_UTC=$(date -u +%Y%m%dT%H%M%SZ)
RESULT_JSON="${RESULT_JSON:-$ROOT/bench/results/g6_${TIER}_${TS_UTC}.json}"
RESULT_TXT="${RESULT_TXT:-$ROOT/docs/g6-bench-result.txt}"
RAW_DIR="${RAW_DIR:-$ROOT/bench/results/g6_raw_${TIER}_${TS_UTC}}"
mkdir -p "$(dirname "$RESULT_JSON")" "$RAW_DIR" "$(dirname "$RESULT_TXT")"

echo "[g6] root=$ROOT tier=$TIER events=$EVENTS players=$PLAYERS poll=${POLL_SEC}s"
echo "[g6] result_json=$RESULT_JSON"

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing: $1"; exit 1; }; }
need docker
need python3
need curl

MEM_AVAIL=$(free -b 2>/dev/null | awk '/Mem:/ {print $7}' || echo 0)
MEM_NOTE=$(free -h 2>/dev/null | awk '/Mem:/ {printf "total=%s used=%s available=%s",$2,$3,$7}' || echo "free unavailable")

echo "[g6] 1/9 stack check (Kafka+Flink required; Doris optional)"
docker compose ps || true

echo "[g6] wait Flink UI + slots"
UI_OK=0
for i in $(seq 1 60); do
  if curl -sf http://127.0.0.1:8081/overview >/dev/null; then
    SLOTS=$(python3 -c 'import json,urllib.request;d=json.load(urllib.request.urlopen("http://127.0.0.1:8081/overview",timeout=5));print(d.get("slots-total",0))' 2>/dev/null || echo 0)
    echo "[g6] Flink UI try=$i slots-total=$SLOTS"
    if [[ "${SLOTS:-0}" -ge 1 ]]; then UI_OK=1; break; fi
  else
    echo "[g6] Flink UI not ready try=$i"
  fi
  sleep 2
done
if [[ "$UI_OK" != "1" ]]; then
  echo "[g6] FAIL: Flink not ready"
  exit 1
fi

docker exec gs-flink-jm ls /opt/flink/usrlib/flink-sql-connector-kafka-3.0.2-1.18.jar >/dev/null
mkdir -p "$ROOT/flink/checkpoints"
docker exec gs-flink-jm bash -lc 'ls -ld /checkpoints && touch /checkpoints/.g6_write_test && rm -f /checkpoints/.g6_write_test' || {
  echo "[g6] WARN: checkpoint volume not writable — CP metrics may be empty"
}

DORIS_ALIVE=false
if docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e "SHOW BACKENDS;" >/dev/null 2>&1; then
  DORIS_ALIVE=$(docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e "SHOW BACKENDS\G" 2>/dev/null | tr -d '\r' | awk -F': ' '/Alive:/ {print $2; exit}')
fi
echo "[g6] Doris Alive=${DORIS_ALIVE} (Kafka sink path used regardless)"

echo "[g6] 2/9 Kafka topics recreate"
for t in "$TOPIC_IN" "$TOPIC_OUT"; do
  docker exec gs-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$t" 2>/dev/null || true
done
sleep 2
for t in "$TOPIC_IN" "$TOPIC_OUT"; do
  docker exec gs-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1
done

cancel_g6() {
  local ids jid
  ids=$(python3 -c '
import json,urllib.request
try:
  data=json.load(urllib.request.urlopen("http://127.0.0.1:8081/jobs/overview",timeout=5))
except Exception:
  data={"jobs":[]}
for j in data.get("jobs",[]):
  if "g6-bench" in j.get("name","") and j.get("state") in ("RUNNING","RESTARTING","CREATED"):
    print(j["jid"])
')
  for jid in $ids; do
    echo "[g6] cancel leftover $jid"
    curl -sf -X PATCH "http://127.0.0.1:8081/jobs/${jid}?mode=cancel" >/dev/null || true
  done
  sleep 2
}
cancel_g6

echo "[g6] 3/9 prepare SQL runtime (group id)"
cp "$SQL_SRC" "$SQL_RUNTIME"
sed -i "s/gamestream-g6-bench-v1/${G6_GROUP}/g" "$SQL_RUNTIME"
sed -i "s/gamestream.g6.events/${TOPIC_IN}/g" "$SQL_RUNTIME"
sed -i "s/gamestream.g6.out/${TOPIC_OUT}/g" "$SQL_RUNTIME"
docker cp "$SQL_RUNTIME" gs-flink-jm:/tmp/g6_bench_runtime.sql

echo "[g6] 4/9 submit Flink streaming SQL"
docker exec gs-flink-jm bash -lc 'rm -f /tmp/g6-sql-client.log; nohup /opt/flink/bin/sql-client.sh -j /opt/flink/usrlib/flink-sql-connector-kafka-3.0.2-1.18.jar -f /tmp/g6_bench_runtime.sql >/tmp/g6-sql-client.log 2>&1 & echo $!'
sleep 8

JOB_ID=""
for i in $(seq 1 40); do
  JOB_ID=$(python3 -c '
import json,urllib.request
try:
  data=json.load(urllib.request.urlopen("http://127.0.0.1:8081/jobs/overview",timeout=5))
except Exception:
  data={"jobs":[]}
for j in data.get("jobs",[]):
  if "g6-bench" in j.get("name","") and j.get("state")=="RUNNING":
    print(j["jid"]); break
')
  if [[ -n "$JOB_ID" ]]; then
    echo "[g6] job RUNNING id=$JOB_ID"
    break
  fi
  if [[ $((i % 5)) -eq 0 ]]; then
    docker exec gs-flink-jm tail -n 40 /tmp/g6-sql-client.log 2>/dev/null || true
  fi
  sleep 2
done
if [[ -z "$JOB_ID" ]]; then
  echo "[g6] FAIL: job not RUNNING"
  docker exec gs-flink-jm cat /tmp/g6-sql-client.log || true
  exit 2
fi
echo "$JOB_ID" > "$RAW_DIR/job_id.txt"

echo "[g6] 5/9 generate + produce events (stamp produce_ts at write)"
# Stamp produce_ts at generation wallclock immediately before console-producer for E2E
GEN_META=$(python3 "$GEN_PY" --events "$EVENTS" --players "$PLAYERS" --seed 42 \
  --out "$EVENTS_FILE" --stamp-produce-ts ${RATE:+--rate "$RATE"})
echo "$GEN_META" | tee "$RAW_DIR/gen_meta.json"

PRODUCE_T0=$(date +%s.%N)
docker exec -i gs-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic "$TOPIC_IN" < "$EVENTS_FILE"
PRODUCE_T1=$(date +%s.%N)
PRODUCE_SEC=$(python3 -c "print(round(float('$PRODUCE_T1')-float('$PRODUCE_T0'),4))")
PRODUCE_EPS=$(python3 -c "print(round($EVENTS/float('$PRODUCE_SEC'),3) if float('$PRODUCE_SEC')>0 else None)")
echo "[g6] produce_wall_sec=$PRODUCE_SEC input_eps=$PRODUCE_EPS"
echo "{\"events\":$EVENTS,\"produce_wall_sec\":$PRODUCE_SEC,\"input_events_per_sec\":$PRODUCE_EPS}" > "$RAW_DIR/produce.json"

echo "[g6] 6/9 sample Kafka lag + Flink metrics while draining"
set +e
# lag snapshot helper
sample_lag() {
  local label="$1" outf="$2"
  {
    echo "=== lag $label $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
    docker exec gs-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server localhost:9092 --describe --group "$G6_GROUP" 2>&1 || echo "consumer-groups describe failed"
  } | tee "$outf"
}

sample_lag "t0_after_produce" "$RAW_DIR/lag_t0.txt"

# background poll
python3 "$SAMPLE_PY" --action poll --jid "$JOB_ID" --seconds "$POLL_SEC" --interval 2 \
  --out "$RAW_DIR/poll.json" > "$RAW_DIR/poll_summary.json" &
POLL_PID=$!

# wait drain: out topic message count ~ events (best-effort)
DRAIN_OK=0
for i in $(seq 1 "$DRAIN_SEC"); do
  # end offset of out topic
  OUT_END=$(docker exec gs-kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic "$TOPIC_OUT" --time -1 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')
  echo "[g6] drain try=$i out_end_offset=${OUT_END:-0} target=$EVENTS"
  if [[ "${OUT_END:-0}" -ge "$EVENTS" ]]; then
    DRAIN_OK=1
    break
  fi
  sleep 1
done
wait "$POLL_PID" 2>/dev/null || true

sample_lag "t1_after_drain" "$RAW_DIR/lag_t1.txt"

{
  echo "=== topic offsets $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
  docker exec gs-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$TOPIC_IN" --time -1 || true
  docker exec gs-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$TOPIC_OUT" --time -1 || true
  docker exec gs-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list || true
} | tee "$RAW_DIR/offsets_and_groups.txt"


# parse lag numbers if present
LAG_JSON=$(LAG0="$RAW_DIR/lag_t0.txt" LAG1="$RAW_DIR/lag_t1.txt" G6_GROUP="$G6_GROUP" python3 - <<'PY'
import json, os
from pathlib import Path

def parse(p):
    text=Path(p).read_text(errors="replace") if Path(p).exists() else ""
    lags=[]
    for line in text.splitlines():
        parts=line.split()
        if len(parts)>=5 and ("gamestream" in parts[0] or parts[0].startswith("gamestream")):
            # group describe: GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
            # or without GROUP when using some versions — detect by finding numeric lag field
            try:
                # find topic field
                if parts[0] == os.environ.get("G6_GROUP",""):
                    topic, part, cur, end, lag = parts[1], parts[2], parts[3], parts[4], parts[5]
                else:
                    topic, part, cur, end, lag = parts[0], parts[1], parts[2], parts[3], parts[4]
                if str(lag).lstrip("-").isdigit():
                    lags.append({"topic":topic,"partition":int(part),"current_offset":int(cur),
                                 "log_end_offset":int(end),"lag":int(lag)})
            except Exception:
                pass
    return lags

out={
  "t0_after_produce": parse(os.environ["LAG0"]),
  "t1_after_drain": parse(os.environ["LAG1"]),
  "source": "kafka-consumer-groups.sh --describe --group",
}
if not out["t0_after_produce"] and not out["t1_after_drain"]:
    out["status"]="未测到"
    out["reason"]="could not parse consumer-groups describe (group may not commit until CP / or format mismatch); see raw lag_*.txt"
else:
    out["status"]="ok"
print(json.dumps(out, ensure_ascii=False))
PY
)
echo "$LAG_JSON" > "$RAW_DIR/lag.json"

set -e
echo "[g6] 7/9 checkpoint stats + backpressure snapshot"
set +e
python3 "$SAMPLE_PY" --action checkpoints --jid "$JOB_ID" > "$RAW_DIR/checkpoints.json"
python3 "$SAMPLE_PY" --action sample-once --jid "$JOB_ID" > "$RAW_DIR/sample_once.json"
python3 "$SAMPLE_PY" --action vertices-bp --jid "$JOB_ID" > "$RAW_DIR/vertices_bp.json" || true
curl -sf "http://127.0.0.1:8081/jobs/${JOB_ID}/checkpoints" > "$RAW_DIR/checkpoints_raw.json" || true

set -e
echo "[g6] 8/9 consume sink for E2E (produce_ts → sink_ts)"
set +e
rm -f "$SINK_FILE"
timeout 45 docker exec gs-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic "$TOPIC_OUT" \
  --from-beginning --timeout-ms 40000 > "$SINK_FILE" 2>/dev/null || true
SINK_LINES=$(wc -l < "$SINK_FILE" | tr -d ' ')
echo "[g6] sink_lines=$SINK_LINES"
cp "$SINK_FILE" "$RAW_DIR/sink_sample.jsonl" 2>/dev/null || true
# keep at most ~50k lines already; for huge, head is fine — we have EVENTS
python3 "$SAMPLE_PY" --action e2e --sink-jsonl "$SINK_FILE" > "$RAW_DIR/e2e.json"

# processing throughput from wall: events / (drain end - produce start approx)
PROCESS_EPS=$(DRAIN_OK="$DRAIN_OK" EVENTS="$EVENTS" PRODUCE_SEC="$PRODUCE_SEC" POLL_SEC="$POLL_SEC" python3 - <<'PY'
import json, os
from pathlib import Path
# Prefer poll summary flink rate; also wall drain estimate stored later
print("")
PY
)

set -e
echo "[g6] clock skew probe (host UTC vs flink/kafka containers)"
HOST_UTC=$(date -u +%Y-%m-%dT%H:%M:%SZ)
JM_UTC=$(docker exec gs-flink-jm date -u +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || echo "未测到")
TM_UTC=$(docker exec gs-flink-tm date -u +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || echo "未测到")
KFK_UTC=$(docker exec gs-kafka date -u +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || echo "未测到")
python3 - "$RAW_DIR/clock_skew.json" "$HOST_UTC" "$JM_UTC" "$TM_UTC" "$KFK_UTC" <<'PY'
import json, sys
from datetime import datetime
from pathlib import Path
out_path, host, jm, tm, kfk = sys.argv[1:6]
def parse(s):
    if not s or s == "未测到":
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None
h,j,t,k = map(parse, [host,jm,tm,kfk])
def skew(a,b):
    if a is None or b is None: return None
    return (b-a).total_seconds()
out={
  "host_utc": host, "flink_jm_utc": jm, "flink_tm_utc": tm, "kafka_utc": kfk,
  "skew_jm_minus_host_sec": skew(h,j),
  "skew_tm_minus_host_sec": skew(h,t),
  "skew_kafka_minus_host_sec": skew(h,k),
  "note": "E2E uses host produce_ts vs Flink CURRENT_TIMESTAMP; large skew invalidates absolute latency"
}
Path(out_path).write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
print(json.dumps(out, ensure_ascii=False, indent=2))
PY
cat "$RAW_DIR/clock_skew.json"

echo "[g6] 9/9 assemble result JSON (measured only)"
HOST=$(hostname 2>/dev/null || echo unknown)
python3 - <<PY
import json, os
from pathlib import Path
from datetime import datetime, timezone

raw = Path("$RAW_DIR")
def load(name, default=None):
    p = raw / name
    if not p.exists() or p.stat().st_size == 0:
        return default
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        return {"status": "未测到", "reason": f"parse {name}: {e}", "raw_head": p.read_text(errors="replace")[:500]}

produce = load("produce.json", {})
poll = load("poll_summary.json", {})
cp = load("checkpoints.json", {})
e2e = load("e2e.json", {})
lag = load("lag.json", {})
sample = load("sample_once.json", {})
vbp = load("vertices_bp.json", {})
gen = load("gen_meta.json", {})
clock = load("clock_skew.json", {})

# steady processing: if drain completed, estimate events / (produce_sec + drain observed)
# We do not invent — only compute from measured produce_sec and poll flink rates
flink_rate = (poll or {}).get("flink_in_rate_per_s") or {}
bp = (poll or {}).get("backpressure") or {}
# merge checkpoint from poll final if richer
cp_final = (poll or {}).get("checkpoints_final") or cp

# wall processing estimate: time from produce start to out_end >= events
# stored via env
drain_ok = "$DRAIN_OK" == "1"
events = int("$EVENTS")
produce_sec = float("$PRODUCE_SEC")
sink_lines = int("$SINK_LINES" or 0)

processing = {
    "definition": "primary: Flink numRecordsInPerSecond samples during poll; secondary: events/produce_wall_sec is INPUT produce rate only",
    "input_produce_events_per_sec": produce.get("input_events_per_sec"),
    "input_produce_wall_sec": produce.get("produce_wall_sec"),
    "events": events,
    "flink_numRecordsInPerSecond": flink_rate,
    "sink_lines_consumed": sink_lines,
    "drain_reached_target": drain_ok,
}

# backpressure combine
bp_out = bp if bp else {"status": "未测到", "reason": "no poll backpressure"}
if bp_out.get("status") != "ok" and sample:
    thr = (sample.get("throughput_bp") or {})
    if thr.get("backPressuredTimeMsPerSecond_max") is not None:
        bp_out = {
            "status": "ok",
            "max_backPressuredTimeMsPerSecond": thr.get("backPressuredTimeMsPerSecond_max"),
            "observed": thr.get("backpressure_observed"),
            "source": thr.get("source"),
        }
# vertex endpoint as extra evidence
if vbp and vbp.get("status") == "ok":
    bp_out["vertices_backpressure_endpoint"] = vbp
    # Prefer vertex backpressure endpoint as measured evidence when metric timeseries absent
    levels = []
    ratios = []
    for vx in (vbp.get("vertices") or []):
        bp = (vx.get("backpressure") or {})
        levels.append(bp.get("backpressureLevel") or bp.get("backpressure-level"))
        for st in (bp.get("subtasks") or []):
            if st.get("ratio") is not None:
                ratios.append(float(st["ratio"]))
    if levels or ratios:
        observed = any((lv not in (None, "ok")) for lv in levels) or any(r > 0 for r in ratios)
        bp_out = {
            "status": "ok",
            "observed": observed,
            "backpressureLevel_samples": levels,
            "subtask_ratio_samples": ratios,
            "max_subtask_ratio": max(ratios) if ratios else None,
            "source": vbp.get("source"),
            "vertices_backpressure_endpoint": vbp,
            "note": "Job-level backPressuredTimeMsPerSecond may be absent; vertex backpressure endpoint used",
        }

result = {
    "schema": "gamestream.g6.bench.v1",
    "timestamp_utc": "$TS_UTC",
    "tier": "$TIER",
    "host": "$HOST",
    "mem_note": "$MEM_NOTE",
    "job_id": "$JOB_ID",
    "group_id": "$G6_GROUP",
    "topic_in": "$TOPIC_IN",
    "topic_out": "$TOPIC_OUT",
    "sink_path": "Kafka only (Doris skipped for G6 measured bench; Alive=$DORIS_ALIVE)",
    "raw_dir": str(raw),
    "definitions": {
        "throughput_input": "events / produce_wall_sec (console-producer wall clock)",
        "throughput_flink": "Flink REST job metrics numRecordsInPerSecond (sampled)",
        "kafka_lag": "kafka-consumer-groups.sh --describe --group for Flink source group",
        "checkpoint_duration": "Flink REST /jobs/:id/checkpoints history[].end_to_end_duration (ms)",
        "e2e_p95": "P95(sink_ts - produce_ts); produce_ts stamped at JSONL write immediately before produce; sink_ts=Flink CURRENT_TIMESTAMP at sink SELECT",
        "backpressure": "Flink metric backPressuredTimeMsPerSecond and/or vertices/:id/backpressure",
    },
    "metrics": {
        "throughput": processing,
        "kafka_consumer_lag": lag,
        "checkpoint_duration": cp_final if cp_final else cp,
        "e2e_latency": e2e,
        "backpressure": bp_out,
    },
    "gen_meta": gen,
    "clock_skew": clock,
    "honesty": "Numbers below are copied from raw sampler outputs. Missing fields use 未测到 with reason — never fabricated.",
}

out = Path("$RESULT_JSON")
out.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
print(f"Wrote {out}")

# human summary txt
lines = []
lines.append(f"G6 bench result  tier=$TIER  ts=$TS_UTC")
lines.append(f"result_json={out}")
lines.append(f"raw_dir={raw}")
lines.append(f"job_id=$JOB_ID  events={events}  doris_alive=$DORIS_ALIVE")
lines.append("")
m = result["metrics"]
def fmt(block, keys):
    if not isinstance(block, dict):
        return str(block)
    if block.get("status") == "未测到":
        return f"未测到 — {block.get('reason')}"
    parts=[]
    for k in keys:
        if k in block and block[k] is not None:
            parts.append(f"{k}={block[k]}")
    return ", ".join(parts) if parts else json.dumps(block, ensure_ascii=False)[:300]

thr = m["throughput"]
lines.append(f"throughput.input_eps={thr.get('input_produce_events_per_sec')} (file produce.json)")
fr = thr.get("flink_numRecordsInPerSecond") or {}
lines.append(f"throughput.flink_in_rate: status={fr.get('status')} max={fr.get('max')} avg={fr.get('avg')} reason={fr.get('reason')}")
lines.append(f"kafka_lag: {fmt(m.get('kafka_consumer_lag'), ['status','reason'])}")
cpb = m.get("checkpoint_duration") or {}
lines.append(f"checkpoint: status={cpb.get('status')} n={cpb.get('n')} avg_ms={cpb.get('avg_ms')} p50_ms={cpb.get('p50_ms')} p95_ms={cpb.get('p95_ms')} reason={cpb.get('reason')}")
e = m.get("e2e_latency") or {}
lines.append(f"e2e: status={e.get('status')} n={e.get('n')} p95_ms={e.get('p95_ms')} p50_ms={e.get('p50_ms')} avg_ms={e.get('avg_ms')} reason={e.get('reason')}")
b = m.get("backpressure") or {}
lines.append(f"backpressure: status={b.get('status')} observed={b.get('observed')} max_bp_ms_per_s={b.get('max_backPressuredTimeMsPerSecond')} reason={b.get('reason')}")
Path("$RESULT_TXT").write_text("\n".join(lines) + "\n", encoding="utf-8")
print("\n".join(lines))
PY

echo "[g6] cancel job"
curl -sf -X PATCH "http://127.0.0.1:8081/jobs/${JOB_ID}?mode=cancel" >/dev/null || true
echo "[g6] DONE → $RESULT_JSON"
