#!/usr/bin/env bash
# GameStream G7 closed loop: NL/Agent (DataPilot or fixtures) → SQLGuard → Doris ADS
# Prefer: cp scripts/g7_closed_loop.sh /tmp/g7.sh && sed -i 's/\r$//' /tmp/g7.sh \
#   && GAMESTREAM_ROOT=/mnt/c/Users/tangy/source/repos/GameStream bash /tmp/g7.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ROOT="${GAMESTREAM_ROOT:-$ROOT}"
if [[ ! -f "$ROOT/docker-compose.yml" ]]; then
  ROOT=/mnt/c/Users/tangy/source/repos/GameStream
fi
cd "$ROOT"

RESULT_FILE="${RESULT_FILE:-docs/g7-closed-loop-result.txt}"
GUARD_URL="${GUARD_URL:-http://127.0.0.1:8787}"
DORIS_URL="${DORIS_URL:-mysql://root@127.0.0.1:9030/ads}"
POLICY="${POLICY:-$ROOT/config/sqlguard/g7_policy.yaml}"
CATALOG="${CATALOG:-$ROOT/config/sqlguard/g7_catalog.json}"
SQLGUARD_REPO="${SQLGUARD_REPO:-/mnt/c/Users/tangy/source/repos/sql-write-gate}"
DATAPILOT_REPO="${DATAPILOT_REPO:-/mnt/c/Users/tangy/source/repos/DataPilot}"
MODE="${MODE:-auto}"   # auto | datapilot | fixture
START_GUARD="${START_GUARD:-1}"
GUARD_PID=""

mkdir -p "$(dirname "$RESULT_FILE")"
: > "$RESULT_FILE"
log() { echo "$@" | tee -a "$RESULT_FILE"; }

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing: $1"; exit 1; }; }
need python3
need curl
need docker

# Prefer local G7 venv if present (WSL): /home/tangy/g7-venv
if [[ -x /home/tangy/g7-venv/bin/python3 ]]; then
  export PATH="/home/tangy/g7-venv/bin:$PATH"
fi

ts_utc8() { TZ=Asia/Shanghai date '+%Y-%m-%d %H:%M:%S CST'; }

json_field() {
  local raw="${1:-}"
  [[ -n "$raw" ]] || { echo ""; return 0; }
  python3 -c "import json,sys; d=json.loads(sys.argv[1]); print(d.get(sys.argv[2],''))" "$raw" "$2"
}

post_json() {
  local path="$1" body="$2"
  curl -sf -X POST "${GUARD_URL}${path}" -H 'Content-Type: application/json' -d "$body"
}

make_body() {
  local sql="$1" summary="$2" db="${3:-$DORIS_URL}"
  python3 -c "import json,sys; print(json.dumps({'sql':sys.argv[1],'actor':'gamestream-g7','model_id':'fixture','prompt_summary':sys.argv[2],'database':sys.argv[3]},ensure_ascii=False))" "$sql" "$summary" "$db"
}

log "=== GameStream G7 closed loop ==="
log "time=$(ts_utc8)"
log "root=$ROOT"
log "doris=$DORIS_URL"
log "policy=$POLICY"
log "catalog=$CATALOG"
log "guard_url=$GUARD_URL"
log "mode=$MODE"
log ""

log "=== 0) Doris ADS probe ==="
DAU_N=$(docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -N -e "SELECT COUNT(*) FROM ads.ads_dau_di;" 2>/dev/null || echo 0)
PAY_N=$(docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -N -e "SELECT COUNT(*) FROM ads.ads_pay_rate_di;" 2>/dev/null || echo 0)
log "ads_dau_di rows=$DAU_N"
log "ads_pay_rate_di rows=$PAY_N"
if [[ "${DAU_N:-0}" -lt 1 || "${PAY_N:-0}" -lt 1 ]]; then
  log "ADS empty — seed via: bash scripts/e2e_g2.sh"
  exit 1
fi
log ""

ensure_python_gate() {
  export PYTHONPATH="${SQLGUARD_REPO}/src${PYTHONPATH:+:$PYTHONPATH}"
  if python3 -c "from write_gate.datapilot import block_or_execute" 2>/dev/null; then
    return 0
  fi
  if [[ -d "$SQLGUARD_REPO" ]]; then
    pip3 install -e "${SQLGUARD_REPO}[mysql]" -q 2>/dev/null \
      || pip3 install -e "$SQLGUARD_REPO" pymysql -q 2>/dev/null \
      || true
  fi
  python3 -c "from write_gate.datapilot import block_or_execute"
}

guard_health() {
  curl -sf "${GUARD_URL}/healthz" >/dev/null 2>&1
}

start_guard_if_needed() {
  if guard_health; then
    log "[guard] already up at $GUARD_URL"
    curl -sf "${GUARD_URL}/healthz" | tee -a "$RESULT_FILE" || true
    log ""
    return 0
  fi
  if [[ "$START_GUARD" != "1" ]]; then
    log "[guard] not running and START_GUARD=0"
    return 1
  fi
  ensure_python_gate || { log "[guard] write_gate unavailable"; return 1; }
  log "[guard] starting serve on 127.0.0.1:8787 ..."
  if command -v sql-write-gate >/dev/null 2>&1; then
    nohup sql-write-gate serve --host 127.0.0.1 --port 8787 \
      --policy "$POLICY" --catalog "$CATALOG" --database "$DORIS_URL" \
      > /tmp/g7_sqlguard_serve.log 2>&1 &
    GUARD_PID=$!
  else
    nohup python3 - "$POLICY" "$CATALOG" "$DORIS_URL" <<'PY' > /tmp/g7_sqlguard_serve.log 2>&1 &
import sys
from write_gate.api import run_serve_cli
pol, cat, db = sys.argv[1:4]
raise SystemExit(run_serve_cli(
    "127.0.0.1", 8787,
    defaults={"database": db, "catalog": cat, "policy": pol, "agent": "gamestream-g7"},
))
PY
    GUARD_PID=$!
  fi
  for _ in $(seq 1 40); do
    if guard_health; then
      log "[guard] up pid=$GUARD_PID"
      curl -sf "${GUARD_URL}/healthz" | tee -a "$RESULT_FILE" || true
      log ""
      return 0
    fi
    sleep 0.4
  done
  log "[guard] failed; tail log:"
  tail -30 /tmp/g7_sqlguard_serve.log 2>/dev/null | tee -a "$RESULT_FILE" || true
  return 1
}

cleanup() {
  if [[ -n "${GUARD_PID:-}" ]]; then
    kill "$GUARD_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

py_gate() {
  local sql="$1" do_exec="$2" summary="$3"
  export PYTHONPATH="${SQLGUARD_REPO}/src${PYTHONPATH:+:$PYTHONPATH}"
  python3 - "$sql" "$do_exec" "$summary" "$CATALOG" "$POLICY" "$DORIS_URL" <<'PY'
import json, sys
from write_gate.datapilot import block_or_execute
sql, do_exec, summary, cat, pol, db = sys.argv[1:7]
payload = block_or_execute(
    sql,
    execute=(do_exec == "1"),
    catalog_path=cat,
    policy_path=pol,
    database=db,
    agent="gamestream-g7",
    prompt_summary=summary or None,
)
print(json.dumps(payload, ensure_ascii=False, default=str))
PY
}

doris_select() {
  docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e "$1"
}

run_fixture_path() {
  local path_id="$1"
  local fixture prompt sql expect metric
  fixture=$(python3 "$ROOT/scripts/g7_agent_fixtures.py" --path "$path_id")
  prompt=$(json_field "$fixture" prompt)
  sql=$(json_field "$fixture" sql)
  expect=$(json_field "$fixture" expect_gate)
  metric=$(json_field "$fixture" metric_id)

  log "======== PATH: $path_id ========"
  log "Prompt: $prompt"
  log "metric_id: ${metric:-n/a}"
  log "SQL: $sql"
  log "expect_gate: $expect"

  local gate_json="" mode_used=""
  if guard_health; then
    mode_used="http POST /v1/check"
    gate_json=$(post_json /v1/check "$(make_body "$sql" "$path_id")") || gate_json=""
  fi
  if [[ -z "$gate_json" ]]; then
    mode_used="python write_gate.datapilot.block_or_execute(execute=False)"
    gate_json=$(py_gate "$sql" "0" "$path_id")
  fi
  log "gate_mode: $mode_used"
  log "gate_result: $gate_json"

  local dp action
  dp=$(json_field "$gate_json" datapilot)
  action=$(json_field "$gate_json" action)
  log "datapilot=$dp action=$action"

  local ok=0
  if [[ "$expect" == "EXECUTE" && ( "$dp" == "EXECUTE" || "$action" == "ALLOW" ) ]]; then
    ok=1
  fi
  if [[ "$expect" == "BLOCK" && ( "$dp" == "BLOCK" || "$action" == "BLOCK" ) ]]; then
    ok=1
  fi
  if [[ "$expect" == "SOFT_DOCUMENT" ]]; then
    ok=1
    log "SOFT_DOCUMENT: recorded datapilot=$dp action=$action (schema strip probe; not a hard fail)"
  fi
  if [[ "$ok" != "1" ]]; then
    log "ERROR: unexpected gate (want $expect got datapilot=$dp action=$action)"
    exit 1
  fi

  if [[ "$expect" == "EXECUTE" ]]; then
    log "--- EXECUTE path: run SELECT on Doris ---"
    local exec_json=""
    if guard_health; then
      exec_json=$(post_json /v1/execute "$(make_body "$sql" "$path_id")" || true)
      log "http POST /v1/execute: $exec_json"
      local executed
      executed=$(json_field "$exec_json" executed)
      if [[ "$executed" == "True" || "$executed" == "true" ]]; then
        log "rows_from_sqlguard_execute:"
        python3 -c "import json,sys; d=json.loads(sys.argv[1]); print(json.dumps({'rowcount':d.get('rowcount'),'rows':d.get('rows')},ensure_ascii=False,default=str,indent=2))" "$exec_json" | tee -a "$RESULT_FILE"
      else
        log "executed=$executed — fallback docker mysql (gate already ALLOW/EXECUTE)"
        doris_select "$sql" | tee -a "$RESULT_FILE"
      fi
    else
      # Python execute=True if pymysql works; else docker mysql after check
      exec_json=$(py_gate "$sql" "1" "$path_id" || true)
      log "python execute payload: $exec_json"
      executed=$(json_field "$exec_json" executed)
      if [[ "$executed" == "True" || "$executed" == "true" ]]; then
        python3 -c "import json,sys; d=json.loads(sys.argv[1]); print(json.dumps({'rowcount':d.get('rowcount'),'rows':d.get('rows')},ensure_ascii=False,default=str,indent=2))" "$exec_json" | tee -a "$RESULT_FILE"
      else
        doris_select "$sql" | tee -a "$RESULT_FILE"
      fi
    fi
  elif [[ "$expect" == "SOFT_DOCUMENT" ]]; then
    log "--- SOFT probe: skip Doris execute (document gate only) ---"
  else
    log "--- BLOCK path: skip Doris execute ---"
  fi
  log ""
}

try_datapilot() {
  [[ -d "$DATAPILOT_REPO/src/datapilot" ]] || return 1
  log "=== optional DataPilot (mock LLM + write_gate + doris) ==="
  export PYTHONPATH="${DATAPILOT_REPO}/src:${SQLGUARD_REPO}/src${PYTHONPATH:+:$PYTHONPATH}"
  if DATAPILOT_LLM_MODE=mock DATAPILOT_GUARD_MODE=write_gate \
      DATAPILOT_QUERY_BACKEND=doris DATAPILOT_DORIS_URL="$DORIS_URL" \
      DATAPILOT_GUARD_CATALOG="$CATALOG" DATAPILOT_GUARD_POLICY="$POLICY" \
      python3 -m datapilot "DAU多少" 2>&1 | tee -a "$RESULT_FILE"; then
    log "[datapilot] ok"
    return 0
  fi
  log "[datapilot] skipped/failed — fixture path remains authoritative for G7 accept"
  return 1
}

# --- main ---
ensure_python_gate || { log "FATAL: cannot import write_gate"; exit 1; }
start_guard_if_needed || log "[warn] HTTP guard down — Python API + docker mysql fallback"

if [[ "$MODE" == "datapilot" || "$MODE" == "auto" ]]; then
  try_datapilot || true
fi

log "=== fixture closed-loop paths (Agent-shaped NL→SQL→SQLGuard→Doris) ==="
run_fixture_path dau
run_fixture_path pay_rate
run_fixture_path block_unknown_column
run_fixture_path block_unknown_table
run_fixture_path cross_db_same_name
run_fixture_path block_delete

log "=== DONE $(ts_utc8) ==="
log "result_file=$RESULT_FILE"
echo "OK: wrote $RESULT_FILE"
