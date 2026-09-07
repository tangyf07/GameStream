#!/usr/bin/env bash
# GameStream 三仓固化验收（suite pins；G8 不在本脚本范围内）
# Prefer CRLF-safe:
#   cp scripts/suite_acceptance.sh /tmp/suite_acc.sh && sed -i 's/\r$//' /tmp/suite_acc.sh
#   GAMESTREAM_ROOT=/mnt/c/Users/tangy/source/repos/GameStream bash /tmp/suite_acc.sh
#
# MODE=strict (default): required FAIL / pin MISMATCH → exit≠0;
#   required SKIP → INCOMPLETE / exit≠0. Evaluate FAIL before SKIP.
# MODE=report-only: always write report; exit 0 even if FAIL/SKIP/MISMATCH.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ROOT="${GAMESTREAM_ROOT:-$ROOT}"
if [[ ! -f "$ROOT/docker-compose.yml" ]]; then
  ROOT=/mnt/c/Users/tangy/source/repos/GameStream
fi
cd "$ROOT"

LOCK="${LOCK_FILE:-$ROOT/versions.lock}"
RESULT_FILE="${RESULT_FILE:-docs/suite-acceptance-result.txt}"
GUARD_URL="${GUARD_URL:-http://127.0.0.1:8787}"
DORIS_URL="${DORIS_URL:-mysql://root@127.0.0.1:9030/ads}"
POLICY="${POLICY:-$ROOT/config/sqlguard/g7_policy.yaml}"
CATALOG="${CATALOG:-$ROOT/config/sqlguard/g7_catalog.json}"
# strict | report-only  (SUITE_MODE alias accepted)
MODE="${MODE:-${SUITE_MODE:-strict}}"

# Default sibling paths (WSL)
SQLGUARD_REPO="${SQLGUARD_REPO:-/mnt/c/Users/tangy/source/repos/sql-write-gate}"
DATAPILOT_REPO="${DATAPILOT_REPO:-/mnt/c/Users/tangy/source/repos/DataPilot}"

# Pin defaults = verification baselines (overridden by versions.lock [pins])
# Results separately record actual full HEAD / dirty — may differ from pin.
PIN_GS="${PIN_GS:-2253b25}"
PIN_SG="${PIN_SG:-7dc85dd}"
PIN_DP="${PIN_DP:-2541623}"
PIN_SG_VER="${PIN_SG_VER:-1.1.2}"

mkdir -p "$(dirname "$RESULT_FILE")"
: > "$RESULT_FILE"

# Prefer WSL G7 venv
if [[ -x /home/tangy/g7-venv/bin/python3 ]]; then
  export PATH="/home/tangy/g7-venv/bin:$PATH"
fi

ts_utc8() { TZ=Asia/Shanghai date '+%Y-%m-%d %H:%M:%S CST'; }
# log to result + stdout (safe outside command substitution)
log() { echo "$@" | tee -a "$RESULT_FILE"; }
# log only to result file (safe inside $(...))
logf() { echo "$@" >>"$RESULT_FILE"; }

# status helpers — never invent PASS
declare -A CHECK_STATUS=()
declare -A CHECK_DETAIL=()
declare -A CHECK_BACKEND=()
declare -A CHECK_REQUIRED=()

# required checks (optional ones may SKIP without INCOMPLETE)
CHECK_REQUIRED[g7_closed_loop]=1
CHECK_REQUIRED[sqlguard_cross_db_hive]=1
CHECK_REQUIRED[datapilot_offline_p0]=1
CHECK_REQUIRED[sqlguard_unit]=1
# datapilot_doris_g7 is optional

set_status() {
  local id="$1" st="$2" detail="${3:-}" backend="${4:-n/a}"
  # Never classify failed+skipped as SKIP — FAIL wins if caller passes both signals
  if [[ "$st" == "SKIP" ]] && [[ "${detail}" == *FAIL* || "${detail}" == *failed* || "${detail}" == *exit=[1-9]* ]]; then
    st="FAIL"
  fi
  CHECK_STATUS["$id"]="$st"
  CHECK_DETAIL["$id"]="$detail"
  CHECK_BACKEND["$id"]="$backend"
  log "[STATUS] $id=$st backend=$backend detail=$detail"
}

read_pins_from_lock() {
  [[ -f "$LOCK" ]] || { log "[pin] no $LOCK — using defaults GS=$PIN_GS SG=$PIN_SG DP=$PIN_DP"; return 0; }
  local k v section=""
  while IFS= read -r line || [[ -n "$line" ]]; do
    line=$(echo "$line" | sed 's/#.*//;s/[[:space:]]*$//;s/^[[:space:]]*//')
    [[ -z "$line" ]] && continue
    if [[ "$line" == \[*\] ]]; then
      section=$(echo "$line" | tr -d '[]')
      continue
    fi
    # Only accept SHA pins from [pins] (ignore [paths.*] which reuse repo keys)
    [[ "$section" == "pins" ]] || continue
    k=$(echo "$line" | cut -d= -f1 | sed 's/[[:space:]]//g')
    v=$(echo "$line" | cut -d= -f2- | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
    [[ -z "$k" || -z "$v" ]] && continue
    case "$k" in
      GameStream) PIN_GS="$v" ;;
      SQLGuard) PIN_SG="$v" ;;
      DataPilot) PIN_DP="$v" ;;
      SQLGuard_version) PIN_SG_VER="$v" ;;
    esac
  done < "$LOCK"
  log "[pin] verification baselines from $LOCK: GameStream=$PIN_GS SQLGuard=$PIN_SG@$PIN_SG_VER DataPilot=$PIN_DP (suite_p0; baseline_p0=285202c)"
}

short_sha() {
  local full="$1"
  echo "${full:0:7}"
}

repo_dirty_summary() {
  local path="$1"
  if [[ ! -d "$path/.git" ]]; then
    echo "n/a"
    return 0
  fi
  local dirty="clean"
  if ! git -C "$path" diff --quiet 2>/dev/null || ! git -C "$path" diff --cached --quiet 2>/dev/null; then
    dirty="dirty"
  fi
  local untracked
  untracked=$(git -C "$path" ls-files --others --exclude-standard 2>/dev/null | wc -l | tr -d ' ')
  local shortstat
  shortstat=$(git -C "$path" diff --shortstat HEAD 2>/dev/null | tr -d '\n' || true)
  if [[ "$untracked" != "0" ]]; then
    dirty="${dirty}+untracked=${untracked}"
  fi
  if [[ -n "$shortstat" ]]; then
    echo "${dirty};${shortstat}"
  else
    echo "$dirty"
  fi
}

# Echo ONLY token to stdout (OK:/MISMATCH:/MISSING:); details via logf
# GameStream: pin is baseline — OK if HEAD == pin OR pin is ancestor of HEAD ("or later").
# SQLGuard/DataPilot: exact short-SHA match required.
verify_repo_pin() {
  local name="$1" path="$2" expect="$3"
  if [[ ! -d "$path/.git" ]]; then
    logf "[pin] $name clone missing at $path → MISSING"
    echo "MISSING:n/a"
    return 0
  fi
  local head_full head_short expect_short dirty
  head_full=$(git -C "$path" rev-parse HEAD 2>/dev/null || echo unknown)
  head_short=$(short_sha "$head_full")
  expect_short=$(short_sha "$expect")
  dirty=$(repo_dirty_summary "$path")
  logf "[pin] $name actual_HEAD_full=$head_full short=$head_short dirty=$dirty pin_baseline=$expect"
  if [[ "$name" == "GameStream" ]]; then
    if [[ "$head_short" == "$expect_short" ]] || [[ "$head_full" == "$expect"* ]] \
      || git -C "$path" merge-base --is-ancestor "$expect" HEAD 2>/dev/null \
      || git -C "$path" merge-base --is-ancestor "$expect_short" HEAD 2>/dev/null; then
      logf "[pin] $name OK (pin $expect_short or later); HEAD=$head_full"
      echo "OK:${head_full}:${dirty}"
      return 0
    fi
    logf "[pin] $name MISMATCH HEAD=$head_full expect_baseline=$expect"
    echo "MISMATCH:${head_full}:${dirty}"
    return 0
  fi
  if [[ "$head_short" == "$expect_short" ]] || [[ "$head_full" == "$expect"* ]]; then
    logf "[pin] $name OK HEAD=$head_full == pin $expect_short"
    echo "OK:${head_full}:${dirty}"
    return 0
  fi
  logf "[pin] $name MISMATCH HEAD=$head_full expect=$expect"
  echo "MISMATCH:${head_full}:${dirty}"
  return 0
}

json_field() {
  local raw="${1:-}"
  [[ -n "$raw" ]] || { echo ""; return 0; }
  python3 -c "import json,sys; d=json.loads(sys.argv[1]); print(d.get(sys.argv[2],''))" "$raw" "$2" 2>/dev/null || echo ""
}

# ---------- 0) pins ----------
log "=== GameStream 三仓固化验收 ==="
log "time=$(ts_utc8)"
log "mode=$MODE (strict|report-only)"
log "root=$ROOT"
log "result_file=$RESULT_FILE"
log "doris=$DORIS_URL"
log "guard_url=$GUARD_URL"
log ""

read_pins_from_lock
log ""
log "=== 0) Pin verification (baselines vs actual full HEAD) ==="
GS_PIN_R=$(verify_repo_pin GameStream "$ROOT" "$PIN_GS")
SG_PIN_R=$(verify_repo_pin SQLGuard "$SQLGUARD_REPO" "$PIN_SG")
DP_PIN_R=$(verify_repo_pin DataPilot "$DATAPILOT_REPO" "$PIN_DP")
# re-echo pin detail lines already in result via logf; also mirror tokens
log "pin_GameStream=$GS_PIN_R"
log "pin_SQLGuard=$SG_PIN_R"
log "pin_DataPilot=$DP_PIN_R"
log ""

# ---------- a) G7 closed loop ----------
log "=== a) G7 strict closed-loop (Doris backend) ==="
G7_SCRIPT="$ROOT/scripts/g7_closed_loop.sh"
G7_OUT="$ROOT/docs/g7-closed-loop-result.txt"
if [[ ! -f "$G7_SCRIPT" ]]; then
  set_status g7_closed_loop SKIP "scripts/g7_closed_loop.sh missing" n/a
else
  # CRLF-safe copy
  cp "$G7_SCRIPT" /tmp/suite_g7.sh
  sed -i 's/\r$//' /tmp/suite_g7.sh
  set +e
  GAMESTREAM_ROOT="$ROOT" MODE=fixture START_GUARD="${START_GUARD:-1}" \
    SQLGUARD_REPO="$SQLGUARD_REPO" DATAPILOT_REPO="$DATAPILOT_REPO" \
    DORIS_URL="$DORIS_URL" GUARD_URL="$GUARD_URL" \
    POLICY="$POLICY" CATALOG="$CATALOG" \
    RESULT_FILE="$G7_OUT" \
    bash /tmp/suite_g7.sh >>"$RESULT_FILE" 2>&1
  G7_RC=$?
  set -e
  # Parse real outcomes from G7 result (never invent)
  backend="unknown"
  rowcount=""
  hive_dp=""
  if [[ -f "$G7_OUT" ]]; then
    if grep -q 'http POST /v1/execute' "$G7_OUT" 2>/dev/null || grep -q 'gate_mode: http' "$G7_OUT" 2>/dev/null; then
      backend="doris"
    elif grep -q 'docker mysql' "$G7_OUT" 2>/dev/null; then
      backend="doris"
    elif grep -q 'mock' "$G7_OUT" 2>/dev/null; then
      backend="mock"
    else
      backend="doris_or_python"
    fi
    rowcount=$(grep -oE '"rowcount"[[:space:]]*:[[:space:]]*[0-9]+' "$G7_OUT" | head -1 | grep -oE '[0-9]+' || true)
    if [[ -z "$rowcount" ]]; then
      rowcount=$(grep -oE 'rowcount[=:][[:space:]]*[0-9]+' "$G7_OUT" | head -1 | grep -oE '[0-9]+' || true)
    fi
    hive_dp=$(grep -E 'cross_db|hive\.ads_dau_di' -A6 "$G7_OUT" | grep -oE 'datapilot=[A-Z]+' | head -1 | cut -d= -f2 || true)
    dau_ads=$(grep -E '^ads_dau_di rows=' "$G7_OUT" | head -1 || true)
    # also copy key lines
    log "--- g7 result excerpt ---"
    grep -E '^(ads_|datapilot=|executed=|rowcount|=== |========|sqlguard|DONE|ERROR|gate_mode|rows_from)' "$G7_OUT" 2>/dev/null | tee -a "$RESULT_FILE" || true
  fi
  if [[ $G7_RC -eq 0 ]]; then
    set_status g7_closed_loop PASS "exit=0 rowcount=${rowcount:-?} hive_gate=${hive_dp:-?} ${dau_ads:-} (see $G7_OUT)" "$backend"
  else
    # ADS empty is a real FAIL/SKIP reason
    if [[ -f "$G7_OUT" ]] && grep -q 'ADS empty' "$G7_OUT"; then
      set_status g7_closed_loop FAIL "ADS empty — seed via e2e_g2.sh; exit=$G7_RC" "$backend"
    else
      set_status g7_closed_loop FAIL "exit=$G7_RC (see $G7_OUT)" "$backend"
    fi
  fi
fi
log ""

# ---------- b) cross-db hive BLOCK (SQLGuard 1.1.2) ----------
log "=== b) Cross-db hive.ads_dau_di BLOCK (SQLGuard ${PIN_SG_VER}) ==="
HZ=$(curl -sf "${GUARD_URL}/healthz" 2>/dev/null || echo "")
if [[ -z "$HZ" ]]; then
  # try start briefly via python if available
  if [[ -d "$SQLGUARD_REPO/src" ]]; then
    export PYTHONPATH="${SQLGUARD_REPO}/src${PYTHONPATH:+:$PYTHONPATH}"
    if command -v sql-write-gate >/dev/null 2>&1 || python3 -c "from write_gate.api import run_serve_cli" 2>/dev/null; then
      log "[b] starting temporary SQLGuard serve for cross-db check..."
      if command -v sql-write-gate >/dev/null 2>&1; then
        nohup sql-write-gate serve --host 127.0.0.1 --port 8787 \
          --policy "$POLICY" --catalog "$CATALOG" --database "$DORIS_URL" \
          >/tmp/suite_sqlguard.log 2>&1 &
        SUITE_GUARD_PID=$!
      else
        nohup python3 -c "
from write_gate.api import run_serve_cli
raise SystemExit(run_serve_cli('127.0.0.1', 8787, defaults={
  'database': '$DORIS_URL', 'catalog': '$CATALOG', 'policy': '$POLICY', 'agent': 'suite-acc'}))
" >/tmp/suite_sqlguard.log 2>&1 &
        SUITE_GUARD_PID=$!
      fi
      for _ in $(seq 1 30); do
        HZ=$(curl -sf "${GUARD_URL}/healthz" 2>/dev/null || echo "")
        [[ -n "$HZ" ]] && break
        sleep 0.3
      done
    fi
  fi
fi

if [[ -z "$HZ" ]]; then
  # fallback: parse from G7 result if present
  if [[ -f "$G7_OUT" ]] && grep -q 'hive.ads_dau_di' "$G7_OUT" && grep -A5 'cross_db' "$G7_OUT" | grep -q 'datapilot=BLOCK'; then
    log "[b] from G7 transcript: hive.ads_dau_di datapilot=BLOCK"
    set_status sqlguard_cross_db_hive PASS "hive.ads_dau_di BLOCK (from g7 transcript); healthz unavailable now" "doris/g7-transcript"
  else
    set_status sqlguard_cross_db_hive SKIP "SQLGuard healthz unavailable and no G7 hive BLOCK evidence" n/a
  fi
else
  log "healthz=$HZ"
  ver=$(json_field "$HZ" version)
  prod=$(json_field "$HZ" product)
  log "product=$prod version=$ver (expect SQLGuard $PIN_SG_VER)"
  BODY=$(python3 -c "import json; print(json.dumps({'sql':\"SELECT dt, server_id, dau, metric_id FROM hive.ads_dau_di WHERE metric_id = 'ads_dau_di'\",'actor':'suite-acc','model_id':'fixture','prompt_summary':'cross_db_hive','database':'$DORIS_URL'},ensure_ascii=False))")
  RESP=$(curl -sf -X POST "${GUARD_URL}/v1/check" -H 'Content-Type: application/json' -d "$BODY" 2>/dev/null || echo "")
  log "check_response=$RESP"
  dp=$(json_field "$RESP" datapilot)
  action=$(json_field "$RESP" action)
  rule=$(json_field "$RESP" rule_id)
  if [[ "$ver" != "$PIN_SG_VER" ]]; then
    set_status sqlguard_cross_db_hive FAIL "healthz version=$ver want=$PIN_SG_VER; datapilot=$dp action=$action rule=$rule" "sqlguard-http"
  elif [[ "$dp" == "BLOCK" || "$action" == "BLOCK" ]]; then
    set_status sqlguard_cross_db_hive PASS "version=$ver datapilot=$dp action=$action rule_id=$rule" "sqlguard-http"
  else
    set_status sqlguard_cross_db_hive FAIL "expected BLOCK got datapilot=$dp action=$action rule=$rule" "sqlguard-http"
  fi
fi
# cleanup temp guard if we started it (don't kill if G7 owns it)
if [[ -n "${SUITE_GUARD_PID:-}" ]]; then
  kill "$SUITE_GUARD_PID" 2>/dev/null || true
fi
log ""

# ---------- c) DataPilot no-mock-fallback / suite_p0 ----------
log "=== c) DataPilot offline P0 (no-mock-fallback @ ${PIN_DP}) ==="
if [[ ! -d "$DATAPILOT_REPO/.git" ]]; then
  set_status datapilot_offline_p0 SKIP "DataPilot clone missing at $DATAPILOT_REPO" n/a
else
  DP_HEAD=$(git -C "$DATAPILOT_REPO" rev-parse --short HEAD 2>/dev/null || echo unknown)
  log "DataPilot HEAD=$DP_HEAD (pin $PIN_DP)"
  # Prefer pinned checkout only if dirty-free and user allows; do NOT force checkout by default
  pushd "$DATAPILOT_REPO" >/dev/null
  # install editable quietly into current python if needed
  set +e
  python3 -c "import datapilot" 2>/dev/null
  IMP=$?
  set -e
  if [[ $IMP -ne 0 ]]; then
    log "[c] pip install -e .[dev] ..."
    pip3 install -e ".[dev]" -q 2>>"$RESULT_FILE" || pip install -e ".[dev]" -q 2>>"$RESULT_FILE" || true
  fi
  # Detect suite_p0 marker
  HAS_SUITE_P0=0
  if grep -q 'suite_p0' pyproject.toml 2>/dev/null || grep -rq 'suite_p0' tests/ 2>/dev/null; then
    HAS_SUITE_P0=1
  fi
  DP_LOG=/tmp/suite_datapilot_p0.txt
  set +e
  if [[ "$HAS_SUITE_P0" == "1" ]] && python3 -m pytest -m suite_p0 -q --collect-only >/dev/null 2>&1; then
    log "[c] entry: pytest -m suite_p0 -q"
    python3 -m pytest -m suite_p0 -q >"$DP_LOG" 2>&1
    DP_RC=$?
    ENTRY="pytest -m suite_p0 -q"
  else
    log "[c] entry: pytest tests/test_gate_no_mock_fallback.py tests/test_time_intent_predicates.py tests/test_sqlguard_http_contract.py -q"
    python3 -m pytest \
      tests/test_gate_no_mock_fallback.py \
      tests/test_time_intent_predicates.py \
      tests/test_sqlguard_http_contract.py \
      -q >"$DP_LOG" 2>&1
    DP_RC=$?
    ENTRY="pytest three P0 files -q"
  fi
  set -e
  cat "$DP_LOG" | tee -a "$RESULT_FILE"
  # Parse pytest summary line honestly
  SUMMARY_LINE=$(tail -n 5 "$DP_LOG" | tr '\n' ' ')
  if [[ $DP_RC -eq 0 ]]; then
    set_status datapilot_offline_p0 PASS "HEAD=$DP_HEAD entry=$ENTRY :: $SUMMARY_LINE" "offline-pytest"
  else
    set_status datapilot_offline_p0 FAIL "HEAD=$DP_HEAD entry=$ENTRY exit=$DP_RC :: $SUMMARY_LINE" "offline-pytest"
  fi
  popd >/dev/null

  # optional Doris true联调 — never call SKIP a PASS
  log "--- c2) optional DataPilot Doris G7 (SKIP if unreachable) ---"
  DORIS_LOG=/tmp/suite_datapilot_doris.txt
  set +e
  pushd "$DATAPILOT_REPO" >/dev/null
  DATAPILOT_QUERY_BACKEND=doris \
  DATAPILOT_DORIS_URL="$DORIS_URL" \
  DATAPILOT_GUARD_MODE=write_gate \
  DATAPILOT_LLM_MODE=mock \
    python3 -m pytest tests/test_doris_g7.py -q >"$DORIS_LOG" 2>&1
  DORIS_RC=$?
  popd >/dev/null
  set -e
  cat "$DORIS_LOG" | tee -a "$RESULT_FILE"
  DORIS_SUM=$(tail -n 5 "$DORIS_LOG" | tr '\n' ' ')
  # FAIL before SKIP: non-zero exit with failures is FAIL even if some tests skipped
  if [[ $DORIS_RC -ne 0 ]] && grep -qiE 'failed|ERROR|FAILURES' "$DORIS_LOG"; then
    set_status datapilot_doris_g7 FAIL "exit=$DORIS_RC :: $DORIS_SUM" "doris-attempted"
  elif [[ $DORIS_RC -eq 0 ]]; then
    set_status datapilot_doris_g7 PASS "$DORIS_SUM" "doris"
  elif grep -qiE 'skipped|SKIP' "$DORIS_LOG"; then
    set_status datapilot_doris_g7 SKIP "optional; $DORIS_SUM" "n/a-or-skip"
  else
    set_status datapilot_doris_g7 FAIL "exit=$DORIS_RC :: $DORIS_SUM" "doris-attempted"
  fi
fi
log ""

# ---------- d) SQLGuard unit tests / CI (pin 7dc85dd / v1.1.2) ----------
log "=== d) SQLGuard unit tests / CI summary (expect @$PIN_SG / $PIN_SG_VER) ==="
log "routes contract: /v1/check|/v1/block|/v1/execute"
if [[ ! -d "$SQLGUARD_REPO/.git" ]]; then
  set_status sqlguard_unit SKIP "sql-write-gate clone missing at $SQLGUARD_REPO" n/a
else
  SG_HEAD=$(git -C "$SQLGUARD_REPO" rev-parse --short HEAD 2>/dev/null || echo unknown)
  log "SQLGuard HEAD=$SG_HEAD (pin $PIN_SG / $PIN_SG_VER)"
  # Prefer pinned tree when clean and not already there (do not force dirty reset)
  if [[ "$SG_HEAD" != "$(echo $PIN_SG | cut -c1-7)" ]]; then
    log "[d] NOTE: HEAD=$SG_HEAD != pin $PIN_SG — tests run on current HEAD; re-checkout recommended"
  fi
  SG_LOG=/tmp/suite_sqlguard_pytest.txt
  pushd "$SQLGUARD_REPO" >/dev/null
  set +e
  if [[ -f Makefile ]] && ! command -v make >/dev/null 2>&1; then
    log "[d] Makefile present but make not installed — fallback pytest"
  fi
  if [[ -f Makefile ]] && command -v make >/dev/null 2>&1; then
    log "[d] entry: make test (PYTHON from PATH/g7-venv)"
    if [[ -x /home/tangy/g7-venv/bin/python3 ]]; then
      make test PYTHON=/home/tangy/g7-venv/bin/python3 >"$SG_LOG" 2>&1
    else
      make test >"$SG_LOG" 2>&1
    fi
    SG_RC=$?
    ENTRY="make test"
  elif [[ -x .venv/bin/pytest ]]; then
    log "[d] entry: .venv/bin/pytest -q"
    .venv/bin/pytest -q >"$SG_LOG" 2>&1
    SG_RC=$?
    ENTRY="pytest -q (.venv)"
  elif [[ -x /home/tangy/g7-venv/bin/pytest ]]; then
    log "[d] entry: g7-venv pytest -q"
    /home/tangy/g7-venv/bin/pytest -q >"$SG_LOG" 2>&1
    SG_RC=$?
    ENTRY="pytest -q (g7-venv)"
  else
    log "[d] entry: python3 -m pytest -q"
    python3 -m pytest -q >"$SG_LOG" 2>&1
    SG_RC=$?
    ENTRY="python3 -m pytest -q"
  fi
  set -e
  # keep report bounded but real (live PG/MySQL SKIP is OK)
  tail -n 50 "$SG_LOG" | tee -a "$RESULT_FILE"
  SG_SUM=$(tail -n 5 "$SG_LOG" | tr '\n' ' ')
  # optional focused v1.1.x file
  if [[ -f tests/test_v110.py ]]; then
    log "--- optional pytest -q tests/test_v110.py ---"
    set +e
    if [[ -x .venv/bin/pytest ]]; then
      .venv/bin/pytest -q tests/test_v110.py 2>&1 | tee -a "$RESULT_FILE"
    else
      python3 -m pytest -q tests/test_v110.py 2>&1 | tee -a "$RESULT_FILE"
    fi
    set -e
  fi
  if [[ $SG_RC -eq 0 ]]; then
    set_status sqlguard_unit PASS "HEAD=$SG_HEAD entry=$ENTRY :: $SG_SUM" "pytest-local"
  else
    set_status sqlguard_unit FAIL "HEAD=$SG_HEAD entry=$ENTRY exit=$SG_RC :: $SG_SUM" "pytest-local"
  fi
  popd >/dev/null

  # optional gh run list (real only)
  CI_NOTE="gh=n/a"
  if command -v gh >/dev/null 2>&1; then
    log "--- gh run list -R tangyf07/sql-write-gate --branch main --limit 3 ---"
    set +e
    GH_OUT=$(gh run list -R tangyf07/sql-write-gate --branch main --limit 3 2>&1)
    echo "$GH_OUT" | tee -a "$RESULT_FILE"
    set -e
    # first line conclusion for latest main
    CI_NOTE=$(echo "$GH_OUT" | head -1 | tr '\t' ' ' | cut -c1-120)
  else
    log "[d] gh not available — skip CI list"
  fi
  # refresh status detail with CI note (keep PASS/FAIL from local run)
  st="${CHECK_STATUS[sqlguard_unit]}"
  be="${CHECK_BACKEND[sqlguard_unit]}"
  de="${CHECK_DETAIL[sqlguard_unit]}"
  set_status sqlguard_unit "$st" "$de | CI: $CI_NOTE" "$be"
fi
log ""

# ---------- summary table + strict gate ----------
log "=== SUMMARY (real statuses only) ==="
log "mode=$MODE"
log "pin_baselines: GameStream=$PIN_GS SQLGuard=$PIN_SG@$PIN_SG_VER DataPilot=$PIN_DP"
log "pin_results(actual): GS=$GS_PIN_R SG=$SG_PIN_R DP=$DP_PIN_R"
for id in g7_closed_loop sqlguard_cross_db_hive datapilot_offline_p0 datapilot_doris_g7 sqlguard_unit; do
  st="${CHECK_STATUS[$id]:-SKIP}"
  be="${CHECK_BACKEND[$id]:-n/a}"
  de="${CHECK_DETAIL[$id]:-not-run}"
  req="${CHECK_REQUIRED[$id]:-0}"
  log "  $id | $st | required=$req | backend=$be | $de"
done

# Gate: evaluate FAIL before SKIP (never treat failed+skipped as SKIP-only).
GATE_FAIL=0
GATE_SKIP=0
GATE_MISMATCH=0
GATE_NOTES=()

for token_name in GS_PIN_R SG_PIN_R DP_PIN_R; do
  tok="${!token_name}"
  case "$tok" in
    MISMATCH:*) GATE_MISMATCH=1; GATE_NOTES+=("pin_$token_name=$tok") ;;
    MISSING:*)  GATE_SKIP=1; GATE_NOTES+=("pin_$token_name=$tok") ;;
  esac
done

for id in g7_closed_loop sqlguard_cross_db_hive datapilot_offline_p0 datapilot_doris_g7 sqlguard_unit; do
  st="${CHECK_STATUS[$id]:-SKIP}"
  req="${CHECK_REQUIRED[$id]:-0}"
  [[ "$req" == "1" ]] || continue
  case "$st" in
    FAIL) GATE_FAIL=1; GATE_NOTES+=("$id=FAIL") ;;
    SKIP|INCOMPLETE) GATE_SKIP=1; GATE_NOTES+=("$id=$st") ;;
  esac
done

OVERALL="PASS"
EXIT_RC=0
if [[ $GATE_FAIL -eq 1 || $GATE_MISMATCH -eq 1 ]]; then
  OVERALL="FAIL"
  EXIT_RC=1
elif [[ $GATE_SKIP -eq 1 ]]; then
  OVERALL="INCOMPLETE"
  EXIT_RC=2
fi

GATE_NOTES_JOINED="${GATE_NOTES[*]}"
log "gate: overall=$OVERALL exit_rc=$EXIT_RC fail=$GATE_FAIL mismatch=$GATE_MISMATCH skip=$GATE_SKIP notes=${GATE_NOTES_JOINED:-none}"
log "DONE $(ts_utc8)"
log "result_file=$RESULT_FILE"

if [[ "$MODE" == "report-only" || "$MODE" == "report_only" || "$MODE" == "REPORT_ONLY" ]]; then
  log "mode=report-only → forcing exit 0 (report written; overall was $OVERALL)"
  echo "OK: wrote $RESULT_FILE (report-only; overall=$OVERALL)"
  exit 0
fi

if [[ $EXIT_RC -eq 0 ]]; then
  echo "OK: wrote $RESULT_FILE (overall=$OVERALL)"
  exit 0
fi
echo "FAIL: wrote $RESULT_FILE (overall=$OVERALL exit=$EXIT_RC)" >&2
exit "$EXIT_RC"
