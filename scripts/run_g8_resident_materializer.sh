#!/usr/bin/env bash
# Standalone resident process for G8 Doris ADS materializer.
# Prefer this on ~7.6Gi WSL (lighter than an extra compose JVM/Python image if deps already local).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export G8_MAT_BOOTSTRAP="${G8_MAT_BOOTSTRAP:-localhost:19092}"
export G8_MAT_GROUP="${G8_MAT_GROUP:-gamestream-g8-doris-materializer}"
export G8_MAT_TOPIC_DAU="${G8_MAT_TOPIC_DAU:-gamestream.g8.ads_dau}"
export G8_MAT_TOPIC_PAY="${G8_MAT_TOPIC_PAY:-gamestream.g8.ads_pay_rate}"
export G8_MAT_DORIS_HOST="${G8_MAT_DORIS_HOST:-127.0.0.1}"
export G8_MAT_DORIS_PORT="${G8_MAT_DORIS_PORT:-9030}"
export G8_MAT_DORIS_USER="${G8_MAT_DORIS_USER:-root}"
export G8_MAT_DORIS_PASSWORD="${G8_MAT_DORIS_PASSWORD:-}"
export G8_MAT_DORIS_DB="${G8_MAT_DORIS_DB:-ads}"

PID_FILE="${G8_MAT_PID_FILE:-/tmp/g8_resident_materializer.pid}"
LOG_FILE="${G8_MAT_LOG_FILE:-/tmp/g8_resident_materializer.log}"

PY="${G8_MAT_PYTHON:-python3}"
if ! "$PY" -c 'import kafka, pymysql' 2>/dev/null; then
  echo "[g8-mat] installing kafka-python + PyMySQL into current interpreter" >&2
  "$PY" -m pip install -q 'kafka-python>=2.0.2' 'PyMySQL>=1.1.0'
fi

cmd="${1:-run}"
case "$cmd" in
  run)
    echo "[g8-mat] foreground bootstrap=$G8_MAT_BOOTSTRAP group=$G8_MAT_GROUP" >&2
    exec "$PY" "$ROOT/scripts/g8_resident_materializer.py"
    ;;
  start)
    if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
      echo "[g8-mat] already running pid=$(cat "$PID_FILE")" >&2
      exit 0
    fi
    nohup "$PY" "$ROOT/scripts/g8_resident_materializer.py" >>"$LOG_FILE" 2>&1 &
    echo $! >"$PID_FILE"
    echo "[g8-mat] started pid=$(cat "$PID_FILE") log=$LOG_FILE" >&2
    ;;
  stop)
    if [[ -f "$PID_FILE" ]]; then
      pid=$(cat "$PID_FILE")
      if kill -0 "$pid" 2>/dev/null; then
        kill "$pid" || true
        for i in $(seq 1 20); do
          kill -0 "$pid" 2>/dev/null || break
          sleep 0.25
        done
        kill -9 "$pid" 2>/dev/null || true
        echo "[g8-mat] stopped pid=$pid" >&2
      else
        echo "[g8-mat] stale pid file ($pid)" >&2
      fi
      rm -f "$PID_FILE"
    else
      echo "[g8-mat] not running (no pid file)" >&2
    fi
    ;;
  status)
    if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
      echo "running pid=$(cat "$PID_FILE")"
      exit 0
    fi
    echo "stopped"
    exit 1
    ;;
  smoke)
    exec "$PY" "$ROOT/scripts/g8_resident_materializer.py" --once-smoke
    ;;
  *)
    echo "usage: $0 {run|start|stop|status|smoke}" >&2
    exit 2
    ;;
esac
