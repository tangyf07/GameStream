#!/usr/bin/env bash
# One-click: deps, quality gate (fail-stop), demo run
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PLAYERS="${PLAYERS:-2000}"
EVENTS="${EVENTS:-20000}"
DAYS="${DAYS:-7}"
SEED="${SEED:-42}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --players) PLAYERS="$2"; shift 2 ;;
    --events)  EVENTS="$2"; shift 2 ;;
    --days)    DAYS="$2"; shift 2 ;;
    --seed)    SEED="$2"; shift 2 ;;
    --help|-h)
      echo "Usage: $0 [--players N] [--events N] [--days N] [--seed N]"
      exit 0
      ;;
    *)
      echo "Unknown arg: $1" >&2
      exit 1
      ;;
  esac
done

echo "=== GameStream run_all ==="

if [[ -z "${VIRTUAL_ENV:-}" ]]; then
  if [[ -d .venv ]]; then
    # shellcheck disable=SC1091
    source .venv/bin/activate
  elif [[ -d venv ]]; then
    # shellcheck disable=SC1091
    source venv/bin/activate
  else
    python3 -m venv .venv
    # shellcheck disable=SC1091
    source .venv/bin/activate
  fi
fi

pip install -q -r requirements.txt pytest pyyaml duckdb

bash scripts/quality_gate.sh

echo "--- demo: players=$PLAYERS events=$EVENTS days=$DAYS seed=$SEED ---"
python pipeline/local_runner.py --players "$PLAYERS" --events "$EVENTS" --days "$DAYS" --seed "$SEED"

echo "=== run_all DONE ==="
echo "DuckDB: data/gamestream.duckdb"
echo "ADS parquet: data/ads/"
