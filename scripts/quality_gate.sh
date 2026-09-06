#!/usr/bin/env bash
# Fail-stop quality gate: schema + metrics presence + pipeline smoke
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "=== GameStream quality gate ==="

# Prefer already-active venv, else .venv, else venv
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

echo "--- AST check (pipeline) ---"
python -c "import ast; ast.parse(open('pipeline/local_runner.py').read()); ast.parse(open('pipeline/kafka_io.py').read()); print('AST-ok')"

echo "--- pytest ---"
python -m pytest tests/ -v --tb=short
echo "=== quality gate PASSED ==="
