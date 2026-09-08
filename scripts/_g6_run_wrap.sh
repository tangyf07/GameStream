#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cp "$ROOT/scripts/g6_bench.sh" /tmp/g6.sh
sed -i 's/\r$//' /tmp/g6.sh
# also ensure helpers have no CRLF
sed -i 's/\r$//' "$ROOT/scripts/g6_sample_metrics.py" "$ROOT/scripts/g6_gen_and_produce.py" "$ROOT/flink/sql/g6_bench.sql"
export GAMESTREAM_ROOT="$ROOT"
export TIER="${TIER:-light}"
export POLL_SEC="${POLL_SEC:-40}"
export DRAIN_SEC="${DRAIN_SEC:-120}"
bash /tmp/g6.sh 2>&1 | tee "$ROOT/docs/g6-run-console.txt"
exit ${PIPESTATUS[0]}
