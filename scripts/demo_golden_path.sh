#!/usr/bin/env bash
# Thin demo wrapper: Golden Path = existing G2 e2e (ads_dau_di).
# No new pipeline. Prefer: copy to /tmp + strip CRLF on WSL.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ROOT="${GAMESTREAM_ROOT:-$ROOT}"
if [[ ! -f "$ROOT/docker-compose.yml" ]]; then
  echo "ERROR: cannot find GameStream repo root (set GAMESTREAM_ROOT or run from repo scripts/)." >&2
  exit 1
fi

E2E="$ROOT/scripts/e2e_g2.sh"
if [[ ! -f "$E2E" ]]; then
  echo "missing: $E2E" >&2
  exit 1
fi

TMP=/tmp/e2e_g2_demo.sh
cp "$E2E" "$TMP"
sed -i 's/\r$//' "$TMP"
echo "[demo] Golden Path → scripts/e2e_g2.sh (metric ads_dau_di)"
GAMESTREAM_ROOT="$ROOT" bash "$TMP" "$@"
