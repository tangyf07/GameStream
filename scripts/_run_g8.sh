#!/bin/bash
set -euo pipefail
SRC=/mnt/c/Users/tangy/source/repos/GameStream
cp "$SRC/scripts/g8_continuous_mainline.sh" /tmp/g8.sh
sed -i 's/\r$//' /tmp/g8.sh
export GAMESTREAM_ROOT="$SRC"
exec bash /tmp/g8.sh
