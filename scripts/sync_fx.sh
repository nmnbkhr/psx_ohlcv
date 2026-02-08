#!/bin/bash
# Sync FX rates: SBP interbank + forex.pk kerb
# Cron: 0 13 * * 1-5 ~/psx_ohlcv/scripts/sync_fx.sh

set -euo pipefail

PROJECT_DIR="$HOME/psx_ohlcv"
CONDA_BIN="/opt/miniconda/bin/conda"
CONDA_ENV="handwriting"
LOG_DIR="/mnt/e/psxdata/logs"
DB_PATH="/mnt/e/psxdata/psx.sqlite"

mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/sync_fx_$(date +%Y%m%d).log"

echo "=== FX Sync: $(date) ===" >> "$LOG_FILE"

cd "$PROJECT_DIR"
$CONDA_BIN run -n "$CONDA_ENV" \
    python -m psx_ohlcv.cli --db "$DB_PATH" fx-rates sbp-sync >> "$LOG_FILE" 2>&1 || true

$CONDA_BIN run -n "$CONDA_ENV" \
    python -m psx_ohlcv.cli --db "$DB_PATH" fx-rates kerb-sync >> "$LOG_FILE" 2>&1 || true

echo "=== Completed: $(date) ===" >> "$LOG_FILE"
