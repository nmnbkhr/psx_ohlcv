#!/bin/bash
# PSX OHLCV Daily Sync
# Runs async sync at 18:30 PKT (market closes 15:30, data available by ~17:00)
# Cron: 30 13 * * 1-5 ~/psx_ohlcv/scripts/daily_sync.sh

set -euo pipefail

PROJECT_DIR="$HOME/psx_ohlcv"
CONDA_BIN="/opt/miniconda/bin/conda"
CONDA_ENV="handwriting"
LOG_DIR="/mnt/e/psxdata/logs"
DB_PATH="/mnt/e/psxdata/psx.sqlite"

mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/sync_$(date +%Y%m%d).log"

echo "=== PSX Daily Sync: $(date) ===" >> "$LOG_FILE"

# Run async EOD sync for all symbols
$CONDA_BIN run -n "$CONDA_ENV" \
    python -m psx_ohlcv.cli --db "$DB_PATH" sync --all --async >> "$LOG_FILE" 2>&1

echo "=== Completed: $(date) ===" >> "$LOG_FILE"

# Run maintenance weekly (on Fridays)
if [ "$(date +%u)" = "5" ]; then
    echo "=== Weekly Maintenance ===" >> "$LOG_FILE"
    $CONDA_BIN run -n "$CONDA_ENV" \
        python -m psx_ohlcv.db.maintenance --analyze --stats >> "$LOG_FILE" 2>&1
    echo "=== Maintenance Complete: $(date) ===" >> "$LOG_FILE"
fi
