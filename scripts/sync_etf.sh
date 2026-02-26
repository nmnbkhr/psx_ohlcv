#!/bin/bash
# Sync ETF data from MUFAP/PSX
# Cron: 0 14 * * 1-5 ~/pakfindata/scripts/sync_etf.sh

set -euo pipefail

PROJECT_DIR="$HOME/pakfindata"
CONDA_BIN="/opt/miniconda/bin/conda"
CONDA_ENV="handwriting"
LOG_DIR="/mnt/e/psxdata/logs"
DB_PATH="/mnt/e/psxdata/psx.sqlite"

mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/sync_etf_$(date +%Y%m%d).log"

echo "=== ETF Sync: $(date) ===" >> "$LOG_FILE"

cd "$PROJECT_DIR"
$CONDA_BIN run -n "$CONDA_ENV" \
    python -m pakfindata.cli --db "$DB_PATH" etf sync >> "$LOG_FILE" 2>&1 || true

echo "=== Completed: $(date) ===" >> "$LOG_FILE"
