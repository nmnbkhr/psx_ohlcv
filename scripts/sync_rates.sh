#!/bin/bash
# Sync KIBOR, KONIA, PKRV, and SBP policy rate
# Cron: 30 12 * * 1-5 ~/pakfindata/scripts/sync_rates.sh

set -euo pipefail

PROJECT_DIR="$HOME/pakfindata"
CONDA_BIN="/opt/miniconda/bin/conda"
CONDA_ENV="handwriting"
LOG_DIR="/mnt/e/psxdata/logs"
DB_PATH="/mnt/e/psxdata/psx.sqlite"

mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/sync_rates_$(date +%Y%m%d).log"

echo "=== Rate Sync: $(date) ===" >> "$LOG_FILE"

cd "$PROJECT_DIR"
$CONDA_BIN run -n "$CONDA_ENV" \
    python -m pakfindata.cli --db "$DB_PATH" rates yield-curve >> "$LOG_FILE" 2>&1 || true

$CONDA_BIN run -n "$CONDA_ENV" \
    python -m pakfindata.cli --db "$DB_PATH" rates kibor >> "$LOG_FILE" 2>&1 || true

$CONDA_BIN run -n "$CONDA_ENV" \
    python -m pakfindata.cli --db "$DB_PATH" rates konia >> "$LOG_FILE" 2>&1 || true

echo "=== Completed: $(date) ===" >> "$LOG_FILE"
