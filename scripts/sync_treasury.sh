#!/bin/bash
# Sync T-Bill + PIB + GIS auctions from SBP
# Cron: 0 16 * * 5 ~/psx_ohlcv/scripts/sync_treasury.sh (weekly Friday)

set -euo pipefail

PROJECT_DIR="$HOME/psx_ohlcv"
CONDA_BIN="/opt/miniconda/bin/conda"
CONDA_ENV="handwriting"
LOG_DIR="/mnt/e/psxdata/logs"
DB_PATH="/mnt/e/psxdata/psx.sqlite"

mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/sync_treasury_$(date +%Y%m%d).log"

echo "=== Treasury Sync: $(date) ===" >> "$LOG_FILE"

cd "$PROJECT_DIR"
$CONDA_BIN run -n "$CONDA_ENV" \
    python -m psx_ohlcv.cli --db "$DB_PATH" treasury tbill-sync >> "$LOG_FILE" 2>&1 || true

$CONDA_BIN run -n "$CONDA_ENV" \
    python -m psx_ohlcv.cli --db "$DB_PATH" treasury pib-sync >> "$LOG_FILE" 2>&1 || true

$CONDA_BIN run -n "$CONDA_ENV" \
    python -m psx_ohlcv.cli --db "$DB_PATH" treasury gis-sync >> "$LOG_FILE" 2>&1 || true

echo "=== Completed: $(date) ===" >> "$LOG_FILE"
