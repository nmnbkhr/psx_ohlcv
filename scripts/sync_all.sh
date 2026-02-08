#!/bin/bash
# Master sync — runs all data sync scripts in sequence
# Cron: not scheduled directly; run manually or use individual scripts

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$HOME/psx_ohlcv"
CONDA_BIN="/opt/miniconda/bin/conda"
CONDA_ENV="handwriting"
LOG_DIR="/mnt/e/psxdata/logs"
DB_PATH="/mnt/e/psxdata/psx.sqlite"

mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/sync_all_$(date +%Y%m%d).log"

echo "======================================" >> "$LOG_FILE"
echo "=== Full Sync: $(date) ===" >> "$LOG_FILE"
echo "======================================" >> "$LOG_FILE"

# 1. EOD sync
echo "--- Step 1: EOD sync ---" >> "$LOG_FILE"
bash "$SCRIPT_DIR/daily_sync.sh" >> "$LOG_FILE" 2>&1 || echo "EOD sync had errors" >> "$LOG_FILE"

# 2. Rates (KIBOR, KONIA, PKRV)
echo "--- Step 2: Rates sync ---" >> "$LOG_FILE"
bash "$SCRIPT_DIR/sync_rates.sh" >> "$LOG_FILE" 2>&1 || echo "Rates sync had errors" >> "$LOG_FILE"

# 3. FX (SBP + kerb)
echo "--- Step 3: FX sync ---" >> "$LOG_FILE"
bash "$SCRIPT_DIR/sync_fx.sh" >> "$LOG_FILE" 2>&1 || echo "FX sync had errors" >> "$LOG_FILE"

# 4. Treasury (T-Bill + PIB + GIS)
echo "--- Step 4: Treasury sync ---" >> "$LOG_FILE"
bash "$SCRIPT_DIR/sync_treasury.sh" >> "$LOG_FILE" 2>&1 || echo "Treasury sync had errors" >> "$LOG_FILE"

# 5. ETF
echo "--- Step 5: ETF sync ---" >> "$LOG_FILE"
bash "$SCRIPT_DIR/sync_etf.sh" >> "$LOG_FILE" 2>&1 || echo "ETF sync had errors" >> "$LOG_FILE"

# 6. CLI sync-all (runs remaining scrapers)
echo "--- Step 6: CLI sync-all ---" >> "$LOG_FILE"
cd "$PROJECT_DIR"
$CONDA_BIN run -n "$CONDA_ENV" \
    python -m psx_ohlcv.cli --db "$DB_PATH" sync-all >> "$LOG_FILE" 2>&1 || echo "CLI sync-all had errors" >> "$LOG_FILE"

echo "======================================" >> "$LOG_FILE"
echo "=== Full Sync Complete: $(date) ===" >> "$LOG_FILE"
echo "======================================" >> "$LOG_FILE"

# Show status
$CONDA_BIN run -n "$CONDA_ENV" \
    python -m psx_ohlcv.cli --db "$DB_PATH" status >> "$LOG_FILE" 2>&1 || true
