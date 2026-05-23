#!/usr/bin/env bash
# Pre-shutdown ritual — prevent psx.sqlite corruption like 2026-05-09.
#
# What this does:
#   1. Stops Streamlit (releases its cached singleton SQLite connection).
#   2. Stops fusion_service (any long-lived writer holding the WAL).
#   3. Runs PRAGMA wal_checkpoint(FULL) + PRAGMA optimize on psx.sqlite.
#   4. Reports the WAL file size — should be <1 KB after a clean checkpoint.
#
# Run this BEFORE: system sleep, machine shutdown, taking a manual backup,
# or any maintenance that could interrupt active writers.

set -e

DB_PATH="${PSX_DB_PATH:-$HOME/psxdata_rescue/psx.sqlite}"

echo "[stop_pakfindata] Stopping Streamlit..."
pkill -f "streamlit run" 2>/dev/null || true
sleep 2

echo "[stop_pakfindata] Stopping fusion_service..."
pkill -f "pakfindata.services.fusion_service" 2>/dev/null || true
sleep 1

echo "[stop_pakfindata] Checkpointing WAL at $DB_PATH..."
sqlite3 "$DB_PATH" "PRAGMA wal_checkpoint(FULL); PRAGMA optimize;"

WAL_SIZE=$(stat -c%s "${DB_PATH}-wal" 2>/dev/null || echo 0)
if [ "$WAL_SIZE" -lt 1000 ]; then
    echo "[stop_pakfindata] WAL clean ($WAL_SIZE bytes). Safe to sleep/shutdown."
else
    echo "[stop_pakfindata] WARNING: WAL still has $WAL_SIZE bytes — investigate before shutdown."
    echo "[stop_pakfindata] Another writer may still be holding the lock."
fi
