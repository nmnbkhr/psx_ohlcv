#!/usr/bin/env bash
# scripts/daily_sync.sh — pakfindata daily refresh
#
# Schedule (intended): 03:45 PKT every day. Skips weekends and PSX holidays
# via pakfindata.utils.trading_calendar.
#
# Pipeline order (writers first, derived data after):
#   1. regular_market snapshot
#   2. indices sync
#   3. rates sync (KIBOR + KONIA + PKRV)
#   4. treasury sync (T-Bill + PIB)  [Mon only — auctions are weekly]
#   5. fx-rates sync-all (interbank + kerb)
#   6. market-summary day (yesterday) → eod_ohlcv
#   7. summary rebuild-today (eod_{symbol,market,sector}_summary)
#   8. announcements sync (announcements + corporate_events + dividend_payouts)
#   9. intraday ticks-fetch (yesterday) → JSON on disk
#  10. intraday ticks-load (yesterday) → intraday_bars + tick_data
#  11. intraday summaries-build (yesterday) → intraday_*_summary
#  12. parquet sync-missing → fill any gaps from manifest to disk
#  13. WAL checkpoint (FULL) + PRAGMA optimize
#
# Each step runs in its own Python process. Per-step failure is logged
# but does NOT abort subsequent steps; the catalog records status='failed'
# so the morning dashboard surfaces the issue.
#
# Logs: ~/.local/share/pakfindata/logs/daily_sync_YYYYMMDD.log
# Retention: 30 days.

set -u  # NOT set -e — we want to continue on per-step failure

export PSX_DATA_ROOT="${PSX_DATA_ROOT:-/mnt/e/psxdata}"
export PSX_DB_PATH="${PSX_DB_PATH:-$HOME/psxdata_rescue/psx.sqlite}"

LOG_DIR="$HOME/.local/share/pakfindata/logs"
mkdir -p "$LOG_DIR"
TODAY=$(date +%Y%m%d)
LOG_FILE="$LOG_DIR/daily_sync_$TODAY.log"

# Activate conda env (cron has no PATH for conda by default)
if [ -f "$HOME/miniforge3/etc/profile.d/conda.sh" ]; then
    # shellcheck disable=SC1091
    source "$HOME/miniforge3/etc/profile.d/conda.sh"
    conda activate psx
fi

log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') $*" | tee -a "$LOG_FILE"
}

run_step() {
    local step_name="$1"
    shift
    log "STEP_START name=$step_name"
    local start_ts
    start_ts=$(date +%s)
    if "$@" >> "$LOG_FILE" 2>&1; then
        local elapsed=$(($(date +%s) - start_ts))
        log "STEP_OK name=$step_name duration_s=$elapsed"
    else
        local rc=$?
        local elapsed=$(($(date +%s) - start_ts))
        log "STEP_FAIL name=$step_name exit_code=$rc duration_s=$elapsed"
    fi
}

log "=== daily_sync start (host=$(hostname), pid=$$) ==="

# Skip non-trading days. Exit code 0 — we're intentionally idle.
if ! python -c "from pakfindata.utils.trading_calendar import is_trading_day; import sys; sys.exit(0 if is_trading_day() else 1)"; then
    log "SKIPPED reason=not_a_trading_day"
    log "=== daily_sync end (skipped) ==="
    exit 0
fi

# Make sure nothing else is holding the DB (Streamlit, REPLs, etc.)
"$HOME/projects/pakfindata/scripts/stop_pakfindata.sh" >> "$LOG_FILE" 2>&1 || true

# The PREVIOUS TRADING DAY — not literal yesterday. On a Monday cron run
# "yesterday" is Sunday (no data); the right date is the prior Friday.
# Same logic handles cron runs after a multi-day Eid cluster.
PREV_TRADING_DAY=$(python -c "
from datetime import date, timedelta
from pakfindata.utils.trading_calendar import last_trading_day
print(last_trading_day(date.today() - timedelta(days=1)))
")
DOW=$(date +%u)  # 1..7, Monday=1

# ── Pipeline ─────────────────────────────────────────────────────────────

run_step "regular_market"     python -m pakfindata.cli regular-market snapshot
run_step "indices"            python -m pakfindata.cli indices sync
run_step "rates"              python -m pakfindata.cli rates sync

# T-Bill / PIB auctions are weekly (typically Wed); cheap enough to retry
# every weekday. Run on weekdays only.
run_step "treasury"           python -m pakfindata.cli treasury sync

run_step "fx_rates"           python -m pakfindata.cli fx-rates sync-all

# EOD market summary for yesterday's trading day. --import-eod writes
# into eod_ohlcv and updates the catalog row in the same transaction.
run_step "market_summary"     python -m pakfindata.cli market-summary day \
                                  --date "$PREV_TRADING_DAY" --import-eod

# Summary tables read eod_ohlcv → must run AFTER market_summary above.
run_step "summary_today"      python -m pakfindata.cli summary rebuild-today

run_step "announcements"      python -m pakfindata.cli announcements sync

# Intraday: 3 sub-steps for yesterday's session.
run_step "intraday_fetch"     python -m pakfindata.cli intraday ticks-fetch \
                                  --date "$PREV_TRADING_DAY"
run_step "intraday_load"      python -m pakfindata.cli intraday ticks-load \
                                  --date "$PREV_TRADING_DAY"
run_step "intraday_summaries" python -m pakfindata.cli intraday summaries-build \
                                  --date "$PREV_TRADING_DAY"

# Parquet: incremental sync (only missing dates) is cheap; run every day.
run_step "parquet_sync"       python -m pakfindata.cli parquet sync-missing

# ── Post-pipeline housekeeping ───────────────────────────────────────────

log "STEP_START name=wal_checkpoint"
sqlite3 "$PSX_DB_PATH" "PRAGMA wal_checkpoint(FULL); PRAGMA optimize;" \
    >> "$LOG_FILE" 2>&1
log "STEP_OK name=wal_checkpoint"

# Show day-of-week label for log readability (Mon=1, Sun=7).
log "=== daily_sync end (dow=$DOW prev_trading_day=$PREV_TRADING_DAY) ==="

# Log retention: keep last 30 days.
find "$LOG_DIR" -maxdepth 1 -name "daily_sync_*.log" -mtime +30 -delete \
    2>/dev/null || true

exit 0
