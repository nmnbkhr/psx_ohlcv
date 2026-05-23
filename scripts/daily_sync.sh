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

# enqueue_and_wait <job_type> [params_json]
# Submit a job to the worker via /v1/jobs/{job_type}, poll until terminal,
# return 0 on ok / 1 on failed | cancelled | unexpected | timeout.
#
# On API-down (health probe fails within 5s) falls back to eval-ing
# $API_FALLBACK_CMD and returns its exit code. On worker-down the job
# enqueues anyway; we wait POLL_TIMEOUT_S then return 1 WITHOUT cancelling
# the job — the worker picks it up on restart; cron exits non-zero so
# log-aggregation alerts surface the gap.
#
# Env required:
#   PAKFINDATA_API_URL    e.g. http://127.0.0.1:8001
#   PAKFINDATA_API_TOKEN  Bearer token
#
# Env optional:
#   POLL_INTERVAL_S    default 5
#   POLL_TIMEOUT_S     default 1800 (30 min — covers big rebuilds)
#   API_FALLBACK_CMD   command eval'd when API health probe fails
enqueue_and_wait() {
    local job_type="$1"
    local params="${2:-{\}}"
    local api_url="${PAKFINDATA_API_URL:-http://127.0.0.1:8001}"
    local token="${PAKFINDATA_API_TOKEN:?PAKFINDATA_API_TOKEN not set}"
    local poll_int="${POLL_INTERVAL_S:-5}"
    local poll_timeout="${POLL_TIMEOUT_S:-1800}"

    # 5s API health probe — falls back to CLI if API is unreachable.
    if ! curl -sf --max-time 5 "${api_url}/health" >/dev/null; then
        echo "  enqueue_and_wait[${job_type}]: API unreachable, falling back"
        if [[ -n "${API_FALLBACK_CMD:-}" ]]; then
            eval "${API_FALLBACK_CMD}"
            return $?
        else
            echo "  enqueue_and_wait[${job_type}]: no API_FALLBACK_CMD; failing"
            return 1
        fi
    fi

    # Enqueue. ?source=cron tags the row so Jobs Monitor's
    # WHERE source='cron' queries cleanly separate scheduled from manual.
    local submit_response job_id
    submit_response=$(curl -sf -X POST \
        "${api_url}/v1/jobs/${job_type}?source=cron" \
        -H "Authorization: Bearer ${token}" \
        -H "Content-Type: application/json" \
        -d "{\"params\": ${params}}" 2>/dev/null)
    if [[ -z "${submit_response}" ]]; then
        echo "  enqueue_and_wait[${job_type}]: enqueue failed (curl no body)"
        return 1
    fi
    job_id=$(echo "${submit_response}" \
        | python3 -c "import json,sys; print(json.load(sys.stdin).get('job_id',''))" \
        2>/dev/null)
    if [[ -z "${job_id}" ]]; then
        echo "  enqueue_and_wait[${job_type}]: no job_id in response: ${submit_response}"
        return 1
    fi
    echo "  enqueue_and_wait[${job_type}]: enqueued as job ${job_id}"

    # Poll until terminal or timeout.
    local elapsed=0 status
    while [[ ${elapsed} -lt ${poll_timeout} ]]; do
        sleep "${poll_int}"
        elapsed=$((elapsed + poll_int))
        status=$(curl -sf --max-time 10 \
            "${api_url}/v1/jobs/${job_id}" \
            -H "Authorization: Bearer ${token}" \
            | python3 -c "import json,sys; print(json.load(sys.stdin).get('status',''))" \
            2>/dev/null)
        case "${status}" in
            ok)
                echo "  enqueue_and_wait[${job_type}]: job ${job_id} ok in ${elapsed}s"
                return 0 ;;
            failed)
                echo "  enqueue_and_wait[${job_type}]: job ${job_id} FAILED in ${elapsed}s"
                return 1 ;;
            cancelled)
                echo "  enqueue_and_wait[${job_type}]: job ${job_id} cancelled"
                return 1 ;;
            pending|running)
                ;;
            "")
                echo "  enqueue_and_wait[${job_type}]: job ${job_id} status fetch failed (transient?)"
                ;;
            *)
                echo "  enqueue_and_wait[${job_type}]: job ${job_id} unexpected status: ${status}"
                return 1 ;;
        esac
    done
    # Timeout: do NOT cancel — worker may catch up after restart. Cron
    # returns non-zero so alerts pick up the gap; data_freshness shows
    # the catalog row whenever the worker eventually finishes.
    echo "  enqueue_and_wait[${job_type}]: job ${job_id} timeout after ${poll_timeout}s (NOT cancelled; worker may finish later)"
    return 1
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
