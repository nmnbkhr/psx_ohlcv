#!/bin/bash
# psxt_archive_pull.sh — Pull complete multi-resolution PSX historical archive
#
# Wraps `python -m pakfindata.sources.psxt_archive_pull` with proper logging,
# conda env activation, and per-timeframe error isolation.
#
# Usage:
#   ./psxt_archive_pull.sh                        # REG market, all timeframes
#   ./psxt_archive_pull.sh --markets REG FUT IDX  # All markets
#   ./psxt_archive_pull.sh --markets IDX          # Indices only
#   ./psxt_archive_pull.sh 1m 5m 1h               # Specific timeframes
#   ./psxt_archive_pull.sh --symbol HUBC          # Single symbol
#   ./psxt_archive_pull.sh --date 2026-04-29      # Specific date
#
# Examples:
#   # Daily EOD backup (cron-friendly)
#   ./psxt_archive_pull.sh 1m 5m 15m 1h --markets REG FUT
#
#   # Full historical archive rebuild
#   ./psxt_archive_pull.sh --markets REG FUT IDX

set -e

# ─── Config ────────────────────────────────────────────────────
ALL_TIMEFRAMES=("1m" "5m" "15m" "1h" "4h" "1d" "1w" "1M")
ALL_MARKETS=("REG" "FUT" "IDX")
PSX_ENV="psx"
OUT_BASE="${HOME}/psxdata_rescue/intraday/psxt_ws"
LOG_DIR="${HOME}/psxdata_rescue/logs"
LOG_FILE="${LOG_DIR}/archive_pull_$(date +%Y%m%d_%H%M%S).log"

# ─── Argument parsing ──────────────────────────────────────────
SYMBOL=""
DATE_FILTER=""
TIMEFRAMES=()
MARKETS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --symbol)
            SYMBOL="$2"; shift 2 ;;
        --date)
            DATE_FILTER="$2"; shift 2 ;;
        --markets)
            shift
            while [[ $# -gt 0 && ! "$1" =~ ^-- && ! "$1" =~ ^[0-9] ]]; do
                MARKETS+=("$1"); shift
            done
            ;;
        --help|-h)
            sed -n '2,18p' "$0" | sed 's/^# \?//'
            exit 0 ;;
        1m|5m|15m|1h|4h|1d|1w|1M)
            TIMEFRAMES+=("$1"); shift ;;
        *)
            echo "Unknown argument: $1"
            echo "Valid timeframes: ${ALL_TIMEFRAMES[*]}"
            echo "Valid markets:    ${ALL_MARKETS[*]}"
            exit 1 ;;
    esac
done

# Defaults
[ ${#TIMEFRAMES[@]} -eq 0 ] && TIMEFRAMES=("${ALL_TIMEFRAMES[@]}")
[ ${#MARKETS[@]} -eq 0 ] && MARKETS=("REG")

# ─── Setup ─────────────────────────────────────────────────────
mkdir -p "$OUT_BASE" "$LOG_DIR"

# Activate conda env
if [ -z "${CONDA_DEFAULT_ENV:-}" ] || [ "$CONDA_DEFAULT_ENV" != "$PSX_ENV" ]; then
    if [ -f "$HOME/miniforge3/etc/profile.d/conda.sh" ]; then
        source "$HOME/miniforge3/etc/profile.d/conda.sh"
    elif [ -f "$HOME/miniconda3/etc/profile.d/conda.sh" ]; then
        source "$HOME/miniconda3/etc/profile.d/conda.sh"
    elif [ -f "/opt/conda/etc/profile.d/conda.sh" ]; then
        source "/opt/conda/etc/profile.d/conda.sh"
    fi
    conda activate "$PSX_ENV" 2>/dev/null || echo "⚠️  Could not activate '$PSX_ENV' env"
fi

# ─── Header ────────────────────────────────────────────────────
{
    echo "════════════════════════════════════════════════════════════════"
    echo "  PSX Archive Pull (shell wrapper)"
    echo "  Started:    $(date '+%Y-%m-%d %H:%M:%S %Z')"
    echo "  Timeframes: ${TIMEFRAMES[*]}"
    echo "  Markets:    ${MARKETS[*]}"
    echo "  Symbol:     ${SYMBOL:-ALL}"
    [ -n "$DATE_FILTER" ] && echo "  Date:       $DATE_FILTER"
    echo "  Output:     $OUT_BASE"
    echo "  Log:        $LOG_FILE"
    echo "════════════════════════════════════════════════════════════════"
    echo ""
} | tee -a "$LOG_FILE"

# ─── Build python command ──────────────────────────────────────
START_TIME=$(date +%s)
SUCCEEDED=()
FAILED=()

for TF in "${TIMEFRAMES[@]}"; do
    echo "" | tee -a "$LOG_FILE"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" | tee -a "$LOG_FILE"
    echo "  [$(date '+%H:%M:%S')] Pulling timeframe: $TF" | tee -a "$LOG_FILE"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" | tee -a "$LOG_FILE"

    CMD=(python -m pakfindata.sources.psxt_archive_pull --timeframes "$TF" --markets "${MARKETS[@]}")

    if [ -n "$SYMBOL" ]; then
        CMD+=(--symbol "$SYMBOL")
    fi
    if [ -n "$DATE_FILTER" ]; then
        CMD+=(--date "$DATE_FILTER")
    fi

    TF_START=$(date +%s)

    if "${CMD[@]}" 2>&1 | tee -a "$LOG_FILE"; then
        TF_END=$(date +%s)
        DURATION=$((TF_END - TF_START))
        # Find the output file (varies by tag based on markets)
        if [ -n "$SYMBOL" ]; then
            TAG="$SYMBOL"
        else
            TAG="all_$(echo "${MARKETS[@]}" | tr ' ' '_' | tr '[:upper:]' '[:lower:]')"
            # Bash sort hack: turn into newlines, sort, turn back
            TAG="all_$(printf '%s\n' "${MARKETS[@]}" | sort | tr '\n' '_' | sed 's/_$//' | tr '[:upper:]' '[:lower:]')"
        fi
        if [ -n "$DATE_FILTER" ]; then
            OUT_FILE="$OUT_BASE/${TAG}_${TF}_${DATE_FILTER}.csv"
        else
            OUT_FILE="$OUT_BASE/${TAG}_${TF}.csv"
        fi
        if [ -f "$OUT_FILE" ]; then
            ROWS=$(wc -l < "$OUT_FILE")
            SIZE=$(du -h "$OUT_FILE" | cut -f1)
            echo "  ✅ $TF complete: $ROWS rows, $SIZE, ${DURATION}s" | tee -a "$LOG_FILE"
            SUCCEEDED+=("$TF ($ROWS rows, $SIZE, ${DURATION}s)")
        else
            echo "  ⚠️  $TF — output file not found: $OUT_FILE" | tee -a "$LOG_FILE"
            FAILED+=("$TF (no output file)")
        fi
    else
        echo "  ❌ $TF FAILED" | tee -a "$LOG_FILE"
        FAILED+=("$TF")
    fi

    sleep 2  # gentle pause between timeframes
done

# ─── Summary ───────────────────────────────────────────────────
END_TIME=$(date +%s)
TOTAL_DURATION=$((END_TIME - START_TIME))

{
    echo ""
    echo "════════════════════════════════════════════════════════════════"
    echo "  SUMMARY"
    echo "════════════════════════════════════════════════════════════════"
    echo "  Total time:     ${TOTAL_DURATION}s ($((TOTAL_DURATION / 60))m $((TOTAL_DURATION % 60))s)"
    echo "  Succeeded:      ${#SUCCEEDED[@]}/${#TIMEFRAMES[@]}"
    echo "  Failed:         ${#FAILED[@]}/${#TIMEFRAMES[@]}"
    echo ""
    if [ ${#SUCCEEDED[@]} -gt 0 ]; then
        echo "  ✅ Successful pulls:"
        for s in "${SUCCEEDED[@]}"; do
            echo "     - $s"
        done
    fi
    if [ ${#FAILED[@]} -gt 0 ]; then
        echo ""
        echo "  ❌ Failed pulls:"
        for f in "${FAILED[@]}"; do
            echo "     - $f"
        done
    fi
    echo ""
    echo "  Files in $OUT_BASE:"
    ls -lh "$OUT_BASE/" 2>/dev/null | tail -n +2 || echo "     (empty)"
    echo "════════════════════════════════════════════════════════════════"
} | tee -a "$LOG_FILE"

[ ${#FAILED[@]} -gt 0 ] && exit 1
exit 0
