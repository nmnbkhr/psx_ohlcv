#!/bin/bash
# Fetch all PSX market data (DPS + PSX Terminal) → CSV + SQLite
# Usage:
#   bash ~/pakfindata/scripts/fetch_market_data.sh          # full backfill
#   bash ~/pakfindata/scripts/fetch_market_data.sh ticks     # today's ticks only
#   bash ~/pakfindata/scripts/fetch_market_data.sh klines    # all klines only
#   bash ~/pakfindata/scripts/fetch_market_data.sh status    # coverage report

set -e
export PATH="/opt/miniconda/envs/psx/bin:$PATH"
cd /home/adnoman/pakfindata

CMD="${1:-all}"

echo "════════════════════════════════════════"
echo "  PSX Market Data Fetch ($CMD)"
echo "  $(date '+%Y-%m-%d %H:%M:%S PKT')"
echo "════════════════════════════════════════"
echo ""

case "$CMD" in
    all)
        python -m pakfindata.sources.psx_market_data all
        ;;
    eod)
        python -m pakfindata.sources.psx_market_data eod $2
        ;;
    ticks)
        python -m pakfindata.sources.psx_market_data ticks $2
        ;;
    klines)
        TF="${2:-1m}"
        python -m pakfindata.sources.psx_market_data klines "$TF" --deep
        ;;
    1m)
        python -m pakfindata.sources.psx_market_data klines 1m --deep
        ;;
    status)
        python -m pakfindata.sources.psx_market_data status
        ;;
    *)
        echo "Usage: $0 [all|eod|ticks|klines|1m|status] [symbol|timeframe]"
        exit 1
        ;;
esac

echo ""
echo "════════════════════════════════════════"
echo "  Done — $(date '+%H:%M:%S')"
echo "  Files: ~/psxdata/intraday/"
echo "════════════════════════════════════════"
