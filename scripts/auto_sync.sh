#!/bin/bash
# Auto-sync cloud ticks every 15 minutes during market hours
# Cron: */15 9-17 * * 1-5 bash ~/pakfindata/scripts/auto_sync.sh
set -e

export PATH="/opt/miniconda/envs/psx/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:$PATH"

CLOUD_FILE="psx-cloud:~/psxdata/tick_logs/ticks_$(date +%Y-%m-%d).jsonl"
LOCAL_DIR="/mnt/e/psxdata/tick_logs_cloud"
DUCKDB="/mnt/e/psxdata/pakfindata.duckdb"
TICK_BARS="/mnt/e/psxdata/tick_bars.db"
TMP="/tmp/pfsync"
LOG=~/psxdata/auto_sync.log
TODAY=$(date +%Y-%m-%d)
JSONL="$LOCAL_DIR/ticks_${TODAY}.jsonl"

mkdir -p "$TMP" "$LOCAL_DIR"

echo "[$(date +%H:%M:%S)] === Auto-sync start ===" >> $LOG

# Step 1: rsync today's JSONL from cloud (only new bytes, append mode)
echo "[$(date +%H:%M:%S)] Syncing JSONL..." >> $LOG
rsync -az --append $CLOUD_FILE $LOCAL_DIR/ 2>>$LOG || echo "[$(date +%H:%M:%S)] rsync JSONL failed" >> $LOG

# Step 2: Sync tick_bars.db for ohlcv_5s (only if changed)
echo "[$(date +%H:%M:%S)] Syncing tick_bars.db..." >> $LOG
rsync -az psx-cloud:~/psxdata/tick_bars.db $TICK_BARS 2>>$LOG || echo "[$(date +%H:%M:%S)] rsync tick_bars failed" >> $LOG

# Step 3: Import JSONL into DuckDB + sync tick_bars.db → DuckDB (all via /tmp/)
if [ -f "$JSONL" ] && [ -f "$DUCKDB" ]; then
    echo "[$(date +%H:%M:%S)] DuckDB sync..." >> $LOG

    cp "$TICK_BARS" "$TMP/source.db" 2>>$LOG
    cp "$DUCKDB" "$TMP/target.duckdb" 2>>$LOG

    python3 -c "
import duckdb, sys

con = duckdb.connect('/tmp/pfsync/target.duckdb')

# --- JSONL → tick_logs ---
try:
    before = con.execute('SELECT COUNT(*) FROM tick_logs').fetchone()[0]
    con.execute('''
        INSERT OR IGNORE INTO tick_logs
        SELECT
            symbol, market, timestamp, \"_ts\",
            price, \"open\", high, low, change,
            \"changePercent\" AS change_pct,
            CAST(volume AS BIGINT) AS volume,
            value,
            CAST(trades AS INTEGER) AS trades,
            bid, ask,
            CAST(\"bidVol\" AS BIGINT) AS bid_vol,
            CAST(\"askVol\" AS BIGINT) AS ask_vol,
            \"previousClose\" AS prev_close,
            \'ticks_${TODAY}.jsonl\' AS source_file,
            strftime(now(), \'%Y-%m-%d %H:%M:%S\') AS ingested_at
        FROM read_json_auto(\'${JSONL}\',
             format=\'newline_delimited\', maximum_object_size=10485760)
    ''')
    after = con.execute('SELECT COUNT(*) FROM tick_logs').fetchone()[0]
    print(f'tick_logs: {before:,} -> {after:,} (+{after-before:,})')
except Exception as e:
    print(f'JSONL error: {e}', file=sys.stderr)

# --- tick_bars.db → ohlcv_5s, index tables ---
try:
    con.execute('INSTALL sqlite; LOAD sqlite;')
    con.execute(\"ATTACH '/tmp/pfsync/source.db' AS src (TYPE SQLITE, READ_ONLY)\")
    for t in ['ohlcv_5s', 'index_ohlcv_5s', 'index_raw_ticks']:
        before = con.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
        cols = [r[0] for r in con.execute(f'DESCRIBE {t}').fetchall()]
        col_list = ', '.join(cols)
        con.execute(f'INSERT OR IGNORE INTO {t} SELECT {col_list} FROM src.{t}')
        after = con.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
        print(f'{t}: {before:,} -> {after:,} (+{after-before:,})')
    con.execute('DETACH src')
except Exception as e:
    print(f'tick_bars error: {e}', file=sys.stderr)

con.close()
" >> $LOG 2>&1

    # Copy result back
    cp "$TMP/target.duckdb" "$DUCKDB" 2>>$LOG
    echo "[$(date +%H:%M:%S)] DuckDB updated" >> $LOG
else
    echo "[$(date +%H:%M:%S)] Skipping DuckDB (files missing)" >> $LOG
fi

# Cleanup
rm -f "$TMP/source.db" "$TMP/target.duckdb" "$TMP"/*.wal 2>/dev/null

echo "[$(date +%H:%M:%S)] === Auto-sync done ===" >> $LOG
