# Claude Code Prompt: Tick Data Audit & Fix — End the Confusion

## Problem

Tick data is scattered across 5+ locations. Pages may read from wrong sources.
Cloud tick_bars.db exists but shouldn't. Need ONE clear data path.

## Step 1: Full Audit — Where is EVERYTHING?

```bash
echo "════════════════════════════════════════════"
echo "  TICK DATA AUDIT"
echo "════════════════════════════════════════════"

echo ""
echo "=== 1. LOCAL FILES ==="
echo "--- /mnt/e/psxdata/tick_bars.db ---"
ls -lh /mnt/e/psxdata/tick_bars.db 2>/dev/null
sqlite3 /mnt/e/psxdata/tick_bars.db ".tables" 2>/dev/null
for t in $(sqlite3 /mnt/e/psxdata/tick_bars.db ".tables" 2>/dev/null); do
    echo -n "  $t: "
    sqlite3 /mnt/e/psxdata/tick_bars.db "SELECT COUNT(*) FROM [$t]" 2>/dev/null
done

echo ""
echo "--- /mnt/e/psxdata/psx.sqlite (tick tables only) ---"
for t in tick_logs tick_data intraday_bars tick_daily_summary; do
    count=$(sqlite3 /mnt/e/psxdata/psx.sqlite "SELECT COUNT(*) FROM [$t]" 2>/dev/null)
    if [ ! -z "$count" ]; then
        echo "  $t: $count"
    fi
done

echo ""
echo "--- /mnt/e/psxdata/pakfindata.duckdb ---"
python3 -c "
import duckdb
con = duckdb.connect('/mnt/e/psxdata/pakfindata.duckdb', read_only=True)
for t in con.execute(\"SELECT table_name FROM information_schema.tables WHERE table_schema='main'\").fetchall():
    count = con.execute(f'SELECT COUNT(*) FROM {t[0]}').fetchone()[0]
    print(f'  {t[0]}: {count:,}')
con.close()
" 2>/dev/null

echo ""
echo "--- /mnt/e/psxdata/tick_logs_cloud/ (JSONL from cloud) ---"
ls -lh /mnt/e/psxdata/tick_logs_cloud/*.jsonl 2>/dev/null | tail -10
echo "  Total files: $(ls /mnt/e/psxdata/tick_logs_cloud/*.jsonl 2>/dev/null | wc -l)"

echo ""
echo "--- /mnt/e/psxdata/tick_logs/ (local tick_service logs) ---"
ls -lh /mnt/e/psxdata/tick_logs/*.jsonl 2>/dev/null | tail -10

echo ""
echo "--- ~/psxdata/intraday/ (klines CSVs) ---"
ls -lh ~/psxdata/intraday/ 2>/dev/null | head -15
echo "  Total files: $(ls ~/psxdata/intraday/ 2>/dev/null | wc -l)"

echo ""
echo "=== 2. CLOUD FILES ==="
echo "--- Cloud tick_logs ---"
ssh psx-cloud "ls -lh ~/psxdata/tick_logs/" 2>/dev/null
echo ""
echo "--- Cloud tick_bars.db ---"
ssh psx-cloud "ls -lh ~/psxdata/tick_bars.db" 2>/dev/null
ssh psx-cloud "sqlite3 ~/psxdata/tick_bars.db '.tables'" 2>/dev/null

echo ""
echo "=== 3. WHAT READS WHAT ==="
echo "--- tick_analytics.py reads from ---"
grep -n "tick_bars\|psx\.sqlite\|duckdb\|jsonl\|tick_logs\|ohlcv_5s\|FROM \|connect\|Path(" \
    ~/pakfindata/src/pakfindata/ui/page_views/tick_analytics.py 2>/dev/null | head -25

echo ""
echo "--- intraday.py reads from ---"
grep -n "tick_bars\|psx\.sqlite\|duckdb\|jsonl\|tick_logs\|intraday_bars\|FROM \|connect\|Path(" \
    ~/pakfindata/src/pakfindata/ui/page_views/intraday.py 2>/dev/null | head -25

echo ""
echo "--- microstructure.py reads from ---"
grep -n "tick_bars\|psx\.sqlite\|duckdb\|jsonl\|tick_logs\|FROM \|connect\|Path(" \
    ~/pakfindata/src/pakfindata/ui/page_views/microstructure.py 2>/dev/null | head -15

echo ""
echo "--- tick_replay.py reads from ---"
grep -n "tick_bars\|psx\.sqlite\|duckdb\|jsonl\|tick_logs\|FROM \|connect\|Path(" \
    ~/pakfindata/src/pakfindata/ui/page_views/tick_replay.py 2>/dev/null | head -15

echo ""
echo "--- signal_dashboard.py reads from ---"
grep -n "tick_bars\|psx\.sqlite\|duckdb\|jsonl\|tick_logs\|FROM \|connect\|Path(" \
    ~/pakfindata/src/pakfindata/ui/page_views/signal_dashboard.py 2>/dev/null | head -15

echo ""
echo "--- connections.py (DB helpers) ---"
cat ~/pakfindata/src/pakfindata/db/connections.py 2>/dev/null | head -40

echo ""
echo "--- duckdb_manager.py paths ---"
grep -n "Path\|DUCKDB\|SQLITE\|JSONL\|tick_bars\|psx\.sqlite" \
    ~/pakfindata/src/pakfindata/db/duckdb_manager.py 2>/dev/null | head -20

echo ""
echo "=== 4. PSXT KLINES — ORPHANED? ==="
echo "--- Tables in SQLite ---"
for t in psxt_klines_1w psxt_klines_1h psxt_klines_15m psxt_klines_5m psxt_klines_1m psx_eod psx_ticks; do
    count=$(sqlite3 /mnt/e/psxdata/psx.sqlite "SELECT COUNT(*) FROM [$t]" 2>/dev/null)
    if [ ! -z "$count" ]; then
        echo "  $t: $count"
    fi
done

echo ""
echo "--- Any page reads psxt_klines? ---"
grep -rn "psxt_klines\|psxt_backfill\|klines_1w\|klines_1h\|klines_15m\|klines_5m\|klines_1m" \
    ~/pakfindata/src/pakfindata/ui/ --include="*.py" 2>/dev/null | grep -v __pycache__

echo ""
echo "--- psx_market_data.py exists? ---"
ls -la ~/pakfindata/src/pakfindata/sources/psx_market_data.py 2>/dev/null && echo "EXISTS" || echo "NOT FOUND"

echo ""
echo "=== 5. CLOUD SYNC SCRIPT ==="
cat ~/sync_psx_cloud.sh 2>/dev/null
```

**STOP — read ALL output and report it to me before making any changes.**

## Step 2: Define the CORRECT data flow

After reading the audit, here's what the architecture SHOULD be:

```
COLLECTION (Cloud VM — automatic):
  tick_service.py runs Mon-Fri 09:14-17:45 PKT
  Writes to:
    ~/psxdata/tick_logs/ticks_YYYY-MM-DD.jsonl    (normalized ticks)
    ~/psxdata/tick_logs/raw_ws_YYYY-MM-DD.jsonl   (raw WebSocket)
    ~/psxdata/tick_bars.db                         (ohlcv_5s, index tables)

DOWNLOAD (Manual — run after market close):
  bash ~/sync_psx_cloud.sh
  Copies cloud files to:
    /mnt/e/psxdata/tick_logs_cloud/ticks_YYYY-MM-DD.jsonl
    /mnt/e/psxdata/tick_logs_cloud/raw_ws_YYYY-MM-DD.jsonl

IMPORT (Tick Analytics → Sync tab):
  JSONL → DuckDB tick_logs table
  "Import ALL Missing Dates → DuckDB"

PAGES READ FROM:
  Tick Analytics Overview   → DuckDB tick_logs (fast aggregations)
  Tick Analytics Intraday   → DuckDB ohlcv_5s OR JSONL via read_json_auto
  Microstructure            → JSONL files directly (bid/ask fields)
  Tick Replay               → JSONL files directly (client-side)
  Signal Dashboard          → DuckDB tick_logs (composite scoring)
  Intraday page             → DuckDB/SQLite intraday_bars (DPS source, NOT ticks)
```

## Step 3: Fix the sync script

The sync script should download BOTH jsonl AND tick_bars.db from cloud:

```bash
cat > ~/sync_psx_cloud.sh << 'SCRIPT'
#!/bin/bash
echo "📥 Syncing PSX Cloud data..."

# Tick JSONL files (normalized + raw)
echo "  Syncing tick JSONL files..."
rsync -avz --progress psx-cloud:~/psxdata/tick_logs/ /mnt/e/psxdata/tick_logs_cloud/

# tick_bars.db (ohlcv_5s, index tables — written by cloud tick_service)
echo "  Syncing tick_bars.db..."
rsync -avz --progress psx-cloud:~/psxdata/tick_bars.db /mnt/e/psxdata/tick_bars_cloud.db

echo "✅ Done"
echo ""
echo "Cloud JSONL files:"
ls -lh /mnt/e/psxdata/tick_logs_cloud/*.jsonl | tail -5
echo ""
echo "Cloud tick_bars.db:"
ls -lh /mnt/e/psxdata/tick_bars_cloud.db
echo ""
echo "Next: Open Tick Analytics → Sync → Import ALL Missing Dates → DuckDB"
SCRIPT
chmod +x ~/sync_psx_cloud.sh
```

## Step 4: Fix tick_analytics.py data loading

Read tick_analytics.py completely:
```bash
cat ~/pakfindata/src/pakfindata/ui/page_views/tick_analytics.py
```

Ensure the data loading chain is:

### Overview tab:
```
PRIMARY: DuckDB tick_logs table (fast aggregations across all dates)
FALLBACK: JSONL files via DuckDB read_json_auto()
```

### Intraday Analytics tab:
```
PRIMARY: DuckDB ohlcv_5s table (5-second bars for charting)
IF ohlcv_5s is empty/missing for selected date:
  FALLBACK 1: JSONL → resample to 5-second bars on-the-fly
  FALLBACK 2: Cloud tick_bars.db (/mnt/e/psxdata/tick_bars_cloud.db)
```

### Sync tab:
Keep existing buttons:
- "Import JSONL → DuckDB tick_logs" ✅ (already works)
- "Import ALL Missing Dates → DuckDB" ✅ (already works)

Add NEW button:
- "Import ohlcv_5s from cloud tick_bars.db"
  ```python
  if st.button("Import ohlcv_5s from Cloud tick_bars.db"):
      cloud_db = Path("/mnt/e/psxdata/tick_bars_cloud.db")
      if cloud_db.exists():
          import duckdb
          con = duckdb.connect(str(DUCKDB_PATH))
          con.execute("INSTALL sqlite; LOAD sqlite;")
          con.execute(f"ATTACH '{cloud_db}' AS cloud (TYPE sqlite, READ_ONLY)")
          
          # Get dates already in DuckDB
          existing = set()
          try:
              existing = set(r[0] for r in con.execute(
                  "SELECT DISTINCT date FROM ohlcv_5s"
              ).fetchall())
          except:
              pass
          
          # Import missing dates from cloud
          con.execute("""
              INSERT OR IGNORE INTO ohlcv_5s 
              SELECT * FROM cloud.ohlcv_5s
              WHERE date NOT IN (SELECT DISTINCT date FROM ohlcv_5s)
          """)
          
          new_count = con.execute("SELECT COUNT(*) FROM ohlcv_5s").fetchone()[0]
          con.execute("DETACH cloud")
          con.close()
          st.success(f"Imported ohlcv_5s: {new_count:,} total rows")
      else:
          st.error("Cloud tick_bars.db not found. Run: bash ~/sync_psx_cloud.sh")
  ```

## Step 5: Fix JSONL file naming

Check if cloud uses `ticks_YYYY-MM-DD.jsonl` but local code expects `YYYY-MM-DD.jsonl`:

```bash
# What does cloud produce?
ssh psx-cloud "ls ~/psxdata/tick_logs/*.jsonl | head -5"

# What does the code expect?
grep -n "jsonl\|\.jsonl\|tick_logs" ~/pakfindata/src/pakfindata/ui/page_views/tick_analytics.py | head -20
grep -n "jsonl\|\.jsonl\|tick_logs" ~/pakfindata/src/pakfindata/db/duckdb_manager.py | head -10
grep -n "jsonl\|\.jsonl\|tick_logs" ~/pakfindata/src/pakfindata/ui/page_views/microstructure.py | head -10
grep -n "jsonl\|\.jsonl\|tick_logs" ~/pakfindata/src/pakfindata/ui/page_views/tick_replay.py | head -10
```

If there's a naming mismatch (e.g., code looks for `2026-03-24.jsonl` but file is 
`ticks_2026-03-24.jsonl`), fix it. Either:
- Rename files during sync: `rsync ... && rename ticks_ '' /mnt/e/psxdata/tick_logs_cloud/ticks_*.jsonl`
- OR update code to look for `ticks_` prefix

## Step 6: Verify cloud tick_bars.db content

```bash
echo "=== Cloud tick_bars.db tables ==="
ssh psx-cloud "sqlite3 ~/psxdata/tick_bars.db '.tables'"

echo "=== Cloud ohlcv_5s row count ==="
ssh psx-cloud "sqlite3 ~/psxdata/tick_bars.db 'SELECT COUNT(*) FROM ohlcv_5s'" 2>/dev/null

echo "=== Cloud ohlcv_5s date range ==="
ssh psx-cloud "sqlite3 ~/psxdata/tick_bars.db \"
SELECT 'rows', COUNT(*) FROM ohlcv_5s 
UNION ALL
SELECT 'symbols', COUNT(DISTINCT symbol) FROM ohlcv_5s
UNION ALL  
SELECT 'min_date', MIN(SUBSTR(ts,1,10)) FROM ohlcv_5s
UNION ALL
SELECT 'max_date', MAX(SUBSTR(ts,1,10)) FROM ohlcv_5s
\"" 2>/dev/null

echo "=== Local tick_bars.db ohlcv_5s ==="
sqlite3 /mnt/e/psxdata/tick_bars.db "SELECT COUNT(*) FROM ohlcv_5s" 2>/dev/null
```

## Step 7: PSXT Klines — decide what to do

The psxt_klines tables (1w/1h/15m/5m/1m) are orphaned — no page reads them.

Options:
A) Delete them — they overlap with ohlcv_5s and intraday_bars
B) Add a "Multi-Timeframe Chart" section to Tick Analytics that reads them
C) Leave them for future use

**Report what you find. I will decide.**

## Step 8: Summary report

After the audit, produce a clear report:

```
TICK DATA MAP
═══════════════════════════════════════

SOURCE → FILE → DB → PAGE

Cloud tick_service → ticks_*.jsonl → DuckDB tick_logs → Tick Analytics Overview
                   → raw_ws_*.jsonl → (raw archive, no DB)
                   → tick_bars.db ohlcv_5s → DuckDB ohlcv_5s → Tick Analytics Intraday
                   → tick_bars.db index_* → DuckDB index_* → Intraday page (KSE100 overlay)

DPS /timeseries/int → psx.sqlite intraday_bars → DuckDB intraday_bars → Intraday page
DPS /timeseries/eod → psx.sqlite eod_ohlcv → DuckDB eod_ohlcv → Dashboard, Screener

PSXT /klines → psx.sqlite psxt_klines_* → NOTHING (orphaned)

DAILY WORKFLOW:
1. Market closes at 15:30/16:30
2. Cloud tick_service exits at 17:45
3. Run: bash ~/sync_psx_cloud.sh
4. Open Tick Analytics → Sync → Import ALL Missing → DuckDB
5. Done — all pages have fresh data
```

## IMPORTANT

1. **Report first, fix second** — show me the audit output before changing anything
2. **Don't modify tick_service.py** — collection side is working fine
3. **Don't delete any data** — just fix the reading paths
4. **Cloud tick_bars.db is VALUABLE** — it has ohlcv_5s data that DuckDB may be missing
5. **JSONL naming must match** — cloud produces `ticks_*.jsonl`, code may expect `*.jsonl`
