# Claude Code Prompt: Standardize ALL Sync Operations + Fix Remaining Issues

## Context

The audit is done. DuckDB ohlcv_5s sync is done using the /tmp/ copy approach.
Now standardize EVERYTHING to use the same fast pattern, fix remaining issues,
and create a clean daily workflow.

Market is open today — data will only be complete after close (~17:45 PKT).
No need to sync today. Build the infrastructure now, use it tonight.

## THE PATTERN: /tmp/ Copy for ALL SQLite ↔ DuckDB Operations

Every sync operation MUST follow this pattern. USB drive (/mnt/e/) is too slow
for DuckDB's sqlite_scanner (30+ min vs 1.4 sec on /tmp/).

```python
import shutil, duckdb
from pathlib import Path

TMP_DIR = Path("/tmp/pfsync")
TMP_DIR.mkdir(exist_ok=True)

def sync_sqlite_to_duckdb(
    sqlite_path: str,      # e.g., "/mnt/e/psxdata/tick_bars.db"
    duckdb_path: str,       # e.g., "/mnt/e/psxdata/pakfindata.duckdb"
    tables: list[str],      # e.g., ["ohlcv_5s", "index_ohlcv_5s"]
    where_clause: str = ""  # e.g., "WHERE ts > '2026-03-19'"
) -> dict:
    """
    Fast SQLite → DuckDB sync via /tmp/ copy.
    
    1. Copy both files to /tmp/ (fast local filesystem)
    2. DuckDB ATTACH SQLite + bulk INSERT
    3. Copy result back to /mnt/e/
    
    Returns: {table: rows_added} dict
    """
    tmp_sqlite = TMP_DIR / "source.db"
    tmp_duckdb = TMP_DIR / "target.duckdb"
    
    # Step 1: Copy to /tmp/
    shutil.copy2(sqlite_path, tmp_sqlite)
    shutil.copy2(duckdb_path, tmp_duckdb)
    
    # Step 2: Attach + bulk INSERT
    con = duckdb.connect(str(tmp_duckdb))
    con.execute("INSTALL sqlite; LOAD sqlite;")
    con.execute(f"ATTACH '{tmp_sqlite}' AS src (TYPE SQLITE, READ_ONLY)")
    
    results = {}
    for table in tables:
        before = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        
        # Get column list from DuckDB target
        cols = [r[0] for r in con.execute(f"DESCRIBE {table}").fetchall()]
        col_list = ", ".join(cols)
        
        sql = f"INSERT OR IGNORE INTO {table} SELECT {col_list} FROM src.{table}"
        if where_clause:
            sql += f" {where_clause}"
        
        con.execute(sql)
        after = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        results[table] = after - before
    
    con.execute("DETACH src")
    con.close()
    
    # Step 3: Copy back to /mnt/e/
    shutil.copy2(tmp_duckdb, duckdb_path)
    
    # Cleanup
    tmp_sqlite.unlink(missing_ok=True)
    tmp_duckdb.unlink(missing_ok=True)
    
    return results
```

## Step 1: Add sync helper to duckdb_manager.py

Add the `sync_sqlite_to_duckdb()` function above to 
`src/pakfindata/db/duckdb_manager.py`.

This is the ONE function all sync buttons will call.

## Step 2: Create ~/sync_psx_cloud.sh (MISSING)

```bash
cat > ~/sync_psx_cloud.sh << 'SCRIPT'
#!/bin/bash
set -e

echo "════════════════════════════════════════"
echo "  PSX Cloud → Local Sync"
echo "════════════════════════════════════════"
echo ""

# 1. Sync JSONL tick files (normalized + raw)
echo "📥 Syncing tick JSONL files..."
rsync -avz --progress psx-cloud:~/psxdata/tick_logs/ /mnt/e/psxdata/tick_logs_cloud/
echo ""

# 2. Sync tick_bars.db (ohlcv_5s, index tables)
echo "📥 Syncing tick_bars.db..."
rsync -avz --progress psx-cloud:~/psxdata/tick_bars.db /mnt/e/psxdata/tick_bars.db
echo ""

# 3. Symlink ticks_ prefix files for code compatibility
echo "📎 Creating symlinks for JSONL compatibility..."
for f in /mnt/e/psxdata/tick_logs_cloud/ticks_*.jsonl; do
    base=$(basename "$f")
    date_part="${base#ticks_}"
    link="/mnt/e/psxdata/tick_logs_cloud/$date_part"
    [ ! -f "$link" ] && ln -sf "$f" "$link" 2>/dev/null && echo "  Linked: $date_part"
done
echo ""

# 4. Summary
echo "════════════════════════════════════════"
echo "  SYNC COMPLETE"
echo "════════════════════════════════════════"
echo ""
echo "JSONL files:"
ls -lh /mnt/e/psxdata/tick_logs_cloud/*.jsonl 2>/dev/null | tail -5
echo ""
echo "tick_bars.db:"
ls -lh /mnt/e/psxdata/tick_bars.db
echo ""
echo "Next steps:"
echo "  1. Open pakfindata → Tick Analytics → Sync tab"
echo "  2. Click 'Full Nightly Sync'"
echo ""
SCRIPT
chmod +x ~/sync_psx_cloud.sh
```

## Step 3: Add sync buttons to Tick Analytics Sync tab

Read the current Sync tab:
```bash
cat ~/pakfindata/src/pakfindata/ui/page_views/tick_analytics.py | grep -n "Sync\|sync\|tick_bars\|DuckDB" | head -30
```

Add these buttons (DO NOT remove existing buttons):

### Button A: "Sync tick_bars.db → DuckDB"

```python
st.markdown("#### Sync tick_bars.db → DuckDB (via /tmp/)")
st.caption("Merges ohlcv_5s + index tables from tick_bars.db into DuckDB. ~10 seconds.")

col1, col2 = st.columns([1, 1])

with col1:
    if st.button("🔄 Sync tick_bars.db → DuckDB", type="primary"):
        with st.spinner("Copying to /tmp/ for fast sync..."):
            from pakfindata.db.duckdb_manager import sync_sqlite_to_duckdb
            results = sync_sqlite_to_duckdb(
                sqlite_path=str(TICK_BARS_DB),
                duckdb_path=str(DUCKDB_PATH),
                tables=["ohlcv_5s", "index_ohlcv_5s", "index_raw_ticks"],
            )
            for table, added in results.items():
                if added > 0:
                    st.success(f"✅ {table}: +{added:,} new rows")
                else:
                    st.info(f"ℹ️ {table}: already up to date")

with col2:
    # Show current counts
    try:
        import duckdb as _ddb, sqlite3 as _sql
        _dc = _ddb.connect(str(DUCKDB_PATH), read_only=True)
        _sc = _sql.connect(str(TICK_BARS_DB))
        for t in ["ohlcv_5s", "index_ohlcv_5s", "index_raw_ticks"]:
            dc = _dc.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            sc = _sc.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            delta = sc - dc
            if delta > 0:
                st.warning(f"{t}: DuckDB {dc:,} | SQLite {sc:,} (⚠️ {delta:,} behind)")
            else:
                st.success(f"{t}: {dc:,} rows ✅")
        _dc.close(); _sc.close()
    except Exception as e:
        st.error(str(e))
```

### Button B: "Full Nightly Sync" (one-click)

```python
st.markdown("---")
st.markdown("#### 🚀 Full Nightly Sync")
st.caption("tick_bars.db → DuckDB + ALL JSONL → DuckDB in one click")

if st.button("🚀 Run Full Nightly Sync", type="primary"):
    progress = st.progress(0, text="Starting...")
    
    # Part 1: tick_bars.db → DuckDB
    progress.progress(10, text="Syncing tick_bars.db → DuckDB...")
    from pakfindata.db.duckdb_manager import sync_sqlite_to_duckdb
    results = sync_sqlite_to_duckdb(
        sqlite_path=str(TICK_BARS_DB),
        duckdb_path=str(DUCKDB_PATH),
        tables=["ohlcv_5s", "index_ohlcv_5s", "index_raw_ticks"],
    )
    for table, added in results.items():
        st.write(f"  {table}: +{added:,}")
    
    # Part 2: JSONL → DuckDB tick_logs
    progress.progress(50, text="Importing JSONL → DuckDB tick_logs...")
    # Use existing JSONL import logic from this page
    # ... (call the existing import function) ...
    
    progress.progress(100, text="✅ Full sync complete!")
```

## Step 4: Fix signal_dashboard.py — DuckDB instead of SQLite

```bash
grep -n "tick_logs\|psx\.sqlite\|sqlite3.*connect" \
    ~/pakfindata/src/pakfindata/ui/page_views/signal_dashboard.py | head -20
```

Replace psx.sqlite tick_logs reads with DuckDB:

```python
# BEFORE:
con = sqlite3.connect("/mnt/e/psxdata/psx.sqlite")
df = pd.read_sql("SELECT * FROM tick_logs WHERE ...", con)

# AFTER:
from pakfindata.db.connections import duck
df = duck("SELECT * FROM tick_logs WHERE ...")
```

DuckDB has 4.6M rows vs SQLite's 2.5M — more data, faster queries.

## Step 5: Fix Intraday Index tab — DuckDB instead of JSONL

```bash
grep -n "index_ohlcv\|index_raw\|JSONL.*index\|jsonl.*index\|tick_logs.*KSE\|tick_logs.*index" \
    ~/pakfindata/src/pakfindata/ui/page_views/intraday.py | head -10
```

If reading JSONL directly for index data, change to DuckDB:

```python
# BEFORE: reads JSONL line-by-line
# AFTER:
from pakfindata.db.connections import duck
df = duck("SELECT * FROM index_ohlcv_5s WHERE symbol = ? AND date = ? ORDER BY ts", [index, date])
```

## Step 6: JSONL file naming — handle both patterns

Code may look for `2026-03-24.jsonl` but cloud produces `ticks_2026-03-24.jsonl`.

Check:
```bash
grep -rn "tick_logs_cloud\|\.jsonl" ~/pakfindata/src/pakfindata/ui/page_views/tick_analytics.py | head -5
grep -rn "tick_logs_cloud\|\.jsonl" ~/pakfindata/src/pakfindata/ui/page_views/tick_replay.py | head -5
grep -rn "tick_logs_cloud\|\.jsonl" ~/pakfindata/src/pakfindata/ui/page_views/microstructure.py | head -5
grep -rn "tick_logs_cloud\|\.jsonl" ~/pakfindata/src/pakfindata/db/duckdb_manager.py | head -5
```

Fix: make all JSONL loaders check BOTH naming patterns:
```python
def find_jsonl(date_str: str) -> Path | None:
    """Find JSONL file for a date — handles both naming patterns."""
    cloud = Path("/mnt/e/psxdata/tick_logs_cloud")
    for pattern in [f"ticks_{date_str}.jsonl", f"{date_str}.jsonl"]:
        path = cloud / pattern
        if path.exists():
            return path
    # Try local
    local = Path("/mnt/e/psxdata/tick_logs")
    for pattern in [f"ticks_{date_str}.jsonl", f"{date_str}.jsonl"]:
        path = local / pattern
        if path.exists():
            return path
    return None
```

Add this helper to `connections.py` and use it everywhere JSONL is loaded.

## Step 7: Verify

```bash
echo "=== 1. sync_psx_cloud.sh exists ==="
ls -la ~/sync_psx_cloud.sh

echo "=== 2. sync function in duckdb_manager ==="
grep -c "sync_sqlite_to_duckdb" ~/pakfindata/src/pakfindata/db/duckdb_manager.py

echo "=== 3. signal_dashboard uses DuckDB ==="
grep -c "duck\b" ~/pakfindata/src/pakfindata/ui/page_views/signal_dashboard.py
grep -c "psx\.sqlite" ~/pakfindata/src/pakfindata/ui/page_views/signal_dashboard.py

echo "=== 4. find_jsonl helper exists ==="
grep -c "find_jsonl" ~/pakfindata/src/pakfindata/db/connections.py

echo "=== 5. DuckDB current state ==="
python3 -c "
import duckdb
con = duckdb.connect('/mnt/e/psxdata/pakfindata.duckdb', read_only=True)
for t in ['ohlcv_5s','index_ohlcv_5s','index_raw_ticks','tick_logs','intraday_bars','eod_ohlcv']:
    try:
        r = con.execute(f'SELECT COUNT(*), MIN(date), MAX(date) FROM {t}').fetchone()
        print(f'  {t}: {r[0]:,} rows | {r[1]} → {r[2]}')
    except: pass
con.close()
"
```

## DAILY WORKFLOW (after market close ~17:45 PKT)

```
Terminal:
  bash ~/sync_psx_cloud.sh

App:
  Tick Analytics → Sync → "Full Nightly Sync"

Done. All pages have fresh data.
```

## IMPORTANT

1. ALL sync uses /tmp/ copy — never DuckDB sqlite_scanner on /mnt/e/
2. Don't modify tick_service.py
3. Don't delete any data
4. sync_psx_cloud.sh downloads BOTH JSONL + tick_bars.db
5. signal_dashboard MUST switch to DuckDB
6. JSONL loader handles both ticks_*.jsonl and *.jsonl patterns
7. Today's data incomplete — test with yesterday's
