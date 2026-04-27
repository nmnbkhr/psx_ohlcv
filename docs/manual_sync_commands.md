# Manual Sync Commands — Reference

Quick-reference for every backfill/sync command used in this codebase. All examples assume `conda activate psx && cd ~/projects/pakfindata`.

DB destinations:
- **NVMe (fast)**: `/home/smnb/psxdata_rescue/psx.sqlite`, `tick_bars.db`, `commod/commod.db`
- **NTFS (bulk)**: `/mnt/e/psxdata/{parquet,intraday,tick_logs_cloud,sbp_easydata,...}`

---

## 1. App lifecycle

### Start (foreground)
```bash
conda activate psx
cd ~/projects/pakfindata
streamlit run src/pakfindata/ui/app.py
```

### Start (background)
```bash
nohup streamlit run src/pakfindata/ui/app.py \
  --server.port 8501 --server.headless true > /tmp/streamlit.log 2>&1 &
disown
```

### Stop
```bash
pkill -f "streamlit run"
```

### Restart (after large DB writes)
Always restart Streamlit after big backfills — its `@st.cache_resource` SQLite connections become stale and throw `disk I/O error` on subsequent reads.

---

## 2. Cloud sync (Oracle VM → local)

```bash
~/sync_psx_cloud.sh
```

What it does:
- `tick_logs/*.jsonl` → `/mnt/e/psxdata/tick_logs_cloud/` (NTFS)
- `intraday/*.json` → `/mnt/e/psxdata/intraday_cloud/` (NTFS)
- `tick_bars.db` → `/home/smnb/psxdata_rescue/tick_bars.db` (NVMe)

Source of truth is `psx-cloud:~/psxdata/`. VM rotates JSONL after ~10 days, so old history only exists locally.

---

## 3. EOD market data (DPS + PSX Terminal)

### Full backfill (all symbols, all sources)
```bash
bash scripts/fetch_market_data.sh all
```

### Subcommands
```bash
bash scripts/fetch_market_data.sh eod           # DPS daily OHLCV (5-yr history)
bash scripts/fetch_market_data.sh ticks         # DPS today's ticks
bash scripts/fetch_market_data.sh klines 1m     # PSX Terminal 1-min klines
bash scripts/fetch_market_data.sh klines 1h --deep    # paginated 1h history
bash scripts/fetch_market_data.sh status        # coverage report
bash scripts/fetch_market_data.sh eod HUBC      # single symbol
```

Writes:
- CSVs → `~/psxdata/intraday/{date}/dps_eod_daily.csv`, `dps_ticks_{date}.csv`, `psxt_*.csv`
- DB tables → `psx_eod`, `psx_ticks`, `psxt_klines_{1m,5m,15m,1h,1d,1w}`

⚠ This script populates **`psx_eod`** (the script's table), NOT the canonical `eod_ohlcv` used by dashboards. For `eod_ohlcv`, use the market_summary path (Section 4).

⚠ Klines from PSX Terminal API only return latest few hundred bars. **No historical backfill.** For historical klines, rebuild from JSONL.

---

## 4. Market Summary (canonical EOD path)

Populates `eod_ohlcv` (the table dashboards read).

```bash
# Download .Z file for one date
python -m pakfindata.sources.market_summary download 2026-04-21

# Range download
python -m pakfindata.sources.market_summary download 2026-04-21 2026-04-24

# Ingest CSV into eod_ohlcv (after download)
# Done automatically by the download command, OR via UI button
```

Source: `dps.psx.com.pk` `.Z` bulk file → CSV → ingest.
Writes:
- CSVs → `~/data/market_summary/csv/{date}.csv`
- `eod_ohlcv` (REG market) + `futures_eod` (FUT/CONT/ODL)

---

## 5. Intraday data

### Fetch per-symbol JSON from PSX API (current session only)
```bash
# UI: Intraday → Sync tab → "Fetch All Ticks → Disk" button
# Or CLI:
python -c "
import sys; sys.path.insert(0, 'src')
import sqlite3
from pakfindata.sync_timeseries import fetch_ticks_to_disk_parallel
con = sqlite3.connect('/home/smnb/psxdata_rescue/psx.sqlite')
print(fetch_ticks_to_disk_parallel(con, '2026-04-24', n_shards=3))
"
```

⚠ PSX API returns **only the latest trading session** regardless of date parameter. To fill historical days, use the JSONL ingest below.

### Load per-symbol JSON → intraday_bars
```bash
python -c "
import sys; sys.path.insert(0, 'src')
import sqlite3
from pakfindata.sync_timeseries import load_ticks_from_disk
con = sqlite3.connect('/home/smnb/psxdata_rescue/psx.sqlite')
print(load_ticks_from_disk(con, '2026-04-24'))
"
```

### JSONL → intraday_bars (historical backfill, all 5 markets)
The DPS API returns only REG market. JSONL captures REG + FUT + IDX + BNB + ODL. For historical days, use the JSONL ingester (one-shot script — see this session's notes for the inline ~50-line implementation, or replicate by reading JSONL with DuckDB and `INSERT OR IGNORE INTO intraday_bars`).

### Build summary tables (intraday_daily_summary, intraday_minute_breadth, intraday_hourly_summary)
```bash
python -c "
import sys; sys.path.insert(0, 'src')
import sqlite3
from pakfindata.db.repositories import intraday_summary as s
con = sqlite3.connect('/home/smnb/psxdata_rescue/psx.sqlite')
print(s.compute_all(con, '2026-04-24'))
"
```

Reads JSONL directly, writes 3 SQLite summary tables.

---

## 6. Rate tables (KIBOR, Policy, T-Bill, PKRV, FX)

### KIBOR + Policy rate (via SBP EasyData)
```bash
# Fetch + sync (full priority datasets — slow, ~5.7 hours)
python -m pakfindata.sources.sbp_easydata fetch-recent --months 2
python -m pakfindata.sources.sbp_easydata sync-db
```

### Targeted KIBOR + Policy fetch (~5 min)
For just the 21 series needed by `kibor_daily` + `sbp_policy_rates`:

```bash
python -c "
import sys, json, csv
sys.path.insert(0, 'src')
import sqlite3
from datetime import datetime, timedelta
from pakfindata.sources.sbp_easydata import (
    get_series_data, SERIES_DIR, sync_kibor_to_db, sync_policy_rate_to_db, CATALOG_FILE,
)

with open(CATALOG_FILE) as f:
    cat = json.load(f)
keys = []
for code in ['TS_GP_BAM_SIRKIBOR_D', 'TS_GP_IR_SIRPR_AH']:
    keys.extend(cat['datasets'][code]['series_keys'])

start = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
end = datetime.now().strftime('%Y-%m-%d')
for sk in keys:
    data = get_series_data(sk, start_date=start, end_date=end)
    if data and data.get('rows'):
        fp = SERIES_DIR / f\"{sk.replace('.', '_')}.json\"
        fp.write_text(json.dumps(data, indent=2))
        fp_csv = SERIES_DIR / f\"{sk.replace('.', '_')}.csv\"
        with open(fp_csv, 'w', newline='') as f:
            w = csv.writer(f)
            w.writerow(data.get('columns', []))
            w.writerows(data.get('rows', []))

con = sqlite3.connect('/home/smnb/psxdata_rescue/psx.sqlite', timeout=30)
con.execute('PRAGMA journal_mode=WAL')
print('kibor:', sync_kibor_to_db(con))
print('policy:', sync_policy_rate_to_db(con))
"
```

### T-Bill / PIB / GIS auctions (SBP PMA scrape)
```bash
python -m pakfindata.sources.sbp_treasury
```

Fast (~10s). Fills `tbill_auctions`, `pib_auctions`, `gis_auctions` from `https://www.sbp.org.pk/dfmd/pma.asp`.

### PKRV / PKISRV / PKFRV (MUFAP yield curves)
```bash
python -m pakfindata.sources.mufap_rates sync
```

Incremental, downloads new files via the working `POST /WebRegulations/GetSecpFileById` endpoint, parses, writes to `pkrv_daily`, `pkisrv_daily`, `pkfrv_daily`.

### Other MUFAP commands
```bash
python -m pakfindata.sources.mufap_rates status        # coverage report
python -m pakfindata.sources.mufap_rates backfill-disk # download all to disk
python -m pakfindata.sources.mufap_rates backfill-db   # disk → DB
```

### Sovereign yield curve (consolidated)
```bash
python -m pakfindata.sources.sbp_rates_processor process
```

Rebuilds `sovereign_curve` from PKRV + PKISRV + MTB cutoffs + PIB cutoffs + KIBOR.

---

## 7. FX

### SBP interbank (USD only — real interbank publishes only USD)
```bash
python -c "
import sys; sys.path.insert(0, 'src')
import sqlite3
from pakfindata.sources.sbp_fx import SBPFXScraper
con = sqlite3.connect('/home/smnb/psxdata_rescue/psx.sqlite')
print(SBPFXScraper().sync_interbank(con))
"
```

Writes 1 row (USD/PKR) to `sbp_fx_interbank`. Other currencies (EUR/GBP/JPY/AED/SAR/CNY) are not published by SBP interbank — synthesize from open market (next).

### Synthesize 5-currency interbank from open market
```bash
python -c "
import sqlite3
con = sqlite3.connect('/home/smnb/psxdata_rescue/psx.sqlite', timeout=30)
con.execute('PRAGMA journal_mode=WAL')
cur = con.execute('''
INSERT OR IGNORE INTO sbp_fx_interbank (date, currency, buying, selling, mid, scraped_at)
SELECT om.date, om.currency, om.buying, om.selling,
       (om.buying + om.selling) / 2.0, 'from-open-market'
FROM sbp_fx_open_market om
LEFT JOIN sbp_fx_interbank ib ON ib.date = om.date AND ib.currency = om.currency
WHERE om.currency IN ('EUR', 'GBP', 'JPY', 'AED', 'SAR', 'CNY')
  AND ib.date IS NULL
  AND om.buying > 0 AND om.selling > 0
''')
con.commit()
print('inserted', cur.rowcount)
"
```

### Open market + kerb (forex.pk scraper)
UI button: FX Dashboard → Sync tab → "Sync Kerb (forex.pk)". CLI:
```bash
python -c "
import sys; sys.path.insert(0, 'src')
import sqlite3
from pakfindata.sources.forex_scraper import sync_kerb
con = sqlite3.connect('/home/smnb/psxdata_rescue/psx.sqlite')
print(sync_kerb(con))
"
```

Writes `forex_kerb`, `sbp_fx_open_market`, `commodity_fx_rates`.

### FX monthly + daily averages (EasyData, 71 series, ~17 min rate-limited)
Same pattern as KIBOR but with FX dataset codes:
```bash
python -c "
import sys, json, csv
sys.path.insert(0, 'src')
import sqlite3
from datetime import datetime, timedelta
from pakfindata.sources.sbp_easydata import (
    get_series_data, SERIES_DIR, sync_fx_to_db, sync_daily_fx_to_db, CATALOG_FILE,
)

with open(CATALOG_FILE) as f:
    cat = json.load(f)
keys = []
for code in ['TS_GP_ER_FAERPKR_M', 'TS_GP_ES_FADERPKR_M']:
    keys.extend(cat['datasets'][code]['series_keys'])

start = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
end = datetime.now().strftime('%Y-%m-%d')
for sk in keys:
    data = get_series_data(sk, start_date=start, end_date=end)
    if data and data.get('rows'):
        fp = SERIES_DIR / f\"{sk.replace('.', '_')}.json\"
        fp.write_text(json.dumps(data, indent=2))
        fp_csv = SERIES_DIR / f\"{sk.replace('.', '_')}.csv\"
        with open(fp_csv, 'w', newline='') as f:
            w = csv.writer(f)
            w.writerow(data.get('columns', []))
            w.writerows(data.get('rows', []))

con = sqlite3.connect('/home/smnb/psxdata_rescue/psx.sqlite', timeout=30)
con.execute('PRAGMA journal_mode=WAL')
print('monthly:', sync_fx_to_db(con))
print('daily:', sync_daily_fx_to_db(con))
"
```

Writes `sbp_fx_monthly_avg` + `sbp_fx_daily_avg`.

---

## 8. Trading sessions / 52w extremes (deep scraper)

```bash
# Scrape PSX company quote pages — slow, ~20-30 min full run
python -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.sources.deep_scraper import scrape_all_active
import sqlite3
con = sqlite3.connect('/home/smnb/psxdata_rescue/psx.sqlite')
scrape_all_active(con)
"
```

Writes `trading_sessions` (per-symbol daily snapshot with 52-week range, circuit limits, VWAP, VaR, etc.). Powers the 52w-extremes widget on Dashboard.

---

## 9. Sync state file

The dashboard reads `/mnt/e/psxdata/last_sync.json` for "X days old" banner. After any backfill that bypasses the standard sync path, refresh the file:

```bash
python -c "
import sys, sqlite3, datetime
sys.path.insert(0, 'src')
from pakfindata.services.sync_state import set_last_eod_date, set_last_intraday_date, set_last_tick_date

c = sqlite3.connect('file:/home/smnb/psxdata_rescue/psx.sqlite?mode=ro', uri=True)
set_last_eod_date(c.execute('SELECT MAX(date) FROM eod_ohlcv').fetchone()[0])
set_last_intraday_date(c.execute('SELECT MAX(date) FROM intraday_bars').fetchone()[0])

tc = sqlite3.connect('file:/home/smnb/psxdata_rescue/tick_bars.db?mode=ro', uri=True)
mx = tc.execute('SELECT MAX(ts) FROM raw_ticks').fetchone()[0]
n = tc.execute('SELECT COUNT(*) FROM raw_ticks').fetchone()[0]
set_last_tick_date(datetime.datetime.fromtimestamp(int(mx), tz=datetime.timezone.utc).strftime('%Y-%m-%d'), n)
"
```

---

## 9b. Date manifest (`/mnt/e/psxdata/date_manifest.json`)

Per-table date index used by `parquet_store.sync_missing` and the **Tick Analytics → Sync** page. When `sync_missing` reports "missing 0" but you know there's a gap, the manifest is almost always stale. Ad-hoc backfills that bypass `add_date()` (intraday JSONL ingest, market_summary CSV ingest, tick_bars rsync) silently leave it behind.

### Inspect

```bash
~/miniforge3/envs/psx/bin/python3.12 -m pakfindata.db.date_manifest show
```

Prints every tracked table with date count + oldest/newest. Flag any `newest` lagging the actual source.

### Compare manifest vs source (verify staleness before fixing)

```bash
PYTHONPATH=src ~/miniforge3/envs/psx/bin/python3.12 -c "
from pakfindata.db.date_manifest import get_dates
import sqlite3

PSX  = '/home/smnb/psxdata_rescue/psx.sqlite'
TICK = '/home/smnb/psxdata_rescue/tick_bars.db'

probes = [
    ('eod_ohlcv',           PSX,  'date'),
    ('futures_eod',         PSX,  'date'),
    ('post_close_turnover', PSX,  'date'),
    ('psx_indices',         PSX,  'index_date'),
    ('intraday_bars',       PSX,  \"SUBSTR(ts,1,10)\"),
    ('ohlcv_5s',            TICK, \"SUBSTR(ts,1,10)\"),
    ('index_ohlcv_5s',      TICK, \"SUBSTR(ts,1,10)\"),
]
for t, db, col in probes:
    md = get_dates(t)
    c = sqlite3.connect(f'file:{db}?mode=ro', uri=True)
    src_max = c.execute(f'SELECT MAX({col}) FROM {t}').fetchone()[0]
    c.close()
    flag = ' ← STALE' if md and src_max and str(src_max)[:10] > md[0] else ''
    print(f'  {t:25s} manifest={md[0] if md else \"-\":>10}  source={str(src_max)[:10]:>10}{flag}')
"
```

### Refresh stale entries (surgical, fast)

```bash
PYTHONPATH=src ~/miniforge3/envs/psx/bin/python3.12 -c "
from pakfindata.db.date_manifest import refresh_tables
print(refresh_tables(['eod_ohlcv','futures_eod','post_close_turnover',
                      'psx_indices','ohlcv_5s','index_ohlcv_5s','tick_logs']))
"
```

### Full rebuild (rescans every registered table)

```bash
~/miniforge3/envs/psx/bin/python3.12 -m pakfindata.db.date_manifest rebuild
```

### Single-table rebuild

```bash
~/miniforge3/envs/psx/bin/python3.12 -m pakfindata.db.date_manifest rebuild-one ohlcv_5s
```

### Known coverage gaps in the registry

- `index_raw_ticks` is in `parquet_store.SOURCES` but NOT in `TABLE_REGISTRY` → `get_dates()` returns `[]` → `sync_missing` always says "missing 0" for it. Use `export_table('index_raw_ticks', date)` directly until added to the registry.
- `futures_eod`, `post_close_turnover`, `psx_indices` are in the manifest but NOT in `parquet_store.SOURCES` → never exported to Parquet (intentional — SQLite-only tables).

---

## 10. DB integrity / health

### Quick check (~2 min on NVMe, ~30 min on FUSE/NTFS)
```bash
python -c "
import sqlite3
c = sqlite3.connect('file:/home/smnb/psxdata_rescue/psx.sqlite?mode=ro', uri=True)
c.execute('PRAGMA cache_size=-262144')
print(c.execute('PRAGMA quick_check(20)').fetchone())
"
```

### Full integrity check
```bash
sqlite3 /home/smnb/psxdata_rescue/psx.sqlite "PRAGMA integrity_check"
```

### REINDEX (after suspected corruption)
```bash
sqlite3 /home/smnb/psxdata_rescue/psx.sqlite "REINDEX intraday_bars; REINDEX tick_data; VACUUM;"
```

---

## 11. Rate-limit reference

- **SBP EasyData**: 250 requests/hour, 2000/day. ~14.4s between requests. **Long fetches will be slow** — prefer targeted commands.
- **PSX DPS API**: no documented limit, but use `RATE_LIMIT = 0.05s` per request, 10 workers per shard.
- **MUFAP**: no limit, fast.
- **forex.pk**: respect a few seconds between requests.

---

## 12. Common gotchas

- After bulk writes, **restart Streamlit** to flush `@st.cache_resource` connections (else `disk I/O error` on next read).
- `eod_ohlcv` (canonical) ≠ `psx_eod` (script's). Dashboards read the former.
- `sbp_fx_interbank` only has real USD/PKR; other currencies are synthesized from open market.
- `intraday_bars` ts column is **PKT timezone** by convention — JSONL ingester must convert UTC→PKT.
- `sbp_policy_rates` is recency-bound; staleness is genuine if MPC hasn't met.
