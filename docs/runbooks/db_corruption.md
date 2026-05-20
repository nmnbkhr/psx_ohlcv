# Runbook: psx.sqlite Corruption Recovery

**Last tested:** 2026-05-20 via `scripts/dr_drill.sh` against a copy of the
live DB (14 GB, 3,469,132 pages). Partial drill: scanned 18.7% of pages
in 1h09m on NTFS-via-FUSE; recovered 29.4% of total rows; high-value
tables in early pages recovered 98-100%. Full scan projected at ~5h.

> Note: The 5h projection is **NTFS-3g latency-bound**, not a recovery-tool
> limit. Running the same recovery on the NVMe ext4 mount where the live
> DB lives is expected to be ~10× faster — but the live disk doesn't have
> enough free space to host a parallel workspace. Plan accordingly.

---

## Detection signs

The following symptoms indicate database corruption:
- Streamlit shows: `Database error: file is not a database`
- `sqlite3 ~/psxdata_rescue/psx.sqlite "SELECT name FROM sqlite_master LIMIT 1;"` returns `Error: in prepare, file is not a database (26)`
- `PRAGMA integrity_check` returns anything other than `ok`
- Database file size jumped > 1 GB without expected data load
- `psx.sqlite-wal` file is larger than the main DB
- `pfsync rates sync` or any other CLI subcommand fails with sqlite3 errors at every safe_writer call

**Do NOT use `SELECT 1` to test for corruption** — it's a constant
expression sqlite evaluates without opening the file, so it returns
`1` even on a destroyed DB.

## Step-by-step recovery

### Step 1 — Stop all writers immediately

```bash
~/projects/pakfindata/scripts/stop_pakfindata.sh

# If anything is still holding the DB, force-kill:
lsof ~/psxdata_rescue/psx.sqlite
# kill -9 <pid>   for any process listed
```

Then **disable cron** so the nightly sync doesn't run mid-recovery:

```bash
# Comment out daily_sync + backup entries (prefix with #DISABLED#)
crontab -l | sed 's@^\([^#].*daily_sync\|.*backup_psx_sqlite\)@#DISABLED# \1@' | crontab -
crontab -l | grep -E "daily_sync|backup_psx"
```

### Step 2 — Preserve forensic evidence

```bash
TS=$(date +%Y%m%d_%H%M%S)
cp ~/psxdata_rescue/psx.sqlite     ~/psxdata_rescue/psx.sqlite.CORRUPT_$TS
cp ~/psxdata_rescue/psx.sqlite-wal ~/psxdata_rescue/psx.sqlite-wal.CORRUPT_$TS 2>/dev/null
cp ~/psxdata_rescue/psx.sqlite-shm ~/psxdata_rescue/psx.sqlite-shm.CORRUPT_$TS 2>/dev/null
ls -lh ~/psxdata_rescue/psx.sqlite*
```

Never delete the corrupted file. Even if recovery succeeds, keep it for
post-mortem. Existing forensic copies from May 9 2026 are at
`~/psxdata_rescue/psx.sqlite.{CORRUPT,BROKEN_REPLACED}_20260509_*` —
~29 GB each, preserved indefinitely.

### Step 3 — Try the cheap path first: latest daily backup

Daily backups live at `/mnt/e/psxdata/backups/psx_YYYYMMDD.sqlite`.

```bash
ls -lh /mnt/e/psxdata/backups/ | tail -10
LATEST_BACKUP=$(ls -t /mnt/e/psxdata/backups/psx_*.sqlite | head -1)
echo "Latest backup: $LATEST_BACKUP"

# Verify the backup is usable. Use quick_check (fast) — full
# integrity_check on a 14GB NTFS file takes 30+ minutes.
sqlite3 "$LATEST_BACKUP" "PRAGMA quick_check;" | head -3
```

**Backup age caveat:** the backup cron at `0 2 * * *` runs on a personal
laptop; if the machine was asleep at 02:00 PKT, that day's backup was
not taken. Check the cron log first:

```bash
tail -20 ~/.cron_backup.log
```

If the backup is < 24 hours old and passes `quick_check`, the fastest
recovery is to restore it and re-run today's sync:

```bash
mv ~/psxdata_rescue/psx.sqlite ~/psxdata_rescue/psx.sqlite.CORRUPT_KEEP
cp "$LATEST_BACKUP" ~/psxdata_rescue/psx.sqlite

# Re-enable cron + run the daily sync to catch up
crontab -l | sed 's/^#DISABLED# //' | crontab -
~/projects/pakfindata/scripts/daily_sync.sh
```

**Data loss with this path:** whatever changed in `data_freshness` and
the source tables between the backup snapshot and the corruption.
Typically < 24 hours of EOD + intraday + announcements work, all of
which `daily_sync.sh` will re-fetch.

### Step 4 — If backup is too old or unusable: page-level recovery

This is the path used for the May 9 2026 incident. It scans every
SQLite page in the corrupted file and rebuilds tables from raw row
data using a schema borrowed from a working DB.

```bash
cd ~/projects/pakfindata && conda activate psx

# Schema donor: a working SQLite — use the latest valid backup
SCHEMA_DB=$(ls -t /mnt/e/psxdata/backups/psx_*.sqlite | head -1)
TS=$(date +%Y%m%d_%H%M%S)
RECOVERY_DIR=/mnt/e/psxdata/recovery_$TS
mkdir -p "$RECOVERY_DIR"

# OPTIMAL FOR EMERGENCY — target high-value tables first.
# These come back at 98-100% in ~20 min based on the 2026-05-20 drill.
python scripts/sqlite_page_recover.py \
    --source ~/psxdata_rescue/psx.sqlite.CORRUPT_<timestamp> \
    --schema-db "$SCHEMA_DB" \
    --output "$RECOVERY_DIR/psx.RECOVERED.sqlite" \
    --checkpoint "$RECOVERY_DIR/recovery_progress.json" \
    --report "$RECOVERY_DIR/recovery_report.json" \
    --log-file "$RECOVERY_DIR/recovery.log" \
    --tables eod_ohlcv,futures_eod,psx_indices,pib_auctions,tbill_auctions,kibor_daily,konia_daily,sovereign_curve,pkrv_daily,sbp_policy_rates,corporate_announcements,corporate_events,dividend_payouts,company_profile,company_listing_status,symbols,sectors,tick_data
```

Then for completeness, run again WITHOUT `--tables` to recover the
remaining intraday / FX / fund tables. Resume from the checkpoint by
re-running the same command (re-uses `recovery_progress.json`):

```bash
python scripts/sqlite_page_recover.py \
    --source ~/psxdata_rescue/psx.sqlite.CORRUPT_<timestamp> \
    --schema-db "$SCHEMA_DB" \
    --output "$RECOVERY_DIR/psx.RECOVERED.sqlite" \
    --checkpoint "$RECOVERY_DIR/recovery_progress.json" \
    --report "$RECOVERY_DIR/recovery_report.json" \
    --log-file "$RECOVERY_DIR/recovery.log"
```

**Expected duration on this laptop (NTFS-3g via FUSE):**
- Targeted (--tables) recovery of 18 high-value tables: ~20-30 min
- Full scan: ~5 hours
- Resume after Ctrl-C: re-run; resumes from `last_page_scanned`

Ctrl-C is safe — the tool catches SIGTERM, finishes the current page,
writes the checkpoint, then exits cleanly. Verified in 2026-05-20 drill.

### Step 5 — Verify the recovered DB

```bash
sqlite3 "$RECOVERY_DIR/psx.RECOVERED.sqlite" "PRAGMA quick_check;" | head -3

# Spot-check critical tables — compare row counts and latest dates
# to data_freshness from the most recent backup
sqlite3 "$RECOVERY_DIR/psx.RECOVERED.sqlite" "
SELECT 'eod_ohlcv'     AS tbl, COUNT(*), MAX(date)        FROM eod_ohlcv
UNION ALL SELECT 'psx_indices',     COUNT(*), MAX(index_date)   FROM psx_indices
UNION ALL SELECT 'pib_auctions',    COUNT(*), MAX(auction_date) FROM pib_auctions
UNION ALL SELECT 'kibor_daily',     COUNT(*), MAX(date)         FROM kibor_daily
UNION ALL SELECT 'tick_data',       COUNT(*), datetime(MAX(timestamp),'unixepoch')
                                                          FROM tick_data
UNION ALL SELECT 'sovereign_curve', COUNT(*), MAX(date)         FROM sovereign_curve;"

# Compare to baseline (the latest backup)
sqlite3 "$SCHEMA_DB" "SELECT * FROM data_freshness ORDER BY domain;"
```

### Step 6 — Swap the recovered DB into place

```bash
TS=$(date +%Y%m%d_%H%M%S)
mv ~/psxdata_rescue/psx.sqlite      ~/psxdata_rescue/psx.sqlite.BROKEN_REPLACED_$TS
mv "$RECOVERY_DIR/psx.RECOVERED.sqlite" ~/psxdata_rescue/psx.sqlite

# Clear any stale WAL/SHM
rm -f ~/psxdata_rescue/psx.sqlite-wal ~/psxdata_rescue/psx.sqlite-shm
```

### Step 7 — Rebuild missing tables via the daily sync

Page-level recovery sometimes misses tables that live in late pages or
have ambiguous column signatures. Re-running the daily sync repopulates
from sources. Re-enable cron, then run once manually:

```bash
crontab -l | sed 's/^#DISABLED# //' | crontab -
crontab -l | grep daily_sync

~/projects/pakfindata/scripts/daily_sync.sh
```

The 2026-05-20 drill showed these tables would be empty after the
recovery and need the sync to repopulate:
- `eod_market_summary`, `eod_sector_summary`, `eod_symbol_summary`
  (derived; built by `pfsync summary rebuild-today`)
- `intraday_daily_summary`, `intraday_minute_breadth`, `intraday_hourly_summary`
  (derived; built by `pfsync intraday summaries-build`)
- `regular_market_snapshots` (re-populates within minutes of next
  `pfsync regular-market snapshot`)
- `sbp_fx_{daily,monthly}_avg` (re-fetch via `pfsync fx-rates sync-all`)

### Step 8 — Refresh parquet exports

`analytics_con()` reads from these — they're cached views of the SQLite tables.

```bash
python -m pakfindata.cli parquet export-all
```

This is slow (~20 min for the full set on NTFS) — can be deferred until
the next morning's cron run if the analytics pages are not urgent.

### Step 9 — Smoke test

```bash
~/projects/pakfindata/scripts/stop_pakfindata.sh
streamlit run src/pakfindata/ui/app.py --server.port 8501
```

Open Dashboard. Verify:
- Header shows latest date matching `data_freshness`
- KSE-100 hero shows numeric values, not "N/A"
- Sync expander green badges are present
- No exception in the Streamlit terminal

## Post-mortem

After any corruption incident, write a post-mortem in
`docs/incidents/YYYYMMDD_<description>.md`:

- Detection time
- Root cause (or hypothesis)
- Data loss (what time range, which tables)
- Recovery duration (from backup vs page-level)
- What prevented faster recovery
- What changes should reduce probability or impact

The May 9 2026 incident's lessons drove Phase 0:
- 0.1 SafeWriter migration → eliminates the per-row-commit-on-cached-singleton class
- 0.2 data_freshness catalog → makes pre-incident state inspectable
- 0.3 daily cron → minimizes the "what was lost since last backup" gap
- 0.4 (this milestone) → makes recovery a tested 30-min path, not a 13-hour panic

## When to rebuild from scratch (last resort)

If page-level recovery returns < 50% of expected rows in critical tables
(eod_ohlcv, intraday_bars, psx_indices), or the recovered DB fails
quick_check:

1. Restore the most recent clean backup as the base (Step 3)
2. Re-run sync for every day from the backup date through today:

   ```bash
   for d in $(seq 0 7); do
       D=$(date -d "$d days ago" +%Y-%m-%d)
       python -m pakfindata.cli market-summary day --date "$D" --import-eod
   done

   python -m pakfindata.cli summary rebuild-missing
   ```

This is slow (each historical day takes a full sync cycle) but
guaranteed to converge to a known-good state. Treasury auctions, FX
rates, and announcements are continuously re-fetchable from their
sources; intraday tick data older than 30 days is **lost** if not in
backups (PSX DPS only retains the current day's intraday API).

---

## Drill schedule

This runbook is only useful if the recovery infrastructure stays sharp.
**Run a drill every quarter** and log the results in
[`docs/operations/dr_drill_log.md`](../operations/dr_drill_log.md).

Drill command (~30 min for the cheap variant):

```bash
~/projects/pakfindata/scripts/dr_drill.sh prepare $(date +%Y%m%d)
~/projects/pakfindata/scripts/dr_drill.sh corrupt $(date +%Y%m%d)
~/projects/pakfindata/scripts/dr_drill.sh recover $(date +%Y%m%d) \
    --tables eod_ohlcv,psx_indices,kibor_daily
~/projects/pakfindata/scripts/dr_drill.sh verify  $(date +%Y%m%d)
```

The drill always runs on a workspace at `/mnt/e/psxdata/dr_drill_<date>/`.
It never touches the live database. The hard-coded safety guard
(`guard_path()`) rejects any path under `~/psxdata_rescue/`.
