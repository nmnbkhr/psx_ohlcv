#!/bin/bash
# Check whether SBP has published a fresh month for the averaged FX series.
#
# Background: on 2026-04-27 a full 71-series fetch (task bi3y9aqvj) ran cleanly
# but every inserted row was on a date <= 2026-03-31 because SBP hadn't published
# April yet. Both `sbp_fx_monthly_avg` and `sbp_fx_daily_avg` are recency-bound,
# not backfill-bound. SBP typically publishes mid-month for the prior month.
#
# Behaviour:
#   1. Check MAX(date) in both tables.
#   2. If already > 2026-03-31, skip the fetch (nothing to do).
#   3. Otherwise run the 71-series fetch (TS_GP_ER_FAERPKR_M + TS_GP_ES_FADERPKR_M),
#      then sync_fx_to_db + sync_daily_fx_to_db, then re-verify.
#
# Logged to /mnt/e/psxdata/logs/check_fx_avg_publication_<DATE>.log
# Wired to system crontab on 2026-04-27 to fire on 2026-05-05, 05-10, 05-15.

set -euo pipefail

PYTHON=/home/smnb/miniforge3/envs/psx/bin/python3.12
PROJECT_DIR=/home/smnb/projects/pakfindata
DB_PATH=/home/smnb/psxdata_rescue/psx.sqlite
LOG_DIR=/mnt/e/psxdata/logs
LOG_FILE="$LOG_DIR/check_fx_avg_publication_$(/usr/bin/date +%Y%m%d_%H%M).log"
WATERMARK="2026-03-31"

/usr/bin/mkdir -p "$LOG_DIR"

{
    echo "=== check_fx_avg_publication: $(/usr/bin/date -Iseconds) ==="
    echo "DB: $DB_PATH"
    echo "Watermark: $WATERMARK"
    echo

    cd "$PROJECT_DIR"
    PYTHONPATH=src "$PYTHON" - <<'PY'
import sqlite3, sys
DB = "/home/smnb/psxdata_rescue/psx.sqlite"
WATERMARK = "2026-03-31"

con_ro = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
m_before = con_ro.execute("SELECT MAX(date) FROM sbp_fx_monthly_avg").fetchone()[0]
d_before = con_ro.execute("SELECT MAX(date) FROM sbp_fx_daily_avg").fetchone()[0]
con_ro.close()

print(f"BEFORE: monthly_avg last={m_before}  daily_avg last={d_before}")

if (m_before or "") > WATERMARK and (d_before or "") > WATERMARK:
    print(f"Both already advanced past {WATERMARK} — nothing to do.")
    sys.exit(0)

print(f"At least one table still <= {WATERMARK} — running 71-series fetch.")
print()

from pakfindata.sources import sbp_easydata as se

cat = se._load_catalog()
if not cat:
    print("ERROR: EasyData catalog missing — run discover first.")
    sys.exit(2)

series_keys = []
for ds_code in ("TS_GP_ER_FAERPKR_M", "TS_GP_ES_FADERPKR_M"):
    ds = cat.get("datasets", {}).get(ds_code, {})
    keys = ds.get("series_keys", [])
    print(f"  {ds_code}: {len(keys)} series")
    series_keys.extend(keys)

print(f"Total: {len(series_keys)} series to fetch")
print()

import time
t0 = time.time()
ok = 0
for i, sk in enumerate(series_keys, 1):
    try:
        se.cmd_fetch_series(sk)
        ok += 1
    except Exception as e:
        print(f"  [{i}/{len(series_keys)}] ERROR {sk}: {e}")
    if i % 10 == 0:
        print(f"  [{i}/{len(series_keys)}] elapsed {int(time.time()-t0)}s")

print()
print(f"Fetched {ok}/{len(series_keys)} in {int(time.time()-t0)}s")
print()

con = sqlite3.connect(DB)
print("Running sync_fx_to_db (monthly) ...")
print(" ", se.sync_fx_to_db(con))
print("Running sync_daily_fx_to_db (daily) ...")
print(" ", se.sync_daily_fx_to_db(con))
con.close()

con_ro = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
m_after = con_ro.execute("SELECT MAX(date) FROM sbp_fx_monthly_avg").fetchone()[0]
d_after = con_ro.execute("SELECT MAX(date) FROM sbp_fx_daily_avg").fetchone()[0]
m_count = con_ro.execute("SELECT COUNT(*) FROM sbp_fx_monthly_avg").fetchone()[0]
d_count = con_ro.execute("SELECT COUNT(*) FROM sbp_fx_daily_avg").fetchone()[0]
con_ro.close()

print()
print(f"AFTER:  monthly_avg last={m_after} rows={m_count:,}  daily_avg last={d_after} rows={d_count:,}")

advanced = (m_after or "") > WATERMARK or (d_after or "") > WATERMARK
if advanced:
    print(f"VERDICT: SBP has published fresh data — watermark advanced past {WATERMARK}.")
else:
    print(f"VERDICT: still recency-bound — SBP has NOT published past {WATERMARK} yet.")
PY

    echo
    echo "=== finished: $(/usr/bin/date -Iseconds) ==="
} >> "$LOG_FILE" 2>&1
