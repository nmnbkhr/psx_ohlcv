# Session Progress — 2026-04-27

Recovery + backfill marathon. NTFS corruption rescued, NVMe migration complete, intraday + rate tables filled.

## Architecture decisions

**Split layout** (per user directive after chkdsk recovery):

| | NVMe ext4 (`/home/smnb/psxdata_rescue/`) | NTFS (`/mnt/e/psxdata/`) |
|---|---|---|
| **DBs** (latency-critical) | `psx.sqlite` (24 GB), `tick_bars.db` (6.0 GB), `commod/commod.db` (5.3 MB) | (stale duplicates pending cleanup, ~38 GB) |
| **Files** (bulk capacity) | — | `parquet/`, `intraday/`, `tick_logs_cloud/`, `sbp_easydata/`, `mufapnav/`, `market_summary/`, `eod_json/`, `commod/{pmex_ohlc,pmex_margins}/` |

`settings.py` defaults reflect this split. `PSX_DB_PATH` and `PSX_DATA_ROOT` env vars work as overrides.

## Code changes (committed in working tree, ready to git diff)

| File | Change |
|---|---|
| `src/pakfindata/sync_timeseries.py` | `load_ticks_from_disk` no longer writes to `tick_data`; `synchronous=OFF` → `NORMAL` |
| `src/pakfindata/ui/components/helpers.py` | `check_data_staleness` self-heals: cross-checks DB if state file says stale |
| `src/pakfindata/ui/page_views/futures.py` | `int(NaN)` bug fixed via `pd.notna()` (line 379) |
| `src/pakfindata/settings.py` | defaults split: `db_path=NVMe`, `data_root=/mnt/e` |
| 104 source files | `/mnt/e/psxdata` → `/home/smnb/psxdata_rescue`, then 89 reverted (file paths back to /mnt/e) |
| 9 modules with dynamic DB paths | `DATA_ROOT / "psx.sqlite"` → hardcoded NVMe path |
| `~/sync_psx_cloud.sh` | split DB_DEST/FILE_DEST, absolute paths |

## Data backfill summary

### Intraday — full 5-market coverage Apr 15-24 (was: REG-only, missing Apr 21-23)

| Date | Total rows | Symbols | Markets | Source |
|---|---|---|---|---|
| 2026-04-15 | 1,248,674 | 637 | 5 | original |
| 2026-04-16 | 1,082,673 | 629 | 5 | original |
| 2026-04-17 | 1,059,001 | 641 | 5 | original |
| 2026-04-20 | 1,443,944 | 691 | 5 | per-symbol API + JSONL top-up |
| 2026-04-21 | 1,308,398 | 709 | 5 | JSONL backfill |
| 2026-04-22 | 1,255,361 | 699 | 5 | JSONL backfill |
| 2026-04-23 | 1,204,327 | 707 | 5 | JSONL backfill |
| 2026-04-24 | 898,324 | 700 | 5 | per-symbol API + JSONL top-up |

Total ~6.1M rows added this session. TZ bug fixed afterward (5.6M rows updated UTC→PKT).

### Rate tables

| Table | Before | After | How |
|---|---|---|---|
| `kibor_daily` | 2026-04-16 (11d) | **2026-04-23 (4d)** | targeted EasyData fetch (21 series) + sync_kibor_to_db |
| `pkrv_daily` | 2026-04-20 (7d) | **2026-04-27 (0d)** | user ran `mufap_rates sync` |
| `pkisrv_daily` | 2026-04-20 | **2026-04-24** | same |
| `sbp_fx_interbank` | 2026-04-09 (18d) | **2026-04-27 (0d)** | SBPFXScraper + open-market synthesis |
| `forex_kerb` | 2026-04-20 | **2026-04-27** | UI "Sync Kerb" button |
| `sbp_fx_open_market` | 2026-04-20 | **2026-04-27** | same |
| `sbp_fx_monthly_avg` | 2026-03-31 (27d) | unchanged — RECENCY (SBP hasn't published April; verified via `bi3y9aqvj`) | — |
| `sbp_fx_daily_avg` | 2026-03-31 (27d) | unchanged — RECENCY (same) | — |
| `sbp_policy_rates` | 2026-03-09 (49d) | unchanged — RECENCY (rate genuinely hasn't changed) |
| `tbill_auctions` | 2026-04-15 (12d) | unchanged — re-run `sbp_treasury` to pick up 2026-04-22 |
| `trading_sessions` | 2026-03-29 (29d) | unchanged — needs deep_scraper (~30 min) |

### Sync state file

`/mnt/e/psxdata/last_sync.json` updated:
```json
{
  "last_eod_date": "2026-04-24",
  "last_intraday_date": "2026-04-24",
  "last_tick_date": "2026-04-24",
  "last_tick_count": 27673446
}
```

## Task `bi3y9aqvj` — FX EasyData fetch + sync — COMPLETED, no advance

- Started 02:39, finished ~02:57 (1094s = 18.2 min)
- Fetched 71 series (`TS_GP_ER_FAERPKR_M` + `TS_GP_ES_FADERPKR_M`) at SBP rate-limit (14s/req)
- Inserted 144 monthly + 2,392 daily rows — **all historical backfill**
- `sbp_fx_monthly_avg` last date still 2026-03-31; `sbp_fx_daily_avg` last date still 2026-03-31

**Verdict:** RECENCY-bound, not backfill. SBP publishes monthly/daily averages mid-month for prior month. Wait for May publication (~mid-May) before re-running. Reclassified in `memory/project_stale_rate_tables.md`.

## Known gotchas (confirmed this session)

1. **PSX DPS API only returns latest trading session** — historical per-symbol intraday cannot be fetched, must use JSONL.
2. **DPS API only returns REG market** — FUT/IDX/BNB/ODL only via JSONL.
3. **Streamlit `@st.cache_resource` SQLite connections go stale after bulk writes** → throws `disk I/O error`. Fix: restart Streamlit.
4. **`eod_ohlcv` (canonical) ≠ `psx_eod` (script's table)**.
5. **`sbp_fx_interbank` real data is USD-only** — other currencies synthesized from open market.
6. **`intraday_bars.ts` column is PKT by convention** — JSONL ingester must write PKT, not UTC.
7. **`last_sync.json` can drift** — bulk writes that bypass `eod_sync_service.py` don't update it. Now self-heals via `check_data_staleness` patch.

## NTFS incident summary

- 2026-04-21 ~22:30: NTFS MFT-bitmap corruption surfaced (`psx.sqlite.bak` "vanishing", `tick_bars.db` failing at 52KB)
- Likely root cause: SQLite `synchronous=OFF` writes via ntfs-3g/FUSE corrupted the MFT bitmap
- Rescued 60 GB of data to NVMe ext4 while reads were intermittently working
- 2026-04-22 (or thereabouts): user ran `chkdsk E: /f` from Windows
  - Result: **0 KB bad sectors** (drive hardware fine), MFT bitmap corrected, 19.6 min runtime
- The "vanished" 22 GB backup reappeared, fully readable
- Live data now on NVMe (faster anyway: 13× speedup on quick_check)

## Outstanding items

1. ~~Wait for FX fetch to finish~~ — DONE 02:57. RECENCY-bound result; wait for SBP May publication.
2. Re-run `python -m pakfindata.sources.sbp_treasury` to verify 2026-04-22 T-Bill auction lands
3. Schedule `deep_scraper` for 52w extremes (~30 min, off-hours)
4. Continue page-by-page review (user was going systematically through nav)
5. Cleanup ~38 GB of duplicate DBs on `/mnt/e` after 2026-05-07 soak window

## Reference docs created this session

- [docs/manual_sync_commands.md](manual_sync_commands.md) — full sync/backfill command reference
- [docs/session_progress_2026_04_27.md](session_progress_2026_04_27.md) — this file

## Memory entries created this session

- `memory/project_dup_dbs_cleanup.md` — 38 GB deletable on /mnt/e after soak
- `memory/project_stale_rate_tables.md` — recency vs backfill diagnosis for the 3 chronically-stale tables
- `memory/project_session_2026_04_27.md` — full memory mirror of this progress file
