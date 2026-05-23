# Cron setup — daily_sync.sh

Phase 0 Milestone 0.3 introduced unattended daily data refresh via cron.
This runbook documents the install steps, expected outputs, and recovery
procedures.

## What it does

`scripts/daily_sync.sh` runs the daily sync pipeline at 03:45 PKT each
day. Pipeline order is writers first, derived data after:

1. `regular-market snapshot`        — REGULAR MARKET table → `regular_market_current` + `_snapshots`
2. `indices sync`                   — 18 PSX indices → `psx_indices`
3. `rates sync`                     — KIBOR, KONIA, PKRV → respective tables
4. `treasury sync`                  — T-Bill + PIB auctions → `tbill_auctions`, `pib_auctions`
5. `fx-rates sync-all`              — interbank + kerb → `sbp_fx_interbank`, `forex_kerb`
6. `market-summary day --import-eod` — yesterday's .Z → CSV → `eod_ohlcv`
7. `summary rebuild-today`          — `eod_{symbol,market,sector}_summary`
8. `announcements sync`             — announcements + corporate_events + dividend_payouts
9. `intraday ticks-fetch`           — HTTP → JSON on disk
10. `intraday ticks-load`           — disk → `intraday_bars` + `tick_data`
11. `intraday summaries-build`      — `intraday_*_summary`
12. `parquet sync-missing`          — fill gaps in `/mnt/e/psxdata/parquet/`
13. WAL checkpoint (FULL) + `PRAGMA optimize`

Every CLI subcommand updates `data_freshness` in the same transaction as
its data writes, so the Dashboard's freshness badges reflect the cron's
results as soon as you open Streamlit.

The intraday date target is the **previous trading day** (via
`pakfindata.utils.trading_calendar.last_trading_day`), not literal
yesterday — handles Monday-after-weekend and post-Eid runs correctly.

## Install

```bash
# Add the daily_sync entry. Removes any prior daily_sync line first so
# re-runs of this command are idempotent.
(crontab -l 2>/dev/null | grep -v daily_sync.sh; \
 echo "45 3 * * * $HOME/projects/pakfindata/scripts/daily_sync.sh >> $HOME/.cron_daily_sync.log 2>&1") \
 | crontab -

crontab -l | grep daily_sync
```

`03:45 PKT` is chosen because:
- Market closes at 15:30 PKT; PSX EOD `.Z` files are reliably available by ~18:00 PKT.
- Nightly backup at 02:00 PKT (`scripts/backup_psx_sqlite.sh`) has 1h45m to complete before our run.
- Most users are asleep — the morning dashboard at 09:00 sees fresh data.

## Verify

```bash
# 1. Crontab entry present
crontab -l | grep daily_sync

# 2. Manual end-to-end run (will hit live APIs; allow 20-40 min)
~/projects/pakfindata/scripts/daily_sync.sh

# 3. Inspect the log
LOG_FILE="$HOME/.local/share/pakfindata/logs/daily_sync_$(date +%Y%m%d).log"
tail -50 "$LOG_FILE"

# 4. Verify catalog rows were refreshed
sqlite3 ~/psxdata_rescue/psx.sqlite "
SELECT domain, last_row_date, row_count, status, source,
       datetime(last_sync_at,'localtime') AS last_sync_local
FROM data_freshness
WHERE last_sync_at > datetime('now','-1 hour')
ORDER BY last_sync_at DESC;"

# 5. WAL should be clean (checkpoint at end of pipeline)
ls -lh ~/psxdata_rescue/psx.sqlite-wal 2>/dev/null \
    || echo "(WAL absent — checkpointed clean)"
```

## Cron-environment test (catch PATH issues)

`cron` runs with a minimal `PATH` and no shell aliases. Scripts that work
interactively can fail under cron. To reproduce cron's environment:

```bash
env -i HOME="$HOME" PATH="/usr/bin:/bin" bash -c \
    "$HOME/projects/pakfindata/scripts/daily_sync.sh"
```

The script handles this by sourcing `conda.sh` and `conda activate psx`
inside the script body — verify the log shows the conda env is active
by looking for the `python -m pakfindata.cli ...` step outputs.

## Trading-day skip

`scripts/daily_sync.sh` early-exits with `STATUS=skipped` on weekends and
PSX public holidays via `pakfindata.utils.trading_calendar.is_trading_day`.
You'll see a single line in the log:

```
SKIPPED reason=not_a_trading_day
```

Holiday list lives in `src/pakfindata/utils/trading_calendar.py`. Update
`PSX_HOLIDAYS_2026` (and add `PSX_HOLIDAYS_2027` when the new year's
calendar is published) once per January — see
https://www.psx.com.pk/psx/exchange/general/calendar-holidays

## Failure handling

`set -u` is enabled but **NOT** `set -e` — per-step failures are
intentional non-fatal: each step is logged with `STEP_FAIL` and the
pipeline continues. The catalog records `status='failed'` for the
affected dataset so the morning dashboard surfaces the issue.

If you need to alert on failures, parse the log for `STEP_FAIL` lines.
Email/Slack alerting is deferred to Phase 2.

## Recovery

If the laptop missed a night (closed lid, power outage, network down),
re-run manually. Every step is idempotent (`INSERT OR REPLACE`,
safe_writer-managed transactions, parquet `sync-missing`):

```bash
# Catch up by running the script directly.
~/projects/pakfindata/scripts/daily_sync.sh

# For backfilling several missed days, run the relevant CLI commands
# directly with --date overrides; e.g. multiple market-summary days:
for d in 2026-05-13 2026-05-14 2026-05-15; do
    python -m pakfindata.cli market-summary day --date "$d" --import-eod
done
```

## Log retention

Logs older than 30 days are auto-deleted at the end of each run:

```bash
find ~/.local/share/pakfindata/logs -maxdepth 1 \
    -name "daily_sync_*.log" -mtime +30 -delete
```

## Remove the cron entry

```bash
crontab -l | grep -v daily_sync.sh | crontab -
crontab -l
```

## Related

- `scripts/daily_sync.sh` — the orchestrator.
- `src/pakfindata/utils/trading_calendar.py` — weekend/holiday filter.
- `src/pakfindata/db/catalog.py` — `data_freshness` writer API.
- Milestone 0.2 — single source of truth catalog.
- Milestone 0.1 — `safe_writer` migration that makes every step transactional.
