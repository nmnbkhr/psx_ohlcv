# Phase 2.A — Data Coverage

**Sub-phase parent:** [`phase2_plan.md`](phase2_plan.md)
**Goal:** No empty widgets on non-DEFER pages. Validate data on the way
in (not on the way out). Restore real regressions from backup where
recoverable; document where not.
**Duration estimate:** 3 weeks.
**Starting point:** Tag `v0.1-phase1` (commit `c842cf9` on `master`).

**Phase 2.A prerequisites met:**
- ☑ safe_writer + data_freshness invariants from Phase 0
- ☑ Worker pipeline for re-runnable backfills (Phase 1.4-1.6)
- ☑ `/v1` endpoints for pages to query data quality alongside data

## Milestones

| # | Title | Effort | Dependencies |
|---|---|---:|---|
| 2.A.1 | Data quality layer — declarative validators + `data_quality` tables | 5d | v0.1-phase1 |
| 2.A.2 | Catalog pollution root-cause fix (ON CONFLICT bug) | 3d | 2.A.1 |
| 2.A.3 | Empty/regressed table audit + backfills | 5d | 2.A.2 |
| 2.A.4 | Composite-aggregator endpoint design + portfolio_scanner migration | 3d | 2.A.3 |

**Why this order:** see [`phase2_plan.md`](phase2_plan.md#2a--data-coverage-weeks-1-3).
Short version: catalog cleanup (2.A.2) before backfills (2.A.3) because
the ON CONFLICT bug actively re-poisons rows on every sync — backfilling
into a broken catalog just generates fresh ZUMA / TBILL / MUFAP / WTL
rows.

---

## Milestone 2.A.1 — Data quality layer

**Design principle: declarative, not imperative.** A validator is a row
in a config table, not a function buried in code. Adding a new check =
`INSERT INTO data_quality_rules`. No redeploy. Introspectable from a
Grafana dashboard (Phase 2.B) without parsing source code.

### Schema

```sql
-- One row per validation rule. Author writes rules; engine applies them.
CREATE TABLE data_quality_rules (
    rule_id          INTEGER PRIMARY KEY,
    domain           TEXT NOT NULL,       -- matches data_freshness.domain
    check_type       TEXT NOT NULL,       -- one of the registered types below
    params           TEXT NOT NULL,       -- JSON config for the check
    severity         TEXT NOT NULL,       -- 'error' | 'warn' | 'info'
    enabled          INTEGER NOT NULL DEFAULT 1,
    description      TEXT,                -- human-readable
    created_at       TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(domain, check_type, params)
);

-- One row per (rule × execution). Engine writes; dashboards read.
CREATE TABLE data_quality_results (
    rule_id          INTEGER NOT NULL,    -- → data_quality_rules.rule_id
    domain           TEXT NOT NULL,
    checked_at       TEXT NOT NULL DEFAULT (datetime('now')),
    status           TEXT NOT NULL,       -- 'pass' | 'fail' | 'error'
    actual           TEXT,                -- what the check actually saw (JSON)
    duration_ms      INTEGER,
    PRIMARY KEY(rule_id, checked_at)
);
```

### Example rule rows

```
domain          | check_type      | params                              | severity
----------------+-----------------+-------------------------------------+--------
psx_indices     | date_format     | {"column":"index_date"}             | error
psx_indices     | row_count_min   | {"min":5}                           | warn
konia_daily     | range           | {"column":"rate_pct","min":0,"max":50} | error
fx_pairs        | reference       | {"column":"base","table":"symbols","ref":"symbol"} | error
tick_data       | not_empty       | {}                                  | warn
sovereign_curve | source_coverage | {"sources":["PKRV","PKISRV","MTB","PIB","KIBOR","POLICY"]} | warn
eod_ohlcv       | range           | {"column":"close","min":0.01,"max":1000000} | error
```

### Check-type registry

`src/pakfindata/quality/checks.py` — one function per `check_type`.
**This** is the only place code changes when *new* check categories
appear. New rules in existing categories are pure config:

| `check_type` | What it asserts | Params |
|---|---|---|
| `date_format` | Column values are valid `YYYY-MM-DD` (or `YYYY-MM-DDTHH:MM:SS`) | `column` |
| `row_count_min` | Table has at least N rows | `min` |
| `range` | Numeric column is within `[min, max]` | `column`, `min`, `max` |
| `reference` | Foreign-key-like: every value in `column` exists in `table.ref` | `column`, `table`, `ref` |
| `not_empty` | Table has > 0 rows | — |
| `not_null` | `column` has no NULLs (or fraction below threshold) | `column`, `max_null_fraction` |
| `source_coverage` | `sovereign_curve.source` contains all listed values for the latest date | `sources` |
| `monotonic` | Column is non-decreasing for a given partition | `column`, `partition_by` |
| `custom_sql` | Arbitrary SELECT — passes iff returns 0 rows | `sql` |

`custom_sql` is the escape hatch for one-offs that don't justify a
new registered type. Used sparingly — the goal is for 95% of rules to
fit the named categories.

### Engine integration: validators run INSIDE safe_writer

This is the key architectural addition. Phase 0.2 added the catalog
write to the same transaction as the sync. Phase 2.A.1 adds the
validators to the same transaction:

```
safe_writer transaction:
  1. Sync writes (rows into base table)
  2. catalog update (data_freshness row)
  3. NEW: run validators for this domain
  4. NEW: write rows to data_quality_results
  5. NEW: if any 'error'-severity rule failed → log + raise
          (transaction rolls back, sync recorded as failed)
COMMIT
```

This catches pollution *before* it persists. If today's `sync_indices`
writes a row with `index_date='WTL'`, the `date_format` rule sees it,
the transaction rolls back, `data_freshness` records the failure, and
the morning dashboard surfaces it — instead of the row sitting in the
DB until someone notices a Grafana panel six months later.

### Page integration: graceful degradation

Pages call `/v1/quality/<domain>` alongside `/v1/freshness`. If any
`error`-severity rule for the domain is failing, the page renders a
banner:

> ⚠️ Treasury data failed validation 2 minutes ago. Showing data as of
> last known-good snapshot (2026-05-22).

Pages refuse to render fabricated values. No `mid = (buy + sell) / 2`
when one side is None. No silent stale-row carry-forward.

### Step 0

```bash
echo "═══ 1. Existing pollution to use as test cases ═══"
sqlite3 ~/psxdata_rescue/psx.sqlite "
SELECT domain, last_row_date, row_count
FROM data_freshness
WHERE last_row_date NOT GLOB '????-??-??'
   AND last_row_date IS NOT NULL;"

echo ""
echo "═══ 2. Empty-table candidates for not_empty / row_count_min ═══"
sqlite3 ~/psxdata_rescue/psx.sqlite "
SELECT domain, row_count
FROM data_freshness
WHERE row_count = 0
ORDER BY domain;"

echo ""
echo "═══ 3. Range-check candidates: rate columns with suspicious values ═══"
sqlite3 ~/psxdata_rescue/psx.sqlite "
SELECT MIN(rate_pct), MAX(rate_pct), AVG(rate_pct) FROM konia_daily;
SELECT MIN(close), MAX(close), AVG(close) FROM eod_ohlcv;"
```

### Sub-waves

- **2.A.1.1 — Schema + check-type registry.** Create `data_quality_rules`
  + `data_quality_results` via Alembic-style migration. Implement the 8
  named check types + `custom_sql` escape hatch.
- **2.A.1.2 — Engine integration.** Wire `run_validators(con, domain)`
  into the safe_writer block in each `etl/<domain>.py`. One function,
  called from N etl modules. Rule failures with severity='error' cause
  rollback; 'warn' and 'info' log only.
- **2.A.1.3 — Seed rules.** Insert ~30 rules covering the catalog
  pollution cases (date_format on every catalog domain) + empty-table
  warnings + obvious range checks (KIBOR < 50%, EOD close > 0).
- **2.A.1.4 — `/v1/quality/<domain>` endpoint + UI banner helper.**
  Page-level graceful degradation.

### Exit criteria

- `data_quality_rules` populated with ≥30 seed rules
- Every `etl/<domain>.sync()` runs validators in its safe_writer block
- A fail-with-severity=error rolls back the sync and records failure
  in `data_freshness`
- `/v1/quality/<domain>` returns the latest results
- At least one page (suggest: dashboard.py) shows the validation
  banner when its dataset's rules fail

### What NOT to do in 2.A.1

- Don't write Python validator subclasses with inheritance. Declarative
  rule rows + dispatch on `check_type` is the entire pattern.
- Don't run validators outside safe_writer transactions (they become
  cosmetic and pollution sneaks back in).
- Don't add per-page custom check types in the registry. Use
  `custom_sql` for one-offs.
- Don't backfill data yet — that's 2.A.3. 2.A.1's job is the framework
  and seed rules; 2.A.3 fills the tables those rules then validate.

---

## Milestone 2.A.2 — Catalog pollution root-cause fix

**The bug** (Phase 0.3 known_debt, confirmed live in Milestone 1.8.2):

`db/catalog.py::update_catalog` issues:

```sql
INSERT INTO data_freshness (domain, date_column, source_table, ...)
VALUES (?, ?, ?, ...)
ON CONFLICT(domain) DO UPDATE SET
    last_sync_at = excluded.last_sync_at,
    last_row_date = excluded.last_row_date,
    row_count = excluded.row_count,
    status = excluded.status
    -- BUG: date_column and source_table NOT in SET clause
```

So when Phase 0.2 backfill inserted rows like
`(domain='announcements', date_column='announcement_date', ...)`, the
ON CONFLICT path *kept the original* `date_column='date'` and overwrote
only the other fields. Subsequent catalog queries then ran
`SELECT MAX(date) FROM announcements` against a table with no `date`
column — query errors, then the alphabetical-MAX of whatever string
column SQLite picked, producing the ZUMA / WTL / TBILL / MUFAP
artifacts.

### Step 0

```bash
echo "═══ Current catalog.py update_catalog ═══"
grep -n "ON CONFLICT\|update_catalog\|date_column\|source_table" \
    src/pakfindata/db/catalog.py | head -40

echo ""
echo "═══ Every poisoned row + what its correct date_column should be ═══"
sqlite3 ~/psxdata_rescue/psx.sqlite "
SELECT domain, date_column, last_row_date
FROM data_freshness
WHERE last_row_date NOT GLOB '????-??-??';"
```

### Sub-waves

- **2.A.2.1 — Reproducer test.** Pytest case: insert a domain row with
  `date_column='announcement_date'`, then call `update_catalog` again
  with no `date_column` parameter, then assert the row still says
  `announcement_date`. Currently fails.
- **2.A.2.2 — Fix.** Add `date_column = excluded.date_column,
  source_table = excluded.source_table` to the SET clause. Tiny diff,
  one line plus tests.
- **2.A.2.3 — One-shot cleanup.** Script
  `scripts/repair_catalog_date_columns.py` that maps the known
  pollution rows to their correct date_column values per CLAUDE.md
  table list:

```
domain                    | correct date_column
--------------------------+------------------
announcements             | announcement_date
tick_data                 | timestamp (unix epoch — needs special handling)
fx_kerb                   | rate_date
konia                     | rate_date
pib                       | auction_date
instrument_membership     | effective_date
regular_market_current    | snapshot_ts
```

Run once, verify, then `data_freshness` is clean.

### Exit criteria

- Reproducer test passes (i.e. the bug is fixed)
- `SELECT * FROM data_freshness WHERE last_row_date NOT GLOB
  '????-??-??'` returns 0 rows
- A fresh `pfsync indices sync` does not re-poison
  `regular_market_current` (regression test from Milestone 1.8.2 finding)

### What NOT to do in 2.A.2

- Don't change `update_catalog`'s signature — only the SET clause.
  Other callers stay unaffected.
- Don't try to fix the pollution by backfilling first — the bug
  reappears immediately. 2.A.2 has to land before 2.A.3 by
  construction.

---

## Milestone 2.A.3 — Empty / regressed table audit + backfills

**Three investigation findings from Phase 2 Step 0 land here:**

### 2.A.3.1 — `tbill_auctions` restoration from backup

**State:** 12 rows (CLAUDE.md 2026-04-20 audit said 175). Loss happened
during Phase 0/1. Restoration is a backup-pull, not a re-sync.

```bash
# Bisect window: which backup still has 175 rows?
for d in 20260511 20260514 20260515 20260517 20260520; do
    f="/mnt/e/psxdata/backups/psx_${d}.sqlite"
    if [ -r "$f" ]; then
        count=$(sqlite3 "$f" "SELECT COUNT(*) FROM tbill_auctions;" 2>/dev/null)
        echo "  $f: $count rows"
    fi
done
```

Most-recent backup with ≥175 rows is the source of truth. Steps:

1. `cp /mnt/e/psxdata/backups/psx_<date>.sqlite /tmp/tbill_recover.sqlite`
2. Extract: `sqlite3 /tmp/tbill_recover.sqlite ".dump tbill_auctions" > /tmp/tbill_dump.sql`
3. Diff against current: identify which rows are missing
4. Re-insert the missing rows via safe_writer ALL-OR-NOTHING transaction
5. Validators (2.A.1) catch any garbage on the way in

Time estimate: 30 minutes once the bisect resolves.

### 2.A.3.2 — `pkisrv_daily` investigation

**State:** 0 rows. CLAUDE.md says 1.5K rows historical. `sovereign_curve`
consolidates PKISRV as a source.

**First question:** was this table ever populated post-May-9 recovery?

```bash
for d in 20260511 20260514 20260520; do
    f="/mnt/e/psxdata/backups/psx_${d}.sqlite"
    if [ -r "$f" ]; then
        count=$(sqlite3 "$f" "SELECT COUNT(*) FROM pkisrv_daily;" 2>/dev/null)
        echo "  $f: $count rows"
    fi
done

# Also check: does sovereign_curve have PKISRV rows?
sqlite3 ~/psxdata_rescue/psx.sqlite "
SELECT COUNT(*), MIN(date), MAX(date)
FROM sovereign_curve
WHERE source IN ('PKISRV', 'PKISRV_SYN');"
```

Three possible outcomes, each with a different action:

| Backup state | sovereign_curve state | Action |
|---|---|---|
| backup has PKISRV rows | curve has PKISRV | restore from backup (like tbill) |
| backup has PKISRV rows | curve has only PKISRV_SYN | restore from backup; investigate why fast-sync skips |
| backup empty too | curve empty PKISRV | upstream MUFAP issue; re-sync via `python -m pakfindata.sources.mufap_rates sync` |
| backup empty too | curve has PKISRV_SYN only | document as upstream-broken, propose Phase 3 deprecation |

### 2.A.3.3 — `sbp_fx_interbank` investigation

**State:** 1 row (USD). CLAUDE.md 2026-04-20 said 127 rows.

Same shape as tbill: bisect backups; restore if recoverable. Distinct
from `sbp_fx_open_market` (23 rows) and `sbp_fx_daily_avg` (72K rows,
healthy). FX Dashboard page renders empty in interbank tab today.

### 2.A.3.4 — Remaining coverage gaps + audit script

For each table the validator framework flags `row_count_min` or
`not_empty` failures, decide per-table:

| Table | Today | Plan |
|---|---|---|
| `gis_auctions` | 61 rows, stale 2023-12 | re-sync via `sbp_treasury`; KEEP if upstream confirmed silent |
| `corporate_events` | 43 rows (date in 2026-10?) | audit data — date column probably broken |
| `mutual_funds` | 0 rows | metadata table; investigate scraper |
| `pkisrv_daily` | covered in 2.A.3.2 | — |
| `sukuk` | 0 rows | investigate scraper |
| `commodities` (catalog domain) | 0 / unknown | scraper status — pmex page is owner |

**Audit script** `scripts/coverage_audit.py` — iterates every table
in `data_freshness`, surfaces row-count + last-row-date + validator
status. Run weekly via cron once 2.A.3 closes. (Cron addition, but
read-only — safe.)

### 2.A.3.5 — broken-dep DEFER pages from Phase 1.7

Three tables to backfill that unblock pages:

| Page | Missing table | Plan |
|---|---|---|
| `ftp_monitor` | `ftp_rates` | Investigate ALM-engine writer; never populated — likely Phase 3 work |
| `website_scan` | `sources/sectors.py` broken scraper | Fix or replace; KEEP if no maintainer |
| `instruments` | thin reads | small migration to `/v1/instruments`; not strictly a coverage issue |

### Step 0

```bash
echo "═══ Inventory backups ═══"
ls -la /mnt/e/psxdata/backups/psx_*.sqlite | head -10

echo ""
echo "═══ Confirm 2.A.2 is closed (no pollution remaining) ═══"
sqlite3 ~/psxdata_rescue/psx.sqlite "
SELECT COUNT(*) FROM data_freshness WHERE last_row_date NOT GLOB '????-??-??';"
# Expect 0 — if not, 2.A.2 needs more work; STOP and finish 2.A.2 first.
```

### Sub-waves

- **2.A.3.1 — tbill_auctions restoration** (per above)
- **2.A.3.2 — pkisrv_daily investigation + restoration** (per above)
- **2.A.3.3 — sbp_fx_interbank investigation + restoration**
- **2.A.3.4 — Remaining coverage gaps + audit script**
- **2.A.3.5 — broken-dep DEFER page enablement**

### Exit criteria

- All previously-empty tables either have data OR documented "won't fix"
- `coverage_audit.py` runs weekly via cron
- ftp_monitor, website_scan, instruments pages either render or have
  a documented permanent-DEFER reason
- Validators from 2.A.1 still all pass (the backfills don't re-pollute)

### What NOT to do in 2.A.3

- Don't backfill without 2.A.2 closed first. (Step 0 guard above.)
- Don't write per-table backfill scripts that aren't re-runnable —
  every restore must be idempotent.
- Don't extend the worker handler registry just to schedule backfills.
  One-shot CLI is fine for restoration; only schedule via worker if
  the table needs ongoing re-sync.

---

## Milestone 2.A.4 — Composite-aggregator endpoint design + portfolio_scanner

Two Phase 1.7 deferrals close here.

### Composite-aggregator pattern (market_research + futures)

Phase 1.7 Group G surfaced this category: pages that compose
cross-domain analytical queries don't fit per-domain `/v1` endpoints
without ballooning the API surface.

**Pattern: `/v1/dashboard/<page>` endpoints.** One endpoint per page,
returns the composite view that page renders. Trade-off:

- ✅ API surface stays sane (one endpoint per dashboard, not 13 per page)
- ✅ Page-level caching is straightforward
- ✅ Engine-level invariants stay enforced (safe_writer + data_freshness)
- ❌ Endpoint shape is page-specific (couples API to UI somewhat)
- ❌ Reuse across pages is low (it's the whole point — each page has
  its own composite query)

**Mitigation for the coupling concern:** the endpoint composes from
existing per-domain methods *internally*. Page-shape lives in the
endpoint; data access stays domain-scoped. If `market_research` and
`futures` ever need the same sub-aggregate, that sub-aggregate gets
its own per-domain method, and both endpoints call it.

### Sub-waves

- **2.A.4.1 — Prototype `/v1/dashboard/research`** for `market_research`
  (39 reads, 13 `_load_*`). Aim: page renders identically; backend hides
  the composition.
- **2.A.4.2 — Pattern documentation** in
  `docs/architecture/composite_endpoints.md`. When to use; when not to.
- **2.A.4.3 — Apply to `futures`** as the second instance. Verify the
  pattern holds.
- **2.A.4.4 — `portfolio_scanner` migration** (Phase 1.7 untouched). Small
  surface; could fit per-domain endpoints or use the composite pattern —
  decide during prototype.

### Step 0

```bash
echo "═══ market_research direct DB reads ═══"
grep -nE "connect\(|analytics_con|sqlite3\." \
    src/pakfindata/ui/page_views/market_research.py | head -20

echo ""
echo "═══ Existing reusable methods that already compose ═══"
grep -nE "def _load|def fetch_" \
    src/pakfindata/ui/page_views/market_research.py | head -20
```

### Exit criteria

- `market_research` and `futures` pages off direct DB reads (composite
  endpoints serve them)
- Composite-pattern doc exists with examples + when-to-use criteria
- `portfolio_scanner` no longer in "untouched" status

### What NOT to do in 2.A.4

- Don't template the pattern after one example. Build two prototypes
  (2.A.4.1 + 2.A.4.3), THEN document.
- Don't pull `/v1/dashboard/<page>` endpoints into the per-domain
  routes. They're page-shape; keep them in their own
  `api/routes/dashboard.py`.

---

## Phase 2.A exit criteria (rollup)

- ☐ Data quality layer operational (rules + validator engine + UI banners)
- ☐ Catalog pollution root cause fixed; `data_freshness` clean
- ☐ Empty/regressed tables restored or documented won't-fix
- ☐ Composite-aggregator pattern documented and applied to 2 pages
- ☐ portfolio_scanner no longer in untouched bucket
- ☐ Every Phase 1.7 broken-dep DEFER page has a final resolution

When all 5 boxes check, send the Phase 2.B kickoff prompt.
