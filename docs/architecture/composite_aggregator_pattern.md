# Composite-Aggregator Endpoint Pattern

**Phase 2.A.4 — 2026-05-23**

Rules for designing /v1 endpoints that serve cross-domain analytical views.
Codified BEFORE prototypes (2.A.4.2 + 2.A.4.3 + 2.A.4.3b) so the patterns
those prototypes test are explicit, not emergent. If implementation forces
revision, the doc gets updated alongside the implementation commit
(`docs(architecture): composite_aggregator_pattern revisions from
implementation` is sub-wave 2.A.4.4).

This doc grounds itself in the seven conflicts surfaced by the Step 0
audit at `/mnt/e/psxdata/phase2_a4_audit_20260523.md`. Each rule below
that resolves a conflict carries an inline note (`(Conflict X)`).

---

## 1. When to use

A composite-aggregator endpoint serves a *domain-scoped data view* that
combines reads from 3+ source tables — or 2 tables joined non-trivially —
and produces a structured response that no single Phase 1.2 per-domain
endpoint can serve cleanly.

Use it when ALL of these are true:

- The view is genuinely cross-domain (eod + sectors + trading_sessions, or
  futures_eod + eod_ohlcv, etc.) — not just "two columns from one table"
- Server-side aggregation produces a smaller payload than the equivalent
  client-side fan-out across multiple per-domain endpoints
- The view has a *semantic name* that doesn't reference a UI page or widget
- The query is sqlite-tractable (sub-second) OR genuinely warrants DuckDB
  (see §6)
- It's read-only

## 2. When NOT to use

- **Single-domain pages.** Use the existing Phase 1.2 per-domain endpoints.
  Adding a `/v1/composite/foo-summary` for what `/v1/foo/summary` already
  serves is duplication.
- **Heavy compute (>5s).** Route through the worker queue, return `job_id`,
  let the client poll. Composites are synchronous request-response.
- **UI-widget-shaped views.** `/v1/research/dashboard-card-3` is the
  anti-pattern. (Conflict B: this discipline matters because the Step 0
  audit found existing /v1/market/* endpoints that are already
  per-widget-shaped; adding more on top of them would compound the issue.)
- **Engine-call-only pages.** Per CLAUDE.md, pages that read raw bars and
  feed engine modules keep direct reads. They aren't composite candidates.
- **File-fed pages** (`live_ticker.py`, `portfolio_scanner.py`,
  `intraday_quant_lab.py`). Not composite-pattern fit. (Conflict, scope-5.)

## 3. Naming convention

`/v1/<domain-scope>/<view-name>` where:

- **`<domain-scope>`** is the SEMANTIC domain of the view, not a UI page name.
  - Good: `/v1/research/`, `/v1/derivatives/`, `/v1/macro/`
  - Bad: `/v1/market_research/`, `/v1/futures_page/`, `/v1/dashboard/`

- **`<view-name>`** describes the DATA SHAPE, not the widget consuming it.
  - Good: `movers-enriched`, `oi-buildup`, `breadth-history`
  - Bad: `dashboard-card-3`, `top-table-widget`, `main-view`

**The test** (Conflict C): every proposed endpoint name needs an
answer to *"would another consumer plausibly want this same view?"*
If you can't name a non-current-page consumer that would naturally
reach for the endpoint, the name is wrong. Concrete examples for the
2.A.4 prototypes:

- `/v1/research/movers-enriched` — a CLI report wanting today's
  gainers with P/E; the `stock_screener` page if it adds a P/E filter;
  an external API consumer doing daily market scans.
- `/v1/derivatives/overview` — a daily-recap script for the trading
  desk; an external integration computing derivatives positioning.

The 2.A.4 audit explicitly rejected `/v1/research/overview` as a name
because "overview" of "research" is a god endpoint — it would bundle
indices + breadth + sectors + movers + macro + fixed-income + funds.
That's the anti-pattern.

## 4. Parameter design

Soft ceiling: **≤ 6 params per endpoint**. If a param set grows past 6,
the endpoint is probably trying to be two endpoints; split it.

Categories (use what fits; don't force unused ones):

- **Time scope:** `from`, `to`, `date`, `period`
- **Domain scope:** `sector`, `symbol`, `instrument_type`
- **Granularity:** `interval`, `top_n`, `limit`
- **Filter:** small set of enum-typed flags

All identifier-like params (table names, column names) MUST be allowlisted
via the existing `_validate_identifier` helper or a regex pattern on the
FastAPI Query — same discipline as Phase 1.7 (`metric` regex in
`/v1/funds/performance/leaders`, etc.).

## 5. Response shape

Composite responses are STRUCTURED, not flat lists. The response is an
object with named sub-objects (or named arrays). Flat lists lose the
multi-domain shape and force the client to recompute structure.

Required top-level fields:

- **`as_of`** — the date the response is computed AGAINST (typically the
  latest source-table date). Clients render this in the UI badge.
- **`data_quality`** — per-source freshness + validator status (see §7).

Additional fields are domain-specific. Example skeleton:

```json
{
  "as_of": "2026-05-23",
  "summary": { ... },        // single-row aggregates
  "rows": [ ... ],           // tabular data
  "by_sector": [ ... ],      // optional named breakdown
  "data_quality": {
    "eod_ohlcv": {"status": "ok", "last_row_date": "2026-05-23"},
    "trading_sessions": {"status": "stale", "last_row_date": "2026-03-29", "days_stale": 55}
  }
}
```

## 6. SQLite vs DuckDB — per-endpoint decision, not pattern-default

(Conflict G + user observation: don't make analytics_con the default.)

The codebase has two read-side connection providers:

- **`get_read_db()`** — SQLite `?mode=ro`. Used by 150+ /v1 endpoints. Fast
  for indexed lookups + small JOINs + GROUP BYs on the canonical tables.
- **`get_analytics_con()`** — DuckDB in-memory + Parquet views. Used by 2
  endpoints today (`/v1/tick-logs/dates`, admin route). Right for large
  GROUP BYs over partitioned Parquet, time-series aggregations that
  benefit from vectorized execution, or queries against datasets that
  outgrew SQLite.

**Use SQLite by default for composites.** Promote to DuckDB only when the
query is genuinely heavy. The first /v1 route to use `analytics_con` was
tick-logs (Phase 1.7 Group F) — a multi-million-row Parquet read. That's
the right shape for DuckDB. The 2.A.4 prototypes (movers-enriched,
derivatives-overview) are sub-second sqlite joins — they don't need it.

DuckDB is for queries that need it. Composites are not automatically
DuckDB.

## 7. data_quality integration

Composite responses surface a `data_quality` field with one entry per
source table the composite reads from. Each entry has:

- **`status`** — one of:
  - `ok` — catalog says ok AND last_row_date is recent (within domain's
    expected freshness)
  - `stale` — catalog ok but data is older than the domain expects
  - `failed` — catalog says failed (e.g., 4 FOLLOWUP-2 rows)
  - `unknown` — catalog says unknown
  - `not_available` — source isn't in DB at all (e.g., disk XLS — see §8)

Plus at least one field that supports the status — which one depends
on the source. Treat `data_quality` entries as a small bag of optional
keys (`last_row_date`, `days_stale`, `row_count`, `source_path_pattern`)
where each composite picks the keys that make sense for the table.
Pydantic schemas keep them all `Optional` so the response shape stays
stable while the populated keys vary.

Typical combinations seen in the 2.A.4 prototypes:

- **Dated source with catalog row** (e.g. `eod_ohlcv` via the
  `equity_eod` catalog row): `status` + `last_row_date`. The catalog
  row is the source of truth; freshness is implicit.
- **Dated source without a catalog row** (e.g. `trading_sessions`):
  `status` + `last_row_date` + `days_stale`. Compute `days_stale`
  against today; status flips to `stale` when it exceeds the per-domain
  threshold (typical: 7 days).
- **Date-less source** (e.g. `sectors`, which has no date column):
  `status` + `row_count`. Use row count as the only available signal.
- **Disk-only source** (e.g. OI XLS — see §8): `status='not_available'`
  + `source_path_pattern` showing the file location. No date, no count
  — the composite can't read the file at request time (§8).

The UI uses `data_quality` to render per-section banners. If
`data_quality.trading_sessions.status == 'stale'`, the page renders
"P/E data 55 days old" above the movers table. (Conflict E resolution.)

(Conflict D: this is honest about what we have. Phase 2.A.1 validators
exist for 4 domains today. For source tables WITHOUT validators, the
`status` derives from `data_freshness` only — that's still useful signal.)

## 8. Composites are DB-native — no disk reads at request time

(Conflict F resolution.)

A composite endpoint reads ONLY from DB (SQLite or DuckDB). If a data
source lives only on disk (XLS files, CSVs, JSONL), the composite does
NOT read it at request time. Three reasons:

- **Request-time IO outside the DB layer breaks the layering rule** (§10).
- **Latency is unpredictable** — file glob + parse can be tens to hundreds
  of milliseconds, and varies with file size.
- **Cache invalidation gets complex** — adding non-DB sources couples the
  cache key to file-system state.

When a composite needs data from a disk-only source, the response surfaces
`data_quality.<source>.status = 'not_available'` and either:

- Omits the section that depends on the disk source (preferred when the
  rest of the response is still useful)
- Returns empty for that section's array

Getting the data into DB is its own milestone — scraper / loader work
under Phase 2.A.5 (or later). Composites adopt the data once it's in DB.

Example: `/v1/derivatives/overview` does NOT include the OI (Open
Interest) section in 2.A.4.3, because OI lives only in disk XLS at
`/mnt/e/psxdata/downloads/daily/<date>/futures/futures_oi_dfc_*.xls`.
Response carries `data_quality.oi = {"status": "not_available", "source_path_pattern": "..."}`.
When OI gets a DB table (Phase 2.A.5+), the endpoint adds the section
and the status flips to `ok` — no breaking change.

## 9. Caching

Cache at the API layer when:

- The param set is small (≤6 params)
- The underlying data has a known freshness signal (`data_freshness.last_sync_at`)
- The cache key includes both params AND a freshness fingerprint

In Phase 2.A.4: **FastAPI's in-process / per-process caching primitives only.
NO Redis.** Redis is Phase 3 infrastructure. Don't add it as a forward-
compatibility option; treat it as out of scope until the infrastructure
work that introduces it.

If a per-process cache becomes a bottleneck under real load, that's signal
to revisit in Phase 2.B (observability) or Phase 3 (multi-tenant) — not
to bolt on Redis now.

## 10. Layering rule

(Conflict A resolution: option B — `db/repositories/composites/`.)

Composite endpoint code lives in three files, one per concern:

```
src/pakfindata/api/routes/<domain>.py
    — thin route handler: validate params, call repo, attach data_quality,
      return. Should be ≤ 20 lines per endpoint.

src/pakfindata/db/repositories/composites/<view>.py
    — repository function: owns the SQL/DuckDB logic for ONE composite
      view. Reusable from non-API call sites (CLI handlers, worker jobs)
      if needed.

src/pakfindata/api/schemas/<domain>.py
    — Pydantic response models. Owns the response shape. Lives alongside
      existing per-domain schemas; can extend an existing file or be new.
```

The `composites/` subdirectory under `db/repositories/` is new (this doc
introduces it). It signals "repository code, specifically composite views"
without inventing a parallel top-level layer. Per-domain repos stay where
they are at `db/repositories/<domain>.py`.

(Conflict A: the prompt sketch proposed a new `db/queries/` top-level
directory. The codebase already has `db/repositories/` as the established
convention with 32+ modules. A parallel top-level layer would create two
ways to put DB logic. The `composites/` subdirectory keeps composites
grouped and discoverable without that fragmentation.)

## 11. What composites do NOT do

- **Write to DB.** Composites are read-only. No safe_writer in handler code,
  no inserts/updates/deletes.
- **Modify worker handlers.** Phase 2.A.4 is read-side; the worker registry
  isn't touched.
- **Replace per-domain endpoints.** Composites are ADDITIVE. The Phase 1.2
  per-domain endpoints stay, and clients can mix per-domain + composite
  reads as makes sense for their use case.
- **Live in the UI layer.** No `pakfindata.ui.api.client` helper that
  fans out to 5 endpoints client-side and stitches the response. If you
  need that, build a composite endpoint instead. (The fan-out is the
  anti-pattern this whole milestone exists to avoid.)
- **Modify `engine/` modules.** Composites can READ via engine modules if
  needed, but cannot change them in this milestone.

## 12. Scope of this doc

This pattern doc covers the patterns the 2.A.4 prototypes use
(`/v1/research/movers-enriched`, `/v1/derivatives/overview`,
`/v1/funds/category-summary` as a Phase-1.2 tagalong). It does NOT
attempt to anticipate every future case.

Future composite work in Phase 2.B (observability dashboards), Phase 2.C
(scope-v2 intraday), or Phase 3 (multi-tenant) refines this doc as the
corpus grows. Each new composite either fits an existing rule here or
forces a deliberate revision — both outcomes are healthy; silent drift
isn't.

---

## Appendix A — Conflict resolutions index

The 2.A.4 Step 0 audit surfaced seven conflicts between the prompt-doc's
pattern sketch (`PHASE2_A4_COMPOSITE.md`) and the codebase reality. Each
is resolved above. Index for future readers:

| Conflict | Resolved in | Choice |
|---|---|---|
| A — Layering rule (`db/queries/` vs `db/repositories/`) | §10 | `db/repositories/composites/<view>.py` (option B) |
| B — Scope premise (4 vs 11 load functions in market_research) | §1, §2 | Build one true composite (`movers-enriched`); others use existing per-domain endpoints |
| C — Naming applied to concrete domains | §3 | `/v1/research/movers-enriched` + `/v1/derivatives/overview`; reject `/v1/research/overview` as god endpoint |
| D — data_quality assumes most domains have validators | §7 | Surface what we have (validators + freshness); honest about what we don't |
| E — trading_sessions 55-day staleness | §7 | Surface staleness in `data_quality`; don't defer the endpoint |
| F — OI XLS is disk-only | §8 | Composites are DB-native; OI excluded from prototype; flagged `not_available` |
| G — analytics_con as default for "heavy aggregation" | §6 | Per-endpoint decision; SQLite default; DuckDB only when genuinely heavy |

---

## Appendix B — When the pattern needs to evolve

If you're about to write a new composite endpoint and:

- The view doesn't fit `<domain-scope>/<view-name>` naturally → name is
  probably wrong, or the endpoint isn't a composite
- The param count is creeping toward 8+ → split, don't bend §4
- The response is a flat list of dicts → restructure with §5
- You're tempted to read from disk for "just this one section" → §8 says no
- You're reaching for DuckDB because "composites are heavy" → §6 says
  measure first
- You're tempted to add Redis "since we're caching anyway" → §9 says no

If a real engineering need pushes against one of these rules, that's
signal to revise the doc deliberately, not to bend it silently.
