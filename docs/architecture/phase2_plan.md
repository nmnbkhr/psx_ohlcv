# Phase 2 — Observability, Scope v2, Data Coverage

**Goal:** Make the system visible (observability), finish what Phase 1
deferred (scope v2 + skip taxonomy resolution), close real data gaps so
the terminal renders something useful instead of empty banners.

**Duration estimate:** 8–12 weeks across 3 sub-phases. Less structural
risk than Phase 1; more breadth.

**Starting point:** Tag `v0.1-phase1` (commit `c842cf9` on `master`).

**Phase 1 prerequisites met:**
- ☑ Streamlit reads via `/v1/*` (41 pages migrated + 1 partial)
- ☑ Worker pipeline owns all dispatchable ETL (15 handlers, jobs queue)
- ☑ `daily_sync.sh` enqueues to worker with CLI fallback
- ☑ One execution path: cron / UI / CLI all converge on `etl.<domain>.<fn>()`
- ☑ safe_writer + data_freshness invariants intact (Phase 0 SHAs unchanged)

## Sub-phases

```
2.A — Data Coverage   (weeks 1-3)  →  visible UX improvement, low risk
2.B — Observability   (weeks 4-7)  →  safety net for 2.C
2.C — Scope v2        (weeks 8-12) →  highest risk; needs 2.B operational
```

Detailed milestone plans live in companion docs:
- [`phase2_a_data_coverage.md`](phase2_a_data_coverage.md)
- [`phase2_b_observability.md`](phase2_b_observability.md)
- [`phase2_c_scope_v2.md`](phase2_c_scope_v2.md)
- [`phase2_risks.md`](phase2_risks.md)

---

## Why this order

### 2.A first (not 2.B first)

- Phase 1 invariants make further data work safe — safe_writer plus
  data_freshness already enforce write discipline. No new safety
  rails needed.
- Empty pages = user perceives system as broken even though
  architecture is sound. Visible wins build credibility for the longer
  Phase 2 tail.
- **2.A.2 catalog cleanup unblocks 2.B.** Without the ON CONFLICT bug
  fix, every Grafana panel built in 2.B would show `date='ZUMA'` /
  `last_row_date='WTL'` rows. We'd be building dashboards over
  corrupted ground truth.

### 2.B before 2.C (not the reverse)

- 2.C touches `tick_service.py` — the May 9 corruption neighborhood.
- Without 2.B metrics, `tick_bars.db` damage during 2.C is invisible
  until Streamlit errors hours later.
- Worker `PartOf=` fix is 2.B.2; without it, the worker silently dies
  during a 2.C deploy and the cron pipeline goes blind.

### Alternatives considered + rejected

| Order | Why rejected |
|---|---|
| 2.B → 2.A → 2.C | front-loads infra; users see empty pages longer; 2.B dashboards built over polluted catalog rows |
| 2.C → 2.A → 2.B | most dangerous — no metrics during May-9-neighborhood work |
| 2.A and 2.B in parallel | solo project, no |

---

## 2.A — Data Coverage (Weeks 1-3)

Quick-win first. Every empty page that renders means "system unusable"
even though architecture is good. Fix the syncs, recover the data,
add a data-quality layer.

**Ordering matters within 2.A.** The catalog ON CONFLICT bug (Phase 0.3
known_debt) is still LIVE post-Phase-1 — `regular_market_current` row
re-poisons on every sync. So:

| # | Title | Duration | Why this order |
|---|---|---|---|
| 2.A.1 | Data quality layer (validators + `data_quality` table) | 1 week | Foundation. Validators added in safe_writer transactions catch new pollution before it persists. |
| 2.A.2 | Catalog pollution root-cause fix (ON CONFLICT bug) | 0.5 week | **MUST run before backfills.** Bug pollutes new rows as you write them; backfilling first just generates fresh ZUMA/TBILL/MUFAP rows. |
| 2.A.3 | Empty/regressed table audit + backfills | 1 week | Includes `tbill_auctions` restoration (175 → 12 row regression discovered in Phase 2 Step 0 inventory) + `pkisrv_daily` empty-table investigation. |
| 2.A.4 | Composite-aggregator endpoint design | 0.5 week | market_research, futures pages off direct DB. Pattern documented for Phase 3 reuse. |

**Exit criteria:**
- No empty widgets on non-DEFER pages
- `data_quality` table tracks validity (date columns hold dates, etc.)
- Pages show a banner when their data fails validation — they don't
  fabricate, don't silently render zero rows
- Composite-aggregator pattern has a Phase 1-style template
- `tbill_auctions` row count restored OR documented as
  source-of-truth-changed
- `pkisrv_daily` populated OR documented as upstream-broken with
  Phase 3 disposition

### Phase 2 Step 0 findings baked into 2.A

Three findings from the inventory pass changed the milestone shape:

**Finding 1 — `tbill_auctions` regression (175 → 12 rows)**.
CLAUDE.md 2026-04-20 audit recorded 175 rows; today's count is 12.
**Loss happened during Phase 0/1.** Bisect plan in 2.A.3:

```bash
sqlite3 /mnt/e/psxdata/backups/psx_20260511.sqlite "SELECT COUNT(*) FROM tbill_auctions;"
sqlite3 /mnt/e/psxdata/backups/psx_20260514.sqlite "SELECT COUNT(*) FROM tbill_auctions;"
sqlite3 /mnt/e/psxdata/backups/psx_20260515.sqlite "SELECT COUNT(*) FROM tbill_auctions;"
```

Narrows the window to safe_writer migration (May 11) vs Phase 1.6
worker handler era. Dedicated 2.A.3 sub-sub-wave: `tbill_auctions
restoration from backup` (~30 min once window is known).

**Finding 2 — `pkisrv_daily` empty (0 rows)**.
CLAUDE.md lists 1.5K rows. `sovereign_curve` claims PKISRV consolidation.
2.A.3 sub-sub-wave: **first determine** whether this table was ever
populated post-May-9 recovery, or has been empty since. Recovery
likelihood depends on the answer (backup vs upstream re-sync vs
Phase 3 deprecation).

**Finding 3 — ON CONFLICT bug is LIVE not stale**.
The pollution row in `regular_market_current` (`last_row_date='WTL'`)
re-appeared *after* my Milestone 1.8.2 refresh on 2026-05-23. So the
Phase 0.3 catalog.py ON CONFLICT bug is not just back-history pollution
— it's actively re-poisoning rows on every sync. That moved catalog
cleanup BEFORE backfills (was 2.A.3, now 2.A.2). Backfilling into a
broken catalog would just produce 175 fresh ZUMA rows.

---

## 2.B — Observability (Weeks 4-7)

Make the system visible. Metrics, dashboards, log aggregation. Worker
lifecycle fix. This unblocks 2.C — you can't touch `tick_service.py`
safely without seeing what it does.

| # | Title | Duration | Dependencies |
|---|---|---|---|
| 2.B.1 | Metrics scaffold + counters/gauges (stack choice deferred — see 2.B doc) | 1 week | 2.A complete |
| 2.B.2 | Worker lifecycle metrics + `PartOf=` fix | 0.5 week | 2.B.1 |
| 2.B.3 | Dashboards (System / Jobs / Data Quality / Sync History) | 1 week | 2.B.1-2 |
| 2.B.4 | Log aggregation + alerts | 1 week | 2.B.1-3 |

**Stack choice deferred to 2.B detailed plan** — Grafana + Prometheus
is conventional but heavy for a solo project. Lightweight alternative
(`journalctl` + daily-digest cron) starts the conversation. Pick in
2.0.3.

**Exit criteria:**
- "Was the system running cleanly yesterday?" is a dashboard view, not a
  `journalctl` query
- 4 dashboards or equivalent surface: System Health / Jobs Queue /
  Data Freshness / Sync History
- Alerts fire on: worker down > 5 min, job failure rate > 10% per hour,
  `daily_sync_step` failed, data quality validation regression
- Worker no longer requires manual restart after API redeploy
  (PartOf=BindsTo / Requires or decoupled lifecycle)

---

## 2.C — Scope v2 (Weeks 8-12)

The big one. `tick_service.py` finally lands. Live Ticker,
intraday_quant_lab get proper homes. `load_ticks_from_disk` goes to
worker pipeline. The intraday read path gets its full API surface.

| # | Title | Duration | Dependencies |
|---|---|---|---|
| 2.C.1 | `tick_service.py` rationalization + commit | 1 week | 2.B complete |
| 2.C.2 | `load_ticks_from_disk` to worker handler | 1 week | 2.C.1 |
| 2.C.3 | `upsert_intraday` + `promote_intraday_to_eod` to worker | 1 week | 2.C.2 |
| 2.C.4 | Live Ticker + intraday_quant_lab page migration | 1 week | 2.C.3 |
| 2.C.5 | intraday.py Index/Dedup/Sync tabs (held in 1.7.E) | 1 week | 2.C.4 |

**Exit criteria:**
- `git status` clean (no pre-existing dirty files for first time since May 9)
- Every intraday sync path through worker, not ad-hoc CLI
- Live Ticker reads from `/v1/intraday/live` (new endpoint)
- intraday_quant_lab reads CSVs via API or has documented data source

---

## Phase 1 carry-over disposition

Every Phase 1.7 skip / defer category has a Phase 2 home (or explicit "won't fix"):

| Category | Count | Phase 2 disposition |
|---|---|---|
| engine-call-only | 14 | KEEP — review only `strategy_*` engines that could become worker jobs in Phase 3 |
| scraper-maintenance | 5 | KEEP — sub-DB observability surface in 2.B (Grafana) |
| composite-aggregator | 2 | **2.A.4** — `/v1/dashboard/<page>` prototype |
| file-fed real-time DEFER | 4 | live_ticker + intraday_quant_lab → **2.C.4**; tick_replay + ws_relay_status → DEFER to Phase 3 (WebSocket territory) |
| broken-dep DEFER | 5 | ftp_monitor + website_scan + instruments → **2.A.3** backfills; advanced_gnn + advanced_rl_exec → permanent KEEP (no torch_geometric) |
| untouched | 1 | portfolio_scanner → **2.A.4** small migration |
| DEBT-PHASE2 items (7 in known_debt.md) | — | All mapped to 2.A / 2.B / 2.C milestones (see companion docs) |

---

## Risks (separate from Phase 1's)

See [`phase2_risks.md`](phase2_risks.md). Top three:

1. **`tick_service.py` landing** (2.C.1) — May 9 neighborhood; mitigation: 2.B operational first, test on tick_bars.db copy.
2. **Composite-aggregator endpoint shape** (2.A.4) — design risk; mitigation: prototype before template.
3. **Observability stack overhead** (2.B) — solo maintenance; mitigation: start lightweight, only escalate if metrics consumption demands it.

---

## What stays out of Phase 2

- Postgres migration (Phase 3)
- Multi-user auth (Phase 3)
- Containerization (Phase 3)
- CI/CD (Phase 3)
- Public API exposure (Phase 3)
- WebSocket push to Streamlit (Phase 3+)
- Root-directory markdown sprawl cleanup (cosmetic; defer indefinitely)
