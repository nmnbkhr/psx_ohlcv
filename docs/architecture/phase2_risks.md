# Phase 2 Risk Register

**Parent:** [`phase2_plan.md`](phase2_plan.md)
**Generated:** 2026-05-23 (Phase 2 planning close)
**Cadence:** Revisit at the close of each sub-phase (2.A, 2.B, 2.C);
risks change shape as work lands.

This register is separate from Phase 1's. Phase 1's risks were all
about migration safety (broken queries, stale catalog rows, write
paths bypassing safe_writer). Phase 2's are about design choices that
lock in long-term debt + about touching legacy paths under new safety
discipline.

---

## Severity scale

| Severity | Meaning |
|---|---|
| **High** | Could derail the milestone or require Phase 3 work to repair |
| **Medium** | Material work to recover but containable within the sub-phase |
| **Low** | Cosmetic / cleanup; doesn't change milestone exit criteria |

Probability scale: **High** = > 50%, **Medium** = 20-50%, **Low** < 20%.

---

## High-impact risks

### R-2.A.4 — Composite-aggregator endpoint shape locks in coupling

**Where:** Milestone 2.A.4
**Impact:** **High.** Wrong endpoint shape couples API tightly to UI
in a way Phase 1's per-domain pattern avoided. Undoing it is a Phase 3
refactor across two pages and an unknown number of future composite
pages that follow the same template.
**Probability:** **Medium.** Two prototypes (`market_research`,
`futures`) is the explicit hedge; the failure mode is "first prototype
looks good, template it too early."

**Mitigation:**
- Build TWO prototypes (2.A.4.1 + 2.A.4.3) before writing the pattern doc
- Documentation lists when to NOT use composite endpoints (Phase 2
  per-page DEFER for thin pages stays preferable to dashboard-shaped)
- Phase 2.B daily digest surfaces composite endpoint usage so misshape
  is visible if it accumulates

**Exit condition for this risk:** pattern doc exists in
`docs/architecture/composite_endpoints.md` AND a third future page
uses the pattern cleanly without reshaping the endpoint.

---

## Medium-impact risks

### R-2.A.3.1 — `tbill_auctions` backups don't contain the lost rows

**Where:** Milestone 2.A.3.1
**Impact:** **Medium.** If no backup contains the 175-row state,
restoration falls back to upstream re-sync from SBP. The SBP scraper
exists but the lost rows may have come from a different upstream
source (SBP EasyData API). Investigation could surface that the data
genuinely doesn't exist anywhere recoverable, in which case the table
is now permanently 12 rows.
**Probability:** **Low.** Backup cadence covers the full Phase 0-1
window; the 175 → 12 drop happened during that window.

**Mitigation:**
- Bisect commands in 2.A.3.1 narrow the loss window to a specific
  commit/migration before restoration starts
- If the loss predates available backups: fall back to SBP scraper
  re-run; document as "best-effort recovery" in `data_freshness.notes`
- Worst case: 12 rows + documented gap; pages calling treasury can
  surface the gap via Phase 2.A.1 validators

### R-2.A.3.2 — `pkisrv_daily` never populated post-May-9 recovery

**Where:** Milestone 2.A.3.2
**Impact:** **Medium.** If the table has been empty since the recovery
(not lost during Phase 0/1), it means the recovery scripts didn't
restore it. Restoration becomes an upstream MUFAP API question, not
a backup pull. May surface that the fast-sync code path
(`backfill_to_db_fast` in `sources/mufap_rates.py`) silently skips
PKISRV writes — which would be a small-but-real bug.
**Probability:** **Medium.** Three-outcome decision matrix in
2.A.3.2 enumerates the possibilities; one of them (backup empty +
curve has only PKISRV_SYN) is plausible given the post-recovery
context.

**Mitigation:**
- 2.A.3.2 starts with the investigation step before any restoration
  attempt
- If fast-sync bug: fix in 2.A.3, regression test against PKISRV +
  PKRV + PKFRV writes
- If upstream genuinely broken: document, propose Phase 3 deprecation
  (use only PKISRV_SYN synthetic from sovereign_curve)

### R-2.C.4 — Live Ticker requires WebSocket for acceptable UX

**Where:** Milestone 2.C.4
**Impact:** **Medium.** Phase 2.C.4 ships HTTP polling; if 2-second
polling latency is unusable for active traders, the page is migrated
but operationally useless. Forces Phase 3 WebSocket work earlier than
planned.
**Probability:** **Medium.** Solo single-user means UX threshold is
"this person is OK with it," not "trader desk requirement." Likely
fine with polling; not guaranteed.

**Mitigation:**
- 2.C.4 ships HTTP polling explicitly; WebSocket is documented as
  Phase 3 scope from the start
- If polling latency complaint surfaces: defer to Phase 3 without
  rolling back the page migration (page works; latency is the issue,
  not the data path)

---

## Low-impact risks

### R-2.C.1 — `tick_service.py` landing triggers regression

**Where:** Milestone 2.C.1
**Impact:** **Low** (downgraded from Medium during planning close).
**Why downgraded.** The diff audit measured the actual dirty content
at 8 lines of insertion — a module-level `ENABLE_DB` constant and two
early-return guards in DB-touching methods. After 16 days of "dirty"
status, the file is more "uncommitted because the rule said so" than
"uncommitted because actively volatile." A small additive change of
this shape does not have May-9-class corruption surface — the new
behavior is `return early before doing anything`, which is the safest
possible mutation.
**Probability:** **Low.** The path that adds the guards never writes
to the DB when triggered.

**Mitigation:**
- Test-on-copy harness from 2.C.1.2 catches anything unexpected; even
  for a small diff, running the harness once produces the gate.
- VM verification (SSH to `psx-cloud`, restart with `ENABLE_DB=0`,
  watch logs for 1 minute) confirms capture-only mode behaves.
- Daily digest from Phase 2.B catches issues within 24h via
  `tick_data` and `intraday_bars` freshness rows.

**Reframed value of the harness (2.C.1.2):** Originally framed as the
safety net for the dirty landing. Given the actual diff size, it's
now better framed as **infrastructure-for-future-work**:
- Every future `tick_service.py` edit reuses the harness
- New ENABLE_DB-gated code added later (which IS more likely to be
  large) has the gate already in place
- The harness is to `tick_service.py` what the
  `scripts/test_tick_service_on_copy.sh` becomes: a reusable artifact,
  not a one-off safety check

The shift is in framing, not in the work itself. The harness still
gets built in 2.C.1.2; it just isn't the load-bearing piece for THIS
landing.

### R-2.A.3 — Catalog pollution root cause not fully fixed

**Where:** Milestone 2.A.2
**Impact:** **Low.** ZUMA-class rows reappear after Phase 2.A.
**Probability:** **Low.** Phase 0.3 ON CONFLICT bug is
well-understood (single SET clause missing two columns); reproducer
test in 2.A.2.1 catches regression.

**Mitigation:**
- Reproducer pytest fails today, passes after 2.A.2.2 fix
- Test stays in tree so future catalog.py changes can't reintroduce
  the bug silently

### R-2.B — Lightweight observability misses an upgrade signal

**Where:** Milestone 2.B (architectural choice)
**Impact:** **Low.** Daily digest stays in place when it should have
escalated to Prometheus + Grafana. Symptom: operational issues take
longer to investigate than they should.
**Probability:** **Low.** Two upgrade triggers documented in 2.B.4
("3+ digest reads in a day" / "second operator joins") fire on
observable behavior, not subjective judgment.

**Mitigation:**
- Triggers baked into the digest's own banner so they're impossible
  to miss
- Phase 3 planning will revisit observability anyway when
  containerization brings its own metrics needs

---

## Risks NOT in this register (deferred / out of scope)

These risks exist but belong to other plans:

- **Postgres migration risks** — Phase 3
- **Multi-user auth contention** — Phase 3
- **Containerization deployment** — Phase 3
- **Public API surface security** — Phase 3
- **WebSocket scale beyond 1 user** — Phase 3 (if 2.C.4 surfaces this)

---

## How to use this register

- At the start of each sub-phase, re-read the relevant risks; note
  any new ones in the working notes (eventually folded back here)
- At the close of each sub-phase, mark which risks materialized vs
  didn't, and revise probability/impact for the remaining ones
- A risk that materializes goes in the related milestone's
  post-mortem section in `roadmap.md`, NOT in this register
  retroactively — keep this register as a "before" view
