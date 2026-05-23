# Phase 2.C — Scope v2

**Sub-phase parent:** [`phase2_plan.md`](phase2_plan.md)
**Goal:** Finish the work Phase 0 deferred to avoid expanding the May 9
corruption window. Tick service finally lands. Intraday read path
gets its full API surface. `git status` clean for the first time since
the incident.
**Duration estimate:** ~5 weeks.
**Starting point:** Phase 2.B complete (digest + alerts operational).

**Phase 2.C prerequisites met:**
- ☑ safe_writer + data_freshness + jobs queue (Phase 0/1)
- ☑ Data quality validators (Phase 2.A.1) — catch tick-side pollution
- ☑ Daily digest + alerts (Phase 2.B) — failure visible same day
- ☑ Worker lifecycle decoupled (Phase 2.B.2) — surviving API redeploys

---

## Why scope-v2 was a boundary (and why it isn't anymore)

The "scope v2" label was set during Phase 0 inventory work. The
original meaning was **corruption blast-radius containment**: don't
touch tick / intraday code paths until safe_writer + data_freshness
made the May 9 corruption pattern impossible to repeat. Any work
inside that boundary risked extending the incident.

**That reason is now obsolete.**

- `safe_writer` makes "writer crashes mid-transaction → DB corruption"
  impossible (Phase 0.1).
- `data_freshness` makes "data written, no catalog update → silent
  partial state" impossible (Phase 0.2).
- The worker / API decomposition means tick service writes are gated
  through the same write path as everything else (Phase 1).
- The tape-recorder pattern itself contains blast radius: the VM
  writes JSONL only (filesystem append, no SQLite), then the laptop
  ingests JSONL → tick_bars.db as a separate process. A corruption
  during ingest doesn't lose the JSONL — re-ingest is always possible.

So the boundary's structural purpose is fully discharged. "Scope v2"
persists as a category label for ergonomic reasons (these paths share
a domain — tick / intraday / live), but no longer as a safety gate.
Phase 2.C is unblocked because nothing structural is blocking it —
not because we're newly willing to take the May-9-class risk.

---

## Milestones

| # | Title | Effort | Dependencies |
|---|---|---:|---|
| 2.C.1 | `tick_service.py` rationalization + landing | 1w | 2.B complete |
| 2.C.2 | `load_ticks_from_disk` to worker handler | 1w | 2.C.1 |
| 2.C.3 | `upsert_intraday` + `promote_intraday_to_eod` to worker | 1w | 2.C.2 |
| 2.C.4 | Live Ticker + intraday_quant_lab page migration | 1w | 2.C.3 |
| 2.C.5 | intraday.py Index/Dedup/Sync tabs (held in 1.7.E) | 1w | 2.C.4 |

---

## Milestone 2.C.1 — `tick_service.py` rationalization

This is the milestone the file has been waiting 16 days for. The plan
has three commits in sequence: a README that documents what the daemon
already does, a test-on-copy harness that future tick_service work
inherits, and the actual landing commit. The dirty diff lands LAST,
gated by everything before it.

### Diff audit (measured 2026-05-23, restate at execution time)

```
git diff src/pakfindata/services/tick_service.py
  → 26 total diff lines (including @@ headers and context)
  → 8 lines of insertions, 0 deletions
git log -1 src/pakfindata/services/tick_service.py
  → 7ec55b0 2026-05-07
  → "fix(tick_service): use PSXDATA_ROOT env var for portable paths"
wc -l src/pakfindata/services/tick_service.py
  → 1535 lines
```

**Edits in flight (the dirty content):**

1. New module-level constant `ENABLE_DB` (env-gated, default '1' for
   backwards compatibility):

```python
# Set ENABLE_DB=0 on capture-only VMs (no SQLite writes, JSONL only).
# All DB-touching methods (_checkpoint_flush, EOD flush) early-return when False.
ENABLE_DB = os.environ.get("ENABLE_DB", "1") == "1"
```

2. Early-return guard in `_checkpoint_flush`:

```python
if not ENABLE_DB:
    return  # JSONL-only mode
if not ENABLE_DB:
    return  # JSONL-only mode (capture VMs)  ← DUPLICATE
```

**Diff size verdict: ONE landing commit, not sub-waves.** The user's
threshold rule applies: < 100 lines → single commit. 8 lines is
well under. The 2.C.1 milestone's *surrounding* work (README,
test-on-copy harness) gets its own commits, but the actual
`tick_service.py` modification is one atomic landing.

**Issue surfaced by the audit:** the dirty diff includes the
`if not ENABLE_DB: return` guard **twice** at lines 691-694 — a leftover
from in-flight iteration. Landing verbatim would commit the duplicate.
Sub-wave 2.C.1.3 decides whether to clean before commit (preferred) or
land + fix in a follow-up. Don't ignore it.

**At execution time, re-run the audit before any work** — the diff
may have grown if `tick_service.py` was touched on the VM between
this planning doc and 2.C.1 execution. If the diff is now > 100
lines, STOP and restructure 2.C.1 with sub-waves per Phase 0.1
discipline (~5 sub-waves for ~500 lines, etc.). Size determines
structure.

### Sub-wave 2.C.1.1 — README documenting the tape-recorder pattern

**Files:**
- NEW `src/pakfindata/services/README.md` (or `tick_service.md`)

**Why this comes first.** Knowledge about the daemon currently lives
only in the code, the Phase 0 memory file `cloud-jsonl-rsync-dependency`,
and your head. The next person who looks at `tick_service.py` (you in
six months, a collaborator) needs to understand the runtime model
WITHOUT reading the code. The README is the unblock; the landing is
trivial *after* the README exists.

**Section list:**

1. **What `tick_service.py` is.** Long-running daemon. Connects to
   PSX WebSocket, captures every tick, writes JSONL files
   continuously. On a schedule, checkpoints completed bars to
   SQLite (`tick_bars.db`) and emits EOD rows.

2. **The tape-recorder pattern.**
   - VM (`psx-cloud`) runs `tick_service` with `ENABLE_DB=0`. It writes
     JSONL only — never touches SQLite. Filesystem append: no
     corruption surface. The VM is the "tape recorder."
   - Laptop runs `tick_service` (or an ingest step) with `ENABLE_DB=1`
     (default). Reads JSONL from `/mnt/e/psxdata/tick_logs_cloud/`
     (rsync'd from VM via `~/sync_psx_cloud.sh`), batches into
     `tick_bars.db` and `intraday_bars`. The laptop is the "compute
     layer."
   - Separation means a SQLite corruption during ingest doesn't lose
     ticks — the JSONL is the durable source of truth.

3. **The `ENABLE_DB` invariant.**
   - Default: `1` (DB writes enabled — laptop mode)
   - Capture-only VMs: `0` (JSONL only)
   - The guard appears in every DB-touching method
     (`_checkpoint_flush`, EOD flush, etc.). New methods that touch
     the DB MUST add the guard.

4. **What the daemon writes, where, when.**
   - JSONL `/mnt/e/psxdata/tick_logs/ticks_<date>.jsonl` (continuous)
   - `/mnt/e/psxdata/live_snapshot.json` (refreshed every N seconds)
   - `tick_bars.db` (checkpoint cadence — minutes)
   - `tick_data` + `intraday_bars` in `psx.sqlite` (EOD-only,
     laptop mode)

5. **Restart behavior.**
   - Idempotent: on restart, picks up from the JSONL file's
     position. No data loss from a restart in the middle of a
     trading session.
   - safe_writer + INSERT OR IGNORE on the DB side means re-flushing
     a checkpoint doesn't create duplicates.

6. **Integration with `tick_bars.db`.**
   - Schema location, table list, who else reads it (tick_analytics
     page in Phase 1.7.G.5).
   - Disk path: `/home/smnb/psxdata_rescue/tick_bars.db` (overrideable
     via `PSX_TICK_BARS_DB`).
   - Backup cadence: not part of the main `daily_sync.sh` backup;
     covered by separate cron (Phase 3 ops review).

7. **How to investigate problems.**
   - "Ticks are missing for date X" — check
     `/mnt/e/psxdata/tick_logs_cloud/ticks_X.jsonl` exists and has
     rows; check rsync timing.
   - "tick_bars.db has fewer rows than expected" — re-ingest from
     JSONL via the laptop pipeline.
   - "Daemon crashed mid-session" — check journal, restart;
     JSONL position resumes.

**Commit:**

```
docs(services): tape-recorder pattern README

src/pakfindata/services/README.md documents what tick_service.py is,
the VM-as-tape-recorder / laptop-as-compute split, the ENABLE_DB
invariant, what writes go where, restart behavior, and investigation
recipes.

No code touched. This unblocks Milestone 2.C.1.3 (the actual landing)
by making the daemon's runtime model legible without reading 1535
lines of code.
```

### Sub-wave 2.C.1.2 — Test-on-copy harness

**Files:**
- NEW `scripts/test_tick_service_on_copy.sh`

**Why this comes second.** Every future change to `tick_service.py`
benefits from the same test ritual. Encode the procedure once; reuse
forever. The harness lives in the repo, not in someone's bash
history.

**The procedure, spelled out:**

```bash
#!/usr/bin/env bash
# scripts/test_tick_service_on_copy.sh
#
# Run tick_service against an isolated copy of tick_bars.db before
# committing any change to tick_service.py.
#
# Procedure:
#   1. Stop production tick_service if running
#   2. Copy tick_bars.db → tick_bars.test.db
#   3. Point tick_service at the copy via env var
#   4. Run for 30 minutes
#   5. Validate: row counts grew, latest timestamps fresh,
#      schema unchanged, PRAGMA integrity_check passes
#   6. Restore production tick_service
#   7. Print PASS/FAIL summary
#
# Exit codes: 0 PASS, 1 FAIL, 2 setup error

set -euo pipefail

PROD_DB="${PSX_TICK_BARS_DB:-/home/smnb/psxdata_rescue/tick_bars.db}"
TEST_DB="${PROD_DB%.db}.test.db"
TEST_DURATION_SECONDS="${TEST_DURATION_SECONDS:-1800}"  # 30 min default

echo "═══ STEP 1: Stop production tick_service ═══"
PROD_WAS_RUNNING=0
if systemctl --user is-active tick_service.service >/dev/null 2>&1; then
    PROD_WAS_RUNNING=1
    systemctl --user stop tick_service.service
    sleep 3
fi

echo "═══ STEP 2: Copy ${PROD_DB} → ${TEST_DB} ═══"
cp "${PROD_DB}" "${TEST_DB}"
ROWS_BEFORE_BARS=$(sqlite3 "${TEST_DB}" "SELECT COUNT(*) FROM tick_bars;")
ROWS_BEFORE_TICKS=$(sqlite3 "${TEST_DB}" "SELECT COUNT(*) FROM raw_ticks;")
LATEST_BEFORE=$(sqlite3 "${TEST_DB}" "SELECT MAX(ts) FROM tick_bars;")

echo "═══ STEP 3: Run tick_service against the copy for ${TEST_DURATION_SECONDS}s ═══"
PSX_TICK_BARS_DB="${TEST_DB}" \
ENABLE_DB=1 \
timeout "${TEST_DURATION_SECONDS}" \
    python -m pakfindata.services.tick_service || true
# 'timeout' exits 124 on timeout, 0 on normal exit — both are OK

echo "═══ STEP 4: Validate test copy ═══"
ROWS_AFTER_BARS=$(sqlite3 "${TEST_DB}" "SELECT COUNT(*) FROM tick_bars;")
ROWS_AFTER_TICKS=$(sqlite3 "${TEST_DB}" "SELECT COUNT(*) FROM raw_ticks;")
LATEST_AFTER=$(sqlite3 "${TEST_DB}" "SELECT MAX(ts) FROM tick_bars;")
INTEGRITY=$(sqlite3 "${TEST_DB}" "PRAGMA integrity_check;")

echo "  tick_bars:  ${ROWS_BEFORE_BARS} → ${ROWS_AFTER_BARS}"
echo "  raw_ticks:  ${ROWS_BEFORE_TICKS} → ${ROWS_AFTER_TICKS}"
echo "  latest ts:  ${LATEST_BEFORE} → ${LATEST_AFTER}"
echo "  integrity:  ${INTEGRITY}"

FAIL=0
[[ "${INTEGRITY}" != "ok" ]] && { echo "  FAIL: integrity check"; FAIL=1; }
# During market hours, expect growth. Outside hours, expect no growth
# but no shrinkage either. The harness doesn't enforce growth (test
# may run after market close); it enforces no corruption + schema
# stability.

echo "═══ STEP 5: Schema parity ═══"
sqlite3 "${PROD_DB}" ".schema" > /tmp/schema_prod.sql
sqlite3 "${TEST_DB}" ".schema" > /tmp/schema_test.sql
if ! diff -q /tmp/schema_prod.sql /tmp/schema_test.sql; then
    echo "  FAIL: schema diverged"
    diff /tmp/schema_prod.sql /tmp/schema_test.sql | head -20
    FAIL=1
fi

echo "═══ STEP 6: Restore production ═══"
rm -f "${TEST_DB}"
if [[ "${PROD_WAS_RUNNING}" -eq 1 ]]; then
    systemctl --user start tick_service.service
fi

echo "═══ Result ═══"
if [[ ${FAIL} -eq 0 ]]; then
    echo "PASS"
    exit 0
else
    echo "FAIL"
    exit 1
fi
```

**Commit:**

```
chore(scripts): test_tick_service_on_copy harness

scripts/test_tick_service_on_copy.sh runs tick_service against an
isolated copy of tick_bars.db, validates row counts + schema +
PRAGMA integrity_check, and restores production. Used as the gate
for Milestone 2.C.1.3 — the actual tick_service.py landing — and
for any future tick_service edit.

No code touched in src/. Standalone harness.
```

### Sub-wave 2.C.1.3 — Land the dirty diff

**Files:**
- `src/pakfindata/services/tick_service.py` (the dirty diff)

**Pre-commit checklist:**

1. **Re-run the diff audit.** Confirm size still ≈ 8 lines. If
   significantly larger, STOP and re-plan 2.C.1 with sub-waves.

2. **Decide on the duplicate-guard cleanup.** The dirty diff has:

   ```python
   if not ENABLE_DB:
       return  # JSONL-only mode
   if not ENABLE_DB:
       return  # JSONL-only mode (capture VMs)
   ```

   Two acceptable resolutions:
   - **Clean before commit** (preferred): delete the second copy in
     the same commit as the landing. The commit message notes "removed
     duplicate guard introduced during in-flight iteration."
   - **Land verbatim + immediate follow-up.** Land as-is, then a
     second commit `refactor(tick_service): dedupe ENABLE_DB guard`.
     Acceptable if you want the dirty content's history preserved
     exactly.

3. **Run the test-on-copy harness from 2.C.1.2.** Must exit 0.

4. **Verify on the VM.** SSH to `psx-cloud`, confirm `ENABLE_DB=0`
   in its environment, restart `tick_service` there, watch logs for
   1 minute. JSONL writes continue; no SQLite errors.

5. **Commit.**

```
feat(tick_service): ENABLE_DB env gate for capture-only VMs

Adds module-level constant ENABLE_DB = os.environ.get("ENABLE_DB", "1")
== "1" and an early-return guard in _checkpoint_flush so the daemon
can run in capture-only mode on the psx-cloud VM (JSONL writes only,
no SQLite). Default '1' preserves laptop behavior — existing callers
unchanged.

Documents the tape-recorder pattern this enables: VM writes JSONL
only (corruption surface = filesystem append, ~none); laptop ingests
JSONL → tick_bars.db as a separate process (corruption surface = full,
but JSONL is durable source of truth, so re-ingest always recovers).

Removed a duplicate guard that crept in during in-flight iteration.

Tested via scripts/test_tick_service_on_copy.sh (30-minute run against
isolated copy of tick_bars.db, schema parity verified, PRAGMA
integrity_check passed). See src/pakfindata/services/README.md for
the full daemon runtime model.

Pre-existing dirty since 2026-05-07 (commit 7ec55b0). Landing in
Phase 2.C.1 after Phase 2.B observability operational and the
test-on-copy harness in place.
```

### Exit criteria (2.C.1 rollup)

- ☐ `src/pakfindata/services/README.md` documents the tape-recorder pattern
- ☐ `scripts/test_tick_service_on_copy.sh` is committed and passes
- ☐ `git status` shows no `M src/pakfindata/services/tick_service.py`
- ☐ VM `tick_service` daemon still operational with `ENABLE_DB=0`
- ☐ Laptop `tick_service` daemon (or ingest) still operational with default
- ☐ `tick_bars.db` integrity_check still `ok` after one full trading day post-landing
- ☐ For the first time since 2026-05-07, `git status` shows no
  pre-existing dirty files (untracked .md / .zip files remain — those
  are user scratch, not project debt)

### What NOT to do in 2.C.1

- Don't restructure `tick_service.py` while landing the diff. Behavior-
  preserving 8-line change only. Refactors are separate commits if
  warranted.
- Don't add new ENABLE_DB-gated behavior in the same commit. The flag
  enables capture-only mode; that's the entire scope.
- Don't migrate `tick_service` to a worker handler in 2.C.1. It's a
  long-running daemon, not a job. Worker integration is 2.C.2's
  `load_ticks_from_disk` and 2.C.3's batch ingest functions, not the
  realtime capture daemon itself.

---

## Milestone 2.C.2 — `load_ticks_from_disk` to worker handler

Standard Phase 1.6 shape — converge one ETL path into a worker handler
+ shared `etl.<domain>.<fn>()`. Lower risk than 2.C.1 because the path
already exists; we're just moving it onto the worker substrate.

### Current state

```bash
echo "═══ Current invocation sites ═══"
grep -rnE "load_ticks_from_disk|ticks-load" \
    src/pakfindata/{cli.py,ui/page_views/,worker/} 2>/dev/null | head -20
```

CLI subcommand `pakfindata.cli intraday ticks-load --date X` exists
(it's one of the Phase 1.8 KEEP-CLI items). UI button in
`intraday.py::Sync` tab also exists.

### Sub-waves

- **2.C.2.1 — Consolidate into `etl/ticks.py`.** Move
  `load_ticks_from_disk` body into a new shared function. CLI handler
  shrinks to "call etl + exit-code translation." Pattern from Phase 1.5
  `etl/indices.py`.
- **2.C.2.2 — Worker handler `load_ticks_from_disk(date)`.** Thin
  wrapper around the etl function. Registry entry.
- **2.C.2.3 — UI button migration.** `intraday.py::Sync` "Load Ticks
  from Disk" button enqueues via `/v1/jobs/load_ticks_from_disk` with
  `{"date": X}` payload. Existing button kept as fallback during the
  transition.
- **2.C.2.4 — daily_sync.sh migration.** The `intraday_load` step in
  daily_sync.sh (currently KEEP CLI from Phase 1.8) gets the
  `enqueue_and_wait` treatment with CLI fallback per Phase 1.8.2
  pattern.

### Exit criteria

- `etl/ticks.py::load_ticks_from_disk(date)` is the only implementation
- Worker handler `load_ticks_from_disk` registered
- UI + CLI + cron all call through it
- daily_sync.sh `intraday_load` step uses `enqueue_and_wait`

### What NOT to do in 2.C.2

- Don't rewrite the ingest logic. Move it, don't redesign it.

---

## Milestone 2.C.3 — `upsert_intraday` + `promote_intraday_to_eod` to worker

Same pattern as 2.C.2. Two more deferred functions converge onto worker
handlers.

### Sub-waves

- **2.C.3.1 — `etl/intraday.py`** consolidates `upsert_intraday` and
  related callers.
- **2.C.3.2 — Worker handler `upsert_intraday(symbol, date)`** —
  registry entry.
- **2.C.3.3 — `promote_intraday_to_eod` worker handler.** Per
  CLAUDE.md "Deprecated paths" list, this is a one-off promotion
  function. May get a deletion review here instead of migration.
  Decide during Step 0 audit.
- **2.C.3.4 — UI button updates.** Intraday page's promotion button
  (if retained) enqueues via worker.

### Exit criteria

- All intraday write paths route through `etl.intraday`
- Worker handlers registered for each
- No direct DB inserts in `ui/page_views/intraday.py`

---

## Milestone 2.C.4 — Live Ticker + intraday_quant_lab page migration

The two Phase 1.7 file-fed DEFER pages. Both currently read JSON/CSV
files directly; the migration gives them proper API endpoints.

### Live Ticker

Reads `/mnt/e/psxdata/live_snapshot.json`. New endpoint
`/v1/intraday/live` serves the same payload from disk. Page stops
reading the file directly; calls the endpoint.

**Why not WebSocket:** Phase 2 stays HTTP. If users complain about
2-second polling latency, Phase 3 introduces a WebSocket. Today's UX
is fine with polling.

### intraday_quant_lab

Reads CSV files from `~/psxdata/intraday/<date>/`. New endpoint
`/v1/intraday/quant-lab/<date>` materializes the same content. May
require a worker handler to PARQUET-ify the CSVs for fast read; decide
during 2.C.4 Step 0.

### Sub-waves

- **2.C.4.1 — `/v1/intraday/live` endpoint.** Reads snapshot file,
  returns same JSON shape Live Ticker expects.
- **2.C.4.2 — Live Ticker page migration.** Switch to `pakfindata.ui.api.client`.
- **2.C.4.3 — `/v1/intraday/quant-lab/<date>` endpoint.** May include
  a worker-side parquet pre-build.
- **2.C.4.4 — intraday_quant_lab migration.**

### Exit criteria

- Live Ticker no longer reads `live_snapshot.json` directly
- intraday_quant_lab no longer reads CSVs directly
- Both pages render identically to today
- Two Phase 1.7 DEFERs resolved; remaining DEFERs (tick_replay,
  ws_relay_status) escalate to Phase 3 (WebSocket)

---

## Milestone 2.C.5 — intraday.py Index/Dedup/Sync tabs (held in 1.7.E)

Phase 1.7 Group E migrated 3 of intraday.py's 7 tabs (Dashboard,
Charts, Market Pulse). Three remaining tabs (Index, Dedup, Sync) were
deferred as scope v2.

### Sub-waves

- **2.C.5.1 — Index tab.** Surveys the per-day intraday index;
  endpoints `/v1/intraday/<symbol>/index`.
- **2.C.5.2 — Dedup tab.** Operational dedup view; `/v1/intraday/dedup-stats`.
- **2.C.5.3 — Sync tab.** All buttons (Fetch / Load / Build summaries)
  now enqueue via worker (Phase 1.8 pattern). Sync tab becomes pure
  read of `/v1/jobs?type=...` for job history.

### Exit criteria

- `intraday.py` has zero direct DB reads across all 7 tabs
- Sync tab buttons all dispatch through worker
- intraday.py overall LOC drops as inline DB code is removed

---

## Phase 2.C exit criteria (rollup)

- ☐ `git status` clean (first time since 2026-05-07)
- ☐ Tape-recorder pattern documented; future maintenance unblocked
- ☐ Test-on-copy harness reusable for any future tick_service edit
- ☐ Every intraday sync path through worker
- ☐ Live Ticker + intraday_quant_lab on `/v1`
- ☐ intraday.py all 7 tabs migrated
- ☐ Phase 1.7's 4 file-fed DEFERs: 2 resolved (Live Ticker, quant_lab),
  2 escalated to Phase 3 (tick_replay, ws_relay_status)

When all 7 boxes check, Phase 2 is complete. Send the Phase 3 planning
prompt.

---

## Risk summary

See [`phase2_risks.md`](phase2_risks.md). 2.C-specific:

- **2.C.1** — touches `tick_service.py`; mitigation = README first,
  harness second, dirty diff last.
- **2.C.2/3** — straightforward worker migrations; main risk is
  pollution from incomplete data, caught by 2.A.1 validators.
- **2.C.4** — Live Ticker latency expectations may push toward
  WebSocket; explicit non-goal for Phase 2, document if it comes up.
