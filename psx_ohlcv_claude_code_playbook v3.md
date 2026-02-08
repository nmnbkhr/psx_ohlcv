# PSX OHLCV — Claude Code Execution Playbook

## Document Purpose
This is a **copy-paste-ready prompt sequence** for Claude Code sessions.
Each prompt is self-contained, verifiable, and produces a measurable output.
No assumptions. No hallucination. Every step has a VERIFY gate.

---

## SESSION STATE FILE — CONTEXT RECOVERY SYSTEM

Claude Code sessions can lose context (timeout, crash, new session). To prevent
rework and confusion, we maintain a persistent state file on disk that tracks
exactly where we are, what's done, and what's next.

### How It Works
1. The state file lives at `~/psx_ohlcv/.claude_session_state.md`
2. It is updated after EVERY completed prompt
3. When starting a NEW Claude Code session, the FIRST thing you do is paste the
   **Session Recovery Prompt** below — Claude reads the file and resumes exactly
   where you left off
4. The file is git-ignored (add to .gitignore) — it's local workflow state, not code

### Prompt S.0 — Create Session State File (RUN ONCE, FIRST TIME ONLY)
```
Create the file ~/psx_ohlcv/.claude_session_state.md with this content:

# PSX OHLCV — Claude Code Session State
# This file tracks progress across Claude Code sessions.
# Updated after each completed prompt. Read this first when resuming.

## Current Status
- **Active Phase**: 0 (Pre-flight)
- **Last Completed Prompt**: None
- **Next Prompt**: 0.1 — Environment Verification
- **Active Branch**: dev
- **Tests Passing**: Unknown
- **Blockers**: None

## Phase Completion Log
| Phase | Status | Branch | Started | Completed | Notes |
|-------|--------|--------|---------|-----------|-------|
| 0: Pre-flight | ⏳ Not started | dev | | | |
| 1: Split db.py | ⏳ Not started | refactor/phase1-split-db | | | |
| 2: Split app.py | ⏳ Not started | refactor/phase2-split-ui | | | |
| 3: PostgreSQL | ⏳ Not started | feat/phase3-postgresql | | | |
| 4: Async+Workers | ⏳ Not started | feat/phase4-async | | | |
| 5: API+AI+CI | ⏳ Not started | feat/phase5-api-ai | | | |
| 6: Integration | ⏳ Not started | dev | | | |

## Prompt Completion Checklist
### Phase 0
- [ ] 0.1 Environment Verification
- [ ] 0.2 Git Safety Baseline
### Phase 1
- [ ] 1.1 Create Package Structure
- [ ] 1.2 Extract Schema SQL
- [ ] 1.3 Extract Connection Logic
- [ ] 1.4 Extract Symbol Repository
- [ ] 1.5 Extract EOD Repository
- [ ] 1.6 Extract Intraday Repository
- [ ] 1.7 Extract Company Repository
- [ ] 1.8 Extract Market Repository
- [ ] 1.9 Extract Instruments Repository
- [ ] 1.10 Extract Fixed Income Repository
- [ ] 1.11 Extract Jobs Repository
- [ ] 1.12 Extract User Repository
- [ ] 1.13 Create __init__.py Re-exports
- [ ] 1.14 Replace Original db.py + Commit
### Phase 2
- [ ] 2.0 Branch Setup
- [ ] 2.1 Analyze app.py Structure
- [ ] 2.2 Create Page Module Structure
- [ ] 2.3 Extract Shared Helpers
- [ ] 2.4-2.13 Extract Pages (10 pages)
- [ ] 2.14 Final Commit
### Phase 3
- [ ] 3.0 Branch Setup
- [ ] 3.1 Docker Infrastructure
- [ ] 3.2 PostgreSQL Schema
- [ ] 3.3 Data Migration Script
- [ ] 3.4 Database Adapter Layer
- [ ] 3.5 Commit Phase 3
### Phase 4
- [ ] 4.0 Branch Setup
- [ ] 4.1 Async HTTP Fetcher
- [ ] 4.2 ARQ Worker Setup
- [ ] 4.3 Commit Phase 4
### Phase 5
- [ ] 5.0 Branch Setup
- [ ] 5.1 Add API Routers
- [ ] 5.2 WebSocket Endpoints
- [ ] 5.3 Consolidate LLM → Agents
- [ ] 5.4 Environment Configuration
- [ ] 5.5 GitHub Actions CI
- [ ] 5.6 Final Commit Phase 5
### Phase 6
- [ ] 6.1 Merge All + Tag Release

## Issues / Decisions Log
(Record any problems, workarounds, or design decisions made during execution)
| Prompt | Issue | Resolution |
|--------|-------|------------|

## File Change Summary
(Updated after each phase commit)
| Phase | Files Created | Files Modified | Files Deleted | Lines Before | Lines After |
|-------|--------------|----------------|---------------|-------------|-------------|

Also add this line to ~/psx_ohlcv/.gitignore:
  .claude_session_state.md
```

### Session Recovery Prompt — USE THIS WHEN STARTING A NEW CLAUDE CODE SESSION
```
I'm resuming work on the psx_ohlcv project. We track progress in a session state file.

Please do the following:
1. Read ~/psx_ohlcv/.claude_session_state.md
2. Tell me:
   - What phase we're in
   - What was the last completed prompt
   - What is the next prompt to execute
   - Which git branch we should be on
   - Whether tests were passing at last checkpoint
3. Run: cd ~/psx_ohlcv && git branch --show-current
4. Run: git status --short
5. Run: pytest tests/ -x --tb=short -q 2>&1 | tail -10

Then wait for my instruction on whether to continue from where we left off.
Do NOT start any work until I confirm.
```

### State Update Template — PASTE THIS AFTER EACH COMPLETED PROMPT
After each prompt completes and VERIFY passes, tell Claude Code:
```
Update ~/psx_ohlcv/.claude_session_state.md:
- Mark prompt [X.Y] as completed (change [ ] to [x])
- Update "Last Completed Prompt" to [X.Y]
- Update "Next Prompt" to [X.Z]
- Update "Tests Passing" to Yes/No based on last pytest run
- If any issues were encountered, add them to the Issues Log
- Update Active Phase if we're moving to a new phase
```

---

## PRE-FLIGHT: Machine Readiness

### Prompt 0.1 — Environment Verification
```
I'm working on the psx_ohlcv project. Before we do anything, verify my environment:

1. Run: `cd ~/psx_ohlcv && git branch --show-current` — confirm we're on `dev`

2. Python version check:
   Run: `python --version`
   The codebase uses `X | None` union type hints (PEP 604) and `list[str]` 
   lowercase generics (PEP 585). This means MINIMUM Python 3.10 is required.
   - Python 3.10+ → OK, proceed
   - Python 3.9 or lower → STOP, need to upgrade (see fix below)
   
   Also check: `cat pyproject.toml | grep -i "python_requires\|requires-python"` 
   to confirm what the project itself declares. If pyproject.toml says a 
   specific version, use THAT as the requirement, not my analysis.

3. Run: `pip install -e ".[dev]" 2>&1 | tail -5` — install deps
4. Run: `wc -l src/psx_ohlcv/db.py` — record actual count (expected ~8607)
5. Run: `wc -l src/psx_ohlcv/ui/app.py` — record actual count (expected ~11265)
6. Run: `find src/ -name "*.py" | wc -l` — record actual count (expected ~85)
7. Run: `find tests/ -name "*.py" | wc -l` — record actual count (expected ~34)
8. Run: `grep -c "^def " src/psx_ohlcv/db.py` — record function count (expected ~191)
9. Run: `pytest tests/ -x --tb=short -q 2>&1 | tail -20` — ALL tests must PASS

IMPORTANT: Steps 4-8 are BASELINE MEASUREMENTS. The actual numbers may differ 
from my estimates if the dev branch has evolved. Record whatever the real numbers 
are — those become our ground truth, not my predictions.

Do NOT proceed to any refactoring until step 9 shows all tests green.
Report the exact output of each command.
```

### Prompt 0.2 — Git Safety Baseline
```
Create a safety branch before we touch anything:

1. `git stash` (if any uncommitted changes)
2. `git checkout -b refactor/phase1-split-db` from dev
3. `git log --oneline -5` — show me the last 5 commits for reference
4. Confirm branch name is `refactor/phase1-split-db`

This branch is our rollback point if anything breaks.
```

---

## PHASE 1: SPLIT db.py (monolith → 10 repository modules)

### Design Principle
- Create `src/psx_ohlcv/db/` package
- Move schema SQL to `schema.py`
- Move connection logic to `connection.py`
- Group ALL public functions into domain repositories
- Create `__init__.py` that re-exports EVERYTHING so existing imports don't break
- **Zero behavior change. Only file reorganization.**
- The exact function count comes from the BASELINE measured in Prompt 0.1

### Prompt 1.1 — Create Package Structure
```
We are splitting src/psx_ohlcv/db.py (8607 lines, 191 functions) into a package.

Step 1: Create the directory structure. Do NOT move any code yet. Just create empty files:

mkdir -p src/psx_ohlcv/db/repositories

Create these empty files (just a docstring in each):
- src/psx_ohlcv/db/__init__.py
- src/psx_ohlcv/db/connection.py
- src/psx_ohlcv/db/schema.py
- src/psx_ohlcv/db/migrations.py
- src/psx_ohlcv/db/repositories/__init__.py
- src/psx_ohlcv/db/repositories/symbols.py
- src/psx_ohlcv/db/repositories/eod.py
- src/psx_ohlcv/db/repositories/intraday.py
- src/psx_ohlcv/db/repositories/company.py
- src/psx_ohlcv/db/repositories/market.py
- src/psx_ohlcv/db/repositories/instruments.py
- src/psx_ohlcv/db/repositories/fixed_income.py
- src/psx_ohlcv/db/repositories/jobs.py
- src/psx_ohlcv/db/repositories/user.py
- src/psx_ohlcv/db/repositories/analytics_db.py

After creating, run: find src/psx_ohlcv/db/ -name "*.py" | sort
Confirm 15 files exist.
```

### Prompt 1.2 — Extract Schema SQL
```
Now extract ONLY the SQL schema from db.py into db/schema.py.

Read src/psx_ohlcv/db.py and find the SCHEMA_SQL variable 
(it's a triple-quoted string starting with CREATE TABLE IF NOT EXISTS symbols).

Copy the ENTIRE SCHEMA_SQL string (all CREATE TABLE and CREATE INDEX statements) 
into src/psx_ohlcv/db/schema.py as:

SCHEMA_SQL = """
... (all the CREATE statements)
"""

IMPORTANT:
- Copy EXACTLY as-is, no modifications
- Include ALL tables (symbols, eod_ohlcv, sync_runs, sync_failures, intraday_bars, 
  intraday_sync_state, sectors, company_profile, company_key_people, 
  company_quote_snapshots, company_signal_snapshots, company_fundamentals, 
  company_fundamentals_history, company_financials, company_ratios, company_payouts,
  financial_announcements, user_interactions, company_snapshots, trading_sessions,
  corporate_announcements, company_announcements, equity_structure, scrape_jobs,
  job_notifications, psx_indices, psx_market_stats, corporate_events, 
  dividend_payouts, announcements_sync_status, instruments, instrument_membership,
  ohlcv_instruments, instrument_rankings, instruments_sync_runs,
  fx_pairs, fx_ohlcv, fx_adjusted_metrics, fx_sync_runs,
  mutual_funds, mutual_fund_nav, mutual_fund_sync_runs,
  bonds_master, bond_quotes, bond_yield_curve, bond_analytics_snapshots, bond_sync_runs,
  sukuk_master, sukuk_quotes, sukuk_yield_curve, sukuk_analytics_snapshots,
  sbp_primary_market_docs, sukuk_sync_runs,
  fi_instruments, fi_quotes, fi_yield_curve, fi_analytics, sbp_pma_docs,
  fi_events, fi_sync_runs, policy_rates, kibor_rates)
- Include ALL CREATE INDEX statements

VERIFY: After extraction, count CREATE TABLE statements in schema.py:
  grep -c "CREATE TABLE" src/psx_ohlcv/db/schema.py
Expected: ~50+ tables

Do NOT modify db.py yet. We are only copying at this stage.
```

### Prompt 1.3 — Extract Connection Logic
```
Extract connection and initialization functions into db/connection.py.

From db.py, move these functions to db/connection.py:
1. connect() — the function that creates sqlite3.Connection
2. init_schema() — the function that executes SCHEMA_SQL
3. Any migration functions: _migrate_symbols_table, _migrate_eod_ohlcv_table, _migrate_scrape_jobs_table

db/connection.py should:
- Import sqlite3, Path
- Import SCHEMA_SQL from .schema
- Import get_db_path, ensure_dirs from psx_ohlcv.config
- Import now_iso from psx_ohlcv.models
- Define connect(), init_schema(), and migration functions

VERIFY: 
  python -c "from psx_ohlcv.db.connection import connect, init_schema; print('OK')"

Do NOT modify the original db.py yet.
```

### Prompt 1.4 — Extract Symbol Repository
```
Extract symbol-related functions into db/repositories/symbols.py.

These functions from db.py go into symbols.py:
- upsert_symbols
- get_symbols_list
- get_symbols_string
- get_unified_symbols_list
- get_unified_symbol_count
- get_sector_map
- get_sector_name
- get_sectors
- upsert_sectors
- sync_sector_names_from_company_profile
- get_symbol_activity

Each function should keep its EXACT same signature (same parameters, same return type).
At the top of symbols.py, add: import sqlite3 and any other needed imports.

VERIFY:
  python -c "from psx_ohlcv.db.repositories.symbols import upsert_symbols, get_symbols_list; print('OK')"
```

### Prompt 1.5 — Extract EOD Repository
```
Extract EOD-related functions into db/repositories/eod.py.

Functions to move:
- upsert_eod
- get_eod_ohlcv
- get_eod_dates
- get_eod_date_range
- get_eod_date_count
- get_eod_date_source_breakdown
- get_eod_source_summary
- check_eod_date_exists
- get_max_date_for_symbol
- get_date_range_for_symbol
- get_global_date_stats
- get_data_coverage_summary
- backfill_eod_sources
- verify_eod_data_sources
- ingest_market_summary_csv
- ingest_all_market_summary_csvs

Also move sync run tracking:
- record_sync_run_start
- record_sync_run_end
- record_failure

Keep exact signatures. Add needed imports at top.

VERIFY:
  python -c "from psx_ohlcv.db.repositories.eod import upsert_eod, get_eod_ohlcv, record_sync_run_start; print('OK')"
```

### Prompt 1.6 — Extract Intraday Repository
```
Extract intraday functions into db/repositories/intraday.py:

- upsert_intraday
- get_intraday_latest
- get_intraday_range
- get_intraday_stats
- get_intraday_sync_state
- update_intraday_sync_state
- _parse_ts_to_epoch

VERIFY:
  python -c "from psx_ohlcv.db.repositories.intraday import upsert_intraday, get_intraday_sync_state; print('OK')"
```

### Prompt 1.7 — Extract Company Repository
```
Extract company-related functions into db/repositories/company.py:

- upsert_company_profile / get_company_profile
- replace_company_key_people / get_company_key_people  
- insert_quote_snapshot / get_quote_snapshots / get_last_quote_hash / get_all_latest_quotes
- upsert_company_fundamentals / get_company_fundamentals / save_fundamentals_history
- upsert_company_financials / get_company_financials
- upsert_company_ratios / get_company_ratios
- upsert_company_payouts / get_company_payouts
- upsert_company_snapshot / get_company_snapshot
- upsert_equity_structure / get_equity_structure
- upsert_trading_session / get_trading_sessions
- upsert_corporate_announcement / get_corporate_announcements
- upsert_financial_announcement / upsert_financial_announcements / get_financial_announcements
- get_company_unified

VERIFY:
  python -c "from psx_ohlcv.db.repositories.company import upsert_company_profile, get_company_unified; print('OK')"
```

### Prompt 1.8 — Extract Market Repository
```
Extract market/index functions into db/repositories/market.py:

- upsert_index_data / get_latest_index / get_index_history / get_all_latest_indices / get_latest_kse100
- get_latest_market_stats
- upsert_yield_curve_point / get_yield_curve / get_latest_yield_curve

VERIFY:
  python -c "from psx_ohlcv.db.repositories.market import get_latest_kse100, get_latest_market_stats; print('OK')"
```

### Prompt 1.9 — Extract Instruments Repository
```
Extract instrument functions into db/repositories/instruments.py:

- upsert_instrument / get_instruments / get_instrument_by_id / get_instrument_by_symbol
- upsert_instruments_batch / resolve_instrument_id
- upsert_ohlcv_instrument / get_ohlcv_instrument / get_instrument_latest_date
- upsert_instrument_ranking / get_instrument_rankings / get_latest_ranking_date
- create_instruments_sync_run / update_instruments_sync_run

VERIFY:
  python -c "from psx_ohlcv.db.repositories.instruments import upsert_instrument, get_instruments; print('OK')"
```

### Prompt 1.10 — Extract Fixed Income Repository
```
Extract all FI functions into db/repositories/fixed_income.py:

BONDS:
- upsert_bond / get_bonds / get_bond / get_bond_by_symbol
- upsert_bond_quote / upsert_bond_quotes_batch / get_bond_quotes / get_bond_latest_quote
- upsert_bond_analytics / get_bond_analytics
- record_bond_sync_run / update_bond_sync_run / get_bond_sync_runs / get_bond_data_summary

SUKUK:
- upsert_sukuk / get_sukuk_list / get_sukuk
- upsert_sukuk_quote / get_sukuk_quotes / get_sukuk_latest_quote
- upsert_sukuk_yield_curve_point / get_sukuk_yield_curve / get_sukuk_latest_yield_curve / get_available_curve_dates
- upsert_sukuk_analytics / get_sukuk_analytics
- upsert_sbp_document / get_sbp_documents
- record_sukuk_sync_run / update_sukuk_sync_run / get_sukuk_sync_runs / get_sukuk_data_summary

FI GENERIC:
- upsert_fi_instrument / get_fi_instruments / get_fi_instrument
- upsert_fi_quote / get_fi_quotes / get_fi_latest_quote
- upsert_fi_curve_point / get_fi_curve / get_fi_curve_dates
- upsert_fi_analytics / get_fi_analytics
- upsert_sbp_pma_doc / get_sbp_pma_docs
- upsert_fi_event
- record_fi_sync_run / update_fi_sync_run / get_fi_sync_runs / get_fi_data_summary

FX:
- upsert_fx_pair / get_fx_pairs / get_fx_pair
- upsert_fx_ohlcv / get_fx_ohlcv / get_fx_latest_rate / get_fx_latest_date
- upsert_fx_adjusted_metric / get_fx_adjusted_metrics
- record_fx_sync_run / update_fx_sync_run / get_fx_sync_runs

MUTUAL FUNDS:
- upsert_mutual_fund / get_mutual_funds / get_mutual_fund / get_mutual_fund_by_symbol
- upsert_mf_nav / get_mf_nav / get_mf_latest_nav / get_mf_latest_date
- record_mf_sync_run / update_mf_sync_run / get_mf_sync_runs / get_mf_data_summary

POLICY RATES:
- upsert_policy_rate / get_latest_policy_rate / get_policy_rates
- upsert_kibor_rate / get_kibor_rates / get_latest_kibor_rates

VERIFY:
  python -c "from psx_ohlcv.db.repositories.fixed_income import upsert_bond, get_sukuk_list, upsert_fx_pair; print('OK')"
```

### Prompt 1.11 — Extract Jobs Repository
```
Extract job/scrape functions into db/repositories/jobs.py:

- create_scrape_job / get_scrape_job / update_scrape_job / get_recent_jobs / get_running_jobs
- create_background_job (if different from create_scrape_job)
- update_job_progress
- request_job_stop / is_job_stop_requested
- add_job_notification / get_unread_notifications / mark_notification_read / mark_all_notifications_read

VERIFY:
  python -c "from psx_ohlcv.db.repositories.jobs import create_scrape_job, update_job_progress; print('OK')"
```

### Prompt 1.12 — Extract User Repository
```
Extract user interaction functions into db/repositories/user.py:

- log_interaction
- get_recent_interactions
- get_session_interactions
- get_interaction_stats

VERIFY:
  python -c "from psx_ohlcv.db.repositories.user import log_interaction; print('OK')"
```

### Prompt 1.13 — Create the __init__.py Re-Export Layer (CRITICAL)
```
THIS IS THE MOST IMPORTANT STEP. The db/__init__.py must re-export EVERY 
function that was previously importable from psx_ohlcv.db.

The reason: the entire codebase has imports like:
  from psx_ohlcv.db import connect, upsert_eod, get_symbols_list

These must ALL continue to work without any changes to the callers.

Create src/psx_ohlcv/db/__init__.py that does:

from .connection import connect, init_schema
from .schema import SCHEMA_SQL
from .repositories.symbols import (upsert_symbols, get_symbols_list, get_symbols_string, ...)
from .repositories.eod import (upsert_eod, get_eod_ohlcv, record_sync_run_start, ...)
from .repositories.intraday import (upsert_intraday, ...)
from .repositories.company import (upsert_company_profile, ...)
from .repositories.market import (...)
from .repositories.instruments import (...)
from .repositories.fixed_income import (...)
from .repositories.jobs import (...)
from .repositories.user import (...)

EVERY function from the original db.py must appear in __init__.py.

VERIFY (critical) — compare against the ORIGINAL db.py, not a hardcoded number:

  # Step 1: Count what the original has
  grep -c "^def " src/psx_ohlcv/db.py
  # Record this number as EXPECTED_COUNT
  
  # Step 2: Count what the new package exports
  python -c "
  import psx_ohlcv.db as db
  funcs = [name for name in dir(db) if not name.startswith('_') and callable(getattr(db, name))]
  print(f'Exported functions: {len(funcs)}')
  for f in sorted(funcs):
      print(f'  {f}')
  "
  # This count must be >= EXPECTED_COUNT from Step 1
  
  # Step 3: Find any missing functions
  python -c "
  import ast, inspect
  import psx_ohlcv.db as db_pkg
  
  # Parse the original file to get all function names
  with open('src/psx_ohlcv/db.py') as f:
      tree = ast.parse(f.read())
  original_funcs = {node.name for node in ast.walk(tree) 
                    if isinstance(node, ast.FunctionDef) and not node.name.startswith('_')}
  
  # Get exported names from package
  pkg_names = {n for n in dir(db_pkg) if not n.startswith('_') and callable(getattr(db_pkg, n))}
  
  missing = original_funcs - pkg_names
  if missing:
      print(f'MISSING {len(missing)} functions:')
      for m in sorted(missing):
          print(f'  {m}')
  else:
      print('ALL functions exported — ready to proceed')
  "

Then run the full test suite:
  pytest tests/ -x --tb=short -q 2>&1 | tail -20

ALL tests must still pass. If any fail, fix the import issue before proceeding.
```

### Prompt 1.14 — Replace Original db.py
```
NOW and only now, replace the original monolith:

1. mv src/psx_ohlcv/db.py src/psx_ohlcv/db_old_backup.py
   (the db/ package directory already exists and Python will use it)

2. Run the full test suite again:
   pytest tests/ -x --tb=short -q

3. Run the verify script:
   python scripts/verify_features.py

4. If ALL pass, commit:
   git add -A
   git commit -m "refactor: split db.py monolith into domain repositories

   Moved into src/psx_ohlcv/db/ package:
   - schema.py: All CREATE TABLE/INDEX SQL
   - connection.py: connect(), init_schema(), migrations
   - repositories/symbols.py: Symbol and sector functions
   - repositories/eod.py: EOD OHLCV and sync tracking
   - repositories/intraday.py: Intraday bars and sync state
   - repositories/company.py: Fundamentals, financials, ratios, payouts
   - repositories/market.py: Indices, market stats, yield curves
   - repositories/instruments.py: ETFs, REITs, instruments universe
   - repositories/fixed_income.py: Bonds, sukuk, FX, mutual funds, policy rates
   - repositories/jobs.py: Scrape jobs, notifications
   - repositories/user.py: User interaction tracking
   
   All functions re-exported from db/__init__.py for backward compatibility.
   Zero behavior change. All existing tests pass."

5. If any test fails: git checkout -- . and debug the specific import.

⚠️ UPDATE SESSION STATE: Mark Phase 1 complete, update branch info, log any issues.
```

---

## PHASE 2: SPLIT app.py (11,265 lines → page modules)

### Prompt 2.0 — Branch Setup
```
Phase 1 is complete on branch refactor/phase1-split-db.
Phase 2 needs the db/ package from Phase 1, so we branch FROM Phase 1.

OPTION A — Branch directly from Phase 1 (simpler, recommended):
  git branch --show-current  
  # Should show: refactor/phase1-split-db
  
  git checkout -b refactor/phase2-split-ui
  # This creates Phase 2 branch WITH all Phase 1 changes included
  
  git log --oneline -3  
  # Should show the Phase 1 commit at top

OPTION B — Merge Phase 1 to dev first, then branch (cleaner history):
  git checkout dev
  git merge refactor/phase1-split-db --no-ff -m "merge: Phase 1 db.py split into domain repositories"
  pytest tests/ -x --tb=short -q 2>&1 | tail -5  # Verify merge is clean
  git checkout -b refactor/phase2-split-ui
  
  # If merge has conflicts: STOP, resolve conflicts, run tests, then continue.

Pick ONE option. Confirm which branch you're on and that tests still pass:
  git branch --show-current
  pytest tests/ -x --tb=short -q 2>&1 | tail -10

Update session state:
  - Active Branch: refactor/phase2-split-ui
  - Active Phase: Phase 2 (Split app.py)
  - Mark 2.0 as [x]
```

### Prompt 2.1 — Analyze app.py Structure
```
Read src/psx_ohlcv/ui/app.py and identify all page sections.

I need you to:
1. Find every st.sidebar section or page tab that defines a distinct page
2. List each page with its approximate line range
3. Identify shared helper functions used across multiple pages
4. Identify shared state (session_state keys used)

Output a mapping like:
  Page: Dashboard → lines X-Y → helper functions used: [...]
  Page: Candlestick Explorer → lines X-Y → ...
  (etc)

Also list all functions defined inside app.py:
  grep -n "^def \|^    def " src/psx_ohlcv/ui/app.py | head -100

Do NOT modify any code. Analysis only.
```

### Prompt 2.2 — Create Page Module Structure
```
Based on the analysis from 2.1, create the page module directory:

mkdir -p src/psx_ohlcv/ui/page_views
mkdir -p src/psx_ohlcv/ui/components

Create empty files with docstrings:
- src/psx_ohlcv/ui/page_views/__init__.py
- src/psx_ohlcv/ui/page_views/dashboard.py
- src/psx_ohlcv/ui/page_views/candlestick.py
- src/psx_ohlcv/ui/page_views/intraday.py
- src/psx_ohlcv/ui/page_views/regular_market.py
- src/psx_ohlcv/ui/page_views/history.py
- src/psx_ohlcv/ui/page_views/symbols.py
- src/psx_ohlcv/ui/page_views/sync_monitor.py
- src/psx_ohlcv/ui/page_views/ai_insights.py
- src/psx_ohlcv/ui/page_views/company_deep.py
- src/psx_ohlcv/ui/page_views/settings.py
- src/psx_ohlcv/ui/components/__init__.py
- src/psx_ohlcv/ui/components/sidebar.py
- src/psx_ohlcv/ui/components/helpers.py

Do not move code yet.
```

### Prompt 2.3 — Extract Shared Helpers
```
Extract helper functions used across multiple pages into components/helpers.py.

From app.py, identify functions like:
- get_sector_names(), add_sector_name_column()
- format_number(), format_pct()
- get_db_connection() or any shared DB accessor
- Any CSS/style injection functions
- Any shared KPI/metric rendering functions

Move these to components/helpers.py with exact same signatures.

VERIFY: python -c "from psx_ohlcv.ui.components.helpers import get_sector_names; print('OK')"
(adjust function names based on what actually exists)
```

### Prompt 2.4-2.13 — Extract Each Page (ONE prompt per page)
```
Extract the [PAGE_NAME] page from app.py into pages/[page_name].py.

Rules:
1. Create a render_[page_name]() function that contains all the page logic
2. Move all page-specific helper functions into the same file
3. Import shared helpers from components/helpers
4. Import DB functions from psx_ohlcv.db (these still work due to Phase 1)
5. Keep ALL streamlit session_state usage exactly as-is
6. The page should work when called as: render_[page_name]()

In app.py, replace the page section with:
  from psx_ohlcv.ui.page_views.[page_name] import render_[page_name]
  render_[page_name]()

After EACH page extraction, verify the Streamlit app still starts:
  timeout 10 streamlit run src/psx_ohlcv/ui/app.py --server.headless true 2>&1 | head -5

If it crashes, revert that page and debug before continuing.
```

### Prompt 2.14 — Final Commit
```
Verify everything works:
1. streamlit run src/psx_ohlcv/ui/app.py --server.headless true (must start without errors)
2. pytest tests/test_ui_charts.py -x -q
3. wc -l src/psx_ohlcv/ui/app.py 
   (should be ~200-500 lines now — compare to BASELINE from Prompt 0.1)

If all pass:
  git add -A
  git commit -m "refactor: split app.py monolith into page modules
  
  Created src/psx_ohlcv/ui/page_views/ with individual page modules.
  Created src/psx_ohlcv/ui/components/ for shared helpers.
  app.py reduced to ~300 lines (routing + shared state only).
  All UI functionality preserved."
```

⚠️ UPDATE SESSION STATE: Mark Phase 2 complete, record new app.py line count.

---

## PHASE 3: DATABASE LAYER — TWO PATHS

### Decision Point: Read This Before Starting

You have two options. Pick based on your actual situation:

**Path A — Optimize SQLite (recommended to start)**
- Zero new infrastructure
- No Docker, no RAM overhead, no disk bloat
- Fixes the real performance issues (WAL mode, connection pooling, indexes)
- Can migrate to PostgreSQL LATER when you actually hit SQLite's wall
- **Do this first. You can always do Path B later.**

**Path B — PostgreSQL + TimescaleDB (when you actually need it)**
- Requires Docker Desktop running (500MB-1GB RAM)
- Better for: concurrent read/write, 100M+ rows, time-series compression
- You need this when: intraday data exceeds 10GB, or multiple users hit the DB simultaneously
- **Do this only when SQLite becomes the bottleneck, not before.**

**Practical thresholds — when to upgrade:**
| Symptom | SQLite is fine | Time for PostgreSQL |
|---------|---------------|-------------------|
| EOD rows | < 5M rows | > 10M rows |
| Intraday rows | < 20M rows | > 50M rows |
| DB file size | < 5GB | > 10GB |
| Concurrent users | 1 (you) | 2+ simultaneous |
| Sync + UI at same time | Occasional lag | Hard locks/timeouts |
| Query speed (complex JOINs) | < 2 seconds | > 5 seconds |

Check your current size:
```bash
ls -lh /mnt/e/psxdata/psx.sqlite
sqlite3 /mnt/e/psxdata/psx.sqlite "SELECT COUNT(*) FROM eod_ohlcv;"
sqlite3 /mnt/e/psxdata/psx.sqlite "SELECT COUNT(*) FROM intraday_bars;"
```

If your DB is under 2GB and you're the only user — **Path A is the right call.**

---

### PATH A: SQLite Optimization (No Docker Required)

### Prompt 3.0A — Branch
```
Starting Phase 3, Path A — SQLite optimization.
No Docker needed. No PostgreSQL.

Check where we are and branch:
  git branch --show-current
  git checkout -b feat/phase3-sqlite-optimize

Confirm prerequisites from Phase 1+2:
  ls src/psx_ohlcv/db/__init__.py  # Phase 1 present
  pytest tests/ -x --tb=short -q 2>&1 | tail -5

Update session state: Active Branch: feat/phase3-sqlite-optimize, Path: A (SQLite)
```

### Prompt 3.1A — Enable WAL Mode + Connection Optimization
```
SQLite performance can be dramatically improved with configuration alone.
No schema changes needed.

Edit src/psx_ohlcv/db/connection.py to add these optimizations to connect():

After creating the connection, execute these PRAGMA statements:
  con.execute("PRAGMA journal_mode=WAL")        -- Write-Ahead Logging: allows concurrent reads during writes
  con.execute("PRAGMA synchronous=NORMAL")       -- Faster writes, still crash-safe with WAL
  con.execute("PRAGMA cache_size=-64000")         -- 64MB cache (default is 2MB)
  con.execute("PRAGMA busy_timeout=5000")         -- Wait 5 seconds on lock instead of failing immediately
  con.execute("PRAGMA temp_store=MEMORY")         -- Temp tables in RAM
  con.execute("PRAGMA mmap_size=268435456")       -- Memory-map 256MB of the DB file for faster reads
  con.execute("PRAGMA foreign_keys=ON")           -- Enforce foreign key constraints

Also add a connection reuse pattern:
  - Create a module-level _connection_cache dict keyed by db_path
  - Return existing connection if path matches and connection is still valid
  - This prevents opening 50+ connections during sync

VERIFY:
  python -c "
  from psx_ohlcv.db.connection import connect
  con = connect()
  result = con.execute('PRAGMA journal_mode').fetchone()
  print(f'Journal mode: {result[0]}')  # Must say 'wal'
  result = con.execute('PRAGMA cache_size').fetchone()
  print(f'Cache size: {result[0]}')    # Must say -64000
  "
  
  pytest tests/ -x --tb=short -q 2>&1 | tail -10
```

### Prompt 3.2A — Add Missing Indexes for Common Queries
```
Analyze the current schema for missing indexes on frequently queried columns.

Run these analysis queries against the actual database:
  sqlite3 /mnt/e/psxdata/psx.sqlite "
    SELECT name FROM sqlite_master WHERE type='index' ORDER BY name;
  "

Check if these high-value indexes exist, add any that are missing:

  -- Company fundamentals is queried by sector often
  CREATE INDEX IF NOT EXISTS idx_fundamentals_sector ON company_fundamentals(sector_name);
  
  -- Financial announcements queried by date range
  CREATE INDEX IF NOT EXISTS idx_fin_ann_date ON financial_announcements(announcement_date);
  
  -- Company quote snapshots queried by symbol+date
  CREATE INDEX IF NOT EXISTS idx_quote_snap_symbol_ts ON company_quote_snapshots(symbol, ts);
  
  -- Regular market snapshots queried by time range
  CREATE INDEX IF NOT EXISTS idx_reg_snap_ts ON regular_market_snapshots(ts);

  -- Covering index for common EOD query pattern (symbol + date range + OHLCV)
  CREATE INDEX IF NOT EXISTS idx_eod_symbol_date ON eod_ohlcv(symbol, date);

Add these to db/schema.py so they're created on fresh databases too.

VERIFY:
  sqlite3 /mnt/e/psxdata/psx.sqlite "
    EXPLAIN QUERY PLAN SELECT * FROM eod_ohlcv WHERE symbol='OGDC' AND date > '2024-01-01';
  "
  # Should show "USING INDEX" not "SCAN TABLE"
  
  pytest tests/ -x --tb=short -q 2>&1 | tail -5
```

### Prompt 3.3A — Add Database Maintenance Utilities
```
Create src/psx_ohlcv/db/maintenance.py with utility functions:

1. vacuum_database(con) — Run VACUUM to reclaim space and defragment
2. analyze_database(con) — Run ANALYZE to update query planner statistics
3. get_db_stats(con) — Return dict with:
   - File size in MB
   - Row counts per table
   - Index count
   - WAL file size
   - Free page count
4. check_integrity(con) — Run PRAGMA integrity_check
5. backup_database(con, backup_path) — Use sqlite3 backup API for hot backup

Add a CLI command to run maintenance:
  psx-ohlcv db-maintenance --vacuum --analyze --stats

VERIFY:
  python -c "
  from psx_ohlcv.db.maintenance import get_db_stats
  from psx_ohlcv.db.connection import connect
  con = connect()
  stats = get_db_stats(con)
  print(f'DB size: {stats[\"file_size_mb\"]:.1f} MB')
  print(f'Tables: {len(stats[\"table_counts\"])}')
  for table, count in sorted(stats[\"table_counts\"].items(), key=lambda x: -x[1])[:10]:
      print(f'  {table}: {count:,} rows')
  "
```

### Prompt 3.4A — Environment Configuration (No Docker)
```
Create src/psx_ohlcv/settings.py using pydantic-settings or plain dataclass:

from dataclasses import dataclass
from pathlib import Path
import os

@dataclass  
class Settings:
    # Database
    db_path: str = os.environ.get("PSX_DB_PATH", "/mnt/e/psxdata/psx.sqlite")
    
    # Data directories
    data_root: str = os.environ.get("PSX_DATA_ROOT", "/mnt/e/psxdata")
    csv_dir: str = ""  # Set in __post_init__
    logs_dir: str = ""
    
    # AI Providers  
    llm_provider: str = os.environ.get("PSX_LLM_PROVIDER", "openai")
    openai_api_key: str = os.environ.get("OPENAI_API_KEY", "")
    anthropic_api_key: str = os.environ.get("ANTHROPIC_API_KEY", "")
    
    # Sync settings
    sync_max_concurrent: int = int(os.environ.get("PSX_SYNC_CONCURRENT", "25"))
    sync_rate_limit: float = float(os.environ.get("PSX_SYNC_RATE_LIMIT", "0.05"))
    
    # API
    api_host: str = os.environ.get("PSX_API_HOST", "0.0.0.0")
    api_port: int = int(os.environ.get("PSX_API_PORT", "8000"))
    
    def __post_init__(self):
        self.csv_dir = os.path.join(self.data_root, "csv")
        self.logs_dir = os.path.join(self.data_root, "logs")

Create .env.example documenting all variables.
Update config.py to use Settings instead of hardcoded paths.

VERIFY:
  python -c "from psx_ohlcv.settings import Settings; s = Settings(); print(f'DB: {s.db_path}')"
  pytest tests/ -x --tb=short -q 2>&1 | tail -5
```

### Prompt 3.5A — Commit Phase 3A
```
pytest tests/ -x --tb=short -q
git add -A
git commit -m "feat: SQLite optimization + environment configuration

- WAL mode enabled (concurrent reads during writes)
- 64MB cache, memory-mapped I/O, 5s busy timeout
- Connection reuse to prevent handle exhaustion during sync
- Added missing indexes for common query patterns
- Database maintenance utilities (vacuum, analyze, stats, backup)
- Environment-based configuration via Settings dataclass
- .env.example for all configurable variables
- Zero infrastructure changes — no Docker required"
```

⚠️ UPDATE SESSION STATE: Mark Phase 3 complete (Path A), record DB stats.

---

### PATH B: PostgreSQL + TimescaleDB (Optional — Only When Needed)

**Prerequisites before starting Path B:**
- You've hit actual SQLite limitations (see threshold table above)
- Docker Desktop installed and configured for WSL2
- WSL2 memory limit set (create ~/.wslconfig):
  ```
  [wsl2]
  memory=8GB
  swap=4GB
  ```
  This prevents Docker from eating all 32GB.
- At least 20GB free disk space on target drive

**Docker disk management tips:**
- Store PostgreSQL data on E: drive (not C:): volume mount to /mnt/e/psxdata/pgdata
- Periodically compact WSL2 disk: `wsl --shutdown` then `Optimize-VHD` in PowerShell
- Monitor with: `docker system df` (shows disk usage)
- Clean up: `docker system prune -a` (removes unused images/containers)

### Prompt 3.0B — Docker Infrastructure
```
Starting Phase 3, Path B — PostgreSQL + TimescaleDB.

FIRST — configure WSL2 memory limit:
  Check if ~/.wslconfig exists:
  cat ~/.wslconfig 2>/dev/null || echo "No .wslconfig"
  
  If it doesn't exist or doesn't limit memory, warn me.
  Docker without limits will use up to 16GB of my 32GB RAM.

Create docker-compose.yml in project root with:

1. TimescaleDB service (timescale/timescaledb:latest-pg16)
   - Port 5432
   - Volume: /mnt/e/psxdata/pgdata (E: drive, NOT the WSL2 virtual disk)
   - Env: POSTGRES_DB=psxdata, POSTGRES_USER=psx, POSTGRES_PASSWORD from .env
   - shm_size: 256mb
   - mem_limit: 2g (cap at 2GB to prevent runaway)
   - Healthcheck: pg_isready

2. Redis service (redis:7-alpine)
   - Port 6379
   - Volume: /mnt/e/psxdata/redis (persistent, on E: drive)
   - mem_limit: 512m

After creating:
  docker compose up -d
  docker compose ps (both must show healthy/running)
  docker stats --no-stream (check actual memory usage)
  docker system df (check disk usage)
  
  docker compose exec timescaledb psql -U psx -d psxdata -c "SELECT version();"
  docker compose exec timescaledb psql -U psx -d psxdata -c "SELECT extversion FROM pg_extension WHERE extname='timescaledb';"

Report memory and disk usage numbers.
```

(Prompts 3.2B through 3.5B remain as originally written in the playbook — 
PostgreSQL schema, data migration, adapter layer, commit.)

---

## PHASE 4: ASYNC FETCHER + BACKGROUND WORKERS

### Prompt 4.0 — Branch
```
Phase 3 is complete. Now we start Phase 4 (Async + Workers).

Check where we are and branch:
  git branch --show-current
  git checkout -b feat/phase4-async
  
  OR if you merged previous phases to dev:
  git checkout dev && git pull
  git checkout -b feat/phase4-async

Confirm:
  git branch --show-current  # feat/phase4-async
  pytest tests/ -x --tb=short -q 2>&1 | tail -5

Check if Redis is available (from Phase 3 Path B):
  redis-cli ping 2>/dev/null || docker compose exec redis redis-cli ping 2>/dev/null || echo "NO_REDIS"

If NO_REDIS: Phase 4 will create the async fetcher (no Redis needed) 
but skip ARQ worker setup. Workers can use simple multiprocessing instead.

Update session state: Active Branch: feat/phase4-async
```

### Prompt 4.1 — Async HTTP Fetcher
```
Create src/psx_ohlcv/sources/async_fetcher.py.

Read the current sync fetcher pattern in:
- src/psx_ohlcv/http.py (create_session, polite_delay, fetch_url)
- src/psx_ohlcv/sources/eod.py (fetch_eod_json)
- src/psx_ohlcv/sync.py (the for-loop over symbols)

Create an async equivalent using aiohttp with:
- Semaphore-based concurrency limit (max 25 concurrent)
- Per-request rate limiting (0.05s between requests)
- TCPConnector with connection pooling (limit=50)
- Timeout handling (30s per request)
- Retry logic (3 retries with exponential backoff)
- Progress callback support

Functions needed:
- async fetch_eod(symbol) → (symbol, data, error)
- async fetch_eod_batch(symbols, progress_cb) → results dict
- async fetch_intraday(symbol) → (symbol, data, error)

Install aiohttp: pip install aiohttp

VERIFY with a timing test:
  python -c "
  import asyncio, time
  from psx_ohlcv.sources.async_fetcher import AsyncPSXFetcher
  
  async def test():
      symbols = ['OGDC', 'HBL', 'MCB', 'LUCK', 'PSO', 'ENGRO', 'PPL', 'HUBC', 'UBL', 'FFC']
      async with AsyncPSXFetcher() as f:
          start = time.time()
          r = await f.fetch_eod_batch(symbols)
          elapsed = time.time() - start
          print(f'{r[\"ok\"]} OK, {r[\"failed\"]} failed in {elapsed:.1f}s')
  
  asyncio.run(test())
  "
  
Should complete 10 symbols in <3 seconds (vs 10+ seconds synchronously).
```

### Prompt 4.2 — Background Worker Setup
```
Check if Redis is available:
  redis-cli ping 2>/dev/null && echo "REDIS_OK" || echo "NO_REDIS"

IF REDIS_OK — Use ARQ (async Redis queue):
  Install ARQ: pip install arq

  Create src/psx_ohlcv/worker_arq.py with:
  1. Task: sync_eod_task(ctx, symbols=None) — async EOD sync using AsyncPSXFetcher
  2. Task: deep_scrape_task(ctx, symbols, batch_size=50)
  3. Task: sync_intraday_task(ctx, symbols)
  4. Cron: Daily EOD sync at 13:00 UTC (18:00 PKT)

  WorkerSettings class with:
  - redis_settings pointing to localhost:6379
  - functions list
  - cron_jobs list

  VERIFY:
    arq psx_ohlcv.worker_arq.WorkerSettings &
    sleep 2
    python -c "
    import asyncio
    from arq import create_pool
    from arq.connections import RedisSettings
    async def test():
        pool = await create_pool(RedisSettings())
        job = await pool.enqueue_job('sync_eod_task', symbols=['OGDC', 'HBL'])
        print(f'Job enqueued: {job.job_id}')
        result = await job.result(timeout=30)
        print(f'Result: {result}')
    asyncio.run(test())
    "
    kill %1

IF NO_REDIS — Use asyncio-based worker (no external dependencies):
  Create src/psx_ohlcv/worker_async.py with:
  1. AsyncWorker class using asyncio.Queue internally
  2. Same task functions but running in-process
  3. Can be started as: python -m psx_ohlcv.worker_async
  4. Schedule via cron/systemd instead of in-process cron

  VERIFY:
    python -c "
    import asyncio
    from psx_ohlcv.worker_async import AsyncWorker
    async def test():
        worker = AsyncWorker()
        result = await worker.run_task('sync_eod', symbols=['OGDC', 'HBL'])
        print(f'Result: {result}')
    asyncio.run(test())
    "

Either approach must complete without errors.
```

### Prompt 4.3 — Commit Phase 4
```
pytest tests/ -x -q  # All tests still pass
git add -A
git commit -m "feat: async HTTP fetcher + ARQ background workers

- AsyncPSXFetcher: aiohttp-based, 25 concurrent, rate-limited
- ARQ worker: async task queue with Redis
- Cron: daily EOD sync at 18:00 PKT
- 20x speed improvement for bulk symbol fetch"
```

⚠️ UPDATE SESSION STATE: Mark Phase 4 complete, record async benchmark times.

---

## PHASE 5: EXPAND FASTAPI + CONSOLIDATE AI

### Prompt 5.0 — Branch
```
Phase 4 is complete. Now we start Phase 5 (API + AI + CI).
Phase 5 needs everything from Phases 1-4.

Check where we are and branch:
  git branch --show-current
  git checkout -b feat/phase5-api-ai
  
  OR if you merged previous phases to dev:
  git checkout dev && git pull
  git checkout -b feat/phase5-api-ai

Confirm prerequisites:
  ls src/psx_ohlcv/db/__init__.py       # Phase 1
  ls src/psx_ohlcv/ui/page_views/ 2>/dev/null # Phase 2
  docker compose ps 2>/dev/null          # Phase 3
  ls src/psx_ohlcv/sources/async_fetcher.py 2>/dev/null  # Phase 4
  pytest tests/ -x --tb=short -q 2>&1 | tail -5

Update session state: Active Branch: feat/phase5-api-ai
```

### Prompt 5.1 — Add API Routers
```
Read the existing API structure:
- src/psx_ohlcv/api/main.py
- src/psx_ohlcv/api/routers/eod.py (existing)
- src/psx_ohlcv/api/routers/tasks.py (existing)

Add new routers:
1. routers/symbols.py
   - GET /api/symbols — list all symbols (with filters: active, sector)
   - GET /api/symbols/{symbol} — symbol detail
   
2. routers/market.py
   - GET /api/market/indices — latest KSE-100, KSE-30, etc.
   - GET /api/market/breadth — gainers/losers/unchanged counts
   - GET /api/market/live — current regular market data
   
3. routers/company.py
   - GET /api/company/{symbol}/profile
   - GET /api/company/{symbol}/fundamentals
   - GET /api/company/{symbol}/financials
   - GET /api/company/{symbol}/quotes — historical quote snapshots
   
4. routers/instruments.py
   - GET /api/instruments — list all instruments (ETFs, REITs, indices)
   - GET /api/instruments/{id}/ohlcv
   
5. routers/fi.py
   - GET /api/fi/bonds
   - GET /api/fi/sukuk
   - GET /api/fi/yield-curve/{curve_name}
   - GET /api/fi/fx-rates

Register all new routers in main.py.

VERIFY:
  uvicorn psx_ohlcv.api.main:app --port 8000 &
  sleep 2
  curl -s http://localhost:8000/docs | head -5  # Should return Swagger HTML
  curl -s http://localhost:8000/api/symbols | python -m json.tool | head -10
  kill %1
```

### Prompt 5.2 — WebSocket Endpoints
```
Add WebSocket support to FastAPI:

1. routers/ws.py
   - WS /ws/market-feed — pushes market data updates
   - WS /ws/sync-status — pushes sync progress updates

2. Use Redis pub/sub as the message bus:
   - Workers publish to Redis channels
   - WebSocket handlers subscribe and forward to clients

VERIFY:
  # Install websockets client: pip install websockets
  python -c "
  import asyncio, websockets, json
  async def test():
      async with websockets.connect('ws://localhost:8000/ws/sync-status') as ws:
          print('Connected to sync-status WebSocket')
          # Just verify connection works
          print('OK')
  asyncio.run(test())
  "
```

### Prompt 5.3 — Consolidate LLM → Agents
```
The codebase has two parallel AI systems:
1. src/psx_ohlcv/llm/ (legacy: cache.py, client.py, data_loader.py, prompts.py)
2. src/psx_ohlcv/agents/ (new: base.py, orchestrator.py, llm_client.py, config.py)

Consolidate by:
1. Move llm/cache.py → agents/cache.py (keep TTL caching logic)
2. Convert llm/data_loader.py functions into agent tools under tools/
3. Move useful prompts from llm/prompts.py into agent system prompts
4. Update all imports in ui/chat.py and ui/app.py

After consolidation:
- The llm/ directory should be empty or deleted
- All AI functionality goes through agents/
- The UI chat should still work

VERIFY:
  python -c "from psx_ohlcv.agents import AgentOrchestrator, chat; print('OK')"
  # Verify old imports are handled:
  python -c "
  try:
      from psx_ohlcv.llm import LLMClient
      print('WARN: old llm module still importable — add deprecation warning')
  except ImportError:
      print('OK: old llm module properly removed')
  "
```

### Prompt 5.4 — Environment Configuration
```
Replace hardcoded paths with environment-based configuration.

Create src/psx_ohlcv/settings.py using pydantic-settings:

from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Database
    db_backend: str = "sqlite"  # "sqlite" or "postgresql"
    sqlite_path: str = "/mnt/e/psxdata/psx.sqlite"
    database_url: str = "postgresql+asyncpg://psx:password@localhost/psxdata"
    
    # Redis
    redis_url: str = "redis://localhost:6379"
    
    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    
    # AI Providers
    openai_api_key: str = ""
    anthropic_api_key: str = ""
    llm_provider: str = "openai"  # "openai" or "anthropic"
    
    # Data
    data_root: str = "/mnt/e/psxdata"
    
    class Config:
        env_prefix = "PSX_"
        env_file = ".env"

Create .env.example with all variables documented.

Update config.py to use Settings instead of hardcoded paths.

VERIFY:
  python -c "from psx_ohlcv.settings import Settings; s = Settings(); print(f'DB: {s.db_backend}, Data: {s.data_root}')"
```

### Prompt 5.5 — GitHub Actions CI
```
Create .github/workflows/ci.yml:

name: CI
on: [push, pull_request]
jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install ruff
      - run: ruff check src/

  test:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: timescale/timescaledb:latest-pg16
        env:
          POSTGRES_DB: test_psxdata
          POSTGRES_USER: test
          POSTGRES_PASSWORD: test
        ports: ["5432:5432"]
        options: --health-cmd pg_isready --health-interval 10s --health-timeout 5s --health-retries 5
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install -e ".[dev]"
      - run: pytest tests/ -x --tb=short -q
        env:
          PSX_DB_BACKEND: sqlite

VERIFY: The YAML is valid:
  python -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml')); print('Valid YAML')"
```

### Prompt 5.6 — Final Commit Phase 5
```
pytest tests/ -x -q
git add -A
git commit -m "feat: expanded API, WebSocket, consolidated AI, CI/CD

- Added 5 new FastAPI routers (symbols, market, company, instruments, fi)
- WebSocket endpoints for market feed and sync status
- Consolidated llm/ module into agents/ system
- Environment-based configuration (pydantic-settings)
- GitHub Actions CI with linting and testing"
```

⚠️ UPDATE SESSION STATE: Mark Phase 5 complete, record API endpoint count.

---

## PHASE 6: MERGE ALL TO DEV

### Prompt 6.1 — Integration
```
All 5 phases are complete. Now merge everything to dev.

First, check which branching strategy was used:
  git branch --list "refactor/*" "feat/*"

SCENARIO A — Branches were CHAINED (each branched from previous):
  The latest branch (feat/phase5-api-ai) already contains ALL changes.
  Just merge that one branch to dev:
  
  git checkout dev
  git merge feat/phase5-api-ai --no-ff -m "merge: all phases (db split, UI split, PostgreSQL, async, API)"

SCENARIO B — Each phase was merged to dev individually along the way:
  dev already has everything. Just verify:
  
  git checkout dev
  git log --oneline -15  # Should show all phase merge commits

SCENARIO C — Mixed (some chained, some merged):
  Merge any unmerged branches to dev in order:
  
  git checkout dev
  # For each unmerged branch, in order:
  git merge <branch> --no-ff -m "merge: <phase description>"
  # Run tests after each merge:
  pytest tests/ -x --tb=short -q 2>&1 | tail -5
  # If conflicts: resolve, test, then continue

Final verification on dev:
  git checkout dev
  pytest tests/ -x --tb=short
  python scripts/verify_features.py
  ruff check src/ 2>/dev/null || echo "ruff not installed, skip lint"
  
  wc -l src/psx_ohlcv/db.py 2>/dev/null || echo "db.py replaced by db/ package ✓"
  wc -l src/psx_ohlcv/ui/app.py
  find src/psx_ohlcv/db/repositories/ -name "*.py" | wc -l
  find src/psx_ohlcv/ui/page_views/ -name "*.py" 2>/dev/null | wc -l

If all clean, tag the release:
  git tag -a v2.0.0 -m "Major refactor: modular DB, split UI, PostgreSQL, async, expanded API"
  git push origin dev --tags

Now safe to delete the backup:
  rm src/psx_ohlcv/db_legacy_backup.py 2>/dev/null
  git add -A && git commit -m "chore: remove db.py legacy backup"

Update session state: ALL PHASES COMPLETE 🎉
```

---

## VERIFICATION CHECKLIST (Run After Each Phase)

```bash
#!/bin/bash
# scripts/verify_all.sh
echo "=== VERIFICATION CHECKLIST ==="

echo "1. Tests..."
pytest tests/ -x --tb=short -q
TESTS=$?

echo "2. Feature verification..."
python scripts/verify_features.py
FEATURES=$?

echo "3. Linting..."
ruff check src/ --select E,F --quiet
LINT=$?

echo "4. Import check..."
python -c "from psx_ohlcv.db import connect, upsert_eod, get_symbols_list; print('DB imports OK')"
python -c "from psx_ohlcv.agents import AgentOrchestrator; print('Agents OK')"
python -c "from psx_ohlcv.sync import sync_all; print('Sync OK')"
IMPORTS=$?

echo "5. API starts..."
timeout 5 uvicorn psx_ohlcv.api.main:app --port 19999 2>/dev/null &
sleep 2
curl -sf http://localhost:19999/health > /dev/null && echo "API OK" || echo "API FAIL"
kill %1 2>/dev/null
API=$?

echo ""
echo "=== RESULTS ==="
[ $TESTS -eq 0 ] && echo "✅ Tests" || echo "❌ Tests"
[ $FEATURES -eq 0 ] && echo "✅ Features" || echo "❌ Features"
[ $LINT -eq 0 ] && echo "✅ Lint" || echo "❌ Lint"
[ $IMPORTS -eq 0 ] && echo "✅ Imports" || echo "❌ Imports"
```

---

## SESSION TIME ESTIMATES

| Phase | Prompts | Est. Time | Dependencies |
|-------|---------|-----------|--------------|
| 0: Pre-flight | 2 | 15 min | None |
| 1: Split db.py | 14 | 5-7 hours | Phase 0 |
| 2: Split app.py | 14 | 4-6 hours | Phase 1 |
| 3: PostgreSQL | 5 | 6-8 hours | Docker, Phase 1 |
| 4: Async + Workers | 3 | 3-4 hours | Redis from Phase 3 |
| 5: API + AI + CI | 6 | 4-6 hours | Phases 1-4 |
| 6: Integration | 1 | 1 hour | All phases |

**Total: ~45 prompts, ~25-35 Claude Code hours**

---

## RULES FOR EVERY PROMPT

1. **One concern per prompt** — Don't ask Claude Code to do 5 things at once
2. **VERIFY gate at end** — Every prompt ends with a concrete verification command
3. **No proceeding on red** — If VERIFY fails, fix before next prompt
4. **Git commit after each phase** — Rollback point always available
5. **No behavior changes during refactoring** — Phase 1 and 2 are pure restructuring
6. **Test suite is the truth** — 33 test files must pass at every checkpoint
7. **Backward compatibility via re-exports** — Old imports must continue working
8. **Update session state after every prompt** — The state file is your lifeline

---

## ISSUE DIAGNOSIS & RESOLUTION — WHEN VERIFY FAILS

### The Problem
After any prompt, the VERIFY step might fail. Claude Code might say:
- "3 tests failed"
- "ImportError: cannot import name 'upsert_eod' from 'psx_ohlcv.db'"
- "pip install failed"
- "Docker container not starting"
- Something unexpected

**You need a structured way to diagnose, fix, and continue — not just stare at it.**

### Universal Issue Diagnosis Prompt
**Paste this whenever a VERIFY step fails:**
```
The VERIFY step for prompt [X.Y] failed. Here's what happened:
[PASTE THE ERROR OUTPUT HERE]

Please diagnose this issue:
1. Read the full error message — what is it actually saying?
2. Identify the ROOT CAUSE (not the symptom)
3. Check if this is a:
   a) Import error → missing re-export in __init__.py
   b) Test failure → show me the exact test that failed with full traceback
   c) Dependency issue → missing pip package
   d) Docker issue → container logs
   e) File not found → wrong path
   f) Syntax error → show exact file and line
4. Propose a FIX (show exact code change, not vague description)
5. After applying fix, re-run the VERIFY step
6. If VERIFY passes now, log the issue and resolution in the session state file

Do NOT move to the next prompt until VERIFY passes.
```

---

### PHASE-SPECIFIC ISSUE GUIDES

#### Phase 0 (Pre-flight) — Common Issues and Fixes

**Issue: Prompt 0.1 Step 1 — Wrong branch**
```
Prompt 0.1 returned branch "master" instead of "dev".

Fix this:
1. Run: git fetch origin
2. Run: git branch -a | grep dev
3. If dev exists: git checkout dev
4. If dev doesn't exist: git checkout -b dev origin/dev
5. Confirm: git branch --show-current (must say "dev")
```

**Issue: Prompt 0.1 Step 2 — Python version too old**
```
Prompt 0.1 shows Python version is below 3.10.

The codebase uses PEP 604 union types (X | None) which require Python 3.10+.
First, check what the project itself declares:
  cat pyproject.toml | grep -i "requires-python"
  
Use whatever version pyproject.toml says. If it doesn't specify, minimum is 3.10.

Fix options (pick one):

Option A — conda (recommended if already using conda):
  1. Run: conda info --envs (list existing environments)
  2. If a psx env exists: conda activate <env_name> && python --version
  3. If no env or wrong version:
     conda create -n psx python=3.10 -y
     conda activate psx
     pip install -e ".[dev]"
  4. Confirm: python --version

Option B — pyenv:
  1. pyenv install 3.10.14
  2. pyenv local 3.10.14
  3. pip install -e ".[dev]"

Option C — system upgrade (Ubuntu 24.04 has 3.12 by default):
  1. Run: python3 --version (check if python3 is newer than python)
  2. If python3 is 3.10+: alias python=python3 or use python3 everywhere

IMPORTANT: After fixing, re-run ALL of Prompt 0.1 from the beginning.
```

**Issue: Prompt 0.1 Step 3 — pip install fails**
```
Prompt 0.1 pip install -e ".[dev]" failed with errors.

Diagnose:
1. Show me the FULL error (not just tail -5, run without tail)
2. Run: cat pyproject.toml | grep -A 20 "\[project.optional-dependencies\]"
3. Common fixes:
   a) Missing system deps: sudo apt install libpq-dev python3-dev
   b) Wrong pip: pip install --upgrade pip setuptools wheel
   c) Conflicting packages: pip install -e ".[dev]" --force-reinstall
4. After fix, re-run: pip install -e ".[dev]"
```

**Issue: Prompt 0.1 Step 4-8 — Counts don't match estimates**
```
Line counts or function counts are different from the playbook estimates.

THIS IS EXPECTED AND NOT A PROBLEM. My estimates were based on a codebase
snapshot. The dev branch evolves. What matters is recording the ACTUAL numbers.

Action:
1. Record the actual values. These are now your baseline:
   - db.py lines: ____
   - app.py lines: ____
   - Python files: ____
   - Test files: ____
   - db.py function count: ____

2. Update the session state file with these actuals:
   "Baseline: db.py=XXXX lines, YYY functions. app.py=XXXX lines."

3. If db.py has significantly MORE functions than 191, you may need to add 
   additional repository files. If significantly FEWER, some repositories 
   may be smaller than planned. Both are fine — adjust as you go.

4. The only blocking issue is if db.py or app.py DON'T EXIST at all.
   If that happens: git log --oneline -5 to check you're on the right branch.

Continue to Step 9 (pytest).
```

**Issue: Prompt 0.1 Step 8 — Tests fail**
```
pytest tests/ -x --tb=short -q shows failures. This is the CRITICAL gate.

Diagnose:
1. Run: pytest tests/ -x --tb=long 2>&1 | tail -50 (get full traceback)
2. Identify which test file and test function failed
3. Common causes:
   a) Missing dependency → pip install <package>
   b) Database path issue → sqlite3 /mnt/e/psxdata/psx.sqlite ".tables" 
      (check if DB exists and is accessible)
   c) Import error → the codebase has a bug on this branch
   d) Network test failing → might need internet for API tests

4. If tests require the actual SQLite database:
   ls -la /mnt/e/psxdata/psx.sqlite (check exists and permissions)
   
5. If tests are specifically network-dependent (hitting PSX API):
   Run only unit tests: pytest tests/ -x -q -k "not sync" --ignore=tests/test_sync_with_mock_http.py

6. Record which tests pass and which fail. 
   If only 1-2 flaky/network tests fail but core tests pass, 
   log it and proceed. If >5 tests fail, STOP and fix.
```

---

#### Phase 1 (Split db.py) — Common Issues and Fixes

**Issue: ImportError after extracting a repository module**
```
After extracting functions to db/repositories/[module].py, I get:
ImportError: cannot import name '[function]' from 'psx_ohlcv.db'

Diagnose:
1. Run: python -c "from psx_ohlcv.db.repositories.[module] import [function]; print('Direct OK')"
   → If this FAILS: the function wasn't properly moved. Check the file.
   → If this PASSES: the __init__.py re-export is missing.

2. Check __init__.py:
   grep "[function]" src/psx_ohlcv/db/__init__.py
   → If not found: add it to the imports in __init__.py

3. Check for circular imports:
   python -c "import psx_ohlcv.db" 2>&1
   → If circular import error: one repository is importing from another
   → Fix: use late/local imports inside functions, not at module top

4. After fix: pytest tests/ -x -q -k "test_db" (run DB tests only first)
```

**Issue: Function needs imports from another repository**
```
After moving function X to repositories/eod.py, it fails because it calls
function Y that's now in repositories/symbols.py.

This is a CROSS-DEPENDENCY issue. Two fixes:

Option A (preferred): Import from the sibling repository
  # In repositories/eod.py
  from .symbols import get_symbols_list

Option B (if circular): Import from parent __init__
  # In repositories/eod.py
  from .. import get_symbols_list

Option C (emergency): Late import inside the function
  def some_func(con):
      from .symbols import get_symbols_list
      symbols = get_symbols_list(con)
```

**Issue: A function uses a helper that's private to db.py**
```
Some functions in db.py might use helper functions (starting with _) 
that are shared across domains. For example _parse_ts_to_epoch.

Move shared helpers to db/connection.py or create db/helpers.py.
Import from there in any repository that needs them.
```

**Issue: Prompt 1.13 — __init__.py missing functions**
```
The re-export count is less than the original db.py function count.

Diagnose — find exactly which functions are missing:
  python -c "
  import ast
  import psx_ohlcv.db as db_pkg
  
  # Parse original
  with open('src/psx_ohlcv/db.py') as f:
      tree = ast.parse(f.read())
  original = {node.name for node in ast.walk(tree) 
              if isinstance(node, ast.FunctionDef) and not node.name.startswith('_')}
  
  # Current exports
  exported = {n for n in dir(db_pkg) if not n.startswith('_') and callable(getattr(db_pkg, n))}
  
  missing = original - exported
  print(f'Missing {len(missing)} functions:')
  for m in sorted(missing):
      print(f'  {m}')
  "

For each missing function:
1. Find which repository it should belong to (by name/domain)
2. Verify it exists in that repository file
3. Add the import to __init__.py
4. Re-verify count
```

---

#### Phase 2 (Split app.py) — Common Issues and Fixes

**Issue: Streamlit page crashes after extraction**
```
After extracting a page into pages/[page].py, the app crashes.

Diagnose:
1. Run: streamlit run src/psx_ohlcv/ui/app.py --server.headless true 2>&1 | head -30
2. Look for the specific error:
   a) ImportError → missing import in the page module
   b) NameError → function defined in app.py but not moved/imported
   c) st.session_state error → state key initialized in app.py, not in page
   d) AttributeError on 'con' → database connection not passed to page function

3. Common fix pattern:
   Each page render function should receive what it needs:
   def render_dashboard(con, config):
       # page logic here
   
   In app.py:
   from psx_ohlcv.ui.page_views.dashboard import render_dashboard
   render_dashboard(con, config)
```

**Issue: Session state not shared between pages**
```
After splitting, st.session_state keys set in app.py aren't visible in pages.

This is normal — Streamlit session_state is global to the app process.
The issue is usually that initialization code was in app.py and the page 
assumes it exists.

Fix: Each page should have a guard:
  if 'key' not in st.session_state:
      st.session_state.key = default_value
```

---

#### Phase 3 (PostgreSQL) — Common Issues and Fixes

**Issue: Docker container won't start**
```
docker compose up -d timescaledb shows errors.

Diagnose:
1. docker compose logs timescaledb (check logs)
2. Common causes:
   a) Port 5432 already in use: lsof -i :5432
   b) Volume permissions: ls -la /mnt/e/psxdata/pgdata/
   c) WSL memory: check Docker Desktop settings
3. Fix port conflict: change port mapping to "5433:5432"
4. Fix permissions: sudo chown -R 999:999 /mnt/e/psxdata/pgdata/
```

**Issue: TimescaleDB extension not available**
```
CREATE EXTENSION timescaledb fails.

Check:
1. docker compose exec timescaledb psql -U psx -d psxdata -c "SELECT * FROM pg_available_extensions WHERE name='timescaledb';"
2. If not listed: wrong Docker image. Use timescale/timescaledb:latest-pg16
3. If listed but won't create: shared_preload_libraries issue
   Add to docker-compose.yml:
   command: postgres -c shared_preload_libraries=timescaledb
```

**Issue: Data migration row count mismatch**
```
SQLite has more rows than PostgreSQL after migration.

Diagnose:
1. Check which table has the mismatch
2. Common cause: date format conversion failed, rows with NULL dates rejected
3. Run migration with verbose logging to see which rows were skipped
4. Check for constraint violations: duplicate keys, foreign key issues
```

---

#### Phase 4 (Async) — Common Issues and Fixes

**Issue: aiohttp SSL errors**
```
aiohttp raises SSL certificate errors when hitting PSX API.

Fix:
  import ssl
  ssl_ctx = ssl.create_default_context()
  ssl_ctx.check_hostname = False
  ssl_ctx.verify_mode = ssl.CERT_NONE
  connector = aiohttp.TCPConnector(ssl=ssl_ctx)
  
Or: pip install certifi && update SSL certs
```

**Issue: Rate limiting / 429 responses from PSX**
```
PSX DPS returns 429 Too Many Requests with async fetcher.

Fix: Increase rate_limit delay:
  AsyncPSXFetcher(max_concurrent=10, rate_limit=0.2)
  
Start conservative, tune up gradually.
```

**Issue: Redis connection refused**
```
ARQ can't connect to Redis.

Check:
1. docker compose ps (is Redis running?)
2. redis-cli ping (should return PONG)
3. If WSL: check if Redis port is mapped correctly
```

---

#### Phase 5 (API+AI) — Common Issues and Fixes

**Issue: FastAPI import errors for new routers**
```
Adding new routers causes import errors.

Check:
1. Each router file has: from fastapi import APIRouter
2. router = APIRouter() at top
3. main.py includes: app.include_router(module.router, prefix="...", tags=["..."])
4. Run: python -c "from psx_ohlcv.api.main import app; print(app.routes)"
```

**Issue: Old llm/ imports break after consolidation**
```
UI code still imports from psx_ohlcv.llm which was removed.

Diagnose:
  grep -rn "from psx_ohlcv.llm" src/
  grep -rn "import psx_ohlcv.llm" src/

Update each occurrence to import from psx_ohlcv.agents instead.
```

---

### THE ISSUE RESOLUTION WORKFLOW (Flowchart)

```
VERIFY fails
    │
    ▼
┌─────────────────────┐
│ READ the full error  │
│ (don't skim it)      │
└────────┬────────────┘
         │
         ▼
┌─────────────────────────────┐
│ Is it an ImportError?        │──Yes──▶ Check __init__.py re-exports
│                               │        Check circular imports
└────────┬──────────────────────┘        Check file paths
         │ No
         ▼
┌─────────────────────────────┐
│ Is it a test failure?        │──Yes──▶ Run: pytest tests/test_XXX.py -x --tb=long
│                               │        Read the assertion error
└────────┬──────────────────────┘        Fix the specific function
         │ No
         ▼
┌─────────────────────────────┐
│ Is it a dependency issue?    │──Yes──▶ pip install <package> --break-system-packages
│ (ModuleNotFoundError)        │        conda install <package>
└────────┬──────────────────────┘
         │ No
         ▼
┌─────────────────────────────┐
│ Is it a Docker/infra issue?  │──Yes──▶ docker compose logs <service>
│                               │        Check ports, volumes, permissions
└────────┬──────────────────────┘
         │ No
         ▼
┌─────────────────────────────┐
│ Paste the Universal Issue   │
│ Diagnosis Prompt to Claude  │
│ Code with full error output │
└─────────────────────────────┘
```

### After Every Issue Resolution
```
Update ~/psx_ohlcv/.claude_session_state.md Issues Log:
Add row:
| Prompt X.Y | [brief issue description] | [brief resolution] |

Then re-run VERIFY. If passes, continue to next prompt.
```

---

## SESSION CONTINUITY — FULL RECOVERY GUIDE

### Why This Matters
Claude Code sessions can break for many reasons:
- Session timeout (long-running prompts)
- Browser crash / tab close
- Context window fills up
- Network disconnection
- You step away and come back hours/days later

Without the state file, you'd need to:
- Remember which prompt you were on
- Check git status manually
- Re-verify what's been done
- Risk repeating or skipping steps

### The State File Solves All This
**Location**: `~/psx_ohlcv/.claude_session_state.md`
**Updated**: After every completed prompt
**Contains**: Exact phase, exact prompt, branch, test status, issues log

### Recovery Workflow (Step by Step)

**Step 1**: Open new Claude Code session

**Step 2**: Paste the Session Recovery Prompt:
```
I'm resuming work on the psx_ohlcv project. We track progress in a session state file.

Please do the following:
1. Read ~/psx_ohlcv/.claude_session_state.md
2. Tell me:
   - What phase we're in
   - What was the last completed prompt
   - What is the next prompt to execute
   - Which git branch we should be on
   - Whether tests were passing at last checkpoint
3. Run: cd ~/psx_ohlcv && git branch --show-current
4. Run: git status --short
5. Run: pytest tests/ -x --tb=short -q 2>&1 | tail -10

Then wait for my instruction on whether to continue from where we left off.
Do NOT start any work until I confirm.
```

**Step 3**: Claude reads the file, reports status. You confirm.

**Step 4**: Paste the next prompt from this playbook.

**Step 5**: After it completes and VERIFY passes, update state:
```
Update ~/psx_ohlcv/.claude_session_state.md:
- Mark prompt [X.Y] as completed (change [ ] to [x])
- Update "Last Completed Prompt" to [X.Y]
- Update "Next Prompt" to [X.Z]
- Update "Tests Passing" to Yes/No
- Log any issues encountered
```

**Step 6**: Repeat from Step 4 until phase is done.

### Emergency Recovery (If State File Somehow Gets Lost)
```
I'm working on the psx_ohlcv project refactoring. The session state file may be missing.
Please help me reconstruct where we are:

1. Run: cd ~/psx_ohlcv && git branch --show-current
2. Run: git log --oneline -10
3. Check if db/ package exists: ls -la src/psx_ohlcv/db/ 2>/dev/null || echo "db.py is still monolith"
4. Check if pages exist: ls -la src/psx_ohlcv/ui/page_views/ 2>/dev/null || echo "app.py is still monolith"
5. Check Docker: docker compose ps 2>/dev/null || echo "No Docker yet"
6. Check for async: ls src/psx_ohlcv/sources/async_fetcher.py 2>/dev/null || echo "No async yet"
7. Run: wc -l src/psx_ohlcv/db.py 2>/dev/null || echo "db.py replaced by package"
8. Run: wc -l src/psx_ohlcv/ui/app.py
9. Run: pytest tests/ -x --tb=short -q 2>&1 | tail -10

Based on this, tell me which phase and prompt we're likely at.
Then recreate the session state file at ~/psx_ohlcv/.claude_session_state.md
```

### Important Notes
- The state file is in `.gitignore` — it's your personal workflow state, not project code
- Each phase has its own git branch — you can always check `git branch` to confirm phase
- If a prompt was PARTIALLY done when the session broke, re-run the full prompt
  (each prompt is designed to be idempotent — safe to re-run)
- The VERIFY step at the end of each prompt tells you whether it completed successfully
