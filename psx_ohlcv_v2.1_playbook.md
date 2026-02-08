# PSX OHLCV v2.1 — Post-Refactor Roadmap & Claude Code Prompts

## Where You Are
- v2.0.0 tagged and pushed
- 12 db repository modules, 21 UI page modules
- 39 API endpoints built, 2 WebSocket endpoints
- Async fetcher built but NOT battle-tested
- SQLite optimized (WAL, cache, indexes)
- 342 tests pass, 1 pre-existing failure, 1 skipped
- Streamlit UI still calls DB directly (not via API)

## What Matters Next (Priority Order)

| # | Task | Impact | Effort | Why |
|---|------|--------|--------|-----|
| 1 | Fix pre-existing test failure | 🟢 Quick win | 15 min | Clean green suite = confidence |
| 2 | Battle-test async fetcher on 540+ symbols | 🔴 Critical | 1 hr | You built it but never proved it works at scale |
| 3 | Wire async fetcher into sync.py | 🔴 Critical | 2 hr | The actual 20x speedup for daily operations |
| 4 | Connect Streamlit → FastAPI | 🟡 High | 4 hr | Proper architecture, demo-ready |
| 5 | Add real-time market page | 🟡 High | 3 hr | Killer feature for demos |
| 6 | Data quality dashboard | 🟡 Medium | 2 hr | Know what's missing, stale, broken |
| 7 | Export API for external consumers | 🟡 Medium | 2 hr | Other apps can consume your data |
| 8 | Automated daily sync via cron | 🟢 Quick win | 30 min | Hands-free operation |

---

## SESSION RECOVERY

Same system as v2.0.0. State file at `~/psx_ohlcv/.claude_session_state.md`.
Use the Session Recovery Prompt from the v2.0.0 playbook when starting a new session.

---

## TASK 1: Fix Pre-existing Test Failure (15 min)

### Prompt T1.1 — Diagnose and Fix
```
I'm working on psx_ohlcv, v2.0.0 just shipped. There's 1 pre-existing test failure.

1. Run: cd ~/psx_ohlcv && git checkout dev
2. Run: pytest tests/ --tb=long -q 2>&1 | grep -A 20 "FAILED"
3. Show me the FULL traceback of the failing test
4. Identify the root cause — is it:
   a) A genuine bug in the code
   b) A test that depends on network/external API
   c) A test that depends on specific data in the DB
   d) A test with a hardcoded date or stale fixture
5. Fix the actual issue (not just skip the test)
6. Run: pytest tests/ -x --tb=short -q
   Must show: ALL passed (0 failed)

If the test depends on external network:
   Mark it with @pytest.mark.network and add to pytest.ini:
   markers = network: tests requiring network access
   
   Then verify: pytest tests/ -x -q -m "not network" passes clean.

7. Commit:
   git add -A
   git commit -m "fix: resolve pre-existing test failure in [test_name]"
```

---

## TASK 2: Battle-Test Async Fetcher (1 hr)

### Prompt T2.1 — Full Symbol Benchmark
```
The async fetcher (src/psx_ohlcv/sources/async_fetcher.py) was built in Phase 4 
but never tested on the full symbol set.

Step 1 — Get the full symbol count:
  python -c "
  from psx_ohlcv.db import connect, get_symbols_list
  con = connect()
  symbols = get_symbols_list(con)
  print(f'Total symbols: {len(symbols)}')
  print(f'First 10: {symbols[:10]}')
  print(f'Last 10: {symbols[-10:]}')
  "

Step 2 — Run the async fetcher on ALL symbols with timing:
  python -c "
  import asyncio, time, json
  from psx_ohlcv.db import connect, get_symbols_list
  from psx_ohlcv.sources.async_fetcher import AsyncPSXFetcher

  async def benchmark():
      con = connect()
      symbols = get_symbols_list(con)
      con.close()
      
      print(f'Fetching {len(symbols)} symbols...')
      
      async with AsyncPSXFetcher() as fetcher:
          start = time.time()
          results = await fetcher.fetch_eod_batch(symbols)
          elapsed = time.time() - start
      
      print(f'Completed in {elapsed:.1f}s')
      print(f'  OK: {results[\"ok\"]}')
      print(f'  Failed: {results[\"failed\"]}')
      print(f'  Rate: {len(symbols)/elapsed:.1f} symbols/sec')
      
      if results.get('errors'):
          print(f'  Top errors:')
          from collections import Counter
          errs = Counter(str(e)[:80] for e in results['errors'].values())
          for err, count in errs.most_common(5):
              print(f'    [{count}x] {err}')
  
  asyncio.run(benchmark())
  "

Step 3 — Compare with synchronous baseline:
  python -c "
  import time
  from psx_ohlcv.db import connect, get_symbols_list
  from psx_ohlcv.sources.eod import fetch_eod_json
  from psx_ohlcv.http import create_session
  
  con = connect()
  symbols = get_symbols_list(con)[:20]  # Only 20 for sync test
  con.close()
  
  session = create_session()
  start = time.time()
  ok = 0
  for sym in symbols:
      try:
          data = fetch_eod_json(session, sym)
          if data: ok += 1
      except: pass
  elapsed = time.time() - start
  
  print(f'Sync: {len(symbols)} symbols in {elapsed:.1f}s ({len(symbols)/elapsed:.1f}/sec)')
  print(f'Async would be: ~{elapsed * len(get_symbols_list(connect())) / len(symbols) / 20:.0f}s estimated')
  "

Report:
- Async: X symbols in Y seconds = Z symbols/sec
- Sync: 20 symbols in Y seconds = Z symbols/sec  
- Speedup: Xx faster
- Failure rate: X% (acceptable if <5%)
- Top error types

If failure rate >10%, we need to tune concurrency down.
If any symbols consistently fail, log them for investigation.
```

### Prompt T2.2 — Fix Issues Found in Benchmark
```
Based on the benchmark results from T2.1:

If failure rate was HIGH (>10%):
  - Reduce max_concurrent from 25 to 10 or 15
  - Increase rate_limit from 0.05 to 0.1 or 0.2
  - Add retry logic if not already present
  - Test again with the adjusted settings

If specific symbols consistently fail:
  - Check if they're delisted or suspended
  - Check if their DPS API endpoint format is different
  - Add to a known_failures list with reason

If SSL errors occurred:
  - Add SSL context configuration to async_fetcher.py

After fixes, re-run the full benchmark:
  [paste the Step 2 benchmark script from T2.1]

Commit:
  git add -A
  git commit -m "fix: tune async fetcher based on full benchmark

  - Tested on [X] symbols: [Y]s async vs [Z]s estimated sync
  - Adjusted concurrency to [N], rate limit to [M]
  - [X]% success rate
  - Known failures: [list any consistent failures]"
```

---

## TASK 3: Wire Async Fetcher into Sync Pipeline (2 hr)

### Prompt T3.1 — Create Async Sync Orchestrator
```
Currently sync.py runs a sequential loop:
  for symbol in symbols:
      data = fetch_eod_json(session, symbol)
      upsert_eod(con, symbol, data)
      polite_delay()

We need to create an async version that:
1. Fetches ALL symbols concurrently via AsyncPSXFetcher
2. Upserts results to SQLite (still synchronous — SQLite doesn't do async)
3. Records sync run in the same format as current sync.py
4. Reports same SyncSummary output

Create src/psx_ohlcv/sync_async.py:

  async def sync_all_async(
      db_path: str,
      refresh_symbols: bool = False,
      limit_symbols: int | None = None,
      symbols_list: list[str] | None = None,
      config: SyncConfig | None = None,
  ) -> SyncSummary:
      """Async version of sync_all. 20x faster."""

The function should:
  1. Get symbols list (same logic as sync.py)
  2. Call AsyncPSXFetcher.fetch_eod_batch(symbols)
  3. Loop through results and upsert_eod for each successful fetch
  4. Record sync run (start, end, stats)
  5. Return SyncSummary

Also add CLI integration:
  In __main__.py or cli.py, add: psxsync async-sync --all
  This calls sync_all_async via asyncio.run()

VERIFY:
  # Test on 20 symbols first
  python -m psx_ohlcv sync async-sync --symbols OGDC,HBL,MCB,LUCK,PSO --db /mnt/e/psxdata/psx.sqlite
  
  # Check data was upserted
  sqlite3 /mnt/e/psxdata/psx.sqlite "SELECT symbol, MAX(date) FROM eod_ohlcv WHERE symbol IN ('OGDC','HBL','MCB','LUCK','PSO') GROUP BY symbol;"

  pytest tests/ -x -q --tb=short 2>&1 | tail -5
```

### Prompt T3.2 — Full Async Sync Test + Commit
```
Now run async sync on ALL symbols:

  time python -m psx_ohlcv sync async-sync --all --db /mnt/e/psxdata/psx.sqlite

Record:
- Total time
- Symbols OK / Failed
- Rows upserted
- Compare with last sync_all run time (check sync_runs table):
  sqlite3 /mnt/e/psxdata/psx.sqlite "SELECT started_at, ended_at, symbols_ok, symbols_failed, rows_upserted FROM sync_runs ORDER BY started_at DESC LIMIT 5;"

Commit:
  git add -A
  git commit -m "feat: async sync pipeline — [X]x faster than sequential

  - sync_async.py: async orchestrator using AsyncPSXFetcher
  - CLI: psxsync async-sync --all
  - Benchmark: [X] symbols in [Y]s (was [Z]s sequential)
  - [N] symbols OK, [M] failed
  - SQLite upserts still synchronous (batched after fetch)"
```

---

## TASK 4: Connect Streamlit → FastAPI (4 hr)

### Prompt T4.1 — Create API Client Module
```
Currently every Streamlit page does:
  from psx_ohlcv.db import connect, get_eod_ohlcv, get_symbols_list
  con = connect()
  data = get_eod_ohlcv(con, symbol, ...)

This should go through FastAPI instead:
  from psx_ohlcv.api_client import PSXClient
  client = PSXClient("http://localhost:8000")
  data = client.get_eod(symbol, ...)

Create src/psx_ohlcv/api_client.py:

  import httpx
  
  class PSXClient:
      def __init__(self, base_url="http://localhost:8000"):
          self._client = httpx.Client(base_url=base_url, timeout=30)
      
      def get_symbols(self, active_only=True) -> list[dict]:
          return self._client.get("/api/symbols", params={"active": active_only}).json()
      
      def get_eod(self, symbol, start=None, end=None) -> list[dict]:
          return self._client.get(f"/api/eod/{symbol}", params={"start": start, "end": end}).json()
      
      def get_market_indices(self) -> list[dict]:
          return self._client.get("/api/market/indices").json()
      
      def get_company_profile(self, symbol) -> dict:
          return self._client.get(f"/api/company/{symbol}/profile").json()
      
      # ... one method per API endpoint that the UI needs

Also add a fallback mode for when API isn't running:
  class PSXClient:
      def __init__(self, base_url=None):
          if base_url:
              self._mode = "api"
              self._client = httpx.Client(...)
          else:
              self._mode = "direct"
              self._con = connect()
      
      def get_symbols(self, ...):
          if self._mode == "api":
              return self._client.get(...).json()
          else:
              return get_symbols_list(self._con)

This way the UI works with OR without the API running.

VERIFY:
  # Start API
  uvicorn psx_ohlcv.api.main:app --port 8000 &
  sleep 2
  
  # Test client in API mode
  python -c "
  from psx_ohlcv.api_client import PSXClient
  client = PSXClient('http://localhost:8000')
  symbols = client.get_symbols()
  print(f'API mode: {len(symbols)} symbols')
  "
  
  # Test client in direct mode
  python -c "
  from psx_ohlcv.api_client import PSXClient
  client = PSXClient()  # No URL = direct DB
  symbols = client.get_symbols()
  print(f'Direct mode: {len(symbols)} symbols')
  "
  
  kill %1

pip install httpx  # if not already installed
```

### Prompt T4.2 — Wire Dashboard Page to API Client
```
Update the Dashboard page (the most visible page) to use PSXClient.

Read src/psx_ohlcv/ui/pages/dashboard.py and:
1. Replace all direct db.connect() + db.get_xxx() calls with PSXClient calls
2. Add at the top of the page:
   from psx_ohlcv.api_client import PSXClient
   client = PSXClient()  # Direct mode by default, API mode if env var set
3. Keep the page looking and functioning exactly the same

Only change the Dashboard page in this prompt. Other pages later.

VERIFY:
  streamlit run src/psx_ohlcv/ui/app.py --server.headless true 2>&1 | head -5
  (must start without errors)
```

### Prompt T4.3-T4.6 — Wire Remaining Key Pages
```
Repeat T4.2 for these pages (one prompt each):
- Candlestick Explorer (T4.3)
- Regular Market Watch (T4.4)
- Company Analytics (T4.5)
- Sync Monitor (T4.6)

Same pattern: replace direct DB calls with PSXClient calls.
VERIFY after each page that Streamlit still starts.
```

### Prompt T4.7 — Commit
```
pytest tests/ -x -q
streamlit run src/psx_ohlcv/ui/app.py --server.headless true 2>&1 | head -5

git add -A
git commit -m "feat: Streamlit UI wired through PSXClient

- api_client.py: HTTP client with direct-DB fallback
- Dashboard, Candlestick, Market Watch, Company, Sync Monitor migrated
- Works with or without FastAPI running
- Remaining pages still use direct DB (migrate later)"
```

---

## TASK 5: Real-Time Market Dashboard Page (3 hr)

### Prompt T5.1 — Live Market Page
```
Create a new Streamlit page: src/psx_ohlcv/ui/pages/live_market.py

This page shows:
1. KSE-100 index with live value + change + sparkline
2. Market breadth: gainers vs losers pie chart
3. Top 10 gainers table (symbol, price, change%, volume)
4. Top 10 losers table  
5. Sector heatmap (each sector colored by avg change%)
6. Auto-refresh every 60 seconds (st.rerun with timer)

Data comes from:
- regular_market_current table (live snapshots)
- PSXClient.get_market_live() or direct DB
- PSXClient.get_market_indices() for KSE-100

Use Plotly for charts. Streamlit's st.metric for KPIs.

Add to app.py navigation.

VERIFY:
  streamlit run src/psx_ohlcv/ui/app.py --server.headless true 2>&1 | head -5
```

---

## TASK 6: Data Quality Dashboard (2 hr)

### Prompt T6.1 — Data Quality Page
```
Create src/psx_ohlcv/ui/pages/data_quality.py

This page answers: "What data do I have, what's missing, what's stale?"

Sections:
1. Coverage Summary
   - Total symbols: X
   - Symbols with EOD data: X
   - Symbols with intraday: X
   - Symbols with company profile: X
   - Symbols with NO data at all: [list them]

2. Freshness Table
   | Data Type | Latest Date | Days Old | Status |
   |-----------|-------------|----------|--------|
   | EOD OHLCV | 2026-02-07 | 1 | ✅ Fresh |
   | Intraday | 2026-02-06 | 2 | ⚠️ Stale |
   | Company Profiles | 2026-01-15 | 24 | 🔴 Old |
   | FX Rates | ... | ... | ... |

3. Gap Detection
   - EOD: Find trading days with missing data
   - Show calendar heatmap (plotly) with green=data, red=missing

4. Duplicate Detection
   - Check for duplicate rows in key tables
   - SELECT symbol, date, COUNT(*) FROM eod_ohlcv GROUP BY symbol, date HAVING COUNT(*) > 1

5. Quick Actions
   - Button: "Run VACUUM" → calls maintenance.vacuum_database()
   - Button: "Run ANALYZE" → calls maintenance.analyze_database()
   - Button: "Backup DB" → calls maintenance.backup_database()
   - Display: DB file size, WAL size, index count

All queries run against SQLite directly (this is an admin page).

VERIFY:
  streamlit run src/psx_ohlcv/ui/app.py --server.headless true 2>&1 | head -5
```

---

## TASK 7: Automated Daily Sync via Cron (30 min)

### Prompt T7.1 — Cron Setup
```
Set up automated daily sync for PSX market data.

Step 1 — Create a sync wrapper script:
  Create scripts/daily_sync.sh:
  
  #!/bin/bash
  # PSX OHLCV Daily Sync
  # Runs async sync at 18:30 PKT (market closes 15:30, data available by ~17:00)
  
  cd ~/psx_ohlcv
  source ~/.bashrc  # or conda activate psx
  
  LOG_DIR="/mnt/e/psxdata/logs"
  mkdir -p "$LOG_DIR"
  LOG_FILE="$LOG_DIR/sync_$(date +%Y%m%d).log"
  
  echo "=== PSX Daily Sync: $(date) ===" >> "$LOG_FILE"
  python -m psx_ohlcv sync async-sync --all --db /mnt/e/psxdata/psx.sqlite >> "$LOG_FILE" 2>&1
  echo "=== Completed: $(date) ===" >> "$LOG_FILE"
  
  # Run maintenance weekly (on Fridays)
  if [ "$(date +%u)" = "5" ]; then
      echo "=== Weekly Maintenance ===" >> "$LOG_FILE"
      python -m psx_ohlcv.db.maintenance --analyze --stats >> "$LOG_FILE" 2>&1
  fi

  chmod +x scripts/daily_sync.sh

Step 2 — Add to crontab:
  crontab -e
  
  Add this line:
  30 13 * * 1-5 ~/psx_ohlcv/scripts/daily_sync.sh
  # 13:30 UTC = 18:30 PKT, Monday-Friday only

Step 3 — Verify cron is working:
  crontab -l | grep psx
  # Should show the entry

  # Test run manually:
  bash scripts/daily_sync.sh
  cat /mnt/e/psxdata/logs/sync_$(date +%Y%m%d).log

VERIFY:
  ls -la /mnt/e/psxdata/logs/
  tail -20 /mnt/e/psxdata/logs/sync_$(date +%Y%m%d).log
  
Commit:
  git add scripts/daily_sync.sh
  git commit -m "ops: automated daily sync via cron

  - scripts/daily_sync.sh: async sync with logging
  - Cron: 18:30 PKT Mon-Fri
  - Weekly ANALYZE on Fridays
  - Logs to /mnt/e/psxdata/logs/"
```

---

## TASK 8: Tag v2.1.0

### Prompt T8.1 — Release
```
Final verification:
  pytest tests/ -x -q  # All green
  python -m psx_ohlcv.db.maintenance --stats  # DB healthy
  
  git log --oneline dev..HEAD  # Show all v2.1 commits

Tag and push:
  git tag -a v2.1.0 -m "v2.1: async sync pipeline, API client, live market, data quality

  - Fixed pre-existing test failure
  - Async fetcher benchmarked on 540+ symbols
  - Async sync pipeline: 20x faster daily sync  
  - PSXClient: API + direct-DB fallback
  - Streamlit pages wired through PSXClient
  - Live market dashboard page
  - Data quality dashboard page
  - Automated daily sync via cron"
  
  git push origin dev --tags
```

---

## EXECUTION ORDER

```
T1 (15 min) → T2 (1 hr) → T3 (2 hr) → T7 (30 min) → T4 (4 hr) → T5 (3 hr) → T6 (2 hr) → T8 (15 min)
                                              ↑
                                     Do this early — 
                                     once async sync works,
                                     automate it immediately
```

**Total: ~13 hours across 2-3 Claude Code sessions**

After v2.1.0 ships, the next big features would be:
- Portfolio tracker (user watchlists + P&L)
- Alerts system (price, volume, technical triggers)
- PDF report generation (daily market summary)
- Mobile-responsive Streamlit or separate React frontend
