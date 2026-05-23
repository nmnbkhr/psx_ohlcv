## Task: MUFAP Mutual Fund NAV History — Full Sync Downloader

### Context
MUFAP (mufap.com.pk) is Pakistan's Mutual Funds Association. The workflow:

1. Go to **Expense Ratios** page: `https://www.mufap.com.pk/Industry/IndustryStatDaily?tab=5`
2. Page shows a **table of all funds** — each row has fund name as a clickable link with FundID
3. **Scan this table** → maintain a **fund master table** (new funds auto-inserted each run)
4. For each fund in master → open `/FundProfile/FundDetail?FundID={fund_id}`
5. Fund Detail page has a **Highcharts NAV Performance chart** with range filters (1M, 3M, 6M, 1Y, **All**)
6. Click **"All"** → extract full NAV history via `chart.getCSV()` (powered by `export-data.js`)

### Output Path & File Convention
- **Output folder:** `/mnt/e/psxdata/mufapnav/`
- **Fund master table:** `/mnt/e/psxdata/mufapnav/_fund_master.csv`
- **NAV files:** `{fund_id}.csv` (e.g. `12768.csv`, `12917.csv`)
- **Full sync / overwrite:** NAV CSVs overwritten every run. Fund master is **append-only** — new funds inserted, existing rows kept.
- Create the output folder if it doesn't exist

### Environment
- WSL2 Ubuntu on ASUS laptop (RTX 4080 12GB, 32GB RAM)
- Conda environment available
- No WSLg/GUI — use **Xvfb** for virtual display

### Requirements

**Step 1: Setup**
- Install system deps: `xvfb`, Chrome/Chromium, required libs
- Install Python packages: `undetected-chromedriver`, `selenium`, `beautifulsoup4`
- Install `xvfbwrapper` for clean Xvfb management

**Step 2: Build `mufap_nav_downloader.py`**

Use `undetected-chromedriver` (non-headless) + Xvfb virtual display approach:

```
Architecture:
  Xvfb (virtual display) → undetected-chromedriver (non-headless) → MUFAP site
```

---

### Phase A — Scan Expense Ratios page → Update Fund Master Table

a1) Navigate to `https://www.mufap.com.pk/Industry/IndustryStatDaily?tab=5`
a2) Wait for page to fully load (server-rendered HTML table)
a3) **Parse every row** in the table. The table has these columns:
   - Sector, AMC, Fund (clickable link), Category, Inception Date, NAV, TER MTD %, TER YTD %, MF %, S&M %, Validity Date
a4) From each row extract:
   - `fund_id` — from the link href `/FundProfile/FundDetail?FundID=12768` → `12768`
   - `fund_name` — the link text (e.g. "ABL Cash Fund")
   - `amc` — AMC column
   - `category` — Category column  
   - `sector` — Sector column
a5) **Load or create fund master table** at `/mnt/e/psxdata/mufapnav/_fund_master.csv`:
   - Columns: `fund_id, fund_name, amc, category, sector, first_seen, last_seen`
   - If file doesn't exist → create with all funds from page
   - If file exists → **merge**: 
     - Existing funds: update `last_seen` timestamp
     - New funds (fund_id not in master): **INSERT** with `first_seen = now`
     - Never delete rows (fund may be temporarily absent from page)
a6) Log:
   ```
   [master] Loaded 700 funds from Expense Ratios page
   [master] Existing in master: 695 | New funds inserted: 5 | Total in master: 705
   [master] New: 15051 ABL Fixed Rate Plan XXIV, 15061 ABL Islamic Fixed Term Plan V, ...
   ```
a7) Save updated master CSV

---

### Phase B — For each fund in master, download NAV history

Use `fund_id` from the master table to construct the URL — no need to click links in the table, just navigate directly:

b1) For each fund in `_fund_master.csv`:
   - Navigate to `https://www.mufap.com.pk/FundProfile/FundDetail?FundID={fund_id}`
   - Wait for page + AJAX to settle (~6 seconds)

b2) **On first fund only** — capture ALL network requests from Chrome performance logs:
   - Filter for `mufap.com.pk` XHR/Fetch calls (skip .js/.css/.png etc)
   - Log each URL, method, POST body
   - Try `Network.getResponseBody` via CDP for API response structure
   - **Print clearly** — this discovers the AJAX endpoint feeding Highcharts

b3) **Click "All" range selector** on the Highcharts chart:
   - Try DOM click: find button with text "All" in highcharts container
   - Fallback: JS `Highcharts.charts[0].rangeSelector.buttons[last].element.click()`
   - Wait 4s for data reload, capture any new network requests on first fund

b4) **Extract chart data via JavaScript**:
   - Primary: `Highcharts.charts[0].getCSV()` → save as `/mnt/e/psxdata/mufapnav/{fund_id}.csv`
   - Fallback: `Highcharts.charts[0].series[].points[]` → convert to CSV manually
   - **Overwrite** if file exists — this is full sync

b5) 2-second delay between funds

---

### Phase C — Logging & Summary

Per-fund logging:
```
[  1/705] 12768 ABL Cash Fund — 3,542 rows — saved
[  2/705] 12917 First Capital Mutual Fund — 2,100 rows — saved
[  3/705] 13045 Some Fund — SKIP (no chart data)
```

On completion:
- Total funds in master, processed, saved, skipped, errors
- List of discovered unique API endpoints (from first fund's network capture)
- Save `_sync_log.json` to output folder:
  ```json
  {
    "run_at": "2026-03-03T15:30:00",
    "master_total": 705,
    "new_funds_inserted": 5,
    "processed": 705,
    "saved": 690,
    "skipped": 10,
    "errors": 5,
    "error_fund_ids": ["13045", "14999", ...],
    "api_endpoints_discovered": ["https://..."]
  }
  ```

---

### Infrastructure

a) **Auto-start Xvfb** if no $DISPLAY is set:
   - Try `xvfbwrapper` first, fallback to raw subprocess `Xvfb :99`
   - Clean up on exit

b) **Create undetected-chromedriver** with:
   - `goog:loggingPrefs: {performance: ALL}` — to capture network requests
   - CDP `Network.enable` — to capture response bodies
   - Non-headless (runs inside Xvfb)

---

### CLI interface
```
python mufap_nav_downloader.py                     # full sync: scan tab=5 → update master → download all NAVs
python mufap_nav_downloader.py --fund-id 12768     # single fund only (skip Phase A, go direct to Phase B)
python mufap_nav_downloader.py --scan-only          # Phase A only: scan tab=5, update master, no NAV downloads
python mufap_nav_downloader.py --no-xvfb           # skip Xvfb (if WSLg works)
```

---

### Step 3: Run for fund 12768 (ABL Cash Fund) first
- Execute the script with `--fund-id 12768`
- Report back: what API endpoints were discovered, what the response structure looks like
- Show the CSV preview (first few lines)
- Then we decide whether to run full sync or build a requests-only version

### Key Technical Notes
- The site is ASP.NET MVC — API endpoints likely follow patterns like `/FundProfile/GetNavChart?FundID=...`
- `export-data.js` is Highcharts' built-in module — calls `chart.getCSV()` client-side
- Chart data format is typically `[[timestamp_ms, nav_value], ...]`
- Expense Ratios page (tab=5) is **server-rendered** — full HTML table, no AJAX needed
- Fund Detail page is **fully dynamic** — empty template, everything via AJAX
- Fund master (`_fund_master.csv`) persists across runs — append-only for new funds
- NAV CSVs (`{fund_id}.csv`) are overwritten every run — full sync

### Success Criteria
1. Fund master table created/updated with all funds from tab=5
2. New funds auto-inserted on subsequent runs
3. We see the exact AJAX URL(s) that load the NAV chart data
4. Working CSV saved as `/mnt/e/psxdata/mufapnav/12768.csv`
5. Full sync works — running again overwrites NAV CSVs, appends new funds to master
