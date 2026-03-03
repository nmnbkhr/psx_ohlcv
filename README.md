# pakfindata

PSX OHLCV data fetcher and sync tool for Pakistan Stock Exchange.

## Setup (WSL with Conda)

```bash
conda create -n psx python=3.11 -y
conda activate psx
pip install -U pip
pip install -e ".[dev]"
```

## CLI Usage

### Symbol Management

```bash
# Refresh symbols from PSX market-watch
pfsync symbols refresh

# Show all symbols as CSV
pfsync symbols show --as csv

# Get symbols as comma-separated string (useful for scripting)
pfsync symbols string
pfsync symbols string --limit 200
```

### Data Synchronization

```bash
# Sync EOD data for all active symbols
pfsync sync --all

# Sync with symbol refresh first
pfsync sync --all --refresh-symbols

# Incremental sync (only fetch data newer than existing)
pfsync sync --all --incremental

# Custom HTTP settings
pfsync sync --all --max-retries 5 --timeout 60 --delay-min 0.5 --delay-max 1.0
```

### Sync Options

| Option | Default | Description |
|--------|---------|-------------|
| `--incremental` | off | Only sync data newer than max date in DB per symbol |
| `--max-retries` | 3 | HTTP retry attempts |
| `--delay-min` | 0.3 | Min delay between requests (seconds) |
| `--delay-max` | 0.7 | Max delay between requests (seconds) |
| `--timeout` | 30 | HTTP request timeout (seconds) |

### Intraday Data

```bash
# Sync intraday data for a symbol
pfsync intraday sync --symbol OGDC

# Sync with full refresh (non-incremental)
pfsync intraday sync --symbol HBL --no-incremental

# Limit rows fetched
pfsync intraday sync --symbol MCB --max-rows 1000

# Show intraday data for a symbol
pfsync intraday show --symbol OGDC

# Show with custom limit
pfsync intraday show --symbol HBL --limit 500
```

**Intraday Sync Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `--symbol` | required | Stock symbol to sync |
| `--no-incremental` | off | Fetch all data (ignore last sync state) |
| `--max-rows` | 2000 | Maximum rows to fetch from API |

**Note:** Intraday data source is `dps.psx.com.pk/timeseries/int/{SYMBOL}`. This is an undocumented endpoint and may change without notice.

### Market Summary (Historical)

Download and parse historical market summary files from PSX DPS:

```bash
# Download a single day
pfsync market-summary day --date 2025-01-15

# Download a date range
pfsync market-summary range --start 2025-01-01 --end 2025-01-15

# Download last N days (relative to today)
pfsync market-summary last --days 30

# Force re-download (overwrite existing)
pfsync market-summary range --start 2025-01-01 --end 2025-01-15 --force

# Include weekends (default: skip Sat/Sun)
pfsync market-summary last --days 30 --include-weekends

# Retry failed downloads (dates that had errors)
pfsync market-summary retry-failed

# Retry missing downloads (dates that returned 404)
pfsync market-summary retry-missing
```

**Market Summary Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `--date` | required (day) | Date in YYYY-MM-DD format |
| `--start` | required (range) | Start date (YYYY-MM-DD) |
| `--end` | required (range) | End date (YYYY-MM-DD) |
| `--days` | required (last) | Number of days to look back |
| `--include-weekends` | off | Include Saturday and Sunday |
| `--force` | off | Re-download even if CSV exists |
| `--keep-raw` | off | Keep extracted raw files |
| `--out-dir` | data/market_summary | Output directory |

**Data Source:** `https://dps.psx.com.pk/download/mkt_summary/YYYY-MM-DD.Z`

**Output:**
- CSV files saved to `data/market_summary/csv/{date}.csv`
- Each CSV contains: symbol, sector_code, company_name, open, high, low, close, volume, prev_close

**Features:**
- Resumable: skips existing CSVs (use `--force` to re-download)
- Weekend skipping: excludes Sat/Sun by default (trading days only)
- 404 handling: logs "not found" for holidays without failing
- Session reuse: efficient HTTP connection pooling for batch downloads

### Company Analytics

Fetch company profiles and quote snapshots from DPS company pages:

```bash
# Refresh company profile and key people
pfsync company refresh --symbol OGDC

# Take a quote snapshot (price, change, OHLCV, ranges)
pfsync company snapshot --symbol OGDC

# Snapshot multiple symbols
pfsync company snapshot --symbols "OGDC,HBL,PSO"

# Continuous monitoring (quotes every 60 seconds)
pfsync company listen --symbol OGDC --interval 60

# Monitor multiple symbols
pfsync company listen --symbols "OGDC,HBL,PSO" --interval 30

# Show stored company data
pfsync company show --symbol OGDC
pfsync company show --symbol OGDC --what profile
pfsync company show --symbol OGDC --what people
pfsync company show --symbol OGDC --what quotes
```

**Company Data Stored:**

| Table | Data |
|-------|------|
| `company_profile` | Company name, sector, description, address, website, registrar, auditor |
| `company_key_people` | CEO, Chairman, CFO, Company Secretary, etc. |
| `company_quote_snapshots` | Time-series of price, change, OHLCV, day/52-week/circuit ranges |

**Listen Mode:**
- Fetches quotes at specified interval (default: 60 seconds)
- Smart-save: skips insert if data unchanged (based on hash)
- Press Ctrl+C to stop

### Master Symbol Data

Refresh symbols from the authoritative PSX listed companies file:

```bash
# Refresh symbols from official listed_cmp.lst.Z file
pfsync master refresh

# Mark symbols not in master file as inactive
pfsync master refresh --deactivate-missing

# List all symbols
pfsync master list
pfsync master list --active-only

# Export to CSV
pfsync master export --out symbols.csv
```

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success (at least one symbol synced) |
| 1 | Configuration or setup error |
| 2 | All symbols failed |

## Scheduling

For automated daily syncs, see [scripts/cron_example.md](scripts/cron_example.md):
- Cron setup for Asia/Karachi timezone (18:00 PKT, after market close)
- Systemd user service for WSL2
- Log rotation configuration

**Quick cron example:**

```cron
# Daily sync at 18:00 PKT (Monday-Friday)
0 18 * * 1-5 cd /path/to/pakfindata && conda run -n psx python -m pakfindata.cli sync --all --incremental
```

## Data Storage

All data is stored in `/mnt/e/psxdata/` (E: drive in WSL):

```text
/mnt/e/psxdata/
├── psx.sqlite       # SQLite database
├── logs/
│   └── pfsync.log  # Application logs
└── docs/
    ├── DESIGN.md    # Architecture documentation
    └── SESSION_LOG.md  # Development session log
```

Override with `--db /path/to/custom.sqlite` if needed.

## Logging

Logs are written to `/mnt/e/psxdata/logs/pfsync.log` with automatic rotation:

- Max file size: 5 MB
- Backup count: 3 files

## Web UI (Streamlit)

A lightweight Streamlit dashboard for exploring data and running syncs:

```bash
# Install with UI dependencies
pip install -e ".[ui]"

# Run the dashboard (from project directory)
cd /home/adnoman/pakfindata
conda activate psx
streamlit run src/pakfindata/ui/app.py

# Or use make
make ui
```

**Access:** http://localhost:8501

**Pages:**
- **📊 Dashboard** - KPIs, recent sync runs, data quality info
- **📈 Candlestick Explorer** - OHLCV charts with volume, CSV export
- **⏱ Intraday Trend** - Live intraday price trends and volume charts
- **📊 Regular Market** - Live market data from PSX market-watch
- **📚 History** - Historical market trends, symbol history, sector performance
- **🧵 Symbols** - Browse, search, export symbols
- **🔄 Sync Monitor** - Run syncs, view status and failures
- **⚙️ Settings** - Config display (read-only)

### History Page

The History page provides historical analysis of market data from stored snapshots:

**Market History Tab:**
- Market breadth over time (gainers/losers/unchanged)
- Total volume trends
- Recent market analytics table

**Symbol History Tab:**
- Price trends for any symbol
- Volume history
- Optional candlestick view when OHLC data available
- Snapshot data table

**Sector History Tab:**
- Sector performance (avg change %) over time
- Sector volume trends
- Top performers in selected sector

**Time Range Options:**
- Last 1 Hour
- Last 3 Hours
- Today
- Last 5 Days
- All Data

**Populating History Data:**

History data comes from stored snapshots. To populate:

```bash
# Run continuous market monitoring (recommended)
pfsync regular-market listen --interval 60

# Or fetch snapshots periodically via UI "Fetch Market Data" button
```

Each fetch creates:
- Market analytics snapshot (gainers/losers/volume)
- Symbol snapshots (price/volume per symbol)
- Sector rollups (per-sector aggregates)

### Runtime Behavior

**Sync from UI:**
- The "Run Full Sync" button executes a blocking sync operation
- UI shows a spinner/status indicator while sync runs
- Typical runtime: 5-15 minutes for ~500 symbols (depends on network)
- Results display with success/error colors and expandable failure table

**Important:** The UI blocks during sync - do not close the browser tab.

### CLI vs UI: When to Use Which

| Use Case | Recommended | Reason |
|----------|-------------|--------|
| **Daily automated sync** | CLI + cron | Headless, scriptable, logging |
| **One-time manual sync** | UI | Visual feedback, no terminal needed |
| **Debugging sync failures** | CLI | Better error output, logs |
| **Exploring data/charts** | UI | Interactive visualization |
| **Exporting CSV data** | UI | Click-to-download |
| **Checking sync history** | Either | Both show sync_runs table |
| **Production/server** | CLI | No browser required |

**Rule of thumb:** Use CLI for automation and debugging, UI for exploration and manual operations.

## AI Insights (LLM-Powered Analysis)

The AI Insights feature provides LLM-powered market analysis using OpenAI's GPT-5.2 model.

### Configuration

Set your OpenAI API key as an environment variable:

```bash
# Linux/macOS/WSL
export OPENAI_API_KEY="sk-your-api-key-here"

# Or add to your .bashrc / .zshrc for persistence
echo 'export OPENAI_API_KEY="sk-your-api-key-here"' >> ~/.bashrc
```

**Security:** Never commit API keys to version control. The key is read from the environment only.

### Accessing AI Insights

1. Start the Streamlit UI: `streamlit run src/pakfindata/ui/app.py`
2. Navigate to "🤖 AI Insights" in the sidebar
3. Select an analysis mode and configure parameters
4. Click "Generate Insight"

### Analysis Modes

| Mode | Description | Data Used |
|------|-------------|-----------|
| **Company Summary** | Comprehensive company analysis with price history, key metrics | EOD OHLCV, company profile, quote snapshots |
| **Intraday Commentary** | Real-time trading pattern analysis | Intraday bars (last 500 points) |
| **Market Summary** | Sector-level market overview | Market analytics, sector rollups |
| **History Analysis** | Multi-day trend analysis for a symbol | EOD OHLCV history (configurable days) |

### Data Caveats

**IMPORTANT:** The LLM is explicitly instructed about data limitations:

1. **Derived High/Low Values:** For EOD data from the PSX timeseries API, high and low prices are derived as `max(open, close)` and `min(open, close)`. True intraday extremes are not captured. The LLM will warn users about this limitation in its analysis.

2. **Data Freshness:** Analysis is only as current as your last data sync. Always run syncs before generating insights for up-to-date analysis.

3. **No Invented Numbers:** The LLM is instructed to never fabricate statistics, prices, or metrics. If data is missing, it will state "data not available."

### Cost Control Tips

AI Insights uses the OpenAI API which has associated costs. To minimize expenses:

1. **Caching Enabled (Default):** Responses are cached for 6 hours. Identical queries use cached results.

2. **Cache Management:**
   - View cache stats in the sidebar
   - Clear cache when needed (e.g., after major data updates)
   - Cache invalidates automatically when underlying data changes

3. **Data Bounding:**
   - Maximum 2000 rows per query
   - Large datasets are automatically downsampled to 300-800 rows
   - Use shorter date ranges for history analysis

4. **Best Practices:**
   - Review the data preview before generating (to ensure you're querying what you want)
   - Use Company mode for single-stock deep dives
   - Use Market mode for broad overviews (less data, lower cost)

### Prompt Engineering

Every prompt sent to the LLM includes:

- **Data Used Section:** Explicit listing of what data was provided
- **Hard Rules:** No invented numbers, acknowledge data gaps, cite sources
- **PSX Caveats:** Warning about derived high/low, data limitations

This ensures consistent, honest, and transparent AI-generated insights.

## Data Quality Note

The PSX DPS API (`dps.psx.com.pk/timeseries/eod/{symbol}`) returns data in the format:
`[timestamp, close, volume, open]`

**Important:** The API does **NOT** provide actual high/low prices. Our implementation derives them as:
- `high = max(open, close)`
- `low = min(open, close)`

This means intraday price extremes are not captured. For technical analysis requiring true high/low values, consider premium data providers like EODHD or Twelve Data.

| Field | Source |
|-------|--------|
| Open | Direct from API |
| Close | Direct from API |
| Volume | Direct from API |
| High | Derived: max(open, close) |
| Low | Derived: min(open, close) |

## Development

```bash
make install     # Install package with dev dependencies
make install-ui  # Install with dev + UI dependencies
make test        # Run tests
make lint        # Run ruff linter
make run-demo    # Run demo script
make ui          # Launch Streamlit dashboard
```

## Documentation

See `/mnt/e/psxdata/docs/` for:

- [DESIGN.md](/mnt/e/psxdata/docs/DESIGN.md) - Architecture, schema, API endpoints
- [SESSION_LOG.md](/mnt/e/psxdata/docs/SESSION_LOG.md) - Development session log

## Data Licensing Notice

**PSX Market Data:** The data fetched by this tool originates from the Pakistan Stock Exchange (PSX) via their DPS portal (`dps.psx.com.pk`). This data is intended for personal, educational, and internal analytics purposes only.

**Commercial Redistribution:** If you intend to redistribute PSX market data commercially (e.g., in a paid app, API service, or data product), you must obtain appropriate licensing from PSX. Contact PSX directly for licensing terms:
- Website: https://www.psx.com.pk
- Market Data Services: https://www.psx.com.pk/psx/market-data-services

**Disclaimer:** This tool is provided as-is for internal use. The authors are not responsible for any misuse of PSX data or violations of PSX's terms of service.
