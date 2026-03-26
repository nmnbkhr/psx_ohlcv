# pakfindata App Lineage — Documentation & Enhancement Prompt

## Part 1: Documentation

### What App Lineage Is

The App Lineage page (`Admin → App Lineage`) is an auto-discovered dependency graph 
that maps the entire pakfindata application — every page, tab, DB table, and navigation 
group — as an interactive force-directed network.

**Current stats:** 81 pages, 191 tabs, 49 DB tables, 11 navigation groups.

### How It Works

The page auto-discovers the application structure by scanning:
- **Pages** (`.py` files in `page_views/`) — shown as colored circles, grouped by nav section
- **Tabs** (within each page) — shown as diamonds (toggled via "Show tabs" checkbox)
- **DB Tables** (DuckDB + SQLite) — shown as boxes (toggled via "Show DB tables" checkbox)
- **Nav Groups** (sidebar sections) — used for color coding

**Edges (connections):** Lines connect pages to the DB tables they query and to tabs they contain.

### Interaction

- **Click a node** → Detail panel appears with metadata (page name, file path, connected tables/tabs)
- **Filter by group** dropdown → isolate EQUITIES, RESEARCH, STRATEGIES, etc.
- **Show tabs** checkbox → adds 191 tab nodes (dense but complete)
- **Show DB tables** checkbox → shows 49 DB table nodes connected to their consumer pages
- **Refresh Lineage** → re-scans the codebase for changes
- **Legend** expander → color key for all 11 nav groups
- **Full page listing** expander → flat table of all pages

### Current Node Layout

Observed from the force graph:

**Dense core cluster:** Most pages (Dashboard, Stock Screener, Company Profile, Sector Analysis, 
Signal Analysis, Microstructure, etc.) connect to shared DB tables (eod_ohlcv, tick_logs, 
psx_indices, company_quotes, etc.) — forming a dense interconnected cluster.

**Satellite nodes (loosely connected):**
- Rate History (top-left, orange) — connects to SBP/rate-specific tables
- AI Chat (left, gray) — standalone
- VWAP Execution (bottom-left, yellow) — strategy page, connects to ohlcv_5s
- Macro Regime (right, yellow) — strategy page, connects to SBP + EOD tables
- App Lineage (far right, cyan) — meta page, no data dependencies

**Strategy pages (yellow nodes):** Currently floating at periphery because they're newly 
added and have fewer DB table connections than the core analytics pages.

### Node Color Coding (11 groups)

| Color | Nav Group | Example Pages |
|-------|-----------|--------------|
| Varies | ADMIN | Data Status, Sync Center, Schema Explorer, App Lineage |
| Varies | ALM | ALM Dashboard, FTP Monitor |
| Varies | COMMODITIES | Commodities, PMEX |
| Varies | EQUITIES | Stock Screener, Company Profile, Sector Analysis, Intraday |
| Varies | FIXED INCOME | Rates Overview, Yield Curves, Treasury Auctions, Bond Market |
| Varies | FUNDS | Fund Explorer, VPS Pension, Top Performers, ETFs |
| Varies | FX & RATES | Currency Dashboard, FX Dashboard, Interbank vs Open |
| Varies | HIDDEN | Pages not in sidebar |
| Varies | MARKET OVERVIEW | Dashboard, Market Pulse, Index Monitor |
| Varies | RESEARCH | Signal Analysis, Microstructure, Tick Analytics, Tick Replay |
| Yellow | STRATEGIES | VPIN, OFI, CVD, Basis Arb, VWAP Execution, Macro Regime |

---

## Part 2: Claude Code Enhancement Prompt

### Claude Code Prompt: Enhance App Lineage with Strategy Data Flow & Live Metrics

#### Context

The App Lineage page at `src/pakfindata/ui/page_views/app_lineage.py` shows an 
auto-discovered dependency graph. Currently:
- Strategy pages (VPIN, OFI, CVD, etc.) float at the periphery as isolated nodes
- DB table nodes show connections but no metadata (row counts, freshness)
- No data flow direction (arrows showing source → consumer)
- No engine/computation layer visible
- Click detail panel is basic

#### Step 1: Understand current implementation

```bash
# Read the lineage page source
cat ~/pakfindata/src/pakfindata/ui/page_views/app_lineage.py

# Check how graph data is built
grep -n "node\|edge\|vertex\|graph\|lineage\|discover" \
    ~/pakfindata/src/pakfindata/ui/page_views/app_lineage.py | head -30

# Check if there's a separate lineage engine
find ~/pakfindata/src/ -name "*lineage*" -o -name "*graph*" -o -name "*discover*" | head -10

# Check the engine directory for strategy files
ls ~/pakfindata/src/pakfindata/engine/
```

**READ ALL OUTPUT before proceeding.**

#### Step 2: Add Strategy → Data Source connections

Currently strategy pages are disconnected. Add edges that connect each strategy 
to its data sources:

```python
# Strategy → DB table connections to add:
STRATEGY_EDGES = {
    "VPIN Strategy": {
        "reads": ["tick_logs", "eod_ohlcv"],
        "engine": "engine/vpin_strategy.py",
        "description": "VPIN toxicity + Hurst regime detection",
    },
    "OFI Alpha": {
        "reads": ["tick_logs"],
        "engine": "engine/ofi_strategy.py",
        "description": "Order Flow Imbalance from bid/ask volumes",
    },
    "CVD Divergence": {
        "reads": ["tick_logs"],
        "engine": "engine/cvd_strategy.py",
        "description": "Cumulative Volume Delta divergence detection",
    },
    "Basis Arb": {
        "reads": ["eod_ohlcv", "futures_contracts"],
        "engine": "engine/basis_strategy.py",
        "description": "Futures basis mean-reversion",
    },
    "VWAP Execution": {
        "reads": ["ohlcv_5s", "eod_ohlcv"],
        "engine": "engine/vwap_execution.py",
        "description": "Volume profile + VWAP execution optimizer",
    },
    "Macro Regime": {
        "reads": ["eod_ohlcv", "kibor_daily", "sbp_easydata"],
        "engine": "engine/macro_regime_hmm.py",
        "description": "Cross-asset HMM regime detection",
    },
    "Sector Rotation": {
        "reads": ["eod_ohlcv", "sector_summary"],
        "engine": "engine/sector_rotation.py",
        "description": "Sector momentum ranking + rotation signals",
    },
    "ML Predictions": {
        "reads": ["eod_ohlcv", "tick_logs", "ohlcv_5s"],
        "engine": "engine/ml_model.py",
        "description": "XGBoost/LightGBM direction prediction",
    },
}
```

Add these edges to the graph builder so strategy nodes connect to their data tables.

#### Step 3: Add Engine layer nodes

Currently the graph shows Pages → DB Tables. Add a middle layer:

```
[Data Sources] → [Engine Files] → [UI Pages]

Example:
  tick_logs (DB) → vpin_strategy.py (Engine) → VPIN Strategy (Page)
  tick_logs (DB) → ofi_strategy.py (Engine) → OFI Alpha (Page)
  eod_ohlcv (DB) → ml_features.py (Engine) → ML Predictions (Page)
```

Engine nodes should be a distinct shape/color:
- **Shape:** hexagon or pentagon (distinct from circles/diamonds/boxes)
- **Color:** `#C8A96E` (gold — matches pakfindata accent)
- **Label:** filename without path (e.g., `vpin_strategy.py`)
- **Toggle:** "Show engines" checkbox alongside "Show tabs" and "Show DB tables"

Discovery method:
```python
# Auto-discover engine files
import os
engine_dir = Path("src/pakfindata/engine")
for f in engine_dir.glob("*.py"):
    if f.name != "__init__.py":
        # Parse imports to find which DB tables it reads
        content = f.read_text()
        tables_used = []
        for table in all_db_tables:
            if table in content:
                tables_used.append(table)
        
        # Find which pages import this engine
        pages_using = []
        for page in all_pages:
            page_content = page.read_text()
            if f.stem in page_content:
                pages_using.append(page.stem)
```

#### Step 4: Add data flow direction (arrows)

Currently edges are undirected lines. Make them directional:

- **DB Table → Engine:** data flows FROM table TO engine (arrow pointing to engine)
- **Engine → Page:** computed results flow FROM engine TO page
- **External Source → DB Table:** PSX WebSocket → tick_logs, SBP API → sbp_easydata

In the D3/Plotly graph, add arrowheads:
```javascript
// For D3 force graph
svg.append("defs").append("marker")
    .attr("id", "arrowhead")
    .attr("viewBox", "0 -5 10 10")
    .attr("refX", 20)
    .attr("refY", 0)
    .attr("markerWidth", 6)
    .attr("markerHeight", 6)
    .attr("orient", "auto")
    .append("path")
    .attr("d", "M0,-5L10,0L0,5")
    .attr("fill", "#6B7280");

link.attr("marker-end", "url(#arrowhead)");
```

#### Step 5: Enhance click detail panel

When a node is clicked, show richer information:

**For Page nodes:**
```
╔══════════════════════════════════════╗
║ VPIN Strategy                        ║
║ Type: Strategy Page                  ║
║ File: page_views/strategy_vpin.py    ║
║ Nav Group: STRATEGIES                ║
║ Tabs: 4 (Live Signal, Backtest,      ║
║        Scanner, Methodology)         ║
║ ─────────────────────────────────── ║
║ Reads from:                          ║
║   📊 tick_logs (4.6M rows)           ║
║   📊 eod_ohlcv (1.2M rows)          ║
║ ─────────────────────────────────── ║
║ Engine: vpin_strategy.py             ║
║ Description: VPIN toxicity +         ║
║   Hurst regime switching signal      ║
║ ─────────────────────────────────── ║
║ [Open Page →]                        ║
╚══════════════════════════════════════╝
```

**For DB Table nodes:**
```
╔══════════════════════════════════════╗
║ tick_logs                            ║
║ Type: DuckDB Table                   ║
║ Database: pakfindata.duckdb          ║
║ ─────────────────────────────────── ║
║ Rows: 4,612,893                      ║
║ Date range: 2025-10-15 → 2026-03-25 ║
║ Last updated: 1 day ago              ║
║ Size: ~280 MB                        ║
║ ─────────────────────────────────── ║
║ Columns: 9                           ║
║   price, volume, bid, ask,           ║
║   bidVol, askVol, change,            ║
║   timestamp, date                    ║
║ ─────────────────────────────────── ║
║ Used by: 8 pages                     ║
║   Microstructure, Tick Analytics,    ║
║   VPIN Strategy, OFI Alpha,         ║
║   CVD Divergence, Signal Analysis,  ║
║   Tick Replay, ML Predictions       ║
║ ─────────────────────────────────── ║
║ Source: PSX WebSocket → Cloud JSONL  ║
║   → DuckDB sync (every 15 min)      ║
╚══════════════════════════════════════╝
```

**For Engine nodes:**
```
╔══════════════════════════════════════╗
║ vpin_strategy.py                     ║
║ Type: Computation Engine             ║
║ Path: engine/vpin_strategy.py        ║
║ ─────────────────────────────────── ║
║ Functions: 7                         ║
║   compute_vpin()                     ║
║   compute_hurst()                    ║
║   classify_vpin_state()              ║
║   generate_signal()                  ║
║   backtest_vpin_strategy()           ║
║ ─────────────────────────────────── ║
║ Reads: tick_logs, eod_ohlcv          ║
║ Used by: VPIN Strategy page          ║
║ Dependencies: scipy, numpy, pandas   ║
╚══════════════════════════════════════╝
```

Implementation: Query DB metadata on click:
```python
def get_table_metadata(table_name: str) -> dict:
    """Get row count, date range, columns for a DB table."""
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    try:
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        cols = con.execute(f"DESCRIBE {table_name}").df()
        
        # Try to get date range
        date_range = None
        if "date" in cols["column_name"].values:
            dr = con.execute(f"SELECT MIN(date), MAX(date) FROM {table_name}").fetchone()
            date_range = {"min": str(dr[0]), "max": str(dr[1])}
        
        return {
            "rows": count,
            "columns": cols["column_name"].tolist(),
            "date_range": date_range,
        }
    except:
        return {}
    finally:
        con.close()
```

#### Step 6: Add external data source nodes

Add nodes for external data sources that feed into DB tables:

```python
EXTERNAL_SOURCES = {
    "PSX WebSocket": {
        "type": "external",
        "color": "#FF6B6B",
        "feeds": ["tick_logs"],
        "description": "Real-time tick data via WebSocket relay",
    },
    "PSX DPS API": {
        "type": "external",
        "color": "#FF6B6B",
        "feeds": ["ohlcv_5s", "intraday_klines"],
        "description": "5-second OHLCV bars via DPS timeseries",
    },
    "PSX Downloads": {
        "type": "external",
        "color": "#FF6B6B",
        "feeds": ["eod_ohlcv", "futures_contracts", "sector_summary", "index_constituents"],
        "description": "Daily EOD, DFC XLS, sector data from PSX website",
    },
    "SBP EasyData": {
        "type": "external",
        "color": "#4ECDC4",
        "feeds": ["kibor_daily", "sbp_rates", "fx_interbank"],
        "description": "195 datasets via SBP EasyData API (key in quotes)",
    },
    "MUFAP": {
        "type": "external",
        "color": "#4ECDC4",
        "feeds": ["mufap_nav", "fund_performance"],
        "description": "Mutual fund NAV data via DrissionPage scraper",
    },
    "Forex.pk": {
        "type": "external",
        "color": "#4ECDC4",
        "feeds": ["forex_kerb"],
        "description": "Kerb/open market FX rates (no EasyData equivalent)",
    },
    "PMEX": {
        "type": "external",
        "color": "#4ECDC4",
        "feeds": ["pmex_ohlcv", "pmex_margins"],
        "description": "Commodity futures via JSON API + DrissionPage",
    },
}
```

Shape: pentagon or star (distinct from pages/engines/tables).
Position: left side of graph (data flows left → right).

#### Step 7: Add "Data Flow View" toggle

Add a new view mode alongside the force graph:

**Force Graph (current):** All nodes freely positioned by D3 force simulation.
**Data Flow View (new):** Layered left-to-right layout:

```
Layer 1 (left)     Layer 2          Layer 3          Layer 4 (right)
External Sources → DB Tables    →   Engines      →   Pages
──────────────    ──────────        ──────────        ──────────
PSX WebSocket  →  tick_logs    →   vpin_strategy →  VPIN Strategy
PSX DPS API    →  ohlcv_5s    →   ofi_strategy  →  OFI Alpha
PSX Downloads  →  eod_ohlcv   →   cvd_strategy  →  CVD Divergence
SBP EasyData   →  kibor_daily →   basis_strategy→  Basis Arb
MUFAP          →  mufap_nav   →   vwap_execution→  VWAP Execution
Forex.pk       →  forex_kerb  →   macro_regime  →  Macro Regime
                                   sector_rotation→ Sector Rotation
                                   ml_model      →  ML Predictions
```

Implementation: Use Plotly Sankey diagram or a custom D3 layered layout.

Toggle between views with a radio button:
```python
view_mode = st.radio("View", ["Force Graph", "Data Flow"], horizontal=True)
```

#### Step 8: Add freshness indicators to DB table nodes

Color DB table nodes by data freshness:
- **Green border:** updated today
- **Yellow border:** updated 1-3 days ago
- **Red border:** updated 4+ days ago

```python
def get_table_freshness(table_name: str) -> str:
    """Returns 'fresh', 'stale', or 'old'."""
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    try:
        cols = [r[0] for r in con.execute(f"DESCRIBE {table_name}").fetchall()]
        if "date" in cols:
            max_date = con.execute(f"SELECT MAX(date) FROM {table_name}").fetchone()[0]
            if max_date:
                days_old = (datetime.now().date() - pd.to_datetime(str(max_date)).date()).days
                if days_old <= 1: return "fresh"
                elif days_old <= 3: return "stale"
                else: return "old"
    except: pass
    finally: con.close()
    return "unknown"
```

#### Step 9: Add strategy pipeline visualization

Below the main graph, add a "Strategy Pipeline" section that shows how 
strategies connect to each other:

```
┌─────────────────────────────────────────────────────────┐
│                  STRATEGY PIPELINE                       │
│                                                         │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐             │
│  │ Strategy 6│  │Strategy 7│  │ ML Pred  │  ALLOCATION │
│  │Macro HMM │→ │Sector Rot│→ │ XGBoost  │  LAYER      │
│  │(regime)  │  │(which sec)│  │(which stk)│             │
│  └──────────┘  └──────────┘  └──────────┘             │
│       ↓                           ↓                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐             │
│  │Strategy 1│  │Strategy 2│  │Strategy 3│  SIGNAL     │
│  │VPIN Reg. │  │OFI Alpha │  │CVD Diverg│  LAYER      │
│  │(when)    │  │(direction)│  │(reversal)│             │
│  └──────────┘  └──────────┘  └──────────┘             │
│       ↓              ↓              ↓                   │
│  ┌──────────────────────────────────────┐              │
│  │         Strategy 4: Basis Arb         │  EXECUTION  │
│  │         Strategy 5: VWAP Execution    │  LAYER      │
│  └──────────────────────────────────────┘              │
└─────────────────────────────────────────────────────────┘
```

Implement as a static Plotly/D3 diagram or Streamlit columns + cards:
```python
st.markdown("### Strategy Pipeline")
col1, col2, col3 = st.columns(3)
with col1:
    st.markdown("**ALLOCATION**")
    st.info("Strategy 6: Macro HMM → WHICH regime")
    st.info("Strategy 7: Sector Rotation → WHICH sectors")
with col2:
    st.markdown("**SIGNAL**")
    st.success("Strategy 1: VPIN → WHEN to trade")
    st.success("Strategy 2: OFI → WHICH direction")
    st.success("Strategy 3: CVD → WHERE reversals")
with col3:
    st.markdown("**EXECUTION**")
    st.warning("Strategy 4: Basis Arb → market-neutral")
    st.warning("Strategy 5: VWAP → HOW to execute")
```

## IMPORTANT NOTES

1. **DO NOT break existing lineage functionality** — all enhancements are additive
2. **Engine discovery must be dynamic** — scan `engine/` dir, don't hardcode
3. **DB metadata queries use read_only=True** — no writes ever
4. **Freshness checks should be cached** — `@st.cache_data(ttl=300)` (5 min)
5. **External source nodes are static** — defined in code, not discovered
6. **Strategy edges from Step 2 can be hardcoded** — these don't change often
7. **Data Flow View is optional but high-impact** — if too complex, skip and focus on Steps 2-5
8. **Node click detail should use `st.session_state`** — store clicked node, show detail in sidebar or below graph
9. **All new checkboxes default OFF** — don't clutter the default view
10. **Match Bloomberg theme** — `#0B0E11` bg, `#C8A96E` gold accent, `#E0E0E0` text
