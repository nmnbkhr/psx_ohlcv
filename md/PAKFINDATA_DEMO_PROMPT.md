# Claude Code Prompt: pakfindata Docker Demo

## CRITICAL RULE

**DO NOT modify ANY file in ~/pakfindata/. The existing codebase stays untouched.**
Everything goes in a NEW folder: `~/pakfindata-demo/`

## Context

pakfindata is a Streamlit multi-page PSX analytics terminal at ~/pakfindata/.
We want a self-contained Docker demo that anyone can run with one command.
Demo uses trimmed data (top 50 symbols), read-only mode, no live scrapers.

## Step 1: Understand the codebase

```bash
# List all pages
ls ~/pakfindata/src/pakfindata/ui/page_views/

# List all data sources/services
ls ~/pakfindata/src/pakfindata/sources/
ls ~/pakfindata/src/pakfindata/services/
ls ~/pakfindata/src/pakfindata/engine/
ls ~/pakfindata/src/pakfindata/db/
ls ~/pakfindata/src/pakfindata/api/

# Find ALL hardcoded paths
grep -rn "/mnt/e/psxdata\|tick_bars\.db\|psx\.sqlite\|pakfindata\.duckdb\|tick_logs_cloud\|tick_logs/" \
    ~/pakfindata/src/ --include="*.py" | grep -v __pycache__ | sort -t: -k1,1 -u

# Find all imports and dependencies
cat ~/pakfindata/requirements.txt 2>/dev/null || \
pip list --format=freeze 2>/dev/null | head -50

# Check current conda/venv
conda list -n psx 2>/dev/null | head -30
```

**STOP — read ALL output before proceeding.**

## Step 2: Create demo folder structure

```bash
mkdir -p ~/pakfindata-demo/{src,data,scripts}
```

```
~/pakfindata-demo/
├── Dockerfile
├── docker-compose.yml
├── .dockerignore
├── requirements.txt
├── README.md
├── entrypoint.sh
├── scripts/
│   ├── create_demo_db.py      # Trims full DB to top 50 symbols
│   └── setup_demo_data.sh     # Copies trimmed data
├── src/
│   └── pakfindata/            # Modified copy of app code
│       ├── config.py          # NEW — centralized path config with env vars
│       ├── demo.py            # NEW — demo mode helpers
│       ├── ui/
│       ├── db/
│       ├── engine/
│       ├── sources/           # Stripped — no live scrapers
│       └── ...
└── data/                      # Demo data (generated)
    ├── psx.sqlite             # Trimmed (~30 MB)
    ├── tick_bars.db           # Trimmed (~10 MB)
    ├── pakfindata.duckdb      # Trimmed (~20 MB)
    ├── tick_logs_cloud/       # 2-3 days sample JSONL
    ├── intraday/              # Sample CSVs
    ├── downloads/             # Sample PSX download files
    └── sbp_easydata/          # Sample SBP data
```

## Step 3: Create centralized config (config.py)

This replaces ALL hardcoded paths in the demo copy. The original code is untouched.

```python
"""
pakfindata configuration — all paths and settings in one place.

In demo mode (Docker), paths point to /app/data/
In production, paths point to /mnt/e/psxdata/

Set via environment variables or defaults.
"""

import os
from pathlib import Path

# ═══════════════════════════════════════
# MODE
# ═══════════════════════════════════════
DEMO_MODE = os.environ.get("DEMO_MODE", "0") == "1"

# ═══════════════════════════════════════
# DATA PATHS
# ═══════════════════════════════════════
DATA_ROOT = Path(os.environ.get("PSX_DATA_ROOT", "/app/data" if DEMO_MODE else "/mnt/e/psxdata"))

SQLITE_PSX_PATH = Path(os.environ.get("PSX_DB_PATH", str(DATA_ROOT / "psx.sqlite")))
SQLITE_TICK_PATH = Path(os.environ.get("TICK_DB_PATH", str(DATA_ROOT / "tick_bars.db")))
DUCKDB_PATH = Path(os.environ.get("DUCKDB_PATH", str(DATA_ROOT / "pakfindata.duckdb")))

JSONL_CLOUD_DIR = Path(os.environ.get("JSONL_CLOUD_DIR", str(DATA_ROOT / "tick_logs_cloud")))
JSONL_LOCAL_DIR = Path(os.environ.get("JSONL_LOCAL_DIR", str(DATA_ROOT / "tick_logs")))
INTRADAY_DIR = Path(os.environ.get("INTRADAY_DIR", str(DATA_ROOT / "intraday")))
DOWNLOADS_DIR = Path(os.environ.get("DOWNLOADS_DIR", str(DATA_ROOT / "downloads")))
SBP_DIR = Path(os.environ.get("SBP_DIR", str(DATA_ROOT / "sbp_easydata")))

# ═══════════════════════════════════════
# LLM CONFIG
# ═══════════════════════════════════════
# Demo supports: OpenAI API, Anthropic API, or none
LLM_PROVIDER = os.environ.get("LLM_PROVIDER", "none")  # "openai", "anthropic", "none"
LLM_API_KEY = os.environ.get("LLM_API_KEY", "")
LLM_MODEL = os.environ.get("LLM_MODEL", "gpt-4o-mini")  # default cheap model

# ═══════════════════════════════════════
# DEMO SETTINGS
# ═══════════════════════════════════════
DEMO_SYMBOLS = 50          # Top N symbols by volume
DEMO_DAYS = 30             # Days of historical data
DEMO_TICK_DAYS = 3         # Days of tick JSONL data
```

## Step 4: Create demo mode helpers (demo.py)

```python
"""
Demo mode utilities — tooltips, badges, feature gating.
"""

import streamlit as st
from pakfindata.config import DEMO_MODE, LLM_PROVIDER, LLM_API_KEY

def demo_badge():
    """Show demo badge in sidebar."""
    if DEMO_MODE:
        st.sidebar.markdown("""
        <div style="background:#1E2530;border:1px solid #C8A96E;border-radius:8px;
                    padding:8px 12px;margin-bottom:16px;text-align:center;">
            <span style="color:#C8A96E;font-weight:bold;font-size:14px;">📊 DEMO MODE</span><br>
            <span style="color:#888;font-size:11px;">Top 50 symbols • 30 days history</span>
        </div>
        """, unsafe_allow_html=True)


def demo_tooltip(feature: str, description: str):
    """Show tooltip explaining a demo feature."""
    if DEMO_MODE:
        with st.expander(f"ℹ️ {feature}", expanded=False):
            st.caption(description)


def gate_live_feature(feature_name: str) -> bool:
    """Block live/scraping features in demo mode."""
    if DEMO_MODE:
        st.info(f"🔒 **{feature_name}** is disabled in demo mode. "
                f"Live data requires PSX API access.")
        return False
    return True


def llm_available() -> bool:
    """Check if LLM is configured."""
    return LLM_PROVIDER != "none" and LLM_API_KEY != ""


def get_llm_response(prompt: str, system: str = "") -> str:
    """Get LLM response using configured provider."""
    if not llm_available():
        return "💡 AI commentary requires an API key. Set LLM_API_KEY in docker-compose.yml"
    
    try:
        if LLM_PROVIDER == "openai":
            import openai
            client = openai.OpenAI(api_key=LLM_API_KEY)
            response = client.chat.completions.create(
                model=LLM_MODEL,
                messages=[
                    {"role": "system", "content": system} if system else {},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=500
            )
            return response.choices[0].message.content
        
        elif LLM_PROVIDER == "anthropic":
            import anthropic
            client = anthropic.Anthropic(api_key=LLM_API_KEY)
            response = client.messages.create(
                model=LLM_MODEL or "claude-sonnet-4-20250514",
                max_tokens=500,
                system=system,
                messages=[{"role": "user", "content": prompt}]
            )
            return response.content[0].text
        
        else:
            return "⚠️ Unknown LLM provider. Use 'openai' or 'anthropic'."
    
    except Exception as e:
        return f"⚠️ LLM error: {str(e)}"
```

## Step 5: Create demo DB trimmer script

```python
"""
scripts/create_demo_db.py — Create trimmed demo databases from full production data.

Keeps top 50 symbols by volume, last 30 days of data.
Output: ~/pakfindata-demo/data/

Run: python scripts/create_demo_db.py
"""

import sqlite3
import shutil
from pathlib import Path
from datetime import datetime, timedelta

FULL_PSX = Path("/mnt/e/psxdata/psx.sqlite")
FULL_TICK = Path("/mnt/e/psxdata/tick_bars.db")
FULL_DUCK = Path("/mnt/e/psxdata/pakfindata.duckdb")
JSONL_DIR = Path("/mnt/e/psxdata/tick_logs_cloud")
INTRADAY_DIR = Path("/mnt/e/psxdata/intraday")
DOWNLOADS_DIR = Path("/mnt/e/psxdata/downloads")
SBP_DIR = Path("/mnt/e/psxdata/sbp_easydata")

DEMO_DIR = Path.home() / "pakfindata-demo" / "data"
TOP_N = 50
DAYS = 30
TICK_DAYS = 3

def main():
    DEMO_DIR.mkdir(parents=True, exist_ok=True)
    
    print("═══════════════════════════════════════")
    print("  Creating Demo Data")
    print("═══════════════════════════════════════")
    
    # ── 1. Find top 50 symbols by volume ──
    print("\n📊 Finding top 50 symbols...")
    con = sqlite3.connect(str(FULL_PSX))
    cutoff = (datetime.now() - timedelta(days=DAYS)).strftime("%Y-%m-%d")
    
    top_symbols = [row[0] for row in con.execute(f"""
        SELECT symbol FROM eod_ohlcv 
        WHERE date >= '{cutoff}'
        GROUP BY symbol 
        ORDER BY SUM(volume) DESC 
        LIMIT {TOP_N}
    """).fetchall()]
    con.close()
    
    if not top_symbols:
        # Fallback — try daily_ohlcv
        con = sqlite3.connect(str(FULL_PSX))
        top_symbols = [row[0] for row in con.execute(f"""
            SELECT symbol FROM daily_ohlcv 
            WHERE date >= '{cutoff}'
            GROUP BY symbol 
            ORDER BY SUM(volume) DESC 
            LIMIT {TOP_N}
        """).fetchall()]
        con.close()
    
    placeholders = ",".join(f"'{s}'" for s in top_symbols)
    print(f"  Top {len(top_symbols)}: {', '.join(top_symbols[:10])}...")
    
    # ── 2. Trim psx.sqlite ──
    print("\n📦 Creating demo psx.sqlite...")
    demo_psx = DEMO_DIR / "psx.sqlite"
    if demo_psx.exists():
        demo_psx.unlink()
    
    # Copy structure and reference tables fully
    src = sqlite3.connect(str(FULL_PSX))
    dst = sqlite3.connect(str(demo_psx))
    
    # Get all tables
    tables = [row[0] for row in src.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    
    for table in tables:
        # Get schema
        schema = src.execute(
            f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}'"
        ).fetchone()[0]
        
        try:
            dst.execute(schema)
        except:
            continue
        
        # Determine if table has symbol column
        cols = [row[1] for row in src.execute(f"PRAGMA table_info({table})").fetchall()]
        
        if "symbol" in cols:
            # Filter by top symbols + date if available
            if "date" in cols:
                rows = src.execute(f"""
                    SELECT * FROM {table} 
                    WHERE symbol IN ({placeholders}) AND date >= '{cutoff}'
                """).fetchall()
            else:
                rows = src.execute(f"""
                    SELECT * FROM {table} 
                    WHERE symbol IN ({placeholders})
                """).fetchall()
        else:
            # Copy all rows (reference table)
            rows = src.execute(f"SELECT * FROM {table}").fetchall()
        
        if rows:
            dst.executemany(
                f"INSERT OR IGNORE INTO {table} VALUES ({','.join('?' * len(rows[0]))})",
                rows
            )
            print(f"  {table}: {len(rows)} rows")
    
    dst.execute("VACUUM")
    dst.commit()
    dst.close()
    src.close()
    
    demo_size = demo_psx.stat().st_size / 1024 / 1024
    print(f"  → demo psx.sqlite: {demo_size:.1f} MB")
    
    # ── 3. Trim tick_bars.db ──
    print("\n📦 Creating demo tick_bars.db...")
    demo_tick = DEMO_DIR / "tick_bars.db"
    if demo_tick.exists():
        demo_tick.unlink()
    
    if FULL_TICK.exists():
        src = sqlite3.connect(str(FULL_TICK))
        dst = sqlite3.connect(str(demo_tick))
        
        tables = [row[0] for row in src.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        
        for table in tables:
            schema = src.execute(
                f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}'"
            ).fetchone()[0]
            
            try:
                dst.execute(schema)
            except:
                continue
            
            cols = [row[1] for row in src.execute(f"PRAGMA table_info({table})").fetchall()]
            
            if "symbol" in cols:
                rows = src.execute(f"""
                    SELECT * FROM {table} WHERE symbol IN ({placeholders})
                    ORDER BY ROWID DESC LIMIT 100000
                """).fetchall()
            else:
                rows = src.execute(f"SELECT * FROM {table} LIMIT 100000").fetchall()
            
            if rows:
                dst.executemany(
                    f"INSERT OR IGNORE INTO {table} VALUES ({','.join('?' * len(rows[0]))})",
                    rows
                )
                print(f"  {table}: {len(rows)} rows")
        
        dst.execute("VACUUM")
        dst.commit()
        dst.close()
        src.close()
    
    # ── 4. Create demo DuckDB ──
    print("\n📦 Creating demo DuckDB...")
    demo_duck = DEMO_DIR / "pakfindata.duckdb"
    if demo_duck.exists():
        demo_duck.unlink()
    
    try:
        import duckdb
        
        src_con = duckdb.connect(str(FULL_DUCK), read_only=True)
        dst_con = duckdb.connect(str(demo_duck))
        
        tables = src_con.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'main'
        """).fetchall()
        
        for (table,) in tables:
            # Get schema
            schema = src_con.execute(f"DESCRIBE {table}").df()
            
            # Check if symbol column exists
            has_symbol = "symbol" in schema["column_name"].values
            has_date = "date" in schema["column_name"].values
            
            if has_symbol:
                where = f"WHERE symbol IN ({placeholders})"
                if has_date:
                    where += f" AND date >= '{cutoff}'"
                
                df = src_con.execute(f"SELECT * FROM {table} {where}").df()
            else:
                df = src_con.execute(f"SELECT * FROM {table} LIMIT 50000").df()
            
            if not df.empty:
                dst_con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df")
                print(f"  {table}: {len(df)} rows")
        
        dst_con.close()
        src_con.close()
    except ImportError:
        print("  ⚠️ DuckDB not available — skipping")
    
    # ── 5. Copy sample JSONL tick files ──
    print("\n📦 Copying sample tick JSONL...")
    demo_jsonl = DEMO_DIR / "tick_logs_cloud"
    demo_jsonl.mkdir(parents=True, exist_ok=True)
    
    if JSONL_DIR.exists():
        jsonl_files = sorted(JSONL_DIR.glob("*.jsonl"), reverse=True)[:TICK_DAYS]
        for f in jsonl_files:
            # Filter to top symbols only (reduce file size)
            import json
            out_path = demo_jsonl / f.name
            count = 0
            with open(f) as src_f, open(out_path, "w") as dst_f:
                for line in src_f:
                    try:
                        rec = json.loads(line.strip())
                        if rec.get("symbol") in top_symbols:
                            dst_f.write(line)
                            count += 1
                    except:
                        continue
            size = out_path.stat().st_size / 1024 / 1024
            print(f"  {f.name}: {count} ticks ({size:.1f} MB)")
    
    # ── 6. Copy sample downloads ──
    print("\n📦 Copying sample downloads...")
    demo_downloads = DEMO_DIR / "downloads"
    if DOWNLOADS_DIR.exists():
        # Copy latest day's downloads
        daily_dirs = sorted(DOWNLOADS_DIR.glob("daily/*/"), reverse=True)[:3]
        for d in daily_dirs:
            dest = demo_downloads / "daily" / d.name
            if d.is_dir():
                shutil.copytree(d, dest, dirs_exist_ok=True)
                print(f"  daily/{d.name}/")
        
        # Copy reference files
        ref_dir = DOWNLOADS_DIR / "reference"
        if ref_dir.exists():
            shutil.copytree(ref_dir, demo_downloads / "reference", dirs_exist_ok=True)
            print(f"  reference/")
    
    # ── 7. Copy sample SBP data ──
    print("\n📦 Copying sample SBP data...")
    demo_sbp = DEMO_DIR / "sbp_easydata"
    if SBP_DIR.exists():
        # Copy catalog
        catalog = SBP_DIR / "catalog.json"
        if catalog.exists():
            demo_sbp.mkdir(parents=True, exist_ok=True)
            shutil.copy2(catalog, demo_sbp / "catalog.json")
        
        # Copy priority series (first 50 files)
        series_dir = SBP_DIR / "series"
        if series_dir.exists():
            demo_series = demo_sbp / "series"
            demo_series.mkdir(parents=True, exist_ok=True)
            for f in sorted(series_dir.glob("*.json"))[:50]:
                shutil.copy2(f, demo_series / f.name)
            for f in sorted(series_dir.glob("*.csv"))[:50]:
                shutil.copy2(f, demo_series / f.name)
            print(f"  {len(list(demo_series.glob('*.json')))} series files")
    
    # ── 8. Summary ──
    total = sum(f.stat().st_size for f in DEMO_DIR.rglob("*") if f.is_file())
    print(f"\n✅ Demo data created: {total / 1024 / 1024:.0f} MB total")
    print(f"   Location: {DEMO_DIR}")
    print(f"   Symbols: {len(top_symbols)}")
    print(f"   Top symbols: {', '.join(top_symbols[:10])}...")


if __name__ == "__main__":
    main()
```

## Step 6: Copy and patch source code

**Copy src/ from production, then patch paths to use config.py.**

```bash
# Copy entire source
cp -r ~/pakfindata/src/pakfindata ~/pakfindata-demo/src/

# Copy streamlit config
cp -r ~/pakfindata/.streamlit ~/pakfindata-demo/ 2>/dev/null || true
```

Now create a patch script that replaces all hardcoded paths:

```python
"""
scripts/patch_paths.py — Replace hardcoded paths with config.py imports.

Scans all .py files in src/ and replaces:
  /mnt/e/psxdata/psx.sqlite     → config.SQLITE_PSX_PATH
  /mnt/e/psxdata/tick_bars.db   → config.SQLITE_TICK_PATH
  /mnt/e/psxdata/pakfindata.duckdb → config.DUCKDB_PATH
  etc.
"""

import re
from pathlib import Path

SRC_DIR = Path.home() / "pakfindata-demo" / "src" / "pakfindata"

# Path replacements
REPLACEMENTS = {
    '"/mnt/e/psxdata/psx.sqlite"': 'str(config.SQLITE_PSX_PATH)',
    "'/mnt/e/psxdata/psx.sqlite'": 'str(config.SQLITE_PSX_PATH)',
    '"/mnt/e/psxdata/tick_bars.db"': 'str(config.SQLITE_TICK_PATH)',
    "'/mnt/e/psxdata/tick_bars.db'": 'str(config.SQLITE_TICK_PATH)',
    '"/mnt/e/psxdata/pakfindata.duckdb"': 'str(config.DUCKDB_PATH)',
    "'/mnt/e/psxdata/pakfindata.duckdb'": 'str(config.DUCKDB_PATH)',
    '"/mnt/e/psxdata/tick_logs_cloud"': 'str(config.JSONL_CLOUD_DIR)',
    "'/mnt/e/psxdata/tick_logs_cloud'": 'str(config.JSONL_CLOUD_DIR)',
    '"/mnt/e/psxdata/tick_logs"': 'str(config.JSONL_LOCAL_DIR)',
    "'/mnt/e/psxdata/tick_logs'": 'str(config.JSONL_LOCAL_DIR)',
    '"/mnt/e/psxdata/intraday"': 'str(config.INTRADAY_DIR)',
    "'/mnt/e/psxdata/intraday'": 'str(config.INTRADAY_DIR)',
    '"/mnt/e/psxdata/downloads"': 'str(config.DOWNLOADS_DIR)',
    "'/mnt/e/psxdata/downloads'": 'str(config.DOWNLOADS_DIR)',
    '"/mnt/e/psxdata/sbp_easydata"': 'str(config.SBP_DIR)',
    "'/mnt/e/psxdata/sbp_easydata'": 'str(config.SBP_DIR)',
}

# Also catch Path() constructs
PATH_REPLACEMENTS = {
    'Path("/mnt/e/psxdata/psx.sqlite")': 'config.SQLITE_PSX_PATH',
    "Path('/mnt/e/psxdata/psx.sqlite')": 'config.SQLITE_PSX_PATH',
    'Path("/mnt/e/psxdata/tick_bars.db")': 'config.SQLITE_TICK_PATH',
    "Path('/mnt/e/psxdata/tick_bars.db')": 'config.SQLITE_TICK_PATH',
    'Path("/mnt/e/psxdata/pakfindata.duckdb")': 'config.DUCKDB_PATH',
    "Path('/mnt/e/psxdata/pakfindata.duckdb')": 'config.DUCKDB_PATH',
    'Path("/mnt/e/psxdata/tick_logs_cloud")': 'config.JSONL_CLOUD_DIR',
    'Path("/mnt/e/psxdata/tick_logs")': 'config.JSONL_LOCAL_DIR',
    'Path("/mnt/e/psxdata/intraday")': 'config.INTRADAY_DIR',
    'Path("/mnt/e/psxdata/downloads")': 'config.DOWNLOADS_DIR',
    'Path("/mnt/e/psxdata/sbp_easydata")': 'config.SBP_DIR',
}

def patch_file(filepath: Path):
    """Patch hardcoded paths in a single file."""
    content = filepath.read_text()
    original = content
    
    # Apply replacements
    for old, new in {**REPLACEMENTS, **PATH_REPLACEMENTS}.items():
        if old in content:
            content = content.replace(old, new)
    
    # Also handle f-string patterns like f"/mnt/e/psxdata/tick_logs_cloud/{date}.jsonl"
    content = re.sub(
        r'f"/mnt/e/psxdata/tick_logs_cloud/\{',
        'f"{config.JSONL_CLOUD_DIR}/',
        content
    )
    content = re.sub(
        r'f"/mnt/e/psxdata/tick_logs/\{',
        'f"{config.JSONL_LOCAL_DIR}/',
        content
    )
    content = re.sub(
        r'f"/mnt/e/psxdata/downloads/\{',
        'f"{config.DOWNLOADS_DIR}/',
        content
    )
    content = re.sub(
        r'f"/mnt/e/psxdata/intraday/\{',
        'f"{config.INTRADAY_DIR}/',
        content
    )
    
    if content != original:
        # Add config import if not already present
        if "from pakfindata" in content or "import pakfindata" in content:
            if "from pakfindata import config" not in content and "from pakfindata.config import" not in content:
                # Add import after existing pakfindata imports
                content = re.sub(
                    r'(from pakfindata\.\w+ import .+?\n)',
                    r'\1from pakfindata import config\n',
                    content,
                    count=1
                )
        elif "import " in content:
            # Add at top after other imports
            lines = content.split("\n")
            insert_idx = 0
            for i, line in enumerate(lines):
                if line.startswith("import ") or line.startswith("from "):
                    insert_idx = i + 1
            lines.insert(insert_idx, "from pakfindata import config")
            content = "\n".join(lines)
        
        filepath.write_text(content)
        return True
    return False


def main():
    patched = 0
    for py_file in SRC_DIR.rglob("*.py"):
        if "__pycache__" in str(py_file):
            continue
        if patch_file(py_file):
            patched += 1
            print(f"  ✅ Patched: {py_file.relative_to(SRC_DIR)}")
    
    print(f"\n  Patched {patched} files")


if __name__ == "__main__":
    main()
```

## Step 7: Add demo tooltips to key pages

Create a script that adds demo tooltips to the copied page files:

```python
"""
scripts/add_demo_tooltips.py — Add guided tooltips to demo pages.
"""

from pathlib import Path

SRC_DIR = Path.home() / "pakfindata-demo" / "src" / "pakfindata"

# Tooltips to inject at the top of each page's render function
TOOLTIPS = {
    "ui/page_views/live_market.py": {
        "feature": "Live Market",
        "tip": "Real-time market data for PSX equities. In demo mode, showing last trading day's closing data. Circuit breaker limits (Upper/Lower columns) highlight symbols near ±7.5% limits."
    },
    "ui/page_views/tick_analytics.py": {
        "feature": "Tick Analytics",
        "tip": "Quant-grade microstructure analysis from 5-second OHLCV bars. Select a date and symbol to see intraday patterns. Demo includes 3 days of tick data for top 50 symbols."
    },
    "ui/page_views/tick_replay.py": {
        "feature": "Tick Replay",
        "tip": "Play back any trading day tick-by-tick at variable speed. Uses TradingView Lightweight Charts at 60fps. Select a date → symbol → Load → Play. Try 25x-100x speed."
    },
    "ui/page_views/microstructure.py": {
        "feature": "Microstructure Analytics",
        "tip": "Bid-ask spread analysis, VPIN toxicity, volume profiles, trade size distribution, and order flow imbalance. Powered by cloud tick JSONL data with bid/ask fields."
    },
    "ui/page_views/futures.py": {
        "feature": "Derivatives Analytics",
        "tip": "Futures open interest from PSX DFC XLS files, basis analysis (premium/discount), OI buildup/unwind matrix, and rollover tracking. OI data comes from daily PSX downloads."
    },
    "ui/page_views/signal_dashboard.py": {
        "feature": "Signal Dashboard",
        "tip": "Multi-factor signal scoring across 500+ symbols. Composite score combines momentum, volume, VPIN, VAR margins, and more. Batch scanner runs against DuckDB for 104x speedup."
    },
    "ui/page_views/sector_analysis.py": {
        "feature": "Sector Analysis",
        "tip": "Treemap visualization sized by KSE100 index weights. Toggle between volume-weighted and index-weight views. Index weights from PSX constituent_data XLS files."
    },
    "ui/page_views/fund_explorer.py": {
        "feature": "Fund Explorer",
        "tip": "Mutual fund NAV tracking from MUFAP. Compare funds, see risk metrics, track performance. NAV data synced from MUFAP portal."
    },
    "ui/page_views/intraday.py": {
        "feature": "Intraday Analysis",
        "tip": "Intraday OHLCV from DPS timeseries API. Supports bar resampling (5s → 1m → 5m → 1h). KSE100 index overlay available. DuckDB sync buttons for fast analytics."
    },
}

def add_tooltips():
    for filepath, info in TOOLTIPS.items():
        full_path = SRC_DIR / filepath
        if not full_path.exists():
            print(f"  ⚠️ Not found: {filepath}")
            continue
        
        content = full_path.read_text()
        
        # Check if already has demo tooltip
        if "demo_tooltip" in content or "demo.demo_tooltip" in content:
            continue
        
        # Find the render function and add tooltip after title
        tooltip_code = f"""
    # ── Demo tooltip ──
    from pakfindata.demo import demo_tooltip, demo_badge, gate_live_feature
    demo_badge()
    demo_tooltip("{info['feature']}", "{info['tip']}")
"""
        
        # Insert after st.title or st.header (first occurrence)
        import re
        match = re.search(r'(st\.(title|header|markdown)\(.+?\))', content)
        if match:
            insert_pos = match.end()
            content = content[:insert_pos] + "\n" + tooltip_code + content[insert_pos:]
            full_path.write_text(content)
            print(f"  ✅ Tooltip added: {filepath}")
        else:
            print(f"  ⚠️ No title found: {filepath}")


if __name__ == "__main__":
    add_tooltips()
```

## Step 8: Gate live features in demo mode

```python
"""
scripts/gate_live_features.py — Disable live scraping/sync in demo mode.

Wraps sync buttons, scraper calls, and API fetches with gate_live_feature() check.
"""

from pathlib import Path
import re

SRC_DIR = Path.home() / "pakfindata-demo" / "src" / "pakfindata"

# Files that have live sync/scrape buttons
LIVE_FEATURES = [
    "sources/",          # All scrapers
    "services/",         # tick_service, etc.
]

def gate_sources():
    """Add demo gate to all source/service files."""
    for pattern in LIVE_FEATURES:
        dir_path = SRC_DIR / pattern
        if not dir_path.exists():
            continue
        
        for py_file in dir_path.glob("*.py"):
            if "__pycache__" in str(py_file):
                continue
            
            content = py_file.read_text()
            
            # Add demo mode check at the top of main execution
            if "if __name__" in content:
                content = content.replace(
                    'if __name__ == "__main__":',
                    '''if __name__ == "__main__":
    from pakfindata.config import DEMO_MODE
    if DEMO_MODE:
        print("⚠️ Live features disabled in demo mode")
        import sys; sys.exit(0)'''
                )
                py_file.write_text(content)
                print(f"  ✅ Gated: {py_file.name}")


if __name__ == "__main__":
    gate_sources()
```

## Step 9: Create Dockerfile

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY src/ ./src/
COPY .streamlit/ ./.streamlit/ 

# Copy demo data
COPY data/ ./data/

# Streamlit config
RUN mkdir -p ~/.streamlit && \
    echo '[server]\nheadless = true\nport = 8501\n\n[theme]\nprimaryColor = "#C8A96E"\nbackgroundColor = "#0B0E11"\nsecondaryBackgroundColor = "#141821"\ntextColor = "#E0E0E0"\nfont = "monospace"' > ~/.streamlit/config.toml

# Environment
ENV DEMO_MODE=1
ENV PSX_DATA_ROOT=/app/data
ENV PYTHONPATH=/app/src
ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

EXPOSE 8501

ENTRYPOINT ["streamlit", "run", "src/pakfindata/ui/app.py", \
            "--server.address=0.0.0.0", \
            "--server.port=8501", \
            "--browser.gatherUsageStatistics=false"]
```

## Step 10: Create docker-compose.yml

```yaml
version: '3.8'

services:
  pakfindata:
    build: .
    container_name: pakfindata-demo
    ports:
      - "8501:8501"
    environment:
      - DEMO_MODE=1
      - PSX_DATA_ROOT=/app/data
      # ── LLM Configuration (optional) ──
      # Uncomment ONE provider and set your API key:
      #
      # Option 1: OpenAI
      # - LLM_PROVIDER=openai
      # - LLM_API_KEY=sk-your-openai-key
      # - LLM_MODEL=gpt-4o-mini
      #
      # Option 2: Anthropic
      # - LLM_PROVIDER=anthropic
      # - LLM_API_KEY=sk-ant-your-anthropic-key
      # - LLM_MODEL=claude-sonnet-4-20250514
      #
      # Option 3: No LLM (default — AI commentary shows placeholder)
      - LLM_PROVIDER=none
    volumes:
      - ./data:/app/data:ro
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8501/_stcore/health"]
      interval: 30s
      timeout: 10s
      retries: 3
```

## Step 11: Create .dockerignore

```
__pycache__
*.pyc
.git
.github
*.egg-info
.eggs
dist
build
*.log
.mypy_cache
.pytest_cache
node_modules
.vscode
.idea
```

## Step 12: Create README.md

```markdown
# pakfindata Demo

Bloomberg Terminal-style analytics for Pakistan Stock Exchange (PSX).

## Quick Start

```bash
docker compose up --build
```

Open: http://localhost:8501

## Features

| Page | Description |
|------|-------------|
| Live Market | Real-time market data with circuit breaker limits |
| Tick Analytics | 5-second OHLCV bars, microstructure metrics |
| Tick Replay | 60fps client-side tick playback (TradingView charts) |
| Microstructure | Bid-ask spreads, VPIN, volume profiles, order flow |
| Derivatives | Futures OI, basis analysis, rollover tracker |
| Signal Dashboard | Multi-factor composite scoring, batch scanner |
| Sector Analysis | KSE100-weighted treemap |
| Fund Explorer | Mutual fund NAV tracking (MUFAP) |
| Intraday | DPS intraday bars with resampling |

## Demo Data

- Top 50 PSX symbols by volume
- 30 days of daily OHLCV
- 3 days of tick-level JSONL data
- Sample SBP EasyData (KIBOR, exchange rates, CPI)
- Sample PSX downloads (OI, circuit limits, index weights)

## AI Commentary (Optional)

Enable AI-powered market commentary by setting an API key:

Edit `docker-compose.yml`:

```yaml
environment:
  - LLM_PROVIDER=openai          # or "anthropic"
  - LLM_API_KEY=sk-your-key-here
  - LLM_MODEL=gpt-4o-mini        # or "claude-sonnet-4-20250514"
```

Without an API key, AI features show placeholder text.

## Architecture

```
DuckDB (pakfindata.duckdb)  → Fast analytics (104x vs SQLite)
SQLite (psx.sqlite)          → Reference data, configs
JSONL files                  → Tick-level market data
```

## Built by Godaitec (godai.tech)
```

## Step 13: Generate requirements.txt

```bash
cd ~/pakfindata
conda activate psx
pip freeze > ~/pakfindata-demo/requirements.txt

# Or manually create a minimal one:
cat > ~/pakfindata-demo/requirements.txt << 'EOF'
streamlit>=1.30.0
pandas>=2.0.0
numpy>=1.24.0
duckdb>=0.9.0
plotly>=5.15.0
openpyxl>=3.1.0
xlrd>=2.0.0
requests>=2.31.0
websockets>=12.0
openai>=1.0.0
anthropic>=0.20.0
EOF
```

## Step 14: Build sequence

Execute in this order:

```bash
cd ~/pakfindata-demo

# 1. Copy source code
cp -r ~/pakfindata/src/pakfindata src/
cp -r ~/pakfindata/.streamlit . 2>/dev/null || true

# 2. Create config.py and demo.py (from Step 3 and 4 above)

# 3. Patch hardcoded paths
python scripts/patch_paths.py

# 4. Add demo tooltips
python scripts/add_demo_tooltips.py

# 5. Gate live features
python scripts/gate_live_features.py

# 6. Create demo data
python scripts/create_demo_db.py

# 7. Generate requirements.txt
conda activate psx && pip freeze > requirements.txt

# 8. Build Docker image
docker compose build

# 9. Run
docker compose up

# 10. Open http://localhost:8501
```

## IMPORTANT NOTES

1. **NEVER modify ~/pakfindata/** — all changes in ~/pakfindata-demo/
2. **Demo data is READ-ONLY** — mounted as `:ro` in Docker
3. **No live scraping** — all sync/fetch buttons disabled in demo mode
4. **LLM is optional** — works without API key, shows placeholders
5. **LLM options:**
   - OpenAI: `gpt-4o-mini` (cheapest, ~$0.15/1M tokens)
   - Anthropic: `claude-sonnet-4-20250514` (better quality)
   - None: no AI commentary, everything else works
6. **Demo badge** — shows in sidebar on every page
7. **Tooltips** — expandable info boxes explaining each feature
8. **Size estimate:** Docker image ~500MB, demo data ~60MB
