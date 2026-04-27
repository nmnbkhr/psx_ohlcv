"""App Lineage — Auto-discovered interactive graph of pages, tabs, and DB tables.

Scans app.py and page_views/*.py source files at runtime to build the lineage.
Click 'Refresh Lineage' to pick up newly added pages/tabs/tables.
"""

import ast
import re
from pathlib import Path

import streamlit.components.v1 as components

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from pyvis.network import Network


# =============================================================================
# Source-code scanner — auto-discover pages, tabs, and tables
# =============================================================================

_UI_DIR = Path(__file__).resolve().parent          # page_views/
_APP_PY = _UI_DIR.parent / "app.py"                # ui/app.py

# Known DB tables to look for (from sqlite_master + repo schemas)
_KNOWN_TABLES: set[str] = {
    # Core trading
    "eod_ohlcv", "intraday_bars", "tick_logs", "tick_daily_summary",
    "symbols", "regular_market_current", "sector_map", "sectors",
    "instruments", "ohlcv_instrument", "index_constituents", "psx_indices",
    "post_close_turnover", "market_summary_tracking",
    # Futures / derivatives
    "futures_eod", "derivatives_eod", "contracts",
    # Company
    "company_profile", "company_overview", "company_fundamentals",
    "company_quotes", "company_payouts", "company_announcements",
    "corporate_announcements", "company_financials", "company_ratios",
    # Fixed income
    "fi_instruments", "fi_quotes", "bonds_master",
    "pkrv_daily", "pkisrv_daily", "kibor_daily", "konia_daily",
    "tbill_auctions", "pib_auctions",
    "sbp_policy_rates", "sbp_benchmark_snapshot", "global_rates",
    # FX
    "sbp_fx_interbank", "forex_kerb", "forex_ohlcv",
    # Funds
    "fund_performance", "fund_performance_latest",
    "mutual_funds", "etfs", "vps_funds",
    # Commodities
    "commodities", "pmex_ohlc", "pmex_margins",
    # ALM / FTP
    "alm_positions", "ftp_rates",
    # DuckDB tick/intraday
    "ohlcv_5s", "index_ohlcv_5s", "index_raw_ticks", "tick_data", "tick_ohlcv",
    # SBP
    "sbp_easydata",
    # System
    "downloaded_market_summary_dates",
}


# Strategy → data-source edges (tables the engine reads that may not appear
# in the page_view source directly).
_STRATEGY_EDGES: dict[str, dict] = {
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
        "reads": ["eod_ohlcv", "futures_eod"],
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
        "reads": ["eod_ohlcv", "sectors"],
        "engine": "engine/sector_rotation.py",
        "description": "Sector momentum ranking + rotation signals",
    },
    "ML Predictions": {
        "reads": ["eod_ohlcv", "tick_logs", "ohlcv_5s"],
        "engine": "engine/ml_model.py",
        "description": "XGBoost/LightGBM direction prediction",
    },
}


def _parse_nav_registry() -> tuple[dict[str, dict], dict[str, list[str]]]:
    """Parse app.py to extract page registry and nav_groups.

    Returns:
        (page_info, nav_groups)
        page_info:  {page_name: {"url": url_path, "func": func_name}}
        nav_groups: {group_name: [page_names]}
    """
    if not _APP_PY.exists():
        return {}, {}

    src = _APP_PY.read_text()

    # Extract page entries:  "Name": st.Page(func, title="...", url_path="...")
    page_info: dict[str, dict] = {}
    page_re = re.compile(
        r'"([^"]+)":\s*st\.Page\(\s*(\w+)\s*,\s*title="[^"]*"\s*,\s*url_path="([^"]*)"'
    )
    for m in page_re.finditer(src):
        name, func, url = m.group(1), m.group(2), m.group(3)
        page_info[name] = {"url": url, "func": func}

    # Extract nav_groups dict
    nav_groups: dict[str, list[str]] = {}
    # Find the nav_groups block
    ng_match = re.search(r'nav_groups\s*=\s*\{(.+?)\n\s*\}', src, re.DOTALL)
    if ng_match:
        block = ng_match.group(1)
        # Each group:  "GROUP_NAME":  ["Page1", "Page2", ...]
        grp_re = re.compile(r'"([^"]+)":\s*\[([^\]]+)\]')
        for gm in grp_re.finditer(block):
            grp_name = gm.group(1)
            items_str = gm.group(2)
            items = re.findall(r'"([^"]+)"', items_str)
            nav_groups[grp_name] = items

    return page_info, nav_groups


def _find_render_file(func_name: str) -> Path | None:
    """Given a page stub function name, find which page_views/*.py it imports from."""
    if not _APP_PY.exists():
        return None
    src = _APP_PY.read_text()
    # Pattern: def func_name():\n    from pakfindata.ui.page_views.X import Y\n    Y()
    pat = re.compile(
        rf'def {re.escape(func_name)}\(\):\s*\n'
        r'\s*from\s+pakfindata\.ui\.page_views\.(\w+)\s+import',
    )
    m = pat.search(src)
    if m:
        module = m.group(1)
        candidate = _UI_DIR / f"{module}.py"
        if candidate.exists():
            return candidate
    return None


def _scan_tabs(source: str) -> list[str]:
    """Extract tab labels from st.tabs([...]) calls in source code."""
    tabs: list[str] = []
    # Match st.tabs([...]) — may span multiple lines
    for m in re.finditer(r'st\.tabs\s*\(\s*\[([^\]]+)\]', source, re.DOTALL):
        inner = m.group(1)
        # Extract string literals, ignoring f-strings and emoji prefixes
        labels = re.findall(r'["\']([^"\']+)["\']', inner)
        tabs.extend(labels)
    return tabs


def _scan_tables(source: str, exclude_self: bool = False) -> list[str]:
    """Extract DB table names referenced in SQL or repository calls."""
    if exclude_self:
        return []
    found: set[str] = set()
    src_lower = source.lower()
    for tbl in _KNOWN_TABLES:
        # Look for table name in SQL context or as a string reference
        if tbl in src_lower:
            found.add(tbl)
    return sorted(found)


_ENGINE_DIR = _UI_DIR.parent / "engine"  # src/pakfindata/engine/


def _discover_engines() -> dict[str, dict]:
    """Auto-discover engine files, their DB table reads, and consuming pages.

    Returns:
        {engine_filename: {"path": str, "tables": [str], "functions": [str]}}
    """
    if not _ENGINE_DIR.exists():
        return {}

    engines: dict[str, dict] = {}
    for f in sorted(_ENGINE_DIR.glob("*.py")):
        if f.name == "__init__.py":
            continue
        content = f.read_text()

        # Find which known tables this engine references
        tables_used = sorted(
            tbl for tbl in _KNOWN_TABLES if tbl in content.lower()
        )

        # Extract top-level function names
        funcs: list[str] = []
        try:
            tree = ast.parse(content)
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef) and not node.name.startswith("_"):
                    funcs.append(node.name)
        except SyntaxError:
            pass

        engines[f.name] = {
            "path": f"engine/{f.name}",
            "tables": tables_used,
            "functions": funcs[:10],  # cap at 10
        }

    return engines


@st.cache_data(ttl=None, show_spinner="Scanning source files...")
def discover_lineage() -> dict[str, dict]:
    """Auto-discover all pages, their tabs, DB table dependencies, and engines.

    Scans app.py for page registry and nav_groups, then scans each
    page_views/*.py file for st.tabs() calls and DB table references.
    Also discovers engine files and maps page→engine relationships.
    """
    page_info, nav_groups = _parse_nav_registry()
    engines = _discover_engines()

    # Build reverse map: page_name -> group
    page_to_group: dict[str, str] = {}
    for grp, pages in nav_groups.items():
        for p in pages:
            page_to_group[p] = grp

    # Also check hidden pages (not in any group)
    for pname in page_info:
        if pname not in page_to_group:
            page_to_group[pname] = "HIDDEN"

    lineage: dict[str, dict] = {}
    for page_name, info in page_info.items():
        url = info["url"]
        func = info["func"]
        group = page_to_group.get(page_name, "HIDDEN")

        tabs: list[str] = []
        tables: list[str] = []

        # Find and scan the actual page_views file
        pv_file = _find_render_file(func)
        if pv_file and pv_file.exists():
            src = pv_file.read_text()
            tabs = _scan_tabs(src)
            is_self = pv_file.name == "app_lineage.py"
            tables = _scan_tables(src, exclude_self=is_self)

        # Merge strategy edges — add tables the engine reads but page doesn't reference
        strat = _STRATEGY_EDGES.get(page_name)
        if strat:
            for tbl in strat["reads"]:
                if tbl not in tables:
                    tables.append(tbl)

        # Auto-detect engine imports from page source
        page_engines: list[str] = []
        if strat and strat["engine"]:
            eng_file = strat["engine"].split("/")[-1]
            if eng_file in engines:
                page_engines.append(eng_file)
        # Also scan page source for `from pakfindata.engine.X import` patterns
        if pv_file and pv_file.exists():
            page_src = pv_file.read_text()
            for eng_name in engines:
                stem = eng_name.replace(".py", "")
                if f"engine.{stem}" in page_src or f"engine import {stem}" in page_src:
                    if eng_name not in page_engines:
                        page_engines.append(eng_name)

        lineage[page_name] = {
            "group": group,
            "url": url,
            "tabs": tabs,
            "tables": tables,
            "source_file": str(pv_file.relative_to(_UI_DIR)) if pv_file else None,
            "engine": strat["engine"] if strat else None,
            "engine_desc": strat["description"] if strat else None,
            "engines": page_engines,
        }

    # Attach engine metadata for use in graph/detail
    lineage["__engines__"] = engines  # type: ignore[assignment]

    return lineage


# =============================================================================
# Color palette
# =============================================================================

_GROUP_COLORS = {
    "MARKET OVERVIEW": "#1f77b4",
    "EQUITIES":        "#ff7f0e",
    "FIXED INCOME":    "#2ca02c",
    "ALM":             "#d62728",
    "FUNDS":           "#9467bd",
    "FX & RATES":      "#8c564b",
    "COMMODITIES":     "#e377c2",
    "RESEARCH":        "#7f7f7f",
    "STRATEGIES":      "#bcbd22",
    "ADMIN":           "#17becf",
    "HIDDEN":          "#444444",
}

_TAB_COLOR = "#555555"
_TABLE_COLOR = "#c44e52"
_ENGINE_COLOR = "#C8A96E"   # gold accent
_EXT_COLOR_PSX = "#FF6B6B"  # red — PSX sources
_EXT_COLOR_GOV = "#4ECDC4"  # teal — SBP/MUFAP/gov sources

# External data sources that feed into DB tables
_EXTERNAL_SOURCES: dict[str, dict] = {
    "PSX WebSocket": {
        "color": _EXT_COLOR_PSX,
        "feeds": ["tick_logs"],
        "description": "Real-time tick data via WebSocket relay",
    },
    "PSX DPS API": {
        "color": _EXT_COLOR_PSX,
        "feeds": ["ohlcv_5s", "index_ohlcv_5s", "intraday_bars"],
        "description": "5-second OHLCV bars via DPS timeseries",
    },
    "PSX Downloads": {
        "color": _EXT_COLOR_PSX,
        "feeds": ["eod_ohlcv", "futures_eod", "sectors", "instruments", "psx_indices"],
        "description": "Daily EOD, DFC XLS, sector data from PSX website",
    },
    "SBP EasyData": {
        "color": _EXT_COLOR_GOV,
        "feeds": ["kibor_daily", "konia_daily", "sbp_policy_rates", "sbp_fx_interbank", "pkrv_daily"],
        "description": "195 datasets via SBP EasyData API",
    },
    "MUFAP": {
        "color": _EXT_COLOR_GOV,
        "feeds": ["mutual_funds", "etfs"],
        "description": "Mutual fund NAV data via DrissionPage scraper",
    },
    "Forex.pk": {
        "color": _EXT_COLOR_GOV,
        "feeds": ["forex_kerb"],
        "description": "Kerb/open market FX rates",
    },
    "PMEX": {
        "color": _EXT_COLOR_GOV,
        "feeds": ["pmex_ohlc", "pmex_margins"],
        "description": "Commodity futures via JSON API + DrissionPage",
    },
}


# =============================================================================
# Build graph
# =============================================================================

def _build_pyvis_html(
    lineage: dict[str, dict],
    show_tabs: bool = True,
    show_tables: bool = True,
    show_engines: bool = False,
    show_sources: bool = False,
    show_freshness: bool = False,
    filter_group: str | None = None,
) -> str:
    """Build a pyvis Network and return its HTML string."""
    net = Network(
        height="700px",
        width="100%",
        bgcolor="#0e1117",
        font_color="#ffffff",
        directed=True,
        select_menu=False,
        filter_menu=False,
    )
    # Physics: stabilize then stop
    net.set_options("""{
        "physics": {
            "enabled": true,
            "barnesHut": {
                "gravitationalConstant": -3000,
                "centralGravity": 0.4,
                "springLength": 130,
                "springConstant": 0.04,
                "damping": 0.6
            },
            "maxVelocity": 25,
            "minVelocity": 1.5,
            "stabilization": {
                "enabled": true,
                "iterations": 250,
                "updateInterval": 25
            }
        },
        "interaction": {
            "hover": true,
            "tooltipDelay": 150,
            "navigationButtons": true
        },
        "edges": {
            "smooth": { "type": "continuous" }
        }
    }""")

    seen_tables: set[str] = set()
    seen_tabs: set[str] = set()
    seen_engines: set[str] = set()
    engines_meta: dict = lineage.get("__engines__", {})
    freshness_data: dict[str, dict] = _get_table_freshness_batch() if show_freshness else {}

    for page_name, info in lineage.items():
        if page_name == "__engines__":
            continue
        grp = info["group"]
        if filter_group and grp != filter_group:
            continue

        color = _GROUP_COLORS.get(grp, "#aaaaaa")
        tab_count = len(info["tabs"])
        tbl_count = len(info["tables"])
        src_file = info.get("source_file") or "?"

        tooltip = (
            f"[{grp}] {page_name}\n"
            f"Tabs: {tab_count} | Tables: {tbl_count}\n"
            f"URL: /{info['url']}\n"
            f"Source: {src_file}"
        )

        net.add_node(
            page_name, label=page_name, size=22, color=color,
            shape="dot", title=tooltip,
            font={"color": "#ffffff", "size": 13},
        )

        # Tabs
        if show_tabs:
            for tab in info["tabs"]:
                tab_id = f"{page_name}::{tab}"
                if tab_id not in seen_tabs:
                    seen_tabs.add(tab_id)
                    net.add_node(
                        tab_id, label=tab, size=10, color=_TAB_COLOR,
                        shape="diamond", title=f"Tab: {tab}\nPage: {page_name}",
                        font={"color": "#cccccc", "size": 10},
                    )
                net.add_edge(page_name, tab_id, color="#666666", width=1)

        # Tables
        if show_tables:
            for tbl in info["tables"]:
                if tbl not in seen_tables:
                    seen_tables.add(tbl)
                    td = freshness_data.get(tbl, {})
                    fr = td.get("freshness", "unknown") if show_freshness else "unknown"
                    border = _FRESHNESS_BORDER.get(fr, "#666666")
                    tooltip = f"DB Table: {tbl}"
                    if show_freshness and td:
                        tooltip += f"\nFreshness: {fr}"
                        if td.get("rows") is not None:
                            tooltip += f"\nRows: {td['rows']:,}"
                        if td.get("max_date"):
                            tooltip += f"\nMax date: {td['max_date']}"
                        if td.get("last_sync"):
                            tooltip += f"\nLast sync: {td['last_sync']}"
                        if td.get("source"):
                            tooltip += f"\nSource: {td['source']}"
                    net.add_node(
                        tbl, label=tbl, size=15,
                        color={"background": _TABLE_COLOR, "border": border},
                        shape="box", title=tooltip,
                        font={"color": "#ffffff", "size": 11},
                        borderWidth=3,
                    )
                net.add_edge(tbl, page_name, color="#884444", width=1.5)

        # Engines
        if show_engines:
            for eng_name in info.get("engines", []):
                eng_meta = engines_meta.get(eng_name, {})
                if eng_name not in seen_engines:
                    seen_engines.add(eng_name)
                    funcs = eng_meta.get("functions", [])
                    eng_tables = eng_meta.get("tables", [])
                    func_list = ", ".join(funcs[:5])
                    net.add_node(
                        eng_name, label=eng_name, size=16,
                        color=_ENGINE_COLOR, shape="hexagon",
                        title=f"Engine: {eng_meta.get('path', eng_name)}\nFunctions: {func_list}\nTables: {', '.join(eng_tables)}",
                        font={"color": "#ffffff", "size": 11},
                    )
                    if show_tables:
                        for etbl in eng_tables:
                            if etbl not in seen_tables:
                                seen_tables.add(etbl)
                                net.add_node(
                                    etbl, label=etbl, size=15,
                                    color=_TABLE_COLOR, shape="box",
                                    title=f"DB Table: {etbl}",
                                    font={"color": "#ffffff", "size": 11},
                                )
                            net.add_edge(etbl, eng_name, color="#8B7535", width=1.5)
                net.add_edge(eng_name, page_name, color=_ENGINE_COLOR, width=2)

    # External sources
    if show_sources:
        for src_name, src_info in _EXTERNAL_SOURCES.items():
            feeds = src_info["feeds"]
            connected = [t for t in feeds if t in seen_tables]
            if not connected and not show_tables:
                continue
            src_id = f"src::{src_name}"
            net.add_node(
                src_id, label=src_name, size=18,
                color=src_info["color"], shape="star",
                title=f"External: {src_name}\n{src_info['description']}\nFeeds: {', '.join(feeds)}",
                font={"color": "#ffffff", "size": 11},
            )
            for tbl in feeds:
                if tbl not in seen_tables:
                    seen_tables.add(tbl)
                    net.add_node(
                        tbl, label=tbl, size=15,
                        color=_TABLE_COLOR, shape="box",
                        title=f"DB Table: {tbl}",
                        font={"color": "#ffffff", "size": 11},
                    )
                net.add_edge(src_id, tbl, color=src_info["color"], width=2)

    return net.generate_html()


# =============================================================================
# DB table metadata (cached)
# =============================================================================

_PARQUET_PATH = "/mnt/e/psxdata/parquet"

# Map sync_runs tables → which data tables they feed
_SYNC_TABLE_MAP: dict[str, list[str]] = {
    "sync_runs":              ["eod_ohlcv", "symbols", "regular_market_current"],
    "instruments_sync_runs":  ["instruments", "ohlcv_instrument", "index_constituents"],
    "fx_sync_runs":           ["sbp_fx_interbank", "forex_kerb", "forex_ohlcv"],
    "mutual_fund_sync_runs":  ["mutual_funds", "etfs", "fund_performance", "fund_performance_latest", "vps_funds"],
    "bond_sync_runs":         ["fi_instruments", "fi_quotes", "bonds_master"],
    "sukuk_sync_runs":        ["fi_instruments", "fi_quotes"],
    "fi_sync_runs":           ["kibor_daily", "konia_daily", "sbp_policy_rates", "pkrv_daily",
                                "pkisrv_daily", "tbill_auctions", "pib_auctions", "sbp_benchmark_snapshot"],
    "commodity_sync_runs":    ["commodities", "pmex_ohlc", "pmex_margins"],
}


def _classify_freshness(max_date_str: str | None) -> str:
    """Classify a date string into fresh/stale/old/unknown."""
    from datetime import datetime
    if not max_date_str:
        return "unknown"
    try:
        max_str = str(max_date_str)[:10]
        max_date = datetime.strptime(max_str, "%Y-%m-%d").date()
        days_old = (datetime.now().date() - max_date).days
        if days_old <= 1:
            return "fresh"
        elif days_old <= 3:
            return "stale"
        return "old"
    except Exception:
        return "unknown"


@st.cache_data(ttl=300, show_spinner="Checking data freshness...")
def _get_table_freshness_batch() -> dict[str, dict]:
    """Batch freshness from SQLite tables, DuckDB tables, and sync run logs.

    Returns {table_name: {
        "freshness": "fresh"|"stale"|"old"|"unknown",
        "max_date": "2026-03-25" or None,
        "rows": int or None,
        "source": "sqlite"|"duckdb"|"sync_log",
        "last_sync": "2026-03-25 17:27:37" or None,
    }}
    """
    import sqlite3

    from pakfindata.config import get_db_path

    result: dict[str, dict] = {}
    for tbl in _KNOWN_TABLES:
        result[tbl] = {"freshness": "unknown", "max_date": None, "rows": None,
                        "source": None, "last_sync": None}

    # --- 1. SQLite: MAX(date) per table ---
    try:
        con = sqlite3.connect(str(get_db_path()), timeout=3)
        con.execute("PRAGMA journal_mode=WAL")
        existing = {
            r[0] for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }

        for tbl in _KNOWN_TABLES:
            if tbl not in existing:
                continue
            # Get row count first
            try:
                cnt = con.execute(f'SELECT COUNT(*) FROM "{tbl}"').fetchone()
                if cnt:
                    result[tbl]["rows"] = cnt[0]
                    result[tbl]["source"] = "sqlite"
            except Exception:
                pass
            # Try date columns for freshness
            for dcol in ("date", "ts", "ingested_at"):
                try:
                    row = con.execute(
                        f'SELECT MAX("{dcol}") FROM "{tbl}" WHERE "{dcol}" IS NOT NULL'
                    ).fetchone()
                    if row and row[0] and len(str(row[0])) >= 10 and str(row[0])[:4].isdigit():
                        result[tbl]["max_date"] = str(row[0])[:10]
                        result[tbl]["freshness"] = _classify_freshness(str(row[0]))
                        result[tbl]["source"] = "sqlite"
                        break
                except Exception:
                    continue

        # --- 2. Sync run logs → enrich with last_sync timestamps ---
        for sync_tbl, data_tables in _SYNC_TABLE_MAP.items():
            if sync_tbl not in existing:
                continue
            try:
                row = con.execute(
                    f'SELECT ended_at, rows_upserted FROM "{sync_tbl}" '
                    f'ORDER BY ended_at DESC LIMIT 1'
                ).fetchone()
                if row and row[0]:
                    for dt in data_tables:
                        if dt in result:
                            result[dt]["last_sync"] = str(row[0])[:19]
                            # If table had no date column, use sync time for freshness
                            if result[dt]["freshness"] == "unknown":
                                result[dt]["freshness"] = _classify_freshness(str(row[0]))
                                result[dt]["source"] = "sync_log"
            except Exception:
                continue

        con.close()
    except Exception:
        pass

    # --- 3. DuckDB: for tick-level tables that only live in DuckDB ---
    try:
        from pakfindata.db.connections import _duck_con
        dcon = _duck_con()
        duck_tables = {r[0] for r in dcon.execute("SHOW TABLES").fetchall()}

        for tbl in _KNOWN_TABLES:
            if tbl not in duck_tables:
                continue
            # Skip if SQLite already has good data
            if result[tbl]["freshness"] in ("fresh", "stale"):
                continue
            try:
                # Try date column first
                row = dcon.execute(
                    f'SELECT COUNT(*), MAX(date) FROM "{tbl}"'
                ).fetchone()
                if row and row[1]:
                    result[tbl]["rows"] = row[0]
                    result[tbl]["max_date"] = str(row[1])[:10]
                    result[tbl]["freshness"] = _classify_freshness(str(row[1]))
                    result[tbl]["source"] = "duckdb"
                    continue
            except Exception:
                pass
            # Fallback: just count
            try:
                row = dcon.execute(f'SELECT COUNT(*) FROM "{tbl}"').fetchone()
                if row:
                    result[tbl]["rows"] = row[0]
                    result[tbl]["source"] = "duckdb"
            except Exception:
                pass

    except Exception:
        pass

    return result


_FRESHNESS_BORDER = {
    "fresh": "#22c55e",    # green
    "stale": "#eab308",    # yellow
    "old": "#ef4444",      # red
    "unknown": "#666666",  # gray
}


# =============================================================================
# Detail panel (shown below graph via expander, since pyvis click events
# don't communicate back to Streamlit)
# =============================================================================

def _render_detail_table(lineage: dict[str, dict]):
    """Render a searchable detail panel — select a page or table to inspect."""
    page_names = sorted(k for k in lineage if k != "__engines__")
    all_tables = sorted({t for k, v in lineage.items() if k != "__engines__" for t in v.get("tables", [])})

    col_p, col_t = st.columns(2)
    with col_p:
        sel_page = st.selectbox("Inspect page", [""] + page_names, index=0, key="detail_page")
    with col_t:
        sel_table = st.selectbox("Inspect table", [""] + all_tables, index=0, key="detail_table")

    if sel_page and sel_page in lineage:
        info = lineage[sel_page]
        st.subheader(sel_page)
        st.caption(f"Group: **{info['group']}** | URL: `/{info['url']}`")
        if info.get("source_file"):
            st.caption(f"Source: `page_views/{info['source_file']}`")

        c1, c2 = st.columns(2)
        with c1:
            if info["tabs"]:
                st.markdown("**Tabs:**")
                for t in info["tabs"]:
                    st.markdown(f"- {t}")
            else:
                st.markdown("*No tabs*")
        with c2:
            if info["tables"]:
                st.markdown("**DB Tables:**")
                for t in info["tables"]:
                    st.markdown(f"- `{t}`")
            else:
                st.markdown("*No DB tables*")

        if info.get("engine"):
            st.markdown(f"**Engine:** `{info['engine']}`")

        # Shared tables
        if info["tables"]:
            shared: dict[str, list[str]] = {}
            for tbl in info["tables"]:
                for other, oi in lineage.items():
                    if other not in ("__engines__", sel_page) and tbl in oi.get("tables", []):
                        shared.setdefault(tbl, []).append(other)
            if shared:
                st.markdown("**Shared table usage:**")
                for tbl, pages in shared.items():
                    st.markdown(f"- `{tbl}` — {', '.join(pages)}")

    if sel_table:
        st.subheader(f"Table: {sel_table}")
        users = [p for p, info in lineage.items() if p != "__engines__" and sel_table in info.get("tables", [])]
        st.markdown(f"**Used by {len(users)} page(s):**")
        for p in users:
            st.markdown(f"- **{p}** ({lineage[p]['group']})")


# =============================================================================
# Sankey data-flow view
# =============================================================================

def _build_sankey(lineage: dict[str, dict], filter_group: str | None = None):
    """Build a Plotly Sankey diagram showing the 4-layer data flow.

    Layers (left → right):
        External Sources → DB Tables → Engines → Pages
    """
    engines_meta: dict = lineage.get("__engines__", {})

    # Collect unique labels per layer
    sources_set: set[str] = set()
    tables_set: set[str] = set()
    engines_set: set[str] = set()
    pages_set: set[str] = set()

    # Pages (filtered)
    for pname, info in lineage.items():
        if pname == "__engines__":
            continue
        if filter_group and info["group"] != filter_group:
            continue
        pages_set.add(pname)
        for tbl in info.get("tables", []):
            tables_set.add(tbl)
        for eng in info.get("engines", []):
            engines_set.add(eng)

    # External sources that feed visible tables
    for sname, sinfo in _EXTERNAL_SOURCES.items():
        if any(t in tables_set for t in sinfo["feeds"]):
            sources_set.add(sname)

    # Engine tables
    for ename in engines_set:
        for tbl in engines_meta.get(ename, {}).get("tables", []):
            tables_set.add(tbl)

    # Build node list (order: sources, tables, engines, pages)
    all_labels = (
        sorted(sources_set) + sorted(tables_set) +
        sorted(engines_set) + sorted(pages_set)
    )
    idx = {label: i for i, label in enumerate(all_labels)}

    # Assign layer colors
    node_colors = []
    for label in all_labels:
        if label in sources_set:
            node_colors.append(_EXTERNAL_SOURCES.get(label, {}).get("color", _EXT_COLOR_PSX))
        elif label in tables_set:
            node_colors.append(_TABLE_COLOR)
        elif label in engines_set:
            node_colors.append(_ENGINE_COLOR)
        else:
            grp = lineage.get(label, {}).get("group", "HIDDEN")
            node_colors.append(_GROUP_COLORS.get(grp, "#aaaaaa"))

    # Layer x-positions
    node_x, node_y = [], []
    layer_items = [sorted(sources_set), sorted(tables_set), sorted(engines_set), sorted(pages_set)]
    x_positions = [0.01, 0.30, 0.60, 0.99]
    for layer_idx, items in enumerate(layer_items):
        n = len(items)
        for i, item in enumerate(items):
            node_x.append(x_positions[layer_idx])
            node_y.append((i + 1) / (n + 1) if n > 0 else 0.5)

    # Build links
    link_src, link_tgt, link_val, link_color = [], [], [], []

    def _add_link(src_label, tgt_label, color="rgba(150,150,150,0.3)"):
        if src_label in idx and tgt_label in idx:
            link_src.append(idx[src_label])
            link_tgt.append(idx[tgt_label])
            link_val.append(1)
            link_color.append(color)

    # Source → Table
    for sname, sinfo in _EXTERNAL_SOURCES.items():
        if sname not in sources_set:
            continue
        c = sinfo["color"].replace(")", ",0.3)").replace("rgb", "rgba") if "rgb" in sinfo["color"] else f"rgba(255,107,107,0.3)"
        for tbl in sinfo["feeds"]:
            if tbl in tables_set:
                _add_link(sname, tbl, c)

    # Table → Engine
    for ename in engines_set:
        for tbl in engines_meta.get(ename, {}).get("tables", []):
            if tbl in tables_set:
                _add_link(tbl, ename, "rgba(200,169,110,0.35)")

    # Engine → Page
    for pname in pages_set:
        for eng in lineage.get(pname, {}).get("engines", []):
            if eng in engines_set:
                _add_link(eng, pname, "rgba(200,169,110,0.35)")

    # Table → Page (direct, no engine)
    for pname in pages_set:
        page_engines = lineage.get(pname, {}).get("engines", [])
        engine_tables = set()
        for eng in page_engines:
            engine_tables.update(engines_meta.get(eng, {}).get("tables", []))
        for tbl in lineage.get(pname, {}).get("tables", []):
            if tbl in tables_set and tbl not in engine_tables:
                _add_link(tbl, pname, "rgba(196,78,82,0.25)")

    fig = go.Figure(go.Sankey(
        arrangement="snap",
        node=dict(
            pad=12,
            thickness=18,
            label=all_labels,
            color=node_colors,
            x=node_x,
            y=node_y,
            line=dict(color="#333", width=0.5),
        ),
        link=dict(
            source=link_src,
            target=link_tgt,
            value=link_val,
            color=link_color,
        ),
    ))
    fig.update_layout(
        height=max(500, len(all_labels) * 14),
        margin=dict(l=10, r=10, t=30, b=10),
        paper_bgcolor="#0e1117",
        font=dict(color="#E0E0E0", size=11),
        title=dict(
            text="External Sources → DB Tables → Engines → Pages",
            font=dict(size=13, color="#888"),
        ),
    )
    return fig


# =============================================================================
# Main render
# =============================================================================

def render_app_lineage():
    """Render the App Lineage admin page."""
    st.title("App Lineage")
    st.caption("Auto-discovered dependency graph — pages, tabs, and DB tables")

    # Refresh button
    if st.button("Refresh Lineage", help="Re-scan source files for new pages, tabs, and tables"):
        discover_lineage.clear()
        st.rerun()

    lineage = discover_lineage()

    # Stats
    all_groups = sorted({v["group"] for k, v in lineage.items() if k != "__engines__"})
    all_tables: set[str] = set()
    total_tabs = 0
    for k, v in lineage.items():
        if k == "__engines__":
            continue
        all_tables.update(v["tables"])
        total_tabs += len(v["tabs"])

    engines_meta: dict = lineage.get("__engines__", {})
    engine_count = len(engines_meta)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Pages", len(lineage) - (1 if "__engines__" in lineage else 0))
    c2.metric("Tabs", total_tabs)
    c3.metric("DB Tables", len(all_tables))
    c4.metric("Engines", engine_count)
    c5.metric("Nav Groups", len(all_groups))

    st.divider()

    # Controls
    col_f, col_t, col_tb, col_e, col_s, col_fr = st.columns(6)
    with col_f:
        groups = ["All"] + all_groups
        filter_group = st.selectbox("Filter by group", groups, index=0)
    with col_t:
        show_tabs = st.checkbox("Show tabs", value=False)
    with col_tb:
        show_tables = st.checkbox("Show DB tables", value=True)
    with col_e:
        show_engines = st.checkbox("Show engines", value=False)
    with col_s:
        show_sources = st.checkbox("Show sources", value=False)
    with col_fr:
        show_freshness = st.checkbox("Freshness", value=False, help="Color table borders by data freshness (slower)")

    grp = None if filter_group == "All" else filter_group

    # View mode toggle
    view_mode = st.radio("View", ["Force Graph", "Data Flow"], horizontal=True, key="lineage_view_mode")

    if view_mode == "Force Graph":
        html = _build_pyvis_html(
            lineage, show_tabs=show_tabs, show_tables=show_tables,
            show_engines=show_engines, show_sources=show_sources,
            show_freshness=show_freshness, filter_group=grp,
        )
        components.html(html, height=720, scrolling=False)
    else:
        # Sankey data flow view
        fig = _build_sankey(lineage, filter_group=grp)
        st.plotly_chart(fig, width='stretch')

    # Detail inspector
    with st.expander("Inspect Page / Table"):
        _render_detail_table(lineage)

    # Data freshness log
    with st.expander("Data Freshness Log"):
        fd = _get_table_freshness_batch()
        _emoji = {"fresh": "🟢", "stale": "🟡", "old": "🔴", "unknown": "⚪"}
        rows = []
        for tbl in sorted(fd.keys()):
            d = fd[tbl]
            rows.append({
                "Status": _emoji.get(d["freshness"], "⚪"),
                "Table": tbl,
                "Freshness": d["freshness"],
                "Max Date": d.get("max_date") or "-",
                "Rows": f"{d['rows']:,}" if d.get("rows") is not None else "-",
                "Last Sync": d.get("last_sync") or "-",
                "Source DB": d.get("source") or "-",
            })
        st.dataframe(pd.DataFrame(rows), width='stretch', hide_index=True, height=400)

        # Summary
        fresh = sum(1 for d in fd.values() if d["freshness"] == "fresh")
        stale = sum(1 for d in fd.values() if d["freshness"] == "stale")
        old = sum(1 for d in fd.values() if d["freshness"] == "old")
        unknown = sum(1 for d in fd.values() if d["freshness"] == "unknown")
        st.caption(
            f"🟢 Fresh (today): {fresh} | 🟡 Stale (1-3d): {stale} | "
            f"🔴 Old (4d+): {old} | ⚪ Unknown: {unknown}"
        )

    # Legend
    with st.expander("Legend"):
        for grp_name in all_groups:
            color = _GROUP_COLORS.get(grp_name, "#aaaaaa")
            st.markdown(
                f'<span style="display:inline-block;width:14px;height:14px;'
                f'background:{color};border-radius:50%;margin-right:6px;'
                f'vertical-align:middle;"></span> {grp_name}',
                unsafe_allow_html=True,
            )
        st.markdown(
            f'<span style="display:inline-block;width:14px;height:14px;'
            f'background:{_TABLE_COLOR};margin-right:6px;'
            f'vertical-align:middle;"></span> DB Table (box)',
            unsafe_allow_html=True,
        )
        st.markdown("&nbsp;&nbsp;&nbsp;Table border = freshness:", unsafe_allow_html=True)
        for fname, fcolor in _FRESHNESS_BORDER.items():
            label = {"fresh": "updated today", "stale": "1-3 days old",
                     "old": "4+ days old", "unknown": "unknown"}[fname]
            st.markdown(
                f'&nbsp;&nbsp;&nbsp;&nbsp;<span style="display:inline-block;width:12px;height:12px;'
                f'background:{_TABLE_COLOR};border:3px solid {fcolor};margin-right:6px;'
                f'vertical-align:middle;"></span> {label}',
                unsafe_allow_html=True,
            )
        st.markdown(
            f'<span style="display:inline-block;width:14px;height:14px;'
            f'background:{_ENGINE_COLOR};clip-path:polygon(50% 0%,100% 25%,100% 75%,50% 100%,0% 75%,0% 25%);'
            f'margin-right:6px;vertical-align:middle;"></span> Engine (hexagon)',
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<span style="display:inline-block;width:14px;height:14px;'
            f'background:{_EXT_COLOR_PSX};clip-path:polygon(50% 0%,61% 35%,98% 35%,68% 57%,79% 91%,50% 70%,21% 91%,32% 57%,2% 35%,39% 35%);'
            f'margin-right:6px;vertical-align:middle;"></span> External Source — PSX (star)',
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<span style="display:inline-block;width:14px;height:14px;'
            f'background:{_EXT_COLOR_GOV};clip-path:polygon(50% 0%,61% 35%,98% 35%,68% 57%,79% 91%,50% 70%,21% 91%,32% 57%,2% 35%,39% 35%);'
            f'margin-right:6px;vertical-align:middle;"></span> External Source — Gov/Other (star)',
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<span style="display:inline-block;width:14px;height:14px;'
            f'background:{_TAB_COLOR};transform:rotate(45deg);margin-right:6px;'
            f'vertical-align:middle;"></span> Tab (diamond)',
            unsafe_allow_html=True,
        )

    # Full listing
    with st.expander("Full page listing"):
        rows = []
        for page_name, info in lineage.items():
            if page_name == "__engines__":
                continue
            rows.append({
                "Page": page_name,
                "Group": info["group"],
                "URL": f"/{info['url']}",
                "Source": info.get("source_file") or "-",
                "Tabs": len(info["tabs"]),
                "Tables": len(info["tables"]),
                "Engines": ", ".join(info.get("engines", [])) or "-",
                "Tab Names": ", ".join(info["tabs"]) if info["tabs"] else "-",
                "Table Names": ", ".join(info["tables"]) if info["tables"] else "-",
            })
        st.dataframe(pd.DataFrame(rows), width='stretch', hide_index=True)

    # ─── Strategy Pipeline ───────────────────────────────────────────────
    with st.expander("Strategy Pipeline"):
        st.caption(
            "How pakfindata strategies connect: Allocation decides regime & sector, "
            "Signal finds entry timing, Execution optimizes fills."
        )

        _PIPELINE = {
            "ALLOCATION": {
                "color": "#3b82f6",
                "strategies": [
                    ("Macro Regime", "macro_regime_hmm.py", "WHICH regime — cross-asset HMM"),
                    ("Sector Rotation", "sector_rotation.py", "WHICH sectors — momentum ranking"),
                    ("ML Predictions", "ml_model.py", "WHICH stocks — XGBoost direction"),
                ],
            },
            "SIGNAL": {
                "color": "#22c55e",
                "strategies": [
                    ("VPIN Strategy", "vpin_strategy.py", "WHEN to trade — toxicity + Hurst regime"),
                    ("OFI Alpha", "ofi_strategy.py", "WHICH direction — order flow imbalance"),
                    ("CVD Divergence", "cvd_strategy.py", "WHERE reversals — cumulative volume delta"),
                ],
            },
            "EXECUTION": {
                "color": "#eab308",
                "strategies": [
                    ("Basis Arb", "basis_strategy.py", "Market-neutral — futures basis mean-reversion"),
                    ("VWAP Execution", "vwap_execution.py", "HOW to execute — volume profile + VWAP"),
                ],
            },
        }

        cols = st.columns(len(_PIPELINE))
        for col, (layer_name, layer_info) in zip(cols, _PIPELINE.items()):
            with col:
                st.markdown(
                    f'<div style="text-align:center;padding:6px 0;margin-bottom:8px;'
                    f'background:{layer_info["color"]}22;border:1px solid {layer_info["color"]};'
                    f'border-radius:6px;"><b style="color:{layer_info["color"]}">'
                    f'{layer_name}</b></div>',
                    unsafe_allow_html=True,
                )
                for sname, engine, desc in layer_info["strategies"]:
                    st.markdown(
                        f'<div style="background:#1a1d23;border-left:3px solid {layer_info["color"]};'
                        f'padding:8px 10px;margin-bottom:6px;border-radius:4px;">'
                        f'<b style="color:#E0E0E0">{sname}</b><br>'
                        f'<span style="color:#888;font-size:0.82em">{desc}</span><br>'
                        f'<code style="color:{_ENGINE_COLOR};font-size:0.78em">{engine}</code>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )

        # Flow arrows between layers
        st.markdown(
            '<div style="text-align:center;color:#666;font-size:0.85em;margin-top:8px;">'
            'Allocation (what & when) → Signal (entry trigger) → Execution (how to fill)'
            '</div>',
            unsafe_allow_html=True,
        )
