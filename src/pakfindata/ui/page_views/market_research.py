"""Market Research — Holistic market analysis with LLM commentary.

Provides a top-down market view (NOT symbol-specific):
  1) Market Snapshot — KSE-100/30, breadth, volume
  2) Sector Heatmap — sector performance ranking
  3) Top Picks — best stocks to trade (momentum) and invest (value)
  4) Macro Dashboard — policy rate, KIBOR, FX, yield curve
  5) AI Market Commentary — LLM-generated holistic analysis
"""

from __future__ import annotations

import os
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from pakfindata.ui.components.helpers import (
    get_connection,
    render_footer,
    format_volume,
)


# ═══════════════════════════════════════════════════════════════════════════
# DESIGN CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════

_C = {
    "up": "#00E676", "down": "#FF5252", "neutral": "#78909C",
    "accent": "#00D4AA", "gold": "#FFD600",
    "bg": "#0e1117", "card": "#1a1a2e", "grid": "#2d2d3d",
    "text": "#e0e0e0", "dim": "#888888",
}

_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color=_C["text"], size=11, family="JetBrains Mono, monospace"),
    xaxis=dict(gridcolor=_C["grid"], zeroline=False),
    yaxis=dict(gridcolor=_C["grid"], zeroline=False),
    legend=dict(bgcolor="rgba(0,0,0,0)"),
    margin=dict(l=40, r=20, t=40, b=30),
)


# ═══════════════════════════════════════════════════════════════════════════
# DATA LOADERS (all queries, no rendering)
# ═══════════════════════════════════════════════════════════════════════════

def _load_indices(con) -> pd.DataFrame:
    """Latest row per index code."""
    try:
        return pd.read_sql_query("""
            SELECT p.* FROM psx_indices p
            INNER JOIN (
                SELECT index_code, MAX(index_date) AS md
                FROM psx_indices GROUP BY index_code
            ) latest ON p.index_code = latest.index_code AND p.index_date = latest.md
            ORDER BY CASE p.index_code
                WHEN 'KSE100' THEN 1 WHEN 'KSE30' THEN 2
                WHEN 'KMI30' THEN 3 ELSE 4 END
        """, con)
    except Exception:
        return pd.DataFrame()


def _load_index_history(con, code: str = "KSE100", days: int = 60) -> pd.DataFrame:
    try:
        return pd.read_sql_query("""
            SELECT DISTINCT index_date, value, change_pct, volume
            FROM psx_indices WHERE index_code = ?
            ORDER BY index_date DESC LIMIT ?
        """, con, params=[code, days])
    except Exception:
        return pd.DataFrame()


def _load_breadth(con) -> dict:
    """Advance/decline from latest EOD data."""
    try:
        row = con.execute("""
            SELECT e.date AS session_date,
                SUM(CASE WHEN e.close > e.prev_close THEN 1
                         WHEN e.close > e.open THEN 1 ELSE 0 END) AS adv,
                SUM(CASE WHEN e.close < e.prev_close THEN 1
                         WHEN e.close < e.open THEN 1 ELSE 0 END) AS dec,
                SUM(CASE WHEN e.close = COALESCE(e.prev_close, e.open) THEN 1 ELSE 0 END) AS unch,
                COUNT(*) AS total,
                SUM(e.turnover) AS total_turnover,
                SUM(e.volume) AS total_volume
            FROM eod_ohlcv e
            WHERE e.date = (SELECT MAX(date) FROM eod_ohlcv)
        """).fetchone()
        return dict(row) if row else {}
    except Exception:
        return {}


def _load_sector_performance(con) -> pd.DataFrame:
    """Sector-level aggregates from latest EOD + sectors lookup."""
    try:
        return pd.read_sql_query("""
            SELECT
                s.sector_name AS sector,
                COUNT(*) AS stocks,
                ROUND(AVG(
                    CASE WHEN e.prev_close > 0 THEN (e.close - e.prev_close) / e.prev_close * 100
                         WHEN e.open > 0 THEN (e.close - e.open) / e.open * 100
                         ELSE NULL END
                ), 2) AS avg_chg,
                ROUND(SUM(e.turnover) / 1e6, 1) AS turnover_m,
                SUM(CASE WHEN e.close > COALESCE(e.prev_close, e.open) THEN 1 ELSE 0 END) AS adv,
                SUM(CASE WHEN e.close < COALESCE(e.prev_close, e.open) THEN 1 ELSE 0 END) AS dec
            FROM eod_ohlcv e
            JOIN sectors s ON '0' || e.sector_code = s.sector_code
            WHERE e.date = (SELECT MAX(date) FROM eod_ohlcv)
              AND e.close > 0
            GROUP BY s.sector_name
            HAVING COUNT(*) >= 2
            ORDER BY avg_chg DESC
        """, con)
    except Exception:
        return pd.DataFrame()


def _load_momentum_picks(con, n: int = 15) -> pd.DataFrame:
    """Top momentum stocks — highest daily change % with decent volume."""
    try:
        return pd.read_sql_query("""
            SELECT e.symbol,
                   e.close,
                   ROUND(CASE WHEN e.prev_close > 0 THEN (e.close - e.prev_close) / e.prev_close * 100
                              WHEN e.open > 0 THEN (e.close - e.open) / e.open * 100
                              ELSE NULL END, 2) AS change_pct,
                   e.volume,
                   e.turnover,
                   ts.ytd_change,
                   ts.pe_ratio_ttm,
                   s.sector_name
            FROM eod_ohlcv e
            LEFT JOIN sectors s ON '0' || e.sector_code = s.sector_code
            LEFT JOIN trading_sessions ts
                ON e.symbol = ts.symbol AND ts.market_type = 'REG'
                AND ts.session_date = (SELECT MAX(session_date) FROM trading_sessions WHERE market_type='REG')
            WHERE e.date = (SELECT MAX(date) FROM eod_ohlcv)
              AND e.volume > 50000
              AND e.close > 0
            ORDER BY change_pct DESC
            LIMIT ?
        """, con, params=[n])
    except Exception:
        return pd.DataFrame()


def _load_value_picks(con, n: int = 15) -> pd.DataFrame:
    """Value / investment picks — low P/E, decent liquidity."""
    try:
        return pd.read_sql_query("""
            SELECT e.symbol,
                   e.close,
                   ROUND(CASE WHEN e.prev_close > 0 THEN (e.close - e.prev_close) / e.prev_close * 100
                              WHEN e.open > 0 THEN (e.close - e.open) / e.open * 100
                              ELSE NULL END, 2) AS change_pct,
                   e.volume,
                   ts.pe_ratio_ttm,
                   ts.ytd_change,
                   ts.year_1_change,
                   s.sector_name
            FROM eod_ohlcv e
            LEFT JOIN sectors s ON '0' || e.sector_code = s.sector_code
            LEFT JOIN trading_sessions ts
                ON e.symbol = ts.symbol AND ts.market_type = 'REG'
                AND ts.session_date = (SELECT MAX(session_date) FROM trading_sessions WHERE market_type='REG')
            WHERE e.date = (SELECT MAX(date) FROM eod_ohlcv)
              AND ts.pe_ratio_ttm > 0 AND ts.pe_ratio_ttm < 15
              AND e.volume > 10000
              AND e.close > 5
            ORDER BY ts.pe_ratio_ttm ASC
            LIMIT ?
        """, con, params=[n])
    except Exception:
        return pd.DataFrame()


def _load_volume_leaders(con, n: int = 10) -> pd.DataFrame:
    """Highest volume traded stocks."""
    try:
        return pd.read_sql_query("""
            SELECT e.symbol,
                   e.close,
                   ROUND(CASE WHEN e.prev_close > 0 THEN (e.close - e.prev_close) / e.prev_close * 100
                              WHEN e.open > 0 THEN (e.close - e.open) / e.open * 100
                              ELSE NULL END, 2) AS change_pct,
                   e.volume,
                   e.turnover,
                   s.sector_name
            FROM eod_ohlcv e
            LEFT JOIN sectors s ON '0' || e.sector_code = s.sector_code
            WHERE e.date = (SELECT MAX(date) FROM eod_ohlcv)
              AND e.volume > 0
            ORDER BY e.volume DESC
            LIMIT ?
        """, con, params=[n])
    except Exception:
        return pd.DataFrame()


def _load_macro_snapshot(con) -> dict:
    """Policy rate, KIBOR, KONIA, FX, PKRV."""
    data: dict = {}
    # Policy rate
    try:
        row = con.execute("SELECT policy_rate, rate_date FROM sbp_policy_rates ORDER BY rate_date DESC LIMIT 1").fetchone()
        if row:
            data["policy_rate"] = row["policy_rate"]
            data["policy_date"] = row["rate_date"]
    except Exception:
        pass
    # KIBOR 6M offer
    try:
        row = con.execute("SELECT offer, date FROM kibor_daily WHERE tenor='6M' ORDER BY date DESC LIMIT 1").fetchone()
        if row:
            data["kibor_6m"] = row["offer"]
            data["kibor_date"] = row["date"]
    except Exception:
        pass
    # KONIA
    try:
        row = con.execute("SELECT rate, date FROM konia_daily ORDER BY date DESC LIMIT 1").fetchone()
        if row:
            data["konia"] = row["rate"]
            data["konia_date"] = row["date"]
    except Exception:
        pass
    # USD/PKR interbank
    try:
        row = con.execute("""
            SELECT selling, date FROM sbp_fx_interbank
            WHERE UPPER(currency) = 'USD'
            ORDER BY date DESC LIMIT 1
        """).fetchone()
        if row:
            data["usd_pkr"] = row["selling"]
            data["fx_date"] = row["date"]
    except Exception:
        pass
    # PKRV short end (3M)
    try:
        row = con.execute("""
            SELECT yield_pct, date FROM pkrv_daily
            WHERE tenor_months = 3
            ORDER BY date DESC LIMIT 1
        """).fetchone()
        if row:
            data["pkrv_3m"] = row["yield_pct"]
    except Exception:
        pass
    # PKRV long end (10Y = 120 months)
    try:
        row = con.execute("""
            SELECT yield_pct, date FROM pkrv_daily
            WHERE tenor_months = 120
            ORDER BY date DESC LIMIT 1
        """).fetchone()
        if row:
            data["pkrv_10y"] = row["yield_pct"]
    except Exception:
        pass
    return data


def _load_fixed_income(con) -> dict:
    """Latest T-bill yields, PIB yields, and bond market data."""
    fi: dict = {}
    # T-Bill latest yields
    try:
        rows = con.execute("""
            SELECT tenor, cutoff_yield, weighted_avg_yield, auction_date
            FROM tbill_auctions
            WHERE auction_date = (SELECT MAX(auction_date) FROM tbill_auctions)
            ORDER BY tenor
        """).fetchall()
        if rows:
            fi["tbill_yields"] = [dict(r) for r in rows]
            fi["tbill_date"] = rows[0]["auction_date"]
    except Exception:
        pass
    # PIB latest yields
    try:
        rows = con.execute("""
            SELECT tenor, cutoff_yield, coupon_rate, auction_date
            FROM pib_auctions
            WHERE auction_date = (SELECT MAX(auction_date) FROM pib_auctions)
            ORDER BY tenor
        """).fetchall()
        if rows:
            fi["pib_yields"] = [dict(r) for r in rows]
            fi["pib_date"] = rows[0]["auction_date"]
    except Exception:
        pass
    # Full PKRV curve (latest date)
    try:
        rows = con.execute("""
            SELECT tenor_months, yield_pct FROM pkrv_daily
            WHERE date = (SELECT MAX(date) FROM pkrv_daily)
            ORDER BY tenor_months
        """).fetchall()
        if rows:
            fi["pkrv_curve"] = {r["tenor_months"]: r["yield_pct"] for r in rows}
    except Exception:
        pass
    # Bond OTC volume trend (last 5 days)
    try:
        rows = con.execute("""
            SELECT date, SUM(total_face_amount) as face_amt
            FROM sbp_bond_trading_summary
            GROUP BY date ORDER BY date DESC LIMIT 5
        """).fetchall()
        if rows:
            fi["bond_otc_volume"] = [dict(r) for r in rows]
    except Exception:
        pass
    return fi


def _load_funds_snapshot(con) -> pd.DataFrame:
    """Top mutual funds by category with latest NAV and returns."""
    try:
        return pd.read_sql_query("""
            SELECT
                mf.category,
                mf.fund_name,
                mf.amc_name,
                nav.nav,
                nav.nav_change_pct AS daily_chg,
                nav.aum_millions AS aum_m,
                nav.date
            FROM mutual_fund_nav nav
            JOIN mutual_funds mf ON nav.fund_id = mf.fund_id
            WHERE nav.date = (SELECT MAX(date) FROM mutual_fund_nav)
              AND mf.is_active = 1
              AND nav.nav > 0
            ORDER BY mf.category, nav.nav_change_pct DESC
        """, con)
    except Exception:
        return pd.DataFrame()


def _load_fund_category_summary(con) -> pd.DataFrame:
    """Average returns by fund category — computes daily change from consecutive NAVs."""
    try:
        return pd.read_sql_query("""
            WITH date_counts AS (
                SELECT date, COUNT(*) AS cnt FROM mutual_fund_nav
                WHERE nav > 0 GROUP BY date ORDER BY date DESC LIMIT 10
            ),
            latest_dates AS (
                SELECT date FROM date_counts WHERE cnt >= 100
                ORDER BY date DESC LIMIT 2
            ),
            ranked AS (
                SELECT date, MAX(date) AS d1, MIN(date) AS d0
                FROM latest_dates
            ),
            changes AS (
                SELECT
                    n1.fund_id,
                    ROUND(CASE
                        WHEN n0.nav > 0
                        THEN (n1.nav - n0.nav) / n0.nav * 100
                        ELSE n1.nav_change_pct
                    END, 2) AS daily_chg,
                    n1.aum
                FROM mutual_fund_nav n1
                JOIN ranked r ON n1.date = r.d1
                LEFT JOIN mutual_fund_nav n0
                    ON n0.fund_id = n1.fund_id AND n0.date = r.d0
                WHERE n1.nav > 0
            )
            SELECT
                mf.category,
                COUNT(*) AS funds,
                ROUND(AVG(c.daily_chg), 2) AS avg_daily_chg,
                ROUND(SUM(c.aum) / 1e6, 0) AS total_aum_m
            FROM changes c
            JOIN mutual_funds mf ON c.fund_id = mf.fund_id
            WHERE mf.is_active = 1
              AND mf.category IS NOT NULL
            GROUP BY mf.category
            ORDER BY avg_daily_chg DESC
        """, con)
    except Exception:
        return pd.DataFrame()


def _load_commodities_snapshot() -> pd.DataFrame:
    """Latest PMEX commodity prices from commod.db."""
    import sqlite3 as _sqlite3
    from pathlib import Path
    commod_db = Path("/mnt/e/psxdata/commod/commod.db")
    if not commod_db.exists():
        return pd.DataFrame()
    try:
        ccon = _sqlite3.connect(str(commod_db))
        ccon.row_factory = _sqlite3.Row
        df = pd.read_sql_query("""
            SELECT p.symbol, p.close, p.settlement_price,
                   p.traded_volume, p.trading_date
            FROM pmex_ohlc p
            WHERE p.trading_date = (SELECT MAX(trading_date) FROM pmex_ohlc)
              AND p.close > 0
            ORDER BY p.traded_volume DESC
            LIMIT 15
        """, ccon)
        ccon.close()
        return df
    except Exception:
        return pd.DataFrame()


# ═══════════════════════════════════════════════════════════════════════════
# LLM COMMENTARY
# ═══════════════════════════════════════════════════════════════════════════

_MARKET_SYSTEM_PROMPT = """You are the Chief Market Strategist at a leading Pakistani brokerage.
Write a comprehensive daily market research note for a diverse audience — from first-time retail investors to professional fund managers.

## STRUCTURE (use these exact headings):
### Market Verdict
One-paragraph summary a layperson can understand. Is the market bullish, bearish, or range-bound? What should an average investor do today?

### Index & Breadth Analysis
Interpret the KSE-100/30 levels, advance-decline ratio, and volume. What does breadth tell us about conviction?

### Sector Spotlight
Which sectors are leading/lagging? Why might that be happening? Any rotation signals?

### Top Trading Ideas
Based on momentum and volume data, highlight 3-5 names worth watching for short-term traders. Explain WHY (not just "it went up").

### Investment Opportunities
Based on valuation (P/E) and YTD performance, highlight 2-3 names for medium/long-term investors. Explain the investment thesis.

### Fixed Income & Bonds
Analyze T-bill/PIB auction yields, yield curve shape (PKRV), and bond OTC volumes. Are government yields attractive vs equities? Is the curve steepening or flattening? What does this signal for rate expectations?

### Mutual Funds Overview
Which fund categories are performing best? Any notable category rotation (e.g., equity funds vs money market)? Are fund flows supportive of the equity market?

### Commodities & Global Inputs
Analyze PMEX commodity trends — gold, crude oil, cotton, and base metals that impact Pakistani industry. How do global commodity moves affect PSX sectors (energy, textiles, fertilizer)?

### Macro & Rates Context
How do current interest rates (policy rate, KIBOR), FX (USD/PKR), and yield curve shape affect equity markets? Is the rate cycle supportive or headwind?

### Asset Allocation View
Given all the data across equities, fixed income, funds, and commodities — what is the optimal allocation tilt? Should investors overweight equities vs bonds? Is it time to diversify into commodities or stay in money market funds?

### Risk Factors
What could go wrong? 2-3 bullet points on key risks to watch across all asset classes.

## RULES
- NEVER invent numbers — only use data provided
- If data is missing, say "data not available"
- Be direct and confident — no wishy-washy language
- Use PKR for prices, percentage terms for comparison
- Reference PSX-specific factors: circuit breakers (±7.5%), T+2 settlement, thin liquidity in small caps
- End each section heading with a signal tag in brackets: [BULLISH], [BEARISH], [NEUTRAL], [CAUTION], or [INFO]. Example: "### Market Verdict [BULLISH]"
"""


def _build_market_prompt(
    indices: pd.DataFrame,
    breadth: dict,
    sectors: pd.DataFrame,
    momentum: pd.DataFrame,
    value: pd.DataFrame,
    volume: pd.DataFrame,
    macro: dict,
    fixed_income: dict | None = None,
    fund_cats: pd.DataFrame | None = None,
    commodities: pd.DataFrame | None = None,
) -> str:
    """Build the user prompt with all market data."""
    parts = [f"**Date:** {datetime.now().strftime('%Y-%m-%d')}\n"]

    # Indices
    if not indices.empty:
        parts.append("## INDEX DATA")
        for _, r in indices.iterrows():
            parts.append(
                f"- **{r.get('index_code')}**: {r.get('value', 'N/A'):,.0f} "
                f"({r.get('change_pct', 0):+.2f}%) | "
                f"YTD: {r.get('ytd_change_pct', 'N/A')}% | "
                f"52W: {r.get('week_52_low', 'N/A'):,.0f}–{r.get('week_52_high', 'N/A'):,.0f}"
            )
    else:
        parts.append("## INDEX DATA\nNot available")

    # Breadth
    if breadth:
        parts.append(f"\n## MARKET BREADTH (as of {breadth.get('session_date', 'N/A')})")
        parts.append(
            f"- Advancing: {breadth.get('adv', 0)} | Declining: {breadth.get('dec', 0)} | "
            f"Unchanged: {breadth.get('unch', 0)} | Total: {breadth.get('total', 0)}"
        )
        tv = breadth.get('total_turnover') or 0
        parts.append(f"- Total Market Turnover: PKR {tv/1e9:.2f}B")
    else:
        parts.append("\n## MARKET BREADTH\nNot available")

    # Sectors
    if not sectors.empty:
        parts.append("\n## SECTOR PERFORMANCE (top 10 + bottom 5)")
        top = sectors.head(10).to_string(index=False)
        bot = sectors.tail(5).to_string(index=False)
        parts.append(f"**Top sectors:**\n```\n{top}\n```")
        parts.append(f"**Bottom sectors:**\n```\n{bot}\n```")

    # Momentum picks
    if not momentum.empty:
        parts.append("\n## TOP MOMENTUM STOCKS (by daily change %)")
        parts.append(f"```\n{momentum.to_string(index=False)}\n```")

    # Value picks
    if not value.empty:
        parts.append("\n## VALUE STOCKS (low P/E, positive YTD)")
        parts.append(f"```\n{value.to_string(index=False)}\n```")

    # Volume leaders
    if not volume.empty:
        parts.append("\n## VOLUME LEADERS")
        parts.append(f"```\n{volume.to_string(index=False)}\n```")

    # Macro
    parts.append("\n## MACRO / RATES DATA")
    parts.append(f"- SBP Policy Rate: {macro.get('policy_rate', 'N/A')}% (as of {macro.get('policy_date', 'N/A')})")
    parts.append(f"- KIBOR 6M Offer: {macro.get('kibor_6m', 'N/A')}%")
    parts.append(f"- KONIA (overnight): {macro.get('konia', 'N/A')}%")
    parts.append(f"- USD/PKR Interbank: {macro.get('usd_pkr', 'N/A')}")
    pkrv3 = macro.get('pkrv_3m')
    pkrv10 = macro.get('pkrv_10y')
    if pkrv3 and pkrv10:
        spread = pkrv10 - pkrv3
        parts.append(f"- PKRV 3M: {pkrv3:.2f}% | 10Y: {pkrv10:.2f}% | Spread: {spread:+.2f}bp")
    elif pkrv3:
        parts.append(f"- PKRV 3M: {pkrv3:.2f}%")

    # Fixed Income
    fi = fixed_income or {}
    if fi:
        parts.append("\n## FIXED INCOME DATA")
        tbills = fi.get("tbill_yields")
        if tbills:
            parts.append(f"**T-Bill Auction ({fi.get('tbill_date', 'N/A')}):**")
            for t in tbills:
                parts.append(f"- {t['tenor']}: cutoff {t.get('cutoff_yield', 'N/A')}%, WAY {t.get('weighted_avg_yield', 'N/A')}%")
        pibs = fi.get("pib_yields")
        if pibs:
            parts.append(f"**PIB Auction ({fi.get('pib_date', 'N/A')}):**")
            for p in pibs:
                parts.append(f"- {p['tenor']}: cutoff {p.get('cutoff_yield', 'N/A')}%, coupon {p.get('coupon_rate', 'N/A')}%")
        curve = fi.get("pkrv_curve")
        if curve:
            curve_str = ", ".join(f"{m}M={y:.2f}%" for m, y in sorted(curve.items()))
            parts.append(f"**PKRV Yield Curve:** {curve_str}")
        otc = fi.get("bond_otc_volume")
        if otc:
            parts.append("**Bond OTC Volumes (last 5 days):**")
            for o in otc:
                parts.append(f"- {o['date']}: PKR {o.get('face_amt', 0)/1e9:.1f}B face value")

    # Mutual Funds
    if fund_cats is not None and not fund_cats.empty:
        parts.append("\n## MUTUAL FUND CATEGORIES (latest NAV date)")
        parts.append(f"```\n{fund_cats.to_string(index=False)}\n```")

    # Commodities
    if commodities is not None and not commodities.empty:
        parts.append("\n## PMEX COMMODITIES (latest trading date)")
        parts.append(f"```\n{commodities.to_string(index=False)}\n```")

    parts.append("\nProvide your comprehensive daily market research note following the structured format.")
    return "\n".join(parts)


_MODEL_TIERS = {
    "Normal": {
        "model": "gpt-4o-mini",
        "label": "GPT-4o Mini",
        "desc": "Fast & affordable — good for a quick daily overview",
        "max_tokens": 1500,
        "temperature": 0.5,
    },
    "Experienced": {
        "model": "gpt-4o",
        "label": "GPT-4o",
        "desc": "Balanced depth & speed — solid research-grade analysis",
        "max_tokens": 2500,
        "temperature": 0.4,
    },
    "Expert": {
        "model": "o3-mini",
        "label": "o3-mini (Reasoning)",
        "desc": "Deep reasoning model — institutional-quality research note",
        "max_tokens": 4000,
        "temperature": 1,  # o3-mini only supports temperature=1
    },
}


def _generate_commentary(prompt: str, tier: str = "Normal") -> str | None:
    """Call LLM for market commentary with tier-based model selection."""
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return None
    cfg = _MODEL_TIERS.get(tier, _MODEL_TIERS["Normal"])
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        is_reasoning = cfg["model"].startswith("o3") or cfg["model"].startswith("o1")
        kwargs: dict = dict(
            model=cfg["model"],
            messages=[
                {"role": "system" if not is_reasoning else "developer", "content": _MARKET_SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
        )
        if is_reasoning:
            kwargs["max_completion_tokens"] = cfg["max_tokens"]
        else:
            kwargs["max_tokens"] = cfg["max_tokens"]
            kwargs["temperature"] = cfg["temperature"]
        resp = client.chat.completions.create(**kwargs)
        return resp.choices[0].message.content
    except Exception as e:
        return f"LLM Error: {str(e)[:300]}"


# ═══════════════════════════════════════════════════════════════════════════
# RENDERING
# ═══════════════════════════════════════════════════════════════════════════

def _metric_card(label: str, value: str, delta: str = "", color: str = "") -> str:
    """HTML metric card."""
    dc = color or (_C["up"] if delta.startswith("+") else _C["down"] if delta.startswith("-") else _C["dim"])
    delta_html = f'<div style="color:{dc};font-size:13px;">{delta}</div>' if delta else ""
    return (
        f'<div style="background:{_C["card"]};border-radius:8px;padding:14px 16px;'
        f'border:1px solid {_C["grid"]};">'
        f'<div style="color:{_C["dim"]};font-size:11px;text-transform:uppercase;">{label}</div>'
        f'<div style="font-size:22px;font-weight:700;font-family:monospace;margin:4px 0;">{value}</div>'
        f'{delta_html}</div>'
    )


def _render_market_snapshot(indices: pd.DataFrame, breadth: dict):
    """Section 1: indices + breadth."""
    st.markdown("### Market Snapshot")

    if indices.empty:
        st.info("No index data available. Sync market data first.")
        return

    # Index cards
    cols = st.columns(min(len(indices), 4))
    for i, (_, r) in enumerate(indices.iterrows()):
        code = r.get("index_code", "")
        val = r.get("value", 0)
        chg = r.get("change_pct", 0) or 0
        sign = "+" if chg >= 0 else ""
        with cols[i % len(cols)]:
            st.markdown(
                _metric_card(code, f"{val:,.0f}", f"{sign}{chg:.2f}%"),
                unsafe_allow_html=True,
            )

    # Breadth bar
    if breadth:
        adv = breadth.get("adv", 0) or 0
        dec = breadth.get("dec", 0) or 0
        unch = breadth.get("unch", 0) or 0
        total = adv + dec + unch or 1
        tv = breadth.get("total_turnover") or 0
        vol = breadth.get("total_volume") or 0

        c1, c2, c3, c4 = st.columns(4)
        c1.markdown(_metric_card("Advancing", str(adv), f"{adv/total*100:.0f}%", _C["up"]), unsafe_allow_html=True)
        c2.markdown(_metric_card("Declining", str(dec), f"{dec/total*100:.0f}%", _C["down"]), unsafe_allow_html=True)
        c3.markdown(_metric_card("Unchanged", str(unch), "", _C["neutral"]), unsafe_allow_html=True)
        c4.markdown(_metric_card("Turnover", f"PKR {tv/1e9:.2f}B", format_volume(vol) + " shares"), unsafe_allow_html=True)


def _render_index_chart(con):
    """KSE-100 recent chart."""
    df = _load_index_history(con, "KSE100", 60)
    if df.empty:
        return
    df = df.sort_values("index_date")
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["index_date"], y=df["value"],
        mode="lines", line=dict(color=_C["accent"], width=2),
        name="KSE-100",
    ))
    fig.update_layout(**_LAYOUT, height=280, title="KSE-100 (60 days)")
    st.plotly_chart(fig, use_container_width=True)


def _render_sector_heatmap(sectors: pd.DataFrame):
    """Section 2: sector performance table + bar chart."""
    st.markdown("### Sector Performance")
    if sectors.empty:
        st.info("No sector data available.")
        return

    # Bar chart
    top15 = sectors.head(15)
    colors = [_C["up"] if v >= 0 else _C["down"] for v in top15["avg_chg"]]
    fig = go.Figure(go.Bar(
        x=top15["avg_chg"], y=top15["sector"], orientation="h",
        marker_color=colors, text=[f"{v:+.2f}%" for v in top15["avg_chg"]],
        textposition="outside",
    ))
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=_LAYOUT["font"], legend=_LAYOUT["legend"], margin=_LAYOUT["margin"],
        height=max(350, len(top15) * 28),
        title="Average Change % by Sector",
        yaxis=dict(autorange="reversed", gridcolor=_C["grid"]),
        xaxis=dict(title="Avg Change %", gridcolor=_C["grid"]),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Table
    with st.expander("Full Sector Table", expanded=False):
        st.dataframe(
            sectors.style.applymap(
                lambda v: f"color: {_C['up']}" if isinstance(v, (int, float)) and v > 0
                else f"color: {_C['down']}" if isinstance(v, (int, float)) and v < 0
                else "",
                subset=["avg_chg"],
            ),
            use_container_width=True, hide_index=True,
        )


def _render_top_picks(momentum: pd.DataFrame, value: pd.DataFrame, volume: pd.DataFrame):
    """Section 3: trading + investing picks."""
    st.markdown("### Top Picks")

    tab_trade, tab_invest, tab_volume = st.tabs(["Momentum (Trade)", "Value (Invest)", "Volume Leaders"])

    with tab_trade:
        if momentum.empty:
            st.info("No momentum data. Ensure trading sessions are synced.")
        else:
            st.caption("Highest daily change % with volume > 50K shares")
            display_cols = ["symbol", "close", "change_pct", "volume", "turnover", "ytd_change", "pe_ratio_ttm", "sector_name"]
            avail = [c for c in display_cols if c in momentum.columns]
            st.dataframe(
                momentum[avail].style.applymap(
                    lambda v: f"color: {_C['up']}" if isinstance(v, (int, float)) and v > 0
                    else f"color: {_C['down']}" if isinstance(v, (int, float)) and v < 0
                    else "",
                    subset=[c for c in ["change_pct", "ytd_change"] if c in avail],
                ),
                use_container_width=True, hide_index=True,
            )

    with tab_invest:
        if value.empty:
            st.info("No value picks. Ensure trading sessions are synced.")
        else:
            st.caption("Low P/E (< 15), price > PKR 5, volume > 10K")
            display_cols = ["symbol", "close", "pe_ratio_ttm", "ytd_change", "year_1_change", "volume", "sector_name"]
            avail = [c for c in display_cols if c in value.columns]
            st.dataframe(value[avail], use_container_width=True, hide_index=True)

    with tab_volume:
        if volume.empty:
            st.info("No volume data.")
        else:
            st.caption("Top 10 by shares traded")
            display_cols = ["symbol", "close", "change_pct", "volume", "turnover", "sector_name"]
            avail = [c for c in display_cols if c in volume.columns]
            st.dataframe(volume[avail], use_container_width=True, hide_index=True)


def _render_macro_dashboard(macro: dict):
    """Section 4: rates + FX snapshot."""
    st.markdown("### Macro & Rates")
    if not macro:
        st.info("No macro data available. Sync KIBOR/KONIA/FX data.")
        return

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(
        _metric_card("Policy Rate", f"{macro.get('policy_rate', 'N/A')}%", macro.get("policy_date", "")),
        unsafe_allow_html=True,
    )
    kibor = macro.get("kibor_6m")
    c2.markdown(
        _metric_card("KIBOR 6M", f"{kibor:.2f}%" if kibor else "N/A", "Offer rate"),
        unsafe_allow_html=True,
    )
    konia = macro.get("konia")
    c3.markdown(
        _metric_card("KONIA", f"{konia:.2f}%" if konia else "N/A", "Overnight"),
        unsafe_allow_html=True,
    )
    usd = macro.get("usd_pkr")
    c4.markdown(
        _metric_card("USD/PKR", f"{usd:.2f}" if usd else "N/A", "Interbank"),
        unsafe_allow_html=True,
    )

    # Yield curve spread
    pkrv3 = macro.get("pkrv_3m")
    pkrv10 = macro.get("pkrv_10y")
    if pkrv3 and pkrv10:
        spread = pkrv10 - pkrv3
        curve_shape = "Normal (steep)" if spread > 1 else "Flat" if abs(spread) < 0.5 else "Inverted" if spread < -0.5 else "Normal"
        cc1, cc2, cc3 = st.columns(3)
        cc1.markdown(_metric_card("PKRV 3M", f"{pkrv3:.2f}%", "Short end"), unsafe_allow_html=True)
        cc2.markdown(_metric_card("PKRV 10Y", f"{pkrv10:.2f}%", "Long end"), unsafe_allow_html=True)
        cc3.markdown(_metric_card("Curve Spread", f"{spread:+.2f}%", curve_shape), unsafe_allow_html=True)


def _render_fixed_income(fi: dict):
    """Section 5: Fixed income snapshot."""
    st.markdown("### Fixed Income & Bonds")
    if not fi:
        st.info("No fixed income data available.")
        return

    # T-Bill + PIB yields
    tbills = fi.get("tbill_yields", [])
    pibs = fi.get("pib_yields", [])
    if tbills or pibs:
        c1, c2 = st.columns(2)
        with c1:
            if tbills:
                st.caption(f"T-Bill Auction ({fi.get('tbill_date', '')})")
                for t in tbills:
                    cy = t.get("cutoff_yield")
                    st.markdown(
                        _metric_card(
                            f"T-Bill {t['tenor']}",
                            f"{cy:.2f}%" if cy else "N/A",
                            "Cutoff yield",
                        ),
                        unsafe_allow_html=True,
                    )
        with c2:
            if pibs:
                st.caption(f"PIB Auction ({fi.get('pib_date', '')})")
                for p in pibs:
                    cy = p.get("cutoff_yield")
                    st.markdown(
                        _metric_card(
                            f"PIB {p['tenor']}",
                            f"{cy:.2f}%" if cy else "N/A",
                            f"Coupon {p.get('coupon_rate', 'N/A')}%",
                        ),
                        unsafe_allow_html=True,
                    )

    # PKRV curve mini-chart
    curve = fi.get("pkrv_curve")
    if curve:
        tenors = sorted(curve.keys())
        yields = [curve[t] for t in tenors]
        labels = [f"{t}M" for t in tenors]
        fig = go.Figure(go.Scatter(
            x=labels, y=yields, mode="lines+markers",
            line=dict(color=_C["accent"], width=2),
            marker=dict(size=6),
        ))
        fig.update_layout(**_LAYOUT, height=220, title="PKRV Yield Curve")
        st.plotly_chart(fig, use_container_width=True)


def _render_funds_overview(fund_cats: pd.DataFrame):
    """Section 6: Mutual fund categories."""
    st.markdown("### Mutual Funds")
    if fund_cats.empty:
        st.info("No fund data available. Sync mutual fund NAVs first.")
        return

    # Bar chart by category
    colors = [_C["up"] if pd.notna(v) and v >= 0 else _C["down"] for v in fund_cats["avg_daily_chg"]]
    fig = go.Figure(go.Bar(
        x=fund_cats["avg_daily_chg"], y=fund_cats["category"], orientation="h",
        marker_color=colors,
        text=[f"{v:+.2f}%" if pd.notna(v) else "" for v in fund_cats["avg_daily_chg"]],
        textposition="outside",
    ))
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=_LAYOUT["font"], margin=_LAYOUT["margin"],
        height=max(250, len(fund_cats) * 30),
        title="Avg Daily NAV Change by Fund Category",
        yaxis=dict(autorange="reversed", gridcolor=_C["grid"]),
        xaxis=dict(title="Avg Daily %", gridcolor=_C["grid"]),
    )
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Category Details", expanded=False):
        st.dataframe(fund_cats, use_container_width=True, hide_index=True)


def _render_commodities(commodities: pd.DataFrame):
    """Section 7: PMEX commodities."""
    st.markdown("### Commodities (PMEX)")
    if commodities.empty:
        st.info("No PMEX data available. Sync commodities first.")
        return

    st.caption(f"Latest trading date: {commodities['trading_date'].iloc[0] if 'trading_date' in commodities.columns else 'N/A'}")
    display_cols = [c for c in ["symbol", "close", "settlement_price", "traded_volume"] if c in commodities.columns]
    st.dataframe(commodities[display_cols], use_container_width=True, hide_index=True)


def _render_ai_commentary(prompt: str):
    """Section 5: LLM commentary with model tier selector."""
    st.markdown("### AI Market Research Note")

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        st.warning("Set OPENAI_API_KEY in .env to enable AI commentary.")
        return

    # Model tier selector
    tier_names = list(_MODEL_TIERS.keys())
    col_sel, col_info = st.columns([1, 2])
    with col_sel:
        tier = st.radio(
            "Analyst Level",
            tier_names,
            index=0,
            key="mr_tier",
            horizontal=True,
        )
    with col_info:
        cfg = _MODEL_TIERS[tier]
        st.caption(f"**{cfg['label']}** — {cfg['desc']}")

    cache_key = f"market_research_commentary_{tier}"

    from pakfindata.ui.components.commentary_renderer import render_styled_commentary

    if cache_key in st.session_state and st.session_state[cache_key]:
        render_styled_commentary(st.session_state[cache_key], "Market Research Note")
        if st.button("Regenerate", key="mr_regen"):
            # Clear only this tier's cache
            st.session_state[cache_key] = None
            st.rerun()
        return

    if st.button(f"Generate Research Note ({cfg['label']})", type="primary", key="mr_gen"):
        with st.spinner(f"Generating with {cfg['label']}..."):
            result = _generate_commentary(prompt, tier)
            if result:
                st.session_state[cache_key] = result
                render_styled_commentary(result, "Market Research Note")
            else:
                st.error("Failed to generate commentary.")


# ═══════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

def render_market_research():
    """Main page renderer."""
    st.markdown(
        '<div style="background:linear-gradient(135deg,#1a237e 0%,#0d47a1 50%,#01579b 100%);'
        'border-radius:12px;padding:24px;margin-bottom:20px;text-align:center;">'
        '<h1 style="color:white;margin:0;">Market Research</h1>'
        '<p style="color:rgba(255,255,255,0.8);margin:8px 0 0;">Holistic market analysis with AI-powered commentary</p>'
        '</div>',
        unsafe_allow_html=True,
    )

    con = get_connection()

    # Load all data up front
    indices = _load_indices(con)
    breadth = _load_breadth(con)
    sectors = _load_sector_performance(con)
    momentum = _load_momentum_picks(con)
    value_picks = _load_value_picks(con)
    vol_leaders = _load_volume_leaders(con)
    macro = _load_macro_snapshot(con)
    fixed_income = _load_fixed_income(con)
    fund_cats = _load_fund_category_summary(con)
    commodities = _load_commodities_snapshot()

    # Render sections
    _render_market_snapshot(indices, breadth)
    _render_index_chart(con)

    st.markdown("---")
    _render_sector_heatmap(sectors)

    st.markdown("---")
    _render_top_picks(momentum, value_picks, vol_leaders)

    st.markdown("---")
    _render_macro_dashboard(macro)

    st.markdown("---")
    _render_fixed_income(fixed_income)

    st.markdown("---")
    _render_funds_overview(fund_cats)

    st.markdown("---")
    _render_commodities(commodities)

    st.markdown("---")
    # Build LLM prompt from all loaded data
    prompt = _build_market_prompt(
        indices, breadth, sectors, momentum, value_picks, vol_leaders, macro,
        fixed_income=fixed_income, fund_cats=fund_cats, commodities=commodities,
    )
    _render_ai_commentary(prompt)

    render_footer()
