"""Research Terminal — AI-powered financial research + SQL query editor.

Migration note: the **SQL Editor** tab runs user-supplied SQL via a
direct DB connection. This is the feature, not an accident — exposing
arbitrary SELECT execution through /v1 would either reintroduce the
same surface or break the editor entirely. The connection is opened
read-only (``get_read_db``-equivalent through ``get_connection`` —
admin Streamlit context) and is allowlist-protected against
``INSERT``/``UPDATE``/``DROP``/etc. via ``_WRITE_KEYWORDS``.

All other tabs (AI Research, Schema Browser) are fully migrated to /v1.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from pakfindata.ui.api import client as api_client
from pakfindata.ui.components.helpers import get_connection, render_footer

_CACHE_TTL = 3600


# ── AI Research data loaders (all /v1) ──────────────────────────────────────


@st.cache_data(ttl=_CACHE_TTL, show_spinner=False)
def _load_kibor_rates() -> pd.DataFrame:
    """Latest KIBOR rows for the AI context strip."""
    rows = api_client.get_kibor_history(days=30) or []
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)[["date", "tenor", "bid", "offer"]].copy()
    return df.sort_values("date", ascending=False).head(20).reset_index(drop=True)


@st.cache_data(ttl=_CACHE_TTL, show_spinner=False)
def _load_policy_rates(limit: int = 10) -> pd.DataFrame:
    rows = api_client.get_policy_rate_history(limit=limit) or []
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if "rate_date" not in df.columns and "date" in df.columns:
        df = df.rename(columns={"date": "rate_date"})
    keep = [c for c in ("rate_date", "policy_rate") if c in df.columns]
    return df[keep] if keep else df


@st.cache_data(ttl=_CACHE_TTL, show_spinner=False)
def _load_tbill_auctions() -> pd.DataFrame:
    rows = api_client.get_tbill_auctions(limit=20) or []
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    keep = [c for c in ("auction_date", "tenor", "cutoff_yield") if c in df.columns]
    return df[keep] if keep else df


@st.cache_data(ttl=_CACHE_TTL, show_spinner=False)
def _load_pib_auctions() -> pd.DataFrame:
    rows = api_client.get_pib_auctions(limit=15) or []
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    keep = [c for c in ("auction_date", "tenor", "cutoff_yield") if c in df.columns]
    return df[keep] if keep else df


@st.cache_data(ttl=_CACHE_TTL, show_spinner=False)
def _load_fx_interbank() -> pd.DataFrame:
    """Last 10 rows × 3 currencies = ~30 rows of interbank FX."""
    parts = []
    for ccy in ("USD", "EUR", "GBP"):
        rows = api_client.get_fx_history(ccy, source="interbank", limit=10) or []
        if rows:
            parts.append(pd.DataFrame(rows))
    if not parts:
        return pd.DataFrame()
    df = pd.concat(parts, ignore_index=True)
    keep = [c for c in ("currency", "date", "buying", "selling") if c in df.columns]
    return df[keep] if keep else df


@st.cache_data(ttl=_CACHE_TTL, show_spinner=False)
def _load_fund_performance() -> pd.DataFrame:
    rows = api_client.get_fund_performance_leaders(
        metric="return_ytd", limit=20, direction="top"
    ) or []
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    keep = [
        c for c in (
            "fund_name", "category", "return_ytd", "return_30d",
            "return_90d", "return_365d",
        ) if c in df.columns
    ]
    return df[keep] if keep else df


@st.cache_data(ttl=_CACHE_TTL, show_spinner=False)
def _load_psx_indices() -> pd.DataFrame:
    """Recent index history (mixed-code) for AI context."""
    rows = api_client.get_all_indices() or []
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    # `get_all_indices` returns latest-per-code; that IS the market summary.
    # For AI context we want recent rows — the latest snapshot is fine.
    keep = [
        c for c in ("index_code", "index_date", "value", "change_pct") if c in df.columns
    ]
    return df[keep] if keep else df


@st.cache_data(ttl=_CACHE_TTL, show_spinner=False)
def _load_market_summary() -> pd.DataFrame:
    """Latest-per-index snapshot — same shape as ``get_all_indices``."""
    return _load_psx_indices()


# ── Schema Browser loaders (all /v1/admin) ──────────────────────────────────


@st.cache_data(ttl=_CACHE_TTL, show_spinner=False)
def _load_schema_tables() -> list[str]:
    """Distinct table names from the admin catalog."""
    rows = api_client.get_admin_tables(include_counts=False) or []
    return sorted(r["name"] for r in rows if r.get("name"))


@st.cache_data(ttl=_CACHE_TTL, show_spinner=False)
def _load_table_info(table_name: str) -> tuple[int, list[dict]]:
    """Row count + column list for one table (via /v1/admin)."""
    tables = api_client.get_admin_tables(include_counts=True) or []
    cnt = next(
        (int(r.get("row_count") or 0) for r in tables if r.get("name") == table_name),
        0,
    )
    cols = api_client.get_admin_table_columns(table_name) or []
    return cnt, cols


# ── SQL Editor: kept as a direct read (admin carve-out) ────────────────────

_WRITE_KEYWORDS = {
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE",
    "TRUNCATE", "REPLACE", "ATTACH", "DETACH",
}

# Quick research prompts
_QUICK_QUERIES = {
    "Rate Outlook": "What is the current interest rate environment in Pakistan? Analyze KIBOR trends, T-Bill yields, and policy rate trajectory.",
    "Top Funds": "Which mutual funds have performed best in the last 30 days? Compare equity, income, and Islamic categories.",
    "PKR Signals": "Analyze USD/PKR trends. Compare interbank vs kerb spreads and identify any intervention signals.",
    "Market Breadth": "Analyze PSX market breadth. How many stocks are advancing vs declining? What does sector rotation look like?",
    "Bond Update": "Summarize the latest T-Bill and PIB auction results. How have yields changed? What does the yield curve shape suggest?",
}

# Saved SQL queries
_SAVED_QUERIES = {
    "T-Bill yield spread (6M vs 12M)": """
SELECT a.auction_date,
       a.cutoff_yield as yield_6m,
       b.cutoff_yield as yield_12m,
       ROUND(b.cutoff_yield - a.cutoff_yield, 4) as spread
FROM tbill_auctions a
INNER JOIN tbill_auctions b ON a.auction_date = b.auction_date
WHERE a.tenor = '6M' AND b.tenor = '12M'
ORDER BY a.auction_date DESC
LIMIT 20""",
    "FX interbank vs kerb premium": """
SELECT i.currency, i.date,
       i.selling as interbank_sell,
       k.selling as kerb_sell,
       ROUND(k.selling - i.selling, 2) as kerb_premium
FROM sbp_fx_interbank i
INNER JOIN forex_kerb k ON i.currency = k.currency AND i.date = k.date
ORDER BY i.date DESC, i.currency
LIMIT 30""",
    "Top dividend yield stocks": """
SELECT p.symbol, s.name, s.sector_name,
       SUM(p.amount) as total_div_last_year,
       e.close as latest_price,
       ROUND(SUM(p.amount) / e.close * 100, 2) as div_yield_pct
FROM company_payouts p
INNER JOIN symbols s ON p.symbol = s.symbol
INNER JOIN eod_ohlcv e ON p.symbol = e.symbol
  AND e.date = (SELECT MAX(date) FROM eod_ohlcv)
WHERE p.payout_type = 'cash'
  AND p.ex_date >= date('now', '-365 days')
GROUP BY p.symbol
ORDER BY div_yield_pct DESC
LIMIT 20""",
    "Sector PE comparison": """
SELECT s.sector_name,
       COUNT(*) as stocks,
       ROUND(AVG(cf.pe_ratio), 2) as avg_pe,
       ROUND(MIN(cf.pe_ratio), 2) as min_pe,
       ROUND(MAX(cf.pe_ratio), 2) as max_pe
FROM company_fundamentals cf
INNER JOIN symbols s ON cf.symbol = s.symbol
WHERE cf.pe_ratio > 0 AND cf.pe_ratio < 100
GROUP BY s.sector_name
ORDER BY avg_pe
LIMIT 20""",
    "Monthly KSE-100 returns": """
SELECT strftime('%Y-%m', index_date) as month,
       MIN(value) as low,
       MAX(value) as high,
       (SELECT value FROM psx_indices p2
        WHERE p2.index_code = 'KSE100'
          AND strftime('%Y-%m', p2.index_date) = strftime('%Y-%m', p.index_date)
        ORDER BY p2.index_date DESC LIMIT 1) as month_close
FROM psx_indices p
WHERE index_code = 'KSE100'
GROUP BY month
ORDER BY month DESC
LIMIT 12""",
    "Fund NAV vs KSE-100 daily": """
SELECT n.date,
       n.nav as fund_nav,
       idx.value as kse100,
       n.nav_change_pct as fund_change_pct,
       idx.change_pct as index_change_pct
FROM mutual_fund_nav n
INNER JOIN psx_indices idx ON n.date = idx.index_date AND idx.index_code = 'KSE100'
WHERE n.fund_id = (SELECT fund_id FROM mutual_funds LIMIT 1)
ORDER BY n.date DESC
LIMIT 30""",
}


def render_research_terminal():
    """AI-powered research terminal with SQL editor."""
    st.markdown("## Research Terminal")

    api_client.render_api_status_banner_if_down()

    tab_ai, tab_sql, tab_schema = st.tabs([
        "AI Research", "SQL Editor", "Schema Browser",
    ])

    with tab_ai:
        _render_ai_research()

    with tab_sql:
        _render_sql_editor()

    with tab_schema:
        _render_schema_browser()

    render_footer()


def _render_ai_research():
    """Natural language research interface."""
    import os

    api_key = os.environ.get("OPENAI_API_KEY") or os.environ.get("ANTHROPIC_API_KEY")

    # Quick query chips
    st.markdown("#### Quick Research")
    cols = st.columns(len(_QUICK_QUERIES))
    selected_quick = None
    for i, (label, prompt) in enumerate(_QUICK_QUERIES.items()):
        with cols[i]:
            if st.button(label, key=f"quick_{i}", width='stretch'):
                selected_quick = prompt

    # Custom query input
    query = st.text_area(
        "Ask anything about Pakistan's financial markets...",
        value=selected_quick or "",
        height=100,
        key="ai_research_query",
        placeholder="e.g., What are the best performing equity funds this year?",
    )

    if not api_key:
        st.info(
            "Set `OPENAI_API_KEY` or `ANTHROPIC_API_KEY` environment variable to enable AI research. "
            "SQL Editor tab is available without API keys."
        )
        return

    if st.button("Research", type="primary", key="ai_research_run") and query.strip():
        with st.spinner("Researching..."):
            try:
                _run_ai_research(query.strip())
            except Exception as e:
                st.error(f"Research failed: {str(e)[:300]}")

    # Query history
    if "research_history" in st.session_state and st.session_state.research_history:
        with st.expander("Query History"):
            for item in reversed(st.session_state.research_history[-10:]):
                st.markdown(f"**Q:** {item['query'][:100]}")
                st.caption(item.get("timestamp", ""))
                st.markdown("---")


def _run_ai_research(query: str):
    """Execute AI research: gather data context and generate analysis."""
    from datetime import datetime

    if "research_history" not in st.session_state:
        st.session_state.research_history = []

    data_context = _build_research_context(query)

    try:
        from pakfindata.agents.llm_client import get_completion
        from pakfindata.agents.prompts import SYSTEM_PROMPT

        full_prompt = f"""{SYSTEM_PROMPT}

The user is asking a research question about Pakistan's financial markets.
Use ONLY the data provided below to answer. Do not make up numbers.

## Available Data Context
{data_context}

## User Question
{query}

Provide a structured analysis with:
1. Key findings (bullet points)
2. Supporting data (reference specific numbers)
3. Assessment (BULLISH / BEARISH / NEUTRAL / MIXED)
4. Caveats and data limitations
"""
        response = get_completion(full_prompt)

        from pakfindata.ui.components.commentary_renderer import render_styled_commentary
        render_styled_commentary(response, "AI Research")

        st.session_state.research_history.append({
            "query": query,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
        })

    except ImportError:
        st.error("AI agents module not available. Check installation.")
    except Exception as e:
        st.error(f"AI generation error: {str(e)[:200]}")

    if data_context:
        with st.expander("Data Context Used"):
            st.text(data_context[:5000])


def _build_research_context(query: str) -> str:
    """Build relevant data context based on query keywords."""
    context_parts = []
    q_lower = query.lower()

    if any(kw in q_lower for kw in ["rate", "kibor", "policy", "interest", "monetary"]):
        df = _load_kibor_rates()
        if not df.empty:
            context_parts.append(f"## KIBOR Rates (Latest 20)\n{df.to_string(index=False)}")
        df = _load_policy_rates(limit=10)
        if not df.empty:
            context_parts.append(f"## Policy Rate History\n{df.to_string(index=False)}")

    if any(kw in q_lower for kw in ["treasury", "tbill", "t-bill", "pib", "bond", "yield", "auction"]):
        df = _load_tbill_auctions()
        if not df.empty:
            context_parts.append(f"## T-Bill Auctions\n{df.to_string(index=False)}")
        df = _load_pib_auctions()
        if not df.empty:
            context_parts.append(f"## PIB Auctions\n{df.to_string(index=False)}")

    if any(kw in q_lower for kw in ["fx", "currency", "pkr", "dollar", "usd", "exchange", "kerb", "interbank"]):
        df = _load_fx_interbank()
        if not df.empty:
            context_parts.append(f"## FX Interbank Rates\n{df.to_string(index=False)}")

    if any(kw in q_lower for kw in ["fund", "mutual", "nav", "mufap", "equity fund", "income fund", "vps"]):
        df = _load_fund_performance()
        if not df.empty:
            context_parts.append(f"## Top Fund Performance\n{df.to_string(index=False)}")

    if any(kw in q_lower for kw in ["market", "kse", "index", "breadth", "sector", "stock"]):
        df = _load_psx_indices()
        if not df.empty:
            context_parts.append(f"## PSX Indices\n{df.to_string(index=False)}")

    if not context_parts:
        df = _load_market_summary()
        if not df.empty:
            context_parts.append(f"## Market Summary\n{df.to_string(index=False)}")
        df = _load_policy_rates(limit=5)
        if not df.empty:
            context_parts.append(f"## Policy Rate\n{df.to_string(index=False)}")

    return "\n\n".join(context_parts)


def _render_sql_editor():
    """SQL editor with saved query templates.

    Uses a direct DB connection — this is intentional. The editor lets
    analysts run arbitrary SELECT statements; routing through /v1 would
    either require an arbitrary-SQL endpoint (security regression) or
    remove the feature. Connection is opened read-only and gated by
    ``_WRITE_KEYWORDS`` + the multi-statement check below.
    """
    selected_template = st.selectbox(
        "Load saved query",
        ["(Custom)"] + list(_SAVED_QUERIES.keys()),
        key="sql_template",
    )

    default_sql = ""
    if selected_template != "(Custom)":
        default_sql = _SAVED_QUERIES[selected_template].strip()

    query = st.text_area(
        "SQL Query (SELECT only)",
        value=default_sql,
        height=200,
        key="sql_query",
        help="Only SELECT queries are allowed. Max 1000 rows returned.",
    )

    col1, col2 = st.columns([1, 4])
    with col1:
        run = st.button("Run Query", type="primary", key="sql_run")
    with col2:
        max_rows = st.number_input("Max rows", 10, 1000, 100, key="sql_max_rows")

    if run and query.strip():
        con = get_connection()
        if con is None:
            st.error("Database connection not available")
            return
        _execute_query(con, query.strip(), max_rows)


def _execute_query(con, query: str, max_rows: int):
    """Execute a SQL query with safety checks (admin carve-out)."""
    first_word = query.split()[0].upper() if query.split() else ""
    if first_word in _WRITE_KEYWORDS:
        st.error("Only SELECT queries are allowed.")
        return

    if ";" in query.rstrip(";"):
        st.error("Multiple statements not allowed.")
        return

    upper_q = query.upper()
    if "LIMIT" not in upper_q:
        query = query.rstrip(";") + f" LIMIT {max_rows}"

    try:
        with st.spinner("Executing..."):
            df = pd.read_sql_query(query, con)

        st.success(f"{len(df)} rows returned")
        st.dataframe(df, width='stretch', hide_index=True)

        # Auto-detect chartable results
        if len(df.columns) >= 2:
            numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
            date_cols = [c for c in df.columns if "date" in c.lower() or c.lower() in ("month", "year")]

            if numeric_cols and date_cols:
                with st.expander("Auto Chart"):
                    fig = go.Figure()
                    x_col = date_cols[0]
                    for nc in numeric_cols[:4]:
                        fig.add_trace(go.Scatter(
                            x=df[x_col], y=df[nc],
                            mode="lines+markers", name=nc,
                        ))
                    fig.update_layout(height=350, margin=dict(l=20, r=20, t=30, b=20))
                    st.plotly_chart(fig, width='stretch')

        csv = df.to_csv(index=False)
        st.download_button(
            "Download CSV", csv, "query_results.csv", "text/csv", key="sql_download",
        )

    except Exception as e:
        st.error(f"Query error: {e}")


def _render_schema_browser():
    """Schema browser showing tables and columns."""
    st.markdown("### Schema Browser")

    search = st.text_input("Search tables", key="schema_search", placeholder="e.g., fund, fx, kibor")

    table_names = _load_schema_tables()

    for table_name in table_names:
        if search and search.lower() not in table_name.lower():
            continue
        cnt, cols = _load_table_info(table_name)
        with st.expander(f"{table_name} ({cnt:,} rows)"):
            for c in cols:
                pk = " PK" if c.get("pk") else ""
                st.text(f"  {c['name']} ({c.get('type', '?')}{pk})")
