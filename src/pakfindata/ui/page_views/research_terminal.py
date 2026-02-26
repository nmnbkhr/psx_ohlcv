"""SQL Research Terminal — custom query editor with saved templates."""

import streamlit as st
import pandas as pd

from pakfindata.ui.components.helpers import get_connection, render_footer

_WRITE_KEYWORDS = {"INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE", "REPLACE", "ATTACH", "DETACH"}

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
       -- First and last of month
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
    """SQL research terminal with editor, results, and saved queries."""
    st.markdown("## Research Terminal")

    con = get_connection()
    if con is None:
        st.error("Database connection not available")
        return

    try:
        col1, col2 = st.columns([3, 1])

        with col2:
            _render_schema_browser(con)

        with col1:
            _render_sql_editor(con)

    except Exception as e:
        st.error(f"Error: {e}")

    render_footer()


def _render_sql_editor(con):
    """SQL editor with saved query templates."""
    # Saved query picker
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
        help="Only SELECT queries are allowed. Max 500 rows returned.",
    )

    col1, col2 = st.columns([1, 4])
    with col1:
        run = st.button("Run Query", type="primary", key="sql_run")
    with col2:
        max_rows = st.number_input("Max rows", 10, 1000, 100, key="sql_max_rows")

    if run and query.strip():
        _execute_query(con, query.strip(), max_rows)


def _execute_query(con, query: str, max_rows: int):
    """Execute a SQL query with safety checks."""
    # Reject write queries
    first_word = query.split()[0].upper() if query.split() else ""
    if first_word in _WRITE_KEYWORDS:
        st.error("Only SELECT queries are allowed.")
        return

    # Reject multiple statements
    if ";" in query.rstrip(";"):
        st.error("Multiple statements not allowed.")
        return

    # Add LIMIT if not present
    upper_q = query.upper()
    if "LIMIT" not in upper_q:
        query = query.rstrip(";") + f" LIMIT {max_rows}"

    try:
        with st.spinner("Executing..."):
            df = pd.read_sql_query(query, con)

        st.success(f"{len(df)} rows returned")
        st.dataframe(df, use_container_width=True, hide_index=True)

        # Download CSV
        csv = df.to_csv(index=False)
        st.download_button(
            "Download CSV",
            csv,
            "query_results.csv",
            "text/csv",
            key="sql_download",
        )

    except Exception as e:
        st.error(f"Query error: {e}")


def _render_schema_browser(con):
    """Sidebar schema browser showing tables and columns."""
    st.markdown("### Schema Browser")

    tables = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()

    for t in tables:
        table_name = t["name"]
        cnt = con.execute(f"SELECT COUNT(*) FROM [{table_name}]").fetchone()[0]
        with st.expander(f"{table_name} ({cnt:,} rows)"):
            cols = con.execute(f"PRAGMA table_info([{table_name}])").fetchall()
            for c in cols:
                pk = " PK" if c["pk"] else ""
                st.text(f"  {c['name']} ({c['type']}{pk})")
