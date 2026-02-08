"""Mutual Fund + ETF Explorer — fund directory, NAV charts, rankings."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from psx_ohlcv.ui.components.helpers import get_connection, render_footer
from psx_ohlcv.sync_mufap import seed_mutual_funds, sync_mutual_funds
from psx_ohlcv.sources.etf_scraper import ETFScraper


def render_fund_explorer():
    """Mutual fund and ETF explorer page."""
    st.markdown("## Fund & ETF Explorer")

    con = get_connection()
    if con is None:
        st.error("Database connection not available")
        return

    try:
        tab1, tab2, tab3 = st.tabs(["Mutual Funds", "ETFs", "Top Performers"])

        with tab1:
            _render_fund_directory(con)
        with tab2:
            _render_etf_section(con)
        with tab3:
            _render_top_performers(con)

    except Exception as e:
        st.error(f"Error loading fund data: {e}")

    # Sync section
    st.markdown("---")
    with st.expander("Sync Fund Data"):
        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button("Seed Funds", type="primary", key="fexp_seed_funds"):
                with st.spinner("Seeding mutual funds from MUFAP..."):
                    try:
                        result = seed_mutual_funds()
                        st.success(
                            f"Seeded {result.get('inserted', 0)} funds "
                            f"(Failed: {result.get('failed', 0)})"
                        )
                        st.rerun()
                    except Exception as e:
                        st.error(f"Sync failed: {e}")

        with col2:
            if st.button("Sync NAV Data", key="fexp_sync_nav"):
                with st.spinner("Syncing NAV data from MUFAP..."):
                    try:
                        summary = sync_mutual_funds(source="AUTO")
                        st.success(
                            f"Synced {summary.ok} funds, "
                            f"{summary.rows_upserted} NAV records"
                        )
                        st.rerun()
                    except Exception as e:
                        st.error(f"Sync failed: {e}")

        with col3:
            if st.button("Sync ETFs", key="fexp_sync_etfs"):
                with st.spinner("Syncing ETF data..."):
                    try:
                        result = ETFScraper().sync_all_etfs(con)
                        st.success(
                            f"ETFs: {result.get('ok', 0)} synced, "
                            f"{result.get('failed', 0)} failed"
                        )
                        st.rerun()
                    except Exception as e:
                        st.error(f"Sync failed: {e}")

    render_footer()


def _render_fund_directory(con):
    """Fund listing with filters and detail view."""
    # Sidebar filters
    col1, col2, col3, col4 = st.columns(4)

    categories = con.execute(
        "SELECT DISTINCT category FROM mutual_funds ORDER BY category"
    ).fetchall()
    cat_list = ["All"] + [r["category"] for r in categories]

    amcs = con.execute(
        "SELECT DISTINCT amc_name FROM mutual_funds WHERE amc_name IS NOT NULL ORDER BY amc_name"
    ).fetchall()
    amc_list = ["All"] + [r["amc_name"] for r in amcs]

    with col1:
        sel_category = st.selectbox("Category", cat_list, key="fund_cat")
    with col2:
        sel_amc = st.selectbox("AMC", amc_list, key="fund_amc")
    with col3:
        sel_shariah = st.selectbox("Shariah", ["All", "Yes", "No"], key="fund_shariah")
    with col4:
        sel_type = st.selectbox("Type", ["All", "OPEN_END", "VPS"], key="fund_type")

    # Build query
    sql = """SELECT f.fund_id, f.symbol, f.fund_name, f.category, f.amc_name,
                    f.is_shariah, f.fund_type,
                    ln.nav as latest_nav, ln.date as nav_date
             FROM mutual_funds f
             LEFT JOIN (
                 SELECT n.fund_id, n.nav, n.date
                 FROM mutual_fund_nav n
                 INNER JOIN (
                     SELECT fund_id, MAX(date) as max_date
                     FROM mutual_fund_nav GROUP BY fund_id
                 ) mx ON n.fund_id = mx.fund_id AND n.date = mx.max_date
             ) ln ON f.fund_id = ln.fund_id
             WHERE 1=1"""
    params: list = []

    if sel_category != "All":
        sql += " AND f.category = ?"
        params.append(sel_category)
    if sel_amc != "All":
        sql += " AND f.amc_name = ?"
        params.append(sel_amc)
    if sel_shariah == "Yes":
        sql += " AND f.is_shariah = 1"
    elif sel_shariah == "No":
        sql += " AND f.is_shariah = 0"
    if sel_type != "All":
        sql += " AND f.fund_type = ?"
        params.append(sel_type)

    sql += " ORDER BY f.fund_name"
    df = pd.read_sql_query(sql, con, params=params)

    if df.empty:
        st.info("No funds match filters")
        return

    st.caption(f"{len(df)} funds found")
    st.dataframe(
        df[["symbol", "fund_name", "category", "amc_name", "is_shariah", "latest_nav", "nav_date"]].rename(columns={
            "symbol": "Symbol", "fund_name": "Fund Name", "category": "Category",
            "amc_name": "AMC", "is_shariah": "Shariah", "latest_nav": "NAV",
            "nav_date": "NAV Date",
        }),
        use_container_width=True, hide_index=True,
    )

    # Fund detail expander
    fund_ids = df["fund_id"].tolist()
    selected_fund = st.selectbox("Select fund for detail view", fund_ids, key="fund_detail")

    if selected_fund:
        _render_fund_detail(con, selected_fund)


def _render_fund_detail(con, fund_id):
    """NAV history chart for a selected fund."""
    fund = con.execute(
        "SELECT * FROM mutual_funds WHERE fund_id = ?", (fund_id,)
    ).fetchone()
    if not fund:
        return

    st.markdown(f"### {fund['fund_name']}")
    col1, col2, col3 = st.columns(3)
    col1.metric("Category", fund["category"])
    col2.metric("AMC", fund["amc_name"] or "N/A")
    col3.metric("Shariah", "Yes" if fund["is_shariah"] else "No")

    # NAV history chart
    df = pd.read_sql_query(
        "SELECT date, nav FROM mutual_fund_nav WHERE fund_id = ? ORDER BY date",
        con, params=(fund_id,),
    )
    if df.empty:
        st.info("No NAV history available")
        return

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["nav"],
        mode="lines", name="NAV",
        line=dict(width=2, color="#FF6B35"),
        fill="tozeroy", fillcolor="rgba(255, 107, 53, 0.1)",
    ))
    fig.update_layout(
        xaxis_title="Date", yaxis_title="NAV (PKR)",
        height=350, margin=dict(l=20, r=20, t=30, b=20),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Return calculation
    if len(df) >= 2:
        latest = df.iloc[-1]["nav"]
        first = df.iloc[0]["nav"]
        total_return = (latest - first) / first * 100
        st.metric("Total Return", f"{total_return:.2f}%", help=f"From {df.iloc[0]['date']} to {df.iloc[-1]['date']}")


def _render_etf_section(con):
    """ETF listing with NAV vs market price."""
    st.markdown("### Listed ETFs")

    df = pd.read_sql_query(
        """SELECT m.symbol, m.name, m.amc, m.benchmark_index,
                  m.shariah_compliant,
                  n.date, n.nav, n.market_price, n.premium_discount, n.aum_millions
           FROM etf_master m
           LEFT JOIN etf_nav n ON m.symbol = n.symbol
             AND n.date = (SELECT MAX(date) FROM etf_nav WHERE symbol = m.symbol)
           ORDER BY m.symbol""",
        con,
    )

    if df.empty:
        st.info("No ETF data. Run `psxsync etf sync` to fetch.")
        return

    st.dataframe(
        df.rename(columns={
            "symbol": "Symbol", "name": "Name", "amc": "AMC",
            "nav": "NAV", "market_price": "Market Price",
            "premium_discount": "Prem/Disc %", "aum_millions": "AUM (M)",
            "date": "Date", "shariah_compliant": "Shariah",
        }),
        use_container_width=True, hide_index=True,
    )

    # Premium/Discount visual
    etfs_with_pd = df.dropna(subset=["premium_discount"])
    if not etfs_with_pd.empty:
        fig = go.Figure()
        colors = ["#4ECDC4" if v >= 0 else "#FF6B35" for v in etfs_with_pd["premium_discount"]]
        fig.add_trace(go.Bar(
            x=etfs_with_pd["symbol"], y=etfs_with_pd["premium_discount"],
            marker_color=colors, text=[f"{v:.1f}%" for v in etfs_with_pd["premium_discount"]],
            textposition="outside",
        ))
        fig.update_layout(
            yaxis_title="Premium / Discount (%)", height=300,
            margin=dict(l=20, r=20, t=30, b=20),
        )
        fig.add_hline(y=0, line_dash="dash", line_color="gray")
        st.plotly_chart(fig, use_container_width=True)


def _render_top_performers(con):
    """Top performing funds by return over different periods."""
    st.markdown("### Top Performers")

    period = st.radio("Period", ["30 days", "90 days", "365 days"], horizontal=True, key="fund_perf_period")
    days = {"30 days": 30, "90 days": 90, "365 days": 365}[period]

    df = pd.read_sql_query(
        """
        WITH latest_nav AS (
            SELECT n.fund_id, n.nav, n.date
            FROM mutual_fund_nav n
            INNER JOIN (
                SELECT fund_id, MAX(date) as max_date FROM mutual_fund_nav GROUP BY fund_id
            ) mx ON n.fund_id = mx.fund_id AND n.date = mx.max_date
        ),
        old_nav AS (
            SELECT n.fund_id, n.nav, n.date
            FROM mutual_fund_nav n
            INNER JOIN (
                SELECT fund_id, MIN(date) as min_date
                FROM mutual_fund_nav WHERE date >= date('now', ? || ' days')
                GROUP BY fund_id
            ) mn ON n.fund_id = mn.fund_id AND n.date = mn.min_date
        )
        SELECT f.fund_name, f.category, f.amc_name,
               l.nav as latest_nav,
               ROUND((l.nav - o.nav) / o.nav * 100, 2) as return_pct
        FROM mutual_funds f
        INNER JOIN latest_nav l ON f.fund_id = l.fund_id
        INNER JOIN old_nav o ON f.fund_id = o.fund_id
        WHERE o.nav > 0
        ORDER BY return_pct DESC LIMIT 20
        """,
        con, params=(f"-{days}",),
    )

    if df.empty:
        st.info("Insufficient NAV history for return calculations")
        return

    st.dataframe(
        df.rename(columns={
            "fund_name": "Fund", "category": "Category",
            "amc_name": "AMC", "latest_nav": "NAV",
            "return_pct": f"Return ({period})",
        }),
        use_container_width=True, hide_index=True,
    )
