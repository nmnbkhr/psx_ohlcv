"""Mutual Fund + ETF Explorer — fund directory, NAV charts, rankings."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from psx_ohlcv.ui.components.helpers import get_connection, render_footer
from psx_ohlcv.sync_mufap import seed_mutual_funds, sync_fund_nav, sync_mutual_funds
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

    # Build query — correlated subquery: 1,182 PK seeks vs 1.9M-row GROUP BY scan
    sql = """SELECT f.fund_id, f.symbol, f.fund_name, f.category, f.amc_name,
                    f.is_shariah, f.fund_type,
                    n.nav as latest_nav, n.date as nav_date
             FROM mutual_funds f
             LEFT JOIN mutual_fund_nav n ON n.fund_id = f.fund_id
                 AND n.date = (SELECT MAX(n2.date) FROM mutual_fund_nav n2
                               WHERE n2.fund_id = f.fund_id)
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

    # Fund detail selector — show symbol + name instead of raw fund_id
    fund_options = {
        row["fund_id"]: f"{row['symbol']} — {row['fund_name']}"
        for _, row in df.iterrows()
    }
    selected_fund = st.selectbox(
        "Select fund for detail view",
        options=list(fund_options.keys()),
        format_func=lambda x: fund_options.get(x, x),
        key="fund_detail",
    )

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
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Category", fund["category"])
    col2.metric("AMC", fund["amc_name"] or "N/A")
    col3.metric("Shariah", "Yes" if fund["is_shariah"] else "No")

    # Sync full history button — sqlite3.Row doesn't have .get(), use try/except
    try:
        mufap_int_id = fund["mufap_int_id"]
    except (KeyError, IndexError):
        mufap_int_id = None
    with col4:
        if mufap_int_id and st.button("Sync Full History", key=f"sync_hist_{fund_id}"):
            with st.spinner("Fetching full NAV history from MUFAP..."):
                try:
                    rows, error = sync_fund_nav(fund_id, incremental=False)
                    if error:
                        st.error(error)
                    else:
                        st.success(f"Synced {rows} NAV records")
                        st.rerun()
                except Exception as e:
                    st.error(f"Sync failed: {e}")

    # NAV history chart
    df = pd.read_sql_query(
        "SELECT date, nav FROM mutual_fund_nav WHERE fund_id = ? ORDER BY date",
        con, params=(fund_id,),
    )
    if df.empty:
        st.info("No NAV history available. Click 'Sync Full History' to fetch.")
        return

    nav_count = len(df)
    date_range = f"{df.iloc[0]['date']} to {df.iloc[-1]['date']}" if nav_count > 1 else df.iloc[0]["date"]
    st.caption(f"{nav_count} NAV records | {date_range}")

    fig = go.Figure()
    nav_min = df["nav"].min()
    nav_max = df["nav"].max()
    pad = max((nav_max - nav_min) * 0.1, nav_min * 0.01)
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["nav"],
        mode="lines", name="NAV",
        line=dict(width=2, color="#FF6B35"),
        hovertemplate="Date: %{x}<br>NAV: Rs. %{y:.4f}<extra></extra>",
    ))
    fig.update_layout(
        xaxis_title="Date", yaxis_title="NAV (PKR)",
        height=350, margin=dict(l=20, r=20, t=30, b=20),
        yaxis=dict(range=[nav_min - pad, nav_max + pad]),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Return calculation
    if nav_count >= 2:
        latest = df.iloc[-1]["nav"]
        first = df.iloc[0]["nav"]
        if first > 0:
            total_return = (latest - first) / first * 100
            st.metric(
                "Total Return", f"{total_return:.2f}%",
                help=f"From {df.iloc[0]['date']} to {df.iloc[-1]['date']}",
            )


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
        SELECT f.fund_name, f.category, f.amc_name,
               nl.nav as latest_nav,
               ROUND((nl.nav - older.nav) / older.nav * 100, 2) as return_pct
        FROM mutual_funds f
        INNER JOIN mutual_fund_nav nl ON nl.fund_id = f.fund_id
            AND nl.date = (SELECT MAX(date) FROM mutual_fund_nav
                           WHERE fund_id = f.fund_id)
        INNER JOIN mutual_fund_nav older ON older.fund_id = f.fund_id
            AND older.date = (SELECT MIN(date) FROM mutual_fund_nav
                              WHERE fund_id = f.fund_id
                              AND date >= date('now', ? || ' days'))
        WHERE older.nav > 0
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

    # Rate benchmarks for comparison
    try:
        bm_cols = st.columns(4)
        pr = con.execute("SELECT policy_rate FROM sbp_policy_rates ORDER BY rate_date DESC LIMIT 1").fetchone()
        kb = con.execute("SELECT bid, offer FROM kibor_daily WHERE tenor='3M' ORDER BY date DESC LIMIT 1").fetchone()
        tb = con.execute("SELECT cutoff_yield FROM tbill_auctions WHERE tenor='3M' ORDER BY auction_date DESC LIMIT 1").fetchone()
        tb6 = con.execute("SELECT cutoff_yield FROM tbill_auctions WHERE tenor='6M' ORDER BY auction_date DESC LIMIT 1").fetchone()
        with bm_cols[0]:
            st.metric("Policy Rate", f"{pr[0]:.1f}%" if pr else "—", help="SBP benchmark")
        with bm_cols[1]:
            if kb and kb[0] and kb[1]:
                st.metric("KIBOR 3M", f"{(kb[0]+kb[1])/2:.2f}%", help="Money market benchmark")
            else:
                st.metric("KIBOR 3M", "—")
        with bm_cols[2]:
            st.metric("T-Bill 3M", f"{tb[0]:.2f}%" if tb else "—", help="Risk-free 3M")
        with bm_cols[3]:
            st.metric("T-Bill 6M", f"{tb6[0]:.2f}%" if tb6 else "—", help="Risk-free 6M")
    except Exception:
        pass
