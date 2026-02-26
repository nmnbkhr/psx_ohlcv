"""Mutual Fund + ETF Explorer — fund directory, NAV charts, rankings."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from pakfindata.ui.components.helpers import get_connection, render_footer
from pakfindata.sync_mufap import (
    seed_mutual_funds,
    sync_fund_nav,
    sync_mutual_funds,
    sync_performance,
    sync_expense_ratios,
)
from pakfindata.sources.etf_scraper import ETFScraper


def render_fund_explorer():
    """Mutual fund and ETF explorer page."""
    st.markdown("## Fund & ETF Explorer")

    con = get_connection()
    if con is None:
        st.error("Database connection not available")
        return

    # Ensure fund_performance schema exists
    try:
        from pakfindata.db.repositories.fixed_income import init_fund_performance_schema
        init_fund_performance_schema(con)
    except Exception:
        pass

    try:
        tab_mf, tab_etf, tab_vps, tab_top = st.tabs([
            "Mutual Funds", "ETFs", "VPS Pension", "Top Performers",
        ])

        with tab_mf:
            _render_category_summary(con)
            _render_fund_directory(con)
        with tab_etf:
            _render_etf_section(con)
        with tab_vps:
            _render_vps_section(con)
        with tab_top:
            _render_top_performers(con)

    except Exception as e:
        st.error(f"Error loading fund data: {e}")

    # Sync section
    st.markdown("---")
    with st.expander("Sync Fund Data"):
        c1, c2, c3, c4, c5 = st.columns(5)

        with c1:
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

        with c2:
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

        with c3:
            if st.button("Sync Performance", key="fexp_sync_perf"):
                with st.spinner("Fetching MUFAP performance data (tab=1)..."):
                    try:
                        result = sync_performance()
                        if result.get("status") == "ok":
                            st.success(
                                f"Stored {result['funds_synced']} fund returns "
                                f"({result['categories']} categories, {result['date']})"
                            )
                        else:
                            st.error(result.get("error", "Unknown error"))
                        st.rerun()
                    except Exception as e:
                        st.error(f"Sync failed: {e}")

        with c4:
            if st.button("Sync Expense", key="fexp_sync_expense"):
                with st.spinner("Fetching expense ratios (tab=5)..."):
                    try:
                        result = sync_expense_ratios()
                        if result.get("status") == "ok":
                            st.success(f"Updated {result['updated']} funds")
                        else:
                            st.error(result.get("error", "Unknown error"))
                        st.rerun()
                    except Exception as e:
                        st.error(f"Sync failed: {e}")

        with c5:
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


# ─────────────────────────────────────────────────────────────────────────────
# Category Summary Cards
# ─────────────────────────────────────────────────────────────────────────────


def _render_category_summary(con):
    """Top-level category group metrics from fund_performance."""
    try:
        perf = pd.read_sql_query(
            """SELECT fp.sector, fp.category, fp.return_ytd FROM fund_performance fp
               WHERE fp.validity_date = (
                   SELECT MAX(fp2.validity_date) FROM fund_performance fp2
                   WHERE fp2.fund_name = fp.fund_name
               )""",
            con,
        )
    except Exception:
        return

    if perf.empty:
        return

    cat_lower = perf["category"].str.lower()

    equity_mask = cat_lower.str.contains("equity", na=False) & ~cat_lower.str.contains("vps", na=False)
    income_mask = (
        cat_lower.str.contains("income|money market|fixed rate", na=False)
        & ~cat_lower.str.contains("shariah|islamic|vps", na=False)
    )
    islamic_mask = cat_lower.str.contains("shariah|islamic", na=False) & ~cat_lower.str.contains("vps", na=False)
    vps_mask = cat_lower.str.contains("vps", na=False)

    groups = [
        ("Equity Funds", equity_mask),
        ("Income/MM", income_mask),
        ("Islamic Funds", islamic_mask),
        ("VPS Pension", vps_mask),
    ]

    cols = st.columns(len(groups))
    for i, (label, mask) in enumerate(groups):
        sub = perf.loc[mask, "return_ytd"].dropna()
        count = len(sub)
        avg = sub.mean() if count > 0 else 0
        with cols[i]:
            st.metric(label, f"{count} funds", f"Avg YTD: {avg:.1f}%")


# ─────────────────────────────────────────────────────────────────────────────
# Mutual Funds Tab
# ─────────────────────────────────────────────────────────────────────────────


def _render_fund_directory(con):
    """Fund listing with filters, performance columns, and detail view."""
    col1, col2, col3, col4, col5 = st.columns(5)

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
        sel_type = st.selectbox("Type", ["All", "OPEN_END", "VPS", "DEDICATED", "EMPLOYER_PENSION", "ETF"], key="fund_type")
    with col5:
        search_term = st.text_input("Search", key="fund_search", placeholder="Fund name...")

    # Build query with LEFT JOIN to fund_performance for return columns
    sql = """SELECT f.fund_id, f.symbol, f.fund_name, f.category, f.amc_name,
                    f.is_shariah, f.fund_type, f.expense_ratio,
                    n.nav as latest_nav, n.date as nav_date,
                    p.return_30d, p.return_90d, p.return_ytd, p.return_365d, p.rating
             FROM mutual_funds f
             LEFT JOIN mutual_fund_nav n ON n.fund_id = f.fund_id
                 AND n.date = (SELECT MAX(n2.date) FROM mutual_fund_nav n2
                               WHERE n2.fund_id = f.fund_id)
             LEFT JOIN fund_performance p ON p.fund_name = f.fund_name
                 AND p.validity_date = (SELECT MAX(p2.validity_date) FROM fund_performance p2
                                        WHERE p2.fund_name = p.fund_name)
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
    if search_term:
        sql += " AND f.fund_name LIKE ?"
        params.append(f"%{search_term}%")

    sql += " ORDER BY f.fund_name"
    df = pd.read_sql_query(sql, con, params=params)

    if df.empty:
        st.info("No funds match filters")
        return

    st.caption(f"{len(df)} funds found")

    # Display table with performance columns
    display_cols = ["symbol", "fund_name", "category", "amc_name", "latest_nav", "return_30d", "return_90d", "return_ytd", "return_365d", "rating"]
    display_df = df[[c for c in display_cols if c in df.columns]].rename(columns={
        "symbol": "Symbol", "fund_name": "Fund Name", "category": "Category",
        "amc_name": "AMC", "latest_nav": "NAV",
        "return_30d": "1M %", "return_90d": "3M %",
        "return_ytd": "YTD %", "return_365d": "1Y %", "rating": "Rating",
    })

    st.dataframe(display_df, use_container_width=True, hide_index=True)

    # Fund detail selector
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
    """NAV history chart + performance returns for a selected fund."""
    fund = con.execute(
        "SELECT * FROM mutual_funds WHERE fund_id = ?", (fund_id,)
    ).fetchone()
    if not fund:
        return

    st.markdown(f"### {fund['fund_name']}")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Category", fund["category"])
    c2.metric("AMC", fund["amc_name"] or "N/A")
    c3.metric("Shariah", "Yes" if fund["is_shariah"] else "No")
    try:
        c4.metric("Expense Ratio", f"{fund['expense_ratio']:.2f}%" if fund["expense_ratio"] else "N/A")
    except (KeyError, IndexError):
        c4.metric("Expense Ratio", "N/A")

    # Sync full history button
    try:
        mufap_int_id = fund["mufap_int_id"]
    except (KeyError, IndexError):
        mufap_int_id = None
    with c5:
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

    # Performance returns bar chart (from fund_performance)
    try:
        perf = con.execute(
            """SELECT return_ytd, return_mtd, return_1d, return_15d, return_30d,
                      return_90d, return_180d, return_270d, return_365d, return_2y, return_3y
               FROM fund_performance fp WHERE fp.fund_name = ?
               AND fp.validity_date = (SELECT MAX(fp2.validity_date) FROM fund_performance fp2
                                       WHERE fp2.fund_name = fp.fund_name)""",
            (fund["fund_name"],),
        ).fetchone()
        if perf:
            labels = ["1D", "15D", "1M", "3M", "6M", "9M", "YTD", "1Y", "2Y", "3Y"]
            vals = [perf["return_1d"], perf["return_15d"], perf["return_30d"],
                    perf["return_90d"], perf["return_180d"], perf["return_270d"],
                    perf["return_ytd"], perf["return_365d"], perf["return_2y"], perf["return_3y"]]
            valid = [(l, v) for l, v in zip(labels, vals) if v is not None]
            if valid:
                bar_labels, bar_vals = zip(*valid)
                colors = ["#4ECDC4" if v >= 0 else "#FF6B35" for v in bar_vals]
                fig = go.Figure(go.Bar(
                    x=list(bar_labels), y=list(bar_vals),
                    marker_color=colors,
                    text=[f"{v:.1f}%" for v in bar_vals],
                    textposition="outside",
                ))
                fig.update_layout(
                    title="Returns by Period", yaxis_title="Return %",
                    height=300, margin=dict(l=20, r=20, t=40, b=20),
                )
                st.plotly_chart(fig, use_container_width=True)
    except Exception:
        pass

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


# ─────────────────────────────────────────────────────────────────────────────
# ETFs Tab
# ─────────────────────────────────────────────────────────────────────────────


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


# ─────────────────────────────────────────────────────────────────────────────
# VPS Pension Tab
# ─────────────────────────────────────────────────────────────────────────────


def _render_vps_section(con):
    """VPS pension fund comparison."""
    st.markdown("### VPS Pension Funds")
    st.caption("Compare pension fund performance across AMCs and sub-fund categories")

    try:
        df = pd.read_sql_query(
            """SELECT fp.fund_name, fp.category, fp.nav, fp.rating,
                      fp.return_ytd, fp.return_30d, fp.return_90d,
                      fp.return_365d, fp.return_2y, fp.return_3y
               FROM fund_performance fp
               WHERE fp.validity_date = (
                   SELECT MAX(fp2.validity_date) FROM fund_performance fp2
                   WHERE fp2.fund_name = fp.fund_name
               )
                 AND (fp.sector LIKE '%VPS%' OR fp.category LIKE 'VPS%')
               ORDER BY fp.category, fp.return_ytd DESC""",
            con,
        )
    except Exception:
        df = pd.DataFrame()

    if df.empty:
        st.info("No VPS performance data. Click **Sync Performance** to fetch from MUFAP.")
        return

    validity = con.execute(
        "SELECT MAX(validity_date) FROM fund_performance WHERE sector LIKE '%VPS%' OR category LIKE 'VPS%'"
    ).fetchone()[0]
    st.caption(f"Data as of: **{validity}** | {len(df)} VPS funds")

    # AMC filter
    # Extract AMC name from fund name (first word typically)
    vps_categories = sorted(df["category"].dropna().unique())
    sel_vps_cat = st.selectbox("VPS Category", ["All"] + list(vps_categories), key="vps_cat_filter")

    if sel_vps_cat != "All":
        df = df[df["category"] == sel_vps_cat]

    # Summary by sub-fund type
    if len(vps_categories) > 1:
        summary = df.groupby("category").agg(
            funds=("fund_name", "count"),
            avg_ytd=("return_ytd", "mean"),
            best_ytd=("return_ytd", "max"),
            worst_ytd=("return_ytd", "min"),
        ).round(2).reset_index()

        st.dataframe(
            summary.rename(columns={
                "category": "Sub-Fund Type", "funds": "Funds",
                "avg_ytd": "Avg YTD %", "best_ytd": "Best YTD %", "worst_ytd": "Worst YTD %",
            }),
            use_container_width=True, hide_index=True,
        )

    # Full fund table
    st.dataframe(
        df.rename(columns={
            "fund_name": "Fund", "category": "Category", "nav": "NAV",
            "rating": "Rating", "return_ytd": "YTD %", "return_30d": "1M %",
            "return_90d": "3M %", "return_365d": "1Y %",
            "return_2y": "2Y %", "return_3y": "3Y %",
        }),
        use_container_width=True, hide_index=True,
    )

    # Gold/Commodity sub-funds highlight
    gold_mask = df["category"].str.contains("Commodit|Gold", case=False, na=False)
    gold_df = df[gold_mask]
    if not gold_df.empty:
        st.markdown("#### Gold / Commodity Sub-Funds")
        st.dataframe(
            gold_df[["fund_name", "category", "nav", "return_ytd", "return_365d"]].rename(columns={
                "fund_name": "Fund", "category": "Category", "nav": "NAV",
                "return_ytd": "YTD %", "return_365d": "1Y %",
            }),
            use_container_width=True, hide_index=True,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Top Performers Tab
# ─────────────────────────────────────────────────────────────────────────────


def _render_top_performers(con):
    """Top performing funds using MUFAP official returns from fund_performance."""
    st.markdown("### Top Performers")

    # Check if fund_performance has data
    try:
        perf_count = con.execute("SELECT COUNT(*) FROM fund_performance").fetchone()[0]
    except Exception:
        perf_count = 0

    if perf_count == 0:
        # Fallback to NAV-computed returns
        _render_top_performers_fallback(con)
        return

    period_map = {
        "YTD": "return_ytd",
        "MTD": "return_mtd",
        "1M": "return_30d",
        "3M": "return_90d",
        "6M": "return_180d",
        "1Y": "return_365d",
        "2Y": "return_2y",
        "3Y": "return_3y",
    }

    c1, c2, c3 = st.columns([2, 2, 1])
    with c1:
        sel_period = st.radio("Period", list(period_map.keys()), horizontal=True, key="top_perf_period")
    with c2:
        # Category filter from fund_performance
        try:
            cats = pd.read_sql_query(
                "SELECT DISTINCT category FROM fund_performance ORDER BY category", con
            )["category"].tolist()
        except Exception:
            cats = []
        sel_cat = st.selectbox("Category", ["All"] + cats, key="top_perf_cat")
    with c3:
        top_n = st.number_input("Top N", min_value=5, max_value=50, value=20, key="top_n")

    period_col = period_map[sel_period]

    query = f"""
        SELECT fund_name, category, sector, nav, rating, {period_col} as return_pct
        FROM fund_performance fp
        WHERE fp.validity_date = (
            SELECT MAX(fp2.validity_date) FROM fund_performance fp2
            WHERE fp2.fund_name = fp.fund_name
        )
          AND {period_col} IS NOT NULL
    """
    params: list = []
    if sel_cat != "All":
        query += " AND category = ?"
        params.append(sel_cat)
    query += f" ORDER BY {period_col} DESC LIMIT ?"
    params.append(top_n)

    df = pd.read_sql_query(query, con, params=params)

    if df.empty:
        st.info("No performance data for selected filters")
        return

    validity = con.execute(
        "SELECT MAX(validity_date) FROM fund_performance"
    ).fetchone()[0]
    st.caption(f"MUFAP official returns as of **{validity}**")

    st.dataframe(
        df.rename(columns={
            "fund_name": "Fund", "category": "Category", "sector": "Sector",
            "nav": "NAV", "rating": "Rating",
            "return_pct": f"Return ({sel_period}) %",
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


def _render_top_performers_fallback(con):
    """Fallback: NAV-computed returns when fund_performance table is empty."""
    st.caption("Performance data not synced — showing NAV-computed returns. Click Sync Performance for MUFAP official returns.")

    period = st.radio("Period", ["30 days", "90 days", "365 days"], horizontal=True, key="fund_perf_period_fb")
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
