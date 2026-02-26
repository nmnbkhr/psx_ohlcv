"""Mutual funds and fund analytics pages."""

from datetime import datetime, timedelta
import pandas as pd
import streamlit as st

from pakfindata.ui.components.helpers import (
    get_connection,
    render_footer,
)


def render_mutual_funds():
    """Mutual Funds Browser - Fund listing with filters."""
    from pakfindata.db import get_mf_nav, get_mutual_fund, get_mutual_funds
    from pakfindata.sync_mufap import (
        clear_nav_staging,
        get_data_summary,
        is_bulk_nav_sync_running,
        read_nav_sync_progress,
        seed_mutual_funds,
        start_bulk_nav_sync,
        sync_fund_nav,
        sync_mutual_funds,
    )

    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 🏦 Mutual Funds")
        st.caption("MUFAP Fund Directory - Pakistan Mutual Funds (Read-Only Analytics)")
    with header_col2:
        st.markdown(
            '<div class="data-info">📊 Analytics Only</div>',
            unsafe_allow_html=True
        )

    con = get_connection()

    # =================================================================
    # FILTERS
    # =================================================================
    st.markdown("---")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        categories = [
            "All", "Equity", "Islamic Equity", "Money Market",
            "Islamic Money Market", "Income", "Islamic Income",
            "Balanced", "VPS", "Asset Allocation"
        ]
        category = st.selectbox("Category", categories, key="mf_category")
        category_filter = None if category == "All" else category

    with col2:
        fund_types = ["All", "OPEN_END", "VPS", "ETF"]
        fund_type = st.selectbox("Fund Type", fund_types, key="mf_type")
        type_filter = None if fund_type == "All" else fund_type

    with col3:
        shariah_only = st.checkbox("Shariah-Compliant Only", key="mf_shariah")

    with col4:
        search = st.text_input("Search Fund", "", key="mf_search")

    # =================================================================
    # FUND LIST
    # =================================================================
    funds = get_mutual_funds(
        con,
        category=category_filter,
        fund_type=type_filter,
        is_shariah=True if shariah_only else None,
        active_only=True,
        search=search if search else None,
    )

    if not funds:
        st.warning("No mutual funds found. Click 'Seed Fund Data' below to populate funds.")
    else:
        # Summary metrics
        st.markdown("---")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Funds", len(funds))
        with col2:
            shariah_count = sum(1 for f in funds if f.get("is_shariah"))
            st.metric("Shariah Funds", shariah_count)
        with col3:
            categories_set = set(f.get("category") for f in funds)
            st.metric("Categories", len(categories_set))
        with col4:
            amcs = set(f.get("amc_code") for f in funds)
            st.metric("AMCs", len(amcs))

        # Fund table
        st.subheader("Fund Directory")
        df = pd.DataFrame(funds)
        display_cols = ["symbol", "fund_name", "category", "amc_name", "fund_type"]
        display_cols = [c for c in display_cols if c in df.columns]

        if "is_shariah" in df.columns:
            df["Shariah"] = df["is_shariah"].apply(lambda x: "Yes" if x else "No")
            display_cols.append("Shariah")

        st.dataframe(
            df[display_cols].rename(columns={
                "symbol": "Symbol",
                "fund_name": "Fund Name",
                "category": "Category",
                "amc_name": "AMC",
                "fund_type": "Type",
            }),
            use_container_width=True,
            height=400,
        )

        # =================================================================
        # FUND DETAIL
        # =================================================================
        st.markdown("---")
        st.subheader("Fund Details")

        fund_options = {f["symbol"]: f["fund_name"] for f in funds}
        selected_symbol = st.selectbox(
            "Select Fund",
            options=list(fund_options.keys()),
            format_func=lambda x: f"{x} - {fund_options[x][:50]}",
            key="mf_selected",
        )

        if selected_symbol:
            fund = next((f for f in funds if f["symbol"] == selected_symbol), None)
            if fund:
                fund_id = fund["fund_id"]

                # Fund info
                col1, col2 = st.columns([2, 1])
                with col1:
                    st.markdown(f"**{fund.get('fund_name', 'N/A')}**")
                    st.caption(f"AMC: {fund.get('amc_name', 'N/A')}")

                with col2:
                    if fund.get("is_shariah"):
                        st.success("Shariah-Compliant")
                    st.caption(f"Type: {fund.get('fund_type', 'N/A')}")

                # Sync full history button
                mufap_int_id = fund.get("mufap_int_id")
                if mufap_int_id:
                    if st.button("Sync Full NAV History", key="mf_sync_hist", type="primary"):
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

                # NAV data
                nav_df = get_mf_nav(con, fund_id, limit=5000)

                if not nav_df.empty:
                    # Latest NAV metrics
                    latest = nav_df.iloc[0]
                    col1, col2, col3, col4 = st.columns(4)

                    with col1:
                        st.metric("Latest NAV", f"Rs. {latest.get('nav', 0):.4f}")
                    with col2:
                        change = latest.get("nav_change_pct", 0) or 0
                        st.metric("Daily Change", f"{change:+.2f}%")
                    with col3:
                        aum = latest.get("aum", 0) or 0
                        st.metric("AUM", f"Rs. {aum:.0f}M")
                    with col4:
                        st.metric("Latest Date", latest.get("date", "N/A"))

                    # NAV chart
                    st.subheader("NAV History")
                    chart_df = nav_df.sort_values("date")
                    st.caption(f"{len(chart_df)} records | {chart_df.iloc[0]['date']} to {chart_df.iloc[-1]['date']}")
                    import plotly.graph_objects as go
                    nav_min = chart_df["nav"].min()
                    nav_max = chart_df["nav"].max()
                    pad = max((nav_max - nav_min) * 0.1, nav_min * 0.01)
                    fig = go.Figure(go.Scatter(
                        x=chart_df["date"], y=chart_df["nav"],
                        mode="lines", line=dict(width=2, color="#00d4aa"),
                        hovertemplate="Date: %{x}<br>NAV: Rs. %{y:.4f}<extra></extra>",
                    ))
                    fig.update_layout(
                        height=300, margin=dict(l=20, r=20, t=10, b=20),
                        yaxis=dict(range=[nav_min - pad, nav_max + pad]),
                        xaxis_title=None, yaxis_title="NAV (Rs.)",
                        template="plotly_dark",
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No NAV data. Click 'Sync Full NAV History' above to fetch.")

    # =================================================================
    # SYNC SECTION
    # =================================================================
    st.markdown("---")
    with st.expander("Sync Mutual Fund Data"):
        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button("Seed Fund Data", type="primary", key="mf_seed_btn"):
                with st.spinner("Seeding mutual funds..."):
                    result = seed_mutual_funds()
                    st.success(
                        f"Seeded {result.get('inserted', 0)} funds "
                        f"(Failed: {result.get('failed', 0)})"
                    )
                    st.rerun()

        with col2:
            if st.button("Sync NAV Data", key="mf_sync_btn"):
                with st.spinner("Syncing NAV data..."):
                    summary = sync_mutual_funds(source="AUTO")
                    st.success(
                        f"Synced {summary.ok} funds, "
                        f"{summary.rows_upserted} NAV records"
                    )
                    st.rerun()

        with col3:
            # Show summary
            data_summary = get_data_summary()
            st.metric("Funds in DB", data_summary.get("total_funds", 0))
            st.metric("NAV Records", data_summary.get("total_nav_rows", 0))

        # Bulk NAV History Sync (background job — two-phase pipeline)
        st.markdown("---")
        st.markdown("**Bulk NAV History Sync** — async fetch + batch DB write (runs in background)")

        running = is_bulk_nav_sync_running()
        progress = read_nav_sync_progress()

        if running:
            st.warning("Bulk sync is running — do not close the app.")
            if progress:
                phase = progress.get("phase", "fetch")
                phase_label = "Fetching from MUFAP..." if phase == "fetch" else "Writing to database..."
                pct = progress["current"] / max(progress["total"], 1)
                st.progress(
                    pct,
                    text="{} {}/{} — {}".format(
                        phase_label, progress["current"],
                        progress["total"], progress.get("current_fund", ""),
                    ),
                )
                c1, c2, c3, c4 = st.columns(4)
                if phase == "fetch":
                    c1.metric("Fetched", progress.get("fetch_ok", 0))
                    c2.metric("Fetch Failed", progress.get("fetch_failed", 0))
                    c3.metric("Skipped (Staged)", progress.get("fetch_skipped", 0))
                    c4.metric("Started", progress.get("started_at", "")[:19])
                else:
                    c1.metric("DB OK", progress.get("ok", 0))
                    c2.metric("DB Failed", progress.get("failed", 0))
                    c3.metric("NAV Rows", "{:,}".format(progress.get("rows_total", 0)))
                    c4.metric("Started", progress.get("started_at", "")[:19])
                if progress.get("errors"):
                    with st.expander("Errors ({})".format(len(progress["errors"]))):
                        for err in progress["errors"]:
                            st.text(err)
            if st.button("Refresh Progress", key="mf_bulk_refresh"):
                st.rerun()
        else:
            if progress and progress.get("status") == "completed":
                st.success(
                    "Last run: {ok}/{total} funds synced, "
                    "{rows:,} NAV rows — finished {fin}".format(
                        ok=progress.get("ok", 0),
                        total=progress["total"],
                        rows=progress.get("rows_total", 0),
                        fin=progress.get("finished_at", "")[:19],
                    )
                )

            col_resume, col_fresh, col_clear = st.columns(3)

            with col_resume:
                if st.button("Resume NAV Sync", key="mf_bulk_resume_btn", type="primary"):
                    started = start_bulk_nav_sync(resume=True)
                    if started:
                        st.success("Bulk NAV sync started (resuming from staged data)!")
                    else:
                        st.warning("Sync is already running.")
                    st.rerun()

            with col_fresh:
                if st.button("Fresh Sync (All)", key="mf_bulk_fresh_btn", type="secondary"):
                    clear_nav_staging()
                    started = start_bulk_nav_sync(resume=False)
                    if started:
                        st.success("Fresh bulk NAV sync started!")
                    else:
                        st.warning("Sync is already running.")
                    st.rerun()

            with col_clear:
                if st.button("Clear Staging Files", key="mf_clear_staging_btn"):
                    count = clear_nav_staging()
                    st.info("Cleared {} staged JSON files.".format(count))

    render_footer()


def render_fund_analytics():
    """Fund Analytics - Performance comparison and rankings."""
    from pakfindata.analytics_mufap import (
        compare_funds,
        get_category_performance,
        get_category_summary,
        get_fund_comparison_table,
        get_mf_analytics,
    )
    from pakfindata.db import get_mutual_funds

    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 📊 Fund Analytics")
        st.caption("Mutual Fund Performance Analysis (Read-Only)")
    with header_col2:
        st.markdown(
            '<div class="data-info">📈 Analytics Only</div>',
            unsafe_allow_html=True
        )

    con = get_connection()

    # Check if we have data
    funds = get_mutual_funds(con, active_only=True)
    if not funds:
        st.warning("No mutual funds found. Go to 'Mutual Funds' page and seed data first.")
        render_footer()
        return

    # =================================================================
    # CATEGORY PERFORMANCE
    # =================================================================
    st.markdown("---")
    st.subheader("Category Performance")

    col1, col2, col3 = st.columns([1, 1, 2])

    with col1:
        categories = [
            "Equity", "Islamic Equity", "Money Market",
            "Islamic Money Market", "Income", "Islamic Income",
            "Balanced", "VPS"
        ]
        category = st.selectbox("Select Category", categories, key="fa_category")

    with col2:
        periods = ["1W", "1M", "3M", "6M", "1Y"]
        period = st.selectbox("Return Period", periods, index=1, key="fa_period")

    with col3:
        # Category summary
        summary = get_category_summary(con, category, period)
        if not summary.get("error"):
            col_a, col_b, col_c = st.columns(3)
            with col_a:
                st.metric(
                    "Avg Return",
                    f"{summary.get('avg_return_pct', 0):+.2f}%"
                )
            with col_b:
                st.metric(
                    "Best Fund",
                    summary.get("best_fund_symbol", "N/A"),
                    f"{summary.get('max_return_pct', 0):+.2f}%"
                )
            with col_c:
                st.metric("Fund Count", summary.get("fund_count", 0))

    # Rankings table
    st.subheader(f"Top {category} Funds ({period})")
    rankings = get_category_performance(con, category, period, top_n=15)

    if rankings:
        rankings_df = pd.DataFrame(rankings)
        display_cols = ["rank", "symbol", "fund_name", "return_pct", "latest_nav"]
        display_cols = [c for c in display_cols if c in rankings_df.columns]

        if "is_shariah" in rankings_df.columns:
            rankings_df["Shariah"] = rankings_df["is_shariah"].apply(
                lambda x: "Yes" if x else "No"
            )
            display_cols.append("Shariah")

        st.dataframe(
            rankings_df[display_cols].rename(columns={
                "rank": "Rank",
                "symbol": "Symbol",
                "fund_name": "Fund Name",
                "return_pct": f"Return ({period})",
                "latest_nav": "NAV",
            }),
            use_container_width=True,
            height=400,
        )
    else:
        st.info(f"No data for {category} category. Sync NAV data first.")

    # =================================================================
    # FUND COMPARISON
    # =================================================================
    st.markdown("---")
    st.subheader("Fund Comparison")

    fund_options = {f["fund_id"]: f"{f['symbol']} - {f['fund_name'][:40]}" for f in funds}

    selected_funds = st.multiselect(
        "Select Funds to Compare (max 5)",
        options=list(fund_options.keys()),
        format_func=lambda x: fund_options[x],
        max_selections=5,
        key="fa_compare",
    )

    if selected_funds:
        # Comparison table
        comparison = get_fund_comparison_table(con, selected_funds)

        if comparison:
            st.subheader("Performance Comparison")

            comp_df = pd.DataFrame(comparison)

            # Format returns as percentages
            for col in ["return_1W", "return_1M", "return_3M", "return_6M", "return_1Y"]:
                if col in comp_df.columns:
                    comp_df[col] = comp_df[col].apply(
                        lambda x: f"{x * 100:+.2f}%" if pd.notna(x) else "N/A"
                    )

            for col in ["vol_1M", "vol_3M"]:
                if col in comp_df.columns:
                    comp_df[col] = comp_df[col].apply(
                        lambda x: f"{x * 100:.2f}%" if pd.notna(x) else "N/A"
                    )

            display_cols = [
                "symbol", "category", "latest_nav",
                "return_1W", "return_1M", "return_3M",
                "vol_1M", "sharpe_ratio"
            ]
            display_cols = [c for c in display_cols if c in comp_df.columns]

            st.dataframe(
                comp_df[display_cols].rename(columns={
                    "symbol": "Symbol",
                    "category": "Category",
                    "latest_nav": "NAV",
                    "return_1W": "1W",
                    "return_1M": "1M",
                    "return_3M": "3M",
                    "vol_1M": "Vol (1M)",
                    "sharpe_ratio": "Sharpe",
                }),
                use_container_width=True,
            )

            # Normalized performance chart
            st.subheader("Normalized Performance (Base = 100)")

            days = st.slider("Chart Period (days)", 30, 365, 90, key="fa_days")
            from datetime import datetime, timedelta
            start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

            perf_df = compare_funds(con, selected_funds, start_date=start_date)

            if not perf_df.empty:
                st.line_chart(perf_df, height=400)
            else:
                st.info("Insufficient data for comparison chart.")

    else:
        st.info("Select funds above to compare their performance.")

    # =================================================================
    # INDIVIDUAL FUND ANALYTICS
    # =================================================================
    st.markdown("---")
    st.subheader("Individual Fund Analytics")

    selected_fund = st.selectbox(
        "Select Fund for Detailed Analytics",
        options=list(fund_options.keys()),
        format_func=lambda x: fund_options[x],
        key="fa_individual",
    )

    if selected_fund:
        analytics = get_mf_analytics(con, selected_fund)

        if not analytics.get("error"):
            col1, col2 = st.columns(2)

            with col1:
                st.markdown(f"**{analytics.get('fund_name', 'N/A')}**")
                st.caption(f"Category: {analytics.get('category', 'N/A')}")
                st.caption(f"AMC: {analytics.get('amc_name', 'N/A')}")

                if analytics.get("latest_nav"):
                    st.metric("Latest NAV", f"Rs. {analytics['latest_nav']:.4f}")

            with col2:
                # Key metrics
                metrics_data = []

                for period in ["1W", "1M", "3M", "6M", "1Y"]:
                    key = f"return_{period}"
                    if analytics.get(key) is not None:
                        metrics_data.append({
                            "Period": period,
                            "Return": f"{analytics[key] * 100:+.2f}%"
                        })

                if metrics_data:
                    st.dataframe(
                        pd.DataFrame(metrics_data),
                        use_container_width=True,
                        hide_index=True,
                    )

            # Risk metrics
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                vol = analytics.get("vol_1M")
                st.metric(
                    "Volatility (1M)",
                    f"{vol * 100:.2f}%" if vol else "N/A"
                )

            with col2:
                sharpe = analytics.get("sharpe_ratio")
                st.metric("Sharpe Ratio", f"{sharpe:.2f}" if sharpe else "N/A")

            with col3:
                dd = analytics.get("max_drawdown")
                st.metric(
                    "Max Drawdown",
                    f"{dd * 100:.2f}%" if dd else "N/A"
                )

            with col4:
                exp = analytics.get("expense_ratio")
                st.metric(
                    "Expense Ratio",
                    f"{exp:.2f}%" if exp else "N/A"
                )

        else:
            st.warning(f"No analytics available: {analytics.get('error')}")

    render_footer()
