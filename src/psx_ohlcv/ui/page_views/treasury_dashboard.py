"""Treasury Market Dashboard — yield curves, auctions, and rate comparisons."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from psx_ohlcv.sources.sbp_gsp import GSPScraper
from psx_ohlcv.sources.sbp_rates import SBPRatesScraper
from psx_ohlcv.sources.sbp_treasury import SBPTreasuryScraper
from psx_ohlcv.ui.components.helpers import get_connection, render_footer


def render_treasury_dashboard():
    """Treasury Market dashboard with yield curves, auctions, and rates."""
    st.markdown("## Treasury Market Dashboard")

    con = get_connection()
    if con is None:
        st.error("Database connection not available")
        return

    try:
        _render_rate_metrics(con)
        st.divider()

        col1, col2 = st.columns(2)
        with col1:
            _render_yield_curve(con)
        with col2:
            _render_rate_history(con)

        st.divider()

        col1, col2 = st.columns(2)
        with col1:
            _render_tbill_auctions(con)
        with col2:
            _render_pib_auctions(con)

        st.divider()
        _render_kibor_history(con)

    except Exception as e:
        st.error(f"Error loading treasury data: {e}")

    # Sync section
    st.markdown("---")
    with st.expander("Sync Treasury Data"):
        st.markdown("##### Daily Sync (latest snapshot from SBP PMA page)")
        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button("Sync T-Bill / PIB", type="primary", key="tsy_sync_treasury"):
                with st.spinner("Syncing treasury auctions from SBP..."):
                    try:
                        result = SBPTreasuryScraper().sync_treasury(con)
                        st.success(
                            f"T-Bills: {result['tbills_ok']}, PIBs: {result['pibs_ok']}, "
                            f"Failed: {result['failed']}"
                        )
                        st.rerun()
                    except Exception as e:
                        st.error(f"Sync failed: {e}")

        with col2:
            if st.button("Sync Rates (KIBOR/KONIA/PKRV)", key="tsy_sync_rates"):
                with st.spinner("Syncing rates from SBP..."):
                    try:
                        result = SBPRatesScraper().sync_rates(con)
                        st.success(
                            f"KIBOR: {result['kibor_ok']}, PKRV points: {result['pkrv_points']}, "
                            f"KONIA: {'OK' if result['konia_ok'] else 'N/A'}"
                        )
                        st.rerun()
                    except Exception as e:
                        st.error(f"Sync failed: {e}")

        with col3:
            if st.button("Sync GIS Auctions", key="tsy_sync_gis"):
                with st.spinner("Syncing GIS auctions from SBP..."):
                    try:
                        result = GSPScraper().sync_gis(con)
                        st.success(f"GIS auctions: {result.get('ok', 0)} synced")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Sync failed: {e}")

        st.markdown("##### Historical Backfill (SBP PDFs)")
        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button("Backfill SIR PDF", key="tsy_backfill_sir",
                          help="T-Bills, PIBs, KIBOR, GIS from SBP SIR PDF (~2-4 years)"):
                with st.spinner("Downloading & parsing SIR PDF..."):
                    try:
                        from psx_ohlcv.sources.sbp_sir import SBPSirScraper
                        counts = SBPSirScraper().sync_sir(con)
                        st.success(
                            f"T-Bills: {counts['tbills']}, PIBs: {counts['pibs']}, "
                            f"KIBOR: {counts['kibor']}, GIS: {counts['gis']}"
                        )
                        st.rerun()
                    except Exception as e:
                        st.error(f"SIR backfill failed: {e}")

        with col2:
            if st.button("Backfill PIB Archive", key="tsy_backfill_pib",
                          help="All PIB auctions from Dec 2000 to present (~42 page PDF)"):
                with st.spinner("Downloading & parsing PIB archive PDF (42 pages)..."):
                    try:
                        from psx_ohlcv.sources.sbp_pib_archive import SBPPibArchiveScraper
                        counts = SBPPibArchiveScraper().sync_pib_archive(con)
                        st.success(
                            f"PIB records: {counts['inserted']}/{counts['total']} inserted"
                        )
                        st.rerun()
                    except Exception as e:
                        st.error(f"PIB archive backfill failed: {e}")

        with col3:
            _render_kibor_backfill_button()

    render_footer()


def _render_kibor_backfill_button():
    """KIBOR history backfill button with progress display."""
    from psx_ohlcv.sources.sbp_kibor_history import (
        is_kibor_history_sync_running,
        read_kibor_sync_progress,
        start_kibor_history_sync,
    )

    if is_kibor_history_sync_running():
        progress = read_kibor_sync_progress()
        if progress:
            pct = progress["current"] / max(progress["total_days"], 1)
            st.progress(pct, text=f"KIBOR: {progress['current_date']} "
                        f"({progress['records_inserted']} records)")
        else:
            st.info("KIBOR history sync running...")
    else:
        progress = read_kibor_sync_progress()
        if progress and progress.get("status") == "completed":
            st.caption(
                f"Last run: {progress['records_inserted']} records, "
                f"{progress['dates_processed']} dates"
            )

        start_year = st.number_input(
            "Start year", min_value=2008, max_value=2026, value=2024,
            key="kibor_start_year", help="Daily KIBOR PDFs from SBP (2008-present)"
        )
        if st.button("Start KIBOR History Sync", key="tsy_backfill_kibor"):
            started = start_kibor_history_sync(start_year=int(start_year))
            if started:
                st.success("KIBOR history sync started in background")
                st.rerun()
            else:
                st.warning("KIBOR sync already running")


def _render_rate_metrics(con):
    """Rate comparison metrics row."""
    cols = st.columns(4)

    # Policy Rate
    row = con.execute(
        "SELECT policy_rate, rate_date FROM sbp_policy_rates ORDER BY rate_date DESC LIMIT 1"
    ).fetchone()
    with cols[0]:
        if row:
            st.metric("SBP Policy Rate", f"{row['policy_rate']:.1f}%", help=f"As of {row['rate_date']}")
        else:
            st.metric("SBP Policy Rate", "N/A")

    # KIBOR 3M
    kibor = con.execute(
        "SELECT bid, offer FROM kibor_daily WHERE tenor = '3M' ORDER BY date DESC LIMIT 1"
    ).fetchone()
    with cols[1]:
        if kibor:
            st.metric("KIBOR 3M", f"{kibor['offer']:.2f}%", help="Offer rate")
        else:
            kibor = con.execute(
                "SELECT tenor, bid, offer FROM kibor_daily ORDER BY date DESC LIMIT 1"
            ).fetchone()
            if kibor:
                st.metric(f"KIBOR {kibor['tenor']}", f"{kibor['offer']:.2f}%")
            else:
                st.metric("KIBOR", "N/A")

    # T-Bill 3M yield
    tbill = con.execute(
        "SELECT cutoff_yield FROM tbill_auctions"
        " WHERE tenor LIKE '%3M%' OR tenor LIKE '%3 M%'"
        " ORDER BY auction_date DESC LIMIT 1"
    ).fetchone()
    with cols[2]:
        if tbill:
            st.metric("T-Bill 3M Yield", f"{tbill['cutoff_yield']:.2f}%")
        else:
            tbill = con.execute(
                "SELECT tenor, cutoff_yield FROM tbill_auctions ORDER BY auction_date DESC LIMIT 1"
            ).fetchone()
            if tbill:
                st.metric(f"T-Bill {tbill['tenor']}", f"{tbill['cutoff_yield']:.2f}%")
            else:
                st.metric("T-Bill Yield", "N/A")

    # KONIA
    konia = con.execute(
        "SELECT rate_pct, date FROM konia_daily ORDER BY date DESC LIMIT 1"
    ).fetchone()
    with cols[3]:
        if konia:
            st.metric("KONIA", f"{konia['rate_pct']:.2f}%", help=f"As of {konia['date']}")
        else:
            st.metric("KONIA", "N/A")


def _render_yield_curve(con):
    """PKRV yield curve chart with comparison dates."""
    st.markdown("### PKRV Yield Curve")

    # Get available dates
    dates = con.execute(
        "SELECT DISTINCT date FROM pkrv_daily ORDER BY date DESC LIMIT 10"
    ).fetchall()

    if not dates:
        st.info("No PKRV yield curve data available. Run `psxsync rates yield-curve` to fetch.")
        return

    date_list = [r["date"] for r in dates]
    latest_date = date_list[0]

    # Current curve
    df = pd.read_sql_query(
        "SELECT tenor_months, yield_pct FROM pkrv_daily WHERE date = ? ORDER BY tenor_months",
        con, params=(latest_date,),
    )

    if df.empty:
        st.info("No yield curve points available")
        return

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["tenor_months"], y=df["yield_pct"],
        mode="lines+markers", name=f"Current ({latest_date})",
        line=dict(width=3, color="#FF6B35"),
    ))

    # Comparison dates from yield_curve_points table
    comp_dates = con.execute(
        "SELECT DISTINCT curve_date FROM yield_curve_points ORDER BY curve_date DESC LIMIT 5"
    ).fetchall()
    colors = ["#4ECDC4", "#45B7D1", "#96CEB4", "#FFEAA7", "#DDA0DD"]
    for i, row in enumerate(comp_dates):
        cdf = pd.read_sql_query(
            "SELECT tenor_months, yield_rate as yield_pct FROM yield_curve_points"
            " WHERE curve_date = ? ORDER BY tenor_months",
            con, params=(row["curve_date"],),
        )
        if not cdf.empty:
            fig.add_trace(go.Scatter(
                x=cdf["tenor_months"], y=cdf["yield_pct"],
                mode="lines+markers", name=row["curve_date"],
                line=dict(width=1, dash="dash", color=colors[i % len(colors)]),
            ))

    fig.update_layout(
        xaxis_title="Tenor (Months)", yaxis_title="Yield (%)",
        height=400, margin=dict(l=20, r=20, t=30, b=20),
        legend=dict(orientation="h", y=-0.15),
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_rate_history(con):
    """Multi-line rate history chart."""
    st.markdown("### Rate History")

    fig = go.Figure()

    # Policy rate
    df = pd.read_sql_query(
        "SELECT rate_date as date, policy_rate as rate FROM sbp_policy_rates ORDER BY rate_date",
        con,
    )
    if not df.empty:
        fig.add_trace(go.Scatter(
            x=df["date"], y=df["rate"],
            mode="lines+markers", name="Policy Rate",
            line=dict(width=2, color="#FF6B35"),
        ))

    # KIBOR (pick one representative tenor)
    df = pd.read_sql_query(
        "SELECT date, offer as rate FROM kibor_daily WHERE tenor = '3M' ORDER BY date", con,
    )
    if df.empty:
        df = pd.read_sql_query(
            "SELECT date, offer as rate FROM kibor_daily ORDER BY date", con,
        )
    if not df.empty:
        fig.add_trace(go.Scatter(
            x=df["date"], y=df["rate"],
            mode="lines+markers", name="KIBOR 3M",
            line=dict(width=2, color="#4ECDC4"),
        ))

    # T-Bill 3M cutoff yield
    df = pd.read_sql_query(
        "SELECT auction_date as date, cutoff_yield as rate"
        " FROM tbill_auctions WHERE tenor = '3M' ORDER BY auction_date",
        con,
    )
    if not df.empty:
        fig.add_trace(go.Scatter(
            x=df["date"], y=df["rate"],
            mode="lines+markers", name="T-Bill 3M",
            line=dict(width=2, color="#96CEB4"),
        ))

    # KONIA
    df = pd.read_sql_query(
        "SELECT date, rate_pct as rate FROM konia_daily ORDER BY date", con,
    )
    if not df.empty:
        fig.add_trace(go.Scatter(
            x=df["date"], y=df["rate"],
            mode="lines+markers", name="KONIA",
            line=dict(width=2, color="#45B7D1"),
        ))

    if fig.data:
        fig.update_layout(
            xaxis_title="Date", yaxis_title="Rate (%)",
            height=400, margin=dict(l=20, r=20, t=30, b=20),
            legend=dict(orientation="h", y=-0.15),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No rate history data available")


def _render_tbill_auctions(con):
    """T-Bill auction results table with tenor filter."""
    st.markdown("### T-Bill Auctions")

    row_count = con.execute("SELECT COUNT(*) FROM tbill_auctions").fetchone()[0]
    if row_count == 0:
        st.info("No T-Bill auction data. Use Sync Treasury Data below to fetch.")
        return

    tenors = [r[0] for r in con.execute(
        "SELECT DISTINCT tenor FROM tbill_auctions ORDER BY tenor"
    ).fetchall()]

    selected_tenor = st.selectbox(
        "Tenor", ["All"] + tenors, key="tbill_tenor_filter"
    )

    where = "WHERE tenor = ?" if selected_tenor != "All" else ""
    params = (selected_tenor,) if selected_tenor != "All" else ()

    df = pd.read_sql_query(
        f"""SELECT auction_date, tenor, cutoff_yield, weighted_avg_yield
           FROM tbill_auctions {where} ORDER BY auction_date DESC LIMIT 50""",
        con, params=params,
    )

    st.caption(f"{row_count} total records")
    st.dataframe(
        df.rename(columns={
            "auction_date": "Date", "tenor": "Tenor",
            "cutoff_yield": "Cutoff (%)", "weighted_avg_yield": "WA Yield (%)",
        }),
        use_container_width=True, hide_index=True,
    )


def _render_pib_auctions(con):
    """PIB auction results table with tenor filter."""
    st.markdown("### PIB Auctions")

    row_count = con.execute("SELECT COUNT(*) FROM pib_auctions").fetchone()[0]
    if row_count == 0:
        st.info("No PIB auction data. Use Sync Treasury Data below to fetch.")
        return

    tenors = [r[0] for r in con.execute(
        "SELECT DISTINCT tenor FROM pib_auctions ORDER BY tenor"
    ).fetchall()]

    selected_tenor = st.selectbox(
        "Tenor", ["All"] + tenors, key="pib_tenor_filter"
    )

    where = "WHERE tenor = ?" if selected_tenor != "All" else ""
    params = (selected_tenor,) if selected_tenor != "All" else ()

    df = pd.read_sql_query(
        f"""SELECT auction_date, tenor, pib_type, cutoff_yield,
                   coupon_rate, amount_accepted_billions
           FROM pib_auctions {where} ORDER BY auction_date DESC LIMIT 50""",
        con, params=params,
    )

    st.caption(f"{row_count} total records")
    st.dataframe(
        df.rename(columns={
            "auction_date": "Date", "tenor": "Tenor", "pib_type": "Type",
            "cutoff_yield": "Yield (%)", "coupon_rate": "Coupon (%)",
            "amount_accepted_billions": "Amt (B)",
        }),
        use_container_width=True, hide_index=True,
    )


def _render_kibor_history(con):
    """KIBOR historical rates table and chart."""
    st.markdown("### KIBOR History")

    row_count = con.execute("SELECT COUNT(*) FROM kibor_daily").fetchone()[0]
    if row_count == 0:
        st.info("No KIBOR history. Use backfill buttons below to load.")
        return

    st.caption(f"{row_count} total records")

    # Chart: KIBOR offer rates over time for key tenors
    df_chart = pd.read_sql_query(
        """SELECT date, tenor, offer FROM kibor_daily
           WHERE tenor IN ('1M', '3M', '6M', '1Y') AND offer IS NOT NULL
           ORDER BY date""",
        con,
    )

    if not df_chart.empty:
        fig = go.Figure()
        colors = {"1M": "#FF6B35", "3M": "#4ECDC4", "6M": "#45B7D1", "1Y": "#96CEB4"}
        for tenor in ["1M", "3M", "6M", "1Y"]:
            tdf = df_chart[df_chart["tenor"] == tenor]
            if not tdf.empty:
                fig.add_trace(go.Scatter(
                    x=tdf["date"], y=tdf["offer"],
                    mode="lines", name=f"KIBOR {tenor}",
                    line=dict(width=2, color=colors.get(tenor, "#999")),
                ))
        fig.update_layout(
            xaxis_title="Date", yaxis_title="Offer Rate (%)",
            height=350, margin=dict(l=20, r=20, t=30, b=20),
            legend=dict(orientation="h", y=-0.15),
        )
        st.plotly_chart(fig, use_container_width=True)

    # Table: latest rates per tenor
    df_latest = pd.read_sql_query(
        """SELECT k.date, k.tenor, k.bid, k.offer
           FROM kibor_daily k
           INNER JOIN (SELECT tenor, MAX(date) as max_date FROM kibor_daily GROUP BY tenor) m
             ON k.tenor = m.tenor AND k.date = m.max_date
           ORDER BY k.tenor""",
        con,
    )

    if not df_latest.empty:
        st.markdown("**Latest KIBOR Rates**")
        st.dataframe(
            df_latest.rename(columns={
                "date": "Date", "tenor": "Tenor",
                "bid": "Bid (%)", "offer": "Offer (%)",
            }),
            use_container_width=True, hide_index=True,
        )
