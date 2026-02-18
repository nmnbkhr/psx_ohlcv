"""Treasury Market Dashboard — yield curves, auctions, and rate comparisons."""

from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from psx_ohlcv.sources.sbp_gsp import GSPScraper
from psx_ohlcv.sources.sbp_rates import SBPRatesScraper
from psx_ohlcv.sources.sbp_treasury import SBPTreasuryScraper
from psx_ohlcv.ui.components.helpers import get_connection, render_footer

# Tenor label → approximate days mapping
_TENOR_DAYS = {
    "1W": 7, "2W": 14, "1M": 30, "2M": 60, "3M": 91, "4M": 122,
    "6M": 182, "9M": 274, "1Y": 365, "2Y": 730, "3Y": 1095,
    "4Y": 1461, "5Y": 1826, "6Y": 2191, "7Y": 2557, "8Y": 2922,
    "9Y": 3287, "10Y": 3652, "12M": 365, "15Y": 5479, "20Y": 7305,
    "25Y": 9131, "30Y": 10957,
}

# Months → approximate days
_MONTHS_TO_DAYS = {
    1: 30, 2: 60, 3: 91, 4: 122, 6: 182, 9: 274, 12: 365, 24: 730,
    36: 1095, 48: 1461, 60: 1826, 72: 2191, 84: 2557, 96: 2922,
    108: 3287, 120: 3652, 180: 5479, 240: 7305, 300: 9131, 360: 10957,
}


def _days_ago(date_str: str) -> int:
    """Calculate days between a date string (YYYY-MM-DD) and today."""
    try:
        return (datetime.now() - datetime.strptime(str(date_str)[:10], "%Y-%m-%d")).days
    except (ValueError, TypeError):
        return -1


def _remaining_days(maturity_str: str) -> int | None:
    """Calculate remaining days to maturity from today."""
    try:
        dt = datetime.strptime(str(maturity_str)[:10], "%Y-%m-%d")
        delta = (dt - datetime.now()).days
        return delta if delta > 0 else 0
    except (ValueError, TypeError):
        return None


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

        st.divider()
        col1, col2 = st.columns(2)
        with col1:
            _render_pkisrv_curve(con)
        with col2:
            _render_pkfrv_bonds(con)

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

        st.markdown("##### MUFAP Yield Curves (PKRV/PKISRV/PKFRV)")
        if st.button("Sync MUFAP Rates", key="tsy_sync_mufap",
                      help="Download new PKRV/PKISRV/PKFRV files from MUFAP (incremental — skips dates already in DB)"):
            with st.spinner("Downloading & parsing new MUFAP rate files..."):
                try:
                    from psx_ohlcv.sources.mufap_rates import download_and_sync
                    result = download_and_sync(con)
                    st.success(
                        f"New: {result['downloaded']}, Skipped (on disk): {result['skipped']}, "
                        f"Skipped (in DB): {result.get('skipped_old', 0)} | "
                        f"PKRV: {result['pkrv_records']} records, "
                        f"PKISRV: {result['pkisrv_records']}, "
                        f"PKFRV: {result['pkfrv_records']}"
                    )
                    st.rerun()
                except Exception as e:
                    st.error(f"MUFAP sync failed: {e}")

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
    """Rate comparison metrics row with days-old tooltips."""
    cols = st.columns(4)

    # Policy Rate
    row = con.execute(
        "SELECT policy_rate, rate_date FROM sbp_policy_rates ORDER BY rate_date DESC LIMIT 1"
    ).fetchone()
    with cols[0]:
        if row:
            age = _days_ago(row["rate_date"])
            st.metric(
                "SBP Policy Rate", f"{row['policy_rate']:.1f}%",
                help=f"As of {row['rate_date']} ({age}d ago)",
            )
        else:
            st.metric("SBP Policy Rate", "N/A")

    # KIBOR 3M
    kibor = con.execute(
        "SELECT date, bid, offer FROM kibor_daily WHERE tenor = '3M' ORDER BY date DESC LIMIT 1"
    ).fetchone()
    with cols[1]:
        if kibor:
            age = _days_ago(kibor["date"])
            st.metric(
                "KIBOR 3M", f"{kibor['offer']:.2f}%",
                help=f"Offer rate | {kibor['date']} ({age}d ago) | 3M = ~91 days",
            )
        else:
            kibor = con.execute(
                "SELECT date, tenor, bid, offer FROM kibor_daily ORDER BY date DESC LIMIT 1"
            ).fetchone()
            if kibor:
                age = _days_ago(kibor["date"])
                days = _TENOR_DAYS.get(kibor["tenor"], "?")
                st.metric(
                    f"KIBOR {kibor['tenor']}", f"{kibor['offer']:.2f}%",
                    help=f"{kibor['date']} ({age}d ago) | {kibor['tenor']} = ~{days} days",
                )
            else:
                st.metric("KIBOR", "N/A")

    # T-Bill 3M yield
    tbill = con.execute(
        "SELECT cutoff_yield, auction_date FROM tbill_auctions"
        " WHERE tenor LIKE '%3M%' OR tenor LIKE '%3 M%'"
        " ORDER BY auction_date DESC LIMIT 1"
    ).fetchone()
    with cols[2]:
        if tbill:
            age = _days_ago(tbill["auction_date"])
            st.metric(
                "T-Bill 3M Yield", f"{tbill['cutoff_yield']:.2f}%",
                help=f"Auction {tbill['auction_date']} ({age}d ago) | 3M = ~91 days",
            )
        else:
            tbill = con.execute(
                "SELECT tenor, cutoff_yield, auction_date"
                " FROM tbill_auctions ORDER BY auction_date DESC LIMIT 1"
            ).fetchone()
            if tbill:
                age = _days_ago(tbill["auction_date"])
                days = _TENOR_DAYS.get(tbill["tenor"], "?")
                st.metric(
                    f"T-Bill {tbill['tenor']}", f"{tbill['cutoff_yield']:.2f}%",
                    help=f"Auction {tbill['auction_date']} ({age}d ago) | {tbill['tenor']} = ~{days} days",
                )
            else:
                st.metric("T-Bill Yield", "N/A")

    # KONIA
    konia = con.execute(
        "SELECT rate_pct, date FROM konia_daily ORDER BY date DESC LIMIT 1"
    ).fetchone()
    with cols[3]:
        if konia:
            age = _days_ago(konia["date"])
            st.metric(
                "KONIA", f"{konia['rate_pct']:.2f}%",
                help=f"Overnight rate | {konia['date']} ({age}d ago) | Duration: 1 day",
            )
        else:
            st.metric("KONIA", "N/A")


def _render_yield_curve(con):
    """PKRV yield curve chart with date picker and comparison."""
    st.markdown("### PKRV Yield Curve")

    # Get available dates
    dates = con.execute(
        "SELECT DISTINCT date FROM pkrv_daily ORDER BY date DESC"
    ).fetchall()

    if not dates:
        st.info("No PKRV yield curve data available. Sync MUFAP rates to fetch.")
        return

    date_list = [r["date"] for r in dates]

    # Date picker for primary curve
    selected_date = st.selectbox(
        "Curve date", date_list, index=0, key="pkrv_date_select"
    )

    # Comparison date (optional)
    compare_date = st.selectbox(
        "Compare with", ["None"] + date_list, index=0, key="pkrv_compare_date"
    )

    # Primary curve
    df = pd.read_sql_query(
        "SELECT tenor_months, yield_pct, change_bps FROM pkrv_daily"
        " WHERE date = ? ORDER BY tenor_months",
        con, params=(selected_date,),
    )

    if df.empty:
        st.info("No yield curve points for selected date")
        return

    tenor_labels = {
        1: "1M", 2: "2M", 3: "3M", 4: "4M", 6: "6M", 9: "9M",
        12: "1Y", 24: "2Y", 36: "3Y", 48: "4Y", 60: "5Y",
        72: "6Y", 84: "7Y", 96: "8Y", 108: "9Y", 120: "10Y",
        180: "15Y", 240: "20Y", 300: "25Y", 360: "30Y",
    }

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["tenor_months"], y=df["yield_pct"],
        mode="lines+markers", name=selected_date,
        line=dict(width=3, color="#FF6B35"),
        hovertemplate="%{text}<br>Yield: %{y:.4f}%<extra></extra>",
        text=[
            f"{tenor_labels.get(t, f'{t}M')} (~{_MONTHS_TO_DAYS.get(t, t * 30)}d)"
            for t in df["tenor_months"]
        ],
    ))

    # Comparison curve
    if compare_date != "None":
        cdf = pd.read_sql_query(
            "SELECT tenor_months, yield_pct FROM pkrv_daily"
            " WHERE date = ? ORDER BY tenor_months",
            con, params=(compare_date,),
        )
        if not cdf.empty:
            fig.add_trace(go.Scatter(
                x=cdf["tenor_months"], y=cdf["yield_pct"],
                mode="lines+markers", name=compare_date,
                line=dict(width=2, dash="dash", color="#4ECDC4"),
            ))

    fig.update_layout(
        xaxis_title="Tenor", yaxis_title="Yield (%)",
        height=420, margin=dict(l=20, r=20, t=30, b=20),
        legend=dict(orientation="h", y=-0.15),
        xaxis=dict(
            tickmode="array",
            tickvals=df["tenor_months"].tolist(),
            ticktext=[tenor_labels.get(t, f"{t}M") for t in df["tenor_months"]],
        ),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Data table
    with st.expander("Curve Data"):
        display = df.copy()
        display["Tenor"] = display["tenor_months"].map(
            lambda t: tenor_labels.get(t, f"{t}M")
        )
        display["Days"] = display["tenor_months"].map(
            lambda t: _MONTHS_TO_DAYS.get(t, t * 30)
        )
        st.dataframe(
            display[["Tenor", "tenor_months", "Days", "yield_pct", "change_bps"]].rename(
                columns={
                    "tenor_months": "Months", "yield_pct": "Yield (%)",
                    "change_bps": "Change (bps)",
                }
            ),
            use_container_width=True, hide_index=True,
        )


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

    df["days"] = df["tenor"].map(lambda t: _TENOR_DAYS.get(t.strip(), ""))

    st.caption(f"{row_count} total records")
    st.dataframe(
        df.rename(columns={
            "auction_date": "Date", "tenor": "Tenor", "days": "Days",
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

    df["days"] = df["tenor"].map(lambda t: _TENOR_DAYS.get(t.strip(), ""))

    st.caption(f"{row_count} total records")
    st.dataframe(
        df.rename(columns={
            "auction_date": "Date", "tenor": "Tenor", "days": "Days",
            "pib_type": "Type", "cutoff_yield": "Yield (%)",
            "coupon_rate": "Coupon (%)", "amount_accepted_billions": "Amt (B)",
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
            days = _TENOR_DAYS.get(tenor, "?")
            if not tdf.empty:
                fig.add_trace(go.Scatter(
                    x=tdf["date"], y=tdf["offer"],
                    mode="lines", name=f"KIBOR {tenor} (~{days}d)",
                    line=dict(width=2, color=colors.get(tenor, "#999")),
                    hovertemplate=f"KIBOR {tenor} (~{days}d)<br>"
                                  "Date: %{x}<br>Offer: %{y:.2f}%<extra></extra>",
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
        df_latest["days"] = df_latest["tenor"].map(lambda t: _TENOR_DAYS.get(t.strip(), ""))
        st.markdown("**Latest KIBOR Rates**")
        st.dataframe(
            df_latest.rename(columns={
                "date": "Date", "tenor": "Tenor", "days": "Days",
                "bid": "Bid (%)", "offer": "Offer (%)",
            }),
            use_container_width=True, hide_index=True,
        )


def _render_pkisrv_curve(con):
    """PKISRV (Islamic Revaluation Rate) yield curve."""
    st.markdown("### PKISRV (Islamic Yield Curve)")

    row_count_row = con.execute(
        "SELECT COUNT(*) as cnt FROM pkisrv_daily"
    ).fetchone()
    row_count = row_count_row["cnt"] if row_count_row else 0

    if row_count == 0:
        st.info("No PKISRV data. Sync MUFAP rates to fetch Islamic yield curve data.")
        return

    # Available dates
    dates = con.execute(
        "SELECT DISTINCT date FROM pkisrv_daily ORDER BY date DESC"
    ).fetchall()
    date_list = [r["date"] for r in dates]

    selected_date = st.selectbox(
        "Curve date", date_list, index=0, key="pkisrv_date_select"
    )

    df = pd.read_sql_query(
        "SELECT tenor, yield_pct FROM pkisrv_daily WHERE date = ? ORDER BY tenor",
        con, params=(selected_date,),
    )

    if df.empty:
        st.info("No data points for selected date")
        return

    df["days"] = df["tenor"].map(lambda t: _TENOR_DAYS.get(t.strip(), 9999))
    df = df.sort_values("days").reset_index(drop=True)

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["tenor"], y=df["yield_pct"],
        mode="lines+markers", name=f"PKISRV ({selected_date})",
        line=dict(width=3, color="#2ECC71"),
        marker=dict(size=8),
        hovertemplate="%{x} (~%{customdata}d)<br>Yield: %{y:.4f}%<extra></extra>",
        customdata=df["days"],
    ))

    fig.update_layout(
        xaxis_title="Tenor", yaxis_title="Yield (%)",
        height=380, margin=dict(l=20, r=20, t=30, b=20),
    )
    st.plotly_chart(fig, use_container_width=True)

    st.caption(f"{row_count} total records | {len(date_list)} dates")

    with st.expander("PKISRV Data"):
        st.dataframe(
            df.rename(columns={"tenor": "Tenor", "days": "Days", "yield_pct": "Yield (%)"}),
            use_container_width=True, hide_index=True,
        )


def _render_pkfrv_bonds(con):
    """PKFRV (Floating Rate Bond Valuations) table and chart."""
    st.markdown("### PKFRV (Floating Rate Bonds)")

    row_count_row = con.execute(
        "SELECT COUNT(*) as cnt FROM pkfrv_daily"
    ).fetchone()
    row_count = row_count_row["cnt"] if row_count_row else 0

    if row_count == 0:
        st.info("No PKFRV data. Sync MUFAP rates to fetch floating rate bond valuations.")
        return

    # Available dates
    dates = con.execute(
        "SELECT DISTINCT date FROM pkfrv_daily ORDER BY date DESC"
    ).fetchall()
    date_list = [r["date"] for r in dates]

    selected_date = st.selectbox(
        "Valuation date", date_list, index=0, key="pkfrv_date_select"
    )

    df = pd.read_sql_query(
        "SELECT bond_code, issue_date, maturity_date, coupon_frequency, fma_price"
        " FROM pkfrv_daily WHERE date = ? ORDER BY bond_code",
        con, params=(selected_date,),
    )

    if df.empty:
        st.info("No bonds for selected date")
        return

    # Calculate remaining days to maturity
    df["remaining_days"] = df["maturity_date"].apply(_remaining_days)

    st.caption(f"{len(df)} bonds on {selected_date} | {row_count} total records | {len(date_list)} dates")

    # FMA price chart (bar chart sorted by maturity)
    df_chart = df[df["fma_price"].notna()].copy()
    if not df_chart.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df_chart["bond_code"], y=df_chart["fma_price"],
            marker_color="#3498DB",
            customdata=df_chart[["maturity_date", "remaining_days"]].values,
            hovertemplate=(
                "<b>%{x}</b><br>FMA: %{y:.4f}<br>"
                "Maturity: %{customdata[0]}<br>"
                "Remaining: %{customdata[1]} days<extra></extra>"
            ),
        ))
        fig.update_layout(
            xaxis_title="Bond", yaxis_title="FMA Price",
            height=380, margin=dict(l=20, r=20, t=30, b=20),
            xaxis_tickangle=-45,
        )
        st.plotly_chart(fig, use_container_width=True)

    # Data table
    with st.expander("PKFRV Bond Data"):
        st.dataframe(
            df.rename(columns={
                "bond_code": "Bond", "issue_date": "Issue Date",
                "maturity_date": "Maturity", "remaining_days": "Rem. Days",
                "coupon_frequency": "Coupon Freq", "fma_price": "FMA Price",
            }),
            use_container_width=True, hide_index=True,
        )

    # Price history for a selected bond
    bonds = df["bond_code"].tolist()
    if bonds:
        selected_bond = st.selectbox("Bond history", bonds, key="pkfrv_bond_select")
        if selected_bond:
            hist = pd.read_sql_query(
                "SELECT date, fma_price FROM pkfrv_daily"
                " WHERE bond_code = ? AND fma_price IS NOT NULL ORDER BY date",
                con, params=(selected_bond,),
            )
            if len(hist) > 1:
                fig2 = go.Figure()
                fig2.add_trace(go.Scatter(
                    x=hist["date"], y=hist["fma_price"],
                    mode="lines", name=selected_bond,
                    line=dict(width=2, color="#E74C3C"),
                ))
                fig2.update_layout(
                    xaxis_title="Date", yaxis_title="FMA Price",
                    height=300, margin=dict(l=20, r=20, t=30, b=20),
                )
                st.plotly_chart(fig2, use_container_width=True)
