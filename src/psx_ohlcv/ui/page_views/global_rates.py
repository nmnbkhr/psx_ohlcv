"""Global Reference Rates — SOFR, EFFR, SONIA, EUSTR, TONA + SOFR-KIBOR Spread."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta


def render_global_rates():
    """Main entry point for global reference rates page."""
    st.title("Global Reference Rates")
    st.caption("Post-LIBOR alternative reference rates (ARRs) — SOFR, EFFR, SONIA, EUSTR, TONA")

    from psx_ohlcv.db.connection import connect
    from psx_ohlcv.db import init_schema
    from psx_ohlcv.db.repositories.global_rates import ensure_tables

    con = connect()
    init_schema(con)
    ensure_tables(con)

    # Sync button
    _render_sync_controls(con)

    st.markdown("---")

    tab1, tab2, tab3, tab4 = st.tabs([
        "Rate Dashboard",
        "SOFR History",
        "SOFR-KIBOR Spread",
        "FCY Instruments",
    ])

    with tab1:
        _render_rate_dashboard(con)

    with tab2:
        _render_sofr_history(con)

    with tab3:
        _render_sofr_kibor_spread(con)

    with tab4:
        _render_fcy_instruments(con)

    # Footer
    st.markdown("---")
    st.caption(
        "Sources: [NY Fed](https://markets.newyorkfed.org/api) · "
        "[BoE](https://www.bankofengland.co.uk) · "
        "[ECB](https://data-api.ecb.europa.eu) · "
        "[BoJ](https://www.stat-search.boj.or.jp) | "
        f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    )


def _render_sync_controls(con):
    """Sync buttons for global rates."""
    with st.expander("Sync Global Rates"):
        col1, col2 = st.columns(2)
        with col1:
            count = st.number_input(
                "Days to fetch", min_value=5, max_value=1000, value=100,
                key="gr_sync_count"
            )
        with col2:
            if st.button("Sync All Global Rates", type="primary", key="gr_sync_btn"):
                with st.spinner("Fetching from NY Fed, BoE, ECB, BoJ..."):
                    try:
                        from psx_ohlcv.sources.global_rates_scraper import GlobalRatesScraper
                        scraper = GlobalRatesScraper()
                        stats = scraper.sync_all(con)
                        parts = [f"{k}: {v}" for k, v in stats.items()]
                        st.success(" | ".join(parts))
                        st.rerun()
                    except Exception as e:
                        st.error(f"Sync failed: {e}")


def _render_rate_dashboard(con):
    """Tab 1: Rate comparison dashboard."""
    from psx_ohlcv.db.repositories.global_rates import (
        get_all_latest_rates,
        get_rate_comparison,
    )

    st.markdown("### Global Rate Comparison")

    comparison = get_rate_comparison(con)

    # Summary metrics row
    cols = st.columns(7)
    rate_labels = [
        ("SOFR", "USD", comparison.get("SOFR")),
        ("EFFR", "USD", comparison.get("EFFR")),
        ("KIBOR 6M", "PKR", comparison.get("KIBOR_6M")),
        ("KONIA", "PKR", comparison.get("KONIA")),
        ("SONIA", "GBP", comparison.get("SONIA")),
        ("EUSTR", "EUR", comparison.get("EUSTR")),
        ("TONA", "JPY", comparison.get("TONA")),
    ]

    for col, (name, ccy, val) in zip(cols, rate_labels):
        with col:
            if val is not None:
                st.metric(label=f"{name} ({ccy})", value=f"{val:.4f}%")
            else:
                st.metric(label=f"{name} ({ccy})", value="N/A")

    # Detailed table
    st.markdown("### All Latest Rates")
    df = get_all_latest_rates(con)
    if df.empty:
        st.info("No global rate data. Click **Sync SOFR/EFFR (NY Fed)** above.")
        return

    # Show with change vs previous
    display_cols = ["date", "rate_name", "tenor", "currency", "rate", "volume", "source"]
    available_cols = [c for c in display_cols if c in df.columns]
    st.dataframe(
        df[available_cols],
        use_container_width=True,
        hide_index=True,
        column_config={
            "rate": st.column_config.NumberColumn("Rate (%)", format="%.4f"),
            "volume": st.column_config.NumberColumn("Volume ($B)", format="%.1f"),
        },
    )


def _render_sofr_history(con):
    """Tab 2: SOFR + EFFR history chart."""
    from psx_ohlcv.db.repositories.global_rates import get_rate_history

    st.markdown("### SOFR & EFFR History")

    days = st.selectbox("Period", [30, 60, 90, 180, 365], index=2, key="sofr_hist_days")
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    sofr_df = get_rate_history(con, rate_name="SOFR", tenor="ON", start_date=start, limit=0)
    effr_df = get_rate_history(con, rate_name="EFFR", tenor="ON", start_date=start, limit=0)

    if sofr_df.empty and effr_df.empty:
        st.info("No SOFR/EFFR data. Sync global rates first.")
        return

    # Dual chart: rates on top, volume bars on bottom
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        row_heights=[0.7, 0.3],
        subplot_titles=["Rate (%)", "SOFR Volume ($B)"],
    )

    if not sofr_df.empty:
        sofr_df = sofr_df.sort_values("date")
        fig.add_trace(
            go.Scatter(
                x=sofr_df["date"], y=sofr_df["rate"],
                name="SOFR", line=dict(color="#2196F3", width=2),
            ),
            row=1, col=1,
        )
        # Volume bars
        if "volume" in sofr_df.columns:
            vol = sofr_df.dropna(subset=["volume"])
            if not vol.empty:
                fig.add_trace(
                    go.Bar(
                        x=vol["date"], y=vol["volume"],
                        name="SOFR Volume", marker_color="rgba(33,150,243,0.3)",
                    ),
                    row=2, col=1,
                )

    if not effr_df.empty:
        effr_df = effr_df.sort_values("date")
        fig.add_trace(
            go.Scatter(
                x=effr_df["date"], y=effr_df["rate"],
                name="EFFR", line=dict(color="#FF9800", width=2, dash="dot"),
            ),
            row=1, col=1,
        )

    fig.update_layout(
        height=500,
        template="plotly_dark",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=40, r=20, t=40, b=20),
    )
    fig.update_yaxes(title_text="Rate (%)", row=1, col=1)
    fig.update_yaxes(title_text="$B", row=2, col=1)
    st.plotly_chart(fig, use_container_width=True)

    # Percentile band info
    if not sofr_df.empty and "percentile_25" in sofr_df.columns:
        latest = sofr_df.iloc[-1]
        p25 = latest.get("percentile_25")
        p75 = latest.get("percentile_75")
        if p25 is not None and p75 is not None:
            st.caption(
                f"Latest SOFR: {latest['rate']:.4f}% | "
                f"25th pctl: {p25:.4f}% | 75th pctl: {p75:.4f}% | "
                f"Date: {latest['date']}"
            )


def _render_sofr_kibor_spread(con):
    """Tab 3: SOFR-KIBOR spread analysis."""
    from psx_ohlcv.db.repositories.global_rates import get_sofr_kibor_spread

    st.markdown("### SOFR-KIBOR Spread Analysis")
    st.caption("Key metric for FX swap pricing — higher spread = wider forward points")

    days = st.selectbox("Period", [30, 60, 90, 180, 365], index=2, key="spread_days")
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    df = get_sofr_kibor_spread(con, start_date=start)

    if df.empty:
        st.info("No spread data. Ensure both KIBOR and SOFR are synced.")
        return

    # Filter to one tenor for charting
    tenors = sorted(df["tenor"].unique().tolist())
    sel_tenor = st.selectbox("KIBOR Tenor", tenors, index=tenors.index("6M") if "6M" in tenors else 0, key="spread_tenor")

    tenor_df = df[df["tenor"] == sel_tenor].copy()
    tenor_df = tenor_df.sort_values("date")

    if tenor_df.empty:
        st.warning(f"No data for tenor {sel_tenor}")
        return

    # Dual-axis: SOFR + KIBOR on left, spread on right
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Only plot rows where SOFR exists
    has_sofr = tenor_df.dropna(subset=["sofr_rate"])

    if not has_sofr.empty:
        fig.add_trace(
            go.Scatter(
                x=has_sofr["date"], y=has_sofr["sofr_rate"],
                name="SOFR", line=dict(color="#2196F3", width=2),
            ),
            secondary_y=False,
        )

    fig.add_trace(
        go.Scatter(
            x=tenor_df["date"], y=tenor_df["kibor_offer"],
            name=f"KIBOR {sel_tenor} (Offer)", line=dict(color="#FF9800", width=2),
        ),
        secondary_y=False,
    )

    if not has_sofr.empty:
        fig.add_trace(
            go.Bar(
                x=has_sofr["date"], y=has_sofr["spread_over_sofr"],
                name="Spread (bps)", marker_color="rgba(76,175,80,0.4)",
            ),
            secondary_y=True,
        )

    fig.update_layout(
        height=450,
        template="plotly_dark",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=40, r=40, t=40, b=20),
    )
    fig.update_yaxes(title_text="Rate (%)", secondary_y=False)
    fig.update_yaxes(title_text="Spread (ppts)", secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)

    # Data table
    st.markdown("#### Spread Data")
    display_df = tenor_df[["date", "kibor_bid", "kibor_offer", "sofr_rate", "spread_over_sofr", "usdpkr"]].copy()
    display_df.columns = ["Date", "KIBOR Bid", "KIBOR Offer", "SOFR", "Spread", "USD/PKR"]
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "KIBOR Bid": st.column_config.NumberColumn(format="%.4f"),
            "KIBOR Offer": st.column_config.NumberColumn(format="%.4f"),
            "SOFR": st.column_config.NumberColumn(format="%.4f"),
            "Spread": st.column_config.NumberColumn(format="%.4f"),
            "USD/PKR": st.column_config.NumberColumn(format="%.2f"),
        },
    )


def _render_fcy_instruments(con):
    """Tab 4: FCY-denominated instrument browser."""
    st.markdown("### FCY Instrument Browser")
    st.caption("Fixed income instruments denominated in foreign currencies")

    # Query FCY instruments from all three tables
    dfs = []

    # fi_instruments
    try:
        fi = pd.read_sql_query(
            """SELECT name, category, maturity_date, coupon_rate,
                      denomination_currency, reference_rate, spread_bps
               FROM fi_instruments
               WHERE denomination_currency IS NOT NULL AND denomination_currency != 'PKR'""",
            con,
        )
        if not fi.empty:
            fi["source_table"] = "fi_instruments"
            dfs.append(fi)
    except Exception:
        pass

    # bonds_master
    try:
        bonds = pd.read_sql_query(
            """SELECT symbol, issuer AS name, bond_type AS category, maturity_date, coupon_rate,
                      denomination_currency, reference_rate, spread_bps
               FROM bonds_master
               WHERE denomination_currency IS NOT NULL AND denomination_currency != 'PKR'""",
            con,
        )
        if not bonds.empty:
            bonds["source_table"] = "bonds_master"
            dfs.append(bonds)
    except Exception:
        pass

    # sukuk_master
    try:
        sukuk = pd.read_sql_query(
            """SELECT name, category, maturity_date, coupon_rate,
                      denomination_currency, reference_rate, spread_bps
               FROM sukuk_master
               WHERE denomination_currency IS NOT NULL AND denomination_currency != 'PKR'""",
            con,
        )
        if not sukuk.empty:
            sukuk["source_table"] = "sukuk_master"
            dfs.append(sukuk)
    except Exception:
        pass

    if not dfs:
        st.info(
            "No FCY-denominated instruments found. "
            "Instruments default to PKR denomination. "
            "Use SQL or admin tools to tag FCY instruments by updating "
            "`denomination_currency`, `reference_rate`, and `spread_bps` columns."
        )
        st.markdown(
            """**Example SQL:**
```sql
UPDATE fi_instruments
SET denomination_currency = 'USD',
    reference_rate = 'SOFR',
    spread_bps = 150
WHERE name LIKE '%Eurobond%';
```"""
        )
        return

    combined = pd.concat(dfs, ignore_index=True)
    st.dataframe(
        combined,
        use_container_width=True,
        hide_index=True,
        column_config={
            "coupon_rate": st.column_config.NumberColumn("Coupon (%)", format="%.2f"),
            "spread_bps": st.column_config.NumberColumn("Spread (bps)", format="%.0f"),
        },
    )
