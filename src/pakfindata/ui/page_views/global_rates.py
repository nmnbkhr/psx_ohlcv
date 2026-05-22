"""Global Reference Rates — SOFR, EFFR, SONIA, EUSTR, TONA + SOFR-KIBOR Spread.

Reads through the /v1 API client (Phase 1.7.B.3). The sync button still
calls the scraper directly because Phase 1.7 migrates reads only —
sync paths follow the Phase 1.6 worker-or-inline pattern unchanged.
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from datetime import datetime, timedelta
from plotly.subplots import make_subplots

from pakfindata.ui.api import client as api_client


@st.cache_data(ttl=3600, show_spinner=False)
def _cached_rate_comparison() -> dict:
    return api_client.get_rate_comparison() or {}


@st.cache_data(ttl=3600, show_spinner=False)
def _cached_all_latest_rates() -> pd.DataFrame:
    rows = api_client.get_global_rates_latest() or []
    return pd.DataFrame(rows) if rows else pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def _cached_rate_history(rate_name, tenor, start_date, limit) -> pd.DataFrame:
    rows = api_client.get_global_rate_history(
        rate_name=rate_name, tenor=tenor,
        from_=start_date, limit=limit,
    ) or []
    return pd.DataFrame(rows) if rows else pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def _cached_sofr_kibor_spread(start_date) -> pd.DataFrame:
    rows = api_client.get_sofr_kibor_spread(from_=start_date) or []
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def render_global_rates():
    """Main entry point for global reference rates page."""
    api_client.render_api_status_banner_if_down()

    st.title("Global Reference Rates")
    st.caption("Post-LIBOR alternative reference rates (ARRs) — SOFR, EFFR, SONIA, EUSTR, TONA")

    # Sync button (write path — uses local connection)
    _render_sync_controls()

    st.markdown("---")

    tab1, tab2, tab3, tab4 = st.tabs([
        "Rate Dashboard",
        "SOFR History",
        "SOFR-KIBOR Spread",
        "FCY Instruments",
    ])

    with tab1:
        _render_rate_dashboard()

    with tab2:
        _render_sofr_history()

    with tab3:
        _render_sofr_kibor_spread_tab()

    with tab4:
        _render_fcy_instruments()

    # Footer
    st.markdown("---")
    st.caption(
        "Sources: [NY Fed](https://markets.newyorkfed.org/api) · "
        "[BoE](https://www.bankofengland.co.uk) · "
        "[ECB](https://data-api.ecb.europa.eu) · "
        "[BoJ](https://www.stat-search.boj.or.jp) | "
        f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    )


def _render_sync_controls():
    """Sync buttons for global rates."""
    with st.expander("Sync Global Rates"):
        col1, col2 = st.columns(2)
        with col1:
            st.number_input(
                "Days to fetch", min_value=5, max_value=1000, value=100,
                key="gr_sync_count"
            )
        with col2:
            if st.button("Sync All Global Rates", type="primary", key="gr_sync_btn"):
                with st.spinner("Fetching from NY Fed, BoE, ECB, BoJ..."):
                    try:
                        from pakfindata.db.connection import connect
                        from pakfindata.sources.global_rates_scraper import GlobalRatesScraper
                        con = connect()
                        scraper = GlobalRatesScraper()
                        stats = scraper.sync_all(con)
                        st.cache_data.clear()
                        parts = [f"{k}: {v}" for k, v in stats.items()]
                        st.success(" | ".join(parts))
                        st.rerun()
                    except Exception as e:
                        st.error(f"Sync failed: {e}")


def _render_rate_dashboard():
    """Tab 1: Rate comparison dashboard."""
    st.markdown("### Global Rate Comparison")

    comparison = _cached_rate_comparison()

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
            # KONIA defense (Group C corruption pattern) — only show
            # values inside a plausible 0..50% band.
            display = (
                f"{val:.4f}%" if val is not None and 0 < val < 50 else "N/A"
            )
            st.metric(label=f"{name} ({ccy})", value=display)

    # Detailed table
    st.markdown("### All Latest Rates")
    df = _cached_all_latest_rates()
    if df.empty:
        st.info("No global rate data. Click **Sync All Global Rates** above.")
        return

    # Show with change vs previous
    display_cols = ["date", "rate_name", "tenor", "currency", "rate", "volume", "source"]
    available_cols = [c for c in display_cols if c in df.columns]
    st.dataframe(
        df[available_cols],
        width='stretch',
        hide_index=True,
        column_config={
            "rate": st.column_config.NumberColumn("Rate (%)", format="%.4f"),
            "volume": st.column_config.NumberColumn("Volume ($B)", format="%.1f"),
        },
    )


def _render_sofr_history():
    """Tab 2: SOFR + EFFR history chart."""
    st.markdown("### SOFR & EFFR History")

    days = st.selectbox("Period", [30, 60, 90, 180, 365], index=2, key="sofr_hist_days")
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    sofr_df = _cached_rate_history(rate_name="SOFR", tenor="ON", start_date=start, limit=0)
    effr_df = _cached_rate_history(rate_name="EFFR", tenor="ON", start_date=start, limit=0)

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
    st.plotly_chart(fig, width='stretch')

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


def _render_sofr_kibor_spread_tab():
    """Tab 3: SOFR-KIBOR spread analysis."""
    st.markdown("### SOFR-KIBOR Spread Analysis")
    st.caption("Key metric for FX swap pricing — higher spread = wider forward points")

    days = st.selectbox("Period", [30, 60, 90, 180, 365], index=2, key="spread_days")
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    df = _cached_sofr_kibor_spread(start_date=start)

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
    st.plotly_chart(fig, width='stretch')

    # Data table
    st.markdown("#### Spread Data")
    display_df = tenor_df[["date", "kibor_bid", "kibor_offer", "sofr_rate", "spread_over_sofr", "usdpkr"]].copy()
    display_df.columns = ["Date", "KIBOR Bid", "KIBOR Offer", "SOFR", "Spread", "USD/PKR"]
    st.dataframe(
        display_df,
        width='stretch',
        hide_index=True,
        column_config={
            "KIBOR Bid": st.column_config.NumberColumn(format="%.4f"),
            "KIBOR Offer": st.column_config.NumberColumn(format="%.4f"),
            "SOFR": st.column_config.NumberColumn(format="%.4f"),
            "Spread": st.column_config.NumberColumn(format="%.4f"),
            "USD/PKR": st.column_config.NumberColumn(format="%.2f"),
        },
    )


def _render_fcy_instruments():
    """Tab 4: FCY-denominated instrument browser via /v1/fi/fcy-instruments."""
    st.markdown("### FCY Instrument Browser")
    st.caption("Fixed income instruments denominated in foreign currencies")

    rows = api_client.get_fcy_instruments() or []
    if not rows:
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

    combined = pd.DataFrame(rows)
    st.dataframe(
        combined,
        width='stretch',
        hide_index=True,
        column_config={
            "coupon_rate": st.column_config.NumberColumn("Coupon (%)", format="%.2f"),
            "spread_bps": st.column_config.NumberColumn("Spread (bps)", format="%.0f"),
        },
    )
