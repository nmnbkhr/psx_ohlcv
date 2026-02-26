"""Live market dashboard — real-time market overview with auto-refresh."""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False
    st_autorefresh = None

from pakfindata.api_client import get_client
from pakfindata.ui.charts import make_market_breadth_chart, make_top_movers_chart
from pakfindata.ui.components.helpers import (
    format_volume,
    render_footer,
    render_market_status_badge,
)


def render_live_market():
    """Live market dashboard with auto-refresh."""

    # Auto-refresh every 60 seconds
    if HAS_AUTOREFRESH and st_autorefresh:
        st_autorefresh(interval=60000, limit=None, key="live_market_refresh")

    client = get_client()

    # =================================================================
    # HEADER
    # =================================================================
    h1, h2, h3 = st.columns([2, 1, 1])
    with h1:
        st.markdown("## \U0001f4e1 Live Market")
        st.caption("Real-time PSX data \u2022 Auto-refreshes every 60s")
    with h2:
        render_market_status_badge()
    with h3:
        if st.button("\U0001f504 Refresh Now", use_container_width=True):
            st.rerun()

    st.markdown("---")

    # =================================================================
    # KSE-100 INDEX
    # =================================================================
    try:
        kse100 = client.get_latest_kse100()
        if kse100:
            value = kse100.get("value", 0)
            change = kse100.get("change", 0) or 0
            change_pct = kse100.get("change_pct", 0) or 0

            if change > 0:
                color = "#00C853"
                arrow = "\u25b2"
                sign = "+"
            elif change < 0:
                color = "#FF1744"
                arrow = "\u25bc"
                sign = ""
            else:
                color = "#78909C"
                arrow = "\u25cf"
                sign = ""

            k1, k2, k3, k4 = st.columns([3, 1, 1, 1])
            with k1:
                st.markdown(f"""
                <div style="background: linear-gradient(135deg, rgba(33,150,243,0.15) 0%, rgba(33,150,243,0.05) 100%);
                            border: 1px solid rgba(33,150,243,0.3); border-radius: 12px; padding: 16px;">
                    <div style="font-size: 11px; color: #888;">KSE-100 Index</div>
                    <div style="display: flex; align-items: baseline; gap: 12px; flex-wrap: wrap;">
                        <span style="font-size: 28px; font-weight: 700; font-family: monospace;">{value:,.2f}</span>
                        <span style="font-size: 16px; font-weight: 600; color: {color}; font-family: monospace;">
                            {arrow} {sign}{change:,.2f} ({sign}{change_pct:.2f}%)
                        </span>
                    </div>
                    <div style="font-size: 10px; color: #666; margin-top: 4px;">
                        {kse100.get("index_date", "")}
                    </div>
                </div>
                """, unsafe_allow_html=True)

            with k2:
                high = kse100.get("high")
                st.metric("High", f"{high:,.0f}" if high else "N/A")
            with k3:
                low = kse100.get("low")
                st.metric("Low", f"{low:,.0f}" if low else "N/A")
            with k4:
                vol = kse100.get("volume")
                st.metric("Volume", format_volume(vol) if vol else "N/A")
    except Exception:
        st.info("KSE-100 index data not available.")

    st.markdown("---")

    # =================================================================
    # LOAD LIVE MARKET DATA
    # =================================================================
    live_data = client.get_live_market(limit=1000)

    if not live_data:
        st.warning(
            "No live market data. Fetch data from the Quote Monitor page first."
        )
        render_footer()
        return

    df = pd.DataFrame(live_data)

    # Ensure change_pct column exists
    if "change_pct" not in df.columns:
        if "change" in df.columns and "ldcp" in df.columns:
            df["change_pct"] = df.apply(
                lambda r: (r["change"] / r["ldcp"] * 100) if r.get("ldcp") else 0,
                axis=1,
            )
        else:
            df["change_pct"] = 0.0

    # =================================================================
    # MARKET BREADTH
    # =================================================================
    gainers = len(df[df["change_pct"] > 0])
    losers = len(df[df["change_pct"] < 0])
    unchanged = len(df[df["change_pct"] == 0])
    total = len(df)

    b1, b2, b3 = st.columns([1, 1, 1])

    with b1:
        st.subheader("Market Breadth")
        fig = make_market_breadth_chart(
            gainers=gainers, losers=losers, unchanged=unchanged, height=280,
        )
        st.plotly_chart(fig, use_container_width=True)

    # =================================================================
    # TOP 10 GAINERS & LOSERS
    # =================================================================
    with b2:
        st.subheader("Top 10 Gainers")
        top_g = df.nlargest(10, "change_pct")[["symbol", "change_pct"]].copy()
        if not top_g.empty:
            fig_g = make_top_movers_chart(
                top_g, title="", chart_type="gainers", height=280,
            )
            st.plotly_chart(fig_g, use_container_width=True)

    with b3:
        st.subheader("Top 10 Losers")
        top_l = df.nsmallest(10, "change_pct")[["symbol", "change_pct"]].copy()
        if not top_l.empty:
            fig_l = make_top_movers_chart(
                top_l, title="", chart_type="losers", height=280,
            )
            st.plotly_chart(fig_l, use_container_width=True)

    st.markdown("---")

    # =================================================================
    # KPI STRIP
    # =================================================================
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Symbols", f"{total:,}")
    m2.metric("\U0001f7e2 Gainers", f"{gainers}")
    m3.metric("\U0001f534 Losers", f"{losers}")

    vol_col = "volume" if "volume" in df.columns else None
    total_vol = df[vol_col].sum() if vol_col and vol_col in df.columns else 0
    m4.metric("Total Volume", format_volume(total_vol))

    st.markdown("---")

    # =================================================================
    # SECTOR HEATMAP
    # =================================================================
    try:
        # Need sector info — get from symbols table
        con = client.connection
        if con:
            sector_df = pd.read_sql_query("""
                SELECT rmc.symbol, rmc.change_pct, rmc.volume, s.sector as sector_code
                FROM regular_market_current rmc
                LEFT JOIN symbols s ON rmc.symbol = s.symbol
                WHERE s.sector IS NOT NULL
            """, con)

            if not sector_df.empty and "sector_code" in sector_df.columns:
                # Aggregate by sector
                sector_agg = sector_df.groupby("sector_code").agg(
                    avg_change=("change_pct", "mean"),
                    total_volume=("volume", "sum"),
                    count=("symbol", "count"),
                ).reset_index()

                if len(sector_agg) > 1:
                    st.subheader("\U0001f5fa Sector Heatmap")

                    # Get sector names
                    sectors_map = client.get_sectors()
                    sector_agg["sector_name"] = sector_agg["sector_code"].map(
                        sectors_map
                    ).fillna(sector_agg["sector_code"])
                    sector_agg["avg_change"] = sector_agg["avg_change"].round(2)

                    fig = px.treemap(
                        sector_agg,
                        path=["sector_name"],
                        values="count",
                        color="avg_change",
                        color_continuous_scale=["#FF1744", "#424242", "#00C853"],
                        color_continuous_midpoint=0,
                        hover_data={
                            "avg_change": ":.2f",
                            "count": True,
                            "total_volume": ":,.0f",
                        },
                        title="",
                    )
                    fig.update_layout(
                        height=400,
                        margin=dict(t=10, l=10, r=10, b=10),
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                    )
                    fig.update_traces(
                        textinfo="label+text",
                        texttemplate="%{label}<br>%{color:+.2f}%",
                    )
                    st.plotly_chart(fig, use_container_width=True)

                    st.markdown("---")
    except Exception:
        pass  # Sector heatmap is non-critical

    # =================================================================
    # DETAILED GAINERS & LOSERS TABLES
    # =================================================================
    t1, t2 = st.columns(2)

    with t1:
        st.subheader("\U0001f7e2 Top 10 Gainers")
        display_cols = ["symbol", "current", "change_pct", "volume"]
        display_cols = [c for c in display_cols if c in df.columns]
        if display_cols:
            g_table = df.nlargest(10, "change_pct")[display_cols].copy()
            st.dataframe(
                g_table,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "symbol": st.column_config.TextColumn("Symbol"),
                    "current": st.column_config.NumberColumn("Price", format="%.2f"),
                    "change_pct": st.column_config.NumberColumn("Change %", format="%.2f"),
                    "volume": st.column_config.NumberColumn("Volume", format="%,d"),
                },
            )

    with t2:
        st.subheader("\U0001f534 Top 10 Losers")
        if display_cols:
            l_table = df.nsmallest(10, "change_pct")[display_cols].copy()
            st.dataframe(
                l_table,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "symbol": st.column_config.TextColumn("Symbol"),
                    "current": st.column_config.NumberColumn("Price", format="%.2f"),
                    "change_pct": st.column_config.NumberColumn("Change %", format="%.2f"),
                    "volume": st.column_config.NumberColumn("Volume", format="%,d"),
                },
            )

    render_footer()
