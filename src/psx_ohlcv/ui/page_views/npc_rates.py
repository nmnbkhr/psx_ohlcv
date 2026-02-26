"""Naya Pakistan Certificates (NPC) — Rates, Yield Curves, Cross-Currency Analytics."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime


def render_npc_rates():
    """Main entry point for NPC rates page."""
    st.title("Naya Pakistan Certificates (NPC)")
    st.caption("Sovereign FCY instruments | SBP-administered | Roshan Digital Accounts")

    from psx_ohlcv.db.connection import connect
    from psx_ohlcv.db import init_schema
    from psx_ohlcv.db.repositories.npc_rates import ensure_tables

    con = connect()
    init_schema(con)
    ensure_tables(con)

    # Sync controls
    _render_sync_controls(con)

    st.markdown("---")

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Current Rates",
        "Yield Curves",
        "vs Global RFR",
        "Carry Trade",
        "Multi-Currency Dashboard",
    ])

    with tab1:
        _render_current_rates(con)
    with tab2:
        _render_yield_curves(con)
    with tab3:
        _render_rfr_spread(con)
    with tab4:
        _render_carry_trade(con)
    with tab5:
        _render_dashboard(con)

    st.markdown("---")
    st.caption(
        "Source: [SBP NPC](https://www.sbp.org.pk/NPC-/page-npc.html) | "
        f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    )


def _render_sync_controls(con):
    """Sync button for NPC rates."""
    with st.expander("Sync NPC Rates"):
        col1, col2 = st.columns([3, 1])
        with col2:
            if st.button("Sync from SBP", type="primary", key="npc_sync_btn"):
                with st.spinner("Fetching from SBP..."):
                    try:
                        from psx_ohlcv.sources.npc_rates_scraper import NPCRatesScraper
                        scraper = NPCRatesScraper()
                        count = scraper.sync(con, force=True)
                        if count > 0:
                            st.success(f"Stored {count} NPC rate records")
                        else:
                            st.info("NPC rates unchanged or scrape failed")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Sync failed: {e}")


def _render_current_rates(con):
    """Tab 1: Current NPC rates as a pivot table."""
    from psx_ohlcv.db.repositories.npc_rates import get_latest_npc_rates

    st.markdown("### Current NPC Rates")

    df = get_latest_npc_rates(con)
    if df.empty:
        st.info("No NPC rate data. Click **Sync from SBP** above.")
        return

    # Pivot: rows=currency, columns=tenor
    pivot = df.pivot_table(
        values="rate", index="currency", columns="tenor", aggfunc="first"
    )
    # Reorder columns
    tenor_order = ["3M", "6M", "12M", "3Y", "5Y"]
    pivot = pivot.reindex(columns=[t for t in tenor_order if t in pivot.columns])
    # Reorder rows
    ccy_order = ["USD", "GBP", "EUR", "PKR"]
    pivot = pivot.reindex([c for c in ccy_order if c in pivot.index])

    try:
        st.dataframe(
            pivot.style.format("{:.2f}%").background_gradient(cmap="RdYlGn", axis=None),
            use_container_width=True,
        )
    except ImportError:
        st.dataframe(pivot.style.format("{:.2f}%"), use_container_width=True)

    # Show effective date if available
    if "effective_date" in df.columns:
        eff = df["effective_date"].dropna().unique()
        if len(eff) > 0:
            st.caption(f"Effective date: {', '.join(str(e) for e in eff)}")

    # Show date of last scrape
    if "date" in df.columns:
        st.caption(f"Last scraped: {df['date'].iloc[0]}")


def _render_yield_curves(con):
    """Tab 2: NPC yield curves for all currencies."""
    from psx_ohlcv.db.repositories.npc_rates import get_npc_yield_curve

    st.markdown("### NPC Yield Curves")

    tenor_labels = ["3M", "6M", "12M", "3Y", "5Y"]
    tenor_keys = ["rate_3m", "rate_6m", "rate_12m", "rate_3y", "rate_5y"]
    # X positions for plotting (months)
    tenor_x = [3, 6, 12, 36, 60]

    fig = go.Figure()
    colors = {"USD": "#2196F3", "GBP": "#FF9800", "EUR": "#4CAF50", "PKR": "#E91E63"}

    for ccy in ["USD", "GBP", "EUR", "PKR"]:
        curve = get_npc_yield_curve(con, currency=ccy)
        if not curve:
            continue
        rates = [curve.get(k) for k in tenor_keys]
        if not any(r is not None for r in rates):
            continue
        fig.add_trace(go.Scatter(
            x=tenor_labels,
            y=rates,
            name=ccy,
            mode="lines+markers",
            line=dict(color=colors.get(ccy, "#999"), width=2),
            marker=dict(size=8),
        ))

    if not fig.data:
        st.info("No yield curve data available.")
        return

    fig.update_layout(
        height=400,
        template="plotly_dark",
        xaxis_title="Tenor",
        yaxis_title="Rate (%)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=40, r=20, t=20, b=40),
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_rfr_spread(con):
    """Tab 3: NPC vs Global RFR spread."""
    from psx_ohlcv.db.repositories.npc_rates import get_npc_vs_rfr_spread

    st.markdown("### NPC vs Global Risk-Free Rates")
    st.caption("NPC sovereign credit premium over SOFR (USD), SONIA (GBP), EUSTR (EUR)")

    df = get_npc_vs_rfr_spread(con)
    if df.empty:
        st.info("No spread data. Ensure NPC and global rates are synced.")
        return

    # Filter to 12M tenor for the chart
    tenor = st.selectbox("Tenor", ["3M", "6M", "12M", "3Y", "5Y"], index=2, key="npc_rfr_tenor")
    tenor_df = df[df["tenor"] == tenor].copy()

    if tenor_df.empty:
        st.warning(f"No data for tenor {tenor}")
        return

    # Grouped bar chart: NPC rate vs RFR for each currency
    fig = go.Figure()
    currencies = tenor_df["currency"].unique()

    fig.add_trace(go.Bar(
        x=currencies,
        y=tenor_df["npc_rate"],
        name=f"NPC {tenor}",
        marker_color="#2196F3",
    ))
    fig.add_trace(go.Bar(
        x=currencies,
        y=tenor_df["rfr_rate"].fillna(0),
        name="Global RFR",
        marker_color="#FF9800",
    ))

    # Add premium annotations
    for _, row in tenor_df.iterrows():
        premium = row.get("npc_premium_over_rfr", 0)
        if premium and premium > 0:
            fig.add_annotation(
                x=row["currency"],
                y=row["npc_rate"],
                text=f"+{premium:.2f}%",
                showarrow=False,
                yshift=15,
                font=dict(size=12, color="#4CAF50"),
            )

    fig.update_layout(
        height=400,
        template="plotly_dark",
        barmode="group",
        yaxis_title="Rate (%)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=40, r=20, t=20, b=40),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Data table
    st.markdown("#### Spread Data")
    display_cols = ["date", "currency", "tenor", "npc_rate", "rfr_name", "rfr_rate", "npc_premium_over_rfr"]
    available = [c for c in display_cols if c in df.columns]
    st.dataframe(
        df[available],
        use_container_width=True,
        hide_index=True,
        column_config={
            "npc_rate": st.column_config.NumberColumn("NPC (%)", format="%.2f"),
            "rfr_rate": st.column_config.NumberColumn("RFR (%)", format="%.4f"),
            "npc_premium_over_rfr": st.column_config.NumberColumn("Premium (%)", format="%.2f"),
        },
    )


def _render_carry_trade(con):
    """Tab 4: NPC vs KIBOR carry trade analysis."""
    from psx_ohlcv.db.repositories.npc_rates import get_carry_trade_analysis

    st.markdown("### Carry Trade Analysis")
    st.caption("KIBOR vs NPC rate — spread represents PKR deposit premium over FCY NPC")

    currency = st.selectbox("NPC Currency", ["USD", "GBP", "EUR"], key="npc_carry_ccy")
    df = get_carry_trade_analysis(con, currency=currency)

    if df.empty:
        st.info("No carry trade data. Ensure NPC, KIBOR, and FX rates are synced.")
        return

    # Metrics row for latest
    latest = df.iloc[0] if not df.empty else {}
    cols = st.columns(4)
    with cols[0]:
        st.metric(f"NPC {currency} 12M", f"{latest.get('npc_rate', 0):.2f}%")
    with cols[1]:
        kibor = latest.get("kibor_offer", 0) or 0
        st.metric("KIBOR (matched)", f"{kibor:.2f}%")
    with cols[2]:
        spread = latest.get("kibor_npc_spread", 0) or 0
        st.metric("Spread", f"{spread:+.2f}%")
    with cols[3]:
        fx = latest.get("fx_rate_pkr", 0) or 0
        st.metric(f"{currency}/PKR", f"{fx:.2f}")

    # Data table
    st.markdown("#### Detail")
    display_cols = ["date", "tenor", "npc_rate", "kibor_offer", "kibor_npc_spread", "fx_rate_pkr"]
    available = [c for c in display_cols if c in df.columns]
    st.dataframe(
        df[available],
        use_container_width=True,
        hide_index=True,
        column_config={
            "npc_rate": st.column_config.NumberColumn(f"NPC {currency} (%)", format="%.2f"),
            "kibor_offer": st.column_config.NumberColumn("KIBOR (%)", format="%.2f"),
            "kibor_npc_spread": st.column_config.NumberColumn("Spread (%)", format="%.2f"),
            "fx_rate_pkr": st.column_config.NumberColumn("FX Rate", format="%.2f"),
        },
    )


def _render_dashboard(con):
    """Tab 5: Multi-currency dashboard."""
    from psx_ohlcv.db.repositories.npc_rates import get_multicurrency_dashboard

    st.markdown("### Multi-Currency Dashboard")
    st.caption("NPC + Global RFR + KIBOR + FX — comprehensive view")

    df = get_multicurrency_dashboard(con)
    if df.empty:
        st.info("No dashboard data. Run sync commands first.")
        return

    # Compute derived columns
    if "npc_rate" in df.columns and "global_rfr" in df.columns:
        df["npc_over_rfr"] = (df["npc_rate"] - df["global_rfr"].fillna(0)).round(4)
    if "kibor_offer" in df.columns and "npc_rate" in df.columns:
        df["kibor_over_npc"] = (df["kibor_offer"].fillna(0) - df["npc_rate"]).round(4)

    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "npc_rate": st.column_config.NumberColumn("NPC (%)", format="%.2f"),
            "global_rfr": st.column_config.NumberColumn("RFR (%)", format="%.4f"),
            "kibor_offer": st.column_config.NumberColumn("KIBOR (%)", format="%.2f"),
            "fx_rate_pkr": st.column_config.NumberColumn("FX Rate", format="%.2f"),
            "npc_over_rfr": st.column_config.NumberColumn("NPC-RFR (%)", format="%.2f"),
            "kibor_over_npc": st.column_config.NumberColumn("KIBOR-NPC (%)", format="%.2f"),
        },
    )
