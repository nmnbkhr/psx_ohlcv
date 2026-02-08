"""FX Rates Comparison Dashboard — interbank, open market, kerb rates."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from psx_ohlcv.ui.components.helpers import get_connection, render_footer
from psx_ohlcv.sources.sbp_fx import SBPFXScraper
from psx_ohlcv.sources.forex_scraper import ForexPKScraper


_KEY_CURRENCIES = ["USD", "EUR", "GBP", "SAR", "AED"]

_FX_TABLES = {
    "Interbank": "sbp_fx_interbank",
    "Open Market": "sbp_fx_open_market",
    "Kerb": "forex_kerb",
}


def render_fx_dashboard():
    """FX rates comparison dashboard."""
    st.markdown("## FX Rates Dashboard")

    con = get_connection()
    if con is None:
        st.error("Database connection not available")
        return

    try:
        _render_rate_cards(con)
        st.divider()

        col1, col2 = st.columns(2)
        with col1:
            _render_history_chart(con)
        with col2:
            _render_spread_analysis(con)

        st.divider()
        _render_all_currencies(con)

    except Exception as e:
        st.error(f"Error loading FX data: {e}")

    # Sync section
    st.markdown("---")
    with st.expander("Sync FX Data"):
        col1, col2 = st.columns(2)

        with col1:
            if st.button("Sync SBP Interbank", type="primary", key="fx_sync_interbank"):
                with st.spinner("Syncing SBP interbank rates..."):
                    try:
                        result = SBPFXScraper().sync_interbank(con)
                        st.success(f"Interbank: {result.get('ok', 0)} rates synced")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Sync failed: {e}")

        with col2:
            if st.button("Sync Kerb (forex.pk)", key="fx_sync_kerb"):
                with st.spinner("Syncing kerb rates from forex.pk..."):
                    try:
                        result = ForexPKScraper().sync_kerb(con)
                        st.success(f"Kerb: {result.get('ok', 0)} rates synced")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Sync failed: {e}")

    render_footer()


def _get_latest_rate(con, table, currency):
    """Get latest rate for a currency from a specific table."""
    row = con.execute(
        f"""SELECT date, buying, selling FROM {table}
            WHERE UPPER(currency) = ? ORDER BY date DESC LIMIT 1""",
        (currency.upper(),),
    ).fetchone()
    return dict(row) if row else None


def _render_rate_cards(con):
    """Rate cards for key currencies across all sources."""
    st.markdown("### Key Currency Rates")

    for currency in _KEY_CURRENCIES:
        cols = st.columns([1, 2, 2, 2, 1])
        cols[0].markdown(f"**{currency}/PKR**")

        for i, (src_name, table) in enumerate(_FX_TABLES.items()):
            rate = _get_latest_rate(con, table, currency)
            with cols[i + 1]:
                if rate:
                    st.metric(
                        src_name,
                        f"{rate['buying']:.2f} / {rate['selling']:.2f}",
                        help=f"Buy / Sell as of {rate['date']}",
                    )
                else:
                    st.metric(src_name, "N/A")

        # Spread (kerb premium over interbank)
        ib = _get_latest_rate(con, "sbp_fx_interbank", currency)
        kerb = _get_latest_rate(con, "forex_kerb", currency)
        with cols[4]:
            if ib and kerb and ib["selling"] and kerb["selling"]:
                spread = kerb["selling"] - ib["selling"]
                st.metric("Spread", f"{spread:+.2f}", help="Kerb premium")
            else:
                st.metric("Spread", "N/A")


def _render_history_chart(con):
    """Historical FX rate chart."""
    st.markdown("### Rate History")

    currency = st.selectbox("Currency", _KEY_CURRENCIES, key="fx_hist_currency")

    fig = go.Figure()
    colors = {"Interbank": "#FF6B35", "Open Market": "#4ECDC4", "Kerb": "#45B7D1"}

    for src_name, table in _FX_TABLES.items():
        df = pd.read_sql_query(
            f"""SELECT date, selling FROM {table}
                WHERE UPPER(currency) = ?
                ORDER BY date LIMIT 365""",
            con, params=(currency.upper(),),
        )
        if not df.empty:
            fig.add_trace(go.Scatter(
                x=df["date"], y=df["selling"],
                mode="lines", name=src_name,
                line=dict(width=2, color=colors.get(src_name, "#999")),
            ))

    if fig.data:
        fig.update_layout(
            xaxis_title="Date",
            yaxis_title=f"{currency}/PKR (Selling)",
            height=350, margin=dict(l=20, r=20, t=30, b=20),
            legend=dict(orientation="h", y=-0.2),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info(f"No historical data for {currency}. Run `psxsync fx-rates sync-all` to fetch.")


def _render_spread_analysis(con):
    """Bar chart of interbank vs kerb spread per currency."""
    st.markdown("### Spread Analysis")

    spreads = []
    for currency in _KEY_CURRENCIES:
        ib = _get_latest_rate(con, "sbp_fx_interbank", currency)
        kerb = _get_latest_rate(con, "forex_kerb", currency)
        if ib and kerb and ib["selling"] and kerb["selling"]:
            spreads.append({
                "Currency": currency,
                "Interbank": ib["selling"],
                "Kerb": kerb["selling"],
                "Spread": round(kerb["selling"] - ib["selling"], 2),
            })

    if not spreads:
        st.info("No spread data available. Sync interbank + kerb rates first.")
        return

    df = pd.DataFrame(spreads)
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["Currency"], y=df["Interbank"],
        name="Interbank", marker_color="#FF6B35",
    ))
    fig.add_trace(go.Bar(
        x=df["Currency"], y=df["Kerb"],
        name="Kerb", marker_color="#45B7D1",
    ))
    fig.update_layout(
        barmode="group", height=350,
        yaxis_title="PKR Rate (Selling)",
        margin=dict(l=20, r=20, t=30, b=20),
        legend=dict(orientation="h", y=-0.2),
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_all_currencies(con):
    """Table of all available currencies from interbank."""
    st.markdown("### All Currency Rates (Interbank)")

    df = pd.read_sql_query(
        """SELECT t.currency, t.date, t.buying, t.selling,
                  ROUND(t.selling - t.buying, 4) as spread
           FROM sbp_fx_interbank t
           INNER JOIN (
               SELECT currency, MAX(date) as max_date
               FROM sbp_fx_interbank GROUP BY currency
           ) mx ON t.currency = mx.currency AND t.date = mx.max_date
           ORDER BY t.currency""",
        con,
    )

    if df.empty:
        # Try kerb as fallback
        df = pd.read_sql_query(
            """SELECT t.currency, t.date, t.buying, t.selling,
                      ROUND(t.selling - t.buying, 4) as spread
               FROM forex_kerb t
               INNER JOIN (
                   SELECT currency, MAX(date) as max_date
                   FROM forex_kerb GROUP BY currency
               ) mx ON t.currency = mx.currency AND t.date = mx.max_date
               ORDER BY t.currency""",
            con,
        )
        if not df.empty:
            st.caption("Showing kerb market rates (interbank data not yet available)")

    if df.empty:
        st.info("No currency data available. Run `psxsync fx-rates sync-all` to fetch.")
        return

    st.dataframe(
        df.rename(columns={
            "currency": "Currency", "date": "Date",
            "buying": "Buying", "selling": "Selling", "spread": "Spread",
        }),
        use_container_width=True, hide_index=True,
    )
