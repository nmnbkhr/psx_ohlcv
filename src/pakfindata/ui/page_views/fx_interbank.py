"""FX Interbank vs Open Market — spread visualization and comparison."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from pakfindata.ui.components.helpers import get_connection, render_footer


_KEY_CURRENCIES = ["USD", "EUR", "GBP", "SAR", "AED"]

_FX_TABLES = {
    "Interbank": "sbp_fx_interbank",
    "Open Market": "sbp_fx_open_market",
    "Kerb": "forex_kerb",
}


def render_fx_interbank():
    """Render Interbank vs Open Market comparison page."""
    st.markdown("## Interbank vs Open Market")
    st.caption("Compare SBP interbank, open market, and kerb rates with spread analysis")

    con = get_connection()
    if con is None:
        st.error("Database connection not available")
        return

    # ── Rate Cards ───────────────────────────────────────────────
    _render_rate_comparison(con)

    st.divider()

    # ── Spread Analysis ──────────────────────────────────────────
    _render_spread_chart(con)

    st.divider()

    # ── Historical Spread ────────────────────────────────────────
    _render_spread_history(con)

    # ── Sync Section ─────────────────────────────────────────────
    st.divider()
    with st.expander("Sync FX Data"):
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Sync SBP Interbank", type="primary", key="fxib_sync"):
                with st.spinner("Syncing..."):
                    try:
                        from pakfindata.sources.sbp_fx import SBPFXScraper
                        result = SBPFXScraper().sync_interbank(con)
                        st.success(f"Interbank: {result.get('ok', 0)} rates synced")
                    except Exception as e:
                        st.error(f"Sync failed: {e}")
        with col2:
            if st.button("Sync Kerb (forex.pk)", key="fxib_kerb"):
                with st.spinner("Syncing..."):
                    try:
                        from pakfindata.sources.forex_scraper import ForexPKScraper
                        result = ForexPKScraper().sync_kerb(con)
                        st.success(f"Kerb: {result.get('ok', 0)} rates synced")
                    except Exception as e:
                        st.error(f"Sync failed: {e}")

    render_footer()


def _get_latest_rate(con, table: str, currency: str) -> dict | None:
    """Get latest rate for a currency from a table."""
    try:
        row = con.execute(
            f"""SELECT date, buying, selling FROM {table}
                WHERE UPPER(currency) = ? ORDER BY date DESC LIMIT 1""",
            (currency.upper(),),
        ).fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def _render_rate_comparison(con):
    """Side-by-side rate comparison for key currencies."""
    st.subheader("Rate Comparison")

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

        # Spread
        ib = _get_latest_rate(con, "sbp_fx_interbank", currency)
        kerb = _get_latest_rate(con, "forex_kerb", currency)
        with cols[4]:
            if ib and kerb and ib.get("selling") and kerb.get("selling"):
                spread = kerb["selling"] - ib["selling"]
                st.metric("Spread", f"{spread:+.2f}", help="Kerb premium")
            else:
                st.metric("Spread", "N/A")


def _render_spread_chart(con):
    """Bar chart comparing interbank vs kerb rates."""
    st.subheader("Spread Analysis")

    spreads = []
    for currency in _KEY_CURRENCIES:
        ib = _get_latest_rate(con, "sbp_fx_interbank", currency)
        kerb = _get_latest_rate(con, "forex_kerb", currency)
        if ib and kerb and ib.get("selling") and kerb.get("selling"):
            spreads.append({
                "Currency": currency,
                "Interbank": ib["selling"],
                "Kerb": kerb["selling"],
                "Spread": round(kerb["selling"] - ib["selling"], 2),
            })

    if not spreads:
        st.info("No spread data. Sync both interbank and kerb rates first.")
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
        legend=dict(orientation="h", y=-0.2),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Summary table
    with st.expander("Spread Details"):
        st.dataframe(df, use_container_width=True, hide_index=True)


def _render_spread_history(con):
    """Historical interbank vs kerb spread for selected currency."""
    st.subheader("Historical Spread")

    currency = st.selectbox("Currency", _KEY_CURRENCIES, key="fxib_hist_curr")

    try:
        ib_df = pd.read_sql_query(
            """SELECT date, selling as ib_selling FROM sbp_fx_interbank
               WHERE UPPER(currency) = ? ORDER BY date LIMIT 365""",
            con, params=(currency.upper(),),
        )
        kerb_df = pd.read_sql_query(
            """SELECT date, selling as kerb_selling FROM forex_kerb
               WHERE UPPER(currency) = ? ORDER BY date LIMIT 365""",
            con, params=(currency.upper(),),
        )

        if ib_df.empty and kerb_df.empty:
            st.info("No historical data for this currency.")
            return

        fig = go.Figure()
        if not ib_df.empty:
            fig.add_trace(go.Scatter(
                x=ib_df["date"], y=ib_df["ib_selling"],
                mode="lines", name="Interbank",
                line=dict(color="#FF6B35", width=2),
            ))
        if not kerb_df.empty:
            fig.add_trace(go.Scatter(
                x=kerb_df["date"], y=kerb_df["kerb_selling"],
                mode="lines", name="Kerb",
                line=dict(color="#45B7D1", width=2),
            ))

        fig.update_layout(
            height=400,
            xaxis_title="Date",
            yaxis_title=f"{currency}/PKR (Selling)",
            legend=dict(orientation="h", y=-0.15),
        )
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Error loading history: {e}")
