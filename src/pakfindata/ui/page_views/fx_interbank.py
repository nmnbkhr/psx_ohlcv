"""FX Interbank vs Open Market — spread visualization and comparison.

Phase 1.7.C.2: page reads via :mod:`pakfindata.ui.api.client`. The Sync
buttons (1.6 territory) keep their existing safe_writer + worker-flag
path; only the read side migrated here.
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from pakfindata.ui.api import client as api_client
from pakfindata.ui.components.helpers import render_footer


_KEY_CURRENCIES = ["USD", "EUR", "GBP", "SAR", "AED", "CNY"]

_SOURCE_LABELS: dict[str, str] = {
    "interbank": "Interbank",
    "open_market": "Open Market",
    "kerb": "Kerb",
}


@st.cache_data(ttl=3600, show_spinner=False)
def _load_latest_rate(source: str, currency: str) -> dict | None:
    """Latest rate for a currency from one FX source."""
    return api_client.get_fx_latest_one(currency, source=source)


@st.cache_data(ttl=3600, show_spinner=False)
def _load_spread_history(currency: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Interbank + kerb history for a currency (last 365 rows each, oldest→newest)."""
    ib_rows = api_client.get_fx_history(currency, source="interbank", limit=365) or []
    kerb_rows = api_client.get_fx_history(currency, source="kerb", limit=365) or []

    # API returns date-DESC; the chart wants ascending. Sort here.
    ib_df = pd.DataFrame(ib_rows)
    if not ib_df.empty:
        ib_df = ib_df.sort_values("date").rename(columns={"selling": "ib_selling"})
        ib_df = ib_df[["date", "ib_selling"]]

    kerb_df = pd.DataFrame(kerb_rows)
    if not kerb_df.empty:
        kerb_df = kerb_df.sort_values("date").rename(columns={"selling": "kerb_selling"})
        kerb_df = kerb_df[["date", "kerb_selling"]]

    return ib_df, kerb_df


def render_fx_interbank():
    """Render Interbank vs Open Market comparison page."""
    if not api_client.render_api_status_banner_if_down():
        return

    st.markdown("## Interbank vs Open Market")
    st.caption("Compare SBP interbank, open market, and kerb rates with spread analysis")

    # ── Rate Cards ───────────────────────────────────────────────
    _render_rate_comparison()

    st.divider()

    # ── Spread Analysis ──────────────────────────────────────────
    _render_spread_chart()

    st.divider()

    # ── Historical Spread ────────────────────────────────────────
    _render_spread_history()

    # ── Sync Section ─────────────────────────────────────────────
    st.divider()
    with st.expander("Sync FX Data"):
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Sync SBP EasyData (FX + KIBOR)", type="primary", key="fxib_sync"):
                with st.spinner("Syncing from SBP EasyData..."):
                    from pakfindata.db.safe_writer import safe_writer, SafeWriterBusyError
                    from pakfindata.db.catalog import update_catalog_from_table, record_catalog_failure
                    try:
                        from pakfindata.sources.sbp_easydata import sync_fx_to_db, sync_kibor_to_db
                        with safe_writer() as wcon:
                            r1 = sync_fx_to_db(wcon)
                            r2 = sync_kibor_to_db(wcon)
                            update_catalog_from_table(wcon, "sbp_fx_monthly_avg", source="sbp_easydata")
                            update_catalog_from_table(wcon, "kibor", source="sbp_easydata")
                        st.cache_data.clear()
                        st.success(f"EasyData: {r1.get('fx_rows',0)} FX + {r2.get('kibor_rows',0)} KIBOR rows")
                    except SafeWriterBusyError:
                        st.error("Another sync is running. Wait a moment and retry.")
                    except Exception as e:
                        st.error(f"Sync failed: {e}")
                        for ds in ("sbp_fx_monthly_avg", "kibor"):
                            record_catalog_failure(ds, source="sbp_easydata", error=e)
        with col2:
            if st.button("Sync Open Market + Kerb (forex.pk)", key="fxib_kerb"):
                with st.spinner("Syncing..."):
                    from pakfindata.db.safe_writer import safe_writer, SafeWriterBusyError
                    from pakfindata.db.catalog import update_catalog_from_table, record_catalog_failure
                    try:
                        from pakfindata.sources.forex_scraper import ForexPKScraper
                        scraper = ForexPKScraper()  # HTTP init outside lock
                        with safe_writer() as wcon:
                            result = scraper.sync_kerb(wcon)
                            update_catalog_from_table(wcon, "fx_kerb", source="forex_pk")
                        st.cache_data.clear()
                        st.success(f"Open Market + Kerb: {result.get('ok', 0)} rates synced")
                    except SafeWriterBusyError:
                        st.error("Another sync is running. Wait a moment and retry.")
                    except Exception as e:
                        st.error(f"Sync failed: {e}")
                        record_catalog_failure("fx_kerb", source="forex_pk", error=e)

    render_footer()


def _render_rate_comparison():
    """Side-by-side rate comparison for key currencies."""
    st.subheader("Rate Comparison")

    for currency in _KEY_CURRENCIES:
        cols = st.columns([1, 2, 2, 2, 1])
        cols[0].markdown(f"**{currency}/PKR**")

        for i, (src_key, src_label) in enumerate(_SOURCE_LABELS.items()):
            rate = _load_latest_rate(src_key, currency)
            with cols[i + 1]:
                if rate and rate.get("buying") is not None and rate.get("selling") is not None:
                    st.metric(
                        src_label,
                        f"{rate['buying']:.2f} / {rate['selling']:.2f}",
                        help=f"Buy / Sell as of {rate['date']}",
                    )
                else:
                    st.metric(src_label, "N/A")

        # Spread
        ib = _load_latest_rate("interbank", currency)
        kerb = _load_latest_rate("kerb", currency)
        with cols[4]:
            if ib and kerb and ib.get("selling") and kerb.get("selling"):
                spread = kerb["selling"] - ib["selling"]
                st.metric("Spread", f"{spread:+.2f}", help="Kerb premium")
            else:
                st.metric("Spread", "N/A")


def _render_spread_chart():
    """Bar chart comparing interbank vs kerb rates."""
    st.subheader("Spread Analysis")

    spreads = []
    for currency in _KEY_CURRENCIES:
        ib = _load_latest_rate("interbank", currency)
        kerb = _load_latest_rate("kerb", currency)
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
    st.plotly_chart(fig, width='stretch')

    # Summary table
    with st.expander("Spread Details"):
        st.dataframe(df, width='stretch', hide_index=True)


def _render_spread_history():
    """Historical interbank vs kerb spread for selected currency."""
    st.subheader("Historical Spread")

    currency = st.selectbox("Currency", _KEY_CURRENCIES, key="fxib_hist_curr")

    ib_df, kerb_df = _load_spread_history(currency)

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
    st.plotly_chart(fig, width='stretch')
