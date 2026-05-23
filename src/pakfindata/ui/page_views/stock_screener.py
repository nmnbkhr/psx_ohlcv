"""Stock Screener — filter by sector, P/E, market cap, and more.

Reads data through the /v1 API client. Direct SQLite reads moved to
``/v1/symbols/screener`` and ``/v1/symbols/sectors`` (Phase 1.7.D.1).
"""

import pandas as pd
import streamlit as st

from pakfindata.ui.api import client as api_client
from pakfindata.ui.components.helpers import render_footer


# ── Cached data loaders ──────────────────────────────────────────────────────

@st.cache_data(ttl=120, show_spinner=False)
def _load_screener_data(
    sector: str,
    min_pe: float,
    max_pe: float,
    min_mcap: float,
    min_vol: float,
) -> pd.DataFrame:
    rows = api_client.get_screener(
        sector=None if sector == "All" else sector,
        min_pe=min_pe,
        max_pe=max_pe if max_pe < 100 else 1000.0,
        min_mcap_m=min_mcap,
        min_volume=min_vol,
        limit=200,
    )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


@st.cache_data(ttl=120, show_spinner=False)
def _load_sector_list() -> list[str]:
    sectors = api_client.get_symbol_sectors()
    return sectors or []


def render_stock_screener():
    """Render the Stock Screener page with fundamental filters."""
    api_client.render_api_status_banner_if_down()

    st.markdown("## Stock Screener")
    st.caption("Filter by sector, P/E, market cap, and other fundamentals")

    # ── Filters ──────────────────────────────────────────────────
    col1, col2, col3, col4 = st.columns(4)

    sectors = ["All"] + _load_sector_list()

    with col1:
        sector = st.selectbox("Sector", sectors, key="scr_sector")

    with col2:
        min_pe = st.number_input("Min P/E", value=0.0, step=1.0, key="scr_min_pe")
        max_pe = st.number_input("Max P/E", value=100.0, step=1.0, key="scr_max_pe")

    with col3:
        min_mcap = st.number_input(
            "Min Market Cap (M)", value=0.0, step=100.0, key="scr_min_mcap"
        )

    with col4:
        min_vol = st.number_input(
            "Min Avg Volume", value=0.0, step=10000.0, key="scr_min_vol"
        )

    # ── Build Query ──────────────────────────────────────────────
    df = _load_screener_data(sector, min_pe, max_pe, min_mcap, min_vol)

    if df.empty:
        st.warning("**No market data available yet.**")
        st.info(
            "**To get started, run one of these:**\n"
            "- `pfsync regular-market snapshot` — fetches live price, volume & change for all stocks (fastest)\n"
            "- `pfsync company deep-scrape --all` — fetches P/E, market cap, free float per stock (slower, ~1 req/symbol)"
        )
        render_footer()
        return

    # Detect fundamental coverage from the response itself
    has_cf = df["pe_ratio"].notna().any() or df["market_cap"].notna().any()
    if not has_cf:
        st.info(
            "Showing price, volume & change from market watch. "
            "Run `pfsync company deep-scrape --all` to add P/E, market cap & free float."
        )

    # ── Results Summary ──────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Matches", len(df))

    avg_pe = df["pe_ratio"].dropna().mean()
    c2.metric("Avg P/E", f"{avg_pe:.1f}" if pd.notna(avg_pe) else "N/A")

    total_mcap = df["market_cap"].dropna().sum()
    if total_mcap >= 1e12:
        c3.metric("Total Market Cap", f"Rs. {total_mcap / 1e12:.1f}T")
    elif total_mcap >= 1e9:
        c3.metric("Total Market Cap", f"Rs. {total_mcap / 1e9:.1f}B")
    elif total_mcap > 0:
        c3.metric("Total Market Cap", f"Rs. {total_mcap / 1e6:.0f}M")
    else:
        c3.metric("Total Market Cap", "N/A")

    total_turnover = df["turnover"].dropna().sum()
    if total_turnover >= 1e9:
        c4.metric("Total Turnover", f"Rs. {total_turnover / 1e9:.2f}B")
    elif total_turnover >= 1e6:
        c4.metric("Total Turnover", f"Rs. {total_turnover / 1e6:.0f}M")
    elif total_turnover > 0:
        c4.metric("Total Turnover", f"Rs. {total_turnover:,.0f}")
    else:
        c4.metric("Total Turnover", "N/A")

    st.divider()

    # ── Results Table ────────────────────────────────────────
    display_df = df[[
        "symbol", "name", "sector", "price", "pe_ratio",
        "market_cap", "free_float_pct", "last_volume", "turnover", "change_pct",
    ]].copy()
    display_df.columns = [
        "Symbol", "Name", "Sector", "Price", "P/E",
        "Market Cap", "Free Float %", "Volume", "Turnover", "Change %",
    ]

    # Format market cap
    display_df["Market Cap"] = display_df["Market Cap"].apply(
        lambda x: f"{x / 1e9:.1f}B" if pd.notna(x) and x >= 1e9
        else (f"{x / 1e6:.0f}M" if pd.notna(x) and x >= 1e6 else "—")
    )

    # Format free float
    display_df["Free Float %"] = display_df["Free Float %"].apply(
        lambda x: f"{x:.1f}%" if pd.notna(x) else "—"
    )

    # Format price
    display_df["Price"] = display_df["Price"].apply(
        lambda x: f"{x:,.2f}" if pd.notna(x) else "—"
    )

    # Format volume
    display_df["Volume"] = display_df["Volume"].apply(
        lambda x: f"{x:,.0f}" if pd.notna(x) else "—"
    )

    # Format turnover
    display_df["Turnover"] = display_df["Turnover"].apply(
        lambda x: f"{x / 1e9:.2f}B" if pd.notna(x) and x >= 1e9
        else (f"{x / 1e6:.1f}M" if pd.notna(x) and x >= 1e6
        else (f"{x:,.0f}" if pd.notna(x) and x > 0 else "—"))
    )

    # Format change %
    display_df["Change %"] = display_df["Change %"].apply(
        lambda x: f"{x:+.2f}%" if pd.notna(x) else "—"
    )

    # Format P/E
    display_df["P/E"] = display_df["P/E"].apply(
        lambda x: f"{x:.1f}" if pd.notna(x) else "—"
    )

    st.dataframe(display_df, width='stretch', hide_index=True)

    # Export button
    csv = df.to_csv(index=False)
    st.download_button(
        "Export CSV", csv, "screener_results.csv", "text/csv",
        key="scr_export",
    )

    render_footer()
