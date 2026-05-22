"""Sector Analysis — sector rotation, relative performance, index weights.

Reads data through the /v1 API client. Direct SQLite reads moved to
``/v1/sectors/performance``, ``/v1/sectors/symbol-map``, and
``/v1/freshness/eod_ohlcv`` (Phase 1.7.D.2).
"""

import glob
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from pakfindata.ui.api import client as api_client
from pakfindata.ui.components.helpers import render_footer


def render_sector_analysis():
    """Render the Sector Analysis page."""
    api_client.render_api_status_banner_if_down()

    st.markdown("## Sector Analysis")
    st.caption("Sector rotation, relative performance, and breadth")

    latest_date = _get_latest_date()
    if not latest_date:
        st.info("No market data. Sync EOD data first.")
        render_footer()
        return

    st.caption(f"Data as of: **{latest_date}**")

    # ── Sector Performance Table ─────────────────────────────────
    _render_sector_performance(latest_date)

    st.divider()

    # ── Sector Breadth ───────────────────────────────────────────
    _render_sector_breadth(latest_date)

    st.divider()

    # ── Sector Returns Heatmap ───────────────────────────────────
    _render_sector_heatmap(latest_date)

    st.divider()

    # ── Index Weights Treemap ─────────────────────────────────────
    _render_index_weights(latest_date)

    render_footer()


@st.cache_data(ttl=1800, show_spinner=False)
def _get_latest_date() -> str | None:
    fresh = api_client.get_dataset_freshness("eod_ohlcv")
    if fresh:
        return fresh.get("last_row_date")
    return None


@st.cache_data(ttl=1800, show_spinner=False)
def _load_sector_performance(date: str, min_stocks: int = 2) -> pd.DataFrame:
    """Load sector performance data for a given date."""
    rows = api_client.get_sector_performance(date=date, min_stocks=min_stocks)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _render_sector_performance(date: str):
    """Sector performance summary — avg return, top stock per sector."""
    st.subheader("Sector Performance")

    df = _load_sector_performance(date, min_stocks=2)
    if df.empty:
        st.info("No sector data available.")
        return

    display = df[["sector", "stocks", "avg_change", "total_volume", "gainers", "losers"]].copy()
    display.columns = ["Sector", "Stocks", "Avg Change %", "Total Volume", "Gainers", "Losers"]
    display["Total Volume"] = display["Total Volume"].apply(
        lambda x: f"{x / 1e6:.1f}M" if pd.notna(x) and x >= 1e6
        else (f"{x:,.0f}" if pd.notna(x) else "—")
    )
    st.dataframe(display, width='stretch', hide_index=True)


def _render_sector_breadth(date: str):
    """Bar chart of gainers vs losers per sector."""
    st.subheader("Sector Breadth")

    df = _load_sector_performance(date, min_stocks=3)
    if df.empty:
        return

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["sector"], y=df["gainers"],
        name="Gainers", marker_color="#00D26A",
    ))
    fig.add_trace(go.Bar(
        x=df["sector"], y=-df["losers"],
        name="Losers", marker_color="#FF4B4B",
    ))
    fig.update_layout(
        barmode="relative", height=400,
        xaxis_title="Sector", yaxis_title="Count",
        xaxis_tickangle=-45,
        margin=dict(b=120),
    )
    st.plotly_chart(fig, width='stretch')


def _render_sector_heatmap(latest_date: str):
    """Multi-period sector return heatmap."""
    st.subheader("Sector Returns (Multi-Period)")

    df = _load_sector_performance(latest_date, min_stocks=2)
    if df.empty:
        st.info("Need at least 2 trading days for heatmap.")
        return

    heatmap_df = df[["sector", "avg_change"]].copy()
    heatmap_df.columns = ["Sector", "1-Day Return %"]
    st.dataframe(heatmap_df, width='stretch', hide_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# Index Weights
# ─────────────────────────────────────────────────────────────────────────────

_CONSTITUENT_DIR = Path("/mnt/e/psxdata/downloads/daily")


@st.cache_data(ttl=3600, show_spinner=False)
def _load_index_weights() -> pd.DataFrame:
    """Load latest constituent_data XLS with index weights."""
    files = sorted(glob.glob(str(_CONSTITUENT_DIR / "*/indices/constituent_data_*.xls")))
    if not files:
        return pd.DataFrame()

    latest = files[-1]
    try:
        df = pd.read_excel(latest, engine="xlrd")
    except Exception:
        return pd.DataFrame()

    if "IDX WT %" not in df.columns or "SYMBOL" not in df.columns:
        return pd.DataFrame()

    df = df[df["IDX WT %"] > 0].copy()
    df["_file"] = Path(latest).stem
    return df


@st.cache_data(ttl=1800, show_spinner=False)
def _load_sector_symbol_map(date: str) -> pd.DataFrame:
    """symbol → sector_name lookup for the given trading day."""
    rows = api_client.get_sector_symbol_map(date=date)
    if not rows:
        return pd.DataFrame(columns=["symbol", "sector"])
    return pd.DataFrame(rows)


def _fetch_psx_daily(date_str: str):
    """Fetch PSX DPS daily gap files (constituent data, futures OI, etc.)."""
    import subprocess
    result = subprocess.run(
        ["python3", "-m", "pakfindata.sources.psx_downloads", "daily", date_str],
        capture_output=True, text=True, timeout=120,
        cwd=str(Path(__file__).resolve().parents[3]),
    )
    return result.stdout + result.stderr


def _render_index_weights(latest_date: str):
    """KSE-100 index weights treemap + top constituents table."""
    st.subheader("KSE-100 Index Weights")

    # Fetch button — runs as independent subprocess
    from datetime import datetime, timezone, timedelta
    PKT = timezone(timedelta(hours=5))
    today = datetime.now(PKT).strftime("%Y-%m-%d")

    files = sorted(glob.glob(str(_CONSTITUENT_DIR / "*/indices/constituent_data_*.xls")))
    file_date = Path(files[-1]).stem.replace("constituent_data_", "") if files else "none"

    if file_date != today:
        st.warning(f"Constituent data is from **{file_date}** — today is {today}")

    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        if st.button(f"Fetch Today ({today})", key="fetch_psx_daily"):
            with st.spinner(f"Downloading PSX DPS files for {today}..."):
                output = _fetch_psx_daily(today)
                st.code(output, language="text")
                _load_index_weights.clear()
                st.rerun()
    with col2:
        from datetime import date as dt_date
        backfill_start = st.date_input("From", value=dt_date.fromisoformat(file_date) if file_date != "none" else None, key="bf_start")
    with col3:
        backfill_end = st.date_input("To", value=dt_date.fromisoformat(today), key="bf_end")

    if st.button("Backfill Date Range", key="fetch_psx_backfill"):
        if backfill_start and backfill_end:
            import subprocess
            with st.spinner(f"Backfilling {backfill_start} → {backfill_end}..."):
                result = subprocess.run(
                    ["python3", "-m", "pakfindata.sources.psx_downloads",
                     "backfill", str(backfill_start), str(backfill_end)],
                    capture_output=True, text=True, timeout=600,
                    cwd=str(Path(__file__).resolve().parents[3]),
                )
                st.code(result.stdout + result.stderr, language="text")
                _load_index_weights.clear()
                st.rerun()

    df = _load_index_weights()
    if df.empty:
        st.info(
            "No constituent data found. "
            "Download from PSX DPS → `/mnt/e/psxdata/downloads/daily/{date}/indices/constituent_data_*.xls`"
        )
        return

    file_label = df["_file"].iloc[0].replace("constituent_data_", "")
    st.caption(f"Source: PSX constituent data ({file_label})")

    # Join with sector lookup for treemap
    sectors_df = _load_sector_symbol_map(latest_date)
    if not sectors_df.empty:
        df = df.merge(sectors_df, left_on="SYMBOL", right_on="symbol", how="left")
        df["sector"] = df["sector"].fillna("Other")
    else:
        df["sector"] = "Unknown"

    # Treemap
    try:
        import plotly.express as px

        tree_df = df[["SYMBOL", "sector", "IDX WT %", "FF BASED MCAP"]].copy()
        tree_df = tree_df[tree_df["IDX WT %"] > 0.01]
        tree_df["label"] = tree_df["SYMBOL"] + " (" + tree_df["IDX WT %"].round(2).astype(str) + "%)"

        fig = px.treemap(
            tree_df,
            path=["sector", "label"],
            values="IDX WT %",
            color="IDX WT %",
            color_continuous_scale="YlOrRd",
            title="KSE-100 Index Weight Treemap",
        )
        fig.update_layout(
            height=600,
            margin=dict(t=40, l=0, r=0, b=0),
        )
        st.plotly_chart(fig, width='stretch')
    except Exception as e:
        st.warning(f"Treemap rendering failed: {e}")

    # Top 20 table
    top = df.nlargest(20, "IDX WT %")[["SYMBOL", "COMPANY", "PRICE", "IDX WT %", "FF BASED MCAP", "sector"]].copy()
    top.columns = ["Symbol", "Company", "Price", "Weight %", "FF MCap", "Sector"]
    top["FF MCap"] = (top["FF MCap"] / 1e9).round(2).astype(str) + "B"
    top["Weight %"] = top["Weight %"].round(3)
    top = top.reset_index(drop=True)
    top.index += 1

    st.markdown("**Top 20 Constituents by Weight**")
    st.dataframe(top, width='stretch')
