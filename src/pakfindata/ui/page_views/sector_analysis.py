"""Sector Analysis — sector rotation, relative performance, index weights."""

import glob
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from pakfindata.ui.components.helpers import get_connection, render_footer


def render_sector_analysis():
    """Render the Sector Analysis page."""
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
    _render_sector_heatmap()

    st.divider()

    # ── Index Weights Treemap ─────────────────────────────────────
    _render_index_weights()

    render_footer()


@st.cache_data(ttl=1800, show_spinner=False)
def _get_latest_date() -> str | None:
    try:
        con = get_connection()
        if con is None:
            return None
        row = con.execute("SELECT MAX(date) FROM eod_ohlcv").fetchone()
        return row[0] if row and row[0] else None
    except Exception:
        return None


@st.cache_data(ttl=1800, show_spinner=False)
def _load_sector_performance(date: str) -> pd.DataFrame:
    """Load sector performance data for a given date."""
    con = get_connection()
    if con is None:
        return pd.DataFrame()
    return pd.read_sql_query(
        """SELECT
             COALESCE(s.sector_name, e.sector_code) as sector,
             COUNT(*) as stocks,
             ROUND(AVG(CASE WHEN e.prev_close > 0
               THEN (e.close - e.prev_close) / e.prev_close * 100 END), 2) as avg_change,
             SUM(e.volume) as total_volume,
             SUM(CASE WHEN e.close > e.prev_close THEN 1 ELSE 0 END) as gainers,
             SUM(CASE WHEN e.close < e.prev_close THEN 1 ELSE 0 END) as losers
           FROM eod_ohlcv e
           LEFT JOIN sectors s ON s.sector_code = CASE WHEN LENGTH(e.sector_code) < 4 THEN '0' || e.sector_code ELSE e.sector_code END
           WHERE e.date = ? AND e.prev_close > 0
           GROUP BY COALESCE(s.sector_name, e.sector_code)
           HAVING stocks >= 2
           ORDER BY avg_change DESC""",
        con, params=(date,),
    )


@st.cache_data(ttl=1800, show_spinner=False)
def _load_sector_breadth(date: str) -> pd.DataFrame:
    """Load sector breadth data for a given date."""
    con = get_connection()
    if con is None:
        return pd.DataFrame()
    return pd.read_sql_query(
        """SELECT
             COALESCE(s.sector_name, e.sector_code) as sector,
             SUM(CASE WHEN e.close > e.prev_close THEN 1 ELSE 0 END) as gainers,
             SUM(CASE WHEN e.close < e.prev_close THEN 1 ELSE 0 END) as losers
           FROM eod_ohlcv e
           LEFT JOIN sectors s ON s.sector_code = CASE WHEN LENGTH(e.sector_code) < 4 THEN '0' || e.sector_code ELSE e.sector_code END
           WHERE e.date = ? AND e.prev_close > 0
           GROUP BY COALESCE(s.sector_name, e.sector_code)
           HAVING COUNT(*) >= 3
           ORDER BY gainers DESC""",
        con, params=(date,),
    )


@st.cache_data(ttl=1800, show_spinner=False)
def _load_sector_heatmap_data() -> dict:
    """Load sector heatmap data (dates and returns)."""
    con = get_connection()
    if con is None:
        return {"dates": [], "df": pd.DataFrame()}

    from pakfindata.db.date_manifest import get_dates
    dates = get_dates("eod_ohlcv")[:5]
    if len(dates) < 2:
        return {"dates": dates, "df": pd.DataFrame()}

    latest = dates[0]

    df = pd.read_sql_query(
        """SELECT
             COALESCE(s.sector_name, e1.sector_code) as sector,
             ROUND(AVG(CASE WHEN e1.prev_close > 0
               THEN (e1.close - e1.prev_close) / e1.prev_close * 100 END), 2) as return_1d
           FROM eod_ohlcv e1
           LEFT JOIN sectors s ON s.sector_code = CASE WHEN LENGTH(e1.sector_code) < 4 THEN '0' || e1.sector_code ELSE e1.sector_code END
           WHERE e1.date = ? AND e1.prev_close > 0
           GROUP BY COALESCE(s.sector_name, e1.sector_code)
           HAVING COUNT(*) >= 2
           ORDER BY return_1d DESC""",
        con, params=(latest,),
    )
    return {"dates": dates, "df": df}


def _render_sector_performance(date: str):
    """Sector performance summary — avg return, top stock per sector."""
    st.subheader("Sector Performance")

    try:
        df = _load_sector_performance(date)
        if not df.empty:
            df.columns = ["Sector", "Stocks", "Avg Change %", "Total Volume", "Gainers", "Losers"]
            df["Total Volume"] = df["Total Volume"].apply(
                lambda x: f"{x / 1e6:.1f}M" if x >= 1e6 else f"{x:,.0f}"
            )
            st.dataframe(df, width='stretch', hide_index=True)
        else:
            st.info("No sector data available.")
    except Exception as e:
        st.error(f"Error loading sector data: {e}")


def _render_sector_breadth(date: str):
    """Bar chart of gainers vs losers per sector."""
    st.subheader("Sector Breadth")

    try:
        df = _load_sector_breadth(date)
        if not df.empty:
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
    except Exception as e:
        st.error(f"Error loading breadth chart: {e}")


def _render_sector_heatmap():
    """Multi-period sector return heatmap."""
    st.subheader("Sector Returns (Multi-Period)")

    try:
        result = _load_sector_heatmap_data()
        dates, df = result["dates"], result["df"]
        if not dates:
            st.error("Database connection not available")
            return
        if len(dates) < 2:
            st.info("Need at least 2 trading days for heatmap.")
            return

        if not df.empty:
            df.columns = ["Sector", "1-Day Return %"]
            st.dataframe(df, width='stretch', hide_index=True)
    except Exception as e:
        st.error(f"Error loading heatmap: {e}")


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


def _fetch_psx_daily(date_str: str):
    """Fetch PSX DPS daily gap files (constituent data, futures OI, etc.)."""
    import subprocess
    result = subprocess.run(
        ["python3", "-m", "pakfindata.sources.psx_downloads", "daily", date_str],
        capture_output=True, text=True, timeout=120,
        cwd=str(Path(__file__).resolve().parents[3]),
    )
    return result.stdout + result.stderr


def _render_index_weights():
    """KSE-100 index weights treemap + top constituents table."""
    st.subheader("KSE-100 Index Weights")

    # Fetch button — runs as independent subprocess
    from datetime import datetime, timezone, timedelta
    PKT = timezone(timedelta(hours=5))
    today = datetime.now(PKT).strftime("%Y-%m-%d")

    files = sorted(glob.glob(str(_CONSTITUENT_DIR / "*/indices/constituent_data_*.xls")))
    latest_date = Path(files[-1]).stem.replace("constituent_data_", "") if files else "none"

    if latest_date != today:
        st.warning(f"Constituent data is from **{latest_date}** — today is {today}")

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
        backfill_start = st.date_input("From", value=dt_date.fromisoformat(latest_date) if latest_date != "none" else None, key="bf_start")
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

    # Join with eod_ohlcv for sector info
    con = get_connection()
    if con:
        try:
            sectors = pd.read_sql_query(
                """SELECT e.symbol, COALESCE(s.sector_name, e.sector_code) as sector
                   FROM eod_ohlcv e
                   LEFT JOIN sectors s ON s.sector_code =
                       CASE WHEN LENGTH(e.sector_code) < 4 THEN '0' || e.sector_code ELSE e.sector_code END
                   WHERE e.date = (SELECT MAX(date) FROM eod_ohlcv)
                   GROUP BY e.symbol""",
                con,
            )
            df = df.merge(sectors, left_on="SYMBOL", right_on="symbol", how="left")
            df["sector"] = df["sector"].fillna("Other")
        except Exception:
            df["sector"] = "Unknown"
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
