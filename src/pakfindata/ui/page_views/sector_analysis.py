"""Sector Analysis — sector rotation, relative performance."""

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

    dates = [str(r[0]) for r in con.execute(
        "SELECT DISTINCT date FROM eod_ohlcv ORDER BY date DESC LIMIT 5"
    ).fetchall()]
    if len(dates) < 2:
        return {"dates": dates, "df": pd.DataFrame()}

    latest = dates[0]

    df = pd.read_sql_query(
        """SELECT
             COALESCE(s.sector_name, e1.sector_code) as sector,
             ROUND(AVG(CASE WHEN e1.prev_close > 0
               THEN (e1.close - e1.prev_close) / e1.prev_close * 100 END), 2) as return_1d
           FROM eod_ohlcv e1
           LEFT JOIN sectors s ON e1.sector_code = s.sector_code
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
            st.dataframe(df, use_container_width=True, hide_index=True)
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
            st.plotly_chart(fig, use_container_width=True)
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
            st.dataframe(df, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Error loading heatmap: {e}")
