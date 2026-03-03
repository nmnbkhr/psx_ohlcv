"""Market Pulse — what moved today, alerts, notable events."""

import pandas as pd
import streamlit as st

from pakfindata.ui.components.helpers import get_connection, render_footer


def render_market_pulse():
    """Render the Market Pulse page — today's notable market events."""
    st.markdown("## Market Pulse")
    st.caption("What moved today — biggest movers, volume leaders, notable events")

    con = get_connection()
    if con is None:
        st.error("Database connection not available")
        return

    # Get latest trading date
    latest_date = _get_latest_date(con)
    if not latest_date:
        st.info("No market data available. Sync EOD data first.")
        render_footer()
        return

    st.caption(f"Market data as of: **{latest_date}**")

    # ── Top Movers ───────────────────────────────────────────────
    col1, col2 = st.columns(2)

    with col1:
        _render_top_gainers(con, latest_date)

    with col2:
        _render_top_losers(con, latest_date)

    st.divider()

    # ── Volume Leaders ───────────────────────────────────────────
    _render_volume_leaders(con, latest_date)

    st.divider()

    # ── Market Breadth ───────────────────────────────────────────
    _render_breadth_summary(con, latest_date)

    st.divider()

    # ── Recent Announcements ─────────────────────────────────────
    _render_recent_announcements(con)

    render_footer()


def _get_latest_date(con) -> str | None:
    """Get the most recent trading date from EOD data."""
    try:
        row = con.execute("SELECT MAX(date) FROM eod_ohlcv").fetchone()
        return row[0] if row and row[0] else None
    except Exception:
        return None


def _render_top_gainers(con, date: str):
    """Top 10 gainers by change percentage."""
    st.markdown("### Top Gainers")
    try:
        df = pd.read_sql_query(
            """SELECT symbol, close, prev_close,
                      ROUND((close - prev_close) / prev_close * 100, 2) as change_pct,
                      volume
               FROM eod_ohlcv
               WHERE date = ? AND prev_close > 0 AND close > prev_close
               ORDER BY change_pct DESC
               LIMIT 10""",
            con, params=(date,),
        )
        if not df.empty:
            df.columns = ["Symbol", "Close", "Prev Close", "Change %", "Volume"]
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No gainers data for this date.")
    except Exception as e:
        st.error(f"Error loading gainers: {e}")


def _render_top_losers(con, date: str):
    """Top 10 losers by change percentage."""
    st.markdown("### Top Losers")
    try:
        df = pd.read_sql_query(
            """SELECT symbol, close, prev_close,
                      ROUND((close - prev_close) / prev_close * 100, 2) as change_pct,
                      volume
               FROM eod_ohlcv
               WHERE date = ? AND prev_close > 0 AND close < prev_close
               ORDER BY change_pct ASC
               LIMIT 10""",
            con, params=(date,),
        )
        if not df.empty:
            df.columns = ["Symbol", "Close", "Prev Close", "Change %", "Volume"]
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No losers data for this date.")
    except Exception as e:
        st.error(f"Error loading losers: {e}")


def _render_volume_leaders(con, date: str):
    """Top 10 stocks by volume."""
    st.markdown("### Volume Leaders")
    try:
        df = pd.read_sql_query(
            """SELECT symbol, close, volume,
                      ROUND((close - prev_close) / prev_close * 100, 2) as change_pct
               FROM eod_ohlcv
               WHERE date = ? AND volume > 0
               ORDER BY volume DESC
               LIMIT 10""",
            con, params=(date,),
        )
        if not df.empty:
            df.columns = ["Symbol", "Close", "Volume", "Change %"]
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No volume data for this date.")
    except Exception as e:
        st.error(f"Error loading volume leaders: {e}")


def _render_breadth_summary(con, date: str):
    """Market breadth — gainers vs losers vs unchanged."""
    st.markdown("### Market Breadth")
    try:
        row = con.execute(
            """SELECT
                 SUM(CASE WHEN close > prev_close THEN 1 ELSE 0 END) as gainers,
                 SUM(CASE WHEN close < prev_close THEN 1 ELSE 0 END) as losers,
                 SUM(CASE WHEN close = prev_close THEN 1 ELSE 0 END) as unchanged,
                 COUNT(*) as total,
                 SUM(volume) as total_volume,
                 SUM(close * volume) as total_value
               FROM eod_ohlcv
               WHERE date = ? AND prev_close > 0""",
            (date,),
        ).fetchone()
        if row:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Gainers", row[0] or 0)
            c2.metric("Losers", row[1] or 0)
            c3.metric("Unchanged", row[2] or 0)
            total_vol = row[4] or 0
            if total_vol >= 1e6:
                c4.metric("Total Volume", f"{total_vol / 1e6:.1f}M")
            else:
                c4.metric("Total Volume", f"{total_vol:,.0f}")
    except Exception as e:
        st.error(f"Error loading breadth: {e}")


def _render_recent_announcements(con):
    """Recent corporate announcements."""
    st.markdown("### Recent Announcements")
    try:
        df = pd.read_sql_query(
            """SELECT symbol, announcement_date, subject
               FROM company_announcements
               ORDER BY announcement_date DESC
               LIMIT 10""",
            con,
        )
        if not df.empty:
            df.columns = ["Symbol", "Date", "Subject"]
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No recent announcements. Run `pfsync announcements sync` to fetch.")
    except Exception:
        st.info("No announcements data available.")
