"""Dashboard page — market overview with KPIs and charts."""

import pandas as pd
import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False
    st_autorefresh = None

from psx_ohlcv.api_client import get_client
from psx_ohlcv.config import get_db_path
from psx_ohlcv.services import (
    is_service_running,
    read_status as read_service_status,
)
from psx_ohlcv.ui.charts import (
    make_market_breadth_chart,
    make_top_movers_chart,
)
from psx_ohlcv.ui.components.helpers import (
    check_data_staleness,
    DATA_QUALITY_NOTICE,
    format_volume,
    get_freshness_badge,
    render_data_info,
    render_data_warning,
    render_footer,
    render_market_status_badge,
)


def render_dashboard():
    """Main dashboard with KPIs, market breadth, and top movers."""

    # =================================================================
    # AUTO-REFRESH WHEN SERVICE IS RUNNING
    # =================================================================
    service_running, _ = is_service_running()
    if service_running and HAS_AUTOREFRESH and st_autorefresh:
        # Refresh every 60 seconds (60000 ms)
        st_autorefresh(interval=60000, limit=None, key="dashboard_autorefresh")

    try:
        client = get_client()
        con = client.connection  # For raw SQL pass-through

        # =================================================================
        # HEADER: Title + Market Status + Data Freshness
        # =================================================================
        header_col1, header_col2, header_col3 = st.columns([2, 1, 1])

        with header_col1:
            st.markdown("## \U0001f4ca Market Dashboard")
            st.caption("Pakistan Stock Exchange \u2022 Real-time Analytics")

        with header_col2:
            # Market Status Badge
            render_market_status_badge()

        with header_col3:
            # Data Freshness + Service Status
            days_old, latest_date = client.get_data_freshness()
            badge_color, badge_text = get_freshness_badge(days_old)
            service_status = read_service_status()
            if latest_date:
                freshness_color = "#00C853" if badge_color == "green" else "#FFC107" if badge_color == "orange" else "#FF1744"
                sync_indicator = "\U0001f7e2" if service_running else "\U0001f534"
                st.markdown(
                    f'<div style="text-align: right; font-size: 12px;">'
                    f'<span style="color: {freshness_color};">\u25cf</span> Data: {badge_text}<br>'
                    f'<span style="color: #888;">As of {latest_date}</span><br>'
                    f'{sync_indicator} Auto-Sync: {"ON" if service_running else "OFF"}</div>',
                    unsafe_allow_html=True
                )

        st.markdown("---")

        # =================================================================
        # DATA STALENESS WARNING
        # =================================================================
        is_stale, stale_msg = check_data_staleness(con)
        if is_stale:
            render_data_warning(
                f"{stale_msg}. Consider syncing fresh data from the Settings page.",
                icon="\U0001f4c5"
            )

        # =================================================================
        # KSE-100 INDEX DISPLAY - Primary Market Benchmark
        # =================================================================
        try:
            # Try to get real KSE-100 index data first
            kse100_data = client.get_latest_kse100()

            # Get market breadth data - use eod_ohlcv for reliable data
            market_perf = con.execute("""
                WITH best_date AS (
                    SELECT date
                    FROM eod_ohlcv
                    GROUP BY date
                    HAVING COUNT(DISTINCT symbol) >= 100
                    ORDER BY date DESC
                    LIMIT 1
                ),
                today AS (
                    SELECT symbol, close, volume
                    FROM eod_ohlcv
                    WHERE date = (SELECT date FROM best_date)
                ),
                prev AS (
                    SELECT symbol, close as prev_close
                    FROM eod_ohlcv
                    WHERE date = (SELECT MAX(date) FROM eod_ohlcv WHERE date < (SELECT date FROM best_date))
                ),
                changes AS (
                    SELECT
                        t.symbol,
                        t.volume,
                        CASE
                            WHEN p.prev_close > 0 THEN ((t.close - p.prev_close) / p.prev_close) * 100
                            ELSE 0
                        END as change_percent
                    FROM today t
                    LEFT JOIN prev p ON t.symbol = p.symbol
                )
                SELECT
                    COUNT(*) as total_stocks,
                    SUM(CASE WHEN change_percent > 0.01 THEN 1 ELSE 0 END) as gainers,
                    SUM(CASE WHEN change_percent < -0.01 THEN 1 ELSE 0 END) as losers,
                    SUM(CASE WHEN change_percent BETWEEN -0.01 AND 0.01 THEN 1 ELSE 0 END) as unchanged,
                    ROUND(AVG(change_percent), 2) as avg_change,
                    SUM(volume) as total_volume,
                    NULL as total_turnover
                FROM changes
            """).fetchone()

            if kse100_data:
                # ===== REAL KSE-100 DATA =====
                idx_col1, idx_col2, idx_col3 = st.columns([2, 1, 1])

                with idx_col1:
                    value = kse100_data.get("value", 0)
                    change = kse100_data.get("change", 0) or 0
                    change_pct = kse100_data.get("change_pct", 0) or 0

                    # Color based on change
                    if change > 0:
                        idx_color = "#00C853"
                        arrow = "\u25b2"
                        change_sign = "+"
                    elif change < 0:
                        idx_color = "#FF1744"
                        arrow = "\u25bc"
                        change_sign = ""
                    else:
                        idx_color = "#78909C"
                        arrow = "\u25cf"
                        change_sign = ""

                    st.markdown(f"""
                    <div style="background: linear-gradient(135deg, rgba(33,150,243,0.15) 0%, rgba(33,150,243,0.05) 100%);
                                border: 1px solid rgba(33,150,243,0.3); border-radius: 12px; padding: 20px;">
                        <div style="font-size: 12px; color: #888; margin-bottom: 4px;">
                            \U0001f4ca KSE-100 Index
                        </div>
                        <div style="display: flex; align-items: baseline; gap: 12px; flex-wrap: wrap;">
                            <span style="font-size: 32px; font-weight: 700; font-family: monospace;">
                                {value:,.2f}
                            </span>
                            <span style="font-size: 18px; font-weight: 600; color: {idx_color}; font-family: monospace;">
                                {arrow} {change_sign}{change:,.2f} ({change_sign}{change_pct:.2f}%)
                            </span>
                        </div>
                        <div style="font-size: 11px; color: #666; margin-top: 8px;">
                            Date: {kse100_data.get("index_date", "N/A")}
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

                with idx_col2:
                    # Index Details - High/Low/Volume
                    high = kse100_data.get("high")
                    low = kse100_data.get("low")
                    volume = kse100_data.get("volume")
                    vol_str = f"{volume/1e6:.0f}M" if volume and volume >= 1e6 else (f"{volume:,}" if volume else "N/A")

                    st.markdown(f"""
                    <div style="background: rgba(255,255,255,0.02); border-radius: 8px; padding: 16px;
                                border: 1px solid rgba(255,255,255,0.1);">
                        <div style="font-size: 12px; color: #888; margin-bottom: 8px;">Today's Range</div>
                        <div style="font-family: monospace; font-size: 14px;">
                            <div style="display: flex; justify-content: space-between; margin-bottom: 4px;">
                                <span style="color: #888;">High:</span>
                                <span style="color: #00C853;">{high:,.2f if high else 'N/A'}</span>
                            </div>
                            <div style="display: flex; justify-content: space-between; margin-bottom: 4px;">
                                <span style="color: #888;">Low:</span>
                                <span style="color: #FF1744;">{low:,.2f if low else 'N/A'}</span>
                            </div>
                            <div style="display: flex; justify-content: space-between;">
                                <span style="color: #888;">Volume:</span>
                                <span>{vol_str}</span>
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

                with idx_col3:
                    # 52-Week Range and YTD
                    week_52_low = kse100_data.get("week_52_low")
                    week_52_high = kse100_data.get("week_52_high")
                    ytd_pct = kse100_data.get("ytd_change_pct")
                    one_year_pct = kse100_data.get("one_year_change_pct")

                    ytd_color = "#00C853" if ytd_pct and ytd_pct > 0 else "#FF1744" if ytd_pct and ytd_pct < 0 else "#888"
                    yr_color = "#00C853" if one_year_pct and one_year_pct > 0 else "#FF1744" if one_year_pct and one_year_pct < 0 else "#888"

                    st.markdown(f"""
                    <div style="background: rgba(255,255,255,0.02); border-radius: 8px; padding: 16px;
                                border: 1px solid rgba(255,255,255,0.1);">
                        <div style="font-size: 12px; color: #888; margin-bottom: 8px;">Performance</div>
                        <div style="font-family: monospace; font-size: 14px;">
                            <div style="display: flex; justify-content: space-between; margin-bottom: 4px;">
                                <span style="color: #888;">YTD:</span>
                                <span style="color: {ytd_color};">{ytd_pct:+.2f}%</span>
                            </div>
                            <div style="display: flex; justify-content: space-between; margin-bottom: 4px;">
                                <span style="color: #888;">1-Year:</span>
                                <span style="color: {yr_color};">{one_year_pct:+.2f}%</span>
                            </div>
                            <div style="font-size: 11px; color: #666; margin-top: 6px;">
                                52W: {week_52_low:,.0f if week_52_low else 'N/A'} - {week_52_high:,.0f if week_52_high else 'N/A'}
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

                # Show market breadth below if available
                if market_perf and market_perf["total_stocks"] > 0:
                    gainers = market_perf["gainers"] or 0
                    losers = market_perf["losers"] or 0
                    turnover = market_perf["total_turnover"] or 0
                    turnover_str = f"Rs.{turnover/1e9:.2f}B" if turnover >= 1e9 else f"Rs.{turnover/1e6:.0f}M" if turnover >= 1e6 else f"Rs.{turnover:,.0f}"

                    st.markdown(f"""
                    <div style="display: flex; gap: 24px; margin-top: 12px; font-size: 13px;">
                        <span style="color: #888;">Market Breadth:</span>
                        <span style="color: #00C853;">{gainers} Gainers</span>
                        <span style="color: #FF1744;">{losers} Losers</span>
                        <span style="color: #888; margin-left: auto;">Turnover: {turnover_str}</span>
                    </div>
                    """, unsafe_allow_html=True)

            elif market_perf and market_perf["total_stocks"] > 0:
                # ===== FALLBACK: PROXY DATA =====
                idx_col1, idx_col2, idx_col3 = st.columns([2, 1, 1])

                with idx_col1:
                    avg_change = market_perf["avg_change"] or 0
                    gainers = market_perf["gainers"] or 0
                    losers = market_perf["losers"] or 0

                    # Color based on market direction
                    if avg_change > 0:
                        idx_color = "#00C853"
                        arrow = "\u25b2"
                    elif avg_change < 0:
                        idx_color = "#FF1744"
                        arrow = "\u25bc"
                    else:
                        idx_color = "#78909C"
                        arrow = "\u25cf"

                    st.markdown(f"""
                    <div style="background: linear-gradient(135deg, rgba(33,150,243,0.1) 0%, rgba(33,150,243,0.05) 100%);
                                border: 1px solid rgba(33,150,243,0.2); border-radius: 12px; padding: 20px;">
                        <div style="font-size: 12px; color: #888; margin-bottom: 4px;">
                            \U0001f4ca KSE-100 Index Proxy (Market Average)
                        </div>
                        <div style="display: flex; align-items: baseline; gap: 12px;">
                            <span style="font-size: 28px; font-weight: 700; color: {idx_color}; font-family: monospace;">
                                {arrow} {avg_change:+.2f}%
                            </span>
                            <span style="font-size: 14px; color: #888;">
                                Avg change across {market_perf["total_stocks"]} stocks
                            </span>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

                with idx_col2:
                    # Market Breadth
                    st.markdown(f"""
                    <div style="background: rgba(255,255,255,0.02); border-radius: 8px; padding: 16px;
                                border: 1px solid rgba(255,255,255,0.1);">
                        <div style="font-size: 12px; color: #888; margin-bottom: 8px;">Market Breadth</div>
                        <div style="display: flex; gap: 16px;">
                            <div>
                                <span style="color: #00C853; font-size: 20px; font-weight: 600;">{gainers}</span>
                                <span style="font-size: 11px; color: #888;"> Gainers</span>
                            </div>
                            <div>
                                <span style="color: #FF1744; font-size: 20px; font-weight: 600;">{losers}</span>
                                <span style="font-size: 11px; color: #888;"> Losers</span>
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

                with idx_col3:
                    # Turnover
                    turnover = market_perf["total_turnover"] or 0
                    if turnover >= 1e9:
                        turnover_str = f"Rs.{turnover/1e9:.2f}B"
                    elif turnover >= 1e6:
                        turnover_str = f"Rs.{turnover/1e6:.0f}M"
                    else:
                        turnover_str = f"Rs.{turnover:,.0f}"

                    st.markdown(f"""
                    <div style="background: rgba(255,255,255,0.02); border-radius: 8px; padding: 16px;
                                border: 1px solid rgba(255,255,255,0.1);">
                        <div style="font-size: 12px; color: #888; margin-bottom: 8px;">Total Turnover</div>
                        <div style="font-size: 20px; font-weight: 600; font-family: monospace;">
                            {turnover_str}
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

                st.markdown("")
        except Exception as e:
            # Show user-friendly error instead of silent failure
            render_data_info(
                "Index data temporarily unavailable. Showing available market data.",
                icon="\U0001f4ca"
            )

        # =================================================================
        # PRIMARY KPIs ROW - Key metrics traders care about
        # =================================================================
        kpi_col1, kpi_col2, kpi_col3, kpi_col4, kpi_col5 = st.columns(5)

        # Get deep data stats
        deep_stats = con.execute("""
            SELECT
                COUNT(DISTINCT symbol) as deep_symbols,
                MAX(snapshot_date) as latest_snapshot
            FROM company_snapshots
        """).fetchone()
        deep_count = deep_stats["deep_symbols"] if deep_stats else 0

        # Get trading session stats - use date with meaningful data (at least 100 symbols)
        session_stats = con.execute("""
            WITH best_date AS (
                SELECT session_date
                FROM trading_sessions
                WHERE market_type = 'REG'
                GROUP BY session_date
                HAVING COUNT(DISTINCT symbol) >= 100
                ORDER BY session_date DESC
                LIMIT 1
            )
            SELECT
                SUM(volume) as total_volume,
                SUM(turnover) as total_turnover,
                COUNT(DISTINCT symbol) as active_symbols,
                (SELECT session_date FROM best_date) as data_date
            FROM trading_sessions
            WHERE session_date = (SELECT session_date FROM best_date)
            AND market_type = 'REG'
        """).fetchone()

        # Fallback to eod_ohlcv if trading_sessions has no good data
        if not session_stats or not session_stats["active_symbols"] or session_stats["active_symbols"] < 10:
            session_stats = con.execute("""
                WITH best_date AS (
                    SELECT date
                    FROM eod_ohlcv
                    GROUP BY date
                    HAVING COUNT(DISTINCT symbol) >= 100
                    ORDER BY date DESC
                    LIMIT 1
                )
                SELECT
                    SUM(volume) as total_volume,
                    NULL as total_turnover,
                    COUNT(DISTINCT symbol) as active_symbols,
                    (SELECT date FROM best_date) as data_date
                FROM eod_ohlcv
                WHERE date = (SELECT date FROM best_date)
            """).fetchone()

        total_vol = session_stats["total_volume"] if session_stats else 0
        active_count = session_stats["active_symbols"] if session_stats else 0

        with kpi_col1:
            st.metric(
                "\U0001f3e2 Companies",
                f"{deep_count:,}",
                help="Companies with deep data profiles"
            )

        with kpi_col2:
            st.metric(
                "\U0001f4c8 Active Today",
                f"{active_count:,}",
                help="Symbols traded today"
            )

        with kpi_col3:
            vol_str = format_volume(total_vol) if total_vol else "N/A"
            st.metric(
                "\U0001f4ca Total Volume",
                vol_str,
                help="Combined volume across all symbols"
            )

        with kpi_col4:
            # EOD data coverage
            eod_count = con.execute("SELECT COUNT(*) FROM eod_ohlcv").fetchone()[0]
            st.metric(
                "\U0001f4c5 Historical Days",
                f"{eod_count:,}",
                help="Total OHLCV records in database"
            )

        with kpi_col5:
            # Announcements today
            ann_count = con.execute("""
                SELECT COUNT(*) FROM corporate_announcements
                WHERE announcement_date = date('now')
            """).fetchone()[0]
            st.metric(
                "\U0001f4e3 Announcements",
                f"{ann_count}",
                help="Corporate announcements today"
            )

        st.markdown("")  # Spacing

        # =====================================================================
        # PSX-Style Trading Segments Summary
        # =====================================================================
        try:
            # Get trading segments data - use date with meaningful data
            segments_query = """
                WITH best_date AS (
                    SELECT session_date
                    FROM trading_sessions
                    WHERE market_type = 'REG'
                    GROUP BY session_date
                    HAVING COUNT(DISTINCT symbol) >= 50
                    ORDER BY session_date DESC
                    LIMIT 1
                )
                SELECT
                    market_type,
                    COUNT(*) as symbols,
                    SUM(volume) as total_volume,
                    AVG(volume) as avg_volume
                FROM trading_sessions
                WHERE session_date = (SELECT session_date FROM best_date)
                GROUP BY market_type
                ORDER BY total_volume DESC
            """
            segments_df = pd.read_sql_query(segments_query, con)

            if not segments_df.empty:
                st.subheader("\U0001f4ca Trading Segments")

                market_labels = {
                    "REG": "Regular Market",
                    "FUT": "Deliverable Futures",
                    "CSF": "Cash Settled Futures",
                    "ODL": "Odd Lot"
                }

                seg_cols = st.columns(len(segments_df))
                for i, row in segments_df.iterrows():
                    with seg_cols[i]:
                        market = row["market_type"]
                        label = market_labels.get(market, market)
                        vol = row["total_volume"]
                        count = row["symbols"]

                        # Format volume
                        if vol >= 1e9:
                            vol_str = f"{vol/1e9:.2f}B"
                        elif vol >= 1e6:
                            vol_str = f"{vol/1e6:.2f}M"
                        else:
                            vol_str = f"{vol:,.0f}"

                        st.metric(
                            label,
                            vol_str,
                            f"{count} symbols",
                            help=f"Total volume in {label}"
                        )

                st.markdown("---")
        except Exception:
            # Trading segments data not critical, continue gracefully
            pass

        # =====================================================================
        # Volume Leaders & 52-Week Range Indicators
        # =====================================================================
        try:
            vol_52w_cols = st.columns(2)

            with vol_52w_cols[0]:
                # Top Volume Leaders - use eod_ohlcv for more reliable data
                volume_query = """
                    WITH best_date AS (
                        SELECT date
                        FROM eod_ohlcv
                        GROUP BY date
                        HAVING COUNT(DISTINCT symbol) >= 100
                        ORDER BY date DESC
                        LIMIT 1
                    ),
                    today AS (
                        SELECT symbol, close, volume
                        FROM eod_ohlcv
                        WHERE date = (SELECT date FROM best_date)
                    ),
                    prev AS (
                        SELECT symbol, close as prev_close
                        FROM eod_ohlcv
                        WHERE date = (SELECT MAX(date) FROM eod_ohlcv WHERE date < (SELECT date FROM best_date))
                    )
                    SELECT
                        t.symbol,
                        t.volume,
                        t.close as price,
                        p.prev_close as ldcp,
                        ROUND(((t.close - p.prev_close) / p.prev_close) * 100, 2) as change_percent
                    FROM today t
                    LEFT JOIN prev p ON t.symbol = p.symbol
                    WHERE t.volume > 0
                    ORDER BY t.volume DESC
                    LIMIT 5
                """
                vol_df = pd.read_sql_query(volume_query, con)

                if not vol_df.empty:
                    st.markdown("**\U0001f4c8 Volume Leaders**")
                    for _, row in vol_df.iterrows():
                        vol = row["volume"]
                        vol_str = f"{vol/1e6:.2f}M" if vol >= 1e6 else f"{vol:,.0f}"
                        change = row["change_percent"] or 0
                        color = "\U0001f7e2" if change > 0 else "\U0001f534" if change < 0 else "\u26aa"
                        st.caption(f"{color} **{row['symbol']}** - {vol_str} ({change:+.2f}%)")

            with vol_52w_cols[1]:
                # 52-Week Range Indicators - use date with meaningful data
                range_query = """
                    WITH best_date AS (
                        SELECT session_date
                        FROM trading_sessions
                        WHERE market_type = 'REG'
                        AND week_52_high > 0 AND week_52_low > 0
                        GROUP BY session_date
                        HAVING COUNT(DISTINCT symbol) >= 100
                        ORDER BY session_date DESC
                        LIMIT 1
                    )
                    SELECT
                        ts.symbol,
                        COALESCE(ts.close, ts.high, ts.ldcp) as price,
                        ts.week_52_low,
                        ts.week_52_high,
                        CASE WHEN (ts.week_52_high - ts.week_52_low) > 0
                            THEN ROUND((COALESCE(ts.close, ts.high, ts.ldcp) - ts.week_52_low) / (ts.week_52_high - ts.week_52_low) * 100, 1)
                            ELSE 50
                        END as position_pct
                    FROM trading_sessions ts
                    WHERE ts.session_date = (SELECT session_date FROM best_date)
                    AND ts.market_type = 'REG'
                    AND ts.week_52_high > 0
                    AND ts.week_52_low > 0
                    AND COALESCE(ts.close, ts.high, ts.ldcp) > 0
                    ORDER BY position_pct DESC
                    LIMIT 3
                """
                high_df = pd.read_sql_query(range_query, con)

                low_query = """
                    WITH best_date AS (
                        SELECT session_date
                        FROM trading_sessions
                        WHERE market_type = 'REG'
                        AND week_52_high > 0 AND week_52_low > 0
                        GROUP BY session_date
                        HAVING COUNT(DISTINCT symbol) >= 100
                        ORDER BY session_date DESC
                        LIMIT 1
                    )
                    SELECT
                        ts.symbol,
                        COALESCE(ts.close, ts.high, ts.ldcp) as price,
                        ts.week_52_low,
                        ts.week_52_high,
                        CASE WHEN (ts.week_52_high - ts.week_52_low) > 0
                            THEN ROUND((COALESCE(ts.close, ts.high, ts.ldcp) - ts.week_52_low) / (ts.week_52_high - ts.week_52_low) * 100, 1)
                            ELSE 50
                        END as position_pct
                    FROM trading_sessions ts
                    WHERE ts.session_date = (SELECT session_date FROM best_date)
                    AND ts.market_type = 'REG'
                    AND ts.week_52_high > 0
                    AND ts.week_52_low > 0
                    AND COALESCE(ts.close, ts.high, ts.ldcp) > 0
                    ORDER BY position_pct ASC
                    LIMIT 3
                """
                low_df = pd.read_sql_query(low_query, con)

                st.markdown("**\U0001f4ca 52-Week Range**")
                if not high_df.empty:
                    st.caption("Near 52W High:")
                    for _, row in high_df.iterrows():
                        st.caption(f"  \U0001f53a **{row['symbol']}** ({row['position_pct']:.0f}% of range)")

                if not low_df.empty:
                    st.caption("Near 52W Low:")
                    for _, row in low_df.iterrows():
                        st.caption(f"  \U0001f53b **{row['symbol']}** ({row['position_pct']:.0f}% of range)")

            st.markdown("---")
        except Exception:
            # Volume leaders/52-week range not critical, continue gracefully
            pass

        # Market Breadth and Top Movers (from analytics tables)
        try:
            from psx_ohlcv.sources.regular_market import init_regular_market_schema
            if con:
                init_regular_market_schema(con)
            client.init_analytics()

            # Get analytics from pre-computed tables
            market_analytics = client.get_latest_market_analytics()

            if market_analytics:
                st.subheader("\U0001f4c8 Market Overview")

                # Use pre-computed analytics
                gainers = market_analytics.get("gainers_count", 0)
                losers = market_analytics.get("losers_count", 0)
                unchanged = market_analytics.get("unchanged_count", 0)
                ts = market_analytics.get("ts", "N/A")

                st.caption(f"As of: {ts[:19] if ts and ts != 'N/A' else 'N/A'}")

                col1, col2, col3 = st.columns([1, 1, 1])

                with col1:
                    # Market breadth donut chart
                    breadth_fig = make_market_breadth_chart(
                        gainers=gainers,
                        losers=losers,
                        unchanged=unchanged,
                        height=300,
                    )
                    st.plotly_chart(breadth_fig, use_container_width=True)

                with col2:
                    # Top 5 Gainers from analytics table
                    top_gainers_df = client.get_top_list("gainers", limit=5)
                    if not top_gainers_df.empty:
                        gainers_fig = make_top_movers_chart(
                            top_gainers_df[["symbol", "change_pct"]],
                            title="Top 5 Gainers",
                            chart_type="gainers",
                            height=300,
                        )
                        st.plotly_chart(gainers_fig, use_container_width=True)
                        # Quick links to company analytics
                        gainer_symbols = top_gainers_df["symbol"].tolist()[:3]
                        gcols = st.columns(len(gainer_symbols))
                        for i, sym in enumerate(gainer_symbols):
                            with gcols[i]:
                                if st.button(f"\U0001f4c8 {sym}", key=f"dash_gainer_{sym}"):
                                    st.session_state.company_symbol = sym
                                    st.session_state.nav_to = "\U0001f3e2 Company Analytics"
                                    st.rerun()

                with col3:
                    # Top 5 Losers from analytics table
                    top_losers_df = client.get_top_list("losers", limit=5)
                    if not top_losers_df.empty:
                        losers_fig = make_top_movers_chart(
                            top_losers_df[["symbol", "change_pct"]],
                            title="Top 5 Losers",
                            chart_type="losers",
                            height=300,
                        )
                        st.plotly_chart(losers_fig, use_container_width=True)
                        # Quick links to company analytics
                        loser_symbols = top_losers_df["symbol"].tolist()[:3]
                        lcols = st.columns(len(loser_symbols))
                        for i, sym in enumerate(loser_symbols):
                            with lcols[i]:
                                if st.button(f"\U0001f4c9 {sym}", key=f"dash_loser_{sym}"):
                                    st.session_state.company_symbol = sym
                                    st.session_state.nav_to = "\U0001f3e2 Company Analytics"
                                    st.rerun()

                st.markdown("---")

                # Sector Leaderboard
                st.subheader("\U0001f4ca Sector Performance")
                sector_df = client.get_sector_leaderboard()
                if not sector_df.empty:
                    # Display sector table
                    display_cols = [
                        "sector_name", "symbols_count", "avg_change_pct",
                        "sum_volume", "top_symbol"
                    ]
                    display_cols = [c for c in display_cols if c in sector_df.columns]
                    st.dataframe(
                        sector_df[display_cols].head(10),
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "sector_name": st.column_config.TextColumn(
                                "Sector", width="medium"
                            ),
                            "symbols_count": st.column_config.NumberColumn(
                                "Symbols", format="%d"
                            ),
                            "avg_change_pct": st.column_config.NumberColumn(
                                "Avg Change %", format="%.2f"
                            ),
                            "sum_volume": st.column_config.NumberColumn(
                                "Total Volume", format="%,.0f"
                            ),
                            "top_symbol": st.column_config.TextColumn(
                                "Top Performer", width="small"
                            ),
                        }
                    )
                else:
                    st.info("No sector data available yet.")

                st.markdown("---")

        except Exception:
            pass  # Analytics data not available

        # Recent sync runs table (limit 10)
        st.subheader("Recent Sync Runs")
        runs_df = client.get_sync_runs(limit=10)

        if runs_df.empty:
            st.info("No sync runs yet. Run `psxsync sync --all` to start.")
        else:
            runs_df.columns = [
                "Run ID", "Started", "Ended", "Mode",
                "Total", "OK", "Failed", "Rows"
            ]
            st.dataframe(runs_df, use_container_width=True, hide_index=True)

        # Data quality indicator
        st.markdown("---")
        with st.expander("\u2139\ufe0f Data Quality Information", expanded=False):
            st.markdown(DATA_QUALITY_NOTICE)
            st.markdown("""
**Data Sources:**
- EOD Time Series: `dps.psx.com.pk/timeseries/eod/{symbol}`
- Market Watch: `dps.psx.com.pk/market-watch`

**Fields Provided by PSX API:**
| Field | Source |
|-------|--------|
| Open | Direct from API |
| Close | Direct from API |
| Volume | Direct from API |
| High | Derived: max(open, close) |
| Low | Derived: min(open, close) |
""")

    except Exception as e:
        st.error(f"Database error: {e}")
        st.info(f"Expected database at: {get_db_path()}")

    render_footer()
