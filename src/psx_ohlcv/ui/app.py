"""
PSX OHLCV Explorer - Streamlit Dashboard.

Run with: streamlit run src/psx_ohlcv/ui/app.py
"""

from datetime import datetime, timedelta
import sys
import time
from pathlib import Path

import pandas as pd
import streamlit as st

# Add src to path to allow running directly without installation
try:
    # Navigate up from src/psx_ohlcv/ui/app.py to src/
    src_path = Path(__file__).resolve().parents[2]
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))
except Exception:
    pass

from psx_ohlcv import init_schema
from psx_ohlcv.analytics import (
    get_current_market_with_sectors,
    get_latest_market_analytics,
    get_sector_leaderboard,
    get_top_list,
    init_analytics_schema,
)
from psx_ohlcv.config import (
    DATA_ROOT,
    DEFAULT_DB_PATH,
    DEFAULT_LOG_FILE,
    DEFAULT_SYNC_CONFIG,
    SyncConfig,
    get_db_path,
)
from psx_ohlcv.db import get_sector_map
from psx_ohlcv.query import (
    get_company_latest_signals,
    get_company_quote_stats,
    get_company_quotes,
    get_intraday_latest,
    get_intraday_stats,
    get_ohlcv_range,
    get_symbols_list,
    get_symbols_string,
    get_symbols_with_profiles,
)
from psx_ohlcv.sync import sync_all, sync_intraday
from psx_ohlcv.ui.charts import (
    make_candlestick,
    make_intraday_chart,
    make_market_breadth_chart,
    make_price_line,
    make_top_movers_chart,
    make_volume_chart,
)
from psx_ohlcv.ui.session_tracker import (
    get_session_id,
    init_session_tracking,
    render_session_activity_panel,
    track_button_click,
    track_download,
    track_page_visit,
    track_refresh,
    track_symbol_search,
)

# Deep scraper imports for Bloomberg-style data
from psx_ohlcv.sources.deep_scraper import (
    deep_scrape_batch,
    deep_scrape_symbol,
)
from psx_ohlcv.db import (
    get_company_snapshot,
    get_trading_sessions,
    get_corporate_announcements,
)

# Page config - must be first Streamlit command
st.set_page_config(
    page_title="PSX OHLCV Explorer",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =============================================================================
# TRADING UI DESIGN SYSTEM
# Professional trading terminal aesthetic with trader-centric UX
# =============================================================================

TRADING_CSS = """
<style>
/* === Color Palette === */
:root {
    --gain-color: #00C853;
    --loss-color: #FF1744;
    --neutral-color: #78909C;
    --accent-color: #2196F3;
    --warning-color: #FFC107;
    --bg-card: rgba(255, 255, 255, 0.02);
    --border-color: rgba(255, 255, 255, 0.1);
}

/* === Typography for Numbers === */
.stMetric [data-testid="stMetricValue"] {
    font-family: 'JetBrains Mono', 'SF Mono', 'Consolas', monospace;
    font-weight: 600;
}

/* === Metric Cards Enhancement === */
[data-testid="stMetric"] {
    background: var(--bg-card);
    border: 1px solid var(--border-color);
    border-radius: 8px;
    padding: 12px 16px;
}

/* === Price Change Colors === */
.price-up { color: #00C853 !important; }
.price-down { color: #FF1744 !important; }
.price-neutral { color: #78909C !important; }

/* === Data Tables === */
.stDataFrame {
    font-family: 'JetBrains Mono', 'SF Mono', monospace;
    font-size: 13px;
}

/* === Section Headers === */
.section-header {
    border-left: 4px solid #2196F3;
    padding-left: 12px;
    margin: 24px 0 16px 0;
}

/* === KPI Row === */
.kpi-row {
    display: flex;
    gap: 16px;
    margin-bottom: 24px;
}

/* === Ticker Tape Style === */
.ticker-item {
    display: inline-block;
    padding: 4px 12px;
    margin: 2px 4px;
    border-radius: 4px;
    font-family: monospace;
    font-size: 13px;
}
.ticker-up { background: rgba(0, 200, 83, 0.15); border: 1px solid rgba(0, 200, 83, 0.3); }
.ticker-down { background: rgba(255, 23, 68, 0.15); border: 1px solid rgba(255, 23, 68, 0.3); }

/* === Market Status Badge === */
.market-status {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 4px 12px;
    border-radius: 20px;
    font-size: 12px;
    font-weight: 600;
}
.market-open { background: rgba(0, 200, 83, 0.2); color: #00C853; }
.market-closed { background: rgba(255, 23, 68, 0.2); color: #FF1744; }

/* === Compact Info Cards === */
.info-card {
    background: linear-gradient(135deg, rgba(33, 150, 243, 0.1) 0%, rgba(33, 150, 243, 0.05) 100%);
    border: 1px solid rgba(33, 150, 243, 0.2);
    border-radius: 8px;
    padding: 16px;
    margin-bottom: 16px;
}

/* === Sidebar Styling === */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, rgba(0,0,0,0.02) 0%, rgba(0,0,0,0.05) 100%);
}

/* === Button Improvements === */
.stButton > button {
    border-radius: 6px;
    font-weight: 500;
    transition: all 0.2s ease;
}

/* === Progress Bars === */
.stProgress > div > div {
    border-radius: 4px;
}

/* === Expander Headers === */
.streamlit-expanderHeader {
    font-weight: 600;
    font-size: 14px;
}

/* === Tab Styling === */
.stTabs [data-baseweb="tab-list"] {
    gap: 8px;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 6px 6px 0 0;
    padding: 8px 16px;
}

/* === Announcement Cards === */
.announcement-card {
    border-left: 3px solid #FFC107;
    padding-left: 12px;
    margin: 8px 0;
}

/* === Hide Streamlit Branding === */
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
</style>
"""

# Inject custom CSS
st.markdown(TRADING_CSS, unsafe_allow_html=True)


def format_price_change(value: float, include_sign: bool = True) -> str:
    """Format price change with color indicator."""
    if value > 0:
        sign = "+" if include_sign else ""
        return f'<span class="price-up">{sign}{value:.2f}%</span>'
    elif value < 0:
        return f'<span class="price-down">{value:.2f}%</span>'
    else:
        return f'<span class="price-neutral">0.00%</span>'


def format_volume(volume: float) -> str:
    """Format volume with appropriate suffix."""
    if volume >= 1e9:
        return f"{volume/1e9:.2f}B"
    elif volume >= 1e6:
        return f"{volume/1e6:.2f}M"
    elif volume >= 1e3:
        return f"{volume/1e3:.1f}K"
    else:
        return f"{volume:,.0f}"


def format_price(price: float, currency: str = "Rs.") -> str:
    """Format price with currency."""
    if price >= 1000:
        return f"{currency} {price:,.2f}"
    else:
        return f"{currency} {price:.2f}"


def render_market_status_badge():
    """Render market open/closed badge."""
    if is_market_closed():
        st.markdown(
            '<span class="market-status market-closed">● Market Closed</span>',
            unsafe_allow_html=True
        )
    else:
        st.markdown(
            '<span class="market-status market-open">● Market Open</span>',
            unsafe_allow_html=True
        )


def render_ticker_tape(symbols_data: list[dict]):
    """Render a horizontal ticker tape of symbols with changes."""
    html_parts = []
    for item in symbols_data[:10]:
        symbol = item.get("symbol", "")
        change = item.get("change_pct", 0) or 0
        css_class = "ticker-up" if change >= 0 else "ticker-down"
        sign = "+" if change >= 0 else ""
        html_parts.append(
            f'<span class="ticker-item {css_class}">'
            f'<b>{symbol}</b> {sign}{change:.2f}%</span>'
        )
    st.markdown(" ".join(html_parts), unsafe_allow_html=True)


def render_price_card(
    label: str,
    price: float,
    change: float = None,
    change_pct: float = None,
    subtitle: str = None
):
    """Render a price card with change indicator."""
    change_html = ""
    if change_pct is not None:
        color = "#00C853" if change_pct >= 0 else "#FF1744"
        sign = "+" if change_pct >= 0 else ""
        change_html = f' <span style="color: {color}; font-size: 14px;">({sign}{change_pct:.2f}%)</span>'

    st.markdown(
        f"""
        <div style="padding: 12px; background: rgba(255,255,255,0.02); border-radius: 8px; border: 1px solid rgba(255,255,255,0.1);">
            <div style="font-size: 12px; color: #888; margin-bottom: 4px;">{label}</div>
            <div style="font-size: 24px; font-weight: 600; font-family: monospace;">
                Rs. {price:,.2f}{change_html}
            </div>
            {f'<div style="font-size: 11px; color: #666; margin-top: 4px;">{subtitle}</div>' if subtitle else ''}
        </div>
        """,
        unsafe_allow_html=True
    )

# Exports directory
EXPORTS_DIR = DATA_ROOT / "exports"

# OHLCV field tooltips
OHLCV_TOOLTIPS = {
    "open": "Opening price - first traded price of the day",
    "high": "High price - derived from max(open, close). "
            "Note: PSX API doesn't provide actual intraday highs.",
    "low": "Low price - derived from min(open, close). "
           "Note: PSX API doesn't provide actual intraday lows.",
    "close": "Closing price - last traded price of the day",
    "volume": "Volume - total number of shares traded during the day",
}

# Data quality notice
DATA_QUALITY_NOTICE = """
**Data Quality Note:** The PSX API provides only open, close, and volume data.
High/Low values are **derived** from max/min(open, close) and do not represent
actual intraday price extremes. For technical analysis requiring true high/low
values, consider premium data providers.
"""

# PSX market hours (Pakistan Standard Time)
MARKET_OPEN_HOUR = 9
MARKET_CLOSE_HOUR = 15
MARKET_DAYS = [0, 1, 2, 3, 4]  # Monday to Friday


# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
def get_connection():
    """
    Get database connection, initializing schema if needed.

    Note: We don't cache the connection because SQLite connections
    are not thread-safe by default. Streamlit runs in multiple threads.
    """
    import sqlite3 as _sqlite3

    db_path = get_db_path()
    # Use check_same_thread=False to allow connection to be used across threads
    con = _sqlite3.connect(str(db_path), check_same_thread=False)
    con.row_factory = _sqlite3.Row

    # Enable WAL mode for better concurrent access
    con.execute("PRAGMA journal_mode=WAL")

    # Initialize schema
    init_schema(con)
    return con


@st.cache_resource
def get_cached_connection():
    """
    Get a cached database connection with thread-safety enabled.

    Uses check_same_thread=False to allow Streamlit's multi-threaded access.
    """
    import sqlite3 as _sqlite3

    db_path = get_db_path()
    con = _sqlite3.connect(str(db_path), check_same_thread=False)
    con.row_factory = _sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    init_schema(con)
    return con


@st.cache_data(ttl=60)
def get_data_freshness(_con) -> tuple[int | None, str | None]:
    """
    Get data freshness info.

    Returns:
        Tuple of (days_old, latest_date_str) or (None, None) if no data.
    """
    result = _con.execute(
        "SELECT MAX(date) as max_date FROM eod_ohlcv"
    ).fetchone()
    if result and result["max_date"]:
        latest_date = datetime.strptime(result["max_date"], "%Y-%m-%d")
        days_old = (datetime.now() - latest_date).days
        return days_old, result["max_date"]
    return None, None


def is_market_closed() -> bool:
    """Check if PSX market is currently closed."""
    now = datetime.now()
    # Weekend check
    if now.weekday() not in MARKET_DAYS:
        return True
    # After hours check (simplified - doesn't account for PKT timezone)
    if now.hour < MARKET_OPEN_HOUR or now.hour >= MARKET_CLOSE_HOUR:
        return True
    return False


def get_freshness_badge(days_old: int | None) -> tuple[str, str]:
    """
    Get freshness badge color and text.

    Returns:
        Tuple of (badge_color, badge_text)
    """
    if days_old is None:
        return "gray", "No data"
    elif days_old == 0:
        return "green", "Fresh (today)"
    elif days_old == 1:
        return "green", "1 day old"
    elif days_old <= 3:
        return "orange", f"{days_old} days old"
    else:
        return "red", f"{days_old} days old"


@st.cache_data(ttl=300)
def get_sector_names(_con) -> dict[str, str]:
    """
    Get cached sector code to sector name mapping.

    Returns:
        Dict mapping sector codes to sector names.
    """
    try:
        return get_sector_map(_con)
    except Exception:
        return {}


def add_sector_name_column(
    df: pd.DataFrame, sector_map: dict[str, str]
) -> pd.DataFrame:
    """
    Add sector_name column to DataFrame based on sector_code.

    Args:
        df: DataFrame with sector_code column
        sector_map: Dict mapping sector codes to names

    Returns:
        DataFrame with sector_name column added
    """
    if "sector_code" not in df.columns and "sector" not in df.columns:
        return df

    df = df.copy()
    sector_col = "sector_code" if "sector_code" in df.columns else "sector"
    df["sector_name"] = df[sector_col].map(sector_map).fillna("")
    return df


def render_footer():
    """Render footer with data source attribution and session activity."""
    st.markdown("---")

    # Session activity panel
    render_session_activity_panel()

    st.caption(
        "Data source: [PSX DPS](https://dps.psx.com.pk) | "
        f"Session: {get_session_id()} | "
        f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    )


# -----------------------------------------------------------------------------
# Page: Dashboard
# -----------------------------------------------------------------------------
def dashboard():
    """Main dashboard with KPIs, market breadth, and top movers."""

    try:
        con = get_connection()

        # =================================================================
        # HEADER: Title + Market Status + Data Freshness
        # =================================================================
        header_col1, header_col2, header_col3 = st.columns([2, 1, 1])

        with header_col1:
            st.markdown("## 📊 Market Dashboard")
            st.caption("Pakistan Stock Exchange • Real-time Analytics")

        with header_col2:
            # Market Status Badge
            render_market_status_badge()

        with header_col3:
            # Data Freshness
            days_old, latest_date = get_data_freshness(con)
            badge_color, badge_text = get_freshness_badge(days_old)
            if latest_date:
                freshness_color = "#00C853" if badge_color == "green" else "#FFC107" if badge_color == "orange" else "#FF1744"
                st.markdown(
                    f'<div style="text-align: right; font-size: 12px;">'
                    f'<span style="color: {freshness_color};">●</span> Data: {badge_text}<br>'
                    f'<span style="color: #888;">As of {latest_date}</span></div>',
                    unsafe_allow_html=True
                )

        st.markdown("---")

        # =================================================================
        # KSE-100 INDEX DISPLAY - Primary Market Benchmark
        # =================================================================
        try:
            # Calculate market-wide metrics as index proxy
            # Get today's market performance
            market_perf = con.execute("""
                SELECT
                    COUNT(*) as total_stocks,
                    SUM(CASE WHEN change_percent > 0 THEN 1 ELSE 0 END) as gainers,
                    SUM(CASE WHEN change_percent < 0 THEN 1 ELSE 0 END) as losers,
                    SUM(CASE WHEN change_percent = 0 OR change_percent IS NULL THEN 1 ELSE 0 END) as unchanged,
                    AVG(change_percent) as avg_change,
                    SUM(volume) as total_volume,
                    SUM(turnover) as total_turnover
                FROM trading_sessions
                WHERE session_date = (SELECT MAX(session_date) FROM trading_sessions)
                AND market_type = 'REG'
            """).fetchone()

            if market_perf and market_perf["total_stocks"] > 0:
                idx_col1, idx_col2, idx_col3 = st.columns([2, 1, 1])

                with idx_col1:
                    avg_change = market_perf["avg_change"] or 0
                    gainers = market_perf["gainers"] or 0
                    losers = market_perf["losers"] or 0

                    # Color based on market direction
                    if avg_change > 0:
                        idx_color = "#00C853"
                        arrow = "▲"
                    elif avg_change < 0:
                        idx_color = "#FF1744"
                        arrow = "▼"
                    else:
                        idx_color = "#78909C"
                        arrow = "●"

                    st.markdown(f"""
                    <div style="background: linear-gradient(135deg, rgba(33,150,243,0.1) 0%, rgba(33,150,243,0.05) 100%);
                                border: 1px solid rgba(33,150,243,0.2); border-radius: 12px; padding: 20px;">
                        <div style="font-size: 12px; color: #888; margin-bottom: 4px;">
                            📊 KSE-100 Index Proxy (Market Average)
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
        except Exception:
            pass  # Graceful degradation if index data unavailable

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

        # Get trading session stats
        session_stats = con.execute("""
            SELECT
                SUM(volume) as total_volume,
                SUM(turnover) as total_turnover,
                COUNT(DISTINCT symbol) as active_symbols
            FROM trading_sessions
            WHERE session_date = (SELECT MAX(session_date) FROM trading_sessions)
            AND market_type = 'REG'
        """).fetchone()

        total_vol = session_stats["total_volume"] if session_stats else 0
        active_count = session_stats["active_symbols"] if session_stats else 0

        with kpi_col1:
            st.metric(
                "🏢 Companies",
                f"{deep_count:,}",
                help="Companies with deep data profiles"
            )

        with kpi_col2:
            st.metric(
                "📈 Active Today",
                f"{active_count:,}",
                help="Symbols traded today"
            )

        with kpi_col3:
            vol_str = format_volume(total_vol) if total_vol else "N/A"
            st.metric(
                "📊 Total Volume",
                vol_str,
                help="Combined volume across all symbols"
            )

        with kpi_col4:
            # EOD data coverage
            eod_count = con.execute("SELECT COUNT(*) FROM eod_ohlcv").fetchone()[0]
            st.metric(
                "📅 Historical Days",
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
                "📣 Announcements",
                f"{ann_count}",
                help="Corporate announcements today"
            )

        st.markdown("")  # Spacing

        # =====================================================================
        # PSX-Style Trading Segments Summary
        # =====================================================================
        try:
            # Get trading segments data from today's sessions
            segments_query = """
                SELECT
                    market_type,
                    COUNT(*) as symbols,
                    SUM(volume) as total_volume,
                    AVG(volume) as avg_volume
                FROM trading_sessions
                WHERE session_date = (SELECT MAX(session_date) FROM trading_sessions)
                GROUP BY market_type
                ORDER BY total_volume DESC
            """
            segments_df = pd.read_sql_query(segments_query, con)

            if not segments_df.empty:
                st.subheader("📊 Trading Segments")

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
            pass

        # =====================================================================
        # Volume Leaders & 52-Week Range Indicators
        # =====================================================================
        try:
            vol_52w_cols = st.columns(2)

            with vol_52w_cols[0]:
                # Top Volume Leaders
                volume_query = """
                    SELECT
                        ts.symbol,
                        ts.volume,
                        COALESCE(ts.close, ts.high, ts.ldcp) as price,
                        ts.ldcp,
                        COALESCE(ts.change_percent,
                            CASE WHEN ts.ldcp > 0
                                THEN ROUND((COALESCE(ts.close, ts.high) - ts.ldcp) / ts.ldcp * 100, 2)
                                ELSE 0
                            END
                        ) as change_percent
                    FROM trading_sessions ts
                    WHERE ts.session_date = (SELECT MAX(session_date) FROM trading_sessions)
                    AND ts.market_type = 'REG'
                    AND ts.volume > 0
                    ORDER BY ts.volume DESC
                    LIMIT 5
                """
                vol_df = pd.read_sql_query(volume_query, con)

                if not vol_df.empty:
                    st.markdown("**📈 Volume Leaders**")
                    for _, row in vol_df.iterrows():
                        vol = row["volume"]
                        vol_str = f"{vol/1e6:.2f}M" if vol >= 1e6 else f"{vol:,.0f}"
                        change = row["change_percent"] or 0
                        color = "🟢" if change > 0 else "🔴" if change < 0 else "⚪"
                        st.caption(f"{color} **{row['symbol']}** - {vol_str} ({change:+.2f}%)")

            with vol_52w_cols[1]:
                # 52-Week Range Indicators (using high as current price proxy)
                range_query = """
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
                    WHERE ts.session_date = (SELECT MAX(session_date) FROM trading_sessions)
                    AND ts.market_type = 'REG'
                    AND ts.week_52_high > 0
                    AND ts.week_52_low > 0
                    AND COALESCE(ts.close, ts.high, ts.ldcp) > 0
                    ORDER BY position_pct DESC
                    LIMIT 3
                """
                high_df = pd.read_sql_query(range_query, con)

                low_query = """
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
                    WHERE ts.session_date = (SELECT MAX(session_date) FROM trading_sessions)
                    AND ts.market_type = 'REG'
                    AND ts.week_52_high > 0
                    AND ts.week_52_low > 0
                    AND COALESCE(ts.close, ts.high, ts.ldcp) > 0
                    ORDER BY position_pct ASC
                    LIMIT 3
                """
                low_df = pd.read_sql_query(low_query, con)

                st.markdown("**📊 52-Week Range**")
                if not high_df.empty:
                    st.caption("Near 52W High:")
                    for _, row in high_df.iterrows():
                        st.caption(f"  🔺 **{row['symbol']}** ({row['position_pct']:.0f}% of range)")

                if not low_df.empty:
                    st.caption("Near 52W Low:")
                    for _, row in low_df.iterrows():
                        st.caption(f"  🔻 **{row['symbol']}** ({row['position_pct']:.0f}% of range)")

            st.markdown("---")
        except Exception:
            pass

        # Market Breadth and Top Movers (from analytics tables)
        try:
            from psx_ohlcv.sources.regular_market import init_regular_market_schema
            init_regular_market_schema(con)
            init_analytics_schema(con)

            # Get analytics from pre-computed tables
            market_analytics = get_latest_market_analytics(con)

            if market_analytics:
                st.subheader("📈 Market Overview")

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
                    top_gainers_df = get_top_list(con, "gainers", limit=5)
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
                                if st.button(f"📈 {sym}", key=f"dash_gainer_{sym}"):
                                    st.session_state.company_symbol = sym
                                    st.session_state.nav_to = "🏢 Company Analytics"
                                    st.rerun()

                with col3:
                    # Top 5 Losers from analytics table
                    top_losers_df = get_top_list(con, "losers", limit=5)
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
                                if st.button(f"📉 {sym}", key=f"dash_loser_{sym}"):
                                    st.session_state.company_symbol = sym
                                    st.session_state.nav_to = "🏢 Company Analytics"
                                    st.rerun()

                st.markdown("---")

                # Sector Leaderboard
                st.subheader("📊 Sector Performance")
                sector_df = get_sector_leaderboard(con)
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
        runs_df = pd.read_sql_query(
            """
            SELECT run_id, started_at, ended_at, mode,
                   symbols_total, symbols_ok, symbols_failed, rows_upserted
            FROM sync_runs
            ORDER BY started_at DESC
            LIMIT 10
            """,
            con,
        )

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
        with st.expander("ℹ️ Data Quality Information", expanded=False):
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


# -----------------------------------------------------------------------------
# Page: Candlestick Explorer
# -----------------------------------------------------------------------------
def candlestick_explorer():
    """Candlestick chart explorer with SMA overlays."""

    try:
        con = get_connection()

        # =================================================================
        # HEADER
        # =================================================================
        header_col1, header_col2 = st.columns([3, 1])
        with header_col1:
            st.markdown("## 📈 Candlestick Explorer")
            st.caption("Technical analysis with OHLCV charts and moving averages")
        with header_col2:
            render_market_status_badge()

        # Load symbols
        symbols = get_symbols_list(con, is_active_only=False)
        if not symbols:
            st.warning("No symbols found. Run `psxsync symbols refresh` first.")
            render_footer()
            return

        st.markdown("---")

        # =================================================================
        # CONTROLS - Compact toolbar style
        # =================================================================
        ctrl_col1, ctrl_col2, ctrl_col3, ctrl_col4 = st.columns([3, 1, 1, 1])

        with ctrl_col1:
            selected = st.selectbox(
                "Symbol",
                symbols,
                index=0,
                label_visibility="collapsed",
                help="Choose a symbol to explore"
            )

        with ctrl_col2:
            range_options = {"1M": 30, "3M": 90, "6M": 180, "1Y": 365, "All": None}
            range_choice = st.selectbox(
                "Range",
                list(range_options.keys()),
                index=3,
                label_visibility="collapsed"
            )

        with ctrl_col3:
            show_sma = st.checkbox("SMA", value=True, help="Show SMA(20) and SMA(50)")

        with ctrl_col4:
            days_old, latest_date = get_data_freshness(con)
            if latest_date:
                st.caption(f"📅 {latest_date}")

        # Calculate date range
        end_date = datetime.now().strftime("%Y-%m-%d")
        days = range_options[range_choice]
        if days:
            start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        else:
            start_date = None

        # Fetch OHLCV data
        df = get_ohlcv_range(con, selected, start_date=start_date, end_date=end_date)

        if df.empty:
            st.warning(
                f"No data for {selected}. Run `psxsync sync --symbols {selected}`."
            )
            render_footer()
            return

        # Price stats
        st.markdown("---")
        latest = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else latest
        change = latest["close"] - prev["close"]
        change_pct = (change / prev["close"]) * 100 if prev["close"] else 0

        col1, col2, col3, col4, col5 = st.columns(5)
        change_str = f"{change:+.2f} ({change_pct:+.1f}%)"
        col1.metric(
            "Close",
            f"PKR {latest['close']:.2f}",
            change_str,
            help=OHLCV_TOOLTIPS["close"]
        )
        col2.metric("Open", f"PKR {latest['open']:.2f}", help=OHLCV_TOOLTIPS["open"])
        col3.metric("High", f"PKR {latest['high']:.2f}", help=OHLCV_TOOLTIPS["high"])
        col4.metric("Low", f"PKR {latest['low']:.2f}", help=OHLCV_TOOLTIPS["low"])
        col5.metric(
            "Volume",
            f"{int(latest['volume']):,}",
            help=OHLCV_TOOLTIPS["volume"]
        )

        st.caption(f"📍 Last close: **PKR {latest['close']:.2f}** on {latest['date']}")

        st.markdown("---")

        # Candlestick chart using the helper
        fig = make_candlestick(
            df,
            title=f"{selected} - OHLC ({range_choice})",
            date_col="date",
            show_sma=show_sma,
            height=650,
        )
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # Data preview
        st.subheader("Data Preview (last 20 rows)")
        preview_df = df.sort_values("date", ascending=False).head(20)
        st.dataframe(
            preview_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "date": st.column_config.DateColumn("Date", format="YYYY-MM-DD"),
                "open": st.column_config.NumberColumn(
                    "Open", format="%.2f", help=OHLCV_TOOLTIPS["open"]
                ),
                "high": st.column_config.NumberColumn(
                    "High", format="%.2f", help=OHLCV_TOOLTIPS["high"]
                ),
                "low": st.column_config.NumberColumn(
                    "Low", format="%.2f", help=OHLCV_TOOLTIPS["low"]
                ),
                "close": st.column_config.NumberColumn(
                    "Close", format="%.2f", help=OHLCV_TOOLTIPS["close"]
                ),
                "volume": st.column_config.NumberColumn(
                    "Volume", format="%d", help=OHLCV_TOOLTIPS["volume"]
                ),
            }
        )

        # Export buttons
        col1, col2 = st.columns(2)

        with col1:
            st.download_button(
                "⬇️ Download CSV",
                df.to_csv(index=False),
                f"{selected}_ohlcv.csv",
                "text/csv",
                help="Download data to your computer"
            )

        with col2:
            if st.button(
                f"💾 Export to /exports/{selected}_ohlcv.csv",
                help="Save to server exports directory"
            ):
                EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
                export_path = EXPORTS_DIR / f"{selected}_ohlcv.csv"
                df.to_csv(export_path, index=False)
                st.success(f"Exported to: {export_path}")

    except Exception as e:
        st.error(f"Error: {e}")

    render_footer()


# -----------------------------------------------------------------------------
# Page: Intraday Trend
# -----------------------------------------------------------------------------
def intraday_trend_page():
    """Intraday price trend visualization and sync."""
    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## ⏱ Intraday Trend")
        st.caption("Live intraday price movements and volume throughout the trading day")
    with header_col2:
        render_market_status_badge()

    # Initialize session state for intraday sync
    if "intraday_sync_result" not in st.session_state:
        st.session_state.intraday_sync_result = None
    if "intraday_sync_running" not in st.session_state:
        st.session_state.intraday_sync_running = False

    try:
        con = get_connection()

        # Load symbols for suggestions
        symbols = get_symbols_list(con)

        if not symbols:
            st.warning("No symbols found. Run `psxsync symbols refresh` first.")
            render_footer()
            return

        st.markdown("---")

        # Symbol selection
        col1, col2 = st.columns([2, 1])

        with col1:
            symbol_input = st.text_input(
                "Enter Symbol",
                value="OGDC",
                placeholder="e.g., HBL, OGDC, MCB",
                help="Enter a stock symbol to view intraday data"
            ).strip().upper()

        with col2:
            selected_from_list = st.selectbox(
                "Or select from list",
                [""] + symbols,
                index=0,
                help="Select a symbol from the dropdown"
            )

        selected_symbol = selected_from_list if selected_from_list else symbol_input

        if not selected_symbol:
            st.info("Enter or select a symbol to view intraday data.")
            render_footer()
            return

        if selected_symbol not in symbols:
            st.warning(
                f"Symbol '{selected_symbol}' not found in database. "
                "It may be invalid or you need to refresh symbols."
            )

        st.markdown("---")

        # Sync controls
        st.subheader("Fetch / Refresh Data")

        col1, col2, col3 = st.columns([1, 1, 2])

        with col1:
            incremental_mode = st.checkbox(
                "Incremental",
                value=True,
                help="Only fetch new data since last sync",
                disabled=st.session_state.intraday_sync_running
            )

        with col2:
            max_rows = st.number_input(
                "Max Rows",
                min_value=100,
                max_value=5000,
                value=2000,
                step=100,
                help="Maximum rows to fetch from API",
                disabled=st.session_state.intraday_sync_running
            )

        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            fetch_btn = st.button(
                "🔄 Fetch / Refresh Intraday"
                if not st.session_state.intraday_sync_running
                else "⏳ Fetching...",
                type="primary",
                disabled=st.session_state.intraday_sync_running,
                help=f"Fetch intraday data for {selected_symbol}"
            )

        with col2:
            if st.session_state.intraday_sync_running:
                st.warning("Fetching...")

        # Execute intraday sync
        if fetch_btn and not st.session_state.intraday_sync_running:
            st.session_state.intraday_sync_result = None
            st.session_state.intraday_sync_running = True

            with st.status(
                f"Fetching intraday data for {selected_symbol}...",
                expanded=True
            ) as status:
                st.write(f"🔄 Fetching intraday data for {selected_symbol}...")

                try:
                    summary = sync_intraday(
                        db_path=get_db_path(),
                        symbol=selected_symbol,
                        incremental=incremental_mode,
                        max_rows=max_rows,
                    )

                    st.session_state.intraday_sync_result = {
                        "success": summary.error is None,
                        "summary": summary,
                    }

                    if summary.error:
                        status.update(
                            label=f"❌ Failed: {summary.error}", state="error"
                        )
                    else:
                        status.update(
                            label=f"✅ Fetched {summary.rows_upserted} rows",
                            state="complete"
                        )

                except Exception as e:
                    st.session_state.intraday_sync_result = {
                        "success": False,
                        "error": str(e),
                    }
                    status.update(label="❌ Fetch failed!", state="error")

                finally:
                    st.session_state.intraday_sync_running = False

        # Display sync result
        if st.session_state.intraday_sync_result is not None:
            result = st.session_state.intraday_sync_result
            if result["success"]:
                summary = result["summary"]
                st.success(
                    f"✅ Fetched {summary.rows_upserted} rows for {summary.symbol}"
                )
                if summary.newest_ts:
                    st.caption(f"Latest timestamp: {summary.newest_ts}")
            else:
                error_msg = result.get("error") or result.get("summary", {}).error
                st.error(f"❌ Error: {error_msg}")

        st.markdown("---")

        # Display controls
        col1, col2 = st.columns(2)
        with col1:
            limit = st.slider(
                "Display Limit",
                min_value=200,
                max_value=5000,
                value=500,
                step=100,
                help="Number of rows to display (most recent)"
            )

        with col2:
            stats = get_intraday_stats(con, selected_symbol)
            if stats["row_count"] > 0:
                st.metric(
                    "Total Rows",
                    f"{stats['row_count']:,}",
                    help="Total intraday records for this symbol"
                )
                st.caption(f"Range: {stats['min_ts']} to {stats['max_ts']}")
            else:
                st.info("No intraday data yet. Click 'Fetch / Refresh Intraday'.")

        st.markdown("---")

        # Fetch and display intraday data
        df = get_intraday_latest(con, selected_symbol, limit=limit)

        if df.empty:
            st.info(
                f"No intraday data for {selected_symbol}. "
                "Click 'Fetch / Refresh Intraday' to fetch data."
            )
            render_footer()
            return

        # Latest price stats
        st.subheader(f"{selected_symbol} - Intraday Stats")

        latest = df.iloc[-1]
        first = df.iloc[0]
        change = latest["close"] - first["open"] if first["open"] else 0
        change_pct = (change / first["open"]) * 100 if first["open"] else 0

        # Calculate VWAP (Volume Weighted Average Price)
        # VWAP = Σ(Typical Price × Volume) / Σ(Volume)
        df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
        df["tp_volume"] = df["typical_price"] * df["volume"]
        cumulative_tp_volume = df["tp_volume"].cumsum()
        cumulative_volume = df["volume"].cumsum()
        df["vwap"] = cumulative_tp_volume / cumulative_volume
        vwap = df["vwap"].iloc[-1] if not df["vwap"].empty else None

        # Session stats
        session_high = df["high"].max()
        session_low = df["low"].min()
        total_volume = df["volume"].sum()

        # First row: Price metrics
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        change_str = f"{change:+.2f} ({change_pct:+.1f}%)"
        col1.metric(
            "Latest Close",
            f"PKR {latest['close']:.2f}" if latest["close"] else "N/A",
            change_str if first["open"] else None,
            help="Most recent close price"
        )
        col2.metric(
            "Session Open",
            f"PKR {first['open']:.2f}" if first["open"] else "N/A",
            help="Session opening price"
        )
        col3.metric(
            "Session High",
            f"PKR {session_high:.2f}" if session_high else "N/A",
            help="Highest price in session"
        )
        col4.metric(
            "Session Low",
            f"PKR {session_low:.2f}" if session_low else "N/A",
            help="Lowest price in session"
        )
        col5.metric(
            "📊 VWAP",
            f"PKR {vwap:.2f}" if vwap else "N/A",
            help="Volume Weighted Average Price - institutional benchmark"
        )
        col6.metric(
            "Total Volume",
            format_volume(total_volume) if total_volume else "N/A",
            help="Total session volume"
        )

        # VWAP context
        if vwap and latest["close"]:
            vwap_diff = latest["close"] - vwap
            vwap_pct = (vwap_diff / vwap) * 100
            if vwap_diff > 0:
                st.caption(f"📍 Latest: {latest['ts']} | Price **above** VWAP by Rs.{vwap_diff:.2f} ({vwap_pct:+.2f}%) - Bullish bias")
            else:
                st.caption(f"📍 Latest: {latest['ts']} | Price **below** VWAP by Rs.{abs(vwap_diff):.2f} ({vwap_pct:+.2f}%) - Bearish bias")
        else:
            st.caption(f"📍 Latest: {latest['ts']}")

        st.markdown("---")

        # Intraday chart using the helper
        fig = make_intraday_chart(
            df,
            title=f"{selected_symbol} - Intraday",
            ts_col="ts",
            height=650,
        )
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # Close Price Trend with VWAP overlay
        st.subheader("📈 Price & VWAP")
        import plotly.graph_objects as go

        chart_df = df.sort_values("ts", ascending=True)
        fig_price = go.Figure()

        # Close price line
        fig_price.add_trace(go.Scatter(
            x=chart_df["ts"],
            y=chart_df["close"],
            mode="lines",
            name="Close",
            line={"color": "#2196F3", "width": 2},
        ))

        # VWAP line
        fig_price.add_trace(go.Scatter(
            x=chart_df["ts"],
            y=chart_df["vwap"],
            mode="lines",
            name="VWAP",
            line={"color": "#FF9800", "width": 2, "dash": "dash"},
        ))

        # Add horizontal line at current VWAP
        if vwap:
            fig_price.add_hline(
                y=vwap,
                line_dash="dot",
                line_color="rgba(255,152,0,0.5)",
                annotation_text=f"VWAP: {vwap:.2f}",
                annotation_position="right"
            )

        fig_price.update_layout(
            title=f"{selected_symbol} - Price vs VWAP",
            xaxis_title="Time",
            yaxis_title="Price (PKR)",
            height=400,
            hovermode="x unified",
            legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "right", "x": 1},
            margin={"l": 60, "r": 20, "t": 60, "b": 60},
        )
        st.plotly_chart(fig_price, use_container_width=True)

        st.caption("**VWAP** (Volume Weighted Average Price) = institutional benchmark. "
                   "Price above VWAP suggests bullish bias; below suggests bearish bias.")

        # Volume chart
        st.subheader("📊 Volume")
        fig_vol = make_volume_chart(df, date_col="ts", height=250)
        st.plotly_chart(fig_vol, use_container_width=True)

        st.markdown("---")

        # Data table
        st.subheader(f"Data Preview (last {min(50, len(df))} rows)")

        preview_df = df.sort_values("ts", ascending=False).head(50)
        st.dataframe(
            preview_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "symbol": st.column_config.TextColumn("Symbol"),
                "ts": st.column_config.TextColumn("Timestamp"),
                "open": st.column_config.NumberColumn("Open", format="%.2f"),
                "high": st.column_config.NumberColumn("High", format="%.2f"),
                "low": st.column_config.NumberColumn("Low", format="%.2f"),
                "close": st.column_config.NumberColumn("Close", format="%.2f"),
                "volume": st.column_config.NumberColumn("Volume", format="%d"),
            }
        )

        st.markdown("---")

        # Export options
        col1, col2 = st.columns(2)

        with col1:
            st.download_button(
                "⬇️ Download CSV",
                df.to_csv(index=False),
                f"{selected_symbol}_intraday.csv",
                "text/csv",
                help="Download intraday data to your computer"
            )

        with col2:
            if st.button(
                f"💾 Export to /exports/{selected_symbol}_intraday.csv",
                help="Save to server exports directory"
            ):
                EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
                export_path = EXPORTS_DIR / f"{selected_symbol}_intraday.csv"
                df.to_csv(export_path, index=False)
                st.success(f"Exported to: {export_path}")

    except Exception as e:
        st.error(f"Error: {e}")

    render_footer()


# -----------------------------------------------------------------------------
# Page: Regular Market Watch
# -----------------------------------------------------------------------------
def regular_market_page():
    """Regular market watch - live market data display."""
    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 📊 Regular Market Watch")
        st.caption("Live market data • Prices, changes, and volume for all symbols")
    with header_col2:
        render_market_status_badge()

    try:
        from psx_ohlcv.analytics import compute_all_analytics
        from psx_ohlcv.sources.regular_market import (
            fetch_regular_market,
            get_all_current_hashes,
            get_current_market,
            init_regular_market_schema,
            insert_snapshots,
            upsert_current,
        )

        con = get_connection()
        init_regular_market_schema(con)
        init_analytics_schema(con)

        # Initialize session state
        if "rm_fetch_result" not in st.session_state:
            st.session_state.rm_fetch_result = None
        if "rm_fetch_running" not in st.session_state:
            st.session_state.rm_fetch_running = False

        st.markdown("---")

        # Fetch controls
        st.subheader("Fetch / Refresh Data")

        col1, col2, col3 = st.columns([1, 1, 2])

        with col1:
            save_unchanged = st.checkbox(
                "Save all rows",
                value=False,
                help="Save all rows to snapshots (even if unchanged)",
                disabled=st.session_state.rm_fetch_running
            )

        with col2:
            fetch_btn = st.button(
                "🔄 Fetch Market Data"
                if not st.session_state.rm_fetch_running
                else "⏳ Fetching...",
                type="primary",
                disabled=st.session_state.rm_fetch_running,
                help="Fetch latest market data from PSX"
            )

        # Execute fetch
        if fetch_btn and not st.session_state.rm_fetch_running:
            st.session_state.rm_fetch_result = None
            st.session_state.rm_fetch_running = True

            with st.status("Fetching market data...", expanded=True) as status:
                st.write("🔄 Fetching from PSX market-watch...")

                try:
                    df = fetch_regular_market()

                    if df.empty:
                        st.session_state.rm_fetch_result = {
                            "success": False,
                            "error": "No data returned from PSX",
                        }
                        status.update(label="❌ No data returned", state="error")
                    else:
                        # CRITICAL: Load previous hashes BEFORE upsert
                        prev_hashes = get_all_current_hashes(con)

                        # Insert snapshots first (using pre-loaded hashes)
                        snapshots_saved = insert_snapshots(
                            con, df,
                            save_unchanged=save_unchanged,
                            prev_hashes=prev_hashes,
                        )

                        # Then upsert current data
                        rows_upserted = upsert_current(con, df)

                        # Compute analytics
                        ts = df["ts"].iloc[0] if not df.empty else None
                        if ts:
                            compute_all_analytics(con, ts)

                        st.session_state.rm_fetch_result = {
                            "success": True,
                            "symbols": len(df),
                            "upserted": rows_upserted,
                            "snapshots": snapshots_saved,
                        }
                        status.update(
                            label=f"✅ Fetched {len(df)} symbols",
                            state="complete"
                        )

                except Exception as e:
                    st.session_state.rm_fetch_result = {
                        "success": False,
                        "error": str(e),
                    }
                    status.update(label="❌ Fetch failed!", state="error")

                finally:
                    st.session_state.rm_fetch_running = False

        # Display fetch result
        if st.session_state.rm_fetch_result is not None:
            result = st.session_state.rm_fetch_result
            if result["success"]:
                st.success(
                    f"✅ Fetched {result['symbols']} symbols, "
                    f"{result['upserted']} upserted, "
                    f"{result['snapshots']} snapshots saved"
                )
            else:
                st.error(f"❌ Error: {result.get('error', 'Unknown error')}")

        st.markdown("---")

        # Load current market data from database with sector names joined
        df = get_current_market_with_sectors(con)

        if df.empty:
            # Fallback to raw data
            df = get_current_market(con)

        if df.empty:
            st.info(
                "No market data available. Click 'Fetch Market Data' to get "
                "the latest data from PSX."
            )
            render_footer()
            return

        # Market overview using pre-computed analytics
        st.subheader("📈 Market Overview")

        market_analytics = get_latest_market_analytics(con)
        if market_analytics:
            total_symbols = market_analytics.get("total_symbols", len(df))
            gainers = market_analytics.get("gainers_count", 0)
            losers = market_analytics.get("losers_count", 0)
            unchanged = market_analytics.get("unchanged_count", 0)
        else:
            # Calculate from data if analytics not available
            total_symbols = len(df)
            gainers = len(df[df["change_pct"] > 0]) if "change_pct" in df.columns else 0
            losers = len(df[df["change_pct"] < 0]) if "change_pct" in df.columns else 0
            unchanged = (
                len(df[df["change_pct"] == 0]) if "change_pct" in df.columns else 0
            )

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Symbols", total_symbols)
        col2.metric("Gainers", gainers, delta=f"+{gainers}")
        col3.metric("Losers", losers, delta=f"-{losers}", delta_color="inverse")
        col4.metric("Unchanged", unchanged)

        # Market breadth chart and top movers
        if "change_pct" in df.columns:
            col1, col2, col3 = st.columns([1, 1, 1])

            with col1:
                breadth_fig = make_market_breadth_chart(
                    gainers=gainers,
                    losers=losers,
                    unchanged=unchanged,
                    height=300,
                )
                st.plotly_chart(breadth_fig, use_container_width=True)

            with col2:
                # Use analytics table for top gainers
                top_gainers_df = get_top_list(con, "gainers", limit=5)
                if top_gainers_df.empty:
                    top_gainers_df = df.nlargest(5, "change_pct")[
                        ["symbol", "change_pct"]
                    ]
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
                        if st.button(f"📈 {sym}", key=f"rm_gainer_{sym}"):
                            st.session_state.company_symbol = sym
                            st.session_state.nav_to = "🏢 Company Analytics"
                            st.rerun()

            with col3:
                # Use analytics table for top losers
                top_losers_df = get_top_list(con, "losers", limit=5)
                if top_losers_df.empty:
                    top_losers_df = df.nsmallest(5, "change_pct")[
                        ["symbol", "change_pct"]
                    ]
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
                        if st.button(f"📉 {sym}", key=f"rm_loser_{sym}"):
                            st.session_state.company_symbol = sym
                            st.session_state.nav_to = "🏢 Company Analytics"
                            st.rerun()

        st.markdown("---")

        # Filters
        st.subheader("🔍 Filter Market Data")

        col1, col2, col3 = st.columns([2, 1, 1])

        with col1:
            search = st.text_input(
                "Search Symbol",
                placeholder="e.g., HBL, OGDC",
                help="Filter by symbol"
            )

        with col2:
            # Use sector_name for filter if available, otherwise sector_code
            if "sector_name" in df.columns and df["sector_name"].notna().any():
                sector_options = sorted(
                    df["sector_name"].dropna().unique().tolist()
                )
                sector_options = [s for s in sector_options if s]  # Remove empty
            elif "sector_code" in df.columns:
                sector_options = sorted(
                    df["sector_code"].dropna().unique().tolist()
                )
            else:
                sector_options = []
            sector_filter = st.selectbox(
                "Sector",
                ["All"] + sector_options,
                help="Filter by sector"
            )

        with col3:
            change_filter = st.selectbox(
                "Change",
                ["All", "Gainers", "Losers", "Unchanged"],
                help="Filter by price change"
            )

        # Apply filters
        filtered_df = df.copy()

        if search:
            filtered_df = filtered_df[
                filtered_df["symbol"].str.contains(search.upper(), na=False)
            ]

        if sector_filter != "All":
            if "sector_name" in filtered_df.columns:
                filtered_df = filtered_df[filtered_df["sector_name"] == sector_filter]
            elif "sector_code" in filtered_df.columns:
                filtered_df = filtered_df[filtered_df["sector_code"] == sector_filter]

        if change_filter != "All" and "change_pct" in filtered_df.columns:
            if change_filter == "Gainers":
                filtered_df = filtered_df[filtered_df["change_pct"] > 0]
            elif change_filter == "Losers":
                filtered_df = filtered_df[filtered_df["change_pct"] < 0]
            elif change_filter == "Unchanged":
                filtered_df = filtered_df[filtered_df["change_pct"] == 0]

        st.caption(f"Showing {len(filtered_df)} of {len(df)} symbols")

        st.markdown("---")

        # Display table
        st.subheader("📋 Market Data")

        # Select columns to display - use sector_name only (not sector_code)
        display_cols = [
            "symbol", "status", "sector_name", "listed_in",
            "ldcp", "open", "high", "low", "current",
            "change", "change_pct", "volume", "ts"
        ]
        display_cols = [c for c in display_cols if c in filtered_df.columns]

        st.dataframe(
            filtered_df[display_cols],
            use_container_width=True,
            hide_index=True,
            column_config={
                "symbol": st.column_config.TextColumn("Symbol", width="small"),
                "status": st.column_config.TextColumn("Status", width="small"),
                "sector_name": st.column_config.TextColumn("Sector", width="medium"),
                "listed_in": st.column_config.TextColumn("Index", width="small"),
                "ldcp": st.column_config.NumberColumn("LDCP", format="%.2f"),
                "open": st.column_config.NumberColumn("Open", format="%.2f"),
                "high": st.column_config.NumberColumn("High", format="%.2f"),
                "low": st.column_config.NumberColumn("Low", format="%.2f"),
                "current": st.column_config.NumberColumn("Current", format="%.2f"),
                "change": st.column_config.NumberColumn("Change", format="%.2f"),
                "change_pct": st.column_config.NumberColumn("Change %", format="%.2f"),
                "volume": st.column_config.NumberColumn("Volume", format="%d"),
                "ts": st.column_config.TextColumn("Timestamp"),
            }
        )

        st.markdown("---")

        # Export options
        col1, col2 = st.columns(2)

        with col1:
            st.download_button(
                "⬇️ Download CSV",
                filtered_df.to_csv(index=False),
                "regular_market.csv",
                "text/csv",
                help="Download market data to your computer"
            )

        with col2:
            if st.button(
                "💾 Export to /exports/regular_market.csv",
                help="Save to server exports directory"
            ):
                EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
                export_path = EXPORTS_DIR / "regular_market.csv"
                filtered_df.to_csv(export_path, index=False)
                st.success(f"Exported to: {export_path}")

    except ImportError:
        st.error(
            "Regular market module not found. "
            "Make sure psx_ohlcv.sources.regular_market is installed."
        )
    except Exception as e:
        st.error(f"Error: {e}")

    render_footer()


# -----------------------------------------------------------------------------
# Page: Symbols
# -----------------------------------------------------------------------------
def symbols_page():
    """Browse and manage all symbols."""
    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 🧵 Symbols")
        st.caption("Master list of all PSX-listed securities")
    with header_col2:
        render_market_status_badge()

    try:
        con = get_connection()

        # Filters
        col1, col2 = st.columns([1, 3])
        with col1:
            show_inactive = st.checkbox(
                "Show inactive",
                value=False,
                help="Include symbols that are no longer actively traded"
            )
        with col2:
            search = st.text_input(
                "Search",
                placeholder="e.g. HBL, Bank",
                help="Filter by symbol or company name"
            )

        # Get active/inactive filter
        is_active_only = not show_inactive

        # Display count of active symbols
        active_count = len(get_symbols_list(con, is_active_only=True))
        st.markdown(f"**Active symbols: {active_count}**")

        # Build query for full symbol details
        # sector_name is now stored directly in symbols table from master file
        query = """
            SELECT symbol, name, sector as sector_code,
                   sector_name, outstanding_shares, is_active, source,
                   discovered_at, updated_at
            FROM symbols
        """
        conditions = []
        if not show_inactive:
            conditions.append("is_active = 1")
        if search:
            search_upper = search.upper()
            conditions.append(
                f"(symbol LIKE '%{search_upper}%' OR name LIKE '%{search}%')"
            )
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY symbol"

        df = pd.read_sql_query(query, con)

        st.markdown(f"**{len(df)} symbols found**")

        if df.empty:
            st.info("No symbols found. Run `psxsync master refresh` to fetch.")
        else:
            # Show symbols table from DB
            df["is_active"] = df["is_active"].map({1: "Yes", 0: "No"})
            # Fill empty sector_name with sector_code
            df["sector_name"] = df["sector_name"].fillna(df["sector_code"])
            # Select and rename columns - only show sector_name, not sector_code
            display_df = df[
                ["symbol", "name", "sector_name",
                 "is_active", "discovered_at", "updated_at"]
            ].copy()
            display_df.columns = [
                "Symbol", "Name", "Sector",
                "Active", "Discovered", "Updated"
            ]
            st.dataframe(display_df, use_container_width=True, hide_index=True)

            # Actions
            col1, col2 = st.columns(2)
            with col1:
                st.download_button(
                    "⬇️ Download CSV",
                    display_df.to_csv(index=False),
                    "psx_symbols.csv",
                    "text/csv",
                    help="Download symbols list to your computer"
                )
            with col2:
                # Copy comma-separated symbols string
                symbols_str = get_symbols_string(con, is_active_only=is_active_only)
                if len(symbols_str) > 100:
                    display_str = symbols_str[:100] + "..."
                else:
                    display_str = symbols_str
                st.code(display_str)
                st.caption("Copy symbols as comma-separated string")

    except Exception as e:
        st.error(f"Error: {e}")

    render_footer()


# -----------------------------------------------------------------------------
# Page: Schema - Database schema documentation and SQL scripts
# -----------------------------------------------------------------------------
def schema_page():
    """Display database schema documentation and SQL creation scripts."""
    from psx_ohlcv.db import SCHEMA_SQL

    # =================================================================
    # HEADER
    # =================================================================
    st.markdown("## 📋 Database Schema")
    st.caption("Table structure, SQL scripts, and data dictionary")

    con = get_connection()
    track_page_visit(con, "Schema")

    # Tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Table Overview",
        "📖 Glossary",
        "💾 SQL Scripts",
        "📈 Database Stats"
    ])

    # Tab 1: Table Overview
    with tab1:
        st.subheader("Table Categories")

        # Core Tables
        st.markdown("### Core Tables")
        core_tables = [
            ("symbols", "symbol", "Master symbol list with metadata"),
            ("eod_ohlcv", "(symbol, date)", "End-of-day OHLCV price data"),
            ("intraday_bars", "(symbol, ts)", "Intraday time series (1-min bars)"),
            ("intraday_sync_state", "symbol", "Last sync timestamp per symbol"),
            ("sectors", "sector_code", "Sector master list"),
        ]
        st.table({"Table": [t[0] for t in core_tables],
                  "Primary Key": [t[1] for t in core_tables],
                  "Description": [t[2] for t in core_tables]})

        # Company Data Tables
        st.markdown("### Company Data Tables")
        company_tables = [
            ("company_profile", "symbol", "Company profile information"),
            ("company_key_people", "(symbol, role, name)", "Directors, executives"),
            ("company_quote_snapshots", "(symbol, ts)", "Point-in-time quote captures"),
            ("company_fundamentals", "symbol", "Latest fundamentals (live)"),
            ("company_fundamentals_history", "(symbol, date)", "Historical fundamentals"),
            ("company_financials", "(symbol, period_end, period_type)", "Income statement data"),
            ("company_ratios", "(symbol, period_end, period_type)", "Financial ratios"),
            ("company_payouts", "(symbol, ex_date, payout_type)", "Dividends and bonuses"),
        ]
        st.table({"Table": [t[0] for t in company_tables],
                  "Primary Key": [t[1] for t in company_tables],
                  "Description": [t[2] for t in company_tables]})

        # Quant Tables
        st.markdown("### Quant/Bloomberg-Style Tables")
        quant_tables = [
            ("company_snapshots", "(symbol, snapshot_date)", "Full JSON document storage"),
            ("trading_sessions", "(symbol, session_date, market_type, contract_month)", "Market microstructure"),
            ("corporate_announcements", "id + unique constraint", "Company announcements"),
            ("equity_structure", "(symbol, as_of_date)", "Ownership and capital structure"),
            ("scrape_jobs", "job_id", "Scrape job tracking"),
        ]
        st.table({"Table": [t[0] for t in quant_tables],
                  "Primary Key": [t[1] for t in quant_tables],
                  "Description": [t[2] for t in quant_tables]})

        # System Tables
        st.markdown("### System Tables")
        system_tables = [
            ("sync_runs", "run_id", "Sync job runs"),
            ("sync_failures", "N/A", "Failed sync records"),
            ("downloaded_market_summary_dates", "date", "Market summary download tracking"),
            ("user_interactions", "id", "UI analytics tracking"),
        ]
        st.table({"Table": [t[0] for t in system_tables],
                  "Primary Key": [t[1] for t in system_tables],
                  "Description": [t[2] for t in system_tables]})

    # Tab 2: Glossary
    with tab2:
        st.subheader("Glossary")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### Market Terms")
            market_terms = {
                "OHLCV": "Open, High, Low, Close, Volume - standard price bar data",
                "EOD": "End of Day - daily closing data",
                "LDCP": "Last Day Close Price - previous trading day's close",
                "VWAP": "Volume Weighted Average Price",
                "VAR": "Value at Risk - risk metric percentage",
                "Haircut": "Margin collateral discount percentage",
                "Circuit Breaker": "Price limit bands (upper/lower)",
                "Free Float": "Shares available for public trading",
            }
            for term, definition in market_terms.items():
                st.markdown(f"**{term}**: {definition}")

            st.markdown("#### Market Types")
            market_types = {
                "REG": "Regular Market - main trading board",
                "FUT": "Futures Market - derivatives",
                "CSF": "Cash Settled Futures",
                "ODL": "Odd Lot Market - small quantity trades",
            }
            for code, desc in market_types.items():
                st.markdown(f"**{code}**: {desc}")

        with col2:
            st.markdown("#### Data Sources")
            sources = {
                "Market Watch": "dps.psx.com.pk/market-watch (Real-time quotes)",
                "Company Page": "dps.psx.com.pk/company/{symbol} (Company details)",
                "Market Summary": "dps.psx.com.pk/download/mkt_summary/{date}.Z (EOD bulk)",
                "Listed Companies": "dps.psx.com.pk/listed-companies (Symbol master)",
            }
            for source, desc in sources.items():
                st.markdown(f"**{source}**: {desc}")

            st.markdown("#### Period Types")
            periods = {
                "annual": "Full fiscal year data",
                "quarterly": "Quarter-end data (Q1, Q2, Q3, Q4)",
                "ttm": "Trailing Twelve Months",
                "ytd": "Year to Date",
            }
            for period, desc in periods.items():
                st.markdown(f"**{period}**: {desc}")

            st.markdown("#### Payout Types")
            payouts = {
                "cash": "Cash dividend per share",
                "bonus": "Bonus shares (stock dividend)",
                "right": "Rights issue offering",
            }
            for ptype, desc in payouts.items():
                st.markdown(f"**{ptype}**: {desc}")

    # Tab 3: SQL Scripts
    with tab3:
        st.subheader("SQL Creation Scripts")

        st.markdown("Full schema SQL from `src/psx_ohlcv/db.py`:")

        # Show the full SQL
        st.code(SCHEMA_SQL, language="sql")

        # Download button
        st.download_button(
            label="📥 Download Schema SQL",
            data=SCHEMA_SQL,
            file_name="psx_ohlcv_schema.sql",
            mime="text/plain"
        )

        st.markdown("---")
        st.markdown("#### Quick Query Examples")

        examples = '''-- List all tables
.tables

-- Show table schema
.schema symbols
.schema eod_ohlcv

-- Recent EOD data
SELECT symbol, date, close, volume
FROM eod_ohlcv
WHERE date = (SELECT MAX(date) FROM eod_ohlcv)
ORDER BY volume DESC
LIMIT 10;

-- Company snapshot JSON extract
SELECT symbol, snapshot_date,
       json_extract(quote_data, '$.price') as price,
       json_extract(quote_data, '$.change_pct') as change_pct
FROM company_snapshots
WHERE snapshot_date = date('now');

-- Daily returns calculation
SELECT symbol, date, close,
       (close - prev_close) / prev_close * 100 AS return_pct
FROM eod_ohlcv
WHERE symbol = 'OGDC'
ORDER BY date DESC
LIMIT 30;
'''
        st.code(examples, language="sql")

    # Tab 4: Database Stats
    with tab4:
        st.subheader("Database Statistics")

        # Get all tables and their row counts
        try:
            tables_query = """
                SELECT name FROM sqlite_master
                WHERE type='table'
                ORDER BY name
            """
            tables = [row[0] for row in con.execute(tables_query).fetchall()]

            stats = []
            for table in tables:
                try:
                    count = con.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]
                    stats.append({"Table": table, "Rows": count})
                except Exception:
                    stats.append({"Table": table, "Rows": "Error"})

            if stats:
                import pandas as pd
                df = pd.DataFrame(stats)
                df = df.sort_values("Rows", ascending=False, key=lambda x: pd.to_numeric(x, errors='coerce'))

                col1, col2 = st.columns([2, 1])
                with col1:
                    st.dataframe(df, use_container_width=True, hide_index=True)
                with col2:
                    total_rows = sum(s["Rows"] for s in stats if isinstance(s["Rows"], int))
                    st.metric("Total Tables", len(stats))
                    st.metric("Total Rows", f"{total_rows:,}")

                    # Database file size
                    from psx_ohlcv.config import get_db_path
                    db_path = get_db_path()
                    if db_path.exists():
                        size_mb = db_path.stat().st_size / (1024 * 1024)
                        st.metric("Database Size", f"{size_mb:.2f} MB")

        except Exception as e:
            st.error(f"Error fetching stats: {e}")

        st.markdown("---")
        st.markdown("#### Connection Info")
        from psx_ohlcv.config import get_db_path
        st.code(f"Database Path: {get_db_path()}")
        st.code(f"SQLite Version: {con.execute('SELECT sqlite_version()').fetchone()[0]}")


# -----------------------------------------------------------------------------
# Page: Settings
# -----------------------------------------------------------------------------
def settings_page():
    """Display configuration (read-only)."""
    # =================================================================
    # HEADER
    # =================================================================
    st.markdown("## ⚙️ Settings")
    st.caption("System configuration (read-only)")

    st.info("Settings are read-only. Use CLI flags or edit config to change.")

    # Database
    st.subheader("Database")
    st.code(f"Path: {DEFAULT_DB_PATH}")

    try:
        con = get_connection()
        tables = ["symbols", "eod_ohlcv", "sync_runs", "sync_failures"]

        # Check for regular market tables
        rm_tables = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND "
            "name LIKE 'regular_market%'"
        ).fetchall()
        if rm_tables:
            tables.extend([t[0] for t in rm_tables])

        # Check for intraday table
        intraday_table = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND "
            "name='intraday_ohlcv'"
        ).fetchone()
        if intraday_table:
            tables.append("intraday_ohlcv")

        sizes = []
        for table in tables:
            try:
                count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                sizes.append({"Table": table, "Rows": count})
            except Exception:
                sizes.append({"Table": table, "Rows": "N/A"})
        st.dataframe(pd.DataFrame(sizes), hide_index=True)
    except Exception as e:
        st.warning(f"Cannot read database: {e}")

    st.markdown("---")

    # Sync configuration
    st.subheader("Sync Configuration")
    config = DEFAULT_SYNC_CONFIG
    st.markdown(f"""
    | Setting | Value |
    |---------|-------|
    | Max Retries | {config.max_retries} |
    | Delay Range | {config.delay_min}s - {config.delay_max}s |
    | Timeout | {config.timeout}s |
    | Incremental | {config.incremental} |
    """)

    st.markdown("---")

    # Logging
    st.subheader("Logging")
    st.code(f"Log Path: {DEFAULT_LOG_FILE}")
    st.markdown("- Max Size: 5 MB\n- Backups: 3 files")

    st.markdown("---")

    # Exports directory
    st.subheader("Exports")
    st.code(f"Export Path: {EXPORTS_DIR}")
    if EXPORTS_DIR.exists():
        exports = list(EXPORTS_DIR.glob("*.csv"))
        if exports:
            st.markdown(f"**{len(exports)} CSV files exported**")
            for f in exports[:10]:
                st.text(f"  - {f.name}")
        else:
            st.info("No exports yet.")
    else:
        st.info("Exports directory not created yet.")

    st.markdown("---")

    # Data source
    st.subheader("Data Source")
    st.markdown("""
    | Endpoint | URL |
    |----------|-----|
    | Market Watch | `https://dps.psx.com.pk/market-watch` |
    | EOD API | `https://dps.psx.com.pk/timeseries/eod/{SYMBOL}` |
    """)

    render_footer()


# -----------------------------------------------------------------------------
# Page: History
# -----------------------------------------------------------------------------
def history_page():
    """Display historical OHLCV data and trends."""
    import plotly.graph_objects as go

    from psx_ohlcv.query import (
        get_ohlcv_market_daily,
        get_ohlcv_range,
        get_ohlcv_stats,
        get_ohlcv_symbol_stats,
    )

    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 📚 Historical Data")
        st.caption("End-of-day OHLCV data synced from PSX")
    with header_col2:
        render_market_status_badge()

    con = get_connection()

    # Check data availability
    ohlcv_stats = get_ohlcv_stats(con)

    if ohlcv_stats["total_rows"] == 0:
        st.warning(
            "No OHLCV history data available yet. To populate history:\n\n"
            "1. Run `psxsync sync --all` to fetch historical EOD data\n"
            "2. Or use `psxsync sync SYMBOL` for specific symbols"
        )
        render_footer()
        return

    st.info(
        f"OHLCV data: **{ohlcv_stats['total_rows']:,}** records for "
        f"**{ohlcv_stats['unique_symbols']}** symbols from "
        f"**{ohlcv_stats['min_date']}** to **{ohlcv_stats['max_date']}**"
    )

    # Tabs for different history views
    tab_market, tab_symbol = st.tabs(
        ["📊 Market Daily", "📈 Symbol OHLCV"]
    )

    # =========================================================================
    # Tab 1: Market Daily Aggregates
    # =========================================================================
    with tab_market:
        st.subheader("Market Daily Aggregates")

        st.markdown("""
        Daily market-wide statistics computed from OHLCV data.
        **Gainers** = symbols where close > open for that day.
        """)

        # Date range selector
        col1, col2 = st.columns(2)
        with col1:
            days_back = st.selectbox(
                "Date Range",
                options=["Last 30 days", "Last 90 days", "Last 180 days", "All data"],
                index=0,
                key="market_daily_range",
            )
        with col2:
            pass  # Reserved for future filters

        # Calculate date range
        from datetime import date as date_type
        from datetime import timedelta as td

        today = date_type.today()
        if days_back == "Last 30 days":
            start_date = (today - td(days=30)).isoformat()
        elif days_back == "Last 90 days":
            start_date = (today - td(days=90)).isoformat()
        elif days_back == "Last 180 days":
            start_date = (today - td(days=180)).isoformat()
        else:
            start_date = None

        # Fetch market daily data
        df = get_ohlcv_market_daily(con, start_date=start_date, limit=500)

        if df.empty:
            st.info("No market daily data available for selected range.")
        else:
            # Sort by date ascending for charts
            df = df.sort_values("date", ascending=True)
            st.caption(f"Showing {len(df)} trading days")

            # Chart 1: Market Breadth Over Time
            st.markdown("#### Daily Gainers vs Losers")
            fig_breadth = go.Figure()
            fig_breadth.add_trace(go.Scatter(
                x=df["date"],
                y=df["gainers"],
                mode="lines",
                name="Gainers",
                line={"color": "#00C853", "width": 2},
                fill="tozeroy",
                fillcolor="rgba(0, 200, 83, 0.1)",
            ))
            fig_breadth.add_trace(go.Scatter(
                x=df["date"],
                y=df["losers"],
                mode="lines",
                name="Losers",
                line={"color": "#FF1744", "width": 2},
            ))
            fig_breadth.add_trace(go.Scatter(
                x=df["date"],
                y=df["unchanged"],
                mode="lines",
                name="Unchanged",
                line={"color": "#9E9E9E", "width": 1, "dash": "dot"},
            ))
            fig_breadth.update_layout(
                title="Daily Market Breadth (Gainers vs Losers)",
                xaxis_title="Date",
                yaxis_title="Number of Symbols",
                height=450,
                hovermode="x unified",
                legend={"orientation": "h", "yanchor": "bottom", "y": 1.02},
            )
            st.plotly_chart(fig_breadth, use_container_width=True)

            # Chart 2: Total Volume Over Time
            st.markdown("#### Daily Total Volume")
            fig_volume = go.Figure()
            fig_volume.add_trace(go.Bar(
                x=df["date"],
                y=df["total_volume"],
                name="Total Volume",
                marker_color="#2196F3",
            ))
            fig_volume.update_layout(
                title="Daily Market Volume",
                xaxis_title="Date",
                yaxis_title="Volume",
                height=450,
                hovermode="x unified",
            )
            st.plotly_chart(fig_volume, use_container_width=True)

            # Chart 3: Average Change %
            st.markdown("#### Daily Average Change %")
            fig_chg = go.Figure()
            colors = [
                "#00C853" if v >= 0 else "#FF1744"
                for v in df["avg_change_pct"]
            ]
            fig_chg.add_trace(go.Bar(
                x=df["date"],
                y=df["avg_change_pct"],
                name="Avg Change %",
                marker_color=colors,
            ))
            fig_chg.add_hline(y=0, line_dash="dash", line_color="gray")
            fig_chg.update_layout(
                title="Daily Average Change % (across all symbols)",
                xaxis_title="Date",
                yaxis_title="Avg Change %",
                height=450,
                hovermode="x unified",
            )
            st.plotly_chart(fig_chg, use_container_width=True)

            # Table: Recent daily data
            st.markdown("#### Daily Summary Table")
            display_df = df.tail(30).sort_values("date", ascending=False)
            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "date": st.column_config.TextColumn("Date"),
                    "total_symbols": st.column_config.NumberColumn("Symbols"),
                    "gainers": st.column_config.NumberColumn("Gainers"),
                    "losers": st.column_config.NumberColumn("Losers"),
                    "unchanged": st.column_config.NumberColumn("Unchanged"),
                    "total_volume": st.column_config.NumberColumn(
                        "Volume", format="%,.0f"
                    ),
                    "avg_change_pct": st.column_config.NumberColumn(
                        "Avg Chg %", format="%.2f"
                    ),
                },
            )

    # =========================================================================
    # Tab 2: Symbol OHLCV History
    # =========================================================================
    with tab_symbol:
        st.subheader("Symbol OHLCV History")

        # Symbol input with suggestions
        symbols_list = get_symbols_list(con, is_active_only=True)
        col1, col2 = st.columns([2, 1])

        with col1:
            symbol_input = st.selectbox(
                "Select Symbol",
                options=[""] + symbols_list,
                index=0,
                key="history_ohlcv_symbol",
                help="Select a symbol to view its OHLCV history",
            )

        with col2:
            # Date range selector
            sym_range_options = [
                "Last 30 days",
                "Last 90 days",
                "Last 180 days",
                "Last 1 year",
                "All data",
            ]
            sym_selected_range = st.selectbox(
                "Date Range",
                options=sym_range_options,
                index=1,  # Default to 90 days
                key="symbol_ohlcv_range",
            )

        if symbol_input:
            # Get symbol stats
            sym_stats = get_ohlcv_symbol_stats(con, symbol_input)

            if sym_stats["total_rows"] == 0:
                st.info(f"No OHLCV history for {symbol_input}.")
            else:
                st.caption(
                    f"{symbol_input}: **{sym_stats['total_rows']}** days from "
                    f"**{sym_stats['min_date']}** to **{sym_stats['max_date']}** | "
                    f"Avg Volume: **{sym_stats['avg_volume']:,.0f}**"
                )

                # Calculate date range
                from datetime import date as date_type
                from datetime import timedelta as td

                today = date_type.today()
                if sym_selected_range == "Last 30 days":
                    start_date = (today - td(days=30)).isoformat()
                elif sym_selected_range == "Last 90 days":
                    start_date = (today - td(days=90)).isoformat()
                elif sym_selected_range == "Last 180 days":
                    start_date = (today - td(days=180)).isoformat()
                elif sym_selected_range == "Last 1 year":
                    start_date = (today - td(days=365)).isoformat()
                else:
                    start_date = None

                # Fetch symbol OHLCV history
                sym_df = get_ohlcv_range(con, symbol_input, start_date=start_date)

                if sym_df.empty:
                    st.info(f"No OHLCV data for {symbol_input} in selected range.")
                else:
                    st.caption(f"Showing {len(sym_df)} trading days")

                    # Chart type toggle
                    chart_type = st.radio(
                        "Chart Type",
                        options=["Candlestick", "Line"],
                        horizontal=True,
                        key="ohlcv_chart_type",
                    )

                    # Chart 1: Price OHLCV
                    st.markdown(f"#### {symbol_input} Price History")

                    if chart_type == "Candlestick":
                        fig_price = go.Figure(data=[go.Candlestick(
                            x=sym_df["date"],
                            open=sym_df["open"],
                            high=sym_df["high"],
                            low=sym_df["low"],
                            close=sym_df["close"],
                            name=symbol_input,
                        )])
                        fig_price.update_layout(
                            title=f"{symbol_input} OHLC",
                            xaxis_title="Date",
                            yaxis_title="Price (Rs.)",
                            height=500,
                            xaxis_rangeslider_visible=False,
                        )
                    else:
                        fig_price = go.Figure()
                        fig_price.add_trace(go.Scatter(
                            x=sym_df["date"],
                            y=sym_df["close"],
                            mode="lines",
                            name="Close",
                            line={"color": "#2196F3", "width": 2},
                        ))
                        fig_price.add_trace(go.Scatter(
                            x=sym_df["date"],
                            y=sym_df["open"],
                            mode="lines",
                            name="Open",
                            line={"color": "#9E9E9E", "width": 1, "dash": "dot"},
                        ))
                        fig_price.update_layout(
                            title=f"{symbol_input} Close Price",
                            xaxis_title="Date",
                            yaxis_title="Price (Rs.)",
                            height=500,
                            hovermode="x unified",
                        )
                    st.plotly_chart(fig_price, use_container_width=True)

                    # Chart 2: Volume
                    st.markdown(f"#### {symbol_input} Volume")
                    # Color bars by price direction
                    vol_colors = [
                        "#00C853" if c >= o else "#FF1744"
                        for o, c in zip(sym_df["open"], sym_df["close"])
                    ]
                    fig_vol = go.Figure()
                    fig_vol.add_trace(go.Bar(
                        x=sym_df["date"],
                        y=sym_df["volume"],
                        name="Volume",
                        marker_color=vol_colors,
                    ))
                    fig_vol.update_layout(
                        title=f"{symbol_input} Daily Volume",
                        xaxis_title="Date",
                        yaxis_title="Volume",
                        height=350,
                    )
                    st.plotly_chart(fig_vol, use_container_width=True)

                    # Table: OHLCV Data
                    st.markdown(f"#### {symbol_input} OHLCV Data")
                    with st.expander("View Data Table", expanded=False):
                        display_sym_df = sym_df.tail(100).sort_values(
                            "date", ascending=False
                        )
                        st.dataframe(
                            display_sym_df,
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                "symbol": st.column_config.TextColumn("Symbol"),
                                "date": st.column_config.TextColumn("Date"),
                                "open": st.column_config.NumberColumn(
                                    "Open", format="%.2f"
                                ),
                                "high": st.column_config.NumberColumn(
                                    "High", format="%.2f"
                                ),
                                "low": st.column_config.NumberColumn(
                                    "Low", format="%.2f"
                                ),
                                "close": st.column_config.NumberColumn(
                                    "Close", format="%.2f"
                                ),
                                "volume": st.column_config.NumberColumn(
                                    "Volume", format="%,.0f"
                                ),
                            },
                        )
        else:
            st.info("Select a symbol to view its OHLCV history.")

    render_footer()


# -----------------------------------------------------------------------------
# Page: Company Analytics
# -----------------------------------------------------------------------------
def company_analytics_page():
    """Company Analytics page for deep-dive into individual stocks."""

    con = get_connection()
    track_page_visit(con, "Company Analytics")

    # =================================================================
    # SEARCH BAR - Bloomberg Terminal Style
    # =================================================================
    all_symbols = get_symbols_list(con)
    symbols_with_profiles = get_symbols_with_profiles(con)
    default_symbol = st.session_state.get("company_symbol", "")

    # Compact search bar
    search_col1, search_col2, search_col3 = st.columns([4, 1, 1])

    with search_col1:
        symbol = st.text_input(
            "🔍 Search Symbol",
            value=default_symbol,
            placeholder="Enter symbol (e.g., OGDC, HBL, ENGRO)",
            label_visibility="collapsed",
        ).strip().upper()

    with search_col2:
        refresh_data = st.button("🔄 Refresh", type="primary", use_container_width=True)

    with search_col3:
        st.caption(f"{len(symbols_with_profiles)} companies")

    # Symbol suggestions
    if symbol and len(symbol) >= 1:
        matching = [s for s in all_symbols if s.startswith(symbol)][:8]
        if matching and symbol not in matching:
            suggestion_html = " ".join([
                f'<span style="background: rgba(33,150,243,0.1); padding: 2px 8px; '
                f'border-radius: 4px; margin: 2px; font-size: 12px;">{s}</span>'
                for s in matching
            ])
            st.markdown(f"Suggestions: {suggestion_html}", unsafe_allow_html=True)

    if not symbol:
        # Welcome screen when no symbol
        st.markdown("---")
        st.markdown("""
        <div style="text-align: center; padding: 60px 20px;">
            <h2 style="color: #2196F3;">🏢 Company Analytics</h2>
            <p style="color: #888; font-size: 16px;">
                Enter a symbol above to view comprehensive company data<br>
                including quotes, trading sessions, announcements, and financials.
            </p>
        </div>
        """, unsafe_allow_html=True)
        render_footer()
        return

    # Track search
    if st.session_state.get("last_searched_symbol") != symbol:
        track_symbol_search(con, symbol, "Company Analytics")
        st.session_state.last_searched_symbol = symbol

    # Handle refresh
    if refresh_data:
        track_button_click(con, "Refresh Data", "Company Analytics", symbol)
        with st.spinner(f"Fetching data for {symbol}..."):
            try:
                from psx_ohlcv.sources.deep_scraper import deep_scrape_symbol
                result = deep_scrape_symbol(con, symbol, save_raw_html=False)
                if result.get("success"):
                    parts = []
                    if result.get("snapshot_saved"):
                        parts.append("Quote")
                    if result.get("trading_sessions_saved", 0) > 0:
                        parts.append(f"{result['trading_sessions_saved']} markets")
                    if result.get("announcements_saved", 0) > 0:
                        parts.append(f"{result['announcements_saved']} announcements")
                    track_refresh(con, "deep_scrape", symbol, "Company Analytics", True, {})
                    st.success(f"✓ Updated: {', '.join(parts)}" if parts else "✓ Refreshed")
                    st.rerun()
                else:
                    st.error(f"Failed: {result.get('error', 'Unknown error')}")
            except Exception as e:
                st.error(f"Error: {e}")

    st.markdown("---")

    # =================================================================
    # FETCH DATA
    # =================================================================
    from psx_ohlcv.db import get_company_unified
    unified_data = get_company_unified(con, symbol)
    signals = get_company_latest_signals(con, symbol)
    quote_stats = get_company_quote_stats(con, symbol)

    if not unified_data:
        st.markdown(f"""
        <div style="text-align: center; padding: 40px; background: rgba(255,193,7,0.1);
                    border: 1px solid rgba(255,193,7,0.3); border-radius: 8px;">
            <h3>No data for {symbol}</h3>
            <p>Click <b>Refresh</b> to fetch data from PSX</p>
        </div>
        """, unsafe_allow_html=True)
        render_footer()
        return

    data = unified_data

    # =================================================================
    # COMPANY HEADER - Prominent Display
    # =================================================================
    company_name = data.get("company_name", symbol)
    sector_name = data.get("sector_name", "")
    snapshot_date = data.get("snapshot_date", "")
    price = data.get("price")
    change = data.get("change") or 0
    change_pct = data.get("change_pct") or 0

    # Determine color based on change
    price_color = "#00C853" if change_pct >= 0 else "#FF1744"
    change_sign = "+" if change_pct >= 0 else ""
    arrow = "▲" if change_pct > 0 else "▼" if change_pct < 0 else "●"

    st.markdown(f"""
    <div style="display: flex; justify-content: space-between; align-items: flex-start;
                padding: 16px 0; border-bottom: 1px solid rgba(255,255,255,0.1);">
        <div>
            <div style="font-size: 28px; font-weight: 700;">{symbol}</div>
            <div style="font-size: 14px; color: #888;">{company_name}</div>
            <div style="font-size: 12px; color: #666; margin-top: 4px;">
                {sector_name} • Data as of {snapshot_date}
            </div>
        </div>
        <div style="text-align: right;">
            <div style="font-size: 32px; font-weight: 700; font-family: monospace;">
                Rs. {price:,.2f}
            </div>
            <div style="font-size: 18px; color: {price_color}; font-family: monospace;">
                {arrow} {change_sign}{change:.2f} ({change_sign}{change_pct:.2f}%)
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("")  # Spacing

    # Quick Stats Row
    qs_col1, qs_col2, qs_col3, qs_col4, qs_col5, qs_col6 = st.columns(6)

    with qs_col1:
        vol = data.get("volume") or 0
        st.metric("Volume", format_volume(vol))

    with qs_col2:
        ldcp = data.get("ldcp")
        st.metric("LDCP", f"Rs. {ldcp:,.2f}" if ldcp else "N/A")

    with qs_col3:
        pe = data.get("pe_ratio")
        # Get sector P/E for comparison
        sector_code = data.get("sector_code") or data.get("sector")
        sector_pe_delta = None
        if pe and sector_code:
            try:
                sector_pe_result = con.execute("""
                    SELECT AVG(CAST(json_extract(cs.snapshot_json, '$.fundamentals.pe_ratio') AS REAL)) as avg_pe
                    FROM company_snapshots cs
                    JOIN symbols s ON cs.symbol = s.symbol
                    WHERE s.sector = ?
                    AND json_extract(cs.snapshot_json, '$.fundamentals.pe_ratio') > 0
                    AND cs.snapshot_date = (SELECT MAX(snapshot_date) FROM company_snapshots cs2 WHERE cs2.symbol = cs.symbol)
                """, (sector_code,)).fetchone()
                if sector_pe_result and sector_pe_result["avg_pe"]:
                    sector_avg_pe = sector_pe_result["avg_pe"]
                    pe_diff = pe - sector_avg_pe
                    # Negative delta is good (cheaper than sector)
                    sector_pe_delta = f"{pe_diff:+.1f} vs sector"
            except Exception:
                pass
        st.metric("P/E Ratio", f"{pe:.2f}" if pe else "N/A",
                  delta=sector_pe_delta, delta_color="inverse" if sector_pe_delta else "off",
                  help="Price-to-Earnings ratio. Lower may indicate undervaluation.")

    with qs_col4:
        mc = data.get("market_cap")
        if mc:
            mc_str = f"Rs. {mc/1e9:.1f}B" if mc >= 1e9 else f"Rs. {mc/1e6:.1f}M"
        else:
            mc_str = "N/A"
        st.metric("Market Cap", mc_str)

    with qs_col5:
        ytd = data.get("ytd_change_pct")
        if ytd:
            st.metric("YTD", f"{ytd:+.1f}%", delta=f"{ytd:+.1f}%")
        else:
            st.metric("YTD", "N/A")

    with qs_col6:
        y1 = data.get("one_year_change_pct")
        if y1:
            st.metric("1Y Change", f"{y1:+.1f}%", delta=f"{y1:+.1f}%")
        else:
            st.metric("1Y Change", "N/A")

    # =================================================================
    # VALUATION COMPARISON - Sector Context
    # =================================================================
    pe = data.get("pe_ratio")
    sector_code = data.get("sector_code") or data.get("sector")

    if pe and sector_code:
        try:
            # Get sector valuation metrics
            sector_valuation = con.execute("""
                SELECT
                    COUNT(*) as sector_count,
                    AVG(CAST(json_extract(cs.snapshot_json, '$.fundamentals.pe_ratio') AS REAL)) as avg_pe,
                    MIN(CAST(json_extract(cs.snapshot_json, '$.fundamentals.pe_ratio') AS REAL)) as min_pe,
                    MAX(CAST(json_extract(cs.snapshot_json, '$.fundamentals.pe_ratio') AS REAL)) as max_pe
                FROM company_snapshots cs
                JOIN symbols s ON cs.symbol = s.symbol
                WHERE s.sector = ?
                AND CAST(json_extract(cs.snapshot_json, '$.fundamentals.pe_ratio') AS REAL) > 0
                AND CAST(json_extract(cs.snapshot_json, '$.fundamentals.pe_ratio') AS REAL) < 500
                AND cs.snapshot_date = (SELECT MAX(snapshot_date) FROM company_snapshots cs2 WHERE cs2.symbol = cs.symbol)
            """, (sector_code,)).fetchone()

            # Get percentile rank within sector
            pe_rank = con.execute("""
                SELECT
                    COUNT(*) as cheaper_count,
                    (SELECT COUNT(*) FROM company_snapshots cs2
                     JOIN symbols s2 ON cs2.symbol = s2.symbol
                     WHERE s2.sector = ?
                     AND CAST(json_extract(cs2.snapshot_json, '$.fundamentals.pe_ratio') AS REAL) > 0
                     AND CAST(json_extract(cs2.snapshot_json, '$.fundamentals.pe_ratio') AS REAL) < 500
                     AND cs2.snapshot_date = (SELECT MAX(snapshot_date) FROM company_snapshots cs3 WHERE cs3.symbol = cs2.symbol)
                    ) as total_count
                FROM company_snapshots cs
                JOIN symbols s ON cs.symbol = s.symbol
                WHERE s.sector = ?
                AND CAST(json_extract(cs.snapshot_json, '$.fundamentals.pe_ratio') AS REAL) > ?
                AND CAST(json_extract(cs.snapshot_json, '$.fundamentals.pe_ratio') AS REAL) < 500
                AND cs.snapshot_date = (SELECT MAX(snapshot_date) FROM company_snapshots cs2 WHERE cs2.symbol = cs.symbol)
            """, (sector_code, sector_code, pe)).fetchone()

            if sector_valuation and sector_valuation["sector_count"] >= 3:
                sector_name = data.get("sector_name") or sector_code

                with st.expander(f"📊 Valuation vs {sector_name} Sector", expanded=False):
                    val_col1, val_col2, val_col3, val_col4 = st.columns(4)

                    with val_col1:
                        st.metric("Your P/E", f"{pe:.1f}")

                    with val_col2:
                        avg_pe = sector_valuation["avg_pe"]
                        diff = pe - avg_pe
                        st.metric("Sector Avg P/E", f"{avg_pe:.1f}",
                                  delta=f"{diff:+.1f}", delta_color="inverse")

                    with val_col3:
                        st.metric("Sector Range",
                                  f"{sector_valuation['min_pe']:.0f} - {sector_valuation['max_pe']:.0f}")

                    with val_col4:
                        if pe_rank and pe_rank["total_count"] > 0:
                            cheaper = pe_rank["cheaper_count"]
                            total = pe_rank["total_count"]
                            percentile = (cheaper / total) * 100
                            st.metric("Cheaper Than", f"{percentile:.0f}% of sector",
                                      help="Percentage of sector stocks with higher P/E (more expensive)")

                    # Visual comparison
                    if sector_valuation["max_pe"] > sector_valuation["min_pe"]:
                        pe_position = (pe - sector_valuation["min_pe"]) / (sector_valuation["max_pe"] - sector_valuation["min_pe"])
                        pe_position = min(1.0, max(0.0, pe_position))
                        st.progress(pe_position)
                        if pe_position < 0.33:
                            st.caption("✅ **Value Zone** - P/E in lower third of sector range")
                        elif pe_position < 0.67:
                            st.caption("⚪ **Fair Value** - P/E in middle of sector range")
                        else:
                            st.caption("⚠️ **Premium Valuation** - P/E in upper third of sector range")
        except Exception:
            pass

    # =================================================================
    # DETAILED QUOTE SECTION
    # =================================================================
    st.markdown("---")
    st.markdown("#### 📊 Quote Details")

    if data:
        # Bid/Ask and Ranges in a cleaner layout
        detail_col1, detail_col2, detail_col3 = st.columns(3)

        with detail_col1:
            st.markdown("**Day Range**")
            day_low = data.get("day_range_low")
            day_high = data.get("day_range_high")
            if day_low and day_high:
                current = data.get("price", 0)
                if day_high > day_low:
                    pct = (current - day_low) / (day_high - day_low)
                    st.progress(min(1.0, max(0.0, pct)))
                st.caption(f"Rs. {day_low:,.2f} — Rs. {day_high:,.2f}")
            else:
                st.caption("N/A")

        with detail_col2:
            st.markdown("**52-Week Range**")
            wk52_low = data.get("wk52_low")
            wk52_high = data.get("wk52_high")
            if wk52_low and wk52_high:
                current = data.get("price", 0)
                if wk52_high > wk52_low:
                    pct = (current - wk52_low) / (wk52_high - wk52_low)
                    st.progress(min(1.0, max(0.0, pct)))
                st.caption(f"Rs. {wk52_low:,.2f} — Rs. {wk52_high:,.2f}")
            else:
                st.caption("N/A")

        with detail_col3:
            st.markdown("**Circuit Breaker**")
            circuit_low = data.get("circuit_low")
            circuit_high = data.get("circuit_high")
            if circuit_low and circuit_high:
                st.caption(f"Lower: Rs. {circuit_low:,.2f}")
                st.caption(f"Upper: Rs. {circuit_high:,.2f}")
            else:
                st.caption("N/A")

        # Equity Structure Row
        st.markdown("")
        eq_col1, eq_col2, eq_col3, eq_col4 = st.columns(4)

        with eq_col1:
            total_shares = data.get("total_shares")
            if total_shares:
                ts_str = f"{total_shares/1e9:.2f}B" if total_shares >= 1e9 else f"{total_shares/1e6:.0f}M"
                st.metric("Total Shares", ts_str)
            else:
                st.metric("Total Shares", "N/A")

        with eq_col2:
            ff = data.get("free_float_shares")
            ff_pct = data.get("free_float_pct")
            if ff:
                ff_str = f"{ff/1e6:.0f}M ({ff_pct:.1f}%)" if ff_pct else f"{ff/1e6:.0f}M"
                st.metric("Free Float", ff_str)
            else:
                st.metric("Free Float", "N/A")

        with eq_col3:
            haircut = data.get("haircut")
            st.metric("Haircut", f"{haircut:.1f}%" if haircut else "N/A")

        with eq_col4:
            var = data.get("variance")
            st.metric("VAR", f"{var:.1f}%" if var else "N/A")

    # ----- Company Profile -----
    profile = data.get("profile_data", {})
    if profile or data.get("company_name"):
        st.subheader("🏢 Company Profile")

        profile_cols = st.columns(2)
        with profile_cols[0]:
            st.markdown(f"**Company Name:** {data.get('company_name') or profile.get('company_name', 'N/A')}")
            st.markdown(f"**Sector:** {data.get('sector_name') or profile.get('sector', 'N/A')}")
            st.markdown(f"**Listed In:** {profile.get('listed_in', 'N/A')}")
            shares = data.get("total_shares") or profile.get("shares_outstanding")
            if shares:
                st.markdown(f"**Shares Outstanding:** {shares:,}")
            else:
                st.markdown("**Shares Outstanding:** N/A")

        with profile_cols[1]:
            paid_up = profile.get("paid_up_capital")
            if paid_up:
                st.markdown(f"**Paid-up Capital:** Rs. {paid_up:,}")
            else:
                st.markdown("**Paid-up Capital:** N/A")
            st.markdown(f"**Face Value:** {profile.get('face_value', 'N/A')}")
            st.markdown(f"**Market Lot:** {profile.get('market_lot', 'N/A')}")
            st.markdown(f"**Fiscal Year End:** {profile.get('fiscal_year_end', 'N/A')}")

        # Additional info in expander
        with st.expander("More Details"):
            st.markdown(f"**Registrar:** {profile.get('registrar', 'N/A')}")
            st.markdown(f"**Last Updated:** {data.get('scraped_at', 'N/A')}")

    # ----- Trading Sessions (Multi-Market) -----
    trading_sessions = data.get("trading_sessions", {})
    trading_data = data.get("trading_data", {})
    if trading_sessions or trading_data:
        st.markdown("---")
        st.subheader("📈 Trading Sessions")

        # Combine today's sessions and snapshot trading data
        all_markets = set(list(trading_sessions.keys()) + list(trading_data.keys()))

        if all_markets:
            market_tabs = st.tabs(sorted(all_markets))
            for i, market in enumerate(sorted(all_markets)):
                with market_tabs[i]:
                    session = trading_sessions.get(market, {}) or trading_data.get(market, {})
                    if session:
                        mcols = st.columns(5)
                        with mcols[0]:
                            st.metric("Open", f"Rs. {session.get('open', 0):,.2f}" if session.get('open') else "N/A")
                        with mcols[1]:
                            st.metric("High", f"Rs. {session.get('high', 0):,.2f}" if session.get('high') else "N/A")
                        with mcols[2]:
                            st.metric("Low", f"Rs. {session.get('low', 0):,.2f}" if session.get('low') else "N/A")
                        with mcols[3]:
                            st.metric("Close", f"Rs. {session.get('close', 0):,.2f}" if session.get('close') else "N/A")
                        with mcols[4]:
                            vol = session.get('volume', 0)
                            if vol:
                                vol_str = f"{vol:,.0f}" if vol < 1000000 else f"{vol/1000000:.2f}M"
                            else:
                                vol_str = "N/A"
                            st.metric("Volume", vol_str)
                    else:
                        st.info(f"No data for {market}")
        else:
            st.info("No trading session data available.")

    # ----- Recent Announcements -----
    announcements = data.get("announcements", [])
    if announcements:
        st.markdown("---")
        st.subheader("📣 Recent Announcements")

        # Show count
        st.caption(f"Showing {len(announcements)} most recent announcements")

        for ann in announcements[:5]:
            with st.expander(f"{ann.get('announcement_date', 'N/A')} - {ann.get('announcement_type', 'News')}"):
                st.markdown(f"**{ann.get('title', 'No title')}**")
                if ann.get("content"):
                    st.markdown(ann.get("content", "")[:500] + "..." if len(ann.get("content", "")) > 500 else ann.get("content", ""))

        if len(announcements) > 5:
            with st.expander(f"View all {len(announcements)} announcements"):
                ann_df = pd.DataFrame(announcements)
                display_cols = ["announcement_date", "announcement_type", "title"]
                available_cols = [c for c in display_cols if c in ann_df.columns]
                if available_cols:
                    st.dataframe(ann_df[available_cols], use_container_width=True, hide_index=True)

    # ----- Charts -----
    st.subheader("📈 Charts")

    # Get quote history for charts
    quotes_df = get_company_quotes(con, symbol, limit=100)

    if not quotes_df.empty and len(quotes_df) > 1:
        import plotly.graph_objects as go

        chart_tabs = st.tabs(["Price Trend", "Volume"])

        with chart_tabs[0]:
            # Price trend line chart with auto-scaling y-axis
            chart_df = quotes_df.sort_values("ts", ascending=True)
            fig_price = go.Figure()
            fig_price.add_trace(go.Scatter(
                x=chart_df["ts"],
                y=chart_df["price"],
                mode="lines",
                name="Price",
                line={"color": "#2196F3", "width": 2},
            ))
            # Auto-scale y-axis to data range with 5% padding
            price_min = chart_df["price"].min()
            price_max = chart_df["price"].max()
            price_range = price_max - price_min
            padding = price_range * 0.05 if price_range > 0 else price_max * 0.05
            fig_price.update_layout(
                xaxis_title="Time",
                yaxis_title="Price (Rs.)",
                height=400,
                hovermode="x unified",
                yaxis={"range": [price_min - padding, price_max + padding]},
                margin={"l": 60, "r": 20, "t": 20, "b": 60},
            )
            st.plotly_chart(fig_price, use_container_width=True)

        with chart_tabs[1]:
            # Volume bar chart
            chart_df = quotes_df.sort_values("ts", ascending=True)
            fig_vol = go.Figure()
            fig_vol.add_trace(go.Bar(
                x=chart_df["ts"],
                y=chart_df["volume"],
                name="Volume",
                marker_color="#673AB7",
            ))
            fig_vol.update_layout(
                xaxis_title="Time",
                yaxis_title="Volume",
                height=400,
                hovermode="x unified",
                margin={"l": 60, "r": 20, "t": 20, "b": 60},
            )
            st.plotly_chart(fig_vol, use_container_width=True)

        # Stats
        if quote_stats:
            with st.expander("Quote Statistics"):
                stat_cols = st.columns(4)
                with stat_cols[0]:
                    total = quote_stats.get("total_snapshots", 0)
                    st.metric("Total Snapshots", total)
                with stat_cols[1]:
                    avg_p = quote_stats.get("avg_price", 0)
                    st.metric("Avg Price", f"Rs. {avg_p:,.2f}")
                with stat_cols[2]:
                    min_p = quote_stats.get("min_price", 0)
                    st.metric("Min Price", f"Rs. {min_p:,.2f}")
                with stat_cols[3]:
                    max_p = quote_stats.get("max_price", 0)
                    st.metric("Max Price", f"Rs. {max_p:,.2f}")
    else:
        st.info("Not enough quote history for charts. Take more snapshots over time.")

    # ----- Financial Data Tabs (from PSX tabs: FINANCIALS, RATIOS, PAYOUTS) -----
    st.markdown("---")
    st.subheader("📊 Financial Data")

    # Fetch financial data from new tables
    from psx_ohlcv.db import (
        get_company_financials as get_financials_df,
        get_company_ratios as get_ratios_df,
        get_company_payouts as get_payouts_df,
    )

    financials_df = get_financials_df(con, symbol)
    ratios_df = get_ratios_df(con, symbol)
    payouts_df = get_payouts_df(con, symbol)

    has_financial_data = (
        not financials_df.empty or
        not ratios_df.empty or
        not payouts_df.empty
    )

    if has_financial_data:
        fin_tabs = st.tabs(["📈 Financials", "📊 Ratios", "💰 Payouts"])

        # FINANCIALS Tab
        with fin_tabs[0]:
            if not financials_df.empty:
                st.markdown("*All numbers in thousands (000's) except EPS*")

                # Pivot for better display
                annual_df = financials_df[financials_df["period_type"] == "annual"]
                quarterly_df = financials_df[financials_df["period_type"] == "quarterly"]

                if not annual_df.empty:
                    st.markdown("**Annual Financials**")
                    display_cols = ["period_end", "sales", "profit_after_tax", "eps"]
                    available_cols = [c for c in display_cols if c in annual_df.columns]
                    col_config = {
                        "period_end": st.column_config.TextColumn("Year"),
                        "sales": st.column_config.NumberColumn("Sales (000s)", format="%,.0f"),
                        "profit_after_tax": st.column_config.NumberColumn("Profit After Tax (000s)", format="%,.0f"),
                        "eps": st.column_config.NumberColumn("EPS", format="%.2f"),
                    }
                    st.dataframe(
                        annual_df[available_cols].head(10),
                        use_container_width=True,
                        hide_index=True,
                        column_config=col_config,
                    )

                if not quarterly_df.empty:
                    st.markdown("**Quarterly Financials**")
                    display_cols = ["period_end", "sales", "profit_after_tax", "eps"]
                    available_cols = [c for c in display_cols if c in quarterly_df.columns]
                    st.dataframe(
                        quarterly_df[available_cols].head(10),
                        use_container_width=True,
                        hide_index=True,
                        column_config=col_config,
                    )
            else:
                st.info("No financial data available. Click 'Refresh Profile' to fetch data.")

        # RATIOS Tab
        with fin_tabs[1]:
            if not ratios_df.empty:
                annual_ratios = ratios_df[ratios_df["period_type"] == "annual"]

                if not annual_ratios.empty:
                    st.markdown("**Annual Ratios**")
                    display_cols = [
                        "period_end", "gross_profit_margin", "net_profit_margin",
                        "eps_growth", "peg_ratio"
                    ]
                    available_cols = [c for c in display_cols if c in annual_ratios.columns]
                    col_config = {
                        "period_end": st.column_config.TextColumn("Year"),
                        "gross_profit_margin": st.column_config.NumberColumn("Gross Margin %", format="%.2f%%"),
                        "net_profit_margin": st.column_config.NumberColumn("Net Margin %", format="%.2f%%"),
                        "eps_growth": st.column_config.NumberColumn("EPS Growth %", format="%.2f%%"),
                        "peg_ratio": st.column_config.NumberColumn("PEG", format="%.2f"),
                    }
                    st.dataframe(
                        annual_ratios[available_cols].head(10),
                        use_container_width=True,
                        hide_index=True,
                        column_config=col_config,
                    )

                # Show key ratios summary
                if len(annual_ratios) > 0:
                    latest = annual_ratios.iloc[0]
                    ratio_cols = st.columns(4)

                    with ratio_cols[0]:
                        gpm = latest.get("gross_profit_margin")
                        st.metric("Gross Margin", f"{gpm:.1f}%" if gpm else "N/A")

                    with ratio_cols[1]:
                        npm = latest.get("net_profit_margin")
                        st.metric("Net Margin", f"{npm:.1f}%" if npm else "N/A")

                    with ratio_cols[2]:
                        epsg = latest.get("eps_growth")
                        if epsg:
                            st.metric("EPS Growth", f"{epsg:+.1f}%", delta=f"{epsg:+.1f}%")
                        else:
                            st.metric("EPS Growth", "N/A")

                    with ratio_cols[3]:
                        peg = latest.get("peg_ratio")
                        st.metric("PEG Ratio", f"{peg:.2f}" if peg else "N/A")
            else:
                st.info("No ratio data available. Click 'Refresh Profile' to fetch data.")

        # PAYOUTS Tab
        with fin_tabs[2]:
            if not payouts_df.empty:
                st.markdown("**Dividend / Payout History**")
                display_cols = [
                    "ex_date", "payout_type", "amount", "fiscal_year",
                    "announcement_date"
                ]
                available_cols = [c for c in display_cols if c in payouts_df.columns]
                col_config = {
                    "ex_date": st.column_config.TextColumn("Ex-Date"),
                    "payout_type": st.column_config.TextColumn("Type"),
                    "amount": st.column_config.NumberColumn("Amount", format="%.2f"),
                    "fiscal_year": st.column_config.TextColumn("Fiscal Year"),
                    "announcement_date": st.column_config.TextColumn("Announced"),
                }
                st.dataframe(
                    payouts_df[available_cols].head(20),
                    use_container_width=True,
                    hide_index=True,
                    column_config=col_config,
                )

                # Summary metrics
                total_div = payouts_df[payouts_df["payout_type"] == "cash"]["amount"].sum()
                cash_count = len(payouts_df[payouts_df["payout_type"] == "cash"])
                bonus_count = len(payouts_df[payouts_df["payout_type"] == "bonus"])

                payout_cols = st.columns(3)
                with payout_cols[0]:
                    st.metric("Total Cash Dividends", f"Rs. {total_div:.2f}" if total_div else "N/A")
                with payout_cols[1]:
                    st.metric("Cash Payouts", cash_count)
                with payout_cols[2]:
                    st.metric("Bonus Issues", bonus_count)
            else:
                st.info("No payout history available.")
    else:
        st.info(
            "No financial data available yet. "
            "Click 'Refresh Profile' to fetch financial data from PSX."
        )

    render_footer()


# -----------------------------------------------------------------------------
# Page: Data Acquisition - Bulk Data Scraping & Collection
# -----------------------------------------------------------------------------
def data_acquisition_page():
    """Bulk data acquisition and scraping page."""
    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 📥 Data Acquisition")
        st.caption("Bulk data collection from PSX • Company profiles, financials & announcements")
    with header_col2:
        render_market_status_badge()

    con = get_connection()
    track_page_visit(con, "Data Acquisition")

    # Tabs for different sections
    tab1, tab2, tab3, tab4 = st.tabs([
        "🔍 Scrape Company",
        "📊 Company Snapshot",
        "📈 Trading Sessions",
        "📣 Announcements"
    ])

    # -------------------------------------------------------------------------
    # Tab 1: Scrape Company
    # -------------------------------------------------------------------------
    with tab1:
        st.subheader("Deep Scrape Company Data")
        st.markdown("""
        Extract **all available data** from PSX company pages including:
        - Trading data (REG/FUT/CSF/ODL markets)
        - Equity structure (market cap, shares, free float)
        - Financial statements & ratios
        - Corporate announcements
        - Key people & company profile
        """)

        col1, col2 = st.columns([2, 1])

        with col1:
            # Single symbol scrape
            symbol_input = st.text_input(
                "Enter Symbol",
                placeholder="e.g., OGDC, HBL, ENGRO",
                help="Enter a PSX stock symbol to deep scrape"
            ).upper().strip()

        with col2:
            save_html = st.checkbox("Save Raw HTML", value=False,
                help="Store raw HTML for reprocessing (increases storage)")

        if st.button("🔬 Deep Scrape", type="primary", disabled=not symbol_input):
            with st.spinner(f"Deep scraping {symbol_input}..."):
                track_button_click(con, "Deep Scrape", "Data Acquisition", symbol_input)
                result = deep_scrape_symbol(con, symbol_input, save_raw_html=save_html)

            if result.get("success"):
                st.success(f"Successfully scraped {symbol_input}!")
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Snapshot", "Saved" if result.get("snapshot_saved") else "Failed")
                col2.metric("Trading Sessions", result.get("trading_sessions_saved", 0))
                col3.metric("Announcements", result.get("announcements_saved", 0))
                col4.metric("Equity Data", "Saved" if result.get("equity_saved") else "N/A")
            else:
                st.error(f"Failed to scrape {symbol_input}: {result.get('error')}")

        st.divider()

        # Batch scrape section
        st.subheader("Batch Scrape Multiple Symbols")

        batch_input = st.text_area(
            "Enter Symbols (one per line or comma-separated)",
            placeholder="OGDC\nHBL\nENGRO\nPPL",
            height=100
        )

        col1, col2 = st.columns([1, 1])
        with col1:
            delay = st.slider("Delay between requests (seconds)", 0.5, 5.0, 1.0, 0.5)
        with col2:
            batch_save_html = st.checkbox("Save Raw HTML (Batch)", value=False)

        if st.button("🚀 Batch Scrape", disabled=not batch_input.strip()):
            # Parse symbols
            symbols = []
            for line in batch_input.strip().split("\n"):
                for sym in line.split(","):
                    sym = sym.strip().upper()
                    if sym:
                        symbols.append(sym)

            if symbols:
                st.info(f"Scraping {len(symbols)} symbols: {', '.join(symbols[:10])}{'...' if len(symbols) > 10 else ''}")

                progress_bar = st.progress(0)
                status_text = st.empty()

                def update_progress(current, total, symbol, result):
                    progress_bar.progress(current / total)
                    status = "✅" if result.get("success") else "❌"
                    status_text.text(f"{status} [{current}/{total}] {symbol}")

                with st.spinner("Batch scraping in progress..."):
                    track_button_click(con, "Batch Scrape", "Data Acquisition", metadata={"count": len(symbols)})
                    summary = deep_scrape_batch(
                        con, symbols,
                        delay=delay,
                        save_raw_html=batch_save_html,
                        progress_callback=update_progress
                    )

                progress_bar.progress(1.0)
                status_text.empty()

                # Show summary
                col1, col2, col3 = st.columns(3)
                col1.metric("Total", summary["total"])
                col2.metric("Completed", summary["completed"], delta_color="normal")
                col3.metric("Failed", summary["failed"], delta_color="inverse")

                if summary.get("errors"):
                    with st.expander("View Errors"):
                        for err in summary["errors"]:
                            st.error(f"{err['symbol']}: {err['error']}")

        st.divider()

        # ---------------------------------------------------------------------
        # Background Bulk Fetch - Runs in separate process
        # ---------------------------------------------------------------------
        st.subheader("Background Bulk Fetch")
        st.markdown("""
        Fetch deep data for **all active symbols** in the background.
        The job runs in a separate process - you can navigate away and come back.
        """)

        # Import background job functions
        from psx_ohlcv.db import (
            create_background_job,
            get_running_jobs,
            get_recent_jobs,
            request_job_stop,
            get_unread_notifications,
            mark_notification_read,
            mark_all_notifications_read,
        )

        # Show notifications
        notifications = get_unread_notifications(con)
        if notifications:
            st.markdown("#### Notifications")
            for notif in notifications:
                notif_type = notif.get("notification_type", "info")
                if notif_type == "completed":
                    st.success(f"**{notif['title']}**\n\n{notif.get('message', '')}")
                elif notif_type == "failed":
                    st.error(f"**{notif['title']}**\n\n{notif.get('message', '')}")
                elif notif_type == "stopped":
                    st.warning(f"**{notif['title']}**\n\n{notif.get('message', '')}")
                else:
                    st.info(f"**{notif['title']}**\n\n{notif.get('message', '')}")

            if st.button("Clear All Notifications", key="clear_notifs"):
                mark_all_notifications_read(con)
                st.rerun()

            st.divider()

        # Check for running jobs
        running_jobs = get_running_jobs(con)

        if running_jobs:
            st.markdown("#### Running Jobs")
            for job in running_jobs:
                job_id = job["job_id"]
                status = job["status"]
                completed = job.get("symbols_completed", 0)
                total = job.get("symbols_requested", 0)
                failed = job.get("symbols_failed", 0)
                current_symbol = job.get("current_symbol", "")
                current_batch = job.get("current_batch", 0)
                total_batches = job.get("total_batches", 0)

                progress = completed / total if total > 0 else 0

                with st.container():
                    st.markdown(f"**Job `{job_id}`** - {status.upper()}")

                    col1, col2, col3, col4 = st.columns(4)
                    col1.metric("Progress", f"{completed}/{total}")
                    col2.metric("Failed", failed)
                    col3.metric("Batch", f"{current_batch}/{total_batches}")
                    col4.metric("Current", current_symbol or "-")

                    st.progress(progress)

                    col1, col2 = st.columns([1, 3])
                    with col1:
                        if st.button("Stop Job", key=f"stop_{job_id}", type="secondary"):
                            request_job_stop(con, job_id)
                            st.warning(f"Stop requested for job {job_id}")
                            st.rerun()
                    with col2:
                        if st.button("Refresh", key=f"refresh_{job_id}"):
                            st.rerun()

                st.divider()
        else:
            # No running jobs - show start new job form
            # Get count of active symbols
            active_count = con.execute(
                "SELECT COUNT(*) FROM symbols WHERE is_active = 1"
            ).fetchone()[0]

            # Get count of symbols already scraped today
            today_str = datetime.now().strftime("%Y-%m-%d")
            scraped_today = con.execute(
                "SELECT COUNT(DISTINCT symbol) FROM company_snapshots WHERE snapshot_date = ?",
                (today_str,)
            ).fetchone()[0]

            col1, col2, col3 = st.columns(3)
            col1.metric("Active Symbols", active_count)
            col2.metric("Scraped Today", scraped_today)
            col3.metric("Remaining", active_count - scraped_today)

            # Job configuration
            st.markdown("#### Job Configuration")

            col1, col2 = st.columns(2)
            with col1:
                batch_size = st.number_input("Batch Size", min_value=10, max_value=200,
                    value=50, step=10, help="Symbols per batch")
                request_delay = st.slider("Request Delay (sec)", 0.5, 5.0, 1.5, 0.5,
                    help="Delay between requests")
            with col2:
                batch_pause = st.number_input("Batch Pause (sec)", min_value=10, max_value=120,
                    value=30, step=10, help="Pause between batches to avoid rate limiting")
                skip_scraped = st.checkbox("Skip Already Scraped Today", value=True)

            col1, col2 = st.columns(2)
            with col1:
                use_limit = st.checkbox("Limit Symbols", value=False,
                    help="Limit total symbols (useful for testing)")
            with col2:
                if use_limit:
                    symbol_limit = st.number_input("Max Symbols", min_value=10,
                        max_value=active_count, value=min(100, active_count), step=10)
                else:
                    symbol_limit = active_count

            # Start job button
            if st.button("Start Background Job", type="primary", key="start_bg_job"):
                # Get symbols to scrape
                if skip_scraped:
                    query = """
                        SELECT s.symbol FROM symbols s
                        WHERE s.is_active = 1
                        AND s.symbol NOT IN (
                            SELECT DISTINCT symbol FROM company_snapshots
                            WHERE snapshot_date = ?
                        )
                        ORDER BY s.symbol
                    """
                    symbols_to_scrape = [r[0] for r in con.execute(query, (today_str,)).fetchall()]
                else:
                    query = "SELECT symbol FROM symbols WHERE is_active = 1 ORDER BY symbol"
                    symbols_to_scrape = [r[0] for r in con.execute(query).fetchall()]

                # Apply limit
                if use_limit:
                    symbols_to_scrape = symbols_to_scrape[:symbol_limit]

                if not symbols_to_scrape:
                    st.warning("No symbols to scrape. All active symbols may already be scraped today.")
                else:
                    # Create job
                    job_id = create_background_job(
                        con,
                        job_type="bulk_deep_scrape",
                        symbols=symbols_to_scrape,
                        batch_size=batch_size,
                        batch_pause_sec=batch_pause,
                        config={
                            "request_delay": request_delay,
                            "save_raw_html": False,
                            "skip_scraped": skip_scraped,
                            "date": today_str,
                        },
                    )

                    # Start worker process
                    import subprocess
                    import sys

                    worker_cmd = [
                        sys.executable, "-m", "psx_ohlcv.worker", job_id
                    ]

                    # Start in background (detached)
                    subprocess.Popen(
                        worker_cmd,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        start_new_session=True,
                    )

                    st.success(f"Started background job `{job_id}` for {len(symbols_to_scrape)} symbols")
                    track_button_click(con, "Start Background Job", "Data Acquisition",
                        metadata={"job_id": job_id, "count": len(symbols_to_scrape)})
                    time.sleep(1)  # Give worker time to start
                    st.rerun()

        # Show recent jobs history
        st.markdown("#### Recent Jobs")
        recent_jobs = get_recent_jobs(con, limit=5)

        if recent_jobs:
            job_data = []
            for job in recent_jobs:
                job_data.append({
                    "Job ID": job["job_id"],
                    "Status": job["status"],
                    "Completed": f"{job.get('symbols_completed', 0)}/{job.get('symbols_requested', 0)}",
                    "Failed": job.get("symbols_failed", 0),
                    "Started": job.get("started_at", "")[:16] if job.get("started_at") else "",
                    "Ended": job.get("ended_at", "")[:16] if job.get("ended_at") else "-",
                })
            st.dataframe(job_data, use_container_width=True, hide_index=True)
        else:
            st.info("No jobs yet. Start a background job above.")

    # -------------------------------------------------------------------------
    # Tab 2: Company Snapshot Viewer
    # -------------------------------------------------------------------------
    with tab2:
        st.subheader("View Company Snapshot")

        # Get list of symbols with snapshots
        snapshot_symbols = con.execute(
            "SELECT DISTINCT symbol FROM company_snapshots ORDER BY symbol"
        ).fetchall()
        symbol_list = [r[0] for r in snapshot_symbols]

        if not symbol_list:
            st.info("No snapshots available yet. Use the 'Scrape Company' tab to capture data.")
        else:
            selected_symbol = st.selectbox("Select Symbol", symbol_list)

            if selected_symbol:
                track_symbol_search(con, selected_symbol, "Data Acquisition - Snapshot")
                snapshot = get_company_snapshot(con, selected_symbol)

                if snapshot:
                    # Header info
                    st.markdown(f"### {snapshot.get('company_name', selected_symbol)}")
                    st.caption(f"Sector: {snapshot.get('sector_name', 'N/A')} | Scraped: {snapshot.get('scraped_at', 'N/A')}")

                    # Trading Data
                    st.markdown("#### Trading Data")
                    trading_data = snapshot.get("trading_data", {})

                    if trading_data:
                        tabs = st.tabs(list(trading_data.keys()))
                        for i, (market, stats) in enumerate(trading_data.items()):
                            with tabs[i]:
                                if stats:
                                    # Create metrics grid
                                    cols = st.columns(4)
                                    metrics = [
                                        ("Open", stats.get("open")),
                                        ("High", stats.get("high")),
                                        ("Low", stats.get("low")),
                                        ("Close", stats.get("close")),
                                        ("Volume", stats.get("volume")),
                                        ("LDCP", stats.get("ldcp")),
                                        ("P/E (TTM)", stats.get("pe_ratio_ttm")),
                                        ("YTD %", stats.get("ytd_change")),
                                    ]
                                    for j, (label, value) in enumerate(metrics):
                                        with cols[j % 4]:
                                            if value is not None:
                                                if "%" in label:
                                                    st.metric(label, f"{value:.2f}%")
                                                elif isinstance(value, float) and value > 1000:
                                                    st.metric(label, f"{value:,.0f}")
                                                else:
                                                    st.metric(label, f"{value:,.2f}" if isinstance(value, float) else value)

                                    # Show ranges
                                    st.markdown("**Ranges**")
                                    range_cols = st.columns(3)
                                    with range_cols[0]:
                                        day_low = stats.get("day_range_low")
                                        day_high = stats.get("day_range_high")
                                        if day_low and day_high:
                                            st.caption(f"Day Range: {day_low:,.2f} - {day_high:,.2f}")
                                    with range_cols[1]:
                                        circuit_low = stats.get("circuit_low")
                                        circuit_high = stats.get("circuit_high")
                                        if circuit_low and circuit_high:
                                            st.caption(f"Circuit: {circuit_low:,.2f} - {circuit_high:,.2f}")
                                    with range_cols[2]:
                                        w52_low = stats.get("week_52_low")
                                        w52_high = stats.get("week_52_high")
                                        if w52_low and w52_high:
                                            st.caption(f"52-Week: {w52_low:,.2f} - {w52_high:,.2f}")

                    # Equity Structure
                    equity = snapshot.get("equity_data", {})
                    if equity:
                        st.markdown("#### Equity Structure")
                        eq_cols = st.columns(4)
                        eq_cols[0].metric("Market Cap", f"{equity.get('market_cap', 0):,.0f}")
                        eq_cols[1].metric("Shares", f"{equity.get('outstanding_shares', 0):,.0f}")
                        eq_cols[2].metric("Free Float", f"{equity.get('free_float_shares', 0):,.0f}")
                        eq_cols[3].metric("Float %", f"{equity.get('free_float_percent', 0):.1f}%")

                    # Financials Summary
                    financials = snapshot.get("financials_data", {})
                    if financials:
                        st.markdown("#### Financials Summary")
                        annual = financials.get("annual", [])
                        if annual:
                            fin_df = pd.DataFrame(annual)
                            if not fin_df.empty:
                                # Reorder columns
                                display_cols = ["period_end", "sales", "profit_after_tax", "eps"]
                                display_cols = [c for c in display_cols if c in fin_df.columns]
                                st.dataframe(fin_df[display_cols], use_container_width=True, hide_index=True)

                    # Ratios Summary
                    ratios = snapshot.get("ratios_data", {})
                    if ratios:
                        st.markdown("#### Ratios Summary")
                        annual_ratios = ratios.get("annual", [])
                        if annual_ratios:
                            ratio_df = pd.DataFrame(annual_ratios)
                            if not ratio_df.empty:
                                display_cols = ["period_end", "gross_profit_margin", "net_profit_margin", "eps_growth", "peg_ratio"]
                                display_cols = [c for c in display_cols if c in ratio_df.columns]
                                st.dataframe(ratio_df[display_cols], use_container_width=True, hide_index=True)

                    # Raw JSON viewer
                    with st.expander("View Raw JSON Data"):
                        # Remove raw_html from display (too large)
                        display_snapshot = {k: v for k, v in snapshot.items() if k != "raw_html"}
                        st.json(display_snapshot)

    # -------------------------------------------------------------------------
    # Tab 3: Trading Sessions
    # -------------------------------------------------------------------------
    with tab3:
        st.subheader("Trading Sessions Database")

        # Filters
        col1, col2, col3 = st.columns(3)

        with col1:
            ts_symbols = con.execute(
                "SELECT DISTINCT symbol FROM trading_sessions ORDER BY symbol"
            ).fetchall()
            ts_symbol_list = ["All"] + [r[0] for r in ts_symbols]
            filter_symbol = st.selectbox("Symbol", ts_symbol_list, key="ts_symbol")

        with col2:
            filter_market = st.selectbox("Market Type", ["All", "REG", "FUT", "CSF", "ODL"])

        with col3:
            filter_limit = st.number_input("Limit", min_value=10, max_value=1000, value=100)

        # Query
        symbol_filter = filter_symbol if filter_symbol != "All" else None
        market_filter = filter_market if filter_market != "All" else None

        df = get_trading_sessions(
            con,
            symbol=symbol_filter,
            market_type=market_filter,
            limit=filter_limit
        )

        if not df.empty:
            st.markdown(f"**{len(df)} records found**")

            # Display columns
            display_cols = [
                "symbol", "session_date", "market_type",
                "open", "high", "low", "close", "volume",
                "ldcp", "change_percent", "pe_ratio_ttm"
            ]
            display_cols = [c for c in display_cols if c in df.columns]

            st.dataframe(
                df[display_cols],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "open": st.column_config.NumberColumn(format="%.2f"),
                    "high": st.column_config.NumberColumn(format="%.2f"),
                    "low": st.column_config.NumberColumn(format="%.2f"),
                    "close": st.column_config.NumberColumn(format="%.2f"),
                    "volume": st.column_config.NumberColumn(format="%d"),
                    "ldcp": st.column_config.NumberColumn(format="%.2f"),
                    "change_percent": st.column_config.NumberColumn(format="%.2f%%"),
                    "pe_ratio_ttm": st.column_config.NumberColumn(format="%.2f"),
                }
            )

            # Download button
            csv = df.to_csv(index=False)
            st.download_button(
                "📥 Download CSV",
                csv,
                "trading_sessions.csv",
                "text/csv"
            )
        else:
            st.info("No trading sessions found. Use 'Scrape Company' to capture data.")

    # -------------------------------------------------------------------------
    # Tab 4: Corporate Announcements
    # -------------------------------------------------------------------------
    with tab4:
        st.subheader("Corporate Announcements")

        # Filters
        col1, col2, col3 = st.columns(3)

        with col1:
            ann_symbols = con.execute(
                "SELECT DISTINCT symbol FROM corporate_announcements ORDER BY symbol"
            ).fetchall()
            ann_symbol_list = ["All"] + [r[0] for r in ann_symbols]
            ann_filter_symbol = st.selectbox("Symbol", ann_symbol_list, key="ann_symbol")

        with col2:
            ann_types = con.execute(
                "SELECT DISTINCT announcement_type FROM corporate_announcements"
            ).fetchall()
            ann_type_list = ["All"] + [r[0] for r in ann_types]
            ann_filter_type = st.selectbox("Type", ann_type_list)

        with col3:
            ann_limit = st.number_input("Limit", min_value=10, max_value=500, value=50, key="ann_limit")

        # Query
        ann_symbol = ann_filter_symbol if ann_filter_symbol != "All" else None
        ann_type = ann_filter_type if ann_filter_type != "All" else None

        ann_df = get_corporate_announcements(
            con,
            symbol=ann_symbol,
            announcement_type=ann_type,
            limit=ann_limit
        )

        if not ann_df.empty:
            st.markdown(f"**{len(ann_df)} announcements found**")

            # Format and display
            display_cols = ["symbol", "announcement_date", "announcement_type", "title"]
            display_cols = [c for c in display_cols if c in ann_df.columns]

            st.dataframe(
                ann_df[display_cols],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "title": st.column_config.TextColumn(width="large"),
                }
            )
        else:
            st.info("No announcements found. Use 'Scrape Company' to capture data.")

    render_footer()


# -----------------------------------------------------------------------------
# Page: Factor Analysis - Quantitative Factor Rankings & Analysis
# -----------------------------------------------------------------------------
def factor_analysis_page():
    """Quantitative factor analysis and stock rankings."""
    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 📊 Factor Analysis")
        st.caption("Quantitative factor rankings • Value, Momentum, Quality & Volatility")
    with header_col2:
        render_market_status_badge()

    con = get_connection()
    track_page_visit(con, "Factor Analysis")

    # Check data availability
    snapshot_count = con.execute(
        "SELECT COUNT(DISTINCT symbol) FROM company_snapshots"
    ).fetchone()[0]

    if snapshot_count < 10:
        st.warning(
            f"Only {snapshot_count} companies with data. "
            "Go to **Data Acquisition** to scrape more companies for meaningful factor analysis."
        )

    st.markdown("---")

    # Tabs for different analyses
    tab1, tab2, tab3, tab4 = st.tabs([
        "📈 Factor Rankings",
        "🔄 Factor Correlations",
        "📊 Sector Exposure",
        "⚠️ Risk Metrics"
    ])

    # =========================================================================
    # Tab 1: Factor Rankings
    # =========================================================================
    with tab1:
        st.subheader("Multi-Factor Stock Rankings")
        st.markdown("""
        Stocks ranked by composite factor score combining **Value**, **Momentum**,
        **Quality**, and **Volatility** factors.
        """)

        # Factor weights
        st.markdown("#### Factor Weights")
        weight_cols = st.columns(4)
        with weight_cols[0]:
            w_value = st.slider("Value", 0.0, 1.0, 0.25, 0.05, key="w_value")
        with weight_cols[1]:
            w_momentum = st.slider("Momentum", 0.0, 1.0, 0.25, 0.05, key="w_momentum")
        with weight_cols[2]:
            w_quality = st.slider("Quality", 0.0, 1.0, 0.25, 0.05, key="w_quality")
        with weight_cols[3]:
            w_volatility = st.slider("Low Volatility", 0.0, 1.0, 0.25, 0.05, key="w_volatility")

        # Normalize weights
        total_weight = w_value + w_momentum + w_quality + w_volatility
        if total_weight > 0:
            w_value /= total_weight
            w_momentum /= total_weight
            w_quality /= total_weight
            w_volatility /= total_weight

        st.caption(f"Normalized: Value={w_value:.0%}, Momentum={w_momentum:.0%}, Quality={w_quality:.0%}, LowVol={w_volatility:.0%}")

        st.markdown("---")

        # Build factor data
        try:
            # Get latest snapshot data for each company
            factor_query = """
                WITH latest_snapshots AS (
                    SELECT
                        cs.symbol,
                        cs.snapshot_date,
                        json_extract(cs.snapshot_json, '$.quote_data.current') as price,
                        json_extract(cs.snapshot_json, '$.quote_data.ldcp') as ldcp,
                        json_extract(cs.snapshot_json, '$.quote_data.volume') as volume,
                        json_extract(cs.snapshot_json, '$.quote_data.high') as high,
                        json_extract(cs.snapshot_json, '$.quote_data.low') as low,
                        json_extract(cs.snapshot_json, '$.quote_data.week_52_low') as wk52_low,
                        json_extract(cs.snapshot_json, '$.quote_data.week_52_high') as wk52_high,
                        json_extract(cs.snapshot_json, '$.fundamentals.pe_ratio') as pe_ratio,
                        json_extract(cs.snapshot_json, '$.fundamentals.price_to_book') as pb_ratio,
                        json_extract(cs.snapshot_json, '$.fundamentals.dividend_yield') as div_yield,
                        json_extract(cs.snapshot_json, '$.fundamentals.eps') as eps,
                        json_extract(cs.snapshot_json, '$.equity_structure.market_cap') as market_cap,
                        s.sector as sector_code,
                        s.name as company_name
                    FROM company_snapshots cs
                    JOIN symbols s ON cs.symbol = s.symbol
                    WHERE cs.snapshot_date = (
                        SELECT MAX(snapshot_date) FROM company_snapshots cs2
                        WHERE cs2.symbol = cs.symbol
                    )
                    AND s.is_active = 1
                ),
                price_history AS (
                    SELECT
                        symbol,
                        (SELECT close FROM eod_ohlcv e2 WHERE e2.symbol = eod_ohlcv.symbol ORDER BY date DESC LIMIT 1) as latest_close,
                        (SELECT close FROM eod_ohlcv e3 WHERE e3.symbol = eod_ohlcv.symbol ORDER BY date DESC LIMIT 1 OFFSET 20) as close_20d_ago,
                        (SELECT close FROM eod_ohlcv e4 WHERE e4.symbol = eod_ohlcv.symbol ORDER BY date DESC LIMIT 1 OFFSET 60) as close_60d_ago,
                        (SELECT AVG(close) FROM (SELECT close FROM eod_ohlcv e5 WHERE e5.symbol = eod_ohlcv.symbol ORDER BY date DESC LIMIT 20)) as sma_20,
                        (SELECT AVG(close) FROM (SELECT close FROM eod_ohlcv e6 WHERE e6.symbol = eod_ohlcv.symbol ORDER BY date DESC LIMIT 50)) as sma_50
                    FROM eod_ohlcv
                    GROUP BY symbol
                )
                SELECT
                    ls.*,
                    ph.latest_close,
                    ph.close_20d_ago,
                    ph.close_60d_ago,
                    ph.sma_20,
                    ph.sma_50,
                    CASE WHEN ph.close_20d_ago > 0
                        THEN (ph.latest_close - ph.close_20d_ago) / ph.close_20d_ago * 100
                        ELSE 0 END as return_20d,
                    CASE WHEN ph.close_60d_ago > 0
                        THEN (ph.latest_close - ph.close_60d_ago) / ph.close_60d_ago * 100
                        ELSE 0 END as return_60d
                FROM latest_snapshots ls
                LEFT JOIN price_history ph ON ls.symbol = ph.symbol
                WHERE ls.price > 0
            """

            factor_df = pd.read_sql_query(factor_query, con)

            if factor_df.empty:
                st.info("No factor data available. Scrape company data first.")
            else:
                # Calculate factor scores (percentile ranks)
                # Value: Lower P/E is better (invert), higher dividend yield is better
                factor_df["pe_ratio"] = pd.to_numeric(factor_df["pe_ratio"], errors="coerce")
                factor_df["pb_ratio"] = pd.to_numeric(factor_df["pb_ratio"], errors="coerce")
                factor_df["div_yield"] = pd.to_numeric(factor_df["div_yield"], errors="coerce")
                factor_df["return_20d"] = pd.to_numeric(factor_df["return_20d"], errors="coerce")
                factor_df["return_60d"] = pd.to_numeric(factor_df["return_60d"], errors="coerce")
                factor_df["market_cap"] = pd.to_numeric(factor_df["market_cap"], errors="coerce")

                # Value Score: Low P/E + Low P/B + High Dividend Yield
                factor_df["value_score"] = 0
                if factor_df["pe_ratio"].notna().sum() > 5:
                    # Invert P/E (lower is better)
                    pe_valid = factor_df["pe_ratio"] > 0
                    factor_df.loc[pe_valid, "value_score"] += (1 - factor_df.loc[pe_valid, "pe_ratio"].rank(pct=True)) * 0.4
                if factor_df["pb_ratio"].notna().sum() > 5:
                    pb_valid = factor_df["pb_ratio"] > 0
                    factor_df.loc[pb_valid, "value_score"] += (1 - factor_df.loc[pb_valid, "pb_ratio"].rank(pct=True)) * 0.3
                if factor_df["div_yield"].notna().sum() > 5:
                    factor_df["value_score"] += factor_df["div_yield"].rank(pct=True).fillna(0) * 0.3

                # Momentum Score: 20-day and 60-day returns
                factor_df["momentum_score"] = 0
                if factor_df["return_20d"].notna().sum() > 5:
                    factor_df["momentum_score"] += factor_df["return_20d"].rank(pct=True).fillna(0) * 0.5
                if factor_df["return_60d"].notna().sum() > 5:
                    factor_df["momentum_score"] += factor_df["return_60d"].rank(pct=True).fillna(0) * 0.5

                # Quality Score: Higher EPS, larger market cap (proxy for stability)
                factor_df["eps"] = pd.to_numeric(factor_df["eps"], errors="coerce")
                factor_df["quality_score"] = 0
                if factor_df["eps"].notna().sum() > 5:
                    eps_positive = factor_df["eps"] > 0
                    factor_df.loc[eps_positive, "quality_score"] += factor_df.loc[eps_positive, "eps"].rank(pct=True) * 0.5
                if factor_df["market_cap"].notna().sum() > 5:
                    factor_df["quality_score"] += factor_df["market_cap"].rank(pct=True).fillna(0) * 0.5

                # Volatility Score: Lower 52-week range is better (inverted)
                factor_df["wk52_low"] = pd.to_numeric(factor_df["wk52_low"], errors="coerce")
                factor_df["wk52_high"] = pd.to_numeric(factor_df["wk52_high"], errors="coerce")
                factor_df["volatility_range"] = (factor_df["wk52_high"] - factor_df["wk52_low"]) / factor_df["wk52_low"]
                factor_df["volatility_score"] = 0
                if factor_df["volatility_range"].notna().sum() > 5:
                    # Invert - lower volatility is better
                    factor_df["volatility_score"] = 1 - factor_df["volatility_range"].rank(pct=True).fillna(0.5)

                # Composite Score
                factor_df["composite_score"] = (
                    w_value * factor_df["value_score"].fillna(0) +
                    w_momentum * factor_df["momentum_score"].fillna(0) +
                    w_quality * factor_df["quality_score"].fillna(0) +
                    w_volatility * factor_df["volatility_score"].fillna(0)
                )

                # Rank by composite score
                factor_df["rank"] = factor_df["composite_score"].rank(ascending=False, method="min")
                factor_df = factor_df.sort_values("composite_score", ascending=False)

                # Display top stocks
                st.markdown("#### Top Ranked Stocks")

                display_cols = [
                    "rank", "symbol", "company_name", "sector_code",
                    "composite_score", "value_score", "momentum_score",
                    "quality_score", "volatility_score",
                    "pe_ratio", "return_20d", "market_cap"
                ]
                display_df = factor_df[display_cols].head(30).copy()
                display_df.columns = [
                    "Rank", "Symbol", "Company", "Sector",
                    "Composite", "Value", "Momentum", "Quality", "LowVol",
                    "P/E", "20D Ret%", "Mkt Cap"
                ]

                # Format market cap
                display_df["Mkt Cap"] = display_df["Mkt Cap"].apply(
                    lambda x: f"Rs.{x/1e9:.1f}B" if pd.notna(x) and x >= 1e9
                    else f"Rs.{x/1e6:.0f}M" if pd.notna(x) else "N/A"
                )

                st.dataframe(
                    display_df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Rank": st.column_config.NumberColumn(format="%d"),
                        "Composite": st.column_config.ProgressColumn(min_value=0, max_value=1, format="%.2f"),
                        "Value": st.column_config.ProgressColumn(min_value=0, max_value=1, format="%.2f"),
                        "Momentum": st.column_config.ProgressColumn(min_value=0, max_value=1, format="%.2f"),
                        "Quality": st.column_config.ProgressColumn(min_value=0, max_value=1, format="%.2f"),
                        "LowVol": st.column_config.ProgressColumn(min_value=0, max_value=1, format="%.2f"),
                        "P/E": st.column_config.NumberColumn(format="%.1f"),
                        "20D Ret%": st.column_config.NumberColumn(format="%.1f%%"),
                    }
                )

                # Quick stats
                st.markdown("---")
                stat_cols = st.columns(4)
                with stat_cols[0]:
                    st.metric("Total Stocks Analyzed", len(factor_df))
                with stat_cols[1]:
                    avg_pe = factor_df["pe_ratio"].median()
                    st.metric("Median P/E", f"{avg_pe:.1f}" if pd.notna(avg_pe) else "N/A")
                with stat_cols[2]:
                    avg_momentum = factor_df["return_20d"].median()
                    st.metric("Median 20D Return", f"{avg_momentum:+.1f}%" if pd.notna(avg_momentum) else "N/A")
                with stat_cols[3]:
                    value_stocks = len(factor_df[factor_df["value_score"] > 0.7])
                    st.metric("High Value Stocks", value_stocks)

        except Exception as e:
            st.error(f"Error calculating factors: {e}")
            import traceback
            st.code(traceback.format_exc())

    # =========================================================================
    # Tab 2: Factor Correlations
    # =========================================================================
    with tab2:
        st.subheader("Factor Correlation Matrix")
        st.markdown("""
        Correlation between different factors. Low correlation means
        factors provide diversified signals.
        """)

        try:
            if 'factor_df' in dir() and not factor_df.empty:
                corr_cols = ["value_score", "momentum_score", "quality_score", "volatility_score"]
                corr_matrix = factor_df[corr_cols].corr()

                # Rename for display
                corr_matrix.index = ["Value", "Momentum", "Quality", "LowVol"]
                corr_matrix.columns = ["Value", "Momentum", "Quality", "LowVol"]

                import plotly.express as px
                fig = px.imshow(
                    corr_matrix,
                    text_auto=".2f",
                    color_continuous_scale="RdBu_r",
                    zmin=-1, zmax=1,
                    title="Factor Correlation Matrix"
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)

                st.markdown("""
                **Interpretation:**
                - Values close to **+1.0** = highly correlated (redundant signals)
                - Values close to **-1.0** = negatively correlated (hedging signals)
                - Values close to **0** = uncorrelated (diversifying signals)
                """)
            else:
                st.info("Run Factor Rankings first to see correlations.")
        except Exception as e:
            st.error(f"Error: {e}")

    # =========================================================================
    # Tab 3: Sector Exposure
    # =========================================================================
    with tab3:
        st.subheader("Factor Exposure by Sector")
        st.markdown("Average factor scores by sector to identify sector biases.")

        try:
            if 'factor_df' in dir() and not factor_df.empty:
                # Get sector names
                sector_map = get_sector_names(con)

                sector_exposure = factor_df.groupby("sector_code").agg({
                    "value_score": "mean",
                    "momentum_score": "mean",
                    "quality_score": "mean",
                    "volatility_score": "mean",
                    "symbol": "count"
                }).reset_index()
                sector_exposure.columns = ["Sector Code", "Value", "Momentum", "Quality", "LowVol", "Count"]
                sector_exposure["Sector"] = sector_exposure["Sector Code"].map(sector_map).fillna(sector_exposure["Sector Code"])
                sector_exposure = sector_exposure.sort_values("Count", ascending=False)

                st.dataframe(
                    sector_exposure[["Sector", "Count", "Value", "Momentum", "Quality", "LowVol"]],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Value": st.column_config.ProgressColumn(min_value=0, max_value=1, format="%.2f"),
                        "Momentum": st.column_config.ProgressColumn(min_value=0, max_value=1, format="%.2f"),
                        "Quality": st.column_config.ProgressColumn(min_value=0, max_value=1, format="%.2f"),
                        "LowVol": st.column_config.ProgressColumn(min_value=0, max_value=1, format="%.2f"),
                    }
                )

                # Bar chart
                import plotly.express as px
                melt_df = sector_exposure.melt(
                    id_vars=["Sector"],
                    value_vars=["Value", "Momentum", "Quality", "LowVol"],
                    var_name="Factor",
                    value_name="Score"
                )
                fig = px.bar(
                    melt_df.head(40),  # Top 10 sectors x 4 factors
                    x="Sector",
                    y="Score",
                    color="Factor",
                    barmode="group",
                    title="Factor Scores by Sector"
                )
                fig.update_layout(height=400, xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Run Factor Rankings first to see sector exposure.")
        except Exception as e:
            st.error(f"Error: {e}")

    # =========================================================================
    # Tab 4: Risk Metrics
    # =========================================================================
    with tab4:
        st.subheader("Portfolio Risk Metrics")
        st.markdown("""
        Risk analysis for top-ranked stocks. Essential for position sizing
        and portfolio construction.
        """)

        try:
            if 'factor_df' in dir() and not factor_df.empty:
                # Get top 20 stocks
                top_stocks = factor_df.head(20)["symbol"].tolist()

                # Calculate volatility from EOD data
                risk_query = """
                    SELECT
                        symbol,
                        COUNT(*) as trading_days,
                        AVG(close) as avg_price,
                        MIN(close) as min_price,
                        MAX(close) as max_price,
                        (MAX(close) - MIN(close)) / AVG(close) * 100 as range_pct
                    FROM eod_ohlcv
                    WHERE symbol IN ({})
                    AND date >= date('now', '-90 days')
                    GROUP BY symbol
                """.format(",".join([f"'{s}'" for s in top_stocks]))

                risk_df = pd.read_sql_query(risk_query, con)

                if not risk_df.empty:
                    # Merge with factor data
                    risk_df = risk_df.merge(
                        factor_df[["symbol", "composite_score", "market_cap"]],
                        on="symbol",
                        how="left"
                    )

                    st.markdown("#### Top 20 Stocks - Risk Profile")
                    risk_df["market_cap_str"] = risk_df["market_cap"].apply(
                        lambda x: f"Rs.{x/1e9:.1f}B" if pd.notna(x) and x >= 1e9
                        else f"Rs.{x/1e6:.0f}M" if pd.notna(x) else "N/A"
                    )

                    st.dataframe(
                        risk_df[["symbol", "trading_days", "avg_price", "range_pct", "composite_score", "market_cap_str"]].rename(columns={
                            "symbol": "Symbol",
                            "trading_days": "Days",
                            "avg_price": "Avg Price",
                            "range_pct": "90D Range%",
                            "composite_score": "Score",
                            "market_cap_str": "Mkt Cap"
                        }),
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "Avg Price": st.column_config.NumberColumn(format="Rs.%.2f"),
                            "90D Range%": st.column_config.NumberColumn(format="%.1f%%"),
                            "Score": st.column_config.ProgressColumn(min_value=0, max_value=1, format="%.2f"),
                        }
                    )

                    # Summary metrics
                    st.markdown("---")
                    st.markdown("#### Portfolio Summary (Equal-Weight Top 20)")

                    metric_cols = st.columns(4)
                    with metric_cols[0]:
                        avg_range = risk_df["range_pct"].mean()
                        st.metric("Avg 90D Range", f"{avg_range:.1f}%")
                    with metric_cols[1]:
                        total_mktcap = risk_df["market_cap"].sum()
                        st.metric("Total Mkt Cap", f"Rs.{total_mktcap/1e9:.0f}B" if total_mktcap else "N/A")
                    with metric_cols[2]:
                        avg_score = risk_df["composite_score"].mean()
                        st.metric("Avg Score", f"{avg_score:.2f}")
                    with metric_cols[3]:
                        st.metric("Stocks", len(risk_df))

                    st.markdown("""
                    ---
                    **Risk Notes:**
                    - 90D Range% shows price volatility - higher = riskier
                    - Consider position sizing inversely to volatility
                    - PSX circuit breakers limit daily moves to ±7.5%
                    - Thin liquidity stocks may have execution slippage
                    """)
                else:
                    st.info("Insufficient price history for risk analysis.")
            else:
                st.info("Run Factor Rankings first to see risk metrics.")
        except Exception as e:
            st.error(f"Error: {e}")

    render_footer()


# -----------------------------------------------------------------------------
# Page: Market Summary
# -----------------------------------------------------------------------------
def market_summary_page():
    """Download and manage market summary history files."""
    from datetime import date as date_type
    from datetime import timedelta as td

    from psx_ohlcv.sources.market_summary import (
        fetch_day_with_tracking,
        fetch_range_with_tracking,
        get_all_tracking_records,
        get_failed_dates,
        get_missing_dates,
        get_tracking_stats,
        init_market_summary_tracking,
    )

    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 📥 Market Summary")
        st.caption("Download daily OHLCV files from PSX DPS")
    with header_col2:
        render_market_status_badge()

    with st.expander("ℹ️ About Market Summary Files"):
        st.markdown("""
        Download daily market summary files from PSX DPS. These files contain
        complete market data (OHLCV + company info) for all traded symbols in a
        single compressed file per day.

        **Source:** `https://dps.psx.com.pk/download/mkt_summary/{date}.Z`
        """)

    con = get_connection()

    # Initialize tracking table
    init_market_summary_tracking(con)

    # Session state for download progress
    if "ms_download_progress" not in st.session_state:
        st.session_state.ms_download_progress = None
    if "ms_download_results" not in st.session_state:
        st.session_state.ms_download_results = []

    # Get stats
    stats = get_tracking_stats(con)

    # Stats row
    st.subheader("Download Statistics")
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Total Tracked", stats["total"])
    with col2:
        st.metric("OK", stats["ok"], delta_color="normal")
    with col3:
        st.metric("Missing (404)", stats["missing"], delta_color="off")
    with col4:
        st.metric("Failed", stats["failed"], delta_color="inverse")
    with col5:
        st.metric("Total Rows", f"{stats['total_rows']:,}")

    if stats["min_date"] and stats["max_date"]:
        st.caption(f"Date range: {stats['min_date']} to {stats['max_date']}")

    st.markdown("---")

    # Tabs for different actions
    tab_single, tab_range, tab_retry, tab_history = st.tabs([
        "📅 Single Day", "📆 Date Range", "🔄 Retry Failed", "📋 History"
    ])

    # =========================================================================
    # Tab 1: Single Day Download
    # =========================================================================
    with tab_single:
        st.subheader("Download Single Day")

        col1, col2 = st.columns([2, 1])
        with col1:
            single_date = st.date_input(
                "Select date",
                value=date_type.today() - td(days=1),
                max_value=date_type.today(),
                key="ms_single_date",
            )
        with col2:
            single_force = st.checkbox(
                "Force re-download",
                value=False,
                key="ms_single_force",
                help="Re-download even if already exists",
            )
            single_keep_raw = st.checkbox(
                "Keep raw files",
                value=False,
                key="ms_single_keep_raw",
                help="Keep the extracted .txt file",
            )

        if st.button("Download", key="ms_single_download", type="primary"):
            with st.spinner(f"Downloading {single_date}..."):
                try:
                    result = fetch_day_with_tracking(
                        con,
                        single_date,
                        force=single_force,
                        keep_raw=single_keep_raw,
                    )
                    if result["status"] == "ok":
                        st.success(
                            f"Downloaded {result['date']}: "
                            f"{result['row_count']} records"
                        )
                    elif result["status"] == "skipped":
                        msg = result.get('message', '')
                        st.info(f"Skipped {result['date']}: {msg}")
                    elif result["status"] == "missing":
                        st.warning(f"No data for {result['date']} (404)")
                    else:
                        st.error(
                            f"Failed {result['date']}: {result.get('message', '')}"
                        )
                except Exception as e:
                    st.error(f"Error: {e}")

    # =========================================================================
    # Tab 2: Date Range Download
    # =========================================================================
    with tab_range:
        st.subheader("Download Date Range")

        col1, col2 = st.columns(2)
        with col1:
            range_start = st.date_input(
                "Start date",
                value=date_type.today() - td(days=30),
                max_value=date_type.today(),
                key="ms_range_start",
            )
        with col2:
            range_end = st.date_input(
                "End date",
                value=date_type.today() - td(days=1),
                max_value=date_type.today(),
                key="ms_range_end",
            )

        col1, col2, col3 = st.columns(3)
        with col1:
            range_skip_weekends = st.checkbox(
                "Skip weekends",
                value=True,
                key="ms_range_skip_weekends",
                help="Skip Saturday and Sunday",
            )
        with col2:
            range_force = st.checkbox(
                "Force re-download",
                value=False,
                key="ms_range_force",
            )
        with col3:
            range_keep_raw = st.checkbox(
                "Keep raw files",
                value=False,
                key="ms_range_keep_raw",
            )

        # Calculate expected dates
        from psx_ohlcv.range_utils import iter_dates
        expected_dates = list(iter_dates(
            range_start, range_end, skip_weekends=range_skip_weekends
        ))
        st.caption(f"Will process {len(expected_dates)} dates")

        if st.button("Download Range", key="ms_range_download", type="primary"):
            if range_start > range_end:
                st.error("Start date must be before end date")
            else:
                progress_bar = st.progress(0)
                status_text = st.empty()
                results_container = st.container()

                ok_count = 0
                skip_count = 0
                missing_count = 0
                fail_count = 0

                for i, result in enumerate(fetch_range_with_tracking(
                    con,
                    range_start,
                    range_end,
                    skip_weekends=range_skip_weekends,
                    force=range_force,
                    keep_raw=range_keep_raw,
                )):
                    progress = (i + 1) / len(expected_dates)
                    progress_bar.progress(progress)

                    status = result["status"]
                    if status == "ok":
                        ok_count += 1
                    elif status == "skipped":
                        skip_count += 1
                    elif status == "missing":
                        missing_count += 1
                    else:
                        fail_count += 1

                    status_text.text(
                        f"Processing {result['date']}: {status} | "
                        f"OK: {ok_count}, Skip: {skip_count}, "
                        f"Missing: {missing_count}, Failed: {fail_count}"
                    )

                with results_container:
                    st.success(
                        f"Completed! OK: {ok_count}, Skipped: {skip_count}, "
                        f"Missing: {missing_count}, Failed: {fail_count}"
                    )

    # =========================================================================
    # Tab 3: Retry Failed/Missing
    # =========================================================================
    with tab_retry:
        st.subheader("Retry Failed Downloads")

        failed_dates = get_failed_dates(con)
        missing_dates = get_missing_dates(con)

        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**Failed dates:** {len(failed_dates)}")
            if failed_dates:
                with st.expander("View failed dates"):
                    for d in failed_dates[:50]:
                        st.text(d)
                    if len(failed_dates) > 50:
                        st.caption(f"...and {len(failed_dates) - 50} more")

        with col2:
            st.markdown(f"**Missing dates (404):** {len(missing_dates)}")
            if missing_dates:
                with st.expander("View missing dates"):
                    for d in missing_dates[:50]:
                        st.text(d)
                    if len(missing_dates) > 50:
                        st.caption(f"...and {len(missing_dates) - 50} more")

        col1, col2 = st.columns(2)
        with col1:
            if st.button(
                "Retry Failed",
                key="ms_retry_failed",
                disabled=len(failed_dates) == 0,
            ):
                progress_bar = st.progress(0)
                status_text = st.empty()
                ok_count = 0
                still_fail = 0

                for i, date_str in enumerate(failed_dates):
                    progress_bar.progress((i + 1) / len(failed_dates))
                    result = fetch_day_with_tracking(
                        con, date_str, force=True, retry_failed=True
                    )
                    if result["status"] == "ok":
                        ok_count += 1
                    else:
                        still_fail += 1
                    status_text.text(f"Retrying {date_str}: {result['status']}")

                st.success(
                    f"Retried {len(failed_dates)}: "
                    f"{ok_count} OK, {still_fail} still failed"
                )

        with col2:
            if st.button(
                "Retry Missing",
                key="ms_retry_missing",
                disabled=len(missing_dates) == 0,
                help="Retry dates that returned 404 (data may now be available)",
            ):
                progress_bar = st.progress(0)
                status_text = st.empty()
                ok_count = 0
                still_missing = 0

                for i, date_str in enumerate(missing_dates):
                    progress_bar.progress((i + 1) / len(missing_dates))
                    result = fetch_day_with_tracking(
                        con, date_str, force=True, retry_missing=True
                    )
                    if result["status"] == "ok":
                        ok_count += 1
                    else:
                        still_missing += 1
                    status_text.text(f"Retrying {date_str}: {result['status']}")

                st.success(
                    f"Retried {len(missing_dates)}: "
                    f"{ok_count} OK, {still_missing} still missing"
                )

    # =========================================================================
    # Tab 4: History
    # =========================================================================
    with tab_history:
        st.subheader("Download History")

        # Filter by status
        status_filter = st.multiselect(
            "Filter by status",
            options=["ok", "missing", "failed"],
            default=["ok", "missing", "failed"],
            key="ms_history_filter",
        )

        records = get_all_tracking_records(con, limit=500)

        if status_filter:
            records = [r for r in records if r["status"] in status_filter]

        if records:
            import pandas as pd
            df = pd.DataFrame(records)
            # Format for display
            cols = ["date", "status", "row_count", "message", "updated_at"]
            df_display = df[cols].copy()
            df_display.columns = ["Date", "Status", "Rows", "Message", "Updated"]

            # Color code status
            def style_status(val):
                if val == "ok":
                    return "background-color: #d4edda"
                elif val == "missing":
                    return "background-color: #fff3cd"
                elif val == "failed":
                    return "background-color: #f8d7da"
                return ""

            styled_df = df_display.style.map(style_status, subset=["Status"])
            st.dataframe(styled_df, use_container_width=True, height=400)

            st.caption(f"Showing {len(records)} records")
        else:
            st.info("No download history found. Start by downloading some dates.")

    render_footer()


# -----------------------------------------------------------------------------
# Page: Sync Monitor
# -----------------------------------------------------------------------------
def sync_monitor():
    """Monitor sync operations and run sync from UI."""
    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 🔄 Sync Monitor")
        st.caption("Data synchronization control center")
    with header_col2:
        render_market_status_badge()

    # Initialize session state for sync
    if "sync_result" not in st.session_state:
        st.session_state.sync_result = None
    if "sync_running" not in st.session_state:
        st.session_state.sync_running = False

    # Run Sync Section
    st.subheader("Run Sync")

    col1, col2 = st.columns([1, 1])

    with col1:
        refresh_symbols = st.checkbox(
            "Refresh symbols before sync",
            value=False,
            help="Fetch latest symbols from PSX market-watch before syncing",
            disabled=st.session_state.sync_running
        )
        incremental_mode = st.checkbox(
            "Incremental mode",
            value=True,
            help="Only fetch data newer than existing records (faster)",
            disabled=st.session_state.sync_running
        )

    with col2:
        cli_flags = "--all"
        if refresh_symbols:
            cli_flags += " --refresh-symbols"
        if incremental_mode:
            cli_flags += " --incremental"
        st.caption("Equivalent CLI command:")
        st.code(f"psxsync sync {cli_flags}", language="bash")

    # Run Sync Button
    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        run_full_sync = st.button(
            "▶️ Run Full Sync" if not st.session_state.sync_running else "⏳ Running...",
            type="primary",
            disabled=st.session_state.sync_running,
            help="Start syncing EOD data for all symbols"
        )

    with col2:
        if st.session_state.sync_running:
            st.warning("Sync in progress...")

    # Execute sync when button clicked
    if run_full_sync and not st.session_state.sync_running:
        st.session_state.sync_result = None
        st.session_state.sync_running = True

        with st.status("Running sync...", expanded=True) as status:
            st.write("🔄 Initializing sync...")

            try:
                config = SyncConfig(
                    incremental=incremental_mode,
                    max_retries=DEFAULT_SYNC_CONFIG.max_retries,
                    delay_min=DEFAULT_SYNC_CONFIG.delay_min,
                    delay_max=DEFAULT_SYNC_CONFIG.delay_max,
                    timeout=DEFAULT_SYNC_CONFIG.timeout,
                )

                if refresh_symbols:
                    st.write("📋 Refreshing symbols from PSX...")

                st.write("📊 Fetching EOD data for all symbols...")

                summary = sync_all(
                    db_path=get_db_path(),
                    refresh_symbols=refresh_symbols,
                    config=config,
                )

                st.session_state.sync_result = {
                    "success": True,
                    "summary": summary,
                }

                if summary.symbols_failed == 0:
                    status.update(
                        label="✅ Sync completed successfully!", state="complete"
                    )
                else:
                    fail_msg = f"⚠️ Completed with {summary.symbols_failed} failures"
                    status.update(label=fail_msg, state="complete")

            except Exception as e:
                st.session_state.sync_result = {
                    "success": False,
                    "error": str(e),
                }
                status.update(label="❌ Sync failed!", state="error")

            finally:
                st.session_state.sync_running = False

    # Display sync result
    if st.session_state.sync_result is not None:
        result = st.session_state.sync_result

        st.markdown("---")
        st.subheader("Sync Result")

        if result["success"]:
            summary = result["summary"]

            if summary.symbols_failed == 0:
                st.success(
                    f"✅ Sync completed: {summary.symbols_ok} symbols, "
                    f"{summary.rows_upserted:,} rows upserted"
                )
            else:
                st.warning(
                    f"⚠️ Sync completed with issues: {summary.symbols_ok} OK, "
                    f"{summary.symbols_failed} failed"
                )

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Symbols", summary.symbols_total)
            col2.metric("Symbols OK", summary.symbols_ok)
            failed_delta = (
                None if summary.symbols_failed == 0
                else f"-{summary.symbols_failed}"
            )
            col3.metric(
                "Symbols Failed",
                summary.symbols_failed,
                delta=failed_delta,
                delta_color="inverse"
            )
            col4.metric("Rows Upserted", f"{summary.rows_upserted:,}")

            if summary.failures:
                with st.expander(
                    f"🔍 View {len(summary.failures)} failures",
                    expanded=summary.symbols_failed <= 10
                ):
                    failures_df = pd.DataFrame(summary.failures)
                    failures_df.columns = ["Symbol", "Error Type", "Error Message"]
                    st.dataframe(failures_df, use_container_width=True, hide_index=True)

            st.caption(f"Run ID: `{summary.run_id}`")

        else:
            st.error(f"❌ Sync failed: {result['error']}")

    st.markdown("---")

    # Last Sync Summary
    try:
        con = get_connection()

        days_old, latest_date = get_data_freshness(con)
        badge_color, badge_text = get_freshness_badge(days_old)

        col1, col2 = st.columns([2, 1])
        with col1:
            st.subheader("Last Sync Summary")
        with col2:
            if badge_color == "green":
                st.success(f"📅 {badge_text}")
            elif badge_color == "orange":
                st.warning(f"📅 {badge_text}")
            elif badge_color == "red":
                st.error(f"📅 {badge_text}")

        last_run = pd.read_sql_query(
            """
            SELECT * FROM sync_runs
            WHERE ended_at IS NOT NULL
            ORDER BY ended_at DESC LIMIT 1
            """,
            con,
        )

        if last_run.empty:
            st.info("No sync runs recorded yet.")
        else:
            run = last_run.iloc[0]
            col1, col2, col3, col4 = st.columns(4)
            started = str(run["started_at"])[:16] if run["started_at"] else "N/A"
            col1.metric("Started", started)
            col2.metric("Symbols OK", run["symbols_ok"])
            col3.metric("Symbols Failed", run["symbols_failed"])
            col4.metric("Rows Upserted", f"{run['rows_upserted']:,}")

        st.markdown("---")

        # Recent Failures
        st.subheader("Recent Failures")
        failures_df = pd.read_sql_query(
            """
            SELECT symbol, error_type, error_message, created_at
            FROM sync_failures
            ORDER BY created_at DESC
            LIMIT 50
            """,
            con,
        )

        if failures_df.empty:
            st.success("✅ No failures recorded!")
        else:
            failures_df.columns = ["Symbol", "Error Type", "Message", "Time"]
            st.dataframe(failures_df, use_container_width=True, hide_index=True)

        st.markdown("---")

        # Sync History
        st.subheader("Sync History")
        history_df = pd.read_sql_query(
            """
            SELECT run_id, started_at, mode, symbols_ok, symbols_failed, rows_upserted
            FROM sync_runs
            ORDER BY started_at DESC
            LIMIT 20
            """,
            con,
        )

        if not history_df.empty:
            history_df.columns = ["Run ID", "Started", "Mode", "OK", "Failed", "Rows"]
            st.dataframe(history_df, use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"Database error: {e}")

    render_footer()


# -----------------------------------------------------------------------------
# Main App with Sidebar Navigation
# -----------------------------------------------------------------------------
def main():
    """Main app with sidebar navigation."""
    # Initialize session tracking
    init_session_tracking()

    # Sidebar
    st.sidebar.title("PSX OHLCV Explorer")
    st.sidebar.markdown("---")

    # Navigation - updated with new pages
    pages = [
        "📊 Dashboard",
        "📈 Candlestick Explorer",
        "⏱ Intraday Trend",
        "📊 Regular Market",
        "🏢 Company Analytics",
        "📥 Data Acquisition",
        "📊 Factor Analysis",
        "📚 History",
        "📥 Market Summary",
        "🧵 Symbols",
        "🔄 Sync Monitor",
        "📋 Schema",
        "⚙️ Settings",
    ]

    # Handle programmatic navigation
    if "nav_to" in st.session_state and st.session_state.nav_to:
        try:
            default_index = pages.index(st.session_state.nav_to)
        except ValueError:
            default_index = 0
        st.session_state.nav_to = None
    else:
        default_index = 0

    page = st.sidebar.radio(
        "Navigation", pages, index=default_index, label_visibility="collapsed"
    )

    st.sidebar.markdown("---")

    # Data freshness in sidebar
    try:
        con = get_connection()
        days_old, latest_date = get_data_freshness(con)
        if latest_date:
            badge_color, badge_text = get_freshness_badge(days_old)
            if badge_color == "green":
                st.sidebar.success(f"📅 {badge_text}")
            elif badge_color == "orange":
                st.sidebar.warning(f"📅 {badge_text}")
            elif badge_color == "red":
                st.sidebar.error(f"📅 {badge_text}")
    except Exception:
        pass

    st.sidebar.markdown("---")
    st.sidebar.caption("CLI: `psxsync --help`")
    st.sidebar.caption(f"DB: `{get_db_path()}`")

    # Route to page
    if page == "📊 Dashboard":
        dashboard()
    elif page == "📈 Candlestick Explorer":
        candlestick_explorer()
    elif page == "⏱ Intraday Trend":
        intraday_trend_page()
    elif page == "📊 Regular Market":
        regular_market_page()
    elif page == "🏢 Company Analytics":
        company_analytics_page()
    elif page == "📥 Data Acquisition":
        data_acquisition_page()
    elif page == "📊 Factor Analysis":
        factor_analysis_page()
    elif page == "📚 History":
        history_page()
    elif page == "📥 Market Summary":
        market_summary_page()
    elif page == "🧵 Symbols":
        symbols_page()
    elif page == "🔄 Sync Monitor":
        sync_monitor()
    elif page == "📋 Schema":
        schema_page()
    elif page == "⚙️ Settings":
        settings_page()


if __name__ == "__main__":
    main()
