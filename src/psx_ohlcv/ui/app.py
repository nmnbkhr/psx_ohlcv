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

# Auto-refresh for live data pages
try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False
    st_autorefresh = None

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
from psx_ohlcv.sync import sync_all, sync_intraday, sync_intraday_bulk
from psx_ohlcv.services import (
    is_service_running,
    read_status as read_service_status,
    start_service_background,
    stop_service,
)
from psx_ohlcv.services.announcements_service import (
    is_service_running as is_announcements_running,
    read_status as read_announcements_status,
    start_service_background as start_announcements_service,
    stop_service as stop_announcements_service,
)
from psx_ohlcv.services.eod_sync_service import (
    is_eod_sync_running,
    read_eod_status,
    start_eod_sync_background,
    stop_eod_sync,
)
from psx_ohlcv.sources.announcements import (
    fetch_announcements,
    fetch_corporate_events,
    fetch_company_payouts,
    save_announcement,
    save_corporate_event,
    save_dividend_payout,
)
from psx_ohlcv.ui.charts import (
    make_candlestick,
    make_intraday_chart,
    make_market_breadth_chart,
    make_price_line,
    make_top_movers_chart,
    make_volume_chart,
)
from psx_ohlcv.ui.themes import (
    get_theme_css,
    get_theme,
    get_chart_colors,
    get_plotly_layout,
    THEME_NAMES,
    ThemeName,
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
    get_latest_kse100,
)

# Phase 1: Instrument universe imports
from psx_ohlcv.db import (
    get_instruments,
    get_ohlcv_instrument,
    get_eod_ohlcv,
)
from psx_ohlcv.analytics_phase1 import (
    compute_rankings,
    get_rankings,
    get_normalized_performance,
    compute_all_metrics,
)
from psx_ohlcv.sync_instruments import sync_instruments_eod

# Phase 2: FX analytics imports
from psx_ohlcv.db import (
    get_fx_pairs,
    get_fx_ohlcv,
    get_fx_latest_rate,
    get_fx_adjusted_metrics,
)
from psx_ohlcv.analytics_fx import (
    get_fx_analytics,
    compute_and_store_fx_adjusted_metrics,
    get_normalized_fx_performance,
    get_fx_impact_summary,
)
from psx_ohlcv.sync_fx import sync_fx_pairs, seed_fx_pairs

# Page config - must be first Streamlit command
st.set_page_config(
    page_title="PSX OHLCV Explorer",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =============================================================================
# THEME SYSTEM
# Supports multiple themes: default (original) and bloomberg (professional terminal)
# Theme is persisted in session_state and applied via CSS injection
# =============================================================================

def init_theme():
    """Initialize theme in session state if not set."""
    if "theme" not in st.session_state:
        # Default to Bloomberg theme for professional trading terminal look
        st.session_state.theme = "bloomberg"


def get_current_theme() -> ThemeName:
    """Get current theme from session state."""
    init_theme()
    return st.session_state.theme


def set_theme(theme_name: ThemeName):
    """Set theme in session state."""
    if theme_name in THEME_NAMES:
        st.session_state.theme = theme_name


def inject_theme_css():
    """Inject current theme CSS into the page."""
    theme_name = get_current_theme()
    css = get_theme_css(theme_name)
    st.markdown(css, unsafe_allow_html=True)


# Initialize theme and inject CSS
init_theme()
inject_theme_css()


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


# =============================================================================
# UI ENHANCEMENT HELPERS
# Loading states, error messages, and data freshness indicators
# =============================================================================

def render_skeleton_loader(height: int = 100, text: str = "Loading..."):
    """Render a skeleton loading placeholder."""
    st.markdown(
        f"""
        <div class="skeleton" style="height: {height}px; display: flex; align-items: center; justify-content: center;">
            <span style="color: #666; font-size: 13px;">{text}</span>
        </div>
        """,
        unsafe_allow_html=True
    )


def render_data_warning(message: str, icon: str = "⚠️"):
    """Render a warning banner for data issues."""
    st.markdown(
        f'<div class="data-warning">{icon} {message}</div>',
        unsafe_allow_html=True
    )


def render_data_info(message: str, icon: str = "ℹ️"):
    """Render an info banner."""
    st.markdown(
        f'<div class="data-info">{icon} {message}</div>',
        unsafe_allow_html=True
    )


def render_data_error(message: str, icon: str = "❌"):
    """Render an error banner for failed operations."""
    st.markdown(
        f'<div class="data-error">{icon} {message}</div>',
        unsafe_allow_html=True
    )


def render_empty_state(message: str, icon: str = "📭"):
    """Render an empty state placeholder."""
    st.markdown(
        f"""
        <div class="empty-state">
            <div class="empty-state-icon">{icon}</div>
            <div>{message}</div>
        </div>
        """,
        unsafe_allow_html=True
    )


def render_freshness_badge(days_old: int | None, date_str: str | None) -> str:
    """Render a data freshness badge with appropriate color."""
    if days_old is None or date_str is None:
        return '<span class="data-old">No data</span>'
    elif days_old == 0:
        return f'<span class="data-fresh">✓ Today ({date_str})</span>'
    elif days_old == 1:
        return f'<span class="data-fresh">Yesterday ({date_str})</span>'
    elif days_old <= 3:
        return f'<span class="data-stale">{days_old} days old ({date_str})</span>'
    else:
        return f'<span class="data-old">{days_old} days old ({date_str})</span>'


def render_section_with_loading(title: str, data_loader_func, empty_message: str = "No data available"):
    """
    Render a section with loading state handling.

    Args:
        title: Section title
        data_loader_func: Function that loads and renders data, should return True if data exists
        empty_message: Message to show if no data
    """
    try:
        with st.spinner(f"Loading {title}..."):
            has_data = data_loader_func()
        if not has_data:
            render_empty_state(empty_message, "📭")
    except Exception as e:
        render_data_error(f"Failed to load {title}: {str(e)[:100]}")


def get_user_friendly_error(error: Exception) -> str:
    """Convert technical errors to user-friendly messages."""
    error_str = str(error).lower()

    if "no such table" in error_str:
        return "Database tables not initialized. Try syncing data first."
    elif "connection" in error_str or "database" in error_str:
        return "Unable to connect to database. Please check your settings."
    elif "timeout" in error_str:
        return "Request timed out. The server may be slow or unavailable."
    elif "network" in error_str or "connection refused" in error_str:
        return "Network error. Please check your internet connection."
    elif "permission" in error_str:
        return "Permission denied. Check file/folder permissions."
    else:
        # Return a truncated version of the original error
        return f"An error occurred: {str(error)[:100]}"


def check_data_staleness(con, table: str = "eod_ohlcv", date_col: str = "date") -> tuple[bool, str]:
    """
    Check if data in a table is stale.

    Also checks regular_market_current for live data.

    Returns:
        Tuple of (is_stale, message)
    """
    try:
        latest_dates = []

        # Check the specified table
        try:
            result = con.execute(
                f"SELECT MAX({date_col}) as max_date FROM {table}"
            ).fetchone()
            if result and result["max_date"]:
                latest_dates.append(str(result["max_date"])[:10])
        except Exception:
            pass

        # Also check regular_market_current for live data
        try:
            result = con.execute(
                "SELECT MAX(DATE(ts)) as max_date FROM regular_market_current"
            ).fetchone()
            if result and result["max_date"]:
                latest_dates.append(str(result["max_date"])[:10])
        except Exception:
            pass

        if latest_dates:
            most_recent = max(latest_dates)
            latest_date = datetime.strptime(most_recent, "%Y-%m-%d")
            days_old = (datetime.now() - latest_date).days
            if days_old > 3:
                return True, f"Data is {days_old} days old (last: {most_recent})"
            return False, ""
        return True, "No data found in database"
    except Exception:
        return True, "Unable to check data freshness"


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
    Get data freshness info from multiple sources.

    Checks eod_ohlcv, regular_market_current, and psx_indices tables
    to find the most recent data timestamp.

    Returns:
        Tuple of (days_old, latest_date_str) or (None, None) if no data.
    """
    latest_dates = []

    # Check eod_ohlcv
    try:
        result = _con.execute(
            "SELECT MAX(date) as max_date FROM eod_ohlcv"
        ).fetchone()
        if result and result["max_date"]:
            latest_dates.append(str(result["max_date"])[:10])
    except Exception:
        pass

    # Check regular_market_current (live market data)
    try:
        result = _con.execute(
            "SELECT MAX(DATE(ts)) as max_date FROM regular_market_current"
        ).fetchone()
        if result and result["max_date"]:
            latest_dates.append(str(result["max_date"])[:10])
    except Exception:
        pass

    # Check psx_indices
    try:
        result = _con.execute(
            "SELECT MAX(index_date) as max_date FROM psx_indices"
        ).fetchone()
        if result and result["max_date"]:
            latest_dates.append(str(result["max_date"])[:10])
    except Exception:
        pass

    if latest_dates:
        # Get the most recent date
        most_recent = max(latest_dates)
        latest_date = datetime.strptime(most_recent, "%Y-%m-%d")
        days_old = (datetime.now() - latest_date).days
        return days_old, most_recent

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

    # =================================================================
    # AUTO-REFRESH WHEN SERVICE IS RUNNING
    # =================================================================
    service_running, _ = is_service_running()
    if service_running and HAS_AUTOREFRESH and st_autorefresh:
        # Refresh every 60 seconds (60000 ms)
        st_autorefresh(interval=60000, limit=None, key="dashboard_autorefresh")

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
            # Data Freshness + Service Status
            days_old, latest_date = get_data_freshness(con)
            badge_color, badge_text = get_freshness_badge(days_old)
            service_status = read_service_status()
            if latest_date:
                freshness_color = "#00C853" if badge_color == "green" else "#FFC107" if badge_color == "orange" else "#FF1744"
                sync_indicator = "🟢" if service_running else "🔴"
                st.markdown(
                    f'<div style="text-align: right; font-size: 12px;">'
                    f'<span style="color: {freshness_color};">●</span> Data: {badge_text}<br>'
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
                icon="📅"
            )

        # =================================================================
        # KSE-100 INDEX DISPLAY - Primary Market Benchmark
        # =================================================================
        try:
            # Try to get real KSE-100 index data first
            kse100_data = get_latest_kse100(con)

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
                        arrow = "▲"
                        change_sign = "+"
                    elif change < 0:
                        idx_color = "#FF1744"
                        arrow = "▼"
                        change_sign = ""
                    else:
                        idx_color = "#78909C"
                        arrow = "●"
                        change_sign = ""

                    st.markdown(f"""
                    <div style="background: linear-gradient(135deg, rgba(33,150,243,0.15) 0%, rgba(33,150,243,0.05) 100%);
                                border: 1px solid rgba(33,150,243,0.3); border-radius: 12px; padding: 20px;">
                        <div style="font-size: 12px; color: #888; margin-bottom: 4px;">
                            📊 KSE-100 Index
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
        except Exception as e:
            # Show user-friendly error instead of silent failure
            render_data_info(
                "Index data temporarily unavailable. Showing available market data.",
                icon="📊"
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
                    st.markdown("**📈 Volume Leaders**")
                    for _, row in vol_df.iterrows():
                        vol = row["volume"]
                        vol_str = f"{vol/1e6:.2f}M" if vol >= 1e6 else f"{vol:,.0f}"
                        change = row["change_percent"] or 0
                        color = "🟢" if change > 0 else "🔴" if change < 0 else "⚪"
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
            # Volume leaders/52-week range not critical, continue gracefully
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
    # AUTO-REFRESH WHEN SERVICE IS RUNNING
    # =================================================================
    service_running, service_pid = is_service_running()
    service_status = read_service_status()

    # Auto-refresh every 60 seconds if service is running and autorefresh is available
    if service_running and HAS_AUTOREFRESH and st_autorefresh:
        # Refresh every 60 seconds (60000 ms)
        count = st_autorefresh(interval=60000, limit=None, key="intraday_autorefresh")

    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2, header_col3 = st.columns([2, 1, 1])
    with header_col1:
        st.markdown("## ⏱ Intraday Trend")
        st.caption("Live intraday price movements and volume throughout the trading day")
    with header_col2:
        render_market_status_badge()
    with header_col3:
        # Show service status
        if service_running:
            st.success("🟢 Auto-Sync ON")
            if service_status.last_run_at:
                last_sync = service_status.last_run_at[:19]
                st.caption(f"Last: {last_sync}")
        else:
            st.info("🔴 Auto-Sync OFF")
            st.caption("Start service on Data Sync page")

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
            # Schema: quote_data, trading_data, equity_data, financials_data, ratios_data (all JSON)
            factor_query = """
                WITH latest_snapshots AS (
                    SELECT
                        cs.symbol,
                        cs.snapshot_date,
                        cs.company_name,
                        cs.sector_name as sector_code,
                        -- From quote_data: close price
                        json_extract(cs.quote_data, '$.close') as price,
                        -- From trading_data (REG segment): volume, high, low, 52-week, P/E
                        json_extract(cs.trading_data, '$.REG.ldcp') as ldcp,
                        json_extract(cs.trading_data, '$.REG.volume') as volume,
                        json_extract(cs.trading_data, '$.REG.high') as high,
                        json_extract(cs.trading_data, '$.REG.low') as low,
                        json_extract(cs.trading_data, '$.REG.week_52_low') as wk52_low,
                        json_extract(cs.trading_data, '$.REG.week_52_high') as wk52_high,
                        json_extract(cs.trading_data, '$.REG.pe_ratio_ttm') as pe_ratio,
                        json_extract(cs.trading_data, '$.REG.ytd_change') as ytd_change,
                        json_extract(cs.trading_data, '$.REG.year_1_change') as year_1_change,
                        -- From equity_data: market cap
                        json_extract(cs.equity_data, '$.market_cap') as market_cap,
                        json_extract(cs.equity_data, '$.free_float_percent') as free_float_pct,
                        -- From financials_data: EPS (latest annual)
                        json_extract(cs.financials_data, '$.annual[0].eps') as eps,
                        -- From ratios_data: profit margins
                        json_extract(cs.ratios_data, '$.annual[0].net_profit_margin') as net_margin,
                        json_extract(cs.ratios_data, '$.annual[0].eps_growth') as eps_growth
                    FROM company_snapshots cs
                    WHERE cs.snapshot_date = (
                        SELECT MAX(snapshot_date) FROM company_snapshots cs2
                        WHERE cs2.symbol = cs.symbol
                    )
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
                # Convert numeric columns
                factor_df["pe_ratio"] = pd.to_numeric(factor_df["pe_ratio"], errors="coerce")
                factor_df["return_20d"] = pd.to_numeric(factor_df["return_20d"], errors="coerce")
                factor_df["return_60d"] = pd.to_numeric(factor_df["return_60d"], errors="coerce")
                factor_df["market_cap"] = pd.to_numeric(factor_df["market_cap"], errors="coerce")
                factor_df["eps"] = pd.to_numeric(factor_df["eps"], errors="coerce")
                factor_df["net_margin"] = pd.to_numeric(factor_df["net_margin"], errors="coerce")
                factor_df["ytd_change"] = pd.to_numeric(factor_df["ytd_change"], errors="coerce")

                # Value Score: Low P/E + High Net Margin (profitable at low valuation)
                factor_df["value_score"] = 0
                if factor_df["pe_ratio"].notna().sum() > 5:
                    # Invert P/E (lower is better)
                    pe_valid = factor_df["pe_ratio"] > 0
                    factor_df.loc[pe_valid, "value_score"] += (1 - factor_df.loc[pe_valid, "pe_ratio"].rank(pct=True)) * 0.6
                if factor_df["net_margin"].notna().sum() > 5:
                    # Higher net margin is better
                    margin_valid = factor_df["net_margin"] > 0
                    factor_df.loc[margin_valid, "value_score"] += factor_df.loc[margin_valid, "net_margin"].rank(pct=True) * 0.4

                # Momentum Score: 20-day, 60-day returns, and YTD change
                factor_df["momentum_score"] = 0
                if factor_df["return_20d"].notna().sum() > 5:
                    factor_df["momentum_score"] += factor_df["return_20d"].rank(pct=True).fillna(0) * 0.4
                if factor_df["return_60d"].notna().sum() > 5:
                    factor_df["momentum_score"] += factor_df["return_60d"].rank(pct=True).fillna(0) * 0.4
                if factor_df["ytd_change"].notna().sum() > 5:
                    factor_df["momentum_score"] += factor_df["ytd_change"].rank(pct=True).fillna(0) * 0.2

                # Quality Score: Higher EPS + larger market cap + higher margins
                factor_df["quality_score"] = 0
                if factor_df["eps"].notna().sum() > 5:
                    eps_positive = factor_df["eps"] > 0
                    factor_df.loc[eps_positive, "quality_score"] += factor_df.loc[eps_positive, "eps"].rank(pct=True) * 0.4
                if factor_df["market_cap"].notna().sum() > 5:
                    factor_df["quality_score"] += factor_df["market_cap"].rank(pct=True).fillna(0) * 0.3
                if factor_df["net_margin"].notna().sum() > 5:
                    margin_valid = factor_df["net_margin"] > 0
                    factor_df.loc[margin_valid, "quality_score"] += factor_df.loc[margin_valid, "net_margin"].rank(pct=True) * 0.3

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

                    # KSE-100 Benchmark Comparison
                    st.markdown("---")
                    st.markdown("#### Benchmark Comparison")

                    kse100 = get_latest_kse100(con)
                    if kse100:
                        bench_cols = st.columns([2, 1, 1])
                        with bench_cols[0]:
                            kse_value = kse100.get("value", 0)
                            kse_change = kse100.get("change_pct", 0) or 0
                            kse_color = "#00C853" if kse_change >= 0 else "#FF1744"

                            st.markdown(f"""
                            <div style="background: rgba(33,150,243,0.1); border-radius: 8px; padding: 12px;
                                        border: 1px solid rgba(33,150,243,0.2);">
                                <div style="font-size: 11px; color: #888;">KSE-100 Index</div>
                                <div style="font-family: monospace; font-size: 18px; font-weight: 600;">
                                    {kse_value:,.2f}
                                    <span style="color: {kse_color}; font-size: 14px;">({kse_change:+.2f}%)</span>
                                </div>
                            </div>
                            """, unsafe_allow_html=True)
                        with bench_cols[1]:
                            ytd = kse100.get("ytd_change_pct")
                            if ytd:
                                ytd_color = "#00C853" if ytd >= 0 else "#FF1744"
                                st.metric("Index YTD", f"{ytd:+.2f}%", delta_color="off")
                        with bench_cols[2]:
                            one_yr = kse100.get("one_year_change_pct")
                            if one_yr:
                                st.metric("Index 1-Year", f"{one_yr:+.2f}%", delta_color="off")

                        st.caption("Compare factor portfolio performance against KSE-100 benchmark to measure alpha generation.")
                    else:
                        st.info("No KSE-100 benchmark data available. Scrape index data to enable benchmark comparison.")

                else:
                    st.info("Insufficient price history for risk analysis.")
            else:
                st.info("Run Factor Rankings first to see risk metrics.")
        except Exception as e:
            st.error(f"Error: {e}")

    render_footer()


# -----------------------------------------------------------------------------
# Page: AI Insights (GPT-5.2 powered analysis)
# -----------------------------------------------------------------------------
def ai_insights_page():
    """AI-powered market insights using OpenAI GPT-5.2.

    This page provides LLM-generated analysis for:
    - Company summaries (profile + quote + OHLCV)
    - Intraday commentary (time series + volume)
    - Market summaries (gainers/losers + sectors)
    - Historical analysis (OHLCV patterns)
    """
    import os

    # =================================================================
    # HEADER
    # =================================================================
    # Custom CSS for AI Insights page theme
    st.markdown("""
    <style>
    /* Page Header Styling */
    .ai-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 12px;
        padding: 24px;
        margin-bottom: 20px;
        text-align: center;
        box-shadow: 0 4px 20px rgba(102, 126, 234, 0.3);
    }
    .ai-header h1 {
        color: white;
        margin: 0;
        font-size: 2em;
    }
    .ai-header p {
        color: rgba(255, 255, 255, 0.85);
        margin: 8px 0 0 0;
    }

    /* Mode Selection Cards */
    .mode-card {
        background: rgba(255, 255, 255, 0.05);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 8px;
        padding: 12px 16px;
        margin: 4px 0;
        transition: all 0.3s ease;
    }
    .mode-card:hover {
        background: rgba(102, 126, 234, 0.1);
        border-color: rgba(102, 126, 234, 0.3);
    }
    .mode-card.active {
        background: rgba(102, 126, 234, 0.15);
        border-color: #667eea;
        box-shadow: 0 2px 10px rgba(102, 126, 234, 0.2);
    }

    /* Generate Button Enhancement */
    .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%) !important;
        border: none !important;
        box-shadow: 0 4px 15px rgba(102, 126, 234, 0.4) !important;
        transition: all 0.3s ease !important;
    }
    .stButton > button[kind="primary"]:hover {
        box-shadow: 0 6px 20px rgba(102, 126, 234, 0.6) !important;
        transform: translateY(-2px);
    }

    /* Info Cards */
    .info-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border-radius: 8px;
        padding: 16px;
        border-left: 3px solid #667eea;
    }
    </style>
    """, unsafe_allow_html=True)

    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 🤖 AI Insights")
        st.caption("GPT-5.2 powered market analysis • Company, Intraday, Market & Historical insights")
    with header_col2:
        render_market_status_badge()

    con = get_connection()
    track_page_visit(con, "AI Insights")

    # =================================================================
    # API KEY CHECK
    # =================================================================
    api_key_set = bool(os.environ.get("OPENAI_API_KEY", "").strip())

    if not api_key_set:
        st.warning(
            "**OpenAI API Key Not Configured**\n\n"
            "To use AI Insights, set the `OPENAI_API_KEY` environment variable:\n"
            "```bash\n"
            "export OPENAI_API_KEY='your-api-key-here'\n"
            "```\n\n"
            "Then restart the Streamlit app."
        )
        st.info(
            "**Why is an API key needed?**\n"
            "AI Insights uses OpenAI's GPT-5.2 model to analyze your stock data "
            "and provide intelligent commentary. This requires an OpenAI API account."
        )
        render_footer()
        return

    st.markdown("---")

    # =================================================================
    # DATA CAVEAT WARNING (Always shown)
    # =================================================================
    with st.expander("⚠️ Important Data Caveats", expanded=False):
        st.warning(
            "**DERIVED HIGH/LOW WARNING**\n\n"
            "The daily high and low values in this application's EOD data are calculated as "
            "`max(open, close)` and `min(open, close)` respectively.\n\n"
            "**These are NOT true intraday highs and lows.** Actual intraday price "
            "extremes may differ significantly from what is shown.\n\n"
            "The AI analysis acknowledges this limitation."
        )
        st.info(
            "**Other Caveats:**\n"
            "- PSX circuit breakers: ±7.5% daily limits\n"
            "- Some stocks may have thin liquidity\n"
            "- Data may have slight delays from live market\n"
            "- Historical data subject to corporate actions"
        )

    # =================================================================
    # INSIGHT MODE SELECTION
    # =================================================================
    st.markdown("### 📊 Select Analysis Mode")

    # Mode details with icons and enhanced descriptions
    mode_details = {
        "Company": {
            "icon": "🏢",
            "title": "Company Summary",
            "desc": "Profile, latest quote, OHLCV history, financials & corporate news",
        },
        "Intraday": {
            "icon": "📈",
            "title": "Intraday Analysis",
            "desc": "Session price/volume patterns, trading activity & momentum",
        },
        "Market": {
            "icon": "🌐",
            "title": "Market Summary",
            "desc": "Gainers, losers, sector performance & market breadth",
        },
        "History": {
            "icon": "📜",
            "title": "Historical Analysis",
            "desc": "Long-term OHLCV patterns, trends & technical insights",
        },
    }

    # Create 4 columns for mode cards
    mode_cols = st.columns(4)
    modes = ["Company", "Intraday", "Market", "History"]

    # Use session state for mode selection
    if "ai_insight_mode" not in st.session_state:
        st.session_state.ai_insight_mode = "Company"

    for i, mode in enumerate(modes):
        with mode_cols[i]:
            details = mode_details[mode]
            is_selected = st.session_state.ai_insight_mode == mode
            if st.button(
                f"{details['icon']} {details['title']}",
                key=f"mode_{mode}",
                use_container_width=True,
                type="primary" if is_selected else "secondary",
            ):
                st.session_state.ai_insight_mode = mode
                st.rerun()

    insight_mode = st.session_state.ai_insight_mode

    # Show selected mode description
    selected_details = mode_details[insight_mode]
    st.info(f"{selected_details['icon']} **{selected_details['title']}**: {selected_details['desc']}")

    st.markdown("---")

    # =================================================================
    # MODE-SPECIFIC CONTROLS
    # =================================================================
    if insight_mode in ["Company", "Intraday", "History"]:
        # Symbol selection - get_symbols_list returns list of strings directly
        symbol_options = get_symbols_list(con)

        if not symbol_options:
            st.warning("No symbols available. Please sync data first.")
            render_footer()
            return

        selected_symbol = st.selectbox(
            "Select Symbol",
            options=symbol_options,
            index=0,
            help="Choose a stock symbol for analysis",
        )
    else:
        selected_symbol = None

    # Mode-specific parameters
    if insight_mode == "Company":
        ohlcv_days = st.slider(
            "OHLCV History (days)",
            min_value=5,
            max_value=90,
            value=30,
            help="Number of trading days of price history to include",
        )
        include_financials = st.checkbox("Include Financial Data", value=True)

    elif insight_mode == "Intraday":
        # Get available intraday dates
        try:
            cur = con.execute(
                """
                SELECT DISTINCT DATE(ts) as date
                FROM intraday_bars
                WHERE symbol = ?
                ORDER BY date DESC
                LIMIT 30
                """,
                (selected_symbol,),
            )
            available_dates = [row[0] for row in cur.fetchall()]
        except Exception:
            available_dates = []

        if available_dates:
            trading_date = st.selectbox(
                "Trading Date",
                options=available_dates,
                index=0,
                help="Select the trading day for intraday analysis",
            )
        else:
            st.warning(f"No intraday data available for {selected_symbol}")
            trading_date = None

    elif insight_mode == "Market":
        # Get available market dates
        try:
            cur = con.execute(
                """
                SELECT DISTINCT session_date
                FROM trading_sessions
                WHERE market_type = 'REG'
                ORDER BY session_date DESC
                LIMIT 30
                """
            )
            market_dates = [row[0] for row in cur.fetchall()]
        except Exception:
            market_dates = []

        if market_dates:
            market_date = st.selectbox(
                "Market Date",
                options=market_dates,
                index=0,
                help="Select the date for market summary",
            )
        else:
            st.warning("No market data available")
            market_date = None

        top_n = st.slider(
            "Top N Movers",
            min_value=5,
            max_value=20,
            value=10,
            help="Number of top gainers/losers to include",
        )

    elif insight_mode == "History":
        history_days = st.slider(
            "History Period (days)",
            min_value=30,
            max_value=365,
            value=90,
            help="Number of trading days to analyze",
        )

    st.markdown("---")

    # =================================================================
    # GENERATE BUTTON AND RESULTS
    # =================================================================
    st.markdown("### 🚀 Generate Analysis")

    gen_col1, gen_col2, gen_col3 = st.columns([2, 1, 1])

    with gen_col1:
        generate_clicked = st.button(
            "✨ Generate AI Insight",
            type="primary",
            use_container_width=True,
            help="Generate AI-powered analysis using GPT-5.2",
        )

    with gen_col2:
        use_cache = st.checkbox("💾 Use Cache", value=True, help="Use cached responses if available (6hr TTL)")

    with gen_col3:
        # Show estimated tokens
        est_tokens = {
            "Company": "~2-3k tokens",
            "Intraday": "~1.5-2.5k tokens",
            "Market": "~2.5-3.5k tokens",
            "History": "~3-4.5k tokens",
        }
        st.caption(f"Est: {est_tokens.get(insight_mode, '~2k tokens')}")

    # Generate insight when button clicked
    if generate_clicked:
        try:
            # Import LLM modules (lazy import to avoid errors if not configured)
            from psx_ohlcv.llm.client import OpenAIClient, LLMError, is_api_key_configured
            from psx_ohlcv.llm.prompts import PromptBuilder, InsightMode as LLMInsightMode
            from psx_ohlcv.llm.cache import LLMCache, init_llm_cache_schema, get_db_freshness_marker
            from psx_ohlcv.llm.data_loader import DataLoader, format_data_for_prompt

            # Initialize cache
            init_llm_cache_schema(con)
            cache = LLMCache(con, ttl_hours=6)
            loader = DataLoader(con)

            # Map UI mode to LLM mode
            mode_mapping = {
                "Company": LLMInsightMode.COMPANY,
                "Intraday": LLMInsightMode.INTRADAY,
                "Market": LLMInsightMode.MARKET,
                "History": LLMInsightMode.HISTORY,
            }
            llm_mode = mode_mapping[insight_mode]

            # Load data based on mode
            with st.spinner("Loading data..."):
                if insight_mode == "Company":
                    data = loader.load_company_data(
                        selected_symbol,
                        ohlcv_days=ohlcv_days,
                        include_financials=include_financials,
                    )
                    prompt_data = format_data_for_prompt(data)
                    cache_symbol = selected_symbol
                    date_range = prompt_data.get("date_range", "")

                elif insight_mode == "Intraday":
                    if not trading_date:
                        st.error("No trading date selected")
                        return
                    data = loader.load_intraday_data(selected_symbol, trading_date)
                    prompt_data = format_data_for_prompt(data)
                    cache_symbol = selected_symbol
                    date_range = trading_date

                elif insight_mode == "Market":
                    if not market_date:
                        st.error("No market date selected")
                        return
                    data = loader.load_market_data(market_date, top_n=top_n)
                    prompt_data = format_data_for_prompt(data)
                    cache_symbol = "MARKET"
                    date_range = market_date

                elif insight_mode == "History":
                    data = loader.load_company_data(
                        selected_symbol,
                        ohlcv_days=history_days,
                        include_financials=False,
                    )
                    prompt_data = format_data_for_prompt(data)
                    cache_symbol = selected_symbol
                    date_range = prompt_data.get("date_range", "")

            # Show data provenance
            with st.expander("📊 Data Used (click to expand)", expanded=False):
                st.markdown("**Tables Queried:**")
                if hasattr(data, 'provenance'):
                    st.write(data.provenance.tables_used)
                    st.markdown(f"**Row Count:** {data.provenance.row_count}")
                    st.markdown(f"**Date Range:** {data.provenance.date_range[0]} to {data.provenance.date_range[1]}")
                    if data.provenance.was_downsampled:
                        st.warning(f"Data was downsampled from {data.provenance.original_row_count} rows")
                st.markdown(f"**Generated At:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            # Build prompt
            builder = PromptBuilder(llm_mode)
            prompt = builder.build(**prompt_data)

            # Check cache
            db_freshness = get_db_freshness_marker(con, cache_symbol if cache_symbol != "MARKET" else None)
            cache_key = cache.compute_key(
                symbol=cache_symbol,
                mode=llm_mode.value,
                date_range=date_range,
                db_freshness=db_freshness,
            )

            cached_response = None
            if use_cache:
                cached_response = cache.get(cache_key)

            if cached_response:
                st.success("✅ Using cached response")
                response_text = cached_response.response_text
                was_cached = True
            else:
                # Generate with LLM
                with st.spinner("🤖 Generating AI insight (this may take a moment)..."):
                    client = OpenAIClient(
                        model="gpt-5.2",
                        timeout=90,
                        max_tokens=4096,
                        temperature=0.3,
                    )

                    response = client.generate(
                        prompt=prompt,
                        system_prompt=builder.system_prompt,
                    )

                    response_text = response.content
                    was_cached = False

                    # Cache the response
                    cache.set(
                        cache_key=cache_key,
                        response_text=response_text,
                        symbol=cache_symbol,
                        mode=llm_mode.value,
                        prompt_tokens=response.prompt_tokens,
                        completion_tokens=response.completion_tokens,
                        model=response.model,
                    )

                    # Show token usage in metrics
                    token_cols = st.columns(4)
                    with token_cols[0]:
                        st.metric("Prompt Tokens", f"{response.prompt_tokens:,}")
                    with token_cols[1]:
                        st.metric("Completion", f"{response.completion_tokens:,}")
                    with token_cols[2]:
                        st.metric("Total", f"{response.total_tokens:,}")
                    with token_cols[3]:
                        est_cost = (response.prompt_tokens * 0.01 + response.completion_tokens * 0.03) / 1000
                        st.metric("Est. Cost", f"${est_cost:.4f}")

            # Display response with enhanced styling
            st.markdown("---")

            # Custom CSS for AI Insights styling
            st.markdown("""
            <style>
            /* AI Insights Theme Styling */
            .ai-insights-container {
                background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
                border-radius: 12px;
                padding: 20px;
                margin: 10px 0;
            }

            /* Assessment Box Styling */
            .assessment-box {
                background: linear-gradient(135deg, #0f3460 0%, #16213e 100%);
                border-left: 4px solid #00d9ff;
                border-radius: 8px;
                padding: 16px 20px;
                margin-bottom: 20px;
                box-shadow: 0 4px 15px rgba(0, 217, 255, 0.1);
            }
            .assessment-bullish {
                border-left-color: #00ff88;
                box-shadow: 0 4px 15px rgba(0, 255, 136, 0.15);
            }
            .assessment-bearish {
                border-left-color: #ff4757;
                box-shadow: 0 4px 15px rgba(255, 71, 87, 0.15);
            }
            .assessment-neutral {
                border-left-color: #ffa502;
                box-shadow: 0 4px 15px rgba(255, 165, 2, 0.15);
            }

            /* Section Styling */
            .ai-section {
                background: rgba(255, 255, 255, 0.03);
                border-radius: 8px;
                padding: 16px;
                margin: 12px 0;
                border: 1px solid rgba(255, 255, 255, 0.08);
            }
            .ai-section h2, .ai-section h3 {
                color: #00d9ff;
                margin-top: 0;
            }

            /* Action Items Styling */
            .action-items {
                background: linear-gradient(135deg, #1e3a5f 0%, #16213e 100%);
                border-radius: 8px;
                padding: 16px;
                margin-top: 16px;
                border: 1px solid rgba(0, 217, 255, 0.2);
            }
            .action-items li {
                padding: 8px 0;
                border-bottom: 1px solid rgba(255, 255, 255, 0.05);
            }
            .action-items li:last-child {
                border-bottom: none;
            }

            /* Metrics Table Styling */
            .ai-insights-container table {
                width: 100%;
                border-collapse: collapse;
                margin: 12px 0;
            }
            .ai-insights-container th {
                background: rgba(0, 217, 255, 0.1);
                padding: 10px;
                text-align: left;
                border-bottom: 2px solid rgba(0, 217, 255, 0.3);
            }
            .ai-insights-container td {
                padding: 10px;
                border-bottom: 1px solid rgba(255, 255, 255, 0.05);
            }

            /* Blockquote Styling for Assessment */
            .ai-insights-container blockquote {
                background: linear-gradient(135deg, #0f3460 0%, #1a1a2e 100%);
                border-left: 4px solid #00d9ff;
                padding: 16px 20px;
                margin: 16px 0;
                border-radius: 0 8px 8px 0;
                font-size: 1.05em;
            }

            /* Disclaimer Styling */
            .ai-disclaimer {
                background: rgba(255, 165, 2, 0.1);
                border: 1px solid rgba(255, 165, 2, 0.3);
                border-radius: 8px;
                padding: 12px 16px;
                margin-top: 20px;
                font-size: 0.85em;
                color: #ffa502;
            }
            </style>
            """, unsafe_allow_html=True)

            # Display header with cache status
            header_cols = st.columns([3, 1])
            with header_cols[0]:
                st.markdown("### 🤖 AI Analysis")
            with header_cols[1]:
                if was_cached:
                    st.markdown("🔄 *Cached*")
                else:
                    st.markdown("✨ *Fresh*")

            # Wrap response in styled container
            st.markdown('<div class="ai-insights-container">', unsafe_allow_html=True)
            st.markdown(response_text)
            st.markdown('</div>', unsafe_allow_html=True)

            # Copy prompt button (in expander)
            with st.expander("🔧 Debug: View Full Prompt", expanded=False):
                st.text_area(
                    "Prompt sent to LLM",
                    value=prompt,
                    height=400,
                    disabled=True,
                )
                if st.button("📋 Copy Prompt"):
                    st.code(prompt)

            # Track the generation
            track_button_click(con, "AI Insights", f"Generate {insight_mode}")

        except ImportError as e:
            st.error(
                f"**LLM Module Import Error**\n\n"
                f"Could not import LLM modules: {e}\n\n"
                "Install missing dependencies: `pip install tabulate`"
            )

        except LLMError as e:
            st.error(f"**LLM Error**\n\n{e}")

        except Exception as e:
            st.error(f"**Error generating insight**\n\n{e}")
            import traceback
            with st.expander("Error Details"):
                st.code(traceback.format_exc())

    # =================================================================
    # CACHE MANAGEMENT (in sidebar or expander)
    # =================================================================
    st.markdown("---")
    st.markdown("### ⚙️ Settings & Cache")

    settings_cols = st.columns(2)

    with settings_cols[0]:
        with st.expander("💾 Cache Management", expanded=False):
            try:
                from psx_ohlcv.llm.cache import LLMCache, init_llm_cache_schema

                init_llm_cache_schema(con)
                cache = LLMCache(con)

                stats = cache.get_stats()

                cache_col1, cache_col2 = st.columns(2)
                with cache_col1:
                    st.metric("📦 Active", stats.get("active_entries", 0))
                    st.metric("⏰ Expired", stats.get("expired_entries", 0))
                with cache_col2:
                    total_tokens = stats.get("total_prompt_tokens", 0) + stats.get("total_completion_tokens", 0)
                    st.metric("🔢 Tokens Used", f"{total_tokens:,}")
                    est_savings = total_tokens * 0.00002  # rough estimate
                    st.metric("💰 Cache Savings", f"~${est_savings:.2f}")

                btn_col1, btn_col2 = st.columns(2)
                with btn_col1:
                    if st.button("🧹 Clear Expired", use_container_width=True):
                        cleared = cache.cleanup_expired()
                        st.success(f"Cleared {cleared} expired entries")
                with btn_col2:
                    if st.button("🗑️ Clear All", type="secondary", use_container_width=True):
                        cleared = cache.clear_all()
                        st.success(f"Cleared {cleared} entries")

            except Exception as e:
                st.warning(f"Cache management unavailable: {e}")

    with settings_cols[1]:
        with st.expander("💡 Cost Control Tips", expanded=False):
            st.markdown("""
            **🎯 Minimize API Costs:**

            | Tip | Impact |
            |-----|--------|
            | ✅ Use Caching | High |
            | 📅 Shorter time windows | Medium |
            | 📊 Fewer top movers | Low |
            | 🔄 Batch analysis | Medium |

            **📈 Token Estimates:**
            - Company (30d): ~2-3k tokens
            - Intraday: ~1.5-2.5k tokens
            - Market (10): ~2.5-3.5k tokens
            - History (90d): ~3-4.5k tokens

            *Cache TTL: 6 hours*
            """)

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

    # =========================================================================
    # EOD SYNC SECTION (Background Service)
    # =========================================================================
    st.subheader("📊 EOD Data Sync")

    # Check if EOD sync is running
    eod_running, eod_pid = is_eod_sync_running()
    eod_status = read_eod_status()

    # Show current status
    if eod_running:
        # Sync is running - show progress
        st.info(f"🔄 **EOD Sync Running** (PID: {eod_pid})")

        # Progress bar
        progress_pct = eod_status.progress / 100.0
        st.progress(progress_pct, text=eod_status.progress_message or "Syncing...")

        # Current symbol
        if eod_status.current_symbol:
            st.caption(f"Currently syncing: **{eod_status.current_symbol}**")

        # Stop button
        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            if st.button("🛑 Stop Sync", type="secondary", key="btn_stop_eod_sync"):
                success, msg = stop_eod_sync()
                if success:
                    st.success(msg)
                else:
                    st.error(msg)
                time.sleep(0.5)
                st.rerun()

        # Auto-refresh to show progress updates
        time.sleep(2)
        st.rerun()

    else:
        # Show last sync result if available
        if eod_status.completed_at:
            result_col1, result_col2, result_col3, result_col4 = st.columns(4)
            with result_col1:
                if eod_status.result == "success":
                    st.success("✅ Last sync: Success")
                elif eod_status.result == "partial":
                    st.warning("⚠️ Last sync: Partial")
                elif eod_status.result == "error":
                    st.error("❌ Last sync: Error")
                elif eod_status.result == "cancelled":
                    st.info("🚫 Last sync: Cancelled")
                else:
                    st.info("ℹ️ Last sync: Unknown")

            with result_col2:
                st.metric("Symbols OK", eod_status.symbols_ok)
            with result_col3:
                st.metric("Failed", eod_status.symbols_failed)
            with result_col4:
                st.metric("Rows", f"{eod_status.rows_upserted:,}")

            if eod_status.completed_at:
                st.caption(f"Completed: {eod_status.completed_at[:19].replace('T', ' ')}")
            if eod_status.error_message:
                st.error(f"Error: {eod_status.error_message}")

        # Sync options
        st.markdown("---")
        col1, col2 = st.columns([1, 1])

        with col1:
            refresh_symbols = st.checkbox(
                "Refresh symbols before sync",
                value=False,
                help="Fetch latest symbols from PSX market-watch before syncing",
            )
            incremental_mode = st.checkbox(
                "Incremental mode",
                value=True,
                help="Only fetch data newer than existing records (faster)",
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
            if st.button("▶️ Run Full Sync", type="primary", help="Start syncing EOD data for all symbols (runs in background)"):
                success, msg = start_eod_sync_background(
                    incremental=incremental_mode,
                    refresh_symbols=refresh_symbols,
                )
                if success:
                    st.success(f"🚀 {msg}")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error(msg)

        with col2:
            st.caption("Sync runs in background - you can navigate away")

    # =========================================================================
    # BULK INTRADAY SYNC SECTION
    # =========================================================================
    st.markdown("---")
    st.subheader("📈 Bulk Intraday Sync")

    # Initialize session state for intraday bulk sync
    if "intraday_bulk_result" not in st.session_state:
        st.session_state.intraday_bulk_result = None
    if "intraday_bulk_running" not in st.session_state:
        st.session_state.intraday_bulk_running = False
    if "sync_running" not in st.session_state:
        st.session_state.sync_running = False
    if "sync_result" not in st.session_state:
        st.session_state.sync_result = None

    # -------------------------------------------------------------------------
    # BACKGROUND SERVICE STATUS
    # -------------------------------------------------------------------------
    service_running, service_pid = is_service_running()
    service_status = read_service_status()

    # Service status display
    st.markdown("#### Background Service")
    if service_running:
        st.success(f"🟢 **Service Running** (PID: {service_pid})")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Mode", service_status.mode.capitalize())
        col2.metric("Interval", f"{service_status.interval_seconds}s")
        col3.metric("Total Runs", service_status.total_runs)
        col4.metric("Rows Synced", f"{service_status.rows_upserted:,}")

        # Progress info
        if service_status.current_symbol:
            st.info(f"📊 Currently syncing: **{service_status.current_symbol}** ({service_status.progress:.1f}%)")
        elif service_status.next_run_at:
            st.info(f"⏰ Next run at: {service_status.next_run_at}")

        # Last run result
        if service_status.last_run_at:
            result_icon = "✅" if service_status.last_run_result == "success" else "⚠️" if service_status.last_run_result == "partial" else "❌"
            st.caption(f"Last run: {service_status.last_run_at} - {result_icon} {service_status.symbols_synced} OK, {service_status.symbols_failed} failed")

        # Stop button
        if st.button("🛑 Stop Service", type="primary", key="btn_stop_service"):
            success, msg = stop_service()
            if success:
                st.success(msg)
                st.rerun()
            else:
                st.error(msg)
    else:
        st.warning("🔴 **Service Stopped**")

        # Service configuration
        col1, col2 = st.columns(2)
        with col1:
            service_mode = st.selectbox(
                "Sync Mode",
                options=["incremental", "full"],
                index=0,
                help="Incremental: only new data. Full: refresh all.",
                key="service_mode"
            )
            service_interval = st.number_input(
                "Interval (seconds)",
                min_value=60,
                max_value=3600,
                value=300,
                step=60,
                help="Time between sync runs (default: 300 = 5 minutes)",
                key="service_interval"
            )

        with col2:
            st.caption("CLI equivalent:")
            st.code(
                f"python -m psx_ohlcv.services.intraday_service start "
                f"--mode {service_mode} --interval {service_interval}",
                language="bash"
            )
            st.caption("Cron example (every 5 min during market hours):")
            st.code("*/5 9-15 * * 1-5 psxsync intraday sync-all -q", language="bash")

        # Start button
        if st.button("▶️ Start Background Service", type="primary", key="btn_start_service"):
            success, msg = start_service_background(
                mode=service_mode,
                interval_seconds=service_interval,
            )
            if success:
                st.success(msg)
                time.sleep(1)
                st.rerun()
            else:
                st.error(msg)

    st.markdown("---")

    # -------------------------------------------------------------------------
    # ONE-TIME SYNC (runs in Streamlit, not as background service)
    # -------------------------------------------------------------------------
    st.markdown("#### One-Time Sync")
    st.caption("Run a single sync operation (blocks UI until complete)")

    col1, col2 = st.columns([1, 1])

    with col1:
        intraday_incremental = st.checkbox(
            "Incremental mode (only new data)",
            value=True,
            help="Only fetch data newer than last sync (faster)",
            disabled=st.session_state.intraday_bulk_running,
            key="intraday_bulk_incremental"
        )
        intraday_limit = st.number_input(
            "Limit symbols (0 = all)",
            min_value=0,
            max_value=500,
            value=0,
            help="Limit number of symbols to sync (0 for all)",
            disabled=st.session_state.intraday_bulk_running,
            key="intraday_bulk_limit"
        )

    with col2:
        cli_flags = ""
        if not intraday_incremental:
            cli_flags += " --no-incremental"
        if intraday_limit > 0:
            cli_flags += f" --limit {intraday_limit}"
        st.caption("Equivalent CLI command:")
        st.code(f"psxsync intraday sync-all{cli_flags}", language="bash")

    # Bulk Intraday Sync Buttons
    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        run_intraday_full = st.button(
            "🔄 Full Sync" if not st.session_state.intraday_bulk_running else "⏳ Running...",
            disabled=st.session_state.intraday_bulk_running or st.session_state.sync_running,
            help="Sync all intraday data (full refresh)",
            key="btn_intraday_full"
        )
    with col2:
        run_intraday_incr = st.button(
            "⚡ Incremental" if not st.session_state.intraday_bulk_running else "⏳ Running...",
            disabled=st.session_state.intraday_bulk_running or st.session_state.sync_running,
            help="Only fetch new intraday data since last sync",
            key="btn_intraday_incr"
        )
    with col3:
        if st.session_state.intraday_bulk_running:
            st.warning("Sync in progress...")

    # Execute bulk intraday sync
    run_bulk_intraday = run_intraday_full or run_intraday_incr
    use_incremental = intraday_incremental if run_intraday_incr else False

    if run_bulk_intraday and not st.session_state.intraday_bulk_running:
        st.session_state.intraday_bulk_result = None
        st.session_state.intraday_bulk_running = True

        with st.status("Running bulk intraday sync...", expanded=True) as status:
            st.write("🔄 Initializing bulk intraday sync...")

            try:
                limit = intraday_limit if intraday_limit > 0 else None
                mode_str = "incremental" if use_incremental else "full"
                st.write(f"📊 Fetching intraday data ({mode_str} mode)...")

                # Progress container
                progress_bar = st.progress(0)
                progress_text = st.empty()

                def update_progress(current, total, symbol, result):
                    progress = current / total
                    progress_bar.progress(progress)
                    status_icon = "✅" if not result.error else "❌"
                    progress_text.text(f"{status_icon} {symbol}: {result.rows_upserted} rows ({current}/{total})")

                summary = sync_intraday_bulk(
                    db_path=get_db_path(),
                    incremental=use_incremental,
                    limit_symbols=limit,
                    progress_callback=update_progress,
                )

                st.session_state.intraday_bulk_result = {
                    "success": True,
                    "summary": summary,
                }

                if summary.symbols_failed == 0:
                    status.update(
                        label="✅ Bulk intraday sync completed!", state="complete"
                    )
                else:
                    status.update(
                        label=f"⚠️ Completed with {summary.symbols_failed} failures",
                        state="complete"
                    )

            except Exception as e:
                st.session_state.intraday_bulk_result = {
                    "success": False,
                    "error": str(e),
                }
                status.update(label="❌ Bulk intraday sync failed!", state="error")

            finally:
                st.session_state.intraday_bulk_running = False

    # Display bulk intraday sync result
    if st.session_state.intraday_bulk_result is not None:
        result = st.session_state.intraday_bulk_result

        if result["success"]:
            summary = result["summary"]

            if summary.symbols_failed == 0:
                st.success(
                    f"✅ Intraday sync completed: {summary.symbols_ok} symbols, "
                    f"{summary.rows_upserted:,} rows upserted"
                )
            else:
                st.warning(
                    f"⚠️ Intraday sync completed with issues: {summary.symbols_ok} OK, "
                    f"{summary.symbols_failed} failed"
                )

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Symbols", summary.symbols_total)
            col2.metric("Symbols OK", summary.symbols_ok)
            col3.metric("Symbols Failed", summary.symbols_failed)
            col4.metric("Rows Upserted", f"{summary.rows_upserted:,}")

            # Show failed symbols if any
            failed_results = [r for r in summary.results if r.error]
            if failed_results:
                with st.expander(f"🔍 View {len(failed_results)} failures"):
                    for r in failed_results[:20]:  # Limit display
                        st.text(f"{r.symbol}: {r.error}")
        else:
            st.error(f"❌ Intraday sync failed: {result['error']}")

    # =========================================================================
    # ANNOUNCEMENTS SYNC SECTION
    # =========================================================================
    st.markdown("---")
    st.subheader("📣 Announcements Sync")
    st.caption("Sync company announcements, AGM/EOGM calendar, and dividend payouts from PSX DPS")

    # Initialize session state
    if "announcements_sync_running" not in st.session_state:
        st.session_state.announcements_sync_running = False
    if "announcements_sync_result" not in st.session_state:
        st.session_state.announcements_sync_result = None

    # -------------------------------------------------------------------------
    # ANNOUNCEMENTS BACKGROUND SERVICE STATUS
    # -------------------------------------------------------------------------
    ann_running, ann_pid = is_announcements_running()
    ann_status = read_announcements_status()

    st.markdown("#### Background Service")
    if ann_running:
        st.success(f"🟢 **Announcements Service Running** (PID: {ann_pid})")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Interval", f"{ann_status.interval_seconds}s")
        col2.metric("Total Runs", ann_status.total_runs)
        col3.metric("Announcements", ann_status.announcements_synced)
        col4.metric("Dividends", ann_status.dividends_synced)

        if ann_status.current_task:
            st.info(f"🔄 Currently: {ann_status.current_task} - {ann_status.current_symbol or ''} ({ann_status.progress:.0f}%)")

        if ann_status.last_run_at:
            st.caption(f"Last run: {ann_status.last_run_at[:19]} | Result: {ann_status.last_run_result or 'N/A'}")

        if ann_status.next_run_at:
            st.caption(f"Next run: {ann_status.next_run_at[:19]}")

        # Stop button
        if st.button("⏹️ Stop Announcements Service", type="secondary", key="btn_stop_ann_service"):
            success, msg = stop_announcements_service()
            if success:
                st.success(msg)
                time.sleep(1)
                st.rerun()
            else:
                st.error(msg)
    else:
        st.info("🔴 Announcements service not running")

        col1, col2 = st.columns(2)
        with col1:
            ann_interval = st.number_input(
                "Interval (seconds)",
                min_value=300,
                max_value=7200,
                value=3600,
                step=300,
                help="Time between sync runs (default: 3600 = 1 hour)",
                key="ann_service_interval"
            )

        with col2:
            st.caption("CLI equivalent:")
            st.code(
                f"psxsync announcements service start --interval {ann_interval}",
                language="bash"
            )

        if st.button("▶️ Start Announcements Service", type="primary", key="btn_start_ann_service"):
            success, msg = start_announcements_service(interval_seconds=ann_interval)
            if success:
                st.success(msg)
                time.sleep(1)
                st.rerun()
            else:
                st.error(msg)

    st.markdown("---")

    # -------------------------------------------------------------------------
    # ONE-TIME ANNOUNCEMENTS SYNC
    # -------------------------------------------------------------------------
    st.markdown("#### One-Time Sync")
    st.caption("Run a single announcements sync (blocks UI until complete)")

    col1, col2, col3 = st.columns(3)
    with col1:
        sync_announcements_flag = st.checkbox("Company Announcements", value=True, key="sync_ann_flag")
    with col2:
        sync_events_flag = st.checkbox("Corporate Events (AGM/EOGM)", value=True, key="sync_events_flag")
    with col3:
        sync_dividends_flag = st.checkbox("Dividend Payouts", value=True, key="sync_dividends_flag")

    if st.button(
        "🔄 Sync Announcements Now" if not st.session_state.announcements_sync_running else "⏳ Syncing...",
        disabled=st.session_state.announcements_sync_running,
        type="primary",
        key="btn_sync_announcements"
    ):
        st.session_state.announcements_sync_running = True
        st.session_state.announcements_sync_result = None

        with st.status("Syncing announcements...", expanded=True) as status:
            try:
                from datetime import timedelta

                con = get_connection()
                stats = {"announcements": 0, "events": 0, "dividends": 0}

                # Sync announcements
                if sync_announcements_flag:
                    st.write("📣 Fetching company announcements...")
                    offset = 0
                    while True:
                        records, total = fetch_announcements(announcement_type="C", offset=offset)
                        if not records:
                            break
                        for record in records:
                            if save_announcement(con, record):
                                stats["announcements"] += 1
                        offset += len(records)
                        if offset >= total or len(records) < 20:
                            break
                    st.write(f"   ✅ {stats['announcements']} announcements saved")

                # Sync corporate events
                if sync_events_flag:
                    st.write("📅 Fetching corporate events...")
                    from_date = datetime.now().strftime("%Y-%m-%d")
                    to_date = (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d")
                    events = fetch_corporate_events(from_date, to_date)
                    for event in events:
                        if save_corporate_event(con, event):
                            stats["events"] += 1
                    st.write(f"   ✅ {stats['events']} events saved")

                # Sync dividends
                if sync_dividends_flag:
                    st.write("💰 Fetching dividend payouts...")
                    cur = con.execute("SELECT symbol FROM symbols WHERE is_active = 1")
                    symbols = [row[0] for row in cur.fetchall()]
                    progress_bar = st.progress(0)
                    for i, symbol in enumerate(symbols):
                        try:
                            payouts = fetch_company_payouts(symbol)
                            for payout in payouts:
                                if save_dividend_payout(con, payout):
                                    stats["dividends"] += 1
                        except Exception:
                            pass
                        progress_bar.progress((i + 1) / len(symbols))
                    st.write(f"   ✅ {stats['dividends']} payouts saved from {len(symbols)} symbols")

                st.session_state.announcements_sync_result = {"success": True, "stats": stats}
                status.update(label="✅ Announcements sync completed!", state="complete")

            except Exception as e:
                st.session_state.announcements_sync_result = {"success": False, "error": str(e)}
                status.update(label="❌ Sync failed!", state="error")

            finally:
                st.session_state.announcements_sync_running = False

    # Display sync result
    if st.session_state.announcements_sync_result is not None:
        result = st.session_state.announcements_sync_result
        if result["success"]:
            stats = result["stats"]
            col1, col2, col3 = st.columns(3)
            col1.metric("Announcements", stats["announcements"])
            col2.metric("Events", stats["events"])
            col3.metric("Dividends", stats["dividends"])
        else:
            st.error(f"❌ Sync failed: {result['error']}")

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


# =============================================================================
# Phase 1: Instruments Page
# =============================================================================
def instruments_page():
    """Browse and explore ETFs, REITs, and Indexes."""
    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 📦 Instruments Browser")
        st.caption("ETFs, REITs, and Indexes - Phase 1 Universe")
    with header_col2:
        render_market_status_badge()

    con = get_connection()

    # Instrument Type Filter
    col1, col2, col3 = st.columns([2, 2, 4])

    with col1:
        inst_type = st.selectbox(
            "Instrument Type",
            ["ALL", "ETF", "REIT", "INDEX"],
            index=0,
        )

    with col2:
        active_only = st.checkbox("Active Only", value=True)

    # Get instruments
    type_filter = None if inst_type == "ALL" else inst_type
    instruments = get_instruments(con, instrument_type=type_filter, active_only=active_only)

    if not instruments:
        st.warning("No instruments found. Run `psxsync universe seed-phase1` to seed the instrument universe.")
        render_footer()
        return

    # Summary metrics
    etf_count = len([i for i in instruments if i.get("instrument_type") == "ETF"])
    reit_count = len([i for i in instruments if i.get("instrument_type") == "REIT"])
    index_count = len([i for i in instruments if i.get("instrument_type") == "INDEX"])

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Instruments", len(instruments))
    with col2:
        st.metric("ETFs", etf_count)
    with col3:
        st.metric("REITs", reit_count)
    with col4:
        st.metric("Indexes", index_count)

    st.markdown("---")

    # Instrument Table
    st.subheader("Instrument List")

    # Convert to DataFrame for display
    df = pd.DataFrame(instruments)
    display_cols = ["symbol", "name", "instrument_type", "source", "is_active"]
    if all(col in df.columns for col in display_cols):
        display_df = df[display_cols].copy()
        display_df.columns = ["Symbol", "Name", "Type", "Source", "Active"]
        display_df["Active"] = display_df["Active"].apply(lambda x: "✓" if x else "✗")
        st.dataframe(display_df, use_container_width=True, hide_index=True)

    st.markdown("---")

    # Individual Instrument Viewer
    st.subheader("Instrument Detail")

    symbol_list = [inst["symbol"] for inst in instruments]
    selected_symbol = st.selectbox("Select Instrument", symbol_list)

    if selected_symbol:
        # Find the instrument
        selected_inst = next((i for i in instruments if i["symbol"] == selected_symbol), None)

        if selected_inst:
            col1, col2 = st.columns([1, 2])

            with col1:
                st.markdown(f"**Symbol:** {selected_inst['symbol']}")
                st.markdown(f"**Name:** {selected_inst.get('name', 'N/A')}")
                st.markdown(f"**Type:** {selected_inst.get('instrument_type', 'N/A')}")
                st.markdown(f"**Source:** {selected_inst.get('source', 'N/A')}")

                # Compute metrics if available
                instrument_id = selected_inst.get("instrument_id")
                if instrument_id:
                    metrics = compute_all_metrics(con, instrument_id)
                    if "error" not in metrics:
                        st.markdown("**Performance Metrics:**")
                        if metrics.get("return_1m"):
                            ret_color = "green" if metrics["return_1m"] > 0 else "red"
                            st.markdown(f"- 1M Return: :{ret_color}[{metrics['return_1m']:.2f}%]")
                        if metrics.get("return_3m"):
                            ret_color = "green" if metrics["return_3m"] > 0 else "red"
                            st.markdown(f"- 3M Return: :{ret_color}[{metrics['return_3m']:.2f}%]")
                        if metrics.get("vol_1m"):
                            st.markdown(f"- 30D Volatility: {metrics['vol_1m']:.2f}%")

            with col2:
                # Get OHLCV data for chart
                # Try eod_ohlcv first, fall back to ohlcv_instruments
                symbol = selected_inst.get("symbol")
                instrument_id = selected_inst.get("instrument_id")

                ohlcv_df = None
                # Try eod_ohlcv first (equities, ETFs, REITs via psxsync eod)
                if symbol:
                    ohlcv_df = get_eod_ohlcv(con, symbol=symbol, limit=90)

                # Fall back to ohlcv_instruments (indices, legacy sync)
                if (ohlcv_df is None or ohlcv_df.empty) and instrument_id:
                    ohlcv_df = get_ohlcv_instrument(con, instrument_id, limit=90)

                if ohlcv_df is not None and not ohlcv_df.empty:
                    ohlcv_df = ohlcv_df.sort_values("date")
                    fig = make_price_line(
                        ohlcv_df,
                        date_col="date",
                        price_col="close",
                        title=f"{selected_symbol} - Last 90 Days"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info(f"No OHLCV data available. Run `psxsync eod {symbol}` to sync data.")

    # Sync Section
    st.markdown("---")
    with st.expander("Sync Instrument Data", expanded=False):
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            if st.button("Seed Universe", type="secondary", key="inst_seed"):
                with st.spinner("Seeding instrument universe..."):
                    from psx_ohlcv.sources.instrument_universe import seed_universe
                    result = seed_universe(get_cached_connection())
                    totals = result.get('totals', {})
                    st.success(
                        f"Seeded {totals.get('inserted', 0)} instruments "
                        f"(Failed: {totals.get('failed', 0)})"
                    )
                    st.rerun()

        with col2:
            sync_types = st.multiselect(
                "Types to Sync",
                ["ETF", "REIT", "INDEX"],
                default=["ETF", "REIT", "INDEX"],
                key="inst_sync_types"
            )

        with col3:
            incremental = st.checkbox("Incremental", value=True, key="inst_incr")

        with col4:
            if st.button("Sync OHLCV", type="primary", key="inst_sync"):
                if sync_types:
                    with st.spinner(f"Syncing {', '.join(sync_types)}..."):
                        summary = sync_instruments_eod(
                            db_path=get_db_path(),
                            instrument_types=sync_types,
                            incremental=incremental,
                        )
                        st.success(
                            f"Sync complete: {summary.ok} OK, "
                            f"{summary.failed} failed, "
                            f"{summary.rows_upserted} rows"
                        )
                        st.rerun()
                else:
                    st.warning("Select at least one instrument type to sync.")

    render_footer()


# =============================================================================
# Phase 1: Rankings Page
# =============================================================================
def rankings_page():
    """View and compare instrument performance rankings."""
    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 🏆 Instrument Rankings")
        st.caption("Performance comparison for ETFs, REITs, and Indexes")
    with header_col2:
        render_market_status_badge()

    con = get_connection()

    # Filter options
    col1, col2, col3 = st.columns([2, 2, 2])

    with col1:
        types_filter = st.multiselect(
            "Instrument Types",
            ["ETF", "REIT", "INDEX"],
            default=["ETF", "REIT", "INDEX"]
        )

    with col2:
        top_n = st.slider("Top N", min_value=5, max_value=50, value=10)

    with col3:
        compute_btn = st.button("Refresh Rankings", type="primary")

    if compute_btn and types_filter:
        with st.spinner("Computing rankings..."):
            result = compute_rankings(
                con,
                as_of_date=None,  # Today
                instrument_types=types_filter,
                top_n=top_n,
            )
            if result.get("success"):
                st.success(f"Computed rankings for {result.get('instruments_ranked', 0)} instruments")
            else:
                st.error(f"Error: {result.get('error', 'Unknown error')}")

    st.markdown("---")

    # Get rankings
    rankings = get_rankings(
        con,
        as_of_date=None,  # Most recent
        instrument_types=types_filter if types_filter else None,
        top_n=top_n,
    )

    if not rankings:
        st.info(
            "No rankings found. Click 'Refresh Rankings' to compute, "
            "or ensure instrument data is synced first."
        )
        render_footer()
        return

    # Rankings Table
    st.subheader("Performance Rankings")

    # Convert to DataFrame
    rankings_df = pd.DataFrame(rankings)

    # Format for display
    display_cols = ["symbol", "name", "instrument_type", "return_1m", "return_3m", "return_6m", "return_1y", "volatility_30d"]
    available_cols = [c for c in display_cols if c in rankings_df.columns]
    display_df = rankings_df[available_cols].copy()

    # Add rank column
    display_df.insert(0, "Rank", range(1, len(display_df) + 1))

    # Format percentages
    pct_cols = ["return_1m", "return_3m", "return_6m", "return_1y", "volatility_30d"]
    for col in pct_cols:
        if col in display_df.columns:
            display_df[col] = display_df[col].apply(
                lambda x: f"{x * 100:.1f}%" if pd.notna(x) else "N/A"
            )

    # Rename columns
    col_names = {
        "symbol": "Symbol",
        "name": "Name",
        "instrument_type": "Type",
        "return_1m": "1M",
        "return_3m": "3M",
        "return_6m": "6M",
        "return_1y": "1Y",
        "volatility_30d": "Vol 30D",
    }
    display_df.rename(columns=col_names, inplace=True)

    st.dataframe(display_df, use_container_width=True, hide_index=True)

    st.markdown("---")

    # Performance Comparison Chart
    st.subheader("Performance Comparison (Normalized)")

    if len(rankings) >= 2:
        # Let user select instruments to compare
        symbols = [r.get("symbol") for r in rankings if r.get("symbol")]

        if symbols:
            selected_symbols = st.multiselect(
                "Select instruments to compare (up to 5)",
                symbols,
                default=symbols[:3] if len(symbols) >= 3 else symbols,
                max_selections=5,
            )

            if selected_symbols:
                # Get instrument IDs for selected symbols
                selected_ids = [
                    r.get("instrument_id")
                    for r in rankings
                    if r.get("symbol") in selected_symbols
                ]

                # Get normalized performance
                perf_df = get_normalized_performance(con, selected_ids)

                if not perf_df.empty:
                    import plotly.graph_objects as go

                    fig = go.Figure()

                    # Map instrument IDs to symbols for legend
                    id_to_symbol = {
                        r.get("instrument_id"): r.get("symbol")
                        for r in rankings
                    }

                    for col in perf_df.columns:
                        symbol = id_to_symbol.get(col, col)
                        fig.add_trace(go.Scatter(
                            x=perf_df.index,
                            y=perf_df[col],
                            mode="lines",
                            name=symbol,
                            hovertemplate=f"{symbol}: %{{y:.1f}}<extra></extra>"
                        ))

                    fig.update_layout(
                        title="Normalized Performance (Base = 100)",
                        xaxis_title="Date",
                        yaxis_title="Value",
                        legend=dict(orientation="h", yanchor="bottom", y=1.02),
                        height=400,
                    )

                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No historical data available for selected instruments.")

    # Summary Statistics
    st.markdown("---")
    st.subheader("Summary Statistics")

    col1, col2, col3 = st.columns(3)

    # Calculate summary stats
    if "return_1m" in rankings_df.columns:
        valid_returns = rankings_df["return_1m"].dropna()
        if len(valid_returns) > 0:
            with col1:
                best = rankings_df.loc[rankings_df["return_1m"].idxmax()]
                st.metric(
                    "Best 1M Return",
                    f"{best.get('symbol', 'N/A')}",
                    f"{best.get('return_1m', 0) * 100:.1f}%"
                )

            with col2:
                worst = rankings_df.loc[rankings_df["return_1m"].idxmin()]
                st.metric(
                    "Worst 1M Return",
                    f"{worst.get('symbol', 'N/A')}",
                    f"{worst.get('return_1m', 0) * 100:.1f}%"
                )

            with col3:
                avg_return = valid_returns.mean() * 100
                st.metric("Avg 1M Return", f"{avg_return:.1f}%")

    # Info about syncing
    st.markdown("---")
    st.info("💡 To seed or sync instrument data, use the **📦 Instruments** page.")

    render_footer()


# =============================================================================
# Phase 1: Index Analytics Page
# =============================================================================
def indices_analytics_page():
    """Comprehensive Index Analytics - All PSX indices with KPIs."""
    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 📊 Index Analytics")
        st.caption("PSX Market Indices - Performance & Trends")
    with header_col2:
        render_market_status_badge()

    con = get_connection()

    # Get all index instruments
    indices = get_instruments(con, instrument_type="INDEX", active_only=True)

    if not indices:
        st.warning("No indices found. Run `psxsync universe seed-phase1` to seed indices.")
        render_footer()
        return

    # =================================================================
    # INDEX OVERVIEW TABLE
    # =================================================================
    st.markdown("---")
    st.subheader("📈 All Indices Performance")

    # Compute metrics for all indices
    all_metrics = []
    for idx in indices:
        metrics = compute_all_metrics(con, idx["instrument_id"])
        if "error" not in metrics:
            metrics["symbol"] = idx["symbol"]
            metrics["name"] = idx.get("name", idx["symbol"])
            metrics["instrument_id"] = idx["instrument_id"]
            all_metrics.append(metrics)

    if all_metrics:
        metrics_df = pd.DataFrame(all_metrics)

        # Get latest close prices
        for i, row in metrics_df.iterrows():
            ohlcv = get_ohlcv_instrument(con, row["instrument_id"], limit=2)
            if not ohlcv.empty:
                latest = ohlcv.sort_values("date", ascending=False).iloc[0]
                metrics_df.at[i, "close"] = latest.get("close", 0)
                if len(ohlcv) > 1:
                    prev = ohlcv.sort_values("date", ascending=False).iloc[1]
                    prev_close = prev.get("close", 0)
                    if prev_close:
                        change_pct = ((latest.get("close", 0) - prev_close) / prev_close) * 100
                        metrics_df.at[i, "change_1d"] = change_pct

        # Display columns
        display_cols = ["symbol", "name", "close", "change_1d", "return_1w", "return_1m", "return_3m", "vol_1m"]
        available_cols = [c for c in display_cols if c in metrics_df.columns]
        display_df = metrics_df[available_cols].copy()

        # Format for display
        if "close" in display_df.columns:
            display_df["close"] = display_df["close"].apply(
                lambda x: f"{x:,.2f}" if pd.notna(x) and x else "N/A"
            )

        pct_cols = ["change_1d", "return_1w", "return_1m", "return_3m", "vol_1m"]
        for col in pct_cols:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(
                    lambda x: f"{x:+.2f}%" if pd.notna(x) else "N/A"
                )

        col_names = {
            "symbol": "Symbol",
            "name": "Name",
            "close": "Last",
            "change_1d": "1D %",
            "return_1w": "1W %",
            "return_1m": "1M %",
            "return_3m": "3M %",
            "vol_1m": "Vol 30D",
        }
        display_df.rename(columns=col_names, inplace=True)

        st.dataframe(display_df, use_container_width=True, hide_index=True)

    # =================================================================
    # KPI SUMMARY
    # =================================================================
    st.markdown("---")
    st.subheader("📊 Market Summary")

    if all_metrics:
        # Calculate summary stats
        valid_1m = [m.get("return_1m") for m in all_metrics if m.get("return_1m") is not None]
        valid_1d = [m.get("change_1d") for m in all_metrics if m.get("change_1d") is not None] if "change_1d" in metrics_df.columns else []

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            gainers = len([r for r in valid_1d if r > 0]) if valid_1d else 0
            st.metric("📈 Gainers (1D)", gainers)

        with col2:
            losers = len([r for r in valid_1d if r < 0]) if valid_1d else 0
            st.metric("📉 Losers (1D)", losers)

        with col3:
            if valid_1m:
                avg_1m = sum(valid_1m) / len(valid_1m)
                st.metric("Avg 1M Return", f"{avg_1m:+.2f}%")
            else:
                st.metric("Avg 1M Return", "N/A")

        with col4:
            st.metric("Total Indices", len(indices))

    # =================================================================
    # INDIVIDUAL INDEX DETAIL
    # =================================================================
    st.markdown("---")
    st.subheader("🔍 Index Detail")

    symbol_list = [idx["symbol"] for idx in indices]
    name_map = {idx["symbol"]: idx.get("name", idx["symbol"]) for idx in indices}
    id_map = {idx["symbol"]: idx.get("instrument_id") for idx in indices}

    col1, col2 = st.columns([2, 2])

    with col1:
        selected_symbol = st.selectbox(
            "Select Index",
            symbol_list,
            format_func=lambda x: f"{x} - {name_map.get(x, x)}"
        )

    with col2:
        date_range = st.selectbox(
            "Time Range",
            ["30 Days", "90 Days", "180 Days", "1 Year"],
            index=1
        )

    # Map range to limit
    range_map = {"30 Days": 30, "90 Days": 90, "180 Days": 180, "1 Year": 365}
    limit = range_map.get(date_range, 90)

    if selected_symbol:
        instrument_id = id_map.get(selected_symbol)

        # Get metrics for selected index
        metrics = compute_all_metrics(con, instrument_id)

        # KPI row for selected index
        st.markdown("#### Performance Metrics")
        col1, col2, col3, col4, col5 = st.columns(5)

        with col1:
            ohlcv = get_ohlcv_instrument(con, instrument_id, limit=2)
            if not ohlcv.empty:
                latest = ohlcv.sort_values("date", ascending=False).iloc[0]
                st.metric("Current Value", f"{latest.get('close', 0):,.2f}")
            else:
                st.metric("Current Value", "N/A")

        with col2:
            if not ohlcv.empty and len(ohlcv) > 1:
                prev = ohlcv.sort_values("date", ascending=False).iloc[1]
                change = latest.get("close", 0) - prev.get("close", 0)
                pct = (change / prev.get("close", 1)) * 100 if prev.get("close") else 0
                st.metric("1D Change", f"{change:+,.2f}", f"{pct:+.2f}%")
            else:
                st.metric("1D Change", "N/A")

        with col3:
            ret_1w = metrics.get("return_1w")
            if ret_1w is not None:
                st.metric("1W Return", f"{ret_1w:+.2f}%")
            else:
                st.metric("1W Return", "N/A")

        with col4:
            ret_1m = metrics.get("return_1m")
            if ret_1m is not None:
                st.metric("1M Return", f"{ret_1m:+.2f}%")
            else:
                st.metric("1M Return", "N/A")

        with col5:
            vol = metrics.get("vol_1m")
            if vol is not None:
                st.metric("30D Volatility", f"{vol:.2f}%")
            else:
                st.metric("30D Volatility", "N/A")

        # Chart
        st.markdown("#### Price Chart")
        ohlcv_df = get_ohlcv_instrument(con, instrument_id, limit=limit)

        if not ohlcv_df.empty:
            ohlcv_df = ohlcv_df.sort_values("date")

            # Use candlestick if all OHLC columns exist, else line chart
            has_ohlc = all(c in ohlcv_df.columns for c in ["open", "high", "low", "close"])

            if has_ohlc and len(ohlcv_df) >= 5:
                fig = make_candlestick(
                    ohlcv_df,
                    title=f"{selected_symbol} - {name_map.get(selected_symbol, '')}",
                    date_col="date",
                    show_sma=True,
                )
            else:
                fig = make_price_line(
                    ohlcv_df,
                    title=f"{selected_symbol} - {name_map.get(selected_symbol, '')}",
                    date_col="date",
                    price_col="close",
                    height=400,
                )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No OHLCV data available. Run `psxsync instruments sync-eod` to sync index data.")

    # =================================================================
    # MULTI-INDEX COMPARISON
    # =================================================================
    st.markdown("---")
    st.subheader("📊 Index Comparison")

    compare_symbols = st.multiselect(
        "Select indices to compare (up to 6)",
        symbol_list,
        default=symbol_list[:4] if len(symbol_list) >= 4 else symbol_list,
        max_selections=6,
    )

    if compare_symbols and len(compare_symbols) >= 2:
        # Get normalized performance
        compare_ids = [id_map.get(s) for s in compare_symbols if id_map.get(s)]

        perf_df = get_normalized_performance(con, compare_ids)

        if not perf_df.empty:
            import plotly.graph_objects as go

            fig = go.Figure()

            # Map IDs back to symbols
            id_to_symbol = {v: k for k, v in id_map.items()}

            for col in perf_df.columns:
                symbol = id_to_symbol.get(col, col)
                fig.add_trace(go.Scatter(
                    x=perf_df.index,
                    y=perf_df[col],
                    mode="lines",
                    name=symbol,
                    hovertemplate=f"{symbol}: %{{y:.1f}}<extra></extra>"
                ))

            fig.update_layout(
                title="Normalized Performance (Base = 100)",
                xaxis_title="Date",
                yaxis_title="Value",
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
                height=400,
            )

            # Apply theme
            from psx_ohlcv.ui.charts import apply_bloomberg_layout
            apply_bloomberg_layout(fig)

            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No historical data available for comparison.")

    # =================================================================
    # CATEGORY VIEW
    # =================================================================
    st.markdown("---")
    st.subheader("📋 Indices by Category")

    # Group indices by category
    categories = {
        "Main Indices": ["KSE100", "ALLSHR", "KSE30", "KMI30"],
        "Islamic Indices": ["KMIALLSHR", "MII30"],
        "Sector Indices": ["BKTI", "OGTI"],
        "Thematic Indices": ["PSXDIV20", "UPP9", "KSE100PR"],
        "ETF Tracking Indices": ["NITPGI", "NBPPGI", "MZNPI", "JSMFI", "ACI", "JSGBKTI", "HBLTTI"],
    }

    for cat_name, cat_symbols in categories.items():
        # Get indices in this category
        cat_indices = [m for m in all_metrics if m.get("symbol") in cat_symbols]

        if cat_indices:
            with st.expander(f"{cat_name} ({len(cat_indices)} indices)", expanded=False):
                cat_df = pd.DataFrame(cat_indices)
                display_cols = ["symbol", "name", "return_1w", "return_1m", "return_3m", "vol_1m"]
                available_cols = [c for c in display_cols if c in cat_df.columns]
                display_df = cat_df[available_cols].copy()

                pct_cols = ["return_1w", "return_1m", "return_3m", "vol_1m"]
                for col in pct_cols:
                    if col in display_df.columns:
                        display_df[col] = display_df[col].apply(
                            lambda x: f"{x:+.2f}%" if pd.notna(x) else "N/A"
                        )

                display_df.rename(columns={
                    "symbol": "Symbol",
                    "name": "Name",
                    "return_1w": "1W",
                    "return_1m": "1M",
                    "return_3m": "3M",
                    "vol_1m": "Vol",
                }, inplace=True)

                st.dataframe(display_df, use_container_width=True, hide_index=True)

    # =================================================================
    # SYNC SECTION
    # =================================================================
    st.markdown("---")
    with st.expander("Sync Index Data", expanded=False):
        col1, col2 = st.columns([2, 4])

        with col1:
            if st.button("Sync Index OHLCV", type="primary", key="idx_sync"):
                with st.spinner("Syncing index OHLCV..."):
                    summary = sync_instruments_eod(
                        db_path=get_db_path(),
                        instrument_types=["INDEX"],
                        incremental=True,
                    )
                    st.success(
                        f"Sync: {summary.ok} OK, {summary.rows_upserted} rows"
                    )
                    st.rerun()

        with col2:
            st.caption("To seed instruments, use the **📦 Instruments** page.")

    render_footer()


# =============================================================================
# Phase 2: FX Overview Page
# =============================================================================
def fx_overview_page():
    """FX Overview - Macro context for currency analysis."""
    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 🌍 FX Overview")
        st.caption("Foreign Exchange Analytics - Macro Context (Read-Only)")
    with header_col2:
        st.markdown(
            '<div class="data-info">📊 Macro Context Only</div>',
            unsafe_allow_html=True
        )

    con = get_connection()

    # Get FX pairs
    fx_pairs = get_fx_pairs(con, active_only=True)

    if not fx_pairs:
        st.warning(
            "No FX pairs found. Run `psxsync fx seed` to seed FX pairs, "
            "then `psxsync fx sync` to fetch data."
        )
        render_footer()
        return

    # Pair selector
    col1, col2 = st.columns([2, 4])

    with col1:
        pair_names = [p["pair"] for p in fx_pairs]
        selected_pair = st.selectbox(
            "Select Currency Pair",
            pair_names,
            index=0 if "USD/PKR" in pair_names else 0,
        )

    # Get analytics for selected pair
    analytics = get_fx_analytics(con, selected_pair)

    if analytics.get("error"):
        st.info(
            f"No data available for {selected_pair}. "
            "Run `psxsync fx sync` to fetch data."
        )
        render_footer()
        return

    # Key metrics
    st.markdown("---")
    st.subheader("Current Rate & Returns")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        latest_rate = analytics.get("latest_close", 0)
        st.metric("Latest Rate", f"{latest_rate:.4f}")

    with col2:
        ret_1w = analytics.get("return_1W", 0) or 0
        st.metric(
            "1 Week",
            f"{ret_1w * 100:+.2f}%",
            delta=f"{ret_1w * 100:+.2f}%",
            delta_color="inverse"  # Red for appreciation (bad for PKR)
        )

    with col3:
        ret_1m = analytics.get("return_1M", 0) or 0
        st.metric(
            "1 Month",
            f"{ret_1m * 100:+.2f}%",
            delta=f"{ret_1m * 100:+.2f}%",
            delta_color="inverse"
        )

    with col4:
        ret_3m = analytics.get("return_3M", 0) or 0
        st.metric(
            "3 Month",
            f"{ret_3m * 100:+.2f}%",
            delta=f"{ret_3m * 100:+.2f}%",
            delta_color="inverse"
        )

    # Trend info
    trend = analytics.get("trend", {})
    if trend:
        col1, col2 = st.columns(2)
        with col1:
            direction = trend.get("trend_direction", "N/A").upper()
            strength = trend.get("trend_strength", "N/A")
            if direction == "UP":
                st.warning(f"📈 Trend: {direction} ({strength}) - PKR Depreciating")
            else:
                st.success(f"📉 Trend: {direction} ({strength}) - PKR Appreciating")

        with col2:
            vol_1m = analytics.get("vol_1M", 0) or 0
            st.metric("30D Volatility", f"{vol_1m * 100:.2f}%")

    # FX Chart
    st.markdown("---")
    st.subheader("FX Rate Chart")

    # Date range selector
    date_range = st.selectbox(
        "Time Range",
        ["30 Days", "90 Days", "180 Days", "1 Year"],
        index=1,
    )

    days_map = {"30 Days": 30, "90 Days": 90, "180 Days": 180, "1 Year": 365}
    days = days_map[date_range]

    # Get OHLCV data
    df = get_fx_ohlcv(con, selected_pair, limit=days)

    if not df.empty:
        df = df.sort_values("date")

        import plotly.graph_objects as go

        # Create candlestick chart
        fig = go.Figure()

        if len(df) <= 60:
            # Candlestick for shorter periods
            fig.add_trace(go.Candlestick(
                x=df["date"],
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"],
                name=selected_pair,
            ))
        else:
            # Line chart for longer periods
            fig.add_trace(go.Scatter(
                x=df["date"],
                y=df["close"],
                mode="lines",
                name=selected_pair,
                line=dict(color="#2196F3", width=2),
            ))

            # Add 50-day MA
            if len(df) >= 50:
                df["ma50"] = df["close"].rolling(window=50).mean()
                fig.add_trace(go.Scatter(
                    x=df["date"],
                    y=df["ma50"],
                    mode="lines",
                    name="50D MA",
                    line=dict(color="#FFC107", width=1, dash="dash"),
                ))

        fig.update_layout(
            title=f"{selected_pair} - {date_range}",
            xaxis_title="Date",
            yaxis_title="Rate",
            height=400,
            xaxis_rangeslider_visible=False,
        )

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No chart data available.")

    # Multi-pair comparison
    st.markdown("---")
    st.subheader("Multi-Pair Comparison")

    if len(pair_names) >= 2:
        compare_pairs = st.multiselect(
            "Select pairs to compare",
            pair_names,
            default=pair_names[:3] if len(pair_names) >= 3 else pair_names,
            max_selections=5,
        )

        if compare_pairs:
            perf_df = get_normalized_fx_performance(con, compare_pairs)

            if not perf_df.empty:
                import plotly.graph_objects as go

                fig = go.Figure()

                for pair in compare_pairs:
                    if pair in perf_df.columns:
                        fig.add_trace(go.Scatter(
                            x=perf_df.index,
                            y=perf_df[pair],
                            mode="lines",
                            name=pair,
                        ))

                fig.update_layout(
                    title="Normalized Performance (Base = 100)",
                    xaxis_title="Date",
                    yaxis_title="Value",
                    height=350,
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )

                st.plotly_chart(fig, use_container_width=True)

    # Sync section
    st.markdown("---")
    st.subheader("Sync FX Data")

    col1, col2 = st.columns(2)

    with col1:
        if st.button("Sync FX Rates", type="primary"):
            with st.spinner("Syncing FX data..."):
                summary = sync_fx_pairs(db_path=get_db_path())
                st.success(
                    f"Sync complete: {summary.ok} OK, "
                    f"{summary.rows_upserted} rows"
                )

    with col2:
        if st.button("Seed FX Pairs"):
            result = seed_fx_pairs(db_path=get_db_path())
            st.success(f"Seeded {result.get('inserted', 0)} pairs")

    render_footer()


# =============================================================================
# Phase 2: FX Impact Page
# =============================================================================
def fx_impact_page():
    """FX Impact - FX-adjusted equity performance analysis."""
    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 📊 FX Impact")
        st.caption("FX-Adjusted Equity Performance (Read-Only Analytics)")
    with header_col2:
        st.markdown(
            '<div class="data-info">📈 Analytics Only</div>',
            unsafe_allow_html=True
        )

    con = get_connection()

    # Get FX pairs
    fx_pairs = get_fx_pairs(con, active_only=True)

    if not fx_pairs:
        st.warning("No FX pairs found. Run `psxsync fx seed` first.")
        render_footer()
        return

    # Filters
    col1, col2, col3 = st.columns(3)

    with col1:
        pair_names = [p["pair"] for p in fx_pairs]
        default_idx = pair_names.index("USD/PKR") if "USD/PKR" in pair_names else 0
        selected_pair = st.selectbox(
            "FX Pair for Adjustment",
            pair_names,
            index=default_idx,
        )

    with col2:
        period = st.selectbox(
            "Return Period",
            ["1W", "1M", "3M"],
            index=1,
        )

    with col3:
        top_n = st.slider("Top N Stocks", min_value=10, max_value=50, value=20)

    # Get FX analytics for context
    fx_analytics = get_fx_analytics(con, selected_pair)

    # Show FX context
    st.markdown("---")
    st.subheader(f"{selected_pair} Context")

    col1, col2, col3 = st.columns(3)

    with col1:
        fx_return = fx_analytics.get(f"return_{period}", 0) or 0
        st.metric(
            f"FX Return ({period})",
            f"{fx_return * 100:+.2f}%",
            help="Positive = PKR depreciation"
        )

    with col2:
        latest = fx_analytics.get("latest_close", 0)
        st.metric("Latest Rate", f"{latest:.2f}")

    with col3:
        vol = fx_analytics.get("vol_1M", 0) or 0
        st.metric("FX Volatility", f"{vol * 100:.1f}%")

    # Explanation
    st.markdown("""
    **How FX-Adjusted Returns Work:**
    - FX-Adjusted Return = Equity Return - FX Return
    - If PKR depreciates by 2% and stock rises 5%, the USD-adjusted return is 3%
    - This helps compare PSX returns with global benchmarks
    """)

    # Get FX-adjusted metrics
    st.markdown("---")
    st.subheader("FX-Adjusted Performance")

    metrics = get_fx_adjusted_metrics(
        con,
        fx_pair=selected_pair,
        period=period,
        limit=top_n,
    )

    if not metrics:
        st.info(
            "No FX-adjusted metrics available. "
            "Run `psxsync fx compute-adjusted` to compute."
        )

        if st.button("Compute FX-Adjusted Metrics", type="primary"):
            with st.spinner("Computing metrics..."):
                result = compute_and_store_fx_adjusted_metrics(
                    con,
                    fx_pair=selected_pair,
                )
                if result.get("success"):
                    st.success(f"Computed {result.get('metrics_stored', 0)} metrics")
                    st.rerun()
                else:
                    st.error(f"Error: {result.get('error')}")
    else:
        # Convert to DataFrame
        df = pd.DataFrame(metrics)

        # Format for display
        display_df = df[["symbol", "equity_return", "fx_return", "fx_adjusted_return"]].copy()
        display_df["equity_return"] = display_df["equity_return"].apply(
            lambda x: f"{x * 100:.2f}%" if pd.notna(x) else "N/A"
        )
        display_df["fx_return"] = display_df["fx_return"].apply(
            lambda x: f"{x * 100:.2f}%" if pd.notna(x) else "N/A"
        )
        display_df["fx_adjusted_return"] = display_df["fx_adjusted_return"].apply(
            lambda x: f"{x * 100:.2f}%" if pd.notna(x) else "N/A"
        )

        display_df.columns = ["Symbol", f"Equity ({period})", f"FX ({period})", "Adjusted"]

        st.dataframe(display_df, use_container_width=True, hide_index=True)

        # Visualization
        st.markdown("---")
        st.subheader("Visual Comparison")

        # Select stocks to visualize
        symbols = df["symbol"].tolist()
        selected_symbols = st.multiselect(
            "Select stocks to compare",
            symbols,
            default=symbols[:5] if len(symbols) >= 5 else symbols,
            max_selections=10,
        )

        if selected_symbols:
            import plotly.graph_objects as go

            filtered_df = df[df["symbol"].isin(selected_symbols)]

            fig = go.Figure()

            # Equity returns
            fig.add_trace(go.Bar(
                name=f"Equity Return ({period})",
                x=filtered_df["symbol"],
                y=filtered_df["equity_return"] * 100,
                marker_color="#2196F3",
            ))

            # FX-adjusted returns
            fig.add_trace(go.Bar(
                name="FX-Adjusted Return",
                x=filtered_df["symbol"],
                y=filtered_df["fx_adjusted_return"] * 100,
                marker_color="#4CAF50",
            ))

            fig.update_layout(
                title=f"Equity vs FX-Adjusted Returns ({period})",
                xaxis_title="Symbol",
                yaxis_title="Return (%)",
                barmode="group",
                height=400,
            )

            st.plotly_chart(fig, use_container_width=True)

        # Summary stats
        st.markdown("---")
        st.subheader("Summary Statistics")

        col1, col2, col3 = st.columns(3)

        valid_adj = df["fx_adjusted_return"].dropna()

        if len(valid_adj) > 0:
            with col1:
                best = df.loc[df["fx_adjusted_return"].idxmax()]
                st.metric(
                    "Best FX-Adjusted",
                    best["symbol"],
                    f"{best['fx_adjusted_return'] * 100:.1f}%"
                )

            with col2:
                worst = df.loc[df["fx_adjusted_return"].idxmin()]
                st.metric(
                    "Worst FX-Adjusted",
                    worst["symbol"],
                    f"{worst['fx_adjusted_return'] * 100:.1f}%"
                )

            with col3:
                avg = valid_adj.mean() * 100
                st.metric("Average Adjusted", f"{avg:.1f}%")

    # Sync Section
    st.markdown("---")
    with st.expander("Sync FX Data & Compute Metrics", expanded=False):
        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button("Seed FX Pairs", key="fxi_seed"):
                result = seed_fx_pairs(db_path=get_db_path())
                st.success(f"Seeded {result.get('inserted', 0)} pairs")
                st.rerun()

        with col2:
            if st.button("Sync FX Rates", type="primary", key="fxi_sync"):
                with st.spinner("Syncing FX data..."):
                    summary = sync_fx_pairs(db_path=get_db_path())
                    st.success(
                        f"Sync: {summary.ok} OK, {summary.rows_upserted} rows"
                    )
                    st.rerun()

        with col3:
            if st.button("Compute FX-Adjusted Metrics", key="fxi_compute"):
                with st.spinner("Computing FX-adjusted metrics..."):
                    result = compute_and_store_fx_adjusted_metrics(
                        con, fx_pair=selected_pair
                    )
                    if result.get("success"):
                        st.success(
                            f"Computed metrics for {result.get('symbols_processed', 0)} symbols"
                        )
                        st.rerun()
                    else:
                        st.error(f"Error: {result.get('error', 'Unknown')}")

    render_footer()


# =============================================================================
# Phase 2.5: Mutual Funds Page
# =============================================================================
def mutual_funds_page():
    """Mutual Funds Browser - Fund listing with filters."""
    from psx_ohlcv.db import get_mf_nav, get_mutual_fund, get_mutual_funds
    from psx_ohlcv.sync_mufap import get_data_summary, seed_mutual_funds, sync_mutual_funds

    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 🏦 Mutual Funds")
        st.caption("MUFAP Fund Directory - Pakistan Mutual Funds (Read-Only Analytics)")
    with header_col2:
        st.markdown(
            '<div class="data-info">📊 Analytics Only</div>',
            unsafe_allow_html=True
        )

    con = get_connection()

    # =================================================================
    # FILTERS
    # =================================================================
    st.markdown("---")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        categories = [
            "All", "Equity", "Islamic Equity", "Money Market",
            "Islamic Money Market", "Income", "Islamic Income",
            "Balanced", "VPS", "Asset Allocation"
        ]
        category = st.selectbox("Category", categories, key="mf_category")
        category_filter = None if category == "All" else category

    with col2:
        fund_types = ["All", "OPEN_END", "VPS", "ETF"]
        fund_type = st.selectbox("Fund Type", fund_types, key="mf_type")
        type_filter = None if fund_type == "All" else fund_type

    with col3:
        shariah_only = st.checkbox("Shariah-Compliant Only", key="mf_shariah")

    with col4:
        search = st.text_input("Search Fund", "", key="mf_search")

    # =================================================================
    # FUND LIST
    # =================================================================
    funds = get_mutual_funds(
        con,
        category=category_filter,
        fund_type=type_filter,
        is_shariah=True if shariah_only else None,
        active_only=True,
        search=search if search else None,
    )

    if not funds:
        st.warning("No mutual funds found. Click 'Seed Fund Data' below to populate funds.")
    else:
        # Summary metrics
        st.markdown("---")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Funds", len(funds))
        with col2:
            shariah_count = sum(1 for f in funds if f.get("is_shariah"))
            st.metric("Shariah Funds", shariah_count)
        with col3:
            categories_set = set(f.get("category") for f in funds)
            st.metric("Categories", len(categories_set))
        with col4:
            amcs = set(f.get("amc_code") for f in funds)
            st.metric("AMCs", len(amcs))

        # Fund table
        st.subheader("Fund Directory")
        df = pd.DataFrame(funds)
        display_cols = ["symbol", "fund_name", "category", "amc_name", "fund_type"]
        display_cols = [c for c in display_cols if c in df.columns]

        if "is_shariah" in df.columns:
            df["Shariah"] = df["is_shariah"].apply(lambda x: "Yes" if x else "No")
            display_cols.append("Shariah")

        st.dataframe(
            df[display_cols].rename(columns={
                "symbol": "Symbol",
                "fund_name": "Fund Name",
                "category": "Category",
                "amc_name": "AMC",
                "fund_type": "Type",
            }),
            use_container_width=True,
            height=400,
        )

        # =================================================================
        # FUND DETAIL
        # =================================================================
        st.markdown("---")
        st.subheader("Fund Details")

        fund_options = {f["symbol"]: f["fund_name"] for f in funds}
        selected_symbol = st.selectbox(
            "Select Fund",
            options=list(fund_options.keys()),
            format_func=lambda x: f"{x} - {fund_options[x][:50]}",
            key="mf_selected",
        )

        if selected_symbol:
            fund = next((f for f in funds if f["symbol"] == selected_symbol), None)
            if fund:
                fund_id = fund["fund_id"]

                # Fund info
                col1, col2 = st.columns([2, 1])
                with col1:
                    st.markdown(f"**{fund.get('fund_name', 'N/A')}**")
                    st.caption(f"AMC: {fund.get('amc_name', 'N/A')}")

                with col2:
                    if fund.get("is_shariah"):
                        st.success("Shariah-Compliant")
                    st.caption(f"Type: {fund.get('fund_type', 'N/A')}")

                # NAV data
                nav_df = get_mf_nav(con, fund_id, limit=90)

                if not nav_df.empty:
                    # Latest NAV metrics
                    latest = nav_df.iloc[0]
                    col1, col2, col3, col4 = st.columns(4)

                    with col1:
                        st.metric("Latest NAV", f"Rs. {latest.get('nav', 0):.4f}")
                    with col2:
                        change = latest.get("nav_change_pct", 0) or 0
                        st.metric("Daily Change", f"{change:+.2f}%")
                    with col3:
                        aum = latest.get("aum", 0) or 0
                        st.metric("AUM", f"Rs. {aum:.0f}M")
                    with col4:
                        st.metric("Latest Date", latest.get("date", "N/A"))

                    # NAV chart
                    st.subheader("NAV History")
                    chart_df = nav_df.sort_values("date")
                    st.line_chart(chart_df.set_index("date")["nav"], height=300)
                else:
                    st.info("No NAV data available. Run 'psxsync mufap sync' to fetch NAV data.")

    # =================================================================
    # SYNC SECTION
    # =================================================================
    st.markdown("---")
    with st.expander("Sync Mutual Fund Data"):
        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button("Seed Fund Data", type="primary", key="mf_seed_btn"):
                with st.spinner("Seeding mutual funds..."):
                    result = seed_mutual_funds()
                    st.success(
                        f"Seeded {result.get('inserted', 0)} funds "
                        f"(Failed: {result.get('failed', 0)})"
                    )
                    st.rerun()

        with col2:
            if st.button("Sync NAV Data", key="mf_sync_btn"):
                with st.spinner("Syncing NAV data..."):
                    summary = sync_mutual_funds(source="AUTO")
                    st.success(
                        f"Synced {summary.ok} funds, "
                        f"{summary.rows_upserted} NAV records"
                    )
                    st.rerun()

        with col3:
            # Show summary
            data_summary = get_data_summary()
            st.metric("Funds in DB", data_summary.get("total_funds", 0))
            st.metric("NAV Records", data_summary.get("total_nav_rows", 0))

    render_footer()


# =============================================================================
# Phase 2.5: Fund Analytics Page
# =============================================================================
def fund_analytics_page():
    """Fund Analytics - Performance comparison and rankings."""
    from psx_ohlcv.analytics_mufap import (
        compare_funds,
        get_category_performance,
        get_category_summary,
        get_fund_comparison_table,
        get_mf_analytics,
    )
    from psx_ohlcv.db import get_mutual_funds

    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 📊 Fund Analytics")
        st.caption("Mutual Fund Performance Analysis (Read-Only)")
    with header_col2:
        st.markdown(
            '<div class="data-info">📈 Analytics Only</div>',
            unsafe_allow_html=True
        )

    con = get_connection()

    # Check if we have data
    funds = get_mutual_funds(con, active_only=True)
    if not funds:
        st.warning("No mutual funds found. Go to 'Mutual Funds' page and seed data first.")
        render_footer()
        return

    # =================================================================
    # CATEGORY PERFORMANCE
    # =================================================================
    st.markdown("---")
    st.subheader("Category Performance")

    col1, col2, col3 = st.columns([1, 1, 2])

    with col1:
        categories = [
            "Equity", "Islamic Equity", "Money Market",
            "Islamic Money Market", "Income", "Islamic Income",
            "Balanced", "VPS"
        ]
        category = st.selectbox("Select Category", categories, key="fa_category")

    with col2:
        periods = ["1W", "1M", "3M", "6M", "1Y"]
        period = st.selectbox("Return Period", periods, index=1, key="fa_period")

    with col3:
        # Category summary
        summary = get_category_summary(con, category, period)
        if not summary.get("error"):
            col_a, col_b, col_c = st.columns(3)
            with col_a:
                st.metric(
                    "Avg Return",
                    f"{summary.get('avg_return_pct', 0):+.2f}%"
                )
            with col_b:
                st.metric(
                    "Best Fund",
                    summary.get("best_fund_symbol", "N/A"),
                    f"{summary.get('max_return_pct', 0):+.2f}%"
                )
            with col_c:
                st.metric("Fund Count", summary.get("fund_count", 0))

    # Rankings table
    st.subheader(f"Top {category} Funds ({period})")
    rankings = get_category_performance(con, category, period, top_n=15)

    if rankings:
        rankings_df = pd.DataFrame(rankings)
        display_cols = ["rank", "symbol", "fund_name", "return_pct", "latest_nav"]
        display_cols = [c for c in display_cols if c in rankings_df.columns]

        if "is_shariah" in rankings_df.columns:
            rankings_df["Shariah"] = rankings_df["is_shariah"].apply(
                lambda x: "Yes" if x else "No"
            )
            display_cols.append("Shariah")

        st.dataframe(
            rankings_df[display_cols].rename(columns={
                "rank": "Rank",
                "symbol": "Symbol",
                "fund_name": "Fund Name",
                "return_pct": f"Return ({period})",
                "latest_nav": "NAV",
            }),
            use_container_width=True,
            height=400,
        )
    else:
        st.info(f"No data for {category} category. Sync NAV data first.")

    # =================================================================
    # FUND COMPARISON
    # =================================================================
    st.markdown("---")
    st.subheader("Fund Comparison")

    fund_options = {f["fund_id"]: f"{f['symbol']} - {f['fund_name'][:40]}" for f in funds}

    selected_funds = st.multiselect(
        "Select Funds to Compare (max 5)",
        options=list(fund_options.keys()),
        format_func=lambda x: fund_options[x],
        max_selections=5,
        key="fa_compare",
    )

    if selected_funds:
        # Comparison table
        comparison = get_fund_comparison_table(con, selected_funds)

        if comparison:
            st.subheader("Performance Comparison")

            comp_df = pd.DataFrame(comparison)

            # Format returns as percentages
            for col in ["return_1W", "return_1M", "return_3M", "return_6M", "return_1Y"]:
                if col in comp_df.columns:
                    comp_df[col] = comp_df[col].apply(
                        lambda x: f"{x * 100:+.2f}%" if pd.notna(x) else "N/A"
                    )

            for col in ["vol_1M", "vol_3M"]:
                if col in comp_df.columns:
                    comp_df[col] = comp_df[col].apply(
                        lambda x: f"{x * 100:.2f}%" if pd.notna(x) else "N/A"
                    )

            display_cols = [
                "symbol", "category", "latest_nav",
                "return_1W", "return_1M", "return_3M",
                "vol_1M", "sharpe_ratio"
            ]
            display_cols = [c for c in display_cols if c in comp_df.columns]

            st.dataframe(
                comp_df[display_cols].rename(columns={
                    "symbol": "Symbol",
                    "category": "Category",
                    "latest_nav": "NAV",
                    "return_1W": "1W",
                    "return_1M": "1M",
                    "return_3M": "3M",
                    "vol_1M": "Vol (1M)",
                    "sharpe_ratio": "Sharpe",
                }),
                use_container_width=True,
            )

            # Normalized performance chart
            st.subheader("Normalized Performance (Base = 100)")

            days = st.slider("Chart Period (days)", 30, 365, 90, key="fa_days")
            from datetime import datetime, timedelta
            start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

            perf_df = compare_funds(con, selected_funds, start_date=start_date)

            if not perf_df.empty:
                st.line_chart(perf_df, height=400)
            else:
                st.info("Insufficient data for comparison chart.")

    else:
        st.info("Select funds above to compare their performance.")

    # =================================================================
    # INDIVIDUAL FUND ANALYTICS
    # =================================================================
    st.markdown("---")
    st.subheader("Individual Fund Analytics")

    selected_fund = st.selectbox(
        "Select Fund for Detailed Analytics",
        options=list(fund_options.keys()),
        format_func=lambda x: fund_options[x],
        key="fa_individual",
    )

    if selected_fund:
        analytics = get_mf_analytics(con, selected_fund)

        if not analytics.get("error"):
            col1, col2 = st.columns(2)

            with col1:
                st.markdown(f"**{analytics.get('fund_name', 'N/A')}**")
                st.caption(f"Category: {analytics.get('category', 'N/A')}")
                st.caption(f"AMC: {analytics.get('amc_name', 'N/A')}")

                if analytics.get("latest_nav"):
                    st.metric("Latest NAV", f"Rs. {analytics['latest_nav']:.4f}")

            with col2:
                # Key metrics
                metrics_data = []

                for period in ["1W", "1M", "3M", "6M", "1Y"]:
                    key = f"return_{period}"
                    if analytics.get(key) is not None:
                        metrics_data.append({
                            "Period": period,
                            "Return": f"{analytics[key] * 100:+.2f}%"
                        })

                if metrics_data:
                    st.dataframe(
                        pd.DataFrame(metrics_data),
                        use_container_width=True,
                        hide_index=True,
                    )

            # Risk metrics
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                vol = analytics.get("vol_1M")
                st.metric(
                    "Volatility (1M)",
                    f"{vol * 100:.2f}%" if vol else "N/A"
                )

            with col2:
                sharpe = analytics.get("sharpe_ratio")
                st.metric("Sharpe Ratio", f"{sharpe:.2f}" if sharpe else "N/A")

            with col3:
                dd = analytics.get("max_drawdown")
                st.metric(
                    "Max Drawdown",
                    f"{dd * 100:.2f}%" if dd else "N/A"
                )

            with col4:
                exp = analytics.get("expense_ratio")
                st.metric(
                    "Expense Ratio",
                    f"{exp:.2f}%" if exp else "N/A"
                )

        else:
            st.warning(f"No analytics available: {analytics.get('error')}")

    render_footer()


# =============================================================================
# Phase 3: Bonds Screener Page
# =============================================================================
def bonds_screener_page():
    """Bonds Screener - Fixed income instruments."""
    import pandas as pd

    from psx_ohlcv.analytics_bonds import get_bond_full_analytics
    from psx_ohlcv.db import get_bond_data_summary, get_bonds
    from psx_ohlcv.sync_bonds import seed_bonds, sync_sample_quotes

    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 🧾 Bonds Screener")
        st.caption("Fixed Income Analytics (Phase 3 - Read-Only)")
    with header_col2:
        st.markdown(
            '<div class="data-info">📈 Analytics Only</div>',
            unsafe_allow_html=True
        )

    con = get_connection()

    # =================================================================
    # DATA SYNC CONTROLS
    # =================================================================
    with st.expander("🔧 Data Management", expanded=False):
        sync_col1, sync_col2, sync_col3 = st.columns(3)

        with sync_col1:
            if st.button("Initialize Bonds", key="bonds_init"):
                with st.spinner("Seeding default bonds..."):
                    result = seed_bonds()
                    if result.get("success"):
                        st.success(f"Seeded {result['inserted']} bonds")
                        st.rerun()
                    else:
                        st.error(f"Error: {result.get('error')}")

        with sync_col2:
            days = st.number_input("Sample Days", min_value=30, max_value=365, value=90)
            if st.button("Generate Sample Quotes", key="bonds_sample"):
                with st.spinner("Generating sample data..."):
                    summary = sync_sample_quotes(days=days)
                    st.success(f"Generated {summary.rows_upserted} quotes")
                    st.rerun()

        with sync_col3:
            summary = get_bond_data_summary(con)
            st.metric("Total Bonds", summary.get("total_bonds", 0))
            st.metric("Quote Rows", summary.get("total_quote_rows", 0))

    # =================================================================
    # FILTERS
    # =================================================================
    st.markdown("### Filters")
    filter_col1, filter_col2, filter_col3, filter_col4 = st.columns(4)

    with filter_col1:
        bond_type = st.selectbox(
            "Bond Type",
            ["ALL", "PIB", "T-Bill", "Sukuk", "TFC", "Corporate"],
            key="bonds_type_filter"
        )

    with filter_col2:
        issuer = st.text_input("Issuer", "", key="bonds_issuer_filter")

    with filter_col3:
        islamic_only = st.checkbox("Islamic Only", key="bonds_islamic")

    with filter_col4:
        min_ytm = st.number_input(
            "Min YTM (%)", min_value=0.0, max_value=30.0, value=0.0,
            step=0.5, key="bonds_min_ytm"
        )

    # =================================================================
    # BONDS TABLE
    # =================================================================
    st.markdown("### Bond Universe")

    bonds = get_bonds(
        con,
        bond_type=None if bond_type == "ALL" else bond_type,
        issuer=issuer if issuer else None,
        is_islamic=True if islamic_only else None,
        active_only=True,
    )

    if not bonds:
        st.warning("No bonds found. Click 'Initialize Bonds' to seed default data.")
    else:
        # Build table with analytics
        table_data = []
        for bond in bonds:
            analytics = get_bond_full_analytics(con, bond["bond_id"])
            ytm = analytics.get("ytm")

            # Apply YTM filter
            if min_ytm > 0 and (ytm is None or ytm * 100 < min_ytm):
                continue

            coupon = bond.get("coupon_rate")
            coupon_str = f"{coupon * 100:.1f}%" if coupon else "Zero"
            price = analytics.get("price")
            price_str = f"{price:.2f}" if price else "N/A"
            mod_dur = analytics.get("modified_duration")
            dur_str = f"{mod_dur:.2f}" if mod_dur else "N/A"

            table_data.append({
                "Symbol": bond.get("symbol"),
                "Type": bond.get("bond_type"),
                "Issuer": bond.get("issuer"),
                "Coupon": coupon_str,
                "Maturity": bond.get("maturity_date"),
                "Price": price_str,
                "YTM": f"{ytm * 100:.2f}%" if ytm else "N/A",
                "Duration": dur_str,
                "Islamic": "Yes" if bond.get("is_islamic") else "No",
            })

        if table_data:
            df = pd.DataFrame(table_data)
            st.dataframe(df, use_container_width=True, hide_index=True)

            # =================================================================
            # BOND DETAILS
            # =================================================================
            st.markdown("### Bond Details")

            bond_options = {b["bond_id"]: b["symbol"] for b in bonds}
            selected_bond = st.selectbox(
                "Select Bond",
                options=list(bond_options.keys()),
                format_func=lambda x: bond_options[x],
                key="bonds_selected"
            )

            if selected_bond:
                analytics = get_bond_full_analytics(con, selected_bond)

                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    price = analytics.get("price")
                    price_val = f"{price:.4f}" if price else "N/A"
                    st.metric("Price", price_val)

                with col2:
                    ytm = analytics.get("ytm")
                    ytm_val = f"{ytm * 100:.4f}%" if ytm else "N/A"
                    st.metric("YTM", ytm_val)

                with col3:
                    dur = analytics.get("duration")
                    dur_val = f"{dur:.4f} yrs" if dur else "N/A"
                    st.metric("Duration", dur_val)

                with col4:
                    conv = analytics.get("convexity")
                    conv_val = f"{conv:.4f}" if conv else "N/A"
                    st.metric("Convexity", conv_val)

                # Additional details
                st.markdown("#### Details")
                cpn = analytics.get("coupon_rate")
                cpn_str = f"{cpn * 100:.2f}%" if cpn else "Zero-coupon"
                detail_data = {
                    "Bond ID": analytics.get("bond_id"),
                    "Issuer": analytics.get("issuer"),
                    "Maturity Date": analytics.get("maturity_date"),
                    "Days to Maturity": analytics.get("days_to_maturity"),
                    "Coupon Rate": cpn_str,
                    "Face Value": analytics.get("face_value"),
                    "Accrued Interest": analytics.get("accrued_interest"),
                    "Dirty Price": analytics.get("dirty_price"),
                }
                st.json(detail_data)
        else:
            st.info("No bonds match the current filters.")

    render_footer()


# =============================================================================
# Phase 3: Yield Curve Page
# =============================================================================
def yield_curve_page():
    """Yield Curve - Interest rate term structure."""
    import pandas as pd
    import plotly.graph_objects as go

    from psx_ohlcv.analytics_bonds import (
        build_yield_curve,
        interpolate_yield,
    )
    from psx_ohlcv.db import get_latest_yield_curve, get_yield_curve

    # =================================================================
    # HEADER
    # =================================================================
    st.markdown("## 📉 Yield Curve")
    st.caption("Government Securities Term Structure (Phase 3)")

    con = get_connection()

    # =================================================================
    # CONTROLS
    # =================================================================
    ctrl_col1, ctrl_col2, ctrl_col3 = st.columns([2, 2, 1])

    with ctrl_col1:
        bond_type = st.selectbox(
            "Curve Type",
            ["PIB", "T-Bill", "Sukuk", "ALL"],
            key="yc_bond_type"
        )

    with ctrl_col2:
        curve_date = st.date_input(
            "Curve Date",
            value=None,
            key="yc_date"
        )
        curve_date_str = curve_date.strftime("%Y-%m-%d") if curve_date else None

    with ctrl_col3:
        if st.button("Build Curve", key="yc_build"):
            with st.spinner("Building yield curve..."):
                points = build_yield_curve(con, curve_date_str, bond_type)
                if points:
                    st.success(f"Built curve with {len(points)} points")
                    st.rerun()
                else:
                    st.warning("No data to build curve")

    # =================================================================
    # YIELD CURVE CHART
    # =================================================================
    st.markdown("### Term Structure")

    if curve_date_str:
        points = get_yield_curve(con, curve_date_str, bond_type)
    else:
        curve_date_str, points = get_latest_yield_curve(con, bond_type)

    if not points:
        st.info("No yield curve data available. Click 'Build Curve' to generate.")
        st.markdown("""
        **To build a yield curve:**
        1. First initialize bonds: `psxsync bonds init`
        2. Generate sample quotes: `psxsync bonds load --sample`
        3. Compute analytics: `psxsync bonds compute --curve`
        """)
    else:
        st.caption(f"Curve Date: {curve_date_str}")

        # Prepare data
        df = pd.DataFrame(points)

        tenor_labels = {
            3: "3M", 6: "6M", 12: "1Y", 24: "2Y",
            36: "3Y", 60: "5Y", 84: "7Y", 120: "10Y",
            180: "15Y", 240: "20Y",
        }
        df["tenor_label"] = df["tenor_months"].apply(
            lambda x: tenor_labels.get(x, f"{x}M")
        )
        df["yield_pct"] = df["yield_rate"] * 100

        # Create chart
        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=df["tenor_months"],
            y=df["yield_pct"],
            mode="lines+markers",
            name=f"{bond_type} Curve",
            line=dict(width=3, color="#1f77b4"),
            marker=dict(size=10),
            hovertemplate=(
                "<b>%{text}</b><br>"
                "Yield: %{y:.2f}%<br>"
                "<extra></extra>"
            ),
            text=df["tenor_label"],
        ))

        fig.update_layout(
            title=f"Yield Curve - {bond_type}",
            xaxis_title="Tenor (Months)",
            yaxis_title="Yield (%)",
            height=400,
            showlegend=False,
            hovermode="x unified",
        )

        # Add tenor labels on x-axis
        fig.update_xaxes(
            tickmode="array",
            tickvals=df["tenor_months"].tolist(),
            ticktext=df["tenor_label"].tolist(),
        )

        st.plotly_chart(fig, use_container_width=True)

        # =================================================================
        # CURVE DATA TABLE
        # =================================================================
        st.markdown("### Curve Points")

        table_df = df[["tenor_label", "tenor_months", "yield_pct"]].copy()
        table_df.columns = ["Tenor", "Months", "Yield (%)"]
        table_df["Yield (%)"] = table_df["Yield (%)"].apply(lambda x: f"{x:.4f}%")

        st.dataframe(table_df, use_container_width=True, hide_index=True)

        # =================================================================
        # INTERPOLATION TOOL
        # =================================================================
        st.markdown("### Yield Interpolation")

        interp_col1, interp_col2 = st.columns([1, 2])

        with interp_col1:
            target_tenor = st.number_input(
                "Target Tenor (months)",
                min_value=1,
                max_value=360,
                value=48,
                key="yc_target_tenor"
            )

        with interp_col2:
            interp_yield = interpolate_yield(points, target_tenor, "LINEAR")
            if interp_yield:
                st.metric(
                    f"Interpolated Yield ({target_tenor}M)",
                    f"{interp_yield * 100:.4f}%"
                )
            else:
                st.info("Cannot interpolate for this tenor")

    render_footer()


# =============================================================================
# Phase 3: Sukuk Screener Page (Additive - separate from bonds)
# =============================================================================
def sukuk_screener_page():
    """Sukuk Screener - Shariah-compliant fixed income instruments."""
    import pandas as pd

    from psx_ohlcv.analytics_sukuk import (
        get_sukuk_analytics_full,
        get_analytics_by_category,
    )
    from psx_ohlcv.db import get_sukuk_data_summary, get_sukuk_list
    from psx_ohlcv.sync_sukuk import seed_sukuk, sync_sukuk_quotes

    con = get_connection()

    st.markdown("## 🕌 Sukuk Screener")
    st.caption("Shariah-compliant fixed income (GOP Sukuk, PIBs, T-Bills)")

    # =================================================================
    # ADMIN CONTROLS (collapsed)
    # =================================================================
    with st.expander("⚙️ Data Management", expanded=False):
        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button("Seed Sukuk Data", key="sukuk_seed"):
                with st.spinner("Seeding sukuk instruments..."):
                    result = seed_sukuk()
                    if result.get("success"):
                        st.success(f"Seeded {result['inserted']} instruments")
                    else:
                        st.error(f"Error: {result.get('error')}")
                st.rerun()

        with col2:
            if st.button("Generate Sample Quotes", key="sukuk_sample"):
                with st.spinner("Generating sample quote data..."):
                    summary = sync_sukuk_quotes(source="SAMPLE", days=90)
                    st.success(f"Generated {summary.rows_upserted} quotes")
                st.rerun()

        with col3:
            summary = get_sukuk_data_summary(con)
            st.metric("Total Instruments", summary.get("total_sukuk", 0))
            st.metric("With Quotes", summary.get("sukuk_with_quotes", 0))

    # =================================================================
    # FILTERS
    # =================================================================
    st.markdown("### Filters")

    filter_col1, filter_col2, filter_col3, filter_col4 = st.columns(4)

    with filter_col1:
        category = st.selectbox(
            "Category",
            ["ALL", "GOP_SUKUK", "PIB", "TBILL", "CORPORATE_SUKUK", "TFC"],
            key="sukuk_category_filter"
        )

    with filter_col2:
        issuer = st.text_input("Issuer", "", key="sukuk_issuer_filter")

    with filter_col3:
        shariah_only = st.checkbox("Shariah Only", key="sukuk_shariah")

    with filter_col4:
        min_ytm = st.number_input(
            "Min YTM (%)", min_value=0.0, max_value=30.0, value=0.0,
            step=0.5, key="sukuk_min_ytm"
        )

    # =================================================================
    # SUKUK LIST
    # =================================================================
    sukuk_list = get_sukuk_list(
        con,
        active_only=True,
        category=None if category == "ALL" else category,
    )

    if not sukuk_list:
        st.warning("No sukuk found. Click 'Seed Sukuk Data' to initialize.")
        render_footer()
        return

    # Apply filters
    if issuer:
        sukuk_list = [
            s for s in sukuk_list
            if issuer.lower() in s.get("issuer", "").lower()
        ]
    if shariah_only:
        sukuk_list = [s for s in sukuk_list if s.get("shariah_compliant")]

    # Get analytics for each sukuk
    analytics_data = get_analytics_by_category(
        category=None if category == "ALL" else category,
        db_path=None
    )

    # Create lookup for analytics
    analytics_lookup = {a["instrument_id"]: a for a in analytics_data}

    # Apply YTM filter
    if min_ytm > 0:
        def check_ytm(s):
            ytm = analytics_lookup.get(s["instrument_id"], {}).get("ytm", 0)
            return (ytm or 0) >= min_ytm
        sukuk_list = [s for s in sukuk_list if check_ytm(s)]

    if sukuk_list:
        # Display table
        table_data = []
        for sukuk in sukuk_list:
            analytics = analytics_lookup.get(sukuk["instrument_id"], {})
            table_data.append({
                "ID": sukuk["instrument_id"],
                "Name": sukuk.get("name", "")[:40],
                "Category": sukuk.get("category", ""),
                "Issuer": sukuk.get("issuer", "")[:20],
                "Maturity": sukuk.get("maturity_date", ""),
                "Coupon": f"{sukuk.get('coupon_rate', 0) or 0:.2f}%",
                "YTM": f"{analytics.get('ytm', 0) or 0:.2f}%",
                "Duration": f"{analytics.get('duration', 0) or 0:.2f}",
                "Shariah": "✓" if sukuk.get("shariah_compliant") else "",
            })

        df = pd.DataFrame(table_data)
        st.dataframe(df, use_container_width=True, hide_index=True)

        st.markdown(f"**Total: {len(sukuk_list)} instruments**")

        # =================================================================
        # SUKUK DETAIL VIEW
        # =================================================================
        st.markdown("### Sukuk Details")

        sukuk_options = {
            s["instrument_id"]: s.get("name", s["instrument_id"])
            for s in sukuk_list
        }
        selected_id = st.selectbox(
            "Select Instrument",
            options=list(sukuk_options.keys()),
            format_func=lambda x: sukuk_options.get(x, x),
            key="sukuk_selected"
        )

        if selected_id:
            result = get_sukuk_analytics_full(selected_id)

            if not result.get("error"):
                sukuk = result.get("sukuk", {})
                quote = result.get("quote", {})
                analytics = result.get("analytics", {})

                col1, col2, col3 = st.columns(3)

                with col1:
                    st.markdown("**Instrument Info**")
                    st.write(f"**ID:** {sukuk.get('instrument_id')}")
                    st.write(f"**Issuer:** {sukuk.get('issuer')}")
                    st.write(f"**Category:** {sukuk.get('category')}")
                    st.write(f"**Maturity:** {sukuk.get('maturity_date')}")
                    shariah = "Yes ✓" if sukuk.get("shariah_compliant") else "No"
                    st.write(f"**Shariah:** {shariah}")

                with col2:
                    st.markdown("**Pricing**")
                    if quote:
                        st.write(f"**Date:** {quote.get('quote_date')}")
                        st.write(f"**Clean Price:** {quote.get('clean_price')}")
                        st.write(f"**Dirty Price:** {quote.get('dirty_price')}")
                        st.write(f"**YTM:** {quote.get('yield_to_maturity')}%")
                    else:
                        st.info("No quote data available")

                with col3:
                    st.markdown("**Analytics**")
                    if analytics.get("yield_to_maturity"):
                        ytm = analytics["yield_to_maturity"]
                        st.metric("YTM", f"{ytm:.4f}%")
                        if analytics.get("modified_duration"):
                            mod_dur = analytics["modified_duration"]
                            st.metric("Mod Duration", f"{mod_dur:.2f} yrs")
                        if analytics.get("convexity"):
                            convex = analytics["convexity"]
                            st.metric("Convexity", f"{convex:.2f}")
                    else:
                        st.info("No analytics computed")
    else:
        st.info("No sukuk match the current filters.")

    render_footer()


# =============================================================================
# Phase 3: Sukuk Yield Curve Page
# =============================================================================
def sukuk_yield_curve_page():
    """Sukuk Yield Curve - Term structure for sukuk instruments."""
    import pandas as pd
    import plotly.graph_objects as go

    from psx_ohlcv.analytics_sukuk import (
        get_yield_curve_data,
        interpolate_yield_curve,
    )
    from psx_ohlcv.sync_sukuk import sync_sample_yield_curves

    st.markdown("## 📈 Sukuk Yield Curve")
    st.caption("Term structure of sukuk yields (GOP Sukuk, PIB, T-Bill)")

    # =================================================================
    # CONTROLS
    # =================================================================
    col1, col2, col3 = st.columns([1, 1, 2])

    with col1:
        curve_name = st.selectbox(
            "Curve",
            ["GOP_SUKUK", "PIB", "TBILL"],
            key="sukuk_curve_name"
        )

    with col2:
        curve_date = st.date_input(
            "Date (blank for latest)",
            value=None,
            key="sukuk_curve_date"
        )

    with col3:
        if st.button("Generate Sample Curves", key="sukuk_gen_curves"):
            with st.spinner("Generating yield curves..."):
                summary = sync_sample_yield_curves(days=30)
                st.success(f"Generated {summary.rows_upserted} curve points")
            st.rerun()

    # =================================================================
    # YIELD CURVE CHART
    # =================================================================
    date_str = curve_date.isoformat() if curve_date else None
    curve_data = get_yield_curve_data(
        curve_name=curve_name,
        curve_date=date_str,
    )

    points = curve_data.get("points", [])

    if not points:
        st.info("No yield curve data. Click 'Generate Sample Curves' to create.")
        st.markdown("""
        **To build a yield curve:**
        1. Seed sukuk data: `psxsync sukuk seed`
        2. Sync sample quotes: `psxsync sukuk sync --include-curves`
        """)
        render_footer()
        return

    # Build DataFrame
    df = pd.DataFrame(points)
    df["yield_pct"] = df["yield_rate"]

    # Create chart
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df["tenor_days"],
        y=df["yield_pct"],
        mode="lines+markers",
        name=curve_name,
        line=dict(width=2),
        marker=dict(size=8),
    ))

    fig.update_layout(
        title=f"Sukuk Yield Curve - {curve_name}",
        xaxis_title="Tenor (Days)",
        yaxis_title="Yield (%)",
        height=450,
        hovermode="x unified",
    )

    # Add tenor labels on x-axis
    fig.update_xaxes(
        tickmode="array",
        tickvals=df["tenor_days"].tolist(),
        ticktext=df["tenor_label"].tolist(),
    )

    st.plotly_chart(fig, use_container_width=True)

    # =================================================================
    # CURVE DATA TABLE
    # =================================================================
    st.markdown("### Curve Points")

    table_df = df[["tenor_label", "tenor_days", "yield_pct"]].copy()
    table_df.columns = ["Tenor", "Days", "Yield (%)"]
    table_df["Yield (%)"] = table_df["Yield (%)"].apply(lambda x: f"{x:.4f}%")

    st.dataframe(table_df, use_container_width=True, hide_index=True)

    # =================================================================
    # INTERPOLATION TOOL
    # =================================================================
    st.markdown("### Yield Interpolation")

    interp_col1, interp_col2 = st.columns([1, 2])

    with interp_col1:
        target_days = st.number_input(
            "Target Tenor (days)",
            min_value=1,
            max_value=3650,
            value=365,
            key="sukuk_target_tenor"
        )

    with interp_col2:
        interp_yield = interpolate_yield_curve(points, target_days)
        if interp_yield:
            st.metric(
                f"Interpolated Yield ({target_days} days)",
                f"{interp_yield:.4f}%"
            )
        else:
            st.info("Cannot interpolate for this tenor")

    render_footer()


# =============================================================================
# Phase 3: SBP Auction Archive Page
# =============================================================================
def sbp_auction_archive_page():
    """SBP Auction Archive - Primary market document archive."""
    import pandas as pd

    from psx_ohlcv.sources.sbp_primary_market import (
        get_documents_by_type,
        get_sbp_document_urls,
        index_documents,
        create_sample_documents,
        DOC_TYPES,
        INSTRUMENT_TYPES,
        DOCS_DIR,
    )
    from psx_ohlcv.sync_sukuk import index_sbp_documents

    st.markdown("## 🏛️ SBP Auction Archive")
    st.caption("State Bank of Pakistan Primary Market Document Archive")

    # =================================================================
    # ADMIN CONTROLS
    # =================================================================
    with st.expander("⚙️ Document Management", expanded=False):
        col1, col2 = st.columns(2)

        with col1:
            if st.button("Create Sample Documents", key="sbp_create_samples"):
                with st.spinner("Creating sample documents..."):
                    created = create_sample_documents()
                    st.success(f"Created {len(created)} sample files")
                st.rerun()

        with col2:
            if st.button("Re-index Documents", key="sbp_reindex"):
                with st.spinner("Indexing documents..."):
                    result = index_sbp_documents()
                    st.success(f"Indexed {result.get('total_documents', 0)} documents")
                st.rerun()

        st.markdown(f"**Document Directory:** `{DOCS_DIR}`")

    # =================================================================
    # SBP URLS
    # =================================================================
    st.markdown("### Official SBP Data Sources")

    urls = get_sbp_document_urls()
    url_data = [
        {"Source": name.replace("_", " ").title(), "URL": url}
        for name, url in urls.items()
    ]
    st.dataframe(pd.DataFrame(url_data), use_container_width=True, hide_index=True)

    st.info("Download documents and place in the document directory.")

    # =================================================================
    # FILTERS
    # =================================================================
    st.markdown("### Document Archive")

    filter_col1, filter_col2 = st.columns(2)

    def fmt_inst(x):
        return INSTRUMENT_TYPES.get(x, x) if x != "ALL" else "All Types"

    def fmt_doc(x):
        return DOC_TYPES.get(x, x) if x != "ALL" else "All Documents"

    with filter_col1:
        inst_type = st.selectbox(
            "Instrument Type",
            ["ALL"] + list(INSTRUMENT_TYPES.keys()),
            format_func=fmt_inst,
            key="sbp_inst_filter"
        )

    with filter_col2:
        doc_type = st.selectbox(
            "Document Type",
            ["ALL"] + list(DOC_TYPES.keys()),
            format_func=fmt_doc,
            key="sbp_doc_filter"
        )

    # =================================================================
    # DOCUMENT LIST
    # =================================================================
    documents = get_documents_by_type(
        instrument_type=None if inst_type == "ALL" else inst_type,
        doc_type=None if doc_type == "ALL" else doc_type,
    )

    if documents:
        table_data = []
        for doc in documents:
            doc_type_val = doc.get("doc_type")
            inst_type_val = doc.get("instrument_type")
            indexed_at = doc.get("indexed_at", "")
            table_data.append({
                "ID": doc.get("doc_id", "")[:30],
                "Type": DOC_TYPES.get(doc_type_val, doc_type_val),
                "Instrument": INSTRUMENT_TYPES.get(inst_type_val, inst_type_val),
                "Auction Date": doc.get("auction_date", ""),
                "File": doc.get("file_name", ""),
                "Indexed": indexed_at[:10] if indexed_at else "",
            })

        df = pd.DataFrame(table_data)
        st.dataframe(df, use_container_width=True, hide_index=True)

        st.markdown(f"**Total: {len(documents)} documents**")
    else:
        st.info("No documents found. Add SBP files and click 'Re-index'.")

    # =================================================================
    # DOCUMENT NAMING GUIDE
    # =================================================================
    with st.expander("📋 Document Naming Convention", expanded=False):
        st.markdown("""
        **Recommended file naming format:**
        ```
        {INSTRUMENT}_{DOCTYPE}_{DATE}.{ext}
        ```

        **Examples:**
        - `TBILL_AUCTION_RESULT_2026-01-15.pdf`
        - `PIB_AUCTION_RESULT_2026-01-10.xlsx`
        - `GOP_SUKUK_YIELD_CURVE_2026-01.csv`

        **Supported formats:** PDF, XLS, XLSX, CSV

        **Instrument types:** TBILL, PIB, GOP_SUKUK, FRB

        **Document types:** AUCTION_RESULT, AUCTION_CALENDAR, YIELD_CURVE, CUT_OFF_YIELD
        """)

    render_footer()


# =============================================================================
# Phase 3.5: Government Fixed Income Pages
# =============================================================================

def govt_fixed_income_page():
    """Government Fixed Income Screener - MTB, PIB, GOP Sukuk."""
    import pandas as pd

    from psx_ohlcv.analytics_fixed_income import (
        compute_analytics_for_instrument,
        get_instruments_by_yield,
    )
    from psx_ohlcv.db import (
        get_fi_instrument,
        get_fi_instruments,
        get_fi_latest_quote,
    )
    from psx_ohlcv.sync_fixed_income import (
        get_fi_status_summary,
        seed_fi_instruments,
        sync_all_fixed_income,
        sync_fi_quotes,
    )
    from psx_ohlcv.services.fi_sync_service import (
        is_fi_sync_running,
        read_fi_status,
        start_fi_sync_background,
        stop_fi_sync,
    )

    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 💰 Government Fixed Income")
        st.caption("MTB, PIB, GOP Sukuk Analytics (Phase 3.5 - Read-Only)")
    with header_col2:
        st.markdown(
            '<div class="data-info">📈 Bond Analytics</div>',
            unsafe_allow_html=True
        )

    con = get_connection()

    # =================================================================
    # DATA SYNC CONTROLS
    # =================================================================
    with st.expander("🔧 Data Management", expanded=False):
        sync_col1, sync_col2, sync_col3 = st.columns(3)

        with sync_col1:
            if st.button("Seed Instruments", key="fi_seed"):
                with st.spinner("Seeding fixed income instruments..."):
                    result = seed_fi_instruments()
                    if result.get("success"):
                        st.success(f"Seeded {result['inserted']} instruments")
                        st.rerun()
                    else:
                        st.error(f"Error: {result.get('errors', [])}")

        with sync_col2:
            if st.button("Sync All Data", key="fi_sync_all"):
                with st.spinner("Syncing all fixed income data..."):
                    results = sync_all_fixed_income()
                    st.success("Sync complete!")
                    for key, summary in results.items():
                        if isinstance(summary, dict):
                            ok_count = summary.get('ok', summary.get('inserted', 0))
                            st.write(f"**{key}**: {ok_count} OK")
                    st.rerun()

        with sync_col3:
            summary = get_fi_status_summary()
            st.metric("Total Instruments", summary.get("total_instruments", 0))
            st.metric("Quote Rows", summary.get("total_quote_rows", 0))

        # Background Service Controls
        st.markdown("---")
        st.markdown("#### 🔄 Background Sync Service")

        svc_running, svc_pid = is_fi_sync_running()
        svc_status = read_fi_status()

        svc_col1, svc_col2, svc_col3 = st.columns(3)

        with svc_col1:
            if svc_running:
                st.success(f"🟢 Service Running (PID: {svc_pid})")
                if st.button("Stop Service", key="fi_svc_stop"):
                    success, msg = stop_fi_sync()
                    if success:
                        st.success(msg)
                    else:
                        st.error(msg)
                    st.rerun()
            else:
                st.warning("🔴 Service Stopped")
                continuous = st.checkbox(
                    "Continuous Mode",
                    key="fi_svc_continuous",
                    help="Run sync every hour automatically"
                )
                if st.button("Start Service", key="fi_svc_start"):
                    success, msg = start_fi_sync_background(continuous=continuous)
                    if success:
                        st.success(msg)
                    else:
                        st.error(msg)
                    st.rerun()

        with svc_col2:
            if svc_status.last_sync_at:
                st.metric("Last Sync", svc_status.last_sync_at[:16])
            if svc_status.sync_count:
                st.metric("Total Syncs", svc_status.sync_count)

        with svc_col3:
            if svc_status.docs_synced:
                st.metric("Docs Synced", svc_status.docs_synced)
            if svc_status.curves_synced:
                st.metric("Curves Synced", svc_status.curves_synced)

        if svc_status.progress_message:
            st.caption(f"Status: {svc_status.progress_message}")

    # =================================================================
    # FILTERS
    # =================================================================
    st.markdown("### Filters")
    filter_col1, filter_col2, filter_col3, filter_col4 = st.columns(4)

    with filter_col1:
        category = st.selectbox(
            "Category",
            ["ALL", "MTB", "PIB", "GOP_SUKUK", "CORP_BOND", "CORP_SUKUK"],
            key="fi_category_filter"
        )

    with filter_col2:
        min_yield = st.number_input(
            "Min YTM (%)", min_value=0.0, max_value=30.0, value=0.0,
            step=0.5, key="fi_min_ytm"
        )

    with filter_col3:
        sort_by = st.selectbox(
            "Sort By",
            ["yield", "duration", "maturity"],
            key="fi_sort"
        )

    with filter_col4:
        shariah_only = st.checkbox("Shariah Only", key="fi_shariah")

    # =================================================================
    # INSTRUMENTS TABLE
    # =================================================================
    st.markdown("### Fixed Income Instruments")

    cat_filter = None if category == "ALL" else category
    min_yield_dec = min_yield / 100 if min_yield > 0 else None

    instruments = get_instruments_by_yield(
        con,
        category=cat_filter,
        min_yield=min_yield_dec,
        sort_by=sort_by,
        limit=100,
    )

    # Filter shariah if needed
    if shariah_only:
        instruments = [i for i in instruments if i.get("is_shariah")]

    if not instruments:
        st.warning("No instruments found. Click 'Seed Instruments' to load sample data.")
    else:
        # Build table
        table_data = []
        for inst in instruments:
            coupon = inst.get("coupon_rate")
            coupon_str = f"{coupon * 100:.2f}%" if coupon else "Zero"
            ytm = inst.get("yield_to_maturity")
            ytm_str = f"{ytm * 100:.2f}%" if ytm else "N/A"
            dur = inst.get("modified_duration")
            dur_str = f"{dur:.2f}" if dur else "N/A"
            price = inst.get("clean_price") or inst.get("dirty_price")
            price_str = f"{price:.2f}" if price else "N/A"

            table_data.append({
                "ISIN": inst.get("isin", "")[:15],
                "Symbol": inst.get("symbol"),
                "Category": inst.get("category"),
                "Coupon": coupon_str,
                "Maturity": inst.get("maturity_date"),
                "Price": price_str,
                "YTM": ytm_str,
                "Duration": dur_str,
                "Shariah": "✓" if inst.get("is_shariah") else "",
            })

        df = pd.DataFrame(table_data)
        st.dataframe(df, use_container_width=True, hide_index=True)

        st.markdown(f"**Total: {len(instruments)} instruments**")

        # =================================================================
        # INSTRUMENT DETAILS
        # =================================================================
        st.markdown("### Instrument Details")

        all_instruments = get_fi_instruments(con, category=cat_filter, active_only=True)
        if all_instruments:
            isin_options = {i["isin"]: f"{i['symbol']} ({i['category']})" for i in all_instruments}
            selected_isin = st.selectbox(
                "Select Instrument",
                options=list(isin_options.keys()),
                format_func=lambda x: isin_options.get(x, x),
                key="fi_selected_isin"
            )

            if selected_isin:
                inst = get_fi_instrument(con, selected_isin)
                quote = get_fi_latest_quote(con, selected_isin)
                analytics = compute_analytics_for_instrument(con, selected_isin)

                if inst:
                    col1, col2 = st.columns(2)

                    with col1:
                        st.markdown("**Instrument Info**")
                        st.write(f"**Symbol:** {inst.get('symbol')}")
                        st.write(f"**ISIN:** {inst.get('isin')}")
                        st.write(f"**Category:** {inst.get('category')}")
                        st.write(f"**Issuer:** {inst.get('issuer', 'N/A')}")
                        st.write(f"**Issue Date:** {inst.get('issue_date', 'N/A')}")
                        st.write(f"**Maturity:** {inst.get('maturity_date')}")
                        coupon = inst.get("coupon_rate")
                        st.write(f"**Coupon:** {coupon * 100:.2f}%" if coupon else "**Coupon:** Zero")
                        st.write(f"**Shariah:** {'Yes' if inst.get('is_shariah') else 'No'}")

                    with col2:
                        st.markdown("**Analytics**")
                        if quote:
                            st.write(f"**Quote Date:** {quote.get('date')}")
                            st.write(f"**Clean Price:** {quote.get('clean_price', 'N/A')}")
                            st.write(f"**Dirty Price:** {quote.get('dirty_price', 'N/A')}")
                            ytm = quote.get("yield_to_maturity")
                            st.write(f"**Quoted YTM:** {ytm * 100:.2f}%" if ytm else "**YTM:** N/A")

                        if analytics and "ytm" in analytics:
                            st.markdown("---")
                            st.write(f"**Computed YTM:** {analytics.get('ytm_pct', 'N/A')}%")
                            st.write(f"**Mac Duration:** {analytics.get('macaulay_duration', 'N/A')} years")
                            st.write(f"**Mod Duration:** {analytics.get('modified_duration', 'N/A')}")
                            st.write(f"**Convexity:** {analytics.get('convexity', 'N/A')}")
                            st.write(f"**PVBP:** {analytics.get('pvbp', 'N/A')}")

    render_footer()


def fi_yield_curve_page():
    """Fixed Income Yield Curve - Term structure visualization."""
    import pandas as pd
    import plotly.graph_objects as go

    from psx_ohlcv.analytics_fixed_income import (
        compare_yield_curves,
        get_yield_curve_analytics,
    )
    from psx_ohlcv.db import get_fi_curve, get_fi_curve_dates
    from psx_ohlcv.sync_fixed_income import sync_fi_curves

    # =================================================================
    # HEADER
    # =================================================================
    st.markdown("## 📊 Fixed Income Yield Curve")
    st.caption("Government Securities Term Structure (Phase 3.5)")

    con = get_connection()

    # =================================================================
    # DATA CONTROLS
    # =================================================================
    with st.expander("🔧 Data Management", expanded=False):
        col1, col2 = st.columns(2)

        with col1:
            if st.button("Sync Yield Curves", key="fi_curves_sync"):
                with st.spinner("Syncing yield curve data..."):
                    summary = sync_fi_curves()
                    st.success(f"Synced {summary.rows_upserted} curve points")
                    st.rerun()

        with col2:
            # Show available curves
            curve_dates = get_fi_curve_dates(con)
            if curve_dates:
                st.write("**Available Curves:**")
                for cd in curve_dates[:5]:
                    st.write(f"- {cd.get('curve_name')}: {cd.get('latest_date')} ({cd.get('count')} points)")

    # =================================================================
    # CURVE SELECTION
    # =================================================================
    st.markdown("### Select Curve")

    col1, col2 = st.columns(2)

    with col1:
        curve_name = st.selectbox(
            "Curve Name",
            ["PKR_MTB", "PKR_PIB", "PKR_GOP_SUKUK"],
            key="fi_curve_name"
        )

    with col2:
        # Get available dates for selected curve
        curve_data = get_fi_curve(con, curve_name)
        if curve_data:
            available_dates = sorted(set(p.get("curve_date") for p in curve_data if p.get("curve_date")), reverse=True)
            curve_date = st.selectbox(
                "Curve Date",
                available_dates if available_dates else ["Latest"],
                key="fi_curve_date"
            )
        else:
            curve_date = None
            st.info("No curve data available")

    # =================================================================
    # YIELD CURVE CHART
    # =================================================================
    if curve_data:
        analytics = get_yield_curve_analytics(con, curve_name, curve_date)

        if not analytics.get("error"):
            points = analytics.get("points", [])
            if points:
                # Sort by tenor
                sorted_points = sorted(points, key=lambda x: x.get("tenor_months", 0))

                # Build chart data
                tenors = []
                yields = []
                tenor_labels = []

                for p in sorted_points:
                    tenor_m = p.get("tenor_months", 0)
                    yld = p.get("yield_value", 0)
                    if yld:
                        tenors.append(tenor_m)
                        yields.append(yld * 100)  # Convert to percentage
                        if tenor_m < 12:
                            tenor_labels.append(f"{tenor_m}M")
                        else:
                            tenor_labels.append(f"{tenor_m // 12}Y")

                # Create chart
                fig = go.Figure()

                fig.add_trace(go.Scatter(
                    x=tenors,
                    y=yields,
                    mode='lines+markers',
                    name=curve_name,
                    line=dict(width=2),
                    marker=dict(size=8),
                ))

                fig.update_layout(
                    title=f"Yield Curve - {curve_name}",
                    xaxis_title="Tenor (Months)",
                    yaxis_title="Yield (%)",
                    xaxis=dict(
                        tickmode='array',
                        tickvals=tenors,
                        ticktext=tenor_labels,
                    ),
                    height=400,
                )

                st.plotly_chart(fig, use_container_width=True)

                # =================================================================
                # CURVE METRICS
                # =================================================================
                st.markdown("### Curve Metrics")

                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    st.metric("Curve Date", analytics.get("curve_date", "N/A"))

                with col2:
                    shape = analytics.get("shape", "N/A")
                    st.metric("Curve Shape", shape.title() if shape else "N/A")

                with col3:
                    steepness = analytics.get("steepness")
                    if steepness is not None:
                        st.metric("Steepness", f"{steepness * 100:.0f} bps")
                    else:
                        st.metric("Steepness", "N/A")

                with col4:
                    st.metric("Points", analytics.get("num_points", 0))

                # =================================================================
                # CURVE DATA TABLE
                # =================================================================
                st.markdown("### Curve Points")

                table_data = []
                for p in sorted_points:
                    tenor_m = p.get("tenor_months", 0)
                    if tenor_m < 12:
                        tenor_str = f"{tenor_m} Months"
                    else:
                        tenor_str = f"{tenor_m // 12} Years"

                    yld = p.get("yield_value", 0)
                    table_data.append({
                        "Tenor": tenor_str,
                        "Months": tenor_m,
                        "Yield (%)": f"{yld * 100:.4f}" if yld else "N/A",
                    })

                df = pd.DataFrame(table_data)
                st.dataframe(df, use_container_width=True, hide_index=True)

        else:
            st.warning(f"No data for curve: {analytics.get('error')}")

    else:
        st.info("No yield curve data. Click 'Sync Yield Curves' to load sample data.")

    render_footer()


def sbp_pma_archive_page():
    """SBP PMA Archive - Primary Market Activities document metadata."""
    import pandas as pd

    from psx_ohlcv.db import get_sbp_pma_docs
    from psx_ohlcv.sources.sbp_pma import (
        PMA_DOCS_DIR,
        SBP_PMA_URL,
        fetch_and_parse_pma,
        get_sample_pma_documents,
    )
    from psx_ohlcv.sync_fixed_income import sync_sbp_pma_docs

    # =================================================================
    # HEADER
    # =================================================================
    st.markdown("## 📝 SBP PMA Archive")
    st.caption("State Bank of Pakistan Primary Market Activities (Phase 3.5)")

    con = get_connection()

    # =================================================================
    # INFO BOX
    # =================================================================
    st.info(f"""
    **Source:** [SBP Primary Market Activities]({SBP_PMA_URL})

    This page archives document metadata from SBP's Primary Market Activities page.
    Documents include MTB, PIB, and GOP Sukuk auction results, calendars, and announcements.
    """)

    # =================================================================
    # DATA CONTROLS
    # =================================================================
    with st.expander("🔧 Data Management", expanded=False):
        col1, col2, col3 = st.columns(3)

        with col1:
            source = st.selectbox(
                "Source",
                ["SBP", "SAMPLE"],
                key="pma_source"
            )

        with col2:
            download = st.checkbox("Download PDFs", key="pma_download")

        with col3:
            category = st.selectbox(
                "Category Filter",
                ["ALL", "MTB", "PIB", "GOP_SUKUK"],
                key="pma_category"
            )

        if st.button("Sync Documents", key="pma_sync"):
            with st.spinner("Syncing SBP PMA documents..."):
                cat_filter = None if category == "ALL" else category
                summary = sync_sbp_pma_docs(
                    source=source,
                    download=download,
                    category=cat_filter,
                )
                st.success(f"Synced {summary.ok} documents, {summary.rows_upserted} stored")
                st.rerun()

        st.markdown(f"**Local Storage:** `{PMA_DOCS_DIR}`")

    # =================================================================
    # FILTERS
    # =================================================================
    st.markdown("### Filters")
    filter_col1, filter_col2 = st.columns(2)

    with filter_col1:
        cat_filter = st.selectbox(
            "Category",
            ["ALL", "MTB", "PIB", "GOP_SUKUK", "OTHER"],
            key="pma_view_category"
        )

    with filter_col2:
        doc_type_filter = st.selectbox(
            "Document Type",
            ["ALL", "RESULT", "CALENDAR", "ANNOUNCEMENT", "CIRCULAR", "TARGET", "OTHER"],
            key="pma_doc_type"
        )

    # =================================================================
    # DOCUMENTS TABLE
    # =================================================================
    st.markdown("### Documents")

    docs = get_sbp_pma_docs(
        con,
        category=None if cat_filter == "ALL" else cat_filter,
        doc_type=None if doc_type_filter == "ALL" else doc_type_filter,
        limit=100,
    )

    if docs:
        table_data = []
        for doc in docs:
            table_data.append({
                "Title": doc.get("title", "")[:50],
                "Category": doc.get("category", "N/A"),
                "Type": doc.get("doc_type", "N/A"),
                "Date": doc.get("doc_date", "N/A"),
                "URL": doc.get("url", "")[:60] + "..." if len(doc.get("url", "")) > 60 else doc.get("url", ""),
            })

        df = pd.DataFrame(table_data)
        st.dataframe(df, use_container_width=True, hide_index=True)

        st.markdown(f"**Total: {len(docs)} documents**")

        # =================================================================
        # DOCUMENT DETAILS
        # =================================================================
        st.markdown("### Document Details")

        doc_options = {d.get("doc_id"): d.get("title", d.get("doc_id")) for d in docs}
        selected_doc = st.selectbox(
            "Select Document",
            options=list(doc_options.keys()),
            format_func=lambda x: doc_options.get(x, x)[:60],
            key="pma_selected_doc"
        )

        if selected_doc:
            doc = next((d for d in docs if d.get("doc_id") == selected_doc), None)
            if doc:
                col1, col2 = st.columns(2)

                with col1:
                    st.write(f"**Title:** {doc.get('title')}")
                    st.write(f"**Category:** {doc.get('category')}")
                    st.write(f"**Type:** {doc.get('doc_type')}")
                    st.write(f"**Date:** {doc.get('doc_date', 'N/A')}")

                with col2:
                    url = doc.get("url")
                    if url:
                        st.markdown(f"**URL:** [{url[:40]}...]({url})")
                    st.write(f"**Fetched:** {doc.get('fetched_at', 'N/A')}")
                    st.write(f"**Parsed:** {'Yes' if doc.get('parsed') else 'No'}")

    else:
        st.info("No documents found. Click 'Sync Documents' to fetch from SBP.")

        # Show preview of what would be fetched
        with st.expander("Preview: Sample Documents"):
            sample_docs = get_sample_pma_documents()
            preview_data = []
            for doc in sample_docs:
                preview_data.append({
                    "Title": doc.title,
                    "Category": doc.category,
                    "Type": doc.doc_type,
                    "Date": doc.doc_date,
                })
            st.dataframe(pd.DataFrame(preview_data), use_container_width=True, hide_index=True)

    render_footer()


def psx_debt_market_page():
    """PSX Debt Market - Live debt securities from PSX DPS with full metrics."""
    import pandas as pd
    import plotly.graph_objects as go

    from psx_ohlcv.sources.psx_debt import (
        DEBT_CATEGORIES,
        DebtSecurity,
        fetch_all_debt_securities,
        fetch_debt_ohlcv,
        fetch_debt_security_detail,
        get_securities_flat_list,
        get_securities_summary,
        parse_symbol_info,
    )
    from psx_ohlcv.fi_analytics import (
        analyze_security,
        build_yield_curve,
        FREQ_SEMI_ANNUAL,
        FREQ_ZERO,
    )

    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 📈 PSX Debt Market")
        st.caption("T-Bills, PIBs, Sukuk, TFCs from PSX Data Portal")
    with header_col2:
        st.markdown(
            '<div class="data-info">💹 Live Data</div>',
            unsafe_allow_html=True
        )

    # =================================================================
    # FETCH DATA FROM PSX
    # =================================================================
    with st.spinner("Loading debt securities from PSX..."):
        securities_by_cat = fetch_all_debt_securities()

    if not any(securities_by_cat.values()):
        st.error("Could not fetch debt securities from PSX. Please try again later.")
        render_footer()
        return

    # Get summary
    summary = get_securities_summary(securities_by_cat)
    all_securities = get_securities_flat_list(securities_by_cat)

    # =================================================================
    # MARKET OVERVIEW KPIs
    # =================================================================
    st.markdown("### 📊 Market Overview")

    kpi_cols = st.columns(5)
    with kpi_cols[0]:
        st.metric("Total Securities", summary["total"])
    with kpi_cols[1]:
        st.metric("Government", summary["government"])
    with kpi_cols[2]:
        st.metric("Corporate", summary["corporate"])
    with kpi_cols[3]:
        st.metric("Islamic/Sukuk", summary["islamic"])
    with kpi_cols[4]:
        # Show by type breakdown
        type_count = len(summary.get("by_type", {}))
        st.metric("Security Types", type_count)

    # Category breakdown
    cat_cols = st.columns(4)
    for i, (cat_code, cat_name) in enumerate(DEBT_CATEGORIES.items()):
        with cat_cols[i]:
            count = len(securities_by_cat.get(cat_code, []))
            st.metric(cat_name, count)

    # =================================================================
    # CATEGORY TABS (matching PSX page structure)
    # =================================================================
    st.markdown("---")

    # Create tabs for each PSX category
    tab_names = [f"{DEBT_CATEGORIES[cat]} ({len(securities_by_cat[cat])})" for cat in DEBT_CATEGORIES]
    tab_names.append("📈 Price Chart")
    tab_names.append("📊 Security Analytics")  # Enhanced with Bloomberg-style metrics
    tab_names.append("📉 Yield Curve")  # New yield curve tab

    tabs = st.tabs(tab_names)

    # --- Category Tabs (gop, pds, cds, gds) ---
    for idx, cat_code in enumerate(DEBT_CATEGORIES.keys()):
        with tabs[idx]:
            cat_securities = securities_by_cat.get(cat_code, [])
            cat_name = DEBT_CATEGORIES[cat_code]

            if not cat_securities:
                st.info(f"No {cat_name} securities available")
                continue

            st.markdown(f"### {cat_name}")

            # Build table with all metrics
            table_data = []
            for sec in cat_securities:
                row = {
                    "Security Code": sec.symbol,
                    "Security Name": (sec.name or "")[:40],
                    "Face Value": f"{sec.face_value:,.0f}" if sec.face_value else "N/A",
                    "Listing Date": sec.listing_date or "N/A",
                    "Issue Date": sec.issue_date or "N/A",
                    "Issue Size": sec.issue_size or "N/A",
                    "Maturity Date": sec.maturity_date or "N/A",
                }

                # Add coupon info if available
                if sec.coupon_rate is not None:
                    row["Coupon Rate"] = f"{sec.coupon_rate:.4f}%"
                if sec.prev_coupon_date:
                    row["Prev Coupon"] = sec.prev_coupon_date
                if sec.next_coupon_date:
                    row["Next Coupon"] = sec.next_coupon_date

                row["Outstanding Days"] = sec.outstanding_days if sec.outstanding_days else "N/A"
                row["Remaining Yrs"] = f"{sec.remaining_years:.1f}" if sec.remaining_years else "N/A"

                # Add derived fields
                if sec.is_islamic:
                    row["Islamic"] = "✓"

                table_data.append(row)

            if table_data:
                df = pd.DataFrame(table_data)
                st.dataframe(df, use_container_width=True, hide_index=True)
                st.caption(f"Total: {len(table_data)} securities")

    # --- Price Chart Tab ---
    with tabs[4]:
        st.markdown("### 📈 Price History")

        chart_col1, chart_col2 = st.columns([3, 1])

        # Get all symbols for selector
        all_symbols = [s.symbol for s in all_securities]

        with chart_col2:
            selected_symbol = st.selectbox(
                "Select Security",
                options=all_symbols[:150],
                key="debt_chart_symbol"
            )

            if selected_symbol:
                # Find the security in our data
                sec = next((s for s in all_securities if s.symbol == selected_symbol), None)
                if sec:
                    st.markdown("**Security Info:**")
                    st.write(f"**Name:** {sec.name or 'N/A'}")
                    st.write(f"**Type:** {sec.security_type or 'N/A'}")
                    st.write(f"**Category:** {sec.category_name or 'N/A'}")
                    if sec.face_value:
                        st.write(f"**Face Value:** Rs. {sec.face_value:,.0f}")
                    if sec.coupon_rate is not None:
                        st.write(f"**Coupon:** {sec.coupon_rate:.4f}%")
                    if sec.maturity_date:
                        st.write(f"**Maturity:** {sec.maturity_date}")
                    if sec.outstanding_days:
                        st.write(f"**Days Left:** {sec.outstanding_days}")
                    if sec.is_islamic:
                        st.success("✓ Shariah Compliant")

        with chart_col1:
            if selected_symbol:
                with st.spinner(f"Loading price data for {selected_symbol}..."):
                    ohlcv = fetch_debt_ohlcv(selected_symbol)

                if ohlcv:
                    df = pd.DataFrame(ohlcv)
                    df["date"] = pd.to_datetime(df["date"])
                    df = df.sort_values("date")

                    # Price chart
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=df["date"],
                        y=df["price"],
                        mode="lines",
                        name="Price",
                        line=dict(color="#00d4aa", width=2),
                    ))

                    fig.update_layout(
                        title=f"{selected_symbol} Price History",
                        xaxis_title="Date",
                        yaxis_title="Price (PKR)",
                        template="plotly_dark",
                        height=400,
                    )
                    st.plotly_chart(fig, use_container_width=True)

                    # Volume chart
                    if df["volume"].sum() > 0:
                        vol_fig = go.Figure()
                        vol_fig.add_trace(go.Bar(
                            x=df["date"],
                            y=df["volume"],
                            name="Volume",
                            marker_color="#ff6b6b",
                        ))
                        vol_fig.update_layout(
                            title="Trading Volume",
                            xaxis_title="Date",
                            yaxis_title="Volume",
                            template="plotly_dark",
                            height=200,
                        )
                        st.plotly_chart(vol_fig, use_container_width=True)

                    # Stats
                    stat_cols = st.columns(4)
                    with stat_cols[0]:
                        st.metric("Latest", f"{df['price'].iloc[-1]:.4f}")
                    with stat_cols[1]:
                        st.metric("High", f"{df['price'].max():.4f}")
                    with stat_cols[2]:
                        st.metric("Low", f"{df['price'].min():.4f}")
                    with stat_cols[3]:
                        if len(df) > 1:
                            chg = ((df['price'].iloc[-1] - df['price'].iloc[0]) / df['price'].iloc[0]) * 100
                            st.metric("Change", f"{chg:.2f}%")
                else:
                    st.warning(f"No price data available for {selected_symbol}")

    # --- Security Analytics Tab (Bloomberg-style) ---
    with tabs[5]:
        st.markdown("### 📊 Security Analytics")
        st.caption("Bloomberg-style yield and risk metrics")

        detail_symbol = st.selectbox(
            "Select Security",
            options=all_symbols[:150],
            key="debt_detail_symbol"
        )

        if detail_symbol:
            # First check our scraped data
            sec = next((s for s in all_securities if s.symbol == detail_symbol), None)

            if sec:
                # Fetch price data for analytics
                ohlcv = fetch_debt_ohlcv(sec.symbol)
                latest_price = ohlcv[0]['price'] if ohlcv else None

                # Basic Info Section
                col1, col2, col3 = st.columns(3)

                with col1:
                    st.markdown("#### Security Info")
                    st.write(f"**Symbol:** {sec.symbol}")
                    st.write(f"**Name:** {sec.name or 'N/A'}")
                    st.write(f"**Type:** {sec.security_type or 'N/A'}")
                    st.write(f"**Category:** {sec.category_name or 'N/A'}")
                    if sec.is_islamic:
                        st.success("✓ Shariah Compliant")

                with col2:
                    st.markdown("#### Issue Details")
                    if sec.face_value:
                        st.write(f"**Face Value:** Rs. {sec.face_value:,.0f}")
                    if sec.issue_size:
                        st.write(f"**Issue Size:** {sec.issue_size}")
                    if sec.issue_date:
                        st.write(f"**Issue Date:** {sec.issue_date}")
                    if sec.maturity_date:
                        st.write(f"**Maturity:** {sec.maturity_date}")

                with col3:
                    st.markdown("#### Coupon/Rental")
                    if sec.coupon_rate is not None and sec.coupon_rate > 0:
                        st.write(f"**Rate:** {sec.coupon_rate:.4f}%")
                        st.write(f"**Frequency:** Semi-Annual")
                    else:
                        st.write("**Type:** Zero Coupon/Discount")
                    if sec.prev_coupon_date:
                        st.write(f"**Last Coupon:** {sec.prev_coupon_date}")
                    if sec.next_coupon_date:
                        st.write(f"**Next Coupon:** {sec.next_coupon_date}")

                st.markdown("---")

                # Calculate Analytics if we have price
                if latest_price and sec.maturity_date and sec.outstanding_days and sec.outstanding_days > 0:
                    # Determine frequency
                    coupon_pct = sec.coupon_rate if sec.coupon_rate else 0
                    freq = FREQ_ZERO if coupon_pct == 0 else FREQ_SEMI_ANNUAL
                    coupon = coupon_pct / 100  # Convert to decimal

                    # Run analytics
                    analytics = analyze_security(
                        symbol=sec.symbol,
                        name=sec.name,
                        security_type=sec.security_type,
                        face_value=sec.face_value or 5000,
                        coupon_rate=coupon,
                        maturity_date=sec.maturity_date,
                        price=latest_price,
                        prev_coupon_date=sec.prev_coupon_date,
                        frequency=freq,
                        price_is_per_100=True,
                    )

                    st.markdown("### 📈 Yield & Risk Analytics (YAS)")
                    st.caption("Bloomberg-style Yield Analysis")

                    # Price & Yield Metrics
                    yield_cols = st.columns(4)
                    with yield_cols[0]:
                        st.metric("Clean Price", f"{latest_price:.4f}")
                    with yield_cols[1]:
                        if analytics.yield_metrics and analytics.yield_metrics.ytm is not None:
                            ytm_pct = analytics.yield_metrics.ytm * 100
                            st.metric("YTM", f"{ytm_pct:.2f}%")
                        else:
                            st.metric("YTM", "N/A")
                    with yield_cols[2]:
                        if analytics.yield_metrics and analytics.yield_metrics.discount_yield is not None:
                            dy_pct = analytics.yield_metrics.discount_yield * 100
                            st.metric("Discount Yield", f"{dy_pct:.2f}%")
                        elif analytics.yield_metrics and analytics.yield_metrics.current_yield is not None:
                            cy_pct = analytics.yield_metrics.current_yield * 100
                            st.metric("Current Yield", f"{cy_pct:.2f}%")
                        else:
                            st.metric("Current Yield", "N/A")
                    with yield_cols[3]:
                        if analytics.yield_metrics and analytics.yield_metrics.bey is not None:
                            bey_pct = analytics.yield_metrics.bey * 100
                            st.metric("BEY", f"{bey_pct:.2f}%")
                        else:
                            st.metric("BEY", "N/A")

                    # Duration & Risk Metrics
                    st.markdown("### 📊 Duration & Risk (DUR)")
                    st.caption("Bloomberg-style Duration Analysis")

                    dur_cols = st.columns(4)
                    with dur_cols[0]:
                        if analytics.years_to_maturity:
                            st.metric("Years to Mat", f"{analytics.years_to_maturity:.2f}")
                        else:
                            st.metric("Years to Mat", "N/A")
                    with dur_cols[1]:
                        if analytics.duration_metrics and analytics.duration_metrics.macaulay_dur is not None:
                            st.metric("Macaulay Dur", f"{analytics.duration_metrics.macaulay_dur:.2f}")
                        else:
                            st.metric("Macaulay Dur", "N/A")
                    with dur_cols[2]:
                        if analytics.duration_metrics and analytics.duration_metrics.modified_dur is not None:
                            st.metric("Modified Dur", f"{analytics.duration_metrics.modified_dur:.2f}")
                        else:
                            st.metric("Modified Dur", "N/A")
                    with dur_cols[3]:
                        if analytics.duration_metrics and analytics.duration_metrics.dv01 is not None:
                            st.metric("DV01", f"{analytics.duration_metrics.dv01:.4f}")
                        else:
                            st.metric("DV01", "N/A")

                    # Analytics Table
                    with st.expander("📋 Full Analytics Details"):
                        analytics_data = {
                            "Metric": [],
                            "Value": [],
                            "Description": [],
                        }

                        # Add all metrics
                        analytics_data["Metric"].append("Symbol")
                        analytics_data["Value"].append(sec.symbol)
                        analytics_data["Description"].append("Security identifier")

                        analytics_data["Metric"].append("Clean Price")
                        analytics_data["Value"].append(f"{latest_price:.4f}")
                        analytics_data["Description"].append("Price excluding accrued interest")

                        if analytics.yield_metrics:
                            ym = analytics.yield_metrics
                            if ym.ytm is not None:
                                analytics_data["Metric"].append("YTM")
                                analytics_data["Value"].append(f"{ym.ytm*100:.4f}%")
                                analytics_data["Description"].append("Yield to Maturity (annualized)")
                            if ym.discount_yield is not None:
                                analytics_data["Metric"].append("Discount Yield")
                                analytics_data["Value"].append(f"{ym.discount_yield*100:.4f}%")
                                analytics_data["Description"].append("Money market discount yield")
                            if ym.bey is not None:
                                analytics_data["Metric"].append("BEY")
                                analytics_data["Value"].append(f"{ym.bey*100:.4f}%")
                                analytics_data["Description"].append("Bond Equivalent Yield")
                            if ym.current_yield is not None:
                                analytics_data["Metric"].append("Current Yield")
                                analytics_data["Value"].append(f"{ym.current_yield*100:.4f}%")
                                analytics_data["Description"].append("Annual coupon / price")

                        if analytics.duration_metrics:
                            dm = analytics.duration_metrics
                            if dm.macaulay_dur is not None:
                                analytics_data["Metric"].append("Macaulay Duration")
                                analytics_data["Value"].append(f"{dm.macaulay_dur:.4f} years")
                                analytics_data["Description"].append("Weighted avg time to cash flows")
                            if dm.modified_dur is not None:
                                analytics_data["Metric"].append("Modified Duration")
                                analytics_data["Value"].append(f"{dm.modified_dur:.4f}")
                                analytics_data["Description"].append("Price sensitivity to yield changes")
                            if dm.dv01 is not None:
                                analytics_data["Metric"].append("DV01")
                                analytics_data["Value"].append(f"{dm.dv01:.6f}")
                                analytics_data["Description"].append("Dollar value of 1bp yield change")
                            if dm.convexity is not None:
                                analytics_data["Metric"].append("Convexity")
                                analytics_data["Value"].append(f"{dm.convexity:.4f}")
                                analytics_data["Description"].append("Second-order price sensitivity")

                        df_analytics = pd.DataFrame(analytics_data)
                        st.dataframe(df_analytics, use_container_width=True, hide_index=True)

                else:
                    st.info("Select a security with available price data to view analytics")

                # Maturity Progress
                st.markdown("---")
                st.markdown("#### Time to Maturity")
                if sec.outstanding_days is not None:
                    if sec.outstanding_days > 0:
                        st.write(f"**Outstanding Days:** {sec.outstanding_days}")
                        # Progress bar
                        if sec.tenor_years and sec.tenor_years > 0:
                            original_days = sec.tenor_years * 365
                            elapsed = original_days - sec.outstanding_days
                            pct_complete = min(1.0, max(0.0, elapsed / original_days))
                            st.progress(pct_complete)
                            st.caption(f"{pct_complete*100:.1f}% of original {sec.tenor_years}Y tenor elapsed")
                    else:
                        st.error("MATURED")
                if sec.remaining_years is not None:
                    st.write(f"**Remaining Years:** {sec.remaining_years:.2f}")
            else:
                st.warning("Security not found in data")

    # --- Yield Curve Tab ---
    with tabs[6]:
        st.markdown("### 📉 PKR Yield Curve")
        st.caption("Government Securities Yield Curve Analysis")

        # Build yield curves from available data
        curve_col1, curve_col2 = st.columns([3, 1])

        with curve_col2:
            curve_type = st.selectbox(
                "Curve Type",
                ["Government (PIB + T-Bill)", "Sukuk (GIS + FRR)", "All Securities"],
                key="yield_curve_type"
            )

        with curve_col1:
            # Filter securities based on curve type
            if curve_type == "Government (PIB + T-Bill)":
                curve_securities = [s for s in all_securities
                                  if s.security_type in ["PIB", "T-Bill", "Floating"]
                                  and s.outstanding_days and s.outstanding_days > 30]
            elif curve_type == "Sukuk (GIS + FRR)":
                curve_securities = [s for s in all_securities
                                  if s.is_islamic and s.outstanding_days and s.outstanding_days > 30]
            else:
                curve_securities = [s for s in all_securities
                                  if s.outstanding_days and s.outstanding_days > 30]

            st.info(f"Building curve from {len(curve_securities)} securities with price data...")

            # Calculate YTM for each security
            curve_data = []
            for sec in curve_securities:
                ohlcv = fetch_debt_ohlcv(sec.symbol)
                if ohlcv and sec.maturity_date:
                    price = ohlcv[0]['price']
                    coupon_pct = sec.coupon_rate if sec.coupon_rate else 0
                    freq = FREQ_ZERO if coupon_pct == 0 else FREQ_SEMI_ANNUAL

                    analytics = analyze_security(
                        symbol=sec.symbol,
                        name=sec.name,
                        security_type=sec.security_type,
                        face_value=sec.face_value or 5000,
                        coupon_rate=coupon_pct / 100,
                        maturity_date=sec.maturity_date,
                        price=price,
                        frequency=freq,
                        price_is_per_100=True,
                    )

                    if analytics.yield_metrics and analytics.yield_metrics.ytm is not None:
                        ytm = analytics.yield_metrics.ytm
                        # Filter out extreme values
                        if 0 < ytm < 0.5:  # 0-50% yield range
                            curve_data.append({
                                "symbol": sec.symbol,
                                "name": sec.name,
                                "type": sec.security_type,
                                "years_to_maturity": analytics.years_to_maturity,
                                "ytm": ytm,
                                "price": price,
                            })

            if curve_data:
                # Sort by maturity
                curve_data.sort(key=lambda x: x["years_to_maturity"])
                df_curve = pd.DataFrame(curve_data)

                # Plot yield curve
                fig = go.Figure()

                # Scatter plot with symbols
                fig.add_trace(go.Scatter(
                    x=df_curve["years_to_maturity"],
                    y=df_curve["ytm"] * 100,  # Convert to percentage
                    mode="markers+text",
                    text=df_curve["type"],
                    textposition="top center",
                    textfont=dict(size=8),
                    marker=dict(
                        size=10,
                        color=df_curve["ytm"] * 100,
                        colorscale="Viridis",
                        showscale=True,
                        colorbar=dict(title="YTM %"),
                    ),
                    hovertemplate=(
                        "<b>%{customdata[0]}</b><br>" +
                        "Maturity: %{x:.2f}Y<br>" +
                        "YTM: %{y:.2f}%<br>" +
                        "Price: %{customdata[1]:.2f}<extra></extra>"
                    ),
                    customdata=list(zip(df_curve["symbol"], df_curve["price"])),
                ))

                # Add smoothed curve line
                if len(df_curve) > 2:
                    fig.add_trace(go.Scatter(
                        x=df_curve["years_to_maturity"],
                        y=df_curve["ytm"] * 100,
                        mode="lines",
                        line=dict(color="#00d4aa", width=2, dash="dash"),
                        name="Trend",
                    ))

                fig.update_layout(
                    title=f"PKR {curve_type} Yield Curve",
                    xaxis_title="Years to Maturity",
                    yaxis_title="Yield to Maturity (%)",
                    template="plotly_dark",
                    height=500,
                    showlegend=False,
                )
                st.plotly_chart(fig, use_container_width=True)

                # Curve statistics
                stat_cols = st.columns(4)
                short_term = [d["ytm"] for d in curve_data if d["years_to_maturity"] < 1]
                med_term = [d["ytm"] for d in curve_data if 1 <= d["years_to_maturity"] < 5]
                long_term = [d["ytm"] for d in curve_data if d["years_to_maturity"] >= 5]

                with stat_cols[0]:
                    if short_term:
                        avg_st = sum(short_term) / len(short_term) * 100
                        st.metric("Short Term (<1Y)", f"{avg_st:.2f}%")
                with stat_cols[1]:
                    if med_term:
                        avg_mt = sum(med_term) / len(med_term) * 100
                        st.metric("Medium Term (1-5Y)", f"{avg_mt:.2f}%")
                with stat_cols[2]:
                    if long_term:
                        avg_lt = sum(long_term) / len(long_term) * 100
                        st.metric("Long Term (>5Y)", f"{avg_lt:.2f}%")
                with stat_cols[3]:
                    if short_term and long_term:
                        spread = (sum(long_term)/len(long_term) - sum(short_term)/len(short_term)) * 10000
                        st.metric("Curve Spread (bps)", f"{spread:.0f}")

                # Data table
                with st.expander("📋 Curve Points Data"):
                    display_df = df_curve.copy()
                    display_df["ytm_pct"] = display_df["ytm"] * 100
                    display_df = display_df[["symbol", "type", "years_to_maturity", "ytm_pct", "price"]]
                    display_df.columns = ["Symbol", "Type", "Years to Mat", "YTM %", "Price"]
                    st.dataframe(display_df, use_container_width=True, hide_index=True)
            else:
                st.warning("Not enough securities with price data to build yield curve")

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

    # Theme toggle
    theme_options = {
        "bloomberg": "Bloomberg Terminal",
        "default": "Default Trading",
    }
    current_theme = get_current_theme()
    selected_theme = st.sidebar.selectbox(
        "Theme",
        options=list(theme_options.keys()),
        format_func=lambda x: theme_options[x],
        index=list(theme_options.keys()).index(current_theme),
        key="theme_selector",
        label_visibility="collapsed",
    )
    if selected_theme != current_theme:
        set_theme(selected_theme)
        st.rerun()

    st.sidebar.markdown("---")

    # =================================================================
    # BLOOMBERG-STYLE GROUPED NAVIGATION
    # =================================================================

    # Navigation groups - Bloomberg Terminal style
    nav_groups = {
        "MARKET": [
            "📊 Dashboard",
            "📈 Market Summary",
        ],
        "EQUITY": [
            "📈 Quote Monitor",      # Regular Market
            "📊 Price Chart",        # Candlestick Explorer
            "⏱ Intraday",            # Intraday Trend
            "🏢 Company",            # Company Analytics
            "🏆 Rankings",
            "📊 Factors",            # Factor Analysis
            "🧵 Symbols",
        ],
        "INDICES": [
            "📊 Index Monitor",      # Indices
            "📦 Instruments",
        ],
        "FIXED INCOME": [
            "📈 FI Overview",        # PSX Debt Market
            "🧾 Bond Search",        # Bonds Screener
            "📉 Yield Curve",
            "🕌 Sukuk",              # Sukuk Screener
            "🏛️ SBP Auctions",       # SBP Archive
        ],
        "FX": [
            "🌍 FX Monitor",         # FX Overview
            "📊 FX Analytics",       # FX Impact
        ],
        "FUNDS": [
            "🏦 Fund Directory",     # Mutual Funds
            "📊 Fund Analytics",
        ],
        "DATA": [
            "📥 Data Sync",          # Data Acquisition
            "📚 History",
            "🤖 AI Insights",
            "🔄 Sync Monitor",
        ],
        "ADMIN": [
            "📋 Schema",
            "⚙️ Settings",
        ],
    }

    # Build flat pages list for routing
    all_pages = []
    for group_pages in nav_groups.values():
        all_pages.extend(group_pages)

    # Map old page names to new names for backwards compatibility
    page_mapping = {
        "📊 Regular Market": "📈 Quote Monitor",
        "📈 Candlestick Explorer": "📊 Price Chart",
        "⏱ Intraday Trend": "⏱ Intraday",
        "🏢 Company Analytics": "🏢 Company",
        "📊 Factor Analysis": "📊 Factors",
        "📊 Indices": "📊 Index Monitor",
        "📈 PSX Debt Market": "📈 FI Overview",
        "🧾 Bonds Screener": "🧾 Bond Search",
        "🕌 Sukuk Screener": "🕌 Sukuk",
        "🏛️ SBP Archive": "🏛️ SBP Auctions",
        "🌍 FX Overview": "🌍 FX Monitor",
        "📊 FX Impact": "📊 FX Analytics",
        "🏦 Mutual Funds": "🏦 Fund Directory",
        "📥 Data Acquisition": "📥 Data Sync",
        "📥 Market Summary": "📈 Market Summary",
    }

    # Initialize current page in session state
    if "current_page" not in st.session_state:
        st.session_state.current_page = "📊 Dashboard"

    # Handle programmatic navigation (nav_to from other pages)
    if "nav_to" in st.session_state and st.session_state.nav_to:
        nav_target = st.session_state.nav_to
        nav_target = page_mapping.get(nav_target, nav_target)
        if nav_target in all_pages:
            st.session_state.current_page = nav_target
        st.session_state.nav_to = None

    # Render grouped navigation with section headers
    for group_name, group_pages in nav_groups.items():
        # Section header with Bloomberg-style formatting
        st.sidebar.markdown(
            f'<div style="font-size: 10px; font-weight: 600; color: #ff9800; '
            f'letter-spacing: 1px; margin: 12px 0 4px 0; padding: 4px 0; '
            f'border-bottom: 1px solid rgba(255,152,0,0.3);">{group_name}</div>',
            unsafe_allow_html=True
        )

        # Pages in this group - use buttons with session state
        for page_name in group_pages:
            is_selected = (page_name == st.session_state.current_page)

            # Custom styled button for each page
            if st.sidebar.button(
                page_name,
                key=f"nav_{page_name}",
                use_container_width=True,
                type="primary" if is_selected else "secondary",
            ):
                st.session_state.current_page = page_name
                st.rerun()

    # Get selected page from session state
    page = st.session_state.current_page

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

    # =================================================================
    # PAGE ROUTING - Maps new Bloomberg-style names to existing functions
    # =================================================================

    # Page function mapping
    page_functions = {
        # MARKET
        "📊 Dashboard": dashboard,
        "📈 Market Summary": market_summary_page,

        # EQUITY
        "📈 Quote Monitor": regular_market_page,
        "📊 Price Chart": candlestick_explorer,
        "⏱ Intraday": intraday_trend_page,
        "🏢 Company": company_analytics_page,
        "🏆 Rankings": rankings_page,
        "📊 Factors": factor_analysis_page,
        "🧵 Symbols": symbols_page,

        # INDICES
        "📊 Index Monitor": indices_analytics_page,
        "📦 Instruments": instruments_page,

        # FIXED INCOME
        "📈 FI Overview": psx_debt_market_page,
        "🧾 Bond Search": bonds_screener_page,
        "📉 Yield Curve": yield_curve_page,
        "🕌 Sukuk": sukuk_screener_page,
        "🏛️ SBP Auctions": sbp_auction_archive_page,

        # FX
        "🌍 FX Monitor": fx_overview_page,
        "📊 FX Analytics": fx_impact_page,

        # FUNDS
        "🏦 Fund Directory": mutual_funds_page,
        "📊 Fund Analytics": fund_analytics_page,

        # DATA
        "📥 Data Sync": data_acquisition_page,
        "📚 History": history_page,
        "🤖 AI Insights": ai_insights_page,
        "🔄 Sync Monitor": sync_monitor,

        # ADMIN
        "📋 Schema": schema_page,
        "⚙️ Settings": settings_page,
    }

    # Execute the selected page function
    if page in page_functions:
        page_functions[page]()
    else:
        st.error(f"Page not found: {page}")


if __name__ == "__main__":
    main()
