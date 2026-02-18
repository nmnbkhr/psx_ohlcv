"""Shared helper functions used across multiple pages.

Contains formatting, rendering, theme, DB connection, and data freshness
helpers extracted from app.py.
"""

from datetime import datetime

import pandas as pd
import streamlit as st

from psx_ohlcv import init_schema
from psx_ohlcv.config import DATA_ROOT, get_db_path
from psx_ohlcv.db import get_sector_map
from psx_ohlcv.ui.themes import get_theme_css, THEME_NAMES, ThemeName
from psx_ohlcv.ui.session_tracker import (
    get_session_id,
    render_session_activity_panel,
)


# =============================================================================
# THEME SYSTEM
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


# =============================================================================
# FORMATTING HELPERS
# =============================================================================

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


# =============================================================================
# RENDERING HELPERS
# =============================================================================

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


def render_data_warning(message: str, icon: str = "\u26a0\ufe0f"):
    """Render a warning banner for data issues."""
    st.markdown(
        f'<div class="data-warning">{icon} {message}</div>',
        unsafe_allow_html=True
    )


def render_data_info(message: str, icon: str = "\u2139\ufe0f"):
    """Render an info banner."""
    st.markdown(
        f'<div class="data-info">{icon} {message}</div>',
        unsafe_allow_html=True
    )


def render_data_error(message: str, icon: str = "\u274c"):
    """Render an error banner for failed operations."""
    st.markdown(
        f'<div class="data-error">{icon} {message}</div>',
        unsafe_allow_html=True
    )


def render_empty_state(message: str, icon: str = "\U0001f4ed"):
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
        return f'<span class="data-fresh">\u2713 Today ({date_str})</span>'
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
            render_empty_state(empty_message, "\U0001f4ed")
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


# =============================================================================
# CONSTANTS
# =============================================================================

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


# =============================================================================
# DATABASE CONNECTION HELPERS
# =============================================================================

def get_connection():
    """
    Get database connection, initializing schema if needed.

    Note: We don't cache the connection because SQLite connections
    are not thread-safe by default. Streamlit runs in multiple threads.
    """
    import sqlite3 as _sqlite3

    db_path = get_db_path()
    # Use check_same_thread=False to allow connection to be used across threads
    # timeout=30 waits up to 30s if DB is locked (e.g. during bulk sync)
    con = _sqlite3.connect(str(db_path), check_same_thread=False, timeout=30)
    con.row_factory = _sqlite3.Row

    # Enable WAL mode for better concurrent access
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=30000")

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
