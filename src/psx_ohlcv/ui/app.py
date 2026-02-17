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
from psx_ohlcv.ui.chat import chat_page

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


# Theme initialised inside main() — no module-level st.markdown() calls
# to avoid rendering outside the st.navigation() page lifecycle.


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
# Page: Live Market
# -----------------------------------------------------------------------------
def live_market_page():
    from psx_ohlcv.ui.page_views.live_market import render_live_market
    render_live_market()


# -----------------------------------------------------------------------------
# Page: Data Quality
# -----------------------------------------------------------------------------
def data_quality_page():
    from psx_ohlcv.ui.page_views.data_quality import render_data_quality
    render_data_quality()


# -----------------------------------------------------------------------------
# Page: Dashboard
# -----------------------------------------------------------------------------
def dashboard():
    from psx_ohlcv.ui.page_views.dashboard import render_dashboard
    render_dashboard()


# -----------------------------------------------------------------------------
# Page: Candlestick Explorer
# -----------------------------------------------------------------------------
def candlestick_explorer():
    from psx_ohlcv.ui.page_views.candlestick import render_candlestick
    render_candlestick()


# -----------------------------------------------------------------------------
# Page: Intraday Trend
# -----------------------------------------------------------------------------
def intraday_trend_page():
    from psx_ohlcv.ui.page_views.intraday import render_intraday
    render_intraday()


# -----------------------------------------------------------------------------
# Page: Regular Market Watch
# -----------------------------------------------------------------------------
def regular_market_page():
    from psx_ohlcv.ui.page_views.regular_market import render_regular_market
    render_regular_market()


# -----------------------------------------------------------------------------
# Page: Symbols
# -----------------------------------------------------------------------------
def symbols_page():
    from psx_ohlcv.ui.page_views.symbols import render_symbols
    render_symbols()


# -----------------------------------------------------------------------------
# Page: Schema - Database schema documentation and SQL scripts
# -----------------------------------------------------------------------------
def schema_page():
    from psx_ohlcv.ui.page_views.schema import render_schema
    render_schema()


# -----------------------------------------------------------------------------
# Page: Settings
# -----------------------------------------------------------------------------
def settings_page():
    from psx_ohlcv.ui.page_views.settings import render_settings
    render_settings()


# -----------------------------------------------------------------------------
# Page: History
# -----------------------------------------------------------------------------
def history_page():
    from psx_ohlcv.ui.page_views.history import render_history
    render_history()


# -----------------------------------------------------------------------------
# Page: EOD Data Loader
# -----------------------------------------------------------------------------
def eod_data_loader_page():
    from psx_ohlcv.ui.page_views.eod_loader import render_eod_loader
    render_eod_loader()


def _eod_data_loader_page_impl():
    from psx_ohlcv.ui.page_views.eod_loader import _eod_data_loader_page_impl
    _eod_data_loader_page_impl()


# -----------------------------------------------------------------------------
# Page: Company Analytics
# -----------------------------------------------------------------------------
def company_analytics_page():
    from psx_ohlcv.ui.page_views.company_deep import render_company_deep
    render_company_deep()


# -----------------------------------------------------------------------------
# Page: Data Acquisition - Bulk Data Scraping & Collection
# -----------------------------------------------------------------------------
def data_acquisition_page():
    from psx_ohlcv.ui.page_views.data_acquisition import render_data_acquisition
    render_data_acquisition()


# -----------------------------------------------------------------------------
# Page: Factor Analysis - Quantitative Factor Rankings & Analysis
# -----------------------------------------------------------------------------
def factor_analysis_page():
    from psx_ohlcv.ui.page_views.factor_analysis import render_factor_analysis
    render_factor_analysis()


# -----------------------------------------------------------------------------
# Page: AI Insights (GPT-5.2 powered analysis)
# -----------------------------------------------------------------------------
def ai_insights_page():
    from psx_ohlcv.ui.page_views.ai_insights import render_ai_insights
    render_ai_insights()


# -----------------------------------------------------------------------------
# Page: Market Summary
# -----------------------------------------------------------------------------
def market_summary_page():
    from psx_ohlcv.ui.page_views.market_summary import render_market_summary
    render_market_summary()


# -----------------------------------------------------------------------------
# Page: Sync Monitor
# -----------------------------------------------------------------------------
def sync_monitor():
    from psx_ohlcv.ui.page_views.sync_monitor import render_sync_monitor
    render_sync_monitor()


# =============================================================================
# Phase 1: Instruments Page
# =============================================================================
def instruments_page():
    from psx_ohlcv.ui.page_views.instruments import render_instruments
    render_instruments()


# =============================================================================
# Phase 1: Rankings Page
# =============================================================================
def rankings_page():
    from psx_ohlcv.ui.page_views.rankings import render_rankings
    render_rankings()


# =============================================================================
# Phase 1: Index Analytics Page
# =============================================================================
def indices_analytics_page():
    from psx_ohlcv.ui.page_views.indices import render_indices
    render_indices()


# =============================================================================
# Phase 2: FX Overview Page
# =============================================================================
def fx_overview_page():
    from psx_ohlcv.ui.page_views.fx import render_fx_overview
    render_fx_overview()


# =============================================================================
# Phase 2: FX Impact Page
# =============================================================================
def fx_impact_page():
    from psx_ohlcv.ui.page_views.fx import render_fx_impact
    render_fx_impact()


# =============================================================================
# Phase 2.5: Mutual Funds Page
# =============================================================================
def mutual_funds_page():
    from psx_ohlcv.ui.page_views.funds import render_mutual_funds
    render_mutual_funds()


# =============================================================================
# Phase 2.5: Fund Analytics Page
# =============================================================================
def fund_analytics_page():
    from psx_ohlcv.ui.page_views.funds import render_fund_analytics
    render_fund_analytics()


# =============================================================================
# Phase 3: Bonds Screener Page
# =============================================================================
def bonds_screener_page():
    from psx_ohlcv.ui.page_views.fixed_income import render_bonds_screener
    render_bonds_screener()


# =============================================================================
# Phase 3: Yield Curve Page
# =============================================================================
def yield_curve_page():
    from psx_ohlcv.ui.page_views.fixed_income import render_yield_curve
    render_yield_curve()


# =============================================================================
# Phase 3: Sukuk Screener Page (Additive - separate from bonds)
# =============================================================================
def sukuk_screener_page():
    from psx_ohlcv.ui.page_views.fixed_income import render_sukuk_screener
    render_sukuk_screener()


# =============================================================================
# Phase 3: Sukuk Yield Curve Page
# =============================================================================
def sukuk_yield_curve_page():
    from psx_ohlcv.ui.page_views.fixed_income import render_sukuk_yield_curve
    render_sukuk_yield_curve()


# =============================================================================
# Phase 3: SBP Auction Archive Page
# =============================================================================
def sbp_auction_archive_page():
    from psx_ohlcv.ui.page_views.fixed_income import render_sbp_auction_archive
    render_sbp_auction_archive()


# =============================================================================
# Phase 3.5: Government Fixed Income Pages
# =============================================================================

def govt_fixed_income_page():
    from psx_ohlcv.ui.page_views.fixed_income import render_govt_fixed_income
    render_govt_fixed_income()


def fi_yield_curve_page():
    from psx_ohlcv.ui.page_views.fixed_income import render_fi_yield_curve
    render_fi_yield_curve()


def sbp_pma_archive_page():
    from psx_ohlcv.ui.page_views.fixed_income import render_sbp_pma_archive
    render_sbp_pma_archive()


def psx_debt_market_page():
    from psx_ohlcv.ui.page_views.fixed_income import render_psx_debt_market
    render_psx_debt_market()


def treasury_dashboard_page():
    from psx_ohlcv.ui.page_views.treasury_dashboard import render_treasury_dashboard
    render_treasury_dashboard()


def fx_dashboard_page():
    from psx_ohlcv.ui.page_views.fx_dashboard import render_fx_dashboard
    render_fx_dashboard()


def fund_explorer_page():
    from psx_ohlcv.ui.page_views.fund_explorer import render_fund_explorer
    render_fund_explorer()


def research_terminal_page():
    from psx_ohlcv.ui.page_views.research_terminal import render_research_terminal
    render_research_terminal()


def website_scan_page():
    from psx_ohlcv.ui.page_views.website_scan import render_website_scan
    render_website_scan()


# -----------------------------------------------------------------------------
# Main App — st.navigation() for framework-guaranteed page isolation
# -----------------------------------------------------------------------------
def main():
    """Main app using Streamlit's native st.navigation() API.

    Each page runs as an isolated unit — the framework guarantees that ONLY
    the selected page's render function executes per script run, eliminating
    cross-page content bleeding.
    """
    # Initialize session tracking
    init_session_tracking()

    # Inject theme CSS (shared across all pages)
    init_theme()
    inject_theme_css()

    # =================================================================
    # BUILD st.Page REGISTRY
    # =================================================================
    _pages = {
        # MARKET
        "📊 Dashboard":      st.Page(dashboard,              title="Dashboard",      url_path="dashboard",      default=True),
        "📡 Live Market":    st.Page(live_market_page,        title="Live Market",    url_path="live-market"),
        "📈 Market Summary": st.Page(market_summary_page,     title="Market Summary", url_path="market-summary"),
        # EQUITY
        "📈 Quote Monitor":  st.Page(regular_market_page,     title="Quote Monitor",  url_path="quote-monitor"),
        "📊 Price Chart":    st.Page(candlestick_explorer,    title="Price Chart",    url_path="price-chart"),
        "⏱ Intraday":       st.Page(intraday_trend_page,     title="Intraday",       url_path="intraday"),
        "🏢 Company":        st.Page(company_analytics_page,  title="Company",        url_path="company"),
        "🏆 Rankings":       st.Page(rankings_page,           title="Rankings",       url_path="rankings"),
        "📊 Factors":        st.Page(factor_analysis_page,    title="Factors",        url_path="factors"),
        "🧵 Symbols":        st.Page(symbols_page,            title="Symbols",        url_path="symbols"),
        # INDICES
        "📊 Index Monitor":  st.Page(indices_analytics_page,  title="Index Monitor",  url_path="index-monitor"),
        "📦 Instruments":    st.Page(instruments_page,        title="Instruments",    url_path="instruments"),
        # FIXED INCOME
        "📈 FI Overview":    st.Page(psx_debt_market_page,    title="FI Overview",    url_path="fi-overview"),
        "🧾 Bond Search":    st.Page(bonds_screener_page,     title="Bond Search",    url_path="bond-search"),
        "📉 Yield Curve":    st.Page(yield_curve_page,        title="Yield Curve",    url_path="yield-curve"),
        "🕌 Sukuk":          st.Page(sukuk_screener_page,     title="Sukuk",          url_path="sukuk"),
        "🏛️ SBP Auctions":   st.Page(sbp_auction_archive_page, title="SBP Auctions", url_path="sbp-auctions"),
        "🏦 Treasury":       st.Page(treasury_dashboard_page, title="Treasury",       url_path="treasury"),
        # FX
        "🌍 FX Monitor":     st.Page(fx_overview_page,        title="FX Monitor",     url_path="fx-monitor"),
        "📊 FX Analytics":   st.Page(fx_impact_page,          title="FX Analytics",   url_path="fx-analytics"),
        "💱 FX Dashboard":   st.Page(fx_dashboard_page,       title="FX Dashboard",   url_path="fx-dashboard"),
        # FUNDS
        "🏦 Fund Directory": st.Page(mutual_funds_page,       title="Fund Directory", url_path="fund-directory"),
        "📊 Fund Analytics": st.Page(fund_analytics_page,     title="Fund Analytics", url_path="fund-analytics"),
        "🔍 Fund Explorer":  st.Page(fund_explorer_page,      title="Fund Explorer",  url_path="fund-explorer"),
        # DATA
        "📥 Data Sync":      st.Page(data_acquisition_page,   title="Data Sync",      url_path="data-sync"),
        "📂 EOD Loader":     st.Page(eod_data_loader_page,    title="EOD Loader",     url_path="eod-loader"),
        "📚 History":        st.Page(history_page,             title="History",        url_path="history"),
        "🔄 Sync Monitor":   st.Page(sync_monitor,            title="Sync Monitor",   url_path="sync-monitor"),
        "🩺 Data Quality":   st.Page(data_quality_page,       title="Data Quality",   url_path="data-quality"),
        "🔗 Website Scan":   st.Page(website_scan_page,       title="Website Scan",   url_path="website-scan"),
        # AI
        "💬 AI Chat":        st.Page(chat_page,                title="AI Chat",        url_path="ai-chat"),
        "🤖 AI Insights":    st.Page(ai_insights_page,        title="AI Insights",    url_path="ai-insights"),
        # ADMIN
        "📋 Schema":         st.Page(schema_page,              title="Schema",         url_path="schema"),
        "🔬 Research":       st.Page(research_terminal_page,   title="Research",       url_path="research"),
        "⚙️ Settings":       st.Page(settings_page,            title="Settings",       url_path="settings"),
    }

    # Navigation groups — Bloomberg Terminal style
    nav_groups = {
        "MARKET":       ["📊 Dashboard", "📡 Live Market", "📈 Market Summary"],
        "EQUITY":       ["📈 Quote Monitor", "📊 Price Chart", "⏱ Intraday",
                         "🏢 Company", "🏆 Rankings", "📊 Factors", "🧵 Symbols"],
        "INDICES":      ["📊 Index Monitor", "📦 Instruments"],
        "FIXED INCOME": ["📈 FI Overview", "🧾 Bond Search", "📉 Yield Curve",
                         "🕌 Sukuk", "🏛️ SBP Auctions", "🏦 Treasury"],
        "FX":           ["🌍 FX Monitor", "📊 FX Analytics", "💱 FX Dashboard"],
        "FUNDS":        ["🏦 Fund Directory", "📊 Fund Analytics", "🔍 Fund Explorer"],
        "DATA":         ["📥 Data Sync", "📂 EOD Loader", "📚 History",
                         "🔄 Sync Monitor", "🩺 Data Quality", "🔗 Website Scan"],
        "AI":           ["💬 AI Chat", "🤖 AI Insights"],
        "ADMIN":        ["📋 Schema", "🔬 Research", "⚙️ Settings"],
    }

    # Build grouped dict of st.Page objects for st.navigation
    nav_dict = {}
    for group_name, page_names in nav_groups.items():
        nav_dict[group_name] = [_pages[name] for name in page_names]

    # =================================================================
    # REGISTER NAVIGATION — hidden (we render our own Bloomberg sidebar)
    # =================================================================
    pg = st.navigation(nav_dict, position="hidden")

    # =================================================================
    # HANDLE PROGRAMMATIC NAVIGATION (nav_to from page_views)
    # =================================================================
    if "nav_to" in st.session_state and st.session_state.nav_to:
        nav_target = st.session_state.nav_to
        st.session_state.nav_to = None

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
        nav_target = page_mapping.get(nav_target, nav_target)
        if nav_target in _pages:
            st.switch_page(_pages[nav_target])

    # =================================================================
    # CUSTOM BLOOMBERG-STYLE SIDEBAR
    # =================================================================
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

    # Identify current page for button highlighting
    current_url = pg.url_path

    # Render grouped navigation with section headers
    for group_name, page_names in nav_groups.items():
        st.sidebar.markdown(
            f'<div style="font-size: 10px; font-weight: 600; color: #ff9800; '
            f'letter-spacing: 1px; margin: 12px 0 4px 0; padding: 4px 0; '
            f'border-bottom: 1px solid rgba(255,152,0,0.3);">{group_name}</div>',
            unsafe_allow_html=True
        )

        for page_name in page_names:
            page_ref = _pages[page_name]
            is_selected = (page_ref.url_path == current_url)

            if st.sidebar.button(
                page_name,
                key=f"nav_{page_name}",
                use_container_width=True,
                type="primary" if is_selected else "secondary",
            ):
                st.switch_page(page_ref)

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
    # EXECUTE SELECTED PAGE — framework guarantees isolation
    # =================================================================
    pg.run()


if __name__ == "__main__":
    main()
