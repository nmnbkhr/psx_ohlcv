"""
PakFinData Explorer - Streamlit Dashboard.

Run with: streamlit run src/pakfindata/ui/app.py
"""

from dotenv import load_dotenv
load_dotenv()  # Load .env file (OPENAI_API_KEY, etc.)

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
    # Navigate up from src/pakfindata/ui/app.py to src/
    src_path = Path(__file__).resolve().parents[2]
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))
except Exception:
    pass

from pakfindata.analytics import (
    get_current_market_with_sectors,
    get_latest_market_analytics,
    get_sector_leaderboard,
    get_top_list,
    init_analytics_schema,
)
from pakfindata.config import (
    DEFAULT_DB_PATH,
    DEFAULT_LOG_FILE,
    DEFAULT_SYNC_CONFIG,
    SyncConfig,
    get_db_path,
)
from pakfindata.query import (
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
from pakfindata.sync import sync_all, sync_intraday, sync_intraday_bulk
from pakfindata.services import (
    is_service_running,
    read_status as read_service_status,
    start_service_background,
    stop_service,
)
from pakfindata.services.announcements_service import (
    is_service_running as is_announcements_running,
    read_status as read_announcements_status,
    start_service_background as start_announcements_service,
    stop_service as stop_announcements_service,
)
from pakfindata.services.eod_sync_service import (
    is_eod_sync_running,
    read_eod_status,
    start_eod_sync_background,
    stop_eod_sync,
)
from pakfindata.sources.announcements import (
    fetch_announcements,
    fetch_corporate_events,
    fetch_company_payouts,
    save_announcement,
    save_corporate_event,
    save_dividend_payout,
)
from pakfindata.ui.charts import (
    make_candlestick,
    make_intraday_chart,
    make_market_breadth_chart,
    make_price_line,
    make_top_movers_chart,
    make_volume_chart,
)
from pakfindata.ui.themes import (
    get_theme,
    get_chart_colors,
    get_plotly_layout,
)
from pakfindata.ui.session_tracker import (
    init_session_tracking,
    track_button_click,
    track_download,
    track_page_visit,
    track_refresh,
    track_symbol_search,
)
from pakfindata.ui.chat import chat_page
from pakfindata.ui.logo import render_logo, render_powered_by, render_disclaimer

# Shared helpers — canonical versions live in helpers.py; import instead of duplicating
from pakfindata.ui.components.helpers import (
    # Theme system
    init_theme,
    get_current_theme,
    set_theme,
    inject_theme_css,
    # Formatting
    format_price_change,
    format_volume,
    format_price,
    # Rendering helpers
    render_market_status_badge,
    render_ticker_tape,
    render_price_card,
    # UI enhancement helpers
    render_skeleton_loader,
    render_data_warning,
    render_data_info,
    render_data_error,
    render_empty_state,
    render_freshness_badge,
    render_section_with_loading,
    get_user_friendly_error,
    # Data freshness & staleness
    check_data_staleness,
    get_data_freshness,
    get_freshness_badge,
    # DB connection
    get_cached_connection,
    get_connection,
    # Market helpers
    is_market_closed,
    # Sector helpers
    get_sector_names,
    add_sector_name_column,
    # Constants
    EXPORTS_DIR,
    OHLCV_TOOLTIPS,
    DATA_QUALITY_NOTICE,
    MARKET_OPEN_HOUR,
    MARKET_CLOSE_HOUR,
    MARKET_DAYS,
    # Footer
    render_footer,
)

# Deep scraper imports for Bloomberg-style data
from pakfindata.sources.deep_scraper import (
    deep_scrape_batch,
    deep_scrape_symbol,
)
from pakfindata.db import (
    get_company_snapshot,
    get_trading_sessions,
    get_corporate_announcements,
    get_latest_kse100,
)

# Phase 1: Instrument universe imports
from pakfindata.db import (
    get_instruments,
    get_ohlcv_instrument,
    get_eod_ohlcv,
)
from pakfindata.analytics_phase1 import (
    compute_rankings,
    get_rankings,
    get_normalized_performance,
    compute_all_metrics,
)
from pakfindata.sync_instruments import sync_instruments_eod

# Phase 2: FX analytics imports
from pakfindata.db import (
    get_fx_pairs,
    get_fx_ohlcv,
    get_fx_latest_rate,
    get_fx_adjusted_metrics,
)
from pakfindata.analytics_fx import (
    get_fx_analytics,
    compute_and_store_fx_adjusted_metrics,
    get_normalized_fx_performance,
    get_fx_impact_summary,
)
from pakfindata.sync_fx import sync_fx_pairs, seed_fx_pairs

# Page config - must be first Streamlit command
# Try to load favicon from SVG asset
_favicon_path = Path(__file__).parent / "assets" / "pakfindata-favicon.svg"
_page_icon = _favicon_path.read_text() if _favicon_path.exists() else "📊"

st.set_page_config(
    page_title="PakFinData Terminal",
    page_icon=_page_icon,
    layout="wide",
    initial_sidebar_state="expanded",
)

# -----------------------------------------------------------------------------
# Page: Live Market
# -----------------------------------------------------------------------------
def live_market_page():
    from pakfindata.ui.page_views.live_market import render_live_market
    render_live_market()


# -----------------------------------------------------------------------------
# Page: Data Quality
# -----------------------------------------------------------------------------
def data_quality_page():
    from pakfindata.ui.page_views.data_quality import render_data_quality
    render_data_quality()


# -----------------------------------------------------------------------------
# Page: Dashboard
# -----------------------------------------------------------------------------
def dashboard():
    from pakfindata.ui.page_views.dashboard import render_dashboard
    render_dashboard()


# -----------------------------------------------------------------------------
# Page: Candlestick Explorer
# -----------------------------------------------------------------------------
def candlestick_explorer():
    from pakfindata.ui.page_views.candlestick import render_candlestick
    render_candlestick()


# -----------------------------------------------------------------------------
# Page: Intraday Trend
# -----------------------------------------------------------------------------
def intraday_trend_page():
    from pakfindata.ui.page_views.intraday import render_intraday
    render_intraday()


# -----------------------------------------------------------------------------
# Page: Regular Market Watch
# -----------------------------------------------------------------------------
def regular_market_page():
    from pakfindata.ui.page_views.regular_market import render_regular_market
    render_regular_market()


# -----------------------------------------------------------------------------
# Page: Symbols
# -----------------------------------------------------------------------------
def symbols_page():
    from pakfindata.ui.page_views.symbols import render_symbols
    render_symbols()


# -----------------------------------------------------------------------------
# Page: Futures & Contracts
# -----------------------------------------------------------------------------
def futures_page():
    from pakfindata.ui.page_views.futures import render_futures
    render_futures()


# -----------------------------------------------------------------------------
# Page: Schema - Database schema documentation and SQL scripts
# -----------------------------------------------------------------------------
def schema_page():
    from pakfindata.ui.page_views.schema import render_schema
    render_schema()


# -----------------------------------------------------------------------------
# Page: Settings
# -----------------------------------------------------------------------------
def settings_page():
    from pakfindata.ui.page_views.settings import render_settings
    render_settings()


# -----------------------------------------------------------------------------
# Page: History
# -----------------------------------------------------------------------------
def history_page():
    from pakfindata.ui.page_views.history import render_history
    render_history()


# -----------------------------------------------------------------------------
# Page: EOD Data Loader
# -----------------------------------------------------------------------------
def eod_data_loader_page():
    from pakfindata.ui.page_views.eod_loader import render_eod_loader
    render_eod_loader()


def _eod_data_loader_page_impl():
    from pakfindata.ui.page_views.eod_loader import _eod_data_loader_page_impl
    _eod_data_loader_page_impl()


# -----------------------------------------------------------------------------
# Page: Company Analytics
# -----------------------------------------------------------------------------
def company_analytics_page():
    from pakfindata.ui.page_views.company_deep import render_company_deep
    render_company_deep()


# -----------------------------------------------------------------------------
# Page: Data Acquisition - Bulk Data Scraping & Collection
# -----------------------------------------------------------------------------
def data_acquisition_page():
    from pakfindata.ui.page_views.data_acquisition import render_data_acquisition
    render_data_acquisition()


# -----------------------------------------------------------------------------
# Page: Factor Analysis - Quantitative Factor Rankings & Analysis
# -----------------------------------------------------------------------------
def factor_analysis_page():
    from pakfindata.ui.page_views.factor_analysis import render_factor_analysis
    render_factor_analysis()


# -----------------------------------------------------------------------------
# Page: AI Insights (GPT-5.2 powered analysis)
# -----------------------------------------------------------------------------
def ai_insights_page():
    from pakfindata.ui.page_views.ai_insights import render_ai_insights
    render_ai_insights()


# -----------------------------------------------------------------------------
# Page: Market Summary
# -----------------------------------------------------------------------------
def market_summary_page():
    from pakfindata.ui.page_views.market_summary import render_market_summary
    render_market_summary()


def post_close_page():
    from pakfindata.ui.page_views.post_close import render_post_close
    render_post_close()


# -----------------------------------------------------------------------------
# Page: Sync Monitor
# -----------------------------------------------------------------------------
def sync_monitor():
    from pakfindata.ui.page_views.sync_monitor import render_sync_monitor
    render_sync_monitor()


# =============================================================================
# Phase 1: Instruments Page
# =============================================================================
def instruments_page():
    from pakfindata.ui.page_views.instruments import render_instruments
    render_instruments()


# =============================================================================
# Phase 1: Rankings Page
# =============================================================================
def rankings_page():
    from pakfindata.ui.page_views.rankings import render_rankings
    render_rankings()


# =============================================================================
# Phase 1: Index Analytics Page
# =============================================================================
def indices_analytics_page():
    from pakfindata.ui.page_views.indices import render_indices
    render_indices()


# =============================================================================
# Phase 2: FX Overview Page
# =============================================================================
def fx_overview_page():
    from pakfindata.ui.page_views.fx import render_fx_overview
    render_fx_overview()


# =============================================================================
# Phase 2: FX Impact Page
# =============================================================================
def fx_impact_page():
    from pakfindata.ui.page_views.fx import render_fx_impact
    render_fx_impact()


# =============================================================================
# Phase 2.5: Mutual Funds Page
# =============================================================================
def mutual_funds_page():
    from pakfindata.ui.page_views.funds import render_mutual_funds
    render_mutual_funds()


# =============================================================================
# Phase 2.5: Fund Analytics Page
# =============================================================================
def fund_analytics_page():
    from pakfindata.ui.page_views.funds import render_fund_analytics
    render_fund_analytics()


# =============================================================================
# Phase 3: Bonds Screener Page
# =============================================================================
def bonds_screener_page():
    from pakfindata.ui.page_views.fixed_income import render_bonds_screener
    render_bonds_screener()


# =============================================================================
# Phase 3: Yield Curve Page
# =============================================================================
def yield_curve_page():
    from pakfindata.ui.page_views.fixed_income import render_yield_curve
    render_yield_curve()


# =============================================================================
# Phase 3: Sukuk Screener Page (Additive - separate from bonds)
# =============================================================================
def sukuk_screener_page():
    from pakfindata.ui.page_views.fixed_income import render_sukuk_screener
    render_sukuk_screener()


# =============================================================================
# Phase 3: Sukuk Yield Curve Page
# =============================================================================
def sukuk_yield_curve_page():
    from pakfindata.ui.page_views.fixed_income import render_sukuk_yield_curve
    render_sukuk_yield_curve()


# =============================================================================
# Phase 3: SBP Auction Archive Page
# =============================================================================
def sbp_auction_archive_page():
    from pakfindata.ui.page_views.fixed_income import render_sbp_auction_archive
    render_sbp_auction_archive()


# =============================================================================
# Phase 3.5: Government Fixed Income Pages
# =============================================================================

def govt_fixed_income_page():
    from pakfindata.ui.page_views.fixed_income import render_govt_fixed_income
    render_govt_fixed_income()


def fi_yield_curve_page():
    from pakfindata.ui.page_views.fixed_income import render_fi_yield_curve
    render_fi_yield_curve()


def sbp_pma_archive_page():
    from pakfindata.ui.page_views.fixed_income import render_sbp_pma_archive
    render_sbp_pma_archive()


def psx_debt_market_page():
    from pakfindata.ui.page_views.fixed_income import render_psx_debt_market
    render_psx_debt_market()


def bond_market_page():
    from pakfindata.ui.page_views.bond_market import render_bond_market
    render_bond_market()


def treasury_dashboard_page():
    from pakfindata.ui.page_views.treasury_dashboard import render_treasury_dashboard
    render_treasury_dashboard()


def fx_dashboard_page():
    from pakfindata.ui.page_views.fx_dashboard import render_fx_dashboard
    render_fx_dashboard()


def fund_explorer_page():
    from pakfindata.ui.page_views.fund_explorer import render_fund_explorer
    render_fund_explorer()


def research_terminal_page():
    from pakfindata.ui.page_views.research_terminal import render_research_terminal
    render_research_terminal()


def signal_dashboard_page():
    from pakfindata.ui.page_views.signal_dashboard import render_signal_dashboard
    render_signal_dashboard()


def microstructure_page():
    from pakfindata.ui.page_views.microstructure import render_microstructure
    render_microstructure()


def strategy_vpin_page():
    from pakfindata.ui.page_views.strategy_vpin import render_page
    render_page()


def strategy_ofi_page():
    from pakfindata.ui.page_views.strategy_ofi import render_page
    render_page()


def strategy_cvd_page():
    from pakfindata.ui.page_views.strategy_cvd import render_page
    render_page()


def strategy_basis_page():
    from pakfindata.ui.page_views.strategy_basis import render_page
    render_page()


def strategy_oi_page():
    from pakfindata.ui.page_views.strategy_oi import render_strategy_oi
    render_strategy_oi()


def strategy_pairs_page():
    from pakfindata.ui.page_views.strategy_pairs import render_strategy_pairs
    render_strategy_pairs()


def strategy_vwap_page():
    from pakfindata.ui.page_views.strategy_vwap import render_page
    render_page()


def strategy_hmm_page():
    from pakfindata.ui.page_views.strategy_hmm import render_page
    render_page()


def strategy_sector_page():
    from pakfindata.ui.page_views.strategy_sector import render_page
    render_page()


def strategy_sentiment_page():
    from pakfindata.ui.page_views.strategy_sentiment import render_page
    render_page()


def strategy_orderbook_page():
    from pakfindata.ui.page_views.strategy_orderbook import render_page
    render_page()


def advanced_gnn_page():
    from pakfindata.ui.page_views.advanced_gnn import render_page
    render_page()


def tick_analytics_page():
    from pakfindata.ui.page_views.tick_analytics import render_tick_analytics
    render_tick_analytics()


def tick_replay_page():
    from pakfindata.ui.page_views.tick_replay import render_tick_replay
    render_tick_replay()


def ml_predictions_page():
    from pakfindata.ui.page_views.ml_predictions import render_ml_predictions
    render_ml_predictions()


def intraday_quant_lab_page():
    from pakfindata.ui.page_views.intraday_quant_lab import render_intraday_quant_lab
    render_intraday_quant_lab()



def macro_cycles_page():
    from pakfindata.ui.page_views.macro_cycles import render_macro_cycles
    render_macro_cycles()


def sector_breadth_page():
    from pakfindata.ui.page_views.sector_breadth import render_sector_breadth
    render_sector_breadth()


def market_research_page():
    from pakfindata.ui.page_views.market_research import render_market_research
    render_market_research()


def website_scan_page():
    from pakfindata.ui.page_views.website_scan import render_website_scan
    render_website_scan()


def live_ohlcv_page():
    from pakfindata.ui.page_views.live_ohlcv import render_live_ohlcv
    render_live_ohlcv()


def live_ticker_page():
    from pakfindata.ui.page_views.live_ticker import render_live_ticker
    render_live_ticker()


def live_indices_page():
    from pakfindata.ui.page_views.live_indices import render_live_indices
    render_live_indices()


def ws_relay_status_page():
    from pakfindata.ui.page_views.ws_relay_status import render_ws_relay_status
    render_ws_relay_status()


def global_rates_page():
    from pakfindata.ui.page_views.global_rates import render_global_rates
    render_global_rates()


def npc_rates_page():
    from pakfindata.ui.page_views.npc_rates import render_npc_rates
    render_npc_rates()


# =============================================================================
# Blueprint Page Wrappers — New pages for 5-pillar navigation
# =============================================================================

def market_pulse_page():
    from pakfindata.ui.page_views.market_pulse import render_market_pulse
    render_market_pulse()


def stock_screener_page():
    from pakfindata.ui.page_views.stock_screener import render_stock_screener
    render_stock_screener()


def company_profile_page():
    from pakfindata.ui.page_views.company_deep import render_company_deep
    render_company_deep()


def sector_analysis_page():
    from pakfindata.ui.page_views.sector_analysis import render_sector_analysis
    render_sector_analysis()


def rates_overview_page():
    from pakfindata.ui.page_views.rates_overview import render_rates_overview
    render_rates_overview()


def yield_curves_page():
    from pakfindata.ui.page_views.fixed_income import render_yield_curve
    render_yield_curve()


def treasury_auctions_page():
    from pakfindata.ui.page_views.treasury_dashboard import render_treasury_dashboard
    render_treasury_dashboard()


def bond_market_otc_page():
    from pakfindata.ui.page_views.bond_market import render_bond_market
    render_bond_market()


def benchmark_monitor_page():
    from pakfindata.ui.page_views.benchmark_monitor import render_benchmark_monitor
    render_benchmark_monitor()


def debt_terminal_page():
    from pakfindata.ui.page_views.debt_terminal import render_debt_terminal
    render_debt_terminal()


def alm_dashboard_page():
    from pakfindata.ui.page_views.alm_dashboard import render_alm_dashboard
    render_alm_dashboard()


def ftp_monitor_page():
    from pakfindata.ui.page_views.ftp_monitor import render_ftp_monitor
    render_ftp_monitor()


def symbol_financials_page():
    from pakfindata.ui.page_views.symbol_financials import render
    render()


def vps_pension_page():
    from pakfindata.ui.page_views.fund_explorer import render_vps_standalone
    render_vps_standalone()


def top_performers_page():
    from pakfindata.ui.page_views.fund_explorer import render_top_performers_standalone
    render_top_performers_standalone()


def etfs_page():
    from pakfindata.ui.page_views.fund_explorer import render_etfs_standalone
    render_etfs_standalone()


def currency_dashboard_page():
    from pakfindata.ui.page_views.fx_dashboard import render_fx_dashboard
    render_fx_dashboard()


def fx_interbank_page():
    from pakfindata.ui.page_views.fx_interbank import render_fx_interbank
    render_fx_interbank()


def fx_history_page():
    from pakfindata.ui.page_views.fx_history import render_fx_history
    render_fx_history()


def data_status_page():
    from pakfindata.ui.page_views.data_quality import render_data_quality
    render_data_quality()


def sync_center_page():
    from pakfindata.ui.page_views.sync_monitor import render_sync_monitor
    render_sync_monitor()


def app_lineage_page():
    from pakfindata.ui.page_views.app_lineage import render_app_lineage
    render_app_lineage()


# -----------------------------------------------------------------------------
# Page: Commodities Dashboard
# -----------------------------------------------------------------------------
def commodities_page():
    from pakfindata.ui.page_views.commodities import render_commodities
    render_commodities()


# -----------------------------------------------------------------------------
# Page: PMEX Commodities (OHLC + Margins)
# -----------------------------------------------------------------------------
def pmex_page():
    from pakfindata.ui.page_views.pmex import render_pmex
    render_pmex()


def sbp_easydata_page():
    from pakfindata.ui.page_views.sbp_easydata import render_sbp_easydata
    render_sbp_easydata()


def psx_scraper_page():
    from pakfindata.ui.page_views.psx_scraper import render_psx_scraper
    render_psx_scraper()


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
    # BUILD st.Page REGISTRY — 5-pillar blueprint navigation
    # =================================================================

    # PRIMARY PAGES — shown in sidebar (the 5 pillars + admin)
    _pages = {
        # MARKET OVERVIEW
        "Dashboard":          st.Page(dashboard,              title="Dashboard",          url_path="dashboard",          default=True),
        "Market Pulse":       st.Page(market_pulse_page,      title="Market Pulse",       url_path="market-pulse"),
        "Index Monitor":      st.Page(indices_analytics_page, title="Index Monitor",      url_path="index-monitor"),
        # EQUITIES
        "Market Summary":     st.Page(market_summary_page,    title="Market Summary",     url_path="market-summary"),
        "Stock Screener":     st.Page(stock_screener_page,    title="Stock Screener",     url_path="stock-screener"),
        "Company Profile":    st.Page(company_profile_page,   title="Company Profile",    url_path="company"),
        "Sector Analysis":    st.Page(sector_analysis_page,   title="Sector Analysis",    url_path="sector-analysis"),
        "Factors":            st.Page(factor_analysis_page,   title="Factors",            url_path="factors"),
        "Intraday":           st.Page(intraday_trend_page,    title="Intraday",           url_path="intraday"),
        "Live Ticker":        st.Page(live_ticker_page,       title="Live Ticker",        url_path="live-ticker"),
        "Futures & Odd Lot":  st.Page(futures_page,           title="Futures & Odd Lot",  url_path="futures"),
        "Post Close":         st.Page(post_close_page,        title="Post Close",         url_path="post-close"),
        # FIXED INCOME
        "Rates Overview":     st.Page(rates_overview_page,    title="Rates Overview",     url_path="rates-overview"),
        "Yield Curves":       st.Page(yield_curves_page,      title="Yield Curves",       url_path="yield-curves"),
        "Treasury Auctions":  st.Page(treasury_auctions_page, title="Treasury Auctions",  url_path="treasury-auctions"),
        "Bond Market":        st.Page(bond_market_otc_page,   title="Bond Market",        url_path="bond-market"),
        "Benchmark Monitor":  st.Page(benchmark_monitor_page, title="Benchmark Monitor",  url_path="benchmark"),
        "Debt Terminal":      st.Page(debt_terminal_page,    title="Debt Terminal",      url_path="debt-terminal"),
        "Treasury":           st.Page(treasury_dashboard_page,  title="Treasury",         url_path="treasury"),
        # ALM
        "ALM Dashboard":      st.Page(alm_dashboard_page,      title="ALM Dashboard",    url_path="alm-dashboard"),
        "FTP Monitor":        st.Page(ftp_monitor_page,         title="FTP Monitor",      url_path="ftp-monitor"),
        # FUNDS
        "Fund Explorer":      st.Page(fund_explorer_page,     title="Fund Explorer",      url_path="fund-explorer"),
        "VPS Pension":        st.Page(vps_pension_page,       title="VPS Pension",        url_path="vps-pension"),
        "Top Performers":     st.Page(top_performers_page,    title="Top Performers",     url_path="top-performers"),
        "Fund Analytics":     st.Page(fund_analytics_page,    title="Fund Analytics",     url_path="fund-analytics"),
        "ETFs":               st.Page(etfs_page,              title="ETFs",               url_path="etfs"),
        # FX & RATES
        "Currency Dashboard": st.Page(currency_dashboard_page, title="Currency Dashboard", url_path="currency-dashboard"),
        "FX Dashboard":       st.Page(fx_dashboard_page,        title="FX Dashboard",     url_path="fx-dashboard"),
        "Interbank vs Open":  st.Page(fx_interbank_page,      title="Interbank vs Open",  url_path="fx-interbank"),
        "Rate History":       st.Page(fx_history_page,        title="Rate History",       url_path="fx-history"),
        # COMMODITIES
        "Commodities":        st.Page(commodities_page,        title="Commodities",        url_path="commodities"),
        "PMEX":               st.Page(pmex_page,               title="PMEX",               url_path="pmex"),
        # COMPANY FINANCIALS
        "Symbol Financials":  st.Page(symbol_financials_page,   title="Symbol Financials", url_path="symbol-financials"),
        # RESEARCH & QUANT
        "Research":           st.Page(research_terminal_page,   title="Research",          url_path="research"),
        "Signal Analysis":   st.Page(signal_dashboard_page,    title="Signal Analysis",   url_path="signal-analysis"),
        "Microstructure":     st.Page(microstructure_page,      title="Microstructure",    url_path="microstructure"),
        "Tick Analytics":    st.Page(tick_analytics_page,      title="Tick Analytics",    url_path="tick-analytics"),
        "Tick Replay":       st.Page(tick_replay_page,        title="Tick Replay",       url_path="tick-replay"),
        "Quant Lab":         st.Page(intraday_quant_lab_page, title="Quant Lab",         url_path="quant-lab"),
        "Macro Cycles":       st.Page(macro_cycles_page,        title="Macro Cycles",      url_path="macro-cycles"),
        "Sector Breadth":     st.Page(sector_breadth_page,      title="Sector Breadth",    url_path="sector-breadth"),
        "Market Research":    st.Page(market_research_page,     title="Market Research",   url_path="market-research"),
        "ML Predictions":    st.Page(ml_predictions_page,      title="ML Predictions",    url_path="ml-predictions"),
        # STRATEGIES
        "VPIN Strategy":     st.Page(strategy_vpin_page,       title="VPIN Strategy",     url_path="vpin-strategy"),
        "OFI Alpha":         st.Page(strategy_ofi_page,        title="OFI Alpha",         url_path="ofi-alpha"),
        "CVD Divergence":    st.Page(strategy_cvd_page,        title="CVD Divergence",    url_path="cvd-divergence"),
        "Basis Arb":         st.Page(strategy_basis_page,      title="Basis Arb",         url_path="basis-arb"),
        "VWAP Execution":    st.Page(strategy_vwap_page,       title="VWAP Execution",    url_path="vwap-execution"),
        "Macro Regime":      st.Page(strategy_hmm_page,        title="Macro Regime",      url_path="macro-regime-hmm"),
        "Sector Rotation":   st.Page(strategy_sector_page,     title="Sector Rotation",   url_path="sector-rotation"),
        "LLM Sentiment":     st.Page(strategy_sentiment_page,  title="LLM Sentiment",     url_path="llm-sentiment"),
        "OI Buildup/Unwind": st.Page(strategy_oi_page,          title="OI Buildup/Unwind", url_path="oi-buildup"),
        "Pairs Trading":    st.Page(strategy_pairs_page,       title="Pairs Trading",     url_path="pairs-trading"),
        "LLM Sentiment":    st.Page(strategy_sentiment_page,  title="LLM Sentiment",     url_path="llm-sentiment"),
        # ADVANCED
        "Order Book Sim":   st.Page(strategy_orderbook_page, title="Order Book Sim",    url_path="orderbook-sim"),
        "Stock Graph (GNN)": st.Page(advanced_gnn_page,      title="Stock Graph (GNN)", url_path="stock-graph-gnn"),
        # ADMIN
        "Data Status":        st.Page(data_status_page,       title="Data Status",        url_path="data-status"),
        "Sync Center":        st.Page(sync_center_page,       title="Sync Center",        url_path="sync-center"),
        "Schema Explorer":    st.Page(schema_page,            title="Schema Explorer",    url_path="schema"),
        "App Lineage":        st.Page(app_lineage_page,       title="App Lineage",        url_path="app-lineage"),
        "SBP EasyData":       st.Page(sbp_easydata_page,      title="SBP EasyData",       url_path="sbp-easydata"),
        "PSX Scraper":        st.Page(psx_scraper_page,       title="PSX Scraper",        url_path="psx-scraper"),
    }

    # Navigation groups — 5-pillar blueprint structure
    nav_groups = {
        "MARKET OVERVIEW": ["Dashboard", "Market Pulse", "Index Monitor"],
        "EQUITIES":        ["Market Summary", "Stock Screener", "Company Profile",
                            "Sector Analysis", "Symbol Financials", "Factors",
                            "Intraday", "Live Ticker",
                            "Futures & Odd Lot", "Post Close"],
        "FIXED INCOME":    ["Rates Overview", "Yield Curves", "Treasury Auctions",
                            "Bond Market", "Benchmark Monitor", "Debt Terminal",
                            "Treasury"],
        "ALM":             ["ALM Dashboard", "FTP Monitor"],
        "FUNDS":           ["Fund Explorer", "VPS Pension", "Top Performers",
                            "Fund Analytics", "ETFs"],
        "FX & RATES":      ["Currency Dashboard", "FX Dashboard", "Interbank vs Open", "Rate History"],
        "COMMODITIES":     ["Commodities", "PMEX"],
        "RESEARCH":        ["Research", "Signal Analysis", "Microstructure", "Tick Analytics", "Tick Replay", "Quant Lab", "Macro Cycles", "Sector Breadth", "Market Research", "ML Predictions"],
        "STRATEGIES":      ["VPIN Strategy", "OFI Alpha", "CVD Divergence", "Basis Arb", "VWAP Execution", "Macro Regime", "Sector Rotation", "OI Buildup/Unwind", "Pairs Trading", "LLM Sentiment"],
        "ADVANCED":        ["Order Book Sim", "Stock Graph (GNN)"],
        "ADMIN":           ["Data Status", "Sync Center", "Schema Explorer", "App Lineage", "SBP EasyData", "PSX Scraper"],
    }

    # HIDDEN PAGES — registered for URL access but no sidebar button
    # Preserves backwards-compatible URLs for bookmarks
    _hidden_pages = {
        "Live Market":      st.Page(live_market_page,        title="Live Market",      url_path="live-market"),
        "Live OHLCV":       st.Page(live_ohlcv_page,         title="Live OHLCV",       url_path="live-ohlcv"),
        "Live Indices":     st.Page(live_indices_page,        title="Live Indices",     url_path="live-indices"),
        "WS Relay":         st.Page(ws_relay_status_page,     title="WS Relay",         url_path="ws-relay"),
        "Quote Monitor":    st.Page(regular_market_page,      title="Quote Monitor",    url_path="quote-monitor"),
        "Price Chart":      st.Page(candlestick_explorer,     title="Price Chart",      url_path="price-chart"),
        "Rankings":         st.Page(rankings_page,            title="Rankings",          url_path="rankings"),
        "Symbols":          st.Page(symbols_page,             title="Symbols",           url_path="symbols"),
        "Instruments":      st.Page(instruments_page,         title="Instruments",      url_path="instruments"),
        "FI Overview":      st.Page(psx_debt_market_page,     title="FI Overview",      url_path="fi-overview"),
        "Bond Search":      st.Page(bonds_screener_page,      title="Bond Search",      url_path="bond-search"),
        "Yield Curve":      st.Page(yield_curve_page,         title="Yield Curve",      url_path="yield-curve"),
        "Sukuk":            st.Page(sukuk_screener_page,      title="Sukuk",            url_path="sukuk"),
        "SBP Auctions":     st.Page(sbp_auction_archive_page, title="SBP Auctions",    url_path="sbp-auctions"),
        "Global Rates":     st.Page(global_rates_page,        title="Global Rates",     url_path="global-rates"),
        "NPC Rates":        st.Page(npc_rates_page,           title="NPC Rates",        url_path="npc-rates"),
        "FX Monitor":       st.Page(fx_overview_page,         title="FX Monitor",       url_path="fx-monitor"),
        "FX Analytics":     st.Page(fx_impact_page,           title="FX Analytics",     url_path="fx-analytics"),
        "Fund Directory":   st.Page(mutual_funds_page,        title="Fund Directory",   url_path="fund-directory"),
        "Data Sync":        st.Page(data_acquisition_page,    title="Data Sync",        url_path="data-sync"),
        "EOD Loader":       st.Page(eod_data_loader_page,     title="EOD Loader",       url_path="eod-loader"),
        "History":          st.Page(history_page,             title="History",           url_path="history"),
        "Sync Monitor":     st.Page(sync_monitor,             title="Sync Monitor",     url_path="sync-monitor"),
        "Data Quality":     st.Page(data_quality_page,        title="Data Quality",     url_path="data-quality"),
        "Website Scan":     st.Page(website_scan_page,        title="Website Scan",     url_path="website-scan"),
        "AI Chat":          st.Page(chat_page,                title="AI Chat",          url_path="ai-chat"),
        "AI Insights":      st.Page(ai_insights_page,         title="AI Insights",      url_path="ai-insights"),
        "Settings":         st.Page(settings_page,            title="Settings",          url_path="settings"),
    }

    # Build grouped dict for st.navigation (primary + hidden)
    nav_dict = {}
    for group_name, page_names in nav_groups.items():
        nav_dict[group_name] = [_pages[name] for name in page_names]

    # Add hidden pages — they get URL routing but no sidebar buttons
    # (position="hidden" means Streamlit doesn't render any group headers)
    nav_dict["OTHER"] = list(_hidden_pages.values())

    # =================================================================
    # REGISTER NAVIGATION — hidden (we render our own Bloomberg sidebar)
    # =================================================================
    pg = st.navigation(nav_dict, position="hidden")

    # =================================================================
    # HANDLE PROGRAMMATIC NAVIGATION (nav_to from page_views)
    # =================================================================
    all_pages = {**_pages, **_hidden_pages}

    if "nav_to" in st.session_state and st.session_state.nav_to:
        nav_target = st.session_state.nav_to
        st.session_state.nav_to = None

        # Map old page names to new names for backwards compatibility
        page_mapping = {
            # Old emoji-prefixed names → new clean names
            "📊 Dashboard": "Dashboard",
            "📡 Live Market": "Live Market",
            "📈 Market Summary": "Market Summary",
            "💰 Post Close": "Post Close",
            "📈 Quote Monitor": "Quote Monitor",
            "📊 Price Chart": "Price Chart",
            "⏱ Intraday": "Intraday",
            "🏢 Company": "Company Profile",
            "🏆 Rankings": "Rankings",
            "📊 Factors": "Factors",
            "🧵 Symbols": "Symbols",
            "📊 Futures": "Futures & Odd Lot",
            "Futures": "Futures & Odd Lot",
            "📊 Index Monitor": "Index Monitor",
            "📦 Instruments": "Instruments",
            "📈 FI Overview": "FI Overview",
            "🧾 Bond Search": "Bond Search",
            "📉 Yield Curve": "Yield Curves",
            "🕌 Sukuk": "Sukuk",
            "🏛️ SBP Auctions": "SBP Auctions",
            "🏦 Treasury": "Treasury",
            "📊 Bond Market": "Bond Market",
            "🌐 Global Rates": "Global Rates",
            "🏦 NPC Rates": "NPC Rates",
            "🌍 FX Monitor": "FX Monitor",
            "📊 FX Analytics": "FX Analytics",
            "💱 FX Dashboard": "Currency Dashboard",
            "🏦 Fund Directory": "Fund Directory",
            "📊 Fund Analytics": "Fund Analytics",
            "🔍 Fund Explorer": "Fund Explorer",
            "📥 Data Sync": "Data Sync",
            "📂 EOD Loader": "EOD Loader",
            "📚 History": "History",
            "🔄 Sync Monitor": "Sync Center",
            "🩺 Data Quality": "Data Status",
            "🔗 Website Scan": "Website Scan",
            "💬 AI Chat": "AI Chat",
            "🤖 AI Insights": "AI Insights",
            "📋 Schema": "Schema Explorer",
            "🔬 Research": "Research",
            "⚙️ Settings": "Settings",
            # Legacy names
            "📊 Regular Market": "Quote Monitor",
            "📈 Candlestick Explorer": "Price Chart",
            "⏱ Intraday Trend": "Intraday",
            "🏢 Company Analytics": "Company Profile",
            "📊 Factor Analysis": "Factors",
            "📊 Indices": "Index Monitor",
            "📈 PSX Debt Market": "FI Overview",
            "🧾 Bonds Screener": "Bond Search",
            "🕌 Sukuk Screener": "Sukuk",
            "🏛️ SBP Archive": "SBP Auctions",
            "🌍 FX Overview": "FX Monitor",
            "📊 FX Impact": "FX Analytics",
            "🏦 Mutual Funds": "Fund Directory",
            "📥 Data Acquisition": "Data Sync",
            "📥 Market Summary": "Market Summary",
        }
        nav_target = page_mapping.get(nav_target, nav_target)
        if nav_target in all_pages:
            st.switch_page(all_pages[nav_target])

    # =================================================================
    # CUSTOM BLOOMBERG-STYLE SIDEBAR — 5-pillar navigation
    # =================================================================
    with st.sidebar:
        render_logo("sidebar")

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
    try:
        current_url = pg.url_path
    except AttributeError:
        current_url = ""

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
            try:
                is_selected = (page_ref.url_path == current_url)
            except AttributeError:
                is_selected = (page_ref.title == pg.title)

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
                st.sidebar.success(f"Data: {badge_text}")
            elif badge_color == "orange":
                st.sidebar.warning(f"Data: {badge_text}")
            elif badge_color == "red":
                st.sidebar.error(f"Data: {badge_text}")
    except Exception:
        pass

    # Tick data freshness
    try:
        import duckdb as _ddb
        from datetime import datetime as _dt, timedelta as _td, timezone as _tz
        _pkt = _tz(_td(hours=5))
        _today = _dt.now(_pkt).strftime("%Y-%m-%d")
        _tcon = _ddb.connect("/mnt/e/psxdata/pakfindata.duckdb", read_only=True)
        _tick_row = _tcon.execute(
            "SELECT MAX(SUBSTR(_ts, 1, 10)), COUNT(*) FROM tick_logs"
        ).fetchone()
        _tcon.close()
        if _tick_row and _tick_row[0]:
            _tick_date, _tick_count = _tick_row
            if str(_tick_date) == _today:
                st.sidebar.success(f"📡 Ticks: {_today} ({_tick_count:,})")
            else:
                st.sidebar.warning(f"📡 Ticks: {_tick_date} ({_tick_count:,})")
    except Exception:
        st.sidebar.caption("📡 Tick status unavailable")

    st.sidebar.markdown("---")
    st.sidebar.caption("CLI: `pfsync --help`")
    st.sidebar.caption(f"DB: `{get_db_path()}`")
    with st.sidebar:
        render_powered_by()

    # =================================================================
    # EXECUTE SELECTED PAGE — framework guarantees isolation
    # =================================================================
    pg.run()

    # Page footer — brand attribution + disclaimer
    render_powered_by()
    render_disclaimer()


if __name__ == "__main__":
    main()
