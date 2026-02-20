"""CLI for PSX OHLCV sync tool."""

import argparse
import sys
from collections import Counter
from pathlib import Path

from .analytics import (
    compute_all_analytics,
    get_current_market_with_sectors,
    init_analytics_schema,
)

# Phase 3.5: Fixed Income (Government Debt) imports
from .analytics_fixed_income import (
    compute_and_store_analytics as compute_fi_analytics,
)
from .analytics_fixed_income import (
    get_instruments_by_yield,
    get_yield_curve_analytics,
)
from .analytics_fx import (
    compute_and_store_fx_adjusted_metrics,
    get_fx_analytics,
)
from .analytics_phase1 import compute_rankings, get_rankings
from .analytics_sukuk import (
    compare_sukuk,
    get_sukuk_analytics_full,
    get_yield_curve_data,
)
from .analytics_sukuk import (
    compute_and_store_analytics as compute_sukuk_analytics,
)
from .analytics_sukuk import (
    get_analytics_by_category as get_sukuk_by_category,
)
from .config import DATA_ROOT, DEFAULT_DB_PATH, SyncConfig, setup_logging
from .db import connect, init_schema
from .query import (
    get_intraday_latest,
    get_intraday_stats,
    get_latest_close,
    get_ohlcv_range,
    get_symbols_string,
)
from .range_utils import parse_date, resolve_range
from .services.announcements_service import (
    read_status as read_announcements_status,
)
from .services.announcements_service import (
    start_service_background as start_announcements_service,
)
from .services.announcements_service import (
    stop_service as stop_announcements_service,
)
from .sources.announcements import (
    fetch_announcements,
    fetch_company_payouts,
    fetch_corporate_events,
    save_announcement,
    save_corporate_event,
    save_dividend_payout,
)
from .sources.company_page import (
    listen_quotes,
    refresh_company_profile,
    take_quote_snapshot,
)

# Phase 1: Instrument universe imports
from .sources.instrument_universe import seed_universe
from .sources.listed_companies import (
    export_master_csv,
    get_master_symbols,
    refresh_listed_companies,
)
from .sources.market_summary import fetch_range_summary
from .sources.market_watch import refresh_symbols
from .sources.regular_market import (
    fetch_regular_market,
    get_all_current_hashes,
    get_current_market,
    init_regular_market_schema,
    insert_snapshots,
    upsert_current,
)
from .sources.sectors import (
    export_sectors_csv,
    get_sector_list,
    refresh_sectors,
)
from .sync import SyncSummary, sync_all, sync_intraday, sync_intraday_bulk
from .sync_fixed_income import (
    get_fi_status_summary,
    get_fi_sync_status,
    seed_fi_instruments,
    sync_all_fixed_income,
    sync_fi_curves,
    sync_fi_quotes,
    sync_sbp_pma_docs,
)
from .sync_fixed_income import (
    setup_csv_templates as setup_fi_csv_templates,
)

# Phase 2: FX imports
from .sync_fx import (
    get_fx_data_summary,
    seed_fx_pairs,
    sync_fx_pairs,
)
from .sync_fx import (
    get_sync_status as get_fx_sync_status,
)
from .sync_instruments import (
    get_sync_status as get_instruments_sync_status,
)
from .sync_instruments import (
    sync_instruments_eod,
    sync_single_instrument,
)
from .sync_sukuk import (
    get_data_summary as get_sukuk_data_summary,
)
from .sync_sukuk import (
    get_sync_status as get_sukuk_sync_status,
)

# Phase 3: Sukuk imports
from .sync_sukuk import (
    index_sbp_documents,
    load_sukuk_csv,
    load_yield_curve_csv,
    seed_sukuk,
    sync_sample_yield_curves,
    sync_sukuk_quotes,
)
from .sync_sukuk import (
    load_quotes_csv as load_sukuk_quotes_csv,
)

# Exit codes
EXIT_SUCCESS = 0
EXIT_ERROR = 1
EXIT_ALL_FAILED = 2


def main(argv: list[str] | None = None) -> int:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="psxsync",
        description="PSX OHLCV data sync tool",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"Path to SQLite database (default: {DEFAULT_DB_PATH})",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # symbols command
    symbols_parser = subparsers.add_parser("symbols", help="Manage symbols")
    symbols_sub = symbols_parser.add_subparsers(dest="symbols_command", required=True)

    # symbols refresh
    symbols_sub.add_parser("refresh", help="Refresh symbols from market watch")

    # symbols list
    list_parser = symbols_sub.add_parser("list", help="List all symbols")
    list_parser.add_argument(
        "--as",
        dest="format",
        choices=["csv", "table"],
        default="table",
        help="Output format (default: table)",
    )

    # symbols string
    string_parser = symbols_sub.add_parser(
        "string", help="Get symbols as comma-separated string"
    )
    string_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of symbols",
    )

    # sync command
    sync_parser = subparsers.add_parser("sync", help="Sync EOD data")
    sync_parser.add_argument(
        "--all",
        action="store_true",
        dest="sync_all",
        help="Sync all active symbols",
    )
    sync_parser.add_argument(
        "--refresh-symbols",
        action="store_true",
        help="Refresh symbols before sync",
    )
    sync_parser.add_argument(
        "--limit-symbols",
        type=int,
        default=None,
        help="Limit number of symbols to sync",
    )
    sync_parser.add_argument(
        "--symbols",
        type=str,
        default=None,
        help="Comma-separated list of symbols to sync (e.g., 'HBL,OGDC')",
    )
    sync_parser.add_argument(
        "--incremental",
        action="store_true",
        help="Only sync data newer than existing max date per symbol",
    )
    sync_parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Max HTTP retries (default: 3)",
    )
    sync_parser.add_argument(
        "--delay-min",
        type=float,
        default=0.3,
        help="Min delay between requests in seconds (default: 0.3)",
    )
    sync_parser.add_argument(
        "--delay-max",
        type=float,
        default=0.7,
        help="Max delay between requests in seconds (default: 0.7)",
    )
    sync_parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="HTTP request timeout in seconds (default: 30)",
    )
    sync_parser.add_argument(
        "--async",
        action="store_true",
        dest="use_async",
        help="Use async fetcher (6-7x faster, concurrent HTTP)",
    )

    # quote command
    quote_parser = subparsers.add_parser("quote", help="Get latest quote for a symbol")
    quote_parser.add_argument(
        "--symbol",
        type=str,
        required=True,
        help="Stock symbol (e.g., 'HBL')",
    )

    # ohlcv command
    ohlcv_parser = subparsers.add_parser("ohlcv", help="Get OHLCV data for a symbol")
    ohlcv_parser.add_argument(
        "--symbol",
        type=str,
        required=True,
        help="Stock symbol (e.g., 'HBL')",
    )
    ohlcv_parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="Start date (YYYY-MM-DD)",
    )
    ohlcv_parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="End date (YYYY-MM-DD)",
    )
    ohlcv_parser.add_argument(
        "--out",
        type=str,
        choices=["csv", "table"],
        default="table",
        help="Output format (default: table)",
    )

    # market-summary command with subcommands
    ms_parser = subparsers.add_parser(
        "market-summary",
        help="Download and convert market summary .Z files"
    )
    ms_sub = ms_parser.add_subparsers(dest="ms_command", required=True)

    # market-summary day (single day)
    ms_day_parser = ms_sub.add_parser(
        "day", help="Download market summary for a single day"
    )
    ms_day_parser.add_argument(
        "--date",
        type=str,
        required=True,
        help="Date in YYYY-MM-DD format",
    )
    ms_day_parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help=f"Output directory (default: {DATA_ROOT / 'market_summary'})",
    )
    ms_day_parser.add_argument(
        "--keep-raw",
        action="store_true",
        help="Keep extracted raw file after processing",
    )
    ms_day_parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if CSV already exists",
    )
    ms_day_parser.add_argument(
        "--import-eod",
        action="store_true",
        help="Import downloaded CSV into eod_ohlcv table",
    )
    ms_day_parser.add_argument(
        "--pdf-fallback",
        action="store_true",
        help="Use PDF closing rates as fallback if .Z file fails",
    )

    # market-summary range (date range)
    ms_range_parser = ms_sub.add_parser(
        "range", help="Download market summaries for a date range"
    )
    ms_range_parser.add_argument(
        "--start",
        type=str,
        required=True,
        help="Start date (YYYY-MM-DD)",
    )
    ms_range_parser.add_argument(
        "--end",
        type=str,
        required=True,
        help="End date (YYYY-MM-DD)",
    )
    ms_range_parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help=f"Output directory (default: {DATA_ROOT / 'market_summary'})",
    )
    ms_range_parser.add_argument(
        "--include-weekends",
        action="store_true",
        help="Include weekends (default: skip Sat/Sun)",
    )
    ms_range_parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if CSV already exists",
    )
    ms_range_parser.add_argument(
        "--keep-raw",
        action="store_true",
        help="Keep extracted raw files",
    )
    ms_range_parser.add_argument(
        "--import-eod",
        action="store_true",
        help="Import downloaded CSVs into eod_ohlcv table",
    )
    ms_range_parser.add_argument(
        "--pdf-fallback",
        action="store_true",
        help="Use PDF closing rates as fallback for failed .Z files",
    )

    # market-summary last (last N days)
    ms_last_parser = ms_sub.add_parser(
        "last", help="Download market summaries for the last N days"
    )
    ms_last_parser.add_argument(
        "--days",
        type=int,
        required=True,
        help="Number of days to look back",
    )
    ms_last_parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help=f"Output directory (default: {DATA_ROOT / 'market_summary'})",
    )
    ms_last_parser.add_argument(
        "--include-weekends",
        action="store_true",
        help="Include weekends (default: skip Sat/Sun)",
    )
    ms_last_parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if CSV already exists",
    )
    ms_last_parser.add_argument(
        "--keep-raw",
        action="store_true",
        help="Keep extracted raw files",
    )
    ms_last_parser.add_argument(
        "--import-eod",
        action="store_true",
        help="Import downloaded CSVs into eod_ohlcv table",
    )
    ms_last_parser.add_argument(
        "--pdf-fallback",
        action="store_true",
        help="Use PDF closing rates as fallback for failed .Z files",
    )

    # market-summary retry-failed
    ms_retry_failed_parser = ms_sub.add_parser(
        "retry-failed", help="Retry downloading dates that previously failed"
    )
    ms_retry_failed_parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help=f"Output directory (default: {DATA_ROOT / 'market_summary'})",
    )
    ms_retry_failed_parser.add_argument(
        "--keep-raw",
        action="store_true",
        help="Keep extracted raw files",
    )

    # market-summary retry-missing
    ms_retry_missing_parser = ms_sub.add_parser(
        "retry-missing", help="Retry downloading dates that were not found (404)"
    )
    ms_retry_missing_parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help=f"Output directory (default: {DATA_ROOT / 'market_summary'})",
    )
    ms_retry_missing_parser.add_argument(
        "--keep-raw",
        action="store_true",
        help="Keep extracted raw files",
    )

    # market-summary status (show status for a date)
    ms_status_parser = ms_sub.add_parser(
        "status", help="Show tracking status for a specific date"
    )
    ms_status_parser.add_argument(
        "--date",
        type=str,
        required=True,
        help="Date in YYYY-MM-DD format",
    )

    # market-summary list-missing
    ms_sub.add_parser(
        "list-missing", help="List dates with status='missing' (404)"
    )

    # market-summary list-failed
    ms_sub.add_parser(
        "list-failed", help="List dates with status='failed' (errors)"
    )

    # intraday command
    intraday_parser = subparsers.add_parser(
        "intraday",
        help="Intraday data operations"
    )
    intraday_sub = intraday_parser.add_subparsers(
        dest="intraday_command", required=True
    )

    # intraday sync
    intraday_sync_parser = intraday_sub.add_parser(
        "sync", help="Sync intraday data for a symbol"
    )
    intraday_sync_parser.add_argument(
        "--symbol",
        type=str,
        required=True,
        help="Stock symbol (e.g., 'OGDC')",
    )
    intraday_sync_parser.add_argument(
        "--no-incremental",
        action="store_true",
        help="Fetch all data, not just new data",
    )
    intraday_sync_parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Limit number of rows to keep (most recent)",
    )

    # intraday show
    intraday_show_parser = intraday_sub.add_parser(
        "show", help="Show intraday data for a symbol"
    )
    intraday_show_parser.add_argument(
        "--symbol",
        type=str,
        required=True,
        help="Stock symbol (e.g., 'OGDC')",
    )
    intraday_show_parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Number of rows to show (default: 200)",
    )
    intraday_show_parser.add_argument(
        "--out",
        type=str,
        choices=["csv", "table"],
        default="table",
        help="Output format (default: table)",
    )

    # intraday sync-all (bulk sync for all symbols - for cron jobs)
    intraday_sync_all_parser = intraday_sub.add_parser(
        "sync-all", help="Sync intraday data for all symbols (bulk)"
    )
    intraday_sync_all_parser.add_argument(
        "--no-incremental",
        action="store_true",
        help="Fetch all data, not just new data (full refresh)",
    )
    intraday_sync_all_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of symbols to sync (default: all)",
    )
    intraday_sync_all_parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Limit number of rows to keep per symbol (most recent)",
    )
    intraday_sync_all_parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Quiet mode - minimal output (useful for cron)",
    )

    # regular-market command
    rm_parser = subparsers.add_parser(
        "regular-market",
        help="Regular market data operations"
    )
    rm_sub = rm_parser.add_subparsers(dest="rm_command", required=True)

    # regular-market snapshot
    rm_snapshot_parser = rm_sub.add_parser(
        "snapshot", help="Fetch and store a single regular market snapshot"
    )
    rm_snapshot_parser.add_argument(
        "--csv",
        type=Path,
        default=None,
        help="Path to save CSV (default: data/regular_market/current.csv)",
    )
    rm_snapshot_parser.add_argument(
        "--save-unchanged",
        action="store_true",
        help="Save all rows to snapshots even if unchanged",
    )

    # regular-market listen
    rm_listen_parser = rm_sub.add_parser(
        "listen", help="Continuously poll regular market data"
    )
    rm_listen_parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Polling interval in seconds (default: 60)",
    )
    rm_listen_parser.add_argument(
        "--csv-dir",
        type=Path,
        default=None,
        help="Directory to save CSVs (default: data/regular_market)",
    )
    rm_listen_parser.add_argument(
        "--save-unchanged",
        action="store_true",
        help="Save all rows to snapshots even if unchanged",
    )

    # regular-market show
    rm_show_parser = rm_sub.add_parser(
        "show", help="Show current regular market data"
    )
    rm_show_parser.add_argument(
        "--out",
        type=str,
        choices=["csv", "table"],
        default="table",
        help="Output format (default: table)",
    )

    # sectors command
    sectors_parser = subparsers.add_parser(
        "sectors",
        help="Sector master data operations"
    )
    sectors_sub = sectors_parser.add_subparsers(
        dest="sectors_command", required=True
    )

    # sectors refresh
    sectors_sub.add_parser(
        "refresh", help="Refresh sectors from PSX sector-summary page"
    )

    # sectors list
    sectors_list_parser = sectors_sub.add_parser(
        "list", help="List all sectors"
    )
    sectors_list_parser.add_argument(
        "--out",
        type=str,
        choices=["csv", "table"],
        default="table",
        help="Output format (default: table)",
    )

    # sectors export
    sectors_export_parser = sectors_sub.add_parser(
        "export", help="Export sectors to CSV file"
    )
    sectors_export_parser.add_argument(
        "--out",
        type=str,
        default="data/sectors.csv",
        help="Output CSV path (default: data/sectors.csv)",
    )

    # master command (authoritative symbol & sector source)
    master_parser = subparsers.add_parser(
        "master",
        help="Symbol master data from listed_cmp.lst.Z (authoritative source)"
    )
    master_sub = master_parser.add_subparsers(
        dest="master_command", required=True
    )

    # master refresh
    master_refresh_parser = master_sub.add_parser(
        "refresh", help="Refresh symbols from official listed companies file"
    )
    master_refresh_parser.add_argument(
        "--deactivate-missing",
        action="store_true",
        help="Mark symbols not in master file as inactive",
    )

    # master list
    master_list_parser = master_sub.add_parser(
        "list", help="List all symbols from master"
    )
    master_list_parser.add_argument(
        "--out",
        type=str,
        choices=["csv", "table"],
        default="table",
        help="Output format (default: table)",
    )
    master_list_parser.add_argument(
        "--active-only",
        action="store_true",
        help="Show only active symbols",
    )

    # master export
    master_export_parser = master_sub.add_parser(
        "export", help="Export symbols to CSV file"
    )
    master_export_parser.add_argument(
        "--out",
        type=str,
        default="data/master/symbols.csv",
        help="Output CSV path (default: data/master/symbols.csv)",
    )

    # company command (DPS company page analytics)
    company_parser = subparsers.add_parser(
        "company",
        help="Company analytics from DPS company pages"
    )
    company_sub = company_parser.add_subparsers(
        dest="company_command", required=True
    )

    # company refresh - update profile and key people
    company_refresh_parser = company_sub.add_parser(
        "refresh", help="Refresh company profile and key people"
    )
    company_refresh_parser.add_argument(
        "--symbol",
        type=str,
        required=True,
        help="Stock symbol (e.g., OGDC)",
    )

    # company snapshot - take a quote snapshot
    company_snapshot_parser = company_sub.add_parser(
        "snapshot", help="Take a quote snapshot"
    )
    company_snapshot_parser.add_argument(
        "--symbol",
        type=str,
        help="Single stock symbol",
    )
    company_snapshot_parser.add_argument(
        "--symbols",
        type=str,
        help="Comma-separated list of symbols (e.g., OGDC,HBL,PSO)",
    )

    # company listen - continuous quote monitoring
    company_listen_parser = company_sub.add_parser(
        "listen", help="Continuously monitor quotes"
    )
    company_listen_parser.add_argument(
        "--symbol",
        type=str,
        help="Single stock symbol",
    )
    company_listen_parser.add_argument(
        "--symbols",
        type=str,
        help="Comma-separated list of symbols",
    )
    company_listen_parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Seconds between snapshots (default: 60)",
    )

    # company show - show stored profile/snapshots
    company_show_parser = company_sub.add_parser(
        "show", help="Show stored company data"
    )
    company_show_parser.add_argument(
        "--symbol",
        type=str,
        required=True,
        help="Stock symbol",
    )
    company_show_parser.add_argument(
        "--what",
        type=str,
        choices=["profile", "people", "quotes", "all"],
        default="all",
        help="What to show (default: all)",
    )

    # company sync-sectors - sync sector names to symbols table
    company_sub.add_parser(
        "sync-sectors",
        help="Sync sector names from company_profile to symbols table"
    )

    # company deep-scrape - full scrape including payouts, financials, etc.
    company_deep_parser = company_sub.add_parser(
        "deep-scrape",
        help="Deep scrape company page (payouts, financials, announcements)"
    )
    company_deep_parser.add_argument(
        "--symbol",
        type=str,
        help="Single stock symbol",
    )
    company_deep_parser.add_argument(
        "--symbols",
        type=str,
        help="Comma-separated list of symbols (e.g., OGDC,HBL,PSO)",
    )
    company_deep_parser.add_argument(
        "--all",
        action="store_true",
        help="Scrape all active symbols",
    )
    company_deep_parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Delay between requests in seconds (default: 1.0)",
    )
    company_deep_parser.add_argument(
        "--save-html",
        action="store_true",
        help="Save raw HTML to database (for debugging)",
    )

    # company import-payouts - import payouts from saved HTML file
    company_import_parser = company_sub.add_parser(
        "import-payouts",
        help="Import payouts from saved HTML page source (for JS-rendered data)"
    )
    company_import_parser.add_argument(
        "--symbol",
        type=str,
        required=True,
        help="Stock symbol for the imported data",
    )
    company_import_parser.add_argument(
        "--file",
        type=str,
        required=True,
        help="Path to saved HTML file (page source from browser)",
    )

    # company fetch-dividends - fetch dividend announcements from PSX
    company_sub.add_parser(
        "fetch-dividends",
        help="Fetch dividend announcements from PSX financial announcements page"
    )

    # announcements command - PSX company announcements sync
    ann_parser = subparsers.add_parser(
        "announcements",
        help="Company announcements, events, and dividend payouts sync"
    )
    ann_sub = ann_parser.add_subparsers(dest="ann_command", required=True)

    # announcements sync - run a one-time sync
    ann_sync_parser = ann_sub.add_parser(
        "sync", help="Sync all announcements, events, and dividends"
    )
    ann_sync_parser.add_argument(
        "--no-announcements",
        action="store_true",
        help="Skip syncing company announcements",
    )
    ann_sync_parser.add_argument(
        "--no-events",
        action="store_true",
        help="Skip syncing corporate events (AGM/EOGM)",
    )
    ann_sync_parser.add_argument(
        "--no-dividends",
        action="store_true",
        help="Skip syncing dividend payouts",
    )

    # announcements service - background service management
    ann_svc_parser = ann_sub.add_parser(
        "service", help="Manage background sync service"
    )
    ann_svc_parser.add_argument(
        "action",
        choices=["start", "stop", "status"],
        help="Service action",
    )
    ann_svc_parser.add_argument(
        "--interval",
        type=int,
        default=3600,
        help="Sync interval in seconds (default: 3600 = 1 hour)",
    )

    # announcements status - show sync status
    ann_sub.add_parser(
        "status", help="Show announcements sync status and stats"
    )

    # =========================================================================
    # Phase 1: universe command - instrument universe management
    # =========================================================================
    universe_parser = subparsers.add_parser(
        "universe",
        help="Manage instrument universe (Phase 1: ETFs, REITs, Indexes)"
    )
    universe_sub = universe_parser.add_subparsers(
        dest="universe_command", required=True
    )

    # universe seed-phase1 - seed instruments from config file
    universe_seed_parser = universe_sub.add_parser(
        "seed-phase1",
        help="Seed instruments table from universe_phase1.json config"
    )
    universe_seed_parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to universe config JSON (default: DATA_ROOT/universe_phase1.json)",
    )
    universe_seed_parser.add_argument(
        "--include-equities",
        action="store_true",
        help="Also seed equity symbols from symbols table",
    )

    # universe list - list instruments
    universe_list_parser = universe_sub.add_parser(
        "list", help="List instruments in universe"
    )
    universe_list_parser.add_argument(
        "--type",
        type=str,
        choices=["ETF", "REIT", "INDEX", "EQUITY", "ALL"],
        default="ALL",
        help="Filter by instrument type (default: ALL)",
    )
    universe_list_parser.add_argument(
        "--out",
        type=str,
        choices=["csv", "table"],
        default="table",
        help="Output format (default: table)",
    )
    universe_list_parser.add_argument(
        "--active-only",
        action="store_true",
        help="Show only active instruments",
    )

    # universe add - add a single instrument
    universe_add_parser = universe_sub.add_parser(
        "add", help="Add a new instrument to universe"
    )
    universe_add_parser.add_argument(
        "--type",
        type=str,
        choices=["ETF", "REIT", "INDEX"],
        required=True,
        help="Instrument type",
    )
    universe_add_parser.add_argument(
        "--symbol",
        type=str,
        required=True,
        help="Instrument symbol (e.g., NIUETF)",
    )
    universe_add_parser.add_argument(
        "--name",
        type=str,
        required=True,
        help="Instrument name (e.g., 'NIT Islamic Equity Fund')",
    )
    universe_add_parser.add_argument(
        "--source",
        type=str,
        choices=["DPS", "MANUAL"],
        default="DPS",
        help="Data source (default: DPS)",
    )

    # =========================================================================
    # Phase 1: instruments command - instrument data operations
    # =========================================================================
    instruments_parser = subparsers.add_parser(
        "instruments",
        help="Instrument data operations (Phase 1: sync EOD, rankings)"
    )
    instruments_sub = instruments_parser.add_subparsers(
        dest="instruments_command", required=True
    )

    # instruments sync-eod - sync EOD data for instruments
    inst_sync_parser = instruments_sub.add_parser(
        "sync-eod",
        help="Sync EOD OHLCV data for non-equity instruments"
    )
    inst_sync_parser.add_argument(
        "--types",
        type=str,
        default="ETF,REIT,INDEX",
        help="Comma-separated instrument types (default: ETF,REIT,INDEX)",
    )
    inst_sync_parser.add_argument(
        "--symbol",
        type=str,
        default=None,
        help="Sync single instrument by symbol",
    )
    inst_sync_parser.add_argument(
        "--incremental",
        action="store_true",
        default=True,
        help="Only fetch data newer than existing (default: True)",
    )
    inst_sync_parser.add_argument(
        "--full",
        action="store_true",
        help="Full refresh (ignore existing data)",
    )
    inst_sync_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of instruments to sync (for testing)",
    )

    # instruments rankings - compute and display rankings
    inst_rankings_parser = instruments_sub.add_parser(
        "rankings",
        help="Compute and display instrument performance rankings"
    )
    inst_rankings_parser.add_argument(
        "--as-of",
        type=str,
        default=None,
        help="Compute rankings as of date (YYYY-MM-DD, default: today)",
    )
    inst_rankings_parser.add_argument(
        "--types",
        type=str,
        default="ETF,REIT,INDEX",
        help="Comma-separated instrument types (default: ETF,REIT,INDEX)",
    )
    inst_rankings_parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Show top N instruments (default: 10)",
    )
    inst_rankings_parser.add_argument(
        "--compute",
        action="store_true",
        help="Force recompute rankings (even if already stored)",
    )
    inst_rankings_parser.add_argument(
        "--out",
        type=str,
        choices=["csv", "table"],
        default="table",
        help="Output format (default: table)",
    )

    # instruments sync-status - show recent sync runs
    instruments_sub.add_parser(
        "sync-status",
        help="Show recent instrument sync runs"
    )

    # =========================================================================
    # Phase 2: fx command - FX analytics
    # =========================================================================
    fx_parser = subparsers.add_parser(
        "fx",
        help="FX (Foreign Exchange) analytics (Phase 2: macro context)"
    )
    fx_sub = fx_parser.add_subparsers(
        dest="fx_command", required=True
    )

    # fx seed - seed default FX pairs
    fx_sub.add_parser(
        "seed",
        help="Seed default FX pairs (USD/PKR, EUR/PKR, GBP/PKR, etc.)"
    )

    # fx sync - sync FX OHLCV data
    fx_sync_parser = fx_sub.add_parser(
        "sync",
        help="Sync FX OHLCV data for pairs"
    )
    fx_sync_parser.add_argument(
        "--pairs",
        type=str,
        default=None,
        help="Comma-separated pairs to sync (default: all active)",
    )
    fx_sync_parser.add_argument(
        "--incremental",
        action="store_true",
        default=True,
        help="Only fetch data newer than existing (default: True)",
    )
    fx_sync_parser.add_argument(
        "--full",
        action="store_true",
        help="Full refresh (ignore existing data)",
    )
    fx_sync_parser.add_argument(
        "--source",
        type=str,
        choices=["AUTO", "SBP", "OPEN_API", "SAMPLE"],
        default="AUTO",
        help="Data source (default: AUTO)",
    )

    # fx show - show FX rate and analytics
    fx_show_parser = fx_sub.add_parser(
        "show",
        help="Show FX rate and analytics for a pair"
    )
    fx_show_parser.add_argument(
        "--pair",
        type=str,
        required=True,
        help="FX pair (e.g., USD/PKR)",
    )

    # fx compute-adjusted - compute FX-adjusted equity metrics
    fx_adjusted_parser = fx_sub.add_parser(
        "compute-adjusted",
        help="Compute FX-adjusted metrics for equities"
    )
    fx_adjusted_parser.add_argument(
        "--as-of",
        type=str,
        default=None,
        help="Date for metrics (YYYY-MM-DD, default: today)",
    )
    fx_adjusted_parser.add_argument(
        "--pair",
        type=str,
        default="USD/PKR",
        help="FX pair for adjustment (default: USD/PKR)",
    )
    fx_adjusted_parser.add_argument(
        "--symbols",
        type=str,
        default=None,
        help="Comma-separated symbols (default: all with recent data)",
    )

    # fx status - show FX data summary
    fx_sub.add_parser(
        "status",
        help="Show FX data summary and sync status"
    )

    # =========================================================================
    # Phase 2.5: mufap command - Mutual Fund analytics (MUFAP Integration)
    # =========================================================================
    mufap_parser = subparsers.add_parser(
        "mufap",
        help="Mutual Fund analytics (Phase 2.5: MUFAP data integration)"
    )
    mufap_sub = mufap_parser.add_subparsers(
        dest="mufap_command", required=True
    )

    # mufap seed - seed mutual fund master data
    mufap_seed_parser = mufap_sub.add_parser(
        "seed",
        help="Seed mutual fund master data from MUFAP"
    )
    mufap_seed_parser.add_argument(
        "--category",
        type=str,
        default=None,
        help="Filter by category (Equity, Money Market, Income, etc.)",
    )
    mufap_seed_parser.add_argument(
        "--no-vps",
        action="store_true",
        help="Exclude VPS (Voluntary Pension Scheme) funds",
    )
    mufap_seed_parser.add_argument(
        "--source",
        type=str,
        choices=["MUFAP", "SAMPLE"],
        default="MUFAP",
        help="Data source (default: MUFAP)",
    )

    # mufap sync - sync NAV data
    mufap_sync_parser = mufap_sub.add_parser(
        "sync",
        help="Sync NAV data for mutual funds"
    )
    mufap_sync_parser.add_argument(
        "--funds",
        type=str,
        default=None,
        help="Comma-separated fund IDs to sync (default: all active)",
    )
    mufap_sync_parser.add_argument(
        "--category",
        type=str,
        default=None,
        help="Filter by category code",
    )
    mufap_sync_parser.add_argument(
        "--incremental",
        action="store_true",
        default=True,
        help="Only fetch data newer than existing (default: True)",
    )
    mufap_sync_parser.add_argument(
        "--full",
        action="store_true",
        help="Full refresh (ignore existing data)",
    )
    mufap_sync_parser.add_argument(
        "--source",
        type=str,
        choices=["AUTO", "MUFAP", "SAMPLE"],
        default="AUTO",
        help="Data source (default: AUTO)",
    )

    # mufap show - show fund analytics
    mufap_show_parser = mufap_sub.add_parser(
        "show",
        help="Show analytics for a mutual fund"
    )
    mufap_show_parser.add_argument(
        "--fund",
        type=str,
        required=True,
        help="Fund ID or symbol (e.g., ABL-ISF)",
    )

    # mufap list - list funds
    mufap_list_parser = mufap_sub.add_parser(
        "list",
        help="List mutual funds"
    )
    mufap_list_parser.add_argument(
        "--category",
        type=str,
        default=None,
        help="Filter by category",
    )
    mufap_list_parser.add_argument(
        "--type",
        type=str,
        choices=["OPEN_END", "VPS", "ETF", "ALL"],
        default="ALL",
        help="Filter by fund type",
    )
    mufap_list_parser.add_argument(
        "--shariah-only",
        action="store_true",
        help="Show only Shariah-compliant funds",
    )

    # mufap rankings - show category rankings
    mufap_rankings_parser = mufap_sub.add_parser(
        "rankings",
        help="Show fund performance rankings by category"
    )
    mufap_rankings_parser.add_argument(
        "--category",
        type=str,
        required=True,
        help="Category (Equity, Money Market, Income, etc.)",
    )
    mufap_rankings_parser.add_argument(
        "--period",
        type=str,
        choices=["1W", "1M", "3M", "6M", "1Y"],
        default="1M",
        help="Return period (default: 1M)",
    )
    mufap_rankings_parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Show top N funds (default: 10)",
    )

    # mufap status - show sync status
    mufap_sub.add_parser(
        "status",
        help="Show mutual fund data summary and sync status"
    )

    # =========================================================================
    # Phase 3: bonds command - Bonds/Sukuk analytics
    # =========================================================================
    bonds_parser = subparsers.add_parser(
        "bonds",
        help="Bonds/Sukuk analytics (Phase 3: Fixed income)"
    )
    bonds_sub = bonds_parser.add_subparsers(
        dest="bonds_command", required=True
    )

    # bonds init - initialize bond tables and seed default bonds
    bonds_init_parser = bonds_sub.add_parser(
        "init",
        help="Initialize bond tables and seed default bonds"
    )
    bonds_init_parser.add_argument(
        "--type",
        type=str,
        choices=["PIB", "T-Bill", "Sukuk", "TFC", "ALL"],
        default="ALL",
        help="Filter by bond type (default: ALL)",
    )
    bonds_init_parser.add_argument(
        "--no-islamic",
        action="store_true",
        help="Exclude Islamic sukuk",
    )

    # bonds load - load bond data from CSV
    bonds_load_parser = bonds_sub.add_parser(
        "load",
        help="Load bond data from CSV file"
    )
    bonds_load_parser.add_argument(
        "--master",
        type=str,
        default=None,
        help="Path to bond master CSV file",
    )
    bonds_load_parser.add_argument(
        "--quotes",
        type=str,
        default=None,
        help="Path to bond quotes CSV file",
    )
    bonds_load_parser.add_argument(
        "--sample",
        action="store_true",
        help="Generate sample quote data",
    )
    bonds_load_parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Days of sample data to generate (default: 90)",
    )

    # bonds compute - compute analytics
    bonds_compute_parser = bonds_sub.add_parser(
        "compute",
        help="Compute bond analytics (YTM, duration, convexity)"
    )
    bonds_compute_parser.add_argument(
        "--bonds",
        type=str,
        default=None,
        help="Comma-separated bond IDs (default: all active)",
    )
    bonds_compute_parser.add_argument(
        "--as-of",
        type=str,
        default=None,
        help="Calculation date (default: today)",
    )
    bonds_compute_parser.add_argument(
        "--curve",
        action="store_true",
        help="Also build yield curve",
    )

    # bonds list - list bonds
    bonds_list_parser = bonds_sub.add_parser(
        "list",
        help="List bonds"
    )
    bonds_list_parser.add_argument(
        "--type",
        type=str,
        choices=["PIB", "T-Bill", "Sukuk", "TFC", "Corporate", "ALL"],
        default="ALL",
        help="Filter by bond type",
    )
    bonds_list_parser.add_argument(
        "--issuer",
        type=str,
        default=None,
        help="Filter by issuer",
    )
    bonds_list_parser.add_argument(
        "--islamic-only",
        action="store_true",
        help="Show only Islamic sukuk",
    )

    # bonds quote - show bond quote/analytics
    bonds_quote_parser = bonds_sub.add_parser(
        "quote",
        help="Show bond quote and analytics"
    )
    bonds_quote_parser.add_argument(
        "--bond",
        type=str,
        required=True,
        help="Bond ID or symbol",
    )

    # bonds curve - show yield curve
    bonds_curve_parser = bonds_sub.add_parser(
        "curve",
        help="Show yield curve"
    )
    bonds_curve_parser.add_argument(
        "--type",
        type=str,
        choices=["PIB", "T-Bill", "Sukuk", "ALL"],
        default="PIB",
        help="Bond type for curve (default: PIB)",
    )
    bonds_curve_parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Curve date (default: latest)",
    )

    # bonds status - show data status
    bonds_sub.add_parser(
        "status",
        help="Show bond data summary and sync status"
    )

    # =========================================================================
    # Phase 3: sukuk command - Sukuk/Debt Market analytics (additive)
    # =========================================================================
    sukuk_parser = subparsers.add_parser(
        "sukuk",
        help="Sukuk/Debt Market analytics (Phase 3: Fixed income - PSX GIS & SBP)"
    )
    sukuk_sub = sukuk_parser.add_subparsers(
        dest="sukuk_command", required=True
    )

    # sukuk seed - seed sukuk master data
    sukuk_seed_parser = sukuk_sub.add_parser(
        "seed",
        help="Seed sukuk master data (GOP Sukuk, PIB, T-Bills, etc.)"
    )
    sukuk_seed_parser.add_argument(
        "--category",
        type=str,
        choices=["GOP_SUKUK", "PIB", "TBILL", "CORPORATE_SUKUK", "TFC", "ALL"],
        default="ALL",
        help="Filter by category (default: ALL)",
    )
    sukuk_seed_parser.add_argument(
        "--shariah-only",
        action="store_true",
        help="Include only shariah-compliant instruments",
    )

    # sukuk sync - sync quotes and yield curves
    sukuk_sync_parser = sukuk_sub.add_parser(
        "sync",
        help="Sync sukuk quotes and yield curves"
    )
    sukuk_sync_parser.add_argument(
        "--instruments",
        type=str,
        default=None,
        help="Comma-separated instrument IDs (default: all)",
    )
    sukuk_sync_parser.add_argument(
        "--source",
        type=str,
        choices=["SAMPLE", "CSV"],
        default="SAMPLE",
        help="Data source (default: SAMPLE)",
    )
    sukuk_sync_parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Days of data for sample generation (default: 90)",
    )
    sukuk_sync_parser.add_argument(
        "--include-curves",
        action="store_true",
        help="Also generate yield curve data",
    )

    # sukuk load - load from CSV files
    sukuk_load_parser = sukuk_sub.add_parser(
        "load",
        help="Load sukuk data from CSV files"
    )
    sukuk_load_parser.add_argument(
        "--master",
        type=str,
        default=None,
        help="Path to sukuk master CSV file",
    )
    sukuk_load_parser.add_argument(
        "--quotes",
        type=str,
        default=None,
        help="Path to sukuk quotes CSV file",
    )
    sukuk_load_parser.add_argument(
        "--curve",
        type=str,
        default=None,
        help="Path to yield curve CSV file",
    )

    # sukuk compute - compute analytics
    sukuk_compute_parser = sukuk_sub.add_parser(
        "compute",
        help="Compute sukuk analytics (YTM, duration, convexity)"
    )
    sukuk_compute_parser.add_argument(
        "--instruments",
        type=str,
        default=None,
        help="Comma-separated instrument IDs (default: all active)",
    )
    sukuk_compute_parser.add_argument(
        "--as-of",
        type=str,
        default=None,
        help="Calculation date (default: today)",
    )

    # sukuk list - list sukuk instruments
    sukuk_list_parser = sukuk_sub.add_parser(
        "list",
        help="List sukuk instruments"
    )
    sukuk_list_parser.add_argument(
        "--category",
        type=str,
        choices=["GOP_SUKUK", "PIB", "TBILL", "CORPORATE_SUKUK", "TFC", "ALL"],
        default="ALL",
        help="Filter by category",
    )
    sukuk_list_parser.add_argument(
        "--issuer",
        type=str,
        default=None,
        help="Filter by issuer",
    )
    sukuk_list_parser.add_argument(
        "--shariah-only",
        action="store_true",
        help="Show only shariah-compliant instruments",
    )

    # sukuk show - show sukuk details and analytics
    sukuk_show_parser = sukuk_sub.add_parser(
        "show",
        help="Show sukuk details and analytics"
    )
    sukuk_show_parser.add_argument(
        "--instrument",
        type=str,
        required=True,
        help="Instrument ID",
    )

    # sukuk curve - show yield curve
    sukuk_curve_parser = sukuk_sub.add_parser(
        "curve",
        help="Show sukuk yield curve"
    )
    sukuk_curve_parser.add_argument(
        "--name",
        type=str,
        choices=["GOP_SUKUK", "PIB", "TBILL"],
        default="GOP_SUKUK",
        help="Curve name (default: GOP_SUKUK)",
    )
    sukuk_curve_parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Curve date (default: latest)",
    )

    # sukuk sbp - index SBP documents
    sukuk_sbp_parser = sukuk_sub.add_parser(
        "sbp",
        help="Index SBP primary market documents"
    )
    sukuk_sbp_parser.add_argument(
        "--docs-dir",
        type=str,
        default=None,
        help="Directory containing SBP documents",
    )
    sukuk_sbp_parser.add_argument(
        "--create-samples",
        action="store_true",
        help="Create sample placeholder documents",
    )

    # sukuk compare - compare multiple instruments
    sukuk_compare_parser = sukuk_sub.add_parser(
        "compare",
        help="Compare multiple sukuk instruments"
    )
    sukuk_compare_parser.add_argument(
        "--instruments",
        type=str,
        required=True,
        help="Comma-separated instrument IDs to compare",
    )

    # sukuk status - show data status
    sukuk_sub.add_parser(
        "status",
        help="Show sukuk data summary and sync status"
    )

    # =========================================================================
    # Phase 3.5: FIXED INCOME (Government Debt) commands
    # =========================================================================
    fi_parser = subparsers.add_parser(
        "fixed-income",
        help="Fixed Income analytics (Phase 3.5: MTB, PIB, GOP Sukuk)"
    )
    fi_sub = fi_parser.add_subparsers(
        dest="fi_command", required=True
    )

    # fixed-income seed - seed instrument master data
    fi_seed_parser = fi_sub.add_parser(
        "seed",
        help="Seed fixed income instruments (MTB, PIB, GOP Sukuk)"
    )
    fi_seed_parser.add_argument(
        "--source",
        type=str,
        choices=["SAMPLE", "CSV"],
        default="SAMPLE",
        help="Data source (default: SAMPLE)",
    )
    fi_seed_parser.add_argument(
        "--csv",
        type=str,
        default=None,
        help="Path to instruments CSV file (when source=CSV)",
    )

    # fixed-income sync - sync quotes and curves
    fi_sync_parser = fi_sub.add_parser(
        "sync",
        help="Sync fixed income quotes and yield curves"
    )
    fi_sync_parser.add_argument(
        "--source",
        type=str,
        choices=["SAMPLE", "CSV"],
        default="SAMPLE",
        help="Data source (default: SAMPLE)",
    )
    fi_sync_parser.add_argument(
        "--quotes-csv",
        type=str,
        default=None,
        help="Path to quotes CSV file",
    )
    fi_sync_parser.add_argument(
        "--curves-csv",
        type=str,
        default=None,
        help="Path to yield curves CSV file",
    )
    fi_sync_parser.add_argument(
        "--all",
        action="store_true",
        dest="sync_all",
        help="Sync instruments, quotes, curves, and SBP docs",
    )

    # fixed-income compute - compute bond analytics
    fi_compute_parser = fi_sub.add_parser(
        "compute",
        help="Compute bond analytics (YTM, duration, convexity)"
    )
    fi_compute_parser.add_argument(
        "--isins",
        type=str,
        default=None,
        help="Comma-separated ISINs (default: all active)",
    )
    fi_compute_parser.add_argument(
        "--as-of",
        type=str,
        default=None,
        help="Calculation date (default: today)",
    )

    # fixed-income list - list instruments
    fi_list_parser = fi_sub.add_parser(
        "list",
        help="List fixed income instruments"
    )
    fi_list_parser.add_argument(
        "--category",
        type=str,
        choices=["MTB", "PIB", "GOP_SUKUK", "CORP_BOND", "CORP_SUKUK", "ALL"],
        default="ALL",
        help="Filter by category",
    )
    fi_list_parser.add_argument(
        "--min-yield",
        type=float,
        default=None,
        help="Minimum yield filter",
    )
    fi_list_parser.add_argument(
        "--sort",
        type=str,
        choices=["yield", "duration", "maturity"],
        default="yield",
        help="Sort field (default: yield)",
    )

    # fixed-income show - show instrument details
    fi_show_parser = fi_sub.add_parser(
        "show",
        help="Show fixed income instrument details and analytics"
    )
    fi_show_parser.add_argument(
        "--isin",
        type=str,
        required=True,
        help="Instrument ISIN",
    )

    # fixed-income curve - show yield curve
    fi_curve_parser = fi_sub.add_parser(
        "curve",
        help="Show yield curve data"
    )
    fi_curve_parser.add_argument(
        "--name",
        type=str,
        choices=["PKR_MTB", "PKR_PIB", "PKR_GOP_SUKUK"],
        default="PKR_MTB",
        help="Curve name (default: PKR_MTB)",
    )
    fi_curve_parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Curve date (default: latest)",
    )

    # fixed-income sbp - sync SBP PMA documents
    fi_sbp_parser = fi_sub.add_parser(
        "sbp",
        help="Sync SBP Primary Market Activities documents"
    )
    fi_sbp_parser.add_argument(
        "--source",
        type=str,
        choices=["SBP", "SAMPLE"],
        default="SBP",
        help="Data source (default: SBP)",
    )
    fi_sbp_parser.add_argument(
        "--download",
        action="store_true",
        help="Also download PDF files",
    )
    fi_sbp_parser.add_argument(
        "--category",
        type=str,
        choices=["MTB", "PIB", "GOP_SUKUK"],
        default=None,
        help="Filter by category",
    )

    # fixed-income templates - create CSV templates
    fi_sub.add_parser(
        "templates",
        help="Create CSV template files for manual data entry"
    )

    # fixed-income status - show data status
    fi_sub.add_parser(
        "status",
        help="Show fixed income data summary and sync status"
    )

    # fixed-income service - background sync service
    fi_service_parser = fi_sub.add_parser(
        "service",
        help="Manage FI background sync service"
    )
    fi_service_parser.add_argument(
        "action",
        type=str,
        choices=["start", "stop", "status"],
        help="Service action",
    )
    fi_service_parser.add_argument(
        "--continuous",
        action="store_true",
        help="Run continuously (auto-sync every interval)",
    )
    fi_service_parser.add_argument(
        "--interval",
        type=int,
        default=3600,
        help="Sync interval in seconds (default: 3600 = 1 hour)",
    )

    # =========================================================================
    # v3.0: ETF commands
    # =========================================================================
    etf_parser = subparsers.add_parser(
        "etf",
        help="ETF data collection (v3.0: NAV, metadata, premium/discount)"
    )
    etf_sub = etf_parser.add_subparsers(dest="etf_command", required=True)

    etf_sub.add_parser("sync", help="Scrape all ETFs from PSX DPS")
    etf_sub.add_parser("list", help="List all ETFs with latest NAV")

    etf_show_parser = etf_sub.add_parser("show", help="Show ETF detail")
    etf_show_parser.add_argument(
        "symbol", type=str, help="ETF symbol (e.g., MZNPETF)"
    )

    # =========================================================================
    # v3.0: Treasury commands (T-Bill + PIB auctions)
    # =========================================================================
    treasury_parser = subparsers.add_parser(
        "treasury",
        help="Treasury auction data (T-Bills, PIBs from SBP)"
    )
    treasury_sub = treasury_parser.add_subparsers(
        dest="treasury_command", required=True
    )

    treasury_sub.add_parser("sync", help="Scrape latest T-Bill + PIB rates from SBP")
    treasury_sub.add_parser("tbill-latest", help="Show latest T-Bill cutoff yields")
    treasury_sub.add_parser("pib-latest", help="Show latest PIB cutoff yields")

    treasury_list_parser = treasury_sub.add_parser(
        "tbill-list", help="List T-Bill auction history"
    )
    treasury_list_parser.add_argument(
        "--tenor", type=str, help="Filter by tenor (e.g., 3M, 6M, 12M)"
    )
    treasury_list_parser.add_argument(
        "--limit", type=int, default=20, help="Max rows (default: 20)"
    )

    treasury_sub.add_parser("gis-sync", help="Scrape GIS (Ijara Sukuk) auction data")
    treasury_sub.add_parser("gis-list", help="List GIS auction history")
    treasury_sub.add_parser("summary", help="Show treasury data summary")

    # =========================================================================
    # v3.0: Rates commands (PKRV yield curve, KONIA, KIBOR)
    # =========================================================================
    rates_parser = subparsers.add_parser(
        "rates",
        help="Yield curve & overnight rates (PKRV, KONIA, KIBOR)"
    )
    rates_sub = rates_parser.add_subparsers(
        dest="rates_command", required=True
    )

    rates_sub.add_parser("sync", help="Scrape KONIA + KIBOR + yield curve from SBP")
    rates_sub.add_parser("konia", help="Show latest KONIA overnight rate")
    rates_sub.add_parser("kibor", help="Show latest KIBOR rates")

    rates_curve_parser = rates_sub.add_parser(
        "curve", help="Show PKRV yield curve"
    )
    rates_curve_parser.add_argument(
        "--date", type=str, help="Date (YYYY-MM-DD), defaults to latest"
    )

    rates_sub.add_parser("summary", help="Show rates data summary")

    # =========================================================================
    # v3.0: FX Extended commands (SBP interbank, open market, kerb)
    # =========================================================================
    fxe_parser = subparsers.add_parser(
        "fx-rates",
        help="FX rates — SBP interbank, open market, kerb (forex.pk)"
    )
    fxe_sub = fxe_parser.add_subparsers(
        dest="fxe_command", required=True
    )

    fxe_sub.add_parser("sbp-sync", help="Scrape SBP interbank USD/PKR rates")
    fxe_sub.add_parser("kerb-sync", help="Scrape kerb rates from forex.pk")
    fxe_sub.add_parser("sync-all", help="Sync both SBP interbank and kerb rates")

    fxe_latest_parser = fxe_sub.add_parser("latest", help="Show latest FX rates")
    fxe_latest_parser.add_argument(
        "--source", choices=["interbank", "kerb", "all"], default="all",
        help="Rate source (default: all)"
    )

    fxe_spread_parser = fxe_sub.add_parser("spread", help="Show FX spread for a currency")
    fxe_spread_parser.add_argument("currency", help="Currency code (e.g. USD, EUR)")

    fxe_sub.add_parser("summary", help="Show FX data summary")

    # =========================================================================
    # v3.0: Dividends commands
    # =========================================================================
    div_parser = subparsers.add_parser(
        "dividends",
        help="Dividend history, yields, and rankings"
    )
    div_sub = div_parser.add_subparsers(
        dest="div_command", required=True
    )

    div_show_parser = div_sub.add_parser("show", help="Show dividend history for a symbol")
    div_show_parser.add_argument("symbol", help="Stock symbol (e.g. OGDC)")
    div_show_parser.add_argument(
        "--years", type=int, default=None, help="Limit to last N years"
    )

    div_yield_parser = div_sub.add_parser("yield", help="Show dividend yield for a symbol")
    div_yield_parser.add_argument("symbol", help="Stock symbol (e.g. OGDC)")
    div_yield_parser.add_argument(
        "--years", type=int, default=1, help="Lookback period (default: 1 year)"
    )

    div_top_parser = div_sub.add_parser("top", help="Top dividend yield stocks")
    div_top_parser.add_argument(
        "--n", type=int, default=20, help="Number of stocks to show (default: 20)"
    )
    div_top_parser.add_argument(
        "--years", type=int, default=1, help="Lookback period (default: 1 year)"
    )

    div_sub.add_parser("upcoming", help="Show upcoming ex-dividend dates")

    # =========================================================================
    # v3.0: IPO commands
    # =========================================================================
    ipo_parser = subparsers.add_parser(
        "ipo",
        help="IPO calendar & listing status"
    )
    ipo_sub = ipo_parser.add_subparsers(
        dest="ipo_command", required=True
    )

    ipo_sub.add_parser("sync", help="Scrape IPO listings from PSX")

    ipo_list_parser = ipo_sub.add_parser("list", help="List IPO records")
    ipo_list_parser.add_argument(
        "--status", type=str, default=None,
        help="Filter by status (upcoming, listed, open)"
    )
    ipo_list_parser.add_argument(
        "--board", type=str, default=None,
        help="Filter by board (main, gem)"
    )

    ipo_sub.add_parser("upcoming", help="Show upcoming IPOs")
    ipo_sub.add_parser("recent", help="Show recently listed IPOs")

    ipo_show_parser = ipo_sub.add_parser("show", help="Show IPO details for a symbol")
    ipo_show_parser.add_argument("symbol", help="Stock symbol")

    # =========================================================================
    # v3.0: VPS (Voluntary Pension System) commands
    # =========================================================================
    vps_parser = subparsers.add_parser(
        "vps",
        help="VPS pension fund data (NAV, performance)"
    )
    vps_sub = vps_parser.add_subparsers(
        dest="vps_command", required=True
    )

    vps_sub.add_parser("list", help="List VPS pension funds")

    vps_nav_parser = vps_sub.add_parser("nav", help="Show NAV history for a VPS fund")
    vps_nav_parser.add_argument("fund_id", help="VPS fund ID (e.g. MUFAP:ABL-VPS-EQ)")

    vps_perf_parser = vps_sub.add_parser("performance", help="Compare VPS fund performance")
    vps_perf_parser.add_argument(
        "--days", type=int, default=365, help="Lookback period in days (default: 365)"
    )

    vps_sub.add_parser("summary", help="Show VPS data summary")

    # =========================================================================
    # v3.0: Unified sync-all + status
    # =========================================================================
    subparsers.add_parser(
        "sync-all",
        help="Run all data scrapers (treasury, rates, FX, ETF, IPO)"
    )

    # backfill-rates: Historical rates backfill from SBP PDFs
    backfill_parser = subparsers.add_parser(
        "backfill-rates",
        help="Backfill historical rates from SBP PDFs (SIR, PIB archive, KIBOR daily)"
    )
    backfill_parser.add_argument(
        "--source",
        choices=["sir", "pib", "kibor", "all"],
        default="all",
        help="Data source to backfill (default: all)",
    )
    backfill_parser.add_argument(
        "--start-year",
        type=int,
        default=2008,
        help="Start year for KIBOR PDF backfill (default: 2008)",
    )
    subparsers.add_parser(
        "status",
        help="Show data freshness dashboard for all domains"
    )

    # Tick collector — live OHLCV from market-watch polling
    tick_parser = subparsers.add_parser(
        "collect-ticks",
        help="Start live tick collector (polls market-watch every N seconds)"
    )
    tick_parser.add_argument(
        "--interval",
        type=int,
        default=5,
        help="Poll interval in seconds (default: 5)",
    )

    # WebSocket tick service — real-time from psxterminal.com
    ts_parser = subparsers.add_parser(
        "tick-service",
        help="Manage WebSocket tick service (psxterminal.com)"
    )
    ts_sub = ts_parser.add_subparsers(dest="ts_action")
    ts_sub.add_parser("start", help="Start tick service in foreground")
    ts_sub.add_parser("daemon", help="Start tick service as background daemon")
    ts_sub.add_parser("stop", help="Stop running tick service")
    ts_sub.add_parser("status", help="Show tick service status")

    args = parser.parse_args(argv)

    try:
        if args.command == "symbols":
            return handle_symbols(args)
        elif args.command == "sync":
            return handle_sync(args)
        elif args.command == "quote":
            return handle_quote(args)
        elif args.command == "ohlcv":
            return handle_ohlcv(args)
        elif args.command == "market-summary":
            return handle_market_summary(args)
        elif args.command == "intraday":
            return handle_intraday(args)
        elif args.command == "regular-market":
            return handle_regular_market(args)
        elif args.command == "sectors":
            return handle_sectors(args)
        elif args.command == "master":
            return handle_master(args)
        elif args.command == "company":
            return handle_company(args)
        elif args.command == "announcements":
            return handle_announcements(args)
        # Phase 1 commands
        elif args.command == "universe":
            return handle_universe(args)
        elif args.command == "instruments":
            return handle_instruments(args)
        # Phase 2 commands
        elif args.command == "fx":
            return handle_fx(args)
        # Phase 2.5 commands
        elif args.command == "mufap":
            return handle_mufap(args)
        # Phase 3 commands
        elif args.command == "bonds":
            return handle_bonds(args)
        elif args.command == "sukuk":
            return handle_sukuk(args)
        # Phase 3.5: Fixed Income commands
        elif args.command == "fixed-income":
            return handle_fixed_income(args)
        # v3.0 commands
        elif args.command == "etf":
            return handle_etf(args)
        elif args.command == "treasury":
            return handle_treasury(args)
        elif args.command == "rates":
            return handle_rates(args)
        elif args.command == "fx-rates":
            return handle_fx_rates(args)
        elif args.command == "dividends":
            return handle_dividends(args)
        elif args.command == "ipo":
            return handle_ipo(args)
        elif args.command == "vps":
            return handle_vps(args)
        elif args.command == "sync-all":
            return handle_sync_all(args)
        elif args.command == "backfill-rates":
            return handle_backfill_rates(args)
        elif args.command == "status":
            return handle_status(args)
        elif args.command == "collect-ticks":
            return handle_collect_ticks(args)
        elif args.command == "tick-service":
            return handle_tick_service(args)
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        return 130
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    return 0


def handle_collect_ticks(args: argparse.Namespace) -> int:
    """Start live tick collector in foreground. Ctrl+C to stop (auto-saves OHLCV)."""
    from .collectors.tick_collector import TickCollector
    from .db.repositories.tick import init_tick_schema

    interval = getattr(args, "interval", 5)
    con = connect()
    init_schema(con)
    init_tick_schema(con)

    collector = TickCollector(interval=interval)
    print(f"Starting tick collector (interval={interval}s). Press Ctrl+C to stop.")
    print("OHLCV will be auto-saved to DB on stop.\n")

    collector._session = __import__("psx_ohlcv.http", fromlist=["create_session"]).create_session()
    collector.started_at = __import__("datetime").datetime.now()

    try:
        while True:
            stats = collector.poll_once()
            ts = collector.last_poll_time.strftime("%H:%M:%S") if collector.last_poll_time else "?"
            print(
                f"[{ts}] Poll #{collector.poll_count}: "
                f"+{stats['new_ticks']} ticks, "
                f"{stats['skipped']} skipped, "
                f"{len(collector.running_ohlcv)} symbols tracked, "
                f"total={collector.total_ticks}",
                flush=True,
            )
            import time as _time
            _time.sleep(interval)
    except KeyboardInterrupt:
        print("\nStopping collector...")
        n = collector.save_ohlcv_to_db()
        print(f"Saved {n} OHLCV rows to tick_ohlcv table.")
        nt = collector.save_ticks_to_db()
        print(f"Saved {nt} raw ticks to tick_data table.")
        return 0


def handle_tick_service(args: argparse.Namespace) -> int:
    """Manage WebSocket tick service."""
    from .services.tick_service import (
        is_tick_service_running,
        read_status,
        stop_tick_service,
        start_tick_service_background,
    )

    action = getattr(args, "ts_action", None)
    if action is None:
        print("Usage: psxsync tick-service {start|daemon|stop|status}")
        return 1

    if action == "start":
        import asyncio
        from .services.tick_service import main as ts_main
        asyncio.run(ts_main())
        return 0

    elif action == "daemon":
        ok, msg = start_tick_service_background()
        print(msg)
        return 0 if ok else 1

    elif action == "stop":
        ok, msg = stop_tick_service()
        print(msg)
        return 0 if ok else 1

    elif action == "status":
        running, pid = is_tick_service_running()
        status = read_status()
        if running:
            print(f"Tick service: RUNNING (PID {pid})")
            print(f"  Connected: {status.connected}")
            print(f"  Ticks:     {status.tick_count:,}")
            print(f"  Bars:      {status.bars_saved:,}")
            print(f"  Symbols:   {status.symbol_count}")
            print(f"  Started:   {status.started_at}")
        else:
            print("Tick service: STOPPED")
        return 0

    return 1


def handle_symbols(args: argparse.Namespace) -> int:
    """Handle symbols subcommands."""
    if args.symbols_command == "refresh":
        print("Refreshing symbols from market watch...")
        result = refresh_symbols(args.db)
        print(
            f"Found {result.symbols_found} symbols, "
            f"upserted {result.symbols_upserted}"
        )
        return 0

    elif args.symbols_command == "list":
        con = connect(args.db)
        init_schema(con)

        cur = con.execute(
            """SELECT symbol, name, sector, sector_name, is_active
               FROM symbols ORDER BY symbol"""
        )
        rows = cur.fetchall()
        con.close()

        if not rows:
            print("No symbols found. Run 'psxsync symbols refresh' first.")
            return 0

        if args.format == "csv":
            print("symbol,name,sector_code,sector_name,is_active")
            for row in rows:
                name = row["name"] or ""
                sector_code = row["sector"] or ""
                sector_name = row["sector_name"] or sector_code
                print(f"{row['symbol']},{name},{sector_code},{sector_name},{row['is_active']}")
        else:
            # Table format - use sector_name for display
            print(f"{'SYMBOL':<12} {'NAME':<30} {'SECTOR':<30} {'ACTIVE'}")
            print("-" * 80)
            for row in rows:
                name = (row["name"] or "")[:30]
                # Prefer sector_name, fall back to sector code
                sector = (row["sector_name"] or row["sector"] or "")[:30]
                active = "Yes" if row["is_active"] else "No"
                print(f"{row['symbol']:<12} {name:<30} {sector:<30} {active}")
            print(f"\nTotal: {len(rows)} symbols")

        return 0

    elif args.symbols_command == "string":
        con = connect(args.db)
        init_schema(con)
        result = get_symbols_string(con, limit=args.limit)
        con.close()

        if not result:
            print(
                "No symbols found. Run 'psxsync symbols refresh' first.",
                file=sys.stderr,
            )
            return 1

        print(result)
        return 0

    return 1


def handle_sync(args: argparse.Namespace) -> int:
    """Handle sync command."""
    from .query import get_symbols_list

    # Setup logging
    setup_logging()

    # Parse explicit symbols list if provided
    symbols_list = None
    if args.symbols:
        symbols_list = [s.strip() for s in args.symbols.split(",") if s.strip()]

    # Determine if we should sync
    if not args.sync_all and not symbols_list:
        print("Use --all to sync all symbols, or --symbols to specify symbols")
        return EXIT_ERROR

    # Check if we have symbols when using --all
    if args.sync_all and not symbols_list:
        con = connect(args.db)
        init_schema(con)
        existing_symbols = get_symbols_list(con)
        con.close()

        if not existing_symbols and not args.refresh_symbols:
            print(
                "No symbols to sync. "
                "Run 'psxsync symbols refresh' first or use --refresh-symbols."
            )
            return EXIT_ERROR

    # Build config
    config = SyncConfig(
        max_retries=args.max_retries,
        delay_min=args.delay_min,
        delay_max=args.delay_max,
        timeout=args.timeout,
        incremental=args.incremental,
    )

    # Run sync (async or sequential)
    mode_str = "incremental" if args.incremental else "full"

    if getattr(args, "use_async", False):
        import asyncio
        from .sync_async import sync_all_async

        print(f"Starting ASYNC EOD sync ({mode_str} mode)...")
        summary = asyncio.run(sync_all_async(
            db_path=args.db,
            refresh_symbols=args.refresh_symbols,
            limit_symbols=args.limit_symbols if args.sync_all else None,
            symbols_list=symbols_list,
            config=config,
        ))
        print_async_summary(summary)
    else:
        print(f"Starting EOD sync ({mode_str} mode)...")
        summary = sync_all(
            db_path=args.db,
            refresh_symbols=args.refresh_symbols,
            limit_symbols=args.limit_symbols if args.sync_all else None,
            symbols_list=symbols_list,
            config=config,
        )
        print_summary(summary)

    # Exit code: 0 if any symbol succeeded, 2 if all failed
    if summary.symbols_total == 0:
        return EXIT_SUCCESS
    if summary.symbols_ok > 0:
        return EXIT_SUCCESS
    return EXIT_ALL_FAILED


def print_summary(summary: SyncSummary) -> None:
    """Print sync summary with failure breakdown."""
    print("\n" + "=" * 50)
    print("SYNC SUMMARY")
    print("=" * 50)
    print(f"  Symbols total:  {summary.symbols_total}")
    print(f"  Symbols OK:     {summary.symbols_ok}")
    print(f"  Symbols failed: {summary.symbols_failed}")
    print(f"  Rows upserted:  {summary.rows_upserted}")
    print(f"  Run ID:         {summary.run_id}")

    if summary.failures:
        print("\n" + "-" * 50)
        print("TOP FAILURES BY ERROR TYPE:")
        print("-" * 50)

        # Count by error type
        error_counts = Counter(f["error_type"] for f in summary.failures)
        for error_type, count in error_counts.most_common():
            print(f"  {error_type}: {count}")

        print("\n" + "-" * 50)
        print("FAILED SYMBOLS (first 10):")
        print("-" * 50)
        for f in summary.failures[:10]:
            msg = f["error_message"][:60] if f["error_message"] else "N/A"
            print(f"  {f['symbol']:<10} {f['error_type']:<12} {msg}")
        if len(summary.failures) > 10:
            print(f"  ... and {len(summary.failures) - 10} more")

    print("=" * 50)


def print_async_summary(summary) -> None:
    """Print async sync summary."""
    print("\n" + "=" * 50)
    print("ASYNC SYNC SUMMARY")
    print("=" * 50)
    print(f"  Symbols total:  {summary.symbols_total}")
    print(f"  Symbols OK:     {summary.symbols_ok}")
    print(f"  Symbols failed: {summary.symbols_failed}")
    print(f"  Rows upserted:  {summary.rows_upserted}")
    print(f"  Elapsed:        {summary.elapsed:.1f}s")
    print(f"  Run ID:         {summary.run_id}")

    if summary.failures:
        print("\n" + "-" * 50)
        print("TOP FAILURES BY ERROR TYPE:")
        print("-" * 50)
        error_counts = Counter(f["error_type"] for f in summary.failures)
        for error_type, count in error_counts.most_common():
            print(f"  {error_type}: {count}")
        print("\nFAILED SYMBOLS (first 10):")
        for f in summary.failures[:10]:
            msg = f["error_message"][:60] if f["error_message"] else "N/A"
            print(f"  {f['symbol']:<10} {f['error_type']:<12} {msg}")
        if len(summary.failures) > 10:
            print(f"  ... and {len(summary.failures) - 10} more")

    print("=" * 50)


def handle_quote(args: argparse.Namespace) -> int:
    """Handle quote command."""
    con = connect(args.db)
    init_schema(con)
    quote = get_latest_close(con, args.symbol)
    con.close()

    if quote is None:
        print(f"No data found for symbol: {args.symbol}", file=sys.stderr)
        return 1

    print(f"Symbol:  {quote['symbol']}")
    print(f"Date:    {quote['date']}")
    print(f"Open:    {quote['open']:.2f}" if quote["open"] else "Open:    N/A")
    print(f"High:    {quote['high']:.2f}" if quote["high"] else "High:    N/A")
    print(f"Low:     {quote['low']:.2f}" if quote["low"] else "Low:     N/A")
    print(f"Close:   {quote['close']:.2f}" if quote["close"] else "Close:   N/A")
    print(f"Volume:  {quote['volume']:,}" if quote["volume"] else "Volume:  N/A")

    return 0


def handle_ohlcv(args: argparse.Namespace) -> int:
    """Handle ohlcv command."""
    con = connect(args.db)
    init_schema(con)
    df = get_ohlcv_range(
        con, args.symbol, start_date=args.start, end_date=args.end
    )
    con.close()

    if df.empty:
        print(f"No data found for symbol: {args.symbol}", file=sys.stderr)
        return 1

    if args.out == "csv":
        print(df.to_csv(index=False))
    else:
        # Table format
        print(f"{'DATE':<12} {'OPEN':>10} {'HIGH':>10} {'LOW':>10} "
              f"{'CLOSE':>10} {'VOLUME':>12}")
        print("-" * 68)
        for _, row in df.iterrows():
            open_val = f"{row['open']:.2f}" if row["open"] else "N/A"
            high_val = f"{row['high']:.2f}" if row["high"] else "N/A"
            low_val = f"{row['low']:.2f}" if row["low"] else "N/A"
            close_val = f"{row['close']:.2f}" if row["close"] else "N/A"
            vol_val = f"{row['volume']:,}" if row["volume"] else "N/A"
            print(
                f"{row['date']:<12} {open_val:>10} {high_val:>10} "
                f"{low_val:>10} {close_val:>10} {vol_val:>12}"
            )
        print(f"\nTotal: {len(df)} rows for {args.symbol}")

    return 0


def handle_market_summary(args: argparse.Namespace) -> int:
    """Handle market-summary subcommands."""
    if args.ms_command == "day":
        return handle_market_summary_day(args)
    elif args.ms_command == "range":
        return handle_market_summary_range(args)
    elif args.ms_command == "last":
        return handle_market_summary_last(args)
    elif args.ms_command == "retry-failed":
        return handle_market_summary_retry_failed(args)
    elif args.ms_command == "retry-missing":
        return handle_market_summary_retry_missing(args)
    elif args.ms_command == "status":
        return handle_market_summary_status(args)
    elif args.ms_command == "list-missing":
        return handle_market_summary_list_missing(args)
    elif args.ms_command == "list-failed":
        return handle_market_summary_list_failed(args)
    return EXIT_ERROR


def handle_market_summary_day(args: argparse.Namespace) -> int:
    """Handle market-summary day command."""
    from .sources.market_summary import (
        fetch_day_with_tracking,
        init_market_summary_tracking,
    )
    from .db import ingest_market_summary_csv

    date_str = args.date
    print(f"Downloading market summary for {date_str}...")

    con = connect(args.db)
    init_schema(con)
    init_market_summary_tracking(con)

    result = fetch_day_with_tracking(
        con,
        date_str,
        out_dir=args.out_dir,
        force=args.force,
        keep_raw=args.keep_raw,
    )

    # PDF fallback if .Z file failed
    pdf_used = False
    if getattr(args, 'pdf_fallback', False) and result["status"] == "failed":
        from .sources.closing_rates_pdf import fetch_day as fetch_pdf_day
        print(f"\n  .Z file failed, trying PDF fallback...")
        pdf_result = fetch_pdf_day(
            date_str,
            out_dir=args.out_dir,
            force=args.force,
        )
        if pdf_result["status"] == "ok":
            result = pdf_result
            result["message"] = "Recovered from PDF fallback"
            pdf_used = True

    print(f"\nMarket Summary: {date_str}")
    print("=" * 50)
    print(f"  Status:         {result['status']}{' (PDF fallback)' if pdf_used else ''}")
    print(f"  Row Count:      {result['row_count']}")
    if result["csv_path"]:
        print(f"  CSV saved to:   {result['csv_path']}")
    if result["message"]:
        print(f"  Message:        {result['message']}")

    # Import to eod_ohlcv if requested
    if args.import_eod and result["csv_path"] and result["status"] in ("ok", "skipped"):
        print(f"\nImporting to eod_ohlcv table...")
        # Set source based on whether PDF fallback was used
        source = "closing_rates_pdf" if pdf_used else "market_summary"
        ingest_result = ingest_market_summary_csv(
            con, result["csv_path"], skip_existing=not args.force, source=source
        )
        print(f"  Import Status:  {ingest_result['status']}")
        print(f"  Rows Inserted:  {ingest_result['rows_inserted']}")
        print(f"  Source:         {source}")
        if ingest_result["message"]:
            print(f"  Message:        {ingest_result['message']}")

    con.close()

    if result["status"] in ("ok", "skipped"):
        return EXIT_SUCCESS
    elif result["status"] == "missing":
        return EXIT_SUCCESS  # Not an error, just no data
    else:
        return EXIT_ERROR


def handle_market_summary_range(args: argparse.Namespace) -> int:
    """Handle market-summary range command."""
    from .db import ingest_all_market_summary_csvs, ingest_market_summary_csv

    try:
        start_date = parse_date(args.start)
        end_date = parse_date(args.end)
    except ValueError as e:
        print(f"Invalid date: {e}", file=sys.stderr)
        return EXIT_ERROR

    skip_weekends = not args.include_weekends
    mode_str = "excluding weekends" if skip_weekends else "including weekends"

    print(f"Downloading market summaries from {args.start} to {args.end}")
    print(f"({mode_str}, force={args.force})")
    print("-" * 50)

    summary = fetch_range_summary(
        start=start_date,
        end=end_date,
        out_dir=args.out_dir,
        skip_weekends=skip_weekends,
        force=args.force,
        keep_raw=args.keep_raw,
    )

    # PDF fallback for failed dates (only when .Z file fails)
    pdf_recovered = 0
    pdf_recovered_dates = []  # Track PDF-recovered dates for separate ingestion
    if getattr(args, 'pdf_fallback', False) and summary["failed"]:
        from .sources.closing_rates_pdf import fetch_day as fetch_pdf_day
        print("\nRetrying failed dates with PDF fallback...")
        remaining_failed = []
        for err in summary["failed"]:
            pdf_result = fetch_pdf_day(err['date'], out_dir=args.out_dir, force=args.force)
            if pdf_result["status"] == "ok":
                pdf_recovered += 1
                pdf_recovered_dates.append((err['date'], pdf_result['csv_path']))
                print(f"  {err['date']}: Recovered from PDF ({pdf_result['row_count']} rows)")
            else:
                remaining_failed.append(err)
        summary["failed"] = remaining_failed
        summary["ok"] += pdf_recovered

    print("\nMarket Summary Range Download")
    print("=" * 50)
    print(f"  Date range:     {summary['start']} to {summary['end']}")
    print(f"  Dates checked:  {summary['total']}")
    print(f"  Downloaded:     {summary['ok']}{f' ({pdf_recovered} from PDF)' if pdf_recovered else ''}")
    print(f"  Skipped:        {summary['skipped']} (already exist)")
    print(f"  Missing:        {summary['missing']} (holidays/weekends)")
    print(f"  Failed:         {len(summary['failed'])}")

    if summary["failed"]:
        print("\nFailed:")
        for err in summary["failed"][:10]:
            print(f"  {err['date']}: {err['message']}")
        if len(summary["failed"]) > 10:
            print(f"  ... and {len(summary['failed']) - 10} more errors")

    # Import to eod_ohlcv if requested
    if args.import_eod and (summary['ok'] > 0 or summary['skipped'] > 0):
        print(f"\nImporting to eod_ohlcv table...")
        con = connect(args.db)
        init_schema(con)

        # Import market_summary CSVs (primary source)
        ingest_result = ingest_all_market_summary_csvs(
            con, csv_dir=args.out_dir, skip_existing=not args.force
        )
        print(f"  Market Summary:")
        print(f"    Files processed: {ingest_result['total_files']}")
        print(f"    Imported:        {ingest_result['ok']}")
        print(f"    Skipped:         {ingest_result['skipped']} (already in DB)")
        print(f"    Total rows:      {ingest_result['total_rows']}")

        # Import PDF-recovered dates separately (fallback source)
        if pdf_recovered_dates:
            print(f"  PDF Fallback:")
            pdf_rows = 0
            for date_str, csv_path in pdf_recovered_dates:
                result = ingest_market_summary_csv(
                    con, csv_path, skip_existing=not args.force, source="closing_rates_pdf"
                )
                if result['status'] == 'ok':
                    pdf_rows += result['rows_inserted']
                    print(f"    {date_str}: {result['rows_inserted']} rows")
            print(f"    Total PDF rows:  {pdf_rows}")

        con.close()

    return EXIT_SUCCESS


def handle_market_summary_last(args: argparse.Namespace) -> int:
    """Handle market-summary last command."""
    from .db import ingest_all_market_summary_csvs, ingest_market_summary_csv

    try:
        start_date, end_date = resolve_range(days=args.days)
    except ValueError as e:
        print(f"Invalid range: {e}", file=sys.stderr)
        return EXIT_ERROR

    skip_weekends = not args.include_weekends
    mode_str = "excluding weekends" if skip_weekends else "including weekends"

    print(f"Downloading market summaries for last {args.days} days")
    print(f"({start_date} to {end_date}, {mode_str})")
    print("-" * 50)

    summary = fetch_range_summary(
        start=start_date,
        end=end_date,
        out_dir=args.out_dir,
        skip_weekends=skip_weekends,
        force=args.force,
        keep_raw=args.keep_raw,
    )

    # PDF fallback for failed dates (only when .Z file fails)
    pdf_recovered = 0
    pdf_recovered_dates = []  # Track PDF-recovered dates for separate ingestion
    if getattr(args, 'pdf_fallback', False) and summary["failed"]:
        from .sources.closing_rates_pdf import fetch_day as fetch_pdf_day
        print("\nRetrying failed dates with PDF fallback...")
        remaining_failed = []
        for err in summary["failed"]:
            pdf_result = fetch_pdf_day(err['date'], out_dir=args.out_dir, force=args.force)
            if pdf_result["status"] == "ok":
                pdf_recovered += 1
                pdf_recovered_dates.append((err['date'], pdf_result['csv_path']))
                print(f"  {err['date']}: Recovered from PDF ({pdf_result['row_count']} rows)")
            else:
                remaining_failed.append(err)
        summary["failed"] = remaining_failed
        summary["ok"] += pdf_recovered

    print("\nMarket Summary Download")
    print("=" * 50)
    print(f"  Date range:     {summary['start']} to {summary['end']}")
    print(f"  Dates checked:  {summary['total']}")
    print(f"  Downloaded:     {summary['ok']}{f' ({pdf_recovered} from PDF)' if pdf_recovered else ''}")
    print(f"  Skipped:        {summary['skipped']} (already exist)")
    print(f"  Missing:        {summary['missing']} (holidays/weekends)")
    print(f"  Failed:         {len(summary['failed'])}")

    if summary["failed"]:
        print("\nFailed:")
        for err in summary["failed"][:10]:
            print(f"  {err['date']}: {err['message']}")
        if len(summary["failed"]) > 10:
            print(f"  ... and {len(summary['failed']) - 10} more errors")

    # Import to eod_ohlcv if requested
    if args.import_eod and (summary['ok'] > 0 or summary['skipped'] > 0):
        print(f"\nImporting to eod_ohlcv table...")
        con = connect(args.db)
        init_schema(con)

        # Import market_summary CSVs (primary source)
        ingest_result = ingest_all_market_summary_csvs(
            con, csv_dir=args.out_dir, skip_existing=not args.force
        )
        print(f"  Market Summary:")
        print(f"    Files processed: {ingest_result['total_files']}")
        print(f"    Imported:        {ingest_result['ok']}")
        print(f"    Skipped:         {ingest_result['skipped']} (already in DB)")
        print(f"    Total rows:      {ingest_result['total_rows']}")

        # Import PDF-recovered dates separately (fallback source)
        if pdf_recovered_dates:
            print(f"  PDF Fallback:")
            pdf_rows = 0
            for date_str, csv_path in pdf_recovered_dates:
                result = ingest_market_summary_csv(
                    con, csv_path, skip_existing=not args.force, source="closing_rates_pdf"
                )
                if result['status'] == 'ok':
                    pdf_rows += result['rows_inserted']
                    print(f"    {date_str}: {result['rows_inserted']} rows")
            print(f"    Total PDF rows:  {pdf_rows}")

        con.close()

    return EXIT_SUCCESS


def handle_market_summary_retry_failed(args: argparse.Namespace) -> int:
    """Handle market-summary retry-failed command."""
    from .sources.market_summary import get_failed_dates, retry_failed_dates

    setup_logging()

    con = connect(args.db)
    init_schema(con)

    # Get count of failed dates
    failed_dates = get_failed_dates(con)
    if not failed_dates:
        print("No failed dates to retry.")
        con.close()
        return EXIT_SUCCESS

    print(f"Found {len(failed_dates)} failed dates to retry...")
    print("-" * 50)

    summary = retry_failed_dates(
        con=con,
        out_dir=args.out_dir,
        keep_raw=args.keep_raw,
    )
    con.close()

    print("\nRetry Failed Dates Summary")
    print("=" * 50)
    print(f"  Total retried:  {summary['total']}")
    print(f"  Now OK:         {summary['ok']}")
    print(f"  Now missing:    {summary['missing']}")
    print(f"  Still failed:   {summary['still_failed']}")

    if summary["failed"]:
        print("\nRemaining errors:")
        for err in summary["failed"][:10]:
            print(f"  {err['date']}: {err['message']}")
        if len(summary["failed"]) > 10:
            print(f"  ... and {len(summary['failed']) - 10} more")

    return EXIT_SUCCESS


def handle_market_summary_retry_missing(args: argparse.Namespace) -> int:
    """Handle market-summary retry-missing command."""
    from .sources.market_summary import get_missing_dates, retry_missing_dates

    setup_logging()

    con = connect(args.db)
    init_schema(con)

    # Get count of missing dates
    missing_dates = get_missing_dates(con)
    if not missing_dates:
        print("No missing dates to retry.")
        con.close()
        return EXIT_SUCCESS

    print(f"Found {len(missing_dates)} not-found dates to retry...")
    print("-" * 50)

    summary = retry_missing_dates(
        con=con,
        out_dir=args.out_dir,
        keep_raw=args.keep_raw,
    )
    con.close()

    print("\nRetry Missing Dates Summary")
    print("=" * 50)
    print(f"  Total retried:  {summary['total']}")
    print(f"  Now OK:         {summary['ok']}")
    print(f"  Still missing:  {summary['still_missing']}")

    if summary["failed"]:
        print("\nNew errors:")
        for err in summary["failed"][:10]:
            print(f"  {err['date']}: {err['message']}")
        if len(summary["failed"]) > 10:
            print(f"  ... and {len(summary['failed']) - 10} more")

    return EXIT_SUCCESS


def handle_market_summary_status(args: argparse.Namespace) -> int:
    """Handle market-summary status command."""
    from .sources.market_summary import (
        get_download_record,
        init_market_summary_tracking,
    )

    con = connect(args.db)
    init_schema(con)
    init_market_summary_tracking(con)

    record = get_download_record(con, args.date)
    con.close()

    if record is None:
        print(f"No tracking record found for date: {args.date}")
        return EXIT_SUCCESS

    print(f"Market Summary Status for {args.date}")
    print("=" * 50)
    print(f"  Status:         {record['status']}")
    print(f"  CSV Path:       {record['csv_path'] or 'N/A'}")
    print(f"  Raw Path:       {record['raw_path'] or 'N/A'}")
    print(f"  Extracted Path: {record['extracted_path'] or 'N/A'}")
    print(f"  Row Count:      {record['row_count']}")
    print(f"  Message:        {record['message'] or 'N/A'}")
    print(f"  Updated At:     {record['updated_at']}")

    return EXIT_SUCCESS


def handle_market_summary_list_missing(args: argparse.Namespace) -> int:
    """Handle market-summary list-missing command."""
    from .sources.market_summary import (
        get_missing_dates,
        init_market_summary_tracking,
    )

    con = connect(args.db)
    init_schema(con)
    init_market_summary_tracking(con)

    missing_dates = get_missing_dates(con)
    con.close()

    if not missing_dates:
        print("No missing dates found.")
        return EXIT_SUCCESS

    print(f"Missing Dates (status='missing'): {len(missing_dates)}")
    print("=" * 50)
    for d in missing_dates:
        print(f"  {d}")

    return EXIT_SUCCESS


def handle_market_summary_list_failed(args: argparse.Namespace) -> int:
    """Handle market-summary list-failed command."""
    from .sources.market_summary import (
        get_failed_dates,
        init_market_summary_tracking,
    )

    con = connect(args.db)
    init_schema(con)
    init_market_summary_tracking(con)

    failed_dates = get_failed_dates(con)
    con.close()

    if not failed_dates:
        print("No failed dates found.")
        return EXIT_SUCCESS

    print(f"Failed Dates (status='failed'): {len(failed_dates)}")
    print("=" * 50)
    for d in failed_dates:
        print(f"  {d}")

    return EXIT_SUCCESS


def handle_intraday(args: argparse.Namespace) -> int:
    """Handle intraday subcommands."""
    if args.intraday_command == "sync":
        return handle_intraday_sync(args)
    elif args.intraday_command == "show":
        return handle_intraday_show(args)
    elif args.intraday_command == "sync-all":
        return handle_intraday_sync_all(args)
    return EXIT_ERROR


def handle_intraday_sync(args: argparse.Namespace) -> int:
    """Handle intraday sync command."""
    setup_logging()

    symbol = args.symbol.upper().strip()
    incremental = not args.no_incremental
    mode_str = "incremental" if incremental else "full"

    print(f"Syncing intraday data for {symbol} ({mode_str} mode)...")

    summary = sync_intraday(
        db_path=args.db,
        symbol=symbol,
        incremental=incremental,
        max_rows=args.max_rows,
    )

    if summary.error:
        print(f"Error: {summary.error}", file=sys.stderr)
        return EXIT_ERROR

    print(f"\nIntraday Sync: {summary.symbol}")
    print("=" * 50)
    print(f"  Rows upserted:  {summary.rows_upserted}")
    print(f"  Newest ts:      {summary.newest_ts or 'N/A'}")

    return EXIT_SUCCESS


def handle_intraday_sync_all(args: argparse.Namespace) -> int:
    """Handle bulk intraday sync command for all symbols.

    This command is designed to be run via cron for automated data collection.

    Example cron entries:
        # Every 5 minutes during market hours (9:30 AM - 3:30 PM PKT, Mon-Fri)
        */5 9-15 * * 1-5 psxsync intraday sync-all --quiet

        # Full refresh every night at midnight
        0 0 * * * psxsync intraday sync-all --no-incremental --quiet
    """
    setup_logging()

    incremental = not args.no_incremental
    quiet = args.quiet
    mode_str = "incremental" if incremental else "full"

    if not quiet:
        print(f"Starting bulk intraday sync ({mode_str} mode)...")
        if args.limit:
            print(f"  Limiting to {args.limit} symbols")

    def progress_callback(current, total, symbol, result):
        if not quiet:
            status_icon = "✓" if not result.error else "✗"
            print(f"  [{current}/{total}] {status_icon} {symbol}: {result.rows_upserted} rows")

    summary = sync_intraday_bulk(
        db_path=args.db,
        incremental=incremental,
        limit_symbols=args.limit,
        max_rows=args.max_rows,
        progress_callback=progress_callback if not quiet else None,
    )

    # Print summary
    if quiet:
        # Minimal output for cron logs
        if summary.symbols_failed > 0:
            print(
                f"WARN: Intraday sync: {summary.symbols_ok}/{summary.symbols_total} OK, "
                f"{summary.symbols_failed} failed, {summary.rows_upserted} rows"
            )
        else:
            print(
                f"OK: Intraday sync: {summary.symbols_ok} symbols, "
                f"{summary.rows_upserted} rows"
            )
    else:
        print("\nBulk Intraday Sync Complete")
        print("=" * 50)
        print(f"  Mode:           {mode_str}")
        print(f"  Total symbols:  {summary.symbols_total}")
        print(f"  Symbols OK:     {summary.symbols_ok}")
        print(f"  Symbols failed: {summary.symbols_failed}")
        print(f"  Rows upserted:  {summary.rows_upserted:,}")

        if summary.symbols_failed > 0:
            print("\nFailed symbols:")
            for result in summary.results:
                if result.error:
                    print(f"  - {result.symbol}: {result.error}")

    return EXIT_SUCCESS if summary.symbols_failed == 0 else EXIT_ERROR


def handle_intraday_show(args: argparse.Namespace) -> int:
    """Handle intraday show command."""
    con = connect(args.db)
    init_schema(con)

    symbol = args.symbol.upper().strip()

    # Get stats first
    stats = get_intraday_stats(con, symbol)

    if stats["row_count"] == 0:
        print(f"No intraday data found for symbol: {symbol}", file=sys.stderr)
        print(
            f"Run 'psxsync intraday sync --symbol {symbol}' to fetch data.",
            file=sys.stderr
        )
        con.close()
        return EXIT_ERROR

    # Get data
    df = get_intraday_latest(con, symbol, limit=args.limit)
    con.close()

    # Print stats header
    print(f"Intraday Data: {symbol}")
    print("=" * 50)
    print(f"  Total rows in DB: {stats['row_count']}")
    print(f"  Date range:       {stats['min_ts']} to {stats['max_ts']}")
    print(f"  Showing:          {len(df)} rows")
    print()

    if args.out == "csv":
        print(df.to_csv(index=False))
    else:
        # Table format
        print(
            f"{'TIMESTAMP':<20} {'OPEN':>10} {'HIGH':>10} "
            f"{'LOW':>10} {'CLOSE':>10} {'VOLUME':>12}"
        )
        print("-" * 78)
        for _, row in df.iterrows():
            ts = row["ts"][:19] if row["ts"] else "N/A"  # Truncate ts for display
            open_val = f"{row['open']:.2f}" if row["open"] else "N/A"
            high_val = f"{row['high']:.2f}" if row["high"] else "N/A"
            low_val = f"{row['low']:.2f}" if row["low"] else "N/A"
            close_val = f"{row['close']:.2f}" if row["close"] else "N/A"
            vol_val = f"{int(row['volume']):,}" if row["volume"] else "N/A"
            print(
                f"{ts:<20} {open_val:>10} {high_val:>10} "
                f"{low_val:>10} {close_val:>10} {vol_val:>12}"
            )

    return EXIT_SUCCESS


def handle_regular_market(args: argparse.Namespace) -> int:
    """Handle regular-market subcommands."""
    if args.rm_command == "snapshot":
        return handle_regular_market_snapshot(args)
    elif args.rm_command == "listen":
        return handle_regular_market_listen(args)
    elif args.rm_command == "show":
        return handle_regular_market_show(args)
    return EXIT_ERROR


def handle_regular_market_snapshot(args: argparse.Namespace) -> int:
    """Handle regular-market snapshot command."""
    import requests

    print("Fetching regular market data...")

    try:
        df = fetch_regular_market()
    except requests.RequestException as e:
        print(f"Failed to fetch data: {e}", file=sys.stderr)
        return EXIT_ERROR

    if df.empty:
        print("No data found in REGULAR MARKET table.", file=sys.stderr)
        return EXIT_ERROR

    # Save to database
    con = connect(args.db)
    init_schema(con)
    init_regular_market_schema(con)
    init_analytics_schema(con)

    # CRITICAL: Load previous hashes BEFORE upsert to detect changes correctly
    prev_hashes = get_all_current_hashes(con)

    # Insert snapshots first (using pre-loaded hashes for comparison)
    inserted = insert_snapshots(
        con, df, save_unchanged=args.save_unchanged, prev_hashes=prev_hashes
    )

    # Then upsert current data
    upserted = upsert_current(con, df)

    # Compute analytics for this timestamp
    ts = df["ts"].iloc[0] if not df.empty else None
    analytics_result = None
    if ts:
        analytics_result = compute_all_analytics(con, ts)

    # Save CSV with joined sector/company names
    csv_path = args.csv
    if csv_path is None:
        csv_path = DATA_ROOT / "regular_market" / "current.csv"

    csv_path.parent.mkdir(parents=True, exist_ok=True)

    # Get data with sector names joined from symbols table
    df_with_sectors = get_current_market_with_sectors(con)
    if not df_with_sectors.empty:
        df_with_sectors.to_csv(csv_path, index=False)
    else:
        df.to_csv(csv_path, index=False)

    con.close()

    # Print summary
    print("\nRegular Market Snapshot")
    print("=" * 50)
    print(f"  Timestamp:         {ts}")
    print(f"  Symbols found:     {len(df)}")
    print(f"  Rows upserted:     {upserted}")
    print(f"  Snapshots saved:   {inserted}")
    print(f"  CSV saved to:      {csv_path}")

    # Print analytics summary
    if analytics_result:
        ma = analytics_result["market_analytics"]
        print("\nMarket Analytics")
        print("-" * 50)
        print(f"  Gainers:           {ma['gainers_count']}")
        print(f"  Losers:            {ma['losers_count']}")
        print(f"  Unchanged:         {ma['unchanged_count']}")
        print(f"  Total Volume:      {ma['total_volume']:,.0f}")
        if ma["top_gainer_symbol"]:
            print(f"  Top Gainer:        {ma['top_gainer_symbol']}")
        if ma["top_loser_symbol"]:
            print(f"  Top Loser:         {ma['top_loser_symbol']}")
        print(f"  Sector rollups:    {analytics_result['sectors_count']}")

    return EXIT_SUCCESS


def handle_regular_market_listen(args: argparse.Namespace) -> int:
    """Handle regular-market listen command (continuous polling)."""
    import time

    import requests

    csv_dir = args.csv_dir
    if csv_dir is None:
        csv_dir = DATA_ROOT / "regular_market"

    csv_dir.mkdir(parents=True, exist_ok=True)

    con = connect(args.db)
    init_schema(con)
    init_regular_market_schema(con)
    init_analytics_schema(con)

    print(f"Listening for regular market updates (interval: {args.interval}s)")
    print(f"CSV directory: {csv_dir}")
    print("Press Ctrl+C to stop.\n")

    iteration = 0
    try:
        while True:
            iteration += 1
            try:
                df = fetch_regular_market()

                if df.empty:
                    print(f"[{iteration}] No data returned")
                else:
                    # CRITICAL: Load previous hashes BEFORE upsert
                    prev_hashes = get_all_current_hashes(con)

                    # Insert snapshots first (using pre-loaded hashes)
                    inserted = insert_snapshots(
                        con, df,
                        save_unchanged=args.save_unchanged,
                        prev_hashes=prev_hashes,
                    )

                    # Then upsert current data
                    upserted = upsert_current(con, df)

                    # Compute analytics for this timestamp
                    ts_full = df["ts"].iloc[0] if not df.empty else None
                    analytics_result = None
                    if ts_full:
                        analytics_result = compute_all_analytics(con, ts_full)

                    # Save current.csv with sector names
                    current_csv = csv_dir / "current.csv"
                    df_with_sectors = get_current_market_with_sectors(con)
                    if not df_with_sectors.empty:
                        df_with_sectors.to_csv(current_csv, index=False)
                    else:
                        df.to_csv(current_csv, index=False)

                    # Save timestamped snapshot if any changes
                    if inserted > 0:
                        ts_safe = ts_full.replace(":", "-").replace(" ", "_")[:19]
                        snapshot_csv = csv_dir / f"snapshot_{ts_safe}.csv"
                        df.to_csv(snapshot_csv, index=False)

                    ts_display = ts_full[:19] if ts_full else "N/A"

                    # Build summary line with analytics
                    summary = (
                        f"[{iteration}] {ts_display} | "
                        f"symbols={len(df)} upserted={upserted} changes={inserted}"
                    )
                    if analytics_result:
                        ma = analytics_result["market_analytics"]
                        summary += (
                            f" | G:{ma['gainers_count']} L:{ma['losers_count']} "
                            f"U:{ma['unchanged_count']}"
                        )
                    print(summary)

            except requests.RequestException as e:
                print(f"[{iteration}] Fetch error: {e}", file=sys.stderr)
            except Exception as e:
                print(f"[{iteration}] Error: {e}", file=sys.stderr)

            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\nStopped by user.")
        con.close()
        return EXIT_SUCCESS

    con.close()
    return EXIT_SUCCESS


def handle_regular_market_show(args: argparse.Namespace) -> int:
    """Handle regular-market show command."""
    con = connect(args.db)
    init_schema(con)
    init_regular_market_schema(con)
    init_analytics_schema(con)

    # Get data with sector names joined
    df = get_current_market_with_sectors(con)

    if df.empty:
        # Fallback to raw data
        df = get_current_market(con)

    con.close()

    if df.empty:
        print("No regular market data found.", file=sys.stderr)
        print(
            "Run 'psxsync regular-market snapshot' to fetch data.",
            file=sys.stderr
        )
        return EXIT_ERROR

    if args.out == "csv":
        print(df.to_csv(index=False))
    else:
        # Table format with sector
        print(
            f"{'SYMBOL':<10} {'SECTOR':<25} {'CURRENT':>10} {'CHG%':>8} "
            f"{'VOLUME':>12}"
        )
        print("-" * 70)
        for _, row in df.iterrows():
            current = f"{row['current']:.2f}" if row.get("current") else "N/A"
            change_pct = f"{row['change_pct']:.2f}" if row.get("change_pct") else "N/A"
            volume = f"{int(row['volume']):,}" if row.get("volume") else "N/A"
            sector = (row.get("sector_name") or row.get("sector_code") or "")[:24]
            print(
                f"{row['symbol']:<10} {sector:<25} {current:>10} {change_pct:>8} "
                f"{volume:>12}"
            )

        ts = df["ts"].iloc[0] if "ts" in df.columns else "N/A"
        print(f"\nTotal: {len(df)} symbols | Last update: {ts}")

    return EXIT_SUCCESS


def handle_sectors(args: argparse.Namespace) -> int:
    """Handle sectors subcommands."""
    if args.sectors_command == "refresh":
        return handle_sectors_refresh(args)
    elif args.sectors_command == "list":
        return handle_sectors_list(args)
    elif args.sectors_command == "export":
        return handle_sectors_export(args)
    return EXIT_ERROR


def handle_sectors_refresh(args: argparse.Namespace) -> int:
    """Handle sectors refresh command."""
    print("Refreshing sectors from PSX sector-summary page...")

    con = connect(args.db)
    init_schema(con)

    result = refresh_sectors(con)
    con.close()

    if result["success"]:
        print("\nSectors Refresh")
        print("=" * 50)
        print(f"  Fetched at:       {result['fetched_at']}")
        print(f"  Sectors found:    {result['sectors_found']}")
        print(f"  Sectors upserted: {result['sectors_upserted']}")
        return EXIT_SUCCESS
    else:
        print(f"Error: {result['error']}", file=sys.stderr)
        return EXIT_ERROR


def handle_sectors_list(args: argparse.Namespace) -> int:
    """Handle sectors list command."""
    con = connect(args.db)
    init_schema(con)

    sectors = get_sector_list(con)
    con.close()

    if not sectors:
        print("No sectors found.", file=sys.stderr)
        print(
            "Run 'psxsync sectors refresh' to fetch sectors.",
            file=sys.stderr
        )
        return EXIT_ERROR

    if args.out == "csv":
        print("sector_code,sector_name")
        for s in sectors:
            print(f"{s['sector_code']},{s['sector_name']}")
    else:
        # Table format
        print(f"{'CODE':<12} {'SECTOR NAME':<50}")
        print("-" * 62)
        for s in sectors:
            print(f"{s['sector_code']:<12} {s['sector_name']:<50}")
        print(f"\nTotal: {len(sectors)} sectors")

    return EXIT_SUCCESS


def handle_sectors_export(args: argparse.Namespace) -> int:
    """Handle sectors export command."""
    con = connect(args.db)
    init_schema(con)

    # Create parent directory if needed
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    count = export_sectors_csv(con, str(out_path))
    con.close()

    if count == 0:
        print("No sectors to export.", file=sys.stderr)
        print(
            "Run 'psxsync sectors refresh' to fetch sectors first.",
            file=sys.stderr
        )
        return EXIT_ERROR

    print(f"Exported {count} sectors to {out_path}")
    return EXIT_SUCCESS


def handle_master(args: argparse.Namespace) -> int:
    """Handle master subcommands."""
    if args.master_command == "refresh":
        return handle_master_refresh(args)
    elif args.master_command == "list":
        return handle_master_list(args)
    elif args.master_command == "export":
        return handle_master_export(args)
    return EXIT_ERROR


def handle_master_refresh(args: argparse.Namespace) -> int:
    """Handle master refresh command."""
    print("Refreshing symbols from official listed companies file...")
    print("Source: https://dps.psx.com.pk/download/text/listed_cmp.lst.Z")

    con = connect(args.db)
    init_schema(con)

    result = refresh_listed_companies(
        con,
        deactivate_missing=args.deactivate_missing,
    )
    con.close()

    if result["success"]:
        print("\nMaster Refresh")
        print("=" * 50)
        print(f"  Fetched at:    {result['fetched_at']}")
        print(f"  Symbols found: {result['symbols_found']}")
        print(f"  Inserted:      {result['inserted']}")
        print(f"  Updated:       {result['updated']}")
        if args.deactivate_missing:
            print(f"  Deactivated:   {result['deactivated']}")
        if result.get("sectors_upserted"):
            print(f"  Sectors:       {result['sectors_upserted']}")
        return EXIT_SUCCESS
    else:
        print(f"Error: {result['error']}", file=sys.stderr)
        return EXIT_ERROR


def handle_master_list(args: argparse.Namespace) -> int:
    """Handle master list command."""
    con = connect(args.db)
    init_schema(con)

    df = get_master_symbols(con)
    con.close()

    if df.empty:
        print("No symbols found.", file=sys.stderr)
        print(
            "Run 'psxsync master refresh' to fetch symbols.",
            file=sys.stderr
        )
        return EXIT_ERROR

    # Filter active only if requested
    if args.active_only:
        df = df[df["is_active"] == 1]

    if args.out == "csv":
        print(df.to_csv(index=False))
    else:
        # Table format - show sector_name as primary sector display
        print(
            f"{'SYMBOL':<12} {'NAME':<35} {'SECTOR':<35} {'ACTIVE'}"
        )
        print("-" * 90)
        for _, row in df.iterrows():
            name = (row["name"] or "")[:35]
            # Use sector_name, fall back to sector code if empty
            sector_display = (row["sector_name"] or row["sector"] or "")[:35]
            active = "Yes" if row["is_active"] else "No"
            print(
                f"{row['symbol']:<12} {name:<35} {sector_display:<35} {active}"
            )
        print(f"\nTotal: {len(df)} symbols")

    return EXIT_SUCCESS


def handle_master_export(args: argparse.Namespace) -> int:
    """Handle master export command."""
    con = connect(args.db)
    init_schema(con)

    # Create parent directory if needed
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    count = export_master_csv(con, str(out_path))
    con.close()

    if count == 0:
        print("No symbols to export.", file=sys.stderr)
        print(
            "Run 'psxsync master refresh' to fetch symbols first.",
            file=sys.stderr
        )
        return EXIT_ERROR

    print(f"Exported {count} symbols to {out_path}")
    return EXIT_SUCCESS


def handle_company(args: argparse.Namespace) -> int:
    """Handle company subcommands."""
    if args.company_command == "refresh":
        return handle_company_refresh(args)
    elif args.company_command == "snapshot":
        return handle_company_snapshot(args)
    elif args.company_command == "listen":
        return handle_company_listen(args)
    elif args.company_command == "show":
        return handle_company_show(args)
    elif args.company_command == "sync-sectors":
        return handle_company_sync_sectors(args)
    elif args.company_command == "deep-scrape":
        return handle_company_deep_scrape(args)
    elif args.company_command == "import-payouts":
        return handle_company_import_payouts(args)
    elif args.company_command == "fetch-dividends":
        return handle_company_fetch_dividends(args)
    return EXIT_ERROR


def handle_company_refresh(args: argparse.Namespace) -> int:
    """Handle company refresh command."""
    symbol = args.symbol.upper()
    print(f"Refreshing company profile for {symbol}...")
    print(f"Source: https://dps.psx.com.pk/company/{symbol}")

    con = connect(args.db)
    init_schema(con)

    result = refresh_company_profile(con, symbol)
    con.close()

    if result["success"]:
        print("\nCompany Profile Refresh")
        print("=" * 50)
        print(f"  Symbol:          {result['symbol']}")
        print(f"  Fetched at:      {result['fetched_at']}")
        print(f"  Profile updated: {result['profile_updated']}")
        print(f"  Key people:      {result['key_people_count']}")
        return EXIT_SUCCESS
    else:
        print(f"Error: {result['error']}", file=sys.stderr)
        return EXIT_ERROR


def handle_company_snapshot(args: argparse.Namespace) -> int:
    """Handle company snapshot command."""
    # Get symbols from --symbol or --symbols
    symbols = []
    if args.symbol:
        symbols = [args.symbol.upper()]
    elif args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",")]

    if not symbols:
        print("Error: --symbol or --symbols required", file=sys.stderr)
        return EXIT_ERROR

    con = connect(args.db)
    init_schema(con)

    print(f"Taking quote snapshot for {len(symbols)} symbol(s)...")
    print("-" * 50)

    success_count = 0
    for symbol in symbols:
        result = take_quote_snapshot(con, symbol)

        if result["success"]:
            q = result["quote"]
            name = q.get("company_name", "")
            sector = q.get("sector_name", "")
            price = q.get("price", "N/A")
            change = q.get("change", "N/A")
            change_pct = q.get("change_pct", "N/A")
            volume = q.get("volume", "N/A")

            # Show name and sector on first line, price on second
            if name:
                print(f"{symbol}: {name}")
            if sector:
                print(f"  Sector: {sector}")
            print(f"  Price: {price} ({change}, {change_pct}%) vol={volume}")
            success_count += 1
        else:
            print(f"{symbol}: Error - {result['error']}", file=sys.stderr)

    con.close()

    print("-" * 50)
    print(f"Snapshots taken: {success_count}/{len(symbols)}")

    return EXIT_SUCCESS if success_count > 0 else EXIT_ERROR


def handle_company_listen(args: argparse.Namespace) -> int:
    """Handle company listen command."""
    # Get symbols from --symbol or --symbols
    symbols = []
    if args.symbol:
        symbols = [args.symbol.upper()]
    elif args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",")]

    if not symbols:
        print("Error: --symbol or --symbols required", file=sys.stderr)
        return EXIT_ERROR

    interval = args.interval

    con = connect(args.db)
    init_schema(con)

    try:
        listen_quotes(con, symbols, interval=interval)
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        con.close()

    return EXIT_SUCCESS


def handle_company_show(args: argparse.Namespace) -> int:
    """Handle company show command."""
    from .db import get_company_key_people, get_company_profile, get_quote_snapshots

    symbol = args.symbol.upper()
    what = args.what

    con = connect(args.db)
    init_schema(con)

    if what in ("profile", "all"):
        profile = get_company_profile(con, symbol)
        if profile:
            print("\nCompany Profile")
            print("=" * 50)
            print(f"  Symbol:      {profile['symbol']}")
            print(f"  Name:        {profile['company_name'] or 'N/A'}")
            print(f"  Sector:      {profile['sector_name'] or 'N/A'}")
            print(f"  Address:     {profile['address'] or 'N/A'}")
            print(f"  Website:     {profile['website'] or 'N/A'}")
            print(f"  Registrar:   {profile['registrar'] or 'N/A'}")
            print(f"  Auditor:     {profile['auditor'] or 'N/A'}")
            print(f"  Fiscal YE:   {profile['fiscal_year_end'] or 'N/A'}")
            print(f"  Updated:     {profile['updated_at']}")
            if profile.get("business_description"):
                desc = profile["business_description"][:200]
                print(f"  Description: {desc}...")
        else:
            print(f"No profile found for {symbol}")

    if what in ("people", "all"):
        people = get_company_key_people(con, symbol)
        if people:
            print("\nKey People")
            print("=" * 50)
            for p in people:
                print(f"  {p['role']:<25} {p['name']}")
        elif what == "people":
            print(f"No key people found for {symbol}")

    if what in ("quotes", "all"):
        df = get_quote_snapshots(con, symbol, limit=10)
        if not df.empty:
            print("\nRecent Quote Snapshots")
            print("=" * 50)
            for _, row in df.iterrows():
                print(
                    f"  {row['ts'][:19]} | "
                    f"{row['price'] or 'N/A':>8} | "
                    f"{row['change_pct'] or 'N/A':>6}% | "
                    f"vol={row['volume'] or 'N/A'}"
                )
        elif what == "quotes":
            print(f"No quote snapshots found for {symbol}")

    con.close()
    return EXIT_SUCCESS


def handle_company_sync_sectors(args: argparse.Namespace) -> int:
    """Handle company sync-sectors command."""
    from .db import sync_sector_names_from_company_profile

    con = connect(args.db)
    init_schema(con)

    print("Syncing sector names from company_profile to symbols table...")
    count = sync_sector_names_from_company_profile(con)
    con.close()

    print(f"Updated {count} symbol(s) with sector names.")
    return EXIT_SUCCESS


def handle_company_deep_scrape(args: argparse.Namespace) -> int:
    """Handle company deep-scrape command.

    Deep scrapes company pages including:
    - Trading data (REG, FUT, CSF, ODL)
    - Company profile and key people
    - Financial statements
    - Financial ratios
    - Corporate announcements
    - Dividend/payout history
    """
    from .sources.deep_scraper import deep_scrape_batch, deep_scrape_symbol

    # Determine symbols to scrape
    symbols = []
    if args.symbol:
        symbols = [args.symbol.upper()]
    elif args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",")]
    elif getattr(args, "all", False):
        # Get all active symbols
        from .db import get_symbols_list
        con = connect(args.db)
        init_schema(con)
        symbols = get_symbols_list(con)
        con.close()

    if not symbols:
        print("Error: --symbol, --symbols, or --all required", file=sys.stderr)
        return EXIT_ERROR

    print(f"Deep scraping {len(symbols)} symbol(s)...")
    print("Data: trading, profile, financials, ratios, announcements, payouts")
    print("-" * 60)

    con = connect(args.db)
    init_schema(con)

    save_html = getattr(args, "save_html", False)

    if len(symbols) == 1:
        # Single symbol - direct scrape
        result = deep_scrape_symbol(con, symbols[0], save_raw_html=save_html)
        con.close()

        if result.get("success"):
            print(f"\n{symbols[0]} - Deep scrape complete:")
            print("  Snapshot:     saved")
            print(f"  Trading:      {result.get('trading_sessions_saved', 0)} market type(s)")
            print(f"  Announcements: {result.get('announcements_saved', 0)}")
            print(f"  Equity:       {'saved' if result.get('equity_saved') else 'n/a'}")
            print(f"  Payouts:      {result.get('payouts_saved', 0)}")
            return EXIT_SUCCESS
        else:
            print(f"Error: {result.get('error')}", file=sys.stderr)
            return EXIT_ERROR
    else:
        # Batch scrape with progress
        def progress_cb(current, total, symbol, result):
            status = "OK" if result.get("success") else f"ERR: {result.get('error', 'unknown')}"
            payouts = result.get("payouts_saved", 0) if result.get("success") else 0
            print(f"[{current}/{total}] {symbol}: {status} (payouts: {payouts})")

        summary = deep_scrape_batch(
            con, symbols,
            delay=args.delay,
            save_raw_html=save_html,
            progress_callback=progress_cb,
        )
        con.close()

        print("-" * 60)
        print("Deep scrape complete:")
        print(f"  Total:     {summary['total']}")
        print(f"  Completed: {summary['completed']}")
        print(f"  Failed:    {summary['failed']}")

        if summary["errors"]:
            print("\nErrors:")
            for err in summary["errors"][:10]:
                print(f"  {err['symbol']}: {err['error']}")
            if len(summary["errors"]) > 10:
                print(f"  ... and {len(summary['errors']) - 10} more")

        return EXIT_SUCCESS if summary["completed"] > 0 else EXIT_ERROR


def handle_company_import_payouts(args: argparse.Namespace) -> int:
    """Handle company import-payouts command.

    Imports payout data from a saved HTML file (page source from browser).
    Use this when the PSX website loads payout data via JavaScript.

    Usage:
        1. Open https://dps.psx.com.pk/company/{SYMBOL} in browser
        2. Wait for page to fully load (payouts section visible)
        3. Right-click > View Page Source (or Ctrl+U)
        4. Save the page source as an HTML file
        5. Run: psxsync company import-payouts --symbol SYMBOL --file path/to/saved.html
    """
    from pathlib import Path

    from lxml import html as lxml_html

    from .db import upsert_company_payouts
    from .sources.deep_scraper import parse_payouts_data

    symbol = args.symbol.upper()
    file_path = Path(args.file)

    if not file_path.exists():
        print(f"Error: File not found: {file_path}", file=sys.stderr)
        return EXIT_ERROR

    print(f"Importing payouts for {symbol} from {file_path}...")

    # Read and parse HTML file
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            html_content = f.read()

        tree = lxml_html.fromstring(html_content)
        payouts = parse_payouts_data(tree)

        if not payouts:
            print("No payouts found in the HTML file.")
            print("Make sure the payouts section is visible in the page source.")
            return EXIT_ERROR

        print(f"Found {len(payouts)} payout records.")

        # Save to database
        con = connect(args.db)
        init_schema(con)

        count = upsert_company_payouts(con, symbol, payouts)
        con.close()

        print(f"Imported {count} payout records for {symbol}.")

        # Show imported data
        print("\nImported payouts:")
        print("-" * 60)
        for p in payouts[:10]:
            ann_date = p.get("announcement_date", "N/A")
            fiscal = p.get("fiscal_year", "N/A")
            amount = p.get("amount", "N/A")
            ptype = p.get("payout_type", "cash")
            print(f"  {ann_date}: {amount}% {ptype} (Fiscal: {fiscal})")

        if len(payouts) > 10:
            print(f"  ... and {len(payouts) - 10} more")

        return EXIT_SUCCESS

    except Exception as e:
        print(f"Error parsing HTML: {e}", file=sys.stderr)
        return EXIT_ERROR


def handle_company_fetch_dividends(args: argparse.Namespace) -> int:
    """Handle company fetch-dividends command.

    Fetches dividend announcements from PSX financial announcements page
    (https://www.psx.com.pk/psx/announcement/financial-announcements)
    and saves them to the company_payouts table.
    """
    from .sources.deep_scraper import scrape_psx_financial_announcements

    print("Fetching dividend announcements from PSX...")
    print("Source: https://www.psx.com.pk/psx/announcement/financial-announcements")
    print("-" * 60)

    con = connect(args.db)
    init_schema(con)

    result = scrape_psx_financial_announcements(con)
    con.close()

    if result["success"]:
        print("\nDividend Fetch Complete:")
        print(f"  Announcements found: {result['total_announcements']}")
        print(f"  Payouts saved:       {result['payouts_saved']}")

        if result["companies_without_symbol"]:
            print(f"\nCompanies without matching symbol ({len(result['companies_without_symbol'])}):")
            for company in result["companies_without_symbol"][:10]:
                print(f"  - {company}")
            if len(result["companies_without_symbol"]) > 10:
                print(f"  ... and {len(result['companies_without_symbol']) - 10} more")

        return EXIT_SUCCESS
    else:
        print(f"Error: {result.get('error')}", file=sys.stderr)
        return EXIT_ERROR


def handle_announcements(args: argparse.Namespace) -> int:
    """Handle announcements subcommands."""
    if args.ann_command == "sync":
        return handle_announcements_sync(args)
    elif args.ann_command == "service":
        return handle_announcements_service(args)
    elif args.ann_command == "status":
        return handle_announcements_status(args)
    return EXIT_ERROR


def handle_announcements_sync(args: argparse.Namespace) -> int:
    """Handle one-time announcements sync."""
    from datetime import datetime, timedelta

    print("Syncing announcements data...")
    print("-" * 50)

    con = connect(args.db)
    init_schema(con)

    stats = {
        "announcements": 0,
        "events": 0,
        "dividends": 0,
        "symbols_processed": 0,
    }

    try:
        # Sync company announcements
        if not args.no_announcements:
            print("Fetching company announcements...")
            offset = 0
            page_size = 20
            while True:
                records, total = fetch_announcements(
                    announcement_type="C",
                    offset=offset,
                )
                if not records:
                    break
                for record in records:
                    if save_announcement(con, record):
                        stats["announcements"] += 1
                offset += len(records)
                print(f"  Progress: {offset}/{total}", end="\r")
                if offset >= total or len(records) < page_size:
                    break
            print(f"  Company announcements: {stats['announcements']} saved")

        # Sync corporate events
        if not args.no_events:
            print("Fetching corporate events calendar...")
            from_date = datetime.now().strftime("%Y-%m-%d")
            to_date = (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d")
            events = fetch_corporate_events(from_date, to_date)
            for event in events:
                if save_corporate_event(con, event):
                    stats["events"] += 1
            print(f"  Corporate events: {stats['events']} saved")

        # Sync dividend payouts
        if not args.no_dividends:
            print("Fetching dividend payouts...")
            cur = con.execute("""
                SELECT symbol FROM symbols WHERE is_active = 1 ORDER BY symbol
            """)
            symbols = [row[0] for row in cur.fetchall()]
            for i, symbol in enumerate(symbols):
                print(f"  Processing {symbol} ({i+1}/{len(symbols)})", end="\r")
                try:
                    payouts = fetch_company_payouts(symbol)
                    for payout in payouts:
                        if save_dividend_payout(con, payout):
                            stats["dividends"] += 1
                    stats["symbols_processed"] += 1
                except Exception:
                    pass  # Skip failed symbols silently
            print(f"  Dividend payouts: {stats['dividends']} saved from {stats['symbols_processed']} symbols")

    finally:
        con.close()

    print("\nSync Complete:")
    print(f"  Announcements: {stats['announcements']}")
    print(f"  Events:        {stats['events']}")
    print(f"  Dividends:     {stats['dividends']}")

    return EXIT_SUCCESS


def handle_announcements_service(args: argparse.Namespace) -> int:
    """Handle announcements background service commands."""
    if args.action == "start":
        success, msg = start_announcements_service(
            interval_seconds=args.interval,
        )
        print(msg)
        return EXIT_SUCCESS if success else EXIT_ERROR

    elif args.action == "stop":
        success, msg = stop_announcements_service()
        print(msg)
        return EXIT_SUCCESS if success else EXIT_ERROR

    elif args.action == "status":
        return handle_announcements_status(args)

    return EXIT_ERROR


def handle_announcements_status(args: argparse.Namespace) -> int:
    """Show announcements sync status."""
    status = read_announcements_status()

    print("Announcements Sync Status")
    print("=" * 50)
    print(f"  Running:           {'Yes' if status.running else 'No'}")
    if status.pid:
        print(f"  PID:               {status.pid}")
    if status.started_at:
        print(f"  Started at:        {status.started_at}")
    if status.last_run_at:
        print(f"  Last run at:       {status.last_run_at}")
    if status.last_run_result:
        print(f"  Last result:       {status.last_run_result}")
    if status.current_task:
        print(f"  Current task:      {status.current_task}")
    if status.current_symbol:
        print(f"  Current symbol:    {status.current_symbol}")
    if status.progress > 0:
        print(f"  Progress:          {status.progress:.1f}%")
    print(f"  Total runs:        {status.total_runs}")
    if status.next_run_at:
        print(f"  Next run at:       {status.next_run_at}")
    if status.error_message:
        print(f"  Error:             {status.error_message}")

    print("\nStats:")
    print(f"  Announcements:     {status.announcements_synced}")
    print(f"  Events:            {status.events_synced}")
    print(f"  Dividends:         {status.dividends_synced}")

    return EXIT_SUCCESS


# =============================================================================
# Phase 1: Universe command handlers
# =============================================================================

def handle_universe(args: argparse.Namespace) -> int:
    """Handle universe subcommands."""
    if args.universe_command == "seed-phase1":
        return handle_universe_seed(args)
    elif args.universe_command == "list":
        return handle_universe_list(args)
    elif args.universe_command == "add":
        return handle_universe_add(args)
    return EXIT_ERROR


def handle_universe_seed(args: argparse.Namespace) -> int:
    """Handle universe seed-phase1 command."""
    print("Seeding instrument universe (Phase 1)...")
    print("-" * 50)

    con = connect(args.db)
    init_schema(con)

    config_path = args.config
    include_equities = args.include_equities

    result = seed_universe(
        con,
        config_path=config_path,
        include_equities=include_equities,
    )
    con.close()

    # Get counts from result
    totals = result.get("totals", {})
    indexes = result.get("indexes", {})
    etfs = result.get("etfs", {})
    reits = result.get("reits", {})
    equities = result.get("equities", {})

    total_count = totals.get("inserted", 0) + totals.get("updated", 0)

    print("\nUniverse Seed Complete")
    print("=" * 50)
    idx_count = indexes.get("inserted", 0) + indexes.get("updated", 0)
    etf_count = etfs.get("inserted", 0) + etfs.get("updated", 0)
    reit_count = reits.get("inserted", 0) + reits.get("updated", 0)
    print(f"  Indexes seeded:  {idx_count}")
    print(f"  ETFs seeded:     {etf_count}")
    print(f"  REITs seeded:    {reit_count}")
    if include_equities:
        equity_count = equities.get("inserted", 0) + equities.get("updated", 0)
        print(f"  Equities seeded: {equity_count}")
    print(f"  Total:           {total_count}")
    return EXIT_SUCCESS


def handle_universe_list(args: argparse.Namespace) -> int:
    """Handle universe list command."""
    from .db import get_instruments

    con = connect(args.db)
    init_schema(con)

    # Get instruments
    inst_type = None if args.type == "ALL" else args.type
    active_only = args.active_only

    instruments = get_instruments(con, instrument_type=inst_type, active_only=active_only)
    con.close()

    if not instruments:
        print("No instruments found.", file=sys.stderr)
        print("Run 'psxsync universe seed-phase1' to seed instruments.", file=sys.stderr)
        return EXIT_ERROR

    if args.out == "csv":
        print("instrument_id,symbol,name,type,source,is_active")
        for inst in instruments:
            print(
                f"{inst['instrument_id']},"
                f"{inst['symbol']},"
                f"\"{inst['name'] or ''}\","
                f"{inst['instrument_type']},"
                f"{inst['source']},"
                f"{inst['is_active']}"
            )
    else:
        # Table format
        print(f"{'SYMBOL':<12} {'NAME':<35} {'TYPE':<8} {'SOURCE':<8} {'ACTIVE'}")
        print("-" * 75)
        for inst in instruments:
            name = (inst["name"] or "")[:35]
            active = "Yes" if inst["is_active"] else "No"
            print(
                f"{inst['symbol']:<12} {name:<35} "
                f"{inst['instrument_type']:<8} {inst['source']:<8} {active}"
            )
        print(f"\nTotal: {len(instruments)} instruments")

    return EXIT_SUCCESS


def handle_universe_add(args: argparse.Namespace) -> int:
    """Handle universe add command."""
    from .db import upsert_instrument

    symbol = args.symbol.upper()
    inst_type = args.type
    name = args.name
    source = args.source

    print(f"Adding instrument: {symbol} ({inst_type})")

    con = connect(args.db)
    init_schema(con)

    # Create instrument ID based on type
    if inst_type == "INDEX":
        instrument_id = f"IDX:{symbol}"
    else:
        instrument_id = f"PSX:{symbol}"

    instrument = {
        "instrument_id": instrument_id,
        "symbol": symbol,
        "name": name,
        "instrument_type": inst_type,
        "exchange": "PSX",
        "currency": "PKR",
        "is_active": 1,
        "source": source,
    }

    try:
        upsert_instrument(con, instrument)
        con.close()

        print("\nInstrument Added")
        print("=" * 50)
        print(f"  ID:     {instrument_id}")
        print(f"  Symbol: {symbol}")
        print(f"  Name:   {name}")
        print(f"  Type:   {inst_type}")
        print(f"  Source: {source}")
        return EXIT_SUCCESS
    except Exception as e:
        con.close()
        print(f"Error: {e}", file=sys.stderr)
        return EXIT_ERROR


# =============================================================================
# Phase 1: Instruments command handlers
# =============================================================================

def handle_instruments(args: argparse.Namespace) -> int:
    """Handle instruments subcommands."""
    if args.instruments_command == "sync-eod":
        return handle_instruments_sync_eod(args)
    elif args.instruments_command == "rankings":
        return handle_instruments_rankings(args)
    elif args.instruments_command == "sync-status":
        return handle_instruments_sync_status(args)
    return EXIT_ERROR


def handle_instruments_sync_eod(args: argparse.Namespace) -> int:
    """Handle instruments sync-eod command."""
    setup_logging()

    # Single symbol mode
    if args.symbol:
        symbol = args.symbol.upper()
        incremental = not args.full
        mode_str = "incremental" if incremental else "full"

        print(f"Syncing EOD data for {symbol} ({mode_str} mode)...")

        rows, error = sync_single_instrument(
            symbol=symbol,
            db_path=args.db,
            incremental=incremental,
        )

        if error:
            print(f"Error: {error}", file=sys.stderr)
            return EXIT_ERROR

        print(f"\nSync Complete: {symbol}")
        print(f"  Rows upserted: {rows}")
        return EXIT_SUCCESS

    # Multi-instrument mode
    types_list = [t.strip().upper() for t in args.types.split(",")]
    incremental = not args.full
    mode_str = "incremental" if incremental else "full"

    print(f"Syncing EOD data for instruments ({mode_str} mode)...")
    print(f"  Types: {', '.join(types_list)}")
    if args.limit:
        print(f"  Limit: {args.limit}")
    print("-" * 50)

    def progress_callback(current, total, symbol):
        print(f"  [{current}/{total}] {symbol}")

    summary = sync_instruments_eod(
        db_path=args.db,
        instrument_types=types_list,
        incremental=incremental,
        limit=args.limit,
        progress_callback=progress_callback,
    )

    print("\nInstrument EOD Sync Summary")
    print("=" * 50)
    print(f"  Total:         {summary.total}")
    print(f"  OK:            {summary.ok}")
    print(f"  No data:       {summary.no_data}")
    print(f"  Failed:        {summary.failed}")
    print(f"  Rows upserted: {summary.rows_upserted}")

    if summary.errors:
        print("\nErrors:")
        for symbol, error in summary.errors[:10]:
            print(f"  {symbol}: {error}")
        if len(summary.errors) > 10:
            print(f"  ... and {len(summary.errors) - 10} more")

    return EXIT_SUCCESS if summary.ok > 0 else EXIT_ERROR


def handle_instruments_rankings(args: argparse.Namespace) -> int:
    """Handle instruments rankings command."""
    from datetime import date

    types_list = [t.strip().upper() for t in args.types.split(",")]
    as_of = args.as_of or date.today().isoformat()
    top_n = args.top
    force_compute = args.compute

    print(f"Instrument Rankings (as of {as_of})")
    print(f"  Types: {', '.join(types_list)}")
    print("-" * 60)

    con = connect(args.db)
    init_schema(con)

    # Check if we need to compute rankings
    if force_compute:
        print("Computing rankings...")
        result = compute_rankings(con, as_of_date=as_of, instrument_types=types_list)
        if not result["success"]:
            print(f"Error: {result.get('error')}", file=sys.stderr)
            con.close()
            return EXIT_ERROR
        print(f"  Computed rankings for {result.get('instruments_ranked', 0)} instruments")

    # Get rankings
    rankings = get_rankings(con, as_of_date=as_of, instrument_types=types_list, top_n=top_n)
    con.close()

    if not rankings:
        print("\nNo rankings found.")
        print("Run 'psxsync instruments rankings --compute' to compute rankings.")
        return EXIT_SUCCESS

    if args.out == "csv":
        print("rank,symbol,name,type,return_1m,return_3m,return_6m,return_1y,volatility_30d,relative_strength")
        for r in rankings:
            print(
                f"{r.get('rank', '')},"
                f"{r['symbol']},"
                f"\"{r.get('name', '')}\","
                f"{r.get('instrument_type', '')},"
                f"{r.get('return_1m', ''):.4f},"
                f"{r.get('return_3m', ''):.4f},"
                f"{r.get('return_6m', ''):.4f},"
                f"{r.get('return_1y', ''):.4f},"
                f"{r.get('volatility_30d', ''):.4f},"
                f"{r.get('relative_strength', ''):.4f}"
            )
    else:
        # Table format
        print(
            f"{'#':<4} {'SYMBOL':<12} {'TYPE':<6} "
            f"{'1M':>8} {'3M':>8} {'6M':>8} {'1Y':>8} {'VOL30':>8} {'RS':>6}"
        )
        print("-" * 80)
        for i, r in enumerate(rankings, 1):
            ret_1m = f"{r.get('return_1m', 0) * 100:.1f}%" if r.get('return_1m') else "N/A"
            ret_3m = f"{r.get('return_3m', 0) * 100:.1f}%" if r.get('return_3m') else "N/A"
            ret_6m = f"{r.get('return_6m', 0) * 100:.1f}%" if r.get('return_6m') else "N/A"
            ret_1y = f"{r.get('return_1y', 0) * 100:.1f}%" if r.get('return_1y') else "N/A"
            vol = f"{r.get('volatility_30d', 0) * 100:.1f}%" if r.get('volatility_30d') else "N/A"
            rs = f"{r.get('relative_strength', 0):.2f}" if r.get('relative_strength') else "N/A"
            print(
                f"{i:<4} {r['symbol']:<12} {r.get('instrument_type', ''):<6} "
                f"{ret_1m:>8} {ret_3m:>8} {ret_6m:>8} {ret_1y:>8} {vol:>8} {rs:>6}"
            )

        print(f"\nShowing top {len(rankings)} instruments")

    return EXIT_SUCCESS


def handle_instruments_sync_status(args: argparse.Namespace) -> int:
    """Handle instruments sync-status command."""
    runs = get_instruments_sync_status(args.db)

    if not runs:
        print("No sync runs found.")
        return EXIT_SUCCESS

    print("Recent Instrument Sync Runs")
    print("=" * 70)
    print(f"{'RUN ID':<10} {'TYPES':<20} {'STATUS':<10} {'STARTED':<20}")
    print("-" * 70)

    for run in runs:
        print(
            f"{run['run_id']:<10} "
            f"{run.get('instrument_types', ''):<20} "
            f"{run.get('status', ''):<10} "
            f"{run.get('started_at', '')[:19]:<20}"
        )

    return EXIT_SUCCESS


# =============================================================================
# Phase 2: FX command handlers
# =============================================================================

def handle_fx(args: argparse.Namespace) -> int:
    """Handle fx subcommands."""
    if args.fx_command == "seed":
        return handle_fx_seed(args)
    elif args.fx_command == "sync":
        return handle_fx_sync(args)
    elif args.fx_command == "show":
        return handle_fx_show(args)
    elif args.fx_command == "compute-adjusted":
        return handle_fx_compute_adjusted(args)
    elif args.fx_command == "status":
        return handle_fx_status(args)
    return EXIT_ERROR


def handle_fx_seed(args: argparse.Namespace) -> int:
    """Handle fx seed command."""
    print("Seeding FX pairs (Phase 2)...")
    print("-" * 50)

    result = seed_fx_pairs(db_path=args.db)

    print("\nFX Seed Complete")
    print("=" * 50)
    print(f"  Pairs seeded: {result.get('inserted', 0)}")
    print(f"  Failed:       {result.get('failed', 0)}")
    print(f"  Total:        {result.get('total', 0)}")

    return EXIT_SUCCESS


def handle_fx_sync(args: argparse.Namespace) -> int:
    """Handle fx sync command."""
    # Parse pairs
    pairs = None
    if args.pairs:
        pairs = [p.strip() for p in args.pairs.split(",")]

    incremental = not args.full
    mode_str = "incremental" if incremental else "full"

    print(f"Syncing FX data ({mode_str} mode)...")
    if pairs:
        print(f"  Pairs: {', '.join(pairs)}")
    else:
        print("  Pairs: all active")
    print(f"  Source: {args.source}")
    print("-" * 50)

    def progress_callback(current, total, pair):
        print(f"  [{current}/{total}] {pair}")

    summary = sync_fx_pairs(
        pairs=pairs,
        db_path=args.db,
        incremental=incremental,
        source=args.source,
        progress_callback=progress_callback,
    )

    print("\nFX Sync Summary")
    print("=" * 50)
    print(f"  Total:         {summary.total}")
    print(f"  OK:            {summary.ok}")
    print(f"  No data:       {summary.no_data}")
    print(f"  Failed:        {summary.failed}")
    print(f"  Rows upserted: {summary.rows_upserted}")

    if summary.errors:
        print("\nErrors:")
        for pair, error in summary.errors[:5]:
            print(f"  {pair}: {error}")

    return EXIT_SUCCESS if summary.ok > 0 else EXIT_ERROR


def handle_fx_show(args: argparse.Namespace) -> int:
    """Handle fx show command."""
    from .db import get_fx_ohlcv, get_fx_pair

    pair = args.pair.upper()

    con = connect(args.db)
    init_schema(con)

    # Get pair info
    pair_info = get_fx_pair(con, pair)
    if not pair_info:
        print(f"FX pair '{pair}' not found.", file=sys.stderr)
        print("Run 'psxsync fx seed' to seed FX pairs.", file=sys.stderr)
        con.close()
        return EXIT_ERROR

    # Get analytics
    analytics = get_fx_analytics(con, pair)

    print(f"\nFX Analytics: {pair}")
    print("=" * 50)
    print(f"  Description: {pair_info.get('description', 'N/A')}")
    print(f"  Source:      {pair_info.get('source', 'N/A')}")
    print(f"  Active:      {'Yes' if pair_info.get('is_active') else 'No'}")

    if analytics.get("error"):
        print("\n  No data available.")
        con.close()
        return EXIT_SUCCESS

    print(f"\n  Latest Date: {analytics.get('latest_date', 'N/A')}")
    print(f"  Latest Rate: {analytics.get('latest_close', 'N/A')}")

    # Returns
    print("\n  Returns:")
    if analytics.get("return_1W"):
        print(f"    1 Week:  {analytics['return_1W'] * 100:.2f}%")
    if analytics.get("return_1M"):
        print(f"    1 Month: {analytics['return_1M'] * 100:.2f}%")
    if analytics.get("return_3M"):
        print(f"    3 Month: {analytics['return_3M'] * 100:.2f}%")

    # Volatility
    print("\n  Volatility (annualized):")
    if analytics.get("vol_1M"):
        print(f"    1 Month: {analytics['vol_1M'] * 100:.2f}%")
    if analytics.get("vol_3M"):
        print(f"    3 Month: {analytics['vol_3M'] * 100:.2f}%")

    # Trend
    trend = analytics.get("trend", {})
    if trend:
        print("\n  Trend Analysis:")
        print(f"    Direction:   {trend.get('trend_direction', 'N/A').upper()}")
        print(f"    Strength:    {trend.get('trend_strength', 'N/A')}")
        print(f"    50D MA:      {trend.get('moving_average', 'N/A')}")
        pct = trend.get('pct_from_ma', 0) * 100
        print(f"    From MA:     {pct:+.2f}%")

    # Recent data
    print("\n  Recent Rates:")
    df = get_fx_ohlcv(con, pair, limit=5)
    if not df.empty:
        for _, row in df.iterrows():
            print(f"    {row['date']}: {row['close']:.4f}")

    con.close()
    return EXIT_SUCCESS


def handle_fx_compute_adjusted(args: argparse.Namespace) -> int:
    """Handle fx compute-adjusted command."""
    from datetime import date

    as_of = args.as_of or date.today().isoformat()
    fx_pair = args.pair
    symbols = None
    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",")]

    print("Computing FX-adjusted metrics")
    print(f"  As of:   {as_of}")
    print(f"  FX Pair: {fx_pair}")
    if symbols:
        print(f"  Symbols: {len(symbols)}")
    else:
        print("  Symbols: all with recent data")
    print("-" * 50)

    con = connect(args.db)
    init_schema(con)

    result = compute_and_store_fx_adjusted_metrics(
        con,
        symbols=symbols,
        fx_pair=fx_pair,
        as_of_date=as_of,
    )

    con.close()

    if result.get("success"):
        print("\nFX-Adjusted Metrics Complete")
        print("=" * 50)
        print(f"  Symbols processed: {result.get('symbols_processed', 0)}")
        print(f"  Metrics stored:    {result.get('metrics_stored', 0)}")
        print(f"  Metrics failed:    {result.get('metrics_failed', 0)}")
        return EXIT_SUCCESS
    else:
        print(f"Error: {result.get('error')}", file=sys.stderr)
        return EXIT_ERROR


def handle_fx_status(args: argparse.Namespace) -> int:
    """Handle fx status command."""
    summary = get_fx_data_summary(args.db)

    print("\nFX Data Summary")
    print("=" * 50)
    print(f"  Total pairs:  {summary.get('total_pairs', 0)}")
    print(f"  Active pairs: {summary.get('active_pairs', 0)}")

    print("\nPair Details:")
    print(f"{'PAIR':<12} {'SOURCE':<10} {'ACTIVE':<8} {'LATEST':<12} {'ROWS':<8}")
    print("-" * 55)

    for pair in summary.get("pairs", []):
        active = "Yes" if pair.get("is_active") else "No"
        latest = pair.get("latest_date") or "N/A"
        rows = pair.get("row_count", 0)
        print(
            f"{pair['pair']:<12} {pair.get('source', 'N/A'):<10} "
            f"{active:<8} {latest:<12} {rows:<8}"
        )

    # Recent sync runs
    runs = get_fx_sync_status(args.db)
    if runs:
        print("\nRecent Sync Runs:")
        print(f"{'RUN ID':<10} {'STATUS':<12} {'ROWS':<8} {'STARTED':<20}")
        print("-" * 55)
        for run in runs[:5]:
            print(
                f"{run['run_id']:<10} "
                f"{run.get('status', 'N/A'):<12} "
                f"{run.get('rows_upserted', 0):<8} "
                f"{run.get('started_at', '')[:19]:<20}"
            )

    return EXIT_SUCCESS


# =============================================================================
# Phase 2.5: MUFAP Handlers (Mutual Fund Analytics)
# =============================================================================


def handle_mufap(args: argparse.Namespace) -> int:
    """Handle mufap subcommands."""
    if args.mufap_command == "seed":
        return handle_mufap_seed(args)
    elif args.mufap_command == "sync":
        return handle_mufap_sync(args)
    elif args.mufap_command == "show":
        return handle_mufap_show(args)
    elif args.mufap_command == "list":
        return handle_mufap_list(args)
    elif args.mufap_command == "rankings":
        return handle_mufap_rankings(args)
    elif args.mufap_command == "status":
        return handle_mufap_status(args)
    return EXIT_ERROR


def handle_mufap_seed(args: argparse.Namespace) -> int:
    """Handle mufap seed command."""
    from .sync_mufap import seed_mutual_funds

    print("Seeding Mutual Funds (Phase 2.5: MUFAP Integration)...")
    print("-" * 50)

    include_vps = not getattr(args, "no_vps", False)

    result = seed_mutual_funds(
        db_path=args.db,
        category=args.category,
        include_vps=include_vps,
    )

    print("\nMutual Fund Seed Complete")
    print("=" * 50)
    print(f"  Funds seeded: {result.get('inserted', 0)}")
    print(f"  Failed:       {result.get('failed', 0)}")
    print(f"  Total:        {result.get('total', 0)}")

    return EXIT_SUCCESS


def handle_mufap_sync(args: argparse.Namespace) -> int:
    """Handle mufap sync command."""
    from .sync_mufap import sync_mutual_funds

    # Parse funds
    fund_ids = None
    if args.funds:
        fund_ids = [f.strip() for f in args.funds.split(",")]

    incremental = not args.full
    mode_str = "incremental" if incremental else "full"

    print(f"Syncing Mutual Fund NAV data ({mode_str} mode)...")
    if fund_ids:
        print(f"  Funds: {', '.join(fund_ids)}")
    else:
        print("  Funds: all active")
    if args.category:
        print(f"  Category: {args.category}")
    print(f"  Source: {args.source}")
    print("-" * 50)

    def progress_callback(current, total, fund_id):
        print(f"  [{current}/{total}] {fund_id}")

    summary = sync_mutual_funds(
        fund_ids=fund_ids,
        db_path=args.db,
        incremental=incremental,
        source=args.source,
        category=args.category,
        progress_callback=progress_callback,
    )

    print("\nMutual Fund Sync Summary")
    print("=" * 50)
    print(f"  Total:         {summary.total}")
    print(f"  OK:            {summary.ok}")
    print(f"  No data:       {summary.no_data}")
    print(f"  Failed:        {summary.failed}")
    print(f"  Rows upserted: {summary.rows_upserted}")

    if summary.errors:
        print("\nErrors:")
        for fund_id, error in summary.errors[:5]:
            print(f"  {fund_id}: {error}")

    return EXIT_SUCCESS if summary.ok > 0 else EXIT_ERROR


def handle_mufap_show(args: argparse.Namespace) -> int:
    """Handle mufap show command."""
    from .analytics_mufap import get_mf_analytics
    from .db import get_mf_nav, get_mutual_fund, get_mutual_fund_by_symbol

    fund_query = args.fund

    con = connect(args.db)
    init_schema(con)

    # Try to find fund by ID or symbol
    fund = get_mutual_fund(con, fund_query)
    if not fund:
        fund = get_mutual_fund_by_symbol(con, fund_query)
    if not fund:
        print(f"Mutual fund '{fund_query}' not found.", file=sys.stderr)
        print("Run 'psxsync mufap seed' to seed mutual funds.", file=sys.stderr)
        con.close()
        return EXIT_ERROR

    fund_id = fund["fund_id"]

    # Get analytics
    analytics = get_mf_analytics(con, fund_id)

    print(f"\nMutual Fund Analytics: {fund.get('symbol', fund_id)}")
    print("=" * 60)
    print(f"  Fund Name:     {fund.get('fund_name', 'N/A')}")
    print(f"  AMC:           {fund.get('amc_name', 'N/A')}")
    print(f"  Category:      {fund.get('category', 'N/A')}")
    print(f"  Fund Type:     {fund.get('fund_type', 'N/A')}")
    print(f"  Shariah:       {'Yes' if fund.get('is_shariah') else 'No'}")
    print(f"  Expense Ratio: {fund.get('expense_ratio', 'N/A')}%")

    if analytics.get("error"):
        print("\n  No NAV data available.")
        con.close()
        return EXIT_SUCCESS

    print(f"\n  Latest Date:   {analytics.get('latest_date', 'N/A')}")
    print(f"  Latest NAV:    Rs. {analytics.get('latest_nav', 'N/A')}")
    if analytics.get("latest_aum"):
        print(f"  AUM:           Rs. {analytics.get('latest_aum', 0):.1f}M")

    # Returns
    print("\n  Returns:")
    for period in ["1W", "1M", "3M", "6M", "1Y"]:
        key = f"return_{period}"
        if analytics.get(key) is not None:
            print(f"    {period}:  {analytics[key] * 100:+.2f}%")

    # Volatility
    if analytics.get("vol_1M") or analytics.get("vol_3M"):
        print("\n  Volatility (annualized):")
        if analytics.get("vol_1M"):
            print(f"    1M:  {analytics['vol_1M'] * 100:.2f}%")
        if analytics.get("vol_3M"):
            print(f"    3M:  {analytics['vol_3M'] * 100:.2f}%")

    # Sharpe ratio
    if analytics.get("sharpe_ratio") is not None:
        print(f"\n  Sharpe Ratio:  {analytics['sharpe_ratio']:.2f}")

    # Max drawdown
    if analytics.get("max_drawdown") is not None:
        print(f"  Max Drawdown:  {analytics['max_drawdown'] * 100:.2f}%")

    # Recent NAV
    print("\n  Recent NAV:")
    df = get_mf_nav(con, fund_id, limit=5)
    if not df.empty:
        for _, row in df.iterrows():
            change = row.get("nav_change_pct", 0) or 0
            print(f"    {row['date']}: Rs. {row['nav']:.4f} ({change:+.2f}%)")

    con.close()
    return EXIT_SUCCESS


def handle_mufap_list(args: argparse.Namespace) -> int:
    """Handle mufap list command."""
    from .db import get_mutual_funds

    con = connect(args.db)
    init_schema(con)

    # Parse filters
    fund_type = None if args.type == "ALL" else args.type
    is_shariah = True if getattr(args, "shariah_only", False) else None

    funds = get_mutual_funds(
        con,
        category=args.category,
        fund_type=fund_type,
        is_shariah=is_shariah,
        active_only=True,
    )

    if not funds:
        print("No mutual funds found.", file=sys.stderr)
        print("Run 'psxsync mufap seed' to seed mutual funds.", file=sys.stderr)
        con.close()
        return EXIT_ERROR

    print(f"\nMutual Funds ({len(funds)} found)")
    print("=" * 80)
    print(
        f"{'SYMBOL':<15} {'CATEGORY':<18} {'TYPE':<10} "
        f"{'SHARIAH':<8} {'AMC':<15}"
    )
    print("-" * 80)

    for fund in funds:
        shariah = "Yes" if fund.get("is_shariah") else "No"
        amc = (fund.get("amc_name") or "N/A")[:15]
        category = (fund.get("category") or "N/A")[:18]
        fund_type = (fund.get("fund_type") or "N/A")[:10]
        symbol = (fund.get("symbol") or "N/A")[:15]
        print(f"{symbol:<15} {category:<18} {fund_type:<10} {shariah:<8} {amc:<15}")

    con.close()
    return EXIT_SUCCESS


def handle_mufap_rankings(args: argparse.Namespace) -> int:
    """Handle mufap rankings command."""
    from .analytics_mufap import get_category_performance, get_category_summary

    con = connect(args.db)
    init_schema(con)

    category = args.category
    period = args.period
    top_n = args.top

    # Get category summary
    summary = get_category_summary(con, category, period)

    if summary.get("error"):
        print(f"No data for category '{category}'.", file=sys.stderr)
        print("Run 'psxsync mufap seed' and 'psxsync mufap sync' first.", file=sys.stderr)
        con.close()
        return EXIT_ERROR

    print(f"\nCategory Performance: {category}")
    print("=" * 70)
    print(f"  Period:      {period}")
    print(f"  Fund Count:  {summary.get('fund_count', 0)}")
    print(f"  Avg Return:  {summary.get('avg_return_pct', 0):+.2f}%")
    print(f"  Best:        {summary.get('max_return_pct', 0):+.2f}% ({summary.get('best_fund_symbol', 'N/A')})")
    print(f"  Worst:       {summary.get('min_return_pct', 0):+.2f}% ({summary.get('worst_fund_symbol', 'N/A')})")

    # Get rankings
    rankings = get_category_performance(con, category, period, top_n)

    if rankings:
        print(f"\nTop {len(rankings)} Funds:")
        print("-" * 70)
        print(f"{'RANK':<6} {'SYMBOL':<15} {'RETURN':<10} {'NAV':<12} {'SHARIAH':<8}")
        print("-" * 70)

        for r in rankings:
            shariah = "Yes" if r.get("is_shariah") else "No"
            nav = r.get("latest_nav")
            nav_str = f"Rs.{nav:.2f}" if nav else "N/A"
            print(
                f"{r.get('rank', '-'):<6} "
                f"{r.get('symbol', 'N/A'):<15} "
                f"{r.get('return_pct', 0):+.2f}%{'':<4} "
                f"{nav_str:<12} "
                f"{shariah:<8}"
            )

    con.close()
    return EXIT_SUCCESS


def handle_mufap_status(args: argparse.Namespace) -> int:
    """Handle mufap status command."""
    from .sync_mufap import get_data_summary, get_sync_status

    summary = get_data_summary(args.db)

    print("\nMutual Fund Data Summary")
    print("=" * 60)
    print(f"  Total funds:     {summary.get('total_funds', 0)}")
    print(f"  Active funds:    {summary.get('active_funds', 0)}")
    print(f"  Funds with NAV:  {summary.get('funds_with_nav', 0)}")
    print(f"  Total NAV rows:  {summary.get('total_nav_rows', 0)}")

    if summary.get("latest_nav_date"):
        print(f"  Latest NAV date: {summary['latest_nav_date']}")
    if summary.get("earliest_nav_date"):
        print(f"  Earliest date:   {summary['earliest_nav_date']}")

    # Category breakdown
    categories = summary.get("categories", {})
    if categories:
        print("\nCategory Breakdown:")
        print(f"  {'CATEGORY':<25} {'COUNT':<8}")
        print("  " + "-" * 35)
        for cat, count in categories.items():
            print(f"  {cat:<25} {count:<8}")

    # Fund type breakdown
    fund_types = summary.get("fund_types", {})
    if fund_types:
        print("\nFund Type Breakdown:")
        print(f"  {'TYPE':<15} {'COUNT':<8}")
        print("  " + "-" * 25)
        for ft, count in fund_types.items():
            print(f"  {ft:<15} {count:<8}")

    # Recent sync runs
    runs = get_sync_status(args.db)
    if runs:
        print("\nRecent Sync Runs:")
        print(f"  {'RUN ID':<10} {'TYPE':<12} {'STATUS':<12} {'FUNDS':<8} {'ROWS':<8}")
        print("  " + "-" * 55)
        for run in runs[:5]:
            print(
                f"  {run['run_id']:<10} "
                f"{run.get('sync_type', 'N/A'):<12} "
                f"{run.get('status', 'N/A'):<12} "
                f"{run.get('funds_ok', 0):<8} "
                f"{run.get('rows_upserted', 0):<8}"
            )

    return EXIT_SUCCESS


# =============================================================================
# Phase 3: Bonds/Sukuk Handlers
# =============================================================================


def handle_bonds(args: argparse.Namespace) -> int:
    """Handle bonds subcommands."""
    cmd = args.bonds_command

    if cmd == "init":
        return handle_bonds_init(args)
    elif cmd == "load":
        return handle_bonds_load(args)
    elif cmd == "compute":
        return handle_bonds_compute(args)
    elif cmd == "list":
        return handle_bonds_list(args)
    elif cmd == "quote":
        return handle_bonds_quote(args)
    elif cmd == "curve":
        return handle_bonds_curve(args)
    elif cmd == "status":
        return handle_bonds_status(args)

    return EXIT_ERROR


def handle_bonds_init(args: argparse.Namespace) -> int:
    """Handle bonds init command."""
    from .sync_bonds import seed_bonds

    bond_type = None if args.type == "ALL" else args.type
    include_islamic = not getattr(args, "no_islamic", False)

    print("Initializing bond tables and seeding default bonds...")

    result = seed_bonds(
        db_path=args.db,
        bond_type=bond_type,
        include_islamic=include_islamic,
    )

    if result.get("success"):
        print(f"Inserted: {result['inserted']} bonds")
        print(f"Failed:   {result['failed']}")
        print(f"Total:    {result['total']}")
        return EXIT_SUCCESS

    print(f"Error: {result.get('error', 'Unknown error')}", file=sys.stderr)
    return EXIT_ERROR


def handle_bonds_load(args: argparse.Namespace) -> int:
    """Handle bonds load command."""
    from .sync_bonds import load_bonds_csv, load_quotes_csv, sync_sample_quotes

    loaded_any = False

    # Load master data
    if args.master:
        print(f"Loading bond master data from {args.master}...")
        result = load_bonds_csv(args.master, args.db)
        if result.get("success"):
            print(f"  Inserted: {result['inserted']} bonds")
            loaded_any = True
        else:
            print(f"  Error: {result.get('error')}", file=sys.stderr)

    # Load quotes
    if args.quotes:
        print(f"Loading bond quotes from {args.quotes}...")
        result = load_quotes_csv(args.quotes, args.db)
        if result.get("success"):
            print(f"  Upserted: {result['rows_upserted']} quotes")
            loaded_any = True
        else:
            print(f"  Error: {result.get('error')}", file=sys.stderr)

    # Generate sample data
    if args.sample:
        print(f"Generating {args.days} days of sample quote data...")
        summary = sync_sample_quotes(db_path=args.db, days=args.days)
        print(f"  Generated: {summary.rows_upserted} quotes")
        print(f"  Bonds:     {summary.ok}")
        loaded_any = True

    if not loaded_any:
        print("No data loaded. Use --master, --quotes, or --sample.", file=sys.stderr)
        return EXIT_ERROR

    return EXIT_SUCCESS


def handle_bonds_compute(args: argparse.Namespace) -> int:
    """Handle bonds compute command."""
    from .analytics_bonds import build_yield_curve, compute_and_store_analytics

    bond_ids = None
    if args.bonds:
        bond_ids = [b.strip() for b in args.bonds.split(",")]

    print("Computing bond analytics...")
    result = compute_and_store_analytics(
        con=connect(args.db),
        bond_ids=bond_ids,
        as_of_date=args.as_of,
    )

    print(f"  Stored: {result.get('stored', 0)} analytics records")
    print(f"  Failed: {result.get('failed', 0)}")

    if args.curve:
        print("\nBuilding yield curve...")
        con = connect(args.db)
        init_schema(con)
        points = build_yield_curve(con, args.as_of)
        con.close()
        print(f"  Curve points: {len(points)}")

    return EXIT_SUCCESS


def handle_bonds_list(args: argparse.Namespace) -> int:
    """Handle bonds list command."""
    from .db import get_bonds

    con = connect(args.db)
    init_schema(con)

    bond_type = None if args.type == "ALL" else args.type
    is_islamic = True if getattr(args, "islamic_only", False) else None

    bonds = get_bonds(
        con,
        bond_type=bond_type,
        issuer=args.issuer,
        is_islamic=is_islamic,
        active_only=True,
    )

    if not bonds:
        print("No bonds found.", file=sys.stderr)
        print("Run 'psxsync bonds init' to seed default bonds.", file=sys.stderr)
        con.close()
        return EXIT_ERROR

    print(f"\nBonds ({len(bonds)} found)")
    print("=" * 90)
    hdr = f"{'SYMBOL':<20} {'TYPE':<10} {'ISSUER':<8} {'COUPON':<8} {'MATURITY':<12} {'ISLAMIC'}"
    print(hdr)
    print("-" * 90)

    for bond in bonds:
        coupon = bond.get("coupon_rate")
        coupon_str = f"{coupon * 100:.1f}%" if coupon else "Zero"
        islamic = "Yes" if bond.get("is_islamic") else "No"
        symbol = (bond.get("symbol") or "N/A")[:20]
        btype = (bond.get("bond_type") or "N/A")[:10]
        issuer = (bond.get("issuer") or "N/A")[:8]
        maturity = bond.get("maturity_date", "N/A")[:12]
        print(f"{symbol:<20} {btype:<10} {issuer:<8} {coupon_str:<8} {maturity:<12} {islamic}")

    con.close()
    return EXIT_SUCCESS


def handle_bonds_quote(args: argparse.Namespace) -> int:
    """Handle bonds quote command."""
    from .analytics_bonds import get_bond_full_analytics
    from .db import get_bond, get_bond_by_symbol

    con = connect(args.db)
    init_schema(con)

    # Find bond by ID or symbol
    bond = get_bond(con, args.bond)
    if not bond:
        bond = get_bond_by_symbol(con, args.bond)

    if not bond:
        print(f"Bond not found: {args.bond}", file=sys.stderr)
        con.close()
        return EXIT_ERROR

    bond_id = bond["bond_id"]
    analytics = get_bond_full_analytics(con, bond_id)

    print(f"\nBond Analytics: {bond.get('symbol', bond_id)}")
    print("=" * 60)
    print(f"  Bond ID:      {bond_id}")
    print(f"  Type:         {bond.get('bond_type')}")
    print(f"  Issuer:       {bond.get('issuer')}")
    print(f"  Islamic:      {'Yes' if bond.get('is_islamic') else 'No'}")
    print(f"  Face Value:   {bond.get('face_value')}")
    coupon = bond.get("coupon_rate")
    print(f"  Coupon:       {coupon * 100:.2f}%" if coupon else "  Coupon:       Zero-coupon")
    print(f"  Maturity:     {bond.get('maturity_date')}")

    if analytics.get("error"):
        print("\n  No quote data available.")
        con.close()
        return EXIT_SUCCESS

    print(f"\n  As of Date:   {analytics.get('as_of_date', 'N/A')}")
    print(f"  Days to Mat:  {analytics.get('days_to_maturity', 'N/A')}")

    # Price and yield
    if analytics.get("price"):
        print(f"\n  Clean Price:  {analytics['price']:.4f}")
    if analytics.get("dirty_price"):
        print(f"  Dirty Price:  {analytics['dirty_price']:.4f}")
    if analytics.get("ytm"):
        print(f"  YTM:          {analytics['ytm'] * 100:.4f}%")
    if analytics.get("accrued_interest"):
        print(f"  Accrued Int:  {analytics['accrued_interest']:.4f}")

    # Duration and convexity
    if analytics.get("duration"):
        print(f"\n  Duration:     {analytics['duration']:.4f} years")
    if analytics.get("modified_duration"):
        print(f"  Mod Duration: {analytics['modified_duration']:.4f}")
    if analytics.get("convexity"):
        print(f"  Convexity:    {analytics['convexity']:.4f}")

    con.close()
    return EXIT_SUCCESS


def handle_bonds_curve(args: argparse.Namespace) -> int:
    """Handle bonds curve command."""
    from .db import get_latest_yield_curve, get_yield_curve

    con = connect(args.db)
    init_schema(con)

    bond_type = args.type
    curve_date = args.date

    if curve_date:
        points = get_yield_curve(con, curve_date, bond_type)
    else:
        curve_date, points = get_latest_yield_curve(con, bond_type)

    if not points:
        print(f"No yield curve data for {bond_type}.", file=sys.stderr)
        print("Run 'psxsync bonds compute --curve' first.", file=sys.stderr)
        con.close()
        return EXIT_ERROR

    print(f"\nYield Curve: {bond_type}")
    print(f"Date: {curve_date}")
    print("=" * 50)
    print(f"{'TENOR':<12} {'YIELD':<12} {'RATE (%)':<10}")
    print("-" * 50)

    tenor_labels = {
        3: "3M", 6: "6M", 12: "1Y", 24: "2Y",
        36: "3Y", 60: "5Y", 84: "7Y", 120: "10Y",
        180: "15Y", 240: "20Y",
    }

    for p in points:
        tenor = p.get("tenor_months", 0)
        label = tenor_labels.get(tenor, f"{tenor}M")
        rate = p.get("yield_rate", 0)
        print(f"{label:<12} {tenor:<12} {rate * 100:.4f}%")

    con.close()
    return EXIT_SUCCESS


def handle_bonds_status(args: argparse.Namespace) -> int:
    """Handle bonds status command."""
    from .sync_bonds import get_data_summary, get_sync_status

    summary = get_data_summary(args.db)

    print("\nBond Data Summary")
    print("=" * 60)
    print(f"  Total bonds:      {summary.get('total_bonds', 0)}")
    print(f"  Active bonds:     {summary.get('active_bonds', 0)}")
    print(f"  Islamic (Sukuk):  {summary.get('islamic_count', 0)}")
    print(f"  With quotes:      {summary.get('bonds_with_quotes', 0)}")
    print(f"  Total quote rows: {summary.get('total_quote_rows', 0)}")
    print(f"  Yield curve pts:  {summary.get('yield_curve_dates', 0)}")

    if summary.get("latest_quote_date"):
        print(f"  Latest quote:     {summary['latest_quote_date']}")
    if summary.get("earliest_quote_date"):
        print(f"  Earliest quote:   {summary['earliest_quote_date']}")

    # Bond type breakdown
    bond_types = summary.get("bond_types", {})
    if bond_types:
        print("\nBond Type Breakdown:")
        print(f"  {'TYPE':<15} {'COUNT':<8}")
        print("  " + "-" * 25)
        for bt, count in bond_types.items():
            print(f"  {bt:<15} {count:<8}")

    # Issuer breakdown
    issuers = summary.get("issuers", {})
    if issuers:
        print("\nIssuer Breakdown:")
        print(f"  {'ISSUER':<15} {'COUNT':<8}")
        print("  " + "-" * 25)
        for issuer, count in issuers.items():
            print(f"  {issuer:<15} {count:<8}")

    # Recent sync runs
    runs = get_sync_status(args.db)
    if runs:
        print("\nRecent Sync Runs:")
        hdr = f"  {'RUN ID':<10} {'TYPE':<15} {'STATUS':<12} {'ITEMS':<8} {'ROWS':<8}"
        print(hdr)
        print("  " + "-" * 60)
        for run in runs[:5]:
            print(
                f"  {run['run_id']:<10} "
                f"{run.get('sync_type', 'N/A'):<15} "
                f"{run.get('status', 'N/A'):<12} "
                f"{run.get('items_ok', 0):<8} "
                f"{run.get('rows_upserted', 0):<8}"
            )

    return EXIT_SUCCESS


# =============================================================================
# Phase 3: Sukuk Handlers (Additive - does not modify existing bonds handlers)
# =============================================================================


def handle_sukuk(args: argparse.Namespace) -> int:
    """Handle sukuk subcommands."""
    cmd = args.sukuk_command

    if cmd == "seed":
        return handle_sukuk_seed(args)
    elif cmd == "sync":
        return handle_sukuk_sync(args)
    elif cmd == "load":
        return handle_sukuk_load(args)
    elif cmd == "compute":
        return handle_sukuk_compute(args)
    elif cmd == "list":
        return handle_sukuk_list(args)
    elif cmd == "show":
        return handle_sukuk_show(args)
    elif cmd == "curve":
        return handle_sukuk_curve(args)
    elif cmd == "sbp":
        return handle_sukuk_sbp(args)
    elif cmd == "compare":
        return handle_sukuk_compare(args)
    elif cmd == "status":
        return handle_sukuk_status(args)

    return EXIT_ERROR


def handle_sukuk_seed(args: argparse.Namespace) -> int:
    """Handle sukuk seed command."""
    category = None if args.category == "ALL" else args.category
    shariah_only = getattr(args, "shariah_only", False)

    print("Seeding sukuk master data...")

    result = seed_sukuk(
        db_path=args.db,
        category=category,
        shariah_only=shariah_only,
    )

    if result.get("success"):
        print(f"Inserted: {result['inserted']} instruments")
        print(f"Failed:   {result['failed']}")
        print(f"Total:    {result['total']}")
        return EXIT_SUCCESS

    print(f"Error: {result.get('error', 'Unknown error')}", file=sys.stderr)
    return EXIT_ERROR


def handle_sukuk_sync(args: argparse.Namespace) -> int:
    """Handle sukuk sync command."""
    instrument_ids = None
    if args.instruments:
        instrument_ids = [i.strip() for i in args.instruments.split(",")]

    print(f"Syncing sukuk quotes (source: {args.source})...")

    summary = sync_sukuk_quotes(
        instrument_ids=instrument_ids,
        db_path=args.db,
        source=args.source,
        days=args.days,
    )

    print(f"  Instruments: {summary.ok}")
    print(f"  Quotes:      {summary.rows_upserted}")
    print(f"  Failed:      {summary.failed}")

    if getattr(args, "include_curves", False):
        print("\nGenerating yield curves...")
        curve_summary = sync_sample_yield_curves(db_path=args.db, days=30)
        print(f"  Curves:  {curve_summary.ok}")
        print(f"  Points:  {curve_summary.rows_upserted}")

    return EXIT_SUCCESS


def handle_sukuk_load(args: argparse.Namespace) -> int:
    """Handle sukuk load command."""
    loaded_any = False

    # Load master data
    if args.master:
        print(f"Loading sukuk master data from {args.master}...")
        result = load_sukuk_csv(args.master, args.db)
        if result.get("success"):
            print(f"  Inserted: {result['inserted']} instruments")
            loaded_any = True
        else:
            print(f"  Error: {result.get('error')}", file=sys.stderr)

    # Load quotes
    if args.quotes:
        print(f"Loading sukuk quotes from {args.quotes}...")
        result = load_sukuk_quotes_csv(args.quotes, args.db)
        if result.get("success"):
            print(f"  Upserted: {result['rows_upserted']} quotes")
            loaded_any = True
        else:
            print(f"  Error: {result.get('error')}", file=sys.stderr)

    # Load yield curve
    if args.curve:
        print(f"Loading yield curve from {args.curve}...")
        result = load_yield_curve_csv(args.curve, args.db)
        if result.get("success"):
            print(f"  Upserted: {result['rows_upserted']} curve points")
            loaded_any = True
        else:
            print(f"  Error: {result.get('error')}", file=sys.stderr)

    if not loaded_any:
        print("No data loaded. Use --master, --quotes, or --curve.", file=sys.stderr)
        return EXIT_ERROR

    return EXIT_SUCCESS


def handle_sukuk_compute(args: argparse.Namespace) -> int:
    """Handle sukuk compute command."""
    instrument_ids = None
    if args.instruments:
        instrument_ids = [i.strip() for i in args.instruments.split(",")]

    print("Computing sukuk analytics (YTM, duration, convexity)...")

    result = compute_sukuk_analytics(
        db_path=args.db,
        instrument_ids=instrument_ids,
        calc_date=getattr(args, "as_of", None),
    )

    if result.get("success"):
        print(f"Computed: {result['computed']} instruments")
        print(f"Failed:   {result['failed']}")
        return EXIT_SUCCESS

    print(f"Error: {result.get('error', 'Unknown error')}", file=sys.stderr)
    return EXIT_ERROR


def handle_sukuk_list(args: argparse.Namespace) -> int:
    """Handle sukuk list command."""
    category = None if args.category == "ALL" else args.category
    shariah_only = getattr(args, "shariah_only", False)

    results = get_sukuk_by_category(category=category, db_path=args.db)

    if shariah_only:
        results = [r for r in results if r.get("shariah_compliant")]

    if args.issuer:
        results = [r for r in results if args.issuer.lower() in str(r.get("name", "")).lower()]

    if not results:
        print("No sukuk instruments found matching criteria.")
        return EXIT_SUCCESS

    # Print header
    hdr = f"{'INSTRUMENT ID':<30} {'NAME':<35} {'CAT':<15} {'COUPON':<8} {'YTM':<8}"
    print(hdr)
    print("-" * 100)

    for r in results:
        inst_id = (r.get("instrument_id") or "")[:30]
        name = (r.get("name") or "")[:35]
        cat = (r.get("category") or "")[:15]
        coupon = f"{r.get('coupon_rate', 0) or 0:.2f}%" if r.get("coupon_rate") else "N/A"
        ytm = f"{r.get('ytm', 0) or 0:.2f}%" if r.get("ytm") else "N/A"
        print(f"{inst_id:<30} {name:<35} {cat:<15} {coupon:<8} {ytm:<8}")

    print(f"\nTotal: {len(results)} instruments")
    return EXIT_SUCCESS


def handle_sukuk_show(args: argparse.Namespace) -> int:
    """Handle sukuk show command."""
    result = get_sukuk_analytics_full(args.instrument, args.db)

    if result.get("error"):
        print(f"Error: {result['error']}", file=sys.stderr)
        return EXIT_ERROR

    sukuk = result.get("sukuk", {})
    quote = result.get("quote", {})
    analytics = result.get("analytics", {})

    print("\nSukuk Details")
    print("=" * 60)
    print(f"  Instrument ID:    {sukuk.get('instrument_id')}")
    print(f"  Name:             {sukuk.get('name')}")
    print(f"  Issuer:           {sukuk.get('issuer')}")
    print(f"  Category:         {sukuk.get('category')}")
    print(f"  Maturity:         {sukuk.get('maturity_date')}")
    print(f"  Coupon Rate:      {sukuk.get('coupon_rate', 'N/A')}%")
    print(f"  Coupon Freq:      {sukuk.get('coupon_frequency', 2)}x per year")
    print(f"  Face Value:       {sukuk.get('face_value', 100)}")
    print(f"  Shariah:          {'Yes' if sukuk.get('shariah_compliant') else 'No'}")

    if quote:
        print("\nLatest Quote")
        print("-" * 40)
        print(f"  Date:             {quote.get('quote_date')}")
        print(f"  Clean Price:      {quote.get('clean_price')}")
        print(f"  Dirty Price:      {quote.get('dirty_price')}")
        print(f"  YTM:              {quote.get('yield_to_maturity')}%")
        print(f"  Bid Yield:        {quote.get('bid_yield')}%")
        print(f"  Ask Yield:        {quote.get('ask_yield')}%")

    if analytics.get("yield_to_maturity"):
        print("\nAnalytics")
        print("-" * 40)
        print(f"  YTM:              {analytics.get('yield_to_maturity')}%")
        print(f"  Macaulay Dur:     {analytics.get('macaulay_duration')} years")
        print(f"  Modified Dur:     {analytics.get('modified_duration')} years")
        print(f"  Convexity:        {analytics.get('convexity')}")
        print(f"  Current Yield:    {analytics.get('current_yield')}%")
        print(f"  Days to Mat:      {analytics.get('days_to_maturity')}")

    return EXIT_SUCCESS


def handle_sukuk_curve(args: argparse.Namespace) -> int:
    """Handle sukuk curve command."""
    result = get_yield_curve_data(
        curve_name=args.name,
        curve_date=args.date,
        db_path=args.db,
    )

    if result.get("error"):
        print(f"Error: {result['error']}", file=sys.stderr)
        print("Tip: Run 'psxsync sukuk sync --include-curves' to generate sample curves.")
        return EXIT_ERROR

    print(f"\nYield Curve: {result.get('curve_name')}")
    print(f"Date: {result.get('curve_date')}")
    print("=" * 40)

    points = result.get("points", [])
    if not points:
        print("No curve points found.")
        return EXIT_SUCCESS

    print(f"{'TENOR':<12} {'DAYS':<12} {'YIELD':<12}")
    print("-" * 36)

    for p in points:
        print(f"{p.get('tenor_label', ''):<12} {p.get('tenor_days', 0):<12} {p.get('yield_rate', 0):.4f}%")

    return EXIT_SUCCESS


def handle_sukuk_sbp(args: argparse.Namespace) -> int:
    """Handle sukuk sbp command - index SBP documents."""
    from .sources.sbp_primary_market import DOCS_DIR, create_sample_documents

    docs_dir = args.docs_dir or DOCS_DIR

    if getattr(args, "create_samples", False):
        print(f"Creating sample documents in {docs_dir}...")
        created = create_sample_documents(docs_dir)
        print(f"  Created: {len(created)} sample files")

    print(f"Indexing SBP documents from {docs_dir}...")
    result = index_sbp_documents(docs_dir=docs_dir, db_path=args.db)

    print(f"  Total documents:  {result.get('total_documents', 0)}")
    print(f"  Inserted:         {result.get('inserted', 0)}")
    print(f"  Failed:           {result.get('failed', 0)}")
    if result.get("index_path"):
        print(f"  Index file:       {result['index_path']}")

    return EXIT_SUCCESS


def handle_sukuk_compare(args: argparse.Namespace) -> int:
    """Handle sukuk compare command."""
    instrument_ids = [i.strip() for i in args.instruments.split(",")]

    if len(instrument_ids) < 2:
        print("Error: Please provide at least 2 instrument IDs to compare.", file=sys.stderr)
        return EXIT_ERROR

    results = compare_sukuk(instrument_ids, args.db)

    if not results:
        print("No instruments found matching the provided IDs.")
        return EXIT_ERROR

    print("\nSukuk Comparison")
    print("=" * 100)

    # Header
    hdr = f"{'INSTRUMENT':<25} {'CATEGORY':<12} {'COUPON':<8} {'YTM':<8} {'MOD DUR':<10} {'CONVEX':<10}"
    print(hdr)
    print("-" * 100)

    for r in results:
        inst = (r.get("instrument_id") or "")[:25]
        cat = (r.get("category") or "")[:12]
        coupon = f"{r.get('coupon_rate', 0) or 0:.2f}%" if r.get("coupon_rate") else "N/A"
        ytm = f"{r.get('ytm', 0) or 0:.2f}%" if r.get("ytm") else "N/A"
        mod_dur = f"{r.get('modified_duration', 0) or 0:.2f}" if r.get("modified_duration") else "N/A"
        convex = f"{r.get('convexity', 0) or 0:.2f}" if r.get("convexity") else "N/A"
        print(f"{inst:<25} {cat:<12} {coupon:<8} {ytm:<8} {mod_dur:<10} {convex:<10}")

    return EXIT_SUCCESS


def handle_sukuk_status(args: argparse.Namespace) -> int:
    """Handle sukuk status command."""
    summary = get_sukuk_data_summary(args.db)

    print("\nSukuk Data Summary")
    print("=" * 60)
    print(f"  Total instruments:   {summary.get('total_sukuk', 0)}")
    print(f"  Active instruments:  {summary.get('active_sukuk', 0)}")
    print(f"  Shariah compliant:   {summary.get('shariah_count', 0)}")
    print(f"  With quotes:         {summary.get('sukuk_with_quotes', 0)}")
    print(f"  Total quote rows:    {summary.get('total_quote_rows', 0)}")
    print(f"  Yield curve dates:   {summary.get('yield_curve_dates', 0)}")
    print(f"  SBP documents:       {summary.get('sbp_doc_count', 0)}")

    if summary.get("latest_quote_date"):
        print(f"  Latest quote:        {summary['latest_quote_date']}")
    if summary.get("earliest_quote_date"):
        print(f"  Earliest quote:      {summary['earliest_quote_date']}")

    # Category breakdown
    categories = summary.get("categories", {})
    if categories:
        print("\nCategory Breakdown:")
        print(f"  {'CATEGORY':<20} {'COUNT':<8}")
        print("  " + "-" * 30)
        for cat, count in categories.items():
            print(f"  {cat:<20} {count:<8}")

    # Recent sync runs
    runs = get_sukuk_sync_status(args.db)
    if runs:
        print("\nRecent Sync Runs:")
        hdr = f"  {'RUN ID':<10} {'TYPE':<15} {'STATUS':<12} {'ITEMS':<8} {'ROWS':<8}"
        print(hdr)
        print("  " + "-" * 60)
        for run in runs[:5]:
            print(
                f"  {run['run_id']:<10} "
                f"{run.get('sync_type', 'N/A'):<15} "
                f"{run.get('status', 'N/A'):<12} "
                f"{run.get('items_ok', 0):<8} "
                f"{run.get('rows_upserted', 0):<8}"
            )

    return EXIT_SUCCESS


# =============================================================================
# Phase 3.5: Fixed Income (Government Debt) handlers
# =============================================================================

def handle_fixed_income(args: argparse.Namespace) -> int:
    """Handle fixed-income subcommands."""
    cmd = args.fi_command

    if cmd == "seed":
        return handle_fi_seed(args)
    elif cmd == "sync":
        return handle_fi_sync(args)
    elif cmd == "compute":
        return handle_fi_compute(args)
    elif cmd == "list":
        return handle_fi_list(args)
    elif cmd == "show":
        return handle_fi_show(args)
    elif cmd == "curve":
        return handle_fi_curve(args)
    elif cmd == "sbp":
        return handle_fi_sbp(args)
    elif cmd == "templates":
        return handle_fi_templates(args)
    elif cmd == "status":
        return handle_fi_status(args)
    elif cmd == "service":
        return handle_fi_service(args)

    return EXIT_ERROR


def handle_fi_seed(args: argparse.Namespace) -> int:
    """Handle fixed-income seed command."""
    source = args.source
    csv_path = getattr(args, "csv", None)

    print(f"Seeding fixed income instruments (source: {source})...")

    result = seed_fi_instruments(
        db_path=args.db,
        source=source,
        csv_path=csv_path,
    )

    if result.get("success"):
        print(f"  Inserted: {result['inserted']} instruments")
        print(f"  Failed:   {result['failed']}")
        print(f"  Total:    {result['total']}")
        return EXIT_SUCCESS

    print(f"Error: {'; '.join(result.get('errors', []))}", file=sys.stderr)
    return EXIT_ERROR


def handle_fi_sync(args: argparse.Namespace) -> int:
    """Handle fixed-income sync command."""
    source = args.source
    quotes_csv = getattr(args, "quotes_csv", None)
    curves_csv = getattr(args, "curves_csv", None)

    if getattr(args, "sync_all", False):
        print("Syncing all fixed income data...")
        results = sync_all_fixed_income(
            db_path=args.db,
            source=source,
            quotes_csv=quotes_csv,
            curves_csv=curves_csv,
        )

        for key, summary in results.items():
            print(f"\n{key.upper()}:")
            if isinstance(summary, dict):
                for k, v in summary.items():
                    if k != "errors":
                        print(f"  {k}: {v}")
        return EXIT_SUCCESS

    # Sync quotes
    print(f"Syncing fixed income quotes (source: {source})...")
    quote_summary = sync_fi_quotes(
        db_path=args.db,
        source="CSV" if quotes_csv else source,
        csv_path=quotes_csv,
    )
    print(f"  Total:    {quote_summary.total}")
    print(f"  OK:       {quote_summary.ok}")
    print(f"  Upserted: {quote_summary.rows_upserted}")

    # Sync curves if provided
    if curves_csv:
        print(f"\nSyncing yield curves from {curves_csv}...")
        curve_summary = sync_fi_curves(
            db_path=args.db,
            source="CSV",
            csv_path=curves_csv,
        )
        print(f"  Total:    {curve_summary.total}")
        print(f"  OK:       {curve_summary.ok}")
        print(f"  Upserted: {curve_summary.rows_upserted}")

    return EXIT_SUCCESS


def handle_fi_compute(args: argparse.Namespace) -> int:
    """Handle fixed-income compute command."""
    isins = None
    if args.isins:
        isins = [i.strip() for i in args.isins.split(",")]

    as_of = getattr(args, "as_of", None)

    print("Computing fixed income analytics (YTM, duration, convexity)...")

    con = connect(args.db)
    result = compute_fi_analytics(con, isins=isins, as_of_date=as_of)
    con.close()

    if result.get("success"):
        print(f"  Computed: {result['stored']} instruments")
        print(f"  Failed:   {result['failed']}")
        return EXIT_SUCCESS

    print(f"Errors: {result.get('errors', [])}", file=sys.stderr)
    return EXIT_ERROR


def handle_fi_list(args: argparse.Namespace) -> int:
    """Handle fixed-income list command."""
    category = None if args.category == "ALL" else args.category
    min_yield = getattr(args, "min_yield", None)
    sort_by = getattr(args, "sort", "yield")

    con = connect(args.db)
    results = get_instruments_by_yield(
        con,
        category=category,
        min_yield=min_yield,
        sort_by=sort_by,
        limit=50,
    )
    con.close()

    if not results:
        print("No fixed income instruments found.")
        return EXIT_SUCCESS

    # Header
    hdr = (
        f"{'ISIN':<20} {'SYMBOL':<12} {'CAT':<10} "
        f"{'MATURITY':<12} {'YTM %':<8} {'DUR':<6}"
    )
    print(hdr)
    print("-" * 70)

    for inst in results:
        ytm = inst.get("yield_to_maturity")
        ytm_str = f"{ytm * 100:.2f}" if ytm else "N/A"
        dur = inst.get("modified_duration")
        dur_str = f"{dur:.2f}" if dur else "N/A"

        print(
            f"{inst.get('isin', 'N/A'):<20} "
            f"{inst.get('symbol', 'N/A'):<12} "
            f"{inst.get('category', 'N/A'):<10} "
            f"{inst.get('maturity_date', 'N/A'):<12} "
            f"{ytm_str:<8} "
            f"{dur_str:<6}"
        )

    print(f"\nTotal: {len(results)} instruments")
    return EXIT_SUCCESS


def handle_fi_show(args: argparse.Namespace) -> int:
    """Handle fixed-income show command."""
    isin = args.isin

    from .analytics_fixed_income import compute_analytics_for_instrument
    from .db import get_fi_instrument, get_fi_latest_quote, init_schema

    con = connect(args.db)
    init_schema(con)

    # Get instrument
    inst = get_fi_instrument(con, isin)
    if not inst:
        print(f"Instrument not found: {isin}", file=sys.stderr)
        con.close()
        return EXIT_ERROR

    print(f"\nFixed Income Instrument: {isin}")
    print("=" * 60)
    print(f"  Symbol:          {inst.get('symbol', 'N/A')}")
    print(f"  Category:        {inst.get('category', 'N/A')}")
    print(f"  Issuer:          {inst.get('issuer', 'N/A')}")
    print(f"  Issue Date:      {inst.get('issue_date', 'N/A')}")
    print(f"  Maturity Date:   {inst.get('maturity_date', 'N/A')}")
    print(f"  Coupon Rate:     {(inst.get('coupon_rate', 0) or 0) * 100:.2f}%")
    print(f"  Face Value:      {inst.get('face_value', 'N/A')}")
    print(f"  Is Shariah:      {'Yes' if inst.get('is_shariah') else 'No'}")

    # Get latest quote
    quote = get_fi_latest_quote(con, isin)
    if quote:
        print(f"\nLatest Quote ({quote.get('date')}):")
        print(f"  Clean Price:     {quote.get('clean_price', 'N/A')}")
        print(f"  Dirty Price:     {quote.get('dirty_price', 'N/A')}")
        ytm_val = (quote.get('yield_to_maturity', 0) or 0) * 100
        print(f"  YTM:             {ytm_val:.2f}%")

    # Compute analytics
    analytics = compute_analytics_for_instrument(con, isin)
    if analytics and "ytm" in analytics:
        print("\nComputed Analytics:")
        print(f"  Computed YTM:    {analytics.get('ytm_pct', 'N/A')}%")
        print(f"  Mac Duration:    {analytics.get('macaulay_duration', 'N/A')} years")
        print(f"  Mod Duration:    {analytics.get('modified_duration', 'N/A')}")
        print(f"  Convexity:       {analytics.get('convexity', 'N/A')}")
        print(f"  PVBP:            {analytics.get('pvbp', 'N/A')}")
        print(f"  Years to Mat:    {analytics.get('years_to_maturity', 'N/A')}")

    con.close()
    return EXIT_SUCCESS


def handle_fi_curve(args: argparse.Namespace) -> int:
    """Handle fixed-income curve command."""
    curve_name = args.name
    curve_date = getattr(args, "date", None)

    con = connect(args.db)
    analytics = get_yield_curve_analytics(con, curve_name, curve_date)
    con.close()

    if analytics.get("error"):
        print(f"Error: {analytics['error']}", file=sys.stderr)
        return EXIT_ERROR

    print(f"\nYield Curve: {curve_name}")
    print(f"Date: {analytics.get('curve_date', 'N/A')}")
    print("=" * 50)

    points = analytics.get("points", [])
    if points:
        print(f"\n{'TENOR':<12} {'YIELD %':<10}")
        print("-" * 25)
        for point in sorted(points, key=lambda x: x.get("tenor_months", 0)):
            tenor = point.get("tenor_months", 0)
            tenor_str = f"{tenor}M" if tenor < 12 else f"{tenor // 12}Y"
            yld = point.get("yield_value", 0) or 0
            print(f"{tenor_str:<12} {yld * 100:.2f}")

    # Summary
    if analytics.get("steepness") is not None:
        print(f"\nCurve Shape:  {analytics.get('shape', 'N/A')}")
        print(f"Steepness:    {analytics['steepness'] * 100:.2f} bps")
        short_t = analytics.get('short_tenor')
        short_y = (analytics.get('short_yield', 0) or 0) * 100
        print(f"Short ({short_t}M): {short_y:.2f}%")
        long_t = analytics.get('long_tenor')
        long_y = (analytics.get('long_yield', 0) or 0) * 100
        print(f"Long ({long_t}M):  {long_y:.2f}%")

    return EXIT_SUCCESS


def handle_fi_sbp(args: argparse.Namespace) -> int:
    """Handle fixed-income sbp command."""
    source = args.source
    download = getattr(args, "download", False)
    category = getattr(args, "category", None)

    print(f"Syncing SBP PMA documents (source: {source})...")

    summary = sync_sbp_pma_docs(
        db_path=args.db,
        source=source,
        download=download,
        category=category,
    )

    print(f"  Total:    {summary.total}")
    print(f"  OK:       {summary.ok}")
    print(f"  Failed:   {summary.failed}")
    print(f"  Upserted: {summary.rows_upserted}")

    if summary.errors:
        print("\nErrors:")
        for title, err in summary.errors[:5]:
            print(f"  {title}: {err}")

    return EXIT_SUCCESS


def handle_fi_templates(args: argparse.Namespace) -> int:
    """Handle fixed-income templates command."""
    print("Creating CSV templates for fixed income data entry...")

    result = setup_fi_csv_templates()

    for name, path in result.items():
        print(f"  Created: {path}")

    print(f"\nTotal: {len(result)} templates created")
    return EXIT_SUCCESS


def handle_fi_status(args: argparse.Namespace) -> int:
    """Handle fixed-income status command."""
    summary = get_fi_status_summary(args.db)

    print("\nFixed Income Data Summary")
    print("=" * 60)
    print(f"  Total instruments:   {summary.get('total_instruments', 0)}")
    print(f"  Active instruments:  {summary.get('active_instruments', 0)}")
    print(f"  With quotes:         {summary.get('instruments_with_quotes', 0)}")
    print(f"  Total quote rows:    {summary.get('total_quote_rows', 0)}")
    print(f"  Yield curves:        {summary.get('total_curve_points', 0)} points")
    print(f"  SBP documents:       {summary.get('sbp_doc_count', 0)}")

    # Latest by category
    latest = summary.get("latest_by_category", {})
    if latest:
        print("\nLatest Quote by Category:")
        for cat, date in latest.items():
            print(f"  {cat:<15} {date}")

    # Curve dates
    curves = summary.get("curve_dates", [])
    if curves:
        print("\nAvailable Curve Dates:")
        for curve in curves[:5]:
            c_name = curve.get('curve_name')
            c_date = curve.get('latest_date')
            c_count = curve.get('count')
            print(f"  {c_name}: {c_date} ({c_count} points)")

    # Recent sync runs
    runs = get_fi_sync_status(args.db)
    if runs:
        print("\nRecent Sync Runs:")
        hdr = f"  {'RUN ID':<10} {'TYPE':<20} {'STATUS':<12} {'ROWS':<8}"
        print(hdr)
        print("  " + "-" * 55)
        for run in runs[:5]:
            print(
                f"  {run['run_id']:<10} "
                f"{run.get('sync_type', 'N/A'):<20} "
                f"{run.get('status', 'N/A'):<12} "
                f"{run.get('rows_upserted', 0):<8}"
            )

    return EXIT_SUCCESS


def handle_fi_service(args: argparse.Namespace) -> int:
    """Handle fixed-income service command."""
    from .services.fi_sync_service import (
        is_fi_sync_running,
        read_fi_status,
        start_fi_sync_background,
        stop_fi_sync,
    )

    action = args.action

    if action == "status":
        running, pid = is_fi_sync_running()
        status = read_fi_status()

        print("\nFixed Income Sync Service Status")
        print("=" * 50)
        print(f"  Running:        {'Yes' if running else 'No'}")
        if running and pid:
            print(f"  PID:            {pid}")
        if status.started_at:
            print(f"  Started:        {status.started_at}")
        if status.last_sync_at:
            print(f"  Last sync:      {status.last_sync_at}")
        if status.next_sync_at:
            print(f"  Next sync:      {status.next_sync_at}")
        if status.continuous:
            print(f"  Mode:           Continuous (every {status.sync_interval}s)")
        else:
            print("  Mode:           One-time")
        print(f"  Sync count:     {status.sync_count}")
        print(f"  Docs synced:    {status.docs_synced}")
        print(f"  Curves synced:  {status.curves_synced}")
        if status.result:
            print(f"  Last result:    {status.result}")
        if status.error_message:
            print(f"  Last error:     {status.error_message}")
        if status.progress_message:
            print(f"  Status:         {status.progress_message}")

        return EXIT_SUCCESS

    elif action == "start":
        continuous = getattr(args, "continuous", False)
        interval = getattr(args, "interval", 3600)

        mode = "continuous" if continuous else "one-time"
        print(f"Starting FI sync service ({mode}, interval={interval}s)...")
        success, msg = start_fi_sync_background(
            continuous=continuous,
            sync_interval=interval,
        )
        print(msg)
        return EXIT_SUCCESS if success else EXIT_ERROR

    elif action == "stop":
        print("Stopping FI sync service...")
        success, msg = stop_fi_sync()
        print(msg)
        return EXIT_SUCCESS if success else EXIT_ERROR

    return EXIT_ERROR


def handle_etf(args: argparse.Namespace) -> int:
    """Handle ETF subcommands."""
    from .db.repositories.etf import (
        get_all_etf_latest_nav,
        get_etf_detail,
        get_etf_list,
        init_etf_schema,
    )
    from .sources.etf_scraper import ETFScraper

    con = connect(args.db)
    init_schema(con)
    init_etf_schema(con)

    if args.etf_command == "sync":
        print("Scraping ETF data from PSX DPS...")
        scraper = ETFScraper()
        result = scraper.sync_all_etfs(con)
        print(
            f"Done: {result['ok']}/{result['total']} ETFs synced, "
            f"{result['failed']} failed"
        )
        return 0

    elif args.etf_command == "list":
        df = get_all_etf_latest_nav(con)
        if df.empty:
            print("No ETF data. Run 'psxsync etf sync' first.")
            return 0
        print(df.to_string(index=False))
        return 0

    elif args.etf_command == "show":
        symbol = args.symbol.upper()
        detail = get_etf_detail(con, symbol)
        if not detail:
            print(f"ETF {symbol} not found. Run 'psxsync etf sync' first.")
            return 1
        print(f"\n{'='*50}")
        print(f"  {detail['symbol']} — {detail['name']}")
        print(f"{'='*50}")
        print(f"  AMC:         {detail.get('amc', 'N/A')}")
        print(f"  Benchmark:   {detail.get('benchmark_index', 'N/A')}")
        print(f"  Inception:   {detail.get('inception_date', 'N/A')}")
        print(f"  Mgmt Fee:    {detail.get('management_fee', 'N/A')}")
        print(f"  Shariah:     {'Yes' if detail.get('shariah_compliant') else 'No'}")
        if detail.get("latest_nav"):
            nav = detail["latest_nav"]
            print(f"\n  Latest NAV ({nav.get('date', 'N/A')}):")
            print(f"    iNAV:      {nav.get('nav', 'N/A')}")
            print(f"    Mkt Price: {nav.get('market_price', 'N/A')}")
            print(f"    Prem/Disc: {nav.get('premium_discount', 'N/A')}%")
            print(f"    AUM:       {nav.get('aum_millions', 'N/A')} M")
        return 0

    return 0


def handle_treasury(args: argparse.Namespace) -> int:
    """Handle treasury subcommands (T-Bill + PIB auctions)."""
    from .db.repositories.treasury import (
        get_latest_pib_yields,
        get_latest_tbill_yields,
        get_tbill_auctions,
        init_treasury_schema,
    )
    from .sources.sbp_treasury import SBPTreasuryScraper

    con = connect(args.db)
    init_schema(con)
    init_treasury_schema(con)

    if args.treasury_command == "sync":
        print("Scraping latest T-Bill + PIB rates from SBP PMA page...")
        scraper = SBPTreasuryScraper()
        result = scraper.sync_treasury(con)
        print(
            f"Done: {result['tbills_ok']} T-Bills, {result['pibs_ok']} PIBs saved, "
            f"{result['failed']} failed"
        )
        if result["auction_date"]:
            print(f"Auction date: {result['auction_date']}")
        return 0

    elif args.treasury_command == "tbill-latest":
        yields = get_latest_tbill_yields(con)
        if not yields:
            print("No T-Bill data. Run 'psxsync treasury sync' first.")
            return 0
        print(f"\n{'='*50}")
        print("  Latest T-Bill Cutoff Yields")
        print(f"{'='*50}")
        for tenor, data in sorted(yields.items()):
            print(
                f"  {tenor:>4s}:  {data.get('cutoff_yield', 'N/A'):>8}%  "
                f"(auction: {data.get('auction_date', 'N/A')})"
            )
        return 0

    elif args.treasury_command == "pib-latest":
        yields = get_latest_pib_yields(con)
        if not yields:
            print("No PIB data. Run 'psxsync treasury sync' first.")
            return 0
        print(f"\n{'='*50}")
        print("  Latest PIB Cutoff Yields")
        print(f"{'='*50}")
        for key, data in sorted(yields.items()):
            tenor = data.get("tenor", key)
            pib_type = data.get("pib_type", "Fixed")
            print(
                f"  {tenor:>4s} ({pib_type}):  {data.get('cutoff_yield', 'N/A'):>8}%  "
                f"(auction: {data.get('auction_date', 'N/A')})"
            )
        return 0

    elif args.treasury_command == "tbill-list":
        df = get_tbill_auctions(con, tenor=args.tenor)
        if df.empty:
            print("No T-Bill auction data. Run 'psxsync treasury sync' first.")
            return 0
        print(df.head(args.limit).to_string(index=False))
        return 0

    elif args.treasury_command == "gis-sync":
        from .sources.sbp_gsp import GSPScraper
        print("Scraping GIS (Ijara Sukuk) auction data...")
        gsp = GSPScraper()
        result = gsp.sync_gis(con)
        print(
            f"Done: {result['ok']}/{result['total']} GIS records saved, "
            f"{result['failed']} failed (source: {result['source']})"
        )
        return 0

    elif args.treasury_command == "gis-list":
        from .db.repositories.treasury import get_gis_auctions
        df = get_gis_auctions(con)
        if df.empty:
            print("No GIS data. Run 'psxsync treasury gis-sync' first.")
            return 0
        print(df.to_string(index=False))
        return 0

    elif args.treasury_command == "summary":
        from .sources.sbp_gsp import GSPScraper
        scraper = SBPTreasuryScraper()
        summary = scraper.get_summary(con)
        gsp = GSPScraper()
        gis_summary = gsp.get_gis_summary(con)
        print(f"\n{'='*50}")
        print("  Treasury Data Summary")
        print(f"{'='*50}")
        print(f"  T-Bill tenors: {summary['tbill_tenors']}")
        print(f"  PIB tenors:    {summary['pib_tenors']}")
        print(f"  Total T-Bill auction records: {summary['total_tbill_records']}")
        print(f"  GIS records:   {gis_summary['total_records']}")
        print(f"  GIS types:     {gis_summary['gis_types']}")
        return 0

    return 0


def handle_rates(args: argparse.Namespace) -> int:
    """Handle rates subcommands (yield curve, KONIA, KIBOR)."""
    from .db.repositories.yield_curves import (
        get_kibor_history,
        get_latest_konia,
        get_pkrv_curve,
        init_yield_curve_schema,
    )
    from .sources.sbp_rates import SBPRatesScraper

    con = connect(args.db)
    init_schema(con)
    init_yield_curve_schema(con)

    if args.rates_command == "sync":
        print("Scraping KONIA + KIBOR + yield curve from SBP PMA page...")
        scraper = SBPRatesScraper()
        result = scraper.sync_rates(con)
        print(
            f"Done: KONIA={'OK' if result['konia_ok'] else 'N/A'}, "
            f"{result['kibor_ok']} KIBOR rates, "
            f"{result['pkrv_points']} yield curve points, "
            f"{result['failed']} failed"
        )
        return 0

    elif args.rates_command == "konia":
        konia = get_latest_konia(con)
        if not konia:
            print("No KONIA data. Run 'psxsync rates sync' first.")
            return 0
        print(f"\n  KONIA (Overnight Rate): {konia['rate_pct']}%")
        print(f"  Date: {konia['date']}")
        return 0

    elif args.rates_command == "kibor":
        df = get_kibor_history(con)
        if df.empty:
            print("No KIBOR data. Run 'psxsync rates sync' first.")
            return 0
        # Show latest date only
        latest_date = df.iloc[0]["date"]
        latest = df[df["date"] == latest_date]
        print(f"\n  KIBOR Rates (as of {latest_date})")
        print(f"  {'Tenor':>6s}  {'Bid':>8s}  {'Offer':>8s}")
        for _, row in latest.iterrows():
            print(f"  {row['tenor']:>6s}  {row['bid']:>8.2f}  {row['offer']:>8.2f}")
        return 0

    elif args.rates_command == "curve":
        df = get_pkrv_curve(con, date=args.date)
        if df.empty:
            print("No yield curve data. Run 'psxsync rates sync' first.")
            return 0
        curve_date = df.iloc[0]["date"]
        print(f"\n{'='*50}")
        print(f"  Yield Curve ({curve_date})")
        print(f"{'='*50}")
        for _, row in df.iterrows():
            months = row["tenor_months"]
            if months < 12:
                label = f"{months}M"
            else:
                label = f"{months // 12}Y"
            print(
                f"  {label:>5s}  {row['yield_pct']:>8.4f}%  ({row['source']})"
            )
        return 0

    elif args.rates_command == "summary":
        scraper = SBPRatesScraper()
        summary = scraper.get_summary(con)
        print(f"\n{'='*50}")
        print("  Rates Data Summary")
        print(f"{'='*50}")
        if summary["latest_konia"]:
            print(
                f"  KONIA: {summary['latest_konia']['rate_pct']}% "
                f"({summary['latest_konia']['date']})"
            )
        print(f"  Yield curve points: {summary['curve_points']}")
        print(f"  KONIA history days: {summary['konia_days']}")
        return 0

    return 0


def handle_fx_rates(args: argparse.Namespace) -> int:
    """Handle fx-rates subcommands (SBP interbank, open market, kerb)."""
    from .db.repositories.fx_extended import (
        get_all_fx_latest,
        get_fx_spread,
        init_fx_extended_schema,
    )
    from .sources.forex_scraper import ForexPKScraper
    from .sources.sbp_fx import SBPFXScraper

    con = connect(args.db)
    init_schema(con)
    init_fx_extended_schema(con)

    if args.fxe_command == "sbp-sync":
        print("Scraping SBP interbank USD/PKR rates...")
        scraper = SBPFXScraper()
        result = scraper.sync_interbank(con)
        print(f"Done: {result['ok']} OK, {result['failed']} failed (total {result['total']})")
        return 0

    elif args.fxe_command == "kerb-sync":
        print("Scraping kerb rates from forex.pk...")
        scraper = ForexPKScraper()
        result = scraper.sync_kerb(con)
        print(f"Done: {result['ok']} OK, {result['failed']} failed (total {result['total']})")
        return 0

    elif args.fxe_command == "sync-all":
        print("Syncing SBP interbank + kerb rates...")
        sbp = SBPFXScraper()
        r1 = sbp.sync_interbank(con)
        print(f"  SBP interbank: {r1['ok']} OK")

        kerb = ForexPKScraper()
        r2 = kerb.sync_kerb(con)
        print(f"  Kerb (forex.pk): {r2['ok']} OK")
        print(f"Done: {r1['ok'] + r2['ok']} total rates synced")
        return 0

    elif args.fxe_command == "latest":
        if args.source in ("interbank", "all"):
            df = get_all_fx_latest(con, source="interbank")
            if not df.empty:
                print(f"\n  SBP Interbank Rates")
                print(f"  {'Currency':>8s}  {'Buying':>10s}  {'Selling':>10s}  {'Date':>12s}")
                for _, row in df.iterrows():
                    print(
                        f"  {row['currency']:>8s}  {row['buying']:>10.2f}  "
                        f"{row['selling']:>10.2f}  {row['date']:>12s}"
                    )
            else:
                print("  No interbank data. Run 'psxsync fx-rates sbp-sync' first.")

        if args.source in ("kerb", "all"):
            df = get_all_fx_latest(con, source="kerb")
            if not df.empty:
                print(f"\n  Kerb / Open Market Rates (forex.pk)")
                print(f"  {'Currency':>8s}  {'Buying':>10s}  {'Selling':>10s}  {'Date':>12s}")
                for _, row in df.iterrows():
                    print(
                        f"  {row['currency']:>8s}  {row['buying']:>10.2f}  "
                        f"{row['selling']:>10.2f}  {row['date']:>12s}"
                    )
            else:
                print("  No kerb data. Run 'psxsync fx-rates kerb-sync' first.")
        return 0

    elif args.fxe_command == "spread":
        spread = get_fx_spread(con, args.currency)
        print(f"\n  FX Spread for {spread['currency']}")
        for source_name in ("interbank", "open_market", "kerb"):
            rate = spread[source_name]
            if rate:
                print(
                    f"  {source_name:>12s}:  Buy {rate['buying']:.2f}  "
                    f"Sell {rate['selling']:.2f}  ({rate['date']})"
                )
            else:
                print(f"  {source_name:>12s}:  No data")
        return 0

    elif args.fxe_command == "summary":
        ib = get_all_fx_latest(con, source="interbank")
        kerb = get_all_fx_latest(con, source="kerb")
        print(f"\n{'='*50}")
        print("  FX Rates Summary")
        print(f"{'='*50}")
        print(f"  Interbank currencies: {len(ib)}")
        print(f"  Kerb currencies:      {len(kerb)}")
        if not ib.empty:
            print(f"  Latest interbank:     {ib.iloc[0]['date']}")
        if not kerb.empty:
            print(f"  Latest kerb:          {kerb.iloc[0]['date']}")
        return 0

    return 0


def handle_dividends(args: argparse.Namespace) -> int:
    """Handle dividends subcommands."""
    from .db.repositories.dividends import (
        get_dividend_history,
        get_dividend_yield,
        get_highest_dividend_stocks,
        get_upcoming_dividends,
    )

    con = connect(args.db)
    init_schema(con)

    if args.div_command == "show":
        df = get_dividend_history(con, args.symbol, years=args.years)
        if df.empty:
            print(f"No dividend data for {args.symbol.upper()}.")
            return 0
        print(f"\n  Dividend History — {args.symbol.upper()} ({len(df)} payouts)")
        print(f"  {'Ex-Date':>12s}  {'Amount':>10s}  {'Fiscal Year'}")
        for _, row in df.iterrows():
            fy = row.get("fiscal_year") or ""
            print(f"  {row['ex_date']:>12s}  {row['amount']:>10.1f}  {fy}")
        return 0

    elif args.div_command == "yield":
        yld = get_dividend_yield(con, args.symbol, years=args.years)
        if yld is None:
            print(f"No dividend yield data for {args.symbol.upper()}.")
            return 0
        print(
            f"\n  {args.symbol.upper()} trailing {args.years}Y dividend yield: {yld:.2f}%"
        )
        return 0

    elif args.div_command == "top":
        df = get_highest_dividend_stocks(con, n=args.n, years=args.years)
        if df.empty:
            print("No dividend data available.")
            return 0
        print(f"\n  Top {len(df)} Dividend Yield Stocks ({args.years}Y trailing)")
        print(f"  {'#':>3s}  {'Symbol':>8s}  {'Yield%':>8s}  {'DPS':>10s}  {'Price':>10s}  {'Payouts':>8s}")
        for i, (_, row) in enumerate(df.iterrows(), 1):
            print(
                f"  {i:>3d}  {row['symbol']:>8s}  {row['yield_pct']:>8.2f}  "
                f"{row['total_dps']:>10.1f}  {row['latest_price']:>10.2f}  "
                f"{int(row['num_payouts']):>8d}"
            )
        return 0

    elif args.div_command == "upcoming":
        df = get_upcoming_dividends(con)
        if df.empty:
            print("No upcoming dividends in the next 30 days.")
            return 0
        print(f"\n  Upcoming Ex-Dividend Dates ({len(df)} records)")
        print(f"  {'Ex-Date':>12s}  {'Symbol':>8s}  {'Type':>6s}  {'Amount':>10s}")
        for _, row in df.iterrows():
            amt = f"{row['amount']:.1f}" if row["amount"] else "N/A"
            print(
                f"  {row['ex_date']:>12s}  {row['symbol']:>8s}  "
                f"{row['payout_type']:>6s}  {amt:>10s}"
            )
        return 0

    return 0


def handle_ipo(args: argparse.Namespace) -> int:
    """Handle ipo subcommands."""
    from .db.repositories.ipo import (
        get_ipo_by_symbol,
        get_ipo_listings,
        get_recent_listings,
        get_upcoming_ipos,
        init_ipo_schema,
    )
    from .sources.ipo_scraper import IPOScraper

    con = connect(args.db)
    init_schema(con)
    init_ipo_schema(con)

    if args.ipo_command == "sync":
        print("Scraping IPO listings from PSX DPS...")
        scraper = IPOScraper()
        result = scraper.sync_listings(con)
        print(f"Done: {result['ok']} OK, {result['failed']} failed (total {result['total']})")
        if result["total"] == 0:
            print("  Note: DPS listings endpoint may be unavailable (returns 500).")
        return 0

    elif args.ipo_command == "list":
        df = get_ipo_listings(con, status=args.status, board=args.board)
        if df.empty:
            print("No IPO records found. Run 'psxsync ipo sync' first.")
            return 0
        print(f"\n  IPO Listings ({len(df)} records)")
        print(f"  {'Symbol':>8s}  {'Board':>5s}  {'Status':>10s}  {'Listing Date':>12s}  {'Company'}")
        for _, row in df.iterrows():
            print(
                f"  {row['symbol']:>8s}  {(row.get('board') or 'N/A'):>5s}  "
                f"{(row.get('status') or 'N/A'):>10s}  "
                f"{(row.get('listing_date') or 'N/A'):>12s}  "
                f"{row.get('company_name') or ''}"
            )
        return 0

    elif args.ipo_command == "upcoming":
        df = get_upcoming_ipos(con)
        if df.empty:
            print("No upcoming IPOs found.")
            return 0
        print(f"\n  Upcoming IPOs ({len(df)} records)")
        for _, row in df.iterrows():
            print(f"  {row['symbol']:>8s}  {row.get('company_name') or ''}")
            if row.get("subscription_open"):
                print(f"           Subscription: {row['subscription_open']} - {row.get('subscription_close', 'TBD')}")
            if row.get("offer_price"):
                print(f"           Offer price: PKR {row['offer_price']:.2f}")
        return 0

    elif args.ipo_command == "recent":
        df = get_recent_listings(con)
        if df.empty:
            print("No recent listings found.")
            return 0
        print(f"\n  Recent Listings ({len(df)} records)")
        print(f"  {'Symbol':>8s}  {'Board':>5s}  {'Listing Date':>12s}  {'Company'}")
        for _, row in df.iterrows():
            print(
                f"  {row['symbol']:>8s}  {(row.get('board') or 'N/A'):>5s}  "
                f"{(row.get('listing_date') or 'N/A'):>12s}  "
                f"{row.get('company_name') or ''}"
            )
        return 0

    elif args.ipo_command == "show":
        ipo = get_ipo_by_symbol(con, args.symbol)
        if not ipo:
            print(f"No IPO record for {args.symbol.upper()}.")
            return 0
        print(f"\n  IPO Details — {ipo['symbol']}")
        for key in ["company_name", "board", "status", "offer_price",
                     "shares_offered", "subscription_open", "subscription_close",
                     "listing_date", "ipo_type"]:
            val = ipo.get(key)
            if val:
                print(f"  {key:>22s}: {val}")
        return 0

    return 0


def handle_vps(args: argparse.Namespace) -> int:
    """Handle vps subcommands."""
    from .db.repositories.vps import (
        compare_vps_performance,
        get_vps_funds,
        get_vps_nav_history,
        get_vps_summary,
    )

    con = connect(args.db)
    init_schema(con)

    if args.vps_command == "list":
        df = get_vps_funds(con)
        if df.empty:
            print("No VPS funds found.")
            return 0
        print(f"\n  VPS Pension Funds ({len(df)} funds)")
        print(f"  {'Fund ID':>25s}  {'Fund Name'}")
        for _, row in df.iterrows():
            shariah = " [Shariah]" if row.get("is_shariah") else ""
            print(f"  {row['fund_id']:>25s}  {row['fund_name']}{shariah}")
        return 0

    elif args.vps_command == "nav":
        df = get_vps_nav_history(con, args.fund_id)
        if df.empty:
            print(f"No NAV data for {args.fund_id}.")
            return 0
        print(f"\n  NAV History — {args.fund_id} ({len(df)} records)")
        print(f"  {'Date':>12s}  {'NAV':>10s}  {'Change%':>8s}")
        for _, row in df.head(20).iterrows():
            chg = f"{row['nav_change_pct']:.2f}" if row.get("nav_change_pct") else "N/A"
            print(f"  {row['date']:>12s}  {row['nav']:>10.4f}  {chg:>8s}")
        if len(df) > 20:
            print(f"  ... showing latest 20 of {len(df)} records")
        return 0

    elif args.vps_command == "performance":
        df = compare_vps_performance(con, days=args.days)
        if df.empty:
            print("No VPS performance data available.")
            return 0
        print(f"\n  VPS Performance ({args.days}D)")
        print(f"  {'Fund Name':>40s}  {'Return%':>8s}  {'Latest NAV':>10s}")
        for _, row in df.iterrows():
            ret = f"{row['return_pct']:.2f}" if row.get("return_pct") is not None else "N/A"
            print(
                f"  {row['fund_name'][:40]:>40s}  {ret:>8s}  {row['latest_nav']:>10.4f}"
            )
        return 0

    elif args.vps_command == "summary":
        summary = get_vps_summary(con)
        print(f"\n{'='*50}")
        print("  VPS Pension Fund Summary")
        print(f"{'='*50}")
        print(f"  Total VPS funds:  {summary['total_funds']}")
        print(f"  Total NAV records: {summary['total_nav_records']}")
        if summary["earliest_date"]:
            print(f"  Date range:       {summary['earliest_date']} to {summary['latest_date']}")
        return 0

    return 0


def handle_sync_all(args: argparse.Namespace) -> int:
    """Run all data scrapers in sequence."""
    import time

    con = connect(args.db)
    init_schema(con)

    steps: list[tuple[str, callable]] = []

    def _add_step(name: str, fn: callable):
        steps.append((name, fn))

    # 1. ETF
    def _sync_etf():
        from .sources.etf_scraper import ETFScraper
        s = ETFScraper()
        return s.sync_etf_data(con)
    _add_step("ETF NAV + metadata", _sync_etf)

    # 2. Treasury (T-Bills + PIBs)
    def _sync_treasury():
        from .sources.sbp_treasury import SBPTreasuryScraper
        s = SBPTreasuryScraper()
        return s.sync_treasury(con)
    _add_step("Treasury auctions (T-Bill + PIB)", _sync_treasury)

    # 3. GIS auctions
    def _sync_gis():
        from .sources.sbp_gsp import GSPScraper
        s = GSPScraper()
        return s.sync_gis(con)
    _add_step("GIS auctions", _sync_gis)

    # 4. Rates (KONIA + KIBOR + yield curve)
    def _sync_rates():
        from .sources.sbp_rates import SBPRatesScraper
        s = SBPRatesScraper()
        return s.sync_rates(con)
    _add_step("KONIA + KIBOR + yield curve", _sync_rates)

    # 5. SBP FX interbank
    def _sync_sbp_fx():
        from .sources.sbp_fx import SBPFXScraper
        s = SBPFXScraper()
        return s.sync_interbank(con)
    _add_step("SBP FX interbank", _sync_sbp_fx)

    # 6. Kerb FX
    def _sync_kerb():
        from .sources.forex_scraper import ForexPKScraper
        s = ForexPKScraper()
        return s.sync_kerb(con)
    _add_step("Kerb FX (forex.pk)", _sync_kerb)

    # 7. FX microservice (interbank + KIBOR overlay)
    def _sync_fx_micro():
        from .sources.fx_sync import sync_fx_rates
        return sync_fx_rates(con)
    _add_step("FX microservice (interbank + KIBOR)", _sync_fx_micro)

    # 8. IPO listings
    def _sync_ipo():
        from .sources.ipo_scraper import IPOScraper
        s = IPOScraper()
        return s.sync_listings(con)
    _add_step("IPO listings", _sync_ipo)

    # 8. SIR PDF backfill (T-Bills, PIBs, KIBOR, GIS history)
    def _sync_sir():
        from .sources.sbp_sir import SBPSirScraper
        s = SBPSirScraper()
        return s.sync_sir(con)
    _add_step("SIR PDF (treasury history)", _sync_sir)

    print(f"\n{'='*60}")
    print(f"  Unified Sync — {len(steps)} data sources")
    print(f"{'='*60}\n")

    ok_count = 0
    fail_count = 0
    t0 = time.time()

    for i, (name, fn) in enumerate(steps, 1):
        print(f"  [{i}/{len(steps)}] {name}...", end=" ", flush=True)
        try:
            result = fn()
            print(f"OK {result}")
            ok_count += 1
        except Exception as e:
            print(f"FAILED ({e})")
            fail_count += 1

    elapsed = time.time() - t0
    print(f"\n{'='*60}")
    print(f"  Done: {ok_count}/{len(steps)} succeeded, {fail_count} failed ({elapsed:.1f}s)")
    print(f"{'='*60}")

    return 0 if fail_count == 0 else 1


def handle_status(args: argparse.Namespace) -> int:
    """Show data freshness dashboard."""
    import os
    from datetime import datetime, timedelta

    con = connect(args.db)
    init_schema(con)

    today = datetime.now().strftime("%Y-%m-%d")

    def _get_stats(table: str, date_col: str = "date") -> tuple[int, str | None]:
        """Get row count and latest date for a table."""
        try:
            count = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            if count == 0:
                return 0, None
            row = con.execute(
                f'SELECT MAX("{date_col}") FROM "{table}"'
            ).fetchone()
            return count, row[0] if row else None
        except Exception:
            return 0, None

    def _freshness(latest: str | None) -> str:
        if not latest:
            return "No data"
        try:
            dt = datetime.strptime(latest[:10], "%Y-%m-%d")
            days = (datetime.now() - dt).days
            if days <= 1:
                return "Fresh"
            elif days <= 7:
                return f"{days}d old"
            else:
                return f"{days}d stale"
        except Exception:
            return "?"

    domains = [
        ("EOD OHLCV", "eod_ohlcv", "date"),
        ("Intraday Bars", "intraday_bars", "ts"),
        ("ETF NAV", "etf_nav", "date"),
        ("ETF Master", "etf_master", "updated_at"),
        ("T-Bill Auctions", "tbill_auctions", "auction_date"),
        ("PIB Auctions", "pib_auctions", "auction_date"),
        ("GIS Auctions", "gis_auctions", "auction_date"),
        ("PKRV Yield Curve", "pkrv_daily", "date"),
        ("KONIA", "konia_daily", "date"),
        ("KIBOR", "kibor_daily", "date"),
        ("SBP FX Interbank", "sbp_fx_interbank", "date"),
        ("Kerb FX", "forex_kerb", "date"),
        ("SBP Policy Rate", "sbp_policy_rates", "rate_date"),
        ("Mutual Funds", "mutual_funds", "updated_at"),
        ("Mutual Fund NAV", "mutual_fund_nav", "date"),
        ("Bonds", "bonds_master", "updated_at"),
        ("Sukuk", "sukuk_master", "created_at"),
        ("IPO Calendar", "ipo_listings", "updated_at"),
        ("Company Profiles", "company_profile", "updated_at"),
        ("Corp. Announcements", "corporate_announcements", "announcement_date"),
        ("Symbols", "symbols", "updated_at"),
        ("Dividends/Payouts", "company_payouts", "ex_date"),
    ]

    print(f"\n  {'Data Domain':<25s}  {'Rows':>10s}  {'Latest':>12s}  {'Freshness'}")
    print(f"  {'-'*25}  {'-'*10}  {'-'*12}  {'-'*12}")

    for label, table, date_col in domains:
        count, latest = _get_stats(table, date_col)
        fresh = _freshness(latest)
        latest_str = latest[:10] if latest else "N/A"
        print(f"  {label:<25s}  {count:>10,d}  {latest_str:>12s}  {fresh}")

    # DB stats
    from .config import get_db_path as _get_db_path
    db_path = str(args.db) if args.db else str(_get_db_path(None))
    try:
        db_size = os.path.getsize(db_path) / (1024 * 1024)
        wal_path = db_path + "-wal"
        wal_size = os.path.getsize(wal_path) / (1024 * 1024) if os.path.exists(wal_path) else 0

        table_count = con.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
        ).fetchone()[0]
        index_count = con.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index'"
        ).fetchone()[0]

        print(f"\n  DB: {db_size:.0f} MB | WAL: {wal_size:.0f} MB | "
              f"Tables: {table_count} | Indexes: {index_count}")
    except Exception:
        pass

    return 0


def handle_backfill_rates(args: argparse.Namespace) -> int:
    """Backfill historical rates from SBP PDFs."""
    import time

    con = connect(args.db)
    init_schema(con)

    source = args.source
    total_ok = 0
    total_fail = 0
    t0 = time.time()

    # ── SIR PDF (T-Bills, PIBs, KIBOR, GIS) ──
    if source in ("sir", "all"):
        print("\n  [SIR] Downloading Structure of Interest Rates PDF...", flush=True)
        try:
            from .sources.sbp_sir import SBPSirScraper
            scraper = SBPSirScraper()
            counts = scraper.sync_sir(con)
            print(f"  [SIR] OK: T-Bills={counts['tbills']}, PIBs={counts['pibs']}, "
                  f"KIBOR={counts['kibor']}, GIS={counts['gis']}, failed={counts['failed']}")
            total_ok += counts["tbills"] + counts["pibs"] + counts["kibor"] + counts["gis"]
            total_fail += counts["failed"]
        except Exception as e:
            print(f"  [SIR] FAILED: {e}")
            total_fail += 1

    # ── PIB Archive PDF (25 years) ──
    if source in ("pib", "all"):
        print("\n  [PIB] Downloading PIB auction archive PDF (42 pages)...", flush=True)
        try:
            from .sources.sbp_pib_archive import SBPPibArchiveScraper
            scraper = SBPPibArchiveScraper()
            counts = scraper.sync_pib_archive(con)
            print(f"  [PIB] OK: {counts['inserted']}/{counts['total']} inserted, "
                  f"failed={counts['failed']}")
            total_ok += counts["inserted"]
            total_fail += counts["failed"]
        except Exception as e:
            print(f"  [PIB] FAILED: {e}")
            total_fail += 1

    # ── KIBOR Daily PDFs (2008-present) ──
    if source in ("kibor", "all"):
        start_year = getattr(args, "start_year", 2008)
        print(f"\n  [KIBOR] Backfilling daily KIBOR PDFs from {start_year}...")
        print(f"          (incremental — skips dates already in DB)")
        print(f"          This may take a while. Press Ctrl+C to stop.", flush=True)
        try:
            from .sources.sbp_kibor_history import SBPKiborHistoryScraper
            scraper = SBPKiborHistoryScraper()
            counts = scraper.sync_kibor_history(
                con, start_year=start_year, incremental=True,
            )
            print(f"  [KIBOR] OK: {counts['dates_processed']} dates, "
                  f"{counts['records_inserted']} records, "
                  f"skipped={counts['skipped']}, failed={counts['failed']}")
            total_ok += counts["records_inserted"]
            total_fail += counts["failed"]
        except KeyboardInterrupt:
            print("\n  [KIBOR] Interrupted by user (progress saved).")
        except Exception as e:
            print(f"  [KIBOR] FAILED: {e}")
            total_fail += 1

    elapsed = time.time() - t0
    print(f"\n  {'='*50}")
    print(f"  Backfill done: {total_ok} records, {total_fail} failures ({elapsed:.1f}s)")
    print(f"  {'='*50}")

    con.close()
    return 0 if total_fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
