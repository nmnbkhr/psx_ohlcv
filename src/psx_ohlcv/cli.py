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
from .sources.company_page import (
    listen_quotes,
    refresh_company_profile,
    take_quote_snapshot,
)
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
from .sync import SyncSummary, sync_all, sync_intraday

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
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        return 130
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    return 0


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

    # Run sync
    mode_str = "incremental" if args.incremental else "full"
    print(f"Starting EOD sync ({mode_str} mode)...")

    summary = sync_all(
        db_path=args.db,
        refresh_symbols=args.refresh_symbols,
        limit_symbols=args.limit_symbols if args.sync_all else None,
        symbols_list=symbols_list,
        config=config,
    )

    # Print summary
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
    con.close()

    print(f"\nMarket Summary: {date_str}")
    print("=" * 50)
    print(f"  Status:         {result['status']}")
    print(f"  Row Count:      {result['row_count']}")
    if result["csv_path"]:
        print(f"  CSV saved to:   {result['csv_path']}")
    if result["message"]:
        print(f"  Message:        {result['message']}")

    if result["status"] in ("ok", "skipped"):
        return EXIT_SUCCESS
    elif result["status"] == "missing":
        return EXIT_SUCCESS  # Not an error, just no data
    else:
        return EXIT_ERROR


def handle_market_summary_range(args: argparse.Namespace) -> int:
    """Handle market-summary range command."""
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

    print("\nMarket Summary Range Download")
    print("=" * 50)
    print(f"  Date range:     {summary['start']} to {summary['end']}")
    print(f"  Dates checked:  {summary['total']}")
    print(f"  Downloaded:     {summary['ok']}")
    print(f"  Skipped:        {summary['skipped']} (already exist)")
    print(f"  Missing:        {summary['missing']} (holidays/weekends)")
    print(f"  Failed:         {len(summary['failed'])}")

    if summary["failed"]:
        print("\nFailed:")
        for err in summary["failed"][:10]:
            print(f"  {err['date']}: {err['message']}")
        if len(summary["failed"]) > 10:
            print(f"  ... and {len(summary['failed']) - 10} more errors")

    return EXIT_SUCCESS


def handle_market_summary_last(args: argparse.Namespace) -> int:
    """Handle market-summary last command."""
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

    print("\nMarket Summary Download")
    print("=" * 50)
    print(f"  Date range:     {summary['start']} to {summary['end']}")
    print(f"  Dates checked:  {summary['total']}")
    print(f"  Downloaded:     {summary['ok']}")
    print(f"  Skipped:        {summary['skipped']} (already exist)")
    print(f"  Missing:        {summary['missing']} (holidays/weekends)")
    print(f"  Failed:         {len(summary['failed'])}")

    if summary["failed"]:
        print("\nFailed:")
        for err in summary["failed"][:10]:
            print(f"  {err['date']}: {err['message']}")
        if len(summary["failed"]) > 10:
            print(f"  ... and {len(summary['failed']) - 10} more errors")

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


if __name__ == "__main__":
    sys.exit(main())
