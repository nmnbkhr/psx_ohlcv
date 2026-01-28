"""Data extraction layer for LLM prompts.

This module extracts and formats data from the SQLite database
into compact, token-efficient payloads for LLM consumption.

Key features:
- Automatic downsampling for large datasets
- Bounded payload sizes (max 2000 rows before downsampling)
- Token-efficient formatting
- Explicit data provenance tracking

IMPORTANT: The daily high/low values are derived from max/min(open, close),
NOT true intraday extremes. This caveat is included in all data payloads.
"""

import json
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# Maximum rows before downsampling
MAX_OHLCV_ROWS = 2000
TARGET_OHLCV_ROWS = 500
MAX_INTRADAY_POINTS = 2000
TARGET_INTRADAY_POINTS = 300


@dataclass
class DataProvenance:
    """Tracks data source and freshness."""

    tables_used: list[str] = field(default_factory=list)
    row_count: int = 0
    date_range: tuple[str, str] = ("", "")
    last_updated: str = ""
    was_downsampled: bool = False
    original_row_count: int = 0

    def to_dict(self) -> dict:
        return {
            "tables_used": self.tables_used,
            "row_count": self.row_count,
            "date_range": self.date_range,
            "last_updated": self.last_updated,
            "was_downsampled": self.was_downsampled,
            "original_row_count": self.original_row_count,
        }


@dataclass
class CompanyData:
    """Container for company-related data."""

    symbol: str
    company_name: str = ""
    sector: str = ""
    profile: dict[str, Any] = field(default_factory=dict)
    quote: dict[str, Any] = field(default_factory=dict)
    ohlcv: pd.DataFrame = field(default_factory=pd.DataFrame)
    financials: dict[str, Any] = field(default_factory=dict)
    payouts: list[dict[str, Any]] = field(default_factory=list)  # Dividend history
    financial_announcements: list[dict[str, Any]] = field(default_factory=list)  # Full financial results
    intraday_summary: dict[str, Any] = field(default_factory=dict)  # Recent intraday trading summary
    corporate_announcements: list[dict[str, Any]] = field(default_factory=list)  # Recent company announcements
    corporate_events: list[dict[str, Any]] = field(default_factory=list)  # Upcoming AGM/EOGM events
    provenance: DataProvenance = field(default_factory=DataProvenance)


@dataclass
class IntradayData:
    """Container for intraday data."""

    symbol: str
    trading_date: str = ""
    bars: pd.DataFrame = field(default_factory=pd.DataFrame)
    daily_context: dict[str, Any] = field(default_factory=dict)
    vwap: float | None = None
    provenance: DataProvenance = field(default_factory=DataProvenance)


@dataclass
class MarketData:
    """Container for market-wide data."""

    market_date: str = ""
    index_data: dict[str, Any] = field(default_factory=dict)
    gainers: pd.DataFrame = field(default_factory=pd.DataFrame)
    losers: pd.DataFrame = field(default_factory=pd.DataFrame)
    volume_leaders: pd.DataFrame = field(default_factory=pd.DataFrame)
    sector_performance: pd.DataFrame = field(default_factory=pd.DataFrame)
    breadth: dict[str, int] = field(default_factory=dict)
    provenance: DataProvenance = field(default_factory=DataProvenance)


class DataLoader:
    """Loads and formats data from SQLite for LLM consumption.

    This class provides methods to extract various data types from
    the database in a format optimized for LLM prompts.

    Example:
        >>> loader = DataLoader(con)
        >>> company_data = loader.load_company_data("OGDC", ohlcv_days=30)
        >>> print(company_data.quote)
    """

    def __init__(self, con: sqlite3.Connection):
        """Initialize the data loader.

        Args:
            con: SQLite connection with row_factory set.
        """
        self.con = con
        # Ensure row_factory for dict-like access
        self.con.row_factory = sqlite3.Row

    def load_company_data(
        self,
        symbol: str,
        ohlcv_days: int = 30,
        include_financials: bool = True,
    ) -> CompanyData:
        """Load comprehensive company data.

        Args:
            symbol: Stock symbol.
            ohlcv_days: Number of trading days of OHLCV history.
            include_financials: Whether to include financial data.

        Returns:
            CompanyData with all available information.
        """
        symbol = symbol.upper().strip()
        data = CompanyData(symbol=symbol)
        data.provenance.tables_used = []

        # Load company profile and snapshot
        data.company_name, data.sector, data.profile = self._load_company_profile(symbol)
        if data.profile:
            data.provenance.tables_used.append("company_snapshots")

        # Load latest quote
        data.quote = self._load_latest_quote(symbol)
        if data.quote:
            data.provenance.tables_used.append("trading_sessions")

        # Load OHLCV history
        data.ohlcv, was_downsampled, original_count = self._load_ohlcv(
            symbol, days=ohlcv_days
        )
        if not data.ohlcv.empty:
            data.provenance.tables_used.append("eod_ohlcv")
            data.provenance.row_count = len(data.ohlcv)
            data.provenance.was_downsampled = was_downsampled
            data.provenance.original_row_count = original_count

            # Set date range
            data.provenance.date_range = (
                data.ohlcv["date"].min(),
                data.ohlcv["date"].max(),
            )

        # Load financials if requested
        if include_financials:
            data.financials = self._load_financials(symbol)
            if data.financials:
                data.provenance.tables_used.append("company_snapshots.financials_data")

        # Load dividend/payout history (last 5 payouts)
        data.payouts = self._load_payouts(symbol, limit=5)
        if data.payouts:
            data.provenance.tables_used.append("company_payouts")

        # Load full financial announcements (last 5 results)
        data.financial_announcements = self._load_financial_announcements(symbol, limit=5)
        if data.financial_announcements:
            data.provenance.tables_used.append("financial_announcements")

        # Load intraday trading summary (recent intraday activity)
        data.intraday_summary = self._load_intraday_summary(symbol)
        if data.intraday_summary:
            data.provenance.tables_used.append("intraday_bars")

        # Load recent corporate announcements (last 10)
        data.corporate_announcements = self._load_corporate_announcements(symbol, limit=10)
        if data.corporate_announcements:
            data.provenance.tables_used.append("company_announcements")

        # Load upcoming corporate events (AGM/EOGM)
        data.corporate_events = self._load_corporate_events(symbol, limit=5)
        if data.corporate_events:
            data.provenance.tables_used.append("corporate_events")

        data.provenance.last_updated = datetime.now().isoformat()

        return data

    def load_intraday_data(
        self,
        symbol: str,
        trading_date: str | None = None,
        max_points: int = MAX_INTRADAY_POINTS,
    ) -> IntradayData:
        """Load intraday bar data.

        Args:
            symbol: Stock symbol.
            trading_date: Specific date (YYYY-MM-DD) or None for latest.
            max_points: Maximum points before downsampling.

        Returns:
            IntradayData with bars and context.
        """
        symbol = symbol.upper().strip()
        data = IntradayData(symbol=symbol)
        data.provenance.tables_used = []

        # Determine trading date
        if trading_date is None:
            trading_date = self._get_latest_intraday_date(symbol)

        data.trading_date = trading_date or ""

        if not trading_date:
            return data

        # Load intraday bars
        data.bars, was_downsampled, original_count = self._load_intraday_bars(
            symbol, trading_date, max_points
        )

        if not data.bars.empty:
            data.provenance.tables_used.append("intraday_bars")
            data.provenance.row_count = len(data.bars)
            data.provenance.was_downsampled = was_downsampled
            data.provenance.original_row_count = original_count

            # Calculate VWAP if volume available
            data.vwap = self._calculate_vwap(data.bars)

            # Set time range
            data.provenance.date_range = (
                data.bars["timestamp"].min(),
                data.bars["timestamp"].max(),
            )

        # Load daily context
        data.daily_context = self._load_daily_context(symbol, trading_date)
        if data.daily_context:
            data.provenance.tables_used.append("trading_sessions")

        data.provenance.last_updated = datetime.now().isoformat()

        return data

    def load_market_data(
        self,
        market_date: str | None = None,
        top_n: int = 10,
    ) -> MarketData:
        """Load market-wide summary data.

        Args:
            market_date: Specific date or None for latest.
            top_n: Number of top gainers/losers/volume leaders.

        Returns:
            MarketData with market overview.
        """
        data = MarketData()
        data.provenance.tables_used = []

        # Determine market date
        if market_date is None:
            market_date = self._get_latest_market_date()

        data.market_date = market_date or ""

        if not market_date:
            return data

        # Load index data (KSE-100)
        data.index_data = self._load_index_data(market_date)
        if data.index_data:
            data.provenance.tables_used.append("psx_indices")

        # Load gainers/losers from trading_sessions
        data.gainers, data.losers, data.breadth = self._load_gainers_losers(
            market_date, top_n
        )
        if not data.gainers.empty or not data.losers.empty:
            data.provenance.tables_used.append("trading_sessions")

        # Load volume leaders
        data.volume_leaders = self._load_volume_leaders(market_date, top_n)

        # Load sector performance
        data.sector_performance = self._load_sector_performance(market_date)
        if not data.sector_performance.empty:
            data.provenance.tables_used.append("sectors")

        data.provenance.row_count = (
            len(data.gainers) + len(data.losers) + len(data.volume_leaders)
        )
        data.provenance.date_range = (market_date, market_date)
        data.provenance.last_updated = datetime.now().isoformat()

        return data

    def _load_company_profile(self, symbol: str) -> tuple[str, str, dict]:
        """Load company profile from snapshots."""
        try:
            cur = self.con.execute(
                """
                SELECT company_name, sector_name, profile_data, equity_data, quote_data
                FROM company_snapshots
                WHERE symbol = ?
                ORDER BY snapshot_date DESC
                LIMIT 1
                """,
                (symbol,),
            )
            row = cur.fetchone()
            if not row:
                return "", "", {}

            profile = {}

            # Parse profile JSON
            if row["profile_data"]:
                try:
                    profile["profile"] = json.loads(row["profile_data"])
                except json.JSONDecodeError:
                    pass

            # Parse equity JSON
            if row["equity_data"]:
                try:
                    profile["equity"] = json.loads(row["equity_data"])
                except json.JSONDecodeError:
                    pass

            return (
                row["company_name"] or "",
                row["sector_name"] or "",
                profile,
            )

        except sqlite3.Error as e:
            logger.warning("Error loading company profile: %s", e)
            return "", "", {}

    def _load_latest_quote(self, symbol: str) -> dict:
        """Load latest quote from trading_sessions and company_snapshots.

        Combines price data with fundamentals like P/E ratio, circuit limits, etc.
        """
        quote = {}

        try:
            # Try trading_sessions first (more current price data)
            cur = self.con.execute(
                """
                SELECT
                    session_date, open, high, low, close, volume, turnover,
                    change_value, change_percent, ldcp
                FROM trading_sessions
                WHERE symbol = ? AND market_type = 'REG'
                ORDER BY session_date DESC
                LIMIT 1
                """,
                (symbol,),
            )
            row = cur.fetchone()

            if row:
                quote = {
                    "date": row["session_date"],
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row["volume"],
                    "turnover": row["turnover"],
                    "change_value": row["change_value"],
                    "change_percent": row["change_percent"],
                    "ldcp": row["ldcp"],
                }

            # Always try to get additional data from company_snapshots
            # (P/E ratio, circuit limits, 52-week range, YTD change, etc.)
            cur = self.con.execute(
                """
                SELECT quote_data, trading_data
                FROM company_snapshots
                WHERE symbol = ?
                ORDER BY snapshot_date DESC
                LIMIT 1
                """,
                (symbol,),
            )
            row = cur.fetchone()

            if row:
                if row["quote_data"]:
                    try:
                        snapshot_quote = json.loads(row["quote_data"])
                        # Only update fields not already set from trading_sessions
                        for key, value in snapshot_quote.items():
                            if key not in quote:
                                quote[key] = value
                    except json.JSONDecodeError:
                        pass

                if row["trading_data"]:
                    try:
                        trading = json.loads(row["trading_data"])
                        if "REG" in trading:
                            reg_data = trading["REG"]
                            # Extract key fundamentals even if we have trading_sessions data
                            fundamental_fields = [
                                "pe_ratio_ttm", "circuit_low", "circuit_high",
                                "week_52_low", "week_52_high", "var_percent",
                                "haircut_percent", "year_1_change", "ytd_change",
                            ]
                            for field in fundamental_fields:
                                if field in reg_data and reg_data[field] is not None:
                                    quote[field] = reg_data[field]

                            # Also fill in missing OHLCV data if needed
                            for key, value in reg_data.items():
                                if key not in quote and value is not None:
                                    quote[key] = value
                    except json.JSONDecodeError:
                        pass

            return quote

        except sqlite3.Error as e:
            logger.warning("Error loading quote: %s", e)
            return quote

    def _load_ohlcv(
        self,
        symbol: str,
        days: int = 30,
    ) -> tuple[pd.DataFrame, bool, int]:
        """Load OHLCV history with optional downsampling.

        Returns:
            Tuple of (DataFrame, was_downsampled, original_count)
        """
        try:
            # First, check total available rows
            cur = self.con.execute(
                "SELECT COUNT(*) FROM eod_ohlcv WHERE symbol = ?",
                (symbol,),
            )
            total_rows = cur.fetchone()[0]

            # Fetch data
            query = """
                SELECT date, open, high, low, close, volume
                FROM eod_ohlcv
                WHERE symbol = ?
                ORDER BY date DESC
                LIMIT ?
            """
            df = pd.read_sql_query(query, self.con, params=(symbol, days))

            if df.empty:
                return pd.DataFrame(), False, 0

            original_count = len(df)
            was_downsampled = False

            # Downsample if needed
            if len(df) > MAX_OHLCV_ROWS:
                df = self._downsample_ohlcv(df, TARGET_OHLCV_ROWS)
                was_downsampled = True

            # Sort ascending for display
            df = df.sort_values("date").reset_index(drop=True)

            return df, was_downsampled, original_count

        except (sqlite3.Error, pd.errors.DatabaseError) as e:
            logger.warning("Error loading OHLCV: %s", e)
            return pd.DataFrame(), False, 0

    def _load_intraday_bars(
        self,
        symbol: str,
        trading_date: str,
        max_points: int,
    ) -> tuple[pd.DataFrame, bool, int]:
        """Load intraday bars with optional downsampling."""
        try:
            query = """
                SELECT ts as timestamp, open, high, low, close, volume
                FROM intraday_bars
                WHERE symbol = ?
                AND DATE(ts) = ?
                ORDER BY ts
            """
            df = pd.read_sql_query(query, self.con, params=(symbol, trading_date))

            if df.empty:
                return pd.DataFrame(), False, 0

            original_count = len(df)
            was_downsampled = False

            # Downsample if needed
            if len(df) > max_points:
                df = self._downsample_intraday(df, TARGET_INTRADAY_POINTS)
                was_downsampled = True

            return df, was_downsampled, original_count

        except (sqlite3.Error, pd.errors.DatabaseError) as e:
            logger.warning("Error loading intraday bars: %s", e)
            return pd.DataFrame(), False, 0

    def _load_financials(self, symbol: str) -> dict:
        """Load financial data from company snapshots."""
        try:
            cur = self.con.execute(
                """
                SELECT financials_data, ratios_data
                FROM company_snapshots
                WHERE symbol = ?
                ORDER BY snapshot_date DESC
                LIMIT 1
                """,
                (symbol,),
            )
            row = cur.fetchone()

            if not row:
                return {}

            financials = {}

            if row["financials_data"]:
                try:
                    financials["financials"] = json.loads(row["financials_data"])
                except json.JSONDecodeError:
                    pass

            if row["ratios_data"]:
                try:
                    financials["ratios"] = json.loads(row["ratios_data"])
                except json.JSONDecodeError:
                    pass

            return financials

        except sqlite3.Error as e:
            logger.warning("Error loading financials: %s", e)
            return {}

    def _load_payouts(self, symbol: str, limit: int = 5) -> list[dict]:
        """Load dividend/payout history from company_payouts table.

        Args:
            symbol: Stock symbol.
            limit: Maximum number of payouts to return.

        Returns:
            List of payout dicts with date, amount, type, etc.
        """
        try:
            cur = self.con.execute(
                """
                SELECT
                    ex_date, payout_type, fiscal_year,
                    announcement_date, book_closure_from, book_closure_to,
                    amount
                FROM company_payouts
                WHERE symbol = ?
                ORDER BY ex_date DESC
                LIMIT ?
                """,
                (symbol, limit),
            )

            payouts = []
            for row in cur.fetchall():
                payout = {
                    "ex_date": row["ex_date"],
                    "payout_type": row["payout_type"],
                    "fiscal_year": row["fiscal_year"],
                    "announcement_date": row["announcement_date"],
                    "book_closure": f"{row['book_closure_from']} to {row['book_closure_to']}" if row["book_closure_from"] else None,
                    "amount": row["amount"],
                }
                payouts.append(payout)

            return payouts

        except sqlite3.Error as e:
            logger.warning("Error loading payouts: %s", e)
            return []

    def _load_financial_announcements(self, symbol: str, limit: int = 5) -> list[dict]:
        """Load full financial announcements from financial_announcements table.

        Args:
            symbol: Stock symbol.
            limit: Maximum number of announcements to return.

        Returns:
            List of announcement dicts with EPS, profit, AGM date, etc.
        """
        try:
            cur = self.con.execute(
                """
                SELECT
                    announcement_date, fiscal_period,
                    profit_before_tax, profit_after_tax, eps,
                    dividend_payout, dividend_amount, payout_type,
                    agm_date, book_closure_from, book_closure_to
                FROM financial_announcements
                WHERE symbol = ?
                ORDER BY announcement_date DESC
                LIMIT ?
                """,
                (symbol, limit),
            )

            announcements = []
            for row in cur.fetchall():
                ann = {
                    "announcement_date": row["announcement_date"],
                    "fiscal_period": row["fiscal_period"],
                    "profit_before_tax": row["profit_before_tax"],
                    "profit_after_tax": row["profit_after_tax"],
                    "eps": row["eps"],
                    "dividend_payout": row["dividend_payout"],
                    "dividend_amount": row["dividend_amount"],
                    "payout_type": row["payout_type"],
                    "agm_date": row["agm_date"],
                    "book_closure": f"{row['book_closure_from']} to {row['book_closure_to']}" if row["book_closure_from"] else None,
                }
                announcements.append(ann)

            return announcements

        except sqlite3.Error as e:
            logger.warning("Error loading financial announcements: %s", e)
            return []

    def _load_intraday_summary(self, symbol: str) -> dict[str, Any]:
        """Load intraday trading summary for recent activity.

        Provides a summary of recent intraday trading patterns including:
        - Latest trading date and time range
        - VWAP (Volume Weighted Average Price)
        - Price range (true intraday high/low)
        - Volume distribution summary
        - Number of bars/data points

        Args:
            symbol: Stock symbol.

        Returns:
            Dict with intraday summary or empty dict if no data.
        """
        try:
            # Get the most recent trading date with intraday data
            cur = self.con.execute(
                """
                SELECT DATE(ts) as trading_date, COUNT(*) as bar_count,
                       MIN(ts) as first_bar, MAX(ts) as last_bar,
                       MIN(low) as intraday_low, MAX(high) as intraday_high,
                       SUM(volume) as total_volume,
                       MIN(open) as session_open
                FROM intraday_bars
                WHERE symbol = ?
                GROUP BY DATE(ts)
                ORDER BY trading_date DESC
                LIMIT 1
                """,
                (symbol,),
            )
            row = cur.fetchone()

            if not row:
                return {}

            trading_date = row["trading_date"]

            # Get detailed bars for VWAP calculation
            cur = self.con.execute(
                """
                SELECT open, high, low, close, volume
                FROM intraday_bars
                WHERE symbol = ? AND DATE(ts) = ?
                ORDER BY ts
                """,
                (symbol, trading_date),
            )
            bars = cur.fetchall()

            # Calculate VWAP
            vwap = None
            if bars:
                total_tp_volume = 0.0
                total_volume = 0.0
                for bar in bars:
                    if bar["volume"] and bar["high"] and bar["low"] and bar["close"]:
                        typical_price = (bar["high"] + bar["low"] + bar["close"]) / 3
                        total_tp_volume += typical_price * bar["volume"]
                        total_volume += bar["volume"]
                if total_volume > 0:
                    vwap = round(total_tp_volume / total_volume, 2)

            # Get closing price from last bar
            last_close = bars[-1]["close"] if bars else None

            return {
                "trading_date": trading_date,
                "first_bar": row["first_bar"],
                "last_bar": row["last_bar"],
                "bar_count": row["bar_count"],
                "intraday_high": row["intraday_high"],
                "intraday_low": row["intraday_low"],
                "total_volume": row["total_volume"],
                "vwap": vwap,
                "last_close": last_close,
            }

        except sqlite3.Error as e:
            logger.warning("Error loading intraday summary: %s", e)
            return {}

    def _load_daily_context(self, symbol: str, trading_date: str) -> dict:
        """Load daily context for intraday analysis."""
        try:
            cur = self.con.execute(
                """
                SELECT open, high, low, close, volume, turnover,
                       change_value, change_percent, ldcp
                FROM trading_sessions
                WHERE symbol = ? AND session_date = ? AND market_type = 'REG'
                """,
                (symbol, trading_date),
            )
            row = cur.fetchone()

            if row:
                return dict(row)
            return {}

        except sqlite3.Error as e:
            logger.warning("Error loading daily context: %s", e)
            return {}

    def _load_corporate_announcements(self, symbol: str, limit: int = 10) -> list[dict]:
        """Load recent corporate announcements from company_announcements table.

        Args:
            symbol: Stock symbol.
            limit: Maximum number of announcements to return.

        Returns:
            List of announcement dicts with date, title, category.
        """
        try:
            cur = self.con.execute(
                """
                SELECT
                    announcement_date, announcement_time,
                    title, category
                FROM company_announcements
                WHERE symbol = ?
                ORDER BY announcement_date DESC, announcement_time DESC
                LIMIT ?
                """,
                (symbol, limit),
            )

            announcements = []
            for row in cur.fetchall():
                ann = {
                    "date": row["announcement_date"],
                    "time": row["announcement_time"],
                    "title": row["title"],
                    "category": row["category"],
                }
                announcements.append(ann)

            return announcements

        except sqlite3.Error as e:
            logger.warning("Error loading corporate announcements: %s", e)
            return []

    def _load_corporate_events(self, symbol: str, limit: int = 5) -> list[dict]:
        """Load upcoming corporate events from corporate_events table.

        Args:
            symbol: Stock symbol.
            limit: Maximum number of events to return.

        Returns:
            List of event dicts with date, type, city, etc.
        """
        try:
            cur = self.con.execute(
                """
                SELECT
                    event_type, event_date, event_time,
                    city, period_end
                FROM corporate_events
                WHERE symbol = ?
                AND event_date >= date('now')
                ORDER BY event_date ASC
                LIMIT ?
                """,
                (symbol, limit),
            )

            events = []
            for row in cur.fetchall():
                event = {
                    "type": row["event_type"],
                    "date": row["event_date"],
                    "time": row["event_time"],
                    "city": row["city"],
                    "period_end": row["period_end"],
                }
                events.append(event)

            return events

        except sqlite3.Error as e:
            logger.warning("Error loading corporate events: %s", e)
            return []

    def _load_index_data(self, market_date: str) -> dict:
        """Load all available PSX indices data (KSE100, KSE30, KMI30, ALLSHR).

        If no data for exact date, finds the closest available date.
        """
        result = {}
        try:
            # First try exact date
            cur = self.con.execute(
                """
                SELECT *
                FROM psx_indices
                WHERE index_date = ?
                ORDER BY
                    CASE index_code
                        WHEN 'KSE100' THEN 1
                        WHEN 'KSE30' THEN 2
                        WHEN 'KMI30' THEN 3
                        WHEN 'ALLSHR' THEN 4
                        ELSE 5
                    END
                """,
                (market_date,),
            )
            rows = cur.fetchall()

            # If no exact date, try nearest date (within 7 days)
            if not rows:
                cur = self.con.execute(
                    """
                    SELECT DISTINCT index_date
                    FROM psx_indices
                    WHERE ABS(julianday(index_date) - julianday(?)) <= 7
                    ORDER BY ABS(julianday(index_date) - julianday(?))
                    LIMIT 1
                    """,
                    (market_date, market_date),
                )
                nearest = cur.fetchone()
                if nearest:
                    cur = self.con.execute(
                        """
                        SELECT *
                        FROM psx_indices
                        WHERE index_date = ?
                        ORDER BY
                            CASE index_code
                                WHEN 'KSE100' THEN 1
                                WHEN 'KSE30' THEN 2
                                WHEN 'KMI30' THEN 3
                                WHEN 'ALLSHR' THEN 4
                                ELSE 5
                            END
                        """,
                        (nearest[0],),
                    )
                    rows = cur.fetchall()
                    if rows:
                        result["index_date_note"] = f"Index data from {nearest[0]} (nearest available)"

            if rows:
                # Store all indices in a list
                result["indices"] = [dict(row) for row in rows]
                # Also keep KSE100 as primary for backward compatibility
                for row in rows:
                    if dict(row).get("index_code") == "KSE100":
                        result.update(dict(row))
                        break
                # If no KSE100, use first index as primary
                if "index_code" not in result and rows:
                    result.update(dict(rows[0]))

            return result

        except sqlite3.Error:
            return {}

    def _load_gainers_losers(
        self,
        market_date: str,
        top_n: int,
    ) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
        """Load top gainers and losers.

        First tries trading_sessions, falls back to eod_ohlcv with calculated changes.
        """
        try:
            # First check if trading_sessions has change_percent data
            cur = self.con.execute(
                """
                SELECT COUNT(*) FROM trading_sessions
                WHERE session_date = ? AND market_type = 'REG' AND change_percent IS NOT NULL
                """,
                (market_date,),
            )
            has_trading_data = cur.fetchone()[0] > 10

            if has_trading_data:
                # Use trading_sessions (original method)
                gainers_query = """
                    SELECT symbol, close, change_value, change_percent, volume
                    FROM trading_sessions
                    WHERE session_date = ? AND market_type = 'REG'
                    AND change_percent > 0
                    ORDER BY change_percent DESC
                    LIMIT ?
                """
                gainers = pd.read_sql_query(gainers_query, self.con, params=(market_date, top_n))

                losers_query = """
                    SELECT symbol, close, change_value, change_percent, volume
                    FROM trading_sessions
                    WHERE session_date = ? AND market_type = 'REG'
                    AND change_percent < 0
                    ORDER BY change_percent ASC
                    LIMIT ?
                """
                losers = pd.read_sql_query(losers_query, self.con, params=(market_date, top_n))

                breadth_query = """
                    SELECT
                        SUM(CASE WHEN change_percent > 0 THEN 1 ELSE 0 END) as gainers,
                        SUM(CASE WHEN change_percent < 0 THEN 1 ELSE 0 END) as losers,
                        SUM(CASE WHEN change_percent = 0 OR change_percent IS NULL THEN 1 ELSE 0 END) as unchanged,
                        COUNT(*) as total
                    FROM trading_sessions
                    WHERE session_date = ? AND market_type = 'REG'
                """
                cur = self.con.execute(breadth_query, (market_date,))
                row = cur.fetchone()
                breadth = {
                    "gainers": row[0] or 0,
                    "losers": row[1] or 0,
                    "unchanged": row[2] or 0,
                    "total": row[3] or 0,
                }
            else:
                # Fallback to eod_ohlcv with calculated change_percent
                # Join today's data with previous day's close
                gainers_query = """
                    WITH today AS (
                        SELECT symbol, date, open, high, low, close, volume
                        FROM eod_ohlcv
                        WHERE date = ?
                    ),
                    prev AS (
                        SELECT symbol, close as prev_close
                        FROM eod_ohlcv
                        WHERE date = (SELECT MAX(date) FROM eod_ohlcv WHERE date < ?)
                    )
                    SELECT
                        t.symbol,
                        t.close,
                        (t.close - p.prev_close) as change_value,
                        ROUND(((t.close - p.prev_close) / p.prev_close) * 100, 2) as change_percent,
                        t.volume
                    FROM today t
                    JOIN prev p ON t.symbol = p.symbol
                    WHERE p.prev_close > 0 AND t.close > p.prev_close
                    ORDER BY change_percent DESC
                    LIMIT ?
                """
                gainers = pd.read_sql_query(gainers_query, self.con, params=(market_date, market_date, top_n))

                losers_query = """
                    WITH today AS (
                        SELECT symbol, date, open, high, low, close, volume
                        FROM eod_ohlcv
                        WHERE date = ?
                    ),
                    prev AS (
                        SELECT symbol, close as prev_close
                        FROM eod_ohlcv
                        WHERE date = (SELECT MAX(date) FROM eod_ohlcv WHERE date < ?)
                    )
                    SELECT
                        t.symbol,
                        t.close,
                        (t.close - p.prev_close) as change_value,
                        ROUND(((t.close - p.prev_close) / p.prev_close) * 100, 2) as change_percent,
                        t.volume
                    FROM today t
                    JOIN prev p ON t.symbol = p.symbol
                    WHERE p.prev_close > 0 AND t.close < p.prev_close
                    ORDER BY change_percent ASC
                    LIMIT ?
                """
                losers = pd.read_sql_query(losers_query, self.con, params=(market_date, market_date, top_n))

                # Breadth from eod_ohlcv
                breadth_query = """
                    WITH today AS (
                        SELECT symbol, close
                        FROM eod_ohlcv
                        WHERE date = ?
                    ),
                    prev AS (
                        SELECT symbol, close as prev_close
                        FROM eod_ohlcv
                        WHERE date = (SELECT MAX(date) FROM eod_ohlcv WHERE date < ?)
                    ),
                    changes AS (
                        SELECT
                            t.symbol,
                            CASE
                                WHEN t.close > p.prev_close THEN 'gainer'
                                WHEN t.close < p.prev_close THEN 'loser'
                                ELSE 'unchanged'
                            END as status
                        FROM today t
                        LEFT JOIN prev p ON t.symbol = p.symbol
                    )
                    SELECT
                        SUM(CASE WHEN status = 'gainer' THEN 1 ELSE 0 END) as gainers,
                        SUM(CASE WHEN status = 'loser' THEN 1 ELSE 0 END) as losers,
                        SUM(CASE WHEN status = 'unchanged' THEN 1 ELSE 0 END) as unchanged,
                        COUNT(*) as total
                    FROM changes
                """
                cur = self.con.execute(breadth_query, (market_date, market_date))
                row = cur.fetchone()
                breadth = {
                    "gainers": row[0] or 0,
                    "losers": row[1] or 0,
                    "unchanged": row[2] or 0,
                    "total": row[3] or 0,
                }

            return gainers, losers, breadth

        except (sqlite3.Error, pd.errors.DatabaseError) as e:
            logger.warning("Error loading gainers/losers: %s", e)
            return pd.DataFrame(), pd.DataFrame(), {}

    def _load_volume_leaders(self, market_date: str, top_n: int) -> pd.DataFrame:
        """Load volume leaders from trading_sessions or eod_ohlcv."""
        try:
            # First try trading_sessions
            query = """
                SELECT symbol, close, change_percent, volume, turnover
                FROM trading_sessions
                WHERE session_date = ? AND market_type = 'REG'
                AND volume > 0
                ORDER BY volume DESC
                LIMIT ?
            """
            df = pd.read_sql_query(query, self.con, params=(market_date, top_n))

            # If trading_sessions has limited data, try eod_ohlcv
            if len(df) < top_n:
                eod_query = """
                    WITH today AS (
                        SELECT symbol, close, volume
                        FROM eod_ohlcv
                        WHERE date = ?
                    ),
                    prev AS (
                        SELECT symbol, close as prev_close
                        FROM eod_ohlcv
                        WHERE date = (SELECT MAX(date) FROM eod_ohlcv WHERE date < ?)
                    )
                    SELECT
                        t.symbol,
                        t.close,
                        ROUND(((t.close - p.prev_close) / p.prev_close) * 100, 2) as change_percent,
                        t.volume,
                        NULL as turnover
                    FROM today t
                    LEFT JOIN prev p ON t.symbol = p.symbol
                    WHERE t.volume > 0
                    ORDER BY t.volume DESC
                    LIMIT ?
                """
                df = pd.read_sql_query(eod_query, self.con, params=(market_date, market_date, top_n))

            return df

        except (sqlite3.Error, pd.errors.DatabaseError) as e:
            logger.warning("Error loading volume leaders: %s", e)
            return pd.DataFrame()

    def _load_sector_performance(self, market_date: str) -> pd.DataFrame:
        """Load sector-level performance from trading_sessions or eod_ohlcv."""
        try:
            # First try trading_sessions with change_percent
            cur = self.con.execute(
                """
                SELECT COUNT(*) FROM trading_sessions
                WHERE session_date = ? AND market_type = 'REG' AND change_percent IS NOT NULL
                """,
                (market_date,),
            )
            has_trading_data = cur.fetchone()[0] > 10

            if has_trading_data:
                query = """
                    SELECT
                        cs.sector_name as sector,
                        COUNT(DISTINCT ts.symbol) as stocks,
                        ROUND(AVG(ts.change_percent), 2) as avg_change,
                        SUM(ts.volume) as total_volume
                    FROM trading_sessions ts
                    JOIN company_snapshots cs ON ts.symbol = cs.symbol
                    WHERE ts.session_date = ? AND ts.market_type = 'REG'
                    AND cs.sector_name IS NOT NULL
                    GROUP BY cs.sector_name
                    ORDER BY avg_change DESC
                """
                return pd.read_sql_query(query, self.con, params=(market_date,))
            else:
                # Fallback to eod_ohlcv with calculated change
                query = """
                    WITH today AS (
                        SELECT symbol, close, volume
                        FROM eod_ohlcv
                        WHERE date = ?
                    ),
                    prev AS (
                        SELECT symbol, close as prev_close
                        FROM eod_ohlcv
                        WHERE date = (SELECT MAX(date) FROM eod_ohlcv WHERE date < ?)
                    ),
                    changes AS (
                        SELECT
                            t.symbol,
                            ROUND(((t.close - p.prev_close) / p.prev_close) * 100, 2) as change_percent,
                            t.volume
                        FROM today t
                        JOIN prev p ON t.symbol = p.symbol
                        WHERE p.prev_close > 0
                    )
                    SELECT
                        cs.sector_name as sector,
                        COUNT(DISTINCT c.symbol) as stocks,
                        ROUND(AVG(c.change_percent), 2) as avg_change,
                        SUM(c.volume) as total_volume
                    FROM changes c
                    JOIN company_snapshots cs ON c.symbol = cs.symbol
                    WHERE cs.sector_name IS NOT NULL
                    GROUP BY cs.sector_name
                    ORDER BY avg_change DESC
                """
                return pd.read_sql_query(query, self.con, params=(market_date, market_date))

        except (sqlite3.Error, pd.errors.DatabaseError) as e:
            logger.warning("Error loading sector performance: %s", e)
            return pd.DataFrame()

    def _get_latest_intraday_date(self, symbol: str) -> str | None:
        """Get the most recent intraday data date."""
        try:
            cur = self.con.execute(
                """
                SELECT DATE(MAX(ts)) as latest_date
                FROM intraday_bars
                WHERE symbol = ?
                """,
                (symbol,),
            )
            row = cur.fetchone()
            return row[0] if row and row[0] else None

        except sqlite3.Error:
            return None

    def _get_latest_market_date(self) -> str | None:
        """Get the most recent market date with meaningful data.

        Prefers eod_ohlcv dates with at least 100 stocks to ensure complete data.
        Falls back to trading_sessions if eod_ohlcv is empty.
        """
        try:
            # First try eod_ohlcv (has complete price data including change calculation)
            cur = self.con.execute(
                """
                SELECT date, COUNT(DISTINCT symbol) as stock_count
                FROM eod_ohlcv
                GROUP BY date
                HAVING stock_count >= 100
                ORDER BY date DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
            if row and row[0]:
                return row[0]

            # Fallback to trading_sessions with substantial data
            cur = self.con.execute(
                """
                SELECT session_date, COUNT(DISTINCT symbol) as stock_count
                FROM trading_sessions
                WHERE market_type = 'REG'
                GROUP BY session_date
                HAVING stock_count >= 100
                ORDER BY session_date DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
            if row and row[0]:
                return row[0]

            # Final fallback - any date with most data
            cur = self.con.execute(
                """
                SELECT date
                FROM eod_ohlcv
                GROUP BY date
                ORDER BY COUNT(DISTINCT symbol) DESC, date DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
            return row[0] if row and row[0] else None

        except sqlite3.Error:
            return None

    def _calculate_vwap(self, bars: pd.DataFrame) -> float | None:
        """Calculate VWAP from intraday bars."""
        if bars.empty or "volume" not in bars.columns:
            return None

        try:
            # Typical price = (high + low + close) / 3
            typical_price = (bars["high"] + bars["low"] + bars["close"]) / 3
            volume = bars["volume"].fillna(0)

            total_tp_volume = (typical_price * volume).sum()
            total_volume = volume.sum()

            if total_volume > 0:
                return round(total_tp_volume / total_volume, 2)
            return None

        except Exception:
            return None

    def _downsample_ohlcv(self, df: pd.DataFrame, target_rows: int) -> pd.DataFrame:
        """Downsample OHLCV data preserving OHLCV semantics."""
        if len(df) <= target_rows:
            return df

        # Calculate step size
        step = len(df) // target_rows

        # Sample at regular intervals
        indices = list(range(0, len(df), step))[:target_rows]

        return df.iloc[indices].reset_index(drop=True)

    def _downsample_intraday(self, df: pd.DataFrame, target_rows: int) -> pd.DataFrame:
        """Downsample intraday data preserving time series structure."""
        if len(df) <= target_rows:
            return df

        # Use pandas resample-like approach
        step = len(df) // target_rows

        indices = list(range(0, len(df), step))[:target_rows]

        return df.iloc[indices].reset_index(drop=True)


def format_data_for_prompt(data: CompanyData | IntradayData | MarketData) -> dict[str, str]:
    """Format loaded data into prompt-ready strings.

    Args:
        data: Loaded data container.

    Returns:
        Dictionary mapping prompt field names to formatted strings.
    """
    result = {}

    if isinstance(data, CompanyData):
        result["symbol"] = data.symbol
        result["company_name"] = data.company_name or "Unknown"
        result["sector"] = data.sector or "Not specified"

        # Format profile
        if data.profile:
            lines = []
            if "profile" in data.profile:
                p = data.profile["profile"]
                if isinstance(p, dict):
                    for key, value in p.items():
                        if value:
                            lines.append(f"- **{key}**: {value}")
            result["profile_data"] = "\n".join(lines) if lines else "No profile data"
        else:
            result["profile_data"] = "No profile data available"

        # Format quote
        if data.quote:
            result["quote_data"] = _format_quote(data.quote)
        else:
            result["quote_data"] = "No quote data available"

        # Format OHLCV (for Company mode)
        if not data.ohlcv.empty:
            result["ohlcv_data"] = _format_ohlcv_table(data.ohlcv)
            result["ohlcv_days"] = str(len(data.ohlcv))

            # Also populate History mode fields for compatibility
            result["ohlcv_history"] = _format_ohlcv_table(data.ohlcv)
            result["trading_days"] = str(len(data.ohlcv))

            # Calculate period statistics
            result["period_stats"] = _format_period_stats(data.ohlcv)

            # Calculate price range summary
            result["range_summary"] = _format_range_summary(data.ohlcv)

            # Date range for History mode
            result["start_date"] = str(data.ohlcv["date"].min())
            result["end_date"] = str(data.ohlcv["date"].max())
            result["period_description"] = f"{len(data.ohlcv)} trading days ({data.ohlcv['date'].min()} to {data.ohlcv['date'].max()})"
        else:
            result["ohlcv_data"] = "No OHLCV data available"
            result["ohlcv_days"] = "0"
            # History mode fallbacks
            result["ohlcv_history"] = "No history data available"
            result["period_stats"] = "No statistics available"
            result["range_summary"] = "No range data available"
            result["start_date"] = "Not specified"
            result["end_date"] = "Not specified"
            result["trading_days"] = "0"
            result["period_description"] = "No data available"

        # Format financials
        if data.financials:
            result["financial_data"] = _format_financials(data.financials)
        else:
            result["financial_data"] = "No financial data available"

        # Format payouts/dividends
        if data.payouts:
            result["payout_data"] = _format_payouts(data.payouts)
        else:
            result["payout_data"] = "No dividend/payout history available"

        # Format full financial announcements
        if data.financial_announcements:
            result["financial_announcements_data"] = _format_financial_announcements(data.financial_announcements)
        else:
            result["financial_announcements_data"] = "No financial announcements available"

        # Format intraday trading summary
        if data.intraday_summary:
            result["intraday_summary_data"] = _format_intraday_summary(data.intraday_summary)
        else:
            result["intraday_summary_data"] = "No intraday data available"

        # Format corporate announcements
        if data.corporate_announcements:
            result["corporate_announcements_data"] = _format_corporate_announcements(data.corporate_announcements)
        else:
            result["corporate_announcements_data"] = "No recent announcements available"

        # Format corporate events
        if data.corporate_events:
            result["corporate_events_data"] = _format_corporate_events(data.corporate_events)
        else:
            result["corporate_events_data"] = "No upcoming events scheduled"

        # Provenance
        result["date_range"] = f"{data.provenance.date_range[0]} to {data.provenance.date_range[1]}"
        result["data_points"] = str(data.provenance.row_count)
        result["last_updated"] = data.provenance.last_updated

    elif isinstance(data, IntradayData):
        result["symbol"] = data.symbol
        result["trading_date"] = data.trading_date
        result["market_status"] = "Closed" if data.trading_date else "Unknown"

        # Format intraday data
        if not data.bars.empty:
            result["intraday_data"] = _format_intraday_table(data.bars)
            result["data_points"] = str(len(data.bars))

            # Volume distribution
            result["volume_data"] = _format_volume_distribution(data.bars)

            # Time range
            result["time_range"] = f"{data.bars['timestamp'].min()} to {data.bars['timestamp'].max()}"
            result["bar_interval"] = "Variable"
        else:
            result["intraday_data"] = "No intraday data available"
            result["volume_data"] = "No volume data"
            result["data_points"] = "0"
            result["time_range"] = "N/A"
            result["bar_interval"] = "N/A"

        # Daily context
        if data.daily_context:
            result["daily_context"] = _format_quote(data.daily_context)
        else:
            result["daily_context"] = "No daily context available"

    elif isinstance(data, MarketData):
        result["market_date"] = data.market_date

        # Top N (infer from gainers/losers count)
        top_n = max(len(data.gainers), len(data.losers), 10)
        result["top_n"] = str(top_n)

        # Index data (now includes all indices: KSE100, KSE30, KMI30, ALLSHR)
        if data.index_data:
            result["market_overview"] = _format_index(data.index_data)
        else:
            result["market_overview"] = "Index data not available"

        # Gainers - with enhanced formatting
        if not data.gainers.empty:
            gainers_df = data.gainers.copy()
            # Add rank column
            gainers_df.insert(0, "#", range(1, len(gainers_df) + 1))
            result["gainers_data"] = f"**{len(gainers_df)} stocks with positive gains:**\n\n" + gainers_df.to_markdown(index=False)
        else:
            result["gainers_data"] = "No gainers data available for this date"

        # Losers - with enhanced formatting
        if not data.losers.empty:
            losers_df = data.losers.copy()
            # Add rank column
            losers_df.insert(0, "#", range(1, len(losers_df) + 1))
            result["losers_data"] = f"**{len(losers_df)} stocks with negative gains:**\n\n" + losers_df.to_markdown(index=False)
        else:
            result["losers_data"] = "No losers data available for this date"

        # Volume leaders - with enhanced formatting
        if not data.volume_leaders.empty:
            vol_df = data.volume_leaders.copy()
            vol_df.insert(0, "#", range(1, len(vol_df) + 1))
            result["volume_leaders"] = f"**Top {len(vol_df)} by trading volume:**\n\n" + vol_df.to_markdown(index=False)
        else:
            result["volume_leaders"] = "No volume data available"

        # Sector data - with enhanced formatting
        if not data.sector_performance.empty:
            sector_df = data.sector_performance.copy()
            result["sector_data"] = f"**{len(sector_df)} sectors analyzed:**\n\n" + sector_df.to_markdown(index=False)
        else:
            result["sector_data"] = "No sector data available"

        # Breadth - enhanced with ratios
        if data.breadth:
            gainers_count = data.breadth.get('gainers', 0)
            losers_count = data.breadth.get('losers', 0)
            unchanged = data.breadth.get('unchanged', 0)
            total = data.breadth.get('total', 0)

            # Calculate ratio
            if losers_count > 0:
                adv_dec_ratio = gainers_count / losers_count
            else:
                adv_dec_ratio = float('inf') if gainers_count > 0 else 0

            result["breadth_data"] = (
                f"- **Advancing (Gainers)**: {gainers_count} stocks 🟢\n"
                f"- **Declining (Losers)**: {losers_count} stocks 🔴\n"
                f"- **Unchanged**: {unchanged} stocks ⚪\n"
                f"- **Total Traded**: {total} stocks\n"
                f"- **Advance/Decline Ratio**: {adv_dec_ratio:.2f}\n"
                f"- **Market Sentiment**: {'BULLISH' if adv_dec_ratio > 1.5 else 'BEARISH' if adv_dec_ratio < 0.67 else 'NEUTRAL'}"
            )
        else:
            result["breadth_data"] = "No breadth data"

        # Counts
        result["total_stocks"] = str(data.breadth.get("total", 0))
        result["sector_count"] = str(len(data.sector_performance))
        result["last_updated"] = data.provenance.last_updated

    return result


def _format_payouts(payouts: list[dict]) -> str:
    """Format dividend/payout history."""
    if not payouts:
        return "No dividend history available"

    lines = ["**Recent Dividend History:**"]

    for payout in payouts:
        date = payout.get("ex_date", "N/A")
        amount = payout.get("amount")
        fiscal = payout.get("fiscal_year", "")
        payout_type = payout.get("payout_type", "cash")

        # Format the amount
        if amount:
            amount_str = f"{amount}%"
        else:
            amount_str = "N/A"

        # Add fiscal year and type info
        period_str = f" ({fiscal})" if fiscal else ""
        type_str = f" [{payout_type}]" if payout_type and payout_type != "cash" else ""

        lines.append(f"- **{date}**: {amount_str}{period_str}{type_str}")

    return "\n".join(lines)


def _format_financial_announcements(announcements: list[dict]) -> str:
    """Format full financial announcements with EPS, profit, AGM, etc."""
    if not announcements:
        return "No financial announcements available"

    lines = ["**Recent Financial Results:**"]

    for ann in announcements:
        date = ann.get("announcement_date", "N/A")
        period = ann.get("fiscal_period", "")

        # Build the announcement summary
        parts = []

        # EPS
        eps = ann.get("eps")
        if eps is not None:
            parts.append(f"EPS: Rs.{eps:.2f}")

        # Profit After Tax
        pat = ann.get("profit_after_tax")
        if pat is not None:
            # Convert to millions for readability
            parts.append(f"PAT: Rs.{pat:,.0f}M")

        # Profit Before Tax
        pbt = ann.get("profit_before_tax")
        if pbt is not None:
            parts.append(f"PBT: Rs.{pbt:,.0f}M")

        # Dividend
        dividend = ann.get("dividend_payout")
        if dividend:
            parts.append(f"Dividend: {dividend}")

        # AGM date
        agm = ann.get("agm_date")
        if agm:
            parts.append(f"AGM: {agm}")

        # Book closure
        book = ann.get("book_closure")
        if book:
            parts.append(f"Book Closure: {book}")

        # Build the line
        period_str = f" ({period})" if period else ""
        details = " | ".join(parts) if parts else "No details"
        lines.append(f"- **{date}{period_str}**: {details}")

    return "\n".join(lines)


def _format_intraday_summary(summary: dict) -> str:
    """Format intraday trading summary for AI analysis.

    Includes VWAP, true intraday high/low, volume, and time range.
    """
    if not summary:
        return "No intraday data available"

    lines = ["**Recent Intraday Trading Summary:**"]

    trading_date = summary.get("trading_date")
    if trading_date:
        lines.append(f"- **Trading Date**: {trading_date}")

    # Time range
    first_bar = summary.get("first_bar")
    last_bar = summary.get("last_bar")
    if first_bar and last_bar:
        lines.append(f"- **Session Time**: {first_bar} to {last_bar}")

    # Bar count
    bar_count = summary.get("bar_count")
    if bar_count:
        lines.append(f"- **Data Points**: {bar_count} intraday bars")

    # True intraday high/low (not derived!)
    intraday_high = summary.get("intraday_high")
    intraday_low = summary.get("intraday_low")
    if intraday_high is not None and intraday_low is not None:
        lines.append(f"- **True Intraday High**: {intraday_high:.2f} *(actual intraday extreme)*")
        lines.append(f"- **True Intraday Low**: {intraday_low:.2f} *(actual intraday extreme)*")
        price_range = intraday_high - intraday_low
        lines.append(f"- **Intraday Range**: {price_range:.2f}")

    # VWAP
    vwap = summary.get("vwap")
    if vwap is not None:
        lines.append(f"- **VWAP**: {vwap:.2f}")

    # Last close
    last_close = summary.get("last_close")
    if last_close is not None:
        lines.append(f"- **Last Traded Price**: {last_close:.2f}")

    # Total volume
    total_volume = summary.get("total_volume")
    if total_volume is not None:
        lines.append(f"- **Total Intraday Volume**: {total_volume:,.0f}")

    return "\n".join(lines)


def _format_quote(quote: dict) -> str:
    """Format quote dictionary."""
    lines = []

    fields = [
        ("close", "Current Price"),
        ("change_value", "Change"),
        ("change_percent", "Change %"),
        ("open", "Open"),
        ("high", "High *(derived)*"),
        ("low", "Low *(derived)*"),
        ("volume", "Volume"),
        ("ldcp", "Previous Close"),
        ("turnover", "Turnover"),
    ]

    for key, label in fields:
        if key in quote and quote[key] is not None:
            value = quote[key]
            if key == "volume" and isinstance(value, (int, float)):
                value = f"{value:,.0f}"
            elif key == "turnover" and isinstance(value, (int, float)):
                value = f"Rs.{value:,.0f}"
            lines.append(f"- **{label}**: {value}")

    return "\n".join(lines) if lines else "No quote data"


def _format_ohlcv_table(df: pd.DataFrame, max_rows: int = 30) -> str:
    """Format OHLCV DataFrame as markdown table."""
    display = df.tail(max_rows).copy()

    # Format volume
    if "volume" in display.columns:
        display["volume"] = display["volume"].apply(
            lambda x: f"{x:,.0f}" if pd.notna(x) else "N/A"
        )

    # Add derived high/low note
    note = "\n*Note: high/low are derived from max/min(open,close), not true intraday extremes.*\n"

    return display.to_markdown(index=False) + note


def _format_period_stats(df: pd.DataFrame) -> str:
    """Calculate and format period statistics from OHLCV data."""
    if df.empty:
        return "No statistics available"

    lines = ["**Period Statistics:**\n"]

    # Price statistics
    if "close" in df.columns:
        close_prices = df["close"].dropna()
        if not close_prices.empty:
            lines.append(f"| Metric | Value |")
            lines.append(f"|--------|-------|")
            lines.append(f"| Starting Price | {close_prices.iloc[0]:,.2f} |")
            lines.append(f"| Ending Price | {close_prices.iloc[-1]:,.2f} |")

            # Calculate return
            start_price = close_prices.iloc[0]
            end_price = close_prices.iloc[-1]
            if start_price > 0:
                period_return = ((end_price - start_price) / start_price) * 100
                lines.append(f"| Period Return | {period_return:+.2f}% |")

            lines.append(f"| Average Close | {close_prices.mean():,.2f} |")
            lines.append(f"| Median Close | {close_prices.median():,.2f} |")
            lines.append(f"| Std Deviation | {close_prices.std():,.2f} |")

    # Volume statistics
    if "volume" in df.columns:
        volume = df["volume"].dropna()
        if not volume.empty:
            lines.append(f"| Total Volume | {volume.sum():,.0f} |")
            lines.append(f"| Avg Daily Volume | {volume.mean():,.0f} |")
            lines.append(f"| Max Daily Volume | {volume.max():,.0f} |")

    # Trading days
    lines.append(f"| Trading Days | {len(df)} |")

    return "\n".join(lines)


def _format_range_summary(df: pd.DataFrame) -> str:
    """Calculate and format price range summary from OHLCV data."""
    if df.empty:
        return "No range data available"

    lines = ["**Price Range Summary:**\n"]
    lines.append("| Metric | Value | Date |")
    lines.append("|--------|-------|------|")

    # Period high (from high column)
    if "high" in df.columns:
        high_prices = df["high"].dropna()
        if not high_prices.empty:
            max_high = high_prices.max()
            max_high_date = df.loc[high_prices.idxmax(), "date"]
            lines.append(f"| Period High | {max_high:,.2f} | {max_high_date} |")

    # Period low (from low column)
    if "low" in df.columns:
        low_prices = df["low"].dropna()
        if not low_prices.empty:
            min_low = low_prices.min()
            min_low_date = df.loc[low_prices.idxmin(), "date"]
            lines.append(f"| Period Low | {min_low:,.2f} | {min_low_date} |")

    # High-low range
    if "high" in df.columns and "low" in df.columns:
        max_high = df["high"].max()
        min_low = df["low"].min()
        range_val = max_high - min_low
        range_pct = (range_val / min_low * 100) if min_low > 0 else 0
        lines.append(f"| Price Range | {range_val:,.2f} ({range_pct:.1f}%) | - |")

    # Highest close
    if "close" in df.columns:
        close_prices = df["close"].dropna()
        if not close_prices.empty:
            max_close = close_prices.max()
            max_close_date = df.loc[close_prices.idxmax(), "date"]
            lines.append(f"| Highest Close | {max_close:,.2f} | {max_close_date} |")

            min_close = close_prices.min()
            min_close_date = df.loc[close_prices.idxmin(), "date"]
            lines.append(f"| Lowest Close | {min_close:,.2f} | {min_close_date} |")

    # Highest volume day
    if "volume" in df.columns:
        volume = df["volume"].dropna()
        if not volume.empty:
            max_vol = volume.max()
            max_vol_date = df.loc[volume.idxmax(), "date"]
            lines.append(f"| Highest Volume | {max_vol:,.0f} | {max_vol_date} |")

    lines.append("\n*Note: High/Low values are derived from max/min(open,close), not true intraday extremes.*")

    return "\n".join(lines)


def _format_intraday_table(df: pd.DataFrame, max_rows: int = 50) -> str:
    """Format intraday bars as markdown table."""
    display = df.tail(max_rows).copy()

    if "volume" in display.columns:
        display["volume"] = display["volume"].apply(
            lambda x: f"{x:,.0f}" if pd.notna(x) else "N/A"
        )

    return display.to_markdown(index=False)


def _format_volume_distribution(df: pd.DataFrame) -> str:
    """Create volume distribution summary."""
    if "volume" not in df.columns:
        return "No volume data"

    total_volume = df["volume"].sum()
    avg_volume = df["volume"].mean()
    max_volume = df["volume"].max()
    max_idx = df["volume"].idxmax()

    lines = [
        f"- **Total Volume**: {total_volume:,.0f}",
        f"- **Average per Bar**: {avg_volume:,.0f}",
        f"- **Peak Volume**: {max_volume:,.0f} at {df.loc[max_idx, 'timestamp']}",
    ]

    return "\n".join(lines)


def _format_financials(financials: dict) -> str:
    """Format financials dictionary."""
    lines = []

    if "financials" in financials:
        fin = financials["financials"]
        if isinstance(fin, dict) and "annual" in fin:
            annual = fin["annual"]
            if isinstance(annual, list) and len(annual) > 0:
                latest = annual[0]
                lines.append("**Latest Annual Results:**")
                for key, value in latest.items():
                    if value is not None:
                        lines.append(f"- {key}: {value}")

    if "ratios" in financials:
        ratios = financials["ratios"]
        if isinstance(ratios, dict) and "annual" in ratios:
            annual = ratios["annual"]
            if isinstance(annual, list) and len(annual) > 0:
                latest = annual[0]
                lines.append("\n**Key Ratios:**")
                for key, value in latest.items():
                    if value is not None:
                        lines.append(f"- {key}: {value}")

    return "\n".join(lines) if lines else "No financial data"


def _format_index(index_data: dict) -> str:
    """Format index data including all available indices."""
    lines = []

    # Check if we have multiple indices
    indices = index_data.get("indices", [])
    if indices:
        lines.append("**PSX Indices Overview:**\n")
        lines.append("| Index | Value | Change | Change % |")
        lines.append("|-------|-------|--------|----------|")

        for idx in indices:
            code = idx.get("index_code", "N/A")
            value = idx.get("value")
            change = idx.get("change")
            change_pct = idx.get("change_pct")

            value_str = f"{value:,.2f}" if value else "N/A"
            change_str = f"{change:+,.2f}" if change else "N/A"
            pct_str = f"{change_pct:+.2f}%" if change_pct else "N/A"

            # Add emoji based on direction
            emoji = "🟢" if change_pct and change_pct > 0 else "🔴" if change_pct and change_pct < 0 else "⚪"
            lines.append(f"| {code} | {value_str} | {change_str} | {pct_str} {emoji} |")

        # Add detailed KSE-100 section
        kse100 = next((idx for idx in indices if idx.get("index_code") == "KSE100"), None)
        if kse100:
            lines.append("\n**KSE-100 Details:**")
            if kse100.get("high"):
                lines.append(f"- **High**: {kse100['high']:,.2f}")
            if kse100.get("low"):
                lines.append(f"- **Low**: {kse100['low']:,.2f}")
            if kse100.get("volume"):
                lines.append(f"- **Volume**: {kse100['volume']:,.0f}")
            if kse100.get("previous_close"):
                lines.append(f"- **Previous Close**: {kse100['previous_close']:,.2f}")
            if kse100.get("week_52_high"):
                lines.append(f"- **52W High**: {kse100['week_52_high']:,.2f}")
            if kse100.get("week_52_low"):
                lines.append(f"- **52W Low**: {kse100['week_52_low']:,.2f}")

    # Fallback for single index format (backward compatibility)
    elif "value" in index_data:
        lines.append(f"- **KSE-100 Value**: {index_data['value']:,.2f}")
        if "change" in index_data:
            lines.append(f"- **Change**: {index_data['change']:+,.2f}")
        if "change_pct" in index_data:
            lines.append(f"- **Change %**: {index_data['change_pct']:+.2f}%")
        if "volume" in index_data:
            lines.append(f"- **Volume**: {index_data['volume']:,.0f}")

    return "\n".join(lines) if lines else "Index data not available"


def _format_corporate_announcements(announcements: list[dict]) -> str:
    """Format corporate announcements for AI analysis."""
    if not announcements:
        return "No recent announcements available"

    lines = ["**Recent Corporate Announcements:**"]

    # Group by category
    categories = {}
    for ann in announcements:
        cat = ann.get("category", "general")
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(ann)

    # Format each category
    category_labels = {
        "results": "📊 Financial Results",
        "dividend": "💰 Dividend Related",
        "agm": "📅 AGM/EOGM",
        "board_meeting": "👥 Board Meetings",
        "book_closure": "📕 Book Closure",
        "corporate_action": "🏢 Corporate Actions",
        "general": "📢 General",
    }

    for cat, anns in categories.items():
        label = category_labels.get(cat, f"📋 {cat.title()}")
        lines.append(f"\n**{label}:**")
        for ann in anns[:3]:  # Limit to 3 per category
            date = ann.get("date", "N/A")
            title = ann.get("title", "")[:80]  # Truncate long titles
            if len(ann.get("title", "")) > 80:
                title += "..."
            lines.append(f"- [{date}] {title}")

    return "\n".join(lines)


def _format_corporate_events(events: list[dict]) -> str:
    """Format upcoming corporate events for AI analysis."""
    if not events:
        return "No upcoming events scheduled"

    lines = ["**Upcoming Corporate Events:**"]

    event_icons = {
        "AGM": "📅",
        "EOGM": "🗓️",
        "ARM": "📋",
        "BOARD": "👥",
    }

    for event in events:
        event_type = event.get("type", "EVENT")
        icon = event_icons.get(event_type.upper(), "📌")
        date = event.get("date", "TBD")
        time = event.get("time", "")
        city = event.get("city", "")
        period = event.get("period_end", "")

        # Build event line
        parts = [f"{icon} **{event_type}**", f"on {date}"]
        if time:
            parts.append(f"at {time}")
        if city:
            parts.append(f"in {city}")
        if period:
            parts.append(f"(Period: {period})")

        lines.append(f"- {' '.join(parts)}")

    return "\n".join(lines)
