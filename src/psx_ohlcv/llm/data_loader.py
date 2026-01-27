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
        """Load latest quote from trading_sessions or snapshots."""
        try:
            # Try trading_sessions first (more current)
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
                return {
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

            # Fallback to company_snapshots
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
                quote = {}
                if row["quote_data"]:
                    try:
                        quote.update(json.loads(row["quote_data"]))
                    except json.JSONDecodeError:
                        pass
                if row["trading_data"]:
                    try:
                        trading = json.loads(row["trading_data"])
                        if "REG" in trading:
                            quote.update(trading["REG"])
                    except json.JSONDecodeError:
                        pass
                return quote

            return {}

        except sqlite3.Error as e:
            logger.warning("Error loading quote: %s", e)
            return {}

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
                SELECT timestamp, open, high, low, close, volume
                FROM intraday_bars
                WHERE symbol = ?
                AND DATE(timestamp) = ?
                ORDER BY timestamp
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

    def _load_index_data(self, market_date: str) -> dict:
        """Load KSE-100 index data."""
        try:
            cur = self.con.execute(
                """
                SELECT *
                FROM psx_indices
                WHERE index_code = 'KSE100'
                AND index_date = ?
                """,
                (market_date,),
            )
            row = cur.fetchone()
            return dict(row) if row else {}

        except sqlite3.Error:
            return {}

    def _load_gainers_losers(
        self,
        market_date: str,
        top_n: int,
    ) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
        """Load top gainers and losers."""
        try:
            # Gainers
            gainers_query = """
                SELECT symbol, close, change_value, change_percent, volume
                FROM trading_sessions
                WHERE session_date = ? AND market_type = 'REG'
                AND change_percent > 0
                ORDER BY change_percent DESC
                LIMIT ?
            """
            gainers = pd.read_sql_query(gainers_query, self.con, params=(market_date, top_n))

            # Losers
            losers_query = """
                SELECT symbol, close, change_value, change_percent, volume
                FROM trading_sessions
                WHERE session_date = ? AND market_type = 'REG'
                AND change_percent < 0
                ORDER BY change_percent ASC
                LIMIT ?
            """
            losers = pd.read_sql_query(losers_query, self.con, params=(market_date, top_n))

            # Breadth
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

            return gainers, losers, breadth

        except (sqlite3.Error, pd.errors.DatabaseError) as e:
            logger.warning("Error loading gainers/losers: %s", e)
            return pd.DataFrame(), pd.DataFrame(), {}

    def _load_volume_leaders(self, market_date: str, top_n: int) -> pd.DataFrame:
        """Load volume leaders."""
        try:
            query = """
                SELECT symbol, close, change_percent, volume, turnover
                FROM trading_sessions
                WHERE session_date = ? AND market_type = 'REG'
                AND volume > 0
                ORDER BY volume DESC
                LIMIT ?
            """
            return pd.read_sql_query(query, self.con, params=(market_date, top_n))

        except (sqlite3.Error, pd.errors.DatabaseError) as e:
            logger.warning("Error loading volume leaders: %s", e)
            return pd.DataFrame()

    def _load_sector_performance(self, market_date: str) -> pd.DataFrame:
        """Load sector-level performance."""
        try:
            query = """
                SELECT
                    cs.sector_name as sector,
                    COUNT(DISTINCT ts.symbol) as stocks,
                    AVG(ts.change_percent) as avg_change,
                    SUM(ts.volume) as total_volume
                FROM trading_sessions ts
                JOIN company_snapshots cs ON ts.symbol = cs.symbol
                WHERE ts.session_date = ? AND ts.market_type = 'REG'
                AND cs.sector_name IS NOT NULL
                GROUP BY cs.sector_name
                ORDER BY avg_change DESC
            """
            return pd.read_sql_query(query, self.con, params=(market_date,))

        except (sqlite3.Error, pd.errors.DatabaseError) as e:
            logger.warning("Error loading sector performance: %s", e)
            return pd.DataFrame()

    def _get_latest_intraday_date(self, symbol: str) -> str | None:
        """Get the most recent intraday data date."""
        try:
            cur = self.con.execute(
                """
                SELECT DATE(MAX(timestamp)) as latest_date
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
        """Get the most recent market date with data."""
        try:
            cur = self.con.execute(
                "SELECT MAX(session_date) FROM trading_sessions WHERE market_type = 'REG'"
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

        # Format OHLCV
        if not data.ohlcv.empty:
            result["ohlcv_data"] = _format_ohlcv_table(data.ohlcv)
            result["ohlcv_days"] = str(len(data.ohlcv))
        else:
            result["ohlcv_data"] = "No OHLCV data available"
            result["ohlcv_days"] = "0"

        # Format financials
        if data.financials:
            result["financial_data"] = _format_financials(data.financials)
        else:
            result["financial_data"] = "No financial data available"

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

        # Index data
        if data.index_data:
            result["market_overview"] = _format_index(data.index_data)
        else:
            result["market_overview"] = "Index data not available"

        # Gainers
        if not data.gainers.empty:
            result["gainers_data"] = data.gainers.to_markdown(index=False)
        else:
            result["gainers_data"] = "No gainers data"

        # Losers
        if not data.losers.empty:
            result["losers_data"] = data.losers.to_markdown(index=False)
        else:
            result["losers_data"] = "No losers data"

        # Volume leaders
        if not data.volume_leaders.empty:
            result["volume_leaders"] = data.volume_leaders.to_markdown(index=False)
        else:
            result["volume_leaders"] = "No volume data"

        # Sector data
        if not data.sector_performance.empty:
            result["sector_data"] = data.sector_performance.to_markdown(index=False)
        else:
            result["sector_data"] = "No sector data"

        # Breadth
        if data.breadth:
            result["breadth_data"] = (
                f"- **Gainers**: {data.breadth.get('gainers', 0)}\n"
                f"- **Losers**: {data.breadth.get('losers', 0)}\n"
                f"- **Unchanged**: {data.breadth.get('unchanged', 0)}\n"
                f"- **Total Stocks**: {data.breadth.get('total', 0)}"
            )
        else:
            result["breadth_data"] = "No breadth data"

        # Counts
        result["total_stocks"] = str(data.breadth.get("total", 0))
        result["sector_count"] = str(len(data.sector_performance))
        result["last_updated"] = data.provenance.last_updated

    return result


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
    """Format index data."""
    lines = []

    if "value" in index_data:
        lines.append(f"- **KSE-100 Value**: {index_data['value']:,.2f}")
    if "change" in index_data:
        lines.append(f"- **Change**: {index_data['change']:+,.2f}")
    if "change_pct" in index_data:
        lines.append(f"- **Change %**: {index_data['change_pct']:+.2f}%")
    if "volume" in index_data:
        lines.append(f"- **Volume**: {index_data['volume']:,.0f}")

    return "\n".join(lines) if lines else "Index data not available"
