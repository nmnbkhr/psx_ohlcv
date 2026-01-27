"""Prompt templates for LLM-based market analysis.

This module provides structured prompt templates for different analysis modes:
- Company Summary: Profile + latest quote + OHLCV history
- Intraday Commentary: Intraday time series + volume analysis
- Market Summary: Gainers/losers + sector performance
- History Analysis: Historical OHLCV patterns

Each prompt includes:
- "Data Used" section: Tables, fields, and time ranges
- "Hard Rules" section: No invented numbers, explicit "Not available" statements
- "PSX Caveats" section: Liquidity warnings, circuit breakers, derived high/low warning
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any


class InsightMode(Enum):
    """Available insight generation modes."""

    COMPANY = "company"
    INTRADAY = "intraday"
    MARKET = "market"
    HISTORY = "history"


# System prompt with safety rules and context
SYSTEM_PROMPT = """You are a financial analyst assistant specializing in Pakistan Stock Exchange (PSX) data analysis.

## YOUR ROLE
- Analyze provided stock market data objectively and factually
- Provide clear, structured insights in markdown format
- Help users understand market movements and company performance

## HARD RULES (MUST FOLLOW)
1. **NEVER invent, estimate, or hallucinate numbers** - only use data explicitly provided
2. If data is missing or unavailable, explicitly state "Data not available" or "Not provided"
3. Do not claim certainty about future price movements
4. Do not provide investment advice or buy/sell recommendations
5. Always acknowledge data limitations and time ranges

## PSX MARKET CONTEXT
- Pakistan Stock Exchange operates Monday-Friday, 9:30 AM - 3:30 PM PKT
- Circuit breakers: Individual stocks have ±7.5% daily limits
- Settlement: T+2 settlement cycle
- Currency: All prices in Pakistani Rupees (PKR)

## CRITICAL DATA CAVEAT
**DERIVED HIGH/LOW WARNING**: In this application, the daily high and low values in EOD (End of Day)
data are calculated as max(open, close) and min(open, close) respectively. These are NOT true
intraday highs and lows. Do NOT claim these represent actual intraday price extremes.

## OUTPUT FORMAT
- Use markdown formatting with headers, bullet points, and tables where appropriate
- Be concise but comprehensive
- Highlight key insights at the beginning
- Include data source attribution
"""


# Template for Company Summary analysis
COMPANY_SUMMARY_TEMPLATE = """## Analysis Request: Company Summary

### SYMBOL: {symbol}
### COMPANY: {company_name}
### SECTOR: {sector}

---

## DATA PROVIDED

### 1. Company Profile
{profile_data}

### 2. Latest Quote Snapshot
{quote_data}

### 3. Recent OHLCV History (Last {ohlcv_days} Trading Days)
{ohlcv_data}

### 4. Key Financial Metrics (if available)
{financial_data}

---

## DATA USED SECTION
- **Tables**: company_snapshots, eod_ohlcv, trading_sessions
- **Symbol**: {symbol}
- **Date Range**: {date_range}
- **Data Points**: {data_points}
- **Last Updated**: {last_updated}

## ANALYSIS REQUEST
Please provide a comprehensive summary covering:
1. **Company Overview** - Business description and sector positioning
2. **Recent Price Action** - Analysis of the provided OHLCV data
3. **Volume Analysis** - Trading activity trends
4. **Key Metrics** - P/E ratio, market cap, dividend yield (if available)
5. **Notable Observations** - Any significant patterns or events

## HARD RULES REMINDER
- Only use the numbers provided above
- If any data field shows "N/A" or is missing, state "Not available"
- Do NOT invent any statistics or percentages not explicitly provided

## PSX CAVEATS REMINDER
- **DERIVED HIGH/LOW**: The high and low in OHLCV data are max/min of open,close - NOT true intraday extremes
- **Liquidity**: Some stocks may have thin trading volumes
- **Circuit Breakers**: Daily price limits of ±7.5% apply
- **Market Hours**: 9:30 AM - 3:30 PM PKT, Monday-Friday
"""


# Template for Intraday Commentary
INTRADAY_TEMPLATE = """## Analysis Request: Intraday Commentary

### SYMBOL: {symbol}
### DATE: {trading_date}
### MARKET STATUS: {market_status}

---

## DATA PROVIDED

### 1. Intraday Price Series
{intraday_data}

### 2. Volume Distribution
{volume_data}

### 3. Daily Context
{daily_context}

---

## DATA USED SECTION
- **Tables**: intraday_bars, trading_sessions
- **Symbol**: {symbol}
- **Trading Date**: {trading_date}
- **Time Range**: {time_range}
- **Data Points**: {data_points} bars
- **Bar Interval**: {bar_interval}

## ANALYSIS REQUEST
Please provide intraday commentary covering:
1. **Session Overview** - How the trading day progressed
2. **Price Movement** - Key price levels and movements
3. **Volume Patterns** - When was trading most active
4. **VWAP Analysis** - If VWAP data is provided
5. **Key Timestamps** - Significant price changes and their times

## HARD RULES REMINDER
- Only reference timestamps and prices explicitly provided
- If VWAP or other metrics are not provided, state "Not calculated"
- Do not extrapolate or predict closing prices for ongoing sessions

## PSX CAVEATS REMINDER
- **Circuit Breakers**: If price hits ±7.5% limit, trading may pause
- **Market Hours**: 9:30 AM - 3:30 PM PKT
- **Pre-market/After-hours**: No trading outside market hours
- **Data Latency**: Intraday data may have slight delays from live market
"""


# Template for Market Summary
MARKET_SUMMARY_TEMPLATE = """## Analysis Request: Market Summary

### DATE: {market_date}
### INDEX: KSE-100

---

## DATA PROVIDED

### 1. Market Overview
{market_overview}

### 2. Top Gainers
{gainers_data}

### 3. Top Losers
{losers_data}

### 4. Volume Leaders
{volume_leaders}

### 5. Sector Performance
{sector_data}

### 6. Market Breadth
{breadth_data}

---

## DATA USED SECTION
- **Tables**: trading_sessions, eod_ohlcv, psx_indices, sectors
- **Date**: {market_date}
- **Stocks Analyzed**: {total_stocks}
- **Sectors Covered**: {sector_count}
- **Data Freshness**: {last_updated}

## ANALYSIS REQUEST
Please provide a market summary covering:
1. **Index Performance** - KSE-100 movement (if provided)
2. **Market Breadth** - Gainers vs losers ratio
3. **Sector Analysis** - Which sectors led/lagged
4. **Volume Analysis** - Overall market activity
5. **Notable Movers** - Stocks with significant changes
6. **Key Observations** - Any market-wide patterns

## HARD RULES REMINDER
- Only cite specific stocks and numbers from the data provided
- If index data is not provided, state "Index data not available"
- Do not invent sector percentages or market-wide statistics

## PSX CAVEATS REMINDER
- **DERIVED HIGH/LOW**: Daily highs/lows in EOD data are derived from open/close, not true extremes
- **Thin Trading**: Some stocks may have limited liquidity
- **Circuit Limits**: Stocks hitting ±7.5% may have limited price discovery
- **Weekend/Holiday**: No trading on weekends and Pakistani public holidays
"""


# Template for Historical Analysis
HISTORY_TEMPLATE = """## Analysis Request: Historical OHLCV Analysis

### SYMBOL: {symbol}
### PERIOD: {period_description}

---

## DATA PROVIDED

### 1. OHLCV History
{ohlcv_history}

### 2. Period Statistics
{period_stats}

### 3. Price Range Summary
{range_summary}

---

## DATA USED SECTION
- **Tables**: eod_ohlcv
- **Symbol**: {symbol}
- **Date Range**: {start_date} to {end_date}
- **Trading Days**: {trading_days}
- **Data Points**: {data_points} rows

## ANALYSIS REQUEST
Please analyze the historical data covering:
1. **Price Trend** - Overall direction and key levels
2. **Volatility** - Price range and movement patterns
3. **Volume Trends** - Changes in trading activity over time
4. **Key Events** - Significant price movements and dates
5. **Statistical Summary** - High, low, average price over period

## HARD RULES REMINDER
- Only use dates and prices from the provided data
- Do not calculate returns or percentages unless explicitly provided
- If calculating from provided data, show the formula used

## PSX CAVEATS REMINDER
- **CRITICAL: DERIVED HIGH/LOW** - The high and low values are calculated as max(open,close) and
  min(open,close). These do NOT represent true intraday highs and lows. Do not claim otherwise.
- **Historical Gaps**: Missing dates indicate non-trading days (weekends, holidays)
- **Corporate Actions**: Stock splits or dividends may affect historical comparisons
- **Data Quality**: Historical data accuracy depends on source reliability
"""


@dataclass
class PromptContext:
    """Context data for prompt generation."""

    symbol: str = ""
    company_name: str = ""
    sector: str = ""
    date_range: str = ""
    data_points: int = 0
    last_updated: str = ""
    trading_date: str = ""
    market_date: str = ""
    extra: dict[str, Any] = None

    def __post_init__(self):
        if self.extra is None:
            self.extra = {}


class PromptBuilder:
    """Builder for constructing LLM prompts with data context.

    This class handles the construction of properly formatted prompts
    that include all required sections and safety rules.

    Example:
        >>> builder = PromptBuilder(InsightMode.COMPANY)
        >>> prompt = builder.build(
        ...     symbol="OGDC",
        ...     company_name="Oil & Gas Development Co",
        ...     profile_data=profile_dict,
        ...     ohlcv_data=ohlcv_df,
        ... )
    """

    def __init__(self, mode: InsightMode):
        """Initialize builder with analysis mode.

        Args:
            mode: The type of analysis to generate prompts for.
        """
        self.mode = mode
        self._templates = {
            InsightMode.COMPANY: COMPANY_SUMMARY_TEMPLATE,
            InsightMode.INTRADAY: INTRADAY_TEMPLATE,
            InsightMode.MARKET: MARKET_SUMMARY_TEMPLATE,
            InsightMode.HISTORY: HISTORY_TEMPLATE,
        }

    @property
    def system_prompt(self) -> str:
        """Get the system prompt with safety rules."""
        return SYSTEM_PROMPT

    def build(self, **kwargs) -> str:
        """Build the complete prompt with provided data.

        Args:
            **kwargs: Data fields to fill in the template.
                      Missing fields will be replaced with "Not provided".

        Returns:
            Formatted prompt string ready for LLM.
        """
        template = self._templates[self.mode]

        # Set defaults for missing fields
        defaults = self._get_defaults()
        for key, value in defaults.items():
            if key not in kwargs or kwargs[key] is None:
                kwargs[key] = value

        # Format any complex data types
        kwargs = self._format_data_fields(kwargs)

        try:
            return template.format(**kwargs)
        except KeyError as e:
            # Handle missing template variables gracefully
            missing_key = str(e).strip("'")
            kwargs[missing_key] = "Not provided"
            return template.format(**kwargs)

    def _get_defaults(self) -> dict[str, str]:
        """Get default values for template fields."""
        return {
            "symbol": "Unknown",
            "company_name": "Not provided",
            "sector": "Not provided",
            "profile_data": "No profile data available",
            "quote_data": "No quote data available",
            "ohlcv_data": "No OHLCV data available",
            "ohlcv_days": "0",
            "financial_data": "No financial data available",
            "date_range": "Not specified",
            "data_points": "0",
            "last_updated": "Unknown",
            "intraday_data": "No intraday data available",
            "volume_data": "No volume data available",
            "daily_context": "No daily context available",
            "trading_date": "Not specified",
            "market_status": "Unknown",
            "time_range": "Not specified",
            "bar_interval": "Unknown",
            "market_overview": "No market overview available",
            "gainers_data": "No gainers data available",
            "losers_data": "No losers data available",
            "volume_leaders": "No volume leaders data available",
            "sector_data": "No sector data available",
            "breadth_data": "No breadth data available",
            "market_date": "Not specified",
            "total_stocks": "0",
            "sector_count": "0",
            "ohlcv_history": "No history data available",
            "period_stats": "No statistics available",
            "range_summary": "No range data available",
            "period_description": "Not specified",
            "start_date": "Not specified",
            "end_date": "Not specified",
            "trading_days": "0",
        }

    def _format_data_fields(self, kwargs: dict) -> dict:
        """Format complex data fields into readable strings."""
        import pandas as pd

        formatted = {}
        for key, value in kwargs.items():
            if isinstance(value, pd.DataFrame):
                # Convert DataFrame to markdown table
                if len(value) > 0:
                    try:
                        formatted[key] = value.to_markdown(index=False)
                    except ImportError:
                        # Fallback if tabulate not installed
                        formatted[key] = value.to_string(index=False)
                else:
                    formatted[key] = "No data rows"
            elif isinstance(value, dict):
                # Convert dict to formatted string
                formatted[key] = self._dict_to_string(value)
            elif isinstance(value, list):
                # Convert list to bullet points
                if len(value) > 0:
                    formatted[key] = "\n".join(f"- {item}" for item in value)
                else:
                    formatted[key] = "No items"
            else:
                formatted[key] = value

        return formatted

    def _dict_to_string(self, d: dict, indent: int = 0) -> str:
        """Convert a dictionary to a formatted string."""
        lines = []
        prefix = "  " * indent

        for key, value in d.items():
            if value is None:
                lines.append(f"{prefix}- **{key}**: N/A")
            elif isinstance(value, dict):
                lines.append(f"{prefix}- **{key}**:")
                lines.append(self._dict_to_string(value, indent + 1))
            elif isinstance(value, list):
                lines.append(f"{prefix}- **{key}**: {', '.join(str(v) for v in value)}")
            else:
                lines.append(f"{prefix}- **{key}**: {value}")

        return "\n".join(lines)

    def get_prompt_hash_inputs(self, **kwargs) -> dict:
        """Extract key fields for cache hash calculation.

        Returns dict with fields that affect prompt content:
        - symbol(s)
        - date ranges
        - mode
        """
        hash_inputs = {
            "mode": self.mode.value,
            "symbol": kwargs.get("symbol", ""),
            "date_range": kwargs.get("date_range", ""),
            "trading_date": kwargs.get("trading_date", ""),
            "market_date": kwargs.get("market_date", ""),
        }
        return {k: v for k, v in hash_inputs.items() if v}


def get_data_caveat_warning() -> str:
    """Get the standard data caveat warning text."""
    return """
**IMPORTANT DATA CAVEAT**:
The daily high and low values in this application's EOD data are derived as
max(open, close) and min(open, close) respectively. These are NOT true intraday
highs and lows. Actual intraday price extremes may differ significantly.
"""


def format_ohlcv_for_prompt(df, max_rows: int = 30) -> str:
    """Format OHLCV DataFrame for prompt inclusion.

    Args:
        df: DataFrame with columns: date, open, high, low, close, volume
        max_rows: Maximum rows to include (most recent)

    Returns:
        Formatted markdown table string.
    """
    import pandas as pd

    if df is None or len(df) == 0:
        return "No OHLCV data available"

    # Take most recent rows
    df = df.tail(max_rows).copy()

    # Format for readability
    display_df = df[["date", "open", "high", "low", "close", "volume"]].copy()
    display_df["volume"] = display_df["volume"].apply(
        lambda x: f"{x:,.0f}" if pd.notna(x) else "N/A"
    )

    try:
        result = display_df.to_markdown(index=False)
    except ImportError:
        # Fallback if tabulate not installed
        result = display_df.to_string(index=False)

    # Add derived high/low warning
    return result + "\n*Note: high/low are derived from max/min(open,close), not true intraday extremes.*"


def format_quote_for_prompt(quote: dict) -> str:
    """Format quote data dictionary for prompt inclusion."""
    if not quote:
        return "No quote data available"

    lines = [
        f"- **Current Price**: {quote.get('close', 'N/A')}",
        f"- **Change**: {quote.get('change_value', 'N/A')} ({quote.get('change_percent', 'N/A')}%)",
        f"- **Open**: {quote.get('open', 'N/A')}",
        f"- **High**: {quote.get('high', 'N/A')} *(derived)*",
        f"- **Low**: {quote.get('low', 'N/A')} *(derived)*",
        f"- **Volume**: {quote.get('volume', 'N/A'):,}" if isinstance(quote.get('volume'), (int, float)) else f"- **Volume**: {quote.get('volume', 'N/A')}",
        f"- **Previous Close**: {quote.get('ldcp', 'N/A')}",
    ]

    # Add optional fields if available
    if quote.get("pe_ratio_ttm"):
        lines.append(f"- **P/E Ratio (TTM)**: {quote['pe_ratio_ttm']}")
    if quote.get("market_cap"):
        lines.append(f"- **Market Cap**: {quote['market_cap']:,}" if isinstance(quote['market_cap'], (int, float)) else f"- **Market Cap**: {quote['market_cap']}")
    if quote.get("week_52_high"):
        lines.append(f"- **52-Week High**: {quote['week_52_high']}")
    if quote.get("week_52_low"):
        lines.append(f"- **52-Week Low**: {quote['week_52_low']}")

    return "\n".join(lines)
