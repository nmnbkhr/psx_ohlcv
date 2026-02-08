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
- Provide a clear assessment and actionable items

## HARD RULES (MUST FOLLOW)
1. **NEVER invent, estimate, or hallucinate numbers** - only use data explicitly provided
2. If data is missing or unavailable, explicitly state "Data not available" or "Not provided"
3. Do not claim certainty about future price movements
4. Provide educational observations, NOT personal investment advice
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

## OUTPUT FORMAT (STRICT)
You MUST structure your response as follows:

### 1. ASSESSMENT BOX (Required First)
Start with a highlighted assessment box using this exact format:
```
> 📊 **ASSESSMENT**: [BULLISH 🟢 / BEARISH 🔴 / NEUTRAL ⚪ / MIXED 🟡]
>
> **One-line summary**: [Your key finding in one sentence]
>
> **Confidence**: [HIGH / MEDIUM / LOW] based on data completeness
```

### 2. KEY METRICS TABLE (Required)
A quick-reference table with the most important numbers.

### 3. DETAILED ANALYSIS
Your comprehensive analysis with sections.

### 4. ACTION ITEMS (Required Last)
End with specific, actionable items:
```
## 📋 Action Items
- [ ] **Monitor**: [Specific thing to watch]
- [ ] **Research**: [What to investigate further]
- [ ] **Alert**: [Set price/volume alerts if applicable]
```

## DISCLAIMER
Always include this disclaimer at the very end:
```
---
*⚠️ Disclaimer: This is data-driven analysis, not investment advice. Always conduct your own research and consult a licensed financial advisor before making investment decisions.*
```
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

### 5. Dividend/Payout History
{payout_data}

### 6. Financial Announcements (Earnings Results)
{financial_announcements_data}

### 7. Intraday Trading Summary
{intraday_summary_data}

### 8. Recent Corporate Announcements
{corporate_announcements_data}

### 9. Upcoming Corporate Events
{corporate_events_data}

---

## DATA USED SECTION
- **Tables**: company_snapshots, eod_ohlcv, trading_sessions, company_payouts, financial_announcements, intraday_bars, company_announcements, corporate_events
- **Symbol**: {symbol}
- **Date Range**: {date_range}
- **Data Points**: {data_points}
- **Last Updated**: {last_updated}

## ANALYSIS REQUEST
Provide a comprehensive analysis with this EXACT structure:

### REQUIRED: Start with Assessment Box
Use the format from system prompt - BULLISH/BEARISH/NEUTRAL/MIXED with confidence level.

### REQUIRED: Key Metrics Table
| Metric | Value | Signal |
|--------|-------|--------|
| Current Price | XXX | - |
| Change % | XXX% | 🟢/🔴 |
| Volume | XXX | Above/Below avg |
| P/E Ratio | XXX | - |
| VWAP | XXX | Above/Below price |

### Detailed Analysis Sections:
1. **Company Overview** - Business description and sector positioning
2. **Price Action Analysis** - Trend direction, support/resistance from OHLCV data
3. **Volume Analysis** - Is volume confirming price movement? Unusual activity?
4. **Financial Health** - P/E, EPS trends, dividend history (if available)
5. **Intraday Patterns** - VWAP positioning, session high/low (if available)
6. **Corporate Activity** - Recent announcements, upcoming AGM/EOGM, dividends
7. **Risk Factors** - Thin trading, circuit limit proximity, data gaps

### REQUIRED: End with Action Items
Specific actionable items using checkbox format.

### REQUIRED: Disclaimer
Standard disclaimer about not being investment advice.

## HARD RULES REMINDER
- Only use the numbers provided above
- If any data field shows "N/A" or is missing, state "Not available"
- Do NOT invent any statistics or percentages not explicitly provided
- Assessment must be based ONLY on provided data, not speculation

## PSX CAVEATS REMINDER
- **DERIVED HIGH/LOW**: The high and low in EOD OHLCV data are max/min of open,close - NOT true intraday extremes
- **TRUE INTRADAY**: The intraday summary provides actual intraday highs/lows from tick data
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
### INDICES: KSE-100, KSE-30, KMI-30, ALL SHARE

---

## DATA PROVIDED

### 1. PSX Indices Overview
{market_overview}

### 2. Top {top_n} Gainers (Stocks with highest % gain)
{gainers_data}

### 3. Top {top_n} Losers (Stocks with highest % loss)
{losers_data}

### 4. Top {top_n} Volume Leaders (Most traded stocks)
{volume_leaders}

### 5. Sector Performance (All sectors)
{sector_data}

### 6. Market Breadth (Total stocks traded)
{breadth_data}

---

## DATA USED SECTION
- **Tables**: trading_sessions, psx_indices, company_snapshots (for sectors)
- **Date**: {market_date}
- **Total Stocks Analyzed**: {total_stocks}
- **Sectors Covered**: {sector_count}
- **Data Freshness**: {last_updated}

## ANALYSIS REQUEST
Provide a comprehensive market summary with this EXACT structure:

### REQUIRED: Start with Assessment Box
Use the format from system prompt - BULLISH/BEARISH/NEUTRAL/MIXED with confidence level based on market breadth and index direction.

### REQUIRED: Key Metrics Table
| Metric | Value | Signal |
|--------|-------|--------|
| KSE-100 | XXX | 🟢/🔴 |
| Market Breadth | XXX gainers / XXX losers | Net +/- |
| Total Volume | XXX | High/Normal/Low |
| Leading Sector | XXX | +X.XX% |

### Detailed Analysis Sections:
1. **Indices Performance** - KSE-100, KSE-30, KMI-30, ALL SHARE movements and comparison
2. **Market Breadth** - Gainers vs losers ratio, sentiment interpretation
3. **Sector Analysis** - Top performing and lagging sectors with reasons
4. **Volume Analysis** - Overall market activity, volume leaders significance
5. **Top Movers Analysis** - Why are gainers gaining? Why are losers losing?
6. **Key Observations** - Market-wide patterns, circuit limit hits, unusual activity

### REQUIRED: End with Action Items
- [ ] **Monitor**: Sectors or stocks to watch
- [ ] **Research**: Events affecting market today
- [ ] **Alert**: Set alerts for key levels or stocks

### REQUIRED: Disclaimer

## HARD RULES REMINDER
- Only cite specific stocks and numbers from the data provided
- ANALYZE all {top_n} gainers and losers, not just 1 stock
- Explain WHY stocks moved (sector trend, volume, circuit limits)
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
            "payout_data": "No dividend/payout history available",
            "financial_announcements_data": "No financial announcements available",
            "intraday_summary_data": "No intraday data available",
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
            "corporate_announcements_data": "No recent corporate announcements",
            "corporate_events_data": "No upcoming corporate events",
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
        pe_val = f"{quote['pe_ratio_ttm']:.2f}" if isinstance(quote['pe_ratio_ttm'], float) else str(quote['pe_ratio_ttm'])
        lines.append(f"- **P/E Ratio (TTM)**: {pe_val} *(based on unconsolidated financials)*")
    if quote.get("market_cap"):
        lines.append(f"- **Market Cap**: {quote['market_cap']:,}" if isinstance(quote['market_cap'], (int, float)) else f"- **Market Cap**: {quote['market_cap']}")
    if quote.get("week_52_high"):
        lines.append(f"- **52-Week High**: {quote['week_52_high']}")
    if quote.get("week_52_low"):
        lines.append(f"- **52-Week Low**: {quote['week_52_low']}")
    if quote.get("circuit_high") and quote.get("circuit_low"):
        lines.append(f"- **Circuit Breaker Range**: {quote['circuit_low']} - {quote['circuit_high']}")
    if quote.get("ytd_change") is not None:
        lines.append(f"- **YTD Change**: {quote['ytd_change']:.2f}%" if isinstance(quote['ytd_change'], float) else f"- **YTD Change**: {quote['ytd_change']}%")
    if quote.get("year_1_change") is not None:
        lines.append(f"- **1-Year Change**: {quote['year_1_change']:.2f}%" if isinstance(quote['year_1_change'], float) else f"- **1-Year Change**: {quote['year_1_change']}%")

    return "\n".join(lines)


def format_payouts_for_prompt(payouts: list[dict]) -> str:
    """Format dividend/payout history for prompt inclusion.

    Args:
        payouts: List of payout dicts from database.

    Returns:
        Formatted string with dividend history.
    """
    if not payouts:
        return "No dividend/payout history available"

    lines = ["**Recent Dividend History:**"]

    for payout in payouts:
        date = payout.get("ex_date", "N/A")
        amount = payout.get("amount")
        details = payout.get("details", "")
        fiscal = payout.get("fiscal_period", "")
        payout_type = payout.get("payout_type", "cash")

        # Format the payout entry
        if amount:
            amount_str = f"{amount}%"
        elif details:
            amount_str = details
        else:
            amount_str = "N/A"

        period_str = f" ({fiscal})" if fiscal else ""
        type_str = f" [{payout_type}]" if payout_type != "cash" else ""

        lines.append(f"- {date}: {amount_str}{period_str}{type_str}")

    return "\n".join(lines)
