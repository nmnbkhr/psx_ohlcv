"""Analytics tools for agentic AI.

Wraps existing analytics functions as callable tools for AI agents.
These tools provide advanced analytics like returns computation,
volatility analysis, and technical indicators.
"""

from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from ..db import get_connection
from ..query import get_ohlcv_range
from ..analytics_phase1 import compute_returns, compute_volatility, compute_relative_strength
from .registry import Tool, ToolCategory, ToolRegistry


# =============================================================================
# Returns Computation Tools
# =============================================================================


def compute_stock_returns(
    symbol: str,
    periods: list[str] | None = None,
) -> dict[str, Any]:
    """Compute returns for a stock over multiple periods.

    Args:
        symbol: Stock symbol
        periods: List of period labels (e.g., ["1d", "1w", "1m", "3m", "1y"])

    Returns:
        Dict with returns for each period
    """
    con = get_connection()
    symbol = symbol.upper()

    # Get 1 year of data to compute all periods
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

    df = get_ohlcv_range(con, symbol, start_date=start_date, end_date=end_date)

    if df.empty:
        return {
            "symbol": symbol,
            "error": f"No data found for {symbol}",
        }

    # Map period labels to trading days
    period_map = {
        "1d": 1,
        "1w": 5,
        "1m": 21,
        "3m": 63,
        "6m": 126,
        "1y": 252,
        "ytd": None,  # Special handling
    }

    # Default periods if not specified
    if periods is None:
        periods = ["1d", "1w", "1m", "3m", "1y"]

    # Convert to trading days
    period_days = []
    for p in periods:
        if p.lower() in period_map:
            days = period_map[p.lower()]
            if days is not None:
                period_days.append(days)

    # Compute returns
    returns = compute_returns(df, periods=period_days)

    # Get YTD if requested
    if "ytd" in [p.lower() for p in periods]:
        year_start = datetime(datetime.now().year, 1, 1).strftime("%Y-%m-%d")
        ytd_df = df[df["date"] >= year_start]
        if len(ytd_df) > 1:
            first_close = ytd_df["close"].iloc[0]
            last_close = ytd_df["close"].iloc[-1]
            if first_close and first_close > 0:
                returns["return_ytd"] = round(
                    ((last_close - first_close) / first_close) * 100, 4
                )

    # Get latest price info
    latest = df.iloc[-1] if not df.empty else None

    return {
        "symbol": symbol,
        "latest_date": str(latest["date"]) if latest is not None else None,
        "latest_close": float(latest["close"]) if latest is not None else None,
        "returns": returns,
        "data_points": len(df),
    }


ToolRegistry.register(
    Tool(
        name="compute_stock_returns",
        description="Compute returns for a stock over multiple time periods (1 day, 1 week, 1 month, 3 months, 1 year, YTD). Use this for performance analysis.",
        function=compute_stock_returns,
        category=ToolCategory.ANALYTICS,
        parameters={
            "type": "object",
            "properties": {
                "symbol": {
                    "type": "string",
                    "description": "PSX stock symbol (e.g., HBL, OGDC, ENGRO)",
                },
                "periods": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of periods to compute (e.g., ['1d', '1w', '1m', '3m', '1y', 'ytd'])",
                    "default": ["1d", "1w", "1m", "3m", "1y"],
                },
            },
            "required": ["symbol"],
        },
        returns_description="Dict with returns for each period as percentages",
    )
)


# =============================================================================
# Volatility Analysis Tools
# =============================================================================


def compute_stock_volatility(
    symbol: str,
    windows: list[int] | None = None,
) -> dict[str, Any]:
    """Compute annualized volatility for a stock.

    Args:
        symbol: Stock symbol
        windows: Rolling windows in days (default: [21, 63] for 1m, 3m)

    Returns:
        Dict with volatility metrics
    """
    con = get_connection()
    symbol = symbol.upper()

    # Get enough data for volatility calculation
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

    df = get_ohlcv_range(con, symbol, start_date=start_date, end_date=end_date)

    if df.empty:
        return {
            "symbol": symbol,
            "error": f"No data found for {symbol}",
        }

    if len(df) < 21:
        return {
            "symbol": symbol,
            "error": f"Insufficient data for volatility calculation (need at least 21 days, have {len(df)})",
        }

    # Default windows
    if windows is None:
        windows = [21, 63]  # 1 month, 3 months

    # Compute volatility
    vol_metrics = compute_volatility(df, windows=windows)

    # Compute daily return statistics
    df_sorted = df.sort_values("date", ascending=True).copy()
    df_sorted["return"] = df_sorted["close"].pct_change()
    returns = df_sorted["return"].dropna()

    return {
        "symbol": symbol,
        "latest_date": str(df.iloc[-1]["date"]),
        "latest_close": float(df.iloc[-1]["close"]),
        "volatility": vol_metrics,
        "return_stats": {
            "mean_daily_return_pct": round(returns.mean() * 100, 4) if len(returns) > 0 else None,
            "max_daily_gain_pct": round(returns.max() * 100, 2) if len(returns) > 0 else None,
            "max_daily_loss_pct": round(returns.min() * 100, 2) if len(returns) > 0 else None,
            "positive_days_pct": round((returns > 0).sum() / len(returns) * 100, 1) if len(returns) > 0 else None,
        },
        "data_points": len(df),
    }


ToolRegistry.register(
    Tool(
        name="compute_stock_volatility",
        description="Compute annualized volatility for a stock using rolling windows. Also provides daily return statistics like max gain/loss and positive day percentage.",
        function=compute_stock_volatility,
        category=ToolCategory.ANALYTICS,
        parameters={
            "type": "object",
            "properties": {
                "symbol": {
                    "type": "string",
                    "description": "PSX stock symbol (e.g., HBL, OGDC, ENGRO)",
                },
                "windows": {
                    "type": "array",
                    "items": {"type": "integer"},
                    "description": "Rolling windows in trading days (default: [21, 63] for 1m, 3m)",
                },
            },
            "required": ["symbol"],
        },
        returns_description="Dict with annualized volatility and return statistics",
    )
)


# =============================================================================
# Relative Strength Tools
# =============================================================================


def compute_relative_performance(
    symbol: str,
    benchmark: str = "KSE100",
    periods: list[str] | None = None,
) -> dict[str, Any]:
    """Compute relative strength vs a benchmark.

    Args:
        symbol: Stock symbol
        benchmark: Benchmark symbol (default: KSE100)
        periods: Periods to compare (default: ["1m", "3m"])

    Returns:
        Dict with relative performance metrics
    """
    con = get_connection()
    symbol = symbol.upper()
    benchmark = benchmark.upper()

    # Get data for both symbol and benchmark
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

    symbol_df = get_ohlcv_range(con, symbol, start_date=start_date, end_date=end_date)
    bench_df = get_ohlcv_range(con, benchmark, start_date=start_date, end_date=end_date)

    if symbol_df.empty:
        return {
            "symbol": symbol,
            "benchmark": benchmark,
            "error": f"No data found for {symbol}",
        }

    if bench_df.empty:
        return {
            "symbol": symbol,
            "benchmark": benchmark,
            "error": f"No data found for benchmark {benchmark}",
        }

    # Map periods to trading days
    period_map = {"1m": 21, "3m": 63, "6m": 126, "1y": 252}

    if periods is None:
        periods = ["1m", "3m"]

    period_days = [period_map.get(p.lower(), 21) for p in periods]

    # Compute returns for both
    symbol_returns = compute_returns(symbol_df, periods=period_days)
    bench_returns = compute_returns(bench_df, periods=period_days)

    # Compute relative strength
    rs_metrics = {}
    for period in periods:
        period_key = f"return_{period.lower()}"
        rs_key = f"rs_{period.lower()}"

        if period_key in symbol_returns and period_key in bench_returns:
            sym_ret = symbol_returns[period_key]
            bench_ret = bench_returns[period_key]
            rs_metrics[rs_key] = round(sym_ret - bench_ret, 4)
            rs_metrics[f"symbol_{period_key}"] = sym_ret
            rs_metrics[f"bench_{period_key}"] = bench_ret

    # Determine outperformance
    outperforming = sum(1 for k, v in rs_metrics.items() if k.startswith("rs_") and v > 0)
    total_periods = sum(1 for k in rs_metrics.keys() if k.startswith("rs_"))

    return {
        "symbol": symbol,
        "benchmark": benchmark,
        "relative_strength": rs_metrics,
        "outperforming_periods": outperforming,
        "total_periods": total_periods,
        "assessment": (
            f"{symbol} outperforming {benchmark}"
            if outperforming > total_periods / 2
            else f"{symbol} underperforming {benchmark}"
        ) if total_periods > 0 else "Insufficient data",
        "data_points": {
            "symbol": len(symbol_df),
            "benchmark": len(bench_df),
        },
    }


ToolRegistry.register(
    Tool(
        name="compute_relative_performance",
        description="Compute relative strength of a stock versus a benchmark (default: KSE100). Shows whether stock is outperforming or underperforming the market.",
        function=compute_relative_performance,
        category=ToolCategory.ANALYTICS,
        parameters={
            "type": "object",
            "properties": {
                "symbol": {
                    "type": "string",
                    "description": "PSX stock symbol (e.g., HBL, OGDC, ENGRO)",
                },
                "benchmark": {
                    "type": "string",
                    "description": "Benchmark symbol (default: KSE100)",
                    "default": "KSE100",
                },
                "periods": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Periods to compare (e.g., ['1m', '3m'])",
                    "default": ["1m", "3m"],
                },
            },
            "required": ["symbol"],
        },
        returns_description="Dict with relative performance metrics and assessment",
    )
)


# =============================================================================
# Technical Indicators Tools
# =============================================================================


def get_technical_indicators(
    symbol: str,
    indicators: list[str] | None = None,
) -> dict[str, Any]:
    """Get technical indicators for a stock.

    Args:
        symbol: Stock symbol
        indicators: List of indicators (default: ["sma", "rsi", "macd"])

    Returns:
        Dict with technical indicators
    """
    con = get_connection()
    symbol = symbol.upper()

    # Get 6 months of data for indicator calculation
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=200)).strftime("%Y-%m-%d")

    df = get_ohlcv_range(con, symbol, start_date=start_date, end_date=end_date)

    if df.empty:
        return {
            "symbol": symbol,
            "error": f"No data found for {symbol}",
        }

    if len(df) < 50:
        return {
            "symbol": symbol,
            "error": f"Insufficient data for technical analysis (need at least 50 days, have {len(df)})",
        }

    # Sort by date ascending
    df = df.sort_values("date", ascending=True).reset_index(drop=True)

    if indicators is None:
        indicators = ["sma", "rsi", "macd", "bollinger"]

    result = {
        "symbol": symbol,
        "latest_date": str(df["date"].iloc[-1]),
        "latest_close": float(df["close"].iloc[-1]),
        "indicators": {},
    }

    # Simple Moving Averages
    if "sma" in indicators:
        result["indicators"]["sma"] = {
            "sma_20": round(df["close"].tail(20).mean(), 2),
            "sma_50": round(df["close"].tail(50).mean(), 2) if len(df) >= 50 else None,
            "sma_200": round(df["close"].tail(200).mean(), 2) if len(df) >= 200 else None,
        }
        # Price vs SMA signals
        latest_close = df["close"].iloc[-1]
        sma_20 = result["indicators"]["sma"]["sma_20"]
        sma_50 = result["indicators"]["sma"]["sma_50"]
        result["indicators"]["sma"]["above_sma_20"] = latest_close > sma_20
        result["indicators"]["sma"]["above_sma_50"] = latest_close > sma_50 if sma_50 else None

    # RSI (Relative Strength Index)
    if "rsi" in indicators:
        delta = df["close"].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)

        # 14-period RSI
        avg_gain = gain.tail(14).mean()
        avg_loss = loss.tail(14).mean()

        if avg_loss != 0:
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
        else:
            rsi = 100 if avg_gain > 0 else 50

        result["indicators"]["rsi"] = {
            "rsi_14": round(rsi, 2),
            "signal": (
                "overbought" if rsi > 70 else
                "oversold" if rsi < 30 else
                "neutral"
            ),
        }

    # MACD
    if "macd" in indicators:
        ema_12 = df["close"].ewm(span=12, adjust=False).mean()
        ema_26 = df["close"].ewm(span=26, adjust=False).mean()
        macd_line = ema_12 - ema_26
        signal_line = macd_line.ewm(span=9, adjust=False).mean()
        histogram = macd_line - signal_line

        result["indicators"]["macd"] = {
            "macd": round(macd_line.iloc[-1], 4),
            "signal": round(signal_line.iloc[-1], 4),
            "histogram": round(histogram.iloc[-1], 4),
            "trend": "bullish" if macd_line.iloc[-1] > signal_line.iloc[-1] else "bearish",
        }

    # Bollinger Bands
    if "bollinger" in indicators:
        sma_20 = df["close"].tail(20).mean()
        std_20 = df["close"].tail(20).std()
        upper_band = sma_20 + (2 * std_20)
        lower_band = sma_20 - (2 * std_20)
        latest_close = df["close"].iloc[-1]

        # Percent B: (Price - Lower) / (Upper - Lower)
        pct_b = (latest_close - lower_band) / (upper_band - lower_band) if (upper_band - lower_band) != 0 else 0.5

        result["indicators"]["bollinger"] = {
            "upper_band": round(upper_band, 2),
            "middle_band": round(sma_20, 2),
            "lower_band": round(lower_band, 2),
            "percent_b": round(pct_b * 100, 1),
            "position": (
                "above_upper" if latest_close > upper_band else
                "below_lower" if latest_close < lower_band else
                "within_bands"
            ),
        }

    # Overall technical summary
    signals = []
    if "rsi" in result["indicators"]:
        if result["indicators"]["rsi"]["rsi_14"] < 30:
            signals.append("RSI oversold (bullish)")
        elif result["indicators"]["rsi"]["rsi_14"] > 70:
            signals.append("RSI overbought (bearish)")

    if "macd" in result["indicators"]:
        if result["indicators"]["macd"]["trend"] == "bullish":
            signals.append("MACD bullish crossover")
        else:
            signals.append("MACD bearish")

    if "sma" in result["indicators"]:
        if result["indicators"]["sma"]["above_sma_20"] and result["indicators"]["sma"].get("above_sma_50"):
            signals.append("Price above SMAs (bullish)")
        elif not result["indicators"]["sma"]["above_sma_20"]:
            signals.append("Price below SMA-20 (bearish)")

    result["technical_signals"] = signals if signals else ["No strong signals"]
    result["data_points"] = len(df)

    return result


ToolRegistry.register(
    Tool(
        name="get_technical_indicators",
        description="Get technical analysis indicators for a stock including SMA (Simple Moving Averages), RSI (Relative Strength Index), MACD, and Bollinger Bands. Provides trading signals.",
        function=get_technical_indicators,
        category=ToolCategory.ANALYTICS,
        parameters={
            "type": "object",
            "properties": {
                "symbol": {
                    "type": "string",
                    "description": "PSX stock symbol (e.g., HBL, OGDC, ENGRO)",
                },
                "indicators": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of indicators to compute (options: 'sma', 'rsi', 'macd', 'bollinger')",
                    "default": ["sma", "rsi", "macd", "bollinger"],
                },
            },
            "required": ["symbol"],
        },
        returns_description="Dict with technical indicators and trading signals",
    )
)


# =============================================================================
# Correlation Analysis Tool
# =============================================================================


def compute_correlation(
    symbols: list[str],
    days: int = 90,
) -> dict[str, Any]:
    """Compute correlation matrix between multiple stocks.

    Args:
        symbols: List of stock symbols (2-10)
        days: Number of days for correlation calculation

    Returns:
        Dict with correlation matrix
    """
    if len(symbols) < 2:
        return {"error": "Need at least 2 symbols for correlation"}
    if len(symbols) > 10:
        symbols = symbols[:10]

    con = get_connection()

    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days + 30)).strftime("%Y-%m-%d")

    # Fetch data for all symbols
    price_data = {}
    for symbol in symbols:
        symbol = symbol.upper()
        df = get_ohlcv_range(con, symbol, start_date=start_date, end_date=end_date)
        if not df.empty:
            df = df.sort_values("date", ascending=True).tail(days)
            price_data[symbol] = df.set_index("date")["close"]

    if len(price_data) < 2:
        return {"error": "Insufficient data for correlation analysis"}

    # Create DataFrame and compute returns
    prices_df = pd.DataFrame(price_data)
    returns_df = prices_df.pct_change().dropna()

    if len(returns_df) < 20:
        return {"error": "Insufficient overlapping data for correlation"}

    # Compute correlation matrix
    corr_matrix = returns_df.corr()

    # Convert to list of pairs with correlation values
    pairs = []
    for i, sym1 in enumerate(corr_matrix.columns):
        for j, sym2 in enumerate(corr_matrix.columns):
            if i < j:
                corr_value = corr_matrix.loc[sym1, sym2]
                pairs.append({
                    "pair": f"{sym1}/{sym2}",
                    "correlation": round(corr_value, 4),
                    "relationship": (
                        "strong positive" if corr_value > 0.7 else
                        "moderate positive" if corr_value > 0.3 else
                        "weak/no correlation" if corr_value > -0.3 else
                        "moderate negative" if corr_value > -0.7 else
                        "strong negative"
                    ),
                })

    # Sort by absolute correlation
    pairs.sort(key=lambda x: abs(x["correlation"]), reverse=True)

    return {
        "symbols": list(price_data.keys()),
        "period_days": days,
        "data_points": len(returns_df),
        "correlations": pairs,
        "matrix": corr_matrix.round(4).to_dict(),
        "highest_correlation": pairs[0] if pairs else None,
        "lowest_correlation": pairs[-1] if pairs else None,
    }


ToolRegistry.register(
    Tool(
        name="compute_correlation",
        description="Compute correlation matrix between multiple stocks. Shows how stocks move together, useful for portfolio diversification analysis.",
        function=compute_correlation,
        category=ToolCategory.ANALYTICS,
        parameters={
            "type": "object",
            "properties": {
                "symbols": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of stock symbols to correlate (2-10 symbols)",
                    "minItems": 2,
                    "maxItems": 10,
                },
                "days": {
                    "type": "integer",
                    "description": "Number of days for correlation calculation (default: 90)",
                    "default": 90,
                },
            },
            "required": ["symbols"],
        },
        returns_description="Dict with correlation matrix and pair-wise correlations",
    )
)
