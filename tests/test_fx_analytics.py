"""Tests for Phase 2: FX analytics functions."""

import pandas as pd
import pytest

from pakfindata import connect, init_schema
from pakfindata.analytics_fx import (
    compute_fx_adjusted_return,
    compute_fx_returns,
    compute_fx_trend,
    compute_fx_volatility,
    get_fx_analytics,
)
from pakfindata.db import (
    upsert_fx_ohlcv,
    upsert_fx_pair,
)


@pytest.fixture
def db():
    """Create an in-memory database for testing."""
    con = connect(":memory:")
    init_schema(con)
    yield con
    con.close()


@pytest.fixture
def db_with_fx_data(db):
    """Database with sample FX data."""
    # Create pair
    upsert_fx_pair(db, {
        "pair": "USD/PKR",
        "base_currency": "USD",
        "quote_currency": "PKR",
        "source": "SAMPLE",
    })

    # Create 60 days of OHLCV data
    dates = pd.date_range("2024-01-01", periods=60)
    df = pd.DataFrame({
        "date": dates.strftime("%Y-%m-%d"),
        "open": [278.0 + i * 0.05 for i in range(60)],
        "high": [278.5 + i * 0.05 for i in range(60)],
        "low": [277.5 + i * 0.05 for i in range(60)],
        "close": [278.0 + i * 0.06 for i in range(60)],  # Slight uptrend
        "volume": [None] * 60,
    })
    upsert_fx_ohlcv(db, "USD/PKR", df)

    return db


class TestFXReturns:
    """Tests for FX return calculations."""

    def test_compute_returns_basic(self):
        """Compute FX returns from OHLCV data."""
        # Create 30 days of data with price increase
        dates = pd.date_range("2024-01-01", periods=30)
        prices = [278.0 + i * 0.1 for i in range(30)]  # 278 to 280.9
        df = pd.DataFrame({
            "date": dates.strftime("%Y-%m-%d"),
            "close": prices,
        })
        # Sort descending as expected
        df = df.sort_values("date", ascending=False)

        returns = compute_fx_returns(df, periods=[5, 21])

        assert "return_1W" in returns
        # 5-day return should be positive (price increased)
        assert returns["return_1W"] > 0

    def test_compute_returns_empty_df(self):
        """Compute returns with empty DataFrame."""
        df = pd.DataFrame()
        returns = compute_fx_returns(df)
        assert returns == {}

    def test_compute_returns_insufficient_data(self):
        """Returns not computed with insufficient data."""
        df = pd.DataFrame({
            "date": ["2024-01-01", "2024-01-02"],
            "close": [278.0, 278.5],
        })
        df = df.sort_values("date", ascending=False)

        returns = compute_fx_returns(df, periods=[21])  # Need 21+ days

        # Should not have 1M return (need 21+ data points)
        assert "return_1M" not in returns


class TestFXVolatility:
    """Tests for FX volatility calculations."""

    def test_compute_volatility_basic(self):
        """Compute FX volatility."""
        # Create data with some variance
        dates = pd.date_range("2024-01-01", periods=30)
        # Oscillating prices for volatility
        prices = [278.0 + (i % 5) * 0.2 for i in range(30)]
        df = pd.DataFrame({
            "date": dates.strftime("%Y-%m-%d"),
            "close": prices,
        })

        volatility = compute_fx_volatility(df, windows=[21])

        assert "vol_1M" in volatility
        assert volatility["vol_1M"] > 0

    def test_compute_volatility_empty_df(self):
        """Volatility with empty DataFrame."""
        df = pd.DataFrame()
        vol = compute_fx_volatility(df)
        assert vol == {}


class TestFXTrend:
    """Tests for FX trend analysis."""

    def test_compute_trend_upward(self):
        """Compute upward trend."""
        # Create uptrending data
        dates = pd.date_range("2024-01-01", periods=60)
        prices = [278.0 + i * 0.1 for i in range(60)]
        df = pd.DataFrame({
            "date": dates.strftime("%Y-%m-%d"),
            "close": prices,
        })

        trend = compute_fx_trend(df, ma_period=50)

        assert "trend_direction" in trend
        assert trend["trend_direction"] == "up"  # Price above MA
        assert trend["above_ma"] == True  # noqa: E712 (numpy bool comparison)

    def test_compute_trend_insufficient_data(self):
        """Trend requires enough data for MA."""
        df = pd.DataFrame({
            "date": ["2024-01-01"],
            "close": [278.0],
        })

        trend = compute_fx_trend(df, ma_period=50)

        assert trend == {}  # Insufficient data


class TestFXAnalyticsIntegration:
    """Integration tests for FX analytics."""

    def test_get_fx_analytics(self, db_with_fx_data):
        """Get comprehensive FX analytics."""
        analytics = get_fx_analytics(db_with_fx_data, "USD/PKR")

        assert analytics["pair"] == "USD/PKR"
        assert "error" not in analytics
        assert "latest_close" in analytics
        # Should have returns
        assert "return_1M" in analytics or "return_1W" in analytics

    def test_get_fx_analytics_no_data(self, db):
        """Analytics for non-existent pair."""
        analytics = get_fx_analytics(db, "XXX/YYY")
        assert "error" in analytics


class TestFXAdjustedReturn:
    """Tests for FX-adjusted return calculation."""

    def test_compute_adjusted_return_basic(self):
        """Basic FX-adjusted return calculation."""
        equity_return = 0.05  # 5%
        fx_return = 0.02  # 2% depreciation

        adjusted = compute_fx_adjusted_return(equity_return, fx_return)

        # 5% - 2% = 3%
        assert abs(adjusted - 0.03) < 0.0001

    def test_compute_adjusted_return_appreciation(self):
        """FX appreciation increases adjusted return."""
        equity_return = 0.05  # 5%
        fx_return = -0.02  # 2% appreciation

        adjusted = compute_fx_adjusted_return(equity_return, fx_return)

        # 5% - (-2%) = 7%
        assert adjusted == 0.07

    def test_compute_adjusted_return_none(self):
        """Handle None values."""
        adjusted = compute_fx_adjusted_return(None, 0.02)
        assert adjusted is None

        adjusted = compute_fx_adjusted_return(0.05, None)
        assert adjusted is None
