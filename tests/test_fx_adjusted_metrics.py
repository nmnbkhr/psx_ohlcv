"""Tests for Phase 2: FX-adjusted metrics calculations and storage."""

import pandas as pd
import pytest

from pakfindata import connect, init_schema
from pakfindata.analytics_fx import (
    compute_and_store_fx_adjusted_metrics,
    compute_equity_fx_adjusted_metrics,
    compute_fx_adjusted_return,
    get_fx_impact_summary,
    get_normalized_fx_performance,
)
from pakfindata.db import (
    get_fx_adjusted_metrics,
    upsert_fx_adjusted_metric,
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
def db_with_data(db):
    """Database with sample equity and FX data."""
    # Create FX pair
    upsert_fx_pair(db, {
        "pair": "USD/PKR",
        "base_currency": "USD",
        "quote_currency": "PKR",
        "source": "SAMPLE",
    })

    # Create 60 days of FX data (slight depreciation)
    dates = pd.date_range("2024-01-01", periods=60)
    fx_df = pd.DataFrame({
        "date": dates.strftime("%Y-%m-%d"),
        "open": [278.0 + i * 0.02 for i in range(60)],
        "high": [278.5 + i * 0.02 for i in range(60)],
        "low": [277.5 + i * 0.02 for i in range(60)],
        "close": [278.0 + i * 0.03 for i in range(60)],  # Depreciation
        "volume": [None] * 60,
    })
    upsert_fx_ohlcv(db, "USD/PKR", fx_df)

    # Create equity data for test symbol
    from datetime import datetime as dt
    now = dt.now().isoformat()
    equity_df = pd.DataFrame({
        "symbol": ["TEST"] * 60,
        "date": dates.strftime("%Y-%m-%d"),
        "open": [100.0 + i * 0.5 for i in range(60)],
        "high": [101.0 + i * 0.5 for i in range(60)],
        "low": [99.0 + i * 0.5 for i in range(60)],
        "close": [100.0 + i * 0.6 for i in range(60)],  # 6% gain over 60 days
        "volume": [1000000] * 60,
        "ingested_at": [now] * 60,
    })

    # Insert equity data
    cols = ["symbol", "date", "open", "high", "low", "close", "volume", "ingested_at"]
    db.executemany(
        """
        INSERT OR REPLACE INTO eod_ohlcv
            (symbol, date, open, high, low, close, volume, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        equity_df[cols].values.tolist()
    )
    db.commit()

    return db


class TestFXAdjustedMetricStorage:
    """Tests for FX-adjusted metric database operations."""

    def test_upsert_fx_adjusted_metric(self, db):
        """Upsert a single FX-adjusted metric."""
        metric = {
            "as_of_date": "2024-03-01",
            "symbol": "TEST",
            "fx_pair": "USD/PKR",
            "equity_return": 0.05,
            "fx_return": 0.02,
            "fx_adjusted_return": 0.03,
            "period": "1M",
        }

        result = upsert_fx_adjusted_metric(db, metric)
        assert result is True

        # Verify stored
        metrics = get_fx_adjusted_metrics(db, symbol="TEST")
        assert len(metrics) == 1
        assert metrics[0]["equity_return"] == 0.05
        assert metrics[0]["fx_adjusted_return"] == 0.03

    def test_upsert_updates_existing(self, db):
        """Upsert updates existing metric."""
        metric = {
            "as_of_date": "2024-03-01",
            "symbol": "TEST",
            "fx_pair": "USD/PKR",
            "equity_return": 0.05,
            "fx_return": 0.02,
            "fx_adjusted_return": 0.03,
            "period": "1M",
        }
        upsert_fx_adjusted_metric(db, metric)

        # Update with new values
        metric["equity_return"] = 0.08
        metric["fx_adjusted_return"] = 0.06
        upsert_fx_adjusted_metric(db, metric)

        # Should still be only 1 record
        metrics = get_fx_adjusted_metrics(db, symbol="TEST")
        assert len(metrics) == 1
        assert metrics[0]["equity_return"] == 0.08

    def test_get_fx_adjusted_metrics_by_symbol(self, db):
        """Get metrics filtered by symbol."""
        # Insert metrics for two symbols
        for symbol in ["AAAA", "BBBB"]:
            upsert_fx_adjusted_metric(db, {
                "as_of_date": "2024-03-01",
                "symbol": symbol,
                "fx_pair": "USD/PKR",
                "equity_return": 0.05,
                "fx_return": 0.02,
                "fx_adjusted_return": 0.03,
                "period": "1M",
            })

        metrics = get_fx_adjusted_metrics(db, symbol="AAAA")
        assert len(metrics) == 1
        assert metrics[0]["symbol"] == "AAAA"

    def test_get_fx_adjusted_metrics_by_period(self, db):
        """Get metrics filtered by period."""
        # Insert metrics for different periods
        for period in ["1W", "1M", "3M"]:
            upsert_fx_adjusted_metric(db, {
                "as_of_date": "2024-03-01",
                "symbol": "TEST",
                "fx_pair": "USD/PKR",
                "equity_return": 0.05,
                "fx_return": 0.02,
                "fx_adjusted_return": 0.03,
                "period": period,
            })

        metrics = get_fx_adjusted_metrics(db, period="1M")
        assert len(metrics) == 1
        assert metrics[0]["period"] == "1M"


class TestComputeEquityFXAdjustedMetrics:
    """Tests for computing FX-adjusted metrics for equities."""

    def test_compute_metrics_basic(self, db_with_data):
        """Compute FX-adjusted metrics for an equity."""
        metrics = compute_equity_fx_adjusted_metrics(
            db_with_data,
            symbol="TEST",
            fx_pair="USD/PKR",
            periods=["1W", "1M"],
        )

        # Should have metrics for requested periods with enough data
        assert len(metrics) >= 1

        for metric in metrics:
            assert "equity_return" in metric
            assert "fx_return" in metric
            assert "fx_adjusted_return" in metric
            assert metric["symbol"] == "TEST"
            assert metric["fx_pair"] == "USD/PKR"

    def test_compute_metrics_no_equity_data(self, db_with_data):
        """No metrics when equity doesn't exist."""
        metrics = compute_equity_fx_adjusted_metrics(
            db_with_data,
            symbol="NONEXISTENT",
            fx_pair="USD/PKR",
        )
        assert metrics == []

    def test_compute_metrics_no_fx_data(self, db_with_data):
        """No metrics when FX pair doesn't exist."""
        metrics = compute_equity_fx_adjusted_metrics(
            db_with_data,
            symbol="TEST",
            fx_pair="XXX/YYY",
        )
        assert metrics == []


class TestComputeAndStoreFXAdjustedMetrics:
    """Tests for batch compute and store operations."""

    def test_compute_and_store_basic(self, db_with_data):
        """Compute and store metrics for symbols."""
        result = compute_and_store_fx_adjusted_metrics(
            db_with_data,
            symbols=["TEST"],
            fx_pair="USD/PKR",
        )

        assert result["success"] is True
        assert result["symbols_processed"] == 1
        assert result["metrics_stored"] >= 1

        # Verify stored
        metrics = get_fx_adjusted_metrics(db_with_data, symbol="TEST")
        assert len(metrics) >= 1

    def test_compute_and_store_no_symbols(self, db):
        """Handle no symbols case."""
        result = compute_and_store_fx_adjusted_metrics(
            db,
            symbols=[],
            fx_pair="USD/PKR",
        )

        assert result["success"] is False
        assert "no_symbols" in result.get("error", "")


class TestFXImpactSummary:
    """Tests for FX impact summary."""

    def test_get_fx_impact_summary(self, db):
        """Get FX impact summary."""
        # Insert some metrics
        for i, symbol in enumerate(["AAA", "BBB", "CCC"]):
            upsert_fx_adjusted_metric(db, {
                "as_of_date": "2024-03-01",
                "symbol": symbol,
                "fx_pair": "USD/PKR",
                "equity_return": 0.05 + i * 0.01,
                "fx_return": 0.02,
                "fx_adjusted_return": 0.03 + i * 0.01,
                "period": "1M",
            })

        summary = get_fx_impact_summary(db, fx_pair="USD/PKR", period="1M", top_n=2)

        # Should return sorted by fx_adjusted_return (desc)
        assert len(summary) <= 2
        if len(summary) >= 2:
            assert summary[0]["fx_adjusted_return"] >= summary[1]["fx_adjusted_return"]

    def test_get_fx_impact_summary_empty(self, db):
        """Handle empty results."""
        summary = get_fx_impact_summary(db, fx_pair="XXX/YYY", period="1M")
        assert summary == []


class TestNormalizedFXPerformance:
    """Tests for normalized FX performance comparison."""

    def test_get_normalized_performance(self, db_with_data):
        """Get normalized performance for comparison."""
        df = get_normalized_fx_performance(
            db_with_data,
            pairs=["USD/PKR"],
            start_date="2024-01-01",
            end_date="2024-02-01",
            base=100.0,
        )

        assert not df.empty
        assert "USD/PKR" in df.columns
        # First value should be 100 (normalized)
        assert abs(df["USD/PKR"].iloc[0] - 100.0) < 0.01

    def test_get_normalized_performance_no_pairs(self, db):
        """Handle no pairs."""
        df = get_normalized_fx_performance(
            db,
            pairs=["XXX/YYY"],
        )
        assert df.empty


class TestFXAdjustedReturnCalculation:
    """Additional tests for FX-adjusted return edge cases."""

    def test_zero_equity_return(self):
        """Zero equity return with FX depreciation."""
        adjusted = compute_fx_adjusted_return(0.0, 0.02)
        assert adjusted == -0.02  # Lost 2% due to FX

    def test_zero_fx_return(self):
        """Equity return with no FX movement."""
        adjusted = compute_fx_adjusted_return(0.05, 0.0)
        assert adjusted == 0.05  # No FX impact

    def test_negative_equity_positive_fx(self):
        """Negative equity return with depreciation."""
        adjusted = compute_fx_adjusted_return(-0.03, 0.02)
        assert adjusted == -0.05  # Both work against investor

    def test_positive_equity_negative_fx(self):
        """Positive equity with currency appreciation."""
        adjusted = compute_fx_adjusted_return(0.03, -0.02)
        assert adjusted == 0.05  # Both work for investor

    def test_large_values(self):
        """Handle larger return values."""
        adjusted = compute_fx_adjusted_return(0.50, 0.10)
        assert abs(adjusted - 0.40) < 0.0001
