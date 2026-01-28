"""Tests for Phase 1: Instrument universe functionality."""

import pandas as pd
import pytest

from psx_ohlcv import connect, init_schema
from psx_ohlcv.db import (
    get_instruments,
    upsert_instrument,
    upsert_ohlcv_instrument,
    get_ohlcv_instrument,
    get_instrument_latest_date,
    upsert_instrument_ranking,
    get_instrument_rankings,
)
from psx_ohlcv.analytics_phase1 import (
    compute_returns,
    compute_volatility,
    compute_all_metrics,
)


@pytest.fixture
def db():
    """Create an in-memory database for testing."""
    con = connect(":memory:")
    init_schema(con)
    yield con
    con.close()


class TestInstrumentSchema:
    """Tests for Phase 1 schema tables."""

    def test_creates_instruments_table(self, db):
        """Verify instruments table is created."""
        cur = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='instruments'"
        )
        assert cur.fetchone() is not None

    def test_creates_ohlcv_instruments_table(self, db):
        """Verify ohlcv_instruments table is created."""
        cur = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='ohlcv_instruments'"
        )
        assert cur.fetchone() is not None

    def test_creates_instrument_rankings_table(self, db):
        """Verify instrument_rankings table is created."""
        cur = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='instrument_rankings'"
        )
        assert cur.fetchone() is not None


class TestInstrumentUpsert:
    """Tests for instrument upsert operations."""

    def test_insert_new_instrument(self, db):
        """Insert a new instrument."""
        instrument = {
            "instrument_id": "PSX:NIUETF",
            "symbol": "NIUETF",
            "name": "NIT Islamic Equity Fund",
            "instrument_type": "ETF",
            "exchange": "PSX",
            "currency": "PKR",
            "is_active": 1,
            "source": "DPS",
        }
        result = upsert_instrument(db, instrument)
        assert result is True

        # Verify it was inserted
        instruments = get_instruments(db, instrument_type="ETF")
        assert len(instruments) == 1
        assert instruments[0]["symbol"] == "NIUETF"
        assert instruments[0]["name"] == "NIT Islamic Equity Fund"

    def test_update_existing_instrument(self, db):
        """Update an existing instrument."""
        instrument = {
            "instrument_id": "PSX:NIUETF",
            "symbol": "NIUETF",
            "name": "NIT Islamic Equity Fund",
            "instrument_type": "ETF",
            "exchange": "PSX",
            "currency": "PKR",
            "is_active": 1,
            "source": "DPS",
        }
        upsert_instrument(db, instrument)

        # Update the name
        instrument["name"] = "NIT Islamic ETF - Updated"
        upsert_instrument(db, instrument)

        instruments = get_instruments(db, instrument_type="ETF")
        assert len(instruments) == 1
        assert instruments[0]["name"] == "NIT Islamic ETF - Updated"

    def test_get_instruments_by_type(self, db):
        """Get instruments filtered by type."""
        # Insert different types
        instruments = [
            {"instrument_id": "PSX:NIUETF", "symbol": "NIUETF", "name": "ETF 1",
             "instrument_type": "ETF", "exchange": "PSX", "currency": "PKR",
             "is_active": 1, "source": "DPS"},
            {"instrument_id": "PSX:DCR", "symbol": "DCR", "name": "REIT 1",
             "instrument_type": "REIT", "exchange": "PSX", "currency": "PKR",
             "is_active": 1, "source": "DPS"},
            {"instrument_id": "IDX:KSE100", "symbol": "KSE100", "name": "Index 1",
             "instrument_type": "INDEX", "exchange": "PSX", "currency": "PKR",
             "is_active": 1, "source": "DPS"},
        ]
        for inst in instruments:
            upsert_instrument(db, inst)

        # Filter by type
        etfs = get_instruments(db, instrument_type="ETF")
        assert len(etfs) == 1
        assert etfs[0]["symbol"] == "NIUETF"

        reits = get_instruments(db, instrument_type="REIT")
        assert len(reits) == 1
        assert reits[0]["symbol"] == "DCR"

        indexes = get_instruments(db, instrument_type="INDEX")
        assert len(indexes) == 1
        assert indexes[0]["symbol"] == "KSE100"

        # Get all
        all_instruments = get_instruments(db)
        assert len(all_instruments) == 3


class TestOHLCVInstrument:
    """Tests for instrument OHLCV data operations."""

    def test_upsert_ohlcv_data(self, db):
        """Upsert OHLCV data for an instrument."""
        # First create the instrument
        instrument = {
            "instrument_id": "PSX:NIUETF",
            "symbol": "NIUETF",
            "name": "NIT ETF",
            "instrument_type": "ETF",
            "exchange": "PSX",
            "currency": "PKR",
            "is_active": 1,
            "source": "DPS",
        }
        upsert_instrument(db, instrument)

        # Create OHLCV data
        df = pd.DataFrame([
            {"date": "2024-01-15", "open": 10.0, "high": 11.0, "low": 9.5,
             "close": 10.5, "volume": 100000},
            {"date": "2024-01-16", "open": 10.5, "high": 12.0, "low": 10.0,
             "close": 11.5, "volume": 150000},
        ])

        rows = upsert_ohlcv_instrument(db, "PSX:NIUETF", df)
        assert rows == 2

        # Verify data
        result_df = get_ohlcv_instrument(db, "PSX:NIUETF")
        assert len(result_df) == 2

    def test_get_instrument_latest_date(self, db):
        """Get latest date for instrument OHLCV data."""
        # Create instrument and data
        instrument = {
            "instrument_id": "PSX:NIUETF",
            "symbol": "NIUETF",
            "name": "NIT ETF",
            "instrument_type": "ETF",
            "exchange": "PSX",
            "currency": "PKR",
            "is_active": 1,
            "source": "DPS",
        }
        upsert_instrument(db, instrument)

        df = pd.DataFrame([
            {"date": "2024-01-15", "open": 10.0, "high": 11.0, "low": 9.5,
             "close": 10.5, "volume": 100000},
            {"date": "2024-01-16", "open": 10.5, "high": 12.0, "low": 10.0,
             "close": 11.5, "volume": 150000},
        ])
        upsert_ohlcv_instrument(db, "PSX:NIUETF", df)

        latest = get_instrument_latest_date(db, "PSX:NIUETF")
        assert latest == "2024-01-16"


class TestInstrumentRankings:
    """Tests for instrument rankings operations."""

    def test_upsert_ranking(self, db):
        """Upsert an instrument ranking."""
        # Create instrument first
        instrument = {
            "instrument_id": "PSX:NIUETF",
            "symbol": "NIUETF",
            "name": "NIT ETF",
            "instrument_type": "ETF",
            "exchange": "PSX",
            "currency": "PKR",
            "is_active": 1,
            "source": "DPS",
        }
        upsert_instrument(db, instrument)

        ranking = {
            "as_of_date": "2024-01-16",
            "instrument_id": "PSX:NIUETF",
            "instrument_type": "ETF",
            "return_1m": 0.05,
            "return_3m": 0.12,
            "return_6m": None,
            "return_1y": None,
            "volatility_30d": 0.15,
            "relative_strength": 0.02,
        }
        result = upsert_instrument_ranking(db, ranking)
        assert result is True

        # Verify
        rankings = get_instrument_rankings(db, as_of_date="2024-01-16")
        assert len(rankings) == 1
        assert rankings[0]["return_1m"] == 0.05

    def test_get_rankings_by_type(self, db):
        """Get rankings filtered by instrument type."""
        # Create instruments
        instruments = [
            {"instrument_id": "PSX:NIUETF", "symbol": "NIUETF", "name": "ETF 1",
             "instrument_type": "ETF", "exchange": "PSX", "currency": "PKR",
             "is_active": 1, "source": "DPS"},
            {"instrument_id": "PSX:DCR", "symbol": "DCR", "name": "REIT 1",
             "instrument_type": "REIT", "exchange": "PSX", "currency": "PKR",
             "is_active": 1, "source": "DPS"},
        ]
        for inst in instruments:
            upsert_instrument(db, inst)

        # Create rankings
        rankings = [
            {"as_of_date": "2024-01-16", "instrument_id": "PSX:NIUETF",
             "instrument_type": "ETF", "return_1m": 0.05, "return_3m": 0.12,
             "return_6m": None, "return_1y": None, "volatility_30d": 0.15,
             "relative_strength": 0.02},
            {"as_of_date": "2024-01-16", "instrument_id": "PSX:DCR",
             "instrument_type": "REIT", "return_1m": 0.08, "return_3m": 0.18,
             "return_6m": None, "return_1y": None, "volatility_30d": 0.20,
             "relative_strength": 0.05},
        ]
        for r in rankings:
            upsert_instrument_ranking(db, r)

        # Get by type
        etf_rankings = get_instrument_rankings(
            db, as_of_date="2024-01-16", instrument_type="ETF"
        )
        assert len(etf_rankings) == 1
        assert etf_rankings[0]["symbol"] == "NIUETF"


class TestAnalytics:
    """Tests for Phase 1 analytics functions."""

    def test_compute_returns_basic(self):
        """Compute returns from OHLCV data."""
        # Create 30 days of data with simple price increase
        dates = pd.date_range("2024-01-01", periods=30)
        prices = [100 + i for i in range(30)]  # 100 to 129
        df = pd.DataFrame({
            "date": dates.strftime("%Y-%m-%d"),
            "close": prices,
        })
        # Sort descending as expected by compute_returns
        df = df.sort_values("date", ascending=False)

        returns = compute_returns(df, periods=[1, 5])

        # 1-day return: (129 - 128) / 128 * 100 = 0.78%
        assert "return_1d" in returns
        assert abs(returns["return_1d"] - 0.78125) < 0.01

    def test_compute_returns_empty_df(self):
        """Compute returns with empty DataFrame."""
        df = pd.DataFrame()
        returns = compute_returns(df)
        assert returns == {}

    def test_compute_volatility_basic(self):
        """Compute volatility from OHLCV data."""
        # Create data with some variance
        dates = pd.date_range("2024-01-01", periods=30)
        prices = [100 + (i % 5) for i in range(30)]  # Oscillating prices
        df = pd.DataFrame({
            "date": dates.strftime("%Y-%m-%d"),
            "close": prices,
        })

        volatility = compute_volatility(df, windows=[21])

        # Should have volatility calculated
        assert "vol_1m" in volatility
        assert volatility["vol_1m"] > 0  # Should be positive

    def test_compute_all_metrics(self, db):
        """Compute all metrics for an instrument."""
        # Create instrument and data
        instrument = {
            "instrument_id": "PSX:NIUETF",
            "symbol": "NIUETF",
            "name": "NIT ETF",
            "instrument_type": "ETF",
            "exchange": "PSX",
            "currency": "PKR",
            "is_active": 1,
            "source": "DPS",
        }
        upsert_instrument(db, instrument)

        # Create 60 days of OHLCV data
        dates = pd.date_range("2024-01-01", periods=60)
        df = pd.DataFrame({
            "date": dates.strftime("%Y-%m-%d"),
            "open": [100 + i * 0.1 for i in range(60)],
            "high": [101 + i * 0.1 for i in range(60)],
            "low": [99 + i * 0.1 for i in range(60)],
            "close": [100 + i * 0.15 for i in range(60)],
            "volume": [100000] * 60,
        })
        upsert_ohlcv_instrument(db, "PSX:NIUETF", df)

        # Compute metrics
        metrics = compute_all_metrics(db, "PSX:NIUETF", benchmark_id=None)

        assert "instrument_id" in metrics
        assert metrics["instrument_id"] == "PSX:NIUETF"
        # Should have return metrics
        assert "return_1m" in metrics or "return_1w" in metrics
