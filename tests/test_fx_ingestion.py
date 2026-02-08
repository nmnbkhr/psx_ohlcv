"""Tests for Phase 2: FX data ingestion."""

import pandas as pd
import pytest

from psx_ohlcv import connect, init_schema
from psx_ohlcv.db import (
    get_fx_latest_date,
    get_fx_latest_rate,
    get_fx_ohlcv,
    get_fx_pair,
    get_fx_pairs,
    upsert_fx_ohlcv,
    upsert_fx_pair,
)
from psx_ohlcv.sources.fx import (
    fetch_fx_sample_data,
    get_default_fx_pairs,
    normalize_fx_dataframe,
)


@pytest.fixture
def db():
    """Create an in-memory database for testing."""
    con = connect(":memory:")
    init_schema(con)
    yield con
    con.close()


class TestFXSchema:
    """Tests for Phase 2 FX schema tables."""

    def test_creates_fx_pairs_table(self, db):
        """Verify fx_pairs table is created."""
        cur = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='fx_pairs'"
        )
        assert cur.fetchone() is not None

    def test_creates_fx_ohlcv_table(self, db):
        """Verify fx_ohlcv table is created."""
        cur = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='fx_ohlcv'"
        )
        assert cur.fetchone() is not None

    def test_creates_fx_adjusted_metrics_table(self, db):
        """Verify fx_adjusted_metrics table is created."""
        cur = db.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='fx_adjusted_metrics'"
        )
        assert cur.fetchone() is not None


class TestFXPairOperations:
    """Tests for FX pair CRUD operations."""

    def test_insert_fx_pair(self, db):
        """Insert a new FX pair."""
        pair_data = {
            "pair": "USD/PKR",
            "base_currency": "USD",
            "quote_currency": "PKR",
            "source": "SBP",
            "description": "US Dollar to PKR",
            "is_active": 1,
        }
        result = upsert_fx_pair(db, pair_data)
        assert result is True

        pairs = get_fx_pairs(db)
        assert len(pairs) == 1
        assert pairs[0]["pair"] == "USD/PKR"
        assert pairs[0]["base_currency"] == "USD"

    def test_update_fx_pair(self, db):
        """Update an existing FX pair."""
        pair_data = {
            "pair": "USD/PKR",
            "base_currency": "USD",
            "quote_currency": "PKR",
            "source": "SBP",
        }
        upsert_fx_pair(db, pair_data)

        # Update source
        pair_data["source"] = "OPEN_API"
        upsert_fx_pair(db, pair_data)

        pair = get_fx_pair(db, "USD/PKR")
        assert pair["source"] == "OPEN_API"

    def test_get_active_pairs_only(self, db):
        """Get only active FX pairs."""
        pairs = [
            {"pair": "USD/PKR", "base_currency": "USD", "quote_currency": "PKR",
             "source": "SBP", "is_active": 1},
            {"pair": "EUR/PKR", "base_currency": "EUR", "quote_currency": "PKR",
             "source": "SBP", "is_active": 0},
        ]
        for p in pairs:
            upsert_fx_pair(db, p)

        active = get_fx_pairs(db, active_only=True)
        assert len(active) == 1
        assert active[0]["pair"] == "USD/PKR"

        all_pairs = get_fx_pairs(db, active_only=False)
        assert len(all_pairs) == 2


class TestFXOHLCVOperations:
    """Tests for FX OHLCV data operations."""

    def test_upsert_fx_ohlcv(self, db):
        """Upsert FX OHLCV data."""
        # First create the pair
        upsert_fx_pair(db, {
            "pair": "USD/PKR",
            "base_currency": "USD",
            "quote_currency": "PKR",
            "source": "SBP",
        })

        # Create OHLCV data
        df = pd.DataFrame([
            {"date": "2024-01-15", "open": 278.0, "high": 279.0,
             "low": 277.5, "close": 278.5, "volume": None},
            {"date": "2024-01-16", "open": 278.5, "high": 280.0,
             "low": 278.0, "close": 279.5, "volume": None},
        ])

        rows = upsert_fx_ohlcv(db, "USD/PKR", df)
        assert rows == 2

        # Verify data
        result_df = get_fx_ohlcv(db, "USD/PKR")
        assert len(result_df) == 2

    def test_get_fx_latest_date(self, db):
        """Get latest date for FX pair."""
        upsert_fx_pair(db, {
            "pair": "USD/PKR", "base_currency": "USD",
            "quote_currency": "PKR", "source": "SBP",
        })

        df = pd.DataFrame([
            {"date": "2024-01-15", "open": 278.0, "high": 279.0,
             "low": 277.5, "close": 278.5, "volume": None},
            {"date": "2024-01-16", "open": 278.5, "high": 280.0,
             "low": 278.0, "close": 279.5, "volume": None},
        ])
        upsert_fx_ohlcv(db, "USD/PKR", df)

        latest = get_fx_latest_date(db, "USD/PKR")
        assert latest == "2024-01-16"

    def test_get_fx_latest_rate(self, db):
        """Get latest FX rate."""
        upsert_fx_pair(db, {
            "pair": "USD/PKR", "base_currency": "USD",
            "quote_currency": "PKR", "source": "SBP",
        })

        df = pd.DataFrame([
            {"date": "2024-01-15", "open": 278.0, "high": 279.0,
             "low": 277.5, "close": 278.5, "volume": None},
            {"date": "2024-01-16", "open": 278.5, "high": 280.0,
             "low": 278.0, "close": 279.5, "volume": None},
        ])
        upsert_fx_ohlcv(db, "USD/PKR", df)

        latest = get_fx_latest_rate(db, "USD/PKR")
        assert latest is not None
        assert latest["close"] == 279.5


class TestFXSampleData:
    """Tests for FX sample data generation."""

    def test_fetch_sample_data(self):
        """Fetch sample FX data."""
        df = fetch_fx_sample_data("USD/PKR")
        assert not df.empty
        assert "date" in df.columns
        assert "close" in df.columns
        assert len(df) > 0

    def test_sample_data_date_range(self):
        """Sample data respects date range."""
        df = fetch_fx_sample_data(
            "USD/PKR",
            start_date="2024-01-01",
            end_date="2024-01-31"
        )
        assert not df.empty
        # All dates should be in January 2024
        dates = pd.to_datetime(df["date"])
        assert dates.min().month == 1
        assert dates.max().month == 1

    def test_normalize_fx_dataframe(self):
        """Normalize FX DataFrame."""
        # DataFrame with only date and close
        df = pd.DataFrame([
            {"date": "2024-01-15", "close": 278.5},
            {"date": "2024-01-16", "close": 279.5},
        ])

        normalized = normalize_fx_dataframe(df)
        assert "open" in normalized.columns
        assert "high" in normalized.columns
        assert "low" in normalized.columns
        # Open/high/low should be filled with close
        assert normalized.iloc[0]["open"] == 278.5


class TestDefaultFXPairs:
    """Tests for default FX pairs configuration."""

    def test_default_pairs_exist(self):
        """Default pairs are defined."""
        pairs = get_default_fx_pairs()
        assert len(pairs) > 0

    def test_default_pairs_have_usd_pkr(self):
        """USD/PKR is in default pairs."""
        pairs = get_default_fx_pairs()
        pair_names = [p["pair"] for p in pairs]
        assert "USD/PKR" in pair_names

    def test_default_pairs_structure(self):
        """Default pairs have correct structure."""
        pairs = get_default_fx_pairs()
        for pair in pairs:
            assert "pair" in pair
            assert "base_currency" in pair
            assert "quote_currency" in pair
            assert "source" in pair
