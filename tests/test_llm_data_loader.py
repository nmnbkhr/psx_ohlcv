"""Tests for LLM data loader and payload bounding.

Verifies:
- Data extraction from database
- Payload bounding (max rows)
- Downsampling behavior
- Format functions for prompts
"""

import sqlite3
from datetime import datetime, timedelta

import pandas as pd
import pytest

from psx_ohlcv.agents.data_loader import (
    DataLoader,
    CompanyData,
    IntradayData,
    MarketData,
    DataProvenance,
    format_data_for_prompt,
    MAX_OHLCV_ROWS,
    TARGET_OHLCV_ROWS,
    MAX_INTRADAY_POINTS,
    TARGET_INTRADAY_POINTS,
)


@pytest.fixture
def db():
    """Create in-memory database with test data."""
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row

    # Create schema
    con.executescript("""
        CREATE TABLE company_snapshots (
            symbol TEXT,
            snapshot_date TEXT,
            company_name TEXT,
            sector_name TEXT,
            profile_data TEXT,
            quote_data TEXT,
            trading_data TEXT,
            equity_data TEXT,
            financials_data TEXT,
            ratios_data TEXT
        );

        CREATE TABLE trading_sessions (
            symbol TEXT,
            session_date TEXT,
            market_type TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            turnover REAL,
            change_value REAL,
            change_percent REAL,
            ldcp REAL
        );

        CREATE TABLE eod_ohlcv (
            symbol TEXT,
            date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER
        );

        CREATE TABLE intraday_bars (
            symbol TEXT NOT NULL,
            ts TEXT NOT NULL,
            ts_epoch INTEGER NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            interval TEXT NOT NULL DEFAULT 'int',
            ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (symbol, ts)
        );

        CREATE TABLE psx_indices (
            index_code TEXT,
            index_date TEXT,
            value REAL,
            change REAL,
            change_pct REAL,
            volume INTEGER
        );
    """)

    yield con
    con.close()


@pytest.fixture
def populated_db(db):
    """Create database with sample data."""
    # Add company snapshot
    db.execute(
        """
        INSERT INTO company_snapshots
        (symbol, snapshot_date, company_name, sector_name, profile_data, quote_data)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            "OGDC",
            "2024-01-15",
            "Oil & Gas Development Co",
            "OIL & GAS EXPLORATION",
            '{"description": "Test company"}',
            '{"close": 150.0, "volume": 1000000}',
        ),
    )

    # Add trading sessions
    for i in range(10):
        date = f"2024-01-{15-i:02d}"
        db.execute(
            """
            INSERT INTO trading_sessions
            (symbol, session_date, market_type, open, high, low, close, volume, change_percent)
            VALUES (?, ?, 'REG', ?, ?, ?, ?, ?, ?)
            """,
            ("OGDC", date, 100 + i, 102 + i, 99 + i, 101 + i, 1000000 + i * 10000, 0.5 - i * 0.1),
        )

    # Add EOD OHLCV
    for i in range(30):
        date = f"2024-01-{15-i if i < 15 else (i-14):02d}"
        if i >= 15:
            date = f"2023-12-{31-(i-15):02d}"
        db.execute(
            """
            INSERT INTO eod_ohlcv
            (symbol, date, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("OGDC", date, 100 + i, 102 + i, 99 + i, 101 + i, 1000000),
        )

    # Add intraday bars
    for i in range(100):
        ts = f"2024-01-15 09:{30 + i // 2:02d}:{(i % 2) * 30:02d}"
        ts_epoch = 1705309800 + i * 30  # base epoch + 30s increments
        db.execute(
            """
            INSERT INTO intraday_bars
            (symbol, ts, ts_epoch, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("OGDC", ts, ts_epoch, 100.0 + i * 0.1, 100.5 + i * 0.1, 99.5 + i * 0.1, 100.2 + i * 0.1, 10000),
        )

    db.commit()
    return db


class TestDataLoader:
    """Tests for DataLoader class."""

    def test_load_company_data_empty_db(self, db):
        """Should handle empty database gracefully."""
        loader = DataLoader(db)
        data = loader.load_company_data("UNKNOWN")

        assert isinstance(data, CompanyData)
        assert data.symbol == "UNKNOWN"
        assert data.company_name == ""
        assert data.ohlcv.empty

    def test_load_company_data_with_data(self, populated_db):
        """Should load company data correctly."""
        loader = DataLoader(populated_db)
        data = loader.load_company_data("OGDC", ohlcv_days=30)

        assert data.symbol == "OGDC"
        assert data.company_name == "Oil & Gas Development Co"
        assert data.sector == "OIL & GAS EXPLORATION"
        assert not data.ohlcv.empty
        assert len(data.ohlcv) <= 30

    def test_load_company_data_provenance(self, populated_db):
        """Should track data provenance correctly."""
        loader = DataLoader(populated_db)
        data = loader.load_company_data("OGDC")

        assert data.provenance.row_count > 0
        assert "eod_ohlcv" in data.provenance.tables_used
        assert data.provenance.date_range[0] is not None

    def test_load_intraday_data_empty_db(self, db):
        """Should handle missing intraday data gracefully."""
        loader = DataLoader(db)
        data = loader.load_intraday_data("UNKNOWN", "2024-01-15")

        assert isinstance(data, IntradayData)
        assert data.bars.empty

    def test_load_intraday_data_with_data(self, populated_db):
        """Should load intraday data correctly."""
        loader = DataLoader(populated_db)
        data = loader.load_intraday_data("OGDC", "2024-01-15")

        assert data.symbol == "OGDC"
        assert data.trading_date == "2024-01-15"
        assert not data.bars.empty
        assert "timestamp" in data.bars.columns

    def test_load_intraday_calculates_vwap(self, populated_db):
        """Should calculate VWAP from intraday bars."""
        loader = DataLoader(populated_db)
        data = loader.load_intraday_data("OGDC", "2024-01-15")

        # VWAP should be calculated if volume data exists
        assert data.vwap is not None or data.bars.empty

    def test_load_market_data(self, populated_db):
        """Should load market data correctly."""
        loader = DataLoader(populated_db)
        data = loader.load_market_data("2024-01-15")

        assert isinstance(data, MarketData)
        assert data.market_date == "2024-01-15"


class TestPayloadBounding:
    """Tests for payload bounding and downsampling."""

    def test_ohlcv_bounded_by_days(self, populated_db):
        """OHLCV should be bounded by requested days."""
        loader = DataLoader(populated_db)

        data_5 = loader.load_company_data("OGDC", ohlcv_days=5)
        data_30 = loader.load_company_data("OGDC", ohlcv_days=30)

        assert len(data_5.ohlcv) <= 5
        assert len(data_30.ohlcv) <= 30

    def test_downsampling_triggers_on_large_data(self, db):
        """Should downsample when data exceeds max rows."""
        # Create large dataset
        for i in range(MAX_OHLCV_ROWS + 100):
            date = f"2020-01-01"  # Simplified for test
            db.execute(
                """
                INSERT INTO eod_ohlcv
                (symbol, date, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (f"TEST", f"date_{i}", 100.0, 101.0, 99.0, 100.5, 1000000),
            )
        db.commit()

        loader = DataLoader(db)
        data = loader.load_company_data("TEST", ohlcv_days=MAX_OHLCV_ROWS + 100)

        # Should be downsampled
        assert data.provenance.was_downsampled or len(data.ohlcv) <= MAX_OHLCV_ROWS

    def test_downsampling_preserves_structure(self, db):
        """Downsampled data should preserve OHLCV column structure."""
        # Create data
        for i in range(100):
            db.execute(
                """
                INSERT INTO eod_ohlcv
                (symbol, date, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("TEST", f"2024-01-{i:02d}", 100.0, 101.0, 99.0, 100.5, 1000000),
            )
        db.commit()

        loader = DataLoader(db)
        data = loader.load_company_data("TEST", ohlcv_days=100)

        # Should have all required columns
        required_cols = ["date", "open", "high", "low", "close", "volume"]
        for col in required_cols:
            assert col in data.ohlcv.columns


class TestDataContainers:
    """Tests for data container classes."""

    def test_company_data_defaults(self):
        """CompanyData should have sensible defaults."""
        data = CompanyData(symbol="TEST")

        assert data.symbol == "TEST"
        assert data.company_name == ""
        assert data.sector == ""
        assert isinstance(data.profile, dict)
        assert isinstance(data.ohlcv, pd.DataFrame)
        assert isinstance(data.provenance, DataProvenance)

    def test_intraday_data_defaults(self):
        """IntradayData should have sensible defaults."""
        data = IntradayData(symbol="TEST")

        assert data.symbol == "TEST"
        assert data.trading_date == ""
        assert isinstance(data.bars, pd.DataFrame)
        assert data.vwap is None

    def test_market_data_defaults(self):
        """MarketData should have sensible defaults."""
        data = MarketData()

        assert data.market_date == ""
        assert isinstance(data.gainers, pd.DataFrame)
        assert isinstance(data.losers, pd.DataFrame)
        assert isinstance(data.breadth, dict)

    def test_provenance_to_dict(self):
        """Provenance should serialize to dict."""
        prov = DataProvenance(
            tables_used=["eod_ohlcv", "trading_sessions"],
            row_count=100,
            date_range=("2024-01-01", "2024-01-31"),
            was_downsampled=True,
            original_row_count=500,
        )

        d = prov.to_dict()

        assert d["tables_used"] == ["eod_ohlcv", "trading_sessions"]
        assert d["row_count"] == 100
        assert d["was_downsampled"] is True
        assert d["original_row_count"] == 500


class TestFormatDataForPrompt:
    """Tests for format_data_for_prompt function."""

    def test_format_company_data(self, populated_db):
        """Should format company data for prompt."""
        loader = DataLoader(populated_db)
        data = loader.load_company_data("OGDC")

        formatted = format_data_for_prompt(data)

        assert "symbol" in formatted
        assert formatted["symbol"] == "OGDC"
        assert "company_name" in formatted
        assert "ohlcv_data" in formatted

    def test_format_intraday_data(self, populated_db):
        """Should format intraday data for prompt."""
        loader = DataLoader(populated_db)
        data = loader.load_intraday_data("OGDC", "2024-01-15")

        formatted = format_data_for_prompt(data)

        assert "symbol" in formatted
        assert "trading_date" in formatted
        assert "intraday_data" in formatted

    def test_format_market_data(self, populated_db):
        """Should format market data for prompt."""
        loader = DataLoader(populated_db)
        data = loader.load_market_data("2024-01-15")

        formatted = format_data_for_prompt(data)

        assert "market_date" in formatted
        assert "breadth_data" in formatted

    def test_format_handles_empty_data(self):
        """Should handle empty data gracefully."""
        data = CompanyData(symbol="EMPTY")

        formatted = format_data_for_prompt(data)

        assert formatted["symbol"] == "EMPTY"
        assert "No" in formatted["ohlcv_data"] or "N/A" in formatted["ohlcv_data"]
