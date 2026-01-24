"""Tests for company analytics signals computation."""

import sqlite3

import pandas as pd
import pytest

from psx_ohlcv.company_analytics import (
    VOLUME_SPIKE_THRESHOLD,
    compute_and_persist_signals,
    compute_company_signals,
    get_company_signals,
    get_signals_history,
    persist_company_signals,
)
from psx_ohlcv.db import init_schema


@pytest.fixture
def con() -> sqlite3.Connection:
    """Create in-memory database with schema."""
    conn = sqlite3.connect(":memory:")
    init_schema(conn)
    return conn


class TestComputeCompanySignals:
    """Test signal computation logic."""

    def test_52_week_position_at_high(self):
        """Test position at 52-week high."""
        quote = {
            "price": 100.0,
            "wk52_low": 50.0,
            "wk52_high": 100.0,
        }
        signals = compute_company_signals(quote)
        assert signals["pos_52w"] == "1.0000"
        assert signals["near_52w_high"] == "true"
        assert signals["near_52w_low"] == "false"

    def test_52_week_position_at_low(self):
        """Test position at 52-week low."""
        quote = {
            "price": 50.0,
            "wk52_low": 50.0,
            "wk52_high": 100.0,
        }
        signals = compute_company_signals(quote)
        assert signals["pos_52w"] == "0.0000"
        assert signals["near_52w_high"] == "false"
        assert signals["near_52w_low"] == "true"

    def test_52_week_position_mid_range(self):
        """Test position in middle of 52-week range."""
        quote = {
            "price": 75.0,
            "wk52_low": 50.0,
            "wk52_high": 100.0,
        }
        signals = compute_company_signals(quote)
        assert signals["pos_52w"] == "0.5000"
        assert signals["near_52w_high"] == "false"
        assert signals["near_52w_low"] == "false"

    def test_52_week_position_near_threshold(self):
        """Test position near threshold boundaries."""
        # At 90% (exactly NEAR_HIGH_THRESHOLD)
        quote = {
            "price": 95.0,
            "wk52_low": 50.0,
            "wk52_high": 100.0,
        }
        signals = compute_company_signals(quote)
        # 95 - 50 = 45 / 50 = 0.9
        assert signals["near_52w_high"] == "true"

        # At 10% (exactly NEAR_LOW_THRESHOLD)
        quote2 = {
            "price": 55.0,
            "wk52_low": 50.0,
            "wk52_high": 100.0,
        }
        signals2 = compute_company_signals(quote2)
        # 55 - 50 = 5 / 50 = 0.1
        assert signals2["near_52w_low"] == "true"

    def test_day_range_position(self):
        """Test day range position calculation."""
        quote = {
            "price": 100.0,
            "day_range_low": 95.0,
            "day_range_high": 105.0,
        }
        signals = compute_company_signals(quote)
        # 100 - 95 = 5 / 10 = 0.5
        assert signals["pos_day"] == "0.5000"

    def test_circuit_breaker_proximity(self):
        """Test circuit breaker proximity calculation."""
        quote = {
            "price": 100.0,
            "circuit_low": 95.0,
            "circuit_high": 105.0,
        }
        signals = compute_company_signals(quote)
        # High proximity: (105 - 100) / 100 * 100 = 5%
        assert signals["circuit_prox_high_pct"] == "5.0000"
        # Low proximity: (100 - 95) / 100 * 100 = 5%
        assert signals["circuit_prox_low_pct"] == "5.0000"

    def test_circuit_proximity_triggers_signal(self):
        """Test that being within 2% of circuit triggers signal."""
        quote = {
            "price": 100.0,
            "circuit_low": 99.0,  # 1% away
            "circuit_high": 101.5,  # 1.5% away
        }
        signals = compute_company_signals(quote)
        signal_summary = signals["signal_summary"]
        assert "near_circuit_low" in signal_summary
        assert "near_circuit_high" in signal_summary

    def test_volume_spike_detection(self):
        """Test volume spike detection."""
        quote = {"price": 100.0, "volume": 10000}
        # History with median volume of 5000
        history_df = pd.DataFrame({
            "volume": [5000, 4500, 5500, 4800, 5200] * 4  # 20 values
        })
        signals = compute_company_signals(quote, history_df)
        # 10000 / 5000 = 2.0 = VOLUME_SPIKE_THRESHOLD
        assert float(signals["rel_volume"]) >= VOLUME_SPIKE_THRESHOLD
        assert signals["volume_spike"] == "true"

    def test_no_volume_spike(self):
        """Test no volume spike when volume is normal."""
        quote = {"price": 100.0, "volume": 5000}
        history_df = pd.DataFrame({
            "volume": [5000, 5500, 4500, 5200, 4800] * 4
        })
        signals = compute_company_signals(quote, history_df)
        assert float(signals["rel_volume"]) < VOLUME_SPIKE_THRESHOLD
        assert signals["volume_spike"] == "false"

    def test_missing_volume_history(self):
        """Test handling of missing volume history."""
        quote = {"price": 100.0, "volume": 10000}
        signals = compute_company_signals(quote, None)
        assert signals["rel_volume"] == "null"
        assert signals["volume_spike"] == "null"

    def test_missing_price_data(self):
        """Test handling of missing price data."""
        quote = {}  # Empty quote
        signals = compute_company_signals(quote)
        assert signals["pos_52w"] == "null"
        assert signals["pos_day"] == "null"
        assert signals["near_52w_high"] == "null"

    def test_signal_summary_multiple_signals(self):
        """Test signal summary with multiple triggered signals."""
        quote = {
            "price": 100.0,
            "wk52_low": 50.0,
            "wk52_high": 101.0,  # Near high
            "day_range_low": 50.0,
            "day_range_high": 101.0,  # Near high
            "circuit_low": 99.0,  # Within 2%
            "circuit_high": 101.0,  # Within 2%
            "volume": 10000,
        }
        history_df = pd.DataFrame({
            "volume": [5000] * 10
        })
        signals = compute_company_signals(quote, history_df)
        summary = signals["signal_summary"]
        assert "near_52w_high" in summary
        assert "near_day_high" in summary
        assert "volume_spike" in summary

    def test_signal_summary_no_signals(self):
        """Test signal summary when no signals triggered."""
        quote = {
            "price": 75.0,
            "wk52_low": 50.0,
            "wk52_high": 100.0,
            "day_range_low": 70.0,
            "day_range_high": 80.0,
            "circuit_low": 60.0,
            "circuit_high": 90.0,
        }
        signals = compute_company_signals(quote)
        assert signals["signal_summary"] == "none"

    def test_zero_range_handling(self):
        """Test handling of zero range (same low and high)."""
        quote = {
            "price": 100.0,
            "wk52_low": 100.0,
            "wk52_high": 100.0,  # Zero range
        }
        signals = compute_company_signals(quote)
        # Should return null when range is zero
        assert signals["pos_52w"] == "null"


class TestPersistCompanySignals:
    """Test signal persistence to database."""

    def test_persist_signals(self, con):
        """Test persisting signals to database."""
        signals = {
            "pos_52w": "0.5000",
            "near_52w_high": "false",
            "volume_spike": "true",
        }
        count = persist_company_signals(con, "OGDC", "2025-01-15 10:00:00", signals)
        assert count == 3

        # Verify in database
        cur = con.execute(
            "SELECT signal_key, signal_value FROM company_signal_snapshots "
            "WHERE symbol = 'OGDC' ORDER BY signal_key"
        )
        rows = cur.fetchall()
        assert len(rows) == 3
        # Check values
        row_dict = {r[0]: r[1] for r in rows}
        assert row_dict["pos_52w"] == "0.5000"
        assert row_dict["near_52w_high"] == "false"
        assert row_dict["volume_spike"] == "true"

    def test_persist_signals_upsert(self, con):
        """Test that persisting signals updates existing values."""
        # First insert
        signals1 = {"pos_52w": "0.5000", "volume_spike": "false"}
        persist_company_signals(con, "OGDC", "2025-01-15 10:00:00", signals1)

        # Second insert with different values (same ts)
        signals2 = {"pos_52w": "0.6000", "volume_spike": "true"}
        persist_company_signals(con, "OGDC", "2025-01-15 10:00:00", signals2)

        # Should have updated values
        cur = con.execute(
            "SELECT signal_value FROM company_signal_snapshots "
            "WHERE symbol = 'OGDC' AND signal_key = 'pos_52w'"
        )
        assert cur.fetchone()[0] == "0.6000"


class TestGetCompanySignals:
    """Test signal retrieval."""

    def test_get_signals_for_timestamp(self, con):
        """Test getting signals for a specific timestamp."""
        signals = {"pos_52w": "0.5000", "volume_spike": "true"}
        persist_company_signals(con, "OGDC", "2025-01-15 10:00:00", signals)

        result = get_company_signals(con, "OGDC", "2025-01-15 10:00:00")
        assert result["pos_52w"] == "0.5000"
        assert result["volume_spike"] == "true"

    def test_get_latest_signals(self, con):
        """Test getting latest signals when no timestamp provided."""
        # Insert signals at two timestamps
        persist_company_signals(con, "OGDC", "2025-01-15 10:00:00", {"pos_52w": "0.5"})
        persist_company_signals(con, "OGDC", "2025-01-15 11:00:00", {"pos_52w": "0.6"})

        # Should return latest
        result = get_company_signals(con, "OGDC")
        assert result["pos_52w"] == "0.6"

    def test_get_signals_empty(self, con):
        """Test getting signals for symbol with no data."""
        result = get_company_signals(con, "NONEXISTENT")
        assert result == {}


class TestSignalsHistory:
    """Test signal history retrieval."""

    def test_get_signals_history(self, con):
        """Test getting signal history for a symbol."""
        persist_company_signals(con, "OGDC", "2025-01-15 10:00:00", {"pos_52w": "0.5"})
        persist_company_signals(con, "OGDC", "2025-01-15 11:00:00", {"pos_52w": "0.6"})

        df = get_signals_history(con, "OGDC")
        assert len(df) == 2
        assert "ts" in df.columns
        assert "signal_key" in df.columns
        assert "signal_value" in df.columns


class TestComputeAndPersistSignals:
    """Test the main entry point function."""

    def test_compute_and_persist_full(self, con):
        """Test compute_and_persist_signals integration."""
        # First, we need a quote snapshot for the history query
        # The function reads from company_quote_snapshots table
        quote_data = {
            "price": 100.0,
            "wk52_low": 50.0,
            "wk52_high": 105.0,
            "day_range_low": 95.0,
            "day_range_high": 105.0,
            "circuit_low": 90.0,
            "circuit_high": 110.0,
            "volume": 10000,
        }

        signals = compute_and_persist_signals(
            con, "OGDC", "2025-01-15 10:00:00", quote_data
        )

        # Verify signals computed
        assert "pos_52w" in signals
        assert "pos_day" in signals
        assert "signal_summary" in signals

        # Verify persisted
        db_signals = get_company_signals(con, "OGDC", "2025-01-15 10:00:00")
        assert db_signals["pos_52w"] == signals["pos_52w"]

    def test_compute_with_history(self, con):
        """Test that historical volume data is used for relative volume."""
        # Insert historical quote snapshots
        for i in range(10):
            con.execute(
                """
                INSERT INTO company_quote_snapshots
                (symbol, ts, price, volume, raw_hash)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("OGDC", f"2025-01-{10+i} 10:00:00", 100.0, 5000, f"hash{i}"),
            )
        con.commit()

        # Now compute signals with current volume higher
        quote_data = {"price": 100.0, "volume": 15000}

        signals = compute_and_persist_signals(
            con, "OGDC", "2025-01-20 10:00:00", quote_data
        )

        # Should detect volume spike (15000 / 5000 = 3x)
        assert signals["volume_spike"] == "true"
        assert float(signals["rel_volume"]) == 3.0
