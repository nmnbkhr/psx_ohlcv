"""Tests for intraday data parser."""

import pandas as pd

from psx_ohlcv.sources.intraday import (
    _empty_intraday_df,
    _extract_data_list,
    _parse_array_item,
    _parse_dict_item,
    _parse_single_item,
    aggregate_intraday_to_ohlcv,
    filter_incremental,
    parse_intraday_payload,
)


class TestExtractDataList:
    """Tests for _extract_data_list function."""

    def test_extract_from_list(self):
        """Direct list should be returned as-is."""
        payload = [[1, 2, 3], [4, 5, 6]]
        result = _extract_data_list(payload)
        assert result == [[1, 2, 3], [4, 5, 6]]

    def test_extract_from_data_key(self):
        """Data key should be extracted."""
        payload = {"data": [[1, 2, 3]]}
        result = _extract_data_list(payload)
        assert result == [[1, 2, 3]]

    def test_extract_from_timeseries_key(self):
        """Timeseries key should be extracted."""
        payload = {"timeseries": [[1, 2, 3]]}
        result = _extract_data_list(payload)
        assert result == [[1, 2, 3]]

    def test_extract_from_records_key(self):
        """Records key should be extracted."""
        payload = {"records": [[1, 2, 3]]}
        result = _extract_data_list(payload)
        assert result == [[1, 2, 3]]

    def test_extract_from_first_list_value(self):
        """First list value in dict should be extracted."""
        payload = {"other_key": [[1, 2, 3]]}
        result = _extract_data_list(payload)
        assert result == [[1, 2, 3]]

    def test_empty_payload(self):
        """Empty payload should return empty list."""
        assert _extract_data_list({}) == []
        assert _extract_data_list([]) == []

    def test_dict_without_lists(self):
        """Dict without list values should return empty list."""
        payload = {"key": "value", "num": 123}
        result = _extract_data_list(payload)
        assert result == []


class TestParseArrayItem:
    """Tests for _parse_array_item function."""

    def test_parse_4_element_array(self):
        """PSX format: [timestamp, close, volume, open]."""
        # Timestamp for 2024-01-15 10:30:00
        item = [1705310400, 103.5, 50000, 100.0]
        result = _parse_array_item("ABOT", item)

        assert result is not None
        assert result["symbol"] == "ABOT"
        assert result["close"] == 103.5
        assert result["volume"] == 50000
        assert result["open"] == 100.0
        assert result["high"] == 103.5  # max(open, close)
        assert result["low"] == 100.0   # min(open, close)
        assert "ts" in result

    def test_parse_3_element_array(self):
        """Format: [timestamp, price, volume]."""
        item = [1705310400, 103.5, 50000]
        result = _parse_array_item("HBL", item)

        assert result is not None
        assert result["symbol"] == "HBL"
        assert result["close"] == 103.5
        assert result["volume"] == 50000
        assert result["open"] == 103.5
        assert result["high"] == 103.5
        assert result["low"] == 103.5

    def test_parse_2_element_array(self):
        """Format: [timestamp, price]."""
        item = [1705310400, 103.5]
        result = _parse_array_item("MCB", item)

        assert result is not None
        assert result["close"] == 103.5
        assert result["volume"] is None

    def test_invalid_timestamp(self):
        """Invalid timestamp should return None."""
        item = [123, 103.5, 50000]  # Too small
        result = _parse_array_item("ABOT", item)
        assert result is None

    def test_short_array(self):
        """Array with less than 2 elements should return None."""
        item = [1705310400]
        result = _parse_array_item("ABOT", item)
        assert result is None


class TestParseDictItem:
    """Tests for _parse_dict_item function."""

    def test_parse_with_ts_key(self):
        """Dict with ts key should be parsed."""
        item = {
            "ts": "2024-01-15 10:30:00",
            "open": 100.0,
            "high": 105.0,
            "low": 99.0,
            "close": 103.5,
            "volume": 50000,
        }
        result = _parse_dict_item("ABOT", item)

        assert result is not None
        assert result["symbol"] == "ABOT"
        assert result["ts"] == "2024-01-15 10:30:00"
        assert result["open"] == 100.0
        assert result["close"] == 103.5

    def test_parse_with_timestamp_key(self):
        """Dict with timestamp key should be parsed."""
        item = {"timestamp": "2024-01-15 10:30:00", "close": 103.5}
        result = _parse_dict_item("ABOT", item)

        assert result is not None
        assert result["ts"] == "2024-01-15 10:30:00"

    def test_parse_with_numeric_timestamp(self):
        """Numeric timestamp should be converted."""
        item = {"ts": 1705310400, "close": 103.5}
        result = _parse_dict_item("ABOT", item)

        assert result is not None
        assert "2024" in result["ts"] or "2025" in result["ts"]

    def test_parse_with_price_key(self):
        """Dict with price key should use it as close."""
        item = {"ts": "2024-01-15 10:30:00", "price": 103.5}
        result = _parse_dict_item("ABOT", item)

        assert result is not None
        assert result["close"] == 103.5

    def test_missing_timestamp(self):
        """Dict without timestamp should return None."""
        item = {"close": 103.5, "volume": 50000}
        result = _parse_dict_item("ABOT", item)
        assert result is None


class TestParseSingleItem:
    """Tests for _parse_single_item function."""

    def test_parse_dict(self):
        """Dict item should be parsed."""
        item = {"ts": "2024-01-15 10:30:00", "close": 103.5}
        result = _parse_single_item("ABOT", item)
        assert result is not None

    def test_parse_list(self):
        """List item should be parsed."""
        item = [1705310400, 103.5, 50000, 100.0]
        result = _parse_single_item("ABOT", item)
        assert result is not None

    def test_parse_invalid(self):
        """Invalid item type should return None."""
        result = _parse_single_item("ABOT", "invalid")
        assert result is None

        result = _parse_single_item("ABOT", 123)
        assert result is None


class TestParseIntradayPayload:
    """Tests for parse_intraday_payload function."""

    def test_parse_array_payload(self):
        """Parse list of arrays."""
        payload = [
            [1705310400, 103.5, 50000, 100.0],
            [1705314000, 104.0, 55000, 103.5],
        ]
        df = parse_intraday_payload("ABOT", payload)

        assert len(df) == 2
        assert "symbol" in df.columns
        assert "ts" in df.columns
        assert df["symbol"].iloc[0] == "ABOT"

    def test_parse_dict_payload_with_data_key(self):
        """Parse dict with data key."""
        payload = {
            "data": [
                [1705310400, 103.5, 50000, 100.0],
                [1705314000, 104.0, 55000, 103.5],
            ]
        }
        df = parse_intraday_payload("HBL", payload)

        assert len(df) == 2
        assert df["symbol"].iloc[0] == "HBL"

    def test_parse_dict_payload_list_items(self):
        """Parse dict items."""
        payload = [
            {"ts": "2024-01-15 10:30:00", "close": 103.5, "volume": 50000},
            {"ts": "2024-01-15 11:00:00", "close": 104.0, "volume": 55000},
        ]
        df = parse_intraday_payload("MCB", payload)

        assert len(df) == 2
        assert df["symbol"].iloc[0] == "MCB"

    def test_empty_payload(self):
        """Empty payload should return empty DataFrame."""
        df = parse_intraday_payload("ABOT", [])
        assert df.empty
        assert "symbol" in df.columns
        assert "ts" in df.columns

    def test_removes_duplicates(self):
        """Duplicate (symbol, ts) should be removed."""
        payload = [
            [1705310400, 103.5, 50000, 100.0],
            [1705310400, 104.0, 55000, 101.0],  # Same timestamp
        ]
        df = parse_intraday_payload("ABOT", payload)

        assert len(df) == 1
        # Should keep last value
        assert df["close"].iloc[0] == 104.0

    def test_sorted_by_ts(self):
        """Results should be sorted by ts ascending."""
        payload = [
            [1705314000, 104.0, 55000, 103.5],  # Later
            [1705310400, 103.5, 50000, 100.0],  # Earlier
        ]
        df = parse_intraday_payload("ABOT", payload)

        assert df["ts"].iloc[0] < df["ts"].iloc[1]

    def test_symbol_uppercase(self):
        """Symbol should be uppercase."""
        payload = [[1705310400, 103.5, 50000, 100.0]]
        df = parse_intraday_payload("abot", payload)

        assert df["symbol"].iloc[0] == "ABOT"

    def test_numeric_conversion(self):
        """Numeric columns should be converted."""
        payload = [
            {"ts": "2024-01-15 10:30:00", "close": "103.5", "volume": "50000"}
        ]
        df = parse_intraday_payload("ABOT", payload)

        assert df["close"].iloc[0] == 103.5
        assert df["volume"].iloc[0] == 50000


class TestFilterIncremental:
    """Tests for filter_incremental function."""

    def test_filter_newer_rows(self):
        """Only rows newer than last_ts_epoch should be returned."""
        # Epoch values: 10:00 = 1705309200, 11:00 = 1705312800, 12:00 = 1705316400
        df = pd.DataFrame({
            "symbol": ["ABOT", "ABOT", "ABOT"],
            "ts": ["2024-01-15 10:00:00", "2024-01-15 11:00:00", "2024-01-15 12:00:00"],
            "ts_epoch": [1705309200, 1705312800, 1705316400],
            "close": [100, 101, 102],
        })

        # Filter with epoch for 10:30 (1705311000)
        result = filter_incremental(df, 1705311000)

        assert len(result) == 2
        assert result["ts"].iloc[0] == "2024-01-15 11:00:00"

    def test_filter_exact_match_excluded(self):
        """Row with exact epoch match should be excluded (> not >=)."""
        df = pd.DataFrame({
            "symbol": ["ABOT", "ABOT"],
            "ts": ["2024-01-15 10:00:00", "2024-01-15 11:00:00"],
            "ts_epoch": [1705309200, 1705312800],
            "close": [100, 101],
        })

        # Filter with exact epoch for 10:00
        result = filter_incremental(df, 1705309200)

        assert len(result) == 1
        assert result["ts"].iloc[0] == "2024-01-15 11:00:00"

    def test_filter_none_last_ts_epoch(self):
        """None last_ts_epoch should return all rows."""
        df = pd.DataFrame({
            "symbol": ["ABOT", "ABOT"],
            "ts": ["2024-01-15 10:00:00", "2024-01-15 11:00:00"],
            "ts_epoch": [1705309200, 1705312800],
            "close": [100, 101],
        })

        result = filter_incremental(df, None)

        assert len(result) == 2

    def test_filter_empty_df(self):
        """Empty DataFrame should return empty."""
        df = pd.DataFrame(columns=["symbol", "ts", "ts_epoch", "close"])
        result = filter_incremental(df, 1705309200)
        assert result.empty


class TestAggregateIntradayToOhlcv:
    """Tests for aggregate_intraday_to_ohlcv function."""

    def test_aggregate_single_day(self):
        """Aggregate intraday data for a single day."""
        df = pd.DataFrame({
            "symbol": ["ABOT", "ABOT", "ABOT"],
            "ts": ["2024-01-15 10:00:00", "2024-01-15 11:00:00", "2024-01-15 12:00:00"],
            "open": [100, 101, 102],
            "high": [101, 103, 105],
            "low": [99, 100, 101],
            "close": [101, 102, 104],
            "volume": [1000, 2000, 1500],
        })

        result = aggregate_intraday_to_ohlcv(df)

        assert len(result) == 1
        assert result["symbol"].iloc[0] == "ABOT"
        assert result["date"].iloc[0] == "2024-01-15"
        assert result["open"].iloc[0] == 100  # First open
        assert result["high"].iloc[0] == 105  # Max high
        assert result["low"].iloc[0] == 99    # Min low
        assert result["close"].iloc[0] == 104  # Last close
        assert result["volume"].iloc[0] == 4500  # Sum volume

    def test_aggregate_multiple_days(self):
        """Aggregate intraday data for multiple days."""
        df = pd.DataFrame({
            "symbol": ["ABOT", "ABOT", "ABOT", "ABOT"],
            "ts": [
                "2024-01-15 10:00:00", "2024-01-15 11:00:00",
                "2024-01-16 10:00:00", "2024-01-16 11:00:00"
            ],
            "open": [100, 101, 105, 106],
            "high": [101, 103, 107, 108],
            "low": [99, 100, 104, 105],
            "close": [101, 102, 106, 107],
            "volume": [1000, 2000, 1500, 2500],
        })

        result = aggregate_intraday_to_ohlcv(df)

        assert len(result) == 2
        # First day
        day1 = result[result["date"] == "2024-01-15"].iloc[0]
        assert day1["open"] == 100
        assert day1["close"] == 102
        assert day1["volume"] == 3000

        # Second day
        day2 = result[result["date"] == "2024-01-16"].iloc[0]
        assert day2["open"] == 105
        assert day2["close"] == 107
        assert day2["volume"] == 4000

    def test_aggregate_empty_df(self):
        """Empty DataFrame should return empty with correct columns."""
        df = pd.DataFrame(
            columns=["symbol", "ts", "open", "high", "low", "close", "volume"]
        )
        result = aggregate_intraday_to_ohlcv(df)

        assert result.empty
        assert "symbol" in result.columns
        assert "date" in result.columns


class TestEmptyIntradayDf:
    """Tests for _empty_intraday_df function."""

    def test_has_correct_columns(self):
        """Empty DataFrame should have correct columns."""
        df = _empty_intraday_df()

        expected_cols = [
            "symbol", "ts", "ts_epoch", "open", "high", "low", "close", "volume"
        ]
        assert list(df.columns) == expected_cols
        assert df.empty
