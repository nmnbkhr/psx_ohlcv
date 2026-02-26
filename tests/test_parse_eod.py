"""Tests for EOD payload parsing."""

import pandas as pd

from pakfindata.sources.eod import parse_eod_payload

# Payload shape 1: Direct list of records
PAYLOAD_LIST = [
    {
        "date": "2024-01-15",
        "open": 150.0,
        "high": 155.0,
        "low": 148.0,
        "close": 153.0,
        "volume": 100000,
    },
    {
        "date": "2024-01-16",
        "open": 153.0,
        "high": 158.0,
        "low": 152.0,
        "close": 156.0,
        "volume": 120000,
    },
    {
        "date": "2024-01-17",
        "open": 156.0,
        "high": 160.0,
        "low": 154.0,
        "close": 159.0,
        "volume": 80000,
    },
]

# Payload shape 2: {"data": [...]}
PAYLOAD_DATA_WRAPPER = {
    "data": [
        {
            "date": "2024-01-15",
            "open": 150.0,
            "high": 155.0,
            "low": 148.0,
            "close": 153.0,
            "volume": 100000,
        },
        {
            "date": "2024-01-16",
            "open": 153.0,
            "high": 158.0,
            "low": 152.0,
            "close": 156.0,
            "volume": 120000,
        },
    ]
}

# Payload shape 3: {"timeseries": [...]}
PAYLOAD_TIMESERIES = {
    "timeseries": [
        {
            "date": "2024-01-15",
            "open": 150.0,
            "high": 155.0,
            "low": 148.0,
            "close": 153.0,
            "volume": 100000,
        },
    ]
}

# Payload with shorthand column names
PAYLOAD_SHORTHAND = [
    {
        "dt": "2024-01-15",
        "o": 150.0,
        "h": 155.0,
        "l": 148.0,
        "c": 153.0,
        "v": 100000,
    },
]

# Payload with various date formats
PAYLOAD_DATE_FORMATS = [
    {
        "date": "2024-01-15T00:00:00",
        "open": 150.0,
        "high": 155.0,
        "low": 148.0,
        "close": 153.0,
        "volume": 100000,
    },
    {
        "date": "2024/01/16",
        "open": 153.0,
        "high": 158.0,
        "low": 152.0,
        "close": 156.0,
        "volume": 120000,
    },
]


class TestParseEodPayload:
    """Tests for parse_eod_payload function."""

    def test_parses_list_payload(self):
        """Should parse direct list payload."""
        df = parse_eod_payload("HBL", PAYLOAD_LIST)

        assert len(df) == 3
        assert list(df.columns) == [
            "symbol",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
        assert df["symbol"].iloc[0] == "HBL"
        assert df["date"].iloc[0] == "2024-01-15"
        assert df["close"].iloc[0] == 153.0

    def test_parses_data_wrapper_payload(self):
        """Should parse {"data": [...]} payload."""
        df = parse_eod_payload("UBL", PAYLOAD_DATA_WRAPPER)

        assert len(df) == 2
        assert df["symbol"].iloc[0] == "UBL"
        assert df["date"].iloc[0] == "2024-01-15"

    def test_parses_timeseries_payload(self):
        """Should parse {"timeseries": [...]} payload."""
        df = parse_eod_payload("MCB", PAYLOAD_TIMESERIES)

        assert len(df) == 1
        assert df["symbol"].iloc[0] == "MCB"
        assert df["date"].iloc[0] == "2024-01-15"

    def test_parses_shorthand_columns(self):
        """Should handle shorthand column names (dt, o, h, l, c, v)."""
        df = parse_eod_payload("ENGRO", PAYLOAD_SHORTHAND)

        assert len(df) == 1
        assert df["date"].iloc[0] == "2024-01-15"
        assert df["open"].iloc[0] == 150.0
        assert df["high"].iloc[0] == 155.0
        assert df["low"].iloc[0] == 148.0
        assert df["close"].iloc[0] == 153.0
        assert df["volume"].iloc[0] == 100000

    def test_normalizes_dates(self):
        """Should normalize dates to YYYY-MM-DD format."""
        df = parse_eod_payload("PSO", PAYLOAD_DATE_FORMATS)

        assert len(df) == 2
        # Both dates should be in YYYY-MM-DD format
        assert df["date"].iloc[0] == "2024-01-15"
        assert df["date"].iloc[1] == "2024-01-16"

    def test_sorts_by_date(self):
        """Should sort results by date."""
        payload = [
            {
                "date": "2024-01-17",
                "open": 156.0,
                "high": 160.0,
                "low": 154.0,
                "close": 159.0,
                "volume": 80000,
            },
            {
                "date": "2024-01-15",
                "open": 150.0,
                "high": 155.0,
                "low": 148.0,
                "close": 153.0,
                "volume": 100000,
            },
        ]
        df = parse_eod_payload("HBL", payload)

        assert df["date"].iloc[0] == "2024-01-15"
        assert df["date"].iloc[1] == "2024-01-17"

    def test_removes_duplicates(self):
        """Should remove duplicate (symbol, date) entries."""
        payload = [
            {
                "date": "2024-01-15",
                "open": 150.0,
                "high": 155.0,
                "low": 148.0,
                "close": 153.0,
                "volume": 100000,
            },
            {
                "date": "2024-01-15",
                "open": 151.0,
                "high": 156.0,
                "low": 149.0,
                "close": 154.0,
                "volume": 110000,
            },
        ]
        df = parse_eod_payload("HBL", payload)

        assert len(df) == 1
        # Should keep last entry
        assert df["close"].iloc[0] == 154.0

    def test_coerces_numeric_values(self):
        """Should coerce numeric values, handling invalid entries."""
        payload = [
            {
                "date": "2024-01-15",
                "open": "150.0",
                "high": "invalid",
                "low": 148.0,
                "close": None,
                "volume": "100000",
            },
        ]
        df = parse_eod_payload("HBL", payload)

        assert len(df) == 1
        assert df["open"].iloc[0] == 150.0
        assert pd.isna(df["high"].iloc[0])
        assert df["low"].iloc[0] == 148.0
        assert pd.isna(df["close"].iloc[0])
        assert df["volume"].iloc[0] == 100000

    def test_empty_payload_returns_empty_df(self):
        """Should return empty DataFrame for empty payload."""
        df = parse_eod_payload("HBL", [])

        assert df.empty
        assert list(df.columns) == [
            "symbol",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]

    def test_empty_dict_returns_empty_df(self):
        """Should return empty DataFrame for empty dict."""
        df = parse_eod_payload("HBL", {})

        assert df.empty

    def test_missing_columns_returns_empty_df(self):
        """Should return empty DataFrame if required columns missing."""
        payload = [{"foo": "bar", "baz": 123}]
        df = parse_eod_payload("HBL", payload)

        assert df.empty

    def test_handles_first_list_value_in_dict(self):
        """Should extract first list value from dict with unknown key."""
        payload = {
            "unknown_key": [
                {
                    "date": "2024-01-15",
                    "open": 150.0,
                    "high": 155.0,
                    "low": 148.0,
                    "close": 153.0,
                    "volume": 100000,
                },
            ]
        }
        df = parse_eod_payload("HBL", payload)

        assert len(df) == 1
        assert df["date"].iloc[0] == "2024-01-15"

    def test_adds_symbol_column(self):
        """Should add symbol column to all rows."""
        df = parse_eod_payload("CUSTOM", PAYLOAD_LIST)

        assert all(df["symbol"] == "CUSTOM")

    def test_drops_invalid_dates(self):
        """Should drop rows with invalid dates."""
        payload = [
            {
                "date": "2024-01-15",
                "open": 150.0,
                "high": 155.0,
                "low": 148.0,
                "close": 153.0,
                "volume": 100000,
            },
            {
                "date": "invalid-date",
                "open": 153.0,
                "high": 158.0,
                "low": 152.0,
                "close": 156.0,
                "volume": 120000,
            },
        ]
        df = parse_eod_payload("HBL", payload)

        assert len(df) == 1
        assert df["date"].iloc[0] == "2024-01-15"
