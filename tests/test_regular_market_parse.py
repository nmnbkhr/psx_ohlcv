"""Tests for regular market HTML parsing."""

from psx_ohlcv.sources.regular_market import (
    _compute_row_hash,
    _empty_regular_market_df,
    _extract_symbol_and_status,
    parse_regular_market_html,
)

# Sample HTML that mimics the PSX market-watch page structure
SAMPLE_HTML = """
<!DOCTYPE html>
<html>
<head><title>PSX Market Watch</title></head>
<body>
<div class="regular-market">
    <table id="regularMarketTable">
        <thead>
            <tr>
                <th>SYMBOL</th>
                <th>SECTOR</th>
                <th>LISTED IN</th>
                <th>LDCP</th>
                <th>OPEN</th>
                <th>HIGH</th>
                <th>LOW</th>
                <th>CURRENT</th>
                <th>CHANGE</th>
                <th>CHANGE (%)</th>
                <th>VOLUME</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>HBL</td>
                <td>BANK</td>
                <td>KSE100</td>
                <td>150.00</td>
                <td>150.50</td>
                <td>152.00</td>
                <td>149.00</td>
                <td>151.25</td>
                <td>1.25</td>
                <td>0.83</td>
                <td>1,500,000</td>
            </tr>
            <tr>
                <td>OGDC NC</td>
                <td>OIL</td>
                <td>KSE100</td>
                <td>100.00</td>
                <td>100.00</td>
                <td>101.00</td>
                <td>99.50</td>
                <td>100.50</td>
                <td>0.50</td>
                <td>0.50</td>
                <td>2,000,000</td>
            </tr>
            <tr>
                <td>MCB XD</td>
                <td>BANK</td>
                <td>KSE100</td>
                <td>200.00</td>
                <td>199.00</td>
                <td>202.00</td>
                <td>198.00</td>
                <td>201.50</td>
                <td>1.50</td>
                <td>0.75</td>
                <td>500,000</td>
            </tr>
        </tbody>
    </table>
</div>
</body>
</html>
"""

SAMPLE_HTML_MINIMAL = """
<html>
<body>
<table>
    <tr>
        <th>SYMBOL</th><th>LDCP</th><th>OPEN</th><th>HIGH</th>
        <th>LOW</th><th>CURRENT</th><th>CHANGE</th><th>CHANGE (%)</th><th>VOLUME</th>
    </tr>
    <tr>
        <td>TEST</td><td>10.00</td><td>10.50</td><td>11.00</td>
        <td>9.50</td><td>10.75</td><td>0.75</td><td>7.50</td><td>100</td>
    </tr>
</table>
</body>
</html>
"""

SAMPLE_HTML_EMPTY = """
<html>
<body>
<table>
    <tr><th>SYMBOL</th><th>LDCP</th><th>CURRENT</th></tr>
</table>
</body>
</html>
"""


class TestExtractSymbolAndStatus:
    """Tests for _extract_symbol_and_status function."""

    def test_symbol_only(self):
        """Symbol without status marker."""
        symbol, status = _extract_symbol_and_status("HBL")
        assert symbol == "HBL"
        assert status is None

    def test_symbol_with_nc_status(self):
        """Symbol with NC (No Change) status."""
        symbol, status = _extract_symbol_and_status("OGDC NC")
        assert symbol == "OGDC"
        assert status == "NC"

    def test_symbol_with_xd_status(self):
        """Symbol with XD (Ex-Dividend) status."""
        symbol, status = _extract_symbol_and_status("MCB XD")
        assert symbol == "MCB"
        assert status == "XD"

    def test_symbol_with_xr_status(self):
        """Symbol with XR (Ex-Rights) status."""
        symbol, status = _extract_symbol_and_status("FFC XR")
        assert symbol == "FFC"
        assert status == "XR"

    def test_symbol_with_xb_status(self):
        """Symbol with XB (Ex-Bonus) status."""
        symbol, status = _extract_symbol_and_status("LUCK XB")
        assert symbol == "LUCK"
        assert status == "XB"

    def test_empty_string(self):
        """Empty string should return empty symbol."""
        symbol, status = _extract_symbol_and_status("")
        assert symbol == ""
        assert status is None

    def test_whitespace_only(self):
        """Whitespace should return empty symbol."""
        symbol, status = _extract_symbol_and_status("   ")
        assert symbol == ""
        assert status is None

    def test_lowercase_converted(self):
        """Symbol should be uppercase."""
        symbol, status = _extract_symbol_and_status("hbl")
        assert symbol == "HBL"
        assert status is None

    def test_symbol_with_lowercase_status(self):
        """Status marker case handling."""
        symbol, status = _extract_symbol_and_status("MCB xd")
        assert symbol == "MCB"
        assert status == "XD"

    def test_unknown_suffix_not_treated_as_status(self):
        """Unknown suffix should be part of symbol."""
        symbol, status = _extract_symbol_and_status("ABC XYZ")
        assert symbol == "ABC XYZ"
        assert status is None


class TestParseRegularMarketHtml:
    """Tests for parse_regular_market_html function."""

    def test_parse_sample_html(self):
        """Parse sample HTML with multiple rows."""
        df = parse_regular_market_html(SAMPLE_HTML)

        assert len(df) == 3
        assert "symbol" in df.columns
        assert "ts" in df.columns
        assert "row_hash" in df.columns

    def test_symbol_column(self):
        """Symbols should be extracted correctly."""
        df = parse_regular_market_html(SAMPLE_HTML)

        symbols = df["symbol"].tolist()
        assert "HBL" in symbols
        assert "OGDC" in symbols
        assert "MCB" in symbols

    def test_status_extraction(self):
        """Status markers should be extracted."""
        df = parse_regular_market_html(SAMPLE_HTML)

        hbl = df[df["symbol"] == "HBL"].iloc[0]
        ogdc = df[df["symbol"] == "OGDC"].iloc[0]
        mcb = df[df["symbol"] == "MCB"].iloc[0]

        assert hbl["status"] is None
        assert ogdc["status"] == "NC"
        assert mcb["status"] == "XD"

    def test_numeric_columns(self):
        """Numeric columns should be converted to numeric types."""
        df = parse_regular_market_html(SAMPLE_HTML)

        hbl = df[df["symbol"] == "HBL"].iloc[0]

        # Should be numeric (float or int or numpy type)
        ldcp_ok = isinstance(hbl["ldcp"], (float, int))
        ldcp_ok = ldcp_ok or hasattr(hbl["ldcp"], "item")
        curr_ok = isinstance(hbl["current"], (float, int))
        curr_ok = curr_ok or hasattr(hbl["current"], "item")
        assert ldcp_ok
        assert curr_ok

        assert hbl["ldcp"] == 150.00
        assert hbl["current"] == 151.25
        assert hbl["volume"] == 1500000

    def test_comma_removal_in_volume(self):
        """Commas should be removed from volume."""
        df = parse_regular_market_html(SAMPLE_HTML)

        hbl = df[df["symbol"] == "HBL"].iloc[0]
        assert hbl["volume"] == 1500000

    def test_timestamp_present(self):
        """Each row should have a timestamp."""
        df = parse_regular_market_html(SAMPLE_HTML)

        assert df["ts"].notna().all()
        # Check timestamp format contains date
        ts = df["ts"].iloc[0]
        assert "202" in ts or "203" in ts  # Year check

    def test_row_hash_present(self):
        """Each row should have a hash."""
        df = parse_regular_market_html(SAMPLE_HTML)

        assert df["row_hash"].notna().all()
        # Hash should be 64 hex characters (SHA256)
        assert len(df["row_hash"].iloc[0]) == 64

    def test_parse_minimal_html(self):
        """Parse HTML with minimal columns."""
        df = parse_regular_market_html(SAMPLE_HTML_MINIMAL)

        assert len(df) == 1
        assert df["symbol"].iloc[0] == "TEST"
        assert df["current"].iloc[0] == 10.75

    def test_empty_html(self):
        """Parse HTML with no data rows."""
        df = parse_regular_market_html(SAMPLE_HTML_EMPTY)

        assert df.empty

    def test_duplicate_symbols_removed(self):
        """Duplicate symbols should keep last occurrence."""
        # Need at least 5 cells per row (parser skips rows with < 5 cells)
        html = """
        <html><body>
        <table>
            <tr>
                <th>SYMBOL</th><th>SECTOR</th><th>LDCP</th><th>OPEN</th>
                <th>HIGH</th><th>LOW</th><th>CURRENT</th><th>VOLUME</th>
            </tr>
            <tr>
                <td>HBL</td><td>BANK</td><td>150</td><td>150</td>
                <td>151</td><td>149</td><td>151</td><td>1000</td>
            </tr>
            <tr>
                <td>HBL</td><td>BANK</td><td>150</td><td>150</td>
                <td>153</td><td>149</td><td>152</td><td>2000</td>
            </tr>
        </table>
        </body></html>
        """
        df = parse_regular_market_html(html)

        assert len(df) == 1
        assert df["symbol"].iloc[0] == "HBL"
        assert df["current"].iloc[0] == 152.0


class TestComputeRowHash:
    """Tests for _compute_row_hash function."""

    def test_hash_deterministic(self):
        """Same input should produce same hash."""
        row = {
            "symbol": "HBL",
            "status": None,
            "ldcp": 150.0,
            "current": 151.0,
            "volume": 1000000,
        }

        hash1 = _compute_row_hash(row)
        hash2 = _compute_row_hash(row)

        assert hash1 == hash2

    def test_hash_differs_on_change(self):
        """Different values should produce different hash."""
        row1 = {"symbol": "HBL", "current": 150.0, "volume": 1000}
        row2 = {"symbol": "HBL", "current": 151.0, "volume": 1000}

        hash1 = _compute_row_hash(row1)
        hash2 = _compute_row_hash(row2)

        assert hash1 != hash2

    def test_hash_handles_none(self):
        """None values should not cause errors."""
        row = {"symbol": "HBL", "current": None, "volume": None}
        hash_val = _compute_row_hash(row)

        assert hash_val is not None
        assert len(hash_val) == 64

    def test_hash_handles_nan(self):
        """NaN values should be treated as empty."""
        row = {"symbol": "HBL", "current": float("nan")}
        hash_val = _compute_row_hash(row)

        assert hash_val is not None
        assert len(hash_val) == 64

    def test_hash_is_sha256(self):
        """Hash should be SHA256 (64 hex characters)."""
        row = {"symbol": "TEST", "current": 100.0}
        hash_val = _compute_row_hash(row)

        assert len(hash_val) == 64
        # Should be hex characters only
        assert all(c in "0123456789abcdef" for c in hash_val)


class TestEmptyRegularMarketDf:
    """Tests for _empty_regular_market_df function."""

    def test_has_expected_columns(self):
        """Empty DataFrame should have correct columns."""
        df = _empty_regular_market_df()

        expected_cols = [
            "ts", "symbol", "status", "sector_code", "listed_in",
            "ldcp", "open", "high", "low", "current",
            "change", "change_pct", "volume", "row_hash"
        ]
        assert list(df.columns) == expected_cols

    def test_is_empty(self):
        """DataFrame should be empty."""
        df = _empty_regular_market_df()
        assert df.empty


class TestParseEdgeCases:
    """Edge case tests for HTML parsing."""

    def test_missing_columns(self):
        """Parse HTML with minimal but sufficient columns."""
        # Need at least 5 cells per row (parser skips rows with < 5 cells)
        html = """
        <table>
            <tr>
                <th>SYMBOL</th><th>SECTOR</th><th>LDCP</th>
                <th>OPEN</th><th>CURRENT</th><th>VOLUME</th>
            </tr>
            <tr>
                <td>HBL</td><td>BANK</td><td>150</td>
                <td>150</td><td>151</td><td>1000</td>
            </tr>
        </table>
        """
        df = parse_regular_market_html(html)

        assert len(df) == 1
        assert df["symbol"].iloc[0] == "HBL"
        assert df["current"].iloc[0] == 151.0

    def test_negative_change(self):
        """Parse negative change values.

        Note: Uses same column order as actual PSX market-watch page.
        """
        # Full table structure matching actual PSX market-watch page
        html = """
        <table>
            <thead>
            <tr>
                <th>SYMBOL</th><th>SECTOR</th><th>LISTED IN</th><th>LDCP</th>
                <th>OPEN</th><th>HIGH</th><th>LOW</th><th>CURRENT</th>
                <th>CHANGE</th><th>CHANGE (%)</th><th>VOLUME</th>
            </tr>
            </thead>
            <tbody>
            <tr>
                <td>HBL</td><td>BANK</td><td>KSE100</td><td>150</td>
                <td>150</td><td>150</td><td>147</td><td>148</td>
                <td>-2.00</td><td>-1.33</td><td>1000</td>
            </tr>
            </tbody>
        </table>
        """
        df = parse_regular_market_html(html)

        assert len(df) == 1
        # Verify negative values are parsed correctly
        assert df["change"].iloc[0] < 0
        assert df["change_pct"].iloc[0] < 0

    def test_special_characters_in_values(self):
        """Parse values with special formatting."""
        # Full table structure for reliable parsing
        html = """
        <table>
            <tr>
                <th>SYMBOL</th><th>SECTOR</th><th>LDCP</th>
                <th>OPEN</th><th>HIGH</th><th>LOW</th><th>CURRENT</th>
                <th>CHANGE</th><th>CHANGE (%)</th><th>VOLUME</th>
            </tr>
            <tr>
                <td>HBL</td><td>BANK</td><td>1,500.00</td>
                <td>1,500.00</td><td>1,510.00</td><td>1,495.00</td><td>1,500.50</td>
                <td>0.50</td><td>0.03</td><td>10,000,000</td>
            </tr>
        </table>
        """
        df = parse_regular_market_html(html)

        assert len(df) == 1
        assert df["current"].iloc[0] == 1500.50
        assert df["volume"].iloc[0] == 10000000
