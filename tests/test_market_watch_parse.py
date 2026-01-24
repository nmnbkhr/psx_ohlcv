"""Tests for market watch HTML parsing."""


from psx_ohlcv.sources.market_watch import (
    _extract_symbols_by_regex,
    _is_valid_symbol,
    parse_symbols_from_market_watch,
)

# Sample HTML with a table containing SYMBOL column
SAMPLE_MARKET_WATCH_HTML = """
<!DOCTYPE html>
<html>
<head><title>PSX Market Watch</title></head>
<body>
<div class="market-data">
    <table class="table">
        <thead>
            <tr>
                <th>SYMBOL</th>
                <th>LDCP</th>
                <th>OPEN</th>
                <th>HIGH</th>
                <th>LOW</th>
                <th>CURRENT</th>
                <th>CHANGE</th>
                <th>VOLUME</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>HBL</td>
                <td>150.00</td>
                <td>151.00</td>
                <td>155.00</td>
                <td>149.00</td>
                <td>153.00</td>
                <td>+3.00</td>
                <td>1000000</td>
            </tr>
            <tr>
                <td>UBL</td>
                <td>200.00</td>
                <td>201.00</td>
                <td>205.00</td>
                <td>199.00</td>
                <td>203.00</td>
                <td>+3.00</td>
                <td>500000</td>
            </tr>
            <tr>
                <td>MCB</td>
                <td>180.00</td>
                <td>181.00</td>
                <td>185.00</td>
                <td>179.00</td>
                <td>183.00</td>
                <td>+3.00</td>
                <td>750000</td>
            </tr>
            <tr>
                <td>ENGRO</td>
                <td>300.00</td>
                <td>301.00</td>
                <td>310.00</td>
                <td>295.00</td>
                <td>305.00</td>
                <td>+5.00</td>
                <td>200000</td>
            </tr>
            <tr>
                <td>PSO</td>
                <td>400.00</td>
                <td>401.00</td>
                <td>410.00</td>
                <td>395.00</td>
                <td>405.00</td>
                <td>+5.00</td>
                <td>150000</td>
            </tr>
        </tbody>
    </table>
</div>
</body>
</html>
"""

# HTML without tables (fallback to regex)
SAMPLE_HTML_NO_TABLE = """
<html>
<body>
<div data-symbol="LUCK">LUCK Industries</div>
<div data-symbol="DGKC">DG Khan Cement</div>
<span>Some text with <td>OGDC</td> in it</span>
</body>
</html>
"""

# Empty/minimal HTML
SAMPLE_EMPTY_HTML = """
<html><body><p>No data</p></body></html>
"""


class TestParseSymbolsFromMarketWatch:
    """Tests for the main parsing function."""

    def test_parses_table_with_symbol_column(self):
        """Should extract symbols from table with SYMBOL column."""
        result = parse_symbols_from_market_watch(SAMPLE_MARKET_WATCH_HTML)

        symbols = [r["symbol"] for r in result]
        assert "HBL" in symbols
        assert "UBL" in symbols
        assert "MCB" in symbols
        assert "ENGRO" in symbols
        assert "PSO" in symbols
        assert len(symbols) == 5

    def test_returns_sorted_symbols(self):
        """Symbols should be sorted alphabetically."""
        result = parse_symbols_from_market_watch(SAMPLE_MARKET_WATCH_HTML)

        symbols = [r["symbol"] for r in result]
        assert symbols == sorted(symbols)

    def test_returns_correct_dict_structure(self):
        """Each symbol dict should have required keys."""
        result = parse_symbols_from_market_watch(SAMPLE_MARKET_WATCH_HTML)

        for item in result:
            assert "symbol" in item
            assert "name" in item
            assert "sector" in item
            assert "is_active" in item
            assert item["is_active"] == 1

    def test_unique_symbols(self):
        """Should not return duplicate symbols."""
        # HTML with duplicate symbol
        html_with_dupe = SAMPLE_MARKET_WATCH_HTML.replace(
            "<td>PSO</td>", "<td>HBL</td>"
        )
        result = parse_symbols_from_market_watch(html_with_dupe)

        symbols = [r["symbol"] for r in result]
        assert len(symbols) == len(set(symbols))

    def test_fallback_regex_extraction(self):
        """Should fall back to regex when no tables found."""
        result = parse_symbols_from_market_watch(SAMPLE_HTML_NO_TABLE)

        symbols = [r["symbol"] for r in result]
        assert "LUCK" in symbols
        assert "DGKC" in symbols
        assert "OGDC" in symbols

    def test_empty_html_returns_empty_list(self):
        """Should return empty list for HTML with no symbols."""
        result = parse_symbols_from_market_watch(SAMPLE_EMPTY_HTML)
        assert result == []


class TestIsValidSymbol:
    """Tests for symbol validation."""

    def test_valid_symbols(self):
        """Should accept valid PSX symbols."""
        assert _is_valid_symbol("HBL") is True
        assert _is_valid_symbol("ENGRO") is True
        assert _is_valid_symbol("PSO") is True
        assert _is_valid_symbol("OGDC") is True
        assert _is_valid_symbol("LUCK") is True
        assert _is_valid_symbol("TRG") is True

    def test_symbols_with_numbers(self):
        """Should accept symbols ending with a digit."""
        assert _is_valid_symbol("ABOT1") is True
        assert _is_valid_symbol("TFC3") is True

    def test_rejects_too_short(self):
        """Should reject single character symbols."""
        assert _is_valid_symbol("A") is False
        assert _is_valid_symbol("") is False

    def test_rejects_too_long(self):
        """Should reject symbols over 10 characters."""
        assert _is_valid_symbol("VERYLONGSYMBOL") is False

    def test_rejects_lowercase(self):
        """Should reject lowercase symbols."""
        assert _is_valid_symbol("hbl") is False
        assert _is_valid_symbol("Hbl") is False

    def test_rejects_blacklisted_words(self):
        """Should reject common column headers and words."""
        assert _is_valid_symbol("SYMBOL") is False
        assert _is_valid_symbol("VOLUME") is False
        assert _is_valid_symbol("CHANGE") is False
        assert _is_valid_symbol("HIGH") is False
        assert _is_valid_symbol("LOW") is False
        assert _is_valid_symbol("OPEN") is False
        assert _is_valid_symbol("CLOSE") is False

    def test_rejects_with_special_chars(self):
        """Should reject symbols with special characters."""
        assert _is_valid_symbol("HBL-A") is False
        assert _is_valid_symbol("HBL.") is False
        assert _is_valid_symbol("HBL ") is False


class TestExtractSymbolsByRegex:
    """Tests for regex-based extraction."""

    def test_extracts_from_td_tags(self):
        """Should extract symbols from <td> tags."""
        html = "<table><tr><td>HBL</td><td>150.00</td></tr></table>"
        result = _extract_symbols_by_regex(html)
        assert "HBL" in result

    def test_extracts_from_data_symbol_attr(self):
        """Should extract from data-symbol attributes."""
        html = '<div data-symbol="ENGRO">Engro Corp</div>'
        result = _extract_symbols_by_regex(html)
        assert "ENGRO" in result

    def test_handles_quoted_attrs(self):
        """Should handle both single and double quotes."""
        html = """
        <div data-symbol="HBL">HBL</div>
        <div data-symbol='UBL'>UBL</div>
        """
        result = _extract_symbols_by_regex(html)
        assert "HBL" in result
        assert "UBL" in result

    def test_filters_invalid_symbols(self):
        """Should not include invalid symbols."""
        html = "<td>VOLUME</td><td>CHANGE</td><td>HBL</td>"
        result = _extract_symbols_by_regex(html)
        assert "HBL" in result
        assert "VOLUME" not in result
        assert "CHANGE" not in result
