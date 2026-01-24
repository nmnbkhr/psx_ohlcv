"""Tests for sectors parsing and database operations."""

import sqlite3

import pandas as pd
import pytest

from psx_ohlcv.db import (
    get_sector_map,
    get_sector_name,
    get_sectors,
    init_schema,
    upsert_sectors,
)
from psx_ohlcv.sources.sectors import (
    _empty_sectors_df,
    get_sector_list,
    parse_sectors_from_sector_summary,
    refresh_sectors,
)


@pytest.fixture
def db_connection():
    """Create in-memory database with schema initialized."""
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    init_schema(con)
    return con


@pytest.fixture
def sample_sectors_df():
    """Create sample sectors DataFrame."""
    return pd.DataFrame({
        "sector_code": ["0101", "0102", "0103", "0201"],
        "sector_name": [
            "Commercial Banks",
            "Investment Banks",
            "Leasing Companies",
            "Oil & Gas Exploration",
        ],
    })


# Sample HTML that mimics PSX sector-summary page structure
SAMPLE_SECTOR_HTML = """
<!DOCTYPE html>
<html>
<head><title>PSX Sector Summary</title></head>
<body>
<div class="sector-summary">
    <table id="sectorTable">
        <thead>
            <tr>
                <th>SECTOR</th>
                <th>TURNOVER</th>
                <th>VOLUME</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td data-sector-code="0101">Commercial Banks</td>
                <td>500,000,000</td>
                <td>100,000,000</td>
            </tr>
            <tr>
                <td data-sector-code="0102">Investment Banks</td>
                <td>150,000,000</td>
                <td>50,000,000</td>
            </tr>
        </tbody>
    </table>
</div>
</body>
</html>
"""

# HTML with sector links
SAMPLE_SECTOR_LINKS_HTML = """
<!DOCTYPE html>
<html>
<body>
<div class="sectors-list">
    <a href="/sector/0101">Commercial Banks</a>
    <a href="/sector/0102">Investment Banks</a>
    <a href="/sector/0201?filter=active">Oil & Gas</a>
</div>
</body>
</html>
"""

# HTML with select dropdown
SAMPLE_SECTOR_SELECT_HTML = """
<!DOCTYPE html>
<html>
<body>
<select name="sector" id="sector-select">
    <option value="">Select Sector</option>
    <option value="0101">Commercial Banks</option>
    <option value="0102">Investment Banks</option>
    <option value="0103">Leasing Companies</option>
</select>
</body>
</html>
"""

EMPTY_HTML = """
<!DOCTYPE html>
<html>
<body>
<div>No sector data available</div>
</body>
</html>
"""


class TestParseSectorsFromSectorSummary:
    """Tests for parse_sectors_from_sector_summary function."""

    def test_parse_table_with_data_attributes(self):
        """Parse HTML with data-sector-code attributes."""
        df = parse_sectors_from_sector_summary(SAMPLE_SECTOR_HTML)

        # Should find 2 sectors
        assert len(df) == 2
        assert "sector_code" in df.columns
        assert "sector_name" in df.columns

        # Check data
        codes = df["sector_code"].tolist()
        assert "0101" in codes
        assert "0102" in codes

    def test_parse_sector_links(self):
        """Parse HTML with sector links."""
        df = parse_sectors_from_sector_summary(SAMPLE_SECTOR_LINKS_HTML)

        # Should find sectors from links
        assert len(df) >= 2
        codes = df["sector_code"].tolist()
        assert "0101" in codes
        assert "0102" in codes

    def test_parse_select_dropdown(self):
        """Parse HTML with sector select dropdown."""
        df = parse_sectors_from_sector_summary(SAMPLE_SECTOR_SELECT_HTML)

        # Should find sectors from dropdown
        assert len(df) >= 2
        codes = df["sector_code"].tolist()
        assert "0101" in codes
        assert "0102" in codes

    def test_parse_empty_html(self):
        """Parse HTML with no sector data."""
        df = parse_sectors_from_sector_summary(EMPTY_HTML)

        assert df.empty
        assert list(df.columns) == ["sector_code", "sector_name"]

    def test_duplicates_removed(self):
        """Duplicate sector codes should be removed."""
        html = """
        <html>
        <body>
        <select name="sector">
            <option value="0101">Commercial Banks</option>
            <option value="0101">Banks (duplicate)</option>
        </select>
        </body>
        </html>
        """
        df = parse_sectors_from_sector_summary(html)

        # Should only have one entry for 0101
        assert len(df[df["sector_code"] == "0101"]) == 1


class TestEmptySectorsDf:
    """Tests for _empty_sectors_df function."""

    def test_has_expected_columns(self):
        """Empty DataFrame should have correct columns."""
        df = _empty_sectors_df()

        assert list(df.columns) == ["sector_code", "sector_name"]

    def test_is_empty(self):
        """DataFrame should be empty."""
        df = _empty_sectors_df()
        assert df.empty


class TestUpsertSectors:
    """Tests for upsert_sectors database function."""

    def test_insert_sectors(self, db_connection, sample_sectors_df):
        """Insert new sectors."""
        count = upsert_sectors(db_connection, sample_sectors_df)

        assert count == 4

        # Verify data
        df = get_sectors(db_connection)
        assert len(df) == 4

    def test_update_existing_sector(self, db_connection):
        """Update existing sector name."""
        # Insert initial
        df1 = pd.DataFrame({
            "sector_code": ["0101"],
            "sector_name": ["Banks"],
        })
        upsert_sectors(db_connection, df1)

        # Update
        df2 = pd.DataFrame({
            "sector_code": ["0101"],
            "sector_name": ["Commercial Banks"],
        })
        upsert_sectors(db_connection, df2)

        # Verify update
        name = get_sector_name(db_connection, "0101")
        assert name == "Commercial Banks"

    def test_empty_df(self, db_connection):
        """Empty DataFrame should not insert anything."""
        empty_df = pd.DataFrame(columns=["sector_code", "sector_name"])
        count = upsert_sectors(db_connection, empty_df)

        assert count == 0

    def test_missing_columns_raises(self, db_connection):
        """Missing required columns should raise ValueError."""
        df = pd.DataFrame({"sector_code": ["0101"]})  # Missing sector_name

        with pytest.raises(ValueError) as exc_info:
            upsert_sectors(db_connection, df)

        assert "missing" in str(exc_info.value).lower()


class TestGetSectors:
    """Tests for get_sectors function."""

    def test_get_sectors(self, db_connection, sample_sectors_df):
        """Get all sectors."""
        upsert_sectors(db_connection, sample_sectors_df)
        df = get_sectors(db_connection)

        assert len(df) == 4
        assert "sector_code" in df.columns
        assert "sector_name" in df.columns
        assert "updated_at" in df.columns
        assert "source" in df.columns

    def test_get_sectors_empty(self, db_connection):
        """Get sectors from empty table."""
        df = get_sectors(db_connection)

        assert df.empty


class TestGetSectorName:
    """Tests for get_sector_name function."""

    def test_get_existing_sector(self, db_connection, sample_sectors_df):
        """Get name for existing sector code."""
        upsert_sectors(db_connection, sample_sectors_df)

        name = get_sector_name(db_connection, "0101")
        assert name == "Commercial Banks"

    def test_get_nonexistent_sector(self, db_connection):
        """Get name for non-existent sector code."""
        name = get_sector_name(db_connection, "9999")
        assert name is None


class TestGetSectorMap:
    """Tests for get_sector_map function."""

    def test_get_sector_map(self, db_connection, sample_sectors_df):
        """Get sector code to name mapping."""
        upsert_sectors(db_connection, sample_sectors_df)

        sector_map = get_sector_map(db_connection)

        assert isinstance(sector_map, dict)
        assert len(sector_map) == 4
        assert sector_map["0101"] == "Commercial Banks"
        assert sector_map["0201"] == "Oil & Gas Exploration"

    def test_get_sector_map_empty(self, db_connection):
        """Get sector map from empty table."""
        sector_map = get_sector_map(db_connection)

        assert sector_map == {}


class TestGetSectorList:
    """Tests for get_sector_list function."""

    def test_get_sector_list(self, db_connection, sample_sectors_df):
        """Get list of all sectors."""
        upsert_sectors(db_connection, sample_sectors_df)

        sectors = get_sector_list(db_connection)

        assert len(sectors) == 4
        assert isinstance(sectors[0], dict)
        assert "sector_code" in sectors[0]
        assert "sector_name" in sectors[0]


class TestRefreshSectors:
    """Tests for refresh_sectors function."""

    def test_refresh_with_html(self, db_connection):
        """Refresh sectors with provided HTML."""
        result = refresh_sectors(db_connection, html_content=SAMPLE_SECTOR_HTML)

        assert result["success"] is True
        assert result["sectors_found"] == 2
        assert result["sectors_upserted"] > 0
        assert result["error"] is None

    def test_refresh_empty_html(self, db_connection):
        """Refresh with HTML that has no sectors."""
        result = refresh_sectors(db_connection, html_content=EMPTY_HTML)

        assert result["success"] is True
        assert result["sectors_found"] == 0
        assert result["error"] is None

    def test_refresh_invalid_html(self, db_connection):
        """Refresh with invalid/malformed HTML."""
        result = refresh_sectors(db_connection, html_content="<html>")

        # Should still succeed but find no sectors
        assert result["success"] is True
        assert result["sectors_found"] == 0


class TestSectorCodeNormalization:
    """Tests for sector code handling."""

    def test_preserve_leading_zeros(self, db_connection):
        """Sector codes with leading zeros should be preserved."""
        df = pd.DataFrame({
            "sector_code": ["0101", "0201"],
            "sector_name": ["Banks", "Oil"],
        })
        upsert_sectors(db_connection, df)

        sector_map = get_sector_map(db_connection)

        # Leading zeros preserved
        assert "0101" in sector_map
        assert "0201" in sector_map

    def test_sector_code_as_string(self, db_connection, sample_sectors_df):
        """Sector codes are stored as strings."""
        upsert_sectors(db_connection, sample_sectors_df)

        df = get_sectors(db_connection)

        # All codes should be strings
        assert df["sector_code"].dtype == object
        assert all(isinstance(code, str) for code in df["sector_code"])
