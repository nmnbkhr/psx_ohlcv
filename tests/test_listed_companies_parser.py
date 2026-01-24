"""Tests for listed companies parser and database operations."""

import sqlite3
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from psx_ohlcv.db import init_schema
from psx_ohlcv.sources.listed_companies import (
    get_master_symbols,
    parse_listed_companies,
    upsert_symbols_from_master,
)


@pytest.fixture
def db_connection():
    """Create in-memory database with schema initialized."""
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    init_schema(con)
    return con


@pytest.fixture
def sample_lst_file():
    """Create a temporary sample .lst file."""
    # Sample lines from the actual listed_cmp.lst format
    lines = [
        "|786|786 Investments Limited|0813|INV. BANKS|14973750||",
        "|ABL|Allied Bank Limited|0807|COMMERCIAL BANKS|1145073830||",
        "|ABOT|Abbott Laboratories|0823|PHARMACEUTICALS|97900302||",
        "|ACPL|Attock Cement Pakistan Limited|0804|CEMENT|137426961||",
        "|OGDC|Oil & Gas Development Company|0816|OIL & GAS EXPLORATION|4301350454||",
        "|PSO|Pakistan State Oil Company|0817|OIL & GAS MARKETING|290886903||",
        "|HBL|Habib Bank Limited|0807|COMMERCIAL BANKS|1467600264||",
    ]
    content = "\n".join(lines) + "\n"
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".lst", delete=False, encoding="utf-8"
    ) as f:
        f.write(content)
        return Path(f.name)


@pytest.fixture
def sample_df():
    """Create sample DataFrame matching master file format."""
    return pd.DataFrame({
        "symbol": ["ABL", "HBL", "OGDC", "PSO"],
        "company_name": [
            "Allied Bank Limited",
            "Habib Bank Limited",
            "Oil & Gas Development Company Limited",
            "Pakistan State Oil Company Limited",
        ],
        "sector_code": ["0807", "0807", "0816", "0817"],
        "sector_name": [
            "COMMERCIAL BANKS",
            "COMMERCIAL BANKS",
            "OIL & GAS EXPLORATION COMPANIES",
            "OIL & GAS MARKETING COMPANIES",
        ],
        "outstanding_shares": [1145073830, 1467600264, 4301350454, 290886903],
    })


class TestParseListedCompanies:
    """Tests for parse_listed_companies function."""

    def test_parse_basic_file(self, sample_lst_file):
        """Parse a basic listed companies file."""
        df = parse_listed_companies(sample_lst_file)

        assert len(df) == 7
        assert "symbol" in df.columns
        assert "company_name" in df.columns
        assert "sector_code" in df.columns
        assert "sector_name" in df.columns
        assert "outstanding_shares" in df.columns

    def test_symbol_uppercase(self, sample_lst_file):
        """Symbols should be uppercase."""
        df = parse_listed_companies(sample_lst_file)

        for symbol in df["symbol"]:
            assert symbol == symbol.upper()

    def test_sector_code_preserved(self, sample_lst_file):
        """Sector codes with leading zeros should be preserved."""
        df = parse_listed_companies(sample_lst_file)

        # Check that sector codes are strings with leading zeros
        sector_codes = df["sector_code"].tolist()
        assert "0807" in sector_codes  # Commercial banks
        assert "0804" in sector_codes  # Cement

    def test_outstanding_shares_numeric(self, sample_lst_file):
        """Outstanding shares should be numeric."""
        df = parse_listed_companies(sample_lst_file)

        # Find ABL row
        abl = df[df["symbol"] == "ABL"]
        assert len(abl) == 1
        assert abl.iloc[0]["outstanding_shares"] == 1145073830

    def test_company_name_parsed(self, sample_lst_file):
        """Company names should be parsed correctly."""
        df = parse_listed_companies(sample_lst_file)

        hbl = df[df["symbol"] == "HBL"]
        assert len(hbl) == 1
        assert hbl.iloc[0]["company_name"] == "Habib Bank Limited"

    def test_sector_name_parsed(self, sample_lst_file):
        """Sector names should be parsed correctly."""
        df = parse_listed_companies(sample_lst_file)

        ogdc = df[df["symbol"] == "OGDC"]
        assert len(ogdc) == 1
        assert ogdc.iloc[0]["sector_name"] == "OIL & GAS EXPLORATION"

    def test_duplicates_removed(self):
        """Duplicate symbols should be removed."""
        content = """|ABL|Allied Bank Limited|0807|COMMERCIAL BANKS|1145073830||
|ABL|Allied Bank (duplicate)|0807|COMMERCIAL BANKS|1000000||
"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".lst", delete=False, encoding="utf-8"
        ) as f:
            f.write(content)
            path = Path(f.name)

        df = parse_listed_companies(path)

        # Should only have one ABL entry
        assert len(df[df["symbol"] == "ABL"]) == 1

    def test_empty_file(self):
        """Parse empty file."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".lst", delete=False, encoding="utf-8"
        ) as f:
            f.write("")
            path = Path(f.name)

        df = parse_listed_companies(path)

        assert df.empty
        assert list(df.columns) == [
            "symbol", "company_name", "sector_code",
            "sector_name", "outstanding_shares"
        ]

    def test_malformed_lines_skipped(self):
        """Malformed lines should be skipped."""
        content = """|ABL|Allied Bank Limited|0807|COMMERCIAL BANKS|1145073830||
incomplete line
|HBL|Habib Bank Limited|0807|COMMERCIAL BANKS|1467600264||
|||invalid||
"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".lst", delete=False, encoding="utf-8"
        ) as f:
            f.write(content)
            path = Path(f.name)

        df = parse_listed_companies(path)

        # Should only parse ABL and HBL
        assert len(df) == 2
        assert set(df["symbol"]) == {"ABL", "HBL"}

    def test_empty_shares_handled(self):
        """Empty outstanding shares should be None."""
        content = "|ABL|Allied Bank Limited|0807|COMMERCIAL BANKS|||\n"
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".lst", delete=False, encoding="utf-8"
        ) as f:
            f.write(content)
            path = Path(f.name)

        df = parse_listed_companies(path)

        assert len(df) == 1
        assert pd.isna(df.iloc[0]["outstanding_shares"])

    def test_sorted_by_symbol(self, sample_lst_file):
        """Result should be sorted by symbol."""
        df = parse_listed_companies(sample_lst_file)

        symbols = df["symbol"].tolist()
        assert symbols == sorted(symbols)


class TestUpsertSymbolsFromMaster:
    """Tests for upsert_symbols_from_master function."""

    def test_insert_new_symbols(self, db_connection, sample_df):
        """Insert new symbols."""
        result = upsert_symbols_from_master(db_connection, sample_df)

        assert result["inserted"] == 4
        assert result["updated"] == 0
        assert result["deactivated"] == 0

    def test_update_existing_symbols(self, db_connection, sample_df):
        """Update existing symbols."""
        # First insert
        upsert_symbols_from_master(db_connection, sample_df)

        # Modify and upsert again
        sample_df["company_name"] = sample_df["company_name"] + " (Updated)"
        result = upsert_symbols_from_master(db_connection, sample_df)

        assert result["inserted"] == 0
        assert result["updated"] == 4

        # Verify update
        cur = db_connection.execute(
            "SELECT name FROM symbols WHERE symbol = 'ABL'"
        )
        row = cur.fetchone()
        assert "(Updated)" in row["name"]

    def test_sector_name_stored(self, db_connection, sample_df):
        """Sector name should be stored in symbols table."""
        upsert_symbols_from_master(db_connection, sample_df)

        cur = db_connection.execute(
            "SELECT sector_name FROM symbols WHERE symbol = 'OGDC'"
        )
        row = cur.fetchone()
        assert row["sector_name"] == "OIL & GAS EXPLORATION COMPANIES"

    def test_outstanding_shares_stored(self, db_connection, sample_df):
        """Outstanding shares should be stored."""
        upsert_symbols_from_master(db_connection, sample_df)

        cur = db_connection.execute(
            "SELECT outstanding_shares FROM symbols WHERE symbol = 'ABL'"
        )
        row = cur.fetchone()
        assert row["outstanding_shares"] == 1145073830

    def test_source_set_to_listed_cmp(self, db_connection, sample_df):
        """Source should be set to LISTED_CMP."""
        upsert_symbols_from_master(db_connection, sample_df)

        cur = db_connection.execute(
            "SELECT source FROM symbols WHERE symbol = 'HBL'"
        )
        row = cur.fetchone()
        assert row["source"] == "LISTED_CMP"

    def test_is_active_set(self, db_connection, sample_df):
        """All symbols should be marked active."""
        upsert_symbols_from_master(db_connection, sample_df)

        cur = db_connection.execute(
            "SELECT is_active FROM symbols WHERE symbol = 'PSO'"
        )
        row = cur.fetchone()
        assert row["is_active"] == 1

    def test_deactivate_missing_false(self, db_connection, sample_df):
        """Should not deactivate missing symbols by default."""
        # Insert extra symbol not in sample_df
        db_connection.execute(
            """
            INSERT INTO symbols (symbol, name, sector, is_active, source,
                                discovered_at, updated_at)
            VALUES ('EXTRA', 'Extra Company', '0000', 1, 'TEST', datetime('now'),
                    datetime('now'))
            """
        )
        db_connection.commit()

        # Upsert sample_df (EXTRA not in it)
        result = upsert_symbols_from_master(
            db_connection, sample_df, deactivate_missing=False
        )

        assert result["deactivated"] == 0

        # EXTRA should still be active
        cur = db_connection.execute(
            "SELECT is_active FROM symbols WHERE symbol = 'EXTRA'"
        )
        row = cur.fetchone()
        assert row["is_active"] == 1

    def test_deactivate_missing_true(self, db_connection, sample_df):
        """Should deactivate missing symbols when flag is True."""
        # Insert extra symbol not in sample_df
        db_connection.execute(
            """
            INSERT INTO symbols (symbol, name, sector, is_active, source,
                                discovered_at, updated_at)
            VALUES ('EXTRA', 'Extra Company', '0000', 1, 'TEST', datetime('now'),
                    datetime('now'))
            """
        )
        db_connection.commit()

        # Upsert sample_df with deactivate_missing=True
        result = upsert_symbols_from_master(
            db_connection, sample_df, deactivate_missing=True
        )

        assert result["deactivated"] == 1

        # EXTRA should now be inactive
        cur = db_connection.execute(
            "SELECT is_active FROM symbols WHERE symbol = 'EXTRA'"
        )
        row = cur.fetchone()
        assert row["is_active"] == 0

    def test_empty_df(self, db_connection):
        """Empty DataFrame should not insert anything."""
        empty_df = pd.DataFrame(columns=[
            "symbol", "company_name", "sector_code",
            "sector_name", "outstanding_shares"
        ])
        result = upsert_symbols_from_master(db_connection, empty_df)

        assert result["inserted"] == 0
        assert result["updated"] == 0
        assert result["deactivated"] == 0


class TestGetMasterSymbols:
    """Tests for get_master_symbols function."""

    def test_get_all_symbols(self, db_connection, sample_df):
        """Get all symbols from database."""
        upsert_symbols_from_master(db_connection, sample_df)
        df = get_master_symbols(db_connection)

        assert len(df) == 4
        assert "symbol" in df.columns
        assert "sector_name" in df.columns
        assert "outstanding_shares" in df.columns

    def test_get_symbols_empty(self, db_connection):
        """Get symbols from empty table."""
        df = get_master_symbols(db_connection)
        assert df.empty


class TestSchemaMigration:
    """Tests for schema migration adding new columns."""

    def test_migration_adds_sector_name(self, db_connection):
        """Schema migration should add sector_name column."""
        # Verify sector_name column exists
        cur = db_connection.execute("PRAGMA table_info(symbols)")
        columns = {row[1] for row in cur.fetchall()}

        assert "sector_name" in columns

    def test_migration_adds_outstanding_shares(self, db_connection):
        """Schema migration should add outstanding_shares column."""
        cur = db_connection.execute("PRAGMA table_info(symbols)")
        columns = {row[1] for row in cur.fetchall()}

        assert "outstanding_shares" in columns

    def test_migration_adds_source(self, db_connection):
        """Schema migration should add source column."""
        cur = db_connection.execute("PRAGMA table_info(symbols)")
        columns = {row[1] for row in cur.fetchall()}

        assert "source" in columns
