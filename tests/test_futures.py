"""Tests for futures classification, repository, and migration."""

import sqlite3

import pandas as pd
import pytest

from pakfindata.sources.market_summary import (
    classify_market_type,
    parse_futures_symbol,
)
from pakfindata.db.repositories.futures import (
    init_futures_schema,
    upsert_futures_eod,
    get_futures_eod,
    get_futures_stats,
    get_contract_comparison,
    get_most_active_futures,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def con():
    """In-memory SQLite with futures_eod table."""
    c = sqlite3.connect(":memory:")
    init_futures_schema(c)
    return c


@pytest.fixture
def sample_df():
    """Sample futures DataFrame."""
    return pd.DataFrame([
        {
            "symbol": "OGDC-FEB", "date": "2026-02-25", "market_type": "FUT",
            "base_symbol": "OGDC", "contract_month": "FEB",
            "sector_code": "40", "company_name": "OGDCL",
            "open": 280.0, "high": 285.0, "low": 278.0, "close": 282.0,
            "volume": 50000, "prev_close": 280.0,
        },
        {
            "symbol": "OGDC-CFEB", "date": "2026-02-25", "market_type": "CONT",
            "base_symbol": "OGDC", "contract_month": "FEB",
            "sector_code": "40", "company_name": "OGDCL",
            "open": 279.0, "high": 284.0, "low": 277.0, "close": 281.0,
            "volume": 30000, "prev_close": 279.0,
        },
        {
            "symbol": "KSE30-FEB", "date": "2026-02-25", "market_type": "IDX_FUT",
            "base_symbol": "KSE30", "contract_month": "FEB",
            "sector_code": "41", "company_name": "KSE30 Index",
            "open": 40000.0, "high": 40500.0, "low": 39800.0, "close": 40200.0,
            "volume": 100, "prev_close": 40000.0,
        },
        {
            "symbol": "P01GIS200826", "date": "2026-02-25", "market_type": "ODL",
            "base_symbol": "P01GIS200826", "contract_month": None,
            "sector_code": "36", "company_name": "GIS Bond",
            "open": 100.0, "high": 100.5, "low": 99.5, "close": 100.2,
            "volume": 5, "prev_close": 100.0,
        },
    ])


# ---------------------------------------------------------------------------
# classify_market_type tests
# ---------------------------------------------------------------------------

class TestClassifyMarketType:
    def test_reg_normal_sector(self):
        assert classify_market_type("0807", "ABL") == "REG"

    def test_reg_sector_0830(self):
        assert classify_market_type("0830", "AATM") == "REG"

    def test_fut_sector_40(self):
        assert classify_market_type("40", "OGDC-FEB") == "FUT"

    def test_fut_b_series(self):
        assert classify_market_type("40", "OGDC-FEBB") == "FUT"

    def test_cont_sector_40(self):
        assert classify_market_type("40", "OGDC-CFEB") == "CONT"

    def test_cont_b_series(self):
        assert classify_market_type("40", "OGDC-CAPRB") == "CONT"

    def test_idx_fut_sector_41(self):
        assert classify_market_type("41", "KSE30-FEB") == "IDX_FUT"

    def test_odl_sector_36(self):
        assert classify_market_type("36", "P01GIS200826") == "ODL"

    def test_whitespace_handling(self):
        assert classify_market_type(" 40 ", "OGDC-FEB") == "FUT"

    def test_sector_40_no_month(self):
        """Sector 40 with -C but no valid month code → FUT, not CONT."""
        assert classify_market_type("40", "ABC-CXYZ") == "FUT"


# ---------------------------------------------------------------------------
# parse_futures_symbol tests
# ---------------------------------------------------------------------------

class TestParseFuturesSymbol:
    def test_fut_simple(self):
        assert parse_futures_symbol("OGDC-FEB", "FUT") == ("OGDC", "FEB")

    def test_fut_b_series(self):
        assert parse_futures_symbol("OGDC-FEBB", "FUT") == ("OGDC", "FEB")

    def test_cont_simple(self):
        assert parse_futures_symbol("OGDC-CFEB", "CONT") == ("OGDC", "FEB")

    def test_cont_b_series(self):
        assert parse_futures_symbol("OGDC-CAPRB", "CONT") == ("OGDC", "APR")

    def test_idx_fut(self):
        assert parse_futures_symbol("KSE30-FEB", "IDX_FUT") == ("KSE30", "FEB")

    def test_odl(self):
        assert parse_futures_symbol("P01GIS200826", "ODL") == ("P01GIS200826", None)

    def test_reg(self):
        assert parse_futures_symbol("OGDC", "REG") == ("OGDC", None)

    def test_no_dash(self):
        assert parse_futures_symbol("NODASH", "FUT") == ("NODASH", None)


# ---------------------------------------------------------------------------
# upsert_futures_eod tests
# ---------------------------------------------------------------------------

class TestUpsertFuturesEod:
    def test_insert(self, con, sample_df):
        count = upsert_futures_eod(con, sample_df)
        assert count == 4

        rows = con.execute("SELECT COUNT(*) FROM futures_eod").fetchone()[0]
        assert rows == 4

    def test_conflict_update(self, con, sample_df):
        upsert_futures_eod(con, sample_df)

        # Update close price
        updated = sample_df.copy()
        updated.loc[updated["symbol"] == "OGDC-FEB", "close"] = 290.0
        upsert_futures_eod(con, updated)

        row = con.execute(
            "SELECT close FROM futures_eod WHERE symbol = 'OGDC-FEB'"
        ).fetchone()
        assert row[0] == 290.0

        # Still 4 rows (not 8)
        total = con.execute("SELECT COUNT(*) FROM futures_eod").fetchone()[0]
        assert total == 4

    def test_change_computed(self, con, sample_df):
        upsert_futures_eod(con, sample_df)

        row = con.execute(
            "SELECT change_value, change_pct FROM futures_eod "
            "WHERE symbol = 'OGDC-FEB'"
        ).fetchone()
        assert abs(row[0] - 2.0) < 0.01  # 282 - 280
        assert abs(row[1] - 0.714) < 0.01  # 2/280*100

    def test_empty_df(self, con):
        count = upsert_futures_eod(con, pd.DataFrame())
        assert count == 0


# ---------------------------------------------------------------------------
# Query function tests
# ---------------------------------------------------------------------------

class TestQueryFunctions:
    def test_get_futures_eod_all(self, con, sample_df):
        upsert_futures_eod(con, sample_df)
        df = get_futures_eod(con, date="2026-02-25")
        assert len(df) == 4

    def test_get_futures_eod_by_type(self, con, sample_df):
        upsert_futures_eod(con, sample_df)
        df = get_futures_eod(con, date="2026-02-25", market_type="FUT")
        assert len(df) == 1
        assert df.iloc[0]["symbol"] == "OGDC-FEB"

    def test_get_futures_eod_by_base(self, con, sample_df):
        upsert_futures_eod(con, sample_df)
        df = get_futures_eod(con, base_symbol="OGDC")
        assert len(df) == 2  # FUT + CONT

    def test_get_futures_stats(self, con, sample_df):
        upsert_futures_eod(con, sample_df)
        stats = get_futures_stats(con)
        assert stats["total_rows"] == 4
        assert stats["fut_rows"] == 1
        assert stats["cont_rows"] == 1
        assert stats["idx_fut_rows"] == 1
        assert stats["odl_rows"] == 1
        assert stats["total_dates"] == 1
        assert stats["min_date"] == "2026-02-25"

    def test_get_contract_comparison(self, con, sample_df):
        upsert_futures_eod(con, sample_df)
        df = get_contract_comparison(con, "OGDC", "2026-02-25")
        assert len(df) == 2  # FUT + CONT
        types = set(df["market_type"])
        assert types == {"FUT", "CONT"}

    def test_get_most_active(self, con, sample_df):
        upsert_futures_eod(con, sample_df)
        df = get_most_active_futures(con, "2026-02-25", limit=2)
        assert len(df) == 2
        # Should be ordered by volume desc
        assert df.iloc[0]["volume"] >= df.iloc[1]["volume"]


# ---------------------------------------------------------------------------
# Migration tests
# ---------------------------------------------------------------------------

class TestMigration:
    def _setup_eod(self, con):
        """Create eod_ohlcv with mixed REG + FUT data."""
        con.execute("""
            CREATE TABLE IF NOT EXISTS eod_ohlcv (
                symbol TEXT NOT NULL,
                date TEXT NOT NULL,
                open REAL, high REAL, low REAL, close REAL,
                volume INTEGER, prev_close REAL,
                sector_code TEXT, company_name TEXT,
                ingested_at TEXT NOT NULL, source TEXT, processname TEXT,
                PRIMARY KEY (symbol, date)
            )
        """)
        con.executemany(
            "INSERT INTO eod_ohlcv VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                ("OGDC", "2026-02-25", 280, 285, 278, 282, 50000, 280,
                 "0824", "OGDCL", "2026-02-25", "market_summary", "eodfile"),
                ("OGDC-FEB", "2026-02-25", 281, 286, 279, 283, 30000, 280,
                 "40", "OGDCL", "2026-02-25", "market_summary", "eodfile"),
                ("OGDC-CFEB", "2026-02-25", 279, 284, 277, 281, 20000, 279,
                 "40", "OGDCL", "2026-02-25", "market_summary", "eodfile"),
                ("P01GIS200826", "2026-02-25", 100, 101, 99, 100, 5, 100,
                 "36", "GIS Bond", "2026-02-25", "market_summary", "eodfile"),
            ],
        )
        con.commit()

    def test_dry_run(self, con):
        from pakfindata.db.repositories.futures import migrate_from_eod_ohlcv

        self._setup_eod(con)
        result = migrate_from_eod_ohlcv(con, dry_run=True)

        assert result["total_eligible"] == 3  # FEB + CFEB + GIS
        assert result["migrated"] == 0
        assert result["dry_run"] is True

        # eod_ohlcv unchanged
        total = con.execute("SELECT COUNT(*) FROM eod_ohlcv").fetchone()[0]
        assert total == 4

    def test_execute_migration(self, con):
        from pakfindata.db.repositories.futures import migrate_from_eod_ohlcv

        self._setup_eod(con)
        result = migrate_from_eod_ohlcv(con, dry_run=False)

        assert result["migrated"] == 3
        assert result["deleted_from_eod"] == 3

        # eod_ohlcv only has REG
        eod_rows = con.execute("SELECT COUNT(*) FROM eod_ohlcv").fetchone()[0]
        assert eod_rows == 1
        eod_sym = con.execute("SELECT symbol FROM eod_ohlcv").fetchone()[0]
        assert eod_sym == "OGDC"

        # futures_eod has 3 rows
        fut_rows = con.execute("SELECT COUNT(*) FROM futures_eod").fetchone()[0]
        assert fut_rows == 3


# ---------------------------------------------------------------------------
# ODL Symbol Parsing tests
# ---------------------------------------------------------------------------

class TestODLSymbolParsing:
    """Test parse_symbol_info with ODL-specific symbols."""

    def test_gis_1y(self):
        from pakfindata.sources.psx_debt import parse_symbol_info
        info = parse_symbol_info("P01GIS200826")
        assert info["security_type"] == "GIS"
        assert info["tenor_years"] == 1
        assert info["is_islamic"] is True
        assert info["is_government"] is True
        assert info["maturity_date"] == "2026-08-20"

    def test_vrr_10y(self):
        from pakfindata.sources.psx_debt import parse_symbol_info
        info = parse_symbol_info("P10VRR211034")
        assert info["security_type"] == "VRR Sukuk"
        assert info["tenor_years"] == 10
        assert info["maturity_date"] == "2034-10-21"
        assert info["is_islamic"] is True

    def test_frr_5y(self):
        from pakfindata.sources.psx_debt import parse_symbol_info
        info = parse_symbol_info("P05FRR070330")
        assert info["security_type"] == "FRR Sukuk"
        assert info["tenor_years"] == 5
        assert info["maturity_date"] == "2030-03-07"

    def test_frz_10y(self):
        from pakfindata.sources.psx_debt import parse_symbol_info
        info = parse_symbol_info("P10FRZ141135")
        assert info["security_type"] == "FRZ"
        assert info["tenor_years"] == 10
        assert info["maturity_date"] == "2035-11-14"
        assert info["is_islamic"] is False

    def test_gvr_3y(self):
        from pakfindata.sources.psx_debt import parse_symbol_info
        info = parse_symbol_info("P03GVR190528")
        assert info["security_type"] == "Variable GIS"
        assert info["tenor_years"] == 3
        assert info["maturity_date"] == "2028-05-19"
        assert info["is_islamic"] is True

    def test_tbill_12m(self):
        from pakfindata.sources.psx_debt import parse_symbol_info
        info = parse_symbol_info("PK12TB210127")
        assert info["security_type"] == "T-Bill"
        assert info["maturity_date"] == "2027-01-21"

    def test_pib_10y(self):
        from pakfindata.sources.psx_debt import parse_symbol_info
        info = parse_symbol_info("P10PIB150136")
        assert info["security_type"] == "PIB"
        assert info["tenor_years"] == 10
        assert info["maturity_date"] == "2036-01-15"
        assert info["is_islamic"] is False

    def test_corporate_tfc_issuer(self):
        from pakfindata.sources.psx_debt import parse_symbol_info
        info = parse_symbol_info("HBLTFC2")
        assert info["security_type"] == "TFC"
        assert info["is_government"] is False
        assert info["issuer"] == "HBL"

    def test_corporate_tfc_multi_letter(self):
        from pakfindata.sources.psx_debt import parse_symbol_info
        info = parse_symbol_info("AKBLTFC6")
        assert info["issuer"] == "AKBL"
        assert info["security_type"] == "TFC"

    def test_corporate_sukuk_issuer(self):
        from pakfindata.sources.psx_debt import parse_symbol_info
        info = parse_symbol_info("KELSC5")
        assert info["security_type"] == "Corporate Sukuk"
        assert info["is_government"] is False
        assert info["is_islamic"] is True
        assert info["issuer"] == "KEL"

    def test_pesc_edge_case(self):
        """P-prefix but not government — PESC is Pak Energy Sukuk."""
        from pakfindata.sources.psx_debt import parse_symbol_info
        info = parse_symbol_info("PESC1")
        assert info["security_type"] == "Corporate Sukuk"
        assert info["is_government"] is False

    def test_display_name_gov(self):
        from pakfindata.sources.psx_debt import parse_symbol_info
        info = parse_symbol_info("P10VRR211034")
        assert "10Y" in info["display_name"]
        assert "VRR Sukuk" in info["display_name"]
        assert "2034-10-21" in info["display_name"]

    def test_display_name_corp(self):
        from pakfindata.sources.psx_debt import parse_symbol_info
        info = parse_symbol_info("HBLTFC2")
        assert "HBL" in info["display_name"]
        assert "TFC" in info["display_name"]

    def test_display_name_with_company(self):
        from pakfindata.sources.psx_debt import parse_symbol_info, build_display_name
        info = parse_symbol_info("PESC1")
        name = build_display_name("PESC1", info, "Pak Energy(Sukuk)")
        assert name == "Pak Energy(Sukuk)"


# ---------------------------------------------------------------------------
# ODL Query function tests
# ---------------------------------------------------------------------------

class TestODLQueries:
    def test_get_odl_symbols(self, con, sample_df):
        upsert_futures_eod(con, sample_df)
        from pakfindata.db.repositories.futures import get_odl_symbols
        df = get_odl_symbols(con)
        assert len(df) == 1
        assert df.iloc[0]["symbol"] == "P01GIS200826"

    def test_get_odl_stats(self, con, sample_df):
        upsert_futures_eod(con, sample_df)
        from pakfindata.db.repositories.futures import get_odl_stats
        stats = get_odl_stats(con)
        assert stats["distinct_symbols"] == 1
        assert stats["total_rows"] == 1

    def test_get_odl_history(self, con, sample_df):
        upsert_futures_eod(con, sample_df)
        from pakfindata.db.repositories.futures import get_odl_history
        hist = get_odl_history(con, "P01GIS200826")
        assert len(hist) == 1
        assert hist.iloc[0]["close"] == 100.2

    def test_get_odl_stats_empty(self, con):
        from pakfindata.db.repositories.futures import get_odl_stats
        stats = get_odl_stats(con)
        assert stats["distinct_symbols"] == 0
        assert stats["total_rows"] == 0
