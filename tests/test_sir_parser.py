"""Tests for SBP SIR PDF parser."""

import sqlite3

import pytest

from pakfindata.sources.sbp_sir import (
    SBPSirScraper,
    _parse_float,
    _parse_month_date,
    _parse_sir_date,
)

# ── Helper tests ─────────────────────────────────────────────────


class TestParseSirDate:
    def test_standard_format(self):
        assert _parse_sir_date("12-Jun-24") == "2024-06-12"

    def test_four_digit_year(self):
        assert _parse_sir_date("25-May-2022") == "2022-05-25"

    def test_single_digit_day(self):
        assert _parse_sir_date("4-Feb-26") == "2026-02-04"

    def test_empty(self):
        assert _parse_sir_date("") is None
        assert _parse_sir_date(None) is None

    def test_garbage(self):
        assert _parse_sir_date("Monthly Average") is None


class TestParseMonthDate:
    def test_standard(self):
        assert _parse_month_date("Jan-25") == "2025-01-01"

    def test_dec(self):
        assert _parse_month_date("Dec-25") == "2025-12-01"

    def test_empty(self):
        assert _parse_month_date("") is None


class TestParseFloat:
    def test_number(self):
        assert _parse_float("10.26") == 10.26

    def test_na(self):
        assert _parse_float("NA") is None

    def test_r(self):
        assert _parse_float("R") is None

    def test_n(self):
        assert _parse_float("N") is None

    def test_empty(self):
        assert _parse_float("") is None
        assert _parse_float(None) is None


# ── Page parser tests ────────────────────────────────────────────

@pytest.fixture
def con():
    """In-memory SQLite connection with required schemas."""
    from pakfindata.db.repositories.treasury import init_treasury_schema
    from pakfindata.db.repositories.yield_curves import init_yield_curve_schema

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    init_treasury_schema(conn)
    init_yield_curve_schema(conn)
    yield conn
    conn.close()


class TestParserPage2:
    """Test page 2 parsing (T-Bills + KIBOR) using a mock table."""

    def test_parse_tbill_rows(self):
        """Verify T-Bill rows are extracted from a simulated page table."""
        scraper = SBPSirScraper()

        # Simulate what pdfplumber returns for a page with extract_tables
        class MockPage:
            def extract_tables(self, settings=None):
                return [[
                    # Header rows (should be skipped)
                    ["Date", "1-m", "3-m", "6-m", "12-m", "1-m", "3-m", "6-m", "12-m",
                     "Date", "1-m", "3-m", "6-m"],
                    # Data row
                    ["4-Feb-26", "10.20", "10.20", "10.32", "10.40",
                     "10.18", "10.10", "10.15", "10.35",
                     "13-Feb-26", "10.75", "10.51", "10.53"],
                    # Row with R (rejected)
                    ["18-Sep-24", "NA", "R", "R", "R",
                     "NA", "17.41", "17.62", "16.83",
                     "Jul-25", "11.30", "11.02", "10.98"],
                ]]

        tbills, kibor = scraper._parse_page2(MockPage())

        # T-Bill assertions
        assert len(tbills) >= 4  # 4 tenors from first data row
        feb_rows = [t for t in tbills if t["auction_date"] == "2026-02-04"]
        assert len(feb_rows) == 4
        assert feb_rows[0]["tenor"] == "1M"
        assert feb_rows[0]["cutoff_yield"] == 10.20
        assert feb_rows[1]["tenor"] == "3M"

        # Rejected bids should have None cutoff but WA yield present
        sep_rows = [t for t in tbills if t["auction_date"] == "2024-09-18"]
        na_3m = [t for t in sep_rows if t["tenor"] == "3M"]
        assert len(na_3m) == 1
        assert na_3m[0]["cutoff_yield"] is None  # R = rejected
        assert na_3m[0]["weighted_avg_yield"] == 17.41

        # KIBOR assertions
        assert len(kibor) >= 3
        feb_kibor = [k for k in kibor if k["date"] == "2026-02-13"]
        assert len(feb_kibor) == 3
        assert feb_kibor[0]["tenor"] == "1M"
        assert feb_kibor[0]["offer"] == 10.75

        # Monthly KIBOR
        jul_kibor = [k for k in kibor if k["date"] == "2025-07-01"]
        assert len(jul_kibor) == 3


class TestParserPage3:
    """Test page 3 parsing (PIBs)."""

    def test_parse_pib_rows(self):
        scraper = SBPSirScraper()

        class MockPage:
            def extract_tables(self, settings=None):
                return [[
                    # Header
                    ["Date", "2-y #", "3-y", "5-y", "10-y", "15-y", "20-y", "30-y",
                     "2-y #", "3-y", "5-y", "10-y", "15-y", "20-y", "30-y"],
                    # Data
                    ["25-May-22", "NA", "14.00", "13.19", "R", "R", "N", "N",
                     "NA", "13.95", "13.04", "R", "R", "N", "N"],
                    ["6-Feb-26", "10.34", "10.25", "10.75", "11.24", "11.50", "NA", "NA",
                     "10.32", "10.21", "10.71", "11.18", "11.43", "NA", "NA"],
                ]]

        pibs = scraper._parse_page3(MockPage())

        assert len(pibs) > 0

        # Check May 2022 row — 3Y and 5Y present, 10Y rejected
        may_rows = [p for p in pibs if p["auction_date"] == "2022-05-25"]
        tenors = {p["tenor"]: p for p in may_rows}
        assert "3Y" in tenors
        assert tenors["3Y"]["cutoff_yield"] == 14.00
        assert tenors["3Y"]["weighted_avg_yield"] == 13.95
        assert "5Y" in tenors

        # Feb 2026 row
        feb_rows = [p for p in pibs if p["auction_date"] == "2026-02-06"]
        assert len(feb_rows) >= 4  # 2Y, 3Y, 5Y, 10Y, 15Y


class TestSyncSir:
    """Test sync to DB."""

    def test_sync_writes_to_db(self, con):
        scraper = SBPSirScraper()

        # Create minimal fake data
        data = {
            "tbills": [{
                "auction_date": "2026-02-04",
                "tenor": "3M",
                "cutoff_yield": 10.20,
                "weighted_avg_yield": 10.10,
            }],
            "pibs": [{
                "auction_date": "2026-02-06",
                "tenor": "5Y",
                "pib_type": "Fixed",
                "cutoff_yield": 10.75,
                "weighted_avg_yield": 10.71,
            }],
            "kibor": [{
                "date": "2026-02-13",
                "tenor": "3M",
                "offer": 10.51,
                "bid": None,
            }],
            "gis_variable": [{
                "auction_date": "2023-11-30",
                "gis_type": "GIS Variable Rate Return",
                "tenor": "3Y",
                "cutoff_rental_rate": 21.12,
            }],
            "gis_fixed": [],
        }

        # Monkey-patch scrape_sir to return our test data
        scraper.scrape_sir = lambda pdf_bytes=None: data
        counts = scraper.sync_sir(con)

        assert counts["tbills"] == 1
        assert counts["pibs"] == 1
        assert counts["kibor"] == 1
        assert counts["gis"] == 1
        assert counts["failed"] == 0

        # Verify in DB
        row = con.execute(
            "SELECT cutoff_yield FROM tbill_auctions WHERE tenor='3M'"
        ).fetchone()
        assert row["cutoff_yield"] == 10.20

        row = con.execute(
            "SELECT cutoff_yield FROM pib_auctions WHERE tenor='5Y'"
        ).fetchone()
        assert row["cutoff_yield"] == 10.75

        row = con.execute(
            "SELECT offer FROM kibor_daily WHERE tenor='3M' AND date='2026-02-13'"
        ).fetchone()
        assert row["offer"] == 10.51
