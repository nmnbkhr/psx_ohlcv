"""Tests for SBP KONIA (Overnight Repo Rate) historical PDF scraper."""

import sqlite3
import pytest
from pakfindata.sources.sbp_konia_history import (
    SBPKoniaHistoryScraper,
    _parse_date,
    _parse_rate,
)


class TestParseDate:
    """Test the various date formats found in SBP PDFs."""

    def test_dash_comma_format(self):
        assert _parse_date("25-May, 2015") == "2015-05-25"

    def test_dash_comma_no_space(self):
        assert _parse_date("01-June,2015") == "2015-06-01"

    def test_space_format(self):
        assert _parse_date("25 May 2015") == "2015-05-25"

    def test_space_long_month(self):
        assert _parse_date("1 September 2020") == "2020-09-01"

    def test_dash_format(self):
        assert _parse_date("25-May-2015") == "2015-05-25"

    def test_short_year_format(self):
        assert _parse_date("06-MAR-26") == "2026-03-06"

    def test_empty(self):
        assert _parse_date("") is None

    def test_garbage(self):
        assert _parse_date("Note: Overnignt repo") is None


class TestParseRate:
    """Test rate parsing."""

    def test_normal(self):
        assert _parse_rate("6.58") == 6.58

    def test_with_percent(self):
        assert _parse_rate("10.66%") == 10.66

    def test_out_of_range(self):
        assert _parse_rate("100.0") is None

    def test_zero(self):
        assert _parse_rate("0") is None

    def test_garbage(self):
        assert _parse_rate("abc") is None


class TestScraper:
    """Integration tests for the KONIA scraper (requires network)."""

    @pytest.fixture
    def scraper(self):
        return SBPKoniaHistoryScraper()

    @pytest.fixture
    def mem_db(self):
        con = sqlite3.connect(":memory:")
        con.row_factory = sqlite3.Row
        con.executescript("""
            CREATE TABLE IF NOT EXISTS konia_daily (
                date TEXT PRIMARY KEY,
                rate_pct REAL NOT NULL,
                volume_billions REAL,
                high REAL,
                low REAL,
                scraped_at TEXT DEFAULT (datetime('now'))
            );
        """)
        return con

    @pytest.mark.integration
    def test_download_archive(self, scraper):
        pdf_bytes = scraper.download_archive()
        assert len(pdf_bytes) > 100_000
        assert pdf_bytes[:4] == b"%PDF"

    @pytest.mark.integration
    def test_parse_archive(self, scraper):
        pdf_bytes = scraper.download_archive()
        records = scraper.parse_archive_pdf(pdf_bytes)
        assert len(records) > 2000
        # Check structure
        for r in records[:10]:
            assert "date" in r
            assert "rate_pct" in r
            assert 0 < r["rate_pct"] < 30
            assert len(r["date"]) == 10  # YYYY-MM-DD
        # Check sorted
        dates = [r["date"] for r in records]
        assert dates == sorted(dates)

    @pytest.mark.integration
    def test_parse_current(self, scraper):
        pdf_bytes = scraper.download_current()
        result = scraper.parse_current_pdf(pdf_bytes)
        assert result is not None
        assert 0 < result["rate_pct"] < 30

    @pytest.mark.integration
    def test_sync_to_db(self, scraper, mem_db):
        pdf_bytes = scraper.download_archive()
        result = scraper.sync_konia_history(mem_db, pdf_bytes=pdf_bytes)
        assert result["inserted"] > 2000
        assert result["failed"] == 0
        # Verify DB
        count = mem_db.execute("SELECT COUNT(*) FROM konia_daily").fetchone()[0]
        assert count == result["inserted"]
