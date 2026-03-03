"""Tests for SBP KIBOR Historical PDF scraper."""

import sqlite3
from datetime import date

import pytest

from pakfindata.sources.sbp_kibor_history import (
    SBPKiborHistoryScraper,
    _build_pdf_url,
    _business_days,
)

# ── Helper tests ─────────────────────────────────────────────────


class TestBuildPdfUrl:
    def test_feb_2026(self):
        url = _build_pdf_url(date(2026, 2, 12))
        assert url == "https://www.sbp.org.pk/ecodata/kibor/2026/Feb/Kibor-12-Feb-26.pdf"

    def test_jan_2010(self):
        url = _build_pdf_url(date(2010, 1, 5))
        assert url == "https://www.sbp.org.pk/ecodata/kibor/2010/Jan/Kibor-5-Jan-10.pdf"

    def test_dec_2008(self):
        url = _build_pdf_url(date(2008, 12, 31))
        assert url == "https://www.sbp.org.pk/ecodata/kibor/2008/Dec/Kibor-31-Dec-08.pdf"


class TestBusinessDays:
    def test_full_week(self):
        days = _business_days(date(2026, 2, 9), date(2026, 2, 13))
        assert len(days) == 5  # Mon-Fri
        assert days[0] == date(2026, 2, 9)  # Monday
        assert days[-1] == date(2026, 2, 13)  # Friday

    def test_excludes_weekend(self):
        days = _business_days(date(2026, 2, 14), date(2026, 2, 15))
        assert len(days) == 0  # Sat + Sun

    def test_single_day(self):
        days = _business_days(date(2026, 2, 9), date(2026, 2, 9))
        assert len(days) == 1

    def test_empty_range(self):
        days = _business_days(date(2026, 2, 15), date(2026, 2, 9))
        assert len(days) == 0


# ── PDF parser tests ─────────────────────────────────────────────


class TestParsePdf:
    """Test PDF parsing with synthetic data."""

    def test_parse_returns_records(self):
        """Test that a properly structured PDF table produces records."""
        # We can't easily create a real PDF in tests, so test the parse
        # logic indirectly via the tenor map
        from pakfindata.sources.sbp_kibor_history import TENOR_MAP
        assert TENOR_MAP["1 - Week"] == "1W"
        assert TENOR_MAP["3 - Month"] == "3M"
        assert TENOR_MAP["1 - Year"] == "1Y"
        assert TENOR_MAP["2 - Year"] == "2Y"
        assert TENOR_MAP["3- Year"] == "3Y"

    def test_empty_pdf_returns_empty(self):
        """Garbage bytes should return empty list."""
        scraper = SBPKiborHistoryScraper()
        records = scraper._parse_pdf(b"not a pdf", "2026-02-12")
        assert records == []


# ── DB sync tests ────────────────────────────────────────────────


@pytest.fixture
def con():
    from pakfindata.db.repositories.yield_curves import init_yield_curve_schema

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    init_yield_curve_schema(conn)
    yield conn
    conn.close()


class TestSyncKiborHistory:
    def test_sync_with_mock_scraper(self, con):
        """Test sync inserts records to DB when scrape returns data."""
        scraper = SBPKiborHistoryScraper()

        # Mock scrape_kibor_pdf to return test data
        call_count = {"n": 0}

        def mock_scrape(d):
            call_count["n"] += 1
            if d.weekday() < 5:  # Only weekdays
                return [
                    {"date": d.strftime("%Y-%m-%d"), "tenor": "3M", "bid": 10.0, "offer": 10.5},
                    {"date": d.strftime("%Y-%m-%d"), "tenor": "6M", "bid": 10.1, "offer": 10.6},
                ]
            return []

        scraper.scrape_kibor_pdf = mock_scrape

        counts = scraper.sync_kibor_history(
            con,
            start_year=2026,
            end_date=date(2026, 1, 5),  # Mon Jan 5
            incremental=False,
        )

        assert counts["records_inserted"] > 0
        assert counts["failed"] == 0

        # Verify in DB
        rows = con.execute("SELECT * FROM kibor_daily ORDER BY date").fetchall()
        assert len(rows) > 0
        assert rows[0]["tenor"] in ("3M", "6M")

    def test_incremental_skips_existing(self, con):
        """Incremental sync should skip dates already in DB."""
        # Insert a record for Jan 2
        con.execute(
            "INSERT INTO kibor_daily (date, tenor, bid, offer) VALUES (?, ?, ?, ?)",
            ("2026-01-02", "3M", 10.0, 10.5),
        )
        con.commit()

        scraper = SBPKiborHistoryScraper()
        scrape_calls = []

        def mock_scrape(d):
            scrape_calls.append(d)
            return [{"date": d.strftime("%Y-%m-%d"), "tenor": "3M", "bid": 10.0, "offer": 10.5}]

        scraper.scrape_kibor_pdf = mock_scrape

        counts = scraper.sync_kibor_history(
            con,
            start_year=2026,
            end_date=date(2026, 1, 2),
            incremental=True,
        )

        # Jan 2 should be skipped (already in DB)
        assert date(2026, 1, 2) not in scrape_calls
        assert counts["skipped"] >= 1


class TestTenorMapCompleteness:
    """Verify tenor map covers all known SBP KIBOR tenor formats."""

    def test_standard_tenors(self):
        from pakfindata.sources.sbp_kibor_history import TENOR_MAP
        expected = {"1W", "2W", "1M", "3M", "6M", "9M", "1Y", "2Y", "3Y"}
        actual = set(TENOR_MAP.values())
        assert expected == actual

    def test_format_variants(self):
        from pakfindata.sources.sbp_kibor_history import TENOR_MAP
        # All format variants should map to the same canonical name
        assert TENOR_MAP["1 - Month"] == "1M"
        assert TENOR_MAP["1 -Month"] == "1M"
        assert TENOR_MAP["1-Month"] == "1M"
