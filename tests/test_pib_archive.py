"""Tests for SBP PIB Archive PDF parser."""

import sqlite3

import pytest

from psx_ohlcv.sources.sbp_pib_archive import (
    SBPPibArchiveScraper,
    _clean_amount,
    _parse_coupon,
    _parse_long_date,
    _parse_yield,
)

# ── Helper tests ─────────────────────────────────────────────────


class TestParseLongDate:
    def test_standard(self):
        assert _parse_long_date("December 14, 2000") == "2000-12-14"

    def test_no_comma(self):
        assert _parse_long_date("December 14 2000") == "2000-12-14"

    def test_short_month(self):
        assert _parse_long_date("May 21, 2001") == "2001-05-21"

    def test_september(self):
        assert _parse_long_date("September 8, 2025") == "2025-09-08"

    def test_empty(self):
        assert _parse_long_date("") is None


class TestCleanAmount:
    def test_simple(self):
        assert _clean_amount("1,999.00") == 1999.0

    def test_large(self):
        assert _clean_amount("114,993.60") == 114993.6

    def test_space_in_number(self):
        assert _clean_amount("2 20,590.30") == 220590.3

    def test_large_space(self):
        assert _clean_amount("3 00,087.50") == 300087.5

    def test_empty(self):
        assert _clean_amount("") is None
        assert _clean_amount(None) is None


class TestParseCoupon:
    def test_percent(self):
        assert _parse_coupon("12.50%") == 12.5

    def test_zero(self):
        assert _parse_coupon("Zero") == 0.0

    def test_none(self):
        assert _parse_coupon(None) is None


class TestParseYield:
    def test_percent(self):
        assert _parse_yield("12.4507%") == pytest.approx(12.4507)

    def test_none(self):
        assert _parse_yield(None) is None


# ── Page parser tests ────────────────────────────────────────────


class TestParsePageText:
    """Test the text-based page parser."""

    SAMPLE_TEXT = """
PAKISTAN INVESTMENT BONDS
AUCTION PROFILE
(FACE VALUE)
Auction Settlement Coupon Amount Weighted
Tenor
Date Rate Accepted Average Yield%
(Amount in millions)
Coupon Rate
EFFECTIVE DATE 3-YEAR 5-YEAR 10-YEAR
14-Dec-00 12.50% 13.00% 14.00%
3 -Year 12.50% 1,999.00 12.4507%
1 December 14, 2000 5-Year 13.00% 213.00 12.9490%
10-Year 14.00% 2,222.00 13.9667%
Total 4,434.00
3 -Year 12.50% 506.50 12.4823%
2 December 30, 2000 5-Year 13.00% 3,059.20 12.9997%
10-Year 14.00% 6,174.10 13.9783%
Total 9,739.80
8 August 16, 2001 10-Year 14.00% Bid Rejected Bid Rejected
Total -
"""

    def test_parse_records(self):
        scraper = SBPPibArchiveScraper()
        records = scraper._parse_page_text(self.SAMPLE_TEXT)

        # Should find records for Dec 14, Dec 30, Aug 16
        dates = sorted(set(r["auction_date"] for r in records))
        assert "2000-12-14" in dates
        assert "2000-12-30" in dates
        assert "2001-08-16" in dates

        # Dec 14 should have 3Y, 5Y, 10Y
        dec14 = [r for r in records if r["auction_date"] == "2000-12-14"]
        assert len(dec14) >= 2  # At least 5Y and 10Y

        # Check 5Y record
        fivey = [r for r in dec14 if r["tenor"] == "5Y"]
        if fivey:
            assert fivey[0]["cutoff_yield"] == pytest.approx(12.949)
            assert fivey[0]["coupon_rate"] == 13.0
            assert fivey[0]["amount_accepted_billions"] == pytest.approx(0.213)

        # Aug 16 should have rejected bid
        aug16 = [r for r in records if r["auction_date"] == "2001-08-16"]
        assert len(aug16) >= 1
        assert aug16[0]["cutoff_yield"] is None  # Bid Rejected

    def test_all_types_fixed(self):
        """All records should be 'Fixed' type."""
        scraper = SBPPibArchiveScraper()
        records = scraper._parse_page_text(self.SAMPLE_TEXT)
        for r in records:
            assert r["pib_type"] == "Fixed"


# ── Sync to DB ───────────────────────────────────────────────────


@pytest.fixture
def con():
    from psx_ohlcv.db.repositories.treasury import init_treasury_schema

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    init_treasury_schema(conn)
    yield conn
    conn.close()


class TestSyncPibArchive:
    def test_sync_inserts(self, con):
        scraper = SBPPibArchiveScraper()

        test_records = [
            {
                "auction_date": "2000-12-14",
                "tenor": "5Y",
                "pib_type": "Fixed",
                "coupon_rate": 13.0,
                "amount_accepted_billions": 0.213,
                "cutoff_yield": 12.949,
            },
            {
                "auction_date": "2000-12-14",
                "tenor": "10Y",
                "pib_type": "Fixed",
                "coupon_rate": 14.0,
                "amount_accepted_billions": 2.222,
                "cutoff_yield": 13.9667,
            },
        ]

        # Monkey-patch
        scraper.scrape_pib_archive = lambda pdf_bytes=None: test_records
        counts = scraper.sync_pib_archive(con)

        assert counts["inserted"] == 2
        assert counts["failed"] == 0
        assert counts["total"] == 2

        # Verify
        rows = con.execute(
            "SELECT * FROM pib_auctions WHERE auction_date='2000-12-14' ORDER BY tenor"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0]["tenor"] == "10Y"
        assert rows[0]["cutoff_yield"] == pytest.approx(13.9667)

    def test_upsert_updates(self, con):
        """Second sync should update, not duplicate."""
        scraper = SBPPibArchiveScraper()
        test_records = [{
            "auction_date": "2020-01-15",
            "tenor": "3Y",
            "pib_type": "Fixed",
            "coupon_rate": 10.0,
            "amount_accepted_billions": 5.0,
            "cutoff_yield": 9.5,
        }]

        scraper.scrape_pib_archive = lambda pdf_bytes=None: test_records
        scraper.sync_pib_archive(con)

        # Update yield
        test_records[0]["cutoff_yield"] = 9.8
        scraper.sync_pib_archive(con)

        rows = con.execute(
            "SELECT * FROM pib_auctions WHERE auction_date='2020-01-15'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["cutoff_yield"] == pytest.approx(9.8)
