"""Tests for company page parsing and database operations."""

import sqlite3

import pytest

from psx_ohlcv.db import (
    get_company_key_people,
    get_company_profile,
    get_last_quote_hash,
    get_quote_snapshots,
    init_schema,
    insert_quote_snapshot,
    replace_company_key_people,
    upsert_company_profile,
)
from psx_ohlcv.sources.company_page import (
    _compute_raw_hash,
    _parse_numeric,
    _parse_range,
    parse_company_profile,
    parse_company_quote,
    parse_key_people,
)


@pytest.fixture
def db_connection():
    """Create in-memory database with schema initialized."""
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    init_schema(con)
    return con


# Sample HTML fixtures - using actual DPS structure
SAMPLE_QUOTE_HTML = """
<!DOCTYPE html>
<html>
<head><title>OGDC - PSX</title></head>
<body>
<div class="quote__details">
    <div class="quote__name">Oil & Gas Development Company Limited</div>
    <div class="quote__sector"><span>OIL & GAS EXPLORATION COMPANIES</span></div>
    <div class="quote__price">
        <div class="quote__close">Rs.331.26</div>
        <div class="quote__change change__text--neg">
            <div class="change__value">-2.64</div>
            <div class="change__percent">(-0.79%)</div>
        </div>
    </div>
    <div class="quote__date">^ As of Wed, Jan 21, 2026 3:49 PM</div>
</div>
<div class="tabs__panel" data-name="REG">
    <div class="stats stats--noborder">
        <div class="stats_item">
            <div class="stats_label">Open</div>
            <div class="stats_value">334.10</div>
        </div>
        <div class="stats_item">
            <div class="stats_label">High</div>
            <div class="stats_value">336.60</div>
        </div>
        <div class="stats_item">
            <div class="stats_label">Low</div>
            <div class="stats_value">330.00</div>
        </div>
        <div class="stats_item">
            <div class="stats_label">Volume</div>
            <div class="stats_value">8,140,937</div>
        </div>
    </div>
    <div class="stats company__quote__rangeStats">
        <div class="stats_item">
            <div class="stats_label">CIRCUIT BREAKER</div>
            <div class="stats_value">300.51 — 367.29</div>
        </div>
        <div class="stats_item">
            <div class="stats_label">DAY RANGE</div>
            <div class="stats_value">330.00 — 336.60</div>
        </div>
        <div class="stats_item">
            <div class="stats_label">52-WEEK RANGE ^</div>
            <div class="stats_value">174.26 — 337.10</div>
        </div>
    </div>
</div>
</body>
</html>
"""

SAMPLE_PROFILE_HTML = """
<!DOCTYPE html>
<html>
<body>
<div class="quote__details">
    <div class="quote__name">Oil & Gas Development Company Limited</div>
    <div class="quote__sector"><span>OIL & GAS EXPLORATION COMPANIES</span></div>
</div>
<div class="company__profile">
    <div class="profile__items">
        <div class="profile__item profile__item--decription">
            <div class="item__head">BUSINESS DESCRIPTION</div>
            <p>OGDC was established in 1997 as a public limited company engaged
            in oil and gas exploration and production activities. The company
            operates several oil and gas fields across Pakistan.</p>
        </div>
    </div>
    <div class="profile__items">
        <div class="profile__item">
            <div class="item__head">ADDRESS</div>
            <p>OGDCL House, Plot No 3, F-6/G-6, Blue Area</p>
            <div class="item__head">WEBSITE</div>
            <p><a href="https://www.ogdcl.com">www.ogdcl.com</a></p>
        </div>
        <div class="profile__item">
            <div class="item__head">REGISTRAR</div>
            <p>CDC Share Registrar Services</p>
        </div>
        <div class="profile__item">
            <div class="item__head">AUDITOR</div>
            <p>A.F. Ferguson & Co.</p>
            <div class="item__head">Fiscal Year End</div>
            <p>June</p>
        </div>
    </div>
</div>
</body>
</html>
"""

SAMPLE_KEY_PEOPLE_HTML = """
<!DOCTYPE html>
<html>
<body>
<div class="company__profile">
    <div class="profile__items">
        <div class="profile__item profile__item--people">
            <div class="item__head">KEY PEOPLE</div>
            <table class="tbl"><tbody class="tbl__body">
                    <tr>
                        <td><strong>Ahmed Hayat Lak</strong></td>
                        <td>CEO</td>
                    </tr>
                    <tr>
                        <td><strong>Zafar Masud</strong></td>
                        <td>Chairman</td>
                    </tr>
                    <tr>
                        <td><strong>Wasim Ahmad</strong></td>
                        <td>Company Secretary</td>
                    </tr>
                    <tr>
                        <td><strong>Muhammad Ali</strong></td>
                        <td>CFO</td>
                    </tr>
            </tbody></table>
        </div>
    </div>
</div>
</body>
</html>
"""

EMPTY_HTML = """
<!DOCTYPE html>
<html>
<body>
<div>No data available</div>
</body>
</html>
"""


class TestParseNumeric:
    """Tests for _parse_numeric helper function."""

    def test_parse_integer(self):
        """Parse plain integer."""
        assert _parse_numeric("100") == 100.0

    def test_parse_float(self):
        """Parse decimal number."""
        assert _parse_numeric("331.26") == 331.26

    def test_parse_with_commas(self):
        """Parse number with thousand separators."""
        assert _parse_numeric("8,140,937") == 8140937.0

    def test_parse_with_percentage(self):
        """Parse number with percentage sign."""
        assert _parse_numeric("-0.79%") == -0.79

    def test_parse_negative(self):
        """Parse negative number."""
        assert _parse_numeric("-2.64") == -2.64

    def test_parse_none(self):
        """None input returns None."""
        assert _parse_numeric(None) is None

    def test_parse_empty(self):
        """Empty string returns None."""
        assert _parse_numeric("") is None

    def test_parse_dash(self):
        """Dash (missing value) returns None."""
        assert _parse_numeric("-") is None
        assert _parse_numeric("—") is None


class TestParseRange:
    """Tests for _parse_range helper function."""

    def test_parse_em_dash_range(self):
        """Parse range with em-dash separator."""
        low, high = _parse_range("330.00 — 336.60")
        assert low == 330.00
        assert high == 336.60

    def test_parse_hyphen_range(self):
        """Parse range with hyphen separator."""
        low, high = _parse_range("174.26 - 337.10")
        assert low == 174.26
        assert high == 337.10

    def test_parse_none(self):
        """None input returns None tuple."""
        assert _parse_range(None) == (None, None)

    def test_parse_empty(self):
        """Empty string returns None tuple."""
        assert _parse_range("") == (None, None)


class TestComputeRawHash:
    """Tests for _compute_raw_hash function."""

    def test_hash_is_deterministic(self):
        """Same input produces same hash."""
        quote = {"price": 100, "change": 5, "volume": 1000}
        hash1 = _compute_raw_hash(quote)
        hash2 = _compute_raw_hash(quote)
        assert hash1 == hash2

    def test_hash_differs_for_different_input(self):
        """Different input produces different hash."""
        quote1 = {"price": 100, "change": 5}
        quote2 = {"price": 101, "change": 5}
        assert _compute_raw_hash(quote1) != _compute_raw_hash(quote2)

    def test_hash_length(self):
        """Hash is 16 characters."""
        quote = {"price": 100}
        assert len(_compute_raw_hash(quote)) == 16


class TestParseCompanyQuote:
    """Tests for parse_company_quote function."""

    def test_parse_price(self):
        """Parse price from quote HTML."""
        quote = parse_company_quote(SAMPLE_QUOTE_HTML)
        assert quote.get("price") == 331.26

    def test_parse_change(self):
        """Parse change values from quote HTML."""
        quote = parse_company_quote(SAMPLE_QUOTE_HTML)
        assert quote.get("change") == -2.64
        assert quote.get("change_pct") == -0.79

    def test_parse_ohlv(self):
        """Parse OHLV values from quote HTML."""
        quote = parse_company_quote(SAMPLE_QUOTE_HTML)
        assert quote.get("open") == 334.10
        assert quote.get("high") == 336.60
        assert quote.get("low") == 330.00
        assert quote.get("volume") == 8140937

    def test_parse_day_range(self):
        """Parse day range from quote HTML."""
        quote = parse_company_quote(SAMPLE_QUOTE_HTML)
        assert quote.get("day_range_low") == 330.00
        assert quote.get("day_range_high") == 336.60

    def test_parse_52week_range(self):
        """Parse 52-week range from quote HTML."""
        quote = parse_company_quote(SAMPLE_QUOTE_HTML)
        assert quote.get("wk52_low") == 174.26
        assert quote.get("wk52_high") == 337.10

    def test_parse_circuit_breaker(self):
        """Parse circuit breaker range from quote HTML."""
        quote = parse_company_quote(SAMPLE_QUOTE_HTML)
        assert quote.get("circuit_low") == 300.51
        assert quote.get("circuit_high") == 367.29

    def test_parse_as_of(self):
        """Parse as_of timestamp from quote HTML."""
        quote = parse_company_quote(SAMPLE_QUOTE_HTML)
        assert "Jan 21, 2026" in (quote.get("as_of") or "")

    def test_raw_hash_included(self):
        """Raw hash is computed and included."""
        quote = parse_company_quote(SAMPLE_QUOTE_HTML)
        assert "raw_hash" in quote
        assert len(quote["raw_hash"]) == 16

    def test_parse_empty_html(self):
        """Parse empty HTML returns dict with raw_hash."""
        quote = parse_company_quote(EMPTY_HTML)
        assert "raw_hash" in quote


class TestParseCompanyProfile:
    """Tests for parse_company_profile function."""

    def test_parse_company_name(self):
        """Parse company name from profile HTML."""
        profile = parse_company_profile(SAMPLE_PROFILE_HTML)
        assert "Oil & Gas Development" in (profile.get("company_name") or "")

    def test_parse_sector_name(self):
        """Parse sector name from profile HTML."""
        profile = parse_company_profile(SAMPLE_PROFILE_HTML)
        assert "OIL & GAS" in (profile.get("sector_name") or "")

    def test_parse_business_description(self):
        """Parse business description from profile HTML."""
        profile = parse_company_profile(SAMPLE_PROFILE_HTML)
        desc = profile.get("business_description") or ""
        assert "1997" in desc or "oil and gas" in desc.lower()

    def test_parse_address(self):
        """Parse address from profile HTML."""
        profile = parse_company_profile(SAMPLE_PROFILE_HTML)
        assert "OGDCL House" in (profile.get("address") or "")

    def test_parse_website(self):
        """Parse website from profile HTML."""
        profile = parse_company_profile(SAMPLE_PROFILE_HTML)
        assert "ogdcl.com" in (profile.get("website") or "")

    def test_parse_registrar(self):
        """Parse registrar from profile HTML."""
        profile = parse_company_profile(SAMPLE_PROFILE_HTML)
        assert "CDC" in (profile.get("registrar") or "")

    def test_parse_auditor(self):
        """Parse auditor from profile HTML."""
        profile = parse_company_profile(SAMPLE_PROFILE_HTML)
        assert "Ferguson" in (profile.get("auditor") or "")

    def test_parse_fiscal_year_end(self):
        """Parse fiscal year end from profile HTML."""
        profile = parse_company_profile(SAMPLE_PROFILE_HTML)
        assert "June" in (profile.get("fiscal_year_end") or "")

    def test_parse_empty_html(self):
        """Parse empty HTML returns empty dict."""
        profile = parse_company_profile(EMPTY_HTML)
        assert isinstance(profile, dict)


class TestParseKeyPeople:
    """Tests for parse_key_people function."""

    def test_parse_ceo(self):
        """Parse CEO from key people HTML."""
        people = parse_key_people(SAMPLE_KEY_PEOPLE_HTML)
        ceo = next((p for p in people if p["role"] == "CEO"), None)
        assert ceo is not None
        assert "Ahmed" in ceo["name"] or "Hayat" in ceo["name"]

    def test_parse_chairman(self):
        """Parse Chairman from key people HTML."""
        people = parse_key_people(SAMPLE_KEY_PEOPLE_HTML)
        chairman = next((p for p in people if "Chairman" in p["role"]), None)
        assert chairman is not None

    def test_parse_secretary(self):
        """Parse Company Secretary from key people HTML."""
        people = parse_key_people(SAMPLE_KEY_PEOPLE_HTML)
        secretary = next(
            (p for p in people if "Secretary" in p["role"]), None
        )
        assert secretary is not None

    def test_parse_empty_html(self):
        """Parse empty HTML returns empty list."""
        people = parse_key_people(EMPTY_HTML)
        assert people == []

    def test_returns_list(self):
        """Returns a list of dicts."""
        people = parse_key_people(SAMPLE_KEY_PEOPLE_HTML)
        assert isinstance(people, list)
        if people:
            assert isinstance(people[0], dict)
            assert "role" in people[0]
            assert "name" in people[0]


class TestUpsertCompanyProfile:
    """Tests for upsert_company_profile database function."""

    def test_insert_profile(self, db_connection):
        """Insert new company profile."""
        profile = {
            "symbol": "OGDC",
            "company_name": "Oil & Gas Development Company",
            "sector_name": "OIL & GAS EXPLORATION",
            "address": "Islamabad",
            "source_url": "https://dps.psx.com.pk/company/OGDC",
        }
        count = upsert_company_profile(db_connection, profile)
        assert count >= 0

        # Verify stored
        stored = get_company_profile(db_connection, "OGDC")
        assert stored is not None
        assert stored["company_name"] == "Oil & Gas Development Company"

    def test_update_profile(self, db_connection):
        """Update existing company profile."""
        profile1 = {
            "symbol": "OGDC",
            "company_name": "OGDC",
            "source_url": "https://dps.psx.com.pk/company/OGDC",
        }
        upsert_company_profile(db_connection, profile1)

        profile2 = {
            "symbol": "OGDC",
            "company_name": "Oil & Gas Development Company Limited",
            "source_url": "https://dps.psx.com.pk/company/OGDC",
        }
        upsert_company_profile(db_connection, profile2)

        stored = get_company_profile(db_connection, "OGDC")
        assert "Limited" in stored["company_name"]

    def test_symbol_required(self, db_connection):
        """Symbol is required."""
        profile = {"company_name": "Test"}
        with pytest.raises(ValueError):
            upsert_company_profile(db_connection, profile)


class TestReplaceCompanyKeyPeople:
    """Tests for replace_company_key_people database function."""

    def test_insert_key_people(self, db_connection):
        """Insert key people for a company."""
        people = [
            {"role": "CEO", "name": "John Doe"},
            {"role": "CFO", "name": "Jane Smith"},
        ]
        count = replace_company_key_people(db_connection, "OGDC", people)
        assert count == 2

        stored = get_company_key_people(db_connection, "OGDC")
        assert len(stored) == 2

    def test_replace_existing(self, db_connection):
        """Replace existing key people."""
        people1 = [{"role": "CEO", "name": "Old CEO"}]
        replace_company_key_people(db_connection, "OGDC", people1)

        people2 = [{"role": "CEO", "name": "New CEO"}]
        replace_company_key_people(db_connection, "OGDC", people2)

        stored = get_company_key_people(db_connection, "OGDC")
        assert len(stored) == 1
        assert stored[0]["name"] == "New CEO"

    def test_empty_list(self, db_connection):
        """Empty list clears key people."""
        people = [{"role": "CEO", "name": "John Doe"}]
        replace_company_key_people(db_connection, "OGDC", people)
        replace_company_key_people(db_connection, "OGDC", [])

        stored = get_company_key_people(db_connection, "OGDC")
        assert len(stored) == 0


class TestInsertQuoteSnapshot:
    """Tests for insert_quote_snapshot database function."""

    def test_insert_snapshot(self, db_connection):
        """Insert a quote snapshot."""
        quote = {
            "price": 331.26,
            "change": -2.64,
            "change_pct": -0.79,
            "volume": 8140937,
            "raw_hash": "abc123",
        }
        ts = "2026-01-21T15:49:00+05:00"
        result = insert_quote_snapshot(db_connection, "OGDC", ts, quote)
        assert result is True

    def test_duplicate_ts_skipped(self, db_connection):
        """Duplicate timestamp is skipped."""
        quote = {"price": 100, "raw_hash": "abc123"}
        ts = "2026-01-21T15:49:00+05:00"

        result1 = insert_quote_snapshot(db_connection, "OGDC", ts, quote)
        result2 = insert_quote_snapshot(db_connection, "OGDC", ts, quote)

        assert result1 is True
        assert result2 is False


class TestGetLastQuoteHash:
    """Tests for get_last_quote_hash database function."""

    def test_get_last_hash(self, db_connection):
        """Get hash of most recent snapshot."""
        quote = {"price": 100, "raw_hash": "first123"}
        insert_quote_snapshot(db_connection, "OGDC", "2026-01-21T10:00:00", quote)

        quote2 = {"price": 101, "raw_hash": "second456"}
        insert_quote_snapshot(db_connection, "OGDC", "2026-01-21T11:00:00", quote2)

        last_hash = get_last_quote_hash(db_connection, "OGDC")
        assert last_hash == "second456"

    def test_no_snapshots(self, db_connection):
        """Returns None when no snapshots exist."""
        assert get_last_quote_hash(db_connection, "OGDC") is None


class TestGetQuoteSnapshots:
    """Tests for get_quote_snapshots database function."""

    def test_get_snapshots(self, db_connection):
        """Get recent snapshots."""
        for i in range(5):
            quote = {"price": 100 + i, "raw_hash": f"hash{i}"}
            insert_quote_snapshot(
                db_connection, "OGDC", f"2026-01-21T1{i}:00:00", quote
            )

        df = get_quote_snapshots(db_connection, "OGDC", limit=3)
        assert len(df) == 3

    def test_empty_result(self, db_connection):
        """Empty result for unknown symbol."""
        df = get_quote_snapshots(db_connection, "UNKNOWN")
        assert df.empty
