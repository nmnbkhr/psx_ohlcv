"""Tests for yield curve repository and SBP rates scraper."""

import sqlite3

import pandas as pd
import pytest


@pytest.fixture
def con():
    """In-memory SQLite with yield curve schema."""
    from pakfindata.db.repositories.yield_curves import init_yield_curve_schema

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    init_yield_curve_schema(conn)
    yield conn
    conn.close()


# ── Schema tests ────────────────────────────────────────────────────

class TestInitYieldCurveSchema:
    def test_tables_created(self, con):
        tables = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        names = [r["name"] for r in tables]
        assert "pkrv_daily" in names
        assert "konia_daily" in names
        assert "kibor_daily" in names

    def test_idempotent(self, con):
        from pakfindata.db.repositories.yield_curves import init_yield_curve_schema
        init_yield_curve_schema(con)
        init_yield_curve_schema(con)


# ── PKRV tests ──────────────────────────────────────────────────────

class TestUpsertPkrvPoint:
    def test_insert(self, con):
        from pakfindata.db.repositories.yield_curves import upsert_pkrv_point
        ok = upsert_pkrv_point(con, {
            "date": "2026-02-08",
            "tenor_months": 3,
            "yield_pct": 10.2,
            "source": "MTB Auction",
        })
        assert ok is True

        row = con.execute(
            "SELECT * FROM pkrv_daily WHERE date=? AND tenor_months=?",
            ("2026-02-08", 3),
        ).fetchone()
        assert row["yield_pct"] == 10.2

    def test_upsert_updates(self, con):
        from pakfindata.db.repositories.yield_curves import upsert_pkrv_point
        upsert_pkrv_point(con, {
            "date": "2026-02-08", "tenor_months": 3, "yield_pct": 10.0,
        })
        upsert_pkrv_point(con, {
            "date": "2026-02-08", "tenor_months": 3, "yield_pct": 10.5,
        })
        row = con.execute(
            "SELECT yield_pct FROM pkrv_daily WHERE date=? AND tenor_months=?",
            ("2026-02-08", 3),
        ).fetchone()
        assert row["yield_pct"] == 10.5


class TestGetPkrvCurve:
    def test_empty(self, con):
        from pakfindata.db.repositories.yield_curves import get_pkrv_curve
        df = get_pkrv_curve(con)
        assert df.empty

    def test_returns_curve(self, con):
        from pakfindata.db.repositories.yield_curves import (
            get_pkrv_curve,
            upsert_pkrv_point,
        )
        for months, rate in [(1, 10.0), (3, 10.2), (12, 10.4), (60, 10.5)]:
            upsert_pkrv_point(con, {
                "date": "2026-02-08", "tenor_months": months, "yield_pct": rate,
            })
        df = get_pkrv_curve(con, date="2026-02-08")
        assert len(df) == 4
        assert df.iloc[0]["tenor_months"] == 1  # sorted

    def test_latest_date(self, con):
        from pakfindata.db.repositories.yield_curves import (
            get_pkrv_curve,
            upsert_pkrv_point,
        )
        upsert_pkrv_point(con, {
            "date": "2026-02-07", "tenor_months": 3, "yield_pct": 10.0,
        })
        upsert_pkrv_point(con, {
            "date": "2026-02-08", "tenor_months": 3, "yield_pct": 10.5,
        })
        df = get_pkrv_curve(con)  # latest
        assert len(df) == 1
        assert df.iloc[0]["date"] == "2026-02-08"


class TestCompareCurves:
    def test_compare(self, con):
        from pakfindata.db.repositories.yield_curves import (
            compare_curves,
            upsert_pkrv_point,
        )
        for months in [3, 12, 60]:
            upsert_pkrv_point(con, {
                "date": "2026-02-07", "tenor_months": months, "yield_pct": 10.0,
            })
            upsert_pkrv_point(con, {
                "date": "2026-02-08", "tenor_months": months, "yield_pct": 10.1,
            })
        df = compare_curves(con, "2026-02-07", "2026-02-08")
        assert len(df) == 3
        # Change should be +10 bps
        assert all(df["change_bps"] == 10.0)


# ── KONIA tests ─────────────────────────────────────────────────────

class TestUpsertKoniaRate:
    def test_insert(self, con):
        from pakfindata.db.repositories.yield_curves import upsert_konia_rate
        ok = upsert_konia_rate(con, {
            "date": "2026-02-08", "rate_pct": 11.16,
        })
        assert ok is True

    def test_get_latest(self, con):
        from pakfindata.db.repositories.yield_curves import (
            get_latest_konia,
            upsert_konia_rate,
        )
        upsert_konia_rate(con, {"date": "2026-02-07", "rate_pct": 11.0})
        upsert_konia_rate(con, {"date": "2026-02-08", "rate_pct": 11.16})
        latest = get_latest_konia(con)
        assert latest["date"] == "2026-02-08"
        assert latest["rate_pct"] == 11.16


class TestGetKoniaHistory:
    def test_empty(self, con):
        from pakfindata.db.repositories.yield_curves import get_konia_history
        df = get_konia_history(con)
        assert df.empty

    def test_with_data(self, con):
        from pakfindata.db.repositories.yield_curves import (
            get_konia_history,
            upsert_konia_rate,
        )
        upsert_konia_rate(con, {"date": "2026-02-07", "rate_pct": 11.0})
        upsert_konia_rate(con, {"date": "2026-02-08", "rate_pct": 11.16})
        df = get_konia_history(con)
        assert len(df) == 2


# ── KIBOR tests ─────────────────────────────────────────────────────

class TestUpsertKiborRate:
    def test_insert(self, con):
        from pakfindata.db.repositories.yield_curves import upsert_kibor_rate
        ok = upsert_kibor_rate(con, {
            "date": "2026-02-06", "tenor": "3M", "bid": 10.26, "offer": 10.51,
        })
        assert ok is True


class TestGetKiborHistory:
    def test_filter_by_tenor(self, con):
        from pakfindata.db.repositories.yield_curves import (
            get_kibor_history,
            upsert_kibor_rate,
        )
        upsert_kibor_rate(con, {
            "date": "2026-02-06", "tenor": "3M", "bid": 10.26, "offer": 10.51,
        })
        upsert_kibor_rate(con, {
            "date": "2026-02-06", "tenor": "6M", "bid": 10.26, "offer": 10.51,
        })
        df = get_kibor_history(con, tenor="3M")
        assert len(df) == 1


# ── Scraper tests ───────────────────────────────────────────────────

class TestSBPRatesScraper:
    def test_sync_rates_to_db(self, con):
        """Test sync with mocked data."""
        from unittest.mock import patch
        from pakfindata.sources.sbp_rates import SBPRatesScraper

        mock_rates = {
            "overnight_rate": 11.16,
            "overnight_date": "2026-02-08",
            "policy_rate": 10.5,
            "kibor": [
                {"date": "2026-02-06", "tenor": "3M", "bid": 10.26, "offer": 10.51},
            ],
            "yield_curve": [
                {"date": "2026-02-08", "tenor_months": 3, "yield_pct": 10.2, "source": "MTB"},
                {"date": "2026-02-08", "tenor_months": 60, "yield_pct": 10.5, "source": "PIB"},
            ],
        }

        scraper = SBPRatesScraper()
        with patch.object(scraper, "scrape_all_rates", return_value=mock_rates):
            result = scraper.sync_rates(con)

        assert result["konia_ok"] is True
        assert result["kibor_ok"] == 1
        assert result["pkrv_points"] == 2
        assert result["failed"] == 0

    def test_build_yield_curve_with_data(self):
        """Test yield curve construction from page text."""
        from pakfindata.sources.sbp_rates import SBPRatesScraper

        text = (
            "MTBs Tenor Cut-off Yield "
            "1-M 10.20% 3-M 10.25% 6-M 10.35% 12-M 10.40% "
            "(as on Feb 04, 2026) "
            "Fixed-rate PIB Tenor Cut-off Rates "
            "2-Y 10.19% 3-Y 10.14% 5-Y 10.53% 10-Y 11.00% "
        )
        curve = SBPRatesScraper._build_yield_curve(text, curve_date="2026-02-08")
        assert len(curve) == 8
        # Check ordering: 1M, 3M, 6M, 12M, 2Y, 3Y, 5Y, 10Y
        assert curve[0]["tenor_months"] == 1
        assert curve[-1]["tenor_months"] == 120
        # All should have same date
        assert all(p["date"] == "2026-02-08" for p in curve)
