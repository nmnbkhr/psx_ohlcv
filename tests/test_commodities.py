"""Tests for commodity data module.

Tests config universe, PKR conversions, schema creation,
and repository functions. Uses in-memory SQLite.
"""

import sqlite3

import pytest


@pytest.fixture
def con():
    """In-memory SQLite with commodity schema."""
    from pakfindata.commodities.models import init_commodity_schema

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    init_commodity_schema(conn)
    yield conn
    conn.close()


# ── Config Universe ─────────────────────────────────────────────────

class TestCommodityConfig:
    def test_universe_not_empty(self):
        from pakfindata.commodities.config import COMMODITY_UNIVERSE
        assert len(COMMODITY_UNIVERSE) > 25

    def test_gold_exists(self):
        from pakfindata.commodities.config import get_commodity
        gold = get_commodity("GOLD")
        assert gold is not None
        assert gold.name == "Gold"
        assert gold.yf_ticker == "GC=F"
        assert gold.unit == "USD/oz"
        assert gold.pk_relevance == "HIGH"

    def test_categories_complete(self):
        from pakfindata.commodities.config import CATEGORIES
        assert "metals" in CATEGORIES
        assert "energy" in CATEGORIES
        assert "agriculture" in CATEGORIES
        assert "fx" in CATEGORIES

    def test_get_by_category(self):
        from pakfindata.commodities.config import get_commodities_by_category
        metals = get_commodities_by_category("metals")
        assert len(metals) >= 5  # Gold, Silver, Platinum, Copper, Iron Ore, ...
        assert all(c.category == "metals" for c in metals)

    def test_pk_high_commodities(self):
        from pakfindata.commodities.config import get_pk_high_commodities
        pk_high = get_pk_high_commodities()
        assert len(pk_high) >= 10
        assert all(c.pk_relevance == "HIGH" for c in pk_high)

    def test_yfinance_tickers(self):
        from pakfindata.commodities.config import get_yfinance_tickers
        tickers = get_yfinance_tickers()
        assert "GOLD" in tickers
        assert tickers["GOLD"] == "GC=F"
        assert "CRUDE_WTI" in tickers
        assert tickers["CRUDE_WTI"] == "CL=F"

    def test_fred_series(self):
        from pakfindata.commodities.config import get_fred_series
        series = get_fred_series()
        assert "COAL" in series
        assert series["COAL"] == "PCOALAUUSDM"
        assert "PALM_OIL" in series
        assert series["PALM_OIL"] == "PPOILUSDM"

    def test_commodities_without_yfinance(self):
        """Verify commodities that should NOT have yfinance tickers."""
        from pakfindata.commodities.config import COMMODITY_UNIVERSE
        for sym in ["ZINC", "NICKEL", "LEAD", "TIN", "COAL", "PALM_OIL", "RUBBER", "STEEL_HRC"]:
            c = COMMODITY_UNIVERSE.get(sym)
            assert c is not None, f"{sym} missing from universe"
            assert c.yf_ticker is None, f"{sym} should not have yf_ticker"

    def test_fx_pairs(self):
        from pakfindata.commodities.config import get_commodities_by_category
        fx = get_commodities_by_category("fx")
        symbols = {c.symbol for c in fx}
        assert "USD_PKR" in symbols
        assert "DXY" in symbols
        assert "USD_SAR" in symbols


# ── PKR Conversions ────────────────────────────────────────────────

class TestPKRConversions:
    """Test PKR conversion math with known values."""

    def test_gold_usd_oz_to_pkr_tola(self):
        from pakfindata.commodities.utils import gold_usd_oz_to_pkr_tola
        # Gold at $2000/oz, USD/PKR = 280
        result = gold_usd_oz_to_pkr_tola(2000, 280)
        # 2000 * 0.40125 * 280 = 224,700
        assert result == pytest.approx(224700, rel=0.01)

    def test_cotton_usd_lb_to_pkr_maund(self):
        from pakfindata.commodities.utils import cotton_usd_lb_to_pkr_maund
        # Cotton at $0.80/lb, USD/PKR = 280
        # 1 maund = 37.3242 kg / 0.453592 kg/lb = ~82.286 lbs
        # 0.80 * 82.286 * 280 = ~18,432
        result = cotton_usd_lb_to_pkr_maund(0.80, 280)
        assert result == pytest.approx(18432, rel=0.02)

    def test_wheat_usd_bu_to_pkr_maund(self):
        from pakfindata.commodities.utils import wheat_usd_bu_to_pkr_maund
        # Wheat at $5.50/bu, USD/PKR = 280
        # 1 maund = 37.3242 kg, 1 bu wheat = 27.2155 kg
        # PKR/maund = 5.50 * (37.3242/27.2155) * 280 = 5.50 * 1.3714 * 280 = ~2112
        result = wheat_usd_bu_to_pkr_maund(5.50, 280)
        assert result == pytest.approx(2112, rel=0.02)

    def test_rice_usd_cwt_to_pkr_maund(self):
        from pakfindata.commodities.utils import rice_usd_cwt_to_pkr_maund
        # Rice at $15.00/cwt, USD/PKR = 280
        # 1 cwt = 45.3592 kg, 1 maund = 37.3242 kg
        # PKR/maund = 15.00 / (45.3592/37.3242) * 280 = 15.00 / 1.2153 * 280 = ~3454
        result = rice_usd_cwt_to_pkr_maund(15.00, 280)
        assert result == pytest.approx(3454, rel=0.02)

    def test_crude_usd_bbl_to_pkr_litre(self):
        from pakfindata.commodities.utils import crude_usd_bbl_to_pkr_litre
        # Crude at $80/bbl, USD/PKR = 280
        # PKR/litre = 80 / 158.987 * 280 = ~140.9
        result = crude_usd_bbl_to_pkr_litre(80, 280)
        assert result == pytest.approx(140.9, rel=0.02)

    def test_sugar_usd_lb_to_pkr_bori(self):
        from pakfindata.commodities.utils import sugar_usd_lb_to_pkr_bori
        # Sugar at $0.22/lb, USD/PKR = 280
        # 1 bori = 100 kg / 0.453592 = ~220.46 lbs
        # PKR/bori = 0.22 * 220.46 * 280 = ~13,580
        result = sugar_usd_lb_to_pkr_bori(0.22, 280)
        assert result == pytest.approx(13580, rel=0.02)

    def test_convert_to_pkr_dispatcher(self):
        from pakfindata.commodities.utils import convert_to_pkr
        result = convert_to_pkr("gold_usd_oz_to_pkr_tola", 2000, 280)
        assert result is not None
        assert result == pytest.approx(224700, rel=0.01)

    def test_convert_to_pkr_unknown(self):
        from pakfindata.commodities.utils import convert_to_pkr
        result = convert_to_pkr("nonexistent_converter", 100, 280)
        assert result is None


# ── Schema ──────────────────────────────────────────────────────────

class TestCommoditySchema:
    def test_tables_created(self, con):
        tables = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        names = [r["name"] for r in tables]
        assert "commodity_symbols" in names
        assert "commodity_eod" in names
        assert "commodity_monthly" in names
        assert "commodity_pkr" in names
        assert "commodity_fx_rates" in names
        assert "commodity_sync_runs" in names

    def test_schema_idempotent(self, con):
        from pakfindata.commodities.models import init_commodity_schema
        init_commodity_schema(con)
        init_commodity_schema(con)  # Should not fail


# ── Repository Functions ────────────────────────────────────────────

class TestCommodityRepository:
    def test_upsert_commodity_symbol(self, con):
        from pakfindata.commodities.models import upsert_commodity_symbol
        ok = upsert_commodity_symbol(con, {
            "symbol": "TEST_GOLD",
            "name": "Test Gold",
            "category": "metals",
            "unit": "USD/oz",
            "pk_relevance": "HIGH",
            "yf_ticker": "GC=F",
            "yf_etf": "GLD",
            "fred_series": None,
            "wb_column": None,
            "pk_unit": "PKR/tola",
            "pk_conversion": "gold_usd_oz_to_pkr_tola",
        })
        assert ok is True
        row = con.execute("SELECT * FROM commodity_symbols WHERE symbol='TEST_GOLD'").fetchone()
        assert row["name"] == "Test Gold"

    def test_upsert_commodity_eod(self, con):
        from pakfindata.commodities.models import upsert_commodity_eod
        rows = [
            {
                "symbol": "GOLD", "date": "2026-02-01",
                "open": 2050.0, "high": 2080.0, "low": 2040.0,
                "close": 2070.0, "volume": 10000, "adj_close": 2070.0,
                "source": "yfinance",
            },
            {
                "symbol": "GOLD", "date": "2026-02-02",
                "open": 2070.0, "high": 2090.0, "low": 2060.0,
                "close": 2085.0, "volume": 12000, "adj_close": 2085.0,
                "source": "yfinance",
            },
        ]
        count = upsert_commodity_eod(con, rows)
        assert count == 2

        total = con.execute("SELECT COUNT(*) as c FROM commodity_eod").fetchone()["c"]
        assert total == 2

    def test_upsert_commodity_eod_dedup(self, con):
        from pakfindata.commodities.models import upsert_commodity_eod
        row = {
            "symbol": "GOLD", "date": "2026-02-01",
            "open": 2050.0, "high": 2080.0, "low": 2040.0,
            "close": 2070.0, "volume": 10000, "adj_close": 2070.0,
            "source": "yfinance",
        }
        upsert_commodity_eod(con, [row])
        # Update close price
        row["close"] = 2075.0
        upsert_commodity_eod(con, [row])

        total = con.execute("SELECT COUNT(*) as c FROM commodity_eod").fetchone()["c"]
        assert total == 1
        updated = con.execute("SELECT close FROM commodity_eod WHERE symbol='GOLD'").fetchone()
        assert updated["close"] == 2075.0

    def test_upsert_commodity_monthly(self, con):
        from pakfindata.commodities.models import upsert_commodity_monthly
        rows = [
            {"symbol": "COAL", "date": "2026-01-01", "price": 120.5, "source": "fred", "series_id": "PCOALAUUSDM"},
        ]
        count = upsert_commodity_monthly(con, rows)
        assert count == 1

    def test_upsert_commodity_pkr(self, con):
        from pakfindata.commodities.models import upsert_commodity_pkr
        rows = [
            {
                "symbol": "GOLD", "date": "2026-02-01",
                "pkr_price": 225000.0, "pk_unit": "PKR/tola",
                "usd_price": 2070.0, "usd_pkr": 280.0,
                "source": "computed",
            },
        ]
        count = upsert_commodity_pkr(con, rows)
        assert count == 1

    def test_upsert_commodity_fx(self, con):
        from pakfindata.commodities.models import upsert_commodity_fx
        rows = [
            {
                "pair": "USD_PKR", "date": "2026-02-01",
                "open": 278.0, "high": 280.0, "low": 277.5,
                "close": 279.5, "volume": 0, "source": "yfinance",
            },
        ]
        count = upsert_commodity_fx(con, rows)
        assert count == 1

    def test_get_commodity_eod_range(self, con):
        from pakfindata.commodities.models import upsert_commodity_eod, get_commodity_eod_range
        rows = [
            {
                "symbol": "GOLD", "date": f"2026-02-{d:02d}",
                "open": 2050.0 + d, "high": 2080.0 + d, "low": 2040.0 + d,
                "close": 2070.0 + d, "volume": 10000, "adj_close": 2070.0 + d,
                "source": "yfinance",
            }
            for d in range(1, 11)
        ]
        upsert_commodity_eod(con, rows)

        result = get_commodity_eod_range(con, "GOLD", start="2026-02-05", limit=10)
        assert len(result) == 6  # Days 5-10

    def test_get_commodity_pkr_latest(self, con):
        from pakfindata.commodities.models import upsert_commodity_pkr, get_commodity_pkr_latest
        rows = [
            {"symbol": "GOLD", "date": "2026-02-01", "pkr_price": 225000.0,
             "pk_unit": "PKR/tola", "usd_price": 2070.0, "usd_pkr": 280.0, "source": "computed"},
            {"symbol": "GOLD", "date": "2026-02-02", "pkr_price": 226000.0,
             "pk_unit": "PKR/tola", "usd_price": 2075.0, "usd_pkr": 280.0, "source": "computed"},
        ]
        upsert_commodity_pkr(con, rows)

        latest = get_commodity_pkr_latest(con, ["GOLD"])
        assert len(latest) == 1
        assert latest[0]["date"] == "2026-02-02"
        assert latest[0]["pkr_price"] == 226000.0

    def test_sync_run_tracking(self, con):
        from pakfindata.commodities.models import record_commodity_sync_start, record_commodity_sync_end
        record_commodity_sync_start(con, "test-run-1", "yfinance_daily", "yfinance")
        record_commodity_sync_end(con, "test-run-1", symbols_total=10, symbols_ok=8, symbols_failed=2, rows_upserted=500)

        row = con.execute("SELECT * FROM commodity_sync_runs WHERE run_id='test-run-1'").fetchone()
        assert row is not None
        assert row["symbols_total"] == 10
        assert row["symbols_ok"] == 8
        assert row["rows_upserted"] == 500


# ── Constants Verification ──────────────────────────────────────────

class TestConstants:
    def test_tola_to_oz(self):
        from pakfindata.commodities.utils import TOLA_TO_OZ
        assert TOLA_TO_OZ == pytest.approx(0.40125, abs=0.0001)

    def test_maund_to_kg(self):
        from pakfindata.commodities.utils import MAUND_TO_KG
        assert MAUND_TO_KG == pytest.approx(37.3242, abs=0.001)

    def test_bbl_to_litre(self):
        from pakfindata.commodities.utils import BBL_TO_LITRE
        assert BBL_TO_LITRE == pytest.approx(158.987, abs=0.01)

    def test_bori_to_kg(self):
        from pakfindata.commodities.utils import BORI_TO_KG
        assert BORI_TO_KG == 100.0
