"""Tests for PSX OHLCV MCP server.

Uses an in-memory SQLite database seeded with minimal test data.
"""

import asyncio
import json
import sqlite3

import pytest


# ── Fixtures ──────────────────────────────────────────────────────

@pytest.fixture
def con():
    """In-memory SQLite with core tables + sample data."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE symbols (
            symbol TEXT PRIMARY KEY, name TEXT, sector TEXT,
            sector_name TEXT, outstanding_shares REAL,
            is_active INTEGER DEFAULT 1, source TEXT DEFAULT 'TEST',
            discovered_at TEXT DEFAULT '', updated_at TEXT DEFAULT ''
        );

        CREATE TABLE eod_ohlcv (
            symbol TEXT NOT NULL, date TEXT NOT NULL,
            open REAL, high REAL, low REAL, close REAL,
            volume INTEGER, prev_close REAL,
            sector_code TEXT, company_name TEXT,
            ingested_at TEXT DEFAULT '', source TEXT DEFAULT 'TEST',
            processname TEXT,
            PRIMARY KEY (symbol, date)
        );

        CREATE TABLE company_fundamentals (
            symbol TEXT PRIMARY KEY, company_name TEXT, sector_name TEXT,
            price REAL, change REAL, change_pct REAL,
            open REAL, high REAL, low REAL, volume INTEGER,
            ldcp REAL, market_cap REAL, pe_ratio REAL,
            total_shares REAL, free_float_shares REAL, free_float_pct REAL,
            wk52_low REAL, wk52_high REAL,
            ytd_change_pct REAL, one_year_change_pct REAL,
            business_description TEXT, website TEXT, fiscal_year_end TEXT,
            updated_at TEXT DEFAULT ''
        );

        CREATE TABLE company_profile (
            symbol TEXT PRIMARY KEY, company_name TEXT,
            sector_name TEXT, business_description TEXT,
            address TEXT, website TEXT, registrar TEXT, auditor TEXT,
            fiscal_year_end TEXT, updated_at TEXT DEFAULT '',
            source_url TEXT DEFAULT ''
        );

        CREATE TABLE company_payouts (
            symbol TEXT NOT NULL, ex_date TEXT NOT NULL,
            payout_type TEXT NOT NULL, announcement_date TEXT,
            book_closure_from TEXT, book_closure_to TEXT,
            amount REAL, fiscal_year TEXT, updated_at TEXT DEFAULT '',
            PRIMARY KEY (symbol, ex_date, payout_type)
        );

        CREATE TABLE psx_indices (
            index_code TEXT NOT NULL, index_date TEXT NOT NULL,
            index_time TEXT, value REAL NOT NULL,
            change REAL, change_pct REAL,
            open REAL, high REAL, low REAL, volume INTEGER,
            previous_close REAL,
            ytd_change_pct REAL, one_year_change_pct REAL,
            week_52_low REAL, week_52_high REAL,
            trades INTEGER, market_cap REAL, turnover REAL,
            scraped_at TEXT,
            PRIMARY KEY (index_code, index_date)
        );

        CREATE TABLE mutual_funds (
            fund_id TEXT PRIMARY KEY, symbol TEXT UNIQUE, fund_name TEXT,
            amc_code TEXT, amc_name TEXT, fund_type TEXT, category TEXT,
            is_shariah INTEGER DEFAULT 0, launch_date TEXT,
            expense_ratio REAL, management_fee REAL,
            is_active INTEGER DEFAULT 1, source TEXT DEFAULT 'MUFAP',
            created_at TEXT DEFAULT '', updated_at TEXT DEFAULT ''
        );

        CREATE TABLE mutual_fund_nav (
            fund_id TEXT NOT NULL, date TEXT NOT NULL,
            nav REAL NOT NULL, offer_price REAL, redemption_price REAL,
            aum REAL, nav_change_pct REAL,
            source TEXT DEFAULT 'MUFAP', ingested_at TEXT DEFAULT '',
            PRIMARY KEY (fund_id, date)
        );

        CREATE TABLE etf_master (
            symbol TEXT PRIMARY KEY, name TEXT NOT NULL, amc TEXT,
            benchmark_index TEXT, inception_date TEXT, expense_ratio REAL,
            management_fee TEXT, shariah_compliant INTEGER DEFAULT 0,
            trustee TEXT
        );

        CREATE TABLE etf_nav (
            symbol TEXT NOT NULL, date TEXT NOT NULL,
            nav REAL, market_price REAL, premium_discount REAL,
            aum_millions REAL, outstanding_units INTEGER,
            PRIMARY KEY (symbol, date)
        );

        CREATE TABLE sukuk_master (
            instrument_id TEXT PRIMARY KEY, issuer TEXT, name TEXT,
            category TEXT, currency TEXT DEFAULT 'PKR',
            issue_date TEXT, maturity_date TEXT,
            coupon_rate REAL, coupon_frequency INTEGER,
            face_value REAL DEFAULT 100.0, issue_size REAL,
            shariah_compliant INTEGER DEFAULT 1, is_active INTEGER DEFAULT 1,
            source TEXT DEFAULT 'MANUAL', notes TEXT, created_at TEXT DEFAULT ''
        );

        CREATE TABLE sukuk_quotes (
            instrument_id TEXT NOT NULL, quote_date TEXT NOT NULL,
            clean_price REAL, dirty_price REAL, yield_to_maturity REAL,
            bid_yield REAL, ask_yield REAL, volume REAL,
            source TEXT DEFAULT 'MANUAL', ingested_at TEXT DEFAULT '',
            PRIMARY KEY (instrument_id, quote_date)
        );

        CREATE TABLE pkrv_daily (
            date TEXT NOT NULL, tenor_months INTEGER NOT NULL,
            yield_pct REAL NOT NULL, source TEXT DEFAULT 'SBP',
            scraped_at TEXT DEFAULT '',
            PRIMARY KEY (date, tenor_months)
        );

        CREATE TABLE tbill_auctions (
            auction_date TEXT NOT NULL, tenor TEXT NOT NULL,
            target_amount_billions REAL, bids_received_billions REAL,
            amount_accepted_billions REAL, cutoff_yield REAL,
            cutoff_price REAL, weighted_avg_yield REAL,
            maturity_date TEXT, settlement_date TEXT,
            scraped_at TEXT DEFAULT '',
            PRIMARY KEY (auction_date, tenor)
        );

        CREATE TABLE pib_auctions (
            auction_date TEXT NOT NULL, tenor TEXT NOT NULL,
            pib_type TEXT NOT NULL DEFAULT 'Fixed',
            target_amount_billions REAL, bids_received_billions REAL,
            amount_accepted_billions REAL, cutoff_yield REAL,
            cutoff_price REAL, coupon_rate REAL,
            maturity_date TEXT, scraped_at TEXT DEFAULT '',
            PRIMARY KEY (auction_date, tenor, pib_type)
        );

        CREATE TABLE gis_auctions (
            auction_date TEXT NOT NULL, gis_type TEXT NOT NULL,
            tenor TEXT, target_amount_billions REAL,
            amount_accepted_billions REAL, cutoff_rental_rate REAL,
            maturity_date TEXT, scraped_at TEXT DEFAULT '',
            PRIMARY KEY (auction_date, gis_type)
        );

        CREATE TABLE kibor_daily (
            date TEXT NOT NULL, tenor TEXT NOT NULL,
            bid REAL, offer REAL, scraped_at TEXT DEFAULT '',
            PRIMARY KEY (date, tenor)
        );

        CREATE TABLE konia_daily (
            date TEXT PRIMARY KEY, rate_pct REAL NOT NULL,
            volume_billions REAL, high REAL, low REAL,
            scraped_at TEXT DEFAULT ''
        );

        CREATE TABLE sbp_policy_rates (
            rate_date TEXT PRIMARY KEY, policy_rate REAL,
            ceiling_rate REAL, floor_rate REAL,
            scraped_at TEXT DEFAULT ''
        );

        CREATE TABLE sbp_fx_interbank (
            date TEXT NOT NULL, currency TEXT NOT NULL,
            buying REAL, selling REAL, mid REAL,
            scraped_at TEXT DEFAULT '',
            PRIMARY KEY (date, currency)
        );

        CREATE TABLE sbp_fx_open_market (
            date TEXT NOT NULL, currency TEXT NOT NULL,
            buying REAL, selling REAL,
            scraped_at TEXT DEFAULT '',
            PRIMARY KEY (date, currency)
        );

        CREATE TABLE forex_kerb (
            date TEXT NOT NULL, currency TEXT NOT NULL,
            buying REAL, selling REAL,
            source TEXT DEFAULT 'forex.pk', scraped_at TEXT DEFAULT '',
            PRIMARY KEY (date, currency, source)
        );

        CREATE TABLE yield_curve_points (
            curve_date TEXT NOT NULL, tenor_months INTEGER NOT NULL,
            yield_rate REAL NOT NULL,
            bond_type TEXT NOT NULL DEFAULT 'PIB',
            interpolation TEXT DEFAULT 'LINEAR',
            computed_at TEXT DEFAULT '',
            PRIMARY KEY(curve_date, tenor_months, bond_type)
        );

        CREATE TABLE ipo_listings (
            symbol TEXT NOT NULL, company_name TEXT, board TEXT,
            status TEXT, offer_price REAL, shares_offered INTEGER,
            subscription_open TEXT, subscription_close TEXT,
            listing_date TEXT NOT NULL DEFAULT '',
            ipo_type TEXT, prospectus_url TEXT, updated_at TEXT DEFAULT '',
            PRIMARY KEY (symbol, listing_date)
        );
    """)

    # Seed sample data
    conn.executescript("""
        INSERT INTO symbols (symbol, name, sector_name, outstanding_shares, is_active)
        VALUES ('OGDC', 'Oil & Gas Dev', 'OIL & GAS EXPLORATION', 4301000000, 1),
               ('HBL', 'Habib Bank', 'COMMERCIAL BANKS', 1467000000, 1),
               ('LUCK', 'Lucky Cement', 'CEMENT', 323000000, 1);

        INSERT INTO eod_ohlcv (symbol, date, open, high, low, close, volume)
        VALUES ('OGDC', '2026-02-04', 325.0, 330.0, 320.0, 328.8, 1776495),
               ('OGDC', '2026-02-06', 329.0, 329.0, 321.18, 321.18, 6350781),
               ('HBL', '2026-02-04', 342.0, 345.0, 340.0, 345.56, 866485),
               ('HBL', '2026-02-06', 340.0, 341.0, 339.0, 340.86, 866485),
               ('LUCK', '2026-02-04', 470.0, 475.0, 468.0, 471.0, 1405871),
               ('LUCK', '2026-02-06', 468.0, 469.0, 466.0, 468.65, 1405871);

        INSERT INTO psx_indices (index_code, index_date, value, change, change_pct)
        VALUES ('KSE100', '2026-02-06', 182338.12, -6030.0, -3.21);

        INSERT INTO mutual_funds (fund_id, symbol, fund_name, amc_code, fund_type, category)
        VALUES ('MUFAP:ABL-ISF', 'ABL-ISF', 'ABL Islamic Stock Fund', 'ABL', 'OPEN_END', 'Equity');

        INSERT INTO mutual_fund_nav (fund_id, date, nav)
        VALUES ('MUFAP:ABL-ISF', '2025-12-01', 40.0),
               ('MUFAP:ABL-ISF', '2026-02-06', 46.2);

        INSERT INTO pkrv_daily (date, tenor_months, yield_pct)
        VALUES ('2026-02-06', 12, 10.5),
               ('2026-02-06', 24, 10.2),
               ('2026-02-06', 36, 10.1);

        INSERT INTO tbill_auctions (auction_date, tenor, cutoff_yield, amount_accepted_billions)
        VALUES ('2026-02-04', '6M', 10.32, 250.0);

        INSERT INTO pib_auctions (auction_date, tenor, pib_type, cutoff_yield)
        VALUES ('2026-01-30', '5Y', 'Fixed', 10.8);

        INSERT INTO kibor_daily (date, tenor, bid, offer)
        VALUES ('2026-02-06', '1W', 12.0, 12.5),
               ('2026-02-06', '1M', 12.1, 12.6);

        INSERT INTO konia_daily (date, rate_pct)
        VALUES ('2026-02-06', 11.16);

        INSERT INTO sbp_policy_rates (rate_date, policy_rate)
        VALUES ('2026-01-27', 12.0);

        INSERT INTO sukuk_master (instrument_id, issuer, name, category, maturity_date)
        VALUES ('SUKUK:TEST-001', 'TEST_ISSUER', 'Test Sukuk 2027', 'GOP_SUKUK', '2027-12-31');

        INSERT INTO etf_master (symbol, name)
        VALUES ('MIIETF', 'Mahaana Islamic ETF');

        INSERT INTO company_payouts (symbol, ex_date, payout_type, amount, fiscal_year)
        VALUES ('OGDC', '2025-11-01', 'cash', 5.0, '2025');

        INSERT INTO sbp_fx_interbank (date, currency, buying, selling)
        VALUES ('2026-02-06', 'USD', 279.0, 279.5);

        INSERT INTO forex_kerb (date, currency, buying, selling)
        VALUES ('2026-02-06', 'USD', 280.0, 281.0);
    """)
    conn.commit()
    yield conn
    conn.close()


@pytest.fixture
def _patch_db(con, monkeypatch):
    """Patch get_db to return the in-memory test connection."""
    import psx_ohlcv.mcp.server as srv
    monkeypatch.setattr(srv, "get_db", lambda: con)


def _run(coro):
    """Run an async coroutine synchronously."""
    return asyncio.get_event_loop().run_until_complete(coro)


# ── Server basics ─────────────────────────────────────────────────

class TestServerStartup:
    def test_server_name(self):
        from psx_ohlcv.mcp.server import server
        assert server.name == "psx-ohlcv"

    def test_list_tools_returns_all(self, _patch_db):
        from psx_ohlcv.mcp.server import list_tools
        tools = _run(list_tools())
        assert len(tools) == 30
        names = [t.name for t in tools]
        assert "get_eod" in names
        assert "get_yield_curve" in names
        assert "get_mutual_funds" in names
        assert "screen_stocks" in names
        assert "run_sql" in names


# ── Equity tools ──────────────────────────────────────────────────

class TestGetEod:
    def test_valid_symbol(self, _patch_db):
        from psx_ohlcv.mcp.server import call_tool
        result = _run(call_tool("get_eod", {"symbol": "OGDC", "limit": 10}))
        data = json.loads(result[0].text)
        assert data["count"] == 2
        assert data["data"][0]["symbol"] == "OGDC" if "symbol" in data["data"][0] else True

    def test_invalid_symbol_returns_empty(self, _patch_db):
        from psx_ohlcv.mcp.server import call_tool
        result = _run(call_tool("get_eod", {"symbol": "ZZZZZ"}))
        data = json.loads(result[0].text)
        assert data["count"] == 0


class TestSearchSymbols:
    def test_search_by_name(self, _patch_db):
        from psx_ohlcv.mcp.server import call_tool
        result = _run(call_tool("search_symbols", {"query": "habib"}))
        data = json.loads(result[0].text)
        assert data["count"] >= 1
        assert any(r["symbol"] == "HBL" for r in data["results"])

    def test_search_by_symbol(self, _patch_db):
        from psx_ohlcv.mcp.server import call_tool
        result = _run(call_tool("search_symbols", {"query": "OGDC"}))
        data = json.loads(result[0].text)
        assert data["count"] >= 1


class TestGetCompanyProfile:
    def test_fallback_to_symbols(self, _patch_db):
        from psx_ohlcv.mcp.server import call_tool
        result = _run(call_tool("get_company_profile", {"symbol": "OGDC"}))
        data = json.loads(result[0].text)
        assert data["symbol"] == "OGDC"
        assert "close" in data  # Should have EOD fallback

    def test_not_found(self, _patch_db):
        from psx_ohlcv.mcp.server import call_tool
        result = _run(call_tool("get_company_profile", {"symbol": "ZZZZZ"}))
        data = json.loads(result[0].text)
        assert "error" in data


class TestGetMarketSnapshot:
    def test_returns_indices(self, _patch_db):
        from psx_ohlcv.mcp.server import call_tool
        result = _run(call_tool("get_market_snapshot", {}))
        data = json.loads(result[0].text)
        assert len(data["indices"]) >= 1
        assert data["latest_eod_date"] == "2026-02-06"


class TestGetTopMovers:
    def test_gainers_and_losers(self, _patch_db):
        from psx_ohlcv.mcp.server import call_tool
        result = _run(call_tool("get_top_movers", {"n": 5, "direction": "both"}))
        data = json.loads(result[0].text)
        assert "gainers" in data
        assert "losers" in data
        assert data["date"] == "2026-02-06"


# ── Fixed income tools ────────────────────────────────────────────

class TestFixedIncome:
    def test_yield_curve(self, _patch_db):
        from psx_ohlcv.mcp.server import call_tool
        result = _run(call_tool("get_yield_curve", {"curve_type": "pkrv"}))
        data = json.loads(result[0].text)
        assert len(data["points"]) == 3
        assert data["date"] == "2026-02-06"

    def test_tbill_auctions(self, _patch_db):
        from psx_ohlcv.mcp.server import call_tool
        result = _run(call_tool("get_tbill_auctions", {}))
        data = json.loads(result[0].text)
        assert data["count"] == 1

    def test_latest_yields(self, _patch_db):
        from psx_ohlcv.mcp.server import call_tool
        result = _run(call_tool("get_latest_yields", {}))
        data = json.loads(result[0].text)
        assert data["policy_rate"]["policy_rate"] == 12.0
        assert len(data["kibor"]["rates"]) == 2

    def test_sukuk(self, _patch_db):
        from psx_ohlcv.mcp.server import call_tool
        result = _run(call_tool("get_sukuk", {}))
        data = json.loads(result[0].text)
        assert data["count"] == 1


# ── Fund + FX tools ───────────────────────────────────────────────

class TestFundFxTools:
    def test_mutual_funds(self, _patch_db):
        from psx_ohlcv.mcp.server import call_tool
        result = _run(call_tool("get_mutual_funds", {}))
        data = json.loads(result[0].text)
        assert data["count"] == 1
        assert data["data"][0]["fund_name"] == "ABL Islamic Stock Fund"

    def test_fund_nav_history(self, _patch_db):
        from psx_ohlcv.mcp.server import call_tool
        result = _run(call_tool("get_fund_nav_history", {"fund_id": "MUFAP:ABL-ISF"}))
        data = json.loads(result[0].text)
        assert data["count"] == 2

    def test_etf_list(self, _patch_db):
        from psx_ohlcv.mcp.server import call_tool
        result = _run(call_tool("get_etf_list", {}))
        data = json.loads(result[0].text)
        assert data["count"] == 1

    def test_fx_rates(self, _patch_db):
        from psx_ohlcv.mcp.server import call_tool
        result = _run(call_tool("get_fx_rates", {"currency": "USD", "source": "all"}))
        data = json.loads(result[0].text)
        assert len(data["interbank"]) == 1
        assert data["interbank"][0]["buying"] == 279.0

    def test_fx_spread(self, _patch_db):
        from psx_ohlcv.mcp.server import call_tool
        result = _run(call_tool("get_fx_spread", {"currency": "USD"}))
        data = json.loads(result[0].text)
        assert data["interbank"]["buying"] == 279.0
        assert data["kerb"]["buying"] == 280.0

    def test_kibor(self, _patch_db):
        from psx_ohlcv.mcp.server import call_tool
        result = _run(call_tool("get_kibor", {}))
        data = json.loads(result[0].text)
        assert len(data["rates"]) == 2


# ── Analytics + system tools ──────────────────────────────────────

class TestAnalytics:
    def test_screen_stocks(self, _patch_db):
        from psx_ohlcv.mcp.server import call_tool
        result = _run(call_tool("screen_stocks", {"limit": 10}))
        data = json.loads(result[0].text)
        assert data["count"] == 3

    def test_compare_securities(self, _patch_db):
        from psx_ohlcv.mcp.server import call_tool
        result = _run(call_tool("compare_securities", {"symbols": ["OGDC", "HBL"]}))
        data = json.loads(result[0].text)
        assert len(data["securities"]) == 2

    def test_calculate_returns(self, _patch_db):
        from psx_ohlcv.mcp.server import call_tool
        result = _run(call_tool("calculate_returns", {"symbol": "OGDC"}))
        data = json.loads(result[0].text)
        assert data["symbol"] == "OGDC"
        assert "price" in data

    def test_sector_performance(self, _patch_db):
        from psx_ohlcv.mcp.server import call_tool
        result = _run(call_tool("get_sector_performance", {}))
        data = json.loads(result[0].text)
        assert len(data["sectors"]) >= 1

    def test_correlation(self, _patch_db):
        from psx_ohlcv.mcp.server import call_tool
        # Only 2 overlapping dates, so correlation needs at least 5
        result = _run(call_tool("get_correlation", {"symbol1": "OGDC", "symbol2": "HBL", "days": 365}))
        data = json.loads(result[0].text)
        assert "error" in data  # Insufficient data (only 2 days)


class TestSystemTools:
    def test_data_freshness(self, _patch_db):
        from psx_ohlcv.mcp.server import call_tool
        result = _run(call_tool("get_data_freshness", {}))
        data = json.loads(result[0].text)
        assert "domains" in data
        assert data["table_count"] > 0

    def test_coverage_summary(self, _patch_db):
        from psx_ohlcv.mcp.server import call_tool
        result = _run(call_tool("get_coverage_summary", {}))
        data = json.loads(result[0].text)
        assert data["active_symbols"] == 3
        assert data["eod_records"] == 6

    def test_run_sql_select(self, _patch_db):
        from psx_ohlcv.mcp.server import call_tool
        result = _run(call_tool("run_sql", {"query": "SELECT COUNT(*) as cnt FROM symbols"}))
        data = json.loads(result[0].text)
        assert data["data"][0]["cnt"] == 3

    def test_run_sql_rejects_write(self, _patch_db):
        from psx_ohlcv.mcp.server import call_tool
        for query in ["DELETE FROM symbols", "DROP TABLE symbols",
                       "INSERT INTO symbols VALUES ('X')", "UPDATE symbols SET name='X'"]:
            result = _run(call_tool("run_sql", {"query": query}))
            data = json.loads(result[0].text)
            assert "error" in data, f"Write query should be rejected: {query}"

    def test_run_sql_rejects_multiple_statements(self, _patch_db):
        from psx_ohlcv.mcp.server import call_tool
        result = _run(call_tool("run_sql", {"query": "SELECT 1; DROP TABLE symbols"}))
        data = json.loads(result[0].text)
        assert "error" in data


# ── Resources + Prompts ───────────────────────────────────────────

class TestResources:
    def test_list_resources(self, _patch_db):
        from psx_ohlcv.mcp.server import list_resources
        resources = _run(list_resources())
        assert len(resources) == 4
        uris = [str(r.uri) for r in resources]
        assert "psx://schema" in uris

    def test_read_schema(self, _patch_db):
        from psx_ohlcv.mcp.server import read_resource
        schema = _run(read_resource("psx://schema"))
        assert "CREATE TABLE" in schema
        assert "eod_ohlcv" in schema

    def test_read_symbols(self, _patch_db):
        from psx_ohlcv.mcp.server import read_resource
        symbols = _run(read_resource("psx://symbols"))
        assert "OGDC" in symbols
        assert "HBL" in symbols

    def test_read_data_dictionary(self, _patch_db):
        from psx_ohlcv.mcp.server import read_resource
        dd = _run(read_resource("psx://data-dictionary"))
        assert "eod_ohlcv" in dd

    def test_read_trading_calendar(self, _patch_db):
        from psx_ohlcv.mcp.server import read_resource
        cal = _run(read_resource("psx://trading-calendar"))
        assert "2026-02-06" in cal


class TestPrompts:
    def test_list_prompts(self, _patch_db):
        from psx_ohlcv.mcp.server import list_prompts
        prompts = _run(list_prompts())
        assert len(prompts) == 6
        names = [p.name for p in prompts]
        assert "daily_market_brief" in names

    def test_get_daily_market_brief(self, _patch_db):
        from psx_ohlcv.mcp.server import get_prompt
        result = _run(get_prompt("daily_market_brief", {}))
        assert "get_market_snapshot" in result.messages[0].content.text

    def test_get_stock_deep_dive(self, _patch_db):
        from psx_ohlcv.mcp.server import get_prompt
        result = _run(get_prompt("stock_deep_dive", {"symbol": "HBL"}))
        assert "HBL" in result.messages[0].content.text

    def test_unknown_tool(self, _patch_db):
        from psx_ohlcv.mcp.server import call_tool
        result = _run(call_tool("nonexistent_tool", {}))
        assert "Unknown tool" in result[0].text
