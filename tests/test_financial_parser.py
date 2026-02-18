"""Tests for the comprehensive financial statement PDF parser.

Tests cover:
- Page classification
- Label matching (P&L and Balance Sheet)
- Currency scale detection
- Period info extraction
- Number extraction and note ref filtering
- Full PDF parsing (mock-based)
- Flatten to financials conversion
"""

import sqlite3

import pytest

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def con():
    """In-memory SQLite connection with schema."""
    from psx_ohlcv.db.connection import init_schema

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    init_schema(conn)
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Page Classification
# ---------------------------------------------------------------------------


class TestClassifyPage:
    def test_pl_statement(self):
        from psx_ohlcv.sources.financial_parser import classify_page

        text = "CONDENSED INTERIM\nSTATEMENT OF PROFIT OR LOSS\nFor the period ended"
        assert classify_page(text) == "pl"

    def test_pl_income_statement(self):
        from psx_ohlcv.sources.financial_parser import classify_page

        text = "INCOME STATEMENT\nFor the year ended December 31, 2024"
        assert classify_page(text) == "pl"

    def test_pl_profit_and_loss(self):
        from psx_ohlcv.sources.financial_parser import classify_page

        text = "PROFIT AND LOSS ACCOUNT\nFor the year ended"
        assert classify_page(text) == "pl"

    def test_bs_financial_position(self):
        from psx_ohlcv.sources.financial_parser import classify_page

        text = "STATEMENT OF FINANCIAL POSITION\nAs at September 30, 2025"
        assert classify_page(text) == "bs"

    def test_bs_balance_sheet(self):
        from psx_ohlcv.sources.financial_parser import classify_page

        text = "BALANCE SHEET\nAs at December 31, 2024"
        assert classify_page(text) == "bs"

    def test_cash_flow(self):
        from psx_ohlcv.sources.financial_parser import classify_page

        text = "STATEMENT OF CASH FLOW\nFor the year ended"
        assert classify_page(text) == "cf"

    def test_comprehensive_income(self):
        from psx_ohlcv.sources.financial_parser import classify_page

        text = "STATEMENT OF COMPREHENSIVE INCOME\nFor the year ended"
        assert classify_page(text) == "ci"

    def test_notes(self):
        from psx_ohlcv.sources.financial_parser import classify_page

        text = "NOTES TO THE FINANCIAL STATEMENTS\nNote 1"
        assert classify_page(text) == "notes"

    def test_unknown(self):
        from psx_ohlcv.sources.financial_parser import classify_page

        text = "Directors Report\nThe board is pleased to present"
        assert classify_page(text) == "unknown"

    def test_empty(self):
        from psx_ohlcv.sources.financial_parser import classify_page

        assert classify_page("") == "unknown"
        assert classify_page(None) == "unknown"


# ---------------------------------------------------------------------------
# Currency Scale Detection
# ---------------------------------------------------------------------------


class TestDetectScale:
    def test_thousands_ascii(self):
        from psx_ohlcv.sources.financial_parser import _detect_scale

        lines = ["Company Name", "(Rupees '000)", "For the year ended"]
        assert _detect_scale(lines) == 1000.0

    def test_thousands_unicode(self):
        from psx_ohlcv.sources.financial_parser import _detect_scale

        lines = ["Company Name", "(Rupees \u2018000)", "For the year ended"]
        assert _detect_scale(lines) == 1000.0

    def test_millions(self):
        from psx_ohlcv.sources.financial_parser import _detect_scale

        lines = ["Company Name", "(in million Rupees)", "For the year ended"]
        assert _detect_scale(lines) == 1_000_000.0

    def test_no_scale(self):
        from psx_ohlcv.sources.financial_parser import _detect_scale

        lines = ["Company Name", "For the year ended", "Note 2025 2024"]
        assert _detect_scale(lines) == 1.0

    def test_thousand_word(self):
        from psx_ohlcv.sources.financial_parser import _detect_scale

        lines = ["Amounts in thousand"]
        assert _detect_scale(lines) == 1000.0


# ---------------------------------------------------------------------------
# Period Info Extraction
# ---------------------------------------------------------------------------


class TestExtractPeriodInfo:
    def test_month_dd_yyyy(self):
        from psx_ohlcv.sources.financial_parser import _extract_period_info_extended

        lines = ["", "For the year ended December 31, 2024", ""]
        info = _extract_period_info_extended(lines)
        assert "2024" in info.get("period_end_date", "")
        assert info.get("period_type") == "annual"

    def test_dd_month_yyyy(self):
        from psx_ohlcv.sources.financial_parser import _extract_period_info_extended

        lines = ["", "For the quarter ended 30 September 2025", ""]
        info = _extract_period_info_extended(lines)
        assert info.get("period_end_date") == "2025-September-30"
        assert info.get("period_type") == "quarterly"

    def test_nine_month_period(self):
        from psx_ohlcv.sources.financial_parser import _extract_period_info_extended

        lines = ["", "For the nine month period ended 30 September 2025", ""]
        info = _extract_period_info_extended(lines)
        assert "2025" in info.get("period_end_date", "")
        assert info.get("period_type") == "quarterly"

    def test_as_at_dd_month(self):
        from psx_ohlcv.sources.financial_parser import _extract_period_info_extended

        lines = ["", "As at 30 September 2025", ""]
        info = _extract_period_info_extended(lines)
        assert info.get("period_end_date") == "2025-September-30"


# ---------------------------------------------------------------------------
# Label Matching
# ---------------------------------------------------------------------------


class TestLabelMatching:
    def test_pl_sales_regex(self):
        from psx_ohlcv.sources.financial_parser import PL_LABELS

        sales_pat = next(p for p, n in PL_LABELS if n == "sales")
        assert sales_pat.search("NET SALES")
        assert sales_pat.search("Revenue from operations")
        assert sales_pat.search("TURNOVER")
        # Note: "COST OF SALES" matches the regex, but _extract_line_items
        # has an exclusion: if field_name == "sales" and "COST" in line_upper

    def test_cost_of_sales_not_extracted_as_sales(self):
        """Verify _extract_line_items correctly excludes 'COST OF SALES' from 'sales'."""
        from psx_ohlcv.sources.financial_parser import PL_LABELS, _extract_line_items

        lines = ["Cost of Sales 30,000,000 25,000,000"]
        result = _extract_line_items(lines, PL_LABELS)
        # Should be extracted as cost_of_sales, not sales
        assert "cost_of_sales" in result
        assert "sales" not in result

    def test_pl_markup_earned(self):
        from psx_ohlcv.sources.financial_parser import PL_LABELS

        pat = next(p for p, n in PL_LABELS if n == "markup_earned")
        assert pat.search("Mark-up / return earned on:")
        assert pat.search("INTEREST EARNED")

    def test_pl_profit_after_tax_no_participation(self):
        from psx_ohlcv.sources.financial_parser import PL_LABELS

        pat = next(p for p, n in PL_LABELS if n == "profit_after_tax")
        assert pat.search("PROFIT AFTER TAX")
        assert pat.search("NET INCOME")
        # Should NOT match "Workers' profit PARTICIPATION fund"
        assert not pat.search("PARTICIPATION")

    def test_bs_total_equity_excludes_and_liabilities(self):
        from psx_ohlcv.sources.financial_parser import BS_LABELS

        pat = next(p for p, n in BS_LABELS if n == "total_equity")
        assert pat.search("TOTAL EQUITY")
        assert pat.search("SHAREHOLDERS EQUITY")
        assert pat.search("NET ASSETS")
        # Must NOT match "TOTAL EQUITY AND LIABILITIES"
        assert not pat.search("TOTAL EQUITY AND LIABILITIES")

    def test_bs_non_current_liabilities_dash(self):
        from psx_ohlcv.sources.financial_parser import BS_LABELS

        pat = next(p for p, n in BS_LABELS if n == "non_current_liabilities")
        assert pat.search("NON-CURRENT LIABILITIES")
        assert pat.search("NON CURRENT LIABILITIES")
        # "NON - CURRENT LIABILITIES" (with spaces around dash)
        assert pat.search("NON - CURRENT LIABILITIES")
        assert pat.search("LONG-TERM DEBT")


# ---------------------------------------------------------------------------
# Line Item Extraction
# ---------------------------------------------------------------------------


class TestExtractLineItems:
    def test_basic_extraction(self):
        from psx_ohlcv.sources.financial_parser import PL_LABELS, _extract_line_items

        lines = [
            "Net Sales 50,000,000 42,000,000",
            "Cost of Sales 30,000,000 25,000,000",
            "Gross Profit 20,000,000 17,000,000",
        ]
        result = _extract_line_items(lines, PL_LABELS)
        assert "sales" in result
        assert result["sales"][0] == 50_000_000
        assert "cost_of_sales" in result
        assert "gross_profit" in result

    def test_taxation_excludes_net_of(self):
        from psx_ohlcv.sources.financial_parser import PL_LABELS, _extract_line_items

        lines = [
            "Taxation 5,000,000 4,000,000",
            "Share of profit in associates- net of taxation 1,000,000 800,000",
        ]
        result = _extract_line_items(lines, PL_LABELS)
        assert "taxation" in result
        assert result["taxation"][0] == 5_000_000

    def test_first_match_wins(self):
        from psx_ohlcv.sources.financial_parser import PL_LABELS, _extract_line_items

        lines = [
            "Profit after tax 10,000,000 8,000,000",
            "Profit after tax for the year 10,000,000 8,000,000",
        ]
        result = _extract_line_items(lines, PL_LABELS)
        assert "profit_after_tax" in result
        assert result["profit_after_tax"][0] == 10_000_000


# ---------------------------------------------------------------------------
# BS Totals Extraction with A=E+L Identity
# ---------------------------------------------------------------------------


class TestExtractBSTotals:
    def test_bank_format(self):
        from psx_ohlcv.sources.financial_parser import _extract_bs_totals

        lines = [
            "ASSETS",
            "Cash and balances 100,000 80,000",
            "Investments 200,000 150,000",
            "300,000 230,000",
            "LIABILITIES",
            "Deposits 250,000 200,000",
            "Other liabilities 20,000 10,000",
            "270,000 210,000",
            "NET ASSETS 30,000 20,000",
        ]
        found: dict = {}
        _extract_bs_totals(lines, found)
        assert "total_assets" in found
        assert found["total_assets"][0] == 300_000
        assert "total_liabilities" in found
        assert found["total_liabilities"][0] == 270_000

    def test_ael_identity_computes_missing_equity(self):
        from psx_ohlcv.sources.financial_parser import _extract_bs_totals

        found: dict = {
            "total_assets": [100_000, 80_000],
            "total_liabilities": [60_000, 50_000],
        }
        _extract_bs_totals([], found)
        assert "total_equity" in found
        assert found["total_equity"][0] == 40_000

    def test_ael_identity_computes_missing_liabilities(self):
        from psx_ohlcv.sources.financial_parser import _extract_bs_totals

        found: dict = {
            "total_assets": [100_000, 80_000],
            "total_equity": [40_000, 30_000],
        }
        _extract_bs_totals([], found)
        assert "total_liabilities" in found
        assert found["total_liabilities"][0] == 60_000

    def test_equity_section_detection(self):
        from psx_ohlcv.sources.financial_parser import _extract_bs_totals

        lines = [
            "EQUITY AND LIABILITIES",
            "EQUITY",
            "Share capital 10,000 10,000",
            "Reserves 30,000 25,000",
            "40,000 35,000",
            "NON - CURRENT LIABILITIES",
            "Long term debt 20,000 18,000",
            "20,000 18,000",
            "CURRENT LIABILITIES",
            "Trade payables 15,000 12,000",
            "15,000 12,000",
            "CONTINGENCIES",
        ]
        found: dict = {}
        _extract_bs_totals(lines, found)
        assert "total_equity" in found
        assert found["total_equity"][0] == 40_000
        assert "total_liabilities" in found
        # NC (20K) + Current (15K) = 35K
        assert found["total_liabilities"][0] == 35_000


# ---------------------------------------------------------------------------
# Flatten to Financials
# ---------------------------------------------------------------------------


class TestFlattenParsed:
    def test_basic_flatten(self):
        from psx_ohlcv.sources.financial_parser import flatten_parsed_to_financials

        parsed = {
            "income_statement": {"sales": 100_000_000, "profit_after_tax": 10_000_000},
            "balance_sheet": {"total_assets": 200_000_000},
            "period_info": {"currency_scale": "thousands"},
            "prior_period": {
                "income_statement": {"sales": 90_000_000},
                "balance_sheet": {},
            },
            "source": "psx_pdf",
        }

        entries = flatten_parsed_to_financials(parsed, "TEST", "2024", "annual")
        assert len(entries) == 2

        current = entries[0]
        assert current["sales"] == 100_000_000
        assert current["total_assets"] == 200_000_000
        assert current["period_end"] == "2024"
        assert current["source"] == "psx_pdf"

        prior = entries[1]
        assert prior["sales"] == 90_000_000
        assert "2023" in prior["period_end"]

    def test_empty_parsed(self):
        from psx_ohlcv.sources.financial_parser import flatten_parsed_to_financials

        parsed = {
            "income_statement": {},
            "balance_sheet": {},
            "period_info": {},
            "prior_period": {"income_statement": {}, "balance_sheet": {}},
            "source": "psx_pdf",
        }
        entries = flatten_parsed_to_financials(parsed, "TEST", "2024", "annual")
        assert len(entries) == 0


# ---------------------------------------------------------------------------
# Financials Repository
# ---------------------------------------------------------------------------


class TestFinancialsSchema:
    def test_pdf_parse_log_created(self, con):
        from psx_ohlcv.db.repositories.financials import init_financials_schema

        init_financials_schema(con)
        tables = [
            r[0]
            for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        ]
        assert "pdf_parse_log" in tables

    def test_new_columns_added(self, con):
        from psx_ohlcv.db.repositories.financials import init_financials_schema

        init_financials_schema(con)
        cols = {
            r[1]
            for r in con.execute(
                "PRAGMA table_info(company_financials)"
            ).fetchall()
        }
        assert "cost_of_sales" in cols
        assert "total_assets" in cols
        assert "current_assets" in cols
        assert "source" in cols

    def test_ratio_columns_added(self, con):
        from psx_ohlcv.db.repositories.financials import init_financials_schema

        init_financials_schema(con)
        cols = {
            r[1]
            for r in con.execute(
                "PRAGMA table_info(company_ratios)"
            ).fetchall()
        }
        assert "debt_to_equity" in cols
        assert "current_ratio" in cols
        assert "interest_coverage" in cols


class TestPdfParseLog:
    def test_upsert_and_query(self, con):
        from psx_ohlcv.db.repositories.financials import (
            get_parsed_pdfs,
            upsert_pdf_parse_log,
        )

        entry = {
            "symbol": "TEST",
            "pdf_source": "psx_pdf",
            "pdf_id": "TEST-Q1-2024",
            "pdf_hash": "abc123",
            "parse_status": "success",
            "items_extracted": 15,
            "confidence": 0.95,
        }
        result = upsert_pdf_parse_log(con, entry)
        assert result is True

        df = get_parsed_pdfs(con, symbol="TEST")
        assert len(df) == 1
        assert df.iloc[0]["parse_status"] == "success"

    def test_is_pdf_parsed(self, con):
        from psx_ohlcv.db.repositories.financials import (
            is_pdf_parsed,
            upsert_pdf_parse_log,
        )

        upsert_pdf_parse_log(con, {
            "symbol": "TEST",
            "pdf_source": "psx_pdf",
            "pdf_hash": "hash123",
            "parse_status": "success",
        })
        assert is_pdf_parsed(con, "TEST", "hash123") is True
        assert is_pdf_parsed(con, "TEST", "other_hash") is False

    def test_is_psx_pdf_parsed(self, con):
        from psx_ohlcv.db.repositories.financials import (
            is_psx_pdf_parsed,
            upsert_pdf_parse_log,
        )

        upsert_pdf_parse_log(con, {
            "symbol": "TEST",
            "pdf_source": "psx_pdf",
            "pdf_id": "TEST-Q1-2024",
            "pdf_hash": "hash456",
            "parse_status": "success",
        })
        assert is_psx_pdf_parsed(con, "TEST", "TEST-Q1-2024") is True
        assert is_psx_pdf_parsed(con, "TEST", "TEST-Q2-2024") is False


class TestComputeRatios:
    def test_nonbank_ratios(self, con):
        from psx_ohlcv.db.repositories.company import upsert_company_financials
        from psx_ohlcv.db.repositories.financials import compute_ratios_from_financials

        # Seed a non-bank financial row
        upsert_company_financials(con, "TEST", [{
            "period_end": "2024",
            "period_type": "annual",
            "sales": 1_000_000_000,
            "gross_profit": 300_000_000,
            "operating_profit": 200_000_000,
            "profit_after_tax": 100_000_000,
            "total_assets": 2_000_000_000,
            "total_equity": 800_000_000,
            "total_liabilities": 1_200_000_000,
            "current_assets": 600_000_000,
            "current_liabilities": 400_000_000,
            "finance_cost": 50_000_000,
            "source": "psx_pdf",
        }])

        n = compute_ratios_from_financials(con, "TEST")
        assert n >= 1

        row = con.execute(
            "SELECT * FROM company_ratios WHERE symbol = 'TEST' AND period_end = '2024'"
        ).fetchone()
        assert row is not None
        assert row["gross_profit_margin"] == 30.0
        assert row["net_profit_margin"] == 10.0
        assert row["return_on_equity"] == 12.5
        assert row["debt_to_equity"] == 1.5
        assert row["current_ratio"] == 1.5
        assert row["interest_coverage"] == 4.0

    def test_bank_ratios(self, con):
        from psx_ohlcv.db.repositories.company import upsert_company_financials
        from psx_ohlcv.db.repositories.financials import compute_ratios_from_financials

        upsert_company_financials(con, "BANK", [{
            "period_end": "2024",
            "period_type": "annual",
            "markup_earned": 500_000_000_000,
            "markup_expensed": 300_000_000_000,
            "total_income": 250_000_000_000,
            "profit_after_tax": 50_000_000_000,
            "total_assets": 7_000_000_000_000,
            "total_equity": 400_000_000_000,
            "total_liabilities": 6_600_000_000_000,
            "source": "psx_pdf",
        }])

        n = compute_ratios_from_financials(con, "BANK")
        assert n >= 1

        row = con.execute(
            "SELECT * FROM company_ratios WHERE symbol = 'BANK' AND period_end = '2024'"
        ).fetchone()
        assert row is not None
        assert row["gross_profit_margin"] == 40.0  # (500-300)/500 * 100
        assert row["net_profit_margin"] == 20.0  # 50/250 * 100
        assert row["debt_to_equity"] == 16.5

    def test_sanity_check_blocks_bad_ratios(self, con):
        from psx_ohlcv.db.repositories.company import upsert_company_financials
        from psx_ohlcv.db.repositories.financials import compute_ratios_from_financials

        # Simulates mixed DPS + PDF data: sales from DPS (unscaled), PAT from PDF (scaled)
        upsert_company_financials(con, "MIX", [{
            "period_end": "2024",
            "period_type": "annual",
            "sales": 13_000,  # DPS, unscaled
            "profit_after_tax": 25_000_000_000,  # PDF, scaled
            "total_assets": 570_000_000_000,
            "total_equity": 255_000_000_000,
            "total_liabilities": 315_000_000_000,
            "source": "psx_pdf",
        }])

        compute_ratios_from_financials(con, "MIX")

        row = con.execute(
            "SELECT * FROM company_ratios WHERE symbol = 'MIX' AND period_end = '2024'"
        ).fetchone()
        assert row is not None
        # NPM should be NULL (blocked by sanity check)
        assert row["net_profit_margin"] is None
        # D/E should still be computed (doesn't depend on revenue)
        assert abs(row["debt_to_equity"] - 1.24) < 0.01


class TestGetParseSummary:
    def test_summary_stats(self, con):
        from psx_ohlcv.db.repositories.financials import (
            get_parse_summary,
            upsert_pdf_parse_log,
        )

        for i, status in enumerate(["success", "success", "failed", "partial"]):
            upsert_pdf_parse_log(con, {
                "symbol": f"SYM{i}",
                "pdf_source": "psx_pdf",
                "pdf_hash": f"hash{i}",
                "parse_status": status,
                "confidence": 0.9 if status == "success" else 0.3,
            })

        summary = get_parse_summary(con)
        assert summary["total"] == 4
        assert summary["success"] == 2
        assert summary["failed"] == 1
        assert summary["partial"] == 1
        assert summary["symbols_parsed"] == 4
