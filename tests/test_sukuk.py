"""Tests for Phase 3 Sukuk/Debt Market Analytics."""

import tempfile
from pathlib import Path

import pytest


class TestSukukManual:
    """Tests for sukuk_manual.py CSV loaders."""

    def test_get_default_sukuk(self):
        """Test default sukuk data generation."""
        from pakfindata.sources.sukuk_manual import get_default_sukuk

        sukuk_list = get_default_sukuk()

        assert len(sukuk_list) > 0
        assert all("instrument_id" in s for s in sukuk_list)
        assert all("maturity_date" in s for s in sukuk_list)
        assert all("category" in s for s in sukuk_list)

        # Check for different categories
        categories = {s["category"] for s in sukuk_list}
        assert "GOP_SUKUK" in categories
        assert "PIB" in categories
        assert "TBILL" in categories

    def test_generate_sample_quotes(self):
        """Test sample quote generation."""
        from pakfindata.sources.sukuk_manual import (
            generate_sample_quotes,
            get_default_sukuk,
        )

        sukuk_list = get_default_sukuk()[:2]
        quotes = generate_sample_quotes(sukuk_list, days=5)

        assert len(quotes) > 0
        assert all("instrument_id" in q for q in quotes)
        assert all("quote_date" in q for q in quotes)
        assert all("yield_to_maturity" in q for q in quotes)

    def test_generate_sample_yield_curve(self):
        """Test sample yield curve generation."""
        from pakfindata.sources.sukuk_manual import generate_sample_yield_curve

        points = generate_sample_yield_curve("2026-01-15", "GOP_SUKUK")

        assert len(points) > 0
        assert all("curve_name" in p for p in points)
        assert all("tenor_days" in p for p in points)
        assert all("yield_rate" in p for p in points)

    def test_load_sukuk_master_csv(self):
        """Test loading sukuk master from CSV."""
        from pakfindata.sources.sukuk_manual import load_sukuk_master_csv

        csv_content = """instrument_id,issuer,name,category,maturity_date,coupon_rate
TEST-SUKUK-1,Test Issuer,Test Sukuk,GOP_SUKUK,2027-01-15,15.0
TEST-SUKUK-2,Test Issuer 2,Test Sukuk 2,PIB,2028-01-15,14.5"""

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as f:
            f.write(csv_content)
            csv_path = f.name

        try:
            sukuk_list = load_sukuk_master_csv(csv_path)
            assert len(sukuk_list) == 2
            assert sukuk_list[0]["instrument_id"] == "TEST-SUKUK-1"
            assert sukuk_list[1]["category"] == "PIB"
        finally:
            Path(csv_path).unlink()


class TestSukukAnalytics:
    """Tests for sukuk analytics calculations."""

    def test_calculate_ytm_coupon_bond(self):
        """Test YTM calculation for coupon bond."""
        from pakfindata.analytics_sukuk import calculate_ytm

        # Bond priced at par should have YTM close to coupon rate
        ytm = calculate_ytm(
            price=100.0,
            face_value=100.0,
            coupon_rate=15.0,
            years_to_maturity=3.0,
            frequency=2,
        )

        assert ytm is not None
        assert 14.5 < ytm < 15.5  # Should be close to coupon rate

    def test_calculate_ytm_zero_coupon(self):
        """Test YTM calculation for zero coupon bond."""
        from pakfindata.analytics_sukuk import calculate_ytm

        # T-Bill style zero coupon
        ytm = calculate_ytm(
            price=96.0,
            face_value=100.0,
            coupon_rate=0.0,
            years_to_maturity=0.25,  # 3 months
            frequency=0,
        )

        assert ytm is not None
        assert ytm > 0

    def test_calculate_macaulay_duration(self):
        """Test Macaulay duration calculation."""
        from pakfindata.analytics_sukuk import calculate_macaulay_duration

        duration = calculate_macaulay_duration(
            ytm=15.0,
            coupon_rate=15.0,
            years_to_maturity=5.0,
            frequency=2,
        )

        assert duration is not None
        assert duration < 5.0  # Should be less than maturity
        assert duration > 0

    def test_calculate_modified_duration(self):
        """Test modified duration calculation."""
        from pakfindata.analytics_sukuk import (
            calculate_macaulay_duration,
            calculate_modified_duration,
        )

        mac_dur = calculate_macaulay_duration(
            ytm=15.0,
            coupon_rate=15.0,
            years_to_maturity=5.0,
            frequency=2,
        )

        mod_dur = calculate_modified_duration(
            macaulay_duration=mac_dur,
            ytm=15.0,
            frequency=2,
        )

        assert mod_dur is not None
        assert mod_dur < mac_dur  # Modified should be less than Macaulay

    def test_calculate_convexity(self):
        """Test convexity calculation."""
        from pakfindata.analytics_sukuk import calculate_convexity

        convexity = calculate_convexity(
            ytm=15.0,
            coupon_rate=15.0,
            years_to_maturity=5.0,
            frequency=2,
        )

        assert convexity is not None
        assert convexity > 0

    def test_interpolate_yield_curve(self):
        """Test yield curve interpolation."""
        from pakfindata.analytics_sukuk import interpolate_yield_curve

        curve_points = [
            {"tenor_days": 365, "yield_rate": 14.0},
            {"tenor_days": 730, "yield_rate": 15.0},
            {"tenor_days": 1095, "yield_rate": 15.5},
        ]

        # Interpolate at 2 years (730 days) should give 15.0
        rate = interpolate_yield_curve(curve_points, 730)
        assert rate == 15.0

        # Interpolate between points
        rate = interpolate_yield_curve(curve_points, 547)  # ~1.5 years
        assert rate is not None
        assert 14.0 < rate < 15.0


class TestSukukSync:
    """Tests for sukuk sync operations."""

    def test_seed_sukuk(self):
        """Test seeding sukuk master data."""
        from pakfindata.sync_sukuk import seed_sukuk

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            result = seed_sukuk(db_path=db_path)

            assert result["success"] is True
            assert result["inserted"] > 0
            assert result["total"] > 0
        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_sync_sample_quotes(self):
        """Test syncing sample quotes."""
        from pakfindata.sync_sukuk import seed_sukuk, sync_sample_quotes

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            # First seed instruments
            seed_sukuk(db_path=db_path)

            # Then sync quotes
            summary = sync_sample_quotes(db_path=db_path, days=5)

            assert summary.rows_upserted > 0
            assert summary.ok > 0
        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_get_data_summary(self):
        """Test data summary retrieval."""
        from pakfindata.sync_sukuk import get_data_summary, seed_sukuk

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            seed_sukuk(db_path=db_path)
            summary = get_data_summary(db_path=db_path)

            assert "total_instruments" in summary
            assert summary["total_instruments"] > 0
        finally:
            Path(db_path).unlink(missing_ok=True)


class TestSBPPrimaryMarket:
    """Tests for SBP primary market document handling."""

    def test_parse_document_filename(self):
        """Test document filename parsing."""
        from pakfindata.sources.sbp_primary_market import parse_document_filename

        # Test standard format
        info = parse_document_filename("TBILL_AUCTION_RESULT_2026-01-15.pdf")
        assert info["instrument_type"] == "TBILL"
        assert info["doc_type"] == "AUCTION_RESULT"
        assert info["auction_date"] == "2026-01-15"

        # Test PIB format
        info = parse_document_filename("PIB_YIELD_CURVE_2026-01.xlsx")
        assert info["instrument_type"] == "PIB"
        assert info["doc_type"] == "YIELD_CURVE"

    def test_generate_doc_id(self):
        """Test document ID generation."""
        from pakfindata.sources.sbp_primary_market import generate_doc_id

        doc_id = generate_doc_id("TBILL", "AUCTION_RESULT", "2026-01-15")
        assert doc_id == "SBP:TBILL:AUCTION_RESULT:2026-01-15"

    def test_get_sbp_document_urls(self):
        """Test SBP URL retrieval."""
        from pakfindata.sources.sbp_primary_market import get_sbp_document_urls

        urls = get_sbp_document_urls()

        assert "primary_market" in urls
        assert "t_bill_auction" in urls
        assert all(url.startswith("https://") for url in urls.values())


class TestDatabaseOperations:
    """Tests for database operations."""

    def test_sukuk_crud(self):
        """Test sukuk CRUD operations."""
        from pakfindata.db import (
            connect,
            get_sukuk,
            get_sukuk_list,
            init_schema,
            upsert_sukuk,
        )

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            con = connect(db_path)
            init_schema(con)

            # Test insert
            sukuk_data = {
                "instrument_id": "TEST-SUKUK-001",
                "issuer": "Test Issuer",
                "name": "Test Sukuk",
                "category": "GOP_SUKUK",
                "maturity_date": "2027-01-15",
                "coupon_rate": 15.0,
                "coupon_frequency": 2,
                "shariah_compliant": True,
            }
            result = upsert_sukuk(con, sukuk_data)
            assert result is True

            # Test get
            sukuk = get_sukuk(con, "TEST-SUKUK-001")
            assert sukuk is not None
            assert sukuk["name"] == "Test Sukuk"

            # Test list
            sukuk_list = get_sukuk_list(con)
            assert len(sukuk_list) >= 1

            con.close()
        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_sukuk_quote_crud(self):
        """Test sukuk quote CRUD operations."""
        from pakfindata.db import (
            connect,
            get_sukuk_latest_quote,
            init_schema,
            upsert_sukuk,
            upsert_sukuk_quote,
        )

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            con = connect(db_path)
            init_schema(con)

            # Insert sukuk first
            upsert_sukuk(con, {
                "instrument_id": "TEST-SUKUK-002",
                "issuer": "Test",
                "name": "Test",
                "category": "PIB",
                "maturity_date": "2028-01-15",
            })

            # Insert quote
            quote_data = {
                "instrument_id": "TEST-SUKUK-002",
                "quote_date": "2026-01-15",
                "clean_price": 98.5,
                "yield_to_maturity": 15.25,
            }
            result = upsert_sukuk_quote(con, quote_data)
            assert result is True

            # Get latest quote
            quote = get_sukuk_latest_quote(con, "TEST-SUKUK-002")
            assert quote is not None
            assert quote["clean_price"] == 98.5

            con.close()
        finally:
            Path(db_path).unlink(missing_ok=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
