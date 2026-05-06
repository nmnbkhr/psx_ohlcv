"""Tests for pakfindata.markets -- symbol classification."""

import pytest
from pakfindata import markets


# --- Symbol-structure parsers (no DB needed for parent_of/expiry_of/contract_series) ---

class TestSymbolStructure:
    """Vendor-format structural parsers (don't require DB)."""

    def test_dfc_pattern_simple(self):
        assert markets.parent_of("OGDC-MAY") == "OGDC"
        assert markets.expiry_of("OGDC-MAY") == "MAY"
        assert markets.contract_series("OGDC-MAY") is None

    def test_csf_pattern_b_series(self):
        # Vendor-format CSF: SYMBOL-MMMB
        assert markets.contract_series("HUBC-MAYB") == "B"

    def test_csf_pattern_c_series(self):
        # Vendor-format CSF: SYMBOL-MMMC
        assert markets.contract_series("OGDC-MAYC") == "C"

    def test_csf_pattern_d_series(self):
        # Vendor-format CSF: SYMBOL-MMMD
        assert markets.contract_series("EFERT-MAYD") == "D"

    def test_equity_no_match(self):
        assert markets.contract_series("OGDC") is None

    def test_invalid_inputs(self):
        assert markets.parent_of("") is None
        assert markets.parent_of(None) is None
        assert markets.classify("") == "UNKNOWN"
        assert markets.classify(None) == "UNKNOWN"

    def test_alphanumeric_symbols(self):
        # Numeric-only doesn't parse as futures
        assert markets.contract_series("786") is None


class TestVendorTranslation:
    """Vendor (psxterminal.com) namespace -> canonical translation."""

    def test_fut_to_dfc(self):
        # Use a symbol not in any master so the structural fallback runs
        assert markets.from_vendor_market("FUT", "ZZZTEST-MAY") == "DFC"

    def test_fut_to_csf_vendor_form(self):
        # Vendor sends suffix-C / suffix-D form for CSF — caught by the vendor
        # regex when the symbol isn't in any master. The B suffix is NOT
        # distinguishable as CSF: _extract_month("MAYB") strips trailing B and
        # returns "MAY", so a B-suffix symbol structurally parses as DFC.
        # (Real PSX data confirms: -MMMB symbols ARE B-series deliverable, not
        # cash-settled.) PSX-known symbols like OGDC-MAYC are real FUT rows
        # and classify as DFC via cache (Critical Rule 6).
        assert markets.from_vendor_market("FUT", "ZZZTEST-MAYC") == "CSF"
        assert markets.from_vendor_market("FUT", "ZZZTEST-MAYD") == "CSF"

    def test_reg_passthrough(self):
        assert markets.from_vendor_market("REG", "OGDC") == "REG"

    def test_idx_passthrough(self):
        assert markets.from_vendor_market("IDX", "KSE100") == "IDX"

    def test_unknown_vendor_market(self):
        # ODL, BNB -- pass through
        assert markets.from_vendor_market("ODL", "FOO") == "ODL"


# --- Classifier (requires DB) ---

@pytest.fixture
def fresh_master():
    """Force master reload before each test."""
    markets.reload_master()
    yield
    markets.reload_master()


@pytest.mark.integration
class TestClassifier:
    """End-to-end classification against the live DB."""

    def test_known_equity(self, fresh_master):
        assert markets.classify("OGDC") == "REG"
        assert markets.classify("HUBC") == "REG"
        assert markets.classify("MARI") == "REG"

    def test_known_index(self, fresh_master):
        assert markets.classify("KSE100") == "IDX"
        assert markets.classify("KMI30") == "IDX"
        assert markets.classify("ALLSHR") == "IDX"

    def test_unknown_returns_unknown(self, fresh_master):
        assert markets.classify("MADEUP_SYMBOL_XYZ") == "UNKNOWN"

    def test_helpers(self, fresh_master):
        assert markets.is_equity("OGDC")
        assert markets.is_index("KSE100")
        assert markets.is_futures("OGDC-MAY")
        assert markets.is_deliverable_futures("OGDC-MAY")
        # OGDC-MAYC is a real FUT row in PSX data (DFC); use a synthetic
        # non-DB symbol to exercise the vendor-CSF regex fallback.
        assert markets.is_cash_settled_futures("ZZZTEST-MAYC")
        assert not markets.is_futures("OGDC")


@pytest.mark.integration
class TestPSXFuturesFormat:
    """PSX EOD format from futures_eod table."""

    def test_cont_simple(self, fresh_master):
        # PSX cash-settled: -C{MMM} prefix form
        assert markets.classify("OGDC-CMAR") == "CSF"
        assert markets.parent_of("OGDC-CMAR") == "OGDC"
        assert markets.expiry_of("OGDC-CMAR") == "MAR"

    def test_n_series_follows_stored_market_type(self, fresh_master):
        # N-series variants like EPQL-CMARN1 are stored as FUT in futures_eod
        # (PSX ingest mislabel — they look CONT-shaped but live in the FUT
        # bucket). Per design (trust stored market_type), they classify as DFC.
        # The parent is still extracted correctly.
        assert markets.classify("EPQL-CMARN1") == "DFC"
        assert markets.parent_of("EPQL-CMARN1") == "EPQL"

    def test_cont_other_real_samples(self, fresh_master):
        for sym, parent in [
            ("PAEL-CFEB", "PAEL"),
            ("ABL-CNOV", "ABL"),
            ("ACPL-CDEC", "ACPL"),
            ("ZAL-CMAY", "ZAL"),
        ]:
            assert markets.classify(sym) == "CSF", f"{sym} should be CSF"
            assert markets.parent_of(sym) == parent, f"{sym} parent should be {parent}"

    def test_psx_dfc_real_samples(self, fresh_master):
        # PSX deliverable FUT: clean SYMBOL-MMM
        for sym, parent in [
            ("ABL-APR", "ABL"),
            ("ABL-AUG", "ABL"),
            ("FEROZ-AUG", "FEROZ"),
            ("HUBC-JUN", "HUBC"),
        ]:
            assert markets.classify(sym) == "DFC", f"{sym} should be DFC"
            assert markets.parent_of(sym) == parent, f"{sym} parent should be {parent}"

    def test_psx_dfc_b_suffix(self, fresh_master):
        # PSX FUT B-suffix variant: SYMBOL-MMMB
        assert markets.classify("ABL-APRB") == "DFC"
        assert markets.parent_of("ABL-APRB") == "ABL"
        # _extract_month strips B -> month is APR
        assert markets.expiry_of("ABL-APRB") == "APR"


@pytest.mark.integration
class TestIDXFutAndODL:
    """New returnable classes in Phase 1.5."""

    def test_idx_fut(self, fresh_master):
        assert markets.classify("BKTI-APR") == "IDX_FUT"
        assert markets.classify("KSE30-APR") == "IDX_FUT"
        assert markets.is_index_futures("BKTI-APR")

    def test_odl(self, fresh_master):
        for sym in ["AKBLTFC6", "BAFLTFC5", "BIPLSC", "BYCOSC"]:
            assert markets.classify(sym) == "ODL", f"{sym} should be ODL"
            # ODL bonds have no parent equity
            assert markets.parent_of(sym) is None
        assert markets.is_odd_lot("BIPLSC")


@pytest.mark.integration
class TestETFCorrection:
    """The 9 ETFs that were mistyped as EQUITY in instruments."""

    @pytest.mark.parametrize("sym", [
        "ACIETF", "HBLTETF", "JSGBETF", "JSMFETF", "MIIETF",
        "MZNPETF", "NBPGETF", "NITGETF", "UBLPETF",
    ])
    def test_etf_classified(self, fresh_master, sym):
        assert markets.classify(sym) == "ETF", f"{sym} should be ETF, got {markets.classify(sym)}"


@pytest.mark.integration
class TestREITOverride:
    """The 4 REITs in instruments mistyped as EQUITY (+ SRR via Pass B)."""

    @pytest.mark.parametrize("sym", [
        "DCR", "GRR", "IREIT", "TPLRF1",  # in instruments as EQUITY -> overridden
        "SRR",                              # not in instruments -> caught by Pass B
    ])
    def test_reit_classified(self, fresh_master, sym):
        assert markets.classify(sym) == "REIT", f"{sym} should be REIT, got {markets.classify(sym)}"


@pytest.mark.integration
class TestStructuralFallback:
    """Symbols not in any master fall back to structural parsing.

    Used in production for fresh contracts arriving via psxterminal.com WS
    that haven't been EOD-ingested yet.
    """

    def test_novel_dfc_via_structure(self, fresh_master):
        # A made-up SYMBOL-MMM that won't be in any master
        # (using clearly fake parent name to avoid collisions)
        assert markets.classify("ZZZTEST-MAY") == "DFC"
        assert markets.parent_of("ZZZTEST-MAY") == "ZZZTEST"

    def test_novel_csf_psx_form_via_structure(self, fresh_master):
        # Made-up SYMBOL-CMMM PSX form
        assert markets.classify("ZZZTEST-CMAR") == "CSF"
        assert markets.parent_of("ZZZTEST-CMAR") == "ZZZTEST"

    def test_novel_csf_vendor_form_via_structure(self, fresh_master):
        # Vendor-format CSF for symbol not in any master
        assert markets.classify("ZZZTEST-MAYC") == "CSF"
