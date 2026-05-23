"""Tests for symbol-only market inference."""

import pytest

from pakfindata.db.market_inference import infer_market


class TestInferMarket:
    def test_empty_returns_reg(self):
        assert infer_market("") == "REG"

    @pytest.mark.parametrize("sym", [
        "OGDC", "HBL", "LUCK", "ENGRO", "786", "ABL",
    ])
    def test_plain_equity(self, sym):
        assert infer_market(sym) == "REG"

    @pytest.mark.parametrize("sym", [
        "OGDC-JAN", "HBL-FEB", "LUCK-MAR", "ENGRO-APR",
        "MARI-MAY", "PPL-JUN", "FCCL-JUL", "MCB-AUG",
        "UBL-SEP", "HUBC-OCT", "KAPCO-NOV", "PSO-DEC",
    ])
    def test_month_suffix_futures(self, sym):
        assert infer_market(sym) == "FUT"

    @pytest.mark.parametrize("sym", [
        "OGDC-JANB", "HBL-FEBB", "LUCK-MARB", "UBL-APRB",
    ])
    def test_b_contract_futures(self, sym):
        assert infer_market(sym) == "FUT"

    @pytest.mark.parametrize("sym", [
        "OGDC-CJAN", "HBL-CFEB", "LUCK-CMAR",
        "OGDC-CAPRB",  # continuous B-contract
    ])
    def test_continuous_contracts(self, sym):
        assert infer_market(sym) == "CONT"

    @pytest.mark.parametrize("sym", ["KSE100", "KSE30", "KMI30", "KMIALLSHR", "JSMFI"])
    def test_index_tickers(self, sym):
        assert infer_market(sym) == "IDX"

    @pytest.mark.parametrize("sym", ["KSE30-FEB", "KMI30-APR"])
    def test_index_futures(self, sym):
        assert infer_market(sym) == "IDX_FUT"

    def test_unknown_suffix_falls_back_to_reg(self):
        # Corporate-action codes shouldn't be classified as FUT
        assert infer_market("OGDC-XD") == "REG"
        assert infer_market("OGDC-NC") == "REG"

    def test_odl_not_inferable_from_symbol(self):
        # ODL shares the symbol with REG — cannot distinguish from symbol alone
        assert infer_market("MZNPETF") == "REG"
