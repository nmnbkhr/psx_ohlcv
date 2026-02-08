"""Tests for LLM prompt construction.

Verifies that prompts contain all required sections:
- Data Used section
- Hard Rules section
- PSX Caveats section (including derived high/low warning)
"""

import pytest

from psx_ohlcv.agents.prompts import (
    PromptBuilder,
    InsightMode,
    SYSTEM_PROMPT,
    format_ohlcv_for_prompt,
    format_quote_for_prompt,
    get_data_caveat_warning,
)


class TestSystemPrompt:
    """Tests for the system prompt content."""

    def test_system_prompt_contains_hard_rules(self):
        """System prompt must contain hard rules section."""
        assert "HARD RULES" in SYSTEM_PROMPT
        assert "NEVER invent" in SYSTEM_PROMPT
        assert "hallucinate" in SYSTEM_PROMPT

    def test_system_prompt_contains_derived_high_low_warning(self):
        """System prompt must warn about derived high/low."""
        assert "DERIVED HIGH/LOW" in SYSTEM_PROMPT
        # Check that the warning mentions the data is not true intraday data
        assert "NOT" in SYSTEM_PROMPT and "intraday" in SYSTEM_PROMPT.lower()
        # Check that it mentions how high/low is calculated
        assert "max" in SYSTEM_PROMPT.lower() or "derived" in SYSTEM_PROMPT.lower()

    def test_system_prompt_contains_psx_context(self):
        """System prompt must contain PSX market context."""
        assert "Pakistan Stock Exchange" in SYSTEM_PROMPT
        assert "circuit breaker" in SYSTEM_PROMPT.lower()
        assert "7.5%" in SYSTEM_PROMPT


class TestPromptBuilder:
    """Tests for PromptBuilder class."""

    def test_company_prompt_contains_required_sections(self):
        """Company prompt must contain all required sections."""
        builder = PromptBuilder(InsightMode.COMPANY)
        prompt = builder.build(
            symbol="TEST",
            company_name="Test Company",
            sector="Test Sector",
        )

        # Check required sections
        assert "DATA USED SECTION" in prompt
        assert "HARD RULES REMINDER" in prompt
        assert "PSX CAVEATS REMINDER" in prompt
        assert "DERIVED HIGH/LOW" in prompt

    def test_intraday_prompt_contains_required_sections(self):
        """Intraday prompt must contain all required sections."""
        builder = PromptBuilder(InsightMode.INTRADAY)
        prompt = builder.build(
            symbol="TEST",
            trading_date="2024-01-15",
        )

        assert "DATA USED SECTION" in prompt
        assert "HARD RULES REMINDER" in prompt
        assert "PSX CAVEATS REMINDER" in prompt

    def test_market_prompt_contains_required_sections(self):
        """Market prompt must contain all required sections."""
        builder = PromptBuilder(InsightMode.MARKET)
        prompt = builder.build(
            market_date="2024-01-15",
        )

        assert "DATA USED SECTION" in prompt
        assert "HARD RULES REMINDER" in prompt
        assert "PSX CAVEATS REMINDER" in prompt
        assert "DERIVED HIGH/LOW" in prompt

    def test_history_prompt_contains_required_sections(self):
        """History prompt must contain all required sections."""
        builder = PromptBuilder(InsightMode.HISTORY)
        prompt = builder.build(
            symbol="TEST",
        )

        assert "DATA USED SECTION" in prompt
        assert "HARD RULES REMINDER" in prompt
        assert "PSX CAVEATS REMINDER" in prompt
        assert "CRITICAL: DERIVED HIGH/LOW" in prompt

    def test_missing_fields_handled_gracefully(self):
        """Builder should handle missing fields without error."""
        builder = PromptBuilder(InsightMode.COMPANY)

        # Should not raise KeyError
        prompt = builder.build(symbol="TEST")

        assert "TEST" in prompt
        assert "Not provided" in prompt or "No" in prompt

    def test_prompt_hash_inputs(self):
        """Verify prompt hash inputs extraction."""
        builder = PromptBuilder(InsightMode.COMPANY)

        hash_inputs = builder.get_prompt_hash_inputs(
            symbol="OGDC",
            date_range="2024-01-01 to 2024-01-31",
        )

        assert hash_inputs["mode"] == "company"
        assert hash_inputs["symbol"] == "OGDC"
        assert "date_range" in hash_inputs


class TestFormatFunctions:
    """Tests for data formatting functions."""

    def test_format_ohlcv_empty_dataframe(self):
        """Format function should handle empty DataFrame."""
        import pandas as pd

        result = format_ohlcv_for_prompt(pd.DataFrame())
        assert "No OHLCV data" in result

    def test_format_ohlcv_none_input(self):
        """Format function should handle None input."""
        result = format_ohlcv_for_prompt(None)
        assert "No OHLCV data" in result

    def test_format_ohlcv_with_data(self):
        """Format function should create markdown table with data."""
        import pandas as pd

        df = pd.DataFrame({
            "date": ["2024-01-15", "2024-01-16"],
            "open": [100.0, 101.0],
            "high": [102.0, 103.0],
            "low": [99.0, 100.0],
            "close": [101.0, 102.0],
            "volume": [1000000, 1500000],
        })

        result = format_ohlcv_for_prompt(df)

        # Should contain data (either as table or string)
        assert "2024-01-15" in result or "date" in result.lower()
        # Should contain derived high/low warning
        assert "derived" in result.lower()

    def test_format_quote_empty_dict(self):
        """Format function should handle empty quote."""
        result = format_quote_for_prompt({})
        assert "No quote data" in result

    def test_format_quote_with_data(self):
        """Format function should format quote data."""
        quote = {
            "close": 150.50,
            "change_value": 2.50,
            "change_percent": 1.69,
            "volume": 1000000,
        }

        result = format_quote_for_prompt(quote)

        # Check price is formatted (may be 150.5 or 150.50)
        assert "150.5" in result
        assert "Volume" in result
        # High/low fields show derived marker
        assert "derived" in result.lower()

    def test_data_caveat_warning_content(self):
        """Verify data caveat warning contains key information."""
        warning = get_data_caveat_warning()

        assert "max(open, close)" in warning
        assert "min(open, close)" in warning
        assert "NOT true intraday" in warning


class TestPromptModes:
    """Test different prompt modes work correctly."""

    @pytest.mark.parametrize("mode", list(InsightMode))
    def test_all_modes_generate_valid_prompts(self, mode):
        """All insight modes should generate valid prompts."""
        builder = PromptBuilder(mode)
        prompt = builder.build(symbol="TEST")

        # All prompts should have content
        assert len(prompt) > 100

        # All prompts should have analysis request
        assert "ANALYSIS REQUEST" in prompt

    def test_mode_enum_values(self):
        """Verify InsightMode enum values."""
        assert InsightMode.COMPANY.value == "company"
        assert InsightMode.INTRADAY.value == "intraday"
        assert InsightMode.MARKET.value == "market"
        assert InsightMode.HISTORY.value == "history"
