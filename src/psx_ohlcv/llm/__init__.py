"""DEPRECATED — LLM module has been consolidated into agents/.

All AI functionality now lives under ``psx_ohlcv.agents``.
This shim re-exports symbols for backward compatibility only.
"""

import warnings as _warnings

_warnings.warn(
    "psx_ohlcv.llm is deprecated — use psx_ohlcv.agents instead",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export from agents for backward compatibility
from ..agents.llm_client import LLMError, LLMRateLimitError, LLMTimeoutError, is_api_key_configured  # noqa: F401
from ..agents.prompts import PromptBuilder, InsightMode  # noqa: F401
from ..agents.cache import LLMCache, init_llm_cache_schema  # noqa: F401
from ..agents.data_loader import DataLoader, CompanyData, IntradayData, MarketData  # noqa: F401

# Legacy client is kept for backward compat but should not be used for new code
from .client import OpenAIClient, get_client  # noqa: F401
