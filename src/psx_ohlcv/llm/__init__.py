"""LLM integration module for PSX OHLCV Explorer.

Provides AI-powered insights using OpenAI GPT-5.2 via the Responses API.
"""

from .client import (
    OpenAIClient,
    get_client,
    is_api_key_configured,
    LLMError,
    LLMRateLimitError,
    LLMTimeoutError,
)
from .prompts import (
    PromptBuilder,
    InsightMode,
)
from .cache import (
    LLMCache,
    init_llm_cache_schema,
)
from .data_loader import (
    DataLoader,
    CompanyData,
    IntradayData,
    MarketData,
)

__all__ = [
    # Client
    "OpenAIClient",
    "get_client",
    "is_api_key_configured",
    "LLMError",
    "LLMRateLimitError",
    "LLMTimeoutError",
    # Prompts
    "PromptBuilder",
    "InsightMode",
    # Cache
    "LLMCache",
    "init_llm_cache_schema",
    # Data loader
    "DataLoader",
    "CompanyData",
    "IntradayData",
    "MarketData",
]
