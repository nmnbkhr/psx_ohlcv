"""Configuration for agentic AI providers and models.

Supports both OpenAI and Anthropic as LLM providers with easy switching.
"""

import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class LLMProvider(Enum):
    """Supported LLM providers."""

    OPENAI = "openai"
    ANTHROPIC = "anthropic"


@dataclass
class ModelConfig:
    """Configuration for a specific model."""

    provider: LLMProvider
    model_id: str
    temperature: float = 0.1
    max_tokens: int = 4096
    # Provider-specific options
    options: dict[str, Any] = field(default_factory=dict)


# =============================================================================
# OpenAI Models
# =============================================================================

# Note: gpt-4o is used as it supports function calling (tools)
# chatgpt-4o-latest does NOT support function calling
OPENAI_GPT4O_LATEST = ModelConfig(
    provider=LLMProvider.OPENAI,
    model_id="gpt-4o",  # Latest GPT-4o with function calling support
    temperature=0.1,
    max_tokens=4096,
)

OPENAI_GPT4O = ModelConfig(
    provider=LLMProvider.OPENAI,
    model_id="gpt-4o",
    temperature=0.1,
    max_tokens=4096,
)

OPENAI_GPT4O_MINI = ModelConfig(
    provider=LLMProvider.OPENAI,
    model_id="gpt-4o-mini",
    temperature=0.0,
    max_tokens=1024,
)

# =============================================================================
# Anthropic Claude Models
# =============================================================================

ANTHROPIC_CLAUDE_SONNET = ModelConfig(
    provider=LLMProvider.ANTHROPIC,
    model_id="claude-sonnet-4-20250514",
    temperature=0.1,
    max_tokens=4096,
)

ANTHROPIC_CLAUDE_HAIKU = ModelConfig(
    provider=LLMProvider.ANTHROPIC,
    model_id="claude-3-haiku-20240307",
    temperature=0.0,
    max_tokens=1024,
)

ANTHROPIC_CLAUDE_OPUS = ModelConfig(
    provider=LLMProvider.ANTHROPIC,
    model_id="claude-opus-4-5-20251101",
    temperature=0.1,
    max_tokens=8192,
)


# =============================================================================
# Agentic Configuration
# =============================================================================

@dataclass
class AgenticConfig:
    """Main configuration for agentic AI system.

    Attributes:
        primary_provider: Main LLM provider to use
        agent_model: Model config for main agent responses
        routing_model: Model config for intent routing (fast, cheap)
        fallback_model: Fallback model if primary fails
        enable_fallback: Whether to use fallback on errors
        cache_enabled: Whether to cache LLM responses
        cache_ttl_hours: Cache time-to-live in hours
        max_retries: Max retries on API errors
        timeout_seconds: API timeout
    """

    primary_provider: LLMProvider = LLMProvider.OPENAI
    agent_model: ModelConfig = field(default_factory=lambda: OPENAI_GPT4O_LATEST)
    routing_model: ModelConfig = field(default_factory=lambda: OPENAI_GPT4O_MINI)
    fallback_model: ModelConfig | None = None
    enable_fallback: bool = False
    cache_enabled: bool = True
    cache_ttl_hours: int = 24
    max_retries: int = 3
    timeout_seconds: int = 60


# =============================================================================
# Preset Configurations
# =============================================================================

# Default: OpenAI GPT-5.2 (no fallback)
CONFIG_OPENAI_PRIMARY = AgenticConfig(
    primary_provider=LLMProvider.OPENAI,
    agent_model=OPENAI_GPT4O_LATEST,
    routing_model=OPENAI_GPT4O_MINI,
    fallback_model=None,
    enable_fallback=False,
)

# Alternative: Anthropic Claude with OpenAI fallback
CONFIG_ANTHROPIC_PRIMARY = AgenticConfig(
    primary_provider=LLMProvider.ANTHROPIC,
    agent_model=ANTHROPIC_CLAUDE_SONNET,
    routing_model=ANTHROPIC_CLAUDE_HAIKU,
    fallback_model=OPENAI_GPT4O,
    enable_fallback=True,
)

# Cost-optimized: Cheapest models
CONFIG_COST_OPTIMIZED = AgenticConfig(
    primary_provider=LLMProvider.OPENAI,
    agent_model=OPENAI_GPT4O_MINI,
    routing_model=OPENAI_GPT4O_MINI,
    fallback_model=None,
    enable_fallback=False,
)

# Maximum quality: Best models
CONFIG_MAX_QUALITY = AgenticConfig(
    primary_provider=LLMProvider.OPENAI,
    agent_model=OPENAI_GPT4O_LATEST,
    routing_model=OPENAI_GPT4O_MINI,
    fallback_model=None,
    enable_fallback=False,
)


# =============================================================================
# Active Configuration
# =============================================================================

def get_config() -> AgenticConfig:
    """Get the active agentic configuration.

    Can be overridden by environment variables:
    - PSX_LLM_PROVIDER: "openai" or "anthropic"
    - PSX_LLM_MODEL: Model ID override

    Returns:
        Active AgenticConfig instance
    """
    # Check for environment variable overrides
    provider_env = os.environ.get("PSX_LLM_PROVIDER", "").lower()

    if provider_env == "anthropic":
        return CONFIG_ANTHROPIC_PRIMARY
    elif provider_env == "openai":
        return CONFIG_OPENAI_PRIMARY

    # Default to OpenAI GPT-5.2
    return CONFIG_OPENAI_PRIMARY


# Global config instance (can be modified at runtime)
_active_config: AgenticConfig | None = None


def set_config(config: AgenticConfig) -> None:
    """Set the active configuration.

    Args:
        config: New configuration to use
    """
    global _active_config
    _active_config = config


def get_active_config() -> AgenticConfig:
    """Get the currently active configuration.

    Returns:
        Active AgenticConfig instance
    """
    global _active_config
    if _active_config is None:
        _active_config = get_config()
    return _active_config


# =============================================================================
# Helper Functions
# =============================================================================

def get_api_key(provider: LLMProvider) -> str | None:
    """Get API key for a provider from environment.

    Args:
        provider: LLM provider

    Returns:
        API key string or None if not set
    """
    if provider == LLMProvider.OPENAI:
        return os.environ.get("OPENAI_API_KEY")
    elif provider == LLMProvider.ANTHROPIC:
        return os.environ.get("ANTHROPIC_API_KEY")
    return None


def validate_config(config: AgenticConfig) -> list[str]:
    """Validate configuration and check for required API keys.

    Args:
        config: Configuration to validate

    Returns:
        List of validation errors (empty if valid)
    """
    errors = []

    # Check primary provider API key
    if not get_api_key(config.primary_provider):
        key_name = (
            "OPENAI_API_KEY"
            if config.primary_provider == LLMProvider.OPENAI
            else "ANTHROPIC_API_KEY"
        )
        errors.append(f"Missing {key_name} environment variable")

    # Check fallback provider API key if enabled
    if config.enable_fallback and config.fallback_model is not None:
        fallback_provider = config.fallback_model.provider
        if not get_api_key(fallback_provider):
            key_name = (
                "OPENAI_API_KEY"
                if fallback_provider == LLMProvider.OPENAI
                else "ANTHROPIC_API_KEY"
            )
            errors.append(f"Missing fallback {key_name} (optional)")

    return errors


def print_config(config: AgenticConfig | None = None) -> str:
    """Print configuration summary.

    Args:
        config: Config to print (uses active if None)

    Returns:
        Formatted config string
    """
    if config is None:
        config = get_active_config()

    fallback_str = config.fallback_model.model_id if config.fallback_model else "None"
    lines = [
        "PakFinData Agentic AI Configuration",
        "=" * 40,
        f"Primary Provider: {config.primary_provider.value}",
        f"Agent Model: {config.agent_model.model_id}",
        f"Routing Model: {config.routing_model.model_id}",
        f"Fallback Enabled: {config.enable_fallback}",
        f"Fallback Model: {fallback_str}",
        f"Cache Enabled: {config.cache_enabled}",
        f"Max Retries: {config.max_retries}",
        "=" * 40,
    ]

    # Check API keys
    errors = validate_config(config)
    if errors:
        lines.append("WARNINGS:")
        for err in errors:
            lines.append(f"  - {err}")

    return "\n".join(lines)
