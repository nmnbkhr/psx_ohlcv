"""LLM client abstraction for multi-provider support.

Provides a unified interface for both OpenAI and Anthropic APIs,
handling tool calls and responses in a provider-agnostic way.
"""

import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from .config import LLMProvider, ModelConfig, get_api_key

logger = logging.getLogger(__name__)


# =============================================================================
# Error Types (consolidated from former llm/client.py)
# =============================================================================

class LLMError(Exception):
    """Base exception for LLM-related errors."""
    pass


class LLMRateLimitError(LLMError):
    """Raised when API rate limit is exceeded."""
    pass


class LLMTimeoutError(LLMError):
    """Raised when API request times out."""
    pass


class LLMAuthError(LLMError):
    """Raised when API authentication fails."""
    pass


def is_api_key_configured() -> bool:
    """Check if any LLM API key is configured in environment."""
    return bool(get_api_key(LLMProvider.OPENAI) or get_api_key(LLMProvider.ANTHROPIC))


# =============================================================================
# Unified Response Types
# =============================================================================

@dataclass
class ToolCall:
    """Represents a tool call from the LLM."""

    id: str
    name: str
    arguments: dict[str, Any]


@dataclass
class LLMResponse:
    """Unified response from any LLM provider.

    Attributes:
        text: Text content of the response
        tool_calls: List of tool calls (if any)
        stop_reason: Why the response stopped
        usage: Token usage info
        raw_response: Original provider response
    """

    text: str
    tool_calls: list[ToolCall]
    stop_reason: str
    usage: dict[str, int]
    raw_response: Any = None

    @property
    def has_tool_calls(self) -> bool:
        """Check if response contains tool calls."""
        return len(self.tool_calls) > 0


# =============================================================================
# Abstract Base Client
# =============================================================================

class BaseLLMClient(ABC):
    """Abstract base class for LLM clients."""

    def __init__(self, config: ModelConfig):
        self.config = config
        self._client = None

    @abstractmethod
    def create_message(
        self,
        messages: list[dict],
        system: str | None = None,
        tools: list[dict] | None = None,
    ) -> LLMResponse:
        """Create a message/completion.

        Args:
            messages: Conversation messages
            system: System prompt
            tools: Available tools

        Returns:
            Unified LLMResponse
        """
        pass

    @abstractmethod
    def format_tool_result(
        self,
        tool_call_id: str,
        result: str,
        is_error: bool = False,
    ) -> dict:
        """Format a tool result for the provider.

        Args:
            tool_call_id: ID of the tool call
            result: Tool execution result
            is_error: Whether this is an error result

        Returns:
            Provider-formatted tool result
        """
        pass

    @abstractmethod
    def format_tools(self, tools: list[dict]) -> list[dict]:
        """Format tools for the provider.

        Args:
            tools: Tools in Anthropic format (our standard)

        Returns:
            Provider-formatted tools
        """
        pass


# =============================================================================
# OpenAI Client
# =============================================================================

class OpenAIClient(BaseLLMClient):
    """OpenAI API client wrapper."""

    @property
    def client(self):
        """Lazy-load OpenAI client."""
        if self._client is None:
            try:
                from openai import OpenAI

                api_key = get_api_key(LLMProvider.OPENAI)
                if not api_key:
                    raise ValueError("OPENAI_API_KEY environment variable not set")
                self._client = OpenAI(api_key=api_key)
            except ImportError:
                raise ImportError(
                    "openai package not installed. Install with: pip install openai"
                )
        return self._client

    def create_message(
        self,
        messages: list[dict],
        system: str | None = None,
        tools: list[dict] | None = None,
    ) -> LLMResponse:
        """Create a chat completion with OpenAI."""
        # Prepare messages with system prompt
        api_messages = []
        if system:
            api_messages.append({"role": "system", "content": system})

        # Convert messages to OpenAI format
        for msg in messages:
            api_messages.append(self._convert_message_to_openai(msg))

        # Prepare request kwargs
        kwargs = {
            "model": self.config.model_id,
            "messages": api_messages,
            "temperature": self.config.temperature,
            "max_tokens": self.config.max_tokens,
        }

        # Add tools if provided
        if tools:
            kwargs["tools"] = self.format_tools(tools)

        # Add reasoning effort if specified (GPT-5.2 feature)
        if "reasoning_effort" in self.config.options:
            kwargs["reasoning_effort"] = self.config.options["reasoning_effort"]

        logger.debug(f"OpenAI request: model={self.config.model_id}")

        response = self.client.chat.completions.create(**kwargs)

        return self._parse_response(response)

    def _convert_message_to_openai(self, msg: dict) -> dict:
        """Convert a message to OpenAI format."""
        role = msg.get("role")
        content = msg.get("content")

        # Handle tool messages (already in OpenAI format)
        if role == "tool":
            return {
                "role": "tool",
                "tool_call_id": msg.get("tool_call_id"),
                "content": content if isinstance(content, str) else str(content),
            }

        # Handle assistant messages with tool_calls (already in OpenAI format)
        if role == "assistant" and "tool_calls" in msg:
            return msg

        # Handle tool results (from Anthropic format)
        if role == "user" and isinstance(content, list):
            # Check if this is tool results
            if content and isinstance(content[0], dict) and content[0].get("type") == "tool_result":
                return {
                    "role": "tool",
                    "tool_call_id": content[0].get("tool_use_id"),
                    "content": content[0].get("content", ""),
                }

        # Handle assistant messages with tool use (from Anthropic format)
        if role == "assistant" and isinstance(content, list):
            tool_calls = []
            text_content = ""
            for block in content:
                if hasattr(block, "type"):
                    if block.type == "tool_use":
                        tool_calls.append({
                            "id": block.id,
                            "type": "function",
                            "function": {
                                "name": block.name,
                                "arguments": json.dumps(block.input),
                            },
                        })
                    elif block.type == "text":
                        text_content = block.text
                elif isinstance(block, dict):
                    if block.get("type") == "tool_use":
                        tool_calls.append({
                            "id": block.get("id"),
                            "type": "function",
                            "function": {
                                "name": block.get("name"),
                                "arguments": json.dumps(block.get("input", {})),
                            },
                        })

            result = {"role": "assistant", "content": text_content or None}
            if tool_calls:
                result["tool_calls"] = tool_calls
            return result

        # Standard message
        return {"role": role, "content": content if isinstance(content, str) else str(content)}

    def _parse_response(self, response) -> LLMResponse:
        """Parse OpenAI response to unified format."""
        choice = response.choices[0]
        message = choice.message

        # Extract text
        text = message.content or ""

        # Extract tool calls
        tool_calls = []
        if message.tool_calls:
            for tc in message.tool_calls:
                try:
                    args = json.loads(tc.function.arguments)
                except json.JSONDecodeError:
                    args = {"raw": tc.function.arguments}

                tool_calls.append(
                    ToolCall(
                        id=tc.id,
                        name=tc.function.name,
                        arguments=args,
                    )
                )

        # Map stop reason
        stop_reason = choice.finish_reason
        if stop_reason == "tool_calls":
            stop_reason = "tool_use"

        # Usage
        usage = {
            "input_tokens": response.usage.prompt_tokens,
            "output_tokens": response.usage.completion_tokens,
        }

        return LLMResponse(
            text=text,
            tool_calls=tool_calls,
            stop_reason=stop_reason,
            usage=usage,
            raw_response=response,
        )

    def format_tool_result(
        self,
        tool_call_id: str,
        result: str,
        is_error: bool = False,
    ) -> dict:
        """Format tool result for OpenAI."""
        return {
            "role": "tool",
            "tool_call_id": tool_call_id,
            "content": result,
        }

    def format_tools(self, tools: list[dict]) -> list[dict]:
        """Convert tools from Anthropic format to OpenAI format."""
        openai_tools = []
        for tool in tools:
            openai_tools.append({
                "type": "function",
                "function": {
                    "name": tool["name"],
                    "description": tool.get("description", ""),
                    "parameters": tool.get("input_schema", {"type": "object", "properties": {}}),
                },
            })
        return openai_tools


# =============================================================================
# Anthropic Client
# =============================================================================

class AnthropicClient(BaseLLMClient):
    """Anthropic API client wrapper."""

    @property
    def client(self):
        """Lazy-load Anthropic client."""
        if self._client is None:
            try:
                from anthropic import Anthropic

                api_key = get_api_key(LLMProvider.ANTHROPIC)
                if not api_key:
                    raise ValueError("ANTHROPIC_API_KEY environment variable not set")
                self._client = Anthropic(api_key=api_key)
            except ImportError:
                raise ImportError(
                    "anthropic package not installed. Install with: pip install anthropic"
                )
        return self._client

    def create_message(
        self,
        messages: list[dict],
        system: str | None = None,
        tools: list[dict] | None = None,
    ) -> LLMResponse:
        """Create a message with Anthropic."""
        kwargs = {
            "model": self.config.model_id,
            "max_tokens": self.config.max_tokens,
            "temperature": self.config.temperature,
            "messages": messages,
        }

        if system:
            kwargs["system"] = system

        if tools:
            kwargs["tools"] = self.format_tools(tools)

        logger.debug(f"Anthropic request: model={self.config.model_id}")

        response = self.client.messages.create(**kwargs)

        return self._parse_response(response)

    def _parse_response(self, response) -> LLMResponse:
        """Parse Anthropic response to unified format."""
        text_parts = []
        tool_calls = []

        for block in response.content:
            if block.type == "text":
                text_parts.append(block.text)
            elif block.type == "tool_use":
                tool_calls.append(
                    ToolCall(
                        id=block.id,
                        name=block.name,
                        arguments=block.input,
                    )
                )

        usage = {
            "input_tokens": response.usage.input_tokens,
            "output_tokens": response.usage.output_tokens,
        }

        return LLMResponse(
            text="\n".join(text_parts),
            tool_calls=tool_calls,
            stop_reason=response.stop_reason,
            usage=usage,
            raw_response=response,
        )

    def format_tool_result(
        self,
        tool_call_id: str,
        result: str,
        is_error: bool = False,
    ) -> dict:
        """Format tool result for Anthropic."""
        return {
            "type": "tool_result",
            "tool_use_id": tool_call_id,
            "content": result,
            **({"is_error": True} if is_error else {}),
        }

    def format_tools(self, tools: list[dict]) -> list[dict]:
        """Tools are already in Anthropic format."""
        return tools


# =============================================================================
# Factory Function
# =============================================================================

def create_client(config: ModelConfig) -> BaseLLMClient:
    """Create an LLM client for the given configuration.

    Args:
        config: Model configuration

    Returns:
        Appropriate LLM client instance
    """
    if config.provider == LLMProvider.OPENAI:
        return OpenAIClient(config)
    elif config.provider == LLMProvider.ANTHROPIC:
        return AnthropicClient(config)
    else:
        raise ValueError(f"Unknown provider: {config.provider}")


# =============================================================================
# Multi-Provider Client with Fallback
# =============================================================================

class MultiProviderClient:
    """Client that supports multiple providers with automatic fallback.

    Uses primary provider by default, falls back to secondary on errors.
    Fallback client is created lazily only when needed.
    """

    def __init__(
        self,
        primary_config: ModelConfig,
        fallback_config: ModelConfig | None = None,
        enable_fallback: bool = True,
    ):
        """Initialize multi-provider client.

        Args:
            primary_config: Primary model configuration
            fallback_config: Fallback model configuration (created lazily)
            enable_fallback: Whether to enable fallback
        """
        self.primary = create_client(primary_config)
        self._fallback_config = fallback_config
        self._fallback: BaseLLMClient | None = None
        self.enable_fallback = enable_fallback and fallback_config is not None
        self._last_provider_used: LLMProvider | None = None

    @property
    def fallback(self) -> BaseLLMClient | None:
        """Lazy-load fallback client only when needed."""
        if self._fallback is None and self._fallback_config is not None:
            try:
                self._fallback = create_client(self._fallback_config)
            except Exception as e:
                logger.warning(f"Failed to create fallback client: {e}")
                self._fallback = None
        return self._fallback

    @property
    def last_provider_used(self) -> LLMProvider | None:
        """Get the provider used for the last request."""
        return self._last_provider_used

    def create_message(
        self,
        messages: list[dict],
        system: str | None = None,
        tools: list[dict] | None = None,
    ) -> LLMResponse:
        """Create a message with automatic fallback.

        Args:
            messages: Conversation messages
            system: System prompt
            tools: Available tools

        Returns:
            LLMResponse from primary or fallback provider
        """
        # Try primary provider
        try:
            self._last_provider_used = self.primary.config.provider
            return self.primary.create_message(messages, system, tools)
        except Exception as e:
            logger.warning(f"Primary provider failed: {e}")

            if not self.enable_fallback or self.fallback is None:
                raise

            # Try fallback
            logger.info("Falling back to secondary provider")
            try:
                self._last_provider_used = self.fallback.config.provider
                return self.fallback.create_message(messages, system, tools)
            except Exception as fallback_error:
                logger.error(f"Fallback provider also failed: {fallback_error}")
                raise

    def format_tool_result(
        self,
        tool_call_id: str,
        result: str,
        is_error: bool = False,
    ) -> dict:
        """Format tool result using the last used provider's format."""
        if self._last_provider_used == LLMProvider.OPENAI:
            return self.primary.format_tool_result(tool_call_id, result, is_error)
        else:
            return self.primary.format_tool_result(tool_call_id, result, is_error)

    def get_active_client(self) -> BaseLLMClient:
        """Get the currently active client (primary)."""
        return self.primary


# =============================================================================
# Convenience Function
# =============================================================================

def get_completion(prompt: str, system: str | None = None) -> str:
    """Simple convenience function for one-shot completions.

    Auto-detects available API key and uses the appropriate provider.

    Args:
        prompt: User prompt text
        system: Optional system prompt

    Returns:
        Model response text

    Raises:
        LLMAuthError: If no API key is configured
    """
    from .config import get_api_key, LLMProvider, ModelConfig

    # Auto-detect provider
    if get_api_key(LLMProvider.ANTHROPIC):
        config = ModelConfig(
            provider=LLMProvider.ANTHROPIC,
            model_id="claude-sonnet-4-20250514",
        )
    elif get_api_key(LLMProvider.OPENAI):
        config = ModelConfig(
            provider=LLMProvider.OPENAI,
            model_id="gpt-4o-mini",
        )
    else:
        raise LLMAuthError("No API key configured. Set ANTHROPIC_API_KEY or OPENAI_API_KEY.")

    client = create_client(config)
    messages = [{"role": "user", "content": prompt}]
    response = client.create_message(messages, system=system)
    return response.text
