"""OpenAI GPT-5.2 client using the Responses API.

This module provides a centralized client for making OpenAI API calls
with proper error handling, retries, timeout, and optional streaming.

Configuration:
    Set OPENAI_API_KEY environment variable with your API key.
    Optionally set OPENAI_ORG_ID for organization-specific usage.

Example:
    >>> from psx_ohlcv.llm import get_client, is_api_key_configured
    >>> if is_api_key_configured():
    ...     client = get_client()
    ...     response = client.generate("Analyze this stock data...")
"""

import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Generator, Iterator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# OpenAI API Configuration
OPENAI_API_BASE = "https://api.openai.com/v1"
DEFAULT_MODEL = "gpt-5.2"
DEFAULT_TIMEOUT = 60  # seconds
DEFAULT_MAX_RETRIES = 3
DEFAULT_MAX_TOKENS = 4096
DEFAULT_TEMPERATURE = 0.3  # Lower for more factual responses


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


class LLMContentError(LLMError):
    """Raised when content is blocked or invalid."""
    pass


@dataclass
class LLMResponse:
    """Structured response from the LLM."""

    content: str
    model: str
    usage: dict[str, int] = field(default_factory=dict)
    finish_reason: str = ""
    response_id: str = ""

    @property
    def prompt_tokens(self) -> int:
        return self.usage.get("prompt_tokens", 0)

    @property
    def completion_tokens(self) -> int:
        return self.usage.get("completion_tokens", 0)

    @property
    def total_tokens(self) -> int:
        return self.usage.get("total_tokens", 0)


def is_api_key_configured() -> bool:
    """Check if OpenAI API key is configured in environment."""
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    return len(api_key) > 10  # Basic sanity check


def get_api_key() -> str:
    """Get OpenAI API key from environment.

    Raises:
        LLMAuthError: If API key is not configured.
    """
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise LLMAuthError(
            "OPENAI_API_KEY environment variable not set. "
            "Please set it with your OpenAI API key."
        )
    return api_key


def _create_session(max_retries: int = DEFAULT_MAX_RETRIES) -> requests.Session:
    """Create a requests session with retry logic."""
    session = requests.Session()

    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=1.0,  # 1s, 2s, 4s backoff
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST"],
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


class OpenAIClient:
    """OpenAI API client using the Responses API.

    This client is designed for use with GPT-5.2 and provides:
    - Automatic retries with exponential backoff
    - Timeout handling
    - Optional streaming responses
    - Token usage tracking

    Attributes:
        model: The model to use (default: gpt-5.2)
        timeout: Request timeout in seconds
        max_retries: Maximum number of retry attempts
        max_tokens: Maximum tokens in response
        temperature: Sampling temperature (0-2)
    """

    def __init__(
        self,
        model: str = DEFAULT_MODEL,
        timeout: int = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        max_tokens: int = DEFAULT_MAX_TOKENS,
        temperature: float = DEFAULT_TEMPERATURE,
    ):
        self.model = model
        self.timeout = timeout
        self.max_retries = max_retries
        self.max_tokens = max_tokens
        self.temperature = temperature
        self._session = _create_session(max_retries)
        self._api_key = get_api_key()
        self._org_id = os.environ.get("OPENAI_ORG_ID", "").strip()

    def _get_headers(self) -> dict[str, str]:
        """Build request headers."""
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }
        if self._org_id:
            headers["OpenAI-Organization"] = self._org_id
        return headers

    def generate(
        self,
        prompt: str,
        system_prompt: str | None = None,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> LLMResponse:
        """Generate a response from the LLM.

        Args:
            prompt: The user prompt/query.
            system_prompt: Optional system prompt to guide behavior.
            max_tokens: Override default max tokens.
            temperature: Override default temperature.

        Returns:
            LLMResponse with content and usage information.

        Raises:
            LLMRateLimitError: If rate limit is exceeded.
            LLMTimeoutError: If request times out.
            LLMContentError: If content is blocked.
            LLMError: For other API errors.
        """
        messages = []

        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})

        messages.append({"role": "user", "content": prompt})

        payload = {
            "model": self.model,
            "messages": messages,
            "max_completion_tokens": max_tokens or self.max_tokens,
            "temperature": temperature if temperature is not None else self.temperature,
        }

        try:
            response = self._session.post(
                f"{OPENAI_API_BASE}/chat/completions",
                headers=self._get_headers(),
                json=payload,
                timeout=self.timeout,
            )

            return self._handle_response(response)

        except requests.exceptions.Timeout:
            logger.error("OpenAI API request timed out after %ds", self.timeout)
            raise LLMTimeoutError(f"Request timed out after {self.timeout} seconds")

        except requests.exceptions.RequestException as e:
            logger.error("OpenAI API request failed: %s", e)
            raise LLMError(f"API request failed: {e}")

    def generate_stream(
        self,
        prompt: str,
        system_prompt: str | None = None,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> Generator[str, None, LLMResponse]:
        """Generate a streaming response from the LLM.

        Yields content chunks as they arrive, returns full LLMResponse at end.

        Args:
            prompt: The user prompt/query.
            system_prompt: Optional system prompt.
            max_tokens: Override default max tokens.
            temperature: Override default temperature.

        Yields:
            String chunks of the response as they arrive.

        Returns:
            Complete LLMResponse after stream ends.
        """
        messages = []

        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})

        messages.append({"role": "user", "content": prompt})

        payload = {
            "model": self.model,
            "messages": messages,
            "max_completion_tokens": max_tokens or self.max_tokens,
            "temperature": temperature if temperature is not None else self.temperature,
            "stream": True,
        }

        try:
            response = self._session.post(
                f"{OPENAI_API_BASE}/chat/completions",
                headers=self._get_headers(),
                json=payload,
                timeout=self.timeout,
                stream=True,
            )

            if response.status_code != 200:
                # Handle error response
                error_data = response.json()
                self._handle_error(response.status_code, error_data)

            full_content = []
            finish_reason = ""
            model = self.model
            response_id = ""

            for line in response.iter_lines():
                if not line:
                    continue

                line_str = line.decode("utf-8")
                if not line_str.startswith("data: "):
                    continue

                data_str = line_str[6:]  # Remove "data: " prefix

                if data_str == "[DONE]":
                    break

                try:
                    chunk = json.loads(data_str)
                    response_id = chunk.get("id", response_id)
                    model = chunk.get("model", model)

                    choices = chunk.get("choices", [])
                    if choices:
                        delta = choices[0].get("delta", {})
                        content = delta.get("content", "")
                        if content:
                            full_content.append(content)
                            yield content

                        if choices[0].get("finish_reason"):
                            finish_reason = choices[0]["finish_reason"]

                except json.JSONDecodeError:
                    continue

            # Return complete response
            return LLMResponse(
                content="".join(full_content),
                model=model,
                usage={},  # Usage not available in streaming
                finish_reason=finish_reason,
                response_id=response_id,
            )

        except requests.exceptions.Timeout:
            logger.error("OpenAI streaming request timed out")
            raise LLMTimeoutError(f"Streaming request timed out after {self.timeout} seconds")

        except requests.exceptions.RequestException as e:
            logger.error("OpenAI streaming request failed: %s", e)
            raise LLMError(f"Streaming request failed: {e}")

    def _handle_response(self, response: requests.Response) -> LLMResponse:
        """Handle API response, raising appropriate errors."""
        try:
            data = response.json()
        except json.JSONDecodeError:
            raise LLMError(f"Invalid JSON response: {response.text[:200]}")

        if response.status_code != 200:
            self._handle_error(response.status_code, data)

        # Extract response content
        choices = data.get("choices", [])
        if not choices:
            raise LLMContentError("No response choices returned")

        message = choices[0].get("message", {})
        content = message.get("content", "")

        return LLMResponse(
            content=content,
            model=data.get("model", self.model),
            usage=data.get("usage", {}),
            finish_reason=choices[0].get("finish_reason", ""),
            response_id=data.get("id", ""),
        )

    def _handle_error(self, status_code: int, data: dict) -> None:
        """Handle API error responses."""
        error = data.get("error", {})
        message = error.get("message", "Unknown error")
        error_type = error.get("type", "")

        logger.error(
            "OpenAI API error: status=%d, type=%s, message=%s",
            status_code, error_type, message
        )

        if status_code == 401:
            raise LLMAuthError(f"Authentication failed: {message}")

        if status_code == 429:
            raise LLMRateLimitError(f"Rate limit exceeded: {message}")

        if status_code == 400 and "content" in message.lower():
            raise LLMContentError(f"Content error: {message}")

        raise LLMError(f"API error ({status_code}): {message}")

    def count_tokens_estimate(self, text: str) -> int:
        """Estimate token count for text (rough approximation).

        Uses a simple heuristic: ~4 characters per token for English text.
        For accurate counts, use tiktoken library.
        """
        return len(text) // 4


# Module-level client instance (lazy initialization)
_client: OpenAIClient | None = None


def get_client(**kwargs) -> OpenAIClient:
    """Get or create the module-level OpenAI client.

    Args:
        **kwargs: Arguments passed to OpenAIClient constructor.

    Returns:
        Configured OpenAIClient instance.
    """
    global _client

    if _client is None or kwargs:
        _client = OpenAIClient(**kwargs)

    return _client
