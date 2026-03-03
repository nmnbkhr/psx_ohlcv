"""Tests for OpenAI client with mocking.

Verifies:
- API configuration checks
- Request handling
- Error handling
- Response parsing
"""

import json
import os
from unittest.mock import MagicMock, patch

import pytest
import requests

from pakfindata.llm.client import (
    OpenAIClient,
    LLMResponse,
    LLMError,
    LLMRateLimitError,
    LLMTimeoutError,
    LLMAuthError,
    LLMContentError,
    is_api_key_configured,
    get_api_key,
    get_client,
    DEFAULT_MODEL,
    DEFAULT_TIMEOUT,
)


class TestAPIKeyConfiguration:
    """Tests for API key configuration."""

    def test_is_api_key_configured_false_when_missing(self):
        """Should return False when no API key set."""
        with patch.dict(os.environ, {}, clear=True):
            # Remove OPENAI_API_KEY if present
            os.environ.pop("OPENAI_API_KEY", None)
            assert is_api_key_configured() is False

    def test_is_api_key_configured_false_when_empty(self):
        """Should return False when API key is empty."""
        with patch.dict(os.environ, {"OPENAI_API_KEY": ""}, clear=True):
            assert is_api_key_configured() is False

    def test_is_api_key_configured_false_when_too_short(self):
        """Should return False when API key is too short."""
        with patch.dict(os.environ, {"OPENAI_API_KEY": "short"}, clear=True):
            assert is_api_key_configured() is False

    def test_is_api_key_configured_true_when_valid(self):
        """Should return True when API key looks valid."""
        with patch.dict(os.environ, {"OPENAI_API_KEY": "sk-test-key-1234567890"}, clear=True):
            assert is_api_key_configured() is True

    def test_get_api_key_raises_when_missing(self):
        """Should raise LLMAuthError when no API key."""
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("OPENAI_API_KEY", None)
            with pytest.raises(LLMAuthError):
                get_api_key()

    def test_get_api_key_returns_key_when_set(self):
        """Should return API key when set."""
        with patch.dict(os.environ, {"OPENAI_API_KEY": "sk-test-key"}, clear=True):
            key = get_api_key()
            assert key == "sk-test-key"


class TestLLMResponse:
    """Tests for LLMResponse dataclass."""

    def test_response_token_properties(self):
        """Should calculate token counts correctly."""
        response = LLMResponse(
            content="Test response",
            model="gpt-5.2",
            usage={
                "prompt_tokens": 100,
                "completion_tokens": 50,
                "total_tokens": 150,
            },
        )

        assert response.prompt_tokens == 100
        assert response.completion_tokens == 50
        assert response.total_tokens == 150

    def test_response_missing_usage(self):
        """Should handle missing usage data."""
        response = LLMResponse(
            content="Test",
            model="gpt-5.2",
            usage={},
        )

        assert response.prompt_tokens == 0
        assert response.completion_tokens == 0
        assert response.total_tokens == 0


class TestOpenAIClient:
    """Tests for OpenAIClient class with mocked HTTP."""

    @pytest.fixture
    def mock_env(self):
        """Set up mock environment with API key."""
        with patch.dict(os.environ, {"OPENAI_API_KEY": "sk-test-key-1234567890"}):
            yield

    @pytest.fixture
    def client(self, mock_env):
        """Create client with mocked API key."""
        return OpenAIClient()

    def test_client_initialization(self, mock_env):
        """Should initialize with default values."""
        client = OpenAIClient()

        assert client.model == DEFAULT_MODEL
        assert client.timeout == DEFAULT_TIMEOUT
        assert client.max_retries == 3

    def test_client_custom_initialization(self, mock_env):
        """Should accept custom parameters."""
        client = OpenAIClient(
            model="gpt-4",
            timeout=120,
            max_tokens=8000,
            temperature=0.7,
        )

        assert client.model == "gpt-4"
        assert client.timeout == 120
        assert client.max_tokens == 8000
        assert client.temperature == 0.7

    def test_generate_success(self, client):
        """Should parse successful API response."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "id": "chatcmpl-123",
            "model": "gpt-5.2",
            "choices": [
                {
                    "message": {"content": "Test response"},
                    "finish_reason": "stop",
                }
            ],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 5,
                "total_tokens": 15,
            },
        }

        with patch.object(client._session, "post", return_value=mock_response):
            response = client.generate("Test prompt")

            assert response.content == "Test response"
            assert response.model == "gpt-5.2"
            assert response.total_tokens == 15
            assert response.finish_reason == "stop"

    def test_generate_with_system_prompt(self, client):
        """Should include system prompt in request."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [{"message": {"content": "Response"}, "finish_reason": "stop"}],
            "model": "gpt-5.2",
            "usage": {},
        }

        with patch.object(client._session, "post", return_value=mock_response) as mock_post:
            client.generate("User prompt", system_prompt="System rules")

            # Check that system prompt was included
            call_args = mock_post.call_args
            payload = call_args.kwargs["json"]
            messages = payload["messages"]

            assert len(messages) == 2
            assert messages[0]["role"] == "system"
            assert messages[0]["content"] == "System rules"
            assert messages[1]["role"] == "user"

    def test_generate_rate_limit_error(self, client):
        """Should raise LLMRateLimitError on 429."""
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.json.return_value = {
            "error": {"message": "Rate limit exceeded", "type": "rate_limit_exceeded"}
        }

        with patch.object(client._session, "post", return_value=mock_response):
            with pytest.raises(LLMRateLimitError):
                client.generate("Test")

    def test_generate_auth_error(self, client):
        """Should raise LLMAuthError on 401."""
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.json.return_value = {
            "error": {"message": "Invalid API key", "type": "invalid_api_key"}
        }

        with patch.object(client._session, "post", return_value=mock_response):
            with pytest.raises(LLMAuthError):
                client.generate("Test")

    def test_generate_timeout_error(self, client):
        """Should raise LLMTimeoutError on timeout."""
        with patch.object(
            client._session, "post", side_effect=requests.exceptions.Timeout()
        ):
            with pytest.raises(LLMTimeoutError):
                client.generate("Test")

    def test_generate_generic_error(self, client):
        """Should raise LLMError on generic failure."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.json.return_value = {
            "error": {"message": "Internal error", "type": "server_error"}
        }

        with patch.object(client._session, "post", return_value=mock_response):
            with pytest.raises(LLMError):
                client.generate("Test")

    def test_token_estimate(self, client):
        """Should estimate token count."""
        text = "This is a test sentence with some words."

        estimate = client.count_tokens_estimate(text)

        # Rough estimate: ~4 chars per token
        assert estimate > 0
        assert estimate == len(text) // 4


class TestGetClient:
    """Tests for get_client factory function."""

    def test_get_client_creates_instance(self):
        """Should create client instance."""
        with patch.dict(os.environ, {"OPENAI_API_KEY": "sk-test-key-1234567890"}):
            # Clear any existing client
            import pakfindata.llm.client as client_module
            client_module._client = None

            client = get_client()
            assert isinstance(client, OpenAIClient)

    def test_get_client_reuses_instance(self):
        """Should reuse existing client instance."""
        with patch.dict(os.environ, {"OPENAI_API_KEY": "sk-test-key-1234567890"}):
            import pakfindata.llm.client as client_module
            client_module._client = None

            client1 = get_client()
            client2 = get_client()

            assert client1 is client2

    def test_get_client_recreates_with_kwargs(self):
        """Should create new client when kwargs provided."""
        with patch.dict(os.environ, {"OPENAI_API_KEY": "sk-test-key-1234567890"}):
            import pakfindata.llm.client as client_module
            client_module._client = None

            client1 = get_client()
            client2 = get_client(timeout=120)

            assert client2.timeout == 120


class TestErrorClasses:
    """Tests for error class hierarchy."""

    def test_error_inheritance(self):
        """All LLM errors should inherit from LLMError."""
        assert issubclass(LLMRateLimitError, LLMError)
        assert issubclass(LLMTimeoutError, LLMError)
        assert issubclass(LLMAuthError, LLMError)
        assert issubclass(LLMContentError, LLMError)

    def test_error_messages(self):
        """Errors should preserve messages."""
        error = LLMError("Test message")
        assert str(error) == "Test message"

        rate_error = LLMRateLimitError("Rate limit hit")
        assert "Rate limit" in str(rate_error)
