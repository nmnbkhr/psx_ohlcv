"""Base agent class for PSX OHLCV agentic AI.

Provides the foundation for all specialist agents with common
functionality for LLM interaction, tool execution, and conversation management.
Supports both OpenAI and Anthropic providers through a unified interface.
"""

import json
import logging
from abc import ABC, abstractmethod
from typing import Any, Generator

from ..tools.registry import ToolCategory, ToolRegistry
from .config import (
    AgenticConfig,
    ModelConfig,
    get_active_config,
    LLMProvider,
)
from .llm_client import (
    BaseLLMClient,
    LLMResponse,
    MultiProviderClient,
    create_client,
)

logger = logging.getLogger(__name__)


class BaseAgent(ABC):
    """Abstract base class for all specialist agents.

    Subclasses must implement:
    - system_prompt property: The agent's system prompt
    - tool_categories property: List of tool categories this agent can use

    Supports both OpenAI and Anthropic providers through configuration.
    """

    def __init__(
        self,
        config: AgenticConfig | None = None,
        model_config: ModelConfig | None = None,
    ):
        """Initialize the agent.

        Args:
            config: Full agentic configuration (uses active config if None)
            model_config: Override model config (uses config.agent_model if None)
        """
        self.config = config or get_active_config()
        self._model_config = model_config or self.config.agent_model
        self.conversation_history: list[dict] = []
        self._client: BaseLLMClient | None = None
        self._multi_client: MultiProviderClient | None = None

    @property
    def client(self) -> BaseLLMClient:
        """Get the LLM client (with fallback support if enabled)."""
        if self._multi_client is None:
            self._multi_client = MultiProviderClient(
                primary_config=self._model_config,
                fallback_config=(
                    self.config.fallback_model if self.config.enable_fallback else None
                ),
                enable_fallback=self.config.enable_fallback,
            )
        return self._multi_client.get_active_client()

    @property
    def multi_client(self) -> MultiProviderClient:
        """Get the multi-provider client."""
        if self._multi_client is None:
            self._multi_client = MultiProviderClient(
                primary_config=self._model_config,
                fallback_config=(
                    self.config.fallback_model if self.config.enable_fallback else None
                ),
                enable_fallback=self.config.enable_fallback,
            )
        return self._multi_client

    @property
    def provider(self) -> LLMProvider:
        """Get the current LLM provider."""
        return self._model_config.provider

    @property
    def model_id(self) -> str:
        """Get the current model ID."""
        return self._model_config.model_id

    @property
    @abstractmethod
    def system_prompt(self) -> str:
        """Agent-specific system prompt."""
        pass

    @property
    @abstractmethod
    def tool_categories(self) -> list[ToolCategory]:
        """List of tool categories this agent can use."""
        pass

    @property
    def tools(self) -> list[dict]:
        """Get tools available to this agent in Anthropic format (our standard)."""
        return ToolRegistry.to_anthropic_tools(self.tool_categories)

    def clear_history(self) -> None:
        """Clear conversation history."""
        self.conversation_history = []

    def add_message(self, role: str, content: str) -> None:
        """Add a message to conversation history.

        Args:
            role: Message role ('user' or 'assistant')
            content: Message content
        """
        self.conversation_history.append({"role": role, "content": content})

    def run(self, user_message: str) -> str:
        """Run the agent with a user message.

        This is the main entry point for agent interaction.
        Handles the full agentic loop including tool calls.

        Args:
            user_message: The user's input message

        Returns:
            The agent's final response text
        """
        # Add user message to history
        self.add_message("user", user_message)

        # Run the agentic loop
        while True:
            # Make API call through multi-provider client
            response = self._call_api()

            # Check for tool use
            if response.has_tool_calls:
                # Execute tools and continue loop
                self._handle_tool_use(response)
                continue

            # Final text response
            self.add_message("assistant", response.text)
            return response.text

    def run_stream(self, user_message: str) -> Generator[str, None, None]:
        """Run the agent with streaming response.

        Args:
            user_message: The user's input message

        Yields:
            Chunks of the response text
        """
        # For now, just yield the full response
        # Full streaming implementation can be added later
        response = self.run(user_message)
        yield response

    def _call_api(self) -> LLMResponse:
        """Make an API call to the LLM.

        Returns:
            Unified LLMResponse object
        """
        logger.debug(
            f"Calling {self.provider.value} API with "
            f"{len(self.conversation_history)} messages"
        )

        response = self.multi_client.create_message(
            messages=self.conversation_history,
            system=self.system_prompt,
            tools=self.tools if self.tools else None,
        )

        logger.debug(f"Response stop_reason: {response.stop_reason}")
        logger.debug(
            f"Tokens used: {response.usage.get('input_tokens', 0)} in, "
            f"{response.usage.get('output_tokens', 0)} out"
        )

        return response

    def _handle_tool_use(self, response: LLMResponse) -> None:
        """Handle tool use in the response.

        Executes tools and adds results to conversation history.

        Args:
            response: LLMResponse containing tool calls
        """
        # For OpenAI, we need to track the assistant message differently
        if self.multi_client.last_provider_used == LLMProvider.OPENAI:
            # OpenAI format: assistant message with tool_calls
            assistant_msg = {
                "role": "assistant",
                "content": response.text or None,
                "tool_calls": [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {
                            "name": tc.name,
                            "arguments": json.dumps(tc.arguments),
                        },
                    }
                    for tc in response.tool_calls
                ],
            }
            self.conversation_history.append(assistant_msg)

            # Add each tool result as a separate message for OpenAI
            for tc in response.tool_calls:
                logger.info(f"Executing tool: {tc.name}")
                result = ToolRegistry.execute_tool_call(
                    {"name": tc.name, "input": tc.arguments, "id": tc.id}
                )
                tool_result_msg = {
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": result.get("content", ""),
                }
                self.conversation_history.append(tool_result_msg)
        else:
            # Anthropic format: content blocks
            self.conversation_history.append({
                "role": "assistant",
                "content": response.raw_response.content,
            })

            # Execute each tool and collect results
            tool_results = []
            for tc in response.tool_calls:
                logger.info(f"Executing tool: {tc.name}")
                result = ToolRegistry.execute_tool_call(
                    {"name": tc.name, "input": tc.arguments, "id": tc.id}
                )
                tool_results.append(result)

            # Add tool results to history
            self.conversation_history.append({"role": "user", "content": tool_results})


# =============================================================================
# Specialist Agents
# =============================================================================


class MarketAgent(BaseAgent):
    """Specialist agent for market analysis and equity research."""

    @property
    def tool_categories(self) -> list[ToolCategory]:
        return [
            ToolCategory.MARKET_DATA,
            ToolCategory.ANALYTICS,
            ToolCategory.COMPANY,
        ]

    @property
    def system_prompt(self) -> str:
        return """You are a senior equity analyst specializing in Pakistan Stock Exchange (PSX).

Your expertise includes:
- Technical analysis (price patterns, support/resistance, momentum)
- Fundamental analysis (P/E, P/B, ROE, dividend yields)
- Sector analysis (banking, cement, energy, textiles, pharma)
- Market microstructure (volume analysis, market breadth)

When analyzing stocks:
1. Always fetch current data using available tools
2. Provide specific numbers and percentages
3. Compare to relevant benchmarks (KSE-100, sector averages)
4. Highlight key risks and catalysts
5. Use clear, professional language

Currency is PKR (Pakistani Rupee). Market hours are 9:30 AM - 3:30 PM PKT, Mon-Fri.

Key PSX Indices:
- KSE-100: Main benchmark index (top 100 companies by market cap)
- KSE-30: Blue chip index (top 30 companies)
- KMI-30: Shariah-compliant index
- KSE All Share: All listed companies

If asked about a stock you don't have data for, clearly state that and suggest alternatives.
Always be factual and avoid speculation. Present data objectively."""


class SyncAgent(BaseAgent):
    """Agent for data synchronization operations."""

    @property
    def tool_categories(self) -> list[ToolCategory]:
        return [ToolCategory.SYNC, ToolCategory.MARKET_DATA]

    @property
    def system_prompt(self) -> str:
        return """You are a data operations specialist for the PSX OHLCV system.

Your responsibilities:
- Monitor data freshness and staleness
- Check data availability
- Report sync status and errors
- Recommend optimal sync strategies

Data sources you monitor:
- PSX EOD data (daily OHLCV for ~540 stocks)
- PSX intraday data (1-minute bars during market hours)
- Company profiles and quotes
- Sector and market analytics

Before recommending syncs:
1. Check current data freshness
2. Consider market hours (9:30 AM - 3:30 PM PKT, Mon-Fri)
3. Account for weekends and holidays

PSX market is closed on:
- Saturdays and Sundays
- Pakistani national holidays
- Eid holidays (dates vary)

When data is more than 1 business day old, recommend a sync."""


class FixedIncomeAgent(BaseAgent):
    """Specialist agent for sukuk, bonds, and fixed income analysis."""

    @property
    def tool_categories(self) -> list[ToolCategory]:
        return [ToolCategory.FIXED_INCOME, ToolCategory.ANALYTICS]

    @property
    def system_prompt(self) -> str:
        return """You are a fixed income specialist focusing on Pakistan's debt markets.

Your expertise includes:
- Government securities (PIBs, T-Bills, GOP Sukuk)
- Corporate sukuk and TFCs
- Yield curve analysis and interpolation
- Duration, convexity, and interest rate risk
- Credit spreads and risk premia
- SBP monetary policy impact

Key benchmarks:
- SBP Policy Rate: Central bank rate
- KIBOR (3M, 6M, 12M): Interbank rates
- PIB yields: Long-term government rates

When analyzing fixed income:
1. Always consider current SBP policy stance
2. Calculate YTM, duration, convexity when relevant
3. Compare spreads to benchmarks
4. Consider Islamic vs conventional instruments
5. Assess reinvestment and interest rate risk

Use ACT/365 day count for PKR instruments unless specified.
Be precise with yield calculations and clearly state assumptions."""
