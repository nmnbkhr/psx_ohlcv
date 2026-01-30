"""Base agent class for PSX OHLCV agentic AI.

Provides the foundation for all specialist agents with common
functionality for LLM interaction, tool execution, and conversation management.
"""

import json
import logging
import os
from abc import ABC, abstractmethod
from typing import Any, Generator

from ..tools.registry import ToolCategory, ToolRegistry

logger = logging.getLogger(__name__)


class BaseAgent(ABC):
    """Abstract base class for all specialist agents.

    Subclasses must implement:
    - system_prompt property: The agent's system prompt
    - tool_categories property: List of tool categories this agent can use
    """

    def __init__(
        self,
        model: str = "claude-sonnet-4-20250514",
        temperature: float = 0.1,
        max_tokens: int = 4096,
    ):
        """Initialize the agent.

        Args:
            model: Model identifier to use
            temperature: Sampling temperature (0.0 - 1.0)
            max_tokens: Maximum tokens in response
        """
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.conversation_history: list[dict] = []
        self._client = None

    @property
    def client(self):
        """Lazy-load the Anthropic client."""
        if self._client is None:
            try:
                from anthropic import Anthropic

                self._client = Anthropic()
            except ImportError:
                raise ImportError(
                    "anthropic package not installed. "
                    "Install with: pip install anthropic"
                )
        return self._client

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
        """Get tools available to this agent in Anthropic format."""
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
            # Make API call
            response = self._call_api()

            # Check for tool use
            if response.stop_reason == "tool_use":
                # Execute tools and continue loop
                self._handle_tool_use(response)
                continue

            # Extract final text response
            text_response = self._extract_text(response.content)
            self.add_message("assistant", text_response)

            return text_response

    def run_stream(self, user_message: str) -> Generator[str, None, None]:
        """Run the agent with streaming response.

        Args:
            user_message: The user's input message

        Yields:
            Chunks of the response text
        """
        # For now, just yield the full response
        # Streaming implementation can be added later
        response = self.run(user_message)
        yield response

    def _call_api(self) -> Any:
        """Make an API call to the LLM.

        Returns:
            The API response object
        """
        logger.debug(f"Calling API with {len(self.conversation_history)} messages")

        response = self.client.messages.create(
            model=self.model,
            max_tokens=self.max_tokens,
            temperature=self.temperature,
            system=self.system_prompt,
            tools=self.tools if self.tools else None,
            messages=self.conversation_history,
        )

        logger.debug(f"API response stop_reason: {response.stop_reason}")
        return response

    def _handle_tool_use(self, response: Any) -> None:
        """Handle tool use in the response.

        Executes tools and adds results to conversation history.

        Args:
            response: API response containing tool use blocks
        """
        # Add assistant message with tool use to history
        self.conversation_history.append(
            {"role": "assistant", "content": response.content}
        )

        # Execute each tool and collect results
        tool_results = []
        for block in response.content:
            if block.type == "tool_use":
                logger.info(f"Executing tool: {block.name}")
                result = ToolRegistry.execute_tool_call(
                    {"name": block.name, "input": block.input, "id": block.id}
                )
                tool_results.append(result)

        # Add tool results to history
        self.conversation_history.append({"role": "user", "content": tool_results})

    def _extract_text(self, content: list) -> str:
        """Extract text from response content blocks.

        Args:
            content: List of content blocks from API response

        Returns:
            Concatenated text from all text blocks
        """
        texts = []
        for block in content:
            if hasattr(block, "text"):
                texts.append(block.text)
        return "\n".join(texts)


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
