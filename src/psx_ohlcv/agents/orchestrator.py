"""Agent orchestrator for PSX OHLCV agentic AI.

Routes user queries to appropriate specialist agents based on intent.
Manages conversation context across agent switches.
Supports both OpenAI and Anthropic providers through configuration.
"""

import logging
from typing import Generator

from .base import MarketAgent, SyncAgent, FixedIncomeAgent
from .config import AgenticConfig, get_active_config
from .llm_client import create_client

logger = logging.getLogger(__name__)


class AgentOrchestrator:
    """Routes user queries to appropriate specialist agents.

    The orchestrator:
    1. Classifies user intent
    2. Routes to the appropriate specialist agent
    3. Manages conversation context
    4. Handles agent switching
    """

    # Intent classification prompt
    ROUTING_PROMPT = """Classify the user's intent into one of these categories:

MARKET - Questions about:
- Stock prices, equity analysis
- Market overview, gainers/losers
- Sector performance
- Company information
- Stock comparisons
- Technical or fundamental analysis

FIXED_INCOME - Questions about:
- Sukuk, bonds, PIBs, T-Bills
- Yield curves, interest rates
- Duration, convexity
- SBP policy rates, KIBOR

SYNC - Questions about:
- Data freshness, last update
- Syncing or refreshing data
- Data availability

GENERAL - For:
- Greetings, help requests
- General questions about the system
- Unclear or ambiguous queries

Respond with ONLY the category name (MARKET, FIXED_INCOME, SYNC, or GENERAL)."""

    def __init__(self, config: AgenticConfig | None = None):
        """Initialize the orchestrator.

        Args:
            config: Agentic configuration (uses active config if None)
        """
        self.config = config or get_active_config()
        self._routing_client = None

        # Initialize specialist agents with shared config
        self.agents = {
            "MARKET": MarketAgent(config=self.config),
            "FIXED_INCOME": FixedIncomeAgent(config=self.config),
            "SYNC": SyncAgent(config=self.config),
        }

        # Track current agent for context
        self.current_agent_name: str | None = None
        self.conversation_context: list[dict] = []

    @property
    def routing_client(self):
        """Lazy-load the LLM client for routing (uses fast/cheap model)."""
        if self._routing_client is None:
            self._routing_client = create_client(self.config.routing_model)
        return self._routing_client

    def route(self, user_message: str) -> str:
        """Determine which agent should handle the message.

        Args:
            user_message: The user's input

        Returns:
            Agent category name (MARKET, FIXED_INCOME, SYNC, or GENERAL)
        """
        try:
            # Use the configured routing model (fast/cheap)
            response = self.routing_client.create_message(
                messages=[
                    {
                        "role": "user",
                        "content": f"{self.ROUTING_PROMPT}\n\nUser message: {user_message}",
                    }
                ],
                max_tokens=20,
            )

            category = response.text.strip().upper()
            logger.info(f"Routed query to: {category} (using {self.config.routing_model.model_id})")

            # Validate category
            if category in self.agents:
                return category
            elif category == "GENERAL":
                return "MARKET"  # Default to market agent for general queries
            else:
                return "MARKET"  # Default fallback

        except Exception as e:
            logger.error(f"Routing error: {e}")
            return "MARKET"  # Default on error

    def process(self, user_message: str) -> str:
        """Process a user message through the appropriate agent.

        Args:
            user_message: The user's input

        Returns:
            The agent's response
        """
        # Route to appropriate agent
        agent_name = self.route(user_message)

        # Check if we're switching agents
        if agent_name != self.current_agent_name:
            logger.info(f"Switching from {self.current_agent_name} to {agent_name}")

            # Transfer context if we have prior conversation
            if self.conversation_context and agent_name in self.agents:
                # Give new agent summary of prior context
                agent = self.agents[agent_name]
                agent.conversation_history = []
                # Could add context summary here if needed

            self.current_agent_name = agent_name

        # Get the agent
        agent = self.agents.get(agent_name)
        if not agent:
            return "I'm not sure how to help with that. Could you rephrase your question?"

        # Run the agent
        response = agent.run(user_message)

        # Update context tracking
        self.conversation_context = agent.conversation_history.copy()

        return response

    def process_stream(self, user_message: str) -> Generator[str, None, None]:
        """Process with streaming response.

        Args:
            user_message: The user's input

        Yields:
            Response chunks
        """
        # For now, yield the full response
        response = self.process(user_message)
        yield response

    def clear_context(self) -> None:
        """Clear conversation context for all agents."""
        for agent in self.agents.values():
            agent.clear_history()
        self.conversation_context = []
        self.current_agent_name = None
        logger.info("Cleared all agent contexts")

    def get_current_agent(self) -> str | None:
        """Get the name of the current active agent."""
        return self.current_agent_name

    def get_agent_summary(self) -> dict:
        """Get summary of available agents.

        Returns:
            Dict with agent names and their tool counts
        """
        return {
            name: {
                "tools": len(agent.tools),
                "categories": [c.value for c in agent.tool_categories],
                "provider": agent.provider.value,
                "model": agent.model_id,
            }
            for name, agent in self.agents.items()
        }

    def get_config_summary(self) -> dict:
        """Get summary of current configuration.

        Returns:
            Dict with provider and model info
        """
        return {
            "primary_provider": self.config.primary_provider.value,
            "agent_model": self.config.agent_model.model_id,
            "routing_model": self.config.routing_model.model_id,
            "fallback_enabled": self.config.enable_fallback,
            "fallback_model": self.config.fallback_model.model_id if self.config.enable_fallback else None,
        }


# Convenience function for quick interactions
def chat(
    message: str,
    orchestrator: AgentOrchestrator | None = None,
    config: AgenticConfig | None = None,
) -> str:
    """Quick chat function for testing.

    Args:
        message: User message
        orchestrator: Optional orchestrator instance (creates new if None)
        config: Optional config for new orchestrator (ignored if orchestrator provided)

    Returns:
        Agent response
    """
    if orchestrator is None:
        orchestrator = AgentOrchestrator(config=config)
    return orchestrator.process(message)
