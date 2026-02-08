"""Tool registry for agentic AI.

Provides a central registry for tools that can be called by AI agents.
Tools wrap existing PSX OHLCV functions with metadata for LLM tool use.
"""

import json
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

logger = logging.getLogger(__name__)


class ToolCategory(Enum):
    """Categories of tools available to agents."""

    MARKET_DATA = "market_data"
    SYNC = "sync"
    ANALYTICS = "analytics"
    FIXED_INCOME = "fixed_income"
    FX = "fx"
    MUTUAL_FUNDS = "mutual_funds"
    COMPANY = "company"
    VISUALIZATION = "visualization"


@dataclass
class Tool:
    """A callable tool that can be invoked by AI agents.

    Attributes:
        name: Unique identifier for the tool
        description: Human-readable description for LLM understanding
        function: The actual callable to execute
        category: Tool category for agent routing
        parameters: JSON Schema for input parameters
        requires_confirmation: If True, requires user confirmation before execution
        returns_description: Description of what the tool returns
    """

    name: str
    description: str
    function: Callable[..., Any]
    category: ToolCategory
    parameters: dict = field(default_factory=dict)
    requires_confirmation: bool = False
    returns_description: str = ""

    def execute(self, **kwargs) -> Any:
        """Execute the tool with given parameters.

        Args:
            **kwargs: Parameters to pass to the function

        Returns:
            Result from the function execution

        Raises:
            Exception: Any exception from the underlying function
        """
        logger.info(f"Executing tool: {self.name} with params: {kwargs}")
        try:
            result = self.function(**kwargs)
            logger.info(f"Tool {self.name} completed successfully")
            return result
        except Exception as e:
            logger.error(f"Tool {self.name} failed: {e}")
            raise

    def to_anthropic_format(self) -> dict:
        """Convert to Anthropic tool_use format.

        Returns:
            Dict compatible with Anthropic's tools API
        """
        return {
            "name": self.name,
            "description": self.description,
            "input_schema": self.parameters,
        }

    def to_openai_format(self) -> dict:
        """Convert to OpenAI function calling format.

        Returns:
            Dict compatible with OpenAI's function calling API
        """
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters,
            },
        }


class ToolRegistry:
    """Central registry for all available tools.

    This is a singleton-style registry that stores all tools
    and provides methods for querying and executing them.
    """

    _tools: dict[str, Tool] = {}
    _initialized: bool = False

    @classmethod
    def register(cls, tool: Tool) -> Tool:
        """Register a tool in the registry.

        Args:
            tool: Tool instance to register

        Returns:
            The registered tool (for decorator chaining)
        """
        if tool.name in cls._tools:
            logger.warning(f"Overwriting existing tool: {tool.name}")
        cls._tools[tool.name] = tool
        logger.debug(f"Registered tool: {tool.name}")
        return tool

    @classmethod
    def get(cls, name: str) -> Tool | None:
        """Get a tool by name.

        Args:
            name: Tool name

        Returns:
            Tool instance or None if not found
        """
        return cls._tools.get(name)

    @classmethod
    def get_all(cls) -> list[Tool]:
        """Get all registered tools.

        Returns:
            List of all Tool instances
        """
        return list(cls._tools.values())

    @classmethod
    def get_by_category(cls, category: ToolCategory) -> list[Tool]:
        """Get tools by category.

        Args:
            category: Tool category to filter by

        Returns:
            List of tools in the specified category
        """
        return [t for t in cls._tools.values() if t.category == category]

    @classmethod
    def get_by_categories(cls, categories: list[ToolCategory]) -> list[Tool]:
        """Get tools by multiple categories.

        Args:
            categories: List of categories to filter by

        Returns:
            List of tools in any of the specified categories
        """
        return [t for t in cls._tools.values() if t.category in categories]

    @classmethod
    def to_anthropic_tools(
        cls, categories: list[ToolCategory] | None = None
    ) -> list[dict]:
        """Convert tools to Anthropic format.

        Args:
            categories: Optional list of categories to filter by.
                       If None, returns all tools.

        Returns:
            List of tool definitions in Anthropic format
        """
        if categories is None:
            tools = cls.get_all()
        else:
            tools = cls.get_by_categories(categories)
        return [t.to_anthropic_format() for t in tools]

    @classmethod
    def to_openai_tools(
        cls, categories: list[ToolCategory] | None = None
    ) -> list[dict]:
        """Convert tools to OpenAI format.

        Args:
            categories: Optional list of categories to filter by.
                       If None, returns all tools.

        Returns:
            List of tool definitions in OpenAI format
        """
        if categories is None:
            tools = cls.get_all()
        else:
            tools = cls.get_by_categories(categories)
        return [t.to_openai_format() for t in tools]

    @classmethod
    def execute(cls, name: str, **kwargs) -> Any:
        """Execute a tool by name.

        Args:
            name: Tool name
            **kwargs: Parameters to pass to the tool

        Returns:
            Result from tool execution

        Raises:
            ValueError: If tool not found
        """
        tool = cls.get(name)
        if tool is None:
            raise ValueError(f"Tool not found: {name}")
        return tool.execute(**kwargs)

    @classmethod
    def execute_tool_call(cls, tool_call: dict) -> dict:
        """Execute a tool call from LLM response.

        Args:
            tool_call: Dict with 'name' and 'input' keys

        Returns:
            Dict with 'tool_use_id', 'content', and optional 'is_error' keys
        """
        tool_name = tool_call.get("name")
        tool_input = tool_call.get("input", {})
        tool_id = tool_call.get("id", "unknown")

        try:
            result = cls.execute(tool_name, **tool_input)
            # Convert result to JSON-serializable format
            if isinstance(result, dict):
                content = json.dumps(result, default=str)
            else:
                content = str(result)

            return {
                "type": "tool_result",
                "tool_use_id": tool_id,
                "content": content,
            }
        except Exception as e:
            logger.error(f"Tool execution error: {e}")
            return {
                "type": "tool_result",
                "tool_use_id": tool_id,
                "content": f"Error executing tool: {str(e)}",
                "is_error": True,
            }

    @classmethod
    def clear(cls) -> None:
        """Clear all registered tools. Useful for testing."""
        cls._tools.clear()

    @classmethod
    def count(cls) -> int:
        """Get the number of registered tools."""
        return len(cls._tools)

    @classmethod
    def list_names(cls) -> list[str]:
        """Get list of all tool names."""
        return list(cls._tools.keys())

    @classmethod
    def summary(cls) -> dict:
        """Get a summary of registered tools by category.

        Returns:
            Dict mapping category names to tool counts
        """
        summary = {}
        for tool in cls._tools.values():
            cat = tool.category.value
            if cat not in summary:
                summary[cat] = []
            summary[cat].append(tool.name)
        return summary


def tool(
    name: str,
    description: str,
    category: ToolCategory,
    parameters: dict | None = None,
    requires_confirmation: bool = False,
    returns_description: str = "",
) -> Callable:
    """Decorator to register a function as a tool.

    Usage:
        @tool(
            name="get_stock_price",
            description="Get stock price for a symbol",
            category=ToolCategory.MARKET_DATA,
            parameters={...}
        )
        def get_stock_price(symbol: str) -> dict:
            ...

    Args:
        name: Tool name
        description: Tool description
        category: Tool category
        parameters: JSON Schema for parameters
        requires_confirmation: If True, requires user confirmation
        returns_description: Description of return value

    Returns:
        Decorator function
    """

    def decorator(func: Callable) -> Callable:
        t = Tool(
            name=name,
            description=description,
            function=func,
            category=category,
            parameters=parameters or {"type": "object", "properties": {}},
            requires_confirmation=requires_confirmation,
            returns_description=returns_description,
        )
        ToolRegistry.register(t)
        return func

    return decorator
