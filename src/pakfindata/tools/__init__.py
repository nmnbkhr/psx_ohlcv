"""Tool registry and callable tools for agentic AI.

Tools wrap existing PSX OHLCV functions to make them callable by AI agents.
Each tool has a name, description, parameters schema, and execution function.
"""

from .registry import Tool, ToolCategory, ToolRegistry, tool

# Import all tool modules to register them
from . import market_tools  # noqa: F401
from . import sync_tools  # noqa: F401
from . import analytics_tools  # noqa: F401
from . import fi_tools  # noqa: F401

__all__ = ["Tool", "ToolCategory", "ToolRegistry", "tool"]
