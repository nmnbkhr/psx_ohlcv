"""Tool registry and callable tools for agentic AI.

Tools wrap existing PSX OHLCV functions to make them callable by AI agents.
Each tool has a name, description, parameters schema, and execution function.
"""

from .registry import Tool, ToolCategory, ToolRegistry, tool

# Import market tools to register them
from . import market_tools  # noqa: F401

__all__ = ["Tool", "ToolCategory", "ToolRegistry", "tool"]
