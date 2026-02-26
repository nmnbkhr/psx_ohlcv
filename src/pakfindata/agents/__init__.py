"""Agentic AI layer for PakFinData.

This module provides AI-powered agents that can autonomously analyze
market data, execute operations, and generate insights through natural
language interaction.

Consolidated from the former ``llm/`` package — all AI functionality
now lives under ``agents/``.
"""

from .base import BaseAgent, MarketAgent, SyncAgent, FixedIncomeAgent
from .orchestrator import AgentOrchestrator, chat
from .cache import LLMCache, init_llm_cache_schema, get_db_freshness_marker
from .prompts import PromptBuilder, InsightMode
from .data_loader import DataLoader, CompanyData, IntradayData, MarketData, format_data_for_prompt

__all__ = [
    # Agents
    "BaseAgent",
    "MarketAgent",
    "SyncAgent",
    "FixedIncomeAgent",
    "AgentOrchestrator",
    "chat",
    # Cache (moved from llm/)
    "LLMCache",
    "init_llm_cache_schema",
    "get_db_freshness_marker",
    # Prompts (moved from llm/)
    "PromptBuilder",
    "InsightMode",
    # Data loader (moved from llm/)
    "DataLoader",
    "CompanyData",
    "IntradayData",
    "MarketData",
    "format_data_for_prompt",
]
