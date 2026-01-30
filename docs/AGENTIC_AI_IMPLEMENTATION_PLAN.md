# PSX OHLCV Agentic AI Implementation Plan

**Version:** 1.0.0
**Branch:** `agentic-ai-v1`
**Baseline:** `v0.2.0-pre-agentic-baseline`
**Author:** Quantitative AI Architect
**Date:** 2026-01-30

---

## Executive Summary

Transform PSX OHLCV Explorer from a button-driven dashboard into an intelligent, conversational agentic system that can autonomously fetch data, analyze markets, and provide actionable insights through natural language interaction.

---

## Architecture Overview

```
                    ┌─────────────────────────────────────────────┐
                    │           User Interface Layer              │
                    │  ┌─────────────┐  ┌──────────────────────┐  │
                    │  │ Chat UI     │  │ Existing Dashboard   │  │
                    │  │ (Streamlit) │  │ (Preserved)          │  │
                    │  └─────────────┘  └──────────────────────┘  │
                    └───────────────────────┬─────────────────────┘
                                            │
                    ┌───────────────────────▼─────────────────────┐
                    │           Agent Orchestrator                │
                    │  ┌─────────────────────────────────────┐    │
                    │  │  Intent Router (LLM-based)          │    │
                    │  │  - Classify user intent             │    │
                    │  │  - Route to specialist agent        │    │
                    │  │  - Handle multi-turn conversations  │    │
                    │  └─────────────────────────────────────┘    │
                    └───────────────────────┬─────────────────────┘
                                            │
        ┌───────────────┬───────────────────┼───────────────────┬────────────────┐
        │               │                   │                   │                │
        ▼               ▼                   ▼                   ▼                ▼
┌───────────────┐ ┌───────────────┐ ┌───────────────┐ ┌───────────────┐ ┌───────────────┐
│ Market Agent  │ │ Sync Agent    │ │ FI Agent      │ │ Report Agent  │ │ Alert Agent   │
│               │ │               │ │               │ │               │ │               │
│ - Price data  │ │ - EOD sync    │ │ - Sukuk/bonds │ │ - Summaries   │ │ - Monitoring  │
│ - Technicals  │ │ - Intraday    │ │ - Yield curves│ │ - Comparisons │ │ - Triggers    │
│ - Sector anal │ │ - FX rates    │ │ - Duration    │ │ - PDF export  │ │ - Notifications│
│ - Rankings    │ │ - Mutual funds│ │ - Spreads     │ │ - Charts      │ │ - Watchlists  │
└───────┬───────┘ └───────┬───────┘ └───────┬───────┘ └───────┬───────┘ └───────┬───────┘
        │               │                   │                   │                │
        └───────────────┴───────────────────┼───────────────────┴────────────────┘
                                            │
                    ┌───────────────────────▼─────────────────────┐
                    │              Tool Registry                  │
                    │  ┌─────────────────────────────────────┐    │
                    │  │ Wrapped existing functions as tools │    │
                    │  │ - get_eod_df()     - sync_all()     │    │
                    │  │ - get_symbols()    - compute_ytm()  │    │
                    │  │ - get_sectors()    - get_nav()      │    │
                    │  └─────────────────────────────────────┘    │
                    └───────────────────────┬─────────────────────┘
                                            │
                    ┌───────────────────────▼─────────────────────┐
                    │            Data Layer (Existing)            │
                    │  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
                    │  │ SQLite   │  │ PSX APIs │  │ SBP APIs │   │
                    │  │ Database │  │ (DPS)    │  │          │   │
                    │  └──────────┘  └──────────┘  └──────────┘   │
                    └─────────────────────────────────────────────┘
```

---

## Implementation Phases

### Phase 1: Foundation (Week 1-2)
**Goal:** Set up the agentic infrastructure without breaking existing functionality.

#### 1.1 Project Structure
```
src/psx_ohlcv/
├── agents/                     # NEW: Agent layer
│   ├── __init__.py
│   ├── base.py                 # BaseAgent class
│   ├── orchestrator.py         # Main router/orchestrator
│   ├── market_agent.py         # Market analysis agent
│   ├── sync_agent.py           # Data sync agent
│   ├── fi_agent.py             # Fixed income agent
│   ├── report_agent.py         # Report generation agent
│   └── alert_agent.py          # Monitoring/alert agent
│
├── tools/                      # NEW: Tool wrappers
│   ├── __init__.py
│   ├── registry.py             # Central tool registry
│   ├── market_tools.py         # Market data tools
│   ├── sync_tools.py           # Sync operation tools
│   ├── analytics_tools.py      # Analytics computation tools
│   ├── fi_tools.py             # Fixed income tools
│   └── chart_tools.py          # Visualization tools
│
├── memory/                     # NEW: Agent memory
│   ├── __init__.py
│   ├── conversation.py         # Conversation history
│   ├── user_context.py         # User preferences/watchlists
│   └── cache.py                # Result caching
│
├── prompts/                    # NEW: Prompt templates
│   ├── __init__.py
│   ├── system_prompts.py       # Agent system prompts
│   ├── templates.py            # Response templates
│   └── few_shot.py             # Few-shot examples
│
├── ui/
│   ├── app.py                  # Existing (preserved)
│   ├── chat.py                 # NEW: Chat interface component
│   └── ...
│
└── ... (existing modules preserved)
```

#### 1.2 Dependencies
Add to `pyproject.toml`:
```toml
[project.optional-dependencies]
agentic = [
    "anthropic>=0.40.0",        # Claude API
    "openai>=1.50.0",           # OpenAI fallback
    "langchain>=0.3.0",         # Agent framework (optional)
    "langgraph>=0.2.0",         # Multi-agent graphs
    "chromadb>=0.5.0",          # Vector store for RAG
    "tiktoken>=0.7.0",          # Token counting
]
```

#### 1.3 Configuration
```python
# config.py additions
AGENTIC_CONFIG = {
    "llm_provider": "anthropic",        # or "openai"
    "model": "claude-3-5-sonnet-20241022",
    "temperature": 0.1,                 # Low for consistency
    "max_tokens": 4096,
    "retry_attempts": 3,
    "tool_timeout_seconds": 60,
    "conversation_memory_limit": 20,    # Messages to retain
    "cache_ttl_hours": 24,
}
```

---

### Phase 2: Tool Layer (Week 2-3)
**Goal:** Wrap all existing functionality as callable tools.

#### 2.1 Tool Interface
```python
# tools/registry.py
from dataclasses import dataclass
from typing import Callable, Any
from enum import Enum

class ToolCategory(Enum):
    MARKET_DATA = "market_data"
    SYNC = "sync"
    ANALYTICS = "analytics"
    FIXED_INCOME = "fixed_income"
    VISUALIZATION = "visualization"

@dataclass
class Tool:
    name: str
    description: str
    function: Callable
    category: ToolCategory
    parameters: dict  # JSON Schema
    requires_confirmation: bool = False  # For destructive ops

class ToolRegistry:
    _tools: dict[str, Tool] = {}

    @classmethod
    def register(cls, tool: Tool):
        cls._tools[tool.name] = tool

    @classmethod
    def get_tool(cls, name: str) -> Tool:
        return cls._tools.get(name)

    @classmethod
    def get_tools_for_agent(cls, categories: list[ToolCategory]) -> list[Tool]:
        return [t for t in cls._tools.values() if t.category in categories]

    @classmethod
    def to_anthropic_tools(cls, categories: list[ToolCategory]) -> list[dict]:
        """Convert to Anthropic tool_use format."""
        tools = cls.get_tools_for_agent(categories)
        return [
            {
                "name": t.name,
                "description": t.description,
                "input_schema": t.parameters
            }
            for t in tools
        ]
```

#### 2.2 Market Data Tools
```python
# tools/market_tools.py
from tools.registry import Tool, ToolCategory, ToolRegistry
from psx_ohlcv.query import get_eod_df, get_symbols, get_sectors
from psx_ohlcv.analytics import compute_market_breadth, get_top_movers

@ToolRegistry.register
def get_stock_price(symbol: str, days: int = 30) -> dict:
    """
    Fetch historical OHLCV data for a PSX symbol.

    Args:
        symbol: PSX stock symbol (e.g., "HBL", "OGDC")
        days: Number of days of history (default: 30)

    Returns:
        Dict with price data including open, high, low, close, volume
    """
    df = get_eod_df(symbol, limit=days)
    if df.empty:
        return {"error": f"No data found for {symbol}"}

    latest = df.iloc[-1]
    return {
        "symbol": symbol,
        "latest_date": str(df.index[-1].date()),
        "latest_close": float(latest["close"]),
        "latest_volume": int(latest["volume"]),
        "period_high": float(df["high"].max()),
        "period_low": float(df["low"].min()),
        "period_return_pct": float((latest["close"] / df.iloc[0]["close"] - 1) * 100),
        "data_points": len(df),
        "history": df.tail(5).to_dict("records")  # Last 5 days
    }

TOOL_GET_STOCK_PRICE = Tool(
    name="get_stock_price",
    description="Fetch historical OHLCV (Open, High, Low, Close, Volume) data for a PSX stock symbol. Use this to get price history, recent performance, and trading data.",
    function=get_stock_price,
    category=ToolCategory.MARKET_DATA,
    parameters={
        "type": "object",
        "properties": {
            "symbol": {
                "type": "string",
                "description": "PSX stock symbol (e.g., HBL, OGDC, ENGRO)"
            },
            "days": {
                "type": "integer",
                "description": "Number of days of history to fetch",
                "default": 30
            }
        },
        "required": ["symbol"]
    }
)

@ToolRegistry.register
def get_market_overview() -> dict:
    """Get current market overview including breadth and top movers."""
    breadth = compute_market_breadth()
    gainers = get_top_movers("gainers", limit=5)
    losers = get_top_movers("losers", limit=5)
    volume = get_top_movers("volume", limit=5)

    return {
        "market_breadth": breadth,
        "top_gainers": gainers,
        "top_losers": losers,
        "top_volume": volume,
        "as_of": datetime.now().isoformat()
    }

@ToolRegistry.register
def get_sector_performance(sector: str = None) -> dict:
    """Get performance metrics for sectors or a specific sector."""
    sectors = get_sectors()
    if sector:
        sectors = [s for s in sectors if sector.lower() in s["sector_name"].lower()]

    return {
        "sectors": sectors,
        "count": len(sectors)
    }

@ToolRegistry.register
def search_symbols(query: str) -> list[dict]:
    """Search for symbols by name or symbol code."""
    all_symbols = get_symbols()
    query_lower = query.lower()
    matches = [
        s for s in all_symbols
        if query_lower in s["symbol"].lower() or
           query_lower in (s.get("name") or "").lower()
    ]
    return matches[:20]  # Limit results
```

#### 2.3 Sync Tools
```python
# tools/sync_tools.py
@ToolRegistry.register
def sync_market_data(
    symbols: list[str] = None,
    incremental: bool = True,
    include_indices: bool = True
) -> dict:
    """
    Synchronize EOD market data from PSX.

    Args:
        symbols: List of symbols to sync (None = all active)
        incremental: Only fetch new data since last sync
        include_indices: Include KSE-100, KSE-30, etc.

    Returns:
        Sync result with counts and any errors
    """
    from psx_ohlcv.sync import sync_all
    result = sync_all(
        symbols=symbols,
        incremental=incremental,
        include_indices=include_indices
    )
    return {
        "status": "completed",
        "symbols_synced": result.symbols_ok,
        "symbols_failed": result.symbols_failed,
        "rows_upserted": result.rows_upserted,
        "errors": result.errors[:5] if result.errors else []
    }

TOOL_SYNC_MARKET_DATA = Tool(
    name="sync_market_data",
    description="Synchronize EOD (End of Day) market data from Pakistan Stock Exchange. Use this when user asks to update data, refresh prices, or sync market information.",
    function=sync_market_data,
    category=ToolCategory.SYNC,
    parameters={...},
    requires_confirmation=True  # Requires user confirmation
)

@ToolRegistry.register
def check_data_freshness() -> dict:
    """Check when market data was last updated."""
    from psx_ohlcv.db import get_connection

    conn = get_connection()
    result = conn.execute("""
        SELECT
            MAX(date) as latest_eod,
            MAX(ingested_at) as last_sync,
            COUNT(DISTINCT symbol) as symbols_count
        FROM eod_ohlcv
    """).fetchone()

    return {
        "latest_eod_date": result["latest_eod"],
        "last_sync_time": result["last_sync"],
        "symbols_count": result["symbols_count"],
        "is_stale": _is_data_stale(result["latest_eod"])
    }
```

#### 2.4 Analytics Tools
```python
# tools/analytics_tools.py
@ToolRegistry.register
def compute_returns(
    symbol: str,
    periods: list[str] = ["1D", "1W", "1M", "3M", "1Y"]
) -> dict:
    """
    Compute returns for a symbol over multiple periods.

    Args:
        symbol: Stock symbol
        periods: List of periods (1D, 1W, 1M, 3M, 6M, 1Y, YTD)
    """
    from psx_ohlcv.analytics_phase1 import compute_returns_for_symbol
    return compute_returns_for_symbol(symbol, periods)

@ToolRegistry.register
def compute_volatility(symbol: str, window: int = 30) -> dict:
    """Compute annualized volatility for a symbol."""
    from psx_ohlcv.analytics_phase1 import compute_volatility
    return compute_volatility(symbol, window)

@ToolRegistry.register
def compare_stocks(symbols: list[str], metric: str = "return_1m") -> list[dict]:
    """Compare multiple stocks on a specific metric."""
    results = []
    for symbol in symbols:
        data = compute_returns(symbol)
        results.append({
            "symbol": symbol,
            "metric": metric,
            "value": data.get(metric)
        })
    return sorted(results, key=lambda x: x["value"] or 0, reverse=True)
```

#### 2.5 Fixed Income Tools
```python
# tools/fi_tools.py
@ToolRegistry.register
def get_sukuk_analytics(instrument_id: str = None) -> dict:
    """Get sukuk analytics including YTM, duration, convexity."""
    from psx_ohlcv.analytics_sukuk import get_sukuk_analytics
    return get_sukuk_analytics(instrument_id)

@ToolRegistry.register
def get_yield_curve(curve_type: str = "PIB", date: str = None) -> dict:
    """Get yield curve data for government securities."""
    from psx_ohlcv.analytics_fixed_income import get_yield_curve
    return get_yield_curve(curve_type, date)

@ToolRegistry.register
def compute_bond_metrics(
    face_value: float,
    coupon_rate: float,
    maturity_years: float,
    market_price: float,
    frequency: int = 2
) -> dict:
    """Compute YTM, duration, and convexity for a bond."""
    from psx_ohlcv.analytics_sukuk import compute_ytm, compute_duration

    ytm = compute_ytm(face_value, coupon_rate, maturity_years, market_price, frequency)
    duration = compute_duration(face_value, coupon_rate, maturity_years, ytm, frequency)

    return {
        "ytm": ytm,
        "macaulay_duration": duration["macaulay"],
        "modified_duration": duration["modified"],
        "convexity": duration["convexity"]
    }
```

---

### Phase 3: Agent Layer (Week 3-4)
**Goal:** Implement specialist agents with tool access.

#### 3.1 Base Agent
```python
# agents/base.py
from abc import ABC, abstractmethod
from typing import Generator
from anthropic import Anthropic
from tools.registry import ToolRegistry, ToolCategory

class BaseAgent(ABC):
    """Base class for all specialist agents."""

    def __init__(self, client: Anthropic = None):
        self.client = client or Anthropic()
        self.model = "claude-3-5-sonnet-20241022"
        self.conversation_history = []
        self.tool_categories: list[ToolCategory] = []

    @property
    @abstractmethod
    def system_prompt(self) -> str:
        """Agent-specific system prompt."""
        pass

    @property
    def tools(self) -> list[dict]:
        """Get tools available to this agent."""
        return ToolRegistry.to_anthropic_tools(self.tool_categories)

    def run(self, user_message: str) -> Generator[str, None, None]:
        """Run agent with streaming response."""
        self.conversation_history.append({
            "role": "user",
            "content": user_message
        })

        while True:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=4096,
                system=self.system_prompt,
                tools=self.tools,
                messages=self.conversation_history
            )

            # Handle tool use
            if response.stop_reason == "tool_use":
                tool_results = self._execute_tools(response.content)
                self.conversation_history.append({
                    "role": "assistant",
                    "content": response.content
                })
                self.conversation_history.append({
                    "role": "user",
                    "content": tool_results
                })
                continue

            # Extract text response
            assistant_message = self._extract_text(response.content)
            self.conversation_history.append({
                "role": "assistant",
                "content": assistant_message
            })

            yield assistant_message
            break

    def _execute_tools(self, content) -> list[dict]:
        """Execute tool calls and return results."""
        results = []
        for block in content:
            if block.type == "tool_use":
                tool = ToolRegistry.get_tool(block.name)
                if tool:
                    try:
                        result = tool.function(**block.input)
                        results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": json.dumps(result)
                        })
                    except Exception as e:
                        results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": f"Error: {str(e)}",
                            "is_error": True
                        })
        return results
```

#### 3.2 Market Agent
```python
# agents/market_agent.py
from agents.base import BaseAgent
from tools.registry import ToolCategory

class MarketAgent(BaseAgent):
    """Specialist agent for market analysis and equity research."""

    tool_categories = [
        ToolCategory.MARKET_DATA,
        ToolCategory.ANALYTICS,
        ToolCategory.VISUALIZATION
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

Available indices: KSE-100, KSE-30, KMI-30, KSE All Share.

If asked about a stock you don't have data for, clearly state that and suggest alternatives."""
```

#### 3.3 Fixed Income Agent
```python
# agents/fi_agent.py
class FixedIncomeAgent(BaseAgent):
    """Specialist agent for sukuk, bonds, and fixed income analysis."""

    tool_categories = [
        ToolCategory.FIXED_INCOME,
        ToolCategory.ANALYTICS
    ]

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
- SBP Policy Rate (current reference for short-term)
- KIBOR (3M, 6M, 12M) for interbank rates
- PIB yields for long-term government

When analyzing fixed income:
1. Always consider current SBP policy stance
2. Calculate proper YTM, duration, convexity
3. Compare spreads to benchmarks
4. Consider Islamic vs conventional instruments
5. Assess reinvestment and interest rate risk

Use ACT/365 day count for PKR instruments unless specified otherwise."""
```

#### 3.4 Sync Agent
```python
# agents/sync_agent.py
class SyncAgent(BaseAgent):
    """Agent for data synchronization operations."""

    tool_categories = [ToolCategory.SYNC]

    @property
    def system_prompt(self) -> str:
        return """You are a data operations specialist for the PSX OHLCV system.

Your responsibilities:
- Monitor data freshness and staleness
- Execute data synchronization jobs
- Report sync status and errors
- Recommend optimal sync strategies

Data sources you manage:
- PSX EOD data (daily OHLCV for ~540 stocks)
- PSX intraday data (1-minute bars during market hours)
- FX rates (USD/PKR, EUR/PKR, etc.)
- Mutual fund NAV (MUFAP data)
- Fixed income (sukuk, bonds from SBP)

Before running syncs:
1. Check current data freshness
2. Estimate sync duration (roughly 5-10 minutes for full EOD)
3. Confirm with user for large operations
4. Report results clearly

PSX market is closed on weekends and Pakistani holidays."""
```

#### 3.5 Orchestrator
```python
# agents/orchestrator.py
from typing import Generator
from anthropic import Anthropic
from agents.market_agent import MarketAgent
from agents.fi_agent import FixedIncomeAgent
from agents.sync_agent import SyncAgent
from agents.report_agent import ReportAgent

class AgentOrchestrator:
    """Routes user queries to appropriate specialist agents."""

    ROUTING_PROMPT = """Classify the user's intent into one of these categories:

1. MARKET - Stock prices, equity analysis, sector performance, technical analysis
2. FIXED_INCOME - Sukuk, bonds, yield curves, duration, interest rates
3. SYNC - Data updates, refresh data, check freshness, sync status
4. REPORT - Generate reports, summaries, comparisons, exports
5. GENERAL - Greetings, help, general questions about the system

Respond with ONLY the category name."""

    def __init__(self):
        self.client = Anthropic()
        self.agents = {
            "MARKET": MarketAgent(self.client),
            "FIXED_INCOME": FixedIncomeAgent(self.client),
            "SYNC": SyncAgent(self.client),
            "REPORT": ReportAgent(self.client),
        }
        self.current_agent = None
        self.conversation_context = []

    def route(self, user_message: str) -> str:
        """Determine which agent should handle the message."""
        response = self.client.messages.create(
            model="claude-3-haiku-20240307",  # Fast model for routing
            max_tokens=20,
            messages=[{
                "role": "user",
                "content": f"{self.ROUTING_PROMPT}\n\nUser message: {user_message}"
            }]
        )
        category = response.content[0].text.strip().upper()
        return category if category in self.agents else "MARKET"

    def process(self, user_message: str) -> Generator[str, None, None]:
        """Process user message through appropriate agent."""
        # Route to specialist
        category = self.route(user_message)

        # Get or switch agent
        if self.current_agent != category:
            self.current_agent = category
            # Transfer context if switching agents
            if self.conversation_context:
                self.agents[category].conversation_history = self.conversation_context.copy()

        # Run agent
        agent = self.agents[category]
        for response in agent.run(user_message):
            yield response

        # Update context
        self.conversation_context = agent.conversation_history.copy()
```

---

### Phase 4: Chat UI (Week 4-5)
**Goal:** Add conversational interface to Streamlit app.

#### 4.1 Chat Component
```python
# ui/chat.py
import streamlit as st
from agents.orchestrator import AgentOrchestrator

def render_chat():
    """Render the chat interface."""
    st.header("AI Market Assistant")

    # Initialize orchestrator
    if "orchestrator" not in st.session_state:
        st.session_state.orchestrator = AgentOrchestrator()
        st.session_state.messages = []

    # Display chat history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Chat input
    if prompt := st.chat_input("Ask about markets, stocks, or fixed income..."):
        # Add user message
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # Get agent response
        with st.chat_message("assistant"):
            message_placeholder = st.empty()
            full_response = ""

            for chunk in st.session_state.orchestrator.process(prompt):
                full_response = chunk
                message_placeholder.markdown(full_response + "...")

            message_placeholder.markdown(full_response)

        st.session_state.messages.append({"role": "assistant", "content": full_response})

def render_chat_sidebar():
    """Render chat controls in sidebar."""
    st.sidebar.subheader("Chat Controls")

    if st.sidebar.button("Clear Conversation"):
        st.session_state.messages = []
        st.session_state.orchestrator = AgentOrchestrator()
        st.rerun()

    st.sidebar.markdown("---")
    st.sidebar.markdown("**Quick Actions:**")

    quick_actions = [
        "What's the market doing today?",
        "Show me top gainers",
        "Analyze HBL stock",
        "What's the current yield curve?",
        "Is my data up to date?",
    ]

    for action in quick_actions:
        if st.sidebar.button(action, key=f"quick_{action}"):
            st.session_state.pending_message = action
            st.rerun()
```

#### 4.2 Integration with Existing App
```python
# ui/app.py modifications
from ui.chat import render_chat, render_chat_sidebar

# Add to navigation
pages = {
    "Dashboard": render_dashboard,
    "AI Assistant": render_chat,  # NEW
    "Candlestick": render_candlestick,
    # ... existing pages
}

# Add chat sidebar when on AI Assistant page
if selected_page == "AI Assistant":
    render_chat_sidebar()
```

---

### Phase 5: Memory & Context (Week 5-6)
**Goal:** Add persistent memory for personalized experience.

#### 5.1 User Context
```python
# memory/user_context.py
from dataclasses import dataclass, field
from typing import Optional
import json
from psx_ohlcv.db import get_connection

@dataclass
class UserContext:
    user_id: str
    watchlist: list[str] = field(default_factory=list)
    alerts: list[dict] = field(default_factory=list)
    preferences: dict = field(default_factory=dict)

    def save(self):
        conn = get_connection()
        conn.execute("""
            INSERT OR REPLACE INTO user_context
            (user_id, watchlist, alerts, preferences, updated_at)
            VALUES (?, ?, ?, ?, datetime('now'))
        """, (
            self.user_id,
            json.dumps(self.watchlist),
            json.dumps(self.alerts),
            json.dumps(self.preferences)
        ))
        conn.commit()

    @classmethod
    def load(cls, user_id: str) -> "UserContext":
        conn = get_connection()
        row = conn.execute(
            "SELECT * FROM user_context WHERE user_id = ?",
            (user_id,)
        ).fetchone()

        if row:
            return cls(
                user_id=user_id,
                watchlist=json.loads(row["watchlist"]),
                alerts=json.loads(row["alerts"]),
                preferences=json.loads(row["preferences"])
            )
        return cls(user_id=user_id)

    def add_to_watchlist(self, symbol: str):
        if symbol not in self.watchlist:
            self.watchlist.append(symbol)
            self.save()

    def set_alert(self, symbol: str, condition: str, value: float):
        alert = {
            "symbol": symbol,
            "condition": condition,  # "price_above", "price_below", "volume_above"
            "value": value,
            "created_at": datetime.now().isoformat(),
            "triggered": False
        }
        self.alerts.append(alert)
        self.save()
```

#### 5.2 Conversation Memory
```python
# memory/conversation.py
class ConversationMemory:
    """Manages conversation history with summarization."""

    def __init__(self, max_messages: int = 20):
        self.max_messages = max_messages
        self.messages: list[dict] = []
        self.summary: Optional[str] = None

    def add(self, role: str, content: str):
        self.messages.append({"role": role, "content": content})

        # Summarize if too long
        if len(self.messages) > self.max_messages:
            self._summarize()

    def _summarize(self):
        """Summarize older messages to save context."""
        # Keep last 10 messages, summarize the rest
        to_summarize = self.messages[:-10]
        self.messages = self.messages[-10:]

        # Use LLM to summarize
        summary_prompt = f"""Summarize this conversation concisely:
        {json.dumps(to_summarize)}

        Focus on: key questions asked, stocks discussed, decisions made."""

        # Get summary from LLM...
        self.summary = summary_text

    def get_context(self) -> list[dict]:
        """Get messages with summary prepended if exists."""
        if self.summary:
            return [
                {"role": "system", "content": f"Previous conversation summary: {self.summary}"}
            ] + self.messages
        return self.messages
```

---

### Phase 6: Advanced Features (Week 6-8)
**Goal:** Add proactive monitoring, alerts, and reporting.

#### 6.1 Alert Monitor Agent
```python
# agents/alert_agent.py
import asyncio
from datetime import datetime, timedelta

class AlertMonitor:
    """Background agent that monitors for alert conditions."""

    def __init__(self):
        self.running = False
        self.check_interval = 300  # 5 minutes

    async def start(self):
        self.running = True
        while self.running:
            await self._check_alerts()
            await asyncio.sleep(self.check_interval)

    async def _check_alerts(self):
        # Get all active alerts
        alerts = self._get_active_alerts()

        for alert in alerts:
            triggered = await self._evaluate_alert(alert)
            if triggered:
                await self._notify_user(alert)

    async def _evaluate_alert(self, alert: dict) -> bool:
        """Check if alert condition is met."""
        current_price = await self._get_current_price(alert["symbol"])

        if alert["condition"] == "price_above":
            return current_price > alert["value"]
        elif alert["condition"] == "price_below":
            return current_price < alert["value"]
        elif alert["condition"] == "volume_above":
            volume = await self._get_current_volume(alert["symbol"])
            return volume > alert["value"]

        return False
```

#### 6.2 Report Generation
```python
# agents/report_agent.py
class ReportAgent(BaseAgent):
    """Agent for generating reports and summaries."""

    tool_categories = [
        ToolCategory.MARKET_DATA,
        ToolCategory.ANALYTICS,
        ToolCategory.VISUALIZATION
    ]

    @property
    def system_prompt(self) -> str:
        return """You are a report generation specialist for PSX market data.

You can generate:
1. Daily market summaries
2. Stock comparison reports
3. Sector analysis reports
4. Portfolio performance summaries
5. Fixed income market updates

Format reports professionally with:
- Executive summary at the top
- Key metrics and numbers
- Charts and visualizations (describe what to show)
- Risk factors and disclaimers

Use markdown formatting for structure."""

    async def generate_daily_brief(self) -> str:
        """Generate morning market brief."""
        # Gather data
        market_data = await self.tools["get_market_overview"]()
        top_news = await self.tools["get_recent_announcements"]()

        # Generate narrative
        prompt = f"""Generate a professional morning market brief using this data:

Market Data: {json.dumps(market_data)}
Recent Announcements: {json.dumps(top_news)}

Format as a concise executive brief."""

        return await self._generate(prompt)
```

---

## Database Schema Additions

```sql
-- User context for personalization
CREATE TABLE user_context (
    user_id             TEXT PRIMARY KEY,
    watchlist           TEXT,           -- JSON array of symbols
    alerts              TEXT,           -- JSON array of alert configs
    preferences         TEXT,           -- JSON preferences
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Conversation history
CREATE TABLE conversation_history (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id             TEXT NOT NULL,
    session_id          TEXT NOT NULL,
    role                TEXT NOT NULL,  -- 'user' or 'assistant'
    content             TEXT NOT NULL,
    agent_type          TEXT,           -- Which agent handled this
    tool_calls          TEXT,           -- JSON of tool calls made
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),

    FOREIGN KEY (user_id) REFERENCES user_context(user_id)
);

-- Alert definitions
CREATE TABLE alerts (
    alert_id            TEXT PRIMARY KEY,
    user_id             TEXT NOT NULL,
    symbol              TEXT NOT NULL,
    condition           TEXT NOT NULL,  -- 'price_above', 'price_below', etc.
    threshold           REAL NOT NULL,
    is_active           INTEGER DEFAULT 1,
    triggered_at        TEXT,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),

    FOREIGN KEY (user_id) REFERENCES user_context(user_id)
);

-- Agent performance metrics
CREATE TABLE agent_metrics (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_type          TEXT NOT NULL,
    query_hash          TEXT NOT NULL,
    tool_calls_count    INTEGER,
    tokens_used         INTEGER,
    response_time_ms    INTEGER,
    user_feedback       INTEGER,        -- 1 = helpful, -1 = not helpful
    created_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_conversation_user ON conversation_history(user_id);
CREATE INDEX idx_conversation_session ON conversation_history(session_id);
CREATE INDEX idx_alerts_user ON alerts(user_id);
CREATE INDEX idx_alerts_active ON alerts(is_active);
```

---

## Testing Strategy

### Unit Tests
```python
# tests/test_tools.py
def test_get_stock_price():
    result = get_stock_price("HBL", days=30)
    assert "latest_close" in result
    assert "period_return_pct" in result

def test_market_overview():
    result = get_market_overview()
    assert "market_breadth" in result
    assert "top_gainers" in result

# tests/test_agents.py
def test_market_agent_routing():
    orchestrator = AgentOrchestrator()
    assert orchestrator.route("What's HBL price?") == "MARKET"
    assert orchestrator.route("Sync my data") == "SYNC"
    assert orchestrator.route("What's the yield curve?") == "FIXED_INCOME"
```

### Integration Tests
```python
# tests/test_integration.py
async def test_full_conversation():
    orchestrator = AgentOrchestrator()

    # Test multi-turn conversation
    response1 = await orchestrator.process("What's HBL trading at?")
    assert "HBL" in response1

    response2 = await orchestrator.process("Compare it to MCB")
    assert "MCB" in response2
    assert "HBL" in response2  # Should remember context
```

---

## Deployment Considerations

### Environment Variables
```bash
# Required
ANTHROPIC_API_KEY=sk-ant-...

# Optional
OPENAI_API_KEY=sk-...              # Fallback LLM
PSX_AGENTIC_MODEL=claude-3-5-sonnet-20241022
PSX_AGENTIC_TEMPERATURE=0.1
PSX_AGENTIC_MAX_TOKENS=4096
```

### Cost Estimation
| Component | Est. Tokens/Query | Cost/1000 Queries |
|-----------|------------------|-------------------|
| Router (Haiku) | ~100 | $0.025 |
| Agent (Sonnet) | ~2000 | $6.00 |
| Tool Results | ~500 | Included |
| **Total** | ~2600 | **~$6.03** |

### Performance Targets
- Query routing: < 500ms
- Simple queries (cached): < 2s
- Complex queries (multiple tools): < 10s
- Data sync: Background, non-blocking

---

## Success Metrics

1. **User Engagement**
   - Chat sessions per day
   - Messages per session
   - Feature adoption rate

2. **Quality**
   - User feedback scores
   - Tool execution success rate
   - Response accuracy (spot checks)

3. **Performance**
   - Average response time
   - Tool call latency
   - Cache hit rate

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| LLM hallucinations | Always use tools for data, verify numbers |
| API rate limits | Implement exponential backoff, caching |
| Cost overruns | Set daily token budgets, use Haiku for routing |
| Data staleness | Clear freshness indicators, auto-sync prompts |
| Security | No PII in prompts, sanitize tool inputs |

---

## Next Steps

1. **Immediate:** Review and approve this plan
2. **Week 1:** Set up project structure, add dependencies
3. **Week 2:** Implement tool registry and core tools
4. **Week 3:** Build Market Agent and Orchestrator
5. **Week 4:** Add chat UI to Streamlit
6. **Week 5:** Implement memory and context
7. **Week 6:** Add alerts and reporting
8. **Week 7-8:** Testing, optimization, documentation

---

## Appendix: Example Conversations

### Example 1: Market Analysis
```
User: How did banking stocks do this week?

Agent: [Calls get_sector_performance("banking")]
       [Calls get_stock_price for top 5 banking stocks]

Agent: The banking sector had a positive week with an average gain of 2.3%:

**Top Performers:**
- HBL: +4.2% (Rs. 148.50) - Strong quarterly results
- MCB: +2.8% (Rs. 185.20) - Dividend announcement
- UBL: +1.9% (Rs. 142.80)

**Underperformers:**
- ABL: -0.5% (Rs. 78.30)

The sector outperformed KSE-100 (+1.1%) this week. Banking index
volume was 23% above the 20-day average, indicating strong interest.

Would you like detailed analysis on any specific bank?
```

### Example 2: Fixed Income Query
```
User: What's the current PIB yield curve showing?

Agent: [Calls get_yield_curve("PIB")]

Agent: Here's the current PKR PIB yield curve as of Jan 30, 2026:

| Tenor | Yield | Change (1W) |
|-------|-------|-------------|
| 3Y    | 12.45%| -15 bps     |
| 5Y    | 12.80%| -10 bps     |
| 10Y   | 13.25%| -5 bps      |

**Analysis:**
- Curve has flattened slightly (10Y-3Y spread: 80 bps, down from 95 bps)
- Short-end rally reflects market expectations of SBP rate cuts
- 5Y PIBs currently offering best risk-adjusted returns

The curve suggests markets are pricing in 50-75 bps of rate cuts
over the next 6 months.

Want me to calculate duration for a specific PIB?
```

### Example 3: Data Sync Request
```
User: My data seems old, can you update it?

Agent: [Calls check_data_freshness()]

Agent: Let me check your data freshness...

**Current Data Status:**
- Latest EOD date: Jan 29, 2026
- Last sync: Jan 29, 2026 at 4:15 PM
- Symbols covered: 542

Your data is 1 day old. Today's market data should be available now
(it's 4:30 PM PKT).

Would you like me to run an incremental sync? This will:
- Fetch today's EOD data for all active symbols
- Update KSE-100, KSE-30, and sector indices
- Estimated time: 5-8 minutes

[Yes, sync now] [No, maybe later]
```

### Example 4: Multi-Turn Conversation
```
User: Add OGDC to my watchlist

Agent: [Calls add_to_watchlist("OGDC")]

Agent: Done! I've added OGDC (Oil & Gas Development Company) to your watchlist.

Your current watchlist:
1. HBL
2. ENGRO
3. OGDC (just added)

Would you like me to set up any price alerts for OGDC?

User: Yes, alert me if it goes above Rs. 140

Agent: [Calls set_alert("OGDC", "price_above", 140)]

Agent: Alert set! I'll notify you when OGDC crosses Rs. 140.

Current OGDC price: Rs. 132.50 (+1.2% today)
Distance to alert: Rs. 7.50 (5.7%)

Based on recent volatility, OGDC moves about Rs. 2-3 per day on average.
```

---

## Glossary

| Term | Definition |
|------|------------|
| **Agent** | LLM-powered component that can reason and use tools |
| **Tool** | Callable function that agents can invoke |
| **Orchestrator** | Routes user queries to appropriate specialist agents |
| **RAG** | Retrieval-Augmented Generation |
| **YTM** | Yield to Maturity |
| **Duration** | Bond price sensitivity to interest rate changes |
| **EOD** | End of Day |
| **OHLCV** | Open, High, Low, Close, Volume |
| **KSE-100** | Karachi Stock Exchange 100 Index |
| **PIB** | Pakistan Investment Bond |
| **SBP** | State Bank of Pakistan |
| **MUFAP** | Mutual Funds Association of Pakistan |