"""
Agentic AI Chat Interface for PakFinData Explorer.

This module provides a Streamlit chat interface for conversational
interaction with the AI agents (Market, Sync, FixedIncome).
"""

import os
import streamlit as st
from datetime import datetime


def init_chat_session():
    """Initialize chat session state."""
    if "chat_messages" not in st.session_state:
        st.session_state.chat_messages = []
    if "chat_orchestrator" not in st.session_state:
        st.session_state.chat_orchestrator = None
    if "chat_initialized" not in st.session_state:
        st.session_state.chat_initialized = False


def get_orchestrator():
    """Get or create the agent orchestrator."""
    if st.session_state.chat_orchestrator is None:
        try:
            from pakfindata.agents import AgentOrchestrator
            from pakfindata.agents.config import get_active_config

            config = get_active_config()
            st.session_state.chat_orchestrator = AgentOrchestrator(config=config)
            st.session_state.chat_initialized = True
        except Exception as e:
            st.error(f"Failed to initialize agents: {e}")
            return None
    return st.session_state.chat_orchestrator


def check_api_keys() -> tuple[bool, str]:
    """Check if required API keys are set.

    Returns:
        Tuple of (has_key, provider_name)
    """
    openai_key = os.environ.get("OPENAI_API_KEY", "").strip()
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()

    if openai_key:
        return True, "OpenAI"
    elif anthropic_key:
        return True, "Anthropic"
    return False, ""


def render_chat_header():
    """Render the chat page header."""
    st.markdown("""
    <style>
    /* Chat Header */
    .chat-header {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border-radius: 12px;
        padding: 20px;
        margin-bottom: 20px;
        border-left: 4px solid #ff9800;
    }
    .chat-header h2 {
        color: #ff9800;
        margin: 0;
        font-size: 1.5em;
    }
    .chat-header p {
        color: rgba(255, 255, 255, 0.7);
        margin: 8px 0 0 0;
        font-size: 0.9em;
    }

    /* Agent Badge */
    .agent-badge {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 16px;
        font-size: 0.75em;
        font-weight: 600;
        margin-right: 8px;
    }
    .agent-market { background: rgba(76, 175, 80, 0.2); color: #4caf50; }
    .agent-sync { background: rgba(33, 150, 243, 0.2); color: #2196f3; }
    .agent-fi { background: rgba(156, 39, 176, 0.2); color: #9c27b0; }

    /* Chat Messages */
    .chat-message {
        padding: 12px 16px;
        border-radius: 12px;
        margin: 8px 0;
        max-width: 85%;
    }
    .chat-user {
        background: rgba(255, 152, 0, 0.15);
        border: 1px solid rgba(255, 152, 0, 0.3);
        margin-left: auto;
    }
    .chat-assistant {
        background: rgba(255, 255, 255, 0.05);
        border: 1px solid rgba(255, 255, 255, 0.1);
    }

    /* Example Queries */
    .example-query {
        background: rgba(255, 255, 255, 0.03);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 8px;
        padding: 8px 12px;
        cursor: pointer;
        transition: all 0.2s;
        font-size: 0.85em;
    }
    .example-query:hover {
        background: rgba(255, 152, 0, 0.1);
        border-color: rgba(255, 152, 0, 0.3);
    }
    </style>
    """, unsafe_allow_html=True)


def render_agent_status(orchestrator):
    """Render the current agent status."""
    if orchestrator is None:
        return

    summary = orchestrator.get_agent_summary()
    config = orchestrator.get_config_summary()

    cols = st.columns(4)

    with cols[0]:
        st.metric("Provider", config.get("primary_provider", "N/A").upper())

    with cols[1]:
        st.metric("Model", config.get("agent_model", "N/A").split("-")[0])

    with cols[2]:
        current = orchestrator.get_current_agent()
        st.metric("Active Agent", current or "None")

    with cols[3]:
        total_tools = sum(a.get("tools", 0) for a in summary.values())
        st.metric("Tools Available", total_tools)


def render_example_queries():
    """Render example query buttons."""
    st.markdown("##### 💡 Try asking:")

    examples = [
        ("📈 Market", "What's the current market overview?"),
        ("🏢 Stock", "Get me the price and details for OGDC"),
        ("📊 Compare", "Compare HBL, MCB, and UBL performance"),
        ("📉 Technical", "Show technical indicators for ENGRO"),
        ("🔄 Sync", "Check if my data is up to date"),
        ("💰 Fixed Income", "What are the current yield curve rates?"),
    ]

    cols = st.columns(3)
    for i, (label, query) in enumerate(examples):
        with cols[i % 3]:
            if st.button(f"{label}", key=f"example_{i}", use_container_width=True):
                return query
    return None


def process_message(orchestrator, message: str) -> str:
    """Process a message through the orchestrator."""
    try:
        response = orchestrator.process(message)
        return response
    except Exception as e:
        return f"Error processing message: {str(e)}"


def render_chat_interface():
    """Render the main chat interface."""
    init_chat_session()
    render_chat_header()

    # Header
    st.markdown("## 💬 AI Chat")
    st.caption("Chat naturally with AI agents about stocks, market data, and fixed income")

    st.markdown("---")

    # Check API keys
    has_key, provider = check_api_keys()

    if not has_key:
        st.warning(
            "**API Key Not Configured**\n\n"
            "Set either `OPENAI_API_KEY` or `ANTHROPIC_API_KEY` environment variable:\n"
            "```bash\n"
            "export OPENAI_API_KEY='your-key'  # For GPT-5.2\n"
            "# or\n"
            "export ANTHROPIC_API_KEY='your-key'  # For Claude\n"
            "```"
        )
        return

    # Initialize orchestrator
    orchestrator = get_orchestrator()

    if orchestrator is None:
        st.error("Failed to initialize AI agents. Check your configuration.")
        return

    # Agent status bar
    with st.expander("🤖 Agent Status", expanded=False):
        render_agent_status(orchestrator)

    # Example queries
    example_query = render_example_queries()

    st.markdown("---")

    # Chat history container
    chat_container = st.container()

    # Display chat history
    with chat_container:
        for msg in st.session_state.chat_messages:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])
                if msg.get("agent"):
                    st.caption(f"🤖 {msg['agent']} Agent")

    # Chat input
    user_input = st.chat_input("Ask about stocks, market data, or fixed income...")

    # Handle example query click
    if example_query:
        user_input = example_query

    if user_input:
        # Add user message
        st.session_state.chat_messages.append({
            "role": "user",
            "content": user_input,
            "timestamp": datetime.now().isoformat(),
        })

        # Display user message
        with chat_container:
            with st.chat_message("user"):
                st.markdown(user_input)

        # Process with agent
        with chat_container:
            with st.chat_message("assistant"):
                with st.spinner("Thinking..."):
                    response = process_message(orchestrator, user_input)

                st.markdown(response)

                # Show which agent handled the query
                current_agent = orchestrator.get_current_agent()
                if current_agent:
                    st.caption(f"🤖 {current_agent} Agent")

        # Save assistant response
        st.session_state.chat_messages.append({
            "role": "assistant",
            "content": response,
            "agent": orchestrator.get_current_agent(),
            "timestamp": datetime.now().isoformat(),
        })

        # Rerun to update the chat display
        st.rerun()

    # Sidebar controls
    with st.sidebar:
        st.markdown("---")
        st.markdown("### 💬 Chat Controls")

        if st.button("🗑️ Clear Chat", use_container_width=True):
            st.session_state.chat_messages = []
            if orchestrator:
                orchestrator.clear_context()
            st.rerun()

        if st.button("🔄 Reset Agents", use_container_width=True):
            st.session_state.chat_orchestrator = None
            st.session_state.chat_initialized = False
            st.rerun()

        # Show message count
        msg_count = len(st.session_state.chat_messages)
        st.caption(f"Messages: {msg_count}")


def chat_page():
    """Main entry point for the chat page."""
    render_chat_interface()
