"""Session Interaction Tracker for PSX OHLCV UI.

Tracks user interactions both in Streamlit session state and persists to database.
"""

import uuid
from datetime import datetime
from typing import Any

import streamlit as st


def get_session_id() -> str:
    """Get or create a unique session ID."""
    if "session_id" not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())[:8]
    return st.session_state.session_id


def init_session_tracking():
    """Initialize session tracking state."""
    if "interactions" not in st.session_state:
        st.session_state.interactions = []
    if "session_start" not in st.session_state:
        st.session_state.session_start = datetime.now().isoformat()
    if "page_visits" not in st.session_state:
        st.session_state.page_visits = {}
    if "symbol_searches" not in st.session_state:
        st.session_state.symbol_searches = []


def _add_to_session(interaction: dict):
    """Add interaction to session state list."""
    init_session_tracking()
    st.session_state.interactions.append(interaction)
    # Keep only last 100 interactions in session state
    if len(st.session_state.interactions) > 100:
        st.session_state.interactions = st.session_state.interactions[-100:]


def track_page_visit(con, page_name: str):
    """Track a page visit.

    Args:
        con: Database connection
        page_name: Name of the page being visited
    """
    from pakfindata.db import log_interaction

    session_id = get_session_id()
    init_session_tracking()

    # Track in session state
    if page_name not in st.session_state.page_visits:
        st.session_state.page_visits[page_name] = 0
    st.session_state.page_visits[page_name] += 1

    interaction = {
        "timestamp": datetime.now().isoformat(),
        "action_type": "page_visit",
        "page_name": page_name,
    }
    _add_to_session(interaction)

    # Persist to database
    try:
        log_interaction(
            con,
            session_id=session_id,
            action_type="page_visit",
            page_name=page_name,
        )
    except Exception:
        pass  # Don't break the app if tracking fails


def track_symbol_search(con, symbol: str, page_name: str | None = None):
    """Track a symbol search/view.

    Args:
        con: Database connection
        symbol: Stock symbol searched
        page_name: Page where search occurred
    """
    from pakfindata.db import log_interaction

    session_id = get_session_id()
    init_session_tracking()

    # Track in session state
    st.session_state.symbol_searches.append({
        "symbol": symbol.upper(),
        "timestamp": datetime.now().isoformat(),
    })
    # Keep only last 50 searches
    if len(st.session_state.symbol_searches) > 50:
        st.session_state.symbol_searches = st.session_state.symbol_searches[-50:]

    interaction = {
        "timestamp": datetime.now().isoformat(),
        "action_type": "search",
        "symbol": symbol.upper(),
        "page_name": page_name,
    }
    _add_to_session(interaction)

    # Persist to database
    try:
        log_interaction(
            con,
            session_id=session_id,
            action_type="search",
            page_name=page_name,
            symbol=symbol,
            action_detail=f"Symbol search: {symbol.upper()}",
        )
    except Exception:
        pass


def track_button_click(
    con,
    button_name: str,
    page_name: str | None = None,
    symbol: str | None = None,
    metadata: dict | None = None,
):
    """Track a button click.

    Args:
        con: Database connection
        button_name: Name/label of the button clicked
        page_name: Page where click occurred
        symbol: Related stock symbol if applicable
        metadata: Additional context data
    """
    from pakfindata.db import log_interaction

    session_id = get_session_id()

    interaction = {
        "timestamp": datetime.now().isoformat(),
        "action_type": "button_click",
        "action_detail": button_name,
        "page_name": page_name,
        "symbol": symbol.upper() if symbol else None,
    }
    _add_to_session(interaction)

    # Persist to database
    try:
        log_interaction(
            con,
            session_id=session_id,
            action_type="button_click",
            page_name=page_name,
            symbol=symbol,
            action_detail=button_name,
            metadata=metadata,
        )
    except Exception:
        pass


def track_refresh(
    con,
    refresh_type: str,
    symbol: str | None = None,
    page_name: str | None = None,
    success: bool = True,
    metadata: dict | None = None,
):
    """Track a data refresh action.

    Args:
        con: Database connection
        refresh_type: Type of refresh ('profile', 'snapshot', 'download', etc.)
        symbol: Related stock symbol if applicable
        page_name: Page where refresh occurred
        success: Whether refresh succeeded
        metadata: Additional context data
    """
    from pakfindata.db import log_interaction

    session_id = get_session_id()

    meta = metadata or {}
    meta["success"] = success

    interaction = {
        "timestamp": datetime.now().isoformat(),
        "action_type": "refresh",
        "action_detail": refresh_type,
        "page_name": page_name,
        "symbol": symbol.upper() if symbol else None,
        "metadata": meta,
    }
    _add_to_session(interaction)

    # Persist to database
    try:
        log_interaction(
            con,
            session_id=session_id,
            action_type="refresh",
            page_name=page_name,
            symbol=symbol,
            action_detail=refresh_type,
            metadata=meta,
        )
    except Exception:
        pass


def track_download(
    con,
    download_type: str,
    details: str | None = None,
    page_name: str | None = None,
    metadata: dict | None = None,
):
    """Track a download action.

    Args:
        con: Database connection
        download_type: Type of download ('market_summary', 'export', etc.)
        details: Additional details about the download
        page_name: Page where download occurred
        metadata: Additional context data
    """
    from pakfindata.db import log_interaction

    session_id = get_session_id()

    interaction = {
        "timestamp": datetime.now().isoformat(),
        "action_type": "download",
        "action_detail": f"{download_type}: {details}" if details else download_type,
        "page_name": page_name,
    }
    _add_to_session(interaction)

    # Persist to database
    try:
        log_interaction(
            con,
            session_id=session_id,
            action_type="download",
            page_name=page_name,
            action_detail=f"{download_type}: {details}" if details else download_type,
            metadata=metadata,
        )
    except Exception:
        pass


def get_session_summary() -> dict[str, Any]:
    """Get summary of current session activity.

    Returns:
        Dict with session summary stats
    """
    init_session_tracking()

    return {
        "session_id": get_session_id(),
        "session_start": st.session_state.get("session_start"),
        "total_interactions": len(st.session_state.interactions),
        "page_visits": st.session_state.page_visits.copy(),
        "recent_symbols": [
            s["symbol"] for s in st.session_state.symbol_searches[-10:]
        ],
        "interactions": st.session_state.interactions[-20:],  # Last 20 interactions
    }


def render_session_activity_panel():
    """Render a panel showing current session activity."""
    summary = get_session_summary()

    with st.expander("📊 Session Activity", expanded=False):
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Session ID", summary["session_id"])

        with col2:
            st.metric("Total Actions", summary["total_interactions"])

        with col3:
            if summary["session_start"]:
                start = datetime.fromisoformat(summary["session_start"])
                duration = datetime.now() - start
                mins = int(duration.total_seconds() // 60)
                st.metric("Duration", f"{mins} min")

        # Recent symbols
        if summary["recent_symbols"]:
            st.markdown("**Recent Symbols:**")
            st.write(", ".join(summary["recent_symbols"][-5:]))

        # Page visit counts
        if summary["page_visits"]:
            st.markdown("**Page Visits:**")
            for page, count in sorted(
                summary["page_visits"].items(),
                key=lambda x: x[1],
                reverse=True,
            )[:5]:
                st.caption(f"{page}: {count}")
