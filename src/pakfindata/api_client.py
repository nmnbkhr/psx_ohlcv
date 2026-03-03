"""PSX API client with direct-DB fallback.

Provides a unified interface for Streamlit pages to access data,
either through the FastAPI HTTP endpoints or directly from SQLite.

Usage:
    from pakfindata.api_client import get_client

    client = get_client()  # Auto-detects: API if running, else direct DB
    symbols = client.get_symbols()
    kse100 = client.get_market_indices()
"""

from __future__ import annotations

import logging
import os
import sqlite3
from typing import Any, Optional

import pandas as pd

logger = logging.getLogger("pakfindata")

# Environment variable to force API mode
_ENV_API_URL = "PSX_API_URL"
_DEFAULT_API_URL = "http://localhost:8000"


class PSXClient:
    """Dual-mode client: HTTP API or direct SQLite."""

    def __init__(self, base_url: str | None = None):
        """Initialize client.

        Args:
            base_url: API URL. If provided, uses HTTP mode.
                      If None, uses direct DB mode.
        """
        if base_url:
            self._mode = "api"
            self._base_url = base_url.rstrip("/")
            self._con = None
            try:
                import httpx
                self._http = httpx.Client(
                    base_url=self._base_url,
                    timeout=30.0,
                    follow_redirects=True,
                )
            except ImportError:
                raise ImportError("httpx is required for API mode: pip install httpx")
        else:
            self._mode = "direct"
            self._http = None
            self._base_url = None
            self._con = self._get_db_connection()

    def _get_db_connection(self) -> sqlite3.Connection:
        """Get a direct SQLite connection."""
        from pakfindata.config import get_db_path
        from pakfindata import init_schema

        db_path = get_db_path()
        con = sqlite3.connect(str(db_path), check_same_thread=False)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
        init_schema(con)
        return con

    @property
    def connection(self) -> sqlite3.Connection | None:
        """Direct DB connection (only in direct mode)."""
        return self._con

    @property
    def mode(self) -> str:
        """Current mode: 'api' or 'direct'."""
        return self._mode

    def close(self):
        """Close resources."""
        if self._http:
            self._http.close()
            self._http = None
        # Don't close DB connection — it may be shared

    # =========================================================================
    # SYMBOLS
    # =========================================================================

    def get_symbols(self, active_only: bool = True, limit: int | None = None) -> list[str]:
        """Get list of symbol strings."""
        if self._mode == "api":
            params = {"active": active_only}
            if limit:
                params["limit"] = limit
            resp = self._http.get("/api/symbols/", params=params)
            resp.raise_for_status()
            return resp.json().get("symbols", [])
        else:
            from pakfindata.query import get_symbols_list
            return get_symbols_list(self._con, limit=limit)

    def get_symbols_with_profiles(self) -> list[dict]:
        """Get symbols that have company profile data."""
        if self._mode == "api":
            # No specific endpoint — fall back to symbols list
            return self.get_symbols()
        else:
            from pakfindata.query import get_symbols_with_profiles
            return get_symbols_with_profiles(self._con)

    def get_symbol_detail(self, symbol: str) -> dict:
        """Get detail for a specific symbol."""
        if self._mode == "api":
            resp = self._http.get(f"/api/symbols/{symbol.upper()}")
            resp.raise_for_status()
            return resp.json()
        else:
            row = self._con.execute(
                "SELECT * FROM symbols WHERE symbol = ?", (symbol.upper(),)
            ).fetchone()
            return dict(row) if row else {}

    def get_sectors(self) -> dict[str, str]:
        """Get sector code → name mapping."""
        if self._mode == "api":
            resp = self._http.get("/api/symbols/sectors")
            resp.raise_for_status()
            return resp.json().get("sectors", {})
        else:
            from pakfindata.db import get_sector_map
            return get_sector_map(self._con)

    # =========================================================================
    # MARKET / INDICES
    # =========================================================================

    def get_market_indices(self) -> list[dict]:
        """Get latest values for all indices."""
        if self._mode == "api":
            resp = self._http.get("/api/market/indices")
            resp.raise_for_status()
            return resp.json().get("indices", [])
        else:
            from pakfindata.db import get_all_latest_indices
            return get_all_latest_indices(self._con)

    def get_latest_kse100(self) -> dict | None:
        """Get latest KSE-100 index data."""
        if self._mode == "api":
            resp = self._http.get("/api/market/indices/KSE100")
            resp.raise_for_status()
            data = resp.json()
            return data.get("latest")
        else:
            from pakfindata.db import get_latest_kse100
            return get_latest_kse100(self._con)

    def get_index_detail(self, index_code: str, days: int = 30) -> dict:
        """Get index latest value and history."""
        if self._mode == "api":
            resp = self._http.get(
                f"/api/market/indices/{index_code.upper()}",
                params={"days": days},
            )
            resp.raise_for_status()
            return resp.json()
        else:
            from pakfindata.db import get_latest_index, get_index_history
            latest = get_latest_index(self._con, index_code.upper())
            history = get_index_history(self._con, index_code.upper(), days=days)
            return {"latest": latest, "history": history}

    def get_market_breadth(self) -> dict:
        """Get market breadth: gainers, losers, unchanged."""
        if self._mode == "api":
            resp = self._http.get("/api/market/breadth")
            resp.raise_for_status()
            return resp.json()
        else:
            try:
                row = self._con.execute("""
                    SELECT
                        COUNT(CASE WHEN change_pct > 0 THEN 1 END) as gainers,
                        COUNT(CASE WHEN change_pct < 0 THEN 1 END) as losers,
                        COUNT(CASE WHEN change_pct = 0 OR change_pct IS NULL THEN 1 END) as unchanged,
                        COUNT(*) as total
                    FROM regular_market_current
                """).fetchone()
                return dict(row) if row else {"gainers": 0, "losers": 0, "unchanged": 0, "total": 0}
            except Exception:
                return {"gainers": 0, "losers": 0, "unchanged": 0, "total": 0}

    def get_live_market(self, limit: int = 50) -> list[dict]:
        """Get current regular market data."""
        if self._mode == "api":
            resp = self._http.get("/api/market/live", params={"limit": limit})
            resp.raise_for_status()
            return resp.json().get("data", [])
        else:
            try:
                rows = self._con.execute(
                    "SELECT * FROM regular_market_current ORDER BY symbol LIMIT ?",
                    (limit,),
                ).fetchall()
                return [dict(r) for r in rows]
            except Exception:
                return []

    def get_market_stats(self) -> dict:
        """Get latest market statistics."""
        if self._mode == "api":
            resp = self._http.get("/api/market/stats")
            resp.raise_for_status()
            return resp.json()
        else:
            from pakfindata.db import get_latest_market_stats
            stats = get_latest_market_stats(self._con)
            return stats if stats else {}

    # =========================================================================
    # EOD DATA
    # =========================================================================

    def get_eod_stats(self) -> dict:
        """Get EOD data statistics."""
        if self._mode == "api":
            resp = self._http.get("/api/eod/stats")
            resp.raise_for_status()
            return resp.json()
        else:
            row = self._con.execute("""
                SELECT COUNT(*) as total_rows,
                       COUNT(DISTINCT date) as total_dates,
                       COUNT(DISTINCT symbol) as total_symbols,
                       MIN(date) as min_date,
                       MAX(date) as max_date
                FROM eod_ohlcv
            """).fetchone()
            return dict(row) if row else {}

    def get_ohlcv_range(
        self,
        symbol: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """Get OHLCV data for a symbol within a date range.

        Returns DataFrame with columns: date, open, high, low, close, volume.
        """
        if self._mode == "api":
            params = {}
            if start_date:
                params["start"] = start_date
            if end_date:
                params["end"] = end_date
            resp = self._http.get(f"/api/company/{symbol.upper()}/quotes", params=params)
            resp.raise_for_status()
            data = resp.json().get("data", [])
            if data:
                return pd.DataFrame(data)
            return pd.DataFrame()
        else:
            from pakfindata.query import get_ohlcv_range
            return get_ohlcv_range(self._con, symbol, start_date=start_date, end_date=end_date)

    # =========================================================================
    # COMPANY DATA
    # =========================================================================

    def get_company_overview(self, symbol: str) -> dict | None:
        """Get unified company data."""
        if self._mode == "api":
            resp = self._http.get(f"/api/company/{symbol.upper()}")
            resp.raise_for_status()
            data = resp.json()
            if "error" in data:
                return None
            return data
        else:
            from pakfindata.db import get_company_unified
            return get_company_unified(self._con, symbol.upper())

    def get_company_profile(self, symbol: str) -> dict | None:
        """Get company profile."""
        if self._mode == "api":
            resp = self._http.get(f"/api/company/{symbol.upper()}/profile")
            resp.raise_for_status()
            data = resp.json()
            if "error" in data:
                return None
            return data
        else:
            from pakfindata.db import get_company_profile
            return get_company_profile(self._con, symbol.upper())

    def get_company_fundamentals(self, symbol: str) -> dict | None:
        """Get company fundamentals."""
        if self._mode == "api":
            resp = self._http.get(f"/api/company/{symbol.upper()}/fundamentals")
            resp.raise_for_status()
            data = resp.json()
            if "error" in data:
                return None
            return data
        else:
            from pakfindata.db import get_company_fundamentals
            return get_company_fundamentals(self._con, symbol.upper())

    def get_company_financials(
        self, symbol: str, period_type: str | None = None, limit: int = 20
    ) -> pd.DataFrame:
        """Get company financial statements."""
        if self._mode == "api":
            params: dict[str, Any] = {"limit": limit}
            if period_type:
                params["period_type"] = period_type
            resp = self._http.get(
                f"/api/company/{symbol.upper()}/financials", params=params
            )
            resp.raise_for_status()
            data = resp.json().get("data", [])
            return pd.DataFrame(data) if data else pd.DataFrame()
        else:
            from pakfindata.db import get_company_financials
            return get_company_financials(self._con, symbol.upper(), period_type=period_type, limit=limit)

    def get_company_quotes(self, symbol: str, limit: int = 100) -> pd.DataFrame:
        """Get historical quote snapshots."""
        if self._mode == "api":
            resp = self._http.get(
                f"/api/company/{symbol.upper()}/quotes",
                params={"limit": limit},
            )
            resp.raise_for_status()
            data = resp.json().get("data", [])
            return pd.DataFrame(data) if data else pd.DataFrame()
        else:
            from pakfindata.query import get_company_quotes
            return get_company_quotes(self._con, symbol, limit=limit)

    def get_company_ratios(self, symbol: str) -> pd.DataFrame:
        """Get company financial ratios."""
        if self._mode == "api":
            # No dedicated endpoint — use fundamentals
            data = self.get_company_fundamentals(symbol)
            return pd.DataFrame([data]) if data else pd.DataFrame()
        else:
            from pakfindata.db import get_company_ratios
            return get_company_ratios(self._con, symbol.upper())

    def get_company_payouts(self, symbol: str) -> pd.DataFrame:
        """Get dividend/payout history."""
        if self._mode == "api":
            # No dedicated endpoint — fall back to direct DB
            if self._con is None:
                self._con = self._get_db_connection()
            from pakfindata.db import get_company_payouts
            return get_company_payouts(self._con, symbol.upper())
        else:
            from pakfindata.db import get_company_payouts
            return get_company_payouts(self._con, symbol.upper())

    def get_company_latest_signals(self, symbol: str) -> dict:
        """Get latest trading signals for a symbol."""
        # No API endpoint — always direct
        if self._con is None:
            self._con = self._get_db_connection()
        from pakfindata.query import get_company_latest_signals
        return get_company_latest_signals(self._con, symbol)

    def get_company_quote_stats(self, symbol: str) -> dict:
        """Get quote statistics for a symbol."""
        # No API endpoint — always direct
        if self._con is None:
            self._con = self._get_db_connection()
        from pakfindata.query import get_company_quote_stats
        return get_company_quote_stats(self._con, symbol)

    # =========================================================================
    # ANALYTICS (direct DB only — no API endpoints for these)
    # =========================================================================

    def get_latest_market_analytics(self) -> dict | None:
        """Get pre-computed market analytics."""
        if self._con is None:
            self._con = self._get_db_connection()
        from pakfindata.analytics import get_latest_market_analytics
        return get_latest_market_analytics(self._con)

    def get_top_list(self, list_type: str, limit: int = 5) -> pd.DataFrame:
        """Get top gainers or losers."""
        if self._con is None:
            self._con = self._get_db_connection()
        from pakfindata.analytics import get_top_list
        return get_top_list(self._con, list_type, limit=limit)

    def get_sector_leaderboard(self) -> pd.DataFrame:
        """Get sector performance leaderboard."""
        if self._con is None:
            self._con = self._get_db_connection()
        from pakfindata.analytics import get_sector_leaderboard
        return get_sector_leaderboard(self._con)

    def init_analytics(self):
        """Initialize analytics schema."""
        if self._con is None:
            self._con = self._get_db_connection()
        from pakfindata.analytics import init_analytics_schema
        init_analytics_schema(self._con)

    # =========================================================================
    # SYNC / TASKS
    # =========================================================================

    def get_sync_runs(self, limit: int = 10) -> pd.DataFrame:
        """Get recent sync runs."""
        if self._mode == "api":
            resp = self._http.get("/api/tasks/list", params={"limit": limit})
            resp.raise_for_status()
            data = resp.json().get("tasks", [])
            return pd.DataFrame(data) if data else pd.DataFrame()
        else:
            return pd.read_sql_query(
                """SELECT run_id, started_at, ended_at, mode,
                          symbols_total, symbols_ok, symbols_failed, rows_upserted
                   FROM sync_runs ORDER BY started_at DESC LIMIT ?""",
                self._con,
                params=(limit,),
            )

    # =========================================================================
    # DATA FRESHNESS (direct DB only)
    # =========================================================================

    def get_data_freshness(self) -> tuple[int | None, str | None]:
        """Get data freshness: (days_old, latest_date_str)."""
        if self._con is None:
            self._con = self._get_db_connection()
        from pakfindata.ui.components.helpers import get_data_freshness
        return get_data_freshness(self._con)

    # =========================================================================
    # RAW SQL PASS-THROUGH (direct DB only)
    # =========================================================================

    def execute_sql(self, query: str, params: tuple = ()) -> list[dict]:
        """Execute raw SQL and return results as list of dicts.

        Only works in direct mode. For complex dashboard queries
        that have no API endpoint.
        """
        if self._con is None:
            self._con = self._get_db_connection()
        rows = self._con.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    def read_sql(self, query: str, params: tuple = ()) -> pd.DataFrame:
        """Execute SQL and return as DataFrame.

        Only works in direct mode.
        """
        if self._con is None:
            self._con = self._get_db_connection()
        return pd.read_sql_query(query, self._con, params=params)


# =========================================================================
# SINGLETON / FACTORY
# =========================================================================

_client_instance: PSXClient | None = None


def get_client() -> PSXClient:
    """Get or create a PSXClient singleton.

    Checks PSX_API_URL env var. If set, uses API mode.
    Otherwise uses direct DB mode.
    """
    global _client_instance
    if _client_instance is None:
        api_url = os.environ.get(_ENV_API_URL)
        if api_url:
            logger.info("PSXClient: API mode (%s)", api_url)
            _client_instance = PSXClient(base_url=api_url)
        else:
            logger.info("PSXClient: direct DB mode")
            _client_instance = PSXClient(base_url=None)
    return _client_instance


def reset_client():
    """Reset the singleton (for testing or reconfiguration)."""
    global _client_instance
    if _client_instance:
        _client_instance.close()
    _client_instance = None
