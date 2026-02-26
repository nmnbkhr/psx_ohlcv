"""
FX Module Client — Drop into PSX OHLCV App
============================================
Thin async client that connects your PSX OHLCV app to the FX microservice.

DROP this file into: pakfindata/sources/fx_client.py (or wherever your sources live)

Usage in your PSX OHLCV app:
    from sources.fx_client import FXClient

    fx = FXClient()  # Defaults to http://localhost:8100

    # Check if FX service is alive
    if fx.is_healthy():
        rates = fx.get_latest_rates()
        kibor = fx.get_kibor()
        snapshot = fx.get_snapshot()  # Everything in one call
        signals = fx.get_signal_report()
    else:
        # Graceful degradation — FX service is down, equity app keeps running
        logger.warning("FX service unavailable, skipping FX data")

Key Design:
    - Every method returns None/empty on failure (never crashes your app)
    - Health check is cached for 60 seconds
    - Timeout is short (5s) so your app doesn't hang
    - All methods are synchronous (easy to drop in) but use connection pooling
"""
import time
import logging
from typing import Optional
from functools import lru_cache

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger("fx_client")


class FXClient:
    """
    Resilient client for the FX Trading Module microservice.
    Designed for graceful degradation — if FX service is down,
    your PSX OHLCV app keeps running normally.
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8100",
        timeout: float = 5.0,
        retries: int = 2,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._session = self._create_session(retries)
        self._health_cache: Optional[bool] = None
        self._health_cache_time: float = 0
        self._health_cache_ttl: float = 60.0  # Cache health for 60s

    def _create_session(self, retries: int) -> requests.Session:
        """Create a session with retry logic and connection pooling."""
        session = requests.Session()
        retry = Retry(
            total=retries,
            backoff_factor=0.3,
            status_forcelist=[502, 503, 504],
        )
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=5,
            pool_maxsize=10,
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _get(self, endpoint: str, params: dict = None) -> Optional[dict]:
        """Safe GET request — returns None on any failure."""
        try:
            resp = self._session.get(
                f"{self.base_url}{endpoint}",
                params=params,
                timeout=self.timeout,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.debug(f"FX service request failed [{endpoint}]: {e}")
            return None

    def _post(self, endpoint: str, data: dict = None) -> Optional[dict]:
        """Safe POST request — returns None on any failure."""
        try:
            resp = self._session.post(
                f"{self.base_url}{endpoint}",
                json=data,
                timeout=self.timeout,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.debug(f"FX service POST failed [{endpoint}]: {e}")
            return None

    # ─── Health ──────────────────────────────────────────────────────

    def is_healthy(self) -> bool:
        """
        Check if FX service is alive. Cached for 60 seconds.
        Use this before calling other methods for fast-fail.
        """
        now = time.time()
        if self._health_cache is not None and (now - self._health_cache_time) < self._health_cache_ttl:
            return self._health_cache

        result = self._get("/health")
        healthy = result is not None and result.get("status") == "healthy"
        self._health_cache = healthy
        self._health_cache_time = now
        return healthy

    # ─── Snapshot (One Call Gets Everything) ──────────────────────────

    def get_snapshot(self) -> Optional[dict]:
        """
        Get complete FX market snapshot in one call.
        Returns: rates, KIBOR, signals summary.
        Best for dashboard sidebar widgets.

        Returns None if service is down.
        """
        return self._get("/snapshot")

    # ─── Rates ───────────────────────────────────────────────────────

    def get_latest_rates(self, source: str = None, pair: str = None) -> list[dict]:
        """Get latest FX rates. Returns empty list on failure."""
        params = {}
        if source:
            params["source"] = source
        if pair:
            params["pair"] = pair
        result = self._get("/rates/latest", params=params)
        return result.get("rates", []) if result else []

    def get_rate(self, pair: str, source: str = None) -> Optional[dict]:
        """Get rate for a specific pair. Returns None on failure."""
        pair_url = pair.replace("/", "-")
        params = {"source": source} if source else None
        result = self._get(f"/rates/{pair_url}", params=params)
        if result and result.get("rates"):
            return result["rates"][0]  # Latest rate
        return None

    def get_interbank_rates(self) -> list[dict]:
        """SBP interbank rates."""
        result = self._get("/rates/interbank")
        return result.get("rates", []) if result else []

    def get_open_market_rates(self) -> list[dict]:
        """Open market / kerb rates."""
        result = self._get("/rates/openmarket")
        return result.get("rates", []) if result else []

    # ─── OHLCV ───────────────────────────────────────────────────────

    def get_ohlcv(
        self,
        pair: str,
        timeframe: str = "1d",
        start_date: str = None,
        end_date: str = None,
        limit: int = 500,
    ) -> list[dict]:
        """Get OHLCV candles for an FX pair. Returns empty list on failure."""
        pair_url = pair.replace("/", "-")
        params = {"timeframe": timeframe, "limit": limit}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        result = self._get(f"/ohlcv/{pair_url}", params=params)
        return result.get("candles", []) if result else []

    def list_pairs(self) -> list[dict]:
        """List all available OHLCV pairs."""
        result = self._get("/ohlcv")
        return result.get("pairs", []) if result else []

    # ─── KIBOR ───────────────────────────────────────────────────────

    def get_kibor(self) -> Optional[dict]:
        """Latest KIBOR rates for all tenors."""
        return self._get("/kibor")

    def get_kibor_6m(self) -> Optional[float]:
        """Quick get KIBOR 6-month rate (most used)."""
        result = self._get("/kibor/6M")
        return result.get("mid") if result else None

    # ─── Signals ─────────────────────────────────────────────────────

    def get_signal_report(self, refresh: bool = False) -> Optional[dict]:
        """Full trading signal report."""
        params = {"refresh": str(refresh).lower()} if refresh else None
        return self._get("/signals/report", params=params)

    def get_carry_trade(self) -> Optional[dict]:
        """Carry trade signals."""
        return self._get("/signals/carry")

    def get_premium_spread(self) -> Optional[dict]:
        """Premium spread signals."""
        return self._get("/signals/premium")

    def get_intervention(self, pair: str = "USD/PKR") -> Optional[dict]:
        """SBP intervention detection."""
        return self._get("/signals/intervention", params={"pair": pair})

    def get_regime(self, pair: str = "USD/PKR") -> Optional[dict]:
        """FX-equity regime signal."""
        return self._get("/signals/regime", params={"pair": pair})

    def get_sector_guide(self) -> Optional[dict]:
        """Sector exposure guide."""
        return self._get("/signals/sectors")

    # ─── Collection Triggers ─────────────────────────────────────────

    def trigger_collection(self, collection_type: str = "all") -> Optional[dict]:
        """Trigger data collection on the FX service."""
        endpoint_map = {
            "all": "/collect",
            "pakistan": "/collect/pakistan",
            "international": "/collect/international",
        }
        endpoint = endpoint_map.get(collection_type, "/collect")
        return self._post(endpoint)

    # ─── Stats ───────────────────────────────────────────────────────

    def get_stats(self) -> Optional[dict]:
        """Get FX service database stats."""
        return self._get("/stats")


# ═══════════════════════════════════════════════════════════════════════
# EXAMPLE INTEGRATION PATTERNS FOR PSX OHLCV APP
# ═══════════════════════════════════════════════════════════════════════

"""
# ─── Pattern 1: Dashboard Sidebar Widget ─────────────────────────────
# In your Streamlit dashboard.py:

fx = FXClient()

with st.sidebar:
    st.subheader("💱 FX Snapshot")
    if fx.is_healthy():
        snap = fx.get_snapshot()
        if snap:
            for pair, data in snap.get("rates", {}).items():
                st.metric(pair, f"{data['mid']:.2f}")
            
            kibor = snap.get("kibor")
            if kibor:
                st.metric(f"KIBOR {kibor['tenor']}", f"{kibor['mid']:.2f}%")
            
            signals = snap.get("signals", {})
            if signals.get("assessment"):
                st.info(signals["assessment"])
    else:
        st.caption("FX service offline")


# ─── Pattern 2: Factor Analysis Enhancement ─────────────────────────
# In your qsresearch/sector_analysis.py:

fx = FXClient()

def get_fx_adjusted_factors(sector_factors: dict) -> dict:
    regime = fx.get_regime()
    if regime and regime.get("regime") == "pkr_weakening":
        # Boost export sector scores
        for sector in ["textiles", "it_services"]:
            if sector in sector_factors:
                sector_factors[sector]["fx_adjusted_score"] = (
                    sector_factors[sector]["score"] * 1.15
                )
    return sector_factors


# ─── Pattern 3: Sync Script Integration ─────────────────────────────
# In your scripts/daily_sync.sh:

# After equity sync, trigger FX collection
curl -X POST http://localhost:8100/collect
echo "FX collection triggered"


# ─── Pattern 4: Cross-Asset Correlation ──────────────────────────────
# In your analytics module:

fx = FXClient()

def get_fx_equity_correlation(equity_returns: pd.Series) -> float:
    fx_candles = fx.get_ohlcv("USD/PKR", timeframe="1d", limit=len(equity_returns))
    if not fx_candles:
        return None
    fx_closes = pd.Series([c["close"] for c in fx_candles])
    fx_returns = fx_closes.pct_change().dropna()
    return equity_returns.corr(fx_returns)
"""
