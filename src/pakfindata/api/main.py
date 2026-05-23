"""pakfindata FastAPI service.

Run via systemd (preferred): the `pakfindata-api.service` user unit
is at `deploy/systemd/pakfindata-api.service`.

Run via uvicorn directly (dev):
    uvicorn pakfindata.api.main:app --host 127.0.0.1 --port 8001

Responsibilities of this module:
- App factory + lifespan hook (logging configuration)
- Global Bearer-auth middleware (skips /health, /docs, /openapi.json)
- CORS — tight; only the local Streamlit on 8501 is allowed
- Register the new-style `routes/` package AND the legacy
  `routers/` package (16 pre-Phase-1 routers; gradually migrated)

NOT this module's job:
- Business logic — that lives in `routes/` and `routers/`
- DB pool / sessions — Phase 1.2 adds `pakfindata.api.deps`
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from pakfindata.api.auth import BearerAuthMiddleware
from pakfindata.api.config import get_settings
from pakfindata.api.logging import configure_logging
from pakfindata.api.routes import admin as admin_route
from pakfindata.api.routes import commodities as commodities_route
from pakfindata.api.routes import eod as eod_route
from pakfindata.api.routes import equities as equities_route
from pakfindata.api.routes import fixed_income as fi_route
from pakfindata.api.routes import freshness as freshness_route
from pakfindata.api.routes import funds as funds_route
from pakfindata.api.routes import futures as futures_route
from pakfindata.api.routes import fx as fx_route
from pakfindata.api.routes import health as health_route
from pakfindata.api.routes import indices as indices_route
from pakfindata.api.routes import intraday as intraday_route
from pakfindata.api.routes import jobs as jobs_route
from pakfindata.api.routes import market as market_route
from pakfindata.api.routes import nccpl as nccpl_route
from pakfindata.api.routes import quality as quality_route
from pakfindata.api.routes import research as research_route
from pakfindata.api.routes import sync as sync_route
from pakfindata.api.routes import tick_logs as tick_logs_route

# Legacy routers (Phase 0-era; full business surface). These continue
# to exist under `/api/*` while Phase 1 migrates them to `/v1/*`
# under `routes/`. Bearer auth applies to them via the global
# middleware just like to anything else.
from .routers import (
    bonds,
    company,
    eod,
    fi,
    funds,
    fx,
    global_rates,
    instruments,
    live,
    market,
    npc_rates,
    rates,
    symbols,
    tasks,
    treasury,
    ws,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    # get_settings() raises if PAKFINDATA_API_TOKEN is missing —
    # this is the fail-fast point for misconfiguration.
    _settings = get_settings()
    yield


app = FastAPI(
    title="pakfindata API",
    description=(
        "Backend API for PSX market data. Endpoints under /v1 follow "
        "the Phase 1 contract (auth required, pydantic models). "
        "Endpoints under /api are legacy Phase-0 routers retained "
        "during the migration."
    ),
    version="0.1.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url=None,
)

# CORS — locked down. Same-machine Streamlit doesn't need CORS for
# server-side fetches, but allowlist the dev URL for browser-side
# DevTools / future direct fetches.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:8501",
        "http://localhost:8501",
    ],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["Authorization", "Content-Type"],
)

# Global Bearer-auth middleware. /health, /docs, /openapi.json are
# the only public paths.
app.add_middleware(BearerAuthMiddleware)


# New-style routes (Phase 1+)
app.include_router(health_route.router)
app.include_router(freshness_route.router)
app.include_router(eod_route.router)
app.include_router(indices_route.router)
app.include_router(market_route.market_router)
app.include_router(market_route.rates_router)
app.include_router(fx_route.fx_router)
app.include_router(fx_route.rates_extra_router)
app.include_router(equities_route.symbols_router)
app.include_router(equities_route.sectors_router)
app.include_router(equities_route.companies_router)
app.include_router(equities_route.factors_router)
app.include_router(fi_route.treasury_router)
app.include_router(fi_route.yield_curves_router)
app.include_router(fi_route.curve_router)
app.include_router(fi_route.bonds_router)
app.include_router(fi_route.benchmark_router)
app.include_router(fi_route.fi_router)
app.include_router(fi_route.alm_router)
app.include_router(fi_route.rates_policy_router)
app.include_router(fi_route.rates_npc_extras_router)
app.include_router(fi_route.rates_global_extras_router)
app.include_router(intraday_route.intraday_router)
app.include_router(intraday_route.turnover_router)
app.include_router(tick_logs_route.tick_logs_router)
app.include_router(futures_route.futures_router)
app.include_router(funds_route.funds_router)
app.include_router(funds_route.etfs_router)
app.include_router(commodities_route.commodities_router)
app.include_router(commodities_route.khistocks_router)
app.include_router(commodities_route.pmex_portal_router)
app.include_router(nccpl_route.router)
app.include_router(admin_route.admin_router)
app.include_router(sync_route.router)
app.include_router(jobs_route.router)
app.include_router(quality_route.router)
app.include_router(research_route.router)


# Legacy routers (Phase 0 era). Bearer auth still applies via
# middleware. These will be migrated under /v1 during Phase 1.
app.include_router(eod.router, prefix="/api/eod", tags=["EOD Data"])
app.include_router(tasks.router, prefix="/api/tasks", tags=["Background Tasks"])
app.include_router(symbols.router, prefix="/api/symbols", tags=["Symbols"])
app.include_router(market.router, prefix="/api/market", tags=["Market Data"])
app.include_router(company.router, prefix="/api/company", tags=["Company Data"])
app.include_router(instruments.router, prefix="/api/instruments", tags=["Instruments"])
app.include_router(fi.router, prefix="/api/fi", tags=["Fixed Income"])
app.include_router(treasury.router, prefix="/api/treasury", tags=["Treasury"])
app.include_router(funds.router, prefix="/api/funds", tags=["Funds"])
app.include_router(rates.router, prefix="/api/rates", tags=["Rates"])
app.include_router(fx.router, prefix="/api/fx", tags=["FX"])
app.include_router(ws.router, prefix="/ws", tags=["WebSocket"])
app.include_router(live.router, prefix="/api/live", tags=["Live Data"])
app.include_router(global_rates.router, prefix="/api/global-rates", tags=["Global Reference Rates"])
app.include_router(npc_rates.router, prefix="/api/npc", tags=["NPC Rates"])
app.include_router(bonds.router, prefix="/api/bonds", tags=["Bond Market"])


@app.get("/")
def root() -> dict:
    """Public root banner."""
    return {
        "name": "pakfindata API",
        "version": "0.1.0",
        "docs": "/docs",
    }
