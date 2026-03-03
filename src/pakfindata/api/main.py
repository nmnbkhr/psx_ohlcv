"""
FastAPI backend for PakFinData application.

Provides REST API endpoints for:
- EOD data statistics and loading
- Background task management
- Sync operations

Run with: uvicorn pakfindata.api.main:app --reload --port 8000
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routers import eod, tasks, symbols, market, company, instruments, fi, ws, treasury, funds, rates, fx, live, global_rates, npc_rates, bonds

app = FastAPI(
    title="PakFinData API",
    description="Backend API for PSX market data — EOD, company, instruments, fixed income, treasury, funds, FX rates",
    version="3.0.0",
)

# CORS middleware for Streamlit frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Streamlit runs on different port
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
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
def root():
    """API root endpoint."""
    return {
        "name": "PakFinData API",
        "version": "3.0.0",
        "docs": "/docs",
    }


@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}
