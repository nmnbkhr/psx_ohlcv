"""
FastAPI backend for PSX OHLCV application.

Provides REST API endpoints for:
- EOD data statistics and loading
- Background task management
- Sync operations

Run with: uvicorn psx_ohlcv.api.main:app --reload --port 8000
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routers import eod, tasks

app = FastAPI(
    title="PSX OHLCV API",
    description="Backend API for PSX EOD data management",
    version="1.0.0",
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


@app.get("/")
def root():
    """API root endpoint."""
    return {
        "name": "PSX OHLCV API",
        "version": "1.0.0",
        "docs": "/docs",
    }


@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}
