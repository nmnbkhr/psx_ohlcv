"""PSX WebSocket Relay — FastAPI real-time push for live ticks.

Runs as a background thread inside tick_service.py (same process).
Pushes every tick to connected WebSocket clients with <1ms latency.

Channels:
  /ws/ticks              — all stock ticks (or ?market=REG for filtered)
  /ws/indices            — all index ticks
  /ws/symbol/{symbol}    — single symbol stream
  /ws/firehose           — everything (stocks + indices)

REST:
  /snapshot              — current market state from memory
  /symbol/{symbol}       — single symbol state
  /health                — server health + client count
  /stats                 — channel statistics
  /docs                  — Swagger UI
"""

import asyncio
import json
import logging
import threading
from collections import defaultdict
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware

logger = logging.getLogger("ws_relay")

app = FastAPI(title="PSX Live Relay", docs_url="/docs")

# Allow all origins — local dev tool
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================
# Connection Manager
# ============================================

class ConnectionManager:
    """Manages WebSocket connections per channel."""

    def __init__(self):
        # channel → set of WebSocket connections
        self.channels: dict[str, set[WebSocket]] = defaultdict(set)
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket, channel: str):
        await ws.accept()
        async with self._lock:
            self.channels[channel].add(ws)
        logger.info(
            "Client connected to %s (%d total)",
            channel, len(self.channels[channel]),
        )

    async def disconnect(self, ws: WebSocket, channel: str):
        async with self._lock:
            self.channels[channel].discard(ws)
        logger.info(
            "Client disconnected from %s (%d remaining)",
            channel, len(self.channels[channel]),
        )

    async def broadcast(self, channel: str, data: dict):
        """Push data to all clients on a channel. Non-blocking."""
        async with self._lock:
            clients = list(self.channels.get(channel, set()))

        if not clients:
            return

        message = json.dumps(data)
        dead = []
        for ws in clients:
            try:
                await ws.send_text(message)
            except Exception:
                dead.append(ws)

        # Clean up dead connections
        if dead:
            async with self._lock:
                for ws in dead:
                    self.channels[channel].discard(ws)

    @property
    def client_count(self) -> int:
        return sum(len(clients) for clients in self.channels.values())

    @property
    def channel_stats(self) -> dict:
        return {
            ch: len(clients)
            for ch, clients in self.channels.items()
            if clients
        }


manager = ConnectionManager()


# ============================================
# Reference to tick service (set at startup)
# ============================================

_collector = None  # Set by tick_service.py before starting server


def set_collector(collector):
    """Called by tick_service to give relay access to live data."""
    global _collector
    _collector = collector


# ============================================
# WebSocket Endpoints
# ============================================

@app.websocket("/ws/ticks")
async def ws_all_ticks(ws: WebSocket, market: str = Query(default=None)):
    """Stream all stock ticks, or filter by market.

    Connect to:
      ws://localhost:8765/ws/ticks            → all stock ticks
      ws://localhost:8765/ws/ticks?market=REG  → only REG market
    """
    channel = f"ticks:{market}" if market else "ticks:all"
    await manager.connect(ws, channel)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        await manager.disconnect(ws, channel)


@app.websocket("/ws/indices")
async def ws_indices(ws: WebSocket):
    """Stream all index ticks.

    Connect to: ws://localhost:8765/ws/indices
    """
    await manager.connect(ws, "indices")
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        await manager.disconnect(ws, "indices")


@app.websocket("/ws/symbol/{symbol}")
async def ws_single_symbol(ws: WebSocket, symbol: str):
    """Stream ticks for a single symbol.

    Connect to: ws://localhost:8765/ws/symbol/HUBC
    """
    channel = f"symbol:{symbol.upper()}"
    await manager.connect(ws, channel)

    # Send current state immediately on connect
    if _collector:
        for key, tick in _collector.live.items():
            if tick.get("symbol", "").upper() == symbol.upper():
                try:
                    await ws.send_text(
                        json.dumps({"type": "snapshot", "data": tick})
                    )
                except Exception:
                    pass
                break

    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        await manager.disconnect(ws, channel)


@app.websocket("/ws/firehose")
async def ws_firehose(ws: WebSocket):
    """Stream EVERYTHING — all stock ticks + all index ticks.

    Connect to: ws://localhost:8765/ws/firehose
    """
    await manager.connect(ws, "firehose")
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        await manager.disconnect(ws, "firehose")


# ============================================
# REST Endpoints
# ============================================

@app.get("/snapshot")
async def get_snapshot():
    """Current market state — equivalent to live_snapshot.json but from memory."""
    if not _collector:
        return {"error": "Collector not connected"}

    return {
        "connected": _collector.connected,
        "tick_count": _collector.tick_count,
        "symbol_count": len(_collector.live),
        "index_count": len(getattr(_collector, "indices", {})),
        "ws_clients": manager.client_count,
        "ws_channels": manager.channel_stats,
        "symbols": list(_collector.live.values()),
        "indices": list(getattr(_collector, "indices", {}).values()),
    }


@app.get("/health")
async def health():
    """Health check."""
    return {
        "status": "ok",
        "connected": _collector.connected if _collector else False,
        "clients": manager.client_count,
        "channels": manager.channel_stats,
    }


@app.get("/symbol/{symbol}")
async def get_symbol(symbol: str):
    """Current state for a single symbol."""
    if not _collector:
        return {"error": "Collector not connected"}

    symbol = symbol.upper()
    # Search in live dict
    for key, tick in _collector.live.items():
        if tick.get("symbol", "").upper() == symbol:
            return tick

    # Search in indices
    idx = getattr(_collector, "indices", {}).get(symbol)
    if idx:
        return idx

    return {"error": f"Symbol {symbol} not found"}


@app.get("/stats")
async def get_stats():
    """Server statistics."""
    return {
        "ws_clients": manager.client_count,
        "ws_channels": manager.channel_stats,
        "tick_count": _collector.tick_count if _collector else 0,
        "symbols": len(_collector.live) if _collector else 0,
        "indices": len(getattr(_collector, "indices", {})) if _collector else 0,
    }


# ============================================
# Broadcast helper — called by tick_service
# ============================================

_loop: asyncio.AbstractEventLoop | None = None


def set_loop(loop: asyncio.AbstractEventLoop):
    global _loop
    _loop = loop


async def _do_broadcast(channel: str, data: dict):
    """Internal async broadcast."""
    await manager.broadcast(channel, data)


def broadcast_tick(tick: dict, market: str, symbol: str):
    """Called by tick_service synchronously for every tick.

    Schedules async broadcast on the relay's event loop.

    Broadcasts to:
      - firehose (always)
      - ticks:all or indices (depending on market)
      - ticks:{market} (market-specific)
      - symbol:{symbol} (symbol-specific)
    """
    if not _loop or not manager.client_count:
        return  # No clients, skip entirely

    message = {"type": "tick", "data": tick}

    try:
        # Schedule on relay's event loop (thread-safe)
        asyncio.run_coroutine_threadsafe(
            _do_broadcast("firehose", message), _loop
        )

        if market == "IDX":
            asyncio.run_coroutine_threadsafe(
                _do_broadcast("indices", message), _loop
            )
        else:
            asyncio.run_coroutine_threadsafe(
                _do_broadcast("ticks:all", message), _loop
            )
            asyncio.run_coroutine_threadsafe(
                _do_broadcast(f"ticks:{market}", message), _loop
            )

        asyncio.run_coroutine_threadsafe(
            _do_broadcast(f"symbol:{symbol.upper()}", message), _loop
        )
    except Exception:
        pass  # Never crash the collector for a broadcast failure


# ============================================
# Server startup — runs in background thread
# ============================================

def start_server(host: str = "0.0.0.0", port: int = 8765):
    """Start FastAPI server in a background thread. Non-blocking."""
    import socket
    import uvicorn

    # Check if port is available first
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind((host, port))
        sock.close()
    except OSError:
        logger.warning(
            "Relay port %d busy — kill old process or set RELAY_PORT env var",
            port,
        )
        print(
            f"  ⚠️ Relay port {port} busy — kill old process or "
            f"set RELAY_PORT env var"
        )
        return None

    def _run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        set_loop(loop)

        config = uvicorn.Config(
            app=app,
            host=host,
            port=port,
            log_level="warning",
            loop="asyncio",
        )
        server = uvicorn.Server(config)
        loop.run_until_complete(server.serve())

    thread = threading.Thread(target=_run, daemon=True, name="ws-relay")
    thread.start()
    logger.info("WebSocket relay started on ws://%s:%d", host, port)
    return thread
