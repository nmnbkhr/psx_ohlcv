# Claude Code Prompt: PSX WebSocket Relay — FastAPI Real-Time Push

## What this is

Add a FastAPI WebSocket relay to PSX OHLCV that takes the ticks already flowing into `tick_collector.py` and pushes them out to any connected WebSocket client in real-time. Zero latency. No more 2-second JSON polling.

**This replaces nothing. The JSON snapshot + Streamlit pages keep working exactly as before. This is a PARALLEL real-time channel for any frontend that wants true streaming — React app, mobile app, Python client, browser console, anything.**

## Rules — READ THESE FIRST

1. **CREATE only 1 file:** `services/ws_relay.py` — FastAPI app with WebSocket endpoints
2. **MODIFY only 1 file:** `services/tick_collector.py` — add broadcast hooks (3 lines of code)
3. **DO NOT modify ANY other files** — zero changes to UI, db, app.py, anything
4. **DO NOT import from any existing PSX OHLCV modules** — ws_relay.py is self-contained
5. **Same process** — tick_collector starts the relay server on a background thread. ONE process, ONE command
6. **pip install fastapi uvicorn** before running (websockets already installed from live ticks)

## Architecture

```
PSX Server                    tick_collector.py                    Clients
wss://psxterminal.com ──push──▶  (memory + broadcast)  ──push──▶  Browser / React / Python
                                      │      │                     via ws://localhost:8765
                                      │      │
                                      │      └──▶ ws_relay.py (FastAPI, same process)
                                      │            ├── /ws/ticks       (all stock ticks)
                                      │            ├── /ws/ticks/REG   (filtered by market)
                                      │            ├── /ws/indices     (all index ticks)
                                      │            ├── /ws/symbol/HUBC (single symbol)
                                      │            └── /snapshot       (REST — current state)
                                      │
                                      └──▶ data/live_snapshot.json (unchanged, still works)
                                      └──▶ data/tick_bars.db at EOD  (unchanged, still works)
```

**Latency: <1ms from tick arrival to client push.** The collector calls `broadcast()` the instant a tick arrives. No file I/O in the path.

---

## File 1: `services/ws_relay.py` (CREATE NEW)

FastAPI application with WebSocket endpoints. Imported and started by tick_collector.py.

### What it does

1. Maintains a set of connected WebSocket clients per channel
2. When tick_collector calls `broadcast(channel, data)`, relay pushes to all clients on that channel instantly
3. Also exposes REST endpoints for current state (snapshot equivalent but from memory)
4. Runs on port 8765 (configurable)

### Full implementation spec

```python
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

# Allow all origins — this is a local dev tool
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
        logger.info(f"Client connected to {channel} ({len(self.channels[channel])} total)")
    
    async def disconnect(self, ws: WebSocket, channel: str):
        async with self._lock:
            self.channels[channel].discard(ws)
        logger.info(f"Client disconnected from {channel} ({len(self.channels[channel])} remaining)")
    
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
        return {ch: len(clients) for ch, clients in self.channels.items() if clients}


manager = ConnectionManager()

# ============================================
# Reference to tick_collector (set at startup)
# ============================================

_collector = None  # Will be set by tick_collector.py before starting server

def set_collector(collector):
    """Called by tick_collector to give relay access to live data."""
    global _collector
    _collector = collector

# ============================================
# WebSocket Endpoints
# ============================================

@app.websocket("/ws/ticks")
async def ws_all_ticks(ws: WebSocket, market: str = Query(default=None)):
    """
    Stream all stock ticks, or filter by market.
    
    Connect to:
      ws://localhost:8765/ws/ticks           → all stock ticks
      ws://localhost:8765/ws/ticks?market=REG → only REG market
    """
    channel = f"ticks:{market}" if market else "ticks:all"
    await manager.connect(ws, channel)
    try:
        while True:
            # Keep connection alive — client can send pings or commands
            msg = await ws.receive_text()
            # Could handle client commands here (e.g., change filter)
    except WebSocketDisconnect:
        pass
    finally:
        await manager.disconnect(ws, channel)


@app.websocket("/ws/indices")
async def ws_indices(ws: WebSocket):
    """
    Stream all index ticks.
    
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
    """
    Stream ticks for a single symbol.
    
    Connect to: ws://localhost:8765/ws/symbol/HUBC
    """
    channel = f"symbol:{symbol.upper()}"
    await manager.connect(ws, channel)
    
    # Send current state immediately on connect
    if _collector:
        for key, tick in _collector.live.items():
            if tick.get("symbol", "").upper() == symbol.upper():
                try:
                    await ws.send_text(json.dumps({"type": "snapshot", "data": tick}))
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
    """
    Stream EVERYTHING — all stock ticks + all index ticks.
    Use for recording or full-market replay.
    
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
# REST Endpoints (bonus — current state)
# ============================================

@app.get("/snapshot")
async def get_snapshot():
    """Get current market state — equivalent to reading live_snapshot.json but from memory."""
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
    """Get current state for a single symbol."""
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
# Broadcast helper — called by tick_collector
# ============================================

_loop: asyncio.AbstractEventLoop = None  # Set when server starts

def set_loop(loop: asyncio.AbstractEventLoop):
    global _loop
    _loop = loop


async def _do_broadcast(channel: str, data: dict):
    """Internal async broadcast."""
    await manager.broadcast(channel, data)


def broadcast_tick(tick: dict, market: str, symbol: str):
    """
    Called by tick_collector synchronously for every tick.
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
        asyncio.run_coroutine_threadsafe(_do_broadcast("firehose", message), _loop)
        
        if market == "IDX":
            asyncio.run_coroutine_threadsafe(_do_broadcast("indices", message), _loop)
        else:
            asyncio.run_coroutine_threadsafe(_do_broadcast("ticks:all", message), _loop)
            asyncio.run_coroutine_threadsafe(_do_broadcast(f"ticks:{market}", message), _loop)
        
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
    import uvicorn
    
    def _run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        set_loop(loop)
        
        config = uvicorn.Config(
            app=app,
            host=host,
            port=port,
            log_level="warning",  # Quiet — tick_collector handles logging
            loop="asyncio",
        )
        server = uvicorn.Server(config)
        loop.run_until_complete(server.serve())
    
    thread = threading.Thread(target=_run, daemon=True, name="ws-relay")
    thread.start()
    logger.info(f"WebSocket relay started on ws://{host}:{port}")
    return thread
```

---

## Changes to File 2: `services/tick_collector.py` (MODIFY)

Only 3 changes needed. Minimal surgery.

### Change 1: Import and start relay at startup

Add near the top of `tick_collector.py`, after existing imports:

```python
# WebSocket relay — real-time push to clients
try:
    from ws_relay import set_collector, broadcast_tick, start_server as start_relay
    HAS_RELAY = True
except ImportError:
    HAS_RELAY = False
```

In the main startup section (before the WebSocket connect loop), add:

```python
if HAS_RELAY:
    RELAY_PORT = int(os.environ.get("RELAY_PORT", "8765"))
    set_collector(self)  # Give relay access to live data
    start_relay(port=RELAY_PORT)
    print(f"📡 WebSocket relay on ws://0.0.0.0:{RELAY_PORT}")
    print(f"   Docs: http://localhost:{RELAY_PORT}/docs")
else:
    print("📡 WebSocket relay not available (pip install fastapi uvicorn)")
```

### Change 2: Broadcast on every stock tick

In the existing stock tick handler (wherever the tick is processed and added to `self.live`), add ONE line at the end:

```python
def _handle_stock_tick(self, tick):
    # ... existing logic that updates self.live, self.raw_ticks, self.builder ...
    
    # NEW — broadcast to WebSocket clients
    if HAS_RELAY:
        broadcast_tick(tick, tick.get("market", "REG"), tick.get("symbol", ""))
```

### Change 3: Broadcast on every index tick

In the index tick handler (from the indices prompt), add ONE line at the end:

```python
def _handle_index_tick(self, tick):
    # ... existing logic that updates self.indices, self.index_ticks, self.index_history ...
    
    # NEW — broadcast to WebSocket clients
    if HAS_RELAY:
        broadcast_tick(tick, "IDX", tick.get("symbol", ""))
```

### Updated console output

```
🚀 PSX Tick Collector (memory mode)
   Snapshot: data/live_snapshot.json
   EOD target: data/tick_bars.db
📡 WebSocket relay on ws://0.0.0.0:8765
   Docs: http://localhost:8765/docs
🔗 Connected
✅ REG ✅ FUT ✅ ODL ✅ BNB ✅ CSF ✅ IDX
⚡ Ticks: 1,247 | Bars: 500 | Symbols: 312 | Indices: 12 | WS Clients: 3 | RAM: 15 MB
```

Add `WS Clients: {manager.client_count if HAS_RELAY else 0}` to the periodic status line.

---

## WebSocket Channels Reference

| Endpoint | What you get | Use case |
|----------|-------------|----------|
| `ws://localhost:8765/ws/ticks` | All stock ticks (REG+FUT+ODL+BNB+CSF) | Full market dashboard |
| `ws://localhost:8765/ws/ticks?market=REG` | Only REG market ticks | Regular market view |
| `ws://localhost:8765/ws/ticks?market=FUT` | Only FUT market ticks | Futures view |
| `ws://localhost:8765/ws/indices` | All 12 index ticks | Index dashboard |
| `ws://localhost:8765/ws/symbol/HUBC` | Single symbol ticks | Symbol detail page |
| `ws://localhost:8765/ws/firehose` | EVERYTHING — all markets + all indices | Recording / replay |

## REST Endpoints Reference

| Endpoint | Method | What it returns |
|----------|--------|----------------|
| `http://localhost:8765/snapshot` | GET | Full current state (like live_snapshot.json but from memory) |
| `http://localhost:8765/symbol/HUBC` | GET | Current state for one symbol |
| `http://localhost:8765/health` | GET | Server health + client count |
| `http://localhost:8765/stats` | GET | Client/channel statistics |
| `http://localhost:8765/docs` | GET | Swagger UI — interactive API docs |

---

## Test

### Step 1: Install

```bash
pip install fastapi uvicorn
# websockets + streamlit-autorefresh already installed from live ticks prompt
```

### Step 2: Run collector (relay starts automatically)

```bash
python services/tick_collector.py
# Should see:
# 📡 WebSocket relay on ws://0.0.0.0:8765
# 🔗 Connected
```

### Step 3: Test REST endpoints

```bash
# Health check
curl http://localhost:8765/health

# Full snapshot from memory
curl http://localhost:8765/snapshot | python -m json.tool | head -20

# Single symbol
curl http://localhost:8765/symbol/HUBC

# Server stats
curl http://localhost:8765/stats

# Swagger docs — open in browser
# http://localhost:8765/docs
```

### Step 4: Test WebSocket — Python client

```python
# Save as test_ws.py and run: python test_ws.py
import asyncio
import json
import websockets

async def test():
    # Connect to all stock ticks
    async with websockets.connect("ws://localhost:8765/ws/ticks") as ws:
        print("Connected to /ws/ticks")
        for i in range(10):
            msg = await ws.recv()
            data = json.loads(msg)
            tick = data["data"]
            sym = tick.get("symbol", "?")
            price = tick.get("price", 0)
            print(f"  {sym:8s}  {price:>10.2f}")
        print("Done — 10 ticks received")

asyncio.run(test())
```

### Step 5: Test WebSocket — browser console

Open browser → F12 → Console → paste:

```javascript
const ws = new WebSocket("ws://localhost:8765/ws/indices");
ws.onmessage = (e) => {
    const tick = JSON.parse(e.data).data;
    console.log(`${tick.symbol}: ${tick.price || tick.value}`);
};
ws.onopen = () => console.log("Connected to indices");
```

### Step 6: Test single symbol stream

```python
import asyncio, json, websockets

async def watch_hubc():
    async with websockets.connect("ws://localhost:8765/ws/symbol/HUBC") as ws:
        print("Watching HUBC...")
        async for msg in ws:
            data = json.loads(msg)
            tick = data["data"]
            print(f"  HUBC  {tick.get('price',0):>10.2f}  vol={tick.get('volume',0):,}")

asyncio.run(watch_hubc())
```

### Step 7: Test firehose (everything)

```python
import asyncio, json, websockets

async def firehose():
    async with websockets.connect("ws://localhost:8765/ws/firehose") as ws:
        count = 0
        async for msg in ws:
            data = json.loads(msg)
            tick = data["data"]
            mkt = tick.get("market", "?")
            sym = tick.get("symbol", "?")
            count += 1
            if count <= 20:
                print(f"  [{mkt}] {sym:8s}  {tick.get('price', tick.get('value', 0)):>12.2f}")
            else:
                print(f"\n{count} ticks received in firehose. Working!")
                break

asyncio.run(firehose())
```

---

## How it all fits together now

```
                    ┌─────────────────────────────────────────────┐
                    │          tick_collector.py (1 process)       │
                    │                                             │
PSX Server ──WSS──▶ │  ┌─────────────┐    ┌──────────────────┐   │
                    │  │ Tick Handler │───▶│ In-Memory Store   │   │
                    │  │ (stock+idx)  │    │ live={}, indices= │   │
                    │  └──────┬───────┘    │ bars, raw_ticks   │   │
                    │         │            └────────┬──────────┘   │
                    │         │                     │              │
                    │    broadcast()           write every 2s     │
                    │         │                     │              │
                    │         ▼                     ▼              │
                    │  ┌──────────────┐   ┌────────────────┐      │
                    │  │  ws_relay.py  │   │ JSON snapshot  │      │
                    │  │  (FastAPI)    │   │ (file on disk) │      │
                    │  │  port 8765    │   └───────┬────────┘      │
                    │  └──────┬───────┘           │               │
                    │         │                    │               │
                    └─────────┼────────────────────┼───────────────┘
                              │                    │
                    ┌─────────▼──────┐   ┌────────▼─────────┐
                    │  React / Any   │   │  Streamlit pages  │
                    │  WebSocket     │   │  (live_market.py  │
                    │  client        │   │   live_indices.py) │
                    │  <1ms latency  │   │  2s polling        │
                    └────────────────┘   └──────────────────┘
```

**Both paths work simultaneously.** Streamlit pages keep reading JSON as before. Any WebSocket client gets true real-time push. Zero conflict.

---

## Graceful degradation

If `fastapi` or `uvicorn` is not installed:
- `HAS_RELAY = False`
- Collector works exactly as before — no relay, no error
- JSON snapshot + Streamlit pages unaffected

If no WebSocket clients are connected:
- `broadcast_tick()` checks `manager.client_count` first
- If zero → returns immediately, zero overhead
- No wasted CPU on serialization or sends

---

## Environment variables

| Variable | Default | What |
|----------|---------|------|
| `RELAY_PORT` | `8765` | Port for WebSocket relay |

```bash
# Custom port
RELAY_PORT=9000 python services/tick_collector.py
```

---

## Files summary

| Action | File | What changes |
|--------|------|-------------|
| CREATE | `services/ws_relay.py` | FastAPI app — ConnectionManager, 4 WebSocket endpoints (/ws/ticks, /ws/indices, /ws/symbol/{sym}, /ws/firehose), 4 REST endpoints (/snapshot, /symbol/{sym}, /health, /stats), broadcast_tick() function, start_server() |
| MODIFY | `services/tick_collector.py` | Import ws_relay (with fallback), call set_collector + start_relay at startup, add ONE broadcast_tick() call in stock handler, add ONE broadcast_tick() call in index handler, add WS client count to status line |

**Nothing else changes. Streamlit pages, JSON snapshot, EOD flush — all untouched. The relay is a parallel real-time channel that costs nothing when no clients are connected.**
