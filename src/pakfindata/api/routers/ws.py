"""WebSocket endpoints for real-time updates."""

import asyncio
import json

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from ..broadcast import hub

router = APIRouter()


async def _ws_loop(ws: WebSocket, channel: str):
    """Subscribe to a broadcast channel and forward messages to the WebSocket client."""
    await ws.accept()
    queue = await hub.subscribe(channel)
    try:
        while True:
            message = await queue.get()
            await ws.send_json(message)
    except WebSocketDisconnect:
        pass
    finally:
        await hub.unsubscribe(channel, queue)


@router.websocket("/market-feed")
async def market_feed(ws: WebSocket):
    """Push market data updates to connected clients.

    Workers call ``hub.publish("market-feed", data)`` when new data arrives.
    """
    await _ws_loop(ws, "market-feed")


@router.websocket("/sync-status")
async def sync_status(ws: WebSocket):
    """Push sync/scrape progress updates to connected clients.

    Workers call ``hub.publish("sync-status", data)`` when progress changes.
    """
    await _ws_loop(ws, "sync-status")
