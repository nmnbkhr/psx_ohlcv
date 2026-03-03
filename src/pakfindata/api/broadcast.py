"""In-memory pub/sub broadcast hub for WebSocket clients. No Redis needed."""

import asyncio
from collections import defaultdict


class BroadcastHub:
    """In-memory pub/sub for WebSocket clients."""

    def __init__(self):
        self._subscribers: dict[str, set[asyncio.Queue]] = defaultdict(set)

    async def subscribe(self, channel: str) -> asyncio.Queue:
        """Subscribe to a channel, returns a queue to read from."""
        queue: asyncio.Queue = asyncio.Queue()
        self._subscribers[channel].add(queue)
        return queue

    async def publish(self, channel: str, message: dict):
        """Publish to all subscribers of a channel."""
        for queue in self._subscribers.get(channel, set()):
            await queue.put(message)

    async def unsubscribe(self, channel: str, queue: asyncio.Queue):
        """Remove a subscriber."""
        self._subscribers[channel].discard(queue)
        if not self._subscribers[channel]:
            del self._subscribers[channel]

    @property
    def channels(self) -> list[str]:
        """List active channels."""
        return list(self._subscribers.keys())

    def subscriber_count(self, channel: str) -> int:
        """Count subscribers on a channel."""
        return len(self._subscribers.get(channel, set()))


# Global singleton — shared across the app
hub = BroadcastHub()
