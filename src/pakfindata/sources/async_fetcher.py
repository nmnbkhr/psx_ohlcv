"""Async HTTP fetcher for PSX data using aiohttp.

Provides concurrent fetching with semaphore-based rate limiting,
connection pooling, retries with exponential backoff, and progress callbacks.

Usage:
    async with AsyncPSXFetcher() as fetcher:
        results = await fetcher.fetch_eod_batch(symbols)
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Any, Callable

import aiohttp

logger = logging.getLogger("pakfindata")

# PSX API endpoints
PSX_BASE = "https://dps.psx.com.pk"
EOD_URL = f"{PSX_BASE}/timeseries/eod/{{symbol}}"
INTRADAY_URL = f"{PSX_BASE}/timeseries/int/{{symbol}}"
COMPANY_URL = f"{PSX_BASE}/company/{{symbol}}"

DEFAULT_USER_AGENT = "pakfindata/0.1.0 (Python; educational project)"


class AsyncPSXFetcher:
    """Async HTTP fetcher with concurrency control and rate limiting."""

    def __init__(
        self,
        max_concurrent: int = 25,
        rate_limit: float = 0.05,
        timeout: int = 30,
        max_retries: int = 3,
        connector_limit: int = 50,
    ):
        self.max_concurrent = max_concurrent
        self.rate_limit = rate_limit
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.max_retries = max_retries
        self.connector_limit = connector_limit
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> AsyncPSXFetcher:
        connector = aiohttp.TCPConnector(limit=self.connector_limit)
        headers = {
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept": "application/json, text/html, */*",
            "Accept-Language": "en-US,en;q=0.5",
        }
        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=self.timeout,
            headers=headers,
        )
        return self

    async def __aexit__(self, *exc: Any) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    async def _fetch_json(self, url: str) -> tuple[Any, str | None]:
        """Fetch JSON from URL with retries and rate limiting.

        Returns:
            Tuple of (data, error). On success error is None.
        """
        last_error = None
        for attempt in range(self.max_retries):
            async with self._semaphore:
                try:
                    async with self._session.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.json(content_type=None)
                            # Polite delay between requests
                            await asyncio.sleep(self.rate_limit)
                            return data, None
                        last_error = f"HTTP {resp.status}"
                except asyncio.TimeoutError:
                    last_error = "timeout"
                except aiohttp.ClientError as e:
                    last_error = str(e)
                except Exception as e:
                    last_error = str(e)

            # Exponential backoff with jitter before retry
            if attempt < self.max_retries - 1:
                delay = (2 ** attempt) * 0.5 + random.uniform(0, 0.5)
                await asyncio.sleep(delay)

        return None, last_error

    async def fetch_eod(self, symbol: str) -> tuple[str, Any, str | None]:
        """Fetch EOD data for a single symbol.

        Returns:
            Tuple of (symbol, data, error).
        """
        url = EOD_URL.format(symbol=symbol.upper())
        data, error = await self._fetch_json(url)
        return symbol.upper(), data, error

    async def fetch_eod_batch(
        self,
        symbols: list[str],
        progress_cb: Callable[[int, int, str, bool], None] | None = None,
    ) -> dict:
        """Fetch EOD data for multiple symbols concurrently.

        Args:
            symbols: List of stock symbols.
            progress_cb: Optional callback(current, total, symbol, success).

        Returns:
            Dict with keys: ok, failed, results, errors, elapsed.
        """
        start = time.time()
        total = len(symbols)
        results = {}
        errors = {}
        completed = 0

        async def _fetch_one(sym: str) -> None:
            nonlocal completed
            symbol, data, error = await self.fetch_eod(sym)
            completed += 1
            if error:
                errors[symbol] = error
                logger.debug("EOD fetch failed for %s: %s", symbol, error)
            else:
                results[symbol] = data
            if progress_cb:
                progress_cb(completed, total, symbol, error is None)

        tasks = [_fetch_one(s) for s in symbols]
        await asyncio.gather(*tasks)

        return {
            "ok": len(results),
            "failed": len(errors),
            "results": results,
            "errors": errors,
            "elapsed": time.time() - start,
        }

    async def fetch_intraday(self, symbol: str) -> tuple[str, Any, str | None]:
        """Fetch intraday data for a single symbol.

        Returns:
            Tuple of (symbol, data, error).
        """
        url = INTRADAY_URL.format(symbol=symbol.upper())
        data, error = await self._fetch_json(url)
        return symbol.upper(), data, error

    async def fetch_intraday_batch(
        self,
        symbols: list[str],
        progress_cb: Callable[[int, int, str, bool], None] | None = None,
    ) -> dict:
        """Fetch intraday data for multiple symbols concurrently.

        Returns:
            Dict with keys: ok, failed, results, errors, elapsed.
        """
        start = time.time()
        total = len(symbols)
        results = {}
        errors = {}
        completed = 0

        async def _fetch_one(sym: str) -> None:
            nonlocal completed
            symbol, data, error = await self.fetch_intraday(sym)
            completed += 1
            if error:
                errors[symbol] = error
            else:
                results[symbol] = data
            if progress_cb:
                progress_cb(completed, total, symbol, error is None)

        tasks = [_fetch_one(s) for s in symbols]
        await asyncio.gather(*tasks)

        return {
            "ok": len(results),
            "failed": len(errors),
            "results": results,
            "errors": errors,
            "elapsed": time.time() - start,
        }

    async def fetch_company_data(self, symbol: str) -> tuple[str, Any, str | None]:
        """Fetch company page data for a single symbol.

        Returns:
            Tuple of (symbol, html_or_data, error).
        """
        url = COMPANY_URL.format(symbol=symbol.upper())
        last_error = None
        for attempt in range(self.max_retries):
            async with self._semaphore:
                try:
                    async with self._session.get(url) as resp:
                        if resp.status == 200:
                            html = await resp.text()
                            await asyncio.sleep(self.rate_limit)
                            return symbol.upper(), html, None
                        last_error = f"HTTP {resp.status}"
                except asyncio.TimeoutError:
                    last_error = "timeout"
                except aiohttp.ClientError as e:
                    last_error = str(e)
            if attempt < self.max_retries - 1:
                delay = (2 ** attempt) * 0.5 + random.uniform(0, 0.5)
                await asyncio.sleep(delay)

        return symbol.upper(), None, last_error
