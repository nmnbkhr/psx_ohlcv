"""Entry point: python -m psx_ohlcv.mcp"""

import asyncio

from .server import main

asyncio.run(main())
