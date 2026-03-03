"""Entry point: python -m pakfindata.mcp"""

import asyncio

from .server import main

asyncio.run(main())
