"""Live data API — status, snapshot, and 5-second bars from tick service."""

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query

try:
    from psx_ohlcv.config import DATA_ROOT
except ImportError:
    DATA_ROOT = Path("/mnt/e/psxdata")

PKT = timezone(timedelta(hours=5))
SNAPSHOT_PATH = DATA_ROOT / "live_snapshot.json"
EOD_DB_PATH = DATA_ROOT / "tick_bars.db"

router = APIRouter()


def _read_snapshot() -> dict | None:
    """Read live snapshot JSON file."""
    if not SNAPSHOT_PATH.exists():
        return None
    try:
        return json.loads(SNAPSHOT_PATH.read_text())
    except (json.JSONDecodeError, IOError):
        return None


@router.get("/status")
async def live_status():
    """Collector health check — is the tick service running?"""
    data = _read_snapshot()
    if data is None:
        return {"running": False, "connected": False}

    try:
        ts = datetime.fromisoformat(data["timestamp"])
        now = datetime.now(ts.tzinfo or PKT)
        age = (now - ts).total_seconds()
    except (KeyError, ValueError):
        age = 999

    return {
        "running": age < 10,
        "connected": data.get("connected", False),
        "tick_count": data.get("tick_count", 0),
        "symbol_count": data.get("symbol_count", 0),
        "bars_in_memory": data.get("bars_in_memory", 0),
        "raw_ticks_in_memory": data.get("raw_ticks_in_memory", 0),
        "ram_mb": data.get("ram_mb", 0),
        "last_update": data.get("timestamp"),
        "age_seconds": round(age, 1),
    }


@router.get("/snapshot")
async def live_snapshot():
    """Full live snapshot for API consumers."""
    data = _read_snapshot()
    if data is None:
        raise HTTPException(503, "Tick service not running")
    return data


@router.get("/bars/{symbol}")
async def live_bars(
    symbol: str,
    minutes: int = Query(30, ge=1, le=480),
    market: str = Query("REG"),
):
    """Recent 5-second OHLCV bars for a symbol from EOD tick database."""
    if not EOD_DB_PATH.exists():
        raise HTTPException(503, "Tick bars database not available (no EOD flush yet)")

    cutoff = (
        datetime.now(PKT) - timedelta(minutes=minutes)
    ).isoformat()

    try:
        con = sqlite3.connect(str(EOD_DB_PATH))
        con.row_factory = sqlite3.Row
        rows = con.execute(
            "SELECT symbol, market, ts, o, h, l, c, v, trades "
            "FROM ohlcv_5s "
            "WHERE symbol = ? AND market = ? AND ts >= ? "
            "ORDER BY ts",
            (symbol.upper(), market.upper(), cutoff),
        ).fetchall()
        con.close()
    except sqlite3.Error as e:
        raise HTTPException(500, f"Database error: {e}")

    return [dict(r) for r in rows]
