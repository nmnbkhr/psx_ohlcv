"""PMEX Official OHLCV + Settlement + FX Rate via GetOHLC JSON API.

Endpoint: POST https://mportal.pmex.com.pk/mt5bonew/Home/GetOHLC
Body:     txtFromDate=MM/DD/YYYY&txtEndDate=MM/DD/YYYY
Auth:     None (public, session cookie required)
Response: JSON array with obfuscated column names
Max:      3 months per request

Column Mapping (confirmed):
  Trader_Id     → symbol
  Post_Date     → trading_date (DD/MM/YYYY)
  Trader_Name   → open
  Trans_Id      → high
  Amount        → low
  acc_type      → close
  Verified_Date → traded_volume
  Status        → settlement_price
  Trans_Date    → fx_rate
"""

import logging
import time
from datetime import date, timedelta

import pandas as pd
import requests

logger = logging.getLogger("pakfindata.commodities.fetcher_pmex_ohlc")

OHLC_API = "https://mportal.pmex.com.pk/mt5bonew/Home/GetOHLC"
PAGE_URL = "https://mportal.pmex.com.pk/mt5bonew/Home/OHLCReport"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": PAGE_URL,
    "Origin": "https://mportal.pmex.com.pk",
}

# Obfuscated JSON keys → meaningful column names
COLUMN_MAP = {
    "Trader_Id": "symbol",
    "Post_Date": "trading_date",
    "Trader_Name": "open",
    "Trans_Id": "high",
    "Amount": "low",
    "acc_type": "close",
    "Verified_Date": "traded_volume",
    "Status": "settlement_price",
    "Trans_Date": "fx_rate",
}


def _build_body(from_date: date, to_date: date) -> str:
    """Build POST body with ISO date format (YYYY-MM-DD)."""
    return (
        f"txtFromDate={from_date.isoformat()}"
        f"&txtEndDate={to_date.isoformat()}"
    )


def _parse_response(data: list) -> pd.DataFrame:
    """Parse GetOHLC JSON array into clean DataFrame."""
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    # Rename obfuscated columns
    df = df.rename(columns=COLUMN_MAP)
    keep = [c for c in COLUMN_MAP.values() if c in df.columns]
    df = df[keep]

    # Parse date
    df["trading_date"] = pd.to_datetime(
        df["trading_date"], format="%d/%m/%Y", errors="coerce"
    )
    # Drop rows with unparseable dates
    df = df.dropna(subset=["trading_date"])

    # Parse numeric columns
    for col in ["open", "high", "low", "close", "settlement_price", "fx_rate"]:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", ""), errors="coerce"
            )

    # Volume as integer
    df["traded_volume"] = (
        pd.to_numeric(
            df["traded_volume"].astype(str).str.replace(",", ""), errors="coerce"
        )
        .fillna(0)
        .astype(int)
    )

    return df.sort_values(["trading_date", "symbol"]).reset_index(drop=True)


def fetch_ohlc(
    from_date: date,
    to_date: date,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    """Fetch PMEX OHLC data for a date range (max 3 months).

    Returns DataFrame with: trading_date, symbol, open, high, low, close,
                            traded_volume, settlement_price, fx_rate
    """
    s = session or requests.Session()

    # Establish session cookie
    s.get(PAGE_URL, headers={"User-Agent": HEADERS["User-Agent"]}, timeout=15)

    body = _build_body(from_date, to_date)
    resp = s.post(OHLC_API, data=body, headers=HEADERS, timeout=30)

    if resp.status_code != 200:
        logger.warning("HTTP %d for %s→%s", resp.status_code, from_date, to_date)
        return pd.DataFrame()

    try:
        data = resp.json()
    except Exception:
        logger.warning("Non-JSON response for %s→%s", from_date, to_date)
        return pd.DataFrame()

    if not isinstance(data, list) or len(data) == 0:
        logger.info("Empty response for %s→%s", from_date, to_date)
        return pd.DataFrame()

    return _parse_response(data)


def backfill(
    start_date: date = date(2020, 1, 1),
    end_date: date | None = None,
    delay: float = 2.0,
    active_only: bool = False,
    progress_callback=None,
) -> pd.DataFrame:
    """Backfill OHLC in 90-day chunks.

    6 years ≈ 25 requests × 2s delay = ~50 seconds.
    progress_callback(current_chunk, total_chunks, chunk_label) if provided.
    """
    if end_date is None:
        end_date = date.today()

    session = requests.Session()
    chunks: list[pd.DataFrame] = []
    cur = start_date

    # Calculate total chunks for progress
    total_days = (end_date - start_date).days
    total_chunks = max(1, (total_days + 89) // 90)
    chunk_num = 0

    while cur < end_date:
        chunk_end = min(cur + timedelta(days=89), end_date)
        chunk_num += 1

        logger.info("Chunk %d/%d: %s → %s", chunk_num, total_chunks, cur, chunk_end)

        df = fetch_ohlc(cur, chunk_end, session)
        if not df.empty:
            if active_only:
                df = df[df["traded_volume"] > 0]
            chunks.append(df)
            logger.info(
                "  %d rows, %d symbols", len(df), df["symbol"].nunique()
            )

        if progress_callback:
            progress_callback(chunk_num, total_chunks, f"{cur} → {chunk_end}")

        cur = chunk_end + timedelta(days=1)
        if cur < end_date:
            time.sleep(delay)

    if chunks:
        result = pd.concat(chunks, ignore_index=True).drop_duplicates(
            subset=["trading_date", "symbol"], keep="last"
        )
        logger.info(
            "TOTAL: %d rows | %d symbols | %s → %s",
            len(result),
            result["symbol"].nunique(),
            result["trading_date"].min().date(),
            result["trading_date"].max().date(),
        )
        return result

    return pd.DataFrame()


def sync_recent(days: int = 3) -> pd.DataFrame:
    """Daily sync: fetch last N days."""
    return fetch_ohlc(date.today() - timedelta(days=days), date.today())
