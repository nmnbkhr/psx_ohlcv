"""
Khistocks NCCPL FIPI/LIPI scraper — fallback for when nccpl.com.pk is blocked.

Endpoints (no Cloudflare):
  POST https://www.khistocks.com/ajax/fipi_sectors  → FIPI data
  POST https://www.khistocks.com/ajax/lipi_sectors  → LIPI data (same schema)
  GET  https://www.khistocks.com/ajax/getAllFipiSectors → sector list

Data goes back to 2016-08-12. Server-side paginated (DataTables protocol).
"""

import logging
import sqlite3
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests

logger = logging.getLogger(__name__)

BASE = "https://www.khistocks.com"
PKT = timezone(timedelta(hours=5))
PAGE_SIZE = 500


def fetch_fipi(from_date: str = None, to_date: str = None) -> pd.DataFrame:
    """Fetch FIPI (Foreign Portfolio Investment) sector-wise data."""
    return _fetch_flow("fipi_sectors", from_date, to_date)


def fetch_lipi(from_date: str = None, to_date: str = None) -> pd.DataFrame:
    """Fetch LIPI (Local Portfolio Investment) sector-wise data."""
    return _fetch_flow("lipi_sectors", from_date, to_date)


def _fetch_flow(endpoint: str, from_date: str = None, to_date: str = None) -> pd.DataFrame:
    """Fetch flow data from khistocks DataTables API.

    The API ignores date params and returns all available data (~30 days).
    We filter client-side after fetching.
    """
    all_rows = []
    start = 0
    draw = 1

    while True:
        try:
            resp = requests.post(
                f"{BASE}/ajax/{endpoint}",
                data={
                    "draw": draw,
                    "start": start,
                    "length": PAGE_SIZE,
                },
                headers={"X-Requested-With": "XMLHttpRequest"},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.warning("khistocks %s fetch failed: %s", endpoint, e)
            break

        rows = data.get("data", [])
        if not rows:
            break

        all_rows.extend(rows)
        total = data.get("recordsTotal", 0)

        if start + PAGE_SIZE >= total:
            break
        start += PAGE_SIZE
        draw += 1

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    # Standardize columns
    col_map = {
        "date": "date_raw",
        "dt": "date_display",
        "ctype_name": "client_type",
        "smallname": "sector",
        "mtype_name": "market_type",
        "buy_volume": "buy_volume",
        "buy_value": "buy_value",
        "sell_volume": "sell_volume",
        "sell_value": "sell_value",
        "net_volume": "net_volume",
        "net_value": "net_value",
        "usd": "net_value_usd",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    # Parse date
    if "date_raw" in df.columns:
        df["date"] = pd.to_datetime(df["date_raw"]).dt.strftime("%Y-%m-%d")
    elif "date_display" in df.columns:
        df["date"] = pd.to_datetime(df["date_display"]).dt.strftime("%Y-%m-%d")

    # Numeric columns
    for col in ["buy_volume", "buy_value", "sell_volume", "sell_value",
                 "net_volume", "net_value", "net_value_usd"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", ""), errors="coerce")

    return df


def fetch_daily_net(from_date: str = None, to_date: str = None) -> pd.DataFrame:
    """Fetch daily net FIPI + LIPI flows (aggregated across sectors).

    Returns DataFrame with columns: date, fipi_net_mn, lipi_net_mn
    """
    fipi = fetch_fipi(from_date, to_date)
    lipi = fetch_lipi(from_date, to_date)

    results = []
    for label, df in [("fipi", fipi), ("lipi", lipi)]:
        if df.empty:
            continue
        daily = df.groupby("date").agg({"net_value": "sum"}).reset_index()
        daily = daily.rename(columns={"net_value": f"{label}_net_mn"})
        daily[f"{label}_net_mn"] = daily[f"{label}_net_mn"] / 1e6  # to millions
        results.append(daily)

    if not results:
        return pd.DataFrame()

    merged = results[0]
    for r in results[1:]:
        merged = merged.merge(r, on="date", how="outer")
    return merged.sort_values("date")


def sync_nccpl_flows(con: sqlite3.Connection, days: int = 90) -> dict:
    """Sync FIPI/LIPI flows from khistocks to nccpl_flows_derived table.

    Returns dict with counts.
    """
    from_date = (datetime.now(PKT) - timedelta(days=days)).strftime("%Y-%m-%d")
    to_date = datetime.now(PKT).strftime("%Y-%m-%d")

    # Ensure columns exist on legacy table
    existing_cols = {c[1] for c in con.execute("PRAGMA table_info(nccpl_flows_derived)").fetchall()}
    if not existing_cols:
        con.execute("""CREATE TABLE nccpl_flows_derived (
            date TEXT PRIMARY KEY,
            fpi_net_4w_mn REAL,
            mf_net_4w_mn REAL,
            source TEXT
        )""")
    else:
        for col, ctype in [("fpi_net_4w_mn", "REAL"), ("mf_net_4w_mn", "REAL"), ("source", "TEXT")]:
            if col not in existing_cols:
                con.execute(f"ALTER TABLE nccpl_flows_derived ADD COLUMN {col} {ctype}")

    daily = fetch_daily_net(from_date, to_date)
    if daily.empty:
        return {"fipi": 0, "lipi": 0, "derived": 0}

    # Compute 4-week rolling sums for HMM features
    daily = daily.sort_values("date")
    if "fipi_net_mn" in daily.columns:
        daily["fpi_net_4w_mn"] = daily["fipi_net_mn"].rolling(20, min_periods=1).sum()
    else:
        daily["fpi_net_4w_mn"] = 0
    if "lipi_net_mn" in daily.columns:
        daily["mf_net_4w_mn"] = daily["lipi_net_mn"].rolling(20, min_periods=1).sum()
    else:
        daily["mf_net_4w_mn"] = 0

    upserted = 0
    for _, row in daily.iterrows():
        # Check if row exists
        existing = con.execute(
            "SELECT 1 FROM nccpl_flows_derived WHERE date = ?", (row["date"],)
        ).fetchone()
        if existing:
            con.execute("""
                UPDATE nccpl_flows_derived SET fpi_net_4w_mn=?, mf_net_4w_mn=?, source='khistocks'
                WHERE date=?
            """, (row.get("fpi_net_4w_mn", 0), row.get("mf_net_4w_mn", 0), row["date"]))
        else:
            con.execute("""
                INSERT INTO nccpl_flows_derived (date, fpi_net_4w_mn, mf_net_4w_mn, source)
                VALUES (?, ?, ?, 'khistocks')
            """, (row["date"], row.get("fpi_net_4w_mn", 0), row.get("mf_net_4w_mn", 0)))
        upserted += 1

    con.commit()
    return {
        "fipi": len(daily[daily.get("fipi_net_mn", 0) != 0]) if "fipi_net_mn" in daily.columns else 0,
        "lipi": len(daily[daily.get("lipi_net_mn", 0) != 0]) if "lipi_net_mn" in daily.columns else 0,
        "derived": upserted,
    }
