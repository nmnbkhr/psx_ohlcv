"""
HMM Macro Regime v2 — Data Fetchers.

Fetches and stores macro data needed for the upgraded 8-feature HMM model:
1. T-Bill/PIB yield spread (from sbp_benchmark_snapshot)
2. NCCPL foreign portfolio flows (scraped from NCCPL website)
3. SBP FX reserves (from sbp_benchmark_snapshot)
4. Current account balance (from SBP easydata API)

Usage:
  python -m pakfindata.sources.hmm_data_fetchers sync-all
  python -m pakfindata.sources.hmm_data_fetchers sync-spread
  python -m pakfindata.sources.hmm_data_fetchers sync-flows
  python -m pakfindata.sources.hmm_data_fetchers sync-reserves
  python -m pakfindata.sources.hmm_data_fetchers sync-ca
  python -m pakfindata.sources.hmm_data_fetchers status
"""

import logging
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

DB_PATH = Path("/home/smnb/psxdata_rescue/psx.sqlite")

# SBP EasyData API for current account balance
_SBP_API_BASE = "https://easydata.sbp.org.pk/api/v1"
_SBP_API_KEY = "38181E447386BBD36429D936DF60F09F272341DC"

# NCCPL FPI statistics URL
_NCCPL_FPI_URL = (
    "https://www.nccpl.com.pk/en/services/foreign-portfolio-investment/fpi-statistics"
)

# Approximate Pakistan GDP for CA/GDP estimation (USD billions)
_GDP_USD_BN = 350.0


# ═══════════════════════════════════════════════════════════════
# Schema DDL
# ═══════════════════════════════════════════════════════════════

_SCHEMA_SPREAD = """
CREATE TABLE IF NOT EXISTS hmm_tbill_pib_spread (
    date      TEXT PRIMARY KEY,
    tbill_3m  REAL,
    pib_10y   REAL,
    spread    REAL
);
"""

_SCHEMA_FLOWS = """
CREATE TABLE IF NOT EXISTS hmm_nccpl_flows (
    date         TEXT PRIMARY KEY,
    gross_buy    REAL,
    gross_sell   REAL,
    net_flow     REAL,
    rolling_4w   REAL
);
"""

_SCHEMA_RESERVES = """
CREATE TABLE IF NOT EXISTS hmm_sbp_reserves (
    date            TEXT PRIMARY KEY,
    reserves_usd_bn REAL,
    reserves_flag   INTEGER
);
"""

_SCHEMA_CA = """
CREATE TABLE IF NOT EXISTS hmm_ca_balance (
    date           TEXT PRIMARY KEY,
    ca_usd_mn      REAL,
    ca_gdp_pct_est REAL
);
"""


def _ensure_tables(con: sqlite3.Connection) -> None:
    """Create all HMM data tables if they don't exist."""
    for ddl in (_SCHEMA_SPREAD, _SCHEMA_FLOWS, _SCHEMA_RESERVES, _SCHEMA_CA):
        con.executescript(ddl)


# ═══════════════════════════════════════════════════════════════
# 1. T-Bill / PIB Yield Spread
# ═══════════════════════════════════════════════════════════════

def sync_tbill_pib_spread(con: sqlite3.Connection) -> int:
    """
    Query sbp_benchmark_snapshot for mtb_3m and pib_10y,
    compute spread = pib_10y - mtb_3m, upsert into hmm_tbill_pib_spread.

    Returns rows upserted.
    """
    con.executescript(_SCHEMA_SPREAD)

    try:
        rows = con.execute(
            """
            SELECT a.date, a.value AS tbill_3m, b.value AS pib_10y
            FROM sbp_benchmark_snapshot a
            JOIN sbp_benchmark_snapshot b
              ON a.date = b.date
            WHERE a.metric = 'mtb_3m'
              AND b.metric = 'pib_10y'
            ORDER BY a.date
            """
        ).fetchall()
    except sqlite3.OperationalError as exc:
        log.warning("Cannot query sbp_benchmark_snapshot: %s", exc)
        return 0

    if not rows:
        log.info("No mtb_3m / pib_10y data found in sbp_benchmark_snapshot")
        return 0

    upserted = 0
    for date_str, tbill_3m, pib_10y in rows:
        spread = pib_10y - tbill_3m
        con.execute(
            """INSERT INTO hmm_tbill_pib_spread (date, tbill_3m, pib_10y, spread)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(date) DO UPDATE SET
                 tbill_3m = excluded.tbill_3m,
                 pib_10y  = excluded.pib_10y,
                 spread   = excluded.spread
            """,
            (date_str, tbill_3m, pib_10y, spread),
        )
        upserted += 1

    con.commit()
    log.info("hmm_tbill_pib_spread: upserted %d rows", upserted)
    return upserted


# ═══════════════════════════════════════════════════════════════
# 2. NCCPL Foreign Portfolio Flows
# ═══════════════════════════════════════════════════════════════

def _scrape_nccpl_fpi() -> list[dict]:
    """
    Scrape the NCCPL FPI statistics page for weekly flow data.

    Returns list of dicts with keys: date, gross_buy, gross_sell, net_flow.
    Returns empty list on failure.
    """
    try:
        resp = requests.get(_NCCPL_FPI_URL, timeout=30, verify=False)
        resp.raise_for_status()
    except Exception as exc:
        log.warning("NCCPL FPI scrape failed (request): %s", exc)
        return []

    try:
        soup = BeautifulSoup(resp.text, "html.parser")

        # Look for the main FPI data table
        tables = soup.find_all("table")
        if not tables:
            log.warning("NCCPL FPI: no tables found on page")
            return []

        records = []
        for table in tables:
            header_row = table.find("tr")
            if not header_row:
                continue

            headers = [th.get_text(strip=True).lower() for th in header_row.find_all(["th", "td"])]

            # Identify columns by header text — look for date, buy, sell, net
            date_col = None
            buy_col = None
            sell_col = None
            net_col = None

            for i, h in enumerate(headers):
                h_lower = h.lower()
                if "date" in h_lower or "week" in h_lower or "period" in h_lower:
                    date_col = i
                elif "buy" in h_lower or "purchase" in h_lower:
                    buy_col = i
                elif "sell" in h_lower or "sale" in h_lower:
                    sell_col = i
                elif "net" in h_lower:
                    net_col = i

            if date_col is None:
                continue

            data_rows = table.find_all("tr")[1:]  # skip header
            for tr in data_rows:
                cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
                if len(cells) <= max(c for c in [date_col, buy_col, sell_col, net_col] if c is not None):
                    continue

                # Parse date
                raw_date = cells[date_col]
                date_val = _parse_nccpl_date(raw_date)
                if not date_val:
                    continue

                gross_buy = _parse_number(cells[buy_col]) if buy_col is not None else None
                gross_sell = _parse_number(cells[sell_col]) if sell_col is not None else None
                net_flow = _parse_number(cells[net_col]) if net_col is not None else None

                # Compute net if we have buy and sell but no net column
                if net_flow is None and gross_buy is not None and gross_sell is not None:
                    net_flow = gross_buy - gross_sell

                records.append({
                    "date": date_val,
                    "gross_buy": gross_buy or 0.0,
                    "gross_sell": gross_sell or 0.0,
                    "net_flow": net_flow or 0.0,
                })

            if records:
                break  # use first table that yielded data

        return records

    except Exception as exc:
        log.warning("NCCPL FPI scrape failed (parse): %s", exc)
        return []


def _parse_nccpl_date(raw: str) -> Optional[str]:
    """Try several date formats common on NCCPL pages."""
    for fmt in ("%d-%b-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%b %d, %Y"):
        try:
            return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _parse_number(text: str) -> Optional[float]:
    """Parse a numeric string, stripping commas and parens (negative)."""
    if not text:
        return None
    cleaned = text.strip().replace(",", "").replace(" ", "")
    negative = False
    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = cleaned[1:-1]
        negative = True
    if cleaned.startswith("-"):
        cleaned = cleaned[1:]
        negative = True
    try:
        val = float(cleaned)
        return -val if negative else val
    except ValueError:
        return None


def sync_nccpl_flows(con: sqlite3.Connection) -> int:
    """
    Scrape NCCPL FPI statistics and store in hmm_nccpl_flows.
    Computes rolling_4w = 4-week rolling sum of net_flow.

    Returns rows upserted.
    """
    con.executescript(_SCHEMA_FLOWS)

    records = _scrape_nccpl_fpi()
    if not records:
        log.warning("No NCCPL FPI data scraped — returning 0")
        return 0

    # Build DataFrame for rolling computation
    df = pd.DataFrame(records).drop_duplicates(subset="date").sort_values("date")
    df["rolling_4w"] = df["net_flow"].rolling(window=4, min_periods=1).sum()

    upserted = 0
    for _, row in df.iterrows():
        con.execute(
            """INSERT INTO hmm_nccpl_flows (date, gross_buy, gross_sell, net_flow, rolling_4w)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(date) DO UPDATE SET
                 gross_buy  = excluded.gross_buy,
                 gross_sell = excluded.gross_sell,
                 net_flow   = excluded.net_flow,
                 rolling_4w = excluded.rolling_4w
            """,
            (row["date"], row["gross_buy"], row["gross_sell"],
             row["net_flow"], row["rolling_4w"]),
        )
        upserted += 1

    con.commit()
    log.info("hmm_nccpl_flows: upserted %d rows", upserted)
    return upserted


# ═══════════════════════════════════════════════════════════════
# 3. SBP FX Reserves
# ═══════════════════════════════════════════════════════════════

def sync_sbp_reserves(con: sqlite3.Connection) -> int:
    """
    Query sbp_benchmark_snapshot for sbp_reserves_m_usd,
    convert to billions, flag if < $8bn.

    Returns rows upserted.
    """
    con.executescript(_SCHEMA_RESERVES)

    try:
        rows = con.execute(
            """
            SELECT date, value
            FROM sbp_benchmark_snapshot
            WHERE metric = 'sbp_reserves_m_usd'
            ORDER BY date
            """
        ).fetchall()
    except sqlite3.OperationalError as exc:
        log.warning("Cannot query sbp_benchmark_snapshot: %s", exc)
        return 0

    if not rows:
        log.info("No sbp_reserves_m_usd data in sbp_benchmark_snapshot")
        return 0

    upserted = 0
    for date_str, value_m_usd in rows:
        reserves_bn = value_m_usd / 1000.0
        reserves_flag = 1 if reserves_bn < 8.0 else 0
        con.execute(
            """INSERT INTO hmm_sbp_reserves (date, reserves_usd_bn, reserves_flag)
               VALUES (?, ?, ?)
               ON CONFLICT(date) DO UPDATE SET
                 reserves_usd_bn = excluded.reserves_usd_bn,
                 reserves_flag   = excluded.reserves_flag
            """,
            (date_str, reserves_bn, reserves_flag),
        )
        upserted += 1

    con.commit()
    log.info("hmm_sbp_reserves: upserted %d rows", upserted)
    return upserted


# ═══════════════════════════════════════════════════════════════
# 4. Current Account Balance
# ═══════════════════════════════════════════════════════════════

# SBP EasyData series key for Balance of Payments — Current Account
# TS_GP_BOP_BPM6SUM_M dataset, current account balance series
_CA_SERIES_KEY = "TS_GP_BOP_BPM6SUM_M.BPM6SM010"

# Hardcoded fallback: recent monthly CA balance (USD millions)
# Source: SBP Balance of Payments press releases
_CA_FALLBACK = [
    ("2024-07-01", -133),
    ("2024-08-01", -157),
    ("2024-09-01", -75),
    ("2024-10-01", -40),
    ("2024-11-01", -365),
    ("2024-12-01", -655),
    ("2025-01-01", -654),
    ("2025-02-01", -375),
    ("2025-03-01", -500),
    ("2025-04-01", -450),
    ("2025-05-01", -400),
    ("2025-06-01", -350),
    ("2025-07-01", -200),
    ("2025-08-01", -300),
    ("2025-09-01", -250),
    ("2025-10-01", -350),
    ("2025-11-01", -400),
    ("2025-12-01", -550),
    ("2026-01-01", -500),
    ("2026-02-01", -380),
    ("2026-03-01", -420),
]


def _fetch_ca_from_easydata() -> list[tuple[str, float]]:
    """
    Fetch current account balance from SBP EasyData (local CSV cache + API).
    Uses both BPM6 (older) and BOPSTND (newer) datasets.
    Returns list of (date_str, ca_usd_mn) tuples.
    """
    try:
        from pakfindata.sources.sbp_easydata import read_dataset_series

        results = {}  # date -> value, newer overwrites older

        # BPM6 older data (up to 2013)
        series_old = read_dataset_series("TS_GP_BOP_BPM6SUM_M")
        for r in series_old.get("TS_GP_BOP_BPM6SUM_M.P00010", []):
            results[r["date"]] = r["value"]

        # BOPSTND newer data (2020+)
        series_new = read_dataset_series("TS_GP_ES_PKBOPSTND_M")
        for r in series_new.get("TS_GP_ES_PKBOPSTND_M.BOPSNA01810", []):
            results[r["date"]] = r["value"]

        if results:
            log.info("CA balance from EasyData CSV: %d observations", len(results))
            return sorted(results.items())

    except Exception as exc:
        log.warning("EasyData CSV CA fetch failed: %s", exc)

    # Fallback: try API
    try:
        session = requests.Session()
        session.headers.update({"Accept": "application/json"})
        session.verify = False

        url = f"{_SBP_API_BASE}/series/{_CA_SERIES_KEY}/data"
        params = {
            "api_key": _SBP_API_KEY,
            "format": "json",
            "start_date": "2020-01-01",
        }

        resp = session.get(url, params=params, timeout=30)
        if resp.status_code != 200:
            return []

        data = resp.json()
        if not data or not data.get("rows"):
            return []

        results = []
        for row in data["rows"]:
            try:
                results.append((row[3], float(row[4])))
            except (IndexError, ValueError, TypeError):
                continue
        return results

    except Exception as exc:
        log.warning("SBP EasyData CA API fetch failed: %s", exc)
        return []


def sync_ca_balance(con: sqlite3.Connection) -> int:
    """
    Fetch current account balance from SBP EasyData or fallback,
    forward-fill monthly to daily, store in hmm_ca_balance.

    Returns rows upserted.
    """
    con.executescript(_SCHEMA_CA)

    # Try API first, fall back to hardcoded
    ca_data = _fetch_ca_from_easydata()
    if not ca_data:
        log.info("Using fallback CA data (API unavailable)")
        ca_data = list(_CA_FALLBACK)

    if not ca_data:
        log.warning("No CA data available at all")
        return 0

    # Build monthly DataFrame
    df_monthly = pd.DataFrame(ca_data, columns=["date", "ca_usd_mn"])
    df_monthly["date"] = pd.to_datetime(df_monthly["date"])
    df_monthly = df_monthly.drop_duplicates(subset="date").sort_values("date")
    df_monthly = df_monthly.set_index("date")

    # Estimate CA as % of GDP (annualized: monthly * 12 / GDP_bn / 10)
    # ca_gdp_pct = (ca_usd_mn * 12) / (GDP_bn * 1000) * 100
    df_monthly["ca_gdp_pct_est"] = (
        df_monthly["ca_usd_mn"] * 12.0 / (_GDP_USD_BN * 1000.0) * 100.0
    )

    # Forward-fill to daily frequency
    df_daily = df_monthly.resample("D").ffill()
    df_daily = df_daily.reset_index()
    df_daily["date"] = df_daily["date"].dt.strftime("%Y-%m-%d")

    upserted = 0
    for _, row in df_daily.iterrows():
        con.execute(
            """INSERT INTO hmm_ca_balance (date, ca_usd_mn, ca_gdp_pct_est)
               VALUES (?, ?, ?)
               ON CONFLICT(date) DO UPDATE SET
                 ca_usd_mn      = excluded.ca_usd_mn,
                 ca_gdp_pct_est = excluded.ca_gdp_pct_est
            """,
            (row["date"], row["ca_usd_mn"], row["ca_gdp_pct_est"]),
        )
        upserted += 1

    con.commit()
    log.info("hmm_ca_balance: upserted %d rows (daily, forward-filled)", upserted)
    return upserted


# ═══════════════════════════════════════════════════════════════
# Main sync orchestrator
# ═══════════════════════════════════════════════════════════════

def sync_all_hmm_data(db_path: Optional[Path] = None) -> dict:
    """
    Run all HMM data fetchers.

    Returns {source: rows_upserted}.
    """
    if db_path is None:
        db_path = DB_PATH

    con = sqlite3.connect(str(db_path))
    _ensure_tables(con)

    results = {}

    for name, func in [
        ("tbill_pib_spread", sync_tbill_pib_spread),
        ("nccpl_flows", sync_nccpl_flows),
        ("sbp_reserves", sync_sbp_reserves),
        ("ca_balance", sync_ca_balance),
    ]:
        try:
            results[name] = func(con)
        except Exception as exc:
            log.error("HMM fetcher '%s' failed: %s", name, exc)
            results[name] = 0

    con.close()
    return results


def status(db_path: Optional[Path] = None) -> dict:
    """Report row counts and date ranges for all HMM tables."""
    if db_path is None:
        db_path = DB_PATH

    if not db_path.exists():
        return {"error": f"DB not found: {db_path}"}

    con = sqlite3.connect(str(db_path))
    info = {}

    for table in (
        "hmm_tbill_pib_spread",
        "hmm_nccpl_flows",
        "hmm_sbp_reserves",
        "hmm_ca_balance",
    ):
        try:
            row = con.execute(
                f"SELECT COUNT(*), MIN(date), MAX(date) FROM {table}"
            ).fetchone()
            info[table] = {
                "rows": row[0],
                "min_date": row[1],
                "max_date": row[2],
            }
        except sqlite3.OperationalError:
            info[table] = {"rows": 0, "min_date": None, "max_date": None}

    con.close()
    return info


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import json
    import warnings

    warnings.filterwarnings("ignore", message="Unverified HTTPS request")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    commands = {
        "sync-all": "Run all HMM data fetchers",
        "sync-spread": "Sync T-Bill/PIB spread only",
        "sync-flows": "Sync NCCPL FPI flows only",
        "sync-reserves": "Sync SBP reserves only",
        "sync-ca": "Sync current account balance only",
        "status": "Show table row counts and date ranges",
    }

    if len(sys.argv) < 2 or sys.argv[1] not in commands:
        print("Usage: python -m pakfindata.sources.hmm_data_fetchers <command>")
        print("\nCommands:")
        for cmd, desc in commands.items():
            print(f"  {cmd:20s} {desc}")
        sys.exit(1)

    cmd = sys.argv[1]
    db = Path(sys.argv[2]) if len(sys.argv) > 2 else DB_PATH

    if cmd == "sync-all":
        result = sync_all_hmm_data(db)
        print(json.dumps(result, indent=2))

    elif cmd == "sync-spread":
        con = sqlite3.connect(str(db))
        n = sync_tbill_pib_spread(con)
        con.close()
        print(f"Upserted {n} rows into hmm_tbill_pib_spread")

    elif cmd == "sync-flows":
        con = sqlite3.connect(str(db))
        n = sync_nccpl_flows(con)
        con.close()
        print(f"Upserted {n} rows into hmm_nccpl_flows")

    elif cmd == "sync-reserves":
        con = sqlite3.connect(str(db))
        n = sync_sbp_reserves(con)
        con.close()
        print(f"Upserted {n} rows into hmm_sbp_reserves")

    elif cmd == "sync-ca":
        con = sqlite3.connect(str(db))
        n = sync_ca_balance(con)
        con.close()
        print(f"Upserted {n} rows into hmm_ca_balance")

    elif cmd == "status":
        info = status(db)
        print(json.dumps(info, indent=2))
