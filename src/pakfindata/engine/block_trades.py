"""Block / Off-Market Trade Analysis Engine.

Parses PSX off-market summary CSVs and computes institutional activity metrics.

Data source: /mnt/e/psxdata/downloads/daily/{date}/off_market/off_market_summary_*.csv
"""

from __future__ import annotations

import glob
import re
from pathlib import Path

import pandas as pd

_DOWNLOAD_DIR = Path("/mnt/e/psxdata/downloads/daily")


def _parse_off_market_csv(filepath: str | Path) -> pd.DataFrame:
    """Parse PSX off-market summary CSV (multi-section format).

    The file has multiple sections separated by blank lines with headers like:
      MEMBER , TO , MEMBER
      CROSS ,TRANSACTIONS, BETWEEN, CLIENT TO ,CLIENT...

    Each section has: Date, Settlement Date, Member Code, Symbol Code, Company, Turnover, Rate, Values
    """
    rows = []
    with open(filepath, "r", errors="replace") as f:
        lines = f.readlines()

    in_data = False
    for line in lines:
        line = line.strip()
        if not line:
            in_data = False
            continue

        # Detect header row
        if line.startswith("Date "):
            in_data = True
            continue

        # Skip section titles
        if any(kw in line.upper() for kw in ["OFF MARKET", "MEMBER", "CROSS", "TRANSACTION", "CLIENT"]):
            continue

        if not in_data:
            continue

        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 8:
            continue

        try:
            turnover = int(re.sub(r"[^\d]", "", parts[5])) if parts[5].strip() else 0
            rate = float(parts[6]) if parts[6].strip() else 0
            value = int(re.sub(r"[^\d]", "", parts[7])) if parts[7].strip() else 0
        except (ValueError, IndexError):
            continue

        rows.append({
            "date": parts[0].strip(),
            "settlement_date": parts[1].strip(),
            "member_code": parts[2].strip(),
            "symbol": parts[3].strip(),
            "company": parts[4].strip(),
            "turnover": turnover,
            "rate": rate,
            "value": value,
        })

    return pd.DataFrame(rows)


def load_off_market(date_str: str) -> pd.DataFrame:
    """Load off-market transactions for a given date."""
    pattern = str(_DOWNLOAD_DIR / date_str / "off_market" / f"off_market_summary_*.csv")
    files = glob.glob(pattern)
    if not files:
        return pd.DataFrame()
    return _parse_off_market_csv(files[0])


def analyze_blocks(date_str: str, eod_df: pd.DataFrame | None = None) -> pd.DataFrame:
    """Analyze off-market trades — aggregate by symbol with volume comparison.

    Args:
        date_str: Date string YYYY-MM-DD
        eod_df: Optional DataFrame with columns [symbol, volume] for regular market volume

    Returns:
        DataFrame with columns: symbol, block_vol, block_value, block_trades,
        avg_rate, regular_vol, block_pct
    """
    df = load_off_market(date_str)
    if df.empty:
        return pd.DataFrame()

    agg = df.groupby("symbol").agg(
        block_vol=("turnover", "sum"),
        block_value=("value", "sum"),
        block_trades=("turnover", "count"),
        avg_rate=("rate", "mean"),
    ).reset_index()

    # Merge with regular volume if available
    if eod_df is not None and not eod_df.empty:
        vol_map = eod_df.set_index("symbol")["volume"].to_dict() if "volume" in eod_df.columns else {}
        agg["regular_vol"] = agg["symbol"].map(vol_map).fillna(0)
        total = agg["block_vol"] + agg["regular_vol"]
        agg["block_pct"] = (agg["block_vol"] / total.clip(lower=1) * 100).round(2)
    else:
        agg["regular_vol"] = 0
        agg["block_pct"] = 0.0

    return agg.sort_values("block_vol", ascending=False)


def block_trade_history(symbol: str, days: int = 30) -> pd.DataFrame:
    """Load block trade history for a symbol across multiple days."""
    date_dirs = sorted(glob.glob(str(_DOWNLOAD_DIR / "*/off_market/")))
    rows = []
    for d in date_dirs[-days:]:
        csv_files = glob.glob(str(Path(d) / "off_market_summary_*.csv"))
        if not csv_files:
            continue
        date_str = Path(d).parent.name
        df = _parse_off_market_csv(csv_files[0])
        sym_df = df[df["symbol"] == symbol]
        if not sym_df.empty:
            rows.append({
                "date": date_str,
                "block_vol": sym_df["turnover"].sum(),
                "block_value": sym_df["value"].sum(),
                "block_trades": len(sym_df),
                "avg_rate": sym_df["rate"].mean(),
            })

    return pd.DataFrame(rows)


def block_trade_score(symbol: str, date_str: str, regular_volume: float = 0) -> float:
    """Score 0-100 based on off-market activity.

    0-30:   No block trades
    30-60:  Small blocks (<10% of volume)
    60-80:  Significant blocks (10-30%)
    80-100: Very heavy blocks (>30%)
    """
    df = load_off_market(date_str)
    if df.empty:
        return 15.0  # baseline — no data

    sym_df = df[df["symbol"] == symbol]
    if sym_df.empty:
        return 15.0

    block_vol = sym_df["turnover"].sum()
    if regular_volume > 0:
        block_pct = block_vol / regular_volume * 100
    else:
        block_pct = 5.0  # assume small if no reference

    if block_pct > 30:
        return min(100, 80 + block_pct - 30)
    elif block_pct > 10:
        return 60 + (block_pct - 10)
    elif block_pct > 0:
        return 30 + block_pct * 3
    return 15.0


def get_available_dates() -> list[str]:
    """Return dates that have off-market data, newest first."""
    pattern = str(_DOWNLOAD_DIR / "*/off_market/off_market_summary_*.csv")
    files = sorted(glob.glob(pattern))
    dates = []
    for f in files:
        date_str = Path(f).parent.parent.name
        if date_str not in dates:
            dates.append(date_str)
    return sorted(dates, reverse=True)
