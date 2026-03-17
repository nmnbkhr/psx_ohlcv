# PMEX Margins — Direct Download URL Pattern (CONFIRMED)

## URL Pattern
```
https://pmex.com.pk/wp-content/uploads/{YYYY}/{MM}/Margins-{DD}-{MM}-{YYYY}.xlsx
```

### Examples
```
https://pmex.com.pk/wp-content/uploads/2026/02/Margins-27-02-2026.xlsx  ✅ (confirmed)
https://pmex.com.pk/wp-content/uploads/2025/12/Margins-31-12-2025.xlsx
https://pmex.com.pk/wp-content/uploads/2024/06/Margins-03-06-2024.xlsx
```

### Rules
- **Trading days only** — Skips Saturdays, Sundays
- **Skips Pakistan public holidays** (Eid ul-Fitr, Eid ul-Adha, 23 March, 14 Aug, etc.)
- **404** for non-trading days → just skip and continue
- **wp-content/uploads/** served by web server directly → NO WAF blocking → plain `requests` works

---

## Implementation: fetcher_pmex_margins.py

```python
"""
pakfindata/commodities/fetcher_pmex_margins.py

Direct download of PMEX daily margins Excel files.
URL pattern: https://pmex.com.pk/wp-content/uploads/YYYY/MM/Margins-DD-MM-YYYY.xlsx

No login, no API key, no Selenium. Just requests.get().
"""

import requests
import pandas as pd
from io import BytesIO
from datetime import date, timedelta
from pathlib import Path
import time
import logging

logger = logging.getLogger(__name__)

BASE_URL = "https://pmex.com.pk/wp-content/uploads"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


def margins_url(dt: date) -> str:
    """Build PMEX margins download URL for a given date."""
    return f"{BASE_URL}/{dt.year}/{dt.month:02d}/Margins-{dt.day:02d}-{dt.month:02d}-{dt.year}.xlsx"


def fetch_margins_file(dt: date, save_dir: str = None) -> pd.ExcelFile | None:
    """
    Download PMEX margins file for a specific date.
    Returns pd.ExcelFile object or None if not available (weekend/holiday/404).
    """
    url = margins_url(dt)
    
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        
        if resp.status_code == 200:
            content_type = resp.headers.get("Content-Type", "")
            if "spreadsheet" in content_type or "excel" in content_type or "octet-stream" in content_type or len(resp.content) > 1000:
                # Optionally save raw file
                if save_dir:
                    path = Path(save_dir) / f"Margins-{dt.day:02d}-{dt.month:02d}-{dt.year}.xlsx"
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(resp.content)
                    logger.info(f"Saved: {path}")
                
                return pd.ExcelFile(BytesIO(resp.content))
            else:
                logger.debug(f"{dt}: Got 200 but not Excel (probably HTML error page)")
                return None
        elif resp.status_code == 404:
            logger.debug(f"{dt}: 404 — not a trading day")
            return None
        else:
            logger.warning(f"{dt}: HTTP {resp.status_code}")
            return None
    except Exception as e:
        logger.error(f"{dt}: {e}")
        return None


def parse_margin_file(xls: pd.ExcelFile, report_date: date) -> pd.DataFrame:
    """
    Parse both sheets of a PMEX margins Excel file into a single DataFrame.
    
    Sheet 1 "Margin File": Product Groups | Contract Code | Reference Price | 
        Initial Magin | Initial Margin Value | WCM | Maintenance Margin | 
        Lower Limit | Upper Limit | FX Rate
    
    Sheet 2 "Agri Margin": Contract Code | Reference Price | Initial Margin | 
        Initial Margin Value | WCM | Maintenance Margin | Lower Limit | 
        Upper Limit | FX Rate
    """
    all_rows = []
    
    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet_name, header=4)
        df.columns = [str(c).strip() for c in df.columns]
        
        # Drop unnamed columns and empty rows
        df = df[[c for c in df.columns if not c.startswith("Unnamed")]]
        df = df.dropna(how="all")
        
        # Normalize column names
        col_map = {}
        for c in df.columns:
            cl = c.lower().replace(" ", "_")
            if "product" in cl:
                col_map[c] = "product_group"
            elif "contract" in cl:
                col_map[c] = "contract_code"
            elif "reference" in cl:
                col_map[c] = "reference_price"
            elif "initial" in cl and "value" in cl:
                col_map[c] = "initial_margin_value_pkr"
            elif "initial" in cl and ("margin" in cl or "magin" in cl):
                col_map[c] = "initial_margin_pct"
            elif cl == "wcm":
                col_map[c] = "wcm"
            elif "maintenance" in cl:
                col_map[c] = "maintenance_margin"
            elif "lower" in cl:
                col_map[c] = "lower_limit"
            elif "upper" in cl:
                col_map[c] = "upper_limit"
            elif "fx" in cl:
                col_map[c] = "fx_rate"
        
        df = df.rename(columns=col_map)
        
        # Forward-fill product groups (merged cells in Sheet 1)
        if "product_group" in df.columns:
            df["product_group"] = df["product_group"].ffill()
        else:
            df["product_group"] = None
        
        # Filter to rows with valid contract codes
        df = df[df["contract_code"].notna()].copy()
        
        # Mark active vs expired (#N/A)
        df["is_active"] = df["reference_price"].apply(
            lambda x: x != "#N/A" and pd.notna(x)
        )
        
        # Convert numeric columns for active rows
        for col in ["reference_price", "initial_margin_pct", "initial_margin_value_pkr",
                     "maintenance_margin", "lower_limit", "upper_limit", "fx_rate"]:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(",", "").replace("#N/A", ""),
                    errors="coerce"
                )
        
        # Handle WCM (can be numeric or "-")
        if "wcm" in df.columns:
            df["wcm"] = pd.to_numeric(
                df["wcm"].astype(str).str.replace("-", "").replace(",", ""),
                errors="coerce"
            )
        
        df["report_date"] = report_date
        df["sheet_name"] = sheet_name
        
        # Clean product_group: remove footer text
        if "product_group" in df.columns:
            footer_keywords = ["UAN:", "YOUR FUTURES", "Copyrights", "pmex.com.pk"]
            df = df[~df["product_group"].astype(str).str.contains("|".join(footer_keywords), na=False)]
        
        all_rows.append(df)
    
    if all_rows:
        result = pd.concat(all_rows, ignore_index=True)
        # Ensure consistent column order
        cols = ["report_date", "sheet_name", "product_group", "contract_code",
                "reference_price", "initial_margin_pct", "initial_margin_value_pkr",
                "wcm", "maintenance_margin", "lower_limit", "upper_limit", 
                "fx_rate", "is_active"]
        return result[[c for c in cols if c in result.columns]]
    
    return pd.DataFrame()


def backfill_margins(
    start_date: date = date(2023, 1, 1),
    end_date: date = None,
    save_dir: str = None,
    delay: float = 0.5,
) -> pd.DataFrame:
    """
    Backfill PMEX margins data over a date range.
    Skips weekends automatically. 404s for holidays are handled gracefully.
    
    With ~250 trading days/year and 0.5s delay:
      1 year  = ~125 seconds (~2 min)
      3 years = ~375 seconds (~6 min)
    """
    if end_date is None:
        end_date = date.today()
    
    all_data = []
    current = start_date
    success_count = 0
    skip_count = 0
    
    while current <= end_date:
        # Skip weekends
        if current.weekday() >= 5:  # Sat=5, Sun=6
            current += timedelta(days=1)
            continue
        
        xls = fetch_margins_file(current, save_dir=save_dir)
        
        if xls is not None:
            df = parse_margin_file(xls, current)
            if not df.empty:
                all_data.append(df)
                active = df["is_active"].sum() if "is_active" in df.columns else len(df)
                success_count += 1
                logger.info(f"✅ {current}: {active} active contracts")
        else:
            skip_count += 1
            logger.debug(f"⏭️  {current}: skipped (holiday/missing)")
        
        current += timedelta(days=1)
        time.sleep(delay)
    
    logger.info(f"Done: {success_count} trading days, {skip_count} skipped")
    
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()


# ── CLI entry points ──

def sync_today(save_dir: str = None) -> pd.DataFrame:
    """Fetch today's margins (or most recent trading day)."""
    today = date.today()
    # Try today, then go back up to 5 days to find last trading day
    for i in range(5):
        dt = today - timedelta(days=i)
        xls = fetch_margins_file(dt, save_dir=save_dir)
        if xls is not None:
            return parse_margin_file(xls, dt)
    return pd.DataFrame()


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    
    if len(sys.argv) > 1 and sys.argv[1] == "backfill":
        start = date.fromisoformat(sys.argv[2]) if len(sys.argv) > 2 else date(2024, 1, 1)
        df = backfill_margins(start_date=start)
        print(f"\nTotal rows: {len(df)}")
        print(f"Date range: {df['report_date'].min()} to {df['report_date'].max()}")
        print(f"Unique contracts: {df['contract_code'].nunique()}")
    else:
        df = sync_today()
        if not df.empty:
            print(f"Date: {df['report_date'].iloc[0]}")
            print(f"Active contracts: {df['is_active'].sum()}")
            print(df[df['is_active']].to_string(index=False))
```

---

## Backfill Economics

| Range | Trading Days | Requests | Time (0.5s delay) | Data Size |
|-------|-------------|----------|--------------------|-----------|
| 1 month | ~22 | 30 (incl weekends 404) | ~15 sec | ~22 × 148 = 3,256 rows |
| 6 months | ~130 | ~180 | ~90 sec | ~19,240 rows |
| 1 year | ~250 | ~365 | ~3 min | ~37,000 rows |
| 3 years (2023-2026) | ~750 | ~1,095 | ~9 min | ~111,000 rows |

**Key question to determine**: How far back do files exist? Test with:
```
https://pmex.com.pk/wp-content/uploads/2020/01/Margins-02-01-2020.xlsx
https://pmex.com.pk/wp-content/uploads/2019/01/Margins-02-01-2019.xlsx
https://pmex.com.pk/wp-content/uploads/2018/01/Margins-02-01-2018.xlsx
```

---

## Updated Source Priority — FINAL FINAL

```
TIER 0A: PMEX OHLC Report     — OHLCV + Settlement + FX (FREE, requests/Selenium)
  └── mportal.pmex.com.pk/mt5bonew/Home/OHLCReport
  └── 3-month chunks, ~25 requests for full backfill

TIER 0B: PMEX Margins File    — Risk + Metadata + Settlement + Circuit Breakers (FREE, requests)
  └── pmex.com.pk/wp-content/uploads/YYYY/MM/Margins-DD-MM-YYYY.xlsx
  └── Direct URL, 1 request per trading day, ~148 contracts/day
  └── NO login, NO Selenium, NO WAF — just requests.get()

TIER 1: yfinance              — 30 international daily OHLCV tickers (FREE)
TIER 2: FRED API              — 20+ monthly series (FREE key)
TIER 3: World Bank Pink Sheet  — 70+ monthly (FREE download)
TIER 4: khistocks.com          — 7 daily PKR feeds (Selenium)
  4a. Lahore Akbari Mandi (30+ food staples — UNIQUE)
  4b. Karachi Bullion (Sarafa gold PKR/Tola)
  4c. LME, Cotton, Intl Bullion, FX rates
TIER 5: GoldPriceZ / SBP      — Supplementary
TIER 6: PMEX MT5 Demo          — Intraday/tick data (Windows only)
TIER 7: PMEX mwatchnew         — Real-time snapshot (Selenium)
```
