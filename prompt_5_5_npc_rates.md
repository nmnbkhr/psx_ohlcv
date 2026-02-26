# Prompt 5.5 -- Naya Pakistan Certificate (NPC) Rates + Cross-Currency Analytics

## Context
You are working on the PSX OHLCV project at `~/psx_ohlcv/`.

CODEBASE CONVENTIONS (MUST FOLLOW):
- Database connection: `connect()` from `db.connection`, NEVER `get_db()`
- Repository files: `db/repositories/`
- UI page files: `ui/page_views/`
- CLI: argparse with hierarchical subparsers, NOT Click
- DB path: `/mnt/e/psxdata/psx.sqlite`

COMPLETED:
- 5.1: global_reference_rates + term_reference_rates tables, SOFR/EFFR scraper, v_sofr_kibor_spread view, FCY columns on FI tables
- 5.2: SONIA scraper (BoE CSV API)
- 5.3: EUSTR scraper (ECB CSV API)
- 5.4: TONA scraper (BoJ)
- Existing: KIBOR rates (kibor_daily), SBP policy rates, FX rates (sbp_fx_interbank, sbp_fx_openmarket), bonds_master, sukuk_master, fi_instruments

## TASK
Add Naya Pakistan Certificate (NPC) rate tracking -- scrape SBP-published rates for conventional NPCs in USD, GBP, EUR, and PKR across all tenors, store historically, and build cross-currency analytics views that join NPC rates with global RFRs (SOFR, SONIA, EUSTR), FX rates, and KIBOR to enable carry trade analysis, FX-adjusted return comparisons, and diaspora investment decision support.

### Why This Matters
NPCs are sovereign instruments issued by Government of Pakistan under Public Debt Act 1944, distributed via Roshan Digital Accounts. With the four data layers we now have (KIBOR, global RFRs, FX rates, and NPC rates), the platform becomes a unique **Pakistan Multi-Asset Financial Intelligence System** capable of analytics no other Pakistani platform offers:
- **Carry trade analysis**: KIBOR vs NPC USD adjusted for FX depreciation
- **FX-adjusted total return**: PKR investment vs FCY NPC over any historical period
- **Covered interest parity**: KIBOR-SOFR spread vs FX forward premium
- **Diaspora decision engine**: NPC GBP vs UK savings at SONIA+spread
- **Yield curve comparison**: NPC USD tenor curve vs US Treasury vs KIBOR

## SESSION STATE
Update `.claude_session_state.md`:
```
Current Phase: 5.5 -- NPC Rates + Cross-Currency Analytics
Status: IN PROGRESS
Branch: feat/npc-rates
```

## GIT
```bash
cd ~/psx_ohlcv
git checkout dev
git pull
git checkout -b feat/npc-rates
```

---

## Step 1 -- Database: `src/psx_ohlcv/db/repositories/npc_rates.py`

Create a new repository module following the same pattern as `db/repositories/global_rates.py`.

### Tables

```sql
-- NPC conventional rates (SBP-published, revised periodically via SRO)
CREATE TABLE IF NOT EXISTS npc_rates (
    date TEXT NOT NULL,               -- date rate was observed/scraped
    effective_date TEXT,              -- SRO effective date (when rate became active)
    currency TEXT NOT NULL,           -- 'USD', 'GBP', 'EUR', 'PKR'
    tenor TEXT NOT NULL,              -- '3M', '6M', '12M', '3Y', '5Y'
    rate REAL NOT NULL,               -- annualized profit rate (e.g. 7.00 for 7.00%)
    certificate_type TEXT NOT NULL DEFAULT 'conventional',  -- 'conventional' or 'islamic'
    sro_reference TEXT,               -- e.g. 'SRO 33(I)2025' (gazette notification)
    source TEXT NOT NULL DEFAULT 'sbp',  -- 'sbp', 'bankalhabib', 'hbl', etc.
    fetched_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (date, currency, tenor, certificate_type)
);

CREATE INDEX IF NOT EXISTS idx_npc_date ON npc_rates(date);
CREATE INDEX IF NOT EXISTS idx_npc_currency ON npc_rates(currency);
CREATE INDEX IF NOT EXISTS idx_npc_tenor ON npc_rates(tenor);
CREATE INDEX IF NOT EXISTS idx_npc_effective ON npc_rates(effective_date);
```

### Cross-Currency Analytics Views

```sql
-- NPC vs Global RFR spread: shows premium NPC offers over risk-free rate
-- e.g. NPC USD 7.00% vs SOFR 4.30% = 2.70% sovereign credit premium
CREATE VIEW IF NOT EXISTS v_npc_vs_rfr_spread AS
SELECT 
    n.date,
    n.currency,
    n.tenor,
    n.rate AS npc_rate,
    n.certificate_type,
    CASE n.currency
        WHEN 'USD' THEN g_usd.rate
        WHEN 'GBP' THEN g_gbp.rate
        WHEN 'EUR' THEN g_eur.rate
        ELSE NULL
    END AS rfr_rate,
    CASE n.currency
        WHEN 'USD' THEN 'SOFR'
        WHEN 'GBP' THEN 'SONIA'
        WHEN 'EUR' THEN 'EUSTR'
        ELSE NULL
    END AS rfr_name,
    ROUND(n.rate - CASE n.currency
        WHEN 'USD' THEN COALESCE(g_usd.rate, 0)
        WHEN 'GBP' THEN COALESCE(g_gbp.rate, 0)
        WHEN 'EUR' THEN COALESCE(g_eur.rate, 0)
        ELSE 0
    END, 4) AS npc_premium_over_rfr
FROM npc_rates n
LEFT JOIN global_reference_rates g_usd 
    ON g_usd.date = n.date AND g_usd.rate_name = 'SOFR' AND g_usd.tenor = 'ON'
LEFT JOIN global_reference_rates g_gbp 
    ON g_gbp.date = n.date AND g_gbp.rate_name = 'SONIA' AND g_gbp.tenor = 'ON'
LEFT JOIN global_reference_rates g_eur 
    ON g_eur.date = n.date AND g_eur.rate_name = 'EUSTR' AND g_eur.tenor = 'ON'
WHERE n.currency IN ('USD', 'GBP', 'EUR')
ORDER BY n.date DESC, n.currency, n.tenor;

-- NPC FCY vs KIBOR: carry trade view
-- Shows spread between PKR deposit (KIBOR) and FCY NPC, plus FX rate for adjustment
CREATE VIEW IF NOT EXISTS v_npc_carry_trade AS
SELECT 
    n.date,
    n.currency AS npc_currency,
    n.tenor,
    n.rate AS npc_rate,
    k.offer AS kibor_offer,
    ROUND(k.offer - n.rate, 4) AS kibor_npc_spread,
    f.selling AS fx_rate_pkr,
    -- Annualized breakeven depreciation: if PKR depreciates more than this,
    -- FCY NPC wins despite lower nominal rate
    ROUND(k.offer - n.rate, 4) AS breakeven_depreciation_pct
FROM npc_rates n
LEFT JOIN kibor_daily k 
    ON k.date = n.date 
    AND k.tenor = CASE n.tenor
        WHEN '3M' THEN '3M'
        WHEN '6M' THEN '6M'
        WHEN '12M' THEN '12M'
        ELSE '12M'  -- use 12M KIBOR for 3Y/5Y comparison
    END
LEFT JOIN sbp_fx_interbank f
    ON f.date = n.date 
    AND f.currency = CASE n.currency
        WHEN 'USD' THEN 'USD'
        WHEN 'GBP' THEN 'GBP'
        WHEN 'EUR' THEN 'EUR'
        ELSE 'USD'
    END
WHERE n.currency IN ('USD', 'GBP', 'EUR')
  AND n.certificate_type = 'conventional'
ORDER BY n.date DESC, n.currency, n.tenor;

-- NPC yield curve: all tenors for a currency on a given date
CREATE VIEW IF NOT EXISTS v_npc_yield_curve AS
SELECT 
    n.date,
    n.currency,
    n.certificate_type,
    MAX(CASE WHEN n.tenor = '3M' THEN n.rate END) AS rate_3m,
    MAX(CASE WHEN n.tenor = '6M' THEN n.rate END) AS rate_6m,
    MAX(CASE WHEN n.tenor = '12M' THEN n.rate END) AS rate_12m,
    MAX(CASE WHEN n.tenor = '3Y' THEN n.rate END) AS rate_3y,
    MAX(CASE WHEN n.tenor = '5Y' THEN n.rate END) AS rate_5y
FROM npc_rates n
GROUP BY n.date, n.currency, n.certificate_type
ORDER BY n.date DESC, n.currency;

-- Comprehensive multi-currency dashboard: latest NPC + RFR + KIBOR + FX
CREATE VIEW IF NOT EXISTS v_multicurrency_dashboard AS
SELECT 
    n.date,
    n.currency,
    n.tenor,
    n.rate AS npc_rate,
    -- Matching global RFR
    CASE n.currency
        WHEN 'USD' THEN g_usd.rate
        WHEN 'GBP' THEN g_gbp.rate
        WHEN 'EUR' THEN g_eur.rate
    END AS global_rfr,
    -- KIBOR for PKR comparison
    k.offer AS kibor_offer,
    -- FX rate
    f.selling AS fx_rate_pkr,
    -- Spreads
    ROUND(n.rate - COALESCE(
        CASE n.currency
            WHEN 'USD' THEN g_usd.rate
            WHEN 'GBP' THEN g_gbp.rate
            WHEN 'EUR' THEN g_eur.rate
        END, 0), 4) AS npc_over_rfr,
    ROUND(COALESCE(k.offer, 0) - n.rate, 4) AS kibor_over_npc
FROM npc_rates n
LEFT JOIN global_reference_rates g_usd 
    ON g_usd.date = n.date AND g_usd.rate_name = 'SOFR' AND g_usd.tenor = 'ON'
LEFT JOIN global_reference_rates g_gbp 
    ON g_gbp.date = n.date AND g_gbp.rate_name = 'SONIA' AND g_gbp.tenor = 'ON'
LEFT JOIN global_reference_rates g_eur 
    ON g_eur.date = n.date AND g_eur.rate_name = 'EUSTR' AND g_eur.tenor = 'ON'
LEFT JOIN kibor_daily k 
    ON k.date = n.date 
    AND k.tenor = CASE n.tenor
        WHEN '3M' THEN '3M'
        WHEN '6M' THEN '6M'
        WHEN '12M' THEN '12M'
        ELSE '12M'
    END
LEFT JOIN sbp_fx_interbank f
    ON f.date = n.date 
    AND f.currency = n.currency
WHERE n.certificate_type = 'conventional'
  AND n.currency IN ('USD', 'GBP', 'EUR')
ORDER BY n.date DESC, n.currency, n.tenor;
```

### Repository functions

```python
# In src/psx_ohlcv/db/repositories/npc_rates.py

def ensure_tables(con): ...  # CREATE TABLE + all views above

def upsert_npc_rate(con, date, currency, tenor, rate, 
                    certificate_type='conventional', effective_date=None,
                    sro_reference=None, source='sbp'): ...

def get_latest_npc_rates(con, currency=None, certificate_type='conventional') -> list[dict]: ...
    """Return latest NPC rates, optionally filtered by currency."""

def get_npc_rate_history(con, currency='USD', tenor='12M', 
                         certificate_type='conventional',
                         start_date=None, end_date=None) -> list[dict]: ...
    """Historical NPC rate for a specific currency+tenor."""

def get_npc_yield_curve(con, currency='USD', date=None) -> dict: ...
    """Return yield curve (all tenors) for a currency on a date."""

def get_npc_vs_rfr_spread(con, currency=None, start_date=None) -> list[dict]: ...
    """Query v_npc_vs_rfr_spread view."""

def get_carry_trade_analysis(con, currency='USD', start_date=None) -> list[dict]: ...
    """Query v_npc_carry_trade view."""

def get_multicurrency_dashboard(con, date=None) -> list[dict]: ...
    """Query v_multicurrency_dashboard for latest or specific date."""
```

Register in `db/__init__.py` or `db/repositories/__init__.py` following the existing pattern.

---

## Step 2 -- Scraper: `src/psx_ohlcv/sources/npc_rates_scraper.py`

### NPC Rate Source Analysis

NPC rates are **NOT daily market rates**. They are set by Government of Pakistan via Statutory Regulatory Orders (SROs) and revised periodically (sometimes months apart). The most recent revision was **SRO 33(I)2025 effective January 28, 2025**.

The current rates (as of February 2026):

| Currency | 3M    | 6M    | 12M   | 3Y    | 5Y    |
|----------|-------|-------|-------|-------|-------|
| USD      | 7.00  | 7.00  | 7.00  | 7.50  | 7.50  |
| PKR      | 13.50 | 13.50 | 13.00 | 12.50 | 12.50 |
| GBP      | 7.25  | 7.25  | 7.25  | 7.50  | 7.50  |
| EUR      | 5.25  | 5.25  | 5.25  | 5.25  | 5.25  |

### Scraping Strategy

**Primary Source: SBP NPC Page**
```
https://www.sbp.org.pk/NPC-/page-npc.html
```

The rate table is in a standard HTML `<table>` with this structure:
- Row headers are currency labels: "USD (%, Annualized)", "PKR (%, Annualized)", "GBP (%, Annualized)", "Euro (%, Annualized)"
- Column headers (in the first row): 3M, 6M, 12M, 3Y, 5Y
- Rate values are in subsequent rows after each currency header

Parse strategy:
1. Fetch the HTML page
2. Find the rate table (the one containing "Annualized" in cells)
3. Extract rates row by row, mapping to currency and tenor
4. Detect rate changes by comparing with last stored rates

**Fallback Source: Bank AL Habib NPC Page**
```
https://www.bankalhabib.com/naya-pakistan-certificate
```
This page also publishes the same SBP-mandated rates and notes the SRO reference.

### Scraper Implementation

```python
# src/psx_ohlcv/sources/npc_rates_scraper.py

import logging
from datetime import date, datetime
from bs4 import BeautifulSoup
import requests

log = logging.getLogger(__name__)

TENORS = ['3M', '6M', '12M', '3Y', '5Y']
CURRENCIES_MAP = {
    'usd': 'USD',
    'pkr': 'PKR',
    'gbp': 'GBP',
    'euro': 'EUR',
    'eur': 'EUR',
}

SBP_NPC_URL = 'https://www.sbp.org.pk/NPC-/page-npc.html'
BAHL_NPC_URL = 'https://www.bankalhabib.com/naya-pakistan-certificate'

class NPCRatesScraper:
    """Scrape Naya Pakistan Certificate rates from SBP website."""
    
    def __init__(self, timeout=30):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'PSX-OHLCV-DataCollector/1.0'
        })
    
    def scrape_sbp(self) -> list[dict]:
        """
        Scrape NPC rates from SBP NPC page.
        
        Returns list of dicts:
        [
            {
                'date': '2026-02-26',
                'currency': 'USD',
                'tenor': '3M',
                'rate': 7.00,
                'certificate_type': 'conventional',
                'source': 'sbp'
            },
            ...
        ]
        """
        log.info("Fetching NPC rates from SBP: %s", SBP_NPC_URL)
        resp = self.session.get(SBP_NPC_URL, timeout=self.timeout)
        resp.raise_for_status()
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # Find the rate table -- look for table containing "Annualized"
        tables = soup.find_all('table')
        rate_table = None
        for table in tables:
            if 'annualized' in table.get_text().lower():
                rate_table = table
                break
        
        if not rate_table:
            log.error("Could not find NPC rate table on SBP page")
            return []
        
        rows = rate_table.find_all('tr')
        rates = []
        current_currency = None
        today = date.today().isoformat()
        
        for row in rows:
            cells = row.find_all(['td', 'th'])
            text = [c.get_text(strip=True) for c in cells]
            
            if not text:
                continue
            
            # Check if this row is a currency header
            # e.g. "USD (%, Annualized)" or "PKR (%, Annualized)"
            row_text = ' '.join(text).lower()
            for key, currency_code in CURRENCIES_MAP.items():
                if key in row_text and 'annualized' in row_text:
                    current_currency = currency_code
                    break
            else:
                # This might be a data row -- try to parse rates
                if current_currency and len(text) >= 5:
                    try:
                        parsed_rates = []
                        for val in text[:5]:
                            cleaned = val.replace(',', '').strip()
                            if cleaned:
                                parsed_rates.append(float(cleaned))
                        
                        if len(parsed_rates) == 5 and all(0 < r < 100 for r in parsed_rates):
                            for tenor, rate_val in zip(TENORS, parsed_rates):
                                rates.append({
                                    'date': today,
                                    'currency': current_currency,
                                    'tenor': tenor,
                                    'rate': rate_val,
                                    'certificate_type': 'conventional',
                                    'source': 'sbp',
                                })
                            current_currency = None  # reset after consuming
                    except (ValueError, IndexError):
                        continue
        
        log.info("Parsed %d NPC rate records from SBP", len(rates))
        return rates
    
    def scrape_bankalhabib(self) -> list[dict]:
        """
        Fallback: scrape NPC rates from Bank AL Habib page.
        Same SBP-mandated rates, may also include SRO reference.
        Implement similar HTML parsing as scrape_sbp().
        """
        log.info("Fetching NPC rates from Bank AL Habib (fallback): %s", BAHL_NPC_URL)
        # Similar implementation to scrape_sbp() but targeting BAHL HTML structure
        # Bank AL Habib also notes "Revised Rates effective from <date> as per SRO <ref>"
        # which can be captured as effective_date and sro_reference
        raise NotImplementedError("Fallback scraper -- implement if SBP source fails")
    
    def scrape(self) -> list[dict]:
        """
        Scrape NPC rates with fallback.
        Try SBP first, fall back to Bank AL Habib.
        """
        try:
            rates = self.scrape_sbp()
            if rates:
                return rates
        except Exception as e:
            log.warning("SBP NPC scrape failed: %s. Trying fallback.", e)
        
        try:
            return self.scrape_bankalhabib()
        except Exception as e:
            log.error("All NPC rate sources failed: %s", e)
            return []
```

### Important Notes for Implementation

1. **NPC rates are NOT daily** -- they change when GoP issues a new SRO. The scraper should:
   - Run daily (or weekly) but detect if rates have changed
   - Only insert a new row when rates differ from the last stored values
   - Store the `effective_date` when detectable (from SRO reference on bank pages)

2. **Deduplication logic**: Before inserting, check if today's rates match the most recent stored rates for the same currency/tenor. If identical, skip insertion. This keeps the table clean -- only rows when rates actually changed, plus a daily "last checked" row.

```python
# Add this to the repository module:

def rates_changed(con, new_rates: list[dict]) -> bool:
    """Check if any scraped rate differs from the latest stored rate."""
    cursor = con.cursor()
    for r in new_rates:
        cursor.execute("""
            SELECT rate FROM npc_rates 
            WHERE currency = ? AND tenor = ? AND certificate_type = ?
            ORDER BY date DESC LIMIT 1
        """, (r['currency'], r['tenor'], r['certificate_type']))
        row = cursor.fetchone()
        if row is None or abs(row[0] - r['rate']) > 0.001:
            return True
    return False
```

3. **Islamic NPCs**: The Islamic variant (INPC) uses Mudarabah with expected/actual profit rates that change monthly. These are published at `https://www.sbp.org.pk/NPC-/PSR-Weightages.htm` and `Real-Announcement-for-INPCCL.htm`. These are complex PDF/HTML documents. **For now, only implement conventional NPC rates.** Islamic rates can be added in a future prompt if needed.

---

## Step 3 -- Collector Integration

Add NPC rate collection to the existing collector pattern. Look at how global rates collection is done (e.g., in `collectors/` or directly via CLI) and follow the same pattern.

```python
# If collectors follow a class pattern:
class NPCRatesCollector:
    """Collect and store NPC rates."""
    
    def __init__(self):
        self.scraper = NPCRatesScraper()
    
    def collect(self, con, force=False):
        """
        Scrape NPC rates and store if changed.
        
        Args:
            con: database connection
            force: if True, store even if rates haven't changed
        """
        rates = self.scraper.scrape()
        if not rates:
            log.warning("No NPC rates scraped")
            return 0
        
        if not force and not rates_changed(con, rates):
            log.info("NPC rates unchanged -- skipping insert")
            return 0
        
        count = 0
        for r in rates:
            upsert_npc_rate(
                con, 
                date=r['date'],
                currency=r['currency'],
                tenor=r['tenor'],
                rate=r['rate'],
                certificate_type=r.get('certificate_type', 'conventional'),
                effective_date=r.get('effective_date'),
                sro_reference=r.get('sro_reference'),
                source=r.get('source', 'sbp')
            )
            count += 1
        
        con.commit()
        log.info("Stored %d NPC rate records", count)
        return count
```

---

## Step 4 -- CLI Commands

Add NPC subcommands to the existing CLI structure. Follow the exact same argparse pattern used for `globalrates`.

```python
# Add 'npc' subparser under the main parser

def add_npc_parser(subparsers):
    npc = subparsers.add_parser('npc', help='Naya Pakistan Certificate rates')
    npc_sub = npc.add_subparsers(dest='npc_cmd')
    
    # psxsync npc sync [--force]
    sync_p = npc_sub.add_parser('sync', help='Scrape and store current NPC rates')
    sync_p.add_argument('--force', action='store_true',
                        help='Store even if rates unchanged')
    
    # psxsync npc latest [--currency USD] [--type conventional]
    latest_p = npc_sub.add_parser('latest', help='Show latest NPC rates')
    latest_p.add_argument('--currency', choices=['USD', 'GBP', 'EUR', 'PKR'],
                          help='Filter by currency')
    latest_p.add_argument('--type', dest='cert_type', default='conventional',
                          choices=['conventional', 'islamic'],
                          help='Certificate type (default: conventional)')
    
    # psxsync npc curve [--currency USD] [--date 2026-02-26]
    curve_p = npc_sub.add_parser('curve', help='Show NPC yield curve')
    curve_p.add_argument('--currency', default='USD',
                         choices=['USD', 'GBP', 'EUR', 'PKR'])
    curve_p.add_argument('--date', help='Date (YYYY-MM-DD), default: latest')
    
    # psxsync npc spread [--currency USD]
    spread_p = npc_sub.add_parser('spread', help='NPC vs global RFR spread')
    spread_p.add_argument('--currency', choices=['USD', 'GBP', 'EUR'])
    
    # psxsync npc carry [--currency USD]
    carry_p = npc_sub.add_parser('carry', help='NPC vs KIBOR carry trade analysis')
    carry_p.add_argument('--currency', default='USD',
                         choices=['USD', 'GBP', 'EUR'])
    
    # psxsync npc dashboard [--date 2026-02-26]
    dash_p = npc_sub.add_parser('dashboard', help='Multi-currency dashboard')
    dash_p.add_argument('--date', help='Date (YYYY-MM-DD)')
    
    return npc


def handle_npc(args, con):
    """Handle NPC CLI commands."""
    if args.npc_cmd == 'sync':
        collector = NPCRatesCollector()
        count = collector.collect(con, force=args.force)
        print(f"NPC rates: {count} records stored")
    
    elif args.npc_cmd == 'latest':
        rates = get_latest_npc_rates(con, currency=args.currency,
                                      certificate_type=args.cert_type)
        if not rates:
            print("No NPC rates found. Run 'psxsync npc sync' first.")
            return
        # Print formatted table
        print(f"\n{'Currency':<10} {'Tenor':<8} {'Rate %':<10} {'Type':<15} {'Date':<12}")
        print("-" * 60)
        for r in rates:
            print(f"{r['currency']:<10} {r['tenor']:<8} {r['rate']:<10.2f} "
                  f"{r['certificate_type']:<15} {r['date']:<12}")
    
    elif args.npc_cmd == 'curve':
        curve = get_npc_yield_curve(con, currency=args.currency, date=args.date)
        if not curve:
            print(f"No yield curve data for {args.currency}")
            return
        print(f"\nNPC {args.currency} Yield Curve ({curve.get('date', 'latest')}):")
        for tenor in TENORS:
            key = f'rate_{tenor.lower()}'
            val = curve.get(key)
            if val is not None:
                print(f"  {tenor:>4}: {val:.2f}%")
    
    elif args.npc_cmd == 'spread':
        spreads = get_npc_vs_rfr_spread(con, currency=args.currency)
        # Print spread table
        print(f"\n{'Date':<12} {'CCY':<6} {'Tenor':<8} {'NPC %':<9} {'RFR %':<9} {'Premium':<10}")
        print("-" * 60)
        for s in spreads[:20]:
            print(f"{s['date']:<12} {s['currency']:<6} {s['tenor']:<8} "
                  f"{s['npc_rate']:<9.2f} {s.get('rfr_rate', 0):<9.2f} "
                  f"{s.get('npc_premium_over_rfr', 0):>+9.2f}")
    
    elif args.npc_cmd == 'carry':
        trades = get_carry_trade_analysis(con, currency=args.currency)
        print(f"\n{'Date':<12} {'Tenor':<8} {'NPC {args.currency} %':<12} "
              f"{'KIBOR %':<10} {'Spread':<10} {'FX Rate':<12}")
        print("-" * 70)
        for t in trades[:20]:
            print(f"{t['date']:<12} {t['tenor']:<8} {t['npc_rate']:<12.2f} "
                  f"{t.get('kibor_offer', 0):<10.2f} "
                  f"{t.get('kibor_npc_spread', 0):>+9.2f} "
                  f"{t.get('fx_rate_pkr', 0):<12.2f}")
    
    elif args.npc_cmd == 'dashboard':
        data = get_multicurrency_dashboard(con, date=args.date)
        if not data:
            print("No dashboard data available. Run sync commands first.")
            return
        print(f"\nMulti-Currency Dashboard ({data[0].get('date', 'latest')}):")
        print(f"{'CCY':<6} {'Tenor':<8} {'NPC %':<9} {'RFR %':<9} {'KIBOR %':<10} "
              f"{'NPC-RFR':<10} {'KIBOR-NPC':<10} {'FX Rate':<10}")
        print("-" * 80)
        for d in data:
            print(f"{d['currency']:<6} {d['tenor']:<8} "
                  f"{d['npc_rate']:<9.2f} "
                  f"{d.get('global_rfr', 0) or 0:<9.2f} "
                  f"{d.get('kibor_offer', 0) or 0:<10.2f} "
                  f"{d.get('npc_over_rfr', 0) or 0:>+9.2f} "
                  f"{d.get('kibor_over_npc', 0) or 0:>+9.2f} "
                  f"{d.get('fx_rate_pkr', 0) or 0:<10.2f}")
```

---

## Step 5 -- FastAPI Routes: `src/psx_ohlcv/api/routers/npc_rates.py`

```python
from fastapi import APIRouter, Query
from typing import Optional

router = APIRouter(prefix="/api/npc", tags=["NPC Rates"])

@router.get("/latest")
def get_latest(currency: Optional[str] = None, 
               certificate_type: str = "conventional"):
    """Latest NPC rates, optionally filtered by currency."""
    ...

@router.get("/history/{currency}/{tenor}")
def get_history(currency: str, tenor: str,
                start_date: Optional[str] = None,
                end_date: Optional[str] = None):
    """Historical NPC rate for a currency+tenor pair."""
    ...

@router.get("/yield-curve/{currency}")
def yield_curve(currency: str, date: Optional[str] = None):
    """NPC yield curve for a currency (all tenors)."""
    ...

@router.get("/spread/rfr")
def rfr_spread(currency: Optional[str] = None):
    """NPC premium over global risk-free rates."""
    ...

@router.get("/spread/carry")
def carry_trade(currency: str = "USD"):
    """NPC vs KIBOR carry trade analysis."""
    ...

@router.get("/dashboard")
def multicurrency_dashboard(date: Optional[str] = None):
    """Comprehensive multi-currency view: NPC + RFR + KIBOR + FX."""
    ...

@router.post("/sync")
def sync_rates(force: bool = False):
    """Trigger NPC rate scrape."""
    ...
```

Register router in `api/main.py` (or wherever routers are registered):
```python
from .routers.npc_rates import router as npc_router
app.include_router(npc_router)
```

---

## Step 6 -- Streamlit Page: `src/psx_ohlcv/ui/page_views/npc_rates.py`

Create a Streamlit page with the following sections:

### 6a. Current NPC Rates Table
- Display all currencies × tenors in a pivot table format
- Color-code: green for highest rates, red for lowest
- Show effective date and SRO reference if available

### 6b. NPC Yield Curves
- Line chart: NPC yield curves for USD, GBP, EUR, PKR overlaid
- X-axis: tenor (3M → 5Y), Y-axis: rate %
- Use st.selectbox for date selection (default: latest)

### 6c. NPC vs Global RFR Spread
- Bar chart: NPC rate vs underlying RFR for each currency
- USD bar pair: NPC USD vs SOFR
- GBP bar pair: NPC GBP vs SONIA
- EUR bar pair: NPC EUR vs EUSTR
- Show the premium/spread as annotation

### 6d. Carry Trade Dashboard
- Table showing KIBOR vs NPC rate for each tenor
- FX rate column and breakeven depreciation
- Historical chart of spread evolution if data available

### 6e. Multi-Currency Dashboard
- Comprehensive view combining all layers
- Selectable currency pair
- Chart showing NPC + RFR + KIBOR over time with FX overlay

### 6f. NPC Rate Change History
- Timeline showing when rates changed (SRO dates)
- Step chart of rate changes over time for each currency

### Layout guidance

```python
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px

def render():
    st.title("🏦 Naya Pakistan Certificates (NPC)")
    st.caption("Sovereign FCY instruments | SBP-administered | Roshan Digital Accounts")
    
    con = connect()
    ensure_tables(con)
    
    # Sync button
    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("🔄 Refresh Rates"):
            collector = NPCRatesCollector()
            count = collector.collect(con)
            st.success(f"Updated: {count} records")
    
    # Tabs for different views
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Current Rates", 
        "📈 Yield Curves", 
        "🔄 vs Global RFR",
        "💱 Carry Trade", 
        "🌍 Dashboard"
    ])
    
    with tab1:
        # Pivot table of current rates
        ...
    
    with tab2:
        # Yield curve charts
        ...
    
    with tab3:
        # NPC vs RFR spread charts
        ...
    
    with tab4:
        # Carry trade analysis
        ...
    
    with tab5:
        # Multi-currency dashboard
        ...
```

Register in the Streamlit sidebar navigation (follow existing pattern for how other pages are listed).

---

## Step 7 -- Cron / Scheduler Integration

Since NPC rates change infrequently (only on SRO revision, sometimes months apart), a **weekly** scrape is sufficient. However, daily scrape is also fine since the deduplication logic prevents unnecessary inserts.

Add to existing cron/scheduler config (follow the pattern used for global rates):

```
# NPC rates -- weekly check (rates change infrequently via SRO)
0 10 * * 1  cd ~/psx_ohlcv && python -m psx_ohlcv npc sync
```

Or if using the project's internal scheduler, add NPC to the schedule alongside global rates.

---

## VERIFY

After implementing, run these checks:

```bash
# 1. Ensure tables created
python -c "
from psx_ohlcv.db.connection import connect
from psx_ohlcv.db.repositories.npc_rates import ensure_tables
con = connect()
ensure_tables(con)
# Check tables exist
cursor = con.cursor()
cursor.execute(\"SELECT name FROM sqlite_master WHERE type='table' AND name='npc_rates'\")
assert cursor.fetchone(), 'npc_rates table missing'
cursor.execute(\"SELECT name FROM sqlite_master WHERE type='view' AND name='v_npc_vs_rfr_spread'\")
assert cursor.fetchone(), 'v_npc_vs_rfr_spread view missing'
cursor.execute(\"SELECT name FROM sqlite_master WHERE type='view' AND name='v_npc_carry_trade'\")
assert cursor.fetchone(), 'v_npc_carry_trade view missing'
cursor.execute(\"SELECT name FROM sqlite_master WHERE type='view' AND name='v_npc_yield_curve'\")
assert cursor.fetchone(), 'v_npc_yield_curve view missing'
cursor.execute(\"SELECT name FROM sqlite_master WHERE type='view' AND name='v_multicurrency_dashboard'\")
assert cursor.fetchone(), 'v_multicurrency_dashboard view missing'
print('✓ All tables and views created')
"

# 2. Scrape NPC rates
python -m psx_ohlcv npc sync
# Should show "NPC rates: 20 records stored" (4 currencies × 5 tenors)

# 3. Verify data
python -m psx_ohlcv npc latest
# Should show rate table for all currencies and tenors

# 4. Verify yield curve
python -m psx_ohlcv npc curve --currency USD
# Should show: 3M: 7.00%, 6M: 7.00%, 12M: 7.00%, 3Y: 7.50%, 5Y: 7.50%

# 5. Verify cross-currency spread (requires SOFR/SONIA/EUSTR data from 5.1-5.4)
python -m psx_ohlcv npc spread
# Should show NPC premium over RFR for each currency

# 6. Verify carry trade analysis (requires KIBOR data)
python -m psx_ohlcv npc carry --currency USD
# Should show KIBOR vs NPC USD spread with FX rate

# 7. Verify dashboard
python -m psx_ohlcv npc dashboard
# Should show comprehensive multi-currency view

# 8. Verify idempotency -- run sync again
python -m psx_ohlcv npc sync
# Should show "NPC rates unchanged -- skipping insert" or "0 records stored"

# 9. Force sync
python -m psx_ohlcv npc sync --force
# Should store records even if unchanged

# 10. API endpoints (if server running)
curl http://localhost:8000/api/npc/latest
curl http://localhost:8000/api/npc/yield-curve/USD
curl http://localhost:8000/api/npc/spread/rfr
curl http://localhost:8000/api/npc/spread/carry?currency=USD
curl http://localhost:8000/api/npc/dashboard

# 11. Streamlit page
# Navigate to NPC Rates page -- verify all tabs render with data
```

---

## COMMIT

```bash
git add -A
git commit -m "feat(5.5): NPC rates scraper + cross-currency analytics

- New: npc_rates table for Naya Pakistan Certificate rates (USD/GBP/EUR/PKR × 5 tenors)
- New: NPCRatesScraper parsing SBP NPC page HTML table
- New: v_npc_vs_rfr_spread view (NPC premium over SOFR/SONIA/EUSTR)
- New: v_npc_carry_trade view (KIBOR vs NPC with FX breakeven)
- New: v_npc_yield_curve view (all tenors pivot by currency)
- New: v_multicurrency_dashboard view (comprehensive NPC+RFR+KIBOR+FX)
- New: CLI commands (psxsync npc sync/latest/curve/spread/carry/dashboard)
- New: FastAPI routes /api/npc/*
- New: Streamlit NPC page with 5-tab layout (rates, curves, spreads, carry, dashboard)
- Smart deduplication: rates only stored when actually changed (SRO-driven)
- Completes Pakistan Multi-Asset Financial Intelligence stack:
  KIBOR + SOFR/SONIA/EUSTR/TONA + FX rates + NPC rates
- Ref: NPC Rules 2020 under Public Debt Act 1944
- Ref: SRO 33(I)2025 effective 28-Jan-2025 (latest rate revision)"
```

Update `.claude_session_state.md`:
```
Current Phase: 5.5 -- NPC Rates + Cross-Currency Analytics
Status: COMPLETE
Branch: feat/npc-rates
Next: Merge to dev
```
