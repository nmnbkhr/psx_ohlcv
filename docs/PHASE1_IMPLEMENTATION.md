# Phase 1 Implementation Guide

This document describes the implementation of Phase 1: Indexes + ETFs + REITs universe expansion.

## Overview

Phase 1 extends the PSX OHLCV Explorer beyond equities to cover ETFs, REITs, and Market Indexes. This is an **additive-only** implementation that does not modify any existing functionality.

## Quick Start

```bash
# 1. Seed the instrument universe
psxsync universe seed-phase1

# 2. Sync EOD data for instruments
psxsync instruments sync-eod --types ETF,REIT,INDEX

# 3. Compute rankings
psxsync instruments rankings --compute

# 4. View in UI
streamlit run src/psx_ohlcv/ui/app.py
# Navigate to "Instruments" or "Rankings" pages
```

## New Components

### Database Tables

Four new tables were added to support the instrument universe:

1. **instruments** - Master table for all instrument types
   - `instrument_id`: Primary key (format: `PSX:SYMBOL` or `IDX:SYMBOL`)
   - `symbol`: Trading symbol
   - `name`: Full name
   - `instrument_type`: 'EQUITY', 'ETF', 'REIT', or 'INDEX'
   - `source`: Data source ('DPS' or 'MANUAL')

2. **instrument_membership** - Index/ETF constituent relationships
   - Tracks which instruments belong to which indexes
   - Supports historical weights via effective_date

3. **ohlcv_instruments** - OHLCV data for non-equity instruments
   - Separate from `eod_ohlcv` to avoid schema conflicts
   - Same structure: date, open, high, low, close, volume

4. **instrument_rankings** - Performance metrics storage
   - return_1m, return_3m, return_6m, return_1y
   - volatility_30d, relative_strength
   - Computed on-demand or scheduled

### New Modules

| Module | Purpose |
|--------|---------|
| `instruments.py` | High-level instrument management |
| `sync_instruments.py` | EOD sync for non-equity instruments |
| `analytics_phase1.py` | Returns, volatility, rankings |
| `sources/instrument_universe.py` | Universe seeding from config |

### CLI Commands

#### Universe Management

```bash
# Seed instruments from config file
psxsync universe seed-phase1
psxsync universe seed-phase1 --config /path/to/custom.json
psxsync universe seed-phase1 --include-equities

# List instruments
psxsync universe list
psxsync universe list --type ETF
psxsync universe list --type REIT --active-only

# Add new instrument manually
psxsync universe add --type ETF --symbol NEWETF --name "New ETF Name"
```

#### Instrument Data Operations

```bash
# Sync EOD data for all non-equity types
psxsync instruments sync-eod

# Sync specific types
psxsync instruments sync-eod --types ETF,REIT

# Sync single instrument
psxsync instruments sync-eod --symbol NIUETF

# Full refresh (ignore existing data)
psxsync instruments sync-eod --full

# View sync status
psxsync instruments sync-status

# Compute and display rankings
psxsync instruments rankings --compute --top 10
```

### UI Pages

Two new pages added to the Streamlit dashboard:

1. **📦 Instruments Browser**
   - Filter by type (ETF, REIT, INDEX)
   - View instrument details and metrics
   - Trigger sync operations
   - Price charts for selected instruments

2. **🏆 Rankings**
   - Performance comparison table
   - Normalized performance chart
   - Summary statistics
   - Refresh rankings on-demand

## Configuration

### Universe Config File

Location: `{DATA_ROOT}/universe_phase1.json`

```json
{
  "indexes": [
    {"symbol": "KSE100", "name": "KSE-100 Index", "source": "DPS"},
    {"symbol": "KSE30", "name": "KSE-30 Index", "source": "DPS"}
  ],
  "etfs": [
    {"symbol": "NIUETF", "name": "NIT Islamic Equity Fund", "source": "DPS"},
    {"symbol": "MIETF", "name": "Meezan Islamic ETF", "source": "DPS"}
  ],
  "reits": [
    {"symbol": "DCR", "name": "Dolmen City REIT", "source": "DPS"},
    {"symbol": "IGILREIT", "name": "IGIL REIT", "source": "DPS"}
  ]
}
```

To add new instruments, edit this file and re-run `psxsync universe seed-phase1`.

## Analytics

### Performance Metrics

| Metric | Description | Window |
|--------|-------------|--------|
| return_1m | 1-month return | 21 trading days |
| return_3m | 3-month return | 63 trading days |
| return_6m | 6-month return | 126 trading days |
| return_1y | 1-year return | 252 trading days |
| volatility_30d | Annualized volatility | 30 days |
| relative_strength | Performance vs KSE-100 | Configurable |

### Normalized Performance

For comparison charts, performance is normalized to a base of 100 at the start date:

```python
normalized_price = (current_close / first_close) * 100
```

## Testing

Run Phase 1 tests:

```bash
pytest tests/test_phase1_instruments.py -v
```

Test coverage includes:
- Schema creation
- Instrument CRUD operations
- OHLCV data upsert/query
- Rankings computation
- Analytics calculations

## API Reference

### Instrument Management

```python
from psx_ohlcv.db import get_instruments, upsert_instrument

# Get all ETFs
etfs = get_instruments(con, instrument_type="ETF", active_only=True)

# Add new instrument
instrument = {
    "instrument_id": "PSX:NEWETF",
    "symbol": "NEWETF",
    "name": "New ETF",
    "instrument_type": "ETF",
    "exchange": "PSX",
    "currency": "PKR",
    "is_active": 1,
    "source": "DPS",
}
upsert_instrument(con, instrument)
```

### EOD Sync

```python
from psx_ohlcv.sync_instruments import sync_instruments_eod

summary = sync_instruments_eod(
    db_path=None,  # Use default
    instrument_types=["ETF", "REIT"],
    incremental=True,
    limit=None,
)
print(f"OK: {summary.ok}, Failed: {summary.failed}")
```

### Analytics

```python
from psx_ohlcv.analytics_phase1 import (
    compute_rankings,
    get_rankings,
    get_normalized_performance,
)

# Compute and store rankings
result = compute_rankings(con, instrument_types=["ETF", "REIT", "INDEX"])

# Get stored rankings
rankings = get_rankings(con, instrument_types=["ETF"], top_n=10)

# Get normalized performance for comparison
perf_df = get_normalized_performance(
    con,
    instrument_ids=["PSX:NIUETF", "IDX:KSE100"],
    start_date="2024-01-01",
)
```

## Limitations

1. **Data Source**: Relies on DPS EOD endpoint; some instruments may not have data
2. **No Intraday**: Intraday data not available for non-equity instruments
3. **Manual Config**: New instruments must be added via config file or CLI
4. **Historical Depth**: Limited by DPS data availability

## Future Enhancements (Post Phase 1)

- Automated instrument discovery
- Real-time data for ETFs (if available)
- Fundamentals for REITs
- Index constituent weights
- Automated daily ranking computation via scheduler
