# Phase 3: Sukuk/Debt Market Analytics Implementation

## Overview

Phase 3 adds comprehensive Sukuk/Debt Market analytics to the PSX OHLCV Explorer. This implementation is **ADDITIVE ONLY** - it does not modify any existing Phase 1, 2, or 2.5 functionality.

### Key Features
- GOP Ijarah Sukuk, PIBs, T-Bills tracking
- Corporate Sukuk and TFCs support
- Yield curve construction and interpolation
- Fixed income analytics (YTM, Duration, Convexity)
- SBP primary market document archiving

## Data Sources

### Official Sources
- **PSX GIS**: https://dps.psx.com.pk/gis/debt-market
- **SBP DFMD**: https://easydata.sbp.org.pk/apex/f?p=10:210

### Data Flow
1. Manual CSV ingestion (primary)
2. Sample data generation (for testing)
3. Future: PSX GIS scraping adapter

## Database Schema

### New Tables

#### `sukuk_master` - Instrument Master
```sql
CREATE TABLE sukuk_master (
    instrument_id       TEXT PRIMARY KEY,
    issuer              TEXT NOT NULL,
    name                TEXT NOT NULL,
    category            TEXT NOT NULL,  -- GOP_SUKUK, PIB, TBILL, etc.
    currency            TEXT DEFAULT 'PKR',
    issue_date          TEXT,
    maturity_date       TEXT NOT NULL,
    coupon_rate         REAL,
    coupon_frequency    INTEGER,
    face_value          REAL DEFAULT 100.0,
    issue_size          REAL,
    shariah_compliant   INTEGER DEFAULT 1,
    is_active           INTEGER DEFAULT 1,
    source              TEXT DEFAULT 'MANUAL',
    created_at          TEXT DEFAULT (datetime('now'))
);
```

#### `sukuk_quotes` - Daily Quotes
```sql
CREATE TABLE sukuk_quotes (
    instrument_id       TEXT NOT NULL,
    quote_date          TEXT NOT NULL,
    clean_price         REAL,
    dirty_price         REAL,
    yield_to_maturity   REAL,
    bid_yield           REAL,
    ask_yield           REAL,
    volume              REAL,
    source              TEXT DEFAULT 'MANUAL',
    ingested_at         TEXT DEFAULT (datetime('now')),
    PRIMARY KEY(instrument_id, quote_date)
);
```

#### `sukuk_yield_curve` - Yield Curve Points
```sql
CREATE TABLE sukuk_yield_curve (
    curve_name          TEXT NOT NULL,  -- GOP_SUKUK, PIB, TBILL
    curve_date          TEXT NOT NULL,
    tenor_days          INTEGER NOT NULL,
    yield_rate          REAL NOT NULL,
    source              TEXT DEFAULT 'SBP',
    PRIMARY KEY(curve_name, curve_date, tenor_days)
);
```

#### `sukuk_analytics_snapshots` - Computed Analytics
```sql
CREATE TABLE sukuk_analytics_snapshots (
    instrument_id       TEXT NOT NULL,
    calc_date           TEXT NOT NULL,
    yield_to_maturity   REAL,
    macaulay_duration   REAL,
    modified_duration   REAL,
    convexity           REAL,
    current_yield       REAL,
    PRIMARY KEY(instrument_id, calc_date)
);
```

#### `sbp_primary_market_docs` - Document Archive
```sql
CREATE TABLE sbp_primary_market_docs (
    doc_id              TEXT PRIMARY KEY,
    doc_type            TEXT NOT NULL,
    auction_date        TEXT NOT NULL,
    instrument_type     TEXT NOT NULL,
    file_path           TEXT NOT NULL,
    file_name           TEXT NOT NULL,
    file_hash           TEXT,
    indexed_at          TEXT DEFAULT (datetime('now'))
);
```

## CLI Commands

### Sukuk Command Group
```bash
# Seed instrument master data
pfsync sukuk seed [--category GOP_SUKUK|PIB|TBILL|...] [--shariah-only]

# Sync quotes and yield curves
pfsync sukuk sync [--instruments] [--source SAMPLE|CSV] [--days 90] [--include-curves]

# Load from CSV files
pfsync sukuk load --master <path> --quotes <path> --curve <path>

# Compute analytics
pfsync sukuk compute [--instruments] [--as-of DATE]

# List instruments
pfsync sukuk list [--category] [--issuer] [--shariah-only]

# Show instrument details
pfsync sukuk show --instrument <id>

# Show yield curve
pfsync sukuk curve [--name GOP_SUKUK|PIB|TBILL] [--date]

# Index SBP documents
pfsync sukuk sbp [--docs-dir] [--create-samples]

# Compare instruments
pfsync sukuk compare --instruments <id1>,<id2>,...

# Show data status
pfsync sukuk status
```

## Analytics Functions

### YTM Calculation
Newton-Raphson solver for yield to maturity:
```python
calculate_ytm(price, face_value, coupon_rate, years_to_maturity, frequency)
```

### Duration
- **Macaulay Duration**: Weighted average time to cash flows
- **Modified Duration**: Price sensitivity to yield changes

### Convexity
Second-order price sensitivity measure for large yield movements.

### Yield Curve Interpolation
Linear interpolation between curve points for custom tenors.

## UI Pages

### 1. Sukuk Screener
- Filter by category, issuer, shariah compliance
- Analytics summary table
- Instrument detail view

### 2. Sukuk Yield Curve
- Interactive curve chart
- Multiple curve types (GOP_SUKUK, PIB, TBILL)
- Yield interpolation tool

### 3. SBP Auction Archive
- Document listing and filtering
- SBP data source URLs
- Document naming guide

## File Structure

```
src/pakfindata/
    sources/
        sukuk_manual.py          # CSV loaders and sample data
        sbp_primary_market.py    # SBP document handling
    sync_sukuk.py               # Sync operations
    analytics_sukuk.py          # Fixed income analytics
    cli.py                      # Extended with sukuk commands
    db.py                       # Extended with sukuk tables
    ui/
        app.py                  # Extended with sukuk pages

data/sukuk/
    sukuk_master_template.csv   # Master data template
    sukuk_quotes_template.csv   # Quotes template
    sukuk_yield_curve_template.csv  # Yield curve template
    sbp_docs/                   # SBP document archive

tests/
    test_sukuk.py              # Phase 3 tests
```

## Categories

| Code | Description |
|------|-------------|
| GOP_SUKUK | Government of Pakistan Ijarah Sukuk |
| PIB | Pakistan Investment Bonds |
| TBILL | Treasury Bills |
| CORPORATE_SUKUK | Corporate Sukuk |
| TFC | Term Finance Certificates |

## Usage Examples

### Quick Start
```bash
# Initialize with sample data
pfsync sukuk seed
pfsync sukuk sync --include-curves

# Check status
pfsync sukuk status

# View instrument analytics
pfsync sukuk show --instrument GOP-IJARA-3Y-2027-06

# View yield curve
pfsync sukuk curve --name GOP_SUKUK
```

### Load Custom Data
```bash
# Load from CSV files
pfsync sukuk load --master data/sukuk/custom_master.csv
pfsync sukuk load --quotes data/sukuk/custom_quotes.csv
pfsync sukuk load --curve data/sukuk/custom_curve.csv

# Compute analytics
pfsync sukuk compute
```

### SBP Document Archive
```bash
# Create sample documents
pfsync sukuk sbp --create-samples

# Index documents from directory
pfsync sukuk sbp --docs-dir /path/to/sbp/pdfs
```

## Verification Checklist

- [x] Database tables created (6 new tables)
- [x] CSV loaders implemented
- [x] Sample data generation
- [x] YTM, Duration, Convexity calculations
- [x] Yield curve interpolation
- [x] CLI commands (seed, sync, load, compute, list, show, curve, sbp, compare, status)
- [x] Sukuk Screener UI page
- [x] Sukuk Yield Curve UI page
- [x] SBP Auction Archive UI page
- [x] Test file created
- [x] Documentation complete

## Notes

1. **ADDITIVE ONLY**: This implementation does not modify existing bonds functionality
2. **Quote/Yield Driven**: Fixed income is quote-driven, not OHLCV
3. **Manual Ingestion**: Primary data source is CSV/manual entry
4. **Regulator Aligned**: Uses PSX GIS and SBP as reference sources
5. **Educational Use**: Analytics for educational purposes, not investment advice
