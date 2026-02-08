# Phase 2: FX Analytics - Implementation Details

This document describes the implementation of Phase 2: FX Analytics for the PSX OHLCV Explorer.

## Overview

Phase 2 adds foreign exchange (FX) rate tracking and analytics to provide macro context for equity analysis. All FX data is **READ-ONLY** and used for informational purposes only—no trading signals or execution logic.

## Architecture

### Design Principles

1. **Additive Only**: No modifications to existing Phase 1 or equity functionality
2. **Read-Only Analytics**: FX data for macro context, not trading
3. **Source Abstraction**: Support multiple FX data sources with fallback
4. **Incremental Sync**: Only fetch new data since last sync

### Module Structure

```
src/psx_ohlcv/
├── db.py                    # Extended with FX tables and helpers
├── analytics_fx.py          # FX analytics functions
├── sync_fx.py               # FX sync operations
├── cli.py                   # Extended with fx command group
├── sources/
│   └── fx.py                # FX data source abstraction
└── ui/
    └── app.py               # Extended with FX pages
```

## Database Schema

### fx_pairs

Stores FX pair metadata.

```sql
CREATE TABLE IF NOT EXISTS fx_pairs (
    pair                TEXT PRIMARY KEY,      -- e.g., "USD/PKR"
    base_currency       TEXT NOT NULL,         -- e.g., "USD"
    quote_currency      TEXT NOT NULL,         -- e.g., "PKR"
    source              TEXT NOT NULL,         -- Data source identifier
    description         TEXT,
    is_active           INTEGER NOT NULL DEFAULT 1,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);
```

### fx_ohlcv

Stores FX rate OHLCV data.

```sql
CREATE TABLE IF NOT EXISTS fx_ohlcv (
    pair                TEXT NOT NULL,
    date                TEXT NOT NULL,
    open                REAL,
    high                REAL,
    low                 REAL,
    close               REAL NOT NULL,
    volume              REAL,
    PRIMARY KEY(pair, date)
);
```

### fx_adjusted_metrics

Stores pre-computed FX-adjusted equity metrics.

```sql
CREATE TABLE IF NOT EXISTS fx_adjusted_metrics (
    as_of_date          TEXT NOT NULL,
    symbol              TEXT NOT NULL,         -- Equity symbol
    fx_pair             TEXT NOT NULL,         -- e.g., "USD/PKR"
    equity_return       REAL,                  -- Local currency return
    fx_return           REAL,                  -- FX return (positive = depreciation)
    fx_adjusted_return  REAL,                  -- equity_return - fx_return
    period              TEXT NOT NULL DEFAULT '1M',
    PRIMARY KEY(as_of_date, symbol, fx_pair, period)
);
```

### fx_sync_runs

Audit trail for FX sync operations.

```sql
CREATE TABLE IF NOT EXISTS fx_sync_runs (
    run_id              TEXT PRIMARY KEY,
    started_at          TEXT NOT NULL,
    completed_at        TEXT,
    status              TEXT NOT NULL DEFAULT 'running',
    pairs_requested     TEXT,
    rows_upserted       INTEGER DEFAULT 0,
    error_message       TEXT
);
```

## Data Sources

### Supported Sources

1. **SBP (State Bank of Pakistan)**: Official exchange rates
2. **Open APIs**: exchangerate-api.com, fixer.io, etc.
3. **Sample Data**: Random walk generator for testing/development

### Source Selection

The `fetch_fx_ohlcv()` function in `sources/fx.py` attempts sources in order:
1. SBP API (if available)
2. Open exchange rate API (if configured)
3. Sample data fallback (always available)

### Default FX Pairs

```python
DEFAULT_FX_PAIRS = [
    {"pair": "USD/PKR", "base_currency": "USD", "quote_currency": "PKR"},
    {"pair": "EUR/PKR", "base_currency": "EUR", "quote_currency": "PKR"},
    {"pair": "GBP/PKR", "base_currency": "GBP", "quote_currency": "PKR"},
    {"pair": "SAR/PKR", "base_currency": "SAR", "quote_currency": "PKR"},
    {"pair": "AED/PKR", "base_currency": "AED", "quote_currency": "PKR"},
]
```

## Analytics Functions

### FX Returns

Calculates period returns for FX pairs:
- 1W (5 trading days)
- 1M (21 trading days)
- 3M (63 trading days)

```python
def compute_fx_returns(df: pd.DataFrame, periods: list[int] | None = None) -> dict:
    """Compute FX returns for various periods."""
```

### FX Volatility

Calculates annualized volatility (standard deviation of returns × √252):

```python
def compute_fx_volatility(df: pd.DataFrame, windows: list[int] | None = None) -> dict:
    """Compute FX volatility (annualized standard deviation of returns)."""
```

### FX Trend

Analyzes trend using moving average:
- Direction (above/below 50-day MA)
- Strength (neutral, weak, moderate, strong)

```python
def compute_fx_trend(df: pd.DataFrame, ma_period: int = 50) -> dict:
    """Compute FX trend indicators."""
```

### FX-Adjusted Return

Converts local currency equity returns to USD terms:

```
fx_adjusted_return = equity_return - fx_return
```

Where:
- `equity_return`: Stock return in PKR
- `fx_return`: USD/PKR change (positive = PKR depreciation)
- `fx_adjusted_return`: Effective return for USD-based investor

## CLI Commands

### fx seed

Seeds default FX pairs into the database.

```bash
psxsync fx seed [--db PATH]
```

### fx sync

Syncs FX OHLCV data for all active pairs.

```bash
psxsync fx sync [--pairs USD/PKR,EUR/PKR] [--full] [--db PATH]
```

Options:
- `--pairs`: Specific pairs to sync (default: all active)
- `--full`: Full sync (ignore incremental)

### fx show

Displays FX analytics for a pair.

```bash
psxsync fx show --pair USD/PKR [--db PATH]
```

### fx compute-adjusted

Computes FX-adjusted metrics for equities.

```bash
psxsync fx compute-adjusted [--symbols SYMBOL1,SYMBOL2] [--fx-pair USD/PKR] [--db PATH]
```

### fx status

Shows FX sync status and data summary.

```bash
psxsync fx status [--db PATH]
```

## UI Pages

### FX Overview (🌍)

Displays:
- Current FX rates with daily change
- FX rate charts (line chart with MA overlay)
- FX analytics summary (returns, volatility, trend)
- Data freshness indicators

### FX Impact (📊)

Displays:
- FX-adjusted equity returns comparison
- Top/bottom performers by FX impact
- Period selection (1W, 1M, 3M)

## Testing

### Test Files

1. `tests/test_fx_ingestion.py` - FX data ingestion and storage
2. `tests/test_fx_analytics.py` - FX analytics calculations
3. `tests/test_fx_adjusted_metrics.py` - FX-adjusted metric storage and computation

### Running Tests

```bash
# Run all FX tests
pytest tests/test_fx*.py -v

# Run specific test class
pytest tests/test_fx_analytics.py::TestFXReturns -v
```

## Configuration

### FX Config File

Located at `data/fx_config.json`:

```json
{
  "pairs": [
    {
      "pair": "USD/PKR",
      "base_currency": "USD",
      "quote_currency": "PKR",
      "source": "AUTO"
    }
  ]
}
```

## Usage Examples

### Seed and Sync FX Data

```bash
# Initialize FX pairs
psxsync fx seed

# Sync FX data
psxsync fx sync

# Check status
psxsync fx status
```

### View FX Analytics

```bash
# Show USD/PKR analytics
psxsync fx show --pair USD/PKR
```

### Compute FX-Adjusted Metrics

```bash
# Compute for all symbols with recent data
psxsync fx compute-adjusted

# Compute for specific symbols
psxsync fx compute-adjusted --symbols OGDC,PPL,PSO
```

### Launch UI

```bash
psxsync ui
# Navigate to "🌍 FX Overview" or "📊 FX Impact" pages
```

## Limitations

1. **Daily Data Only**: No intraday FX rates
2. **PKR Pairs Only**: Focus on Pakistan-relevant currency pairs
3. **Sample Data**: When live APIs unavailable, sample data is used
4. **No Trading Signals**: Analytics are informational only

## Future Enhancements

- Integration with additional FX data providers
- Historical FX correlation analysis
- FX volatility forecasting
- Multi-currency portfolio analysis
