# Regular Market Watcher - Implementation Audit Report

**Date:** 2026-01-21
**Feature:** REGULAR MARKET data fetcher and watcher
**Status:** Complete ✅

---

## Executive Summary

Successfully implemented a REGULAR MARKET watcher feature that fetches live market data from the Pakistan Stock Exchange (PSX) market-watch page. The implementation includes:

- HTML parsing for the REGULAR MARKET table
- Database storage with current state and historical snapshots
- CLI commands for one-shot and continuous polling
- Hash-based change detection for efficient storage
- Comprehensive test coverage (68 new tests)

**Test Results:** 240 total tests passing
**Linter Status:** All ruff checks passing

---

## Files Created/Modified

### New Files

| File | Purpose | Lines |
|------|---------|-------|
| `src/pakfindata/sources/regular_market.py` | Core module for fetching and parsing | ~570 |
| `tests/test_regular_market_parse.py` | HTML parsing tests | ~430 |
| `tests/test_regular_market_db.py` | Database operation tests | ~200 |
| `tests/test_regular_market_hash_skip.py` | Change detection tests | ~325 |

### Modified Files

| File | Changes |
|------|---------|
| `src/pakfindata/cli.py` | Added `regular-market` command group with 3 subcommands |
| `src/pakfindata/db.py` | Minor formatting fixes (line length) |

---

## Architecture

### Data Flow

```
PSX Market Watch (HTML)
        │
        ▼
┌──────────────────────┐
│ fetch_market_watch_  │  HTTP GET with retries
│ html()               │
└──────────────────────┘
        │
        ▼
┌──────────────────────┐
│ parse_regular_       │  lxml HTML parsing
│ market_html()        │  Column mapping
└──────────────────────┘  Status extraction
        │
        ▼
┌──────────────────────┐
│ _compute_row_hash()  │  SHA256 for change detection
└──────────────────────┘
        │
        ├─────────────────────────────────────┐
        ▼                                     ▼
┌──────────────────────┐       ┌──────────────────────┐
│ insert_snapshots()   │       │ upsert_current()     │
│ (if hash changed)    │       │ (always update)      │
└──────────────────────┘       └──────────────────────┘
        │                                     │
        ▼                                     ▼
┌──────────────────────┐       ┌──────────────────────┐
│ regular_market_      │       │ regular_market_      │
│ snapshots (history)  │       │ current (latest)     │
└──────────────────────┘       └──────────────────────┘
```

### Database Schema

#### `regular_market_current` (Latest state per symbol)

| Column | Type | Description |
|--------|------|-------------|
| symbol | TEXT | Primary key |
| ts | TEXT | Timestamp of last update |
| status | TEXT | Status marker (NC/XD/XR/XB/etc) |
| sector_code | TEXT | Sector identifier |
| listed_in | TEXT | Index listing (e.g., KSE100) |
| ldcp | REAL | Last day closing price |
| open | REAL | Opening price |
| high | REAL | Day high |
| low | REAL | Day low |
| current | REAL | Current price |
| change | REAL | Price change |
| change_pct | REAL | Percentage change |
| volume | REAL | Trading volume |
| row_hash | TEXT | SHA256 hash for change detection |
| updated_at | TEXT | Record update timestamp |

#### `regular_market_snapshots` (Historical data)

| Column | Type | Description |
|--------|------|-------------|
| ts | TEXT | Snapshot timestamp (PK part 1) |
| symbol | TEXT | Stock symbol (PK part 2) |
| ... | ... | Same columns as current |
| ingested_at | TEXT | When snapshot was saved |

**Indexes:**
- `idx_rm_snapshots_symbol` - Fast symbol lookups
- `idx_rm_snapshots_ts` - Time range queries

---

## CLI Commands

### `pfsync regular-market snapshot`

One-shot fetch and store of current market data.

```bash
# Basic usage
pfsync regular-market snapshot

# Custom CSV path
pfsync regular-market snapshot --csv /path/to/output.csv

# Save all rows (even unchanged)
pfsync regular-market snapshot --save-unchanged
```

**Output:**
```
Fetching regular market data...

Regular Market Snapshot
==================================================
  Timestamp:         2026-01-21T14:30:00+05:00
  Symbols found:     532
  Rows upserted:     532
  Snapshots saved:   15
  CSV saved to:      data/regular_market/current.csv
```

### `pfsync regular-market listen`

Continuous polling with configurable interval.

```bash
# Default 60-second interval
pfsync regular-market listen

# Custom interval (30 seconds)
pfsync regular-market listen --interval 30

# Custom output directory
pfsync regular-market listen --csv-dir /path/to/snapshots
```

**Output:**
```
Listening for regular market updates (interval: 60s)
CSV directory: data/regular_market
Press Ctrl+C to stop.

[1] 2026-01-21T14:30:00 | symbols=532 upserted=532 changes=45
[2] 2026-01-21T14:31:00 | symbols=532 upserted=532 changes=23
[3] 2026-01-21T14:32:00 | symbols=532 upserted=532 changes=31
```

### `pfsync regular-market show`

Display current market data from database.

```bash
# Table format
pfsync regular-market show

# CSV format
pfsync regular-market show --out csv
```

---

## Key Implementation Details

### 1. Status Marker Extraction

Extracts status markers from symbol cells:
- **NC** - No Change
- **XD** - Ex-Dividend
- **XR** - Ex-Rights
- **XB** - Ex-Bonus
- **XA** - Ex-Annual
- **XI** - Ex-Interim
- **XW** - Ex-Warrant

```python
# Example: "OGDC NC" → symbol="OGDC", status="NC"
symbol, status = _extract_symbol_and_status("OGDC NC")
```

### 2. Hash-Based Change Detection

Uses SHA256 hash of row data to detect changes:

```python
def _compute_row_hash(row: dict) -> str:
    fields = ["symbol", "status", "sector_code", "listed_in",
              "ldcp", "open", "high", "low", "current",
              "change", "change_pct", "volume"]
    values = [str(row.get(f, "")) for f in fields]
    joined = "|".join(values)
    return hashlib.sha256(joined.encode()).hexdigest()
```

**Benefits:**
- Only saves changed rows to snapshots (reduces storage)
- Fast comparison using hash equality
- Deterministic (same data = same hash)

### 3. Column Mapping Strategy

Maps HTML headers to database columns with priority for longer matches:

```python
# Sorted by length descending to match "CHANGE (%)" before "CHANGE"
sorted_map = sorted(COLUMN_MAP.items(), key=lambda x: len(x[0]), reverse=True)
```

### 4. Error Handling

- HTTP retries with exponential backoff
- Graceful handling of missing columns
- Empty table detection
- Duplicate symbol handling (keeps last)

---

## Test Coverage

### Test Files Summary

| File | Tests | Coverage |
|------|-------|----------|
| test_regular_market_parse.py | 30 | HTML parsing, column mapping, numeric conversion |
| test_regular_market_db.py | 24 | Schema creation, upsert, queries |
| test_regular_market_hash_skip.py | 14 | Change detection, workflow scenarios |

### Key Test Scenarios

1. **Parsing Tests**
   - Sample HTML with full structure
   - Status marker extraction (NC, XD, XR, XB)
   - Numeric column conversion (commas removed)
   - Duplicate symbol handling
   - Negative change values
   - Missing columns handling

2. **Database Tests**
   - Schema creation and indexes
   - Upsert operations
   - Snapshot insertion
   - Query with filters (symbol, time range)
   - Case-insensitive symbol lookup

3. **Change Detection Tests**
   - Unchanged rows skipped (`save_unchanged=False`)
   - Changed rows saved
   - New symbols always saved
   - Volume/status changes detected
   - Workflow sequence simulation

---

## Workflow Recommendation

For optimal change detection, the correct order of operations is:

```python
# 1. First, insert snapshots (compares against PREVIOUS current hash)
inserted = insert_snapshots(con, df, save_unchanged=False)

# 2. Then, update current table (sets new baseline)
upserted = upsert_current(con, df)
```

This ensures snapshots only capture actual changes, not every fetch.

---

## Security Considerations

1. **No Secrets Stored** - Only public market data
2. **Safe HTTP Requests** - Timeouts and retries configured
3. **SQL Injection Prevention** - Parameterized queries throughout
4. **Input Validation** - Numeric conversion with error handling

---

## Performance Notes

1. **Batch Processing** - DataFrame operations for efficiency
2. **Index Optimization** - Indexes on symbol and timestamp
3. **WAL Mode** - SQLite WAL for concurrent access
4. **Hash Comparison** - O(1) change detection per row

---

## Future Enhancements (Not Implemented)

1. WebSocket support for real-time updates
2. UI integration (Streamlit page)
3. Alert system for significant price changes
4. Historical analysis queries
5. Export to external databases

---

## Conclusion

The REGULAR MARKET watcher feature is fully implemented and tested. It provides:

- Reliable data fetching from PSX market-watch
- Efficient storage with change detection
- Flexible CLI interface
- Comprehensive test coverage

All 240 tests pass and the codebase is lint-clean.
