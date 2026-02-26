# Treasury & Rates Tables — Current State

> Generated: 2026-02-17 | DB: `/mnt/e/psxdata/psx.sqlite`

## Summary

| Table | Rows | Date Range | Source |
|-------|-----:|------------|--------|
| `tbill_auctions` | 4 | 2026-02-04 | SBP PMA page |
| `pib_auctions` | 5 | 2026-02-04 | SBP PMA page |
| `gis_auctions` | 6 | 2023-12-21 | SBP PMA page |
| `kibor_daily` | 6 | 2026-02-04 to 2026-02-12 | SBP PMA page |
| `konia_daily` | 2 | 2026-02-09 to 2026-02-16 | SBP PMA page |
| `pkrv_daily` | 21 | 2026-02-04 to 2026-02-16 (3 dates) | SBP PMA page |
| `sbp_policy_rates` | 1 | 2026-01-30 | SBP MSM |
| `kibor_rates` | 3 | 2026-01-30 | SBP MSM (legacy) |

---

## tbill_auctions (4 rows)

| auction_date | tenor | cutoff_yield | weighted_avg_yield |
|--------------|-------|-------------:|-------------------:|
| 2026-02-04 | 1M | 10.1977 | — |
| 2026-02-04 | 3M | 10.1983 | — |
| 2026-02-04 | 6M | 10.3237 | — |
| 2026-02-04 | 12M | 10.3997 | — |

*Columns available but empty: target_amount_billions, bids_received_billions, amount_accepted_billions, cutoff_price, maturity_date, settlement_date*

---

## pib_auctions (5 rows)

| auction_date | tenor | pib_type | cutoff_yield | coupon_rate | amount_accepted_billions |
|--------------|-------|----------|-------------:|------------:|-------------------------:|
| 2026-02-04 | 2Y | Fixed | 10.338 | — | — |
| 2026-02-04 | 3Y | Fixed | 10.2489 | — | — |
| 2026-02-04 | 5Y | Fixed | 10.75 | — | — |
| 2026-02-04 | 10Y | Fixed | 11.239 | — | — |
| 2026-02-04 | 15Y | Fixed | 11.4998 | — | — |

*Columns available but empty: target_amount_billions, bids_received_billions, cutoff_price, maturity_date*

---

## gis_auctions (6 rows)

| auction_date | gis_type | tenor | cutoff_rental_rate |
|--------------|----------|-------:|-------------------:|
| 2023-12-21 | GIS Fixed Rate Return | 5Y | 100.0022 |
| 2023-12-21 | GIS Variable Rate Return | 5Y | 98.76 |
| 2023-12-21 | GIS Fixed Rate Return 3Y | 3Y | 100.2842 |
| 2023-12-21 | GIS Fixed Rate Return 5Y | 5Y | 100.0022 |
| 2023-12-21 | GIS Variable Rate Return 3Y | 3Y | 99.08 |
| 2023-12-21 | GIS Variable Rate Return 5Y | 5Y | 98.76 |

---

## kibor_daily (6 rows)

| date | tenor | bid | offer |
|------|-------|----:|------:|
| 2026-02-04 | 3M | 10.26 | 10.51 |
| 2026-02-04 | 6M | 10.26 | 10.51 |
| 2026-02-04 | 12M | 10.26 | 10.76 |
| 2026-02-12 | 3M | 10.26 | 10.51 |
| 2026-02-12 | 6M | 10.28 | 10.53 |
| 2026-02-12 | 12M | 10.29 | 10.79 |

---

## konia_daily (2 rows)

| date | rate_pct | volume_billions | high | low |
|------|--------:|---------:|-----:|----:|
| 2026-02-09 | 11.16 | — | — | — |
| 2026-02-16 | 10.55 | — | — | — |

---

## pkrv_daily (21 rows, 3 dates)

| date | points | min_yield | max_yield |
|------|-------:|----------:|----------:|
| 2026-02-04 | 4 | 10.1977 | 10.3997 |
| 2026-02-09 | 8 | 10.14 | 11.00 |
| 2026-02-16 | 9 | 10.1977 | 11.4998 |

---

## sbp_policy_rates (1 row)

| rate_date | policy_rate | ceiling_rate | floor_rate | source |
|-----------|----------:|----------:|----------:|--------|
| 2026-01-30 | 0.105 (10.5%) | 0.11 | 0.11 | SBP_MSM |

*Note: rates stored as decimals (0.105 = 10.5%), different convention from kibor_daily which uses percentages directly*

---

## kibor_rates (3 rows) — Legacy table

| rate_date | tenor_months | bid | offer | source |
|-----------|------------:|----:|------:|--------|
| 2026-01-30 | 3 | 0.1024 | 0.03 | SBP_MSM |
| 2026-01-30 | 6 | 0.1024 | 0.03 | SBP_MSM |
| 2026-01-30 | 12 | 0.1024 | 0.03 | SBP_MSM |

*Note: This is a legacy table from fi_sync_service.py. Offer values (0.03) look incorrect. The `kibor_daily` table is the primary KIBOR table.*

---

## Backfill Sources Available

| Source | Command | Expected Rows |
|--------|---------|---------------|
| **SIR PDF** — T-Bills, PIBs, KIBOR, GIS (~2-4 years) | `psxsync backfill-rates --source sir` | ~500+ |
| **PIB Archive PDF** — All PIB auctions since Dec 2000 | `psxsync backfill-rates --source pib` | ~800+ |
| **KIBOR Daily PDFs** — Daily KIBOR from 2008 | `psxsync backfill-rates --source kibor` | ~10,000+ |
| **All three** | `psxsync backfill-rates --source all` | — |

UI buttons also available in **Treasury Dashboard > Sync Treasury Data > Historical Backfill**.
