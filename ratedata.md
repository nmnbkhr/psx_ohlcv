# Rate & Treasury Tables — Sample Data (10 rows each)

> Generated: 2026-02-17 | DB: `/mnt/e/psxdata/psx.sqlite`

---

## tbill_auctions (4 rows total)

| auction_date | tenor | cutoff_yield | weighted_avg_yield | cutoff_price | amount_accepted_B |
|--------------|-------|-------------:|-------------------:|-------------:|------------------:|
| 2026-02-04 | 1M | 10.1977 | — | — | — |
| 2026-02-04 | 3M | 10.1983 | — | — | — |
| 2026-02-04 | 6M | 10.3237 | — | — | — |
| 2026-02-04 | 12M | 10.3997 | — | — | — |

**Columns:** auction_date, tenor, target_amount_billions, bids_received_billions, amount_accepted_billions, cutoff_yield, cutoff_price, weighted_avg_yield, maturity_date, settlement_date, scraped_at

---

## pib_auctions (5 rows total)

| auction_date | tenor | pib_type | cutoff_yield | coupon_rate | amount_accepted_B |
|--------------|-------|----------|-------------:|------------:|------------------:|
| 2026-02-04 | 2Y | Fixed | 10.338 | — | — |
| 2026-02-04 | 3Y | Fixed | 10.2489 | — | — |
| 2026-02-04 | 5Y | Fixed | 10.75 | — | — |
| 2026-02-04 | 10Y | Fixed | 11.239 | — | — |
| 2026-02-04 | 15Y | Fixed | 11.4998 | — | — |

**Columns:** auction_date, tenor, pib_type, target_amount_billions, bids_received_billions, amount_accepted_billions, cutoff_yield, cutoff_price, coupon_rate, maturity_date, scraped_at

---

## gis_auctions (6 rows total)

| auction_date | gis_type | tenor | cutoff_rental_rate |
|--------------|----------|------:|-------------------:|
| 2023-12-21 | GIS Fixed Rate Return | 5Y | 100.0022 |
| 2023-12-21 | GIS Variable Rate Return | 5Y | 98.76 |
| 2023-12-21 | GIS Fixed Rate Return 3Y | 3Y | 100.2842 |
| 2023-12-21 | GIS Fixed Rate Return 5Y | 5Y | 100.0022 |
| 2023-12-21 | GIS Variable Rate Return 3Y | 3Y | 99.08 |
| 2023-12-21 | GIS Variable Rate Return 5Y | 5Y | 98.76 |

**Columns:** auction_date, gis_type, tenor, target_amount_billions, amount_accepted_billions, cutoff_rental_rate, maturity_date, scraped_at

---

## kibor_daily (6 rows total)

| date | tenor | bid | offer |
|------|-------|----:|------:|
| 2026-02-04 | 3M | 10.26 | 10.51 |
| 2026-02-04 | 6M | 10.26 | 10.51 |
| 2026-02-04 | 12M | 10.26 | 10.76 |
| 2026-02-12 | 3M | 10.26 | 10.51 |
| 2026-02-12 | 6M | 10.28 | 10.53 |
| 2026-02-12 | 12M | 10.29 | 10.79 |

**Columns:** date, tenor, bid, offer, scraped_at

---

## konia_daily (2 rows total)

| date | rate_pct | volume_billions | high | low |
|------|--------:|----------------:|-----:|----:|
| 2026-02-09 | 11.16 | — | — | — |
| 2026-02-16 | 10.55 | — | — | — |

**Columns:** date, rate_pct, volume_billions, high, low, scraped_at

---

## pkrv_daily (21 rows total — showing 10)

| date | tenor_months | yield_pct | source |
|------|------------:|----------:|--------|
| 2026-02-04 | 1 | 10.1977 | MTB Auction |
| 2026-02-04 | 3 | 10.1983 | MTB Auction |
| 2026-02-04 | 6 | 10.3237 | MTB Auction |
| 2026-02-04 | 12 | 10.3997 | MTB Auction |
| 2026-02-09 | 1 | 10.1977 | MTB Auction |
| 2026-02-09 | 3 | 10.1983 | MTB Auction |
| 2026-02-09 | 24 | 10.19 | PIB Auction |
| 2026-02-09 | 36 | 10.14 | PIB Auction |
| 2026-02-09 | 60 | 10.525 | PIB Auction |
| 2026-02-09 | 120 | 11.00 | PIB Auction |

**Columns:** date, tenor_months, yield_pct, source, scraped_at

---

## sbp_policy_rates (1 row total)

| rate_date | policy_rate | ceiling_rate | floor_rate | overnight_repo_rate | source |
|-----------|----------:|----------:|----------:|--------------------:|--------|
| 2026-01-30 | 0.105 | 0.11 | 0.11 | — | SBP_MSM |

**Note:** Rates stored as decimals (0.105 = 10.5%)

**Columns:** rate_date, policy_rate, ceiling_rate, floor_rate, overnight_repo_rate, source, ingested_at

---

## kibor_rates (3 rows total) — Legacy

| rate_date | tenor_months | bid | offer | source |
|-----------|------------:|----:|------:|--------|
| 2026-01-30 | 3 | 0.1024 | 0.03 | SBP_MSM |
| 2026-01-30 | 6 | 0.1024 | 0.03 | SBP_MSM |
| 2026-01-30 | 12 | 0.1024 | 0.03 | SBP_MSM |

**Note:** Legacy table from fi_sync_service. Offer values (0.03) are incorrect. Use `kibor_daily` instead.

**Columns:** rate_date, tenor_months, bid, offer, source, ingested_at
