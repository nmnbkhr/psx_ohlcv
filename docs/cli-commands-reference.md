# pfsync CLI Commands Reference

> All data stored in SQLite at `/mnt/e/psxdata/psx.sqlite`
> Data root: `/mnt/e/psxdata/` (override with `PSX_DATA_ROOT` env var)
> DB path override: `PSX_DB_PATH` env var

## File System Layout

| Path | What |
|---|---|
| `/mnt/e/psxdata/psx.sqlite` | SQLite database (all tables) |
| `/mnt/e/psxdata/market_summary/` | Market summary CSVs (from `.Z` / PDF downloads) |
| `/mnt/e/psxdata/csv/` | CSV exports |
| `/mnt/e/psxdata/backups/` | Database backups |
| `/mnt/e/psxdata/logs/` | Log files (`pfsync.log`) |

---

## SYMBOLS & MASTER DATA

| Command | Table(s) | Frequency | Granularity |
|---|---|---|---|
| `pfsync symbols refresh` | `symbols` | On demand | All-at-once (PSX market watch) |
| `pfsync symbols list` | — (read) | — | — |
| `pfsync symbols string` | — (read) | — | — |
| `pfsync sectors refresh` | `sectors` | On demand | All-at-once (PSX sector page) |
| `pfsync sectors list` | — (read) | — | — |
| `pfsync sectors export` | — | — | File on disk (CSV) |
| `pfsync master refresh` | `symbols` | On demand | All-at-once (`listed_cmp.lst.Z`) |
| `pfsync master list` | — (read) | — | — |
| `pfsync master export` | — | — | File on disk (CSV) |

---

## EOD OHLCV SYNC

| Command | Table(s) | Frequency | Granularity |
|---|---|---|---|
| `pfsync sync --all` | `eod_ohlcv`, `psx_indices`, `trading_sessions`, `sync_runs`, `sync_failures` | Daily EOD | Per-symbol (sequential) |
| `pfsync sync --all --async` | Same as above | Daily EOD | Per-symbol (concurrent HTTP) |
| `pfsync sync --symbols HBL,OGDC` | Same as above | Daily EOD | Per-symbol (specified list) |

---

## MARKET SUMMARY (.Z / PDF files)

| Command | Table(s) | Frequency | Granularity |
|---|---|---|---|
| `pfsync market-summary day --date YYYY-MM-DD` | `downloaded_market_summary_dates` + File on disk (CSV) | Daily | All-at-once per date |
| `pfsync market-summary day --import-eod` | Above + `eod_ohlcv` | Daily | All symbols in that day's file |
| `pfsync market-summary range --start --end` | `downloaded_market_summary_dates` + Files on disk | Date range | All-at-once per date |
| `pfsync market-summary range --import-eod` | Above + `eod_ohlcv` | Date range | All symbols per date |
| `pfsync market-summary last --days N` | Same as range | Last N days | All-at-once per date |
| `pfsync market-summary retry-failed` | Same | On demand | Failed dates only |
| `pfsync market-summary retry-missing` | Same | On demand | Missing (404) dates only |
| `pfsync market-summary status` | — (read) | — | — |
| `pfsync market-summary list-missing` | — (read) | — | — |
| `pfsync market-summary list-failed` | — (read) | — | — |

---

## INTRADAY

| Command | Table(s) | Frequency | Granularity |
|---|---|---|---|
| `pfsync intraday sync --symbol X` | `intraday_bars`, `intraday_sync_state` | Intraday (1-min bars) | Per-symbol |
| `pfsync intraday sync-all` | `intraday_bars`, `intraday_sync_state` | Intraday (1-min bars) | All active symbols (bulk) |
| `pfsync intraday show --symbol X` | — (read) | — | — |

---

## REGULAR MARKET (live polling)

| Command | Table(s) | Frequency | Granularity |
|---|---|---|---|
| `pfsync regular-market snapshot` | `regular_market_current`, `regular_market_snapshots` + File on disk (CSV) | On demand | All symbols (single snapshot) |
| `pfsync regular-market listen` | `regular_market_current`, `regular_market_snapshots` + Files on disk | Continuous (N-second interval) | All symbols per poll |
| `pfsync regular-market show` | — (read) | — | — |

---

## COMPANY DATA (DPS Company Pages)

| Command | Table(s) | Frequency | Granularity |
|---|---|---|---|
| `pfsync company refresh --symbol X` | `company_profile`, `company_key_people` | On demand | Per-symbol |
| `pfsync company snapshot --symbol X` | `company_quote_snapshots` | On demand | Per-symbol (or comma-separated list) |
| `pfsync company listen --symbol X` | `company_quote_snapshots` | Continuous (interval) | Per-symbol / list |
| `pfsync company deep-scrape --symbol X` | `company_profile`, `company_key_people`, `company_fundamentals`, `company_financials`, `company_ratios`, `company_payouts`, `company_snapshots`, `equity_structure`, `corporate_announcements` | On demand | Per-symbol / `--all` |
| `pfsync company import-payouts --symbol X --file F` | `company_payouts` | On demand | Per-symbol from HTML file on disk |
| `pfsync company fetch-dividends` | `financial_announcements` | On demand | All (PSX announcements page) |
| `pfsync company sync-sectors` | `symbols` (sector_name column) | On demand | All (from company_profile) |
| `pfsync company show --symbol X` | — (read) | — | — |

---

## ANNOUNCEMENTS

| Command | Table(s) | Frequency | Granularity |
|---|---|---|---|
| `pfsync announcements sync` | `company_announcements`, `corporate_events`, `dividend_payouts`, `announcements_sync_status` | On demand | All-at-once (PSX feeds) |
| `pfsync announcements service start` | Same as above | Background (hourly default) | All-at-once |
| `pfsync announcements status` | — (read) | — | — |

---

## UNIVERSE & INSTRUMENTS (Phase 1: ETFs, REITs, Indexes)

| Command | Table(s) | Frequency | Granularity |
|---|---|---|---|
| `pfsync universe seed-phase1` | `instruments`, `instrument_membership` | On demand (seed) | All-at-once from JSON config |
| `pfsync universe add --type --symbol --name` | `instruments` | On demand | Single instrument |
| `pfsync universe list` | — (read) | — | — |
| `pfsync instruments sync-eod` | `ohlcv_instruments`, `instruments_sync_runs` | Daily EOD | Per-instrument (ETF/REIT/INDEX) |
| `pfsync instruments rankings` | `instrument_rankings` | On demand (compute) | All instruments |
| `pfsync instruments sync-status` | — (read) | — | — |

---

## FX (Phase 2: Macro Context)

| Command | Table(s) | Frequency | Granularity |
|---|---|---|---|
| `pfsync fx seed` | `fx_pairs` | On demand (seed) | All default pairs |
| `pfsync fx sync` | `fx_ohlcv`, `fx_sync_runs` | Daily | Per-pair (all active) |
| `pfsync fx compute-adjusted` | `fx_adjusted_metrics` | On demand (compute) | Per-symbol (equity metrics) |
| `pfsync fx show --pair X` | — (read) | — | — |
| `pfsync fx status` | — (read) | — | — |

---

## MUFAP (Phase 2.5: Mutual Funds)

| Command | Table(s) | Frequency | Granularity |
|---|---|---|---|
| `pfsync mufap seed` | `mutual_funds` | On demand (seed) | All funds from MUFAP |
| `pfsync mufap sync` | `mutual_fund_nav`, `mutual_fund_sync_runs` | Daily | Per-fund (all active) |
| `pfsync mufap performance` | `fund_performance` | On demand | All funds (MUFAP tab=1 returns) |
| `pfsync mufap expense` | `mutual_funds` (expense_ratio column) | On demand | All funds (MUFAP tab=5) |
| `pfsync mufap resync-categories` | `mutual_funds` (category columns) | One-time fix | All funds |
| `pfsync mufap backfill-returns` | `fund_performance` | On demand (date range) | All funds, computed from NAV |
| `pfsync mufap show --fund X` | — (read) | — | — |
| `pfsync mufap list` | — (read) | — | — |
| `pfsync mufap rankings` | — (read) | — | — |
| `pfsync mufap status` | — (read) | — | — |

---

## BONDS (Phase 3)

| Command | Table(s) | Frequency | Granularity |
|---|---|---|---|
| `pfsync bonds init` | `bonds_master` | On demand (seed) | All bond types |
| `pfsync bonds load --master CSV` | `bonds_master` | On demand | Load from CSV file |
| `pfsync bonds load --quotes CSV` | `bond_quotes` | On demand | Load from CSV file |
| `pfsync bonds load --sample` | `bond_quotes` | On demand | Generated sample data |
| `pfsync bonds compute` | `bond_analytics_snapshots`, `yield_curve_points` | On demand (compute) | Per-bond or all |
| `pfsync bonds benchmark-sync` | `sbp_benchmark_snapshot` | On demand | All-at-once (SBP scrape) |
| `pfsync bonds smtv-sync` | `sbp_bond_trading_daily`, `sbp_bond_trading_summary` | Daily | All-at-once (SBP SMTV PDF) |
| `pfsync bonds smtv-backfill` | `sbp_bond_trading_daily`, `sbp_bond_trading_summary` | On demand | Historical (SBP archives) |
| `pfsync bonds list` | — (read) | — | — |
| `pfsync bonds quote --bond X` | — (read) | — | — |
| `pfsync bonds curve` | — (read) | — | — |
| `pfsync bonds status` | — (read) | — | — |

---

## SUKUK (Phase 3: PSX GIS & SBP)

| Command | Table(s) | Frequency | Granularity |
|---|---|---|---|
| `pfsync sukuk seed` | `sukuk_master` | On demand (seed) | All categories |
| `pfsync sukuk sync` | `sukuk_quotes`, `sukuk_yield_curve`, `sukuk_sync_runs` | On demand | Per-instrument / sample |
| `pfsync sukuk load --master CSV` | `sukuk_master` | On demand | Load from CSV file |
| `pfsync sukuk load --quotes CSV` | `sukuk_quotes` | On demand | Load from CSV file |
| `pfsync sukuk load --curve CSV` | `sukuk_yield_curve` | On demand | Load from CSV file |
| `pfsync sukuk compute` | `sukuk_analytics_snapshots` | On demand (compute) | Per-instrument or all |
| `pfsync sukuk sbp` | `sbp_primary_market_docs` | On demand | All SBP docs |
| `pfsync sukuk list` | — (read) | — | — |
| `pfsync sukuk show --instrument X` | — (read) | — | — |
| `pfsync sukuk curve` | — (read) | — | — |
| `pfsync sukuk compare` | — (read) | — | — |
| `pfsync sukuk status` | — (read) | — | — |

---

## FIXED INCOME (Phase 3.5: MTB, PIB, GOP Sukuk)

| Command | Table(s) | Frequency | Granularity |
|---|---|---|---|
| `pfsync fixed-income seed` | `fi_instruments` | On demand (seed) | All (sample or CSV) |
| `pfsync fixed-income sync` | `fi_quotes`, `fi_curves`, `fi_sync_runs` | On demand | All / from CSV |
| `pfsync fixed-income sync --all` | `fi_instruments`, `fi_quotes`, `fi_curves`, `sbp_pma_docs`, `fi_sync_runs` | On demand | Full pipeline |
| `pfsync fixed-income compute` | `fi_analytics` | On demand (compute) | Per-ISIN or all |
| `pfsync fixed-income sbp` | `sbp_pma_docs` + PDF files on disk (optional) | On demand | All SBP PMA docs |
| `pfsync fixed-income templates` | — | — | Files on disk (CSV templates) |
| `pfsync fixed-income service start` | All FI tables | Background (hourly) | All-at-once |
| `pfsync fixed-income list` | — (read) | — | — |
| `pfsync fixed-income show --isin X` | — (read) | — | — |
| `pfsync fixed-income curve` | — (read) | — | — |
| `pfsync fixed-income status` | — (read) | — | — |

---

## ETF (v3.0)

| Command | Table(s) | Frequency | Granularity |
|---|---|---|---|
| `pfsync etf sync` | `etf_master`, `etf_nav` | On demand | All ETFs (PSX DPS scrape) |
| `pfsync etf list` | — (read) | — | — |
| `pfsync etf show SYMBOL` | — (read) | — | — |

---

## TREASURY (v3.0: T-Bill + PIB Auctions)

| Command | Table(s) | Frequency | Granularity |
|---|---|---|---|
| `pfsync treasury sync` | `tbill_auctions`, `pib_auctions` | On demand | All-at-once (SBP scrape) |
| `pfsync treasury gis-sync` | `gis_auctions` | On demand | All-at-once (SBP GIS scrape) |
| `pfsync treasury tbill-latest` | — (read) | — | — |
| `pfsync treasury pib-latest` | — (read) | — | — |
| `pfsync treasury tbill-list` | — (read) | — | — |
| `pfsync treasury gis-list` | — (read) | — | — |
| `pfsync treasury summary` | — (read) | — | — |

---

## RATES (v3.0: PKRV, KONIA, KIBOR)

| Command | Table(s) | Frequency | Granularity |
|---|---|---|---|
| `pfsync rates sync` | `pkrv_daily`, `pkisrv_daily`, `pkfrv_daily`, `konia_daily`, `kibor_daily` | Daily | All-at-once (SBP scrape) |
| `pfsync rates konia` | — (read) | — | — |
| `pfsync rates kibor` | — (read) | — | — |
| `pfsync rates curve` | — (read) | — | — |
| `pfsync rates summary` | — (read) | — | — |

---

## FX-RATES (v3.0: SBP Interbank + Kerb)

| Command | Table(s) | Frequency | Granularity |
|---|---|---|---|
| `pfsync fx-rates sbp-sync` | `sbp_fx_interbank`, `sbp_fx_open_market` | Daily | All currencies (SBP scrape) |
| `pfsync fx-rates kerb-sync` | `forex_kerb` | Daily | All currencies (forex.pk scrape) |
| `pfsync fx-rates sync-all` | `sbp_fx_interbank`, `sbp_fx_open_market`, `forex_kerb` | Daily | Both sources |
| `pfsync fx-rates latest` | — (read) | — | — |
| `pfsync fx-rates spread CURRENCY` | — (read) | — | — |
| `pfsync fx-rates summary` | — (read) | — | — |

---

## DIVIDENDS (v3.0)

| Command | Table(s) | Frequency | Granularity |
|---|---|---|---|
| `pfsync dividends show SYMBOL` | — (read from `company_payouts`) | — | Per-symbol |
| `pfsync dividends yield SYMBOL` | — (read/compute) | — | Per-symbol |
| `pfsync dividends top` | — (read/compute) | — | All symbols |
| `pfsync dividends upcoming` | — (read) | — | All |

> Dividend data is populated by `company deep-scrape` and `announcements sync`.

---

## IPO (v3.0)

| Command | Table(s) | Frequency | Granularity |
|---|---|---|---|
| `pfsync ipo sync` | `ipo_listings` | On demand | All-at-once (PSX scrape) |
| `pfsync ipo list` | — (read) | — | — |
| `pfsync ipo upcoming` | — (read) | — | — |
| `pfsync ipo recent` | — (read) | — | — |
| `pfsync ipo show SYMBOL` | — (read) | — | — |

---

## VPS (v3.0: Pension Funds)

| Command | Table(s) | Frequency | Granularity |
|---|---|---|---|
| `pfsync vps list` | — (read from `mutual_funds` where type=VPS) | — | — |
| `pfsync vps nav FUND_ID` | — (read from `mutual_fund_nav`) | — | Per-fund |
| `pfsync vps performance` | — (read/compute) | — | All VPS funds |
| `pfsync vps summary` | — (read) | — | — |

> VPS data is populated by `mufap seed` + `mufap sync`.

---

## GLOBAL RATES (v5.1: SOFR, EFFR, SONIA, EUSTR)

| Command | Table(s) | Frequency | Granularity |
|---|---|---|---|
| `pfsync globalrates sync` | `global_reference_rates`, `term_reference_rates` | On demand | All-at-once (NY Fed API) |
| `pfsync globalrates latest` | — (read) | — | — |
| `pfsync globalrates spread` | — (read/compute) | — | — |
| `pfsync globalrates history RATE` | — (read) | — | Per-rate |

---

## NPC (v5.5: Naya Pakistan Certificate)

| Command | Table(s) | Frequency | Granularity |
|---|---|---|---|
| `pfsync npc sync` | `npc_rates` | On demand | All currencies/tenors (SBP scrape) |
| `pfsync npc latest` | — (read) | — | — |
| `pfsync npc curve` | — (read) | — | — |
| `pfsync npc spread` | — (read/compute) | — | — |
| `pfsync npc carry` | — (read/compute) | — | — |
| `pfsync npc dashboard` | — (read) | — | — |

---

## TICK COLLECTION (Real-time)

| Command | Table(s) | Frequency | Granularity |
|---|---|---|---|
| `pfsync collect-ticks` | `tick_data`, `tick_ohlcv` + (on stop) `eod_ohlcv` | Continuous (N-second polling) | All symbols (market-watch polling) |
| `pfsync tick-service start` | `raw_ticks`, `ohlcv_5s`, `index_raw_ticks`, `index_ohlcv_5s` | Real-time WebSocket | All symbols (psxterminal.com) |
| `pfsync tick-service daemon` | Same as above | Background daemon | All symbols |
| `pfsync tick-service stop` | — | — | — |
| `pfsync tick-service status` | — (read) | — | — |

---

## BACKFILL & ORCHESTRATION

| Command | Table(s) | Frequency | Granularity |
|---|---|---|---|
| `pfsync backfill-rates --source sir` | `tbill_auctions`, `pib_auctions`, `kibor_daily`, `gis_auctions` | One-time historical | All-at-once (SBP SIR PDF) |
| `pfsync backfill-rates --source pib` | `pib_auctions` | One-time historical | All-at-once (SBP PIB archive PDF, 25 years) |
| `pfsync backfill-rates --source kibor` | `kibor_daily`, `sbp_policy_rates`, `kibor_rates` | One-time historical | All-at-once (SBP KIBOR PDFs, per year) |
| `pfsync backfill-rates --source all` | All of the above | One-time historical | All |
| `pfsync sync-all` | Runs 11 steps (see below) | Daily (cron) | All-at-once (unified orchestrator) |
| `pfsync status` | — (read dashboard across 24 domains) | — | — |

### `sync-all` Steps (in order)

1. ETF NAV + metadata → `etf_master`, `etf_nav`
2. Treasury auctions (T-Bill + PIB) → `tbill_auctions`, `pib_auctions`
3. GIS auctions → `gis_auctions`
4. KONIA + KIBOR + yield curve → `pkrv_daily`, `pkisrv_daily`, `pkfrv_daily`, `konia_daily`, `kibor_daily`
5. SBP FX interbank → `sbp_fx_interbank`, `sbp_fx_open_market`
6. Kerb FX (forex.pk) → `forex_kerb`
7. FX microservice (interbank + KIBOR) → `sbp_fx_interbank`
8. IPO listings → `ipo_listings`
9. SBP benchmark snapshot → `sbp_benchmark_snapshot`
10. MUFAP fund performance (tab=1) → `fund_performance`
11. SIR PDF (treasury history) → `tbill_auctions`, `pib_auctions`, `kibor_daily`, `gis_auctions`

---

## ALL TABLES SUMMARY

| # | Table | Filled By |
|---|---|---|
| 1 | `symbols` | `symbols refresh`, `master refresh`, `company sync-sectors` |
| 2 | `sectors` | `sectors refresh` |
| 3 | `eod_ohlcv` | `sync`, `market-summary --import-eod`, `collect-ticks` (on stop) |
| 4 | `sync_runs` | `sync` |
| 5 | `sync_failures` | `sync` |
| 6 | `psx_indices` | `sync` |
| 7 | `trading_sessions` | `sync` |
| 8 | `downloaded_market_summary_dates` | `market-summary day/range/last` |
| 9 | `intraday_bars` | `intraday sync`, `intraday sync-all` |
| 10 | `intraday_sync_state` | `intraday sync`, `intraday sync-all` |
| 11 | `regular_market_current` | `regular-market snapshot/listen` |
| 12 | `regular_market_snapshots` | `regular-market snapshot/listen` |
| 13 | `company_profile` | `company refresh`, `company deep-scrape` |
| 14 | `company_key_people` | `company refresh`, `company deep-scrape` |
| 15 | `company_quote_snapshots` | `company snapshot`, `company listen` |
| 16 | `company_fundamentals` | `company deep-scrape` |
| 17 | `company_financials` | `company deep-scrape` |
| 18 | `company_ratios` | `company deep-scrape` |
| 19 | `company_payouts` | `company deep-scrape`, `company import-payouts` |
| 20 | `company_snapshots` | `company deep-scrape` |
| 21 | `equity_structure` | `company deep-scrape` |
| 22 | `corporate_announcements` | `company deep-scrape` |
| 23 | `financial_announcements` | `company fetch-dividends` |
| 24 | `company_announcements` | `announcements sync` |
| 25 | `corporate_events` | `announcements sync` |
| 26 | `dividend_payouts` | `announcements sync` |
| 27 | `announcements_sync_status` | `announcements sync` |
| 28 | `instruments` | `universe seed-phase1`, `universe add` |
| 29 | `instrument_membership` | `universe seed-phase1` |
| 30 | `ohlcv_instruments` | `instruments sync-eod` |
| 31 | `instrument_rankings` | `instruments rankings` |
| 32 | `instruments_sync_runs` | `instruments sync-eod` |
| 33 | `fx_pairs` | `fx seed` |
| 34 | `fx_ohlcv` | `fx sync` |
| 35 | `fx_adjusted_metrics` | `fx compute-adjusted` |
| 36 | `fx_sync_runs` | `fx sync` |
| 37 | `mutual_funds` | `mufap seed`, `mufap expense`, `mufap resync-categories` |
| 38 | `mutual_fund_nav` | `mufap sync` |
| 39 | `mutual_fund_sync_runs` | `mufap sync` |
| 40 | `fund_performance` | `mufap performance`, `mufap backfill-returns` |
| 41 | `bonds_master` | `bonds init`, `bonds load --master` |
| 42 | `bond_quotes` | `bonds load --quotes`, `bonds load --sample` |
| 43 | `bond_analytics_snapshots` | `bonds compute` |
| 44 | `yield_curve_points` | `bonds compute --curve` |
| 45 | `bond_sync_runs` | `bonds` (internal tracking) |
| 46 | `sbp_benchmark_snapshot` | `bonds benchmark-sync`, `sync-all` |
| 47 | `sbp_bond_trading_daily` | `bonds smtv-sync`, `bonds smtv-backfill` |
| 48 | `sbp_bond_trading_summary` | `bonds smtv-sync`, `bonds smtv-backfill` |
| 49 | `sukuk_master` | `sukuk seed`, `sukuk load --master` |
| 50 | `sukuk_quotes` | `sukuk sync`, `sukuk load --quotes` |
| 51 | `sukuk_yield_curve` | `sukuk sync --include-curves`, `sukuk load --curve` |
| 52 | `sukuk_analytics_snapshots` | `sukuk compute` |
| 53 | `sbp_primary_market_docs` | `sukuk sbp` |
| 54 | `sukuk_sync_runs` | `sukuk` (internal tracking) |
| 55 | `fi_instruments` | `fixed-income seed` |
| 56 | `fi_quotes` | `fixed-income sync` |
| 57 | `fi_curves` | `fixed-income sync` |
| 58 | `fi_analytics` | `fixed-income compute` |
| 59 | `sbp_pma_docs` | `fixed-income sbp` |
| 60 | `fi_events` | `fixed-income` (internal) |
| 61 | `fi_sync_runs` | `fixed-income` (internal tracking) |
| 62 | `etf_master` | `etf sync` |
| 63 | `etf_nav` | `etf sync` |
| 64 | `tbill_auctions` | `treasury sync`, `backfill-rates --source sir` |
| 65 | `pib_auctions` | `treasury sync`, `backfill-rates --source sir/pib` |
| 66 | `gis_auctions` | `treasury gis-sync`, `backfill-rates --source sir` |
| 67 | `pkrv_daily` | `rates sync` |
| 68 | `pkisrv_daily` | `rates sync` |
| 69 | `pkfrv_daily` | `rates sync` |
| 70 | `konia_daily` | `rates sync` |
| 71 | `kibor_daily` | `rates sync`, `backfill-rates --source sir/kibor` |
| 72 | `sbp_fx_interbank` | `fx-rates sbp-sync` |
| 73 | `sbp_fx_open_market` | `fx-rates sbp-sync` |
| 74 | `forex_kerb` | `fx-rates kerb-sync` |
| 75 | `ipo_listings` | `ipo sync` |
| 76 | `tick_data` | `collect-ticks` |
| 77 | `tick_ohlcv` | `collect-ticks` |
| 78 | `raw_ticks` | `tick-service` |
| 79 | `ohlcv_5s` | `tick-service` |
| 80 | `index_raw_ticks` | `tick-service` |
| 81 | `index_ohlcv_5s` | `tick-service` |
| 82 | `global_reference_rates` | `globalrates sync` |
| 83 | `term_reference_rates` | `globalrates sync` |
| 84 | `npc_rates` | `npc sync` |
| 85 | `sbp_policy_rates` | `backfill-rates --source kibor` |
| 86 | `kibor_rates` | `backfill-rates --source kibor` |
| 87 | `pdf_parse_log` | Financial PDF parsing (internal) |
| 88 | `post_close_turnover` | Post-close data (internal) |
| 89 | `company_website_scan` | Website scanner (internal) |
| 90 | `analytics_market_snapshot` | `compute_all_analytics` (internal) |
| 91 | `analytics_symbol_snapshot` | `compute_all_analytics` (internal) |
| 92 | `analytics_sector_snapshot` | `compute_all_analytics` (internal) |
| 93 | `psx_market_stats` | Market stats (internal) |
| 94 | `company_fundamentals_history` | `company deep-scrape` (history tracking) |
| 95 | `company_signal_snapshots` | Signal snapshots (internal) |
| 96 | `user_interactions` | UI interactions (internal) |
| 97 | `scrape_jobs` | Job tracking (internal) |
| 98 | `job_notifications` | Job notifications (internal) |
| 99 | `futures_eod` | Futures EOD data (internal) |
| 100 | `llm_cache` | LLM response cache (internal) |
