# SBP EasyData Series — Requested Downloads

API: `https://easydata.sbp.org.pk/api/v1`
Key: Needs renewal (90-day expiry) — generate at My Account -> Generate API Key

## Series to Fetch

### T-Bills Auction Results (6 series)
Dataset: `TS_GP_BAM_SIRTBIL_AH` — Structure of Interest Rate: T-Bills Auction Result

| Series Key | Suffix | Description |
|---|---|---|
| `TS_GP_BAM_SIRTBIL_AH.TB0010` | TB0010 | 3-Month T-Bill Cut-Off Yield |
| `TS_GP_BAM_SIRTBIL_AH.TB0020` | TB0020 | 6-Month T-Bill Cut-Off Yield |
| `TS_GP_BAM_SIRTBIL_AH.TB0030` | TB0030 | 12-Month T-Bill Cut-Off Yield |
| `TS_GP_BAM_SIRTBIL_AH.TB0040` | TB0040 | 3-Month T-Bill Weighted Avg Yield |
| `TS_GP_BAM_SIRTBIL_AH.TB0050` | TB0050 | 6-Month T-Bill Weighted Avg Yield |
| `TS_GP_BAM_SIRTBIL_AH.TB0060` | TB0060 | 12-Month T-Bill Weighted Avg Yield |

Frequency: Ad-hoc (auction dates)
Use: Treasury dashboard, yield curve construction, fixed income analytics

### Government Debt (2 series)

| Series Key | Dataset | Description |
|---|---|---|
| `TS_GP_BAM_CENGOVTD_M.CGD00430` | Central Government Debt | Monthly central govt domestic debt component |
| `TS_GP_BAM_GOVTDDL_M.GDDL00320` | Govt Domestic Debt & Liabilities | Monthly domestic debt detailed breakdown |

Frequency: Monthly
Use: Macro research, debt-to-GDP analysis, fiscal monitoring

### Non-Bank Financial Institutions (1 series)

| Series Key | Dataset | Description |
|---|---|---|
| `TS_GP_MFS_NBFIAL_HY.NBFIA03100` | Non-Bank FI Assets & Liabilities | Half-yearly NBFI balance sheet item |

Frequency: Half-yearly
Use: Financial sector analysis, NBFI market sizing

## Fetch Command

Once API key is renewed, run:
```bash
cd ~/pakfindata
python -m pakfindata.sources.sbp_easydata fetch-series TS_GP_BAM_SIRTBIL_AH.TB0010
python -m pakfindata.sources.sbp_easydata fetch-series TS_GP_BAM_SIRTBIL_AH.TB0020
python -m pakfindata.sources.sbp_easydata fetch-series TS_GP_BAM_SIRTBIL_AH.TB0030
python -m pakfindata.sources.sbp_easydata fetch-series TS_GP_BAM_SIRTBIL_AH.TB0040
python -m pakfindata.sources.sbp_easydata fetch-series TS_GP_BAM_SIRTBIL_AH.TB0050
python -m pakfindata.sources.sbp_easydata fetch-series TS_GP_BAM_SIRTBIL_AH.TB0060
python -m pakfindata.sources.sbp_easydata fetch-series TS_GP_MFS_NBFIAL_HY.NBFIA03100
python -m pakfindata.sources.sbp_easydata fetch-series TS_GP_BAM_CENGOVTD_M.CGD00430
python -m pakfindata.sources.sbp_easydata fetch-series TS_GP_BAM_GOVTDDL_M.GDDL00320
```

Output: `/mnt/e/psxdata/sbp_easydata/series/{key}.csv` + `.json`

## Already Downloaded (on disk)

| Dataset | Series | Description |
|---|---|---|
| `TS_GP_BAM_SIRKIBOR_D` | 18 CSVs | KIBOR/KIBID daily (1W to 3Y) — 5,400+ obs each |
| `TS_GP_ER_FAERPKR_M` | 48 CSVs | FX avg rates PKR per currency (24 currencies + appreciation) |
| `TS_GP_IR_SIRPR_AH` | 3 CSVs | SBP Policy Rate, Repo, Reverse Repo |

Total: 1,252 series files downloaded

## Also Needed (not in URLs above)

| Dataset | Series | Description |
|---|---|---|
| `TS_GP_IR_REPOMR_D` | 1 | Daily repo market rates (KONIA proxy) |
| `TS_GP_BAM_SIRPIBS_AH` | 16 | PIB auction results (all tenors) |
| `TS_GP_BAM_SIRWALDR_M` | 8 | Weighted avg lending/deposit rates |
| `TS_GP_ER_FMEERPKR_M` | 48 | FX month-end rates |
| `TS_GP_ES_FADERPKR_M` | 23 | FX daily avg rates |

## API Key Status

Current key: `6353A150564FE7E3...` — EXPIRED (400 Bad Request)
Renewal: https://easydata.sbp.org.pk -> Login -> My Account -> Generate API Key
Limits: 250 req/hour, 2000 req/day, 90-day expiry
