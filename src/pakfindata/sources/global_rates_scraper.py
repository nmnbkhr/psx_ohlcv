"""Global reference rates scraper — SOFR/EFFR from NY Fed, stubs for SONIA/EUSTR/TONA.

Post-LIBOR alternative reference rates (ARRs).
Primary source: NY Fed (unauthenticated JSON API).
"""

import logging
import time

import requests

logger = logging.getLogger(__name__)


class GlobalRatesScraper:
    """Scraper for global alternative reference rates (post-LIBOR)."""

    NYFED_BASE = "https://markets.newyorkfed.org/api/rates"

    def __init__(self, session: requests.Session | None = None):
        self.session = session or requests.Session()
        self.session.headers.update({
            "User-Agent": "PSX-OHLCV-Research/3.5",
            "Accept": "application/json",
        })

    def scrape_sofr(self, count: int = 100) -> list[dict]:
        """Fetch last N days of SOFR from NY Fed."""
        url = f"{self.NYFED_BASE}/secured/sofr/last/{count}.json"
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        results = []
        for item in data.get("refRates", []):
            results.append({
                "date": item["effectiveDate"],
                "rate_name": item.get("type", "SOFR"),
                "currency": "USD",
                "tenor": "ON",
                "rate": float(item["percentRate"]),
                "volume": float(item["volumeInBillions"]) if item.get("volumeInBillions") else None,
                "percentile_25": float(item["percentPercentile25"]) if item.get("percentPercentile25") else None,
                "percentile_75": float(item["percentPercentile75"]) if item.get("percentPercentile75") else None,
                "source": "nyfed",
            })

        logger.info("Scraped %d SOFR rates from NY Fed", len(results))
        return results

    def scrape_sofr_averages(self, count: int = 100) -> list[dict]:
        """Fetch SOFR averages (30-day, 90-day, 180-day) and SOFR Index.

        Note: NY Fed may not serve averages at this endpoint anymore.
        Falls back gracefully if only SOFR base rate is returned.
        """
        url = f"{self.NYFED_BASE}/secured/sofr/last/{count}.json"
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        tenor_map = {
            "SOFR": "ON",
            "SOFRINDEX": "INDEX",
            "SOFR30DAVG": "30D_AVG",
            "SOFR90DAVG": "90D_AVG",
            "SOFR180DAVG": "180D_AVG",
        }
        target_types = set(tenor_map.keys()) | {"SOFRAI"}

        results = []
        for item in data.get("refRates", []):
            rtype = item.get("type", "")
            if rtype not in target_types:
                continue

            # Derive rate_name: SOFR for base, SOFR_<suffix> for averages
            if rtype == "SOFR":
                rate_name = "SOFR"
            elif rtype == "SOFRINDEX":
                rate_name = "SOFR_INDEX"
            elif rtype == "SOFRAI":
                rate_name = "SOFR_AI"
            else:
                # SOFR30DAVG -> SOFR_30D_AVG
                suffix = rtype.replace("SOFR", "").replace("DAVG", "D_AVG")
                rate_name = f"SOFR_{suffix}"

            results.append({
                "date": item["effectiveDate"],
                "rate_name": rate_name,
                "currency": "USD",
                "tenor": tenor_map.get(rtype, rtype),
                "rate": float(item["percentRate"]),
                "volume": float(item["volumeInBillions"]) if item.get("volumeInBillions") else None,
                "percentile_25": float(item["percentPercentile25"]) if item.get("percentPercentile25") else None,
                "percentile_75": float(item["percentPercentile75"]) if item.get("percentPercentile75") else None,
                "source": "nyfed",
            })

        logger.info("Scraped %d SOFR average rates from NY Fed", len(results))
        return results

    def scrape_effr(self, count: int = 100) -> list[dict]:
        """Fetch Effective Federal Funds Rate — context for SOFR."""
        url = f"{self.NYFED_BASE}/unsecured/effr/last/{count}.json"
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        results = []
        for item in data.get("refRates", []):
            results.append({
                "date": item["effectiveDate"],
                "rate_name": "EFFR",
                "currency": "USD",
                "tenor": "ON",
                "rate": float(item["percentRate"]),
                "volume": float(item["volumeInBillions"]) if item.get("volumeInBillions") else None,
                "percentile_25": float(item["percentPercentile25"]) if item.get("percentPercentile25") else None,
                "percentile_75": float(item["percentPercentile75"]) if item.get("percentPercentile75") else None,
                "source": "nyfed",
            })

        logger.info("Scraped %d EFFR rates from NY Fed", len(results))
        return results

    def scrape_sonia(self, days: int = 100) -> list[dict]:
        """Fetch SONIA rate from Bank of England.

        Uses the BoE Statistical Interactive Database CSV API.
        Series: IUDSOIA (Sterling Overnight Index Average).
        Endpoint: _iadb-fromshowcolumns.asp (note the _iadb- prefix).
        """
        from datetime import datetime, timedelta

        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        # BoE date format: DD/Mon/YYYY
        date_from = start_date.strftime("%d/%b/%Y")
        date_to = end_date.strftime("%d/%b/%Y")

        url = (
            "https://www.bankofengland.co.uk/boeapps/database/_iadb-fromshowcolumns.asp"
            f"?csv.x=yes&Datefrom={date_from}&Dateto={date_to}"
            "&SeriesCodes=IUDSOIA&CSVF=TN&UsingCodes=Y&VPD=Y&VFD=N"
        )

        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()

        results = []
        lines = resp.text.strip().split("\n")

        # Skip header line
        for line in lines[1:]:
            line = line.strip()
            if not line:
                continue

            parts = line.split(",")
            if len(parts) < 2:
                continue

            date_str = parts[0].strip()
            rate_str = parts[1].strip()

            try:
                rate = float(rate_str)
            except (ValueError, TypeError):
                continue

            # Parse BoE date "DD Mon YYYY" -> "YYYY-MM-DD"
            try:
                dt = datetime.strptime(date_str, "%d %b %Y")
                iso_date = dt.strftime("%Y-%m-%d")
            except ValueError:
                logger.warning("Unparseable SONIA date: %s", date_str)
                continue

            results.append({
                "date": iso_date,
                "rate_name": "SONIA",
                "currency": "GBP",
                "tenor": "ON",
                "rate": rate,
                "volume": None,
                "percentile_25": None,
                "percentile_75": None,
                "source": "boe",
            })

        logger.info("Scraped %d SONIA rates from Bank of England", len(results))
        return results

    def scrape_eustr(self, days: int = 100) -> list[dict]:
        """Fetch EUSTR rate from European Central Bank.

        Uses the ECB Statistical Data Warehouse API (CSV).
        Dataset: EST (Euro Short-Term Rate).
        Series: B.EU000A2X2A25.WT (weighted trimmed mean).
        """
        from datetime import datetime, timedelta

        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        url = (
            "https://data-api.ecb.europa.eu/service/data/EST/B.EU000A2X2A25.WT"
            f"?format=csvdata&startPeriod={start_str}&endPeriod={end_str}"
        )

        # Override Accept header — session default (application/json) makes ECB
        # return JSON instead of CSV despite format=csvdata parameter.
        resp = self.session.get(url, timeout=30, headers={"Accept": "text/csv"})
        resp.raise_for_status()

        import csv
        import io

        results = []
        reader = csv.reader(io.StringIO(resp.text))

        try:
            header = next(reader)
        except StopIteration:
            logger.warning("EUSTR API returned empty response")
            return results

        try:
            date_idx = header.index("TIME_PERIOD")
            value_idx = header.index("OBS_VALUE")
        except ValueError:
            logger.error("EUSTR CSV missing expected columns. Headers: %s", header)
            return results

        for row in reader:
            if len(row) <= max(date_idx, value_idx):
                continue

            date_str = row[date_idx].strip()
            rate_str = row[value_idx].strip()

            if not rate_str:
                continue

            try:
                rate = float(rate_str)
            except (ValueError, TypeError):
                continue

            # ECB dates are already YYYY-MM-DD
            results.append({
                "date": date_str,
                "rate_name": "EUSTR",
                "currency": "EUR",
                "tenor": "ON",
                "rate": rate,
                "volume": None,
                "percentile_25": None,
                "percentile_75": None,
                "source": "ecb",
            })

        logger.info("Scraped %d EUSTR rates from ECB", len(results))
        return results

    def scrape_tona(self, days: int = 100) -> list[dict]:
        """Fetch TONA rate from Bank of Japan.

        Uses the BoJ Time-Series Data pre-built CSV file (Shift_JIS encoded).
        Series: FM01'STRDCLUCON (Uncollateralized Call O/N, daily average).
        URL: stat-search.boj.or.jp/ssi/mtshtml/csv/fm01_d_1.csv

        The CSV contains the full history (~10k rows since 1998).
        We filter to the requested date range client-side.

        Fallback: returns empty list on failure.
        TONA is lower priority — JPY instruments are rare on PSX.
        """
        from datetime import datetime, timedelta

        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        url = "https://www.stat-search.boj.or.jp/ssi/mtshtml/csv/fm01_d_1.csv"

        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.warning("BoJ TONA endpoint failed: %s", e)
            return []

        # CSV is Shift_JIS encoded with 9 metadata header lines,
        # then "YYYY/MM/DD,rate" rows (rate = "NA" on holidays).
        try:
            text = resp.content.decode("shift_jis")
        except (UnicodeDecodeError, LookupError):
            text = resp.text  # fallback to requests' detected encoding

        results = []
        for line in text.split("\n")[9:]:  # skip 9 header lines
            line = line.strip()
            if not line:
                continue

            parts = line.split(",")
            if len(parts) < 2:
                continue

            date_str = parts[0].strip()
            rate_str = parts[1].strip()

            if not rate_str or rate_str == "NA":
                continue

            # Parse BoJ date "YYYY/MM/DD" -> datetime for range filtering
            try:
                dt = datetime.strptime(date_str, "%Y/%m/%d")
            except ValueError:
                continue

            if dt < start_date:
                continue
            if dt > end_date:
                continue

            try:
                rate = float(rate_str)
            except (ValueError, TypeError):
                continue

            results.append({
                "date": dt.strftime("%Y-%m-%d"),
                "rate_name": "TONA",
                "currency": "JPY",
                "tenor": "ON",
                "rate": rate,
                "volume": None,
                "percentile_25": None,
                "percentile_75": None,
                "source": "boj",
            })

        logger.info("Scraped %d TONA rates from Bank of Japan", len(results))
        return results

    def sync_all(self, con) -> dict:
        """Sync all available rates into database."""
        from pakfindata.db.repositories.global_rates import ensure_tables, upsert_global_rate

        ensure_tables(con)
        stats = {}

        # SOFR (primary)
        try:
            sofr_data = self.scrape_sofr(count=100)
            ok = 0
            for row in sofr_data:
                if upsert_global_rate(con, **row):
                    ok += 1
            con.commit()
            stats["SOFR"] = ok
        except Exception as e:
            logger.error("SOFR sync failed: %s", e)
            stats["SOFR"] = f"ERROR: {e}"

        time.sleep(1)  # polite rate limit

        # SOFR Averages
        try:
            avg_data = self.scrape_sofr_averages(count=100)
            ok = 0
            for row in avg_data:
                if upsert_global_rate(con, **row):
                    ok += 1
            con.commit()
            stats["SOFR_AVG"] = ok
        except Exception as e:
            logger.error("SOFR averages sync failed: %s", e)
            stats["SOFR_AVG"] = f"ERROR: {e}"

        time.sleep(1)

        # EFFR
        try:
            effr_data = self.scrape_effr(count=100)
            ok = 0
            for row in effr_data:
                if upsert_global_rate(con, **row):
                    ok += 1
            con.commit()
            stats["EFFR"] = ok
        except Exception as e:
            logger.error("EFFR sync failed: %s", e)
            stats["EFFR"] = f"ERROR: {e}"

        time.sleep(1)

        # SONIA
        try:
            sonia_data = self.scrape_sonia(days=150)  # ~100 business days
            ok = 0
            for row in sonia_data:
                if upsert_global_rate(con, **row):
                    ok += 1
            con.commit()
            stats["SONIA"] = ok
        except Exception as e:
            logger.error("SONIA sync failed: %s", e)
            stats["SONIA"] = f"ERROR: {e}"

        time.sleep(1)

        # EUSTR
        try:
            eustr_data = self.scrape_eustr(days=150)
            ok = 0
            for row in eustr_data:
                if upsert_global_rate(con, **row):
                    ok += 1
            con.commit()
            stats["EUSTR"] = ok
        except Exception as e:
            logger.error("EUSTR sync failed: %s", e)
            stats["EUSTR"] = f"ERROR: {e}"

        time.sleep(1)

        # TONA (lower priority — JPY rare on PSX)
        try:
            tona_data = self.scrape_tona(days=150)
            ok = 0
            for row in tona_data:
                if upsert_global_rate(con, **row):
                    ok += 1
            con.commit()
            stats["TONA"] = ok
        except Exception as e:
            logger.warning("TONA sync failed (non-critical): %s", e)
            stats["TONA"] = f"SKIPPED: {e}"

        logger.info("Global rates sync complete: %s", stats)
        return stats
