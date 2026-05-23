#!/usr/bin/env python3
"""MUFAP NAV History — Browser-based Highcharts CSV Downloader.

Phase A: Scan Expense Ratios page (tab=5) → update _fund_master.csv (append-only)
Phase B: For each fund in master → open FundDetail → click "All" → extract CSV
Phase C: Summary & sync log

Architecture:
  Xvfb (virtual display) → undetected-chromedriver (non-headless) → MUFAP site

Usage:
  python mufap_nav_downloader.py                     # full sync: scan → update master → download NAVs
  python mufap_nav_downloader.py --fund-id 12768     # single fund (skip Phase A)
  python mufap_nav_downloader.py --scan-only          # Phase A only: update master, no NAV downloads
  python mufap_nav_downloader.py --no-xvfb           # skip Xvfb (if WSLg/X11 works)
"""

import argparse
import io
import json
import logging
import os
import random
import shutil
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("mufap_nav_downloader")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MUFAP_BASE = "https://www.mufap.com.pk"
FUND_LIST_URL = f"{MUFAP_BASE}/Industry/IndustryStatDaily?tab=5"
FUND_DETAIL_URL = f"{MUFAP_BASE}/FundProfile/FundDetail?FundID={{fund_id}}"

OUTPUT_DIR = Path("/mnt/e/psxdata/mufapnav")
FUND_MASTER_PATH = OUTPUT_DIR / "_fund_master.csv"

MASTER_COLUMNS = ["fund_id", "fund_name", "amc", "category", "sector", "first_seen", "last_seen"]

STATIC_EXTENSIONS = frozenset([
    ".js", ".css", ".png", ".jpg", ".gif", ".ico",
    ".woff", ".woff2", ".svg", ".ttf", ".eot",
])


# ---------------------------------------------------------------------------
# Display & Chrome helpers
# ---------------------------------------------------------------------------


def _has_display() -> bool:
    return bool(os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"))


def _has_virtual_display() -> bool:
    return shutil.which("Xvfb") is not None


def _find_chrome_binary() -> str | None:
    for name in ["google-chrome", "google-chrome-stable", "chromium", "chromium-browser"]:
        path = shutil.which(name)
        if path:
            return path
    pw_dir = os.path.expanduser("~/.cache/ms-playwright")
    if os.path.isdir(pw_dir):
        for root, _dirs, files in os.walk(pw_dir):
            for f in files:
                if f in ("chrome", "chromium"):
                    full = os.path.join(root, f)
                    if os.access(full, os.X_OK):
                        return full
    return None


def _get_chrome_version(chrome_bin: str) -> int | None:
    try:
        result = subprocess.run(
            [chrome_bin, "--version"],
            capture_output=True, text=True, timeout=10,
        )
        for part in result.stdout.strip().split():
            if "." in part:
                try:
                    return int(part.split(".")[0])
                except ValueError:
                    continue
    except Exception as e:
        logger.debug("Could not detect Chrome version: %s", e)
    return None


# ---------------------------------------------------------------------------
# Xvfb management
# ---------------------------------------------------------------------------


def _start_xvfb():
    if _has_display():
        logger.info("Using existing display: %s", os.environ.get("DISPLAY"))
        return None
    try:
        from pyvirtualdisplay import Display
        display = Display(visible=False, size=(1920, 1080))
        display.start()
        logger.info("Started Xvfb via pyvirtualdisplay (DISPLAY=%s)", os.environ.get("DISPLAY"))
        return display
    except ImportError:
        pass
    except Exception as e:
        logger.debug("pyvirtualdisplay failed: %s", e)
    if _has_virtual_display():
        try:
            proc = subprocess.Popen(
                ["Xvfb", ":99", "-screen", "0", "1920x1080x24"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            os.environ["DISPLAY"] = ":99"
            time.sleep(1)
            logger.info("Started Xvfb via subprocess on :99")
            return proc
        except Exception as e:
            logger.warning("Raw Xvfb failed: %s", e)
    logger.warning("No display available and cannot start Xvfb")
    return None


def _stop_xvfb(display):
    if display is None:
        return
    try:
        if hasattr(display, "stop"):
            display.stop()
        elif hasattr(display, "terminate"):
            display.terminate()
            display.wait(timeout=5)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# UC Driver creation
# ---------------------------------------------------------------------------


def _create_driver(chrome_bin: str, version_main: int | None):
    import undetected_chromedriver as uc

    options = uc.ChromeOptions()
    options.binary_location = chrome_bin
    if not _has_display():
        options.add_argument("--headless=new")
    else:
        options.add_argument("--start-minimized")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    kwargs = {"options": options}
    if version_main:
        kwargs["version_main"] = version_main
    driver = uc.Chrome(**kwargs)
    try:
        driver.execute_cdp_cmd("Network.enable", {})
    except Exception:
        pass
    return driver


# ---------------------------------------------------------------------------
# Fund Master CSV — append-only persistence
# ---------------------------------------------------------------------------


def _load_master() -> pd.DataFrame:
    """Load existing fund master CSV. Returns empty DataFrame if not found."""
    if FUND_MASTER_PATH.exists():
        df = pd.read_csv(FUND_MASTER_PATH, dtype={"fund_id": int})
        logger.info("[master] Loaded %d funds from %s", len(df), FUND_MASTER_PATH)
        return df
    return pd.DataFrame(columns=MASTER_COLUMNS)


def _save_master(df: pd.DataFrame):
    """Save fund master CSV."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(FUND_MASTER_PATH, index=False)
    logger.info("[master] Saved %d funds to %s", len(df), FUND_MASTER_PATH)


def _merge_master(existing: pd.DataFrame, scraped: list[dict]) -> tuple[pd.DataFrame, int]:
    """Merge scraped funds into existing master. Returns (updated_df, new_count)."""
    now = datetime.now().isoformat(timespec="seconds")

    if not scraped:
        # Nothing scraped — return existing master unchanged
        if existing.empty:
            return pd.DataFrame(columns=MASTER_COLUMNS), 0
        return existing, 0

    scraped_df = pd.DataFrame(scraped)

    if existing.empty:
        scraped_df["first_seen"] = now
        scraped_df["last_seen"] = now
        return scraped_df[MASTER_COLUMNS], len(scraped_df)

    existing_ids = set(existing["fund_id"].tolist())
    scraped_ids = set(scraped_df["fund_id"].tolist())

    # Update last_seen for existing funds that appear in scrape
    existing.loc[existing["fund_id"].isin(scraped_ids), "last_seen"] = now

    # Find new funds
    new_ids = scraped_ids - existing_ids
    new_count = len(new_ids)

    if new_ids:
        new_rows = scraped_df[scraped_df["fund_id"].isin(new_ids)].copy()
        new_rows["first_seen"] = now
        new_rows["last_seen"] = now
        # Ensure columns match
        for col in MASTER_COLUMNS:
            if col not in new_rows.columns:
                new_rows[col] = ""
        merged = pd.concat([existing, new_rows[MASTER_COLUMNS]], ignore_index=True)

        # Log new funds
        for _, row in new_rows.iterrows():
            logger.info("[master] New: %d %s", row["fund_id"], row["fund_name"])
    else:
        merged = existing

    return merged, new_count


# ---------------------------------------------------------------------------
# Phase A — Scan Expense Ratios page → Update Fund Master
# ---------------------------------------------------------------------------


def _is_static(url: str) -> bool:
    for ext in STATIC_EXTENSIONS:
        if url.endswith(ext):
            return True
    return False


def phase_a_scan_and_update_master(driver) -> tuple[pd.DataFrame, int]:
    """Scrape tab=5, merge into _fund_master.csv.

    Returns (master_df, new_funds_inserted).
    """
    from bs4 import BeautifulSoup

    logger.info("[Phase A] Loading Expense Ratios page: %s", FUND_LIST_URL)
    driver.get(FUND_LIST_URL)
    time.sleep(8)  # tab=5 can be slow to render

    page_src = driver.page_source
    soup = BeautifulSoup(page_src, "html.parser")

    # Debug: save raw HTML on first run for inspection
    debug_path = OUTPUT_DIR / "_debug_tab5.html"
    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        debug_path.write_text(page_src, encoding="utf-8")
    except Exception:
        pass

    scraped = _parse_fund_table(soup)

    if not scraped:
        logger.warning("[Phase A] No funds parsed from page. Check %s for HTML structure.", debug_path)

    logger.info("[master] Loaded %d funds from Expense Ratios page", len(scraped))

    existing = _load_master()
    merged, new_count = _merge_master(existing, scraped)

    logger.info(
        "[master] Existing in master: %d | New funds inserted: %d | Total in master: %d",
        len(existing), new_count, len(merged),
    )

    _save_master(merged)
    return merged, new_count


def _parse_fund_table(soup) -> list[dict]:
    """Parse the HTML table on tab=5 into a list of fund dicts.

    Table structure:
    - Group header rows (class 'Datatable-Grouping'): contain AMC name
    - Data rows (class 'fund-block'): Sector(d-none) | Fund(link) | Category | Inception | NAV | ...
    - AMC comes from the preceding group header row
    """
    funds = []
    seen = set()
    current_amc = ""

    # Find all <tr> tags in the page (the table may not have class="table")
    all_rows = soup.find_all("tr")
    if not all_rows:
        logger.warning("[Phase A] No <tr> tags found, falling back to link scan")
        return _fallback_link_scan(soup)

    for row in all_rows:
        # Check if this is an AMC group header row
        if "Datatable-Grouping" in " ".join(row.get("class", [])):
            group_link = row.find("a", class_="group-link")
            if group_link:
                current_amc = group_link.get_text(strip=True)
            else:
                td = row.find("td")
                if td:
                    current_amc = td.get_text(strip=True)
            continue

        # Check if this is a fund data row
        link = row.find("a", href=lambda h: h and "FundDetail?FundID=" in h)
        if link is None:
            continue

        href = link["href"]
        try:
            fund_id = int(href.split("FundID=")[1].split("&")[0].split('"')[0])
        except (ValueError, IndexError):
            continue
        if fund_id in seen:
            continue
        seen.add(fund_id)

        fund_name = link.get_text(strip=True) or f"Fund-{fund_id}"
        cells = row.find_all("td")
        cell_texts = [c.get_text(strip=True) for c in cells]

        # Row layout: [0] Sector (d-none hidden) | [1] Fund (link) | [2] Category | [3] Inception | [4] NAV | ...
        sector = ""
        category = ""

        # Sector is the hidden td with class d-none
        for c in cells:
            if "d-none" in " ".join(c.get("class", [])):
                sector = c.get_text(strip=True)
                break

        # Category is the td right after the fund link td
        fund_cell_idx = None
        for idx, c in enumerate(cells):
            if c.find("a", href=lambda h: h and "FundDetail" in str(h)):
                fund_cell_idx = idx
                break
        if fund_cell_idx is not None and fund_cell_idx + 1 < len(cell_texts):
            category = cell_texts[fund_cell_idx + 1]

        funds.append({
            "fund_id": fund_id,
            "fund_name": fund_name,
            "amc": current_amc,
            "category": category,
            "sector": sector,
        })

    return funds


def _fallback_link_scan(soup) -> list[dict]:
    funds = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "FundDetail?FundID=" in href:
            try:
                fid = int(href.split("FundID=")[1].split("&")[0].split('"')[0])
                if fid not in seen:
                    seen.add(fid)
                    funds.append({
                        "fund_id": fid,
                        "fund_name": a.get_text(strip=True) or f"Fund-{fid}",
                        "amc": "", "category": "", "sector": "",
                    })
            except (ValueError, IndexError):
                continue
    return funds


# ---------------------------------------------------------------------------
# Network request capture
# ---------------------------------------------------------------------------


def _capture_network_requests(driver) -> list[dict]:
    requests_found = []
    try:
        logs = driver.get_log("performance")
        for entry in logs:
            try:
                msg = json.loads(entry["message"])["message"]
                method = msg.get("method", "")

                if method == "Network.requestWillBeSent":
                    params = msg.get("params", {})
                    req = params.get("request", {})
                    url = req.get("url", "")
                    if "mufap.com.pk" not in url or _is_static(url):
                        continue
                    requests_found.append({
                        "url": url,
                        "method": req.get("method", "GET"),
                        "type": params.get("type", ""),
                        "postData": req.get("postData", None),
                    })

                elif method == "Network.responseReceived":
                    params = msg.get("params", {})
                    resp = params.get("response", {})
                    url = resp.get("url", "")
                    if "mufap.com.pk" in url and not _is_static(url):
                        request_id = params.get("requestId")
                        if request_id:
                            try:
                                body = driver.execute_cdp_cmd(
                                    "Network.getResponseBody", {"requestId": request_id},
                                )
                                body_text = body.get("body", "")
                                for r in requests_found:
                                    if r["url"] == url:
                                        r["response_preview"] = body_text[:500] if body_text else None
                                        break
                            except Exception:
                                pass
            except (json.JSONDecodeError, KeyError):
                continue
    except Exception as e:
        logger.debug("Performance log capture error: %s", e)
    return requests_found


# ---------------------------------------------------------------------------
# Phase B — For each fund in master, download NAV
# ---------------------------------------------------------------------------


def phase_b_extract_fund(driver, fund_id: int, fund_name: str,
                         index: int, total: int,
                         capture_network: bool = False) -> dict:
    pad = len(str(total))
    url = FUND_DETAIL_URL.format(fund_id=fund_id)
    result = {"fund_id": fund_id, "fund_name": fund_name,
              "rows": 0, "status": "skip", "network_requests": []}

    try:
        driver.get(url)
        time.sleep(6)

        if capture_network:
            result["network_requests"] = _capture_network_requests(driver)
            if result["network_requests"]:
                logger.info("=== NETWORK REQUESTS (fund %d) ===", fund_id)
                for req in result["network_requests"]:
                    post_info = f" POST: {req['postData'][:120]}" if req.get("postData") else ""
                    logger.info("  %s %s [%s]%s", req["method"], req["url"], req["type"], post_info)
                    if req.get("response_preview"):
                        logger.info("    Response: %s...", req["response_preview"][:200])
                logger.info("=== END NETWORK REQUESTS ===")

        _click_all_range(driver)
        time.sleep(4)

        if capture_network:
            new_reqs = _capture_network_requests(driver)
            if new_reqs:
                logger.info("=== NEW REQUESTS after 'All' click ===")
                for req in new_reqs:
                    logger.info("  %s %s", req["method"], req["url"])
                result["network_requests"].extend(new_reqs)

        csv_data = _extract_highcharts_csv(driver)

        if csv_data:
            rows = csv_data.strip().count("\n")
            result["rows"] = rows
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            (OUTPUT_DIR / f"{fund_id}.csv").write_text(csv_data, encoding="utf-8")
            result["status"] = "saved"
            logger.info("[%*d/%d] %d %s \u2014 %s rows \u2014 saved",
                        pad, index, total, fund_id, fund_name, f"{rows:,}")
        else:
            logger.info("[%*d/%d] %d %s \u2014 SKIP (no chart data)",
                        pad, index, total, fund_id, fund_name)

    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)
        logger.warning("[%*d/%d] %d %s \u2014 ERROR: %s",
                       pad, index, total, fund_id, fund_name, e)

    return result


def _click_all_range(driver):
    try:
        driver.execute_script("""
            if (typeof Highcharts !== 'undefined' && Highcharts.charts) {
                for (var i = 0; i < Highcharts.charts.length; i++) {
                    var chart = Highcharts.charts[i];
                    if (chart && chart.rangeSelector && chart.rangeSelector.buttons) {
                        var buttons = chart.rangeSelector.buttons;
                        var allBtn = buttons[buttons.length - 1];
                        if (allBtn && allBtn.element) { allBtn.element.onclick(); }
                    }
                }
            }
        """)
        return
    except Exception:
        pass
    try:
        from selenium.webdriver.common.by import By
        for btn in driver.find_elements(By.CSS_SELECTOR, ".highcharts-range-selector-buttons text"):
            if btn.text.strip().lower() == "all":
                btn.click()
                return
    except Exception:
        pass
    try:
        driver.execute_script("""
            var texts = document.querySelectorAll('.highcharts-range-selector-buttons text');
            for (var i = 0; i < texts.length; i++) {
                if (texts[i].textContent.trim().toLowerCase() === 'all') {
                    texts[i].parentElement.dispatchEvent(new MouseEvent('click', {bubbles: true}));
                    break;
                }
            }
        """)
    except Exception:
        logger.debug("Could not click 'All' range selector")


def _extract_highcharts_csv(driver) -> str | None:
    try:
        csv = driver.execute_script("""
            if (typeof Highcharts !== 'undefined' && Highcharts.charts) {
                for (var i = 0; i < Highcharts.charts.length; i++) {
                    var chart = Highcharts.charts[i];
                    if (chart && typeof chart.getCSV === 'function') { return chart.getCSV(); }
                }
            }
            return null;
        """)
        if csv and len(csv) > 50:
            return csv
    except Exception as e:
        logger.debug("getCSV() failed: %s", e)

    try:
        data = driver.execute_script("""
            if (typeof Highcharts !== 'undefined' && Highcharts.charts) {
                for (var i = 0; i < Highcharts.charts.length; i++) {
                    var chart = Highcharts.charts[i];
                    if (chart && chart.series && chart.series.length > 0) {
                        var points = chart.series[0].points || chart.series[0].data;
                        if (points && points.length > 0) {
                            var rows = [];
                            for (var j = 0; j < points.length; j++) {
                                var p = points[j];
                                if (p && p.x !== undefined && p.y !== undefined) { rows.push([p.x, p.y]); }
                            }
                            return rows;
                        }
                    }
                }
            }
            return null;
        """)
        if data and len(data) > 0:
            lines = ['"DateTime","NAV"']
            for ts, val in data:
                dt = datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m-%d")
                lines.append(f'"{dt}",{val}')
            return "\n".join(lines)
    except Exception as e:
        logger.debug("Series extraction failed: %s", e)

    return None


# ---------------------------------------------------------------------------
# Fast API-based download (no browser needed)
# ---------------------------------------------------------------------------

MUFAP_API_URL = f"{MUFAP_BASE}/AMC/GetFundDetailbyAMCByDate"
_HEADERS_JSON = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Content-Type": "application/json",
    "X-Requested-With": "XMLHttpRequest",
}
_TRANSIENT_ERRORS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
)


def _make_session() -> requests.Session:
    """Create a requests.Session with connection pooling and keep-alive."""
    s = requests.Session()
    s.headers.update(_HEADERS_JSON)
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=14, pool_maxsize=14, max_retries=0,
    )
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def _fetch_nav_via_api(
    fund_id: int, session: requests.Session, max_retries: int = 3,
) -> tuple[list[dict], str | None]:
    """Fetch NAV history for a fund via MUFAP JSON API.

    Returns (nav_records, error_msg). nav_records is a list of {date, nav} dicts.
    Uses a shared Session for connection pooling / keep-alive.
    """
    date = datetime.now().strftime("%Y-%m-%d")
    body = {"FundID": str(fund_id), "Date": date}

    last_error = None
    for attempt in range(max_retries):
        try:
            r = session.post(MUFAP_API_URL, json=body, timeout=120)
            r.raise_for_status()
            break
        except _TRANSIENT_ERRORS as e:
            last_error = e
            delay = (2 ** attempt) + random.uniform(0, 1.0)
            logger.debug("Transient error fund %d (attempt %d/%d): %s", fund_id, attempt + 1, max_retries, e)
            time.sleep(delay)
        except requests.exceptions.HTTPError as e:
            return [], str(e)
    else:
        return [], str(last_error)

    try:
        resp = r.json()
        inner_str = resp.get("data", "{}")
        if isinstance(inner_str, str):
            inner = json.loads(inner_str) if inner_str else {}
        else:
            inner = inner_str

        history = inner.get("Table1", [])
        if not history:
            return [], None

        records = []
        for row in history:
            date_str = row.get("entryDate") or row.get("CalDate")
            nav_val = row.get("netval")
            if date_str and nav_val is not None:
                date_clean = str(date_str)[:10]
                try:
                    nav_float = float(nav_val)
                except (ValueError, TypeError):
                    continue
                records.append({"date": date_clean, "nav": nav_float})
        return records, None
    except Exception as e:
        return [], str(e)


def _save_nav_csv(fund_id: int, records: list[dict]):
    """Save NAV records as CSV — build in memory, write + flush."""
    buf = io.StringIO()
    buf.write('"DateTime","Nav"\n')
    for rec in records:
        buf.write(f'"{rec["date"]}",{rec["nav"]}\n')
    out_path = OUTPUT_DIR / f"{fund_id}.csv"
    fd = os.open(str(out_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
    try:
        os.write(fd, buf.getvalue().encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)


def _download_one_fund(fund_id: int, fund_name: str, session: requests.Session) -> dict:
    """Download NAV for a single fund via API. Returns result dict."""
    records, err = _fetch_nav_via_api(fund_id, session)
    if err:
        return {"fund_id": fund_id, "fund_name": fund_name, "rows": 0, "status": "error", "error": err}
    if not records:
        return {"fund_id": fund_id, "fund_name": fund_name, "rows": 0, "status": "skip"}
    _save_nav_csv(fund_id, records)
    return {"fund_id": fund_id, "fund_name": fund_name, "rows": len(records), "status": "saved"}


def run_download_fast(
    fund_id: int | None = None,
    workers: int = 12,
    progress_callback=None,
) -> dict:
    """Fast API-based download — no browser needed.

    Reads fund IDs from _fund_master.csv, fetches NAV via POST API,
    saves CSVs. Uses ThreadPoolExecutor for concurrency with a shared
    requests.Session (connection pooling / keep-alive) and in-memory
    CSV building with flushed writes.
    """
    summary = {
        "run_at": datetime.now().isoformat(timespec="seconds"),
        "mode": "fast-api",
        "master_total": 0,
        "processed": 0,
        "saved": 0,
        "skipped": 0,
        "errors": 0,
        "error_fund_ids": [],
    }

    master = _load_master()
    if master.empty:
        logger.error("No fund master found at %s. Run --scan-only first.", FUND_MASTER_PATH)
        return summary

    summary["master_total"] = len(master)

    if fund_id:
        funds_to_process = master[master["fund_id"] == fund_id]
        if funds_to_process.empty:
            logger.error("Fund ID %d not found in master.", fund_id)
            return summary
    else:
        funds_to_process = master

    total = len(funds_to_process)
    summary["processed"] = total
    pad = len(str(total))
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("[fast] Processing %d funds via API (workers=%d)...", total, workers)

    fund_list = [(int(row["fund_id"]), str(row["fund_name"])) for _, row in funds_to_process.iterrows()]
    completed = 0
    session = _make_session()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {}
        for fid, fname in fund_list:
            fut = executor.submit(_download_one_fund, fid, fname, session)
            futures[fut] = (fid, fname)

        for fut in as_completed(futures):
            completed += 1
            fid, fname = futures[fut]
            try:
                result = fut.result()
                if result["status"] == "saved":
                    summary["saved"] += 1
                    logger.info("[%*d/%d] %d %s — %s rows — saved",
                                pad, completed, total, fid, fname, f"{result['rows']:,}")
                elif result["status"] == "error":
                    summary["errors"] += 1
                    summary["error_fund_ids"].append(str(fid))
                    logger.warning("[%*d/%d] %d %s — ERROR: %s",
                                   pad, completed, total, fid, fname, result.get("error", ""))
                else:
                    summary["skipped"] += 1
                    logger.info("[%*d/%d] %d %s — SKIP (no NAV data)",
                                pad, completed, total, fid, fname)
            except Exception as e:
                summary["errors"] += 1
                summary["error_fund_ids"].append(str(fid))
                logger.warning("[%*d/%d] %d %s — EXCEPTION: %s",
                               pad, completed, total, fid, fname, e)

            if progress_callback:
                progress_callback(completed, total, f"{fid} {fname}")

    session.close()

    summary["finished_at"] = datetime.now().isoformat(timespec="seconds")

    logger.info("=" * 60)
    logger.info("SUMMARY (fast API mode)")
    logger.info("  Master total: %d", summary["master_total"])
    logger.info("  Processed: %d | Saved: %d | Skipped: %d | Errors: %d",
                summary["processed"], summary["saved"], summary["skipped"], summary["errors"])
    if summary["error_fund_ids"]:
        logger.info("  Error fund IDs: %s", ", ".join(summary["error_fund_ids"][:20]))
    logger.info("=" * 60)

    _save_sync_log(summary)
    return summary


# ---------------------------------------------------------------------------
# Main orchestrator (browser-based)
# ---------------------------------------------------------------------------


def run_download(
    fund_id: int | None = None,
    scan_only: bool = False,
    skip_scan: bool = False,
    no_xvfb: bool = False,
    progress_callback=None,
) -> dict:
    """Main entry point.

    Phase A: Scan tab=5 → update _fund_master.csv (unless --fund-id or --skip-scan)
    Phase B: For each fund in master → download NAV CSV (unless --scan-only)
    Phase C: Summary + sync log
    """
    import undetected_chromedriver  # noqa: F401

    summary = {
        "run_at": datetime.now().isoformat(timespec="seconds"),
        "master_total": 0,
        "new_funds_inserted": 0,
        "processed": 0,
        "saved": 0,
        "skipped": 0,
        "errors": 0,
        "error_fund_ids": [],
        "api_endpoints_discovered": [],
    }

    display = None
    if not no_xvfb:
        display = _start_xvfb()

    chrome_bin = _find_chrome_binary()
    if chrome_bin is None:
        logger.error("No Chrome/Chromium binary found.")
        _stop_xvfb(display)
        return summary

    version_main = _get_chrome_version(chrome_bin)
    logger.info("Chrome: %s (version %s)", chrome_bin, version_main)

    driver = None
    try:
        driver = _create_driver(chrome_bin, version_main)
        logger.info("UC driver created successfully")

        # ── Phase A ── Scan & update master
        if fund_id:
            logger.info("[Phase A] Single fund mode: %d (skipping table scan)", fund_id)
            master = _load_master()
            # If fund not in master, add it
            if master.empty or fund_id not in master["fund_id"].values:
                now = datetime.now().isoformat(timespec="seconds")
                new_row = pd.DataFrame([{
                    "fund_id": fund_id, "fund_name": f"Fund-{fund_id}",
                    "amc": "", "category": "", "sector": "",
                    "first_seen": now, "last_seen": now,
                }])
                master = pd.concat([master, new_row], ignore_index=True)
                _save_master(master)
            new_count = 0
        elif skip_scan:
            logger.info("[Phase A] --skip-scan: using existing master (no web scrape)")
            master = _load_master()
            if master.empty:
                logger.error("No existing master found. Run without --skip-scan first.")
                return summary
            new_count = 0
        else:
            master, new_count = phase_a_scan_and_update_master(driver)

        summary["master_total"] = len(master)
        summary["new_funds_inserted"] = new_count

        if scan_only:
            logger.info("[Phase A] --scan-only: skipping Phase B NAV downloads")
            summary["finished_at"] = datetime.now().isoformat(timespec="seconds")
            _save_sync_log(summary)
            return summary

        # ── Phase B ── Download NAVs from master
        if fund_id:
            # Single fund: just process that one
            funds_to_process = master[master["fund_id"] == fund_id]
        else:
            funds_to_process = master

        total = len(funds_to_process)
        summary["processed"] = total
        logger.info("[Phase B] Processing %d funds from master...", total)

        for i, (_, row) in enumerate(funds_to_process.iterrows()):
            fid = int(row["fund_id"])
            fname = str(row["fund_name"])

            result = phase_b_extract_fund(
                driver, fid, fname,
                index=i + 1, total=total,
                capture_network=(i == 0),
            )

            if result["status"] == "saved":
                summary["saved"] += 1
            elif result["status"] == "error":
                summary["errors"] += 1
                summary["error_fund_ids"].append(str(fid))
            else:
                summary["skipped"] += 1

            if i == 0 and result.get("network_requests"):
                seen = set()
                for req in result["network_requests"]:
                    u = req["url"]
                    if u not in seen:
                        seen.add(u)
                        summary["api_endpoints_discovered"].append(u)

            if progress_callback:
                progress_callback(i + 1, total, f"{fid} {fname}")

            if i < total - 1:
                time.sleep(2)

    except Exception as e:
        logger.error("Fatal error: %s", e)
        summary["fatal_error"] = str(e)
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        _stop_xvfb(display)

    summary["finished_at"] = datetime.now().isoformat(timespec="seconds")

    # ── Phase C ── Summary
    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info("  Master total: %d | New inserted: %d", summary["master_total"], summary["new_funds_inserted"])
    logger.info("  Processed: %d | Saved: %d | Skipped: %d | Errors: %d",
                summary["processed"], summary["saved"], summary["skipped"], summary["errors"])
    if summary["api_endpoints_discovered"]:
        logger.info("  Discovered API endpoints:")
        for ep in summary["api_endpoints_discovered"]:
            logger.info("    %s", ep)
    if summary["error_fund_ids"]:
        logger.info("  Error fund IDs: %s", ", ".join(summary["error_fund_ids"][:20]))
    logger.info("=" * 60)

    _save_sync_log(summary)
    return summary


def _save_sync_log(summary: dict):
    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        log_path = OUTPUT_DIR / "_sync_log.json"
        log_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
        logger.info("Sync log saved: %s", log_path)
    except Exception as e:
        logger.warning("Could not save sync log: %s", e)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="MUFAP NAV History — Full Sync Downloader"
    )
    parser.add_argument("--fund-id", type=int, default=None,
                        help="Single fund by ID (skip Phase A table scan)")
    parser.add_argument("--scan-only", action="store_true",
                        help="Phase A only: scan tab=5, update master, no NAV downloads")
    parser.add_argument("--skip-scan", action="store_true",
                        help="Skip Phase A, use existing master CSV, go straight to NAV downloads")
    parser.add_argument("--no-xvfb", action="store_true",
                        help="Skip Xvfb (if WSLg/X11 available)")
    parser.add_argument("--fast", action="store_true",
                        help="Fast mode: use API requests (no browser). Reads fund IDs from master CSV.")
    parser.add_argument("--workers", type=int, default=12,
                        help="Number of concurrent workers for --fast mode (default: 12)")
    args = parser.parse_args()

    if args.fast:
        run_download_fast(fund_id=args.fund_id, workers=args.workers)
    else:
        run_download(fund_id=args.fund_id, scan_only=args.scan_only,
                     skip_scan=args.skip_scan, no_xvfb=args.no_xvfb)


if __name__ == "__main__":
    main()
