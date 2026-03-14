"""PMEX Margins Excel downloader and parser.

URL Pattern: https://pmex.com.pk/wp-content/uploads/{YYYY}/{MM:02d}/Margins-{DD:02d}-{MM:02d}-{YYYY}.xlsx
Auth:        Cloudflare managed challenge on pmex.com.pk
Frequency:   One file per trading day
404:         Weekend or holiday — not an error, skip gracefully

Download Strategy (in order):
  1. undetected-chromedriver non-headless with WSLg/X11 display (best CF bypass)
  2. undetected-chromedriver non-headless with Xvfb virtual display
  3. undetected-chromedriver headless (CF may block)
  4. Manual upload via Streamlit UI — always works

Note: requests, curl, curl_cffi, cloudscraper all get 403 from Cloudflare.
      Headless Chrome also gets blocked. Non-headless with real display works.

Parsing Quirks:
- header=4 (column headers at row 5)
- "Initial Magin" typo in sheet 1 (missing 'r')
- Product Groups in sheet 1 use merged cells -> ffill()
- Sheet 2 "Agri Margin" has NO product_group column
- Footer rows contain "UAN:", "YOUR FUTURES", "Copyrights", "pmex.com.pk"
- #N/A in reference_price -> is_active = False
- WCM column can be "-" -> None
"""

import logging
import os
import time
from datetime import date, timedelta
from io import BytesIO
from pathlib import Path

import pandas as pd

logger = logging.getLogger("pakfindata.commodities.fetcher_pmex_margins")

BASE_URL = "https://pmex.com.pk/wp-content/uploads"
CF_SOLVE_URL = "https://pmex.com.pk"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

FOOTER_KEYWORDS = ["UAN:", "YOUR FUTURES", "Copyrights", "pmex.com.pk"]

# Default download directory for margins files
MARGINS_DOWNLOAD_DIR = Path("/mnt/e/psxdata/commod/pmex_margins")


def margins_url(dt: date) -> str:
    """Build the direct download URL for a margins file."""
    return (
        f"{BASE_URL}/{dt.year}/{dt.month:02d}/"
        f"Margins-{dt.day:02d}-{dt.month:02d}-{dt.year}.xlsx"
    )


def _is_valid_xlsx(content: bytes) -> bool:
    """Check if content is a real Excel file (not HTML/CF block)."""
    if len(content) < 1000:
        return False
    if b"<!DOCTYPE" in content[:100] or b"<html" in content[:100]:
        return False
    return True


# ---------------------------------------------------------------------------
# Strategy 1: undetected-chromedriver (best CF bypass)
# ---------------------------------------------------------------------------


def _has_uc() -> bool:
    """Check if undetected-chromedriver is available."""
    try:
        import undetected_chromedriver  # noqa: F401
        return True
    except ImportError:
        return False


def _find_chrome_binary() -> str | None:
    """Find Chrome/Chromium binary on the system.

    Prefers native Linux binaries (work with chromedriver).
    Windows Chrome via WSL (/mnt/c/...) does NOT work with chromedriver.
    """
    import shutil

    # 1. Standard Linux paths
    for name in ["google-chrome", "google-chrome-stable", "chromium", "chromium-browser"]:
        path = shutil.which(name)
        if path:
            return path

    # 2. Playwright's bundled Chromium (native Linux binary, works in WSL)
    pw_dir = os.path.expanduser("~/.cache/ms-playwright")
    if os.path.isdir(pw_dir):
        for root, dirs, files in os.walk(pw_dir):
            for f in files:
                if f in ("chrome", "chromium"):
                    full = os.path.join(root, f)
                    if os.access(full, os.X_OK):
                        return full

    # NOTE: Windows Chrome (/mnt/c/.../chrome.exe) is NOT used —
    # chromedriver cannot connect to it across the WSL boundary.

    return None


def _get_chrome_version(chrome_bin: str) -> int | None:
    """Extract major version number from Chrome binary."""
    import subprocess
    try:
        result = subprocess.run(
            [chrome_bin, "--version"],
            capture_output=True, text=True, timeout=10,
        )
        # Output like "Google Chrome for Testing 145.0.7632.6"
        for part in result.stdout.strip().split():
            if "." in part:
                try:
                    return int(part.split(".")[0])
                except ValueError:
                    continue
    except Exception as e:
        logger.debug("Could not detect Chrome version: %s", e)
    return None


def _has_display() -> bool:
    """Check if a display server is available (WSLg, X11, or Wayland)."""
    return bool(os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"))


def _has_virtual_display() -> bool:
    """Check if Xvfb is available for virtual display."""
    import shutil
    return shutil.which("Xvfb") is not None


def _create_uc_driver(download_dir: str | None = None):
    """Create an undetected Chrome driver instance.

    Strategy order (non-headless bypasses Cloudflare, headless does not):
      1. Non-headless with existing display (WSLg / X11) — best
      2. Non-headless with Xvfb virtual display — good
      3. Headless — last resort (CF may block)

    Returns (driver, display) or (None, None).
    """
    import undetected_chromedriver as uc

    # Ensure DISPLAY is set for WSLg — Streamlit may not inherit it
    if not os.environ.get("DISPLAY") and Path("/tmp/.X11-unix/X0").exists():
        os.environ["DISPLAY"] = ":0"
        logger.info("Set DISPLAY=:0 (WSLg detected)")

    chrome_bin = _find_chrome_binary()
    if chrome_bin is None:
        logger.warning("No Chrome/Chromium binary found on system")
        return None, None

    version_main = _get_chrome_version(chrome_bin)
    logger.info("Using Chrome binary: %s (version %s)", chrome_bin, version_main)

    dl_dir = download_dir or str(MARGINS_DOWNLOAD_DIR)
    Path(dl_dir).mkdir(parents=True, exist_ok=True)

    prefs = {
        "download.default_directory": dl_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
    }

    def _make_options(headless: bool) -> uc.ChromeOptions:
        options = uc.ChromeOptions()
        options.binary_location = chrome_bin
        if headless:
            options.add_argument("--headless=new")
        else:
            options.add_argument("--start-minimized")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        options.add_experimental_option("prefs", prefs)
        return options

    def _make_driver(options: uc.ChromeOptions):
        kwargs = {"options": options}
        if version_main:
            kwargs["version_main"] = version_main
        return uc.Chrome(**kwargs)

    # Try 1: Non-headless with existing display (WSLg / X11 / Wayland)
    if _has_display():
        try:
            driver = _make_driver(_make_options(headless=False))
            logger.info("Chrome launched non-headless (display: %s)",
                        os.environ.get("DISPLAY", os.environ.get("WAYLAND_DISPLAY")))
            return driver, None
        except Exception as e:
            logger.debug("Non-headless Chrome failed: %s", e)

    # Try 2: Non-headless with Xvfb virtual display
    display = None
    if _has_virtual_display():
        try:
            from pyvirtualdisplay import Display
            display = Display(visible=False, size=(1920, 1080))
            display.start()
            logger.info("Started Xvfb virtual display for Chrome")

            driver = _make_driver(_make_options(headless=False))
            return driver, display
        except Exception as e:
            logger.debug("Virtual display Chrome failed: %s", e)
            if display:
                try:
                    display.stop()
                except Exception:
                    pass
                display = None

    # Try 3: Headless (last resort — CF may block this)
    try:
        driver = _make_driver(_make_options(headless=True))
        logger.info("Chrome launched headless (CF bypass unlikely)")
        return driver, None
    except Exception as e:
        logger.warning("Failed to create Chrome driver: %s", e)
        return None, None


def _uc_solve_cloudflare(driver) -> bool:
    """Visit pmex.com.pk to solve Cloudflare challenge. Returns True if solved."""
    try:
        logger.info("Solving Cloudflare challenge via undetected-chromedriver...")
        driver.get(CF_SOLVE_URL)
        time.sleep(5)  # Let CF scripts finish

        title = driver.title
        if "just a moment" in title.lower():
            # Wait a bit more
            time.sleep(5)
            title = driver.title

        solved = "just a moment" not in title.lower()
        if solved:
            logger.info("Cloudflare solved (title: %s)", title)
        else:
            logger.warning("Cloudflare may not be solved (title: %s)", title)
        return solved
    except Exception as e:
        logger.warning("CF solve error: %s", e)
        return False


def _uc_download_file(
    driver, url: str, download_dir: str, expected_filename: str, timeout: int = 15
) -> bytes | None:
    """Download a file via Chrome navigation. Returns file bytes or None.

    Detects 404/error pages quickly (within 3s) instead of waiting full timeout.
    """
    fpath = Path(download_dir) / expected_filename

    # Skip if already exists and valid
    if fpath.exists() and fpath.stat().st_size > 1000:
        content = fpath.read_bytes()
        if _is_valid_xlsx(content):
            logger.debug("Using existing file: %s", fpath)
            return content

    # Remove partial/old file
    for ext in ["", ".crdownload"]:
        try:
            (Path(download_dir) / (expected_filename + ext)).unlink(missing_ok=True)
        except OSError:
            pass

    try:
        driver.get(url)
    except Exception as e:
        # Chrome may throw on direct file downloads
        logger.debug("Navigation exception (may be OK for download): %s", e)

    # Wait for download — check for 404/error pages early to avoid long waits
    for i in range(timeout):
        time.sleep(1)

        # Check if file appeared (download started/completed)
        if fpath.exists() and fpath.stat().st_size > 1000:
            crdownload = Path(download_dir) / (expected_filename + ".crdownload")
            if not crdownload.exists():
                content = fpath.read_bytes()
                if _is_valid_xlsx(content):
                    return content
                else:
                    logger.debug("Downloaded file is not valid Excel")
                    return None

        # After 3 seconds, check if Chrome is showing an error page (404/not found)
        # instead of downloading — no point waiting the full timeout
        if i >= 2:
            try:
                page_url = driver.current_url
                title = driver.title.lower()
                # If Chrome stayed on the URL (not redirected) and shows error indicators
                if any(s in title for s in ["not found", "404", "error", "not available"]):
                    logger.debug("404 detected via page title for %s", url)
                    return None
                # If page source is HTML (not a download), it's an error page
                src = driver.page_source
                if src and len(src) < 2000 and ("not found" in src.lower() or "404" in src.lower()):
                    logger.debug("404 detected via page source for %s", url)
                    return None
                # If no .crdownload file appeared after 3s, likely not downloading
                crdownload = Path(download_dir) / (expected_filename + ".crdownload")
                if i >= 4 and not fpath.exists() and not crdownload.exists():
                    logger.debug("No download started after %ds for %s", i + 1, url)
                    return None
            except Exception:
                pass

    return None


def _cleanup_driver(driver, display=None):
    """Safely quit driver and stop virtual display."""
    if driver:
        try:
            driver.quit()
        except Exception:
            pass
    if display:
        try:
            display.stop()
        except Exception:
            pass


def _download_with_uc_single(
    url: str, dt: date, download_dir: str | None = None
) -> bytes | None:
    """Download a single margins file with its own UC driver session."""
    dl_dir = download_dir or str(MARGINS_DOWNLOAD_DIR)
    driver, display = _create_uc_driver(dl_dir)
    if driver is None:
        return None

    try:
        _uc_solve_cloudflare(driver)
        fname = f"Margins-{dt.day:02d}-{dt.month:02d}-{dt.year}.xlsx"
        return _uc_download_file(driver, url, dl_dir, fname)
    finally:
        _cleanup_driver(driver, display)


# ---------------------------------------------------------------------------
# Strategy 2: System curl (fallback for local networks)
# ---------------------------------------------------------------------------


def _download_with_curl(url: str, timeout: int = 30) -> tuple[bytes | None, int]:
    """Download a file using system curl.

    Works from local networks where Cloudflare doesn't challenge.
    Returns (raw_bytes | None, http_status_or_0).
    """
    import shutil
    import subprocess
    import tempfile

    curl_path = shutil.which("curl")
    if not curl_path:
        return None, 0

    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        result = subprocess.run(
            [
                curl_path, "-s", "-L",
                "-o", tmp_path,
                "-w", "%{http_code}",
                "--max-time", str(timeout),
                "-A", UA,
                url,
            ],
            capture_output=True,
            text=True,
            timeout=timeout + 5,
        )

        status_str = result.stdout.strip()
        try:
            status = int(status_str)
        except ValueError:
            status = 0

        if status == 404:
            return None, 404

        if result.returncode != 0:
            logger.debug("curl returned code %d (HTTP %s) for %s", result.returncode, status_str, url)
            return None, status

        content = Path(tmp_path).read_bytes()
        if not _is_valid_xlsx(content):
            return None, status or 403

        return content, 200

    except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as e:
        logger.debug("curl failed: %s", e)
        return None, 0
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Disk cache check (avoid launching Chrome for already-downloaded files)
# ---------------------------------------------------------------------------


def _check_disk_cache(
    target_date: date, walk_back_days: int, dl_dir: str
) -> tuple[bytes | None, date | None]:
    """Check if a valid margins file already exists on disk."""
    for i in range(walk_back_days + 1):
        dt = target_date - timedelta(days=i)
        if dt.weekday() >= 5:
            continue
        fname = f"Margins-{dt.day:02d}-{dt.month:02d}-{dt.year}.xlsx"
        fpath = Path(dl_dir) / fname
        if fpath.exists() and fpath.stat().st_size > 1000:
            content = fpath.read_bytes()
            if _is_valid_xlsx(content):
                logger.info("Using cached file: %s (%d bytes)", fpath.name, len(content))
                return content, dt
    return None, None


# ---------------------------------------------------------------------------
# Main fetch function (tries strategies in order)
# ---------------------------------------------------------------------------


def fetch_margins_file(
    target_date: date | None = None,
    walk_back_days: int = 5,
    driver=None,
    download_dir: str | None = None,
) -> tuple[bytes | None, date | None]:
    """Download margins Excel, walking back up to N days on 404.

    Strategy order:
      1. undetected-chromedriver (if available)
      2. System curl (subprocess)
    If driver is provided, reuses it (caller manages browser lifecycle).
    Returns (raw_bytes, actual_date) or (None, None).
    """
    if target_date is None:
        target_date = date.today()

    dl_dir = download_dir or str(MARGINS_DOWNLOAD_DIR)

    # Check disk cache first — avoid launching Chrome if file already downloaded
    cached = _check_disk_cache(target_date, walk_back_days, dl_dir)
    if cached[0] is not None:
        return cached

    # If caller provides a driver, use it directly
    if driver is not None:
        return _fetch_with_driver(target_date, walk_back_days, driver, dl_dir)

    # Use undetected-chromedriver (only method that bypasses Cloudflare)
    if _has_uc():
        result = _fetch_with_uc(target_date, walk_back_days, dl_dir)
        if result[0] is not None:
            return result

    logger.warning(
        "Could not download margins automatically. "
        "Use the manual upload in Streamlit UI."
    )
    return None, None


def _fetch_with_uc(
    target_date: date, walk_back_days: int, dl_dir: str
) -> tuple[bytes | None, date | None]:
    """Try downloading via undetected-chromedriver (own driver session)."""
    driver, display = _create_uc_driver(dl_dir)
    if driver is None:
        return None, None

    try:
        _uc_solve_cloudflare(driver)
        return _fetch_with_driver(target_date, walk_back_days, driver, dl_dir)
    except Exception as e:
        logger.warning("UC session failed: %s", e)
        return None, None
    finally:
        _cleanup_driver(driver, display)


def _fetch_with_driver(
    target_date: date, walk_back_days: int, driver, dl_dir: str
) -> tuple[bytes | None, date | None]:
    """Download margins using an existing UC driver (CF already solved)."""
    for i in range(walk_back_days + 1):
        dt = target_date - timedelta(days=i)
        if dt.weekday() >= 5:
            continue

        url = margins_url(dt)
        fname = f"Margins-{dt.day:02d}-{dt.month:02d}-{dt.year}.xlsx"

        content = _uc_download_file(driver, url, dl_dir, fname)
        if content is not None:
            logger.info("Downloaded margins for %s via UC (%d bytes)", dt, len(content))
            return content, dt

        logger.debug("No file for %s (holiday?), walking back", dt)

    return None, None


def _fetch_with_curl_walkback(
    target_date: date, walk_back_days: int
) -> tuple[bytes | None, date | None]:
    """Download margins using system curl with walkback."""
    for i in range(walk_back_days + 1):
        dt = target_date - timedelta(days=i)
        if dt.weekday() >= 5:
            continue

        url = margins_url(dt)
        content, status = _download_with_curl(url)

        if content is not None:
            logger.info("Downloaded margins for %s via curl (%d bytes)", dt, len(content))
            return content, dt

        if status == 404:
            logger.debug("404 for %s (holiday?), walking back", dt)
            continue

        if status == 403:
            logger.warning(
                "HTTP 403 for margins %s (Cloudflare). "
                "Use Streamlit upload or download in your browser.",
                dt,
            )
            continue

        if status != 0:
            logger.debug("HTTP %d for margins %s, walking back", status, dt)

    logger.warning("No margins file found within %d days of %s", walk_back_days, target_date)
    return None, None


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


def parse_margins_excel(raw_bytes: bytes, report_date: date) -> pd.DataFrame:
    """Parse both sheets of a margins Excel file.

    Returns DataFrame with standardized columns:
        report_date, sheet_name, product_group, contract_code,
        reference_price, initial_margin_pct, initial_margin_value,
        wcm, maintenance_margin, lower_limit, upper_limit,
        fx_rate, is_active
    """
    xls = pd.ExcelFile(BytesIO(raw_bytes))
    all_rows: list[dict] = []
    report_date_str = report_date.isoformat()

    for sheet_name in xls.sheet_names:
        try:
            df = pd.read_excel(xls, sheet_name=sheet_name, header=4)
        except Exception as e:
            logger.warning("Failed to parse sheet '%s': %s", sheet_name, e)
            continue

        # Drop fully unnamed columns and all-NaN rows
        df = df.loc[:, ~df.columns.str.startswith("Unnamed")]
        df = df.dropna(how="all")

        if df.empty:
            continue

        # Normalize column names to lowercase
        df.columns = [str(c).strip().lower() for c in df.columns]

        # Filter footer rows
        mask = pd.Series(True, index=df.index)
        for col in df.columns:
            if df[col].dtype == object:
                for kw in FOOTER_KEYWORDS:
                    mask = mask & ~df[col].astype(str).str.contains(kw, case=False, na=False)
        df = df[mask]

        if df.empty:
            continue

        # Identify columns by fuzzy matching
        col_map = _map_columns(df.columns.tolist(), sheet_name)

        # Extract product_group (sheet 1 only, with ffill for merged cells)
        has_product_group = col_map.get("product_group") is not None
        if has_product_group:
            pg_col = col_map["product_group"]
            df[pg_col] = df[pg_col].ffill()

        for _, row in df.iterrows():
            contract_code = _get_val(row, col_map.get("contract_code"))
            if not contract_code or not isinstance(contract_code, str):
                continue
            contract_code = contract_code.strip()
            if not contract_code or contract_code.lower() in ("nan", ""):
                continue

            ref_price = _parse_num(row, col_map.get("reference_price"))
            is_active = ref_price is not None

            rec = {
                "report_date": report_date_str,
                "sheet_name": sheet_name,
                "product_group": (
                    _get_val(row, col_map.get("product_group")) if has_product_group else None
                ),
                "contract_code": contract_code,
                "reference_price": ref_price,
                "initial_margin_pct": _parse_num(row, col_map.get("initial_margin_pct")),
                "initial_margin_value": _parse_num(row, col_map.get("initial_margin_value")),
                "wcm": _parse_num(row, col_map.get("wcm")),
                "maintenance_margin": _parse_num(row, col_map.get("maintenance_margin")),
                "lower_limit": _parse_num(row, col_map.get("lower_limit")),
                "upper_limit": _parse_num(row, col_map.get("upper_limit")),
                "fx_rate": _parse_num(row, col_map.get("fx_rate")),
                "is_active": is_active,
            }
            all_rows.append(rec)

    if all_rows:
        result = pd.DataFrame(all_rows)
        logger.info(
            "Parsed margins for %s: %d contracts (%d active)",
            report_date,
            len(result),
            result["is_active"].sum(),
        )
        return result

    return pd.DataFrame()


# ---------------------------------------------------------------------------
# Backfill
# ---------------------------------------------------------------------------


def backfill_margins(
    start_date: date,
    end_date: date | None = None,
    delay: float = 1.0,
    progress_callback=None,
    download_dir: str | None = None,
) -> pd.DataFrame:
    """Backfill margins over a date range.

    Launches Chrome ONCE, solves CF ONCE, then downloads all files.
    Falls back to system curl if UC is unavailable.
    Skips weekends, skips already-downloaded files, 404=holiday.
    """
    if end_date is None:
        end_date = date.today()

    dl_dir = download_dir or str(MARGINS_DOWNLOAD_DIR)

    if _has_uc():
        result = _backfill_with_uc(start_date, end_date, delay, progress_callback, dl_dir)
        if not result.empty:
            return result

    logger.warning(
        "Could not backfill margins automatically. "
        "Use the manual upload in Streamlit UI."
    )
    return pd.DataFrame()


def _backfill_with_uc(
    start_date: date,
    end_date: date,
    delay: float,
    progress_callback,
    dl_dir: str,
) -> pd.DataFrame:
    """Backfill using undetected-chromedriver — launch once, solve CF once."""
    driver, display = _create_uc_driver(dl_dir)
    if driver is None:
        return pd.DataFrame()

    chunks: list[pd.DataFrame] = []
    total_days = (end_date - start_date).days + 1

    try:
        _uc_solve_cloudflare(driver)

        cur = start_date
        day_num = 0

        while cur <= end_date:
            day_num += 1

            if cur.weekday() >= 5:
                cur += timedelta(days=1)
                continue

            url = margins_url(cur)
            fname = f"Margins-{cur.day:02d}-{cur.month:02d}-{cur.year}.xlsx"

            try:
                content = _uc_download_file(driver, url, dl_dir, fname)

                if content is not None:
                    df = parse_margins_excel(content, cur)
                    if not df.empty:
                        chunks.append(df)
                        logger.info("Margins %s: %d contracts", cur, len(df))
                else:
                    logger.debug("No file for %s (holiday?)", cur)

            except Exception as e:
                logger.warning("Margins backfill failed for %s: %s", cur, e)

            if progress_callback:
                progress_callback(day_num, total_days, str(cur))

            cur += timedelta(days=1)
            time.sleep(delay)

    finally:
        _cleanup_driver(driver, display)

    if chunks:
        result = pd.concat(chunks, ignore_index=True).drop_duplicates(
            subset=["report_date", "contract_code"], keep="last"
        )
        logger.info("Margins backfill TOTAL: %d rows", len(result))
        return result

    return pd.DataFrame()


def _backfill_with_curl(
    start_date: date,
    end_date: date,
    delay: float,
    progress_callback,
) -> pd.DataFrame:
    """Backfill using system curl (fallback)."""
    chunks: list[pd.DataFrame] = []
    total_days = (end_date - start_date).days + 1
    cur = start_date
    day_num = 0

    while cur <= end_date:
        day_num += 1

        if cur.weekday() >= 5:
            cur += timedelta(days=1)
            continue

        url = margins_url(cur)
        try:
            content, status = _download_with_curl(url)

            if content is not None:
                df = parse_margins_excel(content, cur)
                if not df.empty:
                    chunks.append(df)
                    logger.info("Margins %s: %d contracts", cur, len(df))
            elif status == 404:
                logger.debug("404 for %s (holiday)", cur)
            else:
                logger.debug("Skip %s: HTTP %d", cur, status)

        except Exception as e:
            logger.warning("Margins backfill failed for %s: %s", cur, e)

        if progress_callback:
            progress_callback(day_num, total_days, str(cur))

        cur += timedelta(days=1)
        time.sleep(delay)

    if chunks:
        result = pd.concat(chunks, ignore_index=True).drop_duplicates(
            subset=["report_date", "contract_code"], keep="last"
        )
        logger.info("Margins backfill TOTAL: %d rows", len(result))
        return result

    return pd.DataFrame()


# ---------------------------------------------------------------------------
# Column mapping helpers
# ---------------------------------------------------------------------------


def _map_columns(cols: list[str], sheet_name: str) -> dict[str, str | None]:
    """Map standardized names to actual column names by fuzzy matching."""
    result: dict[str, str | None] = {
        "product_group": None,
        "contract_code": None,
        "reference_price": None,
        "initial_margin_pct": None,
        "initial_margin_value": None,
        "wcm": None,
        "maintenance_margin": None,
        "lower_limit": None,
        "upper_limit": None,
        "fx_rate": None,
    }

    for col in cols:
        cl = col.lower()

        if "product" in cl and "group" in cl:
            result["product_group"] = col
        elif "contract" in cl or "symbol" in cl or "code" in cl:
            result["contract_code"] = col
        elif "reference" in cl and "price" in cl:
            result["reference_price"] = col
        elif ("initial" in cl) and ("%" in cl or "pct" in cl or "percent" in cl):
            result["initial_margin_pct"] = col
        elif ("initial" in cl) and ("magin" in cl or "margin" in cl) and "%" not in cl:
            # Handle "Initial Magin" typo (missing 'r')
            if result["initial_margin_pct"] is None or col != result["initial_margin_pct"]:
                result["initial_margin_value"] = col
        elif "wcm" in cl:
            result["wcm"] = col
        elif "maintenance" in cl or "maint" in cl:
            result["maintenance_margin"] = col
        elif "lower" in cl and "limit" in cl:
            result["lower_limit"] = col
        elif "upper" in cl and "limit" in cl:
            result["upper_limit"] = col
        elif "fx" in cl or "exchange" in cl or "rate" in cl:
            result["fx_rate"] = col

    return result


def _get_val(row, col_name: str | None):
    """Get a value from a row by column name, handling None col_name."""
    if col_name is None:
        return None
    val = row.get(col_name)
    if pd.isna(val):
        return None
    return val


def _parse_num(row, col_name: str | None) -> float | None:
    """Parse a numeric value from a row, handling '-', '#N/A', etc."""
    val = _get_val(row, col_name)
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if s in ("-", "", "#N/A", "N/A", "nan", "NaN"):
        return None
    try:
        return float(s.replace(",", ""))
    except (ValueError, TypeError):
        return None
