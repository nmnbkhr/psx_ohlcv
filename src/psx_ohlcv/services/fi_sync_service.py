"""Background service for Fixed Income data synchronization.

This service automatically fetches fixed income data from:
- SBP Primary Market Activities (auction results, calendars)
- SBP Money & Securities Markets (policy rates, KIBOR, MTB/PIB yields)
- Yield curve data from available sources

It uses:
- PID file to track running state
- Status JSON file for UI communication
- Log file for detailed logging
- Scheduled periodic sync
"""

import json
import os
import re
import signal
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from ..config import DATA_ROOT, get_db_path, get_logger, setup_logging
from ..db import (
    connect,
    init_schema,
    upsert_fi_curve_point,
    upsert_fi_instrument,
    upsert_kibor_rate,
    upsert_policy_rate,
    upsert_sbp_pma_doc,
)
from ..sources.sbp_msm import (
    convert_msm_to_curve_points,
    fetch_all_msm_data,
)
from ..sources.sbp_pma import (
    PMADocument,
    convert_doc_to_db_record,
    fetch_and_parse_pma,
)

# Service paths
SERVICE_DIR = DATA_ROOT / "services"
PID_FILE = SERVICE_DIR / "fi_sync.pid"
STATUS_FILE = SERVICE_DIR / "fi_sync_status.json"
LOG_FILE = SERVICE_DIR / "fi_sync.log"

# SBP Data URLs
SBP_AUCTION_URL = "https://www.sbp.org.pk/ecodata/auction-results.asp"
SBP_TBILL_URL = "https://www.sbp.org.pk/ecodata/t-bills.asp"

# Sync intervals
DEFAULT_SYNC_INTERVAL = 3600  # 1 hour
MIN_SYNC_INTERVAL = 300  # 5 minutes


@dataclass
class FISyncStatus:
    """Status of the Fixed Income sync service."""

    running: bool = False
    pid: int | None = None
    started_at: str | None = None
    last_sync_at: str | None = None
    next_sync_at: str | None = None
    result: str | None = None  # "success", "partial", "error"
    sync_count: int = 0
    docs_synced: int = 0
    quotes_synced: int = 0
    curves_synced: int = 0
    instruments_synced: int = 0
    policy_rates_synced: int = 0
    kibor_rates_synced: int = 0
    progress: float = 0.0
    progress_message: str | None = None
    sync_interval: int = DEFAULT_SYNC_INTERVAL
    continuous: bool = False
    error_message: str | None = None
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "FISyncStatus":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


def ensure_service_dir():
    """Ensure service directory exists."""
    SERVICE_DIR.mkdir(parents=True, exist_ok=True)


def read_fi_status() -> FISyncStatus:
    """Read current FI sync status from file."""
    ensure_service_dir()
    if STATUS_FILE.exists():
        try:
            with open(STATUS_FILE) as f:
                data = json.load(f)
            status = FISyncStatus.from_dict(data)
            # Verify PID is still running
            if status.running and status.pid:
                if not is_process_running(status.pid):
                    status.running = False
                    status.pid = None
                    write_fi_status(status)
            return status
        except (json.JSONDecodeError, KeyError):
            pass
    return FISyncStatus()


def write_fi_status(status: FISyncStatus):
    """Write FI sync status to file."""
    ensure_service_dir()
    with open(STATUS_FILE, "w") as f:
        json.dump(status.to_dict(), f, indent=2)


def is_process_running(pid: int) -> bool:
    """Check if a process with given PID is running."""
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


def read_pid() -> int | None:
    """Read PID from file."""
    if PID_FILE.exists():
        try:
            return int(PID_FILE.read_text().strip())
        except (ValueError, IOError):
            pass
    return None


def write_pid(pid: int):
    """Write PID to file."""
    ensure_service_dir()
    PID_FILE.write_text(str(pid))


def remove_pid():
    """Remove PID file."""
    if PID_FILE.exists():
        PID_FILE.unlink()


def is_fi_sync_running() -> tuple[bool, int | None]:
    """Check if FI sync is running.

    Returns:
        Tuple of (is_running, pid)
    """
    pid = read_pid()
    if pid and is_process_running(pid):
        return True, pid
    return False, None


def stop_fi_sync() -> tuple[bool, str]:
    """Stop the running FI sync.

    Returns:
        Tuple of (success, message)
    """
    running, pid = is_fi_sync_running()
    if not running:
        remove_pid()
        status = read_fi_status()
        status.running = False
        status.pid = None
        write_fi_status(status)
        return True, "FI sync was not running"

    try:
        os.kill(pid, signal.SIGTERM)
        for _ in range(10):
            time.sleep(0.5)
            if not is_process_running(pid):
                break

        if is_process_running(pid):
            os.kill(pid, signal.SIGKILL)
            time.sleep(0.5)

        remove_pid()
        status = read_fi_status()
        status.running = False
        status.pid = None
        status.result = "cancelled"
        write_fi_status(status)
        return True, f"FI sync stopped (PID: {pid})"

    except OSError as e:
        return False, f"Failed to stop FI sync: {e}"


def fetch_sbp_auction_results() -> list[dict]:
    """Fetch auction results from SBP website.

    Returns list of parsed auction results with yields.
    """
    results = []

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; PSX-OHLCV/1.0)"
        }

        # Try to fetch T-bill rates page
        response = requests.get(SBP_TBILL_URL, headers=headers, timeout=30)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")

            # Look for tables with rate data
            tables = soup.find_all("table")
            for table in tables:
                rows = table.find_all("tr")
                for row in rows:
                    cells = row.find_all(["td", "th"])
                    if len(cells) >= 3:
                        # Try to extract tenor and rate
                        text = " ".join(c.get_text(strip=True) for c in cells)

                        # Look for patterns like "3 Month 12.50%"
                        tenor_match = re.search(
                            r"(\d+)\s*(?:month|day|year)",
                            text,
                            re.IGNORECASE
                        )
                        rate_match = re.search(
                            r"(\d+\.?\d*)\s*%",
                            text
                        )

                        if tenor_match and rate_match:
                            tenor_text = tenor_match.group(0).lower()
                            rate = float(rate_match.group(1)) / 100

                            # Convert to months
                            tenor_val = int(tenor_match.group(1))
                            if "year" in tenor_text:
                                tenor_months = tenor_val * 12
                            elif "day" in tenor_text:
                                tenor_months = tenor_val // 30
                            else:
                                tenor_months = tenor_val

                            if tenor_months > 0:
                                results.append({
                                    "tenor_months": tenor_months,
                                    "yield_value": rate,
                                    "source": "SBP",
                                })

    except requests.RequestException as e:
        print(f"Error fetching SBP auction results: {e}")

    return results


def parse_auction_result_from_doc(doc: PMADocument) -> dict | None:
    """Try to extract auction result data from document metadata.

    Note: This extracts what we can from the title/metadata.
    Full PDF parsing would require additional dependencies.
    """
    if doc.doc_type != "RESULT":
        return None

    result = {
        "category": doc.category,
        "date": doc.doc_date,
        "doc_url": doc.url,
    }

    title_lower = doc.title.lower()

    # Try to extract tenor from title
    tenor_patterns = [
        (r"3[\s-]*month", 3),
        (r"6[\s-]*month", 6),
        (r"12[\s-]*month", 12),
        (r"1[\s-]*year", 12),
        (r"2[\s-]*year", 24),
        (r"3[\s-]*year", 36),
        (r"5[\s-]*year", 60),
        (r"10[\s-]*year", 120),
    ]

    for pattern, months in tenor_patterns:
        if re.search(pattern, title_lower):
            result["tenor_months"] = months
            break

    return result


def sync_sbp_documents(con, logger) -> tuple[int, list[str]]:
    """Sync SBP PMA documents.

    Returns:
        Tuple of (documents_synced, errors)
    """
    synced = 0
    errors = []

    logger.info("Fetching SBP PMA documents...")

    try:
        docs = fetch_and_parse_pma()

        if not docs:
            logger.warning("No documents fetched from SBP PMA")
            return 0, ["No documents fetched from SBP"]

        logger.info("Found %d documents", len(docs))

        for doc in docs:
            try:
                record = convert_doc_to_db_record(doc)
                if upsert_sbp_pma_doc(con, record):
                    synced += 1
            except Exception as e:
                errors.append(f"Doc {doc.title}: {e}")

    except Exception as e:
        logger.error("Failed to fetch SBP documents: %s", e)
        errors.append(f"Fetch failed: {e}")

    return synced, errors


def sync_yield_curves(con, logger) -> tuple[int, list[str]]:
    """Sync yield curve data from available sources.

    Returns:
        Tuple of (points_synced, errors)
    """
    synced = 0
    errors = []

    logger.info("Fetching yield curve data...")

    try:
        # Fetch auction results which include current rates
        auction_data = fetch_sbp_auction_results()

        if auction_data:
            today = datetime.now().strftime("%Y-%m-%d")

            for point in auction_data:
                curve_point = {
                    "curve_name": "PKR_GOVT",
                    "curve_date": today,
                    "tenor_months": point["tenor_months"],
                    "yield_value": point["yield_value"],
                    "source": point.get("source", "SBP"),
                }

                try:
                    if upsert_fi_curve_point(con, curve_point):
                        synced += 1
                except Exception as e:
                    err_msg = f"Curve point {point['tenor_months']}M: {e}"
                    errors.append(err_msg)

            logger.info("Synced %d curve points", synced)
        else:
            logger.info("No auction data available")

    except Exception as e:
        logger.error("Failed to sync curves: %s", e)
        errors.append(f"Curve sync failed: {e}")

    return synced, errors


def sync_msm_data(con, logger) -> tuple[int, int, int, list[str]]:
    """Sync SBP MSM data (policy rates, KIBOR, yields).

    Returns:
        Tuple of (policy_rates_synced, kibor_synced, curves_synced, errors)
    """
    policy_synced = 0
    kibor_synced = 0
    curves_synced = 0
    errors = []

    logger.info("Fetching SBP MSM data...")

    try:
        msm_data = fetch_all_msm_data()

        if msm_data.get("error"):
            errors.append(msm_data["error"])
            return 0, 0, 0, errors

        today = datetime.now().strftime("%Y-%m-%d")

        # Sync policy rates
        policy_rates = msm_data.get("policy_rates")
        if policy_rates:
            policy_data = {
                "rate_date": today,
                "policy_rate": policy_rates.policy_rate,
                "ceiling_rate": policy_rates.ceiling_rate,
                "floor_rate": policy_rates.floor_rate,
                "overnight_repo_rate": policy_rates.overnight_repo_rate,
                "source": "SBP_MSM",
            }
            if upsert_policy_rate(con, policy_data):
                policy_synced = 1
                logger.info("Synced policy rate: %.2f%%",
                            (policy_rates.policy_rate or 0) * 100)

        # Sync KIBOR rates
        for kibor in msm_data.get("kibor_rates", []):
            kibor_data = {
                "rate_date": today,
                "tenor_months": kibor.tenor_months,
                "bid": kibor.bid,
                "offer": kibor.offer,
                "source": "SBP_MSM",
            }
            try:
                if upsert_kibor_rate(con, kibor_data):
                    kibor_synced += 1
            except Exception as e:
                errors.append(f"KIBOR {kibor.tenor_months}M: {e}")

        if kibor_synced:
            logger.info("Synced %d KIBOR rates", kibor_synced)

        # Sync yield curve points from MSM data
        curve_points = convert_msm_to_curve_points(msm_data)
        for point in curve_points:
            # Convert to DB format
            curve_data = {
                "curve_name": point["curve_name"],
                "curve_date": point["curve_date"],
                "tenor_days": point["tenor_months"] * 30,  # Approx conversion
                "rate": point["yield_value"],
                "source": point.get("source", "SBP_MSM"),
            }
            try:
                if upsert_fi_curve_point(con, curve_data):
                    curves_synced += 1
            except Exception as e:
                errors.append(f"Curve {point['curve_name']}: {e}")

        if curves_synced:
            logger.info("Synced %d curve points from MSM", curves_synced)

    except Exception as e:
        logger.error("Failed to sync MSM data: %s", e)
        errors.append(f"MSM sync failed: {e}")

    return policy_synced, kibor_synced, curves_synced, errors


def sync_instruments_from_docs(con, docs, logger) -> tuple[int, list[str]]:
    """Create/update instrument records from auction documents.

    Returns:
        Tuple of (instruments_synced, errors)
    """
    synced = 0
    errors = []

    # Extract unique instruments from auction results
    seen_instruments = set()

    for doc in docs:
        if doc.doc_type != "RESULT":
            continue

        result = parse_auction_result_from_doc(doc)
        if not result or "tenor_months" not in result:
            continue

        category = result["category"]
        tenor = result["tenor_months"]
        instrument_id = f"{category}-{tenor}M"

        if instrument_id in seen_instruments:
            continue
        seen_instruments.add(instrument_id)

        # Create instrument record
        instrument = {
            "instrument_id": instrument_id,
            "issuer": "GOVT_OF_PAKISTAN",
            "name": f"{category} {tenor}-Month",
            "category": category,
            "currency": "PKR",
            "face_value": 100.0,
            "is_active": 1,
            "source": "SBP_PMA",
        }

        # Set coupon info based on category
        if category == "MTB":
            instrument["coupon_rate"] = None  # Zero coupon
            instrument["coupon_frequency"] = 0
        else:
            instrument["coupon_frequency"] = 2  # Semi-annual

        try:
            if upsert_fi_instrument(con, instrument):
                synced += 1
        except Exception as e:
            errors.append(f"Instrument {instrument_id}: {e}")

    return synced, errors


def run_single_sync(status: FISyncStatus, logger) -> FISyncStatus:
    """Run a single sync operation.

    Args:
        status: Current status object
        logger: Logger instance

    Returns:
        Updated status object
    """
    status.progress_message = "Connecting to database..."
    status.progress = 10.0
    write_fi_status(status)

    con = connect(get_db_path())
    init_schema(con)

    all_errors = []

    try:
        # 1. Sync SBP MSM data (policy rates, KIBOR, yields)
        status.progress_message = "Syncing SBP MSM data..."
        status.progress = 15.0
        write_fi_status(status)

        policy_synced, kibor_synced, msm_curves, msm_errors = sync_msm_data(
            con, logger
        )
        status.policy_rates_synced += policy_synced
        status.kibor_rates_synced += kibor_synced
        status.curves_synced += msm_curves
        all_errors.extend(msm_errors)

        # 2. Sync SBP PMA documents
        status.progress_message = "Syncing SBP PMA documents..."
        status.progress = 35.0
        write_fi_status(status)

        docs_synced, doc_errors = sync_sbp_documents(con, logger)
        status.docs_synced += docs_synced
        all_errors.extend(doc_errors)

        # 3. Sync yield curves from auction page
        status.progress_message = "Syncing yield curves..."
        status.progress = 55.0
        write_fi_status(status)

        curves_synced, curve_errors = sync_yield_curves(con, logger)
        status.curves_synced += curves_synced
        all_errors.extend(curve_errors)

        # 4. Try to create instruments from docs
        status.progress_message = "Updating instruments..."
        status.progress = 80.0
        write_fi_status(status)

        docs = fetch_and_parse_pma()
        if docs:
            inst_synced, inst_errors = sync_instruments_from_docs(con, docs, logger)
            status.instruments_synced += inst_synced
            all_errors.extend(inst_errors)

        # Update status
        status.sync_count += 1
        status.last_sync_at = datetime.now().isoformat()
        status.progress = 100.0
        status.errors = all_errors[-10:]  # Keep last 10 errors

        total_curves = msm_curves + curves_synced
        if not all_errors:
            status.result = "success"
            status.progress_message = (
                f"Sync complete: {docs_synced} docs, "
                f"{total_curves} curves, KIBOR: {kibor_synced}"
            )
            status.error_message = None
        else:
            status.result = "partial"
            status.progress_message = f"Sync partial: {len(all_errors)} errors"
            status.error_message = all_errors[0] if all_errors else None

        logger.info(
            "Sync complete: policy=%d, kibor=%d, docs=%d, curves=%d, errors=%d",
            policy_synced,
            kibor_synced,
            docs_synced,
            total_curves,
            len(all_errors),
        )

    except Exception as e:
        logger.error("Sync failed: %s", e)
        status.result = "error"
        status.progress_message = "Sync failed"
        status.error_message = str(e)

    finally:
        con.close()

    return status


def run_fi_sync(
    continuous: bool = False,
    sync_interval: int = DEFAULT_SYNC_INTERVAL,
):
    """Run the FI sync operation.

    Args:
        continuous: If True, run continuously with interval
        sync_interval: Seconds between syncs (if continuous)
    """
    running, existing_pid = is_fi_sync_running()
    if running:
        print(f"FI sync already running (PID: {existing_pid})")
        return

    ensure_service_dir()
    setup_logging(log_file=LOG_FILE, console=True)
    logger = get_logger()

    pid = os.getpid()
    write_pid(pid)

    status = FISyncStatus(
        running=True,
        pid=pid,
        started_at=datetime.now().isoformat(),
        continuous=continuous,
        sync_interval=max(sync_interval, MIN_SYNC_INTERVAL),
        progress_message="Initializing...",
    )
    write_fi_status(status)

    shutdown_requested = False

    def handle_signal(signum, frame):
        nonlocal shutdown_requested
        logger.info("Shutdown signal received")
        shutdown_requested = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    logger.info(
        "FI sync started (PID: %d, continuous: %s, interval: %ds)",
        pid,
        continuous,
        sync_interval,
    )

    try:
        while not shutdown_requested:
            # Run sync
            status = run_single_sync(status, logger)
            write_fi_status(status)

            if not continuous:
                break

            # Calculate next sync time
            next_sync = datetime.now().timestamp() + status.sync_interval
            status.next_sync_at = datetime.fromtimestamp(next_sync).isoformat()
            status.progress_message = f"Next sync at {status.next_sync_at}"
            write_fi_status(status)

            # Wait for next sync
            logger.info("Waiting %d seconds until next sync...", status.sync_interval)
            wait_start = time.time()
            while time.time() - wait_start < status.sync_interval:
                if shutdown_requested:
                    break
                time.sleep(1)

    except KeyboardInterrupt:
        logger.info("FI sync cancelled by user")
        status.result = "cancelled"
        status.progress_message = "Cancelled by user"

    except Exception as e:
        logger.error("FI sync failed: %s", e)
        status.result = "error"
        status.progress_message = "Sync failed"
        status.error_message = str(e)

    finally:
        logger.info("FI sync finished")
        status.running = False
        status.pid = None
        status.next_sync_at = None
        write_fi_status(status)
        remove_pid()


def start_fi_sync_background(
    continuous: bool = False,
    sync_interval: int = DEFAULT_SYNC_INTERVAL,
) -> tuple[bool, str]:
    """Start the FI sync as a background process.

    Returns:
        Tuple of (success, message)
    """
    running, existing_pid = is_fi_sync_running()
    if running:
        return False, f"FI sync already running (PID: {existing_pid})"

    try:
        pid = os.fork()
        if pid > 0:
            time.sleep(1)
            running, child_pid = is_fi_sync_running()
            if running:
                mode = "continuous" if continuous else "one-time"
                return True, f"FI sync started in {mode} mode (PID: {child_pid})"
            else:
                return False, "FI sync failed to start"
    except OSError as e:
        return False, f"Fork failed: {e}"

    # Child process
    try:
        os.setsid()

        pid = os.fork()
        if pid > 0:
            os._exit(0)

        sys.stdin.close()
        sys.stdout = open(LOG_FILE, "a")
        sys.stderr = sys.stdout

        run_fi_sync(
            continuous=continuous,
            sync_interval=sync_interval,
        )

    except Exception as e:
        print(f"Daemon error: {e}", file=sys.stderr)
    finally:
        os._exit(0)


def main():
    """CLI entry point for FI sync."""
    import argparse

    parser = argparse.ArgumentParser(description="Fixed Income sync background service")
    parser.add_argument(
        "action",
        choices=["start", "stop", "status", "run"],
        help="Action to perform",
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Run continuously (default: one-time sync)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_SYNC_INTERVAL,
        help=f"Sync interval in seconds (default: {DEFAULT_SYNC_INTERVAL})",
    )

    args = parser.parse_args()

    if args.action == "status":
        status = read_fi_status()
        print(json.dumps(status.to_dict(), indent=2))

    elif args.action == "stop":
        success, msg = stop_fi_sync()
        print(msg)
        sys.exit(0 if success else 1)

    elif args.action == "start":
        success, msg = start_fi_sync_background(
            continuous=args.continuous,
            sync_interval=args.interval,
        )
        print(msg)
        sys.exit(0 if success else 1)

    elif args.action == "run":
        run_fi_sync(
            continuous=args.continuous,
            sync_interval=args.interval,
        )


if __name__ == "__main__":
    main()
