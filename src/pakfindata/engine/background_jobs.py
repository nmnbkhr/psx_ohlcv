"""
Background job runner with resumable state for long-running data fetches.

Features:
- Runs in a daemon thread (survives Streamlit reruns, dies with the process)
- Persistent progress file on disk (survives app restarts for resume)
- Only marks series/items as "done" AFTER successful save to disk
- Resume: on restart, skips already-completed items
- Rate-limit aware (configurable delay between requests)
- Stop signal via threading.Event

Used by: SBP EasyData fetch, PSX global announcements/payouts scrape.
"""

import csv
import json
import logging
import threading
import time
import warnings
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

# ─── State directory ────────────────────────────────────────────────────

STATE_DIR = Path("/mnt/e/psxdata/job_state")
STATE_DIR.mkdir(parents=True, exist_ok=True)

# ─── Registry of running jobs ──────────────────────────────────────────

_threads: dict[str, threading.Thread] = {}
_stop_events: dict[str, threading.Event] = {}


def _state_file(job_name: str) -> Path:
    return STATE_DIR / f"{job_name}.json"


def _write_state(job_name: str, state: dict) -> None:
    tmp = _state_file(job_name).with_suffix(".tmp")
    tmp.write_text(json.dumps(state, default=str))
    tmp.replace(_state_file(job_name))


def read_state(job_name: str) -> dict | None:
    fp = _state_file(job_name)
    if not fp.exists():
        return None
    try:
        return json.loads(fp.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def is_running(job_name: str) -> bool:
    t = _threads.get(job_name)
    return t is not None and t.is_alive()


def stop_job(job_name: str) -> bool:
    ev = _stop_events.get(job_name)
    if ev:
        ev.set()
        return True
    return False


def clear_state(job_name: str):
    fp = _state_file(job_name)
    if fp.exists():
        fp.unlink()


# ═════════════════════════════════════════════════════════════════════════
# SBP EasyData Background Fetch
# ═════════════════════════════════════════════════════════════════════════

JOB_SBP_FETCH = "sbp_easydata_fetch"


def start_sbp_fetch(
    series_keys: list[str],
    start_date: str = "2000-01-01",
    delay: float = 15.0,
    skip_downloaded: bool = True,
) -> bool:
    """Start SBP EasyData fetch in background. Returns True if started."""
    if is_running(JOB_SBP_FETCH):
        return False

    _stop_events[JOB_SBP_FETCH] = threading.Event()
    t = threading.Thread(
        target=_run_sbp_fetch,
        args=(series_keys, start_date, delay, skip_downloaded),
        daemon=True,
        name="sbp-easydata-fetch",
    )
    _threads[JOB_SBP_FETCH] = t
    t.start()
    return True


def _run_sbp_fetch(
    series_keys: list[str],
    start_date: str,
    delay: float,
    skip_downloaded: bool,
):
    import requests
    warnings.filterwarnings("ignore", message="Unverified HTTPS request")

    from pakfindata.sources.sbp_easydata import API_KEY, API_BASE, SERIES_DIR

    stop_ev = _stop_events[JOB_SBP_FETCH]

    # Load existing state for resume
    prev_state = read_state(JOB_SBP_FETCH)
    completed_set: set[str] = set()
    if prev_state and prev_state.get("status") in ("stopped", "running"):
        # Resume from previous incomplete run
        completed_set = set(prev_state.get("completed_keys", []))

    # Also check disk for already-downloaded series
    if skip_downloaded:
        for sk in series_keys:
            fp = SERIES_DIR / f"{sk.replace('.', '_')}.json"
            if fp.exists():
                completed_set.add(sk)

    # Filter to only pending items
    pending = [sk for sk in series_keys if sk not in completed_set]
    total_all = len(series_keys)
    total_pending = len(pending)

    state = {
        "status": "running",
        "job": JOB_SBP_FETCH,
        "total": total_all,
        "pending": total_pending,
        "completed": len(completed_set),
        "failed": 0,
        "current": 0,
        "current_key": "",
        "completed_keys": list(completed_set),
        "errors": [],
        "started_at": datetime.now().isoformat(),
        "finished_at": None,
        "requests_this_hour": 0,
        "hour_start": time.time(),
    }
    _write_state(JOB_SBP_FETCH, state)

    session = requests.Session()
    session.headers.update({"Accept": "application/json"})
    session.verify = False

    for i, sk in enumerate(pending):
        if stop_ev.is_set():
            state["status"] = "stopped"
            state["finished_at"] = datetime.now().isoformat()
            state["current_key"] = ""
            _write_state(JOB_SBP_FETCH, state)
            logger.info("SBP fetch stopped at %d/%d", i, total_pending)
            return

        state["current"] = len(completed_set) + i + 1
        state["current_key"] = sk
        _write_state(JOB_SBP_FETCH, state)

        # Rate limit check
        now = time.time()
        if now - state["hour_start"] > 3600:
            state["requests_this_hour"] = 0
            state["hour_start"] = now

        if state["requests_this_hour"] >= 240:
            wait = 3600 - (now - state["hour_start"]) + 10
            if wait > 0:
                logger.info("SBP rate limit — waiting %.0fs", wait)
                state["current_key"] = f"(rate limit pause {wait:.0f}s)"
                _write_state(JOB_SBP_FETCH, state)
                # Sleep in small intervals so stop signal is responsive
                for _ in range(int(wait)):
                    if stop_ev.is_set():
                        state["status"] = "stopped"
                        state["finished_at"] = datetime.now().isoformat()
                        _write_state(JOB_SBP_FETCH, state)
                        return
                    time.sleep(1)
            state["requests_this_hour"] = 0
            state["hour_start"] = time.time()

        # Fetch
        try:
            r = session.get(
                f"{API_BASE}/series/{sk}/data",
                params={
                    "api_key": API_KEY,
                    "format": "json",
                    "start_date": start_date,
                    "end_date": datetime.now().strftime("%Y-%m-%d"),
                },
                timeout=30,
            )
            state["requests_this_hour"] += 1

            if r.status_code == 200:
                data = r.json()
                if data and data.get("rows"):
                    # Save to disk FIRST, then mark completed
                    _save_sbp_series(sk, data, SERIES_DIR)
                    completed_set.add(sk)
                    state["completed"] = len(completed_set)
                    state["completed_keys"] = list(completed_set)
                else:
                    # Empty data — mark as done (no data available)
                    completed_set.add(sk)
                    state["completed"] = len(completed_set)
                    state["completed_keys"] = list(completed_set)
            elif r.status_code == 429:
                # Rate limited — wait and retry later (don't mark as done)
                state["errors"].append(f"{sk}: HTTP 429 rate limited")
                state["failed"] += 1
                time.sleep(60)
            else:
                state["errors"].append(f"{sk}: HTTP {r.status_code}")
                state["failed"] += 1

        except Exception as e:
            state["errors"].append(f"{sk}: {e}")
            state["failed"] += 1

        _write_state(JOB_SBP_FETCH, state)
        time.sleep(delay)

    # Done
    state["status"] = "completed"
    state["finished_at"] = datetime.now().isoformat()
    state["current_key"] = ""
    _write_state(JOB_SBP_FETCH, state)
    logger.info("SBP fetch complete: %d completed, %d failed", state["completed"], state["failed"])


def _save_sbp_series(series_key: str, data: dict, series_dir: Path):
    fname = series_key.replace(".", "_")
    fp_json = series_dir / f"{fname}.json"
    fp_csv = series_dir / f"{fname}.csv"

    with open(fp_json, "w") as f:
        json.dump(data, f, indent=2)

    rows = data.get("rows", [])
    columns = data.get("columns", [])
    with open(fp_csv, "w", newline="") as f:
        w = csv.writer(f)
        if columns:
            w.writerow(columns)
        w.writerows(rows)


# ═════════════════════════════════════════════════════════════════════════
# PSX Global Scraper Background Jobs
# ═════════════════════════════════════════════════════════════════════════

JOB_PSX_ANNOUNCEMENTS = "psx_announcements_fetch"
JOB_PSX_PAYOUTS = "psx_payouts_fetch"


def start_psx_announcements(
    ann_type: str = "companies",
    max_pages: int = 50,
    delay: float = 0.5,
) -> bool:
    if is_running(JOB_PSX_ANNOUNCEMENTS):
        return False

    _stop_events[JOB_PSX_ANNOUNCEMENTS] = threading.Event()
    t = threading.Thread(
        target=_run_psx_announcements,
        args=(ann_type, max_pages, delay),
        daemon=True,
        name="psx-announcements",
    )
    _threads[JOB_PSX_ANNOUNCEMENTS] = t
    t.start()
    return True


def _run_psx_announcements(ann_type: str, max_pages: int, delay: float):
    from pakfindata.engine.psx_company_scraper import (
        scrape_global_announcements,
        save_announcements_to_db,
    )

    stop_ev = _stop_events[JOB_PSX_ANNOUNCEMENTS]
    state = {
        "status": "running",
        "job": JOB_PSX_ANNOUNCEMENTS,
        "ann_type": ann_type,
        "max_pages": max_pages,
        "total_scraped": 0,
        "total_saved": 0,
        "current_page": 0,
        "errors": [],
        "started_at": datetime.now().isoformat(),
        "finished_at": None,
    }
    _write_state(JOB_PSX_ANNOUNCEMENTS, state)

    try:
        # Scrape page by page for progress tracking
        import requests
        from lxml import html as lxml_html

        type_map = {"companies": "C", "cdc": "A", "secp": "B", "nccpl": "D", "psx": "E"}
        ann_code = type_map.get(ann_type, "C")
        url = "https://dps.psx.com.pk/announcements"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        all_anns = []

        for page_num in range(max_pages):
            if stop_ev.is_set():
                state["status"] = "stopped"
                break

            state["current_page"] = page_num + 1
            _write_state(JOB_PSX_ANNOUNCEMENTS, state)

            data = {
                "type": ann_code,
                "symbol": "",
                "query": "",
                "count": 50,
                "offset": page_num * 50,
                "date_from": "",
                "date_to": "",
            }
            resp = requests.post(url, data=data, headers=headers, timeout=30)
            if resp.status_code != 200:
                break

            tree = lxml_html.fromstring(resp.text)
            rows = tree.xpath('//tr')
            page_count = 0

            for row in rows:
                cells = row.xpath('.//td')
                if len(cells) < 3:
                    continue
                texts = [c.text_content().strip() for c in cells]
                pdf_url = ""
                links = row.xpath('.//a/@href')
                for href in links:
                    if 'download' in href:
                        pdf_url = href if href.startswith('http') else f"https://dps.psx.com.pk{href}"
                        break

                if len(texts) >= 4:
                    all_anns.append({
                        "date": texts[0], "time": texts[1],
                        "company": texts[2], "subject": texts[3],
                        "pdf_url": pdf_url, "type": ann_code,
                    })
                    page_count += 1
                elif len(texts) >= 3:
                    all_anns.append({
                        "date": texts[0], "time": "",
                        "company": texts[1], "subject": texts[2],
                        "pdf_url": pdf_url, "type": ann_code,
                    })
                    page_count += 1

            state["total_scraped"] = len(all_anns)
            _write_state(JOB_PSX_ANNOUNCEMENTS, state)

            if page_count == 0:
                break
            time.sleep(delay)

        # Save all to DB
        if all_anns:
            saved = save_announcements_to_db(all_anns)
            state["total_saved"] = saved

    except Exception as e:
        state["errors"].append(str(e))

    if state["status"] != "stopped":
        state["status"] = "completed"
    state["finished_at"] = datetime.now().isoformat()
    _write_state(JOB_PSX_ANNOUNCEMENTS, state)


def start_psx_payouts(max_pages: int = 20, delay: float = 0.5) -> bool:
    if is_running(JOB_PSX_PAYOUTS):
        return False

    _stop_events[JOB_PSX_PAYOUTS] = threading.Event()
    t = threading.Thread(
        target=_run_psx_payouts,
        args=(max_pages, delay),
        daemon=True,
        name="psx-payouts",
    )
    _threads[JOB_PSX_PAYOUTS] = t
    t.start()
    return True


def _run_psx_payouts(max_pages: int, delay: float):
    from pakfindata.engine.psx_company_scraper import (
        scrape_global_payouts,
        save_payouts_to_db,
    )

    stop_ev = _stop_events[JOB_PSX_PAYOUTS]
    state = {
        "status": "running",
        "job": JOB_PSX_PAYOUTS,
        "total_scraped": 0,
        "total_saved": 0,
        "errors": [],
        "started_at": datetime.now().isoformat(),
        "finished_at": None,
    }
    _write_state(JOB_PSX_PAYOUTS, state)

    try:
        if stop_ev.is_set():
            state["status"] = "stopped"
        else:
            payouts = scrape_global_payouts(max_pages=max_pages, delay=delay)
            state["total_scraped"] = len(payouts)
            if payouts:
                saved = save_payouts_to_db(payouts)
                state["total_saved"] = saved
    except Exception as e:
        state["errors"].append(str(e))

    if state["status"] != "stopped":
        state["status"] = "completed"
    state["finished_at"] = datetime.now().isoformat()
    _write_state(JOB_PSX_PAYOUTS, state)
