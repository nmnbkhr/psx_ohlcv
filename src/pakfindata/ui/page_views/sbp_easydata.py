"""SBP EasyData admin page — browse, fetch & visualize SBP macro data."""

import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

SERIES_DIR = Path("/mnt/e/psxdata/sbp_easydata/series")
DATASETS_DIR = Path("/mnt/e/psxdata/sbp_easydata/datasets")
CATALOG_FILE = Path("/mnt/e/psxdata/sbp_easydata/catalog.json")

# Subject area grouping for dataset codes
SUBJECT_AREAS = {
    "Interest Rates": ["TS_GP_IR_", "TS_GP_BAM_SIR", "TS_GP_BAM_WALDR"],
    "Exchange Rates": ["TS_GP_ER_", "TS_GP_ES_FADER"],
    "External Sector": ["TS_GP_ES_", "TS_GP_BOP_", "TS_GP_EXT_"],
    "Foreign Investment": ["TS_GP_FI_"],
    "Monetary & Financial": ["TS_GP_MFS_", "TS_GP_BAM_RM", "TS_GP_BAM_M2",
                             "TS_GP_BAM_M3", "TS_GP_BAM_CBS", "TS_GP_BAM_DCS",
                             "TS_GP_BAM_ODCS", "TS_GP_BAM_FCS", "TS_GP_BAM_OFCS",
                             "TS_GP_BAM_ADV", "TS_GP_BAM_CRL", "TS_GP_BAM_DDC",
                             "TS_GP_BAM_DIS", "TS_GP_BAM_LT", "TS_GP_BAM_LP",
                             "TS_GP_BS_"],
    "Debt Profile": ["TS_GP_ED_", "TS_GP_PDL_", "TS_GP_PKDP_",
                     "TS_GP_BAM_CENGOVTD", "TS_GP_BAM_GOVTDDL",
                     "TS_GP_BAM_OUTDDPSE", "TS_GP_BAM_PKDLP", "TS_GP_BAM_SMBYNSS"],
    "Public Finance": ["TS_GP_PF_"],
    "Real Sector": ["TS_GP_RS_", "TS_GP_RLS_", "TS_GP_RL_LSM", "TS_GP_RL_CCS",
                    "TS_GP_RL_BCS", "TS_GP_PT_", "TS_GP_GA_", "TS_GP_FSA_"],
    "Social Sector": ["TS_GP_RL_PAK", "TS_GP_RL_LIT", "TS_GP_RL_EDU",
                      "TS_GP_RL_TCHR", "TS_GP_RL_NMH", "TS_GP_RL_DOC",
                      "TS_GP_RL_HLTH", "TS_GP_RL_EM", "TS_GP_RL_POP"],
}

HIGH_PRIORITY = [
    "TS_GP_BAM_SIRKIBOR_D", "TS_GP_IR_REPOMR_D", "TS_GP_BAM_RM_W",
    "TS_GP_BAM_M2_W", "TS_GP_MFS_SBPSOA20_W", "TS_GP_PT_CPI_M",
    "TS_GP_BOP_WR_M", "TS_GP_ER_FAERPKR_M", "TS_GP_EXT_PAKRES_M",
    "TS_GP_BAM_M3_M", "TS_GP_MFS_TDOSB_M", "TS_GP_MFS_TAOSB_M",
    "TS_GP_BAM_CBS_M", "TS_GP_BAM_DCS_M", "TS_GP_BAM_CENGOVTD_M",
    "TS_GP_BOP_BPM6SUM_M", "TS_GP_FI_SUMFIPK_M", "TS_GP_BAM_WALDR_M",
    "TS_GP_RL_LSM1516_M", "TS_GP_RS_QGDP1516_Q", "TS_GP_ED_PKEDLOUT_Q",
    "TS_GP_BAM_PKDLP_Q", "TS_GP_MFS_FSIIBI_Q",
]


# ─── helpers ────────────────────────────────────────────────────────────────

def _load_catalog() -> dict:
    if CATALOG_FILE.exists():
        with open(CATALOG_FILE) as f:
            return json.load(f)
    return {}


def _downloaded_set() -> set:
    """Set of series file stems that exist on disk (without extension)."""
    if not SERIES_DIR.exists():
        return set()
    return {f.stem for f in SERIES_DIR.glob("*.json")}


def _dataset_subject(code: str) -> str:
    for area, prefixes in SUBJECT_AREAS.items():
        for pfx in prefixes:
            if code.startswith(pfx):
                return area
    return "Other"


def _read_series_df(series_key: str) -> pd.DataFrame | None:
    """Read a downloaded series JSON into a DataFrame."""
    fname = series_key.replace(".", "_")
    fp = SERIES_DIR / f"{fname}.json"
    if not fp.exists():
        return None
    with open(fp) as f:
        data = json.load(f)
    rows = data.get("rows", [])
    cols = data.get("columns", [])
    if not rows:
        return None
    df = pd.DataFrame(rows, columns=cols)
    if "Observation Date" in df.columns:
        df["Observation Date"] = pd.to_datetime(df["Observation Date"], errors="coerce")
        df = df.dropna(subset=["Observation Date"])
    if "Observation Value" in df.columns:
        df["Observation Value"] = pd.to_numeric(df["Observation Value"], errors="coerce")
    return df


def _api_fetch_series(series_key: str, start_date: str = "1947-01-01") -> dict | None:
    """Fetch one series from SBP EasyData API (rate-limit aware)."""
    import requests
    import warnings
    warnings.filterwarnings("ignore", message="Unverified HTTPS request")

    from pakfindata.sources.sbp_easydata import API_KEY, API_BASE

    if API_KEY == "YOUR_API_KEY_HERE":
        return None

    # Track request count in session state for rate limiting
    now = time.time()
    if "sbp_req_times" not in st.session_state:
        st.session_state.sbp_req_times = []

    # Prune old timestamps (older than 1 hour)
    st.session_state.sbp_req_times = [
        t for t in st.session_state.sbp_req_times if now - t < 3600
    ]

    if len(st.session_state.sbp_req_times) >= 240:
        return {"error": "rate_limit", "msg": "Approaching 250/hour limit. Wait before fetching more."}

    try:
        r = requests.get(
            f"{API_BASE}/series/{series_key}/data",
            params={
                "api_key": API_KEY,
                "format": "json",
                "start_date": start_date,
                "end_date": datetime.now().strftime("%Y-%m-%d"),
            },
            verify=False,
            timeout=30,
        )
        st.session_state.sbp_req_times.append(time.time())

        if r.status_code == 200:
            return r.json()
        elif r.status_code == 429:
            return {"error": "rate_limit", "msg": "API returned 429 — rate limited."}
        else:
            return {"error": "http", "msg": f"HTTP {r.status_code}"}
    except Exception as e:
        return {"error": "exception", "msg": str(e)}


def _save_series(series_key: str, data: dict):
    """Save fetched series data to disk as JSON + CSV."""
    import csv as csv_mod
    fname = series_key.replace(".", "_")
    fp_json = SERIES_DIR / f"{fname}.json"
    fp_csv = SERIES_DIR / f"{fname}.csv"

    with open(fp_json, "w") as f:
        json.dump(data, f, indent=2)

    rows = data.get("rows", [])
    columns = data.get("columns", [])
    with open(fp_csv, "w", newline="") as f:
        w = csv_mod.writer(f)
        if columns:
            w.writerow(columns)
        w.writerows(rows)


# ─── render ─────────────────────────────────────────────────────────────────

def render_sbp_easydata():
    st.markdown("## SBP EasyData")
    st.caption(
        "State Bank of Pakistan macro/financial data | "
        "18,000+ variables | API rate limit: 250/hr, 2,000/day"
    )

    catalog = _load_catalog()
    downloaded = _downloaded_set()

    if not catalog:
        st.error("No catalog found at " + str(CATALOG_FILE))
        st.info("Run: `python -m pakfindata.sources.sbp_easydata discover`")
        return

    datasets = catalog.get("datasets", {})
    all_series = catalog.get("series", {})

    # ── top metrics ─────────────────────────────────────────────────
    total_series = len(all_series)
    dl_count = sum(1 for sk in all_series if sk.replace(".", "_") in downloaded)

    from pakfindata.engine.background_jobs import JOB_SBP_FETCH, is_running, read_state
    job_state = read_state(JOB_SBP_FETCH)
    reqs_this_hour = job_state.get("requests_this_hour", 0) if job_state else 0
    job_status = ""
    if is_running(JOB_SBP_FETCH):
        job_status = "RUNNING"

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Datasets", len(datasets))
    c2.metric("Series (catalog)", f"{total_series:,}")
    c3.metric("Downloaded", f"{dl_count:,}", delta=f"{dl_count/total_series*100:.1f}%")
    c4.metric("API reqs/hr", f"{reqs_this_hour}/250")
    if job_status:
        c5.metric("Job", job_status)
    else:
        c5.metric("Job", "Idle")

    tabs = st.tabs([
        "Coverage",
        "Browse Datasets",
        "Fetch Data",
        "Explore Series",
    ])

    # ═════════════════════════════════════════════════════════════════
    # TAB 1 — Coverage overview
    # ═════════════════════════════════════════════════════════════════
    with tabs[0]:
        _render_coverage(datasets, downloaded)

    # ═════════════════════════════════════════════════════════════════
    # TAB 2 — Browse datasets
    # ═════════════════════════════════════════════════════════════════
    with tabs[1]:
        _render_browse(datasets, downloaded)

    # ═════════════════════════════════════════════════════════════════
    # TAB 3 — Fetch data
    # ═════════════════════════════════════════════════════════════════
    with tabs[2]:
        _render_fetch(catalog, downloaded)

    # ═════════════════════════════════════════════════════════════════
    # TAB 4 — Explore downloaded series
    # ═════════════════════════════════════════════════════════════════
    with tabs[3]:
        _render_explore(catalog, downloaded)


def _render_coverage(datasets: dict, downloaded: set):
    st.subheader("Download Coverage")

    rows = []
    for code, info in sorted(datasets.items()):
        skeys = info.get("series_keys", [])
        if not skeys:
            continue
        dl = sum(1 for sk in skeys if sk.replace(".", "_") in downloaded)
        rows.append({
            "Dataset": code,
            "Name": info.get("name", ""),
            "Subject": _dataset_subject(code),
            "Series": len(skeys),
            "Downloaded": dl,
            "Pct": round(dl / len(skeys) * 100, 1) if skeys else 0,
            "Priority": code in HIGH_PRIORITY,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        st.warning("No datasets in catalog.")
        return

    # Filter
    col1, col2 = st.columns(2)
    with col1:
        subject_filter = st.selectbox(
            "Subject Area", ["All"] + sorted(df["Subject"].unique().tolist())
        )
    with col2:
        status_filter = st.selectbox(
            "Status", ["All", "Complete", "Partial", "Not Downloaded", "Priority Only"]
        )

    fdf = df.copy()
    if subject_filter != "All":
        fdf = fdf[fdf["Subject"] == subject_filter]
    if status_filter == "Complete":
        fdf = fdf[fdf["Pct"] == 100]
    elif status_filter == "Partial":
        fdf = fdf[(fdf["Pct"] > 0) & (fdf["Pct"] < 100)]
    elif status_filter == "Not Downloaded":
        fdf = fdf[fdf["Downloaded"] == 0]
    elif status_filter == "Priority Only":
        fdf = fdf[fdf["Priority"]]

    # Summary bar
    complete = len(df[df["Pct"] == 100])
    partial = len(df[(df["Pct"] > 0) & (df["Pct"] < 100)])
    none_dl = len(df[df["Downloaded"] == 0])
    st.markdown(
        f"**{complete}** complete | **{partial}** partial | **{none_dl}** not downloaded"
    )

    st.dataframe(
        fdf[["Dataset", "Name", "Subject", "Series", "Downloaded", "Pct", "Priority"]]
        .sort_values(["Priority", "Pct"], ascending=[False, True])
        .reset_index(drop=True),
        use_container_width=True,
        height=500,
        column_config={
            "Pct": st.column_config.ProgressColumn(
                "Coverage %", min_value=0, max_value=100, format="%.0f%%"
            ),
            "Priority": st.column_config.CheckboxColumn("Priority"),
        },
    )


def _render_browse(datasets: dict, downloaded: set):
    st.subheader("Browse Datasets")

    ds_options = sorted(datasets.keys())
    selected_ds = st.selectbox(
        "Select Dataset",
        ds_options,
        format_func=lambda c: f"{c} — {datasets[c].get('name', '')}",
    )

    if not selected_ds:
        return

    info = datasets[selected_ds]
    skeys = info.get("series_keys", [])

    st.markdown(f"**{info.get('name', '')}** | {len(skeys)} series | Subject: {_dataset_subject(selected_ds)}")

    rows = []
    for sk in skeys:
        stem = sk.replace(".", "_")
        is_dl = stem in downloaded
        obs = 0
        latest = ""
        if is_dl:
            fp = SERIES_DIR / f"{stem}.json"
            if fp.exists():
                try:
                    with open(fp) as f:
                        d = json.load(f)
                    r = d.get("rows", [])
                    obs = len(r)
                    if r:
                        dates = [row[3] for row in r if row[3]]
                        latest = max(dates) if dates else ""
                except Exception:
                    pass
        rows.append({
            "Series Key": sk,
            "Downloaded": is_dl,
            "Observations": obs,
            "Latest Date": latest,
        })

    sdf = pd.DataFrame(rows)
    st.dataframe(
        sdf, use_container_width=True, height=400,
        column_config={"Downloaded": st.column_config.CheckboxColumn("Downloaded")},
    )


def _render_fetch(catalog: dict, downloaded: set):
    st.subheader("Fetch Data from SBP API")

    from pakfindata.sources.sbp_easydata import API_KEY
    from pakfindata.engine.background_jobs import (
        JOB_SBP_FETCH, is_running, read_state, start_sbp_fetch, stop_job, clear_state,
    )

    if API_KEY == "YOUR_API_KEY_HERE":
        st.error("API key not configured. Edit `src/pakfindata/sources/sbp_easydata.py`.")
        return

    st.markdown(f"**API Key:** `{API_KEY[:8]}...{API_KEY[-4:]}`")

    # ── Show running/completed job status ────────────────────────
    running = is_running(JOB_SBP_FETCH)
    state = read_state(JOB_SBP_FETCH)

    if running and state:
        st.warning("Background fetch is running. You can navigate away — it will continue.")
        total = state.get("total", 0)
        completed = state.get("completed", 0)
        failed = state.get("failed", 0)
        current_key = state.get("current_key", "")

        if total > 0:
            st.progress(completed / total, text=f"[{completed}/{total}] {current_key}")

        mc1, mc2, mc3, mc4 = st.columns(4)
        mc1.metric("Completed", completed)
        mc2.metric("Failed", failed)
        mc3.metric("Pending", state.get("pending", 0) - (completed - len(set(state.get("completed_keys", [])))) )
        mc4.metric("Reqs/Hour", state.get("requests_this_hour", 0))

        if state.get("errors"):
            with st.expander(f"Errors ({len(state['errors'])})"):
                for err in state["errors"][:30]:
                    st.text(err)

        col1, col2 = st.columns(2)
        with col1:
            if st.button("Refresh Status", key="sbp_refresh"):
                st.rerun()
        with col2:
            if st.button("Stop Fetch", key="sbp_stop"):
                stop_job(JOB_SBP_FETCH)
                st.info("Stop signal sent. Will finish current request then stop.")
                st.rerun()
        return

    # Show last job result
    if state:
        status = state.get("status", "")
        if status == "completed":
            st.success(
                f"Last run: {state.get('completed', 0)} completed, "
                f"{state.get('failed', 0)} failed — "
                f"finished {(state.get('finished_at') or '')[:19]}"
            )
        elif status == "stopped":
            st.info(
                f"Last run stopped: {state.get('completed', 0)} completed, "
                f"{state.get('failed', 0)} failed. **Will resume from here.**"
            )
        if st.button("Clear previous state", key="sbp_clear"):
            clear_state(JOB_SBP_FETCH)
            st.rerun()

    # ── Fetch controls ──────────────────────────────────────────
    st.markdown("---")
    st.markdown(
        "Runs in the **background** — does not block Streamlit. "
        "Respects 250/hr rate limit. **Resumable** if stopped or app restarts."
    )

    datasets = catalog.get("datasets", {})

    mode = st.radio(
        "Fetch Mode",
        ["Dataset (all series)", "Priority Datasets (missing only)", "Single Series (blocking)"],
        horizontal=True,
    )

    if mode == "Single Series (blocking)":
        series_input = st.text_input(
            "Series Key", placeholder="e.g., TS_GP_BAM_SIRKIBOR_D.KIBOR0030",
        )
        start = st.date_input("Start Date", value=datetime(2000, 1, 1))
        if st.button("Fetch Series", type="primary"):
            if not series_input:
                st.warning("Enter a series key.")
                return
            with st.spinner(f"Fetching {series_input}..."):
                data = _api_fetch_series(series_input, start_date=start.strftime("%Y-%m-%d"))
            if data and "error" in data:
                st.error(data["msg"])
            elif data and data.get("rows"):
                _save_series(series_input, data)
                st.success(f"Saved {len(data['rows'])} observations for {series_input}")
            else:
                st.warning("No data returned for this series key.")

    elif mode == "Dataset (all series)":
        ds_choice = st.selectbox(
            "Dataset",
            sorted(datasets.keys()),
            format_func=lambda c: f"{c} — {datasets[c].get('name', '')} ({datasets[c].get('series_count', 0)} series)",
        )
        start = st.date_input("Start Date", value=datetime(2000, 1, 1), key="ds_start")
        skip_existing = st.checkbox("Skip already downloaded", value=True)

        skeys = datasets.get(ds_choice, {}).get("series_keys", [])
        to_fetch = [sk for sk in skeys if sk.replace(".", "_") not in downloaded] if skip_existing else skeys

        st.markdown(f"**{len(to_fetch)}** series to fetch (of {len(skeys)} total)")

        if st.button("Start Background Fetch", type="primary", key="sbp_ds_start"):
            if not to_fetch:
                st.info("Nothing to fetch.")
            else:
                started = start_sbp_fetch(to_fetch, start_date=start.strftime("%Y-%m-%d"))
                if started:
                    st.success(f"Background fetch started for {len(to_fetch)} series.")
                else:
                    st.warning("A fetch job is already running.")
                st.rerun()

    elif mode == "Priority Datasets (missing only)":
        to_fetch = []
        for ds_code in HIGH_PRIORITY:
            info = datasets.get(ds_code, {})
            for sk in info.get("series_keys", []):
                if sk.replace(".", "_") not in downloaded:
                    to_fetch.append(sk)

        st.markdown(f"**{len(to_fetch)}** priority series not yet downloaded")
        start = st.date_input("Start Date", value=datetime(2000, 1, 1), key="pri_start")

        if st.button("Start Background Fetch", type="primary", key="sbp_pri_start"):
            if not to_fetch:
                st.info("All priority series already downloaded.")
            else:
                started = start_sbp_fetch(to_fetch, start_date=start.strftime("%Y-%m-%d"))
                if started:
                    st.success(f"Background fetch started for {len(to_fetch)} priority series.")
                else:
                    st.warning("A fetch job is already running.")
                st.rerun()


def _render_explore(catalog: dict, downloaded: set):
    st.subheader("Explore Downloaded Series")

    datasets = catalog.get("datasets", {})

    # Build list of downloaded series with their dataset context
    dl_series = []
    for code, info in datasets.items():
        for sk in info.get("series_keys", []):
            if sk.replace(".", "_") in downloaded:
                dl_series.append((sk, code, info.get("name", "")))

    if not dl_series:
        st.info("No series downloaded yet. Use the Fetch tab to download data.")
        return

    # Search
    search = st.text_input("Search series", placeholder="e.g., KIBOR, CPI, USD, remittance")

    filtered = dl_series
    if search:
        q = search.lower()
        filtered = [(sk, c, n) for sk, c, n in dl_series if q in sk.lower() or q in n.lower() or q in c.lower()]

    st.caption(f"{len(filtered)} series found")

    # Select series
    if not filtered:
        return

    options = [f"{sk}  ({n})" for sk, c, n in filtered[:200]]
    sel_idx = st.selectbox("Select series to plot", range(len(options)), format_func=lambda i: options[i])

    sk, ds_code, ds_name = filtered[sel_idx]

    df = _read_series_df(sk)
    if df is None or df.empty:
        st.warning("No data in this series file.")
        return

    # Info
    col1, col2, col3 = st.columns(3)
    col1.metric("Observations", len(df))
    if "Observation Date" in df.columns:
        col2.metric("From", str(df["Observation Date"].min().date()))
        col3.metric("To", str(df["Observation Date"].max().date()))

    # Unit
    if "Unit" in df.columns:
        unit = df["Unit"].dropna().unique()
        if len(unit):
            st.caption(f"Unit: {', '.join(str(u) for u in unit)}")

    # Chart
    if "Observation Date" in df.columns and "Observation Value" in df.columns:
        chart_df = df[["Observation Date", "Observation Value"]].dropna().sort_values("Observation Date")
        chart_df = chart_df.set_index("Observation Date")

        st.line_chart(chart_df, use_container_width=True, height=350)

    # Data table
    with st.expander("Raw Data"):
        st.dataframe(df, use_container_width=True, height=300)
