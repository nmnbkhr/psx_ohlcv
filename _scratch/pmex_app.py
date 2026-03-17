"""
pmex_app.py — PMEX Data Manager (Desktop GUI)

Buttons:
  [Get OHLC Data]      → Fetch from API + Download JSON + Populate DB
  [Get Margins Data]    → Fetch from API + Download Excel + Populate DB
  [Backfill OHLC]       → Historical backfill with progress
  [Backfill Margins]    → Historical backfill with progress
  [View OHLC Table]     → Show pmex_ohlc from DB
  [View Margins Table]  → Show pmex_margins from DB

Storage: /mnt/e/psxdata/commod/
  ├── pmex_ohlc/        JSON files
  ├── pmex_margins/     Excel files
  └── commod.db         SQLite database

Requirements: pip install requests pandas openpyxl
"""

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import threading
import requests
import pandas as pd
import sqlite3
import json
import os
import time
from io import BytesIO
from datetime import date, timedelta
from pathlib import Path
from typing import Optional


# ═══════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════

BASE_DIR = Path("/mnt/e/psxdata/commod")
OHLC_DIR = BASE_DIR / "pmex_ohlc"
MARGINS_DIR = BASE_DIR / "pmex_margins"
DB_PATH = BASE_DIR / "commod.db"

OHLC_API = "https://mportal.pmex.com.pk/mt5bonew/Home/GetOHLC"
OHLC_PAGE = "https://mportal.pmex.com.pk/mt5bonew/Home/OHLCReport"
MARGINS_BASE = "https://pmex.com.pk/wp-content/uploads"

HEADERS_OHLC = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": OHLC_PAGE,
    "Origin": "https://mportal.pmex.com.pk",
}

HEADERS_MARGINS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}

# GetOHLC JSON → real column names (CONFIRMED)
OHLC_COL_MAP = {
    "Trader_Id": "symbol",
    "Post_Date": "trading_date",
    "Trader_Name": "open",
    "Trans_Id": "high",
    "Amount": "low",
    "acc_type": "close",
    "Verified_Date": "traded_volume",
    "Status": "settlement_price",
    "Trans_Date": "fx_rate",
}


# ═══════════════════════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════════════════════

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS pmex_ohlc (
    trading_date     DATE NOT NULL,
    symbol           TEXT NOT NULL,
    open             REAL,
    high             REAL,
    low              REAL,
    close            REAL,
    traded_volume    INTEGER DEFAULT 0,
    settlement_price REAL,
    fx_rate          REAL,
    fetched_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trading_date, symbol)
);
CREATE INDEX IF NOT EXISTS idx_ohlc_sym  ON pmex_ohlc(symbol);
CREATE INDEX IF NOT EXISTS idx_ohlc_dt   ON pmex_ohlc(trading_date);

CREATE TABLE IF NOT EXISTS pmex_margins (
    report_date          DATE NOT NULL,
    sheet_name           TEXT NOT NULL,
    product_group        TEXT,
    contract_code        TEXT NOT NULL,
    reference_price      REAL,
    initial_margin_pct   REAL,
    initial_margin_value REAL,
    wcm                  REAL,
    maintenance_margin   REAL,
    lower_limit          REAL,
    upper_limit          REAL,
    fx_rate              REAL,
    is_active            BOOLEAN DEFAULT 1,
    fetched_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (report_date, contract_code)
);
CREATE INDEX IF NOT EXISTS idx_mgn_dt   ON pmex_margins(report_date);
CREATE INDEX IF NOT EXISTS idx_mgn_code ON pmex_margins(contract_code);
"""


def init_db():
    for d in [BASE_DIR, OHLC_DIR, MARGINS_DIR]:
        d.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA_SQL)
    conn.close()


def get_db():
    if not DB_PATH.exists():
        init_db()
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# ═══════════════════════════════════════════════════════════════════════
# OHLC FETCHER
# ═══════════════════════════════════════════════════════════════════════

def fetch_ohlc(from_date: date, to_date: date, session=None):
    """Fetch → Download JSON → Populate DB. Returns (df, filepath)."""
    s = session or requests.Session()
    s.get(OHLC_PAGE, headers={"User-Agent": HEADERS_OHLC["User-Agent"]}, timeout=15)

    body = f"txtFromDate={from_date.strftime('%m/%d/%Y')}&txtEndDate={to_date.strftime('%m/%d/%Y')}"
    resp = s.post(OHLC_API, data=body, headers=HEADERS_OHLC, timeout=30)

    if resp.status_code != 200:
        return pd.DataFrame(), None

    try:
        data = resp.json()
    except Exception:
        return pd.DataFrame(), None

    if not isinstance(data, list) or len(data) == 0:
        return pd.DataFrame(), None

    # ── DOWNLOAD to disk ──
    OHLC_DIR.mkdir(parents=True, exist_ok=True)
    fname = f"ohlc_{from_date.isoformat()}_{to_date.isoformat()}.json"
    fpath = OHLC_DIR / fname
    fpath.write_text(json.dumps(data, indent=2), encoding="utf-8")

    # ── PARSE ──
    df = pd.DataFrame(data).rename(columns=OHLC_COL_MAP)
    keep = [c for c in OHLC_COL_MAP.values() if c in df.columns]
    df = df[keep]
    df["trading_date"] = pd.to_datetime(df["trading_date"], format="%m/%d/%Y", errors="coerce")
    for col in ["open", "high", "low", "close", "settlement_price", "fx_rate"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", ""), errors="coerce")
    df["traded_volume"] = (
        pd.to_numeric(df["traded_volume"].astype(str).str.replace(",", ""), errors="coerce")
        .fillna(0).astype(int)
    )
    df = df.sort_values(["trading_date", "symbol"]).reset_index(drop=True)

    # ── POPULATE DB ──
    conn = get_db()
    for _, r in df.iterrows():
        conn.execute("""
            INSERT OR REPLACE INTO pmex_ohlc
                (trading_date, symbol, open, high, low, close,
                 traded_volume, settlement_price, fx_rate)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            r["trading_date"].strftime("%Y-%m-%d") if pd.notna(r["trading_date"]) else None,
            r["symbol"], r.get("open"), r.get("high"), r.get("low"), r.get("close"),
            r.get("traded_volume", 0), r.get("settlement_price"), r.get("fx_rate"),
        ))
    conn.commit()
    conn.close()

    return df, str(fpath)


# ═══════════════════════════════════════════════════════════════════════
# MARGINS FETCHER
# ═══════════════════════════════════════════════════════════════════════

def fetch_margins(target_date: date):
    """Fetch → Download Excel → Populate DB. Returns (df, filepath)."""
    url = f"{MARGINS_BASE}/{target_date.year}/{target_date.month:02d}/Margins-{target_date.day:02d}-{target_date.month:02d}-{target_date.year}.xlsx"
    resp = requests.get(url, headers=HEADERS_MARGINS, timeout=30)

    if resp.status_code == 404:
        return pd.DataFrame(), None
    if resp.status_code != 200 or len(resp.content) < 1000:
        return pd.DataFrame(), None

    # ── DOWNLOAD to disk ──
    MARGINS_DIR.mkdir(parents=True, exist_ok=True)
    fname = f"Margins-{target_date.day:02d}-{target_date.month:02d}-{target_date.year}.xlsx"
    fpath = MARGINS_DIR / fname
    fpath.write_bytes(resp.content)

    # ── PARSE both sheets ──
    xls = pd.ExcelFile(BytesIO(resp.content))
    all_rows = []

    for sheet in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet, header=4)
        df.columns = [str(c).strip() for c in df.columns]
        df = df[[c for c in df.columns if not c.startswith("Unnamed")]]
        df = df.dropna(how="all")

        col_map = {}
        for c in df.columns:
            cl = c.lower().replace(" ", "_")
            if "product" in cl:
                col_map[c] = "product_group"
            elif "contract" in cl:
                col_map[c] = "contract_code"
            elif "reference" in cl:
                col_map[c] = "reference_price"
            elif "initial" in cl and "value" in cl:
                col_map[c] = "initial_margin_value"
            elif "initial" in cl and ("margin" in cl or "magin" in cl):
                col_map[c] = "initial_margin_pct"
            elif cl == "wcm":
                col_map[c] = "wcm"
            elif "maintenance" in cl:
                col_map[c] = "maintenance_margin"
            elif "lower" in cl:
                col_map[c] = "lower_limit"
            elif "upper" in cl:
                col_map[c] = "upper_limit"
            elif "fx" in cl:
                col_map[c] = "fx_rate"
        df = df.rename(columns=col_map)

        if "product_group" in df.columns:
            df["product_group"] = df["product_group"].ffill()
        else:
            df["product_group"] = None

        if "contract_code" not in df.columns:
            continue
        df = df[df["contract_code"].notna()].copy()

        df["is_active"] = df["reference_price"].apply(
            lambda x: str(x) != "#N/A" and pd.notna(x)
        )

        for col in ["reference_price", "initial_margin_pct", "initial_margin_value",
                     "maintenance_margin", "lower_limit", "upper_limit", "fx_rate"]:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(",", "").replace("#N/A", ""),
                    errors="coerce"
                )
        if "wcm" in df.columns:
            df["wcm"] = pd.to_numeric(
                df["wcm"].astype(str).str.replace("-", "").replace(",", ""),
                errors="coerce"
            )

        # Clean footer junk
        if "product_group" in df.columns:
            footer_keywords = ["UAN:", "YOUR FUTURES", "Copyrights", "pmex.com.pk"]
            mask = df["product_group"].astype(str).str.contains("|".join(footer_keywords), na=False)
            df = df[~mask]

        df["report_date"] = target_date
        df["sheet_name"] = sheet
        all_rows.append(df)

    if not all_rows:
        return pd.DataFrame(), str(fpath)

    result = pd.concat(all_rows, ignore_index=True)
    cols = ["report_date", "sheet_name", "product_group", "contract_code",
            "reference_price", "initial_margin_pct", "initial_margin_value",
            "wcm", "maintenance_margin", "lower_limit", "upper_limit",
            "fx_rate", "is_active"]
    result = result[[c for c in cols if c in result.columns]]

    # ── POPULATE DB ──
    conn = get_db()
    for _, r in result.iterrows():
        conn.execute("""
            INSERT OR REPLACE INTO pmex_margins
                (report_date, sheet_name, product_group, contract_code,
                 reference_price, initial_margin_pct, initial_margin_value,
                 wcm, maintenance_margin, lower_limit, upper_limit,
                 fx_rate, is_active)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            r["report_date"].isoformat() if isinstance(r["report_date"], date) else str(r["report_date"]),
            r.get("sheet_name"), r.get("product_group"), r.get("contract_code"),
            r.get("reference_price"), r.get("initial_margin_pct"), r.get("initial_margin_value"),
            r.get("wcm"), r.get("maintenance_margin"), r.get("lower_limit"), r.get("upper_limit"),
            r.get("fx_rate"), 1 if r.get("is_active") else 0,
        ))
    conn.commit()
    conn.close()

    return result, str(fpath)


# ═══════════════════════════════════════════════════════════════════════
# GUI APPLICATION
# ═══════════════════════════════════════════════════════════════════════

class PMEXApp:
    def __init__(self, root):
        self.root = root
        self.root.title("PMEX Data Manager — /mnt/e/psxdata/commod/")
        self.root.geometry("1200x750")
        self.root.configure(bg="#1e1e2e")

        style = ttk.Style()
        style.theme_use("clam")

        # Colors
        BG = "#1e1e2e"
        FG = "#cdd6f4"
        BTN_BG = "#313244"
        BTN_FG = "#cdd6f4"
        ACCENT = "#89b4fa"
        GREEN = "#a6e3a1"
        ORANGE = "#fab387"
        HEADER_BG = "#181825"

        style.configure("TFrame", background=BG)
        style.configure("TLabel", background=BG, foreground=FG, font=("Segoe UI", 10))
        style.configure("Title.TLabel", background=BG, foreground=ACCENT, font=("Segoe UI", 14, "bold"))
        style.configure("Status.TLabel", background=HEADER_BG, foreground=GREEN, font=("Consolas", 9))
        style.configure("TButton", background=BTN_BG, foreground=BTN_FG, font=("Segoe UI", 10, "bold"),
                         padding=(12, 6))
        style.map("TButton",
                   background=[("active", ACCENT), ("pressed", ACCENT)],
                   foreground=[("active", "#1e1e2e"), ("pressed", "#1e1e2e")])
        style.configure("Accent.TButton", background=ACCENT, foreground="#1e1e2e")
        style.configure("Green.TButton", background=GREEN, foreground="#1e1e2e")
        style.configure("Orange.TButton", background=ORANGE, foreground="#1e1e2e")
        style.configure("TLabelframe", background=BG, foreground=ACCENT, font=("Segoe UI", 10, "bold"))
        style.configure("TLabelframe.Label", background=BG, foreground=ACCENT)
        style.configure("Treeview", background="#313244", foreground=FG, fieldbackground="#313244",
                         font=("Consolas", 9), rowheight=22)
        style.configure("Treeview.Heading", background=HEADER_BG, foreground=ACCENT,
                         font=("Segoe UI", 9, "bold"))
        style.map("Treeview", background=[("selected", ACCENT)], foreground=[("selected", "#1e1e2e")])

        # ── TOP: Title + Paths ──
        top = ttk.Frame(root)
        top.pack(fill="x", padx=10, pady=(10, 5))
        ttk.Label(top, text="📊 PMEX Data Manager", style="Title.TLabel").pack(side="left")
        ttk.Label(top, text=f"DB: {DB_PATH}   |   Files: {BASE_DIR}",
                  style="Status.TLabel").pack(side="right", padx=5)

        # ── DATE INPUTS ──
        date_frame = ttk.LabelFrame(root, text="Date Range")
        date_frame.pack(fill="x", padx=10, pady=5)

        ttk.Label(date_frame, text="From:").grid(row=0, column=0, padx=(10, 5), pady=8)
        self.from_var = tk.StringVar(value=(date.today() - timedelta(days=30)).isoformat())
        self.from_entry = ttk.Entry(date_frame, textvariable=self.from_var, width=14, font=("Consolas", 11))
        self.from_entry.grid(row=0, column=1, padx=5, pady=8)

        ttk.Label(date_frame, text="To:").grid(row=0, column=2, padx=(20, 5), pady=8)
        self.to_var = tk.StringVar(value=date.today().isoformat())
        self.to_entry = ttk.Entry(date_frame, textvariable=self.to_var, width=14, font=("Consolas", 11))
        self.to_entry.grid(row=0, column=3, padx=5, pady=8)

        ttk.Label(date_frame, text="(YYYY-MM-DD)  Max 3 months per OHLC request",
                  foreground="#6c7086").grid(row=0, column=4, padx=20, pady=8)

        # ── BUTTONS ──
        btn_frame = ttk.Frame(root)
        btn_frame.pack(fill="x", padx=10, pady=5)

        buttons = [
            ("📡 Get OHLC Data", self.do_ohlc_get, "Accent.TButton"),
            ("📡 Get Margins", self.do_margins_get, "Accent.TButton"),
            ("⏪ Backfill OHLC", self.do_ohlc_backfill, "Orange.TButton"),
            ("⏪ Backfill Margins", self.do_margins_backfill, "Orange.TButton"),
            ("📋 View OHLC Table", self.do_view_ohlc, "Green.TButton"),
            ("📋 View Margins Table", self.do_view_margins, "Green.TButton"),
            ("🗃️ Init DB", self.do_init_db, "TButton"),
        ]
        for i, (text, cmd, sty) in enumerate(buttons):
            b = ttk.Button(btn_frame, text=text, command=cmd, style=sty)
            b.grid(row=0, column=i, padx=4, pady=5, sticky="ew")
            btn_frame.columnconfigure(i, weight=1)

        # ── ACTIVE ONLY CHECKBOX ──
        self.active_only = tk.BooleanVar(value=True)
        ttk.Checkbutton(btn_frame, text="Active contracts only (volume > 0)",
                         variable=self.active_only).grid(row=1, column=0, columnspan=3,
                                                          padx=10, pady=2, sticky="w")

        # ── TABLE VIEW ──
        table_frame = ttk.Frame(root)
        table_frame.pack(fill="both", expand=True, padx=10, pady=5)

        # Treeview with scrollbars
        self.tree = ttk.Treeview(table_frame, show="headings", selectmode="browse")

        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(table_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        table_frame.rowconfigure(0, weight=1)
        table_frame.columnconfigure(0, weight=1)

        # ── LOG ──
        log_frame = ttk.LabelFrame(root, text="Log")
        log_frame.pack(fill="x", padx=10, pady=(0, 10))
        self.log = scrolledtext.ScrolledText(log_frame, height=6, bg="#181825", fg="#a6e3a1",
                                              font=("Consolas", 9), wrap="word",
                                              insertbackground="#a6e3a1")
        self.log.pack(fill="x", padx=5, pady=5)

        # ── STATUS BAR ──
        self.status_var = tk.StringVar(value="Ready")
        status_bar = ttk.Label(root, textvariable=self.status_var, style="Status.TLabel",
                                anchor="w", padding=(10, 3))
        status_bar.pack(fill="x", side="bottom")

        # ── PROGRESS BAR ──
        self.progress = ttk.Progressbar(root, mode="determinate")
        self.progress.pack(fill="x", padx=10, side="bottom", pady=(0, 2))

        # Init DB on start
        init_db()
        self._log("✅ Ready. Database: " + str(DB_PATH))

    # ── Helpers ──

    def _log(self, msg):
        self.log.insert("end", msg + "\n")
        self.log.see("end")
        self.root.update_idletasks()

    def _status(self, msg):
        self.status_var.set(msg)
        self.root.update_idletasks()

    def _get_dates(self):
        try:
            f = date.fromisoformat(self.from_var.get().strip())
            t = date.fromisoformat(self.to_var.get().strip())
            return f, t
        except ValueError:
            messagebox.showerror("Date Error", "Use YYYY-MM-DD format")
            return None, None

    def _populate_tree(self, df: pd.DataFrame, max_rows=5000):
        """Load DataFrame into Treeview."""
        # Clear existing
        self.tree.delete(*self.tree.get_children())

        if df.empty:
            self.tree["columns"] = ("message",)
            self.tree.heading("message", text="Message")
            self.tree.column("message", width=400)
            self.tree.insert("", "end", values=("No data",))
            return

        # Limit for performance
        display = df.head(max_rows).copy()

        # Format dates
        for col in display.columns:
            if "date" in col.lower():
                display[col] = display[col].astype(str).str[:10]

        cols = list(display.columns)
        self.tree["columns"] = cols
        for col in cols:
            self.tree.heading(col, text=col, anchor="w")
            # Auto-width based on header + sample data
            max_len = max(len(str(col)), display[col].astype(str).str.len().max() if len(display) > 0 else 5)
            self.tree.column(col, width=min(int(max_len * 9), 200), anchor="w")

        for _, row in display.iterrows():
            vals = [str(v) if pd.notna(v) else "" for v in row]
            self.tree.insert("", "end", values=vals)

        self._log(f"📋 Showing {len(display)}/{len(df)} rows")

    def _run_threaded(self, func):
        """Run function in background thread."""
        t = threading.Thread(target=func, daemon=True)
        t.start()

    # ── Button Actions ──

    def do_init_db(self):
        init_db()
        self._log("✅ Database initialized at " + str(DB_PATH))
        messagebox.showinfo("Done", f"Database ready at:\n{DB_PATH}")

    def do_ohlc_get(self):
        f, t = self._get_dates()
        if not f:
            return

        def work():
            self._status("📡 Fetching OHLC data...")
            self._log(f"📡 GET OHLC: {f} → {t}")
            self.progress["value"] = 20

            try:
                df, fpath = fetch_ohlc(f, t)
                self.progress["value"] = 80

                if df.empty:
                    self._log("⚠️  No data returned. Check dates or network.")
                    self._status("No data")
                    self.progress["value"] = 0
                    return

                if self.active_only.get():
                    display = df[df["traded_volume"] > 0].copy()
                else:
                    display = df.copy()

                self._populate_tree(display)
                self._log(f"💾 Downloaded: {fpath}")
                self._log(f"📊 DB populated: {len(df)} rows ({display['symbol'].nunique()} active symbols)")
                self._log(f"   Date range: {df['trading_date'].min().date()} → {df['trading_date'].max().date()}")
                self._status(f"✅ {len(df)} rows fetched, saved & populated")
                self.progress["value"] = 100

            except Exception as e:
                self._log(f"❌ Error: {e}")
                self._status(f"Error: {e}")
                self.progress["value"] = 0

        self._run_threaded(work)

    def do_margins_get(self):
        _, t = self._get_dates()
        if not t:
            return

        def work():
            self._status("📡 Fetching Margins data...")
            self.progress["value"] = 20

            # Try target date, then walk back up to 5 days for holidays
            for i in range(6):
                dt = t - timedelta(days=i)
                if dt.weekday() >= 5:
                    continue
                self._log(f"📡 GET Margins: {dt}")

                try:
                    df, fpath = fetch_margins(dt)
                    if not df.empty:
                        self.progress["value"] = 80
                        if self.active_only.get():
                            display = df[df["is_active"] == True].copy()
                        else:
                            display = df.copy()

                        self._populate_tree(display)
                        self._log(f"💾 Downloaded: {fpath}")
                        self._log(f"📊 DB populated: {len(df)} rows ({display['contract_code'].nunique()} active)")
                        self._status(f"✅ {len(df)} contracts for {dt}")
                        self.progress["value"] = 100
                        return

                except Exception as e:
                    self._log(f"❌ Error for {dt}: {e}")
                    continue

            self._log("⚠️  No margins file found in last 5 trading days")
            self._status("No data")
            self.progress["value"] = 0

        self._run_threaded(work)

    def do_ohlc_backfill(self):
        f, t = self._get_dates()
        if not f:
            return

        def work():
            self._status("⏪ OHLC Backfill running...")
            self._log(f"⏪ OHLC Backfill: {f} → {t}")

            session = requests.Session()
            total_rows = 0
            cur = f
            chunks_done = 0
            total_days = (t - f).days
            total_chunks = max(1, total_days // 90 + 1)

            while cur < t:
                chunk_end = min(cur + timedelta(days=89), t)
                self._log(f"   {cur} → {chunk_end}")

                try:
                    df, fpath = fetch_ohlc(cur, chunk_end, session)
                    if not df.empty:
                        total_rows += len(df)
                        self._log(f"   ✓ {len(df)} rows, {df['symbol'].nunique()} symbols")
                except Exception as e:
                    self._log(f"   ✗ Error: {e}")

                chunks_done += 1
                pct = int(chunks_done / total_chunks * 100)
                self.progress["value"] = pct
                self._status(f"⏪ Backfill {pct}% — {total_rows} rows so far")

                cur = chunk_end + timedelta(days=1)
                time.sleep(2.0)

            self._log(f"✅ OHLC Backfill complete: {total_rows} total rows")
            self._status(f"✅ Backfill done: {total_rows} rows")
            self.progress["value"] = 100

        self._run_threaded(work)

    def do_margins_backfill(self):
        f, t = self._get_dates()
        if not f:
            return

        def work():
            self._status("⏪ Margins Backfill running...")
            self._log(f"⏪ Margins Backfill: {f} → {t}")

            success = 0
            skipped = 0
            cur = f
            total_days = (t - f).days

            while cur <= t:
                if cur.weekday() >= 5:
                    cur += timedelta(days=1)
                    continue

                try:
                    df, fpath = fetch_margins(cur)
                    if not df.empty:
                        success += 1
                        self._log(f"   ✓ {cur}: {len(df)} contracts")
                    else:
                        skipped += 1
                except Exception as e:
                    self._log(f"   ✗ {cur}: {e}")
                    skipped += 1

                elapsed = (cur - f).days
                pct = int(elapsed / max(1, total_days) * 100)
                self.progress["value"] = pct
                self._status(f"⏪ Margins {pct}% — {success} days fetched, {skipped} skipped")

                cur += timedelta(days=1)
                time.sleep(0.5)

            self._log(f"✅ Margins Backfill: {success} days, {skipped} skipped")
            self._status(f"✅ Margins done: {success} days")
            self.progress["value"] = 100

        self._run_threaded(work)

    def do_view_ohlc(self):
        self._status("Loading OHLC from DB...")
        f, t = self._get_dates()
        if not f:
            return

        conn = get_db()
        where = "WHERE traded_volume > 0" if self.active_only.get() else ""
        query = f"""
            SELECT trading_date, symbol, open, high, low, close,
                   traded_volume, settlement_price, fx_rate
            FROM pmex_ohlc
            {where}
            AND trading_date BETWEEN '{f.isoformat()}' AND '{t.isoformat()}'
            ORDER BY trading_date DESC, symbol
            LIMIT 5000
        """
        # Fix SQL when no WHERE before AND
        if not self.active_only.get():
            query = query.replace("{where}\n            AND", "WHERE")

        df = pd.read_sql_query(query, conn)
        conn.close()

        self._populate_tree(df)
        self._log(f"📋 OHLC from DB: {len(df)} rows ({f} → {t})")
        self._status(f"Showing {len(df)} OHLC rows from DB")

    def do_view_margins(self):
        self._status("Loading Margins from DB...")
        _, t = self._get_dates()
        if not t:
            return

        conn = get_db()
        active_filter = "AND is_active = 1" if self.active_only.get() else ""
        query = f"""
            SELECT report_date, product_group, contract_code,
                   reference_price, initial_margin_pct, initial_margin_value,
                   wcm, maintenance_margin, lower_limit, upper_limit, fx_rate
            FROM pmex_margins
            WHERE report_date = (
                SELECT MAX(report_date) FROM pmex_margins
                WHERE report_date <= '{t.isoformat()}'
            )
            {active_filter}
            ORDER BY product_group, contract_code
        """
        df = pd.read_sql_query(query, conn)
        conn.close()

        self._populate_tree(df)
        self._log(f"📋 Margins from DB: {len(df)} contracts")
        self._status(f"Showing {len(df)} margin rows from DB")


# ═══════════════════════════════════════════════════════════════════════
# RUN
# ═══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    root = tk.Tk()
    app = PMEXApp(root)
    root.mainloop()
