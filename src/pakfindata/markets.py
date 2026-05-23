"""
pakfindata.markets — Single-source-of-truth symbol classification.

Routes all "what is this symbol" questions through one place.
Looks up the existing masters (instruments → symbols → futures_eod) in priority
order; falls back to symbol-structure parsing; returns 'UNKNOWN' if nothing
matches.

DESIGN:
    - Read-only; never writes to any table
    - One-time master load per process (cached)
    - Cheap per-call: dict lookup
    - Vendor-namespace translation included

USAGE:
    from pakfindata.markets import classify, parent_of, is_futures, is_index

    classify("OGDC")          # -> "REG"
    classify("OGDC-CMAR")     # -> "CSF"     (PSX cash-settled, C-prefix)
    classify("OGDC-MAY")      # -> "DFC"     (PSX deliverable / vendor format)
    classify("OGDC-MAYC")     # -> "CSF"     (vendor-only suffix-C)
    classify("KSE100")        # -> "IDX"
    classify("BKTI-APR")      # -> "IDX_FUT" (PSX index future)
    classify("AKBLTFC6")      # -> "ODL"     (PSX odd-lot bond)
    classify("UBLPETF")       # -> "ETF"     (override of mislabel in instruments)
    classify("WHATIS")        # -> "UNKNOWN"

    parent_of("OGDC-CMAR")    # -> "OGDC"
    parent_of("OGDC")         # -> None
    is_futures("OGDC-MAY")    # -> True
    is_index("KSE100")        # -> True

LONG-LIVED PROCESSES:
    The master is cached for the life of the process. After EOD ingest adds new
    symbols (or updates instrument_type), call markets.reload_master() to pick
    them up. Streamlit page reloads re-import the module fresh per session,
    so this only matters for genuinely long-running services (fusion_service,
    background daemons). Per-page-render tools see fresh data automatically.

VENDOR TRANSLATION:
    The psxterminal.com WebSocket lumps DFC + CSF as a single 'FUT' market.
    PSX itself separates them. This module translates:

        from_vendor_market("FUT", "OGDC-MAY")   -> "DFC"
        from_vendor_market("FUT", "OGDC-MAYC")  -> "CSF"
        from_vendor_market("REG", "OGDC")       -> "REG"
        from_vendor_market("IDX", "KSE100")     -> "IDX"

PSX TAXONOMY (canonical):
    REG     Regular equities
    DFC     Deliverable futures (PSX market_type='FUT')
    CSF     Cash-settled futures (PSX market_type='CONT')
    IDX     Indices (KSE100, KMI30, etc.)
    IDX_FUT Index futures (PSX market_type='IDX_FUT')
    ODL     Odd lot (PSX market_type='ODL')
    ETF     Exchange-traded funds (per symbols.sector_name override)
    REIT    Real estate investment trusts (per symbols.sector_name override)
    UNKNOWN Symbol not in any master and not parseable by structure
"""

import logging
import re
import sqlite3
from functools import lru_cache
from threading import Lock
from typing import Optional

from pakfindata.settings import get_settings

logger = logging.getLogger(__name__)

# --- PSX market_type -> canonical class mapping (used by Pass C) ---
PSX_MARKET_TO_CANONICAL = {
    "FUT":     "DFC",
    "CONT":    "CSF",
    "IDX_FUT": "IDX_FUT",
    "ODL":     "ODL",
}

# --- Vendor-format regex (psxterminal.com WebSocket; final fallback) ---
_MONTHS = "JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC"
# Vendor CSF: SYMBOL-MMM with B/C/D suffix (e.g. OGDC-MAYC, HUBC-MAYB)
_VENDOR_CSF_PATTERN = re.compile(rf"^([A-Z0-9]+)-({_MONTHS})([BCD])$")
# Vendor DFC: SYMBOL-MMM with no extra suffix (e.g. OGDC-MAY)
_VENDOR_DFC_PATTERN = re.compile(rf"^([A-Z0-9]+)-({_MONTHS})$")

# --- Cache ---
_master_lock = Lock()
_master_cache: Optional[dict[str, dict]] = None


def _load_master() -> dict[str, dict]:
    """Load all masters into a single in-memory dict.

    Pass A   - instruments table          (REG/IDX from instrument_type)
    Pass A.5 - symbols.sector_name        (override EQUITY -> ETF/REIT)
    Pass B   - symbols (fill-in fallback) (anything not seen yet)
    Pass C   - futures_eod                (CSF/DFC/IDX_FUT/ODL via stored
                                           market_type + parse_futures_symbol
                                           for parent + expiry)

    Returns: {symbol: {market_class, parent_symbol, expiry_month, source, name}}
    """
    global _master_cache
    with _master_lock:
        if _master_cache is not None:
            return _master_cache

        # Lazy import — avoids any chance of circular import at module load.
        from pakfindata.sources.market_summary import parse_futures_symbol

        out: dict[str, dict] = {}
        db_path = get_settings().db_path
        con = sqlite3.connect(db_path)
        con.row_factory = sqlite3.Row

        try:
            # --- Pass A: instruments table (explicit instrument_type) ---
            for r in con.execute("""
                SELECT symbol, instrument_type, name
                FROM instruments
                WHERE is_active = 1
            """):
                t = r["instrument_type"]
                if t == "EQUITY":
                    cls = "REG"
                elif t == "INDEX":
                    cls = "IDX"
                elif t == "ETF":
                    cls = "ETF"
                elif t == "REIT":
                    cls = "REIT"
                else:
                    cls = "REG"  # safe default for unknown instrument_type

                out[r["symbol"]] = {
                    "market_class": cls,
                    "parent_symbol": None,
                    "expiry_month": None,
                    "source": "instruments",
                    "name": r["name"],
                }

            # --- Pass A.5: ETF/REIT override via symbols.sector_name ---
            # instruments table mistypes ETFs/REITs as EQUITY; sector_name is
            # the truth. Override Pass A's class when sector_name matches.
            override_count = 0
            for r in con.execute("""
                SELECT symbol, sector_name FROM symbols
                WHERE is_active = 1
                  AND sector_name IS NOT NULL
                  AND (
                    sector_name LIKE '%EXCHANGE TRADED FUND%'
                    OR sector_name LIKE '%REAL ESTATE INVESTMENT TRUST%'
                  )
            """):
                sector = (r["sector_name"] or "").upper().strip()
                if "EXCHANGE TRADED FUND" in sector:
                    target = "ETF"
                elif "REAL ESTATE INVESTMENT TRUST" in sector:
                    target = "REIT"
                else:
                    continue

                if r["symbol"] in out:
                    if out[r["symbol"]]["market_class"] != target:
                        out[r["symbol"]]["market_class"] = target
                        out[r["symbol"]]["source"] = "symbols.sector_name (override)"
                        override_count += 1
                else:
                    out[r["symbol"]] = {
                        "market_class": target,
                        "parent_symbol": None,
                        "expiry_month": None,
                        "source": "symbols.sector_name",
                        "name": None,
                    }

            # --- Pass B: symbols table (fill-in for anything not yet seen) ---
            for r in con.execute("""
                SELECT symbol, sector_name, name
                FROM symbols
                WHERE is_active = 1
            """):
                if r["symbol"] in out:
                    continue
                sector = (r["sector_name"] or "").upper().strip()
                if "EXCHANGE TRADED FUND" in sector:
                    cls = "ETF"
                elif "REAL ESTATE INVESTMENT TRUST" in sector:
                    cls = "REIT"
                else:
                    cls = "REG"
                out[r["symbol"]] = {
                    "market_class": cls,
                    "parent_symbol": None,
                    "expiry_month": None,
                    "source": "symbols",
                    "name": r["name"],
                }

            # --- Pass C: futures_eod (CSF/DFC/IDX_FUT/ODL via stored type) ---
            # Trust the stored market_type — it was set at ingest from PSX
            # sector_code, which is authoritative. Use parse_futures_symbol
            # for parent + expiry extraction so parent_of/expiry_of become O(1).
            futures_count = 0
            for r in con.execute("""
                SELECT DISTINCT symbol, market_type
                FROM futures_eod
            """):
                sym = r["symbol"]
                mtype = r["market_type"]
                cls = PSX_MARKET_TO_CANONICAL.get(mtype)
                if cls is None:
                    continue  # unknown market_type — skip rather than guess

                # Pass C overrides equity-master entries when the symbol is
                # also (correctly) in futures_eod. In practice this should
                # rarely collide — futures symbols (-CMMM / -MMM) don't appear
                # in instruments — but if they ever do, the futures_eod row
                # with explicit market_type wins.
                try:
                    base, month = parse_futures_symbol(sym, mtype)
                except Exception:
                    base, month = sym, None

                # If base == sym for non-ODL, the parser couldn't extract a
                # meaningful parent (e.g. an unusual variant). Store None for
                # parent rather than the symbol itself, since "parent == self"
                # is wrong semantically for derivatives.
                parent = base if (base and base != sym) else None
                # ODL bonds are leaf instruments — no parent equity.
                if mtype == "ODL":
                    parent = None

                out[sym] = {
                    "market_class": cls,
                    "parent_symbol": parent,
                    "expiry_month": month,
                    "source": f"futures_eod[{mtype}]",
                    "name": None,
                }
                futures_count += 1
        finally:
            con.close()

        _master_cache = out
        logger.info(
            "markets: loaded %d symbols (instruments=%d, etf/reit override=%d, "
            "symbols-fallback=%d, futures_eod=%d)",
            len(out),
            sum(1 for v in out.values() if v["source"] == "instruments"),
            override_count,
            sum(1 for v in out.values() if v["source"] == "symbols"),
            futures_count,
        )
        return out


def reload_master() -> None:
    """Force reload on next classify() call. Use after master tables are updated."""
    global _master_cache
    with _master_lock:
        _master_cache = None


# --- Structural classification (fallback for symbols not in any master) ---

def _classify_via_structure(sym: str) -> Optional[tuple[str, Optional[str], Optional[str]]]:
    """Try to classify a symbol by structure alone.

    Used only for symbols not present in any master table — e.g. fresh
    vendor-WS contracts that haven't been EOD-ingested yet.

    Order:
      1. parse_futures_symbol(sym, "CONT") -> CSF if -C{MMM} shape parses
      2. parse_futures_symbol(sym, "FUT")  -> DFC if -{MMM} shape parses
      3. Vendor regex (suffix BCD)         -> CSF (psxterminal.com WS form)
      4. Vendor regex (no suffix)          -> DFC

    Returns: (market_class, parent_symbol, expiry_month) or None.
    """
    if not sym:
        return None

    # Lazy import to avoid any cycle.
    from pakfindata.sources.market_summary import parse_futures_symbol

    # Pass 1: PSX CONT shape (-C{MMM})
    if "-C" in sym:
        try:
            base, month = parse_futures_symbol(sym, "CONT")
            if base and base != sym:
                return ("CSF", base, month)
        except Exception:
            pass

    # Pass 2: PSX FUT shape (-{MMM})
    if "-" in sym:
        try:
            base, month = parse_futures_symbol(sym, "FUT")
            if base and base != sym and month is not None:
                return ("DFC", base, month)
        except Exception:
            pass

    # Pass 3: vendor CSF (suffix BCD form, e.g. OGDC-MAYC)
    m = _VENDOR_CSF_PATTERN.match(sym)
    if m:
        return ("CSF", m.group(1), m.group(2))

    # Pass 4: vendor DFC (e.g. OGDC-MAY)
    m = _VENDOR_DFC_PATTERN.match(sym)
    if m:
        return ("DFC", m.group(1), m.group(2))

    return None


# --- Public API ---

def classify(symbol: str) -> str:
    """Return the canonical PSX market classification for a symbol.

    Returns: 'REG' | 'DFC' | 'CSF' | 'IDX' | 'IDX_FUT' | 'ODL' | 'ETF' | 'REIT' | 'UNKNOWN'
    """
    if not symbol:
        return "UNKNOWN"
    sym = symbol.upper().strip()

    # Master lookup wins (covers REG/IDX/ETF/REIT and PSX-known CSF/DFC/IDX_FUT/ODL)
    master = _load_master()
    if sym in master:
        return master[sym]["market_class"]

    # Structural fallback for symbols not in any master
    result = _classify_via_structure(sym)
    if result:
        return result[0]

    # Truly unknown — log once per symbol per process
    _log_unknown(sym)
    return "UNKNOWN"


@lru_cache(maxsize=10000)
def _log_unknown(symbol: str) -> None:
    logger.warning("markets: unknown symbol '%s' -- add to instruments master", symbol)


def parent_of(symbol: str) -> Optional[str]:
    """Return the underlying equity for a futures contract, else None.

    OGDC-CMAR -> OGDC   (PSX CSF)
    OGDC-MAY  -> OGDC   (PSX DFC / vendor)
    OGDC-MAYC -> OGDC   (vendor CSF)
    OGDC      -> None   (already underlying)
    KSE100    -> None   (index, no underlying)
    AKBLTFC6  -> None   (ODL bond, no parent)
    """
    if not symbol:
        return None
    sym = symbol.upper().strip()

    # Cached lookup (O(1)) for symbols already classified by Pass C
    master = _load_master()
    if sym in master:
        return master[sym].get("parent_symbol")

    # Structural fallback
    result = _classify_via_structure(sym)
    if result:
        return result[1]
    return None


def expiry_of(symbol: str) -> Optional[str]:
    """Return the 3-letter month code for futures, e.g. 'MAY'. Else None."""
    if not symbol:
        return None
    sym = symbol.upper().strip()

    master = _load_master()
    if sym in master:
        return master[sym].get("expiry_month")

    result = _classify_via_structure(sym)
    if result:
        return result[2]
    return None


def contract_series(symbol: str) -> Optional[str]:
    """Return the B/C/D series letter for vendor-format CSF futures. Else None.

    Note: this only applies to the vendor-format `-MMM[BCD]` shape. PSX-format
    CONT symbols (`-CMMM`) do not have a B/C/D series component.
    """
    if not symbol:
        return None
    m = _VENDOR_CSF_PATTERN.match(symbol.upper().strip())
    if m:
        return m.group(3)
    return None


def is_futures(symbol: str) -> bool:
    """True for both DFC and CSF (excludes IDX_FUT)."""
    return classify(symbol) in ("DFC", "CSF")


def is_deliverable_futures(symbol: str) -> bool:
    return classify(symbol) == "DFC"


def is_cash_settled_futures(symbol: str) -> bool:
    return classify(symbol) == "CSF"


def is_index(symbol: str) -> bool:
    return classify(symbol) == "IDX"


def is_index_futures(symbol: str) -> bool:
    return classify(symbol) == "IDX_FUT"


def is_odd_lot(symbol: str) -> bool:
    return classify(symbol) == "ODL"


def is_equity(symbol: str) -> bool:
    return classify(symbol) == "REG"


def is_known(symbol: str) -> bool:
    return classify(symbol) != "UNKNOWN"


# --- Vendor namespace translation ---

def from_vendor_market(vendor_market: str, symbol: str) -> str:
    """Translate psxterminal.com's market label to canonical pakfindata taxonomy.

    psxterminal.com lumps DFC + CSF as 'FUT'. We split via the unified
    structural classifier so both PSX shapes (-CMMM, -MMM) and vendor
    shapes (-MMMC) route correctly.

    Args:
        vendor_market: 'REG' | 'FUT' | 'IDX' (what the WebSocket sends)
        symbol: needed to disambiguate FUT into DFC vs CSF

    Returns: canonical pakfindata market_class
    """
    sym = (symbol or "").upper().strip()
    if vendor_market == "FUT":
        # Cache lookup wins (covers PSX-known DFC/CSF)
        master = _load_master()
        if sym in master:
            cls = master[sym]["market_class"]
            if cls in ("DFC", "CSF"):
                return cls
        # Structural fallback (also catches the vendor -MMMC form)
        result = _classify_via_structure(sym)
        if result and result[0] in ("DFC", "CSF"):
            return result[0]
        return "FUT"  # passthrough if unparseable
    if vendor_market == "IDX":
        return "IDX"
    if vendor_market == "REG":
        return "REG"
    return vendor_market  # passthrough for ODL/BNB/whatever


# --- Bulk helpers (for migrations and reports) ---

def get_unknown_symbols(observed: list[str]) -> list[str]:
    """Return symbols seen in data but not classified by any master.

    Useful for:
      - Daily report: 'these new symbols showed up, please add them'
      - Loader checkpointing: 'fail loudly when ingesting unknown symbols'
    """
    return [s for s in observed if classify(s) == "UNKNOWN"]


def all_symbols_by_class() -> dict[str, list[str]]:
    """Return {market_class: [symbols]} for everything in the master.

    Doesn't include UNKNOWN (those aren't in any master by definition).
    """
    master = _load_master()
    out: dict[str, list[str]] = {}
    for sym, info in master.items():
        cls = info["market_class"]
        out.setdefault(cls, []).append(sym)
    return out
