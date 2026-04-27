"""PMEX Contract Calendar — parse contract names, track expiries, rollover windows.

Contract naming convention:
    {BASE}{SIZE}-{MONTH_CODE}{YEAR}[ID]

Examples:
    GO100OZ-JU26    → Gold 100oz, June 2026
    CRUDE10-MY26ID  → WTI Crude 10bbl Intraday, May 2026
    TOLAGOLD-WED    → Tola Gold, Weekly Wednesday settlement
    GOLDEURUSD-MY26 → Gold EUR/USD cross, May 2026

Month codes:
    JA=Jan, FB=Feb, MR=Mar, AP=Apr, MY=May, JU=Jun,
    JY=Jul, AU=Aug, SP=Sep, OC=Oct, NV=Nov, DC=Dec

Suffix "ID" = Intraday contract (same commodity, different margin/settlement).
Suffix "WED" = Weekly Wednesday physical delivery (no month-year expiry).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, timedelta

import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
# Month code → calendar month mapping
# ─────────────────────────────────────────────────────────────────────────────

MONTH_CODES: dict[str, int] = {
    "JA": 1, "FB": 2, "MR": 3, "AP": 4, "MY": 5, "JU": 6,
    "JY": 7, "AU": 8, "SP": 9, "OC": 10, "NV": 11, "DC": 12,
}

_MONTH_CODE_REVERSE: dict[int, str] = {v: k for k, v in MONTH_CODES.items()}

# ─────────────────────────────────────────────────────────────────────────────
# Product classification (base → metadata)
# ─────────────────────────────────────────────────────────────────────────────

_PRODUCT_MAP: dict[str, dict] = {
    # Gold
    "GOMOZ":     {"commodity": "Gold", "lot_size": "mini oz", "currency": "USD", "category": "Metals"},
    "GO1OZ":     {"commodity": "Gold", "lot_size": "1 oz", "currency": "USD", "category": "Metals"},
    "GO10OZ":    {"commodity": "Gold", "lot_size": "10 oz", "currency": "USD", "category": "Metals"},
    "GO100OZ":   {"commodity": "Gold", "lot_size": "100 oz", "currency": "USD", "category": "Metals"},
    "TOLAGOLD":  {"commodity": "Gold", "lot_size": "1 tola", "currency": "PKR", "category": "Phy_Gold"},
    "MTOLAGOLD": {"commodity": "Gold", "lot_size": "mini tola", "currency": "PKR", "category": "Phy_Gold"},
    # Silver
    "SL100OZ":   {"commodity": "Silver", "lot_size": "100 oz", "currency": "USD", "category": "Metals"},
    "SL1000OZ":  {"commodity": "Silver", "lot_size": "1000 oz", "currency": "USD", "category": "Metals"},
    # Platinum / Palladium
    "PLATINUM5":   {"commodity": "Platinum", "lot_size": "5 oz", "currency": "USD", "category": "Metals"},
    "PLATINUM50":  {"commodity": "Platinum", "lot_size": "50 oz", "currency": "USD", "category": "Metals"},
    "PALDIUM100":  {"commodity": "Palladium", "lot_size": "100 oz", "currency": "USD", "category": "Metals"},
    # Aluminum / Copper
    "ALUMINUM1":  {"commodity": "Aluminum", "lot_size": "1 MT", "currency": "USD", "category": "Metals"},
    "ALUMINUM5":  {"commodity": "Aluminum", "lot_size": "5 MT", "currency": "USD", "category": "Metals"},
    "COPPER":     {"commodity": "Copper", "lot_size": "1 lb", "currency": "USD", "category": "Metals"},
    "COPPER25K":  {"commodity": "Copper", "lot_size": "25K lb", "currency": "USD", "category": "Metals"},
    # Crude Oil
    "CRUDE10":    {"commodity": "WTI Crude", "lot_size": "10 bbl", "currency": "USD", "category": "Oil"},
    "CRUDE100":   {"commodity": "WTI Crude", "lot_size": "100 bbl", "currency": "USD", "category": "Oil"},
    "CRUDE1000":  {"commodity": "WTI Crude", "lot_size": "1000 bbl", "currency": "USD", "category": "Oil"},
    "BRENT10":    {"commodity": "Brent Crude", "lot_size": "10 bbl", "currency": "USD", "category": "Oil"},
    "BRENT100":   {"commodity": "Brent Crude", "lot_size": "100 bbl", "currency": "USD", "category": "Oil"},
    "BRENT1000":  {"commodity": "Brent Crude", "lot_size": "1000 bbl", "currency": "USD", "category": "Oil"},
    # Energy
    "NGAS10K":    {"commodity": "Natural Gas", "lot_size": "10K mmbtu", "currency": "USD", "category": "Energy"},
    "NGAS1K":     {"commodity": "Natural Gas", "lot_size": "1K mmbtu", "currency": "USD", "category": "Energy"},
    # Agriculture
    "ICORN":      {"commodity": "Corn", "lot_size": "standard", "currency": "USD", "category": "Agri"},
    "ICOTTON":    {"commodity": "Cotton", "lot_size": "standard", "currency": "USD", "category": "Agri"},
    "ICOTTON50K": {"commodity": "Cotton", "lot_size": "50K", "currency": "USD", "category": "Agri"},
    "ISOYBEAN":   {"commodity": "Soybean", "lot_size": "standard", "currency": "USD", "category": "Agri"},
    "IWHEAT":     {"commodity": "Wheat", "lot_size": "standard", "currency": "USD", "category": "Agri"},
    # Indices
    "NSDQ100":    {"commodity": "NASDAQ 100", "lot_size": "standard", "currency": "USD", "category": "Indices"},
    "2NSDQ100":   {"commodity": "Mini NASDAQ 100", "lot_size": "mini", "currency": "USD", "category": "Indices"},
    "SP500":      {"commodity": "S&P 500", "lot_size": "standard", "currency": "USD", "category": "Indices"},
    "DJ":         {"commodity": "Dow Jones", "lot_size": "standard", "currency": "USD", "category": "Indices"},
    "JPYEQTY1":   {"commodity": "JPY Equity 1", "lot_size": "1", "currency": "JPY", "category": "Indices"},
    "JPYEQTY5":   {"commodity": "JPY Equity 5", "lot_size": "5", "currency": "JPY", "category": "Indices"},
}

# Regex: match GOLD cross-currency COTS contracts like GOLDEURUSD-MY26ID
_COTS_RE = re.compile(r"^GOLD([A-Z]{6})$")

# ─────────────────────────────────────────────────────────────────────────────
# Data class
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class PmexContract:
    """Parsed PMEX contract metadata."""
    raw: str             # "GO100OZ-JU26ID"
    base: str            # "GO100OZ"
    expiry_code: str     # "JU26" or "WED"
    is_intraday: bool    # True if suffix ID
    is_weekly: bool      # True if expiry_code == "WED"
    commodity: str       # "Gold"
    category: str        # "Metals"
    lot_size: str        # "100 oz"
    currency: str        # "USD"
    expiry_date: date | None  # 2026-06-25 (last Thursday) or None if weekly


# ─────────────────────────────────────────────────────────────────────────────
# Core parsing
# ─────────────────────────────────────────────────────────────────────────────


def _last_thursday(year: int, month: int) -> date:
    """Return the last Thursday of the given month."""
    # Start from last day of month, walk backward to Thursday
    if month == 12:
        last_day = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = date(year, month + 1, 1) - timedelta(days=1)
    # Thursday = weekday 3
    days_back = (last_day.weekday() - 3) % 7
    return last_day - timedelta(days=days_back)


def expiry_code_to_date(code: str) -> date | None:
    """Convert PMEX expiry code to expiry date.

    Args:
        code: e.g. "JU26", "MY26", "WED"

    Returns:
        Last Thursday of the expiry month, or None for weekly/unparseable.
    """
    if not code or code.upper() == "WED":
        return None

    code = code.upper()
    if len(code) < 3:
        return None

    month_code = code[:2]
    year_str = code[2:]

    month = MONTH_CODES.get(month_code)
    if month is None:
        return None

    try:
        year = 2000 + int(year_str)
    except ValueError:
        return None

    return _last_thursday(year, month)


def parse_contract(contract: str) -> PmexContract:
    """Parse a PMEX contract string into structured metadata.

    Args:
        contract: e.g. "GO100OZ-JU26", "CRUDE10-MY26ID", "TOLAGOLD-WED"

    Returns:
        PmexContract with all fields populated.
    """
    raw = contract.strip()

    # Split on hyphen: BASE-EXPIRY[ID]
    parts = raw.split("-", 1)
    base = parts[0]
    tail = parts[1] if len(parts) > 1 else ""

    # Detect intraday suffix
    is_intraday = tail.upper().endswith("ID") and tail.upper() != "WED"
    expiry_code = tail[:-2] if is_intraday else tail
    is_weekly = expiry_code.upper() == "WED"

    expiry_dt = expiry_code_to_date(expiry_code)

    # Classify product
    info = _classify_base(base)

    return PmexContract(
        raw=raw,
        base=base,
        expiry_code=expiry_code.upper(),
        is_intraday=is_intraday,
        is_weekly=is_weekly,
        commodity=info["commodity"],
        category=info["category"],
        lot_size=info["lot_size"],
        currency=info["currency"],
        expiry_date=expiry_dt,
    )


def _classify_base(base: str) -> dict:
    """Classify a base product into commodity/lot_size/currency/category."""
    # Direct lookup
    info = _PRODUCT_MAP.get(base)
    if info:
        return info

    # COTS cross-currency gold: GOLDEURUSD, GOLDGBPJPY, etc.
    m = _COTS_RE.match(base)
    if m:
        pair = m.group(1)
        return {
            "commodity": f"Gold {pair[:3]}/{pair[3:]}",
            "lot_size": "cross",
            "currency": pair[:3],
            "category": "Cots",
        }

    # Unknown — return sensible defaults
    return {
        "commodity": base,
        "lot_size": "unknown",
        "currency": "USD",
        "category": "Other",
    }


def classify_product(base: str) -> dict:
    """Public wrapper for base product classification.

    Returns:
        Dict with keys: commodity, lot_size, currency, category.
    """
    return _classify_base(base)


# ─────────────────────────────────────────────────────────────────────────────
# Expiry & chain helpers
# ─────────────────────────────────────────────────────────────────────────────


def days_to_expiry(contract: str, as_of: date | None = None) -> int | None:
    """Return days remaining until contract expiry.

    Args:
        contract: Full contract string, e.g. "GO1OZ-JU26"
        as_of: Reference date (defaults to today).

    Returns:
        Integer days to expiry, or None if weekly/unparseable.
    """
    pc = parse_contract(contract)
    if pc.expiry_date is None:
        return None
    ref = as_of or date.today()
    return (pc.expiry_date - ref).days


def get_contract_chain(
    base_product: str,
    contracts: list[str],
    as_of: date | None = None,
) -> list[PmexContract]:
    """Return sorted chain of contracts for a base product (near→far month).

    Filters out expired contracts (unless as_of is None) and intraday variants.

    Args:
        base_product: e.g. "GO1OZ", "CRUDE100"
        contracts: List of all contract strings.
        as_of: Reference date to filter expired.

    Returns:
        List of PmexContract sorted by expiry_date ascending.
    """
    ref = as_of or date.today()
    chain = []
    for c in contracts:
        pc = parse_contract(c)
        if pc.base != base_product:
            continue
        if pc.is_intraday or pc.is_weekly:
            continue
        if pc.expiry_date is None:
            continue
        if pc.expiry_date >= ref:
            chain.append(pc)

    chain.sort(key=lambda p: p.expiry_date)
    return chain


def get_near_month(
    base_product: str,
    contracts: list[str],
    as_of: date | None = None,
) -> str | None:
    """Return the nearest non-expired contract for a base product.

    Returns:
        Contract string (e.g. "GO1OZ-JU26") or None if none found.
    """
    chain = get_contract_chain(base_product, contracts, as_of)
    return chain[0].raw if chain else None


def get_all_base_products(contracts: list[str]) -> list[str]:
    """Extract unique base products from a list of contracts."""
    bases = set()
    for c in contracts:
        pc = parse_contract(c)
        if not pc.is_intraday:
            bases.add(pc.base)
    return sorted(bases)


# ─────────────────────────────────────────────────────────────────────────────
# Rollover calendar
# ─────────────────────────────────────────────────────────────────────────────


def get_rollover_calendar(
    contracts: list[str],
    as_of: date | None = None,
) -> pd.DataFrame:
    """Build a rollover calendar for all active base products.

    Returns DataFrame with columns:
        base, commodity, category, near_contract, near_expiry, near_dte,
        far_contract, far_expiry, far_dte, rollover_imminent
    """
    ref = as_of or date.today()
    bases = get_all_base_products(contracts)
    rows = []

    for base in bases:
        chain = get_contract_chain(base, contracts, ref)
        if not chain:
            continue

        info = classify_product(base)
        near = chain[0]
        far = chain[1] if len(chain) > 1 else None

        near_dte = (near.expiry_date - ref).days if near.expiry_date else None
        far_dte = (far.expiry_date - ref).days if far and far.expiry_date else None

        rows.append({
            "base": base,
            "commodity": info["commodity"],
            "category": info["category"],
            "near_contract": near.raw,
            "near_expiry": near.expiry_date.isoformat() if near.expiry_date else None,
            "near_dte": near_dte,
            "far_contract": far.raw if far else None,
            "far_expiry": far.expiry_date.isoformat() if far and far.expiry_date else None,
            "far_dte": far_dte,
            "rollover_imminent": near_dte is not None and near_dte <= 14,
        })

    return pd.DataFrame(rows) if rows else pd.DataFrame()


def next_wednesday(as_of: date | None = None) -> date:
    """Return the next Wednesday from the reference date."""
    ref = as_of or date.today()
    days_ahead = (2 - ref.weekday()) % 7  # Wednesday = 2
    if days_ahead == 0:
        days_ahead = 7
    return ref + timedelta(days=days_ahead)
