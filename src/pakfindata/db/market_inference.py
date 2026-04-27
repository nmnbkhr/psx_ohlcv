"""Symbol-only market inference — used when sector_code isn't available.

For the comprehensive classifier that uses sector_code, see
`pakfindata.sources.market_summary.classify_market_type`.
"""

from __future__ import annotations

_MONTHS = {"JAN", "FEB", "MAR", "APR", "MAY", "JUN",
           "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"}

_INDEX_SYMBOLS = {
    "KSE100", "KSE100PR", "KSE30", "KMI30", "KMIALLSHR",
    "JSMFI", "JSGMFETF", "MII30", "PSXDIV20", "ALLSHR",
    "UPP9",
}


def _is_month(tok: str) -> bool:
    """True if `tok` is a month code, possibly with trailing B (e.g. 'FEB', 'FEBB')."""
    t = tok.upper()
    return t in _MONTHS or (t.endswith("B") and t[:-1] in _MONTHS)


def infer_market(symbol: str) -> str:
    """Infer market from symbol pattern alone.

    Returns one of: REG | FUT | CONT | IDX_FUT | IDX

      - FUT   : `XXX-FEB`, `XXX-APRB`      (month-suffix futures)
      - CONT  : `XXX-CFEB`, `XXX-CAPRB`   (continuous contracts)
      - IDX   : KSE*/KMI* indices
      - IDX_FUT: `KSE30-FEB`, `KMI30-APR`  (index futures)
      - REG   : everything else (default)

    Note: ODL (odd-lot) cannot be inferred from symbol alone — ODL shares the
    same ticker as the REG listing. Callers should set ODL explicitly when
    they know the source is the odd-lot feed.
    """
    if not symbol:
        return "REG"

    s = symbol.strip().upper()

    # Index tickers (no contract suffix)
    if "-" not in s:
        return "IDX" if s in _INDEX_SYMBOLS else "REG"

    base, _, suffix = s.rpartition("-")

    # Continuous contract: XXX-CFEB, XXX-CAPRB
    if suffix.startswith("C") and _is_month(suffix[1:]):
        return "CONT"

    # Index futures: KSE30-FEB, KMI30-APR
    if _is_month(suffix) and base in _INDEX_SYMBOLS:
        return "IDX_FUT"

    # Regular futures: XXX-FEB, XXX-APRB
    if _is_month(suffix):
        return "FUT"

    # Unknown suffix — treat as REG (e.g., "XD", "XB", "NC", "WU" corporate-action codes)
    return "REG"
