"""Commodity universe configuration.

Defines the full commodity universe with ticker mappings per data source,
categories, units, and Pakistan relevance scores.
"""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class CommodityDef:
    """Definition of a single commodity in the universe."""

    symbol: str  # Internal canonical symbol (e.g., "GOLD", "CRUDE_WTI")
    name: str  # Human-readable name
    category: str  # metals, energy, agriculture, livestock, fx
    unit: str  # USD/oz, USD/bbl, USD/lb, etc.
    pk_relevance: str  # HIGH, MEDIUM, LOW
    yf_ticker: str | None = None  # yfinance futures ticker
    yf_etf: str | None = None  # yfinance ETF proxy ticker
    fred_series: str | None = None  # FRED series ID (monthly)
    wb_column: str | None = None  # World Bank Pink Sheet column name
    pk_unit: str | None = None  # Pakistan local unit (tola, maund, bori)
    pk_conversion: str | None = None  # Name of PKR conversion function


# ─────────────────────────────────────────────────────────────────────────────
# FULL COMMODITY UNIVERSE — free data sources only
# ─────────────────────────────────────────────────────────────────────────────

COMMODITY_UNIVERSE: dict[str, CommodityDef] = {}


def _register(*defs: CommodityDef) -> None:
    for d in defs:
        COMMODITY_UNIVERSE[d.symbol] = d


# ── Precious Metals ──────────────────────────────────────────────────────────
_register(
    CommodityDef(
        symbol="GOLD", name="Gold", category="metals",
        unit="USD/oz", pk_relevance="HIGH",
        yf_ticker="GC=F", yf_etf="GLD",
        fred_series=None, wb_column="Gold",
        pk_unit="PKR/tola", pk_conversion="gold_usd_oz_to_pkr_tola",
    ),
    CommodityDef(
        symbol="SILVER", name="Silver", category="metals",
        unit="USD/oz", pk_relevance="HIGH",
        yf_ticker="SI=F", yf_etf="SLV",
        pk_unit="PKR/tola", pk_conversion="gold_usd_oz_to_pkr_tola",
    ),
    CommodityDef(
        symbol="PLATINUM", name="Platinum", category="metals",
        unit="USD/oz", pk_relevance="LOW",
        yf_ticker="PL=F",
    ),
)

# ── Base Metals ──────────────────────────────────────────────────────────────
_register(
    CommodityDef(
        symbol="COPPER", name="Copper", category="metals",
        unit="USD/lb", pk_relevance="HIGH",
        yf_ticker="HG=F",
    ),
    CommodityDef(
        symbol="IRON_ORE", name="Iron Ore 62%", category="metals",
        unit="USD/ton", pk_relevance="HIGH",
        yf_ticker="TIO=F",
        fred_series="PIORECRUSDM",
    ),
    CommodityDef(
        symbol="ALUMINUM", name="Aluminum", category="metals",
        unit="USD/ton", pk_relevance="MEDIUM",
        yf_ticker="ALI=F",
        fred_series="PAABORUSDM",
    ),
    CommodityDef(
        symbol="ZINC", name="Zinc", category="metals",
        unit="USD/ton", pk_relevance="MEDIUM",
        yf_ticker=None,  # Not on yfinance
        fred_series="PZINCUSDM",
    ),
    CommodityDef(
        symbol="NICKEL", name="Nickel", category="metals",
        unit="USD/ton", pk_relevance="MEDIUM",
        yf_ticker=None,
        fred_series="PNICKUSDM",
    ),
    CommodityDef(
        symbol="LEAD", name="Lead", category="metals",
        unit="USD/ton", pk_relevance="MEDIUM",
        yf_ticker=None,
        fred_series="PLEADUSDM",
    ),
    CommodityDef(
        symbol="TIN", name="Tin", category="metals",
        unit="USD/ton", pk_relevance="LOW",
        yf_ticker=None,
        fred_series="PTINUSDM",
    ),
    CommodityDef(
        symbol="STEEL_HRC", name="Steel (HRC)", category="metals",
        unit="USD/ton", pk_relevance="HIGH",
        yf_ticker=None,
        fred_series="PSTEEUSDM",
    ),
)

# ── Energy ───────────────────────────────────────────────────────────────────
_register(
    CommodityDef(
        symbol="CRUDE_WTI", name="Crude Oil WTI", category="energy",
        unit="USD/bbl", pk_relevance="HIGH",
        yf_ticker="CL=F", yf_etf="USO",
        fred_series="POILWTIUSDM",
        pk_unit="PKR/litre", pk_conversion="crude_usd_bbl_to_pkr_litre",
    ),
    CommodityDef(
        symbol="BRENT", name="Brent Crude", category="energy",
        unit="USD/bbl", pk_relevance="HIGH",
        yf_ticker="BZ=F",
        fred_series="POILBREUSDM",
        pk_unit="PKR/litre", pk_conversion="crude_usd_bbl_to_pkr_litre",
    ),
    CommodityDef(
        symbol="NATURAL_GAS", name="Natural Gas", category="energy",
        unit="USD/mmbtu", pk_relevance="HIGH",
        yf_ticker="NG=F", yf_etf="UNG",
        fred_series="PNGASEUUSDM",
    ),
    CommodityDef(
        symbol="COAL", name="Coal (Newcastle)", category="energy",
        unit="USD/ton", pk_relevance="HIGH",
        yf_ticker=None,  # Not on yfinance
        fred_series="PCOALAUUSDM",
    ),
    CommodityDef(
        symbol="HEATING_OIL", name="Heating Oil", category="energy",
        unit="USD/gal", pk_relevance="MEDIUM",
        yf_ticker="HO=F",
    ),
    CommodityDef(
        symbol="GASOLINE_RBOB", name="Gasoline RBOB", category="energy",
        unit="USD/gal", pk_relevance="MEDIUM",
        yf_ticker="RB=F",
    ),
)

# ── Agriculture ──────────────────────────────────────────────────────────────
_register(
    CommodityDef(
        symbol="COTTON", name="Cotton #2", category="agriculture",
        unit="USD/lb", pk_relevance="HIGH",
        yf_ticker="CT=F", yf_etf="BAL",
        fred_series="PCOTTINDUSDM",
        pk_unit="PKR/maund", pk_conversion="cotton_usd_lb_to_pkr_maund",
    ),
    CommodityDef(
        symbol="RICE", name="Rough Rice", category="agriculture",
        unit="USD/cwt", pk_relevance="HIGH",
        yf_ticker="ZR=F",
        fred_series="PRICENPQUSDM",
        pk_unit="PKR/maund", pk_conversion="rice_usd_cwt_to_pkr_maund",
    ),
    CommodityDef(
        symbol="WHEAT", name="Wheat", category="agriculture",
        unit="USD/bu", pk_relevance="HIGH",
        yf_ticker="ZW=F",
        fred_series="PWHEAMTUSDM",
        pk_unit="PKR/maund", pk_conversion="wheat_usd_bu_to_pkr_maund",
    ),
    CommodityDef(
        symbol="SUGAR", name="Sugar #11", category="agriculture",
        unit="USD/lb", pk_relevance="HIGH",
        yf_ticker="SB=F", yf_etf="SGG",
        fred_series="PSUGAISAUSDM",
        pk_unit="PKR/bori", pk_conversion="sugar_usd_lb_to_pkr_bori",
    ),
    CommodityDef(
        symbol="PALM_OIL", name="Palm Oil (CPO)", category="agriculture",
        unit="USD/ton", pk_relevance="HIGH",
        yf_ticker=None,  # Not on yfinance
        fred_series="PPOILUSDM",
    ),
    CommodityDef(
        symbol="CORN", name="Corn", category="agriculture",
        unit="USD/bu", pk_relevance="MEDIUM",
        yf_ticker="ZC=F",
        fred_series="PMAABORUSDM",
    ),
    CommodityDef(
        symbol="SOYBEANS", name="Soybeans", category="agriculture",
        unit="USD/bu", pk_relevance="MEDIUM",
        yf_ticker="ZS=F",
    ),
    CommodityDef(
        symbol="SOYBEAN_OIL", name="Soybean Oil", category="agriculture",
        unit="USD/lb", pk_relevance="MEDIUM",
        yf_ticker="ZL=F",
    ),
    CommodityDef(
        symbol="COFFEE", name="Coffee Arabica", category="agriculture",
        unit="USD/lb", pk_relevance="LOW",
        yf_ticker="KC=F",
        fred_series="PCOFFOTMUSDM",
    ),
    CommodityDef(
        symbol="COCOA", name="Cocoa", category="agriculture",
        unit="USD/ton", pk_relevance="LOW",
        yf_ticker="CC=F",
    ),
    CommodityDef(
        symbol="RUBBER", name="Rubber", category="agriculture",
        unit="USD/kg", pk_relevance="LOW",
        yf_ticker=None,
        fred_series="PRUBBSLUSDM",
    ),
)

# ── Livestock ────────────────────────────────────────────────────────────────
_register(
    CommodityDef(
        symbol="LIVE_CATTLE", name="Live Cattle", category="livestock",
        unit="USD/lb", pk_relevance="MEDIUM",
        yf_ticker="LE=F",
    ),
    CommodityDef(
        symbol="LUMBER", name="Lumber", category="livestock",
        unit="USD/mbf", pk_relevance="MEDIUM",
        yf_ticker="LBS=F",
    ),
)

# ── FX / Currency ────────────────────────────────────────────────────────────
_register(
    CommodityDef(
        symbol="USD_PKR", name="USD/PKR", category="fx",
        unit="PKR", pk_relevance="HIGH",
        yf_ticker="PKR=X",
    ),
    CommodityDef(
        symbol="EUR_USD", name="EUR/USD", category="fx",
        unit="USD", pk_relevance="MEDIUM",
        yf_ticker="EURUSD=X",
    ),
    CommodityDef(
        symbol="GBP_USD", name="GBP/USD", category="fx",
        unit="USD", pk_relevance="MEDIUM",
        yf_ticker="GBPUSD=X",
    ),
    CommodityDef(
        symbol="USD_SAR", name="USD/SAR", category="fx",
        unit="SAR", pk_relevance="HIGH",
        yf_ticker="SAR=X",
    ),
    CommodityDef(
        symbol="USD_AED", name="USD/AED", category="fx",
        unit="AED", pk_relevance="HIGH",
        yf_ticker="AED=X",
    ),
    CommodityDef(
        symbol="USD_CNY", name="USD/CNY", category="fx",
        unit="CNY", pk_relevance="MEDIUM",
        yf_ticker="CNY=X",
    ),
    CommodityDef(
        symbol="DXY", name="US Dollar Index", category="fx",
        unit="index", pk_relevance="HIGH",
        yf_ticker="DX-Y.NYB",
    ),
)


# ─────────────────────────────────────────────────────────────────────────────
# Query helpers
# ─────────────────────────────────────────────────────────────────────────────

CATEGORIES = sorted({c.category for c in COMMODITY_UNIVERSE.values()})


def get_commodity(symbol: str) -> CommodityDef | None:
    """Look up a commodity by its canonical symbol."""
    return COMMODITY_UNIVERSE.get(symbol.upper())


def get_commodities_by_category(category: str) -> list[CommodityDef]:
    """Return all commodities in a given category."""
    cat = category.lower()
    return [c for c in COMMODITY_UNIVERSE.values() if c.category == cat]


def get_pk_high_commodities() -> list[CommodityDef]:
    """Return all commodities with HIGH Pakistan relevance."""
    return [c for c in COMMODITY_UNIVERSE.values() if c.pk_relevance == "HIGH"]


def get_yfinance_tickers() -> dict[str, str]:
    """Return {symbol: yf_ticker} for all commodities with a yfinance ticker."""
    return {
        c.symbol: c.yf_ticker
        for c in COMMODITY_UNIVERSE.values()
        if c.yf_ticker
    }


def get_fred_series() -> dict[str, str]:
    """Return {symbol: fred_series_id} for all commodities with a FRED series."""
    return {
        c.symbol: c.fred_series
        for c in COMMODITY_UNIVERSE.values()
        if c.fred_series
    }
