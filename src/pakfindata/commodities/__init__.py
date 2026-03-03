"""Commodity data module for pakfindata.

Provides access to global commodity prices from free data sources:
- Tier 0A: PMEX OHLC API (official exchange OHLCV + settlement + FX, commod.db)
- Tier 0B: PMEX Margins Excel (risk/margin data, 148 contracts, commod.db)
- Tier 1: yfinance (daily OHLCV, futures contracts)
- Tier 2: FRED API (monthly benchmark prices)
- Tier 3: World Bank Pink Sheet (monthly, 1960+)
- Tier 4a: khistocks.com (7 Pakistan local market feeds, pure-requests)
- Tier 4b: PMEX Portal (direct JSON API, 134 instruments, 9 categories)
- Tier 5: GoldPriceZ (PKR/Tola gold rates)
- Tier 6: Web scrapers (SBP, Investing.com — Selenium-based)
"""

from .config import COMMODITY_UNIVERSE, CommodityDef, get_commodity, get_commodities_by_category
from .models import init_commodity_schema
from .utils import (
    gold_usd_oz_to_pkr_tola,
    cotton_usd_lb_to_pkr_maund,
    wheat_usd_bu_to_pkr_maund,
    rice_usd_cwt_to_pkr_maund,
    crude_usd_bbl_to_pkr_litre,
    sugar_usd_lb_to_pkr_bori,
)

__all__ = [
    "COMMODITY_UNIVERSE",
    "CommodityDef",
    "get_commodity",
    "get_commodities_by_category",
    "init_commodity_schema",
    "gold_usd_oz_to_pkr_tola",
    "cotton_usd_lb_to_pkr_maund",
    "wheat_usd_bu_to_pkr_maund",
    "rice_usd_cwt_to_pkr_maund",
    "crude_usd_bbl_to_pkr_litre",
    "sugar_usd_lb_to_pkr_bori",
]
