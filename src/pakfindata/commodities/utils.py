"""PKR conversion utilities for commodity prices.

All conversions take a USD price in the commodity's native unit
and a USD/PKR exchange rate, returning the PKR price in Pakistan's
local trade unit.
"""

# ─────────────────────────────────────────────────────────────────────────────
# Constants — Pakistan trade units
# ─────────────────────────────────────────────────────────────────────────────

TOLA_TO_OZ = 0.40125          # 1 Pakistan Tola = 0.40125 troy ounces
OZ_TO_TOLA = 1 / TOLA_TO_OZ  # ~2.4922 tola per troy oz

MAUND_TO_KG = 37.3242         # 1 Maund = 37.3242 kg
KG_TO_MAUND = 1 / MAUND_TO_KG

BORI_TO_KG = 100.0            # 1 Bori = 100 kg (sugar unit)
KG_TO_BORI = 1 / BORI_TO_KG

BBL_TO_LITRE = 158.987        # 1 barrel = 158.987 litres
LITRE_TO_BBL = 1 / BBL_TO_LITRE

LB_TO_KG = 0.453592           # 1 pound = 0.453592 kg
KG_TO_LB = 1 / LB_TO_KG

BU_WHEAT_TO_KG = 27.2155      # 1 bushel of wheat = 27.2155 kg
BU_RICE_TO_KG = 20.4117       # 1 bushel of rough rice ≈ 20.41 kg (USDA: 1cwt = 100lb)
CWT_TO_KG = 45.3592           # 1 hundredweight = 100 lb = 45.3592 kg


# ─────────────────────────────────────────────────────────────────────────────
# Conversion functions
# ─────────────────────────────────────────────────────────────────────────────

def gold_usd_oz_to_pkr_tola(usd_per_oz: float, usd_pkr: float) -> float:
    """Convert gold/silver price from USD/oz to PKR/tola.

    Formula: PKR/tola = (USD/oz) * TOLA_TO_OZ * usd_pkr
    One tola = 0.40125 oz, so PKR/tola = price_per_oz * 0.40125 * exchange_rate
    """
    return usd_per_oz * TOLA_TO_OZ * usd_pkr


def cotton_usd_lb_to_pkr_maund(usd_per_lb: float, usd_pkr: float) -> float:
    """Convert cotton price from USD/lb (ICE) to PKR/maund.

    1 maund = 37.3242 kg, 1 lb = 0.453592 kg
    So 1 maund = 37.3242 / 0.453592 = ~82.286 lbs
    PKR/maund = (USD/lb) * (lbs per maund) * usd_pkr
    """
    lbs_per_maund = MAUND_TO_KG / LB_TO_KG
    return usd_per_lb * lbs_per_maund * usd_pkr


def wheat_usd_bu_to_pkr_maund(usd_per_bu: float, usd_pkr: float) -> float:
    """Convert wheat price from USD/bushel (CBOT) to PKR/maund.

    1 bushel wheat = 27.2155 kg, 1 maund = 37.3242 kg
    maunds per bushel = 27.2155 / 37.3242 = ~0.7292
    PKR/maund = (USD/bu) / (maunds per bushel) * usd_pkr
    i.e. PKR/maund = (USD/bu) * (37.3242/27.2155) * usd_pkr
    """
    maund_per_bu = BU_WHEAT_TO_KG / MAUND_TO_KG
    return (usd_per_bu / maund_per_bu) * usd_pkr


def rice_usd_cwt_to_pkr_maund(usd_per_cwt: float, usd_pkr: float) -> float:
    """Convert rough rice from USD/cwt (CBOT) to PKR/maund.

    1 cwt = 100 lb = 45.3592 kg, 1 maund = 37.3242 kg
    PKR/maund = (USD/cwt) * (37.3242/45.3592) * usd_pkr
    """
    maund_per_cwt = CWT_TO_KG / MAUND_TO_KG
    return (usd_per_cwt / maund_per_cwt) * usd_pkr


def crude_usd_bbl_to_pkr_litre(usd_per_bbl: float, usd_pkr: float) -> float:
    """Convert crude oil from USD/barrel to PKR/litre.

    1 barrel = 158.987 litres
    PKR/litre = (USD/bbl) / 158.987 * usd_pkr
    """
    return (usd_per_bbl / BBL_TO_LITRE) * usd_pkr


def sugar_usd_lb_to_pkr_bori(usd_per_lb: float, usd_pkr: float) -> float:
    """Convert sugar from USD/lb (ICE Sugar #11) to PKR/bori.

    1 bori = 100 kg, 1 lb = 0.453592 kg
    So 1 bori = 100 / 0.453592 = ~220.462 lbs
    PKR/bori = (USD/lb) * (lbs per bori) * usd_pkr
    """
    lbs_per_bori = BORI_TO_KG / LB_TO_KG
    return usd_per_lb * lbs_per_bori * usd_pkr


# ─────────────────────────────────────────────────────────────────────────────
# Dispatcher — call conversion by name (used by sync orchestrator)
# ─────────────────────────────────────────────────────────────────────────────

_CONVERTERS = {
    "gold_usd_oz_to_pkr_tola": gold_usd_oz_to_pkr_tola,
    "cotton_usd_lb_to_pkr_maund": cotton_usd_lb_to_pkr_maund,
    "wheat_usd_bu_to_pkr_maund": wheat_usd_bu_to_pkr_maund,
    "rice_usd_cwt_to_pkr_maund": rice_usd_cwt_to_pkr_maund,
    "crude_usd_bbl_to_pkr_litre": crude_usd_bbl_to_pkr_litre,
    "sugar_usd_lb_to_pkr_bori": sugar_usd_lb_to_pkr_bori,
}


def convert_to_pkr(converter_name: str, usd_price: float, usd_pkr: float) -> float | None:
    """Convert a commodity price to PKR using the named converter.

    Returns None if converter_name is not recognized.
    """
    fn = _CONVERTERS.get(converter_name)
    if fn is None:
        return None
    return fn(usd_price, usd_pkr)
