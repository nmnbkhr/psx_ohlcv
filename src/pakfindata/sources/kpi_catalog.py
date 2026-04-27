"""KPI Catalog — Maps observed financial line item names to canonical KPI codes.

Built from real PSX company financial PDFs. Organized by statement type and
industry format family. Names are stored as-is from documents for fuzzy matching.

Usage:
    from pakfindata.sources.kpi_catalog import match_kpi, SECTOR_FORMAT

    kpi_code = match_kpi("Turnover - net", "pl")  # -> "revenue"
    fmt = SECTOR_FORMAT.get("0807")                # -> "BANK"
"""

from __future__ import annotations

import re

# ─────────────────────────────────────────────────────────────────────────────
# Sector → Format Family mapping (from PSX sector codes)
# ─────────────────────────────────────────────────────────────────────────────

SECTOR_FORMAT: dict[str, str] = {
    # BANK — markup-based P&L
    "0807": "BANK",   # Commercial Banks (19 cos)
    "0813": "BANK",   # Inv. Banks / Securities Cos (41 cos)
    # INSURANCE — premium/claims P&L
    "0812": "INSURANCE",  # Insurance (33 cos)
    # MODARABA — hybrid markup/margin
    "0819": "MODARABA",   # Modarabas (24 cos)
    "0815": "MODARABA",   # Leasing Companies (8 cos)
    # FUND — NAV-based, no traditional P&L
    "0806": "FUND",   # Close-End Mutual Fund (5 cos)
    "0837": "FUND",   # Exchange Traded Funds (9 cos)
    "0836": "FUND",   # Real Estate Investment Trust (5 cos)
    # Everything else → STANDARD (manufacturing, services, etc.)
}

FORMAT_FAMILIES = {"BANK", "INSURANCE", "MODARABA", "FUND", "STANDARD"}

BANK_SECTOR_CODES = {"0807", "0813"}


def get_format_family(sector_code: str | None) -> str:
    """Return format family for a sector code. Defaults to STANDARD."""
    if not sector_code:
        return "STANDARD"
    return SECTOR_FORMAT.get(sector_code, "STANDARD")


# ─────────────────────────────────────────────────────────────────────────────
# KPI Catalog — canonical_code → list of observed names (case-insensitive)
# ─────────────────────────────────────────────────────────────────────────────

PL_CATALOG: dict[str, list[str]] = {
    # ── Revenue / Top Line ──
    "revenue": [
        "Turnover - net", "Turnover-net", "Turnover – net",
        "Net sales", "Revenue", "Net revenue", "Sales",
        "Revenue from contracts", "Revenue from operations",
        "Net turnover",
    ],
    "gross_turnover": [
        "Turnover - gross", "Turnover-gross", "Turnover – gross",
        "Gross sales", "Gross revenue", "Gross turnover",
    ],
    "sales_tax": [
        "Sales tax", "Sales tax and excise duty",
        "Federal excise duty", "Sales tax and FED",
    ],
    # ── Cost of Sales / COGS ──
    "cost_of_sales": [
        "Cost of sales", "Cost of revenue", "Cost of goods sold",
        "Cost of goods manufactured and sold", "Cost of goods manufactured",
        "Cost of services rendered",
    ],
    # ── Gross Profit ──
    "gross_profit": [
        "Gross profit", "Gross (loss) / profit", "Gross loss",
        "Gross (loss)/profit", "Gross profit/(loss)",
    ],
    # ── Operating Expenses ──
    "admin_expenses": [
        "Administrative expenses", "Admin expenses",
        "Administrative and general expenses",
        "General and administrative expenses",
    ],
    "selling_expenses": [
        "Selling and distribution costs", "Selling expenses",
        "Distribution cost", "Selling and distribution expenses",
        "Marketing expenses", "Distribution expenses",
        "Selling, general and administrative expenses",
    ],
    "other_expenses": [
        "Other expenses", "Other charges", "Other operating expenses",
        "Other operating charges",
    ],
    "other_income": [
        "Other income", "Other operating income", "Other revenue",
    ],
    # ── Finance Cost ──
    "finance_cost": [
        "Finance costs", "Finance cost", "Financial charges",
        "Borrowing costs", "Interest expense", "Finance charges",
    ],
    # ── Operating Profit ──
    "operating_profit": [
        "Operating profit", "Operating (loss) / profit", "Operating loss",
        "Profit from operations", "Operating (loss)/profit",
        "Profit/(loss) from operations",
    ],
    # ── Pre-tax ──
    "levy": [
        "Levy", "Workers welfare fund",
        "Workers' profit participation fund",
        "Workers' welfare fund and WPPF",
    ],
    "pbt": [
        "Profit before taxation", "Loss before taxation",
        "(Loss) / profit before income tax",
        "(Loss) / profit before taxation",
        "Profit before levy and taxation",
        "Loss before levy and taxation",
        "(Loss)/profit before taxation",
        "Profit/(loss) before taxation",
    ],
    # ── Tax ──
    "taxation": [
        "Taxation - net", "Taxation", "Income tax expense",
        "Tax expense", "Taxation - Net", "Income tax",
        "Provision for taxation",
    ],
    # ── Profit After Tax ──
    "pat": [
        "Profit after taxation", "Loss after taxation",
        "(Loss) / profit after taxation",
        "(Loss)/profit after taxation",
        "Profit/(loss) after taxation",
        "Net profit", "Net income", "Net loss",
        "Profit for the year", "Profit for the period",
        "Loss for the year", "Loss for the period",
        "(Loss)/profit for the period",
    ],
    # ── EPS ──
    "eps_basic": [
        "Basic and diluted - Rupees", "Basic - Rupees",
        "Earnings per share", "Basic and diluted",
        "Loss per share", "Basic earnings per share",
        "(Loss)/earnings per share",
        "Basic and diluted earnings per share",
    ],
    "eps_diluted": [
        "Diluted - Rupees", "Diluted earnings per share",
        "Diluted",
    ],
    # ── Comprehensive Income ──
    "total_comprehensive": [
        "Total comprehensive income for the year",
        "Total comprehensive income for the period",
        "Total comprehensive loss for the period",
        "Total comprehensive loss for the year",
        "Total comprehensive income/(loss)",
    ],
    # ── Bank-Specific P&L ──
    "markup_earned": [
        "Mark-up / return / interest earned",
        "Markup earned", "Interest / markup earned",
        "Mark-up/return/interest earned",
        "Interest earned", "Return earned",
        "Mark-up income", "Markup income",
        "Interest income", "Interest / markup income",
    ],
    "markup_expensed": [
        "Mark-up / return / interest expensed",
        "Markup expensed", "Interest / markup expensed",
        "Mark-up/return/interest expensed",
        "Interest expensed", "Return expensed",
    ],
    "net_markup": [
        "Net mark-up / interest income",
        "Net markup income", "Net interest income",
        "Net mark-up/interest income after provisions",
        "Net mark-up / interest income after provisions",
    ],
    "provisions": [
        "Provision against advances",
        "Provision for diminution",
        "Provision and write offs",
        "Provision against loans and advances",
        "Credit loss expense",
        "Expected credit losses",
    ],
    "non_markup_income": [
        "Non mark-up / interest income",
        "Non-markup/interest income",
        "Fee and commission income",
        "Fee, commission and brokerage income",
    ],
    "total_income": [
        "Total income", "Total Income",
        "Gross income", "Total revenue",
    ],
    "remuneration_funds": [
        "Remuneration from funds under management",
        "Remuneration from funds under management - net",
        "Remuneration from fund under management",
        "Remuneration from fund under management - net",
    ],
    "income_debt_securities": [
        "Income on debt securities",
        "Income on Pakistan investment bonds",
        "Income on Pakistan investment bond",
        "Income on term finance certificates",
        "Income on TFC's", "Income on TFCs",
        "Income on term deposit receipt",
    ],
    # ── Insurance-Specific ──
    "premium_income": [
        "Net premium revenue", "Premium income",
        "Gross premium", "Net premium earned",
        "Premium / contribution revenue",
        "Premium revenue",
    ],
    "claims_expense": [
        "Net claims", "Claims expense", "Net claims incurred",
        "Net claims / benefits",
    ],
    "underwriting_result": [
        "Underwriting result", "Underwriting profit",
        "Underwriting loss", "Underwriting results",
    ],
    "investment_income": [
        "Investment income", "Income from investments",
        "Return on investments", "Dividend income",
        "Net investment income",
    ],
    "net_commission": [
        "Net commission", "Commission income",
        "Commission expense", "Net commission income",
    ],
    # ── Investment Co / Modaraba / Leasing ──
    "management_fee": [
        "Management fee", "Management fee income",
        "Advisory fee", "Fund management fee",
    ],
    "modaraba_income": [
        "Modaraba income", "Income from modaraba business",
        "Return on modaraba business",
    ],
    "lease_income": [
        "Lease rental income", "Income from leasing",
        "Ijarah rentals", "Finance lease income",
    ],
    "capital_gains": [
        "Capital gain on sale of investments",
        "Net capital gain", "Gain on sale of investments",
        "Realized gain on investments",
    ],
    "unrealized_gains": [
        "Unrealized gain on investments",
        "Net unrealised gain", "Unrealized appreciation",
        "Fair value gains on financial assets",
        "Net realised fair value gains",
    ],
    "rental_income": [
        "Rental income", "Net rental income",
        "Property rental income",
    ],
    # ── REIT-Specific ──
    "rental_revenue": [
        "Rental revenue", "Rental income from properties",
        "Gross rental income",
    ],
    "property_income": [
        "Property income", "Net property income",
    ],
    "nav_per_unit": [
        "NAV per unit", "Net asset value per unit",
        "Net asset value per share",
    ],
    # ── Oil & Gas Specific ──
    "royalty": [
        "Royalty", "Government royalty",
    ],
    "exploration_expense": [
        "Exploration and prospecting expenditure",
        "Exploration costs", "Exploration expense",
    ],
    # ── Common Variations ──
    "direct_cost": [
        "Direct cost", "Direct costs", "Manufacturing cost",
    ],
    "distribution_cost": [
        "Distribution cost", "Distribution and marketing costs",
        "Distribution expenses",
    ],
    "net_insurance_premium": [
        "Net Insurance Premium", "Net insurance premium revenue",
    ],
    "net_insurance_claims": [
        "Net Insurance Claims", "Net insurance claims expense",
    ],
    "finance_income": [
        "Finance income", "Financial income",
        "Finance income / (cost) - net",
    ],
    "share_of_profit_associate": [
        "Share of Profit of Associated Companies",
        "Share of profit of associates",
        "Share of profit from associate",
    ],
    "levies": [
        "Levies", "WPPF and WWF",
    ],
    "loss_before_levies": [
        "(Loss) before levies and tax",
        "Profit before levies and taxation",
    ],
    "trading_profit": [
        "Trading profit", "Trading profit from sale of sugar",
    ],
    "dividend_income": [
        "Dividend - net of zakat", "Dividend income",
        "Dividend income - net",
    ],
    "fx_income": [
        "Foreign exchange income", "Exchange gain",
        "Foreign exchange gain", "Exchange income",
    ],
    "provision_for_tax": [
        "Provision for taxation", "Provision for tax",
    ],
    "finance_lease_income": [
        "Finance leases", "Finance lease income",
        "Net investment in finance leases",
    ],
    "eps_basic_diluted": [
        "Earning per share - basic & dilutive",
        "Earnings per share - basic and diluted",
    ],
    # ── Share Information ──
    "shares_issued": [
        "Number of shares", "Number of ordinary shares",
        "Ordinary shares issued", "Shares issued",
        "Number of shares issued",
        "Total number of shares",
    ],
    "shares_outstanding": [
        "Shares outstanding", "Number of shares outstanding",
        "Outstanding shares",
    ],
    "weighted_avg_shares": [
        "Weighted average number of shares",
        "Weighted average shares",
        "Weighted average number of ordinary shares",
        "Weighted average outstanding shares",
    ],
    "face_value": [
        "Face value per share", "Par value per share",
        "Face value", "Nominal value",
    ],
    # ── Per-Share Metrics (multi-year summary) ──
    "book_value_per_share": [
        "Break-up Value per Share", "Book value per share",
        "Book Value", "Net asset value per share",
        "Break up value per share",
    ],
    "dps": [
        "Cash Dividend per Share", "Dividend per share",
        "DPS", "Cash dividend per share",
        "Cumulative Dividends / share",
    ],
    # ── Return Ratios ──
    "pe_ratio": [
        "Price Earning Ratio", "P/E Ratio",
        "Price earnings ratio", "PE Ratio",
        "Price to earnings ratio",
    ],
    "return_on_equity": [
        "Return on Average Capital Employed",
        "Return on Equity", "Return on equity",
        "ROE", "Return on shareholders equity",
    ],
    "return_on_assets": [
        "Return on Assets", "Return on assets", "ROA",
    ],
    "total_shareholder_return": [
        "Total Shareholder Return", "Total shareholder return",
        "TSR",
    ],
}

BS_CATALOG: dict[str, list[str]] = {
    # ── Totals ──
    "total_assets": ["Total Assets", "Total assets"],
    "total_liabilities": [
        "Total Liabilities", "Total liabilities",
        "Total Capital and Liabilities",
        "Total Equity and Liabilities",
        "Total equity and liabilities",
    ],
    "total_equity": [
        "Total Shareholders' Equity",
        "Total shareholders' equity",
        "Total Equity", "Net Assets",
        "Total equity",
    ],
    # ── Non-Current Assets ──
    "ppe": [
        "Property, plant and equipment",
        "Fixed assets", "Operating fixed assets",
        "Property and equipment",
    ],
    "intangibles": [
        "Intangible asset", "Intangible assets", "Goodwill",
        "Goodwill and intangible assets",
    ],
    "non_current_assets": [
        "Non-Current Assets", "Non-current assets",
        "Total Non-Current Assets",
    ],
    "lt_investments": [
        "Long term investments", "Long-term investments",
        "Investment in subsidiaries",
    ],
    "lt_deposits": [
        "Long term deposits and receivable",
        "Long term deposits", "Long-term deposits",
    ],
    # ── Current Assets ──
    "current_assets": [
        "Current Assets", "Total Current Assets",
        "Total current assets",
    ],
    "inventory": [
        "Stock-in-trade", "Inventories", "Inventory",
    ],
    "stores_spares": [
        "Stores, spare parts and loose tools",
        "Stores and spares", "Stores, spares and loose tools",
    ],
    "trade_receivables": [
        "Trade and other receivables", "Trade debts",
        "Trade receivables", "Sundry debtors",
    ],
    "loans_advances": [
        "Loans and advances", "Short term loans and advances",
        "Advances, deposits, prepayments and other receivables",
    ],
    "cash": [
        "Cash and bank balances", "Cash and cash equivalents",
        "Cash at bank", "Bank balances",
    ],
    "tax_refunds": [
        "Tax refunds due from Government",
        "Taxes recoverable", "Advance tax",
    ],
    # ── Non-Current Liabilities ──
    "non_current_liabilities": [
        "Non-Current Liabilities", "Non-current liabilities",
        "Total Non-Current Liabilities",
    ],
    "borrowings_lt": [
        "Long term borrowings", "Long-term financing",
        "Long-term borrowings", "Long term financing",
    ],
    "deferred_tax": [
        "Deferred tax liability", "Deferred taxation",
        "Deferred tax asset", "Deferred income tax",
    ],
    "lease_liabilities": [
        "Lease liabilities", "Lease liability",
    ],
    # ── Current Liabilities ──
    "current_liabilities": [
        "Current Liabilities", "Total Current Liabilities",
        "Total current liabilities",
    ],
    "trade_payables": [
        "Trade and other payables", "Trade creditors",
        "Trade payables", "Sundry creditors",
    ],
    "borrowings_st": [
        "Short term borrowings", "Short-term borrowings",
        "Running finance", "Short term borrowings - on demand",
    ],
    "accrued_markup": [
        "Accrued markup", "Accrued mark-up",
        "Accrued interest", "Accrued markup - on demand",
    ],
    # ── Insurance / Investment Co BS ──
    "investment_property": [
        "Investment property", "Investment properties",
    ],
    "investments": [
        "Investments", "Available for sale investments",
        "Financial assets", "Short term investments",
        "Investment in securities",
    ],
    "insurance_liabilities": [
        "Insurance contract liabilities",
        "Outstanding claims", "Unearned premium",
        "Premium received in advance",
    ],
    "rou_assets": [
        "Right of use assets", "Right-of-use assets",
    ],
    "deposits": [
        "Deposits", "Deposits and other accounts",
        "Customer deposits",
    ],
    # ── Bank BS ──
    "advances": [
        "Advances", "Advances - net of provisions",
        "Loans and advances to customers",
    ],
    "bills_payable": [
        "Bills payable", "Bills payables",
    ],
    "borrowings_bank": [
        "Borrowings", "Borrowings from financial institutions",
    ],
    "lt_loans": [
        "Long-term loans", "Long term loan",
        "Long term finances - secured",
    ],
    "deferred_liabilities": [
        "Deferred liabilities", "DEFERRED LIABILITIES",
        "Deferred income",
    ],
    "stock_in_trade": [
        "Stock in trade", "Stock-in-trade",
        "Stock- in-trade",
    ],
    "trade_debts": [
        "Trade debtors", "Trade debts",
        "Trade debtors - unsecured",
    ],
    "unappropriated_profit": [
        "Un-appropriated profits", "Unappropriated profit",
        "Unappropriated profits",
    ],
    "advance_tax": [
        "Advance Income Tax", "Advance tax",
        "Taxes recoverable",
    ],
    "unclaimed_dividend": [
        "Unclaimed dividend", "Unclaimed dividends",
    ],
    "non_controlling_interest": [
        "Non-controlling interest", "Non controlling interest",
        "Minority interest",
    ],
    "lt_deposits_bs": [
        "Long term deposits", "Long-term deposits",
        "Long term security deposits",
    ],
    # ── Equity Components ──
    "share_capital": [
        "Issued, subscribed and paid up capital",
        "Share capital", "Paid-up capital",
        "Issued, subscribed and paid-up capital",
    ],
    "reserves": [
        "Unappropriated profit", "Accumulated profit",
        "Accumulated (loss) / profit", "Accumulated losses",
        "Revenue reserves", "Revenue reserve",
        "Retained earnings",
    ],
    "share_premium": [
        "Share premium", "Premium on issue of shares",
    ],
    "revaluation_surplus": [
        "Surplus on revaluation of fixed assets",
        "Surplus on revaluation of fixed assets - net",
        "Revaluation surplus",
    ],
}

# ── Multi-Year Summary / Ratio KPIs ──
RATIO_CATALOG: dict[str, list[str]] = {
    "gross_profit_margin": ["Gross Profit Margin", "Gross profit margin"],
    "operating_profit_margin": ["Operating Profit Margin", "Operating profit margin"],
    "net_profit_margin": ["Net Profit Margin", "Net profit margin"],
    "ebitda_margin": ["EBITDA Margin to Sales", "EBITDA Margin", "EBITDA margin"],
    "roe": [
        "Return on Average Capital Employed", "Return on Equity",
        "Return on equity", "ROE",
    ],
    "current_ratio": ["Current Ratio", "Current ratio"],
    "quick_ratio": ["Acid Test/Quick Ratio", "Quick Ratio", "Quick ratio"],
    "debt_equity": ["Debt to Equity Ratio", "Debt/Equity", "Debt to equity"],
    "eps": ["Earnings per Share", "Earnings Per Share", "EPS"],
    "pe_ratio": ["Price Earning Ratio", "P/E Ratio", "Price earnings ratio"],
    "pb_ratio": ["Price to Book Ratio", "P/B Ratio", "Price to book ratio"],
    "dividend_yield": ["Dividend Yield Ratio", "Dividend Yield", "Dividend yield"],
    "dividend_payout": ["Dividend Payout Ratio", "Dividend payout ratio"],
    "dps": ["Cash Dividend per Share", "Dividend per share", "DPS"],
    "book_value": ["Break-up Value per Share", "Book value per share", "Book Value"],
    "market_cap": ["Market Capitalization", "Market capitalization", "Market cap"],
    "enterprise_value": ["Enterprise Value", "Enterprise value"],
    "market_price_high": ["High during the Year", "52 week high", "Market price - high"],
    "market_price_low": ["Low during the Year", "52 week low", "Market price - low"],
    "market_price_close": ["As on June 30", "Market Value per Share", "Closing price"],
    "total_shareholder_return": ["Total Shareholder Return", "Total shareholder return", "TSR"],
    "shares_issued": ["Number of shares", "Number of ordinary shares", "Shares issued", "Total number of shares"],
    "weighted_avg_shares": ["Weighted average number of shares", "Weighted average shares"],
    "face_value": ["Face value per share", "Par value per share"],
    "return_on_assets": ["Return on Assets", "Return on assets", "ROA"],
}


# ─────────────────────────────────────────────────────────────────────────────
# Matching Logic
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_for_match(text: str) -> str:
    """Normalize text for fuzzy matching: lowercase, strip special chars."""
    t = text.lower().strip()
    # Remove note references like "24", "25" at start
    t = re.sub(r"^\d+\s+", "", t)
    # Normalize whitespace
    t = re.sub(r"\s+", " ", t)
    # Remove trailing dashes/underscores
    t = t.rstrip("-_. ")
    return t


def match_kpi(line_text: str, statement_type: str = "pl") -> str | None:
    """Match a line item name to a canonical KPI code.

    Args:
        line_text: The original line item text from the PDF
        statement_type: "pl", "bs", or "ratio"

    Returns:
        Canonical KPI code (e.g., "revenue", "total_assets") or None if no match.
    """
    catalog = {
        "pl": PL_CATALOG,
        "bs": BS_CATALOG,
        "ratio": RATIO_CATALOG,
    }.get(statement_type, PL_CATALOG)

    normalized = _normalize_for_match(line_text)
    if not normalized or len(normalized) < 3:
        return None

    # Exact match first (case-insensitive)
    for kpi_code, names in catalog.items():
        for name in names:
            if _normalize_for_match(name) == normalized:
                return kpi_code

    # Contains match (for variations like "Gross (loss) / profit")
    for kpi_code, names in catalog.items():
        for name in names:
            norm_name = _normalize_for_match(name)
            if norm_name in normalized or normalized in norm_name:
                return kpi_code

    return None


def get_all_kpi_codes(statement_type: str = "pl") -> list[str]:
    """Return all canonical KPI codes for a statement type."""
    catalog = {"pl": PL_CATALOG, "bs": BS_CATALOG, "ratio": RATIO_CATALOG}
    return list(catalog.get(statement_type, {}).keys())
