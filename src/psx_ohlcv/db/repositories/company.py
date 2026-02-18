"""Company information and financials repository."""

import hashlib
import json
import sqlite3

import pandas as pd

from psx_ohlcv.models import now_iso


# =============================================================================
# Company Profile Functions
# =============================================================================


def upsert_company_profile(con: sqlite3.Connection, profile: dict) -> int:
    """
    Upsert company profile data.

    Args:
        con: Database connection
        profile: Dict with keys matching company_profile columns.
                Required: symbol, source_url

    Returns:
        Number of rows affected (1 for insert/update, 0 if no change)
    """
    symbol = profile.get("symbol")
    if not symbol:
        raise ValueError("profile must include 'symbol'")

    source_url = profile.get("source_url", "")
    now = now_iso()

    cur = con.execute(
        """
        INSERT INTO company_profile (
            symbol, company_name, sector_name, business_description,
            address, website, registrar, auditor, fiscal_year_end,
            updated_at, source_url
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol) DO UPDATE SET
            company_name = excluded.company_name,
            sector_name = excluded.sector_name,
            business_description = excluded.business_description,
            address = excluded.address,
            website = excluded.website,
            registrar = excluded.registrar,
            auditor = excluded.auditor,
            fiscal_year_end = excluded.fiscal_year_end,
            updated_at = excluded.updated_at,
            source_url = excluded.source_url
        """,
        (
            symbol.upper(),
            profile.get("company_name"),
            profile.get("sector_name"),
            profile.get("business_description"),
            profile.get("address"),
            profile.get("website"),
            profile.get("registrar"),
            profile.get("auditor"),
            profile.get("fiscal_year_end"),
            now,
            source_url,
        ),
    )
    con.commit()
    return cur.rowcount


def get_company_profile(con: sqlite3.Connection, symbol: str) -> dict | None:
    """
    Get company profile for a symbol.

    Args:
        con: Database connection
        symbol: Stock symbol

    Returns:
        Dict with profile data or None if not found
    """
    cur = con.execute(
        """
        SELECT symbol, company_name, sector_name, business_description,
               address, website, registrar, auditor, fiscal_year_end,
               updated_at, source_url
        FROM company_profile
        WHERE symbol = ?
        """,
        (symbol.upper(),),
    )
    row = cur.fetchone()
    return dict(row) if row else None


# =============================================================================
# Company Key People Functions
# =============================================================================


def replace_company_key_people(
    con: sqlite3.Connection, symbol: str, key_people: list[dict]
) -> int:
    """
    Replace key people for a company (delete old, insert new).

    Args:
        con: Database connection
        symbol: Stock symbol
        key_people: List of dicts with 'role' and 'name' keys

    Returns:
        Number of rows inserted
    """
    symbol = symbol.upper()
    now = now_iso()

    # Delete existing key people for this symbol
    con.execute("DELETE FROM company_key_people WHERE symbol = ?", (symbol,))

    # Insert new key people
    count = 0
    for person in key_people:
        role = person.get("role", "").strip()
        name = person.get("name", "").strip()
        if role and name:
            con.execute(
                """
                INSERT INTO company_key_people (symbol, role, name, updated_at)
                VALUES (?, ?, ?, ?)
                """,
                (symbol, role, name, now),
            )
            count += 1

    con.commit()
    return count


def get_company_key_people(con: sqlite3.Connection, symbol: str) -> list[dict]:
    """
    Get key people for a company.

    Args:
        con: Database connection
        symbol: Stock symbol

    Returns:
        List of dicts with 'role' and 'name' keys
    """
    cur = con.execute(
        """
        SELECT role, name FROM company_key_people
        WHERE symbol = ?
        ORDER BY role
        """,
        (symbol.upper(),),
    )
    return [{"role": row["role"], "name": row["name"]} for row in cur.fetchall()]


# =============================================================================
# Quote Snapshot Functions
# =============================================================================


def insert_quote_snapshot(
    con: sqlite3.Connection, symbol: str, ts: str, quote: dict
) -> bool:
    """
    Insert a quote snapshot. Does not overwrite if same ts exists.

    Args:
        con: Database connection
        symbol: Stock symbol
        ts: Ingestion timestamp (ISO format)
        quote: Dict with quote data including 'raw_hash'

    Returns:
        True if inserted, False if skipped (duplicate ts)
    """
    symbol = symbol.upper()
    raw_hash = quote.get("raw_hash", "")
    now = now_iso()

    try:
        con.execute(
            """
            INSERT INTO company_quote_snapshots (
                symbol, ts, as_of, price, change, change_pct,
                open, high, low, volume,
                day_range_low, day_range_high,
                wk52_low, wk52_high,
                circuit_low, circuit_high,
                market_mode, raw_hash, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                symbol,
                ts,
                quote.get("as_of"),
                quote.get("price"),
                quote.get("change"),
                quote.get("change_pct"),
                quote.get("open"),
                quote.get("high"),
                quote.get("low"),
                quote.get("volume"),
                quote.get("day_range_low"),
                quote.get("day_range_high"),
                quote.get("wk52_low"),
                quote.get("wk52_high"),
                quote.get("circuit_low"),
                quote.get("circuit_high"),
                quote.get("market_mode"),
                raw_hash,
                now,
            ),
        )
        con.commit()
        return True
    except sqlite3.IntegrityError:
        # Duplicate ts for this symbol, skip
        return False


def get_last_quote_hash(con: sqlite3.Connection, symbol: str) -> str | None:
    """
    Get the raw_hash of the most recent quote snapshot for a symbol.

    Args:
        con: Database connection
        symbol: Stock symbol

    Returns:
        raw_hash string or None if no snapshots exist
    """
    cur = con.execute(
        """
        SELECT raw_hash FROM company_quote_snapshots
        WHERE symbol = ?
        ORDER BY ts DESC
        LIMIT 1
        """,
        (symbol.upper(),),
    )
    row = cur.fetchone()
    return row[0] if row else None


def get_quote_snapshots(
    con: sqlite3.Connection,
    symbol: str,
    limit: int = 100,
) -> pd.DataFrame:
    """
    Get recent quote snapshots for a symbol.

    Args:
        con: Database connection
        symbol: Stock symbol
        limit: Maximum rows to return

    Returns:
        DataFrame with quote snapshot data, sorted by ts desc
    """
    query = """
        SELECT symbol, ts, as_of, price, change, change_pct,
               open, high, low, volume,
               day_range_low, day_range_high,
               wk52_low, wk52_high,
               circuit_low, circuit_high,
               market_mode, raw_hash, ingested_at
        FROM company_quote_snapshots
        WHERE symbol = ?
        ORDER BY ts DESC
        LIMIT ?
    """
    return pd.read_sql_query(query, con, params=[symbol.upper(), limit])


def get_all_latest_quotes(
    con: sqlite3.Connection,
    bond_type: str | None = None,
) -> list[dict]:
    """Get latest quotes for all bonds with optional type filter."""
    query = """
        SELECT bq.*, bm.symbol, bm.issuer, bm.bond_type, bm.coupon_rate,
               bm.maturity_date, bm.is_islamic
        FROM bond_quotes bq
        JOIN bonds_master bm ON bq.bond_id = bm.bond_id
        WHERE bq.date = (
            SELECT MAX(date) FROM bond_quotes WHERE bond_id = bq.bond_id
        )
        AND bm.is_active = 1
    """
    params = []

    if bond_type:
        query += " AND bm.bond_type = ?"
        params.append(bond_type)

    query += " ORDER BY bm.maturity_date ASC"

    try:
        cur = con.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


# =============================================================================
# Company Fundamentals Functions
# =============================================================================


def upsert_company_fundamentals(
    con: sqlite3.Connection,
    symbol: str,
    data: dict,
    save_history: bool = True,
) -> dict:
    """
    Upsert company fundamentals and optionally save to history.

    Args:
        con: Database connection
        symbol: Stock symbol
        data: Dict with all fundamentals fields
        save_history: If True, also insert into history table

    Returns:
        Dict with 'updated' and 'history_saved' status
    """
    symbol = symbol.upper()
    now = now_iso()
    today = now[:10]  # YYYY-MM-DD

    # Build the upsert for company_fundamentals
    fields = [
        "symbol", "company_name", "sector_name",
        "price", "change", "change_pct", "open", "high", "low", "volume", "ldcp",
        "bid_price", "bid_size", "ask_price", "ask_size",
        "day_range_low", "day_range_high", "wk52_low", "wk52_high",
        "circuit_low", "circuit_high",
        "ytd_change_pct", "one_year_change_pct",
        "pe_ratio", "market_cap",
        "total_shares", "free_float_shares", "free_float_pct",
        "haircut", "variance",
        "business_description", "address", "website", "registrar", "auditor",
        "fiscal_year_end", "incorporation_date", "listed_in",
        "as_of", "market_mode", "source_url", "updated_at",
    ]

    values = [
        symbol,
        data.get("company_name"),
        data.get("sector_name"),
        data.get("price"),
        data.get("change"),
        data.get("change_pct"),
        data.get("open"),
        data.get("high"),
        data.get("low"),
        data.get("volume"),
        data.get("ldcp"),
        data.get("bid_price"),
        data.get("bid_size"),
        data.get("ask_price"),
        data.get("ask_size"),
        data.get("day_range_low"),
        data.get("day_range_high"),
        data.get("wk52_low"),
        data.get("wk52_high"),
        data.get("circuit_low"),
        data.get("circuit_high"),
        data.get("ytd_change_pct"),
        data.get("one_year_change_pct"),
        data.get("pe_ratio"),
        data.get("market_cap"),
        data.get("total_shares"),
        data.get("free_float_shares"),
        data.get("free_float_pct"),
        data.get("haircut"),
        data.get("variance"),
        data.get("business_description"),
        data.get("address"),
        data.get("website"),
        data.get("registrar"),
        data.get("auditor"),
        data.get("fiscal_year_end"),
        data.get("incorporation_date"),
        data.get("listed_in"),
        data.get("as_of"),
        data.get("market_mode"),
        data.get("source_url"),
        now,
    ]

    placeholders = ", ".join(["?"] * len(fields))
    field_names = ", ".join(fields)

    # Build ON CONFLICT update clause (exclude symbol)
    update_parts = [f"{f} = excluded.{f}" for f in fields if f != "symbol"]
    update_clause = ", ".join(update_parts)

    con.execute(
        f"""
        INSERT INTO company_fundamentals ({field_names})
        VALUES ({placeholders})
        ON CONFLICT(symbol) DO UPDATE SET {update_clause}
        """,
        values,
    )
    con.commit()

    result = {"updated": True, "history_saved": False}

    # Save to history if requested
    if save_history:
        result["history_saved"] = save_fundamentals_history(con, symbol, today, data)

    return result


def save_fundamentals_history(
    con: sqlite3.Connection,
    symbol: str,
    date: str,
    data: dict,
) -> bool:
    """
    Save fundamentals snapshot to history table.

    Args:
        con: Database connection
        symbol: Stock symbol
        date: Date string (YYYY-MM-DD)
        data: Dict with fundamentals fields

    Returns:
        True if inserted, False if already exists for that date
    """
    symbol = symbol.upper()
    now = now_iso()

    # Check if we already have a record for this symbol+date
    cur = con.execute(
        "SELECT 1 FROM company_fundamentals_history WHERE symbol = ? AND date = ?",
        (symbol, date),
    )
    if cur.fetchone():
        # Update existing record
        con.execute(
            """
            UPDATE company_fundamentals_history SET
                company_name = ?, sector_name = ?,
                price = ?, change = ?, change_pct = ?,
                open = ?, high = ?, low = ?, volume = ?, ldcp = ?,
                bid_price = ?, bid_size = ?, ask_price = ?, ask_size = ?,
                day_range_low = ?, day_range_high = ?,
                wk52_low = ?, wk52_high = ?,
                circuit_low = ?, circuit_high = ?,
                ytd_change_pct = ?, one_year_change_pct = ?,
                pe_ratio = ?, market_cap = ?,
                total_shares = ?, free_float_shares = ?, free_float_pct = ?,
                haircut = ?, variance = ?,
                as_of = ?, market_mode = ?, snapshot_ts = ?
            WHERE symbol = ? AND date = ?
            """,
            (
                data.get("company_name"), data.get("sector_name"),
                data.get("price"), data.get("change"), data.get("change_pct"),
                data.get("open"), data.get("high"), data.get("low"),
                data.get("volume"), data.get("ldcp"),
                data.get("bid_price"), data.get("bid_size"),
                data.get("ask_price"), data.get("ask_size"),
                data.get("day_range_low"), data.get("day_range_high"),
                data.get("wk52_low"), data.get("wk52_high"),
                data.get("circuit_low"), data.get("circuit_high"),
                data.get("ytd_change_pct"), data.get("one_year_change_pct"),
                data.get("pe_ratio"), data.get("market_cap"),
                data.get("total_shares"), data.get("free_float_shares"),
                data.get("free_float_pct"),
                data.get("haircut"), data.get("variance"),
                data.get("as_of"), data.get("market_mode"), now,
                symbol, date,
            ),
        )
        con.commit()
        return True

    # Insert new record
    con.execute(
        """
        INSERT INTO company_fundamentals_history (
            symbol, date, company_name, sector_name,
            price, change, change_pct, open, high, low, volume, ldcp,
            bid_price, bid_size, ask_price, ask_size,
            day_range_low, day_range_high, wk52_low, wk52_high,
            circuit_low, circuit_high,
            ytd_change_pct, one_year_change_pct,
            pe_ratio, market_cap,
            total_shares, free_float_shares, free_float_pct,
            haircut, variance,
            as_of, market_mode, snapshot_ts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            symbol, date,
            data.get("company_name"), data.get("sector_name"),
            data.get("price"), data.get("change"), data.get("change_pct"),
            data.get("open"), data.get("high"), data.get("low"),
            data.get("volume"), data.get("ldcp"),
            data.get("bid_price"), data.get("bid_size"),
            data.get("ask_price"), data.get("ask_size"),
            data.get("day_range_low"), data.get("day_range_high"),
            data.get("wk52_low"), data.get("wk52_high"),
            data.get("circuit_low"), data.get("circuit_high"),
            data.get("ytd_change_pct"), data.get("one_year_change_pct"),
            data.get("pe_ratio"), data.get("market_cap"),
            data.get("total_shares"), data.get("free_float_shares"),
            data.get("free_float_pct"),
            data.get("haircut"), data.get("variance"),
            data.get("as_of"), data.get("market_mode"), now,
        ),
    )
    con.commit()
    return True


def get_company_fundamentals(con: sqlite3.Connection, symbol: str) -> dict | None:
    """
    Get latest company fundamentals.

    Args:
        con: Database connection
        symbol: Stock symbol

    Returns:
        Dict with all fundamentals fields, or None if not found
    """
    cur = con.execute(
        """
        SELECT symbol, company_name, sector_name,
               price, change, change_pct, open, high, low, volume, ldcp,
               bid_price, bid_size, ask_price, ask_size,
               day_range_low, day_range_high, wk52_low, wk52_high,
               circuit_low, circuit_high,
               ytd_change_pct, one_year_change_pct,
               pe_ratio, market_cap,
               total_shares, free_float_shares, free_float_pct,
               haircut, variance,
               business_description, address, website, registrar, auditor,
               fiscal_year_end, incorporation_date, listed_in,
               as_of, market_mode, source_url, updated_at
        FROM company_fundamentals
        WHERE symbol = ?
        """,
        (symbol.upper(),),
    )
    row = cur.fetchone()
    if row is None:
        return None

    return {
        "symbol": row[0],
        "company_name": row[1],
        "sector_name": row[2],
        "price": row[3],
        "change": row[4],
        "change_pct": row[5],
        "open": row[6],
        "high": row[7],
        "low": row[8],
        "volume": row[9],
        "ldcp": row[10],
        "bid_price": row[11],
        "bid_size": row[12],
        "ask_price": row[13],
        "ask_size": row[14],
        "day_range_low": row[15],
        "day_range_high": row[16],
        "wk52_low": row[17],
        "wk52_high": row[18],
        "circuit_low": row[19],
        "circuit_high": row[20],
        "ytd_change_pct": row[21],
        "one_year_change_pct": row[22],
        "pe_ratio": row[23],
        "market_cap": row[24],
        "total_shares": row[25],
        "free_float_shares": row[26],
        "free_float_pct": row[27],
        "haircut": row[28],
        "variance": row[29],
        "business_description": row[30],
        "address": row[31],
        "website": row[32],
        "registrar": row[33],
        "auditor": row[34],
        "fiscal_year_end": row[35],
        "incorporation_date": row[36],
        "listed_in": row[37],
        "as_of": row[38],
        "market_mode": row[39],
        "source_url": row[40],
        "updated_at": row[41],
    }


# =============================================================================
# Company Financials Functions
# =============================================================================


def upsert_company_financials(
    con: sqlite3.Connection,
    symbol: str,
    financials: list[dict],
) -> int:
    """
    Upsert company financial data (annual/quarterly).

    Args:
        con: Database connection
        symbol: Stock symbol
        financials: List of dicts with period data

    Returns:
        Number of rows upserted
    """
    if not financials:
        return 0

    symbol = symbol.upper()
    now = now_iso()
    count = 0

    for item in financials:
        period_end = item.get("period_end")
        period_type = item.get("period_type", "annual")

        if not period_end:
            continue

        cur = con.execute(
            """
            INSERT INTO company_financials (
                symbol, period_end, period_type,
                sales, gross_profit, operating_profit,
                profit_before_tax, profit_after_tax, eps,
                markup_earned, markup_expensed,
                total_assets, total_liabilities, total_equity,
                cost_of_sales, operating_expenses, finance_cost,
                other_income, taxation,
                net_interest_income, non_markup_income, total_income, provisions,
                current_assets, non_current_assets,
                current_liabilities, non_current_liabilities,
                cash_and_equivalents, share_capital,
                source, currency_scale,
                currency, updated_at
            ) VALUES (
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?, ?, ?,
                ?, ?,
                ?, ?,
                ?, ?,
                ?, ?,
                ?, ?
            )
            ON CONFLICT(symbol, period_end, period_type) DO UPDATE SET
                sales = COALESCE(excluded.sales, company_financials.sales),
                gross_profit = COALESCE(excluded.gross_profit, company_financials.gross_profit),
                operating_profit = COALESCE(excluded.operating_profit, company_financials.operating_profit),
                profit_before_tax = COALESCE(excluded.profit_before_tax, company_financials.profit_before_tax),
                profit_after_tax = COALESCE(excluded.profit_after_tax, company_financials.profit_after_tax),
                eps = COALESCE(excluded.eps, company_financials.eps),
                markup_earned = COALESCE(excluded.markup_earned, company_financials.markup_earned),
                markup_expensed = COALESCE(excluded.markup_expensed, company_financials.markup_expensed),
                total_assets = COALESCE(excluded.total_assets, company_financials.total_assets),
                total_liabilities = COALESCE(excluded.total_liabilities, company_financials.total_liabilities),
                total_equity = COALESCE(excluded.total_equity, company_financials.total_equity),
                cost_of_sales = COALESCE(excluded.cost_of_sales, company_financials.cost_of_sales),
                operating_expenses = COALESCE(excluded.operating_expenses, company_financials.operating_expenses),
                finance_cost = COALESCE(excluded.finance_cost, company_financials.finance_cost),
                other_income = COALESCE(excluded.other_income, company_financials.other_income),
                taxation = COALESCE(excluded.taxation, company_financials.taxation),
                net_interest_income = COALESCE(excluded.net_interest_income, company_financials.net_interest_income),
                non_markup_income = COALESCE(excluded.non_markup_income, company_financials.non_markup_income),
                total_income = COALESCE(excluded.total_income, company_financials.total_income),
                provisions = COALESCE(excluded.provisions, company_financials.provisions),
                current_assets = COALESCE(excluded.current_assets, company_financials.current_assets),
                non_current_assets = COALESCE(excluded.non_current_assets, company_financials.non_current_assets),
                current_liabilities = COALESCE(excluded.current_liabilities, company_financials.current_liabilities),
                non_current_liabilities = COALESCE(excluded.non_current_liabilities, company_financials.non_current_liabilities),
                cash_and_equivalents = COALESCE(excluded.cash_and_equivalents, company_financials.cash_and_equivalents),
                share_capital = COALESCE(excluded.share_capital, company_financials.share_capital),
                source = COALESCE(excluded.source, company_financials.source),
                currency_scale = COALESCE(excluded.currency_scale, company_financials.currency_scale),
                currency = excluded.currency,
                updated_at = excluded.updated_at
            """,
            (
                symbol,
                period_end,
                period_type,
                item.get("sales"),
                item.get("gross_profit"),
                item.get("operating_profit"),
                item.get("profit_before_tax"),
                item.get("profit_after_tax"),
                item.get("eps"),
                item.get("markup_earned"),
                item.get("markup_expensed"),
                item.get("total_assets"),
                item.get("total_liabilities"),
                item.get("total_equity"),
                item.get("cost_of_sales"),
                item.get("operating_expenses"),
                item.get("finance_cost"),
                item.get("other_income"),
                item.get("taxation"),
                item.get("net_interest_income"),
                item.get("non_markup_income"),
                item.get("total_income"),
                item.get("provisions"),
                item.get("current_assets"),
                item.get("non_current_assets"),
                item.get("current_liabilities"),
                item.get("non_current_liabilities"),
                item.get("cash_and_equivalents"),
                item.get("share_capital"),
                item.get("source"),
                item.get("currency_scale"),
                item.get("currency", "PKR"),
                now,
            ),
        )
        count += cur.rowcount

    con.commit()
    return count


def get_company_financials(
    con: sqlite3.Connection,
    symbol: str,
    period_type: str | None = None,
    limit: int = 20,
) -> pd.DataFrame:
    """
    Get company financial data.

    Args:
        con: Database connection
        symbol: Stock symbol
        period_type: 'annual' or 'quarterly', or None for both
        limit: Maximum rows to return

    Returns:
        DataFrame with financial data
    """
    query = """
        SELECT symbol, period_end, period_type,
               sales, gross_profit, operating_profit,
               profit_before_tax, profit_after_tax, eps,
               markup_earned, markup_expensed,
               total_assets, total_liabilities, total_equity,
               cost_of_sales, operating_expenses, finance_cost,
               other_income, taxation,
               net_interest_income, non_markup_income, total_income, provisions,
               current_assets, non_current_assets,
               current_liabilities, non_current_liabilities,
               cash_and_equivalents, share_capital,
               source, currency_scale,
               currency, updated_at
        FROM company_financials
        WHERE symbol = ?
    """
    params: list = [symbol.upper()]

    if period_type:
        query += " AND period_type = ?"
        params.append(period_type)

    query += " ORDER BY period_end DESC LIMIT ?"
    params.append(limit)

    return pd.read_sql_query(query, con, params=params)


# =============================================================================
# Company Ratios Functions
# =============================================================================


def upsert_company_ratios(
    con: sqlite3.Connection,
    symbol: str,
    ratios: list[dict],
) -> int:
    """
    Upsert company ratio data.

    Args:
        con: Database connection
        symbol: Stock symbol
        ratios: List of dicts with ratio data per period

    Returns:
        Number of rows upserted
    """
    if not ratios:
        return 0

    symbol = symbol.upper()
    now = now_iso()
    count = 0

    for item in ratios:
        period_end = item.get("period_end")
        period_type = item.get("period_type", "annual")

        if not period_end:
            continue

        cur = con.execute(
            """
            INSERT INTO company_ratios (
                symbol, period_end, period_type,
                gross_profit_margin, net_profit_margin, operating_margin,
                return_on_equity, return_on_assets,
                sales_growth, eps_growth, profit_growth,
                pe_ratio, pb_ratio, peg_ratio,
                debt_to_equity, current_ratio, interest_coverage,
                asset_turnover, equity_multiplier,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, period_end, period_type) DO UPDATE SET
                gross_profit_margin = COALESCE(excluded.gross_profit_margin, company_ratios.gross_profit_margin),
                net_profit_margin = COALESCE(excluded.net_profit_margin, company_ratios.net_profit_margin),
                operating_margin = COALESCE(excluded.operating_margin, company_ratios.operating_margin),
                return_on_equity = COALESCE(excluded.return_on_equity, company_ratios.return_on_equity),
                return_on_assets = COALESCE(excluded.return_on_assets, company_ratios.return_on_assets),
                sales_growth = COALESCE(excluded.sales_growth, company_ratios.sales_growth),
                eps_growth = COALESCE(excluded.eps_growth, company_ratios.eps_growth),
                profit_growth = COALESCE(excluded.profit_growth, company_ratios.profit_growth),
                pe_ratio = COALESCE(excluded.pe_ratio, company_ratios.pe_ratio),
                pb_ratio = COALESCE(excluded.pb_ratio, company_ratios.pb_ratio),
                peg_ratio = COALESCE(excluded.peg_ratio, company_ratios.peg_ratio),
                debt_to_equity = COALESCE(excluded.debt_to_equity, company_ratios.debt_to_equity),
                current_ratio = COALESCE(excluded.current_ratio, company_ratios.current_ratio),
                interest_coverage = COALESCE(excluded.interest_coverage, company_ratios.interest_coverage),
                asset_turnover = COALESCE(excluded.asset_turnover, company_ratios.asset_turnover),
                equity_multiplier = COALESCE(excluded.equity_multiplier, company_ratios.equity_multiplier),
                updated_at = excluded.updated_at
            """,
            (
                symbol,
                period_end,
                period_type,
                item.get("gross_profit_margin"),
                item.get("net_profit_margin"),
                item.get("operating_margin"),
                item.get("return_on_equity"),
                item.get("return_on_assets"),
                item.get("sales_growth"),
                item.get("eps_growth"),
                item.get("profit_growth"),
                item.get("pe_ratio"),
                item.get("pb_ratio"),
                item.get("peg_ratio"),
                item.get("debt_to_equity"),
                item.get("current_ratio"),
                item.get("interest_coverage"),
                item.get("asset_turnover"),
                item.get("equity_multiplier"),
                now,
            ),
        )
        count += cur.rowcount

    con.commit()
    return count


def get_company_ratios(
    con: sqlite3.Connection,
    symbol: str,
    period_type: str | None = None,
    limit: int = 20,
) -> pd.DataFrame:
    """
    Get company ratio data.

    Args:
        con: Database connection
        symbol: Stock symbol
        period_type: 'annual' or 'quarterly', or None for both
        limit: Maximum rows to return

    Returns:
        DataFrame with ratio data
    """
    query = """
        SELECT symbol, period_end, period_type,
               gross_profit_margin, net_profit_margin, operating_margin,
               return_on_equity, return_on_assets,
               sales_growth, eps_growth, profit_growth,
               pe_ratio, pb_ratio, peg_ratio,
               updated_at
        FROM company_ratios
        WHERE symbol = ?
    """
    params: list = [symbol.upper()]

    if period_type:
        query += " AND period_type = ?"
        params.append(period_type)

    query += " ORDER BY period_end DESC LIMIT ?"
    params.append(limit)

    return pd.read_sql_query(query, con, params=params)


# =============================================================================
# Company Payouts Functions
# =============================================================================


def upsert_company_payouts(
    con: sqlite3.Connection,
    symbol: str,
    payouts: list[dict],
) -> int:
    """
    Upsert company payout/dividend data.

    Args:
        con: Database connection
        symbol: Stock symbol
        payouts: List of dicts with payout data

    Returns:
        Number of rows upserted
    """
    if not payouts:
        return 0

    symbol = symbol.upper()
    now = now_iso()
    count = 0

    for item in payouts:
        ex_date = item.get("ex_date")
        payout_type = item.get("payout_type", "cash")

        if not ex_date:
            continue

        cur = con.execute(
            """
            INSERT INTO company_payouts (
                symbol, ex_date, payout_type,
                announcement_date, book_closure_from, book_closure_to,
                amount, fiscal_year, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, ex_date, payout_type) DO UPDATE SET
                announcement_date = excluded.announcement_date,
                book_closure_from = excluded.book_closure_from,
                book_closure_to = excluded.book_closure_to,
                amount = excluded.amount,
                fiscal_year = excluded.fiscal_year,
                updated_at = excluded.updated_at
            """,
            (
                symbol,
                ex_date,
                payout_type,
                item.get("announcement_date"),
                item.get("book_closure_from"),
                item.get("book_closure_to"),
                item.get("amount"),
                item.get("fiscal_year"),
                now,
            ),
        )
        count += cur.rowcount

    con.commit()
    return count


def get_company_payouts(
    con: sqlite3.Connection,
    symbol: str,
    payout_type: str | None = None,
    limit: int = 50,
) -> pd.DataFrame:
    """
    Get company payout/dividend history.

    Args:
        con: Database connection
        symbol: Stock symbol
        payout_type: 'cash', 'bonus', 'right', or None for all
        limit: Maximum rows to return

    Returns:
        DataFrame with payout data
    """
    query = """
        SELECT symbol, ex_date, payout_type,
               announcement_date, book_closure_from, book_closure_to,
               amount, fiscal_year, updated_at
        FROM company_payouts
        WHERE symbol = ?
    """
    params: list = [symbol.upper()]

    if payout_type:
        query += " AND payout_type = ?"
        params.append(payout_type)

    query += " ORDER BY ex_date DESC LIMIT ?"
    params.append(limit)

    return pd.read_sql_query(query, con, params=params)


# =============================================================================
# Financial Announcements Functions
# =============================================================================


def upsert_financial_announcement(
    con: sqlite3.Connection,
    symbol: str,
    announcement: dict,
) -> bool:
    """
    Upsert a single financial announcement.

    Args:
        con: Database connection
        symbol: Stock symbol
        announcement: Dict with announcement data

    Returns:
        True if inserted/updated successfully
    """
    now = now_iso()
    symbol = symbol.upper()

    ann_date = announcement.get("announcement_date")
    fiscal_period = announcement.get("fiscal_period") or announcement.get("fiscal_year", "")

    if not ann_date or not fiscal_period:
        return False

    try:
        con.execute(
            """
            INSERT INTO financial_announcements (
                symbol, announcement_date, fiscal_period,
                profit_before_tax, profit_after_tax, eps,
                dividend_payout, dividend_amount, payout_type,
                agm_date, book_closure_from, book_closure_to,
                company_name, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, announcement_date, fiscal_period) DO UPDATE SET
                profit_before_tax = excluded.profit_before_tax,
                profit_after_tax = excluded.profit_after_tax,
                eps = excluded.eps,
                dividend_payout = excluded.dividend_payout,
                dividend_amount = excluded.dividend_amount,
                payout_type = excluded.payout_type,
                agm_date = excluded.agm_date,
                book_closure_from = excluded.book_closure_from,
                book_closure_to = excluded.book_closure_to,
                company_name = excluded.company_name,
                updated_at = excluded.updated_at
            """,
            (
                symbol,
                ann_date,
                fiscal_period,
                announcement.get("profit_before_tax"),
                announcement.get("profit_after_tax"),
                announcement.get("eps"),
                announcement.get("dividend_payout") or announcement.get("details_raw"),
                announcement.get("dividend_amount") or announcement.get("amount"),
                announcement.get("payout_type"),
                announcement.get("agm_date"),
                announcement.get("book_closure_from"),
                announcement.get("book_closure_to"),
                announcement.get("company_name"),
                now,
            ),
        )
        con.commit()
        return True
    except sqlite3.Error:
        return False


def upsert_financial_announcements(
    con: sqlite3.Connection,
    symbol: str,
    announcements: list[dict],
) -> int:
    """
    Upsert multiple financial announcements.

    Args:
        con: Database connection
        symbol: Stock symbol
        announcements: List of announcement dicts

    Returns:
        Number of rows upserted
    """
    count = 0
    for ann in announcements:
        if upsert_financial_announcement(con, symbol, ann):
            count += 1
    return count


def get_financial_announcements(
    con: sqlite3.Connection,
    symbol: str,
    limit: int = 10,
) -> list[dict]:
    """
    Get financial announcements for a symbol.

    Args:
        con: Database connection
        symbol: Stock symbol
        limit: Maximum rows to return

    Returns:
        List of announcement dicts
    """
    cur = con.execute(
        """
        SELECT symbol, announcement_date, fiscal_period,
               profit_before_tax, profit_after_tax, eps,
               dividend_payout, dividend_amount, payout_type,
               agm_date, book_closure_from, book_closure_to,
               company_name, updated_at
        FROM financial_announcements
        WHERE symbol = ?
        ORDER BY announcement_date DESC
        LIMIT ?
        """,
        (symbol.upper(), limit),
    )

    results = []
    for row in cur.fetchall():
        results.append({
            "symbol": row[0],
            "announcement_date": row[1],
            "fiscal_period": row[2],
            "profit_before_tax": row[3],
            "profit_after_tax": row[4],
            "eps": row[5],
            "dividend_payout": row[6],
            "dividend_amount": row[7],
            "payout_type": row[8],
            "agm_date": row[9],
            "book_closure_from": row[10],
            "book_closure_to": row[11],
            "company_name": row[12],
        })

    return results


# =============================================================================
# Company Snapshot Functions
# =============================================================================


def upsert_company_snapshot(
    con: sqlite3.Connection,
    symbol: str,
    snapshot_date: str,
    data: dict,
    raw_html: str | None = None,
) -> dict:
    """
    Upsert a full company snapshot with all scraped data.

    This is the main function for storing comprehensive company data
    in a NoSQL-style flexible format.

    Args:
        con: Database connection
        symbol: Stock symbol
        snapshot_date: Date of snapshot (YYYY-MM-DD)
        data: Dict containing all scraped data with keys:
              - company_name, sector_code, sector_name
              - quote_data, equity_data, profile_data
              - financials_data, ratios_data, trading_data
              - futures_data, announcements_data
        raw_html: Optional raw HTML for reprocessing

    Returns:
        Dict with status and row count
    """
    symbol = symbol.upper()
    now = now_iso()

    # Serialize nested dicts to JSON
    def to_json(obj):
        return json.dumps(obj) if obj else None

    try:
        con.execute(
            """
            INSERT INTO company_snapshots (
                symbol, snapshot_date, snapshot_time,
                company_name, sector_code, sector_name,
                quote_data, equity_data, profile_data,
                financials_data, ratios_data, trading_data,
                futures_data, announcements_data,
                raw_html, source_url, scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, snapshot_date) DO UPDATE SET
                snapshot_time = excluded.snapshot_time,
                company_name = excluded.company_name,
                sector_code = excluded.sector_code,
                sector_name = excluded.sector_name,
                quote_data = excluded.quote_data,
                equity_data = excluded.equity_data,
                profile_data = excluded.profile_data,
                financials_data = excluded.financials_data,
                ratios_data = excluded.ratios_data,
                trading_data = excluded.trading_data,
                futures_data = excluded.futures_data,
                announcements_data = excluded.announcements_data,
                raw_html = excluded.raw_html,
                source_url = excluded.source_url,
                scraped_at = excluded.scraped_at
            """,
            (
                symbol,
                snapshot_date,
                data.get("snapshot_time"),
                data.get("company_name"),
                data.get("sector_code"),
                data.get("sector_name"),
                to_json(data.get("quote_data")),
                to_json(data.get("equity_data")),
                to_json(data.get("profile_data")),
                to_json(data.get("financials_data")),
                to_json(data.get("ratios_data")),
                to_json(data.get("trading_data")),
                to_json(data.get("futures_data")),
                to_json(data.get("announcements_data")),
                raw_html,
                data.get("source_url"),
                now,
            ),
        )
        con.commit()
        return {"status": "ok", "symbol": symbol, "date": snapshot_date}
    except Exception as e:
        return {"status": "error", "symbol": symbol, "error": str(e)}


def get_company_snapshot(
    con: sqlite3.Connection,
    symbol: str,
    snapshot_date: str | None = None,
) -> dict | None:
    """
    Get a company snapshot, optionally for a specific date.

    Args:
        con: Database connection
        symbol: Stock symbol
        snapshot_date: Specific date or None for latest

    Returns:
        Dict with all snapshot data (JSON fields parsed)
    """
    symbol = symbol.upper()

    if snapshot_date:
        query = """
            SELECT * FROM company_snapshots
            WHERE symbol = ? AND snapshot_date = ?
        """
        cur = con.execute(query, (symbol, snapshot_date))
    else:
        query = """
            SELECT * FROM company_snapshots
            WHERE symbol = ?
            ORDER BY snapshot_date DESC
            LIMIT 1
        """
        cur = con.execute(query, (symbol,))

    row = cur.fetchone()
    if not row:
        return None

    # Convert Row to dict and parse JSON fields
    result = dict(row)
    json_fields = [
        "quote_data", "equity_data", "profile_data",
        "financials_data", "ratios_data", "trading_data",
        "futures_data", "announcements_data"
    ]
    for field in json_fields:
        if result.get(field):
            try:
                result[field] = json.loads(result[field])
            except json.JSONDecodeError:
                pass

    return result


# =============================================================================
# Equity Structure Functions
# =============================================================================


def upsert_equity_structure(
    con: sqlite3.Connection,
    symbol: str,
    as_of_date: str,
    data: dict,
) -> int:
    """
    Upsert equity structure data.

    Args:
        con: Database connection
        symbol: Stock symbol
        as_of_date: Date of the data
        data: Dict with equity structure fields

    Returns:
        Number of rows affected
    """
    symbol = symbol.upper()
    now = now_iso()

    cur = con.execute(
        """
        INSERT INTO equity_structure (
            symbol, as_of_date,
            authorized_shares, issued_shares, outstanding_shares, treasury_shares,
            free_float_shares, free_float_percent,
            market_cap, market_cap_usd,
            ownership_data, face_value, scraped_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol, as_of_date) DO UPDATE SET
            authorized_shares = excluded.authorized_shares,
            issued_shares = excluded.issued_shares,
            outstanding_shares = excluded.outstanding_shares,
            treasury_shares = excluded.treasury_shares,
            free_float_shares = excluded.free_float_shares,
            free_float_percent = excluded.free_float_percent,
            market_cap = excluded.market_cap,
            market_cap_usd = excluded.market_cap_usd,
            ownership_data = excluded.ownership_data,
            face_value = excluded.face_value,
            scraped_at = excluded.scraped_at
        """,
        (
            symbol, as_of_date,
            data.get("authorized_shares"),
            data.get("issued_shares"),
            data.get("outstanding_shares"),
            data.get("treasury_shares"),
            data.get("free_float_shares"),
            data.get("free_float_percent"),
            data.get("market_cap"),
            data.get("market_cap_usd"),
            json.dumps(data.get("ownership_data")) if data.get("ownership_data") else None,
            data.get("face_value"),
            now,
        ),
    )
    con.commit()
    return cur.rowcount


def get_equity_structure(
    con: sqlite3.Connection,
    symbol: str,
    as_of_date: str | None = None,
) -> dict | None:
    """
    Get equity structure for a symbol.

    Args:
        con: Database connection
        symbol: Stock symbol
        as_of_date: Specific date or None for latest

    Returns:
        Dict with equity structure data
    """
    symbol = symbol.upper()

    if as_of_date:
        query = """
            SELECT * FROM equity_structure
            WHERE symbol = ? AND as_of_date = ?
        """
        cur = con.execute(query, (symbol, as_of_date))
    else:
        query = """
            SELECT * FROM equity_structure
            WHERE symbol = ?
            ORDER BY as_of_date DESC
            LIMIT 1
        """
        cur = con.execute(query, (symbol,))

    row = cur.fetchone()
    if not row:
        return None

    result = dict(row)
    if result.get("ownership_data"):
        try:
            result["ownership_data"] = json.loads(result["ownership_data"])
        except json.JSONDecodeError:
            pass

    return result


# =============================================================================
# Trading Session Functions
# =============================================================================


def upsert_trading_session(
    con: sqlite3.Connection,
    symbol: str,
    session_date: str,
    market_type: str,
    data: dict,
) -> int:
    """
    Upsert trading session data with full market microstructure.

    Args:
        con: Database connection
        symbol: Stock symbol
        session_date: Trading date (YYYY-MM-DD)
        market_type: 'REG', 'FUT', 'CSF', 'ODL'
        data: Dict with all trading metrics

    Returns:
        Number of rows affected
    """
    symbol = symbol.upper()
    now = now_iso()
    contract_month = data.get("contract_month", "")

    cur = con.execute(
        """
        INSERT INTO trading_sessions (
            symbol, session_date, market_type, contract_month,
            open, high, low, close, volume,
            ldcp, prev_close, change_value, change_percent,
            bid_price, bid_volume, ask_price, ask_volume, spread,
            day_range_low, day_range_high, circuit_low, circuit_high,
            week_52_low, week_52_high,
            total_trades, turnover, vwap,
            var_percent, haircut_percent, pe_ratio_ttm,
            ytd_change, year_1_change,
            last_update, scraped_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol, session_date, market_type, contract_month) DO UPDATE SET
            open = excluded.open,
            high = excluded.high,
            low = excluded.low,
            close = excluded.close,
            volume = excluded.volume,
            ldcp = excluded.ldcp,
            prev_close = excluded.prev_close,
            change_value = excluded.change_value,
            change_percent = excluded.change_percent,
            bid_price = excluded.bid_price,
            bid_volume = excluded.bid_volume,
            ask_price = excluded.ask_price,
            ask_volume = excluded.ask_volume,
            spread = excluded.spread,
            day_range_low = excluded.day_range_low,
            day_range_high = excluded.day_range_high,
            circuit_low = excluded.circuit_low,
            circuit_high = excluded.circuit_high,
            week_52_low = excluded.week_52_low,
            week_52_high = excluded.week_52_high,
            total_trades = excluded.total_trades,
            turnover = excluded.turnover,
            vwap = excluded.vwap,
            var_percent = excluded.var_percent,
            haircut_percent = excluded.haircut_percent,
            pe_ratio_ttm = excluded.pe_ratio_ttm,
            ytd_change = excluded.ytd_change,
            year_1_change = excluded.year_1_change,
            last_update = excluded.last_update,
            scraped_at = excluded.scraped_at
        """,
        (
            symbol, session_date, market_type, contract_month,
            data.get("open"), data.get("high"), data.get("low"),
            data.get("close"), data.get("volume"),
            data.get("ldcp"), data.get("prev_close"),
            data.get("change_value"), data.get("change_percent"),
            data.get("bid_price"), data.get("bid_volume"),
            data.get("ask_price"), data.get("ask_volume"),
            data.get("spread"),
            data.get("day_range_low"), data.get("day_range_high"),
            data.get("circuit_low"), data.get("circuit_high"),
            data.get("week_52_low"), data.get("week_52_high"),
            data.get("total_trades"), data.get("turnover"),
            data.get("vwap"),
            data.get("var_percent"), data.get("haircut_percent"),
            data.get("pe_ratio_ttm"),
            data.get("ytd_change"), data.get("year_1_change"),
            data.get("last_update"), now,
        ),
    )
    con.commit()
    return cur.rowcount


def get_trading_sessions(
    con: sqlite3.Connection,
    symbol: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    market_type: str | None = None,
    limit: int = 1000,
) -> pd.DataFrame:
    """
    Query trading sessions with filters.

    Args:
        con: Database connection
        symbol: Filter by symbol
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        market_type: Filter by market type ('REG', 'FUT', etc.)
        limit: Max rows

    Returns:
        DataFrame with trading session data
    """
    query = "SELECT * FROM trading_sessions WHERE 1=1"
    params: list = []

    if symbol:
        query += " AND symbol = ?"
        params.append(symbol.upper())
    if start_date:
        query += " AND session_date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND session_date <= ?"
        params.append(end_date)
    if market_type:
        query += " AND market_type = ?"
        params.append(market_type)

    query += " ORDER BY session_date DESC, symbol LIMIT ?"
    params.append(limit)

    return pd.read_sql_query(query, con, params=params)


# =============================================================================
# Corporate Announcement Functions
# =============================================================================


def upsert_corporate_announcement(
    con: sqlite3.Connection,
    symbol: str,
    announcement_date: str,
    announcement_type: str,
    title: str,
    data: dict | None = None,
) -> int:
    """
    Upsert a corporate announcement.

    Args:
        con: Database connection
        symbol: Stock symbol
        announcement_date: Date of announcement
        announcement_type: Type (financial_result, board_meeting, etc.)
        title: Announcement title
        data: Optional additional data (document_url, summary, etc.)

    Returns:
        Number of rows affected
    """
    symbol = symbol.upper()
    now = now_iso()
    data = data or {}

    # Create hash for deduplication
    title_hash = hashlib.md5(title.encode()).hexdigest()

    cur = con.execute(
        """
        INSERT INTO corporate_announcements (
            symbol, announcement_date, announcement_type, category,
            title, title_hash, document_url, document_type,
            summary, key_figures, scraped_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol, announcement_date, title_hash) DO UPDATE SET
            category = excluded.category,
            document_url = excluded.document_url,
            document_type = excluded.document_type,
            summary = excluded.summary,
            key_figures = excluded.key_figures,
            scraped_at = excluded.scraped_at
        """,
        (
            symbol,
            announcement_date,
            announcement_type,
            data.get("category"),
            title,
            title_hash,
            data.get("document_url"),
            data.get("document_type"),
            data.get("summary"),
            json.dumps(data.get("key_figures")) if data.get("key_figures") else None,
            now,
        ),
    )
    con.commit()
    return cur.rowcount


def get_corporate_announcements(
    con: sqlite3.Connection,
    symbol: str | None = None,
    announcement_type: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 100,
) -> pd.DataFrame:
    """
    Query corporate announcements with filters.

    Args:
        con: Database connection
        symbol: Filter by symbol
        announcement_type: Filter by type
        start_date: Start date
        end_date: End date
        limit: Max rows

    Returns:
        DataFrame with announcements
    """
    query = "SELECT * FROM corporate_announcements WHERE 1=1"
    params: list = []

    if symbol:
        query += " AND symbol = ?"
        params.append(symbol.upper())
    if announcement_type:
        query += " AND announcement_type = ?"
        params.append(announcement_type)
    if start_date:
        query += " AND announcement_date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND announcement_date <= ?"
        params.append(end_date)

    query += " ORDER BY announcement_date DESC, symbol LIMIT ?"
    params.append(limit)

    return pd.read_sql_query(query, con, params=params)


# =============================================================================
# Unified Company Data Function
# =============================================================================


def get_company_unified(
    con: sqlite3.Connection,
    symbol: str,
    include_history: bool = False,
) -> dict | None:
    """Get unified company data from Deep Data tables.

    This is the primary function for accessing company data in the hybrid model.
    It reads from company_snapshots, trading_sessions, and corporate_announcements.

    Args:
        con: Database connection
        symbol: Stock symbol
        include_history: If True, include historical snapshots

    Returns:
        Dict with unified company data or None if not found
    """
    from datetime import datetime

    symbol = symbol.upper()
    today = datetime.now().strftime("%Y-%m-%d")

    # Get latest snapshot
    cur = con.execute(
        """
        SELECT * FROM company_snapshots
        WHERE symbol = ?
        ORDER BY snapshot_date DESC, scraped_at DESC
        LIMIT 1
        """,
        (symbol,),
    )
    snapshot_row = cur.fetchone()

    if not snapshot_row:
        return None

    snapshot = dict(snapshot_row)

    # Parse JSON fields
    json_fields = [
        "quote_data", "equity_data", "profile_data", "financials_data",
        "ratios_data", "trading_data", "futures_data", "announcements_data"
    ]
    for field in json_fields:
        if snapshot.get(field):
            try:
                snapshot[field] = json.loads(snapshot[field])
            except json.JSONDecodeError:
                snapshot[field] = {}

    # Get today's trading sessions (all market types)
    cur = con.execute(
        """
        SELECT * FROM trading_sessions
        WHERE symbol = ? AND session_date = ?
        ORDER BY market_type
        """,
        (symbol, today),
    )
    trading_rows = cur.fetchall()
    trading_sessions = {}
    for row in trading_rows:
        session = dict(row)
        market_type = session.get("market_type", "REG")
        contract = session.get("contract_month", "")
        key = f"{market_type}_{contract}" if contract else market_type
        trading_sessions[key] = session

    # Get recent announcements
    cur = con.execute(
        """
        SELECT * FROM corporate_announcements
        WHERE symbol = ?
        ORDER BY announcement_date DESC
        LIMIT 20
        """,
        (symbol,),
    )
    announcements = [dict(row) for row in cur.fetchall()]

    # Get equity structure
    cur = con.execute(
        """
        SELECT * FROM equity_structure
        WHERE symbol = ?
        ORDER BY as_of_date DESC
        LIMIT 1
        """,
        (symbol,),
    )
    equity_row = cur.fetchone()
    equity_structure = dict(equity_row) if equity_row else {}

    # Build unified response
    quote_data = snapshot.get("quote_data", {})
    trading_data = snapshot.get("trading_data", {})
    equity_data = snapshot.get("equity_data", {})
    reg_trading = trading_data.get("REG", {}) if trading_data else {}

    # Get price for calculations
    price = reg_trading.get("close") or quote_data.get("close")
    total_shares = equity_structure.get("outstanding_shares") or equity_data.get("outstanding_shares")

    # Calculate market cap if not available
    market_cap = equity_structure.get("market_cap") or equity_data.get("market_cap")
    if (not market_cap or market_cap == 0) and price and total_shares:
        market_cap = price * total_shares

    result = {
        # Core info
        "symbol": symbol,
        "company_name": snapshot.get("company_name") or quote_data.get("company_name"),
        "sector_code": snapshot.get("sector_code"),
        "sector_name": snapshot.get("sector_name") or quote_data.get("sector_name"),
        "snapshot_date": snapshot.get("snapshot_date"),
        "scraped_at": snapshot.get("scraped_at"),

        # Current quote (from snapshot or trading session)
        "price": price,
        "open": reg_trading.get("open") or quote_data.get("open"),
        "high": reg_trading.get("high") or quote_data.get("high"),
        "low": reg_trading.get("low") or quote_data.get("low"),
        "close": price,
        "volume": reg_trading.get("volume") or quote_data.get("volume"),
        "ldcp": reg_trading.get("ldcp") or quote_data.get("ldcp"),
        "change": quote_data.get("change_value") or quote_data.get("change"),
        "change_pct": reg_trading.get("change_percent") or quote_data.get("change_percent") or quote_data.get("change_pct"),

        # Ranges
        "day_range_low": reg_trading.get("day_range_low") or quote_data.get("day_range_low"),
        "day_range_high": reg_trading.get("day_range_high") or quote_data.get("day_range_high"),
        "wk52_low": reg_trading.get("week_52_low") or quote_data.get("wk52_low"),
        "wk52_high": reg_trading.get("week_52_high") or quote_data.get("wk52_high"),
        "circuit_low": reg_trading.get("circuit_low") or quote_data.get("circuit_low"),
        "circuit_high": reg_trading.get("circuit_high") or quote_data.get("circuit_high"),

        # Valuation
        "pe_ratio": reg_trading.get("pe_ratio_ttm") or quote_data.get("pe_ratio"),
        "market_cap": market_cap,

        # Performance
        "ytd_change_pct": reg_trading.get("ytd_change"),
        "one_year_change_pct": reg_trading.get("year_1_change"),

        # Risk
        "haircut": reg_trading.get("haircut_percent"),
        "variance": reg_trading.get("var_percent"),

        # Equity
        "total_shares": total_shares,
        "free_float_shares": equity_structure.get("free_float_shares") or equity_data.get("free_float_shares"),
        "free_float_pct": equity_structure.get("free_float_percent") or equity_data.get("free_float_percent"),

        # Full data objects
        "quote_data": quote_data,
        "trading_data": trading_data,
        "equity_data": snapshot.get("equity_data", {}),
        "profile_data": snapshot.get("profile_data", {}),
        "financials_data": snapshot.get("financials_data", {}),
        "ratios_data": snapshot.get("ratios_data", {}),
        "futures_data": snapshot.get("futures_data", {}),

        # Live trading sessions (today)
        "trading_sessions": trading_sessions,

        # Announcements
        "announcements": announcements,
        "announcements_count": len(announcements),

        # Equity structure
        "equity_structure": equity_structure,
    }

    # Include historical snapshots if requested
    if include_history:
        cur = con.execute(
            """
            SELECT snapshot_date, scraped_at,
                   json_extract(quote_data, '$.close') as close,
                   json_extract(quote_data, '$.volume') as volume
            FROM company_snapshots
            WHERE symbol = ?
            ORDER BY snapshot_date DESC
            LIMIT 30
            """,
            (symbol,),
        )
        result["history"] = [dict(row) for row in cur.fetchall()]

    return result
