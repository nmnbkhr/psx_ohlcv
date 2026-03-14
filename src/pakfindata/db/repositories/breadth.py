"""Intraday market breadth persistence.

Stores minute-level advance/decline, cumulative A/D, and net tick momentum
computed from intraday_bars via the LAG window function approach.

Table: intraday_breadth
  - Primary key: (date, minute)
  - Per-minute: adv, dec, total, net_ticks, cum_ad, cum_ticks
  - Daily summary row: minute = 'DAILY' with end-of-day totals
"""

import sqlite3

import pandas as pd


_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS intraday_breadth (
    date         TEXT NOT NULL,
    minute       TEXT NOT NULL,
    adv          INTEGER NOT NULL DEFAULT 0,
    dec          INTEGER NOT NULL DEFAULT 0,
    total        INTEGER NOT NULL DEFAULT 0,
    net_ticks    INTEGER NOT NULL DEFAULT 0,
    cum_ad       INTEGER NOT NULL DEFAULT 0,
    cum_ticks    INTEGER NOT NULL DEFAULT 0,
    ingested_at  TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (date, minute)
);
"""

_CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_breadth_date ON intraday_breadth(date)",
]


def init_breadth_schema(con: sqlite3.Connection) -> None:
    """Create intraday_breadth table and indexes (idempotent)."""
    con.executescript(_CREATE_TABLE + "\n".join(_CREATE_INDEXES))


def compute_and_persist_breadth(con: sqlite3.Connection, date_str: str) -> int:
    """Compute breadth from intraday_bars for a date and persist to intraday_breadth.

    Uses tick-to-tick LAG approach:
      - Per symbol per minute: compare each tick's close to previous tick (LAG)
      - Uptick (+1), Downtick (-1), Flat (0)
      - Advancing = symbols with net positive direction, Declining = net negative

    Returns number of minute rows inserted.
    """
    init_breadth_schema(con)

    ts_start = f"{date_str} 00:00:00"
    ts_end = f"{date_str} 23:59:59"

    df = pd.read_sql_query(
        """WITH tick_dir AS (
             SELECT
               symbol,
               SUBSTR(ts, 1, 16) AS minute,
               CASE
                 WHEN close > LAG(close) OVER (PARTITION BY symbol ORDER BY ts_epoch) THEN 1
                 WHEN close < LAG(close) OVER (PARTITION BY symbol ORDER BY ts_epoch) THEN -1
                 ELSE 0
               END AS tick_sign
             FROM intraday_bars
             WHERE ts BETWEEN ? AND ?
           ),
           symbol_minute AS (
             SELECT minute, symbol, SUM(tick_sign) AS net_dir
             FROM tick_dir
             GROUP BY minute, symbol
           )
           SELECT
             minute,
             COUNT(DISTINCT CASE WHEN net_dir > 0 THEN symbol END) AS adv,
             COUNT(DISTINCT CASE WHEN net_dir < 0 THEN symbol END) AS dec,
             COUNT(DISTINCT symbol) AS total,
             SUM(net_dir) AS net_ticks
           FROM symbol_minute
           GROUP BY minute
           ORDER BY minute""",
        con,
        params=[ts_start, ts_end],
    )

    if df.empty:
        return 0

    # Compute cumulative columns
    df["net"] = df["adv"] - df["dec"]
    df["cum_ad"] = df["net"].cumsum()
    df["cum_ticks"] = df["net_ticks"].cumsum()

    # Delete existing rows for this date (full replace)
    con.execute("DELETE FROM intraday_breadth WHERE date = ?", (date_str,))

    # Insert minute rows
    con.executemany(
        """INSERT INTO intraday_breadth (date, minute, adv, dec, total, net_ticks, cum_ad, cum_ticks)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            (date_str, row["minute"], int(row["adv"]), int(row["dec"]),
             int(row["total"]), int(row["net_ticks"]),
             int(row["cum_ad"]), int(row["cum_ticks"]))
            for _, row in df.iterrows()
        ],
    )

    # Insert daily summary row
    last = df.iloc[-1]
    con.execute(
        """INSERT OR REPLACE INTO intraday_breadth
           (date, minute, adv, dec, total, net_ticks, cum_ad, cum_ticks)
           VALUES (?, 'DAILY', ?, ?, ?, ?, ?, ?)""",
        (date_str, int(last["adv"]), int(last["dec"]),
         int(last["total"]), int(df["net_ticks"].sum()),
         int(last["cum_ad"]), int(last["cum_ticks"])),
    )

    con.commit()
    return len(df)


def get_breadth_dates(con: sqlite3.Connection) -> list[str]:
    """Return all dates with persisted breadth data, newest first."""
    init_breadth_schema(con)
    rows = con.execute(
        "SELECT DISTINCT date FROM intraday_breadth WHERE minute = 'DAILY' ORDER BY date DESC"
    ).fetchall()
    return [r[0] if isinstance(r, tuple) else r["date"] for r in rows]


def get_breadth_for_date(con: sqlite3.Connection, date_str: str) -> pd.DataFrame:
    """Load persisted minute-level breadth for a date."""
    init_breadth_schema(con)
    return pd.read_sql_query(
        """SELECT minute, adv, dec, total, net_ticks, cum_ad, cum_ticks
           FROM intraday_breadth
           WHERE date = ? AND minute != 'DAILY'
           ORDER BY minute""",
        con,
        params=[date_str],
    )


def get_breadth_daily_summary(con: sqlite3.Connection, limit: int = 60) -> pd.DataFrame:
    """Load daily summary rows for the breadth history table."""
    init_breadth_schema(con)
    return pd.read_sql_query(
        """SELECT date, adv, dec, total, net_ticks, cum_ad, cum_ticks, ingested_at
           FROM intraday_breadth
           WHERE minute = 'DAILY'
           ORDER BY date DESC
           LIMIT ?""",
        con,
        params=[limit],
    )
