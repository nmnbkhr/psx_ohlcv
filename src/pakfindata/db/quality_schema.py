"""Data quality schema: declarative validator rules + per-run results.

Two tables, parallel infrastructure to ``data_freshness`` (Phase 0.2):

- ``data_quality_rules`` — declarative rule configuration. One row =
  one validation check for one domain. Hand-edited or seeded via
  ``pakfindata.quality.seed_rules``. Engine reads, never writes.

- ``data_quality_results`` — per-run outcomes. The validator engine
  writes a row here for every rule it evaluates, inside the same
  ``safe_writer`` transaction as the ETL data write. API reads it
  to serve ``/v1/quality`` and ``/v1/quality/{domain}``.

The ``domain`` column on both tables joins to
``data_freshness.domain`` (the dataset_id PK in the catalog). For
the ``indices`` domain that means ``domain='indices'``, not
``'psx_indices'`` — the source-table identifier lives in
``data_quality_rules.params.table`` where check handlers need it.

Bootstrap is invoked from ``db/connection.py::init_schema()`` to
match the existing per-domain pattern (``init_etf_schema``,
``init_treasury_schema``, etc.). Idempotent — safe to re-run.

Split-and-loop instead of ``executescript()`` per the Phase 0.1
discipline (executescript autocommits any pending transaction; safer
to keep statement execution explicit even at boot time).
"""

from __future__ import annotations

import sqlite3

__all__ = [
    "QUALITY_SCHEMA_SQL",
    "init_quality_schema",
]


QUALITY_SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS data_quality_rules (
    rule_id       TEXT PRIMARY KEY,
    domain        TEXT NOT NULL,
    check_type    TEXT NOT NULL,
    params        TEXT NOT NULL,
    severity      TEXT NOT NULL,
    enabled       INTEGER NOT NULL DEFAULT 1,
    description   TEXT,
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_dqr_domain  ON data_quality_rules(domain);
CREATE INDEX IF NOT EXISTS idx_dqr_enabled ON data_quality_rules(enabled);

CREATE TABLE IF NOT EXISTS data_quality_results (
    result_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id       TEXT NOT NULL,
    domain        TEXT NOT NULL,
    check_type    TEXT NOT NULL,
    severity      TEXT NOT NULL,
    passed        INTEGER NOT NULL,
    measured      TEXT,
    expected      TEXT,
    error_message TEXT,
    duration_ms   INTEGER,
    run_at        TEXT NOT NULL DEFAULT (datetime('now')),
    sync_run_id   INTEGER,
    FOREIGN KEY (rule_id) REFERENCES data_quality_rules(rule_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_dqres_rule       ON data_quality_results(rule_id);
CREATE INDEX IF NOT EXISTS idx_dqres_domain_run ON data_quality_results(domain, run_at);
CREATE INDEX IF NOT EXISTS idx_dqres_failed     ON data_quality_results(passed, run_at) WHERE passed = 0;
"""


def init_quality_schema(con: sqlite3.Connection) -> None:
    """Create data_quality_rules + data_quality_results if absent.

    Idempotent. Safe to call from ``init_schema()`` at startup. The
    caller owns commit/transaction state; this function commits its
    own work because it runs at boot time, before any safe_writer
    block is active.
    """
    for stmt in QUALITY_SCHEMA_SQL.split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)
    con.commit()
