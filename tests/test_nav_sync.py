"""Tests for optimized bulk NAV sync: batch upsert, staging, resume."""

import json
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from pakfindata.db.repositories.fixed_income import (
    parse_nav_history_to_tuples,
    upsert_mf_nav_batch,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mem_con():
    """In-memory SQLite connection with mutual_fund_nav table."""
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.execute("""
        CREATE TABLE mutual_fund_nav (
            fund_id TEXT NOT NULL,
            date TEXT NOT NULL,
            nav REAL NOT NULL,
            offer_price REAL,
            redemption_price REAL,
            aum REAL,
            nav_change_pct REAL,
            source TEXT DEFAULT 'MUFAP',
            ingested_at TEXT,
            PRIMARY KEY(fund_id, date)
        )
    """)
    con.commit()
    return con


@pytest.fixture
def staging_dir(tmp_path):
    """Temporary staging directory."""
    d = tmp_path / "nav_staging"
    d.mkdir()
    return d


# ---------------------------------------------------------------------------
# parse_nav_history_to_tuples
# ---------------------------------------------------------------------------

class TestParseNavHistoryToTuples:

    def test_valid_records(self):
        history = [
            {"entryDate": "2025-01-15", "netval": 12.34, "OfferPrice": 12.50},
            {"CalDate": "2025-01-16T00:00:00", "netval": 12.40},
        ]
        rows = parse_nav_history_to_tuples("MUFAP:123", history)
        assert len(rows) == 2
        # First row
        assert rows[0][0] == "MUFAP:123"  # fund_id
        assert rows[0][1] == "2025-01-15"  # date
        assert rows[0][2] == 12.34  # nav
        assert rows[0][3] == 12.50  # offer_price
        assert rows[0][7] == "MUFAP"  # source
        # Second row — CalDate format, no offer/redemp → defaults to nav
        assert rows[1][1] == "2025-01-16"
        assert rows[1][3] == 12.40  # offer defaults to nav

    def test_missing_date_skipped(self):
        history = [{"netval": 10.0}]
        rows = parse_nav_history_to_tuples("MUFAP:X", history)
        assert len(rows) == 0

    def test_missing_nav_skipped(self):
        history = [{"entryDate": "2025-01-01"}]
        rows = parse_nav_history_to_tuples("MUFAP:X", history)
        assert len(rows) == 0

    def test_invalid_nav_skipped(self):
        history = [{"entryDate": "2025-01-01", "netval": "N/A"}]
        rows = parse_nav_history_to_tuples("MUFAP:X", history)
        assert len(rows) == 0

    def test_empty_history(self):
        rows = parse_nav_history_to_tuples("MUFAP:X", [])
        assert rows == []

    def test_date_truncation(self):
        history = [{"entryDate": "2025-03-20T12:30:00Z", "netval": 5.0}]
        rows = parse_nav_history_to_tuples("F", history)
        assert rows[0][1] == "2025-03-20"


# ---------------------------------------------------------------------------
# upsert_mf_nav_batch
# ---------------------------------------------------------------------------

class TestUpsertMfNavBatch:

    def test_batch_insert(self, mem_con):
        rows = [
            ("F1", "2025-01-01", 10.0, 10.1, 9.9, None, None, "MUFAP"),
            ("F1", "2025-01-02", 10.5, 10.6, 10.4, None, None, "MUFAP"),
        ]
        count = upsert_mf_nav_batch(mem_con, "F1", rows)
        assert count == 2
        result = mem_con.execute("SELECT COUNT(*) FROM mutual_fund_nav").fetchone()[0]
        assert result == 2

    def test_conflict_update(self, mem_con):
        rows1 = [("F1", "2025-01-01", 10.0, 10.1, 9.9, None, None, "MUFAP")]
        upsert_mf_nav_batch(mem_con, "F1", rows1)

        # Update same date with new NAV
        rows2 = [("F1", "2025-01-01", 11.0, 11.1, 10.9, None, None, "MUFAP")]
        upsert_mf_nav_batch(mem_con, "F1", rows2)

        row = mem_con.execute(
            "SELECT nav FROM mutual_fund_nav WHERE fund_id='F1' AND date='2025-01-01'"
        ).fetchone()
        assert row[0] == 11.0

    def test_empty_rows(self, mem_con):
        count = upsert_mf_nav_batch(mem_con, "F1", [])
        assert count == 0

    def test_multiple_funds(self, mem_con):
        rows = [
            ("F1", "2025-01-01", 10.0, 10.0, 10.0, None, None, "MUFAP"),
            ("F2", "2025-01-01", 20.0, 20.0, 20.0, None, None, "MUFAP"),
        ]
        count = upsert_mf_nav_batch(mem_con, "mixed", rows)
        assert count == 2


# ---------------------------------------------------------------------------
# Staging helpers
# ---------------------------------------------------------------------------

class TestStagingHelpers:

    def test_stage_read_delete_roundtrip(self, staging_dir):
        with patch("pakfindata.sync_mufap.NAV_STAGING_DIR", staging_dir):
            from pakfindata.sync_mufap import (
                _delete_staged_json,
                _read_staged_json,
                _stage_nav_json,
            )

            result = {"nav_history": [{"entryDate": "2025-01-01", "netval": 10}]}
            path = _stage_nav_json("MUFAP:100", "100", result)
            assert path.exists()

            data = _read_staged_json("MUFAP:100")
            assert data is not None
            assert data["fund_id"] == "MUFAP:100"
            assert data["data"]["nav_history"][0]["netval"] == 10

            _delete_staged_json("MUFAP:100")
            assert not path.exists()
            assert _read_staged_json("MUFAP:100") is None

    def test_get_staged_fund_ids(self, staging_dir):
        with patch("pakfindata.sync_mufap.NAV_STAGING_DIR", staging_dir):
            from pakfindata.sync_mufap import _get_staged_fund_ids, _stage_nav_json

            _stage_nav_json("MUFAP:A", "A", {"nav_history": []})
            _stage_nav_json("MUFAP:B", "B", {"nav_history": []})

            ids = _get_staged_fund_ids()
            assert ids == {"MUFAP:A", "MUFAP:B"}

    def test_clear_nav_staging(self, staging_dir):
        with patch("pakfindata.sync_mufap.NAV_STAGING_DIR", staging_dir):
            from pakfindata.sync_mufap import _stage_nav_json, clear_nav_staging

            _stage_nav_json("MUFAP:X", "X", {})
            _stage_nav_json("MUFAP:Y", "Y", {})
            count = clear_nav_staging()
            assert count == 2
            assert list(staging_dir.glob("*.json")) == []

    def test_read_nonexistent_returns_none(self, staging_dir):
        with patch("pakfindata.sync_mufap.NAV_STAGING_DIR", staging_dir):
            from pakfindata.sync_mufap import _read_staged_json

            assert _read_staged_json("MUFAP:NOPE") is None

    def test_clear_empty_dir(self, staging_dir):
        with patch("pakfindata.sync_mufap.NAV_STAGING_DIR", staging_dir):
            from pakfindata.sync_mufap import clear_nav_staging

            assert clear_nav_staging() == 0
