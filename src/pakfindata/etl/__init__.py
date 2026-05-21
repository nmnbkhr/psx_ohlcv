"""Shared ETL entry points.

Each module here exposes a single ``sync()`` function (or similar)
that runs one ETL path end-to-end. Used by three call sites:

- CLI handlers (cron + manual `pfsync …` invocations)
- Worker handlers (UI button → enqueued job → worker dispatch)
- Direct programmatic callers (tests, REPL)

Each ``sync()`` is responsible for:

1. Fetching upstream data (HTTP, file read, etc.) **outside** the
   safe_writer block.
2. Writing rows + updating ``data_freshness`` **inside** the same
   safe_writer block.
3. On exception: recording a catalog-failure row and re-raising so the
   caller can decide how to report.
4. Returning a JSON-serializable result dict on success.

Phase 1.5 introduces the first such module (``etl.indices``). Phase
1.6 fills in the rest.
"""
