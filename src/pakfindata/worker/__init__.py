"""pakfindata worker package.

Single-process job dispatcher. Polls the ``jobs`` table, looks up the
handler in :mod:`pakfindata.worker.registry`, and runs it. All writes
go through ``pakfindata.db.safe_writer`` — the worker is a sibling of
the Phase 0 sync paths, not a bypass.

Entrypoint: ``python -m pakfindata.worker.main`` (or the systemd
``pakfindata-worker.service``).
"""
