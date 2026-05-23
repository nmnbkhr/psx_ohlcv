"""Built-in job handlers.

Each handler is a function:

    def handle_<name>(**params) -> dict | None:
        ...

It runs in the worker process. May raise on failure; the main loop
catches and records ``status='failed'`` with the traceback. Returns a
result dict on success (or None — recorded as ``result={}``).

Real ETL handlers (sync_indices, rebuild_eod_summary, etc.) land in
Phase 1.5+. Today the only handler is ``ping``.
"""
