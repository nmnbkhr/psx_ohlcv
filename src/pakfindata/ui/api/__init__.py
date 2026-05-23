"""Streamlit-side API client wrapper.

Every Wave A UI page that has been migrated under Phase 1.3 imports
from this package, not from ``pakfindata.api_client`` (the smart
client that auto-falls-back to direct DB).

The wrapper is intentionally a flat module of per-data-shape functions:

    from pakfindata.ui.api import client
    breadth = client.get_breadth()
    movers = client.get_top_gainers(limit=5)

so pages stay readable and the page-level diff to migrate stays small.
"""
