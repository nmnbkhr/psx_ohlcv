"""Pydantic response models for the v1 API surface.

Each route's response shape is defined in a submodule:

- ``common`` — shared types (FreshnessRow, ErrorResponse, df helpers)
- ``eod``    — EOD OHLCV row / breadth
- ``indices``— index latest / history / constituents
- ``market`` — denormalized hero + movers + sector / rates strip

The models are the public contract of the API. Field names match
``data_freshness`` / ``psx_indices`` / ``eod_ohlcv`` columns exactly so
``model_validate({**row_dict})`` works without remapping.
"""
