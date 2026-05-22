"""Futures endpoint response models."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class FuturesContractRow(BaseModel):
    """One row from ``futures_eod`` — a single futures/CONT contract.

    Backs the futures-basis tile in signal_dashboard's intelligence
    brief: latest active contract per base symbol with close + volume +
    contract_month.
    """

    base_symbol: str
    symbol: str
    date: str
    market_type: str
    contract_month: Optional[str] = None
    close: Optional[float] = None
    volume: Optional[int] = None
