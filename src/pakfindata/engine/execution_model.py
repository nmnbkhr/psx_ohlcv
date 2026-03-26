"""
Realistic Execution Model for PSX.

Models bid/ask spread, market impact, slippage, fill probability, and commission.

PSX Market Microstructure:
  - Tick size: Rs 0.01
  - Circuit breaker: +/-7.5% from previous close
  - Typical spread: 5-30 bps (liquid), 50-200 bps (illiquid)
  - Commission: 0.10-0.20% (broker-dependent)
  - Settlement: T+2
"""

from dataclasses import dataclass


@dataclass
class ExecutionResult:
    symbol: str
    side: str
    requested_shares: int
    filled_shares: int
    fill_price: float
    mid_price: float
    bid: float
    ask: float
    spread_bps: float
    slippage_bps: float
    total_cost_bps: float
    commission: float
    total_cost: float
    fill_rate: float
    reason: str


COMMISSION_RATE = 0.0015  # 0.15%


def simulate_execution(
    symbol: str, side: str, shares: int,
    bid: float, ask: float, bid_vol: int, ask_vol: int,
    daily_volume: int, price: float,
) -> ExecutionResult:
    """Simulate realistic PSX order execution."""
    mid = (bid + ask) / 2 if bid > 0 and ask > 0 else price

    if bid <= 0 or ask <= 0:
        if daily_volume > 1_000_000:
            est = 0.002
        elif daily_volume > 100_000:
            est = 0.005
        else:
            est = 0.015
        bid = price * (1 - est / 2)
        ask = price * (1 + est / 2)
        mid = price

    spread_bps = (ask - bid) / mid * 10000 if mid > 0 else 0

    if side == "BUY":
        available = ask_vol if ask_vol > 0 else max(daily_volume // 100, 1000)
    else:
        available = bid_vol if bid_vol > 0 else max(daily_volume // 100, 1000)

    fill_shares = min(shares, available)
    fill_rate = fill_shares / shares if shares > 0 else 0

    participation = fill_shares / max(available, 1)
    impact_bps = (participation ** 0.6) * 30

    if side == "BUY":
        fill_price = ask * (1 + impact_bps / 10000)
    else:
        fill_price = bid * (1 - impact_bps / 10000)

    if side == "BUY":
        total_cost_bps = (fill_price - mid) / mid * 10000
    else:
        total_cost_bps = (mid - fill_price) / mid * 10000

    commission = fill_shares * fill_price * COMMISSION_RATE
    total_cost = fill_shares * fill_price + (commission if side == "BUY" else -commission)

    reason = (f"{'ASK' if side == 'BUY' else 'BID'}={bid if side == 'SELL' else ask:.2f} "
              f"+ {impact_bps:.1f}bps impact "
              f"(participation {participation:.0%} of {available:,})")

    return ExecutionResult(
        symbol=symbol, side=side,
        requested_shares=shares, filled_shares=fill_shares,
        fill_price=round(fill_price, 2),
        mid_price=round(mid, 2), bid=round(bid, 2), ask=round(ask, 2),
        spread_bps=round(spread_bps, 1),
        slippage_bps=round(impact_bps, 1),
        total_cost_bps=round(total_cost_bps, 1),
        commission=round(commission, 2),
        total_cost=round(total_cost, 2),
        fill_rate=round(fill_rate, 3),
        reason=reason,
    )
