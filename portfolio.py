"""
Valuation engine. Re-prices each open position against a live Quote and rolls
the results up into per-currency totals plus per-group exposure.

Model: a binary prediction contract pays 1.0 of its settlement currency if the
held outcome occurs, else 0. So the current value per contract == the outcome's
current implied probability, and:
    market_value   = quantity * current_price
    cost_basis     = quantity * avg_price
    unrealized_pnl = market_value - cost_basis

USDC and USD are both treated as ~$1 and grouped as "USD". MANA (Manifold play
money) is kept in its own bucket and never summed into real-money totals.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Iterable

CURRENCY_BUCKET = {"USDC": "USD", "USD": "USD", "MANA": "MANA"}


def value_position(pos, quote) -> dict:
    price = quote.implied_prob if quote else None
    cost_basis = pos.quantity * pos.avg_price
    row = {
        "position_id": pos.id,
        "platform": pos.platform,
        "market_id": pos.market_id,
        "outcome": pos.outcome,
        "group": pos.group,
        "quantity": pos.quantity,
        "avg_price": pos.avg_price,
        "currency": pos.currency,
        "title": quote.title if quote else None,
        "url": getattr(quote, "url", None) if quote else None,
        "current_price": price,
        "cost_basis": round(cost_basis, 6),
        "market_value": None,
        "unrealized_pnl": None,
        "resolved": quote.resolved if quote else False,
        "error": quote.error if quote else "no quote",
    }
    if price is not None:
        market_value = pos.quantity * price
        row["market_value"] = round(market_value, 6)
        row["unrealized_pnl"] = round(market_value - cost_basis, 6)
    return row


def value_portfolio(positions: Iterable, quotes_by_pid: dict) -> dict:
    rows = [value_position(p, quotes_by_pid.get(p.id)) for p in positions]

    totals = defaultdict(lambda: {"market_value": 0.0, "cost_basis": 0.0, "unrealized_pnl": 0.0})
    exposure = defaultdict(lambda: defaultdict(float))  # group -> bucket -> market_value

    for r in rows:
        bucket = CURRENCY_BUCKET.get(r["currency"], r["currency"])
        if r["market_value"] is not None:
            t = totals[bucket]
            t["market_value"] += r["market_value"]
            t["cost_basis"] += r["cost_basis"]
            t["unrealized_pnl"] += r["unrealized_pnl"]
            if r["group"]:
                exposure[r["group"]][bucket] += r["market_value"]

    return {
        "positions": rows,
        "totals_by_currency": {
            k: {kk: round(vv, 6) for kk, vv in v.items()} for k, v in totals.items()
        },
        "exposure_by_group": {
            g: {b: round(v, 6) for b, v in buckets.items()} for g, buckets in exposure.items()
        },
    }
