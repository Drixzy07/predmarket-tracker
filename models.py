"""
Normalized storage model. A Position is platform-agnostic: it references a
market by (platform, market_id, outcome) and stores your entry price as an
implied probability per contract (0..1). Bots "construct" portfolios by
creating positions here via the API.

The optional `group` tag lets you roll up exposure across platforms without
solving full cross-platform identity matching: tag every position you consider
"the same bet" with the same group label (e.g. "fed-cut-march").
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlmodel import Field, SQLModel


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Portfolio(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    owner: str = "default"            # e.g. a bot id, so each bot owns its books
    created_at: datetime = Field(default_factory=_now)


class Position(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    portfolio_id: int = Field(foreign_key="portfolio.id", index=True)
    platform: str                     # "polymarket" | "kalshi" | "manifold"
    market_id: str                    # numeric id / slug / ticker per platform
    outcome: str                      # "YES" | "NO" | a specific outcome label
    quantity: float                   # number of contracts / shares held
    avg_price: float                  # entry implied prob per contract (0..1)
    currency: str = "USD"             # "USDC" | "USD" | "MANA"
    status: str = "open"              # "open" | "settled"
    group: Optional[str] = Field(default=None, index=True)  # cross-platform tag
    opened_at: datetime = Field(default_factory=_now)


class PriceSnapshot(SQLModel, table=True):
    """Optional history table; append a row per (market, outcome) on each sync."""
    id: Optional[int] = Field(default=None, primary_key=True)
    platform: str = Field(index=True)
    market_id: str = Field(index=True)
    outcome: str
    implied_prob: float
    ts: datetime = Field(default_factory=_now, index=True)


class PortfolioSnapshot(SQLModel, table=True):
    """One row per valuation of a portfolio, used to draw the value-over-time chart."""
    id: Optional[int] = Field(default=None, primary_key=True)
    portfolio_id: int = Field(foreign_key="portfolio.id", index=True)
    ts: datetime = Field(default_factory=_now, index=True)
    mv_usd: float = 0.0       # market value in USD/USDC
    pnl_usd: float = 0.0      # unrealized P&L in USD/USDC
    mv_mana: float = 0.0      # market value in Manifold play money
