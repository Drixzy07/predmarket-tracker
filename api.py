"""
The tracking API. This is what your bots and dashboard talk to.

Run it:
    pip install -r requirements.txt
    uvicorn api:app --reload

Then open http://127.0.0.1:8000/docs for the interactive API.

Core flow (a bot "constructs" a portfolio):
    1. POST /portfolios                      -> create a book
    2. POST /portfolios/{id}/positions       -> add each position
    3. GET  /portfolios/{id}                  -> live valuation + P&L + exposure
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import timezone
from pathlib import Path
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlmodel import Session, SQLModel, create_engine, select

import os

import connectors
import portfolio as pf
from models import Portfolio, Position, PriceSnapshot, PortfolioSnapshot

# Use the cloud database if one is provided (DATABASE_URL), otherwise a local
# SQLite file. Cloud hosts wipe local files, so persistent data lives in Postgres.
DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///tracker.db")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
_connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
engine = create_engine(DATABASE_URL, connect_args=_connect_args, pool_pre_ping=True)

# Create tables eagerly so the app works no matter how it's started.
SQLModel.metadata.create_all(engine)


@asynccontextmanager
async def lifespan(app: FastAPI):
    SQLModel.metadata.create_all(engine)
    yield


app = FastAPI(title="Prediction Market Portfolio Tracker", lifespan=lifespan)

STATIC = Path(__file__).parent / "static"


@app.get("/")
def index():
    """Serve the dashboard so the whole app runs as one site at this URL."""
    return FileResponse(STATIC / "index.html")


# ---- request/response schemas ------------------------------------------------

class PortfolioIn(BaseModel):
    name: str
    owner: str = "default"


class PositionIn(BaseModel):
    platform: str
    market_id: str
    outcome: str = "YES"
    quantity: float
    avg_price: float            # entry implied prob per contract, 0..1
    currency: str = "USD"
    group: Optional[str] = None


# ---- helpers -----------------------------------------------------------------

async def _quotes_for(positions: list[Position]) -> dict:
    async with httpx.AsyncClient(timeout=connectors._HTTP_TIMEOUT) as client:
        results = await asyncio.gather(*[
            connectors.fetch_quote(client, p.platform, p.market_id, p.outcome)
            for p in positions
        ])
    return {p.id: q for p, q in zip(positions, results)}


def _get_portfolio(session: Session, pid: int) -> Portfolio:
    obj = session.get(Portfolio, pid)
    if obj is None:
        raise HTTPException(404, f"portfolio {pid} not found")
    return obj


# ---- endpoints ---------------------------------------------------------------

@app.post("/portfolios")
def create_portfolio(body: PortfolioIn):
    with Session(engine) as session:
        obj = Portfolio(name=body.name, owner=body.owner)
        session.add(obj)
        session.commit()
        session.refresh(obj)
        return obj


@app.get("/portfolios")
def list_portfolios(owner: Optional[str] = None):
    with Session(engine) as session:
        stmt = select(Portfolio)
        if owner:
            stmt = stmt.where(Portfolio.owner == owner)
        return session.exec(stmt).all()


@app.post("/portfolios/{pid}/positions")
def add_position(pid: int, body: PositionIn):
    with Session(engine) as session:
        _get_portfolio(session, pid)
        pos = Position(portfolio_id=pid, **body.model_dump())
        session.add(pos)
        session.commit()
        session.refresh(pos)
        return pos


def _iso_utc(dt) -> str:
    """Return an ISO timestamp tagged as UTC so the browser converts it to the
    viewer's local time. SQLite hands datetimes back without tzinfo, so we tag
    them here (the stored values are UTC)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _record_snapshot(pid: int, result: dict) -> None:
    """Best-effort: store the current totals so we can chart value over time."""
    try:
        usd = result["totals_by_currency"].get("USD", {})
        mana = result["totals_by_currency"].get("MANA", {})
        with Session(engine) as session:
            session.add(PortfolioSnapshot(
                portfolio_id=pid,
                mv_usd=usd.get("market_value", 0.0),
                pnl_usd=usd.get("unrealized_pnl", 0.0),
                mv_mana=mana.get("market_value", 0.0),
            ))
            session.commit()
    except Exception:
        pass  # never let charting break the main read


def _record_price_snapshots(rows) -> None:
    """Best-effort: store each position's current probability for per-position charts."""
    try:
        with Session(engine) as session:
            for r in rows:
                if r.get("current_price") is not None:
                    session.add(PriceSnapshot(
                        platform=r["platform"], market_id=r["market_id"],
                        outcome=r["outcome"], implied_prob=r["current_price"],
                    ))
            session.commit()
    except Exception:
        pass


@app.get("/portfolios/{pid}")
async def get_portfolio(pid: int):
    with Session(engine) as session:
        _get_portfolio(session, pid)
        positions = session.exec(
            select(Position).where(Position.portfolio_id == pid)
        ).all()
    quotes = await _quotes_for(positions)
    result = pf.value_portfolio(positions, quotes)
    _record_snapshot(pid, result)
    _record_price_snapshots(result["positions"])
    return result


@app.get("/portfolios/{pid}/history")
def portfolio_history(pid: int, limit: int = 500):
    """Value-over-time points for the chart, oldest first."""
    with Session(engine) as session:
        _get_portfolio(session, pid)
        rows = session.exec(
            select(PortfolioSnapshot)
            .where(PortfolioSnapshot.portfolio_id == pid)
            .order_by(PortfolioSnapshot.ts.desc())
            .limit(limit)
        ).all()
    rows = list(reversed(rows))
    return [
        {"ts": _iso_utc(r.ts), "mv_usd": r.mv_usd, "pnl_usd": r.pnl_usd, "mv_mana": r.mv_mana}
        for r in rows
    ]


@app.get("/positions/{position_id}/history")
def position_history(position_id: int, limit: int = 500):
    """Probability/value/P&L over time for one position, oldest first."""
    with Session(engine) as session:
        pos = session.get(Position, position_id)
        if pos is None:
            raise HTTPException(404, f"position {position_id} not found")
        qty, avg = pos.quantity, pos.avg_price
        meta = {
            "platform": pos.platform, "market_id": pos.market_id, "outcome": pos.outcome,
            "currency": pos.currency, "quantity": qty, "avg_price": avg,
        }
        rows = session.exec(
            select(PriceSnapshot)
            .where(PriceSnapshot.platform == pos.platform)
            .where(PriceSnapshot.market_id == pos.market_id)
            .where(PriceSnapshot.outcome == pos.outcome)
            .order_by(PriceSnapshot.ts.desc())
            .limit(limit)
        ).all()
        rows = list(reversed(rows))
        points = [
            {"ts": _iso_utc(s.ts), "prob": s.implied_prob,
             "value": qty * s.implied_prob, "pnl": (s.implied_prob - avg) * qty}
            for s in rows
        ]
    return {"position": meta, "points": points}


@app.delete("/positions/{position_id}")
def delete_position(position_id: int):
    with Session(engine) as session:
        pos = session.get(Position, position_id)
        if pos is None:
            raise HTTPException(404, f"position {position_id} not found")
        session.delete(pos)
        session.commit()
    return {"deleted": position_id}


@app.delete("/portfolios/{pid}")
def delete_portfolio(pid: int):
    with Session(engine) as session:
        obj = session.get(Portfolio, pid)
        if obj is None:
            raise HTTPException(404, f"portfolio {pid} not found")
        for pos in session.exec(select(Position).where(Position.portfolio_id == pid)).all():
            session.delete(pos)
        for snap in session.exec(select(PortfolioSnapshot).where(PortfolioSnapshot.portfolio_id == pid)).all():
            session.delete(snap)
        session.delete(obj)
        session.commit()
    return {"deleted": pid}


@app.get("/markets/{platform}/{market_id}")
async def get_market_quote(platform: str, market_id: str, outcome: str = "YES"):
    """Discovery / sanity check: live quote for a single market."""
    async with httpx.AsyncClient(timeout=connectors._HTTP_TIMEOUT) as client:
        q = await connectors.fetch_quote(client, platform, market_id, outcome)
    return q.__dict__


@app.get("/debug/metaculus")
async def debug_metaculus():
    """Diagnostic: shows exactly what the live Metaculus API returns, so we can see
    why browse is thin. Visit this URL in your browser and share the output."""
    info = {"build": getattr(connectors, "METACULUS_BUILD", "UNKNOWN-OLD-VERSION")}
    tok = connectors._metaculus_token()
    info["token"] = f"set ({len(tok)} chars)" if tok else "MISSING"
    m = connectors.REGISTRY.get("metaculus")
    async with httpx.AsyncClient(timeout=connectors._HTTP_TIMEOUT) as client:
        # 1) raw activity-ordered list call
        try:
            st, body = await m._get(client, f"{connectors.METACULUS}/api2/questions/",
                                    {"order_by": "-activity", "forecast_type": "binary",
                                     "status": "open", "type": "forecast", "limit": "20"})
            info["api2_status"] = st
            if isinstance(body, dict):
                results = body.get("results") or []
                info["api2_count_field"] = body.get("count")
                info["api2_num_results"] = len(results)
                with_cp, samples = 0, []
                for it in results:
                    node = m._node(it)
                    cp = m._community_prob(node, parent=it)
                    if cp is not None:
                        with_cp += 1
                    if len(samples) < 4:
                        agg = node.get("aggregations")
                        samples.append({
                            "title": (it.get("title") or "")[:45],
                            "type": node.get("type"),
                            "cp": cp,
                            "agg_keys": list(agg.keys()) if isinstance(agg, dict) else str(type(agg).__name__),
                        })
                info["api2_num_with_cp"] = with_cp
                info["samples"] = samples
            else:
                info["api2_body_type"] = type(body).__name__
        except Exception as exc:  # noqa: BLE001
            info["api2_error"] = f"{type(exc).__name__}: {exc}"
        # 2) what browse() actually returns
        try:
            res = await connectors.browse_markets(client, "metaculus", "", 30, cp_only=True)
            info["browse_returned"] = len(res.get("markets", []))
            info["browse_error"] = res.get("error")
        except Exception as exc:  # noqa: BLE001
            info["browse_error"] = f"{type(exc).__name__}: {exc}"
    return info


@app.get("/browse/{platform}")
async def browse(platform: str, q: str = "", limit: int = 30, cp_only: str = "true"):
    """List markets from a platform (optional text search) so you can discover
    and track them. cp_only (Metaculus) hides questions with no community forecast.
    Returns {"markets": [...], "error": optional}."""
    cp_only_flag = str(cp_only).strip().lower() not in ("false", "0", "no", "off", "")
    async with httpx.AsyncClient(timeout=connectors._HTTP_TIMEOUT) as client:
        return await connectors.browse_markets(client, platform, q, limit, cp_only=cp_only_flag)


@app.post("/snapshots/run")
async def run_snapshots():
    """Record one price snapshot per distinct (platform, market_id, outcome).
    Schedule this (cron / Temporal / Celery beat) to build price history."""
    with Session(engine) as session:
        positions = session.exec(select(Position)).all()
    keys = {(p.platform, p.market_id, p.outcome) for p in positions}
    written = 0
    async with httpx.AsyncClient(timeout=connectors._HTTP_TIMEOUT) as client:
        results = await asyncio.gather(*[
            connectors.fetch_quote(client, plat, mid, out) for plat, mid, out in keys
        ])
    with Session(engine) as session:
        for q in results:
            if q.implied_prob is not None:
                session.add(PriceSnapshot(
                    platform=q.platform, market_id=q.market_id,
                    outcome=q.outcome, implied_prob=q.implied_prob,
                ))
                written += 1
        session.commit()
    return {"snapshots_written": written}
