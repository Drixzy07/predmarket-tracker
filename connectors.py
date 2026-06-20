"""
Read-only connectors for prediction-market platforms.

Each connector returns a normalized Quote with the current implied probability
(0..1) of one outcome, plus the market's full list of valid outcomes.

You can identify a market by pasting its FULL web address, or just the
slug/id/ticker. Each connector strips a pasted URL down to what it needs.

All endpoints used are PUBLIC. No trading credentials are needed.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import httpx

GAMMA = "https://gamma-api.polymarket.com"
KALSHI = "https://external-api.kalshi.com/trade-api/v2"
MANIFOLD = "https://api.manifold.markets/v0"

_HTTP_TIMEOUT = httpx.Timeout(10.0)


@dataclass
class Quote:
    platform: str
    market_id: str
    outcome: str
    implied_prob: Optional[float]
    currency: str
    title: Optional[str] = None
    resolved: bool = False
    resolution: Optional[str] = None
    outcomes: Optional[list] = None
    ts: datetime = None                  # type: ignore[assignment]
    error: Optional[str] = None

    def __post_init__(self):
        if self.ts is None:
            self.ts = datetime.now(timezone.utc)


def _as_list(value):
    """Polymarket sometimes JSON-encodes list fields as strings."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return [value]
    return value or []


def _clean_slug(raw) -> str:
    """Reduce a pasted URL or path to its last meaningful segment (the slug)."""
    s = str(raw).strip().split("#")[0].split("?")[0].rstrip("/")
    return s.split("/")[-1] if s else s


def _clean_kalshi_ticker(raw) -> str:
    """Pull the real market ticker out of a pasted Kalshi URL or string, e.g.
    'kxmenworldcup-26?op_market_ticker=KXMENWORLDCUP-26-ES' -> 'KXMENWORLDCUP-26-ES'.
    """
    s = str(raw).strip()
    m = re.search(r"op_market_ticker=([^&\s/]+)", s, re.I)
    if m:
        return m.group(1).upper()
    s = s.split("#")[0].split("?")[0].rstrip("/")
    return (s.split("/")[-1] if s else s).upper()


class PolymarketConnector:
    platform = "polymarket"
    currency = "USDC"

    async def get_quote(self, client: httpx.AsyncClient, market_id: str, outcome: str) -> Quote:
        market_id = _clean_slug(market_id)
        try:
            if str(market_id).isdigit():
                r = await client.get(f"{GAMMA}/markets/{market_id}")
                r.raise_for_status()
                data = r.json()
            else:
                r = await client.get(f"{GAMMA}/markets", params={"slug": market_id})
                r.raise_for_status()
                data = r.json()
                if isinstance(data, list) and not data:
                    # a polymarket.com/event/<slug> URL points at an event, not a market
                    data = await self._market_from_event(client, market_id)
            if isinstance(data, list):
                if not data:
                    return Quote(self.platform, market_id, outcome, None, self.currency,
                                 error="market not found (check the link)")
                data = data[0]
            if not isinstance(data, dict):
                return Quote(self.platform, market_id, outcome, None, self.currency,
                             error="market not found (check the link)")
            resolved_id = data.get("slug") or market_id
            outcomes = [str(o) for o in _as_list(data.get("outcomes"))]
            prices = _as_list(data.get("outcomePrices"))
            idx = next((i for i, o in enumerate(outcomes) if o.lower() == outcome.lower()), None)
            prob = float(prices[idx]) if idx is not None and idx < len(prices) else None
            err = None if (prob is not None or not outcomes) else "pick an outcome below"
            return Quote(
                platform=self.platform, market_id=resolved_id, outcome=outcome,
                implied_prob=prob, currency=self.currency,
                title=data.get("question"), outcomes=outcomes or None,
                resolved=bool(data.get("closed")),
                resolution=str(data.get("outcome")) if data.get("closed") else None,
                error=err,
            )
        except Exception as exc:  # noqa: BLE001
            return Quote(self.platform, market_id, outcome, None, self.currency,
                         error=f"could not reach Polymarket ({type(exc).__name__})")

    @staticmethod
    async def _market_from_event(client, slug):
        try:
            r = await client.get(f"{GAMMA}/events", params={"slug": slug})
            r.raise_for_status()
            ev = r.json()
            ev = ev[0] if isinstance(ev, list) and ev else ev
            mkts = ev.get("markets") if isinstance(ev, dict) else None
            return mkts[0] if mkts else None
        except Exception:  # noqa: BLE001
            return None

    async def browse(self, client, query, limit):
        params = {"closed": "false", "active": "true", "limit": str(max(limit, 40)),
                  "order": "volume24hr", "ascending": "false"}
        r = await client.get(f"{GAMMA}/markets", params=params)
        r.raise_for_status()
        j = r.json()
        out = []
        for m in (j if isinstance(j, list) else []):
            title = m.get("question") or ""
            if query and query.lower() not in title.lower():
                continue
            prices = _as_list(m.get("outcomePrices"))
            out.append({"platform": self.platform, "market_id": m.get("slug") or str(m.get("id")),
                        "title": title, "prob": (float(prices[0]) if prices else None),
                        "currency": self.currency, "volume": m.get("volume24hr") or m.get("volumeNum")})
            if len(out) >= limit:
                break
        return out


class KalshiConnector:
    platform = "kalshi"
    currency = "USD"

    async def get_quote(self, client: httpx.AsyncClient, market_id: str, outcome: str) -> Quote:
        market_id = _clean_kalshi_ticker(market_id)
        try:
            r = await client.get(f"{KALSHI}/markets/{market_id}")
            r.raise_for_status()
            m = r.json().get("market", {})
            yes = self._yes_price(m)
            prob = yes if outcome.upper() == "YES" else (1 - yes) if yes is not None else None
            status = (m.get("status") or "").lower()
            return Quote(
                platform=self.platform, market_id=market_id, outcome=outcome.upper(),
                implied_prob=prob, currency=self.currency,
                title=m.get("title"), outcomes=["YES", "NO"],
                resolved=status in ("settled", "finalized", "closed"),
                resolution=m.get("result") or None,
            )
        except Exception as exc:  # noqa: BLE001
            return Quote(self.platform, market_id, outcome.upper(), None, self.currency,
                         outcomes=["YES", "NO"],
                         error=f"could not reach Kalshi ({type(exc).__name__}) — is the ticker right?")

    @staticmethod
    def _yes_price(m: dict) -> Optional[float]:
        for field in ("last_price_dollars", "yes_bid_dollars"):
            v = m.get(field)
            if v is not None:
                try:
                    return float(v)
                except (TypeError, ValueError):
                    pass
        bid, ask = m.get("yes_bid"), m.get("yes_ask")
        if bid is not None and ask is not None and (bid or ask):
            return (bid + ask) / 200.0          # cents -> prob, market midpoint
        lp = m.get("last_price")
        if lp:
            return lp / 100.0
        if bid:
            return bid / 100.0
        if ask:
            return ask / 100.0
        return None

    @staticmethod
    def _label(event_title, m) -> str:
        sub = (m.get("yes_sub_title") or m.get("subtitle") or m.get("yes_subtitle") or "").strip()
        et = (event_title or "").strip()
        if et and sub and sub.lower() not in et.lower():
            return et + " \u2014 " + sub
        return et or sub or (m.get("ticker") or "")

    async def browse(self, client, query, limit):
        out = []
        try:
            r = await client.get(f"{KALSHI}/events",
                                 params={"limit": "200", "status": "open", "with_nested_markets": "true"})
            r.raise_for_status()
            for ev in r.json().get("events", []):
                ev_title = ev.get("title") or ""
                for m in (ev.get("markets") or []):
                    label = self._label(ev_title, m)
                    if query and query.lower() not in label.lower():
                        continue
                    out.append({"platform": self.platform, "market_id": m.get("ticker"),
                                "title": label, "prob": self._yes_price(m),
                                "currency": self.currency, "volume": m.get("volume")})
                    if len(out) >= limit:
                        return out
        except Exception:  # noqa: BLE001
            out = []
        if out:
            return out
        # fallback: flat markets list
        r = await client.get(f"{KALSHI}/markets", params={"limit": str(max(limit, 50)), "status": "open"})
        r.raise_for_status()
        for m in r.json().get("markets", []):
            label = self._label(m.get("title") or "", m)
            if query and query.lower() not in label.lower():
                continue
            out.append({"platform": self.platform, "market_id": m.get("ticker"),
                        "title": label, "prob": self._yes_price(m),
                        "currency": self.currency, "volume": m.get("volume")})
            if len(out) >= limit:
                break
        return out


class ManifoldConnector:
    platform = "manifold"
    currency = "MANA"

    async def get_quote(self, client: httpx.AsyncClient, market_id: str, outcome: str) -> Quote:
        market_id = _clean_slug(market_id)
        try:
            m = None
            for path in (f"{MANIFOLD}/slug/{market_id}", f"{MANIFOLD}/market/{market_id}"):
                r = await client.get(path)
                if r.status_code == 200:
                    m = r.json()
                    break
            if m is None:
                return Quote(self.platform, market_id, outcome, None, self.currency,
                             error="market not found (check the link)")

            answers = m.get("answers") or []
            is_multi = bool(answers) and m.get("outcomeType") != "BINARY"

            if is_multi:
                names = [str(a.get("text", "?")).strip() for a in answers]
                match = self._match_answer(answers, outcome)
                prob = match.get("probability") if match else None
                err = None if prob is not None else "pick an outcome below"
                return Quote(self.platform, market_id, outcome, prob, self.currency,
                             title=m.get("question"), outcomes=names,
                             resolved=bool(m.get("isResolved")),
                             resolution=m.get("resolution"), error=err)

            p = m.get("probability")
            prob = None if p is None else (p if outcome.upper() == "YES" else 1 - p)
            return Quote(self.platform, market_id, outcome, prob, self.currency,
                         title=m.get("question"), outcomes=["YES", "NO"],
                         resolved=bool(m.get("isResolved")),
                         resolution=m.get("resolution"))
        except Exception as exc:  # noqa: BLE001
            return Quote(self.platform, market_id, outcome, None, self.currency,
                         error=f"could not reach Manifold ({type(exc).__name__})")

    @staticmethod
    def _match_answer(answers, outcome):
        o = outcome.strip().lower()
        for a in answers:
            if str(a.get("text", "")).strip().lower() == o:
                return a
        for a in answers:
            t = str(a.get("text", "")).strip().lower()
            if o and (o in t or t in o):
                return a
        return None

    async def browse(self, client, query, limit):
        if query:
            r = await client.get(f"{MANIFOLD}/search-markets", params={"term": query, "limit": str(limit)})
        else:
            r = await client.get(f"{MANIFOLD}/markets", params={"limit": str(limit)})
        r.raise_for_status()
        j = r.json()
        out = []
        for m in (j if isinstance(j, list) else []):
            is_binary = m.get("outcomeType") == "BINARY" or ("probability" in m and not m.get("answers"))
            out.append({"platform": self.platform, "market_id": m.get("slug") or m.get("id"),
                        "title": m.get("question") or "",
                        "prob": (m.get("probability") if is_binary else None),
                        "currency": self.currency, "volume": m.get("volume")})
            if len(out) >= limit:
                break
        return out


REGISTRY = {c.platform: c() for c in (PolymarketConnector, KalshiConnector, ManifoldConnector)}


async def fetch_quote(client: httpx.AsyncClient, platform: str, market_id: str, outcome: str) -> Quote:
    conn = REGISTRY.get(platform)
    if conn is None:
        return Quote(platform, market_id, outcome, None, "USD",
                     error=f"unknown platform '{platform}'")
    return await conn.get_quote(client, market_id, outcome)


async def browse_markets(client: httpx.AsyncClient, platform: str, query: str, limit) -> dict:
    conn = REGISTRY.get(platform)
    if conn is None or not hasattr(conn, "browse"):
        return {"markets": [], "error": f"cannot browse '{platform}'"}
    try:
        n = min(max(int(limit), 1), 50)
        items = await conn.browse(client, query or "", n)
        return {"markets": items}
    except Exception as exc:  # noqa: BLE001
        return {"markets": [], "error": f"could not browse {platform} ({type(exc).__name__})"}
