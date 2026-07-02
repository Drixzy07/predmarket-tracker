"""
Read-only connectors for prediction-market platforms.

Each connector returns a normalized Quote with the current implied probability
(0..1) of one outcome, the market's valid outcomes, and a link to the market's
public web page. Identify a market by pasting its FULL web address, or just the
slug / id / ticker.

All endpoints used are PUBLIC. No trading credentials are needed.
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import re
import time
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import httpx

GAMMA = "https://gamma-api.polymarket.com"
KALSHI = "https://external-api.kalshi.com/trade-api/v2"
_KALSHI_SERIES_CACHE = {"ts": 0.0, "list": [], "map": {}}   # /series data cached for search
MANIFOLD = "https://api.manifold.markets/v0"
METACULUS = "https://www.metaculus.com"
METACULUS_BUILD = "mc-embed-2026-06-28e"
_MC_QUOTE_CACHE: dict = {}   # (qid, outcome) -> (expiry_ts, Quote); eases Metaculus rate limits
_MC_QUOTE_TTL = 300.0


def _metaculus_token() -> str:
    """Metaculus locked its API behind a free account token. Read it from the
    METACULUS_TOKEN environment variable (set it in your host's env settings)."""
    return os.environ.get("METACULUS_TOKEN", "").strip()

_HTTP_TIMEOUT = httpx.Timeout(12.0)
_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}
_PAGE_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


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
    outcome_probs: Optional[dict] = None
    url: Optional[str] = None
    ts: datetime = None  # type: ignore[assignment]
    error: Optional[str] = None

    def __post_init__(self):
        if self.ts is None:
            self.ts = datetime.now(timezone.utc)


def _num(v):
    """Parse a number that may arrive as a JSON string (Polymarket/Kalshi do this)."""
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _as_list(value):
    """Polymarket sometimes JSON-encodes list fields as strings."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return [value]
    return value or []


def _clean_slug(raw) -> str:
    s = str(raw).strip().split("#")[0].split("?")[0].rstrip("/")
    return s.split("/")[-1] if s else s


def _clean_kalshi_ticker(raw) -> str:
    s = str(raw).strip()
    m = re.search(r"op_market_ticker=([^&\s/]+)", s, re.I)
    if m:
        return m.group(1).upper()
    s = s.split("#")[0].split("?")[0].rstrip("/")
    return (s.split("/")[-1] if s else s).upper()


def _clean_metaculus(raw) -> str:
    s = str(raw).strip()
    m = re.search(r"questions?/(\d+)", s)
    if m:
        return m.group(1)
    m = re.search(r"(\d+)", s)
    return m.group(1) if m else s


class PolymarketConnector:
    platform = "polymarket"
    currency = "USDC"

    @staticmethod
    def _url(slug):
        return f"https://polymarket.com/event/{slug}" if slug else "https://polymarket.com"

    async def get_quote(self, client: httpx.AsyncClient, market_id: str, outcome: str) -> Quote:
        slug = _clean_slug(market_id)
        url = self._url(slug)
        try:
            data = None
            if str(slug).isdigit():
                r = await client.get(f"{GAMMA}/markets/{slug}", headers=_HEADERS)
                r.raise_for_status()
                data = r.json()
            else:
                r = await client.get(f"{GAMMA}/markets", params={"slug": slug}, headers=_HEADERS)
                r.raise_for_status()
                data = r.json()
                if isinstance(data, list) and not data:
                    # A /event/<slug> URL. Multi-outcome events (e.g. "World Cup Winner") are a
                    # group of separate Yes/No sub-markets — expose each as its own trackable option.
                    ev = await self._fetch_event(client, slug)
                    if isinstance(ev, dict):
                        markets = ev.get("markets") or []
                        if len(markets) > 1:
                            return self._multi_quote(ev, markets, outcome, url)
                        data = markets[0] if markets else None
            if isinstance(data, list):
                data = data[0] if data else None
            if not isinstance(data, dict):
                return Quote(self.platform, slug, outcome, None, self.currency, url=url,
                             error="market not found (check the link)")
            evs = data.get("events")
            if isinstance(evs, list) and evs and isinstance(evs[0], dict) and evs[0].get("slug"):
                url = self._url(evs[0]["slug"])
            outcomes = [str(o) for o in _as_list(data.get("outcomes"))]
            prices = _as_list(data.get("outcomePrices"))
            # A single market can itself be multi-outcome (outcomes = [A, B, C, ...]); expose
            # per-option prices so each option shows a % and can be tracked.
            if len(outcomes) > 2:
                pairs = [(o, _num(prices[i])) for i, o in enumerate(outcomes) if i < len(prices)]
                pairs = [(o, p) for o, p in pairs if p is not None]
                pairs.sort(key=lambda x: x[1], reverse=True)
                labels = [o for o, _ in pairs]
                probs = {o: p for o, p in pairs}
                match = next((o for o in labels if o.lower() == outcome.lower()), None)
                prob = probs.get(match) if match else None
                return Quote(self.platform, data.get("slug") or slug, outcome, prob, self.currency,
                             title=data.get("question"), outcomes=labels, outcome_probs=probs, url=url,
                             resolved=bool(data.get("closed")),
                             error=None if prob is not None else "pick an option below")
            idx = next((i for i, o in enumerate(outcomes) if o.lower() == outcome.lower()), None)
            prob = _num(prices[idx]) if idx is not None and idx < len(prices) else None
            err = None if (prob is not None or not outcomes) else "pick an outcome below"
            return Quote(self.platform, data.get("slug") or slug, outcome, prob, self.currency,
                         title=data.get("question"), outcomes=outcomes or None, url=url,
                         resolved=bool(data.get("closed")),
                         resolution=str(data.get("outcome")) if data.get("closed") else None, error=err)
        except Exception as exc:  # noqa: BLE001
            return Quote(self.platform, slug, outcome, None, self.currency, url=url,
                         error=f"could not reach Polymarket ({type(exc).__name__})")

    @staticmethod
    async def _fetch_event(client, slug):
        try:
            r = await client.get(f"{GAMMA}/events", params={"slug": slug}, headers=_HEADERS)
            r.raise_for_status()
            ev = r.json()
            return ev[0] if isinstance(ev, list) and ev else (ev if isinstance(ev, dict) else None)
        except Exception:  # noqa: BLE001
            return None

    def _multi_quote(self, ev, markets, outcome, url):
        """A multi-outcome event = one Yes/No sub-market per option. Expose every priced option
        (with its Yes price), sorted most-likely first, so any of them can be tracked. Unpriced
        placeholder slots (Polymarket's unnamed "Team AL" etc.) are dropped."""
        pairs, seen = [], set()
        for m in markets:
            label = (m.get("groupItemTitle") or m.get("question") or "").strip()
            key = label.lower()
            if not label or key in seen:
                continue
            outs = [str(o).lower() for o in _as_list(m.get("outcomes"))]
            prices = _as_list(m.get("outcomePrices"))
            yi = outs.index("yes") if "yes" in outs else 0
            yes = _num(prices[yi]) if yi < len(prices) else None
            seen.add(key)
            if yes is not None:                       # skip unpriced placeholder slots
                pairs.append((label, yes))
        pairs.sort(key=lambda x: x[1], reverse=True)  # most likely first
        labels = [l for l, _ in pairs]
        probs = {l: p for l, p in pairs}
        prob = None
        if outcome:
            match = next((l for l in labels if l.lower() == outcome.lower()), None)
            prob = probs.get(match) if match else None
        err = None if prob is not None else "pick an option below to track it"
        return Quote(self.platform, ev.get("slug"), outcome, prob, self.currency,
                     title=ev.get("title"), outcomes=labels, outcome_probs=probs, url=url,
                     resolved=bool(ev.get("closed")), error=err)

    def _event_item(self, ev):
        slug = ev.get("slug")
        markets = ev.get("markets") or []
        prob = None
        if len(markets) == 1:
            prices = _as_list(markets[0].get("outcomePrices"))
            if prices:
                prob = _num(prices[0])
        return {"platform": self.platform, "market_id": slug, "title": ev.get("title") or "",
                "prob": prob, "currency": self.currency,
                "volume": _num(ev.get("volume24hr")) or _num(ev.get("volume")),
                "url": self._url(slug)}

    async def browse(self, client, query, limit):
        out = []
        try:
            if query:
                events = []
                try:
                    r = await client.get(f"{GAMMA}/public-search",
                                         params={"q": query, "limit_per_type": str(limit), "events_status": "active"},
                                         headers=_HEADERS)
                    r.raise_for_status()
                    events = (r.json() or {}).get("events") or []
                except Exception:  # noqa: BLE001
                    events = []
                if not events:  # fall back to filtering the active list
                    r = await client.get(f"{GAMMA}/events",
                                         params={"active": "true", "closed": "false", "limit": "200"},
                                         headers=_HEADERS)
                    r.raise_for_status()
                    ql = query.lower()
                    events = [e for e in (r.json() or [])
                              if ql in (e.get("title") or "").lower()
                              or any(ql in (m.get("question") or "").lower() for m in (e.get("markets") or []))]
            else:
                r = await client.get(f"{GAMMA}/events",
                                     params={"active": "true", "closed": "false", "limit": "80",
                                             "order": "volume24hr", "ascending": "false"},
                                     headers=_HEADERS)
                r.raise_for_status()
                events = r.json() or []
            for ev in events:
                if ev.get("slug"):
                    out.append(self._event_item(ev))
                if len(out) >= limit:
                    break
        except Exception:  # noqa: BLE001
            return out
        return out


class KalshiConnector:
    platform = "kalshi"
    currency = "USD"

    @staticmethod
    def _url(series_or_event):
        s = (series_or_event or "").split("-")[0].lower()
        return f"https://kalshi.com/markets/{s}" if s else "https://kalshi.com"

    async def get_quote(self, client: httpx.AsyncClient, market_id: str, outcome: str) -> Quote:
        ticker = _clean_kalshi_ticker(market_id)
        url = self._url(ticker)
        try:
            # 1) Try as a single market (binary Yes/No).
            r = await client.get(f"{KALSHI}/markets/{ticker}", headers=_HEADERS, timeout=15.0)
            if r.status_code == 200:
                m = (r.json() or {}).get("market")
                if isinstance(m, dict) and m.get("ticker"):
                    yes = self._yes_price(m)
                    prob = yes if outcome.upper() == "YES" else (1 - yes) if yes is not None else None
                    status = (m.get("status") or "").lower()
                    return Quote(self.platform, ticker, outcome.upper(), prob, self.currency,
                                 title=m.get("title"), outcomes=["YES", "NO"], url=url,
                                 resolved=status in ("settled", "finalized", "closed", "determined"),
                                 resolution=m.get("result") or None)
            # 2) Otherwise treat it as an EVENT (multi-outcome: one Yes/No sub-market per option).
            er = await client.get(f"{KALSHI}/events/{ticker}",
                                  params={"with_nested_markets": "true"}, headers=_HEADERS, timeout=15.0)
            if er.status_code == 200:
                body = er.json() or {}
                ev = body.get("event") or body
                markets = ev.get("markets") or []
                if markets:
                    return self._multi_quote(ev, markets, outcome, url)
            return Quote(self.platform, ticker, outcome.upper(), None, self.currency, url=url,
                         outcomes=["YES", "NO"], error="market not found (check the ticker/link)")
        except Exception as exc:  # noqa: BLE001
            return Quote(self.platform, ticker, outcome.upper(), None, self.currency, url=url,
                         outcomes=["YES", "NO"],
                         error=f"could not reach Kalshi ({type(exc).__name__}) — is the ticker right?")

    def _multi_quote(self, ev, markets, outcome, url):
        """A Kalshi event with many sub-markets (e.g. each World Cup team / each candidate).
        Expose every option (with its Yes price), sorted most-likely first, individually trackable."""
        pairs = []
        for m in markets:
            label = (m.get("yes_sub_title") or m.get("subtitle") or m.get("title") or "").strip()
            yes = self._yes_price(m)
            if not label or yes is None:
                continue
            pairs.append((label, yes))
        pairs.sort(key=lambda x: x[1], reverse=True)
        labels = [l for l, _ in pairs]
        probs = {l: p for l, p in pairs}
        prob = None
        if outcome:
            match = next((l for l in labels if l.lower() == outcome.lower()), None)
            prob = probs.get(match) if match else None
        err = None if prob is not None else "pick an option below to track it"
        return Quote(self.platform, ev.get("event_ticker") or ev.get("series_ticker"), outcome, prob,
                     self.currency, title=ev.get("title"), outcomes=labels, outcome_probs=probs, url=url,
                     resolved=bool(ev.get("closed")), error=err)

    @classmethod
    def _yes_price(cls, m: dict) -> Optional[float]:
        bid = _num(m.get("yes_bid_dollars"))
        ask = _num(m.get("yes_ask_dollars"))
        if bid is not None and ask is not None and (bid or ask):
            return (bid + ask) / 2
        last = _num(m.get("last_price_dollars"))
        if last:
            return last
        cbid, cask = m.get("yes_bid"), m.get("yes_ask")
        if cbid is not None and cask is not None and (cbid or cask):
            return (cbid + cask) / 200.0
        lp = m.get("last_price")
        if lp:
            return lp / 100.0
        return bid if bid is not None else ask

    @staticmethod
    def _label(event_title, m) -> str:
        sub = (m.get("yes_sub_title") or m.get("subtitle") or "").strip()
        et = (event_title or "").strip()
        if et and sub and sub.lower() not in et.lower():
            return et + " \u2014 " + sub
        return et or sub or (m.get("ticker") or "")

    @staticmethod
    def _matches(q, ev, markets, series_meta=None) -> bool:
        hay = f"{ev.get('title','')} {ev.get('sub_title','')} {ev.get('series_ticker','')} {ev.get('category','')}"
        if series_meta:
            hay += " " + series_meta.get(ev.get("series_ticker", ""), "")
        for m in markets:
            hay += " " + (m.get("yes_sub_title") or m.get("subtitle") or "") + " " + (m.get("title") or "")
        hay = hay.lower()
        return all(w in hay for w in q.split())   # every search word must appear somewhere

    async def _series_list(self, client):
        """One call to /series gives every series' human-readable title, category and tags."""
        now = time.time()
        if now - _KALSHI_SERIES_CACHE["ts"] < 21600 and _KALSHI_SERIES_CACHE["list"]:
            return _KALSHI_SERIES_CACHE["list"]
        try:
            r = await client.get(f"{KALSHI}/series", headers=_HEADERS, timeout=httpx.Timeout(45.0, connect=10.0))
            r.raise_for_status()
            lst = r.json().get("series", []) or []
            m = {s.get("ticker"): f"{s.get('title','')} {s.get('category','')} {' '.join(s.get('tags') or [])}".lower()
                 for s in lst if s.get("ticker")}
            if lst:
                _KALSHI_SERIES_CACHE.update(ts=now, list=lst, map=m)
            return lst
        except Exception:  # noqa: BLE001
            return _KALSHI_SERIES_CACHE.get("list", [])

    async def _series_meta(self, client):
        await self._series_list(client)
        return _KALSHI_SERIES_CACHE.get("map", {})

    async def _search_via_series(self, client, q, limit):
        """Targeted search: find the SERIES whose title/category/tags match the query (e.g.
        'World Cup Advance', category 'World Soccer Cup'), then pull those series' markets DIRECTLY
        via /markets?series_ticker=... and group them by event — so match games titled 'Spain vs
        Austria' are found even though 'world cup' isn't in their title. Fully defensive: any
        single failed call is skipped rather than aborting the whole search."""
        words = q.split()
        try:
            series = await self._series_list(client)
        except Exception:  # noqa: BLE001
            series = []
        matched = [s for s in series
                   if all(w in f"{s.get('title','')} {s.get('category','')} {' '.join(s.get('tags') or [])}".lower()
                          for w in words)]
        matched.sort(key=lambda s: _num(s.get("volume_fp")) or 0, reverse=True)
        groups = {}   # event_ticker -> {"title": str, "markets": [...]}
        for s in matched[:12]:
            for status in ("open", "unopened"):
                cursor = None
                for _ in range(2):
                    params = {"series_ticker": s.get("ticker"), "status": status, "limit": "200"}
                    if cursor:
                        params["cursor"] = cursor
                    try:
                        r = await client.get(f"{KALSHI}/markets", params=params, headers=_HEADERS, timeout=15.0)
                        r.raise_for_status()
                        body = r.json()
                    except Exception:  # noqa: BLE001
                        break
                    for m in body.get("markets", []):
                        if self._yes_price(m) is None:
                            continue
                        if (m.get("status") or "").lower() in ("settled", "finalized", "closed", "determined"):
                            continue
                        et = m.get("event_ticker") or m.get("ticker")
                        g = groups.setdefault(et, {"title": "", "markets": []})
                        g["markets"].append(m)
                        if not g["title"]:
                            g["title"] = m.get("title") or ""
                    cursor = body.get("cursor")
                    if not cursor:
                        break
            if len(groups) >= limit * 3:
                break
        rows = []
        for et, g in groups.items():
            ms = g["markets"]
            vol = sum((_num(m.get("volume_fp")) or _num(m.get("volume")) or 0) for m in ms)
            if len(ms) > 1:
                rows.append({"platform": self.platform, "market_id": et, "title": g["title"] or et,
                             "prob": None, "currency": self.currency, "volume": vol,
                             "url": self._url(et), "options": len(ms)})
            else:
                m = ms[0]
                rows.append({"platform": self.platform, "market_id": m.get("ticker"),
                             "title": self._label(g["title"], m), "prob": self._yes_price(m),
                             "currency": self.currency, "volume": vol, "url": self._url(et)})
        rows.sort(key=lambda x: x.get("volume") or 0, reverse=True)
        return rows[:limit]

    def _browse_row(self, ev, markets):
        ev_title = ev.get("title") or ""
        ev_ticker = ev.get("event_ticker") or ev.get("series_ticker")
        url = self._url(ev.get("series_ticker") or ev_ticker)
        vol = sum((_num(m.get("volume_fp")) or _num(m.get("volume")) or 0) for m in markets)
        if len(markets) > 1:
            # Multi-outcome event -> ONE row; Track opens the option list.
            return {"platform": self.platform, "market_id": ev_ticker, "title": ev_title,
                    "prob": None, "currency": self.currency, "volume": vol, "url": url,
                    "options": len(markets)}
        m = markets[0]
        return {"platform": self.platform, "market_id": m.get("ticker"),
                "title": self._label(ev_title, m), "prob": self._yes_price(m),
                "currency": self.currency, "volume": vol, "url": url}

    async def _collect_events(self, client, statuses, pages, q, limit, series_meta):
        collected, seen = [], set()
        for status in statuses:
            cursor = None
            for _ in range(pages):
                params = {"limit": "200", "with_nested_markets": "true"}
                if status:
                    params["status"] = status
                if cursor:
                    params["cursor"] = cursor
                r = await client.get(f"{KALSHI}/events", params=params, headers=_HEADERS, timeout=15.0)
                r.raise_for_status()
                body = r.json()
                for ev in body.get("events", []):
                    et = ev.get("event_ticker") or ev.get("series_ticker")
                    if et in seen:
                        continue
                    seen.add(et)
                    live = [m for m in (ev.get("markets") or [])
                            if (m.get("status") or "").lower() not in ("settled", "finalized", "closed", "determined")
                            and self._yes_price(m) is not None]
                    if live:
                        collected.append((ev, live))
                cursor = body.get("cursor")
                if not cursor:
                    break
                if q and sum(1 for ev, ms in collected if self._matches(q, ev, ms, series_meta)) >= limit * 2:
                    break
        return collected

    async def browse(self, client, query, limit):
        q = (query or "").lower().strip()
        rows = []
        if q:
            try:
                rows = await self._search_via_series(client, q, limit)   # targeted (series → markets)
            except Exception:  # noqa: BLE001
                rows = []
        if not rows:
            try:
                series_meta = await self._series_meta(client) if q else {}
                statuses = ["open", "unopened"] if q else ["open"]
                collected = await self._collect_events(client, statuses, 8 if q else 2, q, limit, series_meta)
                rows = [self._browse_row(ev, ms) for ev, ms in collected
                        if not q or self._matches(q, ev, ms, series_meta)]
            except Exception:  # noqa: BLE001
                rows = []
        if rows:
            rows.sort(key=lambda x: x.get("volume") or 0, reverse=True)
            return rows[:limit]
        # Last-resort fallback: flat market list.
        out = []
        try:
            r = await client.get(f"{KALSHI}/markets", params={"limit": str(max(limit, 100)), "status": "open"},
                                 headers=_HEADERS, timeout=15.0)
            r.raise_for_status()
            for m in r.json().get("markets", []):
                label = self._label(m.get("title") or "", m)
                if q and q not in label.lower():
                    continue
                out.append({"platform": self.platform, "market_id": m.get("ticker"), "title": label,
                            "prob": self._yes_price(m), "currency": self.currency,
                            "volume": _num(m.get("volume_fp")) or _num(m.get("volume")),
                            "url": self._url(m.get("event_ticker"))})
                if len(out) >= limit:
                    break
        except Exception:  # noqa: BLE001
            out = []
        out.sort(key=lambda x: x.get("volume") or 0, reverse=True)
        return out[:limit]


class ManifoldConnector:
    platform = "manifold"
    currency = "MANA"

    async def get_quote(self, client: httpx.AsyncClient, market_id: str, outcome: str) -> Quote:
        slug = _clean_slug(market_id)
        url = f"https://manifold.markets/market/{slug}"
        try:
            m = None
            for path in (f"{MANIFOLD}/slug/{slug}", f"{MANIFOLD}/market/{slug}"):
                r = await client.get(path, headers=_HEADERS)
                if r.status_code == 200:
                    m = r.json()
                    break
            if m is None:
                return Quote(self.platform, slug, outcome, None, self.currency, url=url,
                             error="market not found (check the link)")
            url = m.get("url") or url
            answers = m.get("answers") or []
            is_multi = bool(answers) and m.get("outcomeType") != "BINARY"
            if is_multi:
                names = [str(a.get("text", "?")).strip() for a in answers]
                probs = {str(a.get("text", "?")).strip(): a.get("probability")
                         for a in answers if a.get("probability") is not None}
                match = self._match_answer(answers, outcome)
                prob = match.get("probability") if match else None
                err = None if prob is not None else "pick an option below to track it"
                return Quote(self.platform, slug, outcome, prob, self.currency, title=m.get("question"),
                             outcomes=names, outcome_probs=probs, url=url, resolved=bool(m.get("isResolved")),
                             resolution=m.get("resolution"), error=err)
            p = m.get("probability")
            prob = None if p is None else (p if outcome.upper() == "YES" else 1 - p)
            return Quote(self.platform, slug, outcome, prob, self.currency, title=m.get("question"),
                         outcomes=["YES", "NO"], url=url, resolved=bool(m.get("isResolved")),
                         resolution=m.get("resolution"))
        except Exception as exc:  # noqa: BLE001
            return Quote(self.platform, slug, outcome, None, self.currency, url=url,
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
            r = await client.get(f"{MANIFOLD}/search-markets", params={"term": query, "limit": str(limit)},
                                 headers=_HEADERS)
        else:
            r = await client.get(f"{MANIFOLD}/markets", params={"limit": str(limit)}, headers=_HEADERS)
        r.raise_for_status()
        j = r.json()
        out = []
        for m in (j if isinstance(j, list) else []):
            is_binary = m.get("outcomeType") == "BINARY" or ("probability" in m and not m.get("answers"))
            out.append({"platform": self.platform, "market_id": m.get("slug") or m.get("id"),
                        "title": m.get("question") or "", "prob": (m.get("probability") if is_binary else None),
                        "currency": self.currency, "volume": _num(m.get("volume")),
                        "url": m.get("url") or "https://manifold.markets"})
            if len(out) >= limit:
                break
        return out


class MetaculusConnector:
    platform = "metaculus"
    currency = "POINTS"

    @staticmethod
    def _url(qid):
        return f"https://www.metaculus.com/questions/{qid}/"

    @staticmethod
    def _node(data):
        """The dict carrying type/aggregations is top-level (api2) or under 'question' (posts API)."""
        if isinstance(data, dict) and isinstance(data.get("question"), dict):
            qn = data["question"]
            if qn.get("aggregations") or qn.get("type"):
                return qn
        return data if isinstance(data, dict) else {}

    @classmethod
    def _community_prob(cls, node, parent=None):
        """Extract community probability from all known locations in the API response."""
        # 1. aggregations.recency_weighted.latest.centers[0] (standard)
        for agg_key in ("recency_weighted", "metaculus_prediction", "unweighted", "single_aggregation"):
            agg = (node.get("aggregations") or {}).get(agg_key) or {}
            latest = agg.get("latest")
            if isinstance(latest, dict):
                centers = latest.get("centers")
                if centers:
                    try:
                        return float(centers[0])
                    except (TypeError, ValueError, IndexError):
                        pass
            hist = agg.get("history") or []
            if hist and isinstance(hist[-1], dict):
                centers = hist[-1].get("centers")
                if centers:
                    try:
                        return float(centers[0])
                    except (TypeError, ValueError, IndexError):
                        pass
        # 2. direct community_prediction field (api2/questions list items)
        for src in (node, parent or {}):
            cp = src.get("community_prediction")
            if cp is not None:
                if isinstance(cp, (int, float)):
                    return float(cp)
                if isinstance(cp, dict):
                    for k in ("full", "q2", "median"):
                        v = cp.get(k)
                        if isinstance(v, (int, float)):
                            return float(v)
                        if isinstance(v, dict):
                            for kk in ("q2", "median", "centers"):
                                vv = v.get(kk)
                                if isinstance(vv, (int, float)):
                                    return float(vv)
                                if isinstance(vv, list) and vv:
                                    try:
                                        return float(vv[0])
                                    except (TypeError, ValueError):
                                        pass
        # 3. forecasts_count > 0 but no probability found -> might be in a different format
        # Just return None; the caller shows a message
        return None

    @staticmethod
    async def _scrape_cp(client, qid):
        """Read the community forecast from Metaculus's PUBLIC embed endpoint. Embeds are built
        for third-party sites, so this is anonymous, lightweight, and shows the number as
        'NN.N%chance' — exactly what a person sees, with no auth or redirect issues."""
        for url in (f"https://www.metaculus.com/questions/embed/{qid}/",
                    f"https://www.metaculus.com/questions/{qid}/"):
            try:
                r = await client.get(url, headers=_PAGE_HEADERS, timeout=15.0, follow_redirects=True)
            except Exception:  # noqa: BLE001
                continue
            if r.status_code != 200 or not r.text:
                continue
            html = r.text
            # "NN.N%chance" is the community prediction. The chars between % and "chance" may be
            # markup but never another digit, so we won't grab the "NN% this week" delta by mistake.
            m = re.search(r'(\d{1,3}(?:\.\d+)?)\s*%[^0-9%]{0,40}?chance', html, re.IGNORECASE)
            if m:
                try:
                    v = float(m.group(1)) / 100.0
                    if 0.0 <= v <= 1.0:
                        return v
                except ValueError:
                    pass
            # Fallback: the embedded post JSON (recency_weighted -> latest -> centers).
            for pat in (r'recency_weighted\\?"\s*:\s*\{.*?latest\\?"\s*:\s*\{.*?centers\\?"\s*:\s*\[\s*([0-9.]+)',
                        r'latest\\?"\s*:\s*\{.*?centers\\?"\s*:\s*\[\s*([0-9.]+)'):
                mm = re.search(pat, html, re.DOTALL)
                if mm:
                    try:
                        v = float(mm.group(1))
                        if 0.0 <= v <= 1.0:
                            return v
                    except ValueError:
                        pass
        return None

    @staticmethod
    async def _get(client, url, params, anon=False):
        """Return (status_code, json_or_None).

        IMPORTANT: Metaculus HIDES the community prediction from an authenticated token whose
        account hasn't forecasted the question (the same "predict before you can see it" rule the
        website applies). Their own frontend fetches the forecast with the auth header stripped
        (passAuthHeader: false). So community-prediction reads must be anonymous (anon=True); the
        token is only useful as a fallback for reaching the data at all."""
        headers = dict(_HEADERS)
        if not anon:
            token = _metaculus_token()
            if token:
                headers["Authorization"] = f"Token {token}"
        try:
            r = await client.get(url, params=params, headers=headers, timeout=10.0)
            try:
                body = r.json()
            except Exception:  # noqa: BLE001
                body = None
            return r.status_code, body
        except Exception:  # noqa: BLE001
            return None, None

    @staticmethod
    def _auth_error(codes):
        """Build a helpful message depending on whether a token is even configured."""
        seen = ", ".join(str(c) for c in codes if c is not None) or "no response"
        tok = _metaculus_token()
        token_info = f"token set ({len(tok)} chars, starts '{tok[:4]}…')" if tok else "NO token set"
        if not tok:
            return (f"Metaculus requires an API token ({token_info}). "
                    f"Set METACULUS_TOKEN in Render Environment Variables and redeploy. "
                    f"(HTTP {seen})")
        if any(c in (401, 403) for c in codes):
            return (f"Metaculus rejected the request (HTTP {seen}; {token_info}). "
                    f"Check the METACULUS_TOKEN value has no extra spaces.")
        return f"Metaculus did not return data (HTTP {seen}; {token_info})."

    @classmethod
    def _mc_options_probs(cls, node):
        """For multiple-choice: return (option_labels, {label: prob})."""
        raw = node.get("options") or []
        labels = []
        for o in raw:
            if isinstance(o, dict):
                labels.append(str(o.get("label") or o.get("name") or o.get("text") or o))
            else:
                labels.append(str(o))
        agg = (node.get("aggregations") or {}).get("recency_weighted") or {}
        latest = agg.get("latest") or {}
        fv = latest.get("forecast_values") or latest.get("centers") or []
        probs = {}
        for i, lab in enumerate(labels):
            if i < len(fv):
                try:
                    probs[lab] = float(fv[i])
                except (TypeError, ValueError):
                    pass
        return labels, probs

    async def get_quote(self, client: httpx.AsyncClient, market_id: str, outcome: str) -> Quote:
        # Short cache: Metaculus rate-limits hard, and the live portfolio refresh re-queries
        # every position. Community forecasts move slowly, so a ~45s cache is plenty fresh.
        qid = _clean_metaculus(market_id)
        key = (qid, (outcome or "").upper())
        now = time.time()
        hit = _MC_QUOTE_CACHE.get(key)
        if hit and hit[0] > now:
            return hit[1]
        q = await self._get_quote_impl(client, qid, outcome)
        if q is not None and q.error is None:
            _MC_QUOTE_CACHE[key] = (now + _MC_QUOTE_TTL, q)
        return q

    async def _get_quote_impl(self, client: httpx.AsyncClient, qid: str, outcome: str) -> Quote:
        url = self._url(qid)
        codes, data = [], None
        cp_override = None
        # Sources of the community forecast, in order. Anonymous FIRST: Metaculus's own frontend
        # fetches the forecast with auth stripped (passAuthHeader:false) because a token tied to a
        # non-forecasting account makes the site HIDE the number. Token is only a metadata fallback.
        for endpoint, anon in ((f"{METACULUS}/api/posts/{qid}/", True),
                               (f"{METACULUS}/api2/questions/{qid}/", True),
                               (f"{METACULUS}/api/posts/{qid}/", False)):
            for attempt in range(2):
                st, body = await self._get(client, endpoint, {"with_cp": "true", "include_conditional_cps": "true"}, anon=anon)
                codes.append(st)
                if st == 200 and body:
                    if data is None:
                        data = body
                    if self._community_prob(self._node(body), parent=body) is not None:
                        data = body
                        break
                if st == 429:
                    await asyncio.sleep(1.3 * (attempt + 1))
                    continue
                break
            if data and self._community_prob(self._node(data), parent=data) is not None:
                break
        # Scrape the public page whenever the API didn't give us a forecast (throttled or hidden).
        # The page renders the same number a person sees, anonymously.
        if data is None or self._community_prob(self._node(data), parent=data) is None:
            cp_override = await self._scrape_cp(client, qid)
        if not data and cp_override is None:
            return Quote(self.platform, qid, outcome.upper(), None, self.currency, url=url,
                         outcomes=["YES", "NO"], error=self._auth_error(codes))
        if not data:
            # We have a scraped forecast but no metadata; still return a usable quote.
            prob = cp_override if outcome.upper() == "YES" else 1 - cp_override
            return Quote(self.platform, qid, outcome.upper(), prob, self.currency, title=None,
                         outcomes=["YES", "NO"], url=url)
        node = self._node(data)
        title = data.get("title") or node.get("title")
        qtype = (node.get("type") or node.get("question_type") or "").lower()
        resolved = bool(node.get("resolution")) or (node.get("status") in ("resolved", "closed"))
        # --- date / numeric: a distribution, not a single probability ---
        if qtype in ("date", "numeric", "continuous", "discrete"):
            return Quote(self.platform, qid, outcome, None, self.currency, title=title, url=url,
                         error=f"this is a {qtype} question (a forecast range, not yes/no) \u2014 "
                               f"only yes/no and multiple-choice questions can be tracked here")
        # --- multiple choice: each option has its own community probability ---
        if qtype == "multiple_choice":
            labels, probs = self._mc_options_probs(node)
            if not labels:
                return Quote(self.platform, qid, outcome, None, self.currency, title=title, url=url,
                             error="multiple-choice question (no options found)")
            if not probs:
                return Quote(self.platform, qid, outcome, None, self.currency, title=title, url=url,
                             outcomes=labels,
                             error="Metaculus hasn\u2019t revealed a community forecast for this question "
                                   "yet, so there\u2019s no number to track")
            p = probs.get(outcome)
            if p is None:
                for lab in labels:
                    if lab.lower() == outcome.strip().lower():
                        p = probs.get(lab)
                        break
            return Quote(self.platform, qid, outcome, p, self.currency, title=title, url=url,
                         outcomes=labels, outcome_probs=probs, resolved=resolved,
                         error=None if p is not None else "pick one of the outcomes below")
        # --- group / conditional posts (several sub-questions) ---
        sub_questions = data.get("group_of_questions") or data.get("sub_questions")
        if sub_questions and not qtype:
            subs = sub_questions.get("questions") if isinstance(sub_questions, dict) else sub_questions
            if isinstance(subs, list) and subs:
                names = [str(sq.get("title") or sq.get("label") or "?") for sq in subs]
                return Quote(self.platform, qid, outcome, None, self.currency, title=title, url=url,
                             outcomes=names, error="this is a group of questions \u2014 open it on Metaculus "
                             "and track each sub-question by its own URL")
        # --- binary (yes / no) ---
        cp = self._community_prob(node, parent=data)
        if cp is None and cp_override is not None:
            cp = cp_override
        prob = None if cp is None else (cp if outcome.upper() == "YES" else 1 - cp)
        err = None
        if cp is None:
            reveal = node.get("cp_reveal_time") or data.get("cp_reveal_time")
            future = False
            if reveal:
                try:
                    future = str(reveal)[:10] > datetime.now(timezone.utc).strftime("%Y-%m-%d")
                except Exception:  # noqa: BLE001
                    future = False
            if future:
                err = f"Metaculus hides the community forecast on this question until {str(reveal)[:10]}"
            else:
                err = "Metaculus isn\u2019t sharing a community forecast for this question through its API"
        return Quote(self.platform, qid, outcome.upper(), prob, self.currency, title=title,
                     outcomes=["YES", "NO"], url=url, resolved=resolved, error=err,
                     resolution=str(node.get("resolution")) if node.get("resolution") is not None else None)

    async def _browse_via(self, client, base, query, limit, anon):
        # Returns candidate questions [(post_id, title, inline_cp)] and the HTTP status.
        # with_cp asks Metaculus to include the community forecast inline for each question.
        params = {"order_by": "-hotness", "forecast_type": "binary", "statuses": "open",
                  "with_cp": "true", "include_conditional_cps": "true", "limit": "100"}
        if query:
            params["search"] = query
        else:
            params["offset"] = str(random.choice([0, 0, 20]))
        st = body = None
        for attempt in range(3):
            st, body = await self._get(client, base, params, anon=anon)
            if st == 200 and isinstance(body, dict):
                break
            if st == 429:
                await asyncio.sleep(1.2 * (attempt + 1))
                params.pop("offset", None)
                continue
            break
        rows = body.get("results") if (st == 200 and isinstance(body, dict)) else []
        cands, seen = [], set()
        for item in (rows or []):
            node = self._node(item)
            qtype = (node.get("type") or node.get("question_type") or "").lower()
            if qtype and "binary" not in qtype:
                continue
            if node.get("resolution") or (node.get("status") or "").lower() in ("resolved", "closed"):
                continue
            post_id = item.get("id") or item.get("post_id") or node.get("id")
            if post_id is None or post_id in seen:
                continue
            seen.add(post_id)
            cp = self._community_prob(node, parent=item)
            title = item.get("title") or node.get("title") or ""
            cands.append((post_id, title, cp))
        return cands, st

    async def browse(self, client, query, limit, cp_only=True):
        # Goal: show the questions that HAVE a public community forecast, ready to track.
        # 1) List candidates anonymously (the token hides forecasts; anonymous reveals them).
        cands, st = await self._browse_via(client, f"{METACULUS}/api/posts/", query, limit, anon=True)
        if not cands:
            cands, st = await self._browse_via(client, f"{METACULUS}/api2/questions/", query, limit, anon=True)
        if not cands:
            cands, st = await self._browse_via(client, f"{METACULUS}/api/posts/", query, limit, anon=False)
        if not cands and st not in (200,):
            raise ConnectionError(self._auth_error([st]))

        target = min(max(int(limit), 12), 30)
        have = [(pid, t, cp) for (pid, t, cp) in cands if cp is not None]
        need = [(pid, t) for (pid, t, cp) in cands if cp is None]

        # 2) For questions whose forecast wasn't inline, read it off the public page (the number
        #    is rendered there). Bounded concurrency + a cap so Browse stays quick and gentle.
        if len(have) < target and need:
            random.shuffle(need)
            to_scrape = need[: min(len(need), max(0, target - len(have)) + 4, 18)]
            sem = asyncio.Semaphore(4)

            async def fill(pid, title):
                async with sem:
                    cp = await self._scrape_cp(client, str(pid))
                    await asyncio.sleep(0.05)
                return (pid, title, cp)

            for r in await asyncio.gather(*[fill(p, t) for p, t in to_scrape], return_exceptions=True):
                if isinstance(r, tuple) and r[2] is not None:
                    have.append(r)
                    if len(have) >= target:
                        break

        out = [{"platform": self.platform, "market_id": str(pid), "title": t, "prob": cp,
                "currency": self.currency, "volume": None, "url": self._url(pid)}
               for (pid, t, cp) in have[:target]]
        # If we genuinely found none with a forecast, still show the question list (no percentages)
        # so Browse is never empty — the forecasts can fill in on the next load.
        if not out and cands:
            out = [{"platform": self.platform, "market_id": str(pid), "title": t, "prob": cp,
                    "currency": self.currency, "volume": None, "url": self._url(pid)}
                   for (pid, t, cp) in cands[:target]]
        random.shuffle(out)
        return out


REGISTRY = {c.platform: c() for c in
            (PolymarketConnector, KalshiConnector, ManifoldConnector, MetaculusConnector)}


async def fetch_quote(client: httpx.AsyncClient, platform: str, market_id: str, outcome: str) -> Quote:
    conn = REGISTRY.get(platform)
    if conn is None:
        return Quote(platform, market_id, outcome, None, "USD", error=f"unknown platform '{platform}'")
    return await conn.get_quote(client, market_id, outcome)


async def browse_markets(client: httpx.AsyncClient, platform: str, query: str, limit, cp_only: bool = True) -> dict:
    conn = REGISTRY.get(platform)
    if conn is None or not hasattr(conn, "browse"):
        return {"markets": [], "error": f"cannot browse '{platform}'"}
    # Browse shows the questions that currently have a public community forecast (read inline
    # from the API, or off the question page when the API withholds it). A few may still slip
    # through without one; those can be tracked by link.
    mc_note = ("Showing Metaculus questions that have a public community forecast \u2014 click Track on "
               "any of them to add it to your portfolio. If one you want isn\u2019t here, paste its link "
               "into \u201cCheck a market\u201d below.")
    try:
        n = min(max(int(limit), 1), 50)
        # When just browsing (no search), pull a bigger pool and randomly sample it,
        # so clicking Browse again surfaces a fresh mix of markets each time.
        pool = n if query else min(n * 4, 50)
        if platform == "metaculus":
            try:
                items = await conn.browse(client, query or "", pool, cp_only=cp_only)
            except (ConnectionError, Exception):  # noqa: BLE001  -> degrade, never show an error
                items = []
        else:
            items = await conn.browse(client, query or "", pool)
        if not query and len(items) > n:
            random.shuffle(items)
            items = items[:n]
        result = {"markets": items}
        if platform == "metaculus":
            result["note"] = mc_note
        return result
    except ConnectionError as exc:
        return {"markets": [], "error": f"Couldn\u2019t reach {platform.title()} right now (HTTP {exc}). "
                f"Give it a moment and try again."}
    except Exception as exc:  # noqa: BLE001
        return {"markets": [], "error": f"Couldn\u2019t load {platform.title()} markets right now. Try again shortly."}
