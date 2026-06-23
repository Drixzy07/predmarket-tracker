"""
Read-only connectors for prediction-market platforms.

Each connector returns a normalized Quote with the current implied probability
(0..1) of one outcome, the market's valid outcomes, and a link to the market's
public web page. Identify a market by pasting its FULL web address, or just the
slug / id / ticker.

All endpoints used are PUBLIC. No trading credentials are needed.
"""

from __future__ import annotations

import json
import os
import re
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import httpx

GAMMA = "https://gamma-api.polymarket.com"
KALSHI = "https://external-api.kalshi.com/trade-api/v2"
MANIFOLD = "https://api.manifold.markets/v0"
METACULUS = "https://www.metaculus.com"


def _metaculus_token() -> str:
    """Metaculus locked its API behind a free account token. Read it from the
    METACULUS_TOKEN environment variable (set it in your host's env settings)."""
    return os.environ.get("METACULUS_TOKEN", "").strip()

_HTTP_TIMEOUT = httpx.Timeout(12.0)
_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
    "Accept": "application/json",
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
            if str(slug).isdigit():
                r = await client.get(f"{GAMMA}/markets/{slug}", headers=_HEADERS)
                r.raise_for_status()
                data = r.json()
            else:
                r = await client.get(f"{GAMMA}/markets", params={"slug": slug}, headers=_HEADERS)
                r.raise_for_status()
                data = r.json()
                if isinstance(data, list) and not data:
                    data = await self._market_from_event(client, slug)  # a /event/<slug> URL
            if isinstance(data, list):
                data = data[0] if data else None
            if not isinstance(data, dict):
                return Quote(self.platform, slug, outcome, None, self.currency, url=url,
                             error="market not found (check the link)")
            # prefer the event slug for the link when present
            evs = data.get("events")
            if isinstance(evs, list) and evs and isinstance(evs[0], dict) and evs[0].get("slug"):
                url = self._url(evs[0]["slug"])
            outcomes = [str(o) for o in _as_list(data.get("outcomes"))]
            prices = _as_list(data.get("outcomePrices"))
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
    async def _market_from_event(client, slug):
        try:
            r = await client.get(f"{GAMMA}/events", params={"slug": slug}, headers=_HEADERS)
            r.raise_for_status()
            ev = r.json()
            ev = ev[0] if isinstance(ev, list) and ev else ev
            mkts = ev.get("markets") if isinstance(ev, dict) else None
            return mkts[0] if mkts else None
        except Exception:  # noqa: BLE001
            return None

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
            r = await client.get(f"{KALSHI}/markets/{ticker}", headers=_HEADERS)
            r.raise_for_status()
            m = r.json().get("market", {})
            yes = self._yes_price(m)
            prob = yes if outcome.upper() == "YES" else (1 - yes) if yes is not None else None
            status = (m.get("status") or "").lower()
            return Quote(self.platform, ticker, outcome.upper(), prob, self.currency,
                         title=m.get("title"), outcomes=["YES", "NO"], url=url,
                         resolved=status in ("settled", "finalized", "closed", "determined"),
                         resolution=m.get("result") or None)
        except Exception as exc:  # noqa: BLE001
            return Quote(self.platform, ticker, outcome.upper(), None, self.currency, url=url,
                         outcomes=["YES", "NO"],
                         error=f"could not reach Kalshi ({type(exc).__name__}) — is the ticker right?")

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

    async def browse(self, client, query, limit):
        out = []
        try:
            r = await client.get(f"{KALSHI}/events",
                                 params={"limit": "200", "status": "open", "with_nested_markets": "true"},
                                 headers=_HEADERS)
            r.raise_for_status()
            for ev in r.json().get("events", []):
                ev_title = ev.get("title") or ""
                url = self._url(ev.get("series_ticker") or ev.get("event_ticker"))
                for m in (ev.get("markets") or []):
                    label = self._label(ev_title, m)
                    if query and query.lower() not in label.lower():
                        continue
                    out.append({"platform": self.platform, "market_id": m.get("ticker"),
                                "title": label, "prob": self._yes_price(m), "currency": self.currency,
                                "volume": _num(m.get("volume_fp")) or _num(m.get("volume")), "url": url})
                    if len(out) >= limit:
                        return out
        except Exception:  # noqa: BLE001
            out = []
        if out:
            return out
        r = await client.get(f"{KALSHI}/markets", params={"limit": str(max(limit, 50)), "status": "open"},
                             headers=_HEADERS)
        r.raise_for_status()
        for m in r.json().get("markets", []):
            label = self._label(m.get("title") or "", m)
            if query and query.lower() not in label.lower():
                continue
            out.append({"platform": self.platform, "market_id": m.get("ticker"), "title": label,
                        "prob": self._yes_price(m), "currency": self.currency,
                        "volume": _num(m.get("volume_fp")) or _num(m.get("volume")),
                        "url": self._url(m.get("event_ticker"))})
            if len(out) >= limit:
                break
        return out


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
                match = self._match_answer(answers, outcome)
                prob = match.get("probability") if match else None
                err = None if prob is not None else "pick an outcome below"
                return Quote(self.platform, slug, outcome, prob, self.currency, title=m.get("question"),
                             outcomes=names, url=url, resolved=bool(m.get("isResolved")),
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
    def _community_prob(cls, node):
        agg = (node.get("aggregations") or {}).get("recency_weighted") or {}
        centers = None
        latest = agg.get("latest")
        if isinstance(latest, dict):
            centers = latest.get("centers")
        if not centers:
            hist = agg.get("history") or []
            if hist and isinstance(hist[-1], dict):
                centers = hist[-1].get("centers")
        if centers:
            try:
                return float(centers[0])
            except (TypeError, ValueError, IndexError):
                return None
        return None

    @staticmethod
    async def _get(client, url, params):
        """Return (status_code, json_or_None). Sends the Metaculus API token when set."""
        headers = dict(_HEADERS)
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

    async def get_quote(self, client: httpx.AsyncClient, market_id: str, outcome: str) -> Quote:
        qid = _clean_metaculus(market_id)
        url = self._url(qid)
        codes, data = [], None
        for endpoint in (f"{METACULUS}/api/posts/{qid}/", f"{METACULUS}/api2/questions/{qid}/"):
            st, body = await self._get(client, endpoint, None)
            codes.append(st)
            if st == 200 and body:
                data = body
                break
        if not data:
            return Quote(self.platform, qid, outcome.upper(), None, self.currency, url=url,
                         outcomes=["YES", "NO"], error=self._auth_error(codes))
        node = self._node(data)
        title = data.get("title") or node.get("title")
        qtype = (node.get("type") or node.get("question_type") or "").lower()
        resolved = bool(node.get("resolution")) or (node.get("status") in ("resolved", "closed"))
        # --- handle non-binary types ---
        if qtype in ("date", "numeric", "continuous"):
            return Quote(self.platform, qid, outcome, None, self.currency, title=title, url=url,
                         error=f"this is a {qtype} question (not yes/no) \u2014 only binary questions can be tracked")
        if qtype == "multiple_choice":
            options = node.get("options") or []
            names = [str(o.get("label") or o.get("name") or o) for o in options] if options else None
            return Quote(self.platform, qid, outcome, None, self.currency, title=title, url=url,
                         outcomes=names, error="pick an outcome below" if names else "multiple-choice (no options found)")
        # --- group posts (contain sub-questions) ---
        sub_questions = data.get("group_of_questions") or data.get("sub_questions")
        if sub_questions and not qtype:
            subs = sub_questions.get("questions") if isinstance(sub_questions, dict) else sub_questions
            if isinstance(subs, list) and subs:
                names = [str(sq.get("title") or sq.get("label") or "?") for sq in subs]
                return Quote(self.platform, qid, outcome, None, self.currency, title=title, url=url,
                             outcomes=names, error="this is a group question \u2014 pick a sub-question outcome below")
        # --- binary ---
        cp = self._community_prob(node)
        if cp is None and not qtype:
            # might be a non-binary type the API didn't label clearly
            return Quote(self.platform, qid, outcome, None, self.currency, title=title, url=url,
                         outcomes=["YES", "NO"],
                         error="no community forecast yet, or this isn\u2019t a binary question")
        prob = None if cp is None else (cp if outcome.upper() == "YES" else 1 - cp)
        return Quote(self.platform, qid, outcome.upper(), prob, self.currency, title=title,
                     outcomes=["YES", "NO"], url=url, resolved=resolved,
                     resolution=str(node.get("resolution")) if node.get("resolution") is not None else None)

    async def browse(self, client, query, limit):
        combos = []
        if query:
            combos.append({"search": query, "limit": str(limit)})
        combos.append({"statuses": "open", "limit": str(limit), "order_by": "-hotness"})
        combos.append({"limit": str(limit)})
        results, used_search, codes = None, False, []
        for endpoint in (f"{METACULUS}/api/posts/", f"{METACULUS}/api2/questions/"):
            for params in combos:
                st, body = await self._get(client, endpoint, params)
                if st is not None:
                    codes.append(st)
                if st == 200 and body is not None:
                    rows = body.get("results") if isinstance(body, dict) else (body if isinstance(body, list) else None)
                    if rows:
                        results, used_search = rows, ("search" in params)
                        break
            if results:
                break
        out = []
        ql = (query or "").lower()
        for item in (results or []):
            node = self._node(item)
            qtype = (node.get("type") or "").lower()
            if qtype in ("numeric", "date", "multiple_choice", "discrete"):
                continue
            title = item.get("title") or node.get("title") or ""
            if ql and not used_search and ql not in title.lower():
                continue
            qid = item.get("id") or item.get("post_id") or node.get("id")
            if qid is None:
                continue
            out.append({"platform": self.platform, "market_id": str(qid), "title": title,
                        "prob": self._community_prob(node), "currency": self.currency,
                        "volume": None, "url": self._url(qid)})
            if len(out) >= limit:
                break
        if not out and not any(c == 200 for c in codes):
            raise ConnectionError(self._auth_error(codes))
        return out


REGISTRY = {c.platform: c() for c in
            (PolymarketConnector, KalshiConnector, ManifoldConnector, MetaculusConnector)}


async def fetch_quote(client: httpx.AsyncClient, platform: str, market_id: str, outcome: str) -> Quote:
    conn = REGISTRY.get(platform)
    if conn is None:
        return Quote(platform, market_id, outcome, None, "USD", error=f"unknown platform '{platform}'")
    return await conn.get_quote(client, market_id, outcome)


async def browse_markets(client: httpx.AsyncClient, platform: str, query: str, limit) -> dict:
    conn = REGISTRY.get(platform)
    if conn is None or not hasattr(conn, "browse"):
        return {"markets": [], "error": f"cannot browse '{platform}'"}
    try:
        n = min(max(int(limit), 1), 50)
        items = await conn.browse(client, query or "", n)
        return {"markets": items}
    except ConnectionError as exc:
        tok = _metaculus_token()
        token_info = f"token set ({len(tok)} chars)" if tok else "NO token set in env"
        return {"markets": [], "error": f"Metaculus browse failed (HTTP {exc}; {token_info}). "
                f"Make sure METACULUS_TOKEN is set in Render's Environment Variables."}
    except Exception as exc:  # noqa: BLE001
        return {"markets": [], "error": f"could not browse {platform} ({type(exc).__name__})"}
