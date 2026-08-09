"""Ponytail news feed — fetch crypto + gold headlines from public RSS.

One module, stdlib only. Cached 5 minutes to avoid spamming feeds every decision cycle.
"""
from __future__ import annotations

import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from urllib.request import Request, urlopen

_FEEDS: dict[str, str] = {
    # venue → RSS URL; crypto feeds for BINANCE, gold/general for MT5/XAUUSD
    "BINANCE": "https://cointelegraph.com/rss",
    "MT5": "https://www.investing.com/rss/news_25.rss",  # gold/commodities
}

_CACHE: dict[str, tuple[float, list[dict[str, str]]]] = {}
_CACHE_TTL = 300  # 5 minutes


@dataclass(frozen=True)
class NewsItem:
    title: str
    source: str
    published: str  # RFC822 or ISO, pass-through


def _fetch_rss(url: str, limit: int = 5) -> list[dict[str, str]]:
    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0 ALMA/1.0"})
        with urlopen(req, timeout=8) as resp:
            root = ET.fromstring(resp.read())
        items: list[dict[str, str]] = []
        for item in root.findall(".//item")[:limit]:
            title_el = item.find("title")
            pub_el = item.find("pubDate")
            title = (title_el.text or "").strip() if title_el is not None and title_el.text else ""
            if not title:
                continue
            items.append(
                {
                    "title": title[:200],
                    "published": (pub_el.text or "")[:40] if pub_el is not None and pub_el.text else "",
                    "source": url.split("/")[2] if "//" in url else "rss",
                }
            )
        return items
    except (ET.ParseError, OSError, TimeoutError, ValueError, RuntimeError):
        return []
    except Exception:  # noqa: BLE001  ponytail: news feed must never crash decision cycle
        return []


def fetch_news(venue: str, limit: int = 5) -> list[dict[str, str]]:
    """Return cached RSS headlines for a venue. Empty list on failure — never raises."""
    url = _FEEDS.get(venue)
    if url is None:
        return []
    now = time.monotonic()
    cached = _CACHE.get(venue)
    if cached and (now - cached[0]) < _CACHE_TTL:
        return cached[1]
    items = _fetch_rss(url, limit=limit)
    _CACHE[venue] = (now, items)
    return items


def news_for_context(venue: str) -> dict[str, str | int | None]:
    """Build the ``news`` dict for ShadowContext from RSS headlines."""
    items = fetch_news(venue, limit=5)
    titles = [i["title"] for i in items]
    if not titles:
        return {"phase": None}
    return {
        "phase": "NEWS",
        "count": len(titles),
        "headlines": " | ".join(titles),
    }


if __name__ == "__main__":
    for v in ("BINANCE", "MT5"):
        n = news_for_context(v)
        print(f"\n=== {v} ===")
        print(f"phase={n.get('phase')} count={n.get('count')}")
        print(f"headlines[:200]={(n.get('headlines') or '')[:200]}")
        assert n["phase"] == "NEWS"
        assert isinstance(n.get("count"), int)
