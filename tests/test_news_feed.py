from unittest.mock import patch

import alma.news_feed as nf
from alma.news_feed import fetch_news, news_for_context


def test_news_for_context_venance_binance():
    nf._CACHE.clear()
    with patch("alma.news_feed._fetch_rss", return_value=[{"title": "BTC pumps 10%", "published": "2026-08-09", "source": "cointelegraph.com"}]):
        result = news_for_context("BINANCE")
    assert result["phase"] == "NEWS"
    assert result["count"] == 1
    assert "BTC pumps" in result["headlines"]


def test_news_for_context_unknown_venue():
    result = news_for_context("UNKNOWN")
    assert result == {"phase": None}


def test_news_for_context_empty_feed():
    nf._CACHE.clear()
    with patch("alma.news_feed._fetch_rss", return_value=[]):
        result = news_for_context("BINANCE")
    assert result == {"phase": None}


def test_fetch_news_caches(monkeypatch):
    import alma.news_feed as nf
    nf._CACHE.clear()
    calls = []

    def mock_fetch(url, limit=5):
        calls.append(url)
        return [{"title": "test", "published": "", "source": ""}]

    monkeypatch.setattr(nf, "_fetch_rss", mock_fetch)
    fetch_news("BINANCE")
    fetch_news("BINANCE")
    assert len(calls) == 1  # second call hit cache
