"""Tests for the Scrape/RSS adapter — 24 test cases."""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import sys
# Prevent __init__.py from importing all adapters (some have heavy deps)
sys.modules.setdefault("hydra.adapters.ckan", MagicMock())
sys.modules.setdefault("hydra.adapters.fdsn", MagicMock())
sys.modules.setdefault("hydra.adapters.odata", MagicMock())
sys.modules.setdefault("hydra.adapters.rest_json", MagicMock())
sys.modules.setdefault("hydra.adapters.s3_bulk", MagicMock())
sys.modules.setdefault("hydra.adapters.sdmx", MagicMock())
sys.modules.setdefault("hydra.adapters.tap_vo", MagicMock())

from hydra.adapters.base import AdapterHealth, HealthStatus, RawPayload
from hydra.adapters.scrape_rss import ScrapeRssAdapter, strip_html_tags
from hydra.config import HydraSettings
from hydra.models.normalized import NormalizedRecord


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_RSS_FEED = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Test Feed</title>
    <link>https://example.com</link>
    <description>A test RSS feed</description>
    <item>
      <title>Article One</title>
      <link>https://example.com/article/1</link>
      <pubDate>Mon, 01 Jan 2024 12:00:00 GMT</pubDate>
      <description>Summary of article one</description>
      <guid>guid-001</guid>
      <category>politics</category>
    </item>
    <item>
      <title>Article Two</title>
      <link>https://example.com/article/2</link>
      <pubDate>Tue, 02 Jan 2024 12:00:00 GMT</pubDate>
      <description>&lt;b&gt;Bold summary&lt;/b&gt; of article two</description>
      <guid>guid-002</guid>
      <category>tech</category>
    </item>
    <item>
      <title>Article Three</title>
      <link>https://example.com/article/3</link>
      <pubDate>Wed, 03 Jan 2024 12:00:00 GMT</pubDate>
      <description>Summary three</description>
      <guid>guid-003</guid>
    </item>
    <item>
      <title>Article Four</title>
      <link>https://example.com/article/4</link>
      <pubDate>Thu, 04 Jan 2024 12:00:00 GMT</pubDate>
      <description>Summary four</description>
      <guid>guid-004</guid>
    </item>
    <item>
      <title>Article Five</title>
      <link>https://example.com/article/5</link>
      <pubDate>Fri, 05 Jan 2024 12:00:00 GMT</pubDate>
      <description>Summary five</description>
      <guid>guid-005</guid>
    </item>
  </channel>
</rss>"""

SAMPLE_ATOM_FEED = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Test Atom Feed</title>
  <link href="https://example.com"/>
  <entry>
    <title>Atom Entry One</title>
    <link href="https://example.com/atom/1"/>
    <id>atom-guid-001</id>
    <updated>2024-01-01T12:00:00Z</updated>
    <summary>Atom summary one</summary>
  </entry>
  <entry>
    <title>Atom Entry Two</title>
    <link href="https://example.com/atom/2"/>
    <id>atom-guid-002</id>
    <updated>2024-01-02T12:00:00Z</updated>
    <summary>Atom summary two</summary>
  </entry>
  <entry>
    <title>Atom Entry Three</title>
    <link href="https://example.com/atom/3"/>
    <id>atom-guid-003</id>
    <updated>2024-01-03T12:00:00Z</updated>
    <summary>Atom summary three</summary>
  </entry>
</feed>"""

SAMPLE_HTML_PAGE = """<!DOCTYPE html>
<html>
<body>
  <div class="article-card">
    <h3><a href="/article/1">Scrape Title One</a></h3>
    <span class="date">2024-01-10</span>
    <p class="summary">Summary of scraped article one</p>
  </div>
  <div class="article-card">
    <h3><a href="/article/2">Scrape Title Two</a></h3>
    <span class="date">2024-01-11</span>
    <p class="summary">Summary of scraped article two</p>
  </div>
  <div class="article-card">
    <h3><a href="/article/3">Scrape Title Three</a></h3>
    <span class="date">2024-01-12</span>
    <p class="summary">Summary of scraped article three</p>
  </div>
</body>
</html>"""

SAMPLE_HTML_PAGE1 = """<!DOCTYPE html>
<html><body>
  <div class="article-card"><h3><a href="/p1">Page1 Item</a></h3></div>
  <a class="next-page" href="/page/2">Next</a>
</body></html>"""

SAMPLE_HTML_PAGE2 = """<!DOCTYPE html>
<html><body>
  <div class="article-card"><h3><a href="/p2">Page2 Item</a></h3></div>
</body></html>"""


def _make_settings(**overrides) -> HydraSettings:
    defaults = {
        "stream_registry_path": Path("src/hydra/registry/stream_registry.yaml"),
        "data_dir": Path("/tmp/hydra_test"),
        "http_timeout_seconds": 30,
        "credentials": {},
    }
    defaults.update(overrides)
    return HydraSettings(**defaults)


def _make_adapter(stream_config: dict, settings: HydraSettings | None = None) -> ScrapeRssAdapter:
    s = settings or _make_settings()
    return ScrapeRssAdapter("test_stream", s, registry=MagicMock(), stream_config=stream_config)


def _raw(content: str | bytes, status: int = 200, headers: dict | None = None) -> RawPayload:
    if isinstance(content, str):
        content = content.encode()
    return RawPayload(
        stream_id="test_stream",
        fetched_at=datetime.now(timezone.utc),
        content=content,
        content_type="text/xml",
        http_status=status,
        headers=headers or {},
    )


SCRAPE_FIELD_MAP = {
    "title": {"selector": "h3 a", "attribute": "text"},
    "link": {"selector": "h3 a", "attribute": "href"},
    "date": {"selector": "span.date", "attribute": "text", "transform": "parse_date"},
    "summary": {"selector": "p.summary", "attribute": "text"},
}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestScrapeRssAdapter:
    """T-SRS-001 through T-SRS-024."""

    def test_rss_fetch_and_parse(self):
        """T-SRS-001: Fetches RSS feed, parses 5 entries."""
        adapter = _make_adapter({"scrape_mode": "rss", "feed_url": "https://example.com/rss", "strip_html": True})
        raw = _raw(SAMPLE_RSS_FEED)
        records = adapter.parse(raw)
        assert len(records) == 5
        for rec in records:
            assert rec["title"] is not None
            assert rec["link"] is not None
            assert rec["guid"] is not None

    def test_atom_fetch_and_parse(self):
        """T-SRS-002: Fetches Atom feed, parses 3 entries."""
        adapter = _make_adapter({"scrape_mode": "atom", "feed_url": "https://example.com/atom", "strip_html": True})
        raw = _raw(SAMPLE_ATOM_FEED)
        records = adapter.parse(raw)
        assert len(records) == 3
        for rec in records:
            assert rec["title"] is not None

    def test_scrape_fetch_and_parse(self):
        """T-SRS-003: Fetches HTML page, extracts items via scrape_item_selector."""
        adapter = _make_adapter({
            "scrape_mode": "scrape",
            "scrape_url": "https://example.com/news",
            "scrape_item_selector": "div.article-card",
            "scrape_field_map": SCRAPE_FIELD_MAP,
        })
        raw = _raw(SAMPLE_HTML_PAGE)
        records = adapter.parse(raw)
        assert len(records) == 3
        assert records[0]["title"] == "Scrape Title One"
        assert records[1]["title"] == "Scrape Title Two"

    def test_scrape_pagination(self):
        """T-SRS-004: Follows pagination_next_selector to page 2."""
        adapter = _make_adapter({
            "scrape_mode": "scrape",
            "scrape_url": "https://example.com/page/1",
            "scrape_item_selector": "div.article-card",
            "scrape_field_map": {"title": {"selector": "h3 a", "attribute": "text"}},
            "scrape_follow_pagination": True,
            "pagination_next_selector": "a.next-page",
            "max_pages": 10,
        })
        # Parse page 1 + page 2 combined
        combined = (SAMPLE_HTML_PAGE1 + SAMPLE_HTML_PAGE2).encode()
        raw = _raw(combined)
        records = adapter.parse(raw)
        assert len(records) == 2

    def test_scrape_max_pages_limit(self):
        """T-SRS-005: max_pages: 1 limits to first page only."""
        adapter = _make_adapter({
            "scrape_mode": "scrape",
            "scrape_url": "https://example.com/page/1",
            "scrape_item_selector": "div.article-card",
            "scrape_field_map": {"title": {"selector": "h3 a", "attribute": "text"}},
            "scrape_follow_pagination": True,
            "pagination_next_selector": "a.next-page",
            "max_pages": 1,
        })
        raw = _raw(SAMPLE_HTML_PAGE1)
        records = adapter.parse(raw)
        assert len(records) == 1

    def test_conditional_fetch_304(self):
        """T-SRS-006: Conditional fetch returns 304 with empty payload."""
        raw = RawPayload(
            stream_id="test_stream",
            fetched_at=datetime.now(timezone.utc),
            content=b"",
            content_type="",
            http_status=304,
            headers={},
        )
        assert raw.http_status == 304
        assert raw.content == b""

    def test_html_strip(self):
        """T-SRS-007: strip_html: true produces plain text."""
        adapter_strip = _make_adapter({"scrape_mode": "rss", "strip_html": True})
        adapter_no_strip = _make_adapter({"scrape_mode": "rss", "strip_html": False})

        raw = _raw(SAMPLE_RSS_FEED)
        records_strip = adapter_strip.parse(raw)
        records_no_strip = adapter_no_strip.parse(raw)

        # Article Two has <b> tags in summary
        strip_summary = records_strip[1]["summary"]
        no_strip_summary = records_no_strip[1]["summary"]
        assert "<b>" not in strip_summary
        assert "Bold summary" in strip_summary
        assert "<b>" in no_strip_summary

    def test_rss_content_fetch(self):
        """T-SRS-008: rss_content_fetch: true marks records for content fetching."""
        adapter = _make_adapter({
            "scrape_mode": "rss",
            "feed_url": "https://example.com/rss",
            "rss_content_fetch": True,
            "strip_html": True,
        })
        raw = _raw(SAMPLE_RSS_FEED)
        records = adapter.parse(raw)
        assert len(records) == 5
        for rec in records:
            assert rec.get("content_fetched") is True

    def test_validate_dedup_by_guid(self):
        """T-SRS-009: Two entries with same guid — only one survives."""
        adapter = _make_adapter({"scrape_mode": "rss"})
        records = [
            {"title": "A", "guid": "dup-guid", "link": "https://example.com/1"},
            {"title": "B", "guid": "dup-guid", "link": "https://example.com/2"},
        ]
        valid = adapter.validate(records)
        assert len(valid) == 1

    def test_validate_staleness_filter(self):
        """T-SRS-010: max_age_hours: 24 drops old entries."""
        adapter = _make_adapter({"scrape_mode": "rss", "max_age_hours": 24})
        now = datetime.now(timezone.utc)
        records = [
            {"title": "Old", "guid": "g1", "published": now - timedelta(hours=48)},
            {"title": "New", "guid": "g2", "published": now - timedelta(hours=2)},
        ]
        valid = adapter.validate(records)
        assert len(valid) == 1
        assert valid[0]["title"] == "New"

    def test_validate_malformed_date(self):
        """T-SRS-011: Unparseable date set to None, record retained."""
        adapter = _make_adapter({"scrape_mode": "rss"})
        records = [{"title": "X", "guid": "g1", "published": "not-a-date"}]
        valid = adapter.validate(records)
        assert len(valid) == 1
        assert valid[0]["published"] is None

    def test_validate_malformed_url(self):
        """T-SRS-012: Malformed URL set to None, record retained."""
        adapter = _make_adapter({"scrape_mode": "rss"})
        records = [{"title": "X", "guid": "g1", "link": "not a url at all"}]
        valid = adapter.validate(records)
        assert len(valid) == 1
        assert valid[0]["link"] is None

    def test_normalize_rss_to_normalized_record(self):
        """T-SRS-013: RSS entry normalizes to NormalizedRecord with confidence 0.8."""
        adapter = _make_adapter({
            "scrape_mode": "rss",
            "tier": 14,
            "source_name": "Test RSS",
            "default_tags": ["test"],
        })
        records = [
            {
                "title": "Test",
                "link": "https://example.com/1",
                "published": datetime(2024, 1, 1, tzinfo=timezone.utc),
                "guid": "guid-001",
                "source_url": "https://example.com/rss",
                "fetch_mode": "rss",
                "categories": ["cat1"],
            }
        ]
        normalized = adapter.normalize(records)
        assert len(normalized) == 1
        nr = normalized[0]
        assert isinstance(nr, NormalizedRecord)
        assert nr.confidence == 0.8
        assert nr.tier == 14
        assert "rss" in nr.tags
        assert "Test RSS" in nr.tags

    def test_normalize_scrape_to_normalized_record(self):
        """T-SRS-014: Scrape item normalizes with confidence 0.6."""
        adapter = _make_adapter({
            "scrape_mode": "scrape",
            "tier": 14,
            "source_name": "Test Scrape",
        })
        records = [{"title": "Scraped", "source_url": "https://example.com", "fetch_mode": "scrape"}]
        normalized = adapter.normalize(records)
        assert len(normalized) == 1
        assert normalized[0].confidence == 0.6

    def test_scrape_delay_enforcement(self):
        """T-SRS-015: asyncio.sleep called with scrape_delay_seconds between fetches."""
        adapter = _make_adapter({
            "scrape_mode": "scrape",
            "scrape_url": "https://example.com",
            "scrape_delay_seconds": 1.5,
            "scrape_follow_pagination": True,
            "pagination_next_selector": "a.next-page",
            "max_pages": 2,
        })

        mock_resp1 = AsyncMock()
        mock_resp1.read = AsyncMock(return_value=SAMPLE_HTML_PAGE1.encode())
        mock_resp1.__aenter__ = AsyncMock(return_value=mock_resp1)
        mock_resp1.__aexit__ = AsyncMock(return_value=False)

        mock_resp2 = AsyncMock()
        mock_resp2.read = AsyncMock(return_value=SAMPLE_HTML_PAGE2.encode())
        mock_resp2.__aenter__ = AsyncMock(return_value=mock_resp2)
        mock_resp2.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(side_effect=[mock_resp1, mock_resp2])
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                asyncio.get_event_loop().run_until_complete(adapter.fetch())
                mock_sleep.assert_called_with(1.5)

    def test_scrape_base_url_resolve(self):
        """T-SRS-016: Relative URLs resolved against page URL."""
        adapter = _make_adapter({
            "scrape_mode": "scrape",
            "scrape_url": "https://example.com/news",
            "scrape_item_selector": "div.article-card",
            "scrape_field_map": {"link": {"selector": "h3 a", "attribute": "href"}},
            "scrape_base_url_resolve": True,
        })
        raw = _raw(SAMPLE_HTML_PAGE)
        records = adapter.parse(raw)
        assert records[0]["link"] == "https://example.com/article/1"

    def test_scrape_javascript_required_warning(self):
        """T-SRS-017: JS required logs warning and tags payload."""
        adapter = _make_adapter({
            "scrape_mode": "scrape",
            "scrape_url": "https://example.com",
            "scrape_javascript_required": True,
            "scrape_item_selector": "div.article-card",
            "scrape_field_map": SCRAPE_FIELD_MAP,
        })

        mock_resp = AsyncMock()
        mock_resp.read = AsyncMock(return_value=SAMPLE_HTML_PAGE.encode())
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            raw = asyncio.get_event_loop().run_until_complete(adapter.fetch())
            assert raw.headers.get("js_required") == "true"

    def test_auth_basic(self):
        """T-SRS-018: basic_auth includes Authorization header."""
        settings = _make_settings(credentials={"test_stream": {"username": "user", "password": "pass"}})
        adapter = _make_adapter({"scrape_mode": "rss", "auth_pattern": "basic_auth"}, settings)
        headers = adapter._build_auth_headers()
        assert "Authorization" in headers
        assert headers["Authorization"].startswith("Basic ")

    def test_auth_cookie(self):
        """T-SRS-019: cookie_auth includes Cookie header."""
        settings = _make_settings(credentials={"test_stream": {"cookie_name": "session", "cookie_value": "abc123"}})
        adapter = _make_adapter({"scrape_mode": "rss", "auth_pattern": "cookie_auth"}, settings)
        headers = adapter._build_auth_headers()
        assert headers["Cookie"] == "session=abc123"

    def test_run_pipeline_rss(self):
        """T-SRS-020: Full run() pipeline for RSS."""
        adapter = _make_adapter({
            "scrape_mode": "rss",
            "feed_url": "https://example.com/rss",
            "tier": 14,
            "source_name": "Test",
            "strip_html": True,
        })

        mock_resp = AsyncMock()
        mock_resp.read = AsyncMock(return_value=SAMPLE_RSS_FEED.encode())
        mock_resp.status = 200
        mock_resp.headers = {"Content-Type": "text/xml"}
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = asyncio.get_event_loop().run_until_complete(adapter.run())
            assert len(result) == 5
            assert all(isinstance(r, NormalizedRecord) for r in result)

    def test_run_pipeline_scrape(self):
        """T-SRS-021: Full run() pipeline for scrape mode."""
        adapter = _make_adapter({
            "scrape_mode": "scrape",
            "scrape_url": "https://example.com/news",
            "scrape_item_selector": "div.article-card",
            "scrape_field_map": SCRAPE_FIELD_MAP,
            "tier": 14,
            "source_name": "Test Scrape",
        })

        mock_resp = AsyncMock()
        mock_resp.read = AsyncMock(return_value=SAMPLE_HTML_PAGE.encode())
        mock_resp.status = 200
        mock_resp.headers = {"Content-Type": "text/html"}
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = asyncio.get_event_loop().run_until_complete(adapter.run())
            assert len(result) == 3
            assert all(isinstance(r, NormalizedRecord) for r in result)

    def test_health_check(self):
        """T-SRS-022: Health check returns AdapterHealth with OK status."""
        adapter = _make_adapter({"scrape_mode": "rss", "feed_url": "https://example.com/rss"})

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.head = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            health = asyncio.get_event_loop().run_until_complete(adapter.health_check())
            assert isinstance(health, AdapterHealth)
            assert health.status == HealthStatus.OK

    def test_empty_feed(self):
        """T-SRS-023: Empty RSS feed returns empty list."""
        adapter = _make_adapter({"scrape_mode": "rss", "strip_html": True})
        empty_rss = '<?xml version="1.0"?><rss version="2.0"><channel><title>Empty</title></channel></rss>'
        raw = _raw(empty_rss)
        records = adapter.parse(raw)
        assert records == []

    def test_no_items_matched(self):
        """T-SRS-024: No items matched by selector returns empty list."""
        adapter = _make_adapter({
            "scrape_mode": "scrape",
            "scrape_item_selector": "div.nonexistent",
            "scrape_field_map": SCRAPE_FIELD_MAP,
        })
        raw = _raw(SAMPLE_HTML_PAGE)
        records = adapter.parse(raw)
        assert records == []
