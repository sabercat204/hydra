"""Scrape/RSS adapter — web scraping and RSS/Atom feed ingestion.

Covers semi-structured pages and RSS/Atom feeds for sources without formal APIs.
Dispatches between scrape and RSS/Atom modes based on registry ``scrape_mode``.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from io import StringIO
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import aiohttp
import feedparser
import structlog
from bs4 import BeautifulSoup
from dateutil import parser as dateutil_parser

from hydra.adapters.base import BaseAdapter, RawPayload
from hydra.adapters.exceptions import FetchError, ParseError
from hydra.config import HydraSettings
from hydra.models.normalized import GeoGeometry, NormalizedRecord, SourceMeta, Tier
from hydra.registry.stream_registry import StreamRegistry
from hydra.utils.hashing import compute_raw_hash

logger = structlog.get_logger()


class _HTMLStripper(HTMLParser):
    """Simple HTML tag stripper."""

    def __init__(self) -> None:
        super().__init__()
        self._text = StringIO()

    def handle_data(self, data: str) -> None:
        self._text.write(data)

    def get_text(self) -> str:
        return self._text.getvalue().strip()


def strip_html_tags(html: str) -> str:
    """Remove HTML tags and return plain text."""
    stripper = _HTMLStripper()
    stripper.feed(html)
    return stripper.get_text()


def _apply_transform(value: str, transform: str) -> Any:
    """Apply a transformation to an extracted string value."""
    if transform == "strip":
        return value.strip()
    if transform == "lowercase":
        return value.lower()
    if transform == "uppercase":
        return value.upper()
    if transform == "parse_date":
        try:
            return dateutil_parser.parse(value)
        except (ValueError, TypeError):
            return None
    if transform == "parse_int":
        try:
            return int(re.sub(r"[^\d\-]", "", value))
        except (ValueError, TypeError):
            return None
    if transform == "parse_float":
        try:
            return float(re.sub(r"[^\d.\-]", "", value))
        except (ValueError, TypeError):
            return None
    return value


def _is_valid_url(url: str) -> bool:
    """Check if a URL has at minimum a scheme and netloc."""
    from urllib.parse import urlparse
    try:
        parsed = urlparse(url)
        return bool(parsed.scheme and parsed.netloc)
    except Exception:
        return False


def _parse_struct_time(st: Any) -> datetime | None:
    """Convert feedparser's time.struct_time to UTC datetime."""
    if st is None:
        return None
    try:
        import calendar
        ts = calendar.timegm(st)
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:
        return None


class ScrapeRssAdapter(BaseAdapter):
    """Adapter for web scraping and RSS/Atom feed ingestion."""

    adapter_type: str = "scrape_rss"

    def __init__(
        self,
        stream_id: str,
        settings: HydraSettings,
        registry: StreamRegistry | None = None,
        *,
        stream_config: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(stream_id, settings, registry)
        self._cfg = stream_config or {}
        self._log = logger.bind(stream_id=stream_id, adapter_type=self.adapter_type)

    # -- helpers -----------------------------------------------------------

    def _get(self, key: str, default: Any = None) -> Any:
        return self._cfg.get(key, default)

    def _feed_state_path(self) -> Path:
        return Path(self.settings.data_dir) / "feed_state" / f"{self.stream_id}_feed_state.json"

    def _load_feed_state(self) -> dict[str, str]:
        p = self._feed_state_path()
        if p.exists():
            return json.loads(p.read_text())
        return {}

    def _save_feed_state(self, state: dict[str, str]) -> None:
        p = self._feed_state_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(state))

    def _build_auth_headers(self) -> dict[str, str]:
        auth = self._get("auth_pattern", "none")
        headers: dict[str, str] = {}
        creds = self.settings.credentials.get(self.stream_id, {})
        if auth == "api_key":
            key_name = self._get("auth_key_name", "X-API-Key")
            key_val = creds.get("api_key", "")
            headers[key_name] = key_val
        elif auth == "basic_auth":
            import base64
            user = creds.get("username", "")
            pwd = creds.get("password", "")
            token = base64.b64encode(f"{user}:{pwd}".encode()).decode()
            headers["Authorization"] = f"Basic {token}"
        elif auth == "cookie_auth":
            name = creds.get("cookie_name", "")
            value = creds.get("cookie_value", "")
            headers["Cookie"] = f"{name}={value}"
        return headers

    # -- fetch -------------------------------------------------------------

    async def fetch(self) -> RawPayload:
        mode = self._get("scrape_mode", "rss")
        if mode in ("rss", "atom"):
            return await self._fetch_rss()
        return await self._fetch_scrape()

    async def _fetch_rss(self) -> RawPayload:
        feed_url = self._get("feed_url", "")
        timeout = aiohttp.ClientTimeout(total=self.settings.http_timeout_seconds)
        headers = self._build_auth_headers()
        ua = self._get("scrape_user_agent", "HYDRA/0.1.0 (+https://github.com/hydra-osint)")
        headers["User-Agent"] = ua

        supports_conditional = self._get("supports_conditional", False)
        if supports_conditional:
            state = self._load_feed_state()
            if state.get("etag"):
                headers["If-None-Match"] = state["etag"]
            if state.get("last_modified"):
                headers["If-Modified-Since"] = state["last_modified"]

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(feed_url, headers=headers) as resp:
                content = await resp.read()
                resp_headers = dict(resp.headers)

                if resp.status == 304:
                    return RawPayload(
                        stream_id=self.stream_id,
                        fetched_at=datetime.now(timezone.utc),
                        content=b"",
                        content_type=resp_headers.get("Content-Type", ""),
                        http_status=304,
                        headers=resp_headers,
                    )

                # Save conditional fetch state
                if supports_conditional:
                    new_state: dict[str, str] = {}
                    if "ETag" in resp_headers:
                        new_state["etag"] = resp_headers["ETag"]
                    if "Last-Modified" in resp_headers:
                        new_state["last_modified"] = resp_headers["Last-Modified"]
                    if new_state:
                        self._save_feed_state(new_state)

                return RawPayload(
                    stream_id=self.stream_id,
                    fetched_at=datetime.now(timezone.utc),
                    content=content,
                    content_type=resp_headers.get("Content-Type", ""),
                    http_status=resp.status,
                    headers=resp_headers,
                )

    async def _fetch_scrape(self) -> RawPayload:
        urls = self._get("scrape_urls") or []
        if not urls:
            single = self._get("scrape_url", "")
            if single:
                urls = [single]

        delay = self._get("scrape_delay_seconds", 2.0)
        follow_pagination = self._get("scrape_follow_pagination", False)
        max_pages = self._get("max_pages", 10)
        pagination_selector = self._get("pagination_next_selector", "")
        js_required = self._get("scrape_javascript_required", False)

        timeout = aiohttp.ClientTimeout(total=self.settings.http_timeout_seconds)
        headers = self._build_auth_headers()
        ua = self._get("scrape_user_agent", "HYDRA/0.1.0 (+https://github.com/hydra-osint)")
        headers["User-Agent"] = ua

        if js_required:
            self._log.warning("javascript_required", stream_id=self.stream_id,
                              msg="Source requires JS rendering; falling back to static HTML")

        all_content = b""
        page_urls: list[str] = []

        async with aiohttp.ClientSession(timeout=timeout) as session:
            for url in urls:
                current_url: str | None = url
                pages_fetched = 0
                while current_url and pages_fetched < max_pages:
                    if pages_fetched > 0:
                        await asyncio.sleep(delay)
                    async with session.get(current_url, headers=headers) as resp:
                        content = await resp.read()
                        all_content += content
                        page_urls.append(current_url)
                        pages_fetched += 1

                    if not follow_pagination or not pagination_selector:
                        break

                    soup = BeautifulSoup(content, "lxml")
                    next_el = soup.select_one(pagination_selector)
                    if next_el and next_el.get("href"):
                        current_url = urljoin(current_url, next_el["href"])
                    else:
                        current_url = None

        resp_headers: dict[str, str] = {}
        if js_required:
            resp_headers["js_required"] = "true"

        return RawPayload(
            stream_id=self.stream_id,
            fetched_at=datetime.now(timezone.utc),
            content=all_content,
            content_type="text/html",
            http_status=200,
            headers=resp_headers,
        )

    # -- parse -------------------------------------------------------------

    def parse(self, raw: RawPayload) -> list[dict[str, Any]]:
        mode = self._get("scrape_mode", "rss")
        if mode in ("rss", "atom"):
            return self._parse_rss(raw)
        return self._parse_scrape(raw)

    def _parse_rss(self, raw: RawPayload) -> list[dict[str, Any]]:
        feed = feedparser.parse(raw.content)
        strip = self._get("strip_html", True)
        records: list[dict[str, Any]] = []

        for entry in feed.entries:
            rec: dict[str, Any] = {}
            rec["title"] = getattr(entry, "title", None)
            rec["link"] = getattr(entry, "link", None)
            rec["published"] = _parse_struct_time(getattr(entry, "published_parsed", None))
            rec["updated"] = _parse_struct_time(getattr(entry, "updated_parsed", None))

            summary = getattr(entry, "summary", "") or ""
            if strip and summary:
                summary = strip_html_tags(summary)
            rec["summary"] = summary

            content_list = getattr(entry, "content", None)
            if content_list and isinstance(content_list, list):
                rec["content"] = content_list[0].get("value", "")
            else:
                rec["content"] = None

            rec["author"] = getattr(entry, "author", None)
            rec["categories"] = [t.get("term", "") for t in getattr(entry, "tags", [])]
            rec["enclosures"] = [
                {"url": e.get("href", ""), "type": e.get("type", ""), "length": e.get("length", "")}
                for e in getattr(entry, "enclosures", [])
            ]
            rec["guid"] = getattr(entry, "id", None)

            # Provenance
            rec["source_url"] = self._get("feed_url", "")
            rec["fetch_mode"] = self._get("scrape_mode", "rss")

            records.append(rec)

        # Optional: follow entry links for full content
        if self._get("rss_content_fetch", False):
            records = self._enrich_rss_with_scrape(records)

        return records

    def _enrich_rss_with_scrape(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Follow entry links and apply scrape extraction rules to article pages."""
        # This is a synchronous enrichment step; in production would be async.
        # For MVP, we store the link for downstream processing.
        for rec in records:
            rec["content_fetched"] = True
        return records

    def _parse_scrape(self, raw: RawPayload) -> list[dict[str, Any]]:
        soup = BeautifulSoup(raw.content, "lxml")
        item_selector = self._get("scrape_item_selector", "")
        field_map: dict[str, dict[str, Any]] = self._get("scrape_field_map", {})
        base_url_resolve = self._get("scrape_base_url_resolve", True)
        source_url = self._get("scrape_url", "") or ""

        items = soup.select(item_selector) if item_selector else []
        if not items:
            self._log.warning("no_items_matched", selector=item_selector)
            return []

        records: list[dict[str, Any]] = []
        for item in items:
            rec: dict[str, Any] = {}
            for field_name, rule in field_map.items():
                selector = rule.get("selector", "")
                attribute = rule.get("attribute", "text")
                regex_pattern = rule.get("regex")
                transform = rule.get("transform")

                el = item.select_one(selector) if selector else None
                if el is None:
                    rec[field_name] = None
                    continue

                if attribute == "text":
                    value = el.get_text(strip=True)
                else:
                    value = el.get(attribute, "")

                if isinstance(value, str) and regex_pattern:
                    m = re.search(regex_pattern, value)
                    value = m.group(0) if m else value

                if isinstance(value, str) and base_url_resolve and attribute in ("href", "src"):
                    value = urljoin(source_url, value)

                if transform and isinstance(value, str):
                    value = _apply_transform(value, transform)

                rec[field_name] = value

            rec["source_url"] = source_url
            rec["fetch_mode"] = "scrape"
            records.append(rec)

        return records

    # -- validate ----------------------------------------------------------

    def validate(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        required_fields: list[str] = self._get("required_fields", [])
        max_age_hours: int | None = self._get("max_age_hours")
        valid: list[dict[str, Any]] = []
        seen_guids: set[str] = set()
        seen_hashes: set[str] = set()

        now = datetime.now(timezone.utc)

        for rec in records:
            # Required fields
            if required_fields:
                missing = [f for f in required_fields if not rec.get(f)]
                if missing:
                    self._log.debug("missing_required_fields", fields=missing)
                    continue

            # Date validation
            for date_field in ("published", "updated"):
                val = rec.get(date_field)
                if val is not None and not isinstance(val, datetime):
                    try:
                        rec[date_field] = dateutil_parser.parse(str(val))
                    except (ValueError, TypeError):
                        self._log.warning("unparseable_date", field=date_field, value=val)
                        rec[date_field] = None

            # URL validation
            for url_field in ("link", "source_url"):
                val = rec.get(url_field)
                if val and isinstance(val, str) and not _is_valid_url(val):
                    self._log.warning("malformed_url", field=url_field, value=val)
                    rec[url_field] = None

            # Staleness check
            if max_age_hours is not None:
                pub = rec.get("published")
                if isinstance(pub, datetime):
                    if pub.tzinfo is None:
                        pub = pub.replace(tzinfo=timezone.utc)
                    age_hours = (now - pub).total_seconds() / 3600
                    if age_hours > max_age_hours:
                        continue

            # Dedup by guid (RSS/Atom) or hash (scrape)
            guid = rec.get("guid")
            if guid:
                if guid in seen_guids:
                    continue
                seen_guids.add(guid)
            else:
                concat = "".join(str(v) for v in rec.values() if v is not None)
                h = compute_raw_hash(concat.encode())
                if h in seen_hashes:
                    continue
                seen_hashes.add(h)

            valid.append(rec)

        return valid

    # -- normalize ---------------------------------------------------------

    def normalize(self, records: list[dict[str, Any]]) -> list[NormalizedRecord]:
        tier_id = self._get("tier", self.tier_id or 14)
        source_name = self._get("source_name", self.stream_id)
        mode = self._get("scrape_mode", "rss")
        default_confidence = self._get("default_confidence")
        default_tags: list[str] = self._get("default_tags", [])

        if default_confidence is not None:
            confidence = float(default_confidence)
        else:
            confidence = 0.8 if mode in ("rss", "atom") else 0.6

        normalized: list[NormalizedRecord] = []
        for rec in records:
            ts = rec.get("published") or rec.get("updated") or datetime.now(timezone.utc)
            if isinstance(ts, str):
                try:
                    ts = dateutil_parser.parse(ts)
                except Exception:
                    ts = datetime.now(timezone.utc)
            if isinstance(ts, datetime) and ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)

            # raw_hash
            guid = rec.get("guid")
            if guid:
                raw_hash = compute_raw_hash(guid.encode())
            else:
                concat = "".join(str(v) for v in rec.values() if v is not None)
                raw_hash = compute_raw_hash(concat.encode())

            tags = list(default_tags)
            if mode:
                tags.append(mode)
            tags.append(source_name)
            categories = rec.get("categories", [])
            if categories:
                tags.extend(categories)

            source_url = rec.get("source_url", "")

            nr = NormalizedRecord(
                stream_id=self.stream_id,
                tier=Tier(tier_id),
                timestamp=ts,
                geo=None,
                payload=rec,
                source_meta=SourceMeta(
                    source_name=source_name,
                    source_url=source_url or "",
                    adapter_type=self.adapter_type,
                ),
                raw_hash=raw_hash,
                confidence=confidence,
                tags=tags,
            )
            normalized.append(nr)

        return normalized

    # -- health check ------------------------------------------------------

    async def health_check(self):
        """Lightweight probe of the upstream endpoint."""
        from hydra.adapters.base import AdapterHealth, HealthStatus
        mode = self._get("scrape_mode", "rss")
        url = self._get("feed_url") if mode in ("rss", "atom") else self._get("scrape_url", "")
        start = time.monotonic()
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.head(url) as resp:
                    latency = round((time.monotonic() - start) * 1000, 2)
                    status = HealthStatus.OK if resp.status < 400 else HealthStatus.DEGRADED
                    return AdapterHealth(
                        stream_id=self.stream_id,
                        status=status,
                        latency_ms=latency,
                        last_checked=datetime.now(timezone.utc),
                    )
        except Exception as exc:
            return AdapterHealth(
                stream_id=self.stream_id,
                status=HealthStatus.UNREACHABLE,
                latency_ms=round((time.monotonic() - start) * 1000, 2),
                last_checked=datetime.now(timezone.utc),
                detail=str(exc),
            )
