"""Document-repository adapter — paginated list → detail → blob fetch.

Designed for the mil_int_public_information surface (tiers 100–106). Most
sources expose an HTML or JSON listing of publications; each listing entry
links to a detail page that anchors a downloadable PDF (or, less commonly,
HTML). The adapter walks the listing, resolves document URLs, optionally
downloads a bounded number of blobs into the storage router's
``_binary_artifact`` channel for MinIO persistence, and emits one
:class:`NormalizedRecord` per discovered document.

Stream behaviour is fully config-driven (``stream_config``) so adding a
source is a YAML / config change, not a code change. The adapter respects
the registry's per-source ``access_policy`` field — anything other than
``open`` (or ``registration`` with credentials present) is short-circuited
to an empty result.
"""

from __future__ import annotations

import asyncio
import re
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin, urlparse

import aiohttp
import structlog
from bs4 import BeautifulSoup
from sloptropy_common import AccessPolicy, is_auto_ingestable

from hydra.adapters.base import AdapterHealth, BaseAdapter, HealthStatus, RawPayload
from hydra.adapters.exceptions import FetchError
from hydra.config import HydraSettings
from hydra.models.normalized import NormalizedRecord, SourceMeta, Tier
from hydra.registry.stream_registry import StreamRegistry, StreamSource
from hydra.utils.hashing import compute_raw_hash

logger = structlog.get_logger()


_DEFAULT_USER_AGENT = "HYDRA-DocRepo/0.1 (+https://github.com/hydra-osint)"
_DEFAULT_MAX_DOCS_PER_RUN = 25
_DEFAULT_MAX_LIST_PAGES = 5
_DEFAULT_FETCH_DELAY_SECONDS = 1.5
_DEFAULT_BLOB_BYTE_LIMIT = 25 * 1024 * 1024  # 25 MB
_BLOB_CONTENT_TYPES = ("application/pdf", "application/octet-stream")


class DocRepoAdapter(BaseAdapter):
    """Adapter for document repositories (PDFs, standards, research papers).

    The expected ``stream_config`` keys are documented inline; sensible
    defaults are applied when a key is absent so a minimal config is
    enough to ingest a well-behaved index page.
    """

    adapter_type: str = "doc_repo"

    def __init__(
        self,
        stream_id: str,
        settings: HydraSettings,
        registry: StreamRegistry | None = None,
        *,
        stream_config: dict[str, Any] | None = None,
        registry_source: StreamSource | None = None,
    ) -> None:
        super().__init__(stream_id, settings, registry)
        self._cfg: dict[str, Any] = stream_config or {}
        self._registry_source: StreamSource | None = (
            registry_source or self._stream_meta.get("source")
        )
        self._log = logger.bind(stream_id=stream_id, adapter_type=self.adapter_type)

    # -- helpers -----------------------------------------------------------

    def _get(self, key: str, default: Any = None) -> Any:
        return self._cfg.get(key, default)

    @property
    def _access_policy(self) -> AccessPolicy:
        raw = (
            self._registry_source.access_policy
            if self._registry_source is not None
            else self._get("access_policy", AccessPolicy.OPEN.value)
        )
        try:
            return AccessPolicy(raw)
        except ValueError:
            return AccessPolicy.OPEN

    def _is_ingestable(self) -> bool:
        """Honour per-source access_policy. ``open`` is always ingestable;
        ``registration`` requires that operator credentials be present.
        Anything else short-circuits."""
        policy = self._access_policy
        if not is_auto_ingestable(policy):
            return False
        if policy == AccessPolicy.REGISTRATION:
            creds = (self.settings.credentials or {}).get(self.stream_id)
            return bool(creds)
        return True

    def _list_url(self) -> str:
        url = self._get("list_url")
        if url:
            return url
        if self._registry_source is not None:
            return self._registry_source.url
        return ""

    def _user_agent(self) -> str:
        return self._get("user_agent", _DEFAULT_USER_AGENT)

    def _build_headers(self) -> dict[str, str]:
        headers = {
            "User-Agent": self._user_agent(),
            "Accept": "text/html,application/xhtml+xml,application/pdf;q=0.9,*/*;q=0.5",
        }
        creds = (self.settings.credentials or {}).get(self.stream_id) or {}
        auth = self._get("auth_pattern", "none")
        if auth == "api_key":
            key_name = self._get("auth_key_name", "X-API-Key")
            api_key = creds.get("api_key", "") if isinstance(creds, dict) else ""
            if api_key:
                headers[key_name] = api_key
        elif auth == "basic_auth" and isinstance(creds, dict):
            import base64

            user = creds.get("username", "")
            pwd = creds.get("password", "")
            if user or pwd:
                token = base64.b64encode(f"{user}:{pwd}".encode()).decode()
                headers["Authorization"] = f"Basic {token}"
        elif auth == "cookie_auth" and isinstance(creds, dict):
            cname = creds.get("cookie_name", "")
            cvalue = creds.get("cookie_value", "")
            if cname:
                headers["Cookie"] = f"{cname}={cvalue}"
        return headers

    # -- fetch -------------------------------------------------------------

    async def fetch(self) -> RawPayload:
        """Fetch the listing page (and walk pagination if configured).

        The blob download phase happens in :meth:`_enrich_with_blobs` during
        :meth:`parse` so we keep ``fetch`` focused on the cheap HTML/JSON
        listing call. This matches the contract of every other adapter
        (``fetch`` returns a single ``RawPayload``).
        """
        if not self._is_ingestable():
            self._log.info(
                "doc_repo_skip_unfetchable",
                access_policy=self._access_policy.value,
            )
            return RawPayload(
                stream_id=self.stream_id,
                fetched_at=datetime.now(timezone.utc),
                content=b"",
                content_type="text/plain",
                http_status=204,
                headers={"x-doc-repo-skipped": self._access_policy.value},
            )

        list_url = self._list_url()
        if not list_url:
            raise FetchError(f"doc_repo stream {self.stream_id!r} has no list_url")

        max_pages = int(self._get("max_list_pages", _DEFAULT_MAX_LIST_PAGES))
        delay = float(self._get("fetch_delay_seconds", _DEFAULT_FETCH_DELAY_SECONDS))
        pagination_selector = self._get("pagination_next_selector", "")
        timeout = aiohttp.ClientTimeout(total=self.settings.http_timeout_seconds)
        headers = self._build_headers()

        accumulated = b""
        ctype = "text/html"
        async with aiohttp.ClientSession(timeout=timeout) as session:
            current_url: str | None = list_url
            for page_idx in range(max_pages):
                if page_idx > 0:
                    await asyncio.sleep(delay)
                if current_url is None:
                    break
                try:
                    async with session.get(current_url, headers=headers) as resp:
                        if resp.status >= 400:
                            raise FetchError(
                                f"List fetch failed: {resp.status} for {current_url}",
                                status_code=resp.status,
                            )
                        body = await resp.read()
                        accumulated += body
                        ctype = resp.headers.get("Content-Type", ctype)
                except aiohttp.ClientError as exc:
                    raise FetchError(f"List fetch error: {exc}") from exc

                if not pagination_selector:
                    break
                soup = BeautifulSoup(body, "lxml")
                next_el = soup.select_one(pagination_selector)
                if next_el and next_el.get("href"):
                    current_url = urljoin(current_url, next_el["href"])
                else:
                    current_url = None

        return RawPayload(
            stream_id=self.stream_id,
            fetched_at=datetime.now(timezone.utc),
            content=accumulated,
            content_type=ctype,
            http_status=200,
            headers={"x-doc-repo-list-url": list_url},
        )

    # -- parse -------------------------------------------------------------

    def parse(self, raw: RawPayload) -> list[dict[str, Any]]:
        """Extract document references from the listing page.

        The default extraction strategy walks every ``<a href>`` and keeps
        anchors whose href ends in a known document extension or matches a
        configured regex (``doc_url_pattern``). Stream configs can override
        this with a CSS ``item_selector`` + ``field_map`` for richer
        listings (similar to scrape_rss).
        """
        if raw.http_status == 204 or not raw.content:
            return []

        item_selector = self._get("item_selector", "")
        if item_selector:
            return self._parse_with_selector(raw, item_selector)
        return self._parse_anchors(raw)

    def _parse_anchors(self, raw: RawPayload) -> list[dict[str, Any]]:
        list_url = self._list_url()
        soup = BeautifulSoup(raw.content, "lxml")
        url_pattern = self._get("doc_url_pattern")
        compiled = re.compile(url_pattern) if url_pattern else None
        extensions = tuple(
            ext.lower()
            for ext in self._get("doc_extensions", [".pdf", ".PDF", ".html", ".htm"])
        )
        max_docs = int(self._get("max_docs_per_run", _DEFAULT_MAX_DOCS_PER_RUN))

        seen: set[str] = set()
        records: list[dict[str, Any]] = []
        for anchor in soup.find_all("a", href=True):
            href = anchor["href"].strip()
            if not href:
                continue
            absolute = urljoin(list_url, href)
            parsed = urlparse(absolute)
            if not parsed.scheme.startswith("http"):
                continue
            path = parsed.path.lower()
            keep = False
            if compiled is not None and compiled.search(absolute):
                keep = True
            elif any(path.endswith(ext) for ext in extensions):
                keep = True
            if not keep:
                continue
            if absolute in seen:
                continue
            seen.add(absolute)

            title = (anchor.get("title") or anchor.get_text() or "").strip()
            records.append(
                {
                    "title": title or absolute.rsplit("/", 1)[-1],
                    "doc_url": absolute,
                    "list_url": list_url,
                    "discovered_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            if len(records) >= max_docs:
                break
        return records

    def _parse_with_selector(self, raw: RawPayload, selector: str) -> list[dict[str, Any]]:
        list_url = self._list_url()
        soup = BeautifulSoup(raw.content, "lxml")
        items = soup.select(selector)
        field_map: dict[str, dict[str, Any]] = self._get("field_map", {}) or {}
        max_docs = int(self._get("max_docs_per_run", _DEFAULT_MAX_DOCS_PER_RUN))

        records: list[dict[str, Any]] = []
        for item in items[:max_docs]:
            rec: dict[str, Any] = {"list_url": list_url}
            for field_name, rule in field_map.items():
                sel = rule.get("selector", "")
                attr = rule.get("attribute", "text")
                el = item.select_one(sel) if sel else None
                if el is None:
                    rec[field_name] = None
                    continue
                if attr == "text":
                    value = el.get_text(strip=True)
                else:
                    raw_attr = el.get(attr, "")
                    value = urljoin(list_url, raw_attr) if attr in ("href", "src") else raw_attr
                rec[field_name] = value
            if rec.get("doc_url"):
                rec["discovered_at"] = datetime.now(timezone.utc).isoformat()
                records.append(rec)
        return records

    # -- validate ----------------------------------------------------------

    def validate(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        valid: list[dict[str, Any]] = []
        seen: set[str] = set()
        for rec in records:
            url = rec.get("doc_url")
            if not url or not isinstance(url, str):
                continue
            parsed = urlparse(url)
            if not parsed.scheme.startswith("http") or not parsed.netloc:
                continue
            if url in seen:
                continue
            seen.add(url)
            valid.append(rec)
        return valid

    # -- normalize ---------------------------------------------------------

    def normalize(self, records: list[dict[str, Any]]) -> list[NormalizedRecord]:
        if not records:
            return []
        tier_id = self.tier_id or self._get("tier", 100)
        source_name = (
            self._registry_source.name
            if self._registry_source is not None
            else self._get("source_name", self.stream_id)
        )
        source_url = (
            self._registry_source.url
            if self._registry_source is not None
            else self._list_url()
        )
        country = self._get("country", "")
        content_type = self._get("content_type", "research_reports")
        access_policy = self._access_policy
        access_policy_value = access_policy.value

        # Optional blob download — defaults off so first-pass ingestion
        # stays quick and the caller can opt in once MinIO is provisioned.
        download_blobs = bool(self._get("download_blobs", False))
        blob_limit = int(self._get("blob_byte_limit", _DEFAULT_BLOB_BYTE_LIMIT))

        normalized: list[NormalizedRecord] = []
        for rec in records:
            doc_url = rec["doc_url"]
            payload: dict[str, Any] = {
                "title": rec.get("title") or doc_url,
                "doc_url": doc_url,
                "list_url": rec.get("list_url", ""),
                "tier": int(tier_id),
                "country": country,
                "content_type": content_type,
                "access_policy": access_policy_value,
                "discovered_at": rec.get("discovered_at"),
                "language": rec.get("language", "en"),
            }

            if download_blobs and is_auto_ingestable(access_policy):
                blob = self._download_blob_sync(doc_url, blob_limit)
                if blob is not None:
                    payload["_binary_artifact"] = blob

            raw_hash = compute_raw_hash(doc_url.encode("utf-8"))
            tags = ["mil_int", source_name, content_type, access_policy_value]
            if country:
                tags.append(country)

            normalized.append(
                NormalizedRecord(
                    stream_id=self.stream_id,
                    tier=Tier(int(tier_id)),
                    timestamp=datetime.now(timezone.utc),
                    geo=None,
                    payload=payload,
                    source_meta=SourceMeta(
                        source_name=source_name,
                        source_url=source_url,
                        adapter_type=self.adapter_type,
                    ),
                    raw_hash=raw_hash,
                    confidence=1.0,
                    tags=tags,
                )
            )
        return normalized

    def _download_blob_sync(
        self, url: str, byte_limit: int
    ) -> dict[str, Any] | None:
        """Download a single document blob via a fresh aiohttp session.

        Synchronous-from-the-caller's-POV via :func:`asyncio.run` because
        :meth:`normalize` is sync; a config-driven adapter loop runs each
        stream as its own task so this nested loop is acceptable.
        """
        try:
            return asyncio.run(self._download_blob(url, byte_limit))
        except RuntimeError:
            # Already inside an event loop (e.g. from a test) — schedule
            # on the running loop and wait. This branch is best-effort and
            # silently drops the blob on failure.
            try:
                loop = asyncio.get_event_loop()
                return loop.run_until_complete(
                    self._download_blob(url, byte_limit)
                )
            except Exception as exc:  # noqa: BLE001
                self._log.warning("doc_repo_blob_loop_error", url=url, error=str(exc))
                return None
        except Exception as exc:  # noqa: BLE001
            self._log.warning("doc_repo_blob_download_failed", url=url, error=str(exc))
            return None

    async def _download_blob(
        self, url: str, byte_limit: int
    ) -> dict[str, Any] | None:
        timeout = aiohttp.ClientTimeout(total=self.settings.http_timeout_seconds)
        headers = self._build_headers()
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=headers) as resp:
                if resp.status >= 400:
                    self._log.warning(
                        "doc_repo_blob_http_error",
                        url=url,
                        status=resp.status,
                    )
                    return None
                ctype = resp.headers.get("Content-Type", "").split(";")[0].strip().lower()
                size = 0
                chunks: list[bytes] = []
                async for chunk in resp.content.iter_chunked(64 * 1024):
                    size += len(chunk)
                    if size > byte_limit:
                        self._log.warning(
                            "doc_repo_blob_truncated",
                            url=url,
                            byte_limit=byte_limit,
                        )
                        return None
                    chunks.append(chunk)
                content = b"".join(chunks)
                return {
                    "content": content,
                    "content_type": ctype or "application/octet-stream",
                    "size_bytes": size,
                    "source_url": url,
                }

    # -- health check ------------------------------------------------------

    async def health_check(self) -> AdapterHealth:
        url = self._list_url()
        start = time.monotonic()
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.head(url, headers=self._build_headers()) as resp:
                    latency = round((time.monotonic() - start) * 1000, 2)
                    status = (
                        HealthStatus.OK
                        if resp.status < 400
                        else HealthStatus.DEGRADED
                    )
                    return AdapterHealth(
                        stream_id=self.stream_id,
                        status=status,
                        latency_ms=latency,
                        last_checked=datetime.now(timezone.utc),
                    )
        except Exception as exc:  # noqa: BLE001
            return AdapterHealth(
                stream_id=self.stream_id,
                status=HealthStatus.UNREACHABLE,
                latency_ms=round((time.monotonic() - start) * 1000, 2),
                last_checked=datetime.now(timezone.utc),
                detail=str(exc),
            )


__all__ = ["DocRepoAdapter"]
