"""CKAN portal adapter — covers ~15 national open-data portals plus HDX.

Targets CKAN Action API v3 endpoints: package_search, package_show, datastore_search.
Supports resource file downloads (CSV, JSON, XLS/XLSX).
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import datetime, timezone
from typing import Any

import aiohttp
import orjson
import structlog

from hydra.config import HydraSettings
from hydra.registry.stream_registry import StreamRegistry
from hydra.utils.hashing import compute_raw_hash

from .base import BaseAdapter, RawPayload
from .exceptions import FetchError, ParseError, RateLimitError

logger = structlog.get_logger()


class CkanAdapter(BaseAdapter):
    """Configuration-driven CKAN portal adapter."""

    adapter_type: str = "ckan"

    def __init__(
        self,
        stream_id: str,
        settings: HydraSettings,
        registry: StreamRegistry | None = None,
        *,
        stream_config: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(stream_id, settings, registry)
        self._cfg: dict[str, Any] = stream_config or {}

    # -- helpers ------------------------------------------------------------

    def _get_cfg(self, key: str, default: Any = None) -> Any:
        return self._cfg.get(key, default)

    def _auth_headers(self) -> dict[str, str]:
        pattern = self._get_cfg("auth_pattern", "none")
        if pattern == "api_key":
            key_name = self._get_cfg("auth_key_name", "Authorization")
            creds: dict[str, str] = getattr(self.settings, "credentials", {}) or {}
            cred = creds.get(self.stream_id)
            if cred:
                return {key_name: cred}
        return {}

    # -- fetch --------------------------------------------------------------

    async def fetch(self) -> RawPayload:
        """Fetch data from a CKAN portal, handling pagination and resource downloads."""
        base_url = self._get_cfg("base_url", "").rstrip("/")
        action = self._get_cfg("ckan_action", "package_search")
        max_pages = self._get_cfg("max_pages", 10)

        headers: dict[str, str] = {
            "User-Agent": "HYDRA/0.1.0",
            "Accept": "application/json",
            **self._auth_headers(),
        }

        timeout = aiohttp.ClientTimeout(total=getattr(self.settings, "http_timeout_seconds", 30))

        all_payloads: list[RawPayload] = []

        async with aiohttp.ClientSession(timeout=timeout) as session:
            if action == "package_search":
                all_payloads = await self._fetch_package_search(session, base_url, headers, max_pages)
            elif action == "package_show":
                all_payloads = await self._fetch_package_show(session, base_url, headers)
            elif action == "datastore_search":
                all_payloads = await self._fetch_datastore_search(session, base_url, headers, max_pages)
            else:
                raise FetchError(f"Unknown ckan_action: {action}")

            # Resource downloads if configured
            if self._get_cfg("download_resources", False) and all_payloads:
                resource_payloads = await self._download_resources(session, all_payloads, headers)
                all_payloads.extend(resource_payloads)

        # Merge all payloads into a single RawPayload with combined content
        if not all_payloads:
            return RawPayload(
                stream_id=self.stream_id,
                fetched_at=datetime.now(timezone.utc),
                content=b"[]",
                content_type="application/json",
                http_status=200,
            )

        combined = self._combine_payloads(all_payloads)
        return combined

    async def _fetch_package_search(
        self,
        session: aiohttp.ClientSession,
        base_url: str,
        headers: dict[str, str],
        max_pages: int,
    ) -> list[RawPayload]:
        """Paginate through package_search results."""
        search_query = self._get_cfg("search_query", "")
        filter_tags = self._get_cfg("filter_tags", [])
        rows = self._get_cfg("rows", 100)

        payloads: list[RawPayload] = []
        start = 0

        for _ in range(max_pages):
            params: dict[str, Any] = {"start": start, "rows": rows}
            if search_query:
                params["q"] = search_query
            if filter_tags:
                params["fq"] = " AND ".join(f"tags:{tag}" for tag in filter_tags)

            url = f"{base_url}/api/3/action/package_search"
            payload = await self._do_get(session, url, headers, params)
            payloads.append(payload)

            # Check if more pages
            try:
                data = orjson.loads(payload.content)
                result = data.get("result", {})
                count = result.get("count", 0)
                results_list = result.get("results", [])
                start += len(results_list)
                if start >= count or not results_list:
                    break
            except Exception:
                break

        return payloads

    async def _fetch_package_show(
        self,
        session: aiohttp.ClientSession,
        base_url: str,
        headers: dict[str, str],
    ) -> list[RawPayload]:
        """Fetch a single dataset by package_id."""
        package_id = self._get_cfg("package_id", "")
        url = f"{base_url}/api/3/action/package_show"
        payload = await self._do_get(session, url, headers, {"id": package_id})
        return [payload]

    async def _fetch_datastore_search(
        self,
        session: aiohttp.ClientSession,
        base_url: str,
        headers: dict[str, str],
        max_pages: int,
    ) -> list[RawPayload]:
        """Paginate through datastore_search results."""
        resource_id = self._get_cfg("resource_id", "")
        limit = self._get_cfg("limit", 100)

        payloads: list[RawPayload] = []
        offset = 0

        for _ in range(max_pages):
            params: dict[str, Any] = {"resource_id": resource_id, "offset": offset, "limit": limit}
            url = f"{base_url}/api/3/action/datastore_search"
            payload = await self._do_get(session, url, headers, params)
            payloads.append(payload)

            try:
                data = orjson.loads(payload.content)
                result = data.get("result", {})
                records = result.get("records", [])
                total = result.get("total", 0)
                offset += len(records)
                if offset >= total or not records:
                    break
            except Exception:
                break

        return payloads

    async def _do_get(
        self,
        session: aiohttp.ClientSession,
        url: str,
        headers: dict[str, str],
        params: dict[str, Any] | None = None,
    ) -> RawPayload:
        """Execute a single GET request with error handling."""
        try:
            async with session.get(url, headers=headers, params=params) as resp:
                if resp.status == 429:
                    retry_after = float(resp.headers.get("Retry-After", "1"))
                    raise RateLimitError(f"Rate limited on {self.stream_id}", retry_after=retry_after)
                if resp.status >= 500:
                    raise FetchError(f"Server error {resp.status} from {self.stream_id}", status_code=resp.status)
                if resp.status >= 400:
                    raise FetchError(f"Client error {resp.status} from {self.stream_id}", status_code=resp.status)

                body = await resp.read()
                return RawPayload(
                    stream_id=self.stream_id,
                    fetched_at=datetime.now(timezone.utc),
                    content=body,
                    content_type=resp.content_type or "application/json",
                    http_status=resp.status,
                    headers={k: v for k, v in resp.headers.items()},
                )
        except aiohttp.ClientError as exc:
            raise FetchError(f"Connection error fetching {self.stream_id}: {exc}") from exc

    async def _download_resources(
        self,
        session: aiohttp.ClientSession,
        payloads: list[RawPayload],
        headers: dict[str, str],
    ) -> list[RawPayload]:
        """Download resource files referenced in package metadata."""
        format_filter = self._get_cfg("resource_format_filter")
        if format_filter:
            format_filter = [f.lower() for f in format_filter]

        resource_urls: list[tuple[str, str]] = []
        for payload in payloads:
            try:
                data = orjson.loads(payload.content)
                result = data.get("result", data)
                # Handle both package_show (single result) and package_search (list)
                datasets = result.get("results", [result]) if isinstance(result, dict) else [result]
                for ds in datasets:
                    if not isinstance(ds, dict):
                        continue
                    for resource in ds.get("resources", []):
                        fmt = resource.get("format", "").lower()
                        url = resource.get("url", "")
                        if not url:
                            continue
                        if format_filter and fmt not in format_filter:
                            continue
                        resource_urls.append((url, fmt))
            except Exception:
                continue

        downloaded: list[RawPayload] = []
        for url, fmt in resource_urls:
            try:
                rp = await self._do_get(session, url, headers)
                # Tag the content type based on format
                rp = RawPayload(
                    stream_id=rp.stream_id,
                    fetched_at=rp.fetched_at,
                    content=rp.content,
                    content_type=f"resource/{fmt}" if fmt else rp.content_type,
                    http_status=rp.http_status,
                    headers={**rp.headers, "_resource_format": fmt, "_resource_url": url},
                )
                downloaded.append(rp)
            except Exception as exc:
                self._log.warning("resource_download_failed", url=url, error=str(exc))
        return downloaded

    def _combine_payloads(self, payloads: list[RawPayload]) -> RawPayload:
        """Combine multiple RawPayloads into one by storing them as a JSON array wrapper."""
        # Store the raw payloads list as metadata on the first payload
        # We use a special content structure for the combined payload
        first = payloads[0]
        combined_content = orjson.dumps({
            "_combined": True,
            "_payloads": [
                {
                    "content": p.content.decode("utf-8", errors="replace"),
                    "content_type": p.content_type,
                    "http_status": p.http_status,
                    "headers": p.headers,
                }
                for p in payloads
            ],
        })
        return RawPayload(
            stream_id=self.stream_id,
            fetched_at=first.fetched_at,
            content=combined_content,
            content_type="application/json",
            http_status=first.http_status,
            headers=first.headers,
        )

    # -- parse --------------------------------------------------------------

    def parse(self, raw: RawPayload) -> list[dict[str, Any]]:
        """Parse CKAN API responses and downloaded resource files."""
        if not raw.content:
            return []

        try:
            wrapper = orjson.loads(raw.content)
        except Exception as exc:
            raise ParseError(f"Failed to decode response for {self.stream_id}: {exc}") from exc

        action = self._get_cfg("ckan_action", "package_search")
        portal_id = self._get_cfg("portal_id", self.stream_id)

        records: list[dict[str, Any]] = []

        # Handle combined payloads
        if isinstance(wrapper, dict) and wrapper.get("_combined"):
            for entry in wrapper.get("_payloads", []):
                content_type = entry.get("content_type", "")
                content_str = entry.get("content", "")
                entry_headers = entry.get("headers", {})
                content_bytes = content_str.encode("utf-8", errors="replace")

                if content_type.startswith("resource/"):
                    fmt = content_type.split("/", 1)[1]
                    resource_id = entry_headers.get("_resource_url", "")
                    parsed = self._parse_resource(content_bytes, fmt)
                    for rec in parsed:
                        rec["portal_id"] = portal_id
                        rec["resource_id"] = resource_id
                    records.extend(parsed)
                else:
                    parsed = self._parse_api_response(content_bytes, action)
                    for rec in parsed:
                        rec["portal_id"] = portal_id
                    records.extend(parsed)
        else:
            # Single payload
            parsed = self._parse_api_response(raw.content, action)
            for rec in parsed:
                rec["portal_id"] = portal_id
            records.extend(parsed)

        return records

    def _parse_api_response(self, content: bytes, action: str) -> list[dict[str, Any]]:
        """Parse a CKAN API JSON response."""
        try:
            data = orjson.loads(content)
        except Exception:
            return []

        result = data.get("result", data)

        if action == "datastore_search":
            if isinstance(result, dict):
                return list(result.get("records", []))
            return []

        # package_search or package_show
        if isinstance(result, dict):
            results = result.get("results", None)
            if results is not None:
                return self._extract_dataset_metadata(results)
            # Single dataset (package_show)
            return self._extract_dataset_metadata([result])

        if isinstance(result, list):
            return self._extract_dataset_metadata(result)

        return []

    def _extract_dataset_metadata(self, datasets: list[Any]) -> list[dict[str, Any]]:
        """Extract metadata from dataset objects."""
        records: list[dict[str, Any]] = []
        for ds in datasets:
            if not isinstance(ds, dict):
                continue
            records.append({
                "id": ds.get("id", ""),
                "title": ds.get("title", ""),
                "description": ds.get("notes", ""),
                "resources": ds.get("resources", []),
                "tags": [t.get("name", "") if isinstance(t, dict) else t for t in ds.get("tags", [])],
                "organization": ds.get("organization", {}).get("title", "") if isinstance(ds.get("organization"), dict) else "",
                "update_frequency": ds.get("update_frequency", ds.get("metadata_modified", "")),
                "metadata_modified": ds.get("metadata_modified", ""),
            })
        return records

    def _parse_resource(self, content: bytes, fmt: str) -> list[dict[str, Any]]:
        """Parse a downloaded resource file based on format."""
        fmt = fmt.lower()
        if fmt == "csv":
            return self._parse_csv(content)
        elif fmt == "json":
            return self._parse_json(content)
        elif fmt in ("xls", "xlsx"):
            return self._parse_xlsx(content)
        return []

    def _parse_csv(self, content: bytes) -> list[dict[str, Any]]:
        """Parse CSV content into list of dicts."""
        text = self._decode_content(content)
        reader = csv.DictReader(io.StringIO(text))
        return [dict(row) for row in reader]

    def _parse_json(self, content: bytes) -> list[dict[str, Any]]:
        """Parse JSON content."""
        try:
            data = orjson.loads(content)
        except Exception:
            return []
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return [data]
        return []

    def _parse_xlsx(self, content: bytes) -> list[dict[str, Any]]:
        """Parse XLS/XLSX content using openpyxl."""
        try:
            import openpyxl
        except ImportError:
            self._log.warning("openpyxl_not_installed", msg="Cannot parse XLSX without openpyxl")
            return []

        wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
        ws = wb.active
        if ws is None:
            return []

        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            return []

        headers = [str(h) if h is not None else f"col_{i}" for i, h in enumerate(rows[0])]
        records: list[dict[str, Any]] = []
        for row in rows[1:]:
            records.append(dict(zip(headers, row)))
        return records

    @staticmethod
    def _decode_content(content: bytes) -> str:
        """Decode bytes to UTF-8, falling back to Latin-1."""
        try:
            return content.decode("utf-8")
        except UnicodeDecodeError:
            return content.decode("latin-1")

    # -- validate -----------------------------------------------------------

    def validate(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Validate records: required fields, schema drift detection, encoding normalization."""
        required_fields = self._get_cfg("required_fields", []) or []
        valid: list[dict[str, Any]] = []
        reference_fields: set[str] | None = None

        for i, rec in enumerate(records):
            # Encoding normalization on string values
            rec = self._normalize_encoding(rec)

            # Required field check
            if not self._check_required(rec, required_fields):
                self._log.warning("validation_dropped", reason="missing_required_field", index=i)
                continue

            # Schema drift detection
            if reference_fields is None and i == 0:
                reference_fields = set(rec.keys())
            elif reference_fields is not None:
                current_fields = set(rec.keys())
                overlap = current_fields & reference_fields
                if reference_fields and len(overlap) < len(reference_fields) * 0.5:
                    self._log.warning(
                        "schema_drift_detected",
                        index=i,
                        expected_fields=len(reference_fields),
                        present_fields=len(overlap),
                    )

            valid.append(rec)

        return valid

    @staticmethod
    def _check_required(rec: dict[str, Any], required: list[str]) -> bool:
        for field_name in required:
            parts = field_name.split(".")
            current: Any = rec
            for part in parts:
                if isinstance(current, dict):
                    current = current.get(part)
                else:
                    return False
            if current is None:
                return False
        return True

    @staticmethod
    def _normalize_encoding(rec: dict[str, Any]) -> dict[str, Any]:
        """Force UTF-8 encoding on string values."""
        normalized: dict[str, Any] = {}
        for k, v in rec.items():
            if isinstance(v, str):
                # Re-encode to handle Latin-1 mislabeled as UTF-8
                try:
                    normalized[k] = v.encode("utf-8").decode("utf-8")
                except (UnicodeEncodeError, UnicodeDecodeError):
                    normalized[k] = v.encode("latin-1", errors="replace").decode("utf-8", errors="replace")
            else:
                normalized[k] = v
        return normalized
