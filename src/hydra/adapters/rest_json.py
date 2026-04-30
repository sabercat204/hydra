"""Generic REST/JSON adapter — covers ~84 streams across most tiers.

Stream-specific behavior is encoded entirely in stream_registry.yaml entries.
Adding a new REST/JSON source requires only a new registry entry, zero code changes.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode, urljoin

import aiohttp
import orjson
import structlog

from hydra.config import HydraSettings
from hydra.registry.stream_registry import StreamRegistry
from hydra.utils.hashing import compute_raw_hash

from .base import BaseAdapter, RawPayload, _resolve_dot_path
from .exceptions import FetchError, ParseError, RateLimitError

logger = structlog.get_logger()


class RestJsonAdapter(BaseAdapter):
    """Configuration-driven REST/JSON adapter."""

    adapter_type: str = "rest_json"

    def __init__(
        self,
        stream_id: str,
        settings: HydraSettings,
        registry: StreamRegistry | None = None,
        *,
        stream_config: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(stream_id, settings, registry)
        # stream_config allows fully config-driven usage without registry lookup
        self._cfg: dict[str, Any] = stream_config or {}
        self._etag: str | None = None
        self._last_modified: str | None = None

    # -- helpers ------------------------------------------------------------

    def _get_cfg(self, key: str, default: Any = None) -> Any:
        return self._cfg.get(key, default)

    def _build_url(self) -> str:
        base = self._get_cfg("base_url", "")
        path = self._get_cfg("endpoint_path", "")
        if base and path:
            # Ensure base ends with / for proper joining
            if not base.endswith("/"):
                base += "/"
            return urljoin(base, path.lstrip("/"))
        return base or path

    def _build_query_params(self, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        params: dict[str, Any] = dict(self._get_cfg("query_params", {}) or {})
        if extra:
            params.update(extra)
        return params

    def _auth_headers(self) -> dict[str, str]:
        pattern = self._get_cfg("auth_pattern", "none")
        if pattern == "none":
            return {}
        if pattern == "api_key":
            key_name = self._get_cfg("auth_key_name", "api_key")
            location = self._get_cfg("auth_key_location", "header")
            # Credential lookup from settings
            cred = self._resolve_credential()
            if location == "header" and cred:
                return {key_name: cred}
        return {}

    def _auth_query_params(self) -> dict[str, str]:
        pattern = self._get_cfg("auth_pattern", "none")
        if pattern == "api_key":
            location = self._get_cfg("auth_key_location", "query")
            if location == "query":
                key_name = self._get_cfg("auth_key_name", "api_key")
                cred = self._resolve_credential()
                if cred:
                    return {key_name: cred}
        return {}

    def _resolve_credential(self) -> str | None:
        """Look up credential for this stream from settings."""
        # Credentials stored as a dict on settings or via env vars
        creds: dict[str, str] = getattr(self.settings, "credentials", {}) or {}
        return creds.get(self.stream_id)

    # -- fetch --------------------------------------------------------------

    async def fetch(self) -> RawPayload:
        """Fetch data from the REST/JSON endpoint, handling pagination and conditional requests."""
        url = self._build_url()
        params = self._build_query_params(self._auth_query_params())
        headers: dict[str, str] = {
            "User-Agent": f"HYDRA/0.1.0",
            "Accept": "application/json",
            **self._auth_headers(),
        }

        # Conditional request headers
        supports_conditional = self._get_cfg("supports_conditional", False)
        if supports_conditional:
            if self._etag:
                headers["If-None-Match"] = self._etag
            if self._last_modified:
                headers["If-Modified-Since"] = self._last_modified

        timeout = aiohttp.ClientTimeout(
            total=getattr(self.settings, "http_timeout_seconds", 30),
        )

        pagination_type = self._get_cfg("pagination_type")
        max_pages = self._get_cfg("max_pages", 10)

        all_content = bytearray()
        final_status = 200
        final_headers: dict[str, str] = {}
        final_content_type = "application/json"

        async with aiohttp.ClientSession(timeout=timeout) as session:
            page = 0
            cursor: str | None = None

            while page < max_pages:
                page_params = dict(params)
                if pagination_type and page > 0:
                    page_params.update(self._pagination_params(pagination_type, page, cursor))

                full_url = url
                if page_params:
                    full_url = f"{url}?{urlencode(page_params, doseq=True)}"

                try:
                    async with session.get(full_url, headers=headers) as resp:
                        final_status = resp.status
                        final_headers = {k: v for k, v in resp.headers.items()}
                        final_content_type = resp.content_type or "application/json"

                        if resp.status == 304:
                            return RawPayload(
                                stream_id=self.stream_id,
                                fetched_at=datetime.now(timezone.utc),
                                content=b"",
                                content_type=final_content_type,
                                http_status=304,
                                headers=final_headers,
                            )

                        if resp.status == 429:
                            retry_after = float(resp.headers.get("Retry-After", "1"))
                            raise RateLimitError(
                                f"Rate limited on {self.stream_id}",
                                retry_after=retry_after,
                            )

                        if resp.status >= 500:
                            raise FetchError(
                                f"Server error {resp.status} from {self.stream_id}",
                                status_code=resp.status,
                            )

                        if resp.status >= 400:
                            raise FetchError(
                                f"Client error {resp.status} from {self.stream_id}",
                                status_code=resp.status,
                            )

                        body = await resp.read()
                        all_content.extend(body)

                        # Cache conditional headers
                        if supports_conditional:
                            if "ETag" in resp.headers:
                                self._etag = resp.headers["ETag"]
                            if "Last-Modified" in resp.headers:
                                self._last_modified = resp.headers["Last-Modified"]

                except aiohttp.ClientError as exc:
                    raise FetchError(f"Connection error fetching {self.stream_id}: {exc}") from exc

                # Check if we should continue paginating
                if not pagination_type:
                    break

                cursor = self._extract_next_cursor(pagination_type, body, final_headers)
                if cursor is None and pagination_type in ("cursor", "link_header"):
                    break

                page += 1
                if pagination_type in ("offset", "page_number") and len(body) == 0:
                    break

        content = bytes(all_content)
        return RawPayload(
            stream_id=self.stream_id,
            fetched_at=datetime.now(timezone.utc),
            content=content,
            content_type=final_content_type,
            http_status=final_status,
            headers=final_headers,
        )

    def _pagination_params(self, ptype: str, page: int, cursor: str | None) -> dict[str, Any]:
        param_name = self._get_cfg("pagination_param", "offset")
        limit = self._get_cfg("pagination_limit", 100)

        if ptype == "offset":
            return {param_name: page * limit, "limit": limit}
        if ptype == "page_number":
            return {param_name: page + 1, "limit": limit}
        if ptype == "cursor" and cursor:
            return {param_name: cursor}
        return {}

    def _extract_next_cursor(
        self, ptype: str, body: bytes, headers: dict[str, str]
    ) -> str | None:
        if ptype == "cursor":
            try:
                data = orjson.loads(body)
                return data.get("next_cursor") or data.get("cursor") or data.get("next")
            except Exception:
                return None
        if ptype == "link_header":
            link = headers.get("Link", "")
            # Parse Link header for rel="next"
            for part in link.split(","):
                if 'rel="next"' in part:
                    url_part = part.split(";")[0].strip().strip("<>")
                    return url_part
            return None
        return None

    # -- parse --------------------------------------------------------------

    def parse(self, raw: RawPayload) -> list[dict[str, Any]]:
        """Decode JSON and extract records using response_root_path."""
        if not raw.content:
            return []

        try:
            data = orjson.loads(raw.content)
        except Exception as exc:
            raise ParseError(f"Failed to decode JSON for {self.stream_id}: {exc}") from exc

        # GeoJSON auto-detection
        fmt = self._get_cfg("format", "json")
        is_geojson = fmt == "geojson" or (raw.content_type and "geo+json" in raw.content_type)

        root_path = self._get_cfg("response_root_path")

        if is_geojson and not root_path:
            root_path = "features"

        if root_path:
            data = _resolve_dot_path(data, root_path) if isinstance(data, dict) else data

        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return [data]
        return []

    # -- validate -----------------------------------------------------------

    def validate(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Apply required-field checks, type coercion, and intra-batch dedup."""
        required = self._get_cfg("required_fields", []) or []
        field_types = self._get_cfg("field_types", {}) or {}

        valid: list[dict[str, Any]] = []
        seen_hashes: set[str] = set()

        for rec in records:
            # Required field check
            if not self._check_required(rec, required):
                self._log.warning("validation_dropped", reason="missing_required_field", record=rec)
                continue

            # Type coercion
            if not self._coerce_types(rec, field_types):
                self._log.warning("validation_dropped", reason="type_coercion_failed", record=rec)
                continue

            # Intra-batch dedup
            rec_hash = compute_raw_hash(orjson.dumps(rec, option=orjson.OPT_SORT_KEYS))
            if rec_hash in seen_hashes:
                self._log.warning("validation_dropped", reason="duplicate", record_hash=rec_hash)
                continue
            seen_hashes.add(rec_hash)

            valid.append(rec)

        return valid

    @staticmethod
    def _check_required(rec: dict[str, Any], required: list[str]) -> bool:
        for field_path in required:
            val = _resolve_dot_path(rec, field_path)
            if val is None:
                return False
        return True

    @staticmethod
    def _coerce_types(rec: dict[str, Any], field_types: dict[str, str]) -> bool:
        for field_path, expected_type in field_types.items():
            val = _resolve_dot_path(rec, field_path)
            if val is None:
                continue
            try:
                if expected_type == "float":
                    coerced = float(val)
                elif expected_type == "int":
                    coerced = int(val)
                elif expected_type == "str":
                    coerced = str(val)
                elif expected_type == "iso8601":
                    if isinstance(val, str):
                        datetime.fromisoformat(val)
                    coerced = val
                else:
                    coerced = val
                # Write back coerced value
                _set_dot_path(rec, field_path, coerced)
            except (ValueError, TypeError):
                return False
        return True


def _set_dot_path(data: dict[str, Any], path: str, value: Any) -> None:
    """Set a value at a dot-delimited path in a nested dict."""
    parts = path.split(".")
    current = data
    for part in parts[:-1]:
        if isinstance(current, dict):
            current = current.setdefault(part, {})
        else:
            return
    if isinstance(current, dict):
        current[parts[-1]] = value
