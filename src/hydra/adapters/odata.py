"""OData v4 adapter — entity set queries with $filter/$select/$expand/$orderby,
server-driven and skip-based pagination, CSDL metadata discovery,
OAuth2 client credentials support.

Stream-specific behavior is encoded entirely in stream_registry.yaml entries.
Adding a new OData source requires only a new registry entry, zero code changes.
"""

from __future__ import annotations

import asyncio
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urlencode, urljoin

import aiohttp
import orjson
import structlog

from hydra.config import HydraSettings
from hydra.registry.stream_registry import StreamRegistry
from hydra.utils.hashing import compute_raw_hash

from .base import BaseAdapter, RawPayload
from .exceptions import FetchError, ParseError, RateLimitError

logger = structlog.get_logger()

# OData CSDL XML namespace
_CSDL_NS = "http://docs.oasis-open.org/odata/ns/edm"


class ODataAdapter(BaseAdapter):
    """Configuration-driven OData v4 adapter."""

    adapter_type: str = "odata"

    def __init__(
        self,
        stream_id: str,
        settings: HydraSettings,
        registry: StreamRegistry | None = None,
        *,
        stream_config: dict[str, Any] | None = None,
        last_fetch_time: str | None = None,
    ) -> None:
        super().__init__(stream_id, settings, registry)
        self._cfg: dict[str, Any] = stream_config or {}
        self._last_fetch_time = last_fetch_time
        self._metadata_properties: dict[str, str] | None = None
        self._oauth_token: str | None = None
        self._oauth_expires_at: float = 0.0

    # -- helpers ------------------------------------------------------------

    def _get_cfg(self, key: str, default: Any = None) -> Any:
        return self._cfg.get(key, default)

    def _resolve_credential(self, key: str | None = None) -> str | None:
        creds: dict[str, Any] = getattr(self.settings, "credentials", {}) or {}
        entry = creds.get(key or self.stream_id, {})
        if isinstance(entry, str):
            return entry
        return None

    def _resolve_credential_dict(self) -> dict[str, str]:
        creds: dict[str, Any] = getattr(self.settings, "credentials", {}) or {}
        entry = creds.get(self.stream_id, {})
        if isinstance(entry, dict):
            return entry
        return {}

    def _build_entity_url(self) -> str:
        base = self._get_cfg("base_url", "")
        entity_set = self._get_cfg("entity_set", "")
        if not base.endswith("/"):
            base += "/"
        return urljoin(base, entity_set)

    def _build_query_options(self, skip: int | None = None) -> dict[str, str]:
        opts: dict[str, str] = {}

        # Static filter
        odata_filter = self._get_cfg("odata_filter")
        dynamic_filter = self._get_cfg("odata_dynamic_filter")

        filter_parts: list[str] = []
        if odata_filter:
            filter_parts.append(odata_filter)
        if dynamic_filter and self._last_fetch_time:
            resolved = dynamic_filter.replace("{last_fetch_time}", self._last_fetch_time)
            filter_parts.append(resolved)

        if filter_parts:
            opts["$filter"] = " and ".join(filter_parts) if len(filter_parts) > 1 else filter_parts[0]

        select = self._get_cfg("odata_select")
        if select:
            opts["$select"] = select

        orderby = self._get_cfg("odata_orderby")
        if orderby:
            opts["$orderby"] = orderby

        top = self._get_cfg("odata_top", 1000)
        opts["$top"] = str(top)

        expand = self._get_cfg("odata_expand")
        if expand:
            opts["$expand"] = expand

        if self._get_cfg("odata_count", False):
            opts["$count"] = "true"

        if skip is not None:
            opts["$skip"] = str(skip)

        return opts

    def _odata_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {
            "Accept": "application/json;odata.metadata=minimal",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
            "User-Agent": "HYDRA/0.1.0",
        }
        return headers

    async def _auth_headers(self, session: aiohttp.ClientSession) -> dict[str, str]:
        pattern = self._get_cfg("auth_pattern", "none")
        if pattern == "none":
            return {}
        if pattern == "api_key":
            key_name = self._get_cfg("auth_key_name", "api_key")
            location = self._get_cfg("auth_key_location", "header")
            cred = self._resolve_credential()
            if location == "header" and cred:
                return {key_name: cred}
        if pattern == "oauth2_client_credentials":
            token = await self._get_oauth_token(session)
            return {"Authorization": f"Bearer {token}"}
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

    async def _get_oauth_token(self, session: aiohttp.ClientSession, force_refresh: bool = False) -> str:
        now = time.monotonic()
        if self._oauth_token and not force_refresh and now < self._oauth_expires_at:
            return self._oauth_token

        cred_dict = self._resolve_credential_dict()
        client_id = cred_dict.get("client_id", "")
        client_secret = cred_dict.get("client_secret", "")
        token_url = cred_dict.get("token_url", "")

        data = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }

        async with session.post(token_url, data=data) as resp:
            if resp.status != 200:
                raise FetchError(f"OAuth2 token request failed with status {resp.status}", status_code=resp.status)
            body = await resp.json()

        self._oauth_token = body["access_token"]
        expires_in = body.get("expires_in", 3600)
        self._oauth_expires_at = now + expires_in - 60  # 60s buffer
        return self._oauth_token

    # -- metadata discovery -------------------------------------------------

    async def _discover_metadata(self, session: aiohttp.ClientSession, headers: dict[str, str]) -> None:
        if not self._get_cfg("odata_discover", False):
            return
        if self._metadata_properties is not None:
            return

        base = self._get_cfg("base_url", "")
        if not base.endswith("/"):
            base += "/"
        metadata_url = urljoin(base, "$metadata")

        try:
            async with session.get(metadata_url, headers={**headers, "Accept": "application/xml"}) as resp:
                if resp.status != 200:
                    self._log.warning("metadata_discovery_failed", status=resp.status)
                    return
                xml_bytes = await resp.read()
        except aiohttp.ClientError as exc:
            self._log.warning("metadata_discovery_error", error=str(exc))
            return

        self._metadata_properties = self._parse_csdl(xml_bytes)

    @staticmethod
    def _parse_csdl(xml_bytes: bytes) -> dict[str, str]:
        """Parse CSDL XML and extract property name → Edm type mapping."""
        props: dict[str, str] = {}
        try:
            root = ET.fromstring(xml_bytes)
            # Search for Property elements across all EntityType definitions
            for elem in root.iter():
                tag = elem.tag
                # Handle namespaced and non-namespaced tags
                local = tag.split("}")[-1] if "}" in tag else tag
                if local == "Property":
                    name = elem.get("Name", "")
                    ptype = elem.get("Type", "")
                    if name:
                        props[name] = ptype
        except ET.ParseError:
            pass
        return props

    # -- fetch --------------------------------------------------------------

    async def fetch(self) -> RawPayload:
        base_headers = self._odata_headers()
        timeout = aiohttp.ClientTimeout(total=getattr(self.settings, "http_timeout_seconds", 30))
        pagination = self._get_cfg("odata_pagination", "next_link")
        max_pages = self._get_cfg("max_pages", 50)
        page_size = self._get_cfg("odata_top", 1000)

        pages: list[Any] = []

        async with aiohttp.ClientSession(timeout=timeout) as session:
            auth_hdrs = await self._auth_headers(session)
            headers = {**base_headers, **auth_hdrs}

            await self._discover_metadata(session, headers)

            auth_qp = self._auth_query_params()

            if pagination == "skip_top":
                pages = await self._fetch_skip_top(session, headers, auth_qp, max_pages, page_size)
            else:
                pages = await self._fetch_next_link(session, headers, auth_qp, max_pages)

        content = orjson.dumps(pages)
        return RawPayload(
            stream_id=self.stream_id,
            fetched_at=datetime.now(timezone.utc),
            content=content,
            content_type="application/json",
            http_status=200,
            headers={},
        )

    async def _fetch_page(
        self,
        session: aiohttp.ClientSession,
        url: str,
        headers: dict[str, str],
    ) -> dict[str, Any]:
        try:
            async with session.get(url, headers=headers) as resp:
                if resp.status == 429:
                    retry_after = float(resp.headers.get("Retry-After", "1"))
                    raise RateLimitError(f"Rate limited on {self.stream_id}", retry_after=retry_after)

                body = await resp.read()

                if resp.status >= 400:
                    # Try to parse OData error body
                    error_msg = self._extract_odata_error(body, resp.status)
                    raise FetchError(error_msg, status_code=resp.status)

                return orjson.loads(body)
        except aiohttp.ClientError as exc:
            raise FetchError(f"Connection error fetching {self.stream_id}: {exc}") from exc

    async def _fetch_page_with_401_retry(
        self,
        session: aiohttp.ClientSession,
        url: str,
        headers: dict[str, str],
    ) -> dict[str, Any]:
        """Fetch a page, retrying once on 401 with a refreshed OAuth token."""
        try:
            return await self._fetch_page(session, url, headers)
        except FetchError as exc:
            if exc.status_code == 401 and self._get_cfg("auth_pattern") == "oauth2_client_credentials":
                token = await self._get_oauth_token(session, force_refresh=True)
                headers = {**headers, "Authorization": f"Bearer {token}"}
                return await self._fetch_page(session, url, headers)
            raise

    async def _fetch_next_link(
        self,
        session: aiohttp.ClientSession,
        headers: dict[str, str],
        auth_qp: dict[str, str],
        max_pages: int,
    ) -> list[dict[str, Any]]:
        query_opts = self._build_query_options()
        if auth_qp:
            query_opts.update(auth_qp)

        entity_url = self._build_entity_url()
        url = f"{entity_url}?{urlencode(query_opts)}"

        pages: list[dict[str, Any]] = []
        for _ in range(max_pages):
            data = await self._fetch_page_with_401_retry(session, url, headers)
            pages.append(data)

            next_link = data.get("@odata.nextLink")
            if not next_link:
                break
            url = next_link

        return pages

    async def _fetch_skip_top(
        self,
        session: aiohttp.ClientSession,
        headers: dict[str, str],
        auth_qp: dict[str, str],
        max_pages: int,
        page_size: int,
    ) -> list[dict[str, Any]]:
        entity_url = self._build_entity_url()
        pages: list[dict[str, Any]] = []

        for page_num in range(max_pages):
            skip = page_num * page_size
            query_opts = self._build_query_options(skip=skip)
            if auth_qp:
                query_opts.update(auth_qp)

            url = f"{entity_url}?{urlencode(query_opts)}"
            data = await self._fetch_page_with_401_retry(session, url, headers)
            pages.append(data)

            records = data.get("value", [])
            if len(records) < page_size:
                break

        return pages

    @staticmethod
    def _extract_odata_error(body: bytes, status: int) -> str:
        try:
            data = orjson.loads(body)
            error = data.get("error", {})
            code = error.get("code", "")
            message = error.get("message", "")
            if message:
                return f"OData error {status} [{code}]: {message}"
        except Exception:
            pass
        return f"HTTP {status} error"

    # -- parse --------------------------------------------------------------

    def parse(self, raw: RawPayload) -> list[dict[str, Any]]:
        if not raw.content:
            return []

        try:
            pages = orjson.loads(raw.content)
        except Exception as exc:
            raise ParseError(f"Failed to decode JSON for {self.stream_id}: {exc}") from exc

        expand_sep = self._get_cfg("expand_flatten_separator", "_")
        entity_set = self._get_cfg("entity_set", "")
        base_url = self._get_cfg("base_url", "")

        records: list[dict[str, Any]] = []
        for page in pages:
            items = page.get("value", [page] if isinstance(page, dict) and "value" not in page else [])
            for item in items:
                cleaned = self._strip_odata_annotations(item)
                cleaned = self._flatten_expanded(cleaned, expand_sep)
                cleaned = self._convert_odata_types(cleaned)
                cleaned["odata_entity_set"] = entity_set
                cleaned["odata_service_url"] = base_url
                records.append(cleaned)

        return records

    @staticmethod
    def _strip_odata_annotations(record: dict[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in record.items() if not k.startswith("@odata.") and not k.startswith("odata.")}

    @staticmethod
    def _flatten_expanded(record: dict[str, Any], separator: str) -> dict[str, Any]:
        flat: dict[str, Any] = {}
        for key, value in record.items():
            if isinstance(value, dict):
                for sub_key, sub_val in value.items():
                    if not sub_key.startswith("@odata."):
                        flat[f"{key}{separator}{sub_key}"] = sub_val
            else:
                flat[key] = value
        return flat

    @staticmethod
    def _convert_odata_types(record: dict[str, Any]) -> dict[str, Any]:
        converted: dict[str, Any] = {}
        for key, value in record.items():
            if value is None:
                converted[key] = None
            elif isinstance(value, str):
                # Try DateTimeOffset parse
                if len(value) >= 19 and "T" in value:
                    try:
                        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                        converted[key] = dt
                        continue
                    except (ValueError, TypeError):
                        pass
                # Try Decimal (string numbers with high precision)
                # Only convert if it looks like a pure number
                converted[key] = value
            else:
                converted[key] = value
        return converted

    # -- validate -----------------------------------------------------------

    def validate(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        required = self._get_cfg("required_fields", []) or []
        non_nullable = self._get_cfg("non_nullable_fields", []) or []
        dedup_keys = self._get_cfg("dedup_key_fields", []) or []

        valid: list[dict[str, Any]] = []
        seen_hashes: set[str] = set()
        seen_dedup_keys: set[str] = set()

        for rec in records:
            # Required field check
            if not self._check_required(rec, required):
                self._log.warning("validation_dropped", reason="missing_required_field")
                continue

            # Non-nullable check
            if not self._check_non_nullable(rec, non_nullable):
                self._log.warning("validation_dropped", reason="null_in_non_nullable_field")
                continue

            # Schema validation against CSDL metadata
            if self._metadata_properties:
                if not self._validate_against_metadata(rec):
                    continue

            # Deduplication
            if dedup_keys:
                dedup_val = "|".join(str(rec.get(k, "")) for k in dedup_keys)
                if dedup_val in seen_dedup_keys:
                    self._log.warning("validation_dropped", reason="duplicate_dedup_key")
                    continue
                seen_dedup_keys.add(dedup_val)
            else:
                rec_hash = compute_raw_hash(orjson.dumps(rec, option=orjson.OPT_SORT_KEYS))
                if rec_hash in seen_hashes:
                    self._log.warning("validation_dropped", reason="duplicate")
                    continue
                seen_hashes.add(rec_hash)

            valid.append(rec)

        return valid

    @staticmethod
    def _check_required(rec: dict[str, Any], required: list[str]) -> bool:
        for field_name in required:
            if field_name not in rec or rec[field_name] is None:
                return False
        return True

    @staticmethod
    def _check_non_nullable(rec: dict[str, Any], non_nullable: list[str]) -> bool:
        for field_name in non_nullable:
            if field_name in rec and rec[field_name] is None:
                return False
        return True

    def _validate_against_metadata(self, rec: dict[str, Any]) -> bool:
        """Validate record against CSDL metadata properties."""
        if not self._metadata_properties:
            return True

        for key, value in rec.items():
            if key in ("odata_entity_set", "odata_service_url"):
                continue
            if key not in self._metadata_properties:
                self._log.warning("unknown_property", property=key)
                # Don't drop — services may extend schemas
                continue

            edm_type = self._metadata_properties[key]
            if value is None:
                continue

            # Type validation for numeric Edm types
            if edm_type in ("Edm.Int32", "Edm.Int64", "Edm.Int16"):
                if not isinstance(value, (int, float)):
                    try:
                        int(value)
                    except (ValueError, TypeError):
                        self._log.warning("type_validation_failed", property=key, edm_type=edm_type)
                        return False
            elif edm_type == "Edm.Decimal":
                if not isinstance(value, (int, float, Decimal)):
                    try:
                        Decimal(str(value))
                    except (InvalidOperation, ValueError, TypeError):
                        self._log.warning("type_validation_failed", property=key, edm_type=edm_type)
                        return False
            elif edm_type == "Edm.Double":
                if not isinstance(value, (int, float)):
                    try:
                        float(value)
                    except (ValueError, TypeError):
                        self._log.warning("type_validation_failed", property=key, edm_type=edm_type)
                        return False

        return True
