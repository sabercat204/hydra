"""STIX/TAXII adapter — cyber threat intelligence ingestion.

Handles STIX 2.1 objects served via TAXII 2.1 servers or direct STIX bundle downloads.
"""

from __future__ import annotations

import csv
import io
import json
import re
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp
import structlog

from hydra.adapters.base import BaseAdapter, RawPayload
from hydra.adapters.exceptions import FetchError, ParseError
from hydra.config import HydraSettings
from hydra.models.normalized import GeoGeometry, NormalizedRecord, SourceMeta, Tier
from hydra.registry.stream_registry import StreamRegistry
from hydra.utils.hashing import compute_raw_hash

logger = structlog.get_logger()

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
)

# STIX SDO types we parse with specific field extraction
_SDO_TYPES = {
    "attack-pattern", "campaign", "indicator", "malware", "threat-actor",
    "vulnerability", "tool", "intrusion-set", "identity",
}
_SRO_TYPES = {"relationship", "sighting"}
_SCO_TYPES = {"ipv4-addr", "ipv6-addr", "domain-name", "url", "file", "email-addr"}


def _is_valid_stix_id(stix_id: str, expected_type: str | None = None) -> bool:
    """Validate STIX ID format: {type}--{uuid}."""
    if "--" not in stix_id:
        return False
    parts = stix_id.split("--", 1)
    if len(parts) != 2:
        return False
    type_prefix, uuid_part = parts
    if expected_type and type_prefix != expected_type:
        return False
    return bool(_UUID_RE.match(uuid_part))


def _flatten_kill_chain_phases(phases: list[dict[str, str]] | None) -> list[str]:
    """Flatten kill_chain_phases to '{chain}:{phase}' strings."""
    if not phases:
        return []
    return [f"{p.get('kill_chain_name', '')}:{p.get('phase_name', '')}" for p in phases]


def _extract_external_ids(refs: list[dict[str, Any]] | None) -> dict[str, str]:
    """Extract mitre_attack_id and cve_id from external_references."""
    result: dict[str, str] = {}
    if not refs:
        return result
    for ref in refs:
        source = ref.get("source_name", "")
        ext_id = ref.get("external_id", "")
        if source == "mitre-attack" and ext_id:
            result["mitre_attack_id"] = ext_id
        elif source == "cve" and ext_id:
            result["cve_id"] = ext_id
    return result


class StixTaxiiAdapter(BaseAdapter):
    """Adapter for STIX 2.1 / TAXII 2.1 cyber threat intelligence."""

    adapter_type: str = "stix_taxii"

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
        self._discovery_cache: dict[str, Any] | None = None
        self._last_fetch_time: str | None = None

    def _get(self, key: str, default: Any = None) -> Any:
        return self._cfg.get(key, default)

    def _bundle_state_path(self) -> Path:
        return Path(self.settings.data_dir) / "stix_state" / f"{self.stream_id}_bundle_state.json"

    def _load_bundle_state(self) -> dict[str, str]:
        p = self._bundle_state_path()
        if p.exists():
            return json.loads(p.read_text())
        return {}

    def _save_bundle_state(self, state: dict[str, str]) -> None:
        p = self._bundle_state_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(state))

    def _build_auth_headers(self) -> dict[str, str]:
        auth = self._get("auth_pattern", "none")
        headers: dict[str, str] = {}
        creds = self.settings.credentials.get(self.stream_id, {})
        if auth == "api_key":
            key_name = self._get("auth_key_name", "X-OTX-API-KEY")
            key_val = creds.get("api_key", "")
            headers[key_name] = key_val
        elif auth == "basic_auth":
            import base64
            user = creds.get("username", "")
            pwd = creds.get("password", "")
            token = base64.b64encode(f"{user}:{pwd}".encode()).decode()
            headers["Authorization"] = f"Basic {token}"
        return headers

    def _build_ssl_context(self) -> Any:
        auth = self._get("auth_pattern", "none")
        if auth == "certificate":
            import ssl
            creds = self.settings.credentials.get(self.stream_id, {})
            ctx = ssl.create_default_context()
            cert_path = creds.get("cert_path", "")
            key_path = creds.get("key_path", "")
            if cert_path and key_path:
                ctx.load_cert_chain(cert_path, key_path)
            return ctx
        return None

    # -- fetch -------------------------------------------------------------

    async def fetch(self) -> RawPayload:
        mode = self._get("stix_mode", "stix_bundle")
        if mode == "taxii":
            return await self._fetch_taxii()
        return await self._fetch_bundle()

    async def _fetch_taxii(self) -> RawPayload:
        taxii_url = self._get("taxii_url", "")
        api_root_path = self._get("api_root_path")
        collection_id = self._get("collection_id")
        max_pages = self._get("max_pages", 100)
        headers = self._build_auth_headers()
        headers["Accept"] = "application/taxii+json;version=2.1"
        headers["Content-Type"] = "application/taxii+json;version=2.1"
        timeout = aiohttp.ClientTimeout(total=self.settings.http_timeout_seconds)
        ssl_ctx = self._build_ssl_context()

        async with aiohttp.ClientSession(timeout=timeout) as session:
            # Discovery
            if not api_root_path:
                api_root_path = await self._discover_api_root(session, taxii_url, headers, ssl_ctx)

            api_root_url = f"{taxii_url}{api_root_path}"

            # Collection resolution
            if not collection_id:
                collection_id = await self._resolve_collection(session, api_root_url, headers, ssl_ctx)

            # Fetch objects with pagination
            objects_url = f"{api_root_url}/collections/{collection_id}/objects/"
            all_objects: list[dict[str, Any]] = []
            params: dict[str, str] = {}

            # Incremental fetch
            if self._last_fetch_time:
                params["added_after"] = self._last_fetch_time

            # Type filter
            type_filter = self._get("stix_type_filter")
            if type_filter:
                params["match[type]"] = ",".join(type_filter)

            # ID filter
            id_filter = self._get("stix_id_filter")
            if id_filter:
                params["match[id]"] = ",".join(id_filter)

            params["match[spec_version]"] = "2.1"

            pages_fetched = 0
            while pages_fetched < max_pages:
                async with session.get(objects_url, headers=headers, params=params, ssl=ssl_ctx) as resp:
                    data = await resp.json()
                    objs = data.get("objects", [])
                    all_objects.extend(objs)
                    pages_fetched += 1

                    if data.get("more") and data.get("next"):
                        params["next"] = data["next"]
                    else:
                        break

        self._last_fetch_time = datetime.now(timezone.utc).isoformat()
        content = json.dumps({"objects": all_objects}).encode()
        return RawPayload(
            stream_id=self.stream_id,
            fetched_at=datetime.now(timezone.utc),
            content=content,
            content_type="application/taxii+json;version=2.1",
            http_status=200,
            headers={"taxii_collection_id": collection_id or ""},
        )

    async def _discover_api_root(
        self, session: aiohttp.ClientSession, taxii_url: str,
        headers: dict[str, str], ssl_ctx: Any,
    ) -> str:
        discovery_url = f"{taxii_url}/taxii2/"
        async with session.get(discovery_url, headers=headers, ssl=ssl_ctx) as resp:
            data = await resp.json()
        api_roots = data.get("api_roots", [])
        if not api_roots:
            raise FetchError("No API roots found in TAXII discovery")
        target_title = self._get("api_root_title")
        if target_title:
            for root in api_roots:
                if isinstance(root, dict) and root.get("title") == target_title:
                    return root.get("path", root.get("url", api_roots[0]))
                elif isinstance(root, str) and target_title in root:
                    return root
        # Return first root
        if isinstance(api_roots[0], dict):
            return api_roots[0].get("path", api_roots[0].get("url", ""))
        return api_roots[0]

    async def _resolve_collection(
        self, session: aiohttp.ClientSession, api_root_url: str,
        headers: dict[str, str], ssl_ctx: Any,
    ) -> str:
        collections_url = f"{api_root_url}/collections/"
        async with session.get(collections_url, headers=headers, ssl=ssl_ctx) as resp:
            data = await resp.json()
        collections = data.get("collections", [])
        if not collections:
            raise FetchError("No collections found")
        target_title = self._get("collection_title")
        if target_title:
            for col in collections:
                if col.get("title") == target_title:
                    return col["id"]
        return collections[0]["id"]

    async def _fetch_bundle(self) -> RawPayload:
        bundle_url = self._get("bundle_url", "")
        bundle_format = self._get("bundle_format", "stix_json")
        headers = self._build_auth_headers()
        timeout = aiohttp.ClientTimeout(total=self.settings.http_timeout_seconds)
        ssl_ctx = self._build_ssl_context()

        # Conditional fetch
        supports_conditional = True
        state = self._load_bundle_state()
        if state.get("etag"):
            headers["If-None-Match"] = state["etag"]
        if state.get("last_modified"):
            headers["If-Modified-Since"] = state["last_modified"]

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(bundle_url, headers=headers, ssl=ssl_ctx) as resp:
                if resp.status == 304:
                    return RawPayload(
                        stream_id=self.stream_id,
                        fetched_at=datetime.now(timezone.utc),
                        content=b"",
                        content_type="",
                        http_status=304,
                        headers=dict(resp.headers),
                    )

                content = await resp.read()
                resp_headers = dict(resp.headers)

                new_state: dict[str, str] = {}
                if "ETag" in resp_headers:
                    new_state["etag"] = resp_headers["ETag"]
                if "Last-Modified" in resp_headers:
                    new_state["last_modified"] = resp_headers["Last-Modified"]
                if new_state:
                    self._save_bundle_state(new_state)

        if bundle_format == "stix_csv":
            content = self._convert_csv_to_stix_bundle(content)

        return RawPayload(
            stream_id=self.stream_id,
            fetched_at=datetime.now(timezone.utc),
            content=content,
            content_type="application/json",
            http_status=200,
            headers=resp_headers,
        )

    def _convert_csv_to_stix_bundle(self, csv_content: bytes) -> bytes:
        """Convert CSV rows to a STIX 2.1 bundle using csv_to_stix_mapping."""
        mapping: dict[str, str] = self._get("csv_to_stix_mapping", {})
        text = csv_content.decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        objects: list[dict[str, Any]] = []

        for row in reader:
            stix_obj: dict[str, Any] = {
                "type": "indicator",
                "spec_version": "2.1",
                "id": f"indicator--{uuid.uuid4()}",
                "created": datetime.now(timezone.utc).isoformat(),
                "modified": datetime.now(timezone.utc).isoformat(),
            }
            for csv_col, stix_field in mapping.items():
                if csv_col in row:
                    stix_obj[stix_field] = row[csv_col]
            objects.append(stix_obj)

        bundle = {
            "type": "bundle",
            "id": f"bundle--{uuid.uuid4()}",
            "objects": objects,
        }
        return json.dumps(bundle).encode()

    # -- parse -------------------------------------------------------------

    def parse(self, raw: RawPayload) -> list[dict[str, Any]]:
        if not raw.content:
            return []
        try:
            data = json.loads(raw.content)
        except Exception as exc:
            raise ParseError(f"JSON decode failed: {exc}") from exc

        objects = data.get("objects", [])
        mode = self._get("stix_mode", "stix_bundle")
        collection_id = raw.headers.get("taxii_collection_id", "")

        records: list[dict[str, Any]] = []
        for obj in objects:
            rec = self._parse_stix_object(obj)
            rec["stix_version"] = "2.1"
            rec["fetch_mode"] = mode
            if collection_id:
                rec["taxii_collection_id"] = collection_id
            records.append(rec)

        return records

    def _parse_stix_object(self, obj: dict[str, Any]) -> dict[str, Any]:
        """Parse a single STIX object into a canonical dict."""
        stix_type = obj.get("type", "")
        rec: dict[str, Any] = {
            "id": obj.get("id", ""),
            "type": stix_type,
            "created": obj.get("created"),
            "modified": obj.get("modified"),
        }

        ext_ids = _extract_external_ids(obj.get("external_references"))
        rec.update(ext_ids)

        kill_chain = _flatten_kill_chain_phases(obj.get("kill_chain_phases"))
        if kill_chain:
            rec["kill_chain_phases"] = kill_chain

        if stix_type == "attack-pattern":
            rec["name"] = obj.get("name")
            rec["description"] = obj.get("description")
            rec["external_references"] = obj.get("external_references")
        elif stix_type == "campaign":
            rec["name"] = obj.get("name")
            rec["description"] = obj.get("description")
            rec["aliases"] = obj.get("aliases")
            rec["first_seen"] = obj.get("first_seen")
            rec["last_seen"] = obj.get("last_seen")
        elif stix_type == "indicator":
            rec["name"] = obj.get("name")
            rec["description"] = obj.get("description")
            rec["pattern"] = obj.get("pattern")
            rec["pattern_type"] = obj.get("pattern_type")
            rec["valid_from"] = obj.get("valid_from")
            rec["valid_until"] = obj.get("valid_until")
        elif stix_type == "malware":
            rec["name"] = obj.get("name")
            rec["description"] = obj.get("description")
            rec["malware_types"] = obj.get("malware_types")
            rec["is_family"] = obj.get("is_family")
            rec["aliases"] = obj.get("aliases")
            rec["first_seen"] = obj.get("first_seen")
            rec["last_seen"] = obj.get("last_seen")
        elif stix_type == "threat-actor":
            rec["name"] = obj.get("name")
            rec["description"] = obj.get("description")
            rec["threat_actor_types"] = obj.get("threat_actor_types")
            rec["aliases"] = obj.get("aliases")
            rec["roles"] = obj.get("roles")
            rec["goals"] = obj.get("goals")
            rec["sophistication"] = obj.get("sophistication")
            rec["resource_level"] = obj.get("resource_level")
            rec["primary_motivation"] = obj.get("primary_motivation")
        elif stix_type == "vulnerability":
            rec["name"] = obj.get("name")
            rec["description"] = obj.get("description")
            rec["external_references"] = obj.get("external_references")
        elif stix_type == "tool":
            rec["name"] = obj.get("name")
            rec["description"] = obj.get("description")
            rec["tool_types"] = obj.get("tool_types")
            rec["aliases"] = obj.get("aliases")
        elif stix_type == "intrusion-set":
            rec["name"] = obj.get("name")
            rec["description"] = obj.get("description")
            rec["aliases"] = obj.get("aliases")
            rec["first_seen"] = obj.get("first_seen")
            rec["last_seen"] = obj.get("last_seen")
            rec["goals"] = obj.get("goals")
            rec["resource_level"] = obj.get("resource_level")
            rec["primary_motivation"] = obj.get("primary_motivation")
        elif stix_type == "identity":
            rec["name"] = obj.get("name")
            rec["identity_class"] = obj.get("identity_class")
            rec["sectors"] = obj.get("sectors")
            rec["contact_information"] = obj.get("contact_information")
        elif stix_type == "relationship":
            rec["relationship_type"] = obj.get("relationship_type")
            rec["source_ref"] = obj.get("source_ref")
            rec["target_ref"] = obj.get("target_ref")
            rec["description"] = obj.get("description")
        elif stix_type == "sighting":
            rec["sighting_of_ref"] = obj.get("sighting_of_ref")
            rec["observed_data_refs"] = obj.get("observed_data_refs")
            rec["first_seen"] = obj.get("first_seen")
            rec["last_seen"] = obj.get("last_seen")
            rec["count"] = obj.get("count")
        elif stix_type in _SCO_TYPES:
            if stix_type in ("ipv4-addr", "ipv6-addr", "domain-name", "url"):
                rec["value"] = obj.get("value")
            elif stix_type == "file":
                rec["name"] = obj.get("name")
                rec["hashes"] = obj.get("hashes")
                rec["size"] = obj.get("size")
            elif stix_type == "email-addr":
                rec["value"] = obj.get("value")
                rec["display_name"] = obj.get("display_name")
        else:
            # Generic fallback — store all remaining fields
            for k, v in obj.items():
                if k not in ("id", "type", "created", "modified"):
                    rec[k] = v

        # Revocation
        if obj.get("revoked"):
            rec["revoked"] = True

        return rec

    # -- validate ----------------------------------------------------------

    def validate(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        validate_patterns = self._get("validate_stix_patterns", False)
        valid: list[dict[str, Any]] = []
        seen_ids: dict[str, dict[str, Any]] = {}  # id -> record (keep latest modified)

        for rec in records:
            stix_id = rec.get("id", "")
            stix_type = rec.get("type", "")

            # STIX ID format validation
            if not _is_valid_stix_id(stix_id, stix_type):
                self._log.warning("invalid_stix_id", stix_id=stix_id, stix_type=stix_type)
                continue

            # Required fields per type
            if stix_type in _SDO_TYPES or stix_type in _SRO_TYPES:
                if not all(rec.get(f) for f in ("id", "type", "created", "modified")):
                    self._log.debug("missing_required_sdo_fields", stix_id=stix_id)
                    continue
            elif stix_type in _SCO_TYPES:
                if not rec.get("id") or not rec.get("type"):
                    continue
                has_identifying = rec.get("value") or rec.get("name") or rec.get("hashes")
                if not has_identifying:
                    self._log.debug("missing_sco_identifying_property", stix_id=stix_id)
                    continue

            # Timestamp validation
            created = rec.get("created")
            modified = rec.get("modified")
            if created and modified:
                try:
                    c = datetime.fromisoformat(str(created).replace("Z", "+00:00")) if isinstance(created, str) else created
                    m = datetime.fromisoformat(str(modified).replace("Z", "+00:00")) if isinstance(modified, str) else modified
                    if isinstance(c, datetime) and isinstance(m, datetime) and m < c:
                        self._log.warning("modified_before_created", stix_id=stix_id)
                        rec["modified"] = rec["created"]
                except Exception:
                    pass

            # STIX pattern validation (indicators only)
            if stix_type == "indicator" and validate_patterns:
                pattern = rec.get("pattern", "")
                if pattern:
                    try:
                        from stix2patterns.v21.pattern import create_pattern
                        create_pattern(pattern)
                        rec["pattern_valid"] = True
                    except Exception:
                        self._log.warning("invalid_stix_pattern", stix_id=stix_id, pattern=pattern[:50])
                        rec["pattern_valid"] = False

            # Dedup by STIX id — keep latest modified
            existing = seen_ids.get(stix_id)
            if existing:
                existing_mod = existing.get("modified", "")
                current_mod = rec.get("modified", "")
                if str(current_mod) > str(existing_mod):
                    seen_ids[stix_id] = rec
            else:
                seen_ids[stix_id] = rec

        valid = list(seen_ids.values())
        return valid

    # -- normalize ---------------------------------------------------------

    def normalize(self, records: list[dict[str, Any]]) -> list[NormalizedRecord]:
        tier_id = self._get("tier", self.tier_id or 16)
        source_name = self._get("source_name", self.stream_id)
        mode = self._get("stix_mode", "stix_bundle")
        default_confidence = self._get("default_confidence")
        default_tags: list[str] = self._get("default_tags", [])

        if default_confidence is not None:
            confidence = float(default_confidence)
        elif mode == "taxii":
            confidence = 0.95
        else:
            confidence = 0.85

        source_url = self._get("taxii_url") or self._get("bundle_url") or ""

        normalized: list[NormalizedRecord] = []
        for rec in records:
            ts_str = rec.get("modified") or rec.get("created")
            if ts_str:
                try:
                    ts = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
                except Exception:
                    ts = datetime.now(timezone.utc)
            else:
                ts = datetime.now(timezone.utc)
            if isinstance(ts, datetime) and ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)

            # raw_hash from id + modified
            stix_id = rec.get("id", "")
            modified = rec.get("modified", "")
            raw_hash = compute_raw_hash(f"{stix_id}{modified}".encode())

            tags = list(default_tags)
            tags.append("2.1")  # stix_version
            stix_type = rec.get("type", "")
            if stix_type:
                tags.append(stix_type)
            fetch_mode = rec.get("fetch_mode", mode)
            tags.append(fetch_mode)
            if rec.get("mitre_attack_id"):
                tags.append(rec["mitre_attack_id"])
            if rec.get("cve_id"):
                tags.append(rec["cve_id"])
            if rec.get("revoked"):
                tags.append("revoked")

            nr = NormalizedRecord(
                stream_id=self.stream_id,
                tier=Tier(tier_id),
                timestamp=ts,
                geo=None,
                payload=rec,
                source_meta=SourceMeta(
                    source_name=source_name,
                    source_url=source_url,
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
        from hydra.adapters.base import AdapterHealth, HealthStatus
        mode = self._get("stix_mode", "stix_bundle")
        start = time.monotonic()

        if mode == "taxii":
            taxii_url = self._get("taxii_url", "")
            url = f"{taxii_url}/taxii2/"
        else:
            url = self._get("bundle_url", "")

        try:
            timeout = aiohttp.ClientTimeout(total=10)
            headers = self._build_auth_headers()
            ssl_ctx = self._build_ssl_context()
            async with aiohttp.ClientSession(timeout=timeout) as session:
                method = session.get if mode == "taxii" else session.head
                async with method(url, headers=headers, ssl=ssl_ctx) as resp:
                    latency = round((time.monotonic() - start) * 1000, 2)
                    status = HealthStatus.OK if resp.status < 400 else HealthStatus.DEGRADED
                    return AdapterHealth(
                        stream_id=self.stream_id, status=status,
                        latency_ms=latency, last_checked=datetime.now(timezone.utc),
                    )
        except Exception as exc:
            return AdapterHealth(
                stream_id=self.stream_id, status=HealthStatus.UNREACHABLE,
                latency_ms=round((time.monotonic() - start) * 1000, 2),
                last_checked=datetime.now(timezone.utc), detail=str(exc),
            )
