"""NIST NVD CVE adapter (R9.2).

Emits one :class:`NormalizedRecord` per CVE with payload:

    {
        "cve_id": str,
        "published": datetime | str,
        "last_modified": datetime | str,
        "cvss_v3_score": float | None,
        "cvss_v3_vector": str | None,
        "cwe_ids": list[str],
        "references": list[str],
        "affected_cpes": list[str],
        "description": str,
    }

``raw_hash = xxhash64(f"nvd:{cve_id}:{last_modified}")`` (R9.2).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, ClassVar

from .base import Tier29RestAdapter

_DEFAULT_STREAM_CONFIG: dict[str, Any] = {
    "base_url": "https://services.nvd.nist.gov/rest/json/",
    "endpoint_path": "cves/2.0",
    "auth_pattern": "api_key",
    "auth_key_name": "apiKey",
    "auth_key_location": "header",
    "response_root_path": "vulnerabilities",
    "pagination_type": "offset",
    "pagination_param": "startIndex",
    "pagination_limit": 2000,
    "supports_conditional": True,
    "format": "json",
}


class NVDCVEAdapter(Tier29RestAdapter):
    """Adapter for the ``nvd-cve`` Tier 29 stream."""

    source_label: ClassVar[str] = "nvd"

    def __init__(
        self,
        stream_id: str = "nvd-cve",
        settings: Any = None,
        registry: Any = None,
        *,
        stream_config: dict[str, Any] | None = None,
    ) -> None:
        merged: dict[str, Any] = dict(_DEFAULT_STREAM_CONFIG)
        if stream_config:
            merged.update(stream_config)
        super().__init__(stream_id, settings, registry, stream_config=merged)

    # -- hooks -------------------------------------------------------------

    def _compute_raw_hash(self, record: dict[str, Any]) -> str:
        cve_id = str(record.get("cve_id", ""))
        last_modified = _stringify(record.get("last_modified"))
        return self._xxhash64(f"nvd:{cve_id}:{last_modified}")

    def _build_payload(self, record: dict[str, Any]) -> dict[str, Any]:
        return {
            "cve_id": record.get("cve_id"),
            "published": record.get("published"),
            "last_modified": record.get("last_modified"),
            "cvss_v3_score": _maybe_float(record.get("cvss_v3_score")),
            "cvss_v3_vector": record.get("cvss_v3_vector"),
            "cwe_ids": list(record.get("cwe_ids") or []),
            "references": list(record.get("references") or []),
            "affected_cpes": list(record.get("affected_cpes") or []),
            "description": record.get("description") or "",
        }

    def _extract_timestamp(self, record: dict[str, Any]) -> datetime | None:
        # Prefer last_modified (the change we're recording); fall back to
        # published if absent.
        ts = self._coerce_datetime(record.get("last_modified"))
        if ts is None:
            ts = self._coerce_datetime(record.get("published"))
        return ts


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _maybe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


__all__ = ["NVDCVEAdapter"]
