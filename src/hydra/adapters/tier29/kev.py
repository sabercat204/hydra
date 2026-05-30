"""CISA Known Exploited Vulnerabilities (KEV) adapter (R9.4).

Emits one :class:`NormalizedRecord` per KEV entry with payload:

    {
        "cve_id": str,
        "vendor": str,
        "product": str,
        "date_added": date | str,
        "due_date": date | str | None,
        "required_action": str,
        "known_ransomware_use": bool,
    }

``raw_hash = xxhash64(f"kev:{cve_id}")`` — KEV entries identify by CVE ID.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, ClassVar

from .base import Tier29RestAdapter

_DEFAULT_STREAM_CONFIG: dict[str, Any] = {
    "base_url": "https://www.cisa.gov/",
    "endpoint_path": "sites/default/files/feeds/known_exploited_vulnerabilities.json",
    "auth_pattern": "none",
    "response_root_path": "vulnerabilities",
    "format": "json",
}


class CISAKEVAdapter(Tier29RestAdapter):
    """Adapter for the ``cisa-kev`` Tier 29 stream."""

    source_label: ClassVar[str] = "kev"

    def __init__(
        self,
        stream_id: str = "cisa-kev",
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
        return self._xxhash64(f"kev:{cve_id}")

    def _build_payload(self, record: dict[str, Any]) -> dict[str, Any]:
        return {
            "cve_id": record.get("cve_id"),
            "vendor": record.get("vendor") or "",
            "product": record.get("product") or "",
            "date_added": record.get("date_added"),
            "due_date": record.get("due_date"),
            "required_action": record.get("required_action") or "",
            "known_ransomware_use": _coerce_bool(record.get("known_ransomware_use")),
        }

    def _extract_timestamp(self, record: dict[str, Any]) -> datetime | None:
        return self._coerce_datetime(record.get("date_added"))


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "yes", "1", "known"}
    if isinstance(value, (int, float)):
        return bool(value)
    return False


# Unused but kept in case subclasses / tests need a stringified date helper.
def _stringify_date(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


__all__ = ["CISAKEVAdapter"]
