"""FIRST EPSS adapter (R9.3).

Emits one :class:`NormalizedRecord` per CVE per day with payload:

    {
        "cve_id": str,
        "epss_score": float | None,
        "epss_percentile": float | None,
        "score_date": date | str,
    }

``raw_hash = xxhash64(f"epss:{cve_id}:{score_date}")`` so that the same CVE
scored on a different day yields a distinct record.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, ClassVar

from .base import Tier29RestAdapter

_DEFAULT_STREAM_CONFIG: dict[str, Any] = {
    "base_url": "https://api.first.org/data/v1/",
    "endpoint_path": "epss",
    "auth_pattern": "none",
    "response_root_path": "data",
    "pagination_type": "offset",
    "pagination_param": "offset",
    "pagination_limit": 100,
    "format": "json",
}


class FirstEPSSAdapter(Tier29RestAdapter):
    """Adapter for the ``first-epss`` Tier 29 stream."""

    source_label: ClassVar[str] = "epss"

    def __init__(
        self,
        stream_id: str = "first-epss",
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
        score_date = _stringify_date(record.get("score_date"))
        return self._xxhash64(f"epss:{cve_id}:{score_date}")

    def _build_payload(self, record: dict[str, Any]) -> dict[str, Any]:
        return {
            "cve_id": record.get("cve_id"),
            "epss_score": _maybe_float(record.get("epss_score")),
            "epss_percentile": _maybe_float(record.get("epss_percentile")),
            "score_date": record.get("score_date"),
        }

    def _extract_timestamp(self, record: dict[str, Any]) -> datetime | None:
        return self._coerce_datetime(record.get("score_date"))


def _stringify_date(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def _maybe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


__all__ = ["FirstEPSSAdapter"]
