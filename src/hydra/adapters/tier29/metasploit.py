"""Metasploit modules adapter (R9.6).

Emits one :class:`NormalizedRecord` per Metasploit module with payload:

    {
        "module_path": str,
        "module_type": str | None,
        "rank": str | int | None,
        "disclosure_date": date | str | None,
        "cve_ids": list[str],
        "description": str,
        "platforms": list[str],
    }

``raw_hash = xxhash64(f"metasploit:{module_path}")`` — ``module_path`` is
the canonical unique identifier across the Metasploit module tree (e.g.
``exploit/linux/http/apache_mod_cgi_bash_env_exec``).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, ClassVar

from .base import Tier29RestAdapter

_DEFAULT_STREAM_CONFIG: dict[str, Any] = {
    "base_url": "https://raw.githubusercontent.com/",
    "endpoint_path": "rapid7/metasploit-framework/master/db/modules_metadata_base.json",
    "auth_pattern": "none",
    "format": "json",
}


class MetasploitAdapter(Tier29RestAdapter):
    """Adapter for the ``metasploit-modules`` Tier 29 stream."""

    source_label: ClassVar[str] = "metasploit"

    def __init__(
        self,
        stream_id: str = "metasploit-modules",
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
        module_path = str(record.get("module_path", ""))
        return self._xxhash64(f"metasploit:{module_path}")

    def _build_payload(self, record: dict[str, Any]) -> dict[str, Any]:
        return {
            "module_path": record.get("module_path"),
            "module_type": record.get("module_type"),
            "rank": record.get("rank"),
            "disclosure_date": record.get("disclosure_date"),
            "cve_ids": list(record.get("cve_ids") or []),
            "description": record.get("description") or "",
            "platforms": list(record.get("platforms") or []),
        }

    def _extract_timestamp(self, record: dict[str, Any]) -> datetime | None:
        return self._coerce_datetime(record.get("disclosure_date"))


__all__ = ["MetasploitAdapter"]
