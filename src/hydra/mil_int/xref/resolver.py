"""Standards cross-reference resolver.

Reads a curated YAML seed (``config/mil_int_xref.yaml``) into an
in-memory bidirectional map. Future iterations can swap the seed for a
relational table fed by ingestion-time tagging.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from hydra.mil_int.metrics import hydra_mil_int_xref_resolutions_total
from hydra.mil_int.schemas.xref import XrefMapping
from hydra.mil_int.xref.families import detect_family, normalize_id


def load_xref_seed(path: Path | str) -> list[dict[str, Any]]:
    """Load the curated cross-reference YAML.

    Each entry is a dict with ``from_family``, ``from_id``, ``to_family``,
    ``to_id``, ``relationship`` (default ``related``), and ``notes``
    (optional). Missing files yield an empty list — the resolver is still
    functional, it just won't return any mappings.
    """
    p = Path(path)
    if not p.exists():
        return []
    with p.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    raw_mappings = data.get("mappings", [])
    if not isinstance(raw_mappings, list):
        return []
    return [m for m in raw_mappings if isinstance(m, dict)]


class XrefResolver:
    """In-memory cross-reference resolver.

    Resolution is symmetric — a seed mapping ``A -> B`` is queryable both
    ways, with the ``relationship`` annotated as the seed described it.
    """

    def __init__(self, mappings: list[dict[str, Any]] | None = None) -> None:
        self._mappings: list[XrefMapping] = []
        self._index: dict[str, list[XrefMapping]] = {}
        for raw in mappings or []:
            self._add(raw)

    @classmethod
    def from_path(cls, path: Path | str) -> "XrefResolver":
        return cls(load_xref_seed(path))

    def _add(self, raw: dict[str, Any]) -> None:
        try:
            forward = XrefMapping(**raw)
        except Exception:  # noqa: BLE001 — skip malformed seed rows
            return
        # Normalise identifiers to keep lookups consistent.
        forward = forward.model_copy(
            update={
                "from_id": normalize_id(forward.from_id),
                "to_id": normalize_id(forward.to_id),
            }
        )
        reverse = XrefMapping(
            from_family=forward.to_family,
            from_id=forward.to_id,
            to_family=forward.from_family,
            to_id=forward.from_id,
            relationship=forward.relationship,
            notes=forward.notes,
        )
        self._mappings.append(forward)
        self._index.setdefault(forward.from_id, []).append(forward)
        self._index.setdefault(reverse.from_id, []).append(reverse)

    def lookup(
        self,
        identifier: str,
        *,
        to_family: str | None = None,
        max_results: int = 50,
    ) -> list[XrefMapping]:
        key = normalize_id(identifier)
        results = list(self._index.get(key, []))
        if to_family:
            results = [m for m in results if m.to_family == to_family]
        for m in results:
            try:
                hydra_mil_int_xref_resolutions_total.labels(
                    from_family=m.from_family,
                    to_family=m.to_family,
                ).inc()
            except Exception:  # noqa: BLE001 — metrics never raise
                pass
        return results[:max_results]

    @property
    def size(self) -> int:
        """Total mapping rows (forward only — reverse is implicit)."""
        return len(self._mappings)

    def detect_family(self, identifier: str) -> str | None:
        return detect_family(identifier)


__all__ = ["XrefResolver", "load_xref_seed"]
