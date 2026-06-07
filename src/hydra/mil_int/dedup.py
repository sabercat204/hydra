"""Cross-tier dedup resolver for known mirrors in the mil_int surface.

Several sources mirror each other (EverySpec ↔ DLA ASSIST for MIL-STDs;
Russia Matters and FAS curate translations of RU MoD docs; the FAS
nuclear archive mirrors DTIC declassified holdings). When a document
shows up under multiple tiers we pick a canonical source per spec
§"OPERATIONAL CONSTRAINTS" item 3.

The resolver is intentionally simple: a fingerprint built from the
filename + a coarse content-type hint, plus a curated authority order.
It's good enough for first-pass dedup; richer matching (PDF hash,
title-author NLP) is a future task.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Iterable
from urllib.parse import urlparse

from hydra.models.normalized import NormalizedRecord


# Authority preference — earlier wins over later. Names match the
# ``source_meta.source_name`` (or ``payload['source_name']``) emitted by
# the doc_repo / rest_json adapter.
_AUTHORITY_ORDER: tuple[str, ...] = (
    "DLA ASSIST",
    "DTIC R&E Gateway",
    "NIST SP 800 Series",
    "DISA STIG Library",
    "NSA Public Guidance",
    "EverySpec",
    "FAS",
    "FAS Nuclear Info Russia",
    "NATO STO Publications",
    "NATO CMRE Open Library",
    "NATO Archives Online",
    "Russia MoD English",
    "Russia Matters Harvard",
    "CAST Moscow Defense Brief",
)


@dataclass(frozen=True)
class DedupKey:
    filename: str
    content_type: str


@dataclass
class DedupResult:
    kept: list[NormalizedRecord] = field(default_factory=list)
    dropped: list[tuple[NormalizedRecord, NormalizedRecord]] = field(default_factory=list)
    """Each tuple is ``(dropped_record, canonical_record)``."""


def _fingerprint(record: NormalizedRecord) -> DedupKey:
    payload = record.payload or {}
    url = str(payload.get("doc_url") or "")
    parsed = urlparse(url)
    filename = os.path.basename(parsed.path).lower()
    if not filename:
        # Fall back to the last meaningful path segment.
        parts = [p for p in parsed.path.split("/") if p]
        filename = parts[-1].lower() if parts else url.lower()
    ctype = str(payload.get("content_type") or "").lower()
    return DedupKey(filename=filename, content_type=ctype)


def _authority_index(name: str) -> int:
    try:
        return _AUTHORITY_ORDER.index(name)
    except ValueError:
        return len(_AUTHORITY_ORDER)


def resolve_mirrors(records: Iterable[NormalizedRecord]) -> DedupResult:
    """Reduce mirror duplicates to one canonical record per fingerprint.

    The canonical record is the one whose ``source_name`` ranks highest in
    :data:`_AUTHORITY_ORDER`. Records whose filename can't be derived (no
    ``doc_url``) are passed through untouched — we don't dedup what we
    can't fingerprint.
    """
    canonical: dict[DedupKey, NormalizedRecord] = {}
    result = DedupResult()

    for rec in records:
        key = _fingerprint(rec)
        if not key.filename:
            result.kept.append(rec)
            continue
        existing = canonical.get(key)
        if existing is None:
            canonical[key] = rec
            continue
        if _authority_index(rec.source_meta.source_name) < _authority_index(
            existing.source_meta.source_name
        ):
            # New record wins; existing was the old canonical.
            canonical[key] = rec
            result.dropped.append((existing, rec))
        else:
            result.dropped.append((rec, existing))

    result.kept.extend(canonical.values())
    return result


__all__ = ["DedupKey", "DedupResult", "resolve_mirrors"]
