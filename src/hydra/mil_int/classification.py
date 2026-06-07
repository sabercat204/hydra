"""Classification boundary enforcement for the mil_int surface.

The surface is UNCLASSIFIED-only. This module provides a single gate
function that inspects a record's payload + tags for classification
markers and either rejects or admits the record. Rejections increment
``hydra_mil_int_access_policy_violations_total{kind="classification"}``.
"""

from __future__ import annotations

import logging
import re
from typing import Iterable

from hydra.models.normalized import NormalizedRecord

logger = logging.getLogger(__name__)


# Common classification markers — kept conservative so we err on the side
# of rejecting borderline content rather than admitting it.
_FORBIDDEN_MARKERS: tuple[str, ...] = (
    "CONFIDENTIAL",
    "SECRET",
    "TOP SECRET",
    "TOPSECRET",
    "TS//SCI",
    "NOFORN",
    "FOUO",
    "CUI//",
    "NATO RESTRICTED",
    "NATO CONFIDENTIAL",
    "NATO SECRET",
    "COSMIC TOP SECRET",
    "CTS//",
    "FVEY",
    "REL TO",
)

_CLASSIFICATION_PATTERN = re.compile(
    r"\b(" + "|".join(re.escape(m) for m in _FORBIDDEN_MARKERS) + r")\b",
    re.IGNORECASE,
)


def detect_classification(text: str) -> str | None:
    """Return the matched marker, or ``None`` when no marker is present."""
    if not text:
        return None
    m = _CLASSIFICATION_PATTERN.search(text)
    return m.group(1).upper() if m else None


def is_unclassified(record: NormalizedRecord) -> tuple[bool, str | None]:
    """Inspect a record. Returns ``(ok, marker_if_any)``.

    Checks the document title, abstract, doc_url, list_url, tags, and any
    explicit ``classification`` field on the payload. The check is text-only
    — it doesn't read PDF blob content (the doc_repo adapter persists those
    to MinIO; classification of full text is a downstream responsibility).
    """
    payload = record.payload or {}

    explicit = str(payload.get("classification", "")).strip().upper()
    if explicit and explicit != "UNCLASSIFIED":
        return False, explicit

    haystacks: list[str] = []
    for key in ("title", "abstract", "summary", "doc_url", "list_url"):
        val = payload.get(key)
        if isinstance(val, str):
            haystacks.append(val)
    haystacks.extend(t for t in record.tags if isinstance(t, str))

    for hay in haystacks:
        marker = detect_classification(hay)
        if marker:
            return False, marker
    return True, None


def filter_unclassified(
    records: Iterable[NormalizedRecord],
    *,
    enforce: bool = True,
    log_only: bool = False,
    on_violation: callable | None = None,  # type: ignore[type-arg]
) -> list[NormalizedRecord]:
    """Drop records that fail the unclassified check.

    When ``log_only`` is True, violations are reported via ``on_violation``
    (typically a metrics counter increment) but the records are admitted.
    When ``enforce`` is False, the gate is bypassed entirely — used by
    tests that need to exercise downstream code with synthetic payloads.
    """
    if not enforce:
        return list(records)

    out: list[NormalizedRecord] = []
    for record in records:
        ok, marker = is_unclassified(record)
        if ok:
            out.append(record)
            continue
        logger.warning(
            "mil_int.classification_violation",
            extra={
                "stream_id": record.stream_id,
                "tier": int(record.tier),
                "marker": marker,
            },
        )
        if on_violation is not None:
            try:
                on_violation(marker or "UNKNOWN")
            except Exception:  # noqa: BLE001 — metrics must never raise
                pass
        if log_only:
            out.append(record)
    return out


__all__ = [
    "detect_classification",
    "is_unclassified",
    "filter_unclassified",
]
