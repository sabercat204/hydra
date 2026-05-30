"""Indicator extraction from normalized records (Design §2.2, R3.1).

``IndicatorExtractor.extract(record)`` walks a
:class:`hydra.models.normalized.NormalizedRecord` payload using the
JSONPath-style expressions in
:class:`hydra.eas.settings.IndicatorExtractionMap` and yields candidate
``(asset_type, value)`` pairs for the :class:`AssetMatcher` to evaluate.

Design constraints:

* **Limited JSONPath dialect.** Only root + dot-separated keys + a single
  ``[*]`` wildcard iterator are supported. A real JSONPath library would
  be overkill for the maps in ``EASSettings.indicator_extraction_map`` —
  all of them are shallow lookups like ``$.ip`` or ``$.indicators[*]``.
* **De-duplication.** The same ``(asset_type, normalized_value)`` may be
  reachable via multiple paths or via the IP / CIDR expansion rule. A
  single record must not produce duplicate indicators; downstream the
  monitor's SETNX dedup handles cross-record dedup but per-record dedup
  is cheaper and cleaner to do here.
* **Multi-type expansion.** A single extracted value may produce multiple
  indicators:

  - An IPv4/IPv6 string yields an ``IP`` indicator **and** a ``CIDR``
    indicator (the latter so that ``AssetMatcher._match_cidr`` can test
    containment against registered CIDR assets).
  - A URL yields a ``DOMAIN`` indicator extracted from the hostname
    **plus** an ``IP`` (and ``CIDR``) indicator when the hostname itself
    parses as an IP literal.
  - A plain domain/hostname yields both ``DOMAIN`` and ``HOSTNAME``
    indicators because the two asset types are distinct in the system.
  - An ``ASxxxx`` string yields an ``ASN`` indicator.

* **Pre-normalization.** Each emitted ``Indicator`` already contains the
  normalized value (``normalize_asset_value`` applied). The matcher and
  repository can therefore compare directly without a second
  normalization pass.

The extractor is deliberately permissive about shape — fields that are
missing, non-string, or malformed are quietly ignored. The ingestion
path must never fail because a payload happened not to contain an
``$.ip`` key.
"""

from __future__ import annotations

import ipaddress
import logging
import re
from dataclasses import dataclass
from typing import Any, Iterator
from urllib.parse import urlparse

from hydra.eas.assets.normalizer import normalize_asset_value
from hydra.eas.schemas.assets import AssetType
from hydra.eas.settings import EASSettings, IndicatorExtractionMap
from hydra.models.normalized import NormalizedRecord

logger = logging.getLogger(__name__)

__all__ = ["Indicator", "IndicatorExtractor"]


# Shape ``ASxxxx`` — a case-insensitive leading "AS" followed by an
# unsigned decimal integer. Used to classify extracted strings as ASN
# indicators. The range check (32-bit) is deferred to the normalizer's
# ``int()`` call, which only rejects non-numerics.
_ASN_RE = re.compile(r"^AS(\d+)$", re.IGNORECASE)


# Conservative domain/hostname shape. The spec-level validation lives in
# ``AssetCreate._validate_by_type``; this regex is the loose check used at
# extraction time to decide whether an extracted string looks "domain-ish"
# enough to emit. False positives here are safe because the matcher's
# case-folding equality and suffix branch will naturally reject garbage.
_DOMAINISH_RE = re.compile(
    r"^(?=.{1,253}$)(?!-)([A-Za-z0-9-]{1,63}(?<!-)\.)+[A-Za-z]{2,63}\.?$"
)


@dataclass(frozen=True, slots=True)
class Indicator:
    """A pre-normalized indicator emitted by :class:`IndicatorExtractor`.

    ``value`` is already in the canonical form produced by
    ``normalize_asset_value``, so consumers don't need to normalize a
    second time before comparing against ``Asset.normalized_value``.
    """

    asset_type: AssetType
    value: str


class IndicatorExtractor:
    """Walk ``record.payload`` via the configured JSONPath-ish expressions.

    Instances hold a reference to :class:`EASSettings` for ``extraction_map``
    access. The map is looked up fresh on every call so that settings
    reloads (task 17.1) take effect without a matcher rebuild.
    """

    def __init__(self, settings: EASSettings) -> None:
        self._settings = settings

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract(self, record: NormalizedRecord) -> list[Indicator]:
        """Extract a de-duplicated list of indicators for ``record``."""

        paths = self._paths_for_tier(int(record.tier))
        if not paths:
            return []

        # Ordered ``dict.fromkeys(...)`` is the idiomatic preserves-order
        # de-dup for Python 3.7+. We key on (asset_type, normalized_value)
        # so that e.g. the same hostname seen via two different paths
        # collapses to one indicator.
        seen: dict[tuple[AssetType, str], Indicator] = {}

        payload = record.payload or {}
        for path in paths:
            for raw_value in _resolve_path(payload, path):
                if not isinstance(raw_value, str):
                    # Values that are not strings (dicts, ints, lists)
                    # don't classify as indicators and are silently
                    # ignored — the payload schema is heterogeneous
                    # across tiers and we cannot assume anything.
                    continue
                for indicator in _classify_and_expand(raw_value):
                    seen.setdefault((indicator.asset_type, indicator.value), indicator)

        return list(seen.values())

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _paths_for_tier(self, tier: int) -> list[str]:
        extraction_map = self._settings.indicator_extraction_map
        attr = f"tier_{tier}"
        return getattr(extraction_map, attr, []) or []


# ----------------------------------------------------------------------
# Module-level helpers (pure, no shared state)
# ----------------------------------------------------------------------


def _resolve_path(payload: dict[str, Any], path: str) -> Iterator[Any]:
    """Yield every value reachable via the limited JSONPath ``path``.

    The accepted grammar is::

        path      := '$' segment*
        segment   := '.' key | '.' key '[*]'

    Examples::

        $.ip
        $.indicators[*]
        $.author_profile

    Anything outside this grammar is ignored (no yield).
    """

    if not path.startswith("$"):
        logger.debug("indicator_extraction_bad_path", extra={"path": path})
        return

    # Strip the leading "$" and then the leading "." if present so the
    # split below produces clean keys.
    remainder = path[1:]
    if remainder.startswith("."):
        remainder = remainder[1:]

    # Empty path means "yield the payload itself", which isn't useful
    # for indicator extraction — bail early.
    if not remainder:
        return

    segments = remainder.split(".")

    # Walk segments with a work list. Each entry is a (current_value,
    # remaining_segments) pair. A ``[*]`` suffix on a segment expands
    # the current list value into multiple work items.
    work: list[tuple[Any, list[str]]] = [(payload, segments)]

    while work:
        current, segs = work.pop()
        if not segs:
            yield current
            continue

        head, *tail = segs
        wildcard = head.endswith("[*]")
        key = head[:-3] if wildcard else head

        if not isinstance(current, dict):
            continue
        next_value = current.get(key)
        if next_value is None:
            continue

        if wildcard:
            if not isinstance(next_value, list):
                continue
            for item in next_value:
                work.append((item, tail))
        else:
            work.append((next_value, tail))


def _classify_and_expand(raw: str) -> Iterator[Indicator]:
    """Classify ``raw`` and yield the set of indicators it expands into.

    Rules mirror the docstring at the top of the module.
    """

    value = raw.strip()
    if not value:
        return

    # --- URL path -------------------------------------------------------
    if value.startswith(("http://", "https://")):
        try:
            parsed = urlparse(value)
        except ValueError:
            return
        host = parsed.hostname
        if not host:
            return
        # Recurse into the extracted hostname so that the classification
        # below picks up IP literals, domains, and hostnames without
        # duplicating logic.
        yield from _classify_and_expand(host)
        return

    # --- IP path --------------------------------------------------------
    if _looks_like_ip(value):
        try:
            ip_value = normalize_asset_value(AssetType.IP, value)
        except ValueError:
            return
        yield Indicator(AssetType.IP, ip_value)
        # CIDR expansion: the same normalized IP can be treated as a
        # /32 or /128 CIDR so that ``AssetMatcher._match_cidr`` can test
        # containment against registered CIDR assets. ``ip_network`` is
        # happy to parse a bare IP with ``strict=False``.
        try:
            cidr_value = normalize_asset_value(AssetType.CIDR, value)
        except ValueError:
            pass
        else:
            yield Indicator(AssetType.CIDR, cidr_value)
        return

    # --- ASN path -------------------------------------------------------
    asn_match = _ASN_RE.match(value)
    if asn_match:
        try:
            asn_value = normalize_asset_value(AssetType.ASN, value)
        except ValueError:
            return
        yield Indicator(AssetType.ASN, asn_value)
        return

    # --- Domain / hostname path ----------------------------------------
    if _DOMAINISH_RE.match(value):
        try:
            dom_value = normalize_asset_value(AssetType.DOMAIN, value)
        except ValueError:
            return
        yield Indicator(AssetType.DOMAIN, dom_value)
        # DOMAIN and HOSTNAME are separate asset types; an extracted
        # name could match either, so emit both. Cross-type false hits
        # are harmless — the matcher's per-type semantics differ enough
        # that a wrong asset_type will fail to match cleanly.
        try:
            host_value = normalize_asset_value(AssetType.HOSTNAME, value)
        except ValueError:
            return
        yield Indicator(AssetType.HOSTNAME, host_value)


def _looks_like_ip(value: str) -> bool:
    """Cheap pre-check: does ``value`` parse as an IP address?"""

    try:
        ipaddress.ip_address(value)
    except (ValueError, TypeError):
        return False
    return True
