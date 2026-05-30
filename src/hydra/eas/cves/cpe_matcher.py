"""CPE 2.3 parsing and matching (Design §3.4, R10.2).

Two dataclasses and a matching function live here:

* :class:`CPEEntry` — a structured view over a CPE 2.3 URI that captures
  the fields the CVE_Pipeline needs for matching: ``vendor``,
  ``product``, an optional pinned ``version`` plus the NVD version-range
  bounds ``version_start_including`` and ``version_end_excluding``.
* :class:`FingerprintTriple` — the `(vendor, product, version)` triple
  extracted from a fingerprint record's payload by
  :class:`hydra.eas.cves.fingerprint.FingerprintExtractor`.

The exposed :func:`cpe_matches` function implements the Design §3.4
pseudocode exactly:

1. Case-insensitive equality on ``(vendor, product)``.
2. In ``"loose"`` mode, return ``True`` at that point — version data is
   intentionally ignored because OSINT fingerprints often lack reliable
   versions.
3. In ``"strict"`` mode, additionally require that ``fp.version`` parses
   as a :class:`packaging.version.Version` **and** falls inside the CPE's
   version range. The range is derived from three fields:

   * ``cpe.version`` — a pinned version when non-wildcard.
   * ``cpe.version_start_including`` — inclusive lower bound.
   * ``cpe.version_end_excluding`` — exclusive upper bound.

   A CPE with ``version == "*"`` (or ``None``) and no start/end bounds
   matches every version, which follows NVD semantics for "all versions
   affected".

``packaging`` is part of the core Python packaging stack and is always
available as a transitive install via ``pip``. Versions that cannot be
parsed (e.g. ``"linux"`` on an OS CPE) trigger a ``False`` return in
strict mode — we never raise from :func:`cpe_matches` so pipelines
don't crash on junk CPE entries.

:func:`parse_cpe` tolerates both the full CPE 2.3 formatted string
``cpe:2.3:<part>:<vendor>:<product>:<version>:<update>:<edition>:<lang>:<sw_edition>:<target_sw>:<target_hw>:<other>``
and the simpler ``<vendor>:<product>:<version>`` shorthand that some
upstream datasets use. Both paths converge on :class:`CPEEntry`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from packaging.version import InvalidVersion, Version

__all__ = [
    "CPEEntry",
    "FingerprintTriple",
    "cpe_matches",
    "parse_cpe",
]


# Sentinel tokens used by CPE 2.3 for "any value" in a component.
_CPE_WILDCARDS: frozenset[str] = frozenset({"*", "-", ""})


@dataclass(slots=True, frozen=True)
class CPEEntry:
    """Structured view of a CPE 2.3 entry for CVE matching (Design §4.9)."""

    vendor: str
    product: str
    version: str | None = None
    version_start_including: str | None = None
    version_end_excluding: str | None = None


@dataclass(slots=True, frozen=True)
class FingerprintTriple:
    """Vendor/product/version triple extracted from a fingerprint record."""

    vendor: str
    product: str
    version: str | None = None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def cpe_matches(
    cpe: CPEEntry,
    fp: FingerprintTriple,
    mode: Literal["loose", "strict"] = "loose",
) -> bool:
    """Return ``True`` iff ``fp`` matches ``cpe`` under ``mode`` (R10.2).

    Semantics follow Design §3.4 pseudocode:

    * Both ``vendor`` and ``product`` must compare equal case-insensitively
      (stripped of surrounding whitespace).
    * In ``"loose"`` mode, version data is ignored — this is the default
      MVP behaviour because many OSINT fingerprint sources omit reliable
      versions.
    * In ``"strict"`` mode, the fingerprint version must be parseable by
      :class:`packaging.version.Version` and fall inside the CPE's
      version range. The range is computed from the CPE's pinned version
      plus the NVD range fields; a wildcard CPE (``version`` of ``"*"`` /
      ``"-"`` / ``""`` / ``None`` and no range bounds) matches every
      fingerprint version.

    Any parsing failure (unparseable ``fp.version``, unparseable CPE
    range bound) yields ``False`` rather than raising — the CVE pipeline
    iterates many CPEs per fingerprint and one broken entry should not
    crash the run.
    """

    if mode not in ("loose", "strict"):
        raise ValueError(f"unknown match mode: {mode!r}")

    if not _equal_ci(cpe.vendor, fp.vendor):
        return False
    if not _equal_ci(cpe.product, fp.product):
        return False

    if mode == "loose":
        return True

    # Strict mode — fingerprint version is mandatory.
    if fp.version is None:
        return False
    try:
        fp_version = Version(fp.version)
    except InvalidVersion:
        return False

    return _version_in_range(fp_version, cpe)


def parse_cpe(cpe_uri: str) -> CPEEntry:
    """Parse ``cpe_uri`` into a :class:`CPEEntry`.

    Two input shapes are accepted:

    * Full CPE 2.3 formatted string prefixed with ``cpe:2.3:``. Components
      are colon-separated; missing trailing components are tolerated so
      that abbreviated entries parse correctly.
    * A ``vendor:product:version`` shorthand used by some upstream
      datasets and fingerprint extractors.

    The returned entry always carries at least ``vendor`` and ``product``;
    ``version`` is ``None`` when the CPE component is a wildcard
    (``"*"`` / ``"-"`` / empty) so that loose-mode callers don't need to
    special-case wildcard tokens.
    """

    if not isinstance(cpe_uri, str):
        raise TypeError("cpe_uri must be a string")

    value = cpe_uri.strip()
    if not value:
        raise ValueError("cpe_uri must not be empty")

    if value.lower().startswith("cpe:2.3:"):
        # Formatted string — drop the ``cpe:2.3:`` prefix and split on ``:``.
        # NVD escapes literal colons as ``\:``; we split carefully to avoid
        # splitting on those escaped colons.
        body = value[len("cpe:2.3:"):]
        components = _split_cpe_components(body)
        # Expected ordering: part, vendor, product, version, update,
        # edition, lang, sw_edition, target_sw, target_hw, other.
        # A well-formed CPE 2.3 string has 11 components, but we pad to
        # cover abbreviated entries.
        while len(components) < 11:
            components.append("*")
        _part, vendor, product, version, *_rest = components
    else:
        # Shorthand ``vendor:product:version``.
        parts = value.split(":")
        if len(parts) < 2:
            raise ValueError(
                "cpe shorthand must have at least vendor:product"
            )
        vendor = parts[0]
        product = parts[1]
        version = parts[2] if len(parts) > 2 else "*"

    return CPEEntry(
        vendor=vendor.strip(),
        product=product.strip(),
        version=_clean_version_token(version),
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _equal_ci(left: str, right: str) -> bool:
    """Case-insensitive equality after trimming whitespace."""

    return left.strip().lower() == right.strip().lower()


def _clean_version_token(token: str | None) -> str | None:
    """Normalize a CPE version component.

    CPE 2.3 represents "any" / "unspecified" as ``"*"`` or ``"-"``; the
    payload shorthand occasionally carries an empty string. All three
    collapse to ``None`` so downstream callers can treat "no pinned
    version" as a single case.
    """

    if token is None:
        return None
    stripped = token.strip()
    if stripped in _CPE_WILDCARDS:
        return None
    return stripped


def _split_cpe_components(body: str) -> list[str]:
    """Split a CPE 2.3 body on unescaped colons.

    NVD escapes literal colons inside components as ``\\:``. The naive
    ``str.split(":")`` would chop those in half. The splitter here walks
    the string character by character, honouring the backslash escape.
    """

    components: list[str] = []
    buffer: list[str] = []
    i = 0
    while i < len(body):
        ch = body[i]
        if ch == "\\" and i + 1 < len(body):
            # Preserve escape sequences verbatim — downstream equality
            # checks strip them out if needed.
            buffer.append(body[i + 1])
            i += 2
            continue
        if ch == ":":
            components.append("".join(buffer))
            buffer = []
            i += 1
            continue
        buffer.append(ch)
        i += 1
    components.append("".join(buffer))
    return components


def _try_parse_version(token: str | None) -> Version | None:
    """Parse ``token`` as a :class:`Version` or return ``None`` on failure."""

    if token is None:
        return None
    try:
        return Version(token)
    except InvalidVersion:
        return None


def _version_in_range(fp_version: Version, cpe: CPEEntry) -> bool:
    """Return ``True`` iff ``fp_version`` is inside the CPE's version range.

    The range is derived from three CPE fields with the following
    precedence:

    1. If either ``version_start_including`` or ``version_end_excluding``
       is set, use them as the bounds (treating unset bounds as open).
    2. Else, if ``cpe.version`` is set (non-wildcard), require exact
       equality to that pinned version.
    3. Else, the range is open on both sides — every version matches.

    Any bound that fails to parse is treated as "open" rather than
    rejecting the whole match, which mirrors NVD's tolerance for
    malformed upstream entries.
    """

    start = _try_parse_version(cpe.version_start_including)
    end = _try_parse_version(cpe.version_end_excluding)

    if start is not None or end is not None:
        if start is not None and fp_version < start:
            return False
        if end is not None and fp_version >= end:
            return False
        return True

    pinned = _try_parse_version(cpe.version)
    if pinned is not None:
        return fp_version == pinned

    # No constraints — the CPE describes "all versions".
    return True
