"""Fingerprint extraction from NormalizedRecord payloads (R10.2).

The :class:`FingerprintExtractor` reads
``EASSettings.cve_fingerprint_map`` (keyed by source tier) to decide
which payload fields carry fingerprint strings. Recognised fingerprint
formats are:

* ``vendor/product/version`` — e.g. ``"apache/httpd/2.4.52"``. This is
  the canonical shape produced by the cyber-threat (Tier 16) adapters.
* ``vendor product version`` — e.g. ``"apache httpd 2.4.52"``. Some
  Tier 17 social-web ingestors strip the slashes.
* Banner-style ``Server: Apache/2.4.52`` — the Tier 28 national-portal
  scrape frequently captures the raw HTTP response header.

Parse failures (malformed strings, unknown formats) return ``None`` so
the CVE_Pipeline can skip the record without aborting the run.

``cve_fingerprint_map`` expressions are simple JSONPath-like strings
(``$.fingerprint``, ``$.service_banner``); the extractor supports
dotted/bracket notation for nested payloads without pulling in the full
JSONPath implementation (Design §9). The supported grammar is:

* ``$.key`` — top-level field.
* ``$.a.b`` — nested field.
* ``$.list[*]`` — iterate a list; the first value that yields a
  parseable fingerprint wins.
* ``$.a.b[0]`` — positional list access.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Iterable

from hydra.eas.cves.cpe_matcher import FingerprintTriple
from hydra.eas.settings import EASSettings
from hydra.models.normalized import NormalizedRecord

logger = logging.getLogger(__name__)

__all__ = ["FingerprintExtractor", "parse_fingerprint_string"]


# Matches ``Server: Apache/2.4.52`` (header prefix optional).
_HEADER_BANNER_RE = re.compile(
    r"^\s*(?:Server|Powered-?By|X-Powered-By)\s*:\s*(?P<rest>.+?)\s*$",
    re.IGNORECASE,
)
# Matches slash-or-space separated triples.
_TRIPLE_RE = re.compile(
    r"^\s*"
    r"(?P<vendor>[^\s/]+)"
    r"[\s/]+"
    r"(?P<product>[^\s/]+)"
    r"(?:[\s/]+(?P<version>[^\s/]+))?"
    r"\s*$"
)
# Matches ``Apache/2.4.52`` (product/version without vendor).
_PRODUCT_SLASH_VERSION_RE = re.compile(
    r"^\s*"
    r"(?P<product>[A-Za-z][A-Za-z0-9_\-.]*)"
    r"\s*/\s*"
    r"(?P<version>[0-9][0-9A-Za-z_.\-+]*)"
    r"\s*$"
)


class FingerprintExtractor:
    """Pull fingerprint strings from a record payload.

    Constructed once during ``setup_eas`` with the live
    :class:`EASSettings` so that the per-tier expression lists are read
    without crossing module boundaries on the hot path.
    """

    __slots__ = ("_settings",)

    def __init__(self, settings: EASSettings) -> None:
        self._settings = settings

    def extract(self, record: NormalizedRecord) -> FingerprintTriple | None:
        """Return the first parseable fingerprint for ``record`` or ``None``.

        Walks the expressions listed for the record's tier under
        ``cve_fingerprint_map`` and returns the first one whose string
        value yields a :class:`FingerprintTriple`. Returns ``None`` when
        the tier has no configured expressions, when none of the
        expressions resolve to a string, or when every resolved string
        fails to parse.
        """

        expressions = self._expressions_for_tier(int(record.tier))
        if not expressions:
            return None

        for expr in expressions:
            for raw in _resolve_expression(record.payload, expr):
                if not isinstance(raw, str):
                    continue
                triple = parse_fingerprint_string(raw)
                if triple is not None:
                    return triple
        return None

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _expressions_for_tier(self, tier: int) -> list[str]:
        """Map an integer tier to its configured fingerprint expressions.

        The ``cve_fingerprint_map`` model only configures tiers 16, 17
        and 28 by default; unknown tiers return an empty list so the
        extractor is safe to call for any record the pipeline sees.
        """

        fmap = self._settings.cve_fingerprint_map
        if tier == 16:
            return list(fmap.tier_16)
        if tier == 17:
            return list(fmap.tier_17)
        if tier == 28:
            return list(fmap.tier_28)
        return []


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def parse_fingerprint_string(raw: str) -> FingerprintTriple | None:
    """Parse a single fingerprint string into a :class:`FingerprintTriple`.

    Supported shapes (in decreasing specificity):

    * Header-style ``"Server: Apache/2.4.52"`` — vendor defaults to
      ``product`` when the header carries only ``product/version``.
    * Slash-separated triple ``"apache/httpd/2.4.52"``.
    * Space-separated triple ``"apache httpd 2.4.52"``.
    * Bare ``"product/version"`` — returned with ``vendor == product``.

    Returns ``None`` when the input does not match any shape. The caller
    can then try the next expression. ``None`` is also returned for
    empty / whitespace-only inputs.
    """

    if raw is None:
        return None
    value = raw.strip()
    if not value:
        return None

    header = _HEADER_BANNER_RE.match(value)
    if header:
        rest = header.group("rest").strip()
        parsed = _parse_product_version(rest)
        if parsed is not None:
            product, version = parsed
            return FingerprintTriple(vendor=product, product=product, version=version)
        # Fall through to try the triple parsers on the remainder.
        value = rest

    # A slash-only ``product/version`` string (no third component, no
    # spaces) is the banner shorthand — handle it before falling into
    # the triple regex, which would otherwise mis-read the version as
    # the product.
    if _is_bare_product_version(value):
        parsed = _parse_product_version(value)
        if parsed is not None:
            product, version = parsed
            return FingerprintTriple(vendor=product, product=product, version=version)

    triple = _TRIPLE_RE.match(value)
    if triple:
        return FingerprintTriple(
            vendor=triple.group("vendor").strip(),
            product=triple.group("product").strip(),
            version=_clean(triple.group("version")),
        )

    parsed = _parse_product_version(value)
    if parsed is not None:
        product, version = parsed
        return FingerprintTriple(vendor=product, product=product, version=version)

    return None


def _is_bare_product_version(value: str) -> bool:
    """Return ``True`` when ``value`` looks like ``product/version``.

    Specifically: exactly one ``/``, no whitespace, and both halves
    non-empty. We use this to steer the parser away from the triple
    regex for header-style shorthand like ``nginx/1.18.0``.
    """

    if "/" not in value or " " in value or "\t" in value:
        return False
    parts = value.split("/")
    return len(parts) == 2 and all(parts)


def _parse_product_version(value: str) -> tuple[str, str | None] | None:
    match = _PRODUCT_SLASH_VERSION_RE.match(value)
    if match is None:
        return None
    product = match.group("product").strip()
    version = _clean(match.group("version"))
    return product, version


def _clean(token: str | None) -> str | None:
    if token is None:
        return None
    stripped = token.strip()
    return stripped or None


# ---------------------------------------------------------------------------
# Mini JSONPath resolver (subset)
# ---------------------------------------------------------------------------


def _resolve_expression(payload: dict[str, Any], expression: str) -> Iterable[Any]:
    """Yield every value reachable from ``expression`` within ``payload``.

    Supported tokens inside the expression:

    * ``$`` — root, required as the first character.
    * ``.key`` — descend into a mapping by key.
    * ``[*]`` — iterate every list element.
    * ``[<int>]`` — list access by zero-based index.

    The resolver yields every matching value; callers typically stop at
    the first parseable one. Non-matching expressions yield nothing.
    """

    expression = expression.strip()
    if not expression.startswith("$"):
        return
    tokens = _tokenize(expression[1:])
    yield from _walk(payload, tokens)


def _tokenize(suffix: str) -> list[tuple[str, Any]]:
    """Convert the expression suffix into a list of ``(kind, value)`` tokens.

    Kinds are ``"key"``, ``"index"``, and ``"star"``.
    """

    tokens: list[tuple[str, Any]] = []
    i = 0
    while i < len(suffix):
        ch = suffix[i]
        if ch == ".":
            # Parse a key up to the next delimiter.
            j = i + 1
            while j < len(suffix) and suffix[j] not in ".[":
                j += 1
            key = suffix[i + 1:j]
            if key:
                tokens.append(("key", key))
            i = j
        elif ch == "[":
            end = suffix.find("]", i)
            if end == -1:
                return tokens  # malformed, bail quietly
            body = suffix[i + 1:end].strip()
            if body == "*":
                tokens.append(("star", None))
            else:
                try:
                    tokens.append(("index", int(body)))
                except ValueError:
                    return tokens
            i = end + 1
        else:
            i += 1
    return tokens


def _walk(current: Any, tokens: list[tuple[str, Any]]) -> Iterable[Any]:
    if not tokens:
        yield current
        return
    kind, value = tokens[0]
    rest = tokens[1:]
    if kind == "key":
        if isinstance(current, dict) and value in current:
            yield from _walk(current[value], rest)
        return
    if kind == "star":
        if isinstance(current, list):
            for item in current:
                yield from _walk(item, rest)
        return
    if kind == "index":
        if isinstance(current, list) and 0 <= value < len(current):
            yield from _walk(current[value], rest)
        return
