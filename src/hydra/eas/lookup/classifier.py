"""Indicator classification for fast lookup (Design §3.7, R16.1).

``classify_indicator(value)`` returns one of ``{"ipv4","ipv6","domain","hostname","hash"}``
or ``None`` when no class matches. The classifier precedence, following the
task brief and R16.1/R16.4:

1. **IP address** — ``ipaddress.ip_address(value)`` decides between ``"ipv4"``
   and ``"ipv6"`` by the returned object's ``version`` attribute.
2. **Hash** — hex strings of length 16, 32, 40, or 64 map to ``"hash"``
   (xxhash64, MD5, SHA-1, SHA-256 per R16.4).
3. **Domain** — RFC 1035-shaped names with at least one label separator
   (a dot) map to ``"domain"``. The ``HOSTNAME`` asset-type is reserved
   for single-label hostnames; for lookup normalization, the "domain"
   path is canonical for anything with dots.
4. **Hostname** — alphanumeric strings without dots (single labels)
   map to ``"hostname"``.
5. Everything else returns ``None`` — the router raises 422
   ``INDICATOR_NOT_CLASSIFIED`` in that case.

The function is pure and idempotent with respect to the normalizer —
``classify_indicator(normalize_indicator(c, x)) == c`` for every value
``x`` the classifier accepted as class ``c``. This underpins Property 3
(normalization fixpoint) in concert with :mod:`.normalizer`.
"""

from __future__ import annotations

import ipaddress
import re

from hydra.eas.schemas.lookup import IndicatorClass

__all__ = ["classify_indicator"]


# Allowed hash lengths per R16.4 — 16 (xxhash64), 32 (MD5), 40 (SHA-1),
# 64 (SHA-256). Case-insensitive: the normalizer lowercases before use.
_HASH_LENGTHS = frozenset({16, 32, 40, 64})
_HEX_RE = re.compile(r"^[0-9a-fA-F]+$")


# RFC 1035-style domain regex, mirroring ``AssetCreate._DOMAIN_RE`` in
# ``schemas/assets.py``. Accepts an optional trailing dot so the classifier
# matches FQDN-form input (``example.com.``) before the normalizer strips
# it. Labels are 1..63 chars, total 1..253 chars.
_DOMAIN_RE = re.compile(
    r"^(?=.{1,253}\.?$)(?!-)([A-Za-z0-9-]{1,63}(?<!-)\.)+[A-Za-z]{2,63}\.?$"
)


# Single-label hostname — alphanumeric with optional hyphens, 1..63 chars.
# The task brief says "contains NO dots and is alphanumeric"; we allow
# hyphens because RFC 1123 does and bare hostnames like ``my-host`` are
# common. No leading/trailing hyphen per RFC 1035 §2.3.1.
_HOSTNAME_RE = re.compile(r"^(?!-)[A-Za-z0-9-]{1,63}(?<!-)$")


def classify_indicator(value: str) -> IndicatorClass | None:
    """Return the :data:`IndicatorClass` for ``value`` or ``None``.

    Precedence order is IP → hash → domain → hostname, matching the task
    brief. A single trailing dot is tolerated on domain-shaped input so
    that fully-qualified names ingest cleanly.
    """

    if not isinstance(value, str):
        return None
    stripped = value.strip()
    if not stripped:
        return None

    # ---- IP address ---------------------------------------------------
    # ``ip_address`` raises ``ValueError`` on anything that isn't a valid
    # v4 or v6 literal. We branch on ``.version`` rather than trying
    # each family separately — the stdlib has already done the parsing
    # work and the object carries the authoritative label.
    try:
        ip_obj = ipaddress.ip_address(stripped)
    except ValueError:
        pass
    else:
        return "ipv4" if ip_obj.version == 4 else "ipv6"

    # ---- Hash (hex of a known length) ---------------------------------
    # The hash check comes before domain because a bare 40-char hex
    # string (SHA-1) has no dots and would otherwise not even reach the
    # hostname branch. R16.4 specifies 16/32/40/64; anything else is not
    # a hash.
    if len(stripped) in _HASH_LENGTHS and _HEX_RE.match(stripped) is not None:
        return "hash"

    # ---- Domain (has at least one dot, RFC 1035) ----------------------
    # ``_DOMAIN_RE`` already enforces at least one label + a final TLD
    # separator, so a dot-less value cannot pass. The normalizer
    # lowercases, IDNA-encodes, and strips a single trailing dot.
    if _DOMAIN_RE.match(stripped) is not None:
        return "domain"

    # ---- Hostname (no dots, alphanumeric/hyphen label) ----------------
    # Single-label hostnames never make it past the domain regex because
    # that regex requires at least one ``label.`` followed by a TLD. A
    # bare ``myhost`` or ``server01`` lands here.
    if "." not in stripped and _HOSTNAME_RE.match(stripped) is not None:
        return "hostname"

    return None
