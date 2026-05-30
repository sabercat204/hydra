"""Indicator normalization for fast lookup (Design §3.7, R16.2–R16.4).

``normalize_indicator(cls, value)`` returns the canonical string used for
cache keys and per-class equality:

* ``ipv4`` / ``ipv6`` → ``ipaddress.ip_address(value).compressed`` per R16.2.
  IPv4 stays numeric-dotted; IPv6 collapses ``::`` and lowercases hex.
* ``domain`` / ``hostname`` → IDNA-encoded ASCII, lowercased, with a single
  trailing dot stripped per R16.3. Non-ASCII input routes through
  ``str.encode("idna").decode("ascii")`` so that U-label equivalents map
  to the same A-label cache key.
* ``hash`` → lowercase hex; length must be one of 16/32/40/64 per R16.4.

**Fixpoint (Property 3).** ``normalize(normalize(x)) == normalize(x)`` for
every accepted input. The IP path is fixpoint-stable because
``.compressed`` is itself canonical. The domain path is stable because
lowercasing + IDNA on an already-ASCII lowercase label is a no-op and
stripping a trailing dot only applies once. The hash path is stable
because lowercase hex of the right length is already canonical.

A :class:`ValueError` is raised when the input doesn't conform to the
class (e.g. a non-hex hash, non-IDNA domain, non-IP string for ``ipv4``).
Callers in the router path have already classified via
:func:`classify_indicator`, so this is defensive.
"""

from __future__ import annotations

import ipaddress

from hydra.eas.schemas.lookup import IndicatorClass

__all__ = ["normalize_indicator"]


# Hash length → label, used for the length check in the ``hash`` branch.
# We don't need the labels, just the set for ``in`` membership.
_VALID_HASH_LENGTHS = frozenset({16, 32, 40, 64})


def normalize_indicator(cls: IndicatorClass, value: str) -> str:
    """Return the canonical form of ``value`` for class ``cls``.

    The class argument drives the per-branch normalization rules. The
    returned string is what the router hashes into the cache key via
    ``hydra:eas:lookup:{cls}:{value}`` (Design §3.7).
    """

    if not isinstance(value, str):
        raise ValueError(f"indicator must be a string, got {type(value).__name__}")
    stripped = value.strip()
    if not stripped:
        raise ValueError("indicator must not be empty")

    if cls in ("ipv4", "ipv6"):
        # ``ip_address`` raises ``ValueError`` for bad input. The
        # ``.compressed`` form is idempotent: v4 is already canonical,
        # v6 lowercases + ``::`` collapses. That makes the fixpoint
        # property trivially true for this branch.
        return ipaddress.ip_address(stripped).compressed

    if cls in ("domain", "hostname"):
        return _normalize_domain_or_hostname(stripped)

    if cls == "hash":
        lowered = stripped.lower()
        if len(lowered) not in _VALID_HASH_LENGTHS:
            raise ValueError(
                f"hash length {len(lowered)} not in {sorted(_VALID_HASH_LENGTHS)}"
            )
        # Validate hex shape — mirroring the classifier's precondition
        # but without the regex import. ``bytes.fromhex`` tolerates
        # odd-length input (lengths 16/32/40/64 are all even, so the
        # length guard above is enough to avoid a fromhex error), and
        # raises ValueError for non-hex characters which we re-raise
        # as a plain message for consistency.
        try:
            bytes.fromhex(lowered)
        except ValueError as exc:
            raise ValueError(f"hash must be hex: {exc}") from exc
        return lowered

    raise ValueError(f"unknown indicator class: {cls!r}")


def _normalize_domain_or_hostname(value: str) -> str:
    """Lowercase + IDNA + strip single trailing dot (R16.3).

    Three-step pipeline:

    1. Strip one trailing dot if present — FQDN notation is semantically
       equivalent to the dot-less form.
    2. IDNA-encode the result when any non-ASCII character is present.
       ``str.encode("idna")`` performs NFC normalization + Punycode per
       RFC 3490, yielding an ASCII A-label (e.g. ``xn--...``). ASCII
       input skips the encode step because ``"abc".encode("idna")``
       still succeeds but forcing the round-trip is a no-op we can
       elide.
    3. Lowercase — RFC 1035 labels are case-insensitive, and
       ``"ABC".encode("idna")`` returns uppercase A-labels on some
       versions, so we lowercase *after* the encode to catch both.

    Fixpoint: once a domain is ASCII-lowercase with no trailing dot,
    re-applying this pipeline is a no-op.
    """

    stripped = value.rstrip(".") if value.endswith(".") else value
    if not stripped:
        raise ValueError("domain/hostname must not be empty")

    # IDNA-encoded when the input has non-ASCII, otherwise pass through.
    # ``str.isascii`` is cheap and avoids the exception-handling dance
    # of attempting IDNA on already-ASCII input.
    if stripped.isascii():
        return stripped.lower()

    try:
        encoded = stripped.encode("idna").decode("ascii")
    except UnicodeError as exc:
        # Bad Unicode labels (empty labels, over-long labels, etc.)
        # raise ``UnicodeError`` subclasses. Re-raise as ValueError
        # for the router's 422 ``INDICATOR_NOT_CLASSIFIED`` path.
        raise ValueError(f"IDNA encoding failed: {exc}") from exc
    return encoded.lower()
