"""Asset-value normalization (Design §3.2, Property 3).

Maps a caller-supplied raw asset value into the canonical ``normalized_value``
persisted in ``assets.normalized_value`` and looked up by
:class:`hydra.eas.assets.matcher.AssetMatcher`. Normalization is **fixpoint
stable** per R1.3 / R27.3 / Property 3:

    normalize(normalize(x)) == normalize(x)

for every accepted ``asset_type``. Idempotency is the contract this module
provides; tests in ``tests/eas/test_assets.py`` hypothesize over all five
types.

Per-type rules:

* ``IP`` — ``ipaddress.ip_address(raw.strip()).compressed``. Both IPv4 and
  IPv6 flow through the same code; ``.compressed`` canonicalizes the
  textual form (lowercase hex, :: collapse, no leading zeros).
* ``CIDR`` — ``str(ipaddress.ip_network(raw.strip(), strict=False))``.
  ``strict=False`` accepts host-bits-set inputs like ``192.0.2.1/24`` and
  normalizes them to ``192.0.2.0/24``. For IPv6 this also compresses the
  network portion.
* ``DOMAIN`` / ``HOSTNAME`` — ``raw.strip().lower()`` with a single
  trailing dot stripped. RFC 1035 labels are case-insensitive; the API
  already validates the input shape via :class:`AssetCreate`.
* ``ASN`` — strip whitespace, uppercase, remove ``AS`` prefix, then pass
  through ``str(int(x))`` to drop leading zeros (``"AS00042" → "42"``).

This module raises :class:`ValueError` when the input cannot be parsed for
the declared type. The Pydantic ``AssetCreate`` validator does the
user-facing 422 rejection; normalizer failures should therefore be rare
and indicate a server-side bug, not user error.
"""

from __future__ import annotations

import ipaddress

from hydra.eas.schemas.assets import AssetType

__all__ = ["normalize_asset_value"]


def normalize_asset_value(asset_type: AssetType, raw: str) -> str:
    """Return the canonical string form of ``raw`` for the given ``asset_type``.

    The returned value is what gets stored in ``assets.normalized_value`` and
    what :class:`AssetMatcher` compares against. The function is a **fixpoint**
    — applying it twice is equivalent to applying it once (Property 3).
    """

    value = raw.strip()

    if asset_type is AssetType.IP:
        # ``ip_address`` accepts both IPv4 and IPv6 forms and ``.compressed``
        # gives the canonical textual form for both families.
        return ipaddress.ip_address(value).compressed

    if asset_type is AssetType.CIDR:
        # ``strict=False`` canonicalizes inputs whose host bits are set
        # (e.g. ``192.0.2.1/24`` → ``192.0.2.0/24``). IPv6 networks are
        # automatically compressed by the ``__str__`` implementation.
        return str(ipaddress.ip_network(value, strict=False))

    if asset_type in (AssetType.DOMAIN, AssetType.HOSTNAME):
        lowered = value.lower()
        # A single trailing FQDN dot is the conventional indicator of a
        # fully-qualified name; it's semantically equivalent and should
        # not be preserved in the stored canonical form.
        if lowered.endswith("."):
            lowered = lowered[:-1]
        return lowered

    if asset_type is AssetType.ASN:
        upper = value.upper()
        stripped = upper.removeprefix("AS")
        # ``int()`` rejects stray whitespace and non-digits; the
        # ``AssetCreate`` validator already guarded this branch, so a
        # parse failure here indicates the caller skipped validation.
        return str(int(stripped))

    # Defensive fallthrough — ``AssetType`` is a closed enum so this branch
    # is unreachable under the type checker; kept here so that a future
    # enum addition fails loudly rather than silently returning junk.
    raise ValueError(f"Unsupported asset_type: {asset_type!r}")
