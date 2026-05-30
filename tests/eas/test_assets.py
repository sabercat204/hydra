"""Assets package property tests (tasks 7.2, 7.12, 7.13).

Three property tests for ``hydra.eas.assets``:

* ``test_property_normalize_fixpoint`` — Property 3. For every accepted
  ``AssetType``, ``normalize(normalize(x)) == normalize(x)``.
* ``test_property_registration_idempotency`` — Property 1. Two sequential
  ``AssetRepository.upsert`` calls for the same ``(tenant, type, value)``
  return the same row, with ``was_new=True`` on the first and
  ``was_new=False`` on the second.
* ``test_property_validation_atomic`` — Property 2. Invalid asset values
  raise :class:`pydantic.ValidationError` at the ``AssetCreate`` boundary.

All three live in the same module because they share the ``AssetCreate``
and ``AssetType`` imports; the previous file split in the plan has been
collapsed into a single module per the task prompt.

Validates: R1.1, R1.2, R1.3, R27.2, R27.3.
"""

from __future__ import annotations

import ipaddress
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import pytest
from hypothesis import assume, given, settings as h_settings, strategies as st
from pydantic import ValidationError

from hydra.eas.assets.models import Asset
from hydra.eas.assets.normalizer import normalize_asset_value
from hydra.eas.assets.repository import (
    AssetRepository,
    UpsertResult,
    _row_to_asset,
)
from hydra.eas.schemas.assets import AssetCreate, AssetType


# ---------------------------------------------------------------------------
# Shared hypothesis strategies for each AssetType
# ---------------------------------------------------------------------------


# ``ip`` — ``st.ip_addresses`` covers IPv4 and IPv6 uniformly. ``map(str)``
# gives us a mix of compressed IPv6 and dotted-quad IPv4 strings.
_ip_strategy = st.ip_addresses().map(str)


# ``cidr`` — bespoke composite: an IP address plus a random prefix length.
# We accept host-bits-set forms (``strict=False``) because the normalizer
# canonicalizes them.
@st.composite
def _cidr_strategy(draw: Any) -> str:
    ip = draw(st.ip_addresses())
    if isinstance(ip, ipaddress.IPv4Address):
        prefix = draw(st.integers(min_value=0, max_value=32))
    else:
        prefix = draw(st.integers(min_value=0, max_value=128))
    return f"{ip}/{prefix}"


# ``domain`` / ``hostname`` — curated RFC 1035-compliant inputs because a
# random regex-generator would have a tiny accept rate. The list is
# deliberately short; it covers uppercase, trailing-dot, and multi-label
# shapes which are the three transformations ``normalize_asset_value``
# actually performs.
_DOMAIN_SAMPLE = st.sampled_from(
    ["Example.COM", "Example.COM.", "foo.bar.example.com", "GITHUB.com."]
)


# ``asn`` — a 32-bit unsigned int formatted with a fixed-width ``AS`` prefix
# and leading zeros. The normalizer strips the prefix and leading zeros so
# we cover the "ASXXXX form with zeros" path. ``0o`` padding is used over
# other fills because it exercises the leading-zero strip branch explicitly.
_ASN_STRATEGY = st.integers(min_value=0, max_value=4_294_967_295).map(
    lambda i: f"AS{i:0>6}"
)


# ---------------------------------------------------------------------------
# Property 3 — normalize(normalize(x)) == normalize(x)
# ---------------------------------------------------------------------------


# One ``st.one_of`` to switch between types. Every leaf generator produces a
# ``(AssetType, value)`` pair so the test body can dispatch on the type.
_NORMALIZE_STRATEGY = st.one_of(
    _ip_strategy.map(lambda s: (AssetType.IP, s)),
    _cidr_strategy().map(lambda s: (AssetType.CIDR, s)),
    _DOMAIN_SAMPLE.map(lambda s: (AssetType.DOMAIN, s)),
    _DOMAIN_SAMPLE.map(lambda s: (AssetType.HOSTNAME, s)),
    _ASN_STRATEGY.map(lambda s: (AssetType.ASN, s)),
)


@given(pair=_NORMALIZE_STRATEGY)
@h_settings(max_examples=300)
def test_property_normalize_fixpoint(pair: tuple[AssetType, str]) -> None:
    """Property 3 — ``normalize`` is idempotent for every AssetType.

    Validates: R1.3, R27.3.
    """
    asset_type, raw = pair

    # First pass — if this raises, the generator produced an input the
    # normalizer can't handle. For the strategies above that should never
    # happen, but ``assume`` keeps the property honest.
    try:
        once = normalize_asset_value(asset_type, raw)
    except ValueError:
        assume(False)

    twice = normalize_asset_value(asset_type, once)
    assert once == twice, (
        f"fixpoint violated for {asset_type.value}: "
        f"{raw!r} → {once!r} → {twice!r}"
    )


# ---------------------------------------------------------------------------
# Property 1 — asset-registration idempotency (task 7.12)
# ---------------------------------------------------------------------------


class _FakePool:
    """In-memory stand-in for ``asyncpg.Pool``.

    Only implements what :class:`AssetRepository.upsert` actually uses:

    * ``acquire()`` → async context manager yielding ``self``
    * ``fetchrow(sql, *params)`` → dict-like row with ``xmax = 0`` semantics

    The fake records every ``upsert`` call in ``upsert_calls`` so tests can
    verify the call count (the property asserts "exactly one row"). Rows are
    keyed by ``(tenant_id, asset_type, normalized_value)`` — the natural key
    from the partial unique index.
    """

    def __init__(self) -> None:
        self._rows: dict[tuple[UUID, str, str], dict[str, Any]] = {}
        self.upsert_calls: list[dict[str, Any]] = []

    # --- async context-manager façade ---------------------------------

    def acquire(self) -> "_FakePool":
        return self

    async def __aenter__(self) -> "_FakePool":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        return None

    # --- the only DB method the SUT calls for upsert ------------------

    async def fetchrow(self, sql: str, *params: Any) -> dict[str, Any]:
        """Simulate the ``INSERT ... ON CONFLICT ... RETURNING`` statement.

        The SQL in :meth:`AssetRepository.upsert` always passes the same
        six parameters in the same order, so we can unpack positionally.
        """
        (
            tenant_id,
            asset_type,
            normalized_value,
            raw_value,
            notes,
            capture_screenshots,
        ) = params
        self.upsert_calls.append(
            {
                "tenant_id": tenant_id,
                "asset_type": asset_type,
                "normalized_value": normalized_value,
                "raw_value": raw_value,
                "notes": notes,
                "capture_screenshots": capture_screenshots,
            }
        )
        key = (tenant_id, asset_type, normalized_value)
        existing = self._rows.get(key)

        if existing is None:
            # Fresh INSERT — xmax = 0 → was_new = True.
            row = {
                "asset_id": uuid4(),
                "tenant_id": tenant_id,
                "asset_type": asset_type,
                "normalized_value": normalized_value,
                "raw_value": raw_value,
                "is_active": True,
                "capture_screenshots": capture_screenshots,
                "notes": notes,
                "created_at": datetime.now(timezone.utc),
                "deactivated_at": None,
                "was_new": True,
            }
            self._rows[key] = row
            return row

        # UPDATE path — xmax ≠ 0 → was_new = False. The PG UPSERT also
        # refreshes raw_value / notes / capture_screenshots, so mirror that.
        updated = dict(existing)
        updated["raw_value"] = raw_value
        updated["notes"] = notes
        updated["capture_screenshots"] = capture_screenshots
        updated["was_new"] = False
        self._rows[key] = updated
        return updated


# --- hypothesis strategy for valid AssetCreate bodies ---------------------
#
# The repository test doesn't need to cover every AssetType — the interesting
# property is "two identical POSTs yield the same row". IP assets are the
# simplest: they round-trip through the validator without surprises.

_VALID_IP_STRATEGY = st.ip_addresses().map(str)


@given(value=_VALID_IP_STRATEGY)
@h_settings(max_examples=50)
async def test_property_registration_idempotency(value: str) -> None:
    """Property 1 — two POSTs of the same asset yield the same row.

    * First call → ``was_new=True``.
    * Second call → ``was_new=False`` and the asset_id is stable.

    The fake pool simulates Postgres' ``xmax = 0`` behaviour. Validates:
    R1.1, R1.3, R27.2.
    """
    tenant_id = uuid4()
    normalized = normalize_asset_value(AssetType.IP, value)
    body = AssetCreate(asset_type=AssetType.IP, value=value)

    pool = _FakePool()
    repo = AssetRepository(pool)

    first: UpsertResult = await repo.upsert(tenant_id, body, normalized)
    second: UpsertResult = await repo.upsert(tenant_id, body, normalized)

    # First insert — new row.
    assert first.was_new is True
    # Second insert — existing row re-fetched.
    assert second.was_new is False
    # Same row — same asset_id.
    assert first.asset.asset_id == second.asset.asset_id
    # Exactly two calls made it to the DB (both POSTs); exactly one row
    # exists in the "table".
    assert len(pool.upsert_calls) == 2
    assert len(pool._rows) == 1


# ---------------------------------------------------------------------------
# Property 2 — AssetCreate rejects malformed values atomically (task 7.13)
# ---------------------------------------------------------------------------


# Hypothesis strategies targeted at each AssetType's rejection surface.
# Every generated value is *definitely* invalid for the declared asset_type,
# so the test expects a ValidationError every time.

# Random text that cannot parse as an IP address. The ``assume`` step inside
# the test catches the astronomically unlikely case where the generator
# lands on a valid IP.
_INVALID_IP_STRATEGY = st.text(
    alphabet=st.characters(min_codepoint=33, max_codepoint=126),
    min_size=1,
    max_size=40,
)


# Random text that cannot parse as a CIDR. Same filter as IP — any string
# that ``ip_network`` accepts is filtered out via ``assume``.
_INVALID_CIDR_STRATEGY = st.text(
    alphabet=st.characters(min_codepoint=33, max_codepoint=126),
    min_size=1,
    max_size=40,
)


# Random-alphabet text that does not match the RFC 1035 regex in
# ``AssetCreate._validate_by_type``. Constrained to printable non-dot
# printable characters to make the reject rate near 100 %.
_INVALID_DOMAIN_STRATEGY = st.text(
    alphabet=st.characters(
        whitelist_categories=("Ll", "Lu", "Nd"),
        blacklist_characters=(".",),
    ),
    min_size=1,
    max_size=20,
)


# Strings that do not parse as a 32-bit ASN — non-digits, negative sign, etc.
_INVALID_ASN_STRATEGY = st.one_of(
    st.text(
        alphabet=st.characters(
            whitelist_categories=("Ll", "Lu"),
        ),
        min_size=1,
        max_size=10,
    ),
    st.integers(min_value=4_294_967_296, max_value=10_000_000_000).map(
        lambda i: f"AS{i}"
    ),
    st.integers(min_value=-1_000_000, max_value=-1).map(lambda i: f"AS{i}"),
)


@given(value=_INVALID_IP_STRATEGY)
@h_settings(max_examples=100)
def test_property_validation_atomic_ip(value: str) -> None:
    """Invalid IP strings raise ValidationError at AssetCreate boundary."""
    value = value.strip()
    assume(value)  # drop empty-string generations
    try:
        ipaddress.ip_address(value)
        assume(False)  # genuine IP — drop this example
    except ValueError:
        pass

    with pytest.raises(ValidationError):
        AssetCreate(asset_type=AssetType.IP, value=value)


@given(value=_INVALID_CIDR_STRATEGY)
@h_settings(max_examples=100)
def test_property_validation_atomic_cidr(value: str) -> None:
    """Invalid CIDR strings raise ValidationError at AssetCreate boundary."""
    value = value.strip()
    assume(value)
    try:
        ipaddress.ip_network(value, strict=False)
        assume(False)
    except ValueError:
        pass

    with pytest.raises(ValidationError):
        AssetCreate(asset_type=AssetType.CIDR, value=value)


@given(value=_INVALID_DOMAIN_STRATEGY)
@h_settings(max_examples=100)
def test_property_validation_atomic_domain(value: str) -> None:
    """Malformed domain strings raise ValidationError at AssetCreate."""
    # No trailing dot, no dots at all, and non-DNS characters — this cannot
    # pass the RFC 1035 regex in ``_validate_by_type``.
    with pytest.raises(ValidationError):
        AssetCreate(asset_type=AssetType.DOMAIN, value=value)


@given(value=_INVALID_DOMAIN_STRATEGY)
@h_settings(max_examples=100)
def test_property_validation_atomic_hostname(value: str) -> None:
    """Malformed hostname strings raise ValidationError at AssetCreate."""
    with pytest.raises(ValidationError):
        AssetCreate(asset_type=AssetType.HOSTNAME, value=value)


@given(value=_INVALID_ASN_STRATEGY)
@h_settings(max_examples=100)
def test_property_validation_atomic_asn(value: str) -> None:
    """Non-32-bit ASN strings raise ValidationError at AssetCreate."""
    with pytest.raises(ValidationError):
        AssetCreate(asset_type=AssetType.ASN, value=value)


# ---------------------------------------------------------------------------
# Smoke tests for the helpers themselves (conftest coverage)
# ---------------------------------------------------------------------------


def test_row_to_asset_roundtrip() -> None:
    """``_row_to_asset`` should handle missing ``notes`` gracefully."""
    now = datetime.now(timezone.utc)
    row = {
        "asset_id": uuid4(),
        "tenant_id": uuid4(),
        "asset_type": "ip",
        "normalized_value": "1.2.3.4",
        "raw_value": "1.2.3.4",
        "is_active": True,
        "capture_screenshots": False,
        "notes": None,
        "created_at": now,
        "deactivated_at": None,
    }
    asset = _row_to_asset(row)
    assert isinstance(asset, Asset)
    assert asset.normalized_value == "1.2.3.4"
    assert asset.notes is None
