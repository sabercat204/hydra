"""AssetMatcher determinism property test (task 7.4).

Property 7 — *Exposure-matching correctness (match determinism)*. For every
asset_type, ``AssetMatcher.is_match(indicator, asset)`` is a pure function
of its inputs, so calling it twice with the same arguments yields the same
boolean. Repeated here with Hypothesis-generated inputs per type because
the production code has per-type branches that each deserve their own
coverage.

Validates: R3.1, R3.2, R27.7, Property 7.
"""

from __future__ import annotations

import ipaddress
from typing import Any
from uuid import uuid4

import pytest
from hypothesis import given, settings as h_settings, strategies as st

from hydra.eas.assets.matcher import AssetMatcher
from hydra.eas.schemas.assets import AssetType

from tests.eas.conftest import make_asset


# ---------------------------------------------------------------------------
# Module-level matcher (ASN path short-circuits to False — no pyasn DB)
# ---------------------------------------------------------------------------


# A single matcher reused across tests: ``asn_database_path=None`` means the
# ASN branch's ``_load_asn_database`` returns ``None``, so ``is_match`` for
# asn-type assets returns ``False`` without raising. That suits a
# determinism test — the result is False but still deterministic.
_MATCHER = AssetMatcher(asn_database_path=None)


# ---------------------------------------------------------------------------
# Per-type indicator / asset-value strategies
# ---------------------------------------------------------------------------


_IP_STRATEGY = st.ip_addresses().map(str)


@st.composite
def _cidr_value(draw: Any) -> str:
    ip = draw(st.ip_addresses())
    if isinstance(ip, ipaddress.IPv4Address):
        prefix = draw(st.integers(min_value=0, max_value=32))
    else:
        prefix = draw(st.integers(min_value=0, max_value=128))
    return f"{ip}/{prefix}"


# Curated domain sample — ``AssetCreate._validate_by_type`` and the matcher
# both expect RFC 1035-like strings, and random generation has a near-zero
# accept rate. Mixed casing ensures the matcher's ``.lower()`` fold fires.
_DOMAIN_SAMPLE = st.sampled_from(
    [
        "example.com",
        "Example.COM",
        "foo.bar.example.com",
        "GITHUB.com",
        "a.b.c.example.net",
        "localhost.localdomain",
    ]
)


# A numeric ASN plus an explicit "AS"-prefixed form; both are valid indicator
# shapes (depending on where the indicator came from). Normalized assets
# store just the number.
_ASN_INT_STRATEGY = st.integers(min_value=0, max_value=4_294_967_295)


# ---------------------------------------------------------------------------
# Property 7 per AssetType
# ---------------------------------------------------------------------------


@given(indicator=_IP_STRATEGY, asset_value=_IP_STRATEGY)
@h_settings(max_examples=200)
def test_is_match_ip_deterministic(indicator: str, asset_value: str) -> None:
    """IP matching is deterministic across repeated calls."""
    asset = make_asset(asset_type=AssetType.IP.value, normalized_value=asset_value)
    a = _MATCHER.is_match(indicator, asset)
    b = _MATCHER.is_match(indicator, asset)
    assert a == b
    assert isinstance(a, bool)


@given(indicator=_IP_STRATEGY, asset_value=_cidr_value())
@h_settings(max_examples=200)
def test_is_match_cidr_deterministic(indicator: str, asset_value: str) -> None:
    """CIDR containment matching is deterministic across repeated calls.

    The asset holds a normalized CIDR (host bits zeroed, IPv6 compressed)
    and the indicator is a plain IP address. The matcher decides
    containment; we only check determinism, not correctness of the
    containment itself.
    """
    # Normalize the CIDR the way the repository would have stored it, so the
    # matcher sees the production-shape input.
    canonical = str(ipaddress.ip_network(asset_value, strict=False))
    asset = make_asset(asset_type=AssetType.CIDR.value, normalized_value=canonical)
    a = _MATCHER.is_match(indicator, asset)
    b = _MATCHER.is_match(indicator, asset)
    assert a == b
    assert isinstance(a, bool)


@given(indicator=_DOMAIN_SAMPLE, asset_value=_DOMAIN_SAMPLE)
@h_settings(max_examples=200)
def test_is_match_domain_deterministic(indicator: str, asset_value: str) -> None:
    """Domain suffix matching is deterministic across repeated calls."""
    # Store the asset value in its normalized (lowercased, trailing-dot
    # stripped) form because that's what the repository column would hold.
    normalized = asset_value.lower().rstrip(".")
    asset = make_asset(
        asset_type=AssetType.DOMAIN.value, normalized_value=normalized
    )
    a = _MATCHER.is_match(indicator, asset)
    b = _MATCHER.is_match(indicator, asset)
    assert a == b
    assert isinstance(a, bool)


@given(indicator=_DOMAIN_SAMPLE, asset_value=_DOMAIN_SAMPLE)
@h_settings(max_examples=200)
def test_is_match_hostname_deterministic(indicator: str, asset_value: str) -> None:
    """Hostname exact-equality matching is deterministic."""
    normalized = asset_value.lower().rstrip(".")
    asset = make_asset(
        asset_type=AssetType.HOSTNAME.value, normalized_value=normalized
    )
    a = _MATCHER.is_match(indicator, asset)
    b = _MATCHER.is_match(indicator, asset)
    assert a == b
    assert isinstance(a, bool)


@given(indicator=_IP_STRATEGY, asn=_ASN_INT_STRATEGY)
@h_settings(max_examples=200)
def test_is_match_asn_deterministic(indicator: str, asn: int) -> None:
    """ASN matching is deterministic (always False without a pyasn DB).

    The module-level ``_MATCHER`` has ``asn_database_path=None``, so every
    ASN lookup returns ``False``. That's exactly the contract we're
    asserting: absent-DB failure is deterministic, not randomized.
    """
    asset = make_asset(asset_type=AssetType.ASN.value, normalized_value=str(asn))
    a = _MATCHER.is_match(indicator, asset)
    b = _MATCHER.is_match(indicator, asset)
    assert a == b
    assert a is False  # documented: no DB → no match


# ---------------------------------------------------------------------------
# Cross-type unified generator — belt-and-braces Property 7 coverage
# ---------------------------------------------------------------------------


@st.composite
def _typed_pair(draw: Any) -> tuple[str, Any]:
    """Build a random ``(indicator, asset)`` pair spanning all AssetTypes."""
    kind = draw(
        st.sampled_from(
            [AssetType.IP, AssetType.CIDR, AssetType.DOMAIN, AssetType.HOSTNAME, AssetType.ASN]
        )
    )
    if kind is AssetType.IP:
        indicator = draw(_IP_STRATEGY)
        asset_value = draw(_IP_STRATEGY)
        asset = make_asset(asset_type="ip", normalized_value=asset_value)
    elif kind is AssetType.CIDR:
        indicator = draw(_IP_STRATEGY)
        raw_cidr = draw(_cidr_value())
        canonical = str(ipaddress.ip_network(raw_cidr, strict=False))
        asset = make_asset(asset_type="cidr", normalized_value=canonical)
    elif kind is AssetType.DOMAIN:
        indicator = draw(_DOMAIN_SAMPLE)
        asset_value = draw(_DOMAIN_SAMPLE).lower().rstrip(".")
        asset = make_asset(asset_type="domain", normalized_value=asset_value)
    elif kind is AssetType.HOSTNAME:
        indicator = draw(_DOMAIN_SAMPLE)
        asset_value = draw(_DOMAIN_SAMPLE).lower().rstrip(".")
        asset = make_asset(asset_type="hostname", normalized_value=asset_value)
    else:  # AssetType.ASN
        indicator = draw(_IP_STRATEGY)
        asn = draw(_ASN_INT_STRATEGY)
        asset = make_asset(asset_type="asn", normalized_value=str(asn))
    return indicator, asset


@given(pair=_typed_pair())
@h_settings(max_examples=300)
def test_is_match_deterministic_all_types(pair: tuple[str, Any]) -> None:
    """Property 7 — determinism holds across every AssetType in one sweep."""
    indicator, asset = pair
    a = _MATCHER.is_match(indicator, asset)
    b = _MATCHER.is_match(indicator, asset)
    assert a == b


# ---------------------------------------------------------------------------
# A couple of unit tests so regressions don't silently no-op
# ---------------------------------------------------------------------------


def test_ip_exact_match_positive() -> None:
    asset = make_asset(asset_type="ip", normalized_value="192.0.2.1")
    assert _MATCHER.is_match("192.0.2.1", asset) is True


def test_ip_exact_match_negative() -> None:
    asset = make_asset(asset_type="ip", normalized_value="192.0.2.1")
    assert _MATCHER.is_match("192.0.2.2", asset) is False


def test_cidr_contains_positive() -> None:
    asset = make_asset(asset_type="cidr", normalized_value="192.0.2.0/24")
    assert _MATCHER.is_match("192.0.2.123", asset) is True


def test_cidr_contains_negative() -> None:
    asset = make_asset(asset_type="cidr", normalized_value="192.0.2.0/24")
    assert _MATCHER.is_match("10.0.0.1", asset) is False


def test_domain_suffix_match() -> None:
    asset = make_asset(asset_type="domain", normalized_value="example.com")
    assert _MATCHER.is_match("foo.example.com", asset) is True
    assert _MATCHER.is_match("example.com", asset) is True
    assert _MATCHER.is_match("counterexample.com", asset) is False


def test_hostname_exact_only() -> None:
    asset = make_asset(asset_type="hostname", normalized_value="host.example.com")
    assert _MATCHER.is_match("host.example.com", asset) is True
    assert _MATCHER.is_match("HOST.example.com", asset) is True  # case-fold
    assert _MATCHER.is_match("sub.host.example.com", asset) is False


def test_asn_match_returns_false_without_database() -> None:
    asset = make_asset(asset_type="asn", normalized_value="64512")
    # No pyasn DB wired → matcher short-circuits to False.
    assert _MATCHER.is_match("203.0.113.1", asset) is False
