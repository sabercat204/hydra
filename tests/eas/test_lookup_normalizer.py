"""Property and unit tests for the indicator normalizer (task 13.2).

Exercises :func:`hydra.eas.lookup.normalizer.normalize_indicator` against
**Property 3 — Normalization fixpoint** from the EAS design doc:
``normalize(normalize(x)) == normalize(x)`` for every indicator ``x`` the
classifier accepted as class ``c``. The property is checked independently
per class (``ipv4``, ``ipv6``, ``domain``, ``hostname``, ``hash``), because
each branch of the normalizer takes a different canonicalization path:

* ``ipv4`` / ``ipv6`` — ``ipaddress.ip_address(value).compressed`` is itself
  canonical, so a second pass is a no-op.
* ``domain`` / ``hostname`` — lowercase + IDNA + single trailing-dot strip.
  Once the output is ASCII-lowercase with no trailing dot, re-applying
  the pipeline changes nothing.
* ``hash`` — lowercase hex of a whitelisted length. Lowercasing twice is
  the same as lowercasing once.

The file also covers the canonical-form sanity checks (one per branch),
error cases (empty / malformed input, unknown class), and a small
classifier+normalizer round-trip so that downstream cache keying
(``hydra:eas:lookup:{cls}:{value}`` per Design §3.7) stays consistent.

Validates: R16.2, R16.3, R16.4, R27.3.
"""

from __future__ import annotations

from ipaddress import IPv6Address

import pytest
from hypothesis import given, settings as h_settings, strategies as st

from hydra.eas.lookup.classifier import classify_indicator
from hydra.eas.lookup.normalizer import normalize_indicator


# ---------------------------------------------------------------------------
# Shared hypothesis strategies — one per indicator class
# ---------------------------------------------------------------------------


# IPv4 — a 32-bit unsigned integer rendered as a dotted quad. Using the
# integer bijection means every valid IPv4 address is reachable and no
# zero-padded octet is ever generated (Python 3.12's ``ipaddress`` rejects
# those after CVE-2021-29921). ``>>`` and ``& 0xFF`` is the canonical way
# to split the 32-bit value into four octets, high byte first.
_ipv4_from_int = st.integers(min_value=0, max_value=2**32 - 1).map(
    lambda n: ".".join(str((n >> (8 * shift)) & 0xFF) for shift in (3, 2, 1, 0))
)


# IPv6 — a 128-bit unsigned integer routed through ``IPv6Address`` so the
# generator emits arbitrary valid IPv6 strings (including the ``::`` zero
# compression forms that the normalizer is expected to canonicalize).
_ipv6_from_int = st.integers(min_value=0, max_value=2**128 - 1).map(
    lambda n: str(IPv6Address(n))
)


# Domain labels — RFC 1035 LDH characters (a-z, 0-9, hyphen). No leading
# or trailing hyphen, 1..20 chars to keep the examples readable. ``filter``
# is the cheapest way to reject the hyphen-at-the-edges cases; hypothesis
# will shrink past the filter for minimal counter-examples.
_domain_label = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyz0123456789-",
    min_size=1,
    max_size=20,
).filter(lambda s: not s.startswith("-") and not s.endswith("-"))


# TLDs — a small fixed set. The real IANA list has thousands of entries
# but the normalizer doesn't validate against it, so any accepted ASCII
# tail works. Sticking to a fixed set keeps the strategy fast and the
# generated domains human-readable.
_tld = st.sampled_from(["com", "org", "net", "io", "co", "edu", "gov"])


# Hostname alphabet — LDH plus uppercase, so the lowercasing branch of the
# normalizer gets exercised. Same no-hyphen-at-the-edges rule as
# ``_domain_label``.
_hostname_label = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-",
    min_size=1,
    max_size=63,
).filter(lambda s: not s.startswith("-") and not s.endswith("-"))


# Hash inputs — 16/32/40/64 hex chars mixed case, matching the four hash
# families R16.4 whitelists (xxhash64, MD5, SHA-1, SHA-256).
def _hex_of(length: int) -> st.SearchStrategy[str]:
    """Return a hypothesis strategy for hex strings of the given length.

    The alphabet is mixed-case so the lowercase normalization branch gets
    exercised.
    """
    return st.text(
        alphabet="0123456789abcdefABCDEF",
        min_size=length,
        max_size=length,
    )


# ---------------------------------------------------------------------------
# Property 3 — normalize(normalize(x)) == normalize(x) per class
# ---------------------------------------------------------------------------


@given(raw=_ipv4_from_int)
@h_settings(max_examples=200)
def test_property_normalize_ipv4_fixpoint(raw: str) -> None:
    """``normalize_indicator("ipv4", x)`` is idempotent.

    Validates: R16.2, R27.3.
    """
    once = normalize_indicator("ipv4", raw)
    twice = normalize_indicator("ipv4", once)
    assert once == twice, (
        f"fixpoint violated for ipv4: {raw!r} -> {once!r} -> {twice!r}"
    )


@given(raw=_ipv6_from_int)
@h_settings(max_examples=200)
def test_property_normalize_ipv6_fixpoint(raw: str) -> None:
    """``normalize_indicator("ipv6", x)`` is idempotent.

    Validates: R16.2, R27.3.
    """
    once = normalize_indicator("ipv6", raw)
    twice = normalize_indicator("ipv6", once)
    assert once == twice, (
        f"fixpoint violated for ipv6: {raw!r} -> {once!r} -> {twice!r}"
    )


@given(
    labels=st.lists(_domain_label, min_size=1, max_size=3),
    top=_tld,
    trailing_dot=st.booleans(),
)
@h_settings(max_examples=200)
def test_property_normalize_domain_fixpoint(
    labels: list[str], top: str, trailing_dot: bool
) -> None:
    """``normalize_indicator("domain", x)`` is idempotent.

    The generator constructs RFC 1035-shaped domain names so the
    normalizer's IDNA + lowercase + trailing-dot-strip path is exercised
    end-to-end.

    Validates: R16.3, R27.3.
    """
    raw = ".".join(labels + [top])
    if trailing_dot:
        raw += "."
    once = normalize_indicator("domain", raw)
    twice = normalize_indicator("domain", once)
    assert once == twice, (
        f"fixpoint violated for domain: {raw!r} -> {once!r} -> {twice!r}"
    )


@given(raw=_hostname_label)
@h_settings(max_examples=200)
def test_property_normalize_hostname_fixpoint(raw: str) -> None:
    """``normalize_indicator("hostname", x)`` is idempotent.

    Validates: R16.3, R27.3.
    """
    once = normalize_indicator("hostname", raw)
    twice = normalize_indicator("hostname", once)
    assert once == twice, (
        f"fixpoint violated for hostname: {raw!r} -> {once!r} -> {twice!r}"
    )


@given(length=st.sampled_from([16, 32, 40, 64]), data=st.data())
@h_settings(max_examples=200)
def test_property_normalize_hash_fixpoint(
    length: int, data: st.DataObject
) -> None:
    """``normalize_indicator("hash", x)`` is idempotent.

    The ``data.draw`` pattern lets us pick the hex length first and then
    generate a hex string of exactly that length. This is the "cleaner"
    variant the task brief suggests over ``st.randoms``.

    Validates: R16.4, R27.3.
    """
    raw = data.draw(_hex_of(length))
    once = normalize_indicator("hash", raw)
    twice = normalize_indicator("hash", once)
    assert once == twice, (
        f"fixpoint violated for hash (len={length}): "
        f"{raw!r} -> {once!r} -> {twice!r}"
    )


# ---------------------------------------------------------------------------
# Canonical-form sanity checks — one per branch
# ---------------------------------------------------------------------------


def test_ipv4_trims_whitespace() -> None:
    """IPv4 canonical form strips surrounding whitespace.

    Note: Python 3.12's ``ipaddress`` rejects zero-padded octets (per
    CVE-2021-29921 hardening), so whitespace-trimming is the primary
    visible transformation for plain IPv4 inputs.

    Validates: R16.2.
    """
    assert normalize_indicator("ipv4", "  192.168.1.1  ") == "192.168.1.1"


def test_ipv6_compresses_zeros() -> None:
    """IPv6 canonical form collapses runs of zeros into ``::``.

    Validates: R16.2.
    """
    assert (
        normalize_indicator(
            "ipv6", "2001:0db8:0000:0000:0000:0000:0000:0001"
        )
        == "2001:db8::1"
    )


def test_ipv6_lowercases_hex() -> None:
    """IPv6 canonical form lowercases hex digits.

    Validates: R16.2.
    """
    assert normalize_indicator("ipv6", "2001:DB8::1") == "2001:db8::1"


def test_domain_lowercases() -> None:
    """Domain canonical form lowercases all characters.

    Validates: R16.3.
    """
    assert normalize_indicator("domain", "Example.COM") == "example.com"


def test_domain_strips_single_trailing_dot() -> None:
    """Domain canonical form strips a single trailing dot.

    Validates: R16.3.
    """
    assert normalize_indicator("domain", "example.com.") == "example.com"


def test_domain_idna_punycode_for_unicode() -> None:
    """Domain canonical form IDNA-encodes non-ASCII input.

    ``bücher.com`` has its U-label ``bücher`` converted to the A-label
    ``xn--bcher-kva`` per RFC 3490. The TLD stays intact.

    Validates: R16.3.
    """
    result = normalize_indicator("domain", "bücher.com")
    assert result.startswith("xn--")
    assert result.endswith(".com")


def test_hostname_lowercases() -> None:
    """Hostname canonical form lowercases the label.

    Validates: R16.3.
    """
    assert normalize_indicator("hostname", "MyHost") == "myhost"


def test_hash_lowercases_xxhash64() -> None:
    """16-char hash canonical form is lowercase hex.

    Validates: R16.4.
    """
    assert (
        normalize_indicator("hash", "ABCDEF0123456789") == "abcdef0123456789"
    )


def test_hash_lowercases_sha256() -> None:
    """64-char hash canonical form is lowercase hex.

    Validates: R16.4.
    """
    assert (
        normalize_indicator(
            "hash",
            "DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF",
        )
        == "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
    )


# ---------------------------------------------------------------------------
# Error cases — invalid input surfaces ValueError for the router's 422 path
# ---------------------------------------------------------------------------


def test_empty_value_raises() -> None:
    """Empty or whitespace-only input raises ``ValueError``."""
    with pytest.raises(ValueError):
        normalize_indicator("ipv4", "")
    with pytest.raises(ValueError):
        normalize_indicator("domain", "   ")


def test_invalid_ipv4_raises() -> None:
    """Non-IPv4 strings passed with ``cls="ipv4"`` raise ``ValueError``."""
    with pytest.raises(ValueError):
        normalize_indicator("ipv4", "not-an-ip")


def test_invalid_ipv6_raises() -> None:
    """Non-IPv6 strings passed with ``cls="ipv6"`` raise ``ValueError``."""
    with pytest.raises(ValueError):
        normalize_indicator("ipv6", "not-an-ipv6")


def test_invalid_hash_length_raises() -> None:
    """Hashes of length not in ``{16, 32, 40, 64}`` raise ``ValueError``."""
    with pytest.raises(ValueError):
        normalize_indicator("hash", "abc")  # length 3
    with pytest.raises(ValueError):
        normalize_indicator("hash", "a" * 33)  # length 33


def test_invalid_hash_non_hex_raises() -> None:
    """Non-hex characters in a length-16 value raise ``ValueError``."""
    # "xyz" + 13 zeros = length 16 but contains non-hex characters.
    with pytest.raises(ValueError):
        normalize_indicator("hash", "xyz" + "0" * 13)


def test_unknown_class_raises() -> None:
    """An unknown indicator class raises ``ValueError``.

    This is the defensive fall-through for callers that bypass the
    classifier. The router always passes one of the five literal values,
    so hitting this branch means something upstream is broken.
    """
    with pytest.raises(ValueError):
        # type: ignore[arg-type] — deliberately bad input for the error path.
        normalize_indicator("unknown_class", "value")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Classifier + normalizer round-trip — tightens the fixpoint contract
# ---------------------------------------------------------------------------


def test_classifier_normalizer_roundtrip() -> None:
    """``classify_indicator(normalize_indicator(c, x)) == c`` for each class.

    The normalizer's output must land back in the same class the
    classifier assigned the raw input to. If it didn't, the router's
    cache key (``hydra:eas:lookup:{cls}:{value}``) would get the wrong
    class on a subsequent request and the fixpoint would not hold across
    request boundaries.

    Validates: R16.2, R16.3, R16.4, R27.3.
    """
    # One sample per class — enough to cover all five code paths in
    # ``classify_indicator`` without turning this into a property test
    # (the per-class fixpoint tests above already stress the normalizer).
    samples = [
        "192.168.1.1",
        "2001:db8::1",
        "example.com",
        "myhost",
        "deadbeef" * 4,  # 32 hex chars — MD5 shape
    ]
    for raw in samples:
        cls = classify_indicator(raw)
        assert cls is not None, f"classifier rejected {raw!r}"
        normalized = normalize_indicator(cls, raw)
        assert classify_indicator(normalized) == cls, (
            f"round-trip failed: {raw!r} -> {normalized!r} "
            f"(expected class {cls})"
        )
