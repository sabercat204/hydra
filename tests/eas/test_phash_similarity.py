"""Property test for perceptual-hash similarity (task 8.3).

Exercises :func:`hydra.eas.screenshots.phash.hamming_similarity` against
Property 11 from the EAS design doc.

Property 11 — Perceptual-hash similarity well-formedness:
    For any two 64-bit perceptual hashes ``a`` and ``b``:

    1. ``hamming_similarity(a, b) ∈ [0.0, 1.0]`` (range bounds).
    2. ``hamming_similarity(a, b) == hamming_similarity(b, a)`` (symmetry).
    3. ``hamming_similarity(a, a) == 1.0`` (identity).
    4. For the two extreme points ``"0" * 16`` and ``"f" * 16`` the bits
       differ in every position so similarity is ``0.0``.
    5. For any base hash and two distances ``d1 < d2``, the candidates
       passing the tighter threshold ``1 - d1/64`` are a subset of those
       passing the looser threshold ``1 - d2/64`` (threshold subset
       monotonicity, which is the scalar equivalent of the `/images/search`
       query behaviour).
    6. Malformed inputs (non-hex, uppercase, wrong length) raise
       ``ValueError`` at the validator boundary — the implementation uses
       a ``^[0-9a-f]{16}$`` regex.

The module under test only depends on Python 3.12's ``int.bit_count`` so
``imagehash`` is not required to exercise these invariants.

Validates: R8.1, R27.4.
"""

from __future__ import annotations

import pytest
from hypothesis import given, settings as h_settings, strategies as st

from hydra.eas.screenshots.phash import hamming_similarity


# ---------------------------------------------------------------------------
# Shared hypothesis strategy — 16-character lowercase hex strings
# ---------------------------------------------------------------------------

# ``int.to_bytes``-style formatting keeps the strategy deterministic: every
# integer in ``[0, 2**64)`` lands on exactly one 16-char hex string, and the
# mapping is a bijection onto the set of valid ``hamming_similarity``
# inputs. Using ``format`` over ``hex()`` drops the ``0x`` prefix and adds
# leading zeros so short values like ``0`` still satisfy the regex.
hex16 = st.integers(min_value=0, max_value=2**64 - 1).map(
    lambda n: format(n, "016x")
)


# ---------------------------------------------------------------------------
# Invariants 1-3: range bounds + symmetry + identity over random pairs
# ---------------------------------------------------------------------------


@given(a=hex16, b=hex16)
@h_settings(max_examples=300)
def test_property_range_and_symmetry(a: str, b: str) -> None:
    """Similarity is in ``[0.0, 1.0]`` and symmetric in its arguments.

    Covers Property 11 invariants 1 and 2 in one pass — they share a
    generator and evaluating ``hamming_similarity`` twice on the same
    inputs is cheap.

    Validates: R8.1, R27.4.
    """
    forward = hamming_similarity(a, b)
    reverse = hamming_similarity(b, a)

    # Invariant 1 — range bounds.
    assert 0.0 <= forward <= 1.0, f"out-of-range result {forward} for ({a}, {b})"
    # Invariant 2 — symmetry.
    assert forward == reverse, (
        f"asymmetry: hamming_similarity({a}, {b}) = {forward}, "
        f"hamming_similarity({b}, {a}) = {reverse}"
    )


@given(a=hex16)
@h_settings(max_examples=200)
def test_property_identity(a: str) -> None:
    """``hamming_similarity(a, a) == 1.0`` for every valid hash.

    ``a ^ a == 0`` so popcount is zero and the quotient collapses to 1.0.

    Validates: R8.1, R27.4.
    """
    assert hamming_similarity(a, a) == 1.0


# ---------------------------------------------------------------------------
# Invariant 4: orthogonal extremes map to similarity 0.0
# ---------------------------------------------------------------------------


def test_orthogonal_hashes_have_zero_similarity() -> None:
    """All-zero and all-``f`` hashes differ in every bit → similarity 0.0.

    This is the only concrete point at the bottom of the range; every
    other pair has at least one matching bit by pigeonhole. Including it
    as a plain assertion (rather than a property) catches regressions in
    the popcount / ``bit_count`` path that random hypothesis draws could
    miss.

    Validates: R8.1, R27.4.
    """
    assert hamming_similarity("0" * 16, "f" * 16) == 0.0
    # Symmetric counterpart — belt-and-braces given the implementation.
    assert hamming_similarity("f" * 16, "0" * 16) == 0.0


# ---------------------------------------------------------------------------
# Invariant 5: threshold subset monotonicity
# ---------------------------------------------------------------------------


@given(
    base=hex16,
    candidates=st.lists(hex16, min_size=1, max_size=20, unique=True),
    d_pair=st.tuples(
        st.integers(min_value=0, max_value=64),
        st.integers(min_value=0, max_value=64),
    ),
)
@h_settings(max_examples=150)
def test_property_threshold_subset_monotonicity(
    base: str,
    candidates: list[str],
    d_pair: tuple[int, int],
) -> None:
    """Tighter thresholds yield a subset of the looser threshold's hits.

    The `/api/v1/images/search?similarity=...` endpoint filters on
    ``hamming_similarity(query, candidate) >= threshold``. For two
    thresholds ``t1 <= t2`` (equivalently ``d1 >= d2`` in distance terms),
    the result set at ``t2`` must be a subset of the result set at ``t1``
    over the same backing data. We express the check in distance terms
    because the integer grid avoids floating-point ambiguity at the
    boundary.

    Strategy: pick ``d_tight <= d_loose`` so
    ``threshold_tight = 1 - d_tight/64`` is **greater than or equal to**
    ``threshold_loose = 1 - d_loose/64``. The tighter-threshold hits are
    the subset.

    Validates: R8.1, R27.4.
    """
    d1, d2 = d_pair
    d_tight = min(d1, d2)
    d_loose = max(d1, d2)

    threshold_tight = 1.0 - (d_tight / 64.0)
    threshold_loose = 1.0 - (d_loose / 64.0)

    tight_hits = {
        c for c in candidates if hamming_similarity(base, c) >= threshold_tight
    }
    loose_hits = {
        c for c in candidates if hamming_similarity(base, c) >= threshold_loose
    }

    assert tight_hits.issubset(loose_hits), (
        f"monotonicity violated: candidates passing threshold "
        f"{threshold_tight} are not a subset of those passing "
        f"{threshold_loose}; tight={tight_hits - loose_hits}"
    )


# ---------------------------------------------------------------------------
# Invariant 6: malformed inputs raise ValueError at the validator
# ---------------------------------------------------------------------------


def test_rejects_non_hex_input() -> None:
    """Non-hex characters trip the ``^[0-9a-f]{16}$`` regex."""
    with pytest.raises(ValueError):
        hamming_similarity("xyz", "0" * 16)


def test_rejects_uppercase_hex_input() -> None:
    """Uppercase hex is rejected — the regex is lowercase-only on purpose.

    The canonical output of :func:`hydra.eas.screenshots.phash.compute_phash`
    is lowercase, so accepting uppercase would let a mis-cased input slip
    through the bounds check.
    """
    with pytest.raises(ValueError):
        hamming_similarity("F" * 16, "0" * 16)


def test_rejects_wrong_length_input() -> None:
    """A 15-char input is not a 16-char hex string."""
    with pytest.raises(ValueError):
        hamming_similarity("0" * 15, "0" * 16)
