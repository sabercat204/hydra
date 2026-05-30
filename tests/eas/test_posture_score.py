"""Posture-score determinism + bounds property tests (task 14.8).

Exercises the pure helpers in :mod:`hydra.eas.observatory.posture`
against Property 22 from Design §3.8 and the acceptance criteria on
R18.3 / R18.4:

* **Bounds** — every score lies in ``[0, 100]`` regardless of input
  magnitude, because the per-component normalizers clip to ``[0, 1]``
  and the weights sum to ``1.0`` (the outer ``max/min`` clip is an
  additional belt-and-braces guard against float drift).
* **Determinism** — ``posture_score(inputs, weights)`` is a pure
  function; two invocations on bit-identical inputs return the same
  ``float`` (no hidden state, no RNG).
* **Delta bounds** — ``trend_deltas`` clips ``absolute_delta`` into
  ``[-100, 100]``, matching the natural range of a difference of two
  ``[0, 100]`` scores.
* **Weight sum** — :func:`validate_weights` rejects tuples that do not
  sum to ``1.0 ± 1e-6``. (Property 24 already covers the happy path
  via the pydantic ``model_validator`` at construction time; this file
  covers the path where a caller bypasses pydantic with
  ``model_construct`` and so avoids the constructor guard.)

Validates: Requirement 18.3, Requirement 18.4, Property 22.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from hypothesis import given, settings as h_settings, strategies as st

from hydra.eas.observatory.posture import (
    posture_score,
    trend_deltas,
    validate_weights,
)
from hydra.eas.settings import PostureScoreWeights


def _default_weights() -> PostureScoreWeights:
    """Default weights sum to 1.0 by construction (see `PostureScoreWeights`)."""
    return PostureScoreWeights()


# ---------------------------------------------------------------------------
# Property 22 — score bounds
# ---------------------------------------------------------------------------
#
# The five float strategies below intentionally extend well past the
# per-component clip thresholds (``/50``, ``/200``, ``/5``, ``/1000``)
# so the generator spends time on both the pre-clip linear region and
# the post-clip saturated region. ``stale_patch_ratio`` is bounded at
# ``1`` because the caller pre-computes it as a ratio; we don't need
# to feed pathological > 1 values to exercise the clip here.


@given(
    kev=st.floats(min_value=0, max_value=1e6, allow_nan=False),
    crit=st.floats(min_value=0, max_value=1e6, allow_nan=False),
    vuln=st.floats(min_value=0, max_value=1e3, allow_nan=False),
    stale=st.floats(min_value=0, max_value=1, allow_nan=False),
    hosts=st.floats(min_value=0, max_value=1e6, allow_nan=False),
)
@h_settings(max_examples=200)
def test_property_posture_score_bounds(
    kev: float, crit: float, vuln: float, stale: float, hosts: float
) -> None:
    """Validates: Requirement 18.3, Property 22.

    Every non-negative finite input tuple produces a score in
    ``[0, 100]`` — holds trivially because each component term is
    clipped to ``[0, 1]`` and the weights sum to 1.
    """

    inputs = {
        "kev_count": kev,
        "critical_count": crit,
        "vuln_cves_per_asset": vuln,
        "stale_patch_ratio": stale,
        "distinct_exposed_hosts": hosts,
    }
    score = posture_score(inputs, _default_weights())
    assert 0.0 <= score <= 100.0


# ---------------------------------------------------------------------------
# Property 22 — determinism
# ---------------------------------------------------------------------------


@given(
    kev=st.floats(min_value=0, max_value=1e6, allow_nan=False),
    crit=st.floats(min_value=0, max_value=1e6, allow_nan=False),
    vuln=st.floats(min_value=0, max_value=1e3, allow_nan=False),
    stale=st.floats(min_value=0, max_value=1, allow_nan=False),
    hosts=st.floats(min_value=0, max_value=1e6, allow_nan=False),
)
@h_settings(max_examples=200)
def test_property_posture_score_deterministic(
    kev: float, crit: float, vuln: float, stale: float, hosts: float
) -> None:
    """Validates: Requirement 18.3, Property 22.

    ``posture_score`` is pure — identical inputs produce bit-identical
    outputs across invocations. Uses strict ``==`` (not ``math.isclose``)
    because the function is deterministic, not merely reproducible.
    """

    inputs = {
        "kev_count": kev,
        "critical_count": crit,
        "vuln_cves_per_asset": vuln,
        "stale_patch_ratio": stale,
        "distinct_exposed_hosts": hosts,
    }
    weights = _default_weights()
    a = posture_score(inputs, weights)
    b = posture_score(inputs, weights)
    assert a == b


# ---------------------------------------------------------------------------
# Property 22 — absolute delta bounds
# ---------------------------------------------------------------------------


@given(
    current=st.floats(min_value=0, max_value=100, allow_nan=False),
    prior=st.floats(min_value=0, max_value=100, allow_nan=False),
)
@h_settings(max_examples=200)
def test_property_absolute_delta_bounds(current: float, prior: float) -> None:
    """Validates: Requirement 18.4, Property 22.

    For any two in-range posture scores, the absolute delta stays in
    ``[-100, 100]``. The clip inside ``trend_deltas`` is defensive —
    the natural range of ``current - prior`` with both in ``[0, 100]``
    is already exactly ``[-100, 100]``.
    """

    deltas = trend_deltas(current, prior)
    assert -100.0 <= deltas["absolute_delta"] <= 100.0


# ---------------------------------------------------------------------------
# Weight-sum validation
# ---------------------------------------------------------------------------


def test_validate_weights_accepts_valid() -> None:
    """Default weights sum to 1.0 and pass the explicit re-check."""
    validate_weights(_default_weights())


def test_validate_weights_rejects_mutated() -> None:
    """Bypassing pydantic's constructor must not bypass ``validate_weights``.

    ``PostureScoreWeights`` enforces the sum at construction time, so
    normal call paths never reach this branch. The danger comes from
    ``model_construct`` (used by deserializers, fixtures, and ad-hoc
    test harnesses) which skips validators entirely. The function under
    test is the last line of defence for Property 22 in that path.
    """

    bad_weights = PostureScoreWeights.model_construct(
        w_kev=0.5,
        w_crit=0.5,
        w_vuln_density=0.5,
        w_stale=0.5,
        w_asset_surface=0.5,
    )
    with pytest.raises(ValueError, match="sum to 1.0"):
        validate_weights(bad_weights)


# ---------------------------------------------------------------------------
# Canonical-value sanity checks
# ---------------------------------------------------------------------------


def test_posture_score_zero_inputs_is_zero() -> None:
    """All-zero inputs hit the floor of the ``[0, 100]`` range."""
    inputs = {
        "kev_count": 0,
        "critical_count": 0,
        "vuln_cves_per_asset": 0,
        "stale_patch_ratio": 0,
        "distinct_exposed_hosts": 0,
    }
    assert posture_score(inputs, _default_weights()) == 0.0


def test_posture_score_saturated_inputs_is_100() -> None:
    """Every component maxed out past its clip threshold saturates at 100.

    Design §3.8 defines the per-component clip thresholds (50, 200, 5,
    1.0, 1000); feeding 1e6 to each overshoots them all, so every
    term clips to 1.0 and the weighted sum is 1.0 × 100 = 100.
    """

    inputs = {
        "kev_count": 1e6,
        "critical_count": 1e6,
        "vuln_cves_per_asset": 1e6,
        "stale_patch_ratio": 1.0,
        "distinct_exposed_hosts": 1e6,
    }
    assert posture_score(inputs, _default_weights()) == 100.0


def test_trend_deltas_zero_prior_guards_division() -> None:
    """A zero prior score must not blow up — the ``max(0.01, prior)`` guard kicks in.

    The expected percent delta is ``100 * 50 / 0.01 = 500000.0``. The
    absurd magnitude is intentional; callers are expected to interpret
    "prior ≈ 0" as "no meaningful baseline" and suppress the percent
    delta in the UI layer, not in this pure helper.
    """

    deltas = trend_deltas(current_score=50.0, prior_score=0.0)
    assert deltas["absolute_delta"] == 50.0
    assert deltas["percent_delta"] == 500000.0


def test_trend_deltas_computation() -> None:
    """Straightforward positive delta: 60 from 50 is +10 absolute, +20%."""
    deltas = trend_deltas(current_score=60.0, prior_score=50.0)
    assert deltas["absolute_delta"] == 10.0
    assert abs(deltas["percent_delta"] - 20.0) < 1e-9


def test_trend_deltas_negative_delta() -> None:
    """Regression direction: 30 from 50 is -20 absolute, -40%."""
    deltas = trend_deltas(current_score=30.0, prior_score=50.0)
    assert deltas["absolute_delta"] == -20.0
    assert abs(deltas["percent_delta"] - (-40.0)) < 1e-9


# ---------------------------------------------------------------------------
# Graceful handling of missing / invalid inputs
# ---------------------------------------------------------------------------
#
# ``posture_score`` is fed from a PG row dict. That row may be missing
# keys (no aggregate join hits), carry NULLs (no data for a country in
# window), or come back as ``Decimal`` (asyncpg's default numeric
# type). All three paths should collapse cleanly to zero or to a
# float-valued score, never raise ``KeyError``/``TypeError``.


def test_posture_score_handles_missing_keys() -> None:
    """Empty inputs dict — every field defaults to 0 via ``_float_or``."""
    assert posture_score({}, _default_weights()) == 0.0


def test_posture_score_handles_none_values() -> None:
    """Explicit ``None`` values — same zero-default path as missing keys."""
    inputs = {
        "kev_count": None,
        "critical_count": None,
        "vuln_cves_per_asset": None,
        "stale_patch_ratio": None,
        "distinct_exposed_hosts": None,
    }
    assert posture_score(inputs, _default_weights()) == 0.0


def test_posture_score_handles_decimal() -> None:
    """asyncpg returns ``Decimal`` for numeric columns — ``float()`` must coerce."""
    inputs = {
        "kev_count": Decimal("10"),
        "critical_count": Decimal("5"),
        "vuln_cves_per_asset": Decimal("2.5"),
        "stale_patch_ratio": Decimal("0.3"),
        "distinct_exposed_hosts": Decimal("500"),
    }
    score = posture_score(inputs, _default_weights())
    assert 0.0 <= score <= 100.0
