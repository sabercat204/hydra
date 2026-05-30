"""Exposure-observatory posture score and trend-delta helpers (R18.3, R18.4).

This module translates the formulas documented in Design §3.8 into pure
Python:

* :func:`posture_score` — weighted linear combination of KEV exposures,
  critical exposures, vulnerability density, stale-patch ratio, and
  exposed asset surface, clipped to ``[0, 100]``. Satisfies R18.3 and
  Property 22's well-formedness + determinism guarantees (same inputs
  → same output).
* :func:`validate_weights` — explicit check that the caller's weight
  tuple sums to ``1.0 ± 1e-6``. :class:`hydra.eas.settings.PostureScoreWeights`
  already enforces this at settings-load time; we re-export the check
  here so callers that build a :class:`PostureScoreWeights` manually can
  validate without reaching into pydantic internals.
* :func:`trend_deltas` — the absolute + percent delta between a
  country's current and prior posture score (R18.4). Both fields are
  clipped / guarded so ``0`` and near-``0`` priors don't blow up the
  division.

Everything here is framework-free: no I/O, no async. The observatory
generator layer handles persistence; this module stays pure so it can
be reused from tests and from the hypothesis-driven PBTs in task 14.8.
"""

from __future__ import annotations

from typing import Any, Mapping

from hydra.eas.settings import PostureScoreWeights

__all__ = [
    "posture_score",
    "validate_weights",
    "trend_deltas",
]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _clip(value: float, lo: float, hi: float) -> float:
    """Clip ``value`` into ``[lo, hi]`` without importing numpy."""

    if value < lo:
        return lo
    if value > hi:
        return hi
    return value


def _float_or(value: Any, default: float) -> float:
    """Best-effort ``float(value)`` that falls back to ``default``.

    The caller may hand in raw PG rows whose numeric columns come back
    as :class:`Decimal` — feeding those through :func:`float` matches
    the behaviour the design assumes. ``None`` / non-numeric values
    collapse to ``default`` so the formula stays well-defined even when
    a join column was NULL.
    """

    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Validation (explicit re-check for callers that build weights manually)
# ---------------------------------------------------------------------------


def validate_weights(weights: PostureScoreWeights) -> None:
    """Assert the five posture-score weights sum to ``1.0 ± 1e-6``.

    :class:`PostureScoreWeights`'s own ``model_validator`` already raises
    at construction time, so this function is effectively a no-op for
    correctly-built instances. It exists so that ad-hoc callers can
    pass an already-bound object through an extra gate — for example,
    a test that patches a weight field directly would bypass pydantic
    and could silently violate Property 22. Re-running the check here
    makes the violation loud.
    """

    total = (
        weights.w_kev
        + weights.w_crit
        + weights.w_vuln_density
        + weights.w_stale
        + weights.w_asset_surface
    )
    if abs(total - 1.0) > 1e-6:
        raise ValueError(
            f"posture_score_weights must sum to 1.0 ± 1e-6 (got {total!r})"
        )


# ---------------------------------------------------------------------------
# Posture score (Design §3.8 / R18.3 / Property 22)
# ---------------------------------------------------------------------------


def posture_score(
    inputs: Mapping[str, Any],
    weights: PostureScoreWeights,
) -> float:
    """Return a country's posture score — ``[0, 100]``, higher = worse.

    Expected keys on ``inputs`` (all numeric; missing keys default to 0):

    * ``kev_count`` — number of KEV-listed exposures in window.
    * ``critical_count`` — number of exposures with severity ``critical``.
    * ``vuln_cves_per_asset`` — ``total_cves / max(1, distinct_exposed_hosts)``
      pre-computed by the caller (Design §3.8 table).
    * ``stale_patch_ratio`` — ``cves_over_30_days_old / max(1, total_cves)``;
      already a ratio in ``[0, 1]`` when the caller did their job, but
      we clip regardless so a misbehaving caller can't push the score
      out of range.
    * ``distinct_exposed_hosts`` — host count for the asset-surface
      normalization.

    Formula (verbatim from Design §3.8):

    .. code-block:: text

        score_raw = (w_kev * clip(kev_count/50, 0, 1)
                   + w_crit * clip(critical_count/200, 0, 1)
                   + w_vuln_density * clip(vuln_cves_per_asset/5, 0, 1)
                   + w_stale * stale_patch_ratio
                   + w_asset_surface * clip(distinct_exposed_hosts/1000, 0, 1))
        score = max(0.0, min(100.0, 100.0 * score_raw))

    The weights must already satisfy :func:`validate_weights`; in
    normal call paths they come from
    ``EASSettings.posture_score_weights`` which validates at load time
    (Property 24 / Property 22).
    """

    # Defensive re-validation — cheap, and it keeps Property 22 honest
    # for callers that build a ``PostureScoreWeights`` with mutated
    # fields after construction.
    validate_weights(weights)

    kev_count = _float_or(inputs.get("kev_count"), 0.0)
    critical_count = _float_or(inputs.get("critical_count"), 0.0)
    vuln_density = _float_or(inputs.get("vuln_cves_per_asset"), 0.0)
    stale_ratio = _float_or(inputs.get("stale_patch_ratio"), 0.0)
    distinct_hosts = _float_or(inputs.get("distinct_exposed_hosts"), 0.0)

    score_raw = (
        weights.w_kev * _clip(kev_count / 50.0, 0.0, 1.0)
        + weights.w_crit * _clip(critical_count / 200.0, 0.0, 1.0)
        + weights.w_vuln_density * _clip(vuln_density / 5.0, 0.0, 1.0)
        + weights.w_stale * _clip(stale_ratio, 0.0, 1.0)
        + weights.w_asset_surface * _clip(distinct_hosts / 1000.0, 0.0, 1.0)
    )

    return _clip(100.0 * score_raw, 0.0, 100.0)


# ---------------------------------------------------------------------------
# Trend deltas (R18.4 / Property 22)
# ---------------------------------------------------------------------------


def trend_deltas(current_score: float, prior_score: float) -> dict[str, float]:
    """Return ``{"absolute_delta", "percent_delta"}`` for a country.

    Per R18.4:

    * ``absolute_delta = current_score - prior_score``, clipped to
      ``[-100.0, 100.0]``. The clip is defensive — both scores are
      already in ``[0, 100]`` by construction, so the natural range is
      already ``[-100, 100]``, but clipping guards against callers
      feeding in slightly out-of-range values from rounding or fixture
      noise.
    * ``percent_delta = 100.0 * absolute_delta / max(0.01, prior_score)``
      — the ``max(0.01, prior)`` guard avoids division by zero when a
      country had no prior-day report (Design §3.8).
    """

    current = _float_or(current_score, 0.0)
    prior = _float_or(prior_score, 0.0)

    absolute = _clip(current - prior, -100.0, 100.0)
    denom = max(0.01, prior)
    percent = 100.0 * absolute / denom

    return {
        "absolute_delta": absolute,
        "percent_delta": percent,
    }
