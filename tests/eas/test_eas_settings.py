"""EASSettings validator property tests (task 1.6).

Validates the two non-trivial field validators on
:class:`hydra.eas.settings.EASSettings` (R25.3 / R25.4) and exercises Property
24 — *EASSettings validator correctness*.

Two properties are tested:

* ``exposure_matching_tiers`` must be a list of integers drawn from ``[1, 29]``
  with no duplicates (R25.3).
* ``maps_aggregation_strategy`` must be exactly ``"h3"`` or ``"geohash"``
  (R25.4).

Plus three happy-path examples covering the typical construction patterns.

Validates: Requirement 25.3, Requirement 25.4, Property 24.
"""

from __future__ import annotations

import pytest
from hypothesis import given, settings as h_settings, strategies as st
from pydantic import ValidationError

from hydra.eas.settings import EASSettings


# ---------------------------------------------------------------------------
# Happy-path unit tests
# ---------------------------------------------------------------------------


def test_default_settings_construct_cleanly() -> None:
    """The bare ``EASSettings()`` must build without error (R25.2)."""
    s = EASSettings()
    assert s.exposure_matching_tiers == [16, 17, 28, 29]
    assert s.maps_aggregation_strategy == "h3"


@pytest.mark.parametrize(
    "tiers",
    [
        [1],
        [29],
        [16, 17, 28, 29],
        [1, 2, 3, 29],
        list(range(1, 30)),  # full range, no duplicates
    ],
)
def test_happy_path_tiers(tiers: list[int]) -> None:
    """Common-case lists that the validator should accept."""
    s = EASSettings(exposure_matching_tiers=tiers)
    assert s.exposure_matching_tiers == tiers


@pytest.mark.parametrize("strategy", ["h3", "geohash"])
def test_happy_path_maps_strategy(strategy: str) -> None:
    """Both supported strategies construct cleanly."""
    s = EASSettings(maps_aggregation_strategy=strategy)
    assert s.maps_aggregation_strategy == strategy


# ---------------------------------------------------------------------------
# Property test — exposure_matching_tiers validator (R25.3, Property 24)
# ---------------------------------------------------------------------------


# ``max_size`` bounds the list at 30 so Hypothesis can thoroughly explore the
# accept / reject boundary without generating pathological inputs. The integer
# range ``[-100, 100]`` deliberately overshoots the accepted ``[1, 29]``
# window so the generator spends time on both in-range and out-of-range
# values.
_TIERS_STRATEGY = st.lists(
    st.integers(min_value=-100, max_value=100),
    max_size=30,
)


@given(tiers=_TIERS_STRATEGY)
@h_settings(max_examples=200)
def test_property_exposure_matching_tiers_validator(tiers: list[int]) -> None:
    """R25.3 — iff every element ∈ [1, 29] and the list has no duplicates."""

    all_in_range = all(1 <= t <= 29 for t in tiers)
    no_duplicates = len(set(tiers)) == len(tiers)
    should_accept = all_in_range and no_duplicates

    if should_accept:
        s = EASSettings(exposure_matching_tiers=tiers)
        # Validator must be order- and identity-preserving — the field is
        # stored exactly as the caller supplied it.
        assert s.exposure_matching_tiers == tiers
    else:
        with pytest.raises(ValidationError):
            EASSettings(exposure_matching_tiers=tiers)


# ---------------------------------------------------------------------------
# Property test — maps_aggregation_strategy validator (R25.4, Property 24)
# ---------------------------------------------------------------------------


# The alphabet is "printable ASCII (upper and lower alpha)" — wide enough to
# include the two valid literals ``h3`` and ``geohash`` plus a large body of
# random strings that must be rejected. We use a ``one_of`` with explicit
# ``sampled_from`` for the two valid literals to guarantee the generator
# hits the accept branch often enough.
_STRATEGY_STRATEGY = st.one_of(
    st.sampled_from(["h3", "geohash"]),
    st.text(
        alphabet=st.characters(min_codepoint=65, max_codepoint=122),
        max_size=12,
    ),
)


@given(strategy=_STRATEGY_STRATEGY)
@h_settings(max_examples=200)
def test_property_maps_aggregation_strategy_validator(strategy: str) -> None:
    """R25.4 — accept iff value ∈ {"h3", "geohash"}."""

    if strategy in {"h3", "geohash"}:
        s = EASSettings(maps_aggregation_strategy=strategy)
        assert s.maps_aggregation_strategy == strategy
    else:
        # Pydantic's ``Literal`` type and our extra field_validator both
        # reject anything outside the set; either layer raising a
        # ``ValidationError`` is acceptable here.
        with pytest.raises(ValidationError):
            EASSettings(maps_aggregation_strategy=strategy)
