"""Property and unit tests for :class:`TileAggregator` (task 10.6).

Covers the three Properties tied to the Maps tile aggregator:

* **Property 12 — Tile count conservation under aggregation** —
  ``sum(cell.count) == len(records)`` whenever no truncation fires
  (Design §3.5, R13.3).
* **Property 13 — Tile zoom monotonicity** — ``precision(z+1) >=
  precision(z)`` with step size ∈ ``{0, 1}``; increasing zoom never
  decreases the returned cell count (R13.2).
* **Property 14 — Tile aggregator returns only bbox-contained cells**
  — every feature centroid, which is a mean of input coordinates, is
  inside the input bbox with non-zero count.

Exercised end-to-end via the geohash strategy. The H3 strategy is not
exercised because the ``h3`` C extension is not part of the default
test environment — but :func:`zoom_to_h3_resolution` is a pure table
lookup and is covered by a table-only monotonicity test.

Validates: R13.2, R13.3, R27.5.
"""

from __future__ import annotations

import random

import pytest
from hypothesis import given, settings as h_settings, strategies as st

from hydra.eas.maps.geohash_cells import zoom_to_geohash_precision
from hydra.eas.maps.h3_cells import zoom_to_h3_resolution
from hydra.eas.maps.tile_aggregator import TileAggregator


# ---------------------------------------------------------------------------
# Property 13 — zoom → resolution/precision monotonicity (pure table lookup)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("zoom", range(18))
def test_property_geohash_precision_monotonic(zoom: int) -> None:
    """Geohash precision table is non-decreasing with unit-or-zero step.

    Validates: Requirements 13.2 (Property 13 — zoom monotonicity).
    """

    p_now = zoom_to_geohash_precision(zoom)
    p_next = zoom_to_geohash_precision(zoom + 1)
    assert p_next >= p_now
    assert p_next - p_now in {0, 1}


@pytest.mark.parametrize("zoom", range(18))
def test_property_h3_resolution_monotonic(zoom: int) -> None:
    """H3 resolution table is monotonic non-decreasing.

    ``zoom_to_h3_resolution`` is a pure table lookup — it does not
    import the ``h3`` C extension itself — so the invariant can be
    validated without the optional dependency installed.

    Note: unlike geohash (where each precision step adds 5 bits), H3
    resolutions encode ~7x cell-area changes between adjacent levels.
    The design intentionally skips some resolutions (zooms 4→5, 6→7,
    8→9 all step by 2) to avoid oversampling the zoom range. So we
    assert the core monotonicity invariant here and not the strict
    step-size-∈-{0,1} sub-clause of Property 13 that applies only to
    geohash by construction.

    Validates: Requirements 13.2 (Property 13 — zoom monotonicity).
    """

    r_now = zoom_to_h3_resolution(zoom)
    r_next = zoom_to_h3_resolution(zoom + 1)
    assert r_next >= r_now


# ---------------------------------------------------------------------------
# Property 12 — count conservation when no truncation fires
# ---------------------------------------------------------------------------


_RECORD_WITHIN_SF_BBOX = st.fixed_dictionaries(
    {
        "lat": st.floats(min_value=37.0, max_value=38.0, allow_nan=False),
        "lon": st.floats(min_value=-123.0, max_value=-122.0, allow_nan=False),
        "tier": st.integers(min_value=1, max_value=29),
    }
)


@given(
    records=st.lists(_RECORD_WITHIN_SF_BBOX, min_size=1, max_size=50),
    zoom=st.integers(min_value=0, max_value=18),
)
@h_settings(max_examples=30, deadline=None)
def test_property_count_conservation(
    records: list[dict[str, float | int]],
    zoom: int,
) -> None:
    """Sum of cell counts equals record count when no truncation fires.

    ``max_cells=10_000`` keeps ``truncated`` ``False`` for the bounded
    ``max_size=50`` list strategy — geohash at any zoom produces at
    most 50 distinct cells for 50 records.

    Validates: Requirements 13.3 (Property 12 — count conservation).
    """

    aggregator = TileAggregator(strategy="geohash")
    cells, truncated, _total = aggregator.aggregate(
        records, zoom=zoom, max_cells=10_000
    )

    assert truncated is False
    assert sum(c.count for c in cells) == len(records)


def test_truncation_flag_and_total_cells_correct() -> None:
    """Truncation reports ``len(cells) == max_cells`` and ``total > max_cells``.

    Seeds records with widely different global coordinates so that at
    zoom 18 (geohash precision 9) every record lands in its own
    cell — guaranteeing the pre-truncation cell count exceeds
    ``max_cells``.
    """

    rng = random.Random(42)
    records = [
        {
            "lat": rng.uniform(-60, 60),
            "lon": rng.uniform(-150, 150),
            "tier": 16,
        }
        for _ in range(200)
    ]
    aggregator = TileAggregator(strategy="geohash")
    cells, truncated, total = aggregator.aggregate(
        records, zoom=18, max_cells=50
    )

    assert truncated is True
    assert total > 50
    assert len(cells) == 50


# ---------------------------------------------------------------------------
# Property 13 — more zoom → more (or equal) pre-truncation cells
# ---------------------------------------------------------------------------


@given(
    records=st.lists(_RECORD_WITHIN_SF_BBOX, min_size=5, max_size=30),
    # Cap at 16 so zoom + 1 stays inside the ``[0, 18]`` table window.
    zoom=st.integers(min_value=0, max_value=16),
)
@h_settings(max_examples=20, deadline=None)
def test_property_more_zoom_more_or_equal_cells(
    records: list[dict[str, float | int]],
    zoom: int,
) -> None:
    """Increasing zoom refines the grid — cell count never decreases.

    Uses the pre-truncation ``total_cells`` (the third return of
    :meth:`TileAggregator.aggregate`) so the comparison is strictly
    over the refinement lattice, unaffected by the ``max_cells`` cap.

    Validates: Requirements 13.2 (Property 13 — zoom monotonicity).
    """

    aggregator = TileAggregator(strategy="geohash")
    _cells_z, _trunc_z, total_z = aggregator.aggregate(
        records, zoom=zoom, max_cells=10_000
    )
    _cells_z1, _trunc_z1, total_z1 = aggregator.aggregate(
        records, zoom=zoom + 1, max_cells=10_000
    )

    assert total_z1 >= total_z


# ---------------------------------------------------------------------------
# Property 14 — emitted centroids sit inside the input bbox
# ---------------------------------------------------------------------------


def test_property_centroids_inside_input_bbox() -> None:
    """Every emitted cell's centroid falls inside the input bbox.

    The aggregator computes ``centroid = mean(input_coords)`` per cell,
    so a centroid cannot escape the convex hull of the inputs (let
    alone their axis-aligned bbox). Paired with ``count > 0`` this
    confirms the aggregator never emits an empty cell.

    Validates: Requirements 27.5 (Property 14 — bbox-contained cells).
    """

    min_lat, max_lat = 37.0, 38.0
    min_lon, max_lon = -123.0, -122.0
    records = [
        {"lat": lat, "lon": lon, "tier": 16}
        for lat in (37.1, 37.5, 37.9)
        for lon in (-122.9, -122.5, -122.1)
    ]
    aggregator = TileAggregator(strategy="geohash")
    cells, _truncated, _total = aggregator.aggregate(
        records, zoom=6, max_cells=100
    )

    assert cells, "expected at least one cell for non-empty input"
    for cell in cells:
        centroid_lon, centroid_lat = cell.centroid
        assert min_lat <= centroid_lat <= max_lat
        assert min_lon <= centroid_lon <= max_lon
        assert cell.count > 0


# ---------------------------------------------------------------------------
# Supporting unit tests for the aggregator surface
# ---------------------------------------------------------------------------


def test_tier_breakdown_sums_to_count() -> None:
    """``sum(tier_breakdown.values()) == count`` and counts are exact.

    Three records at the same point collapse to a single cell; the
    breakdown mirrors the input tier histogram.
    """

    records = [
        {"lat": 37.7, "lon": -122.4, "tier": 16},
        {"lat": 37.7, "lon": -122.4, "tier": 17},
        {"lat": 37.7, "lon": -122.4, "tier": 17},
    ]
    aggregator = TileAggregator(strategy="geohash")
    cells, _truncated, _total = aggregator.aggregate(
        records, zoom=5, max_cells=10
    )

    assert len(cells) == 1
    cell = cells[0]
    assert cell.count == 3
    assert sum(cell.tier_breakdown.values()) == 3
    assert cell.tier_breakdown == {16: 1, 17: 2}


def test_dominant_tag_is_most_common() -> None:
    """``dominant_tag`` is the most common tag across all records in the cell."""

    records = [
        {"lat": 37.7, "lon": -122.4, "tier": 16, "tags": ["critical"]},
        {"lat": 37.7, "lon": -122.4, "tier": 16, "tags": ["critical"]},
        {"lat": 37.7, "lon": -122.4, "tier": 16, "tags": ["warning"]},
    ]
    aggregator = TileAggregator(strategy="geohash")
    cells, _truncated, _total = aggregator.aggregate(
        records, zoom=5, max_cells=10
    )

    assert len(cells) == 1
    assert cells[0].dominant_tag == "critical"


def test_dominant_tag_is_none_when_no_tags() -> None:
    """No ``tags`` field on inputs → ``dominant_tag`` is ``None``."""

    records = [{"lat": 37.7, "lon": -122.4, "tier": 16}]
    aggregator = TileAggregator(strategy="geohash")
    cells, _truncated, _total = aggregator.aggregate(
        records, zoom=5, max_cells=10
    )

    assert len(cells) == 1
    assert cells[0].dominant_tag is None


def test_empty_records_yields_empty_output() -> None:
    """Empty input → empty cell list, ``truncated=False``, ``total=0``."""

    aggregator = TileAggregator(strategy="geohash")
    cells, truncated, total = aggregator.aggregate(
        [], zoom=5, max_cells=10
    )

    assert cells == []
    assert truncated is False
    assert total == 0


def test_invalid_strategy_rejected() -> None:
    """Unknown strategy names fail fast with a clear ``ValueError``."""

    with pytest.raises(ValueError, match="strategy"):
        TileAggregator(strategy="unknown")


def test_cells_sorted_count_desc_then_cell_id_asc() -> None:
    """Emitted cells obey the ``(count DESC, cell_id ASC)`` order (R13.4).

    Seeds three geohash-distinct clusters with predictable counts
    (3, 2, 1) so the expected order is deterministic, then asserts
    both the count ordering and the tie-break ordering by forcing two
    clusters to share the same count.
    """

    # Three clusters separated far enough that even at zoom 3 (geohash
    # precision 2) they land in distinct cells.
    cluster_a = [{"lat": 10.0, "lon": 10.0, "tier": 16}] * 3
    cluster_b = [{"lat": -10.0, "lon": -10.0, "tier": 16}] * 2
    cluster_c = [{"lat": 40.0, "lon": 40.0, "tier": 16}] * 2

    records = cluster_a + cluster_b + cluster_c
    aggregator = TileAggregator(strategy="geohash")
    cells, _truncated, _total = aggregator.aggregate(
        records, zoom=3, max_cells=10
    )

    # Count ordering: the 3-record cluster must come first.
    assert [c.count for c in cells] == [3, 2, 2]

    # Tie-break: among the two count=2 cells, ``cell_id`` is ascending.
    tied = [c for c in cells if c.count == 2]
    assert [c.cell_id for c in tied] == sorted(c.cell_id for c in tied)
