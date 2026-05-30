"""Server-side tile aggregation for the Maps router (Design §3.5, §8.4).

:class:`TileAggregator` groups ``(lat, lon, tier, tags)`` records into
H3 or geohash cells at a zoom-derived resolution, returning one
:class:`TileCellResponse` per non-empty cell. Behaviour tracks R13:

* **R13.1** — ``zoom`` in ``[0, 18]`` selects a resolution/precision
  via the tables in :mod:`hydra.eas.maps.h3_cells` and
  :mod:`hydra.eas.maps.geohash_cells`. One feature per non-empty
  cell is emitted with ``{cell_id, centroid, count, tier_breakdown,
  dominant_tag}``.
* **R13.2** — monotonicity is inherited from the zoom tables.
* **R13.3** — count conservation: when no truncation fires,
  ``sum(cell.count) == len(records)``. Truncation is the only lossy
  transform.
* **R13.4** — when ``len(cells) > max_cells`` the aggregator sorts by
  ``(count DESC, cell_id ASC)`` and keeps the top-N. Callers receive
  ``truncated=True`` and the pre-truncation ``total_cells`` so they
  can warn users or request a higher zoom.

The aggregator is strategy-dispatching: the constructor takes a
``strategy`` string matching ``EASSettings.maps_aggregation_strategy``
(``"h3"`` or ``"geohash"``). We don't import the settings module here
so the class stays trivially unit-testable without booting the full
config tree.

Records are passed as plain dicts rather than ``NormalizedRecord``
instances so the aggregator can be driven directly from the
:class:`MapsRepository` row adapter — the SELECT emits
``{raw_hash, tier, lat, lon, tags, confidence}`` shapes.
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Literal

from hydra.eas.maps.geohash_cells import geohash_of, zoom_to_geohash_precision
from hydra.eas.maps.h3_cells import h3_cell_of, zoom_to_h3_resolution
from hydra.eas.schemas.maps import TileCellResponse

__all__ = ["TileAggregator"]


AggregationStrategy = Literal["h3", "geohash"]


class TileAggregator:
    """Group geospatial records into H3 or geohash cells.

    Parameters
    ----------
    strategy:
        ``"h3"`` or ``"geohash"``; typically wired from
        :attr:`EASSettings.maps_aggregation_strategy`. We validate in
        ``__init__`` rather than trusting the caller so that a typo in
        a settings override fails fast.
    """

    def __init__(self, strategy: str) -> None:
        if strategy not in {"h3", "geohash"}:
            raise ValueError(
                f"TileAggregator strategy must be 'h3' or 'geohash', got {strategy!r}"
            )
        self._strategy: AggregationStrategy = strategy  # type: ignore[assignment]

    # ------------------------------------------------------------------
    # Single-point dispatch
    # ------------------------------------------------------------------

    def cell_of(self, lat: float, lon: float, zoom: int) -> tuple[str, int]:
        """Return ``(cell_id, resolution_or_precision)`` for a point.

        Used by the pipeline-level tests and by callers that want a
        cell id without going through :meth:`aggregate`. The dispatch
        is done here rather than in the leaf helpers so those helpers
        stay storage-strategy-agnostic.
        """

        if self._strategy == "h3":
            res = zoom_to_h3_resolution(zoom)
            return h3_cell_of(lat, lon, res), res
        # geohash branch — strategy set was validated in ``__init__``.
        prec = zoom_to_geohash_precision(zoom)
        return geohash_of(lat, lon, prec), prec

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------

    def aggregate(
        self,
        records: list[dict[str, Any]],
        zoom: int,
        max_cells: int,
    ) -> tuple[list[TileCellResponse], bool, int]:
        """Group ``records`` into cells.

        Parameters
        ----------
        records:
            Each record MUST carry ``lat`` and ``lon`` (floats) plus
            ``tier`` (int). Optional fields: ``tags`` (list[str]).
            Other keys are ignored so callers can pass the raw row
            dict without projection.
        zoom:
            Client zoom level; clamped by the helper tables.
        max_cells:
            Upper bound on returned cells, typically
            :attr:`EASSettings.maps_tile_max_cells` (R13.4).

        Returns
        -------
        ``(cells, truncated, total_cells)`` where:

        * ``cells`` is the (possibly truncated) sorted list of
          :class:`TileCellResponse`.
        * ``truncated`` is ``True`` iff the pre-truncation cell count
          exceeded ``max_cells``.
        * ``total_cells`` is the count *before* truncation — useful
          for response metadata.
        """

        if max_cells < 1:
            # Defensive guard: settings enforce ``ge=1`` already but
            # we avoid returning negative counts if a caller passes a
            # bad override.
            max_cells = 1

        # Bucket records by cell id. We accumulate summable state in a
        # dict-of-dicts so we don't re-walk the records list to compute
        # each aggregate — keeps the aggregator O(N) over records plus
        # O(K log K) over cells for the final sort.
        buckets: dict[str, dict[str, Any]] = {}

        for rec in records:
            lat = float(rec["lat"])
            lon = float(rec["lon"])
            tier = int(rec["tier"])
            tags = rec.get("tags") or []

            cell_id, resolution = self.cell_of(lat, lon, zoom)

            bucket = buckets.get(cell_id)
            if bucket is None:
                bucket = {
                    "cell_id": cell_id,
                    "resolution": resolution,
                    "sum_lat": 0.0,
                    "sum_lon": 0.0,
                    "count": 0,
                    "tiers": Counter(),
                    "tags": Counter(),
                }
                buckets[cell_id] = bucket

            bucket["sum_lat"] += lat
            bucket["sum_lon"] += lon
            bucket["count"] += 1
            bucket["tiers"][tier] += 1
            for tag in tags:
                # ``tag`` is expected to be a string; ``Counter`` happily
                # accepts anything hashable but we cast defensively so
                # ``dominant_tag`` below returns a JSON-safe value.
                bucket["tags"][str(tag)] += 1

        total_cells = len(buckets)

        # Materialize TileCellResponse for every non-empty bucket.
        # ``centroid`` in the response schema is ordered ``(lon, lat)``
        # per GeoJSON convention — mirrors the Feature.geometry coord
        # order the Maps_Router emits below.
        cells: list[TileCellResponse] = []
        for bucket in buckets.values():
            count = int(bucket["count"])
            centroid_lon = bucket["sum_lon"] / count
            centroid_lat = bucket["sum_lat"] / count
            # ``tier_breakdown`` keys are ints (tier values); the
            # schema types them as ``dict[int, int]``.
            tier_breakdown: dict[int, int] = {
                int(k): int(v) for k, v in bucket["tiers"].items()
            }
            # Dominant tag: the most common tag in the bucket, or
            # ``None`` when no tags were seen. ``Counter.most_common(1)``
            # gives a deterministic answer for the top slot — ties go
            # to insertion order, which in turn follows the row order
            # from the PG query, which is already stable.
            if bucket["tags"]:
                dominant_tag: str | None = bucket["tags"].most_common(1)[0][0]
            else:
                dominant_tag = None

            cells.append(
                TileCellResponse(
                    cell_id=str(bucket["cell_id"]),
                    strategy=self._strategy,
                    resolution=int(bucket["resolution"]),
                    centroid=(centroid_lon, centroid_lat),
                    count=count,
                    tier_breakdown=tier_breakdown,
                    dominant_tag=dominant_tag,
                )
            )

        # R13.4 truncation. Always sort by ``(count DESC, cell_id ASC)``
        # before deciding whether to truncate — that way clients see a
        # stable, meaningful order regardless of whether they're on the
        # happy path or the truncated path.
        cells.sort(key=lambda c: (-c.count, c.cell_id))

        truncated = total_cells > max_cells
        if truncated:
            cells = cells[:max_cells]

        return cells, truncated, total_cells
