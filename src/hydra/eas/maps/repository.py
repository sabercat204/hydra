"""PostGIS bbox query helper for the Maps router (Design §6.4).

:class:`MapsRepository` is a thin static/stateless wrapper around a
single parameterized SELECT against ``normalized_records``. It lives
separate from :class:`hydra.eas.assets.repository.AssetRepository` so
the maps hot path doesn't import asset-specific types; the two
repositories speak the same duck-typed pool interface
(``pool.acquire() -> conn`` with ``fetch``/``fetchval``).

The query uses PostGIS:

.. code-block:: sql

    SELECT raw_hash, tier,
           ST_Y(geo::geometry) AS lat,
           ST_X(geo::geometry) AS lon,
           tags, confidence
      FROM normalized_records
     WHERE geo IS NOT NULL
       AND ST_Intersects(geo, ST_MakeEnvelope($1, $2, $3, $4, 4326))
       [AND ...dynamic filters...]
     LIMIT $N

Dynamic filters (all optional): ``tier``, ``time_start``,
``time_end``, ``min_confidence``, ``tag``. The WHERE builder mirrors
:meth:`AssetRepository.list_active` — conditions and parameters are
accumulated into parallel lists so the final SQL string stays
parameter-safe (no string interpolation of values).

Per Design §6.4 the repository caps ``LIMIT`` at
``100 * maps_tile_max_cells`` to bound fetch cost. The router passes
``EASSettings.maps_tile_max_cells`` in; we enforce the cap here
rather than at the router so a test that forgets to hand a cap
still can't exhaust the DB.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

__all__ = ["MapsRepository", "MapsFilters"]


# Hard cap multiplier from Design §6.4 — ``LIMIT 100 * maps_tile_max_cells``.
_LIMIT_CAP_MULTIPLIER = 100


class MapsFilters:
    """Optional filter parameters for :meth:`MapsRepository.query_bbox`.

    Kept as a plain class (rather than a Pydantic model) so the router
    can populate it from Query() params directly without an extra
    validation pass — Pydantic already ran at the router boundary.
    """

    __slots__ = ("tier", "time_start", "time_end", "min_confidence", "tag")

    def __init__(
        self,
        *,
        tier: int | None = None,
        time_start: datetime | None = None,
        time_end: datetime | None = None,
        min_confidence: float | None = None,
        tag: str | None = None,
    ) -> None:
        self.tier = tier
        self.time_start = time_start
        self.time_end = time_end
        self.min_confidence = min_confidence
        self.tag = tag


class MapsRepository:
    """PostGIS bbox queries over ``normalized_records``.

    Methods are classmethods / staticmethods — there's no instance
    state to carry, and that keeps the dependency graph in
    :mod:`hydra.eas.dependencies` minimal (no factory needed).
    """

    @classmethod
    async def query_bbox(
        cls,
        pg_pool: Any,
        bbox: tuple[float, float, float, float],
        filters: MapsFilters | None,
        limit: int,
        *,
        maps_tile_max_cells: int | None = None,
    ) -> list[dict[str, Any]]:
        """Run the bbox SELECT and return row dicts.

        Parameters
        ----------
        pg_pool:
            Anything that implements ``async with pool.acquire() as conn``
            with ``conn.fetch``. asyncpg pools satisfy this directly;
            test doubles can provide the same shape.
        bbox:
            ``(min_lon, min_lat, max_lon, max_lat)``. Passed
            unchanged into ``ST_MakeEnvelope``; SRID 4326 is pinned
            since the ``geo`` column is defined as
            ``geometry(Geometry, 4326)``.
        filters:
            Optional :class:`MapsFilters` struct. ``None`` means "no
            extra filtering".
        limit:
            Fetch ceiling; capped at
            ``100 * maps_tile_max_cells`` when ``maps_tile_max_cells``
            is supplied (Design §6.4). When ``maps_tile_max_cells`` is
            ``None`` the caller is trusted to have pre-capped.

        Returns
        -------
        List of dicts with keys ``raw_hash``, ``tier``, ``lat``,
        ``lon``, ``tags``, ``confidence``. The aggregator downstream
        consumes this exact shape, so changing it requires a matching
        change in :class:`hydra.eas.maps.tile_aggregator.TileAggregator`.
        """

        # Apply the Design §6.4 cap. We keep ``limit`` clamped *before*
        # building the SQL so the generated query can't accidentally
        # leak a higher LIMIT.
        if maps_tile_max_cells is not None:
            ceiling = _LIMIT_CAP_MULTIPLIER * int(maps_tile_max_cells)
            limit = min(int(limit), ceiling)
        else:
            limit = int(limit)

        if limit < 1:
            # Pathological callers — don't emit SQL with LIMIT 0 (which
            # would return nothing but still burn a round trip). Just
            # short-circuit.
            return []

        # Build the WHERE clause and parameter list in lockstep. $1..$4
        # are the bbox corners; filters start at $5.
        min_lon, min_lat, max_lon, max_lat = bbox
        params: list[Any] = [
            float(min_lon),
            float(min_lat),
            float(max_lon),
            float(max_lat),
        ]
        conditions: list[str] = [
            "geo IS NOT NULL",
            "ST_Intersects(geo, ST_MakeEnvelope($1, $2, $3, $4, 4326))",
        ]

        if filters is not None:
            if filters.tier is not None:
                params.append(int(filters.tier))
                conditions.append(f"tier = ${len(params)}")
            if filters.time_start is not None:
                params.append(filters.time_start)
                conditions.append(f"timestamp >= ${len(params)}")
            if filters.time_end is not None:
                params.append(filters.time_end)
                conditions.append(f"timestamp <= ${len(params)}")
            if filters.min_confidence is not None:
                params.append(float(filters.min_confidence))
                conditions.append(f"confidence >= ${len(params)}")
            if filters.tag is not None:
                # ``tags`` is a TEXT[] — Postgres' ``= ANY()`` flips
                # the operand order so the GIN index on ``tags`` can
                # still help.
                params.append(str(filters.tag))
                conditions.append(f"${len(params)} = ANY(tags)")

        sql = f"""
            SELECT raw_hash,
                   tier,
                   ST_Y(geo::geometry) AS lat,
                   ST_X(geo::geometry) AS lon,
                   tags,
                   confidence
              FROM normalized_records
             WHERE {' AND '.join(conditions)}
             LIMIT {limit}
        """

        async with pg_pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)

        # Adapt asyncpg ``Record`` (or dict-like test doubles) to plain
        # dicts. We don't rely on ``Record`` indexing here because the
        # downstream aggregator accepts ``Mapping`` but our own type
        # hints (``list[dict]``) promise dicts.
        return [
            {
                "raw_hash": row["raw_hash"],
                "tier": int(row["tier"]),
                "lat": float(row["lat"]) if row["lat"] is not None else None,
                "lon": float(row["lon"]) if row["lon"] is not None else None,
                "tags": list(row["tags"] or []),
                "confidence": float(row["confidence"])
                if row["confidence"] is not None
                else None,
            }
            for row in rows
        ]
