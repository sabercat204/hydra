"""Geohash precision mapping and encoding helpers (Design §3.5).

Mirrors :mod:`hydra.eas.maps.h3_cells` for the ``geohash`` aggregation
strategy. The precision table is the ``Geohash precision`` column of
the Design §3.5 zoom table and satisfies R13.2: ``precision(z+1) >=
precision(z)`` and the step size is always 0 or 1.

Like the H3 module, ``geohash`` (package: `python-geohash`) is part
of the ``[eas]`` optional extra. The module is lazy-imported so the
zoom lookup is always available without the C extension installed.
"""

from __future__ import annotations

from typing import Any

__all__ = ["zoom_to_geohash_precision", "geohash_of"]


# Design §3.5 zoom → geohash precision table.
_GEOHASH_PRECISION_BY_ZOOM: tuple[int, ...] = (
    1,  # zoom 0
    1,  # zoom 1
    1,  # zoom 2
    2,  # zoom 3
    2,  # zoom 4
    3,  # zoom 5
    3,  # zoom 6
    4,  # zoom 7
    4,  # zoom 8
    5,  # zoom 9
    5,  # zoom 10
    6,  # zoom 11
    6,  # zoom 12
    7,  # zoom 13
    7,  # zoom 14
    8,  # zoom 15
    8,  # zoom 16
    9,  # zoom 17
    9,  # zoom 18
)


def zoom_to_geohash_precision(zoom: int) -> int:
    """Return the geohash precision for a client-supplied ``zoom``.

    Clamped to ``[0, 18]`` identically to
    :func:`hydra.eas.maps.h3_cells.zoom_to_h3_resolution` — kept
    symmetric so the :class:`TileAggregator` dispatcher doesn't have
    to special-case strategy-specific bounds.
    """

    clamped = min(max(int(zoom), 0), 18)
    return _GEOHASH_PRECISION_BY_ZOOM[clamped]


def _load_geohash() -> Any:
    """Import the ``geohash`` module on demand.

    The PyPI distribution is ``python-geohash`` but it installs as the
    ``geohash`` module. Error message points at the EAS extra since
    the dependency is not a hard requirement of the platform.
    """

    try:
        import geohash  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - exercised only w/o extra
        raise ImportError(
            "python-geohash is not installed; install the EAS extra "
            "(`pip install -e '.[eas]'`) to enable the geohash "
            "aggregation strategy"
        ) from exc
    return geohash


def geohash_of(lat: float, lon: float, precision: int) -> str:
    """Return the geohash string for ``(lat, lon)`` at ``precision``.

    Thin wrapper around ``geohash.encode`` with named ``precision``
    kwarg so the callsite stays readable.
    """

    geohash = _load_geohash()
    return geohash.encode(float(lat), float(lon), precision=int(precision))
