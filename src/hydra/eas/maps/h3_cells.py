"""H3 cell resolution mapping and encoding helpers (Design §3.5).

Two public entry points:

* :func:`zoom_to_h3_resolution` — maps a client zoom level in ``[0, 18]``
  to the H3 resolution we aggregate at. The table comes straight from
  Design §3.5 and satisfies R13.2 (monotonicity: ``resolution(z+1) >=
  resolution(z)`` and ``resolution(z+1) - resolution(z) ∈ {0, 1}``).
* :func:`h3_cell_of` — convenience wrapper around
  ``h3.latlng_to_cell`` so callers don't need to know about the import
  name or argument order.

The ``h3`` dependency ships as an optional extra (``pip install
.[eas]``) so this module lazy-imports it at first call. That keeps
``hydra.eas.maps.h3_cells`` importable in minimal deployments — e.g.
for tools that only need :func:`zoom_to_h3_resolution` — without
forcing the C extension to be present. The error message on miss is
actionable so a stack trace points operators at the correct extra.
"""

from __future__ import annotations

from typing import Any

__all__ = ["zoom_to_h3_resolution", "h3_cell_of"]


# Design §3.5 zoom → H3 resolution table. Index by zoom level; slots
# are laid out so the 18-entry table is dense (no gaps) which makes
# the lookup a simple list index rather than a dict.
_H3_RESOLUTION_BY_ZOOM: tuple[int, ...] = (
    0,   # zoom 0
    0,   # zoom 1
    0,   # zoom 2
    1,   # zoom 3
    1,   # zoom 4
    3,   # zoom 5
    3,   # zoom 6
    5,   # zoom 7
    5,   # zoom 8
    7,   # zoom 9
    7,   # zoom 10
    8,   # zoom 11
    8,   # zoom 12
    9,   # zoom 13
    9,   # zoom 14
    10,  # zoom 15
    10,  # zoom 16
    11,  # zoom 17
    11,  # zoom 18
)


def zoom_to_h3_resolution(zoom: int) -> int:
    """Return the H3 resolution for a client-supplied ``zoom`` level.

    ``zoom`` is clamped to ``[0, 18]`` via ``min(max(zoom, 0), 18)``
    before the lookup — Design §3.5 caps the supported range there and
    R13.1 defines ``0 <= zoom <= 18`` as the valid input window. Out-
    of-band values fall back to the nearest in-range resolution rather
    than raising, which keeps the function total and avoids surprising
    the tile aggregator when it sees, e.g., ``zoom = -1`` from a
    malformed client.
    """

    clamped = min(max(int(zoom), 0), 18)
    return _H3_RESOLUTION_BY_ZOOM[clamped]


def _load_h3() -> Any:
    """Import the ``h3`` module on demand.

    Separated out so the import error carries an actionable message
    pointing at the ``[eas]`` extra. ``h3`` is a C extension and not
    cheap to import, so we lazy-load rather than importing at module
    import time — callers that only need the zoom table don't pay the
    cost.
    """

    try:
        import h3  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - exercised only w/o extra
        raise ImportError(
            "h3 is not installed; install the EAS extra "
            "(`pip install -e '.[eas]'`) to enable the H3 aggregation "
            "strategy"
        ) from exc
    return h3


def h3_cell_of(lat: float, lon: float, resolution: int) -> str:
    """Return the H3 cell id containing ``(lat, lon)`` at ``resolution``.

    Thin wrapper around ``h3.latlng_to_cell``; we keep the callsite
    parameter order consistent with ``(lat, lon)`` (the h3 library's
    own convention) so callers that read this code don't have to
    cross-check against the library docs.
    """

    h3 = _load_h3()
    return h3.latlng_to_cell(float(lat), float(lon), int(resolution))
