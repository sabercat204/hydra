"""ISO 3166-1 alpha-2 country-code extraction for observatory records (R18.2).

Implements the precedence chain from Design §3.8:

1. ``record.payload["country_code"]`` when already present and length 2 —
   returned upper-cased.
2. Reverse-geocoding from ``record.geo.coordinates`` against a country
   shapefile. This path is best-effort: when the optional ``shapely`` /
   reverse-geocoding assets are missing we fall through rather than
   raising — keeping the observatory pipeline functional even in leaner
   deployments (R26.2).
3. Lookup ``record.payload["country"]`` (full or partial country name /
   alpha-3 / etc.) via :func:`pycountry.countries.lookup` which returns
   the canonical :attr:`alpha_2` code.
4. ``None`` — the caller aggregates unresolved records under
   ``unknown_region_records`` (Design §3.8).

The record parameter accepts anything with the shape ``{"payload": dict,
"geo": {"coordinates": [lon, lat]} | None}``. We read attributes
defensively so that both :class:`NormalizedRecord` instances and
lightweight test doubles work without a tight import dependency.

Import safety
-------------

``pycountry`` and the reverse-geocoding helpers are imported lazily and
guarded behind module-level availability flags. A missing dependency
never raises at import time — at runtime each resolution path becomes a
silent no-op so ``extract_country_code`` simply returns ``None`` and the
caller aggregates the record as "unknown region" (Design §3.8). This
keeps the observatory capability opt-in without forcing every deployment
to install heavy geospatial dependencies.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Optional-dependency probes
# ---------------------------------------------------------------------------

try:  # pragma: no cover - import-time probe
    import pycountry  # type: ignore[import-not-found]

    _HAVE_PYCOUNTRY = True
except ImportError:  # pragma: no cover - exercised only without pycountry
    pycountry = None  # type: ignore[assignment]
    _HAVE_PYCOUNTRY = False


try:  # pragma: no cover - import-time probe
    # ``shapely`` is imported lazily but probed here so the rest of this
    # module can bail out of the reverse-geocoding path without a deep
    # try/except around every call.
    import shapely  # type: ignore[import-not-found]  # noqa: F401

    _HAVE_SHAPELY = True
except ImportError:  # pragma: no cover
    _HAVE_SHAPELY = False


__all__ = ["extract_country_code"]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def extract_country_code(record: Any) -> str | None:
    """Return the ISO 3166-1 alpha-2 code for ``record``, or ``None``.

    Resolution precedence (Design §3.8):

    1. Direct ``payload.country_code`` — length 2, upper-cased.
    2. Reverse-geocoding on ``geo.coordinates`` (best-effort; requires
       optional ``shapely``-based assets and returns ``None`` otherwise).
    3. ``payload.country`` via :func:`pycountry.countries.lookup` (when
       ``pycountry`` is installed).
    4. ``None``.

    The function never raises on malformed input — any unexpected shape
    is treated as "no country available" so the observatory pipeline
    gracefully routes unknown records to the ``unknown_region_records``
    bucket.
    """

    if record is None:
        return None

    payload = _get_payload(record)

    # ---- Step 1: explicit payload.country_code ----------------------------
    direct = _country_from_payload_code(payload)
    if direct is not None:
        return direct

    # ---- Step 2: reverse-geocode from coordinates -----------------------
    geo_alpha2 = _country_from_geo(record)
    if geo_alpha2 is not None:
        return geo_alpha2

    # ---- Step 3: payload.country via pycountry -------------------------
    lookup_alpha2 = _country_from_payload_name(payload)
    if lookup_alpha2 is not None:
        return lookup_alpha2

    # ---- Step 4: give up ------------------------------------------------
    return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_payload(record: Any) -> dict[str, Any]:
    """Return ``record.payload`` (or ``record["payload"]``) as a dict.

    Anything non-dict-shaped degrades to an empty dict so the downstream
    lookups short-circuit cleanly.
    """

    payload: Any
    if isinstance(record, dict):
        payload = record.get("payload")
    else:
        payload = getattr(record, "payload", None)

    if isinstance(payload, dict):
        return payload
    return {}


def _country_from_payload_code(payload: dict[str, Any]) -> str | None:
    """Step 1 — direct ``payload.country_code`` lookup (no deps)."""

    value = payload.get("country_code")
    if isinstance(value, str):
        stripped = value.strip()
        if len(stripped) == 2 and stripped.isalpha():
            return stripped.upper()
    return None


def _country_from_geo(record: Any) -> str | None:
    """Step 2 — reverse-geocode from ``geo.coordinates`` (best-effort).

    The MVP does not ship a bundled country shapefile and the
    ``shapely`` dependency is optional. We keep this function as a
    well-defined hook: when the optional assets are available in a
    later release the body can plug in a real reverse-geocoder
    without a signature change. For now we return ``None`` whenever
    the path can't be completed.
    """

    if not _HAVE_SHAPELY:
        return None

    geo = _get_geo(record)
    if geo is None:
        return None

    coords = _extract_point_coords(geo)
    if coords is None:
        return None

    # Intentionally no shapefile bundled — downstream implementations
    # can hook a real reverse-geocoder here. Returning ``None`` keeps
    # the observatory pipeline functional without the extra dataset.
    _ = coords
    return None


def _get_geo(record: Any) -> Any:
    """Return ``record.geo`` (or ``record["geo"]``); ``None`` when absent."""

    if isinstance(record, dict):
        return record.get("geo")
    return getattr(record, "geo", None)


def _extract_point_coords(geo: Any) -> tuple[float, float] | None:
    """Return ``(lon, lat)`` from a GeoJSON-Point-like object, else ``None``."""

    if geo is None:
        return None

    # ``geo`` might be a pydantic model (e.g. ``GeoPoint``) or a dict.
    if isinstance(geo, dict):
        geo_type = geo.get("type")
        coords = geo.get("coordinates")
    else:
        geo_type = getattr(geo, "type", None)
        coords = getattr(geo, "coordinates", None)

    if geo_type != "Point":
        return None
    if not isinstance(coords, (list, tuple)) or len(coords) < 2:
        return None

    try:
        lon = float(coords[0])
        lat = float(coords[1])
    except (TypeError, ValueError):
        return None

    if not (-180.0 <= lon <= 180.0 and -90.0 <= lat <= 90.0):
        return None
    return lon, lat


def _country_from_payload_name(payload: dict[str, Any]) -> str | None:
    """Step 3 — resolve ``payload.country`` via :mod:`pycountry`."""

    if not _HAVE_PYCOUNTRY:
        return None

    value = payload.get("country")
    if not isinstance(value, str):
        return None

    trimmed = value.strip()
    if not trimmed:
        return None

    try:
        result = pycountry.countries.lookup(trimmed)  # type: ignore[union-attr]
    except LookupError:
        return None
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug(
            "eas.observatory.country.pycountry_lookup_failed",
            extra={"value": trimmed, "error": str(exc)},
        )
        return None

    alpha2 = getattr(result, "alpha_2", None)
    if isinstance(alpha2, str) and len(alpha2) == 2:
        return alpha2.upper()
    return None
