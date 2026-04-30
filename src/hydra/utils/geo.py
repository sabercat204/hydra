"""Geospatial utility functions."""

from typing import Optional

from hydra.models.normalized import GeoGeometry


def make_point(lon: float, lat: float, alt: Optional[float] = None) -> GeoGeometry:
    """Create a GeoJSON Point geometry."""
    coords: list[float] = [lon, lat] if alt is None else [lon, lat, alt]
    return GeoGeometry(type="Point", coordinates=coords)


def make_bbox(west: float, south: float, east: float, north: float) -> GeoGeometry:
    """Create a GeoJSON Polygon from a bounding box."""
    return GeoGeometry(
        type="Polygon",
        coordinates=[[
            [west, south],
            [east, south],
            [east, north],
            [west, north],
            [west, south],
        ]],
    )


def validate_coordinates(lon: float, lat: float) -> bool:
    """Validate longitude [-180, 180] and latitude [-90, 90]."""
    return -180.0 <= lon <= 180.0 and -90.0 <= lat <= 90.0
