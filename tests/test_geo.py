"""Tests for geospatial utilities."""

from hydra.utils.geo import make_bbox, make_point, validate_coordinates


class TestMakePoint:
    def test_2d_point(self):
        p = make_point(-117.5, 35.8)
        assert p.type == "Point"
        assert p.coordinates == [-117.5, 35.8]

    def test_3d_point(self):
        p = make_point(-117.5, 35.8, 10.0)
        assert p.type == "Point"
        assert p.coordinates == [-117.5, 35.8, 10.0]


class TestMakeBbox:
    def test_bbox_polygon(self):
        p = make_bbox(-120.0, 35.0, -118.0, 37.0)
        assert p.type == "Polygon"
        assert len(p.coordinates[0]) == 5  # closed ring
        assert p.coordinates[0][0] == p.coordinates[0][-1]


class TestValidateCoordinates:
    def test_valid(self):
        assert validate_coordinates(0.0, 0.0) is True
        assert validate_coordinates(-180.0, -90.0) is True
        assert validate_coordinates(180.0, 90.0) is True

    def test_invalid_lon(self):
        assert validate_coordinates(-181.0, 0.0) is False
        assert validate_coordinates(181.0, 0.0) is False

    def test_invalid_lat(self):
        assert validate_coordinates(0.0, -91.0) is False
        assert validate_coordinates(0.0, 91.0) is False
