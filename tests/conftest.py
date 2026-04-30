"""Shared pytest fixtures for HYDRA tests."""

from datetime import datetime, timezone
from pathlib import Path

import pytest

from hydra.models.normalized import (
    GeoGeometry,
    NormalizedRecord,
    SourceMeta,
    Tier,
)
from hydra.registry.stream_registry import StreamRegistry, load_registry
from hydra.utils.hashing import compute_raw_hash


@pytest.fixture
def sample_source_meta() -> SourceMeta:
    """Return a valid SourceMeta for testing."""
    return SourceMeta(
        source_name="USGS Earthquake Hazards",
        source_url="https://earthquake.usgs.gov/fdsnws/event/1/",
        adapter_type="rest_json",
        access_level="green",
        raw_format="GeoJSON",
        api_version="1.0",
        rate_limit_remaining=950,
    )


@pytest.fixture
def sample_normalized_record(sample_source_meta: SourceMeta) -> NormalizedRecord:
    """Return a valid NormalizedRecord with all fields populated."""
    raw = b'{"id":"us7000abc1","magnitude":5.2,"place":"10km NE of Ridgecrest, CA"}'
    return NormalizedRecord(
        stream_id="usgs_earthquake_us7000abc1",
        tier=Tier.GEOPHYSICAL_SEISMIC,
        timestamp=datetime(2024, 6, 15, 12, 30, 0, tzinfo=timezone.utc),
        geo=GeoGeometry(type="Point", coordinates=[-117.5, 35.8, 10.0]),
        payload={
            "id": "us7000abc1",
            "magnitude": 5.2,
            "place": "10km NE of Ridgecrest, CA",
            "depth_km": 10.0,
            "type": "earthquake",
        },
        source_meta=sample_source_meta,
        raw_hash=compute_raw_hash(raw),
        confidence=0.95,
        tags=["earthquake", "california", "usgs"],
    )


@pytest.fixture
def registry() -> StreamRegistry:
    """Load the stream registry from the YAML file."""
    path = Path("src/hydra/registry/stream_registry.yaml")
    return load_registry(path)
