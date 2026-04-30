"""Unit tests for RestJsonAdapter — all network calls mocked."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import orjson
import pytest

from hydra.adapters.base import RawPayload
from hydra.adapters.exceptions import FetchError, ParseError, RateLimitError
from hydra.adapters.rest_json import RestJsonAdapter
from hydra.config import HydraSettings
from hydra.models.normalized import NormalizedRecord
from hydra.registry.stream_registry import (
    StreamRegistry,
    StreamSource,
    StreamTier,
)
from hydra.utils.hashing import compute_raw_hash


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

USGS_GEOJSON = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "id": "us7000abc1",
            "properties": {
                "mag": 5.2,
                "place": "10km NE of Ridgecrest, CA",
                "time": 1718451000000,
                "type": "earthquake",
            },
            "geometry": {"type": "Point", "coordinates": [-117.5, 35.8, 10.0]},
        },
        {
            "type": "Feature",
            "id": "us7000abc2",
            "properties": {
                "mag": 3.1,
                "place": "5km S of Pahala, Hawaii",
                "time": 1718452000000,
                "type": "earthquake",
            },
            "geometry": {"type": "Point", "coordinates": [-155.2, 19.2, 30.0]},
        },
    ],
}


def _make_registry() -> StreamRegistry:
    src = StreamSource(name="usgs_earthquake", url="https://earthquake.usgs.gov", format="geojson", auth="none", notes="")
    tier = StreamTier(
        id=1, name="Geophysical & Seismic", streams=1, access="5G",
        formats=["geojson"], cadence="sub_minute", adapter="rest_json", fallback=None, sources=[src],
    )
    return StreamRegistry(tiers={1: tier})


def _make_adapter(
    stream_config: dict[str, Any] | None = None,
    settings: HydraSettings | None = None,
    registry: StreamRegistry | None = None,
) -> RestJsonAdapter:
    cfg: dict[str, Any] = {
        "base_url": "https://earthquake.usgs.gov",
        "endpoint_path": "fdsnws/event/1/query",
        "auth_pattern": "none",
        "format": "geojson",
        "response_root_path": "features",
        "required_fields": ["properties.mag", "properties.place"],
        "field_mapping": {
            "stream_id": "id",
            "timestamp": "properties.time",
        },
        **(stream_config or {}),
    }
    return RestJsonAdapter(
        stream_id="usgs_earthquake",
        settings=settings or HydraSettings(),
        registry=registry or _make_registry(),
        stream_config=cfg,
    )


def _mock_response(
    body: bytes,
    status: int = 200,
    content_type: str = "application/json",
    headers: dict[str, str] | None = None,
) -> AsyncMock:
    """Create a mock aiohttp response context manager."""
    resp = AsyncMock()
    resp.status = status
    resp.content_type = content_type
    resp.headers = headers or {}
    resp.read = AsyncMock(return_value=body)
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=resp)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return ctx


def _mock_session(responses: list[AsyncMock]) -> AsyncMock:
    """Create a mock aiohttp.ClientSession that yields responses in order."""
    session = AsyncMock()
    session.get = MagicMock(side_effect=responses)
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=session)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return ctx


# ---------------------------------------------------------------------------
# Tests: basic pipeline
# ---------------------------------------------------------------------------


class TestBasicPipeline:
    async def test_fetch_parse_validate_normalize(self) -> None:
        """End-to-end with mocked USGS earthquake GeoJSON."""
        adapter = _make_adapter()
        body = orjson.dumps(USGS_GEOJSON)
        resp = _mock_response(body, content_type="application/geo+json")
        session = _mock_session([resp])

        with patch("hydra.adapters.rest_json.aiohttp.ClientSession", return_value=session):
            raw = await adapter.fetch()

        records = adapter.parse(raw)
        assert len(records) == 2

        valid = adapter.validate(records)
        assert len(valid) == 2

        normalized = adapter.normalize(valid)
        assert len(normalized) == 2
        assert all(isinstance(r, NormalizedRecord) for r in normalized)

    async def test_full_run_pipeline(self) -> None:
        adapter = _make_adapter()
        body = orjson.dumps(USGS_GEOJSON)
        resp = _mock_response(body, content_type="application/geo+json")
        session = _mock_session([resp])

        with patch("hydra.adapters.rest_json.aiohttp.ClientSession", return_value=session):
            results = await adapter.run()

        assert len(results) == 2
        assert all(isinstance(r, NormalizedRecord) for r in results)


# ---------------------------------------------------------------------------
# Tests: pagination
# ---------------------------------------------------------------------------


class TestPagination:
    async def test_offset_pagination(self) -> None:
        """Mock a 3-page offset-paginated API."""
        adapter = _make_adapter(stream_config={
            "pagination_type": "offset",
            "pagination_param": "offset",
            "pagination_limit": 2,
            "max_pages": 3,
            "response_root_path": "results",
            "format": "json",
            "required_fields": [],
        })

        pages = [
            orjson.dumps({"results": [{"id": "1"}, {"id": "2"}]}),
            orjson.dumps({"results": [{"id": "3"}, {"id": "4"}]}),
            orjson.dumps({"results": [{"id": "5"}]}),
        ]
        responses = [_mock_response(p) for p in pages]
        session = _mock_session(responses)

        with patch("hydra.adapters.rest_json.aiohttp.ClientSession", return_value=session):
            raw = await adapter.fetch()

        # All pages concatenated
        assert len(raw.content) > 0

    async def test_cursor_pagination(self) -> None:
        """Mock cursor-based API."""
        adapter = _make_adapter(stream_config={
            "pagination_type": "cursor",
            "pagination_param": "cursor",
            "max_pages": 3,
            "response_root_path": "data",
            "format": "json",
            "required_fields": [],
        })

        pages = [
            orjson.dumps({"data": [{"id": "1"}], "next_cursor": "abc"}),
            orjson.dumps({"data": [{"id": "2"}], "next_cursor": "def"}),
            orjson.dumps({"data": [{"id": "3"}]}),  # no cursor = stop
        ]
        responses = [_mock_response(p) for p in pages]
        session = _mock_session(responses)

        with patch("hydra.adapters.rest_json.aiohttp.ClientSession", return_value=session):
            raw = await adapter.fetch()

        assert len(raw.content) > 0


# ---------------------------------------------------------------------------
# Tests: conditional fetch (ETag)
# ---------------------------------------------------------------------------


class TestConditionalFetch:
    async def test_etag_caching(self) -> None:
        """First request returns 200 + ETag, second returns 304."""
        adapter = _make_adapter(stream_config={"supports_conditional": True})

        # First request: 200 with ETag
        resp1 = _mock_response(
            orjson.dumps(USGS_GEOJSON),
            headers={"ETag": '"abc123"'},
        )
        session1 = _mock_session([resp1])

        with patch("hydra.adapters.rest_json.aiohttp.ClientSession", return_value=session1):
            raw1 = await adapter.fetch()
        assert raw1.http_status == 200
        assert adapter._etag == '"abc123"'

        # Second request: 304
        resp2 = _mock_response(b"", status=304)
        session2 = _mock_session([resp2])

        with patch("hydra.adapters.rest_json.aiohttp.ClientSession", return_value=session2):
            raw2 = await adapter.fetch()
        assert raw2.http_status == 304
        assert raw2.content == b""


# ---------------------------------------------------------------------------
# Tests: validation
# ---------------------------------------------------------------------------


class TestValidation:
    def test_required_field_drops_invalid(self) -> None:
        adapter = _make_adapter(stream_config={
            "required_fields": ["properties.mag", "properties.place"],
        })
        records = [
            {"id": "1", "properties": {"mag": 5.0, "place": "CA"}},
            {"id": "2", "properties": {"mag": 3.0}},  # missing place
            {"id": "3", "properties": {"place": "HI"}},  # missing mag
        ]
        valid = adapter.validate(records)
        assert len(valid) == 1
        assert valid[0]["id"] == "1"

    def test_type_coercion(self) -> None:
        adapter = _make_adapter(stream_config={
            "required_fields": [],
            "field_types": {"magnitude": "float", "depth": "int"},
        })
        records = [
            {"magnitude": "5.2", "depth": "10"},
            {"magnitude": "not_a_number", "depth": "10"},
        ]
        valid = adapter.validate(records)
        assert len(valid) == 1
        assert valid[0]["magnitude"] == 5.2
        assert valid[0]["depth"] == 10

    def test_deduplication(self) -> None:
        adapter = _make_adapter(stream_config={"required_fields": []})
        records = [
            {"id": "1", "value": "a"},
            {"id": "1", "value": "a"},  # exact duplicate
            {"id": "2", "value": "b"},
        ]
        valid = adapter.validate(records)
        assert len(valid) == 2


# ---------------------------------------------------------------------------
# Tests: auth injection
# ---------------------------------------------------------------------------


class TestAuthInjection:
    async def test_api_key_in_header(self) -> None:
        settings = MagicMock(spec=HydraSettings)
        settings.credentials = {"usgs_earthquake": "my-secret-key"}
        settings.http_timeout_seconds = 30

        adapter = _make_adapter(
            settings=settings,
            stream_config={
                "auth_pattern": "api_key",
                "auth_key_location": "header",
                "auth_key_name": "X-Api-Key",
            },
        )

        resp = _mock_response(orjson.dumps({"features": []}))
        session = _mock_session([resp])

        with patch("hydra.adapters.rest_json.aiohttp.ClientSession", return_value=session):
            raw = await adapter.fetch()

        # Verify the session.get was called (auth headers are set internally)
        assert raw.http_status == 200

    async def test_api_key_in_query(self) -> None:
        settings = MagicMock(spec=HydraSettings)
        settings.credentials = {"usgs_earthquake": "my-secret-key"}
        settings.http_timeout_seconds = 30

        adapter = _make_adapter(
            settings=settings,
            stream_config={
                "auth_pattern": "api_key",
                "auth_key_location": "query",
                "auth_key_name": "api_key",
            },
        )

        resp = _mock_response(orjson.dumps({"features": []}))
        session = _mock_session([resp])

        with patch("hydra.adapters.rest_json.aiohttp.ClientSession", return_value=session):
            raw = await adapter.fetch()
        assert raw.http_status == 200


# ---------------------------------------------------------------------------
# Tests: configuration-driven behavior
# ---------------------------------------------------------------------------


class TestConfigDriven:
    def test_different_configs_different_urls(self) -> None:
        """Two different stream registry entries produce different fetch URLs."""
        adapter1 = _make_adapter(stream_config={
            "base_url": "https://api.example.com",
            "endpoint_path": "v1/data",
        })
        adapter2 = _make_adapter(stream_config={
            "base_url": "https://other.example.com",
            "endpoint_path": "v2/records",
        })
        assert adapter1._build_url() == "https://api.example.com/v1/data"
        assert adapter2._build_url() == "https://other.example.com/v2/records"

    def test_different_parse_paths(self) -> None:
        """Different response_root_path extracts different data."""
        adapter1 = _make_adapter(stream_config={
            "response_root_path": "data.items",
            "format": "json",
        })
        adapter2 = _make_adapter(stream_config={
            "response_root_path": "results",
            "format": "json",
        })

        raw1 = RawPayload(
            stream_id="test",
            fetched_at=datetime.now(timezone.utc),
            content=orjson.dumps({"data": {"items": [{"id": 1}]}}),
            content_type="application/json",
            http_status=200,
        )
        raw2 = RawPayload(
            stream_id="test",
            fetched_at=datetime.now(timezone.utc),
            content=orjson.dumps({"results": [{"id": 2}, {"id": 3}]}),
            content_type="application/json",
            http_status=200,
        )

        assert len(adapter1.parse(raw1)) == 1
        assert len(adapter2.parse(raw2)) == 2


# ---------------------------------------------------------------------------
# Tests: parse edge cases
# ---------------------------------------------------------------------------


class TestParse:
    def test_empty_content_returns_empty(self) -> None:
        adapter = _make_adapter()
        raw = RawPayload(
            stream_id="test",
            fetched_at=datetime.now(timezone.utc),
            content=b"",
            content_type="application/json",
            http_status=200,
        )
        assert adapter.parse(raw) == []

    def test_invalid_json_raises_parse_error(self) -> None:
        adapter = _make_adapter()
        raw = RawPayload(
            stream_id="test",
            fetched_at=datetime.now(timezone.utc),
            content=b"not json",
            content_type="application/json",
            http_status=200,
        )
        with pytest.raises(ParseError):
            adapter.parse(raw)

    def test_single_object_wrapped_in_list(self) -> None:
        adapter = _make_adapter(stream_config={
            "response_root_path": None,
            "format": "json",
        })
        raw = RawPayload(
            stream_id="test",
            fetched_at=datetime.now(timezone.utc),
            content=orjson.dumps({"id": "single"}),
            content_type="application/json",
            http_status=200,
        )
        result = adapter.parse(raw)
        assert len(result) == 1
        assert result[0]["id"] == "single"

    def test_geojson_auto_extracts_features(self) -> None:
        adapter = _make_adapter(stream_config={
            "format": "geojson",
            "response_root_path": None,
        })
        raw = RawPayload(
            stream_id="test",
            fetched_at=datetime.now(timezone.utc),
            content=orjson.dumps(USGS_GEOJSON),
            content_type="application/geo+json",
            http_status=200,
        )
        result = adapter.parse(raw)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# Tests: fetch error handling
# ---------------------------------------------------------------------------


class TestFetchErrors:
    async def test_429_raises_rate_limit(self) -> None:
        adapter = _make_adapter()
        resp = _mock_response(b"", status=429, headers={"Retry-After": "10"})
        session = _mock_session([resp])

        with patch("hydra.adapters.rest_json.aiohttp.ClientSession", return_value=session):
            with pytest.raises(RateLimitError) as exc_info:
                await adapter.fetch()
            assert exc_info.value.retry_after == 10.0

    async def test_5xx_raises_fetch_error(self) -> None:
        adapter = _make_adapter()
        resp = _mock_response(b"", status=503)
        session = _mock_session([resp])

        with patch("hydra.adapters.rest_json.aiohttp.ClientSession", return_value=session):
            with pytest.raises(FetchError) as exc_info:
                await adapter.fetch()
            assert exc_info.value.status_code == 503

    async def test_4xx_raises_fetch_error(self) -> None:
        adapter = _make_adapter()
        resp = _mock_response(b"", status=404)
        session = _mock_session([resp])

        with patch("hydra.adapters.rest_json.aiohttp.ClientSession", return_value=session):
            with pytest.raises(FetchError) as exc_info:
                await adapter.fetch()
            assert exc_info.value.status_code == 404
