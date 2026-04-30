"""OData adapter unit tests.

All tests use mocked HTTP responses — no live network connections.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import orjson
import pytest

from hydra.adapters.base import RawPayload
from hydra.adapters.exceptions import FetchError
from hydra.adapters.odata import ODataAdapter
from hydra.config import HydraSettings


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_settings(**overrides: Any) -> HydraSettings:
    s = HydraSettings()
    for k, v in overrides.items():
        object.__setattr__(s, k, v)
    return s


def _make_adapter(
    stream_config: dict[str, Any],
    *,
    settings_overrides: dict[str, Any] | None = None,
    last_fetch_time: str | None = None,
) -> ODataAdapter:
    settings = _make_settings(**(settings_overrides or {}))
    return ODataAdapter(
        stream_id="test_odata_stream",
        settings=settings,
        stream_config=stream_config,
        last_fetch_time=last_fetch_time,
    )


def _odata_response(value: list[dict], *, next_link: str | None = None, count: int | None = None) -> dict:
    resp: dict[str, Any] = {"value": value}
    if next_link:
        resp["@odata.nextLink"] = next_link
    if count is not None:
        resp["@odata.count"] = count
    return resp


def _mock_aiohttp_response(data: dict | bytes, status: int = 200, headers: dict | None = None):
    """Create a mock aiohttp response context manager."""
    resp = AsyncMock()
    resp.status = status
    resp.headers = headers or {}
    resp.content_type = "application/json"
    if isinstance(data, bytes):
        resp.read = AsyncMock(return_value=data)
        resp.json = AsyncMock(return_value=orjson.loads(data))
    else:
        body = orjson.dumps(data)
        resp.read = AsyncMock(return_value=body)
        resp.json = AsyncMock(return_value=data)
    return resp


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestODataBasicQuery:
    """Basic entity set query — WHO GHO style."""

    @pytest.mark.asyncio
    async def test_basic_entity_set_query(self):
        records = [{"IndicatorCode": f"IND_{i}", "Value": i * 10.0, "Country": f"C{i}"} for i in range(5)]
        page_data = _odata_response(records)

        cfg = {
            "base_url": "https://ghoapi.azureedge.net/api",
            "entity_set": "GHO",
            "auth_pattern": "none",
            "odata_top": 1000,
        }
        adapter = _make_adapter(cfg)

        with patch("aiohttp.ClientSession") as mock_session_cls:
            mock_session = AsyncMock()
            mock_session_cls.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            mock_resp = _mock_aiohttp_response(page_data)
            mock_session.get = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_resp), __aexit__=AsyncMock(return_value=False)))

            raw = await adapter.fetch()

        parsed = adapter.parse(raw)
        assert len(parsed) == 5
        assert parsed[0]["IndicatorCode"] == "IND_0"
        assert parsed[2]["Value"] == 20.0
        # Provenance tags
        assert parsed[0]["odata_entity_set"] == "GHO"
        assert parsed[0]["odata_service_url"] == "https://ghoapi.azureedge.net/api"


class TestODataPagination:
    """Pagination tests — both nextLink and skip/top."""

    @pytest.mark.asyncio
    async def test_next_link_pagination(self):
        """3-page response with @odata.nextLink."""
        page1 = _odata_response(
            [{"id": 1}, {"id": 2}],
            next_link="https://api.example.com/data?$skip=2",
        )
        page2 = _odata_response(
            [{"id": 3}, {"id": 4}],
            next_link="https://api.example.com/data?$skip=4",
        )
        page3 = _odata_response([{"id": 5}])

        cfg = {
            "base_url": "https://api.example.com",
            "entity_set": "data",
            "auth_pattern": "none",
            "odata_pagination": "next_link",
            "odata_top": 2,
        }
        adapter = _make_adapter(cfg)

        call_count = 0
        pages = [page1, page2, page3]

        with patch("aiohttp.ClientSession") as mock_session_cls:
            mock_session = AsyncMock()
            mock_session_cls.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            def make_get(*args, **kwargs):
                nonlocal call_count
                resp = _mock_aiohttp_response(pages[min(call_count, len(pages) - 1)])
                call_count += 1
                return AsyncMock(__aenter__=AsyncMock(return_value=resp), __aexit__=AsyncMock(return_value=False))

            mock_session.get = MagicMock(side_effect=make_get)

            raw = await adapter.fetch()

        parsed = adapter.parse(raw)
        assert len(parsed) == 5
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_skip_top_pagination(self):
        """$skip/$top pagination with 250 records across 3 pages."""
        page_size = 100
        page1 = _odata_response([{"id": i} for i in range(100)])
        page2 = _odata_response([{"id": i} for i in range(100, 200)])
        page3 = _odata_response([{"id": i} for i in range(200, 250)])  # < page_size → stop

        cfg = {
            "base_url": "https://api.example.com",
            "entity_set": "items",
            "auth_pattern": "none",
            "odata_pagination": "skip_top",
            "odata_top": page_size,
        }
        adapter = _make_adapter(cfg)

        call_count = 0
        pages = [page1, page2, page3]
        captured_urls: list[str] = []

        with patch("aiohttp.ClientSession") as mock_session_cls:
            mock_session = AsyncMock()
            mock_session_cls.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            def make_get(url, **kwargs):
                nonlocal call_count
                captured_urls.append(url)
                resp = _mock_aiohttp_response(pages[min(call_count, len(pages) - 1)])
                call_count += 1
                return AsyncMock(__aenter__=AsyncMock(return_value=resp), __aexit__=AsyncMock(return_value=False))

            mock_session.get = MagicMock(side_effect=make_get)

            raw = await adapter.fetch()

        parsed = adapter.parse(raw)
        assert len(parsed) == 250
        assert call_count == 3
        # Verify $skip values in URLs
        assert "%24skip=0" in captured_urls[0] or "$skip=0" in captured_urls[0]
        assert "%24skip=100" in captured_urls[1] or "$skip=100" in captured_urls[1]
        assert "%24skip=200" in captured_urls[2] or "$skip=200" in captured_urls[2]


class TestODataDynamicFilter:
    """Dynamic filter injection with {last_fetch_time}."""

    @pytest.mark.asyncio
    async def test_dynamic_filter_resolved(self):
        cfg = {
            "base_url": "https://api.example.com",
            "entity_set": "data",
            "auth_pattern": "none",
            "odata_dynamic_filter": "ModifiedDate gt {last_fetch_time}",
            "odata_top": 100,
        }
        adapter = _make_adapter(cfg, last_fetch_time="2026-04-01T00:00:00Z")

        captured_urls: list[str] = []

        with patch("aiohttp.ClientSession") as mock_session_cls:
            mock_session = AsyncMock()
            mock_session_cls.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            def make_get(url, **kwargs):
                captured_urls.append(url)
                resp = _mock_aiohttp_response(_odata_response([]))
                return AsyncMock(__aenter__=AsyncMock(return_value=resp), __aexit__=AsyncMock(return_value=False))

            mock_session.get = MagicMock(side_effect=make_get)

            await adapter.fetch()

        assert len(captured_urls) >= 1
        # The filter should contain the resolved timestamp
        url = captured_urls[0]
        assert "2026-04-01T00%3A00%3A00Z" in url or "2026-04-01T00:00:00Z" in url


class TestODataExpandFlatten:
    """$expand flattening of navigation properties."""

    def test_expand_flattening(self):
        cfg = {
            "base_url": "https://api.example.com",
            "entity_set": "indicators",
            "auth_pattern": "none",
            "expand_flatten_separator": "_",
        }
        adapter = _make_adapter(cfg)

        page = {
            "value": [
                {
                    "Id": 1,
                    "Country": {"Name": "France", "Code": "FR"},
                    "Value": 42.0,
                }
            ]
        }
        raw = RawPayload(
            stream_id="test",
            fetched_at=datetime.now(timezone.utc),
            content=orjson.dumps([page]),
            content_type="application/json",
            http_status=200,
        )

        parsed = adapter.parse(raw)
        assert len(parsed) == 1
        assert parsed[0]["Country_Name"] == "France"
        assert parsed[0]["Country_Code"] == "FR"
        assert "Country" not in parsed[0]  # original nested dict removed


class TestODataAnnotationStripping:
    """OData metadata annotations stripped from records."""

    def test_annotations_stripped(self):
        cfg = {
            "base_url": "https://api.example.com",
            "entity_set": "data",
            "auth_pattern": "none",
        }
        adapter = _make_adapter(cfg)

        page = {
            "@odata.context": "https://api.example.com/$metadata#data",
            "value": [
                {
                    "@odata.etag": "W/\"abc123\"",
                    "@odata.type": "#Example.Entity",
                    "Id": 1,
                    "Name": "Test",
                }
            ],
        }
        raw = RawPayload(
            stream_id="test",
            fetched_at=datetime.now(timezone.utc),
            content=orjson.dumps([page]),
            content_type="application/json",
            http_status=200,
        )

        parsed = adapter.parse(raw)
        assert len(parsed) == 1
        assert "@odata.etag" not in parsed[0]
        assert "@odata.type" not in parsed[0]
        assert parsed[0]["Id"] == 1
        assert parsed[0]["Name"] == "Test"


class TestODataTypeHandling:
    """OData Edm type conversions."""

    def test_type_conversions(self):
        cfg = {
            "base_url": "https://api.example.com",
            "entity_set": "data",
            "auth_pattern": "none",
        }
        adapter = _make_adapter(cfg)

        page = {
            "value": [
                {
                    "DateField": "2026-04-01T12:00:00Z",
                    "DecimalField": "123.456",
                    "IntField": 42,
                    "NullField": None,
                    "StringField": "hello",
                }
            ]
        }
        raw = RawPayload(
            stream_id="test",
            fetched_at=datetime.now(timezone.utc),
            content=orjson.dumps([page]),
            content_type="application/json",
            http_status=200,
        )

        parsed = adapter.parse(raw)
        assert len(parsed) == 1
        rec = parsed[0]
        # DateTimeOffset → datetime
        assert isinstance(rec["DateField"], datetime)
        assert rec["NullField"] is None
        assert rec["IntField"] == 42
        assert rec["StringField"] == "hello"


class TestODataErrorResponse:
    """OData structured error responses."""

    @pytest.mark.asyncio
    async def test_odata_error_body(self):
        error_body = {
            "error": {
                "code": "BadRequest",
                "message": "Invalid filter",
            }
        }

        cfg = {
            "base_url": "https://api.example.com",
            "entity_set": "data",
            "auth_pattern": "none",
        }
        adapter = _make_adapter(cfg)

        with patch("aiohttp.ClientSession") as mock_session_cls:
            mock_session = AsyncMock()
            mock_session_cls.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            resp = _mock_aiohttp_response(error_body, status=400)
            mock_session.get = MagicMock(
                return_value=AsyncMock(__aenter__=AsyncMock(return_value=resp), __aexit__=AsyncMock(return_value=False))
            )

            with pytest.raises(FetchError, match="Invalid filter"):
                await adapter.fetch()


class TestODataOAuth2:
    """OAuth2 client credentials flow."""

    @pytest.mark.asyncio
    async def test_oauth2_token_obtained_and_injected(self):
        token_response = {"access_token": "test_token_abc", "expires_in": 3600}
        data_response = _odata_response([{"id": 1}])

        cfg = {
            "base_url": "https://api.example.com",
            "entity_set": "data",
            "auth_pattern": "oauth2_client_credentials",
        }
        creds = {
            "test_odata_stream": {
                "client_id": "my_client",
                "client_secret": "my_secret",
                "token_url": "https://auth.example.com/token",
            }
        }
        adapter = _make_adapter(cfg, settings_overrides={"credentials": creds})

        captured_headers: list[dict] = []

        with patch("aiohttp.ClientSession") as mock_session_cls:
            mock_session = AsyncMock()
            mock_session_cls.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            # Token endpoint
            token_resp = AsyncMock()
            token_resp.status = 200
            token_resp.json = AsyncMock(return_value=token_response)

            # Data endpoint
            data_resp = _mock_aiohttp_response(data_response)

            mock_session.post = MagicMock(
                return_value=AsyncMock(__aenter__=AsyncMock(return_value=token_resp), __aexit__=AsyncMock(return_value=False))
            )

            def make_get(url, headers=None, **kwargs):
                if headers:
                    captured_headers.append(dict(headers))
                return AsyncMock(__aenter__=AsyncMock(return_value=data_resp), __aexit__=AsyncMock(return_value=False))

            mock_session.get = MagicMock(side_effect=make_get)

            raw = await adapter.fetch()

        # Verify token was requested
        mock_session.post.assert_called_once()
        # Verify Bearer token in data request headers
        assert any("Authorization" in h and h["Authorization"] == "Bearer test_token_abc" for h in captured_headers)

    @pytest.mark.asyncio
    async def test_oauth2_401_refresh_and_retry(self):
        """On 401, refresh token and retry once."""
        token_response = {"access_token": "refreshed_token", "expires_in": 3600}
        data_response = _odata_response([{"id": 1}])

        cfg = {
            "base_url": "https://api.example.com",
            "entity_set": "data",
            "auth_pattern": "oauth2_client_credentials",
        }
        creds = {
            "test_odata_stream": {
                "client_id": "my_client",
                "client_secret": "my_secret",
                "token_url": "https://auth.example.com/token",
            }
        }
        adapter = _make_adapter(cfg, settings_overrides={"credentials": creds})
        # Pre-set an expired token
        adapter._oauth_token = "old_token"
        adapter._oauth_expires_at = 0

        with patch("aiohttp.ClientSession") as mock_session_cls:
            mock_session = AsyncMock()
            mock_session_cls.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            # Token endpoint (called twice: initial + refresh)
            token_resp = AsyncMock()
            token_resp.status = 200
            token_resp.json = AsyncMock(return_value=token_response)
            mock_session.post = MagicMock(
                return_value=AsyncMock(__aenter__=AsyncMock(return_value=token_resp), __aexit__=AsyncMock(return_value=False))
            )

            # First data request → 401, second → success
            call_count = 0

            def make_get(url, headers=None, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    # 401 response
                    error_body = orjson.dumps({"error": {"code": "Unauthorized", "message": "Token expired"}})
                    resp = AsyncMock()
                    resp.status = 401
                    resp.headers = {}
                    resp.read = AsyncMock(return_value=error_body)
                    return AsyncMock(__aenter__=AsyncMock(return_value=resp), __aexit__=AsyncMock(return_value=False))
                else:
                    resp = _mock_aiohttp_response(data_response)
                    return AsyncMock(__aenter__=AsyncMock(return_value=resp), __aexit__=AsyncMock(return_value=False))

            mock_session.get = MagicMock(side_effect=make_get)

            raw = await adapter.fetch()

        parsed = adapter.parse(raw)
        assert len(parsed) == 1
        # Token was refreshed (post called at least twice)
        assert mock_session.post.call_count >= 2


class TestODataMetadataValidation:
    """Schema validation with $metadata CSDL."""

    def test_metadata_validation_unknown_property_warning(self):
        cfg = {
            "base_url": "https://api.example.com",
            "entity_set": "data",
            "auth_pattern": "none",
            "odata_discover": True,
        }
        adapter = _make_adapter(cfg)

        # Simulate discovered metadata
        adapter._metadata_properties = {
            "Id": "Edm.Int32",
            "Name": "Edm.String",
            "Value": "Edm.Decimal",
        }

        records = [
            {"Id": 1, "Name": "Test", "Value": 42.0, "ExtraField": "unknown"},
        ]

        valid = adapter.validate(records)
        # Unknown property logged but record NOT dropped
        assert len(valid) == 1

    def test_metadata_validation_type_mismatch_drops_record(self):
        cfg = {
            "base_url": "https://api.example.com",
            "entity_set": "data",
            "auth_pattern": "none",
            "odata_discover": True,
        }
        adapter = _make_adapter(cfg)

        adapter._metadata_properties = {
            "Id": "Edm.Int32",
            "Name": "Edm.String",
            "Value": "Edm.Decimal",
        }

        records = [
            {"Id": 1, "Name": "Test", "Value": "not_a_number"},
        ]

        valid = adapter.validate(records)
        assert len(valid) == 0


class TestODataNonNullable:
    """Non-nullable field enforcement."""

    def test_null_in_non_nullable_dropped(self):
        cfg = {
            "base_url": "https://api.example.com",
            "entity_set": "data",
            "auth_pattern": "none",
            "non_nullable_fields": ["IndicatorCode", "Value"],
        }
        adapter = _make_adapter(cfg)

        records = [
            {"IndicatorCode": "IND_1", "Value": None},  # should be dropped
            {"IndicatorCode": "IND_2", "Value": 42.0},  # should pass
        ]

        valid = adapter.validate(records)
        assert len(valid) == 1
        assert valid[0]["IndicatorCode"] == "IND_2"


class TestODataDeduplication:
    """Deduplication via composite key."""

    def test_dedup_key_fields(self):
        cfg = {
            "base_url": "https://api.example.com",
            "entity_set": "data",
            "auth_pattern": "none",
            "dedup_key_fields": ["IndicatorCode", "CountryCode", "Year"],
        }
        adapter = _make_adapter(cfg)

        records = [
            {"IndicatorCode": "IND_1", "CountryCode": "US", "Year": 2025, "Value": 100},
            {"IndicatorCode": "IND_1", "CountryCode": "US", "Year": 2025, "Value": 200},  # duplicate key
            {"IndicatorCode": "IND_1", "CountryCode": "FR", "Year": 2025, "Value": 150},  # different key
        ]

        valid = adapter.validate(records)
        assert len(valid) == 2
        assert valid[0]["Value"] == 100
        assert valid[1]["CountryCode"] == "FR"


class TestODataConfigDriven:
    """Configuration-driven behavior — different registry entries produce different behavior."""

    def test_who_gho_vs_eurostat_config(self):
        who_cfg = {
            "base_url": "https://ghoapi.azureedge.net/api",
            "entity_set": "GHO",
            "auth_pattern": "none",
            "odata_filter": "IndicatorCode eq 'WHOSIS_000001'",
            "odata_pagination": "next_link",
            "odata_top": 1000,
        }
        eurostat_cfg = {
            "base_url": "https://ec.europa.eu/eurostat/api/dissemination/odata",
            "entity_set": "nama_10_gdp",
            "auth_pattern": "none",
            "odata_pagination": "skip_top",
            "odata_top": 500,
            "odata_select": "geo,time,values",
        }

        who_adapter = _make_adapter(who_cfg)
        eurostat_adapter = _make_adapter(eurostat_cfg)

        # Different URLs
        assert who_adapter._build_entity_url() != eurostat_adapter._build_entity_url()
        assert "ghoapi" in who_adapter._build_entity_url()
        assert "eurostat" in eurostat_adapter._build_entity_url()

        # Different query options
        who_opts = who_adapter._build_query_options()
        eurostat_opts = eurostat_adapter._build_query_options()
        assert "$filter" in who_opts
        assert "$select" in eurostat_opts
        assert "$select" not in who_opts
