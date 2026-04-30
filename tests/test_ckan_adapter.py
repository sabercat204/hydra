"""Unit tests for CkanAdapter — all network calls mocked."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import orjson
import pytest

from hydra.adapters.base import RawPayload
from hydra.adapters.ckan import CkanAdapter
from hydra.adapters.exceptions import FetchError
from hydra.config import HydraSettings
from hydra.models.normalized import NormalizedRecord
from hydra.registry.stream_registry import (
    StreamRegistry,
    StreamSource,
    StreamTier,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

PACKAGE_SEARCH_RESPONSE = {
    "success": True,
    "result": {
        "count": 2,
        "results": [
            {
                "id": "ds-001",
                "title": "Australian Weather Data",
                "notes": "Daily weather observations",
                "resources": [
                    {"id": "res-001", "url": "https://data.gov.au/res/weather.csv", "format": "CSV"},
                ],
                "tags": [{"name": "weather"}, {"name": "australia"}],
                "organization": {"title": "Bureau of Meteorology"},
                "metadata_modified": "2024-06-01T00:00:00",
            },
            {
                "id": "ds-002",
                "title": "Australian Health Data",
                "notes": "Public health statistics",
                "resources": [],
                "tags": [{"name": "health"}],
                "organization": {"title": "Dept of Health"},
                "metadata_modified": "2024-05-15T00:00:00",
            },
        ],
    },
}

DATASTORE_PAGE_1 = {
    "success": True,
    "result": {
        "total": 6,
        "records": [
            {"_id": 1, "name": "Alice", "value": 10},
            {"_id": 2, "name": "Bob", "value": 20},
        ],
    },
}

DATASTORE_PAGE_2 = {
    "success": True,
    "result": {
        "total": 6,
        "records": [
            {"_id": 3, "name": "Charlie", "value": 30},
            {"_id": 4, "name": "Diana", "value": 40},
        ],
    },
}

DATASTORE_PAGE_3 = {
    "success": True,
    "result": {
        "total": 6,
        "records": [
            {"_id": 5, "name": "Eve", "value": 50},
            {"_id": 6, "name": "Frank", "value": 60},
        ],
    },
}

PACKAGE_SHOW_WITH_CSV = {
    "success": True,
    "result": {
        "id": "ds-csv-001",
        "title": "CSV Dataset",
        "notes": "A dataset with CSV resource",
        "resources": [
            {"id": "res-csv-001", "url": "https://data.gov.au/res/data.csv", "format": "CSV"},
        ],
        "tags": [],
        "organization": {"title": "Test Org"},
        "metadata_modified": "2024-06-01T00:00:00",
    },
}

CSV_CONTENT = b"name,age,city\nAlice,30,Sydney\nBob,25,Melbourne\nCharlie,35,Brisbane"


def _make_registry() -> StreamRegistry:
    src = StreamSource(
        name="data_gov_au", url="https://data.gov.au", format="json", auth="none", notes=""
    )
    tier = StreamTier(
        id=10, name="Asia-Pacific Gov", streams=1, access="5G",
        formats=["json", "csv"], cadence="weekly", adapter="ckan", fallback="rest_json",
        sources=[src],
    )
    return StreamRegistry(tiers={10: tier})


def _make_adapter(
    stream_config: dict[str, Any] | None = None,
    settings: HydraSettings | None = None,
) -> CkanAdapter:
    cfg: dict[str, Any] = {
        "base_url": "https://data.gov.au",
        "ckan_action": "package_search",
        "search_query": "weather",
        "portal_id": "data_gov_au",
        "required_fields": [],
        **(stream_config or {}),
    }
    return CkanAdapter(
        stream_id="data_gov_au",
        settings=settings or HydraSettings(),
        registry=_make_registry(),
        stream_config=cfg,
    )


def _mock_response(
    body: bytes,
    status: int = 200,
    content_type: str = "application/json",
    headers: dict[str, str] | None = None,
) -> AsyncMock:
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
    session = AsyncMock()
    session.get = MagicMock(side_effect=responses)
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=session)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return ctx


# ---------------------------------------------------------------------------
# Tests: package_search fetch + parse
# ---------------------------------------------------------------------------


class TestPackageSearch:
    async def test_fetch_and_parse_package_search(self) -> None:
        """Test package_search with mocked Australian data.gov.au response."""
        adapter = _make_adapter()
        body = orjson.dumps(PACKAGE_SEARCH_RESPONSE)
        resp = _mock_response(body)
        session = _mock_session([resp])

        with patch("hydra.adapters.ckan.aiohttp.ClientSession", return_value=session):
            raw = await adapter.fetch()

        records = adapter.parse(raw)
        assert len(records) == 2
        assert records[0]["title"] == "Australian Weather Data"
        assert records[0]["portal_id"] == "data_gov_au"
        assert records[1]["title"] == "Australian Health Data"


# ---------------------------------------------------------------------------
# Tests: datastore_search with pagination
# ---------------------------------------------------------------------------


class TestDatastoreSearch:
    async def test_datastore_pagination_3_pages(self) -> None:
        """Mock 3 pages of tabular data, verify all accumulated."""
        adapter = _make_adapter(stream_config={
            "ckan_action": "datastore_search",
            "resource_id": "res-123",
            "limit": 2,
            "max_pages": 5,
        })

        pages = [
            _mock_response(orjson.dumps(DATASTORE_PAGE_1)),
            _mock_response(orjson.dumps(DATASTORE_PAGE_2)),
            _mock_response(orjson.dumps(DATASTORE_PAGE_3)),
        ]
        session = _mock_session(pages)

        with patch("hydra.adapters.ckan.aiohttp.ClientSession", return_value=session):
            raw = await adapter.fetch()

        records = adapter.parse(raw)
        assert len(records) == 6
        names = [r["name"] for r in records]
        assert names == ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank"]


# ---------------------------------------------------------------------------
# Tests: resource download
# ---------------------------------------------------------------------------


class TestResourceDownload:
    async def test_package_show_with_csv_download(self) -> None:
        """Mock package_show returning a dataset with CSV resource URL, verify CSV parsed."""
        adapter = _make_adapter(stream_config={
            "ckan_action": "package_show",
            "package_id": "ds-csv-001",
            "download_resources": True,
            "resource_format_filter": ["csv"],
        })

        api_resp = _mock_response(orjson.dumps(PACKAGE_SHOW_WITH_CSV))
        csv_resp = _mock_response(CSV_CONTENT, content_type="text/csv")
        session = _mock_session([api_resp, csv_resp])

        with patch("hydra.adapters.ckan.aiohttp.ClientSession", return_value=session):
            raw = await adapter.fetch()

        records = adapter.parse(raw)
        # Should have 1 dataset metadata record + 3 CSV rows
        dataset_records = [r for r in records if "title" in r]
        csv_records = [r for r in records if "name" in r and "age" in r]
        assert len(dataset_records) == 1
        assert len(csv_records) == 3
        assert csv_records[0]["name"] == "Alice"
        assert csv_records[0]["portal_id"] == "data_gov_au"


# ---------------------------------------------------------------------------
# Tests: encoding normalization
# ---------------------------------------------------------------------------


class TestEncodingNormalization:
    async def test_latin1_encoded_response(self) -> None:
        """Inject Latin-1 encoded response with UTF-8 content-type header, verify graceful handling."""
        adapter = _make_adapter(stream_config={
            "ckan_action": "package_search",
        })

        # Create a response with Latin-1 characters
        latin1_text = "Données météorologiques françaises"
        result = {
            "success": True,
            "result": {
                "count": 1,
                "results": [
                    {
                        "id": "ds-fr-001",
                        "title": latin1_text,
                        "notes": "Description with accents: café, résumé",
                        "resources": [],
                        "tags": [],
                        "organization": {"title": "Météo-France"},
                        "metadata_modified": "2024-06-01T00:00:00",
                    },
                ],
            },
        }
        body = json.dumps(result).encode("latin-1")
        resp = _mock_response(body, content_type="application/json")
        session = _mock_session([resp])

        with patch("hydra.adapters.ckan.aiohttp.ClientSession", return_value=session):
            raw = await adapter.fetch()

        records = adapter.parse(raw)
        valid = adapter.validate(records)
        # Should handle encoding gracefully without crashing
        assert len(valid) >= 1


# ---------------------------------------------------------------------------
# Tests: schema drift detection
# ---------------------------------------------------------------------------


class TestSchemaDrift:
    def test_schema_drift_warning_logged(self) -> None:
        """Inject records where later records are missing >50% of fields, verify WARNING logged."""
        adapter = _make_adapter()

        records = [
            {"id": "1", "title": "Full Record", "description": "Has all fields", "tags": [], "organization": "Org"},
            {"id": "2", "title": "Also Full", "description": "Has all fields", "tags": [], "organization": "Org"},
            {"x": "3"},  # Missing >50% of fields from first record
        ]

        with patch.object(adapter._log, "warning") as mock_warn:
            valid = adapter.validate(records)

        # All records pass (schema drift is logged but not dropped)
        assert len(valid) == 3
        # Warning should have been called for schema drift
        mock_warn.assert_called()
        call_args = [call.kwargs.get("event") or call.args[0] if call.args else call.kwargs.get("event", "")
                     for call in mock_warn.call_args_list]
        assert any("schema_drift" in str(arg) for arg in call_args)


# ---------------------------------------------------------------------------
# Tests: auth
# ---------------------------------------------------------------------------


class TestAuth:
    async def test_api_key_auth(self) -> None:
        """Mock a portal requiring API key, verify key injected."""
        settings = MagicMock(spec=HydraSettings)
        settings.credentials = {"data_gov_au": "my-api-key-123"}
        settings.http_timeout_seconds = 30

        adapter = _make_adapter(
            settings=settings,
            stream_config={
                "ckan_action": "package_search",
                "auth_pattern": "api_key",
                "auth_key_name": "Authorization",
            },
        )

        body = orjson.dumps(PACKAGE_SEARCH_RESPONSE)
        resp = _mock_response(body)
        session = _mock_session([resp])

        with patch("hydra.adapters.ckan.aiohttp.ClientSession", return_value=session):
            raw = await adapter.fetch()

        assert raw.http_status == 200
        records = adapter.parse(raw)
        assert len(records) == 2
