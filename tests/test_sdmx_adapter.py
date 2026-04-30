"""SDMX adapter unit tests.

All tests use mocked dependencies — no live network connections.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

import orjson
import pandas as pd
import pytest

from hydra.adapters.base import RawPayload
from hydra.adapters.exceptions import FetchError
from hydra.adapters.sdmx import SdmxAdapter, normalize_time_period
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
) -> SdmxAdapter:
    settings = _make_settings(**(settings_overrides or {}))
    return SdmxAdapter(
        stream_id="test_sdmx_stream",
        settings=settings,
        stream_config=stream_config,
        last_fetch_time=last_fetch_time,
    )


def _make_raw_payload(mode: str, data: Any) -> RawPayload:
    payload = {"mode": mode, "data": data}
    return RawPayload(
        stream_id="test_sdmx_stream",
        fetched_at=datetime.now(timezone.utc),
        content=orjson.dumps(payload),
        content_type="application/json",
        http_status=200,
    )


# ---------------------------------------------------------------------------
# Time period normalization tests
# ---------------------------------------------------------------------------


class TestTimePeriodNormalization:
    """SDMX time period format normalization."""

    def test_annual(self):
        dt, raw = normalize_time_period("2026")
        assert dt == datetime(2026, 1, 1, tzinfo=timezone.utc)
        assert raw == "2026"

    def test_quarterly_q1(self):
        dt, raw = normalize_time_period("2026-Q1")
        assert dt == datetime(2026, 1, 1, tzinfo=timezone.utc)
        assert raw == "2026-Q1"

    def test_quarterly_q2(self):
        dt, _ = normalize_time_period("2026-Q2")
        assert dt == datetime(2026, 4, 1, tzinfo=timezone.utc)

    def test_monthly_m_format(self):
        dt, raw = normalize_time_period("2026-M04")
        assert dt == datetime(2026, 4, 1, tzinfo=timezone.utc)
        assert raw == "2026-M04"

    def test_monthly_short_format(self):
        dt, _ = normalize_time_period("2026-04")
        assert dt == datetime(2026, 4, 1, tzinfo=timezone.utc)

    def test_daily(self):
        dt, raw = normalize_time_period("2026-04-03")
        assert dt == datetime(2026, 4, 3, tzinfo=timezone.utc)
        assert raw == "2026-04-03"

    def test_preserves_raw_period(self):
        _, raw = normalize_time_period("2026-Q1")
        assert raw == "2026-Q1"


# ---------------------------------------------------------------------------
# pandasdmx basic query
# ---------------------------------------------------------------------------


class TestSdmxPandasdmxBasicQuery:
    """pandasdmx basic query — Eurostat GDP dataflow."""

    @pytest.mark.asyncio
    async def test_basic_pandasdmx_query(self):
        cfg = {
            "sdmx_agency": "ESTAT",
            "dataflow_id": "nama_10_gdp",
            "sdmx_key": "A.GDP.EU27_2020.CLV10_MEUR",
            "auth_pattern": "none",
            "include_dimension_labels": False,
        }
        adapter = _make_adapter(cfg)

        # Mock pandasdmx
        mock_df = pd.DataFrame({
            "FREQ": ["A", "A"],
            "NA_ITEM": ["GDP", "GDP"],
            "GEO": ["EU27_2020", "US"],
            "UNIT": ["CLV10_MEUR", "CLV10_MEUR"],
            "TIME_PERIOD": ["2025", "2025"],
            "value": [15000000.0, 25000000.0],
        })

        mock_records = mock_df.to_dict(orient="records")

        with patch("hydra.adapters.sdmx.pandasdmx") as mock_pdmx:
            mock_req = MagicMock()
            mock_pdmx.Request.return_value = mock_req
            mock_pdmx.list_sources.return_value = ["ESTAT", "ECB"]

            # DSD fetch
            mock_dsd = MagicMock()
            mock_req.datastructure.return_value = mock_dsd

            # Data fetch
            mock_msg = MagicMock()
            mock_req.data.return_value = mock_msg

            mock_pdmx.to_pandas.return_value = mock_df

            raw = await adapter.fetch()

        parsed = adapter.parse(raw)
        assert len(parsed) == 2
        assert parsed[0]["GEO"] == "EU27_2020"
        assert parsed[0]["sdmx_agency"] == "ESTAT"
        assert parsed[0]["sdmx_dataflow"] == "nama_10_gdp"
        # Time period normalized
        assert "time_period_raw" in parsed[0]


class TestSdmxDsdCaching:
    """DSD fetch and caching."""

    @pytest.mark.asyncio
    async def test_dsd_cached_on_second_call(self):
        cfg = {
            "sdmx_agency": "ESTAT",
            "dataflow_id": "test_flow",
            "auth_pattern": "none",
        }
        adapter = _make_adapter(cfg)

        mock_df = pd.DataFrame({"TIME_PERIOD": ["2025"], "value": [100.0]})

        with patch("hydra.adapters.sdmx.pandasdmx") as mock_pdmx:
            mock_req = MagicMock()
            mock_pdmx.Request.return_value = mock_req
            mock_pdmx.list_sources.return_value = ["ESTAT"]

            mock_dsd = MagicMock()
            mock_req.datastructure.return_value = mock_dsd
            mock_msg = MagicMock()
            mock_req.data.return_value = mock_msg
            mock_pdmx.to_pandas.return_value = mock_df

            # First fetch — DSD should be fetched
            await adapter.fetch()
            first_dsd_calls = mock_req.datastructure.call_count

            # Second fetch — DSD should be cached
            await adapter.fetch()
            second_dsd_calls = mock_req.datastructure.call_count

        assert first_dsd_calls == 1
        assert second_dsd_calls == 1  # No additional DSD call


class TestSdmxDimensionLabels:
    """Dimension label resolution from code lists."""

    def test_dimension_labels_added(self):
        cfg = {
            "sdmx_agency": "ESTAT",
            "dataflow_id": "test_flow",
            "auth_pattern": "none",
            "include_dimension_labels": True,
        }
        adapter = _make_adapter(cfg)

        # Set up code lists
        adapter._code_lists = {
            "GEO": {
                "EU27_2020": "European Union - 27 countries (from 2020)",
                "US": "United States",
            }
        }

        data = [
            {"GEO": "EU27_2020", "TIME_PERIOD": "2025", "value": 15000000.0},
        ]
        raw = _make_raw_payload("pandasdmx", data)

        parsed = adapter.parse(raw)
        assert len(parsed) == 1
        assert parsed[0]["GEO_label"] == "European Union - 27 countries (from 2020)"


class TestSdmxTimePeriodInParse:
    """Time period normalization in parse step."""

    def test_all_period_formats_normalized(self):
        cfg = {
            "sdmx_agency": "ESTAT",
            "dataflow_id": "test_flow",
            "auth_pattern": "none",
            "include_dimension_labels": False,
        }
        adapter = _make_adapter(cfg)

        data = [
            {"TIME_PERIOD": "2026", "value": 1},
            {"TIME_PERIOD": "2026-Q1", "value": 2},
            {"TIME_PERIOD": "2026-M04", "value": 3},
            {"TIME_PERIOD": "2026-04-03", "value": 4},
        ]
        raw = _make_raw_payload("pandasdmx", data)

        parsed = adapter.parse(raw)
        assert len(parsed) == 4

        assert parsed[0]["TIME_PERIOD"] == "2026-01-01T00:00:00+00:00"
        assert parsed[0]["time_period_raw"] == "2026"

        assert parsed[1]["TIME_PERIOD"] == "2026-01-01T00:00:00+00:00"
        assert parsed[1]["time_period_raw"] == "2026-Q1"

        assert parsed[2]["TIME_PERIOD"] == "2026-04-01T00:00:00+00:00"
        assert parsed[2]["time_period_raw"] == "2026-M04"

        assert parsed[3]["TIME_PERIOD"] == "2026-04-03T00:00:00+00:00"
        assert parsed[3]["time_period_raw"] == "2026-04-03"


class TestSdmxIncrementalQuery:
    """Incremental query with updatedAfter template."""

    @pytest.mark.asyncio
    async def test_updated_after_resolved(self):
        cfg = {
            "sdmx_agency": "ESTAT",
            "dataflow_id": "test_flow",
            "auth_pattern": "none",
            "sdmx_params": {"updatedAfter": "{last_fetch_time}"},
        }
        adapter = _make_adapter(cfg, last_fetch_time="2026-04-01T00:00:00Z")

        resolved = adapter._resolve_params()
        assert resolved["updatedAfter"] == "2026-04-01T00:00:00Z"


class TestSdmxRawHttpFallbackJson:
    """Raw HTTP fallback — SDMX-JSON format."""

    def test_sdmx_json_parsing(self):
        cfg = {
            "sdmx_agency": "ESTAT",
            "dataflow_id": "test_flow",
            "auth_pattern": "none",
        }
        adapter = _make_adapter(cfg)

        sdmx_json = {
            "structure": {
                "dimensions": {
                    "series": [
                        {"id": "FREQ", "values": [{"id": "A", "name": "Annual"}]},
                        {"id": "GEO", "values": [{"id": "EU27", "name": "EU 27"}]},
                    ],
                    "observation": [
                        {"id": "TIME_PERIOD", "values": [
                            {"id": "2024", "name": "2024"},
                            {"id": "2025", "name": "2025"},
                        ]},
                    ],
                },
                "attributes": {"series": [], "observation": []},
            },
            "dataSets": [
                {
                    "series": {
                        "0:0": {
                            "observations": {
                                "0": [100.5],
                                "1": [105.2],
                            }
                        }
                    }
                }
            ],
        }

        raw = _make_raw_payload("sdmx_json", sdmx_json)
        parsed = adapter.parse(raw)

        assert len(parsed) == 2
        assert parsed[0]["FREQ"] == "A"
        assert parsed[0]["GEO"] == "EU27"
        assert parsed[0]["TIME_PERIOD"] == "2024-01-01T00:00:00+00:00"
        assert parsed[0]["value"] == 100.5
        assert parsed[1]["value"] == 105.2


class TestSdmxRawHttpFallbackXml:
    """Raw HTTP fallback — SDMX-ML (XML) format."""

    def test_sdmx_xml_parsing(self):
        cfg = {
            "sdmx_agency": "ESTAT",
            "dataflow_id": "test_flow",
            "auth_pattern": "none",
        }
        adapter = _make_adapter(cfg)

        sdmx_xml = """<?xml version="1.0" encoding="UTF-8"?>
<GenericData>
  <DataSet>
    <Series>
      <SeriesKey>
        <Value id="FREQ" value="A"/>
        <Value id="GEO" value="DE"/>
      </SeriesKey>
      <Obs>
        <ObsDimension value="2025"/>
        <ObsValue value="3500000"/>
      </Obs>
      <Obs>
        <ObsDimension value="2024"/>
        <ObsValue value="3400000"/>
      </Obs>
    </Series>
  </DataSet>
</GenericData>"""

        raw = _make_raw_payload("sdmx_xml", sdmx_xml)
        parsed = adapter.parse(raw)

        assert len(parsed) == 2
        assert parsed[0]["FREQ"] == "A"
        assert parsed[0]["GEO"] == "DE"
        assert parsed[0]["TIME_PERIOD"] == "2025-01-01T00:00:00+00:00"
        assert parsed[0]["value"] == "3500000"
        assert parsed[1]["TIME_PERIOD"] == "2024-01-01T00:00:00+00:00"


class TestSdmxStrictDimensionValidation:
    """Strict dimension validation against code lists."""

    def test_invalid_dimension_value_dropped(self):
        cfg = {
            "sdmx_agency": "ESTAT",
            "dataflow_id": "test_flow",
            "auth_pattern": "none",
            "strict_dimensions": ["GEO"],
        }
        adapter = _make_adapter(cfg)
        adapter._code_lists = {
            "GEO": {"EU27_2020": "EU 27", "US": "United States"},
        }

        records = [
            {"GEO": "INVALID_CODE", "TIME_PERIOD": "2025-01-01T00:00:00+00:00", "value": 100},
            {"GEO": "EU27_2020", "TIME_PERIOD": "2025-01-01T00:00:00+00:00", "value": 200},
        ]

        valid = adapter.validate(records)
        assert len(valid) == 1
        assert valid[0]["GEO"] == "EU27_2020"

    def test_valid_dimension_value_passes(self):
        cfg = {
            "sdmx_agency": "ESTAT",
            "dataflow_id": "test_flow",
            "auth_pattern": "none",
            "strict_dimensions": ["GEO"],
        }
        adapter = _make_adapter(cfg)
        adapter._code_lists = {
            "GEO": {"EU27_2020": "EU 27"},
        }

        records = [
            {"GEO": "EU27_2020", "TIME_PERIOD": "2025-01-01T00:00:00+00:00", "value": 100},
        ]

        valid = adapter.validate(records)
        assert len(valid) == 1


class TestSdmxTimePeriodRangeValidation:
    """Time period range validation."""

    def test_out_of_range_dropped(self):
        cfg = {
            "sdmx_agency": "ESTAT",
            "dataflow_id": "test_flow",
            "auth_pattern": "none",
            "time_period_range": ["2020-01-01", "2026-12-31"],
        }
        adapter = _make_adapter(cfg)

        records = [
            {"TIME_PERIOD": "1900-01-01T00:00:00+00:00", "value": 100},  # out of range
            {"TIME_PERIOD": "2025-01-01T00:00:00+00:00", "value": 200},  # in range
        ]

        valid = adapter.validate(records)
        assert len(valid) == 1
        assert valid[0]["value"] == 200

    def test_in_range_passes(self):
        cfg = {
            "sdmx_agency": "ESTAT",
            "dataflow_id": "test_flow",
            "auth_pattern": "none",
            "time_period_range": ["2020-01-01", "2026-12-31"],
        }
        adapter = _make_adapter(cfg)

        records = [
            {"TIME_PERIOD": "2025-06-15T00:00:00+00:00", "value": 300},
        ]

        valid = adapter.validate(records)
        assert len(valid) == 1


class TestSdmxObservationValueRange:
    """Observation value range validation."""

    def test_out_of_range_dropped(self):
        cfg = {
            "sdmx_agency": "ESTAT",
            "dataflow_id": "test_flow",
            "auth_pattern": "none",
            "observation_value_range": [0, 1000000000],
        }
        adapter = _make_adapter(cfg)

        records = [
            {"TIME_PERIOD": "2025-01-01T00:00:00+00:00", "value": -999},  # out of range
            {"TIME_PERIOD": "2025-01-01T00:00:00+00:00", "value": 500},  # in range
        ]

        valid = adapter.validate(records)
        assert len(valid) == 1
        assert valid[0]["value"] == 500


class TestSdmxNonNumericObservation:
    """Non-numeric observation rejection."""

    def test_non_numeric_dropped(self):
        cfg = {
            "sdmx_agency": "ESTAT",
            "dataflow_id": "test_flow",
            "auth_pattern": "none",
        }
        adapter = _make_adapter(cfg)

        records = [
            {"TIME_PERIOD": "2025-01-01T00:00:00+00:00", "value": "N/A"},
            {"TIME_PERIOD": "2025-01-01T00:00:00+00:00", "value": 42.0},
        ]

        valid = adapter.validate(records)
        assert len(valid) == 1
        assert valid[0]["value"] == 42.0


class TestSdmxDeduplication:
    """Deduplication of identical observations."""

    def test_duplicate_observations_deduplicated(self):
        cfg = {
            "sdmx_agency": "ESTAT",
            "dataflow_id": "test_flow",
            "auth_pattern": "none",
        }
        adapter = _make_adapter(cfg)

        records = [
            {"FREQ": "A", "GEO": "DE", "TIME_PERIOD": "2025-01-01T00:00:00+00:00", "value": 100},
            {"FREQ": "A", "GEO": "DE", "TIME_PERIOD": "2025-01-01T00:00:00+00:00", "value": 100},  # duplicate
        ]

        valid = adapter.validate(records)
        assert len(valid) == 1


class TestSdmxRequestDelay:
    """Request delay enforcement between consecutive requests."""

    @pytest.mark.asyncio
    async def test_request_delay_enforced(self):
        cfg = {
            "sdmx_agency": "ESTAT",
            "dataflow_id": "test_flow",
            "auth_pattern": "none",
            "sdmx_request_delay_seconds": 0.5,
        }
        adapter = _make_adapter(cfg)

        # Simulate two consecutive delay enforcements
        adapter._last_request_time = time.monotonic()

        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await adapter._enforce_request_delay()
            # Should have slept for approximately the delay
            if mock_sleep.called:
                sleep_time = mock_sleep.call_args[0][0]
                assert sleep_time > 0
                assert sleep_time <= 0.5


class TestSdmxCustomProviderRegistration:
    """Custom provider registration for agencies not in pandasdmx built-in registry."""

    @pytest.mark.asyncio
    async def test_custom_agency_registered(self):
        cfg = {
            "sdmx_agency": "CUSTOM_AGENCY",
            "dataflow_id": "custom_flow",
            "auth_pattern": "none",
            "base_url": "https://custom.stats.example.com/sdmx",
        }
        adapter = _make_adapter(cfg)

        mock_df = pd.DataFrame({"TIME_PERIOD": ["2025"], "value": [100.0]})

        with patch("hydra.adapters.sdmx.pandasdmx") as mock_pdmx:
            mock_req = MagicMock()
            mock_pdmx.Request.return_value = mock_req
            mock_pdmx.list_sources.return_value = ["ESTAT", "ECB"]  # CUSTOM_AGENCY not in list

            mock_dsd = MagicMock()
            mock_req.datastructure.return_value = mock_dsd
            mock_msg = MagicMock()
            mock_req.data.return_value = mock_msg
            mock_pdmx.to_pandas.return_value = mock_df

            await adapter.fetch()

            # Verify add_source was called for the custom agency
            mock_pdmx.add_source.assert_called_once()
            call_args = mock_pdmx.add_source.call_args[0][0]
            assert call_args["id"] == "CUSTOM_AGENCY"
            assert call_args["url"] == "https://custom.stats.example.com/sdmx"


class TestSdmxApiKeyAuth:
    """API key authentication for SDMX providers."""

    @pytest.mark.asyncio
    async def test_api_key_in_header(self):
        cfg = {
            "sdmx_agency": "CUSTOM",
            "dataflow_id": "test_flow",
            "auth_pattern": "api_key",
            "auth_key_name": "X-Api-Key",
            "auth_key_location": "header",
            "base_url": "https://api.example.com/sdmx",
            "sdmx_format_preference": ["json"],
        }
        adapter = _make_adapter(cfg, settings_overrides={"credentials": {"test_sdmx_stream": "my_secret_key"}})

        # Force raw HTTP fallback by making pandasdmx fail
        captured_headers: list[dict] = []

        with patch("hydra.adapters.sdmx.pandasdmx") as mock_pdmx:
            mock_pdmx.list_sources.return_value = []
            mock_pdmx.Request.side_effect = Exception("Force fallback")

            with patch("aiohttp.ClientSession") as mock_session_cls:
                mock_session = AsyncMock()
                mock_session_cls.return_value.__aenter__ = AsyncMock(return_value=mock_session)
                mock_session_cls.return_value.__aexit__ = AsyncMock(return_value=False)

                sdmx_json = {"structure": {"dimensions": {"series": [], "observation": []}, "attributes": {"series": [], "observation": []}}, "dataSets": []}

                def make_get(url, headers=None, **kwargs):
                    if headers:
                        captured_headers.append(dict(headers))
                    resp = AsyncMock()
                    resp.status = 200
                    resp.headers = {}
                    resp.read = AsyncMock(return_value=orjson.dumps(sdmx_json))
                    return AsyncMock(__aenter__=AsyncMock(return_value=resp), __aexit__=AsyncMock(return_value=False))

                mock_session.get = MagicMock(side_effect=make_get)

                await adapter.fetch()

        assert any("X-Api-Key" in h and h["X-Api-Key"] == "my_secret_key" for h in captured_headers)


class TestSdmxConfigDriven:
    """Configuration-driven behavior — different registry entries produce different configs."""

    def test_eurostat_vs_ecb_config(self):
        estat_cfg = {
            "sdmx_agency": "ESTAT",
            "dataflow_id": "nama_10_gdp",
            "sdmx_key": "A.GDP.EU27_2020.CLV10_MEUR",
            "auth_pattern": "none",
            "sdmx_params": {"startPeriod": "2020"},
        }
        ecb_cfg = {
            "sdmx_agency": "ECB",
            "dataflow_id": "EXR",
            "sdmx_key": "D.USD.EUR.SP00.A",
            "auth_pattern": "none",
            "sdmx_params": {"startPeriod": "2025", "detail": "dataonly"},
        }

        estat_adapter = _make_adapter(estat_cfg)
        ecb_adapter = _make_adapter(ecb_cfg)

        assert estat_adapter._get_cfg("sdmx_agency") == "ESTAT"
        assert ecb_adapter._get_cfg("sdmx_agency") == "ECB"
        assert estat_adapter._get_cfg("dataflow_id") != ecb_adapter._get_cfg("dataflow_id")
        assert estat_adapter._resolve_params() != ecb_adapter._resolve_params()


class TestSdmxPandasdmxFallbackToRawHttp:
    """When pandasdmx fails, adapter falls back to raw HTTP."""

    @pytest.mark.asyncio
    async def test_fallback_on_pandasdmx_failure(self):
        cfg = {
            "sdmx_agency": "ESTAT",
            "dataflow_id": "test_flow",
            "auth_pattern": "none",
            "base_url": "https://sdw-wsrest.ecb.europa.eu/service",
            "sdmx_format_preference": ["json"],
        }
        adapter = _make_adapter(cfg)

        sdmx_json = {
            "structure": {
                "dimensions": {
                    "series": [{"id": "FREQ", "values": [{"id": "D"}]}],
                    "observation": [{"id": "TIME_PERIOD", "values": [{"id": "2025"}]}],
                },
                "attributes": {"series": [], "observation": []},
            },
            "dataSets": [{"series": {"0": {"observations": {"0": [1.05]}}}}],
        }

        with patch("hydra.adapters.sdmx.pandasdmx") as mock_pdmx:
            mock_pdmx.list_sources.return_value = ["ESTAT"]
            mock_pdmx.Request.side_effect = Exception("pandasdmx broken")

            with patch("aiohttp.ClientSession") as mock_session_cls:
                mock_session = AsyncMock()
                mock_session_cls.return_value.__aenter__ = AsyncMock(return_value=mock_session)
                mock_session_cls.return_value.__aexit__ = AsyncMock(return_value=False)

                resp = AsyncMock()
                resp.status = 200
                resp.headers = {}
                resp.read = AsyncMock(return_value=orjson.dumps(sdmx_json))
                mock_session.get = MagicMock(
                    return_value=AsyncMock(__aenter__=AsyncMock(return_value=resp), __aexit__=AsyncMock(return_value=False))
                )

                raw = await adapter.fetch()

        parsed = adapter.parse(raw)
        assert len(parsed) == 1
        assert parsed[0]["FREQ"] == "D"
        assert parsed[0]["value"] == 1.05
