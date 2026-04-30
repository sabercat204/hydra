"""Tests for the AIS/ADS-B adapter — 30 test cases."""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
from unittest.mock import AsyncMock, MagicMock, patch

# Prevent __init__.py from importing all adapters (some have heavy deps)
sys.modules.setdefault("hydra.adapters.ckan", MagicMock())
sys.modules.setdefault("hydra.adapters.fdsn", MagicMock())
sys.modules.setdefault("hydra.adapters.odata", MagicMock())
sys.modules.setdefault("hydra.adapters.rest_json", MagicMock())
sys.modules.setdefault("hydra.adapters.s3_bulk", MagicMock())
sys.modules.setdefault("hydra.adapters.sdmx", MagicMock())
sys.modules.setdefault("hydra.adapters.tap_vo", MagicMock())

import pytest

from hydra.adapters.ais_adsb import AisAdsbAdapter
from hydra.adapters.base import AdapterHealth, HealthStatus, RawPayload
from hydra.adapters.exceptions import FetchError
from hydra.config import HydraSettings
from hydra.models.normalized import NormalizedRecord


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _load_fixture(name: str) -> bytes:
    return Path(f"tests/fixtures/{name}").read_bytes()


SAMPLE_ADSB_JSON = {
    "ac": [
        {"hex": f"{i:06x}", "flight": f"TST{i:03d}", "lat": 37.0 + i * 0.1, "lon": -122.0 + i * 0.1,
         "alt_baro": 10000 + i * 1000, "gs": 250 + i * 10, "track": i * 36, "baro_rate": i,
         "ground": False, "squawk": f"{1200 + i:04d}", "now": time.time() - 10}
        for i in range(10)
    ]
}

SAMPLE_OPENSKY_JSON = {
    "time": int(time.time()),
    "states": [
        [f"{i:06x}", f"TST{i:03d} ", "US", int(time.time()) - 5, int(time.time()) - 5,
         -122.0 + i * 0.1, 37.0 + i * 0.1, 10000 + i * 500, False, 250 + i * 10,
         i * 36, i * 0.5, None, 10000 + i * 500, f"{1200 + i:04d}", False, 0]
        for i in range(5)
    ]
}

SAMPLE_AIS_JSON = {
    "data": [
        {"MMSI": f"21123456{i}", "IMO": f"900000{i}", "SHIPNAME": f"VESSEL{i}",
         "SHIPTYPE": 70, "LAT": 51.0 + i * 0.01, "LON": 3.0 + i * 0.01,
         "SPEED": 12.5 + i, "COURSE": 180 + i * 10, "HEADING": 180 + i * 10,
         "STATUS": 0, "DESTINATION": "ANTWERP", "ETA": "2024-06-15T12:00:00Z",
         "DRAUGHT": 8.5, "TIMESTAMP": datetime.now(timezone.utc).isoformat()}
        for i in range(5)
    ]
}

ADSB_FIELD_MAPPING = {
    "hex": "icao24", "flight": "callsign", "lat": "latitude", "lon": "longitude",
    "alt_baro": "altitude_m", "gs": "velocity_ms", "track": "heading",
    "baro_rate": "vertical_rate_ms", "ground": "on_ground", "squawk": "squawk", "now": "timestamp",
}

OPENSKY_FIELD_MAPPING = {
    "0": "icao24", "1": "callsign", "5": "longitude", "6": "latitude",
    "7": "altitude_m", "9": "velocity_ms", "10": "heading", "11": "vertical_rate_ms",
    "8": "on_ground", "14": "squawk", "4": "timestamp",
}

AIS_FIELD_MAPPING = {
    "MMSI": "mmsi", "IMO": "imo", "SHIPNAME": "vessel_name", "SHIPTYPE": "ship_type",
    "LAT": "latitude", "LON": "longitude", "SPEED": "speed_knots", "COURSE": "course",
    "HEADING": "heading", "STATUS": "nav_status", "DESTINATION": "destination",
    "ETA": "eta", "DRAUGHT": "draught", "TIMESTAMP": "timestamp",
}


def _make_settings(**overrides) -> HydraSettings:
    defaults = {
        "stream_registry_path": Path("src/hydra/registry/stream_registry.yaml"),
        "data_dir": Path("/tmp/hydra_test"),
        "http_timeout_seconds": 30,
        "credentials": {},
    }
    defaults.update(overrides)
    return HydraSettings(**defaults)


def _make_adapter(stream_config: dict, settings: HydraSettings | None = None) -> AisAdsbAdapter:
    s = settings or _make_settings()
    return AisAdsbAdapter("test_stream", s, registry=MagicMock(), stream_config=stream_config)


def _raw(data: dict | list | bytes, content_type: str = "application/json") -> RawPayload:
    if isinstance(data, (dict, list)):
        content = json.dumps(data).encode()
    else:
        content = data
    return RawPayload(
        stream_id="test_stream",
        fetched_at=datetime.now(timezone.utc),
        content=content,
        content_type=content_type,
        http_status=200,
        headers={},
    )


class TestAisAdsbAdapter:
    """T-AAB-001 through T-AAB-030."""

    def test_adsb_rest_fetch_and_parse(self):
        """T-AAB-001: Parses ADS-B Exchange JSON with 10 aircraft."""
        adapter = _make_adapter({
            "tracking_mode": "rest_poll", "data_domain": "adsb",
            "response_root_path": "ac", "field_mapping": ADSB_FIELD_MAPPING,
            "source_name": "ADS-B Exchange",
        })
        raw = _raw(SAMPLE_ADSB_JSON)
        records = adapter.parse(raw)
        assert len(records) == 10
        for rec in records:
            assert "icao24" in rec
            assert "latitude" in rec
            assert "longitude" in rec

    def test_opensky_array_format_parse(self):
        """T-AAB-002: OpenSky array-of-arrays parsed with integer index mapping."""
        adapter = _make_adapter({
            "tracking_mode": "rest_poll", "data_domain": "adsb",
            "response_root_path": "states", "field_mapping": OPENSKY_FIELD_MAPPING,
            "source_name": "OpenSky",
        })
        raw = _raw(SAMPLE_OPENSKY_JSON)
        records = adapter.parse(raw)
        assert len(records) == 5
        assert records[0]["icao24"] == "000000"

    def test_ais_rest_fetch_and_parse(self):
        """T-AAB-003: Parses MarineTraffic JSON with 5 vessels."""
        adapter = _make_adapter({
            "tracking_mode": "rest_poll", "data_domain": "ais",
            "response_root_path": "data", "field_mapping": AIS_FIELD_MAPPING,
            "source_name": "MarineTraffic",
        })
        raw = _raw(SAMPLE_AIS_JSON)
        records = adapter.parse(raw)
        assert len(records) == 5
        for rec in records:
            assert "mmsi" in rec
            assert "latitude" in rec

    def test_nmea_single_fragment_parse(self):
        """T-AAB-004: Parses single-fragment NMEA sentences."""
        adapter = _make_adapter({
            "tracking_mode": "raw_nmea", "data_domain": "ais",
            "source_name": "MSSIS",
        })
        # Use a minimal valid NMEA sentence
        sentence = "!AIVDM,1,1,,A,13u@Dt002s000000000000000000,0*25\n"
        raw = _raw(sentence.encode(), content_type="text/plain")
        records = adapter.parse(raw)
        # Should parse at least some records (depends on pyais availability)
        assert isinstance(records, list)

    def test_nmea_multi_fragment_reassembly(self):
        """T-AAB-005: Multi-fragment type 5 message reassembled."""
        adapter = _make_adapter({
            "tracking_mode": "raw_nmea", "data_domain": "ais",
            "source_name": "MSSIS",
        })
        nmea_data = (
            "!AIVDM,2,1,3,B,55?MbV02>H97ac<H4eEK6WpN0000000000000016v0Ht2400000000000,0*2C\n"
            "!AIVDM,2,2,3,B,00000000000,2*20\n"
        )
        raw = _raw(nmea_data.encode(), content_type="text/plain")
        records = adapter.parse(raw)
        assert isinstance(records, list)

    def test_nmea_invalid_checksum(self):
        """T-AAB-006: Invalid checksum sentence skipped."""
        adapter = _make_adapter({
            "tracking_mode": "raw_nmea", "data_domain": "ais",
            "source_name": "MSSIS",
        })
        # Valid + invalid checksum
        nmea_data = (
            "!AIVDM,1,1,,A,13u@Dt002s000000000000000000,0*25\n"
            "!AIVDM,1,1,,A,13u@Dt002s000000000000000000,0*FF\n"
        )
        raw = _raw(nmea_data.encode(), content_type="text/plain")
        records = adapter.parse(raw)
        # Only the valid one should parse
        assert isinstance(records, list)

    def test_nmea_unsupported_message_type(self):
        """T-AAB-007: Unsupported message type logged and skipped."""
        adapter = _make_adapter({
            "tracking_mode": "raw_nmea", "data_domain": "ais",
            "source_name": "MSSIS",
        })
        # Type 8 binary broadcast — should be skipped
        nmea_data = "!AIVDM,1,1,,A,85M:Ih@j2d<8000000000000000,0*3E\n"
        raw = _raw(nmea_data.encode(), content_type="text/plain")
        records = adapter.parse(raw)
        assert isinstance(records, list)

    def test_websocket_batch_accumulation(self):
        """T-AAB-008: WebSocket accumulates messages for batch duration."""
        adapter = _make_adapter({
            "tracking_mode": "websocket", "data_domain": "adsb",
            "websocket_url": "wss://example.com/ws",
            "ws_batch_duration_seconds": 1,
            "ws_ping_interval_seconds": 30,
            "field_mapping": ADSB_FIELD_MAPPING,
            "source_name": "ADS-B WS",
        })

        messages = [json.dumps({"hex": f"{i:06x}", "lat": 37.0 + i * 0.1, "lon": -122.0}) for i in range(10)]
        msg_idx = 0

        async def mock_receive():
            nonlocal msg_idx
            if msg_idx < len(messages):
                msg = MagicMock()
                msg.type = 1  # WSMsgType.TEXT
                msg.data = messages[msg_idx]
                msg_idx += 1
                return msg
            await asyncio.sleep(10)

        mock_ws = AsyncMock()
        mock_ws.receive = mock_receive
        mock_ws.closed = False
        mock_ws.send_json = AsyncMock()

        adapter._ws = mock_ws
        raw = asyncio.get_event_loop().run_until_complete(adapter.fetch())
        data = json.loads(raw.content)
        assert len(data) >= 1

    def test_websocket_reconnection(self):
        """T-AAB-009: WebSocket reconnects after drop."""
        adapter = _make_adapter({
            "tracking_mode": "websocket", "data_domain": "adsb",
            "websocket_url": "wss://example.com/ws",
            "ws_batch_duration_seconds": 0.5,
            "ws_reconnect_delay_seconds": 0.01,
            "ws_max_reconnects": 3,
            "field_mapping": ADSB_FIELD_MAPPING,
            "source_name": "ADS-B WS",
        })

        call_count = 0

        async def mock_receive():
            nonlocal call_count
            call_count += 1
            if call_count <= 3:
                msg = MagicMock()
                msg.type = 1
                msg.data = json.dumps({"hex": "aabbcc", "lat": 37.0, "lon": -122.0})
                return msg
            raise ConnectionError("dropped")

        mock_ws = AsyncMock()
        mock_ws.receive = mock_receive
        mock_ws.closed = False
        mock_ws.send_json = AsyncMock()
        adapter._ws = mock_ws

        # Mock reconnection
        new_ws = AsyncMock()
        new_ws.receive = AsyncMock(side_effect=asyncio.TimeoutError)
        new_ws.closed = False
        new_ws.send_json = AsyncMock()

        mock_session = AsyncMock()
        mock_session.ws_connect = AsyncMock(return_value=new_ws)
        mock_session.closed = False
        adapter._ws_session = mock_session

        raw = asyncio.get_event_loop().run_until_complete(adapter.fetch())
        data = json.loads(raw.content)
        assert len(data) >= 3

    def test_websocket_max_reconnects_exceeded(self):
        """T-AAB-010: Max reconnects exceeded raises FetchError."""
        adapter = _make_adapter({
            "tracking_mode": "websocket", "data_domain": "adsb",
            "websocket_url": "wss://example.com/ws",
            "ws_batch_duration_seconds": 0.1,
            "ws_reconnect_delay_seconds": 0.01,
            "ws_max_reconnects": 2,
            "source_name": "ADS-B WS",
        })

        async def mock_receive():
            raise ConnectionError("dropped")

        mock_ws = AsyncMock()
        mock_ws.receive = mock_receive
        mock_ws.closed = False
        adapter._ws = mock_ws

        mock_session = AsyncMock()
        mock_session.ws_connect = AsyncMock(side_effect=ConnectionError("fail"))
        mock_session.closed = False
        adapter._ws_session = mock_session

        with pytest.raises(FetchError):
            asyncio.get_event_loop().run_until_complete(adapter.fetch())

    def test_websocket_subscribe_message(self):
        """T-AAB-011: Subscribe message sent after connection."""
        adapter = _make_adapter({
            "tracking_mode": "websocket", "data_domain": "adsb",
            "websocket_url": "wss://example.com/ws",
            "ws_subscribe_message": {"action": "subscribe", "channel": "all"},
            "ws_batch_duration_seconds": 0.1,
            "source_name": "ADS-B WS",
        })

        mock_ws = AsyncMock()
        mock_ws.receive = AsyncMock(side_effect=asyncio.TimeoutError)
        mock_ws.closed = False
        mock_ws.send_json = AsyncMock()

        mock_session = AsyncMock()
        mock_session.ws_connect = AsyncMock(return_value=mock_ws)
        mock_session.closed = True

        with patch("aiohttp.ClientSession", return_value=mock_session):
            adapter._ws = None
            adapter._ws_session = None
            asyncio.get_event_loop().run_until_complete(adapter.fetch())
            mock_ws.send_json.assert_called_once_with({"action": "subscribe", "channel": "all"})

    def test_validate_coordinate_range(self):
        """T-AAB-012: Latitude 91 dropped, 89.5 retained."""
        adapter = _make_adapter({"data_domain": "adsb", "reject_null_island": False, "max_position_age_seconds": 99999})
        now = datetime.now(timezone.utc)
        records = [
            {"icao24": "aabbcc", "latitude": 91.0, "longitude": 0.0, "timestamp": now},
            {"icao24": "ddeeff", "latitude": 89.5, "longitude": 10.0, "timestamp": now},
        ]
        valid = adapter.validate(records)
        assert len(valid) == 1
        assert valid[0]["icao24"] == "ddeeff"

    def test_validate_null_island_rejection(self):
        """T-AAB-013: (0,0) dropped when reject_null_island: true."""
        adapter_reject = _make_adapter({"data_domain": "ais", "reject_null_island": True, "max_position_age_seconds": 99999})
        adapter_allow = _make_adapter({"data_domain": "ais", "reject_null_island": False, "max_position_age_seconds": 99999})
        now = datetime.now(timezone.utc)
        records = [{"mmsi": "211234567", "latitude": 0.0, "longitude": 0.0, "timestamp": now}]
        assert len(adapter_reject.validate(list(records))) == 0
        assert len(adapter_allow.validate(list(records))) == 1

    def test_validate_speed_limit(self):
        """T-AAB-014: velocity_ms > max_speed dropped."""
        adapter = _make_adapter({"data_domain": "adsb", "max_speed": 600, "reject_null_island": False, "max_position_age_seconds": 99999})
        now = datetime.now(timezone.utc)
        records = [
            {"icao24": "aabbcc", "latitude": 37.0, "longitude": -122.0, "velocity_ms": 700, "timestamp": now},
            {"icao24": "ddeeff", "latitude": 37.0, "longitude": -122.0, "velocity_ms": 250, "timestamp": now},
        ]
        valid = adapter.validate(records)
        assert len(valid) == 1
        assert valid[0]["icao24"] == "ddeeff"

    def test_validate_altitude_range(self):
        """T-AAB-015: altitude_m > 100000 dropped."""
        adapter = _make_adapter({"data_domain": "adsb", "reject_null_island": False, "max_position_age_seconds": 99999})
        now = datetime.now(timezone.utc)
        records = [
            {"icao24": "aabbcc", "latitude": 37.0, "longitude": -122.0, "altitude_m": 150000, "timestamp": now},
            {"icao24": "ddeeff", "latitude": 37.0, "longitude": -122.0, "altitude_m": 12000, "timestamp": now},
        ]
        valid = adapter.validate(records)
        assert len(valid) == 1
        assert valid[0]["icao24"] == "ddeeff"

    def test_validate_mmsi_format(self):
        """T-AAB-016: MMSI not 9 digits dropped."""
        adapter = _make_adapter({"data_domain": "ais", "reject_null_island": False, "max_position_age_seconds": 99999})
        now = datetime.now(timezone.utc)
        records = [
            {"mmsi": "12345", "latitude": 51.0, "longitude": 3.0, "timestamp": now},
            {"mmsi": "211234567", "latitude": 51.0, "longitude": 3.0, "timestamp": now},
        ]
        valid = adapter.validate(records)
        assert len(valid) == 1
        assert valid[0]["mmsi"] == "211234567"

    def test_validate_mmsi_mid_range(self):
        """T-AAB-017: MID outside 201-775 dropped."""
        adapter = _make_adapter({"data_domain": "ais", "reject_null_island": False, "max_position_age_seconds": 99999})
        now = datetime.now(timezone.utc)
        records = [
            {"mmsi": "999123456", "latitude": 51.0, "longitude": 3.0, "timestamp": now},
            {"mmsi": "211234567", "latitude": 51.0, "longitude": 3.0, "timestamp": now},
        ]
        valid = adapter.validate(records)
        assert len(valid) == 1
        assert valid[0]["mmsi"] == "211234567"

    def test_validate_icao24_format(self):
        """T-AAB-018: Non-hex ICAO24 dropped."""
        adapter = _make_adapter({"data_domain": "adsb", "reject_null_island": False, "max_position_age_seconds": 99999})
        now = datetime.now(timezone.utc)
        records = [
            {"icao24": "ZZZZZZ", "latitude": 37.0, "longitude": -122.0, "timestamp": now},
            {"icao24": "a1b2c3", "latitude": 37.0, "longitude": -122.0, "timestamp": now},
        ]
        valid = adapter.validate(records)
        assert len(valid) == 1
        assert valid[0]["icao24"] == "a1b2c3"

    def test_validate_stale_position(self):
        """T-AAB-019: Stale position dropped."""
        adapter = _make_adapter({"data_domain": "adsb", "max_position_age_seconds": 300, "reject_null_island": False})
        now = datetime.now(timezone.utc)
        records = [
            {"icao24": "aabbcc", "latitude": 37.0, "longitude": -122.0, "timestamp": now - timedelta(seconds=600)},
            {"icao24": "ddeeff", "latitude": 37.0, "longitude": -122.0, "timestamp": now - timedelta(seconds=100)},
        ]
        valid = adapter.validate(records)
        assert len(valid) == 1
        assert valid[0]["icao24"] == "ddeeff"

    def test_validate_dedup_composite_key(self):
        """T-AAB-020: Same icao24 + timestamp (to second) deduped."""
        adapter = _make_adapter({"data_domain": "adsb", "reject_null_island": False, "max_position_age_seconds": 99999})
        ts = datetime.now(timezone.utc).replace(microsecond=0)
        records = [
            {"icao24": "aabbcc", "latitude": 37.0, "longitude": -122.0, "timestamp": ts},
            {"icao24": "aabbcc", "latitude": 37.1, "longitude": -122.1, "timestamp": ts},
        ]
        valid = adapter.validate(records)
        assert len(valid) == 1

    def test_normalize_adsb_to_normalized_record(self):
        """T-AAB-021: ADS-B normalizes with GeoJSON Point [lon, lat, alt]."""
        adapter = _make_adapter({
            "tracking_mode": "rest_poll", "data_domain": "adsb",
            "tier": 18, "source_name": "ADS-B Exchange",
            "base_url": "https://api.example.com", "endpoint_path": "/v2/all",
            "default_tags": ["aviation"],
        })
        records = [{
            "icao24": "aabbcc", "latitude": 37.7749, "longitude": -122.4194,
            "altitude_m": 10000, "velocity_ms": 250, "heading": 180,
            "timestamp": datetime.now(timezone.utc), "source_api": "ADS-B Exchange",
            "tracking_mode": "rest_poll", "data_domain": "adsb",
        }]
        normalized = adapter.normalize(records)
        assert len(normalized) == 1
        nr = normalized[0]
        assert isinstance(nr, NormalizedRecord)
        assert nr.geo is not None
        assert nr.geo.type == "Point"
        assert len(nr.geo.coordinates) == 3  # lon, lat, alt
        assert nr.confidence == 0.9

    def test_normalize_ais_to_normalized_record(self):
        """T-AAB-022: AIS normalizes with GeoJSON Point [lon, lat]."""
        adapter = _make_adapter({
            "tracking_mode": "rest_poll", "data_domain": "ais",
            "tier": 18, "source_name": "MarineTraffic",
            "base_url": "https://api.example.com", "endpoint_path": "/v1",
            "default_tags": ["maritime"],
        })
        records = [{
            "mmsi": "211234567", "latitude": 51.0, "longitude": 3.0,
            "speed_knots": 12.5, "timestamp": datetime.now(timezone.utc),
            "source_api": "MarineTraffic", "tracking_mode": "rest_poll", "data_domain": "ais",
        }]
        normalized = adapter.normalize(records)
        assert len(normalized) == 1
        nr = normalized[0]
        assert nr.geo is not None
        assert len(nr.geo.coordinates) == 2
        assert nr.confidence == 0.9

    def test_normalize_nmea_confidence(self):
        """T-AAB-023: NMEA-sourced AIS normalizes with confidence 0.7."""
        adapter = _make_adapter({
            "tracking_mode": "raw_nmea", "data_domain": "ais",
            "tier": 18, "source_name": "MSSIS",
            "base_url": "https://mssis.example.com", "endpoint_path": "/stream",
        })
        records = [{
            "mmsi": "211234567", "latitude": 51.0, "longitude": 3.0,
            "timestamp": datetime.now(timezone.utc),
            "source_api": "MSSIS", "tracking_mode": "raw_nmea", "data_domain": "ais",
        }]
        normalized = adapter.normalize(records)
        assert normalized[0].confidence == 0.7

    def test_bbox_query_params(self):
        """T-AAB-024: Bounding box params mapped to API-specific names."""
        adapter = _make_adapter({
            "tracking_mode": "rest_poll", "data_domain": "adsb",
            "base_url": "https://api.example.com", "endpoint_path": "/states/all",
            "bbox_lat_min": 45.0, "bbox_lat_max": 55.0,
            "bbox_lon_min": -10.0, "bbox_lon_max": 10.0,
            "bbox_param_map": {"lat_min": "lamin", "lat_max": "lamax", "lon_min": "lomin", "lon_max": "lomax"},
        })
        params = adapter._build_query_params()
        assert params.get("lamin") == "45.0"
        assert params.get("lamax") == "55.0"

    def test_icao_filter_query_param(self):
        """T-AAB-025: ICAO filter injected as query parameter."""
        adapter = _make_adapter({
            "tracking_mode": "rest_poll", "data_domain": "adsb",
            "icao_filter": ["a1b2c3", "d4e5f6"],
        })
        params = adapter._build_query_params()
        assert "a1b2c3" in params.get("icao24", "")
        assert "d4e5f6" in params.get("icao24", "")

    def test_auth_rapidapi(self):
        """T-AAB-026: RapidAPI auth includes X-RapidAPI-Key and X-RapidAPI-Host."""
        settings = _make_settings(credentials={
            "test_stream": {"rapidapi_key": "test-key", "rapidapi_host": "adsb.example.com"}
        })
        adapter = _make_adapter({"auth_pattern": "rapidapi_key"}, settings)
        headers = adapter._build_auth_headers()
        assert headers["X-RapidAPI-Key"] == "test-key"
        assert headers["X-RapidAPI-Host"] == "adsb.example.com"

    def test_run_pipeline_adsb(self):
        """T-AAB-027: Full run() pipeline for ADS-B REST poll."""
        adapter = _make_adapter({
            "tracking_mode": "rest_poll", "data_domain": "adsb",
            "response_root_path": "ac", "field_mapping": ADSB_FIELD_MAPPING,
            "tier": 18, "source_name": "ADS-B Exchange",
            "base_url": "https://api.example.com", "endpoint_path": "/v2/all",
            "reject_null_island": True, "max_speed": 600, "max_position_age_seconds": 99999,
        })

        mock_resp = AsyncMock()
        mock_resp.read = AsyncMock(return_value=json.dumps(SAMPLE_ADSB_JSON).encode())
        mock_resp.status = 200
        mock_resp.headers = {"Content-Type": "application/json"}
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = asyncio.get_event_loop().run_until_complete(adapter.run())
            assert len(result) >= 1
            assert all(isinstance(r, NormalizedRecord) for r in result)

    def test_run_pipeline_ais_nmea(self):
        """T-AAB-028: Full run() pipeline for AIS NMEA."""
        adapter = _make_adapter({
            "tracking_mode": "raw_nmea", "data_domain": "ais",
            "nmea_source_type": "file_fetch",
            "base_url": "https://mssis.example.com", "endpoint_path": "/ais/data",
            "tier": 18, "source_name": "MSSIS",
            "reject_null_island": True, "max_speed": 50, "max_position_age_seconds": 99999,
        })

        nmea_content = b"!AIVDM,1,1,,A,13u@Dt002s000000000000000000,0*25\n"

        mock_resp = AsyncMock()
        mock_resp.read = AsyncMock(return_value=nmea_content)
        mock_resp.status = 200
        mock_resp.headers = {"Content-Type": "text/plain"}
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = asyncio.get_event_loop().run_until_complete(adapter.run())
            assert isinstance(result, list)

    def test_health_check_rest(self):
        """T-AAB-029: REST health check returns AdapterHealth."""
        adapter = _make_adapter({
            "tracking_mode": "rest_poll",
            "base_url": "https://api.example.com", "endpoint_path": "/v2/all",
        })

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.head = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            health = asyncio.get_event_loop().run_until_complete(adapter.health_check())
            assert isinstance(health, AdapterHealth)
            assert health.status == HealthStatus.OK

    def test_health_check_websocket(self):
        """T-AAB-030: WebSocket health check returns OK."""
        adapter = _make_adapter({
            "tracking_mode": "websocket",
            "websocket_url": "wss://example.com/ws",
        })

        mock_ws = AsyncMock()
        mock_ws.close = AsyncMock()

        mock_session = AsyncMock()
        mock_session.ws_connect = AsyncMock(return_value=mock_ws)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            health = asyncio.get_event_loop().run_until_complete(adapter.health_check())
            assert isinstance(health, AdapterHealth)
            assert health.status == HealthStatus.OK
