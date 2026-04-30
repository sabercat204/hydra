"""Unit tests for FdsnAdapter — all network calls mocked."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import orjson
import pytest

from hydra.adapters.base import RawPayload
from hydra.adapters.exceptions import FetchError, ParseError
from hydra.adapters.fdsn import FdsnAdapter
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

QUAKEML_3_EVENTS = b"""<?xml version="1.0" encoding="UTF-8"?>
<q:quakeml xmlns:q="http://quakeml.org/xmlns/quakeml/1.2"
           xmlns="http://quakeml.org/xmlns/bed/1.2">
  <eventParameters>
    <event publicID="quakeml:us.anss.org/event/us7000abc1">
      <description>
        <text>10km NE of Ridgecrest, CA</text>
      </description>
      <origin>
        <time><value>2024-06-15T12:30:00.000Z</value></time>
        <latitude><value>35.8</value></latitude>
        <longitude><value>-117.5</value></longitude>
        <depth><value>10000</value></depth>
      </origin>
      <magnitude>
        <mag><value>5.2</value></mag>
        <type>mw</type>
      </magnitude>
      <creationInfo>
        <agencyID>us</agencyID>
      </creationInfo>
    </event>
    <event publicID="quakeml:us.anss.org/event/us7000abc2">
      <description>
        <text>5km S of Pahala, Hawaii</text>
      </description>
      <origin>
        <time><value>2024-06-15T13:00:00.000Z</value></time>
        <latitude><value>19.2</value></latitude>
        <longitude><value>-155.2</value></longitude>
        <depth><value>30000</value></depth>
      </origin>
      <magnitude>
        <mag><value>3.1</value></mag>
        <type>ml</type>
      </magnitude>
      <creationInfo>
        <agencyID>us</agencyID>
      </creationInfo>
    </event>
    <event publicID="quakeml:us.anss.org/event/us7000abc3">
      <description>
        <text>15km W of Tokyo, Japan</text>
      </description>
      <origin>
        <time><value>2024-06-15T14:00:00.000Z</value></time>
        <latitude><value>35.6</value></latitude>
        <longitude><value>139.7</value></longitude>
        <depth><value>50000</value></depth>
      </origin>
      <magnitude>
        <mag><value>4.5</value></mag>
        <type>mb</type>
      </magnitude>
      <creationInfo>
        <agencyID>jma</agencyID>
      </creationInfo>
    </event>
  </eventParameters>
</q:quakeml>"""

STATIONXML_RESPONSE = b"""<?xml version="1.0" encoding="UTF-8"?>
<FDSNStationXML xmlns="http://www.fdsn.org/xml/station/1" schemaVersion="1.2">
  <Network code="IU">
    <Station code="ANMO">
      <Latitude>34.9459</Latitude>
      <Longitude>-106.4572</Longitude>
      <Elevation>1850.0</Elevation>
      <Channel code="BHZ" locationCode="00" startDate="2002-11-19T21:07:00" endDate="">
        <Latitude>34.9459</Latitude>
        <Longitude>-106.4572</Longitude>
        <Elevation>1850.0</Elevation>
        <SampleRate>40.0</SampleRate>
      </Channel>
      <Channel code="BHN" locationCode="00" startDate="2002-11-19T21:07:00" endDate="">
        <Latitude>34.9459</Latitude>
        <Longitude>-106.4572</Longitude>
        <Elevation>1850.0</Elevation>
        <SampleRate>40.0</SampleRate>
      </Channel>
    </Station>
    <Station code="CCM">
      <Latitude>38.0557</Latitude>
      <Longitude>-91.2446</Longitude>
      <Elevation>222.0</Elevation>
      <Channel code="BHZ" locationCode="00" startDate="2005-01-01T00:00:00" endDate="">
        <Latitude>38.0557</Latitude>
        <Longitude>-91.2446</Longitude>
        <Elevation>222.0</Elevation>
        <SampleRate>20.0</SampleRate>
      </Channel>
    </Station>
  </Network>
</FDSNStationXML>"""


def _make_registry() -> StreamRegistry:
    src = StreamSource(
        name="iris_fdsn", url="https://service.iris.edu", format="xml", auth="none", notes=""
    )
    tier = StreamTier(
        id=1, name="Geophysical & Seismic", streams=1, access="5G",
        formats=["xml", "miniseed"], cadence="sub_minute", adapter="fdsn", fallback="rest_json",
        sources=[src],
    )
    return StreamRegistry(tiers={1: tier})


def _make_adapter(
    stream_config: dict[str, Any] | None = None,
    settings: HydraSettings | None = None,
) -> FdsnAdapter:
    cfg: dict[str, Any] = {
        "base_url": "https://service.iris.edu",
        "fdsn_service": "event",
        "fdsn_query_params": {
            "starttime": "2024-06-15T00:00:00",
            "endtime": "2024-06-16T00:00:00",
            "minmagnitude": 3.0,
        },
        "max_bisect_depth": 3,
        **(stream_config or {}),
    }
    return FdsnAdapter(
        stream_id="iris_fdsn",
        settings=settings or HydraSettings(),
        registry=_make_registry(),
        stream_config=cfg,
    )


def _mock_response(
    body: bytes,
    status: int = 200,
    content_type: str = "application/xml",
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
# Tests: event service (QuakeML)
# ---------------------------------------------------------------------------


class TestEventService:
    async def test_parse_quakeml_3_events(self) -> None:
        """Mock IRIS QuakeML response with 3 earthquakes, verify 3 dicts extracted."""
        adapter = _make_adapter()
        resp = _mock_response(QUAKEML_3_EVENTS)
        session = _mock_session([resp])

        with patch("hydra.adapters.fdsn.aiohttp.ClientSession", return_value=session):
            raw = await adapter.fetch()

        records = adapter.parse(raw)
        assert len(records) == 3

        # Verify first event
        assert records[0]["publicID"] == "quakeml:us.anss.org/event/us7000abc1"
        assert records[0]["latitude"] == 35.8
        assert records[0]["longitude"] == -117.5
        assert records[0]["magnitude"] == 5.2
        assert records[0]["description"] == "10km NE of Ridgecrest, CA"

        # Verify third event
        assert records[2]["publicID"] == "quakeml:us.anss.org/event/us7000abc3"
        assert records[2]["magnitude"] == 4.5


# ---------------------------------------------------------------------------
# Tests: station service (StationXML)
# ---------------------------------------------------------------------------


class TestStationService:
    async def test_parse_stationxml_channels(self) -> None:
        """Mock StationXML with network → station → channel hierarchy, verify flat channel dicts."""
        adapter = _make_adapter(stream_config={"fdsn_service": "station"})
        resp = _mock_response(STATIONXML_RESPONSE)
        session = _mock_session([resp])

        with patch("hydra.adapters.fdsn.aiohttp.ClientSession", return_value=session):
            raw = await adapter.fetch()

        records = adapter.parse(raw)
        assert len(records) == 3  # 2 channels from ANMO + 1 from CCM

        # Verify first channel
        assert records[0]["network_code"] == "IU"
        assert records[0]["station_code"] == "ANMO"
        assert records[0]["channel_code"] == "BHZ"
        assert records[0]["location_code"] == "00"
        assert records[0]["latitude"] == 34.9459
        assert records[0]["longitude"] == -106.4572
        assert records[0]["sample_rate"] == 40.0

        # Verify CCM station channel
        assert records[2]["station_code"] == "CCM"
        assert records[2]["sample_rate"] == 20.0


# ---------------------------------------------------------------------------
# Tests: dataselect service (miniSEED)
# ---------------------------------------------------------------------------


class TestDataselectService:
    async def test_parse_miniseed_header_only(self) -> None:
        """Mock miniSEED binary, verify header-only metadata extraction."""
        adapter = _make_adapter(stream_config={
            "fdsn_service": "dataselect",
            "parser": "miniseed_header_only",
        })

        # Create a minimal mock miniSEED-like binary with recognizable header fields
        # We'll mock the obspy import to return controlled data
        mock_trace_1 = MagicMock()
        mock_trace_1.stats.network = "IU"
        mock_trace_1.stats.station = "ANMO"
        mock_trace_1.stats.channel = "BHZ"
        mock_trace_1.stats.location = "00"
        mock_trace_1.stats.starttime = "2024-06-15T00:00:00"
        mock_trace_1.stats.endtime = "2024-06-15T01:00:00"
        mock_trace_1.stats.sampling_rate = 40.0
        mock_trace_1.stats.npts = 144000

        mock_stream = MagicMock()
        mock_stream.__iter__ = MagicMock(return_value=iter([mock_trace_1]))

        mock_obspy = MagicMock()
        mock_obspy.read.return_value = mock_stream

        # Mock the fetch
        fake_mseed = b"\x00" * 512  # Minimal binary content
        resp = _mock_response(fake_mseed, content_type="application/vnd.fdsn.mseed")
        session = _mock_session([resp])

        with patch("hydra.adapters.fdsn.aiohttp.ClientSession", return_value=session):
            raw = await adapter.fetch()

        with patch.dict("sys.modules", {"obspy": mock_obspy}):
            records = adapter.parse(raw)

        assert len(records) == 1
        assert records[0]["network"] == "IU"
        assert records[0]["station"] == "ANMO"
        assert records[0]["channel"] == "BHZ"
        assert records[0]["sample_rate"] == 40.0
        assert records[0]["num_samples"] == 144000


# ---------------------------------------------------------------------------
# Tests: HTTP 204 handling
# ---------------------------------------------------------------------------


class TestHttp204:
    async def test_204_returns_empty(self) -> None:
        """Verify empty list returned on HTTP 204, no error raised."""
        adapter = _make_adapter()
        resp = _mock_response(b"", status=204)
        session = _mock_session([resp])

        with patch("hydra.adapters.fdsn.aiohttp.ClientSession", return_value=session):
            raw = await adapter.fetch()

        assert raw.content == b""
        records = adapter.parse(raw)
        assert records == []


# ---------------------------------------------------------------------------
# Tests: HTTP 413 bisection
# ---------------------------------------------------------------------------


class TestHttp413Bisection:
    async def test_413_bisects_and_merges(self) -> None:
        """Mock 413 on full time range, 200 on each half, verify both halves fetched."""
        adapter = _make_adapter(stream_config={
            "fdsn_query_params": {
                "starttime": "2024-06-15T00:00:00+00:00",
                "endtime": "2024-06-16T00:00:00+00:00",
                "minmagnitude": 3.0,
            },
            "max_bisect_depth": 3,
        })

        # First call returns 413, next two return 200 with QuakeML
        quakeml_half1 = b"""<?xml version="1.0" encoding="UTF-8"?>
<q:quakeml xmlns:q="http://quakeml.org/xmlns/quakeml/1.2"
           xmlns="http://quakeml.org/xmlns/bed/1.2">
  <eventParameters>
    <event publicID="quakeml:half1/event1">
      <origin>
        <time><value>2024-06-15T06:00:00.000Z</value></time>
        <latitude><value>35.0</value></latitude>
        <longitude><value>-117.0</value></longitude>
        <depth><value>10000</value></depth>
      </origin>
      <magnitude><mag><value>4.0</value></mag><type>mw</type></magnitude>
    </event>
  </eventParameters>
</q:quakeml>"""

        quakeml_half2 = b"""<?xml version="1.0" encoding="UTF-8"?>
<q:quakeml xmlns:q="http://quakeml.org/xmlns/quakeml/1.2"
           xmlns="http://quakeml.org/xmlns/bed/1.2">
  <eventParameters>
    <event publicID="quakeml:half2/event1">
      <origin>
        <time><value>2024-06-15T18:00:00.000Z</value></time>
        <latitude><value>36.0</value></latitude>
        <longitude><value>-118.0</value></longitude>
        <depth><value>20000</value></depth>
      </origin>
      <magnitude><mag><value>3.5</value></mag><type>ml</type></magnitude>
    </event>
  </eventParameters>
</q:quakeml>"""

        resp_413 = _mock_response(b"", status=413)
        resp_half1 = _mock_response(quakeml_half1)
        resp_half2 = _mock_response(quakeml_half2)
        session = _mock_session([resp_413, resp_half1, resp_half2])

        with patch("hydra.adapters.fdsn.aiohttp.ClientSession", return_value=session):
            raw = await adapter.fetch()

        assert raw.content != b""
        records = adapter.parse(raw)
        assert len(records) == 2
        public_ids = {r["publicID"] for r in records}
        assert "quakeml:half1/event1" in public_ids
        assert "quakeml:half2/event1" in public_ids


# ---------------------------------------------------------------------------
# Tests: coordinate validation
# ---------------------------------------------------------------------------


class TestCoordinateValidation:
    def test_invalid_latitude_dropped(self) -> None:
        """Inject event with latitude 95.0, verify it is dropped."""
        adapter = _make_adapter()
        records = [
            {
                "publicID": "event1",
                "latitude": 95.0,  # Invalid: > 90
                "longitude": -117.5,
                "depth": 10000,
                "magnitude": 5.0,
            },
            {
                "publicID": "event2",
                "latitude": 35.0,
                "longitude": -117.5,
                "depth": 10000,
                "magnitude": 4.0,
            },
        ]
        valid = adapter.validate(records)
        assert len(valid) == 1
        assert valid[0]["publicID"] == "event2"

    def test_invalid_longitude_dropped(self) -> None:
        adapter = _make_adapter()
        records = [
            {
                "publicID": "event1",
                "latitude": 35.0,
                "longitude": 200.0,  # Invalid: > 180
                "depth": 10000,
                "magnitude": 5.0,
            },
        ]
        valid = adapter.validate(records)
        assert len(valid) == 0

    def test_negative_depth_dropped(self) -> None:
        adapter = _make_adapter()
        records = [
            {
                "publicID": "event1",
                "latitude": 35.0,
                "longitude": -117.5,
                "depth": -100,  # Invalid: negative
                "magnitude": 5.0,
            },
        ]
        valid = adapter.validate(records)
        assert len(valid) == 0


# ---------------------------------------------------------------------------
# Tests: deduplication
# ---------------------------------------------------------------------------


class TestDeduplication:
    def test_duplicate_events_by_publicID(self) -> None:
        """Inject two events with same publicID, verify only one passes."""
        adapter = _make_adapter()
        records = [
            {
                "publicID": "quakeml:us/event1",
                "latitude": 35.0,
                "longitude": -117.5,
                "depth": 10000,
                "magnitude": 5.0,
            },
            {
                "publicID": "quakeml:us/event1",  # Duplicate
                "latitude": 35.0,
                "longitude": -117.5,
                "depth": 10000,
                "magnitude": 5.0,
            },
            {
                "publicID": "quakeml:us/event2",
                "latitude": 36.0,
                "longitude": -118.0,
                "depth": 20000,
                "magnitude": 3.5,
            },
        ]
        valid = adapter.validate(records)
        assert len(valid) == 2
        ids = [r["publicID"] for r in valid]
        assert ids == ["quakeml:us/event1", "quakeml:us/event2"]

    def test_duplicate_miniseed_traces(self) -> None:
        """Dedup miniSEED traces by composite key."""
        adapter = _make_adapter(stream_config={"fdsn_service": "dataselect"})
        records = [
            {
                "network": "IU", "station": "ANMO", "channel": "BHZ", "location": "00",
                "starttime": "2024-06-15T00:00:00", "endtime": "2024-06-15T01:00:00",
                "sample_rate": 40.0, "num_samples": 144000,
            },
            {
                "network": "IU", "station": "ANMO", "channel": "BHZ", "location": "00",
                "starttime": "2024-06-15T00:00:00", "endtime": "2024-06-15T01:00:00",
                "sample_rate": 40.0, "num_samples": 144000,
            },  # Duplicate
        ]
        valid = adapter.validate(records)
        assert len(valid) == 1


# ---------------------------------------------------------------------------
# Tests: station validation
# ---------------------------------------------------------------------------


class TestStationValidation:
    def test_invalid_station_sample_rate(self) -> None:
        """Station with sample_rate <= 0 should be dropped."""
        adapter = _make_adapter(stream_config={"fdsn_service": "station"})
        records = [
            {
                "network_code": "IU", "station_code": "ANMO", "channel_code": "BHZ",
                "location_code": "00", "latitude": 34.9, "longitude": -106.4,
                "elevation": 1850.0, "sample_rate": 0.0,  # Invalid
            },
            {
                "network_code": "IU", "station_code": "CCM", "channel_code": "BHZ",
                "location_code": "00", "latitude": 38.0, "longitude": -91.2,
                "elevation": 222.0, "sample_rate": 20.0,  # Valid
            },
        ]
        valid = adapter.validate(records)
        assert len(valid) == 1
        assert valid[0]["station_code"] == "CCM"


# ---------------------------------------------------------------------------
# Tests: miniSEED validation
# ---------------------------------------------------------------------------


class TestMiniseedValidation:
    def test_invalid_num_samples(self) -> None:
        adapter = _make_adapter(stream_config={"fdsn_service": "dataselect"})
        records = [
            {
                "network": "IU", "station": "ANMO", "channel": "BHZ", "location": "00",
                "starttime": "2024-06-15T00:00:00", "endtime": "2024-06-15T01:00:00",
                "sample_rate": 40.0, "num_samples": 0,  # Invalid
            },
        ]
        valid = adapter.validate(records)
        assert len(valid) == 0

    def test_starttime_after_endtime(self) -> None:
        adapter = _make_adapter(stream_config={"fdsn_service": "dataselect"})
        records = [
            {
                "network": "IU", "station": "ANMO", "channel": "BHZ", "location": "00",
                "starttime": "2024-06-16T00:00:00", "endtime": "2024-06-15T00:00:00",
                "sample_rate": 40.0, "num_samples": 100,
            },
        ]
        valid = adapter.validate(records)
        assert len(valid) == 0
