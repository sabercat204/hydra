"""FDSN seismic network adapter — covers IRIS, GEOFON, CTBTO and all FDSN-compliant data centers.

Supports three FDSN web services: fdsnws-event (QuakeML), fdsnws-station (StationXML),
and fdsnws-dataselect (miniSEED binary).
"""

from __future__ import annotations

import io
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any

import aiohttp
import orjson
import structlog

from hydra.config import HydraSettings
from hydra.registry.stream_registry import StreamRegistry
from hydra.utils.hashing import compute_raw_hash

from .base import BaseAdapter, RawPayload
from .exceptions import FetchError, ParseError, RateLimitError

logger = structlog.get_logger()

# FDSN service path mapping
_SERVICE_PATHS: dict[str, str] = {
    "event": "/fdsnws/event/1/query",
    "station": "/fdsnws/station/1/query",
    "dataselect": "/fdsnws/dataselect/1/query",
}

# QuakeML namespace
_QUAKEML_NS = "http://quakeml.org/xmlns/bed/1.2"
_QML = f"{{{_QUAKEML_NS}}}"


class FdsnAdapter(BaseAdapter):
    """FDSN web service adapter for seismic data."""

    adapter_type: str = "fdsn"

    def __init__(
        self,
        stream_id: str,
        settings: HydraSettings,
        registry: StreamRegistry | None = None,
        *,
        stream_config: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(stream_id, settings, registry)
        self._cfg: dict[str, Any] = stream_config or {}

    # -- helpers ------------------------------------------------------------

    def _get_cfg(self, key: str, default: Any = None) -> Any:
        return self._cfg.get(key, default)

    # -- fetch --------------------------------------------------------------

    async def fetch(self) -> RawPayload:
        """Fetch data from an FDSN web service, with HTTP 413 bisection support."""
        base_url = self._get_cfg("base_url", "").rstrip("/")
        service = self._get_cfg("fdsn_service", "event")
        path = _SERVICE_PATHS.get(service, _SERVICE_PATHS["event"])
        url = f"{base_url}{path}"

        query_params = dict(self._get_cfg("fdsn_query_params", {}) or {})

        # Set format parameter
        if service in ("event", "station"):
            query_params.setdefault("format", "xml")
        elif service == "dataselect":
            query_params.setdefault("format", "miniseed")

        max_bisect = self._get_cfg("max_bisect_depth", 3)

        timeout = aiohttp.ClientTimeout(total=getattr(self.settings, "http_timeout_seconds", 60))

        async with aiohttp.ClientSession(timeout=timeout) as session:
            payloads = await self._fetch_with_bisection(
                session, url, query_params, max_bisect, depth=0,
            )

        if not payloads:
            return RawPayload(
                stream_id=self.stream_id,
                fetched_at=datetime.now(timezone.utc),
                content=b"",
                content_type="application/xml",
                http_status=204,
            )

        if len(payloads) == 1:
            return payloads[0]

        # Multiple payloads from bisection — wrap in a JSON envelope so parse
        # can handle each XML document independently.
        envelope = orjson.dumps({
            "_bisected": True,
            "_payloads": [p.content.decode("utf-8", errors="replace") for p in payloads],
        })
        return RawPayload(
            stream_id=self.stream_id,
            fetched_at=payloads[0].fetched_at,
            content=envelope,
            content_type="application/json+bisected",
            http_status=payloads[0].http_status,
            headers=payloads[0].headers,
        )

    async def _fetch_with_bisection(
        self,
        session: aiohttp.ClientSession,
        url: str,
        params: dict[str, Any],
        max_depth: int,
        depth: int,
    ) -> list[RawPayload]:
        """Fetch with recursive time-window bisection on HTTP 413."""
        headers = {"User-Agent": "HYDRA/0.1.0"}

        try:
            async with session.get(url, params=params, headers=headers) as resp:
                if resp.status == 204:
                    return []

                if resp.status == 413:
                    if depth >= max_depth:
                        raise FetchError(
                            f"HTTP 413 at max bisect depth {max_depth} for {self.stream_id}",
                            status_code=413,
                        )
                    return await self._bisect_time_window(session, url, params, max_depth, depth)

                if resp.status == 429:
                    retry_after = float(resp.headers.get("Retry-After", "1"))
                    raise RateLimitError(f"Rate limited on {self.stream_id}", retry_after=retry_after)

                if resp.status >= 500:
                    raise FetchError(
                        f"Server error {resp.status} from {self.stream_id}",
                        status_code=resp.status,
                    )

                if resp.status >= 400:
                    raise FetchError(
                        f"Client error {resp.status} from {self.stream_id}",
                        status_code=resp.status,
                    )

                body = await resp.read()
                return [
                    RawPayload(
                        stream_id=self.stream_id,
                        fetched_at=datetime.now(timezone.utc),
                        content=body,
                        content_type=resp.content_type or "application/xml",
                        http_status=resp.status,
                        headers={k: v for k, v in resp.headers.items()},
                    )
                ]
        except aiohttp.ClientError as exc:
            raise FetchError(f"Connection error fetching {self.stream_id}: {exc}") from exc

    async def _bisect_time_window(
        self,
        session: aiohttp.ClientSession,
        url: str,
        params: dict[str, Any],
        max_depth: int,
        depth: int,
    ) -> list[RawPayload]:
        """Split the time window in half and retry both halves."""
        starttime = params.get("starttime", "")
        endtime = params.get("endtime", "")

        if not starttime or not endtime:
            raise FetchError(
                f"Cannot bisect without starttime/endtime for {self.stream_id}",
                status_code=413,
            )

        start_dt = datetime.fromisoformat(str(starttime).replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(str(endtime).replace("Z", "+00:00"))
        mid_dt = start_dt + (end_dt - start_dt) / 2

        mid_str = mid_dt.isoformat()

        self._log.info("bisecting_time_window", depth=depth + 1, midpoint=mid_str)

        # First half
        params_first = {**params, "starttime": str(starttime), "endtime": mid_str}
        first_half = await self._fetch_with_bisection(session, url, params_first, max_depth, depth + 1)

        # Second half
        params_second = {**params, "starttime": mid_str, "endtime": str(endtime)}
        second_half = await self._fetch_with_bisection(session, url, params_second, max_depth, depth + 1)

        return first_half + second_half

    # -- parse --------------------------------------------------------------

    def parse(self, raw: RawPayload) -> list[dict[str, Any]]:
        """Parse FDSN response based on service type."""
        if not raw.content:
            return []

        service = self._get_cfg("fdsn_service", "event")

        # Handle bisected payloads (multiple XML docs from 413 bisection)
        if raw.content_type == "application/json+bisected":
            try:
                envelope = orjson.loads(raw.content)
                all_records: list[dict[str, Any]] = []
                for xml_str in envelope.get("_payloads", []):
                    xml_bytes = xml_str.encode("utf-8")
                    all_records.extend(self._parse_single(xml_bytes, service))
                return all_records
            except Exception as exc:
                raise ParseError(f"Failed to parse bisected envelope for {self.stream_id}: {exc}") from exc

        return self._parse_single(raw.content, service)

    def _parse_single(self, content: bytes, service: str) -> list[dict[str, Any]]:
        """Parse a single FDSN response payload."""
        try:
            if service == "event":
                return self._parse_quakeml(content)
            elif service == "station":
                return self._parse_stationxml(content)
            elif service == "dataselect":
                return self._parse_miniseed(content)
            else:
                raise ParseError(f"Unknown FDSN service: {service}")
        except ParseError:
            raise
        except ET.ParseError as exc:
            raise ParseError(f"XML parse error for {self.stream_id}: {exc}") from exc
        except Exception as exc:
            raise ParseError(f"Parse error for {self.stream_id}: {exc}") from exc

    def _parse_quakeml(self, content: bytes) -> list[dict[str, Any]]:
        """Parse QuakeML XML into event dicts."""
        root = ET.fromstring(content)
        events: list[dict[str, Any]] = []

        # Handle namespace variations
        ns = self._detect_quakeml_ns(root)

        for event_el in root.iter(f"{ns}event"):
            event: dict[str, Any] = {}
            event["publicID"] = event_el.get("publicID", "")

            # Description
            desc_el = event_el.find(f"{ns}description")
            if desc_el is not None:
                text_el = desc_el.find(f"{ns}text")
                event["description"] = text_el.text if text_el is not None and text_el.text else ""
            else:
                event["description"] = ""

            # Origin
            origin_el = event_el.find(f"{ns}origin")
            if origin_el is not None:
                time_el = origin_el.find(f"{ns}time")
                if time_el is not None:
                    value_el = time_el.find(f"{ns}value")
                    event["time"] = value_el.text if value_el is not None and value_el.text else ""

                lat_el = origin_el.find(f"{ns}latitude")
                if lat_el is not None:
                    value_el = lat_el.find(f"{ns}value")
                    event["latitude"] = float(value_el.text) if value_el is not None and value_el.text else None

                lon_el = origin_el.find(f"{ns}longitude")
                if lon_el is not None:
                    value_el = lon_el.find(f"{ns}value")
                    event["longitude"] = float(value_el.text) if value_el is not None and value_el.text else None

                depth_el = origin_el.find(f"{ns}depth")
                if depth_el is not None:
                    value_el = depth_el.find(f"{ns}value")
                    event["depth"] = float(value_el.text) if value_el is not None and value_el.text else None

            # Magnitude
            mag_el = event_el.find(f"{ns}magnitude")
            if mag_el is not None:
                mag_val_el = mag_el.find(f"{ns}mag")
                if mag_val_el is not None:
                    value_el = mag_val_el.find(f"{ns}value")
                    event["magnitude"] = float(value_el.text) if value_el is not None and value_el.text else None
                type_el = mag_el.find(f"{ns}type")
                event["magnitude_type"] = type_el.text if type_el is not None and type_el.text else ""

            # CreationInfo
            creation_el = event_el.find(f"{ns}creationInfo")
            if creation_el is not None:
                agency_el = creation_el.find(f"{ns}agencyID")
                event["creation_agency"] = agency_el.text if agency_el is not None and agency_el.text else ""

            events.append(event)

        return events

    @staticmethod
    def _detect_quakeml_ns(root: ET.Element) -> str:
        """Detect the QuakeML namespace from the root element."""
        tag = root.tag
        if tag.startswith("{"):
            ns = tag[: tag.index("}") + 1]
            # The events are under the bed namespace, try to find it
            for el in root.iter():
                if "event" in el.tag:
                    event_ns = el.tag[: el.tag.index("}") + 1] if el.tag.startswith("{") else ""
                    return event_ns
            return ns
        return f"{{{_QUAKEML_NS}}}"

    def _parse_stationxml(self, content: bytes) -> list[dict[str, Any]]:
        """Parse StationXML into flat channel dicts."""
        root = ET.fromstring(content)
        channels: list[dict[str, Any]] = []

        # Detect namespace
        ns = ""
        tag = root.tag
        if tag.startswith("{"):
            ns = tag[: tag.index("}") + 1]

        for network_el in root.iter(f"{ns}Network"):
            network_code = network_el.get("code", "")

            for station_el in network_el.iter(f"{ns}Station"):
                station_code = station_el.get("code", "")
                station_lat = self._get_xml_float(station_el, f"{ns}Latitude")
                station_lon = self._get_xml_float(station_el, f"{ns}Longitude")
                station_elev = self._get_xml_float(station_el, f"{ns}Elevation")

                for channel_el in station_el.iter(f"{ns}Channel"):
                    channel_code = channel_el.get("code", "")
                    location_code = channel_el.get("locationCode", "")
                    start_date = channel_el.get("startDate", "")
                    end_date = channel_el.get("endDate", "")

                    chan_lat = self._get_xml_float(channel_el, f"{ns}Latitude")
                    chan_lon = self._get_xml_float(channel_el, f"{ns}Longitude")
                    chan_elev = self._get_xml_float(channel_el, f"{ns}Elevation")
                    sample_rate = self._get_xml_float(channel_el, f"{ns}SampleRate")

                    channels.append({
                        "network_code": network_code,
                        "station_code": station_code,
                        "channel_code": channel_code,
                        "location_code": location_code,
                        "latitude": chan_lat if chan_lat is not None else station_lat,
                        "longitude": chan_lon if chan_lon is not None else station_lon,
                        "elevation": chan_elev if chan_elev is not None else station_elev,
                        "start_date": start_date,
                        "end_date": end_date,
                        "sample_rate": sample_rate,
                    })

        return channels

    @staticmethod
    def _get_xml_float(el: ET.Element, tag: str) -> float | None:
        """Extract a float value from an XML child element."""
        child = el.find(tag)
        if child is not None and child.text:
            try:
                return float(child.text)
            except ValueError:
                return None
        return None

    def _parse_miniseed(self, content: bytes) -> list[dict[str, Any]]:
        """Extract header-only metadata from miniSEED binary data.

        Does NOT parse individual samples — stores raw binary for downstream
        storage in S3-compatible object store.
        """
        try:
            import obspy
        except ImportError:
            self._log.warning("obspy_not_installed", msg="Cannot parse miniSEED without obspy")
            return self._parse_miniseed_fallback(content)

        try:
            stream = obspy.read(io.BytesIO(content), headonly=True)  # type: ignore[attr-defined]
        except Exception as exc:
            raise ParseError(f"Failed to read miniSEED for {self.stream_id}: {exc}") from exc

        traces: list[dict[str, Any]] = []
        for trace in stream:
            stats = trace.stats
            traces.append({
                "network": stats.network,
                "station": stats.station,
                "channel": stats.channel,
                "location": stats.location,
                "starttime": str(stats.starttime),
                "endtime": str(stats.endtime),
                "sample_rate": float(stats.sampling_rate),
                "num_samples": int(stats.npts),
            })
        return traces

    def _parse_miniseed_fallback(self, content: bytes) -> list[dict[str, Any]]:
        """Minimal miniSEED header extraction without obspy.

        Parses fixed data header (48 bytes) per SEED manual.
        """
        records: list[dict[str, Any]] = []
        # miniSEED records are typically 512 or 4096 bytes
        # Fixed header is 48 bytes
        pos = 0
        while pos + 48 <= len(content):
            try:
                # Bytes 8-12: station code (5 chars)
                station = content[pos + 8: pos + 13].decode("ascii").strip()
                # Bytes 13-14: location code (2 chars)
                location = content[pos + 13: pos + 15].decode("ascii").strip()
                # Bytes 15-17: channel code (3 chars)
                channel = content[pos + 15: pos + 18].decode("ascii").strip()
                # Bytes 18-19: network code (2 chars)
                network = content[pos + 18: pos + 20].decode("ascii").strip()

                # Bytes 30-31: number of samples (big-endian unsigned short)
                num_samples = int.from_bytes(content[pos + 30: pos + 32], "big")

                if not station and not network:
                    break

                records.append({
                    "network": network,
                    "station": station,
                    "channel": channel,
                    "location": location,
                    "starttime": "",
                    "endtime": "",
                    "sample_rate": 0.0,
                    "num_samples": num_samples,
                })

                # Try to determine record length from blockette 1000
                # Default to 4096 if we can't determine
                record_len = 4096
                pos += record_len
            except Exception:
                break

        return records

    # -- validate -----------------------------------------------------------

    def validate(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Validate FDSN records based on service type."""
        service = self._get_cfg("fdsn_service", "event")
        valid: list[dict[str, Any]] = []
        seen_keys: set[str] = set()

        for rec in records:
            if service == "event":
                if not self._validate_event(rec):
                    continue
                # Dedup by publicID
                key = rec.get("publicID", "")
                if key and key in seen_keys:
                    self._log.warning("duplicate_event", publicID=key)
                    continue
                if key:
                    seen_keys.add(key)

            elif service == "station":
                if not self._validate_station(rec):
                    continue

            elif service == "dataselect":
                if not self._validate_miniseed(rec):
                    continue
                # Dedup by composite key
                key = f"{rec.get('network')}.{rec.get('station')}.{rec.get('channel')}.{rec.get('location')}.{rec.get('starttime')}"
                if key in seen_keys:
                    self._log.warning("duplicate_trace", key=key)
                    continue
                seen_keys.add(key)

            valid.append(rec)

        return valid

    def _validate_event(self, rec: dict[str, Any]) -> bool:
        """Validate earthquake event record."""
        lat = rec.get("latitude")
        lon = rec.get("longitude")
        depth = rec.get("depth")
        mag = rec.get("magnitude")

        if lat is not None:
            try:
                lat = float(lat)
                if lat < -90 or lat > 90:
                    self._log.warning("invalid_latitude", value=lat)
                    return False
            except (ValueError, TypeError):
                return False

        if lon is not None:
            try:
                lon = float(lon)
                if lon < -180 or lon > 180:
                    self._log.warning("invalid_longitude", value=lon)
                    return False
            except (ValueError, TypeError):
                return False

        if depth is not None:
            try:
                depth = float(depth)
                if depth < 0:
                    self._log.warning("invalid_depth", value=depth)
                    return False
            except (ValueError, TypeError):
                return False

        if mag is not None:
            try:
                float(mag)
            except (ValueError, TypeError):
                self._log.warning("invalid_magnitude", value=mag)
                return False

        return True

    def _validate_station(self, rec: dict[str, Any]) -> bool:
        """Validate station/channel record."""
        lat = rec.get("latitude")
        lon = rec.get("longitude")
        sample_rate = rec.get("sample_rate")

        if lat is not None:
            try:
                lat = float(lat)
                if lat < -90 or lat > 90:
                    return False
            except (ValueError, TypeError):
                return False

        if lon is not None:
            try:
                lon = float(lon)
                if lon < -180 or lon > 180:
                    return False
            except (ValueError, TypeError):
                return False

        if sample_rate is not None:
            try:
                sr = float(sample_rate)
                if sr <= 0:
                    return False
            except (ValueError, TypeError):
                return False

        return True

    def _validate_miniseed(self, rec: dict[str, Any]) -> bool:
        """Validate miniSEED metadata record."""
        num_samples = rec.get("num_samples")
        starttime = rec.get("starttime", "")
        endtime = rec.get("endtime", "")

        if num_samples is not None:
            try:
                ns = int(num_samples)
                if ns <= 0:
                    return False
            except (ValueError, TypeError):
                return False

        if starttime and endtime and starttime >= endtime:
            return False

        return True
