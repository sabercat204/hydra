"""AIS/ADS-B adapter — real-time position tracking for maritime and aviation.

Handles AIS (maritime) and ADS-B (aviation) data via REST polling,
WebSocket streaming, and raw NMEA sentence decoding.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from datetime import datetime, timezone
from typing import Any

import aiohttp
import structlog

from hydra.adapters.base import BaseAdapter, RawPayload
from hydra.adapters.exceptions import FetchError
from hydra.config import HydraSettings
from hydra.models.normalized import GeoGeometry, NormalizedRecord, SourceMeta, Tier
from hydra.registry.stream_registry import StreamRegistry
from hydra.utils.hashing import compute_raw_hash

logger = structlog.get_logger()

_ICAO24_RE = re.compile(r"^[0-9a-fA-F]{6}$")


class AisAdsbAdapter(BaseAdapter):
    """Adapter for AIS and ADS-B position tracking feeds."""

    adapter_type: str = "ais_adsb"

    def __init__(
        self,
        stream_id: str,
        settings: HydraSettings,
        registry: StreamRegistry | None = None,
        *,
        stream_config: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(stream_id, settings, registry)
        self._cfg = stream_config or {}
        self._log = logger.bind(stream_id=stream_id, adapter_type=self.adapter_type)
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._ws_session: aiohttp.ClientSession | None = None
        self._ws_reconnect_count = 0

    def _get(self, key: str, default: Any = None) -> Any:
        return self._cfg.get(key, default)

    def _build_auth_headers(self) -> dict[str, str]:
        auth = self._get("auth_pattern", "none")
        headers: dict[str, str] = {}
        creds = self.settings.credentials.get(self.stream_id, {})
        if auth == "api_key":
            key_name = self._get("auth_key_name", "X-API-Key")
            key_val = creds.get("api_key", "")
            headers[key_name] = key_val
        elif auth == "basic_auth":
            import base64
            user = creds.get("username", "")
            pwd = creds.get("password", "")
            token = base64.b64encode(f"{user}:{pwd}".encode()).decode()
            headers["Authorization"] = f"Basic {token}"
        elif auth == "rapidapi_key":
            headers["X-RapidAPI-Key"] = creds.get("rapidapi_key", "")
            headers["X-RapidAPI-Host"] = creds.get("rapidapi_host", "")
        return headers

    def _build_query_params(self) -> dict[str, str]:
        params: dict[str, str] = {}
        base_params = self._get("query_params", {})
        if base_params:
            params.update(base_params)

        # Bounding box
        bbox_map: dict[str, str] = self._get("bbox_param_map", {})
        for canonical, api_name in bbox_map.items():
            val = self._get(f"bbox_{canonical}") if f"bbox_{canonical}" in self._cfg else self._get(canonical)
            if val is not None:
                params[api_name] = str(val)
        for bbox_field in ("bbox_lat_min", "bbox_lat_max", "bbox_lon_min", "bbox_lon_max"):
            short = bbox_field.replace("bbox_", "")
            if bbox_field in self._cfg and short in bbox_map:
                params[bbox_map[short]] = str(self._cfg[bbox_field])

        # ICAO / MMSI filters
        icao_filter = self._get("icao_filter")
        if icao_filter:
            params["icao24"] = ",".join(icao_filter)
        mmsi_filter = self._get("mmsi_filter")
        if mmsi_filter:
            params["mmsi"] = ",".join(mmsi_filter)

        return params

    # -- fetch -------------------------------------------------------------

    async def fetch(self) -> RawPayload:
        mode = self._get("tracking_mode", "rest_poll")
        if mode == "rest_poll":
            return await self._fetch_rest()
        elif mode == "websocket":
            return await self._fetch_websocket()
        elif mode == "raw_nmea":
            return await self._fetch_nmea()
        raise FetchError(f"Unknown tracking_mode: {mode}")

    async def _fetch_rest(self) -> RawPayload:
        base_url = self._get("base_url", "")
        endpoint = self._get("endpoint_path", "")
        url = f"{base_url}{endpoint}"
        headers = self._build_auth_headers()
        params = self._build_query_params()
        timeout = aiohttp.ClientTimeout(total=self.settings.http_timeout_seconds)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=headers, params=params) as resp:
                content = await resp.read()
                return RawPayload(
                    stream_id=self.stream_id,
                    fetched_at=datetime.now(timezone.utc),
                    content=content,
                    content_type=resp.headers.get("Content-Type", "application/json"),
                    http_status=resp.status,
                    headers=dict(resp.headers),
                )

    async def _fetch_websocket(self) -> RawPayload:
        ws_url = self._get("websocket_url", "")
        batch_duration = self._get("ws_batch_duration_seconds", 10)
        ping_interval = self._get("ws_ping_interval_seconds", 30)
        reconnect_delay = self._get("ws_reconnect_delay_seconds", 5)
        max_reconnects = self._get("ws_max_reconnects", 10)
        subscribe_msg = self._get("ws_subscribe_message")

        messages: list[dict[str, Any]] = []

        if self._ws is None or self._ws.closed:
            await self._connect_ws(ws_url, subscribe_msg, ping_interval)

        start = time.monotonic()
        try:
            while (time.monotonic() - start) < batch_duration:
                try:
                    msg = await asyncio.wait_for(
                        self._ws.receive(),  # type: ignore[union-attr]
                        timeout=max(0.1, batch_duration - (time.monotonic() - start)),
                    )
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        messages.append(json.loads(msg.data))
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        raise ConnectionError("WebSocket closed")
                except asyncio.TimeoutError:
                    break
        except (ConnectionError, Exception) as exc:
            self._log.warning("websocket_disconnected", error=str(exc))
            self._ws = None
            # Attempt reconnection
            while self._ws_reconnect_count < max_reconnects:
                self._ws_reconnect_count += 1
                await asyncio.sleep(reconnect_delay)
                try:
                    await self._connect_ws(ws_url, subscribe_msg, ping_interval)
                    # Continue collecting remaining messages
                    while (time.monotonic() - start) < batch_duration:
                        try:
                            msg = await asyncio.wait_for(
                                self._ws.receive(),  # type: ignore[union-attr]
                                timeout=max(0.1, batch_duration - (time.monotonic() - start)),
                            )
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                messages.append(json.loads(msg.data))
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                        except asyncio.TimeoutError:
                            break
                    break
                except Exception:
                    continue
            else:
                if not messages:
                    raise FetchError(f"WebSocket max reconnects ({max_reconnects}) exceeded")

        content = json.dumps(messages).encode()
        return RawPayload(
            stream_id=self.stream_id,
            fetched_at=datetime.now(timezone.utc),
            content=content,
            content_type="application/json",
            http_status=200,
            headers={},
        )

    async def _connect_ws(
        self, url: str, subscribe_msg: dict | None, ping_interval: float
    ) -> None:
        if self._ws_session is None or self._ws_session.closed:
            self._ws_session = aiohttp.ClientSession()
        self._ws = await self._ws_session.ws_connect(url, heartbeat=ping_interval)
        if subscribe_msg:
            await self._ws.send_json(subscribe_msg)

    async def _fetch_nmea(self) -> RawPayload:
        nmea_type = self._get("nmea_source_type", "file_fetch")
        base_url = self._get("base_url", "")
        endpoint = self._get("endpoint_path", "")
        url = f"{base_url}{endpoint}"
        headers = self._build_auth_headers()
        timeout = aiohttp.ClientTimeout(total=self.settings.http_timeout_seconds)

        if nmea_type == "http_stream":
            batch_duration = self._get("nmea_batch_duration_seconds", 10)
            lines: list[bytes] = []
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, headers=headers) as resp:
                    start = time.monotonic()
                    async for line in resp.content:
                        lines.append(line)
                        if (time.monotonic() - start) >= batch_duration:
                            break
            content = b"\n".join(lines)
        else:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, headers=headers) as resp:
                    content = await resp.read()

        return RawPayload(
            stream_id=self.stream_id,
            fetched_at=datetime.now(timezone.utc),
            content=content,
            content_type="text/plain",
            http_status=200,
            headers={},
        )

    # -- parse -------------------------------------------------------------

    def parse(self, raw: RawPayload) -> list[dict[str, Any]]:
        mode = self._get("tracking_mode", "rest_poll")
        if mode in ("rest_poll", "websocket"):
            return self._parse_json(raw)
        elif mode == "raw_nmea":
            return self._parse_nmea(raw)
        return []

    def _parse_json(self, raw: RawPayload) -> list[dict[str, Any]]:
        import orjson
        try:
            data = orjson.loads(raw.content)
        except Exception as exc:
            from hydra.adapters.exceptions import ParseError
            raise ParseError(f"JSON decode failed: {exc}") from exc

        root_path = self._get("response_root_path")
        field_mapping: dict[str, str] = self._get("field_mapping", {})
        data_domain = self._get("data_domain", "adsb")
        tracking_mode = self._get("tracking_mode", "rest_poll")
        source_api = self._get("source_name", self.stream_id)

        # Extract array from response
        if root_path is None:
            # Data is the top-level object itself (e.g., FlightRadar24 dict of dicts)
            if isinstance(data, dict):
                items = list(data.values())
                items = [i for i in items if isinstance(i, (list, dict))]
                if items and isinstance(items[0], list):
                    # Array-of-arrays wrapped in dict
                    pass
            elif isinstance(data, list):
                items = data
            else:
                items = []
        else:
            items = data
            for part in root_path.split("."):
                if isinstance(items, dict):
                    items = items.get(part, [])
                else:
                    break
            if not isinstance(items, list):
                items = [items] if items else []

        records: list[dict[str, Any]] = []
        for item in items:
            rec: dict[str, Any] = {}
            for src_key, canonical in field_mapping.items():
                if isinstance(item, list):
                    # Array-of-arrays (OpenSky format) — src_key is an index
                    try:
                        idx = int(src_key)
                        rec[canonical] = item[idx] if idx < len(item) else None
                    except (ValueError, IndexError):
                        rec[canonical] = None
                elif isinstance(item, dict):
                    rec[canonical] = item.get(src_key)
                else:
                    rec[canonical] = None

            # Convert timestamp
            ts = rec.get("timestamp")
            if ts is not None:
                if isinstance(ts, (int, float)):
                    rec["timestamp"] = datetime.fromtimestamp(ts, tz=timezone.utc)
                elif isinstance(ts, str):
                    try:
                        rec["timestamp"] = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    except Exception:
                        rec["timestamp"] = None

            rec["tracking_mode"] = tracking_mode
            rec["data_domain"] = data_domain
            rec["source_api"] = source_api
            records.append(rec)

        return records

    def _parse_nmea(self, raw: RawPayload) -> list[dict[str, Any]]:
        """Parse raw NMEA AIS sentences using pyais."""
        from pyais import decode as pyais_decode
        from pyais import NMEAMessage

        data_domain = self._get("data_domain", "ais")
        tracking_mode = "raw_nmea"
        source_api = self._get("source_name", self.stream_id)
        supported_types = {1, 2, 3, 5, 18, 24}

        lines = raw.content.decode("ascii", errors="ignore").strip().split("\n")
        lines = [l.strip() for l in lines if l.strip()]

        # Group multi-fragment messages
        fragments: dict[str, list[str]] = {}
        single_sentences: list[str] = []

        for line in lines:
            if not (line.startswith("!AIVDM") or line.startswith("!AIVDO")):
                continue
            # Validate checksum
            if not self._validate_nmea_checksum(line):
                self._log.warning("invalid_nmea_checksum", sentence=line[:40])
                continue
            parts = line.split(",")
            if len(parts) < 7:
                continue
            frag_count = int(parts[1])
            frag_num = int(parts[2])
            seq_id = parts[3]

            if frag_count == 1:
                single_sentences.append(line)
            else:
                key = seq_id if seq_id else f"auto_{len(fragments)}"
                if key not in fragments:
                    fragments[key] = []
                fragments[key].append(line)

        records: list[dict[str, Any]] = []

        # Parse single-fragment sentences
        for sentence in single_sentences:
            try:
                msg = NMEAMessage(sentence.encode())
                decoded = msg.decode()
                if decoded.msg_type not in supported_types:
                    self._log.debug("unsupported_ais_type", msg_type=decoded.msg_type)
                    continue
                rec = self._ais_decoded_to_dict(decoded)
                rec["tracking_mode"] = tracking_mode
                rec["data_domain"] = data_domain
                rec["source_api"] = source_api
                records.append(rec)
            except Exception as exc:
                self._log.debug("nmea_parse_error", error=str(exc))

        # Parse multi-fragment messages
        for key, frags in fragments.items():
            frags.sort(key=lambda l: int(l.split(",")[2]))
            try:
                msgs = [NMEAMessage(f.encode()) for f in frags]
                decoded = msgs[0].decode(*msgs[1:])
                if decoded.msg_type not in supported_types:
                    self._log.debug("unsupported_ais_type", msg_type=decoded.msg_type)
                    continue
                rec = self._ais_decoded_to_dict(decoded)
                rec["tracking_mode"] = tracking_mode
                rec["data_domain"] = data_domain
                rec["source_api"] = source_api
                records.append(rec)
            except Exception as exc:
                self._log.debug("nmea_multi_parse_error", error=str(exc))

        return records

    @staticmethod
    def _validate_nmea_checksum(sentence: str) -> bool:
        """Validate NMEA checksum (XOR of chars between ! and *)."""
        try:
            if "*" not in sentence:
                return False
            body = sentence[1:sentence.index("*")]
            expected = sentence[sentence.index("*") + 1:].strip()
            computed = 0
            for ch in body:
                computed ^= ord(ch)
            return f"{computed:02X}" == expected.upper()
        except Exception:
            return False

    @staticmethod
    def _ais_decoded_to_dict(decoded: Any) -> dict[str, Any]:
        """Convert a pyais decoded message to a canonical dict."""
        rec: dict[str, Any] = {}
        msg_type = decoded.msg_type

        rec["mmsi"] = str(getattr(decoded, "mmsi", ""))

        if msg_type in (1, 2, 3, 18):
            rec["latitude"] = getattr(decoded, "lat", None)
            rec["longitude"] = getattr(decoded, "lon", None)
            rec["speed_knots"] = getattr(decoded, "speed", None)
            rec["course"] = getattr(decoded, "course", None)
            rec["heading"] = getattr(decoded, "heading", None)
            if msg_type in (1, 2, 3):
                rec["nav_status"] = getattr(decoded, "status", None)
            rec["timestamp"] = datetime.now(timezone.utc)
        elif msg_type == 5:
            rec["imo"] = str(getattr(decoded, "imo", ""))
            rec["callsign"] = getattr(decoded, "callsign", "")
            rec["vessel_name"] = getattr(decoded, "shipname", "")
            rec["ship_type"] = getattr(decoded, "ship_type", None)
            rec["destination"] = getattr(decoded, "destination", "")
            rec["draught"] = getattr(decoded, "draught", None)
            eta_month = getattr(decoded, "month", None)
            eta_day = getattr(decoded, "day", None)
            eta_hour = getattr(decoded, "hour", None)
            eta_minute = getattr(decoded, "minute", None)
            if eta_month and eta_day:
                try:
                    now = datetime.now(timezone.utc)
                    rec["eta"] = datetime(now.year, eta_month, eta_day,
                                          eta_hour or 0, eta_minute or 0, tzinfo=timezone.utc)
                except Exception:
                    rec["eta"] = None
            rec["timestamp"] = datetime.now(timezone.utc)
        elif msg_type == 24:
            rec["vessel_name"] = getattr(decoded, "shipname", "")
            rec["ship_type"] = getattr(decoded, "ship_type", None)
            rec["callsign"] = getattr(decoded, "callsign", "")
            rec["timestamp"] = datetime.now(timezone.utc)

        return rec

    # -- validate ----------------------------------------------------------

    def validate(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        data_domain = self._get("data_domain", "adsb")
        reject_null_island = self._get("reject_null_island", True)
        max_speed = self._get("max_speed", 600 if data_domain == "adsb" else 50)
        max_age = self._get("max_position_age_seconds", 300 if data_domain == "adsb" else 3600)
        now = datetime.now(timezone.utc)

        valid: list[dict[str, Any]] = []
        seen_keys: set[str] = set()

        for rec in records:
            lat = rec.get("latitude")
            lon = rec.get("longitude")

            # Coordinate validation
            if lat is not None and lon is not None:
                try:
                    lat = float(lat)
                    lon = float(lon)
                except (ValueError, TypeError):
                    continue
                if lat < -90 or lat > 90 or lon < -180 or lon > 180:
                    continue
                if reject_null_island and lat == 0.0 and lon == 0.0:
                    continue
                rec["latitude"] = lat
                rec["longitude"] = lon

            # Speed validation
            if data_domain == "adsb":
                speed = rec.get("velocity_ms")
                if speed is not None:
                    try:
                        speed = float(speed)
                    except (ValueError, TypeError):
                        speed = None
                    if speed is not None and (speed < 0 or speed > max_speed):
                        continue
            else:
                speed = rec.get("speed_knots")
                if speed is not None:
                    try:
                        speed = float(speed)
                    except (ValueError, TypeError):
                        speed = None
                    if speed is not None and (speed < 0 or speed > max_speed):
                        continue

            # Altitude validation (ADS-B only)
            if data_domain == "adsb":
                alt = rec.get("altitude_m")
                if alt is not None:
                    try:
                        alt = float(alt)
                    except (ValueError, TypeError):
                        alt = None
                    if alt is not None and (alt < -500 or alt > 100000):
                        continue

            # MMSI validation (AIS only)
            if data_domain == "ais":
                mmsi = rec.get("mmsi")
                if mmsi is not None:
                    mmsi = str(mmsi).strip()
                    if len(mmsi) != 9 or not mmsi.isdigit():
                        self._log.warning("invalid_mmsi", mmsi=mmsi)
                        continue
                    mid = int(mmsi[:3])
                    if mid < 201 or mid > 775:
                        self._log.warning("invalid_mmsi_mid", mmsi=mmsi, mid=mid)
                        continue

            # ICAO24 validation (ADS-B only)
            if data_domain == "adsb":
                icao = rec.get("icao24")
                if icao is not None:
                    icao = str(icao).strip()
                    if not _ICAO24_RE.match(icao):
                        continue
                    rec["icao24"] = icao

            # Timestamp / staleness validation
            ts = rec.get("timestamp")
            if ts is not None and isinstance(ts, datetime):
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                age = (now - ts).total_seconds()
                if age > max_age:
                    continue

            # Dedup by composite key
            if data_domain == "adsb":
                ident = rec.get("icao24", "")
            else:
                ident = rec.get("mmsi", "")
            ts_val = rec.get("timestamp")
            if isinstance(ts_val, datetime):
                ts_key = ts_val.strftime("%Y%m%d%H%M%S")
            else:
                ts_key = str(ts_val)
            dedup_key = f"{ident}_{ts_key}"
            if dedup_key in seen_keys:
                continue
            seen_keys.add(dedup_key)

            valid.append(rec)

        return valid

    # -- normalize ---------------------------------------------------------

    def normalize(self, records: list[dict[str, Any]]) -> list[NormalizedRecord]:
        tier_id = self._get("tier", self.tier_id or 18)
        source_name = self._get("source_name", self.stream_id)
        data_domain = self._get("data_domain", "adsb")
        tracking_mode = self._get("tracking_mode", "rest_poll")
        default_confidence = self._get("default_confidence")
        default_tags: list[str] = self._get("default_tags", [])

        if default_confidence is not None:
            confidence = float(default_confidence)
        elif tracking_mode == "websocket":
            confidence = 0.85
        elif tracking_mode == "raw_nmea":
            confidence = 0.7
        else:
            confidence = 0.9

        base_url = self._get("base_url", "")
        endpoint = self._get("endpoint_path", "")
        source_url = f"{base_url}{endpoint}"

        normalized: list[NormalizedRecord] = []
        for rec in records:
            ts = rec.get("timestamp", datetime.now(timezone.utc))
            if isinstance(ts, datetime) and ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)

            # Build GeoJSON Point
            lat = rec.get("latitude")
            lon = rec.get("longitude")
            geo = None
            if lat is not None and lon is not None:
                coords = [float(lon), float(lat)]
                alt = rec.get("altitude_m")
                if alt is not None and data_domain == "adsb":
                    coords.append(float(alt))
                geo = GeoGeometry(type="Point", coordinates=coords)

            # raw_hash
            if data_domain == "adsb":
                ident = rec.get("icao24", "")
            else:
                ident = rec.get("mmsi", "")
            ts_iso = ts.isoformat() if isinstance(ts, datetime) else str(ts)
            raw_hash = compute_raw_hash(f"{ident}_{ts_iso}".encode())

            tags = list(default_tags)
            tags.extend([data_domain, tracking_mode, rec.get("source_api", "")])

            nr = NormalizedRecord(
                stream_id=self.stream_id,
                tier=Tier(tier_id),
                timestamp=ts,
                geo=geo,
                payload=rec,
                source_meta=SourceMeta(
                    source_name=source_name,
                    source_url=source_url,
                    adapter_type=self.adapter_type,
                ),
                raw_hash=raw_hash,
                confidence=confidence,
                tags=tags,
            )
            normalized.append(nr)

        return normalized

    # -- health check ------------------------------------------------------

    async def health_check(self):
        from hydra.adapters.base import AdapterHealth, HealthStatus
        mode = self._get("tracking_mode", "rest_poll")
        start = time.monotonic()

        if mode == "rest_poll":
            base_url = self._get("base_url", "")
            endpoint = self._get("endpoint_path", "")
            url = f"{base_url}{endpoint}"
            try:
                timeout = aiohttp.ClientTimeout(total=10)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.head(url, headers=self._build_auth_headers()) as resp:
                        latency = round((time.monotonic() - start) * 1000, 2)
                        status = HealthStatus.OK if resp.status < 400 else HealthStatus.DEGRADED
                        return AdapterHealth(
                            stream_id=self.stream_id, status=status,
                            latency_ms=latency, last_checked=datetime.now(timezone.utc),
                        )
            except Exception as exc:
                return AdapterHealth(
                    stream_id=self.stream_id, status=HealthStatus.UNREACHABLE,
                    latency_ms=round((time.monotonic() - start) * 1000, 2),
                    last_checked=datetime.now(timezone.utc), detail=str(exc),
                )
        elif mode == "websocket":
            ws_url = self._get("websocket_url", "")
            try:
                timeout = aiohttp.ClientTimeout(total=10)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    ws = await session.ws_connect(ws_url)
                    await ws.close()
                    latency = round((time.monotonic() - start) * 1000, 2)
                    return AdapterHealth(
                        stream_id=self.stream_id, status=HealthStatus.OK,
                        latency_ms=latency, last_checked=datetime.now(timezone.utc),
                    )
            except Exception as exc:
                return AdapterHealth(
                    stream_id=self.stream_id, status=HealthStatus.UNREACHABLE,
                    latency_ms=round((time.monotonic() - start) * 1000, 2),
                    last_checked=datetime.now(timezone.utc), detail=str(exc),
                )
        else:
            return AdapterHealth(
                stream_id=self.stream_id, status=HealthStatus.DEGRADED,
                latency_ms=round((time.monotonic() - start) * 1000, 2),
                last_checked=datetime.now(timezone.utc), detail="NMEA health check not supported",
            )
