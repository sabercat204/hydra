"""SDMX adapter — pandasdmx-based primary mode with raw HTTP fallback,
DSD-aware validation, time period normalization, dimension label resolution.

Stream-specific behavior is encoded entirely in stream_registry.yaml entries.
Adding a new SDMX source requires only a new registry entry, zero code changes.
"""

from __future__ import annotations

import asyncio
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any

import aiohttp
import orjson
import pandas as pd
import pandasdmx
import structlog

from hydra.config import HydraSettings
from hydra.registry.stream_registry import StreamRegistry
from hydra.utils.hashing import compute_raw_hash

from .base import BaseAdapter, RawPayload
from .exceptions import FetchError, ParseError, RateLimitError

logger = structlog.get_logger()

# Regex patterns for SDMX time period formats
_ANNUAL_RE = re.compile(r"^\d{4}$")
_QUARTERLY_RE = re.compile(r"^(\d{4})-Q([1-4])$")
_MONTHLY_RE = re.compile(r"^(\d{4})-M(\d{2})$")
_MONTHLY_SHORT_RE = re.compile(r"^(\d{4})-(\d{2})$")
_DAILY_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")

# Quarter to month mapping
_QUARTER_MONTH = {"1": "01", "2": "04", "3": "07", "4": "10"}


def normalize_time_period(period: str) -> tuple[datetime, str]:
    """Normalize SDMX time period to ISO 8601 datetime.

    Returns (normalized_datetime, original_period_string).
    """
    period = period.strip()

    m = _DAILY_RE.match(period)
    if m:
        return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), tzinfo=timezone.utc), period

    m = _QUARTERLY_RE.match(period)
    if m:
        year, q = m.group(1), m.group(2)
        month = int(_QUARTER_MONTH[q])
        return datetime(int(year), month, 1, tzinfo=timezone.utc), period

    m = _MONTHLY_RE.match(period)
    if m:
        return datetime(int(m.group(1)), int(m.group(2)), 1, tzinfo=timezone.utc), period

    m = _MONTHLY_SHORT_RE.match(period)
    if m:
        return datetime(int(m.group(1)), int(m.group(2)), 1, tzinfo=timezone.utc), period

    m = _ANNUAL_RE.match(period)
    if m:
        return datetime(int(period), 1, 1, tzinfo=timezone.utc), period

    # Fallback: try ISO parse
    try:
        dt = datetime.fromisoformat(period.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt, period
    except (ValueError, TypeError):
        raise ValueError(f"Cannot parse SDMX time period: {period}")


class SdmxAdapter(BaseAdapter):
    """Configuration-driven SDMX adapter with pandasdmx primary and raw HTTP fallback."""

    adapter_type: str = "sdmx"

    def __init__(
        self,
        stream_id: str,
        settings: HydraSettings,
        registry: StreamRegistry | None = None,
        *,
        stream_config: dict[str, Any] | None = None,
        last_fetch_time: str | None = None,
    ) -> None:
        super().__init__(stream_id, settings, registry)
        self._cfg: dict[str, Any] = stream_config or {}
        self._last_fetch_time = last_fetch_time
        self._dsd_cache: dict[str, Any] | None = None
        self._code_lists: dict[str, dict[str, str]] = {}
        self._last_request_time: float = 0.0

    # -- helpers ------------------------------------------------------------

    def _get_cfg(self, key: str, default: Any = None) -> Any:
        return self._cfg.get(key, default)

    def _resolve_credential(self) -> str | None:
        creds: dict[str, Any] = getattr(self.settings, "credentials", {}) or {}
        entry = creds.get(self.stream_id)
        if isinstance(entry, str):
            return entry
        return None

    def _resolve_credential_dict(self) -> dict[str, str]:
        creds: dict[str, Any] = getattr(self.settings, "credentials", {}) or {}
        entry = creds.get(self.stream_id, {})
        if isinstance(entry, dict):
            return entry
        return {}

    def _resolve_params(self) -> dict[str, str]:
        """Resolve SDMX query params, replacing {last_fetch_time} templates."""
        raw_params: dict[str, str] = dict(self._get_cfg("sdmx_params", {}) or {})
        resolved: dict[str, str] = {}
        for k, v in raw_params.items():
            if isinstance(v, str) and "{last_fetch_time}" in v and self._last_fetch_time:
                resolved[k] = v.replace("{last_fetch_time}", self._last_fetch_time)
            else:
                resolved[k] = str(v)
        return resolved

    async def _enforce_request_delay(self) -> None:
        delay = self._get_cfg("sdmx_request_delay_seconds", 1.0)
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if self._last_request_time > 0 and elapsed < delay:
            await asyncio.sleep(delay - elapsed)
        self._last_request_time = time.monotonic()

    # -- fetch --------------------------------------------------------------

    async def fetch(self) -> RawPayload:
        agency = self._get_cfg("sdmx_agency", "")
        dataflow_id = self._get_cfg("dataflow_id", "")
        key = self._get_cfg("sdmx_key", "")
        params = self._resolve_params()

        try:
            content, mode = await self._fetch_pandasdmx(agency, dataflow_id, key, params)
        except Exception as exc:
            self._log.info("pandasdmx_fallback", reason=str(exc))
            content, mode = await self._fetch_raw_http(agency, dataflow_id, key, params)

        # Encode the result as JSON with mode indicator
        payload = {"mode": mode, "data": content}
        raw_bytes = orjson.dumps(payload)

        return RawPayload(
            stream_id=self.stream_id,
            fetched_at=datetime.now(timezone.utc),
            content=raw_bytes,
            content_type="application/json",
            http_status=200,
            headers={},
        )

    async def _fetch_pandasdmx(
        self,
        agency: str,
        dataflow_id: str,
        key: str,
        params: dict[str, str],
    ) -> tuple[Any, str]:
        """Fetch via pandasdmx library. Runs sync calls in executor."""
        loop = asyncio.get_event_loop()

        def _sync_fetch() -> tuple[Any, str]:
            # Register custom source if not built-in
            base_url = self._get_cfg("base_url")
            known_sources = list(pandasdmx.list_sources())
            if agency.upper() not in [s.upper() for s in known_sources] and base_url:
                pandasdmx.add_source(
                    {
                        "id": agency,
                        "name": agency,
                        "url": base_url,
                    }
                )

            req = pandasdmx.Request(agency)

            # Fetch DSD if not cached
            if self._dsd_cache is None:
                try:
                    dsd_msg = req.datastructure(dataflow_id)
                    self._dsd_cache = dsd_msg
                    self._extract_code_lists(dsd_msg)
                except Exception as dsd_exc:
                    self._log.warning("dsd_fetch_failed", error=str(dsd_exc))

            # Fetch data
            await_enforce = False
            msg = req.data(dataflow_id, key=key, params=params)

            # Convert to pandas DataFrame
            df = pandasdmx.to_pandas(msg.data[0] if msg.data else msg)
            if isinstance(df, pd.Series):
                df = df.reset_index()
            elif isinstance(df, pd.DataFrame):
                if isinstance(df.index, pd.MultiIndex):
                    df = df.reset_index()
            else:
                df = pd.DataFrame(df)

            records = df.to_dict(orient="records")
            return records, "pandasdmx"

        await self._enforce_request_delay()
        return await loop.run_in_executor(None, _sync_fetch)

    def _extract_code_lists(self, dsd_msg: Any) -> None:
        """Extract code lists from DSD message for label resolution."""
        try:
            for cl_id, cl in getattr(dsd_msg, "codelist", {}).items():
                labels: dict[str, str] = {}
                for code_id, code in cl.items():
                    labels[str(code_id)] = str(code.name) if hasattr(code, "name") else str(code)
                self._code_lists[str(cl_id)] = labels
        except Exception:
            pass

    async def _fetch_raw_http(
        self,
        agency: str,
        dataflow_id: str,
        key: str,
        params: dict[str, str],
    ) -> tuple[Any, str]:
        """Fallback: raw HTTP request to SDMX REST endpoint."""
        base_url = self._get_cfg("base_url", "")
        if not base_url:
            raise FetchError(f"No base_url configured for SDMX fallback on {self.stream_id}")

        if not base_url.endswith("/"):
            base_url += "/"
        url = f"{base_url}data/{dataflow_id}/{key}" if key else f"{base_url}data/{dataflow_id}"
        if params:
            url = f"{url}?{urlencode_safe(params)}"

        format_pref = self._get_cfg("sdmx_format_preference", ["json", "xml"])
        timeout = aiohttp.ClientTimeout(total=getattr(self.settings, "http_timeout_seconds", 30))

        headers: dict[str, str] = {"User-Agent": "HYDRA/0.1.0"}

        # Auth
        pattern = self._get_cfg("auth_pattern", "none")
        if pattern == "api_key":
            key_name = self._get_cfg("auth_key_name", "api_key")
            location = self._get_cfg("auth_key_location", "header")
            cred = self._resolve_credential()
            if location == "header" and cred:
                headers[key_name] = cred

        await self._enforce_request_delay()

        async with aiohttp.ClientSession(timeout=timeout) as session:
            for fmt in format_pref:
                accept = (
                    "application/vnd.sdmx.data+json;version=1.0.0"
                    if fmt == "json"
                    else "application/vnd.sdmx.genericdata+xml;version=2.1"
                )
                req_headers = {**headers, "Accept": accept}

                try:
                    async with session.get(url, headers=req_headers) as resp:
                        if resp.status == 429:
                            retry_after = float(resp.headers.get("Retry-After", "1"))
                            raise RateLimitError(f"Rate limited on {self.stream_id}", retry_after=retry_after)
                        if resp.status >= 400:
                            raise FetchError(
                                f"HTTP {resp.status} from SDMX {self.stream_id}",
                                status_code=resp.status,
                            )
                        body = await resp.read()
                        if fmt == "json":
                            return orjson.loads(body), "sdmx_json"
                        else:
                            return body.decode("utf-8"), "sdmx_xml"
                except (aiohttp.ClientError, FetchError):
                    if fmt == format_pref[-1]:
                        raise
                    continue

        raise FetchError(f"All SDMX format attempts failed for {self.stream_id}")

    # -- parse --------------------------------------------------------------

    def parse(self, raw: RawPayload) -> list[dict[str, Any]]:
        if not raw.content:
            return []

        try:
            payload = orjson.loads(raw.content)
        except Exception as exc:
            raise ParseError(f"Failed to decode payload for {self.stream_id}: {exc}") from exc

        mode = payload.get("mode", "")
        data = payload.get("data")

        agency = self._get_cfg("sdmx_agency", "")
        dataflow_id = self._get_cfg("dataflow_id", "")
        sdmx_key = self._get_cfg("sdmx_key", "")
        include_labels = self._get_cfg("include_dimension_labels", True)

        if mode == "pandasdmx":
            records = self._parse_pandasdmx(data, include_labels)
        elif mode == "sdmx_json":
            records = self._parse_sdmx_json(data)
        elif mode == "sdmx_xml":
            records = self._parse_sdmx_xml(data)
        else:
            raise ParseError(f"Unknown SDMX parse mode: {mode}")

        # Tag all records with provenance and normalize time periods
        for rec in records:
            rec["sdmx_agency"] = agency
            rec["sdmx_dataflow"] = dataflow_id
            rec["sdmx_key"] = sdmx_key

            # Time period normalization
            time_val = rec.get("TIME_PERIOD") or rec.get("time_period") or rec.get("TimePeriod")
            if time_val:
                try:
                    normalized_dt, raw_period = normalize_time_period(str(time_val))
                    rec["TIME_PERIOD"] = normalized_dt.isoformat()
                    rec["time_period_raw"] = raw_period
                except ValueError:
                    rec["time_period_raw"] = str(time_val)

        return records

    def _parse_pandasdmx(self, data: list[dict[str, Any]], include_labels: bool) -> list[dict[str, Any]]:
        """Parse records from pandasdmx DataFrame output."""
        records: list[dict[str, Any]] = []
        for rec in data:
            processed: dict[str, Any] = {}
            for k, v in rec.items():
                # Convert pandas NaN to None
                if isinstance(v, float) and pd.isna(v):
                    processed[k] = None
                else:
                    processed[k] = v

            # Add dimension labels from code lists
            if include_labels and self._code_lists:
                for dim_name, dim_val in list(processed.items()):
                    if dim_name in self._code_lists and dim_val is not None:
                        label = self._code_lists[dim_name].get(str(dim_val))
                        if label:
                            processed[f"{dim_name}_label"] = label

            records.append(processed)
        return records

    def _parse_sdmx_json(self, data: dict[str, Any]) -> list[dict[str, Any]]:
        """Parse SDMX-JSON format response."""
        records: list[dict[str, Any]] = []

        try:
            structure = data.get("structure", {})
            dimensions = structure.get("dimensions", {})
            series_dims = dimensions.get("series", [])
            obs_dims = dimensions.get("observation", [])
            attributes = structure.get("attributes", {})
            series_attrs = attributes.get("series", [])
            obs_attrs = attributes.get("observation", [])

            datasets = data.get("dataSets", [])
            if not datasets:
                return []

            series_map = datasets[0].get("series", {})

            for series_key, series_data in series_map.items():
                # Resolve series dimension values
                key_parts = series_key.split(":")
                series_record: dict[str, Any] = {}

                for i, part in enumerate(key_parts):
                    if i < len(series_dims):
                        dim = series_dims[i]
                        dim_name = dim.get("id", f"dim_{i}")
                        values = dim.get("values", [])
                        idx = int(part)
                        if idx < len(values):
                            series_record[dim_name] = values[idx].get("id", values[idx].get("name", part))

                # Resolve series attributes
                s_attrs = series_data.get("attributes", [])
                for i, attr_val in enumerate(s_attrs):
                    if attr_val is not None and i < len(series_attrs):
                        attr_def = series_attrs[i]
                        attr_name = attr_def.get("id", f"attr_{i}")
                        attr_values = attr_def.get("values", [])
                        if isinstance(attr_val, int) and attr_val < len(attr_values):
                            series_record[attr_name] = attr_values[attr_val].get("id", str(attr_val))

                # Process observations
                observations = series_data.get("observations", {})
                for obs_key, obs_values in observations.items():
                    obs_record = dict(series_record)

                    # Resolve observation dimensions
                    obs_key_parts = obs_key.split(":")
                    for i, part in enumerate(obs_key_parts):
                        if i < len(obs_dims):
                            dim = obs_dims[i]
                            dim_name = dim.get("id", f"obs_dim_{i}")
                            values = dim.get("values", [])
                            idx = int(part)
                            if idx < len(values):
                                obs_record[dim_name] = values[idx].get("id", values[idx].get("name", part))

                    # Observation value
                    if obs_values and len(obs_values) > 0:
                        obs_record["value"] = obs_values[0]

                    # Observation attributes
                    if len(obs_values) > 1:
                        for i, attr_val in enumerate(obs_values[1:], start=0):
                            if attr_val is not None and i < len(obs_attrs):
                                attr_def = obs_attrs[i]
                                attr_name = attr_def.get("id", f"obs_attr_{i}")
                                attr_values = attr_def.get("values", [])
                                if isinstance(attr_val, int) and attr_val < len(attr_values):
                                    obs_record[attr_name] = attr_values[attr_val].get("id", str(attr_val))

                    records.append(obs_record)

        except (KeyError, IndexError, TypeError) as exc:
            raise ParseError(f"Failed to parse SDMX-JSON for {self.stream_id}: {exc}") from exc

        return records

    def _parse_sdmx_xml(self, data: str) -> list[dict[str, Any]]:
        """Parse SDMX-ML generic data XML."""
        records: list[dict[str, Any]] = []

        try:
            root = ET.fromstring(data)
        except ET.ParseError as exc:
            raise ParseError(f"Failed to parse SDMX-XML for {self.stream_id}: {exc}") from exc

        # Find all Series and Obs elements (handle namespaces)
        for elem in root.iter():
            tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag

            if tag == "Series":
                series_attrs = dict(elem.attrib)
                # Also check for child Value elements (generic format)
                for child in elem:
                    child_tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                    if child_tag == "SeriesKey":
                        for val_elem in child:
                            val_tag = val_elem.tag.split("}")[-1] if "}" in val_elem.tag else val_elem.tag
                            if val_tag == "Value":
                                series_attrs[val_elem.get("id", "")] = val_elem.get("value", "")

                for obs_elem in elem:
                    obs_tag = obs_elem.tag.split("}")[-1] if "}" in obs_elem.tag else obs_elem.tag
                    if obs_tag == "Obs":
                        obs_record = dict(series_attrs)
                        obs_record.update(obs_elem.attrib)
                        # Handle generic format Obs children
                        for obs_child in obs_elem:
                            oc_tag = obs_child.tag.split("}")[-1] if "}" in obs_child.tag else obs_child.tag
                            if oc_tag == "ObsDimension":
                                obs_record["TIME_PERIOD"] = obs_child.get("value", "")
                            elif oc_tag == "ObsValue":
                                obs_record["value"] = obs_child.get("value", "")
                            elif oc_tag == "Value":
                                obs_record[obs_child.get("id", "")] = obs_child.get("value", "")
                        records.append(obs_record)

        return records

    # -- validate -----------------------------------------------------------

    def validate(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        required = self._get_cfg("required_fields", []) or []
        strict_dims = self._get_cfg("strict_dimensions", []) or []
        time_range = self._get_cfg("time_period_range")
        value_range = self._get_cfg("observation_value_range")
        dedup_keys = self._get_cfg("dedup_key_fields")

        valid: list[dict[str, Any]] = []
        seen_keys: set[str] = set()

        for rec in records:
            # Required field check
            if not self._check_required(rec, required):
                self._log.warning("validation_dropped", reason="missing_required_field")
                continue

            # Strict dimension validation against code lists
            if strict_dims and not self._validate_strict_dimensions(rec, strict_dims):
                continue

            # Numeric observation validation
            if not self._validate_observation_numeric(rec):
                continue

            # Time period range validation
            if time_range and not self._validate_time_range(rec, time_range):
                continue

            # Observation value range validation
            if value_range and not self._validate_value_range(rec, value_range):
                continue

            # Deduplication
            if dedup_keys:
                key_val = "|".join(str(rec.get(k, "")) for k in dedup_keys)
            else:
                # Default: all dimension fields (exclude value, labels, provenance)
                exclude = {"value", "time_period_raw", "sdmx_agency", "sdmx_dataflow", "sdmx_key"}
                key_parts = [f"{k}={v}" for k, v in sorted(rec.items())
                             if k not in exclude and not k.endswith("_label")]
                key_val = "|".join(key_parts)

            if key_val in seen_keys:
                self._log.warning("validation_dropped", reason="duplicate")
                continue
            seen_keys.add(key_val)

            valid.append(rec)

        return valid

    @staticmethod
    def _check_required(rec: dict[str, Any], required: list[str]) -> bool:
        for field_name in required:
            if field_name not in rec or rec[field_name] is None:
                return False
        return True

    def _validate_strict_dimensions(self, rec: dict[str, Any], strict_dims: list[str]) -> bool:
        for dim in strict_dims:
            val = rec.get(dim)
            if val is None:
                continue
            if dim in self._code_lists:
                if str(val) not in self._code_lists[dim]:
                    self._log.warning("strict_dimension_invalid", dimension=dim, value=val)
                    return False
        return True

    def _validate_observation_numeric(self, rec: dict[str, Any]) -> bool:
        """Validate that observation value is numeric when DSD declares numeric measure."""
        value = rec.get("value")
        if value is None:
            return True
        if isinstance(value, (int, float)):
            return True
        if isinstance(value, str):
            try:
                float(value)
                return True
            except (ValueError, TypeError):
                self._log.warning("non_numeric_observation", value=value)
                return False
        return True

    def _validate_time_range(self, rec: dict[str, Any], time_range: list[str]) -> bool:
        time_val = rec.get("TIME_PERIOD")
        if not time_val:
            return True
        try:
            if isinstance(time_val, str):
                dt = datetime.fromisoformat(time_val.replace("Z", "+00:00"))
            elif isinstance(time_val, datetime):
                dt = time_val
            else:
                return True

            start = datetime.fromisoformat(time_range[0]).replace(tzinfo=timezone.utc)
            end = datetime.fromisoformat(time_range[1]).replace(tzinfo=timezone.utc)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)

            if dt < start or dt > end:
                self._log.warning("time_period_out_of_range", time_period=str(time_val))
                return False
        except (ValueError, IndexError):
            pass
        return True

    def _validate_value_range(self, rec: dict[str, Any], value_range: list[float]) -> bool:
        value = rec.get("value")
        if value is None:
            return True
        try:
            num = float(value)
            if num < value_range[0] or num > value_range[1]:
                self._log.warning("observation_value_out_of_range", value=num)
                return False
        except (ValueError, TypeError):
            pass
        return True


def urlencode_safe(params: dict[str, str]) -> str:
    """URL-encode params, handling special characters."""
    from urllib.parse import urlencode
    return urlencode(params)
