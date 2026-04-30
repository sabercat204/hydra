"""TAP/VO adapter — IVOA Table Access Protocol with ADQL query support.

Supports sync and async TAP query modes, VOTable/CSV/FITS response parsing,
coordinate and numeric range validation, and composite-key deduplication.
"""

from __future__ import annotations

import asyncio
import csv
import io
from datetime import datetime, timezone
from typing import Any

import aiohttp
import structlog

from hydra.config import HydraSettings
from hydra.registry.stream_registry import StreamRegistry
from hydra.utils.hashing import compute_raw_hash

from .base import BaseAdapter, RawPayload
from .exceptions import FetchError, ParseError, RateLimitError

logger = structlog.get_logger()


class TapVoAdapter(BaseAdapter):
    """IVOA Table Access Protocol adapter for astronomical databases.

    Targets: NASA Exoplanet Archive, MAST, HEASARC, Gaia DR3.
    """

    adapter_type: str = "tap_vo"

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

    def _resolve_adql(self, last_fetch_time: str | None = None) -> str:
        """Resolve ADQL template with variable substitution."""
        template = self._get_cfg("adql_template", "")
        table_name = self._get_cfg("table_name", "")
        max_rows = self._get_cfg("max_rows", 10000)
        custom_where = self._get_cfg("custom_where", "")
        variables = {
            "last_fetch_time": last_fetch_time or "1970-01-01T00:00:00Z",
            "table_name": table_name,
            "max_rows": max_rows,
            "custom_where": custom_where,
        }
        return template.format_map(variables)

    def _build_auth_headers(self) -> dict[str, str]:
        """Build auth headers/cookies based on stream config."""
        auth_pattern = self._get_cfg("auth_pattern", "none")
        if auth_pattern == "none":
            return {}

        credentials = getattr(self.settings, "credentials", {}) or {}
        creds = credentials.get(self.stream_id, {})
        if not creds:
            return {}

        token = creds.get("token", "")
        location = self._get_cfg("auth_token_location", "header")
        if location == "header":
            return {"Authorization": f"Bearer {token}"}
        elif location == "cookie":
            return {"Cookie": f"auth_token={token}"}
        return {}

    # -- fetch --------------------------------------------------------------

    async def fetch(self) -> RawPayload:
        """Fetch data from a TAP service (sync or async mode)."""
        tap_mode = self._get_cfg("tap_mode", "sync")
        if tap_mode == "async":
            return await self._fetch_async()
        return await self._fetch_sync()

    async def _fetch_sync(self) -> RawPayload:
        """Execute a synchronous TAP query."""
        base_url = self._get_cfg("base_url", "").rstrip("/")
        tap_endpoint = self._get_cfg("tap_endpoint", "/tap/sync")
        response_format = self._get_cfg("response_format", "votable")
        tap_timeout = self._get_cfg("tap_timeout_seconds", 120)
        last_fetch_time = self._get_cfg("last_fetch_time")

        adql = self._resolve_adql(last_fetch_time)
        url = f"{base_url}{tap_endpoint}"

        form_data = {
            "REQUEST": "doQuery",
            "LANG": "ADQL",
            "QUERY": adql,
            "FORMAT": response_format,
        }

        headers = {"User-Agent": "HYDRA/0.1.0"}
        headers.update(self._build_auth_headers())

        timeout = aiohttp.ClientTimeout(total=tap_timeout)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async with session.post(url, data=form_data, headers=headers) as resp:
                    if resp.status == 429:
                        retry_after = float(resp.headers.get("Retry-After", "1"))
                        raise RateLimitError(
                            f"Rate limited on {self.stream_id}", retry_after=retry_after
                        )
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
                    content_type = resp.content_type or _format_to_content_type(response_format)

                    return RawPayload(
                        stream_id=self.stream_id,
                        fetched_at=datetime.now(timezone.utc),
                        content=body,
                        content_type=content_type,
                        http_status=resp.status,
                        headers={
                            **{k: v for k, v in resp.headers.items()},
                            "tap_service_url": url,
                            "adql_query": adql,
                        },
                    )
            except aiohttp.ClientError as exc:
                raise FetchError(f"Connection error fetching {self.stream_id}: {exc}") from exc

    async def _fetch_async(self) -> RawPayload:
        """Execute an asynchronous TAP query (job-based)."""
        base_url = self._get_cfg("base_url", "").rstrip("/")
        tap_endpoint = self._get_cfg("tap_endpoint", "/tap/async")
        response_format = self._get_cfg("response_format", "votable")
        poll_interval = self._get_cfg("async_poll_interval_seconds", 5)
        max_wait = self._get_cfg("async_max_wait_seconds", 600)
        tap_timeout = self._get_cfg("tap_timeout_seconds", 120)
        last_fetch_time = self._get_cfg("last_fetch_time")

        adql = self._resolve_adql(last_fetch_time)
        url = f"{base_url}{tap_endpoint}"

        form_data = {
            "REQUEST": "doQuery",
            "LANG": "ADQL",
            "QUERY": adql,
            "FORMAT": response_format,
        }

        headers = {"User-Agent": "HYDRA/0.1.0"}
        headers.update(self._build_auth_headers())

        timeout = aiohttp.ClientTimeout(total=tap_timeout)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                # Phase 1: Job creation
                async with session.post(
                    url, data=form_data, headers=headers, allow_redirects=False
                ) as resp:
                    if resp.status == 303:
                        job_url = resp.headers.get("Location", "")
                    elif resp.status in (200, 201):
                        body = await resp.text()
                        job_url = body.strip()
                    else:
                        raise FetchError(
                            f"Async job creation failed with status {resp.status} for {self.stream_id}",
                            status_code=resp.status,
                        )

                if not job_url:
                    raise FetchError(f"No job URL returned for {self.stream_id}")

                # Ensure absolute URL
                if not job_url.startswith("http"):
                    job_url = f"{base_url}{job_url}"

                # Phase 2: Poll for completion
                elapsed = 0.0
                while elapsed < max_wait:
                    async with session.get(f"{job_url}/phase", headers=headers) as phase_resp:
                        phase = (await phase_resp.text()).strip().upper()

                    if phase == "COMPLETED":
                        break
                    elif phase == "ERROR":
                        error_detail = ""
                        try:
                            async with session.get(f"{job_url}/error", headers=headers) as err_resp:
                                error_detail = await err_resp.text()
                        except Exception:
                            pass
                        raise FetchError(
                            f"Async TAP job error for {self.stream_id}: {error_detail}",
                            status_code=500,
                        )
                    elif phase == "ABORTED":
                        raise FetchError(
                            f"Async TAP job aborted for {self.stream_id}",
                            status_code=500,
                        )

                    await asyncio.sleep(poll_interval)
                    elapsed += poll_interval
                else:
                    # Timeout — attempt to delete the job
                    try:
                        async with session.delete(job_url, headers=headers):
                            pass
                    except Exception:
                        pass
                    raise FetchError(
                        f"Async TAP job timed out after {max_wait}s for {self.stream_id}",
                        status_code=504,
                    )

                # Phase 3: Retrieve results
                async with session.get(f"{job_url}/results/result", headers=headers) as result_resp:
                    body = await result_resp.read()
                    content_type = result_resp.content_type or _format_to_content_type(response_format)

                    return RawPayload(
                        stream_id=self.stream_id,
                        fetched_at=datetime.now(timezone.utc),
                        content=body,
                        content_type=content_type,
                        http_status=result_resp.status,
                        headers={
                            **{k: v for k, v in result_resp.headers.items()},
                            "tap_service_url": url,
                            "adql_query": adql,
                        },
                    )
            except aiohttp.ClientError as exc:
                raise FetchError(f"Connection error fetching {self.stream_id}: {exc}") from exc

    # -- parse --------------------------------------------------------------

    def parse(self, raw: RawPayload) -> list[dict[str, Any]]:
        """Parse TAP response based on content type / response format."""
        if not raw.content:
            return []

        response_format = self._get_cfg("response_format", "votable")
        content_type = raw.content_type or ""

        # Determine parser
        if "fits" in content_type or response_format == "fits":
            records = self._parse_fits(raw.content)
        elif "csv" in content_type or response_format == "csv":
            records = self._parse_csv(raw.content)
        else:
            # Default to VOTable
            records = self._parse_votable(raw.content)

        # Tag each record with provenance
        tap_service_url = raw.headers.get("tap_service_url", "")
        adql_query = raw.headers.get("adql_query", "")
        for rec in records:
            rec["_tap_service_url"] = tap_service_url
            rec["_adql_query"] = adql_query

        return records

    def _parse_votable(self, content: bytes) -> list[dict[str, Any]]:
        """Parse VOTable XML using astropy."""
        try:
            from astropy.io.votable import parse as votable_parse
        except ImportError:
            raise ParseError("astropy is required for VOTable parsing")

        try:
            votable = votable_parse(io.BytesIO(content), verify="warn")
        except Exception as exc:
            raise ParseError(f"VOTable parse error for {self.stream_id}: {exc}") from exc

        records: list[dict[str, Any]] = []
        column_meta: dict[str, dict[str, str]] = {}

        for resource in votable.resources:
            for table in resource.tables:
                # Extract column metadata
                fields = table.fields
                col_names = [f.name for f in fields]
                for f in fields:
                    column_meta[f.name] = {
                        "unit": str(f.unit) if f.unit else "",
                        "ucd": f.ucd or "",
                        "utype": f.utype or "",
                        "datatype": f.datatype or "",
                    }

                # Iterate rows
                for row in table.array:
                    rec: dict[str, Any] = {}
                    for i, col_name in enumerate(col_names):
                        val = row[i]
                        # Handle VOTable null representations
                        val = _votable_null_to_none(val)
                        rec[col_name] = val
                    rec["_column_meta"] = column_meta
                    records.append(rec)
                # Only process first table in first resource
                break
            break

        return records

    def _parse_csv(self, content: bytes) -> list[dict[str, Any]]:
        """Parse CSV response."""
        try:
            text = content.decode("utf-8")
            reader = csv.DictReader(io.StringIO(text))
            records: list[dict[str, Any]] = []
            for row in reader:
                rec = {k.strip(): v.strip() if v else None for k, v in row.items()}
                records.append(rec)
            return records
        except Exception as exc:
            raise ParseError(f"CSV parse error for {self.stream_id}: {exc}") from exc

    def _parse_fits(self, content: bytes) -> list[dict[str, Any]]:
        """Parse FITS binary table."""
        try:
            from astropy.io import fits as astropy_fits
            from astropy.table import Table
        except ImportError:
            raise ParseError("astropy is required for FITS parsing")

        try:
            hdul = astropy_fits.open(io.BytesIO(content))
            records: list[dict[str, Any]] = []
            header_meta: dict[str, Any] = {}

            for hdu in hdul:
                if hasattr(hdu, "columns") and hdu.columns is not None:
                    table = Table.read(io.BytesIO(content), hdu=hdu.name)
                    col_names = table.colnames
                    for row in table:
                        rec: dict[str, Any] = {}
                        for col in col_names:
                            val = row[col]
                            # Convert numpy types to Python native
                            if hasattr(val, "item"):
                                val = val.item()
                            if isinstance(val, bytes):
                                val = val.decode("utf-8", errors="replace").strip()
                            rec[col] = val
                        rec["_header_meta"] = header_meta
                        records.append(rec)
                    break  # First binary table extension only

            hdul.close()
            return records
        except ParseError:
            raise
        except Exception as exc:
            raise ParseError(f"FITS parse error for {self.stream_id}: {exc}") from exc

    # -- validate -----------------------------------------------------------

    def validate(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Validate TAP/VO records with coordinate, numeric range, and dedup checks."""
        required_fields = self._get_cfg("required_fields", [])
        coordinate_fields = self._get_cfg("coordinate_fields")
        numeric_ranges = self._get_cfg("numeric_ranges", {})
        dedup_key_fields = self._get_cfg("dedup_key_fields")
        column_meta = self._get_cfg("_column_meta_from_votable", {})

        valid: list[dict[str, Any]] = []
        seen_keys: set[str] = set()

        for rec in records:
            # Required-field validation
            if required_fields:
                missing = [f for f in required_fields if rec.get(f) is None]
                if missing:
                    self._log.warning("missing_required_fields", fields=missing)
                    continue

            # Coordinate validation
            if coordinate_fields:
                ra_field = coordinate_fields.get("ra")
                dec_field = coordinate_fields.get("dec")
                if ra_field and dec_field:
                    ra_val = rec.get(ra_field)
                    dec_val = rec.get(dec_field)
                    if ra_val is not None:
                        try:
                            ra_val = float(ra_val)
                            if ra_val < 0 or ra_val > 360:
                                self._log.warning("invalid_ra", value=ra_val)
                                continue
                        except (ValueError, TypeError):
                            self._log.warning("ra_coercion_failed", value=ra_val)
                            continue
                    if dec_val is not None:
                        try:
                            dec_val = float(dec_val)
                            if dec_val < -90 or dec_val > 90:
                                self._log.warning("invalid_dec", value=dec_val)
                                continue
                        except (ValueError, TypeError):
                            self._log.warning("dec_coercion_failed", value=dec_val)
                            continue

            # Numeric range validation
            drop = False
            for field_name, (min_val, max_val) in (numeric_ranges or {}).items():
                val = rec.get(field_name)
                if val is not None:
                    try:
                        val = float(val)
                        if val < min_val or val > max_val:
                            self._log.warning(
                                "numeric_range_violation",
                                field=field_name,
                                value=val,
                                range=[min_val, max_val],
                            )
                            drop = True
                            break
                    except (ValueError, TypeError):
                        self._log.warning("numeric_coercion_failed", field=field_name, value=val)
                        drop = True
                        break
            if drop:
                continue

            # Type coercion from VOTable metadata
            rec_meta = rec.get("_column_meta", {})
            if rec_meta:
                for col_name, meta in rec_meta.items():
                    if col_name.startswith("_"):
                        continue
                    datatype = meta.get("datatype", "")
                    if col_name in rec and rec[col_name] is not None:
                        try:
                            rec[col_name] = _coerce_votable_type(rec[col_name], datatype)
                        except (ValueError, TypeError):
                            self._log.warning(
                                "type_coercion_failed",
                                field=col_name,
                                datatype=datatype,
                            )
                            drop = True
                            break
                if drop:
                    continue

            # Deduplication
            if dedup_key_fields:
                key = "|".join(str(rec.get(f, "")) for f in dedup_key_fields)
            else:
                raw_bytes = _dict_to_bytes_for_dedup(rec)
                key = compute_raw_hash(raw_bytes)

            if key in seen_keys:
                self._log.debug("duplicate_record", key=key)
                continue
            seen_keys.add(key)

            valid.append(rec)

        return valid


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _format_to_content_type(fmt: str) -> str:
    """Map TAP response format to MIME content type."""
    mapping = {
        "votable": "application/x-votable+xml",
        "csv": "text/csv",
        "fits": "application/fits",
    }
    return mapping.get(fmt, "application/x-votable+xml")


def _votable_null_to_none(val: Any) -> Any:
    """Convert VOTable null representations to Python None."""
    import numpy as np

    if val is None:
        return None
    if isinstance(val, (float, np.floating)) and np.isnan(val):
        return None
    if isinstance(val, np.ma.core.MaskedConstant):
        return None
    if hasattr(val, "mask") and hasattr(val, "data"):
        # numpy masked value
        return None
    if isinstance(val, bytes):
        val = val.decode("utf-8", errors="replace").strip()
    if isinstance(val, str) and val.strip() == "":
        return None
    # Convert numpy scalars to Python native
    if hasattr(val, "item"):
        return val.item()
    return val


def _coerce_votable_type(val: Any, datatype: str) -> Any:
    """Coerce a value based on VOTable FIELD datatype."""
    if val is None:
        return None
    if datatype in ("int", "short", "long"):
        return int(val)
    if datatype in ("float", "double"):
        return float(val)
    if datatype in ("char", "unicodeChar"):
        return str(val)
    return val


def _dict_to_bytes_for_dedup(d: dict[str, Any]) -> bytes:
    """Serialize a dict to bytes for hashing, excluding internal metadata keys."""
    import orjson

    clean = {k: v for k, v in d.items() if not k.startswith("_")}
    return orjson.dumps(clean, option=orjson.OPT_SORT_KEYS)
