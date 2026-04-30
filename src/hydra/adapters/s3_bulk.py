"""S3/Bulk adapter — S3 listing, manifest-based, and HTTP bulk ingestion.

Supports multi-format parser dispatch (JSON, CSV, FITS, NetCDF, HDF5,
GeoTIFF, miniSEED, WARC) with differential sync and concurrency control.
"""

from __future__ import annotations

import asyncio
import csv
import fcntl
import hashlib
import io
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp
import structlog

from hydra.config import HydraSettings
from hydra.registry.stream_registry import StreamRegistry
from hydra.utils.hashing import compute_raw_hash

from .base import BaseAdapter, RawPayload
from .exceptions import FetchError, ParseError, ValidationError

logger = structlog.get_logger()

# Sentinel for streamed-to-disk content
_STREAMED_SENTINEL = b"__HYDRA_STREAMED_TO_DISK__"


class S3BulkAdapter(BaseAdapter):
    """S3 and bulk download adapter for large-scale dataset ingestion.

    Supports three modes: s3_listing, manifest, http_bulk.
    """

    adapter_type: str = "s3_bulk"

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
        self._manifest: dict[str, Any] = {}
        self._temp_files: list[str] = []

    # -- helpers ------------------------------------------------------------

    def _get_cfg(self, key: str, default: Any = None) -> Any:
        return self._cfg.get(key, default)

    def _manifest_path(self) -> Path:
        data_dir = getattr(self.settings, "data_dir", Path("data"))
        if isinstance(data_dir, str):
            data_dir = Path(data_dir)
        manifest_dir = data_dir / "manifests"
        manifest_dir.mkdir(parents=True, exist_ok=True)
        return manifest_dir / f"{self.stream_id}_manifest.json"

    def _load_manifest(self) -> dict[str, Any]:
        """Load the local tracking manifest with file locking."""
        path = self._manifest_path()
        if not path.exists():
            return {"stream_id": self.stream_id, "last_sync": None, "objects": {}}
        try:
            with open(path, "r") as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                try:
                    data = json.load(f)
                finally:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            return data
        except (json.JSONDecodeError, OSError):
            return {"stream_id": self.stream_id, "last_sync": None, "objects": {}}

    def _save_manifest(self, manifest: dict[str, Any]) -> None:
        """Save the local tracking manifest with file locking."""
        path = self._manifest_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            try:
                json.dump(manifest, f, indent=2, default=str)
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)

    def _build_s3_client(self) -> Any:
        """Build a boto3 S3 client (anonymous or authenticated)."""
        import boto3
        from botocore import UNSIGNED
        from botocore.config import Config as BotoConfig

        auth_pattern = self._get_cfg("auth_pattern", "none")
        region = self._get_cfg("s3_region", "us-east-1")

        if auth_pattern == "aws_credentials":
            credentials = getattr(self.settings, "credentials", {}) or {}
            creds = credentials.get(self.stream_id, {})
            return boto3.client(
                "s3",
                region_name=region,
                aws_access_key_id=creds.get("aws_access_key_id", ""),
                aws_secret_access_key=creds.get("aws_secret_access_key", ""),
                aws_session_token=creds.get("aws_session_token"),
            )
        else:
            return boto3.client(
                "s3",
                region_name=region,
                config=BotoConfig(signature_version=UNSIGNED),
            )

    def _build_auth_headers(self) -> dict[str, str]:
        """Build HTTP auth headers for manifest/http_bulk modes."""
        auth_pattern = self._get_cfg("auth_pattern", "none")
        if auth_pattern == "none":
            return {}

        credentials = getattr(self.settings, "credentials", {}) or {}
        creds = credentials.get(self.stream_id, {})

        if auth_pattern == "api_key":
            api_key = creds.get("api_key", "")
            key_header = creds.get("key_header", "X-API-Key")
            return {key_header: api_key}
        elif auth_pattern == "account_token":
            token = creds.get("token", "")
            return {"Authorization": f"Bearer {token}"}
        return {}

    # -- fetch --------------------------------------------------------------

    async def fetch(self) -> RawPayload:
        """Fetch data based on bulk_mode (s3_listing, manifest, http_bulk)."""
        bulk_mode = self._get_cfg("bulk_mode", "s3_listing")
        self._manifest = self._load_manifest()

        if bulk_mode == "s3_listing":
            payloads = await self._fetch_s3_listing()
        elif bulk_mode == "manifest":
            payloads = await self._fetch_manifest()
        elif bulk_mode == "http_bulk":
            payloads = await self._fetch_http_bulk()
        else:
            raise FetchError(f"Unknown bulk_mode: {bulk_mode}")

        # Save updated manifest
        self._manifest["last_sync"] = datetime.now(timezone.utc).isoformat()
        self._save_manifest(self._manifest)

        if not payloads:
            return RawPayload(
                stream_id=self.stream_id,
                fetched_at=datetime.now(timezone.utc),
                content=b"",
                content_type="application/octet-stream",
                http_status=204,
            )

        # Wrap multiple payloads in a JSON envelope
        import orjson

        envelope = orjson.dumps({
            "_bulk": True,
            "_payloads": [
                {
                    "content": p.content.decode("utf-8", errors="replace")
                    if p.content != _STREAMED_SENTINEL
                    else None,
                    "content_type": p.content_type,
                    "headers": p.headers,
                    "temp_file": p.headers.get("_temp_file"),
                }
                for p in payloads
            ],
        })
        return RawPayload(
            stream_id=self.stream_id,
            fetched_at=datetime.now(timezone.utc),
            content=envelope,
            content_type="application/json+bulk",
            http_status=200,
        )

    async def _fetch_s3_listing(self) -> list[RawPayload]:
        """Fetch objects from an S3 bucket with differential sync."""
        bucket = self._get_cfg("s3_bucket", "")
        prefix = self._get_cfg("s3_prefix", "")
        ext_filter = self._get_cfg("file_extension_filter", [])
        last_fetch_time = self._get_cfg("last_fetch_time")
        streaming_threshold = self._get_cfg("streaming_threshold_bytes", 52428800)
        chunk_size = self._get_cfg("chunk_size_bytes", 8388608)
        max_concurrent = self._get_cfg("max_concurrent_downloads", 5)
        delay = self._get_cfg("download_delay_seconds", 0)

        s3 = self._build_s3_client()
        objects_meta = self._manifest.get("objects", {})

        # List objects
        candidates: list[dict[str, Any]] = []
        paginator_params: dict[str, Any] = {"Bucket": bucket}
        if prefix:
            paginator_params["Prefix"] = prefix

        continuation_token = None
        while True:
            kwargs = dict(paginator_params)
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token

            response = s3.list_objects_v2(**kwargs)
            for obj in response.get("Contents", []):
                key = obj["Key"]
                etag = obj.get("ETag", "").strip('"')
                size = obj.get("Size", 0)
                last_modified = obj.get("LastModified")

                # Extension filter
                if ext_filter:
                    if not any(key.endswith(ext) for ext in ext_filter):
                        continue

                # Date filter
                if last_fetch_time and last_modified:
                    if isinstance(last_fetch_time, str):
                        last_fetch_dt = datetime.fromisoformat(last_fetch_time.replace("Z", "+00:00"))
                    else:
                        last_fetch_dt = last_fetch_time
                    if last_modified.replace(tzinfo=timezone.utc) <= last_fetch_dt.replace(tzinfo=timezone.utc):
                        continue

                # Differential sync — skip if already fetched with same ETag
                existing = objects_meta.get(key, {})
                if existing.get("etag") == etag:
                    continue

                candidates.append({
                    "key": key,
                    "etag": etag,
                    "size": size,
                    "last_modified": last_modified.isoformat() if last_modified else "",
                })

            if response.get("IsTruncated"):
                continuation_token = response.get("NextContinuationToken")
            else:
                break

        # Download candidates with concurrency control
        semaphore = asyncio.Semaphore(max_concurrent)
        payloads: list[RawPayload] = []

        async def download_one(obj_meta: dict[str, Any]) -> RawPayload | None:
            async with semaphore:
                key = obj_meta["key"]
                size = obj_meta["size"]
                try:
                    s3_resp = s3.get_object(Bucket=bucket, Key=key)
                    body_stream = s3_resp["Body"]

                    if size > streaming_threshold:
                        # Stream to temp file
                        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(key)[1])
                        self._temp_files.append(tmp.name)
                        while True:
                            chunk = body_stream.read(chunk_size)
                            if not chunk:
                                break
                            tmp.write(chunk)
                        tmp.close()
                        content = _STREAMED_SENTINEL
                        headers = {"_temp_file": tmp.name}
                    else:
                        content = body_stream.read()
                        headers = {}

                    # Update manifest
                    objects_meta[key] = {
                        "etag": obj_meta["etag"],
                        "size": obj_meta["size"],
                        "last_modified": obj_meta["last_modified"],
                        "fetched_at": datetime.now(timezone.utc).isoformat(),
                        "status": "ingested",
                    }

                    payload = RawPayload(
                        stream_id=self.stream_id,
                        fetched_at=datetime.now(timezone.utc),
                        content=content,
                        content_type=s3_resp.get("ContentType", "application/octet-stream"),
                        http_status=200,
                        headers={
                            **headers,
                            "s3_key": key,
                            "s3_etag": obj_meta["etag"],
                            "s3_size": str(obj_meta["size"]),
                            "s3_last_modified": obj_meta["last_modified"],
                            "source_object_key": key,
                            "source_bucket": bucket,
                            "object_size_bytes": str(obj_meta["size"]),
                        },
                    )

                    if delay > 0:
                        await asyncio.sleep(delay)

                    return payload
                except Exception as exc:
                    self._log.error("s3_download_error", key=key, error=str(exc))
                    return None

        tasks = [download_one(obj) for obj in candidates]
        results = await asyncio.gather(*tasks)
        payloads = [p for p in results if p is not None]

        return payloads

    async def _fetch_manifest(self) -> list[RawPayload]:
        """Fetch objects listed in a remote manifest file."""
        manifest_url = self._get_cfg("manifest_url", "")
        manifest_format = self._get_cfg("manifest_format", "json")
        max_concurrent = self._get_cfg("max_concurrent_downloads", 5)
        streaming_threshold = self._get_cfg("streaming_threshold_bytes", 52428800)
        chunk_size = self._get_cfg("chunk_size_bytes", 8388608)
        delay = self._get_cfg("download_delay_seconds", 0)
        ext_filter = self._get_cfg("file_extension_filter", [])

        headers = {"User-Agent": "HYDRA/0.1.0"}
        headers.update(self._build_auth_headers())

        async with aiohttp.ClientSession() as session:
            # Fetch manifest
            async with session.get(manifest_url, headers=headers) as resp:
                if resp.status >= 400:
                    raise FetchError(
                        f"Failed to fetch manifest from {manifest_url}: {resp.status}",
                        status_code=resp.status,
                    )
                manifest_content = await resp.read()

            # Parse manifest entries
            entries = self._parse_manifest_entries(manifest_content, manifest_format)

            # Differential sync
            objects_meta = self._manifest.get("objects", {})
            candidates: list[dict[str, Any]] = []
            for entry in entries:
                url_or_key = entry.get("url") or entry.get("key", "")
                if ext_filter:
                    if not any(url_or_key.endswith(ext) for ext in ext_filter):
                        continue
                existing = objects_meta.get(url_or_key, {})
                checksum = entry.get("checksum", "")
                if existing.get("etag") == checksum and checksum:
                    continue
                candidates.append(entry)

            # Download
            semaphore = asyncio.Semaphore(max_concurrent)
            payloads: list[RawPayload] = []

            async def download_one(entry: dict[str, Any]) -> RawPayload | None:
                async with semaphore:
                    url = entry.get("url", "")
                    if not url:
                        return None
                    try:
                        async with session.get(url, headers=headers) as dl_resp:
                            if dl_resp.status >= 400:
                                return None
                            content = await dl_resp.read()

                        url_or_key = entry.get("url") or entry.get("key", "")
                        objects_meta[url_or_key] = {
                            "etag": entry.get("checksum", ""),
                            "size": len(content),
                            "last_modified": entry.get("last_modified", ""),
                            "fetched_at": datetime.now(timezone.utc).isoformat(),
                            "status": "ingested",
                        }

                        payload = RawPayload(
                            stream_id=self.stream_id,
                            fetched_at=datetime.now(timezone.utc),
                            content=content,
                            content_type=dl_resp.content_type or "application/octet-stream",
                            http_status=200,
                            headers={
                                "source_object_key": url_or_key,
                                "object_size_bytes": str(len(content)),
                            },
                        )

                        if delay > 0:
                            await asyncio.sleep(delay)
                        return payload
                    except Exception as exc:
                        self._log.error("manifest_download_error", url=url, error=str(exc))
                        return None

            tasks = [download_one(e) for e in candidates]
            results = await asyncio.gather(*tasks)
            payloads = [p for p in results if p is not None]

        return payloads

    def _parse_manifest_entries(self, content: bytes, fmt: str) -> list[dict[str, Any]]:
        """Parse a manifest file into a list of object entries."""
        text = content.decode("utf-8")
        if fmt == "json":
            data = json.loads(text)
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return data.get("files", data.get("objects", [data]))
            return []
        elif fmt == "csv":
            reader = csv.DictReader(io.StringIO(text))
            return [dict(row) for row in reader]
        elif fmt == "text_lines":
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            return [{"url": line} for line in lines]
        return []

    async def _fetch_http_bulk(self) -> list[RawPayload]:
        """Fetch objects from static URLs or an index page."""
        bulk_urls = self._get_cfg("bulk_urls", [])
        bulk_index_url = self._get_cfg("bulk_index_url")
        link_pattern = self._get_cfg("link_pattern")
        max_concurrent = self._get_cfg("max_concurrent_downloads", 5)
        ext_filter = self._get_cfg("file_extension_filter", [])
        delay = self._get_cfg("download_delay_seconds", 0)

        headers = {"User-Agent": "HYDRA/0.1.0"}
        headers.update(self._build_auth_headers())

        urls: list[str] = list(bulk_urls or [])

        async with aiohttp.ClientSession() as session:
            # Scrape index page if declared
            if bulk_index_url:
                async with session.get(bulk_index_url, headers=headers) as resp:
                    if resp.status < 400:
                        page_text = await resp.text()
                        if link_pattern:
                            found = re.findall(link_pattern, page_text)
                            # Resolve relative URLs
                            base = bulk_index_url.rsplit("/", 1)[0]
                            for link in found:
                                if not link.startswith("http"):
                                    link = f"{base}/{link}"
                                urls.append(link)

            # Extension filter
            if ext_filter:
                urls = [u for u in urls if any(u.endswith(ext) for ext in ext_filter)]

            # Differential sync
            objects_meta = self._manifest.get("objects", {})
            candidates = [u for u in urls if u not in objects_meta]

            # Download
            semaphore = asyncio.Semaphore(max_concurrent)
            payloads: list[RawPayload] = []

            async def download_one(url: str) -> RawPayload | None:
                async with semaphore:
                    try:
                        async with session.get(url, headers=headers) as dl_resp:
                            if dl_resp.status >= 400:
                                return None
                            content = await dl_resp.read()

                        objects_meta[url] = {
                            "etag": "",
                            "size": len(content),
                            "last_modified": "",
                            "fetched_at": datetime.now(timezone.utc).isoformat(),
                            "status": "ingested",
                        }

                        payload = RawPayload(
                            stream_id=self.stream_id,
                            fetched_at=datetime.now(timezone.utc),
                            content=content,
                            content_type=dl_resp.content_type or "application/octet-stream",
                            http_status=200,
                            headers={
                                "source_object_key": url,
                                "object_size_bytes": str(len(content)),
                            },
                        )

                        if delay > 0:
                            await asyncio.sleep(delay)
                        return payload
                    except Exception as exc:
                        self._log.error("http_bulk_download_error", url=url, error=str(exc))
                        return None

            tasks = [download_one(u) for u in candidates]
            results = await asyncio.gather(*tasks)
            payloads = [p for p in results if p is not None]

        return payloads

    # -- parse --------------------------------------------------------------

    def parse(self, raw: RawPayload) -> list[dict[str, Any]]:
        """Parse S3/Bulk response with format-aware parser dispatch."""
        if not raw.content:
            return []

        # Handle bulk envelope
        if raw.content_type == "application/json+bulk":
            import orjson

            envelope = orjson.loads(raw.content)
            all_records: list[dict[str, Any]] = []
            for payload_info in envelope.get("_payloads", []):
                content_bytes = (
                    payload_info["content"].encode("utf-8")
                    if payload_info.get("content")
                    else None
                )
                temp_file = payload_info.get("temp_file")
                if temp_file and os.path.exists(temp_file):
                    with open(temp_file, "rb") as f:
                        content_bytes = f.read()

                if not content_bytes:
                    continue

                headers = payload_info.get("headers", {})
                source_key = headers.get("source_object_key", headers.get("s3_key", ""))
                source_bucket = headers.get("source_bucket", "")
                object_size = headers.get("object_size_bytes", "0")

                records = self._dispatch_parser(content_bytes, source_key, payload_info.get("content_type", ""))
                for rec in records:
                    rec["source_object_key"] = source_key
                    rec["source_bucket"] = source_bucket
                    rec["object_size_bytes"] = int(object_size) if object_size else 0
                all_records.extend(records)
            return all_records

        # Single payload
        source_key = raw.headers.get("source_object_key", raw.headers.get("s3_key", ""))
        source_bucket = raw.headers.get("source_bucket", "")
        object_size = raw.headers.get("object_size_bytes", "0")

        records = self._dispatch_parser(raw.content, source_key, raw.content_type)
        for rec in records:
            rec["source_object_key"] = source_key
            rec["source_bucket"] = source_bucket
            rec["object_size_bytes"] = int(object_size) if object_size else 0
        return records

    def _dispatch_parser(self, content: bytes, source_key: str, content_type: str) -> list[dict[str, Any]]:
        """Route to the correct parser based on extension, content_type, or override."""
        parser_override = self._get_cfg("parser_override")
        if parser_override:
            return self._parse_by_name(content, parser_override)

        ext = _get_extension(source_key)
        ct = (content_type or "").lower()

        if ext in (".json",) or "json" in ct:
            return self._parse_json(content)
        elif ext in (".csv",) or "csv" in ct:
            return self._parse_csv(content)
        elif ext in (".fits",) or "fits" in ct:
            return self._parse_fits(content)
        elif ext in (".nc", ".nc4") or "netcdf" in ct:
            return self._parse_netcdf(content)
        elif ext in (".hdf5", ".h5") or "hdf5" in ct:
            return self._parse_hdf5(content)
        elif ext in (".tif", ".tiff") or "tiff" in ct:
            return self._parse_geotiff(content)
        elif ext in (".mseed",) or "mseed" in ct:
            return self._parse_miniseed(content)
        elif ext in (".warc",) or source_key.endswith(".warc.gz") or "warc" in ct:
            return self._parse_warc(content)
        else:
            # Default: try JSON
            return self._parse_json(content)

    def _parse_by_name(self, content: bytes, parser_name: str) -> list[dict[str, Any]]:
        """Parse using an explicitly named parser."""
        parsers = {
            "json": self._parse_json,
            "csv": self._parse_csv,
            "fits": self._parse_fits,
            "netcdf": self._parse_netcdf,
            "hdf5": self._parse_hdf5,
            "geotiff": self._parse_geotiff,
            "miniseed": self._parse_miniseed,
            "warc": self._parse_warc,
        }
        parser = parsers.get(parser_name, self._parse_json)
        return parser(content)

    def _parse_json(self, content: bytes) -> list[dict[str, Any]]:
        """Parse JSON content."""
        try:
            data = json.loads(content)
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return [data]
            return []
        except Exception as exc:
            raise ParseError(f"JSON parse error for {self.stream_id}: {exc}") from exc

    def _parse_csv(self, content: bytes) -> list[dict[str, Any]]:
        """Parse CSV content."""
        try:
            text = content.decode("utf-8")
            reader = csv.DictReader(io.StringIO(text))
            return [{k.strip(): (v.strip() if v else v) for k, v in row.items()} for row in reader]
        except Exception as exc:
            raise ParseError(f"CSV parse error for {self.stream_id}: {exc}") from exc

    def _parse_fits(self, content: bytes) -> list[dict[str, Any]]:
        """Parse FITS binary table extensions."""
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
                    # Capture header metadata as sidecar
                    for key in hdu.header:
                        if key:
                            header_meta[key] = str(hdu.header[key])

                    table = Table.read(io.BytesIO(content), hdu=hdu.name)
                    for row in table:
                        rec: dict[str, Any] = {}
                        for col in table.colnames:
                            val = row[col]
                            if hasattr(val, "item"):
                                val = val.item()
                            if isinstance(val, bytes):
                                val = val.decode("utf-8", errors="replace").strip()
                            rec[col] = val
                        rec["_header_meta"] = header_meta
                        records.append(rec)
                    break

            hdul.close()
            return records
        except ParseError:
            raise
        except Exception as exc:
            raise ParseError(f"FITS parse error for {self.stream_id}: {exc}") from exc

    def _parse_netcdf(self, content: bytes) -> list[dict[str, Any]]:
        """Parse NetCDF files using xarray."""
        try:
            import xarray as xr
        except ImportError:
            raise ParseError("xarray is required for NetCDF parsing")

        variables = self._get_cfg("netcdf_variables", [])
        flatten_dim = self._get_cfg("netcdf_flatten_dim", "time")

        try:
            ds = xr.open_dataset(io.BytesIO(content))
            records: list[dict[str, Any]] = []

            if not variables:
                variables = list(ds.data_vars)

            # Flatten along the declared dimension
            if flatten_dim in ds.dims:
                for i in range(ds.dims[flatten_dim]):
                    rec: dict[str, Any] = {}
                    for var in variables:
                        if var in ds:
                            val = ds[var].isel({flatten_dim: i}).values
                            if hasattr(val, "item"):
                                val = val.item()
                            rec[var] = val
                    rec[flatten_dim] = str(ds[flatten_dim].values[i])
                    records.append(rec)
            else:
                # Single record with all variables
                rec = {}
                for var in variables:
                    if var in ds:
                        val = ds[var].values
                        if hasattr(val, "tolist"):
                            val = val.tolist()
                        rec[var] = val
                records.append(rec)

            ds.close()
            return records
        except ParseError:
            raise
        except Exception as exc:
            raise ParseError(f"NetCDF parse error for {self.stream_id}: {exc}") from exc

    def _parse_hdf5(self, content: bytes) -> list[dict[str, Any]]:
        """Parse HDF5 datasets."""
        try:
            import h5py
        except ImportError:
            raise ParseError("h5py is required for HDF5 parsing")

        dataset_path = self._get_cfg("hdf5_dataset_path", "/")
        columns = self._get_cfg("hdf5_columns", [])

        try:
            f = h5py.File(io.BytesIO(content), "r")
            dataset = f[dataset_path]
            data = dataset[:]
            records: list[dict[str, Any]] = []

            if columns and len(columns) == data.shape[-1] if len(data.shape) > 1 else False:
                for row in data:
                    rec = {}
                    for i, col in enumerate(columns):
                        val = row[i]
                        if hasattr(val, "item"):
                            val = val.item()
                        rec[col] = val
                    records.append(rec)
            elif hasattr(dataset, "dtype") and dataset.dtype.names:
                # Structured array
                for row in data:
                    rec = {}
                    for name in dataset.dtype.names:
                        val = row[name]
                        if hasattr(val, "item"):
                            val = val.item()
                        if isinstance(val, bytes):
                            val = val.decode("utf-8", errors="replace")
                        rec[name] = val
                    records.append(rec)
            else:
                for i, row in enumerate(data):
                    val = row
                    if hasattr(val, "tolist"):
                        val = val.tolist()
                    records.append({"index": i, "value": val})

            f.close()
            return records
        except ParseError:
            raise
        except Exception as exc:
            raise ParseError(f"HDF5 parse error for {self.stream_id}: {exc}") from exc

    def _parse_geotiff(self, content: bytes) -> list[dict[str, Any]]:
        """Extract metadata from GeoTIFF files (metadata only, no raster data)."""
        rec: dict[str, Any] = {"_parser": "geotiff"}
        try:
            # Try rasterio if available
            import rasterio
            from rasterio.io import MemoryFile

            with MemoryFile(content) as memfile:
                with memfile.open() as dataset:
                    rec["bounds"] = {
                        "left": dataset.bounds.left,
                        "bottom": dataset.bounds.bottom,
                        "right": dataset.bounds.right,
                        "top": dataset.bounds.top,
                    }
                    rec["crs"] = str(dataset.crs) if dataset.crs else None
                    rec["resolution"] = {"x": dataset.res[0], "y": dataset.res[1]}
                    rec["band_count"] = dataset.count
                    rec["width"] = dataset.width
                    rec["height"] = dataset.height
        except ImportError:
            # Fallback: minimal TIFF header parsing
            rec["bounds"] = None
            rec["crs"] = None
            rec["resolution"] = None
            rec["band_count"] = None
            rec["_note"] = "rasterio not available; metadata extraction limited"
        except Exception as exc:
            raise ParseError(f"GeoTIFF parse error for {self.stream_id}: {exc}") from exc

        return [rec]

    def _parse_miniseed(self, content: bytes) -> list[dict[str, Any]]:
        """Extract header-only metadata from miniSEED binary data."""
        try:
            import obspy

            stream = obspy.read(io.BytesIO(content), headonly=True)
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
        except ImportError:
            self._log.warning("obspy_not_installed", msg="Cannot parse miniSEED without obspy")
            return [{"_parser": "miniseed", "_note": "obspy not available"}]
        except Exception as exc:
            raise ParseError(f"miniSEED parse error for {self.stream_id}: {exc}") from exc

    def _parse_warc(self, content: bytes) -> list[dict[str, Any]]:
        """Extract WARC record headers without decompressing payloads."""
        records: list[dict[str, Any]] = []
        try:
            text = content.decode("utf-8", errors="replace")
            # Split on WARC record boundaries
            warc_blocks = text.split("WARC/1.")
            for block in warc_blocks:
                if not block.strip():
                    continue
                rec: dict[str, Any] = {"_parser": "warc"}
                lines = block.split("\r\n")
                for line in lines:
                    if ":" in line:
                        key, _, val = line.partition(":")
                        key = key.strip()
                        val = val.strip()
                        if key in ("WARC-Type", "WARC-Target-URI", "WARC-Date", "Content-Length"):
                            rec[key] = val
                if len(rec) > 1:  # Has at least one header beyond _parser
                    records.append(rec)
        except Exception as exc:
            raise ParseError(f"WARC parse error for {self.stream_id}: {exc}") from exc

        return records if records else [{"_parser": "warc", "_note": "no records found"}]

    # -- validate -----------------------------------------------------------

    def validate(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Validate S3/Bulk records with format-specific checks and dedup."""
        required_fields = self._get_cfg("required_fields", [])
        expected_hdu_names = self._get_cfg("expected_hdu_names", [])

        valid: list[dict[str, Any]] = []
        seen_hashes: set[str] = set()

        for rec in records:
            # Required-field validation
            if required_fields:
                missing = [f for f in required_fields if rec.get(f) is None]
                if missing:
                    self._log.warning("missing_required_fields", fields=missing)
                    continue

            # Deduplication via raw hash
            raw_bytes = _dict_to_bytes_for_dedup(rec)
            raw_hash = compute_raw_hash(raw_bytes)
            if raw_hash in seen_hashes:
                self._log.debug("duplicate_record", raw_hash=raw_hash)
                continue
            seen_hashes.add(raw_hash)

            valid.append(rec)

        return valid

    def validate_checksum(self, content: bytes, expected_checksum: str, algorithm: str = "md5") -> bool:
        """Verify content checksum against expected value."""
        if algorithm == "md5":
            actual = hashlib.md5(content).hexdigest()
        elif algorithm in ("sha256", "sha-256"):
            actual = hashlib.sha256(content).hexdigest()
        else:
            return True  # Unknown algorithm, skip

        if actual != expected_checksum:
            self._log.error(
                "checksum_mismatch",
                expected=expected_checksum,
                actual=actual,
                algorithm=algorithm,
            )
            return False
        return True

    def validate_size(self, content: bytes, expected_size: int) -> bool:
        """Verify content size against expected value."""
        actual = len(content)
        if actual != expected_size:
            self._log.error(
                "size_mismatch",
                expected=expected_size,
                actual=actual,
            )
            return False
        return True


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _get_extension(key: str) -> str:
    """Extract file extension from an S3 key or URL."""
    # Handle .warc.gz specially
    if key.endswith(".warc.gz"):
        return ".warc"
    base = key.rsplit("?", 1)[0]  # Strip query params
    _, ext = os.path.splitext(base)
    return ext.lower()


def _dict_to_bytes_for_dedup(d: dict[str, Any]) -> bytes:
    """Serialize a dict to bytes for hashing, excluding internal metadata keys."""
    import orjson

    clean = {k: v for k, v in d.items() if not k.startswith("_")}
    return orjson.dumps(clean, option=orjson.OPT_SORT_KEYS)
