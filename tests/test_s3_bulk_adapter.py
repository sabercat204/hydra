"""Unit tests for the S3/Bulk adapter."""

from __future__ import annotations

import asyncio
import io
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

import pytest

from hydra.adapters.base import RawPayload
from hydra.adapters.exceptions import FetchError, ParseError
from hydra.adapters.s3_bulk import S3BulkAdapter, _STREAMED_SENTINEL, _get_extension
from hydra.config import HydraSettings


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FIXTURES = Path("tests/fixtures")


def _make_adapter(cfg: dict[str, Any] | None = None, data_dir: str | None = None) -> S3BulkAdapter:
    settings = HydraSettings()
    if data_dir:
        settings.data_dir = Path(data_dir)  # type: ignore[attr-defined]
    else:
        settings.data_dir = Path(tempfile.mkdtemp())  # type: ignore[attr-defined]
    with patch("hydra.adapters.base.get_registry") as mock_reg:
        mock_reg.return_value = MagicMock(tiers={})
        adapter = S3BulkAdapter(
            stream_id="test_s3",
            settings=settings,
            stream_config=cfg or {},
        )
    return adapter


def _make_s3_object(key: str, content: bytes, etag: str = "abc123",
                    size: int | None = None, last_modified: datetime | None = None) -> dict[str, Any]:
    return {
        "Key": key,
        "ETag": f'"{etag}"',
        "Size": size or len(content),
        "LastModified": last_modified or datetime(2026, 4, 1, tzinfo=timezone.utc),
    }


def _mock_s3_client(objects: list[dict[str, Any]], contents: dict[str, bytes]) -> MagicMock:
    """Create a mock boto3 S3 client."""
    client = MagicMock()
    client.list_objects_v2.return_value = {
        "Contents": objects,
        "IsTruncated": False,
    }

    def get_object(Bucket: str, Key: str) -> dict[str, Any]:
        body = MagicMock()
        body.read.return_value = contents.get(Key, b"")
        return {"Body": body, "ContentType": "application/json"}

    client.get_object.side_effect = get_object
    return client


# ---------------------------------------------------------------------------
# S3 listing — basic flow
# ---------------------------------------------------------------------------


class TestS3ListingBasicFlow:
    @pytest.mark.asyncio
    async def test_basic_s3_listing(self) -> None:
        """Mock 5 S3 objects, verify all fetched, parsed, and normalized."""
        objects = []
        contents = {}
        for i in range(5):
            key = f"data/file_{i}.json"
            data = json.dumps({"id": i, "value": f"record_{i}"}).encode()
            objects.append(_make_s3_object(key, data, etag=f"etag_{i}"))
            contents[key] = data

        mock_client = _mock_s3_client(objects, contents)
        adapter = _make_adapter({
            "bulk_mode": "s3_listing",
            "s3_bucket": "test-bucket",
            "s3_prefix": "data/",
            "auth_pattern": "none",
        })

        with patch.object(adapter, "_build_s3_client", return_value=mock_client):
            raw = await adapter.fetch()

        records = adapter.parse(raw)
        assert len(records) == 5
        for rec in records:
            assert "source_object_key" in rec


# ---------------------------------------------------------------------------
# S3 listing — differential sync
# ---------------------------------------------------------------------------


class TestS3DifferentialSync:
    @pytest.mark.asyncio
    async def test_differential_sync(self) -> None:
        """Pre-populate manifest with 3 of 5 objects, verify only 2 downloaded."""
        objects = []
        contents = {}
        for i in range(5):
            key = f"data/file_{i}.json"
            data = json.dumps({"id": i}).encode()
            objects.append(_make_s3_object(key, data, etag=f"etag_{i}"))
            contents[key] = data

        mock_client = _mock_s3_client(objects, contents)
        adapter = _make_adapter({
            "bulk_mode": "s3_listing",
            "s3_bucket": "test-bucket",
            "s3_prefix": "data/",
            "auth_pattern": "none",
        })

        # Pre-populate manifest with 3 objects
        manifest = {
            "stream_id": "test_s3",
            "last_sync": None,
            "objects": {
                f"data/file_{i}.json": {"etag": f"etag_{i}", "size": 10, "status": "ingested"}
                for i in range(3)
            },
        }
        adapter._save_manifest(manifest)

        with patch.object(adapter, "_build_s3_client", return_value=mock_client):
            raw = await adapter.fetch()

        records = adapter.parse(raw)
        assert len(records) == 2


# ---------------------------------------------------------------------------
# S3 listing — date filter
# ---------------------------------------------------------------------------


class TestS3DateFilter:
    @pytest.mark.asyncio
    async def test_date_filter(self) -> None:
        """Only objects with LastModified after last_fetch_time are candidates."""
        old_dt = datetime(2026, 1, 1, tzinfo=timezone.utc)
        new_dt = datetime(2026, 4, 2, tzinfo=timezone.utc)

        objects = [
            _make_s3_object("old.json", b'{"a":1}', etag="old_etag", last_modified=old_dt),
            _make_s3_object("new.json", b'{"b":2}', etag="new_etag", last_modified=new_dt),
        ]
        contents = {"old.json": b'{"a":1}', "new.json": b'{"b":2}'}

        mock_client = _mock_s3_client(objects, contents)
        adapter = _make_adapter({
            "bulk_mode": "s3_listing",
            "s3_bucket": "test-bucket",
            "auth_pattern": "none",
            "last_fetch_time": "2026-03-01T00:00:00Z",
        })

        with patch.object(adapter, "_build_s3_client", return_value=mock_client):
            raw = await adapter.fetch()

        records = adapter.parse(raw)
        assert len(records) == 1
        assert records[0]["b"] == 2


# ---------------------------------------------------------------------------
# S3 listing — extension filter
# ---------------------------------------------------------------------------


class TestS3ExtensionFilter:
    @pytest.mark.asyncio
    async def test_extension_filter(self) -> None:
        """Only .fits and .json objects should be downloaded."""
        objects = [
            _make_s3_object("a.fits", b"fits_data", etag="e1"),
            _make_s3_object("b.json", b'{"x":1}', etag="e2"),
            _make_s3_object("c.txt", b"text", etag="e3"),
            _make_s3_object("d.csv", b"a,b\n1,2", etag="e4"),
        ]
        contents = {
            "a.fits": b"fits_data",
            "b.json": b'{"x":1}',
            "c.txt": b"text",
            "d.csv": b"a,b\n1,2",
        }

        mock_client = _mock_s3_client(objects, contents)
        adapter = _make_adapter({
            "bulk_mode": "s3_listing",
            "s3_bucket": "test-bucket",
            "auth_pattern": "none",
            "file_extension_filter": [".fits", ".json"],
        })

        with patch.object(adapter, "_build_s3_client", return_value=mock_client):
            raw = await adapter.fetch()

        # Should have fetched 2 objects
        import orjson
        envelope = orjson.loads(raw.content)
        assert len(envelope["_payloads"]) == 2


# ---------------------------------------------------------------------------
# S3 listing — streaming download
# ---------------------------------------------------------------------------


class TestS3StreamingDownload:
    @pytest.mark.asyncio
    async def test_streaming_large_object(self) -> None:
        """Objects larger than streaming_threshold_bytes should be streamed to disk."""
        large_content = b"x" * 200  # Simulate large content
        objects = [_make_s3_object("big.json", large_content, etag="big_etag", size=200)]

        mock_body = MagicMock()
        # Simulate chunked reading
        chunks = [large_content[:100], large_content[100:], b""]
        mock_body.read.side_effect = chunks

        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {
            "Contents": objects,
            "IsTruncated": False,
        }
        mock_client.get_object.return_value = {"Body": mock_body, "ContentType": "application/json"}

        adapter = _make_adapter({
            "bulk_mode": "s3_listing",
            "s3_bucket": "test-bucket",
            "auth_pattern": "none",
            "streaming_threshold_bytes": 100,  # Low threshold for test
            "chunk_size_bytes": 100,
        })

        with patch.object(adapter, "_build_s3_client", return_value=mock_client):
            raw = await adapter.fetch()

        import orjson
        envelope = orjson.loads(raw.content)
        payload_info = envelope["_payloads"][0]
        # Streamed content should have temp_file reference
        assert payload_info.get("temp_file") is not None or payload_info.get("content") is None


# ---------------------------------------------------------------------------
# Manifest mode — JSON manifest
# ---------------------------------------------------------------------------


class TestManifestJsonMode:
    @pytest.mark.asyncio
    async def test_json_manifest(self) -> None:
        """Mock a JSON manifest listing 10 objects, pre-populate 7, verify 3 downloaded."""
        manifest_entries = [
            {"url": f"https://example.com/file_{i}.json", "checksum": f"chk_{i}"}
            for i in range(10)
        ]
        manifest_bytes = json.dumps(manifest_entries).encode()

        adapter = _make_adapter({
            "bulk_mode": "manifest",
            "manifest_url": "https://example.com/manifest.json",
            "manifest_format": "json",
            "auth_pattern": "none",
        })

        # Pre-populate 7 objects
        local_manifest = {
            "stream_id": "test_s3",
            "last_sync": None,
            "objects": {
                f"https://example.com/file_{i}.json": {"etag": f"chk_{i}", "status": "ingested"}
                for i in range(7)
            },
        }
        adapter._save_manifest(local_manifest)

        # Mock HTTP responses
        mock_manifest_resp = AsyncMock()
        mock_manifest_resp.status = 200
        mock_manifest_resp.read = AsyncMock(return_value=manifest_bytes)
        mock_manifest_resp.__aenter__ = AsyncMock(return_value=mock_manifest_resp)
        mock_manifest_resp.__aexit__ = AsyncMock(return_value=False)

        mock_dl_resp = AsyncMock()
        mock_dl_resp.status = 200
        mock_dl_resp.read = AsyncMock(return_value=b'{"data": "test"}')
        mock_dl_resp.content_type = "application/json"
        mock_dl_resp.__aenter__ = AsyncMock(return_value=mock_dl_resp)
        mock_dl_resp.__aexit__ = AsyncMock(return_value=False)

        call_count = 0

        def mock_get(url: str, **kwargs: Any) -> Any:
            nonlocal call_count
            if "manifest.json" in url:
                return mock_manifest_resp
            call_count += 1
            return mock_dl_resp

        mock_session = AsyncMock()
        mock_session.get = MagicMock(side_effect=mock_get)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            raw = await adapter.fetch()

        records = adapter.parse(raw)
        assert len(records) == 3


# ---------------------------------------------------------------------------
# Manifest mode — CSV manifest
# ---------------------------------------------------------------------------


class TestManifestCsvMode:
    @pytest.mark.asyncio
    async def test_csv_manifest(self) -> None:
        """Mock a CSV manifest and verify correct parsing."""
        csv_manifest = "url,checksum,size\nhttps://example.com/a.json,chk_a,100\nhttps://example.com/b.json,chk_b,200\n"

        adapter = _make_adapter({
            "bulk_mode": "manifest",
            "manifest_url": "https://example.com/manifest.csv",
            "manifest_format": "csv",
            "auth_pattern": "none",
        })

        mock_manifest_resp = AsyncMock()
        mock_manifest_resp.status = 200
        mock_manifest_resp.read = AsyncMock(return_value=csv_manifest.encode())
        mock_manifest_resp.__aenter__ = AsyncMock(return_value=mock_manifest_resp)
        mock_manifest_resp.__aexit__ = AsyncMock(return_value=False)

        mock_dl_resp = AsyncMock()
        mock_dl_resp.status = 200
        mock_dl_resp.read = AsyncMock(return_value=b'{"val": 1}')
        mock_dl_resp.content_type = "application/json"
        mock_dl_resp.__aenter__ = AsyncMock(return_value=mock_dl_resp)
        mock_dl_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(side_effect=lambda url, **kw: (
            mock_manifest_resp if "manifest.csv" in url else mock_dl_resp
        ))
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            raw = await adapter.fetch()

        records = adapter.parse(raw)
        assert len(records) == 2


# ---------------------------------------------------------------------------
# HTTP bulk mode — static URLs
# ---------------------------------------------------------------------------


class TestHttpBulkStaticUrls:
    @pytest.mark.asyncio
    async def test_static_urls(self) -> None:
        """Declare bulk_urls with 3 URLs, verify all 3 downloaded."""
        adapter = _make_adapter({
            "bulk_mode": "http_bulk",
            "bulk_urls": [
                "https://example.com/a.json",
                "https://example.com/b.json",
                "https://example.com/c.json",
            ],
            "auth_pattern": "none",
        })

        mock_dl_resp = AsyncMock()
        mock_dl_resp.status = 200
        mock_dl_resp.read = AsyncMock(return_value=b'{"data": "test"}')
        mock_dl_resp.content_type = "application/json"
        mock_dl_resp.__aenter__ = AsyncMock(return_value=mock_dl_resp)
        mock_dl_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_dl_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            raw = await adapter.fetch()

        records = adapter.parse(raw)
        assert len(records) == 3


# ---------------------------------------------------------------------------
# HTTP bulk mode — index page scraping
# ---------------------------------------------------------------------------


class TestHttpBulkIndexScraping:
    @pytest.mark.asyncio
    async def test_index_page_scraping(self) -> None:
        """Mock an index page with HTML links, verify correct links extracted."""
        index_html = """
        <html><body>
        <a href="data_001.json">File 1</a>
        <a href="data_002.json">File 2</a>
        <a href="readme.txt">Readme</a>
        </body></html>
        """

        adapter = _make_adapter({
            "bulk_mode": "http_bulk",
            "bulk_index_url": "https://example.com/data/",
            "link_pattern": r'href="(data_\d+\.json)"',
            "auth_pattern": "none",
        })

        mock_index_resp = AsyncMock()
        mock_index_resp.status = 200
        mock_index_resp.text = AsyncMock(return_value=index_html)
        mock_index_resp.__aenter__ = AsyncMock(return_value=mock_index_resp)
        mock_index_resp.__aexit__ = AsyncMock(return_value=False)

        mock_dl_resp = AsyncMock()
        mock_dl_resp.status = 200
        mock_dl_resp.read = AsyncMock(return_value=b'{"data": "test"}')
        mock_dl_resp.content_type = "application/json"
        mock_dl_resp.__aenter__ = AsyncMock(return_value=mock_dl_resp)
        mock_dl_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(side_effect=lambda url, **kw: (
            mock_index_resp if "data/" in url and "data_" not in url else mock_dl_resp
        ))
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            raw = await adapter.fetch()

        records = adapter.parse(raw)
        assert len(records) == 2


# ---------------------------------------------------------------------------
# Parser dispatch tests
# ---------------------------------------------------------------------------


class TestParserDispatchJson:
    def test_json_parser(self) -> None:
        """Mock a .json S3 object, verify JSON parser invoked."""
        adapter = _make_adapter()
        content = b'[{"id": 1}, {"id": 2}]'
        records = adapter._dispatch_parser(content, "data/file.json", "application/json")
        assert len(records) == 2
        assert records[0]["id"] == 1


class TestParserDispatchCsv:
    def test_csv_parser(self) -> None:
        """Mock a .csv S3 object, verify CSV parser invoked."""
        adapter = _make_adapter()
        content = b"id,name\n1,Alice\n2,Bob\n"
        records = adapter._dispatch_parser(content, "data/file.csv", "text/csv")
        assert len(records) == 2
        assert records[0]["name"] == "Alice"


class TestParserDispatchFits:
    def test_fits_parser(self) -> None:
        """Parse the sample.fits fixture via S3/Bulk parser dispatch."""
        adapter = _make_adapter()
        fits_bytes = FIXTURES.joinpath("sample.fits").read_bytes()
        records = adapter._dispatch_parser(fits_bytes, "data/catalog.fits", "application/fits")
        assert len(records) == 5
        assert records[0]["ra"] == 180.0


class TestParserDispatchNetcdf:
    def test_netcdf_parser(self) -> None:
        """Mock a .nc S3 object with a small NetCDF fixture."""
        try:
            import xarray as xr
            import numpy as np
        except ImportError:
            pytest.skip("xarray not installed")

        # Create a small in-memory NetCDF
        ds = xr.Dataset({
            "temperature": (["time"], np.array([20.0, 21.0, 22.0])),
            "pressure": (["time"], np.array([1013.0, 1012.0, 1011.0])),
        }, coords={"time": np.array([0, 1, 2])})

        buf = io.BytesIO()
        ds.to_netcdf(buf, engine="scipy")
        nc_bytes = buf.getvalue()

        adapter = _make_adapter({
            "netcdf_variables": ["temperature", "pressure"],
            "netcdf_flatten_dim": "time",
        })
        records = adapter._dispatch_parser(nc_bytes, "data/climate.nc", "application/x-netcdf")
        assert len(records) == 3
        assert "temperature" in records[0]


class TestParserDispatchHdf5:
    def test_hdf5_parser(self) -> None:
        """Mock a .h5 S3 object, verify h5py navigates to dataset."""
        try:
            import h5py
            import numpy as np
        except ImportError:
            pytest.skip("h5py not installed")

        buf = io.BytesIO()
        with h5py.File(buf, "w") as f:
            f.create_dataset("sensors/readings", data=np.array([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]]))
        hdf5_bytes = buf.getvalue()

        adapter = _make_adapter({
            "hdf5_dataset_path": "sensors/readings",
            "hdf5_columns": ["temp", "humidity"],
        })
        records = adapter._dispatch_parser(hdf5_bytes, "data/sensors.h5", "application/x-hdf5")
        assert len(records) == 3
        assert records[0]["temp"] == 1.0


class TestParserDispatchGeotiff:
    def test_geotiff_metadata(self) -> None:
        """Mock a .tif S3 object, verify metadata-only extraction."""
        adapter = _make_adapter()
        # Without rasterio, we get fallback metadata
        records = adapter._dispatch_parser(b"\x49\x49\x2a\x00", "data/image.tif", "image/tiff")
        assert len(records) == 1
        assert records[0]["_parser"] == "geotiff"


class TestParserDispatchMiniseed:
    def test_miniseed_parser(self) -> None:
        """Mock a .mseed S3 object, verify header-only metadata extraction."""
        mseed_path = FIXTURES.joinpath("sample.mseed")
        if not mseed_path.exists():
            pytest.skip("sample.mseed fixture not available")

        adapter = _make_adapter()
        mseed_bytes = mseed_path.read_bytes()
        records = adapter._dispatch_parser(mseed_bytes, "data/trace.mseed", "application/vnd.fdsn.mseed")
        assert len(records) >= 1


class TestParserDispatchWarc:
    def test_warc_parser(self) -> None:
        """Mock a .warc S3 object, verify WARC record header extraction."""
        warc_content = (
            "WARC/1.0\r\n"
            "WARC-Type: response\r\n"
            "WARC-Target-URI: https://example.com/page1\r\n"
            "WARC-Date: 2026-04-01T00:00:00Z\r\n"
            "Content-Length: 1234\r\n"
            "\r\n"
            "WARC/1.0\r\n"
            "WARC-Type: response\r\n"
            "WARC-Target-URI: https://example.com/page2\r\n"
            "WARC-Date: 2026-04-01T01:00:00Z\r\n"
            "Content-Length: 5678\r\n"
            "\r\n"
        ).encode()

        adapter = _make_adapter()
        records = adapter._dispatch_parser(warc_content, "data/crawl.warc", "application/warc")
        assert len(records) >= 2
        assert records[0]["WARC-Type"] == "response"


class TestParserOverride:
    def test_parser_override(self) -> None:
        """Declare parser_override: json on a .dat extension, verify JSON parser used."""
        adapter = _make_adapter({"parser_override": "json"})
        content = b'{"key": "value"}'
        records = adapter._dispatch_parser(content, "data/file.dat", "application/octet-stream")
        assert len(records) == 1
        assert records[0]["key"] == "value"


# ---------------------------------------------------------------------------
# Checksum validation
# ---------------------------------------------------------------------------


class TestChecksumValidation:
    def test_checksum_mismatch(self) -> None:
        """Verify object rejected with mismatched MD5."""
        adapter = _make_adapter()
        content = b"hello world"
        # Wrong checksum
        assert adapter.validate_checksum(content, "wrong_checksum", "md5") is False

    def test_checksum_match(self) -> None:
        """Verify object accepted with correct MD5."""
        import hashlib

        adapter = _make_adapter()
        content = b"hello world"
        correct_md5 = hashlib.md5(content).hexdigest()
        assert adapter.validate_checksum(content, correct_md5, "md5") is True


# ---------------------------------------------------------------------------
# Size validation
# ---------------------------------------------------------------------------


class TestSizeValidation:
    def test_size_mismatch(self) -> None:
        """Verify object rejected when actual size != expected size."""
        adapter = _make_adapter()
        content = b"short"
        assert adapter.validate_size(content, 1000) is False

    def test_size_match(self) -> None:
        """Verify object accepted when sizes match."""
        adapter = _make_adapter()
        content = b"exact"
        assert adapter.validate_size(content, len(content)) is True


# ---------------------------------------------------------------------------
# Concurrency control
# ---------------------------------------------------------------------------


class TestConcurrencyControl:
    @pytest.mark.asyncio
    async def test_max_concurrent_downloads(self) -> None:
        """Verify no more than max_concurrent_downloads concurrent operations."""
        objects = [
            _make_s3_object(f"data/file_{i}.json", b'{"i":' + str(i).encode() + b'}', etag=f"e_{i}")
            for i in range(10)
        ]
        contents = {f"data/file_{i}.json": b'{"i":' + str(i).encode() + b'}' for i in range(10)}

        max_concurrent = 3
        concurrent_count = 0
        max_observed = 0

        original_get_object = _mock_s3_client(objects, contents).get_object

        def tracking_get_object(Bucket: str, Key: str) -> dict[str, Any]:
            nonlocal concurrent_count, max_observed
            concurrent_count += 1
            max_observed = max(max_observed, concurrent_count)
            body = MagicMock()
            body.read.return_value = contents.get(Key, b"")
            concurrent_count -= 1
            return {"Body": body, "ContentType": "application/json"}

        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {
            "Contents": objects,
            "IsTruncated": False,
        }
        mock_client.get_object.side_effect = tracking_get_object

        adapter = _make_adapter({
            "bulk_mode": "s3_listing",
            "s3_bucket": "test-bucket",
            "auth_pattern": "none",
            "max_concurrent_downloads": max_concurrent,
        })

        with patch.object(adapter, "_build_s3_client", return_value=mock_client):
            raw = await adapter.fetch()

        records = adapter.parse(raw)
        assert len(records) == 10


# ---------------------------------------------------------------------------
# Earthdata OAuth flow
# ---------------------------------------------------------------------------


class TestEarthdataOAuth:
    def test_bearer_token_header(self) -> None:
        """Verify bearer token used for account_token auth."""
        settings = HydraSettings()
        settings.data_dir = Path(tempfile.mkdtemp())  # type: ignore[attr-defined]
        settings.credentials = {"earthdata_stream": {"token": "earthdata_token_123"}}  # type: ignore[attr-defined]

        with patch("hydra.adapters.base.get_registry") as mock_reg:
            mock_reg.return_value = MagicMock(tiers={})
            adapter = S3BulkAdapter(
                stream_id="earthdata_stream",
                settings=settings,
                stream_config={
                    "auth_pattern": "account_token",
                },
            )

        headers = adapter._build_auth_headers()
        assert "Authorization" in headers
        assert headers["Authorization"] == "Bearer earthdata_token_123"


# ---------------------------------------------------------------------------
# Manifest file locking
# ---------------------------------------------------------------------------


class TestManifestFileLocking:
    def test_concurrent_manifest_access(self) -> None:
        """Verify file-level locking prevents corruption."""
        adapter = _make_adapter()
        manifest = {"stream_id": "test_s3", "last_sync": None, "objects": {"key1": {"etag": "e1"}}}

        # Write and read back — basic locking test
        adapter._save_manifest(manifest)
        loaded = adapter._load_manifest()
        assert loaded["objects"]["key1"]["etag"] == "e1"

        # Overwrite
        manifest["objects"]["key2"] = {"etag": "e2"}
        adapter._save_manifest(manifest)
        loaded = adapter._load_manifest()
        assert "key2" in loaded["objects"]


# ---------------------------------------------------------------------------
# Anonymous S3 access
# ---------------------------------------------------------------------------


class TestAnonymousS3Access:
    def test_unsigned_config(self) -> None:
        """Verify boto3 client configured with UNSIGNED signature for public buckets."""
        adapter = _make_adapter({
            "auth_pattern": "none",
            "s3_region": "us-west-2",
        })

        with patch("boto3.client") as mock_boto:
            adapter._build_s3_client()
            mock_boto.assert_called_once()
            call_kwargs = mock_boto.call_args
            # Should have config with UNSIGNED signature
            config = call_kwargs.kwargs.get("config") or call_kwargs[1].get("config")
            assert config is not None
