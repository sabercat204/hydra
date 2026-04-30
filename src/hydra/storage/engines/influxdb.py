"""InfluxDB storage engine — time-series secondary store."""

from __future__ import annotations

import logging
import time
from typing import Any

from hydra.config import HydraSettings
from hydra.models.normalized import NormalizedRecord
from hydra.storage.engines.base import StorageEngine, StoreResult
from hydra.storage.exceptions import StorageConnectionError, StorageWriteError
from hydra.storage.health import StorageHealth

logger = logging.getLogger(__name__)


class InfluxEngine(StorageEngine):
    """InfluxDB v2 time-series secondary store for high-frequency tiers."""

    def __init__(self, settings: HydraSettings, credential_store: Any = None) -> None:
        self._settings = settings
        self._credential_store = credential_store
        self._client: Any = None
        self._write_api: Any = None

    async def connect(self) -> None:
        from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync

        token = ""
        if self._credential_store:
            try:
                creds = self._credential_store.get("influxdb_admin")
                token = creds.get("token", "")
            except Exception:
                logger.warning("influxdb_no_credentials")

        self._client = InfluxDBClientAsync(
            url=self._settings.database.influxdb_url,
            token=token,
            org=self._settings.database.influxdb_org,
            timeout=30_000,
        )
        self._write_api = self._client.write_api()

        # Create bucket if absent
        try:
            buckets_api = self._client.buckets_api()
            bucket = await buckets_api.find_bucket_by_name(self._settings.database.influxdb_bucket)
            if bucket is None:
                orgs_api = self._client.organizations_api()
                orgs = await orgs_api.find_organizations(org=self._settings.database.influxdb_org)
                if orgs:
                    from influxdb_client import BucketRetentionRules
                    await buckets_api.create_bucket(
                        bucket_name=self._settings.database.influxdb_bucket,
                        org_id=orgs[0].id,
                    )
        except Exception as exc:
            logger.warning("influxdb_bucket_create_failed", extra={"error": str(exc)})

    async def disconnect(self) -> None:
        if self._client:
            await self._client.close()
            self._client = None
            self._write_api = None

    async def store(self, records: list[NormalizedRecord], registry_config: dict | None = None) -> StoreResult:
        """Write records as InfluxDB points."""
        if not self._client or not self._write_api:
            raise StorageConnectionError("influxdb", "Not connected")

        from influxdb_client import Point, WritePrecision

        start = time.monotonic()
        config = registry_config or {}
        influx_fields = config.get("influx_fields", [])
        influx_tag_fields = config.get("influx_tag_fields", ["stream_id", "tier"])

        points: list[Point] = []
        for record in records:
            p = Point(record.stream_id).time(record.timestamp, WritePrecision.MS)
            # Tags
            p.tag("tier", str(int(record.tier)))
            p.tag("stream_id", record.stream_id)
            for tag_field in influx_tag_fields:
                if tag_field in ("stream_id", "tier"):
                    continue
                val = record.payload.get(tag_field)
                if val is not None:
                    p.tag(tag_field, str(val))
            # Fields
            field_count = 0
            for field_name in influx_fields:
                val = record.payload.get(field_name)
                if val is None:
                    continue
                if isinstance(val, (int, float)):
                    p.field(field_name, float(val))
                    field_count += 1
                else:
                    logger.debug("influx_skip_non_numeric", extra={"field": field_name, "type": type(val).__name__})
            if field_count > 0:
                points.append(p)

        stored = 0
        failed = 0
        errors: list[dict] = []

        if points:
            try:
                await self._write_api.write(
                    bucket=self._settings.database.influxdb_bucket,
                    org=self._settings.database.influxdb_org,
                    record=points,
                    write_precision=WritePrecision.MS,
                )
                stored = len(points)
            except Exception as exc:
                failed = len(points)
                for record in records:
                    errors.append({"record_hash": record.raw_hash, "error": str(exc)})
                logger.error("influx_write_error", extra={"error": str(exc)})

        duration_ms = (time.monotonic() - start) * 1000
        return StoreResult(
            engine="influxdb",
            stored=stored,
            failed=failed,
            duration_ms=duration_ms,
            errors=errors,
        )

    async def health_check(self) -> StorageHealth:
        start = time.monotonic()
        try:
            if not self._client:
                return StorageHealth(engine="influxdb", status="UNREACHABLE", latency_ms=0.0)
            ok = await self._client.ping()
            latency = (time.monotonic() - start) * 1000
            status = "OK" if ok else "UNREACHABLE"
            return StorageHealth(engine="influxdb", status=status, latency_ms=latency)
        except Exception as exc:
            latency = (time.monotonic() - start) * 1000
            return StorageHealth(engine="influxdb", status="UNREACHABLE", latency_ms=latency, details={"error": str(exc)})
