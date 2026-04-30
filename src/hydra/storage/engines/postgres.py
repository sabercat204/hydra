"""PostgreSQL storage engine — primary system of record."""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, List, Optional

from hydra.config import HydraSettings
from hydra.models.normalized import NormalizedRecord
from hydra.storage.engines.base import StorageEngine, StoreResult
from hydra.storage.exceptions import StorageConnectionError, StorageWriteError
from hydra.storage.health import StorageHealth

logger = logging.getLogger(__name__)


class PostgresEngine(StorageEngine):
    """PostgreSQL + PostGIS primary store. Every NormalizedRecord gets a row here."""

    def __init__(self, settings: HydraSettings) -> None:
        self._settings = settings
        self._pool: Any = None
        # Convert asyncpg DSN: strip the +asyncpg dialect suffix
        self._dsn = settings.database.postgres_dsn.replace("+asyncpg", "")

    async def connect(self) -> None:
        """Create the asyncpg connection pool."""
        import asyncpg

        try:
            self._pool = await asyncpg.create_pool(
                dsn=self._dsn,
                min_size=self._settings.database.pg_pool_min,
                max_size=self._settings.database.pg_pool_max,
                command_timeout=30,
            )
        except Exception as exc:
            raise StorageConnectionError("postgres", f"Failed to connect: {exc}", cause=exc) from exc

    async def disconnect(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def store(self, records: list[NormalizedRecord]) -> StoreResult:
        """Insert records into normalized_records table."""
        if not self._pool:
            raise StorageConnectionError("postgres", "Not connected")

        start = time.monotonic()
        stored = 0
        failed = 0
        deduplicated = 0
        errors: list[dict] = []

        async with self._pool.acquire() as conn:
            for record in records:
                try:
                    row = self._serialize_record(record)
                    await conn.execute(
                        """
                        INSERT INTO normalized_records
                            (stream_id, tier, timestamp, geo, payload, source_name,
                             source_url, adapter_type, access_level, raw_hash,
                             ingested_at, confidence, tags, storage_status, storage_engines)
                        VALUES
                            ($1, $2, $3,
                             CASE WHEN $4::text IS NOT NULL THEN ST_GeomFromGeoJSON($4) ELSE NULL END,
                             $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                        ON CONFLICT (raw_hash) DO NOTHING
                        """,
                        row["stream_id"],
                        row["tier"],
                        row["timestamp"],
                        row["geo"],
                        row["payload"],
                        row["source_name"],
                        row["source_url"],
                        row["adapter_type"],
                        row["access_level"],
                        row["raw_hash"],
                        row["ingested_at"],
                        row["confidence"],
                        row["tags"],
                        row["storage_status"],
                        row["storage_engines"],
                    )
                    stored += 1
                except Exception as exc:
                    try:
                        import asyncpg as _asyncpg
                        is_unique_violation = isinstance(exc, _asyncpg.UniqueViolationError)
                    except ImportError:
                        is_unique_violation = "unique" in str(exc).lower() and "violat" in str(exc).lower()

                    if is_unique_violation:
                        deduplicated += 1
                        logger.debug("pg_dedup_catch", extra={"stream_id": record.stream_id, "raw_hash": record.raw_hash})
                    else:
                        failed += 1
                        errors.append({"record_hash": record.raw_hash, "error": str(exc)})
                        logger.error("pg_write_error", extra={"raw_hash": record.raw_hash, "error": str(exc)})

        duration_ms = (time.monotonic() - start) * 1000
        return StoreResult(
            engine="postgres",
            stored=stored,
            failed=failed,
            deduplicated=deduplicated,
            duration_ms=duration_ms,
            errors=errors,
        )

    async def health_check(self) -> StorageHealth:
        start = time.monotonic()
        try:
            if not self._pool:
                return StorageHealth(engine="postgres", status="UNREACHABLE", latency_ms=0.0)
            async with self._pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
                postgis_ok = True
                try:
                    await conn.fetchval("SELECT PostGIS_Version()")
                except Exception:
                    postgis_ok = False
            latency = (time.monotonic() - start) * 1000
            if postgis_ok:
                return StorageHealth(engine="postgres", status="OK", latency_ms=latency, details={"postgis": True})
            return StorageHealth(engine="postgres", status="DEGRADED", latency_ms=latency, details={"postgis": False})
        except Exception as exc:
            latency = (time.monotonic() - start) * 1000
            return StorageHealth(engine="postgres", status="UNREACHABLE", latency_ms=latency, details={"error": str(exc)})

    @staticmethod
    def _serialize_record(record: NormalizedRecord, storage_engines: list[str] | None = None) -> dict[str, Any]:
        """Convert a NormalizedRecord to a dict suitable for PG insertion."""
        geo_json: str | None = None
        if record.geo is not None:
            geo_json = json.dumps({"type": record.geo.type, "coordinates": record.geo.coordinates})

        return {
            "stream_id": record.stream_id,
            "tier": int(record.tier),
            "timestamp": record.timestamp,
            "geo": geo_json,
            "payload": json.dumps(record.payload),
            "source_name": record.source_meta.source_name,
            "source_url": record.source_meta.source_url,
            "adapter_type": record.source_meta.adapter_type,
            "access_level": record.source_meta.access_level,
            "raw_hash": record.raw_hash,
            "ingested_at": record.ingested_at,
            "confidence": record.confidence,
            "tags": record.tags,
            "storage_status": "pending",
            "storage_engines": storage_engines or [],
        }
