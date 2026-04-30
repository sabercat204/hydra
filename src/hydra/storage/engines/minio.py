"""MinIO storage engine — binary blob store for large/binary artifacts."""

from __future__ import annotations

import io
import logging
import time
from datetime import datetime, timezone
from typing import Any

from hydra.config import HydraSettings
from hydra.models.normalized import NormalizedRecord
from hydra.storage.engines.base import StorageEngine, StoreResult
from hydra.storage.exceptions import StorageConnectionError, StorageEngineError
from hydra.storage.health import StorageHealth

logger = logging.getLogger(__name__)

MAX_SINGLE_UPLOAD = 5 * 1024 * 1024 * 1024  # 5 GB


class MinioEngine(StorageEngine):
    """MinIO S3-compatible binary blob store."""

    def __init__(self, settings: HydraSettings, credential_store: Any = None) -> None:
        self._settings = settings
        self._credential_store = credential_store
        self._client: Any = None
        self._created_buckets: set[str] = set()

    async def connect(self) -> None:
        import boto3
        from botocore.config import Config as BotoConfig

        access_key = "minioadmin"
        secret_key = "minioadmin"
        if self._credential_store:
            try:
                creds = self._credential_store.get("minio_admin")
                access_key = creds.get("access_key", access_key)
                secret_key = creds.get("secret_key", secret_key)
            except Exception:
                logger.warning("minio_no_credentials")

        try:
            self._client = boto3.client(
                "s3",
                endpoint_url=self._settings.database.minio_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name="us-east-1",
                config=BotoConfig(signature_version="s3v4"),
            )
        except Exception as exc:
            raise StorageConnectionError("minio", f"Failed to connect: {exc}", cause=exc) from exc

    async def disconnect(self) -> None:
        self._client = None
        self._created_buckets.clear()

    async def store(self, records: list[NormalizedRecord], minio_bucket: str | None = None) -> StoreResult:
        """Upload binary artifacts to MinIO."""
        if not self._client:
            raise StorageConnectionError("minio", "Not connected")

        start = time.monotonic()
        stored = 0
        failed = 0
        errors: list[dict] = []

        for record in records:
            artifact = record.payload.get("_binary_artifact")
            if not artifact or not isinstance(artifact, dict):
                continue

            content = artifact.get("content")
            content_type = artifact.get("content_type", "application/octet-stream")
            original_key = artifact.get("original_key", "unknown")

            if content is None:
                failed += 1
                errors.append({"record_hash": record.raw_hash, "error": "No content in _binary_artifact"})
                continue

            if isinstance(content, str):
                content = content.encode("utf-8")

            if len(content) > MAX_SINGLE_UPLOAD:
                failed += 1
                errors.append({"record_hash": record.raw_hash, "error": "Exceeds 5GB single-part upload limit"})
                continue

            bucket = minio_bucket or f"hydra-tier-{int(record.tier)}"
            self._ensure_bucket(bucket)

            ts = record.timestamp or datetime.now(timezone.utc)
            object_key = (
                f"{record.stream_id}/{ts.strftime('%Y')}/{ts.strftime('%m')}"
                f"/{ts.strftime('%d')}/{record.raw_hash}_{original_key}"
            )

            try:
                self._client.put_object(
                    Bucket=bucket,
                    Key=object_key,
                    Body=io.BytesIO(content),
                    ContentType=content_type,
                    Metadata={
                        "stream_id": record.stream_id,
                        "tier": str(int(record.tier)),
                        "raw_hash": record.raw_hash,
                        "ingested_at": record.ingested_at.isoformat(),
                    },
                )
                # Replace _binary_artifact with reference
                record.payload["_binary_artifact"] = {
                    "bucket": bucket,
                    "key": object_key,
                    "size": len(content),
                    "content_type": content_type,
                }
                stored += 1
            except Exception as exc:
                failed += 1
                errors.append({"record_hash": record.raw_hash, "error": str(exc)})
                logger.error("minio_upload_error", extra={"raw_hash": record.raw_hash, "error": str(exc)})

        duration_ms = (time.monotonic() - start) * 1000
        return StoreResult(engine="minio", stored=stored, failed=failed, duration_ms=duration_ms, errors=errors)

    async def health_check(self) -> StorageHealth:
        start = time.monotonic()
        try:
            if not self._client:
                return StorageHealth(engine="minio", status="UNREACHABLE", latency_ms=0.0)
            self._client.list_buckets()
            latency = (time.monotonic() - start) * 1000
            return StorageHealth(engine="minio", status="OK", latency_ms=latency)
        except Exception as exc:
            latency = (time.monotonic() - start) * 1000
            return StorageHealth(engine="minio", status="UNREACHABLE", latency_ms=latency, details={"error": str(exc)})

    def _ensure_bucket(self, bucket: str) -> None:
        """Create bucket if it doesn't exist yet."""
        if bucket in self._created_buckets:
            return
        try:
            self._client.head_bucket(Bucket=bucket)
        except Exception:
            try:
                self._client.create_bucket(Bucket=bucket)
            except Exception:
                pass
        self._created_buckets.add(bucket)
