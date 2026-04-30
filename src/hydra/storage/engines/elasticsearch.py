"""Elasticsearch storage engine — full-text search secondary store."""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

from hydra.config import HydraSettings
from hydra.models.normalized import NormalizedRecord
from hydra.storage.engines.base import StorageEngine, StoreResult
from hydra.storage.exceptions import StorageConnectionError
from hydra.storage.health import StorageHealth

logger = logging.getLogger(__name__)

INDEX_MAPPING_TEMPLATE: dict[str, Any] = {
    "mappings": {
        "properties": {
            "stream_id": {"type": "keyword"},
            "tier": {"type": "integer"},
            "timestamp": {"type": "date"},
            "geo": {"type": "geo_shape"},
            "payload": {"type": "object", "dynamic": True},
            "source_name": {"type": "keyword"},
            "source_url": {"type": "keyword"},
            "raw_hash": {"type": "keyword"},
            "confidence": {"type": "float"},
            "tags": {"type": "keyword"},
            "ingested_at": {"type": "date"},
        },
        "dynamic_templates": [
            {
                "strings_as_text": {
                    "match_mapping_type": "string",
                    "mapping": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                    },
                }
            }
        ],
    },
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "refresh_interval": "5s",
    },
}


class ElasticsearchEngine(StorageEngine):
    """Elasticsearch full-text search secondary store for text-heavy tiers."""

    def __init__(self, settings: HydraSettings) -> None:
        self._settings = settings
        self._client: Any = None

    async def connect(self) -> None:
        from elasticsearch import AsyncElasticsearch

        self._client = AsyncElasticsearch(
            hosts=[self._settings.database.elasticsearch_url],
            max_retries=3,
            retry_on_timeout=True,
            request_timeout=30,
        )
        # Create index template
        try:
            await self._client.indices.put_index_template(
                name="hydra-records",
                body={
                    "index_patterns": ["hydra-tier-*"],
                    **INDEX_MAPPING_TEMPLATE,
                },
            )
        except Exception as exc:
            logger.warning("es_template_create_failed", extra={"error": str(exc)})

    async def disconnect(self) -> None:
        if self._client:
            await self._client.close()
            self._client = None

    async def store(self, records: list[NormalizedRecord], registry_config: dict | None = None) -> StoreResult:
        """Bulk index records into Elasticsearch."""
        if not self._client:
            raise StorageConnectionError("elasticsearch", "Not connected")

        from elasticsearch.helpers import async_bulk

        start = time.monotonic()
        config = registry_config or {}
        es_index_prefix = config.get("es_index_prefix")

        actions: list[dict[str, Any]] = []
        for record in records:
            now = record.timestamp or datetime.now(timezone.utc)
            if es_index_prefix:
                index_name = f"{es_index_prefix}-{now.strftime('%Y.%m')}"
            else:
                index_name = f"hydra-tier-{int(record.tier)}-{now.strftime('%Y.%m')}"

            doc = self._serialize_record(record)
            actions.append({
                "_index": index_name,
                "_id": record.raw_hash,
                "_source": doc,
            })

        stored = 0
        failed = 0
        errors: list[dict] = []

        if actions:
            try:
                success, fail_items = await async_bulk(
                    self._client,
                    actions,
                    raise_on_error=False,
                    stats_only=False,
                )
                stored = success
                if isinstance(fail_items, list):
                    for item in fail_items:
                        failed += 1
                        errors.append({"record_hash": "unknown", "error": str(item)})
            except Exception as exc:
                failed = len(actions)
                for record in records:
                    errors.append({"record_hash": record.raw_hash, "error": str(exc)})
                logger.error("es_bulk_error", extra={"error": str(exc)})

        duration_ms = (time.monotonic() - start) * 1000
        return StoreResult(
            engine="elasticsearch",
            stored=stored,
            failed=failed,
            duration_ms=duration_ms,
            errors=errors,
        )

    async def health_check(self) -> StorageHealth:
        start = time.monotonic()
        try:
            if not self._client:
                return StorageHealth(engine="elasticsearch", status="UNREACHABLE", latency_ms=0.0)
            health = await self._client.cluster.health()
            latency = (time.monotonic() - start) * 1000
            status_map = {"green": "OK", "yellow": "DEGRADED", "red": "UNREACHABLE"}
            status = status_map.get(health.get("status", "red"), "UNREACHABLE")
            return StorageHealth(engine="elasticsearch", status=status, latency_ms=latency, details=health)
        except Exception as exc:
            latency = (time.monotonic() - start) * 1000
            return StorageHealth(
                engine="elasticsearch", status="UNREACHABLE", latency_ms=latency, details={"error": str(exc)}
            )

    @staticmethod
    def _serialize_record(record: NormalizedRecord) -> dict[str, Any]:
        """Convert NormalizedRecord to an ES document dict."""
        doc: dict[str, Any] = {
            "stream_id": record.stream_id,
            "tier": int(record.tier),
            "timestamp": record.timestamp.isoformat(),
            "payload": record.payload,
            "source_name": record.source_meta.source_name,
            "source_url": record.source_meta.source_url,
            "raw_hash": record.raw_hash,
            "confidence": record.confidence,
            "tags": record.tags,
            "ingested_at": record.ingested_at.isoformat(),
        }
        if record.geo is not None:
            doc["geo"] = {"type": record.geo.type, "coordinates": record.geo.coordinates}
        return doc
