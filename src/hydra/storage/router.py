"""StorageRouter — trait classification, dedup, dispatch to engine queues."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from hydra.config import HydraSettings
from hydra.models.normalized import NormalizedRecord
from hydra.registry.stream_registry import StreamRegistry
from hydra.storage.exceptions import DedupCacheError
from hydra.storage.redis_cache import RedisCache

logger = logging.getLogger(__name__)


@dataclass
class RouteResult:
    """Result of routing a batch of records."""

    total: int = 0
    routed: int = 0
    deduplicated: int = 0
    failed: int = 0
    engine_counts: dict[str, int] = field(default_factory=dict)
    duration_ms: float = 0.0


# Queue key constants
ENGINE_QUEUE_KEYS: dict[str, str] = {
    "postgres": "hydra:waq:postgres",
    "influxdb": "hydra:waq:influxdb",
    "elasticsearch": "hydra:waq:elasticsearch",
    "neo4j": "hydra:waq:neo4j",
    "minio": "hydra:waq:minio",
}


class StorageRouter:
    """Routes NormalizedRecords to storage engines based on traits and registry overrides."""

    def __init__(
        self,
        redis_cache: RedisCache,
        registry: StreamRegistry,
        settings: HydraSettings,
    ) -> None:
        self._redis = redis_cache
        self._registry = registry
        self._settings = settings
        self._cadence_ttl_map: dict[str, int] = {
            "sub_minute": 86_400,
            "realtime": 86_400,
            "15min": 172_800,
            "hourly": 259_200,
            "daily": 604_800,
            "weekly": 2_592_000,
            "monthly": 7_776_000,
            "quarterly": 15_552_000,
            "annual": 31_536_000,
        }
        self._timeseries_cadences = {"sub_minute", "realtime"}
        self._text_tiers = {14, 15, 16, 17, 19, 20, 21}
        self._payload_size_threshold = 4096

    async def route(self, records: list[NormalizedRecord]) -> RouteResult:
        """Main entry point. Dedup, classify, enqueue."""
        start = time.monotonic()
        result = RouteResult(total=len(records))

        if not records:
            result.duration_ms = (time.monotonic() - start) * 1000
            return result

        # Batch dedup check
        tier = int(records[0].tier)
        hashes = [r.raw_hash for r in records]
        try:
            dup_flags = await self._redis.is_duplicate_batch(tier, hashes)
        except Exception as exc:
            logger.warning("dedup_cache_error", extra={"error": str(exc)})
            dup_flags = [False] * len(records)

        new_hashes: list[str] = []
        for record, is_dup in zip(records, dup_flags):
            if is_dup:
                result.deduplicated += 1
                logger.debug("dedup_drop", extra={"stream_id": record.stream_id, "raw_hash": record.raw_hash})
                continue

            try:
                engines = self._classify(record)
                for engine_name in engines:
                    queue_key = ENGINE_QUEUE_KEYS.get(engine_name)
                    if not queue_key:
                        continue
                    entry = {
                        "record_hash": record.raw_hash,
                        "stream_id": record.stream_id,
                        "tier": int(record.tier),
                        "enqueued_at": datetime.now(timezone.utc).isoformat(),
                        "attempt": 1,
                        "payload": record.model_dump_json(),
                    }
                    await self._redis.enqueue(queue_key, entry)
                    result.engine_counts[engine_name] = result.engine_counts.get(engine_name, 0) + 1

                new_hashes.append(record.raw_hash)
                result.routed += 1
            except Exception as exc:
                result.failed += 1
                logger.error("route_error", extra={"raw_hash": record.raw_hash, "error": str(exc)})

        # Mark new hashes as seen
        if new_hashes:
            ttl = self._get_dedup_ttl(tier)
            try:
                await self._redis.mark_seen_batch(tier, new_hashes, ttl)
            except Exception as exc:
                logger.warning("dedup_mark_error", extra={"error": str(exc)})

        result.duration_ms = (time.monotonic() - start) * 1000
        return result

    def _classify(self, record: NormalizedRecord) -> set[str]:
        """Determine target engines for a single record."""
        tier_id = int(record.tier)
        tier_info = self._registry.get_tier(tier_id)

        # Check for registry override
        if tier_info:
            # Look for storage config in the raw registry data
            storage_config = self._get_storage_config(tier_id)
            if storage_config and "storage_engines" in storage_config:
                engines = set(storage_config["storage_engines"])
                engines.add("postgres")  # Always include postgres
                return engines

        # Trait-based inference
        engines: set[str] = {"postgres"}

        if self._is_timeseries_tier(tier_id):
            engines.add("influxdb")

        if self._is_text_heavy(record):
            engines.add("elasticsearch")

        if self._has_graph_schema(tier_id):
            engines.add("neo4j")

        if self._has_binary_artifact(record):
            engines.add("minio")

        return engines

    def _get_storage_config(self, tier_id: int) -> dict | None:
        """Get storage configuration from registry for a tier."""
        tier = self._registry.get_tier(tier_id)
        if not tier:
            return None
        return tier.storage

    def _get_dedup_ttl(self, tier: int) -> int:
        """Look up cadence for tier, return TTL in seconds."""
        tier_info = self._registry.get_tier(tier)
        if tier_info:
            return self._cadence_ttl_map.get(tier_info.cadence, 604_800)
        return 604_800  # Default: 7 days

    def _has_binary_artifact(self, record: NormalizedRecord) -> bool:
        """Check for _binary_artifact key in payload."""
        artifact = record.payload.get("_binary_artifact")
        return isinstance(artifact, dict) and "content" in artifact

    def _is_text_heavy(self, record: NormalizedRecord) -> bool:
        """Check tier membership or payload size threshold."""
        if int(record.tier) in self._text_tiers:
            return True
        try:
            payload_size = len(json.dumps(record.payload))
            return payload_size > self._payload_size_threshold
        except (TypeError, ValueError):
            return False

    def _has_graph_schema(self, tier: int) -> bool:
        """Check registry for graph_schema declaration."""
        config = self._get_storage_config(tier)
        return config is not None and "graph_schema" in config

    def _is_timeseries_tier(self, tier: int) -> bool:
        """Check tier cadence against timeseries cadences."""
        tier_info = self._registry.get_tier(tier)
        if tier_info:
            return tier_info.cadence in self._timeseries_cadences
        return False

    def _has_geo(self, record: NormalizedRecord) -> bool:
        """Check if record.geo is populated and non-null."""
        return record.geo is not None
