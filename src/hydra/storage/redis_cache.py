"""Redis interface for dedup, write-ahead queues, and DLQ."""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from hydra.storage.health import StorageHealth

logger = logging.getLogger(__name__)


class RedisCache:
    """Unified Redis interface for storage layer operations."""

    def __init__(self, redis_url: str, pool_max: int = 20) -> None:
        self._redis_url = redis_url
        self._pool_max = pool_max
        self._redis: Any = None

    # --- Connection lifecycle ---

    async def connect(self) -> None:
        import redis.asyncio as aioredis

        self._redis = aioredis.from_url(
            self._redis_url,
            max_connections=self._pool_max,
            decode_responses=True,
        )

    async def disconnect(self) -> None:
        if self._redis:
            await self._redis.aclose()
            self._redis = None

    # --- Dedup operations ---

    async def is_duplicate(self, tier: int, raw_hash: str) -> bool:
        """Check if raw_hash exists in the tier's dedup set."""
        key = f"hydra:dedup:{tier}"
        return bool(await self._redis.sismember(key, raw_hash))

    async def is_duplicate_batch(self, tier: int, raw_hashes: list[str]) -> list[bool]:
        """Batch membership check using SMISMEMBER (Redis 6.2+)."""
        if not raw_hashes:
            return []
        key = f"hydra:dedup:{tier}"
        results = await self._redis.smismember(key, raw_hashes)
        return [bool(r) for r in results]

    async def mark_seen(self, tier: int, raw_hash: str, ttl: int) -> None:
        """Add hash to dedup set and refresh TTL."""
        key = f"hydra:dedup:{tier}"
        await self._redis.sadd(key, raw_hash)
        await self._redis.expire(key, ttl)

    async def mark_seen_batch(self, tier: int, raw_hashes: list[str], ttl: int) -> None:
        """Batch add hashes using pipeline."""
        if not raw_hashes:
            return
        key = f"hydra:dedup:{tier}"
        async with self._redis.pipeline(transaction=False) as pipe:
            for h in raw_hashes:
                pipe.sadd(key, h)
            pipe.expire(key, ttl)
            await pipe.execute()

    # --- Write-ahead queue operations ---

    async def enqueue(self, queue_key: str, entry: dict) -> None:
        """Push a serialized entry to the write-ahead queue."""
        await self._redis.rpush(queue_key, json.dumps(entry))

    async def dequeue_batch(self, queue_key: str, batch_size: int, timeout: float) -> list[dict]:
        """Pop up to batch_size entries from the queue.

        Uses BLPOP for the first entry (blocking), then LPOP for the rest.
        """
        entries: list[dict] = []
        # Blocking pop for first entry
        result = await self._redis.blpop(queue_key, timeout=timeout)
        if result is None:
            return entries
        _, raw = result
        entries.append(json.loads(raw))

        # Non-blocking drain for remaining
        while len(entries) < batch_size:
            raw = await self._redis.lpop(queue_key)
            if raw is None:
                break
            entries.append(json.loads(raw))

        return entries

    async def queue_depth(self, queue_key: str) -> int:
        """Return the number of entries in a queue."""
        return await self._redis.llen(queue_key)

    # --- DLQ operations ---

    async def enqueue_dlq(self, dlq_key: str, entry: dict) -> None:
        """Push an entry to the dead letter queue."""
        await self._redis.rpush(dlq_key, json.dumps(entry))

    async def peek_dlq(self, dlq_key: str, count: int) -> list[dict]:
        """Inspect DLQ entries without removing them."""
        raw_entries = await self._redis.lrange(dlq_key, 0, count - 1)
        return [json.loads(r) for r in raw_entries]

    async def remove_dlq(self, dlq_key: str, entry: dict) -> None:
        """Remove a specific entry from the DLQ."""
        await self._redis.lrem(dlq_key, 1, json.dumps(entry))

    async def dlq_depth(self, dlq_key: str) -> int:
        """Return the number of entries in a DLQ."""
        return await self._redis.llen(dlq_key)

    # --- Health ---

    async def health_check(self) -> StorageHealth:
        start = time.monotonic()
        try:
            if not self._redis:
                return StorageHealth(engine="redis", status="UNREACHABLE", latency_ms=0.0)
            await self._redis.ping()
            latency = (time.monotonic() - start) * 1000
            return StorageHealth(engine="redis", status="OK", latency_ms=latency)
        except Exception as exc:
            latency = (time.monotonic() - start) * 1000
            return StorageHealth(
                engine="redis", status="UNREACHABLE", latency_ms=latency, details={"error": str(exc)}
            )
