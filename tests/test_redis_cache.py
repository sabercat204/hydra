"""Tests for RedisCache — 12 tests covering dedup, queues, DLQ, health."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hydra.storage.redis_cache import RedisCache


def _make_cache() -> RedisCache:
    cache = RedisCache("redis://localhost:6379/0")
    cache._redis = AsyncMock()
    return cache


@pytest.mark.asyncio
async def test_is_duplicate_false_for_new():
    """is_duplicate returns False for new hash."""
    cache = _make_cache()
    cache._redis.sismember = AsyncMock(return_value=False)
    result = await cache.is_duplicate(1, "abc123def456789a")
    assert result is False


@pytest.mark.asyncio
async def test_is_duplicate_true_for_seen():
    """is_duplicate returns True for seen hash."""
    cache = _make_cache()
    cache._redis.sismember = AsyncMock(return_value=True)
    result = await cache.is_duplicate(1, "abc123def456789a")
    assert result is True


@pytest.mark.asyncio
async def test_mark_seen_adds_with_ttl():
    """mark_seen adds hash with correct TTL."""
    cache = _make_cache()
    cache._redis.sadd = AsyncMock()
    cache._redis.expire = AsyncMock()
    await cache.mark_seen(1, "abc123def456789a", 86400)
    cache._redis.sadd.assert_called_once_with("hydra:dedup:1", "abc123def456789a")
    cache._redis.expire.assert_called_once_with("hydra:dedup:1", 86400)


@pytest.mark.asyncio
async def test_is_duplicate_batch_uses_smismember():
    """is_duplicate_batch uses SMISMEMBER for batch check."""
    cache = _make_cache()
    cache._redis.smismember = AsyncMock(return_value=[0, 1, 0])
    result = await cache.is_duplicate_batch(1, ["h1", "h2", "h3"])
    assert result == [False, True, False]
    cache._redis.smismember.assert_called_once()


@pytest.mark.asyncio
async def test_mark_seen_batch_uses_pipeline():
    """mark_seen_batch uses pipeline for batch add."""
    cache = _make_cache()
    pipe = AsyncMock()
    pipe.sadd = MagicMock()
    pipe.expire = MagicMock()
    pipe.execute = AsyncMock()
    cache._redis.pipeline = MagicMock(return_value=pipe)
    pipe.__aenter__ = AsyncMock(return_value=pipe)
    pipe.__aexit__ = AsyncMock(return_value=False)
    await cache.mark_seen_batch(1, ["h1", "h2"], 86400)
    assert pipe.sadd.call_count == 2
    pipe.expire.assert_called_once()


@pytest.mark.asyncio
async def test_ttl_expiry():
    """TTL expiry — hash no longer detected after TTL."""
    cache = _make_cache()
    # First call: exists. Second call: expired.
    cache._redis.sismember = AsyncMock(side_effect=[True, False])
    assert await cache.is_duplicate(1, "h1") is True
    assert await cache.is_duplicate(1, "h1") is False


@pytest.mark.asyncio
async def test_enqueue_adds_to_queue():
    """enqueue adds entry to correct queue."""
    cache = _make_cache()
    cache._redis.rpush = AsyncMock()
    entry = {"record_hash": "h1", "payload": "{}"}
    await cache.enqueue("hydra:waq:postgres", entry)
    cache._redis.rpush.assert_called_once()
    call_args = cache._redis.rpush.call_args
    assert call_args[0][0] == "hydra:waq:postgres"


@pytest.mark.asyncio
async def test_dequeue_batch_retrieves():
    """dequeue_batch retrieves and removes entries."""
    cache = _make_cache()
    entry = json.dumps({"record_hash": "h1"})
    cache._redis.blpop = AsyncMock(return_value=("hydra:waq:postgres", entry))
    cache._redis.lpop = AsyncMock(return_value=None)
    result = await cache.dequeue_batch("hydra:waq:postgres", 10, 0.5)
    assert len(result) == 1
    assert result[0]["record_hash"] == "h1"


@pytest.mark.asyncio
async def test_queue_depth():
    """queue_depth returns correct count."""
    cache = _make_cache()
    cache._redis.llen = AsyncMock(return_value=42)
    depth = await cache.queue_depth("hydra:waq:postgres")
    assert depth == 42


@pytest.mark.asyncio
async def test_dlq_operations():
    """DLQ operations — enqueue, peek, remove."""
    cache = _make_cache()
    cache._redis.rpush = AsyncMock()
    cache._redis.lrange = AsyncMock(return_value=[json.dumps({"record_hash": "h1"})])
    cache._redis.lrem = AsyncMock()

    entry = {"record_hash": "h1"}
    await cache.enqueue_dlq("hydra:dlq:postgres", entry)
    cache._redis.rpush.assert_called()

    peeked = await cache.peek_dlq("hydra:dlq:postgres", 10)
    assert len(peeked) == 1

    await cache.remove_dlq("hydra:dlq:postgres", entry)
    cache._redis.lrem.assert_called()


@pytest.mark.asyncio
async def test_dlq_depth():
    """dlq_depth returns correct count."""
    cache = _make_cache()
    cache._redis.llen = AsyncMock(return_value=5)
    depth = await cache.dlq_depth("hydra:dlq:postgres")
    assert depth == 5


@pytest.mark.asyncio
async def test_health_check_ok():
    """Health check returns OK on successful ping."""
    cache = _make_cache()
    cache._redis.ping = AsyncMock(return_value=True)
    health = await cache.health_check()
    assert health.status == "OK"
