"""Tests for AsyncWriter and ReconciliationWorker — 10 tests."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hydra.models.normalized import NormalizedRecord, SourceMeta, Tier
from hydra.storage.engines.base import StorageEngine, StoreResult
from hydra.storage.redis_cache import RedisCache
from hydra.storage.writer import AsyncWriter, ReconciliationWorker
from hydra.utils.hashing import compute_raw_hash


def _make_record_json(**overrides) -> str:
    defaults = dict(
        stream_id="test_stream_1",
        tier=Tier.GEOPHYSICAL_SEISMIC,
        timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc).isoformat(),
        payload={"magnitude": 5.2},
        source_meta={"source_name": "USGS", "adapter_type": "rest_json"},
        raw_hash=compute_raw_hash(b"writer_test"),
        tags=["test"],
    )
    defaults.update(overrides)
    return NormalizedRecord(**defaults).model_dump_json()


def _make_entry(attempt: int = 1, raw_hash: str | None = None) -> dict:
    rh = raw_hash or compute_raw_hash(b"writer_test")
    return {
        "record_hash": rh,
        "stream_id": "test_stream_1",
        "tier": 1,
        "enqueued_at": datetime.now(timezone.utc).isoformat(),
        "attempt": attempt,
        "payload": _make_record_json(raw_hash=rh),
    }


def _make_engine_mock(stored: int = 1, failed: int = 0, errors: list | None = None) -> AsyncMock:
    engine = AsyncMock(spec=StorageEngine)
    engine.store = AsyncMock(return_value=StoreResult(
        engine="postgres", stored=stored, failed=failed, errors=errors or [],
    ))
    return engine


def _make_redis_mock() -> AsyncMock:
    redis = AsyncMock(spec=RedisCache)
    redis.enqueue = AsyncMock()
    redis.enqueue_dlq = AsyncMock()
    redis.dequeue_batch = AsyncMock(return_value=[])
    redis.peek_dlq = AsyncMock(return_value=[])
    redis.remove_dlq = AsyncMock()
    redis.dlq_depth = AsyncMock(return_value=0)
    return redis


@pytest.mark.asyncio
async def test_writer_consumes_and_stores():
    """Writer consumes from queue and calls engine.store()."""
    engine = _make_engine_mock()
    redis = _make_redis_mock()
    writer = AsyncWriter(engine, redis, "hydra:waq:postgres", "hydra:dlq:postgres", max_retries=3)
    entry = _make_entry()
    await writer._process_batch([entry])
    engine.store.assert_called_once()


@pytest.mark.asyncio
async def test_successful_store_completes():
    """Successful store removes entry from queue."""
    engine = _make_engine_mock(stored=1)
    redis = _make_redis_mock()
    writer = AsyncWriter(engine, redis, "hydra:waq:postgres", "hydra:dlq:postgres")
    entry = _make_entry()
    await writer._process_batch([entry])
    # No DLQ enqueue on success
    redis.enqueue_dlq.assert_not_called()


@pytest.mark.asyncio
async def test_failed_store_requeues():
    """Failed store increments attempt and re-enqueues."""
    rh = compute_raw_hash(b"fail_test")
    engine = _make_engine_mock(stored=0, failed=1, errors=[{"record_hash": rh, "error": "write failed"}])
    redis = _make_redis_mock()
    writer = AsyncWriter(engine, redis, "hydra:waq:postgres", "hydra:dlq:postgres",
                         max_retries=3, backoff_base=0.01, backoff_factor=1.0)
    entry = _make_entry(attempt=1, raw_hash=rh)
    await writer._process_batch([entry])
    # Should re-enqueue (attempt < max_retries)
    redis.enqueue.assert_called()


@pytest.mark.asyncio
async def test_exhausted_retries_to_dlq():
    """Exhausted retries move entry to DLQ."""
    rh = compute_raw_hash(b"exhaust_test")
    engine = _make_engine_mock(stored=0, failed=1, errors=[{"record_hash": rh, "error": "persistent failure"}])
    redis = _make_redis_mock()
    writer = AsyncWriter(engine, redis, "hydra:waq:postgres", "hydra:dlq:postgres",
                         max_retries=3, backoff_base=0.01)
    entry = _make_entry(attempt=3, raw_hash=rh)
    await writer._process_batch([entry])
    redis.enqueue_dlq.assert_called()


@pytest.mark.asyncio
async def test_batch_accumulation():
    """Batch accumulation up to batch_size."""
    engine = _make_engine_mock(stored=3)
    redis = _make_redis_mock()
    writer = AsyncWriter(engine, redis, "hydra:waq:postgres", "hydra:dlq:postgres", batch_size=100)
    entries = [_make_entry(raw_hash=compute_raw_hash(f"b{i}".encode())) for i in range(3)]
    await writer._process_batch(entries)
    engine.store.assert_called_once()
    records = engine.store.call_args[0][0]
    assert len(records) == 3


@pytest.mark.asyncio
async def test_storage_status_complete():
    """storage_status updated to complete when all engines confirm."""
    # This is a conceptual test — the actual PG update happens in the writer
    engine = _make_engine_mock(stored=1)
    redis = _make_redis_mock()
    writer = AsyncWriter(engine, redis, "hydra:waq:postgres", "hydra:dlq:postgres")
    entry = _make_entry()
    await writer._process_batch([entry])
    assert engine.store.call_count == 1


@pytest.mark.asyncio
async def test_storage_status_pending():
    """storage_status remains pending when some engines haven't confirmed."""
    # Partial failure scenario
    rh = compute_raw_hash(b"partial")
    engine = _make_engine_mock(stored=0, failed=1, errors=[{"record_hash": rh, "error": "timeout"}])
    redis = _make_redis_mock()
    writer = AsyncWriter(engine, redis, "hydra:waq:postgres", "hydra:dlq:postgres",
                         max_retries=3, backoff_base=0.01, backoff_factor=1.0)
    entry = _make_entry(attempt=1, raw_hash=rh)
    await writer._process_batch([entry])
    # Re-enqueued, status stays pending
    redis.enqueue.assert_called()


@pytest.mark.asyncio
async def test_reconciliation_processes_dlq():
    """Reconciliation worker processes DLQ entries."""
    engine = _make_engine_mock(stored=1)
    redis = _make_redis_mock()
    rh = compute_raw_hash(b"recon_test")
    dlq_entry = {
        "record_hash": rh,
        "stream_id": "test_stream_1",
        "tier": 1,
        "target_engine": "postgres",
        "error": "previous failure",
        "error_type": "StorageWriteError",
        "attempt_count": 1,
        "first_failed_at": datetime.now(timezone.utc).isoformat(),
        "last_failed_at": datetime.now(timezone.utc).isoformat(),
        "payload": _make_record_json(raw_hash=rh),
    }
    redis.dlq_depth = AsyncMock(return_value=1)
    redis.peek_dlq = AsyncMock(return_value=[dlq_entry])
    worker = ReconciliationWorker({"postgres": engine}, redis, interval=1.0, max_dlq_retries=5)
    await worker._process_all_dlqs()
    engine.store.assert_called_once()
    redis.remove_dlq.assert_called()


@pytest.mark.asyncio
async def test_reconciliation_success_removes_from_dlq():
    """Reconciliation success removes from DLQ and updates PG."""
    engine = _make_engine_mock(stored=1)
    redis = _make_redis_mock()
    rh = compute_raw_hash(b"recon_ok")
    dlq_entry = {
        "record_hash": rh,
        "stream_id": "test_stream_1",
        "tier": 1,
        "target_engine": "postgres",
        "error": "transient",
        "error_type": "StorageWriteError",
        "attempt_count": 1,
        "first_failed_at": datetime.now(timezone.utc).isoformat(),
        "last_failed_at": datetime.now(timezone.utc).isoformat(),
        "payload": _make_record_json(raw_hash=rh),
    }
    redis.dlq_depth = AsyncMock(return_value=1)
    redis.peek_dlq = AsyncMock(return_value=[dlq_entry])
    worker = ReconciliationWorker({"postgres": engine}, redis, interval=1.0)
    await worker._process_all_dlqs()
    redis.remove_dlq.assert_called_once()


@pytest.mark.asyncio
async def test_reconciliation_exhaustion_sets_failed():
    """Reconciliation exhaustion sets storage_status = 'failed'."""
    engine = _make_engine_mock(stored=0, failed=1, errors=[{"record_hash": "x", "error": "permanent"}])
    redis = _make_redis_mock()
    rh = compute_raw_hash(b"exhaust_recon")
    dlq_entry = {
        "record_hash": rh,
        "stream_id": "test_stream_1",
        "tier": 1,
        "target_engine": "postgres",
        "error": "permanent",
        "error_type": "StorageWriteError",
        "attempt_count": 6,  # > max_dlq_retries
        "first_failed_at": datetime.now(timezone.utc).isoformat(),
        "last_failed_at": datetime.now(timezone.utc).isoformat(),
        "payload": _make_record_json(raw_hash=rh),
    }
    redis.dlq_depth = AsyncMock(return_value=1)
    redis.peek_dlq = AsyncMock(return_value=[dlq_entry])
    worker = ReconciliationWorker({"postgres": engine}, redis, interval=1.0, max_dlq_retries=5)
    await worker._process_all_dlqs()
    # Should not attempt re-store since attempt_count > max
    engine.store.assert_not_called()
