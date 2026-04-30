"""AsyncWriter — per-engine queue consumer with retry and DLQ semantics."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

from hydra.models.normalized import NormalizedRecord
from hydra.storage.engines.base import StorageEngine, StoreResult
from hydra.storage.redis_cache import RedisCache

logger = logging.getLogger(__name__)


class AsyncWriter:
    """Per-engine queue consumer with retry and DLQ semantics."""

    def __init__(
        self,
        engine: StorageEngine,
        redis_cache: RedisCache,
        queue_key: str,
        dlq_key: str,
        batch_size: int = 100,
        poll_interval: float = 0.5,
        max_retries: int = 3,
        backoff_base: float = 1.0,
        backoff_factor: float = 2.0,
        backoff_ceiling: float = 30.0,
    ) -> None:
        self._engine = engine
        self._redis = redis_cache
        self._queue_key = queue_key
        self._dlq_key = dlq_key
        self._batch_size = batch_size
        self._poll_interval = poll_interval
        self._max_retries = max_retries
        self._backoff_base = backoff_base
        self._backoff_factor = backoff_factor
        self._backoff_ceiling = backoff_ceiling
        self._running = False

    async def run(self) -> None:
        """Main consumer loop. Runs indefinitely until cancelled."""
        self._running = True
        while self._running:
            try:
                entries = await self._redis.dequeue_batch(
                    self._queue_key, self._batch_size, self._poll_interval
                )
                if entries:
                    await self._process_batch(entries)
            except asyncio.CancelledError:
                self._running = False
                break
            except Exception as exc:
                logger.error("writer_loop_error", extra={"queue": self._queue_key, "error": str(exc)})
                await asyncio.sleep(self._poll_interval)

    async def stop(self) -> None:
        self._running = False

    async def _process_batch(self, entries: list[dict]) -> None:
        """Deserialize, call engine.store(), handle results."""
        records: list[NormalizedRecord] = []
        entry_map: dict[str, dict] = {}

        for entry in entries:
            try:
                payload_str = entry.get("payload", "")
                record = NormalizedRecord.model_validate_json(payload_str)
                records.append(record)
                entry_map[record.raw_hash] = entry
            except Exception as exc:
                # Try fixing common serialization issues (e.g., Z suffix on timestamps)
                try:
                    import json as _json
                    data = _json.loads(payload_str)
                    for ts_field in ("timestamp", "ingested_at"):
                        if isinstance(data.get(ts_field), str) and data[ts_field].endswith("Z"):
                            data[ts_field] = data[ts_field][:-1] + "+00:00"
                    sm = data.get("source_meta", {})
                    if isinstance(sm.get("fetch_timestamp"), str) and sm["fetch_timestamp"].endswith("Z"):
                        sm["fetch_timestamp"] = sm["fetch_timestamp"][:-1] + "+00:00"
                    record = NormalizedRecord.model_validate(data)
                    records.append(record)
                    entry_map[record.raw_hash] = entry
                except Exception:
                    logger.error("writer_deserialize_error", extra={"error": str(exc)})
                    await self._move_to_dlq(entry, str(exc))

        if not records:
            return

        try:
            result: StoreResult = await self._engine.store(records)
            # Handle per-record errors
            failed_hashes = {e["record_hash"] for e in result.errors}
            for record in records:
                if record.raw_hash in failed_hashes:
                    entry = entry_map.get(record.raw_hash)
                    if entry:
                        error_msg = next(
                            (e["error"] for e in result.errors if e["record_hash"] == record.raw_hash), "Unknown"
                        )
                        await self._retry_or_dlq(entry, Exception(error_msg))
        except Exception as exc:
            # Entire batch failed
            for entry in entries:
                await self._retry_or_dlq(entry, exc)

    async def _retry_or_dlq(self, entry: dict, error: Exception) -> None:
        """Increment attempt, re-enqueue or move to DLQ."""
        attempt = entry.get("attempt", 1)
        if attempt < self._max_retries:
            entry["attempt"] = attempt + 1
            delay = min(
                self._backoff_base * (self._backoff_factor ** attempt),
                self._backoff_ceiling,
            )
            await asyncio.sleep(delay)
            await self._redis.enqueue(self._queue_key, entry)
        else:
            await self._move_to_dlq(entry, str(error))

    async def _move_to_dlq(self, entry: dict, error_msg: str) -> None:
        """Move a failed entry to the dead letter queue."""
        dlq_entry = {
            "record_hash": entry.get("record_hash", ""),
            "stream_id": entry.get("stream_id", ""),
            "tier": entry.get("tier", 0),
            "target_engine": self._queue_key.split(":")[-1],
            "error": error_msg,
            "error_type": "StorageWriteError",
            "attempt_count": entry.get("attempt", 1),
            "first_failed_at": entry.get("enqueued_at", datetime.now(timezone.utc).isoformat()),
            "last_failed_at": datetime.now(timezone.utc).isoformat(),
            "payload": entry.get("payload", ""),
        }
        await self._redis.enqueue_dlq(self._dlq_key, dlq_entry)


class ReconciliationWorker:
    """Processes dead letter queue entries with extended retry."""

    def __init__(
        self,
        engines: dict[str, StorageEngine],
        redis_cache: RedisCache,
        interval: float = 300.0,
        max_dlq_retries: int = 5,
        alert_threshold: int = 100,
    ) -> None:
        self._engines = engines
        self._redis = redis_cache
        self._interval = interval
        self._max_dlq_retries = max_dlq_retries
        self._alert_threshold = alert_threshold
        self._running = False

    async def run(self) -> None:
        """Periodic DLQ drain loop."""
        self._running = True
        while self._running:
            try:
                await self._process_all_dlqs()
            except asyncio.CancelledError:
                self._running = False
                break
            except Exception as exc:
                logger.error("reconciliation_error", extra={"error": str(exc)})
            await asyncio.sleep(self._interval)

    async def stop(self) -> None:
        self._running = False

    async def _process_all_dlqs(self) -> None:
        """Process DLQ entries for all engines."""
        for engine_name, engine in self._engines.items():
            dlq_key = f"hydra:dlq:{engine_name}"
            depth = await self._redis.dlq_depth(dlq_key)

            if depth == 0:
                continue

            if depth >= self._alert_threshold:
                logger.critical(
                    "dlq_threshold_exceeded",
                    extra={"engine": engine_name, "depth": depth, "threshold": self._alert_threshold},
                )

            entries = await self._redis.peek_dlq(dlq_key, count=min(depth, 100))
            for entry in entries:
                attempt_count = entry.get("attempt_count", 0)
                if attempt_count > self._max_dlq_retries:
                    logger.error(
                        "dlq_exhausted",
                        extra={"engine": engine_name, "record_hash": entry.get("record_hash")},
                    )
                    continue

                try:
                    payload_str = entry.get("payload", "")
                    try:
                        record = NormalizedRecord.model_validate_json(payload_str)
                    except Exception:
                        import json as _json
                        data = _json.loads(payload_str)
                        for ts_field in ("timestamp", "ingested_at"):
                            if isinstance(data.get(ts_field), str) and data[ts_field].endswith("Z"):
                                data[ts_field] = data[ts_field][:-1] + "+00:00"
                        sm = data.get("source_meta", {})
                        if isinstance(sm.get("fetch_timestamp"), str) and sm["fetch_timestamp"].endswith("Z"):
                            sm["fetch_timestamp"] = sm["fetch_timestamp"][:-1] + "+00:00"
                        record = NormalizedRecord.model_validate(data)
                    result = await engine.store([record])
                    if result.failed == 0:
                        await self._redis.remove_dlq(dlq_key, entry)
                    else:
                        entry["attempt_count"] = attempt_count + 1
                        entry["last_failed_at"] = datetime.now(timezone.utc).isoformat()
                except Exception as exc:
                    entry["attempt_count"] = attempt_count + 1
                    entry["last_failed_at"] = datetime.now(timezone.utc).isoformat()
                    logger.error(
                        "reconciliation_retry_failed",
                        extra={"engine": engine_name, "error": str(exc)},
                    )
