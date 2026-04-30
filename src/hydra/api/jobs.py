"""Redis-backed async job manager for long-running operations."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Coroutine

from hydra.api.schemas.common import JobStatus

logger = logging.getLogger(__name__)


class JobManager:
    """Redis-backed async job tracking.

    Redis key pattern: {prefix}:{job_id}
    TTL: configurable (default 3600s / 1 hour)
    Value: JSON-serialized JobStatus
    """

    def __init__(self, redis: Any, prefix: str = "hydra:job", ttl: int = 3600) -> None:
        self._redis = redis
        self._prefix = prefix
        self._ttl = ttl

    def _key(self, job_id: str) -> str:
        return f"{self._prefix}:{job_id}"

    async def create_job(self) -> str:
        """Create a new job with status 'pending'. Returns job_id."""
        job_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        status = JobStatus(
            job_id=job_id,
            status="pending",
            created_at=now,
            updated_at=now,
        )
        await self._redis.setex(
            self._key(job_id),
            self._ttl,
            status.model_dump_json(),
        )
        return job_id

    async def update_job(
        self,
        job_id: str,
        status: str,
        progress: float | None = None,
        result_id: str | None = None,
        error: str | None = None,
    ) -> None:
        """Update job state. Refreshes TTL on every update."""
        existing = await self.get_job(job_id)
        if existing is None:
            return
        now = datetime.now(timezone.utc).isoformat()
        updated = JobStatus(
            job_id=job_id,
            status=status,  # type: ignore[arg-type]
            progress=progress if progress is not None else existing.progress,
            result_id=result_id if result_id is not None else existing.result_id,
            error=error if error is not None else existing.error,
            created_at=existing.created_at,
            updated_at=now,
        )
        await self._redis.setex(
            self._key(job_id),
            self._ttl,
            updated.model_dump_json(),
        )

    async def get_job(self, job_id: str) -> JobStatus | None:
        """Retrieve job status. Returns None if expired or not found."""
        raw = await self._redis.get(self._key(job_id))
        if raw is None:
            return None
        return JobStatus.model_validate_json(raw)

    async def run_in_background(
        self,
        job_id: str,
        coro: Coroutine[Any, Any, str],
    ) -> None:
        """Execute coroutine as background task with job tracking.

        The coroutine should return a result_id string on success.
        """

        async def _wrapper() -> None:
            try:
                await self.update_job(job_id, "running")
                result_id = await coro
                await self.update_job(job_id, "completed", result_id=result_id)
            except Exception as exc:
                logger.exception("Job %s failed: %s", job_id, exc)
                await self.update_job(job_id, "failed", error=str(exc))

        asyncio.create_task(_wrapper())
