"""APICollector — scrapes async-job state and active API-key counts (P12 §6.3).

Concrete :class:`BaseCollector` that inspects Redis for HYDRA async-job
records and PostgreSQL for active API keys, publishing the rollups into
the Prometheus custom metrics registry.

Satisfies Requirements 7.1 and 7.2.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Final

from hydra.monitoring.collectors import BaseCollector
from hydra.monitoring.metrics import (
    hydra_api_active_keys,
    hydra_api_job_status,
)

if TYPE_CHECKING:
    import asyncpg
    from redis.asyncio import Redis

logger = logging.getLogger(__name__)


# Keys created by hydra.api.jobs follow the convention ``hydra:job:<job_id>``.
_JOB_KEY_PATTERN: Final[str] = "hydra:job:*"

# Known job statuses — seeded at zero each cycle so Prometheus scrapers
# see the full set even when Redis contains no jobs of a given status.
_JOB_STATUSES: Final[tuple[str, ...]] = (
    "pending",
    "running",
    "completed",
    "failed",
)

# SQL query for active API keys: non-revoked and either non-expiring or
# unexpired at query time (matches the P11 auth manager semantics).
_ACTIVE_KEYS_SQL: Final[str] = (
    "SELECT COUNT(*)::bigint "
    "FROM api_keys "
    "WHERE is_active = TRUE "
    "  AND (expires_at IS NULL OR expires_at > NOW())"
)


class APICollector(BaseCollector):
    """Collect API subsystem metrics.

    Each ``collect()`` cycle:

    1. Iterates ``hydra:job:*`` keys via ``SCAN``, parses each job record,
       counts by status, and publishes :data:`hydra_api_job_status`.
    2. Queries the ``api_keys`` table for active, non-expired rows and
       publishes :data:`hydra_api_active_keys`.
    """

    def __init__(
        self,
        redis: "Redis",
        pg_pool: "asyncpg.Pool",
        interval: float = 60.0,
    ) -> None:
        super().__init__(interval=interval)
        self._redis = redis
        self._pg_pool = pg_pool

    async def collect(self) -> None:
        await self._update_job_counts()
        await self._update_active_keys()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _update_job_counts(self) -> None:
        """Scan ``hydra:job:*`` and publish per-status counts."""
        counts: dict[str, int] = {status: 0 for status in _JOB_STATUSES}

        async for raw_key in self._redis.scan_iter(match=_JOB_KEY_PATTERN):
            key = raw_key.decode() if isinstance(raw_key, (bytes, bytearray)) else raw_key
            status = await self._read_job_status(key)
            if status is None:
                continue
            counts[status] = counts.get(status, 0) + 1

        for status, count in counts.items():
            hydra_api_job_status.labels(status=status).set(count)

    async def _read_job_status(self, key: str) -> str | None:
        """Fetch a single job record and extract its ``status`` field.

        Jobs are stored as JSON strings by :mod:`hydra.api.jobs`. Records
        that cannot be parsed are skipped so a malformed entry does not
        abort the entire collection cycle.
        """
        raw = await self._redis.get(key)
        if raw is None:
            return None
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode()
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
        status = payload.get("status") if isinstance(payload, dict) else None
        if not isinstance(status, str):
            return None
        return status

    async def _update_active_keys(self) -> None:
        """Query ``api_keys`` for the number of active, non-expired keys."""
        async with self._pg_pool.acquire() as conn:
            count = await conn.fetchval(_ACTIVE_KEYS_SQL)
        hydra_api_active_keys.set(int(count or 0))


__all__ = ["APICollector"]
