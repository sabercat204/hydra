"""ConcurrencyManager — semaphore-based adapter execution limiter."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hydra.config import HydraSettings

logger = logging.getLogger(__name__)


class ConcurrencyManager:
    """Semaphore-based adapter execution limiter.

    Two-level limiting:
    1. Global semaphore — caps total concurrent adapter.run() calls across all DAGs.
    2. Per-cadence semaphore — caps concurrent runs within a single cadence tier.

    Both levels must be acquired before execution proceeds.
    """

    def __init__(self, settings: "HydraSettings") -> None:
        self._global_limit = settings.scheduler.global_concurrency_limit
        self._cadence_limits = dict(settings.scheduler.cadence_concurrency_limits)
        self._global_sem = asyncio.Semaphore(self._global_limit)
        self._cadence_sems: dict[str, asyncio.Semaphore] = {}
        self._active_count: int = 0
        self._active_by_cadence: dict[str, int] = {}
        self._lock = asyncio.Lock()

    def _get_cadence_sem(self, cadence: str) -> asyncio.Semaphore:
        """Lazily create per-cadence semaphore."""
        if cadence not in self._cadence_sems:
            limit = self._cadence_limits.get(cadence, self._global_limit)
            self._cadence_sems[cadence] = asyncio.Semaphore(limit)
        return self._cadence_sems[cadence]

    async def acquire(self, cadence: str, timeout: float = 60.0) -> bool:
        """Acquire both global and cadence semaphore slots.

        Returns True if acquired within timeout, False otherwise.
        On timeout: log WARNING, return False (caller should fail the task).
        """
        cadence_sem = self._get_cadence_sem(cadence)

        try:
            # Acquire global first, then cadence
            acquired_global = await asyncio.wait_for(self._global_sem.acquire(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("concurrency_timeout_global", extra={"cadence": cadence, "timeout": timeout})
            return False

        try:
            acquired_cadence = await asyncio.wait_for(cadence_sem.acquire(), timeout=timeout)
        except asyncio.TimeoutError:
            # Release global since we couldn't get cadence
            self._global_sem.release()
            logger.warning("concurrency_timeout_cadence", extra={"cadence": cadence, "timeout": timeout})
            return False

        async with self._lock:
            self._active_count += 1
            self._active_by_cadence[cadence] = self._active_by_cadence.get(cadence, 0) + 1

        return True

    async def release(self, cadence: str) -> None:
        """Release both semaphore slots."""
        cadence_sem = self._get_cadence_sem(cadence)
        cadence_sem.release()
        self._global_sem.release()

        async with self._lock:
            self._active_count = max(0, self._active_count - 1)
            current = self._active_by_cadence.get(cadence, 0)
            self._active_by_cadence[cadence] = max(0, current - 1)

    @property
    def active_count(self) -> int:
        """Current number of active adapter executions."""
        return self._active_count

    def active_by_cadence(self, cadence: str) -> int:
        """Current active count for a specific cadence."""
        return self._active_by_cadence.get(cadence, 0)
