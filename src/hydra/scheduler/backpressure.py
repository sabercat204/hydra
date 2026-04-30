"""BackpressureMonitor — WAQ depth checks with throttle/reject decisions."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from hydra.config import HydraSettings
    from hydra.storage.redis_cache import RedisCache

logger = logging.getLogger(__name__)

# Redis WAQ key patterns (mirrors StorageRouter)
ENGINE_QUEUE_KEYS: dict[str, str] = {
    "postgres": "hydra:waq:postgres",
    "influxdb": "hydra:waq:influxdb",
    "elasticsearch": "hydra:waq:elasticsearch",
    "neo4j": "hydra:waq:neo4j",
    "minio": "hydra:waq:minio",
}


@dataclass
class EngineBackpressure:
    """Backpressure state for a single storage engine."""

    engine: str
    queue_depth: int
    soft_limit: int
    hard_limit: int
    state: Literal["CLEAR", "THROTTLED", "BLOCKED"]


@dataclass
class BackpressureState:
    """Aggregate backpressure state across all engines."""

    overall: Literal["CLEAR", "THROTTLED", "BLOCKED"]
    engines: dict[str, EngineBackpressure]
    checked_at: str  # ISO 8601 UTC


class BackpressureMonitor:
    """Monitors Redis WAQ depths and makes throttle/reject decisions.

    Three states per engine:
    - CLEAR: queue depth < soft_limit → proceed normally
    - THROTTLED: soft_limit <= queue depth < hard_limit → delay execution
    - BLOCKED: queue depth >= hard_limit → reject execution
    """

    def __init__(self, redis_cache: "RedisCache", settings: "HydraSettings") -> None:
        self._redis = redis_cache
        self._settings = settings
        self._engine_queues = dict(ENGINE_QUEUE_KEYS)

    def _get_limits(self, engine: str) -> tuple[int, int]:
        """Return (soft_limit, hard_limit) for an engine, respecting overrides."""
        overrides = self._settings.scheduler.engine_backpressure_overrides.get(engine, {})
        soft = overrides.get("soft_limit", self._settings.scheduler.backpressure_soft_limit)
        hard = overrides.get("hard_limit", self._settings.scheduler.backpressure_hard_limit)
        return soft, hard

    async def check_engine(self, engine: str) -> EngineBackpressure:
        """Check a single engine's queue depth against thresholds."""
        queue_key = self._engine_queues.get(engine)
        if queue_key is None:
            return EngineBackpressure(
                engine=engine, queue_depth=0, soft_limit=0, hard_limit=0, state="CLEAR"
            )

        depth = await self._redis.queue_depth(queue_key)
        soft, hard = self._get_limits(engine)

        if depth >= hard:
            state: Literal["CLEAR", "THROTTLED", "BLOCKED"] = "BLOCKED"
        elif depth >= soft:
            state = "THROTTLED"
        else:
            state = "CLEAR"

        return EngineBackpressure(
            engine=engine,
            queue_depth=depth,
            soft_limit=soft,
            hard_limit=hard,
            state=state,
        )

    async def check(self) -> BackpressureState:
        """Check all engine queue depths. Returns aggregate state."""
        engines: dict[str, EngineBackpressure] = {}
        for engine in self._engine_queues:
            engines[engine] = await self.check_engine(engine)

        # Determine overall state: worst-case across all engines
        overall: Literal["CLEAR", "THROTTLED", "BLOCKED"] = "CLEAR"
        for ebp in engines.values():
            if ebp.state == "BLOCKED":
                overall = "BLOCKED"
                break
            if ebp.state == "THROTTLED":
                overall = "THROTTLED"

        return BackpressureState(
            overall=overall,
            engines=engines,
            checked_at=datetime.now(timezone.utc).isoformat(),
        )

    async def wait_for_clear(
        self,
        max_wait: float | None = None,
        poll_interval: float | None = None,
    ) -> bool:
        """Block until backpressure clears or timeout.

        Used when state is THROTTLED — wait for queues to drain.
        Returns True if cleared, False if timed out.
        """
        if max_wait is None:
            max_wait = self._settings.scheduler.backpressure_wait_timeout
        if poll_interval is None:
            poll_interval = self._settings.scheduler.backpressure_poll_interval

        start = time.monotonic()
        while (time.monotonic() - start) < max_wait:
            state = await self.check()
            if state.overall == "CLEAR":
                return True
            if state.overall == "BLOCKED":
                # Escalated from THROTTLED to BLOCKED — give up
                return False
            await asyncio.sleep(poll_interval)
        return False
