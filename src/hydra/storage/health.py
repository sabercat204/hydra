"""Storage health dataclass and per-engine health probes."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from hydra.storage.engines.base import StorageEngine
    from hydra.storage.redis_cache import RedisCache


@dataclass
class StorageHealth:
    """Health status for a single storage engine."""

    engine: str
    status: Literal["OK", "DEGRADED", "UNREACHABLE"]
    latency_ms: float
    details: dict[str, Any] | None = None
    checked_at: str = ""
    queue_depth: int = 0
    dlq_depth: int = 0

    def __post_init__(self) -> None:
        if not self.checked_at:
            self.checked_at = datetime.now(timezone.utc).isoformat()


class StorageHealthAggregator:
    """Runs health checks across all engines and Redis, returns aggregate status."""

    def __init__(
        self,
        engines: dict[str, "StorageEngine"],
        redis_cache: "RedisCache",
    ) -> None:
        self._engines = engines
        self._redis = redis_cache

    async def check_all(self) -> dict[str, StorageHealth]:
        """Run all health checks concurrently. Returns dict keyed by engine name."""
        tasks: dict[str, asyncio.Task[StorageHealth]] = {}
        for name, engine in self._engines.items():
            tasks[name] = asyncio.create_task(self._check_engine(name, engine))
        tasks["redis"] = asyncio.create_task(self._check_redis())

        results: dict[str, StorageHealth] = {}
        for name, task in tasks.items():
            try:
                results[name] = await task
            except Exception as exc:
                results[name] = StorageHealth(
                    engine=name,
                    status="UNREACHABLE",
                    latency_ms=0.0,
                    details={"error": str(exc)},
                )
        return results

    async def overall_status(self) -> Literal["OK", "DEGRADED", "UNREACHABLE"]:
        """Aggregate logic:
        - OK: all engines OK
        - DEGRADED: any engine DEGRADED, or any secondary engine UNREACHABLE
        - UNREACHABLE: PostgreSQL or Redis UNREACHABLE (primary systems down)
        """
        checks = await self.check_all()
        primary = {"postgres", "redis"}
        for name in primary:
            if name in checks and checks[name].status == "UNREACHABLE":
                return "UNREACHABLE"
        for health in checks.values():
            if health.status != "OK":
                return "DEGRADED"
        return "OK"

    async def _check_engine(self, name: str, engine: "StorageEngine") -> StorageHealth:
        health = await engine.health_check()
        # Enrich with queue depths
        try:
            health.queue_depth = await self._redis.queue_depth(f"hydra:waq:{name}")
            health.dlq_depth = await self._redis.dlq_depth(f"hydra:dlq:{name}")
        except Exception:
            pass
        return health

    async def _check_redis(self) -> StorageHealth:
        return await self._redis.health_check()
