"""Tests for SchedulerHealthAggregator — aggregate health reporting."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from hydra.scheduler.backpressure import BackpressureState, EngineBackpressure
from hydra.scheduler.concurrency import ConcurrencyManager
from hydra.scheduler.health import SchedulerHealth, SchedulerHealthAggregator
from hydra.storage.health import StorageHealth


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_bp_state(overall: str = "CLEAR") -> BackpressureState:
    return BackpressureState(
        overall=overall,
        engines={"postgres": EngineBackpressure("postgres", 100, 1000, 5000, "CLEAR")},
        checked_at=datetime.now(timezone.utc).isoformat(),
    )


def _make_storage_checks(statuses: dict[str, str] | None = None) -> dict[str, StorageHealth]:
    if statuses is None:
        statuses = {"postgres": "OK", "redis": "OK"}
    return {
        name: StorageHealth(engine=name, status=status, latency_ms=5.0)
        for name, status in statuses.items()
    }


def _make_aggregator(
    bp_overall: str = "CLEAR",
    storage_overall: str = "OK",
    storage_checks: dict[str, StorageHealth] | None = None,
) -> SchedulerHealthAggregator:
    bp = MagicMock()
    bp.check = AsyncMock(return_value=_make_bp_state(bp_overall))

    storage_agg = MagicMock()
    storage_agg.overall_status = AsyncMock(return_value=storage_overall)
    storage_agg.check_all = AsyncMock(return_value=storage_checks or _make_storage_checks())

    settings = MagicMock()
    settings.scheduler.global_concurrency_limit = 10
    settings.scheduler.cadence_concurrency_limits = {
        "sub_minute": 4, "realtime": 3, "daily": 6,
    }

    cm = ConcurrencyManager(settings)

    return SchedulerHealthAggregator(
        concurrency_manager=cm,
        backpressure_monitor=bp,
        storage_health_aggregator=storage_agg,
        settings=settings,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSchedulerHealthAggregator:
    @pytest.mark.asyncio
    async def test_health_ok(self):
        """All systems nominal → OK."""
        agg = _make_aggregator(bp_overall="CLEAR", storage_overall="OK")

        health = await agg.check()

        assert health.status == "OK"
        assert health.active_adapters == 0

    @pytest.mark.asyncio
    async def test_health_degraded_storage(self):
        """Storage DEGRADED → scheduler DEGRADED."""
        agg = _make_aggregator(storage_overall="DEGRADED")

        health = await agg.check()

        assert health.status == "DEGRADED"

    @pytest.mark.asyncio
    async def test_health_degraded_backpressure(self):
        """Backpressure THROTTLED → scheduler DEGRADED."""
        agg = _make_aggregator(bp_overall="THROTTLED")

        health = await agg.check()

        assert health.status == "DEGRADED"

    @pytest.mark.asyncio
    async def test_health_degraded_dead_streams(self):
        """Backpressure BLOCKED → scheduler DEGRADED."""
        agg = _make_aggregator(bp_overall="BLOCKED")

        health = await agg.check()

        assert health.status == "DEGRADED"

    @pytest.mark.asyncio
    async def test_health_unreachable(self):
        """PG/Redis down → UNREACHABLE."""
        agg = _make_aggregator(storage_overall="UNREACHABLE")

        health = await agg.check()

        assert health.status == "UNREACHABLE"
