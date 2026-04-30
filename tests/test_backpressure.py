"""Tests for BackpressureMonitor — WAQ depth checks."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from hydra.scheduler.backpressure import BackpressureMonitor, ENGINE_QUEUE_KEYS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_settings(
    soft: int = 1000,
    hard: int = 5000,
    wait_timeout: float = 60.0,
    poll_interval: float = 5.0,
    overrides: dict | None = None,
) -> MagicMock:
    settings = MagicMock()
    settings.scheduler.backpressure_soft_limit = soft
    settings.scheduler.backpressure_hard_limit = hard
    settings.scheduler.backpressure_wait_timeout = wait_timeout
    settings.scheduler.backpressure_poll_interval = poll_interval
    settings.scheduler.engine_backpressure_overrides = overrides or {}
    return settings


def _make_redis(depths: dict[str, int] | None = None) -> MagicMock:
    """Create a mock RedisCache with configurable queue depths."""
    if depths is None:
        depths = {}
    redis = MagicMock()

    async def queue_depth(key: str) -> int:
        # Map queue key back to engine name
        for engine, qkey in ENGINE_QUEUE_KEYS.items():
            if qkey == key:
                return depths.get(engine, 0)
        return 0

    redis.queue_depth = AsyncMock(side_effect=queue_depth)
    return redis


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestBackpressureMonitor:
    @pytest.mark.asyncio
    async def test_check_all_clear(self):
        """All engines below soft → CLEAR."""
        redis = _make_redis({"postgres": 100, "influxdb": 200, "elasticsearch": 50, "neo4j": 0, "minio": 0})
        monitor = BackpressureMonitor(redis, _make_settings())

        state = await monitor.check()

        assert state.overall == "CLEAR"
        for ebp in state.engines.values():
            assert ebp.state == "CLEAR"

    @pytest.mark.asyncio
    async def test_check_throttled(self):
        """One engine between soft and hard → THROTTLED."""
        redis = _make_redis({"postgres": 2000, "influxdb": 100})
        monitor = BackpressureMonitor(redis, _make_settings(soft=1000, hard=5000))

        state = await monitor.check()

        assert state.overall == "THROTTLED"
        assert state.engines["postgres"].state == "THROTTLED"
        assert state.engines["influxdb"].state == "CLEAR"

    @pytest.mark.asyncio
    async def test_check_blocked(self):
        """One engine at hard → BLOCKED."""
        redis = _make_redis({"postgres": 5000})
        monitor = BackpressureMonitor(redis, _make_settings(soft=1000, hard=5000))

        state = await monitor.check()

        assert state.overall == "BLOCKED"
        assert state.engines["postgres"].state == "BLOCKED"

    @pytest.mark.asyncio
    async def test_per_engine_check(self):
        """Individual engine check returns correct state."""
        redis = _make_redis({"elasticsearch": 3000})
        monitor = BackpressureMonitor(redis, _make_settings(soft=1000, hard=5000))

        ebp = await monitor.check_engine("elasticsearch")

        assert ebp.engine == "elasticsearch"
        assert ebp.queue_depth == 3000
        assert ebp.state == "THROTTLED"

    @pytest.mark.asyncio
    async def test_wait_for_clear_succeeds(self):
        """Queue drains within timeout → returns True."""
        call_count = 0

        async def decreasing_depth(key: str) -> int:
            nonlocal call_count
            call_count += 1
            # First call: throttled, second call: clear
            return 2000 if call_count <= 1 else 500

        redis = MagicMock()
        redis.queue_depth = AsyncMock(side_effect=decreasing_depth)
        monitor = BackpressureMonitor(redis, _make_settings(soft=1000, hard=5000, poll_interval=0.01))

        result = await monitor.wait_for_clear(max_wait=1.0, poll_interval=0.01)

        assert result is True

    @pytest.mark.asyncio
    async def test_wait_for_clear_timeout(self):
        """Queue stays high → returns False after timeout."""
        redis = _make_redis({"postgres": 3000})
        monitor = BackpressureMonitor(redis, _make_settings(soft=1000, hard=5000))

        result = await monitor.wait_for_clear(max_wait=0.05, poll_interval=0.01)

        assert result is False

    @pytest.mark.asyncio
    async def test_engine_override_thresholds(self):
        """Per-engine overrides applied correctly."""
        redis = _make_redis({"postgres": 3000})
        settings = _make_settings(
            soft=1000,
            hard=5000,
            overrides={"postgres": {"soft_limit": 2000, "hard_limit": 10000}},
        )
        monitor = BackpressureMonitor(redis, settings)

        ebp = await monitor.check_engine("postgres")

        assert ebp.soft_limit == 2000
        assert ebp.hard_limit == 10000
        assert ebp.state == "THROTTLED"  # 3000 >= 2000 soft, < 10000 hard
