"""Tests for ConcurrencyManager — semaphore-based adapter execution limiter."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from hydra.scheduler.concurrency import ConcurrencyManager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_settings(global_limit: int = 10, cadence_limits: dict[str, int] | None = None) -> MagicMock:
    settings = MagicMock()
    settings.scheduler.global_concurrency_limit = global_limit
    settings.scheduler.cadence_concurrency_limits = cadence_limits or {
        "sub_minute": 4,
        "realtime": 3,
        "15min": 3,
        "hourly": 4,
        "daily": 6,
        "weekly": 4,
        "monthly_plus": 2,
    }
    return settings


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestConcurrencyManager:
    @pytest.mark.asyncio
    async def test_acquire_release(self):
        """Acquire succeeds, active_count increments, release decrements."""
        cm = ConcurrencyManager(_make_settings())

        acquired = await cm.acquire("daily")
        assert acquired is True
        assert cm.active_count == 1
        assert cm.active_by_cadence("daily") == 1

        await cm.release("daily")
        assert cm.active_count == 0
        assert cm.active_by_cadence("daily") == 0

    @pytest.mark.asyncio
    async def test_global_limit_enforced(self):
        """11th concurrent acquire blocks (global cap 10)."""
        cm = ConcurrencyManager(_make_settings(global_limit=10, cadence_limits={"daily": 20}))

        # Acquire 10 slots
        for _ in range(10):
            assert await cm.acquire("daily") is True

        assert cm.active_count == 10

        # 11th should timeout
        result = await cm.acquire("daily", timeout=0.05)
        assert result is False
        assert cm.active_count == 10

    @pytest.mark.asyncio
    async def test_cadence_limit_enforced(self):
        """Exceeding cadence limit blocks even if global has slots."""
        cm = ConcurrencyManager(_make_settings(global_limit=10, cadence_limits={"sub_minute": 2}))

        assert await cm.acquire("sub_minute") is True
        assert await cm.acquire("sub_minute") is True

        # 3rd sub_minute should timeout even though global has 8 slots left
        result = await cm.acquire("sub_minute", timeout=0.05)
        assert result is False
        assert cm.active_count == 2

    @pytest.mark.asyncio
    async def test_acquire_timeout(self):
        """Timeout returns False, does not deadlock."""
        cm = ConcurrencyManager(_make_settings(global_limit=1))

        assert await cm.acquire("daily") is True

        # Second acquire should timeout quickly
        result = await cm.acquire("daily", timeout=0.05)
        assert result is False

        # Release and verify we can acquire again
        await cm.release("daily")
        assert await cm.acquire("daily") is True

    @pytest.mark.asyncio
    async def test_active_by_cadence(self):
        """Per-cadence active count accurate."""
        cm = ConcurrencyManager(_make_settings())

        await cm.acquire("daily")
        await cm.acquire("daily")
        await cm.acquire("hourly")

        assert cm.active_by_cadence("daily") == 2
        assert cm.active_by_cadence("hourly") == 1
        assert cm.active_by_cadence("weekly") == 0
        assert cm.active_count == 3

        await cm.release("daily")
        assert cm.active_by_cadence("daily") == 1
        assert cm.active_count == 2
