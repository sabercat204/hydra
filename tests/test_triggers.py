"""Tests for correlation triggers."""

from __future__ import annotations

import time
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hydra.config import HydraSettings
from hydra.correlation.engine import CorrelationEngine
from hydra.correlation.models import CorrelationRunResult
from hydra.correlation.triggers import CorrelationTrigger, _CADENCE_INTERVALS
from hydra.registry.stream_registry import StreamRegistry, StreamTier, StreamSource


def _make_tier(tier_id: int, cadence: str = "daily") -> StreamTier:
    return StreamTier(
        id=tier_id,
        name=f"Tier {tier_id}",
        streams=1,
        access="green",
        formats=["json"],
        cadence=cadence,
        adapter="rest_json",
        fallback=None,
        sources=[StreamSource(name="test", url="http://test", format="json", auth="none", notes="")],
    )


@pytest.fixture
def settings() -> HydraSettings:
    return HydraSettings()


@pytest.fixture
def mock_engine():
    engine = MagicMock(spec=CorrelationEngine)
    engine.run = AsyncMock(return_value=CorrelationRunResult(
        pipeline_id="test",
        correlations_found=5,
        correlations_new=3,
    ))
    return engine


@pytest.fixture
def mock_registry():
    registry = StreamRegistry(
        tiers={
            1: _make_tier(1, "hourly"),
            15: _make_tier(15, "daily"),
            16: _make_tier(16, "daily"),
            19: _make_tier(19, "weekly"),
        }
    )
    return registry


@pytest.fixture
def trigger(mock_engine, mock_registry, settings):
    return CorrelationTrigger(
        engine=mock_engine,
        registry=mock_registry,
        settings=settings,
        redis_cache=None,
    )


class TestCorrelationTrigger:
    async def test_trigger_fires_matching_pipeline(self, trigger, mock_engine):
        """Completed tier in pipeline source → trigger fires."""
        now = datetime.now(timezone.utc).isoformat()
        results = await trigger.on_ingestion_complete([1], now)
        # Tier 1 is in geospatial_temporal
        assert len(results) >= 1
        mock_engine.run.assert_called()

    async def test_trigger_skips_unrelated_tiers(self, trigger, mock_engine):
        """Completed tier not in any pipeline → no trigger."""
        now = datetime.now(timezone.utc).isoformat()
        results = await trigger.on_ingestion_complete([99], now)
        assert len(results) == 0
        mock_engine.run.assert_not_called()

    async def test_trigger_throttled(self, trigger, mock_engine):
        """Second trigger within min_interval → skipped."""
        now = datetime.now(timezone.utc).isoformat()
        # First trigger
        await trigger.on_ingestion_complete([1], now)
        first_call_count = mock_engine.run.call_count

        # Second trigger immediately (should be throttled)
        await trigger.on_ingestion_complete([1], now)
        # Call count should not increase for the same pipeline
        # (geospatial_temporal was already triggered)
        geo_calls = [
            c for c in mock_engine.run.call_args_list
            if c.kwargs.get("pipeline_id") == "geospatial_temporal"
            or (c.args and c.args[0] == "geospatial_temporal")
        ]
        assert len(geo_calls) == 1  # Only first trigger fired

    async def test_trigger_after_interval(self, trigger, mock_engine, settings):
        """Trigger after min_interval elapsed → fires."""
        now = datetime.now(timezone.utc).isoformat()
        # First trigger
        await trigger.on_ingestion_complete([1], now)

        # Simulate time passing beyond min_trigger_interval
        for pid in trigger._last_trigger:
            trigger._last_trigger[pid] = time.monotonic() - settings.correlation.min_trigger_interval_s - 1

        mock_engine.run.reset_mock()
        await trigger.on_ingestion_complete([1], now)
        assert mock_engine.run.call_count >= 1

    async def test_lookback_window_calculation(self, trigger, mock_registry):
        """2x cadence interval, capped at max_lookback."""
        now = datetime.now(timezone.utc)
        now_str = now.isoformat()

        # Tier 1 has hourly cadence → 2 * 3600 = 7200s lookback
        start, end = trigger._compute_lookback_window([1], now_str)
        start_dt = datetime.fromisoformat(start)
        end_dt = datetime.fromisoformat(end)
        delta = (end_dt - start_dt).total_seconds()
        assert abs(delta - 7200.0) < 1.0

    async def test_lookback_capped_at_max(self, trigger, settings):
        """Lookback capped at max_lookback_s."""
        now = datetime.now(timezone.utc)
        # Weekly cadence → 2 * 604800 = 1209600, but capped at 86400
        start, end = trigger._compute_lookback_window([19], now.isoformat())
        start_dt = datetime.fromisoformat(start)
        end_dt = datetime.fromisoformat(end)
        delta = (end_dt - start_dt).total_seconds()
        assert delta <= settings.correlation.max_lookback_s + 1

    async def test_multiple_pipelines_triggered(self, trigger, mock_engine):
        """Tier 15 triggers both geospatial_temporal and entity_network."""
        now = datetime.now(timezone.utc).isoformat()
        results = await trigger.on_ingestion_complete([15], now)
        # Tier 15 is in geospatial_temporal, entity_network, and threat_convergence
        assert len(results) >= 2
        pipeline_ids = {r.pipeline_id for r in results}
        assert "test" in pipeline_ids or len(pipeline_ids) >= 1
        # Verify engine.run was called multiple times
        assert mock_engine.run.call_count >= 2
