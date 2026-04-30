"""Post-ingestion correlation triggers from P8 cadence DAGs."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Any

from hydra.config import HydraSettings
from hydra.correlation.engine import CorrelationEngine
from hydra.correlation.exceptions import TriggerThrottledError
from hydra.correlation.models import CorrelationRunResult
from hydra.registry.stream_registry import StreamRegistry

logger = logging.getLogger(__name__)

# Pipeline → source tiers mapping
_PIPELINE_TIER_MAP: dict[str, list[int]] = {
    "geospatial_temporal": [1, 2, 3, 15, 18, 20, 23, 24, 25],
    "entity_network": [8, 14, 15, 16, 19, 21],
    "threat_convergence": [6, 15, 16, 19, 20, 27],
}

# Cadence → interval in seconds (for lookback computation)
_CADENCE_INTERVALS: dict[str, float] = {
    "sub_minute": 60.0,
    "realtime": 60.0,
    "15min": 900.0,
    "hourly": 3600.0,
    "daily": 86400.0,
    "weekly": 604800.0,
    "monthly": 2592000.0,
    "quarterly": 7776000.0,
    "annual": 31536000.0,
    "varies": 86400.0,
}


class CorrelationTrigger:
    """Handles post-ingestion correlation triggers from P8 cadence DAGs."""

    def __init__(
        self,
        engine: CorrelationEngine,
        registry: StreamRegistry,
        settings: HydraSettings,
        redis_cache: Any | None = None,
    ) -> None:
        self._engine = engine
        self._registry = registry
        self._settings = settings
        self._redis = redis_cache
        self._pipeline_tier_map = dict(_PIPELINE_TIER_MAP)
        self._last_trigger: dict[str, float] = {}

    async def on_ingestion_complete(
        self,
        completed_tiers: list[int],
        ingestion_timestamp: str,
    ) -> list[CorrelationRunResult]:
        """Called by P8 cadence DAG after successful ingestion.

        Determines which correlation pipelines should run based on
        which tiers just ingested fresh data.
        """
        results: list[CorrelationRunResult] = []
        completed_set = set(completed_tiers)

        for pipeline_id, source_tiers in self._pipeline_tier_map.items():
            trigger_tiers = sorted(completed_set & set(source_tiers))
            if not trigger_tiers:
                continue

            if not self._should_trigger(pipeline_id):
                logger.info(
                    "correlation_trigger_throttled",
                    extra={"pipeline_id": pipeline_id},
                )
                continue

            # Compute lookback window
            tw_start, tw_end = self._compute_lookback_window(
                completed_tiers, ingestion_timestamp
            )

            try:
                run_result = await self._engine.run(
                    pipeline_id=pipeline_id,
                    time_window_start=tw_start,
                    time_window_end=tw_end,
                    trigger_tiers=trigger_tiers,
                )
                results.append(run_result)
                # Record trigger time
                self._last_trigger[pipeline_id] = time.monotonic()
                if self._redis:
                    try:
                        await self._redis._redis.set(
                            f"hydra:correlation:last_trigger:{pipeline_id}",
                            datetime.now(timezone.utc).isoformat(),
                            ex=int(self._settings.correlation.min_trigger_interval_s * 2),
                        )
                    except Exception:
                        pass
            except Exception as exc:
                logger.error(
                    "correlation_trigger_error",
                    extra={"pipeline_id": pipeline_id, "error": str(exc)},
                )

        return results

    def _should_trigger(self, pipeline_id: str) -> bool:
        """Check minimum trigger interval."""
        min_interval = self._settings.correlation.min_trigger_interval_s
        last = self._last_trigger.get(pipeline_id)
        if last is None:
            return True
        elapsed = time.monotonic() - last
        return elapsed >= min_interval

    def _compute_lookback_window(
        self, completed_tiers: list[int], ingestion_timestamp: str
    ) -> tuple[str, str]:
        """Compute time window for correlation query.

        Default: 2x the longest cadence interval among completed tiers.
        Capped at max_lookback_s.
        """
        max_lookback = self._settings.correlation.max_lookback_s

        # Find the longest cadence interval among completed tiers
        longest_interval = 3600.0  # default 1 hour
        for tier_id in completed_tiers:
            tier = self._registry.get_tier(tier_id)
            if tier:
                cadence = tier.cadence
                interval = _CADENCE_INTERVALS.get(cadence, 3600.0)
                longest_interval = max(longest_interval, interval)

        lookback_s = min(longest_interval * 2, max_lookback)

        try:
            end_dt = datetime.fromisoformat(ingestion_timestamp)
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            end_dt = datetime.now(timezone.utc)

        start_dt = end_dt - timedelta(seconds=lookback_s)
        return start_dt.isoformat(), end_dt.isoformat()
