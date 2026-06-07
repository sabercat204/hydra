"""TaskRunner — adapter execution + storage routing per stream."""

from __future__ import annotations

import asyncio
import importlib
import logging
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal

from hydra.models.normalized import NormalizedRecord
from hydra.scheduler.backpressure import BackpressureMonitor, BackpressureState
from hydra.scheduler.concurrency import ConcurrencyManager
from hydra.scheduler.exceptions import AdapterResolutionError, ConcurrencyTimeout

if TYPE_CHECKING:
    from hydra.auth.manager import AuthManager
    from hydra.config import HydraSettings
    from hydra.registry.stream_registry import StreamRegistry, StreamTier
    from hydra.storage.redis_cache import RedisCache
    from hydra.storage.router import RouteResult, StorageRouter

logger = logging.getLogger(__name__)


def _get_adapter_exceptions() -> tuple[type, type, type, type]:
    """Lazy-import adapter exception classes to avoid triggering heavy adapter __init__.

    Uses direct file loading to bypass hydra.adapters.__init__.py which may
    import modules with incompatible dependencies.
    """
    import importlib.util as _ilu
    from pathlib import Path

    # Check if the module is already loaded in sys.modules
    mod_name = "hydra.adapters.exceptions"
    if mod_name in sys.modules:
        mod = sys.modules[mod_name]
    else:
        # Find the exceptions.py file relative to the hydra package
        import hydra
        pkg_dir = Path(hydra.__file__).parent
        exc_path = pkg_dir / "adapters" / "exceptions.py"
        spec = _ilu.spec_from_file_location(mod_name, str(exc_path))
        mod = _ilu.module_from_spec(spec)
        sys.modules[mod_name] = mod
        spec.loader.exec_module(mod)

    return mod.FetchError, mod.ParseError, mod.RateLimitError, mod.ValidationError


@dataclass
class TaskResult:
    """Execution result for a single stream task."""

    stream_id: str
    adapter_type: str
    status: Literal["success", "partial", "skipped", "failed"]
    records_fetched: int
    records_routed: int
    records_deduplicated: int
    records_failed: int
    route_result: "RouteResult | None"
    duration_ms: float
    error: str | None = None
    fallback_used: bool = False
    backpressure_delayed: bool = False
    timestamp: str = ""

    def __post_init__(self) -> None:
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).isoformat()


# Adapter type → concrete class mapping (lazy import to avoid circular deps)
_ADAPTER_TYPE_MAP: dict[str, str] = {
    "rest_json": "hydra.adapters.rest_json.RestJsonAdapter",
    "fdsn": "hydra.adapters.fdsn.FdsnAdapter",
    "ckan": "hydra.adapters.ckan.CkanAdapter",
    "odata": "hydra.adapters.odata.ODataAdapter",
    "sdmx": "hydra.adapters.sdmx.SdmxAdapter",
    "tap_vo": "hydra.adapters.tap_vo.TapVoAdapter",
    "s3_bulk": "hydra.adapters.s3_bulk.S3BulkAdapter",
    "scrape_rss": "hydra.adapters.scrape_rss.ScrapeRssAdapter",
    "ais_adsb": "hydra.adapters.ais_adsb.AisAdsbAdapter",
    "stix_taxii": "hydra.adapters.stix_taxii.StixTaxiiAdapter",
    "doc_repo": "hydra.adapters.doc_repo.DocRepoAdapter",
}

# Dead stream tracking Redis key pattern
_STREAM_FAILURES_KEY = "hydra:stream_failures:{stream_id}"


class TaskRunner:
    """Executes a single adapter stream: fetch → parse → validate → normalize → route.

    Wires together BaseAdapter, AuthManager, and StorageRouter.
    Called by Airflow PythonOperator tasks.
    """

    def __init__(
        self,
        registry: "StreamRegistry",
        auth_manager: "AuthManager",
        storage_router: "StorageRouter",
        backpressure_monitor: BackpressureMonitor,
        concurrency_manager: ConcurrencyManager,
        settings: "HydraSettings",
        redis_cache: "RedisCache | None" = None,
    ) -> None:
        self._registry = registry
        self._auth = auth_manager
        self._router = storage_router
        self._backpressure = backpressure_monitor
        self._concurrency = concurrency_manager
        self._settings = settings
        self._redis = redis_cache
        self._adapter_cache: dict[str, type] = {}

    def _resolve_adapter_class(self, adapter_type: str) -> type:
        """Map adapter_type string from registry to concrete adapter class."""
        if adapter_type in self._adapter_cache:
            return self._adapter_cache[adapter_type]

        dotted_path = _ADAPTER_TYPE_MAP.get(adapter_type)
        if dotted_path is None:
            raise AdapterResolutionError(adapter_type)

        module_path, class_name = dotted_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        cls = getattr(module, class_name)
        self._adapter_cache[adapter_type] = cls
        return cls

    def _format_bp_detail(self, bp_state: BackpressureState) -> str:
        """Format backpressure state for logging."""
        parts = []
        for engine, ebp in bp_state.engines.items():
            if ebp.state != "CLEAR":
                parts.append(f"{engine}={ebp.queue_depth}/{ebp.hard_limit}({ebp.state})")
        return ", ".join(parts) if parts else "all clear"

    async def execute(self, stream_id: str, **context: Any) -> TaskResult:
        """Full pipeline execution for a single stream."""
        FetchError, ParseError, RateLimitError, ValidationError = _get_adapter_exceptions()

        start = time.monotonic()
        backpressure_delayed = False

        # Look up stream in registry
        tier = self._find_tier_for_stream(stream_id)
        adapter_type = tier.adapter if tier else "rest_json"
        cadence = tier.cadence if tier else "daily"

        # Step 1: Backpressure pre-check
        bp_state = await self._backpressure.check()

        if bp_state.overall == "BLOCKED":
            return TaskResult(
                stream_id=stream_id,
                adapter_type=adapter_type,
                status="skipped",
                records_fetched=0,
                records_routed=0,
                records_deduplicated=0,
                records_failed=0,
                route_result=None,
                duration_ms=self._elapsed(start),
                error=f"Backpressure BLOCKED: {self._format_bp_detail(bp_state)}",
            )

        if bp_state.overall == "THROTTLED":
            cleared = await self._backpressure.wait_for_clear()
            if not cleared:
                return TaskResult(
                    stream_id=stream_id,
                    adapter_type=adapter_type,
                    status="skipped",
                    records_fetched=0,
                    records_routed=0,
                    records_deduplicated=0,
                    records_failed=0,
                    route_result=None,
                    duration_ms=self._elapsed(start),
                    error=f"Backpressure THROTTLED timeout: {self._format_bp_detail(bp_state)}",
                    backpressure_delayed=True,
                )
            backpressure_delayed = True

        # Step 2: Concurrency slot acquisition
        acquired = await self._concurrency.acquire(cadence)
        if not acquired:
            return TaskResult(
                stream_id=stream_id,
                adapter_type=adapter_type,
                status="failed",
                records_fetched=0,
                records_routed=0,
                records_deduplicated=0,
                records_failed=0,
                route_result=None,
                duration_ms=self._elapsed(start),
                error=f"Concurrency timeout for cadence={cadence}",
            )

        try:
            # Steps 3-6: Execute adapter pipeline with fallback
            records, fallback_used, exec_adapter_type = await self._execute_with_fallback(
                stream_id, tier
            )

            # Route records
            route_result = await self._router.route(records)

            # Track success — reset dead stream counter
            await self._reset_failure_counter(stream_id)

            status: Literal["success", "partial", "skipped", "failed"] = "success"
            if route_result.failed > 0:
                status = "partial"

            return TaskResult(
                stream_id=stream_id,
                adapter_type=exec_adapter_type or adapter_type,
                status=status,
                records_fetched=len(records),
                records_routed=route_result.routed,
                records_deduplicated=route_result.deduplicated,
                records_failed=route_result.failed,
                route_result=route_result,
                duration_ms=self._elapsed(start),
                fallback_used=fallback_used,
                backpressure_delayed=backpressure_delayed,
            )

        except (ParseError, ValidationError, RateLimitError) as exc:
            await self._increment_failure_counter(stream_id, str(exc))
            return TaskResult(
                stream_id=stream_id,
                adapter_type=adapter_type,
                status="failed",
                records_fetched=0,
                records_routed=0,
                records_deduplicated=0,
                records_failed=0,
                route_result=None,
                duration_ms=self._elapsed(start),
                error=str(exc),
                backpressure_delayed=backpressure_delayed,
            )

        except Exception as exc:
            await self._increment_failure_counter(stream_id, str(exc))
            logger.error("task_runner_error", extra={"stream_id": stream_id, "error": str(exc)})
            return TaskResult(
                stream_id=stream_id,
                adapter_type=adapter_type,
                status="failed",
                records_fetched=0,
                records_routed=0,
                records_deduplicated=0,
                records_failed=0,
                route_result=None,
                duration_ms=self._elapsed(start),
                error=str(exc),
                backpressure_delayed=backpressure_delayed,
            )

        finally:
            await self._concurrency.release(cadence)

    async def _execute_with_fallback(
        self, stream_id: str, tier: "StreamTier | None"
    ) -> tuple[list[NormalizedRecord], bool, str]:
        """Try primary adapter. On FetchError, try fallback adapter if declared.

        Returns (records, fallback_used, adapter_type_used).
        Fallback is attempted once — no recursive fallback chains.
        RateLimitError (subclass of FetchError) does NOT trigger fallback.
        """
        FetchError, ParseError, RateLimitError, _ = _get_adapter_exceptions()

        adapter_type = tier.adapter if tier else "rest_json"
        fallback_type = tier.fallback if tier else None

        try:
            adapter_cls = self._resolve_adapter_class(adapter_type)
            adapter = adapter_cls(stream_id=stream_id, settings=self._settings, registry=self._registry)
            records = await adapter.run()
            return records, False, adapter_type

        except RateLimitError:
            # RateLimitError does not trigger fallback — re-raise
            raise

        except FetchError as primary_exc:
            if fallback_type is None:
                raise

            logger.warning(
                "fallback_attempt",
                extra={
                    "stream_id": stream_id,
                    "primary_adapter": adapter_type,
                    "error": str(primary_exc),
                    "action": "fallback_attempt",
                },
            )

            try:
                fallback_cls = self._resolve_adapter_class(fallback_type)
                fallback_adapter = fallback_cls(
                    stream_id=stream_id, settings=self._settings, registry=self._registry
                )
                records = await fallback_adapter.run()
                return records, True, fallback_type

            except Exception as fallback_exc:
                logger.error(
                    "fallback_failed",
                    extra={
                        "stream_id": stream_id,
                        "primary_error": str(primary_exc),
                        "fallback_error": str(fallback_exc),
                    },
                )
                raise primary_exc from fallback_exc

    def _find_tier_for_stream(self, stream_id: str) -> "StreamTier | None":
        """Look up the tier containing the given stream_id."""
        for tier in self._registry.tiers.values():
            for src in tier.sources:
                slug = src.name.lower().replace(" ", "_").replace("/", "_")
                if stream_id == slug or stream_id.startswith(slug):
                    return tier
        return None

    async def _increment_failure_counter(self, stream_id: str, error: str) -> None:
        """Increment consecutive failure counter in Redis."""
        if self._redis is None:
            return
        try:
            key = _STREAM_FAILURES_KEY.format(stream_id=stream_id)
            await self._redis._redis.hincrby(key, "consecutive_failures", 1)
            await self._redis._redis.hset(key, "last_failure_at", datetime.now(timezone.utc).isoformat())
            await self._redis._redis.hset(key, "last_error", error[:500])
        except Exception as exc:
            logger.warning("failure_counter_error", extra={"stream_id": stream_id, "error": str(exc)})

    async def _reset_failure_counter(self, stream_id: str) -> None:
        """Reset consecutive failure counter on success."""
        if self._redis is None:
            return
        try:
            key = _STREAM_FAILURES_KEY.format(stream_id=stream_id)
            await self._redis._redis.hset(key, "consecutive_failures", 0)
        except Exception as exc:
            logger.warning("failure_counter_reset_error", extra={"stream_id": stream_id, "error": str(exc)})

    @staticmethod
    def _elapsed(start: float) -> float:
        return round((time.monotonic() - start) * 1000, 2)
