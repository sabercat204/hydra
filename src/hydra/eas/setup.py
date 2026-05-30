"""EAS application wiring — mount routers and wire singletons (task 17.1).

Two entry points:

* :func:`mount_eas_routers` — synchronous helper that registers the
  nine EAS routers on a :class:`FastAPI` app. Safe to call from
  :func:`hydra.api.app.create_app` because it doesn't touch any
  storage clients or run any coroutines. Each router already carries
  its ``/api/v1/...`` path prefix so we include it with an empty
  prefix on :meth:`FastAPI.include_router`.

* :func:`setup_eas` — the full async wiring entry point. Used by
  deployment-specific bootstrap scripts and integration-test fixtures
  to also wire dependency singletons (PG pool, ES client, MinIO,
  Redis, screenshot adapter, trends service, cost quota counter),
  create the Elasticsearch indexes via
  :func:`bootstrap_eas_indices`, register the
  :class:`AssetMonitor.on_record_ingested` hook on the storage
  writer, register the :class:`CVEPipeline` with the correlation
  engine, and stash the :class:`ExposureObservatory` generator so the
  observatory router can pull it directly (:class:`AnalysisEngine`
  has no public ``register_product`` for this pattern).

The split mirrors the recommended approach in the task request:
``create_app`` calls only :func:`mount_eas_routers` synchronously so
that routes are discoverable immediately at app-construction time;
the rest of the wiring is performed by a deployment-owner when the
full set of storage clients is available.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI

if TYPE_CHECKING:  # pragma: no cover - typing-only imports
    from hydra.config import HydraSettings

logger = logging.getLogger(__name__)

__all__ = ["mount_eas_routers", "setup_eas"]


# ---------------------------------------------------------------------------
# Router mount — synchronous, safe to call from ``create_app``.
# ---------------------------------------------------------------------------


def mount_eas_routers(app: FastAPI) -> None:
    """Register the nine EAS routers on ``app`` (Design §2.4, §7.1).

    Each router module (``hydra.eas.routers.*``) already prefixes its
    operations with ``/api/v1/...`` so we include them with an empty
    prefix. Import-time side effects are kept to the minimum required
    by each router — none of them instantiate storage clients at
    import time, so the call is cheap.
    """

    from hydra.eas.routers.assets import router as assets_router
    from hydra.eas.routers.cves import router as cves_router
    from hydra.eas.routers.exploits import router as exploits_router
    from hydra.eas.routers.images import router as images_router
    from hydra.eas.routers.jobs import router as jobs_router
    from hydra.eas.routers.lookup import router as lookup_router
    from hydra.eas.routers.maps import router as maps_router
    from hydra.eas.routers.observatory import router as observatory_router
    from hydra.eas.routers.trends import router as trends_router

    for router in (
        assets_router,
        images_router,
        cves_router,
        exploits_router,
        maps_router,
        trends_router,
        jobs_router,
        lookup_router,
        observatory_router,
    ):
        app.include_router(router)

    logger.info("eas.setup.routers_mounted", extra={"router_count": 9})


# ---------------------------------------------------------------------------
# Async wiring — called from deployment bootstrap / test fixtures.
# ---------------------------------------------------------------------------


async def setup_eas(
    app: FastAPI,
    settings: "HydraSettings",
    *,
    es_client: Any = None,
    minio_client: Any = None,
    redis_client: Any = None,
    pg_pool: Any = None,
    storage_writer: Any = None,
    correlation_engine: Any = None,
    analysis_engine: Any = None,
    screenshot_adapter: Any = None,
    trends_service: Any = None,
    cost_quota_counter: Any = None,
    lookup_cache: Any = None,
    lookup_singleflight: Any = None,
    lookup_assembler: Any = None,
    observatory_generator: Any = None,
) -> None:
    """Wire the EAS subsystem end-to-end.

    Step-by-step:

    1. Ensure routers are mounted (idempotent — :meth:`FastAPI.include_router`
       would duplicate operations if called twice, so we skip this when
       the app already exposes an EAS path).
    2. Register the PG pool on :mod:`hydra.api.dependencies` when
       supplied so tenant-scoped repositories see the same pool as the
       rest of the API.
    3. Register the injected storage clients via
       :func:`hydra.eas.dependencies.set_eas_clients`.
    4. Wire the lookup singletons via
       :func:`hydra.eas.routers.lookup.set_lookup_components` and the
       observatory generator via
       :func:`hydra.eas.routers.observatory.set_observatory_components`.
    5. Bootstrap the EAS Elasticsearch indexes via
       :func:`bootstrap_eas_indices` when ``es_client`` is available.
    6. Register :meth:`AssetMonitor.on_record_ingested` as a post-insert
       hook on the supplied storage writer (best effort — absent /
       incompatible writers are logged and skipped).
    7. Register :class:`CVEPipeline` with the correlation engine when
       one is supplied.
    8. Fail fast per R26.4 when :attr:`EASSettings.screenshot.ocr_enabled`
       is true but :mod:`pytesseract` cannot be imported.

    Every wiring step is guarded so a caller can invoke :func:`setup_eas`
    with only a subset of storage clients (e.g. tests that only need
    routers + ES bootstrap). Missing dependencies degrade specific
    capabilities to 503 responses rather than crashing start-up.
    """

    # (1) Routers — idempotent mount.
    if not _routers_already_mounted(app):
        mount_eas_routers(app)

    # (8) Fail-fast import guard (R26.4). Runs before any wiring so
    # that a misconfigured environment fails loudly instead of half-
    # booting the EAS stack.
    _check_ocr_availability(settings)

    # (2) Register the PG pool with the shared API dependency module
    # so tenant-scoped repositories and the job manager see it.
    if pg_pool is not None:
        from hydra.api.dependencies import set_engines

        set_engines(db_pool=pg_pool)

    # (3) Wire storage singletons. ``set_eas_clients`` only installs
    # non-None arguments, so a partial call is safe.
    from hydra.eas.dependencies import set_eas_clients

    set_eas_clients(
        es_client=es_client,
        minio_client=minio_client,
        screenshot_adapter=screenshot_adapter,
        redis=redis_client,
        trends_service=trends_service,
        cost_quota_counter=cost_quota_counter,
    )

    # (4) Lookup + observatory singletons.
    if lookup_cache is not None or lookup_singleflight is not None or lookup_assembler is not None:
        from hydra.eas.routers.lookup import set_lookup_components

        set_lookup_components(
            cache=lookup_cache,
            singleflight=lookup_singleflight,
            assembler=lookup_assembler,
        )

    if observatory_generator is not None:
        from hydra.eas.routers.observatory import set_observatory_components

        set_observatory_components(generator=observatory_generator)

    # (5) Elasticsearch index bootstrap (R24.5).
    if es_client is not None:
        from hydra.eas.storage.bootstrap import bootstrap_eas_indices

        try:
            results = await bootstrap_eas_indices(es_client)
            logger.info(
                "eas.setup.indices_bootstrapped",
                extra={"results": results},
            )
        except Exception as exc:  # noqa: BLE001 — log and continue
            logger.error(
                "eas.setup.index_bootstrap_failed",
                extra={"error": str(exc)},
            )

    # (6) AssetMonitor hook on the storage writer. Guarded with
    # try/except AttributeError so a writer that doesn't expose the
    # post-insert hook registry (e.g. a test stub) doesn't blow up
    # the whole bootstrap.
    if storage_writer is not None:
        _register_asset_monitor_hook(
            storage_writer=storage_writer,
            settings=settings,
            pg_pool=pg_pool,
            redis_client=redis_client,
        )

    # (7) CVE pipeline registration (R10.1, Design §8.3).
    if correlation_engine is not None:
        _register_cve_pipeline(
            correlation_engine=correlation_engine,
            settings=settings,
        )

    # (Observatory note — Design §8.6 / task 17.1 requirement 6.)
    # AnalysisEngine doesn't expose a public ``register_product``
    # analog for the observatory. Instead, the generator instance is
    # stashed via ``set_observatory_components`` above and the
    # observatory router pulls it directly. When an analysis engine
    # is supplied without an explicit generator, we can't construct
    # one here (the generator needs a PG pool + MinIO client); the
    # caller is responsible for wiring one via
    # ``observatory_generator=`` on :func:`setup_eas`.
    del analysis_engine  # acknowledged but unused; see note above.

    logger.info("eas.setup.complete")


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _routers_already_mounted(app: FastAPI) -> bool:
    """Return ``True`` when an EAS-owned route is already registered.

    Used to make :func:`setup_eas` idempotent when ``create_app`` has
    already mounted the routers via :func:`mount_eas_routers`.
    """

    try:
        paths = {getattr(route, "path", "") for route in app.routes}
    except Exception:  # noqa: BLE001 — defensive
        return False
    # A handful of unique EAS paths — any one of them is a sufficient
    # signal that the mount has already happened.
    sentinel_paths = {
        "/api/v1/assets",
        "/api/v1/lookup/{indicator}",
        "/api/v1/maps/features",
        "/api/v1/trends",
        "/api/v1/observatory/latest",
    }
    return bool(sentinel_paths & paths)


def _check_ocr_availability(settings: "HydraSettings") -> None:
    """Raise :class:`RuntimeError` when OCR is enabled but unavailable (R26.4).

    Called before any other wiring so that a deployment with
    ``EAS__SCREENSHOT__OCR_ENABLED=true`` fails loudly at boot when
    :mod:`pytesseract` isn't installed, rather than producing silent
    runtime errors per-screenshot later.
    """

    screenshot_settings = getattr(settings.eas, "screenshot", None)
    if screenshot_settings is None:
        return
    if not getattr(screenshot_settings, "ocr_enabled", False):
        return

    try:
        import pytesseract  # noqa: F401 - import-only probe
    except ImportError as exc:
        logger.error(
            "eas.setup.ocr_dependency_missing",
            extra={
                "setting": "eas.screenshot.ocr_enabled",
                "missing_module": "pytesseract",
                "error": str(exc),
            },
        )
        raise RuntimeError(
            "eas.screenshot.ocr_enabled is True but pytesseract is not "
            "importable. Install the [eas] extra or disable OCR."
        ) from exc


def _register_asset_monitor_hook(
    *,
    storage_writer: Any,
    settings: "HydraSettings",
    pg_pool: Any,
    redis_client: Any,
) -> None:
    """Build an :class:`AssetMonitor` and wire it as a post-insert hook.

    The monitor needs a full dependency graph (asset repo, exposure
    repo, extractor, matcher, alerter, Redis cache). When any required
    piece is missing (typically ``pg_pool`` in tests that wire a
    minimal writer), we log a warning and return without mutating the
    writer — the downstream effect is simply that exposures aren't
    produced from the ingest path, which is the correct behaviour for
    a partial wiring.
    """

    try:
        register = getattr(storage_writer, "register_post_insert_hook", None)
        if register is None:
            raise AttributeError("storage_writer has no register_post_insert_hook")
    except AttributeError as exc:
        logger.warning(
            "eas.setup.writer_hook_missing",
            extra={"error": str(exc)},
        )
        return

    if pg_pool is None or redis_client is None:
        logger.warning(
            "eas.setup.asset_monitor_skipped",
            extra={
                "reason": "missing pg_pool or redis_client",
                "pg_pool": pg_pool is not None,
                "redis_client": redis_client is not None,
            },
        )
        return

    try:
        from hydra.eas.assets.alerter import ExposureAlerter
        from hydra.eas.assets.extractor import IndicatorExtractor
        from hydra.eas.assets.matcher import AssetMatcher
        from hydra.eas.assets.monitor import AssetMonitor
        from hydra.eas.assets.repository import (
            AssetRepository,
            ExposureRepository,
        )

        asset_repo = AssetRepository(pg_pool)
        exposure_repo = ExposureRepository(pg_pool)
        extractor = IndicatorExtractor(settings.eas)
        matcher = AssetMatcher(
            asn_database_path=settings.eas.asn_database_path,
        )
        alerter = ExposureAlerter(settings=settings.eas, pool=pg_pool)
        monitor = AssetMonitor(
            settings=settings.eas,
            asset_repo=asset_repo,
            exposure_repo=exposure_repo,
            extractor=extractor,
            matcher=matcher,
            alerter=alerter,
            redis_cache=redis_client,
        )
    except Exception as exc:  # noqa: BLE001 — wiring failure is non-fatal
        logger.warning(
            "eas.setup.asset_monitor_build_failed",
            extra={"error": str(exc)},
        )
        return

    try:
        register(monitor.on_record_ingested)
    except AttributeError as exc:
        logger.warning(
            "eas.setup.writer_hook_register_failed",
            extra={"error": str(exc)},
        )
        return

    logger.info("eas.setup.asset_monitor_hooked")


def _register_cve_pipeline(
    *,
    correlation_engine: Any,
    settings: "HydraSettings",
) -> None:
    """Register :class:`CVEPipeline` with the correlation engine (Design §8.3)."""

    try:
        from hydra.eas.cves.pipeline import CVEPipeline

        pipeline = CVEPipeline(settings=settings.eas)
        correlation_engine.register_pipeline(pipeline)
    except Exception as exc:  # noqa: BLE001 — log and continue
        logger.warning(
            "eas.setup.cve_pipeline_register_failed",
            extra={"error": str(exc)},
        )
        return

    logger.info("eas.setup.cve_pipeline_registered")
