"""FastAPI dependency helpers for EAS routers (R20.2).

Re-exports :func:`hydra.api.dependencies.get_current_tenant_id` so EAS
routers can depend on ``Depends(get_current_tenant_id)`` without importing
from the ``hydra.api`` layer directly. The dependency resolves the current
``X-API-Key`` via :func:`hydra.api.dependencies.get_current_api_key` and
returns the caller's ``tenant_id`` as a :class:`uuid.UUID`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from hydra.api.dependencies import (
    APIKeyRecord,
    get_current_api_key,
    get_current_tenant_id,
    get_db_pool,
)
from hydra.config import HydraSettings, settings

if TYPE_CHECKING:  # pragma: no cover - typing-only imports
    from hydra.eas.assets.repository import AssetRepository, ExposureRepository


async def get_eas_settings() -> HydraSettings:
    """Return the process-wide :class:`HydraSettings` instance.

    EAS components and routers use this to reach ``settings.eas.*`` values
    without hard-coding the module-level global. Later tasks can swap the
    implementation (e.g. for test overrides) by monkey-patching this symbol.
    """

    return settings


# ---------------------------------------------------------------------------
# Repository dependencies (task 7.11)
#
# ``setup_eas`` (task 17.1) is responsible for wiring real pools — these
# helpers read from the module-level ``get_db_pool`` hook in
# :mod:`hydra.api.dependencies` so test doubles and production pools flow
# through the same path. We keep the constructor lazy (``AssetRepository(pool)``)
# because import order matters: the repository module imports the schemas
# module, and we want the dependency graph to be acyclic at import time.
# ---------------------------------------------------------------------------


async def get_asset_repository() -> "AssetRepository":
    """Return a :class:`AssetRepository` bound to the active PG pool."""

    from hydra.eas.assets.repository import AssetRepository

    pool = await get_db_pool()
    return AssetRepository(pool)


async def get_exposure_repository() -> "ExposureRepository":
    """Return an :class:`ExposureRepository` bound to the active PG pool."""

    from hydra.eas.assets.repository import ExposureRepository

    pool = await get_db_pool()
    return ExposureRepository(pool)


# ---------------------------------------------------------------------------
# Screenshot / images dependencies (task 8.8)
#
# Singletons are wired by ``setup_eas`` (task 17.1) into the module-level
# private globals below. Until ``setup_eas`` runs the getters return
# ``None``; routers that depend on them should surface a 503 rather than
# crash. Test fixtures override the getters with their own factories.
# ---------------------------------------------------------------------------


_es_client: object | None = None
_minio_client: object | None = None
_screenshot_adapter: object | None = None
_redis: object | None = None
_trends_service: object | None = None
_cost_quota_counter: object | None = None


def set_eas_clients(
    *,
    es_client: object | None = None,
    minio_client: object | None = None,
    screenshot_adapter: object | None = None,
    redis: object | None = None,
    trends_service: object | None = None,
    cost_quota_counter: object | None = None,
) -> None:
    """Wire the singleton storage clients used by EAS routers.

    Called from :func:`hydra.eas.setup.setup_eas` at startup; also used
    by tests to inject fakes. Only non-``None`` arguments are installed so
    that partial overrides during testing don't clobber previously-set
    singletons.
    """

    global _es_client, _minio_client, _screenshot_adapter, _redis, _trends_service
    global _cost_quota_counter
    if es_client is not None:
        _es_client = es_client
    if minio_client is not None:
        _minio_client = minio_client
    if screenshot_adapter is not None:
        _screenshot_adapter = screenshot_adapter
    if redis is not None:
        _redis = redis
    if trends_service is not None:
        _trends_service = trends_service
    if cost_quota_counter is not None:
        _cost_quota_counter = cost_quota_counter


async def get_es_client() -> object | None:
    """Return the shared Elasticsearch client (or ``None`` until wired)."""

    return _es_client


async def get_minio_client() -> object | None:
    """Return the shared MinIO client (or ``None`` until wired)."""

    return _minio_client


async def get_screenshot_adapter() -> object | None:
    """Return the shared :class:`ScreenshotAdapter` instance."""

    return _screenshot_adapter


async def get_eas_redis() -> object | None:
    """Return the shared Redis client used by EAS workers / quotas."""

    return _redis


async def get_trends_service() -> object | None:
    """Return the shared :class:`TrendsService` singleton (R14.1).

    Wired by :func:`hydra.eas.setup.setup_eas` — the router depends on
    this to avoid importing the service module at router-import time,
    which would otherwise force a hard InfluxDB client dep on every
    test that only wants to exercise the validator path.
    """

    return _trends_service


async def get_cost_quota_counter() -> object | None:
    """Return the shared :class:`CostQuotaCounter` singleton (R22.1).

    Wired by :func:`hydra.eas.setup.setup_eas` using the same Redis
    client that ``RateLimitMiddleware`` already owns. Returning ``None``
    before ``setup_eas`` has run is expected; the :func:`enforce_cost_quota`
    dependency converts that into a 503 so unauthenticated probes of
    quota-gated routes don't get a confusing 500.
    """

    return _cost_quota_counter


__all__ = [
    "APIKeyRecord",
    "get_current_api_key",
    "get_current_tenant_id",
    "get_eas_settings",
    "get_asset_repository",
    "get_exposure_repository",
    "set_eas_clients",
    "get_es_client",
    "get_minio_client",
    "get_screenshot_adapter",
    "get_eas_redis",
    "get_trends_service",
    "get_cost_quota_counter",
]
