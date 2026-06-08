"""Mil-Int surface application wiring.

Mirrors :func:`hydra.eas.setup.mount_eas_routers` /
:func:`hydra.eas.setup.setup_eas`. Two entry points:

* :func:`mount_mil_int_routers` — synchronous router mount, safe from
  :func:`hydra.api.app.create_app`.
* :func:`setup_mil_int` — async wiring of singletons (xref resolver,
  search backend) for deployment-time bootstrap and integration tests.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI

if TYPE_CHECKING:
    from hydra.config import HydraSettings

logger = logging.getLogger(__name__)


def mount_mil_int_routers(app: FastAPI) -> None:
    """Register the five mil_int routers on ``app``."""
    from hydra.mil_int.routers import all_routers

    for router in all_routers:
        app.include_router(router)
    logger.info("mil_int.setup.routers_mounted", extra={"router_count": len(all_routers)})


async def setup_mil_int(
    app: FastAPI,
    settings: "HydraSettings",
    *,
    search_backend: Any | None = None,
    es_client: Any | None = None,
) -> None:
    """Wire the mil_int subsystem.

    1. Mount routers (idempotent).
    2. Build / cache the standards xref resolver from the seed file.
    3. Register the search backend. If ``search_backend`` is supplied
       it's used directly; otherwise an ``es_client`` is wrapped in
       :class:`ElasticsearchSearchBackend`. Without either, the search
       endpoint returns 503 until configured.
    """
    if not _routers_already_mounted(app):
        mount_mil_int_routers(app)

    from hydra.mil_int.dependencies import set_mil_int_components
    from hydra.mil_int.xref.resolver import XrefResolver

    resolver = XrefResolver.from_path(settings.mil_int.xref_seed_path)

    backend = search_backend
    if backend is None and es_client is not None:
        from hydra.mil_int.search.elasticsearch import ElasticsearchSearchBackend

        backend = ElasticsearchSearchBackend(es_client)

    set_mil_int_components(
        settings=settings,
        xref_resolver=resolver,
        search_backend=backend,
    )
    logger.info(
        "mil_int.setup.complete",
        extra={
            "xref_size": resolver.size,
            "search_backend": backend is not None,
            "search_backend_kind": type(backend).__name__ if backend else None,
        },
    )


def _routers_already_mounted(app: FastAPI) -> bool:
    sentinels = {
        "/api/v1/mil-int/manifest",
        "/api/v1/mil-int/standards/xref",
        "/api/v1/mil-int/search",
    }
    try:
        paths = {getattr(route, "path", "") for route in app.routes}
    except Exception:  # noqa: BLE001
        return False
    return bool(sentinels & paths)


__all__ = ["mount_mil_int_routers", "setup_mil_int"]
