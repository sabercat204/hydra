"""Router collection — all API routers."""

from hydra.api.routers.correlations import router as correlations_router
from hydra.api.routers.graph import router as graph_router
from hydra.api.routers.health import router as health_router
from hydra.api.routers.products import router as products_router
from hydra.api.routers.records import router as records_router
from hydra.api.routers.registry import router as registry_router
from hydra.api.routers.timeline import router as timeline_router
from hydra.api.routers.watchlists import router as watchlists_router

all_routers = [
    products_router,
    records_router,
    correlations_router,
    graph_router,
    timeline_router,
    health_router,
    watchlists_router,
    registry_router,
]

__all__ = ["all_routers"]
