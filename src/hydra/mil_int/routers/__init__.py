"""FastAPI routers for the mil_int surface."""

from hydra.mil_int.routers.compliance import router as compliance_router
from hydra.mil_int.routers.doctrine import router as doctrine_router
from hydra.mil_int.routers.manifest import router as manifest_router
from hydra.mil_int.routers.search import router as search_router
from hydra.mil_int.routers.standards import router as standards_router

all_routers = (
    search_router,
    standards_router,
    doctrine_router,
    compliance_router,
    manifest_router,
)

__all__ = ["all_routers"]
