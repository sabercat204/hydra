"""Full-text + faceted search over the mil_int document index.

The router delegates the actual query to a pluggable search backend. When
no backend is wired (development / tests without Elasticsearch), the
endpoint returns a 503 — the dependency layer surfaces that uniformly.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends

from hydra.mil_int.dependencies import get_mil_int_settings, get_search_backend
from hydra.mil_int.schemas.search import SearchRequest, SearchResponse
from hydra.mil_int.settings import MilIntSettings


router = APIRouter(prefix="/api/v1/mil-int", tags=["mil-int"])


@router.post("/search", response_model=SearchResponse)
async def search(
    req: SearchRequest,
    backend: Any = Depends(get_search_backend),
    settings: MilIntSettings = Depends(get_mil_int_settings),
) -> SearchResponse:
    """Search documents indexed under the mil_int surface.

    The backend is expected to expose
    ``async def search(request: SearchRequest, *, index: str) -> SearchResponse``.
    Decoupling the router from the concrete backend lets ES/OpenSearch /
    a SQL fallback / a test stub all satisfy the contract.
    """
    page_size = min(req.page_size, settings.search_max_page_size)
    if page_size != req.page_size:
        req = req.model_copy(update={"page_size": page_size})
    return await backend.search(req, index=settings.search_index_name)
