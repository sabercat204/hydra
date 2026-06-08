"""SearchBackend protocol — the contract every backend must satisfy."""

from __future__ import annotations

from typing import Protocol

from hydra.mil_int.schemas.search import SearchRequest, SearchResponse


class SearchBackend(Protocol):
    """Pluggable search backend for the mil_int surface."""

    async def search(self, request: SearchRequest, *, index: str) -> SearchResponse:
        """Return matching records + facet counts for ``request``.

        Implementations MUST honour every filter on the request, return
        results limited to ``request.page_size``, and populate the
        ``facets`` block with at least one bucket per requested filter
        dimension (so the UI can render facet counts even when a filter
        is already active).
        """
        ...


__all__ = ["SearchBackend"]
