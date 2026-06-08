"""In-memory search backend — deterministic fallback for tests and dev.

Holds a list of :class:`MilIntRecord` instances and answers
:meth:`search` against them with naive token matching, exact-match
filters, and Python-side faceting. Not intended for production use; the
algorithmic complexity is O(n) per query.
"""

from __future__ import annotations

from collections import Counter
from typing import Iterable

from hydra.mil_int.schemas.record import MilIntRecord
from hydra.mil_int.schemas.search import (
    SearchFacet,
    SearchFacetValue,
    SearchRequest,
    SearchResponse,
)


class InMemorySearchBackend:
    """Indexless backend that scans a list of records on every query."""

    def __init__(self, records: Iterable[MilIntRecord] | None = None) -> None:
        self._records: list[MilIntRecord] = list(records or [])

    def add(self, record: MilIntRecord) -> None:
        self._records.append(record)

    def add_many(self, records: Iterable[MilIntRecord]) -> None:
        self._records.extend(records)

    def clear(self) -> None:
        self._records.clear()

    @property
    def size(self) -> int:
        return len(self._records)

    async def search(self, request: SearchRequest, *, index: str) -> SearchResponse:
        del index  # in-memory backend ignores the index name

        tokens = [t.strip().lower() for t in request.q.split() if t.strip()]
        filtered = [r for r in self._records if _matches(r, request, tokens)]
        # Stable, deterministic ordering — freshness desc, then title asc.
        filtered.sort(key=lambda r: (-r.freshness_score, r.title))

        page = max(request.page, 1)
        size = max(request.page_size, 1)
        start = (page - 1) * size
        end = start + size
        items = filtered[start:end]

        facets = _build_facets(filtered)

        return SearchResponse(
            items=items,
            total=len(filtered),
            facets=facets,
            page=page,
            page_size=size,
            next_cursor=None if end >= len(filtered) else str(end),
        )


def _matches(record: MilIntRecord, req: SearchRequest, tokens: list[str]) -> bool:
    if tokens:
        haystack = " ".join(
            [
                record.title,
                record.abstract,
                record.url,
                " ".join(record.keywords),
                record.country_org,
                record.content_type,
                record.language,
            ]
        ).lower()
        if not all(tok in haystack for tok in tokens):
            return False
    if req.tier and record.tier not in req.tier:
        return False
    if req.country and record.country_org not in req.country:
        return False
    if req.content_type and record.content_type not in req.content_type:
        return False
    if req.access_policy and record.access_policy not in req.access_policy:
        return False
    if req.language and record.language not in req.language:
        return False
    if record.freshness_score < req.min_freshness:
        return False
    return True


def _build_facets(records: list[MilIntRecord]) -> list[SearchFacet]:
    """Build facet buckets across all dimensions the API exposes."""
    if not records:
        return []
    facet_specs: list[tuple[str, callable]] = [
        ("tier", lambda r: str(r.tier)),
        ("country", lambda r: r.country_org or ""),
        ("content_type", lambda r: r.content_type),
        ("access_policy", lambda r: r.access_policy),
        ("language", lambda r: r.language),
    ]
    facets: list[SearchFacet] = []
    for name, key in facet_specs:
        counts: Counter[str] = Counter(key(r) for r in records if key(r))
        if not counts:
            continue
        values = [
            SearchFacetValue(value=value, count=count)
            for value, count in counts.most_common()
        ]
        facets.append(SearchFacet(name=name, values=values))
    return facets


__all__ = ["InMemorySearchBackend"]
