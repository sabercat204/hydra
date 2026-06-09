"""Elasticsearch search backend for the mil_int surface.

Translates :class:`SearchRequest` into ES query DSL:

- ``q`` becomes a ``multi_match`` over title / abstract / payload text.
- Each filter dimension (tier, country, content_type, access_policy,
  language) becomes a ``terms`` filter.
- ``min_freshness`` becomes a ``range`` filter on
  ``payload.freshness_score`` (defaulting to 1.0 when missing — a record
  with no recorded freshness is assumed fresh).
- Faceting is done via ``terms`` aggregations on the same dimensions so
  the UI can render counts that update with the active filter set.

The backend is index-agnostic — :func:`hydra.mil_int.dependencies.get_search_backend`
hands it the configured index name (typically ``hydra-mil-int-records``),
and the same backend can search the whole mil_int corpus by pointing at
the ``hydra-mil-int-*`` index pattern.
"""

from __future__ import annotations

import logging
from typing import Any

from sloptropy_common import AccessPolicy

from hydra.mil_int.schemas.record import MilIntRecord
from hydra.mil_int.schemas.search import (
    SearchFacet,
    SearchFacetValue,
    SearchRequest,
    SearchResponse,
)

logger = logging.getLogger(__name__)


_FACET_FIELDS: dict[str, str] = {
    # Logical name → ES doc field. The doc field is what mil_int records
    # land at when the storage router writes them via the Elasticsearch
    # engine's `_serialize_record` (see hydra/storage/engines/elasticsearch.py).
    # Adding a `.keyword` suffix on text fields makes the aggregation
    # match the value exactly without analyzer tokenisation.
    "tier": "tier",
    "country": "payload.country.keyword",
    "content_type": "payload.content_type.keyword",
    "access_policy": "payload.access_policy.keyword",
    "language": "payload.language.keyword",
}


class ElasticsearchSearchBackend:
    """Production search backend backed by an AsyncElasticsearch client."""

    def __init__(self, client: Any) -> None:
        self._client = client

    async def search(self, request: SearchRequest, *, index: str) -> SearchResponse:
        body = _build_query(request)
        try:
            resp = await self._client.search(index=index, body=body)
        except Exception as exc:  # noqa: BLE001 — surface as 0 hits, log
            logger.warning(
                "mil_int.es_search_error",
                extra={"index": index, "error": str(exc)},
            )
            return SearchResponse(
                items=[],
                total=0,
                facets=[],
                page=request.page,
                page_size=request.page_size,
            )

        hits = resp.get("hits", {})
        total = _extract_total(hits)
        items = [_hit_to_record(h) for h in hits.get("hits", [])]
        items = [r for r in items if r is not None]
        facets = _aggregations_to_facets(resp.get("aggregations") or {})

        end = request.page * request.page_size
        next_cursor = str(end) if end < total else None

        return SearchResponse(
            items=items,
            total=total,
            facets=facets,
            page=request.page,
            page_size=request.page_size,
            next_cursor=next_cursor,
        )


# ---------------------------------------------------------------------------
# Query construction
# ---------------------------------------------------------------------------


def _build_query(request: SearchRequest) -> dict[str, Any]:
    must: list[dict[str, Any]] = []
    filters: list[dict[str, Any]] = []

    if request.q.strip():
        must.append(
            {
                "multi_match": {
                    "query": request.q.strip(),
                    "fields": [
                        "payload.title^3",
                        "payload.abstract^2",
                        "payload.keywords",
                        "payload.notes",
                        "tags",
                    ],
                    "operator": "and",
                    "type": "best_fields",
                }
            }
        )
    else:
        must.append({"match_all": {}})

    if request.tier:
        filters.append({"terms": {"tier": [int(t) for t in request.tier]}})
    if request.country:
        filters.append({"terms": {"payload.country.keyword": list(request.country)}})
    if request.content_type:
        filters.append(
            {"terms": {"payload.content_type.keyword": list(request.content_type)}}
        )
    if request.access_policy:
        filters.append(
            {"terms": {"payload.access_policy.keyword": list(request.access_policy)}}
        )
    if request.language:
        filters.append(
            {"terms": {"payload.language.keyword": list(request.language)}}
        )
    if request.min_freshness > 0:
        filters.append(
            {
                "range": {
                    "payload.freshness_score": {
                        "gte": request.min_freshness,
                    }
                }
            }
        )

    page = max(request.page, 1)
    size = max(request.page_size, 1)
    return {
        "from": (page - 1) * size,
        "size": size,
        "query": {"bool": {"must": must, "filter": filters}},
        "sort": [
            {"payload.freshness_score": {"order": "desc", "missing": "_last"}},
            "_score",
        ],
        "aggs": {
            facet_name: {"terms": {"field": field, "size": 50}}
            for facet_name, field in _FACET_FIELDS.items()
        },
    }


# ---------------------------------------------------------------------------
# Response decoding
# ---------------------------------------------------------------------------


def _extract_total(hits: dict[str, Any]) -> int:
    total = hits.get("total")
    if isinstance(total, dict):
        return int(total.get("value", 0))
    if isinstance(total, int):
        return total
    return 0


def _hit_to_record(hit: dict[str, Any]) -> MilIntRecord | None:
    src = hit.get("_source") or {}
    payload = src.get("payload") or {}
    try:
        return MilIntRecord(
            source_id=src.get("source_name", "") or src.get("stream_id", ""),
            tier=int(src.get("tier", 100)),
            country_org=str(payload.get("country", "")),
            title=str(payload.get("title", "")),
            url=str(payload.get("doc_url") or src.get("source_url", "")),
            content_type=str(payload.get("content_type", "research_reports")),
            access_policy=AccessPolicy(
                payload.get("access_policy", AccessPolicy.OPEN.value)
            ),
            ingestion_timestamp=src.get("ingested_at") or src.get("timestamp"),
            content_hash=str(src.get("raw_hash", hit.get("_id", ""))),
            abstract=str(payload.get("abstract", "")),
            keywords=list(payload.get("keywords", []) or []),
            geospatial_relevance=bool(payload.get("geospatial_relevance", False)),
            freshness_score=float(payload.get("freshness_score", 1.0)),
            language=str(payload.get("language", "en")),
        )
    except Exception as exc:  # noqa: BLE001 — skip malformed docs
        logger.warning(
            "mil_int.es_hit_decode_failed",
            extra={"id": hit.get("_id"), "error": str(exc)},
        )
        return None


def _aggregations_to_facets(aggs: dict[str, Any]) -> list[SearchFacet]:
    facets: list[SearchFacet] = []
    for name in _FACET_FIELDS.keys():
        bucket = aggs.get(name) or {}
        buckets = bucket.get("buckets") or []
        if not buckets:
            continue
        values = [
            SearchFacetValue(value=str(b.get("key", "")), count=int(b.get("doc_count", 0)))
            for b in buckets
        ]
        facets.append(SearchFacet(name=name, values=values))
    return facets


__all__ = ["ElasticsearchSearchBackend"]
