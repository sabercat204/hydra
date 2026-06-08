"""Tests for the mil_int search backends.

Covers the in-memory backend (algorithmic correctness across filters,
faceting, pagination) and the Elasticsearch backend (query construction,
response decoding) using a stub ES client.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from hydra.mil_int.dependencies import set_mil_int_components
from hydra.mil_int.schemas.record import MilIntRecord
from hydra.mil_int.schemas.search import SearchRequest
from hydra.mil_int.search.elasticsearch import (
    ElasticsearchSearchBackend,
    _build_query,
)
from hydra.mil_int.search.memory import InMemorySearchBackend
from hydra.mil_int.setup import mount_mil_int_routers
from hydra.mil_int.xref.resolver import XrefResolver


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _record(
    *,
    source_id: str,
    tier: int,
    title: str,
    country: str = "US",
    content_type: str = "cybersecurity_frameworks",
    access_policy: str = "open",
    language: str = "en",
    keywords: list[str] | None = None,
    abstract: str = "",
    freshness: float = 1.0,
) -> MilIntRecord:
    return MilIntRecord(
        source_id=source_id,
        tier=tier,
        country_org=country,
        title=title,
        url=f"https://example.com/{source_id}.pdf",
        content_type=content_type,
        access_policy=access_policy,  # type: ignore[arg-type]
        ingestion_timestamp=datetime(2026, 6, 1, tzinfo=timezone.utc),
        content_hash="0" * 16,
        abstract=abstract,
        keywords=keywords or [],
        freshness_score=freshness,
        language=language,
    )


@pytest.fixture
def corpus() -> list[MilIntRecord]:
    return [
        _record(
            source_id="nist-sp-800-53",
            tier=100,
            title="NIST SP 800-53 Rev 5",
            keywords=["controls", "rmf"],
            abstract="Security and privacy controls for information systems.",
            freshness=0.95,
        ),
        _record(
            source_id="nist-sp-800-171",
            tier=100,
            title="NIST SP 800-171",
            keywords=["cui", "controls"],
            abstract="Protecting controlled unclassified information.",
            freshness=0.90,
        ),
        _record(
            source_id="dstl-report-1",
            tier=101,
            title="DSTL Aerospace Composites Report",
            country="UK",
            content_type="research_reports",
            keywords=["composites", "aerospace"],
            freshness=0.6,
        ),
        _record(
            source_id="foi-arctic-doctrine",
            tier=103,
            title="FOI Arctic Defense Doctrine",
            country="SE",
            content_type="strategic_policy",
            language="en",
            keywords=["arctic", "doctrine"],
            freshness=0.4,
        ),
        _record(
            source_id="kjda-paper",
            tier=104,
            title="KJDA Strategic Assessment 2025",
            country="KR",
            content_type="strategic_policy",
            access_policy="subscription",
            keywords=["strategic"],
            freshness=0.3,
        ),
    ]


@pytest.fixture
def memory_backend(corpus: list[MilIntRecord]) -> InMemorySearchBackend:
    return InMemorySearchBackend(corpus)


# ---------------------------------------------------------------------------
# In-memory backend
# ---------------------------------------------------------------------------


class TestInMemoryFiltering:
    @pytest.mark.asyncio
    async def test_full_text_token_match(self, memory_backend: InMemorySearchBackend):
        resp = await memory_backend.search(
            SearchRequest(q="cryptographic"), index="any"
        )
        # No record contains "cryptographic" — empty result, but facets
        # should also be empty and total = 0.
        assert resp.total == 0
        assert resp.items == []

    @pytest.mark.asyncio
    async def test_token_match_finds_substring(
        self, memory_backend: InMemorySearchBackend
    ):
        resp = await memory_backend.search(
            SearchRequest(q="controls"), index="any"
        )
        titles = {r.title for r in resp.items}
        assert "NIST SP 800-53 Rev 5" in titles
        assert "NIST SP 800-171" in titles
        assert resp.total == 2

    @pytest.mark.asyncio
    async def test_tier_filter(self, memory_backend: InMemorySearchBackend):
        resp = await memory_backend.search(
            SearchRequest(q="", tier=[100]), index="any"
        )
        assert resp.total == 2
        assert all(r.tier == 100 for r in resp.items)

    @pytest.mark.asyncio
    async def test_country_filter(self, memory_backend: InMemorySearchBackend):
        resp = await memory_backend.search(
            SearchRequest(q="", country=["UK"]), index="any"
        )
        assert resp.total == 1
        assert resp.items[0].country_org == "UK"

    @pytest.mark.asyncio
    async def test_access_policy_filter(
        self, memory_backend: InMemorySearchBackend
    ):
        resp = await memory_backend.search(
            SearchRequest(q="", access_policy=["subscription"]), index="any"
        )
        assert resp.total == 1
        assert resp.items[0].source_id == "kjda-paper"

    @pytest.mark.asyncio
    async def test_content_type_filter(
        self, memory_backend: InMemorySearchBackend
    ):
        resp = await memory_backend.search(
            SearchRequest(q="", content_type=["strategic_policy"]), index="any"
        )
        types = {r.content_type for r in resp.items}
        assert types == {"strategic_policy"}
        assert resp.total == 2

    @pytest.mark.asyncio
    async def test_min_freshness_filter(
        self, memory_backend: InMemorySearchBackend
    ):
        resp = await memory_backend.search(
            SearchRequest(q="", min_freshness=0.7), index="any"
        )
        assert resp.total == 2
        assert all(r.freshness_score >= 0.7 for r in resp.items)

    @pytest.mark.asyncio
    async def test_combined_filters(self, memory_backend: InMemorySearchBackend):
        resp = await memory_backend.search(
            SearchRequest(q="controls", tier=[100], min_freshness=0.91),
            index="any",
        )
        assert resp.total == 1
        assert resp.items[0].source_id == "nist-sp-800-53"


class TestInMemoryFacets:
    @pytest.mark.asyncio
    async def test_facets_cover_every_dimension(
        self, memory_backend: InMemorySearchBackend
    ):
        resp = await memory_backend.search(SearchRequest(q=""), index="any")
        names = {f.name for f in resp.facets}
        assert names == {"tier", "country", "content_type", "access_policy", "language"}

    @pytest.mark.asyncio
    async def test_facet_counts_track_filtered_set(
        self, memory_backend: InMemorySearchBackend
    ):
        resp = await memory_backend.search(
            SearchRequest(q="", tier=[100]), index="any"
        )
        access_facet = next((f for f in resp.facets if f.name == "access_policy"), None)
        assert access_facet is not None
        # All Tier 100 records are open in the fixture.
        assert access_facet.values[0].value == "open"
        assert access_facet.values[0].count == 2


class TestInMemoryPagination:
    @pytest.mark.asyncio
    async def test_page_size_caps_results(
        self, memory_backend: InMemorySearchBackend
    ):
        resp = await memory_backend.search(
            SearchRequest(q="", page=1, page_size=2), index="any"
        )
        assert len(resp.items) == 2
        assert resp.total == 5

    @pytest.mark.asyncio
    async def test_pagination_wraps(self, memory_backend: InMemorySearchBackend):
        resp1 = await memory_backend.search(
            SearchRequest(q="", page=1, page_size=2), index="any"
        )
        resp2 = await memory_backend.search(
            SearchRequest(q="", page=2, page_size=2), index="any"
        )
        ids1 = {r.source_id for r in resp1.items}
        ids2 = {r.source_id for r in resp2.items}
        assert ids1.isdisjoint(ids2)

    @pytest.mark.asyncio
    async def test_next_cursor_when_more_results(
        self, memory_backend: InMemorySearchBackend
    ):
        resp = await memory_backend.search(
            SearchRequest(q="", page=1, page_size=2), index="any"
        )
        assert resp.next_cursor is not None

    @pytest.mark.asyncio
    async def test_next_cursor_none_at_end(
        self, memory_backend: InMemorySearchBackend
    ):
        resp = await memory_backend.search(
            SearchRequest(q="", page=3, page_size=2), index="any"
        )
        assert resp.next_cursor is None


# ---------------------------------------------------------------------------
# Elasticsearch query construction
# ---------------------------------------------------------------------------


class TestEsQueryBuilder:
    def test_match_all_when_q_empty(self):
        body = _build_query(SearchRequest(q=""))
        must = body["query"]["bool"]["must"]
        assert must == [{"match_all": {}}]

    def test_multi_match_when_q_present(self):
        body = _build_query(SearchRequest(q="cryptographic module"))
        must = body["query"]["bool"]["must"]
        assert must[0]["multi_match"]["query"] == "cryptographic module"
        assert "payload.title^3" in must[0]["multi_match"]["fields"]

    def test_filters_compose(self):
        req = SearchRequest(
            q="",
            tier=[100, 101],
            country=["US"],
            content_type=["cybersecurity_frameworks"],
            access_policy=["open"],
            language=["en"],
            min_freshness=0.5,
        )
        body = _build_query(req)
        filters = body["query"]["bool"]["filter"]
        # Six filters: tier, country, content_type, access_policy, language, min_freshness.
        assert len(filters) == 6

    def test_pagination_translates_to_from_size(self):
        body = _build_query(SearchRequest(q="", page=3, page_size=20))
        assert body["from"] == 40
        assert body["size"] == 20

    def test_aggs_cover_facet_dimensions(self):
        body = _build_query(SearchRequest(q=""))
        assert set(body["aggs"].keys()) == {
            "tier",
            "country",
            "content_type",
            "access_policy",
            "language",
        }


# ---------------------------------------------------------------------------
# Elasticsearch response decoding
# ---------------------------------------------------------------------------


def _es_response(records: list[MilIntRecord]) -> dict[str, Any]:
    """Build a fake ES response from a list of records."""
    return {
        "hits": {
            "total": {"value": len(records), "relation": "eq"},
            "hits": [
                {
                    "_id": r.content_hash,
                    "_source": {
                        "source_name": r.source_id,
                        "stream_id": r.source_id,
                        "tier": r.tier,
                        "raw_hash": r.content_hash,
                        "ingested_at": r.ingestion_timestamp.isoformat(),
                        "timestamp": r.ingestion_timestamp.isoformat(),
                        "source_url": r.url,
                        "payload": {
                            "title": r.title,
                            "doc_url": r.url,
                            "country": r.country_org,
                            "content_type": r.content_type,
                            "access_policy": r.access_policy,
                            "abstract": r.abstract,
                            "keywords": r.keywords,
                            "freshness_score": r.freshness_score,
                            "language": r.language,
                        },
                    },
                }
                for r in records
            ],
        },
        "aggregations": {
            "tier": {"buckets": [{"key": r.tier, "doc_count": 1} for r in records]},
            "country": {
                "buckets": [{"key": r.country_org, "doc_count": 1} for r in records]
            },
            "content_type": {
                "buckets": [{"key": r.content_type, "doc_count": 1} for r in records]
            },
            "access_policy": {
                "buckets": [{"key": r.access_policy, "doc_count": 1} for r in records]
            },
            "language": {
                "buckets": [{"key": r.language, "doc_count": 1} for r in records]
            },
        },
    }


class TestEsResponseDecoding:
    @pytest.mark.asyncio
    async def test_hits_decoded_to_records(self, corpus: list[MilIntRecord]):
        client = AsyncMock()
        client.search.return_value = _es_response(corpus[:2])
        backend = ElasticsearchSearchBackend(client)

        resp = await backend.search(
            SearchRequest(q="controls"), index="hydra-mil-int-records"
        )
        assert resp.total == 2
        ids = {r.source_id for r in resp.items}
        assert ids == {"nist-sp-800-53", "nist-sp-800-171"}

    @pytest.mark.asyncio
    async def test_aggregations_decoded_to_facets(
        self, corpus: list[MilIntRecord]
    ):
        client = AsyncMock()
        client.search.return_value = _es_response(corpus[:3])
        backend = ElasticsearchSearchBackend(client)

        resp = await backend.search(SearchRequest(q=""), index="any")
        names = {f.name for f in resp.facets}
        assert names == {"tier", "country", "content_type", "access_policy", "language"}

    @pytest.mark.asyncio
    async def test_es_error_returns_empty_response(self):
        client = AsyncMock()
        client.search.side_effect = RuntimeError("boom")
        backend = ElasticsearchSearchBackend(client)

        resp = await backend.search(SearchRequest(q=""), index="any")
        assert resp.total == 0
        assert resp.items == []


# ---------------------------------------------------------------------------
# /api/v1/mil-int/search end-to-end with the in-memory backend
# ---------------------------------------------------------------------------


@pytest.fixture
def client(corpus: list[MilIntRecord]) -> TestClient:
    app = FastAPI()
    mount_mil_int_routers(app)
    set_mil_int_components(
        reset=True,
        xref_resolver=XrefResolver.from_path("config/mil_int_xref.yaml"),
        search_backend=InMemorySearchBackend(corpus),
    )
    return TestClient(app)


class TestSearchEndpointWithInMemoryBackend:
    def test_returns_200_when_backend_wired(self, client: TestClient):
        resp = client.post("/api/v1/mil-int/search", json={"q": ""})
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 5
        assert len(body["items"]) <= body["page_size"]

    def test_q_token_match(self, client: TestClient):
        resp = client.post(
            "/api/v1/mil-int/search", json={"q": "arctic"}
        )
        assert resp.status_code == 200
        items = resp.json()["items"]
        assert len(items) == 1
        assert "Arctic" in items[0]["title"]

    def test_filters_apply(self, client: TestClient):
        resp = client.post(
            "/api/v1/mil-int/search",
            json={"q": "", "tier": [100], "access_policy": ["open"]},
        )
        body = resp.json()
        assert body["total"] == 2
        assert all(item["tier"] == 100 for item in body["items"])
