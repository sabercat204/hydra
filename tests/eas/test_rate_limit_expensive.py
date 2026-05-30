"""Integration tests for the ``expensive`` rate-limit tier (task 15.6).

Two layers of test live side by side in this file:

1. **Pure-function classification tests** for
   :func:`hydra.api.middleware._classify_rate_tier`. The classifier is
   a plain function over ``(path, method)`` — no I/O, no state — so a
   block of direct assertions exercises Design §3.9's tier table
   exactly once per rule.

2. **Middleware end-to-end tests** that mount
   :class:`RateLimitMiddleware` onto a stub FastAPI app backed by an
   in-memory :class:`FakeRedis`. These assert that the ``expensive``
   tier's ``X-RateLimit-*`` header contract (R21.4) is honoured and
   that a tenant hitting the limit receives a 429 with ``Retry-After``.

The classification block is intentionally heavy: every row of the
Design §3.9 table corresponds to one test so a regression in the
substring/order-of-matching logic is pin-pointed by which test fails.
The middleware block is deliberately narrow — two representative
flows — because :class:`CostQuotaCounter` already owns the
exhaustively tested quota boundary (Property 23) and the middleware
only needs to prove its plumbing wires up correctly.

Validates: R21.1, R21.2, R21.4.
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from hydra.api.middleware import RateLimitMiddleware, _classify_rate_tier
from hydra.config import HydraSettings


# ---------------------------------------------------------------------------
# _classify_rate_tier — Design §3.9 table, one test per row.
# ---------------------------------------------------------------------------


def test_screenshot_post_is_expensive() -> None:
    """``POST /assets/{id}/screenshot`` is the flagship expensive tier.

    The screenshot adapter launches a Chromium tab — the most
    resource-intensive operation in EAS — so a single tenant must
    not starve the worker pool. R21.2 pins this route to the
    expensive tier.
    """
    assert _classify_rate_tier(
        "/api/v1/assets/abc-123/screenshot", "POST"
    ) == "expensive"


def test_cves_correlate_post_is_expensive() -> None:
    """``POST /cves/correlate`` kicks off a correlation run.

    Correlation jobs can span the whole fingerprint index; the
    expensive tier caps per-tenant job-creation rate.
    """
    assert _classify_rate_tier(
        "/api/v1/cves/correlate", "POST"
    ) == "expensive"


def test_observatory_generate_post_is_expensive() -> None:
    """``POST /observatory/generate`` regenerates the daily posture report.

    This triggers a full aggregation over ``normalized_records`` and
    ``asset_exposures``; expensive-tier rate limiting bounds how
    often a tenant can force a rebuild.
    """
    assert _classify_rate_tier(
        "/api/v1/observatory/generate", "POST"
    ) == "expensive"


def test_screenshot_get_is_not_expensive() -> None:
    """``GET /assets/{id}/screenshot`` is a read, not a render.

    The expensive tier only fires for ``POST``; a ``GET`` on the same
    path is a plain retrieval of the most recent capture and should
    fall through to ``read``.
    """
    assert _classify_rate_tier(
        "/api/v1/assets/abc-123/screenshot", "GET"
    ) == "read"


def test_assets_post_is_write() -> None:
    """``POST /assets`` — asset registration is a ``write`` tier call.

    The plain ``/assets`` POST creates a new asset row and is bounded
    by the cheaper ``write`` token bucket. The screenshot POST (tested
    above) wins over this via ``_EXPENSIVE_PATHS``.
    """
    assert _classify_rate_tier("/api/v1/assets", "POST") == "write"


def test_assets_post_with_id_for_screenshot_is_expensive_not_write() -> None:
    """The expensive check precedes the write check.

    Order matters: if the write check ran first, the ``/assets``
    prefix on a screenshot POST would misclassify it as ``write``.
    This test proves the priority.
    """
    assert _classify_rate_tier(
        "/api/v1/assets/xyz/screenshot", "POST"
    ) == "expensive"


def test_assets_delete_is_write() -> None:
    """``DELETE /assets/{id}`` — soft-delete is a ``write`` tier call.

    Soft-deletes mutate the ``assets`` row (``is_active=FALSE``); the
    ``write`` tier bounds how fast a tenant can churn the table.
    """
    assert _classify_rate_tier(
        "/api/v1/assets/abc-123", "DELETE"
    ) == "write"


def test_maps_features_get_is_search() -> None:
    """``GET /maps/features`` hits PostGIS; Design §3.9 calls it ``search``.

    The bbox aggregation path is more expensive than a point read but
    cheaper than a full correlation; the ``search`` tier is the
    correct bucket.
    """
    assert _classify_rate_tier("/api/v1/maps/features", "GET") == "search"


def test_trends_get_is_search() -> None:
    """``GET /trends`` aggregates Influx or Timescale windows.

    Per Design §3.9, the trends endpoint is a ``search``-tier read.
    """
    assert _classify_rate_tier("/api/v1/trends", "GET") == "search"


def test_cves_search_get_is_read() -> None:
    """``/cves/search`` is a sub-resource search, not top-level search.

    The classifier requires ``/search`` to start a path segment
    directly under ``/api/v1`` to count as the ``search`` tier.
    ``/cves/search`` is a scoped resource search — Elasticsearch
    query by vendor/product — and should fall into ``read``.
    """
    assert _classify_rate_tier("/api/v1/cves/search", "GET") == "read"


def test_images_search_get_is_read() -> None:
    """``/images/search`` — perceptual-hash nearest-neighbour lookup.

    Same rationale as ``/cves/search``: a sub-resource search is a
    ``read``, not a top-level ``search``.
    """
    assert _classify_rate_tier("/api/v1/images/search", "GET") == "read"


def test_lookup_get_is_read() -> None:
    """``GET /lookup/{indicator}`` is a cache-first point read.

    The lookup endpoint is Redis-cached on the hot path (R17.1); the
    ``read`` tier is the right bucket for its per-tenant budget.
    """
    assert _classify_rate_tier(
        "/api/v1/lookup/192.168.1.1", "GET"
    ) == "read"


def test_assets_get_is_read() -> None:
    """``GET`` variants of ``/assets`` paths all fall through to ``read``.

    List, detail, and per-asset exposures are plain PG reads — the
    EAS asset-mutation check should only fire on ``POST`` /
    ``DELETE``.
    """
    assert _classify_rate_tier("/api/v1/assets", "GET") == "read"
    assert _classify_rate_tier("/api/v1/assets/abc", "GET") == "read"
    assert _classify_rate_tier(
        "/api/v1/assets/abc/exposures", "GET"
    ) == "read"


def test_observatory_get_is_read() -> None:
    """``GET`` variants of ``/observatory`` are plain reads.

    Only the ``POST /observatory/generate`` path is ``expensive``;
    the latest report and country drill-down are retrievals and
    should classify as ``read``.
    """
    assert _classify_rate_tier(
        "/api/v1/observatory/latest", "GET"
    ) == "read"
    assert _classify_rate_tier(
        "/api/v1/observatory/countries/US", "GET"
    ) == "read"


# ---------------------------------------------------------------------------
# Expensive-tier defaults — R21.1.
# ---------------------------------------------------------------------------


def test_expensive_tier_defaults_are_2_req_per_min_burst_1() -> None:
    """Default ``expensive`` bucket is 2 req/min with a burst of 1 (R21.1).

    The default caps each tenant at two expensive-tier requests per
    minute. Burst is 1 so the tenant cannot stack allowances.
    """
    settings = HydraSettings()
    assert settings.api.rate_limit_expensive == 2
    assert settings.api.rate_limit_expensive_burst == 1


# ---------------------------------------------------------------------------
# End-to-end middleware — FakeRedis + FastAPI TestClient.
# ---------------------------------------------------------------------------


class FakeRedis:
    """A minimal in-memory Redis double for :class:`RateLimitMiddleware`.

    The middleware speaks ``get`` / ``setex`` / ``decr`` / ``ttl``.
    The fake stores plain ``int`` values and records TTLs verbatim so
    the middleware's ``X-RateLimit-Reset`` header is observable.
    """

    def __init__(self) -> None:
        self.store: dict[str, int] = {}
        self.ttls: dict[str, int] = {}

    async def get(self, key: str) -> str | None:
        return str(self.store[key]) if key in self.store else None

    async def setex(self, key: str, ttl: int, value: str) -> None:
        self.store[key] = int(value)
        self.ttls[key] = int(ttl)

    async def decr(self, key: str) -> int:
        self.store[key] = self.store.get(key, 0) - 1
        return self.store[key]

    async def ttl(self, key: str) -> int:
        return self.ttls.get(key, -1)


def _make_app(redis: FakeRedis, settings: HydraSettings) -> FastAPI:
    """Build a minimal app with just the expensive screenshot endpoint.

    The body of the endpoint is irrelevant — what matters is that
    :class:`RateLimitMiddleware` wraps it. We return a 200 so the
    first (allowed) request can be observed cleanly and the second
    (limited) request's 429 is unambiguous.
    """
    app = FastAPI()

    @app.post("/api/v1/assets/{asset_id}/screenshot")
    async def screenshot(asset_id: str) -> dict[str, str]:
        return {"status": "accepted"}

    app.add_middleware(RateLimitMiddleware, redis=redis, settings=settings)
    return app


def test_expensive_tier_rate_limit_headers_present() -> None:
    """First expensive-tier POST returns 200 with X-RateLimit-* headers (R21.4).

    With default settings ``rate_limit_expensive=2`` and
    ``rate_limit_expensive_burst=1``, the first request is inside the
    budget and must:

    * Succeed (200).
    * Carry ``X-RateLimit-Limit="2"`` — the configured rate.
    * Carry ``X-RateLimit-Remaining`` set to a non-empty value
      (equals ``"1"`` because the middleware seeds the counter at
      ``rate - 1``).
    * Carry ``X-RateLimit-Reset`` with the window's remaining TTL.
    """
    redis = FakeRedis()
    settings = HydraSettings()

    app = _make_app(redis, settings)
    client = TestClient(app)

    response = client.post("/api/v1/assets/abc-123/screenshot")

    assert response.status_code == 200
    assert response.headers.get("X-RateLimit-Limit") == "2"
    # Middleware seeds the counter at rate-1 on the first call.
    assert response.headers.get("X-RateLimit-Remaining") is not None
    assert response.headers.get("X-RateLimit-Reset") is not None


def test_expensive_tier_429_on_exhaust() -> None:
    """Second POST over a 1-request budget returns 429 with the expected headers.

    Tightening ``rate_limit_expensive`` to 1 makes the test
    deterministic in one extra request. The 429 response must:

    * Set ``Retry-After`` — the seconds to the window reset.
    * Report ``X-RateLimit-Remaining="0"`` so a client-side guard
      can suppress its next call.

    The first call consumes the only token; the second must be
    rejected by the middleware's ``count <= 0`` branch.
    """
    redis = FakeRedis()
    settings = HydraSettings()
    # Tighten to a single token so the second request is guaranteed
    # to exhaust the bucket.
    settings.api.rate_limit_expensive = 1

    app = _make_app(redis, settings)
    client = TestClient(app)

    r1 = client.post(
        "/api/v1/assets/abc/screenshot",
        headers={"x-api-key": "test-key"},
    )
    assert r1.status_code == 200

    r2 = client.post(
        "/api/v1/assets/abc/screenshot",
        headers={"x-api-key": "test-key"},
    )
    assert r2.status_code == 429
    assert r2.headers.get("Retry-After") is not None
    assert r2.headers.get("X-RateLimit-Remaining") == "0"
