"""Integration tests for ``GET /api/v1/lookup/{indicator}`` (task 13.10 pt 1).

End-to-end coverage of the lookup router's happy path, 422 classification
failure, and cache-hit/miss meta stamping. Runs against a FastAPI
:class:`TestClient` with the router's lookup components wired to
in-memory fakes so the whole flow (classify → normalize → cache →
single-flight → assemble → compose) executes without Redis / PG / ES.

Scenarios:

1. **Happy path — cache miss then hit.** First request → ``meta.cache
   = "miss"`` and the assembler runs. Second request → ``meta.cache =
   "hit"`` and the assembler is NOT re-invoked (proves the cache is in
   the hot path).
2. **422 INDICATOR_NOT_CLASSIFIED.** A non-classifiable path segment
   (``"not-an-indicator"``) short-circuits before any storage access.
3. **Per-tenant asset_reference.** Same indicator, two tenants: only
   the owner sees ``asset_reference`` populated; cache body is
   identical otherwise (R17.5).
4. **Service unavailable.** When the router's components aren't
   wired, the endpoint returns 503.

Validates: R16.1, R17.1, R17.2, R17.5.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from hydra.api.dependencies import APIKeyRecord, set_api_key_store
from hydra.api.errors import HydraAPIException, hydra_exception_handler
from hydra.eas.assets.models import Asset
from hydra.eas.assets.repository import AssetRepository
from hydra.eas.lookup.assembler import LookupAssembler
from hydra.eas.lookup.cache import IndicatorLookupCache
from hydra.eas.lookup.singleflight import SingleFlightLock
from hydra.eas.routers.lookup import router as lookup_router
from hydra.eas.routers.lookup import set_lookup_components


# ---------------------------------------------------------------------------
# FakeRedis supporting cache + singleflight dialects
# ---------------------------------------------------------------------------


class FakeRedis:
    """Minimal async Redis double covering the union of cache + lock calls.

    Supports ``get``, ``set(..., ex=..., nx=...)``, ``setex``,
    ``delete``, and ``dbsize``. Enough for :class:`IndicatorLookupCache`
    and :class:`SingleFlightLock` combined.
    """

    def __init__(self) -> None:
        self._store: dict[str, bytes] = {}

    async def get(self, key: str) -> bytes | None:
        return self._store.get(key)

    async def set(
        self,
        key: str,
        value: Any,
        *,
        ex: int | None = None,
        nx: bool = False,
    ) -> bool:
        if nx and key in self._store:
            return False
        self._store[key] = value if isinstance(value, bytes) else str(value).encode()
        return True

    async def setex(self, key: str, ttl: int, value: Any) -> None:
        self._store[key] = value if isinstance(value, bytes) else str(value).encode()

    async def delete(self, key: str) -> int:
        existed = key in self._store
        self._store.pop(key, None)
        return 1 if existed else 0

    async def dbsize(self) -> int:
        return len(self._store)


# ---------------------------------------------------------------------------
# Stub AssetRepository — returns an Asset iff the tenant owns it
# ---------------------------------------------------------------------------


class _StubAssetRepository(AssetRepository):
    """Minimal repo surface exercising tenant-scoped asset lookup."""

    def __init__(self, ownership: dict[UUID, set[tuple[str, str]]]) -> None:
        self._ownership = ownership

    async def get_active_by_key(  # type: ignore[override]
        self,
        tenant_id: UUID,
        asset_type: str,
        normalized_value: str,
    ) -> Asset | None:
        if (asset_type, normalized_value) not in self._ownership.get(tenant_id, set()):
            return None
        return Asset(
            asset_id=uuid4(),
            tenant_id=tenant_id,
            asset_type=asset_type,
            normalized_value=normalized_value,
            raw_value=normalized_value,
            is_active=True,
            capture_screenshots=False,
            created_at=datetime.now(timezone.utc),
            deactivated_at=None,
            notes=None,
        )


# ---------------------------------------------------------------------------
# FakePgPool + FakeES for the assembler
# ---------------------------------------------------------------------------


class _FakePgConn:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    async def fetch(self, sql: str, *params: Any) -> list[dict[str, Any]]:
        # ``_fetch_records_and_tags`` is the only caller of ``fetch`` for
        # the assembler; we return the seeded rows unfiltered (the
        # assembler trims by LIMIT inside the repo, but our tiny
        # fixtures always fit).
        return self.rows

    async def fetchrow(self, sql: str, *params: Any) -> dict[str, Any]:
        if not self.rows:
            return {"first_seen": None, "last_seen": None}
        ts = [r["timestamp"] for r in self.rows]
        return {"first_seen": min(ts), "last_seen": max(ts)}


class _FakePgPool:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.rows = rows or []

    def acquire(self) -> "_FakePgPool":
        return self

    async def __aenter__(self) -> _FakePgConn:
        return _FakePgConn(self.rows)

    async def __aexit__(self, *exc: Any) -> None:
        return None


class _FakeES:
    """Tracks assembler ``search`` calls so tests can count them."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def search(self, *, index: str, **kwargs: Any) -> dict[str, Any]:
        self.calls.append({"index": index, **kwargs})
        return {"hits": {"hits": []}}


# ---------------------------------------------------------------------------
# Counting LookupAssembler subclass — lets tests assert the assembler
# fired or didn't.
# ---------------------------------------------------------------------------


class _CountingAssembler(LookupAssembler):
    def __init__(self, *a: Any, **kw: Any) -> None:
        super().__init__(*a, **kw)
        self.assemble_calls = 0

    async def assemble(self, cls, normalized_value, tenant_id):  # type: ignore[override]
        self.assemble_calls += 1
        return await super().assemble(cls, normalized_value, tenant_id)


# ---------------------------------------------------------------------------
# App builder
# ---------------------------------------------------------------------------


_API_KEY = "test-api-key-13-10"


def _register_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(HydraAPIException, hydra_exception_handler)


def _seed_record_rows(indicator: str) -> list[dict[str, Any]]:
    """One seeded ``normalized_records`` row matching ``indicator``."""

    return [
        {
            "raw_hash": "0123456789abcdef",
            "tier": 16,
            "stream_id": "cyber-feed-1",
            "timestamp": datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            "confidence": 0.7,
            "tags": ["threat"],
            "_payload_text": f'{{"ip":"{indicator}"}}',
        }
    ]


@pytest.fixture
def lookup_app(
    request: pytest.FixtureRequest,
) -> tuple[FastAPI, UUID, _CountingAssembler, FakeRedis]:
    """Build a FastAPI app with the lookup router wired to in-memory fakes.

    Returns ``(app, tenant_id, assembler, redis)`` so tests can drive
    the HTTP endpoint and observe the underlying assembler / cache
    state.
    """

    tenant_id = uuid4()
    key_hash = hashlib.sha256(_API_KEY.encode()).hexdigest()
    set_api_key_store(
        {
            key_hash: APIKeyRecord(
                key_id="k", name="n", scopes=["read"], tenant_id=tenant_id
            )
        }
    )

    redis = FakeRedis()
    cache = IndicatorLookupCache(redis, ttl_seconds=60)
    sf_lock = SingleFlightLock(redis)

    # Seed a PG row for ``10.0.0.1`` so the assembler's records list
    # is non-empty. The IP also lives as an ``ip``-type asset owned
    # by the caller's tenant so the asset_reference branch fires.
    pool = _FakePgPool(_seed_record_rows("10.0.0.1"))
    es = _FakeES()
    asset_repo = _StubAssetRepository({tenant_id: {("ip", "10.0.0.1")}})

    assembler = _CountingAssembler(pool, es, asset_repo)

    set_lookup_components(
        cache=cache,
        singleflight=sf_lock,
        assembler=assembler,
        singleflight_wait_timeout_ms=200,
    )

    app = FastAPI()
    _register_error_handlers(app)
    app.include_router(lookup_router)

    def _cleanup() -> None:
        set_api_key_store({})
        # Leave set_lookup_components alone; next test's fixture
        # overwrites anyway.

    request.addfinalizer(_cleanup)

    return app, tenant_id, assembler, redis


def _auth() -> dict[str, str]:
    return {"X-API-Key": _API_KEY}


# ---------------------------------------------------------------------------
# Scenario 1 — miss then hit
# ---------------------------------------------------------------------------


def test_cache_miss_then_hit(
    lookup_app: tuple[FastAPI, UUID, _CountingAssembler, FakeRedis],
) -> None:
    """First request is a miss; the second is a hit served from Redis.

    The assembler's invocation count proves the cache is in the hot
    path: miss → 1 call, hit → still 1 call (the second request never
    reaches the assembler).

    Validates: R17.1.
    """

    app, _, assembler, _ = lookup_app

    with TestClient(app) as client:
        resp_miss = client.get("/api/v1/lookup/10.0.0.1", headers=_auth())
        resp_hit = client.get("/api/v1/lookup/10.0.0.1", headers=_auth())

    assert resp_miss.status_code == 200
    assert resp_hit.status_code == 200
    assert resp_miss.json()["meta"]["cache"] == "miss"
    assert resp_hit.json()["meta"]["cache"] == "hit"

    # Assembler ran exactly once — the hit didn't re-trigger it.
    assert assembler.assemble_calls == 1


def test_cache_hit_body_carries_populated_fields(
    lookup_app: tuple[FastAPI, UUID, _CountingAssembler, FakeRedis],
) -> None:
    """Cache-hit responses include the same populated fields as misses.

    ``records`` / ``tags`` come through from PG on miss and survive
    the msgpack round-trip on hit. We pin this so a cache serializer
    regression can't silently drop fields.
    """

    app, _, _, _ = lookup_app

    with TestClient(app) as client:
        miss = client.get("/api/v1/lookup/10.0.0.1", headers=_auth())
        hit = client.get("/api/v1/lookup/10.0.0.1", headers=_auth())

    miss_body = miss.json()["data"]
    hit_body = hit.json()["data"]
    assert miss_body["records"], "expected at least one record on miss"
    # records / tags byte-equal after round-trip.
    assert miss_body["records"] == hit_body["records"]
    assert miss_body["tags"] == hit_body["tags"]


# ---------------------------------------------------------------------------
# Scenario 2 — 422 INDICATOR_NOT_CLASSIFIED
# ---------------------------------------------------------------------------


def test_unclassifiable_indicator_returns_422(
    lookup_app: tuple[FastAPI, UUID, _CountingAssembler, FakeRedis],
) -> None:
    """The classifier rejects non-indicator paths before any storage work.

    A value with embedded whitespace (not allowed in any class) is
    the cleanest way to guarantee ``classify_indicator`` returns
    ``None`` — the path parameter preserves URL-encoded whitespace
    by default.

    Validates: R16.1.
    """

    app, _, assembler, _ = lookup_app

    with TestClient(app) as client:
        # The literal ``not$an$indicator`` doesn't match any class.
        resp = client.get(
            "/api/v1/lookup/not%24an%24indicator", headers=_auth()
        )

    assert resp.status_code == 422
    body = resp.json()
    errors = body.get("errors") or []
    assert errors
    assert errors[0]["code"] == "INDICATOR_NOT_CLASSIFIED"

    # Assembler never ran — classification short-circuits before
    # storage access.
    assert assembler.assemble_calls == 0


# ---------------------------------------------------------------------------
# Scenario 3 — per-tenant asset_reference
# ---------------------------------------------------------------------------


def test_asset_reference_is_per_tenant(
    lookup_app: tuple[FastAPI, UUID, _CountingAssembler, FakeRedis],
    request: pytest.FixtureRequest,
) -> None:
    """R17.5 — asset_reference reflects the caller's ownership, not the cache.

    The fixture seeds an ``ip`` asset for tenant A. Tenant B (the
    stranger) asks the same indicator — the records / CVE / screenshot
    fields are byte-identical (R21, covered in test_lookup_tenant_isolation)
    but ``asset_reference`` must be ``None`` for B.

    We add a second API key for tenant B and drive the endpoint with
    both keys against the same running app.
    """

    app, tenant_a, _, _ = lookup_app
    tenant_b = uuid4()

    stranger_key = "stranger-api-key"
    stranger_hash = hashlib.sha256(stranger_key.encode()).hexdigest()
    owner_hash = hashlib.sha256(_API_KEY.encode()).hexdigest()

    set_api_key_store(
        {
            owner_hash: APIKeyRecord(
                key_id="owner", name="n", scopes=["read"], tenant_id=tenant_a
            ),
            stranger_hash: APIKeyRecord(
                key_id="stranger", name="s", scopes=["read"], tenant_id=tenant_b
            ),
        }
    )

    def _cleanup() -> None:
        set_api_key_store({})

    request.addfinalizer(_cleanup)

    with TestClient(app) as client:
        owner_resp = client.get(
            "/api/v1/lookup/10.0.0.1", headers={"X-API-Key": _API_KEY}
        )
        stranger_resp = client.get(
            "/api/v1/lookup/10.0.0.1", headers={"X-API-Key": stranger_key}
        )

    assert owner_resp.status_code == 200
    assert stranger_resp.status_code == 200

    owner = owner_resp.json()["data"]
    stranger = stranger_resp.json()["data"]

    # Owner sees a populated asset_reference.
    assert owner["asset_reference"] is not None
    assert owner["asset_reference"]["normalized_value"] == "10.0.0.1"
    # Stranger does not.
    assert stranger["asset_reference"] is None


# ---------------------------------------------------------------------------
# Scenario 4 — service unavailable
# ---------------------------------------------------------------------------


def test_lookup_503_when_components_unwired(
    request: pytest.FixtureRequest,
) -> None:
    """The router returns 503 when ``set_lookup_components`` hasn't populated.

    Without the fixture we have a blank slate: no cache, no
    singleflight lock, no assembler. The auth layer is still
    available, so the request reaches the router and hits the
    service-availability guard.
    """

    # Clear module-level singletons directly — ``set_lookup_components``
    # doesn't offer a "set to None" entry point by design.
    import hydra.eas.routers.lookup as lookup_mod

    lookup_mod._lookup_cache = None
    lookup_mod._lookup_singleflight = None
    lookup_mod._lookup_assembler = None

    key_hash = hashlib.sha256(_API_KEY.encode()).hexdigest()
    set_api_key_store(
        {
            key_hash: APIKeyRecord(
                key_id="k", name="n", scopes=["read"], tenant_id=uuid4()
            )
        }
    )

    def _cleanup() -> None:
        set_api_key_store({})

    request.addfinalizer(_cleanup)

    app = FastAPI()
    _register_error_handlers(app)
    app.include_router(lookup_router)

    with TestClient(app) as client:
        resp = client.get("/api/v1/lookup/10.0.0.1", headers=_auth())

    assert resp.status_code == 503
