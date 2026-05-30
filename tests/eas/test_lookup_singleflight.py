"""Integration tests for the lookup single-flight lock (task 13.10 pt 2).

The single-flight lock (Design §3.7) prevents cache stampede: when N
concurrent requests miss the cache for the same indicator, exactly
ONE of them should run :meth:`LookupAssembler.assemble`; the rest
wait, then serve the payload the winner wrote.

These tests drive :class:`SingleFlightLock` directly plus a counting
:class:`LookupAssembler` so we can assert "N concurrent misses → 1
assembler call". The router layer isn't exercised here — the router
wraps the lock in a specific way (acquire → assemble → release, or
loser → wait_for_value) that we cover end-to-end in
``test_lookup.py::test_cache_miss_then_hit``. This file focuses on
the lock primitive.

Scenarios:

1. **CAS-safe release.** A client can only DEL its own lock — a stale
   release from a dead-TTL winner after a new winner took over must
   be a no-op.
2. **Stampede protection via `asyncio.gather`.** N concurrent
   `acquire` calls — only one returns ``True``.
3. **wait_for_value pickup.** A loser polls the cache key and picks
   up the winner's payload once written.
4. **wait_for_value timeout.** A loser times out cleanly when the
   winner never writes.
5. **Router-level stampede end-to-end.** Multiple concurrent requests
   to the same indicator trigger exactly one assembler invocation.

Validates: R17.1, R17.2 (cache stampede protection).
"""

from __future__ import annotations

import asyncio
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
from hydra.eas.lookup.singleflight import SingleFlightLock, cache_key
from hydra.eas.routers.lookup import router as lookup_router
from hydra.eas.routers.lookup import set_lookup_components


# ---------------------------------------------------------------------------
# FakeRedis — shared across cache + lock dialects (same as test_lookup.py)
# ---------------------------------------------------------------------------


class FakeRedis:
    """Minimal Redis double covering ``cache`` + ``singleflight`` + ``wait``.

    Optionally slows down ``get`` via an injected delay so we can
    simulate a winner that takes a measurable time to populate the
    cache — giving losers a chance to actually poll.
    """

    def __init__(self, *, get_delay_ms: int = 0) -> None:
        self._store: dict[str, bytes] = {}
        self._get_delay_seconds = get_delay_ms / 1000.0

    async def get(self, key: str) -> bytes | None:
        if self._get_delay_seconds:
            await asyncio.sleep(self._get_delay_seconds)
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
# Stub AssetRepository + PG + ES (reused from test_lookup.py shape)
# ---------------------------------------------------------------------------


class _StubAssetRepository(AssetRepository):
    def __init__(self) -> None:
        pass

    async def get_active_by_key(  # type: ignore[override]
        self, tenant_id: UUID, asset_type: str, normalized_value: str
    ) -> Asset | None:
        return None


class _FakePgConn:
    async def fetch(self, sql: str, *params: Any) -> list[dict[str, Any]]:
        return []

    async def fetchrow(self, sql: str, *params: Any) -> dict[str, Any]:
        return {"first_seen": None, "last_seen": None}


class _FakePgPool:
    def acquire(self) -> "_FakePgPool":
        return self

    async def __aenter__(self) -> _FakePgConn:
        return _FakePgConn()

    async def __aexit__(self, *exc: Any) -> None:
        return None


class _FakeES:
    async def search(self, *, index: str, **kwargs: Any) -> dict[str, Any]:
        return {"hits": {"hits": []}}


class _CountingSlowAssembler(LookupAssembler):
    """Assembler that sleeps for ``assemble_delay_ms`` to simulate real work.

    The delay gives concurrent requests a realistic chance to pile up
    on the single-flight lock. We count every ``assemble`` invocation
    so the tests can assert it fired exactly once (or N times for the
    non-stampede control).
    """

    def __init__(self, *a: Any, assemble_delay_ms: int = 0, **kw: Any) -> None:
        super().__init__(*a, **kw)
        self.assemble_calls = 0
        self._delay_seconds = assemble_delay_ms / 1000.0

    async def assemble(self, cls, normalized_value, tenant_id):  # type: ignore[override]
        self.assemble_calls += 1
        if self._delay_seconds:
            await asyncio.sleep(self._delay_seconds)
        return await super().assemble(cls, normalized_value, tenant_id)


# ---------------------------------------------------------------------------
# CAS-safe release
# ---------------------------------------------------------------------------


async def test_release_is_cas_safe_on_ttl_rollover() -> None:
    """A stale release must NOT clear a newer winner's lock.

    Sequence simulating the pathological case:

    1. Client A acquires the lock with id ``"req-A"``.
    2. A's TTL elapses; the lock auto-clears.
    3. Client B acquires the lock with id ``"req-B"``.
    4. A (zombied) now tries to release — it passes its own id, not B's.
    5. The CAS check sees the current value is ``"req-B"`` (not ``"req-A"``)
       and refuses to DEL. B's lock survives.

    Validates: R17.2 (stampede protection under TTL rollover).
    """

    redis = FakeRedis()
    lock = SingleFlightLock(redis)
    key = "hydra:eas:lookup:sf:ipv4:10.0.0.1"

    # (1) A acquires.
    assert await lock.acquire("ipv4", "10.0.0.1", "req-A") is True
    # (2) Simulate TTL clear.
    await redis.delete(key)
    # (3) B acquires.
    assert await lock.acquire("ipv4", "10.0.0.1", "req-B") is True
    # (4) Zombie A tries to release.
    await lock.release("ipv4", "10.0.0.1", "req-A")

    # (5) B's lock survives — a fresh acquire by some third client C
    # would fail because B still holds it.
    third = await lock.acquire("ipv4", "10.0.0.1", "req-C")
    assert third is False


# ---------------------------------------------------------------------------
# Concurrent acquire — exactly one winner
# ---------------------------------------------------------------------------


async def test_concurrent_acquire_yields_single_winner() -> None:
    """Under ``asyncio.gather``, exactly one of N acquire calls returns True.

    This is the core invariant of the single-flight lock — N cache
    misses that race each other get exactly one winner.

    Validates: R17.2.
    """

    redis = FakeRedis()
    lock = SingleFlightLock(redis)

    # 10 concurrent acquire attempts.
    results = await asyncio.gather(
        *[
            lock.acquire("ipv4", "10.0.0.1", f"req-{i}")
            for i in range(10)
        ]
    )

    winners = [r for r in results if r is True]
    assert len(winners) == 1, f"expected exactly 1 winner, got {len(winners)}"


# ---------------------------------------------------------------------------
# wait_for_value — pickup after winner writes
# ---------------------------------------------------------------------------


async def test_wait_for_value_returns_winner_payload() -> None:
    """A loser polling ``wait_for_value`` picks up the payload once written.

    Simulation:

    1. Winner acquires the lock.
    2. In parallel, a loser polls ``wait_for_value``.
    3. After a short delay, the winner writes the cache.
    4. The loser's poll returns the exact bytes the winner wrote.
    """

    redis = FakeRedis()
    lock = SingleFlightLock(redis)

    # Winner acquires immediately.
    assert await lock.acquire("ipv4", "10.0.0.1", "winner") is True

    key = cache_key("ipv4", "10.0.0.1")
    payload = b"winner-payload-bytes"

    # Schedule the cache write for 100 ms from now.
    async def _write_after_delay() -> None:
        await asyncio.sleep(0.1)
        await redis.setex(key, 300, payload)

    # Loser and writer run concurrently.
    write_task = asyncio.create_task(_write_after_delay())
    waited = await lock.wait_for_value(
        key, poll_interval_ms=20, timeout_ms=1000
    )
    await write_task

    assert waited == payload


async def test_wait_for_value_times_out_when_winner_silent() -> None:
    """A loser times out cleanly when the winner never writes.

    Returns ``None`` after ``timeout_ms`` so the router can fall
    through to the uncached assembly path (Design §3.7 safety valve).
    """

    redis = FakeRedis()
    lock = SingleFlightLock(redis)
    key = cache_key("ipv4", "10.0.0.1")

    # No winner writes — expect timeout.
    waited = await lock.wait_for_value(
        key, poll_interval_ms=20, timeout_ms=100
    )
    assert waited is None


async def test_wait_for_value_returns_quickly_when_already_cached() -> None:
    """If the value is already in the cache, the loser returns on the first poll.

    Simulates the race where the winner completes before the loser
    starts polling — the loser's very first ``GET`` succeeds.
    """

    redis = FakeRedis()
    lock = SingleFlightLock(redis)
    key = cache_key("ipv4", "10.0.0.2")
    payload = b"already-there"

    # Pre-populate as if the winner finished before we started polling.
    await redis.setex(key, 300, payload)

    loop = asyncio.get_event_loop()
    t0 = loop.time()
    waited = await lock.wait_for_value(
        key, poll_interval_ms=50, timeout_ms=2000
    )
    elapsed = loop.time() - t0

    assert waited == payload
    # Should return in well under 100 ms since the first poll already
    # found the value.
    assert elapsed < 0.1


# ---------------------------------------------------------------------------
# Router-level stampede: N concurrent requests → 1 assembler call
# ---------------------------------------------------------------------------


_API_KEY = "test-api-key-13-10-sf"


def _register_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(HydraAPIException, hydra_exception_handler)


@pytest.fixture
def stampede_app(
    request: pytest.FixtureRequest,
) -> tuple[FastAPI, _CountingSlowAssembler, FakeRedis]:
    """App fixture with a deliberately slow assembler.

    The slow path guarantees that N concurrent router calls all
    observe the cache as empty, so the single-flight lock is the
    only mechanism preventing N assembler invocations.
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

    # 150 ms delay is long enough that a concurrent batch of requests
    # will pile up on the lock. We pass ``singleflight_wait_timeout_ms``
    # well above the assemble delay so losers have time to wait.
    assembler = _CountingSlowAssembler(
        _FakePgPool(),
        _FakeES(),
        _StubAssetRepository(),
        assemble_delay_ms=150,
    )

    set_lookup_components(
        cache=cache,
        singleflight=sf_lock,
        assembler=assembler,
        singleflight_wait_timeout_ms=5_000,
    )

    app = FastAPI()
    _register_error_handlers(app)
    app.include_router(lookup_router)

    def _cleanup() -> None:
        set_api_key_store({})

    request.addfinalizer(_cleanup)

    return app, assembler, redis


def test_router_stampede_protection_single_assembly_under_concurrency(
    stampede_app: tuple[FastAPI, _CountingSlowAssembler, FakeRedis],
) -> None:
    """Concurrent misses for the same indicator invoke the assembler ONCE.

    We cannot use ``TestClient`` concurrency directly (FastAPI's sync
    TestClient runs requests serially in a thread pool). Instead we
    drive the app via :class:`httpx.AsyncClient` against an ASGI
    transport so the requests genuinely race.

    Validates: R17.1, R17.2 (stampede protection end-to-end).
    """

    import httpx

    app, assembler, _ = stampede_app

    async def _hit() -> int:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            resp = await client.get(
                "/api/v1/lookup/10.0.0.1",
                headers={"X-API-Key": _API_KEY},
            )
            return resp.status_code

    async def _run() -> list[int]:
        # 5 concurrent requests.
        return await asyncio.gather(*[_hit() for _ in range(5)])

    statuses = asyncio.run(_run())

    # Every caller got a successful response.
    assert all(s == 200 for s in statuses), f"statuses = {statuses}"

    # The assembler ran exactly ONCE — all 5 concurrent misses piled
    # up on the single-flight lock; the four losers waited and picked
    # up the cached payload the winner wrote.
    assert assembler.assemble_calls == 1, (
        f"expected 1 assembler call, got {assembler.assemble_calls}"
    )


def test_sequential_calls_still_hit_cache(
    stampede_app: tuple[FastAPI, _CountingSlowAssembler, FakeRedis],
) -> None:
    """Baseline: two sequential calls exercise the plain miss → hit path.

    Sanity check for the stampede test — ensures the fixture's slow
    assembler + FakeRedis wiring still supports the basic miss + hit
    flow (so a stampede-protection pass in the preceding test is
    meaningful).
    """

    app, assembler, _ = stampede_app

    with TestClient(app) as client:
        r1 = client.get(
            "/api/v1/lookup/10.0.0.1", headers={"X-API-Key": _API_KEY}
        )
        r2 = client.get(
            "/api/v1/lookup/10.0.0.1", headers={"X-API-Key": _API_KEY}
        )

    assert r1.status_code == 200
    assert r2.status_code == 200
    assert r1.json()["meta"]["cache"] == "miss"
    assert r2.json()["meta"]["cache"] == "hit"
    assert assembler.assemble_calls == 1
