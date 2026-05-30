"""Integration tests for ``GET /api/v1/jobs/{job_id}/progress`` (task 11.11).

End-to-end tests driven through FastAPI's :class:`TestClient` against
a minimally-wired app. The test stack is:

* :class:`FakeRedis` — backs :class:`JobManager` (the production Redis
  client wrapper is duck-typed, so the fake just needs ``setex`` /
  ``get``).
* An in-memory :class:`APIKeyRecord` store seeded via
  :func:`set_api_key_store`, giving us a deterministic
  ``X-API-Key`` → ``tenant_id`` mapping without touching PG.
* :func:`set_engines` to wire the real :class:`JobManager` instance
  under :func:`get_job_manager` — the jobs router picks it up via
  ``Depends(get_job_manager)``.

Scenarios (R15.1 / R15.3 / R15.4 / R15.5):

1. **200 with progress metadata** — a job with ``progress_current``
   and ``progress_total`` set returns ``progress_ratio`` +
   ``eta_seconds`` in the response.
2. **200 without progress metadata** — a freshly created job (no
   ``update_progress`` call) returns ``progress_ratio = None``.
3. **404 JOB_NOT_FOUND** — an unknown or TTL-expired job returns
   the ``JOB_NOT_FOUND`` error code with status 404.
4. **Backward compatibility** — a job created via the pre-existing
   ``/api/v1/products/jobs/{id}`` path (i.e. straight
   ``JobManager.create_job``) is readable via the new endpoint
   without modification (R15.5).
5. **Authentication** — the endpoint rejects requests without a
   valid ``X-API-Key`` header with 401.
6. **Ratio clamping** — a mismatched ``(current, total)`` that would
   naively yield ``ratio > 1`` is clamped to ``1.0`` by the router.

Validates: R15.1, R15.3, R15.4, R15.5.
"""

from __future__ import annotations

import hashlib
from typing import Any
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from hydra.api.dependencies import (
    APIKeyRecord,
    set_api_key_store,
    set_engines,
)
from hydra.api.errors import HydraAPIException, hydra_exception_handler
from hydra.api.jobs import JobManager
from hydra.eas.routers.jobs import router as jobs_router


def _register_error_handlers(app: FastAPI) -> None:
    """Wire the shared error handler so ``HydraAPIException`` maps to JSON."""

    app.add_exception_handler(HydraAPIException, hydra_exception_handler)


# ---------------------------------------------------------------------------
# FakeRedis — ``setex`` / ``get`` only
# ---------------------------------------------------------------------------


class FakeRedis:
    """Minimal async Redis double used by :class:`JobManager`.

    The manager calls ``setex(key, ttl, value)`` on every job create
    and ``get(key)`` on every read. We keep values verbatim (bytes or
    strings) so ``JobStatus.model_validate_json`` can round-trip them.
    """

    def __init__(self) -> None:
        self.store: dict[str, str] = {}

    async def setex(self, key: str, ttl: int, value: str) -> None:
        self.store[key] = value

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def delete(self, key: str) -> None:
        self.store.pop(key, None)


# ---------------------------------------------------------------------------
# App builder
# ---------------------------------------------------------------------------


_API_KEY = "test-api-key-11-11"


@pytest.fixture
def app_with_jobs(request: pytest.FixtureRequest) -> tuple[FastAPI, JobManager, FakeRedis]:
    """Build a FastAPI app mounting only the jobs router.

    Returns ``(app, jobs_manager, redis)`` so tests can seed jobs into
    Redis directly via the manager and then drive the HTTP endpoint.
    """

    redis = FakeRedis()
    manager = JobManager(redis)

    # Wire the manager as the ``get_job_manager`` singleton. Using the
    # process-wide setter keeps the test path identical to the
    # production wiring.
    set_engines(job_manager=manager)

    # Seed an in-memory API key record so ``get_current_tenant_id``
    # can resolve without a PG lookup.
    key_hash = hashlib.sha256(_API_KEY.encode()).hexdigest()
    tenant_id = uuid4()
    set_api_key_store(
        {
            key_hash: APIKeyRecord(
                key_id="test-key-id",
                name="test-key",
                scopes=["read"],
                tenant_id=tenant_id,
            )
        }
    )

    app = FastAPI()
    _register_error_handlers(app)
    app.include_router(jobs_router)

    # Reset the singletons after the test so subsequent tests start
    # clean. Uses pytest finalizer semantics.
    def _cleanup() -> None:
        set_engines(job_manager=None)
        set_api_key_store({})

    request.addfinalizer(_cleanup)

    return app, manager, redis


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _client(app: FastAPI) -> TestClient:
    return TestClient(app)


def _auth_headers() -> dict[str, str]:
    return {"X-API-Key": _API_KEY}


# ---------------------------------------------------------------------------
# Scenario 1 — 200 with progress metadata
# ---------------------------------------------------------------------------


async def test_progress_endpoint_returns_progress_metadata(
    app_with_jobs: tuple[FastAPI, JobManager, FakeRedis],
) -> None:
    """R15.1 — progress_current, progress_total, progress_ratio, eta_seconds all
    surface in the response when ``update_progress`` has been called.
    """

    app, manager, _ = app_with_jobs
    job_id = await manager.create_job()
    # Seed progress with a known ratio: 25/100 → 0.25 and a finite ETA.
    await manager.update_progress(job_id, current=25, total=100)

    with _client(app) as client:
        resp = client.get(
            f"/api/v1/jobs/{job_id}/progress", headers=_auth_headers()
        )

    assert resp.status_code == 200
    body = resp.json()
    data = body["data"]
    assert data["job_id"] == job_id
    assert data["progress_current"] == 25
    assert data["progress_total"] == 100
    assert 0.0 <= data["progress_ratio"] <= 1.0
    assert abs(data["progress_ratio"] - 0.25) < 1e-9
    # ``eta_seconds`` will be non-zero because the job has some elapsed
    # time between create and update. We only assert it's a
    # non-negative float; the exact value depends on wall-clock.
    assert isinstance(data["eta_seconds"], (int, float))
    assert data["eta_seconds"] >= 0.0


# ---------------------------------------------------------------------------
# Scenario 2 — 200 without progress metadata
# ---------------------------------------------------------------------------


async def test_progress_endpoint_without_progress_metadata(
    app_with_jobs: tuple[FastAPI, JobManager, FakeRedis],
) -> None:
    """R15.3 — ``progress_ratio`` is ``None`` when progress hasn't been tracked.

    A freshly created job has ``progress_current == progress_total ==
    None``. The router's ``_compute_ratio`` returns ``None`` in that
    case, matching the Pydantic field's optional constraint.
    """

    app, manager, _ = app_with_jobs
    job_id = await manager.create_job()

    with _client(app) as client:
        resp = client.get(
            f"/api/v1/jobs/{job_id}/progress", headers=_auth_headers()
        )

    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["progress_current"] is None
    assert data["progress_total"] is None
    assert data["progress_ratio"] is None
    assert data["eta_seconds"] is None
    assert data["status"] == "pending"


# ---------------------------------------------------------------------------
# Scenario 3 — 404 JOB_NOT_FOUND
# ---------------------------------------------------------------------------


async def test_progress_endpoint_unknown_job_returns_404(
    app_with_jobs: tuple[FastAPI, JobManager, FakeRedis],
) -> None:
    """R15.4 — a missing or TTL-expired job produces 404 JOB_NOT_FOUND."""

    app, _, _ = app_with_jobs
    missing = "00000000-0000-0000-0000-000000000000"

    with _client(app) as client:
        resp = client.get(
            f"/api/v1/jobs/{missing}/progress", headers=_auth_headers()
        )

    assert resp.status_code == 404
    body = resp.json()
    errors = body.get("errors") or []
    assert errors, "expected structured error list"
    assert errors[0]["code"] == "JOB_NOT_FOUND"


async def test_progress_endpoint_expired_job_returns_404(
    app_with_jobs: tuple[FastAPI, JobManager, FakeRedis],
) -> None:
    """A job that's been evicted from Redis also maps to 404.

    We simulate expiry by creating a job and then directly deleting
    the Redis key — the manager returns ``None`` from ``get_job``,
    which the router maps to 404.
    """

    app, manager, redis = app_with_jobs
    job_id = await manager.create_job()

    # Simulate TTL eviction.
    await redis.delete(f"hydra:job:{job_id}")

    with _client(app) as client:
        resp = client.get(
            f"/api/v1/jobs/{job_id}/progress", headers=_auth_headers()
        )

    assert resp.status_code == 404
    assert resp.json()["errors"][0]["code"] == "JOB_NOT_FOUND"


# ---------------------------------------------------------------------------
# Scenario 4 — backward compatibility (R15.5)
# ---------------------------------------------------------------------------


async def test_progress_endpoint_reads_jobs_from_other_routers(
    app_with_jobs: tuple[FastAPI, JobManager, FakeRedis],
) -> None:
    """R15.5 — a job created by the pre-existing product/correlation path
    is readable by the new progress endpoint.

    The pre-existing path uses :meth:`JobManager.create_job` directly
    (no ``update_progress`` call). We verify the new endpoint does not
    require extended fields and returns a valid 200 response.
    """

    app, manager, _ = app_with_jobs
    # Simulate "job created by POST /api/v1/products/generate" —
    # ``create_job`` + ``update_job`` is the exact sequence the P11
    # products router uses.
    job_id = await manager.create_job()
    await manager.update_job(job_id, "running")

    with _client(app) as client:
        resp = client.get(
            f"/api/v1/jobs/{job_id}/progress", headers=_auth_headers()
        )

    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["job_id"] == job_id
    assert data["status"] == "running"
    # Extended fields default to ``None`` because ``update_progress``
    # was never invoked. The router exposes this cleanly.
    assert data["progress_current"] is None
    assert data["progress_total"] is None
    assert data["progress_ratio"] is None


# ---------------------------------------------------------------------------
# Scenario 5 — authentication
# ---------------------------------------------------------------------------


async def test_progress_endpoint_rejects_missing_api_key(
    app_with_jobs: tuple[FastAPI, JobManager, FakeRedis],
) -> None:
    """The endpoint requires ``X-API-Key`` — omission produces 422 or 401.

    FastAPI's ``Header(..., alias=...)`` raises 422 when the header is
    absent. A real deployment's ``RateLimitMiddleware`` might intercept
    before the dependency, but in the test app we mount only the
    jobs router so the dependency layer surfaces the missing header
    directly.
    """

    app, manager, _ = app_with_jobs
    job_id = await manager.create_job()

    with _client(app) as client:
        resp = client.get(f"/api/v1/jobs/{job_id}/progress")

    assert resp.status_code in (401, 422)


async def test_progress_endpoint_rejects_invalid_api_key(
    app_with_jobs: tuple[FastAPI, JobManager, FakeRedis],
) -> None:
    """An unknown ``X-API-Key`` is rejected with 401 by the auth dep."""

    app, manager, _ = app_with_jobs
    job_id = await manager.create_job()

    with _client(app) as client:
        resp = client.get(
            f"/api/v1/jobs/{job_id}/progress",
            headers={"X-API-Key": "this-key-does-not-exist"},
        )

    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Scenario 6 — ratio clamping
# ---------------------------------------------------------------------------


async def test_progress_ratio_clamped_when_current_exceeds_total(
    app_with_jobs: tuple[FastAPI, JobManager, FakeRedis],
) -> None:
    """The router clamps ratio to ``[0, 1]`` when upstream values drift.

    A misbehaving worker might push ``current > total``. The Pydantic
    field constraint is ``le=1.0``, so the router clamps the computed
    ratio before the model validates. Without the clamp the response
    would raise a 500 from Pydantic validation.

    We bypass ``update_progress``'s monotonicity guard by writing
    directly to Redis — the test asserts the router is defensive, not
    that ``update_progress`` lets bad values through.
    """

    app, manager, redis = app_with_jobs
    job_id = await manager.create_job()
    # Hand-craft a JobStatus with current > total.
    job = await manager.get_job(job_id)
    assert job is not None
    job_dict = job.model_dump()
    job_dict["progress_current"] = 200
    job_dict["progress_total"] = 100
    job_dict["eta_seconds"] = 0.0

    import json
    await redis.setex(
        f"hydra:job:{job_id}", 3600, json.dumps(job_dict)
    )

    with _client(app) as client:
        resp = client.get(
            f"/api/v1/jobs/{job_id}/progress", headers=_auth_headers()
        )

    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["progress_current"] == 200
    assert data["progress_total"] == 100
    # Clamped to 1.0, not > 1 which would violate the schema.
    assert data["progress_ratio"] == 1.0


async def test_progress_ratio_clamped_when_total_is_zero(
    app_with_jobs: tuple[FastAPI, JobManager, FakeRedis],
) -> None:
    """``total == 0`` yields ``progress_ratio = None`` (avoid divide-by-zero).

    This is the pathological "we tracked total but never got a
    non-zero count" case. The router's ``_compute_ratio`` returns
    ``None`` when total is ``<= 0``.
    """

    app, manager, redis = app_with_jobs
    job_id = await manager.create_job()
    job = await manager.get_job(job_id)
    assert job is not None
    job_dict = job.model_dump()
    job_dict["progress_current"] = 0
    job_dict["progress_total"] = 0
    job_dict["eta_seconds"] = 0.0

    import json
    await redis.setex(
        f"hydra:job:{job_id}", 3600, json.dumps(job_dict)
    )

    with _client(app) as client:
        resp = client.get(
            f"/api/v1/jobs/{job_id}/progress", headers=_auth_headers()
        )

    assert resp.status_code == 200
    assert resp.json()["data"]["progress_ratio"] is None


# ---------------------------------------------------------------------------
# Service-unavailable guard
# ---------------------------------------------------------------------------


async def test_progress_endpoint_503_when_job_manager_unwired(
    request: pytest.FixtureRequest,
) -> None:
    """When ``JobManager`` isn't wired, the endpoint returns 503.

    This path is reached only when ``setup_eas`` hasn't been called
    yet (or was called with a partial config). The router declines
    to serve rather than crash on a None attribute.
    """

    # Force-clear any job manager left over from a sibling test — the
    # module-level singleton in ``hydra.api.dependencies`` is not
    # auto-reset between tests and the ``app_with_jobs`` fixture's
    # finalizer runs *after* this test finishes.
    import hydra.api.dependencies as _deps_module

    _deps_module._job_manager = None

    # Explicitly unwire the job manager.
    set_engines(job_manager=None)
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
    app.include_router(jobs_router)

    with _client(app) as client:
        resp = client.get(
            "/api/v1/jobs/any-id/progress", headers=_auth_headers()
        )

    assert resp.status_code == 503
