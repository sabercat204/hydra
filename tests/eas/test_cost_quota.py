"""Tests for :class:`hydra.eas.quota.counter.CostQuotaCounter` (task 15.5).

These tests exercise Property 23 — cost-quota enforcement at the
boundary — and its surrounding concerns (TTL, tenant isolation, quota
name isolation, Retry-After hint, zero-limit behaviour).

We deliberately drive the counter with an in-memory ``FakeRedis`` double
that speaks the small dialect the counter actually uses:

* ``pipeline(transaction=True)`` returns an object with ``.incr``,
  ``.expire``, ``.decr``, and ``.execute``.
* ``execute()`` is ``async`` and returns an ordered list of per-command
  results, matching the ``redis.asyncio`` contract on which the counter
  is written.

A real Redis instance would of course exercise the same code paths, but
the counter's contract is entirely about the ordering of
``INCR+EXPIRE`` and a compensating ``DECR`` on overage — both trivially
observable against the fake — so a full Redis deployment is not needed
for Property 23. The fake is intentionally permissive: any operation
not in ``{incr, expire, decr}`` would silently be accepted but skipped,
so a drift in the counter's usage would surface as a wrong-result
assertion below rather than an obscure attribute error.

Validates: R21.1, R21.2, R21.4, R22.1, R22.2 (Property 23).
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest
from hypothesis import given, settings as h_settings, strategies as st

from hydra.api.errors import ErrorCode, HydraAPIException
from hydra.eas.quota.counter import CostQuotaCounter


# ---------------------------------------------------------------------------
# FakeRedis double — matches the exact surface used by CostQuotaCounter.
# ---------------------------------------------------------------------------


class FakePipeline:
    """A minimal in-memory ``redis.asyncio`` pipeline double.

    The counter pushes ``incr`` / ``expire`` / ``decr`` onto the
    pipeline and then ``await``s ``execute()``; that's the whole
    surface we need. We accumulate ops in the order they arrive and
    replay them against the parent ``FakeRedis`` on ``execute()`` so
    the returned result list is in the same order — which is critical
    because ``CostQuotaCounter`` reads ``results[0]`` as the
    post-increment count.
    """

    def __init__(self, redis: "FakeRedis") -> None:
        self.redis = redis
        self.ops: list[tuple[Any, ...]] = []

    def incr(self, key: str) -> "FakePipeline":
        self.ops.append(("incr", key))
        return self

    def expire(self, key: str, ttl: int) -> "FakePipeline":
        self.ops.append(("expire", key, ttl))
        return self

    def decr(self, key: str) -> "FakePipeline":
        self.ops.append(("decr", key))
        return self

    async def execute(self) -> list[Any]:
        results: list[Any] = []
        for op in self.ops:
            if op[0] == "incr":
                self.redis.store[op[1]] = self.redis.store.get(op[1], 0) + 1
                results.append(self.redis.store[op[1]])
            elif op[0] == "expire":
                self.redis.ttls[op[1]] = op[2]
                results.append(True)
            elif op[0] == "decr":
                self.redis.store[op[1]] = self.redis.store.get(op[1], 0) - 1
                results.append(self.redis.store[op[1]])
        return results


class FakeRedis:
    """Tiny Redis double backing :class:`FakePipeline`.

    Exposes just the state the tests observe: the key/value store and
    per-key TTLs. The counter only calls ``pipeline``; this class
    therefore never needs ``get``/``set``/``delete`` etc.
    """

    def __init__(self) -> None:
        self.store: dict[str, int] = {}
        self.ttls: dict[str, int] = {}

    def pipeline(self, transaction: bool = True) -> FakePipeline:
        # ``transaction`` is accepted but ignored — the counter always
        # passes ``transaction=True`` and our fake is serial anyway.
        return FakePipeline(self)


# ---------------------------------------------------------------------------
# Happy-path unit tests
# ---------------------------------------------------------------------------


async def test_first_increment_returns_1() -> None:
    """The first call within a fresh UTC day returns ``1``.

    Sanity check for the INCR path — no existing key, so the pipeline
    INCR should lift the counter from an implicit 0 to 1 and the
    counter should return that value.
    """
    redis = FakeRedis()
    counter = CostQuotaCounter(redis)
    tenant_id = uuid4()

    result = await counter.increment_and_check(
        tenant_id, "screenshots_per_day", limit=10
    )

    assert result == 1


async def test_ttl_set_to_48_hours() -> None:
    """EXPIRE is called with 172800 (48h) per R22.3.

    The 48-hour TTL insulates the counter from UTC-midnight races:
    a request that lands a microsecond before midnight does not
    observe an expiring bucket from "yesterday". We assert the TTL is
    exactly the documented value against the single key the counter
    creates for ``screenshots_per_day``.
    """
    redis = FakeRedis()
    counter = CostQuotaCounter(redis)
    tenant_id = uuid4()

    await counter.increment_and_check(
        tenant_id, "screenshots_per_day", limit=10
    )

    keys_with_ttl = [k for k in redis.ttls if "screenshots_per_day" in k]
    assert len(keys_with_ttl) == 1
    assert redis.ttls[keys_with_ttl[0]] == 172_800


# ---------------------------------------------------------------------------
# Property 23 — cost-quota enforcement at the boundary
# ---------------------------------------------------------------------------


@given(limit=st.integers(min_value=1, max_value=20))
@h_settings(max_examples=20, deadline=None)
async def test_property_cost_quota_boundary(limit: int) -> None:
    """Property 23 — boundary behaviour of the per-day counter.

    For every positive ``limit``:

    * The first ``limit`` calls succeed and return the running count
      ``1..limit``.
    * The ``(limit + 1)``-th call raises ``HydraAPIException`` with
      ``COST_QUOTA_EXCEEDED`` and status ``429``; the detail carries
      ``retry_after`` so the exception handler can project it into an
      HTTP header.
    * After the overage, the Redis counter is exactly ``limit`` — the
      compensating ``DECR`` has unwound the over-the-line ``INCR`` so
      the bucket reflects only successful increments.

    ``limit ∈ [1, 20]`` is a deliberately narrow band: the property
    holds for any positive ``limit`` but small values are enough to
    exercise the increment/overage/rollback transition without
    turning each Hypothesis example into a O(limit) hot loop.

    Validates: R22.1, R22.2 (Property 23).
    """
    redis = FakeRedis()
    counter = CostQuotaCounter(redis)
    tenant_id = uuid4()

    # First ``limit`` calls succeed and the returned value equals the
    # running count. The invariant ``result == i + 1`` catches any
    # off-by-one in the return of ``increment_and_check``.
    for i in range(limit):
        result = await counter.increment_and_check(
            tenant_id, "screenshots_per_day", limit=limit
        )
        assert result == i + 1

    # The (limit + 1)-th call is the overage; it must raise with the
    # documented code/status and must carry ``retry_after`` in the
    # detail so the error handler can project the HTTP header.
    with pytest.raises(HydraAPIException) as exc_info:
        await counter.increment_and_check(
            tenant_id, "screenshots_per_day", limit=limit
        )
    exc = exc_info.value
    assert exc.code == ErrorCode.COST_QUOTA_EXCEEDED
    assert exc.status_code == 429
    assert exc.detail is not None
    assert "retry_after" in exc.detail

    # Redis bucket equals ``limit`` — the rollback has unwound the
    # over-the-line increment so the counter reflects only the
    # successful calls.
    matching_keys = [k for k in redis.store if "screenshots_per_day" in k]
    assert len(matching_keys) == 1
    assert redis.store[matching_keys[0]] == limit


# ---------------------------------------------------------------------------
# Retry-After hint structure
# ---------------------------------------------------------------------------


async def test_retry_after_is_seconds_to_utc_midnight() -> None:
    """``retry_after`` is a positive integer bounded by 24 hours.

    The exact number of seconds depends on "now", which we do not
    mock. The invariant is structural: ``retry_after`` is a positive
    ``int`` no greater than ``86_400`` (the longest possible gap to
    UTC midnight). The exception detail also echoes ``quota_name`` and
    ``limit`` so the error body names the exhausted quota (R22.2).
    """
    redis = FakeRedis()
    counter = CostQuotaCounter(redis)
    tenant_id = uuid4()

    # Exhaust a 1-wide quota so the very next call hits the overage
    # path deterministically.
    await counter.increment_and_check(tenant_id, "test_quota", limit=1)

    with pytest.raises(HydraAPIException) as exc_info:
        await counter.increment_and_check(tenant_id, "test_quota", limit=1)

    detail = exc_info.value.detail
    assert detail is not None

    retry_after = detail["retry_after"]
    assert isinstance(retry_after, int)
    # Retry-After is strictly positive (at least the next whole
    # second) and at most 24h — the outer bound on seconds-to-UTC-midnight.
    assert 1 <= retry_after <= 86_400

    assert detail["quota_name"] == "test_quota"
    assert detail["limit"] == 1


# ---------------------------------------------------------------------------
# Tenant isolation
# ---------------------------------------------------------------------------


async def test_tenants_isolated() -> None:
    """Two tenants share no quota state (R20.4 cross-cut).

    Tenant A exhausting its quota of 2 must not block tenant B: the
    Redis key path embeds ``{tenant_id}`` so each tenant owns its own
    counter. We assert by running A up to the limit and then showing B
    can still take its first slot.
    """
    redis = FakeRedis()
    counter = CostQuotaCounter(redis)
    tenant_a = uuid4()
    tenant_b = uuid4()

    for _ in range(2):
        await counter.increment_and_check(tenant_a, "quota", limit=2)

    # Tenant B's first call must still succeed and return 1 — a
    # cross-tenant leak would either raise 429 (if A's counter was
    # shared) or return > 1 (if B was reading A's state).
    result = await counter.increment_and_check(tenant_b, "quota", limit=2)
    assert result == 1


# ---------------------------------------------------------------------------
# Quota-name isolation
# ---------------------------------------------------------------------------


async def test_quota_names_isolated() -> None:
    """Distinct ``quota_name`` values keep independent counters.

    A tenant may hit ``screenshots_per_day`` many times without
    touching ``lookup_requests_per_day`` and vice versa: the Redis key
    path embeds ``{quota_name}`` so each quota is its own bucket. We
    exhaust ``screenshots_per_day`` (limit 1) and show that the first
    ``lookup_requests_per_day`` call still succeeds and returns 1.
    """
    redis = FakeRedis()
    counter = CostQuotaCounter(redis)
    tenant_id = uuid4()

    await counter.increment_and_check(
        tenant_id, "screenshots_per_day", limit=1
    )

    # Different quota name, same tenant — independent counter.
    result = await counter.increment_and_check(
        tenant_id, "lookup_requests_per_day", limit=1
    )
    assert result == 1


# ---------------------------------------------------------------------------
# Degenerate ``limit=0`` path
# ---------------------------------------------------------------------------


async def test_zero_limit_rejects_first_call() -> None:
    """``limit=0`` turns the quota into a hard reject.

    A quota of 0 has no successful slots — the first call must raise
    ``COST_QUOTA_EXCEEDED`` without ever returning a successful count.
    This path is exercised by capability tiers that disable a feature
    per tenant without deleting the quota entry.
    """
    redis = FakeRedis()
    counter = CostQuotaCounter(redis)
    tenant_id = uuid4()

    with pytest.raises(HydraAPIException) as exc_info:
        await counter.increment_and_check(tenant_id, "quota", limit=0)

    assert exc_info.value.code == ErrorCode.COST_QUOTA_EXCEEDED
