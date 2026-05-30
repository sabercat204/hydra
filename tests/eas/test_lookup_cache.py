"""Property 20 — Lookup cache idempotency (task 13.9).

The invariant (Design §3.7, R17.1, R17.2):

> Repeated lookup requests for the same ``(indicator_class, value)``
> within the TTL window return byte-identical bodies (the cached
> payload), differing only in the ``meta.cache`` hint that the router
> flips from ``"miss"`` on first hit to ``"hit"`` on subsequent hits.

These tests drive :class:`IndicatorLookupCache` directly — the router
that layers ``meta.cache`` on top is thin — so they exercise the
transport-level "same bytes out as in" guarantee across repeated
reads. Metrics side-effects (``hydra_eas_lookup_cache_hits_total``,
``hydra_eas_lookup_cache_misses_total``) are asserted alongside.

We also exercise TTL expiry (post-TTL reads are misses, not stale
hits) and set-after-get semantics (put + get round-trip is a true
byte-for-byte).

Validates: R17.1, R17.2, R27.0 (cache idempotency).
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings as h_settings, strategies as st

from hydra.eas.lookup.cache import IndicatorLookupCache
from hydra.eas.metrics import (
    hydra_eas_lookup_cache_hits_total,
    hydra_eas_lookup_cache_misses_total,
)
from hydra.eas.schemas.lookup import IndicatorClass


# ---------------------------------------------------------------------------
# FakeRedis — wall-clock-aware expiry semantics
# ---------------------------------------------------------------------------


class _FakeRedis:
    """Minimal async Redis double for :class:`IndicatorLookupCache`.

    Tracks per-key values and TTLs as (value, expires_at_loop_time)
    tuples. Uses :func:`asyncio.get_event_loop().time()` for expiry
    so tests can advance the "clock" by awaiting ``asyncio.sleep(ttl)``
    when they want to exercise TTL-based eviction.

    Supported methods (whatever :class:`IndicatorLookupCache` actually
    calls): ``get``, ``set``, ``delete``, ``dbsize``.
    """

    def __init__(self) -> None:
        # key -> (payload_bytes, expires_at_monotonic_seconds)
        self._store: dict[str, tuple[bytes, float | None]] = {}

    def _now(self) -> float:
        return asyncio.get_event_loop().time()

    def _expire_if_due(self, key: str) -> None:
        """Remove ``key`` when its TTL has elapsed."""

        entry = self._store.get(key)
        if entry is None:
            return
        _, expires = entry
        if expires is not None and self._now() >= expires:
            self._store.pop(key, None)

    async def get(self, key: str) -> bytes | None:
        self._expire_if_due(key)
        entry = self._store.get(key)
        return entry[0] if entry is not None else None

    async def set(
        self,
        key: str,
        value: bytes,
        *,
        ex: int | None = None,
        nx: bool = False,
    ) -> bool:
        if nx and key in self._store:
            return False
        expires = self._now() + ex if ex else None
        self._store[key] = (bytes(value), expires)
        return True

    async def delete(self, key: str) -> int:
        existed = key in self._store
        self._store.pop(key, None)
        return 1 if existed else 0

    async def dbsize(self) -> int:
        # Expire as we go so the reported size is accurate.
        for k in list(self._store.keys()):
            self._expire_if_due(k)
        return len(self._store)


# ---------------------------------------------------------------------------
# Helpers for metric sampling
# ---------------------------------------------------------------------------


def _counter_value(counter: Any, **labels: str) -> float:
    """Read the current value of a prometheus Counter (labeled or plain).

    ``prometheus_client`` counters expose ``_value.get()`` (unlabeled)
    or ``.labels(...)._value.get()`` (labeled). The no-op fallback in
    :mod:`hydra.eas.metrics` implements ``labels(...).inc()`` as a
    pass-through, so reading the value is only useful when
    ``prometheus_client`` is actually installed. We guard the read so
    the test skips quietly when it isn't.
    """

    try:
        return float(counter.labels(**labels)._value.get())
    except (AttributeError, ValueError):
        pytest.skip("prometheus_client not installed; metric read unavailable")
        return 0.0  # pragma: no cover - reachable only via the skip path


# ---------------------------------------------------------------------------
# Basic round-trip
# ---------------------------------------------------------------------------


async def test_set_then_get_returns_same_bytes() -> None:
    """A ``set`` followed by ``get`` returns the exact payload bytes.

    Establishes the baseline Property 20 contract: the cache is a
    transparent byte transport.
    """

    redis = _FakeRedis()
    cache = IndicatorLookupCache(redis, ttl_seconds=60)

    payload = b"\x93\x01\x02\x03"  # arbitrary msgpack-shaped bytes
    await cache.set("ipv4", "192.168.1.1", payload)
    out = await cache.get("ipv4", "192.168.1.1")

    assert out == payload


async def test_get_on_missing_key_returns_none() -> None:
    """Cold reads return ``None`` (and emit a miss metric)."""

    redis = _FakeRedis()
    cache = IndicatorLookupCache(redis, ttl_seconds=60)

    out = await cache.get("ipv4", "10.0.0.1")
    assert out is None


# ---------------------------------------------------------------------------
# Property 20 — repeated reads are byte-identical
# ---------------------------------------------------------------------------


async def test_property_repeated_gets_are_byte_identical() -> None:
    """Three consecutive reads within TTL return the same ``bytes`` object.

    Not just equal — each call returns the stored payload verbatim.
    The router layers ``meta.cache`` on top, but the *payload* itself
    is identical across reads.

    Validates: R17.1, R17.2.
    """

    redis = _FakeRedis()
    cache = IndicatorLookupCache(redis, ttl_seconds=60)

    payload = b"some-opaque-payload-bytes"
    await cache.set("domain", "example.com", payload)

    first = await cache.get("domain", "example.com")
    second = await cache.get("domain", "example.com")
    third = await cache.get("domain", "example.com")

    assert first == second == third == payload


@given(
    payload=st.binary(min_size=1, max_size=256),
    read_count=st.integers(min_value=2, max_value=10),
)
@h_settings(
    max_examples=30,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
async def test_property_cache_idempotency_pbt(
    payload: bytes, read_count: int
) -> None:
    """Property-based: any N consecutive reads return the same payload.

    Covers arbitrary byte payloads (msgpack, JSON, opaque binary) over
    arbitrary read counts from 2 to 10.

    Validates: R17.1, R17.2.
    """

    redis = _FakeRedis()
    cache = IndicatorLookupCache(redis, ttl_seconds=300)

    await cache.set("ipv4", "10.0.0.1", payload)

    reads = [await cache.get("ipv4", "10.0.0.1") for _ in range(read_count)]
    # Every read returned the same payload.
    assert all(r == payload for r in reads)


# ---------------------------------------------------------------------------
# TTL expiry — post-TTL reads are misses
# ---------------------------------------------------------------------------


async def test_reads_after_ttl_are_misses() -> None:
    """After ``ttl_seconds`` elapses the cache returns ``None``.

    Uses a short TTL (1 s) + ``asyncio.sleep`` so the test is fast.
    Confirms the entry is gone from the underlying store, not just
    returned as ``None`` via a client-side check.
    """

    redis = _FakeRedis()
    # Tiny TTL so the test runs in ~1 s.
    cache = IndicatorLookupCache(redis, ttl_seconds=1)

    await cache.set("ipv4", "10.0.0.2", b"soon-to-expire")
    assert await cache.get("ipv4", "10.0.0.2") == b"soon-to-expire"

    # Advance the fake clock past the TTL.
    await asyncio.sleep(1.05)

    assert await cache.get("ipv4", "10.0.0.2") is None
    # Entry was removed from the store after the TTL lapsed.
    assert await redis.dbsize() == 0


# ---------------------------------------------------------------------------
# Metrics — first read misses, subsequent reads hit
# ---------------------------------------------------------------------------


async def test_metrics_first_read_is_miss_rest_are_hits() -> None:
    """``hydra_eas_lookup_cache_*`` counters tick correctly per read.

    Sequence:

    1. ``get`` on empty key → miss counter +1.
    2. ``set``.
    3. ``get`` twice → hit counter +2.

    The hits / misses counters carry a ``indicator_class`` label that
    we sample before and after to compute deltas.
    """

    redis = _FakeRedis()
    cache = IndicatorLookupCache(redis, ttl_seconds=60)
    cls: IndicatorClass = "ipv4"
    value = "10.0.0.3"

    baseline_miss = _counter_value(
        hydra_eas_lookup_cache_misses_total, indicator_class=cls
    )
    baseline_hit = _counter_value(
        hydra_eas_lookup_cache_hits_total, indicator_class=cls
    )

    assert await cache.get(cls, value) is None
    await cache.set(cls, value, b"payload")
    assert await cache.get(cls, value) == b"payload"
    assert await cache.get(cls, value) == b"payload"

    miss_after = _counter_value(
        hydra_eas_lookup_cache_misses_total, indicator_class=cls
    )
    hit_after = _counter_value(
        hydra_eas_lookup_cache_hits_total, indicator_class=cls
    )

    assert miss_after - baseline_miss == 1.0
    assert hit_after - baseline_hit == 2.0


async def test_metrics_after_ttl_is_another_miss() -> None:
    """Post-TTL reads count as misses, not hits, from the metric's POV.

    This reinforces Property 20: "within TTL" is the qualifier — the
    idempotency guarantee ends when the key expires.
    """

    redis = _FakeRedis()
    cache = IndicatorLookupCache(redis, ttl_seconds=1)
    cls: IndicatorClass = "domain"
    value = "fresh.example.com"

    baseline_miss = _counter_value(
        hydra_eas_lookup_cache_misses_total, indicator_class=cls
    )

    await cache.set(cls, value, b"payload")
    await cache.get(cls, value)  # hit
    await asyncio.sleep(1.05)
    assert await cache.get(cls, value) is None  # miss after expiry

    miss_after = _counter_value(
        hydra_eas_lookup_cache_misses_total, indicator_class=cls
    )
    # Exactly one miss added — the post-expiry read.
    assert miss_after - baseline_miss == 1.0


# ---------------------------------------------------------------------------
# Delete, overwrite, and type guards
# ---------------------------------------------------------------------------


async def test_delete_removes_the_entry() -> None:
    """``delete`` makes a subsequent ``get`` a miss."""

    redis = _FakeRedis()
    cache = IndicatorLookupCache(redis, ttl_seconds=60)

    await cache.set("hash", "a" * 16, b"payload")
    assert await cache.get("hash", "a" * 16) == b"payload"
    await cache.delete("hash", "a" * 16)
    assert await cache.get("hash", "a" * 16) is None


async def test_overwrite_returns_new_payload() -> None:
    """A second ``set`` overwrites the previous payload cleanly.

    Guards against a bug where the cache silently rejects duplicate
    keys — Property 20 assumes the last write wins.
    """

    redis = _FakeRedis()
    cache = IndicatorLookupCache(redis, ttl_seconds=60)

    await cache.set("ipv4", "10.0.0.4", b"first")
    await cache.set("ipv4", "10.0.0.4", b"second")
    assert await cache.get("ipv4", "10.0.0.4") == b"second"


async def test_non_bytes_payload_raises() -> None:
    """``set`` enforces the ``bytes``-in contract to protect msgpack callers.

    A ``str`` payload would silently round-trip as utf-8 on most
    Redis clients but would break ``ormsgpack.unpackb`` on read. The
    cache raises early so the failure mode is obvious.
    """

    redis = _FakeRedis()
    cache = IndicatorLookupCache(redis, ttl_seconds=60)

    with pytest.raises(TypeError, match="payload must be bytes"):
        await cache.set("ipv4", "10.0.0.5", "not-bytes")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Per-class isolation — same value under different classes is a different key
# ---------------------------------------------------------------------------


async def test_different_classes_dont_collide() -> None:
    """Same ``value`` under different ``cls`` maps to different cache keys.

    The router's cache key is ``hydra:eas:lookup:{cls}:{value}``, so a
    literal ``"10.0.0.1"`` under ``ipv4`` and under ``hostname`` (an
    admittedly pathological tenant) should not return the same
    payload.
    """

    redis = _FakeRedis()
    cache = IndicatorLookupCache(redis, ttl_seconds=60)

    await cache.set("ipv4", "10.0.0.1", b"ipv4-payload")
    await cache.set("hostname", "10.0.0.1", b"hostname-payload")

    assert await cache.get("ipv4", "10.0.0.1") == b"ipv4-payload"
    assert await cache.get("hostname", "10.0.0.1") == b"hostname-payload"


# ---------------------------------------------------------------------------
# Size / gauge
# ---------------------------------------------------------------------------


async def test_size_reflects_current_entries() -> None:
    """``cache.size()`` returns the number of currently-live entries.

    The method also updates the ``hydra_eas_lookup_cache_size`` gauge
    as a side effect — we don't assert on the gauge value here because
    the no-op metric path doesn't expose ``set``, but calling
    ``size()`` must not raise either way.
    """

    redis = _FakeRedis()
    cache = IndicatorLookupCache(redis, ttl_seconds=60)

    assert await cache.size() == 0
    await cache.set("ipv4", "10.0.0.1", b"a")
    await cache.set("ipv4", "10.0.0.2", b"b")
    assert await cache.size() == 2
