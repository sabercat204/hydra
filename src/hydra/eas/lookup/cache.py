"""Indicator_Lookup_Cache — Redis-backed, format-agnostic read-through cache (Design §3.7, §8.5).

The lookup router's hot path wraps :meth:`IndicatorLookupCache.get` and
:meth:`IndicatorLookupCache.set`. Payloads are opaque ``bytes`` — the
caller (``LookupAssembler``) is responsible for msgpack encoding /
decoding. That separation lets the cache stay format-agnostic and keeps
this module free of the ``ormsgpack`` import at module load; the lazy
import in :func:`encode_payload` / :func:`decode_payload` means a
deployment without the optional ``[eas]`` extra still loads the cache
module cleanly.

Key scheme: ``hydra:eas:lookup:{indicator_class}:{normalized_value}``,
matching :mod:`.singleflight`. The cache and lock live in the same
Redis DB (``EASSettings.lookup_cache_redis_db``, default 3) so eviction
policy and memory pressure are colocated.

**Metrics.** Each ``get`` emits exactly one of
``hydra_eas_lookup_cache_hits_total{indicator_class}`` or
``hydra_eas_lookup_cache_misses_total{indicator_class}``.
``hydra_eas_lookup_cache_size`` is a gauge refreshed by
:meth:`size` — callers can invoke it from a sampler (task 16.1).
"""

from __future__ import annotations

import logging
from typing import Any

from hydra.eas.lookup.singleflight import cache_key
from hydra.eas.metrics import (
    hydra_eas_lookup_cache_hits_total,
    hydra_eas_lookup_cache_misses_total,
    hydra_eas_lookup_cache_size,
)
from hydra.eas.schemas.lookup import IndicatorClass

logger = logging.getLogger(__name__)

__all__ = ["IndicatorLookupCache", "encode_payload", "decode_payload"]


# Default TTL matches ``EASSettings.lookup_cache_ttl_seconds`` so that
# construction without an override reflects the canonical value. The
# field is overridable per-instance for tests that need sub-second TTLs.
_DEFAULT_TTL = 300


class IndicatorLookupCache:
    """Redis read/write helpers for indicator lookups.

    The constructor takes a duck-typed async Redis client — anything that
    implements ``get(key)``, ``setex(key, ttl, value)``, ``delete(key)``,
    and ``dbsize()`` / ``scan`` will work. ``redis.asyncio.Redis`` meets
    the contract as does ``fakeredis.aioredis.FakeRedis`` for tests.

    The cache is format-agnostic at the transport layer: callers hand
    in ``bytes`` and get ``bytes`` back. The helpers :func:`encode_payload`
    and :func:`decode_payload` provide optional msgpack serialization for
    ``LookupResponse`` objects, lazy-importing ``ormsgpack`` so the
    optional dep is only required when actually invoked.
    """

    def __init__(
        self,
        redis: Any,
        ttl_seconds: int = _DEFAULT_TTL,
        cache_redis_db: int = 3,
    ) -> None:
        self._redis = redis
        self._ttl_seconds = int(ttl_seconds)
        # ``cache_redis_db`` is recorded for diagnostics and for any
        # future code that needs to ``SELECT`` the right db before a
        # command. The canonical wiring is for the caller to hand in a
        # client already bound to the correct db (typically via
        # ``redis.asyncio.Redis(..., db=3)``).
        self._cache_redis_db = int(cache_redis_db)

    # ------------------------------------------------------------------
    # Read / write
    # ------------------------------------------------------------------

    async def get(self, cls: IndicatorClass, value: str) -> bytes | None:
        """Return cached payload bytes for ``(cls, value)`` or ``None`` on miss.

        Emits exactly one metric per call: hits on a populated key,
        misses on empty (or on a Redis error — a client-side error is
        indistinguishable from a miss from the caller's perspective).
        """

        key = cache_key(cls, value)
        try:
            raw = await self._redis.get(key)
        except Exception as exc:  # noqa: BLE001 — defensive
            logger.warning(
                "eas.lookup.cache.get_failed",
                extra={"key": key, "error": str(exc)},
            )
            _inc_miss(cls)
            return None

        if raw is None:
            _inc_miss(cls)
            return None

        _inc_hit(cls)
        # Normalize to bytes — see :meth:`wait_for_value` in singleflight
        # for the matching coercion. ``decode_responses=True`` clients
        # return ``str``; binary clients return ``bytes``.
        if isinstance(raw, (bytes, bytearray)):
            return bytes(raw)
        return str(raw).encode("utf-8")

    async def set(
        self,
        cls: IndicatorClass,
        value: str,
        payload: bytes,
    ) -> None:
        """Write ``payload`` under ``(cls, value)`` with the configured TTL.

        Uses ``SETEX`` (via ``set(key, value, ex=ttl)``) so that the
        cache entry expires after ``ttl_seconds`` regardless of access
        pattern. LRU eviction (configured at Redis start-up) is the
        secondary protection against memory blow-up when the cache is
        under load.
        """

        if not isinstance(payload, (bytes, bytearray)):
            raise TypeError(
                f"payload must be bytes, got {type(payload).__name__}"
            )

        key = cache_key(cls, value)
        try:
            # ``set(..., ex=N)`` is the canonical ``SETEX`` spelling on
            # ``redis.asyncio``. ``setex`` also exists but ``set(..., ex)``
            # keeps the client API symmetric with the lock's
            # ``set(..., nx=True, ex)``.
            await self._redis.set(key, bytes(payload), ex=self._ttl_seconds)
        except Exception as exc:  # noqa: BLE001
            # Failing to cache is non-fatal — the caller's response
            # still goes out, subsequent requests will just miss and
            # re-assemble. We log and move on.
            logger.warning(
                "eas.lookup.cache.set_failed",
                extra={"key": key, "error": str(exc)},
            )

    async def delete(self, cls: IndicatorClass, value: str) -> None:
        """Remove the cache entry for ``(cls, value)`` if present.

        Used by post-MVP invalidation paths (e.g. when an asset changes,
        flush its indicator's cache entry). Silent no-op when the key
        is already absent.
        """

        key = cache_key(cls, value)
        try:
            await self._redis.delete(key)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "eas.lookup.cache.delete_failed",
                extra={"key": key, "error": str(exc)},
            )

    # ------------------------------------------------------------------
    # Size / gauge
    # ------------------------------------------------------------------

    async def size(self) -> int:
        """Return the number of keys in the cache Redis DB.

        Prefers ``DBSIZE`` (O(1)) — the canonical cheap way to query
        key count. Falls back to counting via ``SCAN`` when the client
        doesn't expose ``dbsize`` (some wrappers don't). Updates the
        ``hydra_eas_lookup_cache_size`` gauge as a side effect so the
        caller can treat :meth:`size` as the sampler entry point.
        """

        dbsize_fn = getattr(self._redis, "dbsize", None)
        if dbsize_fn is not None:
            try:
                count = await dbsize_fn()
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "eas.lookup.cache.dbsize_failed",
                    extra={"error": str(exc)},
                )
                count = 0
        else:
            count = await self._scan_count()

        try:
            hydra_eas_lookup_cache_size.set(float(count))
        except AttributeError:
            # No-op metric doesn't have ``set``; silently skip.
            pass
        return int(count)

    async def _scan_count(self) -> int:
        """Walk all ``hydra:eas:lookup:*`` keys via ``SCAN`` as a fallback."""

        scan_fn = getattr(self._redis, "scan_iter", None)
        if scan_fn is None:
            return 0
        count = 0
        try:
            async for _ in scan_fn(match="hydra:eas:lookup:*"):
                count += 1
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "eas.lookup.cache.scan_failed",
                extra={"error": str(exc)},
            )
        return count


# ---------------------------------------------------------------------------
# Optional msgpack helpers (lazy import of ormsgpack)
# ---------------------------------------------------------------------------


def encode_payload(obj: Any) -> bytes:
    """Serialize a Python object to msgpack bytes (Design §3.7).

    ``ormsgpack`` is lazily imported so that a deployment without the
    optional ``[eas]`` extra can still import this module for side
    effects (e.g. the router) without paying the dependency cost. The
    default options handle ``datetime``, ``UUID``, and ``Pydantic``
    models via ``pydantic_dump_json`` /
    ``ormsgpack.OPT_SERIALIZE_PYDANTIC`` flags.
    """

    import ormsgpack  # noqa: PLC0415 - lazy optional dep

    return ormsgpack.packb(
        obj,
        option=(
            ormsgpack.OPT_SERIALIZE_PYDANTIC
            | ormsgpack.OPT_NAIVE_UTC
            | ormsgpack.OPT_UTC_Z
        ),
    )


def decode_payload(data: bytes) -> Any:
    """Deserialize msgpack bytes back to a Python object (Design §3.7)."""

    import ormsgpack  # noqa: PLC0415 - lazy optional dep

    return ormsgpack.unpackb(data)


# ---------------------------------------------------------------------------
# Metric helpers (private)
# ---------------------------------------------------------------------------


def _inc_hit(cls: IndicatorClass) -> None:
    try:
        hydra_eas_lookup_cache_hits_total.labels(indicator_class=cls).inc()
    except Exception:  # pragma: no cover - no-op metric path
        pass


def _inc_miss(cls: IndicatorClass) -> None:
    try:
        hydra_eas_lookup_cache_misses_total.labels(indicator_class=cls).inc()
    except Exception:  # pragma: no cover - no-op metric path
        pass
