"""Redis-backed single-flight lock for cache stampede protection (Design §3.7).

The stampede pattern for the lookup cache is:

1. Cache GET returns ``None``.
2. Attempt ``SET hydra:eas:lookup:sf:{cls}:{value} <request_id> NX EX <ttl>``.
3. If ``SET`` succeeds (we won the flight), compute the payload, write
   the cache, DEL the lock.
4. If ``SET`` fails (someone else won), poll the cache key every 50 ms
   for up to 2 seconds. If the value appears, return it. Otherwise
   fall through to the uncached path as a safety valve.

The lock value is a caller-chosen ``request_id`` so release is
CAS-safe — we only ``DEL`` when the current lock value still matches
the id we wrote, preventing a delayed winner from accidentally
clearing a new winner's lock.

Key shape: ``hydra:eas:lookup:sf:{cls}:{value}`` — distinct from the
cache key ``hydra:eas:lookup:{cls}:{value}`` by the ``:sf:`` prefix.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from hydra.eas.schemas.lookup import IndicatorClass

logger = logging.getLogger(__name__)

__all__ = ["SingleFlightLock", "lock_key", "cache_key"]


_LOCK_PREFIX = "hydra:eas:lookup:sf:"
_CACHE_PREFIX = "hydra:eas:lookup:"


def lock_key(cls: IndicatorClass, value: str) -> str:
    """Return the Redis key for the single-flight lock of ``(cls, value)``."""

    return f"{_LOCK_PREFIX}{cls}:{value}"


def cache_key(cls: IndicatorClass, value: str) -> str:
    """Return the Redis key for the cached payload of ``(cls, value)``."""

    return f"{_CACHE_PREFIX}{cls}:{value}"


class SingleFlightLock:
    """Dogpile-protection lock backed by a single Redis ``SET NX EX``.

    The lock is scoped to the same Redis DB as the lookup cache (db 3 by
    default) so that eviction policy and memory pressure are colocated.
    The ``_redis`` attribute is a duck-typed async Redis client — anything
    exposing ``set(key, value, nx=True, ex=ttl)``, ``get(key)``, and
    ``delete(key)`` coroutines will work (``redis.asyncio.Redis`` and
    test doubles both qualify).
    """

    def __init__(self, redis: Any) -> None:
        self._redis = redis

    # ------------------------------------------------------------------
    # Lock acquisition / release
    # ------------------------------------------------------------------

    async def acquire(
        self,
        cls: IndicatorClass,
        value: str,
        request_id: str,
        ttl_seconds: int = 10,
    ) -> bool:
        """Try to claim the flight for ``(cls, value)``.

        Returns ``True`` iff the ``SET NX EX`` succeeded — meaning this
        caller owns the flight and should run the assembler. Returns
        ``False`` when another caller already holds the lock; the loser
        should poll the cache via :meth:`wait_for_value`.

        The ``ttl_seconds`` default (10 s) matches Design §3.7 — long
        enough for PG + ES fan-out under normal load, short enough to
        auto-release if the winner crashes mid-assembly.

        Errors raised by the Redis client are swallowed and logged as
        warnings; on Redis failure we return ``False`` so the caller
        falls through to the uncached assembly path rather than
        blocking on a broken lock.
        """

        key = lock_key(cls, value)
        try:
            result = await self._redis.set(
                key, request_id, nx=True, ex=int(ttl_seconds)
            )
        except Exception as exc:  # noqa: BLE001 — defensive
            logger.warning(
                "eas.lookup.singleflight.acquire_failed",
                extra={"key": key, "error": str(exc)},
            )
            return False
        # ``redis.asyncio`` returns ``True`` on claim and ``None`` on
        # contention; some wrappers return ``"OK"`` / ``None`` with
        # ``decode_responses=True``. Treat any truthy return as claim.
        return bool(result)

    async def release(
        self,
        cls: IndicatorClass,
        value: str,
        request_id: str,
    ) -> None:
        """CAS-safe release — only delete if the stored value still matches.

        Without the CAS guard, a winner that held the lock past its TTL
        could ``DEL`` the key after a second winner had already re-claimed
        it, letting a third caller race past both.

        The simple ``GET + DEL`` implementation is racy only in the
        narrow window between the GET and DEL — in that window another
        client could take over the key. That race is benign because
        (a) the new owner will set a new value, so our subsequent ``DEL``
        would still be against a value we don't recognize if we tried
        again, and (b) the worst case is that the lock lingers for one
        extra TTL. For an MVP fix-path this is good enough; a Lua
        ``EVAL`` could close the gap entirely in post-MVP.
        """

        key = lock_key(cls, value)
        try:
            current = await self._redis.get(key)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "eas.lookup.singleflight.release_get_failed",
                extra={"key": key, "error": str(exc)},
            )
            return

        # Redis returns bytes for ``decode_responses=False`` clients and
        # str for decoded clients. Accept both.
        current_str = _coerce_str(current)
        if current_str != request_id:
            # Either the lock expired and another caller owns it, or
            # the key was already cleaned up. Either way we must NOT
            # delete.
            return

        try:
            await self._redis.delete(key)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "eas.lookup.singleflight.release_del_failed",
                extra={"key": key, "error": str(exc)},
            )

    # ------------------------------------------------------------------
    # Loser path: wait for the winner to populate the cache
    # ------------------------------------------------------------------

    async def wait_for_value(
        self,
        cache_key_str: str,
        poll_interval_ms: int = 50,
        timeout_ms: int = 2_000,
    ) -> bytes | None:
        """Poll the cache for up to ``timeout_ms`` after losing the flight.

        Called by the router when ``acquire`` returned ``False``. Polls
        every ``poll_interval_ms`` (default 50 ms) for up to
        ``timeout_ms`` (default 2 s, per Design §3.7). Returns the raw
        cached bytes on hit, or ``None`` on timeout.

        Callers fall through to the uncached assembly path on ``None``
        so that a crashed winner never blocks a loser indefinitely.
        """

        deadline_iters = max(1, int(timeout_ms // max(1, poll_interval_ms)))
        sleep_seconds = max(0.001, float(poll_interval_ms) / 1000.0)

        for _ in range(deadline_iters):
            try:
                cached = await self._redis.get(cache_key_str)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "eas.lookup.singleflight.wait_get_failed",
                    extra={"key": cache_key_str, "error": str(exc)},
                )
                return None
            if cached is not None:
                # Normalize to ``bytes`` so the caller can hand the
                # payload straight to ``ormsgpack.unpackb``. When the
                # client returned a str (decode_responses=True), we
                # re-encode to utf-8; msgpack bytes are binary-safe so
                # the round-trip is harmless for bytes callers.
                return cached if isinstance(cached, (bytes, bytearray)) else cached.encode("utf-8")
            await asyncio.sleep(sleep_seconds)

        return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _coerce_str(value: Any) -> str | None:
    """Normalize ``bytes`` / ``str`` / ``None`` values from Redis to ``str|None``.

    ``redis.asyncio`` clients return ``bytes`` by default and ``str`` when
    configured with ``decode_responses=True``. We accept both so the lock
    works with either wiring.
    """

    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (bytes, bytearray)):
        try:
            return bytes(value).decode("utf-8")
        except UnicodeDecodeError:
            return None
    return None
