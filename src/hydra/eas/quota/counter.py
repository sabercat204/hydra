"""Per-tenant cost quota counters (R22, Design §3.9).

This module implements :class:`CostQuotaCounter`, a thin wrapper around
Redis that enforces the daily per-tenant cost quotas declared in
:class:`hydra.eas.settings.CostQuota`. The counter is keyed by tenant,
quota name, and UTC calendar day; every counter has a fixed 48-hour TTL
per R22.3 so that timezone edge cases around UTC midnight never leave
an orphaned counter in Redis.

The atomic increment-and-check sequence follows the pattern described
in Design §3.9:

    MULTI
      INCR  hydra:eas:cost:{tenant_id}:{quota_name}:{yyyymmdd}
      EXPIRE hydra:eas:cost:{tenant_id}:{quota_name}:{yyyymmdd} 172800
    EXEC

The ``EXPIRE`` is issued on every call (not only when the INCR returns
1) because ``EXPIRE`` on an already-TTL'd key is a no-op in Redis but
is safe and idempotent — issuing it unconditionally also removes the
race where two concurrent first-time INCRs both observe a value > 1
and neither sets the TTL. If the post-increment count exceeds the
configured ``limit`` the counter is rolled back with a separate
``DECR`` pipeline and a :class:`hydra.api.errors.HydraAPIException`
with ``ErrorCode.COST_QUOTA_EXCEEDED`` is raised. The ``Retry-After``
header carried by the exception points at the next UTC midnight so the
caller can back off precisely until the per-day bucket resets.

The method also updates the :data:`hydra_eas_quota_usage_ratio` gauge
(R22.4) so the ``HydraEASQuotaNearExhaustion`` alert can fire at
``ratio > 0.9`` per Design §11.2.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from hydra.api.errors import ErrorCode, HydraAPIException
from hydra.eas.metrics import hydra_eas_quota_usage_ratio

logger = logging.getLogger(__name__)

__all__ = ["CostQuotaCounter"]


# TTL matches Design §3.9 / R22.3: 48 hours so a UTC-midnight-adjacent
# request never bumps against an expiring counter from yesterday.
_QUOTA_TTL_SECONDS: int = 172_800


def _utc_day_key(now: datetime) -> str:
    """Return ``YYYYMMDD`` for the UTC day containing ``now``."""

    return now.astimezone(timezone.utc).strftime("%Y%m%d")


def _seconds_to_utc_midnight(now: datetime) -> int:
    """Seconds until the next UTC midnight from ``now``.

    Used as the ``Retry-After`` hint when a tenant exhausts a daily
    quota (R22.2). We always round up so the client never retries a
    nanosecond before the counter resets.
    """

    now_utc = now.astimezone(timezone.utc)
    tomorrow = (now_utc + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    delta = tomorrow - now_utc
    # ``total_seconds()`` may yield a fractional value — round up to the
    # next whole second so the client never retries before the reset.
    seconds = int(delta.total_seconds())
    if delta.total_seconds() > seconds:
        seconds += 1
    return max(seconds, 1)


class CostQuotaCounter:
    """Redis-backed per-tenant daily cost quota counter (Design §3.9).

    The counter is intentionally stateless in Python: every
    :meth:`increment_and_check` call executes an atomic Redis pipeline,
    observes the returned count, and either reports success or
    unwinds the increment with a compensating ``DECR``. There is no
    per-process cache; correctness depends on Redis, not on our memory
    model.

    Parameters
    ----------
    redis:
        An ``async``-capable Redis client exposing ``pipeline``,
        ``incr``, ``expire``, and ``decr``. The EAS process shares the
        same Redis client used by ``RateLimitMiddleware`` — no extra
        connection pool is required.
    """

    def __init__(self, redis: Any) -> None:
        self._redis = redis

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def increment_and_check(
        self,
        tenant_id: UUID | str,
        quota_name: str,
        limit: int,
    ) -> int:
        """Atomically increment the per-day counter and enforce ``limit``.

        Parameters
        ----------
        tenant_id:
            The owning tenant — serialised to its canonical string form
            so that both ``UUID`` and pre-stringified values produce
            the same Redis key.
        quota_name:
            The symbolic name of the quota (e.g. ``screenshots_per_day``).
            This value becomes the ``{quota_name}`` path component of
            the Redis key and the ``quota_name`` label on the usage
            gauge, so it MUST match a field of
            :class:`hydra.eas.settings.CostQuota`.
        limit:
            The configured per-day maximum. When the post-increment
            counter exceeds ``limit`` the increment is rolled back and
            a 429 :class:`HydraAPIException` is raised.

        Returns
        -------
        int
            The post-increment value of the counter (always ``<= limit``
            when the call returns normally).

        Raises
        ------
        HydraAPIException
            Status 429 / ``COST_QUOTA_EXCEEDED`` when the tenant has
            exhausted the daily quota. The exception carries a
            ``Retry-After`` header in its ``detail`` dict so the
            exception handler can surface it unchanged.
        """

        now = datetime.now(timezone.utc)
        day = _utc_day_key(now)
        tenant_str = str(tenant_id)
        key = f"hydra:eas:cost:{tenant_str}:{quota_name}:{day}"

        # 1) Atomic INCR + EXPIRE in a single MULTI/EXEC pipeline.
        #    ``pipeline(transaction=True)`` is the ``redis.asyncio``
        #    flavour of MULTI/EXEC; ``.execute()`` returns the list of
        #    per-command results in order.
        pipe = self._redis.pipeline(transaction=True)
        pipe.incr(key)
        pipe.expire(key, _QUOTA_TTL_SECONDS)
        results = await pipe.execute()
        # ``INCR`` is the first command in the pipeline; cast defensively
        # in case the Redis client returns bytes/str.
        try:
            count = int(results[0])
        except (TypeError, ValueError, IndexError) as exc:
            # A misbehaving Redis client cannot be allowed to silently
            # skip quota enforcement — fail closed.
            logger.warning(
                "eas.quota.pipeline_result_malformed",
                extra={
                    "tenant_id": tenant_str,
                    "quota_name": quota_name,
                    "error": str(exc),
                },
            )
            raise HydraAPIException(
                code=ErrorCode.SERVICE_UNAVAILABLE,
                message="Cost quota service returned an unexpected result",
                status_code=503,
            ) from exc

        # 2) Update the usage-ratio gauge *before* the over-limit check
        #    so dashboards see the over-budget spike even when we roll
        #    the counter back. The gauge label set matches Design §11.1.
        if limit > 0:
            ratio = count / float(limit)
        else:
            # ``limit == 0`` disables the quota completely; reporting
            # an effectively-infinite ratio is not useful, so clamp to
            # zero and fall through — the check below rejects anyway.
            ratio = 0.0
        try:
            hydra_eas_quota_usage_ratio.labels(
                tenant_id=tenant_str, quota_name=quota_name
            ).set(ratio)
        except Exception:  # noqa: BLE001 — metric failures never block IO
            logger.debug("eas.quota.gauge_update_failed", exc_info=True)

        # 3) Over-limit path — undo the increment and raise 429.
        if count > limit:
            await self._rollback(key)
            retry_after = _seconds_to_utc_midnight(now)
            detail = {
                "quota_name": quota_name,
                "limit": int(limit),
                "retry_after": retry_after,
            }
            # ``HydraAPIException`` carries ``detail`` into the API body;
            # the router/exception-handler layer reads it to populate
            # the ``Retry-After`` header (R22.2).
            raise HydraAPIException(
                code=ErrorCode.COST_QUOTA_EXCEEDED,
                message=f"Daily quota '{quota_name}' exceeded",
                detail=detail,
                status_code=429,
            )

        return count

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _rollback(self, key: str) -> None:
        """Undo the most recent INCR on ``key`` with a compensating DECR.

        Executed as a separate pipeline so the compensating action
        cannot be entangled with subsequent increments from concurrent
        requests. A DECR on a missing key creates a -1 value, which is
        harmless: the key still carries a 48-hour TTL and the next
        INCR lifts the value to 0. We silence errors because the
        primary INCR already succeeded; losing the compensating DECR
        is a bounded-cost bug (the counter reaches ``limit + 1`` at
        most) that UTC-midnight TTL expiry repairs.
        """

        try:
            pipe = self._redis.pipeline(transaction=True)
            pipe.decr(key)
            await pipe.execute()
        except Exception:  # noqa: BLE001
            logger.warning(
                "eas.quota.rollback_failed",
                extra={"key": key},
                exc_info=True,
            )
