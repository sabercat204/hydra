"""FastAPI dependency factory for per-tenant cost quotas (R22.1, R22.2).

The :func:`enforce_cost_quota` callable returns an async dependency
that FastAPI resolves on every request to a quota-gated route. The
dependency does four things:

1. Resolves the current ``tenant_id`` via
   :func:`hydra.api.dependencies.get_current_tenant_id` — the same
   path EAS routers already use, so the X-API-Key header is authenticated
   exactly once per request.
2. Resolves the shared :class:`CostQuotaCounter` singleton from
   :func:`hydra.eas.dependencies.get_cost_quota_counter`.
3. Looks up the configured limit for ``quota_name`` on
   ``settings.eas.cost_quota`` (populated at startup from
   ``HydraSettings``).
4. Calls :meth:`CostQuotaCounter.increment_and_check`. On overage the
   counter raises :class:`HydraAPIException` with
   ``ErrorCode.COST_QUOTA_EXCEEDED`` and a ``retry_after`` detail that
   the global exception handler surfaces as the ``Retry-After`` header
   (R22.2). The dependency does not attempt to add the header itself —
   it delegates to the :mod:`hydra.api.errors` handler so that every
   quota-gated route produces a uniform 429 envelope.
"""

from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable
from uuid import UUID

from fastapi import Depends

from hydra.api.dependencies import get_current_tenant_id
from hydra.api.errors import ErrorCode, HydraAPIException
from hydra.eas.quota.counter import CostQuotaCounter

logger = logging.getLogger(__name__)

__all__ = ["enforce_cost_quota"]


def enforce_cost_quota(
    quota_name: str,
) -> Callable[..., Awaitable[None]]:
    """Return a FastAPI dependency enforcing ``quota_name`` per request.

    Parameters
    ----------
    quota_name:
        One of the fields on :class:`hydra.eas.settings.CostQuota`
        (``screenshots_per_day``, ``observatory_regenerations_per_day``,
        ``lookup_requests_per_day``, ``trends_points_per_day``,
        ``cve_correlations_per_day``). The same string is used as the
        Redis key segment and the ``quota_name`` label on
        :data:`hydra_eas_quota_usage_ratio`.

    Returns
    -------
    Callable
        A coroutine factory ready to use with ``Depends(...)``.

    Notes
    -----
    The returned dependency is resolved per-request — FastAPI passes
    the real :class:`CostQuotaCounter` and :class:`HydraSettings`
    through ``Depends`` so tests can override the getters on
    :mod:`hydra.eas.dependencies` without monkey-patching module state.
    """

    async def _dependency(
        tenant_id: UUID = Depends(get_current_tenant_id),
        counter: CostQuotaCounter | None = Depends(
            # Late import inside the Depends default because
            # ``hydra.eas.dependencies`` imports from this module's
            # parent package; doing the import at module load time
            # would create a partial-import cycle under ``setup_eas``.
            lambda: _resolve_counter()
        ),
        settings: Any = Depends(lambda: _resolve_settings()),
    ) -> None:
        # Defensive null checks — both getters can return None before
        # ``setup_eas`` has wired the singletons. Surface a 503 so the
        # client can retry once the EAS stack finishes booting, instead
        # of silently skipping quota enforcement.
        if counter is None:
            raise HydraAPIException(
                code=ErrorCode.SERVICE_UNAVAILABLE,
                message="Cost quota counter is not available",
                status_code=503,
            )

        limit = _lookup_limit(settings, quota_name)
        # ``increment_and_check`` raises ``HydraAPIException`` on
        # overage — let it propagate unchanged so the global handler
        # surfaces the 429 + Retry-After envelope per R22.2.
        await counter.increment_and_check(tenant_id, quota_name, limit)

    # Preserve a readable ``__name__`` so FastAPI's auto-generated
    # OpenAPI docs label the dependency by the quota it enforces.
    _dependency.__name__ = f"enforce_cost_quota[{quota_name}]"
    return _dependency


# ---------------------------------------------------------------------------
# Internal helpers — wrap the eas.dependencies getters at call time so that
# monkey-patched test overrides (which mutate the module-level globals on
# ``hydra.eas.dependencies``) are seen on each request.
# ---------------------------------------------------------------------------


async def _resolve_counter() -> CostQuotaCounter | None:
    # Local import keeps the dependency module free of eager coupling to
    # ``hydra.eas.dependencies`` at import time — the router layer
    # already imports this module during FastAPI app construction, so
    # any import-time cycle would surface as an obscure startup error.
    from hydra.eas.dependencies import get_cost_quota_counter

    return await get_cost_quota_counter()


async def _resolve_settings() -> Any:
    from hydra.eas.dependencies import get_eas_settings

    return await get_eas_settings()


def _lookup_limit(settings: Any, quota_name: str) -> int:
    """Extract ``settings.eas.cost_quota.{quota_name}`` as an int.

    Raises :class:`HydraAPIException` (503) if the quota name does not
    match a configured field — this is a deployment bug, not a client
    error, but the server keeps serving unrelated routes.
    """

    try:
        cost_quota = settings.eas.cost_quota
        value = getattr(cost_quota, quota_name)
    except AttributeError as exc:
        logger.error(
            "eas.quota.unknown_name",
            extra={"quota_name": quota_name, "error": str(exc)},
        )
        raise HydraAPIException(
            code=ErrorCode.SERVICE_UNAVAILABLE,
            message=f"Unknown cost quota: {quota_name}",
            status_code=503,
        ) from exc

    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        logger.error(
            "eas.quota.invalid_limit",
            extra={"quota_name": quota_name, "value": repr(value)},
        )
        raise HydraAPIException(
            code=ErrorCode.SERVICE_UNAVAILABLE,
            message=f"Invalid cost quota limit for {quota_name}",
            status_code=503,
        ) from exc
