"""Per-tenant cost quota counters (R22, Design §3.9).

Exports :class:`CostQuotaCounter` — the Redis-backed daily counter used
by the ``expensive`` rate-limit tier and by quota-gated routes (e.g.
``POST /api/v1/assets/{id}/screenshot``, ``POST /api/v1/cves/correlate``,
``POST /api/v1/observatory/generate``) per Design §2.4 and §3.9.

The :func:`enforce_cost_quota` FastAPI dependency factory is also
re-exported so routers can write::

    @router.post(
        "/api/v1/assets/{asset_id}/screenshot",
        dependencies=[Depends(enforce_cost_quota("screenshots_per_day"))],
    )
    ...

without importing the nested module.
"""

from __future__ import annotations

from hydra.eas.quota.counter import CostQuotaCounter
from hydra.eas.quota.dependency import enforce_cost_quota

__all__ = ["CostQuotaCounter", "enforce_cost_quota"]
