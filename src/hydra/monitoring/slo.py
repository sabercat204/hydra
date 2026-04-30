"""SLO computation — error budgets and burn rates (P12 §8).

This module implements :class:`SLOComputer`, which produces
:class:`SLOStatus` values for the 6 HYDRA SLOs defined in design.md
§"Component 8: SLOComputer" and Requirement 14.1:

1. ``adapter_success_rate``      — success rate target (30d window)
2. ``api_availability``          — availability target (30d window)
3. ``api_latency_p95``           — latency target (30d window)
4. ``product_generation_success`` — success rate target (7d window)
5. ``ingestion_freshness``       — freshness target (7d window)
6. ``storage_availability``      — availability target (30d window)

Targets and window lengths are sourced from
:class:`hydra.config.MonitoringSettings`. The underlying SLI (Service
Level Indicator) values are obtained via an abstract callable injected
at construction time so the computer remains decoupled from the specific
data source (Prometheus HTTP API, recording rules, direct PostgreSQL
queries, or a test stub). See :class:`SLIQueryFn`.

Math (design §"Algorithm 5", Requirements 14.2–14.5)::

    window_minutes            = window_days * 24 * 60
    error_budget_total_min    = (1 - target) * window_minutes
    error_rate                = max(0.0, 1.0 - current_value)
    burn_rate                 = error_rate / (1 - target)       # if (1-target) > 0
    error_budget_remaining    = error_budget_total_min - error_rate * window_minutes
    is_breached               = error_budget_remaining <= 0

On SLI query failure, the computer reports a conservative worst-case
status (``current_value=0.0``, ``is_breached=True``) rather than
propagating the exception — see Requirement 22.4 and Property 12. The
underlying :class:`SLOComputationError` is logged internally.

Requirements: 14.1, 14.2, 14.3, 14.4, 14.5, 14.6, 22.4.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal, Protocol

from hydra.config import MonitoringSettings
from hydra.monitoring.exceptions import SLOComputationError
from hydra.monitoring.metrics import (
    hydra_slo_breached,
    hydra_slo_burn_rate_1h,
    hydra_slo_burn_rate_6h,
    hydra_slo_current,
    hydra_slo_error_budget_remaining,
    hydra_slo_target,
)

logger = logging.getLogger(__name__)


#: Supported SLI kinds. The kind determines the semantic meaning of
#: ``current_value`` (e.g., a success rate vs. a latency-compliance
#: fraction) but the error-budget math is identical across kinds.
SLIType = Literal["success_rate", "availability", "latency", "freshness"]


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SLODefinition:
    """Declarative description of a single SLO.

    Attributes:
        name: Unique SLO identifier, used as the ``slo_name`` label on
            ``hydra_slo_*`` metrics (e.g., ``"adapter_success_rate"``).
        target: Target SLI value in the open interval ``(0.0, 1.0)``,
            e.g. ``0.995`` for 99.5%. Validated at construction.
        window_days: Rolling evaluation window in days. Must be positive.
        sli_type: One of :data:`SLIType`.
        description: Human-readable description of what the SLO measures.
        latency_threshold_seconds: Only meaningful when
            ``sli_type == "latency"`` — the latency above which a request
            is considered to violate the SLO. ``None`` for non-latency
            SLOs.
    """

    name: str
    target: float
    window_days: int
    sli_type: SLIType
    description: str = ""
    latency_threshold_seconds: float | None = None

    def __post_init__(self) -> None:
        if not 0.0 < self.target < 1.0:
            raise ValueError(
                f"SLO target must be in the open interval (0, 1); got {self.target!r}"
            )
        if self.window_days <= 0:
            raise ValueError(
                f"SLO window_days must be positive; got {self.window_days!r}"
            )
        if self.sli_type == "latency" and self.latency_threshold_seconds is None:
            raise ValueError(
                "latency_threshold_seconds is required when sli_type='latency'"
            )

    @property
    def window_minutes(self) -> float:
        """Window length in minutes (``window_days * 24 * 60``)."""
        return float(self.window_days) * 24.0 * 60.0


@dataclass(frozen=True)
class SLOStatus:
    """Computed SLO state for a single evaluation cycle.

    Attributes:
        name: SLO identifier matching the corresponding :class:`SLODefinition`.
        target: The target value copied from the definition (for
            convenience in downstream serialization).
        current_value: Current SLI value for the SLO window, in
            ``[0.0, 1.0]``. ``0.0`` when the SLI query failed (see
            Requirement 22.4).
        error_budget_remaining_minutes: Remaining error budget expressed
            in minutes. May be negative if the budget has been exceeded
            (breached SLO) — the metric is *not* clamped at zero so that
            dashboards can visualize the magnitude of the overrun.
        burn_rate_1h: 1-hour burn rate = ``error_rate_1h / (1 - target)``.
            A value of 1.0 means the budget is being consumed at exactly
            the rate that would exhaust it over the SLO window.
        burn_rate_6h: 6-hour burn rate, same formula over a 6-hour window.
        is_breached: ``True`` iff ``error_budget_remaining_minutes <= 0``
            (Requirement 14.4). Also forced ``True`` on SLI query failure.
        computed_at: UTC timestamp of the computation.
    """

    name: str
    target: float
    current_value: float
    error_budget_remaining_minutes: float
    burn_rate_1h: float
    burn_rate_6h: float
    is_breached: bool
    computed_at: datetime = field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )


# ---------------------------------------------------------------------------
# SLI query interface
# ---------------------------------------------------------------------------


class SLIQueryFn(Protocol):
    """Abstract callable returning a current SLI value for a given window.

    Implementations MAY query Prometheus, recording rules, PostgreSQL,
    or any other backend. They receive the SLO name and the window in
    minutes (short windows for burn-rate computations, long windows for
    the headline SLI). Return values MUST be in ``[0.0, 1.0]``.

    The interface is intentionally minimal so the :class:`SLOComputer`
    can be unit-tested with a deterministic stub (see tests 6.2–6.4).
    """

    async def __call__(self, slo_name: str, window_minutes: float) -> float: ...


async def _default_sli_query(slo_name: str, window_minutes: float) -> float:
    """Default SLI-query stub used when no callable is injected.

    The stub raises :class:`SLOComputationError` for every request,
    causing :meth:`SLOComputer.compute_slo` to fall back to the
    conservative worst-case status (``current_value=0.0``,
    ``is_breached=True``). This matches Requirement 22.4 and ensures
    that wiring the SLOComputer without a real backend does not silently
    report perfect health.
    """
    raise SLOComputationError(
        f"No SLI query backend configured for slo={slo_name!r} "
        f"window_minutes={window_minutes!r}"
    )


# ---------------------------------------------------------------------------
# SLOComputer
# ---------------------------------------------------------------------------


# 1h and 6h burn-rate windows expressed in minutes.
_BURN_RATE_1H_MINUTES: float = 60.0
_BURN_RATE_6H_MINUTES: float = 6.0 * 60.0


class SLOComputer:
    """Compute SLO status for the 6 HYDRA SLOs from a live SLI backend.

    The computer owns the static list of :class:`SLODefinition` objects
    derived from :class:`~hydra.config.MonitoringSettings` and exposes
    :meth:`compute_slo` / :meth:`compute_all` to produce
    :class:`SLOStatus` values. After each computation it updates the
    six ``hydra_slo_*`` Prometheus gauges so downstream alert rules and
    Grafana panels see the latest state.

    The SLI backend is injected as an ``async`` callable matching
    :class:`SLIQueryFn`. Tests supply a deterministic stub; production
    wiring will pass a Prometheus HTTP client adapter.
    """

    def __init__(
        self,
        settings: MonitoringSettings,
        sli_query: SLIQueryFn | None = None,
    ) -> None:
        """Create the computer.

        Args:
            settings: Monitoring configuration providing SLO targets and
                window lengths (Requirement 21.4).
            sli_query: Optional callable supplying current SLI values. If
                ``None``, a stub that always raises
                :class:`SLOComputationError` is used — every computation
                will then fall back to the conservative breached status,
                which is the correct default for an un-wired computer
                (Requirement 22.4).
        """
        self._settings: MonitoringSettings = settings
        self._sli_query: SLIQueryFn = sli_query or _default_sli_query
        self._definitions: tuple[SLODefinition, ...] = self._build_definitions(settings)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def definitions(self) -> tuple[SLODefinition, ...]:
        """The 6 SLO definitions configured on this computer."""
        return self._definitions

    async def compute_all(self) -> list[SLOStatus]:
        """Compute status for every defined SLO.

        Failures in individual SLI queries do not abort the batch — each
        failing SLO is represented by a conservative breached status
        (see :meth:`compute_slo`).

        Returns:
            List of :class:`SLOStatus` in the same order as
            :attr:`definitions`.
        """
        statuses: list[SLOStatus] = []
        for definition in self._definitions:
            status = await self.compute_slo(definition)
            statuses.append(status)
        return statuses

    async def compute_slo(self, slo: SLODefinition) -> SLOStatus:
        """Compute :class:`SLOStatus` for a single SLO.

        Algorithm (Requirements 14.2–14.5):

        1. Query the current SLI value for the full SLO window.
        2. Query short-window SLI values for the 1h and 6h burn rates.
        3. Derive the error budget and burn rates using the formulas in
           the module docstring.
        4. Update the ``hydra_slo_*`` gauges.

        Any :class:`SLOComputationError` raised by the SLI backend
        (including by the default stub) is caught and logged; the
        resulting status reports ``current_value=0.0`` and
        ``is_breached=True`` as the conservative worst-case (Requirement
        22.4). Other unexpected exceptions are also caught and handled
        the same way — an SLO computation failure must never propagate
        out of the monitoring subsystem.

        Args:
            slo: The SLO definition to evaluate.

        Returns:
            :class:`SLOStatus` with the computation outcome. The method
            always returns a status; it never raises.
        """
        target = slo.target
        budget_rate = 1.0 - target  # strictly > 0 by SLODefinition invariant
        window_minutes = slo.window_minutes
        error_budget_total = budget_rate * window_minutes

        current_value: float
        error_rate_window: float
        error_rate_1h: float
        error_rate_6h: float

        try:
            current_value = await self._sli_query(slo.name, window_minutes)
            error_rate_window = max(0.0, 1.0 - current_value)

            sli_1h = await self._sli_query(slo.name, _BURN_RATE_1H_MINUTES)
            sli_6h = await self._sli_query(slo.name, _BURN_RATE_6H_MINUTES)
            error_rate_1h = max(0.0, 1.0 - sli_1h)
            error_rate_6h = max(0.0, 1.0 - sli_6h)
        except SLOComputationError as exc:
            logger.error(
                "SLI query failed for SLO %s; reporting conservative breach: %s",
                slo.name,
                exc,
            )
            status = self._build_failed_status(slo, error_budget_total)
            self._update_metrics(slo, status)
            return status
        except Exception as exc:  # noqa: BLE001 — monitoring must never crash
            logger.error(
                "Unexpected error querying SLI for SLO %s; reporting conservative breach: %s",
                slo.name,
                exc,
                exc_info=True,
            )
            status = self._build_failed_status(slo, error_budget_total)
            self._update_metrics(slo, status)
            return status

        # Success path: compute budgets and burn rates.
        error_consumed_minutes = error_rate_window * window_minutes
        error_budget_remaining_minutes = error_budget_total - error_consumed_minutes

        burn_rate_1h = error_rate_1h / budget_rate
        burn_rate_6h = error_rate_6h / budget_rate

        is_breached = error_budget_remaining_minutes <= 0

        status = SLOStatus(
            name=slo.name,
            target=target,
            current_value=current_value,
            error_budget_remaining_minutes=error_budget_remaining_minutes,
            burn_rate_1h=burn_rate_1h,
            burn_rate_6h=burn_rate_6h,
            is_breached=is_breached,
        )
        self._update_metrics(slo, status)
        return status

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_definitions(
        settings: MonitoringSettings,
    ) -> tuple[SLODefinition, ...]:
        """Derive the 6 SLO definitions from :class:`MonitoringSettings`.

        Window assignments match Requirement 14.1:

        * 30-day window: adapter_success_rate, api_availability,
          api_latency_p95, storage_availability
        * 7-day window: product_generation_success, ingestion_freshness
        """
        long_window = settings.slo_window_days
        short_window = settings.slo_short_window_days
        return (
            SLODefinition(
                name="adapter_success_rate",
                target=settings.slo_adapter_success_target,
                window_days=long_window,
                sli_type="success_rate",
                description="Fraction of adapter fetches that succeed.",
            ),
            SLODefinition(
                name="api_availability",
                target=settings.slo_api_availability_target,
                window_days=long_window,
                sli_type="availability",
                description="Fraction of API requests returning non-5xx.",
            ),
            SLODefinition(
                name="api_latency_p95",
                target=settings.slo_api_latency_p95_target,
                window_days=long_window,
                sli_type="latency",
                description=(
                    "Fraction of API requests with p95 latency below "
                    f"{settings.slo_api_latency_threshold_seconds}s."
                ),
                latency_threshold_seconds=settings.slo_api_latency_threshold_seconds,
            ),
            SLODefinition(
                name="product_generation_success",
                target=settings.slo_product_generation_target,
                window_days=short_window,
                sli_type="success_rate",
                description="Fraction of intelligence product jobs that succeed.",
            ),
            SLODefinition(
                name="ingestion_freshness",
                target=settings.slo_ingestion_freshness_target,
                window_days=short_window,
                sli_type="freshness",
                description="Fraction of streams meeting their freshness SLA.",
            ),
            SLODefinition(
                name="storage_availability",
                target=settings.slo_storage_availability_target,
                window_days=long_window,
                sli_type="availability",
                description="Fraction of storage-engine health checks returning OK.",
            ),
        )

    @staticmethod
    def _build_failed_status(
        slo: SLODefinition, error_budget_total: float
    ) -> SLOStatus:
        """Build the conservative status reported when the SLI query fails.

        Per Requirement 22.4, a failed SLI query must be treated as a
        breach: ``current_value`` is zero, the error budget is fully
        consumed (``remaining = -error_budget_total`` so dashboards can
        see it clearly negative), and burn rates are set to
        ``1 / (1 - target)`` — the worst case corresponding to a 0%
        current SLI.
        """
        budget_rate = 1.0 - slo.target  # > 0 by invariant
        worst_burn_rate = 1.0 / budget_rate
        return SLOStatus(
            name=slo.name,
            target=slo.target,
            current_value=0.0,
            # Fully exhausted plus overrun by one full window's worth of
            # budget — signals "breached" without ambiguity on dashboards.
            error_budget_remaining_minutes=-error_budget_total,
            burn_rate_1h=worst_burn_rate,
            burn_rate_6h=worst_burn_rate,
            is_breached=True,
        )

    @staticmethod
    def _update_metrics(slo: SLODefinition, status: SLOStatus) -> None:
        """Push the computed status into the ``hydra_slo_*`` gauges.

        Satisfies Requirement 14.6. The gauges are labelled with the SLO
        name so dashboards can filter per-SLO.
        """
        labels = {"slo_name": slo.name}
        hydra_slo_target.labels(**labels).set(slo.target)
        hydra_slo_current.labels(**labels).set(status.current_value)
        hydra_slo_error_budget_remaining.labels(**labels).set(
            status.error_budget_remaining_minutes
        )
        hydra_slo_burn_rate_1h.labels(**labels).set(status.burn_rate_1h)
        hydra_slo_burn_rate_6h.labels(**labels).set(status.burn_rate_6h)
        hydra_slo_breached.labels(**labels).set(1.0 if status.is_breached else 0.0)


__all__ = [
    "SLIType",
    "SLIQueryFn",
    "SLODefinition",
    "SLOStatus",
    "SLOComputer",
]
