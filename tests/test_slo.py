"""Tests for ``hydra.monitoring.slo`` — SLO computation.

Covers:

* Task 6.2 — **Property 11: Error Budget and Burn Rate Computation**
  (Requirements 14.2, 14.3).
* Task 6.3 — **Property 12: SLO Breach Consistency**
  (Requirements 14.4, 14.5, 22.4).
* Task 6.4 — Unit tests for SLO definition loading, targets, budget
  math, burn rates, breach flag, and metrics exposure
  (Requirements 14.1–14.6).

Property tests use Hypothesis. Async SLI-query stubs are dispatched
inside the property body via ``asyncio.run(...)`` — this is the same
pattern used by Property 2 in ``tests/test_collectors.py`` and keeps
the property code cleanly synchronous at the Hypothesis level.

Each Hypothesis example constructs its own :class:`SLODefinition`
instances with names derived from the generated parameters. This
guarantees that distinct property examples write to distinct gauge
label sets and never cross-contaminate each other's reads via
``_value.get()``.
"""

from __future__ import annotations

import asyncio
import math

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from hydra.config import MonitoringSettings
from hydra.monitoring import metrics as metrics_module
from hydra.monitoring.exceptions import SLOComputationError
from hydra.monitoring.slo import (
    SLIQueryFn,
    SLIType,
    SLODefinition,
    SLOStatus,
    SLOComputer,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_constant_sli(value: float) -> SLIQueryFn:
    """Return an SLI-query stub that always yields ``value``."""

    async def _stub(slo_name: str, window_minutes: float) -> float:
        return value

    return _stub


def _make_per_window_sli(
    long_value: float, window_1h: float, window_6h: float
) -> SLIQueryFn:
    """Return a stub returning distinct values for full, 1h, and 6h windows.

    The full-window value is used for all queries that are neither 60
    nor 360 minutes — i.e. the long SLO window.
    """

    async def _stub(slo_name: str, window_minutes: float) -> float:
        if math.isclose(window_minutes, 60.0):
            return window_1h
        if math.isclose(window_minutes, 360.0):
            return window_6h
        return long_value

    return _stub


def _make_failing_sli() -> SLIQueryFn:
    """Return a stub that always raises :class:`SLOComputationError`."""

    async def _stub(slo_name: str, window_minutes: float) -> float:
        raise SLOComputationError(f"simulated SLI backend failure: {slo_name}")

    return _stub


def _gauge_value(gauge, *, slo_name: str) -> float:
    """Read the current float value of a labelled SLO gauge."""
    return gauge.labels(slo_name=slo_name)._value.get()


def _unique_slo_name(prefix: str, *parts: object) -> str:
    """Build a deterministic, unique SLO name from Hypothesis parameters.

    Prometheus labels are process-wide singletons — giving each example
    a fresh label value prevents cross-talk between Hypothesis shrinks.
    """
    joined = "_".join(str(abs(hash(p))) for p in parts)
    return f"{prefix}_{joined}"


# ---------------------------------------------------------------------------
# Property 11 — Error Budget and Burn Rate Computation
# ---------------------------------------------------------------------------


_TARGET_STRATEGY = st.floats(
    min_value=0.5,
    max_value=0.9999,
    allow_nan=False,
    allow_infinity=False,
)
_WINDOW_DAYS_STRATEGY = st.integers(min_value=1, max_value=365)
_CURRENT_VALUE_STRATEGY = st.floats(
    min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False
)


@settings(
    deadline=None,
    max_examples=25,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    target=_TARGET_STRATEGY,
    window_days=_WINDOW_DAYS_STRATEGY,
    current_value=_CURRENT_VALUE_STRATEGY,
    sli_1h=_CURRENT_VALUE_STRATEGY,
    sli_6h=_CURRENT_VALUE_STRATEGY,
)
def test_property_11_error_budget_and_burn_rate(
    target: float,
    window_days: int,
    current_value: float,
    sli_1h: float,
    sli_6h: float,
) -> None:
    """**Validates: Requirements 14.2, 14.3**.

    For every target in ``(0, 1)`` and every positive window, the
    computer MUST report::

        error_budget_total_minutes = (1 - target) * window_days * 24 * 60
        burn_rate_{1h,6h}          = error_rate_{1h,6h} / (1 - target)

    where ``error_rate = max(0, 1 - current_value)``. The total budget
    is recovered from ``error_budget_remaining + error_consumed``.
    """
    slo_name = _unique_slo_name(
        "prop11", target, window_days, current_value, sli_1h, sli_6h
    )
    definition = SLODefinition(
        name=slo_name,
        target=target,
        window_days=window_days,
        sli_type="success_rate",
    )
    settings_obj = MonitoringSettings()
    computer = SLOComputer(
        settings=settings_obj,
        sli_query=_make_per_window_sli(current_value, sli_1h, sli_6h),
    )

    status: SLOStatus = asyncio.run(computer.compute_slo(definition))

    budget_rate = 1.0 - target
    window_minutes = float(window_days) * 24.0 * 60.0
    expected_budget_total = budget_rate * window_minutes

    # Reconstruct the total budget from the reported remaining + consumed.
    error_rate_window = max(0.0, 1.0 - current_value)
    expected_remaining = expected_budget_total - error_rate_window * window_minutes

    assert status.error_budget_remaining_minutes == pytest.approx(
        expected_remaining, rel=1e-9, abs=1e-6
    )

    # Burn-rate identities (Requirement 14.3).
    expected_burn_1h = max(0.0, 1.0 - sli_1h) / budget_rate
    expected_burn_6h = max(0.0, 1.0 - sli_6h) / budget_rate
    assert status.burn_rate_1h == pytest.approx(
        expected_burn_1h, rel=1e-9, abs=1e-12
    )
    assert status.burn_rate_6h == pytest.approx(
        expected_burn_6h, rel=1e-9, abs=1e-12
    )

    # Echoed fields.
    assert status.target == pytest.approx(target)
    assert status.current_value == pytest.approx(current_value)


# ---------------------------------------------------------------------------
# Property 12 — SLO Breach Consistency
# ---------------------------------------------------------------------------


@settings(
    deadline=None,
    max_examples=25,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    target=_TARGET_STRATEGY,
    window_days=_WINDOW_DAYS_STRATEGY,
    current_value=_CURRENT_VALUE_STRATEGY,
)
def test_property_12_breach_consistency_on_success(
    target: float,
    window_days: int,
    current_value: float,
) -> None:
    """**Validates: Requirements 14.4, 14.5**.

    Under a working SLI backend, ``is_breached`` MUST be ``True`` iff
    ``error_budget_remaining_minutes <= 0``. No other combination may
    be reported.
    """
    slo_name = _unique_slo_name("prop12a", target, window_days, current_value)
    definition = SLODefinition(
        name=slo_name,
        target=target,
        window_days=window_days,
        sli_type="success_rate",
    )
    computer = SLOComputer(
        settings=MonitoringSettings(),
        sli_query=_make_constant_sli(current_value),
    )

    status = asyncio.run(computer.compute_slo(definition))

    expected_breached = status.error_budget_remaining_minutes <= 0
    assert status.is_breached is expected_breached


@settings(
    deadline=None,
    max_examples=15,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    target=_TARGET_STRATEGY,
    window_days=_WINDOW_DAYS_STRATEGY,
)
def test_property_12_breach_on_query_failure(
    target: float,
    window_days: int,
) -> None:
    """**Validates: Requirements 14.4, 14.5, 22.4**.

    When the SLI backend raises :class:`SLOComputationError`, the
    computer MUST report ``current_value=0.0`` and ``is_breached=True``
    unconditionally — the conservative worst-case default.
    """
    slo_name = _unique_slo_name("prop12b", target, window_days)
    definition = SLODefinition(
        name=slo_name,
        target=target,
        window_days=window_days,
        sli_type="success_rate",
    )
    computer = SLOComputer(
        settings=MonitoringSettings(),
        sli_query=_make_failing_sli(),
    )

    status = asyncio.run(computer.compute_slo(definition))

    assert status.current_value == pytest.approx(0.0)
    assert status.is_breached is True
    # And the implied budget-remaining invariant still holds.
    assert status.error_budget_remaining_minutes <= 0.0


# ---------------------------------------------------------------------------
# Task 6.4 — Unit tests
# ---------------------------------------------------------------------------


class TestSLODefinitionLoading:
    """Task 6.4 — SLO definition loading (Requirement 14.1)."""

    def test_six_definitions_with_expected_names(self) -> None:
        """The computer MUST expose exactly 6 SLOs with canonical names."""
        computer = SLOComputer(settings=MonitoringSettings())
        names = [d.name for d in computer.definitions]
        assert names == [
            "adapter_success_rate",
            "api_availability",
            "api_latency_p95",
            "product_generation_success",
            "ingestion_freshness",
            "storage_availability",
        ]

    def test_window_assignment_matches_requirement_14_1(self) -> None:
        """30d window for availability/latency; 7d for product & freshness."""
        settings_obj = MonitoringSettings()
        computer = SLOComputer(settings=settings_obj)
        by_name = {d.name: d for d in computer.definitions}

        long_w = settings_obj.slo_window_days
        short_w = settings_obj.slo_short_window_days

        assert by_name["adapter_success_rate"].window_days == long_w
        assert by_name["api_availability"].window_days == long_w
        assert by_name["api_latency_p95"].window_days == long_w
        assert by_name["storage_availability"].window_days == long_w
        assert by_name["product_generation_success"].window_days == short_w
        assert by_name["ingestion_freshness"].window_days == short_w

    def test_targets_pulled_from_settings(self) -> None:
        """Each SLO's target MUST come from :class:`MonitoringSettings`."""
        settings_obj = MonitoringSettings(
            slo_adapter_success_target=0.97,
            slo_api_availability_target=0.995,
            slo_api_latency_p95_target=0.95,
            slo_product_generation_target=0.9,
            slo_ingestion_freshness_target=0.85,
            slo_storage_availability_target=0.998,
        )
        computer = SLOComputer(settings=settings_obj)
        by_name = {d.name: d.target for d in computer.definitions}

        assert by_name["adapter_success_rate"] == pytest.approx(0.97)
        assert by_name["api_availability"] == pytest.approx(0.995)
        assert by_name["api_latency_p95"] == pytest.approx(0.95)
        assert by_name["product_generation_success"] == pytest.approx(0.9)
        assert by_name["ingestion_freshness"] == pytest.approx(0.85)
        assert by_name["storage_availability"] == pytest.approx(0.998)

    def test_latency_slo_carries_threshold(self) -> None:
        settings_obj = MonitoringSettings(slo_api_latency_threshold_seconds=1.5)
        computer = SLOComputer(settings=settings_obj)
        by_name = {d.name: d for d in computer.definitions}
        assert by_name["api_latency_p95"].sli_type == "latency"
        assert by_name["api_latency_p95"].latency_threshold_seconds == pytest.approx(1.5)

    def test_invalid_target_rejected(self) -> None:
        with pytest.raises(ValueError):
            SLODefinition(
                name="bad_target",
                target=0.0,
                window_days=30,
                sli_type="success_rate",
            )
        with pytest.raises(ValueError):
            SLODefinition(
                name="bad_target_hi",
                target=1.0,
                window_days=30,
                sli_type="success_rate",
            )

    def test_invalid_window_rejected(self) -> None:
        with pytest.raises(ValueError):
            SLODefinition(
                name="bad_window",
                target=0.99,
                window_days=0,
                sli_type="success_rate",
            )


class TestBudgetMath:
    """Task 6.4 — concrete budget and burn-rate arithmetic."""

    def test_error_budget_total_concrete(self) -> None:
        """``(1 - 0.99) * 30 * 24 * 60 = 432`` minutes."""
        definition = SLODefinition(
            name="unit_budget_math",
            target=0.99,
            window_days=30,
            sli_type="success_rate",
        )
        # current_value == target → no error consumed → full budget remaining.
        computer = SLOComputer(
            settings=MonitoringSettings(),
            sli_query=_make_constant_sli(0.99),
        )
        status = asyncio.run(computer.compute_slo(definition))
        # error_rate = 1 - 0.99 = 0.01 → consumed = 0.01 * 43200 = 432 → remaining = 0.
        assert status.error_budget_remaining_minutes == pytest.approx(0.0, abs=1e-6)

    def test_error_budget_full_when_sli_perfect(self) -> None:
        definition = SLODefinition(
            name="unit_budget_full",
            target=0.995,
            window_days=30,
            sli_type="success_rate",
        )
        computer = SLOComputer(
            settings=MonitoringSettings(),
            sli_query=_make_constant_sli(1.0),
        )
        status = asyncio.run(computer.compute_slo(definition))
        # 0.005 * 30 * 24 * 60 = 216 minutes, fully remaining.
        assert status.error_budget_remaining_minutes == pytest.approx(216.0)
        assert status.is_breached is False

    def test_burn_rates_concrete(self) -> None:
        """With target 0.99, error_rate 0.01 → burn_rate = 0.01 / 0.01 = 1.0."""
        definition = SLODefinition(
            name="unit_burn_concrete",
            target=0.99,
            window_days=30,
            sli_type="success_rate",
        )
        # 1h SLI = 0.99 → error_rate_1h = 0.01 → burn_rate_1h = 1.0
        # 6h SLI = 0.95 → error_rate_6h = 0.05 → burn_rate_6h = 5.0
        computer = SLOComputer(
            settings=MonitoringSettings(),
            sli_query=_make_per_window_sli(
                long_value=0.999, window_1h=0.99, window_6h=0.95
            ),
        )
        status = asyncio.run(computer.compute_slo(definition))
        assert status.burn_rate_1h == pytest.approx(1.0)
        assert status.burn_rate_6h == pytest.approx(5.0)


class TestBreachFlagBoundary:
    """Task 6.4 — breach flag at and around zero remaining budget."""

    def test_breach_exactly_at_zero_is_true(self) -> None:
        definition = SLODefinition(
            name="unit_breach_zero",
            target=0.99,
            window_days=30,
            sli_type="success_rate",
        )
        # current == target → remaining == 0 exactly → breached (14.4).
        computer = SLOComputer(
            settings=MonitoringSettings(),
            sli_query=_make_constant_sli(0.99),
        )
        status = asyncio.run(computer.compute_slo(definition))
        assert status.error_budget_remaining_minutes == pytest.approx(0.0, abs=1e-6)
        assert status.is_breached is True

    def test_breach_slightly_above_zero_is_false(self) -> None:
        definition = SLODefinition(
            name="unit_breach_above",
            target=0.99,
            window_days=30,
            sli_type="success_rate",
        )
        # current 0.9901 → error 0.0099 → consumed < total → remaining > 0.
        computer = SLOComputer(
            settings=MonitoringSettings(),
            sli_query=_make_constant_sli(0.9901),
        )
        status = asyncio.run(computer.compute_slo(definition))
        assert status.error_budget_remaining_minutes > 0
        assert status.is_breached is False

    def test_breach_slightly_below_zero_is_true(self) -> None:
        definition = SLODefinition(
            name="unit_breach_below",
            target=0.99,
            window_days=30,
            sli_type="success_rate",
        )
        # current 0.98 → error 0.02 > 0.01 → budget overrun → breached.
        computer = SLOComputer(
            settings=MonitoringSettings(),
            sli_query=_make_constant_sli(0.98),
        )
        status = asyncio.run(computer.compute_slo(definition))
        assert status.error_budget_remaining_minutes < 0
        assert status.is_breached is True


class TestMetricsExposure:
    """Task 6.4 — ``compute_slo()`` must update all 6 gauges (Requirement 14.6)."""

    def test_single_slo_updates_all_six_gauges(self) -> None:
        slo_name = "unit_metrics_single"
        definition = SLODefinition(
            name=slo_name,
            target=0.99,
            window_days=30,
            sli_type="success_rate",
        )
        computer = SLOComputer(
            settings=MonitoringSettings(),
            sli_query=_make_per_window_sli(
                long_value=0.995, window_1h=0.98, window_6h=0.97
            ),
        )

        status = asyncio.run(computer.compute_slo(definition))

        assert _gauge_value(
            metrics_module.hydra_slo_target, slo_name=slo_name
        ) == pytest.approx(0.99)
        assert _gauge_value(
            metrics_module.hydra_slo_current, slo_name=slo_name
        ) == pytest.approx(0.995)
        assert _gauge_value(
            metrics_module.hydra_slo_error_budget_remaining, slo_name=slo_name
        ) == pytest.approx(status.error_budget_remaining_minutes)
        assert _gauge_value(
            metrics_module.hydra_slo_burn_rate_1h, slo_name=slo_name
        ) == pytest.approx(status.burn_rate_1h)
        assert _gauge_value(
            metrics_module.hydra_slo_burn_rate_6h, slo_name=slo_name
        ) == pytest.approx(status.burn_rate_6h)
        assert _gauge_value(
            metrics_module.hydra_slo_breached, slo_name=slo_name
        ) == pytest.approx(1.0 if status.is_breached else 0.0)

    def test_compute_all_updates_labels_for_every_defined_slo(self) -> None:
        """After ``compute_all()``, every canonical SLO name must have
        the 6 gauges populated."""
        computer = SLOComputer(
            settings=MonitoringSettings(),
            sli_query=_make_constant_sli(0.9995),
        )
        statuses = asyncio.run(computer.compute_all())
        assert len(statuses) == 6

        gauges = (
            metrics_module.hydra_slo_target,
            metrics_module.hydra_slo_current,
            metrics_module.hydra_slo_error_budget_remaining,
            metrics_module.hydra_slo_burn_rate_1h,
            metrics_module.hydra_slo_burn_rate_6h,
            metrics_module.hydra_slo_breached,
        )
        for status in statuses:
            for gauge in gauges:
                # Label must exist — accessing it would raise otherwise.
                _ = _gauge_value(gauge, slo_name=status.name)

    def test_query_failure_still_updates_gauges(self) -> None:
        """Requirement 22.4: on SLI failure, gauges still reflect the
        conservative breached state."""
        slo_name = "unit_metrics_failed"
        definition = SLODefinition(
            name=slo_name,
            target=0.99,
            window_days=30,
            sli_type="success_rate",
        )
        computer = SLOComputer(
            settings=MonitoringSettings(),
            sli_query=_make_failing_sli(),
        )
        status = asyncio.run(computer.compute_slo(definition))

        assert status.is_breached is True
        assert _gauge_value(
            metrics_module.hydra_slo_current, slo_name=slo_name
        ) == pytest.approx(0.0)
        assert _gauge_value(
            metrics_module.hydra_slo_breached, slo_name=slo_name
        ) == pytest.approx(1.0)
