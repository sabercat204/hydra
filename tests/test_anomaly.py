"""Tests for ``hydra.monitoring.anomaly`` — statistical anomaly detection.

Covers:

* Task 7.2 — **Property 3: Anomaly Detection Minimum Samples Guard**
  (Requirements 9.3, 10.2, 10.4).
* Task 7.3 — **Property 4: Z-Score Zero Variance Safety**
  (Requirement 9.4).
* Task 7.4 — **Property 5: Z-Score Anomaly Flag Consistency**
  (Requirements 9.5, 9.6).
* Task 7.5 — **Property 6: EWMA Alpha Computation**
  (Requirement 10.1).
* Task 7.6 — **Property 7: EWMA Deviation Detection**
  (Requirement 10.3).
* Task 7.7 — **Property 8: History Window Bounded**
  (Requirements 9.2, 11.1).
* Task 7.8 — Unit tests for z-score math, EWMA math, min-samples guard,
  zero-stdev guard, flag set/clear, metrics update, and history bounds
  (Requirements 9.1–9.6, 10.1–10.4, 11.1–11.2).

The public detection math (:func:`AnomalyDetector.zscore`,
:func:`AnomalyDetector.ewma`) is a pair of pure static methods, so the
property tests drive them directly without any I/O or pool mocking.
Property 8 (history bounds) exercises the ``_evaluate()`` path and
therefore needs a detector *instance*; we construct one with a
``MagicMock`` pool — ``_evaluate()`` never touches the pool since it is
invoked synchronously by the property body (the pool is only consulted
in ``collect()``).

As in ``tests/test_slo.py``, each Hypothesis example uses a unique
``pipeline_id`` label derived from a hash of the generated inputs so
that concurrent examples cannot cross-contaminate gauge reads via
``_value.get()``.
"""

from __future__ import annotations

import math
from collections import deque
from unittest.mock import MagicMock

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from hydra.config import MonitoringSettings
from hydra.monitoring import metrics as metrics_module
from hydra.monitoring.anomaly import (
    MIN_SAMPLES,
    AnomalyDetector,
)
from hydra.monitoring.anomaly import (
    _DETECTOR_CONFIDENCE_EWMA,
    _DETECTOR_CONFIDENCE_ZSCORE,
    _DETECTOR_VOLUME_EWMA,
    _DETECTOR_VOLUME_ZSCORE,
    _KIND_CONFIDENCE,
    _KIND_VOLUME,
)


# ---------------------------------------------------------------------------
# Shared Hypothesis strategies
# ---------------------------------------------------------------------------

_FLOAT_STRATEGY = st.floats(
    min_value=-1e6,
    max_value=1e6,
    allow_nan=False,
    allow_infinity=False,
    allow_subnormal=False,
)
_THRESHOLD_STRATEGY = st.floats(
    min_value=0.5, max_value=10.0, allow_nan=False, allow_infinity=False
)
# span >= 2 so that alpha = 2/(span+1) is strictly less than 1. (For
# span == 1, alpha == 1.0 which still satisfies Requirement 10.1 but
# collapses the EWMA baseline to the current value — a degenerate
# detector that is out of scope for Property 6's (0,1) assertion.)
_SPAN_STRATEGY = st.integers(min_value=2, max_value=500)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _unique_label(prefix: str, *parts: object) -> str:
    """Build a deterministic, unique pipeline-id from Hypothesis parameters.

    Prometheus gauge labels are process-wide singletons — giving each
    example a fresh label value avoids cross-talk between shrinks.
    """
    joined = "_".join(str(abs(hash(p))) for p in parts)
    return f"{prefix}_{joined}"


def _gauge_flag(detector_label: str, pipeline_id: str) -> float:
    """Read the current value of ``hydra_anomaly_flag`` for a label pair."""
    return metrics_module.hydra_anomaly_flag.labels(
        detector=detector_label, pipeline_id=pipeline_id
    )._value.get()


def _volume_zscore_gauge(pipeline_id: str) -> float:
    """Read the correlation-volume z-score gauge."""
    return metrics_module.hydra_anomaly_correlation_volume_zscore.labels(
        pipeline_id=pipeline_id
    )._value.get()


def _confidence_zscore_gauge(pipeline_id: str) -> float:
    """Read the confidence-drift z-score gauge."""
    return metrics_module.hydra_anomaly_confidence_drift_zscore.labels(
        pipeline_id=pipeline_id
    )._value.get()


def _make_detector(
    *,
    window_size: int = 288,
    zscore_threshold: float = 3.0,
    ewma_span: int = 24,
) -> AnomalyDetector:
    """Build an :class:`AnomalyDetector` with a mock pool.

    ``_evaluate()`` and ``_get_history()`` never consult the pool, so
    a bare :class:`MagicMock` is sufficient for tests that exercise the
    history/gauge path without running ``collect()``.
    """
    settings_obj = MonitoringSettings(
        anomaly_window_size=window_size,
        anomaly_zscore_threshold=zscore_threshold,
        anomaly_ewma_span=ewma_span,
    )
    return AnomalyDetector(pg_pool=MagicMock(), settings=settings_obj)


# ---------------------------------------------------------------------------
# Property 3 — Anomaly Detection Minimum Samples Guard
# ---------------------------------------------------------------------------


@settings(
    deadline=None,
    max_examples=25,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    history=st.lists(_FLOAT_STRATEGY, min_size=0, max_size=MIN_SAMPLES - 1),
    current=_FLOAT_STRATEGY,
    threshold=_THRESHOLD_STRATEGY,
    span=_SPAN_STRATEGY,
)
def test_property_3_min_samples_guard(
    history: list[float],
    current: float,
    threshold: float,
    span: int,
) -> None:
    """**Validates: Requirements 9.3, 10.2, 10.4**.

    For any rolling history with fewer than :data:`MIN_SAMPLES` (30)
    observations, both detectors MUST short-circuit to ``(0.0, False)``
    regardless of the current value, threshold, or span. This is the
    safety guard that prevents spurious flags during warm-up.
    """
    assert len(history) < MIN_SAMPLES  # strategy invariant

    z_value, z_flag = AnomalyDetector.zscore(history, current, threshold)
    assert z_value == 0.0
    assert z_flag is False

    ewma_value, ewma_flag = AnomalyDetector.ewma(
        history, current, span, threshold
    )
    assert ewma_value == 0.0
    assert ewma_flag is False


# ---------------------------------------------------------------------------
# Property 4 — Z-Score Zero Variance Safety
# ---------------------------------------------------------------------------


@settings(
    deadline=None,
    max_examples=25,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    # Any finite, non-NaN float — the zero-variance guard must hold at
    # all magnitudes now that the implementation short-circuits on
    # exact-constant histories and uses ``statistics.pvariance`` for
    # numerical stability.
    constant=_FLOAT_STRATEGY,
    length=st.integers(min_value=MIN_SAMPLES, max_value=200),
    current=_FLOAT_STRATEGY,
    threshold=_THRESHOLD_STRATEGY,
)
def test_property_4_zscore_zero_variance_safety(
    constant: float,
    length: int,
    current: float,
    threshold: float,
) -> None:
    """**Validates: Requirement 9.4**.

    When every entry in the rolling history is identical, the
    population standard deviation is zero and the z-score calculation
    would divide by zero. The detector MUST return ``(0.0, False)`` —
    never raise, never flag.
    """
    history = [constant] * length

    z_value, z_flag = AnomalyDetector.zscore(history, current, threshold)

    assert z_value == 0.0
    assert z_flag is False


# ---------------------------------------------------------------------------
# Property 5 — Z-Score Anomaly Flag Consistency
# ---------------------------------------------------------------------------


@settings(
    deadline=None,
    max_examples=25,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    history=st.lists(_FLOAT_STRATEGY, min_size=MIN_SAMPLES, max_size=200),
    current=_FLOAT_STRATEGY,
    threshold=_THRESHOLD_STRATEGY,
)
def test_property_5_zscore_flag_consistency(
    history: list[float],
    current: float,
    threshold: float,
) -> None:
    """**Validates: Requirements 9.5, 9.6**.

    Given ≥30 history points with non-zero population stdev, the
    returned flag MUST equal ``abs(z) > threshold`` exactly — no
    hysteresis, no off-by-one. The z-score itself MUST satisfy
    ``z = (current - mean) / stdev``.
    """
    n = len(history)
    mean = sum(history) / n
    variance = sum((x - mean) ** 2 for x in history) / n
    stdev = math.sqrt(variance)

    # Skip constant histories here — covered exhaustively by Property 4.
    if stdev == 0.0:
        return

    z_value, z_flag = AnomalyDetector.zscore(history, current, threshold)

    expected_z = (current - mean) / stdev
    assert z_value == pytest.approx(expected_z, rel=1e-9, abs=1e-9)
    assert z_flag is (abs(expected_z) > threshold)


# ---------------------------------------------------------------------------
# Property 6 — EWMA Alpha Computation
# ---------------------------------------------------------------------------


@settings(
    deadline=None,
    max_examples=25,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    span=_SPAN_STRATEGY,
    history=st.lists(_FLOAT_STRATEGY, min_size=MIN_SAMPLES, max_size=120),
    current=_FLOAT_STRATEGY,
    threshold=_THRESHOLD_STRATEGY,
)
def test_property_6_ewma_alpha_identity(
    span: int,
    history: list[float],
    current: float,
    threshold: float,
) -> None:
    """**Validates: Requirement 10.1**.

    For any positive span, the smoothing factor MUST be
    ``alpha = 2 / (span + 1)`` with ``alpha ∈ (0, 1)``. We verify this
    indirectly by reconstructing the baseline with the documented alpha
    and checking that ``current - baseline`` matches the deviation
    returned by :func:`AnomalyDetector.ewma`.
    """
    alpha = 2.0 / (span + 1)
    # Algebraic identity for any positive integer span.
    assert 0.0 < alpha < 1.0

    mean = sum(history) / len(history)
    variance = sum((x - mean) ** 2 for x in history) / len(history)
    stdev = math.sqrt(variance)

    if stdev == 0.0:
        # Zero-variance case is the dedicated Requirement 10.2 path;
        # Property 4 covers it for z-score and the EWMA guard is
        # exercised by the unit tests below.
        return

    # Reconstruct the EWMA baseline with the documented alpha.
    baseline = history[0]
    for x in history[1:]:
        baseline = alpha * x + (1.0 - alpha) * baseline

    expected_deviation = current - baseline
    deviation, _flag = AnomalyDetector.ewma(history, current, span, threshold)

    assert deviation == pytest.approx(
        expected_deviation, rel=1e-9, abs=1e-9
    )


def test_property_6_ewma_rejects_non_positive_span() -> None:
    """**Validates: Requirement 10.1** — the ``span > 0`` precondition.

    Non-positive spans would make ``alpha = 2/(span+1)`` ill-defined
    (zero, negative, or undefined at ``span == -1``). The detector
    MUST raise :class:`ValueError`.
    """
    history = [1.0] * MIN_SAMPLES
    with pytest.raises(ValueError):
        AnomalyDetector.ewma(history, current=1.0, span=0, threshold=3.0)
    with pytest.raises(ValueError):
        AnomalyDetector.ewma(history, current=1.0, span=-5, threshold=3.0)


# ---------------------------------------------------------------------------
# Property 7 — EWMA Deviation Detection
# ---------------------------------------------------------------------------


@settings(
    deadline=None,
    max_examples=25,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    span=_SPAN_STRATEGY,
    history=st.lists(_FLOAT_STRATEGY, min_size=MIN_SAMPLES, max_size=120),
    current=_FLOAT_STRATEGY,
    threshold=_THRESHOLD_STRATEGY,
)
def test_property_7_ewma_deviation_flag(
    span: int,
    history: list[float],
    current: float,
    threshold: float,
) -> None:
    """**Validates: Requirement 10.3**.

    With ≥30 history points and non-zero population stdev, the EWMA
    flag MUST be set iff ``|current - ewma| / stdev > threshold``.
    """
    n = len(history)
    mean = sum(history) / n
    variance = sum((x - mean) ** 2 for x in history) / n
    stdev = math.sqrt(variance)
    if stdev == 0.0:
        return  # dedicated guard path — covered by unit tests

    alpha = 2.0 / (span + 1)
    baseline = history[0]
    for x in history[1:]:
        baseline = alpha * x + (1.0 - alpha) * baseline

    expected_deviation = current - baseline
    expected_flag = abs(expected_deviation) / stdev > threshold

    deviation, flag = AnomalyDetector.ewma(history, current, span, threshold)

    assert deviation == pytest.approx(
        expected_deviation, rel=1e-9, abs=1e-9
    )
    assert flag is expected_flag


# ---------------------------------------------------------------------------
# Property 8 — History Window Bounded
# ---------------------------------------------------------------------------


@settings(
    deadline=None,
    max_examples=25,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    window_size=st.integers(min_value=10, max_value=100),
    extra=st.integers(min_value=0, max_value=200),
    seed=st.integers(min_value=0, max_value=10_000),
)
def test_property_8_history_window_bounded(
    window_size: int,
    extra: int,
    seed: int,
) -> None:
    """**Validates: Requirements 9.2, 11.1**.

    Regardless of how many values are appended via ``_evaluate()``, the
    rolling history MUST NEVER exceed ``window_size`` — the detector
    uses ``deque(maxlen=window_size)`` under the hood and eviction is
    automatic.
    """
    appends = window_size + extra
    pipeline_id = _unique_label("prop8", window_size, extra, seed)

    detector = _make_detector(window_size=window_size)

    for i in range(appends):
        detector._evaluate(
            metric_kind=_KIND_VOLUME,
            pipeline_id=pipeline_id,
            current_value=float(i),
            zscore_gauge_detector=_DETECTOR_VOLUME_ZSCORE,
            ewma_detector=_DETECTOR_VOLUME_EWMA,
        )

    history = detector._get_history(_KIND_VOLUME, pipeline_id)
    assert len(history) <= window_size
    # After more than window_size appends, the deque should be full.
    assert len(history) == min(appends, window_size)


# ---------------------------------------------------------------------------
# Task 7.8 — Unit tests
# ---------------------------------------------------------------------------


class TestZScoreMath:
    """Concrete z-score arithmetic (Requirements 9.1, 9.5, 9.6)."""

    def test_zscore_identity_at_mean(self) -> None:
        """When ``current == mean``, z-score is 0 and flag is False."""
        history = [10.0] * (MIN_SAMPLES - 1) + [20.0]
        mean = sum(history) / len(history)
        z, flag = AnomalyDetector.zscore(history, mean, threshold=3.0)
        assert z == pytest.approx(0.0, abs=1e-12)
        assert flag is False

    def test_zscore_one_stdev_above_mean(self) -> None:
        """Known-inputs check: alternating 0/2 → mean=1, stdev=1, z=1."""
        history = [0.0, 2.0] * (MIN_SAMPLES // 2)  # 30 values, mean 1, stdev 1
        z, flag = AnomalyDetector.zscore(history, current=2.0, threshold=3.0)
        assert z == pytest.approx(1.0, rel=1e-9)
        assert flag is False

    def test_zscore_flags_when_above_threshold(self) -> None:
        """|z| > threshold → flag True."""
        history = [0.0, 2.0] * (MIN_SAMPLES // 2)  # mean 1, stdev 1
        # current=5 → z=(5-1)/1=4 > threshold=3
        z, flag = AnomalyDetector.zscore(history, current=5.0, threshold=3.0)
        assert z == pytest.approx(4.0)
        assert flag is True

    def test_zscore_flag_false_at_threshold_boundary(self) -> None:
        """Requirement 9.5 uses strict ``>`` — equality is NOT an anomaly."""
        history = [0.0, 2.0] * (MIN_SAMPLES // 2)  # mean 1, stdev 1
        # current=4 → z=3 exactly → not greater than threshold 3
        z, flag = AnomalyDetector.zscore(history, current=4.0, threshold=3.0)
        assert z == pytest.approx(3.0)
        assert flag is False


class TestEWMAMath:
    """Concrete EWMA arithmetic (Requirements 10.1, 10.3)."""

    def test_ewma_alpha_formula(self) -> None:
        """``alpha = 2 / (span + 1)`` reproduces the expected baseline."""
        span = 3  # alpha = 0.5
        history = [1.0] * MIN_SAMPLES
        history[-1] = 5.0  # introduce some variance
        # Manual EWMA with alpha=0.5:
        alpha = 2.0 / (span + 1)
        assert alpha == 0.5
        baseline = history[0]
        for x in history[1:]:
            baseline = alpha * x + (1 - alpha) * baseline
        # Current == baseline → deviation 0
        deviation, flag = AnomalyDetector.ewma(
            history, current=baseline, span=span, threshold=3.0
        )
        assert deviation == pytest.approx(0.0, abs=1e-12)
        assert flag is False

    def test_ewma_flags_large_deviation(self) -> None:
        """Current far from baseline exceeds threshold when |dev|/stdev > T."""
        # Constant-ish history with a small perturbation so stdev > 0.
        history = [1.0] * (MIN_SAMPLES - 1) + [1.1]
        # Baseline hovers near 1.0; current at 100 → deviation ≈ 99,
        # stdev is tiny → score huge → flag True.
        _dev, flag = AnomalyDetector.ewma(
            history, current=100.0, span=24, threshold=3.0
        )
        assert flag is True

    def test_ewma_flag_false_for_on_baseline_current(self) -> None:
        """When current tracks the baseline, flag MUST be False."""
        history = [float(i) for i in range(MIN_SAMPLES)]  # 0..29
        alpha = 2.0 / (24 + 1)
        baseline = history[0]
        for x in history[1:]:
            baseline = alpha * x + (1 - alpha) * baseline
        _dev, flag = AnomalyDetector.ewma(
            history, current=baseline, span=24, threshold=3.0
        )
        assert flag is False


class TestMinimumSamplesGuard:
    """Explicit unit coverage of Requirements 9.3, 10.4."""

    def test_zscore_below_min_samples(self) -> None:
        for length in (0, 1, 10, MIN_SAMPLES - 1):
            history = [float(i) for i in range(length)]
            z, flag = AnomalyDetector.zscore(history, 99.0, threshold=1.0)
            assert (z, flag) == (0.0, False)

    def test_ewma_below_min_samples(self) -> None:
        for length in (0, 1, 10, MIN_SAMPLES - 1):
            history = [float(i) for i in range(length)]
            dev, flag = AnomalyDetector.ewma(
                history, 99.0, span=24, threshold=1.0
            )
            assert (dev, flag) == (0.0, False)


class TestZeroStdevGuard:
    """Explicit unit coverage of Requirements 9.4, 10.2."""

    def test_zscore_constant_history(self) -> None:
        history = [7.0] * MIN_SAMPLES
        z, flag = AnomalyDetector.zscore(history, current=99.0, threshold=3.0)
        assert (z, flag) == (0.0, False)

    def test_ewma_constant_history(self) -> None:
        history = [7.0] * MIN_SAMPLES
        dev, flag = AnomalyDetector.ewma(
            history, current=99.0, span=24, threshold=3.0
        )
        # Even though |current - baseline| = 92, stdev is zero → guard trips.
        assert (dev, flag) == (0.0, False)


class TestFlagSetClear:
    """Flag set/clear behaviour through ``_evaluate()`` (9.5, 9.6, 10.3)."""

    def test_flag_set_on_anomaly_then_cleared_on_normal(self) -> None:
        """Two consecutive ``_evaluate()`` calls toggle the flag correctly."""
        pipeline_id = "unit_flag_setclear"
        detector = _make_detector(
            window_size=200, zscore_threshold=3.0, ewma_span=24
        )

        # Seed history with 30 alternating 0/2 values → mean=1, stdev=1.
        history = detector._get_history(_KIND_VOLUME, pipeline_id)
        for i in range(MIN_SAMPLES):
            history.append(0.0 if i % 2 == 0 else 2.0)

        # First evaluate: current=100 → z ≈ 99 → flag 1.
        detector._evaluate(
            metric_kind=_KIND_VOLUME,
            pipeline_id=pipeline_id,
            current_value=100.0,
            zscore_gauge_detector=_DETECTOR_VOLUME_ZSCORE,
            ewma_detector=_DETECTOR_VOLUME_EWMA,
        )
        assert _gauge_flag(_DETECTOR_VOLUME_ZSCORE, pipeline_id) == 1.0

        # Seed a fresh pipeline with the same history and evaluate a
        # normal value → flag 0. (Using a fresh pipeline avoids the 100
        # being baked into the rolling window and skewing the mean.)
        pipeline_normal = "unit_flag_setclear_normal"
        history2 = detector._get_history(_KIND_VOLUME, pipeline_normal)
        for i in range(MIN_SAMPLES):
            history2.append(0.0 if i % 2 == 0 else 2.0)

        detector._evaluate(
            metric_kind=_KIND_VOLUME,
            pipeline_id=pipeline_normal,
            current_value=1.0,  # == mean
            zscore_gauge_detector=_DETECTOR_VOLUME_ZSCORE,
            ewma_detector=_DETECTOR_VOLUME_EWMA,
        )
        assert _gauge_flag(_DETECTOR_VOLUME_ZSCORE, pipeline_normal) == 0.0


class TestMetricsUpdate:
    """``_evaluate()`` MUST write to both detector labels and the z-score gauge."""

    def test_volume_evaluate_updates_all_three_gauges(self) -> None:
        pipeline_id = "unit_metrics_volume"
        detector = _make_detector(
            window_size=200, zscore_threshold=3.0, ewma_span=24
        )
        # Seed 30 samples so z-score produces a real number.
        history = detector._get_history(_KIND_VOLUME, pipeline_id)
        for i in range(MIN_SAMPLES):
            history.append(0.0 if i % 2 == 0 else 2.0)

        detector._evaluate(
            metric_kind=_KIND_VOLUME,
            pipeline_id=pipeline_id,
            current_value=5.0,  # z = 4, |z|>3 → flag 1
            zscore_gauge_detector=_DETECTOR_VOLUME_ZSCORE,
            ewma_detector=_DETECTOR_VOLUME_EWMA,
        )

        assert _volume_zscore_gauge(pipeline_id) == pytest.approx(4.0)
        assert _gauge_flag(_DETECTOR_VOLUME_ZSCORE, pipeline_id) == 1.0
        # EWMA flag is independently computed — assert the label exists
        # with a 0/1 float value (specific value tested in the property).
        ewma_val = _gauge_flag(_DETECTOR_VOLUME_EWMA, pipeline_id)
        assert ewma_val in (0.0, 1.0)

    def test_confidence_evaluate_updates_confidence_gauges(self) -> None:
        """Confidence path must write to the *confidence* z-score gauge.

        Validates that ``_evaluate()`` dispatches on ``metric_kind`` and
        uses the correct per-metric gauge (Requirement 9.5 + metric
        naming in §5.8).
        """
        pipeline_id = "unit_metrics_confidence"
        detector = _make_detector(
            window_size=200, zscore_threshold=3.0, ewma_span=24
        )
        history = detector._get_history(_KIND_CONFIDENCE, pipeline_id)
        for i in range(MIN_SAMPLES):
            history.append(0.5 if i % 2 == 0 else 0.9)
        # mean=0.7, stdev=0.2 → current=0.7 → z=0 → flag 0
        detector._evaluate(
            metric_kind=_KIND_CONFIDENCE,
            pipeline_id=pipeline_id,
            current_value=0.7,
            zscore_gauge_detector=_DETECTOR_CONFIDENCE_ZSCORE,
            ewma_detector=_DETECTOR_CONFIDENCE_EWMA,
        )

        assert _confidence_zscore_gauge(pipeline_id) == pytest.approx(
            0.0, abs=1e-9
        )
        assert _gauge_flag(_DETECTOR_CONFIDENCE_ZSCORE, pipeline_id) == 0.0

    def test_evaluate_appends_to_history_after_scoring(self) -> None:
        """After ``_evaluate()``, the current value MUST be in history.

        This captures the "score against prior window, then append"
        contract documented on ``_evaluate()`` (Requirement 9.2).
        """
        pipeline_id = "unit_metrics_append"
        detector = _make_detector(window_size=50)
        history = detector._get_history(_KIND_VOLUME, pipeline_id)
        assert len(history) == 0

        detector._evaluate(
            metric_kind=_KIND_VOLUME,
            pipeline_id=pipeline_id,
            current_value=42.0,
            zscore_gauge_detector=_DETECTOR_VOLUME_ZSCORE,
            ewma_detector=_DETECTOR_VOLUME_EWMA,
        )
        assert len(history) == 1
        assert history[-1] == 42.0


class TestHistoryBounds:
    """Explicit coverage of Requirements 9.2, 11.1."""

    def test_history_deque_maxlen_matches_window(self) -> None:
        detector = _make_detector(window_size=50)
        history = detector._get_history(_KIND_VOLUME, "pid_bounds")
        assert isinstance(history, deque)
        assert history.maxlen == 50

    def test_history_eviction_is_fifo(self) -> None:
        """After window_size+k appends, the first k values are evicted."""
        detector = _make_detector(window_size=5)
        pipeline_id = "pid_fifo"
        for i in range(10):
            detector._evaluate(
                metric_kind=_KIND_VOLUME,
                pipeline_id=pipeline_id,
                current_value=float(i),
                zscore_gauge_detector=_DETECTOR_VOLUME_ZSCORE,
                ewma_detector=_DETECTOR_VOLUME_EWMA,
            )
        history = detector._get_history(_KIND_VOLUME, pipeline_id)
        # Only the 5 most-recent values remain.
        assert list(history) == [5.0, 6.0, 7.0, 8.0, 9.0]

    def test_get_history_is_per_key_independent(self) -> None:
        """Keys differ by ``(metric_kind, pipeline_id)``; each is isolated."""
        detector = _make_detector(window_size=10)
        h_vol_a = detector._get_history(_KIND_VOLUME, "pipeline_a")
        h_vol_b = detector._get_history(_KIND_VOLUME, "pipeline_b")
        h_conf_a = detector._get_history(_KIND_CONFIDENCE, "pipeline_a")

        h_vol_a.append(1.0)
        h_vol_b.append(2.0)
        h_conf_a.append(3.0)

        assert list(h_vol_a) == [1.0]
        assert list(h_vol_b) == [2.0]
        assert list(h_conf_a) == [3.0]
