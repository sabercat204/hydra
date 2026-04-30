"""AnomalyDetector — statistical anomaly detection on pipeline metrics (P12 §7).

This module implements :class:`AnomalyDetector`, a periodic
:class:`~hydra.monitoring.collectors.BaseCollector` subclass that watches
two signals per correlation pipeline:

* **correlation volume** — number of new correlation results produced
  in the last cycle.
* **confidence drift** — mean confidence score of those results.

Each signal is appended to a bounded rolling history (``deque`` with
``maxlen=window_size``) keyed by ``(metric_kind, pipeline_id)`` and
evaluated with two independent detectors:

1. **Z-score** — classical standardised deviation from the rolling mean
   using population variance (Algorithm 2, Requirements 9.1–9.6).
2. **EWMA** — exponentially-weighted moving average baseline; the
   current value is flagged when its deviation from the EWMA, scaled by
   the rolling standard deviation, exceeds the threshold (Algorithm 3,
   Requirements 10.1–10.4).

Both detectors share three guards:

* **Minimum samples** — Requirement 9.3, 10.2, 10.4: fewer than 30
  historical points yields ``(0.0, False)``.
* **Zero variance** — Requirement 9.4, 10.2: a standard deviation of
  zero yields ``(0.0, False)`` (avoids division by zero when the
  history is constant).
* **Bounded history** — Requirement 11.1: the ``deque.maxlen`` caps
  memory at ``window_size`` samples per ``(metric_kind, pipeline_id)``
  key, guaranteeing O(1) memory growth regardless of uptime.

The detection methods :meth:`AnomalyDetector.zscore` and
:meth:`AnomalyDetector.ewma` are exposed as ``@staticmethod`` so they
can be exercised in isolation by property-based tests (tasks 7.2–7.7)
without constructing a PostgreSQL pool. They are pure functions of
their inputs — no I/O, no state mutation.

The background loop reuses :class:`BaseCollector`'s error-isolation
machinery: any exception from PostgreSQL (Requirement 22.2) is caught,
logged, counted against ``COLLECTOR_ERRORS``, and the loop continues on
the next tick. As in :class:`~hydra.monitoring.collectors.PipelineCollector`,
``last_collection_ts`` advances only after a successful cycle so a
transient DB failure is safely retried with the same time window.

Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 10.1, 10.2, 10.3, 10.4,
11.1, 11.2, 22.2.
"""

from __future__ import annotations

import logging
import math
import statistics
from collections import deque
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Final

from hydra.config import MonitoringSettings
from hydra.monitoring.collectors import BaseCollector
from hydra.monitoring.exceptions import AnomalyDetectionError
from hydra.monitoring.metrics import (
    hydra_anomaly_confidence_drift_zscore,
    hydra_anomaly_correlation_volume_zscore,
    hydra_anomaly_flag,
)

if TYPE_CHECKING:
    import asyncpg

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Module constants
# ---------------------------------------------------------------------------

#: Minimum number of rolling-history samples before either detector will
#: produce a non-trivial answer. Below this threshold, both detectors
#: short-circuit to ``(0.0, False)`` (Requirements 9.3, 10.2, 10.4).
MIN_SAMPLES: Final[int] = 30

#: Metric-kind discriminator for the correlation-volume signal.
_KIND_VOLUME: Final[str] = "correlation_volume"

#: Metric-kind discriminator for the mean-confidence drift signal.
_KIND_CONFIDENCE: Final[str] = "confidence_drift"

#: Detector label values used on the ``hydra_anomaly_flag`` gauge.
#: Matches the Grafana panels and alert rules defined in design §10.
_DETECTOR_VOLUME_ZSCORE: Final[str] = "correlation_volume_zscore"
_DETECTOR_VOLUME_EWMA: Final[str] = "correlation_volume_ewma"
_DETECTOR_CONFIDENCE_ZSCORE: Final[str] = "confidence_drift_zscore"
_DETECTOR_CONFIDENCE_EWMA: Final[str] = "confidence_drift_ewma"


# ---------------------------------------------------------------------------
# SQL queries
# ---------------------------------------------------------------------------

# Correlation counts per pipeline in the time window (start, end].
_VOLUME_SQL: Final[str] = (
    "SELECT pipeline_id, COUNT(*)::bigint AS cnt "
    "FROM correlation_results "
    "WHERE created_at > $1 AND created_at <= $2 "
    "GROUP BY pipeline_id"
)

# Mean confidence per pipeline in the same window. NULL means no rows
# in the window — those pipelines are skipped (no drift to measure).
_CONFIDENCE_SQL: Final[str] = (
    "SELECT pipeline_id, AVG(confidence)::float8 AS mean_confidence "
    "FROM correlation_results "
    "WHERE created_at > $1 AND created_at <= $2 "
    "GROUP BY pipeline_id"
)


# ---------------------------------------------------------------------------
# AnomalyDetector
# ---------------------------------------------------------------------------


class AnomalyDetector(BaseCollector):
    """Periodic z-score + EWMA anomaly detector for correlation metrics.

    The detector inherits the background-loop and error-isolation
    behaviour from :class:`BaseCollector`. Each ``collect()`` cycle:

    1. Captures ``now`` as the window upper bound.
    2. Queries PostgreSQL for per-pipeline correlation volume and mean
       confidence in ``(last_collection_ts, now]``.
    3. For each ``(metric_kind, pipeline_id)`` key, appends the current
       value to the bounded rolling history, runs both detectors, and
       updates the z-score gauge plus the two anomaly-flag gauges
       (z-score and EWMA variants).
    4. Advances ``last_collection_ts`` to ``now`` only after both
       queries have succeeded — on failure the next cycle retries the
       same window, preserving continuity of the rolling baseline.

    The public detection math is exposed as static methods so it can be
    unit- and property-tested without a live pool.
    """

    def __init__(
        self,
        pg_pool: "asyncpg.Pool",
        settings: MonitoringSettings,
        interval: float | None = None,
    ) -> None:
        """Create the detector.

        Args:
            pg_pool: Async PostgreSQL connection pool used to query
                ``correlation_results``.
            settings: Monitoring configuration providing the rolling
                window size, z-score threshold, and EWMA span
                (Requirement 21.2).
            interval: Optional override for the background-loop interval
                in seconds. When ``None`` (default), the interval is
                taken from ``settings.anomaly_detection_interval``.
        """
        super().__init__(
            interval=(
                interval
                if interval is not None
                else settings.anomaly_detection_interval
            )
        )
        self._pg_pool = pg_pool
        self._settings = settings
        self._window_size: int = settings.anomaly_window_size
        self._zscore_threshold: float = settings.anomaly_zscore_threshold
        self._ewma_span: int = settings.anomaly_ewma_span

        # Rolling history per (metric_kind, pipeline_id). Each deque is
        # bounded by ``window_size`` to enforce Requirement 11.1.
        self._history: dict[tuple[str, str], deque[float]] = {}

        # Initialize to construction time so the first cycle reads a
        # single interval of data rather than the whole backlog.
        self.last_collection_ts: datetime = datetime.now(timezone.utc)

    # ------------------------------------------------------------------
    # Pure detection math (exposed as staticmethods for property tests)
    # ------------------------------------------------------------------

    @staticmethod
    def zscore(
        history: "deque[float] | Sequence[float]",
        current: float,
        threshold: float,
    ) -> tuple[float, bool]:
        """Compute the z-score of ``current`` against ``history``.

        Uses population variance (divisor ``n``) to match Algorithm 2
        from the design document. The history is treated as read-only —
        callers are responsible for appending ``current`` before or after
        the call as their bookkeeping requires.

        Args:
            history: Rolling history of past observations (order-
                independent — only the mean and variance are used).
            current: The latest observation to score.
            threshold: Absolute z-score above which the anomaly flag is
                set (Requirement 9.5).

        Returns:
            Tuple ``(zscore, flag)`` where:

            * ``(0.0, False)`` if ``len(history) < MIN_SAMPLES``
              (Requirement 9.3).
            * ``(0.0, False)`` if the population standard deviation of
              ``history`` is zero (Requirement 9.4).
            * ``(z, |z| > threshold)`` otherwise (Requirements 9.5, 9.6).
        """
        n = len(history)
        if n < MIN_SAMPLES:
            return 0.0, False

        # Short-circuit for exact-constant histories: when every entry
        # equals the first, the true population stdev is mathematically
        # zero regardless of the common value's magnitude. Computing it
        # via ``sum((x - mean)**2) / n`` can accumulate ULP-scale drift
        # at mid-to-high magnitudes (e.g. 1e5), producing a tiny positive
        # variance that would break the zero-stdev guard (Requirement
        # 9.4 / Property 4).
        first = history[0]
        if all(x == first for x in history):
            return 0.0, False

        # Use ``statistics.pvariance`` (Welford-style two-pass) for
        # numerical stability on large-magnitude inputs — more reliable
        # than the naive ``sum((x-mean)**2)/n`` at extreme magnitudes.
        variance = statistics.pvariance(history)
        stdev = math.sqrt(variance)

        if stdev == 0.0:
            return 0.0, False

        mean = statistics.fmean(history)
        z = (current - mean) / stdev
        return z, abs(z) > threshold

    @staticmethod
    def ewma(
        history: "deque[float] | Sequence[float]",
        current: float,
        span: int,
        threshold: float,
    ) -> tuple[float, bool]:
        """Compute the EWMA deviation of ``current`` against ``history``.

        Implements Algorithm 3 from the design document. The smoothing
        factor ``alpha = 2 / (span + 1)`` (Requirement 10.1) is applied
        left-to-right across ``history`` to build the baseline, then the
        deviation ``current - ewma`` is normalised by the population
        standard deviation of ``history`` and compared against
        ``threshold``.

        Args:
            history: Rolling history of past observations, in chronological
                order. The first element seeds the EWMA, subsequent
                elements update it.
            current: The latest observation to score.
            span: EWMA span parameter; ``alpha = 2 / (span + 1)``. Must
                be positive (Requirement 10.1).
            threshold: Deviation magnitude (in stdev units) above which
                the anomaly flag is set (Requirement 10.3).

        Returns:
            Tuple ``(deviation, flag)`` where ``deviation = current - ewma``
            (signed; callers can inspect the sign to distinguish spikes
            from drops) and ``flag`` is ``True`` iff
            ``|deviation| / stdev > threshold``.

            Returns ``(0.0, False)`` when:

            * ``len(history) < MIN_SAMPLES`` — Requirement 10.4.
            * Population stdev of ``history`` is zero — Requirement 10.2.

        Raises:
            ValueError: If ``span`` is not positive.
        """
        if span <= 0:
            raise ValueError(f"EWMA span must be positive; got {span!r}")

        n = len(history)
        if n < MIN_SAMPLES:
            return 0.0, False

        # Materialize the iterable once so we can both seed the EWMA and
        # compute variance without consuming a one-shot iterator.
        values = list(history)

        # Short-circuit for exact-constant histories (see the zscore
        # helper for the full rationale). At mid-magnitude constants the
        # naive population-variance computation drifts by a ULP and the
        # stdev guard below would misfire (Requirement 10.2).
        first = values[0]
        if all(x == first for x in values):
            return 0.0, False

        alpha = 2.0 / (span + 1)
        ewma_value = values[0]
        for x in values[1:]:
            ewma_value = alpha * x + (1.0 - alpha) * ewma_value

        # Population variance of the history (not of (history ∪ current))
        # so the baseline stdev is independent of the value being scored.
        # ``statistics.pvariance`` is more numerically stable than the
        # naive two-pass form at extreme magnitudes.
        variance = statistics.pvariance(values)
        stdev = math.sqrt(variance)

        if stdev == 0.0:
            return 0.0, False

        deviation = current - ewma_value
        score = abs(deviation) / stdev
        return deviation, score > threshold

    # ------------------------------------------------------------------
    # Collection cycle
    # ------------------------------------------------------------------

    async def collect(self) -> None:
        """Run one detection cycle.

        Any exception raised by the PostgreSQL queries is wrapped in
        :class:`AnomalyDetectionError` and re-raised — the
        :class:`BaseCollector` loop catches it, increments the
        collector-error counter, and continues (Requirement 22.2).
        """
        now = datetime.now(timezone.utc)
        start = self.last_collection_ts

        try:
            async with self._pg_pool.acquire() as conn:
                volume_rows = await conn.fetch(_VOLUME_SQL, start, now)
                confidence_rows = await conn.fetch(_CONFIDENCE_SQL, start, now)
        except Exception as exc:
            # Wrap and re-raise so BaseCollector._loop counts this as a
            # collector error. Do not advance last_collection_ts — the
            # next cycle retries the same window.
            raise AnomalyDetectionError(
                f"Failed to query correlation_results for anomaly detection: {exc}"
            ) from exc

        # Correlation volume per pipeline.
        for row in volume_rows:
            pipeline_id = str(row["pipeline_id"])
            volume = float(row["cnt"])
            self._evaluate(
                metric_kind=_KIND_VOLUME,
                pipeline_id=pipeline_id,
                current_value=volume,
                zscore_gauge_detector=_DETECTOR_VOLUME_ZSCORE,
                ewma_detector=_DETECTOR_VOLUME_EWMA,
            )

        # Mean confidence per pipeline — AVG is NULL when no rows, but
        # the GROUP BY already excludes those pipelines so the cast is
        # safe.
        for row in confidence_rows:
            pipeline_id = str(row["pipeline_id"])
            mean_confidence = row["mean_confidence"]
            if mean_confidence is None:
                continue
            self._evaluate(
                metric_kind=_KIND_CONFIDENCE,
                pipeline_id=pipeline_id,
                current_value=float(mean_confidence),
                zscore_gauge_detector=_DETECTOR_CONFIDENCE_ZSCORE,
                ewma_detector=_DETECTOR_CONFIDENCE_EWMA,
            )

        # Advance the watermark only after both queries + metric updates
        # succeeded. A failure above raised before reaching this point.
        self.last_collection_ts = now

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_history(
        self, metric_kind: str, pipeline_id: str
    ) -> "deque[float]":
        """Return the bounded deque for the given key, creating it on demand.

        Each deque enforces ``maxlen=window_size`` so history cannot
        grow beyond the configured window (Requirement 11.1, Property 8).
        """
        key = (metric_kind, pipeline_id)
        history = self._history.get(key)
        if history is None:
            history = deque(maxlen=self._window_size)
            self._history[key] = history
        return history

    def _evaluate(
        self,
        *,
        metric_kind: str,
        pipeline_id: str,
        current_value: float,
        zscore_gauge_detector: str,
        ewma_detector: str,
    ) -> None:
        """Run both detectors for a single pipeline and update gauges.

        The rolling history is evaluated *before* ``current_value`` is
        appended so that both detectors see the same baseline — the
        z-score is the standardised distance from the existing mean, not
        a self-referential computation that includes the point being
        scored. This matches the design's "against rolling window"
        semantics and ensures the 30-sample minimum genuinely refers to
        prior observations.

        After evaluation, ``current_value`` is appended to the deque,
        which automatically evicts the oldest sample when the window is
        full (Requirement 11.1).
        """
        history = self._get_history(metric_kind, pipeline_id)

        z_value, z_flag = self.zscore(
            history, current_value, self._zscore_threshold
        )
        ewma_deviation, ewma_flag = self.ewma(
            history, current_value, self._ewma_span, self._zscore_threshold
        )

        # Append after evaluation so the next cycle's baseline includes
        # this observation while today's score is measured against the
        # prior window (Requirement 9.2).
        history.append(current_value)

        # Update the per-metric z-score gauge. EWMA deviation is surfaced
        # via the flag only — the zscore gauge exists for the z-score
        # detector by design §5.8.
        if metric_kind == _KIND_VOLUME:
            hydra_anomaly_correlation_volume_zscore.labels(
                pipeline_id=pipeline_id
            ).set(z_value)
        elif metric_kind == _KIND_CONFIDENCE:
            hydra_anomaly_confidence_drift_zscore.labels(
                pipeline_id=pipeline_id
            ).set(z_value)
        else:  # pragma: no cover — defensive, kinds are module constants
            logger.warning("Unknown metric_kind for anomaly detector: %s", metric_kind)
            return

        # Publish both detector flags under the shared anomaly_flag
        # gauge. Each detector gets its own label so alert rules can
        # match one or both.
        hydra_anomaly_flag.labels(
            detector=zscore_gauge_detector,
            pipeline_id=pipeline_id,
        ).set(1.0 if z_flag else 0.0)
        hydra_anomaly_flag.labels(
            detector=ewma_detector,
            pipeline_id=pipeline_id,
        ).set(1.0 if ewma_flag else 0.0)

        # Unused locally but surfaced for debugging / log context.
        del ewma_deviation


__all__ = [
    "MIN_SAMPLES",
    "AnomalyDetector",
]
