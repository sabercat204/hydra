"""EAS Prometheus metric definitions — complete catalog per Design §11.1 / R23.1.

Every counter, gauge, and histogram listed in the Design §11.1 table is
registered here, backed by the process-wide Prometheus default registry so
the ``/metrics`` endpoint from :mod:`hydra.api` picks them up automatically.

The module degrades gracefully when ``prometheus_client`` is not installed
so that unrelated imports of :mod:`hydra.eas` do not fail before the
optional ``[eas]`` extras are pulled in.

Catalog (aligned with Design §11.1):

* Counters:
  ``hydra_eas_cve_records_total`` (R9.7),
  ``hydra_eas_asn_lookup_failure_total``,
  ``hydra_eas_exposure_events_total`` (R3.4),
  ``hydra_eas_exposure_buffer_overflow_total``,
  ``hydra_eas_screenshot_captures_total`` (R6),
  ``hydra_eas_screenshot_bytes_total``,
  ``hydra_eas_lookup_cache_hits_total`` (R17.6),
  ``hydra_eas_lookup_cache_misses_total`` (R17.6),
  ``hydra_eas_observatory_runs_total`` (R19.2/R19.3).
* Gauges:
  ``hydra_eas_lookup_cache_size`` (R17.6),
  ``hydra_eas_quota_usage_ratio`` (R22.4),
  ``hydra_eas_observatory_last_run_timestamp_seconds`` (R19.2 / Design §11.2).
* Histograms:
  ``hydra_eas_trends_window_bytes`` (per-bucket response size),
  ``hydra_eas_maps_tiles_returned`` (per-strategy tile count).
"""

from __future__ import annotations

from typing import Any

# Tasks like the daily observatory DAG look up metric objects by name via
# :func:`get_observatory_runs_counter` rather than importing the symbol
# directly. That lets the DAG file remain importable on hosts where the
# metrics module hasn't been loaded yet (e.g. Airflow workers without the
# ``[eas]`` extras). See ``dags/eas_observatory_daily.py``.


class _NoopMetric:
    """Fallback used when ``prometheus_client`` is unavailable.

    Implements the tiny slice of the Counter API we actually call
    (``labels(...).inc(n)``) as no-ops. Any additional method access returns
    ``self`` so chained calls stay silent.
    """

    def labels(self, *_: Any, **__: Any) -> "_NoopMetric":
        return self

    def inc(self, *_: Any, **__: Any) -> None:
        return None

    def __getattr__(self, _name: str) -> Any:
        return lambda *a, **kw: self


try:  # pragma: no cover - exercised implicitly by the metric import below
    from prometheus_client import REGISTRY, Counter, Gauge, Histogram

    _PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover - same
    REGISTRY = None  # type: ignore[assignment]
    Counter = None  # type: ignore[assignment,misc]
    Gauge = None  # type: ignore[assignment,misc]
    Histogram = None  # type: ignore[assignment,misc]
    _PROMETHEUS_AVAILABLE = False


def _build_counter(
    name: str, documentation: str, labelnames: tuple[str, ...]
) -> Any:
    """Return the existing Counter with ``name`` if already registered, else create it.

    ``prometheus_client`` raises ``ValueError`` when the same metric name is
    registered twice against the process-wide default registry. This helper
    fetches the existing collector in that case so that re-imports (e.g.
    during the refactor landing with task 16.1) remain idempotent.

    When ``prometheus_client`` is not installed, a no-op stand-in is returned
    so that calling code can still do ``labels(...).inc(...)`` unchanged.
    """

    if not _PROMETHEUS_AVAILABLE:
        return _NoopMetric()

    assert Counter is not None  # narrow type for mypy
    existing = getattr(REGISTRY, "_names_to_collectors", {}).get(name)
    if existing is not None:
        return existing
    try:
        return Counter(name, documentation, labelnames)
    except ValueError:
        existing = getattr(REGISTRY, "_names_to_collectors", {}).get(name)
        if existing is not None:
            return existing
        raise


hydra_eas_cve_records_total = _build_counter(
    "hydra_eas_cve_records_total",
    "CVE-family records persisted per source (R9.7).",
    ("source",),
)


def _build_gauge(
    name: str, documentation: str, labelnames: tuple[str, ...]
) -> Any:
    """Return the existing Gauge with ``name`` if already registered, else create it.

    Mirror of :func:`_build_counter` for gauge metrics. A gauge is the
    appropriate type for "point-in-time" quantities like cache size,
    queue depth, or quota usage ratio — anything that can both rise
    and fall. When ``prometheus_client`` is unavailable a no-op
    ``_NoopMetric`` is returned so calling code doesn't need to know
    about the fallback.
    """

    if not _PROMETHEUS_AVAILABLE:
        return _NoopMetric()

    assert Gauge is not None  # narrow type for mypy
    existing = getattr(REGISTRY, "_names_to_collectors", {}).get(name)
    if existing is not None:
        return existing
    try:
        return Gauge(name, documentation, labelnames)
    except ValueError:
        existing = getattr(REGISTRY, "_names_to_collectors", {}).get(name)
        if existing is not None:
            return existing
        raise


# ``AssetMatcher`` bumps this whenever the ASN lookup cannot be serviced
# because the pyasn database is missing or could not be loaded. A rising
# rate on this counter is the canonical signal that the weekly asn-dataset
# refresh DAG has failed. Task 16.1 will add an Alertmanager rule; until
# then the counter is emitted under the same registry via the shared
# ``_build_counter`` helper so it survives repeated imports.
hydra_eas_asn_lookup_failure_total = _build_counter(
    "hydra_eas_asn_lookup_failure_total",
    "Failed ASN lookups due to missing pyasn database (AssetMatcher).",
    (),
)


# ``AssetMonitor`` increments this once per non-duplicate exposure row
# written to ``asset_exposures`` (R3.4). The ``tenant_id, asset_type,
# tier, severity`` label set matches Design §11.1 / R23.1 so it can be
# consumed unchanged by the dashboards produced by task 16.5.
hydra_eas_exposure_events_total = _build_counter(
    "hydra_eas_exposure_events_total",
    "Exposure events recorded per tenant, asset type, tier, and severity (R3.4).",
    ("tenant_id", "asset_type", "tier", "severity"),
)


# ``AssetMonitor`` increments this when PG writes are blocked and the
# in-process deque must drop the oldest buffered event to make room for
# the newest one (Design §6.1 backpressure note). The label set is
# intentionally empty: a rising rate already means "something is wrong";
# cardinality is not useful.
hydra_eas_exposure_buffer_overflow_total = _build_counter(
    "hydra_eas_exposure_buffer_overflow_total",
    "Exposure events dropped from the in-process buffer when PG is blocked.",
    (),
)


# ``ScreenshotAdapter`` increments this once per render attempt, tagged
# with the outcome (``success``, ``failed``, ``skipped``). The ``failed``
# bucket covers every non-success path the adapter writes: SSRF blocks,
# backpressure rejections, Playwright errors, MinIO upload failures, and
# ES index failures. The ``skipped`` bucket is reserved for future
# cadence-triggered dedup logic; the MVP adapter emits only ``success``
# and ``failed``. Used by Design §11.2 alert ``HydraEASScreenshotFailureRate``.
hydra_eas_screenshot_captures_total = _build_counter(
    "hydra_eas_screenshot_captures_total",
    "Screenshot capture attempts per outcome (R6, Design §11.1).",
    ("status",),
)


# Total PNG bytes written to MinIO by ``ScreenshotAdapter``. Unlabeled
# because per-tenant byte accounting is covered by the cost-quota subsystem
# (task 15.1). A rising rate here tracks aggregate storage pressure on the
# ``hydra-screenshots`` bucket.
hydra_eas_screenshot_bytes_total = _build_counter(
    "hydra_eas_screenshot_bytes_total",
    "Total PNG bytes persisted to MinIO by the Screenshot_Adapter.",
    (),
)


# ``IndicatorLookupCache`` increments these on every cache access —
# ``hits`` on a successful ``GET``, ``misses`` when the key is absent
# and the assembler runs. Labels are the indicator class so dashboards
# can see hit-rate skew across IPs, domains, hashes, etc. Used by the
# Design §11.2 SLO ``HydraEASLookupCacheHitRateLow``.
hydra_eas_lookup_cache_hits_total = _build_counter(
    "hydra_eas_lookup_cache_hits_total",
    "Indicator lookup cache hits per indicator class (R17.1, R17.6).",
    ("indicator_class",),
)


hydra_eas_lookup_cache_misses_total = _build_counter(
    "hydra_eas_lookup_cache_misses_total",
    "Indicator lookup cache misses per indicator class (R17.1, R17.6).",
    ("indicator_class",),
)


# Gauge for the current key count in the dedicated lookup cache DB.
# Sampled by :meth:`IndicatorLookupCache.size` — callers (e.g. a
# periodic sampler in task 16.1) can ``.set(size)`` on this gauge to
# keep the value fresh. Unlabeled: DBSIZE is global to the Redis DB.
hydra_eas_lookup_cache_size = _build_gauge(
    "hydra_eas_lookup_cache_size",
    "Current number of keys in the Indicator_Lookup_Cache Redis DB (R17.6).",
    (),
)


# ``CostQuotaCounter.increment_and_check`` updates this gauge with the
# ratio ``count / limit`` every time a tenant is charged against one of
# the per-day cost quotas (R22.4). Values live in ``[0.0, 1.0]`` under
# normal operation but may temporarily exceed ``1.0`` between the
# INCR and the DECR steps of the MULTI/EXEC transaction when a tenant
# is rejected for quota exhaustion. The label set matches Design §11.1
# so the ``HydraEASQuotaNearExhaustion`` alert (R22.4) can fire at
# ``ratio > 0.9``.
hydra_eas_quota_usage_ratio = _build_gauge(
    "hydra_eas_quota_usage_ratio",
    "Per-tenant per-quota usage ratio (count / limit) — R22.4.",
    ("tenant_id", "quota_name"),
)


def _build_histogram(
    name: str,
    documentation: str,
    labelnames: tuple[str, ...],
    buckets: tuple[float, ...] | None = None,
) -> Any:
    """Return the existing Histogram with ``name`` if already registered, else create it.

    Mirror of :func:`_build_counter` / :func:`_build_gauge` for histogram
    metrics. Histograms are the appropriate type for "distribution"
    quantities whose percentiles are needed in dashboards — tiles per
    map response, bytes per trends response, etc.

    ``buckets`` is forwarded to :class:`prometheus_client.Histogram` when
    creating the collector for the first time. When the histogram already
    exists (duplicate import path, test reload) the existing collector is
    returned unchanged — the ``buckets`` argument is ignored in that case
    because Prometheus histograms are bucket-immutable after creation.
    """

    if not _PROMETHEUS_AVAILABLE:
        return _NoopMetric()

    assert Histogram is not None  # narrow type for mypy
    existing = getattr(REGISTRY, "_names_to_collectors", {}).get(name)
    if existing is not None:
        return existing
    try:
        if buckets is not None:
            return Histogram(name, documentation, labelnames, buckets=buckets)
        return Histogram(name, documentation, labelnames)
    except ValueError:
        existing = getattr(REGISTRY, "_names_to_collectors", {}).get(name)
        if existing is not None:
            return existing
        raise


# ``eas_observatory_daily`` DAG increments this once per run, tagged
# with the outcome (``success`` / ``failed``). Used by the
# ``HydraEASObservatoryStale`` alert (Design §11.2) in conjunction with
# ``hydra_eas_observatory_last_run_timestamp_seconds`` below. Labels
# intentionally keep the cardinality low (two distinct status values).
hydra_eas_observatory_runs_total = _build_counter(
    "hydra_eas_observatory_runs_total",
    "Observatory DAG runs per status (R19.2/R19.3).",
    ("status",),
)


# Wall-clock timestamp (seconds since UTC epoch) of the last completed
# observatory run. The ``HydraEASObservatoryStale`` alert fires when
# ``time() - <gauge> > 172800`` (48 hours). Unlabeled because the
# observatory currently runs as a single global producer.
hydra_eas_observatory_last_run_timestamp_seconds = _build_gauge(
    "hydra_eas_observatory_last_run_timestamp_seconds",
    "Unix timestamp of the most recent successful observatory run (R19.2).",
    (),
)


# Distribution of response sizes (in bytes) returned by the Trends
# router, bucketed by aggregation bucket (``1m``, ``5m``, ``1h``, …).
# Dashboards plot ``histogram_quantile(0.95, ...)`` per bucket label so
# operators can see which bucket sizes are driving response-payload
# growth. Default buckets span 1 KiB to 16 MiB in powers of four because
# real responses are typically a few KiB to a few MiB and we want
# coarser resolution beyond that range.
hydra_eas_trends_window_bytes = _build_histogram(
    "hydra_eas_trends_window_bytes",
    "Bytes returned per trends response, labeled by bucket.",
    ("bucket",),
    (
        1_024.0,
        4_096.0,
        16_384.0,
        65_536.0,
        262_144.0,
        1_048_576.0,
        4_194_304.0,
        16_777_216.0,
    ),
)


# Distribution of tile counts returned by the Maps router, bucketed
# by aggregation strategy (``h3`` vs ``geohash``). Used by Design
# §11.4 dashboard panel "Tiles per response (p95)". Buckets cap at
# ``maps_tile_max_cells`` default (2000); a separate ``+Inf`` bucket
# catches truncation events.
hydra_eas_maps_tiles_returned = _build_histogram(
    "hydra_eas_maps_tiles_returned",
    "Tile cells returned per maps response, labeled by aggregation strategy.",
    ("strategy",),
    (
        1.0,
        10.0,
        50.0,
        100.0,
        250.0,
        500.0,
        1_000.0,
        2_000.0,
        5_000.0,
    ),
)


def get_observatory_runs_counter() -> Any:
    """Convenience accessor used by the daily observatory DAG (R19.2/R19.3).

    The DAG imports this module lazily and looks up the counter via
    ``getattr(metrics, "get_observatory_runs_counter", None)``; returning
    the module-level :data:`hydra_eas_observatory_runs_total` object keeps
    the DAG decoupled from the concrete registration path. The return
    value is always the same counter instance — Prometheus counters are
    module-level singletons.
    """

    return hydra_eas_observatory_runs_total


__all__ = [
    "hydra_eas_cve_records_total",
    "hydra_eas_asn_lookup_failure_total",
    "hydra_eas_exposure_events_total",
    "hydra_eas_exposure_buffer_overflow_total",
    "hydra_eas_screenshot_captures_total",
    "hydra_eas_screenshot_bytes_total",
    "hydra_eas_lookup_cache_hits_total",
    "hydra_eas_lookup_cache_misses_total",
    "hydra_eas_lookup_cache_size",
    "hydra_eas_quota_usage_ratio",
    "hydra_eas_observatory_runs_total",
    "hydra_eas_observatory_last_run_timestamp_seconds",
    "hydra_eas_trends_window_bytes",
    "hydra_eas_maps_tiles_returned",
    "get_observatory_runs_counter",
]
