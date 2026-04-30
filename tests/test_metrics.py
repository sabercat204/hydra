"""Tests for HYDRA custom Prometheus metric definitions.

Covers Task 1.4 (property test: metric naming convention — Requirement 3.2)
and Task 1.5 (unit tests: metric registration, label cardinality, and
basic operations — Requirements 3.1, 3.2, 3.3).

The tests inspect the ``hydra.monitoring.metrics`` module directly rather
than scraping the global ``prometheus_client`` registry, which keeps them
hermetic from any other metrics another test might accidentally register.
The exposed metric family names are still cross-checked via
``REGISTRY.collect()`` for the naming property.
"""

from __future__ import annotations

import inspect
import re
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st
from prometheus_client import REGISTRY, Counter, Gauge, Histogram
from prometheus_client.metrics import MetricWrapperBase

from hydra.monitoring import metrics as metrics_module

# ---------------------------------------------------------------------------
# Naming convention pattern (Requirement 3.2)
# ---------------------------------------------------------------------------

#: ``hydra_{subsystem}_{name}_{unit}`` — at minimum ``hydra`` followed by
#: two or more lowercase-alphanumeric/underscore segments.
METRIC_NAME_PATTERN = re.compile(r"^hydra_[a-z]+(?:_[a-z0-9]+)+$")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _iter_metric_instances() -> list[tuple[str, MetricWrapperBase]]:
    """Return ``(attr_name, metric)`` pairs for every metric instance in the
    ``metrics`` module — including the internal ``COLLECTOR_ERRORS`` counter.
    """
    pairs: list[tuple[str, MetricWrapperBase]] = []
    for attr_name, obj in inspect.getmembers(metrics_module):
        if isinstance(obj, MetricWrapperBase):
            pairs.append((attr_name, obj))
    return pairs


def _exposed_name(metric: MetricWrapperBase) -> str:
    """Return the Prometheus exposition name for a metric instance.

    ``prometheus_client`` stores the normalized base name on ``_name``
    (e.g. a Counter registered as ``hydra_foo_total`` will have
    ``_name == 'hydra_foo'`` and expose both ``hydra_foo_total`` and
    ``hydra_foo_created`` on scrape). For this project every Counter is
    declared with the ``_total`` suffix explicitly, so we reconstruct the
    exposition name from ``_name`` + type convention to validate the
    public-facing name.
    """
    base = metric._name
    if isinstance(metric, Counter):
        return f"{base}_total"
    return base


#: Metric instances keyed by their Python attribute name (evaluated once).
_METRIC_PAIRS: list[tuple[str, MetricWrapperBase]] = _iter_metric_instances()

#: Metric instances for parametrisation.
_METRICS: list[MetricWrapperBase] = [m for _, m in _METRIC_PAIRS]

#: Attribute names for parametrisation.
_METRIC_ATTRS: list[str] = [n for n, _ in _METRIC_PAIRS]


# ---------------------------------------------------------------------------
# Task 1.4 — Property test: metric naming convention (Requirement 3.2)
# ---------------------------------------------------------------------------


class TestMetricNamingProperty:
    """**Property 1: Metric Naming Convention**

    Every custom metric registered by the monitoring subsystem must match
    the ``hydra_{subsystem}_{name}_{unit}`` pattern. The internal
    ``hydra_monitoring_collector_errors_total`` counter is included.

    Validates: Requirement 3.2
    """

    def test_every_module_metric_matches_pattern(self) -> None:
        """Direct inspection: every metric instance in the metrics module
        exposes a name matching the naming pattern."""
        assert _METRICS, "metrics module exposed no metric instances"
        bad: list[str] = []
        for attr, metric in _METRIC_PAIRS:
            name = _exposed_name(metric)
            if not METRIC_NAME_PATTERN.fullmatch(name):
                bad.append(f"{attr} -> {name!r}")
        assert not bad, f"Metrics violating naming convention: {bad}"

    def test_every_registered_hydra_metric_matches_pattern(self) -> None:
        """Cross-check against the global ``REGISTRY``: any metric family
        whose name begins with ``hydra_`` must match the pattern.

        This catches cases where a metric exists on the registry but is
        not exported from the module (e.g. accidentally shadowed)."""
        violations: list[str] = []
        for family in REGISTRY.collect():
            name = family.name
            if not name.startswith("hydra_"):
                continue
            # Counters collect under their base name; re-append _total so
            # the pattern applies uniformly to the exposition form.
            candidate = f"{name}_total" if family.type == "counter" else name
            if not METRIC_NAME_PATTERN.fullmatch(candidate):
                violations.append(candidate)
        assert not violations, (
            f"Registered hydra_* metrics violating naming convention: {violations}"
        )

    @given(metric=st.sampled_from(_METRICS))
    def test_property_all_metrics_match_naming_pattern(
        self, metric: MetricWrapperBase
    ) -> None:
        """Hypothesis-driven: sampling any declared metric at random, its
        exposed name must match the naming pattern."""
        name = _exposed_name(metric)
        assert METRIC_NAME_PATTERN.fullmatch(name), (
            f"metric {metric!r} exposes name {name!r} violating "
            f"hydra_{{subsystem}}_{{name}}_{{unit}} convention"
        )


# ---------------------------------------------------------------------------
# Task 1.5 (a) — Registration tests (Requirement 3.1)
# ---------------------------------------------------------------------------


class TestMetricRegistration:
    """All custom metrics are registered in the ``prometheus_client``
    default registry at import time, are typed correctly, and are exported
    from the module's public API.

    Requirements: 3.1, 3.2
    """

    def test_all_metric_instances_are_exported(self) -> None:
        """Every metric instance in the module is listed in ``__all__``."""
        exported = set(metrics_module.__all__)
        missing = [attr for attr, _ in _METRIC_PAIRS if attr not in exported]
        assert not missing, f"metric instances missing from __all__: {missing}"

    def test_all_exported_metric_names_resolve_to_instances(self) -> None:
        """Every ``hydra_*`` / ``COLLECTOR_ERRORS`` entry in ``__all__``
        resolves to a metric instance."""
        for name in metrics_module.__all__:
            if not (name.startswith("hydra_") or name == "COLLECTOR_ERRORS"):
                continue
            obj = getattr(metrics_module, name)
            assert isinstance(obj, MetricWrapperBase), (
                f"__all__ entry {name!r} is not a prometheus metric instance"
            )

    def test_registered_custom_metric_count_matches_exports(self) -> None:
        """The number of ``hydra_``-prefixed and internal metrics exported
        equals the number of metric instances actually declared."""
        exported_metric_names = [
            n for n in metrics_module.__all__
            if n.startswith("hydra_") or n == "COLLECTOR_ERRORS"
        ]
        assert len(exported_metric_names) == len(_METRIC_PAIRS)

    def test_every_metric_is_counter_gauge_or_histogram(self) -> None:
        """Requirement 3.1 requires Counters, Gauges, and Histograms."""
        for attr, metric in _METRIC_PAIRS:
            assert isinstance(metric, (Counter, Gauge, Histogram)), (
                f"{attr} is {type(metric).__name__}, expected Counter/Gauge/Histogram"
            )

    def test_every_metric_is_registered_in_default_registry(self) -> None:
        """Each declared metric appears in ``REGISTRY.collect()`` output."""
        registered = {family.name for family in REGISTRY.collect()}
        for attr, metric in _METRIC_PAIRS:
            base = metric._name
            # Counter families are exposed under the base name (``_total``
            # / ``_created`` suffixes are added per-sample).
            assert base in registered, (
                f"metric {attr} (base name {base!r}) not found in REGISTRY"
            )

    def test_counter_names_end_with_total(self) -> None:
        """Counters follow the ``_total`` suffix convention."""
        for attr, metric in _METRIC_PAIRS:
            if isinstance(metric, Counter):
                exposed = _exposed_name(metric)
                assert exposed.endswith("_total"), (
                    f"Counter {attr} exposed as {exposed!r}; missing _total suffix"
                )

    def test_internal_collector_errors_counter_is_registered(self) -> None:
        """Requirement 4.2 / Property 2 relies on the internal error counter."""
        assert isinstance(metrics_module.COLLECTOR_ERRORS, Counter)
        assert metrics_module.COLLECTOR_ERRORS._name == (
            "hydra_monitoring_collector_errors"
        )
        assert "collector" in metrics_module.COLLECTOR_ERRORS._labelnames


# ---------------------------------------------------------------------------
# Task 1.5 (b) — Label cardinality constraints (Requirement 3.3)
# ---------------------------------------------------------------------------


# Expected label sets per design §5. Kept explicit so label additions to the
# module are caught by the test rather than silently accepted.
_EXPECTED_LABELS: dict[str, tuple[str, ...]] = {
    # §5.1 Adapter
    "hydra_adapter_fetch_total": ("stream_id", "tier", "adapter_type", "status"),
    "hydra_adapter_fetch_duration_seconds": ("stream_id", "tier", "adapter_type"),
    "hydra_adapter_records_fetched_total": ("stream_id", "tier"),
    "hydra_adapter_records_stored_total": ("stream_id", "tier"),
    "hydra_adapter_records_deduplicated_total": ("stream_id", "tier"),
    "hydra_adapter_fallback_total": ("stream_id", "tier"),
    "hydra_adapter_health_status": ("stream_id", "tier"),
    "hydra_adapter_consecutive_failures": ("stream_id",),
    "hydra_adapter_dead_streams": (),
    # §5.2 Scheduler
    "hydra_scheduler_active_adapters": (),
    "hydra_scheduler_active_by_cadence": ("cadence",),
    "hydra_scheduler_concurrency_limit": ("cadence",),
    "hydra_scheduler_sla_misses_total": ("dag_id", "cadence"),
    "hydra_scheduler_health_status": (),
    # §5.3 Storage
    "hydra_storage_health_status": ("engine",),
    "hydra_storage_health_latency_seconds": ("engine",),
    "hydra_storage_waq_depth": ("engine",),
    "hydra_storage_dlq_depth": ("engine",),
    "hydra_storage_write_total": ("engine", "status"),
    "hydra_storage_write_duration_seconds": ("engine",),
    "hydra_storage_dedup_redis_total": ("tier",),
    "hydra_storage_dedup_pg_fallback_total": ("tier",),
    "hydra_storage_records_total": ("tier", "storage_status"),
    # §5.4 Backpressure
    "hydra_backpressure_state": ("engine",),
    "hydra_backpressure_soft_limit": ("engine",),
    "hydra_backpressure_hard_limit": ("engine",),
    # §5.5 API
    "hydra_api_rate_limit_hits_total": ("tier", "api_key_name"),
    "hydra_api_job_status": ("status",),
    "hydra_api_job_duration_seconds": ("job_type",),
    "hydra_api_active_keys": (),
    "hydra_api_error_total": ("error_code", "endpoint"),
    # §5.6 Product
    "hydra_product_generated_total": ("product_type", "classification"),
    "hydra_product_generation_duration_seconds": ("product_type",),
    "hydra_product_confidence_score": ("product_type",),
    "hydra_product_completeness_score": ("product_type",),
    "hydra_product_record_count": ("product_type",),
    "hydra_product_source_tiers": ("product_type",),
    # §5.7 Correlation
    "hydra_correlation_total": ("pipeline_id",),
    "hydra_correlation_new_total": ("pipeline_id",),
    "hydra_correlation_confidence": ("pipeline_id",),
    "hydra_correlation_run_duration_seconds": ("pipeline_id",),
    "hydra_correlation_pairs_evaluated_total": ("pipeline_id",),
    "hydra_correlation_tier_pair_total": ("tier_a", "tier_b"),
    # §5.8 Anomaly
    "hydra_anomaly_correlation_volume_zscore": ("pipeline_id",),
    "hydra_anomaly_confidence_drift_zscore": ("pipeline_id",),
    "hydra_anomaly_flag": ("detector", "pipeline_id"),
    # §5.9 Capacity
    "hydra_capacity_pg_size_bytes": (),
    "hydra_capacity_pg_table_size_bytes": ("table",),
    "hydra_capacity_es_index_size_bytes": ("index",),
    "hydra_capacity_influx_bucket_size_bytes": (),
    "hydra_capacity_minio_bucket_size_bytes": ("bucket",),
    "hydra_capacity_pg_growth_rate_bytes_per_day": (),
    "hydra_capacity_days_to_threshold": ("engine",),
    "hydra_capacity_ingestion_rate_records_per_minute": ("cadence",),
    "hydra_capacity_query_latency_p95_seconds": ("engine",),
    # §5.10 SLO
    "hydra_slo_target": ("slo_name",),
    "hydra_slo_current": ("slo_name",),
    "hydra_slo_error_budget_remaining": ("slo_name",),
    "hydra_slo_burn_rate_1h": ("slo_name",),
    "hydra_slo_burn_rate_6h": ("slo_name",),
    "hydra_slo_breached": ("slo_name",),
    # Internal
    "COLLECTOR_ERRORS": ("collector",),
}

#: Approved low-cardinality label vocabulary from Requirement 3.3. Any metric
#: label outside this set is flagged. ``status`` / ``storage_status`` /
#: ``classification`` / ``detector`` / ``adapter_type`` / ``job_type`` /
#: ``error_code`` / ``dag_id`` / ``api_key_name`` / ``table`` / ``index`` /
#: ``bucket`` / ``collector`` / ``tier_a`` / ``tier_b`` / ``slo_name`` are
#: structural discriminators that stay low-cardinality by construction per
#: design §5.
_ALLOWED_LABEL_NAMES: frozenset[str] = frozenset(
    {
        # Core low-cardinality labels called out in Requirement 3.3
        "tier",
        "cadence",
        "engine",
        "stream_id",
        "pipeline_id",
        "product_type",
        "endpoint",
        "method",
        "status_code",
        # Structural discriminators (low-cardinality by design §5)
        "status",
        "storage_status",
        "classification",
        "detector",
        "adapter_type",
        "job_type",
        "error_code",
        "dag_id",
        "api_key_name",
        "table",
        "index",
        "bucket",
        "collector",
        "tier_a",
        "tier_b",
        "slo_name",
    }
)


class TestLabelCardinality:
    """Label sets conform to design §5 and Requirement 3.3."""

    def test_expected_labels_cover_every_metric(self) -> None:
        """The ``_EXPECTED_LABELS`` table lists every declared metric.

        Catches drift if new metrics are added to the module without
        updating the expectation table."""
        declared = set(_METRIC_ATTRS)
        expected = set(_EXPECTED_LABELS)
        missing_from_table = declared - expected
        stale_in_table = expected - declared
        assert not missing_from_table, (
            f"metrics missing from _EXPECTED_LABELS: {sorted(missing_from_table)}"
        )
        assert not stale_in_table, (
            f"_EXPECTED_LABELS references unknown metrics: {sorted(stale_in_table)}"
        )

    @pytest.mark.parametrize("attr", sorted(_EXPECTED_LABELS))
    def test_metric_labelnames_match_design(self, attr: str) -> None:
        metric = getattr(metrics_module, attr)
        assert tuple(metric._labelnames) == _EXPECTED_LABELS[attr], (
            f"{attr} labelnames={metric._labelnames!r} expected "
            f"{_EXPECTED_LABELS[attr]!r}"
        )

    def test_every_label_comes_from_approved_vocabulary(self) -> None:
        """All label names across all metrics fall within the approved
        low-cardinality vocabulary (Requirement 3.3)."""
        offenders: list[str] = []
        for attr, metric in _METRIC_PAIRS:
            for label in metric._labelnames:
                if label not in _ALLOWED_LABEL_NAMES:
                    offenders.append(f"{attr}:{label}")
        assert not offenders, (
            f"Labels outside approved vocabulary (Requirement 3.3): {offenders}"
        )


# ---------------------------------------------------------------------------
# Task 1.5 (c) — Operations smoke tests
# ---------------------------------------------------------------------------


def _sample_label_value(label: str) -> str:
    """Return a deterministic, representative label value for smoke tests."""
    # Keep values realistic so that cardinality expectations are exercised.
    sample: dict[str, str] = {
        "tier": "1",
        "cadence": "hourly",
        "engine": "postgres",
        "stream_id": "test_stream",
        "pipeline_id": "entity_network",
        "product_type": "entity_dossier",
        "endpoint": "/api/v1/records",
        "method": "GET",
        "status_code": "200",
        "status": "success",
        "storage_status": "stored",
        "classification": "unclassified",
        "detector": "correlation_volume",
        "adapter_type": "rest_json",
        "job_type": "product_generation",
        "error_code": "VALIDATION_ERROR",
        "dag_id": "cadence_hourly",
        "api_key_name": "test_key",
        "table": "normalized_records",
        "index": "hydra-records",
        "bucket": "hydra-raw",
        "collector": "scheduler",
        "tier_a": "1",
        "tier_b": "2",
        "slo_name": "api_availability",
    }
    return sample[label]


def _call_operation(metric: MetricWrapperBase) -> None:
    """Exercise the natural write operation for the given metric type."""
    labelnames = list(metric._labelnames)
    bound: Any = metric
    if labelnames:
        bound = metric.labels(**{name: _sample_label_value(name) for name in labelnames})
    if isinstance(metric, Counter):
        bound.inc()
    elif isinstance(metric, Gauge):
        bound.set(1.0)
    elif isinstance(metric, Histogram):
        bound.observe(0.5)
    else:  # pragma: no cover - exhaustive above
        pytest.fail(f"unsupported metric type: {type(metric).__name__}")


class TestMetricOperations:
    """Counter ``.inc()``, Gauge ``.set()``, and Histogram ``.observe()``
    succeed for every declared metric with a valid label set.
    """

    @pytest.mark.parametrize("attr", sorted(_METRIC_ATTRS))
    def test_natural_write_operation_does_not_raise(self, attr: str) -> None:
        metric = getattr(metrics_module, attr)
        # Should not raise for any combination.
        _call_operation(metric)

    def test_counter_increment_increases_value(self) -> None:
        c = metrics_module.hydra_adapter_fetch_total
        before = c.labels(
            stream_id="probe_stream",
            tier="1",
            adapter_type="rest_json",
            status="success",
        )._value.get()
        c.labels(
            stream_id="probe_stream",
            tier="1",
            adapter_type="rest_json",
            status="success",
        ).inc()
        after = c.labels(
            stream_id="probe_stream",
            tier="1",
            adapter_type="rest_json",
            status="success",
        )._value.get()
        assert after == pytest.approx(before + 1.0)

    def test_gauge_set_replaces_value(self) -> None:
        g = metrics_module.hydra_storage_waq_depth
        g.labels(engine="postgres").set(42.0)
        assert g.labels(engine="postgres")._value.get() == pytest.approx(42.0)
        g.labels(engine="postgres").set(7.0)
        assert g.labels(engine="postgres")._value.get() == pytest.approx(7.0)

    def test_histogram_observe_records_sample(self) -> None:
        h = metrics_module.hydra_storage_write_duration_seconds
        child = h.labels(engine="postgres")
        before_count = child._sum.get()
        child.observe(0.25)
        after_count = child._sum.get()
        assert after_count == pytest.approx(before_count + 0.25)

    def test_unlabelled_gauge_set_then_read(self) -> None:
        """Gauges without labels still support ``set()`` / ``_value.get()``."""
        g = metrics_module.hydra_capacity_pg_size_bytes
        g.set(1234567.0)
        assert g._value.get() == pytest.approx(1234567.0)

    def test_labels_missing_label_raises(self) -> None:
        """``labels()`` with an unexpected keyword raises — documents
        that the label set is fixed at declaration time."""
        with pytest.raises(ValueError):
            metrics_module.hydra_adapter_fetch_total.labels(
                stream_id="x", tier="1", adapter_type="rest_json"
            )


# ---------------------------------------------------------------------------
# Task 14.4 — Integration tests: /metrics endpoint exposition
# ---------------------------------------------------------------------------
#
# These tests verify the Prometheus-compatible ``/metrics`` endpoint is
# mounted on a FastAPI app that has been instrumented via
# ``hydra.monitoring.instrumentator.instrument_app``. They exercise the
# instrumentator path end-to-end without spinning up the full
# ``setup_monitoring()`` pipeline (which would require a live PostgreSQL
# pool and other infrastructure handles).
#
# Requirements covered:
#   2.1 — GET /metrics returns 200 with text/plain Content-Type
#   2.2 — Response body is Prometheus exposition format and includes
#         all registered custom metrics (spot-check ``hydra_``-prefixed)
#   2.3 — /metrics and /api/v1/health/ping are excluded from
#         HTTP request instrumentation (so they do not appear as
#         ``http_requests_total`` handlers)
#   2.4 — The endpoint returns the latest cached metric values from the
#         default registry (verified by observing a metric and seeing
#         the updated value on scrape)

import httpx
import pytest
from fastapi import FastAPI

from hydra.api.app import create_app
from hydra.monitoring.instrumentator import instrument_app


@pytest.fixture(scope="module")
def instrumented_app() -> FastAPI:
    """Return a fresh FastAPI app with the HYDRA instrumentator attached.

    ``create_app()`` is invoked without ``pg_pool``, so the monitoring
    lifespan is a no-op and ``setup_monitoring`` is NOT called. We then
    explicitly call :func:`instrument_app` to mount ``/metrics`` and bind
    the HTTP request middleware — this isolates the instrumentator path
    from the rest of the monitoring pipeline.

    A trivial ``/__probe`` route is added so tests can trigger
    ``http_requests_total`` without hitting an excluded handler.

    The fixture is module-scoped because ``instrument_app`` registers
    HTTP metrics (``http_requests_total`` etc.) in the process-wide
    ``prometheus_client`` default registry. Re-instantiating it per test
    would hit the "duplicated timeseries" guard in
    ``metrics.default()``, which silently returns ``None`` and leaves
    the subsequent app with an instrumentator middleware that records
    nothing.
    """
    app = create_app()

    @app.get("/__probe", include_in_schema=False)
    async def _probe() -> dict[str, str]:
        return {"status": "probe"}

    instrument_app(app)
    return app


@pytest.fixture
async def metrics_client(instrumented_app: FastAPI):
    transport = httpx.ASGITransport(app=instrumented_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


class TestMetricsEndpoint:
    """End-to-end tests for the Prometheus ``/metrics`` endpoint.

    Validates: Requirements 2.1, 2.2, 2.3, 2.4
    """

    async def test_metrics_endpoint_returns_200_and_text_plain(
        self, metrics_client: httpx.AsyncClient
    ) -> None:
        """Requirement 2.1: GET /metrics → 200 with Prometheus
        ``text/plain`` Content-Type."""
        response = await metrics_client.get("/metrics")

        assert response.status_code == 200
        content_type = response.headers.get("content-type", "")
        # Prometheus exposition format is ``text/plain; version=0.0.4;
        # charset=utf-8``. The prefix check is the contract we care about.
        assert content_type.startswith("text/plain"), (
            f"expected text/plain prefix, got {content_type!r}"
        )

    async def test_metrics_response_contains_hydra_prefixed_metrics(
        self, metrics_client: httpx.AsyncClient
    ) -> None:
        """Requirement 2.2: Response body exposes all registered metrics
        from the default registry in Prometheus exposition format.

        We spot-check that at least one ``hydra_``-prefixed metric line is
        present. Module-level registration in
        ``hydra.monitoring.metrics`` means these metrics exist as soon as
        the module is imported — they appear on scrape even if their
        background collectors haven't run yet.
        """
        response = await metrics_client.get("/metrics")
        body = response.text

        # Prometheus exposition format starts metric families with
        # ``# HELP <name> ...`` / ``# TYPE <name> ...`` lines followed by
        # sample lines. Either is sufficient proof of presence.
        assert "hydra_" in body, "no hydra_-prefixed metrics in /metrics output"

        # Spot-check a specific metric known to be registered at module
        # import time. ``hydra_scheduler_health_status`` is an unlabelled
        # Gauge, so it always appears in the exposition output.
        assert "hydra_scheduler_health_status" in body, (
            "expected hydra_scheduler_health_status metric in /metrics output"
        )

    async def test_instrumentator_exposes_http_requests_total_after_request(
        self, metrics_client: httpx.AsyncClient
    ) -> None:
        """Requirement 2.1 / 2.2: After a request is made to a routed
        endpoint, the FastAPI instrumentator's ``http_requests_total``
        metric appears in the exposition output."""
        # Fire a request at a known, non-excluded route so the
        # instrumentator records it.
        probe_resp = await metrics_client.get("/__probe")
        assert probe_resp.status_code == 200

        # Now scrape /metrics and confirm the request was recorded.
        response = await metrics_client.get("/metrics")
        body = response.text

        assert "http_requests_total" in body, (
            "http_requests_total should be present after a request to a "
            "templated, non-excluded handler"
        )
        assert 'handler="/__probe"' in body, (
            "expected handler=\"/__probe\" label on http_requests_total after "
            "probe request"
        )

    async def test_metrics_endpoint_is_excluded_from_instrumentation(
        self, metrics_client: httpx.AsyncClient
    ) -> None:
        """Requirement 2.3: The ``/metrics`` endpoint itself is excluded
        from HTTP request instrumentation — scraping it must not add a
        ``http_requests_total`` sample with ``handler="/metrics"``.
        """
        # Warm up instrumentator samples by hitting a non-excluded route
        # so ``http_requests_total`` exists in the output at all.
        await metrics_client.get("/__probe")

        # Scrape twice — each scrape would add a /metrics sample if the
        # handler were not excluded.
        await metrics_client.get("/metrics")
        response = await metrics_client.get("/metrics")
        body = response.text

        assert 'handler="/metrics"' not in body, (
            "/metrics handler should be excluded from http_requests_total"
        )

    async def test_ping_endpoint_is_excluded_from_instrumentation(
        self, metrics_client: httpx.AsyncClient
    ) -> None:
        """Requirement 2.3: ``/api/v1/health/ping`` is excluded from
        HTTP request instrumentation."""
        # Hit the liveness probe — it is a matched route but excluded.
        ping_resp = await metrics_client.get("/api/v1/health/ping")
        assert ping_resp.status_code == 200

        # Also hit the probe so ``http_requests_total`` is guaranteed to
        # exist in the output.
        await metrics_client.get("/__probe")

        response = await metrics_client.get("/metrics")
        body = response.text

        assert 'handler="/api/v1/health/ping"' not in body, (
            "/api/v1/health/ping handler should be excluded from "
            "http_requests_total"
        )

    async def test_metrics_endpoint_reflects_latest_registry_values(
        self, metrics_client: httpx.AsyncClient
    ) -> None:
        """Requirement 2.4: The endpoint returns the latest cached
        metric values from the registry — observing a value immediately
        before a scrape must surface the new value in the body."""
        # Pick an unlabelled Gauge and set a distinctive sentinel value.
        metrics_module.hydra_capacity_pg_size_bytes.set(987654321.0)

        response = await metrics_client.get("/metrics")
        body = response.text

        # Prometheus exposition renders floats; accept both ``987654321.0``
        # and the scientific-notation form that ``prometheus_client`` may
        # emit for large numbers.
        assert (
            "hydra_capacity_pg_size_bytes 9.87654321e+08" in body
            or "hydra_capacity_pg_size_bytes 987654321" in body
        ), (
            "scraped /metrics body did not reflect the value just set on "
            "hydra_capacity_pg_size_bytes"
        )
