"""Unit tests for the EAS Prometheus metric catalog (task 16.6).

Verifies that every counter, gauge, and histogram listed in Design §11.1
/ R23.1 exists in :mod:`hydra.eas.metrics`, carries the documented label
set, and can be exercised through the Prometheus client API without
raising. These tests do NOT attempt to observe real production values —
integration tests live under the capability-specific test modules and
check deltas. This module is about the **catalog contract**: label
names, metric types, and label-key cardinality.

The tests skip cleanly if ``prometheus_client`` is not installed (the
fallback ``_NoopMetric`` returns itself from every accessor and has no
observable metric state). In the default Python 3.12 test environment
``prometheus_client`` is installed (confirmed by the Tier 29 tests which
read ``_value.get()``), so the full catalog runs end-to-end.

Validates: R23.1 (catalog completeness), Property 9 (label-set
contract for alert rules).
"""

from __future__ import annotations

from typing import Any

import pytest

from hydra.eas import metrics as eas_metrics

# Skip the entire module if prometheus_client is absent — the
# _NoopMetric fallback hides label errors we want to assert on.
prometheus_client = pytest.importorskip("prometheus_client")


# ---------------------------------------------------------------------------
# Expected catalog — matches Design §11.1
# ---------------------------------------------------------------------------


# Every row is (name, type-class, expected label names).
# Label names are matched as a SET — order shouldn't matter, duplicates
# would be a bug.
_COUNTER_CATALOG: list[tuple[str, frozenset[str]]] = [
    ("hydra_eas_cve_records_total", frozenset({"source"})),
    ("hydra_eas_asn_lookup_failure_total", frozenset()),
    (
        "hydra_eas_exposure_events_total",
        frozenset({"tenant_id", "asset_type", "tier", "severity"}),
    ),
    ("hydra_eas_exposure_buffer_overflow_total", frozenset()),
    ("hydra_eas_screenshot_captures_total", frozenset({"status"})),
    ("hydra_eas_screenshot_bytes_total", frozenset()),
    ("hydra_eas_lookup_cache_hits_total", frozenset({"indicator_class"})),
    ("hydra_eas_lookup_cache_misses_total", frozenset({"indicator_class"})),
    ("hydra_eas_observatory_runs_total", frozenset({"status"})),
]


_GAUGE_CATALOG: list[tuple[str, frozenset[str]]] = [
    ("hydra_eas_lookup_cache_size", frozenset()),
    (
        "hydra_eas_quota_usage_ratio",
        frozenset({"tenant_id", "quota_name"}),
    ),
    (
        "hydra_eas_observatory_last_run_timestamp_seconds",
        frozenset(),
    ),
]


_HISTOGRAM_CATALOG: list[tuple[str, frozenset[str]]] = [
    ("hydra_eas_trends_window_bytes", frozenset({"bucket"})),
    ("hydra_eas_maps_tiles_returned", frozenset({"strategy"})),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _metric_exists(name: str) -> bool:
    """Return True iff ``name`` is registered on the default Prometheus registry."""

    return name in getattr(
        prometheus_client.REGISTRY, "_names_to_collectors", {}
    )


def _metric_object(name: str) -> Any:
    """Return the live metric object for ``name`` (or the module attr fallback).

    Some metric names don't exactly match the Python attribute — e.g.
    the module attribute ``hydra_eas_cve_records_total`` matches the
    metric name of the same spelling — so the two lookups are
    equivalent. We prefer the module attribute because that's the path
    callers use at the site of emission.
    """

    attr = getattr(eas_metrics, name, None)
    if attr is None:
        return prometheus_client.REGISTRY._names_to_collectors.get(name)
    return attr


def _label_names(metric: Any) -> set[str]:
    """Extract the label names from a registered metric collector."""

    # ``prometheus_client`` collectors store their label names on
    # ``_labelnames``. Histograms and Summaries behave the same here.
    return set(getattr(metric, "_labelnames", ()) or ())


# ---------------------------------------------------------------------------
# Presence tests — every catalog entry is registered
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name", [entry[0] for entry in _COUNTER_CATALOG]
)
def test_counter_is_registered(name: str) -> None:
    """Every Design §11.1 counter is registered on the default registry."""

    assert _metric_exists(name), f"counter {name} missing from registry"


@pytest.mark.parametrize(
    "name", [entry[0] for entry in _GAUGE_CATALOG]
)
def test_gauge_is_registered(name: str) -> None:
    """Every Design §11.1 gauge is registered on the default registry."""

    assert _metric_exists(name), f"gauge {name} missing from registry"


@pytest.mark.parametrize(
    "name", [entry[0] for entry in _HISTOGRAM_CATALOG]
)
def test_histogram_is_registered(name: str) -> None:
    """Every Design §11.1 histogram is registered on the default registry."""

    assert _metric_exists(name), f"histogram {name} missing from registry"


# ---------------------------------------------------------------------------
# Type tests — counter vs gauge vs histogram
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name", [entry[0] for entry in _COUNTER_CATALOG]
)
def test_counter_has_counter_type(name: str) -> None:
    """Counters must be instances of ``prometheus_client.Counter``."""

    metric = _metric_object(name)
    assert isinstance(metric, prometheus_client.Counter), (
        f"{name} is not a Counter (got {type(metric).__name__})"
    )


@pytest.mark.parametrize(
    "name", [entry[0] for entry in _GAUGE_CATALOG]
)
def test_gauge_has_gauge_type(name: str) -> None:
    """Gauges must be instances of ``prometheus_client.Gauge``."""

    metric = _metric_object(name)
    assert isinstance(metric, prometheus_client.Gauge), (
        f"{name} is not a Gauge (got {type(metric).__name__})"
    )


@pytest.mark.parametrize(
    "name", [entry[0] for entry in _HISTOGRAM_CATALOG]
)
def test_histogram_has_histogram_type(name: str) -> None:
    """Histograms must be instances of ``prometheus_client.Histogram``."""

    metric = _metric_object(name)
    assert isinstance(metric, prometheus_client.Histogram), (
        f"{name} is not a Histogram (got {type(metric).__name__})"
    )


# ---------------------------------------------------------------------------
# Label-set tests — the Design §11.1 contract
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "expected"),
    _COUNTER_CATALOG,
    ids=[entry[0] for entry in _COUNTER_CATALOG],
)
def test_counter_label_set_matches_design(
    name: str, expected: frozenset[str]
) -> None:
    """Counter label names match Design §11.1 / R23.1."""

    metric = _metric_object(name)
    actual = _label_names(metric)
    assert actual == expected, (
        f"{name}: expected labels {sorted(expected)}, got {sorted(actual)}"
    )


@pytest.mark.parametrize(
    ("name", "expected"),
    _GAUGE_CATALOG,
    ids=[entry[0] for entry in _GAUGE_CATALOG],
)
def test_gauge_label_set_matches_design(
    name: str, expected: frozenset[str]
) -> None:
    """Gauge label names match Design §11.1 / R23.1."""

    metric = _metric_object(name)
    actual = _label_names(metric)
    assert actual == expected, (
        f"{name}: expected labels {sorted(expected)}, got {sorted(actual)}"
    )


@pytest.mark.parametrize(
    ("name", "expected"),
    _HISTOGRAM_CATALOG,
    ids=[entry[0] for entry in _HISTOGRAM_CATALOG],
)
def test_histogram_label_set_matches_design(
    name: str, expected: frozenset[str]
) -> None:
    """Histogram label names match Design §11.1 / R23.1."""

    metric = _metric_object(name)
    actual = _label_names(metric)
    assert actual == expected, (
        f"{name}: expected labels {sorted(expected)}, got {sorted(actual)}"
    )


# ---------------------------------------------------------------------------
# Emission smoke tests — exercise each accessor without error
# ---------------------------------------------------------------------------


def test_counter_inc_with_labels_does_not_raise() -> None:
    """Every labeled counter accepts ``.labels(...).inc()`` smoothly.

    Real prometheus counters reject mismatched label arities with a
    ``ValueError``; unlabeled counters reject any ``labels()`` call
    with no kwargs (you can't label an unlabeled counter). This test
    drives each counter with a realistic label payload derived from
    the catalog and asserts no exception propagates.
    """

    label_values = {
        "source": "nvd",
        "tenant_id": "11111111-1111-1111-1111-111111111111",
        "asset_type": "ip",
        "tier": "16",
        "severity": "critical",
        "status": "success",
        "indicator_class": "ipv4",
    }

    for name, expected_labels in _COUNTER_CATALOG:
        metric = _metric_object(name)
        if not expected_labels:
            # Unlabeled counters — ``inc()`` directly.
            metric.inc()
            continue
        # Build the exact kwarg set the metric demands.
        kwargs = {k: label_values[k] for k in expected_labels}
        metric.labels(**kwargs).inc()


def test_gauge_set_with_labels_does_not_raise() -> None:
    """Every gauge accepts ``.set(value)`` (with or without labels)."""

    label_values = {
        "tenant_id": "22222222-2222-2222-2222-222222222222",
        "quota_name": "screenshots_per_day",
    }

    for name, expected_labels in _GAUGE_CATALOG:
        metric = _metric_object(name)
        if not expected_labels:
            metric.set(0.0)
            continue
        kwargs = {k: label_values[k] for k in expected_labels}
        metric.labels(**kwargs).set(0.42)


def test_histogram_observe_with_labels_does_not_raise() -> None:
    """Every histogram accepts ``.observe(value)`` with the documented labels."""

    label_values = {
        "bucket": "1h",
        "strategy": "h3",
    }

    for name, expected_labels in _HISTOGRAM_CATALOG:
        metric = _metric_object(name)
        if not expected_labels:
            metric.observe(1.0)
            continue
        kwargs = {k: label_values[k] for k in expected_labels}
        metric.labels(**kwargs).observe(100.0)


# ---------------------------------------------------------------------------
# Arity-error tests — mismatched labels fail loudly
# ---------------------------------------------------------------------------


def test_mismatched_labels_raise_value_error() -> None:
    """Supplying the wrong labels to a labeled counter raises ``ValueError``.

    This is a safety net: if a future refactor silently drops a label
    from ``hydra_eas_exposure_events_total`` (for example), Alertmanager
    rules that filter on that label would silently break. The emission
    code-path that bumped the counter would raise instead of silently
    doing the wrong thing because prometheus_client validates label arity.
    """

    # ``hydra_eas_exposure_events_total`` expects 4 labels — supplying
    # 2 should fail.
    with pytest.raises(ValueError):
        eas_metrics.hydra_eas_exposure_events_total.labels(
            tenant_id="x", asset_type="ip"
        ).inc()


def test_no_label_on_labeled_counter_raises() -> None:
    """Calling a labeled counter's ``.inc()`` without a prior ``.labels()`` fails.

    ``prometheus_client`` Counters with labels don't allow direct
    ``inc`` — the call raises ``ValueError`` because there is no
    default label-set. This guard prevents accidental wire-through
    of an unlabeled bump to a labeled metric.
    """

    with pytest.raises(ValueError):
        eas_metrics.hydra_eas_cve_records_total.inc()


def test_unlabeled_counter_rejects_labels() -> None:
    """Calling ``.labels()`` on an unlabeled counter raises.

    Opposite of the above: adding an unexpected label to a counter
    that was declared label-free is a programming error.
    """

    with pytest.raises(ValueError):
        eas_metrics.hydra_eas_exposure_buffer_overflow_total.labels(
            unexpected="x"
        ).inc()


# ---------------------------------------------------------------------------
# Observatory accessor — DAG uses this instead of a direct import
# ---------------------------------------------------------------------------


def test_get_observatory_runs_counter_returns_counter() -> None:
    """``get_observatory_runs_counter`` returns the same counter instance each call.

    The daily observatory DAG is intentionally decoupled from the
    concrete metric symbol; it calls :func:`get_observatory_runs_counter`
    and treats the returned object as a Counter. We pin the contract
    with a type check and a same-instance assertion.
    """

    a = eas_metrics.get_observatory_runs_counter()
    b = eas_metrics.get_observatory_runs_counter()
    assert isinstance(a, prometheus_client.Counter)
    assert a is b
    assert a is eas_metrics.hydra_eas_observatory_runs_total


# ---------------------------------------------------------------------------
# __all__ completeness — every catalog entry is exported
# ---------------------------------------------------------------------------


def test_all_catalog_entries_are_exported() -> None:
    """Every metric symbol in the catalog is listed in ``__all__``.

    Helps prevent accidental private-ification that would break
    downstream imports like ``from hydra.eas.metrics import
    hydra_eas_cve_records_total``.
    """

    exported = set(eas_metrics.__all__)
    catalog_names = (
        {n for n, _ in _COUNTER_CATALOG}
        | {n for n, _ in _GAUGE_CATALOG}
        | {n for n, _ in _HISTOGRAM_CATALOG}
    )
    missing = catalog_names - exported
    assert not missing, f"metrics missing from __all__: {sorted(missing)}"
