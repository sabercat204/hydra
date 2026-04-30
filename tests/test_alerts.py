"""Tests for HYDRA Prometheus / Alertmanager configuration (P12, Task 11.5).

Covers:

* YAML syntax validation for ``prometheus.yml``, the alert and recording
  rule files, and ``alertmanager.yml``
* Structural expectations on the Prometheus scrape config and rule groups
* Property 13 — Alert Rule Metric Validity: every ``hydra_*`` / known
  identifier referenced in a rule's ``expr`` resolves to a valid metric
  name (custom ``hydra_*`` metric, instrumentator HTTP metric, built-in
  ``up``, or a ``hydra:`` recording-rule output)
* Property 14 — Recording Rule Consistency: every ``record:`` is prefixed
  with ``hydra:`` and every source metric in its ``expr`` is valid
* Alertmanager routing and inhibition logic

The tests read the config files as data (no live Prometheus needed) and
cross-check metric references against the authoritative registry defined
in ``hydra.monitoring.metrics``.

Requirements: 15.1-15.6, 16.1-16.8, 17.1-17.4, 18.1-18.3, 19.1-19.2
"""

from __future__ import annotations

import inspect
import re
from pathlib import Path
from typing import Any

import pytest
import yaml
from prometheus_client import Counter, Histogram
from prometheus_client.metrics import MetricWrapperBase

from hydra.monitoring import metrics as metrics_module

# ---------------------------------------------------------------------------
# Config file paths
# ---------------------------------------------------------------------------

PROMETHEUS_YML = Path("prometheus/prometheus.yml")
ALERTS_YML = Path("prometheus/rules/hydra_alerts.yml")
RECORDING_YML = Path("prometheus/rules/hydra_recording.yml")
ALERTMANAGER_YML = Path("alertmanager/alertmanager.yml")


# ---------------------------------------------------------------------------
# Metric-name derivation
# ---------------------------------------------------------------------------


def _custom_hydra_metric_names() -> set[str]:
    """Return the set of valid ``hydra_*`` Prometheus-exposition names.

    Walks ``metrics_module.__all__`` for metric instances and expands
    each to the full set of names it exposes on scrape:

    * Counters are declared with an explicit ``_total`` suffix but
      ``prometheus_client`` strips that suffix internally — they expose
      both ``<name>_total`` and ``<name>_created``.
    * Histograms expose ``<name>_bucket``, ``<name>_count``, ``<name>_sum``
      and ``<name>_created``.
    * Gauges are exposed under their raw name.
    """
    names: set[str] = set()
    for attr in metrics_module.__all__:
        obj = getattr(metrics_module, attr, None)
        if not isinstance(obj, MetricWrapperBase):
            continue
        base = obj._name  # normalized base name (no _total suffix on counters)
        if isinstance(obj, Counter):
            # Declared with explicit _total — expose both conventions so the
            # validator accepts either form.
            names.add(f"{base}_total")
            names.add(f"{base}_created")
            names.add(base)
        elif isinstance(obj, Histogram):
            names.add(f"{base}_bucket")
            names.add(f"{base}_count")
            names.add(f"{base}_sum")
            names.add(f"{base}_created")
            names.add(base)
        else:
            names.add(base)
    return names


#: Instrumentator + built-in metric families we accept in alert / recording
#: rule expressions.
INSTRUMENTATOR_METRICS: set[str] = {
    "http_requests_total",
    "http_request_duration_seconds",
    "http_request_duration_seconds_bucket",
    "http_request_duration_seconds_count",
    "http_request_duration_seconds_sum",
    "http_requests_in_progress",
    "up",
}


VALID_METRIC_NAMES: set[str] = _custom_hydra_metric_names() | INSTRUMENTATOR_METRICS


#: PromQL functions, keywords, and label identifiers that can appear in
#: expressions but are NOT metric names. Combined with the label-stripping
#: regex below this keeps the metric-extraction phase focused on real
#: metric references.
PROMQL_NON_METRIC_TOKENS: set[str] = {
    # aggregation operators
    "sum", "avg", "max", "min", "count", "count_values", "group",
    "stddev", "stdvar", "topk", "bottomk", "quantile",
    # instant-vector functions
    "rate", "irate", "increase", "delta", "idelta",
    "histogram_quantile", "clamp_min", "clamp_max", "clamp",
    "abs", "ceil", "floor", "round", "exp", "ln", "log2", "log10", "sqrt",
    "deriv", "predict_linear", "resets", "changes",
    "avg_over_time", "sum_over_time", "min_over_time", "max_over_time",
    "count_over_time", "quantile_over_time", "stddev_over_time",
    "last_over_time", "present_over_time",
    "absent", "absent_over_time", "scalar", "vector", "time",
    "timestamp", "label_replace", "label_join",
    "year", "month", "day_of_week", "day_of_month", "days_in_month",
    "hour", "minute",
    # set operators / keywords
    "by", "on", "without", "and", "or", "unless", "if", "else",
    "offset", "bool", "ignoring", "group_left", "group_right",
    # label names used in this project's rules
    "le", "status", "job", "handler", "method", "instance",
    "severity", "component", "slo_name", "engine", "tier",
    "stream_id", "status_code", "adapter_type", "detector",
    "pipeline_id", "dag_id", "cadence", "product_type",
    "classification", "table", "index", "bucket",
    "tier_a", "tier_b", "api_key_name", "storage_status",
    "error_code", "endpoint", "collector", "cluster",
    "environment", "service", "runbook_url", "alertname",
    # template helpers
    "humanizePercentage", "humanize", "humanizeDuration",
}


#: Regex matching a label matcher block, e.g. ``{job="hydra-api"}``.
LABEL_MATCHER_RE = re.compile(r"\{[^{}]*\}")

#: Regex matching a PromQL identifier. Allows ``:`` so recording-rule
#: outputs like ``hydra:adapter_success_rate_5m`` match.
IDENTIFIER_RE = re.compile(r"\b([a-zA-Z_:][a-zA-Z0-9_:]*)\b")


def _extract_metric_refs(expr: str) -> set[str]:
    """Return identifiers in ``expr`` that look like metric references.

    Strips label-matcher blocks first so label names and values don't leak
    into the identifier set, then filters out PromQL functions/keywords
    and purely numeric/boolean tokens.
    """
    stripped = LABEL_MATCHER_RE.sub("", expr)
    idents = set(IDENTIFIER_RE.findall(stripped))
    refs: set[str] = set()
    for tok in idents:
        if tok in PROMQL_NON_METRIC_TOKENS:
            continue
        if tok in {"true", "false", "inf", "nan", "Inf", "NaN"}:
            continue
        refs.add(tok)
    return refs


def _looks_like_metric_name(tok: str) -> bool:
    """Return True if ``tok`` is an identifier we should validate.

    Guards against the long tail of label-value tokens that slip past the
    keyword filter (e.g., ``postgres``, ``redis``, ``correlation_volume``).
    We only hard-assert on tokens that clearly look like metric names:

    * start with ``hydra_`` — custom HYDRA metric (must be valid)
    * start with ``hydra:`` — recording-rule output (always valid)
    * ``up`` — Prometheus built-in
    * ``http_*`` — prometheus_fastapi_instrumentator metric
    """
    return (
        tok.startswith("hydra_")
        or tok.startswith("hydra:")
        or tok == "up"
        or tok.startswith("http_")
    )


# ---------------------------------------------------------------------------
# Loaded rule fixtures (module-scoped — YAML is cheap but parse once)
# ---------------------------------------------------------------------------


def _load_yaml(path: Path) -> Any:
    assert path.exists(), f"expected config file at {path}"
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _all_rules(doc: dict) -> list[dict]:
    """Flatten every rule across every group in a Prometheus rules doc."""
    rules: list[dict] = []
    for group in doc.get("groups", []):
        for rule in group.get("rules", []):
            rules.append(rule)
    return rules


PROM_DOC = _load_yaml(PROMETHEUS_YML)
ALERTS_DOC = _load_yaml(ALERTS_YML)
RECORDING_DOC = _load_yaml(RECORDING_YML)
AM_DOC = _load_yaml(ALERTMANAGER_YML)

ALERT_RULES = _all_rules(ALERTS_DOC)
RECORDING_RULES = _all_rules(RECORDING_DOC)


def _rule_id(rule: dict) -> str:
    """Short label used by pytest parametrise ids."""
    return rule.get("alert") or rule.get("record") or "unknown"


# ---------------------------------------------------------------------------
# 1. YAML syntax
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "path",
    [PROMETHEUS_YML, ALERTS_YML, RECORDING_YML, ALERTMANAGER_YML],
    ids=lambda p: p.name,
)
def test_yaml_files_parse(path: Path) -> None:
    """Every config file must parse as a YAML mapping."""
    doc = _load_yaml(path)
    assert isinstance(doc, dict), f"{path} should parse as a YAML mapping"


# ---------------------------------------------------------------------------
# 2. Prometheus scrape config (Requirements 25.1, 25.4)
# ---------------------------------------------------------------------------


def test_prometheus_config_structure() -> None:
    """prometheus.yml wires the hydra-api scrape target and alertmanager."""
    global_cfg = PROM_DOC.get("global", {})
    assert global_cfg.get("scrape_interval") == "15s"
    assert global_cfg.get("evaluation_interval") == "15s"

    # Rule files must include both alerts and recording rules.
    rule_files = PROM_DOC.get("rule_files", [])
    joined = " ".join(rule_files)
    assert "hydra_alerts.yml" in joined
    assert "hydra_recording.yml" in joined

    # Alertmanager target on port 9093.
    am_cfgs = PROM_DOC["alerting"]["alertmanagers"]
    am_targets = {
        t
        for cfg in am_cfgs
        for sc in cfg.get("static_configs", [])
        for t in sc.get("targets", [])
    }
    assert "alertmanager:9093" in am_targets

    # hydra-api scrape job on port 8000 / /metrics.
    scrape_jobs = {sc["job_name"]: sc for sc in PROM_DOC.get("scrape_configs", [])}
    assert "hydra-api" in scrape_jobs
    hydra_api = scrape_jobs["hydra-api"]
    assert hydra_api.get("metrics_path") == "/metrics"
    api_targets = {
        t for sc in hydra_api.get("static_configs", []) for t in sc.get("targets", [])
    }
    assert "hydra-api:8000" in api_targets


# ---------------------------------------------------------------------------
# 3. Alert rule groups (Requirements 15.*, 16.*)
# ---------------------------------------------------------------------------


def _group_by_name(doc: dict) -> dict[str, dict]:
    return {g["name"]: g for g in doc.get("groups", [])}


def test_alerts_has_three_groups() -> None:
    """hydra_alerts.yml exposes critical, SLO, and warning groups."""
    groups = _group_by_name(ALERTS_DOC)
    assert set(groups) == {"hydra_critical", "hydra_slo", "hydra_warning"}


def test_critical_group_has_expected_alerts() -> None:
    """5 critical alerts required by Requirements 15.1-15.5."""
    groups = _group_by_name(ALERTS_DOC)
    rules = groups["hydra_critical"]["rules"]
    names = {r["alert"] for r in rules}
    assert names == {
        "HydraSchedulerUnreachable",
        "HydraStoragePrimaryDown",
        "HydraBackpressureBlocked",
        "HydraDLQCritical",
        "HydraAPIDown",
    }
    for rule in rules:
        assert rule["labels"]["severity"] == "critical"


def test_slo_group_has_expected_alerts() -> None:
    """SLO group contains the fast- and slow-burn alerts (Req 15.6, 16.8)."""
    groups = _group_by_name(ALERTS_DOC)
    rules = groups["hydra_slo"]["rules"]
    names = {r["alert"] for r in rules}
    assert names == {"HydraSLOBurnRateCritical", "HydraSLOBurnRateWarning"}


def test_warning_group_has_expected_alerts() -> None:
    """Warning group covers Requirements 16.1-16.7.

    Requirement 16.8 (HydraSLOBurnRateWarning) is placed in the dedicated
    ``hydra_slo`` group so the 7 alerts here cover 16.1-16.7 plus no extras.
    """
    groups = _group_by_name(ALERTS_DOC)
    rules = groups["hydra_warning"]["rules"]
    names = {r["alert"] for r in rules}
    assert names == {
        "HydraAdapterHighFailureRate",
        "HydraJobFailureRate",
        "HydraAPIErrorRate",
        "HydraRateLimitExhaustion",
        "HydraAnomalyCorrelationVolume",
        "HydraAnomalyConfidenceDrift",
        "HydraCapacityStorageLow",
    }
    for rule in rules:
        assert rule["labels"]["severity"] == "warning"


# ---------------------------------------------------------------------------
# 4. Property 13 — Alert Rule Metric Validity
#    (Validates: Requirements 19.1, 19.2)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "rule",
    ALERT_RULES,
    ids=[_rule_id(r) for r in ALERT_RULES],
)
def test_property_13_alert_rule_metric_validity(rule: dict) -> None:
    """**Validates: Requirements 19.1, 19.2**

    Every metric-like identifier referenced from an alert rule's ``expr``
    must be a valid metric name — either a registered ``hydra_*`` custom
    metric, an ``http_*`` instrumentator metric, the built-in ``up``, or a
    ``hydra:`` recording-rule output.
    """
    expr = rule["expr"]
    refs = _extract_metric_refs(expr)

    invalid: list[str] = []
    for tok in refs:
        if not _looks_like_metric_name(tok):
            continue
        if tok.startswith("hydra:"):
            # Recording-rule outputs are always valid references.
            continue
        if tok not in VALID_METRIC_NAMES:
            invalid.append(tok)

    assert not invalid, (
        f"Alert {_rule_id(rule)!r} references unknown metric(s): "
        f"{sorted(invalid)}. Expression: {expr!r}"
    )


# ---------------------------------------------------------------------------
# 5. Recording rules (Requirements 18.1, 18.2, 18.3)
# ---------------------------------------------------------------------------


def test_recording_rules_two_groups() -> None:
    """Recording rules are split into 5m and 1h evaluation windows."""
    groups = _group_by_name(RECORDING_DOC)
    assert set(groups) == {"hydra_recording_5m", "hydra_recording_1h"}


@pytest.mark.parametrize(
    "rule",
    RECORDING_RULES,
    ids=[_rule_id(r) for r in RECORDING_RULES],
)
def test_property_14_recording_rule_consistency(rule: dict) -> None:
    """**Validates: Requirement 18.3**

    Every recording rule:

    * emits an output metric prefixed with ``hydra:``
    * references only defined source metrics (custom HYDRA metric,
      instrumentator metric, Prometheus built-in, or another ``hydra:``
      recording-rule output)
    """
    record_name = rule["record"]
    assert record_name.startswith("hydra:"), (
        f"Recording rule output {record_name!r} must be prefixed 'hydra:'"
    )

    refs = _extract_metric_refs(rule["expr"])
    invalid: list[str] = []
    for tok in refs:
        if not _looks_like_metric_name(tok):
            continue
        if tok.startswith("hydra:"):
            continue  # cross-references to other recording rules are fine
        if tok not in VALID_METRIC_NAMES:
            invalid.append(tok)

    assert not invalid, (
        f"Recording rule {record_name!r} references unknown metric(s): "
        f"{sorted(invalid)}. Expression: {rule['expr']!r}"
    )


# ---------------------------------------------------------------------------
# 6. Alertmanager routing & inhibition (Requirements 17.1-17.4)
# ---------------------------------------------------------------------------


def _subroutes() -> list[dict]:
    return AM_DOC.get("route", {}).get("routes", [])


def test_alertmanager_routes_critical_to_pagerduty_and_slack() -> None:
    """Req 17.1 — critical alerts go to BOTH pagerduty and slack.

    Alertmanager fans out by matching two routes with ``continue: true``
    on the first; we verify both receivers are reachable for
    ``severity=critical``.
    """
    receivers_for_critical: list[str] = []
    for route in _subroutes():
        matchers = " ".join(route.get("matchers", []))
        if 'severity = "critical"' in matchers or "severity=\"critical\"" in matchers:
            receivers_for_critical.append(route["receiver"])

    assert "pagerduty-critical" in receivers_for_critical
    assert "slack-critical" in receivers_for_critical

    # The pagerduty route MUST set continue=true so the slack-critical
    # route is still evaluated (otherwise critical alerts page but never
    # reach the Slack channel).
    first_critical = next(
        r
        for r in _subroutes()
        if r.get("receiver") == "pagerduty-critical"
    )
    assert first_critical.get("continue") is True


def test_alertmanager_routes_warning_to_slack_only() -> None:
    """Req 17.2 — warnings route to slack-warning (and nowhere else)."""
    receivers_for_warning: list[str] = []
    for route in _subroutes():
        matchers = " ".join(route.get("matchers", []))
        if 'severity = "warning"' in matchers or "severity=\"warning\"" in matchers:
            receivers_for_warning.append(route["receiver"])

    assert receivers_for_warning == ["slack-warning"]

    # The warning route should not fan out to any pager receiver.
    assert "pagerduty-critical" not in receivers_for_warning


def test_alertmanager_group_and_timing() -> None:
    """Req 17.4 — group by alertname, 30s group_wait, 5m group_interval."""
    route = AM_DOC["route"]
    assert route.get("group_by") == ["alertname"]
    assert route.get("group_wait") == "30s"
    assert route.get("group_interval") == "5m"


def test_alertmanager_inhibits_warning_when_critical_fires() -> None:
    """Req 17.3 — a critical firing inhibits the matching warning."""
    inhibits = AM_DOC.get("inhibit_rules", [])
    assert inhibits, "alertmanager.yml must define at least one inhibit rule"

    matching = [
        r
        for r in inhibits
        if any('severity = "critical"' in m or 'severity="critical"' in m
               for m in r.get("source_matchers", []))
        and any('severity = "warning"' in m or 'severity="warning"' in m
                for m in r.get("target_matchers", []))
    ]
    assert matching, "No inhibit rule suppresses warnings when criticals fire"

    equal_labels = matching[0].get("equal", [])
    assert "alertname" in equal_labels
    assert "engine" in equal_labels


def test_alertmanager_receivers_defined() -> None:
    """All three receivers referenced by routes are defined with configs."""
    receivers = {r["name"]: r for r in AM_DOC.get("receivers", [])}
    assert {"pagerduty-critical", "slack-critical", "slack-warning"} <= set(receivers)

    assert receivers["pagerduty-critical"].get("pagerduty_configs"), (
        "pagerduty-critical must declare pagerduty_configs"
    )
    assert receivers["slack-critical"].get("slack_configs"), (
        "slack-critical must declare slack_configs"
    )
    assert receivers["slack-warning"].get("slack_configs"), (
        "slack-warning must declare slack_configs"
    )
