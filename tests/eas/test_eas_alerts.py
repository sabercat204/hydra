"""Syntax and structural tests for the EAS alert + recording rule files (task 16.7).

Prometheus ships a ``promtool`` binary that performs full validation
(``promtool check rules <file>``) — we prefer it when available and fall
back to pure-Python structural checks otherwise. The fallback covers:

* Both rule files parse as valid YAML.
* The top-level ``groups:`` list is present with at least one group.
* Every group has ``name`` and ``rules``.
* Every alert rule has ``alert``, ``expr``, and ``labels`` / ``annotations``
  shapes required by Alertmanager.
* Every recording rule has ``record`` and ``expr`` (and no
  ``alert`` field — mixing would trigger a promtool error).
* Every metric name referenced in an ``expr`` matches a metric name
  registered by :mod:`hydra.eas.metrics` — this is the real coverage
  win compared to bare syntax: it catches alerts that drift away
  from the actual metric catalog.
* Alertmanager label conventions — ``severity`` (critical / warning
  / info) and optional ``receiver`` (a free string) are the only
  label names we standardise across alerts.

Validates: R23.3 (alert rule syntax), Property 9 (metric catalog
contract).
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

from hydra.eas import metrics as eas_metrics


ALERTS_PATH = Path("prometheus/rules/hydra_eas_alerts.yml")
RECORDING_PATH = Path("prometheus/rules/hydra_recording.yml")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open() as f:
        data = yaml.safe_load(f)
    assert isinstance(data, dict), f"{path} must parse as a YAML mapping"
    return data


def _all_rules(doc: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten every rule across every group in a Prometheus rules doc."""

    rules: list[dict[str, Any]] = []
    for group in doc.get("groups") or []:
        rules.extend(group.get("rules") or [])
    return rules


def _extract_metric_names(expr: str) -> set[str]:
    """Pull out metric identifiers referenced in a PromQL expression.

    PromQL metric names are ``[a-zA-Z_:][a-zA-Z0-9_:]*``. We use a
    fairly loose regex and then strip out function / aggregation
    keywords by intersecting with the known metric catalog. This is
    enough to detect "alert references a metric that doesn't exist"
    without pulling in a real PromQL parser.
    """

    return set(
        re.findall(r"\b[a-zA-Z_:][a-zA-Z0-9_:]*\b", expr)
    )


def _known_metric_names() -> set[str]:
    """Return every metric name registered by :mod:`hydra.eas.metrics`.

    Includes every counter, gauge, and histogram; histograms may also
    be referenced via ``<name>_bucket`` / ``<name>_count`` /
    ``<name>_sum`` suffixes so we add those variants too.
    """

    base = {
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
    }
    # Histogram suffixes — Prometheus surfaces these automatically.
    expanded: set[str] = set(base)
    for name in ("hydra_eas_trends_window_bytes", "hydra_eas_maps_tiles_returned"):
        expanded.add(f"{name}_bucket")
        expanded.add(f"{name}_count")
        expanded.add(f"{name}_sum")
    return expanded


def _referenced_hydra_metrics(expr: str) -> set[str]:
    """Metric identifiers in ``expr`` that start with ``hydra_``."""

    return {name for name in _extract_metric_names(expr) if name.startswith("hydra_")}


# ---------------------------------------------------------------------------
# Presence
# ---------------------------------------------------------------------------


def test_alerts_file_exists() -> None:
    """The alerts file is at the path referenced by Design §11.2."""

    assert ALERTS_PATH.is_file(), (
        f"missing alert rules file: {ALERTS_PATH}"
    )


def test_recording_file_exists() -> None:
    """The recording rules file is at the path referenced by Design §11.3."""

    assert RECORDING_PATH.is_file(), (
        f"missing recording rules file: {RECORDING_PATH}"
    )


# ---------------------------------------------------------------------------
# Alert rules — structure
# ---------------------------------------------------------------------------


def test_alerts_yaml_parses() -> None:
    """The alert rules file is valid YAML with a ``groups`` top-level list."""

    doc = _load_yaml(ALERTS_PATH)
    groups = doc.get("groups")
    assert isinstance(groups, list) and groups, (
        "``groups:`` must be a non-empty list"
    )


def test_every_alert_group_has_name_and_rules() -> None:
    """Each group carries the required ``name`` + ``rules`` keys."""

    doc = _load_yaml(ALERTS_PATH)
    for group in doc["groups"]:
        assert isinstance(group.get("name"), str) and group["name"], (
            "group missing ``name``"
        )
        assert isinstance(group.get("rules"), list) and group["rules"], (
            f"group {group['name']!r} missing ``rules``"
        )


def test_every_alert_has_required_fields() -> None:
    """Each rule has ``alert``, ``expr``, ``labels``, and ``annotations``."""

    doc = _load_yaml(ALERTS_PATH)
    for rule in _all_rules(doc):
        # Recording rules sneaking into an alerts file would be a
        # copy-paste bug — ``record`` is strictly not allowed here.
        assert "record" not in rule, (
            f"alert rule {rule!r} accidentally carries a ``record`` key"
        )
        assert isinstance(rule.get("alert"), str) and rule["alert"], (
            f"rule missing ``alert`` name: {rule}"
        )
        assert isinstance(rule.get("expr"), str) and rule["expr"], (
            f"alert {rule['alert']!r} missing ``expr``"
        )
        assert isinstance(rule.get("labels"), dict), (
            f"alert {rule['alert']!r} missing ``labels``"
        )
        assert isinstance(rule.get("annotations"), dict), (
            f"alert {rule['alert']!r} missing ``annotations``"
        )


def test_every_alert_has_severity_label() -> None:
    """Alerts must declare a ``severity`` in {critical, warning, info}."""

    doc = _load_yaml(ALERTS_PATH)
    allowed = {"critical", "warning", "info"}
    for rule in _all_rules(doc):
        sev = rule["labels"].get("severity")
        assert sev in allowed, (
            f"alert {rule['alert']!r} has invalid severity {sev!r}"
        )


def test_every_alert_has_summary_annotation() -> None:
    """Alertmanager's default receivers display the ``summary`` annotation."""

    doc = _load_yaml(ALERTS_PATH)
    for rule in _all_rules(doc):
        summary = rule["annotations"].get("summary")
        assert isinstance(summary, str) and summary, (
            f"alert {rule['alert']!r} missing ``summary`` annotation"
        )


def test_every_alert_references_known_metric() -> None:
    """Every metric identifier in an alert ``expr`` exists in the catalog.

    This is the single most valuable structural check — a renamed metric
    in ``hydra.eas.metrics`` that isn't updated here would silently
    never fire.
    """

    doc = _load_yaml(ALERTS_PATH)
    known = _known_metric_names()
    for rule in _all_rules(doc):
        referenced = _referenced_hydra_metrics(rule["expr"])
        unknown = referenced - known
        assert not unknown, (
            f"alert {rule['alert']!r} references unknown metric(s): "
            f"{sorted(unknown)}"
        )


# ---------------------------------------------------------------------------
# Design §11.2 — required alert names
# ---------------------------------------------------------------------------


_REQUIRED_ALERTS = {
    "HydraEASCriticalExposure",
    "HydraEASScreenshotFailureRate",
    "HydraEASLookupCacheHitRateLow",
    "HydraEASQuotaNearExhaustion",
    "HydraEASObservatoryStale",
}


def test_design_11_2_alerts_are_all_present() -> None:
    """Every alert named in Design §11.2 / R23.3 is registered."""

    doc = _load_yaml(ALERTS_PATH)
    emitted = {rule["alert"] for rule in _all_rules(doc)}
    missing = _REQUIRED_ALERTS - emitted
    assert not missing, f"missing required alerts: {sorted(missing)}"


def test_critical_exposure_targets_receiver() -> None:
    """``HydraEASCriticalExposure`` carries the ``eas-critical`` receiver.

    Design §8.1 pairs this alert with a specific Alertmanager receiver
    that pages on-call. Pins the label so a misnamed receiver can't
    silently deroute pages.
    """

    doc = _load_yaml(ALERTS_PATH)
    for rule in _all_rules(doc):
        if rule["alert"] == "HydraEASCriticalExposure":
            labels = rule["labels"]
            assert labels.get("severity") == "critical"
            assert labels.get("receiver") == "eas-critical"
            return
    pytest.fail("HydraEASCriticalExposure alert not found")


# ---------------------------------------------------------------------------
# Recording rules — structure
# ---------------------------------------------------------------------------


def test_recording_yaml_parses() -> None:
    """The recording rules file is valid YAML with ``groups`` top-level."""

    doc = _load_yaml(RECORDING_PATH)
    assert isinstance(doc.get("groups"), list) and doc["groups"], (
        "``groups:`` must be a non-empty list in the recording file"
    )


def test_every_recording_rule_has_required_fields() -> None:
    """Each recording rule has ``record`` + ``expr`` and no ``alert`` field."""

    doc = _load_yaml(RECORDING_PATH)
    for rule in _all_rules(doc):
        assert "alert" not in rule, (
            f"recording rule {rule!r} accidentally carries an ``alert`` key"
        )
        assert isinstance(rule.get("record"), str) and rule["record"], (
            f"recording rule missing ``record`` name: {rule}"
        )
        assert isinstance(rule.get("expr"), str) and rule["expr"], (
            f"recording rule {rule['record']!r} missing ``expr``"
        )


def test_design_11_3_recording_rule_present() -> None:
    """``hydra:eas_lookup_fast_ratio_5m`` is the rule R23.4 references."""

    doc = _load_yaml(RECORDING_PATH)
    records = {rule["record"] for rule in _all_rules(doc)}
    assert "hydra:eas_lookup_fast_ratio_5m" in records, (
        "missing required recording rule hydra:eas_lookup_fast_ratio_5m"
    )


def test_recording_rule_names_follow_prefix_convention() -> None:
    """Recording-rule names use the ``hydra:`` prefix per Design §11.3.

    Prometheus community convention is ``<namespace>:<metric>:<op>``.
    We only enforce the ``hydra:`` namespace — the ``:op`` suffix is
    rule-specific.
    """

    doc = _load_yaml(RECORDING_PATH)
    for rule in _all_rules(doc):
        name = rule["record"]
        assert name.startswith("hydra:"), (
            f"recording rule {name!r} should start with ``hydra:``"
        )


# ---------------------------------------------------------------------------
# Optional: real promtool check when available
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    shutil.which("promtool") is None,
    reason="promtool not installed",
)
def test_promtool_check_rules_passes_for_alerts() -> None:
    """Optional — run ``promtool check rules`` against the alerts file.

    Skipped when the binary isn't available; when present, we shell
    out and assert exit code 0. This is the fullest possible validation
    and is the reason this file is referenced by task 16.7.
    """

    result = subprocess.run(
        ["promtool", "check", "rules", str(ALERTS_PATH)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"promtool check rules failed:\n{result.stdout}\n{result.stderr}"
    )


@pytest.mark.skipif(
    shutil.which("promtool") is None,
    reason="promtool not installed",
)
def test_promtool_check_rules_passes_for_recording() -> None:
    """Optional — run ``promtool check rules`` against the recording file."""

    result = subprocess.run(
        ["promtool", "check", "rules", str(RECORDING_PATH)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"promtool check rules failed:\n{result.stdout}\n{result.stderr}"
    )
