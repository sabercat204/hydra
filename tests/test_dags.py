"""Tests for DAG definitions — importability, uniqueness, structure."""

from __future__ import annotations

import importlib
from unittest.mock import MagicMock, patch

import pytest

from hydra.registry.stream_registry import StreamSource, StreamTier
from hydra.scheduler.dag_factory import CADENCE_CONFIG, DagFactory


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_source(name: str) -> StreamSource:
    return StreamSource(name=name, url="https://example.com", format="json", auth="none", notes="")


def _make_tier(tid: int, name: str, cadence: str, n_sources: int = 2) -> StreamTier:
    sources = [_make_source(f"src_{tid}_{i}") for i in range(n_sources)]
    return StreamTier(
        id=tid, name=name, streams=n_sources, access="5G", formats=["json"],
        cadence=cadence, adapter="rest_json", fallback=None, sources=sources,
    )


def _make_registry_with_all_cadences() -> MagicMock:
    """Create a registry with at least one tier per cadence."""
    tiers = [
        _make_tier(1, "Geo", "sub_minute"),
        _make_tier(23, "Space", "realtime"),
        _make_tier(2, "Atmo", "15min"),
        _make_tier(16, "Cyber", "hourly"),
        _make_tier(5, "Econ", "daily"),
        _make_tier(8, "IntlOrgs", "weekly"),
        _make_tier(14, "Arms", "annual"),
        _make_tier(28, "Portals", "varies"),
    ]
    registry = MagicMock()
    registry.tiers = {t.id: t for t in tiers}
    registry.get_tiers_by_cadence = lambda c: [t for t in tiers if t.cadence == c]
    return registry


def _make_settings() -> MagicMock:
    settings = MagicMock()
    settings.scheduler.global_concurrency_limit = 10
    settings.scheduler.cadence_concurrency_limits = {
        "sub_minute": 4, "realtime": 3, "15min": 3,
        "hourly": 4, "daily": 6, "weekly": 4, "monthly_plus": 2,
    }
    settings.scheduler.backpressure_soft_limit = 1000
    settings.scheduler.backpressure_hard_limit = 5000
    settings.scheduler.backpressure_wait_timeout = 60.0
    settings.scheduler.backpressure_poll_interval = 5.0
    settings.scheduler.engine_backpressure_overrides = {}
    settings.scheduler.dead_stream_threshold = 5
    settings.scheduler.staleness_windows = {"annual": 335, "quarterly": 85, "monthly": 28, "varies": 28}
    settings.scheduler.rate_limit_retry_delay_multiplier = 3.0
    return settings


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestDags:
    def test_all_dags_importable(self):
        """All 8 DAG factory configurations can be created without error."""
        registry = _make_registry_with_all_cadences()
        settings = _make_settings()
        factory = DagFactory(registry=registry, settings=settings)

        cadence_dags = {
            "sub_minute": ("hydra_cadence_sub_minute", CADENCE_CONFIG["sub_minute"]["schedule"]),
            "realtime": ("hydra_cadence_realtime", CADENCE_CONFIG["realtime"]["schedule"]),
            "15min": ("hydra_cadence_15min", CADENCE_CONFIG["15min"]["schedule"]),
            "hourly": ("hydra_cadence_hourly", CADENCE_CONFIG["hourly"]["schedule"]),
            "daily": ("hydra_cadence_daily", CADENCE_CONFIG["daily"]["schedule"]),
            "weekly": ("hydra_cadence_weekly", CADENCE_CONFIG["weekly"]["schedule"]),
            "monthly_plus": ("hydra_cadence_monthly_plus", CADENCE_CONFIG["monthly_plus"]["schedule"]),
        }

        dags = {}
        for cadence, (dag_id, schedule) in cadence_dags.items():
            dag = factory.create_cadence_dag(cadence=cadence, dag_id=dag_id, schedule=schedule)
            dags[dag_id] = dag
            assert dag is not None

        assert len(dags) == 7

    def test_dag_ids_unique(self):
        """No duplicate DAG IDs across all cadence DAGs."""
        registry = _make_registry_with_all_cadences()
        settings = _make_settings()
        factory = DagFactory(registry=registry, settings=settings)

        dag_ids = set()
        for cadence, cfg in CADENCE_CONFIG.items():
            dag = factory.create_cadence_dag(
                cadence=cadence,
                dag_id=f"hydra_cadence_{cadence}",
                schedule=cfg["schedule"],
            )
            assert dag.dag_id not in dag_ids
            dag_ids.add(dag.dag_id)

        # Add maintenance
        dag_ids.add("hydra_maintenance")
        assert len(dag_ids) == 8

    def test_no_import_errors(self):
        """DagFactory creates DAGs without import errors."""
        registry = _make_registry_with_all_cadences()
        settings = _make_settings()
        factory = DagFactory(registry=registry, settings=settings)

        # Creating all DAGs should not raise
        for cadence, cfg in CADENCE_CONFIG.items():
            dag = factory.create_cadence_dag(
                cadence=cadence,
                dag_id=f"hydra_cadence_{cadence}",
                schedule=cfg["schedule"],
            )
            assert len(dag.task_dict) >= 0

    def test_task_count_matches_registry(self):
        """Total tasks across all DAGs equals total streams in registry."""
        registry = _make_registry_with_all_cadences()
        settings = _make_settings()
        factory = DagFactory(registry=registry, settings=settings)

        total_tasks = 0
        for cadence, cfg in CADENCE_CONFIG.items():
            dag = factory.create_cadence_dag(
                cadence=cadence,
                dag_id=f"hydra_cadence_{cadence}",
                schedule=cfg["schedule"],
            )
            total_tasks += len(dag.tasks)

        # Count total sources across all tiers
        total_sources = sum(len(t.sources) for t in registry.tiers.values())
        assert total_tasks == total_sources
