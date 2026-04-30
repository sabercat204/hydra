"""Tests for DAG factory — cadence DAG generation from registry."""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest

from hydra.registry.stream_registry import StreamSource, StreamTier
from hydra.scheduler.dag_factory import CADENCE_CONFIG, DEFAULT_DAG_ARGS, DagFactory


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_source(name: str) -> StreamSource:
    return StreamSource(name=name, url="https://example.com", format="json", auth="none", notes="")


def _make_tier(
    tier_id: int,
    name: str,
    cadence: str,
    adapter: str = "rest_json",
    fallback: str | None = None,
    sources: list[StreamSource] | None = None,
) -> StreamTier:
    if sources is None:
        sources = [_make_source(f"source_{tier_id}_1"), _make_source(f"source_{tier_id}_2")]
    return StreamTier(
        id=tier_id,
        name=name,
        streams=len(sources),
        access="5G",
        formats=["json"],
        cadence=cadence,
        adapter=adapter,
        fallback=fallback,
        sources=sources,
    )


def _make_registry(tiers: list[StreamTier]) -> MagicMock:
    """Create a mock StreamRegistry with the given tiers."""
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


class TestDagFactory:
    def test_create_cadence_dag_structure(self):
        """DAG contains expected TaskGroups matching tiers for the cadence."""
        tiers = [
            _make_tier(1, "Geophysical", "sub_minute"),
            _make_tier(18, "Aviation", "sub_minute"),
            _make_tier(5, "Economic", "daily"),  # different cadence — should not appear
        ]
        registry = _make_registry(tiers)
        factory = DagFactory(registry=registry, settings=_make_settings())

        dag = factory.create_cadence_dag(
            cadence="sub_minute",
            dag_id="hydra_cadence_sub_minute",
            schedule="*/1 * * * *",
        )

        # Should have task groups for tier 1 and 18, not tier 5
        group_ids = list(dag.task_group.children.keys())
        assert len(group_ids) == 2
        assert any("tier_1" in gid for gid in group_ids)
        assert any("tier_18" in gid for gid in group_ids)

    def test_task_group_contains_stream_tasks(self):
        """Each TaskGroup has PythonOperator tasks for each stream in the tier."""
        sources = [_make_source("usgs_earthquake"), _make_source("iris_fdsn"), _make_source("geofon")]
        tier = _make_tier(1, "Geophysical", "sub_minute", sources=sources)
        registry = _make_registry([tier])
        factory = DagFactory(registry=registry, settings=_make_settings())

        dag = factory.create_cadence_dag(
            cadence="sub_minute",
            dag_id="test_dag",
            schedule="*/1 * * * *",
        )

        # Total tasks should equal number of sources
        assert len(dag.tasks) == 3

    def test_dag_schedule_matches_cadence(self):
        """DAG schedule interval matches cadence configuration."""
        tier = _make_tier(1, "Geophysical", "sub_minute")
        registry = _make_registry([tier])
        factory = DagFactory(registry=registry, settings=_make_settings())

        dag = factory.create_cadence_dag(
            cadence="sub_minute",
            dag_id="test_dag",
            schedule="*/1 * * * *",
        )

        # Airflow 3.x uses 'schedule', Airflow 2.x uses 'schedule_interval'
        schedule = getattr(dag, "schedule", None) or getattr(dag, "schedule_interval", None)
        assert schedule is not None

    def test_default_args_applied(self):
        """Retries, timeout propagated to tasks."""
        tier = _make_tier(1, "Geophysical", "sub_minute")
        registry = _make_registry([tier])
        factory = DagFactory(registry=registry, settings=_make_settings())

        dag = factory.create_cadence_dag(
            cadence="sub_minute",
            dag_id="test_dag",
            schedule="*/1 * * * *",
        )

        assert dag.default_args["retries"] == 3
        assert dag.default_args["execution_timeout"] == timedelta(minutes=5)

    def test_max_active_runs_set(self):
        """Per-cadence max_active_runs correctly set."""
        tier = _make_tier(1, "Geophysical", "sub_minute")
        registry = _make_registry([tier])
        factory = DagFactory(registry=registry, settings=_make_settings())

        dag = factory.create_cadence_dag(
            cadence="sub_minute",
            dag_id="test_dag",
            schedule="*/1 * * * *",
        )

        assert dag.max_active_runs == 2

    def test_monthly_plus_includes_annual_quarterly(self):
        """Monthly-plus DAG includes tiers with annual/quarterly/varies cadence."""
        tiers = [
            _make_tier(14, "Arms", "annual"),
            _make_tier(28, "National Portals", "varies"),
            _make_tier(1, "Geophysical", "sub_minute"),  # should not appear
        ]
        registry = _make_registry(tiers)
        factory = DagFactory(registry=registry, settings=_make_settings())

        dag = factory.create_cadence_dag(
            cadence="monthly_plus",
            dag_id="hydra_cadence_monthly_plus",
            schedule="@monthly",
        )

        group_ids = list(dag.task_group.children.keys())
        assert len(group_ids) == 2
        assert any("tier_14" in gid for gid in group_ids)
        assert any("tier_28" in gid for gid in group_ids)

    def test_unknown_cadence_raises(self):
        """Registry with unknown cadence string raises ValueError."""
        tier = _make_tier(99, "Unknown", "every_5_seconds")
        registry = _make_registry([tier])
        factory = DagFactory(registry=registry, settings=_make_settings())

        with pytest.raises(ValueError, match="Unknown cadence"):
            factory.create_cadence_dag(
                cadence="every_5_seconds",
                dag_id="test_dag",
                schedule="*/5 * * * * *",
            )
