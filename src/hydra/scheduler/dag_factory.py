"""DAG factory — generates Airflow DAGs from stream_registry.yaml cadence definitions."""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

if TYPE_CHECKING:
    from hydra.config import HydraSettings
    from hydra.registry.stream_registry import StreamRegistry, StreamTier

logger = logging.getLogger(__name__)

DEFAULT_DAG_ARGS: dict[str, Any] = {
    "owner": "hydra",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=15),
    "sla": None,
}

# Per-cadence configuration
CADENCE_CONFIG: dict[str, dict[str, Any]] = {
    "sub_minute": {
        "schedule": "*/1 * * * *",
        "sla": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=5),
        "max_active_runs": 2,
    },
    "realtime": {
        "schedule": "*/1 * * * *",
        "sla": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=5),
        "max_active_runs": 2,
    },
    "15min": {
        "schedule": "*/15 * * * *",
        "sla": timedelta(minutes=20),
        "execution_timeout": timedelta(minutes=10),
        "max_active_runs": 1,
    },
    "hourly": {
        "schedule": "@hourly",
        "sla": timedelta(minutes=90),
        "execution_timeout": timedelta(minutes=30),
        "max_active_runs": 1,
    },
    "daily": {
        "schedule": "@daily",
        "sla": timedelta(hours=6),
        "execution_timeout": timedelta(hours=2),
        "max_active_runs": 1,
    },
    "weekly": {
        "schedule": "@weekly",
        "sla": timedelta(hours=24),
        "execution_timeout": timedelta(hours=4),
        "max_active_runs": 1,
    },
    "biweekly": {
        # Airflow uses standard cron — every 14 days, anchored on day 1 & 15
        "schedule": "0 6 1,15 * *",
        "sla": timedelta(hours=48),
        "execution_timeout": timedelta(hours=4),
        "max_active_runs": 1,
    },
    "quarterly": {
        # First day of Jan / Apr / Jul / Oct at 06:00 UTC
        "schedule": "0 6 1 1,4,7,10 *",
        "sla": timedelta(days=2),
        "execution_timeout": timedelta(hours=8),
        "max_active_runs": 1,
    },
    "on_change": {
        # No scheduled trigger — DAG runs on manual / API trigger only.
        "schedule": None,
        "sla": None,
        "execution_timeout": timedelta(hours=2),
        "max_active_runs": 1,
    },
    "monthly_plus": {
        "schedule": "@monthly",
        "sla": timedelta(hours=72),
        "execution_timeout": timedelta(hours=8),
        "max_active_runs": 1,
    },
}

# Cadences that map into the monthly_plus DAG
_MONTHLY_PLUS_CADENCES = {"monthly", "quarterly", "annual", "varies"}

# Staleness windows in days for monthly_plus tiers
_DEFAULT_STALENESS_WINDOWS: dict[str, int] = {
    "annual": 335,
    "quarterly": 85,
    "monthly": 28,
    "varies": 28,
}


def sla_miss_callback(dag: Any, task_list: Any, blocking_task_list: Any, slas: Any, blocking_tis: Any) -> None:
    """Called when any task in the DAG misses its SLA.

    Actions:
    1. Log at ERROR: DAG id, missed tasks, SLA value, actual duration.
    2. Store SLA miss event in Redis: hydra:sla_miss:{dag_id}:{execution_date}.
    3. P12 wires this to alerting channels.
    """
    dag_id = dag.dag_id if dag else "unknown"
    task_ids = [str(t) for t in (task_list or [])]
    logger.error(
        "sla_miss",
        extra={
            "dag_id": dag_id,
            "missed_tasks": task_ids,
            "blocking_tasks": [str(t) for t in (blocking_task_list or [])],
        },
    )


def run_stream_task(stream_id: str, **context: Any) -> None:
    """Airflow PythonOperator callable."""
    from airflow.exceptions import AirflowException, AirflowSkipException

    runner = _get_task_runner()
    result = asyncio.run(runner.execute(stream_id, **context))

    if result.status == "skipped":
        raise AirflowSkipException(f"Backpressure skip: {result.error}")
    elif result.status == "failed":
        raise AirflowException(f"Stream failed: {result.error}")

    # Push metrics to XCom for downstream visibility
    from dataclasses import asdict
    ti = context.get("ti")
    if ti is not None:
        # Convert TaskResult to a serializable dict (strip non-serializable route_result)
        result_dict = {
            "stream_id": result.stream_id,
            "adapter_type": result.adapter_type,
            "status": result.status,
            "records_fetched": result.records_fetched,
            "records_routed": result.records_routed,
            "records_deduplicated": result.records_deduplicated,
            "records_failed": result.records_failed,
            "duration_ms": result.duration_ms,
            "error": result.error,
            "fallback_used": result.fallback_used,
            "backpressure_delayed": result.backpressure_delayed,
            "timestamp": result.timestamp,
        }
        ti.xcom_push(key="task_result", value=result_dict)


# Singleton task runner per worker process
_task_runner_instance: Any = None


def _get_task_runner() -> Any:
    """Initialize shared resources once per Airflow worker process and cache them."""
    global _task_runner_instance
    if _task_runner_instance is not None:
        return _task_runner_instance

    from hydra.config import settings
    from hydra.registry.stream_registry import get_registry
    from hydra.auth.manager import AuthManager
    from hydra.auth.credential_store import CredentialStore
    from hydra.storage.redis_cache import RedisCache
    from hydra.storage.router import StorageRouter
    from hydra.scheduler.backpressure import BackpressureMonitor
    from hydra.scheduler.concurrency import ConcurrencyManager
    from hydra.scheduler.task_runner import TaskRunner

    registry = get_registry()
    credential_store = CredentialStore(settings.credential_store_path)
    auth_manager = AuthManager(
        stream_id="__global__",
        stream_config={"auth_pattern": "none"},
        credential_store=credential_store,
    )
    redis_cache = RedisCache(settings.database.redis_url, settings.database.redis_pool_max)
    storage_router = StorageRouter(redis_cache, registry, settings)
    bp_monitor = BackpressureMonitor(redis_cache, settings)
    concurrency_mgr = ConcurrencyManager(settings)

    _task_runner_instance = TaskRunner(
        registry=registry,
        auth_manager=auth_manager,
        storage_router=storage_router,
        backpressure_monitor=bp_monitor,
        concurrency_manager=concurrency_mgr,
        settings=settings,
        redis_cache=redis_cache,
    )
    return _task_runner_instance


class DagFactory:
    """Generates Airflow DAGs from stream_registry.yaml cadence definitions."""

    def __init__(self, registry: "StreamRegistry", settings: "HydraSettings") -> None:
        self._registry = registry
        self._settings = settings

    def create_cadence_dag(
        self,
        cadence: str,
        dag_id: str,
        schedule: str,
        default_args: dict[str, Any] | None = None,
    ) -> DAG:
        """Create a single cadence DAG with per-tier task groups.

        Each tier matching the cadence gets a TaskGroup.
        Each stream within the tier gets a PythonOperator task.
        Tasks within a TaskGroup run in parallel (up to concurrency limit).
        TaskGroups within a DAG run in parallel.
        """
        cadence_cfg = CADENCE_CONFIG.get(cadence)
        if cadence_cfg is None and cadence not in _MONTHLY_PLUS_CADENCES:
            raise ValueError(f"Unknown cadence: {cadence}")

        merged_args = dict(DEFAULT_DAG_ARGS)
        if cadence_cfg:
            if cadence_cfg.get("execution_timeout"):
                merged_args["execution_timeout"] = cadence_cfg["execution_timeout"]
        if default_args:
            merged_args.update(default_args)

        max_active_runs = cadence_cfg["max_active_runs"] if cadence_cfg else 1

        dag = DAG(
            dag_id=dag_id,
            schedule=schedule,
            default_args=merged_args,
            max_active_runs=max_active_runs,
            catchup=False,
            tags=["hydra", cadence],
        )

        # Find tiers for this cadence
        if cadence == "monthly_plus":
            tiers = self._get_monthly_plus_tiers()
        else:
            tiers = self._registry.get_tiers_by_cadence(cadence)

        for tier in tiers:
            self._create_tier_task_group(tier, dag)

        return dag

    def _get_monthly_plus_tiers(self) -> list["StreamTier"]:
        """Get all tiers with monthly/quarterly/annual/varies cadence."""
        result = []
        for tier in self._registry.tiers.values():
            if tier.cadence in _MONTHLY_PLUS_CADENCES:
                result.append(tier)
        return result

    def _create_tier_task_group(self, tier: "StreamTier", dag: DAG) -> TaskGroup:
        """Create a TaskGroup for a single tier.

        Each stream in the tier becomes a PythonOperator calling
        TaskRunner.execute(stream_id).
        """
        group_id = f"tier_{tier.id}_{tier.name.lower().replace(' ', '_').replace('&', '').replace('/', '_')}"
        # Sanitize group_id for Airflow (alphanumeric, underscore, hyphen, dot)
        group_id = "".join(c if c.isalnum() or c in ("_", "-", ".") else "_" for c in group_id)

        with TaskGroup(group_id=group_id, dag=dag) as tg:
            for source in tier.sources:
                task_id = source.name.lower().replace(" ", "_").replace("/", "_").replace(".", "_")
                task_id = "".join(c if c.isalnum() or c in ("_", "-", ".") else "_" for c in task_id)

                PythonOperator(
                    task_id=task_id,
                    python_callable=run_stream_task,
                    op_kwargs={"stream_id": task_id},
                    pool="hydra_global",
                    dag=dag,
                )

        return tg
