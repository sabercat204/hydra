"""HYDRA scheduler and orchestration."""

from hydra.scheduler.backpressure import BackpressureMonitor, BackpressureState, EngineBackpressure
from hydra.scheduler.concurrency import ConcurrencyManager
from hydra.scheduler.exceptions import (
    AdapterResolutionError,
    BackpressureBlocked,
    ConcurrencyTimeout,
    DeadStreamError,
    SchedulerError,
)
from hydra.scheduler.health import SchedulerHealth, SchedulerHealthAggregator
from hydra.scheduler.task_runner import TaskResult, TaskRunner

# DagFactory requires apache-airflow — import lazily to avoid hard dependency
# in non-Airflow contexts (tests, API server, etc.)


def __getattr__(name: str):
    if name == "DagFactory":
        from hydra.scheduler.dag_factory import DagFactory
        return DagFactory
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "AdapterResolutionError",
    "BackpressureBlocked",
    "BackpressureMonitor",
    "BackpressureState",
    "ConcurrencyManager",
    "ConcurrencyTimeout",
    "DagFactory",
    "DeadStreamError",
    "EngineBackpressure",
    "SchedulerError",
    "SchedulerHealth",
    "SchedulerHealthAggregator",
    "TaskResult",
    "TaskRunner",
]
