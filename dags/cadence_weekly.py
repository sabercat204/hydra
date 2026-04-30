"""DAG definition for weekly cadence tiers."""

from hydra.config import settings
from hydra.registry.stream_registry import get_registry
from hydra.scheduler.dag_factory import CADENCE_CONFIG, DagFactory

registry = get_registry()
factory = DagFactory(registry=registry, settings=settings)

dag = factory.create_cadence_dag(
    cadence="weekly",
    dag_id="hydra_cadence_weekly",
    schedule=CADENCE_CONFIG["weekly"]["schedule"],
)
