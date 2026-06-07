"""DAG definition for biweekly cadence tiers (mil_int 101-103)."""

from hydra.config import settings
from hydra.registry.stream_registry import get_registry
from hydra.scheduler.dag_factory import CADENCE_CONFIG, DagFactory

registry = get_registry()
factory = DagFactory(registry=registry, settings=settings)

dag = factory.create_cadence_dag(
    cadence="biweekly",
    dag_id="hydra_cadence_biweekly",
    schedule=CADENCE_CONFIG["biweekly"]["schedule"],
)
