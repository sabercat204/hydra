"""DAG definition for quarterly cadence tiers (mil_int 106)."""

from hydra.config import settings
from hydra.registry.stream_registry import get_registry
from hydra.scheduler.dag_factory import CADENCE_CONFIG, DagFactory

registry = get_registry()
factory = DagFactory(registry=registry, settings=settings)

dag = factory.create_cadence_dag(
    cadence="quarterly",
    dag_id="hydra_cadence_quarterly",
    schedule=CADENCE_CONFIG["quarterly"]["schedule"],
)
