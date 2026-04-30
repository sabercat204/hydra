"""DAG definition for 15min cadence tiers."""

from hydra.config import settings
from hydra.registry.stream_registry import get_registry
from hydra.scheduler.dag_factory import CADENCE_CONFIG, DagFactory

registry = get_registry()
factory = DagFactory(registry=registry, settings=settings)

dag = factory.create_cadence_dag(
    cadence="15min",
    dag_id="hydra_cadence_15min",
    schedule=CADENCE_CONFIG["15min"]["schedule"],
)
