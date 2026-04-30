"""DAG definition for sub_minute cadence tiers."""

from hydra.config import settings
from hydra.registry.stream_registry import get_registry
from hydra.scheduler.dag_factory import CADENCE_CONFIG, DagFactory

registry = get_registry()
factory = DagFactory(registry=registry, settings=settings)

dag = factory.create_cadence_dag(
    cadence="sub_minute",
    dag_id="hydra_cadence_sub_minute",
    schedule=CADENCE_CONFIG["sub_minute"]["schedule"],
)
