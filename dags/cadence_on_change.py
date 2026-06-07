"""DAG definition for on_change cadence tiers (mil_int 107).

Manual / API-triggered only — no schedule. Used for low-volatility reference
sources whose contents change rarely (NATO CUI registry, ITAR reference,
DTIC distribution-statement taxonomy).
"""

from hydra.config import settings
from hydra.registry.stream_registry import get_registry
from hydra.scheduler.dag_factory import DagFactory

registry = get_registry()
factory = DagFactory(registry=registry, settings=settings)

dag = factory.create_cadence_dag(
    cadence="on_change",
    dag_id="hydra_cadence_on_change",
    schedule=None,
)
