"""DAG definition for entity-network correlation pipeline.

Schedule: @daily — full-window entity resolution across all source tiers.
"""

from __future__ import annotations

import asyncio
from dataclasses import asdict
from datetime import timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "hydra",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

_engine_instance: Any = None


def _get_correlation_engine() -> Any:
    """Singleton correlation engine per worker process."""
    global _engine_instance
    if _engine_instance is not None:
        return _engine_instance

    from hydra.config import settings
    from hydra.correlation.engine import CorrelationEngine
    from hydra.correlation.pipelines.entity_network import EntityNetworkPipeline
    from hydra.registry.stream_registry import get_registry
    from hydra.storage.engines.elasticsearch import ElasticsearchEngine
    from hydra.storage.engines.neo4j import Neo4jEngine
    from hydra.storage.engines.postgres import PostgresEngine

    registry = get_registry()
    pg = PostgresEngine(settings)
    neo4j = Neo4jEngine(settings)
    es = ElasticsearchEngine(settings)

    engine = CorrelationEngine(
        pg_engine=pg,
        neo4j_engine=neo4j,
        es_engine=es,
        registry=registry,
        settings=settings,
    )
    pipeline = EntityNetworkPipeline(settings, es_engine=es)
    engine.register_pipeline(pipeline)
    _engine_instance = engine
    return engine


def run_entity_network_full(**context: Any) -> None:
    """Execute full-window entity-network correlation."""
    engine = _get_correlation_engine()
    result = asyncio.run(engine.run(pipeline_id="entity_network"))
    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="correlation_entity_network", value=asdict(result))


dag = DAG(
    dag_id="correlation_entity_network",
    schedule="@daily",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    catchup=False,
    tags=["hydra", "correlation", "entity_network"],
)

PythonOperator(
    task_id="run_entity_network_full",
    python_callable=run_entity_network_full,
    dag=dag,
)
