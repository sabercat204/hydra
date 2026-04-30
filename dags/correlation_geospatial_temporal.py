"""DAG definition for geospatial-temporal correlation pipeline.

Schedule: @hourly — full-window correlation across all source tiers.
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
    "execution_timeout": timedelta(minutes=30),
}

_engine_instance: Any = None


def _get_correlation_engine() -> Any:
    """Singleton correlation engine per worker process."""
    global _engine_instance
    if _engine_instance is not None:
        return _engine_instance

    from hydra.config import settings
    from hydra.correlation.engine import CorrelationEngine
    from hydra.correlation.pipelines.geospatial_temporal import GeospatialTemporalPipeline
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
    pipeline = GeospatialTemporalPipeline(settings)
    engine.register_pipeline(pipeline)
    _engine_instance = engine
    return engine


def run_geospatial_temporal_full(**context: Any) -> None:
    """Execute full-window geospatial-temporal correlation."""
    engine = _get_correlation_engine()
    result = asyncio.run(engine.run(pipeline_id="geospatial_temporal"))
    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="correlation_geospatial_temporal", value=asdict(result))


dag = DAG(
    dag_id="correlation_geospatial_temporal",
    schedule="@hourly",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    catchup=False,
    tags=["hydra", "correlation", "geospatial_temporal"],
)

PythonOperator(
    task_id="run_geospatial_temporal_full",
    python_callable=run_geospatial_temporal_full,
    dag=dag,
)
