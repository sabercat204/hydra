"""DAG definition for threat-convergence correlation pipeline.

Schedule: every 6 hours — full-window multi-signal threat detection.
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
    from hydra.correlation.pipelines.threat_convergence import ThreatConvergencePipeline
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
    pipeline = ThreatConvergencePipeline(settings)
    engine.register_pipeline(pipeline)
    _engine_instance = engine
    return engine


def run_threat_convergence_full(**context: Any) -> None:
    """Execute full-window threat-convergence correlation."""
    engine = _get_correlation_engine()
    result = asyncio.run(engine.run(pipeline_id="threat_convergence"))
    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="correlation_threat_convergence", value=asdict(result))


dag = DAG(
    dag_id="correlation_threat_convergence",
    schedule="0 */6 * * *",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    catchup=False,
    tags=["hydra", "correlation", "threat_convergence"],
)

PythonOperator(
    task_id="run_threat_convergence_full",
    python_callable=run_threat_convergence_full,
    dag=dag,
)
