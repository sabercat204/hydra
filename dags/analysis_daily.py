"""DAG: analysis_daily — daily intelligence product generation.

Generates:
- 24h Situation Report
- Global Threat Assessment (24h)
- Watchlist Threat Assessments (7d per region)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    "owner": "hydra",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


async def _generate_product(product_type: str, **kwargs: object) -> None:
    """Generate an intelligence product via AnalysisEngine."""
    from hydra.analysis.engine import AnalysisEngine
    from hydra.analysis.graph import GraphAnalyzer
    from hydra.analysis.models import ProductParams
    from hydra.analysis.products.entity_dossier import EntityDossier
    from hydra.analysis.products.situation_report import SituationReport
    from hydra.analysis.products.threat_assessment import ThreatAssessment
    from hydra.analysis.queries import QueryLayer
    from hydra.analysis.timeline import TimelineBuilder
    from hydra.config import settings
    from hydra.storage.engines.neo4j import Neo4jEngine
    from hydra.storage.engines.postgres import PostgresEngine

    pg = PostgresEngine(settings)
    neo4j = Neo4jEngine(settings)
    await pg.connect()
    await neo4j.connect()

    try:
        query_layer = QueryLayer(pg, None, None, settings)
        graph_analyzer = GraphAnalyzer(neo4j, settings)
        timeline_builder = TimelineBuilder(settings)

        engine = AnalysisEngine(query_layer, graph_analyzer, timeline_builder, pg, settings)
        engine.register_product(SituationReport(settings))
        engine.register_product(ThreatAssessment(settings))
        engine.register_product(EntityDossier(settings))

        params_dict = kwargs.get("params", {})
        params = ProductParams(**params_dict) if isinstance(params_dict, dict) else ProductParams()
        await engine.generate(product_type, params)
    finally:
        await pg.disconnect()
        await neo4j.disconnect()


def _run_async(product_type: str, **kwargs: object) -> None:
    import asyncio

    params = kwargs.get("params", {})
    asyncio.run(_generate_product(product_type, params=params))


with DAG(
    dag_id="analysis_daily",
    default_args=default_args,
    description="Daily intelligence product generation",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["analysis", "daily"],
) as dag:
    wait_for_daily_ingestion = ExternalTaskSensor(
        task_id="wait_for_daily_ingestion",
        external_dag_id="hydra_cadence_daily",
        external_task_id=None,
        timeout=7200,
        mode="reschedule",
    )

    generate_sitrep_24h = PythonOperator(
        task_id="generate_sitrep_24h",
        python_callable=_run_async,
        op_kwargs={"product_type": "situation_report"},
    )

    generate_threat_global = PythonOperator(
        task_id="generate_threat_assessment_global",
        python_callable=_run_async,
        op_kwargs={"product_type": "threat_assessment"},
    )

    generate_threat_watchlist = PythonOperator(
        task_id="generate_threat_assessment_watchlist",
        python_callable=_run_async,
        op_kwargs={
            "product_type": "threat_assessment",
            "params": {
                "time_window_start": (datetime.utcnow() - timedelta(days=7)).isoformat(),
            },
        },
    )

    wait_for_daily_ingestion >> [
        generate_sitrep_24h,
        generate_threat_global,
        generate_threat_watchlist,
    ]
