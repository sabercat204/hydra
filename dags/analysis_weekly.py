"""DAG: analysis_weekly — weekly intelligence product generation.

Generates:
- 7-day Situation Report
- Weekly Threat Assessment
- Refreshed Entity Dossiers for watchlist entities
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "hydra",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


async def _generate_product(product_type: str, params_dict: dict | None = None) -> None:
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

        params = ProductParams(**(params_dict or {}))
        await engine.generate(product_type, params)
    finally:
        await pg.disconnect()
        await neo4j.disconnect()


async def _refresh_entity_dossiers() -> None:
    """Refresh dossiers for all entities in the watchlist."""
    from hydra.config import settings

    watchlist = settings.analysis.entity_watchlist
    for entity in watchlist:
        entity_id = entity.get("entity_id")
        entity_name = entity.get("name")
        await _generate_product(
            "entity_dossier",
            {
                "entity_id": entity_id,
                "entity_name": entity_name,
            },
        )


def _run_async_product(product_type: str, params_dict: dict | None = None) -> None:
    import asyncio

    asyncio.run(_generate_product(product_type, params_dict))


def _run_async_dossiers() -> None:
    import asyncio

    asyncio.run(_refresh_entity_dossiers())


with DAG(
    dag_id="analysis_weekly",
    default_args=default_args,
    description="Weekly intelligence product generation",
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["analysis", "weekly"],
) as dag:
    generate_sitrep_7d = PythonOperator(
        task_id="generate_sitrep_7d",
        python_callable=_run_async_product,
        op_kwargs={
            "product_type": "situation_report",
            "params_dict": {
                "time_window_start": (datetime.utcnow() - timedelta(days=7)).isoformat(),
            },
        },
    )

    generate_threat_weekly = PythonOperator(
        task_id="generate_threat_assessment_weekly",
        python_callable=_run_async_product,
        op_kwargs={
            "product_type": "threat_assessment",
            "params_dict": {
                "time_window_start": (datetime.utcnow() - timedelta(days=7)).isoformat(),
            },
        },
    )

    refresh_dossiers = PythonOperator(
        task_id="refresh_entity_dossiers",
        python_callable=_run_async_dossiers,
    )

    [generate_sitrep_7d, generate_threat_weekly, refresh_dossiers]
