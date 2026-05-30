"""DAG: eas_observatory_daily — Exposure Observatory daily run (R19.1–R19.3).

Generates the daily ``exposure_posture_report`` intelligence product
by invoking :class:`hydra.eas.observatory.generator.ExposureObservatory`.

The implementation follows the ``dags/analysis_daily.py`` shape so the
DAG integrates cleanly with the existing P8 scheduler:

* A single ``generate_posture_report`` :class:`PythonOperator` task
  running :func:`_run_observatory` in an ``asyncio.run(...)`` wrapper
  (Airflow callables stay sync).
* ``@daily`` schedule, ``hydra-eas`` owner, and the ``hydra`` + ``eas``
  + ``daily`` tags so Grafana / the monitoring dashboards can filter
  the run log by capability.
* The task emits the ``posture_report_generated product_id=<uuid>
  countries=<n>`` log line on success and increments
  ``hydra_eas_observatory_runs_total{status="success"}``; failures
  increment the same counter with ``status="failed"`` (R19.3) and
  re-raise so Airflow marks the run as failed.

The Prometheus counter is looked up lazily via
``hydra.eas.metrics.get_observatory_runs_counter`` — that hook is wired
up by task 16.1. If it isn't present yet (early phase-13 wiring) the
counter bump is skipped silently and only the log line is emitted.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "hydra-eas",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


# ---------------------------------------------------------------------------
# Task implementation
# ---------------------------------------------------------------------------


async def _run_observatory() -> str:
    """End-to-end observatory generation for the current UTC day.

    Steps:

    1. Build the shared :class:`PostgresEngine`, :class:`MinIO` client,
       :class:`AnalysisEngine` singletons using the process-wide
       :data:`hydra.config.settings`.
    2. Instantiate :class:`ObservatoryRepository` + :class:`ExposureObservatory`.
    3. Invoke :meth:`ExposureObservatory.run` at ``as_of = now()``.
    4. Log the success line and bump the run counter.

    Returns the newly created ``product_id`` so Airflow's ``XCom``
    stream carries it for downstream operators (a future chained DAG
    could consume the id, e.g. to notify consumers).
    """

    from hydra.analysis.engine import AnalysisEngine
    from hydra.analysis.graph import GraphAnalyzer
    from hydra.analysis.queries import QueryLayer
    from hydra.analysis.timeline import TimelineBuilder
    from hydra.config import settings
    from hydra.eas.observatory.generator import ExposureObservatory
    from hydra.eas.observatory.repository import ObservatoryRepository
    from hydra.storage.engines.postgres import PostgresEngine

    pg = PostgresEngine(settings)
    await pg.connect()

    try:
        # We don't need graph / timeline / query layer for this product
        # but :class:`AnalysisEngine` requires them by contract. Passing
        # ``None`` for the optional dependencies keeps the ``_persist_product``
        # code path (the only one we actually call) working.
        query_layer = QueryLayer(pg, None, None, settings)
        graph_analyzer = GraphAnalyzer(None, settings)  # type: ignore[arg-type]
        timeline_builder = TimelineBuilder(settings)
        engine = AnalysisEngine(
            query_layer, graph_analyzer, timeline_builder, pg, settings
        )

        pool = getattr(pg, "_pool", None)
        repo = ObservatoryRepository(pool)

        # MinIO is optional (snapshot publication is a flag). Build a
        # best-effort client and let the generator decide whether to
        # call it.
        minio_client = _build_minio_client(settings)

        observatory = ExposureObservatory(
            settings.eas, repo, minio_client=minio_client
        )
        product = await observatory.run(
            as_of=datetime.now(timezone.utc),
            analysis_engine=engine,
        )

        countries = 0
        params = product.parameters or {}
        country_codes = params.get("country_codes")
        if isinstance(country_codes, list):
            countries = len(country_codes)

        logger.info(
            "posture_report_generated product_id=%s countries=%d",
            product.product_id,
            countries,
        )
        _bump_run_counter("success")
        return product.product_id
    finally:
        await pg.disconnect()


def _run_async() -> None:
    """Airflow PythonOperator entry point — sync wrapper around :func:`_run_observatory`.

    A run failure is recorded against the failure counter and the
    exception is re-raised so Airflow marks the task as failed.
    """

    try:
        asyncio.run(_run_observatory())
    except Exception as exc:
        logger.error(
            "posture_report_failed",
            extra={"error": str(exc)},
        )
        _bump_run_counter("failed")
        raise


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_minio_client(settings: Any) -> Any | None:
    """Construct a best-effort MinIO / S3 client for the snapshot write.

    We import :mod:`boto3` lazily so the DAG file is still parseable
    when the optional dependency isn't installed. A missing client is
    not an error — the generator falls back to "log + continue" when
    ``publish_snapshot_minio`` is true but no client is wired.
    """

    try:
        import boto3  # type: ignore[import-untyped]
    except ImportError:
        logger.debug("eas.observatory.dag.boto3_missing")
        return None

    db = getattr(settings, "database", None)
    minio_url = getattr(db, "minio_url", None) if db is not None else None
    if not isinstance(minio_url, str) or not minio_url:
        return None

    try:
        return boto3.client("s3", endpoint_url=minio_url)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "eas.observatory.dag.minio_client_init_failed",
            extra={"error": str(exc)},
        )
        return None


def _bump_run_counter(status: str) -> None:
    """Increment ``hydra_eas_observatory_runs_total{status=...}`` if wired.

    Task 16.1 registers the counter on :mod:`hydra.eas.metrics`; until
    then the hook returns ``None`` and we just return. This keeps the
    DAG functional when invoked against a workspace where the metrics
    module hasn't been populated yet.
    """

    try:
        from hydra.eas import metrics  # type: ignore[attr-defined]
    except ImportError:
        return

    counter_factory = getattr(metrics, "get_observatory_runs_counter", None)
    if counter_factory is None:
        return

    try:
        counter = counter_factory()
    except Exception as exc:  # noqa: BLE001
        logger.debug(
            "eas.observatory.dag.counter_lookup_failed",
            extra={"error": str(exc)},
        )
        return

    if counter is None:
        return

    try:
        counter.labels(status=status).inc()
    except Exception as exc:  # noqa: BLE001
        logger.debug(
            "eas.observatory.dag.counter_inc_failed",
            extra={"error": str(exc)},
        )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------


with DAG(
    dag_id="eas_observatory_daily",
    default_args=DEFAULT_ARGS,
    description="Daily Exposure Observatory run — posture report per country.",
    schedule="@daily",
    start_date=datetime(2026, 4, 15),
    catchup=False,
    tags=["hydra", "eas", "daily"],
) as dag:
    generate_posture_report = PythonOperator(
        task_id="generate_posture_report",
        python_callable=_run_async,
    )
