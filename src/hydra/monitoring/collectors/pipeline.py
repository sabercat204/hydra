"""PipelineCollector — scrapes intelligence products and correlation metrics (P12 §6.4).

Concrete :class:`BaseCollector` that queries PostgreSQL for rows in
``intelligence_products``, ``correlation_results``, and
``normalized_records`` and publishes the delta through counters,
histograms, and gauges in the Prometheus custom metrics registry.

To avoid double-counting across collection cycles, the collector
maintains ``last_collection_ts`` and queries:

.. code-block:: sql

    WHERE created_at > $1 AND created_at <= $2

where ``$1`` is ``last_collection_ts`` and ``$2`` is ``NOW()`` captured
at the start of the cycle. The collector advances
``last_collection_ts`` only after all queries succeed, so a partial
failure is safe to retry on the next cycle without gaps.

Satisfies Requirements 8.1–8.4.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Final

from hydra.monitoring.collectors import BaseCollector
from hydra.monitoring.metrics import (
    hydra_correlation_total,
    hydra_product_completeness_score,
    hydra_product_confidence_score,
    hydra_product_generated_total,
    hydra_storage_records_total,
)

if TYPE_CHECKING:
    import asyncpg

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# SQL queries
# -----------------------------------------------------------------------------

# New intelligence products in the window, grouped by type + classification
# so we can increment the counter with the correct label combo. Also
# returns the confidence and completeness scores to feed the histograms.
_PRODUCTS_SELECT_SQL: Final[str] = (
    "SELECT product_type, classification, confidence_score, completeness_score "
    "FROM intelligence_products "
    "WHERE generated_at > $1 AND generated_at <= $2"
)

# New correlations in the window, grouped by pipeline for the counter.
_CORRELATIONS_COUNT_SQL: Final[str] = (
    "SELECT pipeline_id, COUNT(*)::bigint AS cnt "
    "FROM correlation_results "
    "WHERE created_at > $1 AND created_at <= $2 "
    "GROUP BY pipeline_id"
)

# Full snapshot of record counts by tier/status — this metric is a Gauge,
# so we set (not increment) the current totals every cycle.
_RECORDS_BY_TIER_STATUS_SQL: Final[str] = (
    "SELECT tier::text AS tier, storage_status, COUNT(*)::bigint AS cnt "
    "FROM normalized_records "
    "GROUP BY tier, storage_status"
)


class PipelineCollector(BaseCollector):
    """Collect intelligence-product and correlation metrics.

    Each ``collect()`` cycle:

    1. Captures ``now`` and queries ``intelligence_products`` for rows
       with ``generated_at`` in ``(last_collection_ts, now]``. For each
       row, increments :data:`hydra_product_generated_total` by
       ``(product_type, classification)`` and observes the confidence +
       completeness scores into :data:`hydra_product_confidence_score`
       and :data:`hydra_product_completeness_score`.
    2. Queries ``correlation_results`` for counts-per-pipeline in the
       same window and increments :data:`hydra_correlation_total` by
       ``pipeline_id``.
    3. Queries the full ``normalized_records`` table grouped by ``tier``
       and ``storage_status`` and publishes
       :data:`hydra_storage_records_total` as a snapshot gauge.
    4. Advances ``last_collection_ts`` to ``now`` for the next cycle.
    """

    def __init__(
        self,
        pg_pool: "asyncpg.Pool",
        interval: float = 300.0,
    ) -> None:
        super().__init__(interval=interval)
        self._pg_pool = pg_pool
        # Initialize to the collector's construction time so the very
        # first cycle doesn't flood counters with the full historical
        # backlog (Requirement 8.1, 8.3).
        self.last_collection_ts: datetime = datetime.now(timezone.utc)

    async def collect(self) -> None:
        # Capture the upper bound up front so all three queries see a
        # consistent "now" and the advance of ``last_collection_ts`` is
        # exact.
        now = datetime.now(timezone.utc)
        start = self.last_collection_ts

        async with self._pg_pool.acquire() as conn:
            await self._update_product_metrics(conn, start, now)
            await self._update_correlation_metrics(conn, start, now)
            await self._update_record_metrics(conn)

        # Only advance the watermark after all queries succeeded — if
        # any raised, the BaseCollector loop will log and retry next
        # cycle with the same ``start`` timestamp (Requirement 22.1).
        self.last_collection_ts = now

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _update_product_metrics(
        self,
        conn: "asyncpg.Connection",
        start: datetime,
        end: datetime,
    ) -> None:
        """Increment product counter and observe confidence/completeness histograms."""
        rows = await conn.fetch(_PRODUCTS_SELECT_SQL, start, end)
        for row in rows:
            product_type = row["product_type"]
            classification = row["classification"]
            confidence = float(row["confidence_score"])
            completeness = float(row["completeness_score"])

            hydra_product_generated_total.labels(
                product_type=product_type,
                classification=classification,
            ).inc()
            hydra_product_confidence_score.labels(
                product_type=product_type
            ).observe(confidence)
            hydra_product_completeness_score.labels(
                product_type=product_type
            ).observe(completeness)

    async def _update_correlation_metrics(
        self,
        conn: "asyncpg.Connection",
        start: datetime,
        end: datetime,
    ) -> None:
        """Increment hydra_correlation_total by pipeline for new correlations."""
        rows = await conn.fetch(_CORRELATIONS_COUNT_SQL, start, end)
        for row in rows:
            pipeline_id = row["pipeline_id"]
            count = int(row["cnt"])
            if count <= 0:
                continue
            hydra_correlation_total.labels(pipeline_id=pipeline_id).inc(count)

    async def _update_record_metrics(self, conn: "asyncpg.Connection") -> None:
        """Set hydra_storage_records_total gauge from the current tier/status rollup."""
        rows = await conn.fetch(_RECORDS_BY_TIER_STATUS_SQL)
        for row in rows:
            tier = row["tier"]
            storage_status = row["storage_status"]
            count = int(row["cnt"])
            hydra_storage_records_total.labels(
                tier=tier,
                storage_status=storage_status,
            ).set(count)


__all__ = ["PipelineCollector"]
