"""CorrelationEngine — orchestrates pipeline execution."""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Any

from hydra.config import HydraSettings
from hydra.correlation.exceptions import (
    CandidateQueryError,
    PersistenceError,
    PipelineNotFoundError,
)
from hydra.correlation.models import (
    CandidateSet,
    CorrelationResult,
    CorrelationRunResult,
    PersistResult,
)
from hydra.correlation.pipelines.base import BasePipeline
from hydra.models.normalized import NormalizedRecord, SourceMeta, Tier, GeoGeometry
from hydra.registry.stream_registry import StreamRegistry
from hydra.storage.engines.elasticsearch import ElasticsearchEngine
from hydra.storage.engines.neo4j import Neo4jEngine
from hydra.storage.engines.postgres import PostgresEngine

logger = logging.getLogger(__name__)


class CorrelationEngine:
    """Orchestrates correlation pipeline execution.

    Responsibilities:
    1. Query candidate records from PostgreSQL (and optionally Elasticsearch).
    2. Dispatch to the appropriate pipeline.
    3. Deduplicate results.
    4. Persist to PostgreSQL and Neo4j.
    """

    def __init__(
        self,
        pg_engine: PostgresEngine,
        neo4j_engine: Neo4jEngine,
        es_engine: ElasticsearchEngine | None,
        registry: StreamRegistry,
        settings: HydraSettings,
    ) -> None:
        self._pg = pg_engine
        self._neo4j = neo4j_engine
        self._es = es_engine
        self._registry = registry
        self._settings = settings
        self._pipelines: dict[str, BasePipeline] = {}

    def register_pipeline(self, pipeline: BasePipeline) -> None:
        """Register a correlation pipeline by its ID."""
        self._pipelines[pipeline.pipeline_id] = pipeline

    async def run(
        self,
        pipeline_id: str,
        time_window_start: str | None = None,
        time_window_end: str | None = None,
        trigger_tiers: list[int] | None = None,
    ) -> CorrelationRunResult:
        """Execute a correlation pipeline."""
        start = time.monotonic()

        pipeline = self._pipelines.get(pipeline_id)
        if pipeline is None:
            raise PipelineNotFoundError(pipeline_id)

        # Default time window
        now = datetime.now(timezone.utc)
        if time_window_end is None:
            time_window_end = now.isoformat()
        if time_window_start is None:
            lookback = self._settings.correlation.scheduled_lookback.get(
                pipeline_id, 7200.0
            )
            start_dt = now - timedelta(seconds=lookback)
            time_window_start = start_dt.isoformat()

        # Build candidate set
        candidates = await self._build_candidate_set(
            pipeline_id=pipeline_id,
            source_tiers=pipeline.source_tiers,
            time_window_start=time_window_start,
            time_window_end=time_window_end,
            trigger_tiers=trigger_tiers,
        )

        # Run pipeline
        raw_results = await pipeline.correlate(candidates)

        # Deduplicate
        new_results, updated_count, dedup_count = await self._deduplicate(raw_results)

        # Persist
        persist = await self._persist_results(new_results)

        duration_ms = (time.monotonic() - start) * 1000
        return CorrelationRunResult(
            pipeline_id=pipeline_id,
            candidates_queried=candidates.total_records,
            pairs_evaluated=len(raw_results) + dedup_count,
            correlations_found=len(raw_results),
            correlations_new=len(new_results),
            correlations_updated=updated_count,
            correlations_deduplicated=dedup_count,
            persisted_pg=persist.pg_stored,
            persisted_neo4j=persist.neo4j_stored,
            duration_ms=duration_ms,
            time_window_start=time_window_start,
            time_window_end=time_window_end,
            trigger_tiers=trigger_tiers,
        )

    async def _build_candidate_set(
        self,
        pipeline_id: str,
        source_tiers: list[int],
        time_window_start: str,
        time_window_end: str,
        trigger_tiers: list[int] | None,
    ) -> CandidateSet:
        """Query PostgreSQL for candidate records."""
        start = time.monotonic()
        records: dict[int, list[NormalizedRecord]] = {}
        total = 0

        try:
            pool = self._pg._pool
            if pool is None:
                raise CandidateQueryError(pipeline_id, "PostgreSQL not connected")

            async with pool.acquire() as conn:
                for tier_id in source_tiers:
                    # If trigger_tiers is set, only query fresh records from trigger tiers
                    # and all records from other tiers in the window
                    rows = await conn.fetch(
                        """
                        SELECT stream_id, tier, timestamp,
                               ST_AsGeoJSON(geo)::text as geo_json,
                               payload::text, raw_hash, ingested_at,
                               confidence, tags
                        FROM normalized_records
                        WHERE tier = $1
                          AND timestamp >= $2::timestamptz
                          AND timestamp <= $3::timestamptz
                        ORDER BY timestamp DESC
                        LIMIT 10000
                        """,
                        tier_id,
                        time_window_start,
                        time_window_end,
                    )
                    tier_records: list[NormalizedRecord] = []
                    for row in rows:
                        geo = None
                        if row["geo_json"]:
                            geo_data = json.loads(row["geo_json"])
                            geo = GeoGeometry(
                                type=geo_data.get("type", "Point"),
                                coordinates=geo_data.get("coordinates"),
                            )
                        payload = json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
                        rec = NormalizedRecord(
                            stream_id=row["stream_id"],
                            tier=Tier(row["tier"]),
                            timestamp=row["timestamp"],
                            geo=geo,
                            payload=payload,
                            source_meta=SourceMeta(
                                source_name="correlation_query",
                                adapter_type="correlation",
                            ),
                            raw_hash=row["raw_hash"],
                            ingested_at=row["ingested_at"],
                            confidence=row["confidence"],
                            tags=row["tags"] or [],
                        )
                        tier_records.append(rec)
                    if tier_records:
                        records[tier_id] = tier_records
                        total += len(tier_records)
        except CandidateQueryError:
            raise
        except Exception as exc:
            raise CandidateQueryError(pipeline_id, str(exc)) from exc

        duration_ms = (time.monotonic() - start) * 1000
        return CandidateSet(
            pipeline_id=pipeline_id,
            source_tiers=source_tiers,
            time_window_start=time_window_start,
            time_window_end=time_window_end,
            records=records,
            total_records=total,
            query_duration_ms=duration_ms,
        )

    async def _persist_results(
        self, results: list[CorrelationResult]
    ) -> PersistResult:
        """Write correlation results to PostgreSQL and Neo4j."""
        persist = PersistResult()
        if not results:
            return persist

        # PostgreSQL persistence
        try:
            pool = self._pg._pool
            if pool:
                async with pool.acquire() as conn:
                    for cr in results:
                        try:
                            await conn.execute(
                                """
                                INSERT INTO correlation_results
                                    (correlation_id, pipeline_id, record_a_hash,
                                     record_b_hash, tier_a, tier_b, confidence,
                                     match_dimensions, evidence, correlation_hash,
                                     tags, created_at, updated_at)
                                VALUES
                                    ($1::uuid, $2, $3, $4, $5, $6, $7,
                                     $8::jsonb, $9::jsonb, $10, $11, $12::timestamptz,
                                     $12::timestamptz)
                                ON CONFLICT (correlation_hash) DO UPDATE SET
                                    confidence = EXCLUDED.confidence,
                                    match_dimensions = EXCLUDED.match_dimensions,
                                    evidence = EXCLUDED.evidence,
                                    updated_at = NOW()
                                """,
                                cr.correlation_id,
                                cr.pipeline_id,
                                cr.record_a_hash,
                                cr.record_b_hash,
                                cr.tier_a,
                                cr.tier_b,
                                cr.confidence,
                                json.dumps(cr.match_dimensions),
                                json.dumps(cr.evidence),
                                cr.correlation_hash,
                                cr.tags,
                                cr.created_at,
                            )
                            persist.pg_stored += 1
                        except Exception as exc:
                            persist.pg_errors.append({
                                "correlation_hash": cr.correlation_hash,
                                "error": str(exc),
                            })
        except Exception as exc:
            logger.error("correlation_pg_persist_error", extra={"error": str(exc)})

        # Neo4j persistence
        try:
            driver = self._neo4j._driver
            if driver:
                async with driver.session() as session:
                    for cr in results:
                        try:
                            await session.run(
                                """
                                MERGE (a:Record {raw_hash: $record_a_hash})
                                ON CREATE SET a.tier = $tier_a,
                                             a.created_at = $now
                                MERGE (b:Record {raw_hash: $record_b_hash})
                                ON CREATE SET b.tier = $tier_b,
                                             b.created_at = $now
                                MERGE (a)-[r:CORRELATED_WITH {
                                    correlation_id: $correlation_id
                                }]->(b)
                                SET r.pipeline_id = $pipeline_id,
                                    r.confidence = $confidence,
                                    r.match_dimensions = $match_dimensions,
                                    r.created_at = $created_at
                                """,
                                record_a_hash=cr.record_a_hash,
                                record_b_hash=cr.record_b_hash,
                                tier_a=cr.tier_a,
                                tier_b=cr.tier_b,
                                correlation_id=cr.correlation_id,
                                pipeline_id=cr.pipeline_id,
                                confidence=cr.confidence,
                                match_dimensions=json.dumps(cr.match_dimensions),
                                created_at=cr.created_at,
                                now=datetime.now(timezone.utc).isoformat(),
                            )
                            persist.neo4j_stored += 1
                        except Exception as exc:
                            persist.neo4j_errors.append({
                                "correlation_hash": cr.correlation_hash,
                                "error": str(exc),
                            })
        except Exception as exc:
            logger.error("correlation_neo4j_persist_error", extra={"error": str(exc)})

        return persist

    async def _deduplicate(
        self, results: list[CorrelationResult]
    ) -> tuple[list[CorrelationResult], int, int]:
        """Remove results that already exist in PG (by correlation_hash).

        Returns (new_results, updated_count, deduplicated_count).
        If a new result has higher confidence, it's kept for update.
        """
        if not results:
            return [], 0, 0

        new_results: list[CorrelationResult] = []
        updated = 0
        deduped = 0

        try:
            pool = self._pg._pool
            if pool is None:
                return results, 0, 0

            hashes = [r.correlation_hash for r in results]
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT correlation_hash, confidence
                    FROM correlation_results
                    WHERE correlation_hash = ANY($1)
                    """,
                    hashes,
                )
                existing: dict[str, float] = {
                    row["correlation_hash"]: row["confidence"] for row in rows
                }

            for r in results:
                if r.correlation_hash not in existing:
                    new_results.append(r)
                elif r.confidence > existing[r.correlation_hash]:
                    new_results.append(r)
                    updated += 1
                else:
                    deduped += 1
        except Exception:
            # If dedup query fails, pass all results through
            return results, 0, 0

        return new_results, updated, deduped
