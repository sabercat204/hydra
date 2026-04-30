"""AnalysisEngine — orchestrates intelligence product generation."""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from hydra.analysis.exceptions import (
    InsufficientDataError,
    ProductNotFoundError,
)
from hydra.analysis.graph import GraphAnalyzer
from hydra.analysis.models import DataBundle, IntelligenceProduct, ProductParams
from hydra.analysis.products.base import BaseProduct
from hydra.analysis.queries import QueryLayer
from hydra.analysis.timeline import TimelineBuilder
from hydra.config import HydraSettings
from hydra.storage.engines.postgres import PostgresEngine
from hydra.utils.hashing import compute_raw_hash

logger = logging.getLogger(__name__)


class AnalysisEngine:
    """Orchestrates intelligence product generation.

    Responsibilities:
    1. Accept product generation requests (type + parameters).
    2. Assemble DataBundle via QueryLayer, GraphAnalyzer, TimelineBuilder.
    3. Dispatch to the appropriate product generator.
    4. Persist the generated product.
    5. Return the product.
    """

    def __init__(
        self,
        query_layer: QueryLayer,
        graph_analyzer: GraphAnalyzer,
        timeline_builder: TimelineBuilder,
        pg_engine: PostgresEngine,
        settings: HydraSettings,
    ) -> None:
        self._queries = query_layer
        self._graph = graph_analyzer
        self._timeline = timeline_builder
        self._pg = pg_engine
        self._settings = settings
        self._products: dict[str, BaseProduct] = {}

    def register_product(self, product: BaseProduct) -> None:
        """Register a product generator by its type ID."""
        self._products[product.product_type] = product

    async def generate(
        self,
        product_type: str,
        params: ProductParams,
    ) -> IntelligenceProduct:
        """Generate an intelligence product.

        Steps:
        1. Resolve product generator.
        2. Build DataBundle.
        3. Call generator.generate(bundle).
        4. Compute product_hash for dedup.
        5. Persist to intelligence_products table.
        6. Return product.
        """
        product = self._products.get(product_type)
        if product is None:
            raise ProductNotFoundError(product_type)

        # Default time window
        now = datetime.now(timezone.utc)
        if params.time_window_end is None:
            params.time_window_end = now.isoformat()
        if params.time_window_start is None:
            lookback = timedelta(hours=product.default_lookback_hours)
            params.time_window_start = (now - lookback).isoformat()

        # Build data bundle
        bundle = await self._build_data_bundle(product, params)

        # Check for sufficient data
        if bundle.total_records == 0:
            tiers = params.tiers or product.source_tiers
            raise InsufficientDataError(product_type, 0, len(tiers))

        # Generate product
        intelligence_product = await product.generate(bundle, params)

        # Persist
        await self._persist_product(intelligence_product)

        return intelligence_product

    async def _build_data_bundle(
        self,
        product: BaseProduct,
        params: ProductParams,
    ) -> DataBundle:
        """Assemble all data needed for product generation.

        Records and correlations are fetched concurrently.
        Graph and timeline depend on record results.
        """
        start = time.monotonic()
        tiers = params.tiers or product.source_tiers
        assert params.time_window_start is not None
        assert params.time_window_end is not None

        # Parallel: records + correlations (correlations need record hashes though)
        # So we fetch records first, then correlations + graph/timeline in parallel
        records = await self._queries.query_records(
            tiers=tiers,
            time_start=params.time_window_start,
            time_end=params.time_window_end,
            region=params.region,
            keywords=params.keywords,
            min_confidence=params.min_confidence,
            max_records=params.max_records,
        )

        total_records = sum(len(recs) for recs in records.values())

        # Collect all record hashes for correlation query
        all_hashes: set[str] = set()
        for recs in records.values():
            for r in recs:
                all_hashes.add(r.raw_hash)

        # Parallel: correlations, graph, timeline
        use_graph = params.include_graph if params.include_graph is not None else product.requires_graph
        use_timeline = params.include_timeline if params.include_timeline is not None else product.requires_timeline

        tasks: dict[str, Any] = {}
        tasks["correlations"] = self._queries.query_correlations(
            all_hashes, min_confidence=params.min_confidence
        )
        if use_graph and all_hashes:
            tasks["graph"] = self._graph.analyze_entity_network(
                entity_hashes=list(all_hashes)[:100],  # cap for performance
                max_depth=2,
                max_nodes=50,
            )
        if use_timeline:
            # Timeline needs correlations, but we can build with empty correlations first
            # and annotate later — or just pass empty and let timeline handle it
            tasks["timeline_placeholder"] = asyncio.sleep(0)

        # Execute parallel tasks
        results: dict[str, Any] = {}
        task_keys = list(tasks.keys())
        task_coros = list(tasks.values())
        gathered = await asyncio.gather(*task_coros, return_exceptions=True)
        for key, result in zip(task_keys, gathered):
            if isinstance(result, Exception):
                logger.warning(f"bundle_task_failed: {key}: {result}")
                results[key] = None
            else:
                results[key] = result

        correlations = results.get("correlations") or []
        graph_data = results.get("graph") if use_graph else None

        # Build timeline with actual correlations
        timeline = None
        if use_timeline:
            try:
                timeline = await self._timeline.build(
                    records=records,
                    correlations=correlations,
                    time_start=params.time_window_start,
                    time_end=params.time_window_end,
                )
            except Exception as exc:
                logger.warning(f"timeline_build_failed: {exc}")

        duration_ms = (time.monotonic() - start) * 1000
        return DataBundle(
            records=records,
            correlations=correlations,
            graph_data=graph_data,
            timeline=timeline,
            time_window_start=params.time_window_start,
            time_window_end=params.time_window_end,
            total_records=total_records,
            query_duration_ms=duration_ms,
        )

    async def _persist_product(self, product: IntelligenceProduct) -> None:
        """Write product to intelligence_products table.

        Dedup: if product_hash already exists, update rather than insert.
        """
        try:
            pool = self._pg._pool
            if pool is None:
                logger.warning("pg_not_connected_skipping_persist")
                return

            sections_json = json.dumps(
                [
                    {
                        "section_id": s.section_id,
                        "title": s.title,
                        "section_type": s.section_type,
                        "content": s.content,
                        "records": s.records,
                        "correlations": s.correlations,
                        "confidence": s.confidence,
                        "order": s.order,
                    }
                    for s in product.sections
                ]
            )
            params_json = json.dumps(product.parameters)

            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO intelligence_products
                        (product_id, product_type, title, classification,
                         generated_at, time_window_start, time_window_end,
                         sections, summary, key_findings,
                         confidence_score, completeness_score,
                         source_tiers, record_count, correlation_count,
                         parameters, product_hash, tags, updated_at)
                    VALUES
                        ($1::uuid, $2, $3, $4,
                         $5::timestamptz, $6::timestamptz, $7::timestamptz,
                         $8::jsonb, $9, $10,
                         $11, $12,
                         $13, $14, $15,
                         $16::jsonb, $17, $18, NOW())
                    ON CONFLICT (product_hash) DO UPDATE SET
                        title = EXCLUDED.title,
                        sections = EXCLUDED.sections,
                        summary = EXCLUDED.summary,
                        key_findings = EXCLUDED.key_findings,
                        confidence_score = EXCLUDED.confidence_score,
                        completeness_score = EXCLUDED.completeness_score,
                        record_count = EXCLUDED.record_count,
                        correlation_count = EXCLUDED.correlation_count,
                        parameters = EXCLUDED.parameters,
                        tags = EXCLUDED.tags,
                        updated_at = NOW()
                    """,
                    product.product_id,
                    product.product_type,
                    product.title,
                    product.classification,
                    product.generated_at,
                    product.time_window_start,
                    product.time_window_end,
                    sections_json,
                    product.summary,
                    product.key_findings,
                    product.confidence_score,
                    product.completeness_score,
                    product.source_tiers,
                    product.record_count,
                    product.correlation_count,
                    params_json,
                    product.product_hash,
                    product.tags,
                )
        except Exception as exc:
            logger.error(f"product_persist_failed: {exc}")

    async def list_products(
        self,
        product_type: str | None = None,
        since: str | None = None,
        limit: int = 50,
    ) -> list[IntelligenceProduct]:
        """Query existing products with optional filters."""
        try:
            pool = self._pg._pool
            if pool is None:
                return []

            params: list[Any] = []
            conditions: list[str] = []
            idx = 1

            if product_type:
                conditions.append(f"product_type = ${idx}")
                params.append(product_type)
                idx += 1
            if since:
                conditions.append(f"generated_at >= ${idx}::timestamptz")
                params.append(since)
                idx += 1

            where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
            sql = f"""
                SELECT product_id::text, product_type, title, classification,
                       generated_at, time_window_start, time_window_end,
                       sections::text, summary, key_findings,
                       confidence_score, completeness_score,
                       source_tiers, record_count, correlation_count,
                       parameters::text, product_hash, tags, updated_at
                FROM intelligence_products
                {where}
                ORDER BY generated_at DESC
                LIMIT ${idx}
            """
            params.append(limit)

            async with pool.acquire() as conn:
                rows = await conn.fetch(sql, *params)

            return [self._row_to_product(row) for row in rows]
        except Exception as exc:
            logger.error(f"list_products_failed: {exc}")
            return []

    async def get_product(self, product_id: str) -> IntelligenceProduct | None:
        """Retrieve a single product by ID."""
        try:
            pool = self._pg._pool
            if pool is None:
                return None

            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT product_id::text, product_type, title, classification,
                           generated_at, time_window_start, time_window_end,
                           sections::text, summary, key_findings,
                           confidence_score, completeness_score,
                           source_tiers, record_count, correlation_count,
                           parameters::text, product_hash, tags, updated_at
                    FROM intelligence_products
                    WHERE product_id = $1::uuid
                    """,
                    product_id,
                )
            if row is None:
                return None
            return self._row_to_product(row)
        except Exception as exc:
            logger.error(f"get_product_failed: {exc}")
            return None

    @staticmethod
    def _row_to_product(row: Any) -> IntelligenceProduct:
        """Convert a PG row to IntelligenceProduct."""
        from hydra.analysis.models import ProductSection

        sections_data = json.loads(row["sections"]) if isinstance(row["sections"], str) else row["sections"]
        sections = [
            ProductSection(
                section_id=s.get("section_id", ""),
                title=s.get("title", ""),
                section_type=s.get("section_type", "narrative"),
                content=s.get("content", ""),
                records=s.get("records", []),
                correlations=s.get("correlations", []),
                confidence=s.get("confidence", 1.0),
                order=s.get("order", 0),
            )
            for s in sections_data
        ]
        params_data = json.loads(row["parameters"]) if isinstance(row["parameters"], str) else row["parameters"]

        return IntelligenceProduct(
            product_id=row["product_id"],
            product_type=row["product_type"],
            title=row["title"],
            classification=row["classification"],
            generated_at=row["generated_at"].isoformat() if hasattr(row["generated_at"], "isoformat") else str(row["generated_at"]),
            time_window_start=row["time_window_start"].isoformat() if hasattr(row["time_window_start"], "isoformat") else str(row["time_window_start"]),
            time_window_end=row["time_window_end"].isoformat() if hasattr(row["time_window_end"], "isoformat") else str(row["time_window_end"]),
            sections=sections,
            summary=row["summary"],
            key_findings=row["key_findings"] or [],
            confidence_score=row["confidence_score"],
            completeness_score=row["completeness_score"],
            source_tiers=row["source_tiers"] or [],
            record_count=row["record_count"],
            correlation_count=row["correlation_count"],
            parameters=params_data,
            product_hash=row["product_hash"],
            tags=row["tags"] or [],
        )
