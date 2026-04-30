"""QueryLayer — unified analytical read interface across PG, ES, InfluxDB."""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from hydra.analysis.exceptions import QueryLayerError
from hydra.config import HydraSettings
from hydra.correlation.models import CorrelationResult
from hydra.models.normalized import GeoGeometry, NormalizedRecord, SourceMeta, Tier
from hydra.storage.engines.elasticsearch import ElasticsearchEngine
from hydra.storage.engines.influxdb import InfluxEngine
from hydra.storage.engines.postgres import PostgresEngine

logger = logging.getLogger(__name__)

# Entity extraction map — mirrors P9 conventions for JSONB containment queries.
ENTITY_EXTRACTION_MAP: dict[int, list[str]] = {
    8: ["entity_name", "organization"],
    14: ["entity_name", "supplier", "recipient"],
    15: ["actor1", "actor2", "source_actor", "target_actor"],
    16: ["stix_id", "name", "threat_actor"],
    19: ["ofac_id", "entity_name", "program"],
    21: ["entity_name", "case_id", "perpetrator", "victim"],
}


class QueryLayer:
    """Unified analytical query interface across storage engines."""

    def __init__(
        self,
        pg_engine: PostgresEngine,
        es_engine: ElasticsearchEngine | None,
        influx_engine: InfluxEngine | None,
        settings: HydraSettings,
    ) -> None:
        self._pg = pg_engine
        self._es = es_engine
        self._influx = influx_engine
        self._settings = settings

    # ------------------------------------------------------------------
    # Record queries
    # ------------------------------------------------------------------

    async def query_records(
        self,
        tiers: list[int],
        time_start: str,
        time_end: str,
        region: str | None = None,
        keywords: list[str] | None = None,
        min_confidence: float = 0.0,
        max_records: int = 10_000,
    ) -> dict[int, list[NormalizedRecord]]:
        """Query NormalizedRecords from PostgreSQL, optionally filtered."""
        start = time.monotonic()
        result: dict[int, list[NormalizedRecord]] = {}

        # If keywords provided and ES available, use ES for discovery first
        if keywords and self._es:
            try:
                es_records = await self._search_text_es(
                    query=" ".join(keywords),
                    tiers=tiers,
                    time_start=time_start,
                    time_end=time_end,
                    limit=max_records,
                )
                for rec in es_records:
                    tier_int = int(rec.tier)
                    result.setdefault(tier_int, []).append(rec)
                return result
            except Exception:
                logger.warning("es_keyword_search_failed_falling_back_to_pg")

        try:
            pool = self._pg._pool
            if pool is None:
                raise QueryLayerError("postgres", "Not connected")

            async with pool.acquire() as conn:
                for tier_id in tiers:
                    params: list[Any] = [tier_id, time_start, time_end, min_confidence]
                    sql = (
                        "SELECT stream_id, tier, timestamp, "
                        "ST_AsGeoJSON(geo)::text AS geo_json, "
                        "payload::text, raw_hash, ingested_at, confidence, tags "
                        "FROM normalized_records "
                        "WHERE tier = $1 "
                        "AND timestamp >= $2::timestamptz "
                        "AND timestamp <= $3::timestamptz "
                        "AND confidence >= $4 "
                    )
                    idx = 5

                    if region:
                        sql += f"AND (payload->>'country_code' = ${idx} OR payload->>'country' = ${idx}) "
                        params.append(region)
                        idx += 1

                    if keywords and not self._es:
                        # PG JSONB text fallback
                        kw_clause = " OR ".join(
                            f"payload::text ILIKE ${idx + i}" for i in range(len(keywords))
                        )
                        sql += f"AND ({kw_clause}) "
                        for kw in keywords:
                            params.append(f"%{kw}%")
                            idx += 1

                    sql += f"ORDER BY timestamp DESC LIMIT ${idx}"
                    params.append(max_records)

                    rows = await conn.fetch(sql, *params)
                    tier_records = [self._row_to_record(row) for row in rows]
                    if tier_records:
                        result[tier_id] = tier_records
        except QueryLayerError:
            raise
        except Exception as exc:
            raise QueryLayerError("postgres", str(exc)) from exc

        return result

    # ------------------------------------------------------------------
    # Correlation queries
    # ------------------------------------------------------------------

    async def query_correlations(
        self,
        record_hashes: set[str],
        min_confidence: float = 0.0,
    ) -> list[CorrelationResult]:
        """Fetch correlations involving any of the given record hashes."""
        if not record_hashes:
            return []
        try:
            pool = self._pg._pool
            if pool is None:
                raise QueryLayerError("postgres", "Not connected")

            hash_list = list(record_hashes)
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT correlation_id::text, pipeline_id,
                           record_a_hash, record_b_hash,
                           tier_a, tier_b, confidence,
                           match_dimensions::text, evidence::text,
                           correlation_hash, tags,
                           created_at
                    FROM correlation_results
                    WHERE (record_a_hash = ANY($1) OR record_b_hash = ANY($1))
                      AND confidence >= $2
                    ORDER BY confidence DESC
                    """,
                    hash_list,
                    min_confidence,
                )
            results: list[CorrelationResult] = []
            for row in rows:
                md = json.loads(row["match_dimensions"]) if isinstance(row["match_dimensions"], str) else row["match_dimensions"]
                ev = json.loads(row["evidence"]) if isinstance(row["evidence"], str) else row["evidence"]
                results.append(
                    CorrelationResult(
                        correlation_id=row["correlation_id"],
                        pipeline_id=row["pipeline_id"],
                        record_a_hash=row["record_a_hash"],
                        record_b_hash=row["record_b_hash"],
                        tier_a=row["tier_a"],
                        tier_b=row["tier_b"],
                        confidence=row["confidence"],
                        match_dimensions=md,
                        evidence=ev,
                        correlation_hash=row["correlation_hash"],
                        created_at=row["created_at"].isoformat() if hasattr(row["created_at"], "isoformat") else str(row["created_at"]),
                        tags=row["tags"] or [],
                    )
                )
            return results
        except QueryLayerError:
            raise
        except Exception as exc:
            raise QueryLayerError("postgres", str(exc)) from exc

    # ------------------------------------------------------------------
    # Entity queries
    # ------------------------------------------------------------------

    async def query_entity_by_id(
        self,
        entity_id: str,
        tiers: list[int],
        time_start: str | None = None,
        time_end: str | None = None,
    ) -> list[NormalizedRecord]:
        """Query records matching an entity ID via JSONB containment."""
        try:
            pool = self._pg._pool
            if pool is None:
                raise QueryLayerError("postgres", "Not connected")

            all_records: list[NormalizedRecord] = []
            async with pool.acquire() as conn:
                for tier_id in tiers:
                    id_fields = ENTITY_EXTRACTION_MAP.get(tier_id, ["entity_id"])
                    for id_field in id_fields:
                        containment = json.dumps({id_field: entity_id})
                        params: list[Any] = [tier_id, containment]
                        sql = (
                            "SELECT stream_id, tier, timestamp, "
                            "ST_AsGeoJSON(geo)::text AS geo_json, "
                            "payload::text, raw_hash, ingested_at, confidence, tags "
                            "FROM normalized_records "
                            "WHERE tier = $1 AND payload @> $2::jsonb "
                        )
                        idx = 3
                        if time_start:
                            sql += f"AND timestamp >= ${idx}::timestamptz "
                            params.append(time_start)
                            idx += 1
                        if time_end:
                            sql += f"AND timestamp <= ${idx}::timestamptz "
                            params.append(time_end)
                            idx += 1
                        sql += "ORDER BY timestamp DESC LIMIT 1000"
                        rows = await conn.fetch(sql, *params)
                        for row in rows:
                            all_records.append(self._row_to_record(row))
            # Deduplicate by raw_hash
            seen: set[str] = set()
            unique: list[NormalizedRecord] = []
            for rec in all_records:
                if rec.raw_hash not in seen:
                    seen.add(rec.raw_hash)
                    unique.append(rec)
            return unique
        except QueryLayerError:
            raise
        except Exception as exc:
            raise QueryLayerError("postgres", str(exc)) from exc

    async def query_entity_by_name(
        self,
        entity_name: str,
        tiers: list[int],
        time_start: str | None = None,
        time_end: str | None = None,
        limit: int = 100,
    ) -> list[NormalizedRecord]:
        """Search for entity by name — ES preferred, PG JSONB fallback."""
        if self._es:
            try:
                return await self._search_entity_name_es(
                    entity_name, tiers, time_start, time_end, limit
                )
            except Exception:
                logger.warning("es_entity_name_search_failed_falling_back_to_pg")

        # PG fallback: ILIKE on payload text
        try:
            pool = self._pg._pool
            if pool is None:
                raise QueryLayerError("postgres", "Not connected")

            all_records: list[NormalizedRecord] = []
            async with pool.acquire() as conn:
                for tier_id in tiers:
                    params: list[Any] = [tier_id, f"%{entity_name}%"]
                    sql = (
                        "SELECT stream_id, tier, timestamp, "
                        "ST_AsGeoJSON(geo)::text AS geo_json, "
                        "payload::text, raw_hash, ingested_at, confidence, tags "
                        "FROM normalized_records "
                        "WHERE tier = $1 AND payload::text ILIKE $2 "
                    )
                    idx = 3
                    if time_start:
                        sql += f"AND timestamp >= ${idx}::timestamptz "
                        params.append(time_start)
                        idx += 1
                    if time_end:
                        sql += f"AND timestamp <= ${idx}::timestamptz "
                        params.append(time_end)
                        idx += 1
                    sql += f"ORDER BY timestamp DESC LIMIT ${idx}"
                    params.append(limit)
                    rows = await conn.fetch(sql, *params)
                    for row in rows:
                        all_records.append(self._row_to_record(row))
            return all_records
        except QueryLayerError:
            raise
        except Exception as exc:
            raise QueryLayerError("postgres", str(exc)) from exc

    # ------------------------------------------------------------------
    # Time-series queries
    # ------------------------------------------------------------------

    async def query_timeseries(
        self,
        stream_ids: list[str],
        time_start: str,
        time_end: str,
        aggregation: str = "raw",
        fields: list[str] | None = None,
    ) -> dict[str, list[dict]]:
        """Query time-series data from InfluxDB, PG fallback."""
        if self._influx:
            try:
                return await self._query_timeseries_influx(
                    stream_ids, time_start, time_end, aggregation, fields
                )
            except Exception:
                logger.warning("influx_timeseries_query_failed_falling_back_to_pg")

        # PG fallback
        try:
            pool = self._pg._pool
            if pool is None:
                raise QueryLayerError("postgres", "Not connected")

            result: dict[str, list[dict]] = {}
            async with pool.acquire() as conn:
                for sid in stream_ids:
                    rows = await conn.fetch(
                        """
                        SELECT timestamp, payload::text, raw_hash
                        FROM normalized_records
                        WHERE stream_id = $1
                          AND timestamp >= $2::timestamptz
                          AND timestamp <= $3::timestamptz
                        ORDER BY timestamp ASC
                        LIMIT 10000
                        """,
                        sid,
                        time_start,
                        time_end,
                    )
                    entries: list[dict] = []
                    for row in rows:
                        payload = json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
                        entry: dict[str, Any] = {
                            "timestamp": row["timestamp"].isoformat(),
                            "raw_hash": row["raw_hash"],
                        }
                        if fields:
                            for f in fields:
                                entry[f] = payload.get(f)
                        else:
                            entry.update(payload)
                        entries.append(entry)
                    result[sid] = entries
            return result
        except QueryLayerError:
            raise
        except Exception as exc:
            raise QueryLayerError("postgres", str(exc)) from exc

    # ------------------------------------------------------------------
    # Full-text search
    # ------------------------------------------------------------------

    async def search_text(
        self,
        query: str,
        tiers: list[int] | None = None,
        time_start: str | None = None,
        time_end: str | None = None,
        limit: int = 100,
    ) -> list[NormalizedRecord]:
        """Full-text search via ES, PG JSONB fallback."""
        if self._es:
            try:
                return await self._search_text_es(query, tiers, time_start, time_end, limit)
            except Exception:
                logger.warning("es_text_search_failed_falling_back_to_pg")

        # PG fallback
        try:
            pool = self._pg._pool
            if pool is None:
                raise QueryLayerError("postgres", "Not connected")

            records: list[NormalizedRecord] = []
            async with pool.acquire() as conn:
                params: list[Any] = [f"%{query}%"]
                sql = (
                    "SELECT stream_id, tier, timestamp, "
                    "ST_AsGeoJSON(geo)::text AS geo_json, "
                    "payload::text, raw_hash, ingested_at, confidence, tags "
                    "FROM normalized_records "
                    "WHERE payload::text ILIKE $1 "
                )
                idx = 2
                if tiers:
                    sql += f"AND tier = ANY(${idx}) "
                    params.append(tiers)
                    idx += 1
                if time_start:
                    sql += f"AND timestamp >= ${idx}::timestamptz "
                    params.append(time_start)
                    idx += 1
                if time_end:
                    sql += f"AND timestamp <= ${idx}::timestamptz "
                    params.append(time_end)
                    idx += 1
                sql += f"ORDER BY timestamp DESC LIMIT ${idx}"
                params.append(limit)
                rows = await conn.fetch(sql, *params)
                for row in rows:
                    records.append(self._row_to_record(row))
            return records
        except QueryLayerError:
            raise
        except Exception as exc:
            raise QueryLayerError("postgres", str(exc)) from exc

    # ------------------------------------------------------------------
    # Private helpers — ES
    # ------------------------------------------------------------------

    async def _search_text_es(
        self,
        query: str,
        tiers: list[int] | None,
        time_start: str | None,
        time_end: str | None,
        limit: int,
    ) -> list[NormalizedRecord]:
        """Elasticsearch multi_match search."""
        assert self._es and self._es._client
        must: list[dict] = [{"multi_match": {"query": query, "fields": ["payload.*", "tags"]}}]
        if tiers:
            must.append({"terms": {"tier": tiers}})
        if time_start or time_end:
            range_q: dict[str, Any] = {}
            if time_start:
                range_q["gte"] = time_start
            if time_end:
                range_q["lte"] = time_end
            must.append({"range": {"timestamp": range_q}})

        body = {"query": {"bool": {"must": must}}, "size": limit, "sort": [{"timestamp": "desc"}]}
        resp = await self._es._client.search(index="hydra-tier-*", body=body)
        records: list[NormalizedRecord] = []
        for hit in resp.get("hits", {}).get("hits", []):
            src = hit["_source"]
            records.append(self._es_hit_to_record(src))
        return records

    async def _search_entity_name_es(
        self,
        entity_name: str,
        tiers: list[int],
        time_start: str | None,
        time_end: str | None,
        limit: int,
    ) -> list[NormalizedRecord]:
        """ES more_like_this for entity name search."""
        assert self._es and self._es._client
        must: list[dict] = [
            {
                "more_like_this": {
                    "fields": ["payload.entity_name", "payload.name", "payload.actor1", "payload.actor2"],
                    "like": entity_name,
                    "min_term_freq": 1,
                    "min_doc_freq": 1,
                }
            }
        ]
        if tiers:
            must.append({"terms": {"tier": tiers}})
        if time_start or time_end:
            range_q: dict[str, Any] = {}
            if time_start:
                range_q["gte"] = time_start
            if time_end:
                range_q["lte"] = time_end
            must.append({"range": {"timestamp": range_q}})

        body = {"query": {"bool": {"must": must}}, "size": limit}
        resp = await self._es._client.search(index="hydra-tier-*", body=body)
        records: list[NormalizedRecord] = []
        for hit in resp.get("hits", {}).get("hits", []):
            src = hit["_source"]
            records.append(self._es_hit_to_record(src))
        return records

    async def _query_timeseries_influx(
        self,
        stream_ids: list[str],
        time_start: str,
        time_end: str,
        aggregation: str,
        fields: list[str] | None,
    ) -> dict[str, list[dict]]:
        """Query InfluxDB for time-series data."""
        assert self._influx and self._influx._client
        result: dict[str, list[dict]] = {}
        query_api = self._influx._client.query_api()
        bucket = self._settings.database.influxdb_bucket

        for sid in stream_ids:
            flux = (
                f'from(bucket: "{bucket}") '
                f"|> range(start: {time_start}, stop: {time_end}) "
                f'|> filter(fn: (r) => r["stream_id"] == "{sid}") '
            )
            if aggregation != "raw":
                flux += f'|> aggregateWindow(every: {aggregation}, fn: mean, createEmpty: false) '
            if fields:
                field_filter = " or ".join(f'r["_field"] == "{f}"' for f in fields)
                flux += f"|> filter(fn: (r) => {field_filter}) "

            tables = await query_api.query(flux)
            entries: list[dict] = []
            for table in tables:
                for record in table.records:
                    entries.append({
                        "timestamp": record.get_time().isoformat() if record.get_time() else "",
                        "field": record.get_field(),
                        "value": record.get_value(),
                    })
            result[sid] = entries
        return result

    # ------------------------------------------------------------------
    # Row conversion helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _row_to_record(row: Any) -> NormalizedRecord:
        """Convert an asyncpg Row to NormalizedRecord."""
        geo = None
        if row["geo_json"]:
            geo_data = json.loads(row["geo_json"])
            geo = GeoGeometry(
                type=geo_data.get("type", "Point"),
                coordinates=geo_data.get("coordinates"),
            )
        payload = json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
        return NormalizedRecord(
            stream_id=row["stream_id"],
            tier=Tier(row["tier"]),
            timestamp=row["timestamp"],
            geo=geo,
            payload=payload,
            source_meta=SourceMeta(
                source_name="analysis_query",
                adapter_type="analysis",
            ),
            raw_hash=row["raw_hash"],
            ingested_at=row["ingested_at"],
            confidence=row["confidence"],
            tags=row["tags"] or [],
        )

    @staticmethod
    def _es_hit_to_record(src: dict) -> NormalizedRecord:
        """Convert an ES hit _source to NormalizedRecord."""
        geo = None
        if src.get("geo"):
            geo = GeoGeometry(
                type=src["geo"].get("type", "Point"),
                coordinates=src["geo"].get("coordinates"),
            )
        return NormalizedRecord(
            stream_id=src.get("stream_id", ""),
            tier=Tier(src.get("tier", 1)),
            timestamp=src.get("timestamp", ""),
            geo=geo,
            payload=src.get("payload", {}),
            source_meta=SourceMeta(
                source_name=src.get("source_name", "es_search"),
                adapter_type="analysis",
            ),
            raw_hash=src.get("raw_hash", ""),
            ingested_at=src.get("ingested_at", ""),
            confidence=src.get("confidence", 1.0),
            tags=src.get("tags", []),
        )
