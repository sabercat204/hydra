"""Neo4j storage engine — graph secondary store for relationship-dense tiers."""

from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timezone
from typing import Any

from hydra.config import HydraSettings
from hydra.models.normalized import NormalizedRecord
from hydra.storage.engines.base import StorageEngine, StoreResult
from hydra.storage.exceptions import StorageConnectionError, StorageWriteError
from hydra.storage.health import StorageHealth

logger = logging.getLogger(__name__)

_NON_ALNUM = re.compile(r"[^a-zA-Z0-9_ ]")


def _to_pascal_case(s: str) -> str:
    """Convert a string to PascalCase for Neo4j node labels."""
    # Replace hyphens and underscores with spaces first, then clean
    cleaned = s.replace("-", " ").replace("_", " ")
    cleaned = _NON_ALNUM.sub("", cleaned)
    parts = cleaned.split()
    if not parts:
        return ""
    return "".join(p.capitalize() for p in parts)


def _to_upper_snake(s: str) -> str:
    """Convert a string to UPPER_SNAKE_CASE for Neo4j edge labels."""
    cleaned = s.replace("-", " ").replace("_", " ")
    cleaned = _NON_ALNUM.sub("", cleaned)
    parts = cleaned.split()
    if not parts:
        return ""
    return "_".join(p.upper() for p in parts)


class Neo4jEngine(StorageEngine):
    """Neo4j graph secondary store for relationship-dense tiers."""

    def __init__(self, settings: HydraSettings, credential_store: Any = None) -> None:
        self._settings = settings
        self._credential_store = credential_store
        self._driver: Any = None

    async def connect(self) -> None:
        from neo4j import AsyncGraphDatabase

        auth = ("neo4j", "neo4j")
        if self._credential_store:
            try:
                creds = self._credential_store.get("neo4j_admin")
                auth = (creds.get("username", "neo4j"), creds.get("password", "neo4j"))
            except Exception:
                logger.warning("neo4j_no_credentials")

        try:
            self._driver = AsyncGraphDatabase.driver(
                self._settings.database.neo4j_uri,
                auth=auth,
                max_connection_pool_size=10,
            )
        except Exception as exc:
            raise StorageConnectionError("neo4j", f"Failed to connect: {exc}", cause=exc) from exc

    async def disconnect(self) -> None:
        if self._driver:
            await self._driver.close()
            self._driver = None

    async def store(self, records: list[NormalizedRecord], graph_schema: dict | None = None) -> StoreResult:
        """Create nodes and edges from records using the graph_schema declaration."""
        if not self._driver:
            raise StorageConnectionError("neo4j", "Not connected")

        if not graph_schema:
            return StoreResult(engine="neo4j", stored=0)

        start = time.monotonic()
        stored = 0
        failed = 0
        errors: list[dict] = []

        node_label_field = graph_schema.get("node_label_field", "type")
        node_id_field = graph_schema.get("node_id_field", "id")
        node_properties = graph_schema.get("node_properties", [])
        edge_rules = graph_schema.get("edge_rules", [])

        async with self._driver.session() as session:
            try:
                async with await session.begin_transaction() as tx:
                    for record in records:
                        try:
                            # Node creation
                            raw_label = str(record.payload.get(node_label_field, ""))
                            label = _to_pascal_case(raw_label)
                            if not label:
                                failed += 1
                                errors.append({"record_hash": record.raw_hash, "error": "Empty node label"})
                                continue

                            node_id = record.payload.get(node_id_field)
                            if node_id is None:
                                node_id = record.raw_hash

                            props: dict[str, Any] = {"id": str(node_id)}
                            for prop in node_properties:
                                val = record.payload.get(prop)
                                if val is not None:
                                    # Neo4j doesn't accept dicts/lists as properties directly
                                    if isinstance(val, (dict, list)):
                                        import json
                                        props[prop] = json.dumps(val)
                                    else:
                                        props[prop] = val

                            # Metadata properties
                            props["_stream_id"] = record.stream_id
                            props["_tier"] = int(record.tier)
                            props["_timestamp"] = record.timestamp.isoformat()
                            props["_raw_hash"] = record.raw_hash
                            props["_ingested_at"] = record.ingested_at.isoformat()

                            cypher_node = f"MERGE (n:{label} {{id: $id}}) SET n += $properties"
                            await tx.run(cypher_node, id=str(node_id), properties=props)

                            # Edge creation
                            for rule in edge_rules:
                                record_type = record.payload.get("type")
                                if record_type != rule.get("type"):
                                    continue

                                source_field = rule.get("source_field")
                                target_field = rule.get("target_field")
                                source_id = record.payload.get(source_field) if source_field else None
                                target_id = record.payload.get(target_field) if target_field else None

                                if source_id and target_id:
                                    edge_label = rule.get("edge_label_static")
                                    if not edge_label:
                                        edge_label_field = rule.get("edge_label_field")
                                        if edge_label_field:
                                            edge_label = _to_upper_snake(str(record.payload.get(edge_label_field, "")))
                                    else:
                                        edge_label = _to_upper_snake(edge_label)

                                    if edge_label:
                                        edge_props: dict[str, Any] = {}
                                        for ep in rule.get("edge_properties", []):
                                            val = record.payload.get(ep)
                                            if val is not None:
                                                if isinstance(val, (dict, list)):
                                                    import json
                                                    edge_props[ep] = json.dumps(val)
                                                else:
                                                    edge_props[ep] = val

                                        cypher_edge = (
                                            f"MATCH (a {{id: $source_id}}), (b {{id: $target_id}}) "
                                            f"MERGE (a)-[r:{edge_label}]->(b) SET r += $properties"
                                        )
                                        await tx.run(
                                            cypher_edge,
                                            source_id=str(source_id),
                                            target_id=str(target_id),
                                            properties=edge_props,
                                        )

                            stored += 1
                        except Exception as exc:
                            failed += 1
                            errors.append({"record_hash": record.raw_hash, "error": str(exc)})

                    await tx.commit()
            except Exception as exc:
                failed = len(records)
                stored = 0
                errors = [{"record_hash": r.raw_hash, "error": str(exc)} for r in records]
                logger.error("neo4j_transaction_error", extra={"error": str(exc)})

        duration_ms = (time.monotonic() - start) * 1000
        return StoreResult(engine="neo4j", stored=stored, failed=failed, duration_ms=duration_ms, errors=errors)

    async def health_check(self) -> StorageHealth:
        start = time.monotonic()
        try:
            if not self._driver:
                return StorageHealth(engine="neo4j", status="UNREACHABLE", latency_ms=0.0)
            async with self._driver.session() as session:
                await session.run("RETURN 1")
            latency = (time.monotonic() - start) * 1000
            return StorageHealth(engine="neo4j", status="OK", latency_ms=latency)
        except Exception as exc:
            latency = (time.monotonic() - start) * 1000
            return StorageHealth(engine="neo4j", status="UNREACHABLE", latency_ms=latency, details={"error": str(exc)})
