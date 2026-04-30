"""Storage engine implementations."""

from __future__ import annotations

from hydra.storage.engines.base import StorageEngine, StoreResult

__all__ = [
    "StorageEngine",
    "StoreResult",
    "PostgresEngine",
    "InfluxEngine",
    "ElasticsearchEngine",
    "Neo4jEngine",
    "MinioEngine",
]


def __getattr__(name: str):
    """Lazy imports to avoid pulling in heavy dependencies at module load."""
    if name == "PostgresEngine":
        from hydra.storage.engines.postgres import PostgresEngine
        return PostgresEngine
    if name == "InfluxEngine":
        from hydra.storage.engines.influxdb import InfluxEngine
        return InfluxEngine
    if name == "ElasticsearchEngine":
        from hydra.storage.engines.elasticsearch import ElasticsearchEngine
        return ElasticsearchEngine
    if name == "Neo4jEngine":
        from hydra.storage.engines.neo4j import Neo4jEngine
        return Neo4jEngine
    if name == "MinioEngine":
        from hydra.storage.engines.minio import MinioEngine
        return MinioEngine
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
