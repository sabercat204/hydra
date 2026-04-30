"""Tests for HydraSettings configuration loading."""

from pathlib import Path

from hydra.config import HydraSettings, get_settings


class TestConfigLoad:
    """Config loads from settings.yaml."""

    def test_default_settings(self):
        s = HydraSettings()
        assert s.database.pg_pool_min == 5
        assert s.api.port == 8000

    def test_load_from_yaml(self):
        s = get_settings(Path("config/settings.yaml"))
        assert s.database.postgres_dsn != ""
        assert s.api.port == 8000

    def test_nested_database_settings(self):
        s = get_settings(Path("config/settings.yaml"))
        assert "postgresql" in s.database.postgres_dsn
        assert "influxdb" in s.database.influxdb_url
        assert "elasticsearch" in s.database.elasticsearch_url
        assert "neo4j" in s.database.neo4j_uri
        assert "minio" in s.database.minio_url
        assert "redis" in s.database.redis_url

    def test_stream_registry_path(self):
        s = get_settings(Path("config/settings.yaml"))
        assert s.stream_registry_path == Path("src/hydra/registry/stream_registry.yaml")

    def test_credential_store_path(self):
        s = get_settings(Path("config/settings.yaml"))
        assert s.credential_store_path == Path("config/credentials.yaml")

    def test_missing_yaml_returns_defaults(self):
        s = get_settings(Path("nonexistent.yaml"))
        assert s.api.port == 8000
