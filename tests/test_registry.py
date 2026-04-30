"""Tests for StreamRegistry loading and lookup."""

from hydra.registry.stream_registry import StreamRegistry


class TestRegistryLoad:
    """StreamRegistry loads successfully from YAML."""

    def test_loads_all_28_tiers(self, registry: StreamRegistry):
        assert len(registry.tiers) == 28

    def test_tier_ids_are_1_to_28(self, registry: StreamRegistry):
        assert set(registry.tiers.keys()) == set(range(1, 29))

    def test_tier_1_name(self, registry: StreamRegistry):
        tier = registry.get_tier(1)
        assert tier is not None
        assert tier.name == "Geophysical & Seismic"

    def test_tier_1_has_7_sources(self, registry: StreamRegistry):
        tier = registry.get_tier(1)
        assert tier is not None
        assert tier.streams == 7
        assert len(tier.sources) == 7

    def test_tier_28_exists(self, registry: StreamRegistry):
        tier = registry.get_tier(28)
        assert tier is not None
        assert tier.name == "National Portal Index"


class TestRegistryLookups:
    """StreamRegistry lookup methods return correct results."""

    def test_get_tier_returns_none_for_invalid(self, registry: StreamRegistry):
        assert registry.get_tier(0) is None
        assert registry.get_tier(99) is None

    def test_get_tiers_by_adapter_rest_json(self, registry: StreamRegistry):
        tiers = registry.get_tiers_by_adapter("rest_json")
        assert len(tiers) > 0
        for t in tiers:
            assert t.adapter == "rest_json"

    def test_get_tiers_by_adapter_fdsn(self, registry: StreamRegistry):
        tiers = registry.get_tiers_by_adapter("fdsn")
        assert len(tiers) >= 1
        assert any(t.id == 1 for t in tiers)

    def test_get_tiers_by_cadence_sub_minute(self, registry: StreamRegistry):
        tiers = registry.get_tiers_by_cadence("sub_minute")
        assert len(tiers) >= 2
        tier_ids = {t.id for t in tiers}
        assert 1 in tier_ids  # Geophysical
        assert 18 in tier_ids  # Aviation & Maritime

    def test_get_tiers_by_cadence_daily(self, registry: StreamRegistry):
        tiers = registry.get_tiers_by_cadence("daily")
        assert len(tiers) >= 5

    def test_get_all_sources(self, registry: StreamRegistry):
        sources = registry.get_all_sources()
        assert len(sources) > 100  # 140+ streams total
        # Each entry is (tier_id, StreamSource)
        for tier_id, src in sources:
            assert 1 <= tier_id <= 28
            assert src.name != ""


class TestRegistrySections:
    """Registry sections are populated."""

    def test_adapters_loaded(self, registry: StreamRegistry):
        assert len(registry.adapters) == 10

    def test_auth_patterns_loaded(self, registry: StreamRegistry):
        assert len(registry.auth_patterns) == 5

    def test_storage_map_loaded(self, registry: StreamRegistry):
        assert len(registry.storage_map) == 6

    def test_scheduler_cadences_loaded(self, registry: StreamRegistry):
        assert len(registry.scheduler_cadences) == 6

    def test_correlation_pipelines_loaded(self, registry: StreamRegistry):
        assert len(registry.correlation_pipelines) == 3
        assert "geospatial_temporal" in registry.correlation_pipelines
        assert "entity_network" in registry.correlation_pipelines
        assert "threat_convergence" in registry.correlation_pipelines


class TestSourceParsing:
    """Source pipe-delimited strings parse correctly."""

    def test_source_has_name_and_url(self, registry: StreamRegistry):
        tier = registry.get_tier(1)
        assert tier is not None
        src = tier.sources[0]
        assert "USGS" in src.name
        assert src.url.startswith("https://")

    def test_source_format_populated(self, registry: StreamRegistry):
        tier = registry.get_tier(1)
        assert tier is not None
        for src in tier.sources:
            assert src.format != ""

    def test_tier_formats_split(self, registry: StreamRegistry):
        tier = registry.get_tier(1)
        assert tier is not None
        assert isinstance(tier.formats, list)
        assert len(tier.formats) >= 2

    def test_fallback_none_when_absent(self, registry: StreamRegistry):
        tier = registry.get_tier(2)
        assert tier is not None
        assert tier.fallback is None

    def test_fallback_present(self, registry: StreamRegistry):
        tier = registry.get_tier(1)
        assert tier is not None
        assert tier.fallback == "rest_json"
