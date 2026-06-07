"""Tests for StreamRegistry loading and lookup."""

from hydra.registry.stream_registry import StreamRegistry


class TestRegistryLoad:
    """StreamRegistry loads successfully from YAML."""

    def test_loads_thematic_tiers_1_to_28(self, registry: StreamRegistry):
        # Core thematic tiers are always 1-28; surface tiers (29, 100+) are
        # additive and may grow without breaking the registry contract.
        thematic = {tid for tid in registry.tiers if 1 <= tid <= 28}
        assert thematic == set(range(1, 29))

    def test_total_tier_count_includes_mil_int(self, registry: StreamRegistry):
        # 28 thematic + 8 mil_int = 36 (tier 29 lives outside the YAML).
        assert len(registry.tiers) >= 28
        mil_int = {tid for tid in registry.tiers if 100 <= tid <= 107}
        assert mil_int == set(range(100, 108))

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
        # Tier 99 is the gap between thematic (1-28, 29) and mil_int (100-107).
        assert registry.get_tier(99) is None
        assert registry.get_tier(200) is None

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
        # Each entry is (tier_id, StreamSource); thematic + mil_int IDs.
        for tier_id, src in sources:
            assert (1 <= tier_id <= 29) or (100 <= tier_id <= 107)
            assert src.name != ""


class TestRegistrySections:
    """Registry sections are populated."""

    def test_adapters_loaded(self, registry: StreamRegistry):
        # 10 core adapters + doc_repo (mil_int).
        assert len(registry.adapters) == 11
        assert "doc_repo" in registry.adapters

    def test_auth_patterns_loaded(self, registry: StreamRegistry):
        assert len(registry.auth_patterns) == 5

    def test_storage_map_loaded(self, registry: StreamRegistry):
        assert len(registry.storage_map) == 6

    def test_scheduler_cadences_loaded(self, registry: StreamRegistry):
        # 6 core cadences + biweekly + quarterly + monthly + on_change.
        assert len(registry.scheduler_cadences) >= 6
        for new_cadence in ("biweekly", "quarterly", "on_change"):
            assert new_cadence in registry.scheduler_cadences

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

    def test_access_policy_default_open_for_legacy_sources(
        self, registry: StreamRegistry
    ):
        # Tiers 1-28 source lines pre-date the access_policy field, so
        # they must default to "open".
        tier = registry.get_tier(1)
        assert tier is not None
        for src in tier.sources:
            assert src.access_policy == "open"

    def test_access_policy_parsed_for_mil_int_sources(
        self, registry: StreamRegistry
    ):
        tier = registry.get_tier(100)
        assert tier is not None
        policies = {src.access_policy for src in tier.sources}
        assert "open" in policies
        assert "registration" in policies  # DLA ASSIST


class TestMilIntTiers:
    """Tiers 100-107 are registered with the right adapter, cadence, and storage."""

    def test_all_eight_mil_int_tiers_present(self, registry: StreamRegistry):
        for tid in range(100, 108):
            assert registry.get_tier(tid) is not None, tid

    def test_mil_int_content_tiers_use_doc_repo(self, registry: StreamRegistry):
        for tid in range(100, 107):
            tier = registry.get_tier(tid)
            assert tier is not None
            assert tier.adapter == "doc_repo"

    def test_tier_107_uses_rest_json(self, registry: StreamRegistry):
        tier = registry.get_tier(107)
        assert tier is not None
        assert tier.adapter == "rest_json"

    def test_mil_int_storage_engines(self, registry: StreamRegistry):
        for tid in range(100, 107):
            tier = registry.get_tier(tid)
            assert tier is not None
            assert tier.storage is not None
            engines = tier.storage.get("storage_engines", [])
            assert "postgres" in engines
            assert "minio" in engines
            assert "elasticsearch" in engines

    def test_tier_107_storage_postgres_only(self, registry: StreamRegistry):
        tier = registry.get_tier(107)
        assert tier is not None
        assert tier.storage is not None
        assert tier.storage.get("storage_engines") == ["postgres"]

    def test_get_tiers_by_cadence_biweekly(self, registry: StreamRegistry):
        ids = {t.id for t in registry.get_tiers_by_cadence("biweekly")}
        assert ids == {101, 102, 103}

    def test_get_tiers_by_cadence_on_change(self, registry: StreamRegistry):
        ids = {t.id for t in registry.get_tiers_by_cadence("on_change")}
        assert ids == {107}

    def test_get_sources_by_access_policy_subscription(
        self, registry: StreamRegistry
    ):
        results = registry.get_sources_by_access_policy("subscription")
        names = {s.name for _, s in results}
        assert "South Korea KIDA/KJDA" in names
        assert "East View UDB-MIL" in names

    def test_get_sources_by_access_policy_archived(
        self, registry: StreamRegistry
    ):
        results = registry.get_sources_by_access_policy("archived")
        names = {s.name for _, s in results}
        assert "CAST Moscow Defense Brief" in names

    def test_get_sources_by_access_policy_monitor_only(
        self, registry: StreamRegistry
    ):
        results = registry.get_sources_by_access_policy("monitor_only")
        names = {s.name for _, s in results}
        assert "China CNKI" in names
