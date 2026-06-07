"""Tests for the standards cross-reference resolver."""

from __future__ import annotations

from pathlib import Path

import pytest

from hydra.mil_int.xref.families import detect_family, normalize_id
from hydra.mil_int.xref.resolver import XrefResolver, load_xref_seed


@pytest.fixture
def resolver() -> XrefResolver:
    return XrefResolver.from_path(Path("config/mil_int_xref.yaml"))


class TestFamilyDetection:
    @pytest.mark.parametrize(
        "identifier,family",
        [
            ("MIL-STD-461", "MIL_STD"),
            ("MIL HDBK 217F", "MIL_HDBK"),
            ("FIPS 140-3", "FIPS"),
            ("NIST SP 800-53", "NIST_SP_800"),
            ("NIST SP 1800-25", "NIST_SP_1800"),
            ("STANAG 4774", "STANAG"),
            ("DEF STAN 00-970", "DEF_STAN"),
            ("STIG General Purpose Operating System SRG", "STIG"),
            ("ISO/IEC 27001", "ISO_IEC"),
            ("RFC 8446", "RFC"),
        ],
    )
    def test_detect_family(self, identifier: str, family: str):
        assert detect_family(identifier) == family

    def test_normalize_id_collapses_whitespace_and_uppercases(self):
        assert normalize_id("  mil-std-461   ") == "MIL-STD-461"


class TestSeedLoad:
    def test_seed_loads_at_least_one_mapping(self):
        rows = load_xref_seed("config/mil_int_xref.yaml")
        assert len(rows) > 0

    def test_missing_seed_returns_empty_list(self, tmp_path: Path):
        missing = tmp_path / "nope.yaml"
        assert load_xref_seed(missing) == []


class TestResolverLookup:
    def test_seed_round_trip(self, resolver: XrefResolver):
        results = resolver.lookup("MIL-STD-461")
        assert len(results) > 0
        assert any(m.to_id == "NIST SP 800-53" for m in results)

    def test_reverse_lookup_works(self, resolver: XrefResolver):
        results = resolver.lookup("NIST SP 800-53")
        # Should include the mirror of MIL-STD-461 → SP 800-53.
        assert any(m.to_id == "MIL-STD-461" for m in results)

    def test_to_family_filter(self, resolver: XrefResolver):
        results = resolver.lookup("FIPS 140-3", to_family="NIST_SP_800")
        assert all(m.to_family == "NIST_SP_800" for m in results)

    def test_unknown_id_returns_empty(self, resolver: XrefResolver):
        assert resolver.lookup("NONEXISTENT-9999") == []

    def test_size_reflects_seed_rows(self, resolver: XrefResolver):
        rows = load_xref_seed("config/mil_int_xref.yaml")
        assert resolver.size == len(rows)
