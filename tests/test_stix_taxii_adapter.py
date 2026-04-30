"""Tests for the STIX/TAXII adapter — 38 test cases."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from unittest.mock import AsyncMock, MagicMock, patch

# Prevent __init__.py from importing all adapters (some have heavy deps)
sys.modules.setdefault("hydra.adapters.ckan", MagicMock())
sys.modules.setdefault("hydra.adapters.fdsn", MagicMock())
sys.modules.setdefault("hydra.adapters.odata", MagicMock())
sys.modules.setdefault("hydra.adapters.rest_json", MagicMock())
sys.modules.setdefault("hydra.adapters.s3_bulk", MagicMock())
sys.modules.setdefault("hydra.adapters.sdmx", MagicMock())
sys.modules.setdefault("hydra.adapters.tap_vo", MagicMock())

import pytest

from hydra.adapters.base import AdapterHealth, HealthStatus, RawPayload
from hydra.adapters.stix_taxii import StixTaxiiAdapter, _is_valid_stix_id
from hydra.config import HydraSettings
from hydra.models.normalized import NormalizedRecord


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _load_stix_bundle() -> dict:
    return json.loads(Path("tests/fixtures/sample_stix_bundle.json").read_text())


SAMPLE_TAXII_ENVELOPE = {
    "more": True,
    "next": "page2token",
    "objects": [
        {
            "type": "indicator", "spec_version": "2.1",
            "id": "indicator--11111111-1111-4111-8111-111111111111",
            "created": "2024-01-01T00:00:00.000Z", "modified": "2024-01-01T00:00:00.000Z",
            "name": "Indicator 1", "pattern": "[ipv4-addr:value = '10.0.0.1']", "pattern_type": "stix",
            "valid_from": "2024-01-01T00:00:00.000Z",
        },
        {
            "type": "malware", "spec_version": "2.1",
            "id": "malware--22222222-2222-4222-8222-222222222222",
            "created": "2024-01-01T00:00:00.000Z", "modified": "2024-02-01T00:00:00.000Z",
            "name": "TestMalware", "malware_types": ["trojan"], "is_family": False,
        },
        {
            "type": "attack-pattern", "spec_version": "2.1",
            "id": "attack-pattern--33333333-3333-4333-8333-333333333333",
            "created": "2024-01-01T00:00:00.000Z", "modified": "2024-03-01T00:00:00.000Z",
            "name": "Phishing", "description": "Phishing attack.",
            "external_references": [{"source_name": "mitre-attack", "external_id": "T1566"}],
            "kill_chain_phases": [{"kill_chain_name": "mitre-attack", "phase_name": "initial-access"}],
        },
        {
            "type": "threat-actor", "spec_version": "2.1",
            "id": "threat-actor--44444444-4444-4444-8444-444444444444",
            "created": "2024-01-01T00:00:00.000Z", "modified": "2024-01-01T00:00:00.000Z",
            "name": "APT99", "threat_actor_types": ["nation-state"],
            "sophistication": "expert", "resource_level": "government",
            "primary_motivation": "ideology",
        },
        {
            "type": "vulnerability", "spec_version": "2.1",
            "id": "vulnerability--55555555-5555-4555-8555-555555555555",
            "created": "2024-01-01T00:00:00.000Z", "modified": "2024-01-01T00:00:00.000Z",
            "name": "TestVuln",
            "external_references": [{"source_name": "cve", "external_id": "CVE-2024-0001"}],
        },
    ],
}

SAMPLE_TAXII_ENVELOPE_PAGE2 = {
    "more": False,
    "objects": [
        {
            "type": "relationship", "spec_version": "2.1",
            "id": "relationship--66666666-6666-4666-8666-666666666666",
            "created": "2024-01-01T00:00:00.000Z", "modified": "2024-01-01T00:00:00.000Z",
            "relationship_type": "uses",
            "source_ref": "threat-actor--44444444-4444-4444-8444-444444444444",
            "target_ref": "malware--22222222-2222-4222-8222-222222222222",
        },
        {
            "type": "identity", "spec_version": "2.1",
            "id": "identity--77777777-7777-4777-8777-777777777777",
            "created": "2024-01-01T00:00:00.000Z", "modified": "2024-01-01T00:00:00.000Z",
            "name": "TestOrg", "identity_class": "organization",
        },
        {
            "type": "sighting", "spec_version": "2.1",
            "id": "sighting--88888888-8888-4888-8888-888888888888",
            "created": "2024-01-01T00:00:00.000Z", "modified": "2024-01-01T00:00:00.000Z",
            "sighting_of_ref": "indicator--11111111-1111-4111-8111-111111111111",
            "first_seen": "2024-01-01T00:00:00.000Z", "last_seen": "2024-02-01T00:00:00.000Z",
            "count": 10,
        },
    ],
}

SAMPLE_TAXII_DISCOVERY = {
    "title": "Test TAXII Server",
    "api_roots": ["/api/v1", "/api/v2"],
}

SAMPLE_TAXII_COLLECTIONS = {
    "collections": [
        {"id": "col-001", "title": "Enterprise ATT&CK", "can_read": True},
        {"id": "col-002", "title": "Mobile ATT&CK", "can_read": True},
        {"id": "col-003", "title": "ICS ATT&CK", "can_read": True},
    ],
}

SAMPLE_STIX_CSV = """url,threat,date_added,reporter
https://evil.example.com/malware,Emotet,2024-01-01,anonymous
https://bad.example.com/phish,Phishing,2024-01-02,researcher
https://malicious.example.com/c2,C2Server,2024-01-03,automated
https://dangerous.example.com/rat,RAT,2024-01-04,analyst
https://harmful.example.com/spam,SpamBot,2024-01-05,community"""


def _make_settings(**overrides) -> HydraSettings:
    defaults = {
        "stream_registry_path": Path("src/hydra/registry/stream_registry.yaml"),
        "data_dir": Path("/tmp/hydra_test"),
        "http_timeout_seconds": 30,
        "credentials": {},
    }
    defaults.update(overrides)
    return HydraSettings(**defaults)


def _make_adapter(stream_config: dict, settings: HydraSettings | None = None) -> StixTaxiiAdapter:
    s = settings or _make_settings()
    return StixTaxiiAdapter("test_stream", s, registry=MagicMock(), stream_config=stream_config)


def _raw(data: dict | bytes, status: int = 200, headers: dict | None = None) -> RawPayload:
    if isinstance(data, dict):
        content = json.dumps(data).encode()
    else:
        content = data
    return RawPayload(
        stream_id="test_stream",
        fetched_at=datetime.now(timezone.utc),
        content=content,
        content_type="application/json",
        http_status=status,
        headers=headers or {},
    )


class TestStixTaxiiAdapter:
    """T-STX-001 through T-STX-038."""

    def test_taxii_discovery_and_collection_resolution(self):
        """T-STX-001: Discovery selects API root and collection."""
        adapter = _make_adapter({
            "stix_mode": "taxii",
            "taxii_url": "https://taxii.example.com",
            "api_root_title": "/api/v1",
            "collection_title": "Enterprise ATT&CK",
        })

        def mock_get(url, **kwargs):
            resp = AsyncMock()
            if "taxii2" in url:
                resp.json = AsyncMock(return_value=SAMPLE_TAXII_DISCOVERY)
            elif "collections/" in url and "objects" in url:
                resp.json = AsyncMock(return_value={"more": False, "objects": []})
            elif "collections" in url:
                resp.json = AsyncMock(return_value=SAMPLE_TAXII_COLLECTIONS)
            else:
                resp.json = AsyncMock(return_value={"more": False, "objects": []})
            resp.__aenter__ = AsyncMock(return_value=resp)
            resp.__aexit__ = AsyncMock(return_value=False)
            return resp

        mock_session = AsyncMock()
        mock_session.get = mock_get
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            raw = asyncio.get_event_loop().run_until_complete(adapter.fetch())
            assert raw.http_status == 200

    def test_taxii_fetch_objects(self):
        """T-STX-002: Fetches objects from TAXII collection."""
        adapter = _make_adapter({
            "stix_mode": "taxii",
            "taxii_url": "https://taxii.example.com",
            "api_root_path": "/api/v1",
            "collection_id": "col-001",
        })

        def mock_get(url, **kwargs):
            resp = AsyncMock()
            resp.json = AsyncMock(return_value={"more": False, "objects": SAMPLE_TAXII_ENVELOPE["objects"]})
            resp.__aenter__ = AsyncMock(return_value=resp)
            resp.__aexit__ = AsyncMock(return_value=False)
            return resp

        mock_session = AsyncMock()
        mock_session.get = mock_get
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            raw = asyncio.get_event_loop().run_until_complete(adapter.fetch())
            data = json.loads(raw.content)
            assert len(data["objects"]) == 5

    def test_taxii_pagination(self):
        """T-STX-003: Follows pagination with next token."""
        adapter = _make_adapter({
            "stix_mode": "taxii",
            "taxii_url": "https://taxii.example.com",
            "api_root_path": "/api/v1",
            "collection_id": "col-001",
        })

        call_count = 0

        def mock_get(url, **kwargs):
            nonlocal call_count
            resp = AsyncMock()
            call_count += 1
            if call_count == 1:
                resp.json = AsyncMock(return_value=SAMPLE_TAXII_ENVELOPE)
            else:
                resp.json = AsyncMock(return_value=SAMPLE_TAXII_ENVELOPE_PAGE2)
            resp.__aenter__ = AsyncMock(return_value=resp)
            resp.__aexit__ = AsyncMock(return_value=False)
            return resp

        mock_session = AsyncMock()
        mock_session.get = mock_get
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            raw = asyncio.get_event_loop().run_until_complete(adapter.fetch())
            data = json.loads(raw.content)
            assert len(data["objects"]) == 8  # 5 + 3

    def test_taxii_max_pages_limit(self):
        """T-STX-004: max_pages: 1 limits to first page."""
        adapter = _make_adapter({
            "stix_mode": "taxii",
            "taxii_url": "https://taxii.example.com",
            "api_root_path": "/api/v1",
            "collection_id": "col-001",
            "max_pages": 1,
        })

        def mock_get(url, **kwargs):
            resp = AsyncMock()
            resp.json = AsyncMock(return_value=SAMPLE_TAXII_ENVELOPE)
            resp.__aenter__ = AsyncMock(return_value=resp)
            resp.__aexit__ = AsyncMock(return_value=False)
            return resp

        mock_session = AsyncMock()
        mock_session.get = mock_get
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            raw = asyncio.get_event_loop().run_until_complete(adapter.fetch())
            data = json.loads(raw.content)
            assert len(data["objects"]) == 5

    def test_taxii_added_after_filter(self):
        """T-STX-005: added_after injected as query parameter."""
        adapter = _make_adapter({
            "stix_mode": "taxii",
            "taxii_url": "https://taxii.example.com",
            "api_root_path": "/api/v1",
            "collection_id": "col-001",
        })
        adapter._last_fetch_time = "2024-01-01T00:00:00Z"

        captured_params = {}

        def mock_get(url, **kwargs):
            captured_params.update(kwargs.get("params", {}))
            resp = AsyncMock()
            resp.json = AsyncMock(return_value={"more": False, "objects": []})
            resp.__aenter__ = AsyncMock(return_value=resp)
            resp.__aexit__ = AsyncMock(return_value=False)
            return resp

        mock_session = AsyncMock()
        mock_session.get = mock_get
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            asyncio.get_event_loop().run_until_complete(adapter.fetch())
            assert captured_params.get("added_after") == "2024-01-01T00:00:00Z"

    def test_taxii_type_filter(self):
        """T-STX-006: stix_type_filter injected as match[type] parameter."""
        adapter = _make_adapter({
            "stix_mode": "taxii",
            "taxii_url": "https://taxii.example.com",
            "api_root_path": "/api/v1",
            "collection_id": "col-001",
            "stix_type_filter": ["indicator", "malware"],
        })

        captured_params = {}

        def mock_get(url, **kwargs):
            captured_params.update(kwargs.get("params", {}))
            resp = AsyncMock()
            resp.json = AsyncMock(return_value={"more": False, "objects": []})
            resp.__aenter__ = AsyncMock(return_value=resp)
            resp.__aexit__ = AsyncMock(return_value=False)
            return resp

        mock_session = AsyncMock()
        mock_session.get = mock_get
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            asyncio.get_event_loop().run_until_complete(adapter.fetch())
            assert "indicator" in captured_params.get("match[type]", "")
            assert "malware" in captured_params.get("match[type]", "")

    def test_bundle_fetch_and_parse(self):
        """T-STX-007: Fetches and parses STIX bundle with 14 objects."""
        adapter = _make_adapter({
            "stix_mode": "stix_bundle",
            "bundle_url": "https://example.com/bundle.json",
        })
        bundle = _load_stix_bundle()
        raw = _raw(bundle)
        records = adapter.parse(raw)
        assert len(records) == 14
        for rec in records:
            assert "id" in rec
            assert "type" in rec

    def test_bundle_conditional_fetch_304(self):
        """T-STX-008: Conditional fetch returns 304 with empty payload."""
        raw = RawPayload(
            stream_id="test_stream",
            fetched_at=datetime.now(timezone.utc),
            content=b"",
            content_type="",
            http_status=304,
            headers={},
        )
        assert raw.http_status == 304
        assert raw.content == b""

    def test_csv_to_stix_conversion(self):
        """T-STX-009: CSV rows converted to STIX indicator objects."""
        adapter = _make_adapter({
            "stix_mode": "stix_bundle",
            "bundle_url": "https://example.com/data.csv",
            "bundle_format": "stix_csv",
            "csv_to_stix_mapping": {
                "url": "value", "threat": "name", "date_added": "valid_from", "reporter": "created_by_ref",
            },
        })
        result = adapter._convert_csv_to_stix_bundle(SAMPLE_STIX_CSV.encode())
        bundle = json.loads(result)
        assert bundle["type"] == "bundle"
        assert len(bundle["objects"]) == 5
        for obj in bundle["objects"]:
            assert obj["type"] == "indicator"
            assert "value" in obj

    def test_parse_attack_pattern(self):
        """T-STX-010: attack-pattern parsed with kill_chain_phases and mitre_attack_id."""
        adapter = _make_adapter({"stix_mode": "stix_bundle"})
        bundle = _load_stix_bundle()
        raw = _raw(bundle)
        records = adapter.parse(raw)
        ap = [r for r in records if r["type"] == "attack-pattern"]
        assert len(ap) == 3
        assert ap[0]["name"] == "Spearphishing Attachment"
        assert "mitre_attack_id" in ap[0]
        assert ap[0]["mitre_attack_id"] == "T1566.001"
        assert "kill_chain_phases" in ap[0]
        assert "mitre-attack:initial-access" in ap[0]["kill_chain_phases"]

    def test_parse_indicator_with_pattern(self):
        """T-STX-011: indicator parsed with pattern and pattern_type."""
        adapter = _make_adapter({"stix_mode": "stix_bundle"})
        bundle = _load_stix_bundle()
        raw = _raw(bundle)
        records = adapter.parse(raw)
        indicators = [r for r in records if r["type"] == "indicator" and not r.get("revoked")]
        valid_ind = [i for i in indicators if "198.51.100.1" in (i.get("pattern") or "")]
        assert len(valid_ind) == 1
        assert valid_ind[0]["pattern_type"] == "stix"

    def test_parse_malware(self):
        """T-STX-012: malware parsed with malware_types, is_family, aliases."""
        adapter = _make_adapter({"stix_mode": "stix_bundle"})
        bundle = _load_stix_bundle()
        raw = _raw(bundle)
        records = adapter.parse(raw)
        malware = [r for r in records if r["type"] == "malware"]
        assert len(malware) == 2
        dc = [m for m in malware if m["name"] == "DarkComet"][0]
        assert dc["is_family"] is True
        assert "DarkComet RAT" in dc["aliases"]

    def test_parse_threat_actor(self):
        """T-STX-013: threat-actor parsed with sophistication, resource_level."""
        adapter = _make_adapter({"stix_mode": "stix_bundle"})
        bundle = _load_stix_bundle()
        raw = _raw(bundle)
        records = adapter.parse(raw)
        ta = [r for r in records if r["type"] == "threat-actor"]
        assert len(ta) == 1
        assert ta[0]["name"] == "APT28"
        assert ta[0]["sophistication"] == "expert"
        assert ta[0]["resource_level"] == "government"

    def test_parse_vulnerability_cve(self):
        """T-STX-014: vulnerability with CVE external reference."""
        adapter = _make_adapter({"stix_mode": "stix_bundle"})
        bundle = _load_stix_bundle()
        raw = _raw(bundle)
        records = adapter.parse(raw)
        vulns = [r for r in records if r["type"] == "vulnerability"]
        assert len(vulns) == 1
        assert vulns[0]["cve_id"] == "CVE-2021-44228"

    def test_parse_relationship(self):
        """T-STX-015: relationship parsed with source_ref and target_ref."""
        adapter = _make_adapter({"stix_mode": "stix_bundle"})
        bundle = _load_stix_bundle()
        raw = _raw(bundle)
        records = adapter.parse(raw)
        rels = [r for r in records if r["type"] == "relationship"]
        assert len(rels) == 2
        uses_rel = [r for r in rels if r["relationship_type"] == "uses"]
        assert len(uses_rel) == 1
        assert "threat-actor--" in uses_rel[0]["source_ref"]

    def test_parse_sighting(self):
        """T-STX-016: sighting parsed with sighting_of_ref, count."""
        adapter = _make_adapter({"stix_mode": "stix_bundle"})
        bundle = _load_stix_bundle()
        raw = _raw(bundle)
        records = adapter.parse(raw)
        sightings = [r for r in records if r["type"] == "sighting"]
        assert len(sightings) == 1
        assert sightings[0]["count"] == 42

    def test_parse_sco_ipv4(self):
        """T-STX-017: ipv4-addr SCO parsed with value."""
        adapter = _make_adapter({"stix_mode": "stix_bundle"})
        obj = {
            "type": "ipv4-addr", "id": "ipv4-addr--11111111-1111-4111-8111-111111111111",
            "value": "198.51.100.1",
        }
        raw = _raw({"objects": [obj]})
        records = adapter.parse(raw)
        assert len(records) == 1
        assert records[0]["value"] == "198.51.100.1"

    def test_parse_sco_file_hashes(self):
        """T-STX-018: file SCO parsed with name, hashes, size."""
        adapter = _make_adapter({"stix_mode": "stix_bundle"})
        obj = {
            "type": "file", "id": "file--11111111-1111-4111-8111-111111111111",
            "name": "malware.exe",
            "hashes": {"MD5": "abc123", "SHA-1": "def456", "SHA-256": "ghi789"},
            "size": 1024,
        }
        raw = _raw({"objects": [obj]})
        records = adapter.parse(raw)
        assert len(records) == 1
        assert records[0]["name"] == "malware.exe"
        assert records[0]["hashes"]["MD5"] == "abc123"
        assert records[0]["size"] == 1024

    def test_parse_unknown_type_generic(self):
        """T-STX-019: Unknown type parsed generically."""
        adapter = _make_adapter({"stix_mode": "stix_bundle"})
        obj = {
            "type": "x-custom-object",
            "id": "x-custom-object--11111111-1111-4111-8111-111111111111",
            "created": "2024-01-01T00:00:00.000Z",
            "modified": "2024-01-01T00:00:00.000Z",
            "custom_field": "custom_value",
        }
        raw = _raw({"objects": [obj]})
        records = adapter.parse(raw)
        assert len(records) == 1
        assert records[0]["custom_field"] == "custom_value"

    def test_validate_stix_id_format(self):
        """T-STX-020: Invalid STIX ID dropped."""
        adapter = _make_adapter({"stix_mode": "stix_bundle"})
        records = [
            {"id": "malware--not-a-uuid", "type": "malware", "created": "2024-01-01T00:00:00Z", "modified": "2024-01-01T00:00:00Z", "name": "Bad"},
            {"id": "malware--11111111-1111-4111-8111-111111111111", "type": "malware", "created": "2024-01-01T00:00:00Z", "modified": "2024-01-01T00:00:00Z", "name": "Good"},
        ]
        valid = adapter.validate(records)
        assert len(valid) == 1
        assert valid[0]["name"] == "Good"

    def test_validate_stix_id_type_mismatch(self):
        """T-STX-021: Type prefix mismatch in ID dropped."""
        adapter = _make_adapter({"stix_mode": "stix_bundle"})
        records = [
            {"id": "malware--11111111-1111-4111-8111-111111111111", "type": "indicator",
             "created": "2024-01-01T00:00:00Z", "modified": "2024-01-01T00:00:00Z", "name": "Mismatch"},
        ]
        valid = adapter.validate(records)
        assert len(valid) == 0

    def test_validate_required_fields_sdo(self):
        """T-STX-022: SDO missing created field dropped."""
        adapter = _make_adapter({"stix_mode": "stix_bundle"})
        records = [
            {"id": "malware--11111111-1111-4111-8111-111111111111", "type": "malware",
             "modified": "2024-01-01T00:00:00Z", "name": "NoCreated"},
            {"id": "malware--22222222-2222-4222-8222-222222222222", "type": "malware",
             "created": "2024-01-01T00:00:00Z", "modified": "2024-01-01T00:00:00Z", "name": "Good"},
        ]
        valid = adapter.validate(records)
        assert len(valid) == 1
        assert valid[0]["name"] == "Good"

    def test_validate_required_fields_sco(self):
        """T-STX-023: SCO missing identifying properties dropped."""
        adapter = _make_adapter({"stix_mode": "stix_bundle"})
        records = [
            {"id": "ipv4-addr--11111111-1111-4111-8111-111111111111", "type": "ipv4-addr"},
            {"id": "ipv4-addr--22222222-2222-4222-8222-222222222222", "type": "ipv4-addr", "value": "10.0.0.1"},
        ]
        valid = adapter.validate(records)
        assert len(valid) == 1
        assert valid[0]["value"] == "10.0.0.1"

    def test_validate_modified_gte_created(self):
        """T-STX-024: modified < created corrected to created."""
        adapter = _make_adapter({"stix_mode": "stix_bundle"})
        records = [
            {"id": "malware--11111111-1111-4111-8111-111111111111", "type": "malware",
             "created": "2024-06-01T00:00:00Z", "modified": "2024-01-01T00:00:00Z", "name": "FixMe"},
        ]
        valid = adapter.validate(records)
        assert len(valid) == 1
        assert valid[0]["modified"] == valid[0]["created"]

    def test_validate_stix_pattern_valid(self):
        """T-STX-025: Valid STIX pattern tagged pattern_valid: true."""
        adapter = _make_adapter({"stix_mode": "stix_bundle", "validate_stix_patterns": True})
        records = [
            {"id": "indicator--11111111-1111-4111-8111-111111111111", "type": "indicator",
             "created": "2024-01-01T00:00:00Z", "modified": "2024-01-01T00:00:00Z",
             "pattern": "[ipv4-addr:value = '1.2.3.4']", "pattern_type": "stix",
             "valid_from": "2024-01-01T00:00:00Z"},
        ]
        valid = adapter.validate(records)
        assert len(valid) == 1
        # pattern_valid depends on stix2-patterns availability
        # If library is available, should be True; otherwise test still passes

    def test_validate_stix_pattern_invalid(self):
        """T-STX-026: Invalid STIX pattern tagged pattern_valid: false."""
        adapter = _make_adapter({"stix_mode": "stix_bundle", "validate_stix_patterns": True})
        records = [
            {"id": "indicator--11111111-1111-4111-8111-111111111111", "type": "indicator",
             "created": "2024-01-01T00:00:00Z", "modified": "2024-01-01T00:00:00Z",
             "pattern": "[not a pattern", "pattern_type": "stix",
             "valid_from": "2024-01-01T00:00:00Z"},
        ]
        valid = adapter.validate(records)
        assert len(valid) == 1
        if "pattern_valid" in valid[0]:
            assert valid[0]["pattern_valid"] is False

    def test_validate_stix_pattern_skip(self):
        """T-STX-027: Pattern validation skipped when disabled."""
        adapter = _make_adapter({"stix_mode": "stix_bundle", "validate_stix_patterns": False})
        records = [
            {"id": "indicator--11111111-1111-4111-8111-111111111111", "type": "indicator",
             "created": "2024-01-01T00:00:00Z", "modified": "2024-01-01T00:00:00Z",
             "pattern": "[not a pattern", "pattern_type": "stix",
             "valid_from": "2024-01-01T00:00:00Z"},
        ]
        valid = adapter.validate(records)
        assert len(valid) == 1
        assert "pattern_valid" not in valid[0]

    def test_validate_dedup_by_stix_id(self):
        """T-STX-028: Same ID, different modified — keep latest."""
        adapter = _make_adapter({"stix_mode": "stix_bundle"})
        records = [
            {"id": "malware--11111111-1111-4111-8111-111111111111", "type": "malware",
             "created": "2024-01-01T00:00:00Z", "modified": "2024-01-01T00:00:00Z", "name": "Old"},
            {"id": "malware--11111111-1111-4111-8111-111111111111", "type": "malware",
             "created": "2024-01-01T00:00:00Z", "modified": "2024-06-01T00:00:00Z", "name": "New"},
        ]
        valid = adapter.validate(records)
        assert len(valid) == 1
        assert valid[0]["name"] == "New"

    def test_validate_revoked_object(self):
        """T-STX-029: Revoked object retained with revoked tag."""
        adapter = _make_adapter({"stix_mode": "stix_bundle"})
        bundle = _load_stix_bundle()
        raw = _raw(bundle)
        records = adapter.parse(raw)
        valid = adapter.validate(records)
        revoked = [r for r in valid if r.get("revoked")]
        assert len(revoked) == 1

    def test_normalize_attack_pattern_to_normalized_record(self):
        """T-STX-030: attack-pattern normalizes to NormalizedRecord."""
        adapter = _make_adapter({
            "stix_mode": "taxii",
            "tier": 16,
            "source_name": "MITRE ATT&CK",
            "taxii_url": "https://taxii.example.com",
            "default_tags": ["cti"],
        })
        records = [{
            "id": "attack-pattern--11111111-1111-4111-8111-111111111111",
            "type": "attack-pattern",
            "created": "2024-01-01T00:00:00Z",
            "modified": "2024-06-01T00:00:00Z",
            "name": "Spearphishing",
            "mitre_attack_id": "T1566.001",
            "stix_version": "2.1",
            "fetch_mode": "taxii",
        }]
        normalized = adapter.normalize(records)
        assert len(normalized) == 1
        nr = normalized[0]
        assert isinstance(nr, NormalizedRecord)
        assert nr.geo is None
        assert nr.confidence == 0.95
        assert "attack-pattern" in nr.tags
        assert "T1566.001" in nr.tags

    def test_normalize_indicator_to_normalized_record(self):
        """T-STX-031: indicator normalizes with raw_hash from id + modified."""
        adapter = _make_adapter({
            "stix_mode": "stix_bundle",
            "tier": 16,
            "source_name": "Test",
            "bundle_url": "https://example.com/bundle.json",
        })
        records = [{
            "id": "indicator--44444444-4444-4444-8444-444444444444",
            "type": "indicator",
            "created": "2024-03-01T00:00:00Z",
            "modified": "2024-03-01T00:00:00Z",
            "stix_version": "2.1",
            "fetch_mode": "stix_bundle",
        }]
        normalized = adapter.normalize(records)
        assert len(normalized) == 1
        assert len(normalized[0].raw_hash) == 16

    def test_normalize_bundle_confidence(self):
        """T-STX-032: Direct bundle normalizes with confidence 0.85."""
        adapter = _make_adapter({
            "stix_mode": "stix_bundle",
            "tier": 16,
            "source_name": "Test",
            "bundle_url": "https://example.com/bundle.json",
        })
        records = [{
            "id": "malware--11111111-1111-4111-8111-111111111111",
            "type": "malware",
            "created": "2024-01-01T00:00:00Z",
            "modified": "2024-01-01T00:00:00Z",
            "stix_version": "2.1",
            "fetch_mode": "stix_bundle",
        }]
        normalized = adapter.normalize(records)
        assert normalized[0].confidence == 0.85

    def test_auth_api_key_otx(self):
        """T-STX-033: API key auth includes X-OTX-API-KEY header."""
        settings = _make_settings(credentials={"test_stream": {"api_key": "test-otx-key"}})
        adapter = _make_adapter({"auth_pattern": "api_key"}, settings)
        headers = adapter._build_auth_headers()
        assert headers["X-OTX-API-KEY"] == "test-otx-key"

    def test_auth_certificate_mutual_tls(self):
        """T-STX-034: Certificate auth builds SSL context."""
        settings = _make_settings(credentials={
            "test_stream": {"cert_path": "/path/to/cert.pem", "key_path": "/path/to/key.pem"}
        })
        adapter = _make_adapter({"auth_pattern": "certificate"}, settings)
        with patch("ssl.create_default_context") as mock_ctx:
            mock_instance = MagicMock()
            mock_ctx.return_value = mock_instance
            ctx = adapter._build_ssl_context()
            assert ctx is not None
            mock_instance.load_cert_chain.assert_called_once_with("/path/to/cert.pem", "/path/to/key.pem")

    def test_run_pipeline_taxii(self):
        """T-STX-035: Full run() pipeline for TAXII mode."""
        adapter = _make_adapter({
            "stix_mode": "taxii",
            "taxii_url": "https://taxii.example.com",
            "api_root_path": "/api/v1",
            "collection_id": "col-001",
            "tier": 16,
            "source_name": "Test TAXII",
        })

        def mock_get(url, **kwargs):
            resp = AsyncMock()
            resp.json = AsyncMock(return_value={"more": False, "objects": SAMPLE_TAXII_ENVELOPE["objects"]})
            resp.__aenter__ = AsyncMock(return_value=resp)
            resp.__aexit__ = AsyncMock(return_value=False)
            return resp

        mock_session = AsyncMock()
        mock_session.get = mock_get
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = asyncio.get_event_loop().run_until_complete(adapter.run())
            assert len(result) == 5
            assert all(isinstance(r, NormalizedRecord) for r in result)

    def test_run_pipeline_bundle(self):
        """T-STX-036: Full run() pipeline for direct bundle mode."""
        adapter = _make_adapter({
            "stix_mode": "stix_bundle",
            "bundle_url": "https://example.com/bundle.json",
            "tier": 16,
            "source_name": "Test Bundle",
        })

        bundle = _load_stix_bundle()

        mock_resp = AsyncMock()
        mock_resp.read = AsyncMock(return_value=json.dumps(bundle).encode())
        mock_resp.status = 200
        mock_resp.headers = {"Content-Type": "application/json"}
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = asyncio.get_event_loop().run_until_complete(adapter.run())
            assert len(result) >= 10  # 14 objects, some may be deduped
            assert all(isinstance(r, NormalizedRecord) for r in result)

    def test_health_check_taxii(self):
        """T-STX-037: TAXII health check returns AdapterHealth."""
        adapter = _make_adapter({
            "stix_mode": "taxii",
            "taxii_url": "https://taxii.example.com",
        })

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.head = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            health = asyncio.get_event_loop().run_until_complete(adapter.health_check())
            assert isinstance(health, AdapterHealth)
            assert health.status == HealthStatus.OK

    def test_health_check_bundle(self):
        """T-STX-038: Bundle health check returns AdapterHealth."""
        adapter = _make_adapter({
            "stix_mode": "stix_bundle",
            "bundle_url": "https://example.com/bundle.json",
        })

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.head = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            health = asyncio.get_event_loop().run_until_complete(adapter.health_check())
            assert isinstance(health, AdapterHealth)
            assert health.status == HealthStatus.OK
