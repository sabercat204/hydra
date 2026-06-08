"""Smoke tests for the mil_int FastAPI routers.

Mounts only the mil_int routers on a bare FastAPI app — avoids the heavy
``create_app()`` dependency graph (asyncpg, redis, monitoring) that isn't
needed for surface-level routing.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from hydra.mil_int.dependencies import set_mil_int_components
from hydra.mil_int.setup import mount_mil_int_routers
from hydra.mil_int.xref.resolver import XrefResolver


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    mount_mil_int_routers(app)
    set_mil_int_components(
        reset=True,
        xref_resolver=XrefResolver.from_path("config/mil_int_xref.yaml"),
    )
    return TestClient(app)


class TestManifestRouter:
    def test_returns_all_mil_int_sources(self, client: TestClient):
        resp = client.get("/api/v1/mil-int/manifest")
        assert resp.status_code == 200
        body = resp.json()
        assert body["surface"] == "mil_int_public_information"
        assert body["total_sources"] >= 50
        assert body["ingestable_sources"] < body["total_sources"]
        # Each entry has the expected shape.
        first = body["entries"][0]
        for key in ("tier", "tier_name", "source_name", "url", "access_policy", "ingestable"):
            assert key in first

    def test_includes_subscription_and_archived(self, client: TestClient):
        resp = client.get("/api/v1/mil-int/manifest")
        assert resp.status_code == 200
        names = {e["source_name"]: e for e in resp.json()["entries"]}
        assert names["South Korea KIDA/KJDA"]["access_policy"] == "subscription"
        assert names["CAST Moscow Defense Brief"]["access_policy"] == "archived"
        assert names["China CNKI"]["access_policy"] == "monitor_only"


class TestStandardsRouter:
    def test_xref_returns_seeded_mapping(self, client: TestClient):
        resp = client.get(
            "/api/v1/mil-int/standards/xref",
            params={"from_id": "MIL-STD-461"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["from_id"] == "MIL-STD-461"
        assert body["total"] >= 1
        targets = {m["to_id"] for m in body["mappings"]}
        assert "NIST SP 800-53" in targets

    def test_xref_to_family_filter(self, client: TestClient):
        resp = client.get(
            "/api/v1/mil-int/standards/xref",
            params={"from_id": "FIPS 140-3", "to_family": "NIST_SP_800"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert all(m["to_family"] == "NIST_SP_800" for m in body["mappings"])

    def test_families_endpoint(self, client: TestClient):
        resp = client.get("/api/v1/mil-int/standards/families")
        assert resp.status_code == 200
        families = resp.json()
        assert "MIL_STD" in families
        assert "STANAG" in families


class TestDoctrineRouter:
    def test_returns_tier_105_sources(self, client: TestClient):
        resp = client.get("/api/v1/mil-int/doctrine/sources")
        assert resp.status_code == 200
        entries = resp.json()
        names = {e["source_name"] for e in entries}
        assert "Russia MoD English" in names
        assert "Russia Matters Harvard" in names

    def test_exclude_archived(self, client: TestClient):
        resp = client.get(
            "/api/v1/mil-int/doctrine/sources",
            params={"include_archived": "false"},
        )
        assert resp.status_code == 200
        names = {e["source_name"] for e in resp.json()}
        assert "CAST Moscow Defense Brief" not in names


class TestComplianceRouter:
    def test_returns_compliance_overlay(self, client: TestClient):
        resp = client.get("/api/v1/mil-int/compliance/sources")
        assert resp.status_code == 200
        names = {e["source_name"] for e in resp.json()}
        assert "DISA STIG Library" in names
        assert "NIST SP 800 Series" in names
        assert "NSA Public Guidance" in names


class TestSearchRouter:
    def test_returns_503_without_backend(self, client: TestClient):
        # No backend wired by default — confirm the dependency surface
        # uniformly returns 503.
        resp = client.post(
            "/api/v1/mil-int/search",
            json={"q": "cryptographic module"},
        )
        assert resp.status_code == 503
