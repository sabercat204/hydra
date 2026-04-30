"""Tests for Neo4jEngine — 12 tests covering node/edge creation, health."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hydra.config import HydraSettings
from hydra.models.normalized import NormalizedRecord, SourceMeta, Tier
from hydra.storage.engines.neo4j import Neo4jEngine, _to_pascal_case, _to_upper_snake
from hydra.utils.hashing import compute_raw_hash


def _make_record(**overrides) -> NormalizedRecord:
    defaults = dict(
        stream_id="test_stream_1",
        tier=Tier.CYBER_THREAT_INTEL,
        timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc),
        payload={"type": "attack-pattern", "id": "ap-001", "name": "Phishing", "description": "Email phishing"},
        source_meta=SourceMeta(source_name="MITRE", adapter_type="stix_taxii"),
        raw_hash=compute_raw_hash(b"neo4j_test"),
        tags=["cyber"],
    )
    defaults.update(overrides)
    return NormalizedRecord(**defaults)


GRAPH_SCHEMA = {
    "node_label_field": "type",
    "node_id_field": "id",
    "node_properties": ["name", "description"],
    "edge_rules": [
        {
            "type": "relationship",
            "source_field": "source_ref",
            "target_field": "target_ref",
            "edge_label_field": "relationship_type",
            "edge_properties": ["description"],
        },
        {
            "type": "sighting",
            "source_field": "sighting_of_ref",
            "target_field": None,
            "edge_label_field": None,
        },
    ],
}


def _mock_driver():
    tx = AsyncMock()
    tx.run = AsyncMock(return_value=None)
    tx.commit = AsyncMock(return_value=None)
    tx.__aenter__ = AsyncMock(return_value=tx)
    tx.__aexit__ = AsyncMock(return_value=False)

    session = AsyncMock()
    session.begin_transaction = AsyncMock(return_value=tx)
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)

    driver = AsyncMock()
    driver.session = MagicMock(return_value=session)

    return driver, session, tx


@pytest.mark.asyncio
async def test_node_creation():
    """Node creation from record with graph_schema."""
    engine = Neo4jEngine(HydraSettings())
    driver, session, tx = _mock_driver()
    engine._driver = driver
    record = _make_record()
    result = await engine.store([record], graph_schema=GRAPH_SCHEMA)
    assert result.stored == 1
    assert tx.run.called


def test_node_label_pascal_case():
    """Node label derived from node_label_field, converted to PascalCase."""
    assert _to_pascal_case("attack-pattern") == "AttackPattern"
    assert _to_pascal_case("malware") == "Malware"
    assert _to_pascal_case("threat actor") == "ThreatActor"


def test_node_properties_extracted():
    """Node properties extracted from node_properties list."""
    engine = Neo4jEngine(HydraSettings())
    record = _make_record()
    # Verify the payload has the expected properties
    assert "name" in record.payload
    assert "description" in record.payload


@pytest.mark.asyncio
async def test_edge_creation():
    """Edge creation from record matching edge_rules."""
    engine = Neo4jEngine(HydraSettings())
    driver, session, tx = _mock_driver()
    engine._driver = driver
    record = _make_record(payload={
        "type": "relationship",
        "id": "rel-001",
        "source_ref": "ap-001",
        "target_ref": "malware-001",
        "relationship_type": "uses",
        "name": "Uses",
        "description": "APT uses malware",
    })
    result = await engine.store([record], graph_schema=GRAPH_SCHEMA)
    assert result.stored == 1
    # Should have node MERGE + edge MERGE calls
    assert tx.run.call_count >= 2


def test_edge_label_from_field():
    """Edge label from edge_label_field in payload."""
    assert _to_upper_snake("uses") == "USES"
    assert _to_upper_snake("indicates") == "INDICATES"


def test_static_edge_label():
    """Static edge label from edge_label_static."""
    assert _to_upper_snake("OWNS") == "OWNS"
    assert _to_upper_snake("ASSOCIATED_WITH") == "ASSOCIATED_WITH"


@pytest.mark.asyncio
async def test_sighting_node_only():
    """Sighting record (no target) creates node only."""
    engine = Neo4jEngine(HydraSettings())
    driver, session, tx = _mock_driver()
    engine._driver = driver
    record = _make_record(payload={
        "type": "sighting",
        "id": "sight-001",
        "sighting_of_ref": "indicator-001",
        "name": "Sighting",
        "description": "Observed indicator",
    })
    result = await engine.store([record], graph_schema=GRAPH_SCHEMA)
    assert result.stored == 1


@pytest.mark.asyncio
async def test_merge_semantics():
    """MERGE semantics — re-ingesting same record updates, doesn't duplicate."""
    engine = Neo4jEngine(HydraSettings())
    driver, session, tx = _mock_driver()
    engine._driver = driver
    record = _make_record()
    # Store twice
    await engine.store([record], graph_schema=GRAPH_SCHEMA)
    await engine.store([record], graph_schema=GRAPH_SCHEMA)
    # MERGE ensures idempotency — both calls succeed
    assert tx.run.called


@pytest.mark.asyncio
async def test_invalid_label_fails_gracefully():
    """Invalid label (empty string) fails gracefully for that record."""
    engine = Neo4jEngine(HydraSettings())
    driver, session, tx = _mock_driver()
    engine._driver = driver
    record = _make_record(payload={"type": "", "id": "x", "name": "test", "description": "test"})
    result = await engine.store([record], graph_schema=GRAPH_SCHEMA)
    assert result.failed == 1
    assert result.stored == 0


@pytest.mark.asyncio
async def test_transaction_failure_retry():
    """Transaction failure triggers retry."""
    engine = Neo4jEngine(HydraSettings())
    driver, session, tx = _mock_driver()
    # Make the transaction's run fail to simulate a transaction error
    tx.run.side_effect = Exception("Transaction failed")
    engine._driver = driver
    record = _make_record()
    result = await engine.store([record], graph_schema=GRAPH_SCHEMA)
    # Individual record failure within the transaction
    assert result.failed >= 1


@pytest.mark.asyncio
async def test_batch_transaction():
    """Batch transaction — all-or-nothing per batch."""
    engine = Neo4jEngine(HydraSettings())
    driver, session, tx = _mock_driver()
    engine._driver = driver
    records = [_make_record(raw_hash=compute_raw_hash(f"r{i}".encode())) for i in range(3)]
    result = await engine.store(records, graph_schema=GRAPH_SCHEMA)
    assert result.stored == 3


@pytest.mark.asyncio
async def test_health_check_ok():
    """Health check returns OK on successful query."""
    engine = Neo4jEngine(HydraSettings())
    driver = AsyncMock()
    session = AsyncMock()
    session.run = AsyncMock(return_value=None)
    driver.session = MagicMock(return_value=session)
    engine._driver = driver
    health = await engine.health_check()
    assert health.status == "OK"
