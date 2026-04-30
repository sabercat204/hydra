"""Unit tests for BaseAdapter, RawPayload, AdapterHealth, and retry logic."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hydra.adapters.base import (
    AdapterHealth,
    BaseAdapter,
    HealthStatus,
    RawPayload,
    _resolve_dot_path,
)
from hydra.adapters.exceptions import (
    AdapterRegistryMismatch,
    FetchError,
    RateLimitError,
)
from hydra.config import HydraSettings
from hydra.models.normalized import NormalizedRecord, Tier
from hydra.registry.stream_registry import (
    StreamRegistry,
    StreamSource,
    StreamTier,
)
from hydra.utils.hashing import compute_raw_hash


# ---------------------------------------------------------------------------
# Helpers — minimal concrete subclass for testing
# ---------------------------------------------------------------------------


def _make_registry(adapter_type: str = "test_adapter") -> StreamRegistry:
    """Build a minimal StreamRegistry with one tier/source."""
    src = StreamSource(name="test_source", url="https://example.com", format="json", auth="none", notes="")
    tier = StreamTier(
        id=1,
        name="Test Tier",
        streams=1,
        access="5G",
        formats=["json"],
        cadence="daily",
        adapter=adapter_type,
        fallback=None,
        sources=[src],
    )
    return StreamRegistry(tiers={1: tier})


class ConcreteAdapter(BaseAdapter):
    """Minimal concrete adapter for testing."""

    adapter_type = "test_adapter"

    def __init__(
        self,
        stream_id: str = "test_source",
        settings: HydraSettings | None = None,
        registry: StreamRegistry | None = None,
        *,
        fetch_result: RawPayload | None = None,
        parse_result: list[dict[str, Any]] | None = None,
        validate_result: list[dict[str, Any]] | None = None,
        fetch_side_effect: Exception | None = None,
    ) -> None:
        self._fetch_result = fetch_result
        self._parse_result = parse_result or []
        self._validate_result = validate_result
        self._fetch_side_effect = fetch_side_effect
        super().__init__(
            stream_id=stream_id,
            settings=settings or HydraSettings(),
            registry=registry or _make_registry(),
        )

    async def fetch(self) -> RawPayload:
        if self._fetch_side_effect:
            raise self._fetch_side_effect
        if self._fetch_result:
            return self._fetch_result
        return RawPayload(
            stream_id=self.stream_id,
            fetched_at=datetime.now(timezone.utc),
            content=b'{"id":"1"}',
            content_type="application/json",
            http_status=200,
        )

    def parse(self, raw: RawPayload) -> list[dict[str, Any]]:
        return self._parse_result

    def validate(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if self._validate_result is not None:
            return self._validate_result
        return records


# ---------------------------------------------------------------------------
# Tests: BaseAdapter cannot be instantiated directly
# ---------------------------------------------------------------------------


class TestBaseAdapterAbstract:
    def test_cannot_instantiate_directly(self) -> None:
        with pytest.raises(TypeError):
            BaseAdapter(stream_id="x", settings=HydraSettings(), registry=_make_registry())  # type: ignore[abstract]

    def test_concrete_subclass_must_implement_all(self) -> None:
        class Incomplete(BaseAdapter):
            adapter_type = "test_adapter"

            async def fetch(self) -> RawPayload:  # type: ignore[override]
                ...

            # Missing parse and validate

        with pytest.raises(TypeError):
            Incomplete(stream_id="test_source", settings=HydraSettings(), registry=_make_registry())  # type: ignore[abstract]


# ---------------------------------------------------------------------------
# Tests: RawPayload
# ---------------------------------------------------------------------------


class TestRawPayload:
    def test_auto_hash(self) -> None:
        rp = RawPayload(
            stream_id="s1",
            fetched_at=datetime.now(timezone.utc),
            content=b"hello",
            content_type="text/plain",
            http_status=200,
        )
        assert rp.raw_hash == compute_raw_hash(b"hello")
        assert len(rp.raw_hash) == 16

    def test_empty_content_no_hash(self) -> None:
        rp = RawPayload(
            stream_id="s1",
            fetched_at=datetime.now(timezone.utc),
            content=b"",
            content_type="text/plain",
            http_status=304,
        )
        assert rp.raw_hash == ""


# ---------------------------------------------------------------------------
# Tests: normalize
# ---------------------------------------------------------------------------


class TestNormalize:
    def test_normalize_maps_fields(self) -> None:
        registry = _make_registry()
        adapter = ConcreteAdapter(
            parse_result=[{"id": "rec1", "quality": 0.85}],
            registry=registry,
        )
        # Inject field_mapping into stream_meta
        adapter._stream_meta["field_mapping"] = {
            "stream_id": "id",
            "confidence": "quality",
        }
        records = adapter.normalize([{"id": "rec1", "quality": 0.85}])
        assert len(records) == 1
        assert isinstance(records[0], NormalizedRecord)
        assert records[0].stream_id == "rec1"
        assert records[0].confidence == 0.85

    def test_normalize_returns_normalized_records(self) -> None:
        adapter = ConcreteAdapter()
        results = adapter.normalize([{"id": "a"}, {"id": "b"}])
        assert len(results) == 2
        for r in results:
            assert isinstance(r, NormalizedRecord)
            assert r.tier == Tier.GEOPHYSICAL_SEISMIC


# ---------------------------------------------------------------------------
# Tests: run pipeline
# ---------------------------------------------------------------------------


class TestRunPipeline:
    async def test_run_calls_in_order(self) -> None:
        raw = RawPayload(
            stream_id="test_source",
            fetched_at=datetime.now(timezone.utc),
            content=b'{"id":"1"}',
            content_type="application/json",
            http_status=200,
        )
        adapter = ConcreteAdapter(
            fetch_result=raw,
            parse_result=[{"id": "1"}],
        )
        results = await adapter.run()
        assert isinstance(results, list)
        assert all(isinstance(r, NormalizedRecord) for r in results)

    async def test_run_short_circuits_on_304(self) -> None:
        raw = RawPayload(
            stream_id="test_source",
            fetched_at=datetime.now(timezone.utc),
            content=b"",
            content_type="application/json",
            http_status=304,
        )
        adapter = ConcreteAdapter(fetch_result=raw)
        results = await adapter.run()
        assert results == []


# ---------------------------------------------------------------------------
# Tests: health_check
# ---------------------------------------------------------------------------


class TestHealthCheck:
    async def test_health_ok(self) -> None:
        adapter = ConcreteAdapter()
        health = await adapter.health_check()
        assert isinstance(health, AdapterHealth)
        assert health.status == HealthStatus.OK
        assert health.stream_id == "test_source"
        assert health.latency_ms >= 0

    async def test_health_unreachable(self) -> None:
        adapter = ConcreteAdapter(fetch_side_effect=FetchError("down", status_code=500))
        health = await adapter.health_check()
        assert health.status == HealthStatus.UNREACHABLE
        assert health.detail is not None


# ---------------------------------------------------------------------------
# Tests: retry policy
# ---------------------------------------------------------------------------


class TestRetryPolicy:
    async def test_retries_on_5xx(self) -> None:
        """Mock a sequence of 503 → 503 → 200 and verify 3 attempts."""
        call_count = 0
        ok_payload = RawPayload(
            stream_id="test_source",
            fetched_at=datetime.now(timezone.utc),
            content=b'{"ok":true}',
            content_type="application/json",
            http_status=200,
        )

        class RetryAdapter(ConcreteAdapter):
            async def fetch(self) -> RawPayload:
                nonlocal call_count
                call_count += 1
                if call_count < 3:
                    raise FetchError(f"503 attempt {call_count}", status_code=503)
                return ok_payload

        adapter = RetryAdapter()
        with patch("hydra.adapters.base.asyncio.sleep", new_callable=AsyncMock):
            result = await adapter._fetch_with_retry()
        assert result.http_status == 200
        assert call_count == 3

    async def test_rate_limit_honors_retry_after(self) -> None:
        """Mock 429 with Retry-After, verify adapter waits."""
        call_count = 0
        ok_payload = RawPayload(
            stream_id="test_source",
            fetched_at=datetime.now(timezone.utc),
            content=b'{"ok":true}',
            content_type="application/json",
            http_status=200,
        )

        class RLAdapter(ConcreteAdapter):
            async def fetch(self) -> RawPayload:
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise RateLimitError("429", retry_after=5.0)
                return ok_payload

        adapter = RLAdapter()
        sleep_mock = AsyncMock()
        with patch("hydra.adapters.base.asyncio.sleep", sleep_mock):
            result = await adapter._fetch_with_retry()
        assert result.http_status == 200
        # Verify sleep was called with the retry_after value
        sleep_mock.assert_awaited()
        assert sleep_mock.call_args_list[0].args[0] == 5.0

    async def test_no_retry_on_4xx(self) -> None:
        """Client errors (except 429) should not be retried."""
        adapter = ConcreteAdapter(fetch_side_effect=FetchError("404", status_code=404))
        with pytest.raises(FetchError):
            await adapter._fetch_with_retry()


# ---------------------------------------------------------------------------
# Tests: AdapterRegistryMismatch
# ---------------------------------------------------------------------------


class TestRegistryMismatch:
    def test_mismatch_raises(self) -> None:
        registry = _make_registry(adapter_type="fdsn")

        class WrongAdapter(ConcreteAdapter):
            adapter_type = "rest_json"

        with pytest.raises(AdapterRegistryMismatch):
            WrongAdapter(stream_id="test_source", settings=HydraSettings(), registry=registry)


# ---------------------------------------------------------------------------
# Tests: helper
# ---------------------------------------------------------------------------


class TestResolveDotPath:
    def test_simple(self) -> None:
        assert _resolve_dot_path({"a": {"b": 1}}, "a.b") == 1

    def test_missing(self) -> None:
        assert _resolve_dot_path({"a": 1}, "b.c") is None
