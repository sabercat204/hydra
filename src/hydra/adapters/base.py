"""Abstract base adapter and shared dataclasses for all HYDRA ingestion adapters."""

from __future__ import annotations

import abc
import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import structlog

from hydra.config import HydraSettings
from hydra.models.normalized import NormalizedRecord, SourceMeta, Tier
from hydra.registry.stream_registry import StreamRegistry, get_registry
from hydra.utils.hashing import compute_raw_hash

from .exceptions import (
    AdapterRegistryMismatch,
    FetchError,
    RateLimitError,
)

logger = structlog.get_logger()


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class RawPayload:
    """Raw response from an upstream source."""

    stream_id: str
    fetched_at: datetime
    content: bytes
    content_type: str
    http_status: int
    headers: dict[str, str] = field(default_factory=dict)
    raw_hash: str = ""

    def __post_init__(self) -> None:
        if not self.raw_hash and self.content:
            self.raw_hash = compute_raw_hash(self.content)


class HealthStatus(str, Enum):
    OK = "OK"
    DEGRADED = "DEGRADED"
    UNREACHABLE = "UNREACHABLE"


@dataclass
class AdapterHealth:
    """Result of a lightweight upstream health probe."""

    stream_id: str
    status: HealthStatus
    latency_ms: float
    last_checked: datetime
    detail: str | None = None


# ---------------------------------------------------------------------------
# Retry constants
# ---------------------------------------------------------------------------

_RETRY_INITIAL = 1.0
_RETRY_FACTOR = 2.0
_RETRY_MAX_ATTEMPTS = 5
_RETRY_CEILING = 60.0


# ---------------------------------------------------------------------------
# Base adapter
# ---------------------------------------------------------------------------


class BaseAdapter(abc.ABC):
    """Abstract base class for all HYDRA ingestion adapters.

    Subclasses must implement ``fetch``, ``parse``, and ``validate``.
    """

    adapter_type: str = ""  # subclasses override

    def __init__(self, stream_id: str, settings: HydraSettings, registry: StreamRegistry | None = None) -> None:
        self.stream_id = stream_id
        self.settings = settings
        self._registry = registry or get_registry()
        self._stream_meta = self._resolve_stream_meta()
        self._log = logger.bind(stream_id=stream_id, adapter_type=self.adapter_type)

    # -- registry helpers ---------------------------------------------------

    def _resolve_stream_meta(self) -> dict[str, Any]:
        """Find the stream entry in the registry and validate adapter type."""
        for tier in self._registry.tiers.values():
            for src in tier.sources:
                if src.name == self.stream_id or self.stream_id.startswith(src.name.lower().replace(" ", "_")):
                    # Validate adapter type matches
                    if self.adapter_type and tier.adapter != self.adapter_type and (
                        tier.fallback is None or tier.fallback != self.adapter_type
                    ):
                        raise AdapterRegistryMismatch(
                            f"Stream '{self.stream_id}' declares adapter '{tier.adapter}' "
                            f"(fallback: {tier.fallback}), but this adapter is '{self.adapter_type}'"
                        )
                    return {
                        "tier_id": tier.id,
                        "tier_name": tier.name,
                        "cadence": tier.cadence,
                        "adapter": tier.adapter,
                        "fallback": tier.fallback,
                        "source": src,
                    }
        # If no exact match found, store minimal meta — allows config-driven usage
        return {}

    @property
    def tier_id(self) -> int | None:
        return self._stream_meta.get("tier_id")

    # -- abstract methods ---------------------------------------------------

    @abc.abstractmethod
    async def fetch(self) -> RawPayload:
        """Retrieve raw data from the upstream source."""

    @abc.abstractmethod
    def parse(self, raw: RawPayload) -> list[dict[str, Any]]:
        """Transform raw bytes into a list of dicts (one per record)."""

    @abc.abstractmethod
    def validate(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Apply stream-specific validation. Return only valid records."""

    # -- concrete methods ---------------------------------------------------

    def normalize(self, records: list[dict[str, Any]]) -> list[NormalizedRecord]:
        """Map validated dicts to NormalizedRecord using field_mapping from registry."""
        field_mapping: dict[str, str] = self._stream_meta.get("field_mapping", {})
        tier_id = self.tier_id or 1
        source = self._stream_meta.get("source")
        source_name = source.name if source else self.stream_id
        source_url = source.url if source else ""

        normalized: list[NormalizedRecord] = []
        for rec in records:
            raw_bytes = _dict_to_bytes(rec)
            raw_hash = compute_raw_hash(raw_bytes)

            mapped: dict[str, Any] = {}
            for norm_field, src_path in field_mapping.items():
                mapped[norm_field] = _resolve_dot_path(rec, src_path)

            record = NormalizedRecord(
                stream_id=mapped.get("stream_id", f"{self.stream_id}_{rec.get('id', '')}"),
                tier=Tier(tier_id),
                timestamp=mapped.get("timestamp", datetime.now(timezone.utc)),
                geo=mapped.get("geo"),
                payload=rec,
                source_meta=SourceMeta(
                    source_name=source_name,
                    source_url=source_url,
                    adapter_type=self.adapter_type,
                ),
                raw_hash=raw_hash,
                confidence=float(mapped.get("confidence", 1.0)),
                tags=mapped.get("tags", []),
            )
            normalized.append(record)
        return normalized

    async def run(self) -> list[NormalizedRecord]:
        """Orchestrate the full fetch → parse → validate → normalize pipeline."""
        start = time.monotonic()
        self._log.info("run_start")

        raw = await self._fetch_with_retry()

        # Short-circuit on 304 Not Modified
        if raw.http_status == 304:
            self._log.debug("run_complete", record_count=0, duration_ms=_elapsed(start), reason="304_not_modified")
            return []

        records = self.parse(raw)
        self._log.info("parse_complete", record_count=len(records))

        valid = self.validate(records)
        self._log.info("validate_complete", record_count=len(valid))

        normalized = self.normalize(valid)
        self._log.info("run_complete", record_count=len(normalized), duration_ms=_elapsed(start))
        return normalized

    async def health_check(self) -> AdapterHealth:
        """Lightweight probe of the upstream endpoint."""
        start = time.monotonic()
        try:
            raw = await self.fetch()
            latency = _elapsed(start)
            status = HealthStatus.OK if raw.http_status < 400 else HealthStatus.DEGRADED
            return AdapterHealth(
                stream_id=self.stream_id,
                status=status,
                latency_ms=latency,
                last_checked=datetime.now(timezone.utc),
            )
        except Exception as exc:
            return AdapterHealth(
                stream_id=self.stream_id,
                status=HealthStatus.UNREACHABLE,
                latency_ms=_elapsed(start),
                last_checked=datetime.now(timezone.utc),
                detail=str(exc),
            )

    # -- retry wrapper ------------------------------------------------------

    async def _fetch_with_retry(self) -> RawPayload:
        """Wrap ``fetch`` with exponential backoff retry logic."""
        delay = _RETRY_INITIAL
        last_exc: Exception | None = None

        for attempt in range(1, _RETRY_MAX_ATTEMPTS + 1):
            try:
                return await self.fetch()
            except RateLimitError as exc:
                last_exc = exc
                wait = max(exc.retry_after, 1.0)
                self._log.info("retry_rate_limit", attempt=attempt, wait_seconds=wait)
                await asyncio.sleep(wait)
            except FetchError as exc:
                last_exc = exc
                if exc.status_code and 400 <= exc.status_code < 500 and exc.status_code != 429:
                    raise  # client errors (except 429) are not retryable
                self._log.info("retry_fetch", attempt=attempt, delay_seconds=delay)
                await asyncio.sleep(delay)
                delay = min(delay * _RETRY_FACTOR, _RETRY_CEILING)

        raise last_exc or FetchError("Max retries exceeded")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _elapsed(start: float) -> float:
    return round((time.monotonic() - start) * 1000, 2)


def _dict_to_bytes(d: dict[str, Any]) -> bytes:
    import orjson
    return orjson.dumps(d, option=orjson.OPT_SORT_KEYS)


def _resolve_dot_path(data: dict[str, Any], path: str) -> Any:
    """Traverse a dot-delimited path into a nested dict."""
    parts = path.split(".")
    current: Any = data
    for part in parts:
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current
