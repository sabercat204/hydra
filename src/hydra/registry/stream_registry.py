"""Stream registry loader — typed access to stream_registry.yaml."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import yaml

from sloptropy_common import AccessPolicy


@dataclass(frozen=True)
class StreamSource:
    """A single data source within a tier.

    The optional ``access_policy`` field annotates whether the source is
    safely ingestable. Defaults to ``"open"`` for backward compatibility
    with tiers 1-29 whose source lines pre-date the field.
    """

    name: str
    url: str
    format: str
    auth: str
    notes: str
    access_policy: str = "open"


@dataclass(frozen=True)
class StreamTier:
    """A thematic data tier with its sources."""

    id: int
    name: str
    streams: int
    access: str
    formats: List[str]
    cadence: str
    adapter: str
    fallback: Optional[str]
    sources: List[StreamSource]
    storage: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class StreamRegistry:
    """Complete parsed registry with lookup helpers."""

    tiers: Dict[int, StreamTier] = field(default_factory=dict)
    adapters: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    auth_patterns: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    storage_map: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    scheduler_cadences: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    correlation_pipelines: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def get_tier(self, tier_id: int) -> Optional[StreamTier]:
        """Return a tier by ID, or None."""
        return self.tiers.get(tier_id)

    def get_tiers_by_adapter(self, adapter_type: str) -> List[StreamTier]:
        """Return all tiers using the given primary adapter."""
        return [t for t in self.tiers.values() if t.adapter == adapter_type]

    def get_tiers_by_cadence(self, cadence: str) -> List[StreamTier]:
        """Return all tiers matching the given cadence."""
        return [t for t in self.tiers.values() if t.cadence == cadence]

    def get_all_sources(self) -> List[Tuple[int, StreamSource]]:
        """Return (tier_id, source) pairs for every source across all tiers."""
        result: List[Tuple[int, StreamSource]] = []
        for tier in self.tiers.values():
            for src in tier.sources:
                result.append((tier.id, src))
        return result

    def get_sources_by_access_policy(
        self, policy: str
    ) -> List[Tuple[int, StreamSource]]:
        """Return all (tier_id, source) pairs whose access_policy matches."""
        return [
            (tid, src)
            for tid, src in self.get_all_sources()
            if src.access_policy == policy
        ]


_VALID_ACCESS_POLICIES: frozenset[str] = frozenset(p.value for p in AccessPolicy)


def _coerce_access_policy(raw: str) -> str:
    """Validate ``raw`` against :class:`AccessPolicy`; fall back to ``open``."""
    return raw if raw in _VALID_ACCESS_POLICIES else AccessPolicy.OPEN.value


def _parse_source(raw: Any) -> StreamSource:
    """Parse a source from pipe-delimited string or dict.

    Pipe-delimited form is ``name|url|format|auth|notes`` with an optional
    trailing ``|access_policy`` (used by mil_int tiers 100-107). Missing
    access_policy defaults to ``open``.
    """
    if isinstance(raw, str):
        parts = raw.split("|")
        while len(parts) < 5:
            parts.append("")
        access_policy_raw = (
            parts[5].strip() if len(parts) > 5 and parts[5].strip() else AccessPolicy.OPEN.value
        )
        access_policy = _coerce_access_policy(access_policy_raw)
        return StreamSource(
            name=parts[0].strip(),
            url=parts[1].strip(),
            format=parts[2].strip(),
            auth=parts[3].strip(),
            notes=parts[4].strip(),
            access_policy=access_policy,
        )
    if isinstance(raw, dict):
        access_policy = _coerce_access_policy(
            str(raw.get("access_policy", AccessPolicy.OPEN.value))
        )
        return StreamSource(
            name=raw.get("name", ""),
            url=raw.get("url", ""),
            format=raw.get("format", ""),
            auth=raw.get("auth", ""),
            notes=raw.get("notes", ""),
            access_policy=access_policy,
        )
    raise ValueError(f"Cannot parse source: {raw!r}")


def _parse_tier(raw: Dict[str, Any]) -> StreamTier:
    """Parse a tier dict from YAML."""
    formats_raw = raw.get("formats", "")
    if isinstance(formats_raw, str):
        formats = [f.strip() for f in formats_raw.split(",") if f.strip()]
    else:
        formats = list(formats_raw)

    sources_raw = raw.get("sources", [])
    sources = [_parse_source(s) for s in sources_raw]

    fallback = raw.get("fallback")
    if fallback is None or fallback == "null":
        fallback = None

    return StreamTier(
        id=raw["id"],
        name=raw["name"],
        streams=raw.get("streams", len(sources)),
        access=raw.get("access", ""),
        formats=formats,
        cadence=raw.get("cadence", ""),
        adapter=raw.get("adapter", ""),
        fallback=fallback,
        sources=sources,
        storage=raw.get("storage"),
    )


def load_registry(path: Union[Path, str]) -> StreamRegistry:
    """Load stream_registry.yaml and return a typed StreamRegistry."""
    path = Path(path)
    with open(path) as f:
        data = yaml.safe_load(f)

    tiers: Dict[int, StreamTier] = {}
    for raw_tier in data.get("tiers", []):
        tier = _parse_tier(raw_tier)
        tiers[tier.id] = tier

    adapters = {a["id"]: a for a in data.get("adapters", [])}
    auth_patterns = {a["id"]: a for a in data.get("auth_patterns", [])}
    storage_map = {s["class"]: s for s in data.get("storage", [])}
    scheduler_cadences = {s["cadence"]: s for s in data.get("scheduler", [])}
    correlation_pipelines = {p["name"]: p for p in data.get("correlation_pipelines", [])}

    return StreamRegistry(
        tiers=tiers,
        adapters=adapters,
        auth_patterns=auth_patterns,
        storage_map=storage_map,
        scheduler_cadences=scheduler_cadences,
        correlation_pipelines=correlation_pipelines,
    )


_registry: Optional[StreamRegistry] = None


def get_registry() -> StreamRegistry:
    """Lazy singleton for the stream registry."""
    global _registry
    if _registry is None:
        from hydra.config import settings
        _registry = load_registry(settings.stream_registry_path)
    return _registry
