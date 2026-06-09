"""Adversary doctrine feed — curated stream from Tier 105."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sloptropy_common import AccessPolicy, is_auto_ingestable

from hydra.mil_int.dependencies import get_mil_int_settings, get_stream_registry
from hydra.mil_int.schemas.manifest import ManifestEntry
from hydra.mil_int.settings import MilIntSettings
from hydra.registry.stream_registry import StreamRegistry


router = APIRouter(prefix="/api/v1/mil-int/doctrine", tags=["mil-int"])


_DOCTRINE_TIERS = (105,)


@router.get("/sources", response_model=list[ManifestEntry])
def doctrine_sources(
    include_archived: bool = Query(default=True),
    registry: StreamRegistry = Depends(get_stream_registry),
    settings: MilIntSettings = Depends(get_mil_int_settings),
) -> list[ManifestEntry]:
    """Return the curated adversary-doctrine source set (Tier 105)."""
    del settings  # unused; reserved for future per-feed filtering
    out: list[ManifestEntry] = []
    for tid in _DOCTRINE_TIERS:
        tier = registry.get_tier(tid)
        if tier is None:
            continue
        for src in tier.sources:
            policy = AccessPolicy(src.access_policy)
            if not include_archived and policy == AccessPolicy.ARCHIVED:
                continue
            out.append(
                ManifestEntry(
                    tier=tid,
                    tier_name=tier.name,
                    source_name=src.name,
                    url=src.url,
                    format=src.format,
                    notes=src.notes,
                    access_policy=policy,
                    ingestable=is_auto_ingestable(policy),
                )
            )
    return out
