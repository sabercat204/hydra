"""Manifest router — list every registered mil_int source with its access policy."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from hydra.mil_int.dependencies import get_mil_int_settings, get_stream_registry
from hydra.mil_int.schemas.manifest import ManifestEntry, ManifestResponse
from hydra.mil_int.settings import MilIntSettings
from hydra.registry.stream_registry import StreamRegistry


router = APIRouter(prefix="/api/v1/mil-int", tags=["mil-int"])


_INGESTABLE_POLICIES = {"open", "registration"}


@router.get("/manifest", response_model=ManifestResponse)
def get_manifest(
    settings: MilIntSettings = Depends(get_mil_int_settings),
    registry: StreamRegistry = Depends(get_stream_registry),
) -> ManifestResponse:
    """Return every source registered under tiers 100-107.

    The manifest exposes non-ingestable sources too (subscription /
    restricted / archived / monitor_only) so operators can see the full
    landscape and plan manual provisioning.
    """
    entries: list[ManifestEntry] = []
    ingestable = 0
    for tid in sorted(settings.source_tiers):
        tier = registry.get_tier(tid)
        if tier is None:
            continue
        for src in tier.sources:
            policy = src.access_policy
            is_ingestable = policy in _INGESTABLE_POLICIES
            if is_ingestable:
                ingestable += 1
            entries.append(
                ManifestEntry(
                    tier=tid,
                    tier_name=tier.name,
                    source_name=src.name,
                    url=src.url,
                    format=src.format,
                    notes=src.notes,
                    access_policy=policy,  # type: ignore[arg-type]
                    ingestable=is_ingestable,
                )
            )
    return ManifestResponse(
        total_sources=len(entries),
        ingestable_sources=ingestable,
        entries=entries,
    )
