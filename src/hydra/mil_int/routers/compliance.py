"""Cybersecurity compliance overlay — STIG + NIST SP 800 + NSA CSI sources."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from hydra.mil_int.dependencies import get_mil_int_settings, get_stream_registry
from hydra.mil_int.schemas.manifest import ManifestEntry
from hydra.mil_int.settings import MilIntSettings
from hydra.registry.stream_registry import StreamRegistry


router = APIRouter(prefix="/api/v1/mil-int/compliance", tags=["mil-int"])


_COMPLIANCE_KEYWORDS = {
    "STIG": ("stig",),
    "NIST_SP_800": ("nist sp 800", "nist sp800", "csrc"),
    "NSA_CSI": ("nsa", "csi"),
}


def _matches_family(source_name: str, notes: str, family: str) -> bool:
    haystack = f"{source_name} {notes}".lower()
    return any(kw in haystack for kw in _COMPLIANCE_KEYWORDS.get(family, ()))


@router.get("/sources", response_model=list[ManifestEntry])
def compliance_sources(
    settings: MilIntSettings = Depends(get_mil_int_settings),
    registry: StreamRegistry = Depends(get_stream_registry),
) -> list[ManifestEntry]:
    """Return mil_int sources that contribute to the compliance overlay."""
    families = settings.compliance_families
    out: list[ManifestEntry] = []
    seen: set[tuple[int, str]] = set()
    for tid in settings.source_tiers:
        tier = registry.get_tier(tid)
        if tier is None:
            continue
        for src in tier.sources:
            if not any(_matches_family(src.name, src.notes, fam) for fam in families):
                continue
            key = (tid, src.name)
            if key in seen:
                continue
            seen.add(key)
            out.append(
                ManifestEntry(
                    tier=tid,
                    tier_name=tier.name,
                    source_name=src.name,
                    url=src.url,
                    format=src.format,
                    notes=src.notes,
                    access_policy=src.access_policy,  # type: ignore[arg-type]
                    ingestable=src.access_policy in {"open", "registration"},
                )
            )
    return out
