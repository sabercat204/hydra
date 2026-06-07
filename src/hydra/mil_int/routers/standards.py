"""Standards cross-reference router."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from hydra.mil_int.dependencies import get_mil_int_settings, get_xref_resolver
from hydra.mil_int.schemas.xref import XrefResponse
from hydra.mil_int.settings import MilIntSettings
from hydra.mil_int.xref.families import FAMILIES
from hydra.mil_int.xref.resolver import XrefResolver

router = APIRouter(prefix="/api/v1/mil-int/standards", tags=["mil-int"])


@router.get("/xref", response_model=XrefResponse)
def xref(
    from_id: str = Query(..., description="e.g. MIL-STD-461 or NIST SP 800-53"),
    to_family: str | None = Query(default=None),
    resolver: XrefResolver = Depends(get_xref_resolver),
    settings: MilIntSettings = Depends(get_mil_int_settings),
) -> XrefResponse:
    """Resolve a standard identifier to its cross-references.

    ``to_family`` constrains results to a single family (see
    ``/families``). Without it, every known mapping is returned.
    """
    mappings = resolver.lookup(
        from_id,
        to_family=to_family,
        max_results=settings.xref_max_results,
    )
    return XrefResponse(from_id=from_id, mappings=mappings, total=len(mappings))


@router.get("/families")
def list_families() -> dict[str, str]:
    """Return the recognised standards families."""
    return FAMILIES
