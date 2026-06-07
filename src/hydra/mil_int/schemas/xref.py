"""Standards cross-reference request / response schemas."""

from __future__ import annotations

from pydantic import BaseModel, Field


class XrefMapping(BaseModel):
    from_family: str
    from_id: str
    to_family: str
    to_id: str
    relationship: str = Field(default="related", description="exact|related|supersedes|implements")
    notes: str = ""


class XrefRequest(BaseModel):
    from_id: str = Field(..., description="e.g. MIL-STD-461 or NIST SP 800-53")
    to_family: str | None = Field(
        default=None,
        description="Limit results to a single family (NIST_SP_800, STANAG, ...)",
    )


class XrefResponse(BaseModel):
    from_id: str
    mappings: list[XrefMapping]
    total: int


__all__ = ["XrefMapping", "XrefRequest", "XrefResponse"]
