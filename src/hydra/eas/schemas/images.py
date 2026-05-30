"""Image / screenshot request/response schemas (Design §4.3).

Implements the public contract for Capability 2 — Visual / Screenshot
Intelligence: ImageMetadataResponse (metadata-only view over MinIO-stored
screenshots), ImageSearchResult (phash-similarity hits), and ImageSearchParams
(query parameters for `/api/v1/images/search`). See R7.2, R8.1.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

__all__ = [
    "ImageMetadataResponse",
    "ImageSearchResult",
    "ImageSearchParams",
]


class ImageMetadataResponse(BaseModel):
    record_hash: str = Field(..., pattern=r"^[0-9a-f]{16}$")
    url: str
    http_status: int | None = Field(default=None, ge=0, le=599)
    title: str | None = None
    phash: str = Field(..., pattern=r"^[0-9a-f]{16}$")
    content_hash: str = Field(..., pattern=r"^[0-9a-f]{64}$")  # SHA-256
    rendered_at: datetime
    viewport: tuple[int, int]
    minio_key: str
    has_ocr: bool = False
    ocr_excerpt: str | None = Field(default=None, max_length=512)


class ImageSearchResult(BaseModel):
    record_hash: str
    url: str
    phash: str
    similarity: float = Field(..., ge=0.0, le=1.0)
    rendered_at: datetime
    title: str | None = None


class ImageSearchParams(BaseModel):
    phash: str = Field(..., pattern=r"^[0-9a-f]{16}$")
    similarity: float = Field(default=0.85, ge=0.0, le=1.0)
    tiers: list[int] | None = None
    since: datetime | None = None
    url_contains: str | None = Field(default=None, max_length=256)
