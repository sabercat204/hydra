"""Pydantic v2 request/response schemas for EAS (Design §2.4, §4).

Each per-capability module exports its own `__all__`; this package re-exports
every public schema so callers can do::

    from hydra.eas.schemas import AssetCreate, LookupResponse, ...
"""

from __future__ import annotations

from hydra.eas.schemas.assets import (
    AssetCreate,
    AssetResponse,
    AssetType,
    ExposureResponse,
    ExposureSeverity,
)
from hydra.eas.schemas.cves import (
    CVEDetailResponse,
    CVESearchParams,
    CVESearchResult,
    ExploitSearchResult,
)
from hydra.eas.schemas.images import (
    ImageMetadataResponse,
    ImageSearchParams,
    ImageSearchResult,
)
from hydra.eas.schemas.lookup import (
    IndicatorClass,
    LookupAssetReference,
    LookupCVECorrelation,
    LookupRecordSummary,
    LookupResponse,
    LookupScreenshotRef,
)
from hydra.eas.schemas.maps import (
    FeatureCollectionResponse,
    FeatureResponse,
    TileCellResponse,
)
from hydra.eas.schemas.observatory import (
    CountryPostureResponse,
    CountryPostureSection,
    ExposurePostureReportResponse,
)
from hydra.eas.schemas.trends import (
    Aggregation,
    Bucket,
    JobProgressResponse,
    TrendPoint,
    TrendRequest,
    TrendResponse,
    TrendSeries,
)

__all__ = [
    # assets
    "AssetType",
    "ExposureSeverity",
    "AssetCreate",
    "AssetResponse",
    "ExposureResponse",
    # images
    "ImageMetadataResponse",
    "ImageSearchResult",
    "ImageSearchParams",
    # cves
    "CVEDetailResponse",
    "CVESearchResult",
    "CVESearchParams",
    "ExploitSearchResult",
    # maps
    "TileCellResponse",
    "FeatureResponse",
    "FeatureCollectionResponse",
    # trends
    "Bucket",
    "Aggregation",
    "TrendRequest",
    "TrendPoint",
    "TrendSeries",
    "TrendResponse",
    "JobProgressResponse",
    # lookup
    "IndicatorClass",
    "LookupAssetReference",
    "LookupRecordSummary",
    "LookupCVECorrelation",
    "LookupScreenshotRef",
    "LookupResponse",
    # observatory
    "CountryPostureSection",
    "CountryPostureResponse",
    "ExposurePostureReportResponse",
]
