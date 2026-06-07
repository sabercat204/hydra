"""HYDRA data adapters."""

from .base import AdapterHealth, BaseAdapter, HealthStatus, RawPayload
from .ckan import CkanAdapter
from .doc_repo import DocRepoAdapter
from .exceptions import (
    AdapterError,
    AdapterRegistryMismatch,
    FetchError,
    ParseError,
    RateLimitError,
    ValidationError,
)
from .fdsn import FdsnAdapter
from .odata import ODataAdapter
from .rest_json import RestJsonAdapter
from .s3_bulk import S3BulkAdapter
from .sdmx import SdmxAdapter
from .tap_vo import TapVoAdapter
from .scrape_rss import ScrapeRssAdapter
from .ais_adsb import AisAdsbAdapter
from .stix_taxii import StixTaxiiAdapter

__all__ = [
    "AdapterHealth",
    "AdapterError",
    "AdapterRegistryMismatch",
    "AisAdsbAdapter",
    "BaseAdapter",
    "CkanAdapter",
    "DocRepoAdapter",
    "FdsnAdapter",
    "FetchError",
    "HealthStatus",
    "ODataAdapter",
    "ParseError",
    "RateLimitError",
    "RawPayload",
    "RestJsonAdapter",
    "S3BulkAdapter",
    "ScrapeRssAdapter",
    "SdmxAdapter",
    "StixTaxiiAdapter",
    "TapVoAdapter",
    "ValidationError",
]
