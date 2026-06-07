"""Standards cross-reference engine."""

from hydra.mil_int.xref.families import detect_family, FAMILIES, normalize_id
from hydra.mil_int.xref.resolver import XrefResolver, load_xref_seed

__all__ = [
    "FAMILIES",
    "XrefResolver",
    "detect_family",
    "load_xref_seed",
    "normalize_id",
]
