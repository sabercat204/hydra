"""Standards families recognised by the cross-reference engine."""

from __future__ import annotations

import re

# Family id -> human label
FAMILIES: dict[str, str] = {
    "MIL_STD": "MIL-STD (DoD military standards)",
    "MIL_HDBK": "MIL-HDBK (DoD military handbooks)",
    "MIL_PRF": "MIL-PRF (DoD performance specifications)",
    "MIL_DTL": "MIL-DTL (DoD detail specifications)",
    "FED_STD": "FED-STD (US federal standards)",
    "FIPS": "FIPS (Federal Information Processing Standards)",
    "NIST_SP_800": "NIST SP 800 series (cybersecurity)",
    "NIST_SP_500": "NIST SP 500 series (information technology)",
    "NIST_SP_1800": "NIST SP 1800 series (practice guides)",
    "STANAG": "NATO Standardization Agreement",
    "DEF_STAN": "UK Defence Standard",
    "STIG": "DISA Security Technical Implementation Guide",
    "NSA_CSI": "NSA Cybersecurity Information Sheet",
    "ISO_IEC": "ISO/IEC standard",
    "RFC": "IETF Request for Comments",
}

_FAMILY_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("MIL_STD", re.compile(r"^\s*MIL[- ]?STD[- ]?\d", re.IGNORECASE)),
    ("MIL_HDBK", re.compile(r"^\s*MIL[- ]?HDBK[- ]?\d", re.IGNORECASE)),
    ("MIL_PRF", re.compile(r"^\s*MIL[- ]?PRF[- ]?\d", re.IGNORECASE)),
    ("MIL_DTL", re.compile(r"^\s*MIL[- ]?DTL[- ]?\d", re.IGNORECASE)),
    ("FED_STD", re.compile(r"^\s*FED[- ]?STD[- ]?\d", re.IGNORECASE)),
    ("FIPS", re.compile(r"^\s*FIPS[- ]?(PUB[- ]?)?\d", re.IGNORECASE)),
    ("NIST_SP_800", re.compile(r"^\s*NIST\s*SP\s*800", re.IGNORECASE)),
    ("NIST_SP_500", re.compile(r"^\s*NIST\s*SP\s*500", re.IGNORECASE)),
    ("NIST_SP_1800", re.compile(r"^\s*NIST\s*SP\s*1800", re.IGNORECASE)),
    ("STANAG", re.compile(r"^\s*STANAG[- ]?\d", re.IGNORECASE)),
    ("DEF_STAN", re.compile(r"^\s*DEF[- ]?STAN[- ]?\d", re.IGNORECASE)),
    ("STIG", re.compile(r"\bSTIG\b", re.IGNORECASE)),
    ("NSA_CSI", re.compile(r"\bCSI[- ]?\d", re.IGNORECASE)),
    ("ISO_IEC", re.compile(r"^\s*ISO/?IEC\s*\d", re.IGNORECASE)),
    ("RFC", re.compile(r"^\s*RFC\s*\d", re.IGNORECASE)),
)


def detect_family(identifier: str) -> str | None:
    """Return the family code (e.g. ``MIL_STD``) for ``identifier``."""
    if not identifier:
        return None
    for family, pat in _FAMILY_PATTERNS:
        if pat.search(identifier):
            return family
    return None


def normalize_id(identifier: str) -> str:
    """Collapse whitespace and uppercase the identifier for comparison."""
    return re.sub(r"\s+", " ", identifier.strip()).upper()


__all__ = ["FAMILIES", "detect_family", "normalize_id"]
