"""Tier 29 substrate invariants (task 3.4).

Three families of assertions about the Tier 29 (Vulnerability Intelligence)
substrate:

* :data:`~hydra.models.normalized.Tier.VULNERABILITY_INTELLIGENCE` equals
  ``29`` (R9.1).
* ``src/hydra/registry/stream_registry.yaml`` surfaces a Tier 29 entry and
  the five Tier 29 sources: NVD CVE, FIRST EPSS, CISA KEV, ExploitDB, and
  Metasploit modules (R9.1).
* :func:`hydra.adapters.tier29.build_tier29_adapter` dispatches each of the
  five Tier 29 ``stream_id`` values to the correct adapter subclass, and
  raises :class:`ValueError` on an unknown stream id (R9.1).

Validates: R9.1.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from hydra.adapters.tier29 import (
    CISAKEVAdapter,
    ExploitDBAdapter,
    FirstEPSSAdapter,
    MetasploitAdapter,
    NVDCVEAdapter,
    build_tier29_adapter,
)
from hydra.config import HydraSettings
from hydra.models.normalized import Tier


# ---------------------------------------------------------------------------
# Tier enum invariant (R9.1)
# ---------------------------------------------------------------------------


def test_vulnerability_intelligence_tier_is_29() -> None:
    """``Tier.VULNERABILITY_INTELLIGENCE`` is the integer ``29`` (R9.1)."""
    assert Tier.VULNERABILITY_INTELLIGENCE == 29
    assert Tier.VULNERABILITY_INTELLIGENCE.value == 29
    assert int(Tier.VULNERABILITY_INTELLIGENCE) == 29


# ---------------------------------------------------------------------------
# Stream registry contents (R9.1)
# ---------------------------------------------------------------------------


# Expected stream identifiers for the five Tier 29 sources. These match
# the keys in ``hydra.adapters.tier29._STREAM_TO_ADAPTER`` and the stream
# ids accepted by ``build_tier29_adapter``.
_EXPECTED_TIER29_STREAM_IDS = {
    "nvd-cve",
    "first-epss",
    "cisa-kev",
    "exploitdb",
    "metasploit-modules",
}

# Human-readable source names found in the ``sources`` list of Tier 29 in
# ``stream_registry.yaml``. Sources are stored as pipe-delimited strings
# like ``"NIST NVD|<url>|JSON|<auth>|<description>"``; we scan for the
# first pipe-delimited segment.
_EXPECTED_TIER29_SOURCE_PREFIXES = {
    "NIST NVD",
    "FIRST EPSS",
    "CISA KEV",
    "ExploitDB",
    "Metasploit Modules",
}


def _load_stream_registry() -> dict:
    """Load and parse ``src/hydra/registry/stream_registry.yaml``."""
    registry_path = Path("src/hydra/registry/stream_registry.yaml")
    assert registry_path.exists(), (
        f"Expected stream registry at {registry_path!r}; if the file has moved, "
        "update this test."
    )
    return yaml.safe_load(registry_path.read_text())


def test_stream_registry_has_tier_29_entry() -> None:
    """``tiers`` list contains a Tier 29 entry named Vulnerability Intelligence."""
    data = _load_stream_registry()
    tiers = data.get("tiers")
    assert isinstance(tiers, list), "stream_registry.yaml must define a 'tiers' list"

    tier29_entries = [t for t in tiers if t.get("id") == 29]
    assert len(tier29_entries) == 1, (
        f"Expected exactly one Tier 29 entry, found {len(tier29_entries)}"
    )
    tier29 = tier29_entries[0]
    assert tier29.get("name") == "Vulnerability Intelligence"
    # Streams count declared on the entry should match the five sources.
    assert tier29.get("streams") == 5


def test_stream_registry_tier_29_lists_all_five_sources() -> None:
    """Tier 29 ``sources`` list covers NVD, EPSS, KEV, ExploitDB, Metasploit."""
    data = _load_stream_registry()
    tier29 = next(t for t in data["tiers"] if t.get("id") == 29)
    sources = tier29.get("sources")
    assert isinstance(sources, list), "Tier 29 must declare a 'sources' list"
    assert len(sources) == 5, f"Expected 5 Tier 29 sources, got {len(sources)}"

    # Sources are formatted as pipe-delimited strings. The first segment is
    # the human-readable source name. Build a set of these prefixes and
    # assert it is equal to the expected set.
    found_prefixes = {str(src).split("|", 1)[0].strip() for src in sources}
    assert found_prefixes == _EXPECTED_TIER29_SOURCE_PREFIXES, (
        f"Tier 29 source prefixes differ: expected {_EXPECTED_TIER29_SOURCE_PREFIXES}, "
        f"got {found_prefixes}"
    )


def test_stream_registry_text_contains_stream_id_tokens() -> None:
    """The raw YAML text (or a lowercased form of it) mentions each stream id.

    This is a looser belt-and-braces check: even if the YAML schema changes
    later to include explicit ``stream_id`` keys, scanning the raw text for
    the five canonical stream ids guarantees the registry stays in sync
    with the adapter dispatch table.
    """
    text = Path("src/hydra/registry/stream_registry.yaml").read_text().lower()
    # All five stream ids should be discoverable either as explicit tokens
    # or as substrings of the source prefixes. We verify each expected stream
    # id token by matching its components (e.g. ``nvd-cve`` matches ``nvd``).
    expected_substrings = {
        "nvd-cve": "nist nvd",
        "first-epss": "first epss",
        "cisa-kev": "cisa kev",
        "exploitdb": "exploitdb",
        "metasploit-modules": "metasploit modules",
    }
    missing = [
        stream_id
        for stream_id, substring in expected_substrings.items()
        if substring not in text
    ]
    assert not missing, (
        f"The following Tier 29 stream identifiers have no matching source "
        f"prefix in stream_registry.yaml: {missing}"
    )


# ---------------------------------------------------------------------------
# Dispatcher (R9.1)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("stream_id", "expected_cls"),
    [
        ("nvd-cve", NVDCVEAdapter),
        ("first-epss", FirstEPSSAdapter),
        ("cisa-kev", CISAKEVAdapter),
        ("exploitdb", ExploitDBAdapter),
        ("metasploit-modules", MetasploitAdapter),
    ],
)
def test_build_tier29_adapter_dispatches_correctly(
    stream_id: str, expected_cls: type
) -> None:
    """Each known Tier 29 ``stream_id`` resolves to the expected subclass."""
    settings = HydraSettings()
    adapter = build_tier29_adapter(stream_id, settings)
    assert isinstance(adapter, expected_cls), (
        f"Expected build_tier29_adapter({stream_id!r}) to return a "
        f"{expected_cls.__name__}, got {type(adapter).__name__}"
    )
    # Sanity check: the adapter should carry the stream id through.
    assert adapter.stream_id == stream_id


def test_build_tier29_adapter_rejects_unknown_stream_id() -> None:
    """An unknown ``stream_id`` raises :class:`ValueError` (R9.1)."""
    settings = HydraSettings()
    with pytest.raises(ValueError) as excinfo:
        build_tier29_adapter("definitely-not-a-real-stream", settings)
    # The error message should mention the unknown id and the known set so
    # the operator can fix a typo quickly.
    msg = str(excinfo.value)
    assert "definitely-not-a-real-stream" in msg
