"""Tier 29 adapter unit tests and metric-emission tests (tasks 3.4, 3.5).

Exercises each of the five Tier 29 adapters (NVD, EPSS, KEV, ExploitDB,
Metasploit) on canned input records fed directly to
``Tier29RestAdapter.normalize``. No HTTP calls are involved.

Three families of assertions:

* Payload shape per R9.2–R9.6 — the emitted ``NormalizedRecord.payload``
  exactly contains the fields the requirement specifies, plus the
  base-class-injected ``source`` key.
* Raw-hash scheme and determinism per R9.2–R9.6 — the ``raw_hash`` is
  reproducible across invocations and matches the source-specific
  xxhash64 scheme.
* Metric emission per R9.7 — calling ``normalize`` on one record
  increments ``hydra_eas_cve_records_total{source=<label>}`` by 1.

The five source labels (``"nvd"``, ``"epss"``, ``"kev"``, ``"exploitdb"``,
``"metasploit"``) are exhaustively exercised via parametrization.

Validates: R9.1, R9.2, R9.3, R9.4, R9.5, R9.6, R9.7.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest
import xxhash

from hydra.adapters.tier29 import (
    CISAKEVAdapter,
    ExploitDBAdapter,
    FirstEPSSAdapter,
    MetasploitAdapter,
    NVDCVEAdapter,
)
from hydra.adapters.tier29.base import Tier29RestAdapter
from hydra.config import HydraSettings
from hydra.eas.metrics import hydra_eas_cve_records_total
from hydra.models.normalized import Tier


# ---------------------------------------------------------------------------
# Canned input records — one realistic record per source
# ---------------------------------------------------------------------------

_CANNED_NVD = {
    "cve_id": "CVE-2024-1234",
    "published": "2024-01-01T00:00:00Z",
    "last_modified": "2024-01-15T12:30:00Z",
    "cvss_v3_score": 9.8,
    "cvss_v3_vector": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H",
    "cwe_ids": ["CWE-79", "CWE-20"],
    "references": ["https://nvd.nist.gov/vuln/detail/CVE-2024-1234"],
    "affected_cpes": ["cpe:2.3:a:acme:widget:1.0:*:*:*:*:*:*:*"],
    "description": "Example critical vulnerability in acme widget.",
}

_CANNED_EPSS = {
    "cve_id": "CVE-2024-5678",
    "epss_score": 0.92345,
    "epss_percentile": 0.987,
    "score_date": "2024-03-01",
}

_CANNED_KEV = {
    "cve_id": "CVE-2023-99999",
    "vendor": "ExampleCorp",
    "product": "ExampleApp",
    "date_added": "2023-06-15",
    "due_date": "2023-07-06",
    "required_action": "Apply vendor patch.",
    "known_ransomware_use": True,
}

_CANNED_EXPLOITDB = {
    "exploit_id": "51234",
    "title": "ExampleCorp ExampleApp RCE",
    "type": "remote",
    "platform": "linux",
    "published_date": "2024-02-10",
    "author": "researcher@example.org",
    "cve_ids": ["CVE-2023-99999"],
    "source_url": "https://www.exploit-db.com/exploits/51234",
}

_CANNED_METASPLOIT = {
    "module_path": "exploit/linux/http/example_rce",
    "module_type": "exploit",
    "rank": "excellent",
    "disclosure_date": "2023-06-15",
    "cve_ids": ["CVE-2023-99999"],
    "description": "Exploits an unauthenticated RCE in ExampleApp.",
    "platforms": ["linux"],
}


# ---------------------------------------------------------------------------
# Parametrization table for the per-adapter invariants
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AdapterCase:
    """Bundle of expectations for a single Tier 29 adapter."""

    adapter_cls: type[Tier29RestAdapter]
    source_label: str
    canned_record: dict[str, Any]
    expected_payload_keys: frozenset[str]
    raw_hash_input: str


# Expected payload keys per R9.2–R9.6 *plus* the ``source`` key that
# ``Tier29RestAdapter.normalize`` injects before emitting each
# ``NormalizedRecord``.
_NVD_PAYLOAD_KEYS = frozenset({
    "cve_id",
    "published",
    "last_modified",
    "cvss_v3_score",
    "cvss_v3_vector",
    "cwe_ids",
    "references",
    "affected_cpes",
    "description",
    "source",
})
_EPSS_PAYLOAD_KEYS = frozenset({
    "cve_id",
    "epss_score",
    "epss_percentile",
    "score_date",
    "source",
})
_KEV_PAYLOAD_KEYS = frozenset({
    "cve_id",
    "vendor",
    "product",
    "date_added",
    "due_date",
    "required_action",
    "known_ransomware_use",
    "source",
})
_EXPLOITDB_PAYLOAD_KEYS = frozenset({
    "exploit_id",
    "title",
    "type",
    "platform",
    "published_date",
    "author",
    "cve_ids",
    "source_url",
    "source",
})
_METASPLOIT_PAYLOAD_KEYS = frozenset({
    "module_path",
    "module_type",
    "rank",
    "disclosure_date",
    "cve_ids",
    "description",
    "platforms",
    "source",
})


_ADAPTER_CASES: list[AdapterCase] = [
    AdapterCase(
        adapter_cls=NVDCVEAdapter,
        source_label="nvd",
        canned_record=_CANNED_NVD,
        expected_payload_keys=_NVD_PAYLOAD_KEYS,
        raw_hash_input=f"nvd:{_CANNED_NVD['cve_id']}:{_CANNED_NVD['last_modified']}",
    ),
    AdapterCase(
        adapter_cls=FirstEPSSAdapter,
        source_label="epss",
        canned_record=_CANNED_EPSS,
        expected_payload_keys=_EPSS_PAYLOAD_KEYS,
        raw_hash_input=f"epss:{_CANNED_EPSS['cve_id']}:{_CANNED_EPSS['score_date']}",
    ),
    AdapterCase(
        adapter_cls=CISAKEVAdapter,
        source_label="kev",
        canned_record=_CANNED_KEV,
        expected_payload_keys=_KEV_PAYLOAD_KEYS,
        raw_hash_input=f"kev:{_CANNED_KEV['cve_id']}",
    ),
    AdapterCase(
        adapter_cls=ExploitDBAdapter,
        source_label="exploitdb",
        canned_record=_CANNED_EXPLOITDB,
        expected_payload_keys=_EXPLOITDB_PAYLOAD_KEYS,
        raw_hash_input=f"exploitdb:{_CANNED_EXPLOITDB['exploit_id']}",
    ),
    AdapterCase(
        adapter_cls=MetasploitAdapter,
        source_label="metasploit",
        canned_record=_CANNED_METASPLOIT,
        expected_payload_keys=_METASPLOIT_PAYLOAD_KEYS,
        raw_hash_input=f"metasploit:{_CANNED_METASPLOIT['module_path']}",
    ),
]


# ``pytest.mark.parametrize`` needs hashable ids so the case list uses its
# ``source_label`` as a human-readable parameter id.
_ADAPTER_IDS = [case.source_label for case in _ADAPTER_CASES]


# ---------------------------------------------------------------------------
# Per-adapter normalize() invariants (R9.2–R9.6)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("case", _ADAPTER_CASES, ids=_ADAPTER_IDS)
def test_normalize_tier_and_source_label(case: AdapterCase) -> None:
    """Every emitted record has tier 29 and the correct ``tags`` label."""
    settings = HydraSettings()
    adapter = case.adapter_cls(settings=settings)

    records = adapter.normalize([case.canned_record])
    assert len(records) == 1

    record = records[0]
    # Tier 29 (VULNERABILITY_INTELLIGENCE) — R9.1.
    assert record.tier == Tier.VULNERABILITY_INTELLIGENCE
    assert record.tier.value == 29
    # ``tags`` is the canonical source-label carrier on the normalized record.
    assert record.tags == [case.source_label]


@pytest.mark.parametrize("case", _ADAPTER_CASES, ids=_ADAPTER_IDS)
def test_normalize_payload_shape_matches_requirement(case: AdapterCase) -> None:
    """Payload contains *exactly* the expected fields per R9.2–R9.6.

    The set equality (rather than a subset check) ensures no spurious
    extra fields leak in and no required field goes missing.
    """
    settings = HydraSettings()
    adapter = case.adapter_cls(settings=settings)

    records = adapter.normalize([case.canned_record])
    record = records[0]

    assert set(record.payload.keys()) == case.expected_payload_keys, (
        f"{case.source_label} payload shape mismatch: "
        f"expected {case.expected_payload_keys}, got {set(record.payload.keys())}"
    )
    # The base class injects ``source`` with the adapter's source_label so
    # downstream consumers can filter without re-inspecting stream metadata.
    assert record.payload["source"] == case.source_label


@pytest.mark.parametrize("case", _ADAPTER_CASES, ids=_ADAPTER_IDS)
def test_raw_hash_is_deterministic(case: AdapterCase) -> None:
    """Calling ``normalize`` twice on the same input yields identical hashes."""
    settings = HydraSettings()
    adapter = case.adapter_cls(settings=settings)

    first = adapter.normalize([case.canned_record])[0]
    second = adapter.normalize([case.canned_record])[0]

    assert first.raw_hash == second.raw_hash, (
        f"{case.source_label}: raw_hash not deterministic across invocations"
    )
    # xxhash64 hex digests are 16 lowercase hex characters.
    assert len(first.raw_hash) == 16
    assert all(c in "0123456789abcdef" for c in first.raw_hash)


@pytest.mark.parametrize("case", _ADAPTER_CASES, ids=_ADAPTER_IDS)
def test_raw_hash_matches_source_specific_scheme(case: AdapterCase) -> None:
    """``raw_hash`` equals ``xxhash64(<source-specific prefix>:<identifier>…)``.

    Exact schemes per R9.2–R9.6:

    * NVD: ``xxhash64("nvd:{cve_id}:{last_modified}")``
    * EPSS: ``xxhash64("epss:{cve_id}:{score_date}")``
    * KEV: ``xxhash64("kev:{cve_id}")``
    * ExploitDB: ``xxhash64("exploitdb:{exploit_id}")``
    * Metasploit: ``xxhash64("metasploit:{module_path}")``
    """
    settings = HydraSettings()
    adapter = case.adapter_cls(settings=settings)

    record = adapter.normalize([case.canned_record])[0]
    expected_hash = xxhash.xxh64(case.raw_hash_input.encode("utf-8")).hexdigest()

    assert record.raw_hash == expected_hash, (
        f"{case.source_label}: raw_hash does not match "
        f"xxhash64({case.raw_hash_input!r})"
    )


# ---------------------------------------------------------------------------
# Counter emission — ``hydra_eas_cve_records_total`` (task 3.5, R9.7)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("case", _ADAPTER_CASES, ids=_ADAPTER_IDS)
def test_normalize_increments_cve_records_counter(case: AdapterCase) -> None:
    """Calling ``normalize`` on one record bumps the per-source counter by 1 (R9.7).

    ``prometheus_client`` ``Counter`` exposes its current value via
    ``.labels(...)._value.get()``; we capture the value before and after a
    single ``normalize`` call and assert the delta is exactly 1. The test
    is tolerant of concurrent counter activity elsewhere in the test run
    because we only compare before-and-after deltas, not absolute values.
    """
    settings = HydraSettings()
    adapter = case.adapter_cls(settings=settings)

    labeled = hydra_eas_cve_records_total.labels(source=case.source_label)
    before = labeled._value.get()

    records = adapter.normalize([case.canned_record])
    assert len(records) == 1

    after = labeled._value.get()
    assert after - before == pytest.approx(1.0), (
        f"{case.source_label}: expected counter delta of 1, got {after - before}"
    )
