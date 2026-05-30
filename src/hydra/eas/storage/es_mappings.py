"""Elasticsearch index mappings for the EAS module (Design §4.11, R24.5).

Two indexes live in this module:

* ``hydra-screenshots`` — metadata + OCR text for every screenshot captured
  by :class:`hydra.eas.screenshots.adapter.ScreenshotAdapter`. Supports
  phash similarity search via the raw-bytes ``phash_bits`` field and
  host-scoped retrieval via ``url_host``.
* ``hydra-cves`` — unified view across the five Tier 29 CVE-family sources
  (NVD, EPSS, KEV, ExploitDB, Metasploit). The ``cpe_vendor`` and
  ``cpe_product`` keyword fields are exploded at index time so that search
  by product vendor does not require a scripted query.

These constants are consumed by :mod:`hydra.eas.storage.bootstrap` (task
6.2) which idempotently ``PUT``s them at application startup via
``setup_eas`` (task 17.1). The mappings are intentionally static — there is
no settings hook for shard/replica counts in the MVP; tune by environment
template if needed in post-MVP deployments.
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "HYDRA_SCREENSHOTS_INDEX",
    "HYDRA_CVES_INDEX",
    "HYDRA_SCREENSHOTS_MAPPING",
    "HYDRA_CVES_MAPPING",
    "ALL_INDEX_MAPPINGS",
]


#: Index name for the screenshots metadata + OCR index.
HYDRA_SCREENSHOTS_INDEX = "hydra-screenshots"

#: Index name for the unified CVE / EPSS / KEV / ExploitDB / Metasploit index.
HYDRA_CVES_INDEX = "hydra-cves"


#: Full index definition for ``hydra-screenshots`` — settings + mappings.
#: Shape matches Design §4.11 verbatim; see the design document for the
#: per-field rationale.
HYDRA_SCREENSHOTS_MAPPING: dict[str, Any] = {
    "settings": {
        "number_of_shards": 2,
        "number_of_replicas": 1,
        "refresh_interval": "30s",
    },
    "mappings": {
        "properties": {
            "record_hash": {"type": "keyword"},
            "url": {"type": "keyword"},
            "url_host": {"type": "keyword"},  # derived from url at index time
            "http_status": {"type": "short"},
            "title": {
                "type": "text",
                "fields": {
                    "keyword": {"type": "keyword", "ignore_above": 256},
                },
            },
            "phash": {"type": "keyword"},  # 16-char hex string
            "phash_bits": {"type": "binary"},  # raw 8 bytes — Hamming via script_score
            "content_hash": {"type": "keyword"},
            "rendered_at": {"type": "date"},
            "viewport_w": {"type": "integer"},
            "viewport_h": {"type": "integer"},
            "tier": {"type": "short"},
            "minio_key": {"type": "keyword"},
            "ocr_text": {"type": "text", "index": True},
            "ocr_excerpt": {"type": "keyword", "ignore_above": 1024},
            "tags": {"type": "keyword"},
        },
    },
}


#: Full index definition for ``hydra-cves`` — unified CVE-family index.
HYDRA_CVES_MAPPING: dict[str, Any] = {
    "settings": {
        "number_of_shards": 2,
        "number_of_replicas": 1,
        "refresh_interval": "60s",
    },
    "mappings": {
        "properties": {
            "cve_id": {"type": "keyword"},
            "source": {"type": "keyword"},  # nvd | epss | kev | exploitdb | metasploit
            "published": {"type": "date"},
            "last_modified": {"type": "date"},
            "cvss_v3_score": {"type": "float"},
            "cvss_v3_vector": {"type": "keyword"},
            "epss_score": {"type": "float"},
            "epss_percentile": {"type": "float"},
            "kev_listed": {"type": "boolean"},
            "kev_due_date": {"type": "date"},
            "known_ransomware_use": {"type": "boolean"},
            "cwe_ids": {"type": "keyword"},
            "affected_cpes": {"type": "keyword"},
            "cpe_vendor": {"type": "keyword"},  # exploded for search
            "cpe_product": {"type": "keyword"},
            "description": {"type": "text"},
            "references": {"type": "keyword"},
            "exploit_ids": {"type": "keyword"},
            "metasploit_modules": {"type": "keyword"},
        },
    },
}


#: Convenience mapping iterated by :func:`hydra.eas.storage.bootstrap.bootstrap_eas_indices`.
ALL_INDEX_MAPPINGS: dict[str, dict[str, Any]] = {
    HYDRA_SCREENSHOTS_INDEX: HYDRA_SCREENSHOTS_MAPPING,
    HYDRA_CVES_INDEX: HYDRA_CVES_MAPPING,
}
