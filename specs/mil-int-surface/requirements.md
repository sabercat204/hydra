# Mil-Int Public Information Surface — Requirements

## Surface

`mil_int_public_information` aggregates 64+ publicly accessible military,
defense, and national security information repositories across 40+ nations
and multinational organizations. Tiers `100-107` map onto the eight source
groupings in the LOOM intake spec (US domestic → access-control reference).

## R1 — Classification

R1.1. The surface MUST operate exclusively on **UNCLASSIFIED** content.
R1.2. Records bearing classification markers (CONFIDENTIAL, SECRET, TS,
NOFORN, FOUO, CUI, NATO RESTRICTED, etc.) MUST be rejected by the
classification gate before reaching storage.
R1.3. Rejections MUST increment
`hydra_mil_int_access_policy_violations_total{kind="classification",marker=...}`.
R1.4. The gate MAY operate in shadow ("log-only") mode for rollouts but
MUST default to enforcement.

## R2 — Access policy

R2.1. Each source MUST carry an `access_policy` of one of:
`open | registration | subscription | restricted | archived | monitor_only`.
R2.2. The doc_repo adapter MUST short-circuit fetch for any source whose
policy is not `open`, or whose policy is `registration` without operator-
provisioned credentials.
R2.3. Sources flagged `monitor_only` (e.g. CNKI post-2023-03) MUST be
documented in the manifest but never auto-fetched.

## R3 — Ingestion

R3.1. The new `doc_repo` adapter MUST honour pagination, deduplication, and
the platform's standard fetch-with-retry semantics.
R3.2. Default behaviour MUST extract document references from `<a href>`
matching configured extensions or regexes; richer extraction via CSS
`item_selector` + `field_map` MUST be supported.
R3.3. Blob downloads MUST be opt-in (`download_blobs: true`), bounded by
`blob_byte_limit`, and routed through the storage router's
`_binary_artifact` channel for MinIO persistence.

## R4 — Storage routing

R4.1. Tiers 100–106 MUST persist to `[postgres, minio, elasticsearch]` so
documents land in MinIO and metadata is full-text searchable.
R4.2. Tier 107 (access-control references) MUST persist to `[postgres]`
only — no blob, no FT.

## R5 — Cross-reference

R5.1. The xref engine MUST seed from `config/mil_int_xref.yaml` and expose
both directions of every mapping.
R5.2. `GET /api/v1/mil-int/standards/xref?from_id=...&to_family=...` MUST
return matched mappings bounded by `xref_max_results`.
R5.3. Recognised families MUST include MIL-STD, MIL-HDBK, FIPS, NIST SP
800/500/1800, STANAG, DEF STAN, STIG, NSA CSI, ISO/IEC, and RFC.

## R6 — Cross-tier dedup

R6.1. The mirror dedup resolver MUST collapse mirrored documents to a
single canonical record, preferring authority order
(DLA ASSIST → DTIC → NIST → DISA → NSA → EverySpec → FAS → ...).

## R7 — API surface

R7.1. The surface MUST expose five routers under `/api/v1/mil-int`:
search, standards, doctrine, compliance, manifest.
R7.2. All response models MUST be Pydantic v2 typed.
R7.3. Search MUST support faceting on `tier`, `country`, `content_type`,
`access_policy`, `language`, `freshness`.

## R8 — Observability

R8.1. The surface MUST emit:
- `hydra_mil_int_documents_indexed_total{tier, country, content_type}`
- `hydra_mil_int_access_policy_violations_total{kind, marker}`
- `hydra_mil_int_xref_resolutions_total{from_family, to_family}`
- `hydra_mil_int_freshness_score`
- `hydra_mil_int_dedup_dropped_total{dropped_source, canonical_source}`

## R9 — Configuration

R9.1. `MilIntSettings` MUST be nested under `HydraSettings.mil_int`.
R9.2. All toggles MUST be overridable via `HYDRA_MIL_INT__*` env vars.

## R10 — Manifest

R10.1. `specs/mil-int-surface/source_manifest.md` MUST enumerate every
registered source with its access policy. The manifest router MUST surface
the same data programmatically.
