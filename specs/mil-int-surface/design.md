# Mil-Int Surface — Design

## §1 Overview

The mil_int_public_information surface aggregates public defense / S&T /
intelligence document repositories. It is a "second-class" HYDRA surface
in the EAS sense: it owns its own settings, routers, schemas, and
business logic, but reuses the platform's universal record schema,
storage router, scheduler, auth manager, and observability stack.

## §2 Module layout (`src/hydra/mil_int/`)

```
mil_int/
├── __init__.py            # MilIntSettings re-export
├── settings.py            # MilIntSettings (Pydantic v2)
├── classification.py      # UNCLASSIFIED-only gate
├── dedup.py               # Mirror dedup resolver (DLA↔EverySpec, RU MoD↔Russia Matters, ...)
├── metrics.py             # Prometheus metrics
├── dependencies.py        # FastAPI DI singletons
├── setup.py               # mount_mil_int_routers + setup_mil_int
├── schemas/               # Pydantic models: record, search, xref, manifest
├── routers/               # FastAPI routers: search, standards, doctrine, compliance, manifest
└── xref/                  # Standards cross-reference engine
    ├── families.py        # MIL-STD, NIST SP 800, STANAG, DEF STAN, FIPS, STIG, ...
    └── resolver.py        # In-memory bidirectional map seeded from YAML
```

## §3 Tiers

Tiers 100–107 register one entry per geographic / organisational source
group (US domestic, Five Eyes, NATO, Nordic, Asia-Pacific, Russia/adversary,
regional, access-control reference). Each tier carries:

- `cadence` — how often the cadence DAG ingests the tier (`weekly` /
  `biweekly` / `monthly` / `quarterly` / `on_change`).
- `adapter` — `doc_repo` for content tiers, `rest_json` for the
  reference tier (107).
- `storage.storage_engines` — explicit override; tiers 100–106 use
  `[postgres, minio, elasticsearch]`, tier 107 uses `[postgres]`.

## §4 Adapter (`doc_repo`)

```
fetch (listing page, pagination)
  → parse (anchors / item_selector + field_map → document URLs)
  → validate (URL well-formedness, dedup within run)
  → normalize (NormalizedRecord with optional _binary_artifact for MinIO)
```

Per-source config keys (all optional, with sensible defaults):

| Key | Default | Purpose |
|---|---|---|
| `list_url` | tier source URL | Listing page entry point |
| `max_list_pages` | 5 | Pagination cap |
| `pagination_next_selector` | "" | CSS selector for "next page" anchor |
| `fetch_delay_seconds` | 1.5 | Politeness delay between page fetches |
| `item_selector` | "" | CSS selector for repeating list items |
| `field_map` | {} | `{field: {selector, attribute}}` per item |
| `doc_url_pattern` | "" | Regex applied to absolute URLs |
| `doc_extensions` | `[".pdf", ".PDF", ".html", ".htm"]` | Fallback URL filter |
| `max_docs_per_run` | 25 | Document cap per run |
| `download_blobs` | `false` | Opt-in MinIO blob persistence |
| `blob_byte_limit` | 25 MiB | Per-document byte ceiling |
| `country` | "" | Country tag emitted on the record |
| `content_type` | `research_reports` | Surface-level content-type |
| `auth_pattern` | none | One of `none|api_key|basic_auth|cookie_auth` |

## §5 Classification gate

`hydra.mil_int.classification.is_unclassified(record)` inspects the
record's title / abstract / URL / tags / explicit `classification`
field for forbidden markers. The gate is invoked from any pipeline
that admits records into the surface — adapter `normalize`, search
indexer, REST ingestion. Rejections increment
`hydra_mil_int_access_policy_violations_total`.

## §6 Mirror dedup

`hydra.mil_int.dedup.resolve_mirrors(records)` fingerprints each record
by `(filename, content_type)` and keeps the highest-authority source
per fingerprint. Authority order is curated in `dedup._AUTHORITY_ORDER`
(DLA ASSIST → DTIC → NIST → DISA → NSA → EverySpec → FAS → ...).

## §7 Cross-reference

The xref engine reads `config/mil_int_xref.yaml` into an in-memory
bidirectional map. Each YAML entry declares `(from_family, from_id,
to_family, to_id, relationship, notes)`. The resolver normalises IDs
(uppercase, single-spaced) for lookup. Reverse mappings are auto-
generated. `XrefResolver.lookup` is the single read API.

## §8 API

```
/api/v1/mil-int/manifest                 GET   list every source + access_policy
/api/v1/mil-int/search                   POST  full-text + faceted (needs backend)
/api/v1/mil-int/standards/xref           GET   from_id → mappings
/api/v1/mil-int/standards/families       GET   list recognised families
/api/v1/mil-int/doctrine/sources         GET   curated Tier 105 sources
/api/v1/mil-int/compliance/sources       GET   STIG + NIST SP 800 + NSA CSI overlay
```

## §9 Wiring

`hydra.api.app.create_app` calls `mount_mil_int_routers(app)` so paths
appear at app-construction time. Deployment bootstrap calls
`await setup_mil_int(app, settings, search_backend=...)` to install
runtime singletons. The xref resolver lazy-loads on first dependency
access if `setup_mil_int` hasn't run.

## §10 Search backends

Two implementations live under `src/hydra/mil_int/search/`:

- `InMemorySearchBackend` — list-of-records, O(n) per query, used by
  tests and dev environments without an Elasticsearch cluster.
- `ElasticsearchSearchBackend` — wraps an `AsyncElasticsearch` client.
  Translates `SearchRequest` into a bool query with `multi_match` over
  `payload.title^3 / payload.abstract^2 / payload.keywords / tags`,
  `terms` filters per dimension, a `range` filter on
  `payload.freshness_score`, and `terms` aggs for facets. Defaults to
  the index pattern `hydra-mil-int-*` (set as `es_index_prefix:
  hydra-mil-int` on tiers 100-106 in the registry).

Wiring: `setup_mil_int(app, settings, search_backend=..., es_client=...)`
takes either a fully-built backend or an ES client and lifts it into the
backend wrapper. Without either, `/api/v1/mil-int/search` returns 503.

## §11 Out of scope (future)

- Live PDF text extraction + OCR (defer to a follow-up task).
- Translation pipeline for non-English sources.
- Bilateral authority store (currently a static curated list).
- Vector / semantic search (current backend is BM25 / lexical only).
