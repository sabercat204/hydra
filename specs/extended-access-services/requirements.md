# Requirements Document

## Introduction

This document defines the requirements for **Extended Access Services (EAS)**, Phase 13 of the HYDRA OSINT Platform. EAS is a cohesive module that surfaces HYDRA's ingested and correlated data through seven consumer-facing capabilities inspired by Shodan's extended product portfolio: asset exposure monitoring, visual/screenshot intelligence, CVE and exploit enrichment, geospatial exploration, historical trends, fast indicator lookup, and an exposure observatory.

EAS does not replace or redesign the existing platform. Every capability is layered on top of existing primitives from P0–P12:

- `NormalizedRecord` and the `Tier` enum (P0)
- The 6 storage engines and `RedisCache` (P7)
- The Airflow DAG factory and cadence scheduler (P8)
- The `CorrelationEngine` and pipeline framework (P9 — extended with a 4th pipeline)
- The `AnalysisEngine`, `BaseProduct` framework and `intelligence_products` table (P10 — extended with a 4th product type)
- The FastAPI application, `APIResponse` envelope, cursor pagination, `JobManager`, `X-API-Key` authentication, rate-limit middleware, and watchlist pattern (P11 — extended with 7 new routers)
- Prometheus metrics, Alertmanager receivers, SLOs, and anomaly detection (P12 — extended with new metrics and alerts)

The requirements below follow EARS patterns and INCOSE quality rules, organized by capability with cross-cutting requirements at the end. Correctness properties suitable for property-based testing are called out explicitly in each capability and consolidated in Requirement 27.

## Glossary

### Core concepts

- **EAS**: Extended Access Services — the complete Phase 13 module (`src/hydra/eas/`) providing the seven capabilities described below.
- **Tenant**: An authenticated API key owner with a `tenant_id` attribute on the `api_keys` PostgreSQL table; introduced by this phase to scope asset monitoring and cost controls.
- **Asset**: A tenant-owned network or identity resource that can be monitored for exposure. Asset types: `ip`, `cidr`, `domain`, `asn`, `hostname`.
- **Indicator**: A single observable used for lookups — an IPv4/IPv6 address, domain, hostname, or record `raw_hash`.
- **Exposure**: A `NormalizedRecord` that references a tenant's asset in its payload (for example a cyber-threat record mentioning the asset's IP) and has `ingested_at` after the asset was registered.
- **Exposure_Event**: An asset/record pairing produced by the exposure-matching job and persisted in the `asset_exposures` table.
- **Perceptual_Hash**: A 64-bit pHash computed with the `imagehash` library over a screenshot image.
- **Hamming_Similarity**: Similarity between two perceptual hashes defined as `1.0 - (hamming_distance / 64.0)`.

### New subsystems

- **AssetMonitor**: The component that evaluates newly ingested `NormalizedRecord` instances against registered tenant assets and produces `Exposure_Event` rows.
- **Screenshot_Adapter**: A new adapter type (`screenshot` in the `adapter_type` field of `SourceMeta`) that takes a URL, renders it with Playwright, and stores a PNG in MinIO.
- **CVE_Pipeline**: A new correlation pipeline (pipeline_id `cve_correlation`, P9 pipeline #4) that joins CVE records against fingerprint fields in other records.
- **VulnerabilityTier**: Tier 29 (`VULNERABILITY_INTELLIGENCE`) — the new thematic tier added to the `Tier` enum for CVE, exploit, and KEV data.
- **ExposureObservatory**: A new intelligence product generator (product_type `exposure_posture_report`, P10 product #4) that aggregates asset-exposure and CVE-correlation data by country/region.
- **Indicator_Lookup_Cache**: A Redis-backed read-through cache with LRU eviction and TTL used by the `/lookup/{indicator}` endpoint.
- **Tile_Aggregator**: The server-side clustering component that groups records into geohash-prefix or H3 cells for the maps endpoint.

### New API surfaces (all mounted under `/api/v1`)

- **Assets_Router**: `/api/v1/assets` — tenant asset CRUD and exposure feed.
- **Images_Router**: `/api/v1/images` — screenshot metadata, similarity search, and blob retrieval.
- **CVEs_Router**: `/api/v1/cves` — CVE lookup, search, and affected-asset listing.
- **Exploits_Router**: `/api/v1/exploits` — ExploitDB / Metasploit module search.
- **Maps_Router**: `/api/v1/maps` — bbox feature collections and tile aggregates.
- **Trends_Router**: `/api/v1/trends` — time-series queries with aggregation and comparison mode.
- **Jobs_Router**: `/api/v1/jobs` — extended job status with progress metadata.
- **Lookup_Router**: `/api/v1/lookup` — fast single-indicator lookup.
- **Observatory_Router**: `/api/v1/observatory` — exposure posture endpoints (also exposed via the existing `/products` router).

### Identifiers and schemas

- **Cursor**: The opaque base64 token defined by P11 pagination (`pagination.encode_cursor`/`decode_cursor`). All EAS list endpoints use the same implementation.
- **APIResponse[T]**: The standard response envelope from P11 `schemas/common.py`.
- **JobStatus**: The P11 Redis-backed job status model; extended with `progress_current`, `progress_total`, and `eta_seconds` fields for the Jobs_Router.
- **Rate_Tier**: The P11 rate-limit tier enum (`read`, `search`, `write`). EAS adds a new `expensive` tier (see Requirement 21) for screenshot capture and observatory report generation.

## Requirements

---

## Capability 1 — Asset Exposure Monitoring

### Requirement 1: Asset Registration

**User Story:** As a tenant analyst, I want to register assets I own with HYDRA, so that I can be notified when new exposures are ingested for those assets.

#### Acceptance Criteria

1. WHEN a client sends an authenticated POST to `/api/v1/assets` with a JSON body containing `asset_type` in `{"ip", "cidr", "domain", "asn", "hostname"}` and a syntactically valid `value`, THE Assets_Router SHALL create an `assets` row with a new `asset_id` (UUID4) bound to the caller's `tenant_id` and return `201 Created` with the persisted asset in the APIResponse envelope.
2. IF the submitted `value` fails RFC-compliant parsing for the given `asset_type` (for example a malformed CIDR or a domain that violates RFC 1035), THEN THE Assets_Router SHALL return `422 Unprocessable Entity` with error code `VALIDATION_ERROR` and no row SHALL be inserted.
3. WHEN a tenant submits an asset with `(tenant_id, asset_type, normalized_value)` equal to an already-registered asset for that tenant, THE Assets_Router SHALL return `200 OK` with the existing row and SHALL NOT create a duplicate, where `normalized_value` is the lower-cased, trimmed, IPv6-compacted, or CIDR-canonicalized form of `value`.
4. THE Assets_Router SHALL enforce per-tenant asset quotas from `EASSettings.asset_quota_per_tenant` (default 1000), returning `409 Conflict` with error code `ASSET_QUOTA_EXCEEDED` when the quota would be exceeded.
5. WHILE an asset row has `is_active = FALSE`, THE AssetMonitor SHALL NOT produce new `Exposure_Event` rows for that asset.

**Correctness properties (PBT-suitable):**
- **Idempotency**: For any valid input `(tenant_id, asset_type, value)`, submitting it twice results in the same `asset_id` and exactly one `assets` row.
- **Normalization fixpoint**: `normalize(normalize(value)) == normalize(value)` for every accepted `asset_type`.

### Requirement 2: Asset Listing and Deletion

**User Story:** As a tenant analyst, I want to list and remove my registered assets, so that I can manage my monitoring scope over time.

#### Acceptance Criteria

1. WHEN a client sends an authenticated GET to `/api/v1/assets`, THE Assets_Router SHALL return only the assets whose `tenant_id` matches the caller's `tenant_id`, paginated with the standard cursor mechanism.
2. WHERE the optional `asset_type` query parameter is provided, THE Assets_Router SHALL restrict the listing to assets of that type.
3. WHEN a client sends an authenticated DELETE to `/api/v1/assets/{asset_id}` and the asset belongs to the caller's tenant, THE Assets_Router SHALL set `is_active = FALSE` and `deactivated_at = NOW()` on the row, return `204 No Content`, and SHALL NOT delete the underlying historical `Exposure_Event` rows.
4. IF a client sends any request to `/api/v1/assets/{asset_id}` whose `asset_id` exists but belongs to a different tenant, THEN THE Assets_Router SHALL return `404 Not Found` with error code `NOT_FOUND` and SHALL NOT reveal the asset's existence.

**Correctness property:** **Tenant isolation** — for every pair of tenants `A` and `B` and every asset-router operation, `A` receives responses identical to those it would receive if `B`'s rows did not exist.

### Requirement 3: Exposure Matching

**User Story:** As a tenant analyst, I want HYDRA to automatically match newly ingested records against my registered assets, so that exposures are discovered without manual queries.

#### Acceptance Criteria

1. WHEN a `NormalizedRecord` is written to `normalized_records` with `tier` in `EASSettings.exposure_matching_tiers` (default Tier 16, Tier 17, Tier 28, and the new Tier 29), THE AssetMonitor SHALL extract candidate indicator values from the record payload using `EASSettings.indicator_extraction_map` and evaluate each against the active `assets` table.
2. WHEN a record indicator matches an active asset under `AssetMonitor.is_match(indicator, asset)`, THE AssetMonitor SHALL write exactly one `asset_exposures` row with `(asset_id, record_hash, tier, matched_indicator, severity, created_at)` where `severity` is computed from `EASSettings.exposure_severity_map`.
3. IF a candidate `(asset_id, record_hash, matched_indicator)` triple already exists in `asset_exposures`, THEN THE AssetMonitor SHALL NOT write a duplicate row.
4. WHEN an `Exposure_Event` row is written, THE AssetMonitor SHALL publish a Prometheus counter increment on `hydra_eas_exposure_events_total` with labels `{tenant_id, asset_type, tier, severity}` and SHALL emit an Alertmanager-compatible payload to the configured EAS receiver when `severity == "critical"`.
5. THE AssetMonitor SHALL process exposures for a given `record_hash` exactly once per scheduler run as tracked by the Redis key `hydra:eas:exposure_processed:{record_hash}` with TTL matching `EASSettings.exposure_dedup_ttl_seconds` (default 86400).

**Correctness properties:**
- **Match determinism**: For identical `(record, asset)` inputs, `AssetMonitor.is_match` returns identical boolean results across runs and processes.
- **Dedup invariance**: For any multiset of identical `(asset_id, record_hash, matched_indicator)` triples, the resulting count of `asset_exposures` rows is at most 1.

### Requirement 4: Exposure Feed Endpoint

**User Story:** As a tenant analyst, I want to list and filter my exposure events through the API, so that I can triage findings.

#### Acceptance Criteria

1. WHEN a client sends an authenticated GET to `/api/v1/assets/{asset_id}/exposures` for an asset owned by the caller's tenant, THE Assets_Router SHALL return `Exposure_Event` rows filtered to that asset, sorted by `created_at DESC`, paginated via cursor, wrapped in `APIResponse[PagedResponse[ExposureResponse]]`.
2. WHERE the `severity` query parameter is provided, THE Assets_Router SHALL restrict the listing to exposures with `severity` in the provided values.
3. WHERE the `since` query parameter is provided as an ISO 8601 timestamp, THE Assets_Router SHALL restrict the listing to exposures with `created_at > since`.
4. WHEN a client sends an authenticated GET to `/api/v1/exposures`, THE Assets_Router SHALL return exposures across all assets owned by the caller's tenant with the same pagination, filtering, and envelope rules.

**Correctness property:** **Pagination round-trip invariance** — concatenating the records of consecutive pages retrieved with `follow_cursor` yields the same multiset as a single unpaginated read with `LIMIT = total`, assuming no concurrent writes.

### Requirement 5: Exposure Alert Routing

**User Story:** As a platform operator, I want exposure alerts to flow through the existing P12 Alertmanager receivers, so that on-call workflows do not need a new paging pipeline.

#### Acceptance Criteria

1. WHEN the AssetMonitor emits a `critical` exposure, THE Monitoring_Subsystem SHALL deliver the alert to the `eas-critical` Alertmanager receiver configured with tenant-label routing in `alertmanager/alertmanager.yml`.
2. WHEN the AssetMonitor emits a `high` exposure, THE Monitoring_Subsystem SHALL deliver the alert to the `eas-warning` Alertmanager receiver.
3. WHERE `EASSettings.per_tenant_webhook_url` is configured for a tenant, THE Monitoring_Subsystem SHALL additionally deliver that tenant's exposures to the tenant's webhook via an Alertmanager `webhook_config` entry generated at configuration load time.
4. IF an Alertmanager delivery attempt fails, THEN THE Monitoring_Subsystem SHALL rely on Alertmanager's own retry and buffering without duplicating the event in `asset_exposures`.

---

## Capability 2 — Visual / Screenshot Intelligence

### Requirement 6: Screenshot Capture

**User Story:** As an analyst, I want HYDRA to capture screenshots of exposed HTTP(S) services found in ingested records, so that I can visually inspect the exposed surface.

#### Acceptance Criteria

1. WHEN the Screenshot_Adapter receives a task for a URL satisfying `url.scheme in {"http", "https"}` and `not url.host.is_private`, THE Screenshot_Adapter SHALL render the page using a headless Playwright Chromium instance with the settings in `EASSettings.screenshot` (default viewport 1280x800, timeout 20 seconds, user-agent `HYDRA-Screenshot/1.0`).
2. WHEN rendering completes successfully, THE Screenshot_Adapter SHALL write the PNG bytes to MinIO at key `hydra-screenshots/{yyyy}/{mm}/{dd}/{sha256(url)}.png`, compute a 64-bit `Perceptual_Hash` with `imagehash.phash`, compute the SHA-256 content hash, and emit a `NormalizedRecord` with `tier = Tier.VULNERABILITY_INTELLIGENCE`, `source_meta.adapter_type = "screenshot"`, and a payload containing `{url, http_status, title, content_hash, phash, minio_key, rendered_at, viewport}`.
3. IF Playwright raises a `TimeoutError`, a navigation error, or a TLS error, THEN THE Screenshot_Adapter SHALL NOT write a blob to MinIO, SHALL emit a `NormalizedRecord` with payload field `error` set to the exception class name, and SHALL record the failure under `hydra_adapter_fetch_total{status="failed", adapter_type="screenshot"}`.
4. THE Screenshot_Adapter SHALL respect the platform backpressure contract from P8: when `BackpressureMonitor.check("minio") == BLOCKED`, the adapter SHALL NOT fetch new URLs.
5. WHERE `EASSettings.screenshot.ocr_enabled` is `True`, THE Screenshot_Adapter SHALL run Tesseract OCR over the rendered image, truncate output to `EASSettings.screenshot.ocr_max_chars` (default 8192), and index the extracted text into the `hydra-screenshots` Elasticsearch index under the same `record_hash`.

**Correctness property:** **Capture determinism for metadata** — for a fixed mock HTML response, repeated invocations of the adapter produce identical `content_hash`, identical `phash`, and identical `viewport`. The `rendered_at` field is allowed to differ.

### Requirement 7: Screenshot Retrieval

**User Story:** As an analyst, I want to retrieve a screenshot by record hash, so that I can view the captured image in my tooling.

#### Acceptance Criteria

1. WHEN a client sends an authenticated GET to `/api/v1/images/{record_hash}`, THE Images_Router SHALL look up the screenshot record in `normalized_records` and return a streaming response with `Content-Type: image/png` sourced from MinIO at the stored `minio_key`.
2. WHERE the client includes the query parameter `metadata_only=true`, THE Images_Router SHALL return `APIResponse[ImageMetadataResponse]` with `{record_hash, url, http_status, phash, content_hash, rendered_at, viewport, minio_key, has_ocr, ocr_excerpt}` and SHALL NOT stream the blob.
3. IF no screenshot record exists for `record_hash`, THEN THE Images_Router SHALL return `404 Not Found` with error code `NOT_FOUND`.
4. IF the screenshot record exists but the MinIO object is missing, THEN THE Images_Router SHALL return `503 Service Unavailable` with error code `BLOB_UNAVAILABLE` and SHALL emit a `hydra_storage_health_status{engine="minio"}` degraded signal through P12.

### Requirement 8: Perceptual-Hash Similarity Search

**User Story:** As an analyst, I want to search for screenshots visually similar to a given hash, so that I can find related exposures and template-based mass exposures.

#### Acceptance Criteria

1. WHEN a client sends an authenticated GET to `/api/v1/images/search?phash={hex}&similarity={value}`, THE Images_Router SHALL parse `phash` as a 16-character lowercase hexadecimal string, require `0.0 <= similarity <= 1.0`, and return screenshot records with `Hamming_Similarity(stored_phash, query_phash) >= similarity`, sorted by similarity descending and paginated with the standard cursor.
2. IF `phash` is not a 16-character lowercase hexadecimal string, THEN THE Images_Router SHALL return `422 Unprocessable Entity` with error code `VALIDATION_ERROR` and MUST NOT execute any database query.
3. WHERE the optional `tiers`, `since`, or `url_contains` query parameters are provided, THE Images_Router SHALL apply them as additional filters before similarity scoring.
4. THE Images_Router SHALL limit each response to `EASSettings.images_search_max_results` (default 500) rows even when `similarity == 0.0`.

**Correctness properties:**
- **Similarity symmetry**: `Hamming_Similarity(a, b) == Hamming_Similarity(b, a)` for any two 64-bit phashes.
- **Similarity bounds**: `0.0 <= Hamming_Similarity(a, b) <= 1.0` for any two 64-bit phashes and `Hamming_Similarity(a, a) == 1.0`.
- **Threshold monotonicity**: For thresholds `t1 <= t2`, the result set for `t2` is a subset of the result set for `t1` over the same underlying data.

---

## Capability 3 — CVE & Exploit Enrichment

### Requirement 9: Vulnerability Tier Ingestion

**User Story:** As an analyst, I want CVE, EPSS, KEV, ExploitDB, and Metasploit data available as first-class HYDRA records, so that I can correlate vulnerabilities with the rest of my data.

#### Acceptance Criteria

1. THE Tier enum SHALL include `VULNERABILITY_INTELLIGENCE = 29` and the stream registry SHALL register streams `nvd-cve`, `first-epss`, `cisa-kev`, `exploitdb`, and `metasploit-modules` under Tier 29.
2. WHEN a Tier 29 adapter fetches NVD CVE data, THE adapter SHALL emit a `NormalizedRecord` per CVE with payload fields `{cve_id, published, last_modified, cvss_v3_score, cvss_v3_vector, cwe_ids, references, affected_cpes, description}` and `raw_hash = xxhash64(f"nvd:{cve_id}:{last_modified}")`.
3. WHEN a Tier 29 adapter fetches EPSS data, THE adapter SHALL emit one `NormalizedRecord` per CVE per day with payload fields `{cve_id, epss_score, epss_percentile, score_date}`.
4. WHEN a Tier 29 adapter fetches CISA KEV data, THE adapter SHALL emit one `NormalizedRecord` per KEV entry with payload fields `{cve_id, vendor, product, date_added, due_date, required_action, known_ransomware_use}`.
5. WHEN a Tier 29 adapter fetches ExploitDB entries, THE adapter SHALL emit one `NormalizedRecord` per entry with payload fields `{exploit_id, title, type, platform, published_date, author, cve_ids, source_url}`.
6. WHEN a Tier 29 adapter fetches Metasploit module metadata, THE adapter SHALL emit one `NormalizedRecord` per module with payload fields `{module_path, module_type, rank, disclosure_date, cve_ids, description, platforms}`.
7. THE Monitoring_Subsystem SHALL expose a new Prometheus metric `hydra_eas_cve_records_total` with label `source in {"nvd", "epss", "kev", "exploitdb", "metasploit"}` for each record persisted.

### Requirement 10: CVE Correlation Pipeline

**User Story:** As an analyst, I want vulnerable assets automatically identified by correlating CVE records against fingerprints in ingested data, so that I do not have to hand-join these datasets.

#### Acceptance Criteria

1. THE CorrelationEngine SHALL register a fourth pipeline with `pipeline_id = "cve_correlation"` that consumes Tier 29 CVE records and fingerprint-bearing records from Tier 16 (cyber threat intel), Tier 17 (social/web OSINT), and Tier 28 (national portal index), following the P9 pipeline contract.
2. WHEN the CVE_Pipeline runs, THE CVE_Pipeline SHALL match a CVE record `C` to a fingerprint record `R` when any CPE in `C.payload.affected_cpes` matches the `(vendor, product, version)` triple extractable from `R.payload.fingerprint` under `EASSettings.cve_fingerprint_map`.
3. WHEN the CVE_Pipeline finds a match, THE CVE_Pipeline SHALL write a `CorrelationResult` to `correlation_results` with `pipeline_id = "cve_correlation"`, `record_a_hash = C.raw_hash`, `record_b_hash = R.raw_hash`, `confidence = min(1.0, 0.5 + 0.1 * cvss_v3_score)`, and `evidence = {cpe_match, cvss_v3_score, epss_score?, kev_listed?}`.
4. WHEN the CVE_Pipeline finds an asset-related match for a fingerprint record tied to a tenant asset, THE CVE_Pipeline SHALL emit an exposure event via the AssetMonitor with severity derived from `EASSettings.cve_severity_map`.
5. THE CVE_Pipeline SHALL be idempotent: running the pipeline twice on the same input set produces the same set of `CorrelationResult` rows identified by the natural key `(pipeline_id, record_a_hash, record_b_hash)`.

**Correctness properties:**
- **Correlation determinism**: Identical `(C, R)` inputs produce identical `confidence`, `evidence`, and natural key.
- **CPE match symmetry not required**: CPE matching is intentionally directional (CVE → fingerprint). Property tests SHALL assert that running the pipeline with swapped `(C, R)` inputs does NOT generate correlations.

### Requirement 11: CVE and Exploit Query Endpoints

**User Story:** As an analyst, I want CVE and exploit lookup endpoints, so that I can query vulnerability data without writing SQL.

#### Acceptance Criteria

1. WHEN a client sends an authenticated GET to `/api/v1/cves/{cve_id}` where `cve_id` matches `^CVE-\d{4}-\d{4,7}$`, THE CVEs_Router SHALL return the latest `nvd-cve` record for that ID joined with the latest `first-epss` and `cisa-kev` records if present, wrapped in `APIResponse[CVEDetailResponse]`.
2. IF `cve_id` does not match the CVE identifier regex, THEN THE CVEs_Router SHALL return `422 Unprocessable Entity` with error code `VALIDATION_ERROR`.
3. WHEN a client sends an authenticated GET to `/api/v1/cves/search` with any of the query parameters `vendor`, `product`, `min_cvss`, `kev_only`, `published_after`, `published_before`, THE CVEs_Router SHALL return matching CVEs paginated with the standard cursor.
4. WHEN a client sends an authenticated GET to `/api/v1/cves/{cve_id}/affected-assets`, THE CVEs_Router SHALL return the tenant-owned assets for the caller's `tenant_id` that have at least one exposure event tied to a correlation result with `pipeline_id = "cve_correlation"` for the given `cve_id`.
5. WHEN a client sends an authenticated GET to `/api/v1/exploits/search` with any of the query parameters `cve_id`, `platform`, `type`, `published_after`, THE Exploits_Router SHALL return matching ExploitDB and Metasploit records paginated with the standard cursor.
6. THE CVEs_Router and Exploits_Router SHALL use the `read` rate-limit tier on GET endpoints.

---

## Capability 4 — Geospatial Exploration API

### Requirement 12: Bounding-Box Feature Query

**User Story:** As an analyst, I want to query records by bounding box and render them on a map, so that I can explore geographic patterns in the data.

#### Acceptance Criteria

1. WHEN a client sends an authenticated GET to `/api/v1/maps/features?bbox={min_lon},{min_lat},{max_lon},{max_lat}`, THE Maps_Router SHALL parse the four floats, require `-180.0 <= min_lon <= max_lon <= 180.0` and `-90.0 <= min_lat <= max_lat <= 90.0`, query PostGIS for records whose `geo` geometry intersects the bbox, and return a valid GeoJSON `FeatureCollection` wrapped in `APIResponse[FeatureCollectionResponse]`.
2. IF the `bbox` parameter fails parsing or fails the coordinate ordering validation, THEN THE Maps_Router SHALL return `422 Unprocessable Entity` with error code `VALIDATION_ERROR` and SHALL NOT execute any PostGIS query.
3. WHERE the query parameters `tier`, `time_start`, `time_end`, `min_confidence`, or `tag` are provided, THE Maps_Router SHALL apply them as additional filters before tile aggregation.
4. WHEN the number of intersecting records exceeds `EASSettings.maps_feature_limit` (default 5000) and no `zoom` parameter is provided, THE Maps_Router SHALL return `413 Payload Too Large` with error code `BBOX_TOO_BROAD` and a response body field `hint` containing the text `supply a zoom parameter to enable server-side aggregation`.

### Requirement 13: Tile Aggregation

**User Story:** As a frontend developer, I want server-side clustering for zoomed-out map views, so that I do not have to render tens of thousands of points client-side.

#### Acceptance Criteria

1. WHERE the `zoom` query parameter is supplied in the range `0 <= zoom <= 18`, THE Tile_Aggregator SHALL group records into cells using `EASSettings.maps_aggregation_strategy` (either `"geohash"` with precision derived from zoom or `"h3"` with resolution derived from zoom) and return one feature per non-empty cell containing `{cell_id, centroid, count, tier_breakdown, dominant_tag}`.
2. THE Tile_Aggregator SHALL map zoom levels to precisions/resolutions monotonically: higher zoom produces finer cells. Specifically, `precision(zoom+1) >= precision(zoom)` and `precision(zoom+1) - precision(zoom) in {0, 1}`.
3. WHEN the same bbox query is served at zoom `z1` and zoom `z2` with `z1 < z2`, THE Tile_Aggregator SHALL ensure that the sum of `count` values at zoom `z1` equals the sum of `count` values at zoom `z2` for the same filter set.
4. THE Tile_Aggregator SHALL cap output at `EASSettings.maps_tile_max_cells` (default 2000) cells per response, selecting the highest-count cells when truncation is required.

**Correctness properties:**
- **Count conservation under re-aggregation**: For any bbox and filter set, `sum(cell.count for cell in response)` equals the count of matching records up to truncation by `maps_tile_max_cells`.
- **Zoom monotonicity**: For the same bbox and filter set, increasing `zoom` does not decrease the number of returned cells and does not increase the `count` of any single cell beyond its zoom-`z1` value.

---

## Capability 5 — Historical Trends API

### Requirement 14: Time-Series Trend Queries

**User Story:** As an analyst, I want to query historical time-series with server-side aggregation, so that I can analyze trends without pulling raw data.

#### Acceptance Criteria

1. WHEN a client sends an authenticated GET to `/api/v1/trends` with query parameters `stream_ids`, `time_start`, `time_end`, `bucket`, and `aggregation`, THE Trends_Router SHALL query InfluxDB for the given streams, downsample to the requested `bucket` in `{"1m","5m","15m","1h","6h","1d","7d"}`, apply the requested `aggregation` in `{"count","sum","mean","min","max","p50","p95","p99"}`, and return `APIResponse[TrendResponse]` where `TrendResponse.series` is a dict keyed by `stream_id` mapping to an ordered list of `{bucket_start, value}` points.
2. IF `time_start >= time_end`, THEN THE Trends_Router SHALL return `422 Unprocessable Entity` with error code `INVALID_TIME_WINDOW`.
3. IF the requested window exceeds `EASSettings.trends_max_window_days` (default 365) for bucket `1m` or `5m`, THEN THE Trends_Router SHALL return `422 Unprocessable Entity` with error code `WINDOW_TOO_LARGE` indicating the maximum window per bucket.
4. WHERE the query parameter `compare_to` is provided as `"previous_period"`, THE Trends_Router SHALL additionally return a `comparison` series computed over the immediately preceding window of equal length and SHALL return a per-bucket `delta` series where `delta = current - comparison`.
5. THE Trends_Router SHALL fall back to PostgreSQL aggregation when `StorageHealth("influxdb").status == UNREACHABLE`, returning `207 Multi-Status` with a `fallback: true` flag in the response meta.

**Correctness properties:**
- **Bucket alignment monotonicity**: For a fixed query, increasing `bucket` size does not increase the number of returned points and preserves total sum for aggregation `"sum"`.
- **Aggregation monotonicity for sum/count**: Over any fixed window, `sum(sum_per_bucket)` equals `sum(raw_values_in_window)` and `sum(count_per_bucket)` equals `count(raw_values_in_window)`.
- **Aggregation bounds**: For any bucket and raw point set, `min(raw) <= min_per_bucket <= mean_per_bucket <= max_per_bucket <= max(raw)` and `p50 <= p95 <= p99`.

### Requirement 15: Job Progress Endpoint

**User Story:** As an API consumer, I want progress metadata on long-running jobs, so that I can render meaningful progress bars.

#### Acceptance Criteria

1. THE JobStatus model SHALL be extended with optional fields `progress_current: int | None`, `progress_total: int | None`, and `eta_seconds: float | None`, persisted in the `hydra:job:*` Redis records alongside existing fields.
2. WHEN a long-running job worker calls `JobManager.update_progress(job_id, current, total)`, THE JobManager SHALL recompute `eta_seconds` as `max(0, (total - current) * elapsed / max(current, 1))` where `elapsed` is seconds since `created_at`.
3. WHEN a client sends an authenticated GET to `/api/v1/jobs/{job_id}/progress`, THE Jobs_Router SHALL return `APIResponse[JobProgressResponse]` with `{status, progress_current, progress_total, progress_ratio, eta_seconds, created_at, updated_at}` where `progress_ratio = progress_current / progress_total` when `progress_total > 0` and `None` otherwise.
4. IF `job_id` is unknown or its Redis record has expired, THEN THE Jobs_Router SHALL return `404 Not Found` with error code `JOB_NOT_FOUND`.
5. THE Jobs_Router SHALL preserve backward compatibility with the existing `/api/v1/products/jobs/{job_id}` and `/api/v1/correlations/jobs/{job_id}` endpoints by sharing a single `JobManager` instance.

**Correctness property:** **Progress monotonicity** — across successive `update_progress` calls for a given `job_id`, `progress_current` never decreases and `progress_ratio` is in `[0.0, 1.0]`.

---

## Capability 6 — Fast Indicator Lookup

### Requirement 16: Indicator Normalization

**User Story:** As an API consumer, I want indicator lookups to accept common notation variants, so that I do not need to pre-format values.

#### Acceptance Criteria

1. WHEN a client sends an authenticated GET to `/api/v1/lookup/{indicator}`, THE Lookup_Router SHALL classify `indicator` as `ipv4`, `ipv6`, `domain`, `hostname`, or `hash` via `EASSettings.indicator_classifier` and return `422 Unprocessable Entity` with error code `VALIDATION_ERROR` when no class matches.
2. WHEN the classifier returns `ipv4` or `ipv6`, THE Lookup_Router SHALL normalize the indicator by using `ipaddress.ip_address(value).compressed` before cache lookup.
3. WHEN the classifier returns `domain` or `hostname`, THE Lookup_Router SHALL normalize the indicator to lowercase ASCII (IDNA if necessary) and strip a single trailing dot before cache lookup.
4. WHEN the classifier returns `hash`, THE Lookup_Router SHALL accept 16-character lowercase hex (xxhash64 `raw_hash`), 32-character hex (MD5), 40-character hex (SHA-1), or 64-character hex (SHA-256).

**Correctness property:** **Normalization fixpoint** — `normalize(normalize(x)) == normalize(x)` for every indicator string accepted by the classifier.

### Requirement 17: Lookup Response and Cache

**User Story:** As an API consumer, I want a single round-trip to retrieve everything HYDRA knows about an indicator, so that dashboards and automations can stay responsive.

#### Acceptance Criteria

1. WHEN the Lookup_Router receives a valid normalized indicator and the Indicator_Lookup_Cache has a fresh entry under key `hydra:eas:lookup:{indicator_class}:{normalized_value}`, THE Lookup_Router SHALL return the cached `APIResponse[LookupResponse]` and SHALL set the response meta field `cache = "hit"`.
2. WHEN the Indicator_Lookup_Cache has no fresh entry, THE Lookup_Router SHALL assemble `LookupResponse` with fields `{indicator, indicator_class, records, tags, cve_correlations, screenshots, first_seen, last_seen, asset_reference}` by querying PostgreSQL (records + tags), correlation_results (cve_correlation pipeline), and screenshot metadata, shall write the result into the cache with TTL `EASSettings.lookup_cache_ttl_seconds` (default 300), and SHALL set the response meta field `cache = "miss"`.
3. THE Indicator_Lookup_Cache SHALL be bounded by `EASSettings.lookup_cache_max_entries` (default 100000) using Redis LRU eviction configured via `maxmemory-policy allkeys-lru` on the cache namespace.
4. WHEN the Lookup_Router serves a cache hit, THE Lookup_Router SHALL target `EASSettings.lookup_p95_latency_ms_target` (default 100) for the p95 response latency, as measured by the existing `http_request_duration_seconds` histogram filtered to `handler=/api/v1/lookup/{indicator}`.
5. THE `LookupResponse.asset_reference` SHALL be populated only when the indicator matches an asset owned by the caller's `tenant_id`; in all other cases the field SHALL be `None` to prevent tenant-data leakage.
6. THE Monitoring_Subsystem SHALL expose Prometheus counters `hydra_eas_lookup_cache_hits_total` and `hydra_eas_lookup_cache_misses_total` and a gauge `hydra_eas_lookup_cache_size`.

**Correctness properties:**
- **Cache idempotency**: Repeated lookups of the same normalized indicator within the TTL return byte-identical payloads except for the `cache` meta field.
- **Tenant-isolation invariant**: For any two tenants and any indicator, the `records`, `tags`, `cve_correlations`, and `screenshots` fields are equal, and only `asset_reference` may differ.

---

## Capability 7 — Exposure Observatory

### Requirement 18: Exposure Posture Report Product

**User Story:** As an analyst, I want a daily per-country exposure posture report, so that I can track aggregate trends and compare regions.

#### Acceptance Criteria

1. THE AnalysisEngine SHALL register a fourth product generator with `product_type = "exposure_posture_report"` that conforms to the P10 `BaseProduct` contract and whose `source_tiers` include Tier 16, Tier 17, Tier 19, Tier 28, and Tier 29.
2. WHEN the ExposureObservatory runs, THE ExposureObservatory SHALL aggregate data per ISO 3166-1 alpha-2 `country_code` using the `region` field of the source records and produce an `IntelligenceProduct` with sections `{overview, service_exposure_breakdown, vulnerability_density, trend_deltas, top_cves, top_exposed_assets}`.
3. THE ExposureObservatory SHALL compute a numeric `posture_score` per country in `[0, 100]` using `EASSettings.posture_score_weights`, higher score indicating worse posture.
4. THE ExposureObservatory SHALL compute `trend_deltas` as the difference between the current day's score and the prior-day score, expressed as `{absolute_delta, percent_delta}` and bounded to `[-100, 100]` for `absolute_delta`.
5. WHEN the ExposureObservatory completes, THE AnalysisEngine SHALL persist the `IntelligenceProduct` in `intelligence_products` with `parameters.country_codes` listing the countries covered.
6. WHERE `EASSettings.observatory.publish_snapshot_minio = True`, THE ExposureObservatory SHALL additionally write a JSON snapshot to MinIO at `hydra-observatory/{yyyy}/{mm}/{dd}/posture.json`.

### Requirement 19: Scheduled Generation and Retrieval

**User Story:** As a platform operator, I want the exposure posture report to be generated on a reliable daily cadence, and to be retrievable through the existing products API, so that consumption does not require a new endpoint shape.

#### Acceptance Criteria

1. THE scheduler SHALL register a new Airflow DAG `dags/eas_observatory_daily.py` that runs the ExposureObservatory once per day under the existing P8 DAG factory, with the cadence tag `daily` and owner `hydra-eas`.
2. WHEN the DAG completes successfully, THE DAG SHALL log a single `INFO` line of the form `posture_report_generated product_id=<uuid> countries=<n>` and SHALL emit `hydra_eas_observatory_runs_total{status="success"}`.
3. IF the DAG fails, THEN THE DAG SHALL emit `hydra_eas_observatory_runs_total{status="failed"}` and THE existing P12 `HydraJobFailureRate` alert SHALL apply without modification.
4. WHEN a client sends an authenticated GET to `/api/v1/products?product_type=exposure_posture_report`, THE existing P11 Products_Router SHALL return exposure posture products with the same pagination and envelope rules as for the other product types.
5. WHEN a client sends an authenticated GET to `/api/v1/observatory/countries/{country_code}` for a valid ISO 3166-1 alpha-2 code, THE Observatory_Router SHALL return the most recent per-country section data extracted from the latest posture report.
6. IF the ISO 3166-1 alpha-2 code is invalid or the latest report does not cover the country, THEN THE Observatory_Router SHALL return `404 Not Found` with error code `NOT_FOUND`.

**Correctness property:** **Score determinism** — for an identical input set of records and correlations, two successive `ExposureObservatory.generate` invocations produce identical `posture_score` values per country.

---

## Cross-cutting Requirements

### Requirement 20: Tenant Identity and Authorization

**User Story:** As a platform operator, I want every EAS endpoint to enforce tenant-scoped authorization, so that tenants cannot read or modify each other's data.

#### Acceptance Criteria

1. THE `api_keys` table SHALL be extended with a `tenant_id UUID NOT NULL` column migrated via Alembic, backfilled to the existing API keys' owning tenants during migration.
2. WHEN the P11 `get_current_api_key` dependency resolves an API key, THE dependency SHALL additionally load the `tenant_id` into `APIKeyRecord.tenant_id` and make it available to all EAS routers via `Depends(get_current_tenant_id)`.
3. THE Assets_Router, Images_Router, Jobs_Router, and Lookup_Router SHALL filter all read queries by `tenant_id = APIKeyRecord.tenant_id` for any table that carries a `tenant_id` column.
4. IF a tenant attempts to read or modify a row whose `tenant_id` differs from its own, THEN the router SHALL return `404 Not Found` with error code `NOT_FOUND` and SHALL NOT disclose the row's existence.
5. THE CVE, Exploit, Maps, Trends, and Observatory endpoints SHALL remain tenant-agnostic for read operations since their underlying data is not tenant-owned.

### Requirement 21: Rate-Limit Tiers

**User Story:** As a platform operator, I want EAS endpoints to participate in the existing rate-limiting framework with appropriate tier assignments, so that expensive operations do not starve cheap ones.

#### Acceptance Criteria

1. THE P11 `Rate_Tier` enum SHALL be extended with a new tier `expensive` with default `rate_limit_expensive = 2 req/min` and `rate_limit_expensive_burst = 1` configurable via `HYDRA__API__RATE_LIMIT_EXPENSIVE`.
2. THE RateLimitMiddleware SHALL assign the `expensive` tier to `POST /api/v1/assets/{asset_id}/screenshot` (on-demand screenshot capture), `POST /api/v1/observatory/generate` (on-demand observatory regeneration), and `POST /api/v1/cves/correlate` (on-demand CVE pipeline run).
3. THE RateLimitMiddleware SHALL assign the `read` tier to all EAS GET endpoints and the `write` tier to asset CRUD and watchlist-style mutations.
4. THE RateLimitMiddleware SHALL include `X-RateLimit-Limit`, `X-RateLimit-Remaining`, and `X-RateLimit-Reset` headers on every EAS response without change to the existing header format.

### Requirement 22: Cost Controls

**User Story:** As a platform operator, I want per-tenant cost controls on the expensive EAS operations, so that a single tenant cannot exhaust the platform budget.

#### Acceptance Criteria

1. THE EAS module SHALL enforce the following per-tenant daily quotas from `EASSettings.cost_quota`: `screenshots_per_day` (default 500), `observatory_regenerations_per_day` (default 5), `lookup_requests_per_day` (default 100000), `trends_points_per_day` (default 10000000 returned points).
2. WHEN a tenant exceeds a daily quota, THE responsible router SHALL return `429 Too Many Requests` with error code `COST_QUOTA_EXCEEDED`, a `Retry-After` header set to seconds until UTC midnight, and a response body that names the exhausted quota.
3. THE cost counter SHALL be tracked in Redis under key `hydra:eas:cost:{tenant_id}:{quota_name}:{yyyymmdd}` with TTL 48 hours (to cover timezone edge cases without unbounded growth).
4. THE Monitoring_Subsystem SHALL expose `hydra_eas_quota_usage_ratio` as a gauge labelled `{tenant_id, quota_name}` with values in `[0.0, 1.0]` and SHALL fire the alert `HydraEASQuotaNearExhaustion` when any ratio exceeds `0.9` for 15 minutes.

### Requirement 23: Observability

**User Story:** As a platform operator, I want the EAS module to emit the same quality of Prometheus metrics and log lines as the rest of the platform, so that existing dashboards and runbooks apply without rewrites.

#### Acceptance Criteria

1. THE EAS module SHALL register the following Prometheus metrics following the P12 `hydra_{subsystem}_{name}_{unit}` naming convention: `hydra_eas_exposure_events_total`, `hydra_eas_screenshot_captures_total`, `hydra_eas_cve_records_total`, `hydra_eas_lookup_cache_hits_total`, `hydra_eas_lookup_cache_misses_total`, `hydra_eas_lookup_cache_size`, `hydra_eas_quota_usage_ratio`, `hydra_eas_observatory_runs_total`, `hydra_eas_trends_window_bytes`, and `hydra_eas_maps_tiles_returned`.
2. THE EAS module SHALL emit structured JSON log lines through the existing P12 logging configuration with an additional `eas_component` field set to one of `assets`, `images`, `cves`, `maps`, `trends`, `lookup`, `observatory`.
3. THE EAS module SHALL register one new Alertmanager rule file `prometheus/rules/hydra_eas_alerts.yml` containing the alerts `HydraEASCriticalExposure`, `HydraEASScreenshotFailureRate`, `HydraEASLookupCacheHitRateLow`, `HydraEASQuotaNearExhaustion`, and `HydraEASObservatoryStale`.
4. THE EAS module SHALL register one SLO `eas_lookup_p95_latency` with target `0.99` over a 7-day window measuring the fraction of `/api/v1/lookup/{indicator}` requests whose `http_request_duration_seconds` is less than `EASSettings.lookup_p95_latency_ms_target / 1000`.

### Requirement 24: Storage Migrations

**User Story:** As a platform operator, I want all new EAS tables to be created via Alembic migrations with indexes that match the query patterns, so that deployments are reproducible and query performance is acceptable.

#### Acceptance Criteria

1. THE EAS module SHALL add Alembic migrations under `alembic/versions/eas_00X_*.py` creating the tables `assets(asset_id UUID PK, tenant_id UUID, asset_type TEXT, normalized_value TEXT, raw_value TEXT, is_active BOOLEAN, created_at TIMESTAMPTZ, deactivated_at TIMESTAMPTZ)`, `asset_exposures(exposure_id UUID PK, asset_id UUID FK, record_hash TEXT, tier INTEGER, matched_indicator TEXT, severity TEXT, created_at TIMESTAMPTZ)`, and `exposure_alert_deliveries(delivery_id UUID PK, exposure_id UUID FK, receiver TEXT, delivered_at TIMESTAMPTZ, status TEXT)`.
2. THE `assets` table SHALL have a unique index on `(tenant_id, asset_type, normalized_value) WHERE is_active = TRUE` to enforce Requirement 1.3.
3. THE `asset_exposures` table SHALL have an index on `(asset_id, created_at DESC)` and a partial unique index on `(asset_id, record_hash, matched_indicator)` to enforce Requirement 3.3.
4. THE Alembic migration for `api_keys.tenant_id` SHALL be reversible (explicit `downgrade()` preserving existing rows).
5. THE EAS module SHALL register the new Elasticsearch indices `hydra-screenshots` and `hydra-cves` with explicit mappings in `src/hydra/eas/storage/es_mappings.py`.

### Requirement 25: Configuration

**User Story:** As a platform operator, I want EAS tunables grouped under a single configuration section, so that deployments use a predictable override pattern.

#### Acceptance Criteria

1. THE `HydraSettings` class SHALL be extended with a nested `eas: EASSettings` section following the P11/P12 pattern, overridable via environment variables `HYDRA__EAS__<FIELD>`.
2. THE `EASSettings` model SHALL expose the defaults referenced elsewhere in this document: `asset_quota_per_tenant = 1000`, `exposure_matching_tiers = [16, 17, 28, 29]`, `exposure_dedup_ttl_seconds = 86400`, `screenshot.viewport = (1280, 800)`, `screenshot.timeout_seconds = 20`, `screenshot.ocr_enabled = False`, `images_search_max_results = 500`, `maps_feature_limit = 5000`, `maps_tile_max_cells = 2000`, `maps_aggregation_strategy = "h3"`, `trends_max_window_days = 365`, `lookup_cache_ttl_seconds = 300`, `lookup_cache_max_entries = 100000`, `lookup_p95_latency_ms_target = 100`, `posture_score_weights = {...}`, and the `cost_quota` fields from Requirement 22.
3. THE `EASSettings` model SHALL validate that `exposure_matching_tiers` contains only integers in the range `[1, 29]` and SHALL reject duplicates.
4. THE `EASSettings` model SHALL validate that `maps_aggregation_strategy` is one of `{"geohash", "h3"}` and SHALL reject any other value.

### Requirement 26: External Dependencies

**User Story:** As a platform operator, I want new external dependencies to be explicit and locked, so that builds are reproducible.

#### Acceptance Criteria

1. THE EAS module SHALL pin the following new runtime dependencies in `pyproject.toml` under a new optional extra `[eas]`: `playwright`, `imagehash`, `h3`, `python-geohash`, `nvdlib`, `cvelib`, `pytesseract` (only when OCR is enabled).
2. WHERE `EASSettings.screenshot.ocr_enabled` is `False`, THE system SHALL NOT require `pytesseract` or the Tesseract binary at runtime.
3. THE EAS Docker image SHALL install the Playwright Chromium binary during build via `playwright install --with-deps chromium` and SHALL NOT download browsers at runtime.
4. THE EAS module SHALL fail fast at startup with a clear log message when a capability is enabled but its external dependency is not available.

---

## Requirement 27: Cross-cutting Correctness Properties (PBT Consolidation)

**User Story:** As a platform operator, I want the critical EAS correctness properties called out in one place, so that property-based tests can be written consistently.

#### Acceptance Criteria

1. THE EAS test suite SHALL include a property-based test for **pagination round-trip invariance** on the endpoints `/api/v1/assets`, `/api/v1/assets/{asset_id}/exposures`, `/api/v1/exposures`, `/api/v1/images/search`, `/api/v1/cves/search`, and `/api/v1/exploits/search`: for any generator-produced dataset and page-size sequence, concatenating follow-cursor pages yields the same multiset as an unpaginated scan.
2. THE EAS test suite SHALL include a property-based test for **asset-registration idempotency**: for any valid input tuple, two sequential POSTs produce the same `asset_id` and exactly one row.
3. THE EAS test suite SHALL include a property-based test for **normalization fixpoint** on asset `normalized_value` and indicator normalization: `normalize(normalize(x)) == normalize(x)`.
4. THE EAS test suite SHALL include a property-based test for **perceptual-hash similarity symmetry and bounds**: for random 64-bit hash pairs, `Hamming_Similarity(a, b) == Hamming_Similarity(b, a)` and the result is in `[0.0, 1.0]` with `Hamming_Similarity(a, a) == 1.0`.
5. THE EAS test suite SHALL include a property-based test for **tile count conservation**: for any randomized dataset and bbox, `sum(cell.count)` equals the matching record count up to the documented truncation limit.
6. THE EAS test suite SHALL include a property-based test for **time-series aggregation monotonicity**: for any generated raw time-series, `sum(sum_per_bucket)` equals `sum(raw_values)` and `min(raw) <= min_per_bucket <= mean_per_bucket <= max_per_bucket <= max(raw)` per bucket.
7. THE EAS test suite SHALL include a property-based test for **CVE correlation determinism**: for any fixed `(CVE record set, fingerprint record set)` input, two runs of the CVE_Pipeline produce identical `CorrelationResult` sets under the natural key `(pipeline_id, record_a_hash, record_b_hash)`.
8. THE EAS test suite SHALL include a property-based test for **lookup tenant-isolation invariance**: for any indicator and any two tenants, the `records`, `tags`, `cve_correlations`, and `screenshots` fields of `LookupResponse` are equal, differing only in `asset_reference`.
9. THE EAS test suite SHALL include a property-based test for **job progress monotonicity**: for any sequence of `update_progress` calls, `progress_current` is non-decreasing and `progress_ratio` is in `[0.0, 1.0]`.
10. THE EAS test suite SHALL include a property-based test for **exposure-matching dedup invariance**: for any multiset of identical `(asset_id, record_hash, matched_indicator)` triples submitted to the AssetMonitor, the resulting number of `asset_exposures` rows is at most 1.

---

## Dependencies Summary

- **P0 (Project Scaffold & Shared Contracts):** `NormalizedRecord`, `Tier` enum (extended with `VULNERABILITY_INTELLIGENCE = 29`), `HydraSettings` (extended with `EASSettings`), `GeoGeometry`, `SourceMeta`.
- **P7 (Storage Layer):** `PostgresEngine`, `RedisCache`, `ElasticsearchEngine`, `MinIOEngine`, `Neo4jEngine` (unchanged). New Alembic migrations and ES index mappings added.
- **P8 (Scheduler / Orchestration):** DAG factory (unchanged). One new DAG `dags/eas_observatory_daily.py`. AssetMonitor hooks into the existing ingestion write path.
- **P9 (Correlation Engine):** `CorrelationEngine` extended with pipeline #4 `cve_correlation` via the existing `BasePipeline` contract.
- **P10 (Analysis / Intelligence Products):** `AnalysisEngine` extended with product #4 `exposure_posture_report` via the existing `BaseProduct` contract. No changes to SITREP, Dossier, or Threat Assessment.
- **P11 (API Layer):** FastAPI app factory extended with seven new routers (`assets`, `images`, `cves`, `exploits`, `maps`, `trends`, `jobs`, `lookup`, `observatory`). `JobStatus` extended with progress fields. `APIKeyRecord` extended with `tenant_id`. New `expensive` rate-limit tier. Everything else (envelope, cursor, job manager, auth dependency) reused without modification.
- **P12 (Monitoring & Alerting):** New Prometheus metrics, one new alert rule file, one new SLO, and new Alertmanager receivers. No changes to the core monitoring framework.

## External Dependencies Summary

- `playwright` with Chromium for headless rendering (Capability 2).
- `imagehash` for perceptual hashing (Capability 2).
- `pytesseract` plus the Tesseract binary, enabled by config only (Capability 2).
- `h3` or `python-geohash` for spatial clustering (Capability 4).
- `nvdlib` and/or `cvelib` for NVD / MITRE CVE ingestion (Capability 3).
- MinIO, PostGIS, Redis, Elasticsearch, InfluxDB — already in the platform; no new storage engines.
