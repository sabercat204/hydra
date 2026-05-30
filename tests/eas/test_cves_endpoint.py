"""Integration tests for ``GET /api/v1/cves/*`` endpoints (task 9.9 pt 1).

Exercises :class:`CVERepository` directly with a FakeES. The router
is a thin ``Depends``-driven wrapper over these methods — the
interesting surface is (a) the ES query shape each method builds and
(b) the result projection into Pydantic schemas. Driving the repo
directly avoids the FastAPI + auth layer that's already covered by
other tests (task 11.11, 13.10) and keeps these tests focused.

Scenarios:

1. **``get_cve_detail`` — NVD + EPSS + KEV merge (R11.1).** Seed
   three ES docs for the same CVE (one per source) and assert the
   merged response pulls fields from each.
2. **``get_cve_detail`` — NVD-missing returns None.** A lookup with
   EPSS + KEV but no NVD doc returns ``None``.
3. **``search_cves`` — filter subset preservation (Property 5).**
   Seed CVEs with varying vendor/product/min_cvss/kev_only; each
   filter produces a subset of the unfiltered result set.
4. **``search_cves`` — pagination round-trip (Property 6).** Walk
   pages of the full seed and assert concatenation equals the
   single-shot fetch.

Validates: R11.1, R11.3, R27.1 (Property 5, Property 6).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from hydra.eas.cves.repository import CVERepository
from hydra.eas.schemas.cves import CVESearchParams


# ---------------------------------------------------------------------------
# FakeES — supports the repository's query shape (filter bool + sort)
# ---------------------------------------------------------------------------


_EPOCH = datetime(2024, 1, 1, tzinfo=timezone.utc)


class FakeES:
    """In-memory Elasticsearch double.

    Accepts a list of canonical docs (dicts) and serves them back
    through :meth:`search`. The fake implements just enough of the
    query DSL for the CVE repository:

    * ``bool.filter`` with ``term`` / ``terms`` / ``range``.
    * ``sort`` with nested ``{"field": {"order": "desc"}}``.
    * ``search_after`` for cursor pagination.
    * ``size`` for page size.
    """

    def __init__(self, docs: list[dict[str, Any]] | None = None) -> None:
        # Each seeded doc includes ``_id`` + ``_source`` fields so the
        # return shape matches ES 8.x ``ObjectApiResponse.body``.
        self.docs: list[dict[str, Any]] = list(docs or [])

    def seed(self, doc: dict[str, Any]) -> None:
        self.docs.append(doc)

    async def search(
        self,
        *,
        index: str,
        query: dict[str, Any] | None = None,
        size: int = 10,
        sort: list[dict[str, Any]] | None = None,
        search_after: list[Any] | None = None,
    ) -> dict[str, Any]:
        del index  # unused — CVE repo only queries hydra-cves

        filters = (query or {}).get("bool", {}).get("filter", [])
        matching = [d for d in self.docs if _filters_match(d["_source"], filters)]

        if sort:
            matching.sort(key=_make_sort_key(sort))

        if search_after:
            matching = [
                d for d in matching
                if _after(_source_for_sort(d["_source"], sort), search_after, sort)
            ]

        return {"hits": {"hits": matching[:size]}}


def _filters_match(source: dict[str, Any], filters: list[dict[str, Any]]) -> bool:
    """Apply the ``bool.filter`` clauses to a seeded doc."""

    for clause in filters:
        if "term" in clause:
            field, value = next(iter(clause["term"].items()))
            field_value = source.get(field)
            # ES ``term`` on a list-valued field matches when ANY
            # element equals the filter value. Mimic that so filters
            # like ``term: {cve_ids: "CVE-..."}`` work against docs
            # that store ``cve_ids`` as a list.
            if isinstance(field_value, list):
                if value not in field_value:
                    return False
            elif field_value != value:
                return False
        elif "terms" in clause:
            field, values = next(iter(clause["terms"].items()))
            field_value = source.get(field)
            if isinstance(field_value, list):
                if not any(v in values for v in field_value):
                    return False
            elif field_value not in values:
                return False
        elif "range" in clause:
            field, bounds = next(iter(clause["range"].items()))
            raw_value = source.get(field)
            if raw_value is None:
                return False
            value = _coerce_comparable(raw_value)
            if "gte" in bounds:
                gte_val = _coerce_comparable(bounds["gte"])
                if value < gte_val:
                    return False
            if "lte" in bounds:
                lte_val = _coerce_comparable(bounds["lte"])
                if value > lte_val:
                    return False
    return True


def _coerce_comparable(value: Any) -> Any:
    """Normalize datetime-ish values for range comparisons.

    ES accepts ISO strings for date ranges; we compare against the
    doc's ``datetime`` or ``float`` values by coercing strings to
    datetime when the source looks like a timestamp.
    """

    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        iso = value.replace("Z", "+00:00") if value.endswith("Z") else value
        try:
            dt = datetime.fromisoformat(iso)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return value
    return value


def _source_for_sort(source: dict[str, Any], sort: list[dict[str, Any]] | None) -> list[Any]:
    """Extract the sort-key tuple for ``search_after`` comparison."""

    if not sort:
        return []
    keys: list[Any] = []
    for clause in sort:
        field = next(iter(clause.keys()))
        keys.append(_coerce_comparable(source.get(field)))
    return keys


def _make_sort_key(sort: list[dict[str, Any]]) -> Any:
    """Build the Python sort key function mirroring the ES sort spec."""

    def key_fn(doc: dict[str, Any]) -> tuple:
        source = doc["_source"]
        parts = []
        for clause in sort:
            field, spec = next(iter(clause.items()))
            raw = source.get(field)
            parts.append(_coerce_comparable(raw) if raw is not None else "")
        return tuple(parts)

    # All sort clauses in the CVE repo are DESC; we reverse at the
    # caller via ``reverse=True``.
    return lambda d: tuple(_reverse_if_desc(k) for k in key_fn(d))


class _Reverser:
    """Wraps a value so the list sort is DESC without having to call reverse()."""

    __slots__ = ("value",)

    def __init__(self, value: Any) -> None:
        self.value = value

    def __lt__(self, other: "_Reverser") -> bool:
        return self.value > other.value

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _Reverser) and self.value == other.value


def _reverse_if_desc(value: Any) -> Any:
    """Every sort clause in the CVE repo is DESC — wrap for reverse order."""

    return _Reverser(value)


def _after(
    source_keys: list[Any],
    search_after_values: list[Any],
    sort: list[dict[str, Any]] | None,
) -> bool:
    """Return True when ``source_keys`` sorts strictly AFTER search_after
    under the sort's DESC ordering — i.e. ``source_keys < search_after``
    under normal ordering.

    This mirrors ES's ``search_after`` semantics for a DESC sort.
    """

    del sort  # every clause is DESC for these queries
    if not search_after_values:
        return True
    normalized = [_coerce_comparable(v) for v in search_after_values]
    # DESC means "older rows come later", so we want source < cursor.
    return tuple(source_keys) < tuple(normalized)


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


def _nvd_doc(
    cve_id: str,
    *,
    cvss: float | None = 7.5,
    vendor: str = "apache",
    product: str = "httpd",
    published: datetime | None = None,
    kev_listed: bool = False,
) -> dict[str, Any]:
    return {
        "_id": f"{cve_id}:nvd",
        "_source": {
            "source": "nvd",
            "cve_id": cve_id,
            "published": (published or _EPOCH).isoformat(),
            "last_modified": (published or _EPOCH).isoformat(),
            "cvss_v3_score": cvss,
            "cvss_v3_vector": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H",
            "cwe_ids": ["CWE-79"],
            "references": ["https://example.com/ref"],
            "affected_cpes": [f"cpe:2.3:a:{vendor}:{product}:*:*:*:*:*:*:*:*"],
            "description": f"Vulnerability in {vendor} {product}",
            "cpe_vendor": vendor,
            "cpe_product": product,
            "kev_listed": kev_listed,
        },
    }


def _epss_doc(cve_id: str, *, score: float, percentile: float) -> dict[str, Any]:
    return {
        "_id": f"{cve_id}:epss",
        "_source": {
            "source": "epss",
            "cve_id": cve_id,
            "epss_score": score,
            "epss_percentile": percentile,
            "score_date": _EPOCH.isoformat(),
            "last_modified": (_EPOCH + timedelta(hours=1)).isoformat(),
        },
    }


def _kev_doc(cve_id: str, *, known_ransomware_use: bool = False) -> dict[str, Any]:
    return {
        "_id": f"{cve_id}:kev",
        "_source": {
            "source": "kev",
            "cve_id": cve_id,
            "due_date": (_EPOCH + timedelta(days=30)).isoformat(),
            "known_ransomware_use": known_ransomware_use,
            "last_modified": (_EPOCH + timedelta(hours=2)).isoformat(),
        },
    }


# ---------------------------------------------------------------------------
# get_cve_detail
# ---------------------------------------------------------------------------


async def test_get_cve_detail_merges_nvd_epss_kev() -> None:
    """R11.1 — the detail view combines all three source docs into one response."""

    es = FakeES()
    cve_id = "CVE-2024-0001"
    es.seed(_nvd_doc(cve_id, cvss=9.1))
    es.seed(_epss_doc(cve_id, score=0.8, percentile=0.95))
    es.seed(_kev_doc(cve_id, known_ransomware_use=True))

    detail = await CVERepository.get_cve_detail(es, cve_id)

    assert detail is not None
    # NVD base fields.
    assert detail.cve_id == cve_id
    assert detail.cvss_v3_score == pytest.approx(9.1)
    assert detail.affected_cpes == ["cpe:2.3:a:apache:httpd:*:*:*:*:*:*:*:*"]
    # EPSS fields.
    assert detail.epss_score == pytest.approx(0.8)
    assert detail.epss_percentile == pytest.approx(0.95)
    # KEV fields.
    assert detail.kev_listed is True
    assert detail.known_ransomware_use is True
    assert detail.kev_due_date is not None


async def test_get_cve_detail_without_nvd_returns_none() -> None:
    """A CVE with only EPSS or KEV but no NVD doc → 404 (``None`` here)."""

    es = FakeES()
    cve_id = "CVE-2024-0002"
    es.seed(_epss_doc(cve_id, score=0.5, percentile=0.7))
    es.seed(_kev_doc(cve_id))

    detail = await CVERepository.get_cve_detail(es, cve_id)
    assert detail is None


async def test_get_cve_detail_without_epss_is_still_returned() -> None:
    """NVD alone is enough for a detail response — EPSS/KEV are optional."""

    es = FakeES()
    cve_id = "CVE-2024-0003"
    es.seed(_nvd_doc(cve_id))

    detail = await CVERepository.get_cve_detail(es, cve_id)
    assert detail is not None
    assert detail.epss_score is None
    assert detail.kev_listed is False


async def test_get_cve_detail_missing_cve_returns_none() -> None:
    """An unknown CVE id returns ``None``."""

    es = FakeES()
    assert await CVERepository.get_cve_detail(es, "CVE-2099-9999") is None


# ---------------------------------------------------------------------------
# search_cves — filter subset preservation
# ---------------------------------------------------------------------------


def _seed_search_corpus(es: FakeES) -> None:
    """Seed a small corpus of 9 CVEs covering the filter surface."""

    # Vendor x product x cvss matrix — 3 apache/httpd, 3 nginx/nginx, 3 mit/openssl.
    # Published dates stagger so pagination ordering is deterministic.
    entries = [
        ("CVE-2024-1001", "apache", "httpd", 9.8, True, 1),
        ("CVE-2024-1002", "apache", "httpd", 7.5, False, 2),
        ("CVE-2024-1003", "apache", "httpd", 5.3, False, 3),
        ("CVE-2024-1004", "nginx", "nginx", 8.2, True, 4),
        ("CVE-2024-1005", "nginx", "nginx", 6.1, False, 5),
        ("CVE-2024-1006", "nginx", "nginx", 4.0, False, 6),
        ("CVE-2024-1007", "mit", "openssl", 9.1, True, 7),
        ("CVE-2024-1008", "mit", "openssl", 7.0, False, 8),
        ("CVE-2024-1009", "mit", "openssl", 3.5, False, 9),
    ]
    for cve_id, vendor, product, cvss, kev, day_offset in entries:
        es.seed(
            _nvd_doc(
                cve_id,
                cvss=cvss,
                vendor=vendor,
                product=product,
                published=_EPOCH + timedelta(days=day_offset),
                kev_listed=kev,
            )
        )


async def test_search_cves_no_filters_returns_all() -> None:
    """Baseline — no filter returns every seeded NVD doc (Property 5)."""

    es = FakeES()
    _seed_search_corpus(es)
    rows, _ = await CVERepository.search_cves(
        es, CVESearchParams(), cursor=None, limit=100
    )
    assert {r.cve_id for r in rows} == {f"CVE-2024-{1001 + i}" for i in range(9)}


async def test_search_cves_vendor_filter_subsets_correctly() -> None:
    """``vendor="apache"`` returns only the 3 Apache CVEs (Property 5)."""

    es = FakeES()
    _seed_search_corpus(es)
    rows, _ = await CVERepository.search_cves(
        es, CVESearchParams(vendor="apache"), cursor=None, limit=100
    )
    assert {r.cve_id for r in rows} == {"CVE-2024-1001", "CVE-2024-1002", "CVE-2024-1003"}


async def test_search_cves_product_filter_subsets_correctly() -> None:
    """``product="openssl"`` returns only the 3 OpenSSL CVEs."""

    es = FakeES()
    _seed_search_corpus(es)
    rows, _ = await CVERepository.search_cves(
        es, CVESearchParams(product="openssl"), cursor=None, limit=100
    )
    assert {r.cve_id for r in rows} == {"CVE-2024-1007", "CVE-2024-1008", "CVE-2024-1009"}


async def test_search_cves_min_cvss_filter_subsets_correctly() -> None:
    """``min_cvss=9.0`` returns only the 2 high-scoring CVEs."""

    es = FakeES()
    _seed_search_corpus(es)
    rows, _ = await CVERepository.search_cves(
        es, CVESearchParams(min_cvss=9.0), cursor=None, limit=100
    )
    # CVE-1001 (9.8) and CVE-1007 (9.1) — both >=9.0.
    assert {r.cve_id for r in rows} == {"CVE-2024-1001", "CVE-2024-1007"}


async def test_search_cves_kev_only_filter_subsets_correctly() -> None:
    """``kev_only=True`` returns only the 3 KEV-listed CVEs."""

    es = FakeES()
    _seed_search_corpus(es)
    rows, _ = await CVERepository.search_cves(
        es, CVESearchParams(kev_only=True), cursor=None, limit=100
    )
    assert {r.cve_id for r in rows} == {
        "CVE-2024-1001",
        "CVE-2024-1004",
        "CVE-2024-1007",
    }


async def test_search_cves_combined_filters_intersect() -> None:
    """``vendor="apache" + min_cvss=7.0`` returns the intersection."""

    es = FakeES()
    _seed_search_corpus(es)
    rows, _ = await CVERepository.search_cves(
        es,
        CVESearchParams(vendor="apache", min_cvss=7.0),
        cursor=None,
        limit=100,
    )
    # CVE-1001 (9.8) and CVE-1002 (7.5); CVE-1003 (5.3) excluded.
    assert {r.cve_id for r in rows} == {"CVE-2024-1001", "CVE-2024-1002"}


# ---------------------------------------------------------------------------
# search_cves — pagination round-trip
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("limit", [1, 2, 4])
async def test_search_cves_pagination_roundtrip(limit: int) -> None:
    """Property 6 — walking all pages yields the full corpus."""

    es = FakeES()
    _seed_search_corpus(es)

    collected: list[str] = []
    cursor: str | None = None
    iters = 0
    while True:
        iters += 1
        rows, cursor = await CVERepository.search_cves(
            es, CVESearchParams(), cursor=cursor, limit=limit
        )
        collected.extend(r.cve_id for r in rows)
        if cursor is None:
            break
        assert iters <= 20  # safety net

    # Every CVE appears exactly once.
    assert sorted(collected) == sorted(
        {f"CVE-2024-{1001 + i}" for i in range(9)}
    )
    assert len(collected) == 9


async def test_search_cves_filter_plus_cursor_preserves_filter() -> None:
    """Filters survive cursor transitions: vendor filter + small limit."""

    es = FakeES()
    _seed_search_corpus(es)

    collected: list[str] = []
    cursor: str | None = None
    while True:
        rows, cursor = await CVERepository.search_cves(
            es, CVESearchParams(vendor="nginx"), cursor=cursor, limit=1
        )
        collected.extend(r.cve_id for r in rows)
        if cursor is None:
            break

    assert sorted(collected) == ["CVE-2024-1004", "CVE-2024-1005", "CVE-2024-1006"]


async def test_search_cves_empty_result_has_no_cursor() -> None:
    """A filter that matches zero rows returns ``(rows=[], cursor=None)``."""

    es = FakeES()
    _seed_search_corpus(es)
    rows, cursor = await CVERepository.search_cves(
        es,
        CVESearchParams(vendor="nosuchvendor"),
        cursor=None,
        limit=10,
    )
    assert rows == []
    assert cursor is None
