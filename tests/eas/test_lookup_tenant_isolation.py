"""Property 21 — Lookup tenant-isolation invariance (task 13.8).

The invariant (Design §2.3, §3.7, R17.5, R27.8):

> For any indicator and any two tenants, ``records``, ``tags``,
> ``cve_correlations``, ``screenshots``, ``first_seen``, and
> ``last_seen`` are byte-equal; only ``asset_reference`` may differ.

The assembler's fan-out is explicitly structured to preserve this:
records / tags / CVE / screenshot queries are tenant-agnostic,
while only the ``asset_reference`` branch consults
:class:`AssetRepository.get_active_by_key` with ``tenant_id``. The
tests below drive :class:`LookupAssembler.assemble` with two different
tenant IDs over the same fake storage and assert the non-tenant
fields are identical across both calls.

Validates: R17.5, R27.8.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import pytest
from hypothesis import HealthCheck, given, settings as h_settings, strategies as st

from hydra.eas.assets.models import Asset
from hydra.eas.assets.repository import AssetRepository
from hydra.eas.lookup.assembler import LookupAssembler
from hydra.eas.schemas.lookup import LookupAssetReference, LookupResponse


# ---------------------------------------------------------------------------
# Fake storage — deterministic, tenant-agnostic PG / ES doubles
# ---------------------------------------------------------------------------


class _FakePgConnection:
    """Duck-typed asyncpg ``Connection`` for the assembler's PG fan-out.

    The assembler issues two queries: one for the record list, one for
    the ``MIN(timestamp)/MAX(timestamp)`` span. We dispatch on the SQL
    text so the fake returns the right shape without parsing a full
    plan.
    """

    def __init__(self, pool: "_FakePgPool") -> None:
        self._pool = pool

    async def fetch(self, sql: str, *params: Any) -> list[dict[str, Any]]:
        # Record-list query — filter the seeded rows by the ILIKE
        # pattern and honour the LIMIT from the parameter list.
        pattern = str(params[0])
        limit = int(params[1]) if len(params) > 1 else 100
        matching = [r for r in self._pool.records if pattern_matches(r, pattern)]
        matching.sort(key=lambda r: r["timestamp"], reverse=True)
        return matching[:limit]

    async def fetchrow(self, sql: str, *params: Any) -> dict[str, Any] | None:
        pattern = str(params[0])
        matching = [r for r in self._pool.records if pattern_matches(r, pattern)]
        if not matching:
            return {"first_seen": None, "last_seen": None}
        ts = [r["timestamp"] for r in matching]
        return {"first_seen": min(ts), "last_seen": max(ts)}


class _FakePgPool:
    """In-memory ``asyncpg.Pool`` stand-in for the records / tags fan-out."""

    def __init__(self, seeded_records: list[dict[str, Any]] | None = None) -> None:
        # Each record carries the columns the assembler SELECTs plus a
        # ``_payload_text`` field used by ``pattern_matches`` to simulate
        # the ``payload::text ILIKE`` predicate.
        self.records: list[dict[str, Any]] = seeded_records or []

    def acquire(self) -> "_FakePgPool":
        return self

    async def __aenter__(self) -> _FakePgConnection:
        return _FakePgConnection(self)

    async def __aexit__(self, *exc: Any) -> None:
        return None


def pattern_matches(row: dict[str, Any], pattern: str) -> bool:
    """Substring match mirroring the ``payload::text ILIKE`` predicate."""

    needle = pattern.strip("%").lower()
    haystack = str(row.get("_payload_text", "")).lower()
    return needle in haystack


class _FakeESClient:
    """In-memory Elasticsearch double — supports only ``search``.

    The assembler calls ``search`` twice (CVEs, screenshots). We key
    the seeded responses on the index name so each call returns the
    right shape without a real query planner.
    """

    def __init__(
        self,
        *,
        cve_hits: list[dict[str, Any]] | None = None,
        screenshot_hits: list[dict[str, Any]] | None = None,
    ) -> None:
        self._responses = {
            "hydra-cves": {"hits": {"hits": cve_hits or []}},
            "hydra-screenshots": {"hits": {"hits": screenshot_hits or []}},
        }

    async def search(self, *, index: str, **kwargs: Any) -> dict[str, Any]:
        # The assembler reads ``response["hits"]["hits"]``; we return a
        # plain dict so the ``_unwrap_es_response`` helper picks it up
        # via the ``isinstance(resp, dict)`` branch.
        return self._responses.get(index, {"hits": {"hits": []}})


class _FakeAssetRepository(AssetRepository):
    """Stubs :meth:`AssetRepository.get_active_by_key` with per-tenant ownership.

    Takes a map ``{tenant_id: set[(asset_type, normalized_value)]}``.
    A call returns an :class:`Asset` iff the tenant owns the requested
    asset — which is how the assembler decides whether to populate
    ``asset_reference``.
    """

    def __init__(self, ownership: dict[UUID, set[tuple[str, str]]]) -> None:
        # Don't call the parent ``__init__`` — we don't need a pool
        # because only ``get_active_by_key`` is exercised.
        self._ownership = ownership

    async def get_active_by_key(  # type: ignore[override]
        self,
        tenant_id: UUID,
        asset_type: str,
        normalized_value: str,
    ) -> Asset | None:
        owned = self._ownership.get(tenant_id, set())
        if (asset_type, normalized_value) not in owned:
            return None
        return Asset(
            asset_id=uuid4(),
            tenant_id=tenant_id,
            asset_type=asset_type,
            normalized_value=normalized_value,
            raw_value=normalized_value,
            is_active=True,
            capture_screenshots=False,
            created_at=datetime.now(timezone.utc),
            deactivated_at=None,
            notes=None,
        )


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def _seed_records(indicator: str, n: int = 3) -> list[dict[str, Any]]:
    """Produce ``n`` seeded PG rows all matching ``indicator`` substring."""

    base = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    return [
        {
            "raw_hash": f"{i:016x}",
            "tier": 16,
            "stream_id": f"stream-{i}",
            "timestamp": base.replace(day=i + 1),
            "confidence": 0.75,
            "tags": ["threat", f"tag-{i}"],
            "_payload_text": f'{{"ip":"{indicator}"}}',
        }
        for i in range(n)
    ]


def _seed_cve_hits() -> list[dict[str, Any]]:
    return [
        {
            "_id": "CVE-2024-1234",
            "_source": {
                "cve_id": "CVE-2024-1234",
                "cvss_v3_score": 9.1,
                "kev_listed": True,
            },
        },
        {
            "_id": "CVE-2024-5678",
            "_source": {
                "cve_id": "CVE-2024-5678",
                "cvss_v3_score": 7.3,
                "kev_listed": False,
            },
        },
    ]


def _seed_screenshot_hits() -> list[dict[str, Any]]:
    return [
        {
            "_id": "ss-1",
            "_source": {
                "record_hash": "ss-1",
                "url": "https://example.com/page",
                "rendered_at": "2024-01-02T15:00:00+00:00",
                "phash": "a" * 16,
            },
        },
    ]


def _make_assembler(
    indicator: str,
    ownership: dict[UUID, set[tuple[str, str]]] | None = None,
    *,
    include_screenshots: bool = False,
    include_cves: bool = True,
) -> LookupAssembler:
    pg = _FakePgPool(seeded_records=_seed_records(indicator))
    es = _FakeESClient(
        cve_hits=_seed_cve_hits() if include_cves else [],
        screenshot_hits=_seed_screenshot_hits() if include_screenshots else [],
    )
    repo = _FakeAssetRepository(ownership or {})
    return LookupAssembler(pg, es, repo)


def _strip_asset(resp: LookupResponse) -> dict[str, Any]:
    """Return the tenant-agnostic slice of ``resp`` for byte-equality checks.

    Excludes only ``asset_reference`` — every other field is part of
    the invariant Property 21 asserts.
    """

    dumped = resp.model_dump(mode="json")
    dumped.pop("asset_reference", None)
    return dumped


# ---------------------------------------------------------------------------
# Property 21 — both tenants see identical non-asset fields
# ---------------------------------------------------------------------------


async def test_both_tenants_see_identical_non_asset_fields() -> None:
    """Two tenants asking the same indicator get identical records/CVE/screenshots.

    Seeds an ``ipv4`` lookup for ``10.0.0.1``. Tenant A owns that IP
    as an asset; tenant B does not. Expectation:

    * ``records`` arrays are byte-equal.
    * ``tags`` arrays are byte-equal.
    * ``cve_correlations`` are byte-equal.
    * ``screenshots`` are byte-equal.
    * ``first_seen`` / ``last_seen`` are byte-equal.
    * ``asset_reference`` is populated for A, ``None`` for B.

    Validates: R17.5, R27.8.
    """

    indicator = "10.0.0.1"
    tenant_a = uuid4()
    tenant_b = uuid4()
    assembler = _make_assembler(
        indicator,
        ownership={tenant_a: {("ip", indicator)}},
    )

    resp_a = await assembler.assemble("ipv4", indicator, tenant_a)
    resp_b = await assembler.assemble("ipv4", indicator, tenant_b)

    # Non-asset fields are byte-equal.
    assert _strip_asset(resp_a) == _strip_asset(resp_b)

    # asset_reference is the ONLY tenant-scoped field.
    assert resp_a.asset_reference is not None
    assert resp_a.asset_reference.normalized_value == indicator
    assert resp_a.asset_reference.asset_type == "ip"
    assert resp_b.asset_reference is None


async def test_record_list_byte_equal_across_tenants() -> None:
    """``records`` field is the same ordered list for both tenants."""

    indicator = "192.168.1.1"
    tenant_a = uuid4()
    tenant_b = uuid4()
    assembler = _make_assembler(indicator)

    resp_a = await assembler.assemble("ipv4", indicator, tenant_a)
    resp_b = await assembler.assemble("ipv4", indicator, tenant_b)

    assert [r.raw_hash for r in resp_a.records] == [
        r.raw_hash for r in resp_b.records
    ]
    # ``confidence`` and ``timestamp`` align too — byte equality per field.
    for row_a, row_b in zip(resp_a.records, resp_b.records):
        assert row_a.model_dump() == row_b.model_dump()


async def test_tags_byte_equal_across_tenants() -> None:
    """``tags`` aggregation is independent of the calling tenant."""

    indicator = "10.0.0.2"
    tenant_a = uuid4()
    tenant_b = uuid4()
    assembler = _make_assembler(indicator)

    resp_a = await assembler.assemble("ipv4", indicator, tenant_a)
    resp_b = await assembler.assemble("ipv4", indicator, tenant_b)

    assert resp_a.tags == resp_b.tags
    # Sanity: tags actually came through.
    assert resp_a.tags, "expected at least one tag seeded into the records"


async def test_cve_correlations_byte_equal_across_tenants() -> None:
    """CVE list / ordering / confidence is tenant-agnostic."""

    indicator = "10.0.0.3"
    tenant_a = uuid4()
    tenant_b = uuid4()
    assembler = _make_assembler(indicator)

    resp_a = await assembler.assemble("ipv4", indicator, tenant_a)
    resp_b = await assembler.assemble("ipv4", indicator, tenant_b)

    assert [c.cve_id for c in resp_a.cve_correlations] == [
        c.cve_id for c in resp_b.cve_correlations
    ]
    for c_a, c_b in zip(resp_a.cve_correlations, resp_b.cve_correlations):
        assert c_a.model_dump() == c_b.model_dump()


async def test_screenshots_byte_equal_across_tenants() -> None:
    """Screenshot list is tenant-agnostic for domain/hostname classes.

    IP classes skip the screenshot query by design, so we use the
    ``domain`` class to exercise the full assembler path.
    """

    indicator = "example.com"
    tenant_a = uuid4()
    tenant_b = uuid4()
    assembler = _make_assembler(indicator, include_screenshots=True)

    resp_a = await assembler.assemble("domain", indicator, tenant_a)
    resp_b = await assembler.assemble("domain", indicator, tenant_b)

    assert [s.record_hash for s in resp_a.screenshots] == [
        s.record_hash for s in resp_b.screenshots
    ]
    for s_a, s_b in zip(resp_a.screenshots, resp_b.screenshots):
        assert s_a.model_dump() == s_b.model_dump()


async def test_timestamps_byte_equal_across_tenants() -> None:
    """``first_seen`` / ``last_seen`` are derived from the same PG rows
    for both tenants and therefore must match exactly.
    """

    indicator = "10.0.0.4"
    tenant_a = uuid4()
    tenant_b = uuid4()
    assembler = _make_assembler(indicator)

    resp_a = await assembler.assemble("ipv4", indicator, tenant_a)
    resp_b = await assembler.assemble("ipv4", indicator, tenant_b)

    assert resp_a.first_seen == resp_b.first_seen
    assert resp_a.last_seen == resp_b.last_seen


# ---------------------------------------------------------------------------
# Property 21 (PBT) — arbitrary tenant pairs and asset ownership sets
# ---------------------------------------------------------------------------


@given(
    tenant_ids=st.lists(st.uuids(), min_size=2, max_size=6, unique=True),
    # ``owner_idx`` picks which tenant (if any) owns the asset.
    # ``-1`` means "nobody owns it".
    owner_idx=st.integers(min_value=-1, max_value=5),
)
@h_settings(
    max_examples=25,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
async def test_property_tenant_isolation_pbt(
    tenant_ids: list[UUID], owner_idx: int
) -> None:
    """For any set of tenant IDs and any single owner, the non-asset
    fields of ``assemble`` are byte-equal across every tenant, and only
    the owner sees a populated ``asset_reference``.

    Validates: R17.5, R27.8.
    """

    indicator = "10.0.0.100"
    owner: UUID | None = None
    if 0 <= owner_idx < len(tenant_ids):
        owner = tenant_ids[owner_idx]

    ownership: dict[UUID, set[tuple[str, str]]] = {}
    if owner is not None:
        ownership[owner] = {("ip", indicator)}

    assembler = _make_assembler(indicator, ownership=ownership)

    responses = {
        t: await assembler.assemble("ipv4", indicator, t) for t in tenant_ids
    }

    # Every response must have the same non-asset slice. Use the first
    # response as the reference.
    reference = _strip_asset(next(iter(responses.values())))
    for tenant, resp in responses.items():
        assert _strip_asset(resp) == reference, (
            f"non-asset fields differ for tenant {tenant}"
        )

    # Only the owner's response has a populated asset_reference.
    for tenant, resp in responses.items():
        if tenant == owner:
            assert resp.asset_reference is not None
            assert isinstance(resp.asset_reference, LookupAssetReference)
            assert resp.asset_reference.normalized_value == indicator
        else:
            assert resp.asset_reference is None


# ---------------------------------------------------------------------------
# Repeat-call determinism (tightens Property 21 to "same tenant twice")
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("tenant_owns_asset", [True, False])
async def test_same_tenant_twice_is_byte_equal(tenant_owns_asset: bool) -> None:
    """Two consecutive calls for the same (indicator, tenant) are equal.

    Aligns with Property 21: tenant A at time T and tenant A at time T+1
    return identical responses (modulo the freshly-minted ``asset_id``
    UUID, which we exclude from the comparison).

    Validates: R17.5, R27.8.
    """

    indicator = "10.0.0.5"
    tenant = uuid4()
    ownership = {tenant: {("ip", indicator)}} if tenant_owns_asset else {}
    assembler = _make_assembler(indicator, ownership=ownership)

    resp_a = await assembler.assemble("ipv4", indicator, tenant)
    resp_b = await assembler.assemble("ipv4", indicator, tenant)

    # Non-asset fields identical.
    assert _strip_asset(resp_a) == _strip_asset(resp_b)

    # asset_reference identity/absence is consistent.
    if tenant_owns_asset:
        assert resp_a.asset_reference is not None
        assert resp_b.asset_reference is not None
        # ``asset_id`` is freshly minted by the fake so only the type +
        # value need to match across calls.
        assert resp_a.asset_reference.asset_type == resp_b.asset_reference.asset_type
        assert (
            resp_a.asset_reference.normalized_value
            == resp_b.asset_reference.normalized_value
        )
    else:
        assert resp_a.asset_reference is None
        assert resp_b.asset_reference is None
