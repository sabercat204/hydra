"""Integration tests for Elasticsearch bootstrap idempotency (task 6.3).

The acceptance criterion (R24.5) is that running
:func:`hydra.eas.storage.bootstrap.bootstrap_eas_indices` twice against an
Elasticsearch instance must not error and must leave mappings stable.

We do not spin up a real ES instance — the bootstrap module only uses two
async methods from ``es_client.indices`` (``exists`` and ``create``), so a
tiny in-memory fake gives us deterministic coverage of the
check-then-create contract:

* first call — both ``hydra-screenshots`` and ``hydra-cves`` get created
  with the exact bodies from
  :data:`hydra.eas.storage.es_mappings.ALL_INDEX_MAPPINGS`
* second call — both are detected as already-present and
  :meth:`FakeIndicesClient.create` is never re-invoked
* partial pre-seed — only the missing index is created
* race path — a ``resource_already_exists_exception`` raised from
  ``create`` is swallowed and treated as equivalent to "index was already
  present"
* non-race errors propagate
* ``exists`` is always consulted before ``create``
* wrapper-style truthy responses (mimicking the 8.x
  ``ObjectApiResponse``) are handled correctly

Validates: R24.5.
"""

from __future__ import annotations

from typing import Any

import pytest

from hydra.eas.storage.bootstrap import (
    bootstrap_eas_indices,
    create_index_if_absent,
)
from hydra.eas.storage.es_mappings import (
    ALL_INDEX_MAPPINGS,
    HYDRA_CVES_INDEX,
    HYDRA_SCREENSHOTS_INDEX,
)


# ---------------------------------------------------------------------------
# In-memory fakes
# ---------------------------------------------------------------------------


class FakeIndicesClient:
    """Minimal stand-in for ``AsyncElasticsearch.indices``.

    Tracks every ``exists`` and ``create`` call so tests can assert on call
    order and payloads. ``create`` raises a ``resource_already_exists``-
    shaped error when the index is already present, mirroring the real 8.x
    behaviour.
    """

    def __init__(self) -> None:
        # name -> mapping body of every index currently "present".
        self.indices: dict[str, dict[str, Any]] = {}
        self.exists_calls: list[str] = []
        self.create_calls: list[tuple[str, dict[str, Any]]] = []

    async def exists(self, index: str) -> bool:
        self.exists_calls.append(index)
        return index in self.indices

    async def create(self, index: str, body: dict[str, Any]) -> None:
        self.create_calls.append((index, body))
        if index in self.indices:
            raise Exception(
                f"resource_already_exists_exception: index [{index}] already exists"
            )
        self.indices[index] = body


class FakeElasticsearch:
    """ES client surface: exposes ``.indices`` only — bootstrap needs nothing else."""

    def __init__(self) -> None:
        self.indices = FakeIndicesClient()


# ---------------------------------------------------------------------------
# Happy path — fresh instance
# ---------------------------------------------------------------------------


async def test_first_call_creates_both_indices() -> None:
    """First call against an empty ES creates both EAS indexes with the
    canonical mappings from :mod:`hydra.eas.storage.es_mappings`.
    """

    es = FakeElasticsearch()
    results = await bootstrap_eas_indices(es)

    assert results == {
        HYDRA_SCREENSHOTS_INDEX: True,
        HYDRA_CVES_INDEX: True,
    }
    # Both indices present with the exact mapping bodies.
    assert HYDRA_SCREENSHOTS_INDEX in es.indices.indices
    assert HYDRA_CVES_INDEX in es.indices.indices
    assert (
        es.indices.indices[HYDRA_SCREENSHOTS_INDEX]
        == ALL_INDEX_MAPPINGS[HYDRA_SCREENSHOTS_INDEX]
    )
    assert (
        es.indices.indices[HYDRA_CVES_INDEX]
        == ALL_INDEX_MAPPINGS[HYDRA_CVES_INDEX]
    )


# ---------------------------------------------------------------------------
# Idempotency — the R24.5 guarantee
# ---------------------------------------------------------------------------


async def test_second_call_is_idempotent() -> None:
    """Calling ``bootstrap_eas_indices`` twice does not error and the second
    call reports ``False`` for every index (already present).
    """

    es = FakeElasticsearch()
    first = await bootstrap_eas_indices(es)
    second = await bootstrap_eas_indices(es)

    assert first == {HYDRA_SCREENSHOTS_INDEX: True, HYDRA_CVES_INDEX: True}
    assert second == {HYDRA_SCREENSHOTS_INDEX: False, HYDRA_CVES_INDEX: False}
    # Only the first call ever reached ``create`` — two creates total, not four.
    assert len(es.indices.create_calls) == 2


async def test_mappings_unchanged_on_second_call() -> None:
    """The index bodies stored after the first call are byte-identical after
    the second call — the bootstrap never reaches ``create`` again and
    therefore never overwrites mappings.
    """

    es = FakeElasticsearch()
    await bootstrap_eas_indices(es)
    screenshots_snapshot = dict(es.indices.indices[HYDRA_SCREENSHOTS_INDEX])
    cves_snapshot = dict(es.indices.indices[HYDRA_CVES_INDEX])

    await bootstrap_eas_indices(es)

    assert es.indices.indices[HYDRA_SCREENSHOTS_INDEX] == screenshots_snapshot
    assert es.indices.indices[HYDRA_CVES_INDEX] == cves_snapshot


# ---------------------------------------------------------------------------
# Partial state
# ---------------------------------------------------------------------------


async def test_partial_state_only_creates_missing() -> None:
    """When one index exists and the other does not, bootstrap creates only
    the missing one and reports accordingly.
    """

    es = FakeElasticsearch()
    # Pre-seed the screenshots index only.
    es.indices.indices[HYDRA_SCREENSHOTS_INDEX] = ALL_INDEX_MAPPINGS[
        HYDRA_SCREENSHOTS_INDEX
    ]

    results = await bootstrap_eas_indices(es)

    assert results == {
        HYDRA_SCREENSHOTS_INDEX: False,  # already existed
        HYDRA_CVES_INDEX: True,  # freshly created
    }
    # Exactly one create call — for the CVEs index.
    assert [c[0] for c in es.indices.create_calls] == [HYDRA_CVES_INDEX]


# ---------------------------------------------------------------------------
# Race tolerance
# ---------------------------------------------------------------------------


class RacingIndicesClient:
    """Simulates the concurrent-boot race.

    ``exists`` always reports the index as absent, forcing the create path,
    but ``create`` loses the race and raises the
    ``resource_already_exists_exception`` that real ES produces on
    concurrent creation.
    """

    def __init__(self) -> None:
        self.indices: dict[str, dict[str, Any]] = {}

    async def exists(self, index: str) -> bool:
        return False

    async def create(self, index: str, body: dict[str, Any]) -> None:
        raise Exception(
            f"resource_already_exists_exception: index [{index}] already exists"
        )


class RacingES:
    def __init__(self) -> None:
        self.indices = RacingIndicesClient()


async def test_create_tolerates_race_condition() -> None:
    """``create_index_if_absent`` must swallow ``resource_already_exists``
    and return ``False`` — the index is effectively present regardless of
    which caller won the race.
    """

    es = RacingES()
    result = await create_index_if_absent(es, "test-index", {"mappings": {}})
    assert result is False


# ---------------------------------------------------------------------------
# Non-race errors must still propagate
# ---------------------------------------------------------------------------


class BrokenIndicesClient:
    async def exists(self, index: str) -> bool:
        return False

    async def create(self, index: str, body: dict[str, Any]) -> None:
        raise Exception("connection refused")


class BrokenES:
    def __init__(self) -> None:
        self.indices = BrokenIndicesClient()


async def test_non_race_error_propagates() -> None:
    """Any error from ``create`` that is *not* the race-condition exception
    must bubble up — a misconfigured or unreachable cluster is a loud
    deployment failure, not something bootstrap silently papers over.
    """

    es = BrokenES()
    with pytest.raises(Exception, match="connection refused"):
        await create_index_if_absent(es, "test-index", {"mappings": {}})


# ---------------------------------------------------------------------------
# Ordering — exists() comes before create()
# ---------------------------------------------------------------------------


async def test_exists_check_called_before_create() -> None:
    """For each index, ``exists`` is consulted once before ``create`` is
    invoked. Order across indexes matches the ``ALL_INDEX_MAPPINGS``
    iteration order.
    """

    es = FakeElasticsearch()
    await bootstrap_eas_indices(es)

    assert es.indices.exists_calls == [
        HYDRA_SCREENSHOTS_INDEX,
        HYDRA_CVES_INDEX,
    ]
    assert [c[0] for c in es.indices.create_calls] == [
        HYDRA_SCREENSHOTS_INDEX,
        HYDRA_CVES_INDEX,
    ]


# ---------------------------------------------------------------------------
# Wrapper-response handling (8.x AsyncElasticsearch behaviour)
# ---------------------------------------------------------------------------


class _Wrapper:
    """Mimic an ``ObjectApiResponse``-ish wrapper with a ``__bool__`` hook."""

    def __init__(self, val: bool) -> None:
        self.val = val

    def __bool__(self) -> bool:
        return self.val


class WrapperIndicesClient:
    """``exists`` returns a wrapper object instead of a bare ``bool`` —
    mirrors what the real 8.x async elasticsearch client does.
    """

    def __init__(self) -> None:
        self.indices: dict[str, dict[str, Any]] = {}

    async def exists(self, index: str) -> _Wrapper:
        return _Wrapper(index in self.indices)

    async def create(self, index: str, body: dict[str, Any]) -> None:
        self.indices[index] = body


class WrapperES:
    def __init__(self) -> None:
        self.indices = WrapperIndicesClient()


async def test_wrapper_response_is_handled() -> None:
    """``create_index_if_absent`` coerces a non-bool wrapper via
    ``bool()`` / ``__bool__``, so both the absent and present cases are
    detected correctly.
    """

    es = WrapperES()
    created = await create_index_if_absent(es, "test-idx", {"mappings": {}})
    assert created is True

    # Second call — ``exists`` now returns a truthy wrapper, so no re-create.
    created_again = await create_index_if_absent(
        es, "test-idx", {"mappings": {}}
    )
    assert created_again is False
