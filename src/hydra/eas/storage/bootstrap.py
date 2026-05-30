"""Elasticsearch index bootstrap for EAS (R24.5).

``bootstrap_eas_indices`` is an idempotent helper called from
:func:`hydra.eas.setup.setup_eas` at application startup. It creates the
``hydra-screenshots`` and ``hydra-cves`` indexes with the mappings from
:mod:`hydra.eas.storage.es_mappings` if they do not already exist.

Idempotency is enforced by checking ``indices.exists`` before issuing a
``PUT``, so a second call within the same process (or across restarts)
never attempts to re-create an existing index.

Deliberately no retries: an Elasticsearch outage at boot is a deployment
error that should fail loudly rather than silently start serving requests
against an incomplete catalog. The upstream caller (``setup_eas``) is
responsible for wrapping this with a circuit breaker if desired.
"""

from __future__ import annotations

import logging
from typing import Any

from hydra.eas.storage.es_mappings import (
    ALL_INDEX_MAPPINGS,
    HYDRA_CVES_INDEX,
    HYDRA_SCREENSHOTS_INDEX,
)

logger = logging.getLogger(__name__)

__all__ = [
    "create_index_if_absent",
    "bootstrap_eas_indices",
]


async def create_index_if_absent(
    es_client: Any,
    name: str,
    mapping: dict[str, Any],
) -> bool:
    """Create ``name`` in Elasticsearch with ``mapping`` iff it doesn't exist.

    Returns ``True`` when a new index was created, ``False`` when the index
    was already present. The check-then-create pattern is inherently racy
    across multiple processes, so we tolerate the ``resource_already_exists``
    error that Elasticsearch returns on concurrent creation and treat it as
    equivalent to "index was already present".

    Parameters
    ----------
    es_client:
        An ``AsyncElasticsearch`` (or equivalent) instance exposing the
        ``indices.exists`` and ``indices.create`` coroutines used here. The
        type is deliberately ``Any`` so we do not force a hard import of
        ``elasticsearch`` at module load — tests pass in fakes directly.
    name:
        The index name to check / create.
    mapping:
        The full index body (``{"settings": ..., "mappings": ...}``) to pass
        to ``indices.create`` when creation is required.
    """

    # ``indices.exists`` returns a bool-y value in the 8.x async client.
    exists = await es_client.indices.exists(index=name)
    if _is_truthy(exists):
        logger.debug("eas.bootstrap.index_exists", extra={"index": name})
        return False

    try:
        await es_client.indices.create(index=name, body=mapping)
    except Exception as exc:  # noqa: BLE001 — we log & narrow below
        message = str(exc).lower()
        # Elasticsearch returns 400 resource_already_exists_exception when two
        # callers race to create the same index. That is equivalent to
        # "already present" for our purposes, so we treat it as a no-op.
        if "resource_already_exists" in message or "already exists" in message:
            logger.debug(
                "eas.bootstrap.index_create_race", extra={"index": name}
            )
            return False
        logger.error(
            "eas.bootstrap.index_create_failed",
            extra={"index": name, "error": str(exc)},
        )
        raise

    logger.info("eas.bootstrap.index_created", extra={"index": name})
    return True


async def bootstrap_eas_indices(es_client: Any) -> dict[str, bool]:
    """Ensure both EAS Elasticsearch indexes exist.

    Returns a dict mapping each index name to a bool: ``True`` when the
    index was freshly created on this call, ``False`` when it was already
    present. The mapping is useful for emitting structured start-up logs in
    ``setup_eas``.
    """

    results: dict[str, bool] = {}
    for name in (HYDRA_SCREENSHOTS_INDEX, HYDRA_CVES_INDEX):
        mapping = ALL_INDEX_MAPPINGS[name]
        created = await create_index_if_absent(es_client, name, mapping)
        results[name] = created
    return results


def _is_truthy(value: Any) -> bool:
    """Treat both 8.x ``ObjectApiResponse`` and plain bools as truthy.

    The 8.x async client returns a lightweight wrapper object that is truthy
    when the underlying HTTP call returned 200 and falsy on 404 — but some
    mocks / fakes just return a bare bool. This helper normalises both.
    """

    if isinstance(value, bool):
        return value
    # Most ``ObjectApiResponse`` wrappers support ``.body`` or ``__bool__``.
    try:
        return bool(value)
    except Exception:  # noqa: BLE001 — defensive
        return False
