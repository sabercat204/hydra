"""Tests for pagination — cursor encode/decode, paginate_query."""

from __future__ import annotations

import pytest

from hydra.api.errors import HydraAPIException
from hydra.api.pagination import build_paged_response, decode_cursor, encode_cursor
from tests.conftest_api import *  # noqa: F401, F403

HEADERS = {"X-API-Key": "test-api-key-12345"}


def test_encode_decode_cursor_roundtrip():
    cursor = encode_cursor("created_at", "2026-01-01T00:00:00Z", "abc-123")
    field, value, id_ = decode_cursor(cursor)
    assert field == "created_at"
    assert value == "2026-01-01T00:00:00Z"
    assert id_ == "abc-123"


def test_decode_invalid_cursor():
    with pytest.raises(HydraAPIException) as exc_info:
        decode_cursor("not-valid-base64!!!")
    assert exc_info.value.code.value == "BAD_CURSOR"


def test_decode_tampered_cursor():
    import base64
    # Valid base64 but invalid JSON
    tampered = base64.urlsafe_b64encode(b"not json").decode()
    with pytest.raises(HydraAPIException) as exc_info:
        decode_cursor(tampered)
    assert exc_info.value.code.value == "BAD_CURSOR"


def test_paginate_query_first_page():
    items = [{"id": str(i), "name": f"item-{i}"} for i in range(6)]
    result, meta = build_paged_response(
        items, limit=5, sort_field="id",
        get_sort_value=lambda x: x["id"],
        get_id=lambda x: x["id"],
    )
    assert len(result) == 5
    assert meta.has_more is True
    assert meta.next_cursor is not None


def test_paginate_query_last_page():
    items = [{"id": str(i), "name": f"item-{i}"} for i in range(3)]
    result, meta = build_paged_response(
        items, limit=5, sort_field="id",
        get_sort_value=lambda x: x["id"],
        get_id=lambda x: x["id"],
    )
    assert len(result) == 3
    assert meta.has_more is False
    assert meta.next_cursor is None


def test_paginate_query_empty():
    result, meta = build_paged_response(
        [], limit=5, sort_field="id",
    )
    assert len(result) == 0
    assert meta.has_more is False
    assert meta.next_cursor is None


@pytest.mark.asyncio
async def test_paginate_query_limit_bounds(client):
    # limit=0 should fail
    resp = await client.get("/api/v1/registry/tiers?limit=0", headers=HEADERS)
    assert resp.status_code == 422

    # limit=501 should fail
    resp = await client.get("/api/v1/registry/tiers?limit=501", headers=HEADERS)
    assert resp.status_code == 422
