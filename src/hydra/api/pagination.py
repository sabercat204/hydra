"""Cursor-based pagination — encode/decode and query builder."""

from __future__ import annotations

import base64
import json
from typing import Any

from hydra.api.errors import ErrorCode, HydraAPIException
from hydra.api.schemas.common import PaginationMeta, PaginationParams


def encode_cursor(sort_field: str, last_value: Any, last_id: str) -> str:
    """Encode pagination state as opaque URL-safe base64 cursor."""
    payload = json.dumps({"f": sort_field, "v": last_value, "id": last_id})
    return base64.urlsafe_b64encode(payload.encode()).decode()


def decode_cursor(cursor: str) -> tuple[str, Any, str]:
    """Decode opaque cursor to (sort_field, last_value, last_id).

    Raises 400 BAD_CURSOR if malformed.
    """
    try:
        raw = base64.urlsafe_b64decode(cursor.encode())
        data = json.loads(raw)
        return data["f"], data["v"], data["id"]
    except Exception:
        raise HydraAPIException(
            code=ErrorCode.BAD_CURSOR,
            message="Invalid pagination cursor",
            status_code=400,
        )


def build_paged_response(
    items: list[Any],
    limit: int,
    sort_field: str,
    get_sort_value: Any = None,
    get_id: Any = None,
) -> tuple[list[Any], PaginationMeta]:
    """Build pagination metadata from a list fetched with limit+1 strategy.

    Caller should fetch limit+1 items. If len > limit, has_more=True and
    we trim to limit, encoding a next_cursor from the last item.
    """
    has_more = len(items) > limit
    if has_more:
        items = items[:limit]

    next_cursor: str | None = None
    if has_more and items and get_sort_value and get_id:
        last = items[-1]
        next_cursor = encode_cursor(sort_field, get_sort_value(last), get_id(last))

    return items, PaginationMeta(
        next_cursor=next_cursor,
        has_more=has_more,
        total_estimate=None,
    )
