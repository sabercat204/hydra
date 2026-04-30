"""Common API schemas — envelope, pagination, errors, jobs."""

from __future__ import annotations

from typing import Any, Generic, Literal, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class PaginationMeta(BaseModel):
    next_cursor: str | None = None
    has_more: bool = False
    total_estimate: int | None = None


class ResponseMeta(BaseModel):
    request_id: str
    timestamp: str
    duration_ms: float
    pagination: PaginationMeta | None = None


class APIError(BaseModel):
    code: str
    message: str
    detail: dict[str, Any] | None = None


class APIResponse(BaseModel, Generic[T]):
    data: T
    meta: ResponseMeta | None = None
    errors: list[APIError] | None = None


class JobStatus(BaseModel):
    job_id: str
    status: Literal["pending", "running", "completed", "failed"]
    progress: float | None = None
    result_id: str | None = None
    error: str | None = None
    created_at: str
    updated_at: str


class PaginationParams(BaseModel):
    cursor: str | None = None
    limit: int = Field(50, ge=1, le=500)
