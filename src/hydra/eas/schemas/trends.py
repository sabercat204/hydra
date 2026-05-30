"""Trend and job-progress request/response schemas (Design §4.6).

Implements the public contract for Capability 5 — Historical Trends and the
extended Jobs_Router progress view: Bucket / Aggregation literals, TrendRequest,
TrendPoint, TrendSeries, TrendResponse, and JobProgressResponse. Satisfies
R14.1, R15.3. The actual `progress_ratio` computation lives in the router; this
module only declares the field constraint (`ge=0.0, le=1.0`).
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

__all__ = [
    "Bucket",
    "Aggregation",
    "TrendRequest",
    "TrendPoint",
    "TrendSeries",
    "TrendResponse",
    "JobProgressResponse",
]


Bucket = Literal["1m", "5m", "15m", "1h", "6h", "1d", "7d"]
Aggregation = Literal["count", "sum", "mean", "min", "max", "p50", "p95", "p99"]


class TrendRequest(BaseModel):
    stream_ids: list[str] = Field(..., min_length=1, max_length=50)
    time_start: datetime
    time_end: datetime
    bucket: Bucket
    aggregation: Aggregation = "count"
    compare_to: Literal["previous_period"] | None = None


class TrendPoint(BaseModel):
    bucket_start: datetime
    value: float


class TrendSeries(BaseModel):
    series: dict[str, list[TrendPoint]]
    comparison: dict[str, list[TrendPoint]] | None = None
    delta: dict[str, list[TrendPoint]] | None = None


class TrendResponse(BaseModel):
    series: TrendSeries
    bucket: Bucket
    aggregation: Aggregation
    fallback: bool = False


class JobProgressResponse(BaseModel):
    job_id: str
    status: Literal["pending", "running", "completed", "failed"]
    progress_current: int | None = Field(default=None, ge=0)
    progress_total: int | None = Field(default=None, ge=0)
    progress_ratio: float | None = Field(default=None, ge=0.0, le=1.0)
    eta_seconds: float | None = Field(default=None, ge=0.0)
    created_at: datetime
    updated_at: datetime
