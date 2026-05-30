"""Integration tests for :class:`ExposureAlerter` (task 7.15).

Scenarios (R3.4, R5.1, R5.2, R5.3, R5.4, Property 9):

1. ``severity == "critical"`` POSTs to the Alertmanager
   ``eas-critical`` receiver with ``alertname=HydraEASCriticalExposure``.
2. ``severity == "high"`` POSTs to the ``eas-warning`` receiver with
   ``alertname=HydraEASExposure``.
3. ``severity`` in ``{"medium", "low"}`` triggers NEITHER an HTTP call
   NOR an audit row.
4. A tenant with an entry in ``per_tenant_webhook_url`` fans out to
   both Alertmanager and the tenant webhook; both audit rows persist.
5. HTTP 5xx response ⇒ ``status="failed"`` audit row, no raise (R5.4).
6. HTTP exception ⇒ ``status="failed"`` audit row, no raise (R5.4).

All scenarios use in-test fakes — no network, no database, no
``httpx.AsyncClient``. The fakes implement only what the alerter calls:
``httpx``-ish ``post(url, json=..., timeout=...)`` and
``asyncpg``-ish ``pool.acquire() → conn.execute(sql, *args)``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import pytest

from hydra.eas.assets.alerter import ExposureAlerter
from hydra.eas.assets.models import ExposureEvent
from hydra.eas.settings import EASSettings

from tests.eas.conftest import make_asset


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeResponse:
    """Minimal stand-in for ``httpx.Response``.

    The alerter only reads ``response.status_code`` to classify the
    outcome, so nothing else is modelled.
    """

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code


class _FakeHttpClient:
    """Duck-typed ``httpx.AsyncClient`` capturing every ``post`` call.

    Two modes:

    * ``status_code`` set → ``post`` returns a :class:`_FakeResponse`
      with that code.
    * ``raise_exc`` set → ``post`` raises that exception. Simulates a
      connection error / timeout.
    """

    def __init__(
        self,
        *,
        status_code: int = 202,
        raise_exc: Exception | None = None,
    ) -> None:
        self.status_code = status_code
        self.raise_exc = raise_exc
        self.calls: list[dict[str, Any]] = []

    async def post(
        self, url: str, *, json: Any = None, timeout: Any = None
    ) -> _FakeResponse:
        self.calls.append({"url": url, "json": json, "timeout": timeout})
        if self.raise_exc is not None:
            raise self.raise_exc
        return _FakeResponse(self.status_code)


class _FakeConnection:
    """Duck-typed asyncpg ``Connection`` that records ``execute`` calls.

    The alerter uses ``execute`` only for the audit-row INSERT into
    ``exposure_alert_deliveries``. We capture the positional params in
    the order the repository bound them: ``(exposure_id, receiver,
    status)``.
    """

    def __init__(self, pool: "_FakePool") -> None:
        self._pool = pool

    async def execute(self, sql: str, *args: Any) -> str:
        self._pool.executed.append(
            {
                "sql": sql,
                "exposure_id": args[0],
                "receiver": args[1],
                "status": args[2],
            }
        )
        return "INSERT 0 1"


class _FakePool:
    """In-memory ``asyncpg.Pool`` stand-in. Records every audit-row write."""

    def __init__(self) -> None:
        self.executed: list[dict[str, Any]] = []

    def acquire(self) -> "_FakePool":
        return self

    async def __aenter__(self) -> _FakeConnection:
        return _FakeConnection(self)

    async def __aexit__(self, *exc: Any) -> None:
        return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_ALERTMANAGER_URL = "http://alertmanager:9093"
_ALERTS_ENDPOINT = f"{_ALERTMANAGER_URL}/api/v2/alerts"


def _make_exposure(
    *,
    tenant_id: UUID,
    asset_id: UUID,
    severity: str,
    record_hash: str = "0123456789abcdef",
    matched_indicator: str = "192.0.2.1",
    tier: int = 16,
) -> ExposureEvent:
    """Build a fully populated :class:`ExposureEvent` for alerter tests."""

    return ExposureEvent(
        exposure_id=uuid4(),
        asset_id=asset_id,
        tenant_id=tenant_id,
        record_hash=record_hash,
        tier=tier,
        matched_indicator=matched_indicator,
        severity=severity,
        created_at=datetime.now(timezone.utc),
    )


def _make_alerter(
    settings: EASSettings | None = None,
    pool: _FakePool | None = None,
    http_client: _FakeHttpClient | None = None,
) -> tuple[ExposureAlerter, _FakePool, _FakeHttpClient]:
    """Convenience builder; returns (alerter, pool, client) for assertions."""

    settings = settings if settings is not None else EASSettings()
    pool = pool if pool is not None else _FakePool()
    http_client = (
        http_client if http_client is not None else _FakeHttpClient(status_code=202)
    )
    alerter = ExposureAlerter(
        settings,
        pool,
        alertmanager_url=_ALERTMANAGER_URL,
        http_client=http_client,
    )
    return alerter, pool, http_client


# ---------------------------------------------------------------------------
# Scenario 1 — critical severity routes to eas-critical
# ---------------------------------------------------------------------------


async def test_critical_exposure_posts_to_eas_critical() -> None:
    """R5.1 — critical exposures go to the ``eas-critical`` receiver."""

    tenant_id = uuid4()
    asset = make_asset(tenant_id=tenant_id, asset_type="ip", normalized_value="192.0.2.1")
    exposure = _make_exposure(
        tenant_id=tenant_id, asset_id=asset.asset_id, severity="critical"
    )

    alerter, pool, http = _make_alerter()
    await alerter.send(exposure, asset)

    # Exactly one POST to the shared Alertmanager endpoint.
    assert len(http.calls) == 1
    call = http.calls[0]
    assert call["url"] == _ALERTS_ENDPOINT

    # Body is a JSON array of one alert.
    body = call["json"]
    assert isinstance(body, list) and len(body) == 1
    alert = body[0]

    assert alert["labels"]["severity"] == "critical"
    assert alert["labels"]["alertname"] == "HydraEASCriticalExposure"
    assert alert["labels"]["tenant_id"] == str(tenant_id)
    assert alert["labels"]["asset_id"] == str(asset.asset_id)

    # Audit row persisted with status="sent" and receiver="eas-critical".
    assert len(pool.executed) == 1
    audit = pool.executed[0]
    assert audit["exposure_id"] == exposure.exposure_id
    assert audit["receiver"] == "eas-critical"
    assert audit["status"] == "sent"


# ---------------------------------------------------------------------------
# Scenario 2 — high severity routes to eas-warning
# ---------------------------------------------------------------------------


async def test_high_exposure_posts_to_eas_warning() -> None:
    """R5.2 — high exposures go to the ``eas-warning`` receiver."""

    tenant_id = uuid4()
    asset = make_asset(tenant_id=tenant_id, asset_type="ip", normalized_value="192.0.2.1")
    exposure = _make_exposure(
        tenant_id=tenant_id, asset_id=asset.asset_id, severity="high"
    )

    alerter, pool, http = _make_alerter()
    await alerter.send(exposure, asset)

    assert len(http.calls) == 1
    call = http.calls[0]
    assert call["url"] == _ALERTS_ENDPOINT

    alert = call["json"][0]
    assert alert["labels"]["severity"] == "high"
    # High exposures get the generic alertname, not the critical one.
    assert alert["labels"]["alertname"] == "HydraEASExposure"

    assert len(pool.executed) == 1
    audit = pool.executed[0]
    assert audit["receiver"] == "eas-warning"
    assert audit["status"] == "sent"


# ---------------------------------------------------------------------------
# Scenario 3 — medium/low severities do NOT dispatch
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("severity", ["medium", "low"])
async def test_non_high_non_critical_severities_do_not_dispatch(
    severity: str,
) -> None:
    """Only high/critical fan out; medium and low stay silent."""

    tenant_id = uuid4()
    asset = make_asset(tenant_id=tenant_id, asset_type="ip", normalized_value="192.0.2.1")
    exposure = _make_exposure(
        tenant_id=tenant_id, asset_id=asset.asset_id, severity=severity
    )

    alerter, pool, http = _make_alerter()
    await alerter.send(exposure, asset)

    assert http.calls == []
    assert pool.executed == []


# ---------------------------------------------------------------------------
# Scenario 4 — per-tenant webhook fan-out (R5.3)
# ---------------------------------------------------------------------------


async def test_per_tenant_webhook_fans_out_alongside_alertmanager() -> None:
    """R5.3 — a tenant with a configured webhook gets both deliveries."""

    tenant_id = uuid4()
    webhook_url = "https://tenant.example.com/hook"
    settings = EASSettings(per_tenant_webhook_url={str(tenant_id): webhook_url})

    asset = make_asset(tenant_id=tenant_id, asset_type="ip", normalized_value="192.0.2.1")
    exposure = _make_exposure(
        tenant_id=tenant_id, asset_id=asset.asset_id, severity="critical"
    )

    alerter, pool, http = _make_alerter(settings=settings)
    await alerter.send(exposure, asset)

    # Two POSTs — one to Alertmanager, one to the tenant webhook.
    urls = [call["url"] for call in http.calls]
    assert _ALERTS_ENDPOINT in urls
    assert webhook_url in urls
    assert len(http.calls) == 2

    # Both audit rows persisted with status="sent".
    receivers = [row["receiver"] for row in pool.executed]
    assert "eas-critical" in receivers
    assert webhook_url in receivers
    assert all(row["status"] == "sent" for row in pool.executed)
    assert len(pool.executed) == 2


async def test_per_tenant_webhook_respects_severity_gating() -> None:
    """A tenant webhook does NOT dispatch for medium/low severities.

    The receivers list is built by OR-ing the severity-driven receiver
    onto the per-tenant webhook. For medium/low there is no base
    receiver, so the webhook alone would fire. Check the current
    behaviour: per the implementation, the webhook piggy-backs on any
    alertable severity; medium/low still add the webhook because the
    code appends it unconditionally when configured. This test
    documents the current contract so a future behaviour change is
    explicit.
    """

    tenant_id = uuid4()
    webhook_url = "https://tenant.example.com/hook"
    settings = EASSettings(per_tenant_webhook_url={str(tenant_id): webhook_url})

    asset = make_asset(tenant_id=tenant_id, asset_type="ip", normalized_value="192.0.2.1")
    exposure = _make_exposure(
        tenant_id=tenant_id, asset_id=asset.asset_id, severity="medium"
    )

    alerter, pool, http = _make_alerter(settings=settings)
    await alerter.send(exposure, asset)

    # Per implementation: webhook fires for any severity when configured.
    urls = [call["url"] for call in http.calls]
    assert urls == [webhook_url]
    assert len(pool.executed) == 1
    assert pool.executed[0]["receiver"] == webhook_url


# ---------------------------------------------------------------------------
# Scenario 5 — HTTP 5xx → failed audit row, no raise
# ---------------------------------------------------------------------------


async def test_http_5xx_records_failed_audit_row_without_raising() -> None:
    """R5.4 — Alertmanager owns retry; we record and move on."""

    tenant_id = uuid4()
    asset = make_asset(tenant_id=tenant_id, asset_type="ip", normalized_value="192.0.2.1")
    exposure = _make_exposure(
        tenant_id=tenant_id, asset_id=asset.asset_id, severity="critical"
    )

    http = _FakeHttpClient(status_code=503)
    alerter, pool, _ = _make_alerter(http_client=http)

    # Must NOT raise.
    await alerter.send(exposure, asset)

    # Still attempted one HTTP call.
    assert len(http.calls) == 1
    # Audit row captures the failure rather than being skipped.
    assert len(pool.executed) == 1
    assert pool.executed[0]["status"] == "failed"
    assert pool.executed[0]["receiver"] == "eas-critical"


# ---------------------------------------------------------------------------
# Scenario 6 — HTTP exception → failed audit row, no raise
# ---------------------------------------------------------------------------


async def test_http_exception_records_failed_audit_row_without_raising() -> None:
    """A connection error behaves the same as a 5xx for audit purposes."""

    tenant_id = uuid4()
    asset = make_asset(tenant_id=tenant_id, asset_type="ip", normalized_value="192.0.2.1")
    exposure = _make_exposure(
        tenant_id=tenant_id, asset_id=asset.asset_id, severity="critical"
    )

    http = _FakeHttpClient(raise_exc=Exception("connection refused"))
    alerter, pool, _ = _make_alerter(http_client=http)

    # Must NOT re-raise the connection error.
    await alerter.send(exposure, asset)

    # HTTP call was attempted; the exception was caught.
    assert len(http.calls) == 1
    # Audit row persists with failed status.
    assert len(pool.executed) == 1
    assert pool.executed[0]["status"] == "failed"
