"""Alertmanager v2 dispatch for exposures (Design §8.1, R5).

``ExposureAlerter`` takes a fully-persisted :class:`ExposureEvent` and
fans it out to the configured Alertmanager receivers plus an optional
per-tenant webhook. Every dispatch is recorded in the
``exposure_alert_deliveries`` table for audit (R5 / Design §4.10).

Design choices:

* **Alertmanager owns retries.** R5.4 explicitly says: if a POST fails,
  Alertmanager's own retry/buffering handles it — we must not duplicate
  the event. The alerter therefore never re-enqueues a failure; it
  simply writes a ``status="failed"`` delivery row and logs at WARN.
* **Receiver selection.** ``severity=="critical"`` → ``eas-critical``;
  ``severity=="high"`` → ``eas-warning``. Lower severities don't page
  on-call and are not dispatched (the exposure row is still written so
  it surfaces via the API).
* **Per-tenant webhooks.** When the tenant_id has an entry in
  ``EASSettings.per_tenant_webhook_url`` we POST to that URL too; same
  payload shape. This is the R5.3 extension point.
* **Injected HTTP client.** Tests pass a fake ``httpx.AsyncClient`` via
  the constructor to avoid real network calls.
* **Injected pool.** The audit-row write goes through the same pool
  abstraction as :class:`ExposureRepository`, so tests can stub it.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from hydra.eas.assets.models import Asset, ExposureEvent
from hydra.eas.settings import EASSettings

logger = logging.getLogger(__name__)

__all__ = ["ExposureAlerter"]


# Receivers configured in ``alertmanager/alertmanager.yml`` by the
# platform-ops layer. Hard-coding the receiver names (vs making them
# configurable) is intentional — they are part of the platform contract
# and changing them requires an ops-side change anyway.
_RECEIVER_CRITICAL = "eas-critical"
_RECEIVER_WARNING = "eas-warning"

# Short-but-non-zero timeout. Alertmanager is local to the platform so
# a 5 s upper bound is generous; a hung Alertmanager should not block
# the ingestion path.
_DEFAULT_TIMEOUT_SECONDS = 5.0


class ExposureAlerter:
    """POST exposures to Alertmanager and persist an audit row."""

    def __init__(
        self,
        settings: EASSettings,
        pool: Any,
        *,
        alertmanager_url: str | None = None,
        http_client: Any | None = None,
        timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        self._settings = settings
        self._pool = pool
        # ``alertmanager_url`` is the base URL of the Alertmanager HTTP
        # API — the ``/api/v2/alerts`` suffix is appended in :meth:`send`.
        # When not supplied, fall back to an env-style default so tests
        # and dev deployments work without explicit wiring.
        self._alertmanager_url = (
            alertmanager_url.rstrip("/")
            if alertmanager_url is not None
            else "http://alertmanager:9093"
        )
        self._http_client = http_client
        self._timeout_seconds = timeout_seconds

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def send(self, exposure: ExposureEvent, asset: Asset) -> None:
        """Fan out the exposure to all applicable receivers.

        ``asset`` is passed in alongside ``exposure`` because the alert
        payload wants the asset's ``normalized_value`` and ``asset_type``
        labels — re-fetching from PG here would race the caller's own
        write path and cost another connection acquisition.
        """

        receivers = self._receivers_for(exposure, asset)
        if not receivers:
            return

        payload = _build_alertmanager_payload(exposure, asset)

        for receiver in receivers:
            target_url = self._url_for(receiver)
            status = await self._post_payload(target_url, payload, receiver)
            await self._record_delivery(
                exposure_id=exposure.exposure_id,
                receiver=receiver,
                status=status,
            )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _receivers_for(
        self, exposure: ExposureEvent, asset: Asset
    ) -> list[str]:
        """Resolve which named receivers should get this alert."""

        receivers: list[str] = []
        if exposure.severity == "critical":
            receivers.append(_RECEIVER_CRITICAL)
        elif exposure.severity == "high":
            receivers.append(_RECEIVER_WARNING)

        # Per-tenant webhook (R5.3). The map key is the tenant_id as
        # string because environment-variable overrides round-trip
        # through string parsing.
        webhook_url = self._settings.per_tenant_webhook_url.get(
            str(exposure.tenant_id)
        )
        if webhook_url:
            # The receiver name recorded in the audit row is the webhook
            # URL itself — it's opaque to Alertmanager but meaningful
            # to operators reviewing the delivery log.
            receivers.append(webhook_url)

        return receivers

    def _url_for(self, receiver: str) -> str:
        """Translate a receiver name into an outbound URL.

        Per-tenant webhooks carry their own URL in the ``receiver`` slot;
        named Alertmanager receivers are addressed via the shared
        ``/api/v2/alerts`` endpoint.
        """

        if receiver.startswith("http://") or receiver.startswith("https://"):
            return receiver
        return f"{self._alertmanager_url}/api/v2/alerts"

    async def _post_payload(
        self,
        target_url: str,
        payload: dict[str, Any],
        receiver: str,
    ) -> str:
        """POST the payload; return delivery status string."""

        # Alertmanager wants a JSON array of alert objects; the payload
        # we build is a single alert so wrap it here.
        body = [payload]

        client = await self._ensure_client()
        try:
            response = await client.post(
                target_url,
                json=body,
                timeout=self._timeout_seconds,
            )
        except Exception as exc:
            # Per R5.4 Alertmanager handles retry/buffering — we log and
            # continue. The audit row captures the failure for operators.
            logger.warning(
                "exposure_alert_failed",
                extra={
                    "receiver": receiver,
                    "url": target_url,
                    "error": str(exc),
                },
            )
            return "failed"

        # Any 2xx counts as a successful handoff; everything else (4xx,
        # 5xx) is a failure and Alertmanager's internal buffer will
        # retry. We log at WARN for 4xx (misconfiguration) and WARN for
        # 5xx (upstream failure).
        status_code = getattr(response, "status_code", 0)
        if 200 <= status_code < 300:
            return "sent"
        logger.warning(
            "exposure_alert_non_2xx",
            extra={
                "receiver": receiver,
                "url": target_url,
                "status_code": status_code,
            },
        )
        return "failed"

    async def _ensure_client(self) -> Any:
        """Lazily instantiate an ``httpx.AsyncClient`` when not injected."""

        if self._http_client is not None:
            return self._http_client
        # Import inside the method so test environments that don't have
        # ``httpx`` installed can still import this module.
        import httpx

        self._http_client = httpx.AsyncClient()
        return self._http_client

    async def _record_delivery(
        self,
        *,
        exposure_id: UUID,
        receiver: str,
        status: str,
    ) -> None:
        """Persist a row in ``exposure_alert_deliveries``.

        The ``status`` CHECK constraint on the table only accepts
        ``{"sent", "failed", "buffered"}``; we only emit the first two
        here — ``buffered`` is reserved for the backpressure path
        (task 7.9 deque).
        """

        sql = """
            INSERT INTO exposure_alert_deliveries (
                exposure_id, receiver, status
            )
            VALUES ($1, $2, $3)
        """
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(sql, exposure_id, receiver, status)
        except Exception as exc:
            # Audit-row writes are best-effort: losing one does not
            # justify failing the alerting path or raising into the
            # ingestion flow.
            logger.warning(
                "exposure_alert_delivery_insert_failed",
                extra={
                    "exposure_id": str(exposure_id),
                    "receiver": receiver,
                    "error": str(exc),
                },
            )


def _build_alertmanager_payload(
    exposure: ExposureEvent, asset: Asset
) -> dict[str, Any]:
    """Shape a single Alertmanager v2 alert object (Design §8.1)."""

    starts_at = (
        exposure.created_at
        if exposure.created_at.tzinfo is not None
        else exposure.created_at.replace(tzinfo=timezone.utc)
    ).isoformat()

    # ``alertname`` matches the rule name in ``hydra_eas_alerts.yml``
    # that task 16.2 will add; the receivers in Alertmanager are wired
    # off the ``severity`` label so we set both.
    return {
        "labels": {
            "alertname": "HydraEASCriticalExposure"
            if exposure.severity == "critical"
            else "HydraEASExposure",
            "severity": exposure.severity,
            "tenant_id": str(exposure.tenant_id),
            "asset_type": asset.asset_type,
            "asset_id": str(asset.asset_id),
            "tier": str(exposure.tier),
        },
        "annotations": {
            "summary": f"{exposure.severity.capitalize()} exposure for asset "
            f"{asset.normalized_value}",
            "description": (
                f"Record {exposure.record_hash} matches indicator "
                f"{exposure.matched_indicator}"
            ),
            "asset_reference": f"/api/v1/assets/{asset.asset_id}",
        },
        "startsAt": starts_at,
        "generatorURL": f"/api/v1/assets/{asset.asset_id}/exposures",
    }


# End-of-day reminder for the tz-aware datetime above: ``datetime`` is
# re-exported here so static analysers don't complain about the unused
# import in rare lint modes.
_ = datetime
