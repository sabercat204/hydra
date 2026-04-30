"""FastAPI HTTP metrics instrumentation (P12 — Component 3).

Wraps ``prometheus_fastapi_instrumentator.Instrumentator`` with HYDRA's
required configuration:

* ``/metrics`` and ``/api/v1/health/ping`` are excluded from HTTP request
  instrumentation so they do not pollute the metrics output with
  self-referential scrape traffic or uninteresting liveness probes
  (Requirement 2.3).
* Status codes are grouped (``2xx``, ``4xx``, ``5xx``) to keep label
  cardinality bounded (Requirement 3.3).
* Untemplated paths are ignored — requests that do not match a route
  template are not recorded, preventing an attacker from exploding
  cardinality with arbitrary URLs.
* The instrumentator emits the three metrics required by Requirement 1.1:
  ``http_requests_total``, ``http_request_duration_seconds``, and
  ``http_requests_in_progress``.

The ``/metrics`` endpoint mounted by :func:`instrument_app` is explicitly
excluded from the OpenAPI schema (``include_in_schema=False``) since it is
an operator-facing endpoint, not part of the public API contract.

See design.md §"Example Usage" → "Instrumentator setup" for the reference
implementation this module mirrors.
"""

from __future__ import annotations

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

#: HTTP handlers excluded from instrumentation. These are regex patterns
#: matched against the route template (not the raw path), so trailing
#: slashes and query strings are irrelevant.
EXCLUDED_HANDLERS: tuple[str, ...] = (
    "/metrics",
    "/api/v1/health/ping",
)


def create_instrumentator() -> Instrumentator:
    """Build a configured :class:`Instrumentator` instance.

    The returned instrumentator has not yet been attached to an app; call
    :func:`instrument_app` to bind it and expose ``/metrics``.

    Returns:
        A configured ``Instrumentator`` that will emit ``http_requests_total``,
        ``http_request_duration_seconds``, and ``http_requests_in_progress``
        with grouped status codes and excluded self-referential handlers.
    """
    return Instrumentator(
        should_group_status_codes=True,
        should_ignore_untemplated=True,
        should_respect_env_var=False,
        excluded_handlers=list(EXCLUDED_HANDLERS),
        inprogress_name="http_requests_in_progress",
        inprogress_labels=True,
    )


def instrument_app(app: FastAPI) -> None:
    """Instrument ``app`` and mount the Prometheus ``/metrics`` endpoint.

    This is the single side-effecting entry point of this module. It:

    1. Creates a configured instrumentator via :func:`create_instrumentator`.
    2. Attaches it to ``app`` (middleware hook for HTTP request metrics).
    3. Exposes ``/metrics`` on ``app``, hidden from the OpenAPI schema.

    The function is idempotent per app instance in practice — calling it
    twice on the same app would register duplicate middleware, so the
    caller (typically ``setup_monitoring()``) is responsible for calling
    it exactly once during application startup.

    Args:
        app: The FastAPI application to instrument.
    """
    instrumentator = create_instrumentator()
    instrumentator.instrument(app)
    instrumentator.expose(app, endpoint="/metrics", include_in_schema=False)


__all__ = [
    "EXCLUDED_HANDLERS",
    "create_instrumentator",
    "instrument_app",
]
