"""Structured JSON logging for the HYDRA monitoring subsystem.

Task 15.1 — Requirements 26.1, 26.2.

This module configures Python's :mod:`logging` so that every log record
emitted by any logger under ``hydra.monitoring.*`` is serialized as a
single-line JSON object on stdout. Each record carries a fixed set of
fields (``timestamp``, ``level``, ``module``, ``message``) plus any
contextual fields supplied via ``extra={...}`` at the call site.

Design decisions:

* **Scoped handler.** The JSON handler is attached to the
  ``hydra.monitoring`` logger only, not to the root logger or to the
  broader ``hydra`` logger. This preserves whatever log configuration
  the surrounding application (API, adapters, storage, etc.) already
  uses. To prevent duplicate emission through the root logger, we set
  ``propagate = False`` on ``hydra.monitoring``.

* **Idempotent configuration.** :func:`configure_monitoring_logging`
  can be called multiple times without stacking handlers — each call
  removes any previously-installed :class:`JSONLogFormatter` handler
  before attaching the new one. Text-mode invocations remove the JSON
  handler entirely so the logger falls back to parent propagation.

* **No global ``logging.basicConfig``.** This module never touches the
  root logger or global defaults. Callers outside of monitoring are
  free to configure logging however they like.

* **Best-effort contextual fields.** :attr:`_KNOWN_CONTEXT_FIELDS`
  lists the contextual keys that are surfaced as top-level JSON keys
  when present on the log record (typically via ``extra={...}``).
  Unknown ``extra`` fields are ignored to keep the JSON shape stable.
"""

from __future__ import annotations

import json
import logging
import sys
import traceback
from datetime import datetime, timezone
from typing import Any, Final

from hydra.config import MonitoringSettings

# The root logger namespace under which every monitoring module logs.
# ``src/hydra/monitoring/*`` already uses ``logging.getLogger(__name__)``,
# so every log record from the subsystem automatically propagates to this
# namespace.
_MONITORING_LOGGER_NAME: Final[str] = "hydra.monitoring"

# Fixed top-level keys the JSON formatter always emits. These are not
# extracted from ``record.__dict__`` — they come from the record directly.
_FIXED_FIELDS: Final[frozenset[str]] = frozenset({
    "timestamp",
    "level",
    "module",
    "message",
})

# Contextual fields that are surfaced as top-level JSON keys when the
# caller passes them via ``extra={...}``. Keep this list narrow to
# preserve a stable output schema — Loki / ELK / CloudWatch parsers
# key off these names and any drift breaks downstream dashboards.
_KNOWN_CONTEXT_FIELDS: Final[frozenset[str]] = frozenset({
    "stream_id",
    "tier",
    "engine",
    "request_id",
    "duration_ms",
    "error",
    "pipeline_id",
    "detector",
    "collector",
    "dag_id",
    "cadence",
    "product_type",
    "slo_name",
    "table",
    "index",
    "bucket",
})


# ---------------------------------------------------------------------------
# Formatter
# ---------------------------------------------------------------------------


def _extract_context_fields(record: logging.LogRecord) -> dict[str, Any]:
    """Return the subset of ``record.__dict__`` that matches the known
    contextual field names.

    This is how ``logger.info("msg", extra={"stream_id": "abc"})`` ends
    up as a top-level ``stream_id`` key in the JSON output.

    Args:
        record: The log record being formatted.

    Returns:
        A fresh dict containing only those keys in
        :data:`_KNOWN_CONTEXT_FIELDS` that are present on ``record``
        with a non-``None`` value.
    """
    extracted: dict[str, Any] = {}
    for field in _KNOWN_CONTEXT_FIELDS:
        value = record.__dict__.get(field)
        if value is not None:
            extracted[field] = value
    return extracted


class JSONLogFormatter(logging.Formatter):
    """Serialize log records as single-line JSON (Requirement 26.1, 26.2).

    Output shape::

        {
          "timestamp": "2026-04-29T12:34:56.789Z",
          "level": "INFO",
          "module": "hydra.monitoring.collectors.scheduler",
          "message": "collection cycle complete",
          "stream_id": "abc",   # optional, from extra=
          ...
        }

    When ``record.exc_info`` is populated (e.g. ``logger.exception(...)``
    or ``logger.error(..., exc_info=e)``), the formatted traceback is
    attached under the ``exception`` key rather than appended to the
    message — keeping the JSON parseable.
    """

    def format(self, record: logging.LogRecord) -> str:  # noqa: D401
        """Return ``record`` as a JSON string (no trailing newline)."""
        # ISO 8601 UTC with millisecond precision. ``created`` is a float
        # POSIX timestamp set by :mod:`logging` when the record is
        # constructed. ``fromtimestamp(..., tz=UTC)`` yields a tz-aware
        # datetime; ``isoformat()`` with ``timespec="milliseconds"``
        # gives ``2026-04-29T12:34:56.789+00:00`` — replace the offset
        # with ``Z`` to match the Prometheus / OTel convention.
        timestamp = (
            datetime.fromtimestamp(record.created, tz=timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )

        payload: dict[str, Any] = {
            "timestamp": timestamp,
            "level": record.levelname,
            # ``record.name`` is the full dotted logger name (e.g.
            # ``hydra.monitoring.collectors.scheduler``) which is more
            # useful for filtering than ``record.module`` (just the file
            # basename). Requirement 26.2 calls this field "module".
            "module": record.name,
            "message": record.getMessage(),
        }

        # Surface contextual extras as top-level keys. We intentionally
        # skip unknown extras so the shape is predictable.
        payload.update(_extract_context_fields(record))

        # Render exception traceback (if any) under a dedicated key so
        # the top-level object stays JSON-valid.
        if record.exc_info:
            payload["exception"] = "".join(
                traceback.format_exception(*record.exc_info)
            ).rstrip()
        elif record.exc_text:
            # Pre-formatted traceback (e.g. when a Formatter upstream
            # already cached it on the record).
            payload["exception"] = record.exc_text

        # ``default=str`` is a safety net for any odd object that slips
        # into ``extra=`` (datetimes, UUIDs, enums). Without it a single
        # un-serializable value would crash the whole log emission.
        return json.dumps(payload, default=str, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def _remove_json_handlers(logger: logging.Logger) -> None:
    """Strip any previously-installed :class:`JSONLogFormatter` handler
    from ``logger``.

    Used by :func:`configure_monitoring_logging` to keep the configuration
    idempotent — repeated calls must not stack handlers, otherwise each
    log record would be printed N times.
    """
    # Copy the list — we mutate it in the loop.
    for handler in list(logger.handlers):
        if isinstance(handler.formatter, JSONLogFormatter):
            logger.removeHandler(handler)
            # Best-effort close to release the stdout stream reference.
            try:
                handler.close()
            except Exception:  # noqa: BLE001 — handler cleanup must not raise
                pass


def configure_monitoring_logging(settings: MonitoringSettings) -> None:
    """Configure JSON logging for the ``hydra.monitoring`` logger.

    Behaviour depends on ``settings.log_format``:

    * ``"json"`` (default) — attach a single ``StreamHandler(sys.stdout)``
      with :class:`JSONLogFormatter` to the ``hydra.monitoring`` logger.
      Set the level from ``settings.log_level``. Disable propagation so
      JSON records do not also flow through the root logger (which would
      produce duplicate, differently-formatted output).

    * ``"text"`` (or any other value) — remove any previously-installed
      JSON handler and leave the logger to inherit its parent's
      formatting. Propagation is re-enabled in this mode.

    The function is safe to call more than once: repeated calls replace
    the existing JSON handler rather than stacking new ones. Tests
    verify this with a handler-count assertion.

    Args:
        settings: The monitoring configuration block. Only
            ``log_format`` and ``log_level`` are consulted here.
    """
    logger = logging.getLogger(_MONITORING_LOGGER_NAME)

    # Apply level regardless of format — the log_level knob is
    # independent of log_format, and we want ``WARNING`` or ``DEBUG``
    # to take effect in text mode too.
    try:
        logger.setLevel(settings.log_level)
    except (ValueError, TypeError):
        # Unknown level name — fall back to INFO rather than raising
        # during application startup.
        logger.setLevel(logging.INFO)

    # Always strip prior JSON handlers before branching on log_format:
    # this makes the configuration idempotent and guarantees a clean
    # state if the operator flipped the format at runtime.
    _remove_json_handlers(logger)

    if settings.log_format == "json":
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JSONLogFormatter())
        logger.addHandler(handler)

        # Suppress propagation so the root logger does not also
        # format+emit the same record (which would duplicate output and
        # likely in a different, plain-text format).
        logger.propagate = False
    else:
        # Text mode — rely on whatever the parent logger already does.
        # Re-enable propagation in case a prior json-mode call disabled
        # it.
        logger.propagate = True


__all__ = [
    "JSONLogFormatter",
    "configure_monitoring_logging",
]
