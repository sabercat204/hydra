"""Tests for ``hydra.monitoring.log_config`` — structured JSON logging.

Covers Task 15.1 — Requirements 26.1, 26.2:

* :class:`JSONLogFormatter` emits valid JSON with the fixed fields
  (``timestamp``, ``level``, ``module``, ``message``).
* Contextual fields supplied via ``extra={...}`` are surfaced as
  top-level JSON keys for the known field names.
* ``exc_info`` is captured under an ``"exception"`` key rather than
  breaking JSON validity.
* :func:`configure_monitoring_logging` is idempotent — repeated calls
  produce exactly one JSON handler.
* Text-format mode leaves no JSON handler attached to the monitoring
  logger.
"""

from __future__ import annotations

import io
import json
import logging

import pytest

from hydra.config import MonitoringSettings
from hydra.monitoring.log_config import (
    JSONLogFormatter,
    configure_monitoring_logging,
)

MONITORING_LOGGER = "hydra.monitoring"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _reset_monitoring_logger() -> None:
    """Strip every handler and restore defaults on the monitoring logger.

    Tests that exercise :func:`configure_monitoring_logging` mutate global
    state (the module-level ``hydra.monitoring`` logger). Reset between
    tests to prevent cross-test contamination.
    """
    logger = logging.getLogger(MONITORING_LOGGER)
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()
    logger.propagate = True
    logger.setLevel(logging.NOTSET)


@pytest.fixture(autouse=True)
def _clean_monitoring_logger():
    """Autouse fixture — reset the monitoring logger before and after."""
    _reset_monitoring_logger()
    yield
    _reset_monitoring_logger()


def _make_record(
    *,
    name: str = "hydra.monitoring.test",
    level: int = logging.INFO,
    msg: str = "hello",
    extra: dict[str, object] | None = None,
    exc_info: tuple | None = None,
) -> logging.LogRecord:
    """Construct a :class:`logging.LogRecord` mirroring what ``logger.log(...)``
    would produce. We drive the formatter directly rather than routing
    through a handler so tests assert on the formatter output in isolation.
    """
    record = logging.LogRecord(
        name=name,
        level=level,
        pathname=__file__,
        lineno=0,
        msg=msg,
        args=(),
        exc_info=exc_info,
    )
    # ``logger.log(..., extra={...})`` merges the extras into
    # ``record.__dict__`` — replicate that here.
    if extra:
        record.__dict__.update(extra)
    return record


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_json_formatter_emits_valid_json_with_required_fields() -> None:
    """The four fixed fields are always present and the output parses."""
    formatter = JSONLogFormatter()
    record = _make_record(name="hydra.monitoring.collectors.scheduler")

    output = formatter.format(record)
    payload = json.loads(output)

    # All four fixed fields present (Requirement 26.2).
    assert set(payload.keys()) >= {"timestamp", "level", "module", "message"}
    assert payload["level"] == "INFO"
    assert payload["module"] == "hydra.monitoring.collectors.scheduler"
    assert payload["message"] == "hello"
    # ISO 8601 UTC with millisecond precision and a trailing ``Z``.
    assert payload["timestamp"].endswith("Z")
    assert "T" in payload["timestamp"]


def test_json_formatter_surfaces_extra_fields_as_top_level_keys() -> None:
    """Known contextual fields passed via ``extra=`` appear at the top level."""
    formatter = JSONLogFormatter()
    record = _make_record(
        extra={
            "stream_id": "abc-123",
            "tier": "1",
            "engine": "postgres",
            "duration_ms": 42.5,
            # Unknown extras are ignored — the schema stays stable.
            "ignored_field": "nope",
        },
    )

    payload = json.loads(formatter.format(record))

    assert payload["stream_id"] == "abc-123"
    assert payload["tier"] == "1"
    assert payload["engine"] == "postgres"
    assert payload["duration_ms"] == 42.5
    assert "ignored_field" not in payload


def test_json_formatter_captures_exception_traceback() -> None:
    """``exc_info`` is rendered under an ``exception`` key, not inlined."""
    formatter = JSONLogFormatter()

    # Synthesize a real exception with a traceback by raising and catching.
    try:
        raise ValueError("boom")
    except ValueError:
        import sys

        exc_info = sys.exc_info()

    record = _make_record(msg="something failed", exc_info=exc_info)

    output = formatter.format(record)
    payload = json.loads(output)

    # The output is still valid JSON (the whole point of a dedicated key).
    assert payload["message"] == "something failed"
    assert "exception" in payload
    assert "ValueError" in payload["exception"]
    assert "boom" in payload["exception"]


def test_configure_monitoring_logging_is_idempotent() -> None:
    """N calls yield exactly one JSON handler on the monitoring logger."""
    settings = MonitoringSettings()  # defaults: log_format="json"

    configure_monitoring_logging(settings)
    configure_monitoring_logging(settings)
    configure_monitoring_logging(settings)

    logger = logging.getLogger(MONITORING_LOGGER)
    json_handlers = [
        h for h in logger.handlers if isinstance(h.formatter, JSONLogFormatter)
    ]
    assert len(json_handlers) == 1
    # Propagation suppressed to avoid duplicate emission via root logger.
    assert logger.propagate is False


def test_configure_monitoring_logging_text_mode_attaches_no_json_handler() -> None:
    """Text mode removes any JSON handler and restores propagation."""
    logger = logging.getLogger(MONITORING_LOGGER)

    # First install JSON, then flip to text and verify the JSON handler is gone.
    configure_monitoring_logging(MonitoringSettings(log_format="json"))
    assert any(isinstance(h.formatter, JSONLogFormatter) for h in logger.handlers)

    configure_monitoring_logging(MonitoringSettings(log_format="text"))

    assert not any(
        isinstance(h.formatter, JSONLogFormatter) for h in logger.handlers
    )
    assert logger.propagate is True


def test_configure_monitoring_logging_end_to_end_writes_json_line() -> None:
    """Attach an in-memory stream, emit a record, assert the line parses.

    This is a small integration test — it verifies that the handler
    wired up by :func:`configure_monitoring_logging` actually produces
    JSON when a real logger call flows through it.
    """
    configure_monitoring_logging(MonitoringSettings(log_format="json"))

    # Swap the handler's stream for a StringIO so we can capture output
    # without touching stdout.
    logger = logging.getLogger(MONITORING_LOGGER)
    json_handler = next(
        h for h in logger.handlers if isinstance(h.formatter, JSONLogFormatter)
    )
    buffer = io.StringIO()
    json_handler.stream = buffer

    logging.getLogger("hydra.monitoring.collectors.api").info(
        "collection complete",
        extra={"collector": "api", "duration_ms": 12.3},
    )

    output = buffer.getvalue().strip()
    assert output, "expected a JSON line on the captured stream"
    payload = json.loads(output)
    assert payload["module"] == "hydra.monitoring.collectors.api"
    assert payload["message"] == "collection complete"
    assert payload["collector"] == "api"
    assert payload["duration_ms"] == 12.3
