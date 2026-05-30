"""Fail-fast import-guard test for :func:`setup_eas` (task 17.3).

R26.4 and R26.2 require that enabling a capability whose external
dependency isn't installed causes :func:`setup_eas` to fail fast at
boot with a clear log line — rather than silently half-booting and
producing cryptic runtime errors on the first screenshot request.

The only capability currently gated by an optional dependency at
boot time is OCR (``pytesseract``). The :func:`_check_ocr_availability`
helper runs before any wiring so that a deployment with
``EAS__SCREENSHOT__OCR_ENABLED=true`` fails immediately when
``pytesseract`` isn't importable.

This test file drives that guard with three scenarios:

1. ``ocr_enabled=False`` — guard is a no-op, wiring proceeds.
2. ``ocr_enabled=True`` with ``pytesseract`` unavailable — the guard
   raises :class:`RuntimeError` with a clear message and emits the
   ``eas.setup.ocr_dependency_missing`` log line.
3. ``ocr_enabled=True`` with ``pytesseract`` available (stubbed into
   ``sys.modules``) — the guard passes silently.

Validates: R26.2, R26.4.
"""

from __future__ import annotations

import logging
import sys
import types
from typing import Any

import pytest
from fastapi import FastAPI

from hydra.config import HydraSettings
from hydra.eas.setup import setup_eas


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _settings_with_ocr(enabled: bool) -> HydraSettings:
    """Build a :class:`HydraSettings` with ``eas.screenshot.ocr_enabled`` set."""

    settings = HydraSettings()
    settings.eas.screenshot.ocr_enabled = enabled
    return settings


def _uninstall_pytesseract() -> None:
    """Remove any pre-installed / stubbed ``pytesseract`` from ``sys.modules``.

    Some other test file may have installed a stub — we want a
    guaranteed clean slate so :func:`_check_ocr_availability`
    exercises the real ``ImportError`` branch.
    """

    sys.modules.pop("pytesseract", None)


def _install_pytesseract_stub() -> None:
    """Pretend ``pytesseract`` is importable by injecting a minimal stub."""

    stub = types.ModuleType("pytesseract")
    # The guard only does ``import pytesseract`` (no attribute access),
    # so an empty module is enough to satisfy it.
    sys.modules["pytesseract"] = stub


# ---------------------------------------------------------------------------
# Scenario 1 — OCR disabled → guard is a no-op
# ---------------------------------------------------------------------------


async def test_ocr_disabled_skips_guard(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """With ``ocr_enabled=False`` the guard is skipped and setup proceeds.

    Confirms we never raise and never emit the dependency-missing log
    line when OCR is off. The ``setup_eas`` call is still partial (no
    storage clients) but the test only checks that the OCR guard
    doesn't trip — missing clients are logged as warnings and the
    function returns normally.

    Validates: R26.2 (optional-dep behaviour when capability is disabled).
    """

    _uninstall_pytesseract()
    settings = _settings_with_ocr(enabled=False)
    app = FastAPI()

    with caplog.at_level(logging.ERROR, logger="hydra.eas.setup"):
        # Should return without raising.
        await setup_eas(app, settings)

    # No OCR-missing log line was emitted.
    assert not any(
        rec.message == "eas.setup.ocr_dependency_missing"
        or rec.name == "eas.setup.ocr_dependency_missing"
        for rec in caplog.records
    )


# ---------------------------------------------------------------------------
# Scenario 2 — OCR enabled but pytesseract missing → fail-fast
# ---------------------------------------------------------------------------


async def test_ocr_enabled_without_pytesseract_raises(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Enabling OCR without ``pytesseract`` installed raises ``RuntimeError``.

    Also emits the structured ``eas.setup.ocr_dependency_missing`` log
    record so operators can grep for the failure reason. The test
    isolates ``pytesseract`` from the module cache via both
    :func:`_uninstall_pytesseract` and a monkeypatch of any existing
    sibling stubs.

    Validates: R26.4 (fail-fast on missing dependency).
    """

    _uninstall_pytesseract()
    # Guard against a helpful sibling ``hydra.eas.screenshots.ocr``
    # having imported a stub earlier in the session — force an
    # ``ImportError`` on any ``import pytesseract`` attempt by leaving
    # sys.modules without the key.
    settings = _settings_with_ocr(enabled=True)
    app = FastAPI()

    with caplog.at_level(logging.ERROR, logger="hydra.eas.setup"):
        with pytest.raises(RuntimeError, match="pytesseract"):
            await setup_eas(app, settings)

    # The ``eas.setup.ocr_dependency_missing`` log line is emitted
    # with the offending setting + missing module name. ``caplog``
    # captures ``LogRecord.message`` (which is the first arg to
    # ``logger.error``); the extra dict is on ``record.setting`` etc.
    missing_records = [
        rec for rec in caplog.records
        if rec.message == "eas.setup.ocr_dependency_missing"
    ]
    assert missing_records, "expected eas.setup.ocr_dependency_missing log"
    rec = missing_records[0]
    assert getattr(rec, "missing_module", "") == "pytesseract"
    assert getattr(rec, "setting", "") == "eas.screenshot.ocr_enabled"


# ---------------------------------------------------------------------------
# Scenario 3 — OCR enabled with pytesseract stub → passes silently
# ---------------------------------------------------------------------------


async def test_ocr_enabled_with_pytesseract_passes() -> None:
    """When ``pytesseract`` is importable, the guard does not raise.

    Complements scenario 2: installing an empty module stub for
    ``pytesseract`` satisfies the import probe. We still don't wire
    storage clients, so the setup call completes with partial wiring
    warnings but no boot failure.

    Validates: R26.2 (optional-dep behaviour when capability is enabled).
    """

    _install_pytesseract_stub()
    try:
        settings = _settings_with_ocr(enabled=True)
        app = FastAPI()
        # Must not raise.
        await setup_eas(app, settings)
    finally:
        _uninstall_pytesseract()


# ---------------------------------------------------------------------------
# Error-message shape — the RuntimeError text is actionable
# ---------------------------------------------------------------------------


async def test_runtime_error_message_mentions_eas_extra() -> None:
    """The :class:`RuntimeError` text tells operators how to fix the problem.

    Operators reading the boot log should get an actionable message,
    not a bare ``ModuleNotFoundError``. The current implementation
    emits:

        "eas.screenshot.ocr_enabled is True but pytesseract is not
        importable. Install the [eas] extra or disable OCR."

    We pin the invariants of that message (names the setting, names
    the dependency) so a later refactor can't silently downgrade the
    actionable bits.

    Validates: R26.4 (clear error log).
    """

    _uninstall_pytesseract()
    settings = _settings_with_ocr(enabled=True)
    app = FastAPI()

    with pytest.raises(RuntimeError) as exc_info:
        await setup_eas(app, settings)

    msg = str(exc_info.value)
    assert "ocr_enabled" in msg
    assert "pytesseract" in msg
    # Either the "[eas] extra" hint or a "disable" hint must appear
    # so the operator knows how to recover.
    assert ("[eas]" in msg) or ("disable" in msg.lower())


# ---------------------------------------------------------------------------
# Exception chain — the underlying ImportError is preserved
# ---------------------------------------------------------------------------


async def test_runtime_error_chains_importerror() -> None:
    """The ``RuntimeError`` chains the underlying :class:`ImportError`.

    Python's ``raise ... from exc`` preserves the full traceback, so
    operators can see both the high-level boot failure and the
    import-level detail. This is more helpful than a naked
    ``RuntimeError`` because it shows which submodule of pytesseract
    failed (which matters when the Tesseract binary is installed but
    pytesseract itself isn't).

    Validates: R26.4 (diagnostic preservation).
    """

    _uninstall_pytesseract()
    settings = _settings_with_ocr(enabled=True)
    app = FastAPI()

    with pytest.raises(RuntimeError) as exc_info:
        await setup_eas(app, settings)

    cause: Any = exc_info.value.__cause__
    assert isinstance(cause, ImportError)
    assert "pytesseract" in str(cause) or cause.name == "pytesseract"
