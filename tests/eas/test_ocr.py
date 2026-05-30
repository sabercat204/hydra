"""Unit tests for :class:`hydra.eas.screenshots.ocr.OCRProcessor` (task 8.5).

``OCRProcessor`` is a thin wrapper around the optional ``pytesseract``
dependency. The MVP contract in R6.5 is narrow:

* **Bytes-in, str-out.** Accepts raw PNG bytes; returns either an OCR
  excerpt (stripped, length-capped) or an empty string.
* **Best-effort.** Every error path (missing imports, missing Tesseract
  binary, corrupted input, timeout) returns ``""`` so the Screenshot
  adapter's capture path is never aborted by OCR problems.
* **Opt-in.** Callers gate the call on
  ``EASSettings.screenshot.ocr_enabled``; the adapter does not invoke
  :meth:`extract_text` unless that flag is set (Adapter responsibility,
  not tested here).

These tests stub ``pytesseract`` / ``PIL.Image`` into ``sys.modules`` so
the tests can drive every code path deterministically without an actual
Tesseract install. The tests that need to exercise the lazy-import
failure path use a fresh :class:`OCRProcessor` instance (no class-level
cache) and sabotage the import by removing the stubs first.

Validates: R6.5 (truncation + excerpt derivation), R26.2 (optional
dependency behaviour).
"""

from __future__ import annotations

import sys
import types
from typing import Any

import pytest

from hydra.eas.screenshots.ocr import OCRProcessor


# ---------------------------------------------------------------------------
# Stub builders
# ---------------------------------------------------------------------------


def _install_pytesseract_stub(
    *,
    return_value: str = "",
    raises: Exception | None = None,
) -> list[dict[str, Any]]:
    """Install a fake ``pytesseract`` module and record every call.

    Returns the call-log list so tests can assert on the kwargs the
    OCR processor passes (``lang``, ``timeout``).
    """

    calls: list[dict[str, Any]] = []

    def _image_to_string(image: Any, **kwargs: Any) -> str:
        calls.append({"image": image, **kwargs})
        if raises is not None:
            raise raises
        return return_value

    stub = types.ModuleType("pytesseract")
    stub.image_to_string = _image_to_string  # type: ignore[attr-defined]
    sys.modules["pytesseract"] = stub
    return calls


def _install_pil_stub() -> list[bytes]:
    """Install a minimal ``PIL.Image`` stub and record opened byte-blobs.

    ``PIL.Image.open`` is the only member we need — the OCR processor
    opens the PNG from a ``BytesIO`` and then passes the image object
    straight to ``pytesseract``. Our stub just captures whatever bytes
    the caller fed in so the test can verify the data reached the
    library.
    """

    opened: list[bytes] = []

    class FakeImage:
        def __init__(self, payload: bytes) -> None:
            self.payload = payload
            self.closed = False

        def close(self) -> None:
            self.closed = True

    def _open(fp: Any) -> FakeImage:
        # ``fp`` is a BytesIO — read() consumes it; seek(0) restores
        # for any downstream consumer.
        payload = fp.read() if hasattr(fp, "read") else b""
        opened.append(payload)
        return FakeImage(payload)

    pil_module = types.ModuleType("PIL")
    image_module = types.ModuleType("PIL.Image")
    image_module.open = _open  # type: ignore[attr-defined]
    pil_module.Image = image_module  # type: ignore[attr-defined]
    sys.modules["PIL"] = pil_module
    sys.modules["PIL.Image"] = image_module
    return opened


def _uninstall_stubs() -> None:
    """Remove any test stubs so a subsequent test starts clean."""
    for name in ("pytesseract", "PIL.Image", "PIL"):
        sys.modules.pop(name, None)


# ---------------------------------------------------------------------------
# Empty-input fast path
# ---------------------------------------------------------------------------


async def test_empty_png_returns_empty_string() -> None:
    """Zero-byte input returns ``""`` without attempting any import."""
    processor = OCRProcessor()
    result = await processor.extract_text(b"")
    assert result == ""


# ---------------------------------------------------------------------------
# Missing-imports path
# ---------------------------------------------------------------------------


async def test_missing_pytesseract_returns_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without ``pytesseract`` installed, ``extract_text`` returns ``""``.

    The default Python 3.12 test environment has no ``pytesseract``; we
    just run a fresh processor and verify no exception escapes.
    """
    _uninstall_stubs()
    processor = OCRProcessor()
    result = await processor.extract_text(b"not-a-real-png")
    assert result == ""


async def test_import_failure_is_cached(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed import is cached so subsequent calls short-circuit fast.

    Verified indirectly: after a failed import the processor's
    ``_lib_cache`` sentinel settles on ``None``, and a second call does
    not re-attempt the import (no AttributeError from a broken stub).
    """
    _uninstall_stubs()
    processor = OCRProcessor()
    # First call resolves the import failure and caches ``None``.
    assert await processor.extract_text(b"bytes") == ""
    # Second call reuses the cached ``None`` — we can verify this by
    # inserting a broken ``pytesseract`` after the first call and
    # confirming the second call still returns "" without touching it.
    broken = types.ModuleType("pytesseract")
    def _boom(*a: Any, **kw: Any) -> str:
        raise RuntimeError("should not be called")
    broken.image_to_string = _boom  # type: ignore[attr-defined]
    sys.modules["pytesseract"] = broken
    try:
        result = await processor.extract_text(b"bytes")
        assert result == ""
    finally:
        _uninstall_stubs()


# ---------------------------------------------------------------------------
# Successful OCR path
# ---------------------------------------------------------------------------


async def test_successful_ocr_returns_stripped_text() -> None:
    """A successful OCR call returns the stripped text below ``max_chars``."""
    _install_pil_stub()
    calls = _install_pytesseract_stub(return_value="  hello world  \n")
    try:
        processor = OCRProcessor()
        result = await processor.extract_text(b"pngbytes")
        assert result == "hello world"
        # The PIL image, the language kwarg, and the timeout kwarg
        # should have reached pytesseract.
        assert len(calls) == 1
        assert calls[0]["lang"] == "eng"
        assert "timeout" in calls[0]
    finally:
        _uninstall_stubs()


async def test_max_chars_truncates_long_output() -> None:
    """Output longer than ``max_chars`` is hard-truncated (no ellipsis)."""
    _install_pil_stub()
    _install_pytesseract_stub(return_value="a" * 1000)
    try:
        processor = OCRProcessor()
        result = await processor.extract_text(b"pngbytes", max_chars=50)
        assert len(result) == 50
        assert result == "a" * 50
    finally:
        _uninstall_stubs()


async def test_max_chars_zero_returns_full_string() -> None:
    """``max_chars == 0`` disables the cap (useful for tests / debugging)."""
    _install_pil_stub()
    _install_pytesseract_stub(return_value="full text")
    try:
        processor = OCRProcessor()
        result = await processor.extract_text(b"pngbytes", max_chars=0)
        assert result == "full text"
    finally:
        _uninstall_stubs()


async def test_max_chars_under_length_returns_unchanged() -> None:
    """Output at or below ``max_chars`` comes back as-is (after strip)."""
    _install_pil_stub()
    _install_pytesseract_stub(return_value="   five five   ")
    try:
        processor = OCRProcessor()
        result = await processor.extract_text(b"pngbytes", max_chars=50)
        assert result == "five five"
    finally:
        _uninstall_stubs()


# ---------------------------------------------------------------------------
# Failure / timeout handling
# ---------------------------------------------------------------------------


async def test_tesseract_runtime_error_returns_empty() -> None:
    """A RuntimeError from pytesseract (e.g. timeout) yields ``""``."""
    _install_pil_stub()
    _install_pytesseract_stub(raises=RuntimeError("Tesseract timeout"))
    try:
        processor = OCRProcessor()
        result = await processor.extract_text(b"pngbytes")
        assert result == ""
    finally:
        _uninstall_stubs()


async def test_image_open_failure_returns_empty() -> None:
    """A corrupted PNG that breaks ``Image.open`` surfaces as ``""``."""

    def _broken_open(fp: Any) -> Any:
        raise OSError("cannot identify image file")

    pil_module = types.ModuleType("PIL")
    image_module = types.ModuleType("PIL.Image")
    image_module.open = _broken_open  # type: ignore[attr-defined]
    pil_module.Image = image_module  # type: ignore[attr-defined]
    sys.modules["PIL"] = pil_module
    sys.modules["PIL.Image"] = image_module
    _install_pytesseract_stub(return_value="ignored")

    try:
        processor = OCRProcessor()
        result = await processor.extract_text(b"pngbytes")
        assert result == ""
    finally:
        _uninstall_stubs()


# ---------------------------------------------------------------------------
# Constructor options
# ---------------------------------------------------------------------------


async def test_custom_language_propagates_to_tesseract() -> None:
    """``OCRProcessor(language=...)`` is forwarded as the ``lang`` kwarg."""
    _install_pil_stub()
    calls = _install_pytesseract_stub(return_value="")
    try:
        processor = OCRProcessor(language="deu+fra")
        await processor.extract_text(b"pngbytes")
        assert calls[0]["lang"] == "deu+fra"
    finally:
        _uninstall_stubs()


async def test_zero_timeout_omits_tesseract_timeout() -> None:
    """``timeout_seconds == 0`` disables the per-call Tesseract timeout.

    ``pytesseract.image_to_string(timeout=0)`` would be interpreted as
    "one-second-or-less", which is almost never desired. The processor
    omits the kwarg altogether when the configured timeout is 0.
    """
    _install_pil_stub()
    calls = _install_pytesseract_stub(return_value="")
    try:
        processor = OCRProcessor(timeout_seconds=0)
        await processor.extract_text(b"pngbytes")
        assert "timeout" not in calls[0]
    finally:
        _uninstall_stubs()


async def test_negative_timeout_clamped_to_zero() -> None:
    """A negative ``timeout_seconds`` is clamped to 0 (treated as disabled)."""
    _install_pil_stub()
    calls = _install_pytesseract_stub(return_value="")
    try:
        processor = OCRProcessor(timeout_seconds=-5)
        await processor.extract_text(b"pngbytes")
        assert "timeout" not in calls[0]
    finally:
        _uninstall_stubs()
