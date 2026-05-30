"""Optional OCR for screenshot captures (R6.5, R26.2).

:class:`OCRProcessor` wraps Tesseract via ``pytesseract``. The whole module
is optional — callers that never turn on OCR never pay the import cost.

Design choices:

* **Lazy import of ``pytesseract`` and ``PIL.Image``.** Both are pulled in
  on first :meth:`extract_text` call, not at module import. Two reasons:

  1. R26.2 says "only hard-require the dependency when the capability is
     actually enabled" — a deployment that leaves
     ``EASSettings.screenshot.ocr_enabled = False`` should not need the
     extras installed.
  2. Importing ``pytesseract`` at module load time executes a subprocess
     check for the Tesseract binary — slow and noisy in test envs that
     don't have the binary.

* **Bytes-in, str-out contract.** :meth:`extract_text` accepts the raw PNG
  bytes produced by :class:`PlaywrightRenderer` and returns a stripped,
  length-capped string. No intermediate files, no caller-visible PIL
  image object.

* **Graceful failure.** If the ``pytesseract``/``PIL`` import or the
  Tesseract call fails (missing binary, corrupted PNG, timeout),
  :meth:`extract_text` returns ``""`` after logging at INFO. The Screenshot
  adapter runs OCR best-effort and downstream metadata omits
  ``ocr_excerpt`` when the text is empty (see ``adapter.py``).

* **Blocking call moved to a thread.** ``pytesseract.image_to_string`` is
  synchronous — we hand it to :func:`asyncio.to_thread` so the OCR pass
  does not block the screenshot worker's event loop. Tesseract itself is
  CPU-bound on big captures (seconds at 2560×1440) so the thread hop is
  meaningful.

* **Hard length cap.** ``max_chars`` from :attr:`EASSettings.screenshot.ocr_max_chars`
  is applied *after* stripping so we do not half-cut a trailing newline
  token. Above the cap we truncate; below it we return as-is.

The class is intentionally thin. More elaborate post-processing (language
detection, confidence filtering, page-segmentation-mode overrides) would
live here if we add it — but the R6.5 contract is just "a truncated
string" so this is all that is required for the MVP.
"""

from __future__ import annotations

import asyncio
import io
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing only
    pass

logger = logging.getLogger(__name__)

__all__ = ["OCRProcessor"]


# Default cap on the returned OCR excerpt. The ES mapping's
# ``ocr_excerpt`` field uses ``ignore_above=1024`` so any call site that
# doesn't pass ``max_chars`` gets a safe upper bound that fits cleanly.
_DEFAULT_MAX_CHARS = 8192


class OCRProcessor:
    """Thin async wrapper around ``pytesseract.image_to_string``.

    Constructed once per process (via :func:`setup_eas`) and shared by
    every :class:`ScreenshotAdapter` instance. Holds no per-call state,
    so concurrency is limited only by Tesseract's own per-process
    behaviour.

    Parameters
    ----------
    language:
        Tesseract language hint; passed as the ``lang`` kwarg on every
        ``image_to_string`` call. Defaults to ``"eng"`` which covers the
        HYDRA MVP — multi-language OCR is a future enhancement.
    timeout_seconds:
        Upper bound on the per-call Tesseract run. Exceeding this
        returns an empty string rather than raising, so the capture
        path is not blocked by a slow OCR pass. ``0`` disables the
        timeout.
    """

    __slots__ = ("_language", "_timeout_seconds", "_lib_cache")

    def __init__(
        self,
        *,
        language: str = "eng",
        timeout_seconds: float = 20.0,
    ) -> None:
        self._language = language
        self._timeout_seconds = max(0.0, float(timeout_seconds))
        # Lazy-populated cache of the imported (pytesseract, Image)
        # pair. Setting it to ``None`` up front lets :meth:`_load_libs`
        # return a stable sentinel when imports fail, so subsequent
        # calls skip the import attempt and return ``""`` fast.
        self._lib_cache: tuple[Any, Any] | None | object = _UNSET

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def extract_text(
        self,
        png_bytes: bytes,
        *,
        max_chars: int = _DEFAULT_MAX_CHARS,
    ) -> str:
        """Return up to ``max_chars`` of OCR text from ``png_bytes``.

        Returns the empty string when:

        * ``png_bytes`` is empty.
        * The optional dependencies (``pytesseract``, ``PIL``) or the
          Tesseract binary are unavailable.
        * The Tesseract call raises or times out.

        The OCR text is stripped of leading / trailing whitespace
        before truncation; downstream consumers (ES ``ocr_excerpt``
        mapping, API response) rely on the returned value being a
        tight UTF-8 string.
        """

        if not png_bytes:
            return ""

        libs = self._load_libs()
        if libs is None:
            return ""
        pytesseract, Image = libs

        try:
            raw = await self._run_tesseract(pytesseract, Image, png_bytes)
        except Exception as exc:  # noqa: BLE001 — OCR is best-effort
            logger.info(
                "eas.ocr.extract_failed",
                extra={"error": str(exc), "png_size": len(png_bytes)},
            )
            return ""

        stripped = (raw or "").strip()
        if max_chars is None or max_chars <= 0:
            return stripped
        if len(stripped) <= max_chars:
            return stripped
        return stripped[:max_chars]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _load_libs(self) -> tuple[Any, Any] | None:
        """Import ``pytesseract`` and ``PIL.Image`` on first call.

        Returns ``None`` when either import fails — the caller then
        returns an empty string. The result is cached on the instance
        so a single failure does not re-trigger the import attempt
        on every future call.
        """

        if self._lib_cache is _UNSET:
            try:
                import pytesseract  # type: ignore[import-not-found]
                from PIL import Image  # type: ignore[import-not-found]
            except ImportError as exc:
                logger.info(
                    "eas.ocr.import_failed",
                    extra={"error": str(exc)},
                )
                self._lib_cache = None
            else:
                self._lib_cache = (pytesseract, Image)
        return self._lib_cache  # type: ignore[return-value]

    async def _run_tesseract(
        self,
        pytesseract: Any,
        Image: Any,
        png_bytes: bytes,
    ) -> str:
        """Run the blocking Tesseract call in a worker thread.

        ``pytesseract.image_to_string`` accepts a PIL ``Image`` directly
        — we decode the PNG from memory, hand the Image object to
        Tesseract, and close it after the call so libpng / PIL release
        their decoder state promptly. The whole pass runs inside
        :func:`asyncio.to_thread` so the screenshot worker's event
        loop is not blocked for the (potentially multi-second) OCR
        duration.
        """

        language = self._language
        timeout = self._timeout_seconds

        def _blocking() -> str:
            # Decode once; ``pytesseract`` will re-read pixel data from
            # this Image so we need to keep it open for the duration of
            # the call. A try/finally guarantees cleanup even if
            # Tesseract raises.
            image = Image.open(io.BytesIO(png_bytes))
            try:
                kwargs: dict[str, Any] = {"lang": language}
                if timeout > 0:
                    # ``pytesseract`` calls the Tesseract binary with
                    # this timeout; raises ``RuntimeError`` on timeout,
                    # which the caller catches.
                    kwargs["timeout"] = timeout
                return pytesseract.image_to_string(image, **kwargs) or ""
            finally:
                try:
                    image.close()
                except Exception:  # noqa: BLE001 — close is best-effort
                    pass

        return await asyncio.to_thread(_blocking)


# Sentinel so we can distinguish "not loaded" from a cached ``None`` that
# means "imports failed, don't try again". ``object()`` is cheaper and
# safer than a string marker — no chance of collision with a real value.
_UNSET: Any = object()
