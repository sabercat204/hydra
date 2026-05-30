"""Playwright wrapper for the Screenshot_Adapter (Design §3.3, §8.2, R6.1).

:class:`PlaywrightRenderer` launches a headless Chromium instance, navigates
to the supplied URL with the caller's viewport/timeout/user-agent, and
returns a :class:`RenderResult` containing PNG bytes, the HTTP status, the
page title, and a classified ``error_class`` on failure.

Key design points:

* **Sandboxing** — ``--no-sandbox --disable-dev-shm-usage --disable-gpu
  --disable-background-networking`` is the minimum arg set for running
  inside the platform container (Design §3.3).
* **DNS pinning** — when ``host_resolver_rules`` is supplied (produced by
  :func:`hydra.eas.screenshots.ssrf_guard.host_resolver_rules`), it is
  appended as ``--host-resolver-rules=<value>`` so Chromium cannot
  rebind the hostname to a different address mid-session.
* **TLS enforcement** — ``ignore_https_errors=False`` (R6.1 extension in
  design §13.2). A TLS failure becomes a recorded render error rather
  than a silently-captured page.
* **Error classification** — every Playwright exception maps to a stable
  string so downstream metrics and records can label failure modes
  (R6.3). Timeouts become ``"TimeoutError"``, TLS errors become
  ``"TLSError"``, generic navigation errors become ``"NavigationError"``,
  everything else becomes ``"Unknown:{exc_type}"``.

Import-safety: Playwright is an optional runtime dependency (``[eas]``
extra). The module imports Playwright lazily inside :meth:`render` so that
test environments without the dependency can still import the module and
call :func:`hydra.eas.screenshots.ssrf_guard.is_safe_url` / similar pure
functions. :meth:`render` raises :class:`ImportError` with a clear message
on first use when Playwright is unavailable.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

__all__ = [
    "PlaywrightRenderer",
    "RenderResult",
]

logger = logging.getLogger(__name__)


# Chromium launch arguments we always pass. Listed as a module-level constant
# so tests can assert them without instantiating the renderer.
_CHROMIUM_ARGS: tuple[str, ...] = (
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-background-networking",
)


@dataclass(frozen=True)
class RenderResult:
    """Outcome of a single render attempt.

    ``error_class`` is ``None`` on success and a short label string on
    failure (``"TimeoutError"``, ``"NavigationError"``, ``"TLSError"``,
    or ``"Unknown:{exc_type}"``). When ``error_class`` is non-``None`` the
    ``png_bytes`` field is always empty — the adapter uses this invariant
    to decide whether to write a blob to MinIO.
    """

    png_bytes: bytes
    http_status: int
    title: str
    error_class: str | None


def _classify_error(exc: BaseException) -> str:
    """Map a Playwright (or other) exception to a stable label.

    Ordering matters: we check for TLS first because some Playwright
    builds surface TLS errors as a generic ``Error`` whose message
    contains ``"ssl"`` or ``"certificate"`` — without the string
    inspection those would be misclassified as ``NavigationError``.
    """

    message = str(exc).lower()
    # TLS / certificate issues — inspect message before class name.
    if any(
        token in message
        for token in (
            "ssl",
            "cert",
            "tls",
            "err_cert_",
            "err_ssl_",
        )
    ):
        return "TLSError"

    exc_class = type(exc).__name__
    if exc_class == "TimeoutError":
        return "TimeoutError"
    # Playwright's base navigation/network error.
    if exc_class == "Error":
        return "NavigationError"
    return f"Unknown:{exc_class}"


class PlaywrightRenderer:
    """Thin async wrapper around Playwright's Chromium launcher.

    Deliberately stateless: every :meth:`render` call spins up a fresh
    browser instance. A future optimisation (post-MVP) is to reuse the
    browser across calls — for the MVP the per-call cost is acceptable
    because the screenshot worker has a small concurrency ceiling (default
    4) and renders are network-bound.
    """

    def __init__(self) -> None:
        # Nothing to initialise up front; kept as a class to match the
        # design diagrams (§8.2) and leave room for a future pool.
        pass

    async def render(
        self,
        url: str,
        viewport: tuple[int, int],
        timeout_seconds: int,
        user_agent: str,
        host_resolver_rules: str | None = None,
    ) -> RenderResult:
        """Render ``url`` and return a :class:`RenderResult`.

        Parameters
        ----------
        url:
            The URL to navigate to. The caller is responsible for running
            the SSRF guard **before** calling this method.
        viewport:
            ``(width, height)`` in CSS pixels.
        timeout_seconds:
            Navigation timeout; Playwright expects milliseconds so we
            multiply by 1000 internally.
        user_agent:
            The ``User-Agent`` string for every outbound request.
        host_resolver_rules:
            When non-``None``, appended to the Chromium launch args as
            ``--host-resolver-rules=<value>`` to pin DNS resolution.
            Produced by :func:`host_resolver_rules` in :mod:`ssrf_guard`.

        Returns
        -------
        :class:`RenderResult`
            On success, ``error_class is None`` and ``png_bytes`` carries
            the rendered PNG. On failure, ``png_bytes == b""`` and
            ``error_class`` is a stable label.
        """

        # Lazy import so module load doesn't require Playwright.
        try:
            from playwright.async_api import (
                Error as PWError,
                TimeoutError as PWTimeoutError,
                async_playwright,
            )
        except ImportError as exc:  # pragma: no cover - import guard
            raise ImportError(
                "Playwright is required for ScreenshotAdapter.render(). "
                "Install the `[eas]` extra (pip install 'hydra[eas]') "
                "and run `playwright install --with-deps chromium`."
            ) from exc

        launch_args = list(_CHROMIUM_ARGS)
        if host_resolver_rules:
            launch_args.append(f"--host-resolver-rules={host_resolver_rules}")

        width, height = viewport
        timeout_ms = timeout_seconds * 1000

        try:
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(
                    headless=True,
                    args=launch_args,
                )
                try:
                    context = await browser.new_context(
                        viewport={"width": int(width), "height": int(height)},
                        user_agent=user_agent,
                        ignore_https_errors=False,
                    )
                    try:
                        page = await context.new_page()
                        try:
                            response = await page.goto(
                                url,
                                timeout=timeout_ms,
                                wait_until="networkidle",
                            )
                        except PWTimeoutError as exc:
                            logger.debug(
                                "eas.renderer.timeout",
                                extra={"url": url, "timeout_ms": timeout_ms},
                            )
                            return RenderResult(
                                png_bytes=b"",
                                http_status=0,
                                title="",
                                error_class=_classify_error(exc),
                            )
                        except PWError as exc:
                            logger.debug(
                                "eas.renderer.navigation_failed",
                                extra={"url": url, "error": str(exc)},
                            )
                            return RenderResult(
                                png_bytes=b"",
                                http_status=0,
                                title="",
                                error_class=_classify_error(exc),
                            )

                        # ``response`` can legitimately be ``None`` when the
                        # navigation is to an about:blank / file: URL or
                        # when a redirect chain is aborted by the browser.
                        # In that case the HTTP status is unknown — record
                        # 0 so downstream code can skip the field.
                        http_status = (
                            int(response.status) if response is not None else 0
                        )

                        try:
                            png_bytes = await page.screenshot(full_page=False)
                        except PWError as exc:
                            return RenderResult(
                                png_bytes=b"",
                                http_status=http_status,
                                title="",
                                error_class=_classify_error(exc),
                            )

                        try:
                            title = await page.title()
                        except PWError:
                            # A missing title after a successful render is
                            # benign; fall back to the empty string.
                            title = ""

                        return RenderResult(
                            png_bytes=bytes(png_bytes),
                            http_status=http_status,
                            title=title,
                            error_class=None,
                        )
                    finally:
                        try:
                            await context.close()
                        except Exception:  # noqa: BLE001 — teardown is best-effort
                            pass
                finally:
                    try:
                        await browser.close()
                    except Exception:  # noqa: BLE001 — teardown is best-effort
                        pass
        except ImportError:  # pragma: no cover - bubbles to caller
            raise
        except Exception as exc:  # noqa: BLE001 — catch-all for Playwright boot failures
            logger.warning(
                "eas.renderer.unexpected_error",
                extra={"url": url, "error": str(exc), "exc_type": type(exc).__name__},
            )
            return RenderResult(
                png_bytes=b"",
                http_status=0,
                title="",
                error_class=_classify_error(exc),
            )
